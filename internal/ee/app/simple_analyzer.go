package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// SimpleEOAAnalyzer 간단한 EOA 분석기 구현체
// ! 테스트용과 프로덕션용 모두 지원하는 기본 구현
type SimpleEOAAnalyzer struct {
	// Core domain components
	groundKnowledge *domain.GroundKnowledge
	dualManager     *domain.DualManager
	graphRepo       domain.GraphRepository

	// Channel processing (backward compatibility)
	txChannel    chan *shareddomain.MarkedTransaction
	stopChannel  chan struct{}
	stopOnce     sync.Once
	shutdownOnce sync.Once
	wg           sync.WaitGroup

	// Transaction consumer (Kafka 기반)
	txConsumer kafka.TransactionConsumer

	// Configuration
	config *EOAAnalyzerConfig

	// Statistics (thread-safe atomic counters)
	stats SimpleAnalyzerStats
}

// SimpleAnalyzerStats 간단한 분석기 통계
type SimpleAnalyzerStats struct {
	TotalProcessed    int64
	SuccessCount      int64
	ErrorCount        int64
	DepositDetections int64
	GraphUpdates      int64
	WindowUpdates     int64
	DroppedTxs        int64
	StartTime         time.Time
}

// NewProductionEOAAnalyzer 프로덕션용 분석기 생성
func NewProductionEOAAnalyzer(config *EOAAnalyzerConfig) (EOAAnalyzer, error) {
	return newSimpleAnalyzer(config)
}

// NewTestingEOAAnalyzer 테스트용 분석기 생성
func NewTestingEOAAnalyzer(config *EOAAnalyzerConfig) (EOAAnalyzer, error) {
	return newSimpleAnalyzer(config)
}

// newSimpleAnalyzer 공통 분석기 생성 로직
func newSimpleAnalyzer(config *EOAAnalyzerConfig) (*SimpleEOAAnalyzer, error) {
	log.Printf("🚀 Initializing Simple EOA Analyzer: %s (Mode: %s)", config.Name, config.Mode)

	// 데이터 디렉토리 생성
	if err := os.MkdirAll(config.DataPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// CEX 저장소 초기화 - 설정에서 파일 경로 사용
	cexFilePath := config.CEXFilePath
	if cexFilePath == "" {
		// 기본 경로 사용 (후방 호환성)
		cexFilePath = "internal/ee/cex.txt"
	}
	cexRepo := infra.NewFileCEXRepository(cexFilePath)
	cexSet, err := cexRepo.LoadCEXSet()
	if err != nil {
		return nil, fmt.Errorf("failed to load CEX set from %s: %w", cexFilePath, err)
	}
	log.Printf("📦 Loaded %d CEX addresses", cexSet.Size())

	// Deposit 저장소 초기화 - 모드에 따른 경로 설정
	var detectedDepositFilePath string
	if config.Mode == TestingMode {
		detectedDepositFilePath = config.DataPath + "/test_detected_deposits.csv"
	} else {
		detectedDepositFilePath = config.DataPath + "/production_detected_deposits.csv"
	}
	depositRepo := infra.NewFileDepositRepository(detectedDepositFilePath)

	// GroundKnowledge 생성
	groundKnowledge := domain.NewGroundKnowledge(cexSet, depositRepo)
	if err := groundKnowledge.Load(); err != nil {
		return nil, fmt.Errorf("failed to load ground knowledge: %w", err)
	}
	log.Printf("🧠 Ground knowledge loaded")

	// Graph Repository 초기화
	graphRepo, err := infra.NewBadgerGraphRepository(config.GraphDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create graph repository: %w", err)
	}
	log.Printf("🗂️  Graph repository at: %s", config.GraphDBPath)

	// DualManager 초기화
	dualManager, err := domain.NewDualManager(groundKnowledge, graphRepo, config.PendingDBPath)
	if err != nil {
		graphRepo.Close()
		return nil, fmt.Errorf("failed to create dual manager: %w", err)
	}
	log.Printf("🔄 DualManager with pending DB at: %s", config.PendingDBPath)

	// Transaction Consumer 초기화 - 모드에 따라 다른 토픽 사용
	kafkaBrokers := []string{"localhost:9092"}
	isTestMode := (config.Mode == TestingMode)
	groupID := fmt.Sprintf("ee-analyzer-%s", strings.ReplaceAll(config.Name, " ", "-"))
	
	txConsumer := kafka.NewKafkaTransactionConsumer(kafkaBrokers, isTestMode, groupID)
	log.Printf("📡 Transaction consumer initialized (test mode: %v)", isTestMode)

	analyzer := &SimpleEOAAnalyzer{
		groundKnowledge: groundKnowledge,
		dualManager:     dualManager,
		graphRepo:       graphRepo,
		txChannel:       make(chan *shareddomain.MarkedTransaction, config.ChannelBufferSize),
		stopChannel:     make(chan struct{}),
		txConsumer:      txConsumer,
		config:          config,
		stats: SimpleAnalyzerStats{
			StartTime: time.Now(),
		},
	}

	log.Printf("✅ Simple EOA Analyzer created: %s", config.Name)
	return analyzer, nil
}

// Start 분석기 시작
func (a *SimpleEOAAnalyzer) Start(ctx context.Context) error {
	log.Printf("🚀 Starting Simple Analyzer: %s", a.config.Name)

	// Transaction consumer 시작
	if a.txConsumer != nil {
		if err := a.txConsumer.Start(ctx, a.txChannel); err != nil {
			return fmt.Errorf("failed to start transaction consumer: %w", err)
		}
	}

	// 워커 고루틴들 시작
	for i := 0; i < a.config.WorkerCount; i++ {
		a.wg.Add(1)
		go a.transactionWorker(ctx, i)
	}

	// 통계 리포터 시작
	a.wg.Add(1)
	go a.statsReporter(ctx)

	log.Printf("✅ Simple Analyzer started: %s (%d workers + kafka consumer)", a.config.Name, a.config.WorkerCount)

	// 컨텍스트 취소 또는 정지 시그널 대기
	select {
	case <-ctx.Done():
		log.Printf("🛑 Context cancelled: %s", a.config.Name)
	case <-a.stopChannel:
		log.Printf("🛑 Stop signal received: %s", a.config.Name)
	}

	return a.shutdown()
}

// Stop 분석기 중지
func (a *SimpleEOAAnalyzer) Stop() error {
	a.stopOnce.Do(func() {
		close(a.stopChannel)
	})
	return nil
}

// ProcessTransaction 트랜잭션 처리 (non-blocking)
func (a *SimpleEOAAnalyzer) ProcessTransaction(tx *shareddomain.MarkedTransaction) error {
	select {
	case a.txChannel <- tx:
		return nil
	default:
		atomic.AddInt64(&a.stats.DroppedTxs, 1)
		return fmt.Errorf("channel full, dropped tx: %s", tx.TxID.String()[:8])
	}
}

// ProcessTransactions 배치 트랜잭션 처리
func (a *SimpleEOAAnalyzer) ProcessTransactions(txs []*shareddomain.MarkedTransaction) error {
	for _, tx := range txs {
		if err := a.ProcessTransaction(tx); err != nil {
			continue // 개별 실패는 무시하고 계속 처리
		}
	}
	return nil
}

// transactionWorker 트랜잭션 처리 워커
func (a *SimpleEOAAnalyzer) transactionWorker(ctx context.Context, workerID int) {
	defer a.wg.Done()
	log.Printf("🔧 Worker %d started", workerID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("🔧 Worker %d stopping (context)", workerID)
			return
		case <-a.stopChannel:
			log.Printf("🔧 Worker %d stopping (signal)", workerID)
			return
		case tx := <-a.txChannel:
			a.processSingleTransaction(tx, workerID)
		}
	}
}

// processSingleTransaction 개별 트랜잭션 처리
func (a *SimpleEOAAnalyzer) processSingleTransaction(tx *shareddomain.MarkedTransaction, workerID int) {
	processedCount := atomic.AddInt64(&a.stats.TotalProcessed, 1)

	// 처음 몇 개 트랜잭션은 디버깅 로그 출력
	if processedCount <= 5 {
		log.Printf("🔄 Worker %d: processing tx #%d | From: %s | To: %s",
			workerID, processedCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
	}

	// EOA-EOA 트랜잭션만 처리
	//TODO 이건 추후 제거 가능. 어차피 EE트랜잭션만 카프카 큐에 보내줄 거라서..
	//TODO 뭐, 놔둬도 상관 없긴 함.
	if tx.TxSyntax[0] != shareddomain.EOAMark || tx.TxSyntax[1] != shareddomain.EOAMark {
		if processedCount <= 5 {
			log.Printf("⏭️  Worker %d: skipping non-EOA tx #%d", workerID, processedCount)
		}
		return
	}

	// DualManager를 통한 트랜잭션 처리
	if err := a.dualManager.CheckTransaction(tx); err != nil {
		atomic.AddInt64(&a.stats.ErrorCount, 1)
		errorCount := atomic.LoadInt64(&a.stats.ErrorCount)
		if errorCount <= 5 { // 처음 5개 에러는 모두 로깅 (디버깅용)
			log.Printf("⚠️ Worker %d: processing error #%d: %v | From: %s | To: %s",
				workerID, errorCount, err, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
		} else if errorCount%20 == 1 { // 이후에는 20번째마다 로깅
			log.Printf("⚠️ Worker %d: processing error #%d: %v", workerID, errorCount, err)
		}
		return
	}

	successCount := atomic.AddInt64(&a.stats.SuccessCount, 1)

	// 처음 몇 개 성공은 로깅
	if successCount <= 5 {
		log.Printf("✅ Worker %d: success #%d | From: %s | To: %s",
			workerID, successCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
	}

	//TODO 배포 환경에선 제거 가능 or 개량 가능
	//TODO 동일 로직 중복 처리 및 분석이라 성능 저하 가능
	a.analyzeTransactionResult(tx)
}

// analyzeTransactionResult 트랜잭션 결과 분석
func (a *SimpleEOAAnalyzer) analyzeTransactionResult(tx *shareddomain.MarkedTransaction) {
	depositDetected := false

	// 처음 5개 트랜잭션의 CEX 체크 과정을 자세히 로깅
	processedCount := atomic.LoadInt64(&a.stats.SuccessCount)

	// 입금 주소 탐지
	isCEX := a.groundKnowledge.IsCEXAddress(tx.To)
	if processedCount <= 5 {
		log.Printf("🔍 CEX Check #%d: To=%s → IsCEX=%t",
			processedCount, tx.To.String(), isCEX)
	}

	if isCEX {
		depositCount := atomic.AddInt64(&a.stats.DepositDetections, 1)
		depositDetected = true
		log.Printf("🎯 DEPOSIT DETECTED #%d: From: %s → CEX: %s",
			depositCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
	}

	// 그래프/윈도우 업데이트 분류
	if a.groundKnowledge.IsDepositAddress(tx.To) {
		graphCount := atomic.AddInt64(&a.stats.GraphUpdates, 1)
		log.Printf("📊 GRAPH UPDATE #%d: From: %s → Deposit: %s",
			graphCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
	} else {
		windowCount := atomic.AddInt64(&a.stats.WindowUpdates, 1)
		if depositDetected {
			log.Printf("📈 WINDOW UPDATE #%d (with deposit): From: %s → To: %s",
				windowCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
		}
	}
}

// statsReporter 주기적 통계 출력
func (a *SimpleEOAAnalyzer) statsReporter(ctx context.Context) {
	defer a.wg.Done()

	ticker := time.NewTicker(time.Duration(a.config.StatsInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopChannel:
			return
		case <-ticker.C:
			a.printStatistics()
		}
	}
}

// printStatistics 통계 출력
func (a *SimpleEOAAnalyzer) printStatistics() {
	total := atomic.LoadInt64(&a.stats.TotalProcessed)
	success := atomic.LoadInt64(&a.stats.SuccessCount)
	errors := atomic.LoadInt64(&a.stats.ErrorCount)
	deposits := atomic.LoadInt64(&a.stats.DepositDetections)
	graphUpdates := atomic.LoadInt64(&a.stats.GraphUpdates)
	windowUpdates := atomic.LoadInt64(&a.stats.WindowUpdates)
	dropped := atomic.LoadInt64(&a.stats.DroppedTxs)

	uptime := time.Since(a.stats.StartTime)
	channelUsage := len(a.txChannel)
	channelCapacity := cap(a.txChannel)
	usagePercent := float64(channelUsage) / float64(channelCapacity) * 100

	log.Printf("📊 [%s] %s Statistics:", a.config.Mode, a.config.Name)
	log.Printf("   Uptime: %v | Processed: %d | Success: %d | Errors: %d",
		uptime.Round(time.Second), total, success, errors)
	log.Printf("   Deposits: %d | Graph: %d | Window: %d | Dropped: %d",
		deposits, graphUpdates, windowUpdates, dropped)
	log.Printf("   Channel: %d/%d (%.1f%%)", channelUsage, channelCapacity, usagePercent)

	if total > 0 {
		tps := float64(total) / uptime.Seconds()
		successRate := float64(success) / float64(total) * 100
		log.Printf("   Rate: %.1f tx/sec | Success Rate: %.1f%%", tps, successRate)
	}

	// DualManager 통계
	if windowStats := a.dualManager.GetWindowStats(); windowStats != nil {
		log.Printf("   Buckets: %v | Pending: %v",
			windowStats["active_buckets"], windowStats["pending_relations"])
	}

	// Graph 통계
	if graphStats, err := a.graphRepo.GetGraphStats(); err == nil {
		log.Printf("   Graph: %v nodes | %v edges",
			graphStats["total_nodes"], graphStats["total_edges"])
	}
}

// GetStatistics 통계 반환
func (a *SimpleEOAAnalyzer) GetStatistics() map[string]interface{} {
	return map[string]interface{}{
		"mode":               string(a.config.Mode),
		"name":               a.config.Name,
		"total_processed":    atomic.LoadInt64(&a.stats.TotalProcessed),
		"success_count":      atomic.LoadInt64(&a.stats.SuccessCount),
		"error_count":        atomic.LoadInt64(&a.stats.ErrorCount),
		"deposit_detections": atomic.LoadInt64(&a.stats.DepositDetections),
		"graph_updates":      atomic.LoadInt64(&a.stats.GraphUpdates),
		"window_updates":     atomic.LoadInt64(&a.stats.WindowUpdates),
		"dropped_txs":        atomic.LoadInt64(&a.stats.DroppedTxs),
		"uptime_seconds":     time.Since(a.stats.StartTime).Seconds(),
		"channel_usage":      len(a.txChannel),
		"channel_capacity":   cap(a.txChannel),
	}
}

// IsHealthy 헬스 상태 체크
func (a *SimpleEOAAnalyzer) IsHealthy() bool {
	total := atomic.LoadInt64(&a.stats.TotalProcessed)
	errors := atomic.LoadInt64(&a.stats.ErrorCount)

	if total == 0 {
		return true // 아직 트랜잭션이 없으면 건강함
	}

	channelUsage := float64(len(a.txChannel)) / float64(cap(a.txChannel))
	errorRate := float64(errors) / float64(total)

	// 채널 사용률 90% 이하, 에러율 10% 이하
	return channelUsage < 0.9 && errorRate < 0.1
}

// GetChannelStatus 채널 상태 반환
func (a *SimpleEOAAnalyzer) GetChannelStatus() (int, int) {
	return len(a.txChannel), cap(a.txChannel)
}

// shutdown 우아한 종료
func (a *SimpleEOAAnalyzer) shutdown() error {
	log.Printf("🔄 Shutting down: %s", a.config.Name)

	// 새 트랜잭션 수신 중지 (한 번만)
	a.shutdownOnce.Do(func() {
		close(a.txChannel)
	})

	// 모든 워커 완료 대기
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("✅ All workers stopped gracefully")
	case <-time.After(10 * time.Second):
		log.Printf("⚠️ Shutdown timeout")
	}

	// 최종 통계 출력
	if a.config.ResultReporting {
		a.printFinalReport()
	}
	a.printStatistics()

	// 리소스 정리
	if err := a.dualManager.Close(); err != nil {
		log.Printf("⚠️ Error closing dual manager: %v", err)
	}

	if err := a.graphRepo.Close(); err != nil {
		log.Printf("⚠️ Error closing graph repository: %v", err)
	}

	// 테스트 모드에서 데이터 정리
	if a.config.AutoCleanup {
		a.cleanup()
	}

	log.Printf("✅ Shutdown completed: %s", a.config.Name)
	return nil
}

// printFinalReport 최종 리포트 출력 (테스트 모드용)
func (a *SimpleEOAAnalyzer) printFinalReport() {
	log.Printf("\n" + strings.Repeat("=", 80))
	log.Printf("🎯 FINAL REPORT: %s", a.config.Name)
	log.Printf(strings.Repeat("=", 80))

	total := atomic.LoadInt64(&a.stats.TotalProcessed)
	success := atomic.LoadInt64(&a.stats.SuccessCount)
	errors := atomic.LoadInt64(&a.stats.ErrorCount)
	deposits := atomic.LoadInt64(&a.stats.DepositDetections)
	graphUpdates := atomic.LoadInt64(&a.stats.GraphUpdates)
	windowUpdates := atomic.LoadInt64(&a.stats.WindowUpdates)
	dropped := atomic.LoadInt64(&a.stats.DroppedTxs)

	uptime := time.Since(a.stats.StartTime)

	log.Printf("📊 Performance Summary:")
	log.Printf("   Total Runtime: %v", uptime.Round(time.Second))
	log.Printf("   Transactions Processed: %d", total)
	log.Printf("   Success Rate: %.2f%% (%d/%d)", float64(success)/float64(total)*100, success, total)
	log.Printf("   Processing Rate: %.1f tx/sec", float64(total)/uptime.Seconds())
	log.Printf("   Errors: %d | Dropped: %d", errors, dropped)

	log.Printf("\n🔍 Analysis Results:")
	log.Printf("   Deposit Detections: %d", deposits)
	log.Printf("   Graph Updates: %d", graphUpdates)
	log.Printf("   Window Updates: %d", windowUpdates)

	// DualManager 최종 통계
	if windowStats := a.dualManager.GetWindowStats(); windowStats != nil {
		log.Printf("\n🪟 Window Manager State:")
		for key, value := range windowStats {
			log.Printf("   %s: %v", key, value)
		}
	}

	// Graph 최종 통계
	if graphStats, err := a.graphRepo.GetGraphStats(); err == nil {
		log.Printf("\n🗂️  Graph Database State:")
		for key, value := range graphStats {
			log.Printf("   %s: %v", key, value)
		}
	}

	log.Printf(strings.Repeat("=", 80) + "\n")
}

// cleanup 테스트 데이터 정리
func (a *SimpleEOAAnalyzer) cleanup() {
	if a.config.Mode != TestingMode {
		return
	}

	log.Printf("🧹 Cleaning up test data: %s", a.config.DataPath)

	if err := os.RemoveAll(a.config.DataPath); err != nil {
		log.Printf("⚠️ Failed to cleanup test data: %v", err)
	} else {
		log.Printf("✅ Test data cleaned up")
	}
}

// Close io.Closer 인터페이스 구현
func (a *SimpleEOAAnalyzer) Close() error {
	// Transaction Consumer 정리
	if a.txConsumer != nil {
		if err := a.txConsumer.Close(); err != nil {
			log.Printf("⚠️ Error closing transaction consumer: %v", err)
		}
	}
	
	return a.Stop()
}
