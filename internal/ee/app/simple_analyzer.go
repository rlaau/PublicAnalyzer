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

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/infra"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
)

// SimpleEOAAnalyzer 간단한 EOA 분석기 구현체
// * 테스트용과 프로덕션용 모두 지원하는 기본 구현
type SimpleEOAAnalyzer struct {
	// Core domain components
	dualManager *DualManager

	// WorkerPool integration
	//내부 채널임
	stopChannel  chan struct{}
	stopOnce     sync.Once
	shutdownOnce sync.Once
	wg           sync.WaitGroup

	// Transaction consumer (Kafka 기반)
	batchMode bool // 배치 모드 활성화 여부

	// Configuration
	config *EOAAnalyzerConfig

	// Statistics (thread-safe atomic counters)
	stats SimpleAnalyzerStats

	infra infra.TotalEOAAnalyzerInfra
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
func NewProductionEOAAnalyzer(config *EOAAnalyzerConfig, ctx context.Context) (EOAAnalyzer, error) {
	infraStructure := NewInfraByConfig(config, ctx)
	return newSimpleAnalyzer(config, infraStructure)
}

// NewTestingEOAAnalyzer 테스트용 분석기 생성
func NewTestingEOAAnalyzer(config *EOAAnalyzerConfig, ctx context.Context) (EOAAnalyzer, error) {
	infraStructure := NewInfraByConfig(config, ctx)
	return newSimpleAnalyzer(config, infraStructure)
}

// newSimpleAnalyzer 공통 분석기 생성 로직
func newSimpleAnalyzer(config *EOAAnalyzerConfig, infraStructure infra.TotalEOAAnalyzerInfra) (*SimpleEOAAnalyzer, error) {
	//전체 EOA인프라에서 꺼내 쓰는 형식
	dualManagerInfra := infra.NewDualManagerInfra(infraStructure.GroundKnowledge, infraStructure.GraphRepo, infraStructure.PendingRelationRepo)
	dualManager, err := NewDualManager(*dualManagerInfra)
	if err != nil {
		infraStructure.GraphRepo.Close()
		return nil, fmt.Errorf("failed to create dual manager: %w", err)
	}
	log.Printf("🔄 DualManager with pending DB at: %s", config.PendingDBPath)

	analyzer := &SimpleEOAAnalyzer{
		infra:       infraStructure,
		dualManager: dualManager,
		stopChannel: make(chan struct{}),
		batchMode:   true, // 기본값: 배치 모드 활성화
		config:      config,
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

	// Consumer 시작 (배치 모드 or 단건 모드)
	if a.batchMode && a.infra.BatchConsumer != nil {
		// 배치 모드: 배치 Consumer 시작
		a.wg.Add(1)
		go a.batchConsumerWorker(ctx)
		log.Printf("🚀 Batch consumer started")
	} else {
		log.Printf("단건 컨슈머는 걍 지웠음.")
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
	job := NewTransactionJob(tx, a, 0) // workerID는 워커풀에서 자동 관리
	select {
	case a.infra.TxJobChannel <- job:
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

// transactionWorker는 이제 워커풀에 의해 대체됨 - 하위 호환성을 위해 주석 처리
// 실제 작업은 TransactionJob.Do()에서 처리됨

// batchConsumerWorker 배치 Consumer 워커 (고성능 배치 처리)
func (a *SimpleEOAAnalyzer) batchConsumerWorker(ctx context.Context) {
	defer a.wg.Done()

	log.Printf("🚀 Batch consumer worker started")

	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 Batch consumer worker stopping (context)")
			return
		case <-a.stopChannel:
			log.Printf("🛑 Batch consumer worker stopping (signal)")
			return
		default:
			// 배치 메시지 읽기 (블로킹)
			messages, err := a.infra.BatchConsumer.ReadMessagesBatch(ctx)
			if err != nil {
				// Context cancellation은 정상적인 종료
				if ctx.Err() != nil {
					return
				}
				// 기타 에러는 로깅하고 계속
				log.Printf("⚠️ Batch read error: %v", err)
				time.Sleep(100 * time.Millisecond) // 에러 시 짧은 대기
				continue
			}

			// 배치가 비어있으면 스킵
			if len(messages) == 0 {
				continue
			}

			// 배치 처리 (진정한 배칭!)
			a.processBatch(messages)
		}
	}
}

// processBatch 배치 메시지 처리 (고효율)
func (a *SimpleEOAAnalyzer) processBatch(messages []kafka.Message[*shareddomain.MarkedTransaction]) {
	batchSize := len(messages)
	processedCount := atomic.LoadInt64(&a.stats.TotalProcessed)

	// 배치 처리 시작 로깅 (처음 몇 배치만)
	if processedCount < 500 {
		log.Printf("📦 Processing batch of %d messages (total processed: %d)", batchSize, processedCount)
	}

	transactions := make([]*shareddomain.MarkedTransaction, 0, batchSize)

	// 1. 메시지에서 직접 트랜잭션 추출 (파싱 불필요!)
	for _, msg := range messages {
		if msg.Value != nil {
			transactions = append(transactions, msg.Value)
		} else {
			atomic.AddInt64(&a.stats.ErrorCount, 1)
		}
	}

	// 2. 트랜잭션 처리 (배치로 처리)
	for _, tx := range transactions {
		// 워커풀로 작업 전달
		job := NewTransactionJob(tx, a, 0)
		select {
		case a.infra.TxJobChannel <- job:
			// 성공
		default:
			// 채널이 가득 찬 경우 드롭
			atomic.AddInt64(&a.stats.DroppedTxs, 1)
		}
	}

	// 배치 처리 완료 로깅 (처음 몇 배치만)
	if processedCount < 500 {
		log.Printf("📦 Batch processed: %d messages → %d transactions", batchSize, len(transactions))
	}
}

// processSingleTransaction 메서드는 TransactionJob.Do()로 이동됨
// 하위 호환성을 위해 삭제

// analyzeTransactionResult 트랜잭션 결과 분석
func (a *SimpleEOAAnalyzer) analyzeTransactionResult(tx *shareddomain.MarkedTransaction) {
	depositDetected := false

	// 처음 5개 트랜잭션의 CEX 체크 과정을 자세히 로깅
	processedCount := atomic.LoadInt64(&a.stats.SuccessCount)

	// 입금 주소 탐지
	isCEX := a.infra.GroundKnowledge.IsCEXAddress(tx.To)
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
	if a.infra.GroundKnowledge.IsDepositAddress(tx.To) {
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
	channelUsage := len(a.infra.TxJobChannel)
	channelCapacity := cap(a.infra.TxJobChannel)
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
	if graphStats, err := a.infra.GraphRepo.GetGraphStats(); err == nil {
		log.Printf("   Graph: %v nodes | %v edges",
			graphStats["total_nodes"], graphStats["total_edges"])
	}
}

// GetStatistics 통계 반환
func (a *SimpleEOAAnalyzer) GetStatistics() map[string]any {
	return map[string]any{
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
		"channel_usage":      len(a.infra.TxJobChannel),
		"channel_capacity":   cap(a.infra.TxJobChannel),
	}
}

// IsHealthy 헬스 상태 체크
func (a *SimpleEOAAnalyzer) IsHealthy() bool {
	total := atomic.LoadInt64(&a.stats.TotalProcessed)
	errors := atomic.LoadInt64(&a.stats.ErrorCount)

	if total == 0 {
		return true // 아직 트랜잭션이 없으면 건강함
	}

	channelUsage := float64(len(a.infra.TxJobChannel)) / float64(cap(a.infra.TxJobChannel))
	errorRate := float64(errors) / float64(total)

	// 채널 사용률 90% 이하, 에러율 10% 이하
	return channelUsage < 0.9 && errorRate < 0.1
}

// GetChannelStatus 채널 상태 반환
func (a *SimpleEOAAnalyzer) GetChannelStatus() (int, int) {
	return len(a.infra.TxJobChannel), cap(a.infra.TxJobChannel)
}

// shutdown 우아한 종료
func (a *SimpleEOAAnalyzer) shutdown() error {
	log.Printf("🔄 Shutting down: %s", a.config.Name)

	// 워커풀 종료
	if a.infra.WorkerPool != nil {
		a.infra.WorkerPool.Shutdown()
		log.Printf("🔧 WorkerPool shutdown completed")
	}

	// 새 트랜잭션 수신 중지 (한 번만)
	a.shutdownOnce.Do(func() {
		close(a.infra.TxJobChannel)
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

	if err := a.infra.GraphRepo.Close(); err != nil {
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
	if graphStats, err := a.infra.GraphRepo.GetGraphStats(); err == nil {
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

	log.Printf("🧹 Cleaning up test data: %s", a.config.FileDBPath)

	if err := os.RemoveAll(a.config.FileDBPath); err != nil {
		log.Printf("⚠️ Failed to cleanup test data: %v", err)
	} else {
		log.Printf("✅ Test data cleaned up")
	}
}

// Close io.Closer 인터페이스 구현
func (a *SimpleEOAAnalyzer) Close() error {

	// Batch Consumer 정리
	if a.infra.BatchConsumer != nil {
		if err := a.infra.BatchConsumer.Close(); err != nil {
			log.Printf("⚠️ Error closing batch consumer: %v", err)
		}
	}

	return a.Stop()
}
