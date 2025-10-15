package app

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/pface"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
)

// CommonTriplet 인터페이스 - 테스트용과 프로덕션용 공통 인터페이스
// 그냥 최소 구조 정도를 정의한 인터페이스.
// * 혹시라도 많이 임포트 할 시 꺼내 쓰라고 만든 정도임. 굳이 이 인터페이스 쓸 필요는 없음
// ! 인터페이스는 쓰는 측에서 정의하는 것
type CommonTriplet interface {
	// 분석기 생명주기 관리
	Start(ctx context.Context) error
	Stop() error

	// 트랜잭션 처리
	//ProcessTransaction(tx *shareddomain.MarkedTransaction) error
	//ProcessTransactions(txs []*shareddomain.MarkedTransaction) error

	// 상태 조회
	GetStatistics() map[string]any
	IsHealthy() bool
	GetChannelStatus() (usage int, capacity int)
	//그래, 맞다. 인터페이스는 이따구로 쓰면 안되지/
	//근데 이거 고치려면 또 리팩토링 해야함. 또!!!
	//그건 나중에 하자고.
	GetRopeDBStats() map[string]any
	GetMyTxBus() *eventbus.EventBus[*shareddomain.MarkedTransaction]
	//TODO 이딴건 당연히 금지임. 추후 로프DB관련 인터페이스 싹 제거하기
	// 리소스 관리
	io.Closer
}

// SimpleTriplet 간단한 EOA 분석기 구현체
// * 테스트용과 프로덕션용 모두 지원하는 기본 구현
type SimpleTriplet struct {
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
	config *TripletConfig

	// Statistics (thread-safe atomic counters)
	stats SimpleAnalyzerStats

	infra   *infra.TripletAndDualManagerInfra
	relPool pface.RelPort
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
func NewProductionEOAAnalyzer(config *TripletConfig, relPool pface.RelPort) (CommonTriplet, error) {
	infraStructure := NewInfraByConfig(config)
	return newSimpleAnalyzer(config, infraStructure, relPool)
}

// NewTestingEOAAnalyzer 테스트용 분석기 생성
func NewTestingEOAAnalyzer(config *TripletConfig, relPool pface.RelPort) (CommonTriplet, error) {
	infraStructure := NewInfraByConfig(config)
	return newSimpleAnalyzer(config, infraStructure, relPool)
}

// newSimpleAnalyzer 공통 분석기 생성 로직
func newSimpleAnalyzer(config *TripletConfig, infraStructure *infra.TripletAndDualManagerInfra, relPool pface.RelPort) (*SimpleTriplet, error) {
	//전체 EOA인프라에서 꺼내 쓰는 형식
	dualManagerInfra := infra.NewDualManagerInfra(infraStructure.HeavyTimeBucketManager)
	dualManager, _ := NewDualManager(dualManagerInfra, relPool)
	log.Printf("듀얼 매니져 초기화. 현재 cex주소 개수: %d", len(dualManager.relPool.GetApooPort().GetNodPort().GetEoPort().CEXAddresses()))
	analyzer := &SimpleTriplet{
		infra:       infraStructure,
		dualManager: dualManager,
		stopChannel: make(chan struct{}),
		batchMode:   true, // 기본값: 배치 모드 활성화
		config:      config,
		stats: SimpleAnalyzerStats{
			StartTime: time.Now(),
		},
		relPool: relPool,
	}

	log.Printf("✅ Simple EOA Analyzer created: %s", config.Name)

	return analyzer, nil
}

// func (a *SimpleTriplet) GraphDB() *badger.DB {
// 	if p, ok := a.relPool.ClusterDB().(roperepo.RawBadgerProvider); ok {
// 		return p.RawBadgerDB()
// 	}
// 	return nil
// }

// func (a *SimpleTriplet) RopeDB() ropeapp.RopeDB {
// 	return a.relPool.ClusterDB()
// }

// Start 분석기 시작
// TODO 추후엔 당연히!!! 배치 컨슈머에서 자기만 직접 받는게 아니라 apool에서부터 정식으로 받을 것
func (a *SimpleTriplet) Start(ctx context.Context) error {
	log.Printf("🚀 Starting Triplet: %s", a.config.Name)
	a.infra.WorkerPool.Restart(ctx)
	// 워커풀 만든 직후
	fmt.Printf("bus@pool   = %p\n", a.infra.WorkerPool.JobChan)

	// Consumer 시작 (배치 모드 or 단건 모드)
	if a.batchMode && a.infra.TxBus != nil {
		// 배치 모드: 배치 Consumer 시작
		a.wg.Add(1)
		go a.ComsumeTransaction(ctx)
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
func (a *SimpleTriplet) Stop() error {
	a.stopOnce.Do(func() {
		close(a.stopChannel)
	})
	return nil
}

// ProcessTransaction 트랜잭션 처리 (non-blocking)
// TODO 현재 이 부분에서 스레드 간 성능 저하 발생함. 스레드 1개나 16개나 동일 성능 보임
// TODO TxJob의 Do()가 서로 경합 발생. 패닉은 아니지만, 암묵적 성능 저하 발생중
// TODO 문제에 대한 진단 및 추후 개선 방안은 /debug의 upgrade_solution.md에 자세히 적어놨음.
func (a *SimpleTriplet) ProcessTransaction(tx *shareddomain.MarkedTransaction) error {
	job := NewTransactionJob(tx, a, 0) // workerID는 워커풀에서 자동 관리
	// 퍼블리시하는 쪽(Analyzer) 직전
	if err := a.infra.TxJobBus.Publish(job); err != nil { // <- 블로킹
		// 종료(stopping/closed) 중엔 에러가 날 수 있으니 드롭 카운트만 올림
		atomic.AddInt64(&a.stats.DroppedTxs, 1)

		return fmt.Errorf("ProcessTransaction: enqueue failed: %v", err)
	}
	return nil
}

func (a *SimpleTriplet) ComsumeTransaction(ctx context.Context) {
	defer a.wg.Done()
	in := a.infra.TxBus.Dequeue()
	log.Printf("🚀 이벤트버스 시작")

	// Shutdown timeout for graceful exit
	shutdownTimer := time.NewTimer(time.Hour) // Initial large value, will be reset on shutdown
	defer shutdownTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 Batch consumer worker stopping (context) - starting graceful shutdown")
			// Start shutdown timer
			shutdownTimer.Reset(2 * time.Second)
			// Process remaining messages quickly during shutdown
			for {
				select {
				case msg, ok := <-in:
					if !ok {
						log.Printf("🛑 TxBus closed during shutdown")
						return
					}
					if msg != nil {
						a.ProcessTransaction(msg) // Ignore errors during shutdown
					}
				case <-shutdownTimer.C:
					log.Printf("🛑 Consumer shutdown timeout - forcing exit")
					return
				default:
					return // No more messages, exit immediately
				}
			}
		case <-a.stopChannel:
			log.Printf("🛑 Batch consumer worker stopping (signal)")
			return
		case msg, ok := <-in: // ← 채널 수신을 select로
			if !ok {
				log.Printf("🛑 TxBus closed; consumer exit")
				return
			}
			if msg == nil {
				continue
			}
			if err := a.ProcessTransaction(msg); err != nil {
				// 워커풀 포화시 약간의 backoff
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (a *SimpleTriplet) GetMyTxBus() *eventbus.EventBus[*shareddomain.MarkedTransaction] {
	return a.infra.TxBus
}

// processTransactionParrell 배치 메시지 처리 (고효율)
func (a *SimpleTriplet) processTransactionParrell(messages []kafka.Message[*shareddomain.MarkedTransaction]) {
	batchSize := len(messages)
	processedCount := atomic.LoadInt64(&a.stats.TotalProcessed)

	// 배치 처리 시작 로깅 (처음 몇 배치만)
	if processedCount < 5 {
		log.Printf("📦 Processing batch of %d messages (total processed: %d)", batchSize, processedCount)
	}

	transactions := make([]*shareddomain.MarkedTransaction, 0, batchSize)

	// 1. 메시지에서 직접 트랜잭션 추출 (파싱 불필요!)
	for _, msg := range messages {
		if msg.Value != nil {
			transactions = append(transactions, msg.Value)
		} else {
			fmt.Printf("processTransactionParrell 에러:메시지를 받지 않음")
			atomic.AddInt64(&a.stats.ErrorCount, 1)
		}
	}

	// 2. 트랜잭션 처리 (배치로 처리)

	for _, tx := range transactions {
		// 워커풀로 작업 전달
		job := NewTransactionJob(tx, a, 0)

		err := a.infra.TxJobBus.Publish(job)
		if err != nil {
			atomic.AddInt64(&a.stats.DroppedTxs, 1)
			fmt.Printf("현재 EOA Analyzer의 워커풀 채널 다 참. 0.1초간 입력을 블로킹함.")
			time.Sleep(10 * time.Millisecond) // 너무 무거운 대기는 피하기
			continue                          // 개별 실패는 무시하고 계속 처리
		}
	}

	// 배치 처리 완료 로깅 (처음 몇 배치만)
	if processedCount < 5 {
		log.Printf("📦 Batch processed: %d messages → %d transactions", batchSize, len(transactions))
	}
}

// processSingleTransaction 메서드는 TransactionJob.Do()로 이동됨
// 하위 호환성을 위해 삭제

// analyzeTransactionResult 트랜잭션 결과 분석
func (a *SimpleTriplet) analyzeTransactionResult(tx *shareddomain.MarkedTransaction) {
	isDebug := false
	depositDetected := false

	// 처음 5개 트랜잭션의 CEX 체크 과정을 자세히 로깅
	processedCount := atomic.LoadInt64(&a.stats.SuccessCount)

	// 입금 주소 탐지
	isCEX := a.dualManager.IsCEXAddress(tx.To)
	if processedCount <= 5 {
		log.Printf("🔍 CEX Check #%d: To=%s → IsCEX=%t",
			processedCount, tx.To.String(), isCEX)
	}

	if isCEX && isDebug {
		depositCount := atomic.AddInt64(&a.stats.DepositDetections, 1)
		depositDetected = true
		log.Printf("🎯 DEPOSIT DETECTED #%d: From: %s → CEX: %s", depositCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
	}

	// 그래프/윈도우 업데이트 분류
	if a.dualManager.IsDepositAddress(tx.To) && isDebug {
		graphCount := atomic.AddInt64(&a.stats.GraphUpdates, 1)
		log.Printf("📊 GRAPH UPDATE #%d: From: %s → Deposit: %s",
			graphCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
	} else {

		windowCount := atomic.AddInt64(&a.stats.WindowUpdates, 1)
		if depositDetected && isDebug {
			log.Printf("📈 WINDOW UPDATE #%d (with deposit): From: %s → To: %s",
				windowCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
		}
	}
}

// statsReporter 주기적 통계 출력
func (a *SimpleTriplet) statsReporter(ctx context.Context) {
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
func (a *SimpleTriplet) printStatistics() {
	total := atomic.LoadInt64(&a.stats.TotalProcessed)
	success := atomic.LoadInt64(&a.stats.SuccessCount)
	errors := atomic.LoadInt64(&a.stats.ErrorCount)
	deposits := atomic.LoadInt64(&a.stats.DepositDetections)
	graphUpdates := atomic.LoadInt64(&a.stats.GraphUpdates)
	windowUpdates := atomic.LoadInt64(&a.stats.WindowUpdates)
	dropped := atomic.LoadInt64(&a.stats.DroppedTxs)

	uptime := time.Since(a.stats.StartTime)
	channelUsage := a.infra.TxJobBus.Len()
	channelCapacity := a.infra.TxJobBus.Cap()
	usagePercent := float64(channelUsage) / float64(channelCapacity) * 100

	log.Printf("📊 [%s] %s Statistics:", a.config.Mode, a.config.Name)
	log.Printf("   Uptime: %v | Processed: %d | Success: %d | Errors: %d",
		uptime.Round(time.Second), total, success, errors)
	log.Printf("   Deposits: %d | Graph: %d | Window: %d | Dropped: %d",
		deposits, graphUpdates, windowUpdates, dropped)
	log.Printf("   Triplet-TxJobChannel: %d/%d (%.1f%%)", channelUsage, channelCapacity, usagePercent)
	log.Printf("   Triplet-MyTxBus: %d/%d", a.GetMyTxBus().Len(), a.GetMyTxBus().Len())
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

	graphStats := a.relPool.GetGraphStats()
	log.Printf("   Graph: %v nodes | %v edges",
		graphStats["total_nodes"], graphStats["total_edges"])

}

// GetStatistics 통계 반환
func (a *SimpleTriplet) GetStatistics() map[string]any {
	return map[string]any{
		"mode":               a.config.Mode.String(),
		"name":               a.config.Name,
		"total_processed":    atomic.LoadInt64(&a.stats.TotalProcessed),
		"success_count":      atomic.LoadInt64(&a.stats.SuccessCount),
		"error_count":        atomic.LoadInt64(&a.stats.ErrorCount),
		"deposit_detections": atomic.LoadInt64(&a.stats.DepositDetections),
		"graph_updates":      atomic.LoadInt64(&a.stats.GraphUpdates),
		"window_updates":     atomic.LoadInt64(&a.stats.WindowUpdates),
		"dropped_txs":        atomic.LoadInt64(&a.stats.DroppedTxs),
		"uptime_seconds":     time.Since(a.stats.StartTime).Seconds(),
		"channel_usage":      a.infra.TxJobBus.Len(),
		"channel_capacity":   a.infra.TxJobBus.Cap(),
	}
}

func (a *SimpleTriplet) GetRopeDBStats() map[string]any {
	return a.relPool.GetGraphStats()
}

// IsHealthy 헬스 상태 체크
func (a *SimpleTriplet) IsHealthy() bool {
	total := atomic.LoadInt64(&a.stats.TotalProcessed)
	errors := atomic.LoadInt64(&a.stats.ErrorCount)

	if total == 0 {
		return true // 아직 트랜잭션이 없으면 건강함
	}

	errorRate := float64(errors) / float64(total)

	// 에러율 10% 이하
	return errorRate < 0.1
}

// GetChannelStatus 채널 상태 반환
func (a *SimpleTriplet) GetChannelStatus() (int, int) {
	return a.infra.TxJobBus.Len(), a.infra.TxJobBus.Cap()
}

// shutdown 우아한 종료
func (a *SimpleTriplet) shutdown() error {
	log.Printf("🔄 Shutting down: %s", a.config.Name)
	// 1) 내부 stop 신호
	_ = a.Stop()
	// 2) 입력 채널 닫아 소비 고루틴 확실히 깨우기
	if a.infra.TxBus != nil {
		a.infra.TxBus.Close() // idempotent 가정
	}

	// 워커풀 종료
	if a.infra.WorkerPool != nil {
		a.infra.WorkerPool.Shutdown()
		log.Printf("🔧 WorkerPool shutdown completed")
	}

	// 모든 워커 완료 대기
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("✅ All workers stopped gracefully")
	case <-time.After(3 * time.Second):
		log.Printf("⚠️ Shutdown timeout - forcing worker termination")
		// Force shutdown by not waiting for remaining workers
	}
	// 활동이 종료된 채널을 닫기
	a.shutdownOnce.Do(func() {
		a.infra.TxJobBus.Close()
	})

	// 최종 통계 출력
	if a.config.ResultReporting {
		a.printFinalReport()
	}
	a.printStatistics()

	// 리소스 정리
	if err := a.dualManager.Close(); err != nil {
		log.Printf("⚠️ Error closing dual manager: %v", err)
	}

	// 테스트 모드에서 데이터 정리
	if a.config.AutoCleanup {
		a.cleanup()
	}

	log.Printf("✅ Triplet Shutdown completed: %s", a.config.Name)
	return nil
}

// printFinalReport 최종 리포트 출력 (테스트 모드용)
func (a *SimpleTriplet) printFinalReport() {
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

	graphStats := a.relPool.GetGraphStats()
	log.Printf("\n🗂️  Graph Database State:")
	for key, value := range graphStats {
		log.Printf("   %s: %v", key, value)
	}

	log.Printf(strings.Repeat("=", 80) + "\n")
}

// cleanup 테스트 데이터 정리
func (a *SimpleTriplet) cleanup() {
	if !a.config.Mode.IsTest() {
		return
	}

	log.Printf("🧹 Cleaning up test data: %s", a.config.IsolatedDBPath)

	if err := os.RemoveAll(a.config.IsolatedDBPath); err != nil {
		log.Printf("⚠️ Failed to cleanup test data: %v", err)
	} else {
		log.Printf("✅ Test data cleaned up")
	}
}

// GetDualManager DualManager 인스턴스 반환 (API 서버용)
func (a *SimpleTriplet) GetDualManager() *DualManager {
	return a.dualManager
}

// Close io.Closer 인터페이스 구현
func (a *SimpleTriplet) Close() error {

	// Batch Consumer 정리
	if a.infra.TxBus != nil {
		a.infra.TxBus.Close()
	}

	return a.Stop()
}

// TODO 추후 삭제할 것. 어쩌다 프로세스 도중에 DB바꿀 일이 있고, 하필 그게 테스트코드라 일다 놔뒀음
// TODO 추후 ropeDB테스트 리팩토링 후 제거할 것
// !!프로덕션 환경에선 절대절대 쓰지 말겄!!!
func (a *SimpleTriplet) NullButAddDB(relPool pface.RelPort) {
	a.relPool = relPool
}
