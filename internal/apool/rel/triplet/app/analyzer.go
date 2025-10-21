package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"golang.org/x/sync/errgroup"
)

type CommonTriplet interface {
	// 분석기 생명주기 관리
	gface.Module
	gface.Child

	// 상태 조회
	GetStatistics() map[string]any
	IsHealthy() bool
	GetChannelStatus() (usage int, capacity int)
	//그래, 맞다. 인터페이스는 이따구로 쓰면 안되지/
	//근데 이거 고치려면 또 리팩토링 해야함. 또!!!
	//그건 나중에 하자고.
	GetRopeDBStats() map[string]any
	GetMyTx() <-chan domain.MarkedTransaction
}

// SimpleTriplet 간단한 EOA 분석기 구현체
// * 테스트용과 프로덕션용 모두 지원하는 기본 구현
type SimpleTriplet struct {
	// Core domain components
	dualManager *DualManager

	// Transaction consumer (Kafka 기반)
	batchMode bool // 배치 모드 활성화 여부

	// Configuration
	config *TripletConfig

	// Statistics (thread-safe atomic counters)
	stats SimpleAnalyzerStats

	infra   *infra.TripletAndDualManagerInfra
	relPool pface.RelPort
	closed  atomic.Bool
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

// NewTriplet 설정에 따라 적절한 분석기 생성
func NewTriplet(config *TripletConfig) (CommonTriplet, error) {
	if config.Mode.IsTest() {
		return NewTestingEOAAnalyzer(config)
	} else {
		return NewProductionEOAAnalyzer(config)
	}
}

// NewProductionEOAAnalyzer 프로덕션용 분석기 생성
func NewProductionEOAAnalyzer(config *TripletConfig) (CommonTriplet, error) {
	infraStructure := NewInfraByConfig(config)
	return newSimpleAnalyzer(config, infraStructure)
}

// NewTestingEOAAnalyzer 테스트용 분석기 생성
func NewTestingEOAAnalyzer(config *TripletConfig) (CommonTriplet, error) {
	infraStructure := NewInfraByConfig(config)
	return newSimpleAnalyzer(config, infraStructure)
}

// newSimpleAnalyzer 공통 분석기 생성 로직
func newSimpleAnalyzer(config *TripletConfig, infraStructure *infra.TripletAndDualManagerInfra) (*SimpleTriplet, error) {
	//전체 EOA인프라에서 꺼내 쓰는 형식

	analyzer := &SimpleTriplet{
		infra:     infraStructure,
		batchMode: true, // 기본값: 배치 모드 활성화
		config:    config,
		stats: SimpleAnalyzerStats{
			StartTime: time.Now(),
		},
	}

	log.Printf("✅ Simple EOA Analyzer created: %s", config.Name)

	return analyzer, nil
}

func (a *SimpleTriplet) Name() gface.ModuleName {
	return "tripletAnalyzer"
}
func (a *SimpleTriplet) Start(parent context.Context) error {
	log.Printf("Starting Triplet: %s", a.config.Name)
	if a.infra.WorkerPool == nil {
		return fmt.Errorf("트리플렛의 워커풀 초기화되지 못함")
	}
	if a.relPool == nil {
		return fmt.Errorf("트리플렛의 relpool존재하지 않음")
	}
	dmi := infra.NewDualManagerInfra(a.infra.HeavyTimeBucketManager)
	dm, err := NewDualManager(dmi, a.relPool)
	if err != nil {
		return err
	}
	a.dualManager = dm
	fmt.Printf("듀얼 매니져 초기화 완료\n")
	log.Printf("듀얼 매니져 초기화. 현재 cex주소 개수: %d", len(a.dualManager.relPool.GetApooPort().GetNodPort().GetEoPort().CEXAddresses()))

	g, ctx := errgroup.WithContext(parent)
	g.Go(func() error { a.infra.WorkerPool.Restart(ctx); return nil })
	g.Go(func() error { return a.ComsumeTransaction(ctx) })
	g.Go(func() error { a.statsReporter(ctx); return nil })
	err = g.Wait()
	a.Close()
	return err
}

// Close io.Closer 인터페이스 구현
func (a *SimpleTriplet) Close() error {
	if a.closed.Swap(true) {
		return nil
	}

	if a.infra.WorkerPool != nil {
		a.infra.WorkerPool.Shutdown()
	}
	if a.dualManager != nil {
		a.dualManager.Close()
	}
	a.printFinalReport()
	a.printStatistics()
	a.cleanup()
	return nil
}
func (a *SimpleTriplet) Closed() bool {
	return a.closed.Load()
}

func (a *SimpleTriplet) RegisterParent(relPool gface.Module) error {
	rp, ok := relPool.(pface.RelPort)
	if !ok {
		return fmt.Errorf("SimpleTriplet이 Register한 Parent가 Relpool이 아님")
	}
	a.relPool = rp
	return nil
}

// ProcessTransaction 트랜잭션 처리 (non-blocking)
// TODO TxJob의 Do()가 서로 경합 발생. 패닉은 아니지만, 암묵적 성능 저하 발생중
// TODO 문제에 대한 진단 및 추후 개선 방안은 /debug의 upgrade_solution.md에 자세히 적어놨음.
func (a *SimpleTriplet) ProcessTransaction(tx shareddomain.MarkedTransaction) error {
	job := NewTransactionJob(tx, a, 0) // workerID는 워커풀에서 자동 관리
	// 퍼블리시하는 쪽(Analyzer) 직전
	if err := a.infra.TxJobBus.Publish(job); err != nil { // <- 블로킹
		// 종료(stopping/closed) 중엔 에러가 날 수 있으니 드롭 카운트만 올림
		atomic.AddInt64(&a.stats.DroppedTxs, 1)

		return fmt.Errorf("ProcessTransaction: enqueue failed: %v", err)
	}
	return nil
}

func (a *SimpleTriplet) ComsumeTransaction(ctx context.Context) error {
	in := a.GetMyTx()
	log.Printf("🚀 이벤트버스 시작")

	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 Batch consumer worker stopping (context) - starting graceful shutdown")
			for {
				select {
				case msg, ok := <-in:
					if !ok {
						log.Printf("🛑 TxBus closed during shutdown")
						return nil
					}
					a.ProcessTransaction(msg) // Ignore errors during shutdown
				default:
					return nil
				}
			}

		case msg, ok := <-in: // ← 채널 수신을 select로
			if !ok {
				log.Printf("🛑 TxBus closed; consumer exit")
				return nil
			}
			if err := a.ProcessTransaction(msg); err != nil {
				return err
			}
		}
	}
}

func (a *SimpleTriplet) GetMyTx() <-chan domain.MarkedTransaction {
	return a.relPool.ConsumeTripletTxByFanout()
}

// processSingleTransaction 메서드는 TransactionJob.Do()로 이동됨
// 하위 호환성을 위해 삭제

// analyzeTransactionResult 트랜잭션 결과 분석
func (a *SimpleTriplet) analyzeTransactionResult(tx shareddomain.MarkedTransaction) {
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

	ticker := time.NewTicker(time.Duration(a.config.StatsInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
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
	log.Printf("   Triplet-MyTxBus: %d/%d", len(a.GetMyTx()), cap(a.GetMyTx()))
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
