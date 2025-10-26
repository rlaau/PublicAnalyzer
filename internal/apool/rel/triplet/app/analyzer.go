package app

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"golang.org/x/sync/errgroup"
)

type TripletAnalyzerDesign interface {
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

type TripletAnalyzer struct {
	// Core domain components
	dualManager *DualManager
	mode        mode.ProcessingMode
	name        gface.ModuleName
	//TODO stats와 관련한 주석은 프로메테우스로 처리
	// stats SimpleAnalyzerStats

	infra   *infra.TripletAndDualManagerInfra
	relPool pface.RelPort
	closed  atomic.Bool
}

// SimpleAnalyzerStats 간단한 분석기 통계

// NewTriplet 설정에 따라 적절한 분석기 생성
func NewTriplet(config TripletConfig) (TripletAnalyzerDesign, error) {
	if config.Mode.IsTest() {
		return NewTestingEOAAnalyzer(config)
	} else {
		return NewProductionEOAAnalyzer(config)
	}
}

// NewProductionEOAAnalyzer 프로덕션용 분석기 생성
func NewProductionEOAAnalyzer(config TripletConfig) (TripletAnalyzerDesign, error) {
	infraStructure := NewInfraByConfig(config)
	return newTripletAnalyzer(config, infraStructure)
}

// NewTestingEOAAnalyzer 테스트용 분석기 생성
func NewTestingEOAAnalyzer(config TripletConfig) (TripletAnalyzerDesign, error) {
	infraStructure := NewInfraByConfig(config)
	return newTripletAnalyzer(config, infraStructure)
}

// newTripletAnalyzer 공통 분석기 생성 로직
func newTripletAnalyzer(config TripletConfig, infraStructure *infra.TripletAndDualManagerInfra) (*TripletAnalyzer, error) {
	//전체 EOA인프라에서 꺼내 쓰는 형식

	analyzer := &TripletAnalyzer{
		name:  gface.ModuleName(config.Name),
		mode:  config.Mode,
		infra: infraStructure,
	}

	log.Printf("✅ Simple EOA Analyzer created: %s", config.Name)

	return analyzer, nil
}

func (a *TripletAnalyzer) Name() gface.ModuleName {
	return "tripletAnalyzer"
}
func (a *TripletAnalyzer) Start(parent context.Context) error {
	log.Printf("Starting Triplet: %s", a.name)
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
	g.Go(func() error { return a.ComsumeTransaction(ctx) })
	g.Go(func() error { a.statsReporter(ctx); return nil })
	g.Go(func() error { a.infra.WorkerPool.Start(ctx); return nil })
	err = g.Wait()
	a.Close()
	return err
}

// Close io.Closer 인터페이스 구현
func (a *TripletAnalyzer) Close() error {
	if a.closed.Swap(true) {
		return nil
	}

	if a.infra.WorkerPool != nil {
		a.infra.WorkerPool.Close()
	}
	if a.dualManager != nil {
		a.dualManager.Close()
	}
	a.printFinalReport()
	a.printStatistics()
	return nil
}
func (a *TripletAnalyzer) Closed() bool {
	return a.closed.Load()
}

func (a *TripletAnalyzer) RegisterParent(relPool gface.Module) error {
	rp, ok := relPool.(pface.RelPort)
	if !ok {
		fmt.Printf("런타임 타입 에러남. relPool이 RelPort조건을 만족하지 않음.\n")
		return fmt.Errorf("SimpleTriplet이 Register한 Parent가 Relpool이 아님\n")
	}
	a.relPool = rp
	fmt.Printf("트리플렛이 relPool을 레지스터링 함\n")
	return nil
}

// ProcessTransaction 트랜잭션 처리 (non-blocking)
// TODO TxJob의 Do()가 서로 경합 발생. 패닉은 아니지만, 암묵적 성능 저하 발생중
// TODO 문제에 대한 진단 및 추후 개선 방안은 /debug의 upgrade_solution.md에 자세히 적어놨음.
func (a *TripletAnalyzer) ProcessTransaction(tx shareddomain.MarkedTransaction) error {
	job := NewTransactionJob(tx, a, 0) // workerID는 워커풀에서 자동 관리
	// 퍼블리시하는 쪽(Analyzer) 직전
	if err := a.infra.WorkerPool.DispatchJob(job); err != nil { // <- 블로킹
		// 종료(stopping/closed) 중엔 에러가 날 수 있으니 드롭 카운트만 올림
		//atomic.AddInt64(&a.stats.DroppedTxs, 1)

		return fmt.Errorf("ProcessTransaction: enqueue failed: %v", err)
	}
	return nil
}

func (a *TripletAnalyzer) ComsumeTransaction(ctx context.Context) error {
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

func (a *TripletAnalyzer) GetMyTx() <-chan domain.MarkedTransaction {
	return a.relPool.ConsumeTripletTxByFanout()
}

// statsReporter 주기적 통계 출력
func (a *TripletAnalyzer) statsReporter(ctx context.Context) {

	ticker := time.NewTicker(time.Duration(time.Second * 20))
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
func (a *TripletAnalyzer) printStatistics() {

	channelUsage := a.infra.WorkerPool.JobsLen()
	channelCapacity := a.infra.WorkerPool.JobsCap()
	usagePercent := float64(channelUsage) / float64(channelCapacity) * 100

	log.Printf("📊 [%s] %s Statistics:", a.mode, a.name)

	log.Printf("   Triplet-TxJobChannel: %d/%d (%.1f%%)", channelUsage, channelCapacity, usagePercent)
	log.Printf("   Triplet-MyTxBus: %d/%d", len(a.GetMyTx()), cap(a.GetMyTx()))

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
func (a *TripletAnalyzer) GetStatistics() map[string]any {
	return map[string]any{
		"mode":             a.mode.String(),
		"name":             a.name,
		"channel_usage":    a.infra.WorkerPool.JobsLen(),
		"channel_capacity": a.infra.WorkerPool.JobsCap(),
	}
}

func (a *TripletAnalyzer) GetRopeDBStats() map[string]any {
	return a.relPool.GetGraphStats()
}

// IsHealthy 헬스 상태 체크
func (a *TripletAnalyzer) IsHealthy() bool {
	return true //errorRate < 0.1
}

// GetChannelStatus 채널 상태 반환
func (a *TripletAnalyzer) GetChannelStatus() (int, int) {
	return a.infra.WorkerPool.JobsLen(), a.infra.WorkerPool.JobsCap()
}

// printFinalReport 최종 리포트 출력 (테스트 모드용)
func (a *TripletAnalyzer) printFinalReport() {
	log.Printf("\n" + strings.Repeat("=", 80))
	log.Printf("🎯 FINAL REPORT: %s", a.name)
	log.Printf(strings.Repeat("=", 80))

	log.Printf("📊 Performance Summary:")

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
