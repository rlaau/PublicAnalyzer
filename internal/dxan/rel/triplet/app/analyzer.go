package app

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"golang.org/x/sync/errgroup"
)

// TripletAnalyzerDesign은 도메인 로직(TripletModel)+부가 로직(제어 로직, 쿼리 로직)을 추가한 디자인
type TripletAnalyzerDesign interface {
	//제어 및 노출용 인터페이스
	cface.TripletPort
	//앱의 도메인 모델
	TripletModel
}

// ! 디자인 강제
var _ TripletAnalyzerDesign = (*TripletAnalyzer)(nil)

// 실제 구현체
type TripletAnalyzer struct {
	mode mode.ProcessingMode
	name gface.ModuleName

	infraPort iface.TripletInfraPort
	relPool   pface.RelPort
	closed    atomic.Bool
}

type TripletConfig struct {
	// 기본 설정
	Name string              // 분석기 이름
	Mode mode.ProcessingMode // 동작 모드 (production/testing)

}

// NewTripletAnalyzer 공통 분석기 생성 로직
func NewTripletAnalyzer(config TripletConfig, infraStructure iface.TripletInfraPort) (*TripletAnalyzer, error) {
	//전체 EOA인프라에서 꺼내 쓰는 형식

	analyzer := &TripletAnalyzer{
		name:      gface.ModuleName(config.Name),
		mode:      config.Mode,
		infraPort: infraStructure,
	}

	log.Printf("✅ Simple EOA Analyzer created: %s", config.Name)

	return analyzer, nil
}

func (a *TripletAnalyzer) Name() gface.ModuleName {
	return "tripletAnalyzer"
}
func (a *TripletAnalyzer) Start(parent context.Context) error {
	log.Printf("Starting Triplet: %s", a.name)
	if a.infraPort.GetWorkerPool() == nil {
		return fmt.Errorf("트리플렛의 워커풀 초기화되지 못함")
	}
	if a.relPool == nil {
		return fmt.Errorf("트리플렛의 relpool존재하지 않음")
	}

	g, ctx := errgroup.WithContext(parent)
	g.Go(func() error { return a.processTransaction(ctx) })
	g.Go(func() error { a.statsReporter(ctx); fmt.Printf("트리플렛 리포터 중단\n"); return nil })
	g.Go(func() error {
		a.infraPort.GetWorkerPool().Start(ctx)
		fmt.Printf("트리플렛 워커풀 중단")
		return nil
	})
	err := g.Wait()
	fmt.Println("트리플렛 Close 시작")
	a.Close()
	return err
}

// Close io.Closer 인터페이스 구현
func (a *TripletAnalyzer) Close() error {
	if a.closed.Swap(true) {
		return nil
	}

	if a.infraPort.GetWorkerPool() != nil {
		a.infraPort.GetWorkerPool().Close()
	}

	if a.infraPort.GetTimeBucketManager() != nil {
		a.infraPort.GetTimeBucketManager().Close()
	}

	a.printStatistics()
	fmt.Printf("트리플렛 종료 완료\n")
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

func (a *TripletAnalyzer) makeTransactionJob(tx shareddomain.MarkedTransaction) TransactionJob {
	return NewTransactionJob(tx, a, 0)
}
func (a *TripletAnalyzer) dispatchJob(ctx context.Context, job TransactionJob) error {
	if err := a.infraPort.GetWorkerPool().DispatchJob(ctx, job); err != nil { // <- 블로킹
		return fmt.Errorf("ProcessTransaction: enqueue failed: %v", err)
	}
	return nil
}
func (a *TripletAnalyzer) processTransaction(ctx context.Context) error {
	defer fmt.Printf("트리플렛 콘슈밍 중단함\n")
	in := a.getMyTx()
	log.Printf("🚀 트리플렛 이벤트버스 시작")

	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 트리플렛 콘슈밍 중단함")
			for {
				select {
				case <-in:
					log.Printf("🛑 TxBus closed during shutdown")
					return nil

				default:
					return nil
				}
			}

		case tx, ok := <-in: // ← 채널 수신을 select로
			if !ok {
				log.Printf("🛑 TxBus closed; consumer exit")
				return nil
			}
			job := a.makeTransactionJob(tx)

			if err := a.dispatchJob(ctx, job); err != nil {
				return err
			}
		}
	}
}

func (a *TripletAnalyzer) getMyTx() <-chan domain.MarkedTransaction {
	return a.relPool.ConsumeTripletTxByFanout()
}

// statsReporter 주기적 통계 출력
func (a *TripletAnalyzer) statsReporter(ctx context.Context) {

	ticker := time.NewTicker(time.Duration(time.Second * 20))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("트리플렛 Stat Reporter종료\n")
			return

		case <-ticker.C:
			a.printStatistics()
		}
	}
}

// printStatistics 통계 출력
func (a *TripletAnalyzer) printStatistics() {

	channelUsage := a.infraPort.GetWorkerPool().JobsLen()
	channelCapacity := a.infraPort.GetWorkerPool().JobsCap()
	usagePercent := float64(channelUsage) / float64(channelCapacity) * 100

	log.Printf("📊 [%s] %s Statistics:", a.mode, a.name)

	log.Printf("   Triplet-TxJobChannel: %d/%d (%.1f%%)", channelUsage, channelCapacity, usagePercent)
	log.Printf("   Triplet-MyTxBus: %d/%d", len(a.getMyTx()), cap(a.getMyTx()))

	graphStats := a.relPool.GetGraphStats()
	log.Printf("   Graph: %v nodes | %v edges",
		graphStats["total_nodes"], graphStats["total_edges"])

}

// GetStatistics 통계 반환
func (a *TripletAnalyzer) GetStatistics() map[string]any {
	return map[string]any{
		"mode":             a.mode.String(),
		"name":             a.name,
		"channel_usage":    a.infraPort.GetWorkerPool().JobsLen(),
		"channel_capacity": a.infraPort.GetWorkerPool().JobsCap(),
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
func (a *TripletAnalyzer) GetJobChannelStatus() (int, int) {
	return a.infraPort.GetWorkerPool().JobsLen(), a.infraPort.GetWorkerPool().JobsCap()
}
