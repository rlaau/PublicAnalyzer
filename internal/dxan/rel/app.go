package rel

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/pface"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"golang.org/x/sync/errgroup"
)

// 관계 분석 시, 각 단일 분석기가 통신하기 위한 풀

type RelationPool struct {
	isTest mode.ProcessingMode
	Dxan  pface.DxanPort

	ports struct {
		triplet cface.TripletPort
		//creation cface.CreationPort
	}

	busTriplet  *eventbus.EventBus[cface.TripletEventMsg]
	busCreation *eventbus.EventBus[cface.CreationEventMsg]

	txDistributor iface.RelPoolFanout
	closed        atomic.Bool
	ClusterRepo   iface.ClusterDB
}

func NewRelPool(isTest mode.ProcessingMode, txDistributor iface.RelPoolFanout, clusterDB iface.ClusterDB, triplet cface.TripletPort) (*RelationPool, error) {

	busTriplet := eventbus.NewInMemory[cface.TripletEventMsg](128, "TripletEventBus")
	r := &RelationPool{
		isTest:      isTest,
		busTriplet:  busTriplet,
		ClusterRepo: clusterDB,
		ports: struct{ triplet cface.TripletPort }{
			triplet: triplet,
		},
		txDistributor: txDistributor,
	}
	triplet.RegisterParent(r)
	fmt.Printf("트리플렛 레지스터링 완료\n")
	return r, nil
}
func (r *RelationPool) RegisterParent(apool gface.Module) error {
	ap, ok := apool.(pface.DxanPort)
	if !ok {
		fmt.Printf("RegisterParent: RelPool이 받은 appool이 apoolPort런타임 체크 실패함")
		return fmt.Errorf("RelationPool이 Register한 apool이 ApoolPort조건을 만족하지 못했음")
	}
	r.Dxan = ap
	return nil
}
func (r *RelationPool) DispatchTraitEvent(tv ropedomain.TraitEvent) error {
	return r.ClusterRepo.HandleTraitEvent(tv)
}
func (r *RelationPool) ClusterDB() iface.ClusterDB {
	return r.ClusterRepo
}
func (r *RelationPool) GetGraphStats() map[string]any {
	return r.ClusterRepo.GetGraphStats()
}

func (r *RelationPool) GetDxanPort() pface.DxanPort {
	return r.Dxan
}

// 포트 기반 뷰어
func (r *RelationPool) GetTripletPort() cface.TripletPort { return r.ports.triplet }

func (r *RelationPool) GetCreationPort() cface.CreationPort {
	panic("현재는 craetion port를 구현도 가지지도 않음")
}

// 이벤트 버스 기반 커멘더
func (r *RelationPool) EnqueueToTriplet(ctx context.Context, v cface.TripletEventMsg) error {
	return r.busTriplet.Enqueue(ctx, v)
}
func (r *RelationPool) EnqueueToCreation(ctx context.Context, v cface.CreationEventMsg) error {
	return r.busCreation.Enqueue(ctx, v)
}

// 이벤트 버스 기반 소비
// 분선 모듈 상호 간의 이벤트버스
func (r *RelationPool) DequeueTriplet() <-chan cface.TripletEventMsg {
	return r.busTriplet.Dequeue()
}
func (r *RelationPool) DequeueCreation() <-chan cface.CreationEventMsg {
	return r.busCreation.Dequeue()
}

// Tx Fanout에서의 소비자.
func (r *RelationPool) ConsumeTripletTxByFanout() <-chan domain.MarkedTransaction {
	return r.txDistributor.GetBus(string(iface.TripletBusName)).Dequeue()
}
func (r *RelationPool) ConsumeCreationTxByFanout() <-chan domain.MarkedTransaction {
	return r.txDistributor.GetBus(string(iface.CreationBusName)).Dequeue()
}

func (r *RelationPool) Name() gface.ModuleName {
	return "RelationPool"
}

func (r *RelationPool) Start(parent context.Context) error {

	if r.txDistributor == nil {
		return fmt.Errorf("relation pool의 txDistributor가 nil임.")
	}
	if r.ports.triplet == nil {
		return fmt.Errorf("relation pool의 ports중 일부가 초기화되지 않음")
	}

	g, ctx := errgroup.WithContext(parent)

	g.Go(func() error { return r.ports.triplet.Start(ctx) })
	g.Go(func() error { return r.txDistributor.Start(ctx) })
	g.Go(func() error {
		upstream := r.Dxan.ConsumeRelTxByFanout()
		defer fmt.Printf("RelPool에서 콘슈밍 종료\n")
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-upstream:
				if !ok {
					fmt.Printf("RelPool upstream 채널 닫힘\n")
					return nil
				}
				if err := r.txDistributor.Publish(ctx, v); err != nil {
					return fmt.Errorf("publish failed: %w", err)
				}
			}
		}
	})
	if err := g.Wait(); err != nil {
		fmt.Printf("RelPool이 에러 받음.%s\n", err.Error())
		r.Close()
		return err
	}

	r.Close()
	return nil

}
func (r *RelationPool) Close() error {
	// 자기 자신만 close해도 start시 연결된 ctx로 자식도 종료됨
	if r.closed.Swap(true) {
		return nil
	}

	_ = r.txDistributor.Close()
	fmt.Printf("relpool의 txDistributor 정리 완료\n")

	r.busTriplet.Close()
	//r.busCreation.Close()
	r.ClusterRepo.Close()
	fmt.Printf("relPool BUS들 정리 완료\n")
	return nil
}

func (r *RelationPool) Closed() bool {
	return r.closed.Load()
}
