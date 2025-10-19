package rel

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/roperepo"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"golang.org/x/sync/errgroup"
)

// 관계 분석 시, 각 단일 분석기가 통신하기 위한 풀
// TODO 대부분의 코드를 RelPool, NodPool에 대해 일반화하기!!
// TODO 구조가 많이 겹침
// TODO 근데 지금은 진짜 하기가 힘이 듦. 조금 쉬고 시간 날때 ㄱ
type RelationPool struct {
	isTest mode.ProcessingMode
	Apool  pface.ApoolPort

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

func CreateRelationPoolFrame(isTest mode.ProcessingMode, apool pface.ApoolPort) (*RelationPool, error) {
	var root func() string
	if isTest.IsTest() {
		root = computation.FindTestingStorageRootPath
	} else {
		root = computation.FindProductionStorageRootPath
	}
	rel := func(name string) string {
		return filepath.Join("relation_pool", "eventbus", fmt.Sprintf("%s.jsonl", name))
	}
	// capLimit: 모드에 따라 조정 (예시 값)
	capLimit := 2048
	if !isTest.IsTest() {
		capLimit = 8192
	}
	busTriplet, err := eventbus.NewWithRoot[cface.TripletEventMsg](root, rel("triplet"), capLimit)
	if err != nil {
		return nil, err
	}
	busCreation, err := eventbus.NewWithRoot[cface.CreationEventMsg](root, rel("creation"), capLimit)
	if err != nil {
		busTriplet.Close()
		return nil, err
	}
	ropeRepo, err := roperepo.NewClusterDB(isTest)
	if err != nil {
		busTriplet.Close()
		busCreation.Close()
		return nil, err
	}

	return &RelationPool{
		isTest:      isTest,
		busTriplet:  busTriplet,
		busCreation: busCreation,
		ClusterRepo: ropeRepo,
		Apool:       apool,
	}, nil
}
func NewRelPool(isTest mode.ProcessingMode, txDistributor iface.RelPoolFanout, triplet cface.TripletPort) (*RelationPool, error) {
	ropeRepo, err := roperepo.NewClusterDB(isTest)
	if err != nil {
		return nil, err
	}
	busTriplet := eventbus.NewInMemory[cface.TripletEventMsg](128)
	r := &RelationPool{
		isTest:      isTest,
		busTriplet:  busTriplet,
		ClusterRepo: ropeRepo,
		ports: struct{ triplet cface.TripletPort }{
			triplet: triplet,
		},
		txDistributor: txDistributor,
	}
	triplet.RegisterParent(r)
	return r, nil
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

// TODO 곧바로 제거
func (r *RelationPool) Register(triplet cface.TripletPort, creation cface.CreationPort) error {

	//	capLimit := 2048
	// if !r.isTest.IsTest() {
	// 	capLimit = 8192
	// }
	var err error
	// if r.Apool.ConsumeRelTxByFanout() != nil {
	// 	//TODO 추후엔 그냥 패닉 내삼!
	// 	r.txDistributor, err = NewTxDistributor(r.isTest, capLimit, r.Apool.ConsumeRelTxByFanout())
	// }
	r.busCreation.Close()
	r.busTriplet.Close()
	return err
	r.ports.triplet = triplet
	//r.ports.creation = creation
	return nil
}

func (r *RelationPool) GetApooPort() pface.ApoolPort {
	return r.Apool
}

// 포트 기반 뷰어
func (r *RelationPool) GetTripletPort() cface.TripletPort { return r.ports.triplet }

func (r *RelationPool) GetCreationPort() cface.CreationPort {
	panic("현재는 craetion port를 구현도 가지지도 않음")
}

// 이벤트 버스 기반 커멘더
func (r *RelationPool) EnqueueToTriplet(v cface.TripletEventMsg) error {
	return r.busTriplet.Publish(v)
}
func (r *RelationPool) EnqueueToCreation(v cface.CreationEventMsg) error {
	return r.busCreation.Publish(v)
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
		upstream := r.Apool.ConsumeRelTxByFanout()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-upstream:
				if !ok {
					return nil
				}
				if err := r.txDistributor.Publish(v); err != nil {
					return fmt.Errorf("publish failed: %w", err)
				}
			}
		}
	})
	err := g.Wait()
	r.Close()
	return err

}
func (r *RelationPool) Close() error {
	// 자기 자신만 close해도 start시 연결된 ctx로 자식도 종료됨
	if r.closed.Swap(true) {
		return nil
	}

	_ = r.txDistributor.Close()
	fmt.Printf("relpool의 txDistributor 정리 완료\n")

	r.busTriplet.Close()
	r.busCreation.Close()
	r.ClusterRepo.Close()
	fmt.Printf("BUS들 정리 완료\n")
	return nil
}

func (r *RelationPool) Closed() bool {
	return r.closed.Load()
}
