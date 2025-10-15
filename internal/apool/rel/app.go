package rel

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/roperepo"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// 관계 분석 시, 각 단일 분석기가 통신하기 위한 풀
// TODO 대부분의 코드를 RelPool, NodPool에 대해 일반화하기!!
// TODO 구조가 많이 겹침
// TODO 근데 지금은 진짜 하기가 힘이 듦. 조금 쉬고 시간 날때 ㄱ
type RelationPool struct {
	isTest mode.ProcessingMode
	Apool  pface.ApoolPort

	ports struct {
		triplet  cface.TripletPort
		creation cface.CreationPort
	}

	busTriplet  *eventbus.EventBus[cface.TripletEventMsg]
	busCreation *eventbus.EventBus[cface.CreationEventMsg]

	txDistributor *TxDistributor
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

func (r *RelationPool) DispatchTraitEvent(tv ropedomain.TraitEvent) error {
	return r.ClusterRepo.HandleTraitEvent(tv)
}
func (r *RelationPool) ClusterDB() iface.ClusterDB {
	return r.ClusterRepo
}
func (r *RelationPool) GetGraphStats() map[string]any {
	return r.ClusterRepo.GetGraphStats()
}

func (r *RelationPool) Register(triplet cface.TripletPort, creation cface.CreationPort) error {

	capLimit := 2048
	if !r.isTest.IsTest() {
		capLimit = 8192
	}
	var err error
	if r.Apool.ConsumeRelTxByFanout() != nil {
		//TODO 추후엔 그냥 패닉 내삼!
		r.txDistributor, err = NewTxDistributor(r.isTest, capLimit, r.Apool.ConsumeRelTxByFanout())
	}
	if err != nil {
		r.busCreation.Close()
		r.busTriplet.Close()
		return err
	}
	r.ports.triplet = triplet
	r.ports.creation = creation
	return nil
}

func (r *RelationPool) GetApooPort() pface.ApoolPort {
	return r.Apool
}

// 포트 기반 뷰어
func (r *RelationPool) GetTripletPort() cface.TripletPort { return r.ports.triplet }

func (r *RelationPool) GetCreationPort() cface.CreationPort { return r.ports.creation }

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
	return r.txDistributor.busTriplet.Dequeue()
}
func (r *RelationPool) ConsumeCreationTxByFanout() <-chan domain.MarkedTransaction {
	return r.txDistributor.busCreation.Dequeue()
}

func (r *RelationPool) Start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	//TODO 추후 이 코드 활성화하기. 크리에이션 생기고 나면 키기
	if !r.isTest.IsTest() && (r.ports.triplet == nil || r.ports.creation == nil) {
		return fmt.Errorf("relpools's ports not registered")
	}
	log.Printf("Starting RelPool")
	//TODO 모듈 개수 늘릴 시 최소 모듈 개수보단 크게 잡기
	moduleDone := make(chan error, 5)
	go func() {
		moduleDone <- r.GetTripletPort().Start(ctx)
	}()
	go func() {
		moduleDone <- r.GetCreationPort().Start(ctx)
	}()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer func() {
		signal.Stop(sigChan)
		close(sigChan)
	}()
	select {
	case <-ctx.Done():
		fmt.Printf("   ⏰ Test completed by timeout\n")
	case err := <-moduleDone:
		cancel()
		if err != nil {
			fmt.Printf("   ⚠️ Relpool's subModule stopped with error: %v\n", err)
		} else {
			fmt.Printf("   ✅ Relpool completed successfully\n")
		}
	case <-sigChan:
		fmt.Printf("   🛑 Shutdown signal received...\n")
		cancel()
	}
	return r.Close()
}
func (r *RelationPool) Close() error {
	//* 자기 자신만 close해도 start시 연결된 ctx로 자식도 종료
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
