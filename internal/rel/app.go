package rel

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
)

// RelationPool내의 관계 분석기들
type TripletPort interface{}
type CreationPort interface{}

// 관계 분석기 간 커멘드 통신 위한 이벤드 정의
type TripletEventMsg struct{}
type CreationEventMsg struct{}

// 관계 분석 시, 각 단일 분석기가 통신하기 위한 풀
type RelationPool struct {
	isTest mode.ProcessingMode

	ports struct {
		triplet  TripletPort
		creation CreationPort
	}

	busTriplet  *eventbus.EventBus[TripletEventMsg]
	busCreation *eventbus.EventBus[CreationEventMsg]

	fanoutManager *TxFanoutManager
	closed        atomic.Bool
}

func CreateRelationPoolFrame(isTest mode.ProcessingMode) (*RelationPool, error) {
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
	busTriplet, err := eventbus.NewWithRoot[TripletEventMsg](root, rel("triplet"), capLimit)
	if err != nil {
		return nil, err
	}
	busCreation, err := eventbus.NewWithRoot[CreationEventMsg](root, rel("creation"), capLimit)
	if err != nil {
		busTriplet.Close()
		return nil, err
	}

	return &RelationPool{
		isTest:      isTest,
		busTriplet:  busTriplet,
		busCreation: busCreation,
	}, nil
}

func (r *RelationPool) Register(triplet TripletPort, creation CreationPort, bp tools.CountingBackpressure) {

	defaultBatchSize := 5000
	defTimeout := 300 //ms
	var kafkaCfg kafka.KafkaBatchConfig
	if r.isTest.IsTest() {
		kafkaCfg = kafka.KafkaBatchConfig{
			Brokers:      kafka.GetGlobalBrokers(),
			Topic:        kafka.TestingTxTopic,
			GroupID:      "testval.relation_pool",
			BatchSize:    defaultBatchSize,
			BatchTimeout: time.Duration(defTimeout) * time.Millisecond,
		}
	} else {
		//TODO 실제 프로덕션 시엔 더 정교하게
		kafkaCfg = kafka.KafkaBatchConfig{
			Brokers:      kafka.GetGlobalBrokers(),
			Topic:        kafka.ProductionTxTopic,
			GroupID:      "production.relation_pool",
			BatchSize:    defaultBatchSize,
			BatchTimeout: time.Duration(defTimeout) * time.Millisecond,
		}
	}
	capLimit := 2048
	if !r.isTest.IsTest() {
		capLimit = 8192
	}
	var err error
	r.fanoutManager, err = NewTxFanoutManager(kafkaCfg, r.isTest, capLimit, bp)
	if err != nil {
		r.busCreation.Close()
		r.busTriplet.Close()
	}
	r.ports.triplet = triplet
	r.ports.creation = creation
}

// 포트 기반 뷰어
func (r *RelationPool) GetTripletPort() TripletPort { return r.ports.triplet }

func (r *RelationPool) GetCreationPort() CreationPort { return r.ports.creation }

// 이벤트 버스 기반 커멘더
func (r *RelationPool) EnqueueToTriplet(v TripletEventMsg) error {
	return r.busTriplet.Publish(v)
}
func (r *RelationPool) EnqueueToCreation(v CreationEventMsg) error {
	return r.busCreation.Publish(v)
}

// 이벤트 버스 기반 소비
// 분선 모듈 상호 간의 이벤트버스
func (r *RelationPool) DequeueTriplet() <-chan TripletEventMsg {
	return r.busTriplet.Dequeue()
}
func (r *RelationPool) DequeueCreation() <-chan CreationEventMsg {
	return r.busCreation.Dequeue()
}

// Tx Fanout에서의 소비자.
func (r *RelationPool) ConsumeTripletTxByFanout() <-chan domain.MarkedTransaction {
	return r.fanoutManager.busTriplet.Dequeue()
}
func (r *RelationPool) ConsumeCreationTxByFanout() <-chan domain.MarkedTransaction {
	return r.fanoutManager.busCreation.Dequeue()
}

func (r *RelationPool) Close(ctx context.Context) error {
	if r.closed.Swap(true) {
		return nil
	}

	_ = r.fanoutManager.Close()
	fmt.Printf("FanoutManager 정리 완료\n")
	r.busTriplet.Close()
	r.busCreation.Close()
	fmt.Printf("BUS들 정리 완료\n")
	return nil
}
