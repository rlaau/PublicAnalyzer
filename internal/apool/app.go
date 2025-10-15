package apool

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
)

type AnalyzerPool struct {
	isTest mode.ProcessingMode

	ports struct {
		rel cface.RelPort
		nod cface.NodPort
	}

	busRel        *eventbus.EventBus[cface.RelMsg]
	busNod        *eventbus.EventBus[cface.NodMsg]
	fanoutManager *TxFanoutManager
	closed        atomic.Bool
}

func CreateAnalzerPoolFrame(isTest mode.ProcessingMode, bp tools.CountingBackpressure) (*AnalyzerPool, error) {

	var root func() string
	if isTest.IsTest() {
		root = computation.FindTestingStorageRootPath
	} else {
		root = computation.FindProductionStorageRootPath
	}
	rel := func(name string) string {
		return filepath.Join("analyzer_pool", "eventbus", fmt.Sprintf("%s.jsonl", name))
	}
	// capLimit: 모드에 따라 조정 (예시 값)
	//TODO 이 값들이 좀 커야 하긴 함.
	//TODO 애초에 백프레셔가 의미 있는 수준으로 가려면
	//TODO 카프카 캡*비율=적정량(한 8만) 보다 좀더 큰 수준으로 큐 길이가 나와야됨
	//TODO 한 10만은 잡기
	//TODO 백프레셔 성능이 좀 구리다 싶음 산하 이벤트버스 캡리밋 조절해보자!
	capLimit := 2048
	if !isTest.IsTest() {
		capLimit = 8192
	}

	busNod, err := eventbus.NewWithRoot[cface.NodMsg](root, rel("nod"), capLimit)
	if err != nil {
		return nil, err
	}
	busRel, err := eventbus.NewWithRoot[cface.RelMsg](root, rel("rel"), capLimit)
	if err != nil {
		busNod.Close()
		return nil, err
	}

	a := &AnalyzerPool{
		isTest: isTest,
		busRel: busRel,
		busNod: busNod,
	}
	defaultBatchSize := 5000
	defTimeout := 300 //ms
	var kafkaCfg kafka.KafkaBatchConfig
	if a.isTest.IsTest() {
		kafkaCfg = kafka.KafkaBatchConfig{
			Brokers:      kafka.GetGlobalBrokers(),
			Topic:        kafka.TestingTxTopic,
			GroupID:      "testval.analyzer_pool",
			BatchSize:    defaultBatchSize,
			BatchTimeout: time.Duration(defTimeout) * time.Millisecond,
		}
	} else {
		//TODO 실제 프로덕션 시엔 더 정교하게
		kafkaCfg = kafka.KafkaBatchConfig{
			Brokers:      kafka.GetGlobalBrokers(),
			Topic:        kafka.ProductionTxTopic,
			GroupID:      "production.analyzer_pool",
			BatchSize:    defaultBatchSize,
			BatchTimeout: time.Duration(defTimeout) * time.Millisecond,
		}
	}
	TxCapLimit := 2048
	if !a.isTest.IsTest() {
		TxCapLimit = 8192
	}
	a.fanoutManager, err = NewTxFanoutManager(kafkaCfg, a.isTest, TxCapLimit, bp)
	return a, nil
}

func (a *AnalyzerPool) Register(rel cface.RelPort, nod cface.NodPort) {
	a.busNod.Close()
	a.busRel.Close()
	a.ports.rel = rel
	a.ports.nod = nod
}

func (a *AnalyzerPool) GetRelPort() cface.RelPort { return a.ports.rel }
func (a *AnalyzerPool) GetNodPort() cface.NodPort { return a.ports.nod }

func (a *AnalyzerPool) EnqueueToRel(v cface.RelMsg) error {
	return a.busRel.Publish(v)
}
func (a *AnalyzerPool) EnqueueToNod(v cface.NodMsg) error {
	return a.busNod.Publish(v)
}

func (a *AnalyzerPool) DeququeRel() <-chan cface.RelMsg {
	return a.busRel.Dequeue()
}

func (a *AnalyzerPool) DeququeNod() <-chan cface.NodMsg {
	return a.busNod.Dequeue()
}

func (a *AnalyzerPool) ConsumeRelTxByFanout() <-chan domain.MarkedTransaction {
	if a.fanoutManager == nil || a.fanoutManager.busRel == nil {
		// 빈(닫힌) 채널을 돌려줘서 호출 측이 안전하게 select 탈출 가능
		ch := make(chan domain.MarkedTransaction)
		close(ch)
		return ch
	}
	return a.fanoutManager.busRel.Dequeue()
}

func (a *AnalyzerPool) ConsumeNodTxByFanout() <-chan domain.MarkedTransaction {
	if a.fanoutManager == nil || a.fanoutManager.busNod == nil {
		ch := make(chan domain.MarkedTransaction)
		close(ch)
		return ch
	}
	return a.fanoutManager.busNod.Dequeue()
}
func (a *AnalyzerPool) Start(parent context.Context) error {
	if !a.isTest.IsTest() && a.fanoutManager == nil {
		return fmt.Errorf("fanout manager not initialized (Register not called or failed)")
	}
	ctx, cancle := context.WithCancel(parent)
	defer cancle()
	if !a.isTest.IsTest() && (a.ports.nod == nil || a.ports.rel == nil) {
		return fmt.Errorf("analyzer pool's ports not registered")
	}

	log.Printf("Starting AnalyzerPool")
	//TODO 모듈 개수 늘릴 시 최소 모듈 개수보단 크게 잡기
	moduleDone := make(chan error, 5)
	go func() {
		moduleDone <- a.GetNodPort().Start(ctx)
	}()
	go func() {
		moduleDone <- a.GetRelPort().Start(ctx)
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
		cancle()
		if err != nil {
			fmt.Printf("   ⚠️ AnalyzerPool's subModule stopped with error: %v\n", err)
		} else {
			fmt.Printf("   ✅ AnalyzerPool's completed successfully\n")
		}
	case <-sigChan:
		cancle()
		fmt.Printf("   🛑 Shutdown signal received...\n")
	}
	return a.Close()
}

func (a *AnalyzerPool) Close() error {
	if a.closed.Swap(true) {
		return nil
	}

	_ = a.fanoutManager.Close()
	fmt.Printf("apool의 fanoutmanger종료 완료\n")
	a.busNod.Close()
	a.busRel.Close()
	fmt.Printf("apool버스들 종료 완료 \n")
	return nil
}

// TODO 추후 지울것.
// TODO 포트 등록은 정식 생성자 통해서 할것!!!
func (a *AnalyzerPool) RegisterPorts(nod cface.NodPort, rel cface.RelPort) {
	if a.busNod != nil {
		a.busNod.Close()
	}
	if a.busRel != nil {
		a.busRel.Close()
	}
	a.ports.rel = rel
	a.ports.nod = nod
}
