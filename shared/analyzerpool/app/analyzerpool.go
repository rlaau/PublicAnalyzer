package app

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/analyzerpool/dto"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/analyzerpool/iface"
	computation "github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	kb "github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

type AnalyzerPool struct {
	isTest mode.ProcessingMode

	ports struct {
		cc  iface.CcPort
		ee  iface.EePort
		cce iface.CcePort
		eec iface.EecPort
	}
	//* 편의를 위해서 그냥 DTO바로 끼워넣음
	busCC  *eventbus.EventBus[dto.CcEvt]
	busEE  *eventbus.EventBus[dto.EeEvt]
	busCCE *eventbus.EventBus[dto.CceEvt]
	busEEC *eventbus.EventBus[dto.EecEvt]
	// 단일 Kafka 배치 소비자 + eventbus 팬아웃 매니저
	fanoutManager *TxFanoutManager

	closed atomic.Bool
}

func CreateAnalyzerPoolFrame(isTest mode.ProcessingMode) (*AnalyzerPool, error) {
	// 1) 루트 분기
	var root func() string
	if isTest.IsTest() {
		root = computation.FindTestingStorageRootPath
	} else {
		root = computation.FindProductionStorageRootPath
	}
	// 상대 경로 상수화
	rel := func(name string) string {
		return filepath.Join("analyzer_pool", "eventbus", fmt.Sprintf("%s.jsonl", name))
	}
	// capLimit: 모드에 따라 조정 (예시 값)
	capLimit := 2048
	if !isTest.IsTest() {
		capLimit = 8192
	}

	// 2) 이벤트 버스 생성 (JSONL 경로 = root()/analyzer_pool/eventbus/*.jsonl)
	bCC, err := eventbus.NewWithRoot[dto.CcEvt](root, rel("cc"), capLimit)
	if err != nil {
		return nil, err
	}
	bEE, err := eventbus.NewWithRoot[dto.EeEvt](root, rel("ee"), capLimit)
	if err != nil {
		bCC.Close()
		return nil, err
	}
	bCCE, err := eventbus.NewWithRoot[dto.CceEvt](root, rel("cce"), capLimit)
	if err != nil {
		bCC.Close()
		bEE.Close()
		return nil, err
	}
	bEEC, err := eventbus.NewWithRoot[dto.EecEvt](root, rel("eec"), capLimit)
	if err != nil {
		bCC.Close()
		bEE.Close()
		bCCE.Close()
		return nil, err
	}

	// 3) 단일 Kafka 배치 소비자 설정
	defBatchSize := 1000
	defTimeout := 300 // ms
	var kafkaCfg kb.KafkaBatchConfig
	if isTest.IsTest() {
		kafkaCfg = kb.KafkaBatchConfig{
			Brokers:      kb.GetGlobalBrokers(),
			Topic:        kb.TestingTxTopic,
			GroupID:      "testval.analyzer_pool",
			BatchSize:    defBatchSize,
			BatchTimeout: time.Duration(defTimeout) * time.Millisecond,
		}
	} else {
		//TODO 실제 프로덕션 시엔 더 정교하게
		kafkaCfg = kb.KafkaBatchConfig{
			Brokers:      kb.GetGlobalBrokers(),
			Topic:        kb.ProductionTxTopic,
			GroupID:      "production.analyzer_pool",
			BatchSize:    defBatchSize,
			BatchTimeout: time.Duration(defTimeout) * time.Millisecond,
		}
	}

	// 4) TxFanoutManager 생성 (단일 카프카 소비자 + eventbus 팬아웃)
	fanoutMgr, err := NewTxFanoutManager(kafkaCfg, isTest, capLimit)
	if err != nil {
		bCC.Close()
		bEE.Close()
		bCCE.Close()
		bEEC.Close()
		return nil, fmt.Errorf("failed to create fanout manager: %w", err)
	}

	return &AnalyzerPool{
		isTest: isTest,
		busCC:  bCC, busEE: bEE, busCCE: bCCE, busEEC: bEEC,
		fanoutManager: fanoutMgr,
	}, nil
}

// ---- Register & Port Views

func (p *AnalyzerPool) Register(cc iface.CcPort, ee iface.EePort, cce iface.CcePort, eec iface.EecPort) {
	p.ports.cc, p.ports.ee, p.ports.cce, p.ports.eec = cc, ee, cce, eec
}

func (p *AnalyzerPool) GetViewCC() iface.CcPort { return p.ports.cc }
func (p *AnalyzerPool) GetViewEE() iface.EePort { return p.ports.ee }
func (p *AnalyzerPool) GetViewCCE() iface.CcePort {
	return p.ports.cce
}
func (p *AnalyzerPool) GetViewEEC() iface.EecPort {
	return p.ports.eec
}

// ---- Event Publish / Dequeue

func (p *AnalyzerPool) PublishToCC(v dto.CcEvt) error {
	return p.busCC.Publish(v)
}
func (p *AnalyzerPool) PublishToEE(v dto.EeEvt) error {
	return p.busEE.Publish(v)
}
func (p *AnalyzerPool) PublishToCCE(v dto.CceEvt) error {
	return p.busCCE.Publish(v)
}
func (p *AnalyzerPool) PublishToEEC(v dto.EecEvt) error {
	return p.busEEC.Publish(v)
}

func (p *AnalyzerPool) DequeueCC() <-chan dto.CcEvt {
	return p.busCC.Dequeue()
}
func (p *AnalyzerPool) DequeueEE() <-chan dto.EeEvt {
	return p.busEE.Dequeue()
}
func (p *AnalyzerPool) DequeueCCE() <-chan dto.CceEvt {
	return p.busCCE.Dequeue()
}
func (p *AnalyzerPool) DequeueEEC() <-chan dto.EecEvt {
	return p.busEEC.Dequeue()
}

// ---- Tx Consume & Count (EventBus 기반)

// 각 모듈은 해당 eventbus에서 트랜잭션을 소비
func (p *AnalyzerPool) ConsumeCCTx() <-chan domain.MarkedTransaction {
	return p.fanoutManager.busCC.Dequeue()
}
func (p *AnalyzerPool) ConsumeEETx() <-chan domain.MarkedTransaction {
	return p.fanoutManager.busEE.Dequeue()
}
func (p *AnalyzerPool) ConsumeCCETx() <-chan domain.MarkedTransaction {
	return p.fanoutManager.busCCE.Dequeue()
}
func (p *AnalyzerPool) ConsumeEECTx() <-chan domain.MarkedTransaction {
	return p.fanoutManager.busEEC.Dequeue()
}

// 트랜잭션 카운트는 fanoutManager에서 제공
func (p *AnalyzerPool) TxCount() uint64 {
	return p.fanoutManager.Count()
}

// ---- Lifecycle

func (p *AnalyzerPool) Close(ctx context.Context) error {
	if p.closed.Swap(true) {
		return nil
	}
	// Kafka fanout manager 먼저 닫기
	_ = p.fanoutManager.Close()
	fmt.Printf("FanoutManager 정리 완료\n")
	// 이벤트 버스는 Close 시 pending 저장
	p.busCC.Close()
	p.busEE.Close()
	p.busCCE.Close()
	p.busEEC.Close()
	fmt.Printf("BUS들 정리 완료\n")
	return nil
}

func (p *AnalyzerPool) Wait() { /* no-op */ }
