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
	//* 여기도 그냥 바로 tx삽입
	fanCC  *txFanout[domain.MarkedTransaction]
	fanEE  *txFanout[domain.MarkedTransaction]
	fanCCE *txFanout[domain.MarkedTransaction]
	fanEEC *txFanout[domain.MarkedTransaction]

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

	// 3) Tx fanout (네가 만든 배치 컨슈머로 구성)
	//    토픽/그룹/브로커는 실제 값으로 대체
	defBatchSize := 1000
	defTimeout := 300 // ms
	var cfg func(string) kb.KafkaBatchConfig
	if isTest.IsTest() {
		cfg = func(groupSuffix string) kb.KafkaBatchConfig {
			return kb.KafkaBatchConfig{
				Brokers:      kb.GetGlobalBrokers(),
				Topic:        kb.TestingTxTopic,
				GroupID:      fmt.Sprintf("testval.%s", groupSuffix),
				BatchSize:    defBatchSize,
				BatchTimeout: time.Duration(defTimeout) * time.Millisecond,
			}
		}
	} else {
		//TODO 실제 프로덕션 시엔 더 정교하게
		cfg = func(groupSuffix string) kb.KafkaBatchConfig {
			return kb.KafkaBatchConfig{
				Brokers:      kb.GetGlobalBrokers(),
				Topic:        kb.ProductionTxTopic,
				GroupID:      fmt.Sprintf("production.%s", groupSuffix),
				BatchSize:    defBatchSize,
				BatchTimeout: time.Duration(defTimeout) * time.Millisecond,
			}
		}
	}

	fCC := newTxFanout[domain.MarkedTransaction](cfg("cc"))
	fEE := newTxFanout[domain.MarkedTransaction](cfg("ee"))
	fCCE := newTxFanout[domain.MarkedTransaction](cfg("cce"))
	fEEC := newTxFanout[domain.MarkedTransaction](cfg("eec"))

	return &AnalyzerPool{
		isTest: isTest,
		busCC:  bCC, busEE: bEE, busCCE: bCCE, busEEC: bEEC,
		fanCC: fCC, fanEE: fEE, fanCCE: fCCE, fanEEC: fEEC,
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

// ---- Tx Consume & Count

func (p *AnalyzerPool) ConsumeCCTx() <-chan domain.MarkedTransaction { return p.fanCC.Ch() }
func (p *AnalyzerPool) ConsumeEETx() <-chan domain.MarkedTransaction { return p.fanEE.Ch() }
func (p *AnalyzerPool) ConsumeCCETx() <-chan domain.MarkedTransaction {
	return p.fanCCE.Ch()
}
func (p *AnalyzerPool) ConsumeEECTx() <-chan domain.MarkedTransaction {
	return p.fanEEC.Ch()
}

func (p *AnalyzerPool) TxCountCC() uint64  { return p.fanCC.Count() }
func (p *AnalyzerPool) TxCountEE() uint64  { return p.fanEE.Count() }
func (p *AnalyzerPool) TxCountCCE() uint64 { return p.fanCCE.Count() }
func (p *AnalyzerPool) TxCountEEC() uint64 { return p.fanEEC.Count() }

// ---- Lifecycle

func (p *AnalyzerPool) Close(ctx context.Context) error {
	if p.closed.Swap(true) {
		return nil
	}
	// Kafka 먼저 닫기
	_ = p.fanCC.Close()
	_ = p.fanEE.Close()
	_ = p.fanCCE.Close()
	_ = p.fanEEC.Close()
	fmt.Printf("FAN들 정리 완료\n")
	// 이벤트 버스는 Close 시 pending 저장
	p.busCC.Close()
	p.busEE.Close()
	p.busCCE.Close()
	p.busEEC.Close()
	fmt.Printf("BUS들 정리 완료\n")
	return nil
}

func (p *AnalyzerPool) Wait() { /* no-op */ }
