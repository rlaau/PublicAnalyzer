package app

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/analyzerpool/iface"
	computation "github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	kb "github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// 내부 상수(placeholder). 실제 값은 사용자 프로젝트에서 수정.
const (
	poolFolder   = "analyzer_pool"
	subEventBus  = "eventbus"
	defaultGroup = "analyzerpool"
)

type modeDefaults struct {
	batchSize    int
	batchTimeout time.Duration
	chanCap      int // 이벤트 pending 상한 (역압)
}

func pickDefaults(m mode.ProcessingMode) modeDefaults {
	if m.IsTest() {
		return modeDefaults{batchSize: 1000, batchTimeout: 150 * time.Millisecond, chanCap: 2048}
	}
	return modeDefaults{batchSize: 10000, batchTimeout: 200 * time.Millisecond, chanCap: 8192}
}

type AnalyzerPool[CcEvt, EeEvt, CceEvt, EecEvt, TX any] struct {
	isTest mode.ProcessingMode

	ports struct {
		cc  iface.CcPort
		ee  iface.EePort
		cce iface.CcePort
		eec iface.EecPort
	}

	busCC  *EventBus[CcEvt]
	busEE  *EventBus[EeEvt]
	busCCE *EventBus[CceEvt]
	busEEC *EventBus[EecEvt]

	fanCC  *txFanout[TX]
	fanEE  *txFanout[TX]
	fanCCE *txFanout[TX]
	fanEEC *txFanout[TX]

	closed atomic.Bool
}

func CreateAnalyzerPoolFrame[CCEvt, EEEvt, CCEEvt, EECEvt, TX any](isTest mode.ProcessingMode) (*AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX], error) {
	def := pickDefaults(isTest)

	// 1) 루트 분기
	var root string
	if isTest.IsTest() {
		root = computation.FindTestingStorageRootPath()
	} else {
		root = computation.FindProductionStorageRootPath()
	}
	analyzerPoolRoot := filepath.Join(root, poolFolder)
	eventBusRoot := filepath.Join(analyzerPoolRoot, subEventBus)

	// 2) 이벤트 버스 파일 경로(JSONL)
	bCC, err := newSimpleBus[CCEvt](filepath.Join(eventBusRoot, "cc.jsonl"), def.chanCap)
	if err != nil {
		return nil, err
	}
	bEE, err := newSimpleBus[EEEvt](filepath.Join(eventBusRoot, "ee.jsonl"), def.chanCap)
	if err != nil {
		bCC.Close()
		return nil, err
	}
	bCCE, err := newSimpleBus[CCEEvt](filepath.Join(eventBusRoot, "cce.jsonl"), def.chanCap)
	if err != nil {
		bCC.Close()
		bEE.Close()
		return nil, err
	}
	bEEC, err := newSimpleBus[EECEvt](filepath.Join(eventBusRoot, "eec.jsonl"), def.chanCap)
	if err != nil {
		bCC.Close()
		bEE.Close()
		bCCE.Close()
		return nil, err
	}

	var kafkaTxTopic string
	if isTest.IsTest() {
		kafkaTxTopic = kafka.TestingTxTopic
	} else {
		kafkaTxTopic = kafka.ProductionTxTopic
	}
	// 3) Kafka 배치 컨슈머 구성 (브로커는 글로벌에서 가져오되 없으면 그대로)
	brokers := kb.GetGlobalBrokers()
	cfg := func(groupSuffix string) kb.KafkaBatchConfig {
		return kb.KafkaBatchConfig{
			Brokers:      brokers,
			Topic:        kafkaTxTopic,
			GroupID:      fmt.Sprintf("%s.%s", defaultGroup, groupSuffix),
			BatchSize:    def.batchSize,
			BatchTimeout: def.batchTimeout,
		}
	}

	fCC := newTxFanout[TX](cfg("cc"))
	fEE := newTxFanout[TX](cfg("ee"))
	fCCE := newTxFanout[TX](cfg("cce"))
	fEEC := newTxFanout[TX](cfg("eec"))

	return &AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]{
		isTest: isTest,
		busCC:  bCC,
		busEE:  bEE,
		busCCE: bCCE,
		busEEC: bEEC,
		fanCC:  fCC,
		fanEE:  fEE,
		fanCCE: fCCE,
		fanEEC: fEEC,
	}, nil
}

// ---- Register & Port Views

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) Register(cc iface.CcPort, ee iface.EePort, cce iface.CcePort, eec iface.EecPort) {
	p.ports.cc, p.ports.ee, p.ports.cce, p.ports.eec = cc, ee, cce, eec
}

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) GetViewCC() iface.CcPort { return p.ports.cc }
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) GetViewEE() iface.EePort { return p.ports.ee }
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) GetViewCCE() iface.CcePort {
	return p.ports.cce
}
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) GetViewEEC() iface.EecPort {
	return p.ports.eec
}

// ---- Event Publish / Dequeue

func (p *AnalyzerPool[CcEvt, EEEvt, CCEEvt, EECEvt, TX]) PublishToCC(v CcEvt) error {
	return p.busCC.Publish(v)
}
func (p *AnalyzerPool[CCEvt, EeEvt, CCEEvt, EECEvt, TX]) PublishToEE(v EeEvt) error {
	return p.busEE.Publish(v)
}
func (p *AnalyzerPool[CCEvt, EEEvt, CceEvt, EECEvt, TX]) PublishToCCE(v CceEvt) error {
	return p.busCCE.Publish(v)
}
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EecEvt, TX]) PublishToEEC(v EecEvt) error {
	return p.busEEC.Publish(v)
}

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) DequeueCC() <-chan CCEvt {
	return p.busCC.Dequeue()
}
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) DequeueEE() <-chan EEEvt {
	return p.busEE.Dequeue()
}
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) DequeueCCE() <-chan CCEEvt {
	return p.busCCE.Dequeue()
}
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) DequeueEEC() <-chan EECEvt {
	return p.busEEC.Dequeue()
}

// ---- Tx Consume & Count

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) ConsumeCCTx() <-chan TX { return p.fanCC.Ch() }
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) ConsumeEETx() <-chan TX { return p.fanEE.Ch() }
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) ConsumeCCETx() <-chan TX {
	return p.fanCCE.Ch()
}
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) ConsumeEECTx() <-chan TX {
	return p.fanEEC.Ch()
}

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) TxCountCC() uint64  { return p.fanCC.Count() }
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) TxCountEE() uint64  { return p.fanEE.Count() }
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) TxCountCCE() uint64 { return p.fanCCE.Count() }
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) TxCountEEC() uint64 { return p.fanEEC.Count() }

// ---- Lifecycle

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) Close(ctx context.Context) error {
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

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) Wait() { /* no-op */ }
