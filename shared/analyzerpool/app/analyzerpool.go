package app

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/analyzerpool/iface"
	computation "github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	kb "github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// 내부 상수(placeholder). 실제 값은 사용자 프로젝트에서 수정.
const (
	poolFolder   = "analyzer_pool"
	subEventBus  = "eventbus"
	defaultTopic = "testval" // 토픽/그룹 접두사는 임시값
	defaultGroup = "testval"
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

// ======== 2) JSONL 동기 백업 버스 (out 하나만 노출) ========

type simpleBus[T any] struct {
	out      chan T
	mu       sync.Mutex
	cv       *sync.Cond
	pending  []T
	capLimit int

	filePath string // JSONL 저장 경로

	stopping bool
	closed   atomic.Bool
	wg       sync.WaitGroup
}

func newSimpleBus[T any](filePath string, capLimit int) (*simpleBus[T], error) {
	b := &simpleBus[T]{
		out:      make(chan T),
		filePath: filePath,
		capLimit: capLimit,
	}
	b.cv = sync.NewCond(&b.mu)

	// 1) 시작 시 JSONL 로드 (동기)
	backlog, err := loadBacklogJSONL[T](filePath)
	if err != nil {
		return nil, err
	}
	_ = os.Remove(filePath) // 중복 로드 방지

	// 2) 런 루프 시작
	b.wg.Add(1)
	go b.run(backlog)
	return b, nil
}

func (b *simpleBus[T]) run(backlog []T) {
	defer b.wg.Done()
	b.mu.Lock()
	b.pending = append(b.pending, backlog...)
	for {
		// 대기: pending 없고, stop 아니면 cond wait
		for !b.stopping && len(b.pending) == 0 {
			b.cv.Wait()
		}
		if b.stopping {
			// 남은 것 저장
			rest := append([]T(nil), b.pending...)
			b.mu.Unlock()
			if err := saveBacklogJSONL(b.filePath, rest); err != nil {
				fmt.Printf("[bus] saveBacklog error: %v", err)
			}
			close(b.out)
			return
		}
		// 하나 팝 & 뮤텍스 잠깐 풀고 out 전송 (생산자 역압 해제 위해 시그널)
		v := b.pending[0]
		b.pending = b.pending[1:]
		b.cv.Signal() // capLimit로 블록 중인 생산자 깨우기
		b.mu.Unlock()

		// out 으로 블로킹 전송
		b.out <- v

		// 다음 루프 준비
		b.mu.Lock()
	}
}

// Publish: 생산자 → pending. capLimit 초과 시 cond wait 로 역압.
func (b *simpleBus[T]) Publish(v T) error {
	if b.closed.Load() {
		return errors.New("bus closed")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for !b.stopping && b.capLimit > 0 && len(b.pending) >= b.capLimit {
		b.cv.Wait()
	}
	if b.stopping {
		return errors.New("bus stopping")
	}
	b.pending = append(b.pending, v)
	b.cv.Signal()
	return nil
}

func (b *simpleBus[T]) Dequeue() <-chan T { return b.out }

func (b *simpleBus[T]) Close() {
	if b.closed.Swap(true) {
		return
	}
	b.mu.Lock()
	b.stopping = true
	b.cv.Broadcast()
	b.mu.Unlock()
	b.wg.Wait()
}

// ---- JSONL helpers

func loadBacklogJSONL[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	dec := json.NewDecoder(r)
	var items []T
	for {
		var v T
		if err := dec.Decode(&v); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		items = append(items, v)
	}
	return items, nil
}

func saveBacklogJSONL[T any](path string, items []T) error {
	if len(items) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(w)
	for _, v := range items {
		if err := enc.Encode(&v); err != nil {
			return err
		}
	}
	return w.Flush()
}

// ======== 3) Kafka Tx Fan-out (사용자 제공 Consumer 사용) ========

type txFanout[TX any] struct {
	cons  *kb.KafkaBatchConsumer[TX]
	out   chan TX
	stop  chan struct{}
	wg    sync.WaitGroup
	count atomic.Uint64
}

func newTxFanout[TX any](cfg kb.KafkaBatchConfig) *txFanout[TX] {
	f := &txFanout[TX]{
		cons: kb.NewKafkaBatchConsumer[TX](cfg),
		out:  make(chan TX, cfg.BatchSize),
		stop: make(chan struct{}),
	}
	f.wg.Add(1)
	go f.run()
	return f
}

func (f *txFanout[TX]) run() {
	defer f.wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		// 배치 읽기 (첫 메시지는 무한대기 가능; 외부 종료 시 cancel)
		msgs, err := f.cons.ReadMessagesBatch(ctx)
		if err != nil {
			select {
			case <-f.stop:
				return
			default:
				fmt.Printf("[tx] read err: %v", err)
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}
		for _, m := range msgs {
			select {
			case f.out <- m.Value:
				f.count.Add(1)
			case <-f.stop:
				return
			}
		}
	}
}

func (f *txFanout[TX]) Close() error {
	close(f.stop)
	f.wg.Wait()
	close(f.out)
	return f.cons.Close()
}

func (f *txFanout[TX]) Ch() <-chan TX { return f.out }
func (f *txFanout[TX]) Count() uint64 { return f.count.Load() }

// ======== 4) AnalyzerPool 본체 ========

type AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX any] struct {
	isTest mode.ProcessingMode

	ports struct {
		cc  iface.CCPort
		ee  iface.EEPort
		cce iface.CCEPort
		eec iface.EECPort
	}

	busCC  *simpleBus[CCEvt]
	busEE  *simpleBus[EEEvt]
	busCCE *simpleBus[CCEEvt]
	busEEC *simpleBus[EECEvt]

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
	apRoot := filepath.Join(root, poolFolder)
	ebRoot := filepath.Join(apRoot, subEventBus)

	// 2) 이벤트 버스 파일 경로(JSONL)
	bCC, err := newSimpleBus[CCEvt](filepath.Join(ebRoot, "cc.jsonl"), def.chanCap)
	if err != nil {
		return nil, err
	}
	bEE, err := newSimpleBus[EEEvt](filepath.Join(ebRoot, "ee.jsonl"), def.chanCap)
	if err != nil {
		bCC.Close()
		return nil, err
	}
	bCCE, err := newSimpleBus[CCEEvt](filepath.Join(ebRoot, "cce.jsonl"), def.chanCap)
	if err != nil {
		bCC.Close()
		bEE.Close()
		return nil, err
	}
	bEEC, err := newSimpleBus[EECEvt](filepath.Join(ebRoot, "eec.jsonl"), def.chanCap)
	if err != nil {
		bCC.Close()
		bEE.Close()
		bCCE.Close()
		return nil, err
	}

	// 3) Kafka 배치 컨슈머 구성 (브로커는 글로벌에서 가져오되 없으면 그대로)
	brokers := kb.GetGlobalBrokers()
	cfg := func(groupSuffix string) kb.KafkaBatchConfig {
		return kb.KafkaBatchConfig{
			Brokers:      brokers,
			Topic:        defaultTopic,
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

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) Register(cc iface.CCPort, ee iface.EEPort, cce iface.CCEPort, eec iface.EECPort) {
	p.ports.cc, p.ports.ee, p.ports.cce, p.ports.eec = cc, ee, cce, eec
}

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) GetViewCC() iface.CCPort { return p.ports.cc }
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) GetViewEE() iface.EEPort { return p.ports.ee }
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) GetViewCCE() iface.CCEPort {
	return p.ports.cce
}
func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) GetViewEEC() iface.EECPort {
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
	// 이벤트 버스는 Close 시 pending 저장
	p.busCC.Close()
	p.busEE.Close()
	p.busCCE.Close()
	p.busEEC.Close()
	return nil
}

func (p *AnalyzerPool[CCEvt, EEEvt, CCEEvt, EECEvt, TX]) Wait() { /* no-op */ }
