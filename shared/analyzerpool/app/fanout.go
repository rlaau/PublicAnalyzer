package app

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	computation "github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	kb "github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// 기존 txFanout - 레거시, 사용하지 않음
type txFanout[TX any] struct {
	cons  *kb.KafkaBatchConsumer[TX]
	out   chan TX
	stop  chan struct{}
	wg    sync.WaitGroup
	count atomic.Uint64
}

// 새로운 단일 Kafka 배치 소비자 + eventbus 팬아웃
type TxFanoutManager struct {
	cons *kb.KafkaBatchConsumer[domain.MarkedTransaction]

	// 각 모듈별 eventbus
	busCC  *eventbus.EventBus[domain.MarkedTransaction]
	busEE  *eventbus.EventBus[domain.MarkedTransaction]
	busCCE *eventbus.EventBus[domain.MarkedTransaction]
	busEEC *eventbus.EventBus[domain.MarkedTransaction]

	stop  chan struct{}
	wg    sync.WaitGroup
	count atomic.Uint64
}

// 새로운 TxFanoutManager 생성자 - 내부적으로 transaction 버스들 생성
func NewTxFanoutManager(cfg kb.KafkaBatchConfig, isTest mode.ProcessingMode, capLimit int) (*TxFanoutManager, error) {
	// 테스트/프로덕션 루트 분기
	var root func() string
	if isTest.IsTest() {
		root = computation.FindTestingStorageRootPath
	} else {
		root = computation.FindProductionStorageRootPath
	}

	// Transaction 분배용 버스들 생성 (tx_fanout 폴더에 저장)
	rel := func(name string) string {
		return filepath.Join("analyzer_pool", "tx_fanout", fmt.Sprintf("%s.jsonl", name))
	}

	busCC, err := eventbus.NewWithRoot[domain.MarkedTransaction](root, rel("tx_cc"), capLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to create CC tx bus: %w", err)
	}

	busEE, err := eventbus.NewWithRoot[domain.MarkedTransaction](root, rel("tx_ee"), capLimit)
	if err != nil {
		busCC.Close()
		return nil, fmt.Errorf("failed to create EE tx bus: %w", err)
	}

	busCCE, err := eventbus.NewWithRoot[domain.MarkedTransaction](root, rel("tx_cce"), capLimit)
	if err != nil {
		busCC.Close()
		busEE.Close()
		return nil, fmt.Errorf("failed to create CCE tx bus: %w", err)
	}

	busEEC, err := eventbus.NewWithRoot[domain.MarkedTransaction](root, rel("tx_eec"), capLimit)
	if err != nil {
		busCC.Close()
		busEE.Close()
		busCCE.Close()
		return nil, fmt.Errorf("failed to create EEC tx bus: %w", err)
	}

	f := &TxFanoutManager{
		cons:   kb.NewKafkaBatchConsumer[domain.MarkedTransaction](cfg),
		busCC:  busCC,
		busEE:  busEE,
		busCCE: busCCE,
		busEEC: busEEC,
		stop:   make(chan struct{}),
	}
	f.wg.Add(1)
	go f.run()
	return f, nil
}

// 새로운 TxFanoutManager의 run 메서드 - 단일 Kafka 소비자 + eventbus 팬아웃
func (f *TxFanoutManager) run() {
	defer f.wg.Done()

	for {
		// 종료 신호 우선 확인
		select {
		case <-f.stop:
			return
		default:
		}

		// 배치 메시지 소비
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		msgs, err := f.cons.ReadMessagesBatch(ctx)
		cancel()

		if err != nil {
			// 종료 신호면 탈출
			select {
			case <-f.stop:
				return
			default:
			}

			// 타임아웃/취소는 정상 폴링 상황
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				continue
			}

			// 그 외 에러만 로그
			fmt.Printf("[TxFanoutManager] read err: %v\n", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// 배치 메시지들을 개별적으로 팬아웃
		for _, msg := range msgs {
			f.fanoutTransaction(msg.Value)
			f.count.Add(1)
		}
	}
}

// 트랜잭션 분배 로직
func (f *TxFanoutManager) fanoutTransaction(tx domain.MarkedTransaction) {
	//TODO: 목표는 애널라이저가 각 모듈에게 적절한 tx분배하는 것. 현재는 오직 EE모듈에만 tx주고 있음. 추후 MarkTransaction기반으로 올바르게 전달할 것.

	// !!!!!!현재는 모든 트랜잭션을 EE 모듈에만 전달
	if err := f.busEE.Publish(tx); err != nil {
		fmt.Printf("[TxFanoutManager] failed to publish to EE: %v\n", err)
	}

	// !!향후 구현될 분배 로직:
	//TODO 내 경우, 포트 기반 분배
	// switch {
	// case tx.TxSyntax[0] == domain.EOAMark && tx.TxSyntax[1] == domain.EOAMark:
	//     // EOA -> EOA: EE 모듈
	//     f.busEE.Publish(tx)
	// case tx.TxSyntax[0] == domain.ContractMark && tx.TxSyntax[1] == domain.ContractMark:
	//     // Contract -> Contract: CC 모듈
	//     f.busCC.Publish(tx)
	// case tx.TxSyntax[0] == domain.ContractMark && tx.TxSyntax[1] == domain.EOAMark:
	//     // Contract -> EOA: CCE 모듈
	//     f.busCCE.Publish(tx)
	// case tx.TxSyntax[0] == domain.EOAMark && tx.TxSyntax[1] == domain.ContractMark:
	//     // EOA -> Contract: EEC 모듈
	//     f.busEEC.Publish(tx)
	// }
}
func (f *txFanout[TX]) Close() error {
	close(f.stop)         // run 루프 깨우기
	f.wg.Wait()           // goroutine 합류
	close(f.out)          // 소비자들에게 종료 신호
	return f.cons.Close() // 카프카 리더 닫기
}

// TxFanoutManager Close 메서드
func (f *TxFanoutManager) Close() error {
	close(f.stop) // run 루프 깨우기
	f.wg.Wait()   // goroutine 합류

	// 자체 관리하는 transaction 버스들 정리
	f.busCC.Close()
	f.busEE.Close()
	f.busCCE.Close()
	f.busEEC.Close()

	return f.cons.Close() // 카프카 리더 닫기
}

// TxFanoutManager Count 메서드
func (f *TxFanoutManager) Count() uint64 {
	return f.count.Load()
}

func (f *txFanout[TX]) Ch() <-chan TX { return f.out }
func (f *txFanout[TX]) Count() uint64 { return f.count.Load() }
