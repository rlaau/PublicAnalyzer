package apool

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
)

// 새로운 단일 Kafka 배치 소비자 + eventbus 팬아웃
type TxFanoutManager struct {
	consumer *kafka.KafkaBatchConsumer[domain.MarkedTransaction]

	// 각 모듈별 eventbus
	busNod *eventbus.EventBus[domain.MarkedTransaction]
	busRel *eventbus.EventBus[domain.MarkedTransaction]

	stop chan struct{}
	wg   sync.WaitGroup
	bp   tools.CountingBackpressure
}

// 새로운 TxFanoutManager 생성자 - 내부적으로 transaction 버스들 생성
func NewTxFanoutManager(cfg kafka.KafkaBatchConfig, isTest mode.ProcessingMode, capLimit int, bp tools.CountingBackpressure) (*TxFanoutManager, error) {
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

	busNod, err := eventbus.NewWithRoot[domain.MarkedTransaction](root, rel("tx_nod"), capLimit)

	busRel, err := eventbus.NewWithRoot[domain.MarkedTransaction](root, rel("tx_rel"), capLimit)
	if err != nil {
		busNod.Close()
		return nil, fmt.Errorf("failed to create CCE tx bus: %w", err)
	}

	f := &TxFanoutManager{
		consumer: kafka.NewKafkaBatchConsumer[domain.MarkedTransaction](cfg),
		busNod:   busNod,
		busRel:   busRel,
		stop:     make(chan struct{}),
		bp:       bp,
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
		msgs, err := f.consumer.ReadMessagesBatch(ctx)
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
			//* 현재는 여기서 하나하나 콘슈밍 계산
			f.bp.CountConsuming()
			f.fanoutTransaction(msg.Value)
		}
	}
}

// 트랜잭션 분배 로직
func (f *TxFanoutManager) fanoutTransaction(tx domain.MarkedTransaction) {
	if err := f.busNod.Publish(tx); err != nil {
		fmt.Printf("[TxFanoutManager] failed to publish to Nod: %v\n", err)
	}
	if err := f.busRel.Publish(tx); err != nil {
		fmt.Printf("[TxFanoutManager] failed to publish to Rel: %v\n", err)
	}
}

// TxFanoutManager Close 메서드
func (f *TxFanoutManager) Close() error {
	close(f.stop) // run 루프 깨우기
	f.wg.Wait()   // goroutine 합류

	// 자체 관리하는 transaction 버스들 정리
	f.busNod.Close()
	f.busRel.Close()

	return f.consumer.Close() // 카프카 리더 닫기
}
