package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	kb "github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
)

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

	for {
		// 종료 신호 우선 확인 (즉시 탈출)
		select {
		case <-f.stop:
			fmt.Printf("도달")
			return
		default:
		}

		// 매회 새로운 컨텍스트 + 타임아웃
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		msgs, err := f.cons.ReadMessagesBatch(ctx)
		cancel() // 누수 방지: 매회 반드시 cancel

		if err != nil {
			// 종료 신호면 탈출
			select {
			case <-f.stop:
				return
			default:
			}

			// 타임아웃/취소는 정상 폴링 상황: 조용히 다음 루프로
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				continue
			}

			// 그 외 에러만 로그 + 짧은 백오프
			fmt.Printf("[tx] read err: %v\n", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// 메시지 팬아웃
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
	close(f.stop)         // run 루프 깨우기
	f.wg.Wait()           // goroutine 합류
	close(f.out)          // 소비자들에게 종료 신호
	return f.cons.Close() // 카프카 리더 닫기
}

func (f *txFanout[TX]) Ch() <-chan TX { return f.out }
func (f *txFanout[TX]) Count() uint64 { return f.count.Load() }
