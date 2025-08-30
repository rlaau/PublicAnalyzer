package workerpool

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
)

type Job interface {
	Do(ctx context.Context) error
}

type Pool struct {
	JobChan    *eventbus.EventBus[Job]
	ErrChan    chan error
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	NumWorkers int
}

func New(ctx context.Context, numWorkers int, jobBus *eventbus.EventBus[Job]) *Pool {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &Pool{
		JobChan:    jobBus,
		ctx:        ctx,
		cancel:     cancel,
		NumWorkers: numWorkers,
	}
	p.spawnWorkers(numWorkers)
	return p
}

func (p *Pool) spawnWorkers(n int) {
	for i := 0; i < n; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()
	in := p.JobChan.Dequeue()                        // EventBus out 채널 (그대로 재사용)
	log.Printf("[worker %d] started; ch=%p", id, in) // ← 시작 확인
	for {
		select {
		case <-p.ctx.Done():
			log.Printf("[worker %d] ctx done", id) // ← 종료 원인 확인
			return
		case job, ok := <-in:

			if !ok {
				log.Printf("[worker %d] channel closed", id) // ← 버스 종료 경로
				return
			}
			///log.Printf("[worker %d] recv job %T", id, job) // ← 실제 수신 확인
			if err := job.Do(p.ctx); err != nil {
				log.Printf("[worker %d] job error: %v", id, err)
			}
		}
	}
}

// 외부 포인터/JobBus는 유지한 채, ctx만 바꿔 재시작
func (p *Pool) Restart(newCtx context.Context) {
	log.Printf("워커 리스타트")
	if newCtx == nil {
		newCtx = context.Background()
	}
	// 1) 기존 워커 전부 중단
	p.cancel()
	p.wg.Wait()
	oldIn := p.JobChan

	// 2) 새 컨텍스트로 교체 후 워커 재스폰
	ctx, cancel := context.WithCancel(newCtx)
	p.ctx = ctx
	p.cancel = cancel
	p.JobChan = oldIn
	fmt.Printf("restart@pool: %p", p.JobChan)
	p.spawnWorkers(p.NumWorkers)
}

// 워커 수까지 함께 바꾸고 싶을 때
func (p *Pool) RestartWith(newCtx context.Context, newNumWorkers int) {
	if newCtx == nil {
		newCtx = context.Background()
	}
	p.cancel()
	p.wg.Wait()

	p.NumWorkers = newNumWorkers
	ctx, cancel := context.WithCancel(newCtx)
	p.ctx = ctx
	p.cancel = cancel
	p.spawnWorkers(p.NumWorkers)
}

// 안전한 풀 종료
func (p *Pool) Shutdown() {
	// 1) 버스 먼저 닫아 out 채널 close 유도
	if p.JobChan != nil {
		p.JobChan.Close()
	}
	// 2) 워커는 out 채널이 닫힐 때까지 읽다가 자연 종료
	p.wg.Wait()

	// (선택) 컨텍스트 취소는 마지막에
	p.cancel()
}
