package workerpool

import (
	"context"
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
	in := p.JobChan.Dequeue() // EventBus out 채널 (그대로 재사용)
	for {
		select {
		case <-p.ctx.Done():
			return
		case job, ok := <-in:
			if !ok {
				return
			}
			_ = job.Do(p.ctx)
		}
	}
}

// 외부 포인터/JobBus는 유지한 채, ctx만 바꿔 재시작
func (p *Pool) Restart(newCtx context.Context) {
	if newCtx == nil {
		newCtx = context.Background()
	}
	// 1) 기존 워커 전부 중단
	p.cancel()
	p.wg.Wait()

	// 2) 새 컨텍스트로 교체 후 워커 재스폰
	ctx, cancel := context.WithCancel(newCtx)
	p.ctx = ctx
	p.cancel = cancel
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

func (p *Pool) Shutdown() {
	p.cancel()
	p.wg.Wait()
}
