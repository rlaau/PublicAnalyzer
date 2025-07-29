package workerpool

import (
	"context"
	"sync"
)

// 인터페이스 Job: 각 작업은 Do()를 수행함
// * 제너릭을 쓰려 헀지만, 작업 자유도 측면에서 그냥 interface-struct사용하기로 함 (제너릭 쓴다면 함수 제너릭이겠지만, 굳이?)
type Job interface {
	Do(ctx context.Context) error
}

type Pool struct {
	JobChan <-chan Job // 외부에서 job 생산
	ErrChan chan error // 에러를 외부로 전달
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// 워커풀 생성: 외부 채널을 받아 처리
func New(ctx context.Context, numWorkers int, jobChan <-chan Job) *Pool {
	ctx, cancel := context.WithCancel(ctx)
	p := &Pool{
		JobChan: jobChan,
		ctx:     ctx,
		cancel:  cancel,
	}

	for i := 0; i < numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	return p
}

// 개별 워커 goroutine
func (p *Pool) worker(id int) {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case job, ok := <-p.JobChan:
			if !ok {
				return
			}
			_ = job.Do(p.ctx) // 에러 로깅 등은 외부 처리
		}
	}
}

// shutdown + join
func (p *Pool) Shutdown() {
	p.cancel()
	p.wg.Wait()
}
