package workerpool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"golang.org/x/sync/errgroup"
)

type Job interface {
	Do(ctx context.Context) error
}
type Pool struct {
	name       gface.ModuleName
	JobChan    *eventbus.EventBus[Job]
	NumWorkers int

	ownsBus  bool // 버스 닫기 책임
	failFast bool // 에러 시 즉시 중단(기본 true)
	closed   atomic.Bool
	running  atomic.Bool
}

func New(numWorkers int, name gface.ModuleName) *Pool {
	jobChanName := fmt.Sprintf("WorkerPool%s JobChan", name)
	return &Pool{
		JobChan:    eventbus.NewInMemory[Job](2000, gface.ModuleName(jobChanName)),
		NumWorkers: numWorkers,
		name:       name,
		failFast:   true,
		ownsBus:    false,
	}
}

var (
	ErrPoolClosed      = fmt.Errorf("pool closed")
	ErrPoolNotRunning  = fmt.Errorf("pool not running")
	ErrNilJob          = fmt.Errorf("nil job")
	ErrEnqueueCanceled = fmt.Errorf("enqueue canceled")
)

func (p *Pool) DispatchJob(j Job) error {
	if j == nil {
		return ErrNilJob
	}
	// 종료/미가동 가드
	if p.closed.Load() {
		return ErrPoolClosed
	}
	if !p.running.Load() {
		return ErrPoolNotRunning
	}

	// EventBus에 컨텍스트 지원 Enqueue가 있다면 권장
	if err := p.JobChan.Enqueue(j); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ErrEnqueueCanceled
		}
		return err
	}
	return nil
}

func (p *Pool) WithFailFast(b bool) *Pool { p.failFast = b; return p }
func (p *Pool) WithOwnsBus(b bool) *Pool  { p.ownsBus = b; return p }
func (p *Pool) Name() gface.ModuleName {
	return p.name
}
func (p *Pool) Start(parent context.Context) error {
	if !p.running.CompareAndSwap(false, true) {
		return fmt.Errorf("pool %s already running", p.name)
	}
	defer p.running.Store(false)

	if p.JobChan == nil {
		return fmt.Errorf("pool %s has no job bus", p.name)
	}

	g, ctx := errgroup.WithContext(parent)
	log.Printf("[pool %s] start (bus=%p workers=%d)", p.name, p.JobChan, p.NumWorkers)

	for i := 0; i < p.NumWorkers; i++ {
		id := i
		g.Go(func() error { return p.worker(ctx, id) })
	}

	// 항상 종료 정리 시도 (소유 시에만 실제 close)
	defer func() {
		_ = p.Close()
	}()

	err := g.Wait()
	if err != nil {
		log.Printf("[pool %s] stopped with error: %v", p.name, err)
		return err
	}
	log.Printf("[pool %s] stopped gracefully", p.name)
	return nil
}

func (p *Pool) worker(ctx context.Context, id int) (err error) {
	in := p.JobChan.Dequeue()
	log.Printf("[pool %s] worker#%d started; ch=%p", p.name, id, in)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in worker %d: %v", id, r)
			log.Printf("[pool %s] %v", p.name, err)
		}
		log.Printf("[pool %s] worker#%d exit (%v)", p.name, id, err)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-in:
			if !ok {
				return nil // 버스 종료
			}
			if job == nil {
				if p.failFast {
					return fmt.Errorf("nil job")
				}
				log.Printf("[pool %s] worker#%d: nil job (ignored)", p.name, id)
				continue
			}
			if jerr := job.Do(ctx); jerr != nil {
				log.Printf("[pool %s] worker#%d: job error: %v", p.name, id, jerr)
				if p.failFast {
					return jerr
				}
				// 계속 처리 모드면 다음 작업으로
			}
		}
	}
}
func (p *Pool) Closed() bool {
	return p.closed.Load()
}
func (p *Pool) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	if p.ownsBus && p.JobChan != nil {
		p.JobChan.Close()
	}
	log.Printf("[pool %s] closed (ownsBus=%v)", p.name, p.ownsBus)
	return nil
}

func (p *Pool) JobsLen() int {
	return p.JobChan.Len()
}
func (p *Pool) JobsCap() int {
	return p.JobChan.Cap()
}
