package apool

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"golang.org/x/sync/errgroup"
)

type AnalyzerPool struct {
	isTest mode.ProcessingMode

	ports struct {
		rel cface.RelPort
		nod cface.NodPort
	}

	busRel *eventbus.EventBus[cface.RelMsg]
	busNod *eventbus.EventBus[cface.NodMsg]

	fanoutManager iface.ApoolFanoutManager
	closed        atomic.Bool
}

func NewAnalyzerPool(istest mode.ProcessingMode, fanoutManager iface.ApoolFanoutManager, nod cface.NodPort, rel cface.RelPort) *AnalyzerPool {
	capLimit := 128
	busNod := eventbus.NewInMemory[cface.NodMsg](capLimit)

	busRel := eventbus.NewInMemory[cface.RelMsg](capLimit)
	a := &AnalyzerPool{
		isTest: istest,
		busRel: busRel,
		busNod: busNod,
		ports: struct {
			rel cface.RelPort
			nod cface.NodPort
		}{
			rel: rel,
			nod: nod,
		},
		fanoutManager: fanoutManager,
	}
	return a
}
func CreateAnalzerPoolFrame(istest mode.ProcessingMode, fanoutManager iface.ApoolFanoutManager) (*AnalyzerPool, error) {
	capLimit := 128
	busNod := eventbus.NewInMemory[cface.NodMsg](capLimit)

	busRel := eventbus.NewInMemory[cface.RelMsg](capLimit)
	a := &AnalyzerPool{
		isTest:        istest,
		busNod:        busNod,
		busRel:        busRel,
		fanoutManager: fanoutManager,
	}
	return a, nil
}
func (a *AnalyzerPool) Name() gface.ModuleName {
	return "AnalyzerPool"
}
func (a *AnalyzerPool) Start(parent context.Context) error {
	if a.fanoutManager == nil {
		return fmt.Errorf("fanout manager not initialized")
	}
	if a.ports.nod == nil || a.ports.rel == nil {
		return fmt.Errorf("ports not registered")
	}

	g, ctx := errgroup.WithContext(parent)
	if a.ports.nod != nil {
		g.Go(func() error { return a.ports.nod.Start(ctx) })
	}
	if a.ports.rel != nil {
		g.Go(func() error { return a.ports.rel.Start(ctx) })
	}
	if a.fanoutManager != nil {
		g.Go(func() error { return a.fanoutManager.Start(ctx) })
	}
	if err := g.Wait(); err != nil {
		_ = a.Close()
		return fmt.Errorf("analyzer pool stopped with error: %w", err)
	}
	_ = a.Close()
	return nil
}

func (a *AnalyzerPool) Close() error {
	if a.closed.Swap(true) {
		return nil
	}
	if a.fanoutManager != nil {
		_ = a.fanoutManager.Close()
	}
	if a.busNod != nil {
		a.busNod.Close()
	}
	if a.busRel != nil {
		a.busRel.Close()
	}
	return nil
}
func (a *AnalyzerPool) Closed() bool {
	return a.closed.Load()
}

func (a *AnalyzerPool) GetRelPort() cface.RelPort { return a.ports.rel }
func (a *AnalyzerPool) GetNodPort() cface.NodPort { return a.ports.nod }

func (a *AnalyzerPool) EnqueueToRel(v cface.RelMsg) error {
	return a.busRel.Publish(v)
}
func (a *AnalyzerPool) EnqueueToNod(v cface.NodMsg) error {
	return a.busNod.Publish(v)
}

func (a *AnalyzerPool) DeququeRel() <-chan cface.RelMsg {
	return a.busRel.Dequeue()
}

func (a *AnalyzerPool) DeququeNod() <-chan cface.NodMsg {
	return a.busNod.Dequeue()
}

func (a *AnalyzerPool) ConsumeRelTxByFanout() <-chan domain.MarkedTransaction {
	if a.fanoutManager == nil {
		// 빈(닫힌) 채널을 돌려줘서 호출 측이 안전하게 select 탈출 가능
		ch := make(chan domain.MarkedTransaction)
		close(ch)
		return ch
	}
	// 이런 "문자열 인자" 전달은 나도 싫어함.
	// 그치만 모듈간 결함을 낮추려면 싫더라도 이렇게 문자열 인자 전달을 쓸 수밖에 없음.
	return a.fanoutManager.GetBus(string(iface.RelBusName)).Dequeue()
}

func (a *AnalyzerPool) ConsumeNodTxByFanout() <-chan domain.MarkedTransaction {
	if a.fanoutManager == nil {
		ch := make(chan domain.MarkedTransaction)
		close(ch)
		return ch
	}
	return a.fanoutManager.GetBus(string(iface.NodBusName)).Dequeue()
}

// TODO 추후 삭제. 포트 등록은 정식 생성자 통해서 할것!!!
func (a *AnalyzerPool) RegisterPorts(nod cface.NodPort, rel cface.RelPort) {
	if a.busNod != nil {
		a.busNod.Close()
	}
	if a.busRel != nil {
		a.busRel.Close()
	}
	a.ports.rel = rel
	a.ports.nod = nod
}

// TODO 추후 삭제.
func (a *AnalyzerPool) Register(rel cface.RelPort, nod cface.NodPort) {

	a.ports.rel = rel
	a.ports.nod = nod
}
