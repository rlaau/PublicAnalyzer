package app

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/fanout"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"golang.org/x/sync/errgroup"
)

type NodPoolDesign interface {
	gface.Module
	GetCoPort() cface.CoPort
	GetEoPort() cface.EoPort
}
type NodPool struct {
	isTest mode.ProcessingMode
	Apool  pface.ApoolPort

	ports struct {
		eo cface.EoPort
		co cface.CoPort
	}

	busEo *eventbus.EventBus[cface.EoMsg]
	busCo *eventbus.EventBus[cface.CoMsg]
	//TODO 추후 iface, infra구분해서 리팩토링 하기. Apool의 사례 참고.

	txDistributor iface.NodPoolFanoutManager
	closed        atomic.Bool
}

func NewNodPool(istest mode.ProcessingMode, fanoutMnager *fanout.FanoutInMemory[domain.MarkedTransaction],
	eo cface.EoPort, co cface.CoPort) *NodPool {
	capLimit := 128
	busEo := eventbus.NewInMemory[cface.EoMsg](capLimit, "EoEventBus")
	busCo := eventbus.NewInMemory[cface.CoMsg](capLimit, "CoEventBus")

	n := &NodPool{
		isTest:        istest,
		busEo:         busEo,
		busCo:         busCo,
		txDistributor: fanoutMnager,
		ports: struct {
			eo cface.EoPort
			co cface.CoPort
		}{
			eo: eo,
			co: co,
		},
	}
	// 중요. 트리 구조에선 반드시 부모가 자식을 책임지고 레지스터 해야함
	eo.RegisterParent(n)
	co.RegisterParent(n)
	return n
}
func (n *NodPool) RegisterParent(apool gface.Module) error {
	ap, ok := apool.(pface.ApoolPort)
	if !ok {
		fmt.Printf("RegisterParent: nodPool이 받은 apool의 ApoolPort의 런타임 타입체크 실패함\n")
		return fmt.Errorf("NodPool이 받은 Apool이 ApoolPort조건을 만족하지 못함")
	}
	n.Apool = ap
	return nil
}
func (n *NodPool) Name() gface.ModuleName { return gface.ParseModuleName("NodPool") }
func (n *NodPool) Start(parent context.Context) error {
	if n.txDistributor == nil {
		return fmt.Errorf("Nodpool fanout manager not initialized")
	}

	if n.ports.co == nil || n.ports.eo == nil {
		return fmt.Errorf("Nodpool의 모듈 중 일부가 초기화되지 않음.")
	}
	g, ctx := errgroup.WithContext(parent)
	// 분배기로 분배
	g.Go(func() error { return n.ports.co.Start(ctx) })
	g.Go(func() error { return n.ports.eo.Start(ctx) })
	g.Go(func() error { return n.txDistributor.Start(ctx) })
	//분배기에 먹일 tx를 위에서 받아옴
	g.Go(func() error {
		upstream := n.Apool.ConsumeNodTxByFanout()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-upstream:
				if !ok {
					return nil
				}
				if err := n.txDistributor.Publish(v); err != nil {
					return fmt.Errorf("publish failed: %w", err)
				}
			}
		}
	})

	err := g.Wait()
	n.Close()
	return err
}

func (n *NodPool) Close() error {
	if n.closed.Swap(true) {
		return nil
	}
	if n.txDistributor != nil {
		n.txDistributor.Close()
	}
	if n.busCo != nil {
		n.busCo.Close()
	}
	if n.busEo != nil {
		n.busEo.Close()
	}
	return nil
}
func (n *NodPool) Closed() bool { return n.closed.Load() }

func (n *NodPool) GetCoPort() cface.CoPort { return n.ports.co }
func (n *NodPool) GetEoPort() cface.EoPort { return n.ports.eo }

func (n *NodPool) ConsumeCoTxByFanout() <-chan domain.MarkedTransaction {
	return n.txDistributor.GetBus(string(iface.CoBusName)).Dequeue()
}
func (n *NodPool) ConsumeEoTxByFanout() <-chan domain.MarkedTransaction {
	return n.txDistributor.GetBus(string(iface.EoBusName)).Dequeue()
}

// TODO 수직 슬라이스 테스트를 위해 어쩔 수 없이 만든 함수임.
// TODO 추후 정식 생성자 통해서 모든 것을 제대로 초기화 할 것!!
func (n *NodPool) SetCoPort(c cface.CoPort) {
	n.ports.co = c
}

func (n *NodPool) SetEoPort(e cface.EoPort) {
	n.ports.eo = e
}
