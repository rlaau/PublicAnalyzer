package nod

import (
	"context"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

type NodPoolDesign interface {
	gface.Module
	GetCoPort() cface.CoPort
	GetEoPort() cface.EoPort
}

// TODO 추후 스트리밍 가능하게 만든 후 아래 시그니처 채우기!!!!
func (n *NodPool) Name() gface.ModuleName          { return gface.ParseModuleName("NodPool") }
func (n *NodPool) Start(ctx context.Context) error { panic("not implemented") }
func (n *NodPool) Close() error                    { panic("not implemented") }

type NodPool struct {
	isTest mode.ProcessingMode
	Apool  pface.ApoolPort

	ports struct {
		eo cface.EoPort
		co cface.CoPort
	}

	busEo *eventbus.EventBus[cface.EoMsg]
	busCo *eventbus.EventBus[cface.CoMsg]

	txDistributor *TxDistributor
	closed        atomic.Bool
}

// TODO 추후 마저 쓰기!
// TODO 생성자 초기화 후, NodPool꽉 채우기
func (n *NodPool) GetCoPort() cface.CoPort {
	return n.ports.co
}
func (n *NodPool) GetEoPort() cface.EoPort {
	return n.ports.eo
}

// TODO 수직 슬라이스 테스트를 위해 어쩔 수 없이 만든 함수임.
// TODO 추후 정식 생성자 통해서 모든 것을 제대로 초기화 할 것!!
func (n *NodPool) SetCoPort(c cface.CoPort) {
	n.ports.co = c
}

func (n *NodPool) SetEoPort(e cface.EoPort) {
	n.ports.eo = e
}
