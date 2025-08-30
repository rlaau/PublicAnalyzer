package nod

import (
	"context"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/sharedface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// TODO 추후 일반화하기
type NodPool struct {
	isTest mode.ProcessingMode
	Apool  iface.ApoolPort

	ports struct {
		eo sharedface.EoPort
		co sharedface.CoPort
	}

	busEo *eventbus.EventBus[sharedface.EoMsg]
	busCo *eventbus.EventBus[sharedface.CoMsg]

	txDistributor *TxDistributor
	closed        atomic.Bool
}

// TODO 추후 마저 쓰기!
// TODO 생성자 초기화 후, NodPool꽉 채우기
func (n *NodPool) GetCoPort() sharedface.CoPort {
	return n.ports.co
}

func (n *NodPool) Start(ctx context.Context) error {
	panic("구현 안함 아직")
}
func (n *NodPool) Close() {
	panic("아직 미구현")
}
