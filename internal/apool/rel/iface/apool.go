package iface

import (
	"context"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type ApoolPort interface {
	Register(rel iface.RelPort, nod iface.NodPort)
	GetNodPort() iface.NodPort
	EnqueueToNod(v iface.NodMsg) error
	DeququeRel() <-chan iface.RelMsg
	ConsumeRelTxByFanout() <-chan domain.MarkedTransaction
	Start(parent context.Context) error
	Close() error
}
