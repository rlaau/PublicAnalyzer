package pface

import (
	"context"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type ApoolPort interface {
	Register(rel cface.RelPort, nod cface.NodPort)
	GetNodPort() cface.NodPort
	EnqueueToNod(v cface.NodMsg) error
	DeququeRel() <-chan cface.RelMsg
	ConsumeRelTxByFanout() <-chan domain.MarkedTransaction
	Start(parent context.Context) error
	Close() error
}
