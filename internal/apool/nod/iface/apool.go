package iface

import (
	"context"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/sharedface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type ApoolPort interface {
	Register(rel sharedface.RelPort, nod sharedface.NodPort)
	GetNodPort() sharedface.NodPort
	EnqueueToNod(v sharedface.NodMsg) error
	DeququeRel() <-chan sharedface.RelMsg
	ConsumeRelTxByFanout() <-chan domain.MarkedTransaction
	Start(parent context.Context) error
	Close() error
}
