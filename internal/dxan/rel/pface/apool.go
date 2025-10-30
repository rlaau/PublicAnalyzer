package pface

import (
	"context"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type DxanPort interface {
	gface.Module
	Register(rel cface.RelPort, nod cface.NodPort)
	GetNodPort() cface.NodPort
	EnqueueToNod(ctx context.Context, v cface.NodMsg) error
	DeququeRel() <-chan cface.RelMsg
	ConsumeRelTxByFanout() <-chan domain.MarkedTransaction
}
