package pface

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type ApoolPort interface {
	gface.Module
	Register(rel cface.RelPort, nod cface.NodPort)
	GetNodPort() cface.NodPort
	EnqueueToNod(v cface.NodMsg) error
	DeququeRel() <-chan cface.RelMsg
	ConsumeRelTxByFanout() <-chan domain.MarkedTransaction
	ConsumeNodTxByFanout() <-chan domain.MarkedTransaction
}
