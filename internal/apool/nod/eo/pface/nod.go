package pface

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type NodPort interface {
	gface.Module
	ConsumeEoTxByFanout() <-chan domain.MarkedTransaction
}
