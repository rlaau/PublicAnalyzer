package domain

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// DetectedDepositWithEvidence represents a deposit address that has been identified
type DetectedDepositWithEvidence struct {
	Address    domain.Address
	DetectedAt chaintimer.ChainTime
	CEXAddress domain.Address // The CEX address this deposit was detected from
	TxCount    int64          // Number of transactions seen
}
