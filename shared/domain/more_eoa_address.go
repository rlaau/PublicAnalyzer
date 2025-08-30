package domain

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"

// DetectedDeposit represents a deposit address that has been identified
type DetectedDeposit struct {
	Address    Address
	DetectedAt chaintimer.ChainTime
	CEXAddress Address // The CEX address this deposit was detected from
	TxCount    int64   // Number of transactions seen
}
