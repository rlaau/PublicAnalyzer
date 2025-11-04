package domain

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"

// DepositDetection represents a deposit address that has been identified
type DepositDetection struct {
	DepositAddress Address
	DetectedAt     chaintimer.ChainTime
	CEXAddress     Address // The CEX address this deposit was detected from
	TxCount        int64   // Number of transactions seen
}
