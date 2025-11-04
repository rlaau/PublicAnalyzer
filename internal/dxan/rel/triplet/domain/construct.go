package domain

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
)

// NewFromScala creates a FromScala with initial values
func NewFromScala(from domain.Address, txId domain.TxId, txTime chaintimer.ChainTime, valueOrNull fp.Option[domain.Value]) FromScala {
	volume := int32(1)
	return FromScala{
		FromAddress: from,
		LastTxId:    txId,
		LastTime:    txTime,
		Volume:      volume,
		ValueOrNull: valueOrNull,
	}
}
