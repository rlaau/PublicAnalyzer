package domain

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/shopspring/decimal"
)

// From 주소 입장에서 본 To주소와의 거래 강도
type FromScala struct {
	FromAddress domain.Address       `json:"from_address"`
	LastTxId    domain.TxId          `json:"last_tx_id"`
	LastTime    chaintimer.ChainTime `json:"last_time"`
	Volume      int32                `json:"volum"`
	ValueOrNull decimal.Decimal      `json:"value,omitempty"` // ← 0이면 "0"로 저장됨
}
