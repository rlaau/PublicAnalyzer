package sharedface

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type CoPort interface {
	RegisterContract(creator domain.Address, nonce domain.Nonce, blockTime chaintimer.ChainTime, txId domain.TxId) (domain.Address, error)
	SaveDetectedDeposit(deposit *domain.DetectedDeposit) error
	IsDepositAddress(addr domain.Address) (bool, error)
	IsCex(addr domain.Address) bool
	UpdateDepositTxCount(addr domain.Address, count int64) error
	GetDepositInfo(addr domain.Address) (*domain.DetectedDeposit, error)
	CheckIsContract(address domain.Address) bool
	CEXAddresses() map[string]struct{}
	//TODO 추후 지울 것!
	ChangeDBPath(target string, path string)
}

type CoMsg any
type EoPort interface {
}

type EoMsg any
