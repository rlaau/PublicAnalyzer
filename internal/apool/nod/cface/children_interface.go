package cface

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type CoPort interface {
	gface.Module
	gface.Child
	RegisterContract(creator domain.Address, nonce domain.Nonce, blockTime chaintimer.ChainTime, txId domain.TxId) (domain.Address, error)

	CheckIsContract(address domain.Address) bool
	//TODO 추후 지울 것!
	ChangeDBPath(target string, path string)
	GetContDB() *infra.ContractDB
}

type CoMsg any
type EoPort interface {
	gface.Module
	gface.Child
	SaveDetectedDeposit(deposit *domain.DetectedDeposit) error
	IsDepositAddress(addr domain.Address) (bool, error)
	IsCex(addr domain.Address) bool
	UpdateDepositTxCount(addr domain.Address, count int64) error
	GetDepositInfo(addr domain.Address) (*domain.DetectedDeposit, error)
	CEXAddresses() map[string]struct{}
}

type EoMsg any
