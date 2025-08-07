package app

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"
)

type CCEService interface {
	RegisterContract(creator domain.Address, nonce uint64, blockTime chaintimer.ChainTime) (domain.Address, error)
	CheckIsContract(address domain.Address) bool
	calculateContractAddress(creator domain.Address, nonce uint64) domain.Address
}
