package app

import (
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type CCEService interface {
	RegisterContract(creator domain.Address, nonce uint64, blockTime time.Time) (domain.Address, error)
	CheckIsContract(address domain.Address) bool
	calculateContractAddress(creator domain.Address, nonce uint64) domain.Address
}
