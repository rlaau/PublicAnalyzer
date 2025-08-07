package txingester

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"
)

// CCEService defines what txIngester needs from CCE module
// This follows Dependency Inversion Principle - consumer defines the interface
type CCEService interface {
	// RegisterContract registers a new contract creation
	// Takes creator address, nonce, and block time
	// Returns the calculated contract address
	RegisterContract(creator domain.Address, nonce uint64, blockTime chaintimer.ChainTime) (domain.Address, error)

	// CheckIsContract checks if the given address is a registered contract
	CheckIsContract(address domain.Address) bool
}
