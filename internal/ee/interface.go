package ee

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"

// CEXRepository defines the interface for CEX address data access
type CEXRepository interface {
	// LoadCEXSet loads all CEX addresses from storage into a set
	LoadCEXSet() (*domain.CEXSet, error)

	// AddCEXAddress adds a new CEX address to storage
	AddCEXAddress(address string) error

	// RemoveCEXAddress removes a CEX address from storage
	RemoveCEXAddress(address string) error

	// Reload reloads CEX addresses from storage
	Reload() error
}
