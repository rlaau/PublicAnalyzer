package domain

import (
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// DetectedDepositWithEvidence represents a deposit address that has been identified
type DetectedDepositWithEvidence struct {
	Address    domain.Address
	DetectedAt chaintimer.ChainTime
	CEXAddress domain.Address // The CEX address this deposit was detected from
	TxCount    int64          // Number of transactions seen
}

// DetectedDepositSet manages a collection of detected deposit addresses in memory
type DetectedDepositSet struct {
	addresses map[string]*DetectedDepositWithEvidence
	mutex     sync.RWMutex
}

// NewDetectedDepositSet creates a new detected deposit address set
func NewDetectedDepositSet() *DetectedDepositSet {
	return &DetectedDepositSet{
		addresses: make(map[string]*DetectedDepositWithEvidence),
	}
}

// Add adds a newly detected deposit address
func (s *DetectedDepositSet) Add(addr domain.Address, cexAddr domain.Address) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	addrStr := addr.String()
	if existing, exists := s.addresses[addrStr]; exists {
		// Update existing
		existing.TxCount++
		return
	}

	// Add new detected deposit address
	s.addresses[addrStr] = &DetectedDepositWithEvidence{
		Address: addr,
		//DetectedAt: chaintimer.Now(),
		CEXAddress: cexAddr,
		TxCount:    1,
	}
}

// LoadFromPersisted loads detected deposits from persistent storage
func (s *DetectedDepositSet) LoadFromPersisted(deposits []*DetectedDepositWithEvidence) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, deposit := range deposits {
		s.addresses[deposit.Address.String()] = deposit
	}
}

// Contains checks if an address is a detected deposit address
func (s *DetectedDepositSet) Contains(addr domain.Address) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	_, exists := s.addresses[addr.String()]
	return exists
}

// Get retrieves a detected deposit address info
func (s *DetectedDepositSet) Get(addr domain.Address) (*DetectedDepositWithEvidence, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	deposit, exists := s.addresses[addr.String()]
	return deposit, exists
}

// Size returns the number of detected deposit addresses
func (s *DetectedDepositSet) Size() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return len(s.addresses)
}

// GetAll returns all detected deposit addresses
func (s *DetectedDepositSet) GetAll() []*DetectedDepositWithEvidence {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	result := make([]*DetectedDepositWithEvidence, 0, len(s.addresses))
	for _, deposit := range s.addresses {
		result = append(result, deposit)
	}
	return result
}
