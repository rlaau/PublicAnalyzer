package domain

import (
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/ct"
)

// DetectedDepositAddress represents a deposit address that has been identified
type DetectedDepositAddress struct {
	Address    domain.Address
	DetectedAt ct.ChainTime
	CEXAddress domain.Address // The CEX address this deposit was detected from
	TxCount    int64          // Number of transactions seen
}

// DetectedDepositSet manages a collection of detected deposit addresses in memory
type DetectedDepositSet struct {
	addresses map[string]*DetectedDepositAddress
	mutex     sync.RWMutex
}

// NewDetectedDepositSet creates a new detected deposit address set
func NewDetectedDepositSet() *DetectedDepositSet {
	return &DetectedDepositSet{
		addresses: make(map[string]*DetectedDepositAddress),
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
	s.addresses[addrStr] = &DetectedDepositAddress{
		Address:    addr,
		DetectedAt: ct.Now(),
		CEXAddress: cexAddr,
		TxCount:    1,
	}
}

// LoadFromPersisted loads detected deposits from persistent storage
func (s *DetectedDepositSet) LoadFromPersisted(deposits []*DetectedDepositAddress) {
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
func (s *DetectedDepositSet) Get(addr domain.Address) (*DetectedDepositAddress, bool) {
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
func (s *DetectedDepositSet) GetAll() []*DetectedDepositAddress {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	result := make([]*DetectedDepositAddress, 0, len(s.addresses))
	for _, deposit := range s.addresses {
		result = append(result, deposit)
	}
	return result
}
