package domain

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// DetectedDepositAddress represents a deposit address that has been identified
type DetectedDepositAddress struct {
	Address    domain.Address
	DetectedAt time.Time
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
		DetectedAt: time.Now(),
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

// DepositRepository defines the interface for deposit address persistence
type DepositRepository interface {
	// SaveDetectedDeposit saves a detected deposit address to persistent storage
	SaveDetectedDeposit(deposit *DetectedDepositAddress) error

	// LoadDetectedDeposits loads all detected deposit addresses from storage
	LoadDetectedDeposits() ([]*DetectedDepositAddress, error)

	// IsDepositAddress checks if an address is a known deposit address
	IsDepositAddress(addr domain.Address) (bool, error)

	// GetDepositInfo retrieves deposit address information
	GetDepositInfo(addr domain.Address) (*DetectedDepositAddress, error)

	// UpdateTxCount updates the transaction count for a deposit address
	UpdateTxCount(addr domain.Address, count int64) error
}

// GroundKnowledge contains all known facts about addresses
type GroundKnowledge struct {
	cexSet             *domain.CEXSet
	detectedDepositSet *DetectedDepositSet
	depositRepository  DepositRepository
	mutex              sync.RWMutex
	loaded             bool // Track if knowledge has been loaded
}

// NewGroundKnowledge creates a new ground knowledge instance
func NewGroundKnowledge(cexSet *domain.CEXSet, depositRepo DepositRepository) *GroundKnowledge {
	return &GroundKnowledge{
		cexSet:             cexSet,
		detectedDepositSet: NewDetectedDepositSet(),
		depositRepository:  depositRepo,
		loaded:             false,
	}
}

// Load loads all existing knowledge from persistent storage
// This should be called once at startup to restore previous state
func (gk *GroundKnowledge) Load() error {
	gk.mutex.Lock()
	defer gk.mutex.Unlock()

	if gk.loaded {
		return nil // Already loaded
	}

	// Load detected deposits from persistent storage
	deposits, err := gk.depositRepository.LoadDetectedDeposits()
	if err != nil {
		return fmt.Errorf("failed to load detected deposits: %w", err)
	}

	// Load them into in-memory set
	gk.detectedDepositSet.LoadFromPersisted(deposits)
	gk.loaded = true

	return nil
}

// IsCEXAddress checks if an address is a known CEX address
func (gk *GroundKnowledge) IsCEXAddress(addr domain.Address) bool {
	gk.mutex.RLock()
	defer gk.mutex.RUnlock()

	return gk.cexSet.Contains(addr.String())
}

// IsDepositAddress checks if an address is a detected deposit address
func (gk *GroundKnowledge) IsDepositAddress(addr domain.Address) bool {
	gk.mutex.RLock()
	defer gk.mutex.RUnlock()

	return gk.detectedDepositSet.Contains(addr)
}

// DetectNewDepositAddress detects and adds a new deposit address
// This is called when we see: someAddress -> CEXAddress transaction
func (gk *GroundKnowledge) DetectNewDepositAddress(fromAddr, cexAddr domain.Address) error {
	fmt.Printf("   🔍 DetectNewDepositAddress: %s → CEX %s\n", fromAddr.String()[:10]+"...", cexAddr.String()[:10]+"...")

	gk.mutex.Lock()
	defer gk.mutex.Unlock()

	// Check if already exists (incremental detection)
	if existing, exists := gk.detectedDepositSet.addresses[fromAddr.String()]; exists {
		// Just increment count for existing deposit
		existing.TxCount++
		fmt.Printf("   📈 Existing deposit, updating count to %d\n", existing.TxCount)
		return gk.depositRepository.UpdateTxCount(fromAddr, existing.TxCount)
	}

	fmt.Printf("   ✨ New deposit address detected\n")

	// Add new detection to in-memory set
	gk.detectedDepositSet.Add(fromAddr, cexAddr)
	fmt.Printf("   💾 Added to in-memory set (size: %d)\n", gk.detectedDepositSet.Size())

	// Persist new detection to storage
	deposit := &DetectedDepositAddress{
		Address:    fromAddr,
		DetectedAt: time.Now(),
		CEXAddress: cexAddr,
		TxCount:    1,
	}

	fmt.Printf("   💽 Saving to persistent storage...\n")
	err := gk.depositRepository.SaveDetectedDeposit(deposit)
	if err != nil {
		fmt.Printf("   ❌ SaveDetectedDeposit failed: %v\n", err)
		return err
	}
	fmt.Printf("   ✅ SaveDetectedDeposit succeeded\n")

	return nil
}

// GetDepositInfo retrieves information about a deposit address
func (gk *GroundKnowledge) GetDepositInfo(addr domain.Address) (*DetectedDepositAddress, bool) {
	gk.mutex.RLock()
	defer gk.mutex.RUnlock()

	return gk.detectedDepositSet.Get(addr)
}

// GetCEXAddresses returns all known CEX addresses
func (gk *GroundKnowledge) GetCEXAddresses() []string {
	gk.mutex.RLock()
	defer gk.mutex.RUnlock()

	return gk.cexSet.GetAll()
}

// GetDetectedDeposits returns all detected deposit addresses
func (gk *GroundKnowledge) GetDetectedDeposits() []*DetectedDepositAddress {
	gk.mutex.RLock()
	defer gk.mutex.RUnlock()

	return gk.detectedDepositSet.GetAll()
}

// GetStats returns statistics about the ground knowledge
func (gk *GroundKnowledge) GetStats() map[string]interface{} {
	gk.mutex.RLock()
	defer gk.mutex.RUnlock()

	return map[string]interface{}{
		"cex_addresses":     gk.cexSet.Size(),
		"detected_deposits": gk.detectedDepositSet.Size(),
		"loaded":            gk.loaded,
	}
}

// parseAddressFromString converts hex string to Address
func parseAddressFromString(hexStr string) (domain.Address, error) {
	var addr domain.Address

	// Remove 0x prefix if present
	if strings.HasPrefix(hexStr, "0x") {
		hexStr = hexStr[2:]
	}

	if len(hexStr) != 40 {
		return addr, fmt.Errorf("invalid address length: %d", len(hexStr))
	}

	// Convert hex string to bytes
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return addr, fmt.Errorf("invalid hex string: %w", err)
	}

	copy(addr[:], bytes)
	return addr, nil
}
