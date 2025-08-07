package infra

import (
	"fmt"
	"sync"

	localdomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"
)

// TODO: 추후 에 도메인 놀리지는 그라운드 놀리지에 편입 후 제거!
// GroundKnowledge contains all known facts about addresses
type GroundKnowledge struct {
	cexSet             *domain.CEXSet
	detectedDepositSet *localdomain.DetectedDepositSet
	depositRepository  DepositRepository
	mutex              sync.RWMutex
	loaded             bool // Track if knowledge has been loaded
}

// NewDomainKnowledge creates a new ground knowledge instance
func NewDomainKnowledge(cexSet *domain.CEXSet, depositRepo DepositRepository) *GroundKnowledge {
	return &GroundKnowledge{
		cexSet:             cexSet,
		detectedDepositSet: localdomain.NewDetectedDepositSet(),
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
	if existing, exists := gk.detectedDepositSet.Get(fromAddr); exists {
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
	deposit := &localdomain.DetectedDepositWithEvidence{
		Address:    fromAddr,
		DetectedAt: chaintimer.Now(),
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
func (gk *GroundKnowledge) GetDepositInfo(addr domain.Address) (*localdomain.DetectedDepositWithEvidence, bool) {
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
func (gk *GroundKnowledge) GetDetectedDeposits() []*localdomain.DetectedDepositWithEvidence {
	gk.mutex.RLock()
	defer gk.mutex.RUnlock()

	return gk.detectedDepositSet.GetAll()
}

// GetStats returns statistics about the ground knowledge
func (gk *GroundKnowledge) GetStats() map[string]any {
	gk.mutex.RLock()
	defer gk.mutex.RUnlock()

	return map[string]interface{}{
		"cex_addresses":     gk.cexSet.Size(),
		"detected_deposits": gk.detectedDepositSet.Size(),
		"loaded":            gk.loaded,
	}
}
