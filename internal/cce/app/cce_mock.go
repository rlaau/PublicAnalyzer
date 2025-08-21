package app

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// MockCCEService provides a temporary mock implementation of CCEService for testing
// TODO This is NOT the real CCE service. The real CCE should use DB + Bloom Filter
// TODO 근데 사실상 그 차이 뿐이라 블룸필터_DB만 쓰게하면 바로 완성 가능
// This is just for rapid development and TODO removal from TestingIngester
type MockCCEService struct {
	mu        sync.RWMutex
	contracts map[domain.Address]domain.Address // contract -> creator mapping
}

// NewMockCCEService creates a new mock CCE service
// Returns an implementation that satisfies txingester.CCEService interface
func NewMockCCEService() *MockCCEService {
	return &MockCCEService{
		contracts: make(map[domain.Address]domain.Address),
	}
}

// RegisterContract registers a new contract creation
// TODO 현재 RegisterContract는 creater는 잡지만, owner는 잡지 않음. 추가적인 로직 통해, creator와 owner를 모두 잡아야 함
// TODO 당연하지만, 프로덕션 시엔 해당 데이터는 badger, 혹은 bolt에 쓰기. 벤치마크는 S_A_Benchmark보고 판단
// Calculates contract address from creator+nonce and stores the mapping
func (m *MockCCEService) RegisterContract(creator domain.Address, nonce uint64, blockTime chaintimer.ChainTime) (domain.Address, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Calculate contract address using creator + nonce (simplified version)
	contractAddr := m.calculateContractAddress(creator, nonce)

	// Store the mapping
	m.contracts[contractAddr] = creator

	//log.Printf("[MockCCEService] Registered contract %s with creator %s (nonce: %d)",		contractAddr.String(), creator.String(), nonce)

	return contractAddr, nil
}

// CheckIsContract checks if the given address is a registered contract
func (m *MockCCEService) CheckIsContract(address domain.Address) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.contracts[address]
	return exists
}

// calculateContractAddress computes contract address from creator address and nonce
// TODO This is a simplified version for testing. Real implementation would use RLP encoding
func (m *MockCCEService) calculateContractAddress(creator domain.Address, nonce uint64) domain.Address {
	// Simple hash-based calculation for testing
	hasher := sha256.New()
	hasher.Write(creator[:])

	nonceBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nonceBytes, nonce)
	hasher.Write(nonceBytes)

	hash := hasher.Sum(nil)

	var contractAddr domain.Address
	copy(contractAddr[:], hash[:20])

	return contractAddr
}
