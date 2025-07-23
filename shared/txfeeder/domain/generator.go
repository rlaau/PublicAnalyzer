package domain

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// MockDepositAddressSet represents a set of mocked deposit addresses
type MockDepositAddressSet struct {
	addresses []domain.Address
}

// NewMockDepositAddressSet creates a new mock deposit address set
func NewMockDepositAddressSet() *MockDepositAddressSet {
	return &MockDepositAddressSet{
		addresses: make([]domain.Address, 0),
	}
}

// LoadFromFile loads mock deposit addresses from file
func (s *MockDepositAddressSet) LoadFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		addr, err := s.parseAddress(line)
		if err != nil {
			continue // Skip invalid addresses
		}

		s.addresses = append(s.addresses, addr)
	}

	return scanner.Err()
}

// GetRandomAddress returns a random address from the set
func (s *MockDepositAddressSet) GetRandomAddress() domain.Address {
	if len(s.addresses) == 0 {
		return domain.Address{} // Return zero address if empty
	}

	idx := s.randomInt(len(s.addresses))
	return s.addresses[idx]
}

// Size returns the number of addresses in the set
func (s *MockDepositAddressSet) Size() int {
	return len(s.addresses)
}

// parseAddress converts hex string to Address type
func (s *MockDepositAddressSet) parseAddress(hexStr string) (domain.Address, error) {
	var addr domain.Address

	// Remove 0x prefix if present
	if strings.HasPrefix(hexStr, "0x") {
		hexStr = hexStr[2:]
	}

	if len(hexStr) != 40 {
		return addr, fmt.Errorf("invalid address length: %d", len(hexStr))
	}

	// Convert hex string to bytes
	for i := 0; i < 20; i++ {
		var b byte
		_, err := fmt.Sscanf(hexStr[i*2:i*2+2], "%02x", &b)
		if err != nil {
			return addr, err
		}
		addr[i] = b
	}

	return addr, nil
}

// randomInt returns a random integer in range [0, max)
func (s *MockDepositAddressSet) randomInt(max int) int {
	if max <= 0 {
		return 0
	}

	b := make([]byte, 4)
	rand.Read(b)

	val := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	if val < 0 {
		val = -val
	}

	return val % max
}

// TxGeneratorConfig holds configuration for transaction generation
type TxGeneratorConfig struct {
	TotalTransactions            int           // 400,000,000 total transactions
	TransactionsPerSecond        int           // 1,000,000 transactions per second
	TransactionsPerTimeIncrement int           // 15 transactions per time increment
	TimeIncrementDuration        time.Duration // 1 second per 15 transactions
	StartTime                    time.Time     // 2025-01-01 as base time

	// Transaction pattern ratios
	DepositToCexRatio    int // 1 in 50 (2%)
	RandomToDepositRatio int // 1 in 20 (5%)
}

// NewDefaultTxGeneratorConfig creates default configuration
func NewDefaultTxGeneratorConfig() *TxGeneratorConfig {
	startTime, _ := time.Parse("2006-01-02", "2025-01-01")

	return &TxGeneratorConfig{
		TotalTransactions:            400_000_000,
		TransactionsPerSecond:        1_000_000,
		TransactionsPerTimeIncrement: 1,
		TimeIncrementDuration:        time.Second,
		StartTime:                    startTime,
		DepositToCexRatio:            50,
		RandomToDepositRatio:         20,
	}
}

// TxGeneratorState holds the current state of the generator
type TxGeneratorState struct {
	GeneratedCount              int64
	CurrentTime                 time.Time
	TxCounter                   int // Counter for time increments
	timeIncrement               time.Duration
	transactionPerTimeIncrement int
}

// NewTxGeneratorState creates initial generator state
func NewTxGeneratorState(startTime time.Time, timeIncrement time.Duration, transactionPerTimeIncrement int) *TxGeneratorState {
	return &TxGeneratorState{
		GeneratedCount:              0,
		CurrentTime:                 startTime,
		TxCounter:                   0,
		timeIncrement:               timeIncrement,
		transactionPerTimeIncrement: transactionPerTimeIncrement,
	}
}

// IncrementTransaction updates state for next transaction
func (s *TxGeneratorState) IncrementTransaction() {
	s.GeneratedCount++
	s.TxCounter++

	// transactionPerTimeIncrement마다 timeIncrement만큼 시간 지난 것으로 측정
	if s.TxCounter%s.transactionPerTimeIncrement == 0 {
		s.CurrentTime = s.CurrentTime.Add(s.timeIncrement)
	}
}

// GenerateRandomAddress generates a random Ethereum address
func GenerateRandomAddress() domain.Address {
	var addr domain.Address
	rand.Read(addr[:])
	return addr
}

// GenerateRandomTxID generates a random transaction ID
func GenerateRandomTxID() domain.TxId {
	var txID domain.TxId
	rand.Read(txID[:])
	return txID
}
