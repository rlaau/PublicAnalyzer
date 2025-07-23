package app

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	sharedDomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/domain"
)

// TxGenerator generates transactions for testing EE module
type TxGenerator struct {
	config           *domain.TxGeneratorConfig
	state            *domain.TxGeneratorState
	mockDepositAddrs *domain.MockDepositAddressSet
	cexSet           *domain.CEXSet

	// Channels for transaction output
	txChannel chan sharedDomain.MarkedTransaction

	// Control channels
	stopChannel chan struct{}
	doneChannel chan struct{}

	// Synchronization
	mutex    sync.RWMutex
	stopOnce sync.Once // 중복 stop 방지
}

// NewTxGenerator creates a new transaction generator
func NewTxGenerator(config *domain.TxGeneratorConfig, cexSet *domain.CEXSet) *TxGenerator {
	return &TxGenerator{
		config:           config,
		state:            domain.NewTxGeneratorState(config.StartTime, config.TimeIncrementDuration, config.TransactionsPerTimeIncrement),
		mockDepositAddrs: domain.NewMockDepositAddressSet(),
		cexSet:           cexSet,
		txChannel:        make(chan sharedDomain.MarkedTransaction, 100_000), // Buffer for 10k transactions
		stopChannel:      make(chan struct{}),
		doneChannel:      make(chan struct{}),
	}
}

// LoadMockDepositAddresses loads mock deposit addresses from file
func (g *TxGenerator) LoadMockDepositAddresses(filePath string) error {
	return g.mockDepositAddrs.LoadFromFile(filePath)
}

// Start begins transaction generation
func (g *TxGenerator) Start(ctx context.Context) error {
	go g.generateTransactions(ctx)
	return nil
}

// Stop stops transaction generation (safe for multiple calls)
func (g *TxGenerator) Stop() {
	g.stopOnce.Do(func() {
		close(g.stopChannel)
	})
	<-g.doneChannel
}

// GetTxChannel returns the channel for receiving generated transactions
func (g *TxGenerator) GetTxChannel() <-chan sharedDomain.MarkedTransaction {
	return g.txChannel
}

// GetGeneratedCount returns the current count of generated transactions
func (g *TxGenerator) GetGeneratedCount() int64 {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	return g.state.GeneratedCount
}

// generateTransactions is the main generation loop running in goroutine
func (g *TxGenerator) generateTransactions(ctx context.Context) {
	defer close(g.doneChannel)
	defer close(g.txChannel)
	fmt.Printf("Starting transaction generation: %d total transactions at %d tx/sec\n",
		g.config.TotalTransactions, g.config.TransactionsPerSecond)
	// Calculate dynamic interval based on config.TPS
	interval := time.Second / time.Duration(g.config.TransactionsPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Transaction generation stopped by context")
			return
		case <-g.stopChannel:
			fmt.Println("Transaction generation stopped by stop signal")
			return
		case <-ticker.C:
			// Check if we've generated enough transactions
			g.mutex.RLock()
			currentCount := g.state.GeneratedCount
			g.mutex.RUnlock()

			if currentCount >= int64(g.config.TotalTransactions) {
				fmt.Printf("Generated all %d transactions. Stopping.\n", g.config.TotalTransactions)
				return
			}

			// Generate and send transaction
			tx := g.generateSingleTransaction()

			select {
			case g.txChannel <- tx:
				// Transaction sent successfully
			case <-ctx.Done():
				return
			case <-g.stopChannel:
				return
			}
		}
	}
}

// generateSingleTransaction generates a single MarkedTransaction
func (g *TxGenerator) generateSingleTransaction() sharedDomain.MarkedTransaction {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	// Determine transaction type based on patterns
	txType := g.determineTransactionType()

	var tx sharedDomain.MarkedTransaction

	switch txType {
	case DepositToCexTx:
		tx = g.generateDepositToCexTransaction()
		// 처음 5개 특별 케이스는 로깅
		if g.state.GeneratedCount < 5 && txType == DepositToCexTx {
			fmt.Printf("   ✨ Generated Deposit→CEX: From=%s → To=%s\n",
				tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
		}
	case RandomToDepositTx:
		tx = g.generateRandomToDepositTransaction()
		if g.state.GeneratedCount < 5 && txType == RandomToDepositTx {
			fmt.Printf("   ✨ Generated Random→Deposit: From=%s → To=%s\n",
				tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
		}
	default:
		tx = g.generateRandomTransaction()
	}

	// Update state
	g.state.IncrementTransaction()

	return tx
}

type TransactionType int

const (
	RandomTx          TransactionType = iota
	DepositToCexTx                    // mockedAndHiddenDepositAddress -> CEX
	RandomToDepositTx                 // random address -> mockedAndHiddenDepositAddress
)

func (t TransactionType) String() string {
	switch t {
	case RandomTx:
		return "Random"
	case DepositToCexTx:
		return "Deposit→CEX"
	case RandomToDepositTx:
		return "Random→Deposit"
	default:
		return "Unknown"
	}
}

// determineTransactionType determines what type of transaction to generate
func (g *TxGenerator) determineTransactionType() TransactionType {
	count := int(g.state.GeneratedCount)

	// 디버깅: 처음 10개 트랜잭션의 타입 결정 과정 로깅
	var txType TransactionType
	var reason string

	// 1 in 5 chance for DepositToCex transaction
	if count%g.config.DepositToCexRatio == 0 {
		txType = DepositToCexTx
		reason = fmt.Sprintf("count=%d %% %d == 0", count, g.config.DepositToCexRatio)
	} else if count%g.config.RandomToDepositRatio == 0 {
		// 1 in 8 chance for RandomToDeposit transaction
		txType = RandomToDepositTx
		reason = fmt.Sprintf("count=%d %% %d == 0", count, g.config.RandomToDepositRatio)
	} else {
		txType = RandomTx
		reason = fmt.Sprintf("count=%d, random", count)
	}

	// 처음 10개는 디버깅 출력
	if count < 10 {
		fmt.Printf("   🎲 TX #%d: %v (%s)\n", count, txType, reason)
	}

	return txType
}

// generateDepositToCexTransaction generates mockedDepositAddress -> CEX transaction
func (g *TxGenerator) generateDepositToCexTransaction() sharedDomain.MarkedTransaction {
	fromAddr := g.mockDepositAddrs.GetRandomAddress()
	toAddr := g.getRandomCexAddress()

	return g.createMarkedTransaction(fromAddr, toAddr)
}

// generateRandomToDepositTransaction generates random -> mockedDepositAddress transaction
func (g *TxGenerator) generateRandomToDepositTransaction() sharedDomain.MarkedTransaction {
	fromAddr := domain.GenerateRandomAddress()
	toAddr := g.mockDepositAddrs.GetRandomAddress()

	return g.createMarkedTransaction(fromAddr, toAddr)
}

// generateRandomTransaction generates a completely random transaction
func (g *TxGenerator) generateRandomTransaction() sharedDomain.MarkedTransaction {
	fromAddr := domain.GenerateRandomAddress()
	toAddr := domain.GenerateRandomAddress()

	return g.createMarkedTransaction(fromAddr, toAddr)
}

// createMarkedTransaction creates a MarkedTransaction with given from/to addresses
func (g *TxGenerator) createMarkedTransaction(from, to sharedDomain.Address) sharedDomain.MarkedTransaction {
	txID := domain.GenerateRandomTxID()

	// Generate random value (0.1 to 10 ETH in wei)
	minWei := new(big.Int)
	minWei.SetString("100000000000000000", 10) // 0.1 ETH in wei
	maxWei := new(big.Int)
	maxWei.SetString("10000000000000000000", 10) // 10 ETH in wei
	diff := new(big.Int).Sub(maxWei, minWei)

	// Simple random generation for value
	randomBytes := make([]byte, 8)
	for i := range randomBytes {
		randomBytes[i] = byte(g.state.GeneratedCount >> (i * 8))
	}
	randomValue := new(big.Int).SetBytes(randomBytes)
	randomValue.Mod(randomValue, diff)
	randomValue.Add(randomValue, minWei)

	return sharedDomain.MarkedTransaction{
		BlockTime:   g.state.CurrentTime,
		TxID:        txID,
		TxSyntax:    [2]sharedDomain.ContractBoolMark{sharedDomain.EOAMark, sharedDomain.EOAMark}, // Assume EOA-to-EOA for simplicity
		Nonce:       uint64(g.state.GeneratedCount),
		BlockNumber: sharedDomain.BlockNumber(g.state.GeneratedCount / 100), // Rough block number
		From:        from,
		To:          to,
		Value:       sharedDomain.BigInt{Int: randomValue},
		GasLimit:    sharedDomain.BigInt{Int: big.NewInt(21000)}, // Standard ETH transfer gas
		Input:       "",                                          // Empty for ETH transfers
	}
}

// getRandomCexAddress returns a random CEX address from the loaded set
func (g *TxGenerator) getRandomCexAddress() sharedDomain.Address {
	addresses := g.cexSet.GetAll()
	if len(addresses) == 0 {
		return domain.GenerateRandomAddress() // Fallback to random if no CEX addresses
	}

	// Simple random selection
	idx := int(g.state.GeneratedCount) % len(addresses)

	// Convert string address to Address type
	addr, err := g.parseAddressString(addresses[idx])
	if err != nil {
		return domain.GenerateRandomAddress() // Fallback on parse error
	}

	return addr
}

// parseAddressString converts hex string to Address type
func (g *TxGenerator) parseAddressString(hexStr string) (sharedDomain.Address, error) {
	var addr sharedDomain.Address

	// Remove 0x prefix if present
	if len(hexStr) >= 2 && hexStr[:2] == "0x" {
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
