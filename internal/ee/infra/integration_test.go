package infra

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// TestDualManagerIntegration tests the complete workflow of dual manager
func TestDualManagerIntegration(t *testing.T) {
	startTime := time.Now()
	fmt.Println("🚀 Starting DualManager Integration Test")
	fmt.Printf("⏰ Start time: %s\n", startTime.Format("2006-01-02 15:04:05"))

	// Setup test directories
	testDir := "./test_ee_data"
	graphDBPath := filepath.Join(testDir, "graph")
	pendingDBPath := filepath.Join(testDir, "pending")

	// Clean up function with error handling
	cleanup := func() {
		fmt.Println("🧹 Starting cleanup...")
		if err := os.RemoveAll(testDir); err != nil {
			log.Printf("⚠️  Cleanup error: %v", err)
		} else {
			fmt.Println("✅ Test data cleaned up successfully")
		}
	}
	defer cleanup()

	// Error handling with cleanup
	handleError := func(msg string, err error) {
		log.Printf("💥 ERROR: %s - %v", msg, err)
		cleanup()
		t.Fatalf("%s: %v", msg, err)
	}

	// Create test directories
	if err := os.MkdirAll(testDir, 0755); err != nil {
		handleError("Failed to create test directory", err)
	}

	pprofDir := filepath.Join(testDir, "pprof")
	if err := os.MkdirAll(pprofDir, 0755); err != nil {
		handleError("Failed to create pprof directory", err)
	}

	// Start CPU profiling
	cpuProfilePath := filepath.Join(pprofDir, "cpu.prof")
	cpuFile, err := os.Create(cpuProfilePath)
	if err != nil {
		handleError("Failed to create CPU profile file", err)
	}
	
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		cpuFile.Close()
		handleError("Failed to start CPU profiling", err)
	}
	
	defer func() {
		pprof.StopCPUProfile()
		cpuFile.Close()
		fmt.Printf("📊 CPU profile saved to: %s\n", cpuProfilePath)
	}()

	// Setup test components
	fmt.Println("🔧 Setting up test components...")
	
	// 1. Create mock CEX set and ground knowledge
	cexSet := domain.NewCEXSet()
	mockCEXAddresses := []string{
		"0x1234567890abcdef1234567890abcdef12345678",
		"0xabcdef1234567890abcdef1234567890abcdef12",
		"0x567890abcdef1234567890abcdef1234567890ab",
	}
	for _, addr := range mockCEXAddresses {
		cexSet.Add(addr)
	}
	fmt.Printf("🏦 Created %d mock CEX addresses\n", len(mockCEXAddresses))

	depositRepo := &MockDepositRepo{deposits: make(map[string]*domain.DetectedDepositAddress)}
	groundKnowledge := domain.NewGroundKnowledge(cexSet, depositRepo)
	if err := groundKnowledge.Load(); err != nil {
		handleError("Failed to load ground knowledge", err)
	}

	// 2. Create graph repository
	graphRepo, err := NewBadgerGraphRepository(graphDBPath)
	if err != nil {
		handleError("Failed to create graph repository", err)
	}
	defer func() {
		if err := graphRepo.Close(); err != nil {
			log.Printf("⚠️  Graph repository close error: %v", err)
		}
	}()

	// 3. Create dual manager
	dualManager, err := domain.NewDualManager(groundKnowledge, graphRepo, pendingDBPath)
	if err != nil {
		handleError("Failed to create dual manager", err)
	}
	defer func() {
		if err := dualManager.Close(); err != nil {
			log.Printf("⚠️  Dual manager close error: %v", err)
		}
	}()

	fmt.Println("✅ Test components setup complete")

	// Run transaction simulation
	fmt.Println("🚀 Starting transaction simulation...")
	
	txCount, errorCount := runSimpleTransactionSimulation(t, dualManager, groundKnowledge, mockCEXAddresses)

	// Memory profiling
	memProfilePath := filepath.Join(pprofDir, "mem.prof")
	memFile, err := os.Create(memProfilePath)
	if err != nil {
		log.Printf("⚠️  Failed to create memory profile: %v", err)
	} else {
		runtime.GC()
		if err := pprof.WriteHeapProfile(memFile); err != nil {
			log.Printf("⚠️  Failed to write memory profile: %v", err)
		} else {
			fmt.Printf("📊 Memory profile saved to: %s\n", memProfilePath)
		}
		memFile.Close()
	}

	// Print results
	printSimpleTestResults(dualManager, graphRepo, txCount, errorCount)

	// Final timing
	duration := time.Since(startTime)
	fmt.Printf("⏱️  Total execution time: %v\n", duration)
	fmt.Printf("🏁 Test completed at: %s\n", time.Now().Format("2006-01-02 15:04:05"))
}

func runSimpleTransactionSimulation(t *testing.T, dm *domain.DualManager, gk *domain.GroundKnowledge, cexAddresses []string) (int, int) {
	fmt.Println("📊 Running simplified transaction simulation...")

	// Generate test addresses
	testAddresses := make([]shareddomain.Address, 20)
	for i := 0; i < 20; i++ {
		rand.Read(testAddresses[i][:])
	}

	depositCandidates := make([]shareddomain.Address, 5)
	for i := 0; i < 5; i++ {
		rand.Read(depositCandidates[i][:])
	}

	// Convert CEX addresses to Address type
	var cexAddrs []shareddomain.Address
	for _, cexStr := range cexAddresses {
		var addr shareddomain.Address
		if bytes, err := parseHexAddress(cexStr); err == nil {
			copy(addr[:], bytes)
			cexAddrs = append(cexAddrs, addr)
		}
	}

	txCount := 0
	errorCount := 0
	baseTime := time.Now().Add(-24 * time.Hour)

	// Phase 1: Regular EOA-EOA transactions
	fmt.Println("Phase 1: Building pending relations...")
	for i := 0; i < 100; i++ {
		fromAddr := testAddresses[rand.Intn(len(testAddresses))]
		toAddr := depositCandidates[rand.Intn(len(depositCandidates))]
		
		tx := createSimpleTransaction(fromAddr, toAddr, baseTime.Add(time.Duration(i)*time.Minute))
		
		if err := dm.CheckTransaction(tx); err != nil {
			errorCount++
			log.Printf("⚠️  Transaction %d failed: %v", i, err)
			continue
		}
		txCount++
		
		if i%25 == 0 && i > 0 {
			fmt.Printf("  ✅ Processed %d transactions\n", i)
		}
	}

	// Phase 2: Deposit transactions (trigger CEX detection)
	fmt.Println("Phase 2: Triggering deposit detection...")
	for i := 0; i < 10; i++ {
		depositAddr := depositCandidates[rand.Intn(len(depositCandidates))]
		cexAddr := cexAddrs[rand.Intn(len(cexAddrs))]
		
		tx := createSimpleTransaction(depositAddr, cexAddr, baseTime.Add(time.Duration(200+i)*time.Minute))
		
		if err := dm.CheckTransaction(tx); err != nil {
			errorCount++
			log.Printf("⚠️  Deposit transaction %d failed: %v", i, err)
			continue
		}
		txCount++
		
		if gk.IsDepositAddress(depositAddr) {
			fmt.Printf("  ✅ Detected deposit address: %s\n", depositAddr.String()[:10]+"...")
		}
	}

	fmt.Printf("📈 Simulation complete: %d transactions, %d errors\n", txCount, errorCount)
	return txCount, errorCount
}

func createSimpleTransaction(from, to shareddomain.Address, txTime time.Time) *shareddomain.MarkedTransaction {
	var txID shareddomain.TxId
	rand.Read(txID[:])
	
	return &shareddomain.MarkedTransaction{
		BlockTime:   txTime,
		TxID:        txID,
		TxSyntax:    [2]shareddomain.ContractBoolMark{shareddomain.EOAMark, shareddomain.EOAMark},
		From:        from,
		To:          to,
		Value:       shareddomain.NewBigInt("1000000000000000000"),
		BlockNumber: shareddomain.BlockNumber(rand.Intn(1000000)),
		Nonce:       uint64(rand.Intn(1000)),
		GasLimit:    shareddomain.NewBigInt("21000"),
		Input:       "",
	}
}

func printSimpleTestResults(dm *domain.DualManager, graphRepo domain.GraphRepository, txCount, errorCount int) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("🎯 TEST RESULTS")
	fmt.Println(strings.Repeat("=", 80))

	// Runtime statistics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\n💾 Memory Statistics:\n")
	fmt.Printf("  Allocated: %d KB\n", bToKb(m.Alloc))
	fmt.Printf("  Total Allocated: %d KB\n", bToKb(m.TotalAlloc))
	fmt.Printf("  System: %d KB\n", bToKb(m.Sys))
	fmt.Printf("  GC Runs: %d\n", m.NumGC)

	// Transaction statistics
	fmt.Printf("\n📊 Transaction Statistics:\n")
	fmt.Printf("  Total Processed: %d\n", txCount)
	fmt.Printf("  Errors: %d\n", errorCount)
	fmt.Printf("  Success Rate: %.2f%%\n", float64(txCount)/float64(txCount+errorCount)*100)

	// Dual Manager Statistics
	if windowStats := dm.GetWindowStats(); windowStats != nil {
		fmt.Println("\n🪟 Dual Manager Window Statistics:")
		for key, value := range windowStats {
			fmt.Printf("  %s: %v\n", key, value)
		}
	}

	// Graph Database Statistics
	if graphStats, err := graphRepo.GetGraphStats(); err == nil {
		fmt.Println("\n🗂️  Graph Database Statistics:")
		for key, value := range graphStats {
			fmt.Printf("  %s: %v\n", key, value)
		}
	} else {
		log.Printf("⚠️  Failed to get graph stats: %v", err)
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("✅ Test completed successfully!")
	fmt.Println(strings.Repeat("=", 80))
}

// Helper functions
func bToKb(b uint64) uint64 {
	return b / 1024
}

func parseHexAddress(hexStr string) ([]byte, error) {
	if strings.HasPrefix(hexStr, "0x") {
		hexStr = hexStr[2:]
	}
	bytes := make([]byte, 20)
	for i := 0; i < 20 && i*2 < len(hexStr); i++ {
		var val byte
		for j := 0; j < 2; j++ {
			if i*2+j < len(hexStr) {
				c := hexStr[i*2+j]
				var v byte
				if c >= '0' && c <= '9' {
					v = c - '0'
				} else if c >= 'a' && c <= 'f' {
					v = c - 'a' + 10
				} else if c >= 'A' && c <= 'F' {
					v = c - 'A' + 10
				}
				val = val*16 + v
			}
		}
		bytes[i] = val
	}
	return bytes, nil
}

// Mock deposit repository for testing
type MockDepositRepo struct {
	deposits map[string]*domain.DetectedDepositAddress
}

func (m *MockDepositRepo) SaveDetectedDeposit(deposit *domain.DetectedDepositAddress) error {
	m.deposits[deposit.Address.String()] = deposit
	fmt.Printf("💾 Saved deposit address: %s\n", deposit.Address.String()[:10]+"...")
	return nil
}

func (m *MockDepositRepo) LoadDetectedDeposits() ([]*domain.DetectedDepositAddress, error) {
	result := make([]*domain.DetectedDepositAddress, 0, len(m.deposits))
	for _, deposit := range m.deposits {
		result = append(result, deposit)
	}
	return result, nil
}

func (m *MockDepositRepo) IsDepositAddress(addr shareddomain.Address) (bool, error) {
	_, exists := m.deposits[addr.String()]
	return exists, nil
}

func (m *MockDepositRepo) GetDepositInfo(addr shareddomain.Address) (*domain.DetectedDepositAddress, error) {
	deposit, exists := m.deposits[addr.String()]
	if !exists {
		return nil, fmt.Errorf("deposit not found")
	}
	return deposit, nil
}

func (m *MockDepositRepo) UpdateTxCount(addr shareddomain.Address, count int64) error {
	if deposit, exists := m.deposits[addr.String()]; exists {
		deposit.TxCount = count
	}
	return nil
}