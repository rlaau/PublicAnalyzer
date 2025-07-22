package app

import (
	"context"
	"fmt"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/infra"
)

// RunTxGeneratorExample demonstrates how to use the TxGenerator
func RunTxGeneratorExample() error {
	fmt.Println("=== Transaction Generator Example ===")

	// 1. Load CEX addresses
	cexRepo := infra.NewFileCEXRepository("/home/rlaaudgjs5638/chainAnalyzer/internal/ee/cex.txt")
	cexSet, err := cexRepo.LoadCEXSet()
	if err != nil {
		return fmt.Errorf("failed to load CEX addresses: %w", err)
	}
	fmt.Printf("Loaded %d CEX addresses\n", cexSet.Size())

	// 2. Create generator config (smaller scale for testing)
	config := &domain.TxGeneratorConfig{
		TotalTransactions:           1000,        // Generate 1000 transactions for testing
		TransactionsPerSecond:       100,        // 100 tx/sec for testing
		TransactionsPerTimeIncrement: 15,        // 15 tx per time increment
		TimeIncrementDuration:       time.Second,
		StartTime:                   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		DepositToCexRatio:          50,          // 1 in 50
		RandomToDepositRatio:       20,          // 1 in 20
	}
	

	// 3. Create generator
	generator := NewTxGenerator(config, cexSet)

	// 4. Load mock deposit addresses
	err = generator.LoadMockDepositAddresses("/home/rlaaudgjs5638/chainAnalyzer/internal/ee/mockedAndHiddenDepositAddress.txt")
	if err != nil {
		return fmt.Errorf("failed to load mock deposit addresses: %w", err)
	}
	fmt.Printf("Loaded mock deposit addresses\n")

	// 5. Start generation
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = generator.Start(ctx)
	if err != nil {
		return fmt.Errorf("failed to start generator: %w", err)
	}

	// 6. Consume transactions
	fmt.Println("Starting transaction consumption...")
	
	consumedCount := 0
	depositToCexCount := 0
	randomToDepositCount := 0
	regularTxCount := 0
	
	startTime := time.Now()
	
	go func() {
		for tx := range generator.GetTxChannel() {
			consumedCount++
			
			// Analyze transaction patterns (basic classification)
			if consumedCount%50 == 0 {
				depositToCexCount++
				fmt.Printf("[%d] DepositToCex: %s -> %s (Value: %s ETH)\n", 
					consumedCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...", 
					weiToEth(tx.Value.String()))
			} else if consumedCount%20 == 0 {
				randomToDepositCount++
				fmt.Printf("[%d] RandomToDeposit: %s -> %s (Value: %s ETH)\n", 
					consumedCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...", 
					weiToEth(tx.Value.String()))
			} else {
				regularTxCount++
			}
			
			if consumedCount%100 == 0 {
				fmt.Printf("Progress: %d transactions processed\n", consumedCount)
			}
		}
		
		endTime := time.Now()
		duration := endTime.Sub(startTime)
		
		fmt.Printf("\n=== Generation Complete ===\n")
		fmt.Printf("Total transactions: %d\n", consumedCount)
		fmt.Printf("DepositToCex transactions: %d\n", depositToCexCount)
		fmt.Printf("RandomToDeposit transactions: %d\n", randomToDepositCount)
		fmt.Printf("Regular transactions: %d\n", regularTxCount)
		fmt.Printf("Duration: %v\n", duration)
		fmt.Printf("Average TPS: %.2f\n", float64(consumedCount)/duration.Seconds())
	}()

	// Wait for completion or timeout
	<-ctx.Done()
	generator.Stop()
	
	fmt.Println("Transaction generation example completed")
	return nil
}

// weiToEth converts wei string to ETH string (simplified)
func weiToEth(weiStr string) string {
	// This is a simplified conversion for display purposes
	if len(weiStr) > 18 {
		ethPart := weiStr[:len(weiStr)-18]
		if ethPart == "" {
			ethPart = "0"
		}
		return ethPart
	}
	return "0"
}

// ProductionTxGeneratorExample shows how to set up for full 400M transaction generation
func ProductionTxGeneratorExample() *TxGenerator {
	// Load CEX addresses
	cexRepo := infra.NewFileCEXRepository("/home/rlaaudgjs5638/chainAnalyzer/internal/ee/cex.txt")
	cexSet, _ := cexRepo.LoadCEXSet()

	// Production config
	config := domain.NewDefaultTxGeneratorConfig() // 400M transactions at 1M tx/sec

	// Create generator
	generator := NewTxGenerator(config, cexSet)
	generator.LoadMockDepositAddresses("/home/rlaaudgjs5638/chainAnalyzer/internal/ee/mockedAndHiddenDepositAddress.txt")

	return generator
}