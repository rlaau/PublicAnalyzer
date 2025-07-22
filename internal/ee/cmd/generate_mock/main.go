package main

import (
	"crypto/rand"
	"fmt"
	"os"
)

func main() {
	filePath := "/home/rlaaudgjs5638/chainAnalyzer/internal/ee/mockedAndHiddenDepositAddress.txt"
	
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer file.Close()

	// Write header comment
	file.WriteString("# Mocked and Hidden Deposit Addresses\n")
	file.WriteString("# This file contains 100,000 mock deposit addresses for testing\n") 
	file.WriteString("# Format: one Ethereum address per line\n")

	// Generate 100,000 random addresses
	for i := 0; i < 100000; i++ {
		address := make([]byte, 20)
		_, err := rand.Read(address)
		if err != nil {
			fmt.Printf("Error generating address: %v\n", err)
			return
		}

		// Write as hex string
		fmt.Fprintf(file, "0x%x\n", address)
	}

	fmt.Printf("Successfully generated 100,000 mock deposit addresses in %s\n", filePath)
}