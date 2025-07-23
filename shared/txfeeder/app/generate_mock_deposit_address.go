package app

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
)

// GenerateMockDepositAddresses generates 100,000 mock deposit addresses
// This is a one-time utility function to create the mock data file
func GenerateMockDepositAddresses(filePath string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Write header comment
	_, err = file.WriteString("# Mocked and Hidden Deposit Addresses\n")
	if err != nil {
		return err
	}

	_, err = file.WriteString("# This file contains 100,000 mock deposit addresses for testing\n")
	if err != nil {
		return err
	}

	_, err = file.WriteString("# Format: one Ethereum address per line\n")
	if err != nil {
		return err
	}

	// Generate 100,000 random addresses
	for i := 0; i < 100000; i++ {
		address := make([]byte, 20)
		_, err := rand.Read(address)
		if err != nil {
			return fmt.Errorf("failed to generate random address: %w", err)
		}

		// Write as hex string
		_, err = file.WriteString(fmt.Sprintf("0x%x\n", address))
		if err != nil {
			return fmt.Errorf("failed to write address: %w", err)
		}
	}

	return nil
}
