package infra

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// FileCEXRepository implements CEXRepository using file storage
type FileCEXRepository struct {
	filePath string
	mutex    sync.RWMutex
	cexSet   *domain.CEXSet
}

// NewFileCEXRepository creates a new file-based CEX repository
func NewFileCEXRepository(filePath string) *FileCEXRepository {
	return &FileCEXRepository{
		filePath: filePath,
		cexSet:   domain.NewCEXSet(),
	}
}

// LoadCEXSet loads all CEX addresses from file into a set
func (r *FileCEXRepository) LoadCEXSet() (*domain.CEXSet, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Create file if it doesn't exist
	if _, err := os.Stat(r.filePath); os.IsNotExist(err) {
		if err := r.createFileIfNotExists(); err != nil {
			return nil, fmt.Errorf("failed to create CEX file: %w", err)
		}
	}

	file, err := os.Open(r.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CEX file: %w", err)
	}
	defer file.Close()

	r.cexSet = domain.NewCEXSet()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		cexAddr := domain.NewCEXAddress(line)
		if cexAddr.IsValid() {
			r.cexSet.Add(cexAddr.Address)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read CEX file: %w", err)
	}

	return r.cexSet, nil
}

// AddCEXAddress adds a new CEX address to storage
func (r *FileCEXRepository) AddCEXAddress(address string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	cexAddr := domain.NewCEXAddress(address)
	if !cexAddr.IsValid() {
		return fmt.Errorf("invalid address format: %s", address)
	}

	// Check if already exists
	if r.cexSet.Contains(cexAddr.Address) {
		return nil // Already exists, no need to add
	}

	// Add to memory set
	r.cexSet.Add(cexAddr.Address)

	// Append to file
	file, err := os.OpenFile(r.filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open CEX file for writing: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%s\n", cexAddr.Address))
	if err != nil {
		return fmt.Errorf("failed to write to CEX file: %w", err)
	}

	return nil
}

// RemoveCEXAddress removes a CEX address from storage
func (r *FileCEXRepository) RemoveCEXAddress(address string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	cexAddr := domain.NewCEXAddress(address)

	// Read all addresses except the one to remove
	file, err := os.Open(r.filePath)
	if err != nil {
		return fmt.Errorf("failed to open CEX file: %w", err)
	}

	var lines []string
	scanner := bufio.NewScanner(file)
	found := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			lines = append(lines, scanner.Text())
			continue
		}

		if strings.ToLower(line) == cexAddr.Address {
			found = true
			continue // Skip this line
		}

		lines = append(lines, scanner.Text())
	}
	file.Close()

	if !found {
		return fmt.Errorf("address not found: %s", address)
	}

	// Rewrite the file
	if err := r.writeLinesToFile(lines); err != nil {
		return fmt.Errorf("failed to update CEX file: %w", err)
	}

	// Reload the in-memory set
	return r.Reload()
}

// Reload reloads CEX addresses from storage
func (r *FileCEXRepository) Reload() error {
	_, err := r.LoadCEXSet()
	return err
}

// createFileIfNotExists creates the CEX file with default content if it doesn't exist
func (r *FileCEXRepository) createFileIfNotExists() error {
	dir := filepath.Dir(r.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(r.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	defaultContent := `# CEX Hot Wallet Addresses
# This file contains known centralized exchange hot wallet addresses
# Format: one Ethereum address per line
# Lines starting with # are comments and will be ignored
`
	_, err = file.WriteString(defaultContent)
	return err
}

// writeLinesToFile writes all lines to the file
func (r *FileCEXRepository) writeLinesToFile(lines []string) error {
	file, err := os.Create(r.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, line := range lines {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return err
		}
	}

	return nil
}
