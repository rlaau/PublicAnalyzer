package infra

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	sharedDomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// FileDepositRepository implements DepositRepository using CSV file storage
type FileDepositRepository struct {
	filePath string
	mutex    sync.RWMutex
}

// NewFileDepositRepository creates a new file-based deposit repository
func NewFileDepositRepository(filePath string) *FileDepositRepository {
	return &FileDepositRepository{
		filePath: filePath,
	}
}

// SaveDetectedDeposit saves a detected deposit address to CSV file
func (r *FileDepositRepository) SaveDetectedDeposit(deposit *domain.DetectedDepositAddress) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Create file and directory if not exists
	if err := r.ensureFileExists(); err != nil {
		return fmt.Errorf("failed to ensure file exists: %w", err)
	}

	// Load existing deposits without lock to avoid deadlock
	deposits, err := r.loadDetectedDepositsNoLock()
	if err != nil {
		return fmt.Errorf("failed to load deposits: %w", err)
	}

	// Check if deposit already exists
	// TODO 여기도 체킹 로직 제거 가능. 중복임.
	//TODO 그러나 이 중복은 set을 in-memory로 올려놓을 떄나 중복 로직이고, 만약 불가 시 여기서 한번 더 체크는 필요.
	//TODO 현재는 중복임
	//TODO Save호출하기 전 Set에서 한번 확인 후 제거함.
	addrStr := deposit.Address.String()
	found := false
	for _, existing := range deposits {
		if existing.Address.String() == addrStr {
			// Update existing deposit
			existing.TxCount = deposit.TxCount
			found = true
			break
		}
	}

	if !found {
		// Add new deposit
		deposits = append(deposits, deposit)
	}

	// Rewrite file with all deposits
	//* 증분 방식으로 바꿨으나 검증 필요
	return r.appendDepositToFile(deposit)
}

// LoadDetectedDeposits loads all detected deposit addresses from CSV file
func (r *FileDepositRepository) LoadDetectedDeposits() ([]*domain.DetectedDepositAddress, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.loadDetectedDepositsNoLock()
}

// loadDetectedDepositsNoLock loads deposits without taking mutex lock (for internal use)
func (r *FileDepositRepository) loadDetectedDepositsNoLock() ([]*domain.DetectedDepositAddress, error) {
	// Return empty slice if file doesn't exist
	if _, err := os.Stat(r.filePath); os.IsNotExist(err) {
		return []*domain.DetectedDepositAddress{}, nil
	}

	file, err := os.Open(r.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comment = '#' // Allow comments in CSV

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV records: %w", err)
	}

	var deposits []*domain.DetectedDepositAddress

	for i, record := range records {
		// Skip header if present
		if i == 0 && strings.HasPrefix(record[0], "address") {
			continue
		}

		if len(record) < 4 {
			continue // Skip malformed records
		}

		deposit, err := r.parseDepositRecord(record)
		if err != nil {
			// Log error but continue processing
			continue
		}

		deposits = append(deposits, deposit)
	}

	return deposits, nil
}

// IsDepositAddress checks if an address is a known deposit address
func (r *FileDepositRepository) IsDepositAddress(addr sharedDomain.Address) (bool, error) {
	deposits, err := r.LoadDetectedDeposits()
	if err != nil {
		return false, err
	}

	addrStr := addr.String()
	for _, deposit := range deposits {
		if deposit.Address.String() == addrStr {
			return true, nil
		}
	}

	return false, nil
}

// GetDepositInfo retrieves deposit address information
func (r *FileDepositRepository) GetDepositInfo(addr sharedDomain.Address) (*domain.DetectedDepositAddress, error) {
	deposits, err := r.LoadDetectedDeposits()
	if err != nil {
		return nil, err
	}

	addrStr := addr.String()
	for _, deposit := range deposits {
		if deposit.Address.String() == addrStr {
			return deposit, nil
		}
	}

	return nil, fmt.Errorf("deposit address not found: %s", addrStr)
}

// UpdateTxCount updates the transaction count for a deposit address
func (r *FileDepositRepository) UpdateTxCount(addr sharedDomain.Address, count int64) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Load all deposits without lock to avoid deadlock
	deposits, err := r.loadDetectedDepositsNoLock()
	if err != nil {
		return fmt.Errorf("failed to load deposits: %w", err)
	}

	// Find and update the specific deposit
	addrStr := addr.String()
	found := false
	for _, deposit := range deposits {
		if deposit.Address.String() == addrStr {
			deposit.TxCount = count
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("deposit address not found for update: %s", addrStr)
	}

	// Rewrite entire file with updated data
	return r.rewriteFile(deposits)
}

// ensureFileExists creates the file and directory if they don't exist
func (r *FileDepositRepository) ensureFileExists() error {
	dir := filepath.Dir(r.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Create file with header if it doesn't exist
	if _, err := os.Stat(r.filePath); os.IsNotExist(err) {
		file, err := os.Create(r.filePath)
		if err != nil {
			return err
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		// Write CSV header
		header := []string{"address", "detected_at", "cex_address", "tx_count"}
		return writer.Write(header)
	}

	return nil
}

// rewriteFile rewrites the entire file with given deposits
func (r *FileDepositRepository) rewriteFile(deposits []*domain.DetectedDepositAddress) error {
	// Create temporary file
	tmpPath := r.filePath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tmpFile.Close()

	writer := csv.NewWriter(tmpFile)
	defer writer.Flush()

	// Write header
	header := []string{"address", "detected_at", "cex_address", "tx_count"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write all deposits
	for _, deposit := range deposits {
		record := []string{
			deposit.Address.String(),
			deposit.DetectedAt.Format(time.RFC3339),
			deposit.CEXAddress.String(),
			strconv.FormatInt(deposit.TxCount, 10),
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write deposit record: %w", err)
		}
	}

	// Atomic replace
	return os.Rename(tmpPath, r.filePath)
}
func (r *FileDepositRepository) appendDepositToFile(deposit *domain.DetectedDepositAddress) error {
	// Check if file exists and whether it's empty (헤더가 필요할 수 있음)
	needHeader := false
	if _, err := os.Stat(r.filePath); errors.Is(err, os.ErrNotExist) {
		needHeader = true
	} else {
		info, err := os.Stat(r.filePath)
		if err != nil {
			return fmt.Errorf("failed to stat deposit file: %w", err)
		}
		if info.Size() == 0 {
			needHeader = true
		}
	}

	// Open file in append mode
	file, err := os.OpenFile(r.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open deposit file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if needed
	if needHeader {
		header := []string{"address", "detected_at", "cex_address", "tx_count"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Write one record
	record := []string{
		deposit.Address.String(),
		deposit.DetectedAt.Format(time.RFC3339),
		deposit.CEXAddress.String(),
		strconv.FormatInt(deposit.TxCount, 10),
	}
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write deposit record: %w", err)
	}

	return nil
}

// parseDepositRecord parses a CSV record into DetectedDepositAddress
func (r *FileDepositRepository) parseDepositRecord(record []string) (*domain.DetectedDepositAddress, error) {
	if len(record) < 4 {
		return nil, fmt.Errorf("invalid record length: %d", len(record))
	}

	// Parse address
	addr, err := r.parseAddress(record[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse address: %w", err)
	}

	// Parse detected time
	detectedAt, err := time.Parse(time.RFC3339, record[1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse detected time: %w", err)
	}

	// Parse CEX address
	cexAddr, err := r.parseAddress(record[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse CEX address: %w", err)
	}

	// Parse transaction count
	txCount, err := strconv.ParseInt(record[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse tx count: %w", err)
	}

	return &domain.DetectedDepositAddress{
		Address:    addr,
		DetectedAt: detectedAt,
		CEXAddress: cexAddr,
		TxCount:    txCount,
	}, nil
}

// parseAddress converts hex string to Address
func (r *FileDepositRepository) parseAddress(hexStr string) (sharedDomain.Address, error) {
	var addr sharedDomain.Address

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
