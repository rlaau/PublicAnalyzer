package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type ContractData struct {
	Address    string `json:"address"`
	Creator    string `json:"creator,omitempty"`
	CreationTx string `json:"creation_tx,omitempty"`
	CreatedAt  string `json:"created_at"`
	LastSeenAt string `json:"last_seen_at"`
}

// func main() {
// 	contDB, err := app.NewContractDB(mode.ProductionModeProcess)
// 	if err != nil {
// 		log.Fatalf("Failed to create ContractDB: %v", err)
// 	}
// 	defer contDB.Close()

// 	jsonlDir := "/home/rlaaudgjs5638/chainAnalyzer/internal/cce/cmd/load_all_contracts_once/out_jsonl"

// 	pattern := filepath.Join(jsonlDir, "contracts-*.jsonl")
// 	files, err := filepath.Glob(pattern)
// 	if err != nil {
// 		log.Fatalf("Failed to find jsonl files: %v", err)
// 	}

// 	if len(files) == 0 {
// 		log.Fatal("No contract jsonl files found")
// 	}

// 	fmt.Printf("Found %d contract files\n", len(files))

// 	totalContracts := 0
// 	batchSize := 1000

// 	for _, filename := range files {
// 		fmt.Printf("Processing file: %s\n", filepath.Base(filename))

// 		contracts, err := loadContractsFromFile(filename, batchSize)
// 		if err != nil {
// 			log.Printf("Error processing file %s: %v", filename, err)
// 			continue
// 		}

// 		for i, batch := range contracts {
// 			fmt.Printf("  Inserting batch %d (%d contracts)\n", i+1, len(batch))
// 			err := contDB.BatchPut(batch)
// 			if err != nil {
// 				log.Printf("Error inserting batch %d: %v", i+1, err)
// 				continue
// 			}
// 			totalContracts += len(batch)
// 		}
// 	}

// 	fmt.Printf("Successfully inserted %d contracts\n", totalContracts)

// 	count, err := contDB.Count()
// 	if err != nil {
// 		log.Printf("Error getting count: %v", err)
// 	} else {
// 		fmt.Printf("Total contracts in DB: %d\n", count)
// 	}
// }

func loadContractsFromFile(filename string, batchSize int) ([][]domain.Contract, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var allBatches [][]domain.Contract
	var currentBatch []domain.Contract

	scanner := bufio.NewScanner(file)
	lineCount := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var contractData ContractData
		if err := json.Unmarshal([]byte(line), &contractData); err != nil {
			log.Printf("Error parsing JSON at line %d: %v", lineCount+1, err)
			continue
		}

		contract, err := convertToContract(contractData)
		if err != nil {
			log.Printf("Error converting contract at line %d: %v", lineCount+1, err)
			continue
		}

		currentBatch = append(currentBatch, contract)

		if len(currentBatch) >= batchSize {
			allBatches = append(allBatches, currentBatch)
			currentBatch = []domain.Contract{}
		}

		lineCount++
	}

	if len(currentBatch) > 0 {
		allBatches = append(allBatches, currentBatch)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return allBatches, nil
}

func convertToContract(data ContractData) (domain.Contract, error) {
	contract := domain.Contract{}

	addr, err := domain.ParseAddressFromString(data.Address)
	if err != nil {
		return contract, fmt.Errorf("failed to parse address: %v", err)
	}
	contract.Address = addr

	if data.Creator != "" {
		creator, err := domain.ParseAddressFromString(data.Creator)
		if err != nil {
			return contract, fmt.Errorf("failed to parse creator: %v", err)
		}
		contract.CreatorOrZero = creator
	}

	if data.CreationTx != "" {
		txId, err := parseTxIdFromString(data.CreationTx)
		if err != nil {
			return contract, fmt.Errorf("failed to parse creation_tx: %v", err)
		}
		contract.CreationTxOrZero = txId
	}

	createdAt, err := time.Parse(time.RFC3339, data.CreatedAt)
	if err != nil {
		return contract, fmt.Errorf("failed to parse created_at: %v", err)
	}
	contract.CreatedAt = chaintimer.ChainTime(createdAt)

	return contract, nil
}

func parseTxIdFromString(hexStr string) (domain.TxId, error) {
	var txId domain.TxId

	if strings.HasPrefix(hexStr, "0x") {
		hexStr = hexStr[2:]
	}

	if len(hexStr) != 64 {
		return txId, fmt.Errorf("invalid tx id length: %d", len(hexStr))
	}

	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return txId, fmt.Errorf("invalid hex string: %w", err)
	}

	copy(txId[:], bytes)
	return txId, nil
}
