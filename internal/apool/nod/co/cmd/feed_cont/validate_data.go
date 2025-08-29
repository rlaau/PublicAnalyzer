package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

type ValidationResult struct {
	Address      string
	JsonlData    *ContractData
	DbData       *domain.Contract
	IsMatch      bool
	ErrorMessage string
}

func main() {
	contDB, err := infra.NewContractDB(mode.ProductionModeProcess, "")
	if err != nil {
		log.Fatalf("Failed to create ContractDB: %v", err)
	}
	defer contDB.Close()

	fmt.Println("=== Contract Data Validation ===")

	// 1. JSONL 파일에서 랜덤 1000개 추출
	fmt.Println("1. Extracting 1000 random contracts from JSONL files...")
	jsonlContracts, err := extractRandomFromJsonl(1000)
	if err != nil {
		log.Fatalf("Failed to extract random contracts from JSONL: %v", err)
	}
	fmt.Printf("   Extracted %d contracts from JSONL\n", len(jsonlContracts))

	// 2. 해당 주소들로 DB에서 조회
	fmt.Println("2. Querying DB with the same addresses...")
	results := validateContractsAgainstDB(contDB, jsonlContracts)

	// 3. 결과 출력
	printValidationResults(results)
}

func extractRandomFromJsonl(count int) (map[string]ContractData, error) {
	jsonlDir := "/home/rlaaudgjs5638/chainAnalyzer/internal/cce/cmd/load_all_contracts_once/out_jsonl"
	pattern := filepath.Join(jsonlDir, "contracts-*.jsonl")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no contract jsonl files found")
	}

	// 랜덤하게 10개 파일 선택
	maxFiles := 10
	if len(files) > maxFiles {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		r.Shuffle(len(files), func(i, j int) {
			files[i], files[j] = files[j], files[i]
		})
		files = files[:maxFiles]
	}

	result := make(map[string]ContractData)
	targetPerFile := count / len(files)
	if targetPerFile == 0 {
		targetPerFile = 10 // 최소 10개씩
	}

	for _, filename := range files {
		contracts, err := extractRandomFromFile(filename, targetPerFile)
		if err != nil {
			log.Printf("Error loading from %s: %v", filename, err)
			continue
		}

		for _, contract := range contracts {
			result[contract.Address] = contract
			if len(result) >= count {
				break
			}
		}

		if len(result) >= count {
			break
		}
	}

	fmt.Printf("   Extracted %d contracts from %d files\n", len(result), len(files))
	return result, nil
}

func loadContractsFromJsonlFile(filename string) ([]ContractData, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var contracts []ContractData
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var contractData ContractData
		if err := json.Unmarshal([]byte(line), &contractData); err != nil {
			continue // 파싱 에러는 무시하고 계속
		}

		contracts = append(contracts, contractData)
	}

	return contracts, scanner.Err()
}

func extractRandomFromFile(filename string, count int) ([]ContractData, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 파일 전체를 읽어서 라인 수 확인
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(lines) == 0 {
		return []ContractData{}, nil
	}

	// 랜덤 인덱스들 선택
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	selectedCount := count
	if len(lines) < count {
		selectedCount = len(lines)
	}

	indices := make([]int, len(lines))
	for i := range indices {
		indices[i] = i
	}
	r.Shuffle(len(indices), func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})

	var contracts []ContractData
	for i := 0; i < selectedCount; i++ {
		line := lines[indices[i]]
		var contractData ContractData
		if err := json.Unmarshal([]byte(line), &contractData); err != nil {
			continue
		}
		contracts = append(contracts, contractData)
	}

	return contracts, nil
}

func validateContractsAgainstDB(contDB *infra.ContractDB, jsonlContracts map[string]ContractData) []ValidationResult {
	var results []ValidationResult

	for address, jsonlData := range jsonlContracts {
		result := ValidationResult{
			Address:   address,
			JsonlData: &jsonlData,
		}

		// DB에서 해당 주소 조회
		addr, err := domain.ParseAddressFromString(address)
		if err != nil {
			result.IsMatch = false
			result.ErrorMessage = fmt.Sprintf("Failed to parse address: %v", err)
			results = append(results, result)
			continue
		}

		dbContract, err := contDB.Get(addr)
		if err != nil {
			result.IsMatch = false
			result.ErrorMessage = fmt.Sprintf("Contract not found in DB: %v", err)
			results = append(results, result)
			continue
		}

		result.DbData = &dbContract

		// 데이터 비교
		isMatch, errMsg := compareContractData(jsonlData, dbContract)
		result.IsMatch = isMatch
		result.ErrorMessage = errMsg

		results = append(results, result)
	}

	return results
}

func compareContractData(jsonlData ContractData, dbData domain.Contract) (bool, string) {
	// Address 비교
	if jsonlData.Address != dbData.Address.String() {
		return false, "Address mismatch"
	}

	// Creator 비교 (빈 값 처리 주의)
	expectedCreator := ""
	if jsonlData.Creator != "" {
		expectedCreator = jsonlData.Creator
	}
	actualCreator := ""
	if !isZeroAddress(dbData.CreatorOrZero) {
		actualCreator = dbData.CreatorOrZero.String()
	}

	if expectedCreator != actualCreator {
		return false, fmt.Sprintf("Creator mismatch: expected '%s', got '%s'", expectedCreator, actualCreator)
	}

	// CreationTx 비교
	expectedTx := ""
	if jsonlData.CreationTx != "" {
		expectedTx = jsonlData.CreationTx
	}
	actualTx := ""
	if !isZeroTxId(dbData.CreationTxOrZero) {
		actualTx = dbData.CreationTxOrZero.String()
	}

	if expectedTx != actualTx {
		return false, fmt.Sprintf("CreationTx mismatch: expected '%s', got '%s'", expectedTx, actualTx)
	}

	// CreatedAt 비교
	expectedTime, err := time.Parse(time.RFC3339, jsonlData.CreatedAt)
	if err != nil {
		return false, fmt.Sprintf("Failed to parse expected time: %v", err)
	}

	actualTime := time.Time(dbData.CreatedAt)

	// 시간은 초 단위까지만 비교 (나노초 차이 무시)
	if !expectedTime.Truncate(time.Second).Equal(actualTime.Truncate(time.Second)) {
		return false, fmt.Sprintf("CreatedAt mismatch: expected '%s', got '%s'",
			expectedTime.Format(time.RFC3339), actualTime.Format(time.RFC3339))
	}

	return true, ""
}

func isZeroAddress(addr domain.Address) bool {
	return domain.IsNullAddress(addr)
}

func isZeroTxId(txId domain.TxId) bool {
	var zero domain.TxId
	return txId == zero
}

func printValidationResults(results []ValidationResult) {
	matchCount := 0
	mismatchCount := 0

	fmt.Println("\n=== Validation Results ===")

	for i, result := range results {
		if result.IsMatch {
			matchCount++
		} else {
			mismatchCount++
			fmt.Printf("%d. MISMATCH - Address: %s\n", i+1, result.Address)
			fmt.Printf("   Error: %s\n", result.ErrorMessage)
			if result.JsonlData != nil {
				fmt.Printf("   JSONL: creator=%s, tx=%s, created_at=%s\n",
					result.JsonlData.Creator, result.JsonlData.CreationTx, result.JsonlData.CreatedAt)
			}
			if result.DbData != nil {
				fmt.Printf("   DB: creator=%s, tx=%s, created_at=%s\n",
					result.DbData.CreatorOrZero.String(), result.DbData.CreationTxOrZero.String(),
					time.Time(result.DbData.CreatedAt).Format(time.RFC3339))
			}
			fmt.Println()
		}
	}

	fmt.Printf("=== Summary ===\n")
	fmt.Printf("Total compared: %d\n", len(results))
	fmt.Printf("Matches: %d (%.1f%%)\n", matchCount, float64(matchCount)/float64(len(results))*100)
	fmt.Printf("Mismatches: %d (%.1f%%)\n", mismatchCount, float64(mismatchCount)/float64(len(results))*100)

	if matchCount == len(results) {
		fmt.Println("✅ All data matches perfectly!")
	} else {
		fmt.Printf("❌ Found %d mismatches\n", mismatchCount)
	}
}
