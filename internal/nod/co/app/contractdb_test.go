package app

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

func createTestContract(addressStr string) domain.Contract {
	addr, _ := domain.ParseAddressFromString(addressStr)
	return domain.Contract{
		Address:          addr,
		CreatorOrZero:    domain.Address{},
		CreationTxOrZero: domain.TxId{},
		CreatedAt:        chaintimer.ChainTime{},
		OwnerOrZero:      domain.Address{},
		OwnerLogOrZero:   []domain.Address{},
		IsERC20:          true,
		IsERC721:         false,
	}
}

func TestContractDB_BasicOperations(t *testing.T) {
	// 임시 디렉토리 생성
	tmpDir, err := os.MkdirTemp("", "contractdb_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// ContractDB 생성
	db, err := NewContractDBWithPath(dbPath)
	if err != nil {
		t.Fatalf("Failed to create ContractDB: %v", err)
	}
	defer db.Close()

	// 테스트용 Contract 생성
	contract1 := createTestContract("0x1234567890123456789012345678901234567890")
	contract2 := createTestContract("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")

	// Put 테스트
	err = db.Put(contract1)
	if err != nil {
		t.Fatalf("Failed to put contract1: %v", err)
	}

	// IsContain 테스트
	if !db.IsContain(contract1.Address) {
		t.Error("Contract1 should exist in DB")
	}

	if db.IsContain(contract2.Address) {
		t.Error("Contract2 should not exist in DB")
	}

	// Get 테스트
	retrieved, err := db.Get(contract1.Address)
	if err != nil {
		t.Fatalf("Failed to get contract1: %v", err)
	}

	if retrieved.Address != contract1.Address {
		t.Error("Retrieved contract address doesn't match")
	}
	if retrieved.IsERC20 != contract1.IsERC20 {
		t.Error("Retrieved contract IsERC20 doesn't match")
	}

	// Get 존재하지 않는 contract 테스트
	_, err = db.Get(contract2.Address)
	if err == nil {
		t.Error("Should return error for non-existent contract")
	}
}

func TestContractDB_BatchOperations(t *testing.T) {
	// 임시 디렉토리 생성
	tmpDir, err := os.MkdirTemp("", "contractdb_batch_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test_batch.db")

	// ContractDB 생성
	db, err := NewContractDBWithPath(dbPath)
	if err != nil {
		t.Fatalf("Failed to create ContractDB: %v", err)
	}
	defer db.Close()

	// 테스트용 Contract들 생성
	contracts := []domain.Contract{
		createTestContract("0x1111111111111111111111111111111111111111"),
		createTestContract("0x2222222222222222222222222222222222222222"),
		createTestContract("0x3333333333333333333333333333333333333333"),
	}

	// BatchPut 테스트
	err = db.BatchPut(contracts)
	if err != nil {
		t.Fatalf("Failed to batch put contracts: %v", err)
	}

	// 모든 contract가 저장되었는지 확인
	for _, contract := range contracts {
		if !db.IsContain(contract.Address) {
			t.Errorf("Contract %s should exist in DB", contract.Address.String())
		}
	}

	// BatchGet 테스트
	addresses := make([]domain.Address, len(contracts))
	for i, contract := range contracts {
		addresses[i] = contract.Address
	}

	results, err := db.BatchGet(addresses)
	if err != nil {
		t.Fatalf("Failed to batch get contracts: %v", err)
	}

	if len(results) != len(contracts) {
		t.Errorf("Expected %d results, got %d", len(contracts), len(results))
	}

	for _, contract := range contracts {
		if _, exists := results[contract.Address]; !exists {
			t.Errorf("Contract %s should be in batch get results", contract.Address.String())
		}
	}
}

func TestContractDB_Count(t *testing.T) {
	// 임시 디렉토리 생성
	tmpDir, err := os.MkdirTemp("", "contractdb_count_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test_count.db")

	// ContractDB 생성
	db, err := NewContractDBWithPath(dbPath)
	if err != nil {
		t.Fatalf("Failed to create ContractDB: %v", err)
	}
	defer db.Close()

	// 초기 카운트 확인 (0이어야 함)
	count, err := db.Count()
	if err != nil {
		t.Fatalf("Failed to get initial count: %v", err)
	}
	if count != 0 {
		t.Errorf("Initial count should be 0, got %d", count)
	}

	// Contract 추가
	contracts := []domain.Contract{
		createTestContract("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		createTestContract("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		createTestContract("0xcccccccccccccccccccccccccccccccccccccccc"),
	}

	err = db.BatchPut(contracts)
	if err != nil {
		t.Fatalf("Failed to batch put contracts: %v", err)
	}

	// 카운트 확인
	count, err = db.Count()
	if err != nil {
		t.Fatalf("Failed to get count after insert: %v", err)
	}
	if count != 3 {
		t.Errorf("Count should be 3, got %d", count)
	}
}

func TestContractDB_ConcurrentAccess(t *testing.T) {
	// 임시 디렉토리 생성
	tmpDir, err := os.MkdirTemp("", "contractdb_concurrent_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test_concurrent.db")

	// ContractDB 생성
	db, err := NewContractDBWithPath(dbPath)
	if err != nil {
		t.Fatalf("Failed to create ContractDB: %v", err)
	}
	defer db.Close()

	// 동시에 여러 goroutine에서 Put 수행
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(index int) {
			// 각 goroutine마다 고유한 주소 생성
			addr := fmt.Sprintf("0x%040d", index)
			contract := createTestContract(addr)
			err := db.Put(contract)
			if err != nil {
				t.Errorf("Failed to put contract in goroutine %d: %v", index, err)
			}
			done <- true
		}(i)
	}

	// 모든 goroutine 완료 대기
	for i := 0; i < 10; i++ {
		<-done
	}

	// 결과 확인
	count, err := db.Count()
	if err != nil {
		t.Fatalf("Failed to get final count: %v", err)
	}
	if count != 10 {
		t.Errorf("Final count should be 10, got %d", count)
	}
}

func TestContractDB_GCFunctionality(t *testing.T) {
	// 임시 디렉토리 생성
	tmpDir, err := os.MkdirTemp("", "contractdb_gc_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test_gc.db")

	// ContractDB 생성
	db, err := NewContractDBWithPath(dbPath)
	if err != nil {
		t.Fatalf("Failed to create ContractDB: %v", err)
	}
	defer db.Close()

	// 초기 연산 횟수 확인 (0이어야 함)
	initialCount := db.GetOperationCount()
	if initialCount != 0 {
		t.Errorf("Initial operation count should be 0, got %d", initialCount)
	}

	// 여러 연산을 통해 연산 횟수 증가 테스트
	contract := createTestContract("0x1234567890123456789012345678901234567890")
	
	// Put 연산 (1회)
	err = db.Put(contract)
	if err != nil {
		t.Fatalf("Failed to put contract: %v", err)
	}
	
	// Get 연산 (1회)
	_, err = db.Get(contract.Address)
	if err != nil {
		t.Fatalf("Failed to get contract: %v", err)
	}
	
	// IsContain 연산 (1회)
	_ = db.IsContain(contract.Address)
	
	// 연산 횟수 확인 (3이어야 함)
	opCount := db.GetOperationCount()
	if opCount != 3 {
		t.Errorf("Operation count should be 3, got %d", opCount)
	}

	// ForceGC 테스트 (에러가 발생하지 않으면 성공)
	err = db.ForceGC()
	if err != nil {
		// ErrNoRewrite나 "Value log GC attempt didn't result in any cleanup"는 
		// GC가 불필요한 경우이므로 무시
		if err.Error() != "no rewrite" && 
		   err.Error() != "Value log GC attempt didn't result in any cleanup" {
			t.Errorf("ForceGC failed with unexpected error: %v", err)
		}
	}
}
