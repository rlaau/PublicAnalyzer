package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/nod/co/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

const (
	// mmap 레코드 포맷 (64B):
	// 0..19  from_address [20] - creator address
	// 20..27 nonce u64 (big-endian)
	// 28..47 receipt_contract_address [20] - contract address (key)
	// 48..55 block_timestamp_us i64
	// 56..63 pad [8]
	recSize = 64

	offFrom = 0  // from_address (creator)
	offNonc = 20 // nonce
	offRC   = 28 // receipt_contract_address (contract address)
	offTS   = 48 // timestamp
	offPad  = 56 // padding

	// 배치 처리 크기
	BATCH_SIZE = 10000
)

type CreationRecord struct {
	Creator         domain.Address // from_address
	Nonce           uint64
	ContractAddress domain.Address // receipt_contract_address
	TimestampUs     int64
}

func main() {
	// 프로덕션 모드로 ContractDB 열기
	log.Printf("Opening ContractDB in production mode...")
	db, err := app.NewContractDB(mode.ProductionModeProcess)
	if err != nil {
		log.Fatalf("Failed to open ContractDB: %v", err)
	}
	defer db.Close()

	// mmap 데이터 디렉토리
	mmapDir := "/home/rlaaudgjs5638/chainAnalyzer/internal/cce/cmd/load_null_to_and_contract/out_creations"

	// mmap 파일들 찾기
	paths, err := filepath.Glob(filepath.Join(mmapDir, "txc_*.mm"))
	if err != nil {
		log.Fatalf("Failed to glob mmap files: %v", err)
	}
	if len(paths) == 0 {
		log.Fatalf("No mmap files found in %s", mmapDir)
	}

	log.Printf("Found %d mmap files to process", len(paths))

	totalRecords := 0
	totalUpdated := 0
	startTime := time.Now()

	// 각 mmap 파일 처리
	for i, path := range paths {
		log.Printf("Processing file %d/%d: %s", i+1, len(paths), filepath.Base(path))

		records, updated, err := processFile(path, db)
		if err != nil {
			log.Printf("Error processing file %s: %v", path, err)
			continue
		}

		totalRecords += records
		totalUpdated += updated

		// GC 상태 출력
		opCount := db.GetOperationCount()
		log.Printf("File processed: records=%d, updated=%d, total_ops=%d",
			records, updated, opCount)
	}

	elapsed := time.Since(startTime)
	log.Printf("=== Summary ===")
	log.Printf("Total files processed: %d", len(paths))
	log.Printf("Total records processed: %d", totalRecords)
	log.Printf("Total contracts updated: %d", totalUpdated)
	log.Printf("Total time elapsed: %v", elapsed)
	log.Printf("Records per second: %.2f", float64(totalRecords)/elapsed.Seconds())
	log.Printf("Final operation count: %d", db.GetOperationCount())
}

func processFile(filePath string, db *app.ContractDB) (totalRecords, totalUpdated int, err error) {
	// 파일 정보 확인
	fi, err := os.Stat(filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("stat file: %w", err)
	}

	if fi.Size()%recSize != 0 {
		return 0, 0, fmt.Errorf("invalid file size, not multiple of %d", recSize)
	}

	numRecords := fi.Size() / recSize
	if numRecords == 0 {
		return 0, 0, nil
	}

	// mmap으로 파일 열기
	f, err := os.Open(filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	mem, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return 0, 0, fmt.Errorf("mmap file: %w", err)
	}
	defer mem.Unmap()

	// 배치 단위로 처리
	var wg sync.WaitGroup
	resultCh := make(chan int, (int(numRecords)/BATCH_SIZE)+1)
	errorCh := make(chan error, (int(numRecords)/BATCH_SIZE)+1)

	for offset := int64(0); offset < numRecords; offset += BATCH_SIZE {
		batchEnd := offset + BATCH_SIZE
		if batchEnd > numRecords {
			batchEnd = numRecords
		}

		wg.Add(1)
		go func(start, end int64) {
			defer wg.Done()

			updated, err := processBatch(mem, start, end, db)
			if err != nil {
				errorCh <- fmt.Errorf("batch %d-%d: %w", start, end, err)
				return
			}

			resultCh <- updated
		}(offset, batchEnd)
	}

	// 모든 배치 완료 대기
	go func() {
		wg.Wait()
		close(resultCh)
		close(errorCh)
	}()

	// 에러 확인
	for err := range errorCh {
		if err != nil {
			return int(numRecords), 0, err
		}
	}

	// 결과 집계
	for updated := range resultCh {
		totalUpdated += updated
	}

	return int(numRecords), totalUpdated, nil
}

func processBatch(mem []byte, start, end int64, db *app.ContractDB) (int, error) {
	batchSize := int(end - start)

	// 1. 배치에서 레코드 읽고 contract addresses 수집
	contractAddresses := make([]domain.Address, 0, batchSize)
	creationMap := make(map[domain.Address]domain.Address) // contract -> creator 매핑

	for i := start; i < end; i++ {
		record := readRecord(mem, i)

		// 유효한 contract address만 처리 (zero address 제외)
		if !domain.IsNullAddress(record.ContractAddress) {
			contractAddresses = append(contractAddresses, record.ContractAddress)
			creationMap[record.ContractAddress] = record.Creator
		}
	}

	if len(contractAddresses) == 0 {
		return 0, nil
	}

	// 2. BatchGet으로 기존 contracts 조회
	existingContracts, err := db.BatchGet(contractAddresses)
	if err != nil {
		return 0, fmt.Errorf("batch get contracts: %w", err)
	}

	// 3. Creator 정보 업데이트
	toUpdate := make([]domain.Contract, 0, len(existingContracts))

	for contractAddr, contract := range existingContracts {
		creator, exists := creationMap[contractAddr]
		if !exists {
			continue
		}

		// Creator가 zero address가 아니고, 기존 값과 다르면 업데이트
		if !domain.IsNullAddress(creator) && contract.CreatorOrZero != creator {
			contract.CreatorOrZero = creator
			toUpdate = append(toUpdate, contract)
		}
	}

	// 4. BatchPut으로 업데이트된 contracts 저장
	if len(toUpdate) > 0 {
		err = db.BatchPut(toUpdate)
		if err != nil {
			return 0, fmt.Errorf("batch put contracts: %w", err)
		}
	}

	return len(toUpdate), nil
}

func readRecord(mem []byte, idx int64) CreationRecord {
	base := idx * recSize

	var record CreationRecord

	// from_address (creator) - bytes [0:20]
	copy(record.Creator[:], mem[base+offFrom:base+offFrom+20])

	// nonce - uint64 big-endian [20:28]
	record.Nonce = binary.BigEndian.Uint64(mem[base+offNonc : base+offNonc+8])

	// receipt_contract_address - bytes [28:48]
	copy(record.ContractAddress[:], mem[base+offRC:base+offRC+20])

	// block_timestamp_us - int64 big-endian [48:56]
	record.TimestampUs = int64(binary.BigEndian.Uint64(mem[base+offTS : base+offTS+8]))

	return record
}

// 디버깅용 헬퍼 함수
func addressToHex(addr domain.Address) string {
	if domain.IsNullAddress(addr) {
		return "0x" + strings.Repeat("00", 20)
	}
	return "0x" + strings.ToLower(hex.EncodeToString(addr[:]))
}
