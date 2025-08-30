package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	// 👇 경로는 프로젝트 구조에 맞게 조정
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/infra"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	sharedDomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

func main() {
	// === 경로 구성 ===
	rootDir := computation.ComputeThisTestingStorage("nod/co")
	dbPath := filepath.Join(rootDir, "deposit")

	csvPath := filepath.Join(
		computation.ComputeThisTestingStorage("feed_ingest_ee_test"),
		"test_detected_deposits.csv",
	)

	// === DB 오픈 ===
	depDb, err := infra.NewBadgerDepositRepository(mode.TestingModeProcess, dbPath, 256)
	if err != nil {
		panic(fmt.Errorf("open deposit badger: %w", err))
	}
	defer func() {
		_ = depDb.Close()
	}()

	// === CSV 오픈 ===
	f, err := os.Open(csvPath)
	if err != nil {
		panic(fmt.Errorf("open csv: %w", err))
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = 4 // address, detected_at, cex_address, tx_count

	// 헤더 스킵
	if _, err := r.Read(); err != nil {
		panic(fmt.Errorf("read header: %w", err))
	}

	var (
		n    int
		ok   int
		fail int
	)
	for {
		rec, err := r.Read()
		if errors.Is(err, os.ErrClosed) {
			break
		}
		if err != nil {
			// EOF 포함
			if err.Error() == "EOF" {
				break
			}
			fail++
			fmt.Printf("[WARN] read row failed: %v\n", err)
			continue
		}
		n++

		// Trim
		addrHex := strings.TrimSpace(rec[0])       // "0x...."
		detectedAtStr := strings.TrimSpace(rec[1]) // RFC3339 기대
		cexAddrHex := strings.TrimSpace(rec[2])    // "0x...."
		txCountStr := strings.TrimSpace(rec[3])    // int64

		// Address 파싱
		addr, err := sharedDomain.ParseAddressFromString(addrHex)
		if err != nil {
			fail++
			fmt.Printf("[WARN] bad address %q: %v\n", addrHex, err)
			continue
		}
		cexAddr, err := sharedDomain.ParseAddressFromString(cexAddrHex)
		if err != nil {
			fail++
			fmt.Printf("[WARN] bad cex_address %q: %v\n", cexAddrHex, err)
			continue
		}

		// detected_at 파싱 (RFC3339, 예: 0001-01-01T00:00:00Z)
		ct, err := parseChainTimeRFC3339(detectedAtStr)
		if err != nil {
			fail++
			fmt.Printf("[WARN] bad detected_at %q: %v\n", detectedAtStr, err)
			continue
		}

		// tx_count 파싱
		txCount, err := strconv.ParseInt(txCountStr, 10, 64)
		if err != nil {
			fail++
			fmt.Printf("[WARN] bad tx_count %q: %v\n", txCountStr, err)
			continue
		}

		// 도메인 객체 구성
		dd := &sharedDomain.DetectedDeposit{
			Address:    addr,
			DetectedAt: ct,
			CEXAddress: cexAddr,
			TxCount:    txCount,
		}

		// 저장 (신규/업데이트 모두 처리)
		if err := depDb.SaveDetectedDeposit(dd); err != nil {
			fail++
			fmt.Printf("[WARN] save failed for %s: %v\n", addr.String(), err)
			continue
		}
		ok++
	}

	fmt.Printf("done. rows=%d, ok=%d, fail=%d\n", n, ok, fail)
}

// parseChainTimeRFC3339: 프로젝트의 chaintimer 형식에 맞춰 RFC3339 문자열을 파싱한다.
// chaintimer 패키지 구현에 따라 아래 둘 중 하나를 사용해.
// 1) chaintimer가 time.Time alias/랩퍼일 경우: FromTime 같은 팩토리를 사용
// 2) 필드가 공개인 struct면 직접 할당
func parseChainTimeRFC3339(s string) (chaintimer.ChainTime, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return chaintimer.ChainTime{}, err
	}

	// -- 프로젝트 구현에 맞게 한 가지 선택 --
	// (A) 팩토리/헬퍼가 있을 때:
	// return chaintimer.FromTime(t), nil

	// (B) 공개 필드를 직접 할당해야 할 때(예시):
	return chaintimer.ChainTime(t), nil
}
