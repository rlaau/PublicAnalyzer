// main.go
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	cloudbq "cloud.google.com/go/bigquery"
	mmap "github.com/edsrzf/mmap-go"
	"google.golang.org/api/iterator"
)

/*
Config
*/
type Config struct {
	ProjectID  string
	Location   string // "US"
	SrcTable   string // e.g. "chain-analyzer-eth-1754795549.precomp.legacy_tx_with_contract_partitioned"
	OutDir     string // "./out"
	Workers    int
	FromDate   string // "2023-09-01"
	ToDateExcl string // "2025-01-01"
	MaxBytes   int64  // 0 = unlimited
	Stage1     bool   // fetch (BQ -> raw .mm)
	Stage2     bool   // sort (raw .mm -> sorted .mm, second-level order)
}

/*
고정 길이 레코드 포맷 (160 bytes, Big-Endian)

	0  .. 31   tx_hash [32]
	32 .. 39   nonce u64 [8]
	40 .. 59   from [20]
	60 .. 79   to [20] (null -> zero)
	80 .. 99   receipt_contract_address [20] (null -> zero)

100 .. 131  value_wei u256 [32]
132 .. 139  block_timestamp_us i64 [8]
140 .. 147  block_number u64 [8]   (이번 버전은 0으로 채움)
148 .. 151  transaction_index u32 [4] (이번 버전은 0으로 채움)
152 .. 159  pad [8]
*/
const (
	recSize = 160

	offHash  = 0
	offNonce = 32
	offFrom  = 40
	offTo    = 60
	offRC    = 80
	offVal   = 100
	offTS    = 132
	offBN    = 140
	offTXI   = 148
	offPad   = 152
)

// BigQuery에서 읽을 행 (7개 컬럼만; value는 STRING으로 캐스팅해서 받음)
type bqRow struct {
	Hash   string             `bigquery:"hash"`
	Nonce  int64              `bigquery:"nonce"`
	From   string             `bigquery:"from_address"`
	To     cloudbq.NullString `bigquery:"to_address"`
	Value  cloudbq.NullString `bigquery:"value"` // CAST(value AS STRING)
	TS     time.Time          `bigquery:"block_timestamp"`
	RCAddr cloudbq.NullString `bigquery:"receipt_contract_address"`
}

func main() {
	cfg := Config{
		ProjectID:  "chain-analyzer-eth-1754795549",
		Location:   "US",
		SrcTable:   "chain-analyzer-eth-1754795549.precomp.legacy_tx_with_contract_partitioned",
		OutDir:     "./out",
		Workers:    max(32, runtime.NumCPU()),
		FromDate:   "2023-09-01",
		ToDateExcl: "2025-01-01",
		MaxBytes:   0,
		Stage1:     true, // BQ -> raw .mm
		Stage2:     true, // raw .mm -> sorted .mm
	}

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg Config) error {
	if err := os.MkdirAll(filepath.Join(cfg.OutDir, "raw"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(cfg.OutDir, "sorted"), 0o755); err != nil {
		return err
	}

	ctx := context.Background()
	bq, err := cloudbq.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("bq client: %w", err)
	}
	defer bq.Close()
	if cfg.Location != "" {
		bq.Location = cfg.Location
	}

	days := enumerateDays(cfg.FromDate, cfg.ToDateExcl)

	if cfg.Stage1 {
		if err := stage1Fetch(ctx, bq, cfg, days); err != nil {
			return fmt.Errorf("stage1: %w", err)
		}
	}
	if cfg.Stage2 {
		if err := stage2Sort(cfg, days); err != nil {
			return fmt.Errorf("stage2: %w", err)
		}
	}
	return nil
}

/* ---------------- Stage 1: 병렬 수집 (무정렬, mmap-friendly 이진) ---------------- */

// 주의: `hash`는 예약어 충돌 → 반드시 백틱으로 감싼다.
// value는 NUMERIC → STRING으로 캐스팅해서 받는다.
const daySQL = "SELECT\n" +
	"  `hash`, nonce, from_address, to_address,\n" +
	"  CAST(value AS STRING) AS value,\n" +
	"  block_timestamp, receipt_contract_address\n" +
	"FROM %s\n" +
	"WHERE DATE(block_timestamp) = @d\n"

func stage1Fetch(ctx context.Context, bq *cloudbq.Client, cfg Config, days []string) error {
	type job struct{ day string }
	jobs := make(chan job, len(days))
	errs := make(chan error, cfg.Workers)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := dumpDay(ctx, bq, cfg, j.day); err != nil {
					errs <- fmt.Errorf("day %s: %w", j.day, err)
					return
				}
				errs <- nil
			}
		}()
	}
	for _, d := range days {
		jobs <- job{day: d}
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(errs)
	}()

	for e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func dumpDay(ctx context.Context, bq *cloudbq.Client, cfg Config, day string) error {
	out := filepath.Join(cfg.OutDir, "raw", fmt.Sprintf("tx_%s.mm", day))
	if _, err := os.Stat(out); err == nil {
		log.Printf("[day %s] exists, skip", day)
		return nil
	}

	log.Printf("[day %s] fetch...", day)
	q := bq.Query(fmt.Sprintf(daySQL, cfg.SrcTable))
	q.Location = cfg.Location
	q.Parameters = []cloudbq.QueryParameter{{Name: "d", Value: day}}
	if cfg.MaxBytes > 0 {
		q.QueryConfig.MaxBytesBilled = cfg.MaxBytes
	}

	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	// 큰 버퍼로 batched write
	buf := make([]byte, 0, 8<<20) // 8MB
	total := 0

	for {
		var r bqRow
		if err := it.Next(&r); err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		rec, err := encodeRecord(r)
		if err != nil {
			return err
		}
		buf = append(buf, rec...)

		if len(buf) >= cap(buf) {
			if _, err := f.Write(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
		total++
	}
	if len(buf) > 0 {
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}

	log.Printf("[day %s] wrote rows=%d file=%s", day, total, out)
	return nil
}

func encodeRecord(r bqRow) ([]byte, error) {
	rec := make([]byte, recSize)

	// tx hash (32)
	if err := writeHexFixed(rec[offHash:offHash+32], r.Hash, 32); err != nil {
		return nil, fmt.Errorf("hash: %w", err)
	}
	// nonce u64
	binary.BigEndian.PutUint64(rec[offNonce:offNonce+8], uint64(r.Nonce))

	// from (20)
	if err := writeHexFixed(rec[offFrom:offFrom+20], r.From, 20); err != nil {
		return nil, fmt.Errorf("from: %w", err)
	}
	// to (20) or zero
	if r.To.Valid {
		if err := writeHexFixed(rec[offTo:offTo+20], r.To.StringVal, 20); err != nil {
			return nil, fmt.Errorf("to: %w", err)
		}
	} else {
		zero(rec[offTo : offTo+20])
	}
	// receipt_contract_address (20) or zero
	if r.RCAddr.Valid {
		if err := writeHexFixed(rec[offRC:offRC+20], r.RCAddr.StringVal, 20); err != nil {
			return nil, fmt.Errorf("rcaddr: %w", err)
		}
	} else {
		zero(rec[offRC : offRC+20])
	}
	// value (STRING decimal -> u256 32B)
	if r.Value.Valid {
		u256, err := decimalToU256(r.Value.StringVal)
		if err != nil {
			return nil, fmt.Errorf("value: %w", err)
		}
		copy(rec[offVal:offVal+32], u256[:])
	} else {
		zero(rec[offVal : offVal+32])
	}
	// block_timestamp_us i64
	tsUs := r.TS.UTC().UnixMicro()
	binary.BigEndian.PutUint64(rec[offTS:offTS+8], uint64(tsUs))

	// block_number u64 (이번 버전은 0)
	binary.BigEndian.PutUint64(rec[offBN:offBN+8], 0)
	// transaction_index u32 (이번 버전은 0)
	binary.BigEndian.PutUint32(rec[offTXI:offTXI+4], 0)

	// pad
	zero(rec[offPad : offPad+8])
	return rec, nil
}

func writeHexFixed(dst []byte, hexStr string, want int) error {
	s := strings.TrimPrefix(strings.ToLower(hexStr), "0x")
	b, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	if len(b) > want {
		// 과길이 안전 처리(정상적이면 안 옴)
		b = b[len(b)-want:]
	}
	// left-pad zeros
	for i := 0; i < want-len(b); i++ {
		dst[i] = 0x00
	}
	copy(dst[want-len(b):], b)
	return nil
}

func decimalToU256(dec string) ([32]byte, error) {
	var out [32]byte
	n := new(big.Int)
	_, ok := n.SetString(dec, 10)
	if !ok || n.Sign() < 0 {
		return out, fmt.Errorf("invalid decimal %q", dec)
	}
	b := n.Bytes()
	if len(b) > 32 {
		return out, errors.New("overflow u256")
	}
	copy(out[32-len(b):], b)
	return out, nil
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

/* ---------------- Stage 2: 초 단위 외부 정렬 (카운팅 정렬, mmap) ---------------- */

func stage2Sort(cfg Config, days []string) error {
	type job struct{ day string }
	jobs := make(chan job, len(days))
	errs := make(chan error, cfg.Workers)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := sortOneDay(cfg, j.day); err != nil {
					errs <- fmt.Errorf("day %s: %w", j.day, err)
					return
				}
				errs <- nil
			}
		}()
	}
	for _, d := range days {
		jobs <- job{day: d}
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(errs)
	}()
	for e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func sortOneDay(cfg Config, day string) error {
	in := filepath.Join(cfg.OutDir, "raw", fmt.Sprintf("tx_%s.mm", day))
	out := filepath.Join(cfg.OutDir, "sorted", fmt.Sprintf("tx_%s.mm", day))

	fi, err := os.Stat(in)
	if os.IsNotExist(err) {
		log.Printf("[sort %s] skip (no input)", day)
		return nil
	}
	if err != nil {
		return err
	}
	if fi.Size()%recSize != 0 {
		return fmt.Errorf("broken input %s: size%%recSize != 0", in)
	}
	n := fi.Size() / recSize
	if n == 0 {
		log.Printf("[sort %s] empty, skip", day)
		return nil
	}

	// map input R/O
	fin, err := os.Open(in)
	if err != nil {
		return err
	}
	defer fin.Close()
	inMap, err := mmap.Map(fin, mmap.RDONLY, 0)
	if err != nil {
		return err
	}
	defer inMap.Unmap()

	// Pass 1: 초별 카운팅 (0..86399)
	var counts [86400]uint64
	dayStart, err := time.Parse("2006-01-02", day)
	if err != nil {
		return err
	}
	dayStart = dayStart.UTC()
	dayStartUs := dayStart.UnixMicro()

	for i := int64(0); i < n; i++ {
		base := i * recSize
		tsUs := int64(binary.BigEndian.Uint64(inMap[base+offTS : base+offTS+8]))
		sec := int((tsUs - dayStartUs) / 1_000_000)
		if sec < 0 {
			sec = 0
		} else if sec >= 86400 {
			sec = 86399
		}
		counts[sec]++
	}

	// prefix sum -> 시작 인덱스
	var startIdx [86400]uint64
	var acc uint64
	for s := 0; s < 86400; s++ {
		startIdx[s] = acc
		acc += counts[s]
	}
	total := acc // == n

	// 출력 파일 allocate & map R/W
	fout, err := os.OpenFile(out, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer fout.Close()
	if err := fout.Truncate(int64(total) * recSize); err != nil {
		return err
	}
	outMap, err := mmap.Map(fout, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer func() {
		_ = outMap.Flush()
		_ = outMap.Unmap()
	}()

	// 초별 커서
	var cursor [86400]uint64

	// Pass 2: 배치
	for i := int64(0); i < n; i++ {
		base := i * recSize
		tsUs := int64(binary.BigEndian.Uint64(inMap[base+offTS : base+offTS+8]))
		sec := int((tsUs - dayStartUs) / 1_000_000)
		if sec < 0 {
			sec = 0
		} else if sec >= 86400 {
			sec = 86399
		}
		idx := startIdx[sec] + cursor[sec]
		cursor[sec]++

		dst := int64(idx) * recSize
		copy(outMap[dst:dst+recSize], inMap[base:base+recSize])
	}

	log.Printf("[sort %s] done -> %s (rows=%d)", day, out, total)

	// 공간 회수 원하면 원본 삭제:
	// _ = os.Remove(in)

	return nil
}

/* ---------------- utils ---------------- */

func enumerateDays(from, toExcl string) []string {
	const layout = "2006-01-02"
	st, _ := time.Parse(layout, from)
	en, _ := time.Parse(layout, toExcl)
	st = st.UTC()
	en = en.UTC()
	var out []string
	for d := st; d.Before(en); d = d.AddDate(0, 0, 1) {
		out = append(out, d.Format(layout))
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
