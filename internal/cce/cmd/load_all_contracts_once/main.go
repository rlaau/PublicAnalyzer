// main.go
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	cloudbq "cloud.google.com/go/bigquery"
)

/* ------------ Config ------------ */
type Config struct {
	ProjectID      string
	Location       string
	StartUTC       time.Time
	EndUTC         time.Time
	BatchLimit     int64
	MaxBytesBilled int64
	OutputDir      string
	FilePerm       os.FileMode
	FlushEveryN    int
	CutoffUTC      time.Time // 보통 EndUTC와 동일
	TenMinStep     time.Duration
}

/* ------------ File sink (4개월 롤링) ------------ */
type fileSink struct {
	fw   *os.File
	bw   *bufio.Writer
	path string
	n    int
}

func openSink(dir, name string, perm os.FileMode) (*fileSink, error) {
	if err := os.MkdirAll(dir, perm); err != nil {
		return nil, err
	}
	full := filepath.Join(dir, name)
	f, err := os.OpenFile(full, os.O_CREATE|os.O_WRONLY|os.O_APPEND, perm)
	if err != nil {
		return nil, err
	}
	return &fileSink{fw: f, bw: bufio.NewWriterSize(f, 1<<20), path: full}, nil
}
func (s *fileSink) WriteJSONL(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := s.bw.Write(b); err != nil {
		return err
	}
	if err := s.bw.WriteByte('\n'); err != nil {
		return err
	}
	s.n++
	return nil
}
func (s *fileSink) Flush() error { return s.bw.Flush() }
func (s *fileSink) Close() error { _ = s.bw.Flush(); return s.fw.Close() }

/* ------------ 4개월 윈도우 & 파일명 ------------ */
// 주어진 시점 t가 속한 4개월 구간의 [start, end) 반환
func fourMonthRange(t time.Time) (time.Time, time.Time) {
	m := ((int(t.Month())-1)/4)*4 + 1 // 1,5,9,13(=다음해1월)
	start := time.Date(t.Year(), time.Month(m), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 4, 0) // exclusive
	return start, end
}

// 파일명: kind-{start}_{endInclusive}.jsonl  (endInclusive = end-1d)
func fileNameFor(kind string, t time.Time) string {
	fs, fe := fourMonthRange(t)
	endInclusive := fe.Add(-24 * time.Hour)
	return fmt.Sprintf("%s-%04d-%02d-%02d_%04d-%02d-%02d.jsonl",
		kind,
		fs.Year(), fs.Month(), fs.Day(),
		endInclusive.Year(), endInclusive.Month(), endInclusive.Day(),
	)
}

/* ------------ Dry-run utils ------------ */
const WarnWideScan int64 = 50 * 1024 * 1024 * 1024 // 50 GB

func estimateBytes(ctx context.Context, q *cloudbq.Query) (int64, error) {
	if q.Location == "" {
		q.Location = "US"
	}
	cfg := q.QueryConfig
	cfg.DryRun = true
	q.QueryConfig = cfg

	job, err := q.Run(ctx)
	if err != nil {
		return 0, err
	}
	st := job.LastStatus()
	if st == nil {
		var err2 error
		st, err2 = job.Status(ctx)
		if err2 != nil {
			return 0, err2
		}
	}
	if err := st.Err(); err != nil {
		return 0, err
	}
	stats, ok := st.Statistics.Details.(*cloudbq.QueryStatistics)
	if !ok {
		return 0, fmt.Errorf("no query statistics")
	}
	return int64(stats.TotalBytesProcessed), nil
}

func human(b int64) string {
	const KB = 1024
	const MB = KB * 1024
	const GB = MB * 1024
	const TB = GB * 1024
	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
func mustParseUTC(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

/* ------------ SQL ------------ */
/* 토큰: 시간 범위 + (timestamp,address) keyset */
func buildTokensSQL() string {
	return `
SELECT
  LOWER(address) AS address,
  symbol,
  name,
  SAFE_CAST(decimals AS INT64) AS decimals,
  block_timestamp
FROM ` + "`bigquery-public-data.crypto_ethereum.tokens`" + `
WHERE block_timestamp >= @start_ts AND block_timestamp < @end_ts
  AND ( @has_ckpt = FALSE
        OR block_timestamp > @last_ts
        OR (block_timestamp = @last_ts AND LOWER(address) > @last_addr) )
ORDER BY block_timestamp ASC, address ASC
LIMIT @lim
`
}

func buildContractsSQL() string {
	return `
WITH slice_contracts AS (
  SELECT 
    LOWER(address) AS address, 
    block_timestamp,
    block_number,
    is_erc20, 
    is_erc721
  FROM ` + "`bigquery-public-data.crypto_ethereum.contracts`" + `
  WHERE block_timestamp >= @start_ts AND block_timestamp < @end_ts
    AND ( @has_ckpt = FALSE
       OR block_timestamp > @last_ts
       OR (block_timestamp = @last_ts AND LOWER(address) > @last_addr) )
),
creation_tx AS (
  SELECT
    sc.address,
    LOWER(t.from_address) AS creator,
    LOWER(t.hash) AS creation_tx,
    sc.block_timestamp AS created_at  -- 이미 정확한 시간
  FROM slice_contracts sc
  INNER JOIN ` + "`bigquery-public-data.crypto_ethereum.transactions`" + ` t
    ON LOWER(t.receipt_contract_address) = sc.address
    AND t.block_number = sc.block_number
    -- 핵심: timestamp 범위로 파티션 프루닝
    AND t.block_timestamp BETWEEN 
        TIMESTAMP_SUB(sc.block_timestamp, INTERVAL 1 MINUTE) 
        AND TIMESTAMP_ADD(sc.block_timestamp, INTERVAL 1 MINUTE)
)
SELECT
  sc.address,
  COALESCE(ct.creator, '') AS creator,
  COALESCE(ct.creation_tx, '') AS creation_tx,
  COALESCE(ct.created_at, sc.block_timestamp) AS created_at,
  sc.is_erc20,
  sc.is_erc721,
  sc.block_timestamp AS slice_block_ts
FROM slice_contracts sc
LEFT JOIN creation_tx ct USING(address)
ORDER BY sc.block_timestamp ASC, sc.address ASC
LIMIT @lim
`
}

// /* ------------ Extractors ------------ */
// // 토큰: 월(月) 단위 슬라이스 → 파일은 4개월 단위로 롤링(지연 오픈)
// func extractTokensMonthly(ctx context.Context, bq *cloudbq.Client, cfg Config) error {
// 	// 현재 오픈된 4개월 롤링 파일 핸들
// 	var sink *fileSink
// 	var sinkPath string
// 	defer func() {
// 		if sink != nil {
// 			_ = sink.Close()
// 		}
// 	}()

// 	for monthStart := time.Date(cfg.StartUTC.Year(), cfg.StartUTC.Month(), 1, 0, 0, 0, 0, time.UTC); monthStart.Before(cfg.EndUTC); monthStart = monthStart.AddDate(0, 1, 0) {

// 		monthEnd := monthStart.AddDate(0, 1, 0)
// 		if monthEnd.After(cfg.EndUTC) {
// 			monthEnd = cfg.EndUTC
// 		}
// 		log.Printf("=== TOKENS SLICE (MONTH): %s ~ %s ===", monthStart, monthEnd)

// 		var hasCkpt bool
// 		var lastTS time.Time
// 		var lastAddr string

// 		for {
// 			// Dry-run
// 			q := bq.Query(buildTokensSQL())
// 			q.Location = cfg.Location
// 			q.Parameters = []cloudbq.QueryParameter{
// 				{Name: "start_ts", Value: monthStart},
// 				{Name: "end_ts", Value: monthEnd},
// 				{Name: "lim", Value: cfg.BatchLimit},
// 				{Name: "has_ckpt", Value: hasCkpt},
// 				{Name: "last_ts", Value: lastTS},
// 				{Name: "last_addr", Value: lastAddr},
// 			}
// 			if cfg.MaxBytesBilled > 0 {
// 				q.QueryConfig.MaxBytesBilled = cfg.MaxBytesBilled
// 			}
// 			est, err := estimateBytes(ctx, q)
// 			if err != nil {
// 				return fmt.Errorf("tokens dry-run: %w", err)
// 			}
// 			log.Printf("[SCAN][TOKENS][%s~%s] %s", monthStart, monthEnd, human(est))
// 			if cfg.MaxBytesBilled > 0 && est > cfg.MaxBytesBilled {
// 				return fmt.Errorf("tokens dry-run exceeds limit: need %s > limit %s",
// 					human(est), human(cfg.MaxBytesBilled))
// 			}

// 			// 실행
// 			qExec := bq.Query(buildTokensSQL())
// 			qExec.Location = cfg.Location
// 			qExec.Parameters = q.Parameters
// 			if cfg.MaxBytesBilled > 0 {
// 				qExec.QueryConfig.MaxBytesBilled = cfg.MaxBytesBilled
// 			}
// 			it, err := qExec.Read(ctx)
// 			if err != nil {
// 				return fmt.Errorf("tokens read: %w", err)
// 			}

// 			type trow struct {
// 				Address string             `bigquery:"address"`
// 				Symbol  cloudbq.NullString `bigquery:"symbol"`
// 				Name    cloudbq.NullString `bigquery:"name"`
// 				Dec     cloudbq.NullInt64  `bigquery:"decimals"`
// 				TS      time.Time          `bigquery:"block_timestamp"`
// 			}

// 			rows := 0
// 			for {
// 				var r trow
// 				if err := it.Next(&r); err == iterator.Done {
// 					break
// 				} else if err != nil {
// 					return fmt.Errorf("tokens iter: %w", err)
// 				}

// 				// 4개월 롤링 파일 경로 계산 (월 슬라이스의 시작시각 기준)
// 				target := fileNameFor("tokens", monthStart)
// 				if sink == nil || sinkPath != target {
// 					if sink != nil {
// 						_ = sink.Close()
// 					}
// 					newSink, err := openSink(cfg.OutputDir, target, cfg.FilePerm)
// 					if err != nil {
// 						return err
// 					}
// 					sink, sinkPath = newSink, target
// 					log.Printf("[file] tokens -> %s", filepath.Join(cfg.OutputDir, sinkPath))
// 				}

// 				var decPtr *int64
// 				if r.Dec.Valid {
// 					v := r.Dec.Int64
// 					decPtr = &v
// 				}
// 				rec := domain.RawToken{
// 					Address:  strings.ToLower(r.Address),
// 					Symbol:   r.Symbol.StringVal,
// 					Name:     r.Name.StringVal,
// 					Decimals: decPtr,
// 				}
// 				if err := sink.WriteJSONL(rec); err != nil {
// 					return err
// 				}
// 				rows++

// 				lastTS = r.TS
// 				lastAddr = strings.ToLower(r.Address)
// 				if cfg.FlushEveryN > 0 && (sink.n%cfg.FlushEveryN == 0) {
// 					_ = sink.Flush()
// 				}
// 			}

// 			log.Printf("[TOKENS][%04d-%02d] batch wrote: %d", monthStart.Year(), monthStart.Month(), rows)
// 			if rows == 0 || int64(rows) < cfg.BatchLimit {
// 				break // 이 월 슬라이스 완료
// 			}
// 			hasCkpt = true
// 		}
// 	}
// 	log.Printf("=== TOKENS DONE (MONTHLY, 4-month rolling files) ===")
// 	return nil
// }

// func main() {
// 	cfg := Config{
// 		ProjectID:      "chain-analyzer-eth-1754795549",
// 		Location:       "US",
// 		StartUTC:       mustParseUTC("2015-07-30T00:00:00Z"),
// 		EndUTC:         mustParseUTC("2025-01-01T00:00:00Z"),
// 		BatchLimit:     1_000_000,               // 배치 페이지네이션 크기
// 		MaxBytesBilled: 10 * 1024 * 1024 * 1024, //10 GB 예시
// 		OutputDir:      "./out_jsonl",
// 		FilePerm:       0o755,
// 		FlushEveryN:    10_000,
// 		CutoffUTC:      mustParseUTC("2025-01-01T00:00:00Z"),
// 		TenMinStep:     10 * time.Minute,
// 	}

// 	ctx := context.Background()
// 	bq, err := cloudbq.NewClient(ctx, cfg.ProjectID)
// 	if err != nil {
// 		log.Fatalf("bq client: %v", err)
// 	}
// 	defer bq.Close()
// 	if cfg.Location != "" {
// 		bq.Location = cfg.Location
// 	}

// 	// // 1) 토큰: 월 단위 슬라이스, 파일은 4개월 롤링
// 	// if err := extractTokensMonthly(ctx, bq, cfg); err != nil {
// 	// 	log.Fatalf("TOKENS: %v", err)
// 	// }

// }
