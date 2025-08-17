// main.go
package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	cloudbq "cloud.google.com/go/bigquery"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"
	"google.golang.org/api/iterator"
)

/*
생성 트xn 맵(보수적): 4개월 범위, 블록당 생성자가 유일한 경우, 즉 확정 가능 경우 시에만 채택
  - receipt_contract_address IS NOT NULL
  - to_address IS NULL 또는 zero address
  - rn=1 AND cnt=1 → creator/creation_tx/created_at 확정
*/
func buildCreationTx4MoMapSQL() string {
	return `
WITH cand AS (
  SELECT
    block_number,
    LOWER(from_address) AS creator,
    LOWER(` + "`hash`" + `) AS creation_tx,   -- 'hash'는 예약어 → 백틱 필수
    block_timestamp     AS created_at,
    transaction_index
  FROM ` + "`bigquery-public-data.crypto_ethereum.transactions`" + `
  WHERE block_timestamp >= @start_ts AND block_timestamp < @end_ts
    AND receipt_contract_address IS NOT NULL
    AND (to_address IS NULL
         OR to_address = '0x0000000000000000000000000000000000000000')
),
-- 블록별로 creator가 유일한지: MIN=MAX면 distinct=1과 동치
unique_blocks AS (
  SELECT block_number
  FROM (
    SELECT block_number,
           MIN(creator) AS min_c,
           MAX(creator) AS max_c
    FROM cand
    GROUP BY block_number
  )
  WHERE min_c = max_c
),
-- 유일 creator인 블록에서 가장 이른 tx 1건만 뽑기
ranked AS (
  SELECT
    c.block_number,
    c.creator,
    c.creation_tx,
    c.created_at,
    ROW_NUMBER() OVER (
      PARTITION BY c.block_number
      ORDER BY c.transaction_index ASC, c.creation_tx ASC
    ) AS rn
  FROM cand c
  JOIN unique_blocks u USING (block_number)
)
SELECT
  block_number,
  creator,
  creation_tx,
  created_at
FROM ranked
WHERE rn = 1
  AND ( @has_ckpt = FALSE OR block_number > @last_block )
ORDER BY block_number ASC
LIMIT @lim
`
}

/*
PASS 2: 4개월 범위의 contracts 원시 행(creator 제외)
  - 키셋 페이지네이션: (block_timestamp, address)
*/
func buildContractsBare4MoSQL() string {
	return `
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
ORDER BY block_timestamp ASC, address ASC
LIMIT @lim
`
}

/* ------------ SCAN SUMMARY ------------ */
type scanStats struct {
	TotalEstimated int64
	WideSlices     int
}

// 함수 상단(또는 파일 상단)에 추가/유지
type creatorInfo struct {
	Creator    string
	CreationTx string
	CreatedAt  time.Time
}

// fourMonthStart returns the UTC beginning of the 4-month window that contains t.
// Windows are: [Jan–Apr), [May–Aug), [Sep–Dec+next Jan).
func fourMonthStart(t time.Time) time.Time {
	t = t.UTC()
	startMonth := ((int(t.Month())-1)/4)*4 + 1 // 1,5,9
	return time.Date(t.Year(), time.Month(startMonth), 1, 0, 0, 0, 0, time.UTC)
}

/* ------------ Contracts: 4개월 창 Two-Pass ------------ */
func extractContractsFourMonthMerge(ctx context.Context, bq *cloudbq.Client, cfg Config) error {
	nowUTC := time.Now().UTC()
	stats := &scanStats{}

	// 4개월 윈도우 반복
	for winStart := fourMonthStart(cfg.StartUTC); winStart.Before(cfg.EndUTC); winStart = winStart.AddDate(0, 4, 0) {
		winEnd := winStart.AddDate(0, 4, 0)
		if winEnd.After(cfg.EndUTC) {
			winEnd = cfg.EndUTC
		}

		log.Printf("=== WINDOW (4mo): %s ~ %s ===", winStart, winEnd)

		/* --- PASS 1: 생성 트xn 맵 적재 (block_number -> creator) --- */
		type mapRow struct {
			Block      int64     `bigquery:"block_number"`
			Creator    string    `bigquery:"creator"`
			CreationTx string    `bigquery:"creation_tx"`
			CreatedAt  time.Time `bigquery:"created_at"`
		}
		creMap := make(map[int64]creatorInfo, 1<<20)

		var lastBlock int64
		var hasCkptMap bool
		for {
			q := bq.Query(buildCreationTx4MoMapSQL())
			q.Location = cfg.Location
			q.Parameters = []cloudbq.QueryParameter{
				{Name: "start_ts", Value: winStart},
				{Name: "end_ts", Value: winEnd},
				{Name: "lim", Value: cfg.BatchLimit},
				{Name: "has_ckpt", Value: hasCkptMap},
				{Name: "last_block", Value: lastBlock},
			}
			if cfg.MaxBytesBilled > 0 {
				q.QueryConfig.MaxBytesBilled = cfg.MaxBytesBilled
			}

			est, err := estimateBytes(ctx, q)
			if err != nil {
				return fmt.Errorf("creation-map dry-run: %w", err)
			}
			log.Printf("[SCAN][CREMAP][%s~%s] %s", winStart, winEnd, human(est))
			stats.TotalEstimated += est
			if est > WarnWideScan {
				stats.WideSlices++
			}

			qExec := bq.Query(buildCreationTx4MoMapSQL())
			qExec.Location = cfg.Location
			qExec.Parameters = q.Parameters
			if cfg.MaxBytesBilled > 0 {
				qExec.QueryConfig.MaxBytesBilled = cfg.MaxBytesBilled
			}

			it, err := qExec.Read(ctx)
			if err != nil {
				return fmt.Errorf("creation-map read: %w", err)
			}

			rows := 0
			for {
				var r mapRow
				if err := it.Next(&r); err == iterator.Done {
					break
				} else if err != nil {
					return fmt.Errorf("creation-map iter: %w", err)
				}
				if _, ok := creMap[r.Block]; !ok {
					creMap[r.Block] = creatorInfo{
						Creator:    strings.ToLower(r.Creator),
						CreationTx: strings.ToLower(r.CreationTx),
						CreatedAt:  r.CreatedAt,
					}
				}
				lastBlock = r.Block
				rows++
			}
			log.Printf("[CREMAP] +%d (total %d)", rows, len(creMap))
			if rows == 0 || int64(rows) < cfg.BatchLimit {
				break
			}
			hasCkptMap = true
		}

		/* --- PASS 2: contracts 스트리밍 + 메모리 맵 매칭 → 4개월 파일 저장 --- */
		var sink *fileSink
		var sinkPath string
		defer func() {
			if sink != nil {
				_ = sink.Close()
			}
		}()

		var hasCkpt bool
		var lastTS time.Time
		var lastAddr string

		for {
			q := bq.Query(buildContractsBare4MoSQL())
			q.Location = cfg.Location
			q.Parameters = []cloudbq.QueryParameter{
				{Name: "start_ts", Value: winStart},
				{Name: "end_ts", Value: winEnd},
				{Name: "lim", Value: cfg.BatchLimit},
				{Name: "has_ckpt", Value: hasCkpt},
				{Name: "last_ts", Value: lastTS},
				{Name: "last_addr", Value: lastAddr},
			}
			if cfg.MaxBytesBilled > 0 {
				q.QueryConfig.MaxBytesBilled = cfg.MaxBytesBilled
			}

			est, err := estimateBytes(ctx, q)
			if err != nil {
				return fmt.Errorf("contracts dry-run: %w", err)
			}
			log.Printf("[SCAN][CONTRACTS][%s~%s] %s", winStart, winEnd, human(est))
			stats.TotalEstimated += est
			if est > WarnWideScan {
				stats.WideSlices++
			}

			qExec := bq.Query(buildContractsBare4MoSQL())
			qExec.Location = cfg.Location
			qExec.Parameters = q.Parameters
			if cfg.MaxBytesBilled > 0 {
				qExec.QueryConfig.MaxBytesBilled = cfg.MaxBytesBilled
			}

			it, err := qExec.Read(ctx)
			if err != nil {
				return fmt.Errorf("contracts read: %w", err)
			}

			type cRow struct {
				Address     string    `bigquery:"address"`
				BlockTS     time.Time `bigquery:"block_timestamp"`
				BlockNumber int64     `bigquery:"block_number"`
				IsERC20     bool      `bigquery:"is_erc20"`
				IsERC721    bool      `bigquery:"is_erc721"`
			}

			rows := 0
			for {
				var r cRow
				if err := it.Next(&r); err == iterator.Done {
					break
				} else if err != nil {
					return fmt.Errorf("contracts iter: %w", err)
				}

				// 4개월 롤링 파일 (지연 오픈)
				target := fileNameFor("contracts", winStart)
				if sink == nil || sinkPath != target {
					if sink != nil {
						_ = sink.Close()
					}
					ns, err := openSink(cfg.OutputDir, target, cfg.FilePerm)
					if err != nil {
						return err
					}
					sink, sinkPath = ns, target
					log.Printf("[file] contracts -> %s", filepath.Join(cfg.OutputDir, sinkPath))
				}

				var (
					creatorPtr    *string
					creationTxPtr *string
					createdAtPtr  *chaintimer.ChainTime
				)

				if info, ok := creMap[r.BlockNumber]; ok {
					// 메모리 딕셔너리에서 크리에이터 추론
					c := strings.ToLower(info.Creator)
					creatorPtr = &c

					tx := strings.ToLower(info.CreationTx)
					creationTxPtr = &tx

					ct := chaintimer.ChainTime(info.CreatedAt)
					createdAtPtr = &ct
				} else {
					// 확정 불가 → 보수적 처리 (creator=nil, creationTx 생략, createdAt은 컨트랙트 블록 시각)
					ct := chaintimer.ChainTime(r.BlockTS)
					createdAtPtr = &ct
				}

				rc := domain.RawContract{
					Address:    strings.ToLower(r.Address),
					Creator:    creatorPtr,    // nil이면 생략
					CreationTx: creationTxPtr, // 빈 문자열이면 omitempty로 생략
					CreatedAt:  createdAtPtr,  // 확정 tx가 있으면 그 ts, 없으면 블록 ts
					IsERC20:    r.IsERC20,
					IsERC721:   r.IsERC721,
					LastSeenAt: chaintimer.ChainTime(nowUTC),
				}
				if err := sink.WriteJSONL(rc); err != nil {
					return err
				}
				rows++

				lastTS = r.BlockTS
				lastAddr = strings.ToLower(r.Address)
				if cfg.FlushEveryN > 0 && (sink.n%cfg.FlushEveryN == 0) {
					_ = sink.Flush()
				}
			}
			log.Printf("[CONTRACTS] batch wrote: %d", rows)
			if rows == 0 || int64(rows) < cfg.BatchLimit {
				break
			}
			hasCkpt = true
		}

		// 창 끝날 때 파일 닫기
		if sink != nil {
			_ = sink.Close()
		}
	}

	log.Printf("[SCAN][SUMMARY] total_estimated=%s, wide_slices=%d",
		human(stats.TotalEstimated), stats.WideSlices)

	return nil
}

/* ------------ main ------------ */
func main() {
	cfg := Config{
		ProjectID:      "chain-analyzer-eth-1754795549",
		Location:       "US",
		StartUTC:       mustParseUTC("2024-05-01T00:00:00Z"), //mustParseUTC("2015-01-30T00:00:00Z"),
		EndUTC:         mustParseUTC("2025-01-01T00:00:00Z"),
		BatchLimit:     100_000,                       // 5k~50k 조절
		MaxBytesBilled: 2 * 1024 * 1024 * 1024 * 1024, // 2 TiB 예시
		OutputDir:      "./out_jsonl",
		FilePerm:       0o755,
		FlushEveryN:    100000,
	}

	ctx := context.Background()
	bq, err := cloudbq.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		log.Fatalf("bq client: %v", err)
	}
	defer bq.Close()
	if cfg.Location != "" {
		bq.Location = cfg.Location
	}

	// 컨트랙트 4개월 병합(two-pass)
	if err := extractContractsFourMonthMerge(ctx, bq, cfg); err != nil {
		log.Fatalf("CONTRACTS: %v", err)
	}
}
