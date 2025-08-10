package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/cce/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Config struct {
	ProjectID      string
	UsePrecomp     bool   // true면 precompDataset 사용
	PrecompDataset string // 예: "your-project.precomp"
	Repo           *infra.Repo
	CheckpointPath string

	SliceStep          time.Duration // 권장 15m~60m
	BatchLimit         int           // 5k~10k
	Delay              time.Duration // 최신영역 지연 버퍼 (2~5m)
	MaximumBytesBilled int64         // per job 상한 (GiB 단위 변환해서 넣기)
	DryRun             bool          // 드라이런이면 쓰기/체크포인트 저장 안함
	MaxSlices          int           // 드라이런 안전 제한(0=무제한)
	MaxRows            int           // 드라이런 안전 제한(0=무제한)
}

type ckpt struct {
	LastTS   time.Time `json:"last_ts"`
	LastAddr string    `json:"last_addr"`
	HasCkpt  bool      `json:"has_ckpt"`
}

// ---- 체크포인트 helpers
func loadCkpt(path string) (ckpt, error) {
	if path == "" {
		return ckpt{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ckpt{}, nil
	}
	var c ckpt
	_ = json.Unmarshal(b, &c)
	return c, nil
}
func saveCkpt(path string, c ckpt) error {
	if path == "" {
		return nil
	}
	b, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(path, b, 0644)
}

// ---- 실행 1회 (드라이런 추정 + 시간창 자동 축소 + 초과 시 실행 차단)
func RunOnce(ctx context.Context, cfg Config) error {
	pid := cfg.ProjectID
	if pid == "" {
		pid = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}
	if pid == "" {
		return fmt.Errorf("missing project id")
	}

	if cfg.SliceStep <= 0 {
		cfg.SliceStep = 30 * time.Minute
	}
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 5000
	}
	if cfg.Delay <= 0 {
		cfg.Delay = 2 * time.Minute
	}
	if cfg.MaximumBytesBilled <= 0 {
		cfg.MaximumBytesBilled = 20 << 30 // 20GiB
	}

	var opts []option.ClientOption
	if cred := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); cred != "" {
		opts = append(opts, option.WithCredentialsFile(cred))
	}
	bq, err := bigquery.NewClient(ctx, pid, opts...)
	if err != nil {
		return err
	}
	defer bq.Close()

	// 범위: ckpt.LastTS ~ (now-Delay)
	ck, _ := loadCkpt(cfg.CheckpointPath)
	from := ck.LastTS
	if from.IsZero() {
		// 최초 실행: 너무 멀리 가지 않게 최근 24h부터 시작(필요시 조정)
		from = time.Now().UTC().Add(-24 * time.Hour)
	}
	to := time.Now().UTC().Add(-cfg.Delay)
	if !to.After(from) {
		log.Printf("[ingest] up-to-date: from=%s to=%s", from, to)
		return nil
	}

	sql := BuildContractBatchSQL(cfg.UsePrecomp, cfg.PrecompDataset)

	totalRows := 0
	slices := 0
	lastTS := ck.LastTS
	lastAddr := ck.LastAddr
	hasCkpt := ck.HasCkpt

	const minSliceStep = 5 * time.Minute // 시간창 줄일 때 이 이하로는 더 줄이지 않음

	for cur := from; cur.Before(to); cur = cur.Add(cfg.SliceStep) {
		sliceEnd := cur.Add(cfg.SliceStep)
		if sliceEnd.After(to) {
			sliceEnd = to
		}
		slices++

		// 이 슬라이스는 가변적으로 줄일 수 있으니 별도 변수 사용
		curStart := cur
		curEnd := sliceEnd

	retrySlice:
		for {
			q := bq.Query(sql)
			q.Location = "US"
			q.Parameters = []bigquery.QueryParameter{
				{Name: "start_ts", Value: curStart},
				{Name: "end_ts", Value: curEnd},
				{Name: "last_ts", Value: lastTS},
				{Name: "last_addr", Value: lastAddr},
				{Name: "has_ckpt", Value: hasCkpt},
				{Name: "lim", Value: int64(cfg.BatchLimit)},
			}

			// 1) 드라이런으로 예상 바이트 파악
			q.DryRun = true
			dryJob, err := q.Run(ctx)
			if err != nil {
				return err
			}
			st, err := dryJob.Status(ctx)
			if err != nil {
				return err
			}
			var est int64
			if st != nil && st.Statistics != nil {
				est = st.Statistics.TotalBytesProcessed
			}
			q.DryRun = false

			// 1-1) 예상 바이트가 상한 초과면 시간창 절반으로 줄이며 재시도
			if est > cfg.MaximumBytesBilled {
				win := curEnd.Sub(curStart)
				if win > minSliceStep {
					newDur := win / 2
					if newDur < minSliceStep {
						newDur = minSliceStep
					}
					curEnd = curStart.Add(newDur)
					log.Printf("[guard] est=%d > cap=%d; shrink window to %s ~ %s",
						est, cfg.MaximumBytesBilled,
						curStart.Format(time.RFC3339), curEnd.Format(time.RFC3339))
					goto retrySlice
				}
				// 더 못 줄이면 안전을 위해 실행 중단(비용 가드)
				return fmt.Errorf("[guard] estimated bytes %d exceed cap %d even at min window (%s~%s). "+
					"Increase cap or reduce SliceStep/BatchLimit",
					est, cfg.MaximumBytesBilled,
					curStart.Format(time.RFC3339), curEnd.Format(time.RFC3339))
			}

			// 2) 실제 실행
			job, err := q.Run(ctx)
			if err != nil {
				return err
			}
			it, err := job.Read(ctx)
			if err != nil {
				return err
			}

			// 3) 수집
			batch := make([]domain.Contract, 0, cfg.BatchLimit)
			rows := 0
			for {
				var r struct {
					Address           string    `bigquery:"address"`
					Creator           string    `bigquery:"creator"`
					CreationTx        string    `bigquery:"creation_tx"`
					CreatedAt         time.Time `bigquery:"created_at"`
					CreatedBlock      int64     `bigquery:"created_block"`
					BytecodeSHA256    string    `bigquery:"bytecode_sha256"`
					IsERC20           bool      `bigquery:"is_erc20"`
					IsERC721          bool      `bigquery:"is_erc721"`
					FunctionSighashes []string  `bigquery:"function_sighashes"`
					Symbol            *string   `bigquery:"symbol"`
					Name              *string   `bigquery:"name"`
					Decimals          *int64    `bigquery:"decimals"`
					Owner             *string   `bigquery:"owner"`
				}
				if err := it.Next(&r); err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}

				c := domain.Contract{
					Address:           strings.ToLower(r.Address),
					Creator:           strings.ToLower(r.Creator),
					CreationTx:        strings.ToLower(r.CreationTx),
					CreatedAt:         chaintimer.ChainTime(r.CreatedAt),
					CreatedBlock:      r.CreatedBlock,
					BytecodeSHA256:    strings.ToLower(r.BytecodeSHA256),
					IsERC20:           r.IsERC20,
					IsERC721:          r.IsERC721,
					FunctionSighashes: r.FunctionSighashes,
					LastSeenAt:        chaintimer.ChainTime(time.Now().UTC()),
				}
				if r.Symbol != nil {
					c.Symbol = *r.Symbol
				}
				if r.Name != nil {
					c.Name = *r.Name
				}
				if r.Decimals != nil {
					c.Decimals = r.Decimals
				}
				if r.Owner != nil && *r.Owner != "" {
					o := strings.ToLower(*r.Owner)
					if strings.HasPrefix(o, "0x") {
						c.Owner = &o
					}
				}
				batch = append(batch, c)
				rows++
				lastTS, lastAddr, hasCkpt = r.CreatedAt, r.Address, true

				// 드라이런 안전 제한
				if cfg.DryRun && cfg.MaxRows > 0 && (totalRows+rows) >= cfg.MaxRows {
					break
				}
			}

			// 통계 로그
			if st2, _ := job.Status(ctx); st2 != nil && st2.Statistics != nil {
				log.Printf("[ingest] %s~%s rows=%d bytes=%d (dry=%v)",
					curStart.Format(time.RFC3339), curEnd.Format(time.RFC3339),
					rows, st2.Statistics.TotalBytesProcessed, cfg.DryRun)
			}

			// DB 쓰기
			if !cfg.DryRun && len(batch) > 0 && cfg.Repo != nil {
				if err := cfg.Repo.UpsertBatch(batch); err != nil {
					return err
				}
			}

			totalRows += rows

			// 페이지(=주소 기준 페이지네이션) 끝 조건
			if rows < cfg.BatchLimit {
				break
			}
			if cfg.DryRun && cfg.MaxRows > 0 && totalRows >= cfg.MaxRows {
				break
			}
			// rows == BatchLimit 이면 동일 시간창에서 다음 페이지 계속(ckpt로 진행)
		}

		if cfg.DryRun && cfg.MaxSlices > 0 && slices >= cfg.MaxSlices {
			break
		}
	}

	// 체크포인트 저장
	if !cfg.DryRun && hasCkpt {
		if err := saveCkpt(cfg.CheckpointPath, ckpt{
			LastTS: lastTS, LastAddr: lastAddr, HasCkpt: hasCkpt,
		}); err != nil {
			return err
		}
	}
	log.Printf("[ingest] done: slices=%d totalRows=%d dry=%v", slices, totalRows, cfg.DryRun)
	return nil
}
