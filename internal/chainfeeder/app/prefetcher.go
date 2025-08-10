package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/chainfeeder/domain"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// ===== 설정 =====
type PrefetcherConfig struct {
	ProjectID   string
	StartAt     time.Time // 시작 시각(UTC 기준). 일 경계로 내림됨
	BatchLimit  int       // 한 번에 읽을 건수 (권장 10000)
	OutCapacity int       // 배치 채널 용량(배치 개수 기준)
	Checkpoint  string    // 체크포인트 파일 경로(JSON)
	SQL         string    // 비우면 기본 SQL 사용
}

// ===== 본체 =====
type Prefetcher struct {
	cfg    PrefetcherConfig
	bq     *bigquery.Client
	out    chan []domain.RawTransaction
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// pause/resume
	mu      sync.RWMutex
	paused  bool
	resumeC chan struct{}

	// 진행 커서(체크포인트)
	dayStartUTC time.Time // 현재 처리 중인 "날짜 시작(UTC 자정)"
	lastTS      time.Time // 당일 내 마지막 block_timestamp
	lastTx      string    // 당일 내 마지막 hash(동일 ts 내 정렬 안정)
	ckptMu      sync.Mutex
}

// TODO 일단 실제 빅쿼리에 작동하는 놈이고, 디버깅 시 돈-데이터 다 빠져나가므로 주의
// TODO 그러나 현재 속도, 최적화는 상당히 미흡하므로, 대규모 작업 시 반드시 검증 필요.
func NewPrefetcher(ctx context.Context, cfg PrefetcherConfig) (*Prefetcher, error) {
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 10000
	}
	if cfg.OutCapacity <= 0 {
		cfg.OutCapacity = 8
	}
	if cfg.SQL == "" {
		// 데이 파티션 + (last_ts, last_tx) 키셋 페이지네이션 + 시간순 정렬
		cfg.SQL =
			"SELECT\n" +
				"  block_timestamp                 AS ts,\n" +
				"  `hash`                          AS txid,\n" +
				"  from_address                    AS from_address,\n" +
				"  IFNULL(to_address, '')          AS to_address,\n" + // NULL 방지
				"  CAST(nonce AS STRING)           AS nonce\n" +
				"FROM `bigquery-public-data.crypto_ethereum.transactions`\n" +
				"WHERE block_timestamp >= @day_start\n" +
				"  AND block_timestamp <  @day_end\n" +
				"  AND (\n" +
				"        @has_ckpt = FALSE\n" +
				"     OR block_timestamp > @last_ts\n" +
				"     OR (block_timestamp = @last_ts AND `hash` > @last_tx)\n" +
				"  )\n" +
				"ORDER BY block_timestamp, `hash`\n" +
				"LIMIT @lim"
	}

	// ProjectID 세팅
	pid := cfg.ProjectID
	if pid == "" || pid == "YOUR_GCP_PROJECT" {
		pid = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}
	if pid == "" || pid == "YOUR_GCP_PROJECT" {
		return nil, fmt.Errorf("ProjectID not set: set cfg.ProjectID or GOOGLE_CLOUD_PROJECT")
	}

	// BigQuery 클라이언트
	var (
		bq  *bigquery.Client
		err error
	)
	if cred := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); cred != "" {
		bq, err = bigquery.NewClient(ctx, pid, option.WithCredentialsFile(cred))
	} else {
		bq, err = bigquery.NewClient(ctx, pid) // ADC fallback
	}
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}

	pctx, cancel := context.WithCancel(ctx)
	p := &Prefetcher{
		cfg:     cfg,
		bq:      bq,
		out:     make(chan []domain.RawTransaction, cfg.OutCapacity),
		ctx:     pctx,
		cancel:  cancel,
		resumeC: make(chan struct{}, 1),
	}

	// 커서 초기화: 체크포인트 → 없으면 StartAt(UTC 00:00)로
	_ = p.loadCheckpoint()
	if p.dayStartUTC.IsZero() {
		p.dayStartUTC = truncateDayUTC(cfg.StartAt)
	}
	if p.lastTS.IsZero() {
		p.lastTS = p.dayStartUTC
	}
	return p, nil
}

func (p *Prefetcher) Out() <-chan []domain.RawTransaction { return p.out }

func (p *Prefetcher) Start() {
	p.wg.Add(1)
	go p.loop()
}

func (p *Prefetcher) Stop() {
	p.cancel()
	p.wg.Wait()
	_ = p.bq.Close()
	close(p.out)
}

// 외부에서 버퍼 드레인 후 재개 신호
func (p *Prefetcher) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused {
		select {
		case p.resumeC <- struct{}{}:
		default:
		}
		p.paused = false
	}
}

// ===== 내부 루프 =====
func (p *Prefetcher) loop() {
	defer p.wg.Done()

	for p.ctx.Err() == nil {
		// 1) pause면 Resume까지 대기
		if p.isPaused() {
			log.Printf("[prefetch] paused; waiting Resume()")
			select {
			case <-p.ctx.Done():
				return
			case <-p.resumeC:
			}
		}

		// 2) 하루 내에서 다음 배치(예: 1만건) 읽기 (시간순)
		batch, nextLastTS, nextLastTx, err := p.fetchDayPage(p.dayStartUTC, p.lastTS, p.lastTx, p.cfg.BatchLimit)
		if err != nil {
			log.Printf("[prefetch] fetch error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// 3) 당일 소진(= 더 없음) → 다음 날로 이동
		if len(batch) == 0 {
			p.dayStartUTC = p.dayStartUTC.Add(24 * time.Hour)
			p.lastTS = p.dayStartUTC
			p.lastTx = ""
			_ = p.saveCheckpoint()
			continue
		}

		// 4) out 채널 꽉 차있으면 즉시 pause 후 Resume에서 밀어넣기
		select {
		case p.out <- batch:
			p.lastTS, p.lastTx = nextLastTS, nextLastTx
			_ = p.saveCheckpoint()
		default:
			log.Printf("[prefetch] out buffer full; pausing (batch=%d)", len(batch))
			p.setPaused(true)
			select {
			case <-p.ctx.Done():
				return
			case <-p.resumeC:
				p.out <- batch
				p.setPaused(false)
				p.lastTS, p.lastTx = nextLastTS, nextLastTx
				_ = p.saveCheckpoint()
			}
		}
	}
}

// ===== BigQuery 단일 페이지(당일, 시간순) =====
func (p *Prefetcher) fetchDayPage(dayStart, lastTS time.Time, lastTx string, limit int) ([]domain.RawTransaction, time.Time, string, error) {
	ctx, cancel := context.WithTimeout(p.ctx, 2*time.Minute)
	defer cancel()

	hasCkpt := !(lastTS.IsZero() && lastTx == "")

	q := p.bq.Query(p.cfg.SQL)
	q.Location = "US" // 공개 데이터셋은 US
	q.Parameters = []bigquery.QueryParameter{
		{Name: "day_start", Value: dayStart},
		{Name: "day_end", Value: dayStart.Add(24 * time.Hour)},
		{Name: "last_ts", Value: lastTS},
		{Name: "last_tx", Value: lastTx},
		{Name: "has_ckpt", Value: hasCkpt},
		{Name: "lim", Value: int64(limit)},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return nil, lastTS, lastTx, err
	}
	it, err := job.Read(ctx)
	if err != nil {
		return nil, lastTS, lastTx, err
	}

	type rowT struct {
		Ts       time.Time `bigquery:"ts"`
		TxID     string    `bigquery:"txid"`
		FromAddr string    `bigquery:"from_address"`
		ToAddr   string    `bigquery:"to_address"`
		Nonce    string    `bigquery:"nonce"`
	}

	batch := make([]domain.RawTransaction, 0, limit)
	nextTS := lastTS
	nextTx := lastTx

	for {
		var r rowT
		err := it.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return batch, nextTS, nextTx, err
		}
		batch = append(batch, domain.RawTransaction{
			BlockTime: r.Ts.UTC().Format("2006-01-02 15:04:05"),
			TxId:      r.TxID,
			From:      r.FromAddr,
			To:        r.ToAddr,
			Nonce:     r.Nonce,
		})
		nextTS = r.Ts
		nextTx = r.TxID
	}

	// (선택) 통계 로그
	if status, _ := job.Status(ctx); status != nil && status.Statistics != nil {
		log.Printf("[prefetch] day=%s batch=%d rows, bytesProcessed=%d",
			dayStart.Format("2006-01-02"), len(batch), status.Statistics.TotalBytesProcessed)
	}

	return batch, nextTS, nextTx, nil
}

// ===== 체크포인트(JSON) =====
type ckpt struct {
	DayStart string `json:"day_start"` // RFC3339
	LastTS   string `json:"last_ts"`   // RFC3339
	LastTx   string `json:"last_tx"`
}

func (p *Prefetcher) saveCheckpoint() error {
	if p.cfg.Checkpoint == "" {
		return nil
	}
	p.ckptMu.Lock()
	defer p.ckptMu.Unlock()

	c := ckpt{
		DayStart: p.dayStartUTC.UTC().Format(time.RFC3339),
		LastTS:   p.lastTS.UTC().Format(time.RFC3339),
		LastTx:   p.lastTx,
	}
	b, _ := json.Marshal(c)
	return os.WriteFile(p.cfg.Checkpoint, b, 0644)
}

func (p *Prefetcher) loadCheckpoint() error {
	if p.cfg.Checkpoint == "" {
		return nil
	}
	b, err := os.ReadFile(p.cfg.Checkpoint)
	if err != nil {
		return nil
	}
	var c ckpt
	if err := json.Unmarshal(b, &c); err != nil {
		return nil
	}
	if t, err := time.Parse(time.RFC3339, c.DayStart); err == nil {
		p.dayStartUTC = t
	}
	if t, err := time.Parse(time.RFC3339, c.LastTS); err == nil {
		p.lastTS = t
	}
	p.lastTx = c.LastTx
	return nil
}

// ===== 헬퍼 =====
func truncateDayUTC(t time.Time) time.Time {
	tt := t.UTC()
	return time.Date(tt.Year(), tt.Month(), tt.Day(), 0, 0, 0, 0, time.UTC)
}

func (p *Prefetcher) isPaused() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.paused
}
func (p *Prefetcher) setPaused(v bool) {
	p.mu.Lock()
	p.paused = v
	p.mu.Unlock()
}
