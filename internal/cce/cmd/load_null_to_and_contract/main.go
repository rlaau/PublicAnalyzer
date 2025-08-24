package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	cloudbq "cloud.google.com/go/bigquery"
	mmap "github.com/edsrzf/mmap-go"
	"google.golang.org/api/iterator"
)

/*
  Extract "contract creations" via: to_address IS NULL
  Save fields: from_address, nonce, receipt_contract_address, block_timestamp

  Record layout (64 bytes, Big-Endian):
   0..19   from_address [20]
  20..27   nonce u64    [8]
  28..47   receipt_contract_address [20] (nil -> zero)
  48..55   block_timestamp_us i64  [8]
  56..63   pad [8]

  Output per-day file: ./out_creations/txc_YYYY-MM-DD.mm
*/

// const (
// 	recSize = 64

// 	offFrom = 0
// 	offNonc = 20
// 	offRC   = 28
// 	offTS   = 48
// 	offPad  = 56
// )

type Config struct {
	ProjectID  string
	Location   string
	FromDate   string // inclusive, e.g., "2023-09-01"
	ToDateExcl string // exclusive, e.g., "2025-01-01"
	OutDir     string // "./out_creations"
	Workers    int    // e.g., 96
	MaxBytes   int64  // 0 = unlimited
	DoSample   bool   // after fetch: sample 50 records and print
	SampleN    int
}

type bqRow struct {
	From   string             `bigquery:"from_address"`
	Nonce  int64              `bigquery:"nonce"`
	RCAddr cloudbq.NullString `bigquery:"receipt_contract_address"`
	TS     time.Time          `bigquery:"block_timestamp"`
}

// to_address IS NULL + 일 단위 파티션 필터
const daySQL = "SELECT\n" +
	"  from_address, nonce, receipt_contract_address, block_timestamp\n" +
	"FROM `bigquery-public-data.crypto_ethereum.transactions`\n" +
	"WHERE to_address IS NULL AND DATE(block_timestamp) = @d\n"

// func main() {
// 	cfg := Config{
// 		ProjectID:  "chain-analyzer-eth-1754795549",
// 		Location:   "US",
// 		FromDate:   "2015-07-30", // 제네시스 이후 전체를 원하면 이렇게 시작점 지정
// 		ToDateExcl: "2025-01-01",
// 		OutDir:     "./out_creations",
// 		Workers:    96,
// 		MaxBytes:   0,
// 		DoSample:   true,
// 		SampleN:    50,
// 	}

// 	// flags(원하면 CLI로 바꿔 쓰기)
// 	flag.StringVar(&cfg.FromDate, "from", cfg.FromDate, "start date inclusive (YYYY-MM-DD)")
// 	flag.StringVar(&cfg.ToDateExcl, "toexcl", cfg.ToDateExcl, "end date exclusive (YYYY-MM-DD)")
// 	flag.StringVar(&cfg.ProjectID, "project", cfg.ProjectID, "GCP project id")
// 	flag.StringVar(&cfg.Location, "location", cfg.Location, "BigQuery location")
// 	flag.StringVar(&cfg.OutDir, "out", cfg.OutDir, "output directory")
// 	flag.IntVar(&cfg.Workers, "workers", cfg.Workers, "parallel workers")
// 	flag.IntVar(&cfg.SampleN, "sample", cfg.SampleN, "sample size to print")
// 	flag.BoolVar(&cfg.DoSample, "dosample", cfg.DoSample, "print random decoded records after fetch")
// 	flag.Parse()

// 	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
// 		log.Fatalf("mkdir: %v", err)
// 	}

// 	if err := run(cfg); err != nil {
// 		log.Fatal(err)
// 	}
// }

func run(cfg Config) error {
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

	// Stage: fetch with day partitioning and 96 workers
	if err := fetchDays(ctx, bq, cfg, days); err != nil {
		return err
	}

	// After fetch: pick random 50 across all files and print decoded
	if cfg.DoSample && cfg.SampleN > 0 {
		if err := sampleAndPrint(cfg.OutDir, cfg.SampleN); err != nil {
			return fmt.Errorf("sample: %w", err)
		}
	}
	return nil
}

func fetchDays(ctx context.Context, bq *cloudbq.Client, cfg Config, days []string) error {
	type job struct{ day string }
	jobs := make(chan job, len(days))
	errs := make(chan error, cfg.Workers)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range jobs {
				if err := dumpOneDay(ctx, bq, cfg, j.day); err != nil {
					errs <- fmt.Errorf("day %s: %w", j.day, err)
					return
				}
				errs <- nil
			}
		}(i)
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

func dumpOneDay(ctx context.Context, bq *cloudbq.Client, cfg Config, day string) error {
	out := filepath.Join(cfg.OutDir, fmt.Sprintf("txc_%s.mm", day))
	if _, err := os.Stat(out); err == nil {
		// 이미 있으면 skip (idempotent)
		return nil
	}

	q := bq.Query(daySQL)
	q.Location = cfg.Location
	q.Parameters = []cloudbq.QueryParameter{{Name: "d", Value: day}}
	if cfg.MaxBytes > 0 {
		q.QueryConfig.MaxBytesBilled = cfg.MaxBytes
	}

	// 쿼리 읽기
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	// 파일 열기
	f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, 0, 4<<20) // 4MB 버퍼
	wrote := 0

	for {
		var r bqRow
		if err := it.Next(&r); err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		rec := make([]byte, recSize)

		// from (20)
		if err := writeHexFixed(rec[offFrom:offFrom+20], r.From, 20); err != nil {
			return fmt.Errorf("from_address: %w", err)
		}
		// nonce u64
		if r.Nonce < 0 {
			return fmt.Errorf("negative nonce? %d", r.Nonce)
		}
		binary.BigEndian.PutUint64(rec[offNonc:offNonc+8], uint64(r.Nonce))

		// rcaddr (20) or zero
		if r.RCAddr.Valid {
			if err := writeHexFixed(rec[offRC:offRC+20], r.RCAddr.StringVal, 20); err != nil {
				return fmt.Errorf("rcaddr: %w", err)
			}
		} else {
			zero(rec[offRC : offRC+20])
		}

		// timestamp micros
		tsUs := r.TS.UTC().UnixMicro()
		binary.BigEndian.PutUint64(rec[offTS:offTS+8], uint64(tsUs))

		// pad
		zero(rec[offPad : offPad+8])

		buf = append(buf, rec...)
		if len(buf) >= cap(buf) {
			if _, err := f.Write(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
		wrote++
	}

	if len(buf) > 0 {
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}

	// (선택) 진행 로그
	if wrote > 0 {
		log.Printf("[day %s] creations wrote rows=%d -> %s", day, wrote, out)
	}
	return nil
}

/* ------------ 샘플 50개 복호화(바이너리→사람 가독 출력) ------------ */
type pick struct {
	path string
	idx  int64
}

func sampleAndPrint(dir string, n int) error {
	paths, err := filepath.Glob(filepath.Join(dir, "txc_*.mm"))
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("no files found in %s", dir)
	}

	// 무작위 샘플 n개: 파일 -> 오프셋 랜덤 선택
	rand.Seed(time.Now().UnixNano())

	var picks []pick

	for len(picks) < n {
		p := paths[rand.Intn(len(paths))]
		fi, err := os.Stat(p)
		if err != nil || fi.Size() == 0 || fi.Size()%recSize != 0 {
			continue
		}
		total := fi.Size() / recSize
		if total == 0 {
			continue
		}
		idx := rand.Int63n(total)
		picks = append(picks, pick{path: p, idx: idx})
	}

	// 파일별로 묶어서 읽기 (mmap 여러 번 열기)
	type one struct {
		from  string
		nonce uint64
		rc    string
		ts    time.Time
		src   string
		pos   int64
	}
	var out []one

	for _, p := range groupByPath(picks) {
		// mmap
		f, err := os.Open(p.path)
		if err != nil {
			return err
		}
		data, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			_ = f.Close()
			return err
		}

		for _, idx := range p.indices {
			base := idx * recSize

			from := "0x" + strings.ToLower(hex.EncodeToString(data[base+offFrom:base+offFrom+20]))
			nonce := binary.BigEndian.Uint64(data[base+offNonc : base+offNonc+8])
			rcb := data[base+offRC : base+offRC+20]
			rc := ""
			if !allZero(rcb) {
				rc = "0x" + strings.ToLower(hex.EncodeToString(rcb))
			}
			tsUs := int64(binary.BigEndian.Uint64(data[base+offTS : base+offTS+8]))
			ts := time.Unix(0, tsUs*1000).UTC() // micro → ns

			out = append(out, one{from: from, nonce: nonce, rc: rc, ts: ts, src: p.path, pos: idx})
		}

		_ = data.Unmap()
		_ = f.Close()
	}

	// 프린트
	log.Printf("=== Sample %d (contract creations: to_address IS NULL) ===", n)
	for i, r := range out {
		fmt.Printf("[%02d] %s idx=%d\n  from=%s\n  nonce=%d\n  rcaddr=%s\n  ts=%s\n",
			i+1, filepath.Base(r.src), r.pos, r.from, r.nonce, r.rc, r.ts.Format(time.RFC3339))
	}
	return nil
}

type grouped struct {
	path    string
	indices []int64
}

func groupByPath(picks []pick) []grouped {
	mp := make(map[string][]int64)
	for _, p := range picks {
		mp[p.path] = append(mp[p.path], p.idx)
	}
	var out []grouped
	for k, v := range mp {
		out = append(out, grouped{path: k, indices: v})
	}
	return out
}

/* ------------ 공통 유틸 ------------ */

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

func writeHexFixed(dst []byte, hexStr string, want int) error {
	s := strings.TrimPrefix(strings.ToLower(hexStr), "0x")
	b, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	if len(b) > want {
		b = b[len(b)-want:] // safety
	}
	// left-pad zeros
	for i := 0; i < want-len(b); i++ {
		dst[i] = 0x00
	}
	copy(dst[want-len(b):], b)
	return nil
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}
