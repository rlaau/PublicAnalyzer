// cmd/validate_sort/main.go
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	mmap "github.com/edsrzf/mmap-go"
)

const (
	recSize = 160

	offTS = 132 // block_timestamp_us i64 (big-endian)
)

func main() {
	dir := flag.String("dir", "../load_all_tx/out/sorted", "directory containing sorted .mm files")
	pick := flag.Int("pick", 10, "number of files to validate")
	sample := flag.Int("sample", 10000, "number of records to sample per file")
	minStep := flag.Int("minstep", 1, "minimum step between sampled indices")
	maxStep := flag.Int("maxstep", 1000, "maximum step between sampled indices")
	flag.Parse()

	files, err := filepath.Glob(filepath.Join(*dir, "tx_*.mm"))
	if err != nil {
		log.Fatalf("glob: %v", err)
	}
	if len(files) == 0 {
		log.Fatalf("no files found in %s", *dir)
	}

	// 무작위로 10개(또는 지정 개수) 선택
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(files), func(i, j int) { files[i], files[j] = files[j], files[i] })
	if *pick < len(files) {
		files = files[:*pick]
	}

	log.Printf("Selected %d files for validation", len(files))
	for _, f := range files {
		if err := validateFile(f, *sample, *minStep, *maxStep); err != nil {
			log.Printf("[FAIL] %s: %v", filepath.Base(f), err)
		} else {
			log.Printf("[OK]   %s", filepath.Base(f))
		}
	}
}

func validateFile(path string, sampleCount, minStep, maxStep int) error {
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if fi.Size()%recSize != 0 {
		return fmt.Errorf("invalid file size: %d not multiple of %d", fi.Size(), recSize)
	}
	total := fi.Size() / recSize
	if total == 0 {
		return fmt.Errorf("empty file")
	}

	// mmap read-only
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	defer data.Unmap()

	// 파일명에서 날짜 추출 (tx_YYYY-MM-DD.mm)
	day, _ := extractDayFromName(filepath.Base(path))

	// 샘플 전략:
	// - step은 [minStep, maxStep] 중 랜덤
	// - 시작 인덱스는 [0 .. total-1] 중 랜덤
	// - 증가하며 sampleCount개 뽑되, 파일 끝 넘으면 wrap 없이 종료 (실제 샘플 수가 줄 수 있음)
	if minStep <= 0 {
		minStep = 1
	}
	if maxStep < minStep {
		maxStep = minStep
	}
	step := rand.Intn(maxStep-minStep+1) + minStep
	var start int64
	if total > 1 {
		start = rand.Int63n(total - 1)
	}

	// 수집
	indices := make([]int64, 0, sampleCount)
	for i := 0; i < sampleCount; i++ {
		idx := start + int64(i*step)
		if idx >= total {
			break
		}
		indices = append(indices, idx)
	}
	if len(indices) == 0 {
		return fmt.Errorf("no indices sampled (total=%d, start=%d, step=%d)", total, start, step)
	}

	// 검증: seconds 단위 비내림
	var prevSec int64 = -1
	violations := 0
	const maxReport = 10

	type bad struct {
		Idx   int64
		Sec   int64
		Prev  int64
		Delta int64
	}
	var bads []bad

	for _, idx := range indices {
		base := idx * recSize
		tsUs := int64(binary.BigEndian.Uint64(data[base+offTS : base+offTS+8]))
		sec := tsUs / 1_000_000

		// 파일명 날짜 대략적 검증(선택): 동일 일자 범위인지 (warning)
		if day.IsZero() == false {
			ds := day.Unix()
			if sec < ds || sec >= ds+86400 {
				// 경계 밖이면 경고 (블록 타임이 파일명과 하루 차이날 수 있음)
				// 그냥 로그로만 알림
				// log.Printf("[warn] %s idx=%d ts_sec=%d out of day '%s'", filepath.Base(path), idx, sec, day.Format("2006-01-02"))
			}
		}

		if prevSec > sec {
			violations++
			if len(bads) < maxReport {
				bads = append(bads, bad{
					Idx:   idx,
					Sec:   sec,
					Prev:  prevSec,
					Delta: sec - prevSec,
				})
			}
		}
		if sec > prevSec {
			prevSec = sec
		}
	}

	// 간단한 요약 출력
	log.Printf("[check] %s  total=%d  sample=%d  step=%d  start=%d  violations=%d",
		filepath.Base(path), total, len(indices), step, start, violations)

	if violations > 0 {
		// 세부 위반 몇 개만 보여주기
		sort.Slice(bads, func(i, j int) bool { return bads[i].Idx < bads[j].Idx })
		msg := "violations:\n"
		for _, b := range bads {
			msg += fmt.Sprintf("  idx=%d  sec=%d  prev_sec=%d  (delta=%d)\n", b.Idx, b.Sec, b.Prev, b.Delta)
		}
		return fmt.Errorf("%d violation(s)\n%s", violations, msg)
	}
	return nil
}

var dayRe = regexp.MustCompile(`^tx_(\d{4})-(\d{2})-(\d{2})\.mm$`)

func extractDayFromName(name string) (time.Time, bool) {
	m := dayRe.FindStringSubmatch(name)
	if m == nil {
		return time.Time{}, false
	}
	day, err := time.Parse("2006-01-02", fmt.Sprintf("%s-%s-%s", m[1], m[2], m[3]))
	if err != nil {
		return time.Time{}, false
	}
	return day.UTC(), true
}
