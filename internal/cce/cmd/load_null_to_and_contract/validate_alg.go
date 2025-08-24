// cmd/validate_creations/main.go
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/cce/algo"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

const (
	// 레코드 포맷 (64B):
	// 0..19  from(20)
	// 20..27 nonce u64 (big-endian)
	// 28..47 rcaddr(20) (to_address NULL → 컨트랙트 주소)
	// 48..55 ts_us i64 (unused here)
	// 56..63 pad(8)
	recSize = 64

	offFrom = 0
	offNonc = 20
	offRC   = 28
	offTS   = 48
	offPad  = 56
)

type Rec struct {
	From   [20]byte
	Nonce  uint64
	RCAddr [20]byte
}

func main() {
	dir := flag.String("dir", "./out_creations", "directory containing txc_*.mm files")
	filesPick := flag.Int("files", 100, "number of files to randomly select")
	perFile := flag.Int("perfile", 100, "records to randomly sample per selected file")
	printGood := flag.Int("printgood", 10, "number of correct pairs to print")
	flag.Parse()

	paths, err := filepath.Glob(filepath.Join(*dir, "txc_*.mm"))
	if err != nil {
		log.Fatalf("glob: %v", err)
	}
	if len(paths) == 0 {
		log.Fatalf("no files found in %s", *dir)
	}

	// 무작위 파일 선택
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(paths), func(i, j int) { paths[i], paths[j] = paths[j], paths[i] })
	if *filesPick < len(paths) {
		paths = paths[:*filesPick]
	}
	log.Printf("Selected %d files (target %d each => up to %d checks)\n", len(paths), *perFile, len(paths)**perFile)

	total := 0
	correct := 0
	printedGood := 0
	var mismatches []string

	for _, p := range paths {
		ok, tot, msgs := validateFile(p, *perFile, *printGood-printedGood, &printedGood)
		total += tot
		correct += ok
		mismatches = append(mismatches, msgs...)
	}

	// 결과 출력
	for _, m := range mismatches {
		fmt.Print(m)
	}
	acc := 0.0
	if total > 0 {
		acc = float64(correct) / float64(total) * 100.0
	}
	fmt.Printf("\nSummary:\n  Total checked: %d\n  Correct: %d (%.2f%%)\n  Incorrect: %d (%.2f%%)\n",
		total, correct, acc, total-correct, 100.0-acc)
}

func validateFile(path string, perFile int, printGoodBudget int, printedGood *int) (correct int, sampled int, mismatchMsgs []string) {
	fi, err := os.Stat(path)
	if err != nil {
		log.Printf("[skip] stat error %s: %v\n", path, err)
		return
	}
	if fi.Size()%recSize != 0 {
		log.Printf("[skip] invalid file size (%%rec!=0) %s\n", path)
		return
	}
	n := fi.Size() / recSize
	if n == 0 {
		return
	}

	f, err := os.Open(path)
	if err != nil {
		log.Printf("[skip] open error %s: %v\n", path, err)
		return
	}
	defer f.Close()

	mem, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		log.Printf("[skip] mmap error %s: %v\n", path, err)
		return
	}
	defer mem.Unmap()

	// 파일에서 perFile개 인덱스 무작위 선택 (중복 없이)
	k := perFile
	if int64(k) > n {
		k = int(n)
	}
	indexes := pickKDistinct(n, k)

	for _, idx := range indexes {
		r := readRec(mem, idx)

		pred, isOk := algo.TryPredictCreatedContractAddress(r.From, domain.NewNonce(r.Nonce))
		if !isOk {
			//이 경우, Create2방식으로 만들어진 컨트렉트임
			continue
		}

		sampled++
		if bytes.Equal(pred[:], r.RCAddr[:]) {
			correct++
			// 맞은 케이스도 최대 printGoodBudget만큼 출력
			if printGoodBudget > 0 {
				fmt.Printf("✅ OK  [%s idx=%d]\n  from=%s\n  nonce=%d\n  expected(rcaddr)=%s\n  predicted=%s\n\n",
					filepath.Base(path), idx,
					hex20(r.From[:]), r.Nonce, hex20nz(r.RCAddr[:]), hex20(pred[:]),
				)
				printGoodBudget--
				*printedGood++
			}
		} else {
			msg := fmt.Sprintf("❌ NG  [%s idx=%d]\n  from=%s\n  nonce=%d\n  expected(rcaddr)=%s\n  predicted=%s\n\n",
				filepath.Base(path), idx,
				hex20(r.From[:]), r.Nonce, hex20nz(r.RCAddr[:]), hex20(pred[:]),
			)
			mismatchMsgs = append(mismatchMsgs, msg)
		}
	}
	log.Printf("[DONE] %s -> sampled=%d correct=%d incorrect=%d\n",
		filepath.Base(path), sampled, correct, sampled-correct)

	return
}

func readRec(mem []byte, idx int64) Rec {
	base := idx * recSize
	var r Rec
	copy(r.From[:], mem[base+offFrom:base+offFrom+20])
	r.Nonce = binary.BigEndian.Uint64(mem[base+offNonc : base+offNonc+8])
	copy(r.RCAddr[:], mem[base+offRC:base+offRC+20])
	return r
}

// utilities

func pickKDistinct(n int64, k int) []int64 {
	if k <= 0 {
		return []int64{}
	}
	// reservoir sampling
	res := make([]int64, 0, k)
	for i := int64(0); i < n && len(res) < k; i++ {
		res = append(res, i)
	}
	for i := int64(len(res)); i < n; i++ {
		j := rand.Int63n(i + 1)
		if j < int64(k) {
			res[j] = i
		}
	}
	// shuffle
	rand.Shuffle(len(res), func(i, j int) { res[i], res[j] = res[j], res[i] })
	return res
}

func hex20(b []byte) string {
	return "0x" + strings.ToLower(hex.EncodeToString(b))
}

func hex20nz(b []byte) string {
	zero := true
	for _, v := range b {
		if v != 0 {
			zero = false
			break
		}
	}
	if zero {
		return "0x" + strings.Repeat("00", 20)
	}
	return hex20(b)
}
