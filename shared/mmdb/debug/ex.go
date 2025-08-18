// ============================================================================
// cmd/mmdb_bench/main.go
// Standalone benchmark program for the mmdb library.
// Workload (default):
//   - value type: int32
//   - 1,000,000 appends (monotonic keys, distributed to shards by id%shards)
//   - 1,000,000 random gets (all hits)
//   - 1,000,000 membership checks (~50% hits)
//   - 1,000,000 updates
//
// Usage:
//
//	go run ./cmd/mmdb_bench
//
// Flags:
//
//	-n        number of operations per stage (default 1_000_000)
//	-shards   number of shards (default 8)
//	-segcap   segment capacity per shard (default 100_000)
//	-dir      data dir (default ./mmdb_benchdata)
//	-seed     RNG seed (default 42)
//	-clean    remove data dir before run (default true)
//	-pod      use POD auto codec (unsafe, arch-specific layout) instead of LE codec
//
// ----------------------------------------------------------------------------
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	mmdb "github.com/rlaaudgjs5638/chainAnalyzer/shared/mmdb" // <-- replace with actual import path
)

func main() {
	var (
		n      = flag.Int("n", 1_000_000, "ops per stage")
		shards = flag.Int("shards", 8, "number of shards")
		segcap = flag.Int("segcap", 100_000, "segment capacity per shard")
		dir    = flag.String("dir", "./mmdb_benchdata", "data directory")
		seed   = flag.Int64("seed", 42, "rng seed")
		clean  = flag.Bool("clean", true, "remove data dir before run")
		usePOD = flag.Bool("pod", false, "use POD auto-codec (unsafe/raw) instead of portable LE codec")
	)
	flag.Parse()

	if *clean {
		_ = os.RemoveAll(*dir)
	}

	rng := rand.New(rand.NewSource(*seed))

	// -------------------- Setup keys & values --------------------
	fmt.Println("[SETUP] Generating monotonic keys...")
	t0 := time.Now()
	keys := generateMonotonicKeys(*n, 1_000, 5, rng) // strictly increasing with small random gaps
	genDur := time.Since(t0)
	fmt.Println("[SETUP] Key generation:", genDur)

	// Partition keys per shard to preserve per-shard monotonicity for appends
	perShard := partitionByShard(keys, *shards)

	// -------------------- Open DB (typed int32) --------------------
	opts := mmdb.Options{Dir: *dir, NumShards: *shards, SegmentCapacity: uint32(*segcap)}

	var tdb *mmdb.TypedDB[int32]
	var err error
	if *usePOD {
		tdb, err = mmdb.OpenTypedPOD[int32](opts)
	} else {
		tdb, err = mmdb.OpenTyped[int32](opts, mmdb.Int32LECodec{})
	}
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer tdb.Close()

	// -------------------- 1) Appends --------------------
	fmt.Printf("[APPEND] Appending %d records...", *n)
	t := time.Now()
	{
		var wg sync.WaitGroup
		wg.Add(*shards)
		for s := 0; s < *shards; s++ {
			s := s
			go func() {
				defer wg.Done()
				for _, id := range perShard[s] {
					// For benchmarking value payload is not important; use i32(id) truncated
					if err := tdb.Append(id, int32(id)); err != nil {
						log.Fatalf("append err: %v", err)
					}
				}
			}()
		}
		wg.Wait()
	}
	appendDur := time.Since(t)
	fmt.Println("[APPEND] Duration:", appendDur, rate(*n, appendDur))

	// -------------------- 2) Random GET (hits) --------------------
	fmt.Printf("[GET] %d random hits...", *n)
	t = time.Now()
	{
		ids := make([]uint32, len(keys))
		copy(ids, keys)
		rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		parts := partitionByShard(ids, *shards)
		var wg sync.WaitGroup
		wg.Add(*shards)
		for s := 0; s < *shards; s++ {
			s := s
			go func() {
				defer wg.Done()
				for _, id := range parts[s] {
					if _, ok := tdb.Get(id); !ok {
						log.Fatalf("get miss for id=%d during GET stage", id)
					}
				}
			}()
		}
		wg.Wait()
	}
	getDur := time.Since(t)
	fmt.Println("[GET] Duration:", getDur, rate(*n, getDur))

	// -------------------- 3) Membership (~50% hits) --------------------
	fmt.Printf("[MEMBERSHIP] %d mixed (≈50%% hits)...", *n)
	t = time.Now()
	var found uint64
	{
		hits := keys[:len(keys)/2]
		missBase := keys[len(keys)-1] + 10_000
		misses := make([]uint32, *n-len(hits))
		for i := range misses {
			misses[i] = missBase + uint32(i*2+1)
		}
		mix := make([]uint32, 0, *n)
		mix = append(mix, hits...)
		mix = append(mix, misses...)
		rng.Shuffle(len(mix), func(i, j int) { mix[i], mix[j] = mix[j], mix[i] })
		parts := partitionByShard(mix, *shards)

		var wg sync.WaitGroup
		wg.Add(*shards)
		for s := 0; s < *shards; s++ {
			s := s
			go func() {
				defer wg.Done()
				local := 0
				for _, id := range parts[s] {
					if _, ok := tdb.Get(id); ok {
						local++
					}
				}
				atomic.AddUint64(&found, uint64(local))
			}()
		}
		wg.Wait()
	}
	memDur := time.Since(t)
	hitPct := 100 * float64(found) / float64(*n)
	fmt.Printf("[MEMBERSHIP] Found %d / %d (%.1f%%)", found, *n, hitPct)
	fmt.Println("[MEMBERSHIP] Duration:", memDur, rate(*n, memDur))

	// -------------------- 4) Updates --------------------
	fmt.Printf("[UPDATE] %d updates...", *n)
	t = time.Now()
	{
		ids := make([]uint32, len(keys))
		copy(ids, keys)
		rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		parts := partitionByShard(ids, *shards)
		var wg sync.WaitGroup
		wg.Add(*shards)
		for s := 0; s < *shards; s++ {
			s := s
			go func() {
				defer wg.Done()
				for _, id := range parts[s] {
					_ = tdb.Update(id, -1) // overwrite with -1
				}
			}()
		}
		wg.Wait()
	}
	updDur := time.Since(t)
	fmt.Println("[UPDATE] Duration:", updDur, rate(*n, updDur))

	// -------------------- Summary --------------------
	fmt.Println("===== Summary =====")
	fmt.Println("Go:", runtime.Version())
	fmt.Println("OS/Arch:", runtime.GOOS+"/"+runtime.GOARCH)
	fmt.Println("Shards:", *shards, " SegCap:", *segcap, " N:", *n)
	fmt.Println("Append:", appendDur, rate(*n, appendDur))
	fmt.Println("Get:", getDur, rate(*n, getDur))
	fmt.Println("Membership:", memDur, rate(*n, memDur))
	fmt.Println("Update:", updDur, rate(*n, updDur))
}

// generateMonotonicKeys returns n strictly increasing keys with random gap in [1..maxGap]
func generateMonotonicKeys(n int, start uint32, maxGap uint32, rng *rand.Rand) []uint32 {
	keys := make([]uint32, n)
	cur := start
	for i := 0; i < n; i++ {
		gap := uint32(rng.Intn(int(maxGap))) + 1
		cur += gap
		keys[i] = cur
	}
	return keys
}

func partitionByShard(keys []uint32, shards int) [][]uint32 {
	parts := make([][]uint32, shards)
	for _, k := range keys {
		sid := int(k % uint32(shards))
		parts[sid] = append(parts[sid], k)
	}
	return parts
}

func rate(n int, d time.Duration) string {
	if d <= 0 {
		return "(∞ ops/s)"
	}
	ops := float64(n) / d.Seconds()
	return fmt.Sprintf("(%.0f ops/s)", ops)
}
