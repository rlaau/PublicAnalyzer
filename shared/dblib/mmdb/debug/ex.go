// ============================================================================
// cmd/mmdb_bench/main.go
// Standalone benchmark program for the mmdb library and BadgerDB.
// Workload (default):
//   - value type: int32
//   - 1,000,000 appends (monotonic keys, distributed to shards by id%shards)
//   - 1,000,000 random gets (all hits)
//   - 1,000,000 membership checks (~50% hits)
//   - 1,000,000 updates (READ -> value+1 -> WRITE)
//
// Usage:
//
//	go run ./cmd/mmdb_bench
//
// Flags:
//
//	-n        number of operations per stage (default 1_000_000)
//	-shards   number of shards / concurrency (default 8)
//	-segcap   segment capacity per shard (default 100_000)
//	-dir      data dir for mmdb (default ./mmdb_benchdata)
//	-seed     RNG seed (default 42)
//	-clean    remove data dirs before run (default true)
//	-pod      use POD auto codec for mmdb (unsafe/raw) instead of portable LE codec
//	-bdir     data dir for badger (default ./badger_benchdata)
//
// ----------------------------------------------------------------------------
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	mmdb "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/mmdb"
)

type durations struct {
	append     time.Duration
	get        time.Duration
	membership time.Duration
	update     time.Duration
}

func main() {
	var (
		n      = flag.Int("n", 1_000_000, "ops per stage")
		shards = flag.Int("shards", 8, "number of shards / concurrency")
		segcap = flag.Int("segcap", 100_000, "segment capacity per shard")
		dirMM  = flag.String("dir", "./mmdb_benchdata", "mmdb data directory")
		dirBG  = flag.String("bdir", "./badger_benchdata", "badger data directory")
		seed   = flag.Int64("seed", 42, "rng seed")
		clean  = flag.Bool("clean", true, "remove data dirs before run")
		usePOD = flag.Bool("pod", false, "use POD auto-codec (unsafe/raw) for mmdb")
	)
	flag.Parse()

	if *clean {
		_ = os.RemoveAll(*dirMM)
		_ = os.RemoveAll(*dirBG)
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

	// -------------------- MMDB bench --------------------
	opts := mmdb.Options{Dir: *dirMM, NumShards: *shards, SegmentCapacity: uint32(*segcap)}
	var tdb *mmdb.TypedDB[int32]
	var err error
	if *usePOD {
		tdb, err = mmdb.OpenTypedPOD[int32](opts)
	} else {
		tdb, err = mmdb.OpenTyped[int32](opts, mmdb.Int32LECodec{})
	}
	if err != nil {
		log.Fatalf("mmdb open: %v", err)
	}
	defer tdb.Close()

	mmDur := runMMDBBench(tdb, keys, perShard, *shards)

	// -------------------- Badger bench --------------------
	bgDur, err := runBadgerBench(keys, *dirBG, *shards)
	if err != nil {
		log.Fatalf("badger bench: %v", err)
	}

	// -------------------- Summary --------------------
	fmt.Println("===== Per-DB Summary =====")
	fmt.Println("Go:", runtime.Version())
	fmt.Println("OS/Arch:", runtime.GOOS+"/"+runtime.GOARCH)
	fmt.Println("Shards/Concurrency:", *shards, " SegCap:", *segcap, " N:", *n)

	fmt.Printf("MMDB    → Append: %v %s", mmDur.append, rate(*n, mmDur.append))
	fmt.Printf("           Get: %v %s", mmDur.get, rate(*n, mmDur.get))
	fmt.Printf("   Membership: %v %s", mmDur.membership, rate(*n, mmDur.membership))
	fmt.Printf("        Update: %v %s", mmDur.update, rate(*n, mmDur.update))

	fmt.Printf("Badger  → Append: %v %s", bgDur.append, rate(*n, bgDur.append))
	fmt.Printf("           Get: %v %s", bgDur.get, rate(*n, bgDur.get))
	fmt.Printf("   Membership: %v %s", bgDur.membership, rate(*n, bgDur.membership))
	fmt.Printf("        Update: %v %s", bgDur.update, rate(*n, bgDur.update))

	// -------------------- Head-to-head --------------------
	fmt.Println("===== Comparison (ops/s) =====")
	fmt.Printf("Stage        MMDB           Badger")
	fmt.Printf("Append     %10.0f    %10.0f", ops(*n, mmDur.append), ops(*n, bgDur.append))
	fmt.Printf("Get        %10.0f    %10.0f", ops(*n, mmDur.get), ops(*n, bgDur.get))
	fmt.Printf("Membership %10.0f    %10.0f", ops(*n, mmDur.membership), ops(*n, bgDur.membership))
	fmt.Printf("Update     %10.0f    %10.0f", ops(*n, mmDur.update), ops(*n, bgDur.update))
}

// -------------------- MMDB runner --------------------

func runMMDBBench(tdb *mmdb.TypedDB[int32], keys []uint32, perShard [][]uint32, shards int) durations {
	var d durations

	// 1) Appends
	fmt.Printf("[MMDB][APPEND] %d records...", len(keys))
	t := time.Now()
	{
		var wg sync.WaitGroup
		wg.Add(shards)
		for s := 0; s < shards; s++ {
			s := s
			go func() {
				defer wg.Done()
				for _, id := range perShard[s] {
					if err := tdb.Append(id, int32(id)); err != nil {
						log.Fatalf("mmdb append: %v", err)
					}
				}
			}()
		}
		wg.Wait()
	}
	d.append = time.Since(t)

	// 2) Random GET (hits)
	fmt.Printf("[MMDB][GET] %d random hits...", len(keys))
	t = time.Now()
	{
		ids := make([]uint32, len(keys))
		copy(ids, keys)
		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		parts := partitionByShard(ids, shards)
		var wg sync.WaitGroup
		wg.Add(shards)
		for s := 0; s < shards; s++ {
			s := s
			go func() {
				defer wg.Done()
				for _, id := range parts[s] {
					if _, ok := tdb.Get(id); !ok {
						log.Fatalf("mmdb get miss id=%d", id)
					}
				}
			}()
		}
		wg.Wait()
	}
	d.get = time.Since(t)

	// 3) Membership (~50% hits)
	fmt.Printf("[MMDB][MEMBERSHIP] %d mixed...", len(keys))
	t = time.Now()
	{
		hits := keys[:len(keys)/2]
		missBase := keys[len(keys)-1] + 10_000
		misses := make([]uint32, len(keys)-len(hits))
		for i := range misses {
			misses[i] = missBase + uint32(i*2+1)
		}
		mix := append(append(make([]uint32, 0, len(keys)), hits...), misses...)
		rand.Shuffle(len(mix), func(i, j int) { mix[i], mix[j] = mix[j], mix[i] })
		parts := partitionByShard(mix, shards)

		var wg sync.WaitGroup
		var found uint64
		wg.Add(shards)
		for s := 0; s < shards; s++ {
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
		hitPct := 100 * float64(found) / float64(len(keys))
		fmt.Printf("[MMDB][MEMBERSHIP] Found %d / %d (%.1f%%)", found, len(keys), hitPct)
	}
	d.membership = time.Since(t)

	// 4) Updates: READ -> +1 -> WRITE
	fmt.Printf("[MMDB][UPDATE] %d updates (+1)...", len(keys))
	t = time.Now()
	{
		ids := make([]uint32, len(keys))
		copy(ids, keys)
		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		parts := partitionByShard(ids, shards)
		var wg sync.WaitGroup
		wg.Add(shards)
		for s := 0; s < shards; s++ {
			s := s
			go func() {
				defer wg.Done()
				for _, id := range parts[s] {
					if v, ok := tdb.Get(id); ok {
						_ = tdb.Update(id, v+1)
					}
				}
			}()
		}
		wg.Wait()
	}
	d.update = time.Since(t)

	return d
}

// -------------------- Badger runner --------------------

func runBadgerBench(keys []uint32, dir string, concurrency int) (durations, error) {
	var d durations
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return d, err
	}
	defer db.Close()

	// 1) Appends (batched txns)
	fmt.Printf("[BADGER][APPEND] %d records...", len(keys))
	t := time.Now()
	const batch = 1000
	for i := 0; i < len(keys); {
		if err := db.Update(func(txn *badger.Txn) error {
			for j := 0; j < batch && i < len(keys); j, i = j+1, i+1 {
				k := make([]byte, 4)
				binary.LittleEndian.PutUint32(k, keys[i])
				v := make([]byte, 4)
				binary.LittleEndian.PutUint32(v, uint32(int32(keys[i])))
				if err := txn.Set(k, v); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return d, err
		}
	}
	d.append = time.Since(t)

	// 2) Random GET (hits) with concurrency workers
	fmt.Printf("[BADGER][GET] %d random hits...", len(keys))
	t = time.Now()
	ids := make([]uint32, len(keys))
	copy(ids, keys)
	rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	chunks := chunkIDs(ids, concurrency)
	{
		var wg sync.WaitGroup
		wg.Add(len(chunks))
		for _, ch := range chunks {
			ch := ch
			go func() {
				defer wg.Done()
				if err := db.View(func(txn *badger.Txn) error {
					for _, id := range ch {
						k := make([]byte, 4)
						binary.LittleEndian.PutUint32(k, id)
						if _, err := txn.Get(k); err != nil {
							return fmt.Errorf("miss id=%d: %w", id, err)
						}
					}
					return nil
				}); err != nil {
					log.Fatalf("badger get: %v", err)
				}
			}()
		}
		wg.Wait()
	}
	d.get = time.Since(t)

	// 3) Membership (~50% hits)
	fmt.Printf("[BADGER][MEMBERSHIP] %d mixed...", len(keys))
	t = time.Now()
	hits := keys[:len(keys)/2]
	missBase := keys[len(keys)-1] + 10_000
	misses := make([]uint32, len(keys)-len(hits))
	for i := range misses {
		misses[i] = missBase + uint32(i*2+1)
	}
	mix := append(append(make([]uint32, 0, len(keys)), hits...), misses...)
	rand.Shuffle(len(mix), func(i, j int) { mix[i], mix[j] = mix[j], mix[i] })
	chunks = chunkIDs(mix, concurrency)

	var found uint64
	{
		var wg sync.WaitGroup
		wg.Add(len(chunks))
		for _, ch := range chunks {
			ch := ch
			go func() {
				defer wg.Done()
				count := 0
				if err := db.View(func(txn *badger.Txn) error {
					for _, id := range ch {
						k := make([]byte, 4)
						binary.LittleEndian.PutUint32(k, id)
						if _, err := txn.Get(k); err == nil {
							count++
						}
					}
					return nil
				}); err != nil {
					log.Fatalf("badger membership: %v", err)
				}
				atomic.AddUint64(&found, uint64(count))
			}()
		}
		wg.Wait()
	}
	hitPct := 100 * float64(found) / float64(len(keys))
	fmt.Printf("[BADGER][MEMBERSHIP] Found %d / %d (%.1f%%)", found, len(keys), hitPct)
	d.membership = time.Since(t)

	// 4) Updates: READ -> +1 -> WRITE (batched txns)
	fmt.Printf("[BADGER][UPDATE] %d updates (+1)...", len(keys))
	t = time.Now()
	ids = make([]uint32, len(keys))
	copy(ids, keys)
	rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	// Use batched read-modify-write in writable transactions
	const commitEvery = 1000
	for i := 0; i < len(ids); {
		if err := db.Update(func(txn *badger.Txn) error {
			for j := 0; j < commitEvery && i < len(ids); j, i = j+1, i+1 {
				k := make([]byte, 4)
				binary.LittleEndian.PutUint32(k, ids[i])
				it, err := txn.Get(k)
				if err != nil {
					continue
				} // miss tolerated
				val, err := it.ValueCopy(nil)
				if err != nil {
					return err
				}
				v := int32(binary.LittleEndian.Uint32(val)) + 1
				nb := make([]byte, 4)
				binary.LittleEndian.PutUint32(nb, uint32(v))
				if err := txn.Set(k, nb); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return d, err
		}
	}
	d.update = time.Since(t)

	return d, nil
}

// -------------------- helpers --------------------

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

func chunkIDs(ids []uint32, chunks int) [][]uint32 {
	if chunks <= 1 {
		return [][]uint32{ids}
	}
	out := make([][]uint32, chunks)
	for i, id := range ids {
		out[i%chunks] = append(out[i%chunks], id)
	}
	return out
}

func rate(n int, d time.Duration) string {
	if d <= 0 {
		return "(∞ ops/s)"
	}
	ops := float64(n) / d.Seconds()
	return fmt.Sprintf("(%.0f ops/s)", ops)
}

func ops(n int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}
