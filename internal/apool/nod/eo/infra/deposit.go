// file: internal/apool/rel/triplet/infra/deposit_badger_repo.go
package infra

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"strings"
	"sync"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	sharedDomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// DepositRepository: Badger(MVCC) 기반 저장소 구현
type DepositRepository struct {
	db         *badger.DB
	shards     []sync.Mutex // 낙관적 락(샤드 뮤텍스)
	shardMask  uint32       // len(shards)=power of two → mask로 인덱싱
	keyPrefix  []byte       // "deposit:"
	maxRetries int          // MVCC 재시도 횟수(예: 3)
}

// NewBadgerDepositRepository: path 아래에 Badger DB를 엽니다.
func NewBadgerDepositRepository(mode mode.ProcessingMode, path string, numShards int) (*DepositRepository, error) {
	var root string
	if path == "" {
		if !mode.IsTest() {
			root = computation.ComputeThisProductionStorage("nod", "eo", "deposit")
		} else {
			root = computation.ComputeThisTestingStorage("nod", "eo", "deposit")
		}
	} else {
		root = path
	}
	if numShards <= 0 {
		numShards = 256
	}
	// shard 개수는 2^n으로 맞추는 게 편함
	numShards = nextPow2(numShards)

	opts := badger.DefaultOptions(filepath.Clean(root))
	//TODO 안전 기본값(원하면 튜닝 가능)
	//TODO 추후 성능 충분 시 싱크라이팅 켜기
	opts = opts.WithSyncWrites(false).
		WithLogger(nil) // badger 로그 끔(원하면 주입)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	shards := make([]sync.Mutex, numShards)
	return &DepositRepository{
		db:         db,
		shards:     shards,
		shardMask:  uint32(numShards - 1),
		keyPrefix:  []byte("deposit:"),
		maxRetries: 3,
	}, nil
}

// Close: DB 닫기 (선택)
func (r *DepositRepository) Close() error {
	return r.db.Close()
}

// // ---- DepositRepository 인터페이스 구현 ----
// func (r *DepositRepository) HandleDepositDetection(cexAddr, depositAddr sharedDomain.Address, tx *sharedDomain.MarkedTransaction, time chaintimer.ChainTime) error {
// 	depositDetectWithEvidence

// }

// SaveDetectedDeposit: 신규면 Insert, 기존이면 변경 있을 때만 Update.
// MVCC 충돌 시 3회까지 재시도, 실패 시 샤드 뮤텍스로 직렬화 후 1회 재시도.
func (r *DepositRepository) SaveDetectedDeposit(deposit *sharedDomain.DetectedDeposit) error {
	if deposit == nil {
		return errors.New("nil deposit")
	}
	key := r.makeKey(deposit.Address)

	fn := func(txn *badger.Txn) error {
		// 현재 값 조회(없으면 신규)
		var cur sharedDomain.DetectedDeposit
		itm, err := txn.Get(key)
		switch {
		case err == nil:
			val, e := itm.ValueCopy(nil)
			if e != nil {
				return e
			}
			if e = json.Unmarshal(val, &cur); e != nil {
				return e
			}
			// 변경 사항 있는지 비교(정책에 맞게 갱신)
			changed := false
			if cur.TxCount != deposit.TxCount {
				cur.TxCount = deposit.TxCount
				changed = true
			}
			// 필요 시 다음 필드도 유지/갱신(정책 선택)
			if cur.DetectedAt != deposit.DetectedAt {
				cur.DetectedAt = deposit.DetectedAt
				changed = true
			}
			if cur.CEXAddress != deposit.CEXAddress {
				cur.CEXAddress = deposit.CEXAddress
				changed = true
			}
			if !changed {
				return nil // noop
			}
			buf, e := json.Marshal(&cur)
			if e != nil {
				return e
			}
			return txn.Set(key, buf)

		case errors.Is(err, badger.ErrKeyNotFound):
			// 신규 Insert
			buf, e := json.Marshal(deposit)
			if e != nil {
				return e
			}
			return txn.Set(key, buf)

		default:
			return err
		}
	}

	return r.doWithRetriesAndShard(key, fn)
}

// LoadDetectedDeposits: prefix 스캔
func (r *DepositRepository) LoadDetectedDeposits() ([]*sharedDomain.DetectedDeposit, error) {
	var out []*sharedDomain.DetectedDeposit
	err := r.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := r.keyPrefix
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			val, e := item.ValueCopy(nil)
			if e != nil {
				return e
			}
			var d sharedDomain.DetectedDeposit
			if e = json.Unmarshal(val, &d); e != nil {
				return e
			}
			out = append(out, &d)
		}
		return nil
	})
	return out, err
}

// IsDepositAddress: 키 존재 여부로 판단(빠름)
func (r *DepositRepository) IsDepositAddress(addr sharedDomain.Address) (bool, error) {
	key := r.makeKey(addr)
	err := r.db.View(func(txn *badger.Txn) error {
		_, e := txn.Get(key)
		return e
	})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	return false, err
}

// GetDepositInfo: 1건 조회
func (r *DepositRepository) GetDepositInfo(addr sharedDomain.Address) (*sharedDomain.DetectedDeposit, error) {
	key := r.makeKey(addr)
	var out *sharedDomain.DetectedDeposit
	err := r.db.View(func(txn *badger.Txn) error {
		itm, e := txn.Get(key)
		if e != nil {
			return e
		}
		val, e := itm.ValueCopy(nil)
		if e != nil {
			return e
		}
		var d sharedDomain.DetectedDeposit
		if e = json.Unmarshal(val, &d); e != nil {
			return e
		}
		out = &d
		return nil
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, fmt.Errorf("deposit address not found: %s", addr.String())
		}
		return nil, err
	}
	return out, nil
}

// UpdateTxCount: 주소별 CAS 업데이트. 3회 재시도 후 샤드 락으로 직렬화.
func (r *DepositRepository) UpdateTxCount(addr sharedDomain.Address, count int64) error {
	key := r.makeKey(addr)

	fn := func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf("deposit address not found for update: %s", addr.String())
			}
			return err
		}
		val, e := itm.ValueCopy(nil)
		if e != nil {
			return e
		}
		var d sharedDomain.DetectedDeposit
		if e = json.Unmarshal(val, &d); e != nil {
			return e
		}
		if d.TxCount == count {
			return nil // noop
		}
		d.TxCount = count
		buf, e := json.Marshal(&d)
		if e != nil {
			return e
		}
		return txn.Set(key, buf)
	}

	return r.doWithRetriesAndShard(key, fn)
}

// ---- 내부 유틸 ----

// MVCC 트랜잭션을 maxRetries까지 재시도하고, 계속 실패하면 샤드 락을 잡은 뒤 1회 더 시도
func (r *DepositRepository) doWithRetriesAndShard(key []byte, fn func(txn *badger.Txn) error) error {
	// 1) MVCC 낙관적 재시도
	var lastErr error
	for i := 0; i < r.maxRetries; i++ {
		lastErr = r.db.Update(func(txn *badger.Txn) error {
			return fn(txn)
		})
		if lastErr == nil {
			return nil
		}
		if !errors.Is(lastErr, badger.ErrConflict) {
			return lastErr
		}
		// ErrConflict → 재시도
	}

	// 2) 계속 충돌이면 샤드 뮤텍스 잠깐 잡고 직렬화
	ix := r.shardIndex(key)
	r.shards[ix].Lock()
	defer r.shards[ix].Unlock()

	// 잠금 상태에서 다시 1회 시도
	return r.db.Update(func(txn *badger.Txn) error {
		return fn(txn)
	})
}

func (r *DepositRepository) makeKey(addr sharedDomain.Address) []byte {
	// 주소 문자열을 일관되게(소문자, 0x 포함/미포함 무관)
	hex := strings.ToLower(addr.String()) // addr.String()이 0x프리픽스 포함이라면 그대로 OK
	// 키: "deposit:"+<hex>
	var b bytes.Buffer
	b.Grow(len(r.keyPrefix) + len(hex))
	b.Write(r.keyPrefix)
	b.WriteString(hex)
	return b.Bytes()
}

func (r *DepositRepository) shardIndex(key []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return h.Sum32() & r.shardMask
}

func nextPow2(n int) int {
	// n >= 1
	x := 1
	for x < n {
		x <<= 1
	}
	return x
}
