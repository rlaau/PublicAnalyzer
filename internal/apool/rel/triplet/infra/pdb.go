// file: internal/apool/rel/triplet/infra/pending_repo.go
package infra

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type PendingRelationRepo interface {
	Close() error
	GetPendingRelations(toAddr domain.Address) ([]FromScala, error)
	DeletePendingRelations(toAddr domain.Address) error
	AddToPendingRelations(toAddr domain.Address, from FromScala) error
	CountPendingRelations() int
}

// PendingDB의 key는 toAddress, Val은 []FromScala
type FromScala struct {
	FromAddress domain.Address       `json:"from_address"`
	LastTxId    domain.TxId          `json:"last_tx_id"`
	LastTime    chaintimer.ChainTime `json:"last_time"`
	Volume      int32                `json:"volum"`
}

type BadgerPendingRelationRepo struct {
	db *badger.DB
	// --- MVCC 재시도 + 샤드 락(필수 처리 경로) ---
	shards     []sync.Mutex
	shardMask  uint32
	maxRetries int
}

const (
	metaCountKey     = "__meta:pending_relations_count__" // 현재 to-key 개수
	metaDeleteCntKey = "__meta:pending_relations_delete_cnt__"
	gcDeleteTriggerN = 1024 // 삭제 N회마다 GC 시도
	gcDiscardRatio   = 0.5  // badger.RunValueLogGC 매개값
)

// NewBadgerPendingRelationRepo: 기본 셋업(샤드=256, maxRetries=3)
func NewBadgerPendingRelationRepo(dbPath string) (*BadgerPendingRelationRepo, error) {
	opts := badger.DefaultOptions(dbPath).WithLogger(nil) // Disable badger logging
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	// 메타 카운터 키 초기화(없으면 0)
	_ = db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte(metaCountKey)); err == badger.ErrKeyNotFound {
			var z [8]byte
			if e := txn.Set([]byte(metaCountKey), z[:]); e != nil {
				return e
			}
		}
		if _, err := txn.Get([]byte(metaDeleteCntKey)); err == badger.ErrKeyNotFound {
			var z [8]byte
			if e := txn.Set([]byte(metaDeleteCntKey), z[:]); e != nil {
				return e
			}
		}
		return nil
	})

	numShards := nextPow2(256)
	return &BadgerPendingRelationRepo{
		db:         db,
		shards:     make([]sync.Mutex, numShards),
		shardMask:  uint32(numShards - 1),
		maxRetries: 3,
	}, nil
}

func (r *BadgerPendingRelationRepo) Close() error { return r.db.Close() }

// ───────────────────────────────────────────────────────────────────────────────
// 내부: 카운터 유틸

func readU64(txn *badger.Txn, key []byte) (uint64, error) {
	itm, err := txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}
	var v uint64
	err = itm.Value(func(val []byte) error {
		if len(val) >= 8 {
			v = binary.LittleEndian.Uint64(val[:8])
		}
		return nil
	})
	return v, err
}

func writeU64(txn *badger.Txn, key []byte, v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return txn.Set(key, buf[:])
}

func incrU64(txn *badger.Txn, key []byte, delta int64) error {
	cur, err := readU64(txn, key)
	if err != nil {
		return err
	}
	if delta < 0 {
		d := uint64(-delta)
		if cur < d {
			return fmt.Errorf("counter underflow: key=%s have=%d dec=%d", string(key), cur, d)
		}
		return writeU64(txn, key, cur-d)
	}
	return writeU64(txn, key, cur+uint64(delta))
}

// ───────────────────────────────────────────────────────────────────────────────

func (r *BadgerPendingRelationRepo) GetPendingRelations(toAddr domain.Address) ([]FromScala, error) {
	var list []FromScala
	err := r.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte(toAddr.String()))
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error {
			return json.Unmarshal(val, &list)
		})
	})
	if err == badger.ErrKeyNotFound {
		return []FromScala{}, nil
	}
	return list, err
}

func (r *BadgerPendingRelationRepo) DeletePendingRelations(toAddr domain.Address) error {
	key := []byte(toAddr.String())
	return r.db.Update(func(txn *badger.Txn) error {
		// 존재할 때만 삭제 및 카운터 감소/삭제수 증가
		if _, err := txn.Get(key); err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		if err := txn.Delete(key); err != nil {
			return err
		}
		if err := incrU64(txn, []byte(metaCountKey), -1); err != nil {
			return err
		}
		if err := incrU64(txn, []byte(metaDeleteCntKey), 1); err != nil {
			return err
		}
		return nil
	})
}

// AddToPendingRelations:
// - FromAddress 가 없으면 append (Volume<=0 이면 1로 보정)
// - 있으면 LastTxId/LastTime 덮어쓰기 + Volume++
func (r *BadgerPendingRelationRepo) AddToPendingRelations(toAddr domain.Address, from FromScala) error {
	key := []byte(toAddr.String())
	var needGC bool

	fn := func(txn *badger.Txn) error {
		var list []FromScala
		itm, err := txn.Get(key)
		switch err {
		case nil:
			// 기존 값 → 디코드
			if err := itm.Value(func(val []byte) error {
				return json.Unmarshal(val, &list)
			}); err != nil {
				return err
			}
			// 업데이트 or append
			found := false
			for i := range list {
				if list[i].FromAddress == from.FromAddress {
					list[i].LastTxId = from.LastTxId
					list[i].LastTime = from.LastTime
					if list[i].Volume < math.MaxInt32 {
						list[i].Volume++
					}
					found = true
					break
				}
			}
			if !found {
				if from.Volume <= 0 {
					from.Volume = 1
				}
				list = append(list, from)
			}
			b, e := json.Marshal(list)
			if e != nil {
				return e
			}
			if e = txn.Set(key, b); e != nil {
				return e
			}

			// 삭제 횟수 기반 GC 트리거 판단(선택)
			delCnt, e := readU64(txn, []byte(metaDeleteCntKey))
			if e == nil && delCnt%gcDeleteTriggerN == 0 && delCnt > 0 {
				needGC = true
			}
			return nil

		case badger.ErrKeyNotFound:
			// 신규 키
			if from.Volume <= 0 {
				from.Volume = 1
			}
			list = []FromScala{from}
			b, e := json.Marshal(list)
			if e != nil {
				return e
			}
			if e = txn.Set(key, b); e != nil {
				return e
			}
			if e := incrU64(txn, []byte(metaCountKey), 1); e != nil {
				return e
			}
			return nil

		default:
			return err
		}
	}

	// 1) MVCC 낙관적 재시도
	var lastErr error
	for i := 0; i < r.maxRetries; i++ {
		if err := r.db.Update(fn); err != nil {
			lastErr = err
			if err == badger.ErrConflict {
				continue // 재시도
			}
			// 다른 에러는 즉시 리턴
			return fmt.Errorf("AddToPendingRelations: update failed: %w", err)
		}
		lastErr = nil
		break
	}

	// 2) 계속 충돌이면 샤드 락으로 직렬화 → mustProcess 1회
	if lastErr == badger.ErrConflict {
		ix := r.shardIndex(key)
		r.shards[ix].Lock()
		defer r.shards[ix].Unlock()

		if err := r.db.Update(fn); err != nil {
			return fmt.Errorf("AddToPendingRelations: serialized update failed: %w", err)
		}
	}

	// 3) 트랜잭션 밖에서 value log GC(선택적)
	if needGC {
		go r.runGC()
	}
	return nil
}

func (r *BadgerPendingRelationRepo) CountPendingRelations() int {
	var v uint64
	_ = r.db.View(func(txn *badger.Txn) error {
		n, err := readU64(txn, []byte(metaCountKey))
		if err == nil {
			v = n
		}
		return nil
	})
	if v > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(v)
}

// runGC: Badger GC 실행 (비동기)
func (r *BadgerPendingRelationRepo) runGC() {
	for {
		if err := r.db.RunValueLogGC(gcDiscardRatio); err != nil {
			break // reclaim 없음 또는 에러 → 중단
		}
	}
}

// ---- 내부 유틸 (샤딩/해시/보조) ----

func (r *BadgerPendingRelationRepo) shardIndex(key []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return h.Sum32() & r.shardMask
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	x := 1
	for x < n {
		x <<= 1
	}
	return x
}
