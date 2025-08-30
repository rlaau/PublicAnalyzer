// file: internal/apool/rel/triplet/infra/pending_repo.go
package infra

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type PendingRelationRepo interface {
	Close() error
	GetPendingRelations(toAddr domain.Address) ([]FromScala, error)
	DeletePendingRelations(toAddr domain.Address) error
	// 새로 추가: 배치 삭제 (반환값: 실제 삭제된 키 개수)
	DeletePendingRelationsBatch(addrs []domain.Address) (int, error)
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

	// per-key 직렬화(핫키)용 샤드 락 (1024 샤드)
	shards     []sync.Mutex
	shardMask  uint32
	maxRetries int

	// 전역 메타 키(__meta:*) 직렬화용 락
	metaMu sync.Mutex
}

const (
	metaCountKey     = "__meta:pending_relations_count__"
	metaDeleteCntKey = "__meta:pending_relations_delete_cnt__"
	gcDeleteTriggerN = 1024
	gcDiscardRatio   = 0.5
)

// 기존 호출자와 이름 호환
func NewBadgerPendingRelationRepo(dbPath string) (*BadgerPendingRelationRepo, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	// 메타 키 초기화(없으면 0 설정)
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

	numShards := 1024 // 2^10
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
// 샤드 유틸

func (r *BadgerPendingRelationRepo) shardIndexFromAddr(a domain.Address) uint32 {
	// 1024 샤드 → 하위 10비트 사용 후 마스크
	return uint32(a.HashAddress1024()) & r.shardMask
}

// writer 직렬화가 진행 중이면 “짧게 대기 후 통과”
//
//nolint:staticcheck // SA2001: 의도적으로 writer 종료 대기(빈 크리티컬 섹션)
func (r *BadgerPendingRelationRepo) waitShardFree(ix uint32) {
	r.shards[ix].Lock()
	r.shards[ix].Unlock()
}

// ───────────────────────────────────────────────────────────────────────────────

func (r *BadgerPendingRelationRepo) GetPendingRelations(toAddr domain.Address) ([]FromScala, error) {
	var list []FromScala

	// 읽기는 MVCC로 자유롭게. 다만 같은 샤드에 writer 직렬화 중이면 잠깐 대기.
	ix := r.shardIndexFromAddr(toAddr)
	r.waitShardFree(ix)

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

// 삭제(단건): 낙관적 재시도 → 충돌 시 샤드락+메타락 직렬화 경로 재시도
func (r *BadgerPendingRelationRepo) DeletePendingRelations(toAddr domain.Address) error {
	_, err := r.DeletePendingRelationsBatch([]domain.Address{toAddr})
	return err
}

// 배치 삭제: 샤드별로 묶어 정렬 → 샤드 단위로 트랜잭션 수행(메타키 보정은 델타 합산 1회)
func (r *BadgerPendingRelationRepo) DeletePendingRelationsBatch(addrs []domain.Address) (int, error) {
	if len(addrs) == 0 {
		return 0, nil
	}

	// 1) 중복 제거 & 샤드별 묶기
	perShard := make(map[uint32][]domain.Address, 64)
	seen := make(map[domain.Address]struct{}, len(addrs))
	for _, a := range addrs {
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		ix := r.shardIndexFromAddr(a)
		perShard[ix] = append(perShard[ix], a)
	}

	// 2) 샤드 인덱스 정렬(락 순서 고정)
	ixs := make([]int, 0, len(perShard))
	for ix := range perShard {
		ixs = append(ixs, int(ix))
	}
	sort.Ints(ixs)

	totalDeleted := 0

	// 3) 샤드 단위로 처리
	for _, ixi := range ixs {
		ix := uint32(ixi)
		keys := perShard[ix]

		// 낙관적 재시도 함수
		try := func(txn *badger.Txn) (int, error) {
			deleted := 0
			for _, a := range keys {
				k := []byte(a.String())
				if _, err := txn.Get(k); err == badger.ErrKeyNotFound {
					continue
				} else if err != nil {
					return 0, err
				}
				if err := txn.Delete(k); err != nil {
					return 0, err
				}
				deleted++
			}
			if deleted > 0 {
				if err := incrU64(txn, []byte(metaCountKey), -int64(deleted)); err != nil {
					return 0, err
				}
				if err := incrU64(txn, []byte(metaDeleteCntKey), int64(deleted)); err != nil {
					return 0, err
				}
			}
			return deleted, nil
		}

		// 3-1) 낙관적 재시도
		var lastErr error
		var shardDeleted int
		for i := 0; i < r.maxRetries; i++ {
			err := r.db.Update(func(txn *badger.Txn) error {
				n, e := try(txn)
				if e == nil {
					shardDeleted = n
				}
				return e
			})
			if err == nil {
				lastErr = nil
				break
			}
			lastErr = err
			if err != badger.ErrConflict {
				return totalDeleted, fmt.Errorf("DeletePendingRelationsBatch: update failed: %w", err)
			}
			runtime.Gosched()
		}

		// 3-2) 여전히 충돌 → 샤드락+메타락 직렬화 경로에서 성공할 때까지 재시도
		if lastErr == badger.ErrConflict {
			r.shards[ix].Lock()
			r.metaMu.Lock()

			const (
				maxSpin   = 256
				baseSleep = 100 * time.Microsecond
				maxSleep  = 2 * time.Millisecond
			)
			var err error
			for attempt := 0; attempt < maxSpin; attempt++ {
				err = r.db.Update(func(txn *badger.Txn) error {
					n, e := try(txn)
					if e == nil {
						shardDeleted = n
					}
					return e
				})
				if err == nil {
					break
				}
				if err != badger.ErrConflict {
					break
				}
				d := time.Duration(attempt+1) * baseSleep
				if d > maxSleep {
					d = maxSleep
				}
				time.Sleep(d)
			}

			r.metaMu.Unlock()
			r.shards[ix].Unlock()

			if err != nil {
				return totalDeleted, fmt.Errorf("DeletePendingRelationsBatch: serialized update failed: %w", err)
			}
		}

		totalDeleted += shardDeleted
	}

	return totalDeleted, nil
}

// AddToPendingRelations:
// - FromAddress 없으면 append (Volume<=0 → 1 보정) + metaCountKey++
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
			// 신규 키: metaCountKey++ (메타 키 수정)
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

	// 1) 낙관적 재시도 (빠름)
	var lastErr error
	for i := 0; i < r.maxRetries; i++ {
		if err := r.db.Update(fn); err != nil {
			lastErr = err
			if err == badger.ErrConflict {
				// 가벼운 양보
				runtime.Gosched()
				continue
			}
			return fmt.Errorf("AddToPendingRelations: update failed: %w", err)
		}
		lastErr = nil
		break
	}

	// 2) 계속 충돌 → 샤드락 + (신규키 가능성 대비) 메타락으로 직렬화 경로
	if lastErr == badger.ErrConflict {
		ix := r.shardIndexFromAddr(toAddr)
		r.shards[ix].Lock()
		r.metaMu.Lock()

		const (
			maxSpin   = 256
			baseSleep = 100 * time.Microsecond
			maxSleep  = 2 * time.Millisecond
		)
		var err error
		for attempt := 0; attempt < maxSpin; attempt++ {
			err = r.db.Update(fn)
			if err == nil {
				break
			}
			if err != badger.ErrConflict {
				break
			}
			// 백오프 (선형 + 캡)
			d := time.Duration(attempt+1) * baseSleep
			if d > maxSleep {
				d = maxSleep
			}
			time.Sleep(d)
		}

		r.metaMu.Unlock()
		r.shards[ix].Unlock()

		if err != nil {
			return fmt.Errorf("AddToPendingRelations: serialized update failed: %w", err)
		}
	}

	// 3) 트랜잭션 밖에서 GC(선택)
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

// Badger GC (비동기)
func (r *BadgerPendingRelationRepo) runGC() {
	for {
		if err := r.db.RunValueLogGC(gcDiscardRatio); err != nil {
			break // reclaim 없음 또는 에러 → 중단
		}
	}
}
