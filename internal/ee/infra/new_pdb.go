package infra

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

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

// PendingDB의 key는 toAddress, Val은 FromScala
type FromScala struct {
	FromAddress domain.Address       `json:"from_address"`
	LastTxId    domain.TxId          `json:"last_tx_id"`
	LastTime    chaintimer.ChainTime `json:"last_time"`
	Volume      int32                `json:"volum"`
}

type FFBadgerPendingRelationRepo struct {
	db *badger.DB
}

const (
	metaCountKey     = "__meta:pending_relations_count__" // 현재 to-key 개수
	metaDeleteCntKey = "__meta:pending_relations_delete_cnt__"
	gcDeleteTriggerN = 1024 // 삭제 N회마다 GC 시도
	gcDiscardRatio   = 0.5  // badger.RunValueLogGC 매개값
)

// NewBadgerGraphRepository creates a new BadgerDB graph repository
func NewFFBadgerPendingRelationRepo(dbPath string) (*FFBadgerPendingRelationRepo, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable badger logging

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	// 메타 카운터 키 초기화
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

	return &FFBadgerPendingRelationRepo{db: db}, nil
}

// Close closes the BadgerDB connection
func (r *FFBadgerPendingRelationRepo) Close() error {
	return r.db.Close()
}

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

func (r *FFBadgerPendingRelationRepo) GetPendingRelations(toAddr domain.Address) ([]FromScala, error) {
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

func (r *FFBadgerPendingRelationRepo) DeletePendingRelations(toAddr domain.Address) error {
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

		// to-key 개수 감소
		if err := incrU64(txn, []byte(metaCountKey), -1); err != nil {
			return err
		}
		// 삭제 횟수 증가
		if err := incrU64(txn, []byte(metaDeleteCntKey), 1); err != nil {
			return err
		}

		// 임계치 도달 시 GC 시도
		delCnt, err := readU64(txn, []byte(metaDeleteCntKey))
		if err == nil && delCnt%gcDeleteTriggerN == 0 {
			// 트랜잭션 외부에서 GC를 돌려야 하므로 여기서는 마킹만 하고
			// 커밋 후 호출자가 끝나면 아래 defer에서 GC
		}
		return nil
	})
}

// AddToPendingRelations:
// - FromAddress 가 없으면 append (Volum==0이면 1로 보정)
// - 있으면 LastTxId/LastTime 덮어쓰기 + Volum++
func (r *FFBadgerPendingRelationRepo) AddToPendingRelations(toAddr domain.Address, from FromScala) error {
	key := []byte(toAddr.String())
	var needGC bool

	err := r.db.Update(func(txn *badger.Txn) error {
		var list []FromScala
		itm, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			// 신규 키 생성 → 카운터 +1
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
		}
		if err != nil {
			return err
		}

		if err := itm.Value(func(val []byte) error {
			return json.Unmarshal(val, &list)
		}); err != nil {
			return err
		}

		// 존재 여부 체크
		found := false
		for i := range list {
			if list[i].FromAddress == from.FromAddress {
				// 업데이트: Tx/Time 덮어쓰기 + Volum++
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
			// 신입 추가
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

		// add도 공간 변동이 크면 주기적으로 GC를 고려할 수 있음:
		// 여기서는 삭제 횟수 기준으로만 GC → 삭제 카운터 읽어서 트리거 판단
		delCnt, e := readU64(txn, []byte(metaDeleteCntKey))
		if e == nil && delCnt%gcDeleteTriggerN == 0 && delCnt > 0 {
			needGC = true
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 트랜잭션 밖에서 value log GC 시도 (비동기)
	if needGC {
		go r.runGC() // 고루틴으로 비동기 실행
	}
	return nil
}

func (r *FFBadgerPendingRelationRepo) CountPendingRelations() int {
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
func (r *FFBadgerPendingRelationRepo) runGC() {
	fmt.Printf("Running GC for pending relation repository (delete count reached %d)\n", gcDeleteTriggerN)
	
	// Badger의 내장 GC 실행
	for {
		if err := r.db.RunValueLogGC(gcDiscardRatio); err != nil {
			break // reclaim 없음 혹은 에러 → 중단
		}
	}
	
	fmt.Println("Pending relation repository GC completed")
}
