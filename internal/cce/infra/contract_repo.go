package infra

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// TODO 추후 GC는 더 업글하기. 현재는 그냥 무지성 고루틴임
const (
	gcTriggerCount = 50_000 // UpsertBatch 5만 번마다 GC 실행
)

type Repo struct {
	db         *badger.DB
	batchCount int64 // UpsertBatch 호출 횟수
}

func NewRepo(path string) (*Repo, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &Repo{db: db}, nil
}
func (r *Repo) Close() error { return r.db.Close() }

// UpsertBatch: 존재하면 덮어쓰기(필요 시 merge 전략 넣기)
func (r *Repo) UpsertBatch(items []domain.RawContract) error {
	wb := r.db.NewWriteBatch()
	defer wb.Cancel()

	for i := range items {
		val, _ := json.Marshal(&items[i])
		if err := wb.Set(domain.KeyFor(items[i].Address), val); err != nil {
			return err
		}
	}

	err := wb.Flush()
	if err != nil {
		return err
	}

	// 배치 카운트 증가 및 GC 체크
	count := atomic.AddInt64(&r.batchCount, 1)
	if count >= gcTriggerCount {
		atomic.StoreInt64(&r.batchCount, 0) // 카운터 리셋
		go r.runGC()                        // 고루틴으로 비동기 실행
	}

	return nil
}

func (r *Repo) Get(addr string) (*domain.RawContract, error) {
	var out domain.RawContract
	err := r.db.View(func(txn *badger.Txn) error {
		itm, e := txn.Get(domain.KeyFor(addr))
		if e != nil {
			return e
		}
		return itm.Value(func(v []byte) error { return json.Unmarshal(v, &out) })
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// runGC: Badger GC 실행 (0.4 임계값 사용)
func (r *Repo) runGC() {
	fmt.Printf("Running GC for contract repository (batch count reached %d)\n", gcTriggerCount)

	// Badger의 내장 GC 실행
	for {
		err := r.db.RunValueLogGC(0.4) // 40% 임계값으로 value log GC
		if err != nil {
			break
		}
	}

	fmt.Println("Contract repository GC completed")
}
