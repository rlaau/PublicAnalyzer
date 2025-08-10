package infra

import (
	"encoding/json"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type Repo struct{ db *badger.DB }

func NewRepo(path string) (*Repo, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &Repo{db: db}, nil
}
func (r *Repo) Close() error { return r.db.Close() }

// UpsertBatch: 존재하면 덮어쓰기(필요 시 merge 전략 넣기)
func (r *Repo) UpsertBatch(items []domain.Contract) error {
	wb := r.db.NewWriteBatch()
	defer wb.Cancel()

	for i := range items {
		val, _ := json.Marshal(&items[i])
		if err := wb.Set(domain.KeyFor(items[i].Address), val); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (r *Repo) Get(addr string) (*domain.Contract, error) {
	var out domain.Contract
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
