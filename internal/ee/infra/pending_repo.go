package infra

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
)

type PendingRelationRepo interface {
	Close() error
	GetPendingRelations(toAddr domain.Address) ([]domain.Address, error)
	DeletePendingRelations(toAddr domain.Address) error
	AddToPendingRelations(toAddr, fromAddr domain.Address) error
	CountPendingRelations() int
}
type BadgerPendingRelationRepo struct {
	db *badger.DB
}

// NewBadgerGraphRepository creates a new BadgerDB graph repository
func NewBadgerPendingRelationRepo(dbPath string) (*BadgerPendingRelationRepo, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable badger logging

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	return &BadgerPendingRelationRepo{
		db: db,
	}, nil
}

// Close closes the BadgerDB connection
func (r *BadgerPendingRelationRepo) Close() error {
	return r.db.Close()
}

func (r *BadgerPendingRelationRepo) GetPendingRelations(toAddr domain.Address) ([]domain.Address, error) {
	var fromAddresses []string

	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(toAddr.String()))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &fromAddresses)
		})
	})

	if err == badger.ErrKeyNotFound {
		return []domain.Address{}, nil // Return empty slice if not found
	}
	serializedFromAddress, err := fp.MapOrError(fromAddresses, domain.ParseAddressFromString)

	return serializedFromAddress, err
}

func (r *BadgerPendingRelationRepo) DeletePendingRelations(toAddr domain.Address) error {
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(toAddr.String()))
	})
}

// AddToPendingRelations adds a from address to the list of a to address
func (r *BadgerPendingRelationRepo) AddToPendingRelations(toAddr, fromAddr domain.Address) error {
	return r.db.Update(func(txn *badger.Txn) error {
		// Get existing relations
		var fromAddresses []string
		item, err := txn.Get([]byte(toAddr.String()))
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		if err == nil {
			err = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &fromAddresses)
			})
			if err != nil {
				return err
			}
		}

		// Check if from address already exists
		for _, existing := range fromAddresses {
			if existing == fromAddr.String() {
				return nil // Already exists
			}
		}

		// Add new from address
		fromAddresses = append(fromAddresses, fromAddr.String())

		// Save updated list
		data, err := json.Marshal(fromAddresses)
		if err != nil {
			return err
		}

		return txn.Set([]byte(toAddr.String()), data)
	})
}

// TODO 현재는 순회를 통해서 카운트함. 성능 개선 필요
// ! 주요 성능 개선 필요 구간임!
func (r *BadgerPendingRelationRepo) CountPendingRelations() int {
	count := 0
	r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return count
}
