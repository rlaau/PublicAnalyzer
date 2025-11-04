package app

import (
	"encoding/json"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
)

// GetGraphStats: v:는 V-DB에서, r:/t:/pr:는 M-DB에서 집계 (락 없이 - 약간의 오차 허용)
func (b *BadgerRopeDB) GetGraphStats() map[string]any {
	var nodes, ropes, traits, polyRopes uint64

	// V-DB 조회 (락 없이 - 성능 우선, 약간의 오차 허용)
	_ = b.dbV.View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = false
		it := txn.NewIterator(itOpts)
		defer it.Close()
		pfx := []byte(kV)
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			nodes++
		}
		return nil
	})

	// M-DB 조회 (원래부터 락 불필요)
	_ = b.dbM.View(func(txn *badger.Txn) error {
		countPrefix := func(pfx []byte) uint64 {
			var c uint64
			itOpts := badger.DefaultIteratorOptions
			itOpts.PrefetchValues = false
			it := txn.NewIterator(itOpts)
			defer it.Close()
			for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
				c++
			}
			return c
		}
		ropes = countPrefix([]byte(kR))
		traits = countPrefix([]byte(kT))
		polyRopes = countPrefix([]byte(kPR))
		return nil
	})

	return map[string]any{
		"nodes":     nodes,
		"ropes":     ropes,
		"traits":    traits,
		"polyRopes": polyRopes,
	}
}

func (b *BadgerRopeDB) ViewAllTraitMarkByCode(t domain.TraitCode) ([]domain.TraitMark, error) {
	var result []domain.TraitMark
	err := b.dbM.View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		it := txn.NewIterator(itOpts)
		defer it.Close()

		pfx := []byte(kT)
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			item := it.Item()
			var tm domain.TraitMark
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &tm)
			})
			if err != nil {
				continue
			}
			if tm.Trait == t {
				result = append(result, tm)
			}
		}
		return nil
	})
	return result, err
}

// 내부: 랜덤 샘플링 후 Members가 있는 것 중 최대 Volume 선택
func (b *BadgerRopeDB) topRopeFromRandomSample(sampleSize int) (ropedomain.RopeID, ropedomain.RopeMark) {
	var candidates []ropedomain.RopeMark
	_ = b.dbM.View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		it := txn.NewIterator(itOpts)
		defer it.Close()
		pfx := []byte(kR)
		count := 0
		for it.Seek(pfx); it.ValidForPrefix(pfx) && count < sampleSize; it.Next() {
			item := it.Item()
			var rm ropedomain.RopeMark
			_ = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &rm)
			})
			if rm.ID == 0 || len(rm.Members) == 0 {
				continue // Members가 없는 rope는 건너뜀
			}
			candidates = append(candidates, rm)
			count++
		}
		return nil
	})

	// 샘플 중 최고 선택
	var best ropedomain.RopeMark
	for _, rm := range candidates {
		if betterRope(rm, best) {
			best = rm
		}
	}
	return best.ID, best
}
