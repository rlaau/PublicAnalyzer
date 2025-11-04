package app

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/shopspring/decimal"
)

const (
	kV         = "v:"       // v:<address>
	kR         = "r:"       // r:<ropeID>
	kT         = "t:"       // t:<traitID>
	kPR        = "pr:"      // pr:<polyRopeID> - PolyRopeMark 저장
	kCRope     = "ctr:rope" // BigEndian uint64
	kCTrait    = "ctr:trait"
	kCPolyRope = "ctr:polyrope" // BigEndian uint64 - PolyRope counter
)

// vKey - address key (lowercase normalized)
func vKey(a shareddomain.Address) []byte {
	// "v:" + 40 hex = 42 bytes
	prefix := []byte(kV)
	addrBytes := a.LowerBytes() // already 40 lowercase hex bytes
	out := make([]byte, len(prefix)+len(addrBytes))
	copy(out, prefix)
	copy(out[len(prefix):], addrBytes)
	return out
}

// rKey - rope key
func rKey(ropeID domain.RopeID) []byte {
	// "r:" + big-endian uint64 (8B)
	prefix := []byte(kR)
	out := make([]byte, len(prefix)+8)
	copy(out, prefix)
	binary.BigEndian.PutUint64(out[len(prefix):], uint64(ropeID))
	return out
}

// tKey - trait key
func tKey(traitID domain.TraitID) []byte {
	// "t:" + big-endian uint64 (8B)
	prefix := []byte(kT)
	out := make([]byte, len(prefix)+8)
	copy(out, prefix)
	binary.BigEndian.PutUint64(out[len(prefix):], uint64(traitID))
	return out
}

// prKey - polyrope key
func prKey(polyRopeID domain.PolyRopeID) []byte {
	// "pr:" + big-endian uint64 (8B)
	prefix := []byte(kPR)
	out := make([]byte, len(prefix)+8)
	copy(out, prefix)
	binary.BigEndian.PutUint64(out[len(prefix):], uint64(polyRopeID))
	return out
}

// ----- NoLock 버전: Push 경로에서 muV 이미 보유함 -----
func (b *BadgerRopeDB) getOrCreateVertexNoLock(a shareddomain.Address) *domain.Vertex {
	key := vKey(a)
	var v domain.Vertex
	err := b.dbV.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error {
			return json.Unmarshal(val, &v)
		})
	})
	if err == nil && !shareddomain.IsNullAddress(v.Address) {
		return &v
	}
	return &domain.Vertex{Address: a, Ropes: nil, Traits: nil, PolyRopes: nil}
}
func (b *BadgerRopeDB) putVertexNoLock(v *domain.Vertex) {
	key := vKey(v.Address)
	err := b.dbV.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return txn.Set(key, data)
	})
	if err != nil {
		fmt.Printf("putVertexNoLock failed: key=%s err=%v\n", key, err)
	}
}

func (b *BadgerRopeDB) writeVertices(vertices []*domain.Vertex) error {
	if len(vertices) == 0 {
		return nil
	}
	return b.dbV.Update(func(txn *badger.Txn) error {
		for _, v := range vertices {
			key := vKey(v.Address)

			// JSON 직렬화
			data, err := json.Marshal(v)
			if err != nil {
				continue
			}

			if err := txn.Set(key, data); err != nil {
				return err
			}
		}
		return nil
	})
}

const MaxRopeSize uint32 = 1000

func (b *BadgerRopeDB) getRopeMark(id domain.RopeID) domain.RopeMark {
	key := rKey(id)
	var rm domain.RopeMark
	_ = b.dbM.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error {
			return json.Unmarshal(val, &rm)
		})
	})
	return rm
}

func (b *BadgerRopeDB) updateRopeMark(op domain.RopeMarkUpsert) error {
	if b.ropeSizeCache.Get(op.RopeID) >= MaxRopeSize {
		//Max size보다 클 시엔 그냥 리턴
		return nil
	}
	key := rKey(op.RopeID)
	return b.dbM.Update(func(txn *badger.Txn) error {
		var rm domain.RopeMark
		if itm, err := txn.Get(key); err == nil {
			_ = itm.Value(func(val []byte) error {
				return json.Unmarshal(val, &rm)
			})
		}
		if rm.ID == 0 {
			rm = domain.RopeMark{
				ID:          op.RopeID,
				Trait:       op.Trait,
				Size:        0,
				Members:     nil,
				Volume:      0,
				LastSeen:    op.LastSeen,
				ValueOrNull: shareddomain.ParseValueOption(decimal.Zero),
			}
		}

		// 값 누적
		if !op.ValueOrNull.V.IsZero() {
			rm.ValueOrNull = shareddomain.ParseValueOption(rm.ValueOrNull.V.Add(op.ValueOrNull.V))
		}
		if len(op.AddMembers) > 0 {
			rm.Members = computation.DedupAppend(rm.Members, op.AddMembers)
		}
		if op.SizeDelta != 0 {
			rm.Size = uint32(len(rm.Members))
		}
		if op.VolumeDelta > 0 {
			rm.Volume += op.VolumeDelta
		}
		if op.LastSeen.After(rm.LastSeen) {
			rm.LastSeen = op.LastSeen
		}
		for _, sid := range op.MergeFrom {
			if sid == rm.ID {
				continue
			}
			skey := []byte(fmt.Sprintf("%s%d", kR, uint64(sid)))
			_ = txn.Delete(skey)
		}
		if int(rm.Size) != len(rm.Members) {
			rm.Size = uint32(len(rm.Members))
		}
		if len(op.TrapVertexes) > 0 {
			rm.TrapVertexes = MergeTrapVerticesUniquePairs(rm.TrapVertexes, op.TrapVertexes)
		}

		// JSON Marshal로 저장
		data, err := json.Marshal(rm)
		if err != nil {
			return err
		}

		// 사이즈 캐시 갱신
		if b.ropeSizeCache != nil {
			b.ropeSizeCache.Put(op.RopeID, rm.Size)
		}

		return txn.Set(key, data)
	})
}

func (b *BadgerRopeDB) getPolyRopeMark(id domain.PolyRopeID) domain.PolyRopeMark {
	key := prKey(id)
	var prm domain.PolyRopeMark
	_ = b.dbM.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error {
			return json.Unmarshal(val, &prm)
		})
	})
	return prm
}
func (b *BadgerRopeDB) updatePolymarkAgg(agg map[domain.PolyRopeID]*polyRopeAgg) error {
	return b.dbM.Update(func(txn *badger.Txn) error {
		// Set
		for id, a := range agg {
			key := prKey(id)

			// JSON Marshal
			data, err := json.Marshal(a.prm)
			if err != nil {
				return err
			}
			if err := txn.Set(key, data); err != nil {
				return err
			}
		}
		// Delete (mergeFrom)
		for _, a := range agg {
			for _, sid := range a.toDelete {
				if sid == a.prm.ID {
					continue
				}
				sKey := []byte(fmt.Sprintf("%s%d", kPR, uint64(sid)))
				_ = txn.Delete(sKey)
			}
		}
		return nil
	})
}

// getTraitMark - TraitMark 로드
func (b *BadgerRopeDB) getTraitMark(id domain.TraitID) domain.TraitMark {
	key := tKey(id)
	var tm domain.TraitMark

	_ = b.dbM.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &tm)
		})
	})

	return tm
}

func (b *BadgerRopeDB) updateTraitAgg(agg map[domain.TraitID]*traitAgg) error {
	return b.dbM.Update(func(txn *badger.Txn) error {
		for id, a := range agg {
			key := tKey(id)

			// JSON Marshal
			data, err := json.Marshal(a.tm)
			if err != nil {
				return err
			}
			if err := txn.Set(key, data); err != nil {
				return err
			}
		}
		return nil
	})
}
