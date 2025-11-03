// ===== rope_agg_cache.go =====
package app

import (
	"fmt"
	"sync"

	"github.com/shopspring/decimal"

	chaintimer "github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// ---- Badger용 키 생성(원하면 나중에 BE u64 고정키로 교체) ----
func makeRopeKeyPrefixFmt(id domain.RopeID) []byte {
	return []byte(fmt.Sprintf("%s%d", kR, uint64(id)))
}

// ---- 캐시 엔트리: base(DB) + delta(agg) ----
type ropeAggEntry struct {
	// base: DB에서 1회 로드
	base       domain.RopeMark
	baseLoaded bool

	// delta: 이번 배치 동안의 누적
	addMembers  []shareddomain.Address
	valueDelta  domain.ValueOrNull
	volumeDelta uint32
	lastSeenMax chaintimer.ChainTime
	trapDelta   []domain.TrapVertex
	toDelete    []domain.RopeID // 머지 등으로 삭제할 로프
	trait       domain.TraitCode
	dirty       bool
}

// 스냅샷: base + delta를 합쳐 일관된 도메인 뷰 생성
func (e *ropeAggEntry) snapshot() domain.RopeMark {
	rm := e.base
	// Trait/ID는 base가 없을 수도 있으니 보정
	rm.Trait = e.trait

	// Members
	if len(e.addMembers) > 0 {
		rm.Members = computation.DedupAppend(rm.Members, e.addMembers)
		rm.Size = uint32(len(rm.Members))
	} else {
		rm.Size = uint32(len(rm.Members))
	}

	// Value / Volume
	if !e.valueDelta.IsZero() {
		rm.ValueOrNull = rm.ValueOrNull.Add(e.valueDelta)
	}
	if e.volumeDelta > 0 {
		rm.Volume += e.volumeDelta
	}

	// LastSeen
	if e.lastSeenMax.After(rm.LastSeen) {
		rm.LastSeen = e.lastSeenMax
	}

	// Trap
	if len(e.trapDelta) > 0 {
		rm.TrapVertexes = MergeTrapVerticesUniquePairs(rm.TrapVertexes, e.trapDelta)
	}

	return rm
}

type kvPut struct{ key, val []byte }
type kvDel struct{ key []byte }

// ---- 캐시 본체 ----
type RopeAggCache struct {
	mu          sync.RWMutex
	m           map[domain.RopeID]*ropeAggEntry
	loadDB      func(id domain.RopeID) domain.RopeMark // DB 로더 주입
	makeRopeKey func(domain.RopeID) []byte             // 키 생성기
}

func NewRopeAggCache(loader func(domain.RopeID) domain.RopeMark) *RopeAggCache {
	return &RopeAggCache{
		m:           make(map[domain.RopeID]*ropeAggEntry),
		loadDB:      loader,
		makeRopeKey: func(id domain.RopeID) []byte { return makeRopeKeyPrefixFmt(id) },
	}
}

// id, trait를 만족하는 rope를 로드
func (c *RopeAggCache) ensure(id domain.RopeID, trait domain.TraitCode) *ropeAggEntry {
	e := c.m[id]
	if e != nil {
		return e
	}
	e = &ropeAggEntry{trait: trait}
	// DB 1회 로드
	rm := c.loadDB(id)
	e.base = rm
	e.baseLoaded = true

	c.m[id] = e
	return e
}

// 외부에서 “현재 일관 뷰”를 읽고 싶을 때 (읽기 전용)
func (c *RopeAggCache) Snapshot(id domain.RopeID, trait domain.TraitCode) domain.RopeMark {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e := c.ensure(id, trait) // ensure는 write지만 single-writer 전제면 안전; 엄밀히는 ensureRO 따로 두는게 이상적
	return e.snapshot()
}

// 워커가 Upsert를 누적
func (c *RopeAggCache) Upsert(op domain.RopeMarkUpsert) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.ensure(op.RopeID, op.Trait)
	// members
	if len(op.AddMembers) > 0 {
		// 델타 값은 일단 그냥 append시킴.
		//추후 snapshot과정에서 dedup됨.
		e.addMembers = append(e.addMembers, op.AddMembers...)
	}
	// value/volume/lastSeen
	if !op.ValueOrNull.IsZero() {
		e.valueDelta = e.valueDelta.Add(op.ValueOrNull)
	}
	if op.VolumeDelta > 0 {
		e.volumeDelta += op.VolumeDelta
	}
	if op.LastSeen.After(e.lastSeenMax) {
		e.lastSeenMax = op.LastSeen
	}
	// traps
	if len(op.TrapVertexes) > 0 {
		e.trapDelta = MergeTrapVerticesUniquePairs(e.trapDelta, op.TrapVertexes)
	}
	// delete 예약(일반 upsert에서 MergeFrom이 전달될 수도 있음)
	if len(op.MergeFrom) > 0 {
		e.toDelete = append(e.toDelete, op.MergeFrom...)
	}
	e.dirty = true
}

// ConsumeMerge는 타겟과 소스를 병합하는 "업서트 오퍼레이션"을 생성해줌.
// 그리고 나서 "소스"를 어그리게이터에서 제거함.
func (c *RopeAggCache) GetMergingRopeUpsertOpertation(target, source domain.RopeID, trait domain.TraitCode) domain.RopeMarkUpsert {
	c.mu.Lock()
	defer c.mu.Unlock()

	te := c.ensure(target, trait)
	se := c.ensure(source, trait)

	// source 스냅샷
	srcSnap := se.snapshot()

	// target 스냅샷 기반으로 합치되, 실제 저장은 delta에 쌓기
	combined := make([]shareddomain.Address, len(srcSnap.Members)) // len = n
	copy(combined, srcSnap.Members)                                // ✅ n개 복사

	// 삭제 예약
	te.toDelete = append(te.toDelete, source)

	// source 엔트리 제거(이후 다시 생성될 수 있으나, 이번 배치에선 제거)
	delete(c.m, source)

	te.dirty = true

	return domain.RopeMarkUpsert{
		RopeID:       te.base.ID,
		TrapVertexes: srcSnap.TrapVertexes,
		Trait:        trait,
		AddMembers:   combined,
		SizeDelta:    int32(srcSnap.Size),
		VolumeDelta:  srcSnap.Volume,
		ValueOrNull:  srcSnap.ValueOrNull,
	}
}

// 플러시 배치 만들기: 마샬링된 put/del 준비 + 스냅샷 반환
// 주의: 여기서는 캐시를 건드리지 않음. CommitFlush에서 정리.
func (c *RopeAggCache) BuildFlushBatch(marshal func(domain.RopeMark) ([]byte, error)) (puts []kvPut, dels []kvDel, snaps map[domain.RopeID]domain.RopeMark) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	puts = make([]kvPut, 0, len(c.m))
	dels = make([]kvDel, 0, len(c.m))
	snaps = make(map[domain.RopeID]domain.RopeMark, len(c.m))

	seenDel := make(map[domain.RopeID]struct{})

	for id, e := range c.m {
		if !e.dirty {
			continue
		}
		snap := e.snapshot()
		data, err := marshal(snap)
		if err != nil {
			// 호출측에서 로그
			continue
		}
		puts = append(puts, kvPut{key: c.makeRopeKey(id), val: data})
		snaps[id] = snap

		for _, sid := range e.toDelete {
			if sid == id {
				continue
			}
			if _, ok := seenDel[sid]; ok {
				continue
			}
			seenDel[sid] = struct{}{}
			dels = append(dels, kvDel{key: c.makeRopeKey(sid)})
		}
	}
	return
}

// DB 반영 성공 후 델타 정리
// DB 반영 성공 후 델타 정리(스냅샷 재활용 버전)
func (c *RopeAggCache) CommitFlushSnaps(snaps map[domain.RopeID]domain.RopeMark) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, snap := range snaps {
		if e, ok := c.m[id]; ok {
			// 스냅샷을 그대로 base로 채택 (깊은 복사 아님: slice header만 복사)
			e.base = snap

			// 델타 리셋
			if len(e.addMembers) > 0 {
				e.addMembers = e.addMembers[:0]
			}
			e.valueDelta = domain.ParseValueOrNull(decimal.Zero)
			e.volumeDelta = 0
			if len(e.trapDelta) > 0 {
				e.trapDelta = nil
			}
			if len(e.toDelete) > 0 {
				e.toDelete = nil
			}
			e.dirty = false
		}
	}
}
