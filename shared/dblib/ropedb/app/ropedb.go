package app

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// ------------------------------------------------------------
// 1) 키-스키마 & 상수
// ------------------------------------------------------------

const (
	kV      = "v:"       // v:<address>
	kR      = "r:"       // r:<ropeID>
	kT      = "t:"       // t:<traitID>
	kCRope  = "ctr:rope" // BigEndian uint64
	kCTrait = "ctr:trait"

	maxRopeSize = 1000 // 도메인 상한
)

func NewRopeDB(isTest mode.ProcessingMode, dbName string, traitLegend map[domain.TraitCode]string, ruleLegend map[domain.RuleCode]string) (RopeDB, error) {
	var root string
	if isTest.IsTest() {
		root = computation.FindTestingStorageRootPath()
	} else {
		root = computation.FindProductionStorageRootPath()
	}
	return NewRopeDBWithRoot(isTest, root, dbName, traitLegend, ruleLegend)
}

type RopeDB interface {
	// 이벤트 입력(내부적으로 Vertex 동기 갱신 + Rope/Trait Upsert 큐잉)
	PushTraitEvent(ev domain.TraitEvent) error

	// 조회
	ViewRopeByNode(a shareddomain.Address) (*domain.Rope, error) // 첫 번째 Rope 기준(필요 시 Trait 선택 버전 추가)
	ViewRope(id domain.RopeID) (*domain.Rope, error)
	ViewInSameRope(a1, a2 shareddomain.Address) (bool, error)
	GetGraphStats() map[string]any

	// 트레이트 기반 조회
	ViewAllTraitMarkByCode(t domain.TraitCode) ([]domain.TraitMark, error)
	ViewAllTraitMarkByString(s string) ([]domain.TraitMark, error)

	Close() error
}

// ------------------------------------------------------------
// 5) 구현체
// ------------------------------------------------------------
// * BadgerDB자체가 SWMR모델이라서, 내 설계에 부합함. 쓰기는 비동기로 미루면서, 읽기 핫패스는 부담 줄이기 가능
type BadgerRopeDB struct {
	db     *badger.DB
	isTest bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 범례 (코드 → 문자열 매핑)
	traitLegend map[domain.TraitCode]string
	ruleLegend  map[domain.RuleCode]string

	// 비동기 쓰기 버스
	busRope  *eventbus.EventBus[domain.RopeMarkUpsert]
	busTrait *eventbus.EventBus[domain.TraitMarkUpsert]
}

// NewRopeDBWithRoot: 모드(true=test, false=prod)로 루트 분기
func NewRopeDBWithRoot(isTest mode.ProcessingMode, root string, dbname string, traitLegend map[domain.TraitCode]string, ruleLegend map[domain.RuleCode]string) (RopeDB, error) {
	// Badger
	dbDir := filepath.Join(root, "rope_db", dbname, "badger")
	opts := badger.DefaultOptions(dbDir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	// e.g. <root>/rope_db/<name>/eventbus/...
	evRopeRel := filepath.Join("rope_db", dbname, "eventbus", "ropemark.jsonl")
	evTraitRel := filepath.Join("rope_db", dbname, "eventbus", "traitmark.jsonl")
	// EventBus(JSONL 경로는 루트부터 분기)
	busR, err := eventbus.NewWithRoot[domain.RopeMarkUpsert](func() string { return root }, evRopeRel, 4096)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	busT, err := eventbus.NewWithRoot[domain.TraitMarkUpsert](func() string { return root }, evTraitRel, 4096)
	if err != nil {
		busR.Close()
		_ = db.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &BadgerRopeDB{
		db: db, isTest: isTest.IsTest(),
		ctx: ctx, cancel: cancel,
		traitLegend: traitLegend,
		ruleLegend:  ruleLegend,
		busRope:     busR, busTrait: busT,
	}

	// 워커 가동
	b.wg.Add(2)
	go b.traitWorker()
	go b.ropeWorker()
	return b, nil
}
func (b *BadgerRopeDB) RawBadgerDB() *badger.DB {
	return b.db
}

func (b *BadgerRopeDB) Close() error {
	b.cancel()
	// 이벤트버스: pending 저장 & 채널 close
	b.busTrait.Close()
	b.busRope.Close()
	b.wg.Wait()
	return b.db.Close()
}

// ------------------------------------------------------------
// 6) 퍼블릭 API (Push*, View*)
// ------------------------------------------------------------

func (b *BadgerRopeDB) PushTraitEvent(ev domain.TraitEvent) error {
	a1 := ev.AddressA
	a2 := ev.AddressB
	if shareddomain.IsNullAddress(a1) || shareddomain.IsNullAddress(a2) {
		return errors.New("address empty")
	}
	if a2.LessThan(a1) {
		a1, a2 = a2, a1
		ev.AddressA, ev.AddressB = ev.AddressB, ev.AddressA
		ev.RuleA, ev.RuleB = ev.RuleB, ev.RuleA
	}
	// --- 1) Vertex 동기 로드/생성
	v1 := b.getOrCreateVertex(a1)
	v2 := b.getOrCreateVertex(a2)

	// --- 2) 링크 업서트 → TraitID 확보(둘 다 동일 ID 사용)
	tid, created := b.ensureLink(v1, v2, ev.Trait)

	// --- 3) TraitMark Upsert 큐잉(단일 커맨드)
	_ = b.busTrait.Publish(domain.TraitMarkUpsert{
		TraitID:     tid,
		Trait:       ev.Trait,
		AddressA:    ev.AddressA,
		RuleA:       ev.RuleA,
		AddressB:    ev.AddressB,
		RuleB:       ev.RuleB,
		ScoreDelta:  ev.Score,
		VolumeDelta: 1,
		LastSeen:    ev.Time,
	})

	// --- 4) Rope 결정/병합
	r1, isExsit1 := b.ropeIDByTrait(v1, ev.Trait)
	r2, isExsit2 := b.ropeIDByTrait(v2, ev.Trait)
	switch {
	case !isExsit1 && !isExsit2:
		// 새 로프
		newID := b.nextRopeID()
		setRopeForTrait(v1, newID, ev.Trait)
		setRopeForTrait(v2, newID, ev.Trait)
		_ = b.busRope.Publish(domain.RopeMarkUpsert{
			RopeID:      newID,
			Trait:       ev.Trait,
			AddMembers:  []shareddomain.Address{a1, a2},
			SizeDelta:   2,
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})
	case isExsit1 && !isExsit2:
		// v1 로프에 v2 편입
		id := r1
		setRopeForTrait(v2, id, ev.Trait)
		_ = b.busRope.Publish(domain.RopeMarkUpsert{
			RopeID:      id,
			Trait:       ev.Trait,
			AddMembers:  []shareddomain.Address{a2},
			SizeDelta:   1,
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})
	case !isExsit1 && isExsit2:
		// v2 로프에 v1 편입
		id := r2
		setRopeForTrait(v1, id, ev.Trait)
		_ = b.busRope.Publish(domain.RopeMarkUpsert{
			RopeID:      id,
			Trait:       ev.Trait,
			AddMembers:  []shareddomain.Address{a1},
			SizeDelta:   1,
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})
	default:
		// 둘 다 있음 → 같은 로프면 볼륨만, 다르면 병합 지시
		id1, id2 := r1, r2
		// 근데 사실 도메인 로직상 이 둘이 절대 같은 로프 ID가 될수 없음
		if id1 == id2 {
			_ = b.busRope.Publish(domain.RopeMarkUpsert{
				RopeID:      id1,
				Trait:       ev.Trait,
				VolumeDelta: 1,
				LastSeen:    ev.Time,
			})
		} else {
			// 크기 비교 위해 로프마크 읽기(없으면 편의상 id1을 기준)
			rm1 := b.getRopeMark(id1)
			rm2 := b.getRopeMark(id2)
			target := id1
			source := id2
			if sizeOf(rm2) > sizeOf(rm1) {
				target, source = id2, id1
			}
			// source 멤버 전부 target으로 매핑
			src := b.getRopeMark(source)
			for _, addr := range src.Members {
				v := b.getOrCreateVertex(addr)
				setRopeForTrait(v, target, ev.Trait)
				b.putVertex(v)
			}
			// target 로프마크에 흡수 Upsert, source는 이후 제거/무시
			_ = b.busRope.Publish(domain.RopeMarkUpsert{
				RopeID:      target,
				Trait:       ev.Trait,
				AddMembers:  src.Members,
				SizeDelta:   int32(len(src.Members)),
				VolumeDelta: uint32(src.Volume) + 1, //거래 볼륨이므로. 상대+자신+이번 거래
				LastSeen:    ev.Time,
				MergeFrom:   []domain.RopeID{source},
			})
		}
	}

	// --- 5) Vertex 동기 저장
	b.putVertex(v1)
	b.putVertex(v2)
	_ = created // created 여부는 외부로 굳이 노출하지 않음
	return nil
}

// setRopeForTrait: 해당 Trait의 RopeRef를 정확히 1칸만 유지.
// - 이미 Trait 엔트리가 있으면 ID를 덮어씀
// - 없으면 새로 추가
func setRopeForTrait(v *domain.Vertex, id domain.RopeID, t domain.TraitCode) {
	for i := range v.Ropes {
		if v.Ropes[i].Trait == t {
			v.Ropes[i].ID = id
			return
		}
	}
	v.Ropes = append(v.Ropes, domain.RopeRef{ID: id, Trait: t})
}

func (b *BadgerRopeDB) ViewRopeByNode(a shareddomain.Address) (*domain.Rope, error) {
	v := b.getOrCreateVertex(a)
	if len(v.Ropes) == 0 {
		return nil, nil
	}
	ref := v.Ropes[0]
	return b.ViewRope(ref.ID)
}

func (b *BadgerRopeDB) ViewRope(id domain.RopeID) (*domain.Rope, error) {
	rm := b.getRopeMark(id)
	if rm.ID == 0 {
		return nil, nil
	}
	nodes := make([]domain.Vertex, 0, len(rm.Members))
	for _, addr := range rm.Members {
		nodes = append(nodes, *b.getOrCreateVertex(addr))
	}
	return &domain.Rope{ID: rm.ID, Trait: rm.Trait, Nodes: nodes}, nil
}

func (b *BadgerRopeDB) ViewInSameRope(a1, a2 shareddomain.Address) (bool, error) {
	v1 := b.getOrCreateVertex(a1)
	v2 := b.getOrCreateVertex(a2)
	return inSameRope(v1, v2), nil
}

// ------------------------------------------------------------
// 7) 워커(비동기 Upsert 적용)
// ------------------------------------------------------------

func (b *BadgerRopeDB) traitWorker() {
	defer b.wg.Done()
	for {
		select {
		case op, ok := <-b.busTrait.Dequeue():
			if !ok {
				return
			}
			b.applyTraitUpsert(op)
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *BadgerRopeDB) ropeWorker() {
	defer b.wg.Done()
	for {
		select {
		case op, ok := <-b.busRope.Dequeue():
			if !ok {
				return
			}
			b.applyRopeUpsert(op)
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *BadgerRopeDB) applyTraitUpsert(op domain.TraitMarkUpsert) {
	key := []byte(fmt.Sprintf("%s%d", kT, uint64(op.TraitID)))
	_ = b.db.Update(func(txn *badger.Txn) error {
		var tm domain.TraitMark
		if itm, err := txn.Get(key); err == nil {
			_ = itm.Value(func(val []byte) error { return json.Unmarshal(val, &tm) })
		}
		if tm.ID == 0 {
			// 신규 생성
			tm = domain.TraitMark{
				ID:       op.TraitID,
				Trait:    op.Trait,
				AddressA: op.AddressA,
				RuleA:    op.RuleA,
				AddressB: op.AddressB,
				RuleB:    op.RuleB,
				Score:    0,
				Volume:   0,
				LastSeen: op.LastSeen,
			}
		}
		tm.Score += op.ScoreDelta
		tm.Volume += op.VolumeDelta
		if op.LastSeen.After(tm.LastSeen) {
			tm.LastSeen = op.LastSeen
		}
		data, _ := json.Marshal(tm)
		return txn.Set(key, data)
	})
}

func (b *BadgerRopeDB) applyRopeUpsert(op domain.RopeMarkUpsert) {
	key := []byte(fmt.Sprintf("%s%d", kR, uint64(op.RopeID)))
	_ = b.db.Update(func(txn *badger.Txn) error {
		var rm domain.RopeMark
		if itm, err := txn.Get(key); err == nil {
			_ = itm.Value(func(val []byte) error { return json.Unmarshal(val, &rm) })
		}
		if rm.ID == 0 {
			rm = domain.RopeMark{
				ID:       op.RopeID,
				Trait:    op.Trait,
				Size:     0,
				Members:  nil,
				Volume:   0,
				LastSeen: op.LastSeen,
			}
		}
		// 멤버 추가 + dedup
		if len(op.AddMembers) > 0 {
			rm.Members = dedupAppend(rm.Members, op.AddMembers)
		}
		// 사이즈/볼륨 증분
		if op.SizeDelta != 0 {
			// Size는 Members 길이에 맞추고 싶다면 아래 한 줄로 고정화 가능:
			rm.Size = uint32(len(rm.Members))
		}
		if op.VolumeDelta > 0 {
			rm.Volume += op.VolumeDelta
		}
		if op.LastSeen.After(rm.LastSeen) {
			rm.LastSeen = op.LastSeen
		}
		// 병합 소스는 논리상 제거 처리(선택): 여기서는 단순히 소스 로프마크를 비워둠
		for _, sid := range op.MergeFrom {
			if sid == rm.ID {
				continue
			}
			skey := []byte(fmt.Sprintf("%s%d", kR, uint64(sid)))
			_ = txn.Delete(skey) // 간단 처리: 소스 로프마크 삭제(원복 필요하면 Tombstone 등으로 운영)
		}
		// Size를 Members 길이에 맞추려면 마지막에 동기화:
		if int(rm.Size) != len(rm.Members) {
			rm.Size = uint32(len(rm.Members))
		}
		data, _ := json.Marshal(rm)
		return txn.Set(key, data)
	})
}

// ------------------------------------------------------------
// 8) VertexDB 동기 I/O & 유틸
// ------------------------------------------------------------

func (b *BadgerRopeDB) getOrCreateVertex(a shareddomain.Address) *domain.Vertex {
	key := []byte(kV + a.String())
	var v domain.Vertex
	err := b.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error { return json.Unmarshal(val, &v) })
	})
	if err == nil && !shareddomain.IsNullAddress(v.Address) {
		return &v
	}
	return &domain.Vertex{Address: a, Ropes: nil, Traits: nil}
}

func (b *BadgerRopeDB) putVertex(v *domain.Vertex) {
	key := []byte(kV + v.Address.String())
	_ = b.db.Update(func(txn *badger.Txn) error {
		data, _ := json.Marshal(v)
		return txn.Set(key, data)
	})
}

// ropeIDByTrait는 ropeID와 함께 존재 여부 반환함
// (로프ID, isExist반환)
func (b *BadgerRopeDB) ropeIDByTrait(v *domain.Vertex, t domain.TraitCode) (domain.RopeID, bool) {
	for _, r := range v.Ropes {
		if r.Trait == t {
			return r.ID, true
		}
	}
	return 0, false
}

func inSameRope(v1, v2 *domain.Vertex) bool {
	seen := make(map[domain.RopeID]struct{}, len(v1.Ropes))
	for _, r := range v1.Ropes {
		seen[r.ID] = struct{}{}
	}
	for _, r := range v2.Ropes {
		if _, ok := seen[r.ID]; ok {
			return true
		}
	}
	return false
}

func (b *BadgerRopeDB) getRopeMark(id domain.RopeID) domain.RopeMark {
	key := []byte(fmt.Sprintf("%s%d", kR, uint64(id)))
	var rm domain.RopeMark
	_ = b.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error { return json.Unmarshal(val, &rm) })
	})
	return rm
}

func (b *BadgerRopeDB) nextTraitID() domain.TraitID {
	return domain.TraitID(b.incr(kCTrait))
}
func (b *BadgerRopeDB) nextRopeID() domain.RopeID {
	return domain.RopeID(b.incr(kCRope))
}
func (b *BadgerRopeDB) incr(key string) uint64 {
	var next uint64
	_ = b.db.Update(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte(key))
		if err == nil {
			_ = itm.Value(func(val []byte) error {
				next = binary.BigEndian.Uint64(val)
				return nil
			})
		}
		next++
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], next)
		return txn.Set([]byte(key), buf[:])
	})
	return next
}

// v1<->v2, Trait 기준으로 링크 업서트하여 공통 TraitID 보장
func (b *BadgerRopeDB) ensureLink(v1, v2 *domain.Vertex, t domain.TraitCode) (domain.TraitID, bool) {
	find := func(v *domain.Vertex, p shareddomain.Address, t domain.TraitCode) (int, bool) {
		for i, l := range v.Traits {
			if l.Partner == p && l.Trait == t {
				return i, true
			}
		}
		return -1, false
	}
	i1, ok1 := find(v1, v2.Address, t)
	i2, ok2 := find(v2, v1.Address, t)

	switch {
	case ok1 && ok2:
		return v1.Traits[i1].TraitID, false
	case ok1 && !ok2:
		id := v1.Traits[i1].TraitID
		v2.Traits = append(v2.Traits, domain.TraitRef{Partner: v1.Address, Trait: t, TraitID: id})
		return id, false
	case !ok1 && ok2:
		id := v2.Traits[i2].TraitID
		v1.Traits = append(v1.Traits, domain.TraitRef{Partner: v2.Address, Trait: t, TraitID: id})
		return id, false
	default:
		id := b.nextTraitID()
		v1.Traits = append(v1.Traits, domain.TraitRef{Partner: v2.Address, Trait: t, TraitID: id})
		v2.Traits = append(v2.Traits, domain.TraitRef{Partner: v1.Address, Trait: t, TraitID: id})
		return id, true
	}
}

// 유틸: dedup + 정렬(순서 안정성)
func dedupAppend(base, inc []shareddomain.Address) []shareddomain.Address {
	if len(inc) == 0 {
		return base
	}
	m := map[shareddomain.Address]struct{}{}
	out := make([]shareddomain.Address, 0, len(base)+len(inc))
	for _, x := range base {
		m[x] = struct{}{}
		out = append(out, x)
	}
	for _, y := range inc {
		if _, ok := m[y]; ok {
			continue
		}
		m[y] = struct{}{}
		out = append(out, y)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare([]byte(out[i].String()), []byte(out[j].String())) < 0 })
	return out
}

func sizeOf(rm domain.RopeMark) int { return int(rm.Size) }

// GetGraphStas returns basic graph statistics:
// - nodes:  total number of vertices (keys with prefix "v:")
// - ropes:  total number of rope marks (keys with prefix "r:")
// - traits: total number of trait marks (keys with prefix "t:")
func (b *BadgerRopeDB) GetGraphStats() map[string]any {
	var nodes, ropes, traits uint64

	_ = b.db.View(func(txn *badger.Txn) error {
		// 공통 카운터 헬퍼
		countPrefix := func(pfx []byte) uint64 {
			var c uint64
			itOpts := badger.DefaultIteratorOptions
			itOpts.PrefetchValues = false // 키 개수만 필요
			it := txn.NewIterator(itOpts)
			defer it.Close()

			for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
				c++
			}
			return c
		}

		nodes = countPrefix([]byte(kV))  // "v:"
		ropes = countPrefix([]byte(kR))  // "r:"
		traits = countPrefix([]byte(kT)) // "t:"
		return nil
	})

	return map[string]any{
		"nodes":  nodes,
		"ropes":  ropes,
		"traits": traits,
	}
}

// ViewAllTraitMarkByCode returns all TraitMark records with the given trait code
func (b *BadgerRopeDB) ViewAllTraitMarkByCode(t domain.TraitCode) ([]domain.TraitMark, error) {
	var result []domain.TraitMark
	err := b.db.View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		it := txn.NewIterator(itOpts)
		defer it.Close()

		pfx := []byte(kT) // "t:" prefix for trait marks
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			item := it.Item()
			var tm domain.TraitMark
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &tm)
			})
			if err != nil {
				continue // skip invalid entries
			}
			if tm.Trait == t {
				result = append(result, tm)
			}
		}
		return nil
	})
	return result, err
}

// ViewAllTraitMarkByString returns all TraitMark records with the trait name matching the given string
func (b *BadgerRopeDB) ViewAllTraitMarkByString(s string) ([]domain.TraitMark, error) {
	// Find trait code by string using legend
	var targetCode domain.TraitCode
	found := false
	for code, name := range b.traitLegend {
		if name == s {
			targetCode = code
			found = true
			break
		}
	}

	if !found {
		return []domain.TraitMark{}, nil // return empty slice if string not found in legend
	}

	return b.ViewAllTraitMarkByCode(targetCode)
}

// TODO 테스트 용으로 퍼블릭하게 만들다보니 의도치 않게 전환해버림... 추후 리팩토링 할 것
func (b *BadgerRopeDB) GetOrCreateVertex(a shareddomain.Address) *domain.Vertex {
	return b.getOrCreateVertex(a)
}

// TODO 테스트 용으로 퍼블릭하게 만들다보니 의도치 않게 전환해버림... 추후 리팩토링 할 것
func (b *BadgerRopeDB) RopeIDByTrait(v *domain.Vertex, t domain.TraitCode) (domain.RopeID, bool) {
	return b.ropeIDByTrait(v, t)
}

// TODO 테스트 용으로 퍼블릭하게 만들다보니 의도치 않게 전환해버림... 추후 리팩토링 할 것

func (b *BadgerRopeDB) GetRopeMark(rid domain.RopeID) domain.RopeMark {
	return b.getRopeMark(rid)
}
