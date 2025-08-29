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
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	chaintimer "github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
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
	kV         = "v:"       // v:<address>
	kR         = "r:"       // r:<ropeID>
	kT         = "t:"       // t:<traitID>
	kPR        = "pr:"      // pr:<polyRopeID> - PolyRopeMark 저장
	kCRope     = "ctr:rope" // BigEndian uint64
	kCTrait    = "ctr:trait"
	kCPolyRope = "ctr:polyrope" // BigEndian uint64 - PolyRope counter

	maxRopeSize = 1000 // 도메인 상한
)

func NewRopeDB(isTest mode.ProcessingMode, dbName string, traitLegend map[domain.TraitCode]string, ruleLegend map[domain.RuleCode]string, polyTraits map[domain.PolyTraitCode]domain.PolyNameAndTraits) (RopeDB, error) {
	var root string
	if isTest.IsTest() {
		root = computation.FindTestingStorageRootPath()
	} else {
		root = computation.FindProductionStorageRootPath()
	}
	return NewRopeDBWithRoot(isTest, root, dbName, traitLegend, ruleLegend, polyTraits)
}

type RopeDB interface {
	// 이벤트 입력(내부적으로 Vertex 동기 갱신 + Rope/Trait Upsert 큐잉)
	PushTraitEvent(ev domain.TraitEvent) error

	// 조회
	ViewRopeByNode(a shareddomain.Address) (*domain.Rope, error) // 첫 번째 Rope 기준(필요 시 Trait 선택 버전 추가)
	ViewRope(id domain.RopeID) (*domain.Rope, error)
	ViewInSameRope(a1, a2 shareddomain.Address) (bool, error)
	ViewInSameRopeByPolyTrait(a1, a2 shareddomain.Address, polyTrait domain.PolyTraitCode) (bool, error)
	GetGraphStats() map[string]any

	// 트레이트 기반 조회
	ViewAllTraitMarkByCode(t domain.TraitCode) ([]domain.TraitMark, error)
	ViewAllTraitMarkByString(s string) ([]domain.TraitMark, error)
	RawBadgerDB() *badger.DB

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

	// PolyTrait 매핑 (PolyTraitCode → 정의)
	polyTraits map[domain.PolyTraitCode]domain.PolyNameAndTraits
	// Trait → PolyTrait 역방향 매핑 (빠른 조회용)
	traitToPolyTrait map[domain.TraitCode][]domain.PolyTraitCode

	// 비동기 쓰기 버스
	busRope     *eventbus.EventBus[domain.RopeMarkUpsert]
	busTrait    *eventbus.EventBus[domain.TraitMarkUpsert]
	busPolyRope *eventbus.EventBus[PolyRopeMarkUpsert]

	// GC 관련
	gcTicker       *time.Ticker
	gcRunning      sync.Mutex
	publishCounter int64 // 발행 이벤트 카운터
}

// PolyRopeMark Upsert 커맨드
type PolyRopeMarkUpsert struct {
	PolyRopeID  domain.PolyRopeID
	PolyTrait   domain.PolyTraitCode
	AddRopes    []domain.RopeID // 추가할 Rope들
	VolumeDelta uint32
	LastSeen    chaintimer.ChainTime
	MergeFrom   []domain.PolyRopeID // 병합할 다른 PolyRope들
}

// NewRopeDBWithRoot: 모드(true=test, false=prod)로 루트 분기
func NewRopeDBWithRoot(isTest mode.ProcessingMode, root string, dbname string, traitLegend map[domain.TraitCode]string, ruleLegend map[domain.RuleCode]string, polyTraits map[domain.PolyTraitCode]domain.PolyNameAndTraits) (RopeDB, error) {
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
	evPolyRopeRel := filepath.Join("rope_db", dbname, "eventbus", "polyropemark.jsonl")

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
	busPR, err := eventbus.NewWithRoot[PolyRopeMarkUpsert](func() string { return root }, evPolyRopeRel, 4096)
	if err != nil {
		busR.Close()
		busT.Close()
		_ = db.Close()
		return nil, err
	}

	// Trait → PolyTrait 역방향 매핑 구축
	traitToPolyTrait := make(map[domain.TraitCode][]domain.PolyTraitCode)
	for polyCode, polyDef := range polyTraits {
		for _, traitCode := range polyDef.Traits {
			traitToPolyTrait[traitCode] = append(traitToPolyTrait[traitCode], polyCode)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &BadgerRopeDB{
		db: db, isTest: isTest.IsTest(),
		ctx: ctx, cancel: cancel,
		traitLegend:      traitLegend,
		ruleLegend:       ruleLegend,
		polyTraits:       polyTraits,
		traitToPolyTrait: traitToPolyTrait,
		busRope:          busR,
		busTrait:         busT,
		busPolyRope:      busPR,
	}

	// GC 스케쥴러 설정
	b.startGC()

	// 워커 가동
	b.wg.Add(4) // GC 워커 포함 4개
	go b.traitWorker()
	go b.ropeWorker()
	go b.polyRopeWorker()
	go b.gcWorker()
	return b, nil
}
func (b *BadgerRopeDB) RawBadgerDB() *badger.DB {
	return b.db
}

func (b *BadgerRopeDB) Close() error {
	b.cancel()
	// GC 타이머 정지
	if b.gcTicker != nil {
		b.gcTicker.Stop()
	}
	// 이벤트버스: pending 저장 & 채널 close
	b.busTrait.Close()
	b.busRope.Close()
	b.busPolyRope.Close()
	b.wg.Wait()
	return b.db.Close()
}

// * 결국 로프 DB 튜닝의 핵심
// * PushTraitEvent가 빠르게 작동하면 됨
// *근데, Vertex 읽기 쓰기 외엔 전부 비동기로 동작 중임
func (b *BadgerRopeDB) PushTraitEvent(ev domain.TraitEvent) error {
	debugEnabled := true
	if b.publishCounter%10 == 0 && debugEnabled {
		fmt.Printf(`
		========ev정보=========
		Trait: %v,
		AddressA: %v,
		RuleA: %v,
		AddressB: %v,
		RuleB: %v, 
		`, b.traitLegend[ev.Trait], ev.AddressA.String(),
			b.ruleLegend[ev.RuleA], ev.AddressB.String(), b.ruleLegend[ev.RuleB])
	}
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
	tid, created := b.ensureLink(v1, v2, ev.Trait, ev.RuleA, ev.RuleB)

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

	// --- 6) PolyTrait 처리 (해당 Trait가 PolyTrait에 속하는 경우)
	if polyTraitCodes, exists := b.traitToPolyTrait[ev.Trait]; exists {
		for _, polyTraitCode := range polyTraitCodes {
			b.processPolyTrait(v1, v2, polyTraitCode, ev)
		}
	}

	// --- 7) publish 카운터 증가
	atomic.AddInt64(&b.publishCounter, 1)

	_ = created // created 여부는 외부로 굳이 노출하지 않음
	return nil
}

// processPolyTrait: PolyTrait 레벨에서 PolyRope 처리
func (b *BadgerRopeDB) processPolyTrait(v1, v2 *domain.Vertex, polyTraitCode domain.PolyTraitCode, ev domain.TraitEvent) {
	// 각 Vertex의 PolyRopeRef 찾기
	pr1, prExist1 := b.getPolyRopeFromVertex(v1, polyTraitCode)
	pr2, prExist2 := b.getPolyRopeFromVertex(v2, polyTraitCode)

	// 현재 이벤트의 Trait로 생성된 RopeID 찾기
	currentRopeID, ropeExists := b.ropeIDByTrait(v1, ev.Trait)
	if !ropeExists {
		currentRopeID, ropeExists = b.ropeIDByTrait(v2, ev.Trait)
		if !ropeExists {
			return // Rope가 없으면 PolyRope 처리 불가
		}
	}

	switch {
	case !prExist1 && !prExist2:
		// 둘 다 PolyRope 없음 - 새 PolyRope 생성
		newPolyID := b.nextPolyRopeID()

		// 두 Vertex에 PolyRopeRef 추가
		b.setPolyRopeInVertex(v1, newPolyID, polyTraitCode)
		b.setPolyRopeInVertex(v2, newPolyID, polyTraitCode)

		// PolyRopeMark 생성 (현재 Trait로 생성된 Rope만 포함)
		_ = b.busPolyRope.Publish(PolyRopeMarkUpsert{
			PolyRopeID:  newPolyID,
			PolyTrait:   polyTraitCode,
			AddRopes:    []domain.RopeID{currentRopeID},
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})

	case prExist1 && !prExist2:
		// v1만 PolyRope 있음 - v2를 v1의 PolyRope에 편입
		b.setPolyRopeInVertex(v2, pr1, polyTraitCode)

		// 현재 Rope를 PolyRope에 추가 (중복 제거는 dedupRopes에서 처리)
		_ = b.busPolyRope.Publish(PolyRopeMarkUpsert{
			PolyRopeID:  pr1,
			PolyTrait:   polyTraitCode,
			AddRopes:    []domain.RopeID{currentRopeID},
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})

	case !prExist1 && prExist2:
		// v2만 PolyRope 있음 - v1을 v2의 PolyRope에 편입
		b.setPolyRopeInVertex(v1, pr2, polyTraitCode)

		// 현재 Rope를 PolyRope에 추가 (중복 제거는 dedupRopes에서 처리)
		_ = b.busPolyRope.Publish(PolyRopeMarkUpsert{
			PolyRopeID:  pr2,
			PolyTrait:   polyTraitCode,
			AddRopes:    []domain.RopeID{currentRopeID},
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})

	default:
		// 둘 다 PolyRope 있음
		if pr1 == pr2 {
			// 같은 PolyRope면 현재 Rope만 추가 (중복 제거는 dedupRopes에서 처리)
			_ = b.busPolyRope.Publish(PolyRopeMarkUpsert{
				PolyRopeID:  pr1,
				PolyTrait:   polyTraitCode,
				AddRopes:    []domain.RopeID{currentRopeID},
				VolumeDelta: 1,
				LastSeen:    ev.Time,
			})
		} else {
			// 다른 PolyRope면 병합 - PolyRopeMark의 실제 멤버 수로 크기 비교
			prm1 := b.getPolyRopeMark(pr1)
			prm2 := b.getPolyRopeMark(pr2)

			// 각 PolyRope의 실제 멤버 수 계산
			size1 := b.calculatePolyRopeSize(prm1.Ropes)
			size2 := b.calculatePolyRopeSize(prm2.Ropes)

			target := pr1
			source := pr2

			// 실제 멤버 수로 크기 비교
			if size2 > size1 {
				target, source = pr2, pr1
			}

			// source PolyRope의 모든 Vertex들의 PolyRopeRef를 target으로 변경
			b.updateAllVerticesPolyRope(source, target, polyTraitCode)

			// 합병된 PolyRopeMark 업서트 (source Ropes + 현재 Rope, 중복 제거)
			srcMark := b.getPolyRopeMark(source)
			addingRopes := append(srcMark.Ropes, currentRopeID)

			_ = b.busPolyRope.Publish(PolyRopeMarkUpsert{
				PolyRopeID:  target,
				PolyTrait:   polyTraitCode,
				AddRopes:    addingRopes, // 중복 제거는 dedupRopes에서 처리
				VolumeDelta: 1,
				LastSeen:    ev.Time,
				MergeFrom:   []domain.PolyRopeID{source},
			})
		}
	}

	// Vertex 동기 저장
	b.putVertex(v1)
	b.putVertex(v2)
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

func (b *BadgerRopeDB) ViewInSameRopeByPolyTrait(a1, a2 shareddomain.Address, polyTrait domain.PolyTraitCode) (bool, error) {
	v1 := b.getOrCreateVertex(a1)
	v2 := b.getOrCreateVertex(a2)
	return inSamePolyRopeByPolyTrait(v1, v2, polyTrait), nil
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

func (b *BadgerRopeDB) polyRopeWorker() {
	defer b.wg.Done()
	for {
		select {
		case op, ok := <-b.busPolyRope.Dequeue():
			if !ok {
				return
			}
			b.applyPolyRopeUpsert(op)
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

func (b *BadgerRopeDB) applyPolyRopeUpsert(op PolyRopeMarkUpsert) {
	key := []byte(fmt.Sprintf("%s%d", kPR, uint64(op.PolyRopeID)))
	_ = b.db.Update(func(txn *badger.Txn) error {
		var prm domain.PolyRopeMark
		if itm, err := txn.Get(key); err == nil {
			_ = itm.Value(func(val []byte) error { return json.Unmarshal(val, &prm) })
		}
		if prm.ID == 0 {
			prm = domain.PolyRopeMark{
				ID:        op.PolyRopeID,
				PolyTrait: op.PolyTrait,
				Ropes:     nil,
			}
		}

		// Rope 추가 + dedup
		if len(op.AddRopes) > 0 {
			prm.Ropes = dedupRopes(prm.Ropes, op.AddRopes)
		}

		// 병합 처리
		for _, sid := range op.MergeFrom {
			if sid == prm.ID {
				continue
			}
			skey := []byte(fmt.Sprintf("%s%d", kPR, uint64(sid)))
			_ = txn.Delete(skey)
		}

		data, _ := json.Marshal(prm)
		return txn.Set(key, data)
	})
}

// dedupRopes: RopeID 중복 제거
func dedupRopes(base, inc []domain.RopeID) []domain.RopeID {
	if len(inc) == 0 {
		return base
	}
	m := map[domain.RopeID]struct{}{}
	out := make([]domain.RopeID, 0, len(base)+len(inc))
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
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
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

func inSamePolyRopeByPolyTrait(v1, v2 *domain.Vertex, polyTrait domain.PolyTraitCode) bool {
	// v1에서 해당 PolyTrait의 PolyRopeID 찾기
	var polyRopeID1 domain.PolyRopeID
	found1 := false
	for _, pr := range v1.PolyRopes {
		if pr.PolyTrait == polyTrait {
			polyRopeID1 = pr.Id
			found1 = true
			break
		}
	}

	// v2에서 해당 PolyTrait의 PolyRopeID 찾기
	var polyRopeID2 domain.PolyRopeID
	found2 := false
	for _, pr := range v2.PolyRopes {
		if pr.PolyTrait == polyTrait {
			polyRopeID2 = pr.Id
			found2 = true
			break
		}
	}

	// 둘 다 PolyRope가 있고, 같은 ID면 true
	return found1 && found2 && polyRopeID1 == polyRopeID2
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
func (b *BadgerRopeDB) nextPolyRopeID() domain.PolyRopeID {
	return domain.PolyRopeID(b.incr(kCPolyRope))
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
func (b *BadgerRopeDB) ensureLink(v1, v2 *domain.Vertex, t domain.TraitCode, ruleA, ruleB domain.RuleCode) (domain.TraitID, bool) {
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

	if ok1 && ok2 {
		// 이미 양쪽에 링크가 있으면 룰 정보 업데이트
		v1.Traits[i1].MyRule = ruleA
		v1.Traits[i1].PartnerRule = ruleB
		v2.Traits[i2].MyRule = ruleB
		v2.Traits[i2].PartnerRule = ruleA
		return v1.Traits[i1].TraitID, false
	} else {
		id := b.nextTraitID()
		// 양쪽 모두 새로 생성 (룰 정보 포함)
		v1.Traits = append(v1.Traits, domain.TraitRef{
			TraitID:     id,
			Trait:       t,
			Partner:     v2.Address,
			MyRule:      ruleA,
			PartnerRule: ruleB,
		})
		v2.Traits = append(v2.Traits, domain.TraitRef{
			TraitID:     id,
			Trait:       t,
			Partner:     v1.Address,
			MyRule:      ruleB,
			PartnerRule: ruleA,
		})
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

// getPolyRopeMark: PolyRopeMark 조회
func (b *BadgerRopeDB) getPolyRopeMark(id domain.PolyRopeID) domain.PolyRopeMark {
	key := []byte(fmt.Sprintf("%s%d", kPR, uint64(id)))
	var prm domain.PolyRopeMark
	_ = b.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error { return json.Unmarshal(val, &prm) })
	})
	return prm
}

// getPolyRopeFromVertex: Vertex에서 특정 PolyTrait의 PolyRope ID 가져오기
func (b *BadgerRopeDB) getPolyRopeFromVertex(v *domain.Vertex, polyTrait domain.PolyTraitCode) (domain.PolyRopeID, bool) {
	for _, pr := range v.PolyRopes {
		if pr.PolyTrait == polyTrait {
			return pr.Id, true
		}
	}
	return 0, false
}

// setPolyRopeInVertex: Vertex에 PolyRope 설정
func (b *BadgerRopeDB) setPolyRopeInVertex(v *domain.Vertex, polyRopeID domain.PolyRopeID, polyTrait domain.PolyTraitCode) {
	for i, pr := range v.PolyRopes {
		if pr.PolyTrait == polyTrait {
			v.PolyRopes[i].Id = polyRopeID
			return
		}
	}
	// 없으면 새로 추가
	v.PolyRopes = append(v.PolyRopes, domain.PolyRopeRef{
		Id:        polyRopeID,
		PolyTrait: polyTrait,
	})
}

// calculatePolyRopeSize: PolyRope의 실제 멤버 수 계산
func (b *BadgerRopeDB) calculatePolyRopeSize(ropeIDs []domain.RopeID) int {
	totalSize := 0
	for _, ropeID := range ropeIDs {
		ropeMark := b.getRopeMark(ropeID)
		totalSize += int(ropeMark.Size)
	}
	return totalSize
}

// updateAllVerticesPolyRope: source PolyRope에 속한 모든 Vertex를 target PolyRope로 업데이트
func (b *BadgerRopeDB) updateAllVerticesPolyRope(sourcePolyRopeID, targetPolyRopeID domain.PolyRopeID, polyTrait domain.PolyTraitCode) {
	// source PolyRope의 모든 Rope 가져오기
	srcMark := b.getPolyRopeMark(sourcePolyRopeID)

	// 각 Rope의 모든 멤버 Vertex 업데이트
	for _, ropeID := range srcMark.Ropes {
		ropeMark := b.getRopeMark(ropeID)
		for _, addr := range ropeMark.Members {
			v := b.getOrCreateVertex(addr)
			b.setPolyRopeInVertex(v, targetPolyRopeID, polyTrait)
			b.putVertex(v)
		}
	}
}

// GetGraphStas returns basic graph statistics:
// - nodes:  total number of vertices (keys with prefix "v:")
// - ropes:  total number of rope marks (keys with prefix "r:")
// - traits: total number of trait marks (keys with prefix "t:")
// - polyRopes: total number of poly rope marks (keys with prefix "pr:")
func (b *BadgerRopeDB) GetGraphStats() map[string]any {
	var nodes, ropes, traits, polyRopes uint64

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

		nodes = countPrefix([]byte(kV))      // "v:"
		ropes = countPrefix([]byte(kR))      // "r:"
		traits = countPrefix([]byte(kT))     // "t:"
		polyRopes = countPrefix([]byte(kPR)) // "pr:"
		return nil
	})

	return map[string]any{
		"nodes":     nodes,
		"ropes":     ropes,
		"traits":    traits,
		"polyRopes": polyRopes,
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

func (b *BadgerRopeDB) GetPolyTraitLegend() map[domain.PolyTraitCode]domain.PolyNameAndTraits {
	return b.polyTraits
}

// ------------------------------------------------------------
// 9) GC (Garbage Collection) 관련 로직
// ------------------------------------------------------------

const (
	gcInterval       = 10 * time.Minute // GC 체크 주기 (짧게 조정)
	gcThreshold      = 0.4              // GC 임계값 (0.4 = 40%)
	gcPublishTrigger = 50_000           // publish 10000번마다 GC 트리거
	//gcBatchSize      = 1000             // 배치 단위로 정리
	//gcLowVolumeLimit = 5                // 낮은 볼륨 임계값
	//gcLowScoreLimit  = 1.0              // 낮은 스코어 임계값
)

// startGC: GC 스케쥴러 시작
func (b *BadgerRopeDB) startGC() {
	b.gcTicker = time.NewTicker(gcInterval)
}

// gcWorker: GC 워커 - 주기적으로 GC 조건 체크 및 실행
func (b *BadgerRopeDB) gcWorker() {
	defer b.wg.Done()
	for {
		select {
		case <-b.gcTicker.C:
			if b.shouldRunGC() {
				b.runGC()
			}
		case <-b.ctx.Done():
			return
		}
	}
}

// shouldRunGC: GC 실행 조건 체크
func (b *BadgerRopeDB) shouldRunGC() bool {
	// 이미 GC가 실행 중이면 건너뛰기
	if !b.gcRunning.TryLock() {
		return false
	}
	defer b.gcRunning.Unlock()

	// publish 카운터 체크 (주요 트리거)
	publishCount := atomic.LoadInt64(&b.publishCounter)
	if publishCount >= gcPublishTrigger {
		// 카운터 리셋
		atomic.StoreInt64(&b.publishCounter, 0)
		return true
	}

	// Badger DB 상태 체크 (보조 트리거)
	lsm, vlog := b.db.Size()
	totalSize := lsm + vlog

	// 간단한 크기 기반 트리거 (100MB 초과시)
	if totalSize > 100*1024*1024 {
		return true
	}

	return false
}

// runGC: 실제 GC 실행
func (b *BadgerRopeDB) runGC() {
	b.gcRunning.Lock()
	defer b.gcRunning.Unlock()

	startTime := time.Now()

	// // 1. 오래된 Trait 정리
	//TODO 이상적인 GC모델에선 아래 주석처럼 논리적인 GC도 하겠지만, 우선은 그냥 badger thombstone기반 GC만 실행함
	// cleanedTraits := b.cleanOldTraits()

	// // 2. 빈 Rope 정리
	// cleanedRopes := b.cleanEmptyRopes()

	// // 3. 고아 Vertex 정리
	// cleanedVertices := b.cleanOrphanVertices()

	// 4. Badger 내장 GC 실행
	b.runBadgerGC()

	duration := time.Since(startTime)

	// GC 통계 로깅 (프로덕션에서는 적절한 로거 사용)
	if !b.isTest {
		fmt.Printf("GC completed in %v\n",
			duration)
	}
}

// // cleanOldTraits: 낮은 볼륨과 스코어의 Trait 정리
// func (b *BadgerRopeDB) cleanOldTraits() int {
// 	var cleaned int

// 	_ = b.db.Update(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		prefix := []byte(kT) // "t:" prefix for traits
// 		var keysToDelete [][]byte

// 		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keysToDelete) < gcBatchSize; it.Next() {
// 			item := it.Item()
// 			var tm domain.TraitMark

// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &tm)
// 			})

// 			if err != nil {
// 				continue
// 			}

// 			// 조건: 볼륨이 낮고 스코어가 낮은 경우만 정리
// 			if tm.Volume < gcLowVolumeLimit && tm.Score < gcLowScoreLimit {
// 				key := make([]byte, len(item.Key()))
// 				copy(key, item.Key())
// 				keysToDelete = append(keysToDelete, key)
// 			}
// 		}

// 		// 배치 삭제
// 		for _, key := range keysToDelete {
// 			_ = txn.Delete(key)
// 			cleaned++
// 		}

// 		return nil
// 	})

// 	return cleaned
// }

// // cleanEmptyRopes: 멤버가 없거나 낮은 볼륨의 Rope 정리
// func (b *BadgerRopeDB) cleanEmptyRopes() int {
// 	var cleaned int

// 	_ = b.db.Update(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		prefix := []byte(kR) // "r:" prefix for ropes
// 		var keysToDelete [][]byte

// 		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keysToDelete) < gcBatchSize; it.Next() {
// 			item := it.Item()
// 			var rm domain.RopeMark

// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &rm)
// 			})

// 			if err != nil {
// 				continue
// 			}

// 			// 조건: 멤버가 없거나, 매우 낮은 볼륨
// 			if len(rm.Members) == 0 || rm.Volume < 3 {
// 				key := make([]byte, len(item.Key()))
// 				copy(key, item.Key())
// 				keysToDelete = append(keysToDelete, key)
// 			}
// 		}

// 		// 배치 삭제
// 		for _, key := range keysToDelete {
// 			_ = txn.Delete(key)
// 			cleaned++
// 		}

// 		return nil
// 	})

// 	return cleaned
// }

// // cleanOrphanVertices: 고아 Vertex 정리 (Rope나 Trait 참조가 없는 경우)
// func (b *BadgerRopeDB) cleanOrphanVertices() int {
// 	var cleaned int

// 	_ = b.db.Update(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		it := txn.NewIterator(opts)
// 		defer it.Close()

// 		prefix := []byte(kV) // "v:" prefix for vertices
// 		var keysToDelete [][]byte

// 		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keysToDelete) < gcBatchSize; it.Next() {
// 			item := it.Item()
// 			var v domain.Vertex

// 			err := item.Value(func(val []byte) error {
// 				return json.Unmarshal(val, &v)
// 			})

// 			if err != nil {
// 				continue
// 			}

// 			// 조건: Rope나 Trait 참조가 전혀 없는 고아 Vertex
// 			if len(v.Ropes) == 0 && len(v.Traits) == 0 {
// 				key := make([]byte, len(item.Key()))
// 				copy(key, item.Key())
// 				keysToDelete = append(keysToDelete, key)
// 			}
// 		}

// 		// 배치 삭제
// 		for _, key := range keysToDelete {
// 			_ = txn.Delete(key)
// 			cleaned++
// 		}

// 		return nil
// 	})

// 	return cleaned
// }

// runBadgerGC: Badger 내장 GC 실행
func (b *BadgerRopeDB) runBadgerGC() {
	// Badger의 내장 GC 실행 (반환값은 GC가 실제로 실행되었는지 여부)
	for {
		err := b.db.RunValueLogGC(0.4) // 40% 임계값으로 value log GC
		if err != nil {
			break
		}
	}
}
