package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
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
	"github.com/shopspring/decimal"
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

func NewRopeDB(isTest mode.ProcessingMode, dbName string, traitLegend map[domain.TraitCode]string,
	ruleLegend map[domain.RuleCode]string,
	polyTraits map[domain.PolyTraitCode]domain.PolyNameAndTraits,
	trapLegend map[domain.TrapCode]domain.TrapNameAndTraits,
	authorLegned map[domain.AuthorCode]string) (RopeDB, error) {
	var root string
	if isTest.IsTest() {
		root = computation.FindTestingStorageRootPath()
	} else {
		root = computation.FindProductionStorageRootPath()
	}
	return NewRopeDBWithRoot(isTest, root, dbName, traitLegend, ruleLegend, polyTraits, trapLegend, authorLegned)
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
	//RawBadgerDB() *badger.DB

	Close() error
}

// ------------------------------------------------------------
// 5) 구현체
// ------------------------------------------------------------

// ==== PATCH BEGIN: struct fields (replace your BadgerRopeDB definition) ====

type BadgerRopeDB struct {
	// NOTE: 호환을 위해 남겨둠. 이제 b.db 는 "마크 DB(M-DB)"를 가리킴.
	dbV *badger.DB // Vertex 전용 (v:)
	dbM *badger.DB // Marks 전용 (r:, t:, pr:)

	// ▼ 추가: V-DB 보호용 RWMutex
	muV sync.RWMutex

	isTest bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 범례 (코드 → 문자열 매핑)
	traitLegend  map[domain.TraitCode]string
	ruleLegend   map[domain.RuleCode]string
	authorLegend map[domain.AuthorCode]string

	// PolyTrait 매핑 (PolyTraitCode → 정의)
	polyTraits map[domain.PolyTraitCode]domain.PolyNameAndTraits
	// Trait → PolyTrait 역방향 매핑 (빠른 조회용)
	traitToPolyTraits map[domain.TraitCode][]domain.PolyTraitCode

	// Trap-> Traits의 매핑
	trapToTraits            map[domain.TrapCode][]domain.TraitCode
	trapToRequiredRuleOrNot map[domain.TrapCode]domain.RequiredRuleOrZero

	// 비동기 쓰기 버스
	busRope     *eventbus.EventBus[domain.RopeMarkUpsert]
	busTrait    *eventbus.EventBus[domain.TraitMarkUpsert]
	busPolyRope *eventbus.EventBus[PolyRopeMarkUpsert]

	// 카운터(Sequence)
	seqTrait    *badger.Sequence
	seqRope     *badger.Sequence
	seqPolyRope *badger.Sequence

	// GC 관련
	gcTicker       *time.Ticker
	gcRunning      sync.Mutex
	publishCounter int64 // 발행 이벤트 카운터
}

// ==== PATCH END ====

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
// ==== PATCH BEGIN: NewRopeDBWithRoot (replace entire function) ====

func NewRopeDBWithRoot(isTest mode.ProcessingMode, root string, dbname string, traitLegend map[domain.TraitCode]string,
	ruleLegend map[domain.RuleCode]string,
	polyTraits map[domain.PolyTraitCode]domain.PolyNameAndTraits,
	trapLegend map[domain.TrapCode]domain.TrapNameAndTraits,
	authorLegend map[domain.AuthorCode]string) (RopeDB, error) {

	// 2-way 분리: V-DB, M-DB
	vDir := filepath.Join(root, "rope_db", dbname, "badger_v")
	mDir := filepath.Join(root, "rope_db", dbname, "badger_m")

	vOpts := badger.DefaultOptions(vDir).WithLogger(nil).WithSyncWrites(false)
	mOpts := badger.DefaultOptions(mDir).WithLogger(nil).WithSyncWrites(false)

	dbV, err := badger.Open(vOpts)
	if err != nil {
		return nil, err
	}
	dbM, err := badger.Open(mOpts)
	if err != nil {
		_ = dbV.Close()
		return nil, err
	}

	// EventBus(JSONL 경로는 루트부터 분기)
	evRopeRel := filepath.Join("rope_db", dbname, "eventbus", "ropemark.jsonl")
	evTraitRel := filepath.Join("rope_db", dbname, "eventbus", "traitmark.jsonl")
	evPolyRopeRel := filepath.Join("rope_db", dbname, "eventbus", "polyropemark.jsonl")

	busR, err := eventbus.NewWithRoot[domain.RopeMarkUpsert](func() string { return root }, evRopeRel, 4096)
	if err != nil {
		_ = dbV.Close()
		_ = dbM.Close()
		return nil, err
	}
	busT, err := eventbus.NewWithRoot[domain.TraitMarkUpsert](func() string { return root }, evTraitRel, 4096)
	if err != nil {
		busR.Close()
		_ = dbV.Close()
		_ = dbM.Close()
		return nil, err
	}
	busPR, err := eventbus.NewWithRoot[PolyRopeMarkUpsert](func() string { return root }, evPolyRopeRel, 4096)
	if err != nil {
		busR.Close()
		busT.Close()
		_ = dbV.Close()
		_ = dbM.Close()
		return nil, err
	}

	// Trait → PolyTrait 역방향 매핑 구축
	traitToPolyTraits := make(map[domain.TraitCode][]domain.PolyTraitCode)
	for polyCode, polyDef := range polyTraits {
		for _, traitCode := range polyDef.Traits {
			traitToPolyTraits[traitCode] = append(traitToPolyTraits[traitCode], polyCode)
		}
	}
	trapToTraits := make(map[domain.TrapCode][]domain.TraitCode)
	trapToRequiredRules := make(map[domain.TrapCode]domain.RequiredRuleOrZero)
	for trapCode, trapNameAndTraits := range trapLegend {
		trapToTraits[trapCode] = trapNameAndTraits.Traits
		trapToRequiredRules[trapCode] = trapNameAndTraits.RuleExpectingOrZero
	}

	// Sequence (ctr:*) - 배치 크기는 소폭(4096)
	seqTrait, err := dbM.GetSequence([]byte(kCTrait), 4096)
	if err != nil {
		busR.Close()
		busT.Close()
		busPR.Close()
		_ = dbV.Close()
		_ = dbM.Close()
		return nil, err
	}
	seqRope, err := dbM.GetSequence([]byte(kCRope), 4096)
	if err != nil {
		_ = seqTrait.Release()
		busR.Close()
		busT.Close()
		busPR.Close()
		_ = dbV.Close()
		_ = dbM.Close()
		return nil, err
	}
	seqPoly, err := dbM.GetSequence([]byte(kCPolyRope), 4096)
	if err != nil {
		_ = seqTrait.Release()
		_ = seqRope.Release()
		busR.Close()
		busT.Close()
		busPR.Close()
		_ = dbV.Close()
		_ = dbM.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &BadgerRopeDB{
		// 호환성: b.db 는 마크 DB를 바라보게 둠
		dbV:                     dbV,
		dbM:                     dbM,
		isTest:                  isTest.IsTest(),
		ctx:                     ctx,
		cancel:                  cancel,
		traitLegend:             traitLegend,
		ruleLegend:              ruleLegend,
		authorLegend:            authorLegend,
		polyTraits:              polyTraits,
		traitToPolyTraits:       traitToPolyTraits,
		trapToRequiredRuleOrNot: trapToRequiredRules,
		trapToTraits:            trapToTraits,
		busRope:                 busR,
		busTrait:                busT,
		busPolyRope:             busPR,
		seqTrait:                seqTrait,
		seqRope:                 seqRope,
		seqPolyRope:             seqPoly,
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

// ==== PATCH END ====

// ==== PATCH BEGIN: Close (replace entire function) ====

func (b *BadgerRopeDB) Close() error {
	b.cancel()
	if b.gcTicker != nil {
		b.gcTicker.Stop()
	}
	b.busTrait.Close()
	b.busRope.Close()
	b.busPolyRope.Close()
	b.wg.Wait()

	// Sequence release
	if b.seqTrait != nil {
		_ = b.seqTrait.Release()
	}
	if b.seqRope != nil {
		_ = b.seqRope.Release()
	}
	if b.seqPolyRope != nil {
		_ = b.seqPolyRope.Release()
	}

	// Close DBs (marks 먼저, vertex 나중)
	var err1, err2 error
	if b.dbM != nil {
		err1 = b.dbM.Close()
	}
	if b.dbV != nil {
		err2 = b.dbV.Close()
	}

	if err1 != nil {
		return err1
	}
	return err2
}

// ==== PATCH END ====

// * 결국 로프 DB 튜닝의 핵심
// * PushTraitEvent가 빠르게 작동하면 됨
// *근데, Vertex 읽기 쓰기 외엔 전부 비동기로 동작 중임
func (b *BadgerRopeDB) PushTraitEvent(ev domain.TraitEvent) error {
	debugEnabled := false
	if b.publishCounter%1000 == 0 && debugEnabled {
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
	//순서에 맞추어 교환
	//AddressA가 먼저 오는 알파벳
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
	// ===== V-DB 전역 Lock 시작 =====
	//* vdb만 lock
	b.muV.Lock()
	defer b.muV.Unlock()
	// --- 1) Vertex 동기 로드/생성
	// --- 1) Vertex 동기 로드/생성 (NoLock)
	v1 := b.getOrCreateVertexNoLock(a1)
	v2 := b.getOrCreateVertexNoLock(a2)

	// --- 2) 링크 업서트 → TraitID 확보(둘 다 동일 ID 사용)
	tid, created := b.ensureLink(v1, v2, ev.Trait, ev.RuleA, ev.RuleB)

	// --- 3) TraitMark Upsert 큐잉(단일 커맨드)
	_ = b.busTrait.Publish(domain.TraitMarkUpsert{
		AuthorCode:  ev.Author,
		LastTx:      ev.TxID,
		TraitID:     tid,
		Trait:       ev.Trait,
		AddressA:    ev.AddressA,
		RuleA:       ev.RuleA,
		AddressB:    ev.AddressB,
		RuleB:       ev.RuleB,
		ScoreDelta:  ev.Score,
		VolumeDelta: 1,
		ValueOrNull: ev.ValueOrNull, // ✅ 새로 추가 (없으면 decimal.Zero 사용)
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
			ValueOrNull: ev.ValueOrNull,
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
			ValueOrNull: ev.ValueOrNull,

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
			ValueOrNull: ev.ValueOrNull,

			LastSeen: ev.Time,
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
				ValueOrNull: ev.ValueOrNull,
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
				v := b.getOrCreateVertexNoLock(addr)
				setRopeForTrait(v, target, ev.Trait)
				b.putVertexNoLock(v)

			}
			// target 로프마크에 흡수 Upsert, source는 이후 제거/무시
			_ = b.busRope.Publish(domain.RopeMarkUpsert{
				RopeID:      target,
				Trait:       ev.Trait,
				AddMembers:  src.Members,
				SizeDelta:   int32(len(src.Members)),
				ValueOrNull: ev.ValueOrNull.Add(src.ValueOrNull),

				VolumeDelta: uint32(src.Volume) + 1, //거래 볼륨이므로. 상대+자신+이번 거래
				LastSeen:    ev.Time,
				MergeFrom:   []domain.RopeID{source},
			})
		}
	}
	//트레이트 등록 & 로프까지 다 등록 이후=> 트랩 여부 체크
	b.HandleTrapDetection(v1)
	b.HandleTrapDetection(v2)

	// --- 6) PolyTrait 처리 (해당 Trait가 PolyTrait에 속하는 경우)
	if polyTraitCodes, exists := b.traitToPolyTraits[ev.Trait]; exists {
		for _, polyTraitCode := range polyTraitCodes {
			b.processPolyTrait(v1, v2, polyTraitCode, ev)
		}
	}
	// --- 5) Vertex 동기 저장
	b.putVertexNoLock(v1)
	b.putVertexNoLock(v2)

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
	_ = b.dbM.Update(func(txn *badger.Txn) error {
		var tm domain.TraitMark
		if itm, err := txn.Get(key); err == nil {
			_ = itm.Value(func(val []byte) error { return json.Unmarshal(val, &tm) })
		}
		if tm.ID == 0 {
			tm = domain.TraitMark{
				AuthorCodes: []domain.AuthorCode{op.AuthorCode},
				LastTx:      op.LastTx,
				ID:          op.TraitID,
				Trait:       op.Trait,
				AddressA:    op.AddressA,
				RuleA:       op.RuleA,
				AddressB:    op.AddressB,
				RuleB:       op.RuleB,
				Score:       0,
				Volume:      0,
				ValueOrNull: decimal.Zero, // ✅ 새로 추가: 기본 0으로
				LastSeen:    op.LastSeen,
			}
		}
		// 값 누적 (null이면 Zero가 와서 변화 없음)
		if !op.ValueOrNull.IsZero() {
			tm.ValueOrNull = tm.ValueOrNull.Add(op.ValueOrNull) // ✅ 누적
		}
		tm.Score += op.ScoreDelta
		tm.Volume += op.VolumeDelta
		if op.LastSeen.After(tm.LastSeen) {
			tm.LastTx = op.LastTx
			tm.LastSeen = op.LastSeen
		}
		tm.AuthorCodes = mergeAuthor(tm.AuthorCodes, op.AuthorCode)
		data, _ := json.Marshal(tm)
		return txn.Set(key, data)
	})
}

func mergeAuthor(authors []domain.AuthorCode, target domain.AuthorCode) []domain.AuthorCode {
	if slices.Contains(authors, target) {
		return authors
	}
	return append(authors, target)
}

func (b *BadgerRopeDB) applyRopeUpsert(op domain.RopeMarkUpsert) {
	key := []byte(fmt.Sprintf("%s%d", kR, uint64(op.RopeID)))
	_ = b.dbM.Update(func(txn *badger.Txn) error {
		var rm domain.RopeMark
		if itm, err := txn.Get(key); err == nil {
			_ = itm.Value(func(val []byte) error { return json.Unmarshal(val, &rm) })
		}
		if rm.ID == 0 {
			rm = domain.RopeMark{
				ID:          op.RopeID,
				Trait:       op.Trait,
				Size:        0,
				Members:     nil,
				Volume:      0,
				LastSeen:    op.LastSeen,
				ValueOrNull: decimal.Zero,
			}
		}
		if len(rm.Members) >= maxRopeSize {
			fmt.Printf("도메인 특수 감지: %d번 로프의 멤버가 %d개를 초과하려 함.\nDB로 1000개 이상의 증폭은 막음", rm.ID, maxRopeSize)
			return nil
		}
		// 값 누적 (null이면 Zero가 와서 변화 없음)
		if !op.ValueOrNull.IsZero() {
			rm.ValueOrNull = rm.ValueOrNull.Add(op.ValueOrNull) // ✅ 누적
		}
		if len(op.AddMembers) > 0 {
			rm.Members = dedupAppend(rm.Members, op.AddMembers)
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

		data, _ := json.Marshal(rm)
		return txn.Set(key, data)
	})
}

// 주소·트랩 "쌍" 기준으로 유니크하게 합치기 (첫 등장 순서 유지)
func MergeTrapVerticesUniquePairs(slices ...[]domain.TrapVertex) []domain.TrapVertex {
	type pair struct {
		Addr shareddomain.Address
		Trap domain.TrapCode
	}

	// 대략 용량 예측(선택): 전체 길이 합을 잡아주면 리할로 줄일 수 있음
	total := 0
	for _, s := range slices {
		total += len(s)
	}

	seen := make(map[pair]struct{}, total)
	out := make([]domain.TrapVertex, 0, total)

	for _, s := range slices {
		for _, v := range s {
			k := pair{Addr: v.Address, Trap: v.Trap}
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}

func (b *BadgerRopeDB) applyPolyRopeUpsert(op PolyRopeMarkUpsert) {
	key := []byte(fmt.Sprintf("%s%d", kPR, uint64(op.PolyRopeID)))
	_ = b.dbM.Update(func(txn *badger.Txn) error {
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
		if len(op.AddRopes) > 0 {
			prm.Ropes = dedupRopes(prm.Ropes, op.AddRopes)
		}
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

// ------------------------------------------------------------
// 8) VertexDB 동기 I/O & 유틸
// ------------------------------------------------------------

// ----- NoLock 버전: Push 경로에서 muV 이미 보유함 -----
func (b *BadgerRopeDB) getOrCreateVertexNoLock(a shareddomain.Address) *domain.Vertex {
	key := []byte(kV + a.String())
	var v domain.Vertex
	err := b.dbV.View(func(txn *badger.Txn) error {
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

func (b *BadgerRopeDB) putVertexNoLock(v *domain.Vertex) {
	key := []byte(kV + v.Address.String())
	_ = b.dbV.Update(func(txn *badger.Txn) error {
		data, _ := json.Marshal(v)
		return txn.Set(key, data)
	})
}

// ----- Safe 버전: 외부(조회 등)에서는 내부에서 락을 잡음 -----
func (b *BadgerRopeDB) getOrCreateVertex(a shareddomain.Address) *domain.Vertex {
	b.muV.RLock()
	defer b.muV.RUnlock()
	return b.getOrCreateVertexNoLock(a)
}

func (b *BadgerRopeDB) putVertex(v *domain.Vertex) {
	b.muV.Lock()
	defer b.muV.Unlock()
	b.putVertexNoLock(v)
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
	_ = b.dbM.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error { return json.Unmarshal(val, &rm) })
	})
	return rm
}

func (b *BadgerRopeDB) getPolyRopeMark(id domain.PolyRopeID) domain.PolyRopeMark {
	key := []byte(fmt.Sprintf("%s%d", kPR, uint64(id)))
	var prm domain.PolyRopeMark
	_ = b.dbM.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			return err
		}
		return itm.Value(func(val []byte) error { return json.Unmarshal(val, &prm) })
	})
	return prm
}

func (b *BadgerRopeDB) nextTraitID() domain.TraitID {
	n, _ := b.seqTrait.Next()
	return domain.TraitID(n)
}
func (b *BadgerRopeDB) nextRopeID() domain.RopeID {
	n, _ := b.seqRope.Next()
	return domain.RopeID(n)
}
func (b *BadgerRopeDB) nextPolyRopeID() domain.PolyRopeID {
	n, _ := b.seqPolyRope.Next()
	return domain.PolyRopeID(n)
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

// GetGraphStats: v:는 V-DB에서, r:/t:/pr:는 M-DB에서 집계
func (b *BadgerRopeDB) GetGraphStats() map[string]any {
	var nodes, ropes, traits, polyRopes uint64
	// v: 집계만 RLock (M-DB는 락 불필요)
	b.muV.RLock()
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
	b.muV.RUnlock()
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
			err := item.Value(func(val []byte) error { return json.Unmarshal(val, &tm) })
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
