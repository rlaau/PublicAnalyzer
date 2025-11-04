package app

import (
	"context"
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
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/meter/rsv"
	"github.com/shopspring/decimal"
)

func NewRopeDB(ctx context.Context, isTest mode.ProcessingMode, dbName string, traitLegend map[domain.TraitCode]string,
	ruleLegend map[domain.RuleCode]string,
	polyTraits map[domain.PolyTraitCode]domain.PolyNameAndTraits,
	trapLegend map[domain.TrapCode]domain.TrapNameAndTraits,
	authorLegned map[domain.AuthorCode]string) (RopeDB, error) {
	var root string
	if isTest.IsTest() {
		root = computation.DockerTestingStorage()
	} else {
		root = computation.DockerProductionStorage()
	}
	return NewRopeDBWithRoot(ctx, isTest, root, dbName, traitLegend, ruleLegend, polyTraits, trapLegend, authorLegned)
}

type RopeDB interface {
	//TODO 추후 반드시 삭제할 것!
	// TODO컨텍스트를 가지는 것도 모자라 바꿔 끼우는 함수라니!
	RegisterContext(parent context.Context)
	// 이벤트 입력(내부적으로 Vertex 동기 갱신 + Rope/Trait Upsert 큐잉)
	HandleTraitEvent(ev domain.TraitEvent) error
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
	dbV *badger.DB // Vertex 전용 (v:) - 락프리 (일관성 오류 허용)
	dbM *badger.DB // Marks 전용 (r:, t:, pr:)

	perfMeter *rsv.ReservoirMeter

	// 인메모리 버텍스 캐시
	vertexCache *VertexCache

	// 인메모리 로프 사이즈 캐시 (로프 ID -> 사이즈 매핑)
	ropeSizeCache *RopeSizeCache

	//배칭용 로프 어그리게이터 케시 (주석 처리)
	// ropeAggCache *RopeAggCache

	isTest bool
	//TODO 이거 지우기
	//TODO 진짜 미친거냐.
	//TODO 대체 한달 전엔 무슨 생각으로 컨텍스트를 필드에 박은거임
	//TODO 반드시 리팩토링 할 것!!
	//TODO ctx필드 지우고, 워커 기반 디스패칭 한 후에 ctx는 워커 스타트 시 전파만 하기!!!!
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

	// 로프 병합 계측
	ropeMergeMetrics *RopeMergeMetrics
}

// TODO 추후 반드시 지울 것!!!!!
// TODO 컨텍스트는 ropeDB.Start로만 받게 하고, 이후에도 내부 워커들이 인자로만 받게 하기!!!
func (b *BadgerRopeDB) RegisterContext(parent context.Context) {
	ctx, cancle := context.WithCancel(parent)
	b.ctx = ctx
	b.cancel = cancle
}

// ==== PATCH END ====

// RopeMergeMetrics는 로프 병합 작업의 계측 정보를 저장합니다
type RopeMergeMetrics struct {
	callCount      atomic.Int64 // 병합 블록 호출 횟수
	totalTime      atomic.Int64 // 총 소요 시간 (나노초)
	totalMergeSize atomic.Int64 // 병합된 총 로프 크기
	mergeCount     atomic.Int64 // 병합 횟수 (평균 계산용)
}

// NewRopeMergeMetrics는 새로운 계측 구조체를 생성합니다
func NewRopeMergeMetrics() *RopeMergeMetrics {
	metrics := &RopeMergeMetrics{}
	// 10초마다 통계 출력
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			calls := metrics.callCount.Load()
			totalTimeNs := metrics.totalTime.Load()
			totalSize := metrics.totalMergeSize.Load()
			merges := metrics.mergeCount.Load()

			if calls > 0 {
				avgTimeMs := float64(totalTimeNs) / float64(calls) / 1e6
				avgSize := float64(totalSize) / float64(merges)
				if merges == 0 {
					avgSize = 0
				}

				fmt.Printf("[RopeMerge Metrics] 호출횟수: %d | 총시간: %.2fms | 평균시간: %.2fms | 총병합크기: %d | 평균병합크기: %.2f\n",
					calls, float64(totalTimeNs)/1e6, avgTimeMs, totalSize, avgSize)
			}
		}
	}()
	return metrics
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
// ==== PATCH BEGIN: NewRopeDBWithRoot (replace entire function) ====

func NewRopeDBWithRoot(parent context.Context, isTest mode.ProcessingMode, root string, dbname string, traitLegend map[domain.TraitCode]string,
	ruleLegend map[domain.RuleCode]string,
	polyTraits map[domain.PolyTraitCode]domain.PolyNameAndTraits,
	trapLegend map[domain.TrapCode]domain.TrapNameAndTraits,
	authorLegend map[domain.AuthorCode]string) (RopeDB, error) {

	base := func(dir string) badger.Options {
		return badger.
			DefaultOptions(dir).
			WithLogger(nil).
			WithSyncWrites(false).
			WithNumVersionsToKeep(1) // 이전 버전 보관 안 함

	}
	// 2-way 분리: V-DB, M-DB
	vDir := filepath.Join(root, "rope_db", dbname, "badger_v")
	mDir := filepath.Join(root, "rope_db", dbname, "badger_m")

	// V-DB 메모리 최적화 - Arena 메모리 부족 문제 해결
	vOpts := base(vDir).
		WithBlockCacheSize(128 << 20). // 128MB 블록 캐시 - 대폭 축소
		WithIndexCacheSize(64 << 20).  // 64MB 인덱스 캐시 - 대폭 축소
		WithMemTableSize(16 << 20).    // 16MB 메모리 테이블 크기 - 2개 × 16MB = 32MB
		WithNumCompactors(2).          // 컴팩터 2개로 축소
		WithDetectConflicts(false)     // 충돌 감지 끔

	// M-DB는 기본 설정 유지 (마크 데이터는 상대적으로 적음)
	mOpts := base(mDir)
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
	//TODO추후 josn기반 ㄱㄱ
	// evRopeRel := filepath.Join("rope_db", dbname, "eventbus", "ropemark.jsonl")
	// evTraitRel := filepath.Join("rope_db", dbname, "eventbus", "traitmark.jsonl")
	// evPolyRopeRel := filepath.Join("rope_db", dbname, "eventbus", "polyropemark.jsonl")

	busR := eventbus.NewInMemory[domain.RopeMarkUpsert](40_000, "RopeMarkBus")
	// if err != nil {
	// 	_ = dbV.Close()
	// 	_ = dbM.Close()
	// 	return nil, err
	// }
	busT := eventbus.NewInMemory[domain.TraitMarkUpsert](40_000, "TraitMarkBus")
	// if err != nil {
	// 	busR.Close()
	// 	_ = dbV.Close()
	// 	_ = dbM.Close()
	// 	return nil, err
	// }
	busPR := eventbus.NewInMemory[PolyRopeMarkUpsert](40_000, "PolyRopeMarkBus")
	// if err != nil {
	// 	busR.Close()
	// 	busT.Close()
	// 	_ = dbV.Close()
	// 	_ = dbM.Close()
	// 	return nil, err
	// }

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
	ctx, cancle := context.WithCancel(parent)
	b := &BadgerRopeDB{
		// 호환성: b.db 는 마크 DB를 바라보게 둠
		dbV:                     dbV,
		dbM:                     dbM,
		isTest:                  isTest.IsTest(),
		perfMeter:               rsv.NewFunctionReservoirMeter(10_000),
		ctx:                     ctx,
		cancel:                  cancle,
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

	// 인메모리 버텍스 캐시 초기화 (5만개 캐시)
	b.vertexCache = NewVertexCache(b, 500000)
	//로프 어그리게이터 케시 초기화 (주석 처리)
	// b.ropeAggCache = NewRopeAggCache(b.getRopeMark)

	// 인메모리 로프 사이즈 캐시 초기화 (최대 1천만개)
	b.ropeSizeCache = NewRopeSizeCache(10_000_000, b)

	// GC 스케쥴러 설정
	b.startGC()

	// 워커 가동
	b.wg.Add(6) // GC 워커 포함 6개 (추가: statsLogger, vertexCacheFlush)
	go b.traitWorker()
	go b.ropeWorker()
	go b.polyRopeWorker()
	go b.gcWorker()
	go b.statsLoggerWorker()
	go b.vertexCacheFlushWorker()

	// 로프 병합 계측 초기화
	b.ropeMergeMetrics = NewRopeMergeMetrics()

	return b, nil
}

// ==== PATCH END ====

// ==== PATCH BEGIN: Close (replace entire function) ====

func (b *BadgerRopeDB) Close() error {
	fmt.Printf("\n🔄 RopeDB 종료 시작...\n")
	b.cancel()
	if b.gcTicker != nil {
		b.gcTicker.Stop()
	}

	// 캐시 모든 데이터를 DB에 플러시
	if b.vertexCache != nil {
		fmt.Printf("   - VertexCache 플러시 중...\n")
		b.vertexCache.FlushAll()
	}

	fmt.Printf("   - Bus 채널 닫는 중...\n")
	b.busTrait.Close()
	b.busRope.Close()
	b.busPolyRope.Close()

	fmt.Printf("   - 워커 종료 대기 중...\n")
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Printf("   - 모든 워커 종료 완료\n")
	case <-time.After(30 * time.Second):
		fmt.Printf("⚠️  워커 종료 타임아웃 (30초 초과) - 강제 진행\n")
	}

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
// * HandleTraitEvent가 빠르게 작동하면 됨
// *근데, Vertex 읽기 쓰기 외엔 전부 비동기로 동작 중임
func (b *BadgerRopeDB) HandleTraitEvent(ev domain.TraitEvent) error {
	debugEnabled := true
	if b.publishCounter%10_000 == 0 && debugEnabled {
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
	// ===== V-DB 락프리 시작 (일관성 오류 허용) =====
	// --- 1) Vertex 로드/생성 (락 없음)
	v1 := b.getOrCreateVertex(a1)
	v2 := b.getOrCreateVertex(a2)

	// --- 2) 링크 업서트 → TraitID 확보(둘 다 동일 ID 사용)
	tid, created := b.ensureLink(v1, v2, ev.Trait, ev.RuleA, ev.RuleB)

	// --- 3) TraitMark Upsert 큐잉(단일 커맨드)
	traitErr := b.busTrait.Enqueue(b.ctx, domain.TraitMarkUpsert{
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
	if traitErr != nil {
		fmt.Printf("트레이트 마크 삽입 중 에러 발생: %s\n", traitErr.Error())
	}

	// --- 4) Rope 결정/병합
	r1, isExsit1 := b.ropeIDByTrait(v1, ev.Trait)
	r2, isExsit2 := b.ropeIDByTrait(v2, ev.Trait)
	switch {
	case !isExsit1 && !isExsit2:
		// 새 로프
		newID := b.nextRopeID()
		setRopeForTrait(v1, newID, ev.Trait)
		setRopeForTrait(v2, newID, ev.Trait)
		ropeErr := b.busRope.Enqueue(b.ctx, domain.RopeMarkUpsert{
			RopeID:      newID,
			Trait:       ev.Trait,
			AddMembers:  []shareddomain.Address{a1, a2},
			SizeDelta:   2,
			VolumeDelta: 1,
			ValueOrNull: ev.ValueOrNull,
			LastSeen:    ev.Time,
		})
		if ropeErr != nil {
			fmt.Printf("로프 마크 삽입 중 에러 발생: %s\n", ropeErr.Error())
		}
	case isExsit1 && !isExsit2:
		// v1 로프에 v2 편입
		id := r1
		setRopeForTrait(v2, id, ev.Trait)
		ropeErr := b.busRope.Enqueue(b.ctx, domain.RopeMarkUpsert{
			RopeID:      id,
			Trait:       ev.Trait,
			AddMembers:  []shareddomain.Address{a2},
			ValueOrNull: ev.ValueOrNull,

			SizeDelta:   1,
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})

		if ropeErr != nil {
			fmt.Printf("로프 마크 삽입 중 에러 발생: %s\n", ropeErr.Error())
		}
	case !isExsit1 && isExsit2:
		// v2 로프에 v1 편입
		id := r2
		setRopeForTrait(v1, id, ev.Trait)
		ropeErr := b.busRope.Enqueue(b.ctx, domain.RopeMarkUpsert{
			RopeID:      id,
			Trait:       ev.Trait,
			AddMembers:  []shareddomain.Address{a1},
			SizeDelta:   1,
			VolumeDelta: 1,
			ValueOrNull: ev.ValueOrNull,

			LastSeen: ev.Time,
		})

		if ropeErr != nil {
			fmt.Printf("로프 마크 삽입 중 에러 발생: %s\n", ropeErr.Error())
		}
	default:
		// 둘 다 있음 → 같은 로프면 볼륨만, 다르면 병합 지시
		id1, id2 := r1, r2
		// 근데 사실 도메인 로직상 이 둘이 절대 같은 로프 ID가 될수 없음
		if id1 == id2 {
			ropeErr := b.busRope.Enqueue(b.ctx, domain.RopeMarkUpsert{
				RopeID:      id1,
				Trait:       ev.Trait,
				VolumeDelta: 1,
				LastSeen:    ev.Time,
				ValueOrNull: ev.ValueOrNull,
			})
			if ropeErr != nil {
				fmt.Printf("로프 마크 삽입 중 에러 발생: %s\n", ropeErr.Error())
			}
		} else {
			// 크기 비교 위해 캐시에서 사이즈 읽기(없으면 편의상 id1을 기준)
			//* 로프 병합
			// 계측 시작
			mergeStart := time.Now()
			b.ropeMergeMetrics.callCount.Add(1)

			size1 := b.GetRopeSize(id1)
			size2 := b.GetRopeSize(id2)
			target := id1
			source := id2
			if size2 > size1 {
				target, source = id2, id1
			}

			// source 로프의 멤버들을 DB에서 직접 가져와서 병합
			sourceRopeMark := b.getRopeMark(source)

			// 계측: 병합된 로프 크기 기록
			b.ropeMergeMetrics.totalMergeSize.Add(int64(sourceRopeMark.Size))
			b.ropeMergeMetrics.mergeCount.Add(1)

			// source의 모든 멤버들을 target으로 변경
			for _, addr := range sourceRopeMark.Members {
				v := b.getOrCreateVertex(addr)
				setRopeForTrait(v, target, ev.Trait)
				b.putVertex(v)
			}

			// target 로프마크에 source 멤버들 흡수 Upsert
			ropeErr := b.busRope.Enqueue(b.ctx, domain.RopeMarkUpsert{
				RopeID:      target,
				Trait:       ev.Trait,
				AddMembers:  sourceRopeMark.Members,
				SizeDelta:   int32(sourceRopeMark.Size),
				ValueOrNull: domain.ParseValueOption(sourceRopeMark.ValueOrNull.V.Add(ev.ValueOrNull.V)),
				VolumeDelta: sourceRopeMark.Volume + 1, // source 볼륨 + 이번 거래
				LastSeen:    ev.Time,
				MergeFrom:   []domain.RopeID{source},
			})
			if ropeErr != nil {
				fmt.Printf("로프 마크 삽입 중 에러 발생: %s\n", ropeErr.Error())
			}

			// 계측: 소요 시간 기록
			elapsed := time.Since(mergeStart)
			b.ropeMergeMetrics.totalTime.Add(elapsed.Nanoseconds())
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
	// --- 5) Vertex 저장 (락프리)
	b.putVertex(v1)
	b.putVertex(v2)

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
		pErr := b.busPolyRope.Enqueue(b.ctx, PolyRopeMarkUpsert{
			PolyRopeID:  newPolyID,
			PolyTrait:   polyTraitCode,
			AddRopes:    []domain.RopeID{currentRopeID},
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})
		if pErr != nil {
			fmt.Printf("폴리 마크 사입중 에러 발생%s\n", pErr.Error())
		}

	case prExist1 && !prExist2:
		// v1만 PolyRope 있음 - v2를 v1의 PolyRope에 편입
		b.setPolyRopeInVertex(v2, pr1, polyTraitCode)

		// 현재 Rope를 PolyRope에 추가 (중복 제거는 dedupRopes에서 처리)
		pErr := b.busPolyRope.Enqueue(b.ctx, PolyRopeMarkUpsert{
			PolyRopeID:  pr1,
			PolyTrait:   polyTraitCode,
			AddRopes:    []domain.RopeID{currentRopeID},
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})

		if pErr != nil {
			fmt.Printf("폴리 마크 사입중 에러 발생%s\n", pErr.Error())
		}
	case !prExist1 && prExist2:
		// v2만 PolyRope 있음 - v1을 v2의 PolyRope에 편입
		b.setPolyRopeInVertex(v1, pr2, polyTraitCode)

		// 현재 Rope를 PolyRope에 추가 (중복 제거는 dedupRopes에서 처리)
		pErr := b.busPolyRope.Enqueue(b.ctx, PolyRopeMarkUpsert{
			PolyRopeID:  pr2,
			PolyTrait:   polyTraitCode,
			AddRopes:    []domain.RopeID{currentRopeID},
			VolumeDelta: 1,
			LastSeen:    ev.Time,
		})

		if pErr != nil {
			fmt.Printf("폴리 마크 사입중 에러 발생%s\n", pErr.Error())
		}

	default:
		// 둘 다 PolyRope 있음
		if pr1 == pr2 {
			// 같은 PolyRope면 현재 Rope만 추가 (중복 제거는 dedupRopes에서 처리)
			pErr := b.busPolyRope.Enqueue(b.ctx, PolyRopeMarkUpsert{
				PolyRopeID:  pr1,
				PolyTrait:   polyTraitCode,
				AddRopes:    []domain.RopeID{currentRopeID},
				VolumeDelta: 1,
				LastSeen:    ev.Time,
			})

			if pErr != nil {
				fmt.Printf("폴리 마크 사입중 에러 발생%s\n", pErr.Error())
			}
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

			pErr := b.busPolyRope.Enqueue(b.ctx, PolyRopeMarkUpsert{
				PolyRopeID:  target,
				PolyTrait:   polyTraitCode,
				AddRopes:    addingRopes, // 중복 제거는 dedupRopes에서 처리
				VolumeDelta: 1,
				LastSeen:    ev.Time,
				MergeFrom:   []domain.PolyRopeID{source},
			})

			if pErr != nil {
				fmt.Printf("폴리 마크 사입중 에러 발생%s\n", pErr.Error())
			}
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

type traitAgg struct {
	tm        domain.TraitMark
	hasLoaded bool
}

func (b *BadgerRopeDB) traitWorker() {
	defer b.wg.Done()

	agg := make(map[domain.TraitID]*traitAgg)
	totalItems := 0

	// ✅ 한 번만 받아서 고정
	traitCh := b.busTrait.Dequeue()
	// 타이머를 '비활성' 상태로 시작
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	// 20초마다 상태 로깅용 타이머
	statusTicker := time.NewTicker(20 * time.Second)
	defer statusTicker.Stop()

	startTimer := func() {
		// 안전하게 멈춘 뒤 드레인하고 Reset
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(batchMaxWait)
	}
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	flush := func() {
		if totalItems == 0 {
			return
		}
		err := b.updateTraitAgg(agg)
		if err != nil {
			fmt.Printf("trait flush err: %v\n", err)
		}

		// reset
		for k := range agg {
			delete(agg, k)
		}
		totalItems = 0

		// 배치가 비었으니 타이머 정지 (다음 첫 아이템에서 다시 시작)
		stopTimer()
	}

	for {
		select {
		case <-b.ctx.Done():
			flush()
			return

		case <-statusTicker.C:
			// 20초마다 버스 상태 출력
			fmt.Printf("[TraitWorker Status] busTrait 큐 - len: %d, cap: %d\n",
				b.busTrait.Len(), b.busTrait.Cap())

		case op, ok := <-traitCh:
			if !ok {
				flush()
				return
			}

			// 🔸 첫 아이템이면 타이머 시작
			if totalItems == 0 {
				startTimer()
			}

			a := agg[op.TraitID]
			if a == nil {
				a = &traitAgg{}
				// 기존 데이터 로드 시도
				if tm := b.getTraitMark(op.TraitID); tm.ID != 0 {
					a.tm = tm
					a.hasLoaded = true
				} else {
					a.tm = domain.TraitMark{
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
						ValueOrNull: domain.ParseValueOption(decimal.Zero),
						LastSeen:    op.LastSeen,
					}
				}
				agg[op.TraitID] = a
			}

			// 누적 규칙
			if !op.ValueOrNull.V.IsZero() {
				a.tm.ValueOrNull = domain.ParseValueOption(a.tm.ValueOrNull.V.Add(op.ValueOrNull.V))
			}
			a.tm.Score += op.ScoreDelta
			a.tm.Volume += op.VolumeDelta
			if op.LastSeen.After(a.tm.LastSeen) {
				a.tm.LastTx = op.LastTx
				a.tm.LastSeen = op.LastSeen
			}
			a.tm.AuthorCodes = mergeAuthor(a.tm.AuthorCodes, op.AuthorCode)

			totalItems++
			if totalItems >= batchMaxItems {
				flush()
			}

		case <-timer.C:
			flush()
		}
	}
}

const (
	batchMaxItems = 2048
	batchMaxWait  = 50 * time.Millisecond
)

func (b *BadgerRopeDB) ropeWorker() {
	defer b.wg.Done()

	saveCount := 0 // 1000회마다 프린트하기 위한 카운터
	ropeCh := b.busRope.Dequeue()

	statusTicker := time.NewTicker(20 * time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return

		case <-statusTicker.C:
			fmt.Printf("[RopeWorker Status] busRope 큐 - len: %d, cap: %d\n",
				b.busRope.Len(), b.busRope.Cap())

		case op, ok := <-ropeCh:
			if !ok {
				return
			}

			err := b.updateRopeMark(op)
			if err != nil {
				fmt.Printf("로프 업서트 중 에러발생 %s\n", err.Error())
				continue
			}

			// 1000회마다 프린트
			saveCount++
			if saveCount%1000 == 0 {
				fmt.Printf("[RopeWorker] 로프 마크 %d개 저장 완료\n", saveCount)
			}
		}
	}
}

type polyRopeAgg struct {
	prm       domain.PolyRopeMark
	toDelete  []domain.PolyRopeID
	hasLoaded bool
}

func (b *BadgerRopeDB) polyRopeWorker() {
	defer b.wg.Done()

	agg := make(map[domain.PolyRopeID]*polyRopeAgg)
	totalItems := 0
	polyCh := b.busPolyRope.Dequeue()
	// 타이머를 '비활성' 상태로 시작
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	// 20초마다 상태 로깅용 타이머
	statusTicker := time.NewTicker(20 * time.Second)
	defer statusTicker.Stop()

	startTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(batchMaxWait)
	}
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	flush := func() {
		if totalItems == 0 {
			return
		}
		err := b.updatePolymarkAgg(agg)
		if err != nil {
			fmt.Printf("polyRope flush err: %v\n", err)
		}

		// reset
		for k := range agg {
			delete(agg, k)
		}
		totalItems = 0

		// 배치가 비었으니 타이머 정지
		stopTimer()
	}

	for {
		select {
		case <-b.ctx.Done():
			flush()
			return

		case <-statusTicker.C:
			// 20초마다 버스 상태 출력
			fmt.Printf("[PolyRopeWorker Status] busPolyRope 큐 - len: %d, cap: %d\n",
				b.busPolyRope.Len(), b.busPolyRope.Cap())

		case op, ok := <-polyCh:
			if !ok {
				flush()
				return
			}

			// 🔸 첫 아이템이면 타이머 시작
			if totalItems == 0 {
				startTimer()
			}

			a := agg[op.PolyRopeID]
			if a == nil {
				a = &polyRopeAgg{}
				// 기존 데이터 로드 시도
				if prm := b.getPolyRopeMark(op.PolyRopeID); prm.ID != 0 {
					a.prm = prm
					a.hasLoaded = true
				} else {
					a.prm = domain.PolyRopeMark{
						ID:        op.PolyRopeID,
						PolyTrait: op.PolyTrait,
						Ropes:     nil,
					}
				}
				agg[op.PolyRopeID] = a
			}

			// 누적 규칙
			if len(op.AddRopes) > 0 {
				a.prm.Ropes = computation.DedupAppend(a.prm.Ropes, op.AddRopes)
			}
			if len(op.MergeFrom) > 0 {
				a.toDelete = append(a.toDelete, op.MergeFrom...)
			}

			totalItems++
			if totalItems >= batchMaxItems {
				flush()
			}

		case <-timer.C:
			flush()
		}
	}
}

func mergeAuthor(authors []domain.AuthorCode, target domain.AuthorCode) []domain.AuthorCode {
	if slices.Contains(authors, target) {
		return authors
	}
	return append(authors, target)
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

// ----- 캐시 기반 버전: 인메모리 캐시 우선 사용 -----
func (b *BadgerRopeDB) getOrCreateVertex(a shareddomain.Address) *domain.Vertex {
	if b.vertexCache != nil {
		return b.vertexCache.Get(a)
	}
	return b.getOrCreateVertexNoLock(a)
}

func (b *BadgerRopeDB) putVertex(v *domain.Vertex) {
	if b.vertexCache != nil {
		b.vertexCache.Put(v.Address, v)
	} else {
		b.putVertexNoLock(v)
	}
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

func (b *BadgerRopeDB) nextTraitID() domain.TraitID {
	for {
		n, _ := b.seqTrait.Next()
		if n != 0 {
			return domain.TraitID(n)
		}
		// 0이면 한 번 더 뽑는다
	}
}

func (b *BadgerRopeDB) nextRopeID() domain.RopeID {
	for {
		n, _ := b.seqRope.Next()
		if n != 0 {
			return domain.RopeID(n)
		}
	}
}

func (b *BadgerRopeDB) nextPolyRopeID() domain.PolyRopeID {
	for {
		n, _ := b.seqPolyRope.Next()
		if n != 0 {
			return domain.PolyRopeID(n)
		}
	}
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

// GetRopeSize 로프 사이즈를 캐시에서 조회 (캐시에 없으면 DB에서 로드)
func (b *BadgerRopeDB) GetRopeSize(ropeID domain.RopeID) uint32 {
	if b.ropeSizeCache != nil {
		return b.ropeSizeCache.Get(ropeID)
	}
	// 캐시가 없으면 직접 DB에서 조회
	rm := b.getRopeMark(ropeID)
	return rm.Size
}

// GetRopeSizeCached 로프 사이즈를 캐시에서만 조회 (없으면 0 반환)
func (b *BadgerRopeDB) GetRopeSizeCached(ropeID domain.RopeID) uint32 {
	if b.ropeSizeCache != nil {
		b.ropeSizeCache.mutex.RLock()
		defer b.ropeSizeCache.mutex.RUnlock()

		if size, exists := b.ropeSizeCache.cache[ropeID]; exists {
			return size
		}
	}
	return 0
}

// statsLoggerWorker - 20초마다 GetGraphStats()를 호출하여 출력
func (b *BadgerRopeDB) statsLoggerWorker() {
	defer b.wg.Done()

	// 시작 시 즉시 한 번 출력
	fmt.Printf("[RopeDB Stats Worker] Started - will report every 20 seconds\n")

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			fmt.Printf("[RopeDB Stats Worker] Shutting down\n")
			return
		case <-ticker.C:
			stats := b.GetGraphStats()

			// 캐시 통계도 함께 출력
			vertexCacheStats := map[string]interface{}{}
			if b.vertexCache != nil {
				vertexCacheStats = b.vertexCache.GetStats()
			}

			ropeSizeCacheStats := map[string]interface{}{}
			if b.ropeSizeCache != nil {
				ropeSizeCacheStats = b.ropeSizeCache.GetStats()
			}

			fmt.Printf("=== [RopeDB Stats] nodes: %v, ropes: %v, traits: %v, polyRopes: %v ===\n",
				stats["nodes"], stats["ropes"], stats["traits"], stats["polyRopes"])
			fmt.Printf("=== [Vertex Cache Stats] size: %v/%v, hitRate: %.2f%%, hits: %v, misses: %v ===\n",
				vertexCacheStats["size"], vertexCacheStats["maxSize"],
				vertexCacheStats["hitRate"].(float64)*100,
				vertexCacheStats["hits"], vertexCacheStats["misses"])
			fmt.Printf("=== [RopeSize Cache Stats] size: %v/%v ===\n",
				ropeSizeCacheStats["size"], ropeSizeCacheStats["maxSize"])
		}
	}
}

// vertexCacheFlushWorker - 주기적으로 캐시를 DB에 플러시
func (b *BadgerRopeDB) vertexCacheFlushWorker() {
	defer b.wg.Done()

	// 5초마다 캐시 플러시
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	fmt.Printf("[Vertex Cache Flush Worker] Started - will flush every 5 seconds\n")

	for {
		select {
		case <-b.ctx.Done():
			fmt.Printf("[Vertex Cache Flush Worker] Final flush and shutting down\n")
			if b.vertexCache != nil {
				flushed := b.vertexCache.FlushAll()
				fmt.Printf("[Vertex Cache Flush Worker] Final flush: %d vertices\n", flushed)
			}
			return
		case <-ticker.C:
			if b.vertexCache != nil {
				flushed := b.vertexCache.FlushAll()
				if flushed > 0 {
					fmt.Printf("[Vertex Cache Flush] Flushed %d vertices to DB\n", flushed)
				}
			}
		}
	}
}
