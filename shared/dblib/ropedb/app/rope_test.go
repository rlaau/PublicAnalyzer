package app_test

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	relapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	relapi "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/api"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/server"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	. "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// --- 유틸 ---
func addr(n int) shareddomain.Address {
	var a shareddomain.Address
	// n을 뒤쪽 8바이트에 기록해서 유니크하게 만듦
	binary.BigEndian.PutUint64(a[12:], uint64(n))
	return a
}

const (
	//* EOA-EOA표현
	TraitCexAndDeposit domain.TraitCode = 1
	TraitDepostAndUser domain.TraitCode = 2
	TraitSomeOther     domain.TraitCode = 3
	//* EOA-Cont표현
	TraitCreatorAndCreated domain.TraitCode = 4
	TraitAnother           domain.TraitCode = 5
)

var (
	PolyTraitSameCluster domain.PolyTraitCode = 1
	PolyTraitOthers      domain.PolyTraitCode = 2
)
var PolyTraitLegend = map[domain.PolyTraitCode]domain.PolyNameAndTraits{
	PolyTraitSameCluster: {
		Name:   "PolyTraitSameCluster",
		Traits: []domain.TraitCode{TraitDepostAndUser, TraitCreatorAndCreated},
	},
	PolyTraitOthers: {
		Name:   "PolyTraitOthers",
		Traits: []domain.TraitCode{TraitAnother, TraitSomeOther},
	},
}

var (
	TrapFraude domain.TrapCode = 1
)
var TrapLegend = map[domain.TrapCode]domain.TrapNameAndTraits{
	TrapFraude: {
		Name:                "TrapFraud",
		Traits:              []domain.TraitCode{TraitSomeOther, TraitAnother},
		RuleExpectingOrZero: []domain.RuleCode{RuleUser, RuleDeposit},
	},
}

// 트레이트 코드 → 이름 매핑
var TraitLegend = map[domain.TraitCode]string{
	TraitCexAndDeposit:     "TraitCexAndDeposit",
	TraitDepostAndUser:     "TraitDepositAndUser",
	TraitCreatorAndCreated: "TraitCreatorAndCreated",
	TraitSomeOther:         "TraitSomeother",
	TraitAnother:           "TraitAnother",
}

const (
	RuleUser    domain.RuleCode = 1
	RuleDeposit domain.RuleCode = 2
	RuleCEX     domain.RuleCode = 3
	//이 밑으로 추가만!!
	RuleCreator  domain.RuleCode = 4
	RuleCreated  domain.RuleCode = 5
	RuleAnother  domain.RuleCode = 6
	RuleTheother domain.RuleCode = 7
)

// 룰 코드 → 이름 매핑
var RuleLegend = map[domain.RuleCode]string{
	RuleUser:     "RuleUser",
	RuleDeposit:  "RuleDeposit",
	RuleCEX:      "RuleCex",
	RuleCreator:  "RuleCreator",
	RuleCreated:  "RuleCreated",
	RuleAnother:  "RuleAnother",
	RuleTheother: "RuleTheother",
}

const AuthorSystem domain.AuthorCode = 1

var AuthorLegend = map[domain.AuthorCode]string{
	AuthorSystem: "AuthorSystem",
}

func link(t *testing.T, db RopeDB, a1, a2 shareddomain.Address, trait domain.TraitCode) {
	t.Helper()
	cexOrDeposit := uint16(trait)%2 == 0
	var rc domain.RuleCode
	if cexOrDeposit {
		rc = RuleUser
	} else {
		rc = RuleCEX
	}
	ev := domain.NewTraitEvent(AuthorSystem, trait, domain.AddressAndRule{
		Address: a1,
		Rule:    rc,
	}, domain.AddressAndRule{
		Address: a2,
		Rule:    RuleDeposit,
	},
		domain.TxScala{
			Time:     chaintimer.ChainTime(time.Now()),
			ScoreInc: 1,
		})
	if err := db.HandleTraitEvent(ev); err != nil {
		t.Fatalf("PushTraitEvent(%v,%v,trait=%v): %v", a1, a2, trait, err)
	}
}

func containsAll(hay []shareddomain.Address, needles ...shareddomain.Address) bool {
	m := make(map[shareddomain.Address]struct{}, len(hay))
	for _, a := range hay {
		m[a] = struct{}{}
	}
	for _, n := range needles {
		if _, ok := m[n]; !ok {
			return false
		}
	}
	return true
}

// impl의 내부 상태를 이용해 (a, trait) 가 속한 RopeMark 멤버를 기다린다.
func waitRopeMembers(t *testing.T, impl *BadgerRopeDB, a shareddomain.Address, trait domain.TraitCode,
	expectCount int, expectMembers ...shareddomain.Address,
) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second) // 여유 있게
	for {
		// 배치 플러시 강제 실행

		v := impl.GetOrCreateVertex(a)
		rid, ok := impl.RopeIDByTrait(v, trait)
		if ok {
			rm := impl.GetRopeMark(rid)
			if rm.ID != 0 && len(rm.Members) == expectCount && containsAll(rm.Members, expectMembers...) {
				return
			}
			// 디버깅 보조
			t.Logf("[waitRopeMembers] a=%v trait=%v rid=%d ok=%v members=%v size=%d",
				a, trait, rid, ok, rm.Members, len(rm.Members))
		} else {
			t.Logf("[waitRopeMembers] a=%v trait=%v: ropeID not yet assigned in vertex. v.Ropes=%v", a, trait, v.Ropes)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting rope members: a=%v trait=%v wantCount=%d want=%v",
				a, trait, expectCount, expectMembers)
			fmt.Printf("%v: %v", a, v.Ropes)
		}
		time.Sleep(100 * time.Millisecond) // 배치 플러시 후 짧은 대기
	}
}

func ropeCountOf(t *testing.T, impl *BadgerRopeDB, a shareddomain.Address) int {
	t.Helper()
	return len(impl.GetOrCreateVertex(a).Ropes)
}

func linksOf(t *testing.T, impl *BadgerRopeDB, a shareddomain.Address) []domain.TraitRef {
	t.Helper()
	return impl.GetOrCreateVertex(a).Traits
}

// --- 시나리오 ---

func TestRopeDB_UseCase_Spec(t *testing.T) {
	testDir := computation.FindTestingStorageRootPath() + "/rope_visual"
	computation.SetCleanedDir(testDir)
	db, err := NewRopeDBWithRoot(mode.TestingModeProcess, testDir, "rope_test", TraitLegend, RuleLegend, PolyTraitLegend, TrapLegend, AuthorLegend)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	impl := db.(*BadgerRopeDB)
	monitoringServer := server.NewServer(":8080")
	monitoringServer.SetupBasicRoutes()
	//TODO 그냥 서버용 API가 필요했기에 만듦.
	relPool := &relapp.RelationPool{
		ClusterRepo: db,
	}
	analyzer := &app.SimpleTriplet{}
	analyzer.NullButAddDB(relPool)
	relAPI := relapi.NewRelPoolAPIHandler(relPool) // analyzer.GraphDB()가 BadgerRopeDB를 물고 있어야 함
	if err := monitoringServer.RegisterModule(relAPI); err != nil {
		fmt.Printf("   ❌ Failed to register Triplet API: %v\n", err)
	} else {
		fmt.Printf("   ✅ Triplet Analyzer API registered successfully\n")
	}
	go func() {
		fmt.Printf("   🌐 Starting API server on :8080\n")
		if err := monitoringServer.Start(); err != nil {
			fmt.Printf("   ⚠️ API server stopped: %v\n", err)
		}
	}()
	// 초기 그래프 구성
	link(t, db, addr(1), addr(2), TraitCexAndDeposit)
	link(t, db, addr(2), addr(3), TraitCexAndDeposit)

	link(t, db, addr(3), addr(4), TraitDepostAndUser)

	link(t, db, addr(3), addr(5), TraitCreatorAndCreated)
	link(t, db, addr(5), addr(6), TraitCreatorAndCreated)

	link(t, db, addr(7), addr(8), TraitCexAndDeposit)
	link(t, db, addr(8), addr(9), TraitCexAndDeposit)

	link(t, db, addr(11), addr(12), TraitSomeOther)
	link(t, db, addr(12), addr(13), TraitAnother)
	link(t, db, addr(11), addr(10), TraitSomeOther)
	// 10은 독립

	// 초기 안정화(개별 대기)
	waitRopeMembers(t, impl, addr(1), TraitCexAndDeposit, 3, addr(1), addr(2), addr(3))
	waitRopeMembers(t, impl, addr(3), TraitDepostAndUser, 2, addr(3), addr(4))
	waitRopeMembers(t, impl, addr(3), TraitCreatorAndCreated, 3, addr(3), addr(5), addr(6))
	waitRopeMembers(t, impl, addr(7), TraitCexAndDeposit, 3, addr(7), addr(8), addr(9))

	// 액션1: 3-4를 traitC로 연결 → C 로프가 [3,4,5,6]
	link(t, db, addr(3), addr(4), TraitCreatorAndCreated)
	waitRopeMembers(t, impl, addr(3), TraitCreatorAndCreated, 4, addr(3), addr(4), addr(5), addr(6))

	// 4: 파트너링크 2개(3과 B,C), 로프 개수 2개(B,C)
	{
		links := linksOf(t, impl, addr(4))
		hasB, hasC := false, false
		for _, l := range links {
			if l.Partner == addr(3) && l.Trait == TraitDepostAndUser {
				hasB = true
			}
			if l.Partner == addr(3) && l.Trait == TraitCreatorAndCreated {
				hasC = true
			}
		}
		if !hasB || !hasC {
			t.Fatalf("vertex(4) must have partner=3 with traitB and traitC; got=%v", links)
		}
		rc := ropeCountOf(t, impl, addr(4))
		if rc != 2 {
			t.Fatalf("vertex(4) rope count want=2 got=%d", rc)
		}
	}

	// 3의 로프 개수=3 (A,B,C)
	if rc := ropeCountOf(t, impl, addr(3)); rc != 3 {
		t.Fatalf("vertex(3) rope count want=3 got=%d", rc)
	}

	// 액션2: 10-4 traitB → B 로프가 [3,4,10]
	link(t, db, addr(10), addr(4), TraitDepostAndUser)
	waitRopeMembers(t, impl, addr(4), TraitDepostAndUser, 3, addr(3), addr(4), addr(10))

	// 액션3: 10-6 traitA → 새 A 로프 [6,10]
	link(t, db, addr(10), addr(6), TraitCexAndDeposit)
	waitRopeMembers(t, impl, addr(10), TraitCexAndDeposit, 2, addr(6), addr(10))

	// 액션4: 10-2 traitA → (6,10) 가 (1,2,3) 로프와 병합 → [1,2,3,6,10]
	link(t, db, addr(10), addr(2), TraitCexAndDeposit)
	waitRopeMembers(t, impl, addr(2), TraitCexAndDeposit, 5, addr(1), addr(2), addr(3), addr(6), addr(10))

	// 액션5: 3-7 traitA → 위 로프와 (7,8,9) 로프 병합 → [1,2,3,6,7,8,9,10]
	link(t, db, addr(3), addr(7), TraitCexAndDeposit)
	waitRopeMembers(t, impl, addr(3), TraitCexAndDeposit, 8, addr(1), addr(2), addr(3), addr(6), addr(7), addr(8), addr(9), addr(10))

	// 3-7 사이 traitA 링크 존재
	{
		links3 := linksOf(t, impl, addr(3))
		ok := false
		for _, l := range links3 {
			if l.Partner == addr(7) && l.Trait == TraitCexAndDeposit {
				ok = true
				break
			}
		}
		if !ok {
			t.Fatalf("partner link (3<->7, traitA) not found; links=%v", links3)
		}
	}

	// 3의 로프 개수는 여전히 3 (A,B,C)
	if rc := ropeCountOf(t, impl, addr(3)); rc != 3 {
		t.Fatalf("vertex(3) rope count want=3 got=%d", rc)
	}

	// PolyTrait 테스트 - PolyTraitSameCluster는 TraitDepostAndUser(2)와 TraitCreatorAndCreated(4)를 포함
	t.Log("=== PolyTrait 테스트 시작 ===")

	// PolyRope 안정화 대기 (비동기 처리이므로 시간 필요)

	// addr(10)과 addr(5)는 PolyTrait로 같은 로프여야 함
	// 연결 경로: 10-4(TraitDepostAndUser) → 4-3(TraitDepostAndUser) → 3-5(TraitCreatorAndCreated)
	inSame10_5, err := db.ViewInSameRopeByPolyTrait(addr(10), addr(5), PolyTraitSameCluster)
	if err != nil {
		t.Fatalf("ViewInSameRopeByPolyTrait(10, 5) error: %v", err)
	}
	if !inSame10_5 {
		t.Fatalf("addr(10) and addr(5) should be in same PolyRope for PolyTraitSameCluster")
	}
	t.Logf("✅ addr(10) and addr(5) are in same PolyRope: %v", inSame10_5)

	// addr(10)과 addr(2)는 PolyTrait로 다른 로프여야 함
	// addr(2)는 TraitCexAndDeposit(1)로만 연결되어 PolyTrait에 속하지 않음
	inSame10_2, err := db.ViewInSameRopeByPolyTrait(addr(10), addr(2), PolyTraitSameCluster)
	if err != nil {
		t.Fatalf("ViewInSameRopeByPolyTrait(10, 2) error: %v", err)
	}
	if inSame10_2 {
		t.Fatalf("addr(10) and addr(2) should NOT be in same PolyRope for PolyTraitSameCluster")
	}
	t.Logf("✅ addr(10) and addr(2) are in different PolyRope groups: %v", inSame10_2)

	// addr(5)와 addr(2)는 PolyTrait로 다른 로프여야 함 (addr(2)는 PolyTrait 없음)
	inSame5_2, err := db.ViewInSameRopeByPolyTrait(addr(5), addr(2), PolyTraitSameCluster)
	if err != nil {
		t.Fatalf("ViewInSameRopeByPolyTrait(5, 2) error: %v", err)
	}
	if inSame5_2 {
		t.Fatalf("addr(5) and addr(2) should NOT be in same PolyRope for PolyTraitSameCluster")
	}
	t.Logf("✅ addr(5) and addr(2) are in different PolyRope groups: %v", inSame5_2)

	t.Log("=== PolyTrait 테스트 완료 ===")
	isV12Trap := impl.GetOrCreateVertex(addr(12)).Traps.IsContatin(TrapFraude)
	if !isV12Trap {
		t.Fatalf("12번은 Fraude여야 함 %v", impl.GetOrCreateVertex(addr(12)))
	}
	v11 := impl.GetOrCreateVertex(addr(11))
	if v11.Traps.IsContatin(TrapFraude) {
		t.Fatalf("11은 트랩이면 안됨")
	}
	if isContain, traps := impl.AreTrapsInMyRopes(v11); !isContain {
		t.Fatalf("11의 로프는 트랩 %v를 지녀야 함", traps)
	}
	v10 := impl.GetOrCreateVertex(addr(10))
	if isContain, _ := impl.AreTrapsInMyPolyRope(v10, PolyTraitSameCluster); isContain {
		t.Fatalf("10의 same Cluster 폴리 로프는 트랩 없어야 함")
	}

	if isContatin, _ := impl.AreTrapsInMyPolyRope(v10, PolyTraitOthers); !isContatin {
		fmt.Printf("10의 폴리로프&트레이트 %v", v10.PolyRopes)
		t.Fatalf("10은 others 폴리 로프에선 트랩 지녀야 함")
	}

	// 필요하면 테스트 로그에 경로 출력
	t.Log("wrote graph_frame.html & index.html")
	time.Sleep(10 * time.Minute)
}
