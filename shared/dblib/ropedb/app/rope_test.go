package app

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
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
func traitA() domain.TraitCode { return domain.TraitCode(100) }
func traitB() domain.TraitCode { return domain.TraitCode(200) }
func traitC() domain.TraitCode { return domain.TraitCode(300) }

func link(t *testing.T, db RopeDB, a1, a2 shareddomain.Address, trait domain.TraitCode) {
	t.Helper()
	ev := domain.TraitEvent{
		Trait: trait,
		RuleA: domain.RuleCustomer,
		RuleB: domain.RuleDeposit,
		Time:  chaintimer.ChainTime(time.Now()),
		Score: 1,
	}
	if err := db.PushTraitEvent(a1, a2, ev); err != nil {
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
func waitRopeMembers(t *testing.T, impl *badgerRopeDB, a shareddomain.Address, trait domain.TraitCode,
	expectCount int, expectMembers ...shareddomain.Address,
) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second) // 여유 있게
	for {
		v := impl.getOrCreateVertex(a)
		rid, ok := impl.ropeIDByTrait(v, trait)
		if ok {
			rm := impl.getRopeMark(rid)
			if rm.ID != 0 && len(rm.Members) == expectCount && containsAll(rm.Members, expectMembers...) {
				return
			}
			// 디버깅 보조
			t.Logf("[waitRopeMembers] a=%v trait=%v rid=%d ok=%v members=%v size=%d",
				a, trait, rid, ok, rm.Members, len(rm.Members))
		} else {
			t.Logf("[waitRopeMembers] a=%v trait=%v: ropeID not yet assigned in vertex", a, trait)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting rope members: a=%v trait=%v wantCount=%d want=%v",
				a, trait, expectCount, expectMembers)
		}
		time.Sleep(3000 * time.Millisecond)
	}
}

func ropeCountOf(t *testing.T, impl *badgerRopeDB, a shareddomain.Address) int {
	t.Helper()
	return len(impl.getOrCreateVertex(a).Ropes)
}

func linksOf(t *testing.T, impl *badgerRopeDB, a shareddomain.Address) []domain.PartnerLink {
	t.Helper()
	return impl.getOrCreateVertex(a).Links
}

// --- 시나리오 ---

func TestRopeDB_UseCase_Spec(t *testing.T) {
	tmp := t.TempDir()
	db, err := NewRopeDBWithRoot(mode.TestingModeProcess, tmp, "tmp_test") // 항상 새/빈 디렉터리
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	impl := db.(*badgerRopeDB)

	// 초기 그래프 구성
	link(t, db, addr(1), addr(2), traitA())
	link(t, db, addr(2), addr(3), traitA())

	link(t, db, addr(3), addr(4), traitB())

	link(t, db, addr(3), addr(5), traitC())
	link(t, db, addr(5), addr(6), traitC())

	link(t, db, addr(7), addr(8), traitA())
	link(t, db, addr(8), addr(9), traitA())
	// 10은 독립

	// 초기 안정화(개별 대기)
	waitRopeMembers(t, impl, addr(1), traitA(), 3, addr(1), addr(2), addr(3))
	waitRopeMembers(t, impl, addr(3), traitB(), 2, addr(3), addr(4))
	waitRopeMembers(t, impl, addr(3), traitC(), 3, addr(3), addr(5), addr(6))
	waitRopeMembers(t, impl, addr(7), traitA(), 3, addr(7), addr(8), addr(9))

	// 액션1: 3-4를 traitC로 연결 → C 로프가 [3,4,5,6]
	link(t, db, addr(3), addr(4), traitC())
	waitRopeMembers(t, impl, addr(3), traitC(), 4, addr(3), addr(4), addr(5), addr(6))

	// 4: 파트너링크 2개(3과 B,C), 로프 개수 2개(B,C)
	{
		links := linksOf(t, impl, addr(4))
		hasB, hasC := false, false
		for _, l := range links {
			if l.Partner == addr(3) && l.Trait == traitB() {
				hasB = true
			}
			if l.Partner == addr(3) && l.Trait == traitC() {
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
	link(t, db, addr(10), addr(4), traitB())
	waitRopeMembers(t, impl, addr(4), traitB(), 3, addr(3), addr(4), addr(10))

	// 액션3: 10-6 traitA → 새 A 로프 [6,10]
	link(t, db, addr(10), addr(6), traitA())
	waitRopeMembers(t, impl, addr(10), traitA(), 2, addr(6), addr(10))

	// 액션4: 10-2 traitA → (6,10) 가 (1,2,3) 로프와 병합 → [1,2,3,6,10]
	link(t, db, addr(10), addr(2), traitA())
	waitRopeMembers(t, impl, addr(2), traitA(), 5, addr(1), addr(2), addr(3), addr(6), addr(10))

	// 액션5: 3-7 traitA → 위 로프와 (7,8,9) 로프 병합 → [1,2,3,6,7,8,9,10]
	link(t, db, addr(3), addr(7), traitA())
	waitRopeMembers(t, impl, addr(3), traitA(), 8, addr(1), addr(2), addr(3), addr(6), addr(7), addr(8), addr(9), addr(10))

	// 3-7 사이 traitA 링크 존재
	{
		links3 := linksOf(t, impl, addr(3))
		ok := false
		for _, l := range links3 {
			if l.Partner == addr(7) && l.Trait == traitA() {
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
}
