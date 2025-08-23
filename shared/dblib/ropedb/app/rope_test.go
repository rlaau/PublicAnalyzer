package app

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
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
	TraitApple   domain.TraitCode = 100
	TraitBravo   domain.TraitCode = 200
	TraitCharlie domain.TraitCode = 300
)

// 트레이트 코드 → 이름 매핑
var TraitLegend = map[domain.TraitCode]string{
	TraitApple:   "TraitApple",
	TraitBravo:   "TraitBravo",
	TraitCharlie: "TraitCharlie",
}

const (
	RuleCustomer domain.RuleCode = 1
	RuleDeposit  domain.RuleCode = 2
)

// 룰 코드 → 이름 매핑
var RuleLegend = map[domain.RuleCode]string{
	RuleCustomer: "RuleCustomer",
	RuleDeposit:  "RuleDeposit",
}

func link(t *testing.T, db RopeDB, a1, a2 shareddomain.Address, trait domain.TraitCode) {
	t.Helper()
	ev := domain.NewTraitEvent(trait, domain.AddressAndRule{
		Address: a1,
		Rule:    RuleCustomer,
	}, domain.AddressAndRule{
		Address: a2,
		Rule:    RuleDeposit,
	},
		domain.TxScala{
			Time:     chaintimer.ChainTime(time.Now()),
			ScoreInc: 1,
		})
	if err := db.PushTraitEvent(ev); err != nil {
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

func ropeCountOf(t *testing.T, impl *BadgerRopeDB, a shareddomain.Address) int {
	t.Helper()
	return len(impl.getOrCreateVertex(a).Ropes)
}

func linksOf(t *testing.T, impl *BadgerRopeDB, a shareddomain.Address) []domain.TraitRef {
	t.Helper()
	return impl.getOrCreateVertex(a).Traits
}
func clearDir(path string) error {
	dirEntries, err := os.ReadDir(path)
	if err != nil {
		// 디렉토리가 없다면 새로 만들기
		if os.IsNotExist(err) {
			return os.MkdirAll(path, 0o755)
		}
		return err
	}
	for _, entry := range dirEntries {
		entryPath := filepath.Join(path, entry.Name())
		if err := os.RemoveAll(entryPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", entryPath, err)
		}
	}
	return nil
}

// --- 시나리오 ---

func TestRopeDB_UseCase_Spec(t *testing.T) {
	testDir := computation.FindTestingStorageRootPath() + "/rope_visual"
	clearDir(testDir)
	db, err := NewRopeDBWithRoot(mode.TestingModeProcess, testDir, "rope_test", TraitLegend, RuleLegend) // 항상 새/빈 디렉터리
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	impl := db.(*BadgerRopeDB)

	// 초기 그래프 구성
	link(t, db, addr(1), addr(2), TraitApple)
	link(t, db, addr(2), addr(3), TraitApple)

	link(t, db, addr(3), addr(4), TraitBravo)

	link(t, db, addr(3), addr(5), TraitCharlie)
	link(t, db, addr(5), addr(6), TraitCharlie)

	link(t, db, addr(7), addr(8), TraitApple)
	link(t, db, addr(8), addr(9), TraitApple)
	// 10은 독립

	// 초기 안정화(개별 대기)
	waitRopeMembers(t, impl, addr(1), TraitApple, 3, addr(1), addr(2), addr(3))
	waitRopeMembers(t, impl, addr(3), TraitBravo, 2, addr(3), addr(4))
	waitRopeMembers(t, impl, addr(3), TraitCharlie, 3, addr(3), addr(5), addr(6))
	waitRopeMembers(t, impl, addr(7), TraitApple, 3, addr(7), addr(8), addr(9))

	// 액션1: 3-4를 traitC로 연결 → C 로프가 [3,4,5,6]
	link(t, db, addr(3), addr(4), TraitCharlie)
	waitRopeMembers(t, impl, addr(3), TraitCharlie, 4, addr(3), addr(4), addr(5), addr(6))

	// 4: 파트너링크 2개(3과 B,C), 로프 개수 2개(B,C)
	{
		links := linksOf(t, impl, addr(4))
		hasB, hasC := false, false
		for _, l := range links {
			if l.Partner == addr(3) && l.Trait == TraitBravo {
				hasB = true
			}
			if l.Partner == addr(3) && l.Trait == TraitCharlie {
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
	link(t, db, addr(10), addr(4), TraitBravo)
	waitRopeMembers(t, impl, addr(4), TraitBravo, 3, addr(3), addr(4), addr(10))

	// 액션3: 10-6 traitA → 새 A 로프 [6,10]
	link(t, db, addr(10), addr(6), TraitApple)
	waitRopeMembers(t, impl, addr(10), TraitApple, 2, addr(6), addr(10))

	// 액션4: 10-2 traitA → (6,10) 가 (1,2,3) 로프와 병합 → [1,2,3,6,10]
	link(t, db, addr(10), addr(2), TraitApple)
	waitRopeMembers(t, impl, addr(2), TraitApple, 5, addr(1), addr(2), addr(3), addr(6), addr(10))

	// 액션5: 3-7 traitA → 위 로프와 (7,8,9) 로프 병합 → [1,2,3,6,7,8,9,10]
	link(t, db, addr(3), addr(7), TraitApple)
	waitRopeMembers(t, impl, addr(3), TraitApple, 8, addr(1), addr(2), addr(3), addr(6), addr(7), addr(8), addr(9), addr(10))

	// 3-7 사이 traitA 링크 존재
	{
		links3 := linksOf(t, impl, addr(3))
		ok := false
		for _, l := range links3 {
			if l.Partner == addr(7) && l.Trait == TraitApple {
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
	frame := impl.MakeGraphFrameHTML("Rope Graph Viewer")

	// 2) 백그라운드 HTML (내부에서 FetchDefaultGraphJson 호출)
	bg, err := impl.MakeBackgroundHTML("graph_frame.html")
	if err != nil {
		t.Logf("bg warn: %v", err)
	}

	// 3) 파일로 덤프
	_ = os.WriteFile(filepath.Join(testDir, "graph_frame.html"), []byte(frame), 0o644)
	_ = os.WriteFile(filepath.Join(testDir, "index.html"), []byte(bg), 0o644)

	// 필요하면 테스트 로그에 경로 출력
	t.Log("wrote graph_frame.html & index.html")

}
