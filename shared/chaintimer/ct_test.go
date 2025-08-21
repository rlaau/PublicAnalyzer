package chaintimer_test

import (
	"testing"
	"time"

	. "github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
)

// 체인 시간 업데이트를 밀어 넣는다. (실제 시간 Sleep 없음: 완전 결정적)
func pushSteps(ch chan<- ChainTime, start ChainTime, steps ...ChainDuration) {
	cur := start
	for _, d := range steps {
		cur = cur.Add(d)
		ch <- cur
	}
}

func TestTimer_TickerAndTimeout_BasicAndBigJump(t *testing.T) {
	start := NewChainTime(2024, 1, 1, 0, 0, 0, 0)

	updates := make(chan ChainTime, 32)
	timer := NewChainTimer(updates, start)

	// 주기 10s 티커, 15s 타임아웃
	tk := NewTicker(timer, NewChainDurationSeconds(10))
	defer tk.Stop()
	to := NewTimeout(timer, NewChainDurationSeconds(15))

	// 타임라인 전진 시나리오:
	//  +5s   ->  5  (no tick)
	//  +5s   -> 10  (tick at 10)
	//  +1s   -> 11  (no tick)
	//  +9s   -> 20  (tick at 20) + timeout at 15 (fires here)
	//  +30s  -> 50  (big jump: ticks at 30, 40, 50 → 3 ticks at once)
	pushSteps(updates, start,
		NewChainDurationSeconds(5),
		NewChainDurationSeconds(5),
		NewChainDurationSeconds(1),
		NewChainDurationSeconds(9),
		NewChainDurationSeconds(30),
	)
	close(updates) // 업데이트 종료(선택)

	// 1) 첫 번째 틱: +10s
	wantTick1 := start.Add(NewChainDurationSeconds(10))
	gotTick1 := <-tk.C
	if !gotTick1.Equal(wantTick1) {
		t.Fatalf("tick#1 mismatch: got=%v, want=%v", gotTick1, wantTick1)
	}

	// 2) 타임아웃: +15s에 정확히 1회
	wantDeadline := start.Add(NewChainDurationSeconds(15))
	gotDeadline := <-to.C
	if !gotDeadline.Equal(wantDeadline) {
		t.Fatalf("timeout mismatch: got=%v, want=%v", gotDeadline, wantDeadline)
	}
	// 다시 읽으면 닫혀 있어야 함(더 이상 이벤트 없음)
	select {
	case _, ok := <-to.C:
		if ok {
			t.Fatalf("timeout channel should be closed after single fire")
		}
	default:
		// non-blocking: 이미 닫혔으면 여기로 오지 않을 수 있으니 추가로 정확히 확인
	}

	// 3) 두 번째 틱: +20s
	wantTick2 := start.Add(NewChainDurationSeconds(20))
	gotTick2 := <-tk.C
	if !gotTick2.Equal(wantTick2) {
		t.Fatalf("tick#2 mismatch: got=%v, want=%v", gotTick2, wantTick2)
	}

	// 4) 빅 점프(+30s)에서 누락된 틱 3개: 30, 40, 50
	expectTicks := []ChainTime{
		start.Add(NewChainDurationSeconds(30)),
		start.Add(NewChainDurationSeconds(40)),
		start.Add(NewChainDurationSeconds(50)),
	}
	for i, want := range expectTicks {
		select {
		case got := <-tk.C:
			if !got.Equal(want) {
				t.Fatalf("big-jump tick#%d mismatch: got=%v, want=%v", i+1, got, want)
			}
		case <-time.After(50 * time.Millisecond):
			t.Fatalf("big-jump tick#%d timed out waiting for %v", i+1, want)
		}
	}

	// 5) Stop 이후에는 채널이 닫히고 더 이상 값이 오지 않아야 함
	tk.Stop()
	// Stop 이후 즉시 읽으면, 채널은 닫혀 있어 ok=false 여야 함.
	select {
	case _, ok := <-tk.C:
		if ok {
			t.Fatalf("ticker channel should be closed after Stop()")
		}
	default:
		// non-blocking으로 비어있을 수도 있으니, 닫힘을 확정하려면 한번 더 블로킹 수신해 확인
		select {
		case _, ok := <-tk.C:
			if ok {
				t.Fatalf("ticker channel should be closed after Stop() (2nd read)")
			}
		case <-time.After(50 * time.Millisecond):
			// 닫힌 채널이라면 즉시 수신되어야 함. 타임아웃이면 닫히지 않은 것처럼 동작하는 상태.
			t.Fatalf("ticker channel did not close promptly after Stop()")
		}
	}
}

// ! 현재 채널 변경 <-> now로 읽기 시간 차이 존재함을 확인함
// ! 근데 이건 mutex Lock이 있는데도 경합이 일어나는 것. 그 이유는 스레드간 강제 동기화는 본질적으로 불가하기 때문
// ! 테스트 결과 약 30마이크로 초 선의 콜드스타트가 존재하는듯. 밑의 또다른 테스트에서 이를 ack chan기반으로 해결은 했다만 이는 Now만큼 직관적이진 못함
// ! 콜드스타트의 극 일부를 제외하곤 현재 Now는 준-동기-확정적으로 작동하니, 이 부분을 알고만 있고, 사용은 그대로 하면 될듯.
// *다만, 때문에 이 테스트의 실패는 컴퓨터 성능에 따라 다를 것임. 클락타임이 빠른 컴퓨터면 더 성공 확률이 높을거고, 스레드 생성 및 스케쥴링 시간이 50mircoSec이상이면 실패 가능
func TestTimer_SinceUntil_API(t *testing.T) {
	start := NewChainTime(2024, 1, 1, 0, 0, 0, 0)
	updates := make(chan ChainTime, 8)
	timer := NewChainTimer(updates, start)

	// 현재 Now()는 start여야 한다.
	if !timer.Now().Equal(start) {
		t.Fatalf("Now mismatch at init: got=%v, want=%v", timer.Now(), start)
	}

	// +12s로 전진
	updates <- start.Add(NewChainDurationSeconds(12))
	close(updates)

	time.Sleep(50 * time.Microsecond)
	// Since(start) == 12s
	if got := timer.Since(start); got != NewChainDurationSeconds(12) {
		t.Fatalf("Since mismatch: got=%v, want=%v", got, NewChainDurationSeconds(12))
	}

	// Until(start+20s) == 8s
	wantTarget := start.Add(NewChainDurationSeconds(20))
	if got := timer.Until(wantTarget); got != NewChainDurationSeconds(8) {
		t.Fatalf("Until mismatch: got=%v, want=%v", got, NewChainDurationSeconds(8))
	}
}

// ! 이게 바로 그 ack chan(applied chan)이용한 것
// ! 이 경우 time.Sleep불필요
func TestTimer_WaitUntil_API(t *testing.T) {
	start := NewChainTime(2024, 1, 1, 0, 0, 0, 0)
	updates := make(chan ChainTime, 8)
	timer := NewChainTimer(updates, start)

	// 현재 Now()는 start여야 한다.
	if !timer.Now().Equal(start) {
		t.Fatalf("Now mismatch at init: got=%v, want=%v", timer.Now(), start)
	}

	// +12s로 전진
	updates <- start.Add(NewChainDurationSeconds(12))
	close(updates)

	// Since(start) == 12s
	if !timer.WaitUntilAtLeast(start.Add(NewChainDurationSeconds(12))) {
		t.Fatalf("timer not reached target; now=%v target=%v", timer.Now(), start.Add(NewChainDurationSeconds(12)))
	}

	// Until(start+20s) == 8s
	wantTarget := start.Add(NewChainDurationSeconds(20))
	if got := timer.Until(wantTarget); got != NewChainDurationSeconds(8) {
		t.Fatalf("Until mismatch: got=%v, want=%v", got, NewChainDurationSeconds(8))
	}
}
