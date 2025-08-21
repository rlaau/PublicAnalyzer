package chaintimer

import "sync"

// 큰 점프 시, 누락된 틱 수만큼 밀어넣을 수 있으므로 버퍼는 넉넉히.
const TickChannelBuffer int32 = 100

// ChainTicker는 체인 시간 기반의 주기적 틱을 생성합니다.
// 타이머의 시간이 nextTick 이상으로 전진할 때마다, 해당 시각을 기준으로 틱을 발생시킵니다.
// (실제 “그때의 시각”을 내려보냄)
type ChainTicker struct {
	timer    *ChainTimer
	C        <-chan ChainTime
	c        chan ChainTime
	duration ChainDuration
	nextTick ChainTime
	stopped  bool
	mu       sync.Mutex
}

// NewTicker는 특정 duration 주기의 티커를 생성합니다.
// 첫 틱 목표 시각은 timer.Now().Add(d)로 설정됩니다.
func NewTicker(timer *ChainTimer, d ChainDuration) *ChainTicker {
	timer.mu.RLock()
	start := timer.currentTime
	timer.mu.RUnlock()

	c := make(chan ChainTime, TickChannelBuffer)
	tk := &ChainTicker{
		timer:    timer,
		C:        c,
		c:        c,
		duration: d,
		nextTick: start.Add(d),
		stopped:  false,
	}

	// 타이머에 등록 (업데이트 루프가 tk.updateLocked를 호출)
	timer.mu.Lock()
	timer.tickers = append(timer.tickers, tk)
	timer.mu.Unlock()

	return tk
}

// updateLocked는 ChainTimer가 시간을 전진할 때 호출됩니다.
// newTime이 nextTick 이상이면, 누락된 모든 틱을 밀어 넣습니다.
func (t *ChainTicker) updateLocked(newTime ChainTime) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped {
		return
	}

	for !newTime.Before(t.nextTick) {
		select {
		case t.c <- t.nextTick:
		default:
			// 버퍼 초과 시 드롭
		}
		t.nextTick = t.nextTick.Add(t.duration)
	}
}

// Stop은 티커를 정지시키고 채널을 닫습니다.
func (t *ChainTicker) Stop() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
	close(t.c)
	t.mu.Unlock()

	// 타이머의 목록에서 제거
	t.timer.closeTicker(t)
}
