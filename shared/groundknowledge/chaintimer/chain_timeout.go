package chaintimer

import (
	"sync"
	"time"
)

// ChainTimeout은 특정 시간 이후에 신호를 보내는 타이머입니다
type ChainTimeout struct {
	C        <-chan time.Time
	c        chan time.Time
	deadline time.Time
	timer    *ChainTimer
	stopped  bool
	mu       sync.Mutex
}

// NewTimeout은 특정 duration 이후에 신호를 보내는 타임아웃을 생성합니다
// * 타임아웃은 티커같은 결과치 보정 없음. 그러니까, big jump후 타임아웃 발생은 하는데, 그 경우 타임아웃의 발생시점은 그 큰 미래시점임.
// ticker는 big jump일어나도, "티킹 했어야 했을 시간"으로 보정해서 타임 전달함.
func (ct *ChainTimer) NewTimeout(d time.Duration) *ChainTimeout {
	ct.mu.RLock()
	deadline := ct.currentTime.Add(d)
	currentTime := ct.currentTime
	ct.mu.RUnlock()

	c := make(chan time.Time, 1)
	timeout := &ChainTimeout{
		C:        c,
		c:        c,
		deadline: deadline,
		timer:    ct,
		stopped:  false,
	}

	// 현재 시간이 이미 deadline을 지났거나 같으면 즉시 신호
	if !currentTime.Before(deadline) {
		c <- currentTime
		close(c)
		timeout.stopped = true
	} else {
		// 타이머에 리스너로 등록
		go timeout.watch()
	}

	return timeout
}

// watch는 시간 업데이트를 감시하고 deadline에 도달하면 신호를 보냅니다
func (t *ChainTimeout) watch() {
	updates := t.timer.Subscribe()
	defer t.timer.Unsubscribe(updates)

	for newTime := range updates {
		t.mu.Lock()
		if t.stopped {
			t.mu.Unlock()
			return
		}

		// deadline을 지났거나 같으면 타임아웃 발생
		if !newTime.Before(t.deadline) {
			select {
			case t.c <- newTime:
			default:
			}
			close(t.c)
			t.stopped = true
			t.mu.Unlock()
			return
		}
		t.mu.Unlock()
	}
}

// Stop은 타임아웃을 취소합니다
func (t *ChainTimeout) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.stopped {
		t.stopped = true
		close(t.c)
		return true
	}
	return false
}

// After는 duration 이후에 신호를 받을 채널을 반환합니다
func (ct *ChainTimer) After(d time.Duration) <-chan time.Time {
	return ct.NewTimeout(d).C
}

// Sleep는 지정된 duration 동안 대기합니다 (체인 시간 기준)
// 주의: 실제로 블록하지 않고, 체인 시간이 해당 시점을 지날 때까지 대기합니다
func (ct *ChainTimer) Sleep(d time.Duration) {
	<-ct.After(d)
}
