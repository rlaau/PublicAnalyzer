package ct

import (
	"sync"
)

// ChainTimeout은 특정 시간 이후에 신호를 보내는 타이머입니다
type ChainTimeout struct {
	C        <-chan ChainTime
	c        chan ChainTime
	deadline ChainTime
	stopped  bool
	mu       sync.Mutex
}

// After는 duration 이후에 신호를 받을 채널을 반환합니다
func After(d ChainDuration) <-chan ChainTime {
	return NewTimeout(d).C
}

// Sleep는 지정된 duration 동안 대기합니다 (체인 시간 기준)
// 주의: 실제로 블록하지 않고, 체인 시간이 해당 시점을 지날 때까지 대기합니다
func Sleep(d ChainDuration) {
	<-After(d)
}

// NewTimeout은 특정 duration 이후에 신호를 보내는 타임아웃을 생성합니다
// * 타임 아웃이 전달하는 시간은 타임아웃 했어야 할 시간으로 보정함. 티커와 같은 방식
func NewTimeout(d ChainDuration) *ChainTimeout {
	currentTime := Now()
	deadline := currentTime.Add(d)

	c := make(chan ChainTime, 1)
	timeout := &ChainTimeout{
		C:        c,
		c:        c,
		deadline: deadline,
		stopped:  false,
	}

	// 현재 시간이 이미 deadline을 지났거나 같으면 즉시 신호
	// 데드라인을 전달함
	if !currentTime.Before(deadline) {
		c <- deadline
		close(c)
		timeout.stopped = true
	} else {
		// 타이머에 리스너로 등록
		go timeout.watch()
	}

	return timeout
}

// watch는 시간 업데이트를 감시하고 deadline에 도달하면 신호를 보냅니다
// * 타임 아웃이 전달하는 시간은 타임아웃 했어야 할 시간으로 보정함. 티커와 같은 방식
func (t *ChainTimeout) watch() {
	updates := Subscribe()
	defer Unsubscribe(updates)

	for newTime := range updates {
		t.mu.Lock()
		if t.stopped {
			t.mu.Unlock()
			return
		}

		// deadline을 지났거나 같으면 타임아웃 발생
		if !newTime.Before(t.deadline) {
			select {
			case t.c <- t.deadline:
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
