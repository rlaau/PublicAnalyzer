package chaintimer

import "sync"

// ChainTimeout은 특정 deadline(= 생성 시점 + d)에 도달하면 신호를 한 번 보내는 타이머입니다.
type ChainTimeout struct {
	timer    *ChainTimer
	C        <-chan ChainTime
	c        chan ChainTime
	deadline ChainTime
	stopped  bool
	mu       sync.Mutex
}

// After는 timer 기준 d 이후에 신호를 받을 채널을 반환합니다.
func After(timer *ChainTimer, d ChainDuration) <-chan ChainTime {
	return NewTimeout(timer, d).C
}

// Sleep는 지정된 duration 동안 “체인 시간”이 흐를 때까지 대기합니다.
func Sleep(timer *ChainTimer, d ChainDuration) {
	<-After(timer, d)
}

// NewTimeout은 timer.Now()+d에 도달하면 정확히 한 번 deadline 시각을 내보내는 타임아웃을 생성합니다.
func NewTimeout(timer *ChainTimer, d ChainDuration) *ChainTimeout {
	current := timer.Now()
	deadline := current.Add(d)

	c := make(chan ChainTime, 1)
	to := &ChainTimeout{
		timer:    timer,
		C:        c,
		c:        c,
		deadline: deadline,
		stopped:  false,
	}

	// 이미 지난 경우 즉시 발화
	if !current.Before(deadline) {
		c <- deadline
		close(c)
		to.stopped = true
		return to
	}

	// 업데이트 감시
	go to.watch()
	return to
}

func (t *ChainTimeout) watch() {
	updates := t.timer.Subscribe()
	defer t.timer.Unsubscribe(updates)

	for newTime := range updates {
		t.mu.Lock()
		if t.stopped {
			t.mu.Unlock()
			return
		}
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

// Stop은 타임아웃을 취소합니다.
func (t *ChainTimeout) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	t.stopped = true
	close(t.c)
	return true
}
