package chaintimer

import (
	"fmt"
	"sync"
	"time"
)

// ChainTimer는 블록체인 TX 기반의 이산적 시간 업데이트를 위한 타이머입니다
type ChainTimer struct {
	mu          sync.RWMutex
	currentTime time.Time
	listeners   []chan time.Time
	tickers     []*ChainTicker
}

// New는 새로운 ChainTimer를 생성합니다
func New(initialTime time.Time) *ChainTimer {
	return &ChainTimer{
		currentTime: initialTime,
		listeners:   make([]chan time.Time, 0),
		tickers:     make([]*ChainTicker, 0),
	}
}

// Now는 현재 체인 시간을 반환합니다
func (ct *ChainTimer) Now() time.Time {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.currentTime
}

// AdvanceTo는 시간을 특정 시점으로 전진시킵니다
// 새로운 시간이 현재 시간보다 이전이거나 같으면 무시됩니다
func (ct *ChainTimer) AdvanceTo(newTime time.Time) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if newTime.Equal(ct.currentTime) {
		//시간이 동일 시엔 무시
		return
	}
	// 시간이 역행하거나 같으면 무시
	if newTime.Before(ct.currentTime) {
		fmt.Printf("입력받은 시간이 타이머의 시스템 시간보다 이전입니다. 때문에 시간 진보가 불가능합니다.")
		return
	}

	ct.currentTime = newTime

	// 모든 리스너에게 시간 업데이트 알림
	for _, listener := range ct.listeners {
		select {
		case listener <- newTime:
		default:
			// 비블로킹 전송 - 리스너가 받을 준비가 안 되어 있으면 스킵
		}
	}

	// 모든 ticker 업데이트
	for _, ticker := range ct.tickers {
		ticker.update(newTime)
	}
}

// Subscribe는 시간 업데이트를 받을 채널을 등록합니다
func (ct *ChainTimer) Subscribe() <-chan time.Time {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	//고속 처리 환경에서 혹시 몰라서 안전 용도로 5의 용량 부여
	ch := make(chan time.Time, 5)
	ct.listeners = append(ct.listeners, ch)
	return ch
}

// Unsubscribe는 리스너를 제거합니다
func (ct *ChainTimer) Unsubscribe(ch <-chan time.Time) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	for i, listener := range ct.listeners {
		if listener == ch {
			ct.listeners = append(ct.listeners[:i], ct.listeners[i+1:]...)
			close(listener)
			break
		}
	}
}

// Since는 특정 시점 이후 경과한 시간을 반환합니다
func (ct *ChainTimer) Since(t time.Time) time.Duration {
	return ct.Now().Sub(t)
}

// Until는 특정 시점까지 남은 시간을 반환합니다
func (ct *ChainTimer) Until(t time.Time) time.Duration {
	return t.Sub(ct.Now())
}
