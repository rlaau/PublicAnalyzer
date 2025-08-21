package chaintimer

import (
	"fmt"
	"sync"
	"time"
)

// ChainTimer는 블록체인 TX 기반의 이산적 시간 업데이트를 “단일 채널 소스”에서만 수신하여
// 내부 시계를 전진시키고, 구독자/티커/타임아웃에 브로드캐스트합니다.
type ChainTimer struct {
	mu          sync.RWMutex
	currentTime ChainTime

	// 단일 업데이트 소스
	updates <-chan ChainTime

	// 구독자/티커 관리
	listeners []chan ChainTime
	tickers   []*ChainTicker

	// ✅ 적용 완료 브로드캐스트 (ACK)
	// advanceTo가 완료되어 currentTime/티커/리스너 반영이 끝난 뒤 newTime을 흘려보냄
	applied chan ChainTime

	closed bool
}

// NewChainTimer는 하나의 업데이트 채널(updates)과 시작 시각(startTime)을 받아 타이머를 생성합니다.
// 외부에서는 시간을 직접 바꿀 수 없고, 오직 updates 채널을 통해서만 시간이 전파됩니다.
func NewChainTimer(updates <-chan ChainTime, startTime ChainTime) *ChainTimer {
	t := &ChainTimer{
		currentTime: startTime,
		updates:     updates,
		listeners:   make([]chan ChainTime, 0),
		tickers:     make([]*ChainTicker, 0),
		// ✅ ACK 채널 버퍼는 워크로드에 맞게 조정 (드롭 허용)
		applied: make(chan ChainTime, 1024),
	}

	// 단일 소스에서 시간을 비동기 수신
	go func() {
		for newTime := range updates {
			t.advanceTo(newTime)
		}
		// 업데이트 채널이 닫히면 더 이상 전진하지 않음
		t.mu.Lock()
		t.closed = true
		t.mu.Unlock()
	}()

	return t
}

// 내부 전진 로직: 단일 소스에서만 호출됨
func (t *ChainTimer) advanceTo(newTime ChainTime) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}
	if newTime.Equal(t.currentTime) {
		// 동일 시각은 상태변화가 없으니 ACK도 굳이 내보내지 않음 (원하면 여기서 내보내도 됨)
		return
	}
	if newTime.Before(t.currentTime) {
		// 역행은 무시 (원하면 로그만 남김)
		fmt.Printf("[ChainTimer] received past time; ignored. now=%v, got=%v\n", t.currentTime, newTime)
		return
	}

	// 1) 시계 전진
	t.currentTime = newTime

	// 2) 리스너에 브로드캐스트 (비블로킹)
	for _, ch := range t.listeners {
		select {
		case ch <- newTime:
		default:
		}
	}

	// 3) 등록된 티커들 업데이트
	for _, tk := range t.tickers {
		tk.updateLocked(newTime) // tk 내부 락을 자체적으로 잡음
	}

	// 4) ✅ 적용 완료 ACK 브로드캐스트 (비블로킹, 드롭 허용)
	select {
	case t.applied <- newTime:
	default:
	}
}

// Now는 현재 체인 시간을 반환합니다.
func (t *ChainTimer) Now() ChainTime {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.currentTime
}

// Since는 특정 시점 이후 경과한 시간을 반환합니다.
func (t *ChainTimer) Since(since ChainTime) ChainDuration {
	return t.Now().Sub(since)
}

// Until은 특정 시점까지 남은 시간을 반환합니다.
func (t *ChainTimer) Until(target ChainTime) ChainDuration {
	return target.Sub(t.Now())
}

// Subscribe는 시간 업데이트를 받을 채널을 등록합니다.
func (t *ChainTimer) Subscribe() <-chan ChainTime {
	t.mu.Lock()
	defer t.mu.Unlock()

	ch := make(chan ChainTime, 5) // 고속 처리 대비 버퍼
	t.listeners = append(t.listeners, ch)
	return ch
}

// Unsubscribe는 리스너를 제거합니다.
func (t *ChainTimer) Unsubscribe(ch <-chan ChainTime) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, listener := range t.listeners {
		if listener == ch {
			t.listeners = append(t.listeners[:i], t.listeners[i+1:]...)
			close(listener)
			break
		}
	}
}

// closeTicker는 타이머 내부 목록에서 해당 티커를 제거합니다. (ChainTicker.Stop에서 호출)
func (t *ChainTimer) closeTicker(target *ChainTicker) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, tk := range t.tickers {
		if tk == target {
			t.tickers = append(t.tickers[:i], t.tickers[i+1:]...)
			break
		}
	}
}

// ✅ ACK 인터페이스 (추가 메서드: 기존 인터페이스 변화 없음)

// Applied는 advanceTo 적용 완료가 발생할 때마다 newTime을 흘려보내는 채널을 반환합니다.
// - 리더는 이 채널을 통해 “적용 완료 이벤트”를 구독할 수 있습니다.
// - 버퍼가 가득 차면 드롭되니, 결정적 대기는 WaitUntilAtLeast를 쓰세요.
func (t *ChainTimer) Applied() <-chan ChainTime {
	return t.applied
}

// WaitUntilAtLeast는 체인 시간이 target 이상이 될 때까지(= 적용 완료 시점 이후) 대기합니다.
// d는 타임아웃입니다. true면 도달, false면 타임아웃.
func (t *ChainTimer) WaitUntilAtLeast(target ChainTime) bool {
	// 빠른 경로: 이미 도달
	if !t.Now().Before(target) {
		return true
	}

	// ACK 채널로 관찰
	timeout := time.After(100 * time.Millisecond)
	for {
		select {
		case at := <-t.applied:
			// 드롭이 있을 수 있으므로, 채널 값 자체(at)에 의존하지 말고 Now 재확인
			if !t.Now().Before(target) {
				return true
			}
			// 아직 못 미치면 계속
			_ = at
		case <-timeout:
			return false
		}
	}
}
