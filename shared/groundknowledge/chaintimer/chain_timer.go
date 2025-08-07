package chaintimer

import (
	"fmt"
	"sync"
)

var (
	instance         *ChainTimer
	once             sync.Once
	DefaultChainTime = NewChainTime(2024, 1, 1, 0, 0, 0, 0)
)

// ChainTimer는 블록체인 TX 기반의 이산적 시간 업데이트를 위한 타이머입니다
type ChainTimer struct {
	mu          sync.RWMutex
	currentTime ChainTime
	listeners   []chan ChainTime
	tickers     []*ChainTicker
	initialized bool
}

// Since는 특정 시점 이후 경과한 시간을 반환합니다
func Since(t ChainTime) ChainDuration {
	return Now().Sub(t)
}

// Until는 특정 시점까지 남은 시간을 반환합니다
func Until(t ChainTime) ChainDuration {
	return t.Sub(Now())
}

// AdvanceTo는 시간을 특정 시점으로 전진시킵니다
// 새로운 시간이 현재 시간보다 이전이거나 같으면 무시됩니다
func AdvanceTo(newTime ChainTime) {
	timer := GetChainTimer()
	timer.mu.Lock()
	defer timer.mu.Unlock()

	if newTime.Equal(timer.currentTime) {
		// 시간이 동일 시엔 무시
		return
	}

	// 시간이 역행하면 무시
	if newTime.Before(timer.currentTime) {
		fmt.Printf("입력받은 시간이 타이머의 시스템 시간보다 이전입니다. 때문에 시간 진보가 불가능합니다.\n")
		return
	}

	timer.currentTime = newTime

	// 모든 리스너에게 시간 업데이트 알림
	for _, listener := range timer.listeners {
		select {
		case listener <- newTime:
		default:
			// 비블로킹 전송 - 리스너가 받을 준비가 안 되어 있으면 스킵
		}
	}

	// 모든 ticker 업데이트
	for _, ticker := range timer.tickers {
		ticker.update(newTime)
	}
}

// Subscribe는 시간 업데이트를 받을 채널을 등록합니다
func Subscribe() <-chan ChainTime {
	timer := GetChainTimer()
	timer.mu.Lock()
	defer timer.mu.Unlock()

	// 고속 처리 환경에서 혹시 몰라서 안전 용도로 5의 용량 부여
	ch := make(chan ChainTime, 5)
	timer.listeners = append(timer.listeners, ch)
	return ch
}

// Unsubscribe는 리스너를 제거합니다
func Unsubscribe(ch <-chan ChainTime) {
	timer := GetChainTimer()
	timer.mu.Lock()
	defer timer.mu.Unlock()

	for i, listener := range timer.listeners {
		if listener == ch {
			timer.listeners = append(timer.listeners[:i], timer.listeners[i+1:]...)
			close(listener)
			break
		}
	}
}

// GetChainTimer는 ChainTimer의 싱글톤 인스턴스를 반환합니다
// 초기화되지 않았다면 DefaultChainTime으로 자동 초기화됩니다
func GetChainTimer() *ChainTimer {
	once.Do(func() {
		instance = &ChainTimer{
			listeners: make([]chan ChainTime, 0),
			tickers:   make([]*ChainTicker, 0),
		}
	})

	// 초기화 체크는 lock 없이 먼저 확인 (더블 체크 패턴)
	if !instance.initialized {
		instance.mu.Lock()
		if !instance.initialized {
			instance.currentTime = DefaultChainTime
			instance.initialized = true
		}
		instance.mu.Unlock()
	}

	return instance
}

// Initialize는 싱글톤 ChainTimer를 초기화합니다
// 이미 초기화되었다면 false를 반환합니다
func Initialize(initialTime ChainTime) bool {
	timer := GetChainTimer()
	timer.mu.Lock()
	defer timer.mu.Unlock()

	if timer.initialized {
		return false
	}

	timer.currentTime = initialTime
	timer.initialized = true
	return true
}

// Reset는 테스트 용도로 싱글톤을 리셋합니다
// * => 프로덕션에서는 사용하지 마삼
func Reset() {
	instance = nil
	once = sync.Once{}
}

// Now는 현재 체인 시간을 반환합니다
func Now() ChainTime {
	timer := GetChainTimer()
	timer.mu.RLock()
	defer timer.mu.RUnlock()

	return timer.currentTime
}
