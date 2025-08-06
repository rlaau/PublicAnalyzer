package tm

import (
	"sync"
	"time"
)

type TimeMeter interface {
	SetStartTime(startTime time.Time)
	GetSinceStartTime() time.Duration
}

// RealTimeMeter는 스레드 안전한 타임 미터입니다.
type RealTimeMeter struct {
	startTime time.Time
	mu        sync.RWMutex
}

// NewRealTimeMeter는 새 SafeTimeMeter 인스턴스를 생성합니다.
func NewRealTimeMeter() *RealTimeMeter {
	return &RealTimeMeter{}
}

// SetStartTime은 타이머 시작 시각을 등록합니다.
func (m *RealTimeMeter) SetStartTime(startTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startTime = startTime
}

// GetSinceStartTime은 시작 시각 이후 경과한 시간을 반환합니다.
func (m *RealTimeMeter) GetSinceStartTime() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.startTime.IsZero() {
		return 0
	}
	return time.Since(m.startTime)
}
