package tmmtr

import (
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"
)

// ChainTimeMeter는 스레드 안전한 타임 미터입니다.
type ChainTimeMeter struct {
	startTime chaintimer.ChainTime
	mu        sync.RWMutex
}

// NewChainTimeMeter는 새 SafeTimeMeter 인스턴스를 생성합니다.
func NewChainTimeMeter() *ChainTimeMeter {
	return &ChainTimeMeter{}
}

// SetStartTime은 체인타이머로부터 타이머 시작 시각을 등록합니다.
func (m *ChainTimeMeter) SetStartTime(startTime chaintimer.ChainTime) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startTime = startTime
}

// GetSinceStartTime은 체인타이머로부터 시작 시각 이후 경과한 시간을 반환합니다.
func (m *ChainTimeMeter) GetSinceStartTime() chaintimer.ChainDuration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.startTime == chaintimer.ChainTime(chaintimer.DefaultChainTime) {
		return 0
	}
	return chaintimer.Since(m.startTime)
}
