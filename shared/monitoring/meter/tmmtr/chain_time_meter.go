package tmmtr

import (
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
)

// ChainTimeMeter는 스레드 안전한 타임 미터입니다.
type ChainTimeMeter struct {
	chainTimer *chaintimer.ChainTimer
	mu         sync.RWMutex
}

// NewChainTimeMeter는 새 SafeTimeMeter 인스턴스를 생성합니다.
func NewChainTimeMeter(ct *chaintimer.ChainTimer) *ChainTimeMeter {
	return &ChainTimeMeter{chainTimer: ct}
}
