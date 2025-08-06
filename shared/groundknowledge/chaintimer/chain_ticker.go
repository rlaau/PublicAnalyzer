package chaintimer

import (
	"sync"
	"time"
)

// Ticker는 "큰 시간 점프"시에, 그 시간 간격의 크기만큼 틱을 밀어넣음. 이때 채널 부하 발생 가능함.
const TickChannelBuffer int32 = 100

// ChainTicker는 체인 시간 기반의 주기적 틱을 생성합니다
// ChainTicker는 체인 시간 기반의 주기적 틱을 생성합니다
type ChainTicker struct {
	C        <-chan time.Time
	c        chan time.Time
	duration time.Duration
	nextTick time.Time // 다음 틱 목표 시간
	stopped  bool
	mu       sync.Mutex
}

// NewTicker는 새로운 ChainTicker를 생성합니다
func NewTicker(d time.Duration) *ChainTicker {
	timer := GetChainTimer()
	timer.mu.Lock()
	defer timer.mu.Unlock()

	c := make(chan time.Time, 1)
	ticker := &ChainTicker{
		C:        c,
		c:        c,
		duration: d,
		nextTick: timer.currentTime.Add(d), // 첫 틱 목표 시간 설정
		stopped:  false,
	}

	timer.tickers = append(timer.tickers, ticker)
	return ticker
}

// update는 시간이 전진할 때 호출되어 필요한 만큼 틱을 생성합니다
// * 틱은 시간의 big jump시 "그 시간 간격동안 발생했어야 할 틱"을 한번에 전송함
func (t *ChainTicker) update(newTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped {
		return
	}

	// 새 시간이 다음 틱 목표 시간을 지났거나 같으면 틱 발생
	// 이산적 시간이므로 정확한 시간이 아닌 "목표 시간 도달" 시 틱
	for !newTime.Before(t.nextTick) {
		// 현재 시간으로 틱 발생 (이산적 시간이므로)
		select {
		//*티커는 티킹 시 "티킹 했어야 할 시간"을 전달함.
		case t.c <- t.nextTick:
		default:
			// 채널이 가득 차면 스킵
		}

		// 다음 틱 목표 시간 설정
		t.nextTick = t.nextTick.Add(t.duration)
	}
}

// Stop은 ticker를 정지시킵니다
func (t *ChainTicker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.stopped {
		t.stopped = true
		close(t.c)

		// timer의 ticker 목록에서 제거
		timer := GetChainTimer()
		timer.mu.Lock()
		defer timer.mu.Unlock()
		for i, ticker := range timer.tickers {
			if ticker == t {
				timer.tickers = append(timer.tickers[:i], timer.tickers[i+1:]...)
				break
			}
		}
	}
}
