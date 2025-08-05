package chaintimer

import (
	"sync"
	"time"
)

// Ticker는 "큰 시간 점프"시에, 그 시간 간격의 크기만큼 틱을 밀어넣음. 이때 채널 부하 발생 가능함.
const TickChannelBuffer int32 = 100

// ChainTicker는 체인 시간 기반의 주기적 틱을 생성합니다
type ChainTicker struct {
	C        <-chan time.Time
	c        chan time.Time
	duration time.Duration
	nextTick time.Time // lastTick 대신 nextTick 사용 (다음 틱 목표 시간)
	timer    *ChainTimer
	stopped  bool
	mu       sync.Mutex
}

// NewChainTicker는 새로운 ChainTicker를 생성합니다
// * Ticker는 큰 시간 점프에 대해서 "이때까지의 차이 동안 발생했을 틱"을 한번에 flush함.
func (ct *ChainTimer) NewTicker(d time.Duration) *ChainTicker {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	c := make(chan time.Time, TickChannelBuffer)
	ticker := &ChainTicker{
		C:        c,
		c:        c,
		duration: d,
		nextTick: ct.currentTime.Add(d), // 첫 틱 목표 시간 설정
		timer:    ct,
		stopped:  false,
	}

	ct.tickers = append(ct.tickers, ticker)
	return ticker
}

// update는 시간이 전진할 때 호출되어 필요한 만큼 틱을 생성합니다
// * Ticker는 큰 시간 점프에 대해서 "이때까지의 차이 동안 발생했을 틱"을 한번에 flush함.
func (t *ChainTicker) update(newTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped {
		return
	}

	// 새 시간이 다음 틱 목표 시간을 지났거나 같으면 틱 발생
	// 이산적 시간이므로 정확한 시간이 아닌 "목표 시간 도달" 시 틱
	//반복적인 for루프에서 알 수 있듯, 이 절차는 큰 시간 점프에 대해서 듀레이션씩 계쏙 업뎃해가며 많은 수의 틱을 발생시킴
	for !newTime.Before(t.nextTick) {
		//tick하기로 설정되었던 시간
		tickTime := t.nextTick.Add(t.duration)
		// 현재 시간으로 틱 발생 (이산적 시간이므로)
		select {
		//tickTime을 flush함으로써, "받는 입장"에선 "정확한 시간에 틱을 받는"셈이 됨
		case t.c <- tickTime:
		default:
			// 채널이 가득 차면 스킵
		}
		t.nextTick = tickTime

		// 다음 틱 목표 시간 설정
		//처음에 값 기반 생성된 nextTick에 오직 듀레이션만을 더해서 시간 오차 증폭 막음.
		// 그러므로 큰 시간 점프가 있었다면 그만큼 계속 티킹함.
		// 값이 업데이트-> 그 직후 다시 for루프 조건검사를 만족의 반복.
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
		t.timer.mu.Lock()
		defer t.timer.mu.Unlock()
		for i, ticker := range t.timer.tickers {
			if ticker == t {
				t.timer.tickers = append(t.timer.tickers[:i], t.timer.tickers[i+1:]...)
				break
			}
		}
	}
}
