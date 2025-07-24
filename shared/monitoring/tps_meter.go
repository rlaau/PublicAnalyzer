package monitoring

import (
	"sync/atomic"
	"time"
)

// SimpleTPSMeter 원자적 카운터 기반 간단한 TPS 측정기
type SimpleTPSMeter struct {
	// 원자적 카운터들
	currentCounter int64 // 현재 1초 동안의 카운터
	totalCounter   int64 // 전체 누적 카운터
	
	// 현재 TPS (마지막 1초 동안의 처리량)
	currentTPS int64 // atomic으로 관리
	
	// 시작 시간
	startTime time.Time
	
	// Ticker 제어
	ticker  *time.Ticker
	stopCh  chan struct{}
	running int32 // atomic flag
}

// NewSimpleTPSMeter 새로운 간단한 TPS 측정기 생성
func NewSimpleTPSMeter() TPSMeter {
	meter := &SimpleTPSMeter{
		startTime: time.Now(),
		ticker:    time.NewTicker(1 * time.Second), // 1초마다 tick
		stopCh:    make(chan struct{}),
	}
	
	// 1초마다 TPS 계산하는 고루틴 시작
	atomic.StoreInt32(&meter.running, 1)
	go meter.tickerRoutine()
	
	return meter
}

// RecordEvent 이벤트 발생 기록 (워커가 호출)
func (m *SimpleTPSMeter) RecordEvent() {
	atomic.AddInt64(&m.currentCounter, 1)
	atomic.AddInt64(&m.totalCounter, 1)
}

// RecordEvents 여러 이벤트를 한 번에 기록
func (m *SimpleTPSMeter) RecordEvents(count int) {
	if count <= 0 {
		return
	}
	atomic.AddInt64(&m.currentCounter, int64(count))
	atomic.AddInt64(&m.totalCounter, int64(count))
}

// GetCurrentTPS 현재 TPS 반환 (마지막 1초 동안의 처리량)
func (m *SimpleTPSMeter) GetCurrentTPS() float64 {
	return float64(atomic.LoadInt64(&m.currentTPS))
}

// GetAverageTPS 전체 평균 TPS 반환
func (m *SimpleTPSMeter) GetAverageTPS() float64 {
	totalEvents := atomic.LoadInt64(&m.totalCounter)
	if totalEvents == 0 {
		return 0.0
	}
	
	duration := time.Since(m.startTime).Seconds()
	if duration <= 0 {
		return 0.0
	}
	
	return float64(totalEvents) / duration
}

// GetTotalEvents 총 이벤트 수 반환
func (m *SimpleTPSMeter) GetTotalEvents() int64 {
	return atomic.LoadInt64(&m.totalCounter)
}

// Reset 통계 초기화
func (m *SimpleTPSMeter) Reset() {
	atomic.StoreInt64(&m.currentCounter, 0)
	atomic.StoreInt64(&m.totalCounter, 0)
	atomic.StoreInt64(&m.currentTPS, 0)
	m.startTime = time.Now()
}

// Close 리소스 정리
func (m *SimpleTPSMeter) Close() {
	if atomic.CompareAndSwapInt32(&m.running, 1, 0) {
		m.ticker.Stop()
		close(m.stopCh)
	}
}

// tickerRoutine 1초마다 TPS 계산하는 고루틴
func (m *SimpleTPSMeter) tickerRoutine() {
	for {
		select {
		case <-m.ticker.C:
			// 현재 카운터를 TPS로 설정하고 카운터 리셋
			currentCount := atomic.SwapInt64(&m.currentCounter, 0)
			atomic.StoreInt64(&m.currentTPS, currentCount)
			
		case <-m.stopCh:
			return
		}
	}
}

// GetStats 현재 통계 정보 반환
func (m *SimpleTPSMeter) GetStats(component string) MonitoringStats {
	return MonitoringStats{
		Timestamp:     time.Now(),
		CurrentTPS:    m.GetCurrentTPS(),
		AverageTPS:    m.GetAverageTPS(),
		TotalEvents:   m.GetTotalEvents(),
		WindowSeconds: 1.0, // 1초 윈도우
		Component:     component,
	}
}