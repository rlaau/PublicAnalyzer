package monitoring

import (
	"log"
	"sync/atomic"
	"time"
)

// KafkaMonitor Kafka Producer/Consumer TPS 모니터링
type KafkaMonitor struct {
	ProducerTPS TPSMeter // Producer TPS 측정기
	ConsumerTPS TPSMeter // Consumer TPS 측정기

	// 리포팅 설정
	reportInterval time.Duration
	reportCallback ReportCallback

	// 리포팅 제어
	reportTicker *time.Ticker
	reportStopCh chan struct{}
	reporting    int32 // atomic flag
}

// NewKafkaMonitor 새로운 Kafka 모니터 생성
func NewKafkaMonitor() *KafkaMonitor {
	return &KafkaMonitor{
		ProducerTPS:  NewSimpleTPSMeter(),
		ConsumerTPS:  NewSimpleTPSMeter(),
		reportStopCh: make(chan struct{}),
	}
}

// StartReporting 주기적 리포팅 시작
func (km *KafkaMonitor) StartReporting(interval time.Duration) {
	if atomic.CompareAndSwapInt32(&km.reporting, 0, 1) {
		km.reportInterval = interval
		km.reportTicker = time.NewTicker(interval)
		go km.reportingRoutine()
		log.Printf("[KafkaMonitor] Started reporting every %v", interval)
	}
}

// StopReporting 리포팅 중지
func (km *KafkaMonitor) StopReporting() {
	if atomic.CompareAndSwapInt32(&km.reporting, 1, 0) {
		if km.reportTicker != nil {
			km.reportTicker.Stop()
		}
		close(km.reportStopCh)
		log.Printf("[KafkaMonitor] Stopped reporting")
	}
}

// SetCallback 리포팅 콜백 설정
func (km *KafkaMonitor) SetCallback(callback ReportCallback) {
	km.reportCallback = callback
}

// GetKafkaStats 현재 Kafka 통계 반환
func (km *KafkaMonitor) GetKafkaStats() KafkaStats {
	return KafkaStats{
		Timestamp:     time.Now(),
		ProducerTPS:   km.ProducerTPS.GetCurrentTPS(),
		ConsumerTPS:   km.ConsumerTPS.GetCurrentTPS(),
		ProducerTotal: km.ProducerTPS.GetTotalEvents(),
		ConsumerTotal: km.ConsumerTPS.GetTotalEvents(),
	}
}

// Close 모든 리소스 정리
func (km *KafkaMonitor) Close() {
	km.StopReporting()
	km.ProducerTPS.Close()
	km.ConsumerTPS.Close()
}

// reportingRoutine 주기적 리포팅 고루틴
func (km *KafkaMonitor) reportingRoutine() {
	for {
		select {
		case <-km.reportTicker.C:
			km.doReport()
		case <-km.reportStopCh:
			return
		}
	}
}

// doReport 실제 리포팅 수행
func (km *KafkaMonitor) doReport() {
	stats := km.GetKafkaStats()

	// 기본 로그 출력
	log.Printf("[KafkaMonitor] Producer TPS: %.2f (Total: %d), Consumer TPS: %.2f (Total: %d)",
		stats.ProducerTPS, stats.ProducerTotal,
		stats.ConsumerTPS, stats.ConsumerTotal)

	// 콜백이 설정되어 있으면 호출
	if km.reportCallback != nil {
		// Producer 통계
		producerStats := MonitoringStats{
			Timestamp:     time.Now(),
			CurrentTPS:    km.ProducerTPS.GetCurrentTPS(),
			AverageTPS:    km.ProducerTPS.GetAverageTPS(),
			TotalEvents:   km.ProducerTPS.GetTotalEvents(),
			WindowSeconds: 1.0,
			Component:     "producer",
		}
		km.reportCallback(producerStats)

		// Consumer 통계
		consumerStats := MonitoringStats{
			Timestamp:     time.Now(),
			CurrentTPS:    km.ConsumerTPS.GetCurrentTPS(),
			AverageTPS:    km.ConsumerTPS.GetAverageTPS(),
			TotalEvents:   km.ConsumerTPS.GetTotalEvents(),
			WindowSeconds: 1.0,
			Component:     "consumer",
		}
		km.reportCallback(consumerStats)
	}
}

// RecordProducerEvent Producer 이벤트 기록 (편의 메서드)
func (km *KafkaMonitor) RecordProducerEvent() {
	km.ProducerTPS.RecordEvent()
}

// RecordProducerEvents Producer 배치 이벤트 기록
func (km *KafkaMonitor) RecordProducerEvents(count int) {
	km.ProducerTPS.RecordEvents(count)
}

// RecordConsumerEvent Consumer 이벤트 기록 (편의 메서드)
func (km *KafkaMonitor) RecordConsumerEvent() {
	km.ConsumerTPS.RecordEvent()
}

// RecordConsumerEvents Consumer 배치 이벤트 기록
func (km *KafkaMonitor) RecordConsumerEvents(count int) {
	km.ConsumerTPS.RecordEvents(count)
}
