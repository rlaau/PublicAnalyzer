package kfkmntr

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/meter/tpsmtr"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/monitor"
)

// * TPS는 리얼 타임 기반 시스템 상태를 측정하므로, time.Time을 사용한다.

// KafkaStats Kafka 전용 통계 정보
type KafkaStats struct {
	Timestamp     time.Time `json:"timestamp"`
	ProducerTPS   float64   `json:"producer_tps"`
	ConsumerTPS   float64   `json:"consumer_tps"`
	ProducerTotal int64     `json:"producer_total"`
	ConsumerTotal int64     `json:"consumer_total"`
}

// TpsStats 모니터링 통계 정보
type TpsStats struct {
	Timestamp     time.Time `json:"timestamp"`
	CurrentTPS    float64   `json:"current_tps"`
	AverageTPS    float64   `json:"average_tps"`
	TotalEvents   int64     `json:"total_events"`
	WindowSeconds float64   `json:"window_seconds"`
	Component     string    `json:"component"` // "producer", "consumer", etc.
}

// KafkaMonitor Kafka Producer/Consumer TPS 모니터링
type KafkaMonitor struct {
	ProducerTPS tpsmtr.TPSMeter // Producer TPS 측정기
	ConsumerTPS tpsmtr.TPSMeter // Consumer TPS 측정기

	// TPS 리포팅 설정
	tpsReportInterval time.Duration
	tpsReportCallback monitor.ReportCallback[TpsStats]

	// 리포팅 제어
	reportTicker *time.Ticker
	reportStopCh chan struct{}
	reporting    int32 // atomic flag
}

// NewKafkaMonitor 새로운 Kafka 모니터 생성
func NewKafkaMonitor() *KafkaMonitor {
	return &KafkaMonitor{
		ProducerTPS:  tpsmtr.NewSafeTPSMeter(),
		ConsumerTPS:  tpsmtr.NewSafeTPSMeter(),
		reportStopCh: make(chan struct{}),
	}
}

// RecordProducerTpsEvent Producer 이벤트 기록 (편의 메서드)
func (km *KafkaMonitor) RecordProducerTpsEvent() {
	km.ProducerTPS.RecordTpsEvent()
}

// RecordProducerTpsEvents Producer 배치 이벤트 기록
func (km *KafkaMonitor) RecordProducerTpsEvents(count int) {
	km.ProducerTPS.RecordTpsEvents(count)
}

// RecordConsumerTpsEvent Consumer 이벤트 기록 (편의 메서드)
func (km *KafkaMonitor) RecordConsumerTpsEvent() {
	km.ConsumerTPS.RecordTpsEvent()
}

// RecordConsumerTpsEvents Consumer 배치 이벤트 기록
func (km *KafkaMonitor) RecordConsumerTpsEvents(count int) {
	km.ConsumerTPS.RecordTpsEvents(count)
}

func (km *KafkaMonitor) GetProducerTPS() float64 {
	return km.ProducerTPS.GetCurrentTPS()
}
func (km *KafkaMonitor) GetConsumerTPS() float64 {
	return km.ConsumerTPS.GetCurrentTPS()
}

// StartReporting 주기적 리포팅 시작
func (km *KafkaMonitor) StartReporting(interval time.Duration) {
	if atomic.CompareAndSwapInt32(&km.reporting, 0, 1) {
		//여러 인터벌이 있다고 가정한 상태에서, 현재는 TPS만 작동시킴
		km.tpsReportInterval = interval
		km.reportTicker = time.NewTicker(interval)
		go km.reportingRoutine()
		log.Printf("[KafkaMonitor] Started reporting every %v", interval)
	}
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
	if km.tpsReportCallback != nil {
		// Producer 통계
		producerStats := TpsStats{
			Timestamp:     time.Now(),
			CurrentTPS:    km.ProducerTPS.GetCurrentTPS(),
			AverageTPS:    km.ProducerTPS.GetAverageTPS(),
			TotalEvents:   km.ProducerTPS.GetTotalEvents(),
			WindowSeconds: 1.0,
			Component:     "producer",
		}
		km.tpsReportCallback(producerStats)

		// Consumer 통계
		consumerStats := TpsStats{
			Timestamp:     time.Now(),
			CurrentTPS:    km.ConsumerTPS.GetCurrentTPS(),
			AverageTPS:    km.ConsumerTPS.GetAverageTPS(),
			TotalEvents:   km.ConsumerTPS.GetTotalEvents(),
			WindowSeconds: 1.0,
			Component:     "consumer",
		}
		km.tpsReportCallback(consumerStats)
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
func (km *KafkaMonitor) SetCallback(callback monitor.ReportCallback[TpsStats]) {
	km.tpsReportCallback = callback
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
