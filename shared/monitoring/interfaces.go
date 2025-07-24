package monitoring

import "time"

// TPSMeter TPS 측정을 위한 인터페이스
type TPSMeter interface {
	// RecordEvent 이벤트 발생 기록 (thread-safe)
	RecordEvent()
	
	// RecordEvents 여러 이벤트를 한 번에 기록 (배치 처리)
	RecordEvents(count int)
	
	// GetCurrentTPS 현재 TPS 반환 (5초 슬라이딩 윈도우)
	GetCurrentTPS() float64
	
	// GetAverageTPS 전체 평균 TPS 반환
	GetAverageTPS() float64
	
	// GetTotalEvents 총 이벤트 수 반환
	GetTotalEvents() int64
	
	// Reset 통계 초기화
	Reset()
	
	// Close 리소스 정리
	Close()
}

// Reporter 주기적 보고를 위한 인터페이스
type Reporter interface {
	// StartReporting 주기적 리포팅 시작
	StartReporting(interval time.Duration)
	
	// StopReporting 리포팅 중지
	StopReporting()
	
	// SetCallback 리포팅 콜백 설정
	SetCallback(callback ReportCallback)
}

// ReportCallback 리포팅 콜백 함수 타입
type ReportCallback func(stats MonitoringStats)

// MonitoringStats 모니터링 통계 정보
type MonitoringStats struct {
	Timestamp     time.Time `json:"timestamp"`
	CurrentTPS    float64   `json:"current_tps"`
	AverageTPS    float64   `json:"average_tps"`
	TotalEvents   int64     `json:"total_events"`
	WindowSeconds float64   `json:"window_seconds"`
	Component     string    `json:"component"` // "producer", "consumer", etc.
}

// KafkaStats Kafka 전용 통계 정보
type KafkaStats struct {
	Timestamp    time.Time `json:"timestamp"`
	ProducerTPS  float64   `json:"producer_tps"`
	ConsumerTPS  float64   `json:"consumer_tps"`
	ProducerTotal int64    `json:"producer_total"`
	ConsumerTotal int64    `json:"consumer_total"`
}