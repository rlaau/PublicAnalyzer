package tps

// TPSMeter TPS 측정을 위한 인터페이스
type TPSMeter interface {
	// RecordTpsEvent 이벤트 발생 기록 (thread-safe)
	RecordTpsEvent()

	// RecordTpsEvents 여러 이벤트를 한 번에 기록 (배치 처리)
	RecordTpsEvents(count int)

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
