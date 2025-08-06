package monitor

import "time"

// * 모니터링은 "정말로 1초마다" 모니터링 하므로, time.Time을 사용한다
type Monitor interface {
	// StartReporting 주기적 리포팅 시작
	StartReporting(interval time.Duration)

	// StopReporting 리포팅 중지
	StopReporting()

	// SetCallback 리포팅 콜백 설정
	SetCallback(callback func(anyStats any))
}

type ReportCallback[Stats any] func(stats Stats)
