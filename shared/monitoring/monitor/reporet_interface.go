package monitor

import "time"

type Monitor interface {
	// StartReporting 주기적 리포팅 시작
	StartReporting(interval time.Duration)

	// StopReporting 리포팅 중지
	StopReporting()

	// SetCallback 리포팅 콜백 설정
	SetCallback(callback func(anyStats any))
}

type ReportCallback[Stats any] func(stats Stats)
