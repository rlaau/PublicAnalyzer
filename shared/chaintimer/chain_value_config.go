package chaintimer

import "time"

const (
	EhtTimeFormat = time.RFC3339
)

// ChainDuration 상수들 (time 패키지와 동일)
const (
	Nanosecond  ChainDuration = ChainDuration(time.Nanosecond)
	Microsecond ChainDuration = ChainDuration(time.Microsecond)
	Millisecond ChainDuration = ChainDuration(time.Millisecond)
	Second      ChainDuration = ChainDuration(time.Second)
	Minute      ChainDuration = ChainDuration(time.Minute)
	Hour        ChainDuration = ChainDuration(time.Hour)
)
