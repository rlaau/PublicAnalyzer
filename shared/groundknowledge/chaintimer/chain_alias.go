package chaintimer

import (
	"fmt"
	"time"
)

// ChainTime은 time.ChainTime을 감싼 alias임
// 체인의 시간을 이산적으로 받아들이는 시간 타입.
// 모든 기능은 time.ChainTime과 같지만, 타입 구분 통한 안정성 확보 위한 타입임
// 그러므로 이 파일 코드를 굳이 읽을 필요는 없음. 그냥 alias이후 대체 위해서 time.ChainTime래핑한게 전부임.
type ChainTime time.Time

type ChainMonth time.Month

type ChainWeekday time.Weekday

// Time은 내부 time.Time 값을 반환합니다
func (ct ChainTime) Time() time.Time {
	return time.Time(ct)
}

func (ct ChainTime) Unix() int64 {
	return ct.Time().Unix()
}

// ChainDuration은 블록체인 시간 간격을 나타내는 타입입니다
// 얘도 걍  alias임
type ChainDuration time.Duration

// Duration은 내부 time.Duration 값을 반환합니다
func (d ChainDuration) Duration() time.Duration {
	return time.Duration(d)
}

// Add는 duration을 더한 새로운 ChainTime을 반환합니다
func (ct ChainTime) Add(d ChainDuration) ChainTime {
	return ChainTime(ct.Time().Add(d.Duration()))
}

// Sub는 두 ChainTime 간의 duration을 반환합니다
func (ct ChainTime) Sub(u ChainTime) ChainDuration {
	return ChainDuration(ct.Time().Sub(u.Time()))
}

// Before는 ct가 u보다 이전인지 확인합니다
func (ct ChainTime) Before(u ChainTime) bool {
	return ct.Time().Before(time.Time(u))
}

// After는 ct가 u보다 이후인지 확인합니다
func (ct ChainTime) After(u ChainTime) bool {
	return ct.Time().After(time.Time(u))
}

// Equal은 두 ChainTime이 같은지 확인합니다
func (ct ChainTime) Equal(u ChainTime) bool {
	return time.Time(ct).Equal(time.Time(u))
}

// IsZero는 ChainTime이 zero value인지 확인합니다
func (ct ChainTime) IsZero() bool {
	return time.Time(ct).IsZero()
}

// Format은 시간을 지정된 형식으로 포맷합니다
func (ct ChainTime) Format(layout string) string {
	return time.Time(ct).Format(layout)
}

func (ct ChainTime) UTC() ChainTime {
	return ChainTime(ct.Time().UTC())
}

// String은 기본 문자열 표현을 반환합니다
func (ct ChainTime) String() string {
	return time.Time(ct).String()
}

// UnixNano는 Unix timestamp를 나노초 단위로 반환합니다
func (ct ChainTime) UnixNano() int64 {
	return time.Time(ct).UnixNano()
}

func (ct ChainTime) Date() (int, ChainMonth, int) {
	year, month, day := time.Time(ct).Date()
	return year, ChainMonth(month), day
}

func (ct ChainTime) Weekday() ChainWeekday {
	return ChainWeekday(time.Time(ct).Weekday())
}

func (ct ChainTime) Location() *time.Location {
	return time.Time(ct).Location()
}

// Seconds는 duration을 초 단위로 반환합니다
func (d ChainDuration) Seconds() float64 {
	return time.Duration(d).Seconds()
}

// Minutes는 duration을 분 단위로 반환합니다
func (d ChainDuration) Minutes() float64 {
	return time.Duration(d).Minutes()
}

// Hours는 duration을 시간 단위로 반환합니다
func (d ChainDuration) Hours() float64 {
	return time.Duration(d).Hours()
}

// Milliseconds는 duration을 밀리초 단위로 반환합니다
func (d ChainDuration) Milliseconds() int64 {
	return time.Duration(d).Milliseconds()
}

// Microseconds는 duration을 마이크로초 단위로 반환합니다
func (d ChainDuration) Microseconds() int64 {
	return time.Duration(d).Microseconds()
}

// Nanoseconds는 duration을 나노초 단위로 반환합니다
func (d ChainDuration) Nanoseconds() int64 {
	return time.Duration(d).Nanoseconds()
}

// String은 duration의 문자열 표현을 반환합니다
func (d ChainDuration) String() string {
	return time.Duration(d).String()
}

// Truncate는 m의 배수로 내림한 duration을 반환합니다
func (d ChainDuration) Truncate(m ChainDuration) ChainDuration {
	return ChainDuration(time.Duration(d).Truncate(time.Duration(m)))
}

// Round는 m의 배수로 반올림한 duration을 반환합니다
func (d ChainDuration) Round(m ChainDuration) ChainDuration {
	return ChainDuration(time.Duration(d).Round(time.Duration(m)))
}

// Abs는 duration의 절대값을 반환합니다
func (d ChainDuration) Abs() ChainDuration {
	if d < 0 {
		return -d
	}
	return d
}

// Add는 두 ChainDuration을 더합니다
func (d ChainDuration) Add(other ChainDuration) ChainDuration {
	return ChainDuration(time.Duration(d) + time.Duration(other))
}

// Sub는 두 ChainDuration을 뺍니다
func (d ChainDuration) Sub(other ChainDuration) ChainDuration {
	return ChainDuration(time.Duration(d) - time.Duration(other))
}

// Mul는 ChainDuration에 정수를 곱합니다
func (d ChainDuration) Mul(n int64) ChainDuration {
	return ChainDuration(time.Duration(d) * time.Duration(n))
}

// Div는 ChainDuration을 정수로 나눕니다
func (d ChainDuration) Div(n int64) ChainDuration {
	return ChainDuration(time.Duration(d) / time.Duration(n))
}

// DivDuration는 두 ChainDuration을 나누어 비율을 반환합니다
func (d ChainDuration) DivDuration(other ChainDuration) float64 {
	return float64(d) / float64(other)
}

// IsZero는 duration이 0인지 확인합니다
func (d ChainDuration) IsZero() bool {
	return d == 0
}

// IsNegative는 duration이 음수인지 확인합니다
func (d ChainDuration) IsNegative() bool {
	return d < 0
}

// IsPositive는 duration이 양수인지 확인합니다
func (d ChainDuration) IsPositive() bool {
	return d > 0
}

// Compare는 두 ChainDuration을 비교합니다
// d < other이면 -1, d == other이면 0, d > other이면 1을 반환합니다
func (d ChainDuration) Compare(other ChainDuration) int {
	if d < other {
		return -1
	}
	if d > other {
		return 1
	}
	return 0
}

// NewChainDuration은 time.Duration으로부터 ChainDuration을 생성합니다
func NewChainDuration(d time.Duration) ChainDuration {
	return ChainDuration(d)
}

// ParseChainDuration은 문자열을 파싱하여 ChainDuration을 생성합니다
func ParseChainDuration(s string) (ChainDuration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid chain duration: %w", err)
	}
	return ChainDuration(d), nil
}
