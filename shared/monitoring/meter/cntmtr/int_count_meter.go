package cntmtr

import (
	"sync/atomic"
)

// IntCountMeterDesign 인터페이스 정의
type IntCountMeterDesign interface {
	Increase()
	Increases(u uint)
	Decrease()
	Decreases(u uint)
	TotalSum() int64
}

// IntCountMeter는 스레드 안전한 정수 카운터입니다
type IntCountMeter struct {
	value int64
}

// NewIntCountMeter는 새로운 IntCountMeter를 생성합니다
func NewIntCountMeter() *IntCountMeter {
	return &IntCountMeter{value: 0}
}

// NewIntCountMeterWithValue는 초기값을 가진 IntCountMeter를 생성합니다
func NewIntCountMeterWithValue(initialValue int64) *IntCountMeter {
	return &IntCountMeter{value: initialValue}
}

// Increase는 카운터를 1 증가시킵니다
func (icm *IntCountMeter) Increase() {
	atomic.AddInt64(&icm.value, 1)
}

// Increases는 카운터를 지정된 양만큼 증가시킵니다
func (icm *IntCountMeter) Increases(u uint) {
	if u > 0 {
		atomic.AddInt64(&icm.value, int64(u))
	}
}

// Decrease는 카운터를 1 감소시킵니다
func (icm *IntCountMeter) Decrease() {
	atomic.AddInt64(&icm.value, -1)
}

// Decreases는 카운터를 지정된 양만큼 감소시킵니다
func (icm *IntCountMeter) Decreases(u uint) {
	if u > 0 {
		atomic.AddInt64(&icm.value, -int64(u))
	}
}

// TotalSum은 현재 카운터 값을 반환합니다
func (icm *IntCountMeter) TotalSum() int64 {
	return atomic.LoadInt64(&icm.value)
}

// Reset은 카운터를 0으로 리셋합니다
func (icm *IntCountMeter) Reset() {
	atomic.StoreInt64(&icm.value, 0)
}

// Set은 카운터를 특정 값으로 설정합니다
func (icm *IntCountMeter) Set(value int64) {
	atomic.StoreInt64(&icm.value, value)
}

// CompareAndSwap은 현재 값이 old와 같으면 new로 설정합니다
func (icm *IntCountMeter) CompareAndSwap(old, new int64) bool {
	return atomic.CompareAndSwapInt64(&icm.value, old, new)
}
