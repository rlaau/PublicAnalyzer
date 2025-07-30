package meter

import (
	"sync"
	"sync/atomic"
)

// CountMeter는 OK/ERR/기타 식별자별 카운팅을 위한 인터페이스
type CountMeter interface {
	AddErrCount()
	AddErrCounts(i int64)
	ErrCount() int64

	AddOkCount()
	AddOkCounts(i int64)
	OkCount() int64

	AddCount(s string)
	AddCounts(s string, i int64)
}

// ThreadSafeCountMeter는 병렬 안전한 카운팅 메터
type ThreadSafeCountMeter struct {
	okCount    int64
	errCount   int64
	counterMu  sync.RWMutex
	counterMap map[string]int64
}

// NewParallelSafeMeter 생성자
func NewParallelSafeMeter() *ThreadSafeCountMeter {
	return &ThreadSafeCountMeter{
		counterMap: make(map[string]int64),
	}
}

// AddOkCount는 OK 카운트를 1 증가시킴
func (m *ThreadSafeCountMeter) AddOkCount() {
	atomic.AddInt64(&m.okCount, 1)
}

// AddOkCounts는 OK 카운트를 지정량만큼 증가시킴
func (m *ThreadSafeCountMeter) AddOkCounts(i int64) {
	atomic.AddInt64(&m.okCount, i)
}

// AddErrCount는 ERR 카운트를 1 증가시킴
func (m *ThreadSafeCountMeter) AddErrCount() {
	atomic.AddInt64(&m.errCount, 1)
}

// AddErrCounts는 ERR 카운트를 지정량만큼 증가시킴
func (m *ThreadSafeCountMeter) AddErrCounts(i int64) {
	atomic.AddInt64(&m.errCount, i)
}

// AddThisCount는 주어진 식별자 카운트를 1 증가시킴
func (m *ThreadSafeCountMeter) AddCount(s string) {
	m.AddCounts(s, 1)
}

// AddThisCounts는 주어진 식별자 카운트를 지정량만큼 증가시킴
func (m *ThreadSafeCountMeter) AddCounts(s string, i int64) {
	m.counterMu.Lock()
	defer m.counterMu.Unlock()
	m.counterMap[s] += i
}
func (m *ThreadSafeCountMeter) OkCount() int64 {
	return atomic.LoadInt64(&m.okCount)
}

func (m *ThreadSafeCountMeter) ErrCount() int64 {
	return atomic.LoadInt64(&m.errCount)
}

func (m *ThreadSafeCountMeter) GetCount(key string) int64 {
	m.counterMu.RLock()
	defer m.counterMu.RUnlock()
	return m.counterMap[key]
}

// GetAll은 현재까지의 전체 식별자별 카운트를 복사해 반환합니다.
// 병렬 안전을 위해 내부 맵을 깊은 복사합니다.
func (m *ThreadSafeCountMeter) GetAll() map[string]int64 {
	m.counterMu.RLock()
	defer m.counterMu.RUnlock()

	copyMap := make(map[string]int64, len(m.counterMap))
	for k, v := range m.counterMap {
		copyMap[k] = v
	}
	return copyMap
}
