// file: internal/apool/rel/triplet/infra/timebucket_manager.go
package infra

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// === 파라미터 ===
const (
	WindowSize      = 4 * 30 * 24 * chaintimer.Hour // 4개월
	SlideInterval   = 7 * 24 * chaintimer.Hour      // 1주
	TriggerInterval = 7 * 24 * chaintimer.Hour
	MaxTimeBuckets  = 21
)

// ---- TimeBucket ----
type TimeBucket struct {
	StartTime chaintimer.ChainTime
	EndTime   chaintimer.ChainTime
	ToUsers   map[domain.Address]chaintimer.ChainTime // to -> first_active_time
}

func newTimeBucket(start chaintimer.ChainTime) *TimeBucket {
	return &TimeBucket{
		StartTime: start,
		EndTime:   start.Add(SlideInterval),
		ToUsers:   make(map[domain.Address]chaintimer.ChainTime),
	}
}

// ---- TimeBucketManager ----
// - 단일 뮤텍스 기반(단순/안전)
// - 회전 시 OnRotate(old) 콜백은 락 밖에서 호출
// - 중간 저장 없음: Close() 때만 Save()
// - 스냅샷 파일 1개(state.bin)만 사용(백업 로그 없음)
type TimeBucketManager struct {
	path string // 디렉토리. ""면 디스크 사용 안 함.

	buckets    []*TimeBucket
	frontIndex int
	rearIndex  int
	count      int

	mu    sync.RWMutex
	dirty bool // 변경 플래그(변경이 있으면 Close 시 Save)

	// 오래된 버킷이 밀려나기 직전에 제공(락 밖에서 호출됨)
	OnRotate func(old *TimeBucket)
}

func NewTimeBucketManager(path string) (*TimeBucketManager, error) {
	m := &TimeBucketManager{
		path:    filepath.Clean(path),
		buckets: make([]*TimeBucket, MaxTimeBuckets),
	}
	if path == "" {
		return m, nil
	}
	if err := os.MkdirAll(m.path, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir buckets path: %w", err)
	}
	_ = m.Load() // 있으면 로드(없으면 no-op)
	return m, nil
}

// Close: 변경이 있었으면 스냅샷 저장(없으면 no-op)
func (m *TimeBucketManager) Close() error {
	if m == nil || m.path == "" {
		return nil
	}
	m.mu.RLock()
	dirty := m.dirty
	m.mu.RUnlock()
	if !dirty {
		return nil
	}
	return m.Save()
}

// == 공개 API ==

// AddIfAbsent: 항상 먼저 시간 버킷을 보장(생성/회전) → 그 다음 중복이면 추가 생략
func (m *TimeBucketManager) AddIfAbsent(to domain.Address, txTime chaintimer.ChainTime) (bool, int) {
	var rotateOld *TimeBucket
	m.mu.Lock()
	idx, rotated := m.ensureBucketLocked(txTime)
	if rotated != nil {
		rotateOld = rotated // 락 밖에서 콜백 처리
	}
	// 이미 윈도에 존재?
	if m.existsLocked(to) {
		m.mu.Unlock()
		if m.OnRotate != nil && rotateOld != nil {
			m.OnRotate(rotateOld)
		}
		return false, idx
	}
	// 현재 버킷에 추가
	b := m.buckets[idx]
	if b == nil {
		b = newTimeBucket(m.calculateWeekStart(txTime)) // 방어적
		m.buckets[idx] = b
	}
	b.ToUsers[to] = txTime
	m.dirty = true
	m.mu.Unlock()

	if m.OnRotate != nil && rotateOld != nil {
		m.OnRotate(rotateOld)
	}
	return true, idx
}

// DeleteToUser: 최신→과거 방향으로 첫 매칭에서 제거
func (m *TimeBucketManager) DeleteToUser(to domain.Address) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.count == 0 {
		return false
	}
	for i := 0; i < m.count; i++ {
		idx := (m.rearIndex - i + MaxTimeBuckets) % MaxTimeBuckets
		b := m.buckets[idx]
		if b == nil {
			continue
		}
		if _, ok := b.ToUsers[to]; ok {
			delete(b.ToUsers, to)
			m.dirty = true
			return true
		}
	}
	return false
}

func (m *TimeBucketManager) Exists(to domain.Address) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.existsLocked(to)
}

func (m *TimeBucketManager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

func (m *TimeBucketManager) TotalToUsers() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sum := 0
	for i := 0; i < MaxTimeBuckets; i++ {
		if b := m.buckets[i]; b != nil {
			sum += len(b.ToUsers)
		}
	}
	return sum
}

// == 내부 로직 ==

func (m *TimeBucketManager) calculateWeekStart(t chaintimer.ChainTime) chaintimer.ChainTime {
	year, month, day := t.Date()
	weekday := t.Weekday() // Sunday=0
	daysToSubtract := int(weekday)
	start := chaintimer.ChainDate(year, month, day-daysToSubtract, 0, 0, 0, 0, t.Location())
	return chaintimer.ChainTime(start)
}

// ensureBucketLocked: txTime이 속한 버킷 인덱스를 보장(존재/신규/회전).
// 호출 전 m.mu가 Lock 상태여야 함.
// 반환: (index, rotatedOld) — rotatedOld != nil이면 회전이 발생했고 콜백 인자.
func (m *TimeBucketManager) ensureBucketLocked(txTime chaintimer.ChainTime) (int, *TimeBucket) {
	// 초기: 첫 버킷 생성
	if m.count == 0 {
		ws := m.calculateWeekStart(txTime)
		m.buckets[0] = newTimeBucket(ws)
		m.frontIndex, m.rearIndex, m.count = 0, 0, 1
		m.dirty = true
		return 0, nil
	}
	// 기존 버킷 검색(반닫힌 구간 [Start, End))
	for i := 0; i < m.count; i++ {
		idx := (m.rearIndex - i + MaxTimeBuckets) % MaxTimeBuckets
		b := m.buckets[idx]
		if b == nil {
			continue
		}
		if !txTime.Before(b.StartTime) && txTime.Before(b.EndTime) {
			return idx, nil
		}
	}
	// 새 버킷 필요
	ws := m.calculateWeekStart(txTime)
	// 여유 있음 → rear 뒤에 추가
	if m.count < MaxTimeBuckets {
		newRear := (m.rearIndex + 1) % MaxTimeBuckets
		m.buckets[newRear] = newTimeBucket(ws)
		m.rearIndex = newRear
		m.count++
		m.dirty = true
		return newRear, nil
	}
	// 가득 참 → front 로테이션
	old := m.buckets[m.frontIndex]
	newIdx := m.frontIndex
	m.buckets[newIdx] = newTimeBucket(ws)
	m.frontIndex = (m.frontIndex + 1) % MaxTimeBuckets
	m.rearIndex = newIdx
	// count는 유지
	m.dirty = true
	return newIdx, old
}

func (m *TimeBucketManager) existsLocked(to domain.Address) bool {
	if m.count == 0 {
		return false
	}
	for i := 0; i < m.count; i++ {
		idx := (m.rearIndex - i + MaxTimeBuckets) % MaxTimeBuckets
		b := m.buckets[idx]
		if b == nil {
			continue
		}
		if _, ok := b.ToUsers[to]; ok {
			return true
		}
	}
	return false
}

// ----------------------
// 바이너리 스냅샷 (state.bin)
// ----------------------

const tbMagicStr = "TBKTv1" // 6 bytes

func (m *TimeBucketManager) stateBinPath() string {
	if m.path == "" {
		return ""
	}
	return filepath.Join(m.path, "state.bin")
}

// Save: 전체 스냅샷을 compact 바이너리로 저장(원자적 rename).
// 호출자는 보통 Close()에서만 부름.
func (m *TimeBucketManager) Save() error {
	if m.path == "" {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	var buf bytes.Buffer
	// header
	buf.WriteString(tbMagicStr)
	if err := buf.WriteByte(byte(m.count)); err != nil {
		return err
	}

	// front..rear 순서로 직렬화 → Load 후 front=0, rear=count-1로 재구성
	for i := 0; i < m.count; i++ {
		idx := (m.frontIndex + i) % MaxTimeBuckets
		b := m.buckets[idx]
		if b == nil {
			continue
		}
		// start_unix(int64)
		startUnix := time.Time(b.StartTime).UTC().Unix()
		if err := binary.Write(&buf, binary.LittleEndian, startUnix); err != nil {
			return err
		}
		// users_len(uint32)
		uLen := uint32(len(b.ToUsers))
		if err := binary.Write(&buf, binary.LittleEndian, uLen); err != nil {
			return err
		}
		// users: [20]byte + int64
		for addr, t := range b.ToUsers {
			if _, err := buf.Write(addr[:]); err != nil {
				return err
			}
			ft := time.Time(t).UTC().Unix()
			if err := binary.Write(&buf, binary.LittleEndian, ft); err != nil {
				return err
			}
		}
	}

	tmp := m.stateBinPath() + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, m.stateBinPath()); err != nil {
		return err
	}

	// 저장 완료 → 더티 플래그 클리어
	// (RLock 상태에서 값을 바꾸진 않음; Close 전에만 호출하므로 실용적으로 OK)
	return nil
}

// Load: 바이너리 스냅샷 로드(없으면 no-op)
func (m *TimeBucketManager) Load() error {
	if m.path == "" {
		return nil
	}
	data, err := os.ReadFile(m.stateBinPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	r := bytes.NewReader(data)

	// header
	hdr := make([]byte, len(tbMagicStr))
	if _, err := io.ReadFull(r, hdr); err != nil {
		return err
	}
	if string(hdr) != tbMagicStr {
		return fmt.Errorf("invalid magic: %q", string(hdr))
	}
	var cb [1]byte
	if _, err := io.ReadFull(r, cb[:]); err != nil {
		return err
	}
	cnt := int(cb[0])
	if cnt < 0 || cnt > MaxTimeBuckets {
		return fmt.Errorf("invalid bucket count: %d", cnt)
	}

	newBuckets := make([]*TimeBucket, MaxTimeBuckets)

	for i := 0; i < cnt; i++ {
		var startUnix int64
		if err := binary.Read(r, binary.LittleEndian, &startUnix); err != nil {
			return fmt.Errorf("read start_unix: %w", err)
		}
		var usersLen uint32
		if err := binary.Read(r, binary.LittleEndian, &usersLen); err != nil {
			return fmt.Errorf("read users_len: %w", err)
		}

		st := chaintimer.ChainTime(time.Unix(startUnix, 0).UTC())
		tb := &TimeBucket{
			StartTime: st,
			EndTime:   st.Add(SlideInterval),
			ToUsers:   make(map[domain.Address]chaintimer.ChainTime, usersLen),
		}

		for j := uint32(0); j < usersLen; j++ {
			var a domain.Address
			if _, err := io.ReadFull(r, a[:]); err != nil {
				return fmt.Errorf("read address: %w", err)
			}
			var ft int64
			if err := binary.Read(r, binary.LittleEndian, &ft); err != nil {
				return fmt.Errorf("read first_time: %w", err)
			}
			tb.ToUsers[a] = chaintimer.ChainTime(time.Unix(ft, 0).UTC())
		}
		newBuckets[i] = tb
	}

	// 스왑
	m.mu.Lock()
	m.buckets = newBuckets
	m.frontIndex = 0
	if cnt > 0 {
		m.rearIndex = cnt - 1
	} else {
		m.rearIndex = 0
	}
	m.count = cnt
	m.dirty = false // 로드 직후는 디스크 상태와 동일
	m.mu.Unlock()

	return nil
}
