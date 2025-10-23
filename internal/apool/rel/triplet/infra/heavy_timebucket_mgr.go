// file: internal/apool/rel/triplet/infra/heavy_timebucket_mgr.go
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
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// HeavyTimeBucket - 메모리 기반으로 toAddress -> []FromScala 매핑을 포함
type HeavyTimeBucket struct {
	StartTime chaintimer.ChainTime
	EndTime   chaintimer.ChainTime
	// 핵심: toAddress -> []FromScala 매핑을 메모리에 저장
	ToUsers map[domain.Address][]FromScala
}

func newHeavyTimeBucket(start chaintimer.ChainTime) *HeavyTimeBucket {
	return &HeavyTimeBucket{
		StartTime: start,
		EndTime:   start.Add(SlideInterval),
		ToUsers:   make(map[domain.Address][]FromScala),
	}
}

// HeavyTimeBucketManager - TimeBucketManager + PendingRelationRepo 기능을 통합한 메모리 기반 매니저
// 단순화된 API: AddIfAbsent(to, fromScala), GetFromScalas(toAddr) 만 제공
type HeavyTimeBucketManager struct {
	path string // 디렉토리. ""면 디스크 사용 안 함.

	buckets    []*HeavyTimeBucket
	frontIndex int
	rearIndex  int
	count      int

	mu    sync.RWMutex
	dirty bool // 변경 플래그
}

func NewHeavyTimeBucketManager(mode mode.ProcessingMode) (*HeavyTimeBucketManager, error) {
	var path string
	if mode.IsTest() {
		path = filepath.Join(computation.DockerTestingStorage(), "triplet/timebucket")
	} else {
		path = filepath.Join(computation.DockerProductionStorage(), "triplet/timebucket")
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir buckets path: %w", err)
	}
	m := &HeavyTimeBucketManager{
		path:    filepath.Clean(path),
		buckets: make([]*HeavyTimeBucket, MaxTimeBuckets),
	}
	_ = m.Load() // 있으면 로드(없으면 no-op)
	return m, nil
}

// Close: 변경이 있었으면 스냅샷 저장(없으면 no-op)
func (m *HeavyTimeBucketManager) Close() error {
	if m == nil || m.path == "" {
		return nil
	}
	m.mu.RLock()
	dirty := m.dirty
	m.mu.RUnlock()
	fmt.Printf("HeavyTimeBUckManager Close() 호출: 더티마크:%v\n", dirty)
	if !dirty {
		return nil
	}
	return m.Save()
}

// == 공개 API (단순화됨) ==

// AddIfAbsent: to 주소에 fromScala를 추가 (방어적 추가)
// 해당 시간의 버킷이 없으면 생성하고, to 주소가 윈도우에 없으면 새로 추가
// fromAddress가 이미 있으면 업데이트, 없으면 새로 추가
// 반환: (added, bucketIndex) - added는 새로운 to 주소가 추가되었는지 여부
func (m *HeavyTimeBucketManager) AddIfAbsent(to domain.Address, fromScala FromScala) (bool, int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. 시간에 맞는 버킷 확보 (생성/회전)
	idx, _ := m.ensureBucketLocked(fromScala.LastTime)

	// 2. 현재 버킷 가져오기
	bucket := m.buckets[idx]
	if bucket == nil {
		bucket = newHeavyTimeBucket(m.calculateWeekStart(fromScala.LastTime))
		m.buckets[idx] = bucket
	}

	// 3. to 주소가 이미 존재하는지 확인
	fromScalas, exists := bucket.ToUsers[to]
	newToUser := !exists

	if !exists {
		// 새로운 to 주소: 빈 슬라이스로 초기화
		fromScalas = make([]FromScala, 0, 1)
	}

	// 4. 동일한 FromAddress가 있는지 찾기
	found := false
	for i := range fromScalas {
		if fromScalas[i].FromAddress == fromScala.FromAddress {
			// 업데이트: LastTxId, LastTime 갱신, Volume 증가
			fromScalas[i].LastTxId = fromScala.LastTxId
			fromScalas[i].LastTime = fromScala.LastTime
			if fromScalas[i].Volume < 2147483647 { // math.MaxInt32
				fromScalas[i].Volume++
			}
			if !fromScala.ValueOrNull.IsZero() {
				fromScalas[i].ValueOrNull = fromScalas[i].ValueOrNull.Add(fromScala.ValueOrNull)
			}
			found = true
			break
		}
	}

	if !found {
		// 새로운 FromAddress 추가
		if fromScala.Volume <= 0 {
			fromScala.Volume = 1
		}
		fromScalas = append(fromScalas, fromScala)
	}

	// 5. 변경 사항 저장
	bucket.ToUsers[to] = fromScalas
	m.dirty = true

	return newToUser, idx
}

// GetFromScalas: toAddr에 대한 모든 FromScala 반환
func (m *HeavyTimeBucketManager) GetFromScalas(toAddr domain.Address) []FromScala {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 최신 버킷부터 검색
	for i := 0; i < m.count; i++ {
		idx := (m.rearIndex - i + MaxTimeBuckets) % MaxTimeBuckets
		b := m.buckets[idx]
		if b == nil {
			continue
		}
		if fromScalas, exists := b.ToUsers[toAddr]; exists {
			// 복사본 반환 (동시성 안전)
			result := make([]FromScala, len(fromScalas))
			copy(result, fromScalas)
			return result
		}
	}

	return []FromScala{}
}

// == 통계/모니터링 API ==

// ActiveCount: 활성 버킷 수
func (m *HeavyTimeBucketManager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// TotalToUsers: 전체 to 사용자 수
func (m *HeavyTimeBucketManager) TotalToUsers() int {
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

// CountPendingRelations: 전체 pending relation 개수 (모든 FromScala 개수의 합)
func (m *HeavyTimeBucketManager) CountPendingRelations() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sum := 0
	for i := 0; i < MaxTimeBuckets; i++ {
		if b := m.buckets[i]; b != nil {
			for _, fromScalas := range b.ToUsers {
				sum += len(fromScalas)
			}
		}
	}
	return sum
}

// DeleteToUser: 특정 to 주소의 모든 pending relations 제거 (내부 사용용)
func (m *HeavyTimeBucketManager) DeleteToUser(to domain.Address) bool {
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

// == 내부 로직 ==

func (m *HeavyTimeBucketManager) calculateWeekStart(t chaintimer.ChainTime) chaintimer.ChainTime {
	year, month, day := t.Date()
	weekday := t.Weekday() // Sunday=0
	daysToSubtract := int(weekday)
	start := chaintimer.ChainDate(year, month, day-daysToSubtract, 0, 0, 0, 0, t.Location())
	return chaintimer.ChainTime(start)
}

// ensureBucketLocked: txTime이 속한 버킷 인덱스를 보장(존재/신규/회전).
// 호출 전 m.mu가 Lock 상태여야 함.
// 반환: (index, rotatedOld) — rotatedOld != nil이면 회전이 발생했고 콜백 인자.
func (m *HeavyTimeBucketManager) ensureBucketLocked(txTime chaintimer.ChainTime) (int, *HeavyTimeBucket) {
	// 초기: 첫 버킷 생성
	if m.count == 0 {
		ws := m.calculateWeekStart(txTime)
		m.buckets[0] = newHeavyTimeBucket(ws)
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
		m.buckets[newRear] = newHeavyTimeBucket(ws)
		m.rearIndex = newRear
		m.count++
		m.dirty = true
		return newRear, nil
	}
	// 가득 참 → front 로테이션
	old := m.buckets[m.frontIndex]
	totalFromScalas := 0
	if old != nil {
		for _, fromScalas := range old.ToUsers {
			totalFromScalas += len(fromScalas)
		}
	}
	fmt.Printf("HeavyTimeBucket: 버킷이 가득 참. %d개 to-user, %d개 pending relations가 있는 old버킷을 제거함\n",
		len(old.ToUsers), totalFromScalas)
	newIdx := m.frontIndex
	m.buckets[newIdx] = newHeavyTimeBucket(ws)
	m.frontIndex = (m.frontIndex + 1) % MaxTimeBuckets
	m.rearIndex = newIdx
	// count는 유지
	m.dirty = true
	return newIdx, old
}

// ----------------------
// 바이너리 스냅샷 (heavy_state.bin)
// ----------------------

const heavyTbMagicStr = "HTBKv1" // 6 bytes

func (m *HeavyTimeBucketManager) stateBinPath() string {
	if m.path == "" {
		return ""
	}
	return filepath.Join(m.path, "heavy_state.bin")
}

func (m *HeavyTimeBucketManager) Save() error {
	if m.path == "" {
		return nil
	}

	// (옵션) 더티 체크: 더티 아니면 스킵
	m.mu.RLock()
	if !m.dirty {
		m.mu.RUnlock()
		return nil
	}

	// 빠른 스냅샷용 얕은 복사 + map deep copy
	cnt := m.count
	front := m.frontIndex
	snap := make([]*HeavyTimeBucket, MaxTimeBuckets)
	for i := 0; i < MaxTimeBuckets; i++ {
		if b := m.buckets[i]; b != nil {
			nb := &HeavyTimeBucket{
				StartTime: b.StartTime,
				EndTime:   b.EndTime,
				ToUsers:   make(map[domain.Address][]FromScala, len(b.ToUsers)),
			}
			for k, v := range b.ToUsers {
				cp := make([]FromScala, len(v))
				copy(cp, v)
				nb.ToUsers[k] = cp
			}
			snap[i] = nb
		}
	}
	m.mu.RUnlock()

	var buf bytes.Buffer
	// header
	buf.WriteString(heavyTbMagicStr)
	if err := buf.WriteByte(byte(cnt)); err != nil {
		return err
	}

	// front..rear 순서로 직렬화
	for i := 0; i < cnt; i++ {
		idx := (front + i) % MaxTimeBuckets
		b := snap[idx]
		if b == nil {
			continue
		}
		// start_unix(int64)
		startUnix := time.Time(b.StartTime).UTC().Unix()
		if err := binary.Write(&buf, binary.LittleEndian, startUnix); err != nil {
			return err
		}
		// to_users_len(uint32)
		toUsersLen := uint32(len(b.ToUsers))
		if err := binary.Write(&buf, binary.LittleEndian, toUsersLen); err != nil {
			return err
		}
		// to_users: [20]byte + encoded FromScalas
		for addr, fromScalas := range b.ToUsers {
			if _, err := buf.Write(addr[:]); err != nil {
				return err
			}
			if err := writeFromScalas(&buf, fromScalas); err != nil {
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

	// 저장 성공 → dirty reset
	m.mu.Lock()
	m.dirty = false
	m.mu.Unlock()
	return nil
}

func (m *HeavyTimeBucketManager) Load() error {
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
	hdr := make([]byte, len(heavyTbMagicStr))
	if _, err := io.ReadFull(r, hdr); err != nil {
		return err
	}
	if string(hdr) != heavyTbMagicStr {
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

	newBuckets := make([]*HeavyTimeBucket, MaxTimeBuckets)

	for i := 0; i < cnt; i++ {
		var startUnix int64
		if err := binary.Read(r, binary.LittleEndian, &startUnix); err != nil {
			return fmt.Errorf("read start_unix: %w", err)
		}
		var toUsersLen uint32
		if err := binary.Read(r, binary.LittleEndian, &toUsersLen); err != nil {
			return fmt.Errorf("read to_users_len: %w", err)
		}

		st := chaintimer.ChainTime(time.Unix(startUnix, 0).UTC())
		tb := &HeavyTimeBucket{
			StartTime: st,
			EndTime:   st.Add(SlideInterval),
			ToUsers:   make(map[domain.Address][]FromScala, toUsersLen),
		}

		for j := uint32(0); j < toUsersLen; j++ {
			var a domain.Address
			if _, err := io.ReadFull(r, a[:]); err != nil {
				return fmt.Errorf("read address: %w", err)
			}
			fromScalas, err := readFromScalas(r)
			if err != nil {
				return fmt.Errorf("decode fromScalas: %w", err)
			}
			tb.ToUsers[a] = fromScalas
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
	m.dirty = false // 로드 직후는 디스크와 동기화
	m.mu.Unlock()

	return nil
}
