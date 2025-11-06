// file: internal/apool/rel/triplet/infra/heavy_timebucket_mgr.go
package infra

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ldomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/domain"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// === 파라미터 ===
const (
	WindowSize      = 4 * 30 * 24 * chaintimer.Hour // 4개월
	SlideInterval   = 7 * 24 * chaintimer.Hour      // 1주
	TriggerInterval = 7 * 24 * chaintimer.Hour
	MaxTimeBuckets  = 5 //21 일단 5개만 들고 있기.
)

// HeavyTimeBucket - 메모리 기반으로 toAddress -> FromScalas 매핑을 포함
type HeavyTimeBucket struct {
	StartTime chaintimer.ChainTime
	EndTime   chaintimer.ChainTime
	// 핵심: toAddress -> FromScalas 매핑을 메모리에 저장
	ToUsers map[domain.Address]ldomain.FromScalas
}

func newHeavyTimeBucket(start chaintimer.ChainTime) *HeavyTimeBucket {
	return &HeavyTimeBucket{
		StartTime: start,
		EndTime:   start.Add(SlideInterval),
		ToUsers:   make(map[domain.Address]ldomain.FromScalas),
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

	mu     sync.RWMutex
	dirty  bool // 변경 플래그\
	closed atomic.Bool
}

func NewHeavyTimeBucketManager() (*HeavyTimeBucketManager, error) {
	var path string = computation.GetStoragePath("triplet/timebucket")

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
	fmt.Println("헤비타임버킷 클로즈 시작")
	if m.closed.Swap(true) {
		return nil
	}
	if m == nil || m.path == "" {
		return nil
	}
	fmt.Println("헤비 타임버킷 락 블로킹")
	m.mu.RLock()
	dirty := m.dirty
	m.mu.RUnlock()

	fmt.Printf("HeavyTimeBUckManager Close() 호출: 더티마크:%v\n", dirty)
	if !dirty {
		return nil
	}
	return m.save()
}

// == 공개 API (단순화됨) ==

// AddIfAbsent: to 주소에 fromScala를 추가 (방어적 추가)
// 해당 시간의 버킷이 없으면 생성하고, to 주소가 윈도우에 없으면 새로 추가
// fromAddress가 이미 있으면 업데이트, 없으면 새로 추가
// 반환: (added, bucketIndex) - added는 새로운 to 주소가 추가되었는지 여부
func (m *HeavyTimeBucketManager) AddIfAbsent(to domain.Address, fromScala ldomain.FromScala) (bool, int) {
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

	// 3. 도메인 로직 위임: FromScalas의 AddOrUpdate 사용
	fromScalas := bucket.ToUsers[to]
	newToUser := len(fromScalas) == 0

	bucket.ToUsers[to] = fromScalas.AddOrUpdate(
		fromScala.FromAddress,
		fromScala.LastTxId,
		fromScala.LastTime,
		fromScala.ValueOrNull,
	)
	m.dirty = true

	return newToUser, idx
}

// GetFromScalas: toAddr에 대한 모든 FromScala 반환
func (m *HeavyTimeBucketManager) GetFromScalas(toAddr domain.Address) ldomain.FromScalas {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 최신 버킷부터 검색
	for i := 0; i < m.count; i++ {
		idx := (m.rearIndex - i + MaxTimeBuckets) % MaxTimeBuckets
		b := m.buckets[idx]
		if b == nil {
			continue
		}
		if fromScalas := b.ToUsers[toAddr]; len(fromScalas) > 0 {
			// 도메인 로직 위임: Copy 사용 (동시성 안전)
			return fromScalas.Copy()
		}
	}

	return ldomain.FromScalas{}
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

type heavyTimeBucketJSON struct {
	StartTime string                        `json:"startTime"` // RFC3339
	EndTime   string                        `json:"endTime"`   // RFC3339
	ToUsers   map[string]ldomain.FromScalas `json:"toUsers"`   // key: 0x + 40 hex
}

// 저장 전체 스냅샷(순서 보존)
type heavyStateJSON struct {
	Version int                   `json:"v"`       // 포맷 버전(필요 시 마이그레이션용)
	Buckets []heavyTimeBucketJSON `json:"buckets"` // front..rear 순서 유지
}

func hexToAddr(s string) (domain.Address, error) {
	var out domain.Address
	ss := strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if len(ss) != 40 {
		return out, fmt.Errorf("addr hex length %d != 40", len(ss))
	}
	b, err := hex.DecodeString(ss)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

func (m *HeavyTimeBucketManager) stateBinPath() string {
	if m.path == "" {
		return ""
	}
	return filepath.Join(m.path, "heavy_state.bin")
}

func (m *HeavyTimeBucketManager) save() error {
	if m.path == "" {
		return nil
	}

	// (옵션) 더티 체크
	m.mu.RLock()
	if !m.dirty {
		m.mu.RUnlock()
		return nil
	}

	startTime := time.Now()
	fmt.Printf("\n🔄 HeavyTimeBucket 저장 시작(JSON) → %s\n", m.stateBinPath())

	// 스냅샷 (필요 최소한만 유지)
	cnt := m.count
	front := m.frontIndex

	snap := make([]*HeavyTimeBucket, MaxTimeBuckets)
	totalToUsers := 0

	for i := 0; i < MaxTimeBuckets; i++ {
		if b := m.buckets[i]; b != nil {
			nb := &HeavyTimeBucket{
				StartTime: b.StartTime,
				EndTime:   b.EndTime,
				ToUsers:   make(map[domain.Address]ldomain.FromScalas, len(b.ToUsers)),
			}
			for k, v := range b.ToUsers {
				nb.ToUsers[k] = v.Copy() // 도메인 로직에 위임
				totalToUsers++
			}
			snap[i] = nb
		}
	}
	m.mu.RUnlock()

	// 진행률 출력(선택)
	var processedUsers int32
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				current := int(atomic.LoadInt32(&processedUsers))
				var pct float64
				if totalToUsers > 0 {
					pct = float64(current) / float64(totalToUsers) * 100
				}
				elapsed := time.Since(startTime)
				fmt.Printf("⏱️  저장 중: %d/%d 유저 (%.1f%%) - 경과: %.1f초\n",
					current, totalToUsers, pct, elapsed.Seconds())
			}
		}
	}()

	// front..rear 순서대로 JSON DTO 구성
	state := heavyStateJSON{
		Version: 1,
		Buckets: make([]heavyTimeBucketJSON, 0, cnt),
	}
	for i := 0; i < cnt; i++ {
		idx := (front + i) % MaxTimeBuckets
		b := snap[idx]
		if b == nil {
			continue
		}
		j := heavyTimeBucketJSON{
			StartTime: time.Time(b.StartTime).UTC().Format(time.RFC3339Nano),
			EndTime:   time.Time(b.EndTime).UTC().Format(time.RFC3339Nano),
			ToUsers:   make(map[string]ldomain.FromScalas, len(b.ToUsers)),
		}
		for addr, fromScalas := range b.ToUsers {
			j.ToUsers[addr.ToHex()] = fromScalas // FromScalas는 자체 JSON 사용
			atomic.AddInt32(&processedUsers, 1)
		}
		state.Buckets = append(state.Buckets, j)
	}

	// 파일로 바로 인코딩 (tmp 파일 → rename, fsync 유지)
	tmp := m.stateBinPath() + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		close(done)
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	// enc.SetIndent("", "  ") // 사람이 보는 용도면 주석 해제

	if err := enc.Encode(&state); err != nil {
		f.Close()
		os.Remove(tmp)
		close(done)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		close(done)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		close(done)
		return err
	}
	if err := os.Rename(tmp, m.stateBinPath()); err != nil {
		close(done)
		return err
	}
	close(done)

	// 저장 성공 → dirty reset
	m.mu.Lock()
	m.dirty = false
	m.mu.Unlock()

	elapsed := time.Since(startTime)
	fmt.Printf("✅ HeavyTimeBucket 저장 완료(JSON): %d 유저 (%.2f초)\n\n", totalToUsers, elapsed.Seconds())
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

	var state heavyStateJSON
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode heavy state json: %w", err)
	}
	// 버전 관리가 필요하면 여기서 state.Version 검사/마이그레이션

	newBuckets := make([]*HeavyTimeBucket, MaxTimeBuckets)

	// front..rear 순서로 채움
	cnt := len(state.Buckets)
	if cnt > MaxTimeBuckets {
		return fmt.Errorf("invalid bucket count: %d > MaxTimeBuckets(%d)", cnt, MaxTimeBuckets)
	}
	for i := 0; i < cnt; i++ {
		bj := state.Buckets[i]

		st, err := time.Parse(time.RFC3339Nano, bj.StartTime)
		if err != nil {
			return fmt.Errorf("parse startTime: %w", err)
		}
		et, err := time.Parse(time.RFC3339Nano, bj.EndTime)
		if err != nil {
			return fmt.Errorf("parse endTime: %w", err)
		}

		tb := &HeavyTimeBucket{
			StartTime: chaintimer.ChainTime(st.UTC()),
			EndTime:   chaintimer.ChainTime(et.UTC()),
			ToUsers:   make(map[domain.Address]ldomain.FromScalas, len(bj.ToUsers)),
		}

		for hexKey, fromScalas := range bj.ToUsers {
			a, err := hexToAddr(hexKey)
			if err != nil {
				return fmt.Errorf("addr decode: %w", err)
			}
			tb.ToUsers[a] = fromScalas // FromScalas는 JSON에서 이미 복원됨
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
