package idmap

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/edsrzf/mmap-go"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

// ===== 파일 포맷 상수 =====

const (
	fileMagic         = "IDMAPV1" // 7 bytes
	headerSize        = 64
	entrySize         = 21 // 20(addr) + 1(flag)
	version    uint32 = 1

	flagContract byte = 1
)

// 헤더 레이아웃 (binary.LittleEndian):
// [0:7]   MAGIC ("IDMAPV1")
// [8:12]  VERSION (u32)
// [12:16] ENTRY_SIZE (u32) == 21
// [16:24] SIZE (u64)      : 커밋 완료된 엔트리 수(가시성 기준)
// [24:32] CAPACITY (u64)  : 현재 파일 수용 엔트리 수
// [32:64] RESERVED

// ===== 싱글톤 & 모드 배타 =====

var (
	activeMode int32 // 0:none, 1:test, 2:prod
	prodOnce   sync.Once
	testOnce   sync.Once

	prodInst *Mapper
	testInst *Mapper
)

// ===== Mapper 본체 =====

type Mapper struct {
	mu       sync.RWMutex
	addr2id  *badger.DB
	mf       *os.File
	mmap     mmap.MMap
	filePath string

	removeOnClose bool // 테스트 모드에선 false (요청 반영)
	dbDir         string

	// 고정 경량 워커풀
	jobCh chan workerpool.Job
	wp    *workerpool.Pool
}

// ===== 설정 =====

type Config struct {
	// Badger 경로 & mmap 파일 경로
	DBDir    string
	FilePath string

	// 초기 capacity (엔트리 수); 0이면 기본값 사용
	InitialCapacity uint64

	// 안전성 우선: SyncWrites true 권장
	BadgerSyncWrites bool

	// (프로덕션/테스트 공통) 종료 시 파일/디렉토리 삭제 여부
	// 요청에 따라 테스트는 false를 권장
	RemoveOnClose bool

	// 워커풀 설정
	NumWorkers    int // 고정 워커 수 (예: 8~16)
	PoolQueueSize int // 내부 job 큐 크기 (예: 4096)
}

// ===== Public: 모드별 싱글톤 =====

func GetProductionMapper() *Mapper {
	if atomic.LoadInt32(&activeMode) == 1 {
		panic("testing mapper already initialized; production is not allowed")
	}
	prodOnce.Do(func() {
		atomic.StoreInt32(&activeMode, 2)
		cfg := Config{
			DBDir:            "./idmap.badger",
			FilePath:         "./idmap.mmap",
			InitialCapacity:  1 << 20, // 1M 엔트리
			BadgerSyncWrites: true,
			RemoveOnClose:    false,

			NumWorkers:    12,
			PoolQueueSize: 4096,
		}
		m, err := openMapper(cfg)
		if err != nil {
			panic(fmt.Errorf("open production mapper failed: %w", err))
		}
		prodInst = m
	})
	return prodInst
}

func GetTestingMapper() *Mapper {
	if atomic.LoadInt32(&activeMode) == 2 {
		panic("production mapper already initialized; testing is not allowed")
	}
	testOnce.Do(func() {
		atomic.StoreInt32(&activeMode, 1)
		// 요청 사항: temp/자동삭제 X, 경로만 다르게
		cfg := Config{
			DBDir:            "./idmap_test.badger",
			FilePath:         "./idmap_test.mmap",
			InitialCapacity:  1 << 16, // 65,536 엔트리
			BadgerSyncWrites: true,
			RemoveOnClose:    false,

			NumWorkers:    8,
			PoolQueueSize: 4096,
		}
		m, err := openMapper(cfg)
		if err != nil {
			panic(fmt.Errorf("open testing mapper failed: %w", err))
		}
		testInst = m
	})
	return testInst
}

// ===== 파일/DB 오픈 =====

func openMapper(cfg Config) (*Mapper, error) {
	// 1) Badger
	opts := badger.DefaultOptions(cfg.DBDir)
	opts.SyncWrites = cfg.BadgerSyncWrites
	if err := os.MkdirAll(cfg.DBDir, 0o755); err != nil {
		return nil, err
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	// 2) mmap 파일
	newFile := false
	if _, err := os.Stat(cfg.FilePath); errors.Is(err, os.ErrNotExist) {
		newFile = true
	}
	f, err := os.OpenFile(cfg.FilePath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	var capInit uint64 = 1 << 20 // 기본 1M
	if cfg.InitialCapacity > 0 {
		capInit = cfg.InitialCapacity
	}

	if newFile {
		if err := f.Truncate(int64(headerSize + capInit*entrySize)); err != nil {
			_ = db.Close()
			_ = f.Close()
			return nil, err
		}
	}

	mm, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		_ = db.Close()
		_ = f.Close()
		return nil, err
	}

	mp := &Mapper{
		addr2id:  db,
		mf:       f,
		mmap:     mm,
		filePath: cfg.FilePath,

		removeOnClose: cfg.RemoveOnClose,
		dbDir:         cfg.DBDir,
	}

	// 헤더 준비/검증
	if newFile {
		if err := mp.writeFreshHeader(capInit); err != nil {
			_ = mp.Close()
			return nil, err
		}
	} else {
		if err := mp.checkHeader(); err != nil {
			_ = mp.Close()
			return nil, err
		}
	}

	// 3) 고정 워커풀 준비
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 8
	}
	if cfg.PoolQueueSize <= 0 {
		cfg.PoolQueueSize = 2048
	}
	mp.jobCh = make(chan workerpool.Job, cfg.PoolQueueSize)
	mp.wp = workerpool.New(context.Background(), cfg.NumWorkers, mp.jobCh)

	return mp, nil
}

func (m *Mapper) Close() error {
	// 워커풀 종료
	if m.wp != nil {
		m.wp.Shutdown()
	}
	if m.jobCh != nil {
		close(m.jobCh) // 더 이상 잡을 보내지 않음
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var err1, err2, err3 error
	if m.mmap != nil {
		err1 = m.mmap.Flush()
		err2 = m.mmap.Unmap()
		m.mmap = nil
	}
	if m.mf != nil {
		err3 = m.mf.Close()
		m.mf = nil
	}
	if m.addr2id != nil {
		_ = m.addr2id.Close()
		m.addr2id = nil
	}
	if m.removeOnClose {
		_ = os.Remove(m.filePath)
		_ = os.RemoveAll(m.dbDir)
	}
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

// ===== 헤더 I/O =====

func (m *Mapper) writeFreshHeader(capacity uint64) error {
	h := m.mmap[:headerSize]
	// zero out
	for i := range h {
		h[i] = 0
	}
	copy(h[0:7], []byte(fileMagic))
	binary.LittleEndian.PutUint32(h[8:12], version)
	binary.LittleEndian.PutUint32(h[12:16], entrySize)
	// size = 0
	binary.LittleEndian.PutUint64(h[16:24], 0)
	binary.LittleEndian.PutUint64(h[24:32], capacity)
	return m.mmap.Flush()
}

func (m *Mapper) checkHeader() error {
	h := m.mmap[:headerSize]
	if string(h[0:7]) != fileMagic {
		return fmt.Errorf("invalid magic")
	}
	if binary.LittleEndian.Uint32(h[8:12]) != version {
		return fmt.Errorf("invalid version")
	}
	if binary.LittleEndian.Uint32(h[12:16]) != entrySize {
		return fmt.Errorf("invalid entry size")
	}
	// 간단 무결성 체크
	cap := binary.LittleEndian.Uint64(h[24:32])
	if cap == 0 {
		return fmt.Errorf("zero capacity")
	}
	return nil
}

func (m *Mapper) headerSizeVal() uint64 {
	return binary.LittleEndian.Uint64(m.mmap[16:24])
}

func (m *Mapper) headerCapacityVal() uint64 {
	return binary.LittleEndian.Uint64(m.mmap[24:32])
}

func (m *Mapper) headerSetSize(newSize uint64) error {
	binary.LittleEndian.PutUint64(m.mmap[16:24], newSize)
	return m.mmap.Flush()
}

func (m *Mapper) headerSetCapacity(newCap uint64) error {
	binary.LittleEndian.PutUint64(m.mmap[24:32], newCap)
	return m.mmap.Flush()
}

// ===== 내부 유틸 =====

func offForID(id uint64) int64 {
	// id는 1부터 시작
	return int64(headerSize) + int64((id-1)*entrySize)
}

func keyAddr(a domain.Address) []byte {
	k := make([]byte, 1+20)
	k[0] = 'A'
	copy(k[1:], a[:])
	return k
}

func encodeVal(id uint32, isContract bool) []byte {
	var v [5]byte
	binary.LittleEndian.PutUint32(v[0:4], id)
	if isContract {
		v[4] = flagContract
	}
	return v[:]
}

func decodeVal(v []byte) (id uint32, isContract bool, err error) {
	if len(v) != 5 {
		return 0, false, fmt.Errorf("invalid value len")
	}
	id = binary.LittleEndian.Uint32(v[0:4])
	isContract = v[4]&flagContract == flagContract
	return
}

// ensureCapacity: 필요 엔트리를 수용하도록 파일 확장 + remap
func (m *Mapper) ensureCapacity(need uint64) error {
	curCap := m.headerCapacityVal()
	if need <= curCap {
		return nil
	}
	newCap := curCap * 2
	if newCap < need {
		newCap = need
	}
	newSizeBytes := int64(headerSize + newCap*entrySize)

	// Unmap → Truncate → Remap → header 갱신
	if err := m.mmap.Unmap(); err != nil {
		return err
	}
	if err := m.mf.Truncate(newSizeBytes); err != nil {
		return err
	}
	mm, err := mmap.Map(m.mf, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	m.mmap = mm
	return m.headerSetCapacity(newCap)
}

// ===== 퍼블릭 단건 API (SWMR) =====

func (m *Mapper) AddressToID(a domain.Address) domain.IDInfo {
	// 1) 빠른 조회 (RLock)
	m.mu.RLock()
	idInfo, ok := m.lookupAddrNoLock(a)
	if ok {
		m.mu.RUnlock()
		return idInfo
	}
	m.mu.RUnlock()

	// 2) 쓰기 경합 (Lock)
	m.mu.Lock()
	defer m.mu.Unlock()

	// 더블체크
	if idInfo, ok := m.lookupAddrNoLock(a); ok {
		return idInfo
	}

	// 새 ID 준비 (아직 header.SIZE 증가하지 않음)
	curSize := m.headerSizeVal()
	newIDu64 := curSize + 1
	if err := m.ensureCapacity(newIDu64); err != nil {
		panic(err)
	}

	// 2-1) mmap 엔트리 바디 먼저 기록(주소/flag)
	base := offForID(newIDu64)
	copy(m.mmap[base:base+20], a[:])
	m.mmap[base+20] = 0 // isContract=false로 시작
	if err := m.mmap.Flush(); err != nil {
		// 원복(깨끗이 지움)
		for i := 0; i < entrySize; i++ {
			m.mmap[base+int64(i)] = 0
		}
		_ = m.mmap.Flush()
		panic(err)
	}

	// 2-2) Badger 트랜잭션 커밋
	err := m.addr2id.Update(func(txn *badger.Txn) error {
		return txn.Set(keyAddr(a), encodeVal(uint32(newIDu64), false))
	})
	if err != nil {
		// mmap 바디 롤백(지움)
		for i := range entrySize {
			m.mmap[base+int64(i)] = 0
		}
		_ = m.mmap.Flush()
		panic(err)
	}

	// 2-3) 헤더 SIZE 증가(가시성 커밋)
	if err := m.headerSetSize(newIDu64); err != nil {
		panic(err)
	}

	return domain.IDInfo{ID: domain.ID(uint32(newIDu64)), IsContract: domain.IsContract(false)}
}

func (m *Mapper) IdToAddress(id domain.ID) domain.Address {
	m.mu.RLock()
	defer m.mu.RUnlock()
	idu := uint64(id)
	if idu == 0 || idu > m.headerSizeVal() {
		panic("id가 0이거나 id가 현재 max값보다 큶")
	}
	base := offForID(idu)
	var a domain.Address
	copy(a[:], m.mmap[base:base+20])
	return a
}

func (m *Mapper) MarkContract(id domain.ID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idu := uint64(id)
	if idu == 0 || idu > m.headerSizeVal() {
		return fmt.Errorf("id out of range")
	}
	base := offForID(idu)

	// mmap 갱신
	m.mmap[base+20] = flagContract
	if err := m.mmap.Flush(); err != nil {
		return err
	}

	// Badger 값도 갱신(기존 주소 얻어서 value 재기록)
	var addr domain.Address
	copy(addr[:], m.mmap[base:base+20]) // ← 20바이트 정확히 복사 (버그 수정)

	key := keyAddr(addr)
	return m.addr2id.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			idv, _, err := decodeVal(v)
			if err != nil {
				return err
			}
			if idv != uint32(idu) {
				return fmt.Errorf("badger/maps mismatch: %d vs %d", idv, idu)
			}
			return txn.Set(key, encodeVal(idv, true))
		})
	})
}

// ===== 내부 조회 헬퍼 (락 보유 전제) =====

func (m *Mapper) lookupAddrNoLock(a domain.Address) (domain.IDInfo, bool) {
	var info domain.IDInfo
	err := m.addr2id.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(keyAddr(a))
		if err != nil {
			return err
		}
		return itm.Value(func(v []byte) error {
			id, isC, e := decodeVal(v)
			if e != nil {
				return e
			}
			info = domain.IDInfo{ID: domain.ID(id), IsContract: domain.IsContract(isC)}
			return nil
		})
	})
	if err != nil {
		return domain.IDInfo{}, false
	}
	return info, true
}

// ==================================================================
// ====================== 배치 READ / WRITE =========================
// ==================================================================

// 1) mmap 배치 읽기: RLock 1회로 초고속
func (m *Mapper) IdToAddressBatch(ids []domain.ID) []domain.Address {
	out := make([]domain.Address, len(ids))
	m.mu.RLock()
	max := m.headerSizeVal() // 스냅샷
	for i, id := range ids {
		idu := uint64(id)
		if idu == 0 || idu > max {
			panic("id가 0이거나, id가 max값보다 큼.")
		}
		base := offForID(idu)
		copy(out[i][:], m.mmap[base:base+20])
	}
	m.mu.RUnlock()
	return out
}

// 내부: 조회 잡
type lookupChunkJob struct {
	m       *Mapper
	addrs   []domain.Address
	start   int
	end     int
	infos   []domain.IDInfo
	present []bool

	// 첫 에러만 기록 (간단화)
	errPtr *atomic.Value
}

func (j *lookupChunkJob) Do(ctx context.Context) error {
	// Badger 스냅샷 View 한 번으로 청크 처리
	viewErr := j.m.addr2id.View(func(txn *badger.Txn) error {
		for i := j.start; i < j.end; i++ {
			a := j.addrs[i]
			itm, e := txn.Get(keyAddr(a))
			if e != nil {
				if errors.Is(e, badger.ErrKeyNotFound) {
					continue
				}
				return e
			}
			e = itm.Value(func(v []byte) error {
				id, isC, de := decodeVal(v)
				if de != nil {
					return de
				}
				j.infos[i] = domain.IDInfo{
					ID: domain.ID(id), IsContract: domain.IsContract(isC),
				}
				j.present[i] = true
				return nil
			})
			if e != nil {
				return e
			}
		}
		return nil
	})
	if viewErr != nil {
		j.errPtr.CompareAndSwap(nil, viewErr)
		return viewErr
	}
	return nil
}

// 2) Badger 병렬 조회: 고정 워커풀 사용, 청크당 1 View 트랜잭션
func (m *Mapper) LookupExistingBatch(ctx context.Context, addrs []domain.Address, chunkSize int) (infos []domain.IDInfo, present []bool, err error) {
	n := len(addrs)
	if n == 0 {
		return nil, nil, nil
	}
	if chunkSize <= 0 {
		chunkSize = 1024
	}
	infos = make([]domain.IDInfo, n)
	present = make([]bool, n)

	var wg sync.WaitGroup
	var errPtr atomic.Value

	// 잡 발행
	for s := 0; s < n; s += chunkSize {
		e := min(s + chunkSize, n)
		wg.Add(1)
		job := &lookupChunkJob{
			m:       m,
			addrs:   addrs,
			start:   s,
			end:     e,
			infos:   infos,
			present: present,
			errPtr:  &errPtr,
		}
		// 완료 신호는 외부에서 wait
		go func(j workerpool.Job) {
			defer wg.Done()
			m.jobCh <- j
		}(job)
	}

	wg.Wait()
	if v := errPtr.Load(); v != nil {
		return nil, nil, v.(error)
	}
	return infos, present, nil
}

// 3) 배치 쓰기: 존재하지 않는 주소만 한 번에 연속 쓰기 + WriteBatch
//   - 1단계: 병렬 조회로 miss 선별 (LookupExistingBatch)
//   - 2단계: 단일 Lock 구간에서 mmap 일괄 쓰기 + Badger WriteBatch + header SIZE 1회 갱신
func (m *Mapper) GetOrAssignBatch(ctx context.Context, addrs []domain.Address, chunkSize int) (infos []domain.IDInfo, created []bool, err error) {
	n := len(addrs)
	if n == 0 {
		return nil, nil, nil
	}
	infos = make([]domain.IDInfo, n)
	created = make([]bool, n)

	// 1) 병렬 조회
	found, present, err := m.LookupExistingBatch(ctx, addrs, chunkSize)
	if err != nil {
		return nil, nil, err
	}
	copy(infos, found)

	// miss 목록 수집
	missesIdx := make([]int, 0, n/2)
	for i, ok := range present {
		if !ok {
			missesIdx = append(missesIdx, i)
		}
	}
	if len(missesIdx) == 0 {
		return infos, created, nil
	}

	// 2) 단일 Lock 구간: 연속 mmap 쓰기 + Badger WriteBatch + 헤더 1회 증가
	m.mu.Lock()
	defer m.mu.Unlock()

	curSize := m.headerSizeVal()
	need := curSize + uint64(len(missesIdx))
	if err := m.ensureCapacity(need); err != nil {
		return nil, nil, err
	}

	// 2-1) mmap 바디 일괄 기록
	nextID := curSize + 1
	for _, idx := range missesIdx {
		a := addrs[idx]
		base := offForID(nextID)
		copy(m.mmap[base:base+20], a[:])
		m.mmap[base+20] = 0 // isContract=false
		nextID++
	}
	// 한 번만 flush
	if err := m.mmap.Flush(); err != nil {
		// best-effort 롤백은 생략(필요 시 zero-fill 가능)
		return nil, nil, err
	}

	// 2-2) Badger WriteBatch
	wb := m.addr2id.NewWriteBatch()
	defer wb.Cancel()

	assignID := curSize + 1
	for _, idx := range missesIdx {
		a := addrs[idx]
		if err := wb.Set(keyAddr(a), encodeVal(uint32(assignID), false)); err != nil {
			return nil, nil, err
		}
		// 결과 채워두기
		infos[idx] = domain.IDInfo{ID: domain.ID(uint32(assignID)), IsContract: false}
		created[idx] = true
		assignID++
	}
	if err := wb.Flush(); err != nil {
		return nil, nil, err
	}

	// 2-3) 헤더 SIZE 1회 갱신
	if err := m.headerSetSize(curSize + uint64(len(missesIdx))); err != nil {
		return nil, nil, err
	}

	return infos, created, nil
}
