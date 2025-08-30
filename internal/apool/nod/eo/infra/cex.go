package infra

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// CEX DB 인터페이스 (다른 DB들과 동일한 패턴)
type CEXDB interface {
	IsContain(address string) bool // 메모리-우선 조회
	Add(address string) error      // 단건 추가(append + flush)
	Put(addresses []string) error  // 전체 교체(스냅샷 원자적 재기록)
	Close() error                  // 종료 시 flush/close
}

// FileCEXRepository: 파일 기반 Set 저장소 (append-on-add, snapshot-on-put)
// - New 시 파일을 읽어 메모리 set 로드
// - Get: 메모리-우선 조회
// - Add: set에 추가 및 파일 append (즉시 flush)
// - Put: set 전체를 교체 및 파일 스냅샷 원자적 재기록
type FileCEXRepository struct {
	filePath string

	mu        sync.RWMutex
	set       map[string]struct{} // 메모리-우선 조회용
	appendF   *os.File            // append 파일 핸들
	appendBuf *bufio.Writer       // append 버퍼
}

const defaultHeader = `# CEX Hot Wallet Addresses
# This file contains known centralized exchange hot wallet addresses
# Format: one Ethereum address per line
# Lines starting with # are comments and will be ignored
`

// NewFileCEXRepository: 파일 로드 + 메모리 set 구성 + append writer 준비
func NewFileCEXRepository(processMode mode.ProcessingMode, filePath string) (*FileCEXRepository, error) {
	var path string
	if filePath == "" {
		if processMode.IsTest() {
			path = computation.FindTestingStorageRootPath() + "/nod/eo/cex.txt"
		} else {
			path = computation.FindProductionStorageRootPath() + "/nod/eo/cex.txt"
		}
	} else {
		path = filePath
	}
	r := &FileCEXRepository{
		filePath: path,
		set:      make(map[string]struct{}, 4096),
	}
	if err := r.ensureFileExists(); err != nil {
		return nil, err
	}
	if err := r.loadIntoMemory(); err != nil {
		return nil, err
	}
	// append writer는 항상 유지(추가가 자주 일어나는 환경 가정)
	if err := r.ensureAppendWriter(); err != nil {
		return nil, err
	}
	return r, nil
}

// ------------------- Public API (CEXDB) -------------------

func (r *FileCEXRepository) IsContain(address string) bool {
	addr := normalizeAddr(address)
	if addr == "" {
		return false
	}
	r.mu.RLock()
	_, ok := r.set[addr]
	r.mu.RUnlock()
	return ok
}

func (r *FileCEXRepository) GetCexSet() map[string]struct{} {
	return r.set
}
func (r *FileCEXRepository) Add(address string) error {
	addr := normalizeAddr(address)
	if addr == "" {
		return fmt.Errorf("invalid address: %q", address)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// 이미 있으면 NOP
	if _, exists := r.set[addr]; exists {
		return nil
	}
	r.set[addr] = struct{}{}

	// append + 즉시 flush
	if err := r.ensureAppendWriter(); err != nil {
		return err
	}
	if _, err := r.appendBuf.WriteString(addr + "\n"); err != nil {
		return fmt.Errorf("append write failed: %w", err)
	}
	if err := r.appendBuf.Flush(); err != nil {
		return fmt.Errorf("append flush failed: %w", err)
	}
	return nil
}

// Put: 전체 교체(스냅샷 원자적 재기록). 대량 업데이트/동기화 용도.
// - 메모리 set을 주어진 addresses로 대체
// - temp 파일에 헤더 + 정렬된 주소를 기록한 뒤 원자적으로 rename
// - append writer 재오픈
func (r *FileCEXRepository) Put(addresses []string) error {
	// 미리 정규화 + 중복 제거
	newSet := make(map[string]struct{}, len(addresses))
	for _, a := range addresses {
		n := normalizeAddr(a)
		if n == "" {
			continue
		}
		newSet[n] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// 메모리에 반영
	r.set = newSet

	// 스냅샷 파일 원자적 재기록
	return r.saveSnapshotNoLock()
}

func (r *FileCEXRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 종료 시 스냅샷으로 정리(정렬/중복 제거된 단일 파일)
	if err := r.saveSnapshotNoLock(); err != nil {
		// 실패해도 writer는 닫는다
		r.closeAppendWriter()
		return err
	}
	r.closeAppendWriter()
	return nil
}

// ------------------- Internal helpers -------------------

func normalizeAddr(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" || !strings.HasPrefix(s, "0x") || len(s) != 42 { // 0x + 40 hex
		return ""
	}
	return s
}

func (r *FileCEXRepository) ensureFileExists() error {
	dir := filepath.Dir(r.filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(r.filePath); os.IsNotExist(err) {
		f, e := os.Create(r.filePath)
		if e != nil {
			return e
		}
		defer f.Close()
		if _, e = f.WriteString(defaultHeader); e != nil {
			return e
		}
	}
	return nil
}

func (r *FileCEXRepository) ensureAppendWriter() error {
	if r.appendBuf != nil && r.appendF != nil {
		return nil
	}
	f, err := os.OpenFile(r.filePath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open append: %w", err)
	}
	r.appendF = f
	r.appendBuf = bufio.NewWriterSize(f, 64*1024)
	return nil
}

func (r *FileCEXRepository) closeAppendWriter() {
	if r.appendBuf != nil {
		_ = r.appendBuf.Flush()
		r.appendBuf = nil
	}
	if r.appendF != nil {
		_ = r.appendF.Close()
		r.appendF = nil
	}
}

func (r *FileCEXRepository) loadIntoMemory() error {
	f, err := os.Open(r.filePath)
	if err != nil {
		return fmt.Errorf("open for load: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	// 매우 긴 라인 가능성 낮지만, 필요하면 Buffer 확장:
	// sc.Buffer(make([]byte, 64*1024), 2*1024*1024)

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		addr := normalizeAddr(line)
		if addr != "" {
			r.set[addr] = struct{}{}
		}
	}
	fmt.Printf("cex 로드 완료 %v라인", len(r.set))
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	return nil
}

func (r *FileCEXRepository) saveSnapshotNoLock() error {
	if err := r.ensureFileExists(); err != nil {
		return err
	}
	// 교체 전 append writer 닫기(파일 대체 예정)
	r.closeAppendWriter()

	// 정렬된 리스트로 temp 파일 작성
	all := make([]string, 0, len(r.set))
	for a := range r.set {
		all = append(all, a)
	}
	sort.Strings(all)

	tmp := r.filePath + ".tmp"
	tmpF, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	bw := bufio.NewWriterSize(tmpF, 128*1024)

	if _, err = bw.WriteString(defaultHeader); err != nil {
		_ = tmpF.Close()
		return fmt.Errorf("write header: %w", err)
	}
	for _, a := range all {
		if _, err = bw.WriteString(a + "\n"); err != nil {
			_ = tmpF.Close()
			return fmt.Errorf("write addr: %w", err)
		}
	}
	if err = bw.Flush(); err != nil {
		_ = tmpF.Close()
		return fmt.Errorf("flush tmp: %w", err)
	}
	if err = tmpF.Close(); err != nil {
		return fmt.Errorf("close tmp: %w", err)
	}

	// 원자적 교체
	if err = os.Rename(tmp, r.filePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename tmp->target: %w", err)
	}

	// 새 파일에 대해 append writer 재개방
	if err := r.ensureAppendWriter(); err != nil {
		return err
	}
	return nil
}
