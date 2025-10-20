// Package eventbus: SPSC(다중생산자→다중소비자) 이벤트 버스
// 정확히 말하면 컨슈머는 단일 채널이긴 한데, 병렬로 채널에서 읽어도 문제가 없음
// - 내부: pending []T + sync.Cond + out <-chan T(1개)
// - 시작 시 JSONL 로드→삭제 / 종료 시 pending JSONL 저장 (동기)
// - busy-wait 없음 (Cond 기반 대기)
// - 제네릭 T, 직렬화는 JSON 고정
package eventbus

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// EventBusMode EventBus 동작 모드
type EventBusMode int

const (
	PersistentMode EventBusMode = iota // JSONL 저장/로드 지원
	MemoryOnlyMode                     // 메모리 전용, 채널 직접 사용
)

// EventBus[T] : 단일 소비자용 이벤트 버스
type EventBus[T any] struct {
	out      chan T
	mu       sync.Mutex
	cv       *sync.Cond
	pending  []T
	capLimit int

	filePath string       // JSONL 저장 경로
	mode     EventBusMode // 동작 모드

	stopping bool
	closed   atomic.Bool
	wg       sync.WaitGroup
}

//TODO 상태 저장 가능하도록 "종료"시 백로그 저장되게 하기
//*다만 save, load기능이 있는 이벤트버스는 락을 잘 걸어야 함. 콘슈밍 중 종료 시 남은 것들만 정확히 기록 필요

// NewInMemory: 단순한 메모리 전용 EventBus (채널 기반)
// JSONL, condition variable, waitgroup 없는 간단한 구조
// 워커풀이나 인터페이스 타입용으로 적합
func NewInMemory[T any](bufferSize int) *EventBus[T] {
	if bufferSize <= 0 {
		bufferSize = 1000 // 기본 버퍼 크기
	}

	return &EventBus[T]{
		out:      make(chan T, bufferSize),
		filePath: "",             // 빈 경로로 JSONL 비활성화 표시
		capLimit: 0,              // 무제한
		mode:     MemoryOnlyMode, // 메모리 전용 모드
	}
}

// Publish: 생산자 → pending. capLimit 초과 시 cond wait로 역압.
func (b *EventBus[T]) Publish(v T) error {
	if b.closed.Load() {
		return errors.New("eventbus: closed")
	}

	// ✅ 메모리 전용 모드: 블로킹 send (대기하면서 흘러가게)
	if b.mode == MemoryOnlyMode {
		b.out <- v // 블로킹
		return nil
	}

	// 퍼시스턴트 모드
	b.mu.Lock()
	defer b.mu.Unlock()
	for !b.stopping && b.capLimit > 0 && len(b.pending) >= b.capLimit {
		b.cv.Wait()
	}
	if b.stopping {
		return errors.New("eventbus: stopping")
	}
	b.pending = append(b.pending, v)
	b.cv.Signal()
	return nil
}

func (b *EventBus[T]) PublishBatch(vs []T) error {
	if b.closed.Load() {
		return errors.New("eventbus: closed")
	}

	// ✅ 메모리 전용 모드: 채널로 순차 송신 (블로킹)
	if b.mode == MemoryOnlyMode {
		for i := range vs {
			b.out <- vs[i] // 공간 생길 때까지 대기
		}
		return nil
	}

	// 퍼시스턴트 모드: 남은 공간만큼 쪼개서 넣기
	start := 0
	for start < len(vs) {
		b.mu.Lock()
		for !b.stopping && b.capLimit > 0 && len(b.pending) >= b.capLimit {
			b.cv.Wait()
		}
		if b.stopping {
			b.mu.Unlock()
			return errors.New("eventbus: stopping")
		}

		room := len(vs) - start
		if b.capLimit > 0 {
			capRoom := b.capLimit - len(b.pending)
			if room > capRoom {
				room = capRoom
			}
		}
		if room > 0 {
			b.pending = append(b.pending, vs[start:start+room]...)
			start += room
			b.cv.Signal()
		}
		b.mu.Unlock()
	}
	return nil
}

func (b *EventBus[T]) run(backlog []T) {
	defer b.wg.Done()
	local := make([]T, 0, 128)

	b.mu.Lock()
	b.pending = append(b.pending, backlog...)
	for {
		for !b.stopping && len(b.pending) == 0 {
			b.cv.Wait()
		}
		if b.stopping {
			// pending + local의 미전송 항목 수집
			rest := append([]T(nil), b.pending...)
			rest = append(rest, local...)
			b.mu.Unlock()

			// ✅ 퍼시스턴트 모드에서만 JSONL 저장
			if b.mode == PersistentMode && len(rest) > 0 && b.filePath != "" {
				_ = saveBacklogJSONL(b.filePath, rest)
			}
			close(b.out)
			return
		}

		// swap pending -> local (드레인-버스트)
		local = local[:0]
		local = append(local, b.pending...)
		b.pending = b.pending[:0]
		b.cv.Signal()
		b.mu.Unlock()

		// local에서 out 채널로 전송 시도
		sent := 0
		for i := 0; i < len(local); i++ {
			select {
			case b.out <- local[i]:
				sent++
			default:
				// 전송 실패 → 나머지를 pending으로 복원
				b.mu.Lock()
				b.pending = append(local[i:], b.pending...)
				local = local[:0] // local 비우기 (모두 pending으로 이동)
				b.mu.Unlock()
				goto nextLoop
			}
		}
		// 모두 전송 성공 → local 비우기
		local = local[:0]
	nextLoop:
		b.mu.Lock()
	}
}

// Dequeue: 단일 소비자 채널
func (b *EventBus[T]) Dequeue() <-chan T { return b.out }

// Close: 생산 중단 → pending 스냅샷 JSONL 저장 → out close
func (b *EventBus[T]) Close() {
	if b.closed.Swap(true) {
		return
	}

	// 메모리 전용 모드: 단순히 채널만 닫기
	if b.mode == MemoryOnlyMode {
		close(b.out)
		return
	}

	// 영구 저장 모드: 기존 방식
	b.mu.Lock()
	b.stopping = true
	b.cv.Broadcast() // 대기 중 전부 깨워서 종료 경로 진입
	b.mu.Unlock()
	b.wg.Wait()
}

// Len: 현재 pending 큐 길이(정확값). 내부 락으로 보호됨.
func (b *EventBus[T]) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mode == MemoryOnlyMode {
		return len(b.out)
	}
	return len(b.pending)
}

// Cap: 설정된 capLimit을 반환. 0 또는 음수면 '무제한' 의미.
func (b *EventBus[T]) Cap() int {
	if b.mode == MemoryOnlyMode {
		return cap(b.out)
	}
	return b.capLimit
}

// ---- JSONL helpers ----

func loadBacklogJSONL[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	dec := json.NewDecoder(r)
	var items []T
	for {
		var v T
		if err := dec.Decode(&v); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		items = append(items, v)
	}
	return items, nil
}

func saveBacklogJSONL[T any](path string, items []T) error {
	if len(items) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(w)
	for _, v := range items {
		if err := enc.Encode(&v); err != nil {
			return err
		}
	}
	return w.Flush()
}
