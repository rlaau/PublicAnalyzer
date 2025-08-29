// Package eventbus: SPSC(다중생산자→단일소비자) 이벤트 버스
// - 내부: pending []T + sync.Cond + out <-chan T(1개)
// - 시작 시 JSONL 로드→삭제 / 종료 시 pending JSONL 저장 (동기)
// - busy-wait 없음 (Cond 기반 대기)
// - 제네릭 T, 직렬화는 JSON 고정
package eventbus

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// EventBus[T] : 단일 소비자용 이벤트 버스
type EventBus[T any] struct {
	out      chan T
	mu       sync.Mutex
	cv       *sync.Cond
	pending  []T
	capLimit int

	filePath string // JSONL 저장 경로

	stopping bool
	closed   atomic.Bool
	wg       sync.WaitGroup
}

// NewWithPath: filePath(JSONL)와 capLimit로 버스 생성.
// - 시작 시 filePath의 JSONL을 로드해 pending에 적재, 로드 후 파일 삭제.
// - capLimit>0이면 Publish가 cap을 넘을 때 cond wait로 역압.
func NewWithPath[T any](filePath string, capLimit int) (*EventBus[T], error) {
	b := &EventBus[T]{
		out:      make(chan T),
		filePath: filePath,
		capLimit: capLimit,
	}
	b.cv = sync.NewCond(&b.mu)

	// 1) 시작 시 backlog 로드(동기)
	backlog, err := loadBacklogJSONL[T](filePath)
	if err != nil {
		return nil, err
	}
	_ = os.Remove(filePath) // 중복 로드 방지

	// 2) 런 루프 시작
	b.wg.Add(1)
	go b.run(backlog)
	return b, nil
}

// NewWithRoot: 루트 디렉토리 + 상대 경로로 JSONL 경로를 구성해서 생성
// 예) rootResolver() == "/tmp/test"; rel == "analyzer_pool/eventbus/cc.jsonl"
func NewWithRoot[T any](rootResolver func() string, rel string, capLimit int) (*EventBus[T], error) {
	root := rootResolver()
	fp := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(fp), 0o755); err != nil {
		return nil, err
	}
	return NewWithPath[T](fp, capLimit)
}

func (b *EventBus[T]) run(backlog []T) {
	defer b.wg.Done()
	b.mu.Lock()
	b.pending = append(b.pending, backlog...)
	for {
		// 대기: pending 없고, stop 아니면 cond wait
		for !b.stopping && len(b.pending) == 0 {
			b.cv.Wait()
		}
		if b.stopping {
			// 남은 것 저장
			rest := append([]T(nil), b.pending...)
			b.mu.Unlock()
			if err := saveBacklogJSONL(b.filePath, rest); err != nil {
				fmt.Printf("[eventbus] saveBacklog error: %v\n", err)
			}
			close(b.out)
			return
		}
		// 하나 pop & 생산자 역압 해제
		v := b.pending[0]
		b.pending = b.pending[1:]
		b.cv.Signal() // capLimit로 블록 중인 생산자 깨우기
		b.mu.Unlock()

		// out 으로 블로킹 전송
		b.out <- v

		// 다음 루프 준비
		b.mu.Lock()
	}
}

// Publish: 생산자 → pending. capLimit 초과 시 cond wait로 역압.
func (b *EventBus[T]) Publish(v T) error {
	if b.closed.Load() {
		return errors.New("eventbus: closed")
	}
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
	b.mu.Lock()
	defer b.mu.Unlock()

	for !b.stopping && b.capLimit > 0 && len(b.pending)+len(vs) > b.capLimit {
		b.cv.Wait()
	}
	if b.stopping {
		return errors.New("eventbus: stopping")
	}
	b.pending = append(b.pending, vs...)
	// 한 번만 깨워도 드레인 루프가 계속 처리함
	b.cv.Signal()
	return nil
}

// Dequeue: 단일 소비자 채널
func (b *EventBus[T]) Dequeue() <-chan T { return b.out }

// Close: 생산 중단 → pending 스냅샷 JSONL 저장 → out close
func (b *EventBus[T]) Close() {
	if b.closed.Swap(true) {
		return
	}
	b.mu.Lock()
	b.stopping = true
	b.cv.Broadcast() // 대기 중 전부 깨워서 종료 경로 진입
	b.mu.Unlock()
	b.wg.Wait()
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
