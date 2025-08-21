package app

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

// ======== 2) JSONL 동기 백업 버스 (out 하나만 노출) ========
// Multi writer(appender), single reader
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

func newSimpleBus[T any](filePath string, capLimit int) (*EventBus[T], error) {
	b := &EventBus[T]{
		out:      make(chan T),
		filePath: filePath,
		capLimit: capLimit,
	}
	b.cv = sync.NewCond(&b.mu)

	// 1) 시작 시 JSONL 로드 (동기)
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

func (b *EventBus[T]) run(backlog []T) {
	defer b.wg.Done()
	b.mu.Lock()
	b.pending = append(b.pending, backlog...)
	for {
		// 대기: pending 없고, stop 아니면 cond wait
		// 이 for루프는 착한? for loop이란 거. 걍 조건문이라 보면 됨.
		//무한 반복을 위한 조건문이 아니라, 신호 통해서 wait로 꺠어난 경우 다시 for의 조건문 한번 더 검증하기 위함임.
		for !b.stopping && len(b.pending) == 0 {
			b.cv.Wait()
		}
		if b.stopping {
			// 남은 것 저장
			rest := append([]T(nil), b.pending...)
			b.mu.Unlock()
			if err := saveBacklogJSONL(b.filePath, rest); err != nil {
				fmt.Printf("[bus] saveBacklog error: %v", err)
			}
			close(b.out)
			return
		}
		// 하나 팝 & 뮤텍스 잠깐 풀고 out 전송 (생산자 역압 해제 위해 시그널)
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

// Publish: 생산자 → pending. capLimit 초과 시 cond wait 로 역압.
func (b *EventBus[T]) Publish(v T) error {
	if b.closed.Load() {
		return errors.New("bus closed")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for !b.stopping && b.capLimit > 0 && len(b.pending) >= b.capLimit {
		b.cv.Wait()
	}
	if b.stopping {
		return errors.New("bus stopping")
	}
	b.pending = append(b.pending, v)
	b.cv.Signal()
	return nil
}

func (b *EventBus[T]) Dequeue() <-chan T { return b.out }

func (b *EventBus[T]) Close() {
	if b.closed.Swap(true) {
		return
	}
	b.mu.Lock()
	b.stopping = true
	b.cv.Broadcast()
	b.mu.Unlock()
	b.wg.Wait()
}

// ---- JSONL helpers

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

// ======== 3) Kafka Tx Fan-out (사용자 제공 Consumer 사용) ========
