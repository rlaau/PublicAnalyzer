// Package eventbus: SPSC(다중생산자→다중소비자) 이벤트 버스
// 정확히 말하면 컨슈머는 단일 채널이긴 한데, 병렬로 채널에서 읽어도 문제가 없음
// - 내부: pending []T + sync.Cond + out <-chan T(1개)
// - 시작 시 JSONL 로드→삭제 / 종료 시 pending JSONL 저장 (동기)
// - busy-wait 없음 (Cond 기반 대기)
// - 제네릭 T, 직렬화는 JSON 고정
package eventbus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

// EventBusMode EventBus 동작 모드
type EventBusMode int

const (
	PersistentMode EventBusMode = iota // JSONL 저장/로드 지원
	MemoryOnlyMode                     // 메모리 전용, 채널 직접 사용
)

// EventBus[T] : 단일 소비자용 이벤트 버스
type EventBus[T any] struct {
	out chan T
	mu  sync.Mutex
	cv  *sync.Cond

	capLimit int
	name     gface.ModuleName
	stopping bool
	closed   atomic.Bool
	wg       sync.WaitGroup
}

//TODO 상태 저장 가능하도록 "종료"시 백로그 저장되게 하기
//*다만 save, load기능이 있는 이벤트버스는 락을 잘 걸어야 함. 콘슈밍 중 종료 시 남은 것들만 정확히 기록 필요

// NewInMemory: 단순한 메모리 전용 EventBus (채널 기반)
// JSONL, condition variable, waitgroup 없는 간단한 구조
// 워커풀이나 인터페이스 타입용으로 적합
func NewInMemory[T any](bufferSize int, name gface.ModuleName) *EventBus[T] {
	if bufferSize <= 0 {
		bufferSize = 1000 // 기본 버퍼 크기
	}

	return &EventBus[T]{
		out:      make(chan T, bufferSize),
		name:     name,
		capLimit: 0, // 무제한
	}
}

func (b *EventBus[T]) Name() gface.ModuleName {
	return b.name
}
func (b *EventBus[T]) Closed() bool {
	return b.closed.Load()
}

// Close: 생산 중단 → pending 스냅샷 JSONL 저장 → out close
func (b *EventBus[T]) Close() error {
	if b.closed.Swap(true) {
		return nil
	}

	// 메모리 전용 모드: 단순히 채널만 닫기
	close(b.out)
	return nil
}

// Enqueue: 생산자 → pending. capLimit 초과 시 cond wait로 역압.
func (b *EventBus[T]) Enqueue(ctx context.Context, v T) error {
	if b.closed.Load() {
		return errors.New("eventbus: closed")
	}

	// ✅ 메모리 전용 모드: 블로킹 send (대기하면서 흘러가게)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.out <- v:
		return nil
	}

}

func (b *EventBus[T]) EnqueueBatch(vs []T) error {
	if b.closed.Load() {
		return errors.New("eventbus: closed")
	}

	// ✅ 메모리 전용 모드: 채널로 순차 송신 (블로킹)
	for i := range vs {
		b.out <- vs[i] // 공간 생길 때까지 대기
	}
	return nil
}

// Dequeue: 단일 소비자 채널
func (b *EventBus[T]) Dequeue() <-chan T { return b.out }

// Len: 현재 pending 큐 길이(정확값). 내부 락으로 보호됨.
func (b *EventBus[T]) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.out)

}

// Cap: 설정된 capLimit을 반환. 0 또는 음수면 '무제한' 의미.
func (b *EventBus[T]) Cap() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cap(b.out)
}
