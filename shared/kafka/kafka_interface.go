package kafka

import (
	"context"
	"time"
)

// Message 배치 전송을 위한 제너릭 메시지 구조체 (kafkaLib.Message 추상화)
type Message[T any] struct {
	Key   []byte
	Value T
}

// Producer Kafka 메시지 발행을 위한 제너릭 인터페이스
type Producer[T any] interface {
	PublishMessage(ctx context.Context, key []byte, value T) error
	Close() error
}

// Consumer Kafka 메시지 수신을 위한 제너릭 인터페이스
type Consumer[T any] interface {
	ReadMessage(ctx context.Context) ([]byte, T, error) // key, value, error
	Close() error
}

// BatchProducer 배치 프로듀서 제너릭 인터페이스
type BatchProducer[T any] interface {
	Producer[T] // 단건 호환성
	PublishMessagesBatch(ctx context.Context, messages []Message[T]) error
	SetBatchConfig(batchSize int, timeout time.Duration) error
}

// BatchConsumer 배치 컨슈머 제너릭 인터페이스
type BatchConsumer[T any] interface {
	Consumer[T] // 단건 호환성
	ReadMessagesBatch(ctx context.Context) ([]Message[T], error)
	SetBatchConfig(batchSize int, timeout time.Duration) error
}
