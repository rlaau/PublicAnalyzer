package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkaLib "github.com/segmentio/kafka-go"
)

// KafkaBatchConsumer 배치 컨슈머 제너릭 구현체
type KafkaBatchConsumer[T any] struct {
	reader    *kafkaLib.Reader
	batchSize int
	//카프카의 타임아웃은 리얼타임 타임아웃 기반으로 해야 의미가 있음
	//더이상 입력이 없는 상황에서도 리얼타임 듀레이션 기반으로 요청을 빼내는 것임
	batchTimeout time.Duration
}

// KafkaBatchProducer 배치 프로듀서 제너릭 구현체
type KafkaBatchProducer[T any] struct {
	writer       *kafkaLib.Writer
	batchSize    int
	batchTimeout time.Duration
}

type KafkaBatchConfig struct {
	Brokers      []string
	Topic        string
	GroupID      string
	BatchSize    int
	BatchTimeout time.Duration
}

// NewKafkaBatchConsumer 제너릭 배치 컨슈머 생성
func NewKafkaBatchConsumer[T any](config KafkaBatchConfig) *KafkaBatchConsumer[T] {
	// 글로벌 브로커 설정 업데이트
	if len(config.Brokers) > 0 {
		SetGlobalBrokers(config.Brokers)
	} else {
		config.Brokers = GetGlobalBrokers()
	}

	return &KafkaBatchConsumer[T]{
		reader: kafkaLib.NewReader(kafkaLib.ReaderConfig{
			Brokers:        config.Brokers,
			Topic:          config.Topic,
			GroupID:        config.GroupID,
			MinBytes:       1,
			MaxBytes:       10e6,                   // 큰 배치를 위한 최대 바이트
			CommitInterval: 100 * time.Millisecond, // 빠른 커밋
			StartOffset:    kafkaLib.LastOffset,
		}),
		batchSize:    config.BatchSize,
		batchTimeout: config.BatchTimeout,
	}
}

// ReadMessagesBatch 진정한 배치 읽기 (한 번에 여러 메시지)
func (c *KafkaBatchConsumer[T]) ReadMessagesBatch(ctx context.Context) ([]Message[T], error) {
	kafkaMessages := make([]kafkaLib.Message, 0, c.batchSize)

	// 첫 번째 메시지는 블로킹으로 기다림
	firstMsg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}
	kafkaMessages = append(kafkaMessages, firstMsg)

	// 나머지 메시지들은 논블로킹으로 수집 (진정한 배칭!)
	timeoutCtx, cancel := context.WithTimeout(ctx, c.batchTimeout)
	defer cancel()

	for len(kafkaMessages) < c.batchSize {
		select {
		case <-timeoutCtx.Done():
			// 타임아웃 도달하면 현재까지 수집된 배치 반환
			goto convertMessages
		default:
			// 논블로킹으로 추가 메시지 읽기 시도
			readCtx, readCancel := context.WithTimeout(ctx, 1*time.Millisecond)
			msg, err := c.reader.ReadMessage(readCtx)
			readCancel()

			if err != nil {
				// 에러 발생시 현재까지 수집된 배치 반환
				goto convertMessages
			}
			kafkaMessages = append(kafkaMessages, msg)
		}
	}

convertMessages:
	// kafkaLib.Message를 Message[T]로 변환
	messages := make([]Message[T], 0, len(kafkaMessages))
	for _, kafkaMsg := range kafkaMessages {
		// JSON 역직렬화
		var value T
		if err := json.Unmarshal(kafkaMsg.Value, &value); err != nil {
			// 파싱 실패한 메시지는 스킵
			continue
		}
		messages = append(messages, Message[T]{
			Key:   kafkaMsg.Key,
			Value: value,
		})
	}

	return messages, nil
}

// ReadMessage 단건 호환성 (Consumer 인터페이스 구현)
func (c *KafkaBatchConsumer[T]) ReadMessage(ctx context.Context) ([]byte, T, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		var zero T
		return nil, zero, err
	}

	// JSON 역직렬화
	var value T
	if err := json.Unmarshal(msg.Value, &value); err != nil {
		var zero T
		return nil, zero, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return msg.Key, value, nil
}

// SetBatchConfig 런타임 배치 설정 변경
func (c *KafkaBatchConsumer[T]) SetBatchConfig(batchSize int, timeout time.Duration) error {
	c.batchSize = batchSize
	c.batchTimeout = timeout
	return nil
}

// Close BatchConsumer 종료
func (c *KafkaBatchConsumer[T]) Close() error {
	return c.reader.Close()
}

// NewKafkaBatchProducer 제너릭 배치 프로듀서 생성
func NewKafkaBatchProducer[T any](config KafkaBatchConfig) *KafkaBatchProducer[T] {
	if config.BatchSize <= 0 {
		config.BatchSize = 10_000
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 200 * time.Millisecond
	}

	// 글로벌 브로커 설정 업데이트
	if len(config.Brokers) > 0 {
		SetGlobalBrokers(config.Brokers)
	} else {
		config.Brokers = GetGlobalBrokers()
	}

	return &KafkaBatchProducer[T]{
		writer: &kafkaLib.Writer{
			Addr:         kafkaLib.TCP(config.Brokers...),
			Topic:        config.Topic,
			Balancer:     &kafkaLib.Hash{},
			RequiredAcks: kafkaLib.RequireOne,
			Async:        true,                // 비동기 전송
			BatchSize:    config.BatchSize,    // 설정 가능한 배치 크기
			BatchTimeout: config.BatchTimeout, // 설정 가능한 타임아웃
			Compression:  kafkaLib.Snappy,     // 압축 활성화
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
		},
		batchSize:    config.BatchSize,
		batchTimeout: config.BatchTimeout,
	}
}

// PublishMessagesBatch 진정한 배치 전송 (한 번에 여러 메시지)
func (p *KafkaBatchProducer[T]) PublishMessagesBatch(ctx context.Context, messages []Message[T]) error {
	// Message[T]를 kafkaLib.Message로 변환
	kafkaMessages := make([]kafkaLib.Message, len(messages))
	for i, msg := range messages {
		data, err := json.Marshal(msg.Value)
		if err != nil {
			return fmt.Errorf("failed to marshal message %d: %w", i, err)
		}
		kafkaMessages[i] = kafkaLib.Message{
			Key:   msg.Key,
			Value: data,
		}
	}
	return p.writer.WriteMessages(ctx, kafkaMessages...)
}

// PublishMessage 단건 호환성 (Producer 인터페이스 구현)
func (p *KafkaBatchProducer[T]) PublishMessage(ctx context.Context, key []byte, value T) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafkaLib.Message{
		Key:   key,
		Value: data,
	})
}

// SetBatchConfig 런타임 배치 설정 변경
func (p *KafkaBatchProducer[T]) SetBatchConfig(batchSize int, timeout time.Duration) error {
	p.batchSize = batchSize
	p.batchTimeout = timeout
	// Writer 설정도 업데이트
	p.writer.BatchSize = batchSize
	p.writer.BatchTimeout = timeout
	return nil
}

// Close BatchProducer 종료
func (p *KafkaBatchProducer[T]) Close() error {
	return p.writer.Close()
}
