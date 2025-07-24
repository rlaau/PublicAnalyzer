package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	kafkaLib "github.com/segmentio/kafka-go"
)

// KafkaProducer Producer 제너릭 구현체
type KafkaProducer[T any] struct {
	writer *kafkaLib.Writer
}

// KafkaConsumer Consumer 제너릭 구현체
type KafkaConsumer[T any] struct {
	reader *kafkaLib.Reader
}

// NewKafkaConsumer 제너릭 Consumer 생성 (성능 최적화)
func NewKafkaConsumer[T any](brokers []string, topic string, groupID string) *KafkaConsumer[T] {
	return &KafkaConsumer[T]{
		reader: kafkaLib.NewReader(kafkaLib.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,                      // 최소 바이트 (즉시 처리)
			MaxBytes:       10e6,                   // 10MB 까지 한번에 읽기
			CommitInterval: 100 * time.Millisecond, // 커밋 간격 단축
			StartOffset:    kafkaLib.LastOffset,    // 최신 메시지부터 읽기
		}),
	}
}

// ReadMessage 메시지 수신
func (c *KafkaConsumer[T]) ReadMessage(ctx context.Context) ([]byte, T, error) {
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

// Close Consumer 종료
func (c *KafkaConsumer[T]) Close() error {
	return c.reader.Close()
}

// NewKafkaProducer 제너릭 Producer 생성 (고성능 비동기 모드)
// 아 걍 나중에 리팩토링해
func NewKafkaProducer[T any](config KafkaBatchConfig) *KafkaProducer[T] {
	if config.BatchSize <= 0 {
		config.BatchSize = 10_000
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 20 * time.Millisecond
	}
	return &KafkaProducer[T]{
		writer: &kafkaLib.Writer{
			Addr:         kafkaLib.TCP(config.Brokers...),
			Topic:        config.Topic,
			Balancer:     &kafkaLib.Hash{}, // Hash balancer (더 균등한 분배)
			RequiredAcks: kafkaLib.RequireOne,
			Async:        true,                // 비동기 전송 (로컬 환경 안전)
			BatchSize:    config.BatchSize,    // 대용량 배치 (10K 메시지)
			BatchTimeout: config.BatchTimeout, // 매우 짧은 타임아웃 (지연 최소화)
			Compression:  kafkaLib.Snappy,     // 압축 활성화
			ReadTimeout:  2 * time.Second,     // 읽기 타임아웃 단축
			WriteTimeout: 2 * time.Second,     // 쓰기 타임아웃 단축
		},
	}
}

// PublishMessage 메시지 발행
func (p *KafkaProducer[T]) PublishMessage(ctx context.Context, key []byte, value T) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafkaLib.Message{
		Key:   key,
		Value: data,
	})
}

// Close Producer 종료
func (p *KafkaProducer[T]) Close() error {
	return p.writer.Close()
}

// topicCache 토픽 존재 여부 캐시
var topicCache = make(map[string]bool)

// CreateTopicIfNotExists 토픽이 존재하지 않으면 생성 (캐싱 최적화)
func CreateTopicIfNotExists(brokers []string, topic string, partitions int, replicationFactor int) error {
	// 캐시 확인
	if topicCache[topic] {
		return nil
	}

	// broker 연결 (타임아웃 설정)
	conn, err := kafkaLib.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	// 토픽 존재 확인 (빠른 체크)
	partitionResp, err := conn.ReadPartitions(topic)
	if err == nil && len(partitionResp) > 0 {
		topicCache[topic] = true // 캐시에 저장
		return nil
	}

	// 토픽 생성
	topicConfigs := []kafkaLib.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
		},
	}

	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		// 토픽이 이미 존재하는 경우의 에러는 무시
		if err.Error() != "Topic already exists" {
			return fmt.Errorf("failed to create topic '%s': %w", topic, err)
		}
	}

	topicCache[topic] = true // 성공적으로 생성되었으므로 캐시에 저장
	log.Printf("✅ Topic '%s' ensured with %d partitions", topic, partitions)
	return nil
}

// EnsureKafkaConnection Kafka 연결 상태 확인
func EnsureKafkaConnection(brokers []string) error {
	for _, broker := range brokers {
		conn, err := net.DialTimeout("tcp", broker, 5*time.Second)
		if err != nil {
			return fmt.Errorf("failed to connect to broker %s: %w", broker, err)
		}
		conn.Close()
	}
	log.Printf("✅ Kafka brokers are reachable: %v", brokers)
	return nil
}

// CleanupTopic 토픽 데이터 정리 (테스트용) - offset 진행만
// CleanupTopicComplete 토픽 완전 삭제 후 재생성 (깨끗한 초기화)
func CleanupTopicComplete(brokers []string, topic string, partitions int, replicationFactor int) error {
	// 1. 토픽 삭제
	err := DeleteTopic(brokers, topic)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	// 2. Kafka가 삭제를 완료할 시간을 줌
	time.Sleep(2 * time.Second)

	// 3. 토픽 재생성
	err = CreateTopicIfNotExists(brokers, topic, partitions, replicationFactor)
	if err != nil {
		return fmt.Errorf("failed to recreate topic: %w", err)
	}

	log.Printf("✅ Topic '%s' completely cleaned and recreated", topic)
	return nil
}
func CleanupTopic(brokers []string, topic string) error {
	// 토픽의 모든 메시지를 읽어서 offset을 최신으로 만들어 정리 효과
	reader := kafkaLib.NewReader(kafkaLib.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "cleanup-group-" + topic,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 사용 가능한 모든 메시지를 빠르게 읽어서 offset 진행
	for {
		_, err := reader.ReadMessage(ctx)
		if err != nil {
			// timeout이나 EOF면 정리 완료
			break
		}
	}

	return nil
}

// DeleteTopic 토픽 완전 삭제 (완전한 정리용)
func DeleteTopic(brokers []string, topic string) error {
	// broker 연결
	conn, err := kafkaLib.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	// 토픽 존재 확인
	_, err = conn.ReadPartitions(topic)
	if err != nil {
		// 토픽이 존재하지 않으면 이미 삭제된 것으로 간주
		log.Printf("⚠️ Topic '%s' does not exist, skipping deletion", topic)
		return nil
	}

	// 토픽 삭제
	err = conn.DeleteTopics(topic)
	if err != nil {
		return fmt.Errorf("failed to delete topic '%s': %w", topic, err)
	}

	// 캐시에서도 제거
	delete(topicCache, topic)

	log.Printf("✅ Topic '%s' completely deleted", topic)
	return nil
}
