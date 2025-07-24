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


// Producer Kafka 메시지 발행을 위한 인터페이스
type Producer interface {
	PublishMessage(ctx context.Context, key []byte, value any) error
	Close() error
}

// Consumer Kafka 메시지 수신을 위한 인터페이스
type Consumer interface {
	ReadMessage(ctx context.Context) ([]byte, []byte, error) // key, value, error
	Close() error
}

// KafkaProducer Producer 인터페이스 구현체
type KafkaProducer struct {
	writer *kafkaLib.Writer
}

// KafkaConsumer Consumer 인터페이스 구현체
type KafkaConsumer struct {
	reader *kafkaLib.Reader
}

// NewProducer Producer 생성 (고성능 비동기 모드)
func NewProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafkaLib.Writer{
			Addr:         kafkaLib.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafkaLib.Hash{},  // Hash balancer (더 균등한 분배)
			RequiredAcks: kafkaLib.RequireOne,
			Async:        true,              // 비동기 전송 (로컬 환경 안전)
			BatchSize:    5000,              // 대용량 배치 (5K 메시지)
			BatchTimeout: 10 * time.Millisecond, // 매우 짧은 타임아웃 (지연 최소화)
			Compression:  kafkaLib.Snappy,   // 압축 활성화
			ReadTimeout:  2 * time.Second,   // 읽기 타임아웃 단축
			WriteTimeout: 2 * time.Second,   // 쓰기 타임아웃 단축
		},
	}
}

// PublishMessage 메시지 발행
func (p *KafkaProducer) PublishMessage(ctx context.Context, key []byte, value any) error {
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
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

// NewConsumer Consumer 생성 (성능 최적화)
func NewConsumer(brokers []string, topic string, groupID string) *KafkaConsumer {
	return &KafkaConsumer{
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
func (c *KafkaConsumer) ReadMessage(ctx context.Context) ([]byte, []byte, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, nil, err
	}
	return msg.Key, msg.Value, nil
}

// Close Consumer 종료
func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
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

// CleanupTopic 토픽 데이터 정리 (테스트용)
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
