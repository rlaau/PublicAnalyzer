package kafka

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// CreateTopicIfNotExists 개선된 토픽 생성 함수
func CreateTopicIfNotExists(brokers []string, topic string, numPartitions int, replicationFactor int) error {
	// 연결 재시도 로직 추가
	maxRetries := 3
	retryDelay := 2 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := createTopicWithRetry(brokers, topic, numPartitions, replicationFactor)
		if err == nil {
			log.Printf("Topic '%s' created successfully", topic)
			return nil
		}

		// 이미 존재하는 토픽인 경우
		if err.Error() == "Topic with this name already exists" ||
			err.Error() == fmt.Sprintf("Topic '%s' already exists", topic) {
			log.Printf("Topic '%s' already exists", topic)
			return nil
		}

		log.Printf("Attempt %d/%d failed to create topic: %v", attempt, maxRetries, err)

		if attempt < maxRetries {
			log.Printf("Retrying in %v...", retryDelay)
			time.Sleep(retryDelay)
		}
	}

	return fmt.Errorf("failed to create topic after %d attempts", maxRetries)
}

func createTopicWithRetry(brokers []string, topic string, numPartitions int, replicationFactor int) error {
	// 각 브로커에 대해 시도
	for _, broker := range brokers {
		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			log.Printf("Failed to connect to broker %s: %v", broker, err)
			continue
		}
		defer conn.Close()

		// 컨트롤러 찾기
		controller, err := conn.Controller()
		if err != nil {
			log.Printf("Failed to get controller from %s: %v", broker, err)
			continue
		}

		// 컨트롤러에 연결
		controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
		if err != nil {
			log.Printf("Failed to connect to controller: %v", err)
			continue
		}
		defer controllerConn.Close()

		// 토픽 생성
		topicConfigs := []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     numPartitions,
				ReplicationFactor: replicationFactor,
			},
		}

		err = controllerConn.CreateTopics(topicConfigs...)
		if err != nil {
			// 이미 존재하는 경우는 성공으로 처리
			if err.Error() == "Topic with this name already exists" {
				return nil
			}
			log.Printf("Failed to create topic on controller: %v", err)
			continue
		}

		// 성공
		return nil
	}

	return fmt.Errorf("failed to create topic '%s' on any broker", topic)
}

// DeleteTopic 토픽 완전 삭제 (완전한 정리용)
func DeleteTopic(brokers []string, topic string) error {
	// broker 연결
	conn, err := kafka.DialContext(context.Background(), "tcp", brokers[0])
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

	log.Printf("✅ Topic '%s' completely deleted", topic)
	return nil
}
