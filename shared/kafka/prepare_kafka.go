package kafka

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// PrepareKafkaConfigByDefalut는 기본 설정으로 카프카의 토픽을 생성하고 기다림
func PrepareKafkaConfigByDefalut(brokers []string, topic string) error {
	log.Println("Waiting for Kafka to be ready...")
	err := WaitForKafka(brokers, 30*time.Second)
	if err != nil {
		panic("카프카 열지 못함")
	}
	log.Printf("Creating topic '%s' with brokers: %v", topic, brokers)
	err = CreateTopicIfNotExists(brokers, topic, 1, 1)
	if err != nil {
		panic("카프카 토픽 생성 실패함")
	}
	return nil
}

// WaitForKafka Kafka가 준비될 때까지 대기
func WaitForKafka(brokers []string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for Kafka to be ready")
		case <-ticker.C:
			if isKafkaReady(brokers) {
				log.Println("Kafka is ready")
				return nil
			}
			log.Println("Waiting for Kafka to be ready...")
		}
	}
}

func isKafkaReady(brokers []string) bool {
	for _, broker := range brokers {
		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			log.Printf("dial fail %s: %v", broker, err)
			continue
		}
		_, err = conn.Brokers()
		_ = conn.Close()
		if err != nil {
			log.Printf("metadata fail %s: %v", broker, err)
			continue
		}
		return true
	}
	return false
}

// 내부 헬퍼: 토픽이 "사용 가능(파티션 존재 + 각 파티션 리더 배정)" 상태가 될 때까지 대기
func waitForTopicReady(brokers []string, topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// 여러 브로커 중 하나라도 준비 신호 주면 OK
		for _, b := range brokers {
			conn, err := kafka.Dial("tcp", b)
			if err != nil {
				continue
			}
			parts, err := conn.ReadPartitions(topic)
			_ = conn.Close()
			if err != nil || len(parts) == 0 {
				continue
			}
			ready := true
			for _, p := range parts {
				// 리더 정보가 비어있거나 미배정(-1)인 경우 준비 안됨
				if p.Leader.Host == "" || p.Leader.ID == -1 {
					ready = false
					break
				}
			}
			if ready {
				log.Printf("Topic %q is ready (partitions=%d)", topic, len(parts))
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for topic %q to be ready", topic)
}

// CreateTopicIfNotExists 개선된 토픽 생성 함수
// - 토픽이 없으면 생성 시도
// - 이미 있거나 생성 성공 후, "토픽 준비 상태"가 될 때까지 블로킹 (메타데이터/리더 전파 대기)
func CreateTopicIfNotExists(brokers []string, topic string, numPartitions int, replicationFactor int) error {
	// 먼저 "이미 존재/사용 가능"인지 빠르게 체크 (짧은 타임박스)
	if err := waitForTopicReady(brokers, topic, 2*time.Second); err == nil {
		log.Printf("Topic '%s' already exists and is ready", topic)
		return nil
	}

	// 연결 재시도 로직
	maxRetries := 3
	retryDelay := 2 * time.Second

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		lastErr = createTopicOnce(brokers, topic, numPartitions, replicationFactor)
		if lastErr == nil {
			log.Printf("Topic '%s' created successfully", topic)
			// 생성 직후 전파 대기 (여기가 핵심)
			if err := waitForTopicReady(brokers, topic, 60*time.Second); err != nil {
				return fmt.Errorf("topic created but not ready: %w", err)
			}
			return nil
		}

		// "이미 존재"로 보이면 존재로 처리 후 준비 대기
		// (kafka-go의 에러 문자열이 브로커/버전에 따라 다를 수 있어 안전하게 존재 확인)
		if err := waitForTopicReady(brokers, topic, 5*time.Second); err == nil {
			log.Printf("Topic '%s' already exists (detected after error: %v)", topic, lastErr)
			return nil
		}

		log.Printf("Attempt %d/%d failed to create topic '%s': %v", attempt, maxRetries, topic, lastErr)
		if attempt < maxRetries {
			log.Printf("Retrying in %v...", retryDelay)
			time.Sleep(retryDelay)
		}
	}

	return fmt.Errorf("failed to create topic '%s' after %d attempts: %v", topic, maxRetries, lastErr)
}

func createTopicOnce(brokers []string, topic string, numPartitions int, replicationFactor int) error {
	// 각 브로커에 대해 시도
	for _, broker := range brokers {
		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			log.Printf("Failed to connect to broker %s: %v", broker, err)
			continue
		}
		controller, err := conn.Controller()
		_ = conn.Close()
		if err != nil {
			log.Printf("Failed to get controller from %s: %v", broker, err)
			continue
		}

		controllerAddr := net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port))
		controllerConn, err := kafka.Dial("tcp", controllerAddr)
		if err != nil {
			log.Printf("Failed to connect to controller %s: %v", controllerAddr, err)
			continue
		}

		// 컨트롤러 연결은 이 스코프에서 닫힘
		func() {
			defer controllerConn.Close()
			topicConfigs := []kafka.TopicConfig{
				{
					Topic:             topic,
					NumPartitions:     numPartitions,
					ReplicationFactor: replicationFactor,
				},
			}
			err = controllerConn.CreateTopics(topicConfigs...)
		}()

		if err == nil {
			return nil
		}

		// 이미 존재할 수 있으므로 바로 존재/준비 확인
		if e := waitForTopicReady(brokers, topic, 3*time.Second); e == nil {
			return nil
		}

		log.Printf("CreateTopics on controller failed: %v", err)
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
