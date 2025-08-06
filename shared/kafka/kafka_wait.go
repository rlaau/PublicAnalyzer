package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

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
			continue
		}

		// 메타데이터 확인
		_, err = conn.Brokers()
		conn.Close()

		if err == nil {
			return true
		}
	}
	return false
}
