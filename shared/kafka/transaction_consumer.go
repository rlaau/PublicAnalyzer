package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dto"
)

// TransactionConsumer 트랜잭션 수신을 위한 인터페이스
type TransactionConsumer interface {
	Start(ctx context.Context, txChannel chan<- *shareddomain.MarkedTransaction) error
	Close() error
}

// KafkaTransactionConsumer Kafka 기반 트랜잭션 수신기 (제너릭)
type KafkaTransactionConsumer struct {
	consumer Consumer
	topic    string
	isTest   bool // 테스트 모드 여부
}

// NewKafkaTransactionConsumer Kafka 트랜잭션 수신기 생성
func NewKafkaTransactionConsumer(brokers []string, isTestMode bool, groupID string) *KafkaTransactionConsumer {
	var topic string
	if isTestMode {
		topic = dto.FedTxTopic // "fed-tx" - 테스트용 토픽
	} else {
		topic = dto.IngestedTransactionsTopic // "ingested-transactions" - 프로덕션용 토픽
	}

	consumer := NewConsumer(brokers, topic, groupID)

	return &KafkaTransactionConsumer{
		consumer: consumer,
		topic:    topic,
		isTest:   isTestMode,
	}
}

// Start Kafka 메시지 수신 시작
func (k *KafkaTransactionConsumer) Start(ctx context.Context, txChannel chan<- *shareddomain.MarkedTransaction) error {
	log.Printf("📡 Starting Kafka transaction consumer for topic: %s", k.topic)

	// 토픽 존재 확인 및 생성 (consumer는 읽기만 하므로 빈 토픽이라도 생성)
	brokers := []string{"localhost:9092"} // consumer에서 브로커 정보 가져오기
	if err := CreateTopicIfNotExists(brokers, k.topic, 1, 1); err != nil {
		log.Printf("⚠️ Failed to ensure topic exists: %v", err)
		// consumer는 토픽이 나중에 생성될 수도 있으므로 계속 진행
	}

	go k.consumeMessages(ctx, txChannel)
	return nil
}

// consumeMessages 메시지 수신 루프 (최적화됨)
func (k *KafkaTransactionConsumer) consumeMessages(ctx context.Context, txChannel chan<- *shareddomain.MarkedTransaction) {
	readTimeout := 100 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			log.Printf("📡 Kafka consumer stopping (context)")
			return
		default:
			// 짧은 타임아웃으로 Kafka에서 메시지 수신
			readCtx, cancel := context.WithTimeout(ctx, readTimeout)
			_, value, err := k.consumer.ReadMessage(readCtx)
			cancel()

			if err != nil {
				// Context timeout은 정상, 다른 에러는 로그
				if err != context.DeadlineExceeded {
					// 에러 로그 빈도 제한
					continue
				}
				continue
			}

			// 트랜잭션 파싱 (에러 처리 최적화)
			tx, err := k.parseMessage(value)
			if err != nil {
				// 파싱 에러는 건너뛰고 계속
				continue
			}

			// 트랜잭션을 채널로 전달 (non-blocking)
			select {
			case txChannel <- tx:
				// 성공
			case <-ctx.Done():
				return
			default:
				log.Printf("⚠️ - Transaction dropped - - channel full") // 채널이 가득 찬 경우 드롭 (로그 빈도 제한)
				// 채널이 풀이면 드롭 (로그 빈도 제한)
			}
		}
	}
}

// parseMessage 메시지 타입에 따른 트랜잭션 파싱 (제너릭 처리)
func (k *KafkaTransactionConsumer) parseMessage(value []byte) (*shareddomain.MarkedTransaction, error) {
	if k.isTest {
		// 테스트 모드: FedTxMessage 처리
		var message dto.FedTxMessage
		if err := json.Unmarshal(value, &message); err != nil {
			return nil, fmt.Errorf("failed to unmarshal FedTxMessage: %w", err)
		}
		return message.Transaction, nil
	} else {
		// 프로덕션 모드: IngestedTransactionMessage 처리
		var message dto.IngestedTransactionMessage
		if err := json.Unmarshal(value, &message); err != nil {
			return nil, fmt.Errorf("failed to unmarshal IngestedTransactionMessage: %w", err)
		}
		return message.Transaction, nil
	}
}

// Close 리소스 정리
func (k *KafkaTransactionConsumer) Close() error {
	if k.consumer != nil {
		return k.consumer.Close()
	}
	return nil
}
