package app

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	cce "github.com/rlaaudgjs5638/chainAnalyzer/internal/cce/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/txingester"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/monitor/kfkmntr"
)

const IngesterTestingTopic string = "test-ingested-topic"

func TestTestingIngester(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kafkaConfig := kafka.KafkaBatchConfig{
		Brokers: []string{kafka.DefaultKafkaPort},
		Topic:   IngesterTestingTopic,
	}

	// Kafka가 준비될 때까지 대기
	log.Println("Waiting for Kafka to be ready...")
	err := kafka.WaitForKafka(kafkaConfig.Brokers, 30*time.Second)
	if err != nil {
		t.Fatalf("Kafka is not ready: %v", err)
	}

	// 약간의 추가 대기 시간
	time.Sleep(2 * time.Second)

	// 테스트 시작 전 토픽 생성
	//* 아직까진 이렇게 수동으로 create가 필요한듯
	log.Printf("Creating topic '%s' with brokers: %v", kafkaConfig.Topic, kafkaConfig.Brokers)
	err = kafka.CreateTopicIfNotExists(kafkaConfig.Brokers, kafkaConfig.Topic, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("Topic '%s' creation completed", kafkaConfig.Topic)

	// Producer 생성 전에 잠시 대기
	time.Sleep(1 * time.Second)

	kafkaProducer := kafka.NewKafkaProducer[domain.MarkedTransaction](kafkaConfig)
	defer kafkaProducer.Close()

	// CCE 서비스 생성
	cceService := cce.NewMockCCEService()

	// 모니터링 미터 생성
	kafkaMonitor := kfkmntr.NewKafkaMonitor()
	defer kafkaMonitor.Close()

	infra := txingester.NewInfrastructure(cceService, *kafkaMonitor, "", kafkaProducer, nil)
	testing := true
	ingester := NewTxIngester(testing, *infra)

	// Start ingesting in a goroutine
	go func() {
		err := ingester.IngestTransaction(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	// Consumer 생성 전에 잠시 대기
	time.Sleep(2 * time.Second)

	// Start consuming messages with reservoir sampling
	go func() {
		consumer := kafka.NewKafkaConsumer[domain.MarkedTransaction](
			[]string{kafka.DefaultKafkaPort},
			IngesterTestingTopic,
			"test-consumer-group",
		)
		defer consumer.Close()

		const targetMessages = 1000000
		const reservoirSize = 1000

		reservoir := make([]string, 0, reservoirSize)
		messageCount := 0
		rand.Seed(time.Now().UnixNano())

		log.Printf("Starting to consume messages from topic '%s'...", IngesterTestingTopic)
		log.Printf("Target: %d messages, Reservoir size: %d", targetMessages, reservoirSize)

		for messageCount < targetMessages {
			select {
			case <-ctx.Done():
				log.Printf("Context canceled, consumed %d messages", messageCount)
				goto printSamples
			default:
			}

			_, tx, err := consumer.ReadMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					break
				}
				// 에러 로깅 개선
				if err.Error() != "context deadline exceeded" {
					log.Printf("Error reading message: %v", err)
				}
				continue
			}

			messageCount++

			// Reservoir sampling algorithm
			if len(reservoir) < reservoirSize {
				reservoir = append(reservoir, fmt.Sprintf("Msg_%d_From_%s_To_%s",
					messageCount, tx.From.String()[:10], tx.To.String()[:10]))
			} else {
				j := rand.Intn(messageCount)
				if j < reservoirSize {
					reservoir[j] = fmt.Sprintf("Msg_%d_From_%s_To_%s",
						messageCount, tx.From.String()[:10], tx.To.String()[:10])
				}
			}

			if messageCount%10000 == 0 {
				log.Printf("Consumed %d messages so far... (Reservoir: %d/%d)",
					messageCount, len(reservoir), reservoirSize)
			}
		}

	printSamples:
		log.Printf("Total messages consumed: %d", messageCount)
		log.Printf("Final reservoir size: %d", len(reservoir))

		if len(reservoir) > 0 {
			sampleCount := 50
			if len(reservoir) < 50 {
				sampleCount = len(reservoir)
			}

			log.Printf("Printing %d random samples from reservoir:", sampleCount)
			for i := 0; i < sampleCount; i++ {
				idx := rand.Intn(len(reservoir))
				fmt.Printf("Sample %d: %s\n", i+1, reservoir[idx])
			}
		}
	}()

	// Let the test run for the full timeout duration
	<-ctx.Done()
	log.Printf("Test completed")
}
