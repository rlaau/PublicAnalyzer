package app

import (
	"context"

	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	cce "github.com/rlaaudgjs5638/chainAnalyzer/internal/cce/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring"
)

func TestTestingIngester(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	kafkaConfig := kafka.KafkaBatchConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "ingested-tx",
	}

	// 테스트 시작 전 토픽 명시적 생성
	log.Printf("Creating topic '%s' with brokers: %v", kafkaConfig.Topic, kafkaConfig.Brokers)
	err := kafka.CreateTopicIfNotExists(kafkaConfig.Brokers, kafkaConfig.Topic, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("Topic '%s' creation completed", kafkaConfig.Topic)

	kafkaProducer := kafka.NewKafkaProducer[domain.MarkedTransaction](kafkaConfig)
	defer kafkaProducer.Close()

	// CCE 서비스 생성
	cceService := cce.NewMockCCEService()
	
	// 모니터링 미터 생성
	consumerMonitor := monitoring.NewSimpleTPSMeter()
	producerMonitor := monitoring.NewSimpleTPSMeter()
	defer consumerMonitor.Close()
	defer producerMonitor.Close()
	
	ingester := NewTestingIngester(kafkaProducer, cceService, consumerMonitor, producerMonitor)

	// Start ingesting in a goroutine
	go func() {
		err := ingester.IngestTransaction(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	// Start consuming messages with reservoir sampling
	go func() {
		consumer := kafka.NewKafkaConsumer[domain.MarkedTransaction]([]string{"localhost:9092"}, "ingested-tx", "test-consumer-group")
		defer consumer.Close()

		const targetMessages = 1000000 // 100만개 목표 (메모리 안정성)
		const reservoirSize = 1000   // 1천개 레저버 샘플링

		reservoir := make([]string, 0, reservoirSize)
		messageCount := 0
		rand.Seed(time.Now().UnixNano())

		log.Printf("Starting to consume messages from topic 'ingested-tx'...")
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
				log.Printf("Error reading message: %v", err)
				continue
			}

			messageCount++

			// Reservoir sampling algorithm
			if len(reservoir) < reservoirSize {
				// Fill the reservoir initially
				reservoir = append(reservoir, fmt.Sprintf("Msg_%d_From_%s_To_%s", messageCount, tx.From.String()[:10], tx.To.String()[:10]))
			} else {
				// Replace random element with probability k/n
				j := rand.Intn(messageCount)
				if j < reservoirSize {
					reservoir[j] = fmt.Sprintf("Msg_%d_From_%s_To_%s", messageCount, tx.From.String()[:10], tx.To.String()[:10])
				}
			}

			if messageCount%50000 == 0 {
				log.Printf("Consumed %d messages so far... (Reservoir: %d/%d)",
					messageCount, len(reservoir), reservoirSize)
			}
		}

	printSamples:
		log.Printf("Total messages consumed: %d", messageCount)
		log.Printf("Final reservoir size: %d", len(reservoir))

		// Print 50 random samples from reservoir
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
	log.Printf("Test completed after 10 seconds")
}
