package app

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/cce/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
)

func TestTestingIngester(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kafkaProducer := kafka.NewProducer([]string{"localhost:9092"}, "ingest-testing-tx")
	defer kafkaProducer.Close()

	// CCE 서비스 생성
	cceService := app.NewMockCCEService()
	ingester := NewTestingIngester(kafkaProducer, cceService)

	// Start ingesting in a goroutine
	go func() {
		err := ingester.IngestTransaction(ctx)
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	// Start consuming messages with reservoir sampling
	go func() {
		consumer := kafka.NewConsumer([]string{"localhost:9092"}, "ingest-testing-tx", "test-consumer-group")
		defer consumer.Close()
		
		const targetMessages = 10000    // 1만개 목표 (메모리 안정성)
		const reservoirSize = 1000      // 1천개 레저버 샘플링
		
		reservoir := make([]string, 0, reservoirSize)
		messageCount := 0
		rand.Seed(time.Now().UnixNano())

		log.Printf("Starting to consume messages from topic 'ingest-testing-tx'...")
		log.Printf("Target: %d messages, Reservoir size: %d", targetMessages, reservoirSize)
		
		for messageCount < targetMessages {
			select {
			case <-ctx.Done():
				log.Printf("Context canceled, consumed %d messages", messageCount)
				goto printSamples
			default:
			}

			_, value, err := consumer.ReadMessage(ctx)
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
				reservoir = append(reservoir, fmt.Sprintf("Msg_%d_Value_%s", messageCount, string(value)))
			} else {
				// Replace random element with probability k/n
				j := rand.Intn(messageCount)
				if j < reservoirSize {
					reservoir[j] = fmt.Sprintf("Msg_%d_Value_%s", messageCount, string(value))
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
	log.Printf("Test completed after 100 seconds")
}
