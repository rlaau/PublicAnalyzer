package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
)

const filePath string = "./test_storage"

// 경로 생성 쪽: OS 독립 & 안전
func ReturnIsolatedStorage(i int) string {
	return filepath.Join(filePath, strconv.Itoa(i))
}

// ExampleMessage 예시 메시지 타입
type ExampleMessage struct {
	ID        string    `json:"id"`
	Data      string    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

// ========================================
// 예시 1: Buffered 모드 (버퍼 사용)
// ========================================
func bufferedModeExample() {
	log.Println("=== BUFFERED MODE EXAMPLE ===")

	// 백프레셔 생성
	backpressure := tools.LoadKafkaCountingBackpressure(
		tools.RequestedQueueSpec{
			QueueCapacity:    10000,
			TargetSaturation: 0.5,
		},
		tools.SeedProducingConfig{
			SeedInterval:  1,
			SeedBatchSize: 100,
		},
		ReturnIsolatedStorage(1),
	)

	// Buffered 모드 프로듀서 생성
	producer := kafka.NewKafkaBatchProducerWithBackpressure[ExampleMessage](
		kafka.BackpressureProducerConfig{
			Brokers:          []string{"localhost:9092"},
			Topic:            "buffered-topic",
			Mode:             kafka.ModeBuffered, // 버퍼 모드
			InitialBatchSize: 100,
			InitialInterval:  1 * time.Second,
			MaxBufferSize:    100000,
			MaxBatchSize:     50000,
			MaxInterval:      30 * time.Second,
		},
		backpressure,
	)

	// 프로듀서 루프 시작 (백프레셔 기반 자동 전송)
	if err := producer.Start(); err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Stop()

	// 컨슈머 시뮬레이션 (별도 고루틴) - 실제 Kafka 컨슈머
	config := kafka.KafkaBatchConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "direct-topic",
		GroupID:      fmt.Sprintf("consumer-group-%s-%d", "direct-topic", time.Now().Unix()),
		BatchSize:    50,
		BatchTimeout: 500 * time.Millisecond,
	}
	// 컨슈머 시뮬레이션 - 실제 Kafka 컨슈머
	go runRealConsumer(backpressure, config, 15*time.Second)

	// 메시지 생성 및 버퍼에 추가
	for i := 0; i < 1000; i++ {
		msg := kafka.Message[ExampleMessage]{
			Key: []byte(fmt.Sprintf("key-%d", i)),
			Value: ExampleMessage{
				ID:        fmt.Sprintf("msg-%d", i),
				Data:      fmt.Sprintf("Buffered message %d", i),
				Timestamp: time.Now(),
			},
		}

		// 버퍼에 추가 (즉시 리턴, 프로듀서 루프가 백프레셔에 따라 처리)
		if err := producer.QueueMessages([]kafka.Message[ExampleMessage]{msg}); err != nil {
			log.Printf("Failed to queue message: %v", err)
		}

		if i%100 == 0 {
			log.Printf("Queued %d messages, buffer size: %d", i, producer.GetBufferSize())
		}
	}

	// 처리 대기
	time.Sleep(10 * time.Second)

	log.Printf("Buffered mode - Total produced: %d, Buffer remaining: %d",
		producer.GetTotalProduced(), producer.GetBufferSize())
}

// ========================================
// 예시 2: Direct 모드 (버퍼 없음, 채널 직접 사용)
// ========================================
func directModeExample() {
	log.Println("=== DIRECT MODE EXAMPLE ===")

	// 백프레셔 생성
	backpressure := tools.LoadKafkaCountingBackpressure(
		tools.RequestedQueueSpec{
			QueueCapacity:    5000,
			TargetSaturation: 0.4,
		},
		tools.SeedProducingConfig{
			SeedInterval:  2,
			SeedBatchSize: 50,
		},
		ReturnIsolatedStorage(2),
	)

	// Direct 모드 프로듀서 생성
	producer := kafka.NewKafkaBatchProducerWithBackpressure[ExampleMessage](
		kafka.BackpressureProducerConfig{
			Brokers:           []string{"localhost:9092"},
			Topic:             "direct-topic",
			Mode:              kafka.ModeDirect, // Direct 모드
			InitialBatchSize:  50,
			InitialInterval:   2 * time.Second,
			DirectChannelSize: 1000, // 채널 크기
			MaxBatchSize:      10000,
			MaxInterval:       10 * time.Second,
		},
		backpressure,
	)

	// 프로듀서 루프 시작 (백프레셔 기반 자동 전송)
	if err := producer.Start(); err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Stop()
	config := kafka.KafkaBatchConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "direct-topic",
		GroupID:      fmt.Sprintf("consumer-group-%s-%d", "direct-topic", time.Now().Unix()),
		BatchSize:    50,
		BatchTimeout: 500 * time.Millisecond,
	}
	// 컨슈머 시뮬레이션 - 실제 Kafka 컨슈머
	go runRealConsumer(backpressure, config, 15*time.Second)

	// 메시지 생성 및 채널로 전송
	for i := 0; i < 500; i++ {
		msg := kafka.Message[ExampleMessage]{
			Key: []byte(fmt.Sprintf("key-%d", i)),
			Value: ExampleMessage{
				ID:        fmt.Sprintf("msg-%d", i),
				Data:      fmt.Sprintf("Direct message %d", i),
				Timestamp: time.Now(),
			},
		}

		// Direct 채널로 전송 (프로듀서 루프가 백프레셔에 따라 처리)
		if err := producer.SendDirect([]kafka.Message[ExampleMessage]{msg}); err != nil {
			log.Printf("Failed to send: %v", err)
		}

		// 실제 컨슈머가 처리하므로 시뮬레이션 제거
		if i%50 == 0 {
			log.Printf("Sent %d messages, channel size: %d", i, producer.GetDirectChannelSize())
		}
	}

	// 처리 대기
	time.Sleep(10 * time.Second)

	log.Printf("Direct mode - Total produced: %d", producer.GetTotalProduced())
}

// ========================================
// 예시 3: 통합 Publish 메서드 사용
// ========================================
func unifiedPublishExample() {
	log.Println("=== UNIFIED PUBLISH EXAMPLE ===")

	// 백프레셔 생성
	backpressure := tools.LoadKafkaCountingBackpressure(
		tools.RequestedQueueSpec{
			QueueCapacity:    8000,
			TargetSaturation: 0.45,
		},
		tools.SeedProducingConfig{
			SeedInterval:  1,
			SeedBatchSize: 75,
		},
		ReturnIsolatedStorage(3),
	)

	// 두 개의 프로듀서 생성 (하나는 Buffered, 하나는 Direct)
	bufferedProducer := kafka.NewKafkaBatchProducerWithBackpressure[string](
		kafka.BackpressureProducerConfig{
			Brokers:          []string{"localhost:9092"},
			Topic:            "unified-topic-buffered",
			Mode:             kafka.ModeBuffered,
			InitialBatchSize: 75,
			InitialInterval:  1 * time.Second,
			MaxBufferSize:    50000,
		},
		backpressure,
	)

	directProducer := kafka.NewKafkaBatchProducerWithBackpressure[string](
		kafka.BackpressureProducerConfig{
			Brokers:           []string{"localhost:9092"},
			Topic:             "unified-topic-direct",
			Mode:              kafka.ModeDirect,
			InitialBatchSize:  75,
			InitialInterval:   1 * time.Second,
			DirectChannelSize: 5000,
		},
		backpressure,
	)

	// 두 프로듀서 모두 시작
	bufferedProducer.Start()
	directProducer.Start()
	defer bufferedProducer.Stop()
	defer directProducer.Stop()

	// 실제 컨슈머 2개 실행 (각 프로듀서 토픽에 대해)
	config := kafka.KafkaBatchConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "unified-topic-buffered",
		GroupID:      fmt.Sprintf("consumer-group-%s-%d", "unified-topic-buffered", time.Now().Unix()),
		BatchSize:    50,
		BatchTimeout: 500 * time.Millisecond,
	}
	// 컨슈머 시뮬레이션 - 실제 Kafka 컨슈머
	go runRealConsumer(backpressure, config, 15*time.Second)
	config2 := kafka.KafkaBatchConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "unified-topic-direct",
		GroupID:      fmt.Sprintf("consumer-group-%s-%d", "unified-topic-direct", time.Now().Unix()),
		BatchSize:    50,
		BatchTimeout: 500 * time.Millisecond,
	}
	// 컨슈머 시뮬레이션 - 실제 Kafka 컨슈머
	go runRealConsumer(backpressure, config2, 15*time.Second)

	// 동일한 Publish 메서드 사용 (모드에 따라 다르게 동작)
	for i := 0; i < 200; i++ {
		message := []kafka.Message[string]{
			{
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: fmt.Sprintf("Message %d", i),
			},
		}

		// Buffered 프로듀서: 내부적으로 QueueMessages 호출
		if err := bufferedProducer.Publish(message); err != nil {
			log.Printf("Buffered publish failed: %v", err)
		}

		// Direct 프로듀서: 내부적으로 SendDirect 호출
		if err := directProducer.Publish(message); err != nil {
			log.Printf("Direct publish failed: %v", err)
		}
	}

	time.Sleep(5 * time.Second)

	log.Printf("Buffered producer - Total: %d, Buffer: %d",
		bufferedProducer.GetTotalProduced(), bufferedProducer.GetBufferSize())
	log.Printf("Direct producer - Total: %d, Channel: %d",
		directProducer.GetTotalProduced(), directProducer.GetDirectChannelSize())
}

// ========================================
// 예시 4: 성능 비교
// ========================================
func performanceComparisonExample() {
	log.Println("=== PERFORMANCE COMPARISON ===")

	// 동일한 백프레셔 설정
	backpressureConfig := tools.RequestedQueueSpec{
		QueueCapacity:    20000,
		TargetSaturation: 0.5,
	}
	seedConfig := tools.SeedProducingConfig{
		SeedInterval:  1,
		SeedBatchSize: 200,
	}

	// Buffered 모드 테스트
	bufferedBackpressure := tools.LoadKafkaCountingBackpressure(backpressureConfig, seedConfig, ReturnIsolatedStorage(4))
	bufferedProducer := kafka.NewKafkaBatchProducerWithBackpressure[ExampleMessage](
		kafka.BackpressureProducerConfig{
			Brokers:          []string{"localhost:9092"},
			Topic:            "perf-buffered",
			Mode:             kafka.ModeBuffered,
			InitialBatchSize: 200,
			InitialInterval:  1 * time.Second,
			MaxBufferSize:    100000,
		},
		bufferedBackpressure,
	)

	// Direct 모드 테스트
	directBackpressure := tools.LoadKafkaCountingBackpressure(backpressureConfig, seedConfig, ReturnIsolatedStorage(5))
	directProducer := kafka.NewKafkaBatchProducerWithBackpressure[ExampleMessage](
		kafka.BackpressureProducerConfig{
			Brokers:           []string{"localhost:9092"},
			Topic:             "perf-direct",
			Mode:              kafka.ModeDirect,
			InitialBatchSize:  200,
			InitialInterval:   1 * time.Second,
			DirectChannelSize: 10000,
		},
		directBackpressure,
	)

	// 두 프로듀서 시작
	bufferedProducer.Start()
	directProducer.Start()
	defer bufferedProducer.Stop()
	defer directProducer.Stop()

	// 실제 컨슈머 시작
	var consumerWg sync.WaitGroup
	consumerWg.Add(2)
	config := kafka.KafkaBatchConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "perf-buffered",
		GroupID:      fmt.Sprintf("consumer-group-%s-%d", "perf-buffered", time.Now().Unix()),
		BatchSize:    50,
		BatchTimeout: 500 * time.Millisecond,
	}
	// Buffered 토픽 컨슈머
	go func() {
		defer consumerWg.Done()
		runRealConsumer(bufferedBackpressure, config, 20*time.Second)
	}()
	config2 := kafka.KafkaBatchConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "perf-direct",
		GroupID:      fmt.Sprintf("consumer-group-%s-%d", "perf-direct", time.Now().Unix()),
		BatchSize:    50,
		BatchTimeout: 500 * time.Millisecond,
	}
	// Direct 토픽 컨슈머
	go func() {
		defer consumerWg.Done()
		runRealConsumer(directBackpressure, config2, 20*time.Second)
	}()

	// 동시에 대량 메시지 생성
	messageCount := 10000
	var wg sync.WaitGroup

	// Buffered 모드 성능 테스트
	wg.Add(1)
	go func() {
		defer wg.Done()
		start := time.Now()

		for i := 0; i < messageCount; i++ {
			bufferedProducer.PublishSingle(
				[]byte(fmt.Sprintf("b-key-%d", i)),
				ExampleMessage{
					ID:   fmt.Sprintf("b-msg-%d", i),
					Data: "Buffered test data",
				},
			)
		}

		elapsed := time.Since(start)
		log.Printf("[Buffered] Queued %d messages in %v", messageCount, elapsed)
	}()

	// Direct 모드 성능 테스트
	wg.Add(1)
	go func() {
		defer wg.Done()
		start := time.Now()

		for i := 0; i < messageCount; i++ {
			directProducer.PublishSingle(
				[]byte(fmt.Sprintf("d-key-%d", i)),
				ExampleMessage{
					ID:   fmt.Sprintf("d-msg-%d", i),
					Data: "Direct test data",
				},
			)
		}

		elapsed := time.Since(start)
		log.Printf("[Direct] Sent %d messages in %v", messageCount, elapsed)
	}()

	wg.Wait()

	// 전송 완료 대기
	time.Sleep(15 * time.Second)

	// 결과 비교
	log.Println("=== FINAL RESULTS ===")
	log.Printf("Buffered: Produced=%d, Errors=%d, Buffer=%d",
		bufferedProducer.GetTotalProduced(),
		bufferedProducer.GetTotalErrors(),
		bufferedProducer.GetBufferSize())
	log.Printf("Direct: Produced=%d, Errors=%d, Channel=%d",
		directProducer.GetTotalProduced(),
		directProducer.GetTotalErrors(),
		directProducer.GetDirectChannelSize())
}

// 실제 컨슈머 헬퍼 (컨텍스트 포함)
func runRealConsumer(backpressure tools.CountingBackpressure, config kafka.KafkaBatchConfig, duration time.Duration) {

	consumer := kafka.NewKafkaBatchConsumerWithBackpressure[ExampleMessage](config, backpressure)
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, err := consumer.ReadMessagesBatch(ctx)
			if err != nil {
				if err == context.DeadlineExceeded || err == context.Canceled {
					continue
				}
				// 에러 무시하고 계속
				time.Sleep(100 * time.Millisecond)
				continue
			}

		}
	}
}

// ========================================
// 메인 함수
// ========================================
func main() {
	examples := []struct {
		name string
		fn   func()
	}{
		{"Buffered Mode", bufferedModeExample},
		{"Direct Mode", directModeExample},
		{"Unified Publish", unifiedPublishExample},
		{"Performance Comparison", performanceComparisonExample},
	}

	for _, example := range examples {
		log.Printf(">>> Running: %s", example.name)
		example.fn()
		log.Println("---")
		time.Sleep(3 * time.Second)
	}
}
