package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
	kafkaLib "github.com/segmentio/kafka-go"
)

// ProducerMode 프로듀서 동작 모드
type ProducerMode int

const (
	// ModeBuffered 버퍼를 사용하는 모드
	ModeBuffered ProducerMode = iota
	// ModeDirect 버퍼 없이 즉시 처리 모드
	ModeDirect
)

// KafkaBatchProducerWithBackpressure 백프레셔가 통합된 배치 프로듀서
type KafkaBatchProducerWithBackpressure[T any] struct {
	// 기본 카프카 writer
	writer *kafkaLib.Writer

	// 백프레셔
	backpressure tools.CountingBackpressure

	// 설정
	mode         ProducerMode
	topic        string
	maxBatchSize int           // 백프레셔가 제안하는 최대 배치 크기 제한
	maxInterval  time.Duration // 백프레셔가 제안하는 최대 인터벌 제한

	// 메시지 버퍼 (ModeBuffered에서만 사용)
	messageBuffer []Message[T]
	bufferMu      sync.Mutex

	// Direct 모드용 메시지 채널
	directChannel chan []Message[T]

	// 제어
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   bool
	runningMu sync.RWMutex

	// 통계
	totalProduced uint64
	totalErrors   uint64
	statsMu       sync.RWMutex
}

// BackpressureProducerConfig 백프레셔 프로듀서 설정
type BackpressureProducerConfig struct {
	// Kafka 설정
	Brokers []string
	Topic   string

	// 동작 모드
	Mode ProducerMode

	// 백프레셔 설정 (시드값)
	InitialBatchSize  int
	InitialInterval   time.Duration
	MaxBufferSize     int // 내부 버퍼 최대 크기 (ModeBuffered에서만 사용)
	DirectChannelSize int // Direct 모드 채널 크기

	// 제한값
	MaxBatchSize int           // 백프레셔가 제안해도 이 값을 초과하지 않음
	MaxInterval  time.Duration // 백프레셔가 제안해도 이 값을 초과하지 않음
}

// NewKafkaBatchProducerWithBackpressure 백프레셔 통합 프로듀서 생성
func NewKafkaBatchProducerWithBackpressure[T any](
	config BackpressureProducerConfig,
	backpressure tools.CountingBackpressure,
) *KafkaBatchProducerWithBackpressure[T] {
	// 기본값 설정
	if config.InitialBatchSize <= 0 {
		config.InitialBatchSize = 100
	}
	if config.InitialInterval <= 0 {
		config.InitialInterval = 1 * time.Second
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 10000
	}
	if config.MaxInterval <= 0 {
		config.MaxInterval = 60 * time.Second
	}
	if config.Mode == ModeBuffered && config.MaxBufferSize <= 0 {
		config.MaxBufferSize = 100000
	}
	if config.Mode == ModeDirect && config.DirectChannelSize <= 0 {
		config.DirectChannelSize = 1000
	}

	// 글로벌 브로커 설정
	if len(config.Brokers) > 0 {
		SetGlobalBrokers(config.Brokers)
	} else {
		config.Brokers = GetGlobalBrokers()
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Kafka가 준비될 때까지 대기
	log.Println("Waiting for Kafka to be ready...")
	err := WaitForKafka(config.Brokers, 30*time.Second)
	if err != nil {
		panic("카프카 열지 못함")
	}
	log.Printf("Creating topic '%s' with brokers: %v", config.Topic, config.Brokers)
	err = CreateTopicIfNotExists(config.Brokers, config.Topic, 1, 1)
	if err != nil {
		panic("카프카 토픽 생성 실패함")
	}
	log.Printf("Topic '%s' creation completed", config.Topic)
	p := &KafkaBatchProducerWithBackpressure[T]{
		writer: &kafkaLib.Writer{
			Addr:         kafkaLib.TCP(config.Brokers...),
			Topic:        config.Topic,
			Balancer:     &kafkaLib.Hash{},
			RequiredAcks: kafkaLib.RequireOne,
			Async:        false, // 백프레셔 카운팅 정확도를 위해 동기식
			Compression:  kafkaLib.Snappy,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
		},
		backpressure: backpressure,
		mode:         config.Mode,
		topic:        config.Topic,
		maxBatchSize: config.MaxBatchSize,
		maxInterval:  config.MaxInterval,
		ctx:          ctx,
		cancel:       cancel,
		running:      false,
	}

	// 모드별 초기화
	if config.Mode == ModeBuffered {
		p.messageBuffer = make([]Message[T], 0, config.MaxBufferSize)
	} else {
		p.directChannel = make(chan []Message[T], config.DirectChannelSize)
	}

	return p
}

// Start 백프레셔 기반 프로듀서 루프 시작
func (p *KafkaBatchProducerWithBackpressure[T]) Start() error {
	p.runningMu.Lock()
	defer p.runningMu.Unlock()

	if p.running {
		return fmt.Errorf("producer already running")
	}

	p.running = true
	p.wg.Add(1)

	if p.mode == ModeBuffered {
		go p.bufferedProducerLoop()
		log.Printf("KafkaBatchProducerWithBackpressure started in Buffered mode for topic: %s", p.topic)
	} else {
		go p.directProducerLoop()
		log.Printf("KafkaBatchProducerWithBackpressure started in Direct mode for topic: %s", p.topic)
	}

	return nil
}

// bufferedProducerLoop Buffered 모드 프로듀서 루프
func (p *KafkaBatchProducerWithBackpressure[T]) bufferedProducerLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			// 종료 시 남은 메시지 모두 전송
			p.flushBuffer()
			return
		default:
			// 백프레셔로부터 다음 신호 받기
			signal := p.backpressure.GetNextSignal()

			// 신호 제한 적용
			batchSize := signal.ProducingPerInterval
			if batchSize > p.maxBatchSize {
				batchSize = p.maxBatchSize
			}

			interval := time.Duration(signal.ProducingIntervalSecond) * time.Second
			if interval > p.maxInterval {
				interval = p.maxInterval
			}

			// 디버깅 로그
			log.Printf("[Buffered] Backpressure signal - Batch: %d, Interval: %v, Saturation: %.2f%%",
				batchSize, interval, signal.CurrentSaturation*100)

			// 버퍼에서 메시지 가져오기
			messages := p.extractFromBuffer(batchSize)

			if len(messages) > 0 {
				// Kafka로 전송
				if err := p.publishBatch(messages); err != nil {
					log.Printf("Failed to publish batch: %v", err)
					p.incrementErrors()
					// 실패한 메시지를 버퍼 앞쪽에 다시 넣기
					p.reinsertToBuffer(messages)
				} else {
					// 성공적으로 전송됨 - 백프레셔에 알림
					p.backpressure.CountProducings(len(messages))
					p.incrementProduced(uint64(len(messages)))

					log.Printf("[Buffered] Successfully published %d messages", len(messages))
				}
			}

			// 인터벌 대기
			select {
			case <-time.After(interval):
				// 정상 대기
			case <-p.ctx.Done():
				// 종료 신호
				p.flushBuffer()
				return
			}
		}
	}
}

// directProducerLoop Direct 모드 프로듀서 루프
func (p *KafkaBatchProducerWithBackpressure[T]) directProducerLoop() {
	defer p.wg.Done()

	var pendingMessages []Message[T]

	for {
		// 백프레셔로부터 다음 신호 받기
		signal := p.backpressure.GetNextSignal()

		// 신호 제한 적용
		batchSize := signal.ProducingPerInterval
		if batchSize > p.maxBatchSize {
			batchSize = p.maxBatchSize
		}

		interval := time.Duration(signal.ProducingIntervalSecond) * time.Second
		if interval > p.maxInterval {
			interval = p.maxInterval
		}

		log.Printf("[Direct] Backpressure signal - Batch: %d, Interval: %v, Saturation: %.2f%%",
			batchSize, interval, signal.CurrentSaturation*100)

		// 타임아웃 설정
		timer := time.NewTimer(interval)

		// 메시지 수집
	collectLoop:
		for len(pendingMessages) < batchSize {
			select {
			case <-p.ctx.Done():
				timer.Stop()
				// 종료 시 남은 메시지 전송
				if len(pendingMessages) > 0 {
					p.publishBatch(pendingMessages)
					p.backpressure.CountProducings(len(pendingMessages))
				}
				p.flushDirectChannel()
				return

			case messages := <-p.directChannel:
				// 새 메시지 수신
				pendingMessages = append(pendingMessages, messages...)

			case <-timer.C:
				// 타임아웃 - 현재까지 수집된 메시지로 전송
				break collectLoop
			}
		}

		timer.Stop()

		// 배치 크기만큼 전송
		if len(pendingMessages) > 0 {
			sendBatch := pendingMessages
			if len(sendBatch) > batchSize {
				sendBatch = pendingMessages[:batchSize]
				pendingMessages = pendingMessages[batchSize:]
			} else {
				pendingMessages = nil
			}

			// Kafka로 전송
			if err := p.publishBatch(sendBatch); err != nil {
				log.Printf("Failed to publish batch: %v", err)
				p.incrementErrors()
				// Direct 모드에서는 재시도하지 않음 (데이터 손실 가능)
			} else {
				// 성공적으로 전송됨 - 백프레셔에 알림
				p.backpressure.CountProducings(len(sendBatch))
				p.incrementProduced(uint64(len(sendBatch)))

				log.Printf("[Direct] Successfully published %d messages", len(sendBatch))
			}
		}
	}
}

// Publish 메시지 발행 (모드에 따라 다르게 동작)
func (p *KafkaBatchProducerWithBackpressure[T]) Publish(messages []Message[T]) error {
	if p.mode == ModeBuffered {
		return p.QueueMessages(messages)
	} else {
		return p.SendDirect(messages)
	}
}

// PublishSingle 단일 메시지 발행
func (p *KafkaBatchProducerWithBackpressure[T]) PublishSingle(key []byte, value T) error {
	return p.Publish([]Message[T]{{Key: key, Value: value}})
}

// QueueMessages 메시지를 버퍼에 추가 (ModeBuffered용)
func (p *KafkaBatchProducerWithBackpressure[T]) QueueMessages(messages []Message[T]) error {
	if p.mode != ModeBuffered {
		return fmt.Errorf("QueueMessages is only available in Buffered mode")
	}

	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()

	// 버퍼 오버플로우 체크
	if len(p.messageBuffer)+len(messages) > cap(p.messageBuffer) {
		return fmt.Errorf("buffer overflow: cannot add %d messages, buffer capacity exceeded", len(messages))
	}

	p.messageBuffer = append(p.messageBuffer, messages...)

	return nil
}

// SendDirect Direct 모드로 메시지 전송 (채널에 추가)
func (p *KafkaBatchProducerWithBackpressure[T]) SendDirect(messages []Message[T]) error {
	if p.mode != ModeDirect {
		return fmt.Errorf("SendDirect is only available in Direct mode")
	}

	if !p.IsRunning() {
		return fmt.Errorf("producer is not running")
	}

	select {
	case p.directChannel <- messages:
		return nil
	case <-p.ctx.Done():
		return fmt.Errorf("producer is shutting down")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout: direct channel is full")
	}
}

// extractFromBuffer 버퍼에서 지정된 수만큼 메시지 추출 (ModeBuffered용)
func (p *KafkaBatchProducerWithBackpressure[T]) extractFromBuffer(count int) []Message[T] {
	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()

	// 버퍼가 비어있으면 조금 대기 (조건 변수 사용하지 않고 단순하게)
	if len(p.messageBuffer) == 0 {
		p.bufferMu.Unlock()
		time.Sleep(100 * time.Millisecond)
		p.bufferMu.Lock()

		// 여전히 비어있으면 nil 반환
		if len(p.messageBuffer) == 0 {
			return nil
		}
	}

	// 추출할 메시지 수 결정
	extractCount := count
	if extractCount > len(p.messageBuffer) {
		extractCount = len(p.messageBuffer)
	}

	if extractCount == 0 {
		return nil
	}

	// 메시지 추출
	messages := make([]Message[T], extractCount)
	copy(messages, p.messageBuffer[:extractCount])

	// 버퍼에서 제거
	p.messageBuffer = p.messageBuffer[extractCount:]

	return messages
}

// reinsertToBuffer 실패한 메시지를 버퍼 앞쪽에 다시 삽입 (ModeBuffered용)
func (p *KafkaBatchProducerWithBackpressure[T]) reinsertToBuffer(messages []Message[T]) {
	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()

	// 앞쪽에 삽입
	p.messageBuffer = append(messages, p.messageBuffer...)
}

// publishBatch Kafka로 배치 전송
func (p *KafkaBatchProducerWithBackpressure[T]) publishBatch(messages []Message[T]) error {
	if len(messages) == 0 {
		return nil
	}

	kafkaMessages := make([]kafkaLib.Message, len(messages))

	for i, msg := range messages {
		data, err := json.Marshal(msg.Value)
		if err != nil {
			return fmt.Errorf("failed to marshal message %d: %w", i, err)
		}
		kafkaMessages[i] = kafkaLib.Message{
			Key:   msg.Key,
			Value: data,
		}
	}

	ctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
	defer cancel()

	return p.writer.WriteMessages(ctx, kafkaMessages...)
}

// flushBuffer 버퍼의 모든 메시지 전송 (ModeBuffered용)
func (p *KafkaBatchProducerWithBackpressure[T]) flushBuffer() {
	for {
		messages := p.extractFromBuffer(p.maxBatchSize)
		if len(messages) == 0 {
			break
		}

		if err := p.publishBatch(messages); err != nil {
			log.Printf("Failed to flush messages: %v", err)
			p.incrementErrors()
			// 플러시 중 실패는 재시도하지 않음 (종료 중이므로)
			break
		} else {
			p.backpressure.CountProducings(len(messages))
			p.incrementProduced(uint64(len(messages)))
			log.Printf("Flushed %d messages during shutdown", len(messages))
		}
	}
}

// flushDirectChannel Direct 채널의 모든 메시지 전송
func (p *KafkaBatchProducerWithBackpressure[T]) flushDirectChannel() {
	close(p.directChannel)

	var allMessages []Message[T]
	for messages := range p.directChannel {
		allMessages = append(allMessages, messages...)
	}

	if len(allMessages) > 0 {
		// 배치로 나누어 전송
		for i := 0; i < len(allMessages); i += p.maxBatchSize {
			end := i + p.maxBatchSize
			if end > len(allMessages) {
				end = len(allMessages)
			}

			batch := allMessages[i:end]
			if err := p.publishBatch(batch); err != nil {
				log.Printf("Failed to flush direct messages: %v", err)
				p.incrementErrors()
			} else {
				p.backpressure.CountProducings(len(batch))
				p.incrementProduced(uint64(len(batch)))
				log.Printf("Flushed %d direct messages during shutdown", len(batch))
			}
		}
	}
}

// Stop 프로듀서 중지
func (p *KafkaBatchProducerWithBackpressure[T]) Stop() error {
	p.runningMu.Lock()
	if !p.running {
		p.runningMu.Unlock()
		return nil
	}
	p.running = false
	p.runningMu.Unlock()

	log.Printf("Stopping KafkaBatchProducerWithBackpressure (%v mode) for topic: %s", p.mode, p.topic)

	// 컨텍스트 취소로 고루틴 종료 신호
	p.cancel()

	// 모든 고루틴 종료 대기
	p.wg.Wait()

	// Writer 종료
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	log.Printf("KafkaBatchProducerWithBackpressure stopped. Total produced: %d, Total errors: %d",
		p.GetTotalProduced(), p.GetTotalErrors())

	return nil
}

// GetMode 현재 동작 모드 반환
func (p *KafkaBatchProducerWithBackpressure[T]) GetMode() ProducerMode {
	return p.mode
}

// GetBufferSize 현재 버퍼 크기 반환 (ModeBuffered에서만 유효)
func (p *KafkaBatchProducerWithBackpressure[T]) GetBufferSize() int {
	if p.mode != ModeBuffered {
		return 0
	}
	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()
	return len(p.messageBuffer)
}

// GetDirectChannelSize Direct 채널의 현재 크기 반환
func (p *KafkaBatchProducerWithBackpressure[T]) GetDirectChannelSize() int {
	if p.mode != ModeDirect {
		return 0
	}
	return len(p.directChannel)
}

// GetTotalProduced 총 전송된 메시지 수
func (p *KafkaBatchProducerWithBackpressure[T]) GetTotalProduced() uint64 {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return p.totalProduced
}

// GetTotalErrors 총 에러 수
func (p *KafkaBatchProducerWithBackpressure[T]) GetTotalErrors() uint64 {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return p.totalErrors
}

// incrementProduced 전송 카운트 증가
func (p *KafkaBatchProducerWithBackpressure[T]) incrementProduced(count uint64) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.totalProduced += count
}

// incrementErrors 에러 카운트 증가
func (p *KafkaBatchProducerWithBackpressure[T]) incrementErrors() {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.totalErrors++
}

// IsRunning 실행 중인지 확인
func (p *KafkaBatchProducerWithBackpressure[T]) IsRunning() bool {
	p.runningMu.RLock()
	defer p.runningMu.RUnlock()
	return p.running
}
