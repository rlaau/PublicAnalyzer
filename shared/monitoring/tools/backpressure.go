package tools

import (
	"log"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/meter/cntmtr"
)

type SpeedSignal struct {
	ProducingIntervalSecond int
	ProducingPerInterval    int
}

type CountingBackpressure interface {
	CountConsuming()
	CountConsumings(i int)
	CountProducing()
	CountProducings(i int)
	RegisterBackpressureChannel(c chan SpeedSignal)
	MakeBackpressureEvent()
	Start()
	Stop()
}

type KafkaCountingBackpressure struct {
	// 미터기들 - 프로듀서와 컨슈머만 사용
	producerMeter *cntmtr.IntCountMeter
	consumerMeter *cntmtr.IntCountMeter

	// 백프레셔가 받는 요구사항
	queueCapacity    int64   // 큐의 전체 용량
	targetSaturation float64 // 목표 포화도 (0.0 ~ 1.0)
	// 백프레셔가 요구사항 달성 위해 리턴시키는 값
	currentInterval  int // 현재 인터벌 (초)
	currentBatchSize int // 현재 배치 크기

	// 채널 관리
	signalChannels []chan SpeedSignal
	channelsMu     sync.RWMutex

	// 제어 관련
	//여기서 time은 시스템의 초당 처리량 관련이기에 진짜 time을 사용
	ticker   *time.Ticker
	stopChan chan struct{}
	stopOnce sync.Once // Stop 멱등성 보장
	running  bool
	runMu    sync.RWMutex
	// 속도 조절 상수
	strongIncreaseRate float64
	weakIncreaseRate   float64
	weakDecreaseRate   float64
	strongDecreaseRate float64
	// 배치 크기 제한
	maxBatchRatio float64 // 큐 용량 대비 최대 배치 크기 비율
}

// NewKafkaCountingBackpressure 새로운 백프레셔 인스턴스 생성
func NewKafkaCountingBackpressure(queueCapacity int64, targetSaturation float64, seedInterval int, seedBatchSize int) *KafkaCountingBackpressure {
	return &KafkaCountingBackpressure{
		producerMeter:      cntmtr.NewIntCountMeter(),
		consumerMeter:      cntmtr.NewIntCountMeter(),
		queueCapacity:      queueCapacity,
		targetSaturation:   targetSaturation,
		currentInterval:    seedInterval,
		currentBatchSize:   seedBatchSize,
		signalChannels:     make([]chan SpeedSignal, 0),
		stopChan:           make(chan struct{}),
		strongIncreaseRate: 1.5,
		weakIncreaseRate:   1.2,
		weakDecreaseRate:   0.8,
		strongDecreaseRate: 0.5,
		maxBatchRatio:      0.33, // 큐 용량의 33%
		running:            false,
	}
}

// CountConsuming 단일 컨슈밍 카운트
func (k *KafkaCountingBackpressure) CountConsuming() {
	k.consumerMeter.Increase()
}

// CountConsumings 다중 컨슈밍 카운트
func (k *KafkaCountingBackpressure) CountConsumings(i int) {
	if i > 0 {
		k.consumerMeter.Increases(uint(i))
	}
}

// CountProducing 단일 프로듀싱 카운트
func (k *KafkaCountingBackpressure) CountProducing() {
	k.producerMeter.Increase()
}

// CountProducings 다중 프로듀싱 카운트
func (k *KafkaCountingBackpressure) CountProducings(i int) {
	if i > 0 {
		k.producerMeter.Increases(uint(i))
	}
}

// RegisterBackpressureChannel 신호를 받을 채널 등록
func (k *KafkaCountingBackpressure) RegisterBackpressureChannel(c chan SpeedSignal) {
	k.channelsMu.Lock()
	defer k.channelsMu.Unlock()
	k.signalChannels = append(k.signalChannels, c)
}

// Start 백프레셔 모니터링 시작
func (k *KafkaCountingBackpressure) Start() {
	k.ticker = time.NewTicker(100 * time.Millisecond) // 0.1초마다 체크
	go k.monitorLoop()
}

// Stop 백프레셔 모니터링 중지 (멱등성 보장)
func (k *KafkaCountingBackpressure) Stop() {
	k.stopOnce.Do(func() {
		k.runMu.Lock()
		defer k.runMu.Unlock()

		if !k.running {
			return
		}

		close(k.stopChan)
		if k.ticker != nil {
			k.ticker.Stop()
		}
		k.running = false
	})
}

// monitorLoop 주기적으로 큐 상태를 모니터링하고 조정
func (k *KafkaCountingBackpressure) monitorLoop() {
	for {
		select {
		case <-k.ticker.C:
			k.normalizeMeters()
			k.MakeBackpressureEvent()
		case <-k.stopChan:
			return
		}
	}
}

// normalizeMeters 컨슈머 값을 빼서 미터기 정규화
func (k *KafkaCountingBackpressure) normalizeMeters() {
	consumed := k.consumerMeter.TotalSum()
	// 컨슈머 값이 0이면 정규화할 필요 없음
	if consumed == 0 {
		return
	}

	// 컨슈머 값만큼 양쪽에서 빼기
	// 걍 컨슈머는 consumed-consumed므로 0으로 설정
	k.consumerMeter.Set(0)
	k.producerMeter.Decreases(uint(consumed))

	// 프로듀서가 음수가 되면 오류 - 이론상 불가능
	if k.producerMeter.TotalSum() < 0 {
		log.Printf("ERROR: Producer meter became negative after normalization! Producer: %d, Consumer: %d",
			k.producerMeter.TotalSum(), consumed)
		// 음수 방지를 위해 0으로 설정
		k.producerMeter.Set(0)
	}
}

// MakeBackpressureEvent 백프레셔 이벤트 생성 및 전송
func (k *KafkaCountingBackpressure) MakeBackpressureEvent() {
	signal := k.decideSingnaling()

	// 신호가 변경되었을 때만 전송
	if signal.ProducingIntervalSecond != k.currentInterval ||
		signal.ProducingPerInterval != k.currentBatchSize {
		k.currentInterval = signal.ProducingIntervalSecond
		k.currentBatchSize = signal.ProducingPerInterval

		k.channelsMu.RLock()
		channels := k.signalChannels
		k.channelsMu.RUnlock()

		// 모든 등록된 채널에 신호 전송 (논블로킹)
		for _, ch := range channels {
			select {
			case ch <- signal:
			default:
				// 채널이 가득 찬 경우 스킵
			}
		}
	}
}

// decideSingnaling 현재 큐 상태를 기반으로 적절한 속도 결정
func (k *KafkaCountingBackpressure) decideSingnaling() SpeedSignal {
	// 프로듀서 미터가 곧 큐에 쌓인 메시지 수 (오프셋)
	queuedMessages := k.producerMeter.TotalSum()
	saturation := float64(queuedMessages) / float64(k.queueCapacity)

	newInterval := k.currentInterval
	newBatchSize := k.currentBatchSize

	// 포화도에 따른 속도 조절
	switch {
	case saturation < 0.1:
		// 매우 낮은 부하 - 강한 속도 증가
		newBatchSize = int(float64(k.currentBatchSize) * k.strongIncreaseRate)
		newInterval = int(float64(k.currentInterval) * 0.5)

	case saturation >= 0.1 && saturation < 0.3:
		// 낮은 부하 - 약한 속도 증가
		newBatchSize = int(float64(k.currentBatchSize) * k.weakIncreaseRate)
		newInterval = int(float64(k.currentInterval) * 0.8)
	case saturation >= 0.3 && saturation < 0.4:
		// 약간 낮은 부하 - 미세 증가
		newBatchSize = int(float64(k.currentBatchSize) * 1.1)
		newInterval = int(float64(k.currentInterval) * 0.9)
	case saturation >= 0.4 && saturation < 0.6:
		// 적정 부하 - 속도 유지
		// 목표 포화도 범위 내에 있으므로 변경 없음

	case saturation >= 0.6 && saturation < 0.7:
		// 약간 높은 부하 - 미세 감소
		newBatchSize = int(float64(k.currentBatchSize) * 0.9)
		newInterval = int(float64(k.currentInterval) * 1.1)

	case saturation >= 0.7 && saturation < 0.8:
		// 높은 부하 - 약한 속도 감소
		newBatchSize = int(float64(k.currentBatchSize) * k.weakDecreaseRate)
		newInterval = int(float64(k.currentInterval) * 1.5)

	case saturation >= 0.8:
		// 매우 높은 부하 - 강한 속도 감소
		newBatchSize = int(float64(k.currentBatchSize) * k.strongDecreaseRate)
		newInterval = int(float64(k.currentInterval) * 2.0)
	}

	// 목표 포화도에 더 가까워지도록 미세 조정
	targetOffset := int64(float64(k.queueCapacity) * k.targetSaturation)
	if queuedMessages > targetOffset {
		// 목표보다 높으면 추가 감속
		adjustment := float64(queuedMessages) / float64(targetOffset)
		if adjustment > 1.2 {
			newBatchSize = int(float64(newBatchSize) / adjustment)
		}
	}
	// 배치 크기 제한 (최소 1, 최대 큐 용량의 33%)
	maxBatch := int(float64(k.queueCapacity) * k.maxBatchRatio)
	if newBatchSize < 1 {
		newBatchSize = 1
	} else if newBatchSize > maxBatch {
		newBatchSize = maxBatch
	}
	// 배치 인터벌 제한
	// 아무리 배치를 빨리 한다 해도 1초보다 잦게는 하지 않음.
	if newInterval < 1 {
		newInterval = 1
	}

	return SpeedSignal{
		ProducingIntervalSecond: newInterval,
		ProducingPerInterval:    newBatchSize,
	}
}

// GetCurrentSaturation 현재 큐 포화도 반환 (디버깅용)
func (k *KafkaCountingBackpressure) GetCurrentSaturation() float64 {
	queuedMessages := k.producerMeter.TotalSum()
	return float64(queuedMessages) / float64(k.queueCapacity)
}

// GetQueueSize 현재 큐 크기 반환 (디버깅용)
func (k *KafkaCountingBackpressure) GetQueueSize() int64 {
	return k.producerMeter.TotalSum()
}

// GetMetersStatus 미터기 상태 반환 (디버깅용)
func (k *KafkaCountingBackpressure) GetMetersStatus() (producer int64, consumer int64) {
	return k.producerMeter.TotalSum(), k.consumerMeter.TotalSum()
}

// ResetMeters 모든 미터기 리셋 (테스트용)
func (k *KafkaCountingBackpressure) ResetMeters() {
	k.producerMeter.Reset()
	k.consumerMeter.Reset()
}

// IsRunning 모니터링 실행 중인지 확인
func (k *KafkaCountingBackpressure) IsRunning() bool {
	k.runMu.RLock()
	defer k.runMu.RUnlock()
	return k.running
}
