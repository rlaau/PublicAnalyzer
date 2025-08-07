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
	Timestamp               time.Time // 신호 생성 시간
	CurrentSaturation       float64   // 현재 포화도
}

// CountingBackpressure 백프레셔 인터페이스
type CountingBackpressure interface {
	// 카운팅 메서드
	CountConsuming()
	CountConsumings(i int)
	CountProducing()
	CountProducings(i int)

	// Pull 방식: 클라이언트가 필요할 때 호출
	// 걍 얘가 젤 나음.
	GetNextSignal() SpeedSignal

	// Push 방식 (옵션): 여전히 채널 기반도 지원
	RegisterBackpressureChannel(c chan SpeedSignal)

	// 모니터링 제어
	Start()
	Stop()

	// 디버깅용
	GetCurrentSaturation() float64
	GetQueueSize() int64
	GetMetersStatus() (producer int64, consumer int64)
}

type KafkaCountingBackpressure struct {
	// 미터기들
	producerMeter *cntmtr.IntCountMeter
	consumerMeter *cntmtr.IntCountMeter

	// 백프레셔 설정
	queueCapacity    int64
	targetSaturation float64

	// 현재 상태 (Pull 방식에서는 캐싱용)
	currentInterval  int
	currentBatchSize int
	lastSignalTime   time.Time
	signalMu         sync.RWMutex

	// Push 방식용 채널 (옵션)
	signalChannels []chan SpeedSignal
	channelsMu     sync.RWMutex

	// 정규화용 타이머
	normalizeTicker *time.Ticker
	stopChan        chan struct{}
	stopOnce        sync.Once
	running         bool
	runMu           sync.RWMutex

	// 속도 조절 상수
	strongIncreaseRate float64
	weakIncreaseRate   float64
	weakDecreaseRate   float64
	strongDecreaseRate float64
	maxBatchRatio      float64

	// 적응형 조절을 위한 히스토리
	lastSaturation    float64
	saturationTrend   float64   // 포화도 변화 추세
	adjustmentHistory []float64 // 최근 조정 비율 히스토리
	historyMu         sync.RWMutex
}

// 클라이언트가 달성하길 원하는 채널 스펙
type RequestedQueueSpec struct {
	QueueCapacity    int64
	TargetSaturation float64
}

// 클라이언트가 처음으로 프로듀싱할 스펙
// 이거에다 가중치 곱하는 식으로 백프레셔는 점진적 최적화 달성
type SeedProducingConfig struct {
	SeedInterval  int
	SeedBatchSize int
}

// NewKafkaCountingBackpressure 새로운 백프레셔 인스턴스 생성
func NewKafkaCountingBackpressure(requestedQueueSpec RequestedQueueSpec, seedProducingConfig SeedProducingConfig) *KafkaCountingBackpressure {
	return &KafkaCountingBackpressure{
		producerMeter:      cntmtr.NewIntCountMeter(),
		consumerMeter:      cntmtr.NewIntCountMeter(),
		queueCapacity:      requestedQueueSpec.QueueCapacity,
		targetSaturation:   requestedQueueSpec.TargetSaturation,
		currentInterval:    seedProducingConfig.SeedInterval,
		currentBatchSize:   seedProducingConfig.SeedBatchSize,
		lastSignalTime:     time.Now(),
		signalChannels:     make([]chan SpeedSignal, 0),
		stopChan:           make(chan struct{}),
		strongIncreaseRate: 1.5,
		weakIncreaseRate:   1.2,
		weakDecreaseRate:   0.8,
		strongDecreaseRate: 0.5,
		maxBatchRatio:      0.30,
		adjustmentHistory:  make([]float64, 0, 10),
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

// GetNextSignal Pull 방식: 클라이언트가 호출하면 즉시 현재 상태 기반으로 신호 생성
func (k *KafkaCountingBackpressure) GetNextSignal() SpeedSignal {
	// 먼저 정규화
	k.normalizeMeters()

	// 현재 상태 기반으로 신호 계산
	signal := k.calculateSignal()

	// 상태 업데이트
	k.signalMu.Lock()
	k.currentInterval = signal.ProducingIntervalSecond
	k.currentBatchSize = signal.ProducingPerInterval
	k.lastSignalTime = signal.Timestamp
	k.signalMu.Unlock()

	// 트렌드 업데이트
	k.updateTrend(signal.CurrentSaturation)

	return signal
}

// calculateSignal 현재 상태를 기반으로 신호 계산
func (k *KafkaCountingBackpressure) calculateSignal() SpeedSignal {
	queuedMessages := k.producerMeter.TotalSum()
	saturation := float64(queuedMessages) / float64(k.queueCapacity)

	if k.queueCapacity == 0 {
		return SpeedSignal{
			ProducingIntervalSecond: 1,
			ProducingPerInterval:    1,
			Timestamp:               time.Now(),
			CurrentSaturation:       0,
		}
	}

	k.signalMu.RLock()
	newInterval := k.currentInterval
	newBatchSize := k.currentBatchSize
	k.signalMu.RUnlock()

	// 트렌드 기반 조정 팩터 계산
	trendFactor := k.calculateTrendFactor()

	// 포화도에 따른 속도 조절 (트렌드 반영)
	switch {
	case saturation < 0.1:
		// 매우 낮은 부하 - 강한 속도 증가
		newBatchSize = int(float64(newBatchSize) * k.strongIncreaseRate * trendFactor)
		newInterval = int(float64(newInterval) * 0.2)

	case saturation < 0.3:
		// 낮은 부하 - 약한 속도 증가
		newBatchSize = int(float64(newBatchSize) * k.weakIncreaseRate * trendFactor)
		newInterval = int(float64(newInterval) * 0.4)

	case saturation < 0.4:
		// 약간 낮은 부하 - 미세 증가
		newBatchSize = int(float64(newBatchSize) * 1.1 * trendFactor)
		newInterval = int(float64(newInterval) * 0.8)

	case saturation < 0.6:
		// 적정 부하 - 미세 조정만
		// 목표 포화도와의 차이에 따라 미세 조정
		deviation := saturation - k.targetSaturation
		if deviation > 0.05 {
			newBatchSize = int(float64(newBatchSize) * 0.95)
		} else if deviation < -0.05 {
			newBatchSize = int(float64(newBatchSize) * 1.05)
		}

	case saturation < 0.7:
		// 약간 높은 부하 - 미세 감소
		newBatchSize = int(float64(newBatchSize) * 0.9 / trendFactor)
		newInterval = int(float64(newInterval) * 1.5)

	case saturation < 0.8:
		// 높은 부하 - 약한 속도 감소
		newBatchSize = int(float64(newBatchSize) * k.weakDecreaseRate / trendFactor)
		newInterval = int(float64(newInterval) * 2.0)

	default: // saturation >= 0.8
		// 매우 높은 부하 - 강한 속도 감소
		newBatchSize = int(float64(newBatchSize) * k.strongDecreaseRate / trendFactor)
		newInterval = int(float64(newInterval) * 4.0)

		// 극도로 높은 포화도에서는 비상 조치
		if saturation > 0.95 {
			newBatchSize = int(float64(newBatchSize) * 0.3)
			newInterval = int(float64(newInterval) * 8.0)
		}
	}

	// 배치 크기 제한
	maxBatch := int(float64(k.queueCapacity) * k.maxBatchRatio)
	if newBatchSize < 1 {
		newBatchSize = 1
	} else if newBatchSize > maxBatch {
		newBatchSize = maxBatch
	}

	// 인터벌 제한
	if newInterval < 1 {
		newInterval = 1
	} else if newInterval > 60 {
		newInterval = 60
	}

	// 조정 히스토리 기록
	k.recordAdjustment(float64(newBatchSize) / float64(k.currentBatchSize))

	return SpeedSignal{
		ProducingIntervalSecond: newInterval,
		ProducingPerInterval:    newBatchSize,
		Timestamp:               time.Now(),
		CurrentSaturation:       saturation,
	}
}

// calculateTrendFactor 포화도 변화 추세를 기반으로 조정 팩터 계산
func (k *KafkaCountingBackpressure) calculateTrendFactor() float64 {
	k.historyMu.RLock()
	trend := k.saturationTrend
	k.historyMu.RUnlock()

	// 트렌드가 급격히 증가하면 더 강한 감소
	// 트렌드가 급격히 감소하면 더 강한 증가
	if trend > 0.1 { // 포화도가 빠르게 증가 중
		return 0.8 // 증가를 억제
	} else if trend < -0.1 { // 포화도가 빠르게 감소 중
		return 1.2 // 증가를 장려
	}
	return 1.0 // 중립
}

// updateTrend 포화도 변화 추세 업데이트
func (k *KafkaCountingBackpressure) updateTrend(currentSaturation float64) {
	k.historyMu.Lock()
	defer k.historyMu.Unlock()

	// 추세 계산 (지수 이동 평균)
	alpha := 0.3 // 스무딩 팩터
	trend := currentSaturation - k.lastSaturation
	k.saturationTrend = alpha*trend + (1-alpha)*k.saturationTrend
	k.lastSaturation = currentSaturation
}

// recordAdjustment 조정 비율 기록
func (k *KafkaCountingBackpressure) recordAdjustment(ratio float64) {
	k.historyMu.Lock()
	defer k.historyMu.Unlock()

	k.adjustmentHistory = append(k.adjustmentHistory, ratio)
	if len(k.adjustmentHistory) > 10 {
		k.adjustmentHistory = k.adjustmentHistory[1:]
	}
}

// normalizeMeters 컨슈머 값을 빼서 미터기 정규화
func (k *KafkaCountingBackpressure) normalizeMeters() {
	consumed := k.consumerMeter.TotalSum()

	if consumed == 0 {
		return
	}

	k.consumerMeter.Set(0)
	k.producerMeter.Decreases(uint(consumed))

	if k.producerMeter.TotalSum() < 0 {
		log.Printf("WARNING: Producer meter became negative after normalization: %d",
			k.producerMeter.TotalSum())
		k.producerMeter.Set(0)
	}
}

// RegisterBackpressureChannel Push 방식용 채널 등록 (옵션)
func (k *KafkaCountingBackpressure) RegisterBackpressureChannel(c chan SpeedSignal) {
	k.channelsMu.Lock()
	defer k.channelsMu.Unlock()
	k.signalChannels = append(k.signalChannels, c)
}

// Start 백그라운드 정규화 시작 (옵션)
func (k *KafkaCountingBackpressure) Start() {
	k.runMu.Lock()
	defer k.runMu.Unlock()

	if k.running {
		log.Println("KafkaCountingBackpressure: already running")
		return
	}

	// 정규화만 주기적으로 수행
	k.normalizeTicker = time.NewTicker(100 * time.Millisecond)
	k.running = true
	go k.backgroundLoop()
}

// backgroundLoop 백그라운드 작업 (정규화 및 옵션 푸시)
func (k *KafkaCountingBackpressure) backgroundLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("KafkaCountingBackpressure: recovered from panic: %v", r)
		}
	}()

	for {
		select {
		case <-k.normalizeTicker.C:
			k.normalizeMeters()

			// Push 방식 채널이 등록되어 있으면 신호 전송
			k.channelsMu.RLock()
			hasChannels := len(k.signalChannels) > 0
			k.channelsMu.RUnlock()

			if hasChannels {
				signal := k.calculateSignal()
				k.broadcastSignal(signal)
			}

		case <-k.stopChan:
			return
		}
	}
}

// broadcastSignal 등록된 채널에 신호 전송
func (k *KafkaCountingBackpressure) broadcastSignal(signal SpeedSignal) {
	k.channelsMu.RLock()
	channels := k.signalChannels
	k.channelsMu.RUnlock()

	for _, ch := range channels {
		select {
		case ch <- signal:
		default:
			// 논블로킹
		}
	}
}

// Stop 백그라운드 작업 중지
func (k *KafkaCountingBackpressure) Stop() {
	k.stopOnce.Do(func() {
		k.runMu.Lock()
		defer k.runMu.Unlock()

		if !k.running {
			return
		}

		close(k.stopChan)
		if k.normalizeTicker != nil {
			k.normalizeTicker.Stop()
		}
		k.running = false
	})
}

// GetCurrentSaturation 현재 큐 포화도 반환
func (k *KafkaCountingBackpressure) GetCurrentSaturation() float64 {
	if k.queueCapacity == 0 {
		return 0
	}
	return float64(k.producerMeter.TotalSum()) / float64(k.queueCapacity)
}

// GetQueueSize 현재 큐 크기 반환
func (k *KafkaCountingBackpressure) GetQueueSize() int64 {
	return k.producerMeter.TotalSum()
}

// GetMetersStatus 미터기 상태 반환
func (k *KafkaCountingBackpressure) GetMetersStatus() (producer int64, consumer int64) {
	return k.producerMeter.TotalSum(), k.consumerMeter.TotalSum()
}

// GetAdjustmentHistory 최근 조정 히스토리 반환 (디버깅용)
func (k *KafkaCountingBackpressure) GetAdjustmentHistory() []float64 {
	k.historyMu.RLock()
	defer k.historyMu.RUnlock()

	history := make([]float64, len(k.adjustmentHistory))
	copy(history, k.adjustmentHistory)
	return history
}

// ResetMeters 모든 미터기 리셋
func (k *KafkaCountingBackpressure) ResetMeters() {
	k.producerMeter.Reset()
	k.consumerMeter.Reset()

	k.historyMu.Lock()
	k.lastSaturation = 0
	k.saturationTrend = 0
	k.adjustmentHistory = k.adjustmentHistory[:0]
	k.historyMu.Unlock()
}
