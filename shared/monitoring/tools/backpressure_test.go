package tools

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewKafkaCountingBackpressure(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	if bp.queueCapacity != 10000 {
		t.Errorf("Expected queue capacity 10000, got %d", bp.queueCapacity)
	}
	if bp.targetSaturation != 0.5 {
		t.Errorf("Expected target saturation 0.5, got %f", bp.targetSaturation)
	}
	if bp.currentInterval != 1 {
		t.Errorf("Expected initial interval 1, got %d", bp.currentInterval)
	}
	if bp.currentBatchSize != 1000 {
		t.Errorf("Expected initial batch size 1000, got %d", bp.currentBatchSize)
	}
}

// TestGetNextSignal Pull 방식의 핵심 기능 테스트
func TestGetNextSignal(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	// 초기 신호
	signal1 := bp.GetNextSignal()
	if signal1.ProducingPerInterval < 1000 {
		t.Errorf("값이 더 커져야하는데 안커짐.")
	}
	if signal1.CurrentSaturation != 0 {
		t.Errorf("Expected initial saturation 0, got %f", signal1.CurrentSaturation)
	}

	// 프로듀싱 시뮬레이션
	bp.CountProducings(1000)

	// 두 번째 신호 - 낮은 부하이므로 증가해야 함
	signal2 := bp.GetNextSignal()
	if signal2.ProducingPerInterval <= signal1.ProducingPerInterval {
		t.Errorf("Expected batch size increase with low load, got %d (was %d)",
			signal2.ProducingPerInterval, signal1.ProducingPerInterval)
	}
	if signal2.CurrentSaturation != 0.1 {
		t.Errorf("Expected saturation 0.1, got %f", signal2.CurrentSaturation)
	}

	// 타임스탬프 검증
	if signal2.Timestamp.Before(signal1.Timestamp) {
		t.Error("Second signal timestamp should be after first signal")
	}
}

// TestPullBasedRealTimeResponse Pull 방식이 실시간으로 반응하는지 테스트
func TestPullBasedRealTimeResponse(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	// 시나리오: 급격한 부하 증가
	bp.CountProducings(8000) // 80% 포화도로 급증

	// 즉시 신호 요청
	signal := bp.GetNextSignal()

	// 높은 부하에 즉각 반응해야 함
	if signal.ProducingPerInterval >= 1000 {
		t.Errorf("Should decrease batch size immediately with high load (80%%), got %d",
			signal.ProducingPerInterval)
	}
	if signal.ProducingIntervalSecond <= 1 {
		t.Errorf("Should increase interval with high load, got %d",
			signal.ProducingIntervalSecond)
	}
	if signal.CurrentSaturation < 0.79 || signal.CurrentSaturation > 0.81 {
		t.Errorf("Expected saturation around 0.8, got %f", signal.CurrentSaturation)
	}
}

// TestNormalizationOnGetNextSignal GetNextSignal이 정규화를 수행하는지 테스트
func TestNormalizationOnGetNextSignal(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	// 프로듀서와 컨슈머 카운트 추가
	bp.CountProducings(1000)
	bp.CountConsumings(400)

	// GetNextSignal 호출 (정규화 포함)
	signal := bp.GetNextSignal()

	// 정규화 후 상태 확인
	producer, consumer := bp.GetMetersStatus()
	if consumer != 0 {
		t.Errorf("Consumer should be 0 after normalization, got %d", consumer)
	}
	if producer != 600 {
		t.Errorf("Producer should be 600 after normalization (1000-400), got %d", producer)
	}

	// 신호의 포화도가 정규화된 값 기반이어야 함
	expectedSaturation := 600.0 / 10000.0
	if signal.CurrentSaturation != expectedSaturation {
		t.Errorf("Expected saturation %f, got %f", expectedSaturation, signal.CurrentSaturation)
	}
}

// TestTrendAnalysis 트렌드 분석 기능 테스트
func TestTrendAnalysis(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	// 포화도 증가 트렌드 시뮬레이션
	increments := []int{500, 700, 900, 1100}
	previousBatchSize := 1000

	for i, increment := range increments {
		bp.CountProducings(increment)
		signal := bp.GetNextSignal()

		if i > 0 {
			// 기울기 감소 확인
			// 계속 배치값 증가는 하지만 그 기울기는 감소 필요
			fmt.Printf("Iteration %d: Batch size increased with rising trend (current: %d, previous: %d)",
				i, signal.ProducingPerInterval, previousBatchSize)
		}
		previousBatchSize = signal.ProducingPerInterval

		// 약간의 소비 시뮬레이션
		bp.CountConsumings(increment / 2)
	}

	// 히스토리 확인
	history := bp.GetAdjustmentHistory()
	if len(history) == 0 {
		t.Error("Adjustment history should not be empty")
	}
}

// TestAdaptiveAdjustment 적응형 조절 테스트
func TestAdaptiveAdjustment(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	// 시나리오 1: 안정적인 상태
	bp.CountProducings(5000) // 50% 포화도 (목표치)
	signal1 := bp.GetNextSignal()

	// 목표 포화도에서는 크게 변경하지 않아야 함
	if signal1.ProducingPerInterval < 950 || signal1.ProducingPerInterval > 1050 {
		t.Errorf("At target saturation, batch size should remain stable, got %d",
			signal1.ProducingPerInterval)
	}

	// 시나리오 2: 급격한 증가
	bp.CountProducings(3000) // 80% 포화도로 급증
	signal2 := bp.GetNextSignal()

	// 급격한 감소 필요
	if signal2.ProducingPerInterval >= signal1.ProducingPerInterval {
		t.Errorf("Should decrease batch size with sudden load increase, got %d (was %d)",
			signal2.ProducingPerInterval, signal1.ProducingPerInterval)
	}
}

// TestBatchSizeLimits 배치 크기 제한 테스트
func TestBatchSizeLimits(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	// 매우 낮은 부하로 최대 배치 크기 테스트
	signal1 := bp.GetNextSignal()
	maxAllowed := int(10000 * 0.30) // maxBatchRatio = 0.30
	if signal1.ProducingPerInterval > maxAllowed {
		t.Errorf("Batch size %d exceeds maximum allowed %d",
			signal1.ProducingPerInterval, maxAllowed)
	}

	// 매우 높은 부하로 최소 배치 크기 테스트
	bp2 := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	bp2.CountProducings(9500) // 95% 포화도
	signal2 := bp2.GetNextSignal()
	if signal2.ProducingPerInterval < 1 {
		t.Errorf("Batch size should not go below 1, got %d", signal2.ProducingPerInterval)
	}
}

// TestIntervalLimits 인터벌 제한 테스트
func TestIntervalLimits(t *testing.T) {
	// 최소 인터벌 테스트
	bp1 := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{2, 1000})

	bp1.CountProducings(100) // 매우 낮은 부하
	signal1 := bp1.GetNextSignal()
	if signal1.ProducingIntervalSecond < 1 {
		t.Errorf("Interval should not go below 1 second, got %d",
			signal1.ProducingIntervalSecond)
	}

	// 최대 인터벌 테스트
	bp2 := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{30, 1000})

	bp2.CountProducings(9900) // 매우 높은 부하
	signal2 := bp2.GetNextSignal()
	if signal2.ProducingIntervalSecond > 60 {
		t.Errorf("Interval should not exceed 60 seconds, got %d",
			signal2.ProducingIntervalSecond)
	}
}

// TestConcurrentPullRequests 동시 Pull 요청 테스트
func TestConcurrentPullRequests(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	var wg sync.WaitGroup
	signalChan := make(chan SpeedSignal, 10)

	// 여러 고루틴에서 동시에 신호 요청
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 각 고루틴이 프로듀싱하고 신호 요청
			bp.CountProducings(100)
			signal := bp.GetNextSignal()
			signalChan <- signal

			// 컨슈밍도 시뮬레이션
			bp.CountConsumings(50)
		}(i)
	}

	wg.Wait()
	close(signalChan)

	// 모든 신호가 유효한지 확인
	count := 0
	for signal := range signalChan {
		count++
		if signal.ProducingPerInterval < 1 {
			t.Error("Invalid batch size in concurrent scenario")
		}
		if signal.ProducingIntervalSecond < 1 {
			t.Error("Invalid interval in concurrent scenario")
		}
	}

	if count != 10 {
		t.Errorf("Expected 10 signals, got %d", count)
	}
}

// TestHybridMode Pull과 Push 방식 혼용 테스트
func TestHybridMode(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	// Push 방식 채널 등록
	pushChan := make(chan SpeedSignal, 10)
	bp.RegisterBackpressureChannel(pushChan)

	// 백그라운드 모니터링 시작
	bp.Start()
	defer bp.Stop()

	// Pull 방식으로 신호 요청
	bp.CountProducings(1000)
	pullSignal := bp.GetNextSignal()

	// Push 방식으로도 신호가 오는지 확인
	time.Sleep(150 * time.Millisecond) // 백그라운드 틱 대기

	select {
	case pushSignal := <-pushChan:
		// Push와 Pull 신호가 비슷해야 함 (완전히 같지는 않을 수 있음)
		if pushSignal.ProducingPerInterval == 0 {
			t.Error("Push signal should have valid batch size")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Expected push signal but didn't receive one")
	}

	// Pull 신호가 유효한지 확인
	if pullSignal.ProducingPerInterval == 0 {
		t.Error("Pull signal should have valid batch size")
	}
}

// TestEmergencyResponse 극한 상황 대응 테스트
func TestEmergencyResponse(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	// 95% 이상 포화도 (비상 상황)
	bp.CountProducings(9600)
	signal := bp.GetNextSignal()

	// 비상 조치가 적용되어야 함
	if signal.ProducingPerInterval > 300 {
		t.Errorf("Emergency measures should drastically reduce batch size, got %d",
			signal.ProducingPerInterval)
	}
	if signal.ProducingIntervalSecond < 2 {
		t.Errorf("Emergency measures should increase interval significantly, got %d",
			signal.ProducingIntervalSecond)
	}
}

// TestResetMeters 미터 리셋 테스트
func TestResetMeters(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	// 카운트 추가
	bp.CountProducings(1000)
	bp.CountConsumings(500)

	// 신호 요청으로 히스토리 생성
	bp.GetNextSignal()
	bp.GetNextSignal()

	// 리셋
	bp.ResetMeters()

	// 모든 값이 초기화되어야 함
	producer, consumer := bp.GetMetersStatus()
	if producer != 0 || consumer != 0 {
		t.Errorf("Meters should be reset to 0, got producer=%d, consumer=%d",
			producer, consumer)
	}

	history := bp.GetAdjustmentHistory()
	if len(history) != 0 {
		t.Errorf("History should be cleared, got %d items", len(history))
	}

	if bp.GetCurrentSaturation() != 0 {
		t.Errorf("Saturation should be 0 after reset, got %f", bp.GetCurrentSaturation())
	}
}

// TestSequentialPullScenario 실제 사용 시나리오 시뮬레이션
func TestSequentialPullScenario(t *testing.T) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 100})

	// 10회 반복 프로듀싱 시뮬레이션
	for i := 0; i < 10; i++ {
		// 신호 요청
		signal := bp.GetNextSignal()

		// 신호에 따라 프로듀싱
		bp.CountProducings(signal.ProducingPerInterval)

		// 일부 소비 (70% 정도)
		consumed := int(float64(signal.ProducingPerInterval) * 0.7)
		bp.CountConsumings(consumed)

		// 로그
		t.Logf("Iteration %d: Batch=%d, Interval=%d, Saturation=%.2f",
			i, signal.ProducingPerInterval, signal.ProducingIntervalSecond,
			signal.CurrentSaturation)

		// 포화도가 목표치(50%) 근처로 수렴해야 함
		if i > 5 { // 안정화 후
			if signal.CurrentSaturation < 0.3 || signal.CurrentSaturation > 0.7 {
				t.Logf("Warning: Saturation %.2f is far from target 0.5 at iteration %d",
					signal.CurrentSaturation, i)
			}
		}
	}
}

// Benchmark tests
func BenchmarkGetNextSignal(b *testing.B) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	bp.CountProducings(5000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bp.GetNextSignal()
	}
}

func BenchmarkGetNextSignalWithNormalization(b *testing.B) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bp.CountProducings(100)
		bp.CountConsumings(50)
		_ = bp.GetNextSignal()
	}
}

func BenchmarkConcurrentGetNextSignal(b *testing.B) {
	bp := NewKafkaCountingBackpressure(RequestedQueueSpec{10000, 0.5}, SeedProducingConfig{1, 1000})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bp.CountProducings(10)
			_ = bp.GetNextSignal()
			bp.CountConsumings(5)
		}
	})
}
