package tools

import (
	"sync"
	"testing"
	"time"
)

func TestNewKafkaCountingBackpressure(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)

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

func TestCountingMethods(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)

	// Test single counting
	bp.CountProducing()
	if bp.producerMeter.TotalSum() != 1 {
		t.Errorf("Expected producer count 1, got %d", bp.producerMeter.TotalSum())
	}

	bp.CountConsuming()
	if bp.consumerMeter.TotalSum() != 1 {
		t.Errorf("Expected consumer count 1, got %d", bp.consumerMeter.TotalSum())
	}

	// Test multiple counting
	bp.CountProducings(100)
	if bp.producerMeter.TotalSum() != 101 {
		t.Errorf("Expected producer count 101, got %d", bp.producerMeter.TotalSum())
	}

	bp.CountConsumings(50)
	if bp.consumerMeter.TotalSum() != 51 {
		t.Errorf("Expected consumer count 51, got %d", bp.consumerMeter.TotalSum())
	}
}

func TestNormalizeMeters(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)

	// Setup: Producer 100, Consumer 30
	bp.CountProducings(100)
	bp.CountConsumings(30)

	// Normalize
	bp.normalizeMeters()

	// After normalization: Producer should be 70, Consumer should be 0
	if bp.producerMeter.TotalSum() != 70 {
		t.Errorf("Expected producer count 70 after normalization, got %d", bp.producerMeter.TotalSum())
	}
	if bp.consumerMeter.TotalSum() != 0 {
		t.Errorf("Expected consumer count 0 after normalization, got %d", bp.consumerMeter.TotalSum())
	}

	// Test when consumer is 0 (should do nothing)
	bp.normalizeMeters()
	if bp.producerMeter.TotalSum() != 70 {
		t.Errorf("Producer count should remain 70 when consumer is 0, got %d", bp.producerMeter.TotalSum())
	}
}

func TestNormalizeMetersNegativeCase(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)

	// Artificially create impossible situation: Consumer > Producer
	bp.consumerMeter.Set(100)
	bp.producerMeter.Set(50)

	// This should trigger error handling
	bp.normalizeMeters()

	// Producer should be reset to 0
	if bp.producerMeter.TotalSum() != 0 {
		t.Errorf("Expected producer count 0 after negative detection, got %d", bp.producerMeter.TotalSum())
	}
	if bp.consumerMeter.TotalSum() != 0 {
		t.Errorf("Expected consumer count 0 after normalization, got %d", bp.consumerMeter.TotalSum())
	}
}

func TestDecideSignaling(t *testing.T) {
	tests := []struct {
		name                string
		queuedMessages      int64
		expectedBatchChange string // "increase", "decrease", "maintain"
	}{
		{"Very Low Load (<10%)", 500, "increase"},
		{"Low Load (10-30%)", 2000, "increase"},
		{"Slightly Low (30-40%)", 3500, "increase"},
		{"Optimal (40-60%)", 5000, "maintain"},
		{"Slightly High (60-70%)", 6500, "decrease"},
		{"High Load (70-80%)", 7500, "decrease"},
		{"Very High Load (>80%)", 8500, "decrease"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)
			bp.producerMeter.Set(tt.queuedMessages)

			signal := bp.decideSingnaling()

			switch tt.expectedBatchChange {
			case "increase":
				if signal.ProducingPerInterval <= bp.currentBatchSize {
					t.Errorf("%s: Expected batch size increase, got %d (was %d)",
						tt.name, signal.ProducingPerInterval, bp.currentBatchSize)
				}
				if signal.ProducingIntervalSecond > bp.currentInterval {
					t.Errorf("%s: Expected interval decrease, got %d (was %d)",
						tt.name, signal.ProducingIntervalSecond, bp.currentInterval)
				}
			case "decrease":
				if signal.ProducingPerInterval >= bp.currentBatchSize {
					t.Errorf("%s: Expected batch size decrease, got %d (was %d)",
						tt.name, signal.ProducingPerInterval, bp.currentBatchSize)
				}
				if signal.ProducingIntervalSecond < bp.currentInterval {
					t.Errorf("%s: Expected interval increase, got %d (was %d)",
						tt.name, signal.ProducingIntervalSecond, bp.currentInterval)
				}
			case "maintain":
				if signal.ProducingPerInterval != bp.currentBatchSize {
					t.Errorf("%s: Expected batch size to maintain at %d, got %d",
						tt.name, bp.currentBatchSize, signal.ProducingPerInterval)
				}
				if signal.ProducingIntervalSecond != bp.currentInterval {
					t.Errorf("%s: Expected interval to maintain at %d, got %d",
						tt.name, bp.currentInterval, signal.ProducingIntervalSecond)
				}
			}
		})
	}
}

func TestBatchSizeLimits(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 10000)

	// Test maximum limit (queue capacity / 3)
	bp.producerMeter.Set(100) // Very low load
	signal := bp.decideSingnaling()
	maxAllowed := int(bp.queueCapacity / 3)
	if signal.ProducingPerInterval > maxAllowed {
		t.Errorf("Batch size %d exceeds maximum allowed %d", signal.ProducingPerInterval, maxAllowed)
	}

	// Test minimum limit
	bp2 := NewKafkaCountingBackpressure(10000, 0.5, 1, 2)
	bp2.producerMeter.Set(9000) // Very high load
	signal2 := bp2.decideSingnaling()
	if signal2.ProducingPerInterval < 1 {
		t.Errorf("Batch size %d is below minimum 1", signal2.ProducingPerInterval)
	}
}

func TestIntervalLimits(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)

	// Test minimum interval (should not go below 1 second)
	bp.producerMeter.Set(100) // Very low load
	signal := bp.decideSingnaling()
	if signal.ProducingIntervalSecond < 1 {
		t.Errorf("Interval %d is below minimum 1 second", signal.ProducingIntervalSecond)
	}
}

func TestRegisterBackpressureChannel(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)

	ch1 := make(chan SpeedSignal, 1)
	ch2 := make(chan SpeedSignal, 1)

	bp.RegisterBackpressureChannel(ch1)
	bp.RegisterBackpressureChannel(ch2)

	if len(bp.signalChannels) != 2 {
		t.Errorf("Expected 2 registered channels, got %d", len(bp.signalChannels))
	}
}

func TestMakeBackpressureEvent(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)
	ch := make(chan SpeedSignal, 10)
	bp.RegisterBackpressureChannel(ch)
	// Change load to trigger different signal

	bp.producerMeter.Set(8000) // High load
	bp.MakeBackpressureEvent()

	select {
	case signal := <-ch:
		if signal.ProducingPerInterval >= 501 {
			t.Errorf("Expected decreased batch size, got %d", signal.ProducingPerInterval)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected signal but didn't receive one")
	}
	//값 범위 정상화시키기=> 목표 포화도 달성 시 시그널 x여야 함
	bp.producerMeter.Set(5000)
	bp.MakeBackpressureEvent()

	select {
	case <-ch:
		t.Error("Should not receive duplicate signal")
	case <-time.After(100 * time.Millisecond):
		// Expected behavior
	}
}

func TestConcurrentOperations(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)

	var wg sync.WaitGroup
	iterations := 1000

	// Concurrent producers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			bp.CountProducing()
		}
	}()

	// Concurrent consumers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			bp.CountConsuming()
		}
	}()

	// Concurrent batch producers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations/10; i++ {
			bp.CountProducings(10)
		}
	}()

	// Concurrent batch consumers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations/10; i++ {
			bp.CountConsumings(10)
		}
	}()

	wg.Wait()

	// Each side should have 2000 counts (1000 + 100*10)
	expectedCount := int64(2000)
	if bp.producerMeter.TotalSum() != expectedCount {
		t.Errorf("Expected producer count %d, got %d", expectedCount, bp.producerMeter.TotalSum())
	}
	if bp.consumerMeter.TotalSum() != expectedCount {
		t.Errorf("Expected consumer count %d, got %d", expectedCount, bp.consumerMeter.TotalSum())
	}
}

func TestStartStop(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)
	ch := make(chan SpeedSignal, 10)
	bp.RegisterBackpressureChannel(ch)

	// Start monitoring
	bp.Start()

	// Simulate load
	bp.CountProducings(1000)
	bp.CountConsumings(500)

	// Wait for at least one monitoring cycle
	time.Sleep(150 * time.Millisecond)

	// Check that normalization happened
	producer, consumer := bp.GetMetersStatus()
	if consumer != 0 {
		t.Errorf("Expected consumer to be normalized to 0, got %d", consumer)
	}
	if producer != 500 {
		t.Errorf("Expected producer to be 500 after normalization, got %d", producer)
	}

	// Stop monitoring
	bp.Stop()

	// Add more counts after stop
	bp.CountProducings(100)
	time.Sleep(150 * time.Millisecond)

	// Should not normalize after stop
	producer2, _ := bp.GetMetersStatus()
	if producer2 != 600 {
		t.Errorf("Expected producer to be 600 (not normalized after stop), got %d", producer2)
	}
}

func TestGetters(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)
	bp.producerMeter.Set(5000)

	// Test GetCurrentSaturation
	saturation := bp.GetCurrentSaturation()
	expectedSaturation := 0.5
	if saturation != expectedSaturation {
		t.Errorf("Expected saturation %f, got %f", expectedSaturation, saturation)
	}

	// Test GetQueueSize
	queueSize := bp.GetQueueSize()
	if queueSize != 5000 {
		t.Errorf("Expected queue size 5000, got %d", queueSize)
	}

	// Test GetMetersStatus
	bp.consumerMeter.Set(1000)
	producer, consumer := bp.GetMetersStatus()
	if producer != 5000 || consumer != 1000 {
		t.Errorf("Expected meters (5000, 1000), got (%d, %d)", producer, consumer)
	}
}

func TestResetMeters(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)

	bp.CountProducings(100)
	bp.CountConsumings(50)

	bp.ResetMeters()

	if bp.producerMeter.TotalSum() != 0 {
		t.Errorf("Expected producer meter to be 0 after reset, got %d", bp.producerMeter.TotalSum())
	}
	if bp.consumerMeter.TotalSum() != 0 {
		t.Errorf("Expected consumer meter to be 0 after reset, got %d", bp.consumerMeter.TotalSum())
	}
}

func TestTargetSaturationAdjustment(t *testing.T) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)

	// Set load way above target (50%)
	bp.producerMeter.Set(7000) // 70% saturation
	signal := bp.decideSingnaling()

	// Should reduce batch size more aggressively
	targetOffset := int64(float64(bp.queueCapacity) * bp.targetSaturation)
	adjustment := float64(7000) / float64(targetOffset) // 7000/5000 = 1.4

	// With adjustment > 1.2, batch size should be divided by adjustment
	expectedMaxBatch := int(float64(bp.currentBatchSize) * 0.8 / adjustment)
	if signal.ProducingPerInterval > expectedMaxBatch+100 { // Allow some margin
		t.Errorf("Batch size %d not adjusted enough for high saturation", signal.ProducingPerInterval)
	}
}

// Benchmark tests
func BenchmarkCountProducing(b *testing.B) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bp.CountProducing()
	}
}

func BenchmarkCountProducings(b *testing.B) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bp.CountProducings(100)
	}
}

func BenchmarkNormalizeMeters(b *testing.B) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bp.producerMeter.Set(1000)
		bp.consumerMeter.Set(500)
		bp.normalizeMeters()
	}
}

func BenchmarkDecideSignaling(b *testing.B) {
	bp := NewKafkaCountingBackpressure(10000, 0.5, 1, 1000)
	bp.producerMeter.Set(5000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bp.decideSingnaling()
	}
}
