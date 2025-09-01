package rsv

import (
	"fmt"
	"testing"
	"time"
)

// ExampleFunctionReservoirMeter 사용 예제
func ExampleFunctionReservoirMeter() {
	// 1024개 샘플을 저장하는 미터 생성
	meter := NewFunctionReservoirMeter(1024)

	// 다양한 함수의 실행 시간 기록
	meter.Record("processTransaction", 10*time.Millisecond)
	meter.Record("validateInput", 2*time.Millisecond)
	meter.Record("saveToDatabase", 50*time.Millisecond)
	meter.Record("processTransaction", 15*time.Millisecond)
	meter.Record("validateInput", 3*time.Millisecond)
	
	// 특정 함수의 통계 조회
	if stats, exists := meter.GetFunctionStats("processTransaction"); exists {
		fmt.Printf("processTransaction - Mean: %v, P95: %v\n", stats.Mean, stats.P95)
	}

	// 모든 함수의 통계 조회
	allStats := meter.GetAllStats()
	for funcName, stats := range allStats {
		fmt.Printf("%s: Mean=%v, Count=%d\n", funcName, stats.Mean, stats.Count)
	}

	// 병목 함수 출력 (상위 3개)
	meter.PrintBottlenecks(3)
}

// TestFunctionReservoirMeter 기본 기능 테스트
func TestFunctionReservoirMeter(t *testing.T) {
	meter := NewFunctionReservoirMeter(10) // 작은 용량으로 테스트

	// 여러 함수에 데이터 기록
	testData := []struct {
		funcName string
		duration time.Duration
	}{
		{"func1", 10 * time.Millisecond},
		{"func2", 20 * time.Millisecond},
		{"func1", 15 * time.Millisecond},
		{"func3", 30 * time.Millisecond},
		{"func1", 12 * time.Millisecond},
	}

	for _, data := range testData {
		meter.Record(data.funcName, data.duration)
	}

	// func1 통계 확인
	stats, exists := meter.GetFunctionStats("func1")
	if !exists {
		t.Error("func1 통계가 존재하지 않음")
	}
	if stats.Count != 3 {
		t.Errorf("func1 카운트가 잘못됨: expected 3, got %d", stats.Count)
	}

	// 모든 통계 확인
	allStats := meter.GetAllStats()
	if len(allStats) != 3 {
		t.Errorf("함수 개수가 잘못됨: expected 3, got %d", len(allStats))
	}

	// 정렬된 통계 확인
	sortedStats := meter.GetSortedStats()
	if len(sortedStats) != 3 {
		t.Error("정렬된 통계 개수가 잘못됨")
	}
	
	// 가장 느린 함수가 첫 번째에 와야 함
	if sortedStats[0].FunctionName != "func3" {
		t.Errorf("정렬이 잘못됨: expected func3 first, got %s", sortedStats[0].FunctionName)
	}
}

// TestMeasureFunction defer 패턴 테스트
func TestMeasureFunction(t *testing.T) {
	meter := NewFunctionReservoirMeter(100)

	// defer 패턴으로 함수 측정
	func() {
		defer MeasureFunction(meter, "testFunction")()
		time.Sleep(10 * time.Millisecond) // 시뮬레이션
	}()

	stats, exists := meter.GetFunctionStats("testFunction")
	if !exists {
		t.Error("testFunction 통계가 존재하지 않음")
	}
	if stats.Count != 1 {
		t.Errorf("카운트가 잘못됨: expected 1, got %d", stats.Count)
	}
	if stats.Mean < 10*time.Millisecond {
		t.Errorf("측정 시간이 너무 짧음: %v", stats.Mean)
	}
}

// BenchmarkFunctionReservoirMeter 성능 벤치마크
func BenchmarkFunctionReservoirMeter(b *testing.B) {
	meter := NewFunctionReservoirMeter(1024)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			meter.Record("benchmarkFunction", time.Duration(b.N)*time.Nanosecond)
		}
	})
}