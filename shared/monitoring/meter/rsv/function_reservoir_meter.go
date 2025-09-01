package rsv

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

// FunctionReservoirMeter는 함수별로 Reservoir Sampling을 사용하여 성능 메트릭을 측정하는 미터입니다.
type FunctionReservoirMeter struct {
	mu        sync.RWMutex
	functions map[string]*reservoirSampler
	capacity  int
}

// reservoirSampler는 개별 함수의 샘플을 관리합니다.
type reservoirSampler struct {
	samples   []time.Duration
	capacity  int
	count     uint64
	totalTime time.Duration
	rng       *rand.Rand
}

// NewFunctionReservoirMeter는 새로운 FunctionReservoirMeter를 생성합니다.
func NewFunctionReservoirMeter(capacity int) *FunctionReservoirMeter {
	if capacity <= 0 {
		capacity = 1024 // 기본값
	}

	return &FunctionReservoirMeter{
		functions: make(map[string]*reservoirSampler),
		capacity:  capacity,
	}
}

// Record는 함수명과 듀레이션을 기록합니다.
func (frm *FunctionReservoirMeter) Record(functionName string, duration time.Duration) {
	frm.mu.Lock()
	defer frm.mu.Unlock()

	sampler, exists := frm.functions[functionName]
	if !exists {
		sampler = &reservoirSampler{
			samples:  make([]time.Duration, 0, frm.capacity),
			capacity: frm.capacity,
			rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		frm.functions[functionName] = sampler
	}

	sampler.record(duration)
}

// record는 개별 샘플러에 듀레이션을 기록합니다.
func (rs *reservoirSampler) record(duration time.Duration) {
	rs.count++
	rs.totalTime += duration

	// Reservoir Sampling 알고리즘
	if len(rs.samples) < rs.capacity {
		// 아직 용량에 여유가 있으면 그냥 추가
		rs.samples = append(rs.samples, duration)
	} else {
		// 용량이 꽉 찬 경우, 랜덤하게 교체할지 결정
		if rs.rng.Float64() < float64(rs.capacity)/float64(rs.count) {
			idx := rs.rng.Intn(rs.capacity)
			rs.samples[idx] = duration
		}
	}
}

// FunctionStats는 개별 함수의 통계 정보를 담는 구조체입니다.
type FunctionStats struct {
	FunctionName string        `json:"function_name"`
	Count        uint64        `json:"count"`
	Mean         time.Duration `json:"mean"`
	P50          time.Duration `json:"p50"`
	P95          time.Duration `json:"p95"`
	P99          time.Duration `json:"p99"`
	Min          time.Duration `json:"min"`
	Max          time.Duration `json:"max"`
	SampleSize   int           `json:"sample_size"`
	TotalTime    time.Duration `json:"total_time"`
}

// GetFunctionStats는 특정 함수의 통계를 반환합니다.
func (frm *FunctionReservoirMeter) GetFunctionStats(functionName string) (FunctionStats, bool) {
	frm.mu.RLock()
	defer frm.mu.RUnlock()

	sampler, exists := frm.functions[functionName]
	if !exists {
		return FunctionStats{}, false
	}

	return sampler.calculateStats(functionName), true
}

// GetAllStats는 모든 함수의 통계를 반환합니다.
func (frm *FunctionReservoirMeter) GetAllStats() map[string]FunctionStats {
	frm.mu.RLock()
	defer frm.mu.RUnlock()

	result := make(map[string]FunctionStats)
	for funcName, sampler := range frm.functions {
		result[funcName] = sampler.calculateStats(funcName)
	}

	return result
}

// GetSortedStats는 평균 시간 기준으로 정렬된 모든 함수의 통계를 반환합니다.
func (frm *FunctionReservoirMeter) GetSortedStats() []FunctionStats {
	allStats := frm.GetAllStats()
	
	stats := make([]FunctionStats, 0, len(allStats))
	for _, stat := range allStats {
		stats = append(stats, stat)
	}

	// 평균 시간 기준으로 내림차순 정렬 (병목이 큰 순서)
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Mean > stats[j].Mean
	})

	return stats
}

// calculateStats는 개별 샘플러의 통계를 계산합니다.
func (rs *reservoirSampler) calculateStats(functionName string) FunctionStats {
	if rs.count == 0 {
		return FunctionStats{
			FunctionName: functionName,
			Count:        0,
			Mean:         0,
			P50:          0,
			P95:          0,
			P99:          0,
			Min:          0,
			Max:          0,
			SampleSize:   0,
			TotalTime:    0,
		}
	}

	// 샘플 복사 및 정렬
	samples := make([]time.Duration, len(rs.samples))
	copy(samples, rs.samples)
	sort.Slice(samples, func(i, j int) bool {
		return samples[i] < samples[j]
	})

	stats := FunctionStats{
		FunctionName: functionName,
		Count:        rs.count,
		Mean:         rs.totalTime / time.Duration(rs.count),
		SampleSize:   len(samples),
		TotalTime:    rs.totalTime,
	}

	if len(samples) > 0 {
		stats.Min = samples[0]
		stats.Max = samples[len(samples)-1]
		stats.P50 = percentile(samples, 0.50)
		stats.P95 = percentile(samples, 0.95)
		stats.P99 = percentile(samples, 0.99)
	}

	return stats
}

// percentile은 정렬된 샘플에서 지정된 백분위수를 계산합니다.
func percentile(sortedSamples []time.Duration, p float64) time.Duration {
	if len(sortedSamples) == 0 {
		return 0
	}

	if p <= 0 {
		return sortedSamples[0]
	}
	if p >= 1 {
		return sortedSamples[len(sortedSamples)-1]
	}

	// 선형 보간법 사용
	index := p * float64(len(sortedSamples)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))

	if lower == upper {
		return sortedSamples[lower]
	}

	// 보간 계산
	weight := index - float64(lower)
	lowerValue := float64(sortedSamples[lower])
	upperValue := float64(sortedSamples[upper])

	return time.Duration(lowerValue + weight*(upperValue-lowerValue))
}

// Reset은 모든 함수의 미터를 초기화합니다.
func (frm *FunctionReservoirMeter) Reset() {
	frm.mu.Lock()
	defer frm.mu.Unlock()

	frm.functions = make(map[string]*reservoirSampler)
}

// ResetFunction은 특정 함수의 미터만 초기화합니다.
func (frm *FunctionReservoirMeter) ResetFunction(functionName string) {
	frm.mu.Lock()
	defer frm.mu.Unlock()

	delete(frm.functions, functionName)
}

// GetFunctionNames는 등록된 모든 함수명을 반환합니다.
func (frm *FunctionReservoirMeter) GetFunctionNames() []string {
	frm.mu.RLock()
	defer frm.mu.RUnlock()

	names := make([]string, 0, len(frm.functions))
	for name := range frm.functions {
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}

// PrintBottlenecks는 병목 함수들을 콘솔에 출력합니다.
func (frm *FunctionReservoirMeter) PrintBottlenecks(topN int) {
	stats := frm.GetSortedStats()
	
	fmt.Printf("\n=== Top %d Function Bottlenecks ===\n", topN)
	fmt.Printf("%-30s %12s %12s %12s %12s %12s %8s\n", 
		"Function", "Count", "Mean", "P50", "P95", "P99", "Samples")
	fmt.Println(strings.Repeat("-", 110))

	for i, stat := range stats {
		if i >= topN {
			break
		}

		fmt.Printf("%-30s %12d %12s %12s %12s %12s %8d\n",
			truncateString(stat.FunctionName, 30),
			stat.Count,
			formatDuration(stat.Mean),
			formatDuration(stat.P50),
			formatDuration(stat.P95),
			formatDuration(stat.P99),
			stat.SampleSize,
		)
	}
	fmt.Println()
}

// formatDuration은 듀레이션을 읽기 쉬운 형태로 포맷합니다.
func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%.0fns", float64(d))
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.1fμs", float64(d)/float64(time.Microsecond))
	} else if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	} else {
		return fmt.Sprintf("%.3fs", float64(d)/float64(time.Second))
	}
}

// truncateString은 문자열을 지정된 길이로 자릅니다.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}