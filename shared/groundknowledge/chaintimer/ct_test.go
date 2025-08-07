package chaintimer_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer" // 실제 모듈 경로로 변경 필요
)

// TestChainTimerBasic 기본 기능 테스트
func TestChainTimerBasic(t *testing.T) {
	defer chaintimer.Reset()
	startTime := chaintimer.NewChainTime(2024, 1, 1, 0, 0, 0, 0)
	// 초기 시간 확인
	if !chaintimer.Now().Equal(startTime) {
		t.Errorf("Expected initial time %v, got %v", startTime, chaintimer.Now())
	}

	// 시간 전진
	newTime := startTime.Add(10 * chaintimer.Second)
	chaintimer.AdvanceTo(newTime)

	if !chaintimer.Now().Equal(newTime) {
		t.Errorf("Expected time %v after advance, got %v", newTime, chaintimer.Now())
	}

	// 같은 시간으로 업데이트 (무시되어야 함)
	chaintimer.AdvanceTo(newTime)
	if !chaintimer.Now().Equal(newTime) {
		t.Errorf("Time should remain %v, got %v", newTime, chaintimer.Now())
	}

	// 과거 시간으로 업데이트 (무시되어야 함)
	pastTime := startTime.Add(5 * chaintimer.Second)
	chaintimer.AdvanceTo(pastTime)
	if !chaintimer.Now().Equal(newTime) {
		t.Errorf("Time should remain %v after past update, got %v", newTime, chaintimer.Now())
	}
}

// TestChainTimerSimulation 실시간 시뮬레이션 테스트
func TestChainTimerSimulation(t *testing.T) {
	defer chaintimer.Reset()

	fmt.Println("=== ChainTimer Simulation Test ===")

	startTime := chaintimer.NewChainTime(2024, 1, 1, 0, 0, 0, 0)

	// 테스트 시작 시간
	testStart := time.Now()

	// 시뮬레이션: 0.5초마다 체인 시간을 1초씩 전진
	go func() {
		for i := 1; i <= 10; i++ {
			time.Sleep(500 * time.Millisecond) // 0.5초 대기
			newTime := startTime.Add(chaintimer.ChainDuration(i) * chaintimer.Second)
			chaintimer.AdvanceTo(newTime)
			fmt.Printf("[%.1fs] Chain time advanced to: %v\n",
				time.Since(testStart).Seconds(),
				newTime.Format("15:04:05"))
		}
	}()

	// 시간 업데이트 구독
	updates := chaintimer.Subscribe()
	updateCount := 0

	go func() {
		for update := range updates {
			updateCount++
			fmt.Printf("  → Time update received: %v\n", update.Format("15:04:05"))
			if updateCount >= 10 {
				return
			}
		}
	}()

	// 5.5초 후 테스트 종료 (10번의 업데이트가 완료될 시간)
	time.Sleep(5500 * time.Millisecond)

	// 최종 시간 확인
	finalTime := chaintimer.Now()
	expectedFinal := startTime.Add(10 * chaintimer.Second)
	if !finalTime.Equal(expectedFinal) {
		t.Errorf("Expected final time %v, got %v", expectedFinal, finalTime)
	}

	fmt.Printf("\nFinal chain time: %v\n", finalTime.Format("15:04:05"))
	fmt.Printf("Total updates received: %d\n", updateCount)
}

// TestChainTicker 티커 테스트
func TestChainTicker(t *testing.T) {
	defer chaintimer.Reset()

	fmt.Println("\n=== ChainTicker Test ===")

	startTime := chaintimer.NewChainTime(2024, 1, 1, 0, 0, 0, 0)
	chaintimer.Initialize(startTime)

	// 3초 간격 티커 생성
	ticker := chaintimer.NewTicker(3 * chaintimer.Second)
	defer ticker.Stop()

	tickCount := 0
	var mu sync.Mutex

	// 티커 수신 고루틴
	go func() {
		for tick := range ticker.C {
			mu.Lock()
			tickCount++
			fmt.Printf("Tick #%d at chain time: %v\n", tickCount, tick.Format("15:04:05"))
			mu.Unlock()
		}
	}()

	// 시뮬레이션: 이산적 시간 업데이트
	testCases := []struct {
		advance chaintimer.ChainDuration
		sleep   time.Duration
		desc    string
	}{
		{2 * chaintimer.Second, 200 * time.Millisecond, "Before first tick"},
		{2 * chaintimer.Second, 200 * time.Millisecond, "Trigger first tick (4s total)"},
		{1 * chaintimer.Second, 200 * time.Millisecond, "Before second tick (5s total)"},
		{2 * chaintimer.Second, 200 * time.Millisecond, "Trigger second tick (7s total)"},
		{5 * chaintimer.Second, 200 * time.Millisecond, "Big jump - trigger third tick (12s total)"},
	}

	currentTime := startTime
	for _, tc := range testCases {
		time.Sleep(tc.sleep)
		currentTime = currentTime.Add(tc.advance)
		chaintimer.AdvanceTo(currentTime)
		fmt.Printf("Advanced to %v - %s\n", currentTime.Format("15:04:05"), tc.desc)
	}

	// 잠시 대기 후 확인
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	// 12초까지 진행했으므로: 3초, 6초, 9초, 12초 = 4개의 틱
	if tickCount != 4 {
		t.Errorf("Expected 4 ticks, got %d", tickCount)
	}
	mu.Unlock()

	fmt.Printf("Total ticks received: %d\n", tickCount)
}

// TestChainTimeout 타임아웃 테스트
func TestChainTimeout(t *testing.T) {
	defer chaintimer.Reset()

	fmt.Println("\n=== ChainTimeout Test ===")

	startTime := chaintimer.NewChainTime(2024, 1, 1, 0, 0, 0, 0)
	chaintimer.Initialize(startTime)
	// 5초 타임아웃 설정
	timeout := chaintimer.NewTimeout(5 * chaintimer.Second)

	// 타임아웃 수신 대기
	var timeoutReceived bool
	var timeoutTime chaintimer.ChainTime

	go func() {
		for t := range timeout.C {

			timeoutReceived = true
			timeoutTime = t
			fmt.Printf("Timeout triggered at chain time: %v\n", t.Format("15:04:05"))
		}

	}()

	// 시뮬레이션: 단계적 시간 전진
	steps := []chaintimer.ChainDuration{
		2 * chaintimer.Second, // 2초
		2 * chaintimer.Second, // 4초
		2 * chaintimer.Second, // 6초 - 타임아웃 발생 예상
	}

	currentTime := startTime
	for i, step := range steps {
		time.Sleep(300 * time.Millisecond)
		currentTime = currentTime.Add(step)
		chaintimer.AdvanceTo(currentTime)
		fmt.Printf("Step %d: Advanced to %v\n", i+1, currentTime.Format("15:04:05"))
	}

	// 타임아웃 확인
	time.Sleep(500 * time.Millisecond)
	if !timeoutReceived {
		t.Error("Timeout should have been triggered")
	}

	if !timeoutTime.Equal(startTime.Add(5 * chaintimer.Second)) {
		t.Errorf("Timeout triggered at wrong time: %v", timeoutTime)
	}
}

// TestChainAfter After 메서드 테스트
func TestChainAfter(t *testing.T) {
	defer chaintimer.Reset()

	fmt.Println("\n=== ChainAfter Test ===")

	startTime := chaintimer.NewChainTime(2024, 1, 1, 0, 0, 0, 0)
	chaintimer.Initialize(startTime)

	// 여러 After 채널 생성
	after3s := chaintimer.After(3 * chaintimer.Second)
	after5s := chaintimer.After(5 * chaintimer.Second)
	after7s := chaintimer.After(7 * chaintimer.Second)

	received := make(map[string]bool)
	mu := sync.Mutex{}

	// 각 채널 모니터링
	go func() {
		<-after3s
		mu.Lock()
		received["3s"] = true
		fmt.Println("After 3s triggered")
		mu.Unlock()
	}()

	go func() {
		<-after5s
		mu.Lock()
		received["5s"] = true
		fmt.Println("After 5s triggered")
		mu.Unlock()
	}()

	go func() {
		<-after7s
		mu.Lock()
		received["7s"] = true
		fmt.Println("After 7s triggered")
		mu.Unlock()
	}()

	// 큰 점프로 한 번에 시간 전진
	time.Sleep(500 * time.Millisecond)
	chaintimer.AdvanceTo(startTime.Add(6 * chaintimer.Second))
	fmt.Printf("Jumped to %v\n", startTime.Add(6*chaintimer.Second).Format("15:04:05"))

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	if !received["3s"] || !received["5s"] {
		t.Error("3s and 5s timers should have triggered")
	}
	if received["7s"] {
		t.Error("7s timer should not have triggered yet")
	}
	mu.Unlock()

	// 7초 지점으로 전진
	chaintimer.AdvanceTo(startTime.Add(8 * chaintimer.Second))
	fmt.Printf("Advanced to %v\n", startTime.Add(8*chaintimer.Second).Format("15:04:05"))

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	if !received["7s"] {
		t.Error("7s timer should have triggered")
	}
	mu.Unlock()
}

// TestComplexSimulation 복잡한 시뮬레이션 테스트
func TestComplexSimulation(t *testing.T) {
	defer chaintimer.Reset()

	fmt.Println("\n=== Complex Simulation Test ===")
	fmt.Println("Simulating blockchain with irregular block times")

	startTime := chaintimer.NewChainTime(2024, 1, 1, 0, 0, 0, 0)

	chaintimer.Initialize(startTime)

	// 2초 간격 티커
	ticker := chaintimer.NewTicker(2 * chaintimer.Second)
	defer ticker.Stop()

	// 5초 타임아웃
	timeout := chaintimer.After(5 * chaintimer.Second)

	// 통계 수집
	stats := struct {
		ticks   int
		updates int
		timeout bool
		mu      sync.Mutex
	}{}

	// 티커 모니터링
	go func() {
		for range ticker.C {
			stats.mu.Lock()
			stats.ticks++
			fmt.Printf("  [TICK %d]\n", stats.ticks)
			stats.mu.Unlock()
		}
	}()

	// 타임아웃 모니터링
	go func() {
		<-timeout
		stats.mu.Lock()
		stats.timeout = true
		fmt.Println("  [TIMEOUT!]")
		stats.mu.Unlock()
	}()

	// 불규칙한 블록 시간 시뮬레이션
	blockTimes := []struct {
		delay   time.Duration // 실제 대기 시간
		advance time.Duration // 체인 시간 전진량
	}{
		{300 * time.Millisecond, 1 * time.Second},        // Block 1
		{500 * time.Millisecond, 2 * time.Second},        // Block 2 (tick expected)
		{200 * time.Millisecond, 500 * time.Millisecond}, // Block 3
		{400 * time.Millisecond, 3 * time.Second},        // Block 4 (tick & timeout expected)
		{300 * time.Millisecond, 1 * time.Second},        // Block 5
	}

	currentTime := startTime
	for i, block := range blockTimes {
		time.Sleep(block.delay)
		currentTime = currentTime.Add(chaintimer.ChainDuration(block.advance))
		chaintimer.AdvanceTo(currentTime)

		stats.mu.Lock()
		stats.updates++
		elapsed := currentTime.Sub(startTime)
		fmt.Printf("Block %d: Chain time = %v (+%v), Total elapsed = %v\n",
			i+1, currentTime.Format("15:04:05"), block.advance, elapsed)
		stats.mu.Unlock()
	}

	// 최종 확인
	time.Sleep(500 * time.Millisecond)

	stats.mu.Lock()
	fmt.Printf("\n=== Final Statistics ===\n")
	fmt.Printf("Total blocks: %d\n", stats.updates)
	fmt.Printf("Total ticks: %d\n", stats.ticks)
	fmt.Printf("Timeout triggered: %v\n", stats.timeout)
	fmt.Printf("Final chain time: %v\n", chaintimer.Now().Format("15:04:05"))

	// 예상값 검증
	if stats.ticks < 3 {
		t.Errorf("Expected at least 3 ticks, got %d", stats.ticks)
	}
	if !stats.timeout {
		t.Error("Timeout should have been triggered")
	}
	stats.mu.Unlock()
}

// BenchmarkChainTimer 성능 벤치마크
func BenchmarkChainTimer(b *testing.B) {
	defer chaintimer.Reset()

	ticker := chaintimer.NewTicker(chaintimer.Second)
	defer ticker.Stop()

	currentTime := chaintimer.ChainTime(time.Now())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		currentTime = currentTime.Add(chaintimer.Millisecond)
		chaintimer.AdvanceTo(currentTime)
	}
}
