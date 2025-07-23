package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/app"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	txFeeder "github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/app"
	feederDomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/domain"
)

func main() {
	runFixedIntegrationTest()
}

// TxPipeline TxGenerator와 EOAAnalyzer를 연결하는 파이프라인 (수정버전)
type TxPipeline struct {
	// 통신 채널
	txChannel   chan *shareddomain.MarkedTransaction
	stopChannel chan struct{}

	// 컴포넌트
	generator *txFeeder.MockTxFeeder
	analyzer  app.EOAAnalyzer

	// 통계 (atomic operations for thread safety)
	stats PipelineStats

	// 동기화 및 상태 관리
	wg          sync.WaitGroup
	stopOnce    sync.Once // 채널 중복 닫기 방지
	channelOnce sync.Once // 트랜잭션 채널 중복 닫기 방지

	// 디버깅
	debugStats DebugStats
}

// PipelineStats 파이프라인 통계
type PipelineStats struct {
	Generated   int64 // 생성된 트랜잭션 수
	Transmitted int64 // 전송된 트랜잭션 수
	Processed   int64 // 처리된 트랜잭션 수
	Dropped     int64 // 드롭된 트랜잭션 수 (채널 풀)
	StartTime   time.Time
}

// DebugStats 디버깅용 통계
type DebugStats struct {
	CexToAddresses     int64 // CEX를 to로 하는 트랜잭션
	DepositToAddresses int64 // Deposit을 to로 하는 트랜잭션
	RandomTransactions int64 // 랜덤 트랜잭션
	MatchFailures      int64 // 매칭 실패
}

// IsolatedTestConfig 격리 테스트 설정 (수정버전)
type IsolatedTestConfig struct {
	BaseDir         string
	IsolatedDir     string
	CEXFilePath     string
	MockDepositFile string
	GraphDBPath     string
	PendingDBPath   string

	// Pipeline 설정 (최적화됨)
	ChannelBufferSize int
	TestDuration      time.Duration
	TotalTransactions int
	GenerationRate    int
	AnalysisWorkers   int
}

// runFixedIntegrationTest 수정된 통합 테스트 실행
func runFixedIntegrationTest() {
	fmt.Println("🚀 Fixed Queue-Based Integration Test: TxGenerator → Channel → EOAAnalyzer")
	fmt.Println("🔧 Improvements: CEX matching debug, channel sync fix, enhanced monitoring")

	// 에러 핸들링 개선 - defer가 실행되도록 보장
	if err := runFixedIntegrationTestInternal(); err != nil {
		log.Fatalf("❌ Integration test failed: %v", err)
	}

	fmt.Println("\n✅ Fixed integration test completed successfully!")
}

func runFixedIntegrationTestInternal() error {
	// 1. 테스트 설정 (개선됨)
	config := setupFixedTestConfig()

	// 2. 환경 준비는 이제 MockTxFeeder가 담당

	// 3. 파이프라인 생성 (수정버전)
	pipeline, err := createFixedTxPipeline(config)
	if err != nil {
		return fmt.Errorf("failed to create pipeline: %w", err)
	}
	
	// defer로 확실한 정리 보장
	defer func() {
		if pipeline != nil {
			pipeline.SafeClose()
			// MockTxFeeder 정리 (트랜잭션 생성 중지)
			pipeline.generator.Close()
			// 환경 정리는 여기서 명시적으로 담당
			pipeline.generator.CleanupEnvironment()
		}
	}()

	// 4. 통합 테스트 실행
	if err := runFixedPipelineTest(pipeline, config); err != nil {
		return fmt.Errorf("pipeline test failed: %w", err)
	}

	return nil
}

// setupFixedTestConfig 수정된 테스트 설정 생성
func setupFixedTestConfig() *IsolatedTestConfig {
	fmt.Println("\n1️⃣ Setting up fixed test configuration...")

	baseDir := findProjectRoot()
	isolatedDir := filepath.Join(baseDir, "debug_queue_fixed")

	config := &IsolatedTestConfig{
		BaseDir:         baseDir,
		IsolatedDir:     isolatedDir,
		CEXFilePath:     filepath.Join(isolatedDir, "cex.txt"),
		MockDepositFile: filepath.Join(isolatedDir, "deposits.txt"),
		GraphDBPath:     filepath.Join(isolatedDir, "graph"),
		PendingDBPath:   filepath.Join(isolatedDir, "pending"),

		// 버킷 성능 테스트 설정 - rear/front 인덱스 성능 검증
		ChannelBufferSize: 1_000_000,        // 충분한 버퍼
		TestDuration:      60 * time.Second, // 1분 테스트 (성능 검증용)
		TotalTransactions: 2_000_000,        // 200만개로 충분한 순환 확인
		GenerationRate:    50_000,           // 초당 5만개로 고속 진행
		AnalysisWorkers:   8,                // 워커 8 유지
	}

	fmt.Printf("   ✅ Isolated directory: %s\n", config.IsolatedDir)
	fmt.Printf("   📊 Fixed config: %d txs, %d tx/sec, %d workers, %d buffer\n",
		config.TotalTransactions, config.GenerationRate, config.AnalysisWorkers, config.ChannelBufferSize)
	return config
}

// createFixedTxPipeline 수정된 파이프라인 생성
func createFixedTxPipeline(config *IsolatedTestConfig) (*TxPipeline, error) {
	fmt.Println("\n3️⃣ Creating fixed transaction pipeline...")

	// 공유 채널 생성
	txChannel := make(chan *shareddomain.MarkedTransaction, config.ChannelBufferSize)
	stopChannel := make(chan struct{})

	// TxFeeder 먼저 생성 (빈 cexSet으로 시작)
	startTime, _ := time.Parse("2006-01-02", "2025-01-01") // 단일 시간 소스: tx.BlockTime의 기준점
	genConfig := &feederDomain.TxGeneratorConfig{
		TotalTransactions:            config.TotalTransactions,
		TransactionsPerSecond:        config.GenerationRate, //기계적으로 생성하는 시간당 tx수
		StartTime:                    startTime,             // tx.BlockTime 기준이 되는 유일한 시작점
		TransactionsPerTimeIncrement: 1,                     //하나의 tx마다 10분이 지난 것으로 설정 (순환 테스트 가속화)
		TimeIncrementDuration:        10 * time.Minute,      //10분씩 시간 증가 (1주=1008분=약17tx, 21주=357tx)
		DepositToCexRatio:            50,                    // 1/50 비율로 CEX 주소 사용
		RandomToDepositRatio:         30,                    //1/15 비율로 Deposit 주소 사용
	}

	// 빈 CEXSet으로 MockTxFeeder 생성
	emptyCexSet := shareddomain.NewCEXSet()
	generator := txFeeder.NewTxFeeder(genConfig, emptyCexSet)
	
	// 환경 설정을 위한 EnvironmentConfig 생성
	envConfig := &txFeeder.EnvironmentConfig{
		BaseDir:           config.BaseDir,
		IsolatedDir:       config.IsolatedDir,
		CEXFilePath:       config.CEXFilePath,
		MockDepositFile:   config.MockDepositFile,
		GraphDBPath:       config.GraphDBPath,
		PendingDBPath:     config.PendingDBPath,
		ChannelBufferSize: config.ChannelBufferSize,
		TestDuration:      config.TestDuration,
		TotalTransactions: config.TotalTransactions,
		GenerationRate:    config.GenerationRate,
		AnalysisWorkers:   config.AnalysisWorkers,
	}

	// 환경 설정 (이전의 prepareIsolatedEnvironment 기능)
	if err := generator.SetupEnvironment(envConfig); err != nil {
		return nil, fmt.Errorf("failed to setup environment: %w", err)
	}

	// CEX Set 로딩 (이전의 cexRepo 로직)
	_, err := generator.LoadCEXSetFromFile(config.CEXFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load CEX set: %w", err)
	}

	fmt.Printf("Load MockAndHiddenDeposit from %s", config.MockDepositFile)
	if err := generator.LoadMockDepositAddresses(config.MockDepositFile); err != nil {
		return nil, fmt.Errorf("failed to load mock deposits: %w", err)
	}
	fmt.Printf("   ⚙️  TxGenerator: CEX ratio 1/%d (%.1f%%), Deposit ratio 1/%d (%.1f%%)\n",
		genConfig.DepositToCexRatio, 100.0/float64(genConfig.DepositToCexRatio),
		genConfig.RandomToDepositRatio, 100.0/float64(genConfig.RandomToDepositRatio))

	// EOAAnalyzer 생성
	analyzerConfig := &app.EOAAnalyzerConfig{
		Name:                "Fixed-Pipeline-Analyzer",
		Mode:                app.TestingMode,
		ChannelBufferSize:   config.ChannelBufferSize,
		WorkerCount:         config.AnalysisWorkers,
		StatsInterval:       2_000_000_000, // 2초
		HealthCheckInterval: 3_000_000_000, // 3초
		DataPath:            config.IsolatedDir,
		GraphDBPath:         config.GraphDBPath,
		PendingDBPath:       config.PendingDBPath,
		CEXFilePath:         config.CEXFilePath, // 격리된 환경의 CEX 파일 사용
		AutoCleanup:         true,
		ResultReporting:     true,
	}

	analyzer, err := app.CreateAnalyzer(analyzerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create analyzer: %w", err)
	}
	fmt.Printf("   ⚙️  EOAAnalyzer created with %d workers\n", config.AnalysisWorkers)

	pipeline := &TxPipeline{
		txChannel:   txChannel,
		stopChannel: stopChannel,
		generator:   generator,
		analyzer:    analyzer,
		stats: PipelineStats{
			StartTime: time.Now(),
		},
		debugStats: DebugStats{}, // 디버깅 통계 초기화
	}

	fmt.Printf("   ✅ Fixed pipeline created\n")
	return pipeline, nil
}

// runFixedPipelineTest 수정된 파이프라인 테스트 실행
func runFixedPipelineTest(pipeline *TxPipeline, config *IsolatedTestConfig) error {
	fmt.Println("\n4️⃣ Running fixed pipeline test...")

	ctx, cancel := context.WithTimeout(context.Background(), config.TestDuration)
	defer cancel()

	// 1. EOA Analyzer 시작
	analyzerDone := make(chan error, 1)
	pipeline.wg.Add(1)
	go func() {
		defer pipeline.wg.Done()
		analyzerDone <- pipeline.analyzer.Start(ctx)
	}()
	fmt.Printf("   🔄 EOA Analyzer started\n")

	// 2. TxGenerator 시작
	if err := pipeline.generator.Start(ctx); err != nil {
		return fmt.Errorf("failed to start generator: %w", err)
	}
	fmt.Printf("   🔄 TxGenerator started\n")

	// 3. 수정된 Generator → Channel 브리지
	pipeline.wg.Add(1)
	go pipeline.runFixedGeneratorBridge(ctx)
	fmt.Printf("   🌉 Fixed generator bridge started\n")

	// 4. 수정된 Channel → Analyzer 브리지
	pipeline.wg.Add(1)
	go pipeline.runFixedAnalyzerBridge(ctx)
	fmt.Printf("   🌉 Fixed analyzer bridge started\n")

	// 5. 강화된 모니터링
	pipeline.wg.Add(1)
	go pipeline.runEnhancedMonitoring(ctx)
	fmt.Printf("   📊 Enhanced monitoring started\n")

	// 6. 테스트 완료 대기
	select {
	case <-ctx.Done():
		fmt.Printf("   ⏰ Test completed by timeout\n")
	case err := <-analyzerDone:
		if err != nil {
			fmt.Printf("   ⚠️ Analyzer stopped with error: %v\n", err)
		} else {
			fmt.Printf("   ✅ Analyzer completed successfully\n")
		}
	}

	// 7. 안전한 정리
	pipeline.SafeStop()
	pipeline.wg.Wait()

	pipeline.printEnhancedResults()
	return nil
}

// runFixedGeneratorBridge 수정된 Generator 브리지 (채널 동기화 수정)
func (p *TxPipeline) runFixedGeneratorBridge(ctx context.Context) {
	defer p.wg.Done()

	generatorChannel := p.generator.GetTxChannel()

	for {
		select {
		case <-ctx.Done():
			p.safeCloseTxChannel() // 안전한 채널 닫기
			return
		case <-p.stopChannel:
			p.safeCloseTxChannel() // 안전한 채널 닫기
			return
		case tx, ok := <-generatorChannel:
			if !ok {
				// Generator가 완료됨
				p.safeCloseTxChannel() // 안전한 채널 닫기
				return
			}

			atomic.AddInt64(&p.stats.Generated, 1)

			// 디버깅: 트랜잭션 타입 분석
			//*더이상 기능할 수 없는 코드
			//*고부하 환경에서 돌렸다간 성능저하 극심&제대로된 레포팅도 아님
			//analyzeTransactionType(&tx)

			// 공유 채널로 전달 (non-blocking)
			txPtr := &tx
			select {
			case p.txChannel <- txPtr:
				atomic.AddInt64(&p.stats.Transmitted, 1)
			default:
				// 채널이 풀이면 드롭
				atomic.AddInt64(&p.stats.Dropped, 1)
			}
		}
	}
}

// analyzeTransactionType 트랜잭션 타입 분석 (디버깅용)
// func (p *TxPipeline) analyzeTransactionType(tx *shareddomain.MarkedTransaction) {
// 	// 간단한 패턴 매칭으로 타입 추정
// 	toAddrStr := tx.To.String()

// 	// CEX 주소 체크 (하드코딩 체크)
// 	if strings.HasPrefix(toAddrStr, "0x0681d8db095565fe8a346fa0277bffde9c0edbbf") ||
// 		strings.HasPrefix(toAddrStr, "0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67") ||
// 		strings.HasPrefix(toAddrStr, "0x4ed6cf63bd9c009d247ee51224fc1c7041f517f1") {
// 		atomic.AddInt64(&p.debugStats.CexToAddresses, 1)
// 		return
// 	}

// 	// Mock Deposit 주소 체크
// 	if strings.HasPrefix(toAddrStr, "0xaaaaaaaaaa") ||
// 		strings.HasPrefix(toAddrStr, "0xbbbbbbbb") ||
// 		strings.HasPrefix(toAddrStr, "0xcccccccc") {
// 		atomic.AddInt64(&p.debugStats.DepositToAddresses, 1)
// 		return
// 	}

// 	atomic.AddInt64(&p.debugStats.RandomTransactions, 1)
// }

// runFixedAnalyzerBridge 수정된 Analyzer 브리지
func (p *TxPipeline) runFixedAnalyzerBridge(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChannel:
			return
		case tx, ok := <-p.txChannel:
			if !ok {
				// 채널이 닫힘 (Generator 완료)
				return
			}

			// 분석기로 전달 (에러 처리 강화)
			if err := p.analyzer.ProcessTransaction(tx); err != nil {
				atomic.AddInt64(&p.debugStats.MatchFailures, 1)
			} else {
				atomic.AddInt64(&p.stats.Processed, 1)
			}
		}
	}
}

// runEnhancedMonitoring 강화된 실시간 모니터링
func (p *TxPipeline) runEnhancedMonitoring(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChannel:
			return
		case <-ticker.C:
			p.printEnhancedRealtimeStats()
		}
	}
}

// printEnhancedRealtimeStats 강화된 실시간 통계 출력
func (p *TxPipeline) printEnhancedRealtimeStats() {
	// 기본 통계
	generated := atomic.LoadInt64(&p.stats.Generated)
	processed := atomic.LoadInt64(&p.stats.Processed)

	// 디버깅 통계
	// cexTxs := atomic.LoadInt64(&p.debugStats.CexToAddresses)
	// depositTxs := atomic.LoadInt64(&p.debugStats.DepositToAddresses)
	// randomTxs := atomic.LoadInt64(&p.debugStats.RandomTransactions)
	//failures := atomic.LoadInt64(&p.debugStats.MatchFailures)

	uptime := time.Since(p.stats.StartTime).Seconds()
	channelUsage := len(p.txChannel)
	channelCapacity := cap(p.txChannel)

	genRate := float64(generated) / uptime
	processRate := float64(processed) / uptime
	channelPct := float64(channelUsage) / float64(channelCapacity) * 100

	// 분석기 통계
	analyzerStats := p.analyzer.GetStatistics()
	analyzerHealthy := p.analyzer.IsHealthy()

	fmt.Printf("📊 [%.1fs] Gen: %d (%.0f/s) | Proc: %d (%.0f/s) | Ch: %d/%d (%.1f%%) | Healthy: %t\n",
		uptime, generated, genRate, processed, processRate,
		channelUsage, channelCapacity, channelPct, analyzerHealthy)

	// fmt.Printf("    🎯 Types: CEX→%d (%.1f%%) | Deposit→%d (%.1f%%) | Random→%d (%.1f%%) | Fail→%d\n",
	// 	cexTxs, float64(cexTxs)/float64(generated)*100,
	// 	depositTxs, float64(depositTxs)/float64(generated)*100,
	// 	randomTxs, float64(randomTxs)/float64(generated)*100,
	// 	failures)

	// 상세 분석기 통계 (주기적)
	if int(uptime)%6 == 0 {
		fmt.Printf("    📈 Analyzer: Success: %v | Deposits: %v | Graph: %v | Window: %v | Dropped: %v\n",
			analyzerStats["success_count"], analyzerStats["deposit_detections"],
			analyzerStats["graph_updates"], analyzerStats["window_updates"],
			analyzerStats["dropped_txs"])
	}
}

// printEnhancedResults 강화된 최종 결과 출력
func (p *TxPipeline) printEnhancedResults() {
	fmt.Println("\n" + strings.Repeat("=", 90))
	fmt.Println("📊 FIXED QUEUE-BASED INTEGRATION TEST RESULTS")
	fmt.Println(strings.Repeat("=", 90))

	// Pipeline 통계
	generated := atomic.LoadInt64(&p.stats.Generated)
	transmitted := atomic.LoadInt64(&p.stats.Transmitted)
	processed := atomic.LoadInt64(&p.stats.Processed)
	dropped := atomic.LoadInt64(&p.stats.Dropped)
	uptime := time.Since(p.stats.StartTime).Seconds()

	// 디버깅 통계
	cexTxs := atomic.LoadInt64(&p.debugStats.CexToAddresses)
	depositTxs := atomic.LoadInt64(&p.debugStats.DepositToAddresses)
	randomTxs := atomic.LoadInt64(&p.debugStats.RandomTransactions)
	failures := atomic.LoadInt64(&p.debugStats.MatchFailures)

	fmt.Printf("🔢 Pipeline Stats:\n")
	fmt.Printf("   Generated:    %d transactions\n", generated)
	fmt.Printf("   Transmitted:  %d transactions\n", transmitted)
	fmt.Printf("   Processed:    %d transactions\n", processed)
	fmt.Printf("   Dropped:      %d transactions\n", dropped)
	fmt.Printf("   Runtime:      %.1f seconds\n", uptime)

	if generated > 0 {
		transmissionRate := float64(transmitted) / float64(generated) * 100
		processingRate := float64(processed) / float64(transmitted) * 100
		overallRate := float64(processed) / float64(generated) * 100

		fmt.Printf("   Transmission: %.1f%% (%d/%d)\n", transmissionRate, transmitted, generated)
		fmt.Printf("   Processing:   %.1f%% (%d/%d)\n", processingRate, processed, transmitted)
		fmt.Printf("   Overall:      %.1f%% (%d/%d)\n", overallRate, processed, generated)

		genTPS := float64(generated) / uptime
		procTPS := float64(processed) / uptime
		fmt.Printf("   Gen Rate:     %.1f tx/sec\n", genTPS)
		fmt.Printf("   Proc Rate:    %.1f tx/sec\n", procTPS)
	}

	// 디버깅 통계
	fmt.Printf("\n🎯 Transaction Type Analysis:\n")
	fmt.Printf("   CEX Transactions:     %d (%.1f%%)\n", cexTxs, float64(cexTxs)/float64(generated)*100)
	fmt.Printf("   Deposit Transactions: %d (%.1f%%)\n", depositTxs, float64(depositTxs)/float64(generated)*100)
	fmt.Printf("   Random Transactions:  %d (%.1f%%)\n", randomTxs, float64(randomTxs)/float64(generated)*100)
	fmt.Printf("   Processing Failures:  %d\n", failures)

	// 분석기 상세 통계
	fmt.Printf("\n⚡ Analyzer Details:\n")
	analyzerStats := p.analyzer.GetStatistics()
	for key, value := range analyzerStats {
		fmt.Printf("   %-20s: %v\n", key, value)
	}

	fmt.Printf("\n💚 System Health: %t\n", p.analyzer.IsHealthy())

	// 문제 진단
	fmt.Printf("\n🔧 Diagnostic Summary:\n")
	if analyzerStats["deposit_detections"].(int64) == 0 {
		fmt.Printf("   ❌ No deposit detections - check CEX address matching logic\n")
		if cexTxs == 0 {
			fmt.Printf("   ❌ No CEX transactions generated - check TxGenerator CEX ratio\n")
		}
	}
	if analyzerStats["graph_updates"].(int64) == 0 {
		fmt.Printf("   ❌ No graph updates - check deposit address detection\n")
	}

	expectedCexTxs := generated / 5 // 20% 기대치
	if cexTxs < expectedCexTxs/2 {
		fmt.Printf("   ⚠️  CEX transaction ratio lower than expected (%d vs %d expected)\n", cexTxs, expectedCexTxs)
	}

	fmt.Println(strings.Repeat("=", 90))
}

// safeCloseTxChannel 안전한 트랜잭션 채널 닫기
func (p *TxPipeline) safeCloseTxChannel() {
	p.channelOnce.Do(func() {
		close(p.txChannel)
	})
}

// SafeStop 안전한 파이프라인 중지
func (p *TxPipeline) SafeStop() {
	p.stopOnce.Do(func() {
		close(p.stopChannel)
	})
	p.generator.Stop()
	p.analyzer.Stop()
}

// SafeClose 안전한 파이프라인 리소스 정리
func (p *TxPipeline) SafeClose() error {
	p.SafeStop()
	return p.analyzer.Close()
}

// 기존 유틸 함수들 재사용
// * 상대적 관점에서의 프로젝트 루트 찾는 로직이므로, 파일 위치 바뀌면 변경 필요한 함수임
func findProjectRoot() string {
	currentDir, _ := os.Getwd()

	for currentDir != "/" {
		if strings.HasSuffix(currentDir, "chainAnalyzer") {
			return currentDir
		}

		if data, err := os.ReadFile(filepath.Join(currentDir, "go.mod")); err == nil {
			if strings.Contains(string(data), "chainAnalyzer") {
				return currentDir
			}
		}

		currentDir = filepath.Dir(currentDir)
	}

	workingDir, _ := os.Getwd()
	return filepath.Join(workingDir, "../../../")
}

// 이제 더 이상 사용하지 않는 함수들 (MockTxFeeder로 이동됨)
// prepareIsolatedEnvironment, copyFile, createMockDeposits, cleanupIsolatedEnvironment
