package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/infra"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
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
	generator *app.TxGenerator
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

	// 1. 테스트 설정 (개선됨)
	config := setupFixedTestConfig()
	defer cleanupIsolatedEnvironment(config)

	// 2. 격리된 환경 구성
	if err := prepareIsolatedEnvironment(config); err != nil {
		log.Fatalf("❌ Failed to prepare environment: %v", err)
	}

	// 3. 파이프라인 생성 (수정버전)
	pipeline, err := createFixedTxPipeline(config)
	if err != nil {
		log.Fatalf("❌ Failed to create pipeline: %v", err)
	}
	defer pipeline.SafeClose()

	// 4. 통합 테스트 실행
	if err := runFixedPipelineTest(pipeline, config); err != nil {
		log.Fatalf("❌ Pipeline test failed: %v", err)
	}

	fmt.Println("\n✅ Fixed integration test completed successfully!")
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

		// 극소 테스트 설정 - CEX 매칭에 집중
		ChannelBufferSize: 1_000_000,         // 최소 버퍼
		TestDuration:      200 * time.Second, // 매우 짧은 테스트
		TotalTransactions: 100_00000,         // 극소 데이터로 빠른 결과 확인
		GenerationRate:    10_000,            // 매우 느린 생성률
		AnalysisWorkers:   8,                 // 워커 4개 유지
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

	// CEX Set 로딩 및 검증
	fmt.Printf("   🔍 CEX file path: %s\n", config.CEXFilePath)

	// 파일 존재 확인
	if _, err := os.Stat(config.CEXFilePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("CEX file does not exist: %s", config.CEXFilePath)
	}

	cexRepo := infra.NewFileCEXRepository(config.CEXFilePath)
	cexSet, err := cexRepo.LoadCEXSet()
	if err != nil {
		return nil, fmt.Errorf("failed to load CEX set: %w", err)
	}
	fmt.Printf("   📦 CEX addresses loaded: %d\n", cexSet.Size())

	// CEX 로딩이 실패한 경우 추가 디버깅
	if cexSet.Size() == 0 {
		fmt.Printf("   ❌ CEX loading failed - checking file contents...\n")
		if fileData, err := os.ReadFile(config.CEXFilePath); err == nil {
			lines := strings.Split(string(fileData), "\n")
			nonEmptyLines := 0
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "#") {
					nonEmptyLines++
				}
			}
			fmt.Printf("   📄 File contains %d lines, %d non-empty non-comment lines\n", len(lines), nonEmptyLines)
		}
		return nil, fmt.Errorf("no CEX addresses loaded from file")
	}

	// CEX 주소 샘플 출력 (디버깅)
	cexAddresses := cexSet.GetAll()
	if len(cexAddresses) >= 3 {
		fmt.Printf("   🔍 CEX samples: %s, %s, %s\n",
			cexAddresses[0][:10]+"...",
			cexAddresses[1][:10]+"...",
			cexAddresses[2][:10]+"...")
	}

	// TxGenerator 생성 (CEX 비율 증가)
	genConfig := &domain.TxGeneratorConfig{
		TotalTransactions:            config.TotalTransactions,
		TransactionsPerSecond:        config.GenerationRate, //기계적으로 생성하는 시간당 tx수
		StartTime:                    time.Now(),
		TransactionsPerTimeIncrement: 1,           //하나의 tx마나 1초가 지난 것으로 설정
		TimeIncrementDuration:        time.Second, //1초씩 시간 증가
		DepositToCexRatio:            50,          // 1/50 비율로 CEX 주소 사용
		RandomToDepositRatio:         30,          //1/15 비율로 Deposit 주소 사용
	}

	generator := app.NewTxGenerator(genConfig, cexSet)
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
			p.analyzeTransactionType(&tx)

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
func (p *TxPipeline) analyzeTransactionType(tx *shareddomain.MarkedTransaction) {
	// 간단한 패턴 매칭으로 타입 추정
	toAddrStr := tx.To.String()

	// CEX 주소 체크 (하드코딩 체크)
	if strings.HasPrefix(toAddrStr, "0x0681d8db095565fe8a346fa0277bffde9c0edbbf") ||
		strings.HasPrefix(toAddrStr, "0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67") ||
		strings.HasPrefix(toAddrStr, "0x4ed6cf63bd9c009d247ee51224fc1c7041f517f1") {
		atomic.AddInt64(&p.debugStats.CexToAddresses, 1)
		return
	}

	// Mock Deposit 주소 체크
	if strings.HasPrefix(toAddrStr, "0xaaaaaaaaaa") ||
		strings.HasPrefix(toAddrStr, "0xbbbbbbbb") ||
		strings.HasPrefix(toAddrStr, "0xcccccccc") {
		atomic.AddInt64(&p.debugStats.DepositToAddresses, 1)
		return
	}

	atomic.AddInt64(&p.debugStats.RandomTransactions, 1)
}

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
	cexTxs := atomic.LoadInt64(&p.debugStats.CexToAddresses)
	depositTxs := atomic.LoadInt64(&p.debugStats.DepositToAddresses)
	randomTxs := atomic.LoadInt64(&p.debugStats.RandomTransactions)
	failures := atomic.LoadInt64(&p.debugStats.MatchFailures)

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

	fmt.Printf("    🎯 Types: CEX→%d (%.1f%%) | Deposit→%d (%.1f%%) | Random→%d (%.1f%%) | Fail→%d\n",
		cexTxs, float64(cexTxs)/float64(generated)*100,
		depositTxs, float64(depositTxs)/float64(generated)*100,
		randomTxs, float64(randomTxs)/float64(generated)*100,
		failures)

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

func prepareIsolatedEnvironment(config *IsolatedTestConfig) error {
	fmt.Println("\n2️⃣ Preparing isolated environment...")

	os.RemoveAll(config.IsolatedDir)
	if err := os.MkdirAll(config.IsolatedDir, 0755); err != nil {
		return fmt.Errorf("failed to create isolated directory: %w", err)
	}

	// CEX 데이터 복제
	sourceCEX := filepath.Join(config.BaseDir, "internal", "ee", "cex.txt")
	fmt.Printf("   🔍 Source CEX: %s\n", sourceCEX)
	fmt.Printf("   🔍 Target CEX: %s\n", config.CEXFilePath)

	// 소스 파일 존재 확인
	if _, err := os.Stat(sourceCEX); os.IsNotExist(err) {
		return fmt.Errorf("source CEX file does not exist: %s", sourceCEX)
	}

	if err := copyFile(sourceCEX, config.CEXFilePath); err != nil {
		return fmt.Errorf("failed to copy CEX file: %w", err)
	}

	// 복사 후 검증
	if copiedData, err := os.ReadFile(config.CEXFilePath); err == nil {
		lines := strings.Split(string(copiedData), "\n")
		nonEmptyLines := 0
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				nonEmptyLines++
			}
		}
		fmt.Printf("   📄 CEX data copied - %d lines, %d addresses\n", len(lines), nonEmptyLines)
	} else {
		fmt.Printf("   ⚠️  CEX data copied but could not verify: %v\n", err)
	}

	// 모의 입금 주소 생성
	if err := createMockDeposits(config.MockDepositFile); err != nil {
		return fmt.Errorf("failed to create mock deposits: %w", err)
	}
	fmt.Printf("   📄 Mock deposits created\n")

	fmt.Printf("   ✅ Environment prepared\n")
	return nil
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

// * 제너레이터는 mockedAndHiddenDepositAddress.txt 파일을 "debug"용 tmp폴더에 create로 복사 후, 그 파일을 로드함
func createMockDeposits(filePath string) error {
	fmt.Printf("   🔍 Creating mock deposit addresses at %s\n", filePath)
	file, err := os.Create(filePath)

	if err != nil {
		return err
	}
	defer file.Close()
	root := findProjectRoot()

	depositFilePath := filepath.Join(root, "internal", "ee", "mockedAndHiddenDepositAddress.txt")
	fmt.Printf("loading mockedAndHiddenDepositAddress.txt from %s\n", depositFilePath)

	deposits, err := os.Open(depositFilePath)
	if err != nil {
		return err
	}
	defer deposits.Close()

	file.WriteString("# Mock Deposit Addresses for Fixed Queue Test\n\n")
	// 4. 한 줄씩 읽어서 복사
	scanner := bufio.NewScanner(deposits)
	totalLength := 0
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		file.WriteString(line + "\n")
		totalLength += len(line)
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading deposit file: %w", err)
	}
	fmt.Printf("   ✅ Copied %d lines (total %d bytes of address strings)\n", lineCount, totalLength)
	return nil
}

func cleanupIsolatedEnvironment(config *IsolatedTestConfig) {
	fmt.Println("\n🧹 Cleaning up isolated environment...")

	if err := os.RemoveAll(config.IsolatedDir); err != nil {
		log.Printf("⚠️ Warning: cleanup failed: %v", err)
	} else {
		fmt.Printf("   ✅ Cleaned: %s\n", config.IsolatedDir)
	}

	fmt.Println("🔒 No permanent changes to system")
}
