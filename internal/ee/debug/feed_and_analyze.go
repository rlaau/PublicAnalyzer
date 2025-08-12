package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/api"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/server"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	txFeeder "github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/app"
	feederDomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/domain"
)

func main() {
	runFixedIntegrationTest()
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
	config := setupIsolatedEviromentConfig()
	ctx, cancel := context.WithTimeout(context.Background(), config.TestDuration)
	defer cancel()

	// 1.5 시작 전에 이전 데이터 싹 정리(삭제 후 생성)  ←★ 추가
	if err := resetIsolatedEnvironment(config); err != nil {
		return fmt.Errorf("failed pre-clean: %w", err)
	}

	// 2. 파이프라인 생성 (API 서버 포함)
	generator, analyzer, err := createSimplifiedPipeline(config, ctx)
	if err != nil {
		return fmt.Errorf("failed to create pipeline: %w", err)
	}
	defer func() {
		if generator != nil {
			generator.Close()
		}

	}()

	// 4. 통합 테스트 실행
	if err := runSimplifiedPipelineTest(generator, analyzer, config, ctx); err != nil {
		return fmt.Errorf("pipeline test failed: %w", err)
	}

	// ✅ 여기서 Analyzer를 닫아 DB 락을 해제시킴
	if analyzer != nil {
		analyzer.Close()
	}

	// (선택) Badger가 잠금 파일 정리할 시간을 아주 살짝 줌
	time.Sleep(100 * time.Millisecond)

	// ✅ 이제 리포트 생성: Badger(RO) 오픈 성공할 타이밍
	if err := generateGraphReportWithDB(config, analyzer.GraphDB()); err != nil {
		fmt.Printf("   ⚠️ Graph report failed: %v\n", err)
	} else {
		fmt.Printf("   📁 Graph report saved under: %s\n", filepath.Join(config.IsolatedDir, "report"))
	}

	return nil
}

// setupIsolatedEviromentConfig 수정된 테스트 설정 생성
func setupIsolatedEviromentConfig() *IsolatedTestConfig {
	fmt.Println("\n1️⃣ Setting up fixed test configuration...")

	baseDir := computation.GetModuleRoot()
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
		AnalysisWorkers:   1,                //TODO 현재 기능적 저하 심각.
	}

	fmt.Printf("   ✅ Isolated directory: %s\n", config.IsolatedDir)
	fmt.Printf("   📊 Fixed config: %d txs, %d tx/sec, %d workers, %d buffer\n",
		config.TotalTransactions, config.GenerationRate, config.AnalysisWorkers, config.ChannelBufferSize)
	return config
}

// 테스트 시작 전에 이전 데이터/디렉토리를 싹 밀고 재생성한다. ←★ 추가
func resetIsolatedEnvironment(cfg *IsolatedTestConfig) error {
	paths := []string{
		cfg.IsolatedDir,
		cfg.GraphDBPath,
		cfg.PendingDBPath,
	}
	// 1) 모두 제거
	for _, p := range paths {
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("pre-clean remove failed for %s: %w", p, err)
		}
	}
	// 2) 필요한 디렉토리 재생성
	dirs := []string{
		cfg.IsolatedDir,
		cfg.GraphDBPath,
		cfg.PendingDBPath,
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("pre-clean mkdir failed for %s: %w", d, err)
		}
	}
	fmt.Println("   🧹 Pre-clean done: removed old data and recreated directories")
	return nil
}

// createSimplifiedPipeline 새로운 채널 등록 방식으로 간소화된 파이프라인 생성 (API 서버 포함)
func createSimplifiedPipeline(config *IsolatedTestConfig, ctx context.Context) (*txFeeder.TxFeeder, app.EOAAnalyzer, error) {
	fmt.Println("\n3️⃣ Creating simplified transaction pipeline with API server...")

	// TxFeeder 생성 (빈 cexSet으로 시작)
	startTime, _ := time.Parse("2006-01-02", "2025-01-01") // 단일 시간 소스: tx.BlockTime의 기준점
	genConfig := &feederDomain.TxGeneratorConfig{
		TotalTransactions:            config.TotalTransactions,
		TransactionsPerSecond:        config.GenerationRate, // 기계적으로 생성하는 시간당 tx 수
		StartTime:                    startTime,             // tx.BlockTime 기준이 되는 유일한 시작점
		TransactionsPerTimeIncrement: 1,                     // 하나의 tx마다 10분이 지난 것으로 설정 (순환 테스트 가속화)
		TimeIncrementDuration:        10 * time.Minute,      // 10분씩 시간 증가
		DepositToCexRatio:            50,                    // 1/50 비율로 CEX 주소 사용
		RandomToDepositRatio:         30,                    // 1/15 비율로 Deposit 주소 사용
	}

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

	// 배치 모드를 위한 통합 설정으로 TxFeeder 생성
	feederConfig := &txFeeder.TxFeederConfig{
		GenConfig:    genConfig,
		EnvConfig:    envConfig,
		BatchMode:    true,                  // 배치 모드 활성화
		BatchSize:    200,                   // 200개씩 배치
		BatchTimeout: 10 * time.Millisecond, // 10ms 타임아웃
	}

	transactionFeeder, err := txFeeder.NewTxFeederWithComplexConfig(feederConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create TxFeeder: %w", err)
	}
	fmt.Printf("   ⚙️  TxGenerator: CEX ratio 1/%d (%.1f%%), Deposit ratio 1/%d (%.1f%%)\n",
		genConfig.DepositToCexRatio, 100.0/float64(genConfig.DepositToCexRatio),
		genConfig.RandomToDepositRatio, 100.0/float64(genConfig.RandomToDepositRatio))

	// EOAAnalyzer 생성
	analyzerConfig := &app.EOAAnalyzerConfig{
		Name:                "Simplified-Pipeline-Analyzer",
		Mode:                app.TestingMode,
		ChannelBufferSize:   config.ChannelBufferSize,
		WorkerCount:         config.AnalysisWorkers,
		StatsInterval:       2_000_000_000, // 2초
		HealthCheckInterval: 3_000_000_000, // 3초
		FileDBPath:          config.IsolatedDir,
		GraphDBPath:         config.GraphDBPath,
		PendingDBPath:       config.PendingDBPath,
		CEXFilePath:         config.CEXFilePath, // 격리된 환경의 CEX 파일 사용
		AutoCleanup:         false,              // ←★ 결과 보존 위해 비활성화
		ResultReporting:     true,
	}
	analyzer, err := app.CreateAnalyzer(analyzerConfig, ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create analyzer: %w", err)
	}
	fmt.Printf("   ⚙️  EOAAnalyzer created with %d workers\n", config.AnalysisWorkers)

	fmt.Printf("   ✅ Simplified pipeline with API server created\n")
	return transactionFeeder, analyzer, nil
}

// runSimplifiedPipelineTest 간소화된 파이프라인 테스트 실행 (API 서버 포함)
func runSimplifiedPipelineTest(txFeeder *txFeeder.TxFeeder, analyzer app.EOAAnalyzer, config *IsolatedTestConfig, ctx context.Context) error {
	fmt.Println("\n4️⃣ Running simplified pipeline test with API server...")

	//1. HTTP 서버 생성 및 API 등록 (일시적으로 주석처리)
	srv := server.NewServer(":8080")
	srv.SetupBasicRoutes()

	// EE Analyzer API 등록
	eeAPI := api.NewEEAPIHandler(analyzer)
	if err := srv.RegisterModule(eeAPI); err != nil {
		fmt.Printf("   ❌ Failed to register EE API: %v\n", err)
	} else {
		fmt.Printf("   ✅ EE Analyzer API registered successfully\n")
	}

	// 서버를 백그라운드에서 시작
	go func() {
		fmt.Printf("   🌐 Starting API server on :8080\n")
		if err := srv.Start(); err != nil {
			fmt.Printf("   ⚠️ API server stopped: %v\n", err)
		}
	}()

	go func() {
		if err := txFeeder.Start(ctx); err != nil {
			fmt.Printf("   ❌ TxGenerator failed to start: %v\n", err)
		}
	}()
	fmt.Printf("   🔄 TxGenerator started (publishing to Kafka)\n")

	// 2. EOA Analyzer 시작 (Kafka에서 트랜잭션 받기)
	analyzerDone := make(chan error, 1)
	go func() {
		analyzerDone <- analyzer.Start(ctx)
	}()
	fmt.Printf("   🔄 EOA Analyzer started with Kafka consumer\n")

	// 3. 모니터링 (간소화됨)
	go runSimplifiedMonitoring(txFeeder, analyzer, ctx)
	fmt.Printf("   📊 Monitoring started\n")

	fmt.Println("   📍 Available endpoints:")
	fmt.Println("   - GET http://localhost:8080/health             - Server health")
	fmt.Println("   - GET http://localhost:8080/ee/statistics      - EE Analyzer statistics")
	fmt.Println("   - GET http://localhost:8080/ee/health          - EE Analyzer health")
	fmt.Println("   - GET http://localhost:8080/ee/channel-status  - EE Channel status")
	fmt.Println("   - GET http://localhost:8080/ee/dual-manager/window-stats - Window statistics")
	fmt.Println("   - GET http://localhost:8080/ee/graph/stats     - Graph DB statistics")
	fmt.Println("   - GET http://localhost:8080/                   - Web Dashboard")

	// 4. 종료 시그널 대기 및 테스트 완료 대기
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("   🎯 Server and analyzer running! Press Ctrl+C to stop...\n")

	select {
	case <-ctx.Done():
		fmt.Printf("   ⏰ Test completed by timeout\n")
	case err := <-analyzerDone:
		if err != nil {
			fmt.Printf("   ⚠️ Analyzer stopped with error: %v\n", err)
		} else {
			fmt.Printf("   ✅ Analyzer completed successfully\n")
		}
	case <-sigChan:
		fmt.Printf("   🛑 Shutdown signal received...\n")
	}

	//5. 서버 정리
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	fmt.Printf("   🛑 Shutting down API server...\n")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("   ⚠️ Server shutdown error: %v\n", err)
	} else {
		fmt.Printf("   ✅ API server shutdown completed\n")
	}

	// 6. 정리 (삭제는 하지 않음)
	txFeeder.Stop()
	printSimplifiedResults(txFeeder, analyzer)

	return nil
}

// runSimplifiedMonitoring TPS 모니터링 포함
func runSimplifiedMonitoring(generator *txFeeder.TxFeeder, analyzer app.EOAAnalyzer, ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := generator.GetPipelineStats()
			analyzerStats := analyzer.GetStatistics()
			tps := generator.GetTPS()

			fmt.Printf("📊 [%.1fs] Gen: %d | Kafka: %d | TPS: %.0f | Analyzer: %v | 🚀 BATCH MODE\n",
				time.Since(stats.StartTime).Seconds(),
				stats.Generated,
				stats.Transmitted,
				tps,
				analyzerStats["success_count"])

			// 목표 달성 확인
			if tps >= 10000 {
				fmt.Printf("🎯 TARGET ACHIEVED! TPS: %.0f >= 10,000\n", tps)
			}
		}
	}
}

// printSimplifiedResults 간소화된 결과 출력
func printSimplifiedResults(generator *txFeeder.TxFeeder, analyzer app.EOAAnalyzer) {
	stats := generator.GetPipelineStats()
	analyzerStats := analyzer.GetStatistics()

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📊 SIMPLIFIED PIPELINE TEST RESULTS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Generated: %d | Transmitted: %d | Runtime: %.1fs\n",
		stats.Generated, stats.Transmitted, time.Since(stats.StartTime).Seconds())
	fmt.Printf("Analyzer Success: %v | Healthy: %t\n",
		analyzerStats["success_count"], analyzer.IsHealthy())
	fmt.Println(strings.Repeat("=", 60))
}
