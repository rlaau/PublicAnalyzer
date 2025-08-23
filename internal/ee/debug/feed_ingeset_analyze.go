package main

import (
	"context"
	"fmt"
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
	feederdomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/domain"
)

// 테스팅 시에 사용할 고립 환경의 설정임
type IsolatedPathConfig struct {
	//고립 저장소의 루트
	RootOfIsolatedDir string
	//여기서부턴 고립 저장소에 들어갈 것들. 원본이 아닌 사본, 혹은 생성물들
	CEXFilePath     string
	MockDepositFile string
	GraphDBPath     string
	PendingDBPath   string
}

func main() {
	//고립 환경을 위한 경로 생성
	//모든 테스팅 상태는 루트/testing_storage/feed_ingest_ee_test디렉터리 내에서 생성됨
	testingRootPath := computation.FindTestingStorageRootPath()
	isolatedDir := filepath.Join(testingRootPath, "feed_ingest_ee_test")
	isolatedPathConfig := &IsolatedPathConfig{
		RootOfIsolatedDir: isolatedDir,
		CEXFilePath:       filepath.Join(isolatedDir, "cex.txt"),
		MockDepositFile:   filepath.Join(isolatedDir, "deposits.txt"),
		GraphDBPath:       filepath.Join(isolatedDir, "graph"),
		PendingDBPath:     filepath.Join(isolatedDir, "pending"),
	}
	// 테스트 시작 전에 이전 데이터 정리 (삭제 후 생성 로직
	if err := resetIsolatedEnvironmentPaths(isolatedPathConfig); err != nil {
		panic("failed pre-clean")
	}
	//테스트 시간 설정
	startTime, _ := time.Parse("2006-01-02", "2025-01-01")
	testDuration := 60 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)

	//TxDefineLoader만들기 (결정론적 트랜잭션 로더)
	//TxDefineLoader의 생성 설정에 집중한 컨피겨
	txFeederGenConfig := &feederdomain.TxGeneratorConfig{
		TotalTransactions:            4_000, // TxDefineLoader에서 생성할 결정론적 트랜잭션 수
		TransactionsPerSecond:        1_000, // 전송 속도 (결정론적이므로 빠르게)
		StartTime:                    startTime,
		TransactionsPerTimeIncrement: 1,               // 하나의 tx마다 1분이 지난 것으로 설정
		TimeIncrementDuration:        1 * time.Minute, // 1분씩 시간 증가
		DepositToCexRatio:            50,              // TxDefineLoader에서는 사용하지 않지만 호환성을 위해 유지
		RandomToDepositRatio:         30,              // TxDefineLoader에서는 사용하지 않지만 호환성을 위해 유지
	}
	//TxDefineLoader의 총 설정
	txFeederConfig := &txFeeder.TxFeederConfig{
		GenConfig: txFeederGenConfig,
		//원본 소스를 찾기 위해서 프로젝트 루트를 얻어서, 복사할 데이터가 어디 존재하는지 확인
		ProjectRootDir: computation.FindProjectRootPath(),
		// 고립 환경의 루트를 참고 용으로 받음
		TargetIsolatedTestingDir: isolatedPathConfig.RootOfIsolatedDir,
		//고립 환경에서 CEX파일을 복사 후 참조-쓰기 하는 것
		TargetIsolatedCEXFilePath:         isolatedPathConfig.CEXFilePath,
		TargetIsolatedMockDepositFilePath: isolatedPathConfig.MockDepositFile,

		BatchMode:    true,                  // 배치 모드 활성화
		BatchSize:    100,                   // 100개씩 배치 (결정론적이므로 작은 배치)
		BatchTimeout: 50 * time.Millisecond, // 50ms 타임아웃
	}
	//TxDefineLoader 생성 (결정론적 트랜잭션)
	transactionFeeder, err := txFeeder.NewTxDefineLoader(txFeederConfig)
	if err != nil {
		panic("failed to create TxDefineLoader")
	}

	//EOA analyzer 만들기
	analyzerConfig := &app.EOAAnalyzerConfig{
		Name:                "Simplified-Pipeline-Analyzer",
		Mode:                app.TestingMode,
		ChannelBufferSize:   1_000_000,
		WorkerCount:         1,
		StatsInterval:       2_000_000_000, // 2초
		HealthCheckInterval: 3_000_000_000, // 3초
		//모든 경로는 IsolatedConfiger의 경로를 씀으로써 안전하게 고립된 값만 사용함
		IsolatedDBPath:  isolatedPathConfig.RootOfIsolatedDir,
		GraphDBPath:     isolatedPathConfig.GraphDBPath,
		PendingDBPath:   isolatedPathConfig.PendingDBPath,
		CEXFilePath:     isolatedPathConfig.CEXFilePath, // 격리된 환경의 CEX 파일 사용
		AutoCleanup:     false,                          // ←★ 결과 보존 위해 비활성화
		ResultReporting: true,
	}
	analyzer, err := app.CreateAnalyzer(analyzerConfig, ctx)
	if err != nil {
		panic("failed to create analyzer")
	}
	fmt.Printf("   ✅ Simplified pipeline with API server created\n")

	// 4. 서버 생성
	fmt.Println("\n4️⃣ Running simplified pipeline test with API server...")
	monitoringServer := server.NewServer(":8080")
	monitoringServer.SetupBasicRoutes()
	// EE Analyzer API 등록
	eeAPI := api.NewEEAPIHandler(analyzer)
	if err := monitoringServer.RegisterModule(eeAPI); err != nil {
		fmt.Printf("   ❌ Failed to register EE API: %v\n", err)
	} else {
		fmt.Printf("   ✅ EE Analyzer API registered successfully\n")
	}

	//*세팅 끝.본격적으로 시작하는 파트
	// 1. 서버를 백그라운드에서 시작
	go func() {
		fmt.Printf("   🌐 Starting API server on :8080\n")
		if err := monitoringServer.Start(); err != nil {
			fmt.Printf("   ⚠️ API server stopped: %v\n", err)
		}
	}()
	printServerInfo()

	//2. TxFeeder시작
	go func() {
		if err := transactionFeeder.Start(ctx); err != nil {
			fmt.Printf("   ❌ TxGenerator failed to start: %v\n", err)
		}
	}()
	fmt.Printf("   🔄 TxGenerator started (publishing to Kafka)\n")
	// 3. EOA Analyzer 시작 (Kafka에서 트랜잭션 받기)
	analyzerDone := make(chan error, 1)
	go func() {
		analyzerDone <- analyzer.Start(ctx)
	}()
	fmt.Printf("   🔄 EOA Analyzer started with Kafka consumer\n")
	// 4. 모니터링 (간소화됨) - TxDefineLoader용
	go runDeterministicMonitoring(transactionFeeder, analyzer, ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("   🎯 Server and analyzer running! Press Ctrl+C to stop...\n")

	//*동작 마무리 후 정리 시작. 끝날 떄까지 대기
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
	shutdownCtx, shutdownServerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	fmt.Printf("   🛑 Shutting down API server...\n")
	if err := monitoringServer.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("   ⚠️ Server shutdown error: %v\n", err)
	} else {
		fmt.Printf("   ✅ API server shutdown completed\n")
	}
	// 6. 정리 (삭제는 하지 않음)
	transactionFeeder.Stop()

	//*종료 후 결과 보고 - TxDefineLoader용
	printDeterministicResults(transactionFeeder, analyzer)
	if err := generateGraphReportWithDB(isolatedPathConfig, analyzer.GraphDB()); err != nil {
		fmt.Printf("   ⚠️ Graph report failed: %v\n", err)
	} else {
		fmt.Printf("   📁 Graph report saved under: %s\n", filepath.Join(isolatedDir, "report"))
	}

	// TxDefineLoader 결과 검증
	validateDeterministicResults(transactionFeeder, analyzer)
	//* 함수 종료 후 최종 정리
	defer func() {
		cancel()
		if transactionFeeder != nil {
			transactionFeeder.Close()
		}
		if analyzer != nil {
			analyzer.Close()
		}
		if monitoringServer != nil {
			shutdownServerCancel()
		}
	}()
}

func printServerInfo() {

	fmt.Println("   📍 Available endpoints (Chi Router):")
	fmt.Println("   💡 API Endpoints (JSON responses):")
	fmt.Println("   - GET http://localhost:8080/api/health             - Server health")
	fmt.Println("   - GET http://localhost:8080/api/ee/statistics      - EE Analyzer statistics")
	fmt.Println("   - GET http://localhost:8080/api/ee/health          - EE Analyzer health")
	fmt.Println("   - GET http://localhost:8080/api/ee/channel-status  - EE Channel status")
	fmt.Println("   - GET http://localhost:8080/api/ee/dual-manager/window-stats - Window statistics")
	fmt.Println("   - GET http://localhost:8080/api/ee/graph/stats     - Graph DB statistics")
	fmt.Println("   🌐 UI Endpoints (HTML pages):")
	fmt.Println("   - GET http://localhost:8080/ui/dashboard           - Main Dashboard")
	fmt.Println("   - GET http://localhost:8080/ui/ee/                 - EE Module Page")
	fmt.Println("   - GET http://localhost:8080/ui/cce/                - CCE Module Page")
	fmt.Println("   🔄 Legacy Redirects (for backward compatibility):")
	fmt.Println("   - GET http://localhost:8080/health → /api/health")
	fmt.Println("   - GET http://localhost:8080/ee/* → /api/ee/*")
	fmt.Println("   - GET http://localhost:8080/ → /ui/dashboard")
}

// resetIsolatedEnvironmentPaths
// - 고립 환경을 "삭제 후 생성"으로 초기화
// - 루트가 없으면 "지울 것 없음" 로그만 남기고 생성 단계로 진행
func resetIsolatedEnvironmentPaths(cfg *IsolatedPathConfig) error {
	// 0) 기본 검증
	root := strings.TrimSpace(cfg.RootOfIsolatedDir)
	if root == "" {
		return fmt.Errorf("invalid root: empty")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("invalid root(abs): %w", err)
	}
	// 위험 경로 보호
	if absRoot == "/" || absRoot == "." || len(absRoot) < 5 {
		return fmt.Errorf("refusing to operate on unsafe root: %q", absRoot)
	}

	// 1) 루트 삭제 (없으면 스킵 + 로그)
	if _, statErr := os.Stat(absRoot); os.IsNotExist(statErr) {
		fmt.Printf("   ℹ️  Root not found, nothing to delete: %s\n", absRoot)
	} else {
		if err := os.RemoveAll(absRoot); err != nil {
			return fmt.Errorf("remove root failed: %w", err)
		}
	}

	// 2) 루트 및 서브 디렉터리 재생성
	dirs := []string{
		absRoot,
		cfg.GraphDBPath,
		cfg.PendingDBPath,
	}
	for _, raw := range dirs {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		d, err := filepath.Abs(raw)
		if err != nil {
			return fmt.Errorf("abs path failed for %q: %w", raw, err)
		}
		// 반드시 root 하위만 허용
		if !strings.HasPrefix(d, absRoot+string(os.PathSeparator)) && d != absRoot {
			return fmt.Errorf("refusing to create dir outside root: %q", d)
		}
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("mkdir failed for %s: %w", d, err)
		}
	}

	// 3) 파일 초기화 (있으면 삭제 후 빈 파일 생성)
	for _, f := range []string{cfg.CEXFilePath, cfg.MockDepositFile} {
		if strings.TrimSpace(f) == "" {
			continue
		}
		af, err := filepath.Abs(f)
		if err != nil {
			return fmt.Errorf("abs file path failed for %q: %w", f, err)
		}
		if !strings.HasPrefix(af, absRoot+string(os.PathSeparator)) {
			return fmt.Errorf("refusing to touch file outside root: %q", af)
		}
		_ = os.Remove(af) // 있으면 삭제
		if err := os.MkdirAll(filepath.Dir(af), 0o755); err != nil {
			return fmt.Errorf("mkdir for file dir failed: %w", err)
		}
		if err := os.WriteFile(af, []byte{}, 0o644); err != nil {
			return fmt.Errorf("create empty file failed for %s: %w", af, err)
		}
	}

	fmt.Println("   🧹 Pre-clean done (paths): removed old data (if any) and recreated dirs/files")
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

// runDeterministicMonitoring TxDefineLoader용 모니터링
func runDeterministicMonitoring(loader *txFeeder.TxDefineLoader, analyzer app.EOAAnalyzer, ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := loader.GetPipelineStats()
			analyzerStats := analyzer.GetStatistics()
			tps := loader.GetTPS()
			progress := loader.GetProgress()

			fmt.Printf("📊 [%.1fs] Gen: %d | Kafka: %d | TPS: %.0f | Progress: %.1f%% | Analyzer: %v | 🎯 DETERMINISTIC\n",
				time.Since(stats.StartTime).Seconds(),
				stats.Generated,
				stats.Transmitted,
				tps,
				progress*100,
				analyzerStats["success_count"])

			// 완료 확인
			if loader.IsCompleted() {
				fmt.Printf("🎯 DETERMINISTIC LOADING COMPLETED! All transactions sent\n")
				return
			}
		}
	}
}

// printDeterministicResults TxDefineLoader용 결과 출력
func printDeterministicResults(loader *txFeeder.TxDefineLoader, analyzer app.EOAAnalyzer) {
	stats := loader.GetPipelineStats()
	analyzerStats := analyzer.GetStatistics()
	graphStructure := loader.GetGraphStructure()

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📊 DETERMINISTIC PIPELINE TEST RESULTS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Generated: %d | Transmitted: %d | Runtime: %.1fs\n",
		stats.Generated, stats.Transmitted, time.Since(stats.StartTime).Seconds())
	fmt.Printf("Analyzer Success: %v | Healthy: %t\n",
		analyzerStats["success_count"], analyzer.IsHealthy())

	fmt.Println("\n🎯 Deterministic Graph Structure:")
	fmt.Printf("  CEX Addresses: %d\n", graphStructure.ExpectedCEXCount)
	fmt.Printf("  Deposit Addresses: %d\n", graphStructure.ExpectedDepositCount)
	fmt.Printf("  User Addresses: %d\n", graphStructure.ExpectedUserCount)
	fmt.Printf("  Total Predefined Transactions: %d\n", graphStructure.TotalTransactions)

	if len(graphStructure.TransactionsByCategory) > 0 {
		fmt.Println("  📈 Transactions by Category:")
		for category, count := range graphStructure.TransactionsByCategory {
			fmt.Printf("    - %s: %d\n", category, count)
		}
	}

	fmt.Println(strings.Repeat("=", 60))
}

// validateDeterministicResults TxDefineLoader 결과 검증
func validateDeterministicResults(loader *txFeeder.TxDefineLoader, analyzer app.EOAAnalyzer) {
	fmt.Println("\n🔍 DETERMINISTIC RESULTS VALIDATION")
	fmt.Println(strings.Repeat("-", 40))

	validation := loader.ValidateResults()

	fmt.Printf("Expected Results:\n")
	fmt.Printf("  CEX Count: %v\n", validation["expected_cex_count"])
	fmt.Printf("  Deposit Count: %v\n", validation["expected_deposit_count"])
	fmt.Printf("  User Count: %v\n", validation["expected_user_count"])
	fmt.Printf("  Total Transactions: %v\n", validation["total_transactions"])

	fmt.Printf("\nGraph Structure Validation:\n")
	fmt.Printf("  Multi-User Deposits: %v (expected: 150)\n", validation["multi_user_deposits"])
	fmt.Printf("  Single-User Deposits: %v (expected: 50)\n", validation["single_user_deposits"])
	fmt.Printf("  Multi-Deposit Users: %v (expected: 500)\n", validation["multi_deposit_users"])
	fmt.Printf("  Single-Deposit Users: %v (expected: 300)\n", validation["single_deposit_users"])
	fmt.Printf("  Inter-User Only: %v (expected: 200)\n", validation["inter_user_only_users"])
	fmt.Printf("  User-to-User Pairs: %v (expected: ~100)\n", validation["user_to_user_pairs"])

	// 분석기 결과와 비교
	analyzerStats := analyzer.GetStatistics()
	fmt.Printf("\nAnalyzer Processing Results:\n")
	fmt.Printf("  Processed Transactions: %v\n", analyzerStats["success_count"])
	fmt.Printf("  Expected vs Actual: %v / %v\n", validation["total_transactions"], analyzerStats["success_count"])
	//ropeDB결과 출력
	graphStats := analyzer.GetRopeDBStats()
	fmt.Printf("모든 노드 수 %d, 모든 로프 수 %d, 모든 트레이트 수 %d", graphStats["nodes"], graphStats["ropes"], graphStats["traits"])
	// 성공률 계산
	if expectedTotal, ok := validation["total_transactions"].(int); ok {
		if processedCount, ok := analyzerStats["success_count"].(int64); ok {
			successRate := float64(processedCount) / float64(expectedTotal) * 100
			fmt.Printf("  Success Rate: %.2f%%\n", successRate)

			if successRate >= 95.0 {
				fmt.Printf("  ✅ VALIDATION PASSED (Success Rate >= 95%%)\n")
			} else {
				fmt.Printf("  ❌ VALIDATION FAILED (Success Rate < 95%%)\n")
			}
		}
	}

	fmt.Println(strings.Repeat("-", 40))
}
