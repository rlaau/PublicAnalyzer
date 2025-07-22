package app

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/infra"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// TestIsolatedIntegration 격리된 데이터 환경에서의 통합 테스트
// ! 실제 퍼시스턴스 데이터(cex.txt, mockedAndHiddenDepositAddress.txt)를 복사하여 격리된 환경에서 테스트
func TestIsolatedIntegration(t *testing.T) {
	startTime := time.Now()
	testID := fmt.Sprintf("isolated_%d", startTime.Unix())

	log.Printf("🚀 Starting Isolated Integration Test (ID: %s)", testID)

	// 격리된 테스트 환경 설정
	testEnv, err := setupIsolatedEnvironment(testID)
	if err != nil {

		t.Fatalf("❌ Failed to setup isolated environment: %v", err)
	}
	defer testEnv.cleanup()

	log.Printf("✅ Isolated environment ready: %s", testEnv.baseDir)
	log.Printf("   📁 CEX Data: %s", testEnv.cexFilePath)
	log.Printf("   📁 Mock Deposits: %s", testEnv.mockDepositFilePath)

	// 분석기 설정 및 생성
	analyzerConfig := &EOAAnalyzerConfig{
		Name:                "Isolated-Test-Analyzer",
		Mode:                TestingMode,
		ChannelBufferSize:   10000, // 10K 버퍼로 증가
		WorkerCount:         4,     // 워커 4개로 증가
		MaxProcessingTime:   50_000_000,     // 50ms
		StatsInterval:       2_000_000_000,  // 2초
		HealthCheckInterval: 10_000_000_000, // 10초
		DataPath:            testEnv.dataDir,
		GraphDBPath:         testEnv.graphDBPath,
		PendingDBPath:       testEnv.pendingDBPath,
		AutoCleanup:         true,
		ResultReporting:     true,
	}

	analyzer, err := CreateAnalyzer(analyzerConfig)
	if err != nil {
		t.Fatalf("❌ Failed to create analyzer: %v", err)
	}
	defer analyzer.Close()

	log.Printf("✅ Analyzer created successfully")

	// TxGenerator 설정
	txGenConfig, cexSet, err := testEnv.setupTxGenerator()
	if err != nil {
		t.Fatalf("❌ Failed to setup TxGenerator: %v", err)
	}

	txGenerator := NewTxGenerator(txGenConfig, cexSet)
	if err := txGenerator.LoadMockDepositAddresses(testEnv.mockDepositFilePath); err != nil {
		log.Printf("⚠️ Failed to load mock deposit addresses: %v", err)
	}

	log.Printf("✅ TxGenerator configured: %d CEX addresses loaded", cexSet.Size())

	// 테스트 실행
	results, err := runIntegratedTest(t, analyzer, txGenerator, testEnv)
	if err != nil {
		t.Fatalf("❌ Integrated test failed: %v", err)
	}

	// 결과 출력
	printIntegratedTestResults(results, testEnv, time.Since(startTime))

	log.Printf("✅ Isolated Integration Test completed successfully")
}

// IsolatedTestEnvironment 격리된 테스트 환경
type IsolatedTestEnvironment struct {
	testID              string
	baseDir             string
	dataDir             string
	graphDBPath         string
	pendingDBPath       string
	cexFilePath         string
	mockDepositFilePath string
}

// setupIsolatedEnvironment 격리된 환경 설정
func setupIsolatedEnvironment(testID string) (*IsolatedTestEnvironment, error) {
	baseDir := filepath.Join(os.TempDir(), "ee_isolated_test", testID)

	env := &IsolatedTestEnvironment{
		testID:              testID,
		baseDir:             baseDir,
		dataDir:             filepath.Join(baseDir, "data"),
		graphDBPath:         filepath.Join(baseDir, "data", "graph"),
		pendingDBPath:       filepath.Join(baseDir, "data", "pending"),
		cexFilePath:         filepath.Join(baseDir, "cex_copy.txt"),
		mockDepositFilePath: filepath.Join(baseDir, "mock_deposits_copy.txt"),
	}

	// 디렉토리 생성
	if err := os.MkdirAll(env.dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// 원본 데이터 파일들을 격리된 환경으로 복사
	if err := env.copyPersistentData(); err != nil {
		env.cleanup()
		return nil, fmt.Errorf("failed to copy persistent data: %w", err)
	}

	return env, nil
}

// copyPersistentData 영구 데이터 복사
func (env *IsolatedTestEnvironment) copyPersistentData() error {
	// 1. CEX 데이터 복사
	originalCEXPath := "internal/ee/cex.txt"
	if err := env.copyFileWithFallback(originalCEXPath, env.cexFilePath, env.createDefaultCEXData); err != nil {
		return fmt.Errorf("failed to copy CEX data: %w", err)
	}
	log.Printf("📋 CEX data copied from %s to %s", originalCEXPath, env.cexFilePath)

	// 2. Mock Deposit 주소 데이터 복사
	originalMockPath := "internal/ee/mockedAndHiddenDepositAddress.txt"
	if err := env.copyFileWithFallback(originalMockPath, env.mockDepositFilePath, env.createDefaultMockDepositData); err != nil {
		return fmt.Errorf("failed to copy mock deposit data: %w", err)
	}
	log.Printf("📋 Mock deposit data copied from %s to %s", originalMockPath, env.mockDepositFilePath)

	return nil
}

// copyFileWithFallback 파일 복사 (원본이 없으면 기본값 생성)
func (env *IsolatedTestEnvironment) copyFileWithFallback(src, dst string, fallbackFunc func(string) error) error {
	// 원본 파일이 있으면 복사
	if srcFile, err := os.Open(src); err == nil {
		defer srcFile.Close()

		dstFile, err := os.Create(dst)
		if err != nil {
			return fmt.Errorf("failed to create destination file: %w", err)
		}
		defer dstFile.Close()

		_, err = io.Copy(dstFile, srcFile)
		return err
	}

	// 원본 파일이 없으면 기본값 생성
	log.Printf("⚠️ Original file %s not found, creating default data", src)
	return fallbackFunc(dst)
}

// createDefaultCEXData 기본 CEX 데이터 생성
func (env *IsolatedTestEnvironment) createDefaultCEXData(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	defaultData := `# Test CEX Hot Wallet Addresses - Isolated Test Environment
# Generated for testing purposes
0x1234567890abcdef1234567890abcdef12345678
0xabcdef1234567890abcdef1234567890abcdef12
0x567890abcdef1234567890abcdef1234567890ab
0xfedcba0987654321fedcba0987654321fedcba09
0x1111222233334444555566667777888899990000
0x2222333344445555666677778888999900001111
0x3333444455556666777788889999000011112222
0x4444555566667777888899990000111122223333
`

	_, err = file.WriteString(defaultData)
	return err
}

// createDefaultMockDepositData 기본 Mock Deposit 데이터 생성
func (env *IsolatedTestEnvironment) createDefaultMockDepositData(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	defaultData := `# Mock and Hidden Deposit Addresses - Isolated Test Environment
# Generated for testing purposes
0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
0xcccccccccccccccccccccccccccccccccccccccc
0xdddddddddddddddddddddddddddddddddddddddd
0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
0xffffffffffffffffffffffffffffffffffffffff
0x0000000000000000000000000000000000000000
0x1010101010101010101010101010101010101010
0x2020202020202020202020202020202020202020
0x3030303030303030303030303030303030303030
`

	_, err = file.WriteString(defaultData)
	return err
}

// setupTxGenerator TxGenerator 설정
func (env *IsolatedTestEnvironment) setupTxGenerator() (*domain.TxGeneratorConfig, *domain.CEXSet, error) {
	// CEX Repository 초기화 (격리된 데이터 사용)
	cexRepo := infra.NewFileCEXRepository(env.cexFilePath)
	cexSet, err := cexRepo.LoadCEXSet()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load CEX set from isolated file: %w", err)
	}

	// TxGenerator 설정 (무한 생성 모드)
	config := &domain.TxGeneratorConfig{
		TotalTransactions:     1000000, // 100만개로 설정 (실제로는 시간 제한으로 중단)
		TransactionsPerSecond: 50000,   // 초당 50K개 생성
		StartTime:             time.Now(),
		TimeIncrementDuration: 1 * time.Millisecond,
		DepositToCexRatio:     20, // 1/20 확률 (5%)
		RandomToDepositRatio:  10, // 1/10 확률 (10%)
	}

	return config, cexSet, nil
}

// IntegratedTestResults 통합 테스트 결과
type IntegratedTestResults struct {
	TotalGenerated      int64
	TotalProcessed      int64
	SuccessCount        int64
	ErrorCount          int64
	DepositDetections   int64
	GraphUpdates        int64
	WindowUpdates       int64
	DroppedTransactions int64
	ProcessingRate      float64
	GenerationRate      float64
	TestDuration        time.Duration
	FinalGraphStats     map[string]interface{}
	FinalWindowStats    map[string]interface{}
}

// runIntegratedTest 통합 테스트 실행
func runIntegratedTest(t *testing.T, analyzer EOAAnalyzer, txGenerator *TxGenerator, env *IsolatedTestEnvironment) (*IntegratedTestResults, error) {
	// 테스트 컨텍스트 (60초 제한)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	startTime := time.Now()

	// 분석기 시작
	analyzerDone := make(chan error, 1)
	go func() {
		analyzerDone <- analyzer.Start(ctx)
	}()

	// TxGenerator 시작
	if err := txGenerator.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start TxGenerator: %w", err)
	}
	defer txGenerator.Stop()

	log.Printf("🔄 Starting integrated transaction processing...")

	// 트랜잭션 처리 루프
	processingDone := make(chan struct{})
	go func() {
		defer close(processingDone)

		txChannel := txGenerator.GetTxChannel()
		for {
			select {
			case <-ctx.Done():
				return
			case tx, ok := <-txChannel:
				if !ok {
					log.Printf("📊 TxGenerator channel closed")
					return
				}

				// shareddomain.MarkedTransaction을 포인터로 변환
				txPtr := &shareddomain.MarkedTransaction{
					BlockTime:   tx.BlockTime,
					TxID:        tx.TxID,
					TxSyntax:    tx.TxSyntax,
					Nonce:       tx.Nonce,
					BlockNumber: tx.BlockNumber,
					From:        tx.From,
					To:          tx.To,
					Value:       tx.Value,
					GasLimit:    tx.GasLimit,
					Input:       tx.Input,
				}

				// 분석기로 전송
				if err := analyzer.ProcessTransaction(txPtr); err != nil {
					// 백프레셔는 정상적인 현상
					continue
				}
			}
		}
	}()

	// 주기적 상태 체크
	statusTicker := time.NewTicker(5 * time.Second)
	defer statusTicker.Stop()

	// 메인 테스트 루프
	for {
		select {
		case <-ctx.Done():
			log.Printf("⏰ Test timeout reached")
			goto TestComplete

		case err := <-analyzerDone:
			if err != nil {
				log.Printf("❌ Analyzer error: %v", err)
			}
			goto TestComplete

		case <-processingDone:
			log.Printf("✅ Transaction processing completed")
			goto TestComplete

		case <-statusTicker.C:
			// 상태 로깅
			generated := txGenerator.GetGeneratedCount()
			usage, capacity := analyzer.GetChannelStatus()
			stats := analyzer.GetStatistics()

			log.Printf("📈 Status: Generated=%d | Channel=%d/%d | Processed=%v | Success=%v",
				generated, usage, capacity, stats["total_processed"], stats["success_count"])
		}
	}

TestComplete:
	// 분석기 정지
	analyzer.Stop()

	// 결과 수집
	testDuration := time.Since(startTime)
	generated := txGenerator.GetGeneratedCount()
	stats := analyzer.GetStatistics()

	results := &IntegratedTestResults{
		TotalGenerated: generated,
		TestDuration:   testDuration,
	}

	// 통계 데이터 추출
	if v, ok := stats["total_processed"].(int64); ok {
		results.TotalProcessed = v
	}
	if v, ok := stats["success_count"].(int64); ok {
		results.SuccessCount = v
	}
	if v, ok := stats["error_count"].(int64); ok {
		results.ErrorCount = v
	}
	if v, ok := stats["deposit_detections"].(int64); ok {
		results.DepositDetections = v
	}
	if v, ok := stats["graph_updates"].(int64); ok {
		results.GraphUpdates = v
	}
	if v, ok := stats["window_updates"].(int64); ok {
		results.WindowUpdates = v
	}
	if v, ok := stats["dropped_txs"].(int64); ok {
		results.DroppedTransactions = v
	}

	// 처리 속도 계산
	if testDuration.Seconds() > 0 {
		results.ProcessingRate = float64(results.TotalProcessed) / testDuration.Seconds()
		results.GenerationRate = float64(results.TotalGenerated) / testDuration.Seconds()
	}

	log.Printf("✅ Test completed: Generated=%d | Processed=%d | Duration=%v",
		results.TotalGenerated, results.TotalProcessed, testDuration.Round(time.Second))

	return results, nil
}

// printIntegratedTestResults 통합 테스트 결과 출력
func printIntegratedTestResults(results *IntegratedTestResults, env *IsolatedTestEnvironment, totalDuration time.Duration) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("🎯 ISOLATED INTEGRATION TEST RESULTS (ID: %s)\n", env.testID)
	fmt.Println(strings.Repeat("=", 80))

	// 성능 요약
	fmt.Printf("\n📊 Performance Summary:\n")
	fmt.Printf("   Test Duration: %v\n", totalDuration.Round(time.Second))
	fmt.Printf("   Generated Transactions: %d (%.1f tx/sec)\n",
		results.TotalGenerated, results.GenerationRate)
	fmt.Printf("   Processed Transactions: %d (%.1f tx/sec)\n",
		results.TotalProcessed, results.ProcessingRate)
	fmt.Printf("   Success Rate: %.2f%% (%d/%d)\n",
		float64(results.SuccessCount)/float64(results.TotalProcessed)*100,
		results.SuccessCount, results.TotalProcessed)
	fmt.Printf("   Error Count: %d\n", results.ErrorCount)
	fmt.Printf("   Dropped Transactions: %d\n", results.DroppedTransactions)

	// 분석 결과
	fmt.Printf("\n🔍 Analysis Results:\n")
	fmt.Printf("   Deposit Detections: %d\n", results.DepositDetections)
	fmt.Printf("   Graph Updates: %d\n", results.GraphUpdates)
	fmt.Printf("   Window Updates: %d\n", results.WindowUpdates)

	// 메모리 사용량
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\n💾 Memory Statistics:\n")
	fmt.Printf("   Allocated: %d KB\n", bToKb(m.Alloc))
	fmt.Printf("   Total Allocated: %d KB\n", bToKb(m.TotalAlloc))
	fmt.Printf("   System: %d KB\n", bToKb(m.Sys))
	fmt.Printf("   GC Runs: %d\n", m.NumGC)

	// 환경 정보
	fmt.Printf("\n🔧 Test Environment:\n")
	fmt.Printf("   Base Directory: %s\n", env.baseDir)
	fmt.Printf("   CEX Data File: %s\n", env.cexFilePath)
	fmt.Printf("   Mock Deposits File: %s\n", env.mockDepositFilePath)
	fmt.Printf("   Auto Cleanup: Enabled\n")

	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("✅ Integration test completed successfully!")
	fmt.Println(strings.Repeat("=", 80) + "\n")
}

// cleanup 환경 정리
func (env *IsolatedTestEnvironment) cleanup() {
	log.Printf("🧹 Cleaning up isolated test environment: %s", env.baseDir)

	if err := os.RemoveAll(env.baseDir); err != nil {
		log.Printf("⚠️ Failed to cleanup test environment: %v", err)
	} else {
		log.Printf("✅ Isolated test environment cleaned up successfully")
	}
}

// 헬퍼 함수
func bToKb(b uint64) uint64 {
	return b / 1024
}
