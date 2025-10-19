package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	relapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	txFeeder "github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/app"
	feederDomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/domain"
)

func main() {
	log.Println("🚀 Starting Simple EOA Load Test")

	// 임시 테스트 디렉토리 생성
	testDir := filepath.Join(os.TempDir(), "simple_eoa_test", "test_"+time.Now().Format("20060102_150405"))

	// 종료 시 정리
	defer func() {
		log.Printf("🧹 Cleaning up test data: %s", testDir)
		if err := os.RemoveAll(testDir); err != nil {
			log.Printf("⚠️ Cleanup error: %v", err)
		} else {
			log.Printf("✅ Test data cleaned up")
		}
	}()

	// 테스트 설정
	config := &app.TripletConfig{
		Name:                "Simple-Load-Test",
		Mode:                mode.TestingModeProcess,
		BusCapLimit:         500,
		WorkerCount:         2,
		MaxProcessingTime:   50_000_000,
		StatsInterval:       3_000_000_000, // 3초
		HealthCheckInterval: 5_000_000_000, // 5초
		IsolatedDBPath:      testDir,
		AutoCleanup:         true,
		ResultReporting:     true,
	}

	// 컨텍스트 설정 (15초 테스트)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	relPool, err := relapp.CreateRelationPoolFrame(mode.TestingModeProcess, nil)
	// 분석기 생성
	analyzer, err := app.NewTriplet(config)
	relPool.Register(analyzer, nil)
	if err != nil {
		log.Fatalf("❌ Failed to create analyzer: %v", err)
	}
	defer analyzer.Close()

	log.Println("✅ Analyzer created successfully")

	// 분석기 시작
	analyzerDone := make(chan error, 1)
	go func() {
		analyzerDone <- analyzer.Start(ctx)
	}()

	// // TxGenerator 설정 및 시작
	// cexRepo := infra.NewFileCEXRepository("internal/triplet/cex.txt")
	// cexSet, err := cexRepo.LoadCEXSet()
	// if err != nil {
	// 	log.Printf("⚠️ Failed to load CEX set: %v", err)
	// 	cexSet = shareddomain.NewCEXSet() // 빈 CEX 세트로 계속
	// }

	genConfig := &feederDomain.TxGeneratorConfig{
		TotalTransactions:     5000, // 5000개 트랜잭션
		TransactionsPerSecond: 500,  // 초당 500개
		StartTime:             time.Now(),
		TimeIncrementDuration: 2 * time.Millisecond,
		DepositToCexRatio:     50,
		RandomToDepositRatio:  20,
	}
	cexSet := &domain.CEXSet{}

	txGenerator := txFeeder.GetRawTxFeeder(genConfig, cexSet)

	// 모의 입금 주소 로드 (선택사항)
	if err := txGenerator.LoadMockDepositAddresses("internal/triplet/mockedAndHiddenDepositAddress.txt"); err != nil {
		log.Printf("⚠️ Failed to load mock deposit addresses: %v (continuing...)", err)
	}

	// 트랜잭션 생성기 시작
	if err := txGenerator.Start(ctx); err != nil {
		log.Fatalf("❌ Failed to start transaction generator: %v", err)
	}
	defer txGenerator.Stop()

	log.Println("📊 Starting transaction processing...")

	// 트랜잭션 처리
	go func() {
		txChannel := txGenerator.GetTxChannel()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-txChannel:
				if !ok {
					return
				}

				// 트랜잭션 포인터로 변환
				// txPtr := &shareddomain.MarkedTransaction{
				// 	BlockTime: tx.BlockTime,
				// 	TxID:      tx.TxID,
				// 	TxSyntax:  tx.TxSyntax,
				// 	Nonce:     tx.Nonce,
				// 	From:      tx.From,
				// 	To:        tx.To,
				// }

				// // 분석기로 전송
				// if err := analyzer.ProcessTransaction(txPtr); err != nil {
				// 	// 백프레셔 발생 시 무시하고 계속
				// 	continue
				// }
			}
		}
	}()

	// 주기적 상태 체크
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// 테스트 실행
	for {
		select {
		case <-ctx.Done():
			log.Println("⏰ Test duration completed")
			goto TestComplete

		case err := <-analyzerDone:
			if err != nil {
				log.Printf("❌ Analyzer error: %v", err)
			}
			goto TestComplete

		case <-ticker.C:
			generated := txGenerator.GetGeneratedCount()
			usage, capacity := analyzer.GetChannelStatus()
			stats := analyzer.GetStatistics()

			log.Printf("📈 Status: Generated=%d, Channel=%d/%d, Processed=%v",
				generated, usage, capacity, stats["total_processed"])
		}
	}

TestComplete:
	// 분석기 정지
	analyzer.Close()

	// 완료 대기
	select {
	case err := <-analyzerDone:
		if err != nil {
			log.Printf("⚠️ Analyzer shutdown error: %v", err)
		}
	case <-time.After(5 * time.Second):
		log.Printf("⚠️ Analyzer shutdown timeout")
	}

	// 최종 결과 출력
	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("🎯 SIMPLE LOAD TEST RESULTS")
	log.Println(strings.Repeat("=", 60))

	generated := txGenerator.GetGeneratedCount()
	stats := analyzer.GetStatistics()

	log.Printf("📊 Generation: %d transactions", generated)
	log.Printf("📊 Processing: %v transactions", stats["total_processed"])
	log.Printf("📊 Success Rate: %.1f%%",
		float64(stats["success_count"].(int64))/float64(stats["total_processed"].(int64))*100)
	log.Printf("📊 Deposits Detected: %v", stats["deposit_detections"])
	log.Printf("📊 Graph Updates: %v", stats["graph_updates"])
	log.Printf("📊 Window Updates: %v", stats["window_updates"])

	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("✅ Simple Load Test Completed!")
}
