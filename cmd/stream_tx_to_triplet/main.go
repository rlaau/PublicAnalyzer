package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/sharedface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	relapi "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/api"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/streamfeeder/bus"
	"github.com/rlaaudgjs5638/chainAnalyzer/server"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1) 올바른 레이아웃 사용 + 에러 체크
	startTime, err := time.Parse("2006-01-02", "2023-09-01")
	if err != nil {
		log.Fatalf("startTime parse: %v", err)
	}
	endTime, err := time.Parse("2006-01-02", "2024-12-31")
	if err != nil {
		log.Fatalf("endTime parse: %v", err)
	}
	sb, err := bus.NewStreamBus(bus.StreamBusConfig{
		RootPathFunc:      func() string { return computation.FindProjectRootPath() },
		BusRelPath:        "streambus/rawtx.jsonl", // 원하면 경로 변경
		BufferSize:        100_000,                 // 버퍼(큐) 크기
		PositionDataDir:   computation.FindProductionStorageRootPath() + "/streambus_pos",
		SaveInterval:      time.Second,
		SortedStoragePath: computation.FindProjectRootPath() + "/stream_storage/sorted",
	})

	if err != nil {
		log.Fatalf("StreamBus create error: %v", err)
	}

	relPool, err := GetRelPool()
	if err != nil {
		panic("relpool생성 중 에러")
	}

	monitoringServer := server.NewServer(":8080")
	RegisterModule(monitoringServer, relPool)

	tripletBus := relPool.GetTripletPort().GetMyTxBus()

	// WaitGroup for tracking goroutines
	var wg sync.WaitGroup

	fmt.Printf("4️⃣ Running & 이벤트버스...\n")
	wg.Add(1)
	go func() {
		defer wg.Done()
		LoadAndFilterAndMarkAndIngestTransaction(sb.Bus(),
			tripletBus,
			relPool.GetApooPort().GetNodPort().GetCoPort(), ctx)
	}()
	fmt.Printf("4️⃣ Running 서버...\n")
	go func() {
		fmt.Printf("   🌐 Starting API server on :8080\n")
		if err := monitoringServer.Start(); err != nil {
			fmt.Printf("   ⚠️ API server stopped: %v\n", err)
		}
	}()
	printServerInfo()
	if err := sb.Start(); err != nil {
		log.Fatalf("Failed to start StreamBus: %v\n", err)
	}
	fmt.Printf("stream bus 스타트: %s에서 %s까지 이벤트버스 스트리밍\n.", startTime, endTime)

	// Channel to signal when StreamBetween completes
	streamDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sb.StreamBetween(
			startTime.Format("2006-01-02"),
			endTime.Format("2006-01-02"),
		); err != nil {
			log.Printf("Stream error: %v", err)
		} else {
			log.Printf("   ✅ StreamBus completed streaming all tx Between data")
		}
		close(streamDone)
	}()

	tripletDone := make(chan error, 1)
	fmt.Printf("   🔄 Triplet started with 트리플렛 버스\n")
	go func() {
		tripletDone <- relPool.GetTripletPort().Start(ctx)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	fmt.Printf("   🎯 Server and Triplet running! Press Ctrl+C to stop...\n")

	// Main event loop
	select {
	case <-streamDone:
		// StreamBus finished - wait 30 seconds before shutdown
		fmt.Printf("   ✅ StreamBus completed all data streaming\n")
		fmt.Printf("   ⏱️ Waiting 30 seconds before graceful shutdown...\n")

		// Create a timer for 30 seconds
		shutdownTimer := time.NewTimer(30 * time.Second)
		defer shutdownTimer.Stop()

		// Wait for either the timer or manual interrupt
		select {
		case <-shutdownTimer.C:
			fmt.Printf("   ⏰ 30 seconds elapsed - initiating graceful shutdown\n")
		case <-sigChan:
			fmt.Printf("   🛑 Manual shutdown signal received before timer\n")
		case err := <-tripletDone:
			if err != nil {
				fmt.Printf("   ⚠️ Triplet stopped with error: %v\n", err)
			} else {
				fmt.Printf("   ✅ Triplet completed successfully\n")
			}
		}
	case <-ctx.Done():
		fmt.Printf("   ⏰ Test completed by timeout\n")
	case err := <-tripletDone:
		if err != nil {
			fmt.Printf("   ⚠️ Triplet  stopped with error: %v\n", err)
		} else {
			fmt.Printf("   ✅ Triplet  completed successfully\n")
		}
	case <-sigChan:
		fmt.Printf("   🛑 Shutdown signal received...\n")
	}

	// Shutdown sequence
	log.Printf("   🛑 Starting shutdown sequence...")

	// 1) Cancel context to signal all goroutines immediately
	cancel()

	// 2) Stop StreamBus first to prevent new data
	log.Printf("   🛑 Stopping StreamBus...")
	if err := sb.Stop(); err != nil {
		log.Printf("   ⚠️ StreamBus stop error: %v", err)
	}

	// 3) Force stop Triplet with timeout
	log.Printf("   🛑 Stopping Triplet...")
	if err := relPool.GetTripletPort().Stop(); err != nil {
		log.Printf("   ⚠️ Triplet stop error: %v", err)
	}

	// 4) Wait for Triplet completion with shorter timeout
	select {
	case err := <-tripletDone:
		if err != nil {
			log.Printf("   ⚠️ Triplet completed with error: %v", err)
		} else {
			log.Printf("   ✅ Triplet completed successfully")
		}
	case <-time.After(30 * time.Second):
		log.Printf("   ⏱️ Triplet stop timeout - forcing shutdown")
	}

	// 5) Wait for all goroutines with shorter timeout
	log.Printf("   ⏳ Waiting for all goroutines to complete...")
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("   ✅ All goroutines stopped")
	case <-time.After(5 * time.Second):
		log.Printf("   ⚠️ Goroutine shutdown timeout - forcing exit")
	}

	// 6) Shutdown API server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()

	log.Printf("   🛑 Shutting down API server...")
	if err := monitoringServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("   ⚠️ Server shutdown error: %v", err)
	} else {
		log.Printf("   ✅ API server shutdown completed")
	}

	// 7) Final cleanup - force close all resources
	log.Printf("   🛑 Final resource cleanup...")
	if relPool.GetTripletPort() != nil {
		relPool.GetTripletPort().Close()
		log.Printf("   ✅ Triplet port closed")
	}

	log.Printf("   ✅ Shutdown sequence completed")

	// Final safety: force exit after total timeout
	go func() {
		time.Sleep(15 * time.Second)
		log.Printf("   ⚠️ EMERGENCY: Force exit after 15s timeout")
		os.Exit(1)
	}()
}

func LoadAndFilterAndMarkAndIngestTransaction(
	txBridge *eventbus.EventBus[domain.RawTransaction],
	tripletBus *eventbus.EventBus[*domain.MarkedTransaction],
	coPort sharedface.CoPort,
	ctx context.Context,
) {
	count := 0
	lastPrinted := 0

	log.Printf("🚀 Batch consumer worker started")
	in := txBridge.Dequeue()

	// Context timeout for graceful shutdown
	shutdownTimer := time.NewTimer(5 * time.Second)
	defer shutdownTimer.Stop()

	// 10초 간격으로 버스 상태를 출력하는 타이머
	statusTicker := time.NewTicker(10 * time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 Batch consumer worker stopping (context cancelled)")
			// Start shutdown timer
			shutdownTimer.Reset(2 * time.Second)
			for {
				select {
				case msg, ok := <-in:
					if !ok {
						log.Printf("🛑 RawTx bus closed during shutdown")
						return
					}
					// Process remaining messages quickly during shutdown
					if marked := transformAndFilterRawTxs(coPort, &msg); marked != nil {
						tripletBus.Publish(marked) // Ignore errors during shutdown
					}
				case <-shutdownTimer.C:
					log.Printf("🛑 Consumer shutdown timeout - forcing exit")
					return
				}
			}

		case <-statusTicker.C:
			// 10초마다 txBridge와 tripletBus 상태 출력
			log.Printf("📊 [Bus Status] txBridge - len: %d, cap: %d | tripletBus - len: %d, cap: %d",
				txBridge.Len(), txBridge.Cap(),
				tripletBus.Len(), tripletBus.Cap())

		case msg, ok := <-in:
			if !ok {
				// EventBus가 Close되어 채널 닫힘 → 정상 종료
				log.Printf("🛑 RawTx bus closed; consumer exit")
				return
			}

			// 변환/필터링
			if marked := transformAndFilterRawTxs(coPort, &msg); marked != nil {
				// Triplet 버스가 꽉 차면 잠깐 대기하거나 드롭 (정책 선택)
				// 가장 단순: 블록 송신
				if err := tripletBus.Publish(marked); err != nil {
					// 보통 Publish는 에러 없게 설계되지만, 만약 에러 타입이 있다면 로깅
					log.Printf("triplet publish err: %v", err)
				}
			}

			count++
			if count/1_000_000 > lastPrinted/1_000_000 {
				log.Printf("📈 processed %d messages", count)
				lastPrinted = count
			}
		}
	}
}

// * 두 기능을 함
// * 1) 필터: 오직 eoa to eoa만 받음
// * 2) 변환. 트랜잭션을 marking함
func transformAndFilterRawTxs(co sharedface.CoPort, tx *domain.RawTransaction) *domain.MarkedTransaction {
	from, _ := domain.ParseAddressFromString(tx.From)
	to, _ := domain.ParseAddressFromString(tx.To)
	//eoa-eoa의 거래일 경우
	//*필터링
	if !co.CheckIsContract(from) && !co.CheckIsContract(to) {
		tid, _ := domain.ParseTxId(tx.TxId)
		//*변환
		markedTx := &domain.MarkedTransaction{
			TxID:      tid,
			Nonce:     tx.Nonce,
			BlockTime: chaintimer.ParseEthTime(tx.BlockTime),

			From:        from,
			To:          to,
			TxSyntax:    [2]domain.ContractBoolMark{domain.EOAMark, domain.EOAMark},
			ValueOrNull: tx.ValueOrNull,
		}
		return markedTx
	}
	return nil
}

func RegisterModule(monitoringServer *server.Server, relPool *rel.RelationPool) {
	monitoringServer.SetupBasicRoutes()
	relAPI := relapi.NewRelPoolAPIHandler(relPool)
	if err := monitoringServer.RegisterModule(relAPI); err != nil {
		fmt.Printf("   ❌ Failed to register Relation Pool API: %v\n", err)
	} else {
		fmt.Printf("   ✅ Relation Pool API registered successfully\n")
	}

}

func printServerInfo() {

	fmt.Println("   📍 Available endpoints (Chi Router):")
	fmt.Println("   💡 API Endpoints (JSON responses):")
	fmt.Println("   - GET http://localhost:8080/api/health             - Server health")
	fmt.Println("   - GET http://localhost:8080/api/triplet/statistics      - Triplet  statistics")
	fmt.Println("   - GET http://localhost:8080/api/triplet/health          - Triplet  health")
	fmt.Println("   - GET http://localhost:8080/api/triplet/channel-status  - Triplet Channel status")
	fmt.Println("   - GET http://localhost:8080/api/triplet/dual-manager/window-stats - Window statistics")
	fmt.Println("   - GET http://localhost:8080/api/triplet/graph/stats     - Graph DB statistics")
	fmt.Println("   🌐 UI Endpoints (HTML pages):")
	fmt.Println("   - GET http://localhost:8080/ui/dashboard           - Main Dashboard")
	fmt.Println("   - GET http://localhost:8080/ui/triplet/            - Triplet Module Page")
	fmt.Println("   - GET http://localhost:8080/ui/cce/                - CCE Module Page")
	fmt.Println("   🔄 Legacy Redirects (for backward compatibility):")
	fmt.Println("   - GET http://localhost:8080/health → /api/health")
	fmt.Println("   - GET http://localhost:8080/triplet/* → /api/triplet/*")
	fmt.Println("   - GET http://localhost:8080/ → /ui/dashboard")
}
