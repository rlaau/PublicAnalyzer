package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/sharedface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	relapi "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/api"
	"github.com/rlaaudgjs5638/chainAnalyzer/server"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startTime, _ := time.Parse("2006-01-01", "2023-09-01")
	endTime, _ := time.Parse("2006-01-01", "2023-12-31")
	streamFeeder, bp, err := GetStreamFeeder()
	if err != nil {
		panic("스트림 피더 생성 중 에러")
	}
	relPool, err := GetRelPool()
	if err != nil {
		panic("relpool생성 중 에러")
	}

	monitoringServer := server.NewServer(":8080")
	RegisterModule(monitoringServer, relPool)
	rawTxBridge := GetRawTxConsumer()
	tripletBus := relPool.GetTripletPort().GetMyTxBus()
	fmt.Printf("4️⃣ Running 카프카 & 이벤트버스...\n")
	go LoadAndFilterAndMarkAndIngestTransaction(rawTxBridge,
		tripletBus,
		relPool.GetApooPort().GetNodPort().GetCoPort(), bp, ctx)
	fmt.Printf("4️⃣ Running 서버...\n")
	go func() {
		fmt.Printf("   🌐 Starting API server on :8080\n")
		if err := monitoringServer.Start(); err != nil {
			fmt.Printf("   ⚠️ API server stopped: %v\n", err)
		}
	}()
	if err := streamFeeder.Start(); err != nil {
		log.Fatalf("Failed to start TxStreamFeeder: %v\n", err)
	}
	printServerInfo()
	fmt.Printf("stream feeder 스타트: %s에서 %s까지 카프카 스트리밍 with 백프레셔\n.", startTime.String(), endTime.String())
	go func() {
		// Example 1: Stream between specific dates
		if err := streamFeeder.StreamBetween(startTime.String(), endTime.String()); err != nil {
			log.Printf("Stream error: %v", err)
		}
	}()

	tripletDone := make(chan error, 1)
	fmt.Printf("   🔄 Triplet started with 트리플렛 버스\n")
	go func() {
		tripletDone <- relPool.GetTripletPort().Start(ctx)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	fmt.Printf("   🎯 Server and Triplet running! Press Ctrl+C to stop...\n")
	select {
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

	cancel() // 1) 전체 파이프라인 중지 신호
	// 6. 정리 (삭제는 하지 않음)
	streamFeeder.Stop()
	// 3) Kafka 배치 컨슈머 명시 종료(반드시!)
	if rawTxBridge != nil {
		rawTxBridge.Close()
	}

	// Triplet 종료 대기(이미 위 select에서 받아오지 못했을 수도 있으니 짧게 추가 대기)
	select {
	case <-tripletDone:
	case <-time.After(5 * time.Second):
		log.Printf("⏱️ Triplet stop timeout; continue shutdown")
	}

	//5. 서버 정리
	shutdownCtx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	fmt.Printf("   🛑 Shutting down API server...\n")
	if err := monitoringServer.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("   ⚠️ Server shutdown error: %v\n", err)
	} else {
		fmt.Printf("   ✅ API server shutdown completed\n")
	}
	defer func() {

		if relPool.GetTripletPort() != nil {
			relPool.GetTripletPort().Close()
		}

	}()
}

func LoadAndFilterAndMarkAndIngestTransaction(txBridge *kafka.KafkaBatchConsumer[*domain.RawTransaction],
	tripletBus *eventbus.EventBus[*domain.MarkedTransaction],
	coPort sharedface.CoPort, bp tools.CountingBackpressure,
	ctx context.Context) {

	log.Printf("🚀 Batch consumer worker started")
	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 Batch consumer worker stopping (context)")
			return
		default:
			messages, err := txBridge.ReadMessagesBatch(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("⚠️ Batch read error: %v", err)
				time.Sleep(100 * time.Millisecond) // 에러 시 짧은 대기
				continue
			}
			if len(messages) == 0 {
				// 빈 배치가 자주 온다면 과도한 루프를 피하려 소폭 sleep
				time.Sleep(5 * time.Millisecond)
				continue
			}
			batchSize := len(messages)

			// EOA to EOA만 받음
			transactions := make([]*domain.RawTransaction, 0, batchSize)

			for _, msg := range messages {
				if msg.Value != nil {
					transactions = append(transactions, msg.Value)
				} else {
					fmt.Printf("processTransactionParrell 에러:메시지를 받지 않음")
				}
			}
			//* 변환과 필터링 거쳐서 eoa-eoa만을 퍼블리시
			markedTransactions := transformAndFilterRawTxs(coPort, transactions)
			tripletBus.PublishBatch(markedTransactions)
			//*소비 자체는 모든 메시지 소비로 처리
			bp.CountConsumings(len(messages))
		}
	}
}

// * 두 기능을 함
// * 1) 필터: 오직 eoa to eoa만 받음
// * 2) 변환. 트랜잭션을 marking함
func transformAndFilterRawTxs(co sharedface.CoPort, rawTxs []*domain.RawTransaction) []*domain.MarkedTransaction {
	transactions := make([]*domain.MarkedTransaction, 0, len(rawTxs))
	for _, tx := range rawTxs {
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
			transactions = append(transactions, markedTx)
		}
		continue
	}
	return transactions
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
