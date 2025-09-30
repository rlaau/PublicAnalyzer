package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	aapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool"
	nodapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod"
	coapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/app"
	eoapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/eo/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	relapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	relapi "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/api"
	"github.com/rlaaudgjs5638/chainAnalyzer/server"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relPool, err := GetRelPool()
	if err != nil {
		panic("relpool생성 중 에러")
	}
	// main.go
	monitoringServer := server.NewServer("127.0.0.1:8080")

	RegisterModule(monitoringServer, relPool)
	go func() {
		fmt.Printf("   🌐 Starting API server on :8080\n")
		if err := monitoringServer.Start(); err != nil {
			fmt.Printf("   ⚠️ API server stopped: %v\n", err)
		}
	}()
	printServerInfo()

	tripletDone := make(chan error, 1)
	fmt.Printf("   🔄 Triplet started with 트리플렛 버스\n")
	go func() {
		tripletDone <- relPool.GetTripletPort().Start(ctx)
	}()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	fmt.Printf("   🎯 Server and Triplet running! Press Ctrl+C to stop...\n")

	select {

	case <-sigChan:
		fmt.Printf("   🛑 Manual shutdown signal received before timer\n")
	case err := <-tripletDone:
		if err != nil {
			fmt.Printf("   ⚠️ Triplet stopped with error: %v\n", err)
		} else {
			fmt.Printf("   ✅ Triplet completed successfully\n")
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
	log.Printf("   🛑 Starting shutdown sequence...")
	cancel()
	// 3) Force stop Triplet with timeout
	log.Printf("   🛑 Stopping Triplet...")
	if err := relPool.GetTripletPort().Stop(); err != nil {
		log.Printf("   ⚠️ Triplet stop error: %v", err)
	}
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
	// Final safety: force exit after total timeout
	go func() {
		time.Sleep(15 * time.Second)
		log.Printf("   ⚠️ EMERGENCY: Force exit after 15s timeout")
		os.Exit(1)
	}()
}

func GetRelPool() (*relapp.RelationPool, error) {
	apool := &aapp.AnalyzerPool{}
	//* nodPool을 강제로 생성
	nodPool := &nodapp.NodPool{}
	//* CO생성 및 nodPool에 강제 등록
	co := coapp.NewCo(coapp.CoCfg{Mode: mode.ProductionModeProcess}, nodPool)
	eo := eoapp.NewEo(mode.ProductionModeProcess, nodPool)
	nodPool.SetCoPort(co)
	nodPool.SetEoPort(eo)
	apool.RegisterPorts(nodPool, nil)
	relPool := relapp.ComposeRelPool(mode.ProductionModeProcess, "stream_tx_to_triplet", apool)
	apool.RegisterPorts(nodPool, relPool)
	return relPool, nil

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
