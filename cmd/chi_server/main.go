package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/cce/api"
	eeapi "github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/api"
	"github.com/rlaaudgjs5638/chainAnalyzer/server"
)

func main() {
	// 서버 주소 설정
	addr := ":8080"
	if envAddr := os.Getenv("SERVER_ADDR"); envAddr != "" {
		addr = envAddr
	}

	// Chi 라우터 기반 서버 생성
	srv := server.NewServer(addr)

	// 기본 라우트 설정
	srv.SetupBasicRoutes()

	// EE 모듈 등록 (실제 분석기는 개발 중이므로 nil 전달)
	// TODO: 실제 환경에서는 적절한 EE analyzer 인스턴스를 생성해야 함
	eeHandler := eeapi.NewEEAPIHandler(nil) // 임시로 nil 전달
	if err := srv.RegisterModule(eeHandler); err != nil {
		log.Fatalf("Failed to register EE module: %v", err)
	}

	// CCE 모듈 등록 (개발 중)
	cceHandler := api.NewCCEAPIHandler()
	if err := srv.RegisterModule(cceHandler); err != nil {
		log.Fatalf("Failed to register CCE module: %v", err)
	}

	// 서버 시작 (고루틴에서 실행)
	go func() {
		log.Printf("🚀 ChainAnalyzer Server starting on %s", addr)
		log.Printf("📊 Dashboard: http://localhost%s/ui/dashboard", addr)
		log.Printf("🔍 EE Module: http://localhost%s/ui/ee/", addr)
		log.Printf("🏢 CCE Module: http://localhost%s/ui/cce/", addr)
		log.Printf("🔌 API Health: http://localhost%s/api/health", addr)
		
		if err := srv.Start(); err != nil {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// 우아한 종료를 위한 신호 처리
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down server...")

	// 5초 타임아웃으로 서버 종료
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("✅ Server exited gracefully")
}

// mockEEAnalyzer 테스트용 간단한 EE 분석기 목 (미사용, 참고용)
type mockEEAnalyzer struct{}

func (m *mockEEAnalyzer) GetStatistics() map[string]any {
	return map[string]any{
		"processed_transactions": 12345,
		"success_rate":          0.987,
		"tps":                   156.7,
	}
}

func (m *mockEEAnalyzer) IsHealthy() bool {
	return true
}

func (m *mockEEAnalyzer) GetChannelStatus() (int, int) {
	return 75, 100 // 75/100 사용량
}

func (m *mockEEAnalyzer) GraphDB() any {
	return "mock-graphdb"
}