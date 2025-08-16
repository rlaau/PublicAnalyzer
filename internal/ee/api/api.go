package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
)

// EEAPIHandler EE Analyzer API 핸들러
type EEAPIHandler struct {
	analyzer app.EOAAnalyzer
}

// NewEEAPIHandler EE API 핸들러 생성
func NewEEAPIHandler(analyzer app.EOAAnalyzer) *EEAPIHandler {
	return &EEAPIHandler{
		analyzer: analyzer,
	}
}

// RegisterRoutes ModuleRegistrar 인터페이스 구현
// 페이지 라우팅: ui/* (HTML 페이지 서빙)
// API 라우팅: api/* (JSON API 응답)
func (h *EEAPIHandler) RegisterRoutes(router *chi.Mux) error {
	// Get absolute path to web directory
	rootPath := computation.FindProjectRootPath()
	webDir := filepath.Join(rootPath, "web")

	// API 라우팅 - JSON API 응답
	router.Route("/api/ee", func(r chi.Router) {
		// EE Analyzer 상태 조회 엔드포인트들
		r.Get("/statistics", h.handleGetStatistics)
		r.Get("/health", h.handleHealthCheck)
		r.Get("/channel-status", h.handleChannelStatus)

		// DualManager 관련 엔드포인트들
		r.Get("/dual-manager/window-stats", h.handleWindowStats)

		// 그래프 DB 관련 엔드포인트들 (조회용)
		r.Get("/graph/stats", h.handleGraphStats)
	})

	// 페이지 라우팅 - HTML 페이지 서빙
	router.Route("/ui/ee", func(r chi.Router) {
		// EE 모듈 메인 페이지
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(webDir, "ee", "index.html"))
		})
		r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(webDir, "ee", "index.html"))
		})

		// 향후 추가될 EE 모듈 서브페이지들
		r.Get("/transactions", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 거래 분석 페이지 구현
			http.ServeFile(w, r, filepath.Join(webDir, "ee", "index.html")) // 임시로 메인 페이지 서빙
		})
		r.Get("/graph", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 그래프 조회 페이지 구현
			http.ServeFile(w, r, filepath.Join(webDir, "ee", "index.html")) // 임시로 메인 페이지 서빙
		})
		r.Get("/debug", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 디버그 도구 페이지 구현
			http.ServeFile(w, r, filepath.Join(webDir, "ee", "index.html")) // 임시로 메인 페이지 서빙
		})
	})

	// 역호환성을 위한 기존 API 엔드포인트 리다이렉트
	router.Get("/ee/statistics", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/ee/statistics", http.StatusMovedPermanently)
	})
	router.Get("/ee/health", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/ee/health", http.StatusMovedPermanently)
	})
	router.Get("/ee/channel-status", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/ee/channel-status", http.StatusMovedPermanently)
	})
	router.Get("/ee/dual-manager/window-stats", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/ee/dual-manager/window-stats", http.StatusMovedPermanently)
	})
	router.Get("/ee/graph/stats", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/ee/graph/stats", http.StatusMovedPermanently)
	})

	return nil
}

// handleGetStatistics 분석기 통계 조회
func (h *EEAPIHandler) handleGetStatistics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// analyzer가 nil인 경우 기본 응답 반환
	var stats map[string]interface{}
	if h.analyzer == nil {
		stats = map[string]interface{}{
			"processed_transactions": 0,
			"success_rate":           0.0,
			"tps":                    0.0,
			"status":                 "analyzer_not_initialized",
			"message":                "EE Analyzer is not initialized",
		}
	} else {
		stats = h.analyzer.GetStatistics()
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// handleHealthCheck 헬스 체크
func (h *EEAPIHandler) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// analyzer가 nil인 경우 비건강 상태로 응답
	var isHealthy bool
	var status string
	if h.analyzer == nil {
		isHealthy = false
		status = "analyzer_not_initialized"
	} else {
		isHealthy = h.analyzer.IsHealthy()
		status = "running"
	}

	response := map[string]interface{}{
		"healthy": isHealthy,
		"service": "ee-analyzer",
		"status":  status,
	}

	w.Header().Set("Content-Type", "application/json")
	if !isHealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(response)
}

// handleChannelStatus 채널 상태 조회
func (h *EEAPIHandler) handleChannelStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	usage, capacity := h.analyzer.GetChannelStatus()
	usagePercent := float64(usage) / float64(capacity) * 100

	response := map[string]interface{}{
		"usage":         usage,
		"capacity":      capacity,
		"usage_percent": usagePercent,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleWindowStats DualManager 윈도우 통계 조회
func (h *EEAPIHandler) handleWindowStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// SimpleEOAAnalyzer에서 DualManager에 접근
	if analyzer, ok := h.analyzer.(*app.SimpleEOAAnalyzer); ok {
		dualManager := analyzer.GetDualManager()
		windowStats := dualManager.GetWindowStats()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(windowStats); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}
	} else {
		http.Error(w, "DualManager not accessible", http.StatusServiceUnavailable)
		return
	}
}

// handleGraphStats 그래프 DB 통계 조회
func (h *EEAPIHandler) handleGraphStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// BadgerDB에 직접 접근하여 통계 조회
	db := h.analyzer.GraphDB()
	if db == nil {
		http.Error(w, "Graph DB not accessible", http.StatusServiceUnavailable)
		return
	}

	// 간단한 DB 통계 생성 (실제 구현은 infra 레이어에서 처리)
	response := map[string]interface{}{
		"database_available": true,
		"message":            "Graph database is accessible",
	}

	// 추가적인 통계가 필요한 경우 infra 레이어의 GraphRepo를 통해 조회
	// 현재는 기본적인 접근 가능성만 확인

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// 유틸리티 함수들

// parseIntParam URL 파라미터에서 정수값 파싱
func parseIntParam(r *http.Request, paramName string, defaultValue int) int {
	if value := r.URL.Query().Get(paramName); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

// writeErrorResponse 에러 응답 생성
func writeErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := map[string]string{
		"error":   message,
		"service": "ee-analyzer",
	}

	json.NewEncoder(w).Encode(response)
}

// writeJSONResponse JSON 응답 생성
func writeJSONResponse(w http.ResponseWriter, data interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(data)
}
