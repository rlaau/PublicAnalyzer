package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/app"
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
func (h *EEAPIHandler) RegisterRoutes(mux *http.ServeMux) error {
	// EE Analyzer 상태 조회 엔드포인트들
	mux.HandleFunc("/ee/statistics", h.handleGetStatistics)
	mux.HandleFunc("/ee/health", h.handleHealthCheck)
	mux.HandleFunc("/ee/channel-status", h.handleChannelStatus)
	
	// DualManager 관련 엔드포인트들
	mux.HandleFunc("/ee/dual-manager/window-stats", h.handleWindowStats)
	
	// 그래프 DB 관련 엔드포인트들 (조회용)
	mux.HandleFunc("/ee/graph/stats", h.handleGraphStats)
	
	return nil
}

// handleGetStatistics 분석기 통계 조회
func (h *EEAPIHandler) handleGetStatistics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := h.analyzer.GetStatistics()
	
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

	isHealthy := h.analyzer.IsHealthy()
	
	response := map[string]interface{}{
		"healthy": isHealthy,
		"service": "ee-analyzer",
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
		"message": "Graph database is accessible",
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