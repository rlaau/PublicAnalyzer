package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"

	"github.com/go-chi/chi/v5"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/server/utils"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
)

// TripletAPIHandler Triplet Analyzer API 핸들러
type TripletAPIHandler struct {
	analyzer app.CommonTriplet
}

// NewTripletAPIHandler Triplet API 핸들러 생성
func NewTripletAPIHandler(analyzer app.CommonTriplet) *TripletAPIHandler {
	return &TripletAPIHandler{
		analyzer: analyzer,
	}
}

// RegisterRoutes ModuleRegistrar 인터페이스 구현
// 페이지 라우팅: ui/* (HTML 페이지 서빙)
// API 라우팅: api/* (JSON API 응답)
func (h *TripletAPIHandler) RegisterRoutes(router *chi.Mux) error {
	// Get absolute path to web directory

	// API 라우팅 - JSON API 응답
	router.Route("/api/triplet", func(r chi.Router) {
		// Triplet Analyzer 상태 조회 엔드포인트들
		r.Get("/statistics", h.handleGetStatistics)
		r.Get("/health", h.handleHealthCheck)
		r.Get("/channel-status", h.handleChannelStatus)

		// DualManager 관련 엔드포인트들
		r.Get("/dual-manager/window-stats", h.handleWindowStats)

	})

	// 페이지 라우팅 - HTML 페이지 서빙
	router.Route("/ui/triplet", func(r chi.Router) {
		// Triplet 모듈 메인 페이지
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(computation.FindWebDir(), "triplet", "index.html"))
		})
		r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(computation.FindWebDir(), "triplet", "index.html"))
		})

		// 향후 추가될 Triplet 모듈 서브페이지들
		r.Get("/transactions", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 거래 분석 페이지 구현
			http.ServeFile(w, r, filepath.Join(computation.FindWebDir(), "triplet", "index.html")) // 임시로 메인 페이지 서빙
		})
		r.Get("/graph", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 그래프 조회 페이지 구현
			http.ServeFile(w, r, filepath.Join(computation.FindWebDir(), "triplet", "index.html")) // 임시로 메인 페이지 서빙
		})
		r.Get("/debug", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 디버그 도구 페이지 구현
			http.ServeFile(w, r, filepath.Join(computation.FindWebDir(), "triplet", "index.html")) // 임시로 메인 페이지 서빙
		})
	})

	// 역호환성을 위한 기존 API 엔드포인트 리다이렉트
	router.Get("/triplet/statistics", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/triplet/statistics", http.StatusMovedPermanently)
	})
	router.Get("/triplet/health", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/triplet/health", http.StatusMovedPermanently)
	})
	router.Get("/triplet/channel-status", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/triplet/channel-status", http.StatusMovedPermanently)
	})
	router.Get("/triplet/dual-manager/window-stats", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/triplet/dual-manager/window-stats", http.StatusMovedPermanently)
	})
	router.Get("/triplet/graph/stats", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/triplet/graph/stats", http.StatusMovedPermanently)
	})

	return nil
}

// handleGetStatistics 분석기 통계 조회
func (h *TripletAPIHandler) handleGetStatistics(w http.ResponseWriter, r *http.Request) {
	if h.analyzer == nil {
		utils.WriteJSONResponse(w, map[string]any{
			"processed_transactions": 0, "success_rate": 0.0, "tps": 0.0,
			"status": "analyzer_not_initialized",
		})
		return
	}
	// 혹시 모를 패닉까지도 흡수
	defer func() {
		if rec := recover(); rec != nil {
			utils.WriteJSONResponse(w, map[string]any{
				"processed_transactions": 0, "success_rate": 0.0, "tps": 0.0,
				"status": "stats_unavailable",
			})
		}
	}()
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
			"message":                "Triplet Analyzer is not initialized",
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
func (h *TripletAPIHandler) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
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
		"service": "triplet-analyzer",
		"status":  status,
	}

	w.Header().Set("Content-Type", "application/json")
	if !isHealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(response)
}

// handleChannelStatus 채널 상태 조회
func (h *TripletAPIHandler) handleChannelStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.analyzer == nil {
		utils.WriteJSONResponse(w, map[string]any{"usage": 0, "capacity": 1, "usage_percent": 0.0})
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			utils.WriteJSONResponse(w, map[string]any{"usage": 0, "capacity": 1, "usage_percent": 0.0})
		}
	}()
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
func (h *TripletAPIHandler) handleWindowStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// ✅ SimpleEOAAnalyzer가 아니거나 DualManager가 nil이면 503 대신 “빈 정상응답”으로
	if sa, ok := h.analyzer.(*app.SimpleTriplet); ok {
		if dm := sa.GetDualManager(); dm != nil {
			ws := dm.GetWindowStats()
			utils.WriteJSONResponse(w, ws)
			return
		}
	}
	// 빈 구조를 반환(프론트가 안전하게 처리)
	utils.WriteJSONResponse(w, map[string]any{
		"message": "dual manager not initialized", "ok": false,
	})
}
