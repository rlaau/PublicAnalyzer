package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/triplet/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
)

// TripletAPIHandler Triplet Analyzer API 핸들러
type TripletAPIHandler struct {
	analyzer app.TripletAnalyzer
}

// NewTripletAPIHandler Triplet API 핸들러 생성
func NewTripletAPIHandler(analyzer app.TripletAnalyzer) *TripletAPIHandler {
	return &TripletAPIHandler{
		analyzer: analyzer,
	}
}

// RegisterRoutes ModuleRegistrar 인터페이스 구현
// 페이지 라우팅: ui/* (HTML 페이지 서빙)
// API 라우팅: api/* (JSON API 응답)
func (h *TripletAPIHandler) RegisterRoutes(router *chi.Mux) error {
	// Get absolute path to web directory
	rootPath := computation.FindProjectRootPath()
	webDir := filepath.Join(rootPath, "web")

	// API 라우팅 - JSON API 응답
	router.Route("/api/triplet", func(r chi.Router) {
		// Triplet Analyzer 상태 조회 엔드포인트들
		r.Get("/statistics", h.handleGetStatistics)
		r.Get("/health", h.handleHealthCheck)
		r.Get("/channel-status", h.handleChannelStatus)

		// DualManager 관련 엔드포인트들
		r.Get("/dual-manager/window-stats", h.handleWindowStats)

		// 그래프 DB 관련 엔드포인트들 (조회용)
		// ✅ 그래프 JSON 엔드포인트
		r.Get("/graph/default", h.handleGraphDefault)
		r.Get("/graph/expand", h.handleGraphExpand) // ?v=<address>
		r.Get("/graph/trait", h.handleGraphByTrait) // ?code=<traitCode>
		r.Get("/graph/rope", h.handleGraphByRope)   // ?id=<ropeId>
		r.Get("/graph/stats", h.handleGraphStats)

		// 로프 메타데이터 엔드포인트
		r.Get("/rope-info", h.handleRopeInfo) // ?id=<ropeId>

		// PolyTrait 관련 엔드포인트들
		r.Get("/polytrait/legend", h.handlePolyTraitLegend) // PolyTrait 범례 정보
		r.Get("/polyrope/search", h.handlePolyRopeSearch)   // ?address1=<addr>&address2=<addr>&polytraitcode=<code>
	})
	router.Route("/api/triplet/graph", func(r chi.Router) {
		// GET /api/triplet/graph/default
		r.Get("/default", h.handleGraphDefault)

		// GET /api/triplet/graph/expand?vertex=0x...&depth=1
		r.Get("/expand", h.handleGraphExpand)

		// GET /api/triplet/graph/trait/{code}
		r.Get("/trait/{code}", h.handleGraphByTrait)

		// GET /api/triplet/graph/rope/{id}
		r.Get("/rope/{id}", h.handleGraphByRope)

		// GET /api/triplet/graph/stats
		r.Get("/stats", h.handleGraphStats)

		// GET /api/triplet/graph/rope-info/{id}
		r.Get("/rope-info/{id}", h.handleRopeInfo)

		// PolyTrait 관련 엔드포인트들
		r.Get("/polytrait/legend", h.handlePolyTraitLegend)
		r.Get("/polyrope/search", h.handlePolyRopeSearch)
	})
	// 페이지 라우팅 - HTML 페이지 서빙
	router.Route("/ui/triplet", func(r chi.Router) {
		// Triplet 모듈 메인 페이지
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(webDir, "triplet", "index.html"))
		})
		r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(webDir, "triplet", "index.html"))
		})

		// 향후 추가될 Triplet 모듈 서브페이지들
		r.Get("/transactions", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 거래 분석 페이지 구현
			http.ServeFile(w, r, filepath.Join(webDir, "triplet", "index.html")) // 임시로 메인 페이지 서빙
		})
		r.Get("/graph", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 그래프 조회 페이지 구현
			http.ServeFile(w, r, filepath.Join(webDir, "triplet", "index.html")) // 임시로 메인 페이지 서빙
		})
		r.Get("/debug", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 디버그 도구 페이지 구현
			http.ServeFile(w, r, filepath.Join(webDir, "triplet", "index.html")) // 임시로 메인 페이지 서빙
		})
	})

	// UI 페이지 (정적 파일 서빙; webDir/ee/graph/*.html 가정)
	router.Route("/ui/triplet/graph", func(r chi.Router) {
		// 부모(Background)
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(webDir, "triplet", "graph", "index.html"))
		})
		// 자식(Frame/Sigma)
		r.Get("/frame", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(webDir, "triplet", "graph", "frame.html"))
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
		writeJSONResponse(w, map[string]any{
			"processed_transactions": 0, "success_rate": 0.0, "tps": 0.0,
			"status": "analyzer_not_initialized",
		})
		return
	}
	// 혹시 모를 패닉까지도 흡수
	defer func() {
		if rec := recover(); rec != nil {
			writeJSONResponse(w, map[string]any{
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
		writeJSONResponse(w, map[string]any{"usage": 0, "capacity": 1, "usage_percent": 0.0})
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			writeJSONResponse(w, map[string]any{"usage": 0, "capacity": 1, "usage_percent": 0.0})
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
	if sa, ok := h.analyzer.(*app.SimpleEOAAnalyzer); ok {
		if dm := sa.GetDualManager(); dm != nil {
			ws := dm.GetWindowStats()
			writeJSONResponse(w, ws)
			return
		}
	}
	// 빈 구조를 반환(프론트가 안전하게 처리)
	writeJSONResponse(w, map[string]any{
		"message": "dual manager not initialized", "ok": false,
	})
}

// handleGraphStats 그래프 DB 통계 조회
func (h *TripletAPIHandler) handleGraphStats(w http.ResponseWriter, r *http.Request) {
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
		"service": "triplet-analyzer",
	}

	json.NewEncoder(w).Encode(response)
}

// writeJSONResponse JSON 응답 생성
func writeJSONResponse(w http.ResponseWriter, data interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(data)
}
