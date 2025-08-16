package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"

	"github.com/go-chi/chi/v5"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
)

// CCEAPIHandler CCE Analyzer API 핸들러 (개발 중)
type CCEAPIHandler struct {
	// TODO: CCE analyzer 인터페이스 추가 예정
}

// NewCCEAPIHandler CCE API 핸들러 생성
func NewCCEAPIHandler() *CCEAPIHandler {
	return &CCEAPIHandler{}
}

// RegisterRoutes ModuleRegistrar 인터페이스 구현
// 페이지 라우팅: ui/* (HTML 페이지 서빙)
// API 라우팅: api/* (JSON API 응답)
func (h *CCEAPIHandler) RegisterRoutes(router *chi.Mux) error {
	// Get absolute path to web directory
	rootPath := computation.FindProjectRootPath()
	webDir := filepath.Join(rootPath, "web")

	// API 라우팅 - JSON API 응답 (개발 중)
	router.Route("/api/cce", func(r chi.Router) {
		// CCE Analyzer 상태 조회 엔드포인트들 (개발 예정)
		r.Get("/health", h.handleHealthCheck)
		r.Get("/statistics", h.handleGetStatistics)
		r.Get("/interactions", h.handleGetInteractions)
		r.Get("/defi", h.handleGetDeFiStats)
	})

	// 페이지 라우팅 - HTML 페이지 서빙
	router.Route("/ui/cce", func(r chi.Router) {
		// CCE 모듈 메인 페이지
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(webDir, "cce", "index.html"))
		})
		r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(webDir, "cce", "index.html"))
		})

		// 향후 추가될 CCE 모듈 서브페이지들
		r.Get("/interactions", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 컨트랙트 상호작용 분석 페이지 구현
			http.ServeFile(w, r, filepath.Join(webDir, "cce", "index.html")) // 임시로 메인 페이지 서빙
		})
		r.Get("/defi", func(w http.ResponseWriter, r *http.Request) {
			// TODO: DeFi 모니터링 페이지 구현
			http.ServeFile(w, r, filepath.Join(webDir, "cce", "index.html")) // 임시로 메인 페이지 서빙
		})
		r.Get("/visualization", func(w http.ResponseWriter, r *http.Request) {
			// TODO: 그래프 시각화 페이지 구현
			http.ServeFile(w, r, filepath.Join(webDir, "cce", "index.html")) // 임시로 메인 페이지 서빙
		})
	})

	return nil
}

// handleHealthCheck 헬스 체크 (개발 중)
func (h *CCEAPIHandler) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// TODO: 실제 CCE analyzer 상태 확인 로직 구현
	response := map[string]any{
		"healthy": false, // 개발 중이므로 false
		"service": "cce-analyzer",
		"status":  "under_development",
		"message": "CCE Analyzer module is under development",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable) // 개발 중이므로 503

	json.NewEncoder(w).Encode(response)
}

// handleGetStatistics 분석기 통계 조회 (개발 중)
func (h *CCEAPIHandler) handleGetStatistics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// TODO: 실제 CCE analyzer 통계 구현
	stats := map[string]any{
		"analyzed_contracts":     0,
		"contract_interactions":  0,
		"defi_protocols_tracked": 0,
		"success_rate":           0.0,
		"tps":                    0.0,
		"status":                 "under_development",
		"message":                "Statistics will be available when CCE module is implemented",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleGetInteractions 컨트랙트 상호작용 조회 (개발 중)
func (h *CCEAPIHandler) handleGetInteractions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// TODO: 실제 컨트랙트 상호작용 분석 구현
	response := map[string]any{
		"interactions": []any{},
		"total_count":  0,
		"status":       "under_development",
		"message":      "Contract interaction analysis will be available when CCE module is implemented",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetDeFiStats DeFi 프로토콜 통계 조회 (개발 중)
func (h *CCEAPIHandler) handleGetDeFiStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// TODO: 실제 DeFi 프로토콜 모니터링 구현
	response := map[string]any{
		"protocols":    []any{},
		"total_tvl":    0,
		"transactions": 0,
		"status":       "under_development",
		"message":      "DeFi protocol monitoring will be available when CCE module is implemented",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
