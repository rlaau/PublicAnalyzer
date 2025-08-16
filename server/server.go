package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
)

// Server represents the HTTP server with chi router
type Server struct {
	router *chi.Mux
	addr   string
	server *http.Server
}

// NewServer creates a new HTTP server instance with chi router
func NewServer(addr string) *Server {
	router := chi.NewRouter()

	// 기본 미들웨어 설정
	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Timeout(60 * time.Second))

	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &Server{
		router: router,
		addr:   addr,
		server: server,
	}
}

// GetRouter returns the chi router for module registration
func (s *Server) GetRouter() *chi.Mux {
	return s.router
}

// RegisterModule registers a module's API endpoints
func (s *Server) RegisterModule(module ModuleRegistrar) error {
	return module.RegisterRoutes(s.router)
}

// Start starts the HTTP server
func (s *Server) Start() error {
	log.Printf("Starting server on %s", s.addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	log.Printf("Shutting down server...")
	return s.server.Shutdown(ctx)
}

// SetupBasicRoutes sets up basic server routes with chi router
// 페이지 라우팅: ui/* (HTML 페이지 서빙)
// API 라우팅: api/* (JSON API 응답)
func (s *Server) SetupBasicRoutes() {
	// Get absolute path to web directory
	rootPath := computation.FindProjectRootPath()
	webDir := filepath.Join(rootPath, "web")

	// API 라우팅 그룹 - JSON API 응답
	s.router.Route("/api", func(r chi.Router) {
		// 시스템 헬스 체크 API
		r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			// 프론트엔드가 기대하는 형식으로 응답
			fmt.Fprintf(w, `{
				"status": "healthy", 
				"timestamp": "%s",
				"modules": {
					"server": {
						"healthy": true,
						"status": "running"
					}
				}
			}`, time.Now().Format(time.RFC3339))
		})
	})

	// UI 라우팅 그룹 - HTML 페이지 서빙
	s.router.Route("/ui", func(r chi.Router) {
		// 메인 대시보드 페이지
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
			http.ServeFile(w, r, filepath.Join(webDir, "index.html"))
		})
		r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
			http.ServeFile(w, r, filepath.Join(webDir, "index.html"))
		})
	})

	// 정적 파일 서빙 (CSS, JS) - 캐시 방지 헤더 추가
	s.router.Get("/app.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		http.ServeFile(w, r, filepath.Join(webDir, "app.js"))
	})
	s.router.Get("/style.css", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		http.ServeFile(w, r, filepath.Join(webDir, "style.css"))
	})

	// 루트 리다이렉트 - 메인 대시보드로 이동
	s.router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/dashboard", http.StatusSeeOther)
	})

	// 역호환성을 위한 기존 API 엔드포인트 리다이렉트
	s.router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/health", http.StatusMovedPermanently)
	})

}
