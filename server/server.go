package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type ServerModel interface {
	GetRouter() *chi.Mux
	RegisterModule(module ModuleRegistrar) error
	SetupBasicRoutes()
}
type ServerDesign interface {
	gface.Module
	ServerModel
}

// Server represents the HTTP server with chi router
type Server struct {
	router *chi.Mux
	addr   string
	server *http.Server
	closed atomic.Bool
}

// ! 디자인 강제
var _ ServerDesign = (*Server)(nil)

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

func (s *Server) Name() gface.ModuleName {
	return "Server"
}
func (s *Server) Start(ctx context.Context) error {
	// 1) 리스너를 먼저 열어 바인딩 에러를 즉시 반환
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	log.Printf("HTTP server listening on %s", s.addr)

	// 2) 서버 고루틴
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.server.Serve(ln) // ln 닫히면 반환, ErrServerClosed 가능
	}()

	// 3) 컨트롤 루프: ctx 취소 vs 서버 종료
	select {
	case <-ctx.Done():
		fmt.Println("Server: ctx Done에 의해 종료됨.")
	case err := <-serveErr:
		if s.closed.Swap(true) {
			return err
		}
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			fmt.Println("Server: serveErr와 함께 종료")
		} else {
			fmt.Printf("Server: 서버에서 에러 발생하며 종료함 %s\n", err.Error())
		}
	}
	return s.Close()
}

// Close gracefully shuts down the server
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	err := s.server.Shutdown(shutdownCtx)
	if err != nil {
		log.Printf("⚠️  서버 종료 중 에러: %v\n", err)
	}
	return err
}
func (s *Server) Closed() bool {
	return s.closed.Load()
}

// GetRouter returns the chi router for module registration
func (s *Server) GetRouter() *chi.Mux {
	return s.router
}

// RegisterModule registers a module's API endpoints
func (s *Server) RegisterModule(module ModuleRegistrar) error {
	return module.RegisterRoutes(s.router)
}

// SetupBasicRoutes sets up basic server routes with chi router
// 페이지 라우팅: ui/* (HTML 페이지 서빙)
// API 라우팅: api/* (JSON API 응답)
func (s *Server) SetupBasicRoutes() {
	// Get absolute path to web directory
	rootPath := computation.DockerRoot()
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
