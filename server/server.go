package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Server represents the HTTP server with mux router
type Server struct {
	mux    *http.ServeMux
	addr   string
	server *http.Server
}

// NewServer creates a new HTTP server instance
func NewServer(addr string) *Server {
	mux := http.NewServeMux()

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &Server{
		mux:    mux,
		addr:   addr,
		server: server,
	}
}

// GetMux returns the HTTP mux for module registration
func (s *Server) GetMux() *http.ServeMux {
	return s.mux
}

// RegisterModule registers a module's API endpoints
func (s *Server) RegisterModule(module ModuleRegistrar) error {
	return module.RegisterRoutes(s.mux)
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

// SetupBasicRoutes sets up basic server routes
func (s *Server) SetupBasicRoutes() {
	// Static files first (specific paths)
	s.mux.HandleFunc("/app.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/app.js")
	})
	s.mux.HandleFunc("/style.css", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/style.css")
	})

	// Health check endpoint
	s.mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
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

	// Root - serve index.html (last, so it doesn't override other routes)
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.ServeFile(w, r, "web/index.html")
			return
		}
		http.NotFound(w, r)
	})

}
