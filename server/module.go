package server

import "net/http"

// ModuleRegistrar defines the interface for registering module API endpoints
type ModuleRegistrar interface {
	RegisterRoutes(mux *http.ServeMux) error
}