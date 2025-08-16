package server

import "github.com/go-chi/chi/v5"

// ModuleRegistrar defines the interface for registering module API endpoints
// 모듈들은 chi 라우터에 라우트를 등록합니다
// 페이지 라우팅: ui/* (HTML 페이지 서빙)
// API 라우팅: api/* (JSON API 응답)
type ModuleRegistrar interface {
	RegisterRoutes(router *chi.Mux) error
}