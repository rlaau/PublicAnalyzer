package api

import (
	"net/http"
	"path/filepath"

	"github.com/go-chi/chi/v5"
	relapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	tripletapi "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/api"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
)

type RelAPIPoolHandler struct {
	relPool *relapp.RelationPool
}

func NewRelPoolAPIHandler(relPool *relapp.RelationPool) *RelAPIPoolHandler {

	return &RelAPIPoolHandler{
		relPool: relPool,
	}
}

func (h *RelAPIPoolHandler) RegisterRoutes(router *chi.Mux) error {
	tripet := h.relPool.GetTripletPort()
	tripetHandler := tripletapi.NewTripletAPIHandler(tripet)
	err := tripetHandler.RegisterRoutes(router)
	if err != nil {
		return err
	}
	return h.registerRelationPool(router)

}

func (h *RelAPIPoolHandler) registerRelationPool(router *chi.Mux) error {

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

	// UI 페이지 (정적 파일 서빙; webDir/ee/graph/*.html 가정)
	router.Route("/ui/triplet/graph", func(r chi.Router) {
		// 부모(Background)
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(computation.FindWebDir(), "triplet", "graph", "index.html"))
		})
		// 자식(Frame/Sigma)
		r.Get("/frame", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(computation.FindWebDir(), "triplet", "graph", "frame.html"))
		})
	})
	return nil
}
