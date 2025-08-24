// internal/ee/api/graph_http.go
package api

import (
	"net/http"
	"strconv"

	ropedbapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
)

type graphProvider interface {
	FetchDefaultGraphJson() (ropedbapp.GraphData, error)
	FetchExpandedGraphByVertex(vertexID string) (ropedbapp.GraphData, error)
	FetchGraphByTraitCode(code ropedomain.TraitCode) (ropedbapp.GraphData, error)
	FetchGraphByRopeID(id ropedomain.RopeID) (ropedbapp.GraphData, error)
	GetGraphStats() map[string]any
}

func (h *EEAPIHandler) withGraph(w http.ResponseWriter) (graphProvider, bool) {
	if h.analyzer == nil || h.analyzer.GraphDB() == nil {
		writeErrorResponse(w, "Graph DB not accessible", http.StatusServiceUnavailable)
		return nil, false
	}
	gp, ok := h.analyzer.RopeDB().(graphProvider)
	if !ok {
		writeErrorResponse(w, "Graph provider not supported by DB implementation", http.StatusServiceUnavailable)
		return nil, false
	}
	return gp, true
}

func (h *EEAPIHandler) handleGraphDefault(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	gp, ok := h.withGraph(w)
	if !ok {
		return
	}
	g, err := gp.FetchDefaultGraphJson()
	if err != nil {
		writeErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSONResponse(w, g)
}

func (h *EEAPIHandler) handleGraphExpand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	v := r.URL.Query().Get("v")
	if v == "" {
		writeErrorResponse(w, "missing query param v (vertex id)", http.StatusBadRequest)
		return
	}
	gp, ok := h.withGraph(w)
	if !ok {
		return
	}
	g, err := gp.FetchExpandedGraphByVertex(v)
	if err != nil {
		writeErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSONResponse(w, g)
}

func (h *EEAPIHandler) handleGraphByTrait(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	codeStr := r.URL.Query().Get("code")
	if codeStr == "" {
		writeErrorResponse(w, "missing query param code", http.StatusBadRequest)
		return
	}
	u, err := strconv.ParseUint(codeStr, 10, 32)
	if err != nil {
		writeErrorResponse(w, "invalid trait code", http.StatusBadRequest)
		return
	}
	gp, ok := h.withGraph(w)
	if !ok {
		return
	}
	g, err := gp.FetchGraphByTraitCode(ropedomain.TraitCode(u))
	if err != nil {
		writeErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSONResponse(w, g)
}

func (h *EEAPIHandler) handleGraphByRope(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		writeErrorResponse(w, "missing query param id", http.StatusBadRequest)
		return
	}
	u, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		writeErrorResponse(w, "invalid rope id", http.StatusBadRequest)
		return
	}
	gp, ok := h.withGraph(w)
	if !ok {
		return
	}
	g, err := gp.FetchGraphByRopeID(ropedomain.RopeID(u))
	if err != nil {
		writeErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSONResponse(w, g)
}
