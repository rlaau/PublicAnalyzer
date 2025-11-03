package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/protocol"
	"github.com/rlaaudgjs5638/chainAnalyzer/server/utils"
	ropedbapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type graphProvider interface {
	FetchDefaultGraphJson() (ropedbapp.GraphData, error)
	FetchExpandedGraphByVertex(vertexID string) (ropedbapp.GraphData, error)
	FetchGraphByTraitCode(code ropedomain.TraitCode) (ropedbapp.GraphData, error)
	FetchGraphByRopeID(id ropedomain.RopeID) (ropedbapp.GraphData, error)
	GetGraphStats() map[string]any
}

func (h *RelAPIPoolHandler) withGraph(w http.ResponseWriter) (graphProvider, bool) {
	if h.relPool == nil || h.relPool.ClusterRepo == nil {
		utils.WriteErrorResponse(w, "Graph DB not accessible", http.StatusServiceUnavailable)
		return nil, false
	}
	gp, ok := h.relPool.ClusterRepo.(graphProvider)
	if !ok {
		utils.WriteErrorResponse(w, "Graph provider not supported by DB implementation", http.StatusServiceUnavailable)
		return nil, false
	}
	return gp, true
}

func (h *RelAPIPoolHandler) handleGraphDefault(w http.ResponseWriter, r *http.Request) {
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
		utils.WriteErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	utils.WriteJSONResponse(w, g)
}

func (h *RelAPIPoolHandler) handleGraphExpand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	v := r.URL.Query().Get("v")
	if v == "" {
		utils.WriteErrorResponse(w, "missing query param v (vertex id)", http.StatusBadRequest)
		return
	}
	gp, ok := h.withGraph(w)
	if !ok {
		return
	}
	g, err := gp.FetchExpandedGraphByVertex(v)
	if err != nil {
		utils.WriteErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	utils.WriteJSONResponse(w, g)
}

func (h *RelAPIPoolHandler) handleGraphByTrait(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	codeStr := r.URL.Query().Get("code")
	if codeStr == "" {
		utils.WriteErrorResponse(w, "missing query param code", http.StatusBadRequest)
		return
	}
	u, err := strconv.ParseUint(codeStr, 10, 32)
	if err != nil {
		utils.WriteErrorResponse(w, "invalid trait code", http.StatusBadRequest)
		return
	}
	gp, ok := h.withGraph(w)
	if !ok {
		return
	}
	g, err := gp.FetchGraphByTraitCode(ropedomain.TraitCode(u))
	if err != nil {
		utils.WriteErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	utils.WriteJSONResponse(w, g)
}

func (h *RelAPIPoolHandler) handleGraphByRope(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Try path parameter first (for /rope/{id}), then fall back to query parameter (for /rope?id=)
	idStr := chi.URLParam(r, "id")
	if idStr == "" {
		idStr = r.URL.Query().Get("id")
	}

	if idStr == "" {
		utils.WriteErrorResponse(w, "missing rope id (provide as path parameter or query param id)", http.StatusBadRequest)
		return
	}

	u, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		utils.WriteErrorResponse(w, "invalid rope id", http.StatusBadRequest)
		return
	}

	gp, ok := h.withGraph(w)
	if !ok {
		return
	}

	g, err := gp.FetchGraphByRopeID(ropedomain.RopeID(u))
	if err != nil {
		utils.WriteErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	utils.WriteJSONResponse(w, g)
}

func (h *RelAPIPoolHandler) handleRopeInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Try path parameter first (for /rope-info/{id}), then fall back to query parameter (for /rope-info?id=)
	idStr := chi.URLParam(r, "id")
	if idStr == "" {
		idStr = r.URL.Query().Get("id")
	}

	if idStr == "" {
		utils.WriteErrorResponse(w, "missing rope id (provide as path parameter or query param id)", http.StatusBadRequest)
		return
	}

	u, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		utils.WriteErrorResponse(w, "invalid rope id", http.StatusBadRequest)
		return
	}

	// RopeDB 접근 확인
	if h.relPool == nil || h.relPool.ClusterRepo == nil {
		utils.WriteErrorResponse(w, "Graph DB not accessible", http.StatusServiceUnavailable)
		return
	}

	// RopeDB에서 직접 RopeMark 정보를 가져오는 새로운 메소드가 필요
	if ropeDB, ok := h.relPool.ClusterRepo.(interface {
		GetRopeInfo(id ropedomain.RopeID) (map[string]interface{}, error)
	}); ok {
		info, err := ropeDB.GetRopeInfo(ropedomain.RopeID(u))
		if err != nil {
			utils.WriteErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		utils.WriteJSONResponse(w, info)
	} else {
		utils.WriteErrorResponse(w, "Rope info not supported by DB implementation", http.StatusServiceUnavailable)
	}
}

func (h *RelAPIPoolHandler) handlePolyTraitLegend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// RopeDB 접근 확인
	if h.relPool == nil || h.relPool.ClusterRepo == nil {
		utils.WriteErrorResponse(w, "Graph DB not accessible", http.StatusServiceUnavailable)
		return
	}

	// PolyTrait 범례 정보를 제공하는 메소드가 필요
	if ropeDB, ok := h.relPool.ClusterRepo.(interface {
		GetPolyTraitLegend() map[ropedomain.PolyTraitCode]ropedomain.PolyNameAndTraits
	}); ok {
		legend := ropeDB.GetPolyTraitLegend()
		utils.WriteJSONResponse(w, legend)
	} else {
		utils.WriteErrorResponse(w, "PolyTrait legend not supported by DB implementation", http.StatusServiceUnavailable)
	}
}

func (h *RelAPIPoolHandler) handlePolyRopeSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 필수 파라미터 확인
	address1Str := r.URL.Query().Get("address1")
	address2Str := r.URL.Query().Get("address2")
	polyTraitCodeStr := r.URL.Query().Get("polytraitcode")

	if address1Str == "" || address2Str == "" || polyTraitCodeStr == "" {
		utils.WriteErrorResponse(w, "missing required parameters: address1, address2, polytraitcode", http.StatusBadRequest)
		return
	}

	// PolyTraitCode 파싱
	polyTraitCodeInt, err := strconv.ParseUint(polyTraitCodeStr, 10, 64)
	if err != nil {
		utils.WriteErrorResponse(w, "invalid polytrait code", http.StatusBadRequest)
		return
	}
	polyTraitCode := ropedomain.PolyTraitCode(polyTraitCodeInt)

	// Address 파싱 (hex string을 Address로 변환)
	address1, err := shareddomain.ParseAddressFromString(address1Str)
	if err != nil {
		utils.WriteErrorResponse(w, "invalid address1 format: "+err.Error(), http.StatusBadRequest)
		return
	}
	address2, err := shareddomain.ParseAddressFromString(address2Str)
	if err != nil {
		utils.WriteErrorResponse(w, "invalid address2 format: "+err.Error(), http.StatusBadRequest)
		return
	}

	// RopeDB 접근 확인
	if h.relPool == nil || h.relPool.ClusterRepo == nil {
		utils.WriteErrorResponse(w, "Graph DB not accessible", http.StatusServiceUnavailable)
		return
	}

	// ViewInSameRopeByPolyTrait 메소드 호출
	if ropeDB, ok := h.relPool.ClusterRepo.(interface {
		ViewInSameRopeByPolyTrait(addr1, addr2 shareddomain.Address, polyTraitCode ropedomain.PolyTraitCode) (bool, error)
	}); ok {
		inSameRope, err := ropeDB.ViewInSameRopeByPolyTrait(address1, address2, polyTraitCode)
		if err != nil {
			utils.WriteErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		response := map[string]interface{}{
			"address1":      address1Str,
			"address2":      address2Str,
			"polytraitcode": polyTraitCode,
			"in_same_rope":  inSameRope,
		}
		utils.WriteJSONResponse(w, response)
	} else {
		utils.WriteErrorResponse(w, "PolyRope search not supported by DB implementation", http.StatusServiceUnavailable)
	}
}

// handleAuthorLegend Author Legend 조회
func (h *RelAPIPoolHandler) handleAuthorLegend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// AuthorLegend를 직접 반환
	legend := protocol.AuthorLegend
	utils.WriteJSONResponse(w, legend)
}

// handleTraitDetail Trait 상세 정보 조회
func (h *RelAPIPoolHandler) handleTraitDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Try path parameter first
	idStr := chi.URLParam(r, "id")
	if idStr == "" {
		idStr = r.URL.Query().Get("id")
	}

	if idStr == "" {
		utils.WriteErrorResponse(w, "missing trait id", http.StatusBadRequest)
		return
	}

	u, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		utils.WriteErrorResponse(w, "invalid trait id", http.StatusBadRequest)
		return
	}

	// RopeDB 접근 확인
	if h.relPool == nil || h.relPool.ClusterRepo == nil {
		utils.WriteErrorResponse(w, "Graph DB not accessible", http.StatusServiceUnavailable)
		return
	}

	// TraitMark 정보를 가져오는 메소드가 필요
	if ropeDB, ok := h.relPool.ClusterRepo.(interface {
		GetTraitDetail(id ropedomain.TraitID) (map[string]interface{}, error)
	}); ok {
		detail, err := ropeDB.GetTraitDetail(ropedomain.TraitID(u))
		if err != nil {
			utils.WriteErrorResponse(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		utils.WriteJSONResponse(w, detail)
	} else {
		utils.WriteErrorResponse(w, "Trait detail not supported by DB implementation", http.StatusServiceUnavailable)
	}
}

// handleGraphStats 그래프 DB 통계 조회
func (h *RelAPIPoolHandler) handleGraphStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// RopeDB에서 실제 그래프 통계 조회
	stats := h.relPool.GetGraphStats()

	// 추가 메타데이터
	response := map[string]interface{}{
		"database_available": true,
		"message":            "Graph statistics retrieved successfully",
		"stats":              stats,
		// 호환성을 위해 stats의 내용을 최상위 레벨에도 복사
		"nodes":     stats["nodes"],
		"ropes":     stats["ropes"],
		"traits":    stats["traits"],
		"polyRopes": stats["polyRopes"],
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
