// app/graph_codes.go
package app

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// ===== 형식 =====
type GraphData struct {
	Meta   MetaInfo   `json:"meta"`
	Nodes  []NodeInfo `json:"nodes"`
	Edges  []EdgeInfo `json:"edges"`
	Legend LegendInfo `json:"legend"`
}

type MetaInfo struct {
	GeneratedAt int64  `json:"generatedAt"`
	NodeCount   int    `json:"nodeCount"`
	EdgeCount   int    `json:"edgeCount"`
	GraphType   string `json:"graphType"` // default | expanded | trait | rope | convolution
	StartNode   string `json:"startNode,omitempty"`
}

// 모든 로프를 담는 간단 구조 (대표 로프는 NodeInfo.RopeID/RopeName/Color 그대로 유지)
type RopeBrief struct {
	ID    ropedomain.RopeID `json:"id"`
	Name  string            `json:"name"`
	Color string            `json:"color"`
}

type NodeInfo struct {
	ID       string            `json:"id"`
	Label    string            `json:"label"`
	Color    string            `json:"color"` // 대표 로프 색
	Size     int               `json:"size"`
	RopeID   ropedomain.RopeID `json:"ropeId"`   // 대표 로프
	RopeName string            `json:"ropeName"` // 대표 로프명
	Ropes    []RopeBrief       `json:"ropes,omitempty"`
	Traits   []NodeTrait       `json:"traits"`
}

type NodeTrait struct {
	TraitCode ropedomain.TraitCode `json:"traitCode"`
	TraitName string               `json:"traitName"`
	Partner   string               `json:"partner"`
	TraitID   ropedomain.TraitID   `json:"traitId"`
	RuleCode  ropedomain.RuleCode  `json:"ruleCode"`
}

// EdgeInfo: omitempty 제거 (0도 직렬화되게)
type EdgeInfo struct {
	ID        string               `json:"id"`
	Source    string               `json:"source"`
	Target    string               `json:"target"`
	Color     string               `json:"color"`
	Weight    int                  `json:"weight"`
	TraitCode ropedomain.TraitCode `json:"traitCode"`
	TraitName string               `json:"traitName"`
	TraitID   ropedomain.TraitID   `json:"traitId"`
	LastSeen  int64                `json:"lastSeen"`

	Pair          string `json:"pair"`          // "lo|hi"
	ParallelIndex int    `json:"parallelIndex"` // 0..(ParallelCount-1)
	ParallelCount int    `json:"parallelCount"` // 동일 Pair 내 총 개수
}

type LegendInfo struct {
	Traits map[string]LegendItem `json:"traits"`
	Ropes  map[string]LegendItem `json:"ropes"`
	Rules  map[string]LegendItem `json:"rules"`
}

type LegendItem struct {
	Color string `json:"color"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func (g GraphData) Marshal() ([]byte, error)       { return json.Marshal(g) }
func (g GraphData) MarshalPretty() ([]byte, error) { return json.MarshalIndent(g, "", "  ") }

// ===== 기능(JSON) =====

// 5) 기본 그래프: 가장 큰 로프 중심
func (b *BadgerRopeDB) FetchDefaultGraphJson() (GraphData, error) {
	ropeID, rm := b.topRopeByVolume()
	if ropeID == 0 || len(rm.Members) == 0 {
		return GraphData{Meta: MetaInfo{GeneratedAt: time.Now().Unix(), GraphType: "default"}}, fmt.Errorf("no ropes found")
	}
	start := rm.Members[0]
	return b.buildGraphFromVertex(start, 3, 0, "default")
}

// 7) 1hop 확장 (클릭 이벤트용)
func (b *BadgerRopeDB) FetchExpandedGraphByVertex(vertexID string) (GraphData, error) {
	addr, err := shareddomain.ParseAddressFromString(vertexID)
	if err != nil {
		return GraphData{}, fmt.Errorf("invalid vertex id: %w", err)
	}
	return b.buildGraphFromVertex(addr, 1, 0, "expanded")
}

// 6) 합성(병합)
func ConvolutionGraph(orig, inc GraphData) GraphData {
	out := orig
	// nodes
	nm := map[string]NodeInfo{}
	for _, n := range orig.Nodes {
		nm[n.ID] = n
	}
	for _, n := range inc.Nodes {
		nm[n.ID] = n
	}
	out.Nodes = out.Nodes[:0]
	for _, n := range nm {
		out.Nodes = append(out.Nodes, n)
	}
	// edges
	em := map[string]EdgeInfo{}
	for _, e := range orig.Edges {
		em[e.ID] = e
	}
	for _, e := range inc.Edges {
		em[e.ID] = e
	}
	out.Edges = out.Edges[:0]
	for _, e := range em {
		out.Edges = append(out.Edges, e)
	}
	out.Edges = annotateParallel(out.Edges) // ★ 여기!
	// legend
	if out.Legend.Traits == nil {
		out.Legend.Traits = map[string]LegendItem{}
	}
	if out.Legend.Ropes == nil {
		out.Legend.Ropes = map[string]LegendItem{}
	}
	for k, v := range inc.Legend.Traits {
		out.Legend.Traits[k] = v
	}
	for k, v := range inc.Legend.Ropes {
		out.Legend.Ropes[k] = v
	}

	out.Meta.GeneratedAt = time.Now().Unix()
	out.Meta.GraphType = "convolution"
	out.Meta.NodeCount = len(out.Nodes)
	out.Meta.EdgeCount = len(out.Edges)
	return out
}

// 8) 트레이트 전체 로드
func (b *BadgerRopeDB) FetchGraphByTraitCode(code ropedomain.TraitCode) (GraphData, error) {
	marks, err := b.ViewAllTraitMarkByCode(code)
	if err != nil {
		return GraphData{}, err
	}
	if len(marks) == 0 {
		return GraphData{Meta: MetaInfo{GeneratedAt: time.Now().Unix(), GraphType: "trait"}}, fmt.Errorf("no marks for trait %d", code)
	}
	start := marks[0].AddressA
	return b.buildGraphFromVertex(start, 3, code, "trait")
}

// 9) 로프ID 기반 로드 (간단 버전)
func (b *BadgerRopeDB) FetchGraphByRopeID(id ropedomain.RopeID) (GraphData, error) {
	// 1. RopeMark로 모든 멤버 가져오기
	ropeMark := b.getRopeMark(id)
	if ropeMark.ID == 0 {
		return GraphData{}, fmt.Errorf("rope not found: %d", id)
	}
	if len(ropeMark.Members) == 0 {
		return GraphData{Meta: MetaInfo{GeneratedAt: time.Now().Unix(), GraphType: "rope"}}, fmt.Errorf("rope %d has no members", id)
	}

	nodeMap := map[string]NodeInfo{}
	edgeMap := map[string]EdgeInfo{}
	traitIDSet := map[ropedomain.TraitID]bool{}

	// 2. 각 멤버 버텍스 조회 및 노드 생성
	for _, memberAddr := range ropeMark.Members {
		vertex := b.getOrCreateVertex(memberAddr)
		nodeInfo := b.createNodeInfo(vertex)
		nodeMap[nodeInfo.ID] = nodeInfo

		// 3. 각 멤버의 트레이트 ID 수집
		for _, trait := range vertex.Traits {
			traitIDSet[trait.TraitID] = true
		}
	}

	// 4. 수집된 트레이트 ID 기반으로 트레이트 조회 및 엣지 생성
	for traitID := range traitIDSet {
		traitMark := b.getTraitMark(traitID)
		if traitMark.ID == 0 {
			continue
		}

		// 양 끝 노드가 모두 Rope 멤버에 포함된 경우만 엣지 추가
		addrA := traitMark.AddressA.String()
		addrB := traitMark.AddressB.String()

		if _, hasA := nodeMap[addrA]; !hasA {
			continue
		}
		if _, hasB := nodeMap[addrB]; !hasB {
			continue
		}

		// 엣지 생성
		edgeID := fmt.Sprintf("%s_%s_%d", addrA, addrB, traitMark.Trait)
		if _, exists := edgeMap[edgeID]; !exists {
			edgeInfo := EdgeInfo{
				ID:        edgeID,
				Source:    addrA,
				Target:    addrB,
				TraitCode: traitMark.Trait,
				TraitName: b.traitName(traitMark.Trait),
				TraitID:   traitID,
				Color:     b.generateTraitColor(traitMark.Trait),
				Weight:    int(traitMark.Volume),
				LastSeen:  traitMark.LastSeen.Unix(),
			}
			edgeMap[edgeID] = edgeInfo
		}
	}

	// 병렬 엣지 처리
	edges := make([]EdgeInfo, 0, len(edgeMap))
	for _, edge := range edgeMap {
		edges = append(edges, edge)
	}
	edges = annotateParallel(edges)

	// 노드 배열 생성
	nodes := make([]NodeInfo, 0, len(nodeMap))
	for _, node := range nodeMap {
		nodes = append(nodes, node)
	}

	// 범례 생성
	legend := b.generateLegend(nodes, edges)

	return GraphData{
		Nodes:  nodes,
		Edges:  edges,
		Legend: legend,
		Meta: MetaInfo{
			GeneratedAt: time.Now().Unix(),
			NodeCount:   len(nodes),
			EdgeCount:   len(edges),
			GraphType:   "rope",
			StartNode:   "", // 시작 노드 개념 없음
		},
	}, nil
}

// ===== 내부 구현 =====

func (b *BadgerRopeDB) buildGraphFromVertex(start shareddomain.Address, depth int, filter ropedomain.TraitCode, gtype string) (GraphData, error) {
	nodeMap := map[string]NodeInfo{}
	edgeMap := map[string]EdgeInfo{}
	seen := map[string]bool{}

	q := []shareddomain.Address{start}
	seen[start.String()] = true

	for d := 0; d < depth && len(q) > 0; d++ {
		nxt := []shareddomain.Address{}
		for _, a := range q {
			v := b.getOrCreateVertex(a)
			ninfo := b.createNodeInfo(v)
			nodeMap[ninfo.ID] = ninfo

			for _, tr := range v.Traits {
				if filter != 0 && tr.Trait != filter {
					continue
				}
				p := tr.Partner
				ps := p.String()

				// ensure partner node
				if _, ok := nodeMap[ps]; !ok {
					pv := b.getOrCreateVertex(p)
					nodeMap[ps] = b.createNodeInfo(pv)
				}
				if !seen[ps] && len(nodeMap) < 500 {
					seen[ps] = true
					nxt = append(nxt, p)
				}

				// undirected canonical key
				src, dst := a.String(), ps
				if dst < src {
					src, dst = dst, src
				}
				eid := fmt.Sprintf("e:%s|%s|%d|%d", src, dst, tr.Trait, tr.TraitID)
				if _, ok := edgeMap[eid]; !ok {
					e := b.createEdgeInfo(src, dst, tr, eid)
					// pair 키 저장(평행 간선 그룹화용)
					e.Pair = fmt.Sprintf("%s|%s", src, dst)
					edgeMap[eid] = e
				}
			}
		}
		q = nxt
	}

	// materialize
	nodes := make([]NodeInfo, 0, len(nodeMap))
	for _, n := range nodeMap {
		nodes = append(nodes, n)
	}
	edges := make([]EdgeInfo, 0, len(edgeMap))
	for _, e := range edgeMap {
		edges = append(edges, e)
	}
	edges = annotateParallel(edges) // ★ 여기!

	// === 평행 간선 인덱싱 ===
	groups := map[string][]int{} // pair -> indices
	for i := range edges {
		p := edges[i].Pair
		if p == "" {
			lo, hi := edges[i].Source, edges[i].Target
			if hi < lo {
				lo, hi = hi, lo
			}
			p = lo + "|" + hi
			edges[i].Pair = p
		}
		groups[p] = append(groups[p], i)
	}
	for _, idxs := range groups {
		n := len(idxs)
		for k, i := range idxs {
			edges[i].ParallelIndex = k
			edges[i].ParallelCount = n
		}
	}

	legend := b.generateLegend(nodes, edges)

	return GraphData{
		Meta: MetaInfo{
			GeneratedAt: time.Now().Unix(),
			NodeCount:   len(nodes),
			EdgeCount:   len(edges),
			GraphType:   gtype,
			StartNode:   start.String(),
		},
		Nodes:  nodes,
		Edges:  edges,
		Legend: legend,
	}, nil
}

func (b *BadgerRopeDB) createNodeInfo(v *ropedomain.Vertex) NodeInfo {
	var ropeID ropedomain.RopeID
	var ropeColor, ropeName string

	// 모든 로프 수집
	var allRopes []RopeBrief
	for _, r := range v.Ropes {
		allRopes = append(allRopes, RopeBrief{
			ID:    r.ID,
			Name:  fmt.Sprintf("Rope %d", r.ID),
			Color: b.generateRopeColor(r.ID),
		})
	}
	// 대표 로프(첫 번째)로 기본 색/이름 설정
	if len(allRopes) > 0 {
		ropeID = allRopes[0].ID
		ropeName = allRopes[0].Name
		ropeColor = allRopes[0].Color
	} else {
		ropeColor = "#888888"
		ropeName = "No Rope"
	}

	traits := make([]NodeTrait, len(v.Traits))
	for i, tr := range v.Traits {
		// TraitMark를 조회해서 실제 RuleCode 가져오기
		tm := b.getTraitMark(tr.TraitID)
		
		// 현재 노드 주소에 해당하는 Rule 결정
		var ruleCode ropedomain.RuleCode
		if tm.AddressA == v.Address {
			ruleCode = tm.RuleA
		} else if tm.AddressB == v.Address {
			ruleCode = tm.RuleB
		} else {
			ruleCode = 0 // 기본값
		}
		
		traits[i] = NodeTrait{
			TraitCode: tr.Trait,
			TraitName: b.traitName(tr.Trait),
			Partner:   tr.Partner.String(),
			TraitID:   tr.TraitID,
			RuleCode:  ruleCode,
		}
	}
	// 주소 축약
	addr := v.Address.String()
	shortAddr := addr
	if len(addr) > 13 {
		shortAddr = addr[:6] + ".." + addr[len(addr)-4:]
	}
	
	// 라벨에 더 많은 정보 포함
	ropeCount := len(allRopes)
	traitCount := len(v.Traits)
	
	var label string
	if ropeCount > 0 && traitCount > 0 {
		label = fmt.Sprintf("%s\nR:%d T:%d", shortAddr, ropeCount, traitCount)
	} else if ropeCount > 0 {
		label = fmt.Sprintf("%s\nR:%d", shortAddr, ropeCount)
	} else if traitCount > 0 {
		label = fmt.Sprintf("%s\nT:%d", shortAddr, traitCount)
	} else {
		label = shortAddr
	}

	return NodeInfo{
		ID:       v.Address.String(),
		Label:    label,
		Color:    ropeColor,
		Size:     5 + len(v.Traits),
		RopeID:   ropeID,
		RopeName: ropeName,
		Ropes:    allRopes,
		Traits:   traits,
	}
}

// GetRopeInfo returns detailed information about a specific rope
func (b *BadgerRopeDB) GetRopeInfo(id ropedomain.RopeID) (map[string]interface{}, error) {
	ropeMark := b.getRopeMark(id)
	if ropeMark.ID == 0 {
		return nil, fmt.Errorf("rope not found: %d", id)
	}
	
	// 멤버 주소를 문자열로 변환
	memberStrs := make([]string, len(ropeMark.Members))
	for i, addr := range ropeMark.Members {
		memberStrs[i] = addr.String()
	}
	
	info := map[string]interface{}{
		"id":       int64(ropeMark.ID),
		"trait":    int64(ropeMark.Trait),
		"size":     int64(ropeMark.Size),
		"volume":   int64(ropeMark.Volume),
		"lastSeen": ropeMark.LastSeen.Unix(),
		"members":  memberStrs,
		"traitName": b.traitName(ropeMark.Trait),
	}
	
	return info, nil
}

func (b *BadgerRopeDB) createEdgeInfo(src, dst string, tr ropedomain.TraitRef, id string) EdgeInfo {
	tm := b.getTraitMark(tr.TraitID)
	return EdgeInfo{
		ID:        id,
		Source:    src,
		Target:    dst,
		Color:     b.generateTraitColor(tr.Trait),
		Weight:    1,
		TraitCode: tr.Trait,
		TraitName: b.traitName(tr.Trait),
		TraitID:   tr.TraitID,
		LastSeen:  tm.LastSeen.Unix(),
	}
}

func (b *BadgerRopeDB) traitName(code ropedomain.TraitCode) string {
	if b != nil && b.traitLegend != nil {
		if n, ok := b.traitLegend[code]; ok {
			return n
		}
	}
	return fmt.Sprintf("Trait %d", code)
}

func (b *BadgerRopeDB) generateTraitColor(code ropedomain.TraitCode) string {
	// RopeColor와 동일한 방식으로 명확한 색상 구분
	h := (int(code)*89 + 180) % 360 // RopeColor와 유사하지만 다른 오프셋
	s := 65 + (int(code)%4)*8       // 65-89% 범위 (4단계)
	l := 40 + (int(code)%5)*5       // 40-60% 범위 (5단계)
	return fmt.Sprintf("hsl(%d, %d%%, %d%%)", h, s, l)
}
func (b *BadgerRopeDB) generateRopeColor(id ropedomain.RopeID) string {
	h := (int(id)*89 + 45) % 360
	s := 60 + (int(id)%3)*10
	l := 35 + (int(id)%4)*5
	return fmt.Sprintf("hsl(%d, %d%%, %d%%)", h, s, l)
}

func (b *BadgerRopeDB) generateRuleColor(ruleCode ropedomain.RuleCode) string {
	// Rule별 고유 색상 생성 (Trait, Rope와 구별되는 색상 범위)
	h := (int(ruleCode)*73 + 270) % 360 // 다른 오프셋으로 색상 충돌 방지
	s := 70 + (int(ruleCode)%3)*10      // 70-90% 범위
	l := 45 + (int(ruleCode)%4)*5       // 45-60% 범위
	return fmt.Sprintf("hsl(%d, %d%%, %d%%)", h, s, l)
}

func (b *BadgerRopeDB) ruleName(ruleCode ropedomain.RuleCode) string {
	if b != nil && b.ruleLegend != nil {
		if n, ok := b.ruleLegend[ruleCode]; ok {
			return n
		}
	}
	return fmt.Sprintf("Rule %d", ruleCode)
}

func (b *BadgerRopeDB) generateLegend(nodes []NodeInfo, edges []EdgeInfo) LegendInfo {
	traits := map[string]LegendItem{}
	ropes := map[string]LegendItem{}
	rules := map[string]LegendItem{}

	tcnt := map[ropedomain.TraitCode]int{}
	rcnt := map[ropedomain.RopeID]int{}
	ruleCnt := map[ropedomain.RuleCode]int{}

	for _, e := range edges {
		tcnt[e.TraitCode]++
	}
	// ▲ 모든 로프 멤버십을 반영
	for _, n := range nodes {
		for _, rb := range n.Ropes {
			rcnt[rb.ID]++
		}
		// RuleCode 추출
		for _, trait := range n.Traits {
			if trait.RuleCode != 0 {
				ruleCnt[trait.RuleCode]++
			}
		}
	}

	for code, c := range tcnt {
		traits[fmt.Sprintf("%d", code)] = LegendItem{
			Color: b.generateTraitColor(code),
			Name:  b.traitName(code),
			Count: c,
		}
	}
	for id, c := range rcnt {
		ropes[fmt.Sprintf("%d", id)] = LegendItem{
			Color: b.generateRopeColor(id),
			Name:  fmt.Sprintf("Rope %d", id),
			Count: c,
		}
	}
	for ruleCode, c := range ruleCnt {
		rules[fmt.Sprintf("%d", ruleCode)] = LegendItem{
			Color: b.generateRuleColor(ruleCode),
			Name:  b.ruleName(ruleCode),
			Count: c,
		}
	}
	return LegendInfo{Traits: traits, Ropes: ropes, Rules: rules}
}

// 내부: RopeMark 중 최대 Volume 선택(동률이면 Size, 그 다음 LastSeen)
func (b *BadgerRopeDB) topRopeByVolume() (ropedomain.RopeID, ropedomain.RopeMark) {
	var best ropedomain.RopeMark
	_ = b.db.View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		it := txn.NewIterator(itOpts)
		defer it.Close()
		pfx := []byte(kR)
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			item := it.Item()
			var rm ropedomain.RopeMark
			_ = item.Value(func(val []byte) error { return json.Unmarshal(val, &rm) })
			if rm.ID == 0 {
				continue
			}
			if betterRope(rm, best) {
				best = rm
			}
		}
		return nil
	})
	return best.ID, best
}

func betterRope(a, b ropedomain.RopeMark) bool {
	if b.ID == 0 {
		return true
	}
	if a.Volume != b.Volume {
		return a.Volume > b.Volume
	}
	if a.Size != b.Size {
		return a.Size > b.Size
	}
	return a.LastSeen.After(b.LastSeen)
}

// (사소 유틸)
func (b *BadgerRopeDB) getTraitMark(id ropedomain.TraitID) ropedomain.TraitMark {
	key := []byte(fmt.Sprintf("%s%d", kT, uint64(id)))
	var tm ropedomain.TraitMark
	_ = b.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			return nil
		}
		return itm.Value(func(val []byte) error { return json.Unmarshal(val, &tm) })
	})
	return tm
}
