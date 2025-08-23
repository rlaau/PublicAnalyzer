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

type NodeInfo struct {
	ID       string            `json:"id"`
	Label    string            `json:"label"`
	Color    string            `json:"color"`
	Size     int               `json:"size"`
	RopeID   ropedomain.RopeID `json:"ropeId"`
	RopeName string            `json:"ropeName"`
	Traits   []NodeTrait       `json:"traits"`
}

type NodeTrait struct {
	TraitCode ropedomain.TraitCode `json:"traitCode"`
	TraitName string               `json:"traitName"`
	Partner   string               `json:"partner"`
	TraitID   ropedomain.TraitID   `json:"traitId"`
}

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
}

type LegendInfo struct {
	Traits map[string]LegendItem `json:"traits"`
	Ropes  map[string]LegendItem `json:"ropes"`
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
	r, err := b.ViewRope(id)
	if err != nil {
		return GraphData{}, err
	}
	if r == nil || len(r.Nodes) == 0 {
		return GraphData{Meta: MetaInfo{GeneratedAt: time.Now().Unix(), GraphType: "rope"}}, fmt.Errorf("rope not found or empty: %d", id)
	}
	start := r.Nodes[0].Address
	return b.buildGraphFromVertex(start, 2, r.Trait, "rope")
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
					edgeMap[eid] = b.createEdgeInfo(src, dst, tr, eid)
				}
			}
		}
		q = nxt
	}

	nodes := make([]NodeInfo, 0, len(nodeMap))
	for _, n := range nodeMap {
		nodes = append(nodes, n)
	}
	edges := make([]EdgeInfo, 0, len(edgeMap))
	for _, e := range edgeMap {
		edges = append(edges, e)
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
	if len(v.Ropes) > 0 {
		ropeID = v.Ropes[0].ID
		ropeColor = b.generateRopeColor(ropeID)
		ropeName = fmt.Sprintf("Rope %d", ropeID)
	} else {
		ropeColor = "#888888"
		ropeName = "No Rope"
	}

	traits := make([]NodeTrait, len(v.Traits))
	for i, tr := range v.Traits {
		traits[i] = NodeTrait{
			TraitCode: tr.Trait,
			TraitName: b.traitName(tr.Trait),
			Partner:   tr.Partner.String(),
			TraitID:   tr.TraitID,
		}
	}
	label := v.Address.String()
	if len(label) > 13 {
		label = label[:10] + "..."
	}

	return NodeInfo{
		ID:       v.Address.String(),
		Label:    label,
		Color:    ropeColor,
		Size:     5 + len(v.Traits),
		RopeID:   ropeID,
		RopeName: ropeName,
		Traits:   traits,
	}
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
	h := (int(code)*89 + 180) % 360  // RopeColor와 유사하지만 다른 오프셋
	s := 65 + (int(code)%4)*8        // 65-89% 범위 (4단계)
	l := 40 + (int(code)%5)*5        // 40-60% 범위 (5단계)
	return fmt.Sprintf("hsl(%d, %d%%, %d%%)", h, s, l)
}
func (b *BadgerRopeDB) generateRopeColor(id ropedomain.RopeID) string {
	h := (int(id)*89 + 45) % 360
	s := 60 + (int(id)%3)*10
	l := 35 + (int(id)%4)*5
	return fmt.Sprintf("hsl(%d, %d%%, %d%%)", h, s, l)
}

func (b *BadgerRopeDB) generateLegend(nodes []NodeInfo, edges []EdgeInfo) LegendInfo {
	traits := map[string]LegendItem{}
	ropes := map[string]LegendItem{}

	tcnt := map[ropedomain.TraitCode]int{}
	rcnt := map[ropedomain.RopeID]int{}
	for _, e := range edges {
		tcnt[e.TraitCode]++
	}
	for _, n := range nodes {
		if n.RopeID != 0 {
			rcnt[n.RopeID]++
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
	return LegendInfo{Traits: traits, Ropes: ropes}
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
