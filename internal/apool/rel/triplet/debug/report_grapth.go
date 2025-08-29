package main

// ---------- Graph Report (Same-Handle, Zero-Deps SVG) ----------
import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// infra와 동일 JSON 형태 최소 정의
type reportConnectionData struct {
	ConnectedAddr string `json:"connectedAddr"`
	DepositAddr   string `json:"depositAddr"`
	FirstDetectTx string `json:"firstDetectTx"`
	TxCount       int64  `json:"txCount"`
	TotalVolume   string `json:"totalVolume"`
	FirstSeen     int64  `json:"firstSeen"`
	LastConfirmed int64  `json:"lastConfirmed"`
}
type reportAdjacencyData struct {
	Address     string                 `json:"address"`
	Connections []reportConnectionData `json:"connections"`
}

const reportAdjPrefix = "adj:" // infra와 동일

type component struct {
	ID    int
	Nodes []string
	Edges [][2]string
	Size  int
}

// 동일 핸들 버전: db는 Analyzer.GraphDB()로 받은 핸들
func generateGraphReportWithDB(cfg *IsolatedPathConfig, db *badger.DB) error {
	if db == nil {
		return fmt.Errorf("graph DB handle is nil")
	}
	reportDir := filepath.Join(cfg.RootOfIsolatedDir, "report")
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		return fmt.Errorf("mkdir report: %w", err)
	}

	// 1) 전체 그래프 수집 (무방향, 중복 제거)
	nodes := make(map[string]struct{})
	adj := make(map[string]map[string]struct{})

	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(reportAdjPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			var ad reportAdjacencyData
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &ad)
			}); err != nil {
				return nil // skip
			}
			if ad.Address == "" {
				continue
			}
			nodes[ad.Address] = struct{}{}
			if _, ok := adj[ad.Address]; !ok {
				adj[ad.Address] = make(map[string]struct{})
			}
			for _, c := range ad.Connections {
				nodes[c.ConnectedAddr] = struct{}{}
				adj[ad.Address][c.ConnectedAddr] = struct{}{}
				if _, ok := adj[c.ConnectedAddr]; !ok {
					adj[c.ConnectedAddr] = make(map[string]struct{})
				}
				adj[c.ConnectedAddr][ad.Address] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan adj: %w", err)
	}

	if len(nodes) == 0 {
		return writeEmptyReport(reportDir)
	}

	// 2) 연결요소(BFS)
	visited := make(map[string]bool, len(nodes))
	comps := make([]component, 0, 1024)
	compID := 0

	for addr := range nodes {
		if visited[addr] {
			continue
		}
		queue := []string{addr}
		visited[addr] = true

		var c component
		c.ID = compID
		nodeIndex := make(map[string]int)

		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]

			nodeIndex[cur] = len(c.Nodes)
			c.Nodes = append(c.Nodes, cur)

			for neigh := range adj[cur] {
				if !visited[neigh] {
					visited[neigh] = true
					queue = append(queue, neigh)
				}
			}
		}
		for a := range nodeIndex {
			for b := range adj[a] {
				if _, ok := nodeIndex[b]; ok && a < b {
					c.Edges = append(c.Edges, [2]string{a, b})
				}
			}
		}
		c.Size = len(c.Nodes)
		comps = append(comps, c)
		compID++
	}

	// 3) 통계
	totalNodes := 0
	cntSizeEq2 := 0
	cntSizeGe2 := 0
	for _, c := range comps {
		totalNodes += c.Size
		if c.Size == 2 {
			cntSizeEq2++
		}
		if c.Size >= 2 {
			cntSizeGe2++
		}
	}
	avgSize := float64(totalNodes) / float64(len(comps))
	sort.Slice(comps, func(i, j int) bool { return comps[i].Size > comps[j].Size })
	top100 := comps
	if len(top100) > 100 {
		top100 = top100[:100]
	}

	// 4) 산출
	summary := map[string]any{
		"components_total":      len(comps),
		"nodes_total":           totalNodes,
		"avg_component_size":    avgSize,
		"count_size_eq_2":       cntSizeEq2,
		"count_size_ge_2":       cntSizeGe2,
		"top100_max_size":       top100[0].Size,
		"top100_min_size":       top100[len(top100)-1].Size,
		"generated_at_unix_sec": time.Now().Unix(),
		"graph_db_path":         cfg.GraphDBPath,
		"isolated_dir":          cfg.RootOfIsolatedDir,
	}
	if err := writeJSON(filepath.Join(reportDir, "summary.json"), summary); err != nil {
		return err
	}
	if err := writeTopCSV(filepath.Join(reportDir, "components_top100.csv"), top100); err != nil {
		return err
	}

	maxSVG := 10
	if len(top100) < maxSVG {
		maxSVG = len(top100)
	}
	for k := 0; k < maxSVG; k++ {
		if err := writeComponentSVG(filepath.Join(reportDir, fmt.Sprintf("top_component_%02d.svg", k+1)), top100[k]); err != nil {
			fmt.Printf("   ⚠️ SVG failed for comp %d: %v\n", top100[k].ID, err)
		}
	}
	return nil
}

// 이하 writeEmptyReport / writeJSON / writeTopCSV / writeComponentSVG 는
// 이전에 드린 그대로 사용 (변경 없음)

func writeEmptyReport(reportDir string) error {
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		return err
	}
	sum := map[string]any{
		"components_total":   0,
		"nodes_total":        0,
		"avg_component_size": 0,
	}
	return writeJSON(filepath.Join(reportDir, "summary.json"), sum)
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func writeTopCSV(path string, comps []component) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"rank", "component_id", "size"})
	for i, c := range comps {
		_ = w.Write([]string{
			fmt.Sprintf("%d", i+1),
			fmt.Sprintf("%d", c.ID),
			fmt.Sprintf("%d", c.Size),
		})
	}
	return nil
}

// 매우 단순한 "원형 배치" SVG 그리기 (외부 라이브러리 X)
func writeComponentSVG(path string, comp component) error {
	n := len(comp.Nodes)
	if n == 0 {
		return os.WriteFile(path, []byte("<svg xmlns='http://www.w3.org/2000/svg' width='800' height='800'></svg>"), 0o644)
	}

	W, H := 900.0, 900.0
	cx, cy := W/2.0, H/2.0
	R := math.Min(W, H) * 0.36

	// 노드 좌표
	pos := make(map[string][2]float64, n)
	for i, addr := range comp.Nodes {
		theta := 2 * math.Pi * float64(i) / float64(n)
		x := cx + R*math.Cos(theta)
		y := cy + R*math.Sin(theta)
		pos[addr] = [2]float64{x, y}
	}

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%0.f" height="%0.f">`, W, H) + "\n")
	b.WriteString(`<rect width="100%" height="100%" fill="#ffffff"/>` + "\n")

	// 타이틀
	b.WriteString(fmt.Sprintf(`<text x="20" y="30" font-size="18" font-family="monospace">Component %d  |  size=%d  |  edges=%d</text>`,
		comp.ID, comp.Size, len(comp.Edges)) + "\n")

	// 간선
	b.WriteString(`<g stroke="#999" stroke-width="1" opacity="0.9">` + "\n")
	for _, e := range comp.Edges {
		a := pos[e[0]]
		c := pos[e[1]]
		b.WriteString(fmt.Sprintf(`<line x1="%0.1f" y1="%0.1f" x2="%0.1f" y2="%0.1f"/>`+"\n", a[0], a[1], c[0], c[1]))
	}
	b.WriteString(`</g>` + "\n")

	// 노드
	b.WriteString(`<g>` + "\n")
	for _, addr := range comp.Nodes {
		p := pos[addr]
		b.WriteString(fmt.Sprintf(`<circle cx="%0.1f" cy="%0.1f" r="8" fill="#1f77b4"/>`+"\n", p[0], p[1]))
		short := addr
		if len(short) > 10 {
			short = short[:6] + "…" + short[len(short)-4:]
		}
		b.WriteString(fmt.Sprintf(`<text x="%0.1f" y="%0.1f" font-size="10" font-family="monospace" text-anchor="middle" dy="-12">%s</text>`+"\n",
			p[0], p[1], short))
	}
	b.WriteString(`</g>` + "\n")

	b.WriteString(`</svg>`)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
