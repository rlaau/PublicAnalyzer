package app

import (
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"time"
)

// === UI HTML (Sigma iframe & Background) ===

// 1) 그래프 프레임(iframe 안에서 동작)
func (b *BadgerRopeDB) MakeGraphFrameHTML(title string) string {
	tpl := `<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>__TITLE__</title>
  <style>
    html,body{height:100%;margin:0}
    #container{position:absolute;inset:0}
    #hud{position:absolute;top:10px;left:10px;z-index:10;background:rgba(255,255,255,.92);padding:8px 10px;border-radius:10px;font:12px/1.3 system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Noto Sans,Arial;box-shadow:0 2px 10px rgba(0,0,0,.08)}
    #hud code{padding:2px 6px;background:#eee;border-radius:6px}
    #diag{position:absolute;right:10px;bottom:10px;z-index:10;background:rgba(0,0,0,.6);color:#fff;padding:8px 10px;border-radius:8px;font:12px/1.3 system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Noto Sans,Arial;white-space:pre-wrap;max-width:40vw}
  </style>
  <script src="https://unpkg.com/graphology@0.25.4/dist/graphology.umd.min.js"></script>
  <script src="https://unpkg.com/sigma@2.4.0/build/sigma.min.js"></script>
</head>
<body>
  <div id="hud"><strong>__TITLE__</strong>
    <div>nodes: <code id="ncount">0</code> | edges: <code id="ecount">0</code></div>
  </div>
  <div id="container"></div>
  <div id="diag"></div>

<script>
(function(){
  const diag = document.getElementById('diag');
  const log  = (...a)=>{ console.log(...a); diag.textContent += a.map(x=>typeof x==='string'?x:JSON.stringify(x)).join(' ')+'\\n'; };

  const SigmaCtor = (window && (window.Sigma || (window.sigma && window.sigma.Sigma))) || null;
  const GraphologyGraph = (window && window.graphology && window.graphology.Graph) ? window.graphology.Graph : null;

  if (!SigmaCtor) { diag.textContent = 'sigma UMD not found\\n'; }
  if (!GraphologyGraph) { diag.textContent += 'graphology UMD not found\\n'; }

  let GRAPH = null, RENDERER = null, DATA = null;

  // --- Color helpers (모두 HEX로 통일) ---
  function clamp01(x){ return Math.max(0, Math.min(1, x)); }
  function hslToHex(h, s, l){
    // h: 0..360, s/l: 0..100
    h = ((h%360)+360)%360; s = clamp01(s/100); l = clamp01(l/100);
    const c = (1 - Math.abs(2*l - 1)) * s;
    const hp = h / 60;
    const x = c * (1 - Math.abs((hp % 2) - 1));
    let r=0,g=0,b=0;
    if (0<=hp && hp<1){ r=c; g=x; b=0; }
    else if (1<=hp && hp<2){ r=x; g=c; b=0; }
    else if (2<=hp && hp<3){ r=0; g=c; b=x; }
    else if (3<=hp && hp<4){ r=0; g=x; b=c; }
    else if (4<=hp && hp<5){ r=x; g=0; b=c; }
    else if (5<=hp && hp<6){ r=c; g=0; b=x; }
    const m = l - c/2;
    r = Math.round((r+m)*255);
    g = Math.round((g+m)*255);
    b = Math.round((b+m)*255);
    const toHex=n=>n.toString(16).padStart(2,'0');
    return '#'+toHex(r)+toHex(g)+toHex(b);
  }
  function parseColorToHex(c){
    if (!c) return '#9aa0a6';
    if (c[0] === '#') return c; // 이미 hex
    const s = String(c).trim();
    console.log('parseColorToHex input:', s);
    // hsl(h, s%, l%)
    let m = /^hsl\(\s*([\d.]+)\s*,\s*([\d.]+)%\s*,\s*([\d.]+)%\s*\)$/i.exec(s);
    if (m) {
      const result = hslToHex(parseFloat(m[1]), parseFloat(m[2]), parseFloat(m[3]));
      console.log('HSL matched:', m[1], m[2], m[3], '→', result);
      return result;
    }
    // rgb(r,g,b)
    m = /^rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$/i.exec(s);
    if (m){
      const r = Math.max(0, Math.min(255, parseInt(m[1],10)));
      const g = Math.max(0, Math.min(255, parseInt(m[2],10)));
      const b = Math.max(0, Math.min(255, parseInt(m[3],10)));
      const toHex=n=>n.toString(16).padStart(2,'0');
      const result = '#'+toHex(r)+toHex(g)+toHex(b);
      console.log('RGB matched:', r, g, b, '→', result);
      return result;
    }
    // 알 수 없으면 기본값
    console.log('No pattern matched for:', s, '→ using default #9aa0a6');
    return '#9aa0a6';
  }

  // === 팔레트: 범례 우선 ===
  function buildTraitPaletteFromLegend() {
    const pal = {};
    try {
      const L = DATA && DATA.legend && DATA.legend.traits ? DATA.legend.traits : {};
      Object.keys(L).forEach(k=>{
        const key = Number(k);
        const item = L[k] || {};
        if (key === 0 && !item.color) return; // 방어
        pal[key] = parseColorToHex(item.color || '#9aa0a6');
      });
    } catch (_) {}
    return pal;
  }
  function traitColorFromLegend(tc, fallback) {
    const c = TRAIT_PALETTE[tc];
    return c || (fallback ? parseColorToHex(fallback) : null);
  }
  function genTraitColor(code){
    // RopeColor와 동일한 방식으로 명확한 색상 구분
    const h = (code * 89 + 180) % 360;  // RopeColor와 유사하지만 다른 오프셋
    const s = 65 + (code % 4) * 8;      // 65-89% 범위 (4단계)
    const l = 40 + (code % 5) * 5;      // 40-60% 범위 (5단계)
    return hslToHex(h, s, l);
  }

  // === 빌더 ===
  let TRAIT_PALETTE = {};

  function buildGraphFromData(data){
    DATA = data;
    TRAIT_PALETTE = buildTraitPaletteFromLegend();

    // multi-edge 허용
    const G = new GraphologyGraph({ type:'undirected', multi:true });

    // 1) 노드: 중립(단색)
    const NODE_COLOR = '#222222';
    const NODE_LABEL = '#ffffff';
    const N = (data.nodes||[]).length || 1;
    (data.nodes||[]).forEach((n,i)=>{
      G.addNode(n.id, {
        label: n.label || n.id,
        size: Math.max(4, Math.min(16, n.size||8)),
        color: NODE_COLOR,
        labelColor: NODE_LABEL,
        x: Math.cos(2*Math.PI*i/N)*120,
        y: Math.sin(2*Math.PI*i/N)*120
      });
    });

    // 2) 간선: 트레이트 팔레트 색 + 굵기 가변
    const pairCount = Object.create(null); // "lo|hi" -> count
    const eseen = new Set();

    (data.edges||[]).forEach((e)=>{
      if (eseen.has(e.id)) return; eseen.add(e.id);
      const src = e.source, dst = e.target;
      if (!G.hasNode(src)) G.addNode(src, { label: src, size: 6, color: NODE_COLOR, labelColor: NODE_LABEL });
      if (!G.hasNode(dst)) G.addNode(dst, { label: dst, size: 6, color: NODE_COLOR, labelColor: NODE_LABEL });

      const lo = src < dst ? src : dst;
      const hi = src < dst ? dst : src;
      const key = lo + '|' + hi;
      const k = (pairCount[key] = (pairCount[key] || 0) + 1);

      const width = Math.max(1, Math.min(5, (e.weight||1) * (1 + (k-1)*0.2)));
      const tc = Number(e.traitCode) || 0;
      // 색상 우선순위: 1) EdgeInfo.Color, 2) 범례에서 TraitCode로 찾기, 3) 생성된 색상
      let color;
      if (e.color) {
        color = parseColorToHex(e.color);
        console.log("TraitCode " + tc + ": EdgeInfo.color: " + e.color + " → " + color + " (edge: " + e.id + ")");
      } else {
        const legendColor = traitColorFromLegend(tc, null);
        const genColor = genTraitColor(tc);
        color = legendColor || genColor;
        // HSL 값 계산 표시 (RopeColor 방식)
        const h = (tc * 89 + 180) % 360;
        const s = 65 + (tc % 4) * 8;
        const l = 40 + (tc % 5) * 5;
        console.log("TraitCode " + tc + ": HSL(" + h + ", " + s + "%, " + l + "%) → " + color + " (edge: " + e.id + ")");
      }

      G.addEdgeWithKey(e.id, src, dst, {
        size: width,
        color: color,
        traitCode: e.traitCode, traitName: e.traitName, traitId: e.traitId, lastSeen: e.lastSeen||0,
      });
    });
    return G;
  }

  function mount(G){
    const container = document.getElementById('container');
    if (RENDERER) { RENDERER.kill(); RENDERER = null; }
    GRAPH = G;

    RENDERER = new SigmaCtor(GRAPH, container, {
      renderLabels: true,
      labelDensity: 0.85,
      labelSize: 10,
      labelColor: { color: 'data(labelColor)' },
      edgeColor:'data',
      color: 'data',
      // edgeColor:'data'는 브라우저/빌드에 따라 무시될 수 있으니 reducer로 강제
      enableEdgeHoverEvents: true,
      edgeHoverPrecision: 6,
      edgeReducer: (edge, data) => {
        // 색/두께를 항상 최종적으로 강제
        const c = parseColorToHex(data.color || '#9aa0a6');
        console.log("Edge", edge, "Final color:", c, "Original data.color:", data.color);
        let w = Math.max(4, Math.min(7, data.size || 2));
        if (data.hovered || GRAPH.getEdgeAttribute(edge, '__hover__')) {
          w = Math.max(w, 7);
        }

        return { ...data, color: c, size: w };
      },
    });

    // HUD
    document.getElementById('ncount').textContent = String(GRAPH.order);
    document.getElementById('ecount').textContent = String(GRAPH.size);

    // ── 기본 이벤트 (노드/엣지) ──
    RENDERER.on('enterNode', ({ node }) => {
      const attrs = GRAPH.getNodeAttributes(node);
      const payload = { id: node, label: attrs.label || '', ropeColor: attrs.color };
      window.parent && window.parent.postMessage({ type: 'node-hover', payload }, '*');
    });
    RENDERER.on('leaveNode', () => {
      window.parent && window.parent.postMessage({ type: 'node-hover', payload: null }, '*');
    });

    RENDERER.on('enterEdge', ({ edge }) => {
      const a = GRAPH.getEdgeAttributes(edge);
      window.parent && window.parent.postMessage({ type:'edge-hover', payload:{
        id: edge, traitCode: a.traitCode||0, traitName: a.traitName||'', traitId: a.traitId||0, lastSeen: a.lastSeen||0
      }}, '*');
    });
    RENDERER.on('leaveEdge', () => {
      window.parent && window.parent.postMessage({ type:'edge-hover', payload:null }, '*');
    });

    RENDERER.on('clickNode', ({ node }) => {
      window.parent && window.parent.postMessage({ type:'node-click', payload:{ id: node }}, '*');
    });

    // ── 커스텀 엣지 픽킹 (넓은 히트존) ──
    let raf = 0;
    let lastHovered = null;
    const BASE_PICK_RADIUS = 512; // px: 넓게

    function distToSegment(px, py, x1, y1, x2, y2) {
      const vx = x2 - x1, vy = y2 - y1;
      const wx = px - x1, wy = py - y1;
      const c1 = vx*wx + vy*wy;
      if (c1 <= 0) return Math.hypot(px - x1, py - y1);
      const c2 = vx*vx + vy*vy;
      if (c2 <= c1) return Math.hypot(px - x2, py - y2);
      const t = c1 / c2;
      const projx = x1 + t * vx, projy = y1 + t * vy;
      return Math.hypot(px - projx, py - projy);
    }

    function pickEdgeAt(clientX, clientY) {
      const rect = container.getBoundingClientRect();
      const x = clientX - rect.left;
      const y = clientY - rect.top;
      let bestEdge = null, bestDist = Infinity;

      for (const edge of GRAPH.edges()) {
        const dd = RENDERER.getEdgeDisplayData(edge);
        if (!dd) continue; // 뷰포트 밖
        const d = distToSegment(x, y, dd.x1, dd.y1, dd.x2, dd.y2);
        if (d < bestDist) { bestDist = d; bestEdge = edge; }
      }
      return (bestDist <= BASE_PICK_RADIUS) ? bestEdge : null;
    }

    function handleMove(ev) {
      if (raf) cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() => {
        const e = ev.touches ? ev.touches[0] : ev;
        const hit = pickEdgeAt(e.clientX, e.clientY);
        if (hit !== lastHovered) {
          if (lastHovered && GRAPH.hasEdge(lastHovered))
            GRAPH.setEdgeAttribute(lastHovered, '__hover__', false);
          lastHovered = hit;

          if (hit) {
            GRAPH.setEdgeAttribute(hit, '__hover__', true);
            const a = GRAPH.getEdgeAttributes(hit);
            window.parent && window.parent.postMessage({ type:'edge-hover', payload:{
              id: hit, traitCode: a.traitCode||0, traitName: a.traitName||'', traitId: a.traitId||0, lastSeen: a.lastSeen||0
            }}, '*');
          } else {
            window.parent && window.parent.postMessage({ type:'edge-hover', payload:null }, '*');
          }
          RENDERER.refresh();
        }
      });
    }

    function handleLeave() {
      if (lastHovered && GRAPH.hasEdge(lastHovered)) {
        GRAPH.setEdgeAttribute(lastHovered, '__hover__', false);
        lastHovered = null;
        RENDERER.refresh();
      }
      window.parent && window.parent.postMessage({ type:'edge-hover', payload:null }, '*');
    }

    // 이벤트 바인딩
    container.addEventListener('mousemove', handleMove, { passive: true });
    container.addEventListener('touchmove', handleMove, { passive: true });
    container.addEventListener('mouseleave', handleLeave, { passive: true });

  } // mount

  // 메시징
  window.addEventListener('message', (ev)=>{
    const msg = ev.data||{};
    if (msg.type==='load-graph'){
      try {
        const g=buildGraphFromData(msg.payload);
        mount(g);
        window.parent && window.parent.postMessage({type:'child-ready', payload:{ok:true}}, '*');
      } catch(e){
        log('load-graph error', e);
        window.parent && window.parent.postMessage({type:'child-ready', payload:{ok:false, error:String(e)}}, '*');
      }
    } else if (msg.type==='expand'){
      const v = msg.payload && msg.payload.vertexId;
      if (!v) return;
      const nbr = new Set(); const edges=[]; const nodes=[];
      (DATA.edges||[]).forEach(e=>{ if (e.source===v){nbr.add(e.target);edges.push(e)} else if(e.target===v){nbr.add(e.source);edges.push(e)} });
      (DATA.nodes||[]).forEach(n=>{ if(n.id===v || nbr.has(n.id)) nodes.push(n) });
      const inc = { meta:{generatedAt:Math.floor(Date.now()/1000), graphType:'expanded', startNode:v, nodeCount:nodes.length, edgeCount:edges.length}, nodes, edges, legend: DATA.legend||{traits:{}, ropes:{}} };
      window.parent && window.parent.postMessage({type:'expand-result', payload:inc}, '*');
    } else if (msg.type==='apply-graph'){
      const g = buildGraphFromData(msg.payload);
      mount(g);
    }
  });

  // 초기 준비 신호
  window.parent && window.parent.postMessage({type:'child-mounted'}, '*');
})();
</script>

</body></html>`
	return strings.ReplaceAll(tpl, "__TITLE__", html.EscapeString(title))
}

// 2) 배경(컨테이너) 페이지
func (b *BadgerRopeDB) MakeBackgroundHTML(frameSrc string) (string, error) {
	init, err := b.FetchDefaultGraphJson()
	if err != nil {
		// 그래도 틀은 반환
		init = GraphData{Meta: MetaInfo{GeneratedAt: time.Now().Unix(), GraphType: "default"}}
	}
	j, _ := json.Marshal(init)

	traitHTML := b.legendHTML(init.Legend.Traits)
	ropeHTML := b.legendHTML(init.Legend.Ropes)

	tpl := `<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>RopeDB Visualizer</title>
<style>
  *{box-sizing:border-box}
  body{margin:0;height:100vh;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;display:grid;grid-template-columns:60% 40%;grid-template-rows:60% 40%;gap:10px;background:#f5f5f5;padding:10px}
  #graph,#search,#legend,#info{background:#fff;border-radius:10px;box-shadow:0 2px 10px rgba(0,0,0,.08);overflow:hidden}
  #graph{grid-column:1;grid-row:1}
  #search{grid-column:2;grid-row:1;padding:16px}
  #legend{grid-column:1;grid-row:2;padding:16px}
  #info{grid-column:2;grid-row:2;padding:16px}
  #f{width:100%;height:100%;border:0}
  .sec h3{margin:0 0 10px 0;font-size:14px;color:#333}
  .row{margin-bottom:12px}
  input[type=text]{width:100%;padding:10px;border:1px solid #ddd;border-radius:8px;font-size:13px}
  button{margin-top:6px;width:100%;padding:10px;border:0;border-radius:8px;cursor:pointer;background:#2f6fed;color:#fff;font-weight:600}
  button.green{background:#28a745}
  .legend-item{display:flex;align-items:center;margin-bottom:6px}
  .legend-color{width:16px;height:16px;border-radius:3px;margin-right:8px}
  .kv{font-size:12px;color:#555}.kv b{color:#222}.muted{color:#888;font-size:12px}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
</style>
</head>
<body>
  <section id="graph"><iframe id="f" src="__FRAME__"></iframe></section>

  <section id="search">
    <div class="sec">
      <h3>🔍 Address</h3>
      <div class="row"><input id="addr" type="text" placeholder="0x1234..."/></div>
      <button id="addrBtn">Search</button>
    </div>
    <div class="sec" style="margin-top:14px">
      <h3>🏷 Trait</h3>
      <div class="row"><input id="trait" type="text" placeholder="TraitApple or 100"/></div>
      <button id="traitBtn">Search</button>
    </div>
    <div class="sec" style="margin-top:14px">
      <h3>🧵 Rope</h3>
      <div class="row"><input id="rope" type="text" placeholder="Rope ID"/></div>
      <button id="ropeBtn">Search</button>
    </div>
    <button id="defBtn" class="green" style="margin-top:14px">🏠 Default</button>
  </section>

  <section id="legend">
    <div class="sec"><h3>🎨 Trait Colors</h3><div id="traitLegend">__TL__</div></div>
    <div class="sec" style="margin-top:10px"><h3>🧵 Rope Colors</h3><div id="ropeLegend">__RL__</div></div>
  </section>

  <section id="info">
    <div class="sec">
      <h3>ℹ️ Info</h3>
      <div id="stat" class="kv">
        Nodes: <b id="sn">0</b> | Edges: <b id="se">0</b><br/>
        Type: <b id="st">-</b><br/>
        Start: <span id="ss" class="mono muted">-</span>
      </div>
    </div>
    <div class="sec" style="margin-top:10px">
      <h3>Node</h3>
      <div id="nodeBox" class="kv muted">Hover a node…</div>
    </div>
    <div class="sec" style="margin-top:10px">
      <h3>Edge</h3>
      <div id="edgeBox" class="kv muted">Hover an edge…</div>
    </div>
  </section>

<script>
const INITIAL = __INITIAL__;
const f = document.getElementById('f');
const sn=document.getElementById('sn'), se=document.getElementById('se'), st=document.getElementById('st'), ss=document.getElementById('ss');
const nodeBox=document.getElementById('nodeBox'), edgeBox=document.getElementById('edgeBox');

function syncStats(g){
  sn.textContent = String(g.meta?.nodeCount || (g.nodes?g.nodes.length:0) || 0);
  se.textContent = String(g.meta?.edgeCount || (g.edges?g.edges.length:0) || 0);
  st.textContent = g.meta?.graphType || '-';
  ss.textContent = g.meta?.startNode || '-';
}
syncStats(INITIAL);

window.addEventListener('message', (ev)=>{
  const msg = ev.data||{};
  if (msg.type==='child-mounted'){
    f.contentWindow.postMessage({type:'load-graph', payload: INITIAL}, '*');
  } else if (msg.type==='child-ready'){
    // ok 여부만 확인
  } else if (msg.type==='node-hover'){
    if (!msg.payload){ nodeBox.textContent='Hover a node…'; nodeBox.classList.add('muted'); return; }
    nodeBox.classList.remove('muted');
    nodeBox.innerHTML = 'ID: <span class="mono">'+msg.payload.id+'</span><br/>Label: <b>'+(msg.payload.label||'')+'</b>';
  } else if (msg.type==='edge-hover'){
    if (!msg.payload){ edgeBox.textContent='Hover an edge…'; edgeBox.classList.add('muted'); return; }
    edgeBox.classList.remove('muted');
    const p = msg.payload;
    edgeBox.innerHTML = 'Trait: <b>'+(p.traitName||('code '+p.traitCode))+'</b> <span class="mono">#'+(p.traitId||0)+'</span>';
  } else if (msg.type==='node-click'){
    const id = msg.payload && msg.payload.id;
    if (id){ f.contentWindow.postMessage({type:'expand', payload:{vertexId:id}}, '*'); }
  } else if (msg.type==='expand-result'){
    const inc = msg.payload; if (!inc) return;
    const merged = mergeGraph(INITIAL, inc);
    Object.assign(INITIAL, merged);
    syncStats(INITIAL);
    f.contentWindow.postMessage({type:'apply-graph', payload: INITIAL}, '*');
  }
});

function mergeGraph(base, inc){
  const nm = new Map(), em = new Map();
  (base.nodes||[]).forEach(n=>nm.set(n.id,n));
  (inc.nodes||[]).forEach(n=>nm.set(n.id,n));
  (base.edges||[]).forEach(e=>em.set(e.id,e));
  (inc.edges||[]).forEach(e=>em.set(e.id,e));
  const nodes=[...nm.values()], edges=[...em.values()];
  return { meta:{generatedAt:Math.floor(Date.now()/1000),graphType:'convolution',
           startNode: base.meta?.startNode || inc.meta?.startNode || '',
           nodeCount:nodes.length,edgeCount:edges.length}, nodes, edges,
           legend: base.legend || inc.legend || {traits:{},ropes:{}} };
}

// 검색(훅 자리만)
document.getElementById('defBtn').onclick = ()=>{ f.contentWindow.postMessage({type:'load-graph', payload: INITIAL}, '*'); syncStats(INITIAL); };
document.getElementById('addrBtn').onclick = ()=>{
  const v=(document.getElementById('addr').value||'').trim();
  if (!v) return alert('enter address');
  // 서버붙이면 여기서 FetchExpandedGraphByVertex 호출 → ConvolutionGraph 후 apply-graph
  f.contentWindow.postMessage({type:'expand', payload:{vertexId:v}}, '*');
};
document.getElementById('traitBtn').onclick = ()=>{ alert('Trait search: hook to FetchGraphByTraitCode later'); };
document.getElementById('ropeBtn').onclick = ()=>{ alert('Rope search: hook to FetchGraphByRopeID later'); };
</script>
</body></html>`

	out := strings.ReplaceAll(tpl, "__FRAME__", html.EscapeString(frameSrc))
	out = strings.ReplaceAll(out, "__INITIAL__", string(j))
	out = strings.ReplaceAll(out, "__TL__", traitHTML)
	out = strings.ReplaceAll(out, "__RL__", ropeHTML)
	return out, nil
}

// 범례 HTML(공용)
func (b *BadgerRopeDB) legendHTML(m map[string]LegendItem) string {
	var sb strings.Builder
	for _, it := range m {
		sb.WriteString(fmt.Sprintf(
			`<div class="legend-item"><div class="legend-color" style="background:%s"></div><span>%s</span><span style="margin-left:auto;color:#888;font-size:11px">%d</span></div>`,
			it.Color, html.EscapeString(it.Name), it.Count))
	}
	return sb.String()
}
