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
  const Graph     = (window && window.graphology && window.graphology.Graph) ? window.graphology.Graph : null;

  if (!SigmaCtor) { diag.textContent = 'sigma UMD not found\\n'; }
  if (!Graph)     { diag.textContent += 'graphology UMD not found\\n'; }

  let GRAPH = null, RENDERER = null, DATA = null;

  function colorHsl(h,s,l){ return 'hsl('+h+','+s+'%,'+l+'%)'; }
  function ropeColor(id){ const h=(id*89+45)%360, s=60+(id%3)*10, l=35+(id%4)*5; return colorHsl(h,s,l); }
  function traitColor(code){ const h=(code*137)%360; return colorHsl(h,70,45); }
  function labelColor(bg){
    const m=/hsl\\(\\s*(\\d+)\\s*,\\s*(\\d+)%\\s*,\\s*(\\d+)%\\s*\\)/.exec(bg); const l=m?+m[3]:50; return l>55?'#222':'#fff';
  }

  function buildGraphFromData(data){
    DATA = data;
    const G = new Graph({ type:'undirected', multi:true }); // multi-edge 허용
    // nodes
    (data.nodes||[]).forEach((n,i)=>{
      const c = n.color || (n.ropeId ? ropeColor(n.ropeId) : '#888');
      G.addNode(n.id, {
        label: n.label || n.id,
        size: Math.max(4, Math.min(16, n.size||8)),
        color: c,
        labelColor: labelColor(c),
        x: Math.cos(2*Math.PI*i/Math.max(1,(data.nodes||[]).length))*100,
        y: Math.sin(2*Math.PI*i/Math.max(1,(data.nodes||[]).length))*100
      });
    });

    // edges (쌍별 카운팅으로 시각적 분산값 기록)
    const pairCounts = Object.create(null); // "lo|hi" -> count
    const eseen = new Set();
    (data.edges||[]).forEach((e)=>{
      if (eseen.has(e.id)) return; eseen.add(e.id);
      const src = e.source, dst = e.target;
      if (!G.hasNode(src)) { const c='#888'; G.addNode(src,{label:src,size:6,color:c,labelColor:labelColor(c)}); }
      if (!G.hasNode(dst)) { const c='#888'; G.addNode(dst,{label:dst,size:6,color:c,labelColor:labelColor(c)}); }

      const lo = src < dst ? src : dst;
      const hi = src < dst ? dst : src;
      const k  = (pairCounts[lo+'|'+hi] = (pairCounts[lo+'|'+hi]||0)+1);
      const curveness = 0.15 + (k-1)*0.08; // (UMD 에선 실제 커브 렌더는 보류됨; 값은 보관)

      G.addEdgeWithKey(e.id, src, dst, {
        size: Math.max(1, Math.min(5, e.weight||1)),
        color: e.color || traitColor(e.traitCode||0),
        traitCode: e.traitCode, traitName: e.traitName, traitId: e.traitId, lastSeen: e.lastSeen||0,
        curveness: curveness
      });
    });
    return G;
  }

  function mount(G){
    const container = document.getElementById('container');
    if (RENDERER) { RENDERER.kill(); RENDERER=null; }
    GRAPH = G;
    RENDERER = new SigmaCtor(GRAPH, container, {
      renderLabels: true, labelDensity: 0.85, labelSize: 10,
      labelColor: { color: 'data(labelColor)' }
    });
    document.getElementById('ncount').textContent = String(GRAPH.order);
    document.getElementById('ecount').textContent = String(GRAPH.size);

    // hover: node
    RENDERER.on('enterNode', ({node})=>{
      const a = GRAPH.getNodeAttributes(node);
      window.parent && window.parent.postMessage({type:'node-hover', payload:{ id: node, label: a.label, color: a.color }}, '*');
    });
    RENDERER.on('leaveNode', ()=>{ window.parent && window.parent.postMessage({type:'node-hover', payload:null}, '*'); });

    // hover: edge
    RENDERER.on('enterEdge', ({edge})=>{
      const a = GRAPH.getEdgeAttributes(edge);
      window.parent && window.parent.postMessage({type:'edge-hover', payload:{
        id: edge, traitCode: a.traitCode||0, traitName: a.traitName||'', traitId: a.traitId||0, lastSeen: a.lastSeen||0
      }}, '*');
    });
    RENDERER.on('leaveEdge', ()=>{ window.parent && window.parent.postMessage({type:'edge-hover', payload:null}, '*'); });

    // click: node
    RENDERER.on('clickNode', ({node})=>{
      window.parent && window.parent.postMessage({type:'node-click', payload:{ id: node }}, '*');
    });
  }

  // local 1-hop (테스트용, 서버 붙이면 parent가 직접 JSON 호출)
  function localOneHop(vid){
    if (!DATA) return null;
    const nbr = new Set(); const edges=[]; const nodes=[];
    (DATA.edges||[]).forEach(e=>{ if (e.source===vid){nbr.add(e.target);edges.push(e)} else if(e.target===vid){nbr.add(e.source);edges.push(e)} });
    (DATA.nodes||[]).forEach(n=>{ if(n.id===vid || nbr.has(n.id)) nodes.push(n) });
    return {
      meta:{generatedAt:Math.floor(Date.now()/1000), graphType:'expanded', startNode:vid, nodeCount:nodes.length, edgeCount:edges.length},
      nodes, edges, legend: DATA.legend||{traits:{}, ropes:{}}
    };
  }

  window.addEventListener('message', (ev)=>{
    const msg = ev.data||{};
    if (msg.type==='load-graph'){
      try { const g=buildGraphFromData(msg.payload); mount(g);
        window.parent && window.parent.postMessage({type:'child-ready', payload:{ok:true}}, '*');
      } catch(e){ log('load-graph error', e); window.parent && window.parent.postMessage({type:'child-ready', payload:{ok:false, error:String(e)}}, '*'); }
    } else if (msg.type==='expand'){
      const v = msg.payload && msg.payload.vertexId;
      if (!v) return;
      const inc = localOneHop(v);
      window.parent && window.parent.postMessage({type:'expand-result', payload:inc}, '*');
    } else if (msg.type==='apply-graph'){
      const g = buildGraphFromData(msg.payload);
      mount(g);
    }
  });

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
