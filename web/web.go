// Package web serves the runtime-filter dashboard: query plan tree, filter
// lineage/size/FPR/rejection stats and per-operator timings.
package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtimefilter/verify"
)

// Server exposes the latest suite report.
type Server struct {
	Report verify.SuiteReport
}

// Handler builds the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.index)
	mux.HandleFunc("/api/report", s.report)
	return mux
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, indexHTML)
}

func (s *Server) report(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(s.Report)
}

const indexHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<title>运行时过滤器效果面板</title>
<style>
:root { --bg:#0f1420; --panel:#171e2e; --line:#26314a; --txt:#d7deec; --dim:#8493ad; --ok:#3fb96f; --bad:#e05b5b; --accent:#5b9dff; --warn:#e8a33d; }
* { box-sizing: border-box; }
body { margin:0; font: 13px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:var(--bg); color:var(--txt); }
header { padding:14px 22px; border-bottom:1px solid var(--line); display:flex; gap:18px; align-items:center; flex-wrap:wrap; }
header h1 { font-size:16px; margin:0; }
.badge { padding:2px 9px; border-radius:11px; background:#1d2a44; color:var(--accent); font-weight:600; }
.badge.ok { color:var(--ok); background:#14301f; }
.badge.bad { color:var(--bad); background:#351a1a; }
main { display:grid; grid-template-columns: 300px 1fr 380px; gap:14px; padding:14px 22px; align-items:start; }
.panel { background:var(--panel); border:1px solid var(--line); border-radius:10px; overflow:hidden; }
.panel h2 { font-size:13px; margin:0; padding:10px 14px; border-bottom:1px solid var(--line); color:var(--dim); text-transform:uppercase; letter-spacing:.06em; }
.scenario { padding:9px 14px; border-bottom:1px solid var(--line); cursor:pointer; }
.scenario:hover { background:#1c2538; }
.scenario.active { background:#1d2c47; border-left:3px solid var(--accent); padding-left:11px; }
.scenario .name { font-weight:600; }
.scenario .tags { color:var(--dim); font-size:11px; margin-top:2px; }
pre.plan { margin:0; padding:14px; font-family:ui-monospace,Menlo,monospace; white-space:pre-wrap; font-size:12px; }
table { width:100%; border-collapse:collapse; font-size:12px; }
th,td { text-align:left; padding:7px 10px; border-bottom:1px solid var(--line); vertical-align:top; }
th { color:var(--dim); font-weight:600; }
.kpi { display:grid; grid-template-columns:repeat(3,1fr); gap:10px; padding:12px 14px; }
.kpi .box { background:#121a2b; border:1px solid var(--line); border-radius:8px; padding:10px; }
.kpi .v { font-size:18px; font-weight:700; }
.kpi .l { color:var(--dim); font-size:11px; }
.mode-pill { display:inline-block; padding:1px 8px; border-radius:10px; font-weight:700; font-size:11px; }
.mode-EXACT { background:#14301f; color:var(--ok); }
.mode-BITMAP { background:#122b33; color:#4fd1c5; }
.mode-BLOOM { background:#1d2a44; color:var(--accent); }
.mode-MINMAX { background:#3a2c14; color:var(--warn); }
.hop { padding:7px 10px; border-bottom:1px solid var(--line); }
.hop .a { font-weight:700; }
.hop.PUSHED .a { color:var(--ok); }
.hop.BLOCKED .a { color:var(--bad); }
.hop.DEGRADED .a { color:var(--warn); }
.bar { height:8px; background:#101828; border-radius:5px; overflow:hidden; margin-top:4px; }
.bar i { display:block; height:100%; background:var(--accent); }
.bar.reject i { background:var(--ok); }
.bar.skip i { background:var(--warn); }
.muted { color:var(--dim); }
.rightcol { display:flex; flex-direction:column; gap:14px; }
.tag { display:inline-block; background:#1d2a44; color:var(--accent); border-radius:8px; padding:0 7px; margin:1px 3px 1px 0; font-size:11px; }
.err { color:var(--bad); padding:10px 14px; font-weight:600; }
</style>
</head>
<body>
<header>
  <h1>蚕种场 · 哈希连接运行时过滤器面板</h1>
  <span class="badge" id="seed">seed -</span>
  <span class="badge ok" id="passed">-</span>
  <span class="badge bad" id="failed" style="display:none"></span>
</header>
<main>
  <section class="panel" style="max-height:86vh;overflow:auto">
    <h2>测试场景</h2>
    <div id="scenarios"></div>
  </section>
  <section class="rightcol" style="display:contents">
    <div style="display:flex;flex-direction:column;gap:14px;min-width:0">
      <div class="panel"><h2>查询计划树</h2><pre class="plan" id="plan"></pre></div>
      <div class="panel">
        <h2>各算子耗时</h2>
        <table><thead><tr><th>算子</th><th>总耗时(ms)</th><th>构建(ms)</th><th>探测(ms)</th><th>输出行</th><th>块跳过/晚到</th></tr></thead>
        <tbody id="ops"></tbody></table>
      </div>
      <div class="panel">
        <h2>三种执行模式对比</h2>
        <table><thead><tr><th>模式</th><th>耗时(ms)</th><th>结果行数</th></tr></thead>
        <tbody id="modes"></tbody></table>
      </div>
    </div>
    <div style="display:flex;flex-direction:column;gap:14px;min-width:0">
      <div class="panel"><h2>运行时过滤器</h2><div id="filters"><div class="muted" style="padding:12px">选择场景</div></div></div>
    </div>
  </section>
</main>
<script>
let DATA=null, current=0;
function pct(x,y){ return y>0 ? (100*x/y).toFixed(1)+'%' : '0.0%'; }
function fmtBytes(n){ if(n>1048576)return (n/1048576).toFixed(1)+'MB'; if(n>1024)return (n/1024).toFixed(1)+'KB'; return n+'B'; }
async function load(){
  const r = await fetch('/api/report'); DATA = await r.json();
  document.getElementById('seed').textContent = 'seed '+DATA.seed;
  document.getElementById('passed').textContent = DATA.passed+' / '+DATA.total+' 通过';
  if(DATA.failed>0){ const f=document.getElementById('failed'); f.style.display=''; f.textContent=DATA.failed+' 失败'; }
  const sc = document.getElementById('scenarios');
  DATA.scenarios.forEach((s,i)=>{
    const d=document.createElement('div'); d.className='scenario'+(i===0?' active':'');
    d.innerHTML = '<div class="name">'+(s.passed?'✅':'❌')+' '+s.name+'</div><div class="tags">'+s.tags.map(t=>'<span class="tag">'+t+'</span>').join('')+'</div>';
    d.onclick=()=>{ current=i; render(); document.querySelectorAll('.scenario').forEach((e,j)=>e.classList.toggle('active',j===i)); };
    sc.appendChild(d);
  });
  render();
}
function render(){
  const s=DATA.scenarios[current];
  document.getElementById('plan').textContent=s.planText + (s.error?'\n\nERROR: '+s.error:'');
  const modes=document.getElementById('modes');
  modes.innerHTML = ['noRf','rf','nl'].map((k,i)=>{
    const label=['无运行时过滤','运行时过滤','嵌套循环对照'][i];
    const m=s[k]; return '<tr><td>'+label+'</td><td>'+m.elapsedMs.toFixed(3)+'</td><td>'+m.rows+'</td></tr>';
  }).join('');
  const ops=document.getElementById('ops');
  ops.innerHTML = s.ops.map(o=>'<tr><td>'+o.kind+' #'+o.nodeId+'<div class="muted">'+o.name+'</div></td><td>'+o.totalMs.toFixed(3)+'<div class="bar"><i style="width:'+barW(o.totalMs,s)+'"></i></div></td><td>'+o.buildMs.toFixed(3)+'</td><td>'+o.probeMs.toFixed(3)+'</td><td>'+o.rows+'</td><td class="muted">'+(o.extras.blocksSkipped||0)+' / '+(o.extras.lateRows||0)+'</td></tr>').join('');
  const fl=document.getElementById('filters');
  if(!s.filters.length){ fl.innerHTML='<div class="muted" style="padding:12px">该计划没有可下推的过滤器（可能被外连接规则阻断）</div>'; }
  fl.innerHTML = s.filters.map(f=>filterCard(f)).join('');
}
function barW(v,s){ const max=Math.max(...s.ops.map(o=>o.totalMs),0.001); return Math.max(2,100*v/max)+'%'; }
function filterCard(f){
  const rejPct=pct(f.rowsRejected,f.rowsTested);
  const skipPct=pct(f.blocksSkipped,f.blocksSeen+f.blocksSkipped);
  const fpr=f.hasObserved?(f.observedFpr*100).toFixed(2)+'%':'-';
  return '<div style="padding:12px 14px;border-bottom:1px solid var(--line)">'
    +'<div style="display:flex;justify-content:space-between;align-items:center"><strong>RF#'+f.id+' · '+f.joinName+'</strong><span class="mode-pill mode-'+f.mode+'">'+f.mode+'</span></div>'
    +'<div class="kpi"><div class="box"><div class="v">'+fmtBytes(f.sizeBytes)+'</div><div class="l">过滤器大小</div></div>'
    +'<div class="box"><div class="v">'+f.distinctKeys+'</div><div class="l">不同键 / 构建行 '+f.buildRows+'</div></div>'
    +'<div class="box"><div class="v">'+fpr+'</div><div class="l">实测误判率 (理论 '+(f.estimatedFpr>=0?(f.estimatedFpr*100).toFixed(2)+'%':'区间模式')+')</div></div></div>'
    +'<table><tr><td>键区间</td><td>'+(f.ranges||[]).join(' ')+'</td></tr>'
    +'<tr><td>NULL 语义</td><td>'+(f.nullPasses?'NULL 放行（保留侧）':'NULL 拒绝（等值连接）')+'</td></tr>'
    +'<tr><td>扫描行 / 测试 / 放行 / 滤掉</td><td>'+f.rowsScanned+' / '+f.rowsTested+' / '+f.rowsPassed+' / <b>'+f.rowsRejected+'</b></td></tr>'
    +'<tr><td>行级滤除率</td><td>'+rejPct+'<div class="bar reject"><i style="width:'+rejPct+'"></i></div></td></tr>'
    +'<tr><td>块级跳过</td><td>'+f.blocksSkipped+' / '+(f.blocksSeen+f.blocksSkipped)+' 块 ('+skipPct+')<div class="bar skip"><i style="width:'+skipPct+'"></i></div></td></tr>'
    +'<tr><td>NULL 行 / 晚到行</td><td>'+f.nullRows+' / '+f.lateRows+'</td></tr>'
    +'<tr><td>状态</td><td>'+(f.disabled?'<span style="color:var(--warn)">自适应停用（滤除太少）</span>':'启用')+(f.degraded?' · <span style="color:var(--warn)">超尺寸退化为区间</span>':'')+'</td></tr></table>'
    +'<div style="margin-top:8px"><div class="muted" style="padding:0 10px 4px">下推路径（从哪生成 → 推到哪）</div>'
    +f.hops.map(h=>'<div class="hop '+h.action+'"><span class="a">'+h.action+'</span> · '+h.fromNode+' → '+h.toNode+'<div class="muted">'+h.note+'</div></div>').join('')
    +'</div></div>';
}
load();
</script>
</body>
</html>`
