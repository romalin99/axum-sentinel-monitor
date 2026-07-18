use crate::Config;

// Fiber v3 subtracts 200 ms from the collection interval so the browser asks
// for the next snapshot shortly before the following server-side sample.
const FIBER_TIMEOUT_DIFF_MS: u128 = 200;
// Fiber v3 removes the oldest point once a chart has more than 50 labels,
// which results in a rolling window of at most 51 points.
const FIBER_HISTORY_POINTS: usize = 51;

pub(crate) fn render(config: &Config) -> String {
    let poll_ms = config
        .refresh
        .as_millis()
        .saturating_sub(FIBER_TIMEOUT_DIFF_MS)
        .max(FIBER_TIMEOUT_DIFF_MS)
        .to_string();
    let font = asset("link", "href", &config.font_url);
    let chart = asset("script", "src", &config.chart_js_url);
    TEMPLATE
        .replace("__TITLE__", &escape_text(&config.title))
        .replace("__FONT__", &font)
        .replace("__CHART__", &chart)
        .replace("__POLL_MS__", &poll_ms)
        .replace("__HISTORY_POINTS__", &FIBER_HISTORY_POINTS.to_string())
        .replace("__CUSTOM_HEAD__", &config.custom_head)
}

fn asset(tag: &str, attribute: &str, url: &str) -> String {
    if url.is_empty() {
        String::new()
    } else if tag == "link" {
        format!(
            r#"<link rel="stylesheet" {attribute}="{}">"#,
            escape_attribute(url)
        )
    } else {
        format!(
            r#"<script {attribute}="{}"></script>"#,
            escape_attribute(url)
        )
    }
}

fn escape_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn escape_attribute(value: &str) -> String {
    escape_text(value)
}

const TEMPLATE: &str = r##"<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__</title>__FONT____CHART__
<style>
:root{color-scheme:dark;--bg:#080c18;--panel:#121a30;--panel2:#0e1526;--line:rgba(148,163,194,.14);--text:#eef2fb;--muted:#8b98b8;--cyan:#38bdf8;--green:#34d399;--violet:#a78bfa;--teal:#22d3ee;--amber:#fbbf24;--pink:#f472b6;--indigo:#818cf8}
*{box-sizing:border-box}
html,body{margin:0;min-height:100%}
body{background:radial-gradient(1200px 620px at 15% -10%,rgba(56,189,248,.14),transparent 60%),radial-gradient(1000px 560px at 110% 0%,rgba(167,139,250,.12),transparent 55%),var(--bg);color:var(--text);font-family:Roboto,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;-webkit-font-smoothing:antialiased}
main{width:min(1360px,94vw);margin:34px auto 48px}
header{display:flex;align-items:flex-end;justify-content:space-between;gap:18px;margin-bottom:22px;padding-bottom:18px;border-bottom:1px solid var(--line)}
h1{margin:2px 0 0;font-size:clamp(1.6rem,3.2vw,2.6rem);font-weight:900;letter-spacing:-.01em;background:linear-gradient(92deg,#fff,#a9c7ff);-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
.label{color:var(--muted);font-size:.72rem;text-transform:uppercase;letter-spacing:.14em;font-weight:700}
.status{display:inline-flex;align-items:center;gap:8px;color:var(--muted);font-size:.82rem;padding:7px 13px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.02)}
.status::before{content:"";width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 0 0 rgba(52,211,153,.6);animation:pulse 2s infinite}
.status.down::before{background:#f87171;animation:none}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(52,211,153,.5)}70%{box-shadow:0 0 0 7px rgba(52,211,153,0)}100%{box-shadow:0 0 0 0 rgba(52,211,153,0)}}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:14px}
.card,.panel{position:relative;background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:16px;box-shadow:0 18px 40px rgba(0,0,0,.35);overflow:hidden}
.card{padding:17px 18px}
.card::before{content:"";position:absolute;inset:0 0 auto 0;height:3px;background:var(--accent,var(--cyan));opacity:.85}
.card .value{font-size:1.9rem;font-weight:900;margin-top:9px;letter-spacing:-.02em;color:var(--accent,var(--text));font-variant-numeric:tabular-nums}
.card .detail{color:var(--muted);font-size:.76rem;margin-top:4px;min-height:16px}
.panels{display:grid;grid-template-columns:repeat(2,1fr);gap:14px}
.panel{padding:16px 18px 12px;height:280px;display:flex;flex-direction:column;transition:border-color .2s,transform .2s}
.panel:hover{border-color:rgba(148,163,194,.3);transform:translateY(-2px)}
.panel .head{display:flex;align-items:baseline;justify-content:space-between;gap:12px;margin-bottom:10px}
.panel .metric{font-size:.78rem;color:var(--muted);text-transform:uppercase;letter-spacing:.09em;font-weight:700}
.panel .big{font-size:1.55rem;font-weight:900;letter-spacing:-.02em;font-variant-numeric:tabular-nums;color:var(--accent,var(--text))}
.panel .big .sub{font-size:.72rem;font-weight:700;color:var(--muted);margin-left:6px;letter-spacing:0}
.panel .canvas-wrap{position:relative;flex:1}
canvas{width:100%!important;height:100%!important}
.fallback{color:var(--muted);display:flex;align-items:center;justify-content:center;height:100%;font-size:.85rem}
@media(max-width:850px){.kpis,.panels{grid-template-columns:repeat(2,1fr)}}
@media(max-width:540px){.kpis,.panels{grid-template-columns:1fr}header{align-items:flex-start;flex-direction:column;gap:12px}}
</style>__CUSTOM_HEAD__</head><body><main>
<!-- Fiber v3 metric set: uptime, CPU, RAM, client latency, TCP connections,
     cumulative/request-delta counts, and goroutines (process threads here). -->
<header><div><div class="label">Live process telemetry</div><h1>__TITLE__</h1></div><div id="status" class="status">Connecting…</div></header>
<section class="kpis">
<div class="card" style="--accent:var(--cyan)"><div class="label">Total requests</div><div id="requests" class="value">0</div><div id="reqRate" class="detail">+0 since last sample</div></div>
<div class="card" style="--accent:var(--green)"><div class="label">Uptime</div><div id="uptime" class="value">0s</div><div class="detail">process runtime</div></div>
<div class="card" style="--accent:var(--indigo)"><div class="label">Threads</div><div id="threads" class="value">0</div><div class="detail">process tasks</div></div>
<div class="card" style="--accent:var(--amber)"><div class="label">System load</div><div id="load" class="value">0.00</div><div class="detail">1-minute average</div></div>
</section><section class="panels">
<div class="panel" style="--accent:var(--cyan)"><div class="head"><span class="metric">CPU Usage</span><span class="big"><span id="cpuVal">0.0%</span><span class="sub" id="cpuSub">OS 0.0%</span></span></div><div class="canvas-wrap"><canvas id="cpu"></canvas></div></div>
<div class="panel" style="--accent:var(--violet)"><div class="head"><span class="metric">Memory Usage</span><span class="big"><span id="memVal">0 B</span><span class="sub" id="memSub"></span></span></div><div class="canvas-wrap"><canvas id="memory"></canvas></div></div>
<div class="panel" style="--accent:var(--amber)"><div class="head"><span class="metric">Response Time</span><span class="big"><span id="rtVal">0 ms</span><span class="sub">client</span></span></div><div class="canvas-wrap"><canvas id="latency"></canvas></div></div>
<div class="panel" style="--accent:var(--teal)"><div class="head"><span class="metric">Open Connections</span><span class="big"><span id="connVal">0</span><span class="sub" id="connSub">OS 0</span></span></div><div class="canvas-wrap"><canvas id="connections"></canvas></div></div>
<div class="panel" style="--accent:var(--pink)"><div class="head"><span class="metric">Requests / sample</span><span class="big"><span id="rateVal">+0</span><span class="sub" id="rateSub">0 total</span></span></div><div class="canvas-wrap"><canvas id="requestRate"></canvas></div></div>
<div class="panel" style="--accent:var(--indigo)"><div class="head"><span class="metric">Threads</span><span class="big"><span id="thVal">0</span><span class="sub">process tasks</span></span></div><div class="canvas-wrap"><canvas id="threadChart"></canvas></div></div>
</section></main><script>
(() => {
  // Keep Fiber v3's polling cadence and rolling history semantics.
  const limit=Number("__HISTORY_POINTS__"),pollMs=Number("__POLL_MS__"),labels=[];
  const values={pcpu:[],ocpu:[],pram:[],oram:[],ototal:[],latency:[],pconns:[],oconns:[],rate:[],threads:[]};
  let previous=null;
  // Fiber v3 renders memory values with binary (1024-based) units.
  function formatBytes(bytes){
    bytes=Number(bytes)||0;if(bytes===0)return"0 B";
    const k=1024,sizes=["B","KB","MB","GB","TB","PB"],i=Math.floor(Math.log(bytes)/Math.log(k));
    return parseFloat((bytes/Math.pow(k,i)).toFixed(1))+" "+sizes[i];
  }
  const options={
    responsive:true,maintainAspectRatio:false,animation:{duration:0},
    elements:{line:{tension:.3}},
    legend:{display:true,position:"top",align:"end",labels:{fontColor:"#9aa7c2",boxWidth:10,fontSize:11,usePointStyle:true,padding:12}},
    tooltips:{mode:"index",intersect:false,backgroundColor:"rgba(8,12,24,.92)",titleFontColor:"#eef2fb",bodyFontColor:"#c7d2e6",borderColor:"rgba(148,163,194,.2)",borderWidth:1,cornerRadius:8,caretPadding:6},
    hover:{mode:"index",intersect:false},
    scales:{xAxes:[{type:"time",time:{unit:"second",unitStepSize:30,displayFormats:{second:"HH:mm:ss"}},ticks:{fontColor:"#65718e",maxTicksLimit:6,fontSize:10},gridLines:{display:false,drawBorder:false}}],yAxes:[{ticks:{beginAtZero:true,fontColor:"#7c89a8",maxTicksLimit:5,padding:8,fontSize:11},gridLines:{color:"rgba(148,163,194,.08)",zeroLineColor:"rgba(148,163,194,.12)",drawBorder:false}}]}
  };
  function fill(ctx,color){const g=ctx.createLinearGradient(0,0,0,230);g.addColorStop(0,color+"59");g.addColorStop(1,color+"00");return g}
  function chart(id,names,keys,colors){
    const el=document.getElementById(id);
    if(!window.Chart){const node=document.createElement("div");node.className="fallback";node.textContent="Chart.js is unavailable.";el.parentNode.replaceChild(node,el);return null}
    const ctx=el.getContext("2d");
    return new Chart(ctx,{type:"line",data:{labels,datasets:names.map((name,i)=>({label:name,data:values[keys[i]],borderColor:colors[i],backgroundColor:fill(ctx,colors[i]),fill:true,pointRadius:0,pointHoverRadius:4,pointHoverBackgroundColor:colors[i],pointHoverBorderColor:"#fff",pointHoverBorderWidth:1,borderWidth:2}))},options})
  }
  const charts=[
    chart("cpu",["Process","System"],["pcpu","ocpu"],["#38bdf8","#34d399"]),
    chart("memory",["Process","OS used","OS total"],["pram","oram","ototal"],["#a78bfa","#fbbf24","#34d399"]),
    chart("latency",["Client ms"],["latency"],["#fbbf24"]),
    chart("connections",["Process","System"],["pconns","oconns"],["#22d3ee","#34d399"]),
    chart("requestRate",["Requests"],["rate"],["#f472b6"]),
    chart("threadChart",["Threads"],["threads"],["#818cf8"])
  ].filter(Boolean);
  const push=(array,value)=>{array.push(Number(value)||0);if(array.length>limit)array.shift()};
  const duration=value=>{let s=Number(value)||0,d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);return`${d?d+"d ":""}${h?h+"h ":""}${m?m+"m ":""}${s%60}s`};
  const setText=(id,text)=>{document.getElementById(id).textContent=text};
  const setStatus=(text,down)=>{const node=document.getElementById("status");node.textContent=text;node.classList.toggle("down",!!down)};
  // Requests are encoded as a decimal string by the server. BigInt preserves
  // uint64 precision; the chart delta is clamped before converting to Number.
  function requestDelta(current){
    if(previous===null)return 0;
    let delta=current-previous;
    if(delta<0n)delta=0n;
    const max=BigInt(Number.MAX_SAFE_INTEGER);
    if(delta>max)delta=max;
    return Number(delta);
  }
  async function sample(){
    const started=performance.now();
    try{
      const response=await fetch(location.href,{headers:{Accept:"application/json"},cache:"no-store"});
      if(!response.ok)throw new Error(`HTTP ${response.status}`);
      const data=await response.json(),rtime=Math.round(performance.now()-started);
      const requestText=String(data.pid.requests),requests=BigInt(requestText);
      const rate=requestDelta(requests);
      labels.push(Date.now());if(labels.length>limit)labels.shift();
      push(values.pcpu,data.pid.cpu);push(values.ocpu,data.os.cpu);
      push(values.pram,data.pid.ram/1e6);push(values.oram,data.os.ram/1e6);push(values.ototal,data.os.total_ram/1e6);
      push(values.latency,rtime);push(values.pconns,data.pid.conns);push(values.oconns,data.os.conns);
      push(values.rate,rate);push(values.threads,data.pid.goroutines);previous=requests;
      setText("requests",requests.toLocaleString());
      setText("reqRate",`+${rate.toLocaleString()} since last sample`);
      setText("uptime",duration(data.pid.uptime));
      setText("threads",Number(data.pid.goroutines).toLocaleString());
      setText("load",Number(data.os.load_avg).toFixed(2));
      setText("cpuVal",`${Number(data.pid.cpu).toFixed(1)}%`);
      setText("cpuSub",`OS ${Number(data.os.cpu).toFixed(1)}%`);
      setText("memVal",formatBytes(data.pid.ram));
      document.getElementById("memSub").innerHTML=`/ <span style="color:var(--amber)">${formatBytes(data.os.ram)}</span> / <span style="color:var(--green)">${formatBytes(data.os.total_ram)}</span>`;
      setText("rtVal",`${rtime} ms`);
      setText("connVal",Number(data.pid.conns).toLocaleString());
      setText("connSub",`OS ${Number(data.os.conns).toLocaleString()}`);
      setText("rateVal",`+${rate.toLocaleString()}`);
      setText("rateSub",`${requests.toLocaleString()} total`);
      setText("thVal",Number(data.pid.goroutines).toLocaleString());
      setStatus(`Updated ${new Date().toLocaleTimeString()}`,false);charts.forEach(item=>item.update())
    }catch(error){setStatus(`Unavailable: ${error.message}`,true)}
    finally{setTimeout(sample,pollMs)}
  }sample()
})();
</script></body></html>
"##;
