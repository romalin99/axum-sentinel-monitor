use crate::Config;

pub(crate) fn render(config: &Config) -> String {
    let poll_ms = config
        .refresh
        .as_millis()
        .saturating_sub(200)
        .max(200)
        .to_string();
    let font = asset("link", "href", &config.font_url);
    let chart = asset("script", "src", &config.chart_js_url);
    TEMPLATE
        .replace("__TITLE__", &escape_text(&config.title))
        .replace("__FONT__", &font)
        .replace("__CHART__", &chart)
        .replace("__POLL_MS__", &poll_ms)
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
:root{color-scheme:dark;--bg:#0b1020;--panel:#151c31;--line:#25314e;--text:#e8ecf7;--muted:#9aa7c2}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:Roboto,system-ui,sans-serif}
main{width:min(1360px,94vw);margin:30px auto}header{display:flex;align-items:end;justify-content:space-between;gap:18px;margin-bottom:18px}
h1{margin:0;font-size:clamp(1.5rem,3vw,2.4rem)}.muted,.label{color:var(--muted)}.label{font-size:.73rem;text-transform:uppercase;letter-spacing:.09em}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px}.card,.chart{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:16px}
.value{font-size:1.45rem;font-weight:900;margin-top:7px}.charts{display:grid;grid-template-columns:repeat(2,1fr);gap:12px}.chart{height:270px}.chart h2{font-size:.85rem;color:var(--muted);margin:0 0 10px}
canvas{width:100%!important;height:215px!important}.fallback{color:var(--muted)}@media(max-width:850px){.cards,.charts{grid-template-columns:repeat(2,1fr)}}@media(max-width:540px){.cards,.charts{grid-template-columns:1fr}header{align-items:start;flex-direction:column}}
</style>__CUSTOM_HEAD__</head><body><main>
<header><div><div class="label">Live process telemetry</div><h1>__TITLE__</h1></div><div id="status" class="muted">Connecting…</div></header>
<section class="cards">
<div class="card"><div class="label">Requests</div><div id="requests" class="value">0</div></div>
<div class="card"><div class="label">Uptime</div><div id="uptime" class="value">0s</div></div>
<div class="card"><div class="label">Threads</div><div id="threads" class="value">0</div></div>
<div class="card"><div class="label">System load</div><div id="load" class="value">0.00</div></div>
</section><section class="charts">
<div class="chart"><h2>CPU usage (%)</h2><canvas id="cpu"></canvas></div>
<div class="chart"><h2>Memory usage</h2><canvas id="memory"></canvas></div>
<div class="chart"><h2>Monitor response time (ms)</h2><canvas id="latency"></canvas></div>
<div class="chart"><h2>TCP connections</h2><canvas id="connections"></canvas></div>
<div class="chart"><h2>Requests per sample</h2><canvas id="requestRate"></canvas></div>
<div class="chart"><h2>Threads</h2><canvas id="threadChart"></canvas></div>
</section></main><script>
(() => {
  const limit=51,pollMs=Number("__POLL_MS__"),labels=[];
  const values={pcpu:[],ocpu:[],pram:[],oram:[],latency:[],pconns:[],oconns:[],rate:[],threads:[]};
  let previous=null;
  const options={responsive:true,maintainAspectRatio:false,animation:{duration:0},legend:{labels:{fontColor:"#9aa7c2"}},scales:{xAxes:[{display:false}],yAxes:[{ticks:{beginAtZero:true,fontColor:"#9aa7c2"},gridLines:{color:"#25314e"}}]}};
  function chart(id,names,keys){
    if(!window.Chart){const node=document.createElement("div");node.className="fallback";node.textContent="Chart.js is unavailable.";document.getElementById(id).replaceWith(node);return null}
    return new Chart(document.getElementById(id),{type:"line",data:{labels,datasets:names.map((name,i)=>({label:name,data:values[keys[i]],borderColor:["#38bdf8","#34d399"][i],backgroundColor:"transparent",pointRadius:0,borderWidth:2}))},options})
  }
  const charts=[
    chart("cpu",["Process","System"],["pcpu","ocpu"]),chart("memory",["Process MiB","System %"],["pram","oram"]),
    chart("latency",["Response"],["latency"]),chart("connections",["Process","System"],["pconns","oconns"]),
    chart("requestRate",["Requests"],["rate"]),chart("threadChart",["Threads"],["threads"])
  ].filter(Boolean);
  const push=(array,value)=>{array.push(Number(value)||0);if(array.length>limit)array.shift()};
  const duration=value=>{let s=Number(value)||0,d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);return`${d?d+"d ":""}${h?h+"h ":""}${m?m+"m ":""}${s%60}s`};
  async function sample(){
    const started=performance.now();
    try{
      const response=await fetch(location.href,{headers:{Accept:"application/json"},cache:"no-store"});
      if(!response.ok)throw new Error(`HTTP ${response.status}`);
      const data=await response.json(),elapsed=performance.now()-started,requestText=String(data.pid.requests),requests=BigInt(requestText);
      const difference=previous===null?0n:requests-previous,rate=Number(difference>BigInt(Number.MAX_SAFE_INTEGER)?BigInt(Number.MAX_SAFE_INTEGER):difference);
      labels.push(new Date().toLocaleTimeString());if(labels.length>limit)labels.shift();
      push(values.pcpu,data.pid.cpu);push(values.ocpu,data.os.cpu);push(values.pram,data.pid.ram/1048576);
      push(values.oram,data.os.total_ram?data.os.ram/data.os.total_ram*100:0);push(values.latency,elapsed);
      push(values.pconns,data.pid.conns);push(values.oconns,data.os.conns);push(values.rate,Math.max(0,rate));
      push(values.threads,data.pid.goroutines);previous=requests;
      document.getElementById("requests").textContent=requests.toLocaleString();document.getElementById("uptime").textContent=duration(data.pid.uptime);
      document.getElementById("threads").textContent=data.pid.goroutines;document.getElementById("load").textContent=Number(data.os.load_avg).toFixed(2);
      document.getElementById("status").textContent=`Updated ${new Date().toLocaleTimeString()}`;charts.forEach(item=>item.update())
    }catch(error){document.getElementById("status").textContent=`Unavailable: ${error.message}`}
    finally{setTimeout(sample,pollMs)}
  }sample()
})();
</script></body></html>
"##;
