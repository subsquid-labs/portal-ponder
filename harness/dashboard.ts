export const DASHBOARD_HTML = String.raw`<!doctype html>
<html><head><meta charset="utf-8"><title>Portal Stress Dashboard</title>
<style>
 body{margin:0;background:#0b0e14;color:#cdd6f4;font:13px/1.4 ui-monospace,Menlo,monospace}
 header{padding:12px 18px;background:#11151f;border-bottom:1px solid #1f2430;display:flex;gap:24px;align-items:center;flex-wrap:wrap}
 h1{font-size:15px;margin:0;color:#89b4fa}
 .kpi{display:flex;flex-direction:column} .kpi b{font-size:18px;color:#a6e3a1} .kpi span{font-size:11px;color:#6c7086}
 .banner{padding:4px 12px;border-radius:6px;font-weight:bold}
 .ok{background:#1e3a1e;color:#a6e3a1} .bad{background:#3a1e1e;color:#f38ba8}
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;padding:14px}
 .card{background:#11151f;border:1px solid #1f2430;border-radius:8px;padding:10px}
 .card h3{margin:0 0 6px;font-size:12px;color:#bac2de;font-weight:600}
 canvas{width:100%;height:200px;display:block}
 .legend{display:flex;gap:14px;font-size:11px;margin-top:4px;flex-wrap:wrap}
 .legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:4px;vertical-align:middle}
 table{font-size:11px;border-collapse:collapse;width:100%} td,th{padding:2px 8px;text-align:right;border-bottom:1px solid #1f2430} th{color:#6c7086}
</style></head>
<body>
<header>
 <h1>⚡ Portal Stress — multi-chain Euler</h1>
 <div id="banner" class="banner ok">HEALTHY</div>
 <div class="kpi"><b id="k_blk">–</b><span>blocks/s (now)</span></div>
 <div class="kpi"><b id="k_peak">–</b><span>peak blocks/s</span></div>
 <div class="kpi"><b id="k_mb">–</b><span>MB/s</span></div>
 <div class="kpi"><b id="k_req">–</b><span>req/s</span></div>
 <div class="kpi"><b id="k_conc">–</b><span>concurrency</span></div>
 <div class="kpi"><b id="k_p90">–</b><span>p90 latency</span></div>
 <div class="kpi"><b id="k_err">–</b><span>err/s</span></div>
 <div class="kpi"><b id="k_t">–</b><span>elapsed</span></div>
</header>
<div class="grid">
 <div class="card"><h3>Throughput — blocks/s (vs concurrency ramp)</h3><canvas id="c_thru"></canvas><div class="legend"><span><i style="background:#a6e3a1"></i>blocks/s</span><span><i style="background:#89b4fa"></i>concurrency</span></div></div>
 <div class="card"><h3>Requests/s & Errors/s — WHEN it breaks</h3><canvas id="c_req"></canvas><div class="legend"><span><i style="background:#94e2d5"></i>req/s ok</span><span><i style="background:#f38ba8"></i>err/s (503/529/429)</span></div></div>
 <div class="card"><h3>Latency (ms)</h3><canvas id="c_lat"></canvas><div class="legend"><span><i style="background:#a6e3a1"></i>p50</span><span><i style="background:#f9e2af"></i>p90</span><span><i style="background:#f38ba8"></i>p99</span></div></div>
 <div class="card"><h3>Per-chain blocks/s</h3><canvas id="c_chain"></canvas><div class="legend" id="chainLegend"></div></div>
 <div class="card"><h3>MB/s</h3><canvas id="c_mb"></canvas></div>
 <div class="card"><h3>HTTP status (cumulative) & chains</h3><div id="statusTbl"></div></div>
</div>
<script>
const COLORS=['#89b4fa','#f9e2af','#cba6f7','#fab387','#94e2d5','#f38ba8'];
function draw(id,lines,opts){opts=opts||{};const cv=document.getElementById(id);const dpr=devicePixelRatio||1;const W=cv.clientWidth,H=cv.clientHeight;cv.width=W*dpr;cv.height=H*dpr;const x=cv.getContext('2d');x.scale(dpr,dpr);x.clearRect(0,0,W,H);
 const pad={l:46,r:opts.r2?46:10,t:8,b:18};const all=lines.flatMap(l=>l.pts);if(!all.length)return;
 const xs=all.map(p=>p[0]),maxX=Math.max(...xs,1),minX=Math.min(...xs,0);
 const sx=v=>pad.l+(v-minX)/(maxX-minX||1)*(W-pad.l-pad.r);
 function axisY(vals){const mx=Math.max(...vals,1);return {mx,sy:v=>H-pad.b-(v/(mx||1))*(H-pad.t-pad.b)}}
 const prim=lines.filter(l=>!l.r2),sec=lines.filter(l=>l.r2);
 const ay=axisY(prim.flatMap(l=>l.pts.map(p=>p[1])));
 x.strokeStyle='#1f2430';x.fillStyle='#6c7086';x.font='10px monospace';
 for(let i=0;i<=4;i++){const yy=pad.t+i*(H-pad.t-pad.b)/4;x.beginPath();x.moveTo(pad.l,yy);x.lineTo(W-pad.r,yy);x.stroke();x.fillText(fmt(ay.mx*(1-i/4)),2,yy+3)}
 for(const l of prim){x.strokeStyle=l.color;x.lineWidth=1.5;x.beginPath();l.pts.forEach((p,i)=>{const X=sx(p[0]),Y=ay.sy(p[1]);i?x.lineTo(X,Y):x.moveTo(X,Y)});x.stroke()}
 if(sec.length){const ay2=axisY(sec.flatMap(l=>l.pts.map(p=>p[1])));for(const l of sec){x.strokeStyle=l.color;x.lineWidth=1.2;x.setLineDash([4,3]);x.beginPath();l.pts.forEach((p,i)=>{const X=sx(p[0]),Y=ay2.sy(p[1]);i?x.lineTo(X,Y):x.moveTo(X,Y)});x.stroke();x.setLineDash([])}x.fillStyle='#89b4fa';for(let i=0;i<=4;i++){const yy=pad.t+i*(H-pad.t-pad.b)/4;x.fillText(fmt(ay2.mx*(1-i/4)),W-pad.r+2,yy+3)}}
 if(opts.brk!=null){const X=sx(opts.brk);x.strokeStyle='#f38ba8';x.setLineDash([3,3]);x.beginPath();x.moveTo(X,pad.t);x.lineTo(X,H-pad.b);x.stroke();x.setLineDash([])}
}
function fmt(n){n=+n;if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=1e3)return(n/1e3).toFixed(0)+'k';return(n%1?n.toFixed(1):n)}
async function tick(){let d;try{d=await(await fetch('/data')).json()}catch(e){return}const s=d.series;const bn=document.getElementById('banner');if(!s.length){bn.className='banner ok';bn.textContent='⏳ setting up (discovering vaults / cold-start)…';return}const last=s[s.length-1];const peak=s.reduce((a,p)=>p.blocksPerSec>a.blocksPerSec?p:a,s[0]);
 document.getElementById('k_blk').textContent=fmt(last.blocksPerSec);
 document.getElementById('k_peak').textContent=fmt(peak.blocksPerSec);
 document.getElementById('k_mb').textContent=last.mbPerSec;
 document.getElementById('k_req').textContent=last.reqPerSec;
 document.getElementById('k_conc').textContent=last.concurrency;
 document.getElementById('k_p90').textContent=last.p90+'ms';
 document.getElementById('k_err').textContent=last.errPerSec;
 document.getElementById('k_t').textContent=Math.floor(last.t/60)+'m'+(last.t%60)+'s';
 const b=document.getElementById('banner');if(d.breakingAt!=null){b.className='banner bad';b.textContent='⚠ BREAKING at T+'+d.breakingAt+'s';}else{b.className='banner ok';b.textContent='HEALTHY';}
 const X=p=>p.t;
 draw('c_thru',[{color:'#a6e3a1',pts:s.map(p=>[X(p),p.blocksPerSec])},{color:'#89b4fa',r2:1,pts:s.map(p=>[X(p),p.concurrency])}],{r2:1,brk:d.breakingAt});
 draw('c_req',[{color:'#94e2d5',pts:s.map(p=>[X(p),p.okPerSec])},{color:'#f38ba8',pts:s.map(p=>[X(p),p.errPerSec])}],{brk:d.breakingAt});
 draw('c_lat',[{color:'#a6e3a1',pts:s.map(p=>[X(p),p.p50])},{color:'#f9e2af',pts:s.map(p=>[X(p),p.p90])},{color:'#f38ba8',pts:s.map(p=>[X(p),p.p99])}],{brk:d.breakingAt});
 const names=Object.keys(last.chains||{});
 draw('c_chain',names.map((n,i)=>({color:COLORS[i%COLORS.length],pts:s.map(p=>[X(p),(p.chains[n]||{}).blocksPerSec||0])})),{brk:d.breakingAt});
 document.getElementById('chainLegend').innerHTML=names.map((n,i)=>'<span><i style="background:'+COLORS[i%COLORS.length]+'"></i>'+n+'</span>').join('');
 draw('c_mb',[{color:'#fab387',pts:s.map(p=>[X(p),p.mbPerSec])}],{brk:d.breakingAt});
 const st=last.status||{};
 let html='<table><tr><th>status</th>'+Object.keys(st).map(k=>'<th>'+k+'</th>').join('')+'</tr><tr><td>count</td>'+Object.values(st).map(v=>'<td>'+fmt(v)+'</td>').join('')+'</tr></table>';
 html+='<table style="margin-top:8px"><tr><th>chain</th><th>blk/s</th><th>req/s</th><th>err/s</th><th>vaults</th><th>windows</th></tr>'+(d.chains||[]).map(c=>{const cc=last.chains[c.name]||{};return '<tr><td style="text-align:left">'+c.name+'</td><td>'+fmt(cc.blocksPerSec||0)+'</td><td>'+(cc.reqPerSec||0)+'</td><td>'+(cc.errPerSec||0)+'</td><td>'+c.vaults+'</td><td>'+c.windowsDone+'/'+c.windows+'</td></tr>'}).join('')+'</table>';
 document.getElementById('statusTbl').innerHTML=html;
}
setInterval(tick,3000);tick();
</script></body></html>`;
