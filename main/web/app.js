// ═════════════════════════════════════════════════════════
// JINNI GRID — Mission Control
// ═════════════════════════════════════════════════════════

const state = {
  workers: [],
  activePage: "home",
  feedItems: [],
  chartWorker: null,
  chart: null,
  candleSeries: null,
  chartBars: [],          // cached bars in lightweight-charts format
  chartMarkers: [],
  ws: null,
  liveTrades: [],
};

const $  = sel => document.querySelector(sel);
const $$ = sel => Array.from(document.querySelectorAll(sel));

// ─── api ──────────────────────────────────────────────────
const api = {
  get:  u    => fetch(u).then(r => r.json()),
  post: u    => fetch(u, {method:"POST"}).then(r => r.json()),
  put:  (u,b)=> fetch(u, {method:"PUT", headers:{"Content-Type":"application/json"},
                          body: JSON.stringify(b)}).then(r => r.json()),
};

// ─── format ──────────────────────────────────────────────
const fmt = {
  money: v => v == null ? "—" : (v >= 0 ? "+" : "") + "$" + Number(v).toFixed(2),
  num:   (v, d=2) => v == null ? "—" : Number(v).toFixed(d),
  pct:   v => v == null ? "—" : v.toFixed(1) + "%",
  time:  v => v ? new Date(v).toLocaleTimeString("en-GB", {hour12:false}) : "—",
  short: v => v ? new Date(v).toLocaleTimeString("en-GB", {hour12:false}).slice(0,8) : "—",
};

// ═════════════════════════════════════════════════════════
// NAV
// ═════════════════════════════════════════════════════════
$$(".nav-item").forEach(btn => {
  btn.onclick = () => navigate(btn.dataset.page);
});

function navigate(page) {
  state.activePage = page;
  $$(".nav-item").forEach(b => b.classList.toggle("active", b.dataset.page === page));
  $$(".page").forEach(p => p.classList.toggle("active", p.dataset.page === page));
  if (page === "fleet")   loadFleet();
  if (page === "trades")  loadTrades();
  if (page === "logs")    loadLogs();
  if (page === "config")  loadConfigList();
  if (page === "charts")  initChartsPage();
}

// ═════════════════════════════════════════════════════════
// CLOCK
// ═════════════════════════════════════════════════════════
setInterval(() => {
  const d = new Date();
  $("#clock").textContent = d.toUTCString().slice(17, 25) + " UTC";
}, 1000);

// ═════════════════════════════════════════════════════════
// WEBSOCKET
// ═════════════════════════════════════════════════════════
function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws/ui`);
  state.ws = ws;

  ws.onopen = () => {
    $("#conn-pill").classList.remove("off");
    $("#conn-text").textContent = "live";
  };
  ws.onclose = () => {
    $("#conn-pill").classList.add("off");
    $("#conn-text").textContent = "reconnecting";
    setTimeout(connectWS, 2000);
  };
  ws.onmessage = e => handleWSMessage(JSON.parse(e.data));
}

function handleWSMessage(msg) {
  const t = msg.type;

  if (t === "heartbeat" || t === "worker.update") {
    loadFleet({silent: true});
    if (state.activePage === "home") loadHome();
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) {
      updateChartSideFromHeartbeat(msg.payload);
    }
  }

  if (t === "trade.opened") {
    pushFeed("trade-opened", msg.worker_id,
      `OPEN ${msg.payload.dir === 1 ? "▲ LONG" : "▼ SHORT"} ${msg.payload.symbol} @ ${fmt.num(msg.payload.actual_entry)}`);
    if (state.activePage === "home") loadHome();
    if (state.activePage === "trades") loadTrades();
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) {
      addChartMarker(msg.payload, "open");
      pushFill(msg.payload, "open");
    }
    toast("info", `Trade opened on ${msg.worker_id}`);
  }

  if (t === "trade.closed") {
    const p = msg.payload;
    const cls = (p.net_pnl || 0) >= 0 ? "trade-closed-win" : "trade-closed-loss";
    pushFeed(cls, msg.worker_id,
      `CLOSE ${p.symbol} · ${fmt.money(p.net_pnl)} · ${p.hit_sl ? "SL" : "TP"}`);
    if (state.activePage === "home")   loadHome();
    if (state.activePage === "trades") loadTrades();
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) {
      addChartMarker(p, "close");
      pushFill(p, "close");
    }
    toast(p.net_pnl >= 0 ? "success" : "error",
          `${msg.worker_id}: ${fmt.money(p.net_pnl)}`);
  }

  if (t === "log") {
    if (state.activePage === "logs") appendLogLive(msg);
  }

  if (t === "error") {
    pushFeed("error", msg.worker_id, msg.payload.message);
    toast("error", `[${msg.worker_id}] ${msg.payload.message}`);
  }

  if (t === "bar") {
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) {
      addChartBar(msg.payload.bar);
      updateChartSideFromBar(msg.payload);
    }
  }
}

// ═════════════════════════════════════════════════════════
// FEED
// ═════════════════════════════════════════════════════════
function pushFeed(cls, worker, msg) {
  const item = { cls, worker, msg, ts: new Date() };
  state.feedItems.unshift(item);
  state.feedItems = state.feedItems.slice(0, 80);
  renderFeed();
}
function renderFeed() {
  const el = $("#activity-feed");
  if (!el) return;
  el.innerHTML = state.feedItems.map(i => `
    <div class="feed-item ${i.cls}">
      <span class="feed-time">${fmt.short(i.ts)}</span>
      <div class="feed-body">
        <span class="feed-worker">${i.worker || "—"}</span>${escapeHtml(i.msg)}
      </div>
    </div>`).join("");
}
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}

// ═════════════════════════════════════════════════════════
// TOASTS
// ═════════════════════════════════════════════════════════
function toast(kind, msg) {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = msg;
  $("#toast-stack").appendChild(el);
  setTimeout(() => { el.style.opacity = "0"; el.style.transform = "translateX(20px)"; }, 3200);
  setTimeout(() => el.remove(), 3600);
}

// ═════════════════════════════════════════════════════════
// HOME / MISSION CONTROL
// ═════════════════════════════════════════════════════════
let equityChart = null, equitySeries = null;

async function loadHome() {
  const [workers, portfolio, equity] = await Promise.all([
    api.get("/api/workers"), api.get("/api/portfolio"), api.get("/api/equity"),
  ]);
  state.workers = workers;

  // KPIs
  const pnlEl = $("#kpi-pnl").querySelector(".kpi-value");
  pnlEl.textContent = fmt.money(portfolio.net_pnl);
  pnlEl.classList.toggle("positive", portfolio.net_pnl > 0);
  pnlEl.classList.toggle("negative", portfolio.net_pnl < 0);
  $("#kpi-wr").querySelector(".kpi-value").textContent = fmt.pct(portfolio.win_rate);
  $("#kpi-wl").textContent = `${portfolio.wins} W / ${portfolio.losses} L`;

  const running = workers.filter(w => w.state === "RUNNING").length;
  $("#kpi-fleet").querySelector(".kpi-value").textContent = `${running} / ${workers.length}`;
  const positions = workers.reduce((s,w) => s + (w.open_positions || 0), 0);
  $("#kpi-positions").querySelector(".kpi-value").textContent = positions;

  // Fleet pulse
  $("#fleet-pulse-sub").textContent = `${running} active · ${workers.length} total`;
  $("#fleet-pulse").innerHTML = workers.map(w => `
    <div class="pulse-card">
      <span class="pulse-state ${w.state}"></span>
      <div class="pulse-info">
        <div class="pulse-id">${w.id}</div>
        <div class="pulse-meta">${w.state} · ${w.connected ? "🔗" : "—"} · ${w.broker || "—"}</div>
      </div>
      <div class="pulse-balance">${w.last_balance != null ? "$" + Number(w.last_balance).toFixed(0) : "—"}</div>
    </div>`).join("") || `<div style="padding:20px;color:var(--text-3);font-size:12px">no workers registered</div>`;

  renderEquity(equity);
}

function renderEquity(equity) {
  const el = $("#equity-chart");
  if (!el) return;
  if (!equityChart) {
    equityChart = LightweightCharts.createChart(el, equityChartOpts(el));
    equitySeries = equityChart.addAreaSeries({
      lineColor: "rgba(0, 217, 255, 1)",
      topColor:  "rgba(0, 217, 255, 0.35)",
      bottomColor: "rgba(0, 217, 255, 0.02)",
      lineWidth: 2,
    });
    new ResizeObserver(() => equityChart.resize(el.clientWidth, el.clientHeight)).observe(el);
  }
  const data = equity.map((e, i) => ({
    time: Math.floor(new Date(e.ts || Date.now()).getTime() / 1000) + i,  // ensure strictly increasing
    value: e.cum,
  }));
  equitySeries.setData(data);
  if (data.length) equityChart.timeScale().fitContent();
}
function equityChartOpts(el) {
  return {
    width: el.clientWidth, height: el.clientHeight,
    layout: { background: { color: "transparent" }, textColor: "#8b95ad", fontFamily: "JetBrains Mono" },
    grid: { vertLines: { color: "rgba(255,255,255,0.04)" }, horzLines: { color: "rgba(255,255,255,0.04)" } },
    rightPriceScale: { borderColor: "rgba(255,255,255,0.08)" },
    timeScale:       { borderColor: "rgba(255,255,255,0.08)", timeVisible: true, secondsVisible: false },
    crosshair: { mode: 1 },
  };
}

// ═════════════════════════════════════════════════════════
// FLEET PAGE
// ═════════════════════════════════════════════════════════
async function loadFleet(opts = {}) {
  const workers = await api.get("/api/workers");
  state.workers = workers;
  const grid = $("#fleet-grid");
  if (!grid) return;
  grid.innerHTML = workers.map(w => `
    <div class="worker-card">
      <div class="worker-card-head">
        <div class="worker-card-id">
          <span class="pulse-state ${w.state}" style="width:12px;height:12px;"></span>
          ${w.id}
        </div>
        <span class="worker-state-badge ${w.state}">${w.state}</span>
      </div>
      <div class="worker-card-stats">
        <div class="stat-cell"><label>broker</label><b>${w.broker || "—"}</b></div>
        <div class="stat-cell"><label>account</label><b>${w.account || "—"}</b></div>
        <div class="stat-cell"><label>balance</label><b>${w.last_balance != null ? "$"+Number(w.last_balance).toFixed(2) : "—"}</b></div>
        <div class="stat-cell"><label>equity</label><b>${w.last_equity  != null ? "$"+Number(w.last_equity).toFixed(2)  : "—"}</b></div>
        <div class="stat-cell"><label>positions</label><b>${w.open_positions ?? 0}</b></div>
        <div class="stat-cell"><label>bars in mem</label><b>${w.mem_bars ?? 0}</b></div>
        <div class="stat-cell"><label>last hb</label><b>${w.last_heartbeat ? fmt.short(w.last_heartbeat) : "—"}</b></div>
        <div class="stat-cell"><label>version</label><b>${w.version || "—"}</b></div>
      </div>
      <div class="worker-card-actions">
        <button onclick="cmd('${w.id}','start')">start</button>
        <button onclick="cmd('${w.id}','stop')">stop</button>
        <button onclick="cmd('${w.id}','restart')">restart</button>
        <button onclick="cmd('${w.id}','reload_config')">↻ cfg</button>
        <button onclick="cmd('${w.id}','ping')">ping</button>
      </div>
    </div>`).join("") || `<div style="padding:32px;color:var(--text-3)">no workers — add a config in main/configs/&lt;worker_id&gt;.json</div>`;
}
async function cmd(id, action) {
  const r = await api.post(`/api/workers/${id}/${action}`);
  toast(r.ok ? "success" : "error", `${id} · ${action} → ${r.msg || (r.ok ? "ok" : "fail")}`);
  loadFleet();
}

// ═════════════════════════════════════════════════════════
// TRADES PAGE
// ═════════════════════════════════════════════════════════
async function loadTrades() {
  const wid = $("#trade-filter-worker").value.trim();
  const st  = $("#trade-filter-status").value;
  const qs = new URLSearchParams({limit: 200});
  if (wid) qs.set("worker_id", wid);
  if (st)  qs.set("status", st);
  const rows = await api.get("/api/trades?" + qs);
  const tb = $("#trades-table tbody");
  tb.innerHTML = rows.map(t => `
    <tr class="${(t.net_pnl||0)>0?"win":(t.net_pnl||0)<0?"loss":""}">
      <td>${t.id}</td>
      <td><span style="color:var(--accent)">${t.worker_id}</span></td>
      <td>${t.ticket || "—"}</td>
      <td>${t.symbol || ""}</td>
      <td class="${t.direction===1?"dir-long":"dir-short"}">${t.direction===1?"▲ LONG":t.direction===-1?"▼ SHORT":"—"}</td>
      <td>${t.status}</td>
      <td>${fmt.num(t.actual_entry)}</td>
      <td>${fmt.num(t.actual_exit)}</td>
      <td>${fmt.num(t.lots)}</td>
      <td class="pnl">${t.net_pnl != null ? fmt.money(t.net_pnl) : "—"}</td>
      <td>${t.r_multiple != null ? t.r_multiple.toFixed(2)+"R" : "—"}</td>
      <td>${verdictHtml(t.validator_verdict)}</td>
    </tr>`).join("");
}
function verdictHtml(v) {
  if (!v) return "—";
  if (v.match) return `<span class="verdict-ok">✓ match</span>`;
  return `<span class="verdict-bad" title="${escapeHtml(v.reason||"")}">⚠ ${(v.reason||"").slice(0,30)}</span>`;
}
$("#trade-filter-worker")?.addEventListener("input", () => clearTimeout(window._tf) || (window._tf = setTimeout(loadTrades, 300)));
$("#trade-filter-status")?.addEventListener("change", loadTrades);

// ═════════════════════════════════════════════════════════
// CHARTS PAGE
// ═════════════════════════════════════════════════════════
async function initChartsPage() {
  const workers = await api.get("/api/workers");
  state.workers = workers;
  const sel = $("#chart-worker");
  sel.innerHTML = workers.map(w => `<option value="${w.id}">${w.id} · ${w.state}</option>`).join("");
  sel.onchange = () => rebindChart();
  if (!state.chartWorker && workers.length) state.chartWorker = workers[0].id;
  if (state.chartWorker) {
    sel.value = state.chartWorker;
    rebindChart();
  }
}

async function rebindChart() {
  const wid = $("#chart-worker").value;
  state.chartWorker = wid;
  state.chartBars = []; state.chartMarkers = []; state.liveTrades = [];
  $("#chart-fills").innerHTML = "";

  const el = $("#live-chart");
  if (!state.chart) {
    state.chart = LightweightCharts.createChart(el, equityChartOpts(el));
    state.candleSeries = state.chart.addCandlestickSeries({
      upColor: "#00ffae", downColor: "#ff3b6b",
      wickUpColor: "#00ffae", wickDownColor: "#ff3b6b",
      borderVisible: false,
    });
    new ResizeObserver(() => state.chart.resize(el.clientWidth, el.clientHeight)).observe(el);
  }

  const { bars, markers } = await api.get(`/api/bars/${wid}`);
  state.chartBars   = bars.map(toCandle);
  state.candleSeries.setData(state.chartBars);
  const lwMarkers = markers.map(toMarker).filter(Boolean);
  state.chartMarkers = lwMarkers;
  state.candleSeries.setMarkers(lwMarkers);
  state.chart.timeScale().fitContent();

  // initial side-panel data
  const w = state.workers.find(x => x.id === wid);
  if (w) updateChartSideFromHeartbeat(w);
}

function toCandle(b) {
  return { time: b.time, open: b.open, high: b.high, low: b.low, close: b.close };
}
function toMarker(m) {
  if (!m.ts) return null;
  return {
    time: Math.floor(new Date(m.ts).getTime() / 1000),
    position: m.type === "open" ? (m.dir === 1 ? "belowBar" : "aboveBar") : "inBar",
    color:    m.type === "open" ? "#00d9ff" : ((m.net_pnl || 0) >= 0 ? "#00ffae" : "#ff3b6b"),
    shape:    m.type === "open" ? (m.dir === 1 ? "arrowUp" : "arrowDown") : "circle",
    text:     m.type === "open" ? `#${m.ticket}` : `${(m.net_pnl||0)>=0?"+":""}${Number(m.net_pnl||0).toFixed(0)}`,
  };
}

function addChartBar(bar) {
  if (!state.candleSeries) return;
  state.candleSeries.update(toCandle(bar));
}
function addChartMarker(trade, type) {
  if (!state.candleSeries) return;
  const m = toMarker({
    ts:    type === "open" ? trade.entry_time : trade.exit_time,
    type, dir: trade.dir,
    price: type === "open" ? trade.actual_entry : trade.actual_exit,
    net_pnl: trade.net_pnl, ticket: trade.ticket,
  });
  if (!m) return;
  state.chartMarkers.push(m);
  state.candleSeries.setMarkers(state.chartMarkers.slice(-100));
}
function updateChartSideFromBar(p) {
  $("#chart-engine").textContent   = p.engine_state || "—";
  $("#chart-livebars").textContent = p.live_bars_seen ?? 0;
  $("#chart-lastts").textContent   = p.bar ? new Date(p.bar.time * 1000).toLocaleTimeString() : "—";
}
function updateChartSideFromHeartbeat(p) {
  $("#chart-balance").textContent = p.last_balance ?? p.balance ?? "—";
  $("#chart-equity").textContent  = p.last_equity  ?? p.equity  ?? "—";
  $("#chart-ticket").textContent  = p.open_ticket || "—";
  $("#chart-engine").textContent  = p.engine_state || p.state || "—";
  $("#chart-livebars").textContent = p.live_bars_seen ?? 0;
}
function pushFill(trade, kind) {
  const el = $("#chart-fills");
  if (!el) return;
  const isClose = kind === "close";
  const cls = isClose ? ((trade.net_pnl||0) >= 0 ? "win" : "loss") : "";
  const html = `
    <div class="fill ${cls}">
      <div class="fill-row"><b>${isClose ? "CLOSE" : "OPEN"} #${trade.ticket}</b>
        <span>${trade.dir===1?"LONG":"SHORT"}</span></div>
      <div class="fill-row">
        <span>${isClose ? fmt.num(trade.actual_exit) : fmt.num(trade.actual_entry)}</span>
        <b>${isClose ? fmt.money(trade.net_pnl) : ""}</b>
      </div>
    </div>`;
  el.insertAdjacentHTML("afterbegin", html);
}

// ═════════════════════════════════════════════════════════
// LOGS PAGE
// ═════════════════════════════════════════════════════════
async function loadLogs() {
  const w = $("#log-filter-worker").value.trim();
  const l = $("#log-filter-level").value;
  const qs = new URLSearchParams({limit: 400});
  if (w) qs.set("worker_id", w);
  if (l) qs.set("level", l);
  const rows = await api.get("/api/logs?" + qs);
  const el = $("#logs-out");
  el.innerHTML = rows.reverse().map(r => logLineHtml(r)).join("");
  if ($("#log-autoscroll").checked) el.scrollTop = el.scrollHeight;
}
function logLineHtml(r) {
  return `<div class="log-line">
    <span class="log-ts">${fmt.short(r.ts)}</span>
    <span class="log-level ${r.level}">${r.level}</span>
    <span class="log-worker">${r.worker_id || "—"}</span>
    <span class="log-msg">${escapeHtml(r.message || "")}</span>
  </div>`;
}
function appendLogLive(msg) {
  const filterW = $("#log-filter-worker").value.trim();
  const filterL = $("#log-filter-level").value;
  if (filterW && filterW !== msg.worker_id) return;
  if (filterL && filterL !== msg.payload.level) return;
  const el = $("#logs-out");
  el.insertAdjacentHTML("beforeend", logLineHtml({
    ts: new Date().toISOString(),
    level: msg.payload.level || "INFO",
    worker_id: msg.worker_id,
    message: msg.payload.message,
  }));
  if ($("#log-autoscroll").checked) el.scrollTop = el.scrollHeight;
}
$("#log-filter-worker")?.addEventListener("input", () => clearTimeout(window._lf) || (window._lf = setTimeout(loadLogs, 300)));
$("#log-filter-level")?.addEventListener("change", loadLogs);

// ═════════════════════════════════════════════════════════
// CONFIG PAGE
// ═════════════════════════════════════════════════════════
async function loadConfigList() {
  const ids = await api.get("/api/configs");
  const sel = $("#config-select");
  sel.innerHTML = ids.map(i => `<option value="${i}">${i}</option>`).join("");
  if (ids.length) loadConfig();
}
async function loadConfig() {
  const id = $("#config-select").value; if (!id) return;
  const cfg = await api.get(`/api/configs/${id}`);
  $("#config-editor").value = JSON.stringify(cfg, null, 2);
}
async function saveConfig() {
  const id = $("#config-select").value;
  let cfg;
  try { cfg = JSON.parse($("#config-editor").value); }
  catch (e) { toast("error", "invalid JSON: " + e.message); return; }
  const r = await api.put(`/api/configs/${id}`, cfg);
  toast(r.ok ? "success" : "error", r.ok ? `${id} saved & pushed` : "save failed");
}

// ═════════════════════════════════════════════════════════
// COMMAND PALETTE
// ═════════════════════════════════════════════════════════
const palette = $("#cmd-palette");
const cmdInput = $("#cmd-input");
const cmdResults = $("#cmd-results");
let cmdSel = 0;

const cmdActions = [
  {ico: "◉", label: "Go to Mission Control", sub: "home", fn: () => navigate("home")},
  {ico: "▦", label: "Go to Fleet",           sub: "fleet", fn: () => navigate("fleet")},
  {ico: "↯", label: "Go to Trades",          sub: "trades", fn: () => navigate("trades")},
  {ico: "▲", label: "Go to Live Charts",     sub: "charts", fn: () => navigate("charts")},
  {ico: "≡", label: "Go to Logs",            sub: "logs", fn: () => navigate("logs")},
  {ico: "◈", label: "Go to Configs",         sub: "config", fn: () => navigate("config")},
];

function openPalette() {
  palette.classList.remove("hidden");
  cmdInput.value = ""; cmdInput.focus(); renderCmd("");
}
function closePalette() { palette.classList.add("hidden"); }
$("#cmd-trigger").onclick = openPalette;
window.addEventListener("keydown", e => {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") { e.preventDefault(); openPalette(); }
  if (e.key === "Escape") closePalette();
  if (!palette.classList.contains("hidden")) {
    if (e.key === "ArrowDown") { cmdSel = Math.min(cmdSel + 1, currentResults().length - 1); renderCmd(cmdInput.value, false); e.preventDefault(); }
    if (e.key === "ArrowUp")   { cmdSel = Math.max(cmdSel - 1, 0); renderCmd(cmdInput.value, false); e.preventDefault(); }
    if (e.key === "Enter") {
      const r = currentResults()[cmdSel];
      if (r) { r.fn(); closePalette(); }
    }
  }
});
cmdInput.addEventListener("input", () => { cmdSel = 0; renderCmd(cmdInput.value); });
palette.addEventListener("click", e => { if (e.target === palette) closePalette(); });

function currentResults() {
  const q = cmdInput.value.trim().toLowerCase();
  const workerActions = state.workers.flatMap(w => ([
    {ico: "▶", label: `Start ${w.id}`,   sub: "worker action", fn: () => cmd(w.id, "start")},
    {ico: "■", label: `Stop ${w.id}`,    sub: "worker action", fn: () => cmd(w.id, "stop")},
    {ico: "⟳", label: `Restart ${w.id}`, sub: "worker action", fn: () => cmd(w.id, "restart")},
  ]));
  const all = [...cmdActions, ...workerActions];
  if (!q) return all.slice(0, 10);
  return all.filter(a => a.label.toLowerCase().includes(q) || a.sub.toLowerCase().includes(q)).slice(0, 12);
}
function renderCmd(_q, reset = true) {
  if (reset) cmdSel = 0;
  const results = currentResults();
  cmdResults.innerHTML = results.map((r, i) => `
    <div class="cmd-result ${i === cmdSel ? "sel" : ""}" data-i="${i}">
      <span class="ico">${r.ico}</span>
      <span class="lbl">${r.label}</span>
      <span class="sub">${r.sub}</span>
    </div>`).join("");
  cmdResults.querySelectorAll(".cmd-result").forEach(el => {
    el.onclick = () => { cmdSel = +el.dataset.i; results[cmdSel].fn(); closePalette(); };
  });
}

// ═════════════════════════════════════════════════════════
// BOOT
// ═════════════════════════════════════════════════════════
connectWS();
loadHome();
loadFleet({silent: true});
setInterval(() => { if (state.activePage === "home") loadHome(); }, 8000);