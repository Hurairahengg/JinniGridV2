// ═══ JINNI GRID — Mission Control ═══

const state = {
  workers: [], activePage: "home", feedItems: [],
  chartWorker: null, chart: null, candleSeries: null,
  chartBars: [], chartMarkers: [], ws: null,
};

const $  = sel => document.querySelector(sel);
const $$ = sel => Array.from(document.querySelectorAll(sel));

const api = {
  get:  u    => fetch(u).then(r => r.json()),
  post: u    => fetch(u, {method:"POST"}).then(r => r.json()),
  put:  (u,b)=> fetch(u, {method:"PUT", headers:{"Content-Type":"application/json"},
                          body: JSON.stringify(b)}).then(r => r.json()),
};

const fmt = {
  money: v => v == null ? "—" : (v >= 0 ? "+" : "") + "$" + Number(v).toFixed(2),
  num:   (v,d=2) => v == null ? "—" : Number(v).toFixed(d),
  pct:   v => v == null ? "—" : Number(v).toFixed(1) + "%",
  time:  v => v ? new Date(v).toLocaleTimeString("en-GB", {hour12:false}) : "—",
  short: v => v ? new Date(v).toLocaleTimeString("en-GB", {hour12:false}).slice(0,8) : "—",
  big:   v => v == null ? "—" : (v >= 0 ? "+" : "") + "$" + Number(v).toLocaleString("en-US", {maximumFractionDigits: 2}),
};

// ═══ THEME ═══
const THEME_KEY = "jg-theme", ACCENT_KEY = "jg-accent", DENSITY_KEY = "jg-density";

function applyTheme(name)   { document.documentElement.setAttribute("data-theme", name);  localStorage.setItem(THEME_KEY, name);   refreshChartThemes(); }
function applyAccent(name)  { document.documentElement.setAttribute("data-accent", name); localStorage.setItem(ACCENT_KEY, name);  refreshChartThemes(); }
function applyDensity(name) { document.documentElement.setAttribute("data-density", name); localStorage.setItem(DENSITY_KEY, name); }

applyTheme  (localStorage.getItem(THEME_KEY)  || "midnight");
applyDensity(localStorage.getItem(DENSITY_KEY) || "normal");
const savedAccent = localStorage.getItem(ACCENT_KEY); if (savedAccent) applyAccent(savedAccent);

const themePanel = $("#theme-panel");
$("#theme-btn").onclick = e => { e.stopPropagation(); themePanel.classList.toggle("hidden"); markActiveTheme(); };
document.addEventListener("click", e => {
  if (!themePanel.contains(e.target) && e.target.id !== "theme-btn") themePanel.classList.add("hidden");
});
$$(".theme-swatch").forEach(b => b.onclick = () => { applyTheme(b.dataset.theme); markActiveTheme(); });
$$(".accent-dot")  .forEach(b => b.onclick = () => { applyAccent(b.dataset.accent); markActiveTheme(); });
$$(".density-btn") .forEach(b => b.onclick = () => { applyDensity(b.dataset.density); markActiveTheme(); });

function markActiveTheme() {
  const t = document.documentElement.dataset.theme;
  const a = document.documentElement.dataset.accent || "";
  const d = document.documentElement.dataset.density;
  $$(".theme-swatch").forEach(b => b.classList.toggle("active", b.dataset.theme === t));
  $$(".accent-dot") .forEach(b => b.classList.toggle("active", b.dataset.accent === a));
  $$(".density-btn").forEach(b => b.classList.toggle("active", b.dataset.density === d));
}

function refreshChartThemes() {
  setTimeout(() => {
    if (equityChart) equityChart.applyOptions(chartBaseOpts($("#equity-chart")));
    if (portEquityChart) portEquityChart.applyOptions(chartBaseOpts($("#port-equity")));
    if (portDdChart) portDdChart.applyOptions(chartBaseOpts($("#port-dd")));
    if (portHistChart) portHistChart.applyOptions(chartBaseOpts($("#port-hist")));
    if (state.chart) state.chart.applyOptions(chartBaseOpts($("#live-chart")));
  }, 30);
}

// ═══ NAV ═══
$$(".nav-item").forEach(btn => btn.onclick = () => navigate(btn.dataset.page));

function navigate(page) {
  state.activePage = page;
  $$(".nav-item").forEach(b => b.classList.toggle("active", b.dataset.page === page));
  $$(".page").forEach(p => p.classList.toggle("active", p.dataset.page === page));
  if (page === "home")      loadHome();
  if (page === "portfolio") loadPortfolio();
  if (page === "fleet")     loadFleet();
  if (page === "logs")      loadLogs();
  if (page === "config")    loadConfigList();
  if (page === "charts")    initChartsPage();
}

// ═══ CLOCK ═══
setInterval(() => { $("#clock").textContent = new Date().toUTCString().slice(17, 25) + " UTC"; }, 1000);

// ═══ WS ═══
function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws/ui`);
  state.ws = ws;
  ws.onopen  = () => { $("#conn-pill").classList.remove("off"); $("#conn-text").textContent = "live"; };
  ws.onclose = () => { $("#conn-pill").classList.add("off");    $("#conn-text").textContent = "reconnecting"; setTimeout(connectWS, 2000); };
  ws.onmessage = e => handleWS(JSON.parse(e.data));
}

function handleWS(msg) {
  const t = msg.type;
  if (t === "heartbeat" || t === "worker.update") {
    if (state.activePage === "home")  loadHome();
    if (state.activePage === "fleet") loadFleet({silent:true});
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) updateSideFromHB(msg.payload);
  }
  if (t === "trade.opened") {
    pushFeed("trade-opened", msg.worker_id, `OPEN ${msg.payload.dir === 1 ? "▲ LONG" : "▼ SHORT"} ${msg.payload.symbol} @ ${fmt.num(msg.payload.actual_entry)}`);
    if (state.activePage === "home")      loadHome();
    if (state.activePage === "portfolio") loadPortfolio();
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) { addChartMarker(msg.payload, "open"); pushFill(msg.payload, "open"); }
    toast("info", `${msg.worker_id}: trade opened`);
  }
  if (t === "trade.closed") {
    const p = msg.payload;
    const cls = (p.net_pnl || 0) >= 0 ? "trade-closed-win" : "trade-closed-loss";
    pushFeed(cls, msg.worker_id, `CLOSE ${p.symbol} · ${fmt.money(p.net_pnl)} · ${p.hit_sl ? "SL" : "TP"}`);
    if (state.activePage === "home")      loadHome();
    if (state.activePage === "portfolio") loadPortfolio();
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) { addChartMarker(p, "close"); pushFill(p, "close"); }
    toast(p.net_pnl >= 0 ? "success" : "error", `${msg.worker_id}: ${fmt.money(p.net_pnl)}`);
  }
  if (t === "log")   { if (state.activePage === "logs") appendLogLive(msg); }
  if (t === "error") { pushFeed("error", msg.worker_id, msg.payload.message); toast("error", `[${msg.worker_id}] ${msg.payload.message}`); }
  if (t === "bar")   { if (state.activePage === "charts" && msg.worker_id === state.chartWorker) { addChartBar(msg.payload.bar); updateSideFromBar(msg.payload); } }
}

// ═══ FEED ═══
function pushFeed(cls, worker, msg) {
  state.feedItems.unshift({cls, worker, msg, ts: new Date()});
  state.feedItems = state.feedItems.slice(0, 80);
  renderFeed();
}
function renderFeed() {
  const el = $("#activity-feed"); if (!el) return;
  el.innerHTML = state.feedItems.map(i => `
    <div class="feed-item ${i.cls}">
      <span class="feed-time">${fmt.short(i.ts)}</span>
      <div class="feed-body"><span class="feed-worker">${i.worker || "—"}</span>${escapeHtml(i.msg)}</div>
    </div>`).join("");
}
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}

// ═══ TOASTS ═══
function toast(kind, msg) {
  const el = document.createElement("div");
  el.className = `toast ${kind}`; el.textContent = msg;
  $("#toast-stack").appendChild(el);
  setTimeout(() => { el.style.opacity = "0"; el.style.transform = "translateX(20px)"; }, 3200);
  setTimeout(() => el.remove(), 3600);
}

// ═══ CHART OPTS (theme-aware) ═══
function cssVar(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }
function chartBaseOpts(el) {
  return {
    width: el ? el.clientWidth : 600,
    height: el ? el.clientHeight : 300,
    layout: { background: { color: "transparent" }, textColor: cssVar("--text-2"), fontFamily: "JetBrains Mono" },
    grid: { vertLines: { color: "color-mix(in srgb, " + cssVar("--text-2") + " 8%, transparent)" }, horzLines: { color: "color-mix(in srgb, " + cssVar("--text-2") + " 8%, transparent)" } },
    rightPriceScale: { borderColor: "color-mix(in srgb, " + cssVar("--text-2") + " 14%, transparent)" },
    timeScale:       { borderColor: "color-mix(in srgb, " + cssVar("--text-2") + " 14%, transparent)", timeVisible: true, secondsVisible: false },
    crosshair: { mode: 1 },
  };
}

// ═══ HOME / MISSION CONTROL (revamped) ═══
let equityChart = null, equitySeries = null;
let equityRange = "all";  // all | 100 | 50 | 20
let equityRaw = [];

async function loadHome() {
  const [workers, portfolio, equity, trades24] = await Promise.all([
    api.get("/api/workers"),
    api.get("/api/portfolio"),
    api.get("/api/equity"),
    api.get("/api/trades?status=closed&limit=200"),
  ]);
  state.workers = workers;
  equityRaw = equity;

  // ─── KPI: pnl ───
  const pnlEl = $("#kpi-pnl");
  const pnlVal = pnlEl.querySelector(".kc-value");
  pnlVal.textContent = fmt.money(portfolio.net_pnl);
  pnlVal.classList.toggle("positive", portfolio.net_pnl > 0);
  pnlVal.classList.toggle("negative", portfolio.net_pnl < 0);
  $("#kpi-trades").textContent = `${portfolio.n_trades} trades`;
  // mini spark gradient color
  $("#kpi-pnl-spark").style.background =
    portfolio.net_pnl >= 0
      ? "linear-gradient(180deg, transparent, color-mix(in srgb, var(--success) 18%, transparent))"
      : "linear-gradient(180deg, transparent, color-mix(in srgb, var(--danger) 18%, transparent))";

  // ─── KPI: win rate ───
  $("#kpi-wr").querySelector(".kc-value").textContent = fmt.pct(portfolio.win_rate);
  $("#kpi-wl").textContent = `${portfolio.wins}W / ${portfolio.losses}L`;
  $("#kpi-wr-bar").style.width = (portfolio.win_rate || 0) + "%";

  // ─── KPI: fleet ───
  const running = workers.filter(w => w.state === "RUNNING").length;
  $("#kpi-fleet").querySelector(".kc-value").textContent = `${running} / ${workers.length}`;
  $("#kpi-fleet-dots").innerHTML = workers.slice(0, 20).map(w =>
    `<span class="kc-dot ${w.state}" title="${w.id} · ${w.state}"></span>`
  ).join("");

  // ─── KPI: positions ───
  const positions = workers.reduce((s,w) => s + (w.open_positions || 0), 0);
  $("#kpi-positions").querySelector(".kc-value").textContent = positions;
  const openWorkers = workers.filter(w => (w.open_positions || 0) > 0).map(w => w.id);
  $("#kpi-pos-mini").textContent = openWorkers.length
    ? openWorkers.slice(0, 3).join(" · ") + (openWorkers.length > 3 ? ` +${openWorkers.length - 3}` : "")
    : "—";

  // ─── header quick stats ───
  const today = todaysPnl(trades24);
  const hdrToday = $("#hdr-today");
  hdrToday.textContent = fmt.money(today);
  hdrToday.classList.toggle("pos", today > 0);
  hdrToday.classList.toggle("neg", today < 0);
  $("#hdr-24h").textContent = trades24Count(trades24);

  // ─── fleet pulse ───
  $("#fleet-pulse-sub").textContent = `${running} active · ${workers.length} total`;
  $("#fleet-pulse").innerHTML = workers.map(w => `
    <div class="pulse-card">
      <span class="pulse-state ${w.state}"></span>
      <div class="pulse-info">
        <div class="pulse-id">${w.id}</div>
        <div class="pulse-meta">${w.state} · ${w.broker || "no broker"}</div>
      </div>
      <div class="pulse-balance">${w.last_balance != null ? "$" + Number(w.last_balance).toFixed(0) : "—"}</div>
    </div>`).join("") ||
    `<div style="padding:20px;color:var(--text-3);font-size:11px;text-align:center">no workers registered</div>`;

  // ─── movers ───
  renderMovers(portfolio.by_worker || [], trades24);

  // ─── equity curve ───
  renderHomeEquity();
}

function todaysPnl(trades) {
  const start = new Date(); start.setHours(0,0,0,0);
  return trades
    .filter(t => t.exit_time && new Date(t.exit_time) >= start)
    .reduce((s,t) => s + (t.net_pnl || 0), 0);
}
function trades24Count(trades) {
  const cutoff = Date.now() - 24 * 3600 * 1000;
  return trades.filter(t => t.exit_time && new Date(t.exit_time).getTime() >= cutoff).length;
}

function renderMovers(byWorker, trades24) {
  const el = $("#movers"); if (!el) return;

  // top + bottom 3 by net pnl
  const sorted = [...byWorker].sort((a,b) => b.net_pnl - a.net_pnl);
  const top = sorted.slice(0, 3);
  const bot = sorted.slice(-3).reverse();

  // recent trades — top 5 by abs pnl in last 24h
  const cutoff = Date.now() - 24 * 3600 * 1000;
  const recent = trades24
    .filter(t => t.exit_time && new Date(t.exit_time).getTime() >= cutoff)
    .sort((a,b) => Math.abs(b.net_pnl||0) - Math.abs(a.net_pnl||0))
    .slice(0, 5);

  const rowHtml = (rank, id, val) => `
    <div class="mover-row">
      <span class="m-rank">${rank}</span>
      <span class="m-id">${id}</span>
      <span class="m-pnl ${val >= 0 ? "pos" : "neg"}">${fmt.money(val)}</span>
    </div>`;

  let html = "";
  if (top.length) {
    html += `<div class="movers-section">▲ top workers</div>`;
    html += top.map((w,i) => rowHtml(i+1, w.worker_id, w.net_pnl)).join("");
  }
  if (bot.length && bot[0].net_pnl < 0) {
    html += `<div class="movers-section">▼ bottom workers</div>`;
    html += bot.filter(w => w.net_pnl < 0).map((w,i) => rowHtml(i+1, w.worker_id, w.net_pnl)).join("");
  }
  if (recent.length) {
    html += `<div class="movers-section">↯ biggest 24h</div>`;
    html += recent.map((t,i) => rowHtml(i+1, `${t.worker_id} #${t.ticket||"?"}`, t.net_pnl||0)).join("");
  }
  el.innerHTML = html || `<div class="movers-empty">no movement yet</div>`;
}

function renderHomeEquity() {
  const el = $("#equity-chart"); if (!el) return;

  if (!equityChart) {
    equityChart = LightweightCharts.createChart(el, {
      ...chartBaseOpts(el),
      handleScroll: false,
      handleScale: false,
      timeScale: {
        ...chartBaseOpts(el).timeScale,
        rightOffset: 4, fixLeftEdge: true, fixRightEdge: true,
        lockVisibleTimeRangeOnResize: true,
      },
    });
    equitySeries = equityChart.addAreaSeries({
      lineColor: cssVar("--accent"),
      topColor:  "color-mix(in srgb, " + cssVar("--accent") + " 35%, transparent)",
      bottomColor:"color-mix(in srgb, " + cssVar("--accent") + " 2%, transparent)",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
    });
    new ResizeObserver(() => {
      equityChart.resize(el.clientWidth, el.clientHeight);
      equityChart.timeScale().fitContent();
    }).observe(el);
  }

  // slice by selected range
  let data = equityRaw.slice();
  if (equityRange !== "all") {
    const n = parseInt(equityRange, 10);
    data = data.slice(-n);
  }

  // use sequential integer "time" so it never drifts even if trade timestamps are weird
  const points = data.map((e, i) => ({ time: i + 1, value: e.cum }));
  equitySeries.setData(points);
  equityChart.timeScale().fitContent();

  // meta line
  const last = points.length ? points[points.length - 1].value : 0;
  const first = points.length ? points[0].value : 0;
  const delta = last - first;
  $("#equity-meta").innerHTML = points.length
    ? `${points.length} trades · <span style="color:${delta >= 0 ? "var(--success)" : "var(--danger)"}">${fmt.money(delta)}</span> over range`
    : "no closed trades yet";
}

// range selector
document.addEventListener("click", e => {
  if (!e.target.classList.contains("seg-btn")) return;
  if (!e.target.closest(".eq-controls")) return;
  $$(".eq-controls .seg-btn").forEach(b => b.classList.toggle("active", b === e.target));
  equityRange = e.target.dataset.range;
  renderHomeEquity();
});


// ═══ PORTFOLIO ═══
let portEquityChart = null, portEquitySeries = null;
let portDdChart = null, portDdSeries = null;
let portHistChart = null, portHistSeries = null;

async function loadPortfolio() {
  const [p, equity] = await Promise.all([api.get("/api/portfolio"), api.get("/api/equity")]);
  renderMetrics(p);
  renderPortfolioEquity(equity);
  renderDrawdown(equity);
  renderByWorker(p.by_worker || []);
  renderHistogram(equity);
  loadTrades();
}

function renderMetrics(p) {
  const cell = (label, value, sub, kind="") => `
    <div class="metric">
      <div class="metric-label">${label}</div>
      <div class="metric-value ${kind}">${value}</div>
      ${sub ? `<div class="metric-sub">${sub}</div>` : ""}
    </div>`;
  const pnlKind = p.net_pnl > 0 ? "pos" : (p.net_pnl < 0 ? "neg" : "");
  const ddKind  = p.max_drawdown > 0 ? "neg" : "";
  const strkKind= p.current_streak_kind === "win" ? "pos" : (p.current_streak_kind === "loss" ? "neg" : "");
  $("#metric-grid").innerHTML = [
    cell("Net PnL",        fmt.big(p.net_pnl),                       `${p.n_trades} trades`, pnlKind),
    cell("Win Rate",       fmt.pct(p.win_rate),                       `${p.wins} W · ${p.losses} L`),
    cell("Profit Factor",  isFinite(p.profit_factor) ? p.profit_factor.toFixed(2) : "∞", "gross win / gross loss"),
    cell("Expectancy",     fmt.money(p.expectancy),                    "avg per trade", p.expectancy > 0 ? "pos" : (p.expectancy < 0 ? "neg" : "")),
    cell("Avg Win",        fmt.money(p.avg_win),                       "per winning trade", "pos"),
    cell("Avg Loss",       fmt.money(p.avg_loss),                      "per losing trade",  "neg"),
    cell("Best Trade",     fmt.money(p.best_trade),                    "single max",        "pos"),
    cell("Worst Trade",    fmt.money(p.worst_trade),                   "single min",        "neg"),
    cell("Max Drawdown",   fmt.money(-p.max_drawdown),                 `${p.max_dd_pct.toFixed(1)}% from peak`, ddKind),
    cell("Sharpe (per-tr)",p.sharpe.toFixed(2),                        "per-trade ratio"),
    cell("Avg R",          p.avg_r.toFixed(2) + "R",                   "risk-multiple"),
    cell("Avg Hold",       p.avg_bars_held.toFixed(1),                 "bars per trade"),
    cell("Longest Win",    p.longest_win_streak + "",                  "consecutive wins",  "pos"),
    cell("Longest Loss",   p.longest_loss_streak + "",                 "consecutive losses","neg"),
    cell("Current Streak", (p.current_streak || 0) + " " + (p.current_streak_kind || ""),  "live", strkKind),
    cell("Commission",     "$" + p.total_commission.toFixed(2),         "total paid"),
  ].join("");
}

function renderPortfolioEquity(equity) {
  const el = $("#port-equity"); if (!el) return;
  if (!portEquityChart) {
    portEquityChart = LightweightCharts.createChart(el, chartBaseOpts(el));
    portEquitySeries = portEquityChart.addAreaSeries({
      lineColor: cssVar("--success"),
      topColor:  "color-mix(in srgb, " + cssVar("--success") + " 30%, transparent)",
      bottomColor:"color-mix(in srgb, " + cssVar("--success") + " 2%, transparent)",
      lineWidth: 2,
    });
    new ResizeObserver(() => portEquityChart.resize(el.clientWidth, el.clientHeight)).observe(el);
  }
  const data = equity.map((e, i) => ({ time: i + 1, value: e.cum }));
  portEquitySeries.setData(data);
  if (data.length) portEquityChart.timeScale().fitContent();
}

function renderDrawdown(equity) {
  const el = $("#port-dd"); if (!el) return;
  if (!portDdChart) {
    portDdChart = LightweightCharts.createChart(el, chartBaseOpts(el));
    portDdSeries = portDdChart.addAreaSeries({
      lineColor: cssVar("--danger"),
      topColor:  "color-mix(in srgb, " + cssVar("--danger") + " 2%, transparent)",
      bottomColor:"color-mix(in srgb, " + cssVar("--danger") + " 30%, transparent)",
      lineWidth: 2,
    });
    new ResizeObserver(() => portDdChart.resize(el.clientWidth, el.clientHeight)).observe(el);
  }
  let peak = 0; const data = equity.map((e, i) => {
    if (e.cum > peak) peak = e.cum;
    return { time: i + 1, value: -(peak - e.cum) };
  });
  portDdSeries.setData(data);
  if (data.length) portDdChart.timeScale().fitContent();
}

function renderByWorker(byW) {
  const el = $("#port-by-worker"); if (!el) return;
  if (!byW.length) { el.innerHTML = `<div style="color:var(--text-3);font-size:12px">no trades yet</div>`; return; }
  const maxAbs = Math.max(...byW.map(w => Math.abs(w.net_pnl))) || 1;
  el.innerHTML = byW.map(w => {
    const pct = Math.abs(w.net_pnl) / maxAbs * 100;
    const kind = w.net_pnl >= 0 ? "pos" : "neg";
    return `
      <div class="bw-row">
        <div class="bw-id">${w.worker_id}</div>
        <div class="bw-bar"><div class="bw-fill ${kind}" style="width:${pct}%"></div></div>
        <div class="bw-val ${kind}">${fmt.money(w.net_pnl)}<br><span style="color:var(--text-3);font-size:10px">${w.trades}t · ${w.win_rate}%</span></div>
      </div>`;
  }).join("");
}

function renderHistogram(equity) {
  const el = $("#port-hist"); if (!el) return;
  if (!portHistChart) {
    portHistChart = LightweightCharts.createChart(el, chartBaseOpts(el));
    portHistSeries = portHistChart.addHistogramSeries({ priceFormat: { type: "volume" } });
    new ResizeObserver(() => portHistChart.resize(el.clientWidth, el.clientHeight)).observe(el);
  }
  const pnls = equity.map(e => e.pnl || 0).filter(p => p !== 0);
  if (!pnls.length) { portHistSeries.setData([]); return; }
  // bucket into 14 buckets
  const min = Math.min(...pnls), max = Math.max(...pnls);
  const buckets = 14;
  const step = (max - min) / buckets || 1;
  const counts = new Array(buckets).fill(0);
  pnls.forEach(p => {
    const i = Math.min(buckets - 1, Math.floor((p - min) / step));
    counts[i] += 1;
  });
  const data = counts.map((c, i) => {
    const center = min + step * (i + 0.5);
    return {
      time: i + 1,
      value: c,
      color: center >= 0 ? cssVar("--success") : cssVar("--danger"),
    };
  });
  portHistSeries.setData(data);
  portHistChart.timeScale().fitContent();
}

// ═══ FLEET ═══
async function loadFleet() {
  const workers = await api.get("/api/workers");
  state.workers = workers;
  const grid = $("#fleet-grid"); if (!grid) return;
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

// ═══ TRADES (now embedded in portfolio) ═══
async function loadTrades() {
  const wEl = $("#trade-filter-worker"); const sEl = $("#trade-filter-status");
  const wid = wEl ? wEl.value.trim() : ""; const st = sEl ? sEl.value : "";
  const qs = new URLSearchParams({limit: 200});
  if (wid) qs.set("worker_id", wid);
  if (st)  qs.set("status", st);
  const rows = await api.get("/api/trades?" + qs);
  const tb = $("#trades-table tbody"); if (!tb) return;
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
      <td>${t.bars_held ?? "—"}</td>
      <td>${verdictHtml(t.validator_verdict)}</td>
    </tr>`).join("");
}
function verdictHtml(v) {
  if (!v) return "—";
  if (v.match) return `<span class="verdict-ok">✓ match</span>`;
  return `<span class="verdict-bad" title="${escapeHtml(v.reason||"")}">⚠ ${(v.reason||"").slice(0,30)}</span>`;
}
document.addEventListener("input", e => {
  if (e.target.id === "trade-filter-worker") { clearTimeout(window._tf); window._tf = setTimeout(loadTrades, 300); }
  if (e.target.id === "log-filter-worker")   { clearTimeout(window._lf); window._lf = setTimeout(loadLogs, 300); }
});
document.addEventListener("change", e => {
  if (e.target.id === "trade-filter-status") loadTrades();
  if (e.target.id === "log-filter-level")    loadLogs();
});

// ═══ CHARTS PAGE ═══
async function initChartsPage() {
  const workers = await api.get("/api/workers");
  state.workers = workers;
  const sel = $("#chart-worker");
  sel.innerHTML = workers.map(w => `<option value="${w.id}">${w.id} · ${w.state}</option>`).join("");
  sel.onchange = () => rebindChart();
  if (!state.chartWorker && workers.length) state.chartWorker = workers[0].id;
  if (state.chartWorker) { sel.value = state.chartWorker; rebindChart(); }
}

async function rebindChart() {
  const wid = $("#chart-worker").value;
  state.chartWorker = wid;
  state.chartBars = []; state.chartMarkers = [];
  $("#chart-fills").innerHTML = "";

  const el = $("#live-chart");
  if (!state.chart) {
    state.chart = LightweightCharts.createChart(el, chartBaseOpts(el));
    state.candleSeries = state.chart.addCandlestickSeries({
      upColor: cssVar("--success"), downColor: cssVar("--danger"),
      wickUpColor: cssVar("--success"), wickDownColor: cssVar("--danger"),
      borderVisible: false,
    });
    new ResizeObserver(() => state.chart.resize(el.clientWidth, el.clientHeight)).observe(el);
  }

  const { bars, markers } = await api.get(`/api/bars/${wid}`);
  state.chartBars = bars.map(toCandle);
  state.candleSeries.setData(state.chartBars);
  const lwMarkers = markers.map(toMarker).filter(Boolean);
  state.chartMarkers = lwMarkers;
  state.candleSeries.setMarkers(lwMarkers);
  state.chart.timeScale().fitContent();

  const w = state.workers.find(x => x.id === wid);
  if (w) updateSideFromHB(w);
}

function toCandle(b) { return { time: b.time, open: b.open, high: b.high, low: b.low, close: b.close }; }
function toMarker(m) {
  if (!m.ts) return null;
  return {
    time: Math.floor(new Date(m.ts).getTime() / 1000),
    position: m.type === "open" ? (m.dir === 1 ? "belowBar" : "aboveBar") : "inBar",
    color:    m.type === "open" ? cssVar("--accent") : ((m.net_pnl || 0) >= 0 ? cssVar("--success") : cssVar("--danger")),
    shape:    m.type === "open" ? (m.dir === 1 ? "arrowUp" : "arrowDown") : "circle",
    text:     m.type === "open" ? `#${m.ticket}` : `${(m.net_pnl||0)>=0?"+":""}${Number(m.net_pnl||0).toFixed(0)}`,
  };
}

function addChartBar(bar) { if (state.candleSeries) state.candleSeries.update(toCandle(bar)); }
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
function updateSideFromBar(p) {
  $("#chart-engine").textContent   = p.engine_state || "—";
  $("#chart-livebars").textContent = p.live_bars_seen ?? 0;
  $("#chart-lastts").textContent   = p.bar ? new Date(p.bar.time * 1000).toLocaleTimeString() : "—";
}
function updateSideFromHB(p) {
  $("#chart-balance").textContent = p.last_balance ?? p.balance ?? "—";
  $("#chart-equity").textContent  = p.last_equity  ?? p.equity  ?? "—";
  $("#chart-ticket").textContent  = p.open_ticket || "—";
  $("#chart-engine").textContent  = p.engine_state || p.state || "—";
  $("#chart-livebars").textContent = p.live_bars_seen ?? 0;
}
function pushFill(trade, kind) {
  const el = $("#chart-fills"); if (!el) return;
  const isClose = kind === "close";
  const cls = isClose ? ((trade.net_pnl||0) >= 0 ? "win" : "loss") : "";
  el.insertAdjacentHTML("afterbegin", `
    <div class="fill ${cls}">
      <div class="fill-row"><b>${isClose ? "CLOSE" : "OPEN"} #${trade.ticket}</b>
        <span>${trade.dir===1?"LONG":"SHORT"}</span></div>
      <div class="fill-row">
        <span>${isClose ? fmt.num(trade.actual_exit) : fmt.num(trade.actual_entry)}</span>
        <b>${isClose ? fmt.money(trade.net_pnl) : ""}</b>
      </div>
    </div>`);
}

// ═══ LOGS ═══
async function loadLogs() {
  const w = $("#log-filter-worker").value.trim();
  const l = $("#log-filter-level").value;
  const qs = new URLSearchParams({limit: 400});
  if (w) qs.set("worker_id", w);
  if (l) qs.set("level", l);
  const rows = await api.get("/api/logs?" + qs);
  const el = $("#logs-out");
  el.innerHTML = rows.reverse().map(logLineHtml).join("");
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

// ═══ CONFIG ═══
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

// ═══ COMMAND PALETTE ═══
const palette = $("#cmd-palette");
const cmdInput = $("#cmd-input");
const cmdResults = $("#cmd-results");
let cmdSel = 0;

const cmdActions = [
  {ico: "◉", label: "Go to Mission Control", sub: "home",      fn: () => navigate("home")},
  {ico: "$", label: "Go to Portfolio",       sub: "portfolio", fn: () => navigate("portfolio")},
  {ico: "▦", label: "Go to Fleet",           sub: "fleet",     fn: () => navigate("fleet")},
  {ico: "▲", label: "Go to Live Charts",     sub: "charts",    fn: () => navigate("charts")},
  {ico: "≡", label: "Go to Logs",            sub: "logs",      fn: () => navigate("logs")},
  {ico: "◈", label: "Go to Configs",         sub: "config",    fn: () => navigate("config")},
  {ico: "◐", label: "Toggle theme panel",    sub: "theme",     fn: () => $("#theme-panel").classList.toggle("hidden")},
];

function openPalette()  { palette.classList.remove("hidden"); cmdInput.value = ""; cmdInput.focus(); renderCmd(""); }
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
    {ico: "↻", label: `Reload config ${w.id}`, sub: "worker action", fn: () => cmd(w.id, "reload_config")},
  ]));
  const themeActions = ["midnight","abyss","terminal","paper","daylight"].map(t => (
    {ico: "◐", label: `Theme: ${t}`, sub: "appearance", fn: () => { applyTheme(t); markActiveTheme(); }}
  ));
  const all = [...cmdActions, ...themeActions, ...workerActions];
  if (!q) return all.slice(0, 10);
  return all.filter(a => a.label.toLowerCase().includes(q) || a.sub.toLowerCase().includes(q)).slice(0, 14);
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

// ═══ BOOT ═══
connectWS();
loadHome();
setInterval(() => { if (state.activePage === "home") loadHome(); }, 8000);