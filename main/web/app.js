// ═══════════════════════════════════════════════════════════
// JINNI GRID — Mission Control v0.2
// ═══════════════════════════════════════════════════════════

const state = {
  workers: [], activePage: "home", feedItems: [],
  chartWorker: null, chart: null, candleSeries: null,
  chartBars: [], chartMarkers: [], ws: null,
  dbTable: null, dbOffset: 0, dbLimit: 50,
  liveAcct: { balance: 0, equity: 0, floating: 0 },
};

const $  = sel => document.querySelector(sel);
const $$ = sel => Array.from(document.querySelectorAll(sel));

const api = {
  get:  u    => fetch(u).then(r => r.json()),
  post: (u,b)=> fetch(u, {method:"POST", headers:{"Content-Type":"application/json"}, body: b?JSON.stringify(b):undefined}).then(r => r.json()),
  put:  (u,b)=> fetch(u, {method:"PUT", headers:{"Content-Type":"application/json"}, body: JSON.stringify(b)}).then(r => r.json()),
  del:  u    => fetch(u, {method:"DELETE"}).then(r => r.json()),
};

const fmt = {
  money: v => {
    if (v == null) return "—";
    const n = Number(v);
    const sign = n > 0 ? "+" : (n < 0 ? "-" : "");
    return sign + "$" + Math.abs(n).toFixed(2);
  },
  big: v => {
    if (v == null) return "$0";
    const n = Number(v);
    const sign = n < 0 ? "-" : "";
    return sign + "$" + Math.abs(n).toLocaleString("en-US", {maximumFractionDigits: 0});
  },
  num:   (v,d=2) => v == null ? "—" : Number(v).toFixed(d),
  pct:   v => v == null ? "—" : Number(v).toFixed(1) + "%",
  short: v => v ? new Date(v).toLocaleTimeString("en-GB", {hour12:false}).slice(0,8) : "—",
};

const escapeHtml = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));

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
  if (!themePanel.contains(e.target) && e.target.id !== "theme-btn" && !e.target.closest("#theme-btn")) themePanel.classList.add("hidden");
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
    if (liveAccountChart) liveAccountChart.applyOptions(chartBaseOpts($("#live-account-chart")));
    if (portEquityChart) portEquityChart.applyOptions(chartBaseOpts($("#port-equity-chart")));
    if (portDdChart) portDdChart.applyOptions(chartBaseOpts($("#port-dd")));
    if (portHistChart) portHistChart.applyOptions(chartBaseOpts($("#port-hist")));
    if (state.chart) state.chart.applyOptions(chartBaseOpts($("#live-chart")));
  }, 30);
}

// ═══ MOBILE NAV ═══
const sidebar = $("#sidebar");
const backdrop = $("#mobile-backdrop");
function openNav()  { sidebar.classList.add("open"); backdrop.classList.remove("hidden"); }
function closeNav() { sidebar.classList.remove("open"); backdrop.classList.add("hidden"); }
$("#nav-toggle").onclick = () => sidebar.classList.contains("open") ? closeNav() : openNav();
backdrop.onclick = closeNav;

// ═══ NAV ═══
$$(".nav-item").forEach(btn => btn.onclick = () => { navigate(btn.dataset.page); closeNav(); });
function navigate(page) {
  state.activePage = page;
  $$(".nav-item").forEach(b => b.classList.toggle("active", b.dataset.page === page));
  $$(".page").forEach(p => p.classList.toggle("active", p.dataset.page === page));
  if (page === "home")      loadHome();
  if (page === "portfolio") loadPortfolio();
  if (page === "fleet")     loadFleet();
  if (page === "logs")      loadLogs();
  if (page === "config")    initSettings();
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
  ws.onclose = () => { $("#conn-pill").classList.add("off"); $("#conn-text").textContent = "reconnecting"; setTimeout(connectWS, 2000); };
  ws.onmessage = e => handleWS(JSON.parse(e.data));
}

function handleWS(msg) {
  const t = msg.type;
  if (t === "heartbeat") {
    // live balance/equity refresh — every 5s per worker
    refreshLiveTopline();
    if (state.activePage === "home") {
      updateLiveAccountChart();
      refreshOpenPositions();
    }
    if (state.activePage === "portfolio") refreshPortfolioLive();
    if (state.activePage === "fleet")     loadFleet({silent:true});
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) updateSideFromHB(msg.payload);
  }
  if (t === "worker.update") {
    if (state.activePage === "home" || state.activePage === "fleet") loadFleet({silent:true});
    refreshLiveTopline();
  }
  if (t === "trade.opened") {
    pushFeed("trade-opened", msg.worker_id,
      `OPEN ${msg.payload.dir === 1 ? "▲ LONG" : "▼ SHORT"} ${msg.payload.symbol} @ ${fmt.num(msg.payload.actual_entry)}`);
    flashKpi("kpi-floating");
    if (state.activePage === "home") refreshOpenPositions();
    if (state.activePage === "portfolio") loadPortfolio();
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) { addChartMarker(msg.payload, "open"); pushFill(msg.payload, "open"); }
    toast("info", `${msg.worker_id}: trade opened`);
  }
  if (t === "trade.closed") {
    const p = msg.payload;
    const cls = (p.net_pnl || 0) >= 0 ? "trade-closed-win" : "trade-closed-loss";
    pushFeed(cls, msg.worker_id, `CLOSE ${p.symbol} · ${fmt.money(p.net_pnl)} · ${p.hit_sl ? "SL" : "TP"}`);
    flashKpi("kpi-pnl", p.net_pnl >= 0);
    if (state.activePage === "home")      { loadHome(); }
    if (state.activePage === "portfolio") { loadPortfolio(); }
    if (state.activePage === "charts" && msg.worker_id === state.chartWorker) { addChartMarker(p, "close"); pushFill(p, "close"); }
    toast(p.net_pnl >= 0 ? "success" : "error", `${msg.worker_id}: ${fmt.money(p.net_pnl)}`);
  }
  if (t === "log")   { if (state.activePage === "logs") appendLogLive(msg); }
  if (t === "error") { pushFeed("error", msg.worker_id, msg.payload.message); toast("error", `[${msg.worker_id}] ${msg.payload.message}`); }
  if (t === "bar")   { if (state.activePage === "charts" && msg.worker_id === state.chartWorker) { addChartBar(msg.payload.bar); updateSideFromBar(msg.payload); } }
}

// ═══ LIVE TOPLINE ═══
async function refreshLiveTopline() {
  try {
    const live = await api.get("/api/equity_live");
    state.liveAcct.balance = live.total_balance;
    state.liveAcct.equity  = live.total_equity;
    state.liveAcct.floating = live.total_floating;
    paintTopline();
  } catch (e) { /* ignore */ }
}
function paintTopline() {
  const eq = state.liveAcct.equity;
  const fl = state.liveAcct.floating;

  $("#hdr-equity").textContent = fmt.big(eq);
  const fEl = $("#hdr-floating");
  fEl.textContent = fmt.money(fl);
  fEl.classList.toggle("pos", fl > 0);
  fEl.classList.toggle("neg", fl < 0);

  // home cells
  const balEl = $("#kpi-balance"); if (balEl) balEl.querySelector(".kc-value").textContent = fmt.big(state.liveAcct.balance);
  const eqEl  = $("#kpi-equity");  if (eqEl)  eqEl.querySelector(".kc-value").textContent = fmt.big(eq);
  const flEl  = $("#kpi-floating");
  if (flEl) {
    const v = flEl.querySelector(".kc-value");
    v.textContent = fmt.money(fl);
    v.classList.toggle("positive", fl > 0);
    v.classList.toggle("negative", fl < 0);
  }

  // portfolio cells
  const pb = $("#port-balance");  if (pb) pb.querySelector(".kc-value").textContent = fmt.big(state.liveAcct.balance);
  const pe = $("#port-equity");   if (pe) pe.querySelector(".kc-value").textContent = fmt.big(eq);
  const pf = $("#port-floating");
  if (pf) {
    const v = pf.querySelector(".kc-value");
    v.textContent = fmt.money(fl);
    v.classList.toggle("positive", fl > 0);
    v.classList.toggle("negative", fl < 0);
  }
}
function flashKpi(id, pos = null) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.remove("flash-green", "flash-red");
  void el.offsetWidth;
  if (pos === true || pos === null) el.classList.add("flash-green");
  else el.classList.add("flash-red");
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
      <div class="feed-body"><span class="feed-worker">${escapeHtml(i.worker || "—")}</span>${escapeHtml(i.msg)}</div>
    </div>`).join("");
}

// ═══ TOASTS ═══
function toast(kind, msg) {
  const el = document.createElement("div");
  el.className = `toast ${kind}`; el.textContent = msg;
  $("#toast-stack").appendChild(el);
  setTimeout(() => { el.style.opacity = "0"; el.style.transform = "translateX(20px)"; }, 3200);
  setTimeout(() => el.remove(), 3600);
}

// ═══ CHART OPTS ═══
function cssVar(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }
function hexToRgba(hex, alpha) {
  hex = (hex || "").trim().replace("#", "");
  if (hex.length === 3) hex = hex.split("").map(c => c + c).join("");
  if (hex.length !== 6) return `rgba(140,140,140,${alpha})`;
  const r = parseInt(hex.slice(0,2), 16);
  const g = parseInt(hex.slice(2,4), 16);
  const b = parseInt(hex.slice(4,6), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}
function themeRgba(varName, alpha) {
  const v = cssVar(varName);
  if (v.startsWith("#")) return hexToRgba(v, alpha);
  if (v.startsWith("rgb")) {
    // already rgb / rgba — coerce to rgba with new alpha
    const m = v.match(/rgba?\(([^)]+)\)/);
    if (m) {
      const parts = m[1].split(",").map(s => s.trim());
      return `rgba(${parts[0]},${parts[1]},${parts[2]},${alpha})`;
    }
  }
  return `rgba(140,140,140,${alpha})`;
}

function chartBaseOpts(el) {
  return {
    width:  el ? el.clientWidth  : 600,
    height: el ? el.clientHeight : 300,
    layout: { background: { color: "transparent" }, textColor: cssVar("--text-2"), fontFamily: "JetBrains Mono", fontSize: 11 },
    grid: {
      vertLines: { color: themeRgba("--text-2", 0.08) },
      horzLines: { color: themeRgba("--text-2", 0.08) },
    },
    rightPriceScale: { borderColor: themeRgba("--text-2", 0.14) },
    timeScale:       { borderColor: themeRgba("--text-2", 0.14), timeVisible: true, secondsVisible: false },
    crosshair: { mode: 1 },
  };
}

// ═══════════════════════════════════════════════
// HOME / MISSION CONTROL
// ═══════════════════════════════════════════════
let liveAccountChart = null, balanceSeries = null, equitySeries = null;

async function loadHome() {
  const [workers, portfolio, trades24, live] = await Promise.all([
    api.get("/api/workers"),
    api.get("/api/portfolio"),
    api.get("/api/trades?status=closed&limit=200"),
    api.get("/api/equity_live"),
  ]);
  state.workers = workers;
  state.liveAcct = {
    balance: live.total_balance, equity: live.total_equity, floating: live.total_floating,
  };
  paintTopline();

  // ─── KPI: net pnl ───
  const pnlEl = $("#kpi-pnl");
  const pnlVal = pnlEl.querySelector(".kc-value");
  pnlVal.textContent = fmt.money(portfolio.net_pnl);
  pnlVal.classList.toggle("positive", portfolio.net_pnl > 0);
  pnlVal.classList.toggle("negative", portfolio.net_pnl < 0);
  $("#kpi-trades").textContent = `${portfolio.n_trades} trades · ${portfolio.win_rate}% wr`;

  // ─── KPI: fleet ───
  const running = workers.filter(w => w.state === "RUNNING").length;
  $("#kpi-fleet").querySelector(".kc-value").textContent = `${running} / ${workers.length}`;
  $("#kpi-fleet-dots").innerHTML = workers.slice(0, 24).map(w =>
    `<span class="kc-dot ${w.state}" title="${w.id} · ${w.state}"></span>`
  ).join("");

  // ─── header quick stats ───
  const today = todaysPnl(trades24);
  const hdrToday = $("#hdr-today");
  hdrToday.textContent = fmt.money(today);
  hdrToday.classList.toggle("pos", today > 0);
  hdrToday.classList.toggle("neg", today < 0);
  $("#hdr-24h").textContent = trades24Count(trades24);

  // ─── fleet pulse ───
  $("#fleet-pulse-sub").textContent = `${running} active · ${workers.length} total`;
  $("#fleet-pulse").innerHTML = workers.map(w => {
    const fl = (w.last_equity != null && w.last_balance != null) ? (w.last_equity - w.last_balance) : null;
    return `
      <div class="pulse-card">
        <span class="pulse-state ${w.state}"></span>
        <div class="pulse-info">
          <div class="pulse-id">${w.id}</div>
          <div class="pulse-meta">${w.state} · ${w.broker || "no broker"}</div>
        </div>
        <div class="pulse-right">
          <div class="pulse-balance">${w.last_equity != null ? "$" + Number(w.last_equity).toFixed(0) : "—"}</div>
          ${fl != null ? `<div class="pulse-floating ${fl>=0?"pos":"neg"}">${fmt.money(fl)}</div>` : ""}
        </div>
      </div>`;
  }).join("") || `<div class="pos-empty">no workers registered</div>`;

  // ─── open positions ───
  refreshOpenPositions();

  // ─── live account chart ───
  renderLiveAccountChart(live.per_worker);
}

function refreshOpenPositions() {
  const open = state.workers.filter(w => (w.open_positions || 0) > 0);
  $("#positions-sub").textContent = `${open.reduce((s,w)=>s+w.open_positions,0)} open`;
  const el = $("#positions-list");
  if (!el) return;
  if (!open.length) {
    el.innerHTML = `<div class="pos-empty">no open positions</div>`;
    return;
  }
  el.innerHTML = open.map(w => {
    const fl = (w.last_equity != null && w.last_balance != null) ? (w.last_equity - w.last_balance) : null;
    return `
      <div class="pos-row">
        <div class="pos-meta">
          <div class="pos-worker">${w.id}</div>
          <div class="pos-sub">${w.open_positions} position${w.open_positions>1?"s":""} · ${w.broker || ""}</div>
        </div>
        <div class="pos-price">${w.last_equity != null ? "$"+Number(w.last_equity).toFixed(2) : "—"}</div>
        <div>${fl != null ? `<span class="pos-dir ${fl>=0?"LONG":"SHORT"}">${fmt.money(fl)}</span>` : "—"}</div>
      </div>`;
  }).join("");
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

function buildAggSeries(perWorker) {
  // align all worker series by timestamp; for missing values, carry-forward last known per worker
  const all = []; // [{ts: <iso>, perWid: {wid: {bal, eq}}}]
  const seen = new Map();
  for (const [wid, arr] of Object.entries(perWorker)) {
    for (const p of arr) {
      const t = new Date(p.ts).getTime();
      if (!seen.has(t)) seen.set(t, {ts: t, per: {}});
      seen.get(t).per[wid] = {balance: p.balance, equity: p.equity};
    }
  }
  const sorted = Array.from(seen.values()).sort((a,b)=>a.ts-b.ts);
  // carry-forward
  const last = {};
  const balSeries = [], eqSeries = [];
  for (const pt of sorted) {
    for (const [wid, v] of Object.entries(pt.per)) last[wid] = v;
    let totalBal = 0, totalEq = 0;
    for (const v of Object.values(last)) { totalBal += v.balance; totalEq += v.equity; }
    const tSec = Math.floor(pt.ts / 1000);
    balSeries.push({ time: tSec, value: totalBal });
    eqSeries.push({  time: tSec, value: totalEq });
  }
  // dedupe equal timestamps (lightweight-charts strict)
  function dedupe(arr) {
    const out = [];
    let prev = -1;
    for (const p of arr) {
      let t = p.time;
      if (t <= prev) t = prev + 1;
      out.push({ time: t, value: p.value });
      prev = t;
    }
    return out;
  }
  return { bal: dedupe(balSeries), eq: dedupe(eqSeries) };
}

function renderLiveAccountChart(perWorker) {
  const el = $("#live-account-chart");
  if (!el) return;
  if (!liveAccountChart) {
    liveAccountChart = LightweightCharts.createChart(el, {
      ...chartBaseOpts(el),
      handleScroll: false, handleScale: false,
      timeScale: { ...chartBaseOpts(el).timeScale, rightOffset: 4, fixLeftEdge: true, fixRightEdge: true },
    });
    balanceSeries = liveAccountChart.addLineSeries({
      color: cssVar("--accent"), lineWidth: 2,
      priceLineVisible: false, lastValueVisible: true,
    });
    equitySeries = liveAccountChart.addAreaSeries({
      lineColor: cssVar("--success"),
      topColor: themeRgba("--success", 0.22),
      bottomColor: themeRgba("--success", 0.0),
      lineWidth: 2,
      priceLineVisible: false, lastValueVisible: true,
    });
    new ResizeObserver(() => { liveAccountChart.resize(el.clientWidth, el.clientHeight); liveAccountChart.timeScale().fitContent(); }).observe(el);
  }
  const { bal, eq } = buildAggSeries(perWorker || {});
  balanceSeries.setData(bal);
  equitySeries.setData(eq);
  if (eq.length) {
    liveAccountChart.timeScale().fitContent();
    const last = eq[eq.length - 1];
    const first = eq[0];
    const delta = last.value - first.value;
    $("#equity-meta").innerHTML = `${eq.length} pts · <span style="color:${delta>=0?'var(--success)':'var(--danger)'}">${delta>=0?"+":""}$${delta.toFixed(2)}</span> session`;
  } else {
    $("#equity-meta").textContent = "waiting for heartbeats…";
  }
}

async function updateLiveAccountChart() {
  const live = await api.get("/api/equity_live");
  state.liveAcct = { balance: live.total_balance, equity: live.total_equity, floating: live.total_floating };
  paintTopline();
  renderLiveAccountChart(live.per_worker);
}

// ═══════════════════════════════════════════════
// PORTFOLIO
// ═══════════════════════════════════════════════
let portEquityChart = null, portEquitySeries = null;
let portDdChart = null, portDdSeries = null;
let portHistChart = null, portHistSeries = null;

async function loadPortfolio() {
  let p = null, equity = [], live = null;
  try { p      = await api.get("/api/portfolio");    } catch (e) { console.error("portfolio failed:", e); toast("error", "portfolio failed: " + e.message); }
  try { equity = await api.get("/api/equity");       } catch (e) { console.error("equity failed:", e); }
  try { live   = await api.get("/api/equity_live");  } catch (e) { console.error("equity_live failed:", e); }

  if (live) {
    state.liveAcct = { balance: live.total_balance, equity: live.total_equity, floating: live.total_floating };
    paintTopline();
  }

  // each render in its own try so one failure doesn't kill the rest
  if (p) { try { renderMetrics(p); } catch (e) { console.error("renderMetrics:", e); } }
  try { renderPortfolioEquity(equity); } catch (e) { console.error("renderPortfolioEquity:", e); }
  try { renderDrawdown(equity); }       catch (e) { console.error("renderDrawdown:", e); }
  if (p) { try { renderByWorker(p.by_worker || []); } catch (e) { console.error("renderByWorker:", e); } }
  try { renderHistogram(equity); }      catch (e) { console.error("renderHistogram:", e); }
  try { await loadTrades(); }           catch (e) { console.error("loadTrades:", e); toast("error", "trades failed: " + e.message); }
}
async function refreshPortfolioLive() {
  const live = await api.get("/api/equity_live");
  state.liveAcct = { balance: live.total_balance, equity: live.total_equity, floating: live.total_floating };
  paintTopline();
}

function renderMetrics(p) {
  p = p || {};
  // defensive defaults so missing fields don't throw
  const safe = (v, d=0) => (v == null || Number.isNaN(v)) ? d : v;
  p = {
    n_trades: safe(p.n_trades), wins: safe(p.wins), losses: safe(p.losses),
    win_rate: safe(p.win_rate), net_pnl: safe(p.net_pnl),
    profit_factor: safe(p.profit_factor), expectancy: safe(p.expectancy),
    avg_win: safe(p.avg_win), avg_loss: safe(p.avg_loss),
    best_trade: safe(p.best_trade), worst_trade: safe(p.worst_trade),
    max_drawdown: safe(p.max_drawdown), max_dd_pct: safe(p.max_dd_pct),
    sharpe: safe(p.sharpe), avg_r: safe(p.avg_r), avg_bars_held: safe(p.avg_bars_held),
    longest_win_streak: safe(p.longest_win_streak),
    longest_loss_streak: safe(p.longest_loss_streak),
    current_streak: safe(p.current_streak), current_streak_kind: p.current_streak_kind || "—",
    total_commission: safe(p.total_commission), by_worker: p.by_worker || [],
  };
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
    cell("Profit Factor",  p.profit_factor >= 999 ? "∞" : p.profit_factor.toFixed(2), "gross win / gross loss"),
    cell("Expectancy",     fmt.money(p.expectancy),                    "avg per trade", p.expectancy > 0 ? "pos" : (p.expectancy < 0 ? "neg" : "")),
    cell("Avg Win",        fmt.money(p.avg_win),                       "per winning trade", "pos"),
    cell("Avg Loss",       fmt.money(p.avg_loss),                      "per losing trade",  "neg"),
    cell("Best Trade",     fmt.money(p.best_trade),                    "single max",        "pos"),
    cell("Worst Trade",    fmt.money(p.worst_trade),                   "single min",        "neg"),
    cell("Max Drawdown",   fmt.money(-p.max_drawdown),                 `${p.max_dd_pct.toFixed(1)}% from peak`, ddKind),
    cell("Sharpe (tr)",    p.sharpe.toFixed(2),                        "per-trade ratio"),
    cell("Avg R",          p.avg_r.toFixed(2) + "R",                   "risk-multiple"),
    cell("Avg Hold",       p.avg_bars_held.toFixed(1),                 "bars per trade"),
    cell("Longest Win",    p.longest_win_streak + "",                  "consecutive wins",  "pos"),
    cell("Longest Loss",   p.longest_loss_streak + "",                 "consecutive losses","neg"),
    cell("Current Streak", (p.current_streak || 0) + " " + (p.current_streak_kind || ""),  "live", strkKind),
    cell("Commission",     "$" + p.total_commission.toFixed(2),         "total paid"),
  ].join("");
}

function renderPortfolioEquity(equity) {
  const el = $("#port-equity-chart"); if (!el) return;
  if (!Array.isArray(equity)) equity = [];
  if (!portEquityChart) {
    portEquityChart = LightweightCharts.createChart(el, {
      ...chartBaseOpts(el), handleScroll: false, handleScale: false,
    });
    portEquitySeries = portEquityChart.addAreaSeries({
      lineColor: cssVar("--success"),
      topColor: themeRgba("--success", 0.30),
      bottomColor: themeRgba("--success", 0.0),
      lineWidth: 2,
    });
    new ResizeObserver(() => { portEquityChart.resize(el.clientWidth, el.clientHeight); portEquityChart.timeScale().fitContent(); }).observe(el);
  }
  const data = equity.map((e, i) => ({ time: i + 1, value: e.cum }));
  portEquitySeries.setData(data);
  if (data.length) portEquityChart.timeScale().fitContent();
}

function renderDrawdown(equity) {
  const el = $("#port-dd"); if (!el) return;
  if (!portDdChart) {
    portDdChart = LightweightCharts.createChart(el, {
      ...chartBaseOpts(el), handleScroll: false, handleScale: false,
    });
    portDdSeries = portDdChart.addAreaSeries({
      lineColor: cssVar("--danger"),
      topColor: themeRgba("--danger", 0.02),
      bottomColor: themeRgba("--danger", 0.30),
      lineWidth: 2,
    });
    new ResizeObserver(() => { portDdChart.resize(el.clientWidth, el.clientHeight); portDdChart.timeScale().fitContent(); }).observe(el);
  }
  let peak = 0; const data = equity.map((e, i) => { if (e.cum > peak) peak = e.cum; return { time: i + 1, value: -(peak - e.cum) }; });
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
    portHistChart = LightweightCharts.createChart(el, {
      ...chartBaseOpts(el), handleScroll: false, handleScale: false,
    });
    portHistSeries = portHistChart.addHistogramSeries({ priceFormat: { type: "volume" } });
    new ResizeObserver(() => { portHistChart.resize(el.clientWidth, el.clientHeight); portHistChart.timeScale().fitContent(); }).observe(el);
  }
  const pnls = equity.map(e => e.pnl || 0).filter(p => p !== 0);
  if (!pnls.length) { portHistSeries.setData([]); return; }
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
    return { time: i + 1, value: c, color: center >= 0 ? cssVar("--success") : cssVar("--danger") };
  });
  portHistSeries.setData(data);
  portHistChart.timeScale().fitContent();
}

// ═══════════════════════════════════════════════
// FLEET
// ═══════════════════════════════════════════════
async function loadFleet() {
  const workers = await api.get("/api/workers");
  state.workers = workers;
  const grid = $("#fleet-grid"); if (!grid) return;
  grid.innerHTML = workers.map(w => {
    const fl = (w.last_equity != null && w.last_balance != null) ? (w.last_equity - w.last_balance) : null;
    return `
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
          <div class="stat-cell"><label>floating</label>${fl != null ? `<b class="${fl>=0?'pos':'neg'}">${fmt.money(fl)}</b>` : "<b>—</b>"}</div><div class="stat-cell"><label>floating</label>${fl != null ? `<b class="${fl>=0?'pos':'neg'}">${fl>=0?"+":""}$${fl.toFixed(2)}</b>` : "<b>—</b>"}</div>
          <div class="stat-cell"><label>positions</label><b>${w.open_positions ?? 0}</b></div>
          <div class="stat-cell"><label>bars in mem</label><b>${w.mem_bars ?? 0}</b></div>
          <div class="stat-cell"><label>last hb</label><b>${w.last_heartbeat ? fmt.short(w.last_heartbeat) : "—"}</b></div>
        </div>
        <div class="worker-card-actions">
          <button onclick="cmd('${w.id}','start')"><i class="fa-solid fa-play"></i></button>
          <button onclick="cmd('${w.id}','stop')"><i class="fa-solid fa-stop"></i></button>
          <button onclick="cmd('${w.id}','restart')"><i class="fa-solid fa-rotate-right"></i></button>
          <button onclick="cmd('${w.id}','reload_config')"><i class="fa-solid fa-arrows-rotate"></i></button>
          <button onclick="cmd('${w.id}','ping')"><i class="fa-solid fa-satellite-dish"></i></button>
        </div>
      </div>`;
  }).join("") || `<div style="padding:32px;color:var(--text-3)">no workers — add a config in main/configs/&lt;worker_id&gt;.json</div>`;
}
async function cmd(id, action) {
  const r = await api.post(`/api/workers/${id}/${action}`);
  toast(r.ok ? "success" : "error", `${id} · ${action} → ${r.msg || (r.ok ? "ok" : "fail")}`);
  loadFleet();
}

// ═══════════════════════════════════════════════
// TRADES
// ═══════════════════════════════════════════════
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
  if (v.inconclusive) return `<span style="color:var(--text-3)">➖ inconclusive</span>`;
  if (v.match) return `<span class="verdict-ok"><i class="fa-solid fa-check"></i> match</span>`;
  return `<span class="verdict-bad" title="${escapeHtml(v.reason||"")}"><i class="fa-solid fa-triangle-exclamation"></i> ${(v.reason||"").slice(0,30)}</span>`;
}
document.addEventListener("input", e => {
  if (e.target.id === "trade-filter-worker") { clearTimeout(window._tf); window._tf = setTimeout(loadTrades, 300); }
  if (e.target.id === "log-filter-worker")   { clearTimeout(window._lf); window._lf = setTimeout(loadLogs, 300); }
});
document.addEventListener("change", e => {
  if (e.target.id === "trade-filter-status") loadTrades();
  if (e.target.id === "log-filter-level")    loadLogs();
});

// ═══════════════════════════════════════════════
// CHARTS
// ═══════════════════════════════════════════════
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
    ts: type === "open" ? trade.entry_time : trade.exit_time,
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

// ═══════════════════════════════════════════════
// LOGS
// ═══════════════════════════════════════════════
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
    <span class="log-worker">${escapeHtml(r.worker_id || "—")}</span>
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

// ═══════════════════════════════════════════════
// SETTINGS (configs + DB)
// ═══════════════════════════════════════════════
function initSettings() {
  // tabs
  $$("#settings-tabs .seg-btn").forEach(b => b.onclick = () => {
    $$("#settings-tabs .seg-btn").forEach(x => x.classList.toggle("active", x === b));
    $$(".settings-pane").forEach(p => p.classList.toggle("active", p.dataset.pane === b.dataset.tab));
    if (b.dataset.tab === "db") loadDbTables();
    if (b.dataset.tab === "configs") loadConfigList();
  });
  loadConfigList();
}

let currentConfigId = null;
async function loadConfigList() {
  const ids = await api.get("/api/configs");
  $("#config-list-items").innerHTML = ids.map(i => `
    <div class="config-list-item ${i === currentConfigId ? "active" : ""}" data-id="${i}">
      <b>${i}</b>
      <small><i class="fa-solid fa-file-code"></i></small>
    </div>`).join("") || `<div style="padding:16px;color:var(--text-3);font-size:11px">no configs</div>`;
  $$("#config-list-items .config-list-item").forEach(el => {
    el.onclick = () => { currentConfigId = el.dataset.id; loadConfig(); loadConfigList(); };
  });
  if (!currentConfigId && ids.length) { currentConfigId = ids[0]; loadConfig(); loadConfigList(); }
}
async function loadConfig() {
  if (!currentConfigId) return;
  const cfg = await api.get(`/api/configs/${currentConfigId}`);
  $("#config-editor-title").textContent = currentConfigId;
  $("#config-editor").value = JSON.stringify(cfg, null, 2);
}
async function saveConfig() {
  if (!currentConfigId) return;
  let cfg;
  try { cfg = JSON.parse($("#config-editor").value); }
  catch (e) { toast("error", "invalid JSON: " + e.message); return; }
  const r = await api.put(`/api/configs/${currentConfigId}`, cfg);
  toast(r.ok ? "success" : "error", r.ok ? `${currentConfigId} saved & pushed` : "save failed");
}

// ═══ DB EDITOR ═══
async function loadDbTables() {
  const tables = await api.get("/api/db/tables");
  $("#db-table-list").innerHTML = tables.map(t => `
    <div class="db-table-item ${t.name === state.dbTable ? "active" : ""}" data-name="${t.name}">
      <b>${t.name}</b>
      <small>${t.row_count} rows</small>
    </div>`).join("");
  $$("#db-table-list .db-table-item").forEach(el => {
    el.onclick = () => { state.dbTable = el.dataset.name; state.dbOffset = 0; loadDbTable(); loadDbTables(); };
  });
  if (!state.dbTable && tables.length) { state.dbTable = tables[0].name; loadDbTable(); loadDbTables(); }
}
async function loadDbTable() {
  if (!state.dbTable) return;
  const d = await api.get(`/api/db/table/${state.dbTable}?limit=${state.dbLimit}&offset=${state.dbOffset}`);
  $("#db-table-title").textContent = d.name;
  $("#db-pagination").textContent = `${state.dbOffset+1}–${Math.min(state.dbOffset+state.dbLimit,d.total)} / ${d.total}`;
  const thead = $("#db-rows thead"), tbody = $("#db-rows tbody");
  thead.innerHTML = `<tr>${d.columns.map(c => `<th>${c}</th>`).join("")}<th></th></tr>`;
  tbody.innerHTML = d.rows.map(r => `
    <tr>
      ${d.columns.map(c => `<td title="${escapeHtml(formatCell(r[c]))}">${escapeHtml(formatCell(r[c]).slice(0,80))}</td>`).join("")}
      <td><button class="btn-ghost" onclick="dbDeleteRow('${r[d.pk[0]]}')" style="padding:2px 8px;height:24px"><i class="fa-solid fa-trash"></i></button></td>
    </tr>`).join("") || `<tr><td colspan="${d.columns.length+1}" style="text-align:center;color:var(--text-3);padding:24px">empty</td></tr>`;
}
function formatCell(v) {
  if (v == null) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
async function dbDeleteRow(id) {
  if (!confirm(`Delete row ${id} from ${state.dbTable}?`)) return;
  const r = await api.del(`/api/db/table/${state.dbTable}/${id}`);
  toast(r.ok ? "success" : "error", r.ok ? "deleted" : "delete failed");
  loadDbTable(); loadDbTables();
}
$("#db-prev").onclick = () => { state.dbOffset = Math.max(0, state.dbOffset - state.dbLimit); loadDbTable(); };
$("#db-next").onclick = () => { state.dbOffset += state.dbLimit; loadDbTable(); };

async function runDbQuery() {
  const q = $("#db-query").value.trim();
  if (!q) return;
  const r = await api.post("/api/db/query", { query: q });
  const out = $("#db-query-result");
  if (!r.ok) {
    out.innerHTML = `<div style="padding:12px;color:var(--danger);font-family:var(--font-mono);font-size:11px">${escapeHtml(r.error)}</div>`;
    return;
  }
  if (!r.rows.length) { out.innerHTML = `<div style="padding:12px;color:var(--text-3);font-size:11px">0 rows</div>`; return; }
  const cols = Object.keys(r.rows[0]);
  out.innerHTML = `
    <table class="data-table">
      <thead><tr>${cols.map(c => `<th>${c}</th>`).join("")}</tr></thead>
      <tbody>${r.rows.map(row => `<tr>${cols.map(c => `<td>${escapeHtml(formatCell(row[c]).slice(0,80))}</td>`).join("")}</tr>`).join("")}</tbody>
    </table>
    <div style="padding:8px 12px;color:var(--text-3);font-size:11px">${r.count} rows</div>`;
}

// ═══════════════════════════════════════════════
// COMMAND PALETTE
// ═══════════════════════════════════════════════
const palette = $("#cmd-palette");
const cmdInput = $("#cmd-input");
const cmdResults = $("#cmd-results");
let cmdSel = 0;

const cmdActions = [
  {ico: "fa-gauge-high", label: "Mission Control", sub: "go",  fn: () => navigate("home")},
  {ico: "fa-chart-pie",  label: "Portfolio",       sub: "go",  fn: () => navigate("portfolio")},
  {ico: "fa-server",     label: "Fleet",           sub: "go",  fn: () => navigate("fleet")},
  {ico: "fa-chart-line", label: "Live Charts",     sub: "go",  fn: () => navigate("charts")},
  {ico: "fa-terminal",   label: "Logs",            sub: "go",  fn: () => navigate("logs")},
  {ico: "fa-sliders",    label: "Settings",        sub: "go",  fn: () => navigate("config")},
  {ico: "fa-palette",    label: "Toggle theme panel", sub: "appearance", fn: () => $("#theme-panel").classList.toggle("hidden")},
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
    {ico: "fa-play",          label: `Start ${w.id}`,    sub: "worker", fn: () => cmd(w.id, "start")},
    {ico: "fa-stop",          label: `Stop ${w.id}`,     sub: "worker", fn: () => cmd(w.id, "stop")},
    {ico: "fa-rotate-right",  label: `Restart ${w.id}`,  sub: "worker", fn: () => cmd(w.id, "restart")},
    {ico: "fa-arrows-rotate", label: `Reload config ${w.id}`, sub: "worker", fn: () => cmd(w.id, "reload_config")},
  ]));
  const themeActions = ["midnight","abyss","terminal","paper","daylight"].map(t => (
    {ico: "fa-palette", label: `Theme: ${t}`, sub: "theme", fn: () => { applyTheme(t); markActiveTheme(); }}
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
      <span class="ico"><i class="fa-solid ${r.ico}"></i></span>
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
refreshLiveTopline();
setInterval(refreshLiveTopline, 5000);
setInterval(() => { if (state.activePage === "home") loadHome(); }, 12000);