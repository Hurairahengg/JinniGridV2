// ── tabs ─────────────────────────────────────────────────────
document.querySelectorAll(".tab").forEach(b => {
  b.onclick = () => {
    document.querySelectorAll(".tab").forEach(x => x.classList.remove("active"));
    document.querySelectorAll(".tab-pane").forEach(x => x.classList.remove("active"));
    b.classList.add("active");
    document.getElementById("tab-" + b.dataset.tab).classList.add("active");
    if (b.dataset.tab === "trades")  loadTrades();
    if (b.dataset.tab === "logs")    loadLogs();
    if (b.dataset.tab === "configs") loadConfigList();
  };
});

// ── WS to mother ─────────────────────────────────────────────
let ws;
function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/ui`);
  ws.onopen  = () => setConn(true);
  ws.onclose = () => { setConn(false); setTimeout(connectWS, 2000); };
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === "heartbeat" || msg.type === "worker.update") loadFleet();
    if (msg.type === "trade.opened" || msg.type === "trade.closed") {
      loadFleet(); loadPortfolio();
      if (document.getElementById("tab-trades").classList.contains("active")) loadTrades();
    }
    if (msg.type === "log" || msg.type === "error") {
      if (document.getElementById("tab-logs").classList.contains("active")) loadLogs();
    }
  };
}
function setConn(on) {
  const el = document.getElementById("conn-status");
  el.textContent = on ? "live" : "disconnected";
  el.className = "conn " + (on ? "on" : "off");
}

// ── api helpers ───────────────────────────────────────────────
const api = {
  get:  (u)      => fetch(u).then(r => r.json()),
  post: (u)      => fetch(u, {method:"POST"}).then(r => r.json()),
  put:  (u, b)   => fetch(u, {method:"PUT", headers:{"Content-Type":"application/json"},
                              body: JSON.stringify(b)}).then(r => r.json()),
};

// ── fleet ────────────────────────────────────────────────────
async function loadFleet() {
  const ws = await api.get("/api/workers");
  const tb = document.querySelector("#fleet-table tbody");
  tb.innerHTML = ws.map(w => `
    <tr>
      <td>${w.id}</td>
      <td><span class="badge ${w.state}">${w.state}</span>
          ${w.connected ? "🔗" : "—"}</td>
      <td>${w.broker || "–"}</td>
      <td>${w.account || "–"}</td>
      <td>${fmt(w.last_balance)}</td>
      <td>${fmt(w.last_equity)}</td>
      <td>${w.open_positions ?? 0}</td>
      <td>${w.mem_bars ?? 0}</td>
      <td>${w.last_heartbeat ? new Date(w.last_heartbeat).toLocaleTimeString() : "–"}</td>
      <td class="actions">
        <button onclick="cmd('${w.id}','start')">▶</button>
        <button onclick="cmd('${w.id}','stop')">■</button>
        <button onclick="cmd('${w.id}','restart')">⟳</button>
        <button onclick="cmd('${w.id}','reload_config')">⇩cfg</button>
        <button onclick="cmd('${w.id}','ping')">ping</button>
      </td>
    </tr>`).join("");
}
async function loadPortfolio() {
  const p = await api.get("/api/portfolio");
  document.getElementById("pf-n").textContent   = p.n_trades;
  document.getElementById("pf-pnl").textContent = "$" + p.net_pnl.toFixed(2);
  document.getElementById("pf-wr").textContent  = p.win_rate + "%";
  document.getElementById("pf-wl").textContent  = `${p.wins} / ${p.losses}`;
}
async function cmd(id, action) {
  const r = await api.post(`/api/workers/${id}/${action}`);
  if (!r.ok) alert(`${action} failed: ${r.msg}`);
  loadFleet();
}

// ── trades ───────────────────────────────────────────────────
async function loadTrades() {
  const w = document.getElementById("trade-filter-worker").value.trim();
  const s = document.getElementById("trade-filter-status").value;
  const qs = new URLSearchParams();
  if (w) qs.set("worker_id", w);
  if (s) qs.set("status", s);
  qs.set("limit", "200");
  const rows = await api.get("/api/trades?" + qs.toString());
  const tb = document.querySelector("#trades-table tbody");
  tb.innerHTML = rows.map(t => `
    <tr class="${t.net_pnl > 0 ? "win" : t.net_pnl < 0 ? "loss" : ""}">
      <td>${t.id}</td><td>${t.worker_id}</td><td>${t.ticket || "–"}</td>
      <td>${t.symbol || ""}</td>
      <td>${t.direction === 1 ? "L" : t.direction === -1 ? "S" : "–"}</td>
      <td>${t.status}</td>
      <td>${fmt(t.actual_entry)}</td>
      <td>${fmt(t.actual_exit)}</td>
      <td>${fmt(t.lots)}</td>
      <td>${t.net_pnl != null ? "$" + t.net_pnl.toFixed(2) : "–"}</td>
      <td>${t.r_multiple != null ? t.r_multiple.toFixed(2) + "R" : "–"}</td>
      <td>${verdictBadge(t.validator_verdict)}</td>
    </tr>`).join("");
}
function verdictBadge(v) {
  if (!v) return "–";
  return v.match ? "✅" : `⚠️ ${v.reason || ""}`;
}

// ── logs ─────────────────────────────────────────────────────
async function loadLogs() {
  const w = document.getElementById("log-filter-worker").value.trim();
  const l = document.getElementById("log-filter-level").value;
  const qs = new URLSearchParams();
  if (w) qs.set("worker_id", w);
  if (l) qs.set("level", l);
  qs.set("limit", "300");
  const rows = await api.get("/api/logs?" + qs.toString());
  document.getElementById("logs-out").textContent = rows.map(r =>
    `[${r.ts}] ${r.level.padEnd(5)} ${(r.worker_id||"-").padEnd(10)} ${r.message}`
  ).join("\n");
}

// ── configs ──────────────────────────────────────────────────
async function loadConfigList() {
  const ids = await api.get("/api/configs");
  const sel = document.getElementById("config-select");
  sel.innerHTML = ids.map(i => `<option value="${i}">${i}</option>`).join("");
  if (ids.length) loadConfig();
}
async function loadConfig() {
  const id = document.getElementById("config-select").value;
  if (!id) return;
  const cfg = await api.get(`/api/configs/${id}`);
  document.getElementById("config-editor").value = JSON.stringify(cfg, null, 2);
}
async function saveConfig() {
  const id = document.getElementById("config-select").value;
  let cfg;
  try { cfg = JSON.parse(document.getElementById("config-editor").value); }
  catch (e) { alert("invalid JSON: " + e); return; }
  const r = await api.put(`/api/configs/${id}`, cfg);
  alert(r.ok ? "saved + pushed" : "save failed");
}

// ── utils ────────────────────────────────────────────────────
function fmt(v) { return v == null ? "–" : (typeof v === "number" ? v.toFixed(2) : v); }

// ── boot ─────────────────────────────────────────────────────
connectWS();
loadFleet();
loadPortfolio();
setInterval(loadPortfolio, 10000);