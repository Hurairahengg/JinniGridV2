"""
main.py — Mother control plane.

Run:   python main.py
That's it. Boots the web server on port 5000.
"""

import asyncio
import uvicorn
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import math, json as _json

def _safe_default(o):
    if isinstance(o, float):
        if math.isinf(o): return 999999.99 if o > 0 else -999999.99
        if math.isnan(o): return 0.0
    raise TypeError(f"not serializable: {type(o)}")

class SafeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return _json.dumps(content, default=_safe_default, allow_nan=False,
                           ensure_ascii=False).encode("utf-8")

import store
from fleet import fleet, load_config, save_config, list_configs

WEB_DIR = Path(__file__).parent / "web"
HOST = "0.0.0.0"
PORT = 5000

app = FastAPI(title="Jinni Grid — Mother", default_response_class=SafeJSONResponse)


# ═══════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════
@app.on_event("startup")
async def _startup():
    store.init_db()
    asyncio.create_task(fleet.heartbeat_loop())


# ═══════════════════════════════════════════════════════════════
# WEBSOCKETS  (single endpoint, worker identifies via hello)
# ═══════════════════════════════════════════════════════════════
@app.websocket("/ws")
async def ws_worker(ws: WebSocket):
    await ws.accept()
    worker_id = None
    try:
        # ─── wait for hello ───
        try:
            first = await asyncio.wait_for(ws.receive_json(), timeout=10)
        except Exception:
            await ws.close(code=4001, reason="hello timeout"); return

        if first.get("type") != "hello":
            await ws.close(code=4002, reason="hello required"); return

        worker_id = (first.get("payload") or {}).get("worker_id")
        if not worker_id:
            await ws.close(code=4003, reason="missing worker_id"); return

        # mother just checks: does this worker have a config?
        if load_config(worker_id) is None:
            await ws.close(code=4004, reason=f"unknown worker_id: {worker_id}"); return

        await fleet.attach(worker_id, ws, first.get("payload") or {})
        await fleet.hello_ack(worker_id, ws)

        # ─── main message loop ───
        while True:
            msg = await ws.receive_json()
            await fleet.handle_message(worker_id, msg)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        if worker_id:
            store.insert_log(worker_id, "ERROR", f"ws loop crashed: {e}")
    finally:
        if worker_id:
            await fleet.detach(worker_id)


@app.websocket("/ws/ui")
async def ws_ui(ws: WebSocket):
    await ws.accept()
    fleet.attach_ui(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        fleet.detach_ui(ws)


# ═══════════════════════════════════════════════════════════════
# REST API
# ═══════════════════════════════════════════════════════════════
@app.get("/api/workers")
def api_workers():
    workers = store.list_workers()
    # also surface workers that have configs but never connected
    known_ids = {w["id"] for w in workers}
    for cid in list_configs():
        if cid not in known_ids:
            workers.append({
                "id": cid, "state": "OFFLINE", "broker": None, "account": None,
                "last_balance": None, "last_equity": None, "open_positions": 0,
                "mem_bars": 0, "last_heartbeat": None, "version": None,
            })
    connected = set(fleet.workers.keys())
    for w in workers:
        w["connected"] = w["id"] in connected
    workers.sort(key=lambda x: x["id"])
    return workers


@app.get("/api/workers/{worker_id}")
def api_worker_detail(worker_id: str):
    w = store.get_worker(worker_id) or {"id": worker_id, "state": "OFFLINE"}
    w["connected"] = worker_id in fleet.workers
    w["config"] = load_config(worker_id)
    return w


@app.post("/api/workers/{worker_id}/start")
async def api_start(worker_id: str):
    ok, msg = await fleet.send_command(worker_id, "cmd.start"); return {"ok": ok, "msg": msg}
@app.get("/api/bars/{worker_id}")
def api_bars(worker_id: str):
    bars = list(fleet.bar_history.get(worker_id, []))
    markers = list(fleet.recent_trade_markers.get(worker_id, []))
    return {"bars": bars, "markers": markers}


@app.get("/api/equity")
def api_equity():
    """Cumulative net_pnl over closed trades, for the home equity curve."""
    trades = store.list_trades(status="closed", limit=10000)
    trades = sorted(trades, key=lambda t: t.get("exit_time") or "")
    out, cum = [], 0.0
    for t in trades:
        cum += (t.get("net_pnl") or 0)
        out.append({"ts": t.get("exit_time"), "cum": round(cum, 2),
                    "pnl": t.get("net_pnl"), "worker": t.get("worker_id")})
    return out

@app.post("/api/workers/{worker_id}/stop")
async def api_stop(worker_id: str):
    ok, msg = await fleet.send_command(worker_id, "cmd.stop"); return {"ok": ok, "msg": msg}


@app.post("/api/workers/{worker_id}/restart")
async def api_restart(worker_id: str):
    ok, msg = await fleet.send_command(worker_id, "cmd.restart"); return {"ok": ok, "msg": msg}


@app.post("/api/workers/{worker_id}/ping")
async def api_ping(worker_id: str):
    ok, msg = await fleet.send_command(worker_id, "cmd.ping"); return {"ok": ok, "msg": msg}


@app.post("/api/workers/{worker_id}/reload_config")
async def api_reload_config(worker_id: str):
    ok, msg = await fleet.push_config(worker_id); return {"ok": ok, "msg": msg}


@app.get("/api/configs")
def api_list_configs():
    return list_configs()


@app.get("/api/configs/{worker_id}")
def api_get_config(worker_id: str):
    cfg = load_config(worker_id)
    if cfg is None:
        raise HTTPException(404, "no config")
    return cfg


@app.put("/api/configs/{worker_id}")
async def api_put_config(worker_id: str, cfg: dict):
    if not isinstance(cfg, dict):
        raise HTTPException(400, "config must be a dict")
    save_config(worker_id, cfg)
    store.insert_event(worker_id, "config.update", "operator", {"keys": list(cfg.keys())})
    if worker_id in fleet.workers:
        await fleet.push_config(worker_id)
    return {"ok": True}


@app.get("/api/trades")
def api_trades(worker_id: str = None, status: str = None, limit: int = 100):
    return store.list_trades(worker_id=worker_id, status=status, limit=limit)


@app.get("/api/portfolio")
def api_portfolio():
    return store.portfolio_stats()


@app.get("/api/logs")
def api_logs(worker_id: str = None, level: str = None, limit: int = 200):
    return store.list_logs(worker_id=worker_id, level=level, limit=limit)


# ═══════════════════════════════════════════════════════════════
# WEB UI
# ═══════════════════════════════════════════════════════════════
app.mount("/web", StaticFiles(directory=str(WEB_DIR)), name="web")


@app.get("/")
def root():
    return FileResponse(WEB_DIR / "index.html")


# ═══════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"╔══════════════════════════════════════╗")
    print(f"║      🤖 JINNI GRID — MOTHER          ║")
    print(f"╚══════════════════════════════════════╝")
    print(f"  Dashboard:  http://localhost:{PORT}")
    print(f"  WS (worker): ws://<host>:{PORT}/ws")
    print()
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")