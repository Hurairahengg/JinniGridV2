"""
main.py — Mother control plane.

FastAPI app exposing:
  • /ws/worker/{worker_id}   — persistent worker channel
  • /ws/ui                   — dashboard live feed
  • REST /api/*              — fleet ops, configs, trades, logs
  • /                        — serves web/index.html

Run:   uvicorn main:app --host 0.0.0.0 --port 8080
"""

import os
import json
import asyncio
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import store
from fleet import fleet, load_config, save_config, list_configs, validate_token

WEB_DIR = Path(__file__).parent / "web"

app = FastAPI(title="Jinni Grid — Mother", version="0.1.0")


# ═══════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════
@app.on_event("startup")
async def _startup():
    store.init_db()
    asyncio.create_task(fleet.heartbeat_loop())


# ═══════════════════════════════════════════════════════════════
# WEBSOCKETS
# ═══════════════════════════════════════════════════════════════
@app.websocket("/ws/worker/{worker_id}")
async def ws_worker(ws: WebSocket, worker_id: str):
    await ws.accept()

    # ─── expect hello first ───
    try:
        first = await asyncio.wait_for(ws.receive_json(), timeout=10)
    except Exception:
        await ws.close(code=4001, reason="hello timeout"); return

    if first.get("type") != "hello":
        await ws.close(code=4002, reason="hello required"); return

    token = (first.get("payload") or {}).get("token")
    if not validate_token(worker_id, token):
        await ws.close(code=4003, reason="auth failed"); return

    await fleet.attach(worker_id, ws, first.get("payload") or {})
    await fleet.hello_ack(worker_id, ws)

    # ─── main message loop ───
    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("worker_id") and msg["worker_id"] != worker_id:
                continue   # ignore spoofed
            await fleet.handle_message(worker_id, msg)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        store.insert_log(worker_id, "ERROR", f"ws loop crashed: {e}")
    finally:
        await fleet.detach(worker_id)


@app.websocket("/ws/ui")
async def ws_ui(ws: WebSocket):
    await ws.accept()
    fleet.attach_ui(ws)
    try:
        while True:
            await ws.receive_text()   # UI -> server: ignored for now (pings)
    except WebSocketDisconnect:
        pass
    finally:
        fleet.detach_ui(ws)


# ═══════════════════════════════════════════════════════════════
# REST API
# ═══════════════════════════════════════════════════════════════

# ── fleet ───────────────────────────────────────────────────────
@app.get("/api/workers")
def api_workers():
    workers = store.list_workers()
    connected = set(fleet.workers.keys())
    for w in workers:
        w["connected"] = w["id"] in connected
    return workers


@app.get("/api/workers/{worker_id}")
def api_worker_detail(worker_id: str):
    w = store.get_worker(worker_id)
    if not w:
        raise HTTPException(404, "not found")
    w["connected"] = worker_id in fleet.workers
    w["config"] = load_config(worker_id)
    return w


# ── commands ────────────────────────────────────────────────────
@app.post("/api/workers/{worker_id}/start")
async def api_start(worker_id: str):
    ok, msg = await fleet.send_command(worker_id, "cmd.start")
    return {"ok": ok, "msg": msg}


@app.post("/api/workers/{worker_id}/stop")
async def api_stop(worker_id: str):
    ok, msg = await fleet.send_command(worker_id, "cmd.stop")
    return {"ok": ok, "msg": msg}


@app.post("/api/workers/{worker_id}/restart")
async def api_restart(worker_id: str):
    ok, msg = await fleet.send_command(worker_id, "cmd.restart")
    return {"ok": ok, "msg": msg}


@app.post("/api/workers/{worker_id}/ping")
async def api_ping(worker_id: str):
    ok, msg = await fleet.send_command(worker_id, "cmd.ping")
    return {"ok": ok, "msg": msg}


@app.post("/api/workers/{worker_id}/reload_config")
async def api_reload_config(worker_id: str):
    ok, msg = await fleet.push_config(worker_id)
    return {"ok": ok, "msg": msg}


# ── configs ─────────────────────────────────────────────────────
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
    if not isinstance(cfg, dict) or "auth_token" not in cfg:
        raise HTTPException(400, "config must be a dict with auth_token")
    save_config(worker_id, cfg)
    store.insert_event(worker_id, "config.update", "operator", {"keys": list(cfg.keys())})
    # auto-push if connected
    if worker_id in fleet.workers:
        await fleet.push_config(worker_id)
    return {"ok": True}


# ── trades ──────────────────────────────────────────────────────
@app.get("/api/trades")
def api_trades(worker_id: str = None, status: str = None, limit: int = 100):
    return store.list_trades(worker_id=worker_id, status=status, limit=limit)


@app.get("/api/portfolio")
def api_portfolio():
    return store.portfolio_stats()


# ── logs ────────────────────────────────────────────────────────
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