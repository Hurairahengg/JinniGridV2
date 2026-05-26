"""
fleet.py — Fleet manager: live worker registry + event router + command dispatch.
No auth. Worker identity = the worker_id it claims in hello.
Mother only accepts worker_ids that have a config file in main/configs/.
"""

import json
import uuid
import asyncio
from datetime import datetime, timezone
from pathlib import Path

import store
import validator        # YOUR existing file, untouched
import telegram_bot     # YOUR existing file, untouched


PROTOCOL_VERSION  = 1
HEARTBEAT_TIMEOUT = 25
HEARTBEAT_TICK    = 5
CONFIGS_DIR       = Path(__file__).parent / "configs"


# ─── config files (per-VM JSON in main/configs/) ────────────────
def config_path(worker_id):
    return CONFIGS_DIR / f"{worker_id}.json"

def load_config(worker_id):
    p = config_path(worker_id)
    if not p.exists():
        return None
    with open(p) as f:
        return json.load(f)

def save_config(worker_id, cfg):
    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    with open(config_path(worker_id), "w") as f:
        json.dump(cfg, f, indent=2)

def list_configs():
    if not CONFIGS_DIR.exists():
        return []
    return sorted([p.stem for p in CONFIGS_DIR.glob("*.json")])


# ─── fleet manager ──────────────────────────────────────────────
BAR_HISTORY_LEN = 300   # ring buffer per worker for charts

class FleetManager:
    def __init__(self):
        self.workers = {}          # worker_id -> WebSocket
        self.ui_clients = set()
        self.bar_history = {}      # worker_id -> deque of bars
        self.recent_trade_markers = {}  # worker_id -> deque of {ts, type, price, dir}

    async def attach(self, worker_id, ws, hello_payload):
        if worker_id in self.workers:
            old = self.workers[worker_id]
            try:
                await old.close(code=4000, reason="superseded")
            except Exception:
                pass
        self.workers[worker_id] = ws
        store.upsert_worker(
            worker_id,
            broker=hello_payload.get("broker"),
            account=hello_payload.get("account"),
            version=hello_payload.get("version"),
            state="IDLE",
        )
        store.set_worker_state(worker_id, "IDLE")
        store.insert_event(worker_id, "worker.connect", "system", hello_payload)
        await self._broadcast_ui({"type": "worker.update", "worker_id": worker_id})
        try:
            telegram_bot.send_status(f"🟢 Worker {worker_id} connected")
        except Exception:
            pass

    async def detach(self, worker_id):
        self.workers.pop(worker_id, None)
        store.set_worker_state(worker_id, "OFFLINE")
        store.insert_event(worker_id, "worker.disconnect", "system")
        await self._broadcast_ui({"type": "worker.update", "worker_id": worker_id})
        try:
            telegram_bot.send_status(f"🔴 Worker {worker_id} disconnected")
        except Exception:
            pass
    def _add_trade_marker(self, worker_id, trade, marker_type):
        from collections import deque
        buf = self.recent_trade_markers.setdefault(worker_id, deque(maxlen=100))
        buf.append({
            "ts":    trade.get("entry_time") if marker_type == "open" else trade.get("exit_time"),
            "type":  marker_type,
            "dir":   trade.get("dir"),
            "price": trade.get("actual_entry") if marker_type == "open" else trade.get("actual_exit"),
            "net_pnl": trade.get("net_pnl"),
            "ticket": trade.get("ticket"),
        })
        
    async def handle_message(self, worker_id, msg):
        mtype   = msg.get("type")
        payload = msg.get("payload", {}) or {}

        if mtype == "heartbeat":
            store.update_heartbeat(worker_id, payload)
            self._add_trade_marker(worker_id, payload, marker_type="open")
            await self._broadcast_ui({"type": "heartbeat", "worker_id": worker_id, "payload": payload})

        elif mtype == "trade.opened":
            store.insert_open_trade(worker_id, payload)
            self._add_trade_marker(worker_id, payload, marker_type="close")
            store.insert_event(worker_id, "trade.opened", "system", {"ticket": payload.get("ticket")})
            try:    telegram_bot.send_signal(payload)
            except Exception as e: store.insert_log(worker_id, "ERROR", f"telegram send_signal failed: {e}")
            await self._broadcast_ui({"type": "trade.opened", "worker_id": worker_id, "payload": payload})

        elif mtype == "trade.closed":
            verdict = {"match": False, "reason": "no_window", "expected_pnl": 0.0,
                       "pnl_diff": 0.0, "pnl_diff_pct": 0.0}
            try:
                if payload.get("bars_window") and payload.get("signal_idx_in_window") is not None:
                    verdict = validator.validate_trade(payload)
            except Exception as e:
                verdict["reason"] = f"validator_exception: {e}"
                store.insert_log(worker_id, "ERROR", f"validator failed: {e}")
            store.close_trade(worker_id, payload, verdict)
            store.insert_event(worker_id, "trade.closed", "system",
                               {"ticket": payload.get("ticket"), "net_pnl": payload.get("net_pnl"),
                                "match": verdict.get("match")})
            try:    telegram_bot.send_close(payload, verdict)
            except Exception as e: store.insert_log(worker_id, "ERROR", f"telegram send_close failed: {e}")
            await self._broadcast_ui({"type": "trade.closed", "worker_id": worker_id,
                                      "payload": payload, "verdict": verdict})
        elif mtype == "bar":
            from collections import deque
            buf = self.bar_history.setdefault(worker_id, deque(maxlen=BAR_HISTORY_LEN))
            buf.append(payload.get("bar"))
            await self._broadcast_ui({"type": "bar", "worker_id": worker_id, "payload": payload})
            
        elif mtype == "log":
            store.insert_log(worker_id, payload.get("level", "INFO"),
                             payload.get("message", ""), payload.get("context"))
            await self._broadcast_ui({"type": "log", "worker_id": worker_id, "payload": payload})

        elif mtype == "error":
            store.insert_log(worker_id, "ERROR", payload.get("message", ""), payload.get("context"))
            try:    telegram_bot.send_error(f"[{worker_id}] {payload.get('message','')}")
            except Exception: pass
            await self._broadcast_ui({"type": "error", "worker_id": worker_id, "payload": payload})
        elif mtype == "warmup.done":
            store.insert_event(worker_id, "warmup.done", "system", payload)
            store.insert_log(worker_id, "INFO",
                             f"warmup done: {payload.get('bars')} bars, "
                             f"{payload.get('in_memory')} in memory")
            await self._broadcast_ui({"type": "warmup.done", "worker_id": worker_id, "payload": payload})

        elif mtype == "position.resync":
            store.insert_event(worker_id, "position.resync", "system", payload)
            await self._broadcast_ui({"type": "position.resync", "worker_id": worker_id, "payload": payload})

        elif mtype == "ack":
            pass
        else:
            store.insert_log(worker_id, "WARN", f"unknown message type: {mtype}")

    async def send_command(self, worker_id, cmd_type, payload=None, actor="operator"):
        ws = self.workers.get(worker_id)
        if ws is None:
            return False, "worker offline"
        msg = _envelope(cmd_type, worker_id, payload or {})
        try:
            await ws.send_json(msg)
        except Exception as e:
            return False, f"send failed: {e}"
        store.insert_event(worker_id, cmd_type, actor, payload or {})
        return True, "ok"

    async def push_config(self, worker_id, actor="operator"):
        cfg = load_config(worker_id)
        if cfg is None:
            return False, "no config"
        return await self.send_command(worker_id, "cmd.reload_config", {"config": cfg}, actor=actor)

    async def hello_ack(self, worker_id, ws):
        cfg = load_config(worker_id) or {}
        await ws.send_json(_envelope("hello.ack", worker_id, {
            "config": cfg,
            "server_time": datetime.now(timezone.utc).isoformat(),
        }))

    def attach_ui(self, ws):
        self.ui_clients.add(ws)

    def detach_ui(self, ws):
        self.ui_clients.discard(ws)

    async def _broadcast_ui(self, msg):
        dead = []
        for ws in list(self.ui_clients):
            try:    await ws.send_json(msg)
            except Exception: dead.append(ws)
        for ws in dead:
            self.ui_clients.discard(ws)

    async def heartbeat_loop(self):
        while True:
            try:
                now = datetime.now(timezone.utc)
                for w in store.list_workers():
                    if w["state"] in ("OFFLINE", "DEAD"):
                        continue
                    lh = w.get("last_heartbeat")
                    if lh is None:
                        continue
                    last = datetime.fromisoformat(lh) if isinstance(lh, str) else lh
                    if last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    if (now - last).total_seconds() > HEARTBEAT_TIMEOUT:
                        store.set_worker_state(w["id"], "DEAD")
                        store.insert_event(w["id"], "worker.dead", "system", {"last_heartbeat": lh})
                        try:    telegram_bot.send_error(f"⚠️ Worker {w['id']} heartbeat lost")
                        except Exception: pass
                        await self._broadcast_ui({"type": "worker.update", "worker_id": w["id"]})
            except Exception as e:
                store.insert_log(None, "ERROR", f"heartbeat_loop error: {e}")
            await asyncio.sleep(HEARTBEAT_TICK)


def _envelope(type_, worker_id, payload):
    return {
        "v": PROTOCOL_VERSION,
        "type": type_,
        "worker_id": worker_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "msg_id": str(uuid.uuid4()),
        "payload": payload,
    }


fleet = FleetManager()