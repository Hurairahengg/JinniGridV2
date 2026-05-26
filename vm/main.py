"""
main.py — VM Worker runtime.

config.json fields:
  worker_id   — string, must match a file in mother/configs/<worker_id>.json
  mother_url  — base URL, e.g. "http://192.168.3.232:5000"  (scheme converted automatically)
  auto_start  — bool
  fallback_config — optional strategy dict used if Mother is offline at boot

Run:  python main.py
"""

import os
import sys
import json
import time
import asyncio
import logging
import logging.handlers
import threading
import queue
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import websockets

import live_engine


CONFIG_FILE        = Path(__file__).parent / "config.json"
LOG_FILE           = Path(__file__).parent / "worker.log"
PROTOCOL_VERSION   = 1
HEARTBEAT_INTERVAL = 5
RECONNECT_BACKOFF  = [1, 2, 5, 10, 30, 60]
WS_PING_INTERVAL   = 20
WS_PING_TIMEOUT    = 20
MAX_BUFFER         = 1000


# ─── local logger ─────────────────────────────────────────────────
def setup_local_logger():
    log = logging.getLogger("worker")
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt)
    log.addHandler(fh); log.addHandler(sh)
    log.propagate = False
    return log


# ─── URL builder: turn mother_url base into a clean ws:// or wss:// URL ─
def build_ws_url(mother_url):
    """
    Accepts:   http://host:5000   https://host:5000   ws://host:5000   wss://host:5000
               (with or without trailing slash, with or without /ws path)
    Returns:   ws://host:5000/ws  or  wss://host:5000/ws
    """
    u = urlparse(mother_url.strip().rstrip("/"))
    scheme_map = {"http": "ws", "https": "wss", "ws": "ws", "wss": "wss"}
    scheme = scheme_map.get(u.scheme.lower())
    if not scheme:
        raise ValueError(f"unsupported scheme: {u.scheme}")
    path = u.path if u.path.endswith("/ws") else "/ws"
    return urlunparse((scheme, u.netloc, path, "", "", ""))


# ═════════════════════════════════════════════════════════════════
# WORKER
# ═════════════════════════════════════════════════════════════════
class Worker:
    def __init__(self, local_cfg):
        self.local_cfg     = local_cfg
        self.worker_id     = local_cfg["worker_id"]
        self.ws_url        = build_ws_url(local_cfg["mother_url"])
        self.auto_start    = local_cfg.get("auto_start", False)
        self.fallback_cfg  = local_cfg.get("fallback_config", {})

        self.log = setup_local_logger()
        self.log.info(f"worker_id={self.worker_id}  ws_url={self.ws_url}")

        self.active_config   = None
        self.strategy_thread = None
        self.strategy_stop   = threading.Event()
        self.engine_ref      = None
        self.engine_state    = "STOPPED"

        self.event_q         = queue.Queue()
        self.outbound_buffer = deque(maxlen=MAX_BUFFER)
        self.shutdown_evt    = None
        self.version         = "0.1.0"

    # ─── event callback (called from strategy thread) ───────────
    def on_event(self, type_, payload):
        if type_ == "engine.ready":
            self.engine_ref = payload.get("engine"); return
        try:
            self.event_q.put_nowait({"type": type_, "payload": payload})
        except queue.Full:
            pass

    # ─── strategy thread lifecycle ──────────────────────────────
    def start_strategy(self):
        if self.strategy_thread and self.strategy_thread.is_alive():
            self.log.warning("start_strategy: already running"); return False
        if self.active_config is None:
            self.log.error("start_strategy: no active config"); return False

        self.strategy_stop.clear()
        self.engine_state = "STARTING"; self.engine_ref = None

        def _runner():
            try:
                live_engine.run(self.active_config, self.on_event, self.strategy_stop)
                self.engine_state = "STOPPED"
                self.on_event("log", {"level": "INFO", "message": "strategy thread exited cleanly"})
            except Exception as e:
                self.engine_state = "ERROR"
                self.on_event("error", {"message": f"strategy thread crashed: {e}"})

        self.strategy_thread = threading.Thread(target=_runner, daemon=True, name="strategy")
        self.strategy_thread.start()
        self.engine_state = "RUNNING"
        self.log.info("strategy thread started")
        return True

    def stop_strategy(self, timeout=15):
        if not self.strategy_thread or not self.strategy_thread.is_alive():
            self.engine_state = "STOPPED"; return True
        self.log.info("stopping strategy thread…")
        self.strategy_stop.set()
        self.strategy_thread.join(timeout=timeout)
        if self.strategy_thread.is_alive():
            self.log.error("strategy thread did NOT stop within timeout — left as daemon")
            return False
        self.engine_state = "STOPPED"; self.engine_ref = None
        return True

    def restart_strategy(self):
        self.stop_strategy(); time.sleep(1); return self.start_strategy()

    # ─── ws supervisor (reconnect forever) ──────────────────────
    async def ws_supervisor(self):
        backoff_i = 0
        while not self.shutdown_evt.is_set():
            try:
                self.log.info(f"connecting to {self.ws_url}…")
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=WS_PING_INTERVAL,
                    ping_timeout=WS_PING_TIMEOUT,
                    max_size=10_000_000,
                ) as ws:
                    backoff_i = 0
                    await self._session(ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.warning(f"ws connect/loop error: {e}")

            if self.shutdown_evt.is_set(): break
            delay = RECONNECT_BACKOFF[min(backoff_i, len(RECONNECT_BACKOFF) - 1)]
            backoff_i += 1
            self.log.info(f"reconnecting in {delay}s (attempt #{backoff_i})…")
            try:
                await asyncio.wait_for(self.shutdown_evt.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _session(self, ws):
        # ─── hello (identifies who we are) ───
        acct = live_engine.get_account_snapshot()
        hello = self._envelope("hello", {
            "worker_id": self.worker_id,
            "broker":    acct.get("broker"),
            "account":   acct.get("account"),
            "version":   self.version,
        })
        await ws.send(json.dumps(hello))

        # ─── wait for hello.ack ───
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
        except asyncio.TimeoutError:
            self.log.error("hello.ack timeout"); return
        msg = json.loads(raw)
        if msg.get("type") != "hello.ack":
            self.log.error(f"expected hello.ack, got {msg.get('type')}"); return

        new_cfg = (msg.get("payload") or {}).get("config") or {}
        if new_cfg:
            self.active_config = new_cfg
            self.log.info("active config received from Mother")
        elif self.fallback_cfg and not self.active_config:
            self.active_config = self.fallback_cfg
            self.log.warning("Mother sent empty config — using local fallback_config")

        self.log.info("connected, session established")

        if self.auto_start and self.engine_state == "STOPPED" and self.active_config:
            self.start_strategy()

        await self._flush_buffer(ws)

        tasks = [
            asyncio.create_task(self._recv_loop(ws), name="recv"),
            asyncio.create_task(self._send_loop(ws), name="send"),
            asyncio.create_task(self._heartbeat_loop(ws), name="hb"),
        ]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in pending: t.cancel()
            for t in done:
                exc = t.exception()
                if exc and not isinstance(exc, asyncio.CancelledError):
                    self.log.warning(f"session task {t.get_name()} ended: {exc}")
        finally:
            for t in tasks:
                if not t.done(): t.cancel()

    async def _recv_loop(self, ws):
        async for raw in ws:
            try:    msg = json.loads(raw)
            except Exception: continue
            await self._handle_command(ws, msg)

    async def _handle_command(self, ws, msg):
        mtype   = msg.get("type")
        payload = msg.get("payload") or {}

        if mtype == "cmd.start":
            ok = self.start_strategy(); self.log.info(f"cmd.start → {ok}")
        elif mtype == "cmd.stop":
            ok = self.stop_strategy();  self.log.info(f"cmd.stop → {ok}")
        elif mtype == "cmd.restart":
            ok = self.restart_strategy(); self.log.info(f"cmd.restart → {ok}")
        elif mtype == "cmd.reload_config":
            new_cfg = payload.get("config") or {}
            self.active_config = new_cfg
            self.log.info("config reloaded from Mother")
            if self.strategy_thread and self.strategy_thread.is_alive():
                self.log.info("strategy running — restarting to apply new config")
                self.restart_strategy()
        elif mtype == "cmd.ping":
            await ws.send(json.dumps(self._envelope("ack", {"of": "cmd.ping"})))
        else:
            self.log.warning(f"unknown command: {mtype}")

    async def _send_loop(self, ws):
        while True:
            ev = await asyncio.get_event_loop().run_in_executor(None, self.event_q.get)
            envelope = self._envelope(ev["type"], ev["payload"])
            try:
                await ws.send(json.dumps(envelope, default=str))
            except Exception as e:
                self.log.warning(f"send failed, buffering event {ev['type']}: {e}")
                self._buffer_event(envelope); raise

    def _buffer_event(self, envelope):
        if len(self.outbound_buffer) >= MAX_BUFFER:
            for i, e in enumerate(self.outbound_buffer):
                if e.get("type") in ("log", "heartbeat"):
                    del self.outbound_buffer[i]; break
            else:
                self.outbound_buffer.popleft()
        self.outbound_buffer.append(envelope)

    async def _flush_buffer(self, ws):
        if not self.outbound_buffer: return
        self.log.info(f"flushing {len(self.outbound_buffer)} buffered events…")
        while self.outbound_buffer:
            ev = self.outbound_buffer.popleft()
            try:    await ws.send(json.dumps(ev, default=str))
            except Exception as e:
                self.outbound_buffer.appendleft(ev)
                self.log.warning(f"flush failed: {e}"); return

    async def _heartbeat_loop(self, ws):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            envelope = self._envelope("heartbeat", self._build_heartbeat())
            try:    await ws.send(json.dumps(envelope, default=str))
            except Exception as e:
                self.log.warning(f"heartbeat send failed: {e}"); raise

    def _build_heartbeat(self):
        acct = live_engine.get_account_snapshot()
        eng  = self.engine_ref.status_snapshot() if self.engine_ref else {}
        return {
            "state":           self._public_state(),
            "balance":         acct.get("balance"),
            "equity":          acct.get("equity"),
            "open_positions":  acct.get("open_positions"),
            "mem_bars":        eng.get("mem_bars", 0),
            "last_bar_ts":     eng.get("last_bar_ts"),
            "engine_state":    eng.get("engine_state"),
            "live_bars_seen":  eng.get("live_bars_seen", 0),
            "open_ticket":     eng.get("open_ticket"),
        }

    def _public_state(self):
        if self.engine_state == "RUNNING":   return "RUNNING"
        if self.engine_state == "ERROR":     return "ERROR"
        if self.engine_state == "STARTING":  return "RUNNING"
        return "IDLE"

    def _envelope(self, type_, payload):
        return {
            "v": PROTOCOL_VERSION,
            "type": type_,
            "worker_id": self.worker_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "msg_id": str(uuid.uuid4()),
            "payload": payload,
        }

    async def run(self):
        self.shutdown_evt = asyncio.Event()
        if self.auto_start and self.fallback_cfg and self.engine_state == "STOPPED":
            self.active_config = self.fallback_cfg
            self.log.info("preemptively starting strategy with fallback_config")
            self.start_strategy()
        try:
            await self.ws_supervisor()
        except asyncio.CancelledError:
            pass
        finally:
            self.log.info("shutdown initiated, stopping strategy…")
            self.stop_strategy(timeout=10)
            self.log.info("worker exit")


# ─── boot ─────────────────────────────────────────────────────────
def main():
    if not CONFIG_FILE.exists():
        print(f"FATAL: {CONFIG_FILE} not found"); sys.exit(1)
    with open(CONFIG_FILE) as f:
        cfg = json.load(f)
    for required in ("worker_id", "mother_url"):
        if required not in cfg:
            print(f"FATAL: config.json missing '{required}'"); sys.exit(1)
    worker = Worker(cfg)
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        print("\nworker stopped by user")


if __name__ == "__main__":
    main()