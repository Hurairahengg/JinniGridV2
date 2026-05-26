"""
main.py — VM Worker runtime.

config.json fields:
  worker_id   — string, must match a file in mother/configs/<worker_id>.json
  mother_url  — base URL, e.g. "http://192.168.3.232:5000"
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
import platform
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import websockets

import live_engine


# ─── force UTF-8 stdout on Windows (kills the cp1252 crash) ───────
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


CONFIG_FILE        = Path(__file__).parent / "config.json"
LOG_FILE           = Path(__file__).parent / "worker.log"
PROTOCOL_VERSION   = 1
HEARTBEAT_INTERVAL = 5
RECONNECT_BACKOFF  = [1, 2, 5, 10, 30, 60]
WS_PING_INTERVAL   = 20
WS_PING_TIMEOUT    = 20
MAX_BUFFER         = 1000


# ─── local logger (UTF-8 everywhere) ─────────────────────────────
def setup_local_logger():
    log = logging.getLogger("worker")
    log.handlers.clear()
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(fh); log.addHandler(sh)
    log.propagate = False
    return log


def build_ws_url(mother_url):
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
        self.log.info("=" * 60)
        self.log.info(f"JINNI WORKER booting")
        self.log.info(f"  worker_id      = {self.worker_id}")
        self.log.info(f"  ws_url         = {self.ws_url}")
        self.log.info(f"  auto_start     = {self.auto_start}")
        self.log.info(f"  python         = {platform.python_version()}")
        self.log.info(f"  os             = {platform.system()} {platform.release()}")
        self.log.info(f"  fallback_cfg   = {'YES' if self.fallback_cfg else 'NO'}")
        self.log.info("=" * 60)

        self.active_config   = None
        self.strategy_thread = None
        self.strategy_stop   = threading.Event()
        self.engine_ref      = None
        self.engine_state    = "STOPPED"

        self.event_q         = queue.Queue()
        self.outbound_buffer = deque(maxlen=MAX_BUFFER)
        self.shutdown_evt    = None
        self.version         = "0.1.0"

    # ─── unified logging: local file/stdout AND ship to Mother ──
    def _log(self, level, msg, ctx=None):
        getattr(self.log, level.lower(), self.log.info)(msg)
        try:
            self.event_q.put_nowait({
                "type": "log",
                "payload": {"level": level.upper(), "message": msg, "context": ctx or {}}
            })
        except queue.Full:
            pass

    def _err(self, msg, ctx=None):
        self.log.error(msg)
        try:
            self.event_q.put_nowait({
                "type": "error",
                "payload": {"message": msg, "context": ctx or {}}
            })
        except queue.Full:
            pass

    # ─── event callback from strategy thread ────────────────────
    def on_event(self, type_, payload):
        if type_ == "engine.ready":
            self.engine_ref = payload.get("engine")
            self._log("INFO", "engine.ready -- handle published, heartbeats now live")
            return
        if type_ == "warmup.done":
            self._log("INFO", f"warmup.done -- bars={payload.get('bars')} in_memory={payload.get('in_memory')}")
        try:
            self.event_q.put_nowait({"type": type_, "payload": payload})
        except queue.Full:
            pass

    # ─── strategy lifecycle ─────────────────────────────────────
    def start_strategy(self):
        if self.strategy_thread and self.strategy_thread.is_alive():
            self._log("WARN", "start_strategy: already running")
            return False
        if self.active_config is None:
            self._err("start_strategy: no active config (Mother hasn't sent one and no fallback)")
            return False

        cfg = self.active_config
        self._log("INFO", f"start_strategy -- symbol={cfg.get('symbol')} "
                          f"brick={cfg.get('brick_size')} streak={cfg.get('streak_size')} "
                          f"sl={cfg.get('fixed_sl_points')} tp_after={cfg.get('tp_close_after')}")

        self.strategy_stop.clear()
        self.engine_state = "STARTING"
        self.engine_ref = None

        def _runner():
            try:
                self._log("INFO", "strategy thread: entering live_engine.run()")
                live_engine.run(self.active_config, self.on_event, self.strategy_stop)
                self.engine_state = "STOPPED"
                self._log("INFO", "strategy thread: live_engine.run() returned cleanly")
            except Exception as e:
                self.engine_state = "ERROR"
                self._err(f"strategy thread CRASHED: {e}", ctx={"exception": str(e)})

        self.strategy_thread = threading.Thread(target=_runner, daemon=True, name="strategy")
        self.strategy_thread.start()
        self.engine_state = "RUNNING"
        self._log("INFO", "strategy thread launched")
        return True

    def stop_strategy(self, timeout=15):
        if not self.strategy_thread or not self.strategy_thread.is_alive():
            self.engine_state = "STOPPED"
            return True
        self._log("INFO", f"stop_strategy -- signaling thread to stop (timeout={timeout}s)")
        self.strategy_stop.set()
        self.strategy_thread.join(timeout=timeout)
        if self.strategy_thread.is_alive():
            self._err("strategy thread did NOT stop within timeout -- leaving as daemon")
            return False
        self.engine_state = "STOPPED"
        self.engine_ref = None
        self._log("INFO", "strategy thread stopped")
        return True

    def restart_strategy(self):
        self._log("INFO", "restart_strategy -- stopping then starting")
        self.stop_strategy()
        time.sleep(1)
        return self.start_strategy()

    # ─── ws supervisor ──────────────────────────────────────────
    async def ws_supervisor(self):
        backoff_i = 0
        while not self.shutdown_evt.is_set():
            try:
                self._log("INFO", f"connecting to {self.ws_url}...")
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

            if self.shutdown_evt.is_set():
                break
            delay = RECONNECT_BACKOFF[min(backoff_i, len(RECONNECT_BACKOFF) - 1)]
            backoff_i += 1
            self.log.info(f"reconnecting in {delay}s (attempt #{backoff_i})...")
            try:
                await asyncio.wait_for(self.shutdown_evt.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _session(self, ws):
        acct = live_engine.get_account_snapshot()
        self._log("INFO", f"sending hello -- broker={acct.get('broker')} account={acct.get('account')}")
        hello = self._envelope("hello", {
            "worker_id": self.worker_id,
            "broker":    acct.get("broker"),
            "account":   acct.get("account"),
            "version":   self.version,
        })
        await ws.send(json.dumps(hello))

        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
        except asyncio.TimeoutError:
            self._err("hello.ack timeout (Mother didn't respond in 10s)")
            return
        msg = json.loads(raw)
        if msg.get("type") != "hello.ack":
            self._err(f"expected hello.ack, got {msg.get('type')}")
            return

        new_cfg = (msg.get("payload") or {}).get("config") or {}
        if new_cfg:
            self.active_config = new_cfg
            self._log("INFO", f"active config received from Mother ({len(new_cfg)} keys)")
        elif self.fallback_cfg and not self.active_config:
            self.active_config = self.fallback_cfg
            self._log("WARN", "Mother sent empty config -- using local fallback_config")

        self._log("INFO", "session established with Mother")

        if self.auto_start and self.engine_state == "STOPPED" and self.active_config:
            self._log("INFO", "auto_start enabled -- starting strategy now")
            self.start_strategy()

        await self._flush_buffer(ws)

        tasks = [
            asyncio.create_task(self._recv_loop(ws), name="recv"),
            asyncio.create_task(self._send_loop(ws), name="send"),
            asyncio.create_task(self._heartbeat_loop(ws), name="hb"),
        ]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in pending:
                t.cancel()
            for t in done:
                exc = t.exception()
                if exc and not isinstance(exc, asyncio.CancelledError):
                    self.log.warning(f"session task {t.get_name()} ended: {exc}")
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    async def _recv_loop(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            await self._handle_command(ws, msg)

    async def _handle_command(self, ws, msg):
        mtype   = msg.get("type")
        payload = msg.get("payload") or {}

        if mtype == "cmd.start":
            ok = self.start_strategy()
            self._log("INFO", f"cmd.start -> ok={ok}")
        elif mtype == "cmd.stop":
            ok = self.stop_strategy()
            self._log("INFO", f"cmd.stop -> ok={ok}")
        elif mtype == "cmd.restart":
            ok = self.restart_strategy()
            self._log("INFO", f"cmd.restart -> ok={ok}")
        elif mtype == "cmd.reload_config":
            new_cfg = payload.get("config") or {}
            self.active_config = new_cfg
            self._log("INFO", f"cmd.reload_config -- {len(new_cfg)} keys")
            if self.strategy_thread and self.strategy_thread.is_alive():
                self._log("INFO", "strategy running -- restarting to apply new config")
                self.restart_strategy()
        elif mtype == "cmd.ping":
            await ws.send(json.dumps(self._envelope("ack", {"of": "cmd.ping"})))
            self._log("INFO", "cmd.ping -> ack")
        else:
            self._log("WARN", f"unknown command: {mtype}")

    async def _send_loop(self, ws):
        while True:
            ev = await asyncio.get_event_loop().run_in_executor(None, self.event_q.get)
            envelope = self._envelope(ev["type"], ev["payload"])
            try:
                await ws.send(json.dumps(envelope, default=str))
            except Exception as e:
                self.log.warning(f"send failed, buffering event {ev['type']}: {e}")
                self._buffer_event(envelope)
                raise

    def _buffer_event(self, envelope):
        if len(self.outbound_buffer) >= MAX_BUFFER:
            for i, e in enumerate(self.outbound_buffer):
                if e.get("type") in ("log", "heartbeat"):
                    del self.outbound_buffer[i]; break
            else:
                self.outbound_buffer.popleft()
        self.outbound_buffer.append(envelope)

    async def _flush_buffer(self, ws):
        if not self.outbound_buffer:
            return
        self.log.info(f"flushing {len(self.outbound_buffer)} buffered events...")
        while self.outbound_buffer:
            ev = self.outbound_buffer.popleft()
            try:
                await ws.send(json.dumps(ev, default=str))
            except Exception as e:
                self.outbound_buffer.appendleft(ev)
                self.log.warning(f"flush failed: {e}")
                return

    async def _heartbeat_loop(self, ws):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            envelope = self._envelope("heartbeat", self._build_heartbeat())
            try:
                await ws.send(json.dumps(envelope, default=str))
            except Exception as e:
                self.log.warning(f"heartbeat send failed: {e}")
                raise

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
            self._log("INFO", "preemptive start with fallback_config (Mother not yet contacted)")
            self.start_strategy()
        try:
            await self.ws_supervisor()
        except asyncio.CancelledError:
            pass
        finally:
            self._log("INFO", "shutdown initiated, stopping strategy...")
            self.stop_strategy(timeout=10)
            self.log.info("worker exit")


def main():
    if not CONFIG_FILE.exists():
        print(f"FATAL: {CONFIG_FILE} not found"); sys.exit(1)
    with open(CONFIG_FILE, encoding="utf-8") as f:
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