"""
Koko Live Engine — USTEC streaming + 2-streak/3-tp strategy
============================================================
- MT5 tick stream → Koko bars (8pt grid, 2x rev, clean ON)
- Warmup: pulls recent ticks, builds bars up to NOW, then goes live
- Strategy: 1:1 with backtester (streak=2, tp=3), OVERLAPPING positions allowed
- Time filter: NO new entries during UTC (GMT+0) hour 0
- Risk: scaling, $1 per $100 balance, 1pt × 1lot = $1
- BULLETPROOF TRADE MGMT: atomic close, async broker/notify, broker reconciliation
- Flask serves chart.html + JSON state on :5000
"""

import time
import queue
import threading
from datetime import datetime, timedelta, timezone

import MetaTrader5 as mt5
from flask import Flask, jsonify, send_from_directory

import telegram_bot as tg
import validator

# ============================ CONFIG ============================

SYMBOL                = "USTEC"
RANGE_SIZE            = 8.0
REV_BRICKS            = 2.0
CLEAN_MODE            = True
PRICE_DECIMALS        = 2
WARMUP_BARS           = 100
WARMUP_LOOKBACK_DAYS  = 7

# Strategy (1:1 with backtester)
STREAK_SIZE           = 2
TP_CLOSE_AFTER        = 3
FIXED_SL_POINTS       = 16.0
SLIPPAGE_POINTS       = 0.3
COMMISSION_PER_LOT    = 0.8
POINT_VALUE_PER_LOT   = 1.0

# Time filter (UTC / GMT+0)
BLOCKED_HOURS_UTC     = [0]

# Risk
STARTING_BALANCE      = 3100.0
RISK_PER_100          = 1.0
MIN_LOTS              = 0.01
MAX_LOTS              = 600.0

# Execution
LIVE_TRADING          = True
MAGIC_NUMBER          = 770808
DEVIATION_POINTS      = 20
TICK_POLL_MS          = 50
MT5_LOT_STEP          = 0.01
BROKER_RETRY_MAX      = 3
BROKER_RETRY_DELAY_S  = 0.5
RECONCILE_INTERVAL_S  = 30.0

# Web
HTTP_PORT             = 5000
CHART_MAX_BARS        = 500

# Position states
ST_OPEN    = "OPEN"
ST_CLOSING = "CLOSING"
ST_CLOSED  = "CLOSED"

# ============================ HELPERS ============================

def candle_dir(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0

def bar_hour_utc(ts):
    return int((int(ts) // 3600) % 24)

def is_blocked_hour_utc(ts=None):
    if ts is None:
        h = datetime.now(timezone.utc).hour
    else:
        h = bar_hour_utc(ts)
    return h in BLOCKED_HOURS_UTC

def round_to_step(x, step):
    if step <= 0: return x
    return max(step, round(round(x / step) * step, 8))

def tick_to_price(t):
    bid = float(t.bid) if hasattr(t, "bid") else float(t["bid"])
    ask = float(t.ask) if hasattr(t, "ask") else float(t["ask"])
    ts  = int(t.time)  if hasattr(t, "time") else int(t["time"])
    try:
        vol = float(t.volume) if hasattr(t, "volume") else float(t["volume"])
    except Exception:
        vol = 0.0
    if bid > 0 and ask > 0:
        price = (bid + ask) / 2.0
    elif bid > 0:
        price = bid
    elif ask > 0:
        price = ask
    else:
        return None
    return {"ts": ts, "price": price, "volume": vol, "bid": bid, "ask": ask}


# =================== KOKO STREAMER (1:1 range_bars.py) ===================

class KokoCandleStreamer:
    def __init__(self, range_size, price_decimals, rev_bricks, clean_mode, on_bar_close):
        self.rs           = float(range_size)
        self.pd           = price_decimals
        self.rev_bricks   = rev_bricks
        self.clean_mode   = clean_mode
        self.on_bar_close = on_bar_close
        self.trend = 0
        self.level = None
        self.bar   = None
        self.bar_count = 0
        self._last_emitted_ts = None

    def _snap(self, price):
        return round(round(price / self.rs) * self.rs, self.pd)

    def _make_bar(self, open_, high_, low_, close_):
        return {
            "time":   int(self.bar["time"]),
            "open":   round(open_, self.pd),
            "high":   round(high_, self.pd),
            "low":    round(low_, self.pd),
            "close":  round(close_, self.pd),
            "volume": round(self.bar["volume"], 2),
        }

    def _emit(self, bar_dict):
        ts = int(bar_dict["time"])
        if self._last_emitted_ts is not None and ts <= self._last_emitted_ts:
            ts = self._last_emitted_ts + 1
        bar_dict["time"] = ts
        self._last_emitted_ts = ts
        self.bar_count += 1
        self.on_bar_close(bar_dict)

    def _reset_bar(self, tick):
        self.bar = {
            "time": tick["ts"], "open": self.level, "high": self.level,
            "low": self.level, "close": self.level, "volume": 0.0,
        }

    def get_working_bar(self):
        if self.bar is None: return None
        ts = self.bar["time"]
        if self._last_emitted_ts is not None and ts <= self._last_emitted_ts:
            ts = self._last_emitted_ts + 1
        return {
            "time": int(ts),
            "open":  round(self.bar["open"],  self.pd),
            "high":  round(self.bar["high"],  self.pd),
            "low":   round(self.bar["low"],   self.pd),
            "close": round(self.bar["close"], self.pd),
            "volume": round(self.bar["volume"], 2),
        }

    def process_tick(self, tick):
        p  = tick["price"]
        v  = tick["volume"]
        rs = self.rs
        if self.bar is None:
            self.level = self._snap(p)
            self.bar = {"time": tick["ts"], "open": self.level, "high": self.level,
                        "low": self.level, "close": self.level, "volume": v}
            return
        self.bar["volume"] += v
        self.bar["close"]   = p
        self.bar["high"]    = max(self.bar["high"], p)
        self.bar["low"]     = min(self.bar["low"],  p)

        while True:
            lvl = self.level
            if self.trend == 0:
                up_t   = round(lvl + rs, self.pd)
                down_t = round(lvl - rs, self.pd)
                if p >= up_t:
                    self._emit(self._make_bar(lvl, max(self.bar["high"], up_t), self.bar["low"], up_t))
                    self.trend = 1; self.level = up_t; self._reset_bar(tick); continue
                elif p <= down_t:
                    self._emit(self._make_bar(lvl, self.bar["high"], min(self.bar["low"], down_t), down_t))
                    self.trend = -1; self.level = down_t; self._reset_bar(tick); continue
                else: break
            elif self.trend == 1:
                cont_t = round(lvl + rs, self.pd)
                rev_t  = round(lvl - self.rev_bricks * rs, self.pd)
                if p >= cont_t:
                    self._emit(self._make_bar(lvl, max(self.bar["high"], cont_t), self.bar["low"], cont_t))
                    self.level = cont_t; self._reset_bar(tick); continue
                elif p <= rev_t:
                    if self.clean_mode:
                        b_close = rev_t
                        b_open  = round(rev_t + rs, self.pd)
                        b_high  = max(self.bar["high"], lvl)
                        b_low   = min(self.bar["low"],  b_close)
                    else:
                        b_open  = lvl; b_close = rev_t
                        b_high  = self.bar["high"]; b_low = min(self.bar["low"], b_close)
                    self._emit(self._make_bar(b_open, b_high, b_low, b_close))
                    self.trend = -1; self.level = rev_t; self._reset_bar(tick); continue
                else: break
            elif self.trend == -1:
                cont_t = round(lvl - rs, self.pd)
                rev_t  = round(lvl + self.rev_bricks * rs, self.pd)
                if p <= cont_t:
                    self._emit(self._make_bar(lvl, self.bar["high"], min(self.bar["low"], cont_t), cont_t))
                    self.level = cont_t; self._reset_bar(tick); continue
                elif p >= rev_t:
                    if self.clean_mode:
                        b_close = rev_t
                        b_open  = round(rev_t - rs, self.pd)
                        b_high  = max(self.bar["high"], b_close)
                        b_low   = min(self.bar["low"],  lvl)
                    else:
                        b_open  = lvl; b_close = rev_t
                        b_high  = max(self.bar["high"], b_close); b_low = self.bar["low"]
                    self._emit(self._make_bar(b_open, b_high, b_low, b_close))
                    self.trend = 1; self.level = rev_t; self._reset_bar(tick); continue
                else: break


# ============================ STRATEGY ============================

class StrategyEngine:
    """
    Bulletproof 1:1 backtester replica.

    Invariants:
      - Every position closes exactly ONCE.
      - All mutations to (positions, trades, equity, blocked_setups) happen
        under self.lock — no exceptions.
      - Side effects (MT5 broker, telegram, validator) NEVER run under the lock
        and NEVER block the tick path.
      - SL check on every tick. TP check on bar close. Both atomic.
      - On a tick that hits SL: position state goes OPEN → CLOSING → CLOSED in
        one critical section. Subsequent ticks see ST_CLOSED and skip.
    """

    def __init__(self):
        self.lock                = threading.RLock()
        self.positions           = []          # only ST_OPEN/ST_CLOSING positions
        self.trades              = []
        self.markers             = []
        self.pending_validations = []
        self.validations         = []
        self.warmup_done         = False
        self.equity              = 0.0
        self.blocked_setups      = 0
        self._trade_id_seq       = 0
        self._validate_fn        = None

        # Async workers
        self._broker_q  = queue.Queue()
        self._notify_q  = queue.Queue()
        self._running   = True

        threading.Thread(target=self._broker_worker, daemon=True, name="broker").start()
        threading.Thread(target=self._notify_worker, daemon=True, name="notify").start()

    # ── PUBLIC: lifecycle ──────────────────────────────────────

    def shutdown(self):
        self._running = False

    # ── INTERNAL: id / sizing ──────────────────────────────────

    def _next_trade_id(self):
        # Always called under self.lock
        self._trade_id_seq += 1
        return self._trade_id_seq

    def _current_risk_dollars(self):
        balance = STARTING_BALANCE + self.equity
        if balance <= 0:
            return 0.0
        return (balance / 100.0) * RISK_PER_100

    def _calc_lots(self):
        risk = self._current_risk_dollars()
        if risk <= 0:
            return 0.0
        raw = risk / FIXED_SL_POINTS
        return max(MIN_LOTS, min(MAX_LOTS, raw))

    # ── INTERNAL: signal ───────────────────────────────────────

    def _check_signal(self, bars):
        if len(bars) < STREAK_SIZE + 1:
            return 0
        recent = bars[-(STREAK_SIZE + 1):]
        dirs   = [candle_dir(b) for b in recent]
        streak_dir = dirs[0]
        if streak_dir == 0:
            return 0
        for d in dirs[:STREAK_SIZE]:
            if d != streak_dir:
                return 0
        rev_dir = dirs[STREAK_SIZE]
        if rev_dir != -streak_dir:
            return 0
        return rev_dir

    # ── OPEN ───────────────────────────────────────────────────

    def _open_position(self, direction, signal_bar):
        """Always called under self.lock."""
        theoretical_entry = signal_bar["close"]
        if direction == 1:
            entry_price = theoretical_entry + SLIPPAGE_POINTS
            sl_price    = entry_price - FIXED_SL_POINTS
        else:
            entry_price = theoretical_entry - SLIPPAGE_POINTS
            sl_price    = entry_price + FIXED_SL_POINTS

        lots = self._calc_lots()
        if lots <= 0:
            print("[NO ENTRY] balance non-positive, skipping signal")
            return

        tid = self._next_trade_id()
        pos = {
            "id":            tid,
            "state":         ST_OPEN,
            "dir":           direction,
            "entry_price":   round(entry_price, PRICE_DECIMALS),
            "sl_price":      round(sl_price,    PRICE_DECIMALS),
            "lots":          lots,
            "entry_time":    signal_bar["time"],
            "entry_utc_h":   datetime.now(timezone.utc).hour,
            "bars_held":     0,
            "mfe_points":    0.0,
            "mae_points":    0.0,
            "risk_used":     self._current_risk_dollars(),
            "broker_ticket": None,
        }
        self.positions.append(pos)

        self.markers.append({
            "time":     signal_bar["time"],
            "position": "belowBar" if direction == 1 else "aboveBar",
            "color":    "#4caf50" if direction == 1 else "#ef5350",
            "shape":    "arrowUp" if direction == 1 else "arrowDown",
            "text":     f"#{tid} {'L' if direction == 1 else 'S'} {entry_price:.2f}",
        })

        print(f"[ENTRY #{tid}] {'LONG' if direction == 1 else 'SHORT'} @ {entry_price:.2f}  "
              f"SL={sl_price:.2f}  lots={lots:.4f}  risk=${pos['risk_used']:.2f}  "
              f"hUTC={pos['entry_utc_h']:02d}  (open={self._open_count()})")

        # Side effects — queued, NOT executed under lock
        if LIVE_TRADING:
            self._broker_q.put(("OPEN", pos["id"], dict(pos)))
        self._notify_q.put(("ENTRY", {
            "trade_id": tid, "direction": direction, "entry": entry_price, "sl": sl_price,
            "lots": lots, "bar_time": signal_bar["time"], "position_count": self._open_count(),
        }))

    def _open_count(self):
        return sum(1 for p in self.positions if p["state"] == ST_OPEN)

    # ── CLOSE (atomic) ─────────────────────────────────────────

    def _close_position(self, pos, exit_price, reason, exit_time):
        """
        ATOMIC close. Called under self.lock.

        Guards against double-close via pos["state"]. If pos is not ST_OPEN,
        this is a no-op. Once we transition to ST_CLOSING we are committed.
        """
        if pos["state"] != ST_OPEN:
            # Already closing/closed — ignore. This is the double-close guard.
            return None

        pos["state"] = ST_CLOSING

        if pos["dir"] == 1:
            pnl_points = exit_price - pos["entry_price"]
        else:
            pnl_points = pos["entry_price"] - exit_price

        gross      = pnl_points * pos["lots"] * POINT_VALUE_PER_LOT
        commission = pos["lots"] * COMMISSION_PER_LOT
        net        = gross - commission

        self.equity += net

        trade = {
            "id":         pos["id"],
            "dir":        pos["dir"],
            "entry":      pos["entry_price"],
            "exit":       round(exit_price, PRICE_DECIMALS),
            "sl":         pos["sl_price"],
            "lots":       pos["lots"],
            "pnl_points": round(pnl_points, 4),
            "net_pnl":    round(net, 2),
            "reason":     reason,
            "entry_time": pos["entry_time"],
            "exit_time":  exit_time,
            "bars_held":  pos["bars_held"],
            "mfe_points": round(pos["mfe_points"], 4),
            "mae_points": round(pos["mae_points"], 4),
            "risk_used":  pos["risk_used"],
            "entry_utc_h": pos.get("entry_utc_h"),
        }
        self.trades.append(trade)

        self.markers.append({
            "time":     exit_time,
            "position": "aboveBar" if pos["dir"] == 1 else "belowBar",
            "color":    "#26c6da" if reason == "TP" else "#ff9800",
            "shape":    "circle",
            "text":     f"#{pos['id']} {reason} {net:+.2f}",
        })

        # Mark fully closed and remove from active list
        pos["state"] = ST_CLOSED
        try:
            self.positions.remove(pos)
        except ValueError:
            pass

        print(f"[EXIT/{reason} #{pos['id']}] @ {exit_price:.2f}  "
              f"pnl={net:+.2f}$  bars_held={pos['bars_held']}  equity=${self.equity:.2f}  "
              f"(open={self._open_count()})")

        # Side effects — queued
        if LIVE_TRADING and pos["broker_ticket"] is not None:
            self._broker_q.put(("CLOSE", pos["id"], dict(pos)))
        self._notify_q.put(("EXIT", {
            "trade_id": pos["id"], "direction": pos["dir"], "entry": pos["entry_price"],
            "exit_px": exit_price, "net_pnl": net, "reason": reason,
            "bars_held": pos["bars_held"], "equity": self.equity,
            "position_count": self._open_count(),
        }))

        if self._validate_fn is not None:
            self.pending_validations.append(trade)
            self._notify_q.put(("VALIDATE_KICK", None))

        return trade

    # ── TICK PATH (SL check) ───────────────────────────────────

    def on_tick(self, tick):
        """Called from live_loop or warmup feed. SL check is atomic."""
        price = tick["price"]
        ts    = tick["ts"]

        with self.lock:
            if not self.positions:
                return

            # Snapshot to iterate, but guarded by state check inside _close_position
            for pos in list(self.positions):
                if pos["state"] != ST_OPEN:
                    continue

                # MFE/MAE (tick-level — more accurate than backtester bar-level,
                # but for Koko bars without gaps the diff is negligible)
                if pos["dir"] == 1:
                    fav = price - pos["entry_price"]
                    adv = pos["entry_price"] - price
                else:
                    fav = pos["entry_price"] - price
                    adv = price - pos["entry_price"]
                if fav > pos["mfe_points"]: pos["mfe_points"] = fav
                if adv > pos["mae_points"]: pos["mae_points"] = adv

                # SL check
                if pos["dir"] == 1 and price <= pos["sl_price"]:
                    self._close_position(pos, pos["sl_price"] - SLIPPAGE_POINTS, "SL", ts)
                elif pos["dir"] == -1 and price >= pos["sl_price"]:
                    self._close_position(pos, pos["sl_price"] + SLIPPAGE_POINTS, "SL", ts)

    # ── BAR CLOSE PATH (TP + signal scan) ──────────────────────

    def on_bar_close(self, bar, all_bars):
        with self.lock:
            # 1) Age positions and TP-exit any at horizon.
            #    bars_held >= TP_CLOSE_AFTER + 1 matches backtester exit at bar i+4.
            for pos in list(self.positions):
                if pos["state"] != ST_OPEN:
                    continue
                pos["bars_held"] += 1
                if pos["bars_held"] >= TP_CLOSE_AFTER + 1:
                    if pos["dir"] == 1:
                        exit_price = bar["close"] - SLIPPAGE_POINTS
                    else:
                        exit_price = bar["close"] + SLIPPAGE_POINTS
                    self._close_position(pos, exit_price, "TP", bar["time"])

            # 2) New signal scan (warmup gate)
            if not self.warmup_done:
                return

            sig = self._check_signal(all_bars)
            if sig != 0:
                if is_blocked_hour_utc():
                    self.blocked_setups += 1
                    now_h = datetime.now(timezone.utc).hour
                    print(f"[BLOCKED] {('LONG' if sig==1 else 'SHORT')} signal at UTC hour "
                          f"{now_h:02d} — skipped (total blocked={self.blocked_setups})")
                else:
                    self._open_position(sig, bar)

    # ── ASYNC WORKERS ──────────────────────────────────────────

    def _broker_worker(self):
        """Processes MT5 open/close orders serially, with retries. Never blocks tick loop."""
        while self._running:
            try:
                action, pos_id, snap = self._broker_q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if action == "OPEN":
                    self._broker_open(pos_id, snap)
                elif action == "CLOSE":
                    self._broker_close(pos_id, snap)
            except Exception as e:
                print(f"[BROKER-ERR] action={action} #{pos_id}: {e}")

    def _broker_open(self, pos_id, snap):
        t = mt5.symbol_info_tick(SYMBOL)
        if t is None:
            print(f"[MT5] #{pos_id} OPEN: no tick, abort")
            return
        price = t.ask if snap["dir"] == 1 else t.bid
        order_type = mt5.ORDER_TYPE_BUY if snap["dir"] == 1 else mt5.ORDER_TYPE_SELL
        mt5_lots = max(MIN_LOTS, round_to_step(snap["lots"], MT5_LOT_STEP))

        req = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       SYMBOL,
            "volume":       mt5_lots,
            "type":         order_type,
            "price":        price,
            "sl":           snap["sl_price"],
            "deviation":    DEVIATION_POINTS,
            "magic":        MAGIC_NUMBER,
            "comment":      f"koko_#{pos_id}",
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        for attempt in range(1, BROKER_RETRY_MAX + 1):
            result = mt5.order_send(req)
            if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                # Patch the live position with the broker ticket
                with self.lock:
                    for p in self.positions:
                        if p["id"] == pos_id:
                            p["broker_ticket"] = result.order
                            break
                    else:
                        # Position already closed by SL/TP before broker filled.
                        # Issue a CLOSE for the ticket we just got.
                        print(f"[MT5] #{pos_id} filled ticket={result.order} but already "
                              f"closed internally → queuing broker close")
                        ghost = dict(snap)
                        ghost["broker_ticket"] = result.order
                        self._broker_q.put(("CLOSE", pos_id, ghost))
                        return
                print(f"[MT5] #{pos_id} OPEN filled ticket={result.order} lots={mt5_lots}")
                return
            print(f"[MT5] #{pos_id} OPEN attempt {attempt}/{BROKER_RETRY_MAX} failed: {result}")
            time.sleep(BROKER_RETRY_DELAY_S)
        print(f"[MT5] #{pos_id} OPEN GAVE UP after {BROKER_RETRY_MAX} attempts")

    def _broker_close(self, pos_id, snap):
        ticket = snap.get("broker_ticket")
        if ticket is None:
            print(f"[MT5] #{pos_id} CLOSE skipped — no broker ticket recorded")
            return
        close_type = mt5.ORDER_TYPE_SELL if snap["dir"] == 1 else mt5.ORDER_TYPE_BUY
        mt5_lots = max(MIN_LOTS, round_to_step(snap["lots"], MT5_LOT_STEP))

        for attempt in range(1, BROKER_RETRY_MAX + 1):
            t = mt5.symbol_info_tick(SYMBOL)
            if t is None:
                time.sleep(BROKER_RETRY_DELAY_S); continue
            price = t.bid if snap["dir"] == 1 else t.ask
            req = {
                "action":       mt5.TRADE_ACTION_DEAL,
                "symbol":       SYMBOL,
                "volume":       mt5_lots,
                "type":         close_type,
                "position":     ticket,
                "price":        price,
                "deviation":    DEVIATION_POINTS,
                "magic":        MAGIC_NUMBER,
                "comment":      f"koko_close_#{pos_id}",
                "type_time":    mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            result = mt5.order_send(req)
            if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                print(f"[MT5] #{pos_id} CLOSE done ticket={ticket}")
                return
            # If broker already closed it (e.g. SL fired server-side), positions_get won't have it.
            still_open = mt5.positions_get(ticket=ticket)
            if not still_open:
                print(f"[MT5] #{pos_id} CLOSE: ticket {ticket} not on broker — already closed.")
                return
            print(f"[MT5] #{pos_id} CLOSE attempt {attempt}/{BROKER_RETRY_MAX} failed: {result}")
            time.sleep(BROKER_RETRY_DELAY_S)
        print(f"[MT5] #{pos_id} CLOSE GAVE UP after {BROKER_RETRY_MAX} attempts — MANUAL CHECK NEEDED")

    def _notify_worker(self):
        """Telegram + validator runner. Never blocks tick path."""
        while self._running:
            try:
                kind, payload = self._notify_q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if kind == "ENTRY":
                    tg.notify_entry(**payload)
                elif kind == "EXIT":
                    tg.notify_exit(**payload)
                elif kind == "VALIDATE_KICK":
                    self._run_pending_validations()
            except Exception as e:
                print(f"[NOTIFY-ERR] kind={kind}: {e}")

    def _run_pending_validations(self):
        """Validator runner — pops a snapshot of pending under lock, processes outside lock."""
        with self.lock:
            pending = list(self.pending_validations)
            self.pending_validations = []
        still_pending = []
        for trade in pending:
            try:
                v = self._validate_fn(trade)
                if v["status"] == "SKIP" and any("insufficient post-signal" in i for i in v.get("issues", [])):
                    still_pending.append(trade)
                    continue
                with self.lock:
                    self.validations.append(v)
                print(validator.format_report(v))
                tg.notify_validation(v)
            except Exception as e:
                print(f"[VALIDATOR-ERR] {e}")
                still_pending.append(trade)
        if still_pending:
            with self.lock:
                self.pending_validations.extend(still_pending)

    # ── RECONCILIATION ────────────────────────────────────────

    def reconcile_with_broker(self):
        """
        Compares internal open positions vs broker open positions.
        Logs drift. Does NOT auto-correct internal state (backtester is source of
        truth for accounting). But it will queue close orders for any orphan
        broker positions tagged with our MAGIC_NUMBER.
        """
        if not LIVE_TRADING:
            return
        try:
            broker_positions = mt5.positions_get(symbol=SYMBOL) or []
        except Exception as e:
            print(f"[RECONCILE-ERR] {e}")
            return

        broker_by_ticket = {p.ticket: p for p in broker_positions if p.magic == MAGIC_NUMBER}

        with self.lock:
            internal_tickets = {p["broker_ticket"] for p in self.positions if p["broker_ticket"]}
            internal_open = [(p["id"], p["broker_ticket"]) for p in self.positions]

        # Internal-open but not on broker (broker closed it independently)
        missing_on_broker = [(pid, tk) for pid, tk in internal_open if tk and tk not in broker_by_ticket]
        # On broker but not in internal (orphan from a previous run or failed close)
        orphan_on_broker = [tk for tk in broker_by_ticket if tk not in internal_tickets]

        if missing_on_broker or orphan_on_broker:
            print(f"[RECONCILE] internal_open={len(internal_open)} broker_open={len(broker_by_ticket)}  "
                  f"missing_on_broker={missing_on_broker} orphan_on_broker={orphan_on_broker}")

        # Force-close orphans on the broker so we don't leak risk
        for tk in orphan_on_broker:
            bp = broker_by_ticket[tk]
            direction = 1 if bp.type == mt5.POSITION_TYPE_BUY else -1
            snap = {
                "dir":           direction,
                "lots":          bp.volume,
                "broker_ticket": tk,
                "sl_price":      bp.sl,
            }
            print(f"[RECONCILE] queuing emergency CLOSE for orphan ticket={tk}")
            self._broker_q.put(("CLOSE", -1, snap))


# ============================ ENGINE ============================

class LiveEngine:
    def __init__(self):
        self.bars     = []
        self.strategy = StrategyEngine()
        self.strategy._validate_fn = lambda trade: validator.validate(trade, self.bars)
        self.streamer = KokoCandleStreamer(
            range_size=RANGE_SIZE, price_decimals=PRICE_DECIMALS,
            rev_bricks=REV_BRICKS, clean_mode=CLEAN_MODE,
            on_bar_close=self._on_bar_close,
        )
        self.last_tick_time = 0
        self.last_price     = None
        self.last_bid       = None
        self.last_ask       = None
        self.lock           = threading.Lock()
        self.running        = True
        self.tick_count     = 0

    def _on_bar_close(self, bar):
        self.bars.append(bar)
        self.strategy.on_bar_close(bar, self.bars)

    def init_mt5(self):
        if not mt5.initialize():
            raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")
        info = mt5.symbol_info(SYMBOL)
        if info is None:
            raise RuntimeError(f"Symbol {SYMBOL} not found")
        if not info.visible:
            mt5.symbol_select(SYMBOL, True)
        print(f"[MT5] connected. symbol={SYMBOL} digits={info.digits} point={info.point}")

    def _feed_ticks(self, ticks):
        for raw in ticks:
            t = tick_to_price(raw)
            if t is None: continue
            if t["ts"] < self.last_tick_time:
                t["ts"] = self.last_tick_time
            self.strategy.on_tick(t)
            self.streamer.process_tick(t)
            self.last_tick_time = t["ts"]
            self.last_price     = t["price"]
            self.last_bid       = t["bid"]
            self.last_ask       = t["ask"]

    def warmup(self):
        lookback = WARMUP_LOOKBACK_DAYS
        attempt  = 0
        while True:
            attempt += 1
            end   = datetime.now(timezone.utc)
            start = end - timedelta(days=lookback)
            print(f"[WARMUP attempt {attempt}] fetching last {lookback}d of ticks...")
            ticks = mt5.copy_ticks_range(SYMBOL, start, end, mt5.COPY_TICKS_ALL)
            if ticks is None or len(ticks) == 0:
                raise RuntimeError(f"no ticks returned for last {lookback}d")
            print(f"[WARMUP] {len(ticks):,} ticks → streaming through bar builder...")

            self.bars = []
            self.streamer = KokoCandleStreamer(
                range_size=RANGE_SIZE, price_decimals=PRICE_DECIMALS,
                rev_bricks=REV_BRICKS, clean_mode=CLEAN_MODE,
                on_bar_close=self._on_bar_close,
            )
            self.last_tick_time = 0
            self._feed_ticks(ticks)

            if len(self.bars) >= WARMUP_BARS:
                print(f"[WARMUP] built {len(self.bars)} bars, last_price={self.last_price}")
                break
            print(f"[WARMUP] only {len(self.bars)} bars from {lookback}d, extending lookback...")
            lookback *= 2
            if lookback > 90:
                print(f"[WARMUP] capped at {len(self.bars)} bars; continuing live anyway")
                break

        keep = max(WARMUP_BARS, 200)
        if len(self.bars) > keep:
            self.bars = self.bars[-keep:]

        gap_start = datetime.fromtimestamp(self.last_tick_time, tz=timezone.utc)
        gap_end   = datetime.now(timezone.utc)
        if (gap_end - gap_start).total_seconds() > 1:
            print(f"[WARMUP] gap-filling from {gap_start.isoformat()} → now")
            gap = mt5.copy_ticks_range(SYMBOL, gap_start, gap_end, mt5.COPY_TICKS_ALL)
            if gap is not None and len(gap) > 0:
                print(f"[WARMUP] {len(gap):,} gap ticks → feeding")
                self._feed_ticks(gap)

        self.strategy.warmup_done = True
        cur_h = datetime.now(timezone.utc).hour
        blk = " [BLOCKED HOUR — no entries until next hour]" if cur_h in BLOCKED_HOURS_UTC else ""
        print(f"\n{'='*60}\n  WARMUP COMPLETE — {len(self.bars)} bars\n"
              f"  Last price: {self.last_price}\n"
              f"  Current UTC hour: {cur_h:02d}{blk}\n"
              f"  Blocked hours (UTC): {BLOCKED_HOURS_UTC}\n"
              f"  STRATEGY IS NOW LIVE\n{'='*60}\n")
        tg.notify_warmup_done(len(self.bars), self.last_price)

    def live_loop(self):
        print("[LIVE] tick poll started")
        last_heartbeat = time.time()
        last_reconcile = time.time()
        none_count = 0

        while self.running:
            try:
                raw = mt5.symbol_info_tick(SYMBOL)
            except Exception as e:
                print(f"[LIVE-ERR] {e}")
                time.sleep(1.0); continue

            if raw is None:
                none_count += 1
                if none_count <= 3 or none_count % 100 == 0:
                    print(f"[LIVE-WARN] symbol_info_tick=None (x{none_count}) — is {SYMBOL} in Market Watch?")
                time.sleep(0.5); continue
            none_count = 0

            t = tick_to_price(raw)
            if t is None:
                time.sleep(TICK_POLL_MS / 1000.0); continue

            ts, price = t["ts"], t["price"]
            if ts < self.last_tick_time:
                ts = self.last_tick_time
            if ts == self.last_tick_time and price == self.last_price:
                time.sleep(TICK_POLL_MS / 1000.0); continue

            # tick path — fast, atomic, never blocks on network
            with self.lock:
                self.strategy.on_tick(t)
                self.streamer.process_tick(t)

            self.last_tick_time = ts
            self.last_price     = price
            self.last_bid       = t["bid"]
            self.last_ask       = t["ask"]
            self.tick_count    += 1

            now = time.time()

            # heartbeat
            if now - last_heartbeat >= 10.0:
                cur_h = datetime.now(timezone.utc).hour
                blk = "BLOCKED" if cur_h in BLOCKED_HOURS_UTC else "OPEN"
                print(f"[HEARTBEAT] ticks={self.tick_count}  price={price}  "
                      f"bars={len(self.bars)}  pos={self.strategy._open_count()}  "
                      f"equity=${self.strategy.equity:.2f}  "
                      f"hUTC={cur_h:02d}({blk})  blocked={self.strategy.blocked_setups}  "
                      f"qB={self.strategy._broker_q.qsize()} qN={self.strategy._notify_q.qsize()}")
                last_heartbeat = now

            # broker reconciliation
            if now - last_reconcile >= RECONCILE_INTERVAL_S:
                self.strategy.reconcile_with_broker()
                last_reconcile = now

            time.sleep(TICK_POLL_MS / 1000.0)

    def snapshot(self):
        with self.lock:
            working = self.streamer.get_working_bar()
            chart_bars = self.bars[-CHART_MAX_BARS:] if len(self.bars) > CHART_MAX_BARS else self.bars
            cur_h_utc = datetime.now(timezone.utc).hour
            with self.strategy.lock:
                positions_snap = [dict(p) for p in self.strategy.positions]
                trades_snap    = list(self.strategy.trades)
                validations    = list(self.strategy.validations)
                markers        = list(self.strategy.markers)
                pending_n      = len(self.strategy.pending_validations)
                equity         = self.strategy.equity
                blocked_set    = self.strategy.blocked_setups
            return {
                "bars":             list(chart_bars),
                "working_bar":      working,
                "markers":          markers,
                "pending_validations": pending_n,
                "trades":           trades_snap,
                "validations":      validations,
                "positions":        positions_snap,
                "warmup_done":      self.strategy.warmup_done,
                "bar_count":        len(self.bars),
                "equity":           round(equity, 2),
                "balance":          round(STARTING_BALANCE + equity, 2),
                "starting_balance": STARTING_BALANCE,
                "last_price":       self.last_price,
                "last_bid":         self.last_bid,
                "last_ask":         self.last_ask,
                "tick_count":       self.tick_count,
                "symbol":           SYMBOL,
                "live_trading":     LIVE_TRADING,
                "blocked_setups":   blocked_set,
                "blocked_hours_utc": BLOCKED_HOURS_UTC,
                "current_utc_hour": cur_h_utc,
                "is_blocked_now":   cur_h_utc in BLOCKED_HOURS_UTC,
                "broker_queue":     self.strategy._broker_q.qsize(),
                "notify_queue":     self.strategy._notify_q.qsize(),
                "config": {
                    "range":        RANGE_SIZE,
                    "rev":          REV_BRICKS,
                    "clean":        CLEAN_MODE,
                    "streak":       STREAK_SIZE,
                    "tp":           TP_CLOSE_AFTER,
                    "sl":           FIXED_SL_POINTS,
                    "slippage":     SLIPPAGE_POINTS,
                    "commission":   COMMISSION_PER_LOT,
                    "risk_per_100": RISK_PER_100,
                    "blocked_hours_utc": BLOCKED_HOURS_UTC,
                },
            }


# ============================ FLASK ============================

engine = LiveEngine()
app = Flask(__name__)

@app.route("/")
def index():
    return send_from_directory(".", "chart.html")

@app.route("/api/state")
def state():
    return jsonify(engine.snapshot())

@app.route("/api/debug")
def debug():
    cur_h = datetime.now(timezone.utc).hour
    with engine.strategy.lock:
        open_n = engine.strategy._open_count()
        closed_n = len(engine.strategy.trades)
        eq = engine.strategy.equity
        blocked = engine.strategy.blocked_setups
    return jsonify({
        "running":          engine.running,
        "warmup_done":      engine.strategy.warmup_done,
        "bar_count":        len(engine.bars),
        "tick_count":       engine.tick_count,
        "last_tick_time":   engine.last_tick_time,
        "last_tick_dt_utc": datetime.fromtimestamp(engine.last_tick_time, tz=timezone.utc).isoformat() if engine.last_tick_time else None,
        "last_price":       engine.last_price,
        "now_utc":          datetime.now(timezone.utc).isoformat(),
        "current_utc_hour": cur_h,
        "is_blocked_now":   cur_h in BLOCKED_HOURS_UTC,
        "blocked_hours_utc": BLOCKED_HOURS_UTC,
        "blocked_setups":   blocked,
        "open_positions":   open_n,
        "closed_trades":    closed_n,
        "equity":           round(eq, 2),
        "broker_queue":     engine.strategy._broker_q.qsize(),
        "notify_queue":     engine.strategy._notify_q.qsize(),
    })


# ============================ MAIN ============================

def main():
    print("=" * 60)
    print(f"  KOKO LIVE ENGINE — {SYMBOL} {RANGE_SIZE}pt  |  LIVE_TRADING={LIVE_TRADING}")
    print(f"  Strategy: streak={STREAK_SIZE} tp={TP_CLOSE_AFTER} sl={FIXED_SL_POINTS}pt")
    print(f"  Blocked UTC hours: {BLOCKED_HOURS_UTC}")
    print(f"  Broker async + reconciliation every {RECONCILE_INTERVAL_S}s")
    print("=" * 60)

    engine.init_mt5()

    tg.notify_start(
        symbol=SYMBOL, range_size=RANGE_SIZE, rev=REV_BRICKS, clean=CLEAN_MODE,
        streak=STREAK_SIZE, tp=TP_CLOSE_AFTER, sl=FIXED_SL_POINTS,
        starting_balance=STARTING_BALANCE, risk_per_100=RISK_PER_100,
        live_trading=LIVE_TRADING,
    )

    engine.warmup()

    t = threading.Thread(target=engine.live_loop, daemon=True)
    t.start()

    print(f"[WEB] chart → http://localhost:{HTTP_PORT}")
    try:
        app.run(host="0.0.0.0", port=HTTP_PORT, debug=False, use_reloader=False, threaded=True)
    finally:
        engine.running = False
        engine.strategy.shutdown()
        mt5.shutdown()


if __name__ == "__main__":
    main()