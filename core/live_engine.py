"""
Koko Live Engine — USTEC streaming + 2-streak/3-tp strategy
============================================================
- MT5 tick stream → Koko bars (8pt grid, 2x rev, clean ON)
- Strategy: 1:1 with backtester (streak=2, tp=3), overlapping positions OK
- Time filter: NO new entries during UTC (GMT+0) hour 0
- Risk: scaling, $1 per $100 balance
- BULLETPROOF TRADE MGMT:
    * persistent broker_mirror (ticket → metadata) survives internal close
    * stuck-close fast lane: aggressive reconcile until broker confirms
    * smart retcode handling: terminal vs retryable errors
    * fresh price re-fetch on every retry
    * reconcile every 5s (was 30s) + on-demand after any close failure
    * SHUTDOWN: flushes & force-closes all positions before exit
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

BROKER_RETRY_MAX      = 5         # was 3
BROKER_RETRY_DELAY_S  = 0.4
RECONCILE_INTERVAL_S  = 5.0       # was 30 — catch stuck closes fast
STUCK_NOTIFY_AFTER    = 3         # telegram alert after this many failed attempts

# Web
HTTP_PORT             = 5000
CHART_MAX_BARS        = 500

ST_OPEN    = "OPEN"
ST_CLOSING = "CLOSING"
ST_CLOSED  = "CLOSED"

# MT5 retcode classification
def _is_retryable_retcode(rc):
    if rc is None:
        return True
    return rc in (
        mt5.TRADE_RETCODE_REQUOTE,
        mt5.TRADE_RETCODE_PRICE_OFF,
        mt5.TRADE_RETCODE_PRICE_CHANGED,
        mt5.TRADE_RETCODE_TIMEOUT,
        mt5.TRADE_RETCODE_CONNECTION,
        mt5.TRADE_RETCODE_NO_CHANGES,
        mt5.TRADE_RETCODE_TOO_MANY_REQUESTS,
    )

def _is_already_closed_retcode(rc):
    if rc is None:
        return False
    return rc in (
        mt5.TRADE_RETCODE_POSITION_CLOSED,
        mt5.TRADE_RETCODE_INVALID_ORDER,  # often = ticket no longer exists
    )

# ============================ HELPERS ============================

def candle_dir(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0

def bar_hour_utc(ts):
    return int((int(ts) // 3600) % 24)

def is_blocked_hour_utc(ts=None):
    h = datetime.now(timezone.utc).hour if ts is None else bar_hour_utc(ts)
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
    if bid > 0 and ask > 0:   price = (bid + ask) / 2.0
    elif bid > 0:             price = bid
    elif ask > 0:             price = ask
    else:                     return None
    return {"ts": ts, "price": price, "volume": vol, "bid": bid, "ask": ask}


# =================== KOKO STREAMER (1:1 range_bars.py) ===================

class KokoCandleStreamer:
    def __init__(self, range_size, price_decimals, rev_bricks, clean_mode, on_bar_close):
        self.rs = float(range_size); self.pd = price_decimals
        self.rev_bricks = rev_bricks; self.clean_mode = clean_mode
        self.on_bar_close = on_bar_close
        self.trend = 0; self.level = None; self.bar = None
        self.bar_count = 0; self._last_emitted_ts = None

    def _snap(self, price): return round(round(price / self.rs) * self.rs, self.pd)

    def _make_bar(self, open_, high_, low_, close_):
        return {"time": int(self.bar["time"]),
                "open": round(open_, self.pd), "high": round(high_, self.pd),
                "low":  round(low_,  self.pd), "close": round(close_, self.pd),
                "volume": round(self.bar["volume"], 2)}

    def _emit(self, bar_dict):
        ts = int(bar_dict["time"])
        if self._last_emitted_ts is not None and ts <= self._last_emitted_ts:
            ts = self._last_emitted_ts + 1
        bar_dict["time"] = ts; self._last_emitted_ts = ts
        self.bar_count += 1; self.on_bar_close(bar_dict)

    def _reset_bar(self, tick):
        self.bar = {"time": tick["ts"], "open": self.level, "high": self.level,
                    "low": self.level, "close": self.level, "volume": 0.0}

    def get_working_bar(self):
        if self.bar is None: return None
        ts = self.bar["time"]
        if self._last_emitted_ts is not None and ts <= self._last_emitted_ts:
            ts = self._last_emitted_ts + 1
        return {"time": int(ts),
                "open": round(self.bar["open"], self.pd),
                "high": round(self.bar["high"], self.pd),
                "low":  round(self.bar["low"],  self.pd),
                "close":round(self.bar["close"],self.pd),
                "volume":round(self.bar["volume"],2)}

    def process_tick(self, tick):
        p, v, rs = tick["price"], tick["volume"], self.rs
        if self.bar is None:
            self.level = self._snap(p)
            self.bar = {"time": tick["ts"], "open": self.level, "high": self.level,
                        "low": self.level, "close": self.level, "volume": v}
            return
        self.bar["volume"] += v
        self.bar["close"] = p
        self.bar["high"] = max(self.bar["high"], p)
        self.bar["low"]  = min(self.bar["low"],  p)
        while True:
            lvl = self.level
            if self.trend == 0:
                up_t = round(lvl + rs, self.pd); down_t = round(lvl - rs, self.pd)
                if p >= up_t:
                    self._emit(self._make_bar(lvl, max(self.bar["high"], up_t), self.bar["low"], up_t))
                    self.trend = 1; self.level = up_t; self._reset_bar(tick); continue
                elif p <= down_t:
                    self._emit(self._make_bar(lvl, self.bar["high"], min(self.bar["low"], down_t), down_t))
                    self.trend = -1; self.level = down_t; self._reset_bar(tick); continue
                else: break
            elif self.trend == 1:
                cont_t = round(lvl + rs, self.pd); rev_t = round(lvl - self.rev_bricks * rs, self.pd)
                if p >= cont_t:
                    self._emit(self._make_bar(lvl, max(self.bar["high"], cont_t), self.bar["low"], cont_t))
                    self.level = cont_t; self._reset_bar(tick); continue
                elif p <= rev_t:
                    if self.clean_mode:
                        b_close=rev_t; b_open=round(rev_t+rs,self.pd)
                        b_high=max(self.bar["high"],lvl); b_low=min(self.bar["low"],b_close)
                    else:
                        b_open=lvl; b_close=rev_t; b_high=self.bar["high"]; b_low=min(self.bar["low"],b_close)
                    self._emit(self._make_bar(b_open,b_high,b_low,b_close))
                    self.trend=-1; self.level=rev_t; self._reset_bar(tick); continue
                else: break
            elif self.trend == -1:
                cont_t = round(lvl - rs, self.pd); rev_t = round(lvl + self.rev_bricks * rs, self.pd)
                if p <= cont_t:
                    self._emit(self._make_bar(lvl, self.bar["high"], min(self.bar["low"], cont_t), cont_t))
                    self.level = cont_t; self._reset_bar(tick); continue
                elif p >= rev_t:
                    if self.clean_mode:
                        b_close=rev_t; b_open=round(rev_t-rs,self.pd)
                        b_high=max(self.bar["high"],b_close); b_low=min(self.bar["low"],lvl)
                    else:
                        b_open=lvl; b_close=rev_t
                        b_high=max(self.bar["high"],b_close); b_low=self.bar["low"]
                    self._emit(self._make_bar(b_open,b_high,b_low,b_close))
                    self.trend=1; self.level=rev_t; self._reset_bar(tick); continue
                else: break


# ============================ STRATEGY ============================

class StrategyEngine:
    """
    Bulletproof 1:1 backtester replica with reliable trade lifecycle.

    Internal accounting (equity, trades list, etc.) is the source of truth
    for backtester matching. Broker reconciliation is a safety net to ensure
    that what we THINK closed actually closed.
    """

    def __init__(self):
        self.lock = threading.RLock()

        # Trading state
        self.positions           = []   # only ST_OPEN/ST_CLOSING
        self.trades              = []
        self.markers             = []
        self.pending_validations = []
        self.validations         = []
        self.warmup_done         = False
        self.equity              = 0.0
        self.blocked_setups      = 0
        self._trade_id_seq       = 0
        self._validate_fn        = None

        # Broker mirror — PERSISTS across internal close
        # ticket → {pos_id, dir, lots, sl_price, state, attempts, last_error, opened_at}
        self._broker_mirror      = {}
        # Tickets that failed to close and need urgent reconcile
        self._stuck_closes       = set()
        # All tickets we've ever opened (for reconciler ownership)
        self._known_tickets      = set()

        # Async workers
        self._broker_q  = queue.Queue()
        self._notify_q  = queue.Queue()
        self._reconcile_trigger = threading.Event()
        self._running   = True

        threading.Thread(target=self._broker_worker, daemon=True, name="broker").start()
        threading.Thread(target=self._notify_worker, daemon=True, name="notify").start()

    def shutdown(self):
        self._running = False

    # ── id / sizing ────────────────────────────────────────────

    def _next_trade_id(self):
        self._trade_id_seq += 1
        return self._trade_id_seq

    def _current_risk_dollars(self):
        bal = STARTING_BALANCE + self.equity
        return (bal / 100.0) * RISK_PER_100 if bal > 0 else 0.0

    def _calc_lots(self):
        risk = self._current_risk_dollars()
        if risk <= 0: return 0.0
        return max(MIN_LOTS, min(MAX_LOTS, risk / FIXED_SL_POINTS))

    def _open_count(self):
        return sum(1 for p in self.positions if p["state"] == ST_OPEN)

    # ── signal ────────────────────────────────────────────────

    def _check_signal(self, bars):
        if len(bars) < STREAK_SIZE + 1: return 0
        recent = bars[-(STREAK_SIZE + 1):]
        dirs = [candle_dir(b) for b in recent]
        sd = dirs[0]
        if sd == 0: return 0
        for d in dirs[:STREAK_SIZE]:
            if d != sd: return 0
        rd = dirs[STREAK_SIZE]
        if rd != -sd: return 0
        return rd

    # ── OPEN ──────────────────────────────────────────────────

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
            "id": tid, "state": ST_OPEN, "dir": direction,
            "entry_price": round(entry_price, PRICE_DECIMALS),
            "sl_price":    round(sl_price,    PRICE_DECIMALS),
            "lots": lots, "entry_time": signal_bar["time"],
            "entry_utc_h": datetime.now(timezone.utc).hour,
            "bars_held": 0, "mfe_points": 0.0, "mae_points": 0.0,
            "risk_used": self._current_risk_dollars(),
            "broker_ticket": None,
        }
        self.positions.append(pos)
        self.markers.append({
            "time": signal_bar["time"],
            "position": "belowBar" if direction == 1 else "aboveBar",
            "color":    "#4caf50" if direction == 1 else "#ef5350",
            "shape":    "arrowUp" if direction == 1 else "arrowDown",
            "text":     f"#{tid} {'L' if direction == 1 else 'S'} {entry_price:.2f}",
        })
        print(f"[ENTRY #{tid}] {'LONG' if direction==1 else 'SHORT'} @ {entry_price:.2f}  "
              f"SL={sl_price:.2f}  lots={lots:.4f}  risk=${pos['risk_used']:.2f}  "
              f"hUTC={pos['entry_utc_h']:02d}  (open={self._open_count()})")

        if LIVE_TRADING:
            self._broker_q.put(("OPEN", pos["id"], dict(pos)))
        self._notify_q.put(("ENTRY", {
            "trade_id": tid, "direction": direction, "entry": entry_price, "sl": sl_price,
            "lots": lots, "bar_time": signal_bar["time"],
            "position_count": self._open_count(),
        }))

    # ── CLOSE (atomic) ────────────────────────────────────────

    def _close_position(self, pos, exit_price, reason, exit_time):
        """ATOMIC. Under self.lock. Double-close guarded by pos['state']."""
        if pos["state"] != ST_OPEN:
            return None
        pos["state"] = ST_CLOSING

        pnl_points = (exit_price - pos["entry_price"]) if pos["dir"] == 1 \
                     else (pos["entry_price"] - exit_price)
        gross      = pnl_points * pos["lots"] * POINT_VALUE_PER_LOT
        commission = pos["lots"] * COMMISSION_PER_LOT
        net        = gross - commission
        self.equity += net

        trade = {
            "id": pos["id"], "dir": pos["dir"],
            "entry": pos["entry_price"], "exit": round(exit_price, PRICE_DECIMALS),
            "sl": pos["sl_price"], "lots": pos["lots"],
            "pnl_points": round(pnl_points, 4), "net_pnl": round(net, 2),
            "reason": reason, "entry_time": pos["entry_time"], "exit_time": exit_time,
            "bars_held": pos["bars_held"],
            "mfe_points": round(pos["mfe_points"], 4),
            "mae_points": round(pos["mae_points"], 4),
            "risk_used": pos["risk_used"], "entry_utc_h": pos.get("entry_utc_h"),
        }
        self.trades.append(trade)
        self.markers.append({
            "time": exit_time,
            "position": "aboveBar" if pos["dir"] == 1 else "belowBar",
            "color":    "#26c6da" if reason == "TP" else "#ff9800",
            "shape":    "circle",
            "text":     f"#{pos['id']} {reason} {net:+.2f}",
        })

        # Snapshot broker info BEFORE removing from positions list
        broker_ticket = pos.get("broker_ticket")
        snap_for_close = dict(pos)

        pos["state"] = ST_CLOSED
        try: self.positions.remove(pos)
        except ValueError: pass

        print(f"[EXIT/{reason} #{pos['id']}] @ {exit_price:.2f}  "
              f"pnl={net:+.2f}$  bars_held={pos['bars_held']}  equity=${self.equity:.2f}  "
              f"(open={self._open_count()}  ticket={broker_ticket})")

        # Queue broker close. ALWAYS queue, even if ticket is None —
        # broker_worker will check the mirror to recover the ticket if
        # the open was still in flight.
        if LIVE_TRADING:
            self._broker_q.put(("CLOSE", pos["id"], snap_for_close))

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

    # ── TICK PATH (SL check) ──────────────────────────────────

    def on_tick(self, tick):
        price = tick["price"]; ts = tick["ts"]
        with self.lock:
            if not self.positions: return
            for pos in list(self.positions):
                if pos["state"] != ST_OPEN: continue
                if pos["dir"] == 1:
                    fav = price - pos["entry_price"]; adv = pos["entry_price"] - price
                else:
                    fav = pos["entry_price"] - price; adv = price - pos["entry_price"]
                if fav > pos["mfe_points"]: pos["mfe_points"] = fav
                if adv > pos["mae_points"]: pos["mae_points"] = adv
                if pos["dir"] == 1 and price <= pos["sl_price"]:
                    self._close_position(pos, pos["sl_price"] - SLIPPAGE_POINTS, "SL", ts)
                elif pos["dir"] == -1 and price >= pos["sl_price"]:
                    self._close_position(pos, pos["sl_price"] + SLIPPAGE_POINTS, "SL", ts)

    # ── BAR CLOSE PATH ────────────────────────────────────────

    def on_bar_close(self, bar, all_bars):
        with self.lock:
            # 1) TP horizon
            for pos in list(self.positions):
                if pos["state"] != ST_OPEN: continue
                pos["bars_held"] += 1
                if pos["bars_held"] >= TP_CLOSE_AFTER + 1:
                    if pos["dir"] == 1: exit_price = bar["close"] - SLIPPAGE_POINTS
                    else:               exit_price = bar["close"] + SLIPPAGE_POINTS
                    self._close_position(pos, exit_price, "TP", bar["time"])

            # 2) Signal scan
            if not self.warmup_done: return
            sig = self._check_signal(all_bars)
            if sig != 0:
                if is_blocked_hour_utc():
                    self.blocked_setups += 1
                    print(f"[BLOCKED] {('LONG' if sig==1 else 'SHORT')} signal at UTC hour "
                          f"{datetime.now(timezone.utc).hour:02d} — skipped "
                          f"(total blocked={self.blocked_setups})")
                else:
                    self._open_position(sig, bar)

    # ============== BROKER WORKER + RELIABILITY ==============

    def _broker_worker(self):
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
                # Re-queue with backoff so it isn't lost
                if action == "CLOSE":
                    time.sleep(1.0)
                    self._broker_q.put((action, pos_id, snap))

    def _broker_open(self, pos_id, snap):
        order_type = mt5.ORDER_TYPE_BUY if snap["dir"] == 1 else mt5.ORDER_TYPE_SELL
        mt5_lots = max(MIN_LOTS, round_to_step(snap["lots"], MT5_LOT_STEP))

        last_rc = None
        for attempt in range(1, BROKER_RETRY_MAX + 1):
            t = mt5.symbol_info_tick(SYMBOL)
            if t is None:
                time.sleep(BROKER_RETRY_DELAY_S); continue
            price = t.ask if snap["dir"] == 1 else t.bid
            req = {
                "action": mt5.TRADE_ACTION_DEAL, "symbol": SYMBOL,
                "volume": mt5_lots, "type": order_type, "price": price,
                "sl": snap["sl_price"], "deviation": DEVIATION_POINTS,
                "magic": MAGIC_NUMBER, "comment": f"koko_#{pos_id}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            result = mt5.order_send(req)
            last_rc = result.retcode if result is not None else None
            if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                ticket = result.order
                with self.lock:
                    self._known_tickets.add(ticket)
                    self._broker_mirror[ticket] = {
                        "pos_id": pos_id, "dir": snap["dir"],
                        "lots": mt5_lots, "sl_price": snap["sl_price"],
                        "state": "OPEN", "attempts": 0, "last_error": None,
                        "opened_at": time.time(),
                    }
                    found = False
                    for p in self.positions:
                        if p["id"] == pos_id:
                            p["broker_ticket"] = ticket; found = True; break
                if not found:
                    # Position was already closed internally before broker filled.
                    print(f"[MT5] #{pos_id} filled ticket={ticket} BUT internal already closed → emergency close queued")
                    ghost = dict(snap); ghost["broker_ticket"] = ticket
                    self._broker_q.put(("CLOSE", pos_id, ghost))
                else:
                    print(f"[MT5] #{pos_id} OPEN filled ticket={ticket} lots={mt5_lots}")
                return
            print(f"[MT5] #{pos_id} OPEN attempt {attempt}/{BROKER_RETRY_MAX} failed: {result}")
            if not _is_retryable_retcode(last_rc):
                print(f"[MT5] #{pos_id} OPEN terminal error rc={last_rc}, giving up early")
                break
            time.sleep(BROKER_RETRY_DELAY_S)

        print(f"[MT5] #{pos_id} OPEN GAVE UP after {BROKER_RETRY_MAX} attempts (last rc={last_rc})")
        self._notify_q.put(("STUCK", {
            "kind": "OPEN_FAILED", "pos_id": pos_id, "retcode": last_rc,
        }))

    def _broker_close(self, pos_id, snap):
        ticket = snap.get("broker_ticket")

        # If we don't have a ticket, check the mirror in case the open
        # filled after the internal close.
        if ticket is None:
            with self.lock:
                for tk, m in self._broker_mirror.items():
                    if m["pos_id"] == pos_id and m["state"] == "OPEN":
                        ticket = tk; break
        if ticket is None:
            # Open may STILL be in flight — defer once and retry.
            print(f"[MT5] #{pos_id} CLOSE: no ticket yet, deferring")
            time.sleep(0.8)
            with self.lock:
                for tk, m in self._broker_mirror.items():
                    if m["pos_id"] == pos_id and m["state"] == "OPEN":
                        ticket = tk; break
            if ticket is None:
                print(f"[MT5] #{pos_id} CLOSE: no ticket recorded — broker open may have failed; skipping")
                return

        # Pre-check: maybe broker already closed it server-side (SL or manual).
        existing = mt5.positions_get(ticket=ticket)
        if not existing:
            print(f"[MT5] #{pos_id} ticket {ticket} already gone on broker — close considered done")
            with self.lock:
                if ticket in self._broker_mirror:
                    self._broker_mirror[ticket]["state"] = "CLOSED"
                self._stuck_closes.discard(ticket)
            return

        close_type = mt5.ORDER_TYPE_SELL if snap["dir"] == 1 else mt5.ORDER_TYPE_BUY
        mt5_lots = max(MIN_LOTS, round_to_step(snap["lots"], MT5_LOT_STEP))

        last_rc = None
        for attempt in range(1, BROKER_RETRY_MAX + 1):
            t = mt5.symbol_info_tick(SYMBOL)
            if t is None:
                time.sleep(BROKER_RETRY_DELAY_S); continue
            price = t.bid if snap["dir"] == 1 else t.ask
            req = {
                "action": mt5.TRADE_ACTION_DEAL, "symbol": SYMBOL,
                "volume": mt5_lots, "type": close_type, "position": ticket,
                "price": price, "deviation": DEVIATION_POINTS,
                "magic": MAGIC_NUMBER, "comment": f"koko_close_#{pos_id}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            result = mt5.order_send(req)
            last_rc = result.retcode if result is not None else None

            if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                print(f"[MT5] #{pos_id} CLOSE done ticket={ticket}")
                with self.lock:
                    if ticket in self._broker_mirror:
                        self._broker_mirror[ticket]["state"] = "CLOSED"
                    self._stuck_closes.discard(ticket)
                return

            # Maybe broker SL/something closed it between attempts
            still_open = mt5.positions_get(ticket=ticket)
            if not still_open:
                print(f"[MT5] #{pos_id} ticket {ticket} disappeared mid-retry — treat as closed")
                with self.lock:
                    if ticket in self._broker_mirror:
                        self._broker_mirror[ticket]["state"] = "CLOSED"
                    self._stuck_closes.discard(ticket)
                return

            if _is_already_closed_retcode(last_rc):
                print(f"[MT5] #{pos_id} retcode says already closed (rc={last_rc})")
                with self.lock:
                    if ticket in self._broker_mirror:
                        self._broker_mirror[ticket]["state"] = "CLOSED"
                    self._stuck_closes.discard(ticket)
                return

            print(f"[MT5] #{pos_id} CLOSE attempt {attempt}/{BROKER_RETRY_MAX} failed: rc={last_rc} result={result}")
            with self.lock:
                if ticket in self._broker_mirror:
                    self._broker_mirror[ticket]["attempts"] = attempt
                    self._broker_mirror[ticket]["last_error"] = str(last_rc)
            if not _is_retryable_retcode(last_rc) and attempt >= 2:
                print(f"[MT5] #{pos_id} terminal close error rc={last_rc}, escalating to stuck")
                break
            time.sleep(BROKER_RETRY_DELAY_S)

        # GIVE UP path → mark stuck, trigger reconcile, alert
        with self.lock:
            self._stuck_closes.add(ticket)
            if ticket in self._broker_mirror:
                self._broker_mirror[ticket]["state"] = "STUCK"
        self._reconcile_trigger.set()
        print(f"[MT5] #{pos_id} CLOSE STUCK ticket={ticket} (rc={last_rc}) — reconciler taking over")
        self._notify_q.put(("STUCK", {
            "kind": "CLOSE_STUCK", "pos_id": pos_id, "ticket": ticket, "retcode": last_rc,
        }))

    # ── NOTIFY WORKER ─────────────────────────────────────────

    def _notify_worker(self):
        while self._running:
            try:
                kind, payload = self._notify_q.get(timeout=0.5)
            except queue.Empty: continue
            try:
                if kind == "ENTRY":   tg.notify_entry(**payload)
                elif kind == "EXIT":  tg.notify_exit(**payload)
                elif kind == "VALIDATE_KICK": self._run_pending_validations()
                elif kind == "STUCK":
                    if hasattr(tg, "notify_stuck"):
                        tg.notify_stuck(**payload)
                    else:
                        print(f"[TG-STUB] STUCK {payload}")
            except Exception as e:
                print(f"[NOTIFY-ERR] kind={kind}: {e}")

    def _run_pending_validations(self):
        with self.lock:
            pending = list(self.pending_validations)
            self.pending_validations = []
        still = []
        for trade in pending:
            try:
                v = self._validate_fn(trade)
                if v["status"] == "SKIP" and any("insufficient post-signal" in i for i in v.get("issues", [])):
                    still.append(trade); continue
                with self.lock: self.validations.append(v)
                print(validator.format_report(v))
                tg.notify_validation(v)
            except Exception as e:
                print(f"[VALIDATOR-ERR] {e}")
                still.append(trade)
        if still:
            with self.lock: self.pending_validations.extend(still)

    # ── RECONCILER (the safety net) ───────────────────────────

    def reconcile_with_broker(self):
        if not LIVE_TRADING: return
        try:
            broker_positions = mt5.positions_get(symbol=SYMBOL) or []
        except Exception as e:
            print(f"[RECONCILE-ERR] {e}"); return

        by_ticket = {p.ticket: p for p in broker_positions if p.magic == MAGIC_NUMBER}

        with self.lock:
            internal_tickets   = {p["broker_ticket"] for p in self.positions if p["broker_ticket"]}
            stuck              = list(self._stuck_closes)
            known              = set(self._known_tickets)

        # 1) STUCK CLOSES — top priority
        for tk in stuck:
            if tk not in by_ticket:
                with self.lock:
                    self._stuck_closes.discard(tk)
                    if tk in self._broker_mirror:
                        self._broker_mirror[tk]["state"] = "CLOSED"
                print(f"[RECONCILE] stuck ticket {tk} confirmed closed")
                continue
            bp = by_ticket[tk]
            print(f"[RECONCILE] retrying stuck close ticket={tk}")
            self._force_close_ticket(tk, bp)

        # 2) Orphans (on broker but unknown to us) — force close
        orphans = [tk for tk in by_ticket if tk not in internal_tickets and tk not in stuck]
        for tk in orphans:
            bp = by_ticket[tk]
            print(f"[RECONCILE] orphan ticket {tk} on broker — force closing")
            self._force_close_ticket(tk, bp)

        # 3) Drift logging
        missing_on_broker = [(p["id"], p["broker_ticket"]) for p in self.positions
                             if p["broker_ticket"] and p["broker_ticket"] not in by_ticket]
        if missing_on_broker:
            print(f"[RECONCILE] internal OPEN but missing on broker (likely server SL): {missing_on_broker}")

    def _force_close_ticket(self, ticket, bp):
        """Aggressively close a single ticket regardless of internal state."""
        direction = 1 if bp.type == mt5.POSITION_TYPE_BUY else -1
        close_type = mt5.ORDER_TYPE_SELL if direction == 1 else mt5.ORDER_TYPE_BUY
        t = mt5.symbol_info_tick(SYMBOL)
        if t is None: return
        price = t.bid if direction == 1 else t.ask
        req = {
            "action": mt5.TRADE_ACTION_DEAL, "symbol": SYMBOL,
            "volume": bp.volume, "type": close_type, "position": ticket,
            "price": price, "deviation": DEVIATION_POINTS * 2,
            "magic": MAGIC_NUMBER, "comment": f"koko_force_{ticket}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(req)
        rc = result.retcode if result is not None else None
        if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"[RECONCILE] force-close ticket={ticket} OK")
            with self.lock:
                self._stuck_closes.discard(ticket)
                if ticket in self._broker_mirror:
                    self._broker_mirror[ticket]["state"] = "CLOSED"
        else:
            print(f"[RECONCILE] force-close ticket={ticket} FAILED rc={rc}")

    # ── EMERGENCY SHUTDOWN ────────────────────────────────────

    def emergency_close_all(self):
        if not LIVE_TRADING: return
        print("[SHUTDOWN] emergency closing all open broker positions for symbol+magic...")
        try:
            broker_positions = mt5.positions_get(symbol=SYMBOL) or []
        except Exception as e:
            print(f"[SHUTDOWN-ERR] {e}"); return
        for bp in broker_positions:
            if bp.magic != MAGIC_NUMBER: continue
            self._force_close_ticket(bp.ticket, bp)


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
        self.last_price = None; self.last_bid = None; self.last_ask = None
        self.lock = threading.Lock()
        self.running = True
        self.tick_count = 0

    def _on_bar_close(self, bar):
        self.bars.append(bar)
        self.strategy.on_bar_close(bar, self.bars)

    def init_mt5(self):
        if not mt5.initialize():
            raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")
        info = mt5.symbol_info(SYMBOL)
        if info is None: raise RuntimeError(f"Symbol {SYMBOL} not found")
        if not info.visible: mt5.symbol_select(SYMBOL, True)
        print(f"[MT5] connected. symbol={SYMBOL} digits={info.digits} point={info.point}")

    def _feed_ticks(self, ticks):
        for raw in ticks:
            t = tick_to_price(raw)
            if t is None: continue
            if t["ts"] < self.last_tick_time: t["ts"] = self.last_tick_time
            self.strategy.on_tick(t)
            self.streamer.process_tick(t)
            self.last_tick_time = t["ts"]
            self.last_price = t["price"]; self.last_bid = t["bid"]; self.last_ask = t["ask"]

    def warmup(self):
        lookback = WARMUP_LOOKBACK_DAYS; attempt = 0
        while True:
            attempt += 1
            end = datetime.now(timezone.utc); start = end - timedelta(days=lookback)
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
            print(f"[WARMUP] only {len(self.bars)} bars, extending lookback...")
            lookback *= 2
            if lookback > 90:
                print(f"[WARMUP] capped at {len(self.bars)} bars; continuing"); break

        keep = max(WARMUP_BARS, 200)
        if len(self.bars) > keep: self.bars = self.bars[-keep:]

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
        blk = " [BLOCKED HOUR]" if cur_h in BLOCKED_HOURS_UTC else ""
        print(f"\n{'='*60}\n  WARMUP COMPLETE — {len(self.bars)} bars\n"
              f"  Last price: {self.last_price}\n"
              f"  Current UTC hour: {cur_h:02d}{blk}\n"
              f"  Blocked hours (UTC): {BLOCKED_HOURS_UTC}\n"
              f"  STRATEGY IS NOW LIVE\n{'='*60}\n")
        tg.notify_warmup_done(len(self.bars), self.last_price)

        # Initial reconcile (in case engine was restarted while positions open)
        self.strategy.reconcile_with_broker()

    def live_loop(self):
        print("[LIVE] tick poll started")
        last_heartbeat = time.time(); last_reconcile = time.time()
        none_count = 0
        while self.running:
            try:
                raw = mt5.symbol_info_tick(SYMBOL)
            except Exception as e:
                print(f"[LIVE-ERR] {e}"); time.sleep(1.0); continue
            if raw is None:
                none_count += 1
                if none_count <= 3 or none_count % 100 == 0:
                    print(f"[LIVE-WARN] symbol_info_tick=None (x{none_count})")
                time.sleep(0.5); continue
            none_count = 0

            t = tick_to_price(raw)
            if t is None: time.sleep(TICK_POLL_MS/1000.0); continue
            ts, price = t["ts"], t["price"]
            if ts < self.last_tick_time: ts = self.last_tick_time
            if ts == self.last_tick_time and price == self.last_price:
                time.sleep(TICK_POLL_MS/1000.0); continue

            with self.lock:
                self.strategy.on_tick(t)
                self.streamer.process_tick(t)
            self.last_tick_time = ts; self.last_price = price
            self.last_bid = t["bid"]; self.last_ask = t["ask"]
            self.tick_count += 1

            now = time.time()
            if now - last_heartbeat >= 10.0:
                cur_h = datetime.now(timezone.utc).hour
                blk = "BLOCKED" if cur_h in BLOCKED_HOURS_UTC else "OPEN"
                with self.strategy.lock:
                    stuck_n = len(self.strategy._stuck_closes)
                    mirror_n = len(self.strategy._broker_mirror)
                print(f"[HEARTBEAT] ticks={self.tick_count} price={price} "
                      f"bars={len(self.bars)} pos={self.strategy._open_count()} "
                      f"equity=${self.strategy.equity:.2f} hUTC={cur_h:02d}({blk}) "
                      f"blocked={self.strategy.blocked_setups} "
                      f"stuck={stuck_n} mirror={mirror_n} "
                      f"qB={self.strategy._broker_q.qsize()} qN={self.strategy._notify_q.qsize()}")
                last_heartbeat = now

            # Reconcile on timer OR on demand
            if (now - last_reconcile >= RECONCILE_INTERVAL_S) \
               or self.strategy._reconcile_trigger.is_set():
                self.strategy._reconcile_trigger.clear()
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
                stuck_list     = list(self.strategy._stuck_closes)
                mirror_open    = sum(1 for m in self.strategy._broker_mirror.values() if m["state"] == "OPEN")
                mirror_stuck   = sum(1 for m in self.strategy._broker_mirror.values() if m["state"] == "STUCK")
            return {
                "bars": list(chart_bars), "working_bar": working,
                "markers": markers, "pending_validations": pending_n,
                "trades": trades_snap, "validations": validations,
                "positions": positions_snap, "warmup_done": self.strategy.warmup_done,
                "bar_count": len(self.bars), "equity": round(equity, 2),
                "balance": round(STARTING_BALANCE + equity, 2),
                "starting_balance": STARTING_BALANCE,
                "last_price": self.last_price, "last_bid": self.last_bid, "last_ask": self.last_ask,
                "tick_count": self.tick_count, "symbol": SYMBOL,
                "live_trading": LIVE_TRADING,
                "blocked_setups": blocked_set, "blocked_hours_utc": BLOCKED_HOURS_UTC,
                "current_utc_hour": cur_h_utc, "is_blocked_now": cur_h_utc in BLOCKED_HOURS_UTC,
                "broker_queue": self.strategy._broker_q.qsize(),
                "notify_queue": self.strategy._notify_q.qsize(),
                "stuck_tickets": stuck_list,
                "broker_mirror_open":  mirror_open,
                "broker_mirror_stuck": mirror_stuck,
                "config": {
                    "range": RANGE_SIZE, "rev": REV_BRICKS, "clean": CLEAN_MODE,
                    "streak": STREAK_SIZE, "tp": TP_CLOSE_AFTER, "sl": FIXED_SL_POINTS,
                    "slippage": SLIPPAGE_POINTS, "commission": COMMISSION_PER_LOT,
                    "risk_per_100": RISK_PER_100, "blocked_hours_utc": BLOCKED_HOURS_UTC,
                },
            }


# ============================ FLASK ============================

engine = LiveEngine()
app = Flask(__name__)

@app.route("/")
def index(): return send_from_directory(".", "chart.html")

@app.route("/api/state")
def state(): return jsonify(engine.snapshot())

@app.route("/api/debug")
def debug():
    cur_h = datetime.now(timezone.utc).hour
    with engine.strategy.lock:
        open_n = engine.strategy._open_count()
        closed_n = len(engine.strategy.trades)
        eq = engine.strategy.equity
        blocked = engine.strategy.blocked_setups
        stuck = list(engine.strategy._stuck_closes)
        mirror = dict(engine.strategy._broker_mirror)
    return jsonify({
        "running": engine.running, "warmup_done": engine.strategy.warmup_done,
        "bar_count": len(engine.bars), "tick_count": engine.tick_count,
        "last_tick_time": engine.last_tick_time,
        "last_tick_dt_utc": datetime.fromtimestamp(engine.last_tick_time, tz=timezone.utc).isoformat() if engine.last_tick_time else None,
        "last_price": engine.last_price, "now_utc": datetime.now(timezone.utc).isoformat(),
        "current_utc_hour": cur_h, "is_blocked_now": cur_h in BLOCKED_HOURS_UTC,
        "blocked_hours_utc": BLOCKED_HOURS_UTC, "blocked_setups": blocked,
        "open_positions": open_n, "closed_trades": closed_n,
        "equity": round(eq, 2),
        "broker_queue": engine.strategy._broker_q.qsize(),
        "notify_queue": engine.strategy._notify_q.qsize(),
        "stuck_tickets": stuck,
        "broker_mirror": mirror,
    })

@app.route("/api/reconcile", methods=["POST", "GET"])
def force_reconcile():
    """Manual trigger from dashboard."""
    engine.strategy._reconcile_trigger.set()
    return jsonify({"triggered": True})


# ============================ MAIN ============================

def main():
    print("=" * 60)
    print(f"  KOKO LIVE ENGINE — {SYMBOL} {RANGE_SIZE}pt  |  LIVE_TRADING={LIVE_TRADING}")
    print(f"  Strategy: streak={STREAK_SIZE} tp={TP_CLOSE_AFTER} sl={FIXED_SL_POINTS}pt")
    print(f"  Blocked UTC hours: {BLOCKED_HOURS_UTC}")
    print(f"  Reconcile every {RECONCILE_INTERVAL_S}s + on-demand after any close failure")
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
        # Optional: uncomment to force-close everything on Ctrl+C
        # engine.strategy.emergency_close_all()
        mt5.shutdown()


if __name__ == "__main__":
    main()