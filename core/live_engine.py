"""
Koko Live Engine — USTEC streaming + 2-streak/3-tp strategy
============================================================
- MT5 tick stream → Koko bars (8pt grid, 2x rev, clean ON)
- Warmup: pulls recent ticks, builds bars up to NOW, then goes live
- Strategy: 1:1 with backtester, OVERLAPPING positions allowed
- Risk: scaling, $1 per $100 balance, 1pt × 1lot = $1
- Flask serves chart.html + JSON state on :5000
"""

import time
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
WARMUP_LOOKBACK_DAYS  = 7      # auto-extends if not enough bars

# Strategy
STREAK_SIZE           = 2
TP_CLOSE_AFTER        = 3
FIXED_SL_POINTS       = 16.0
SLIPPAGE_POINTS       = 0.3
COMMISSION_PER_LOT    = 0.8    # one-side, scales with lots
POINT_VALUE_PER_LOT   = 1.0    # 1pt × 1lot = $1

# Risk (scaling: 1$ per 100$ of balance)
STARTING_BALANCE      = 3100.0
RISK_PER_100          = 1.0
MIN_LOTS              = 0.01
MAX_LOTS              = 600.0

# Execution
LIVE_TRADING          = True
MAGIC_NUMBER          = 770808
DEVIATION_POINTS      = 20
TICK_POLL_MS          = 50

# Web
HTTP_PORT             = 5000
CHART_MAX_BARS        = 500    # payload trim

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
            "time":   tick["ts"],
            "open":   self.level,
            "high":   self.level,
            "low":    self.level,
            "close":  self.level,
            "volume": 0.0,
        }

    def get_working_bar(self):
        if self.bar is None:
            return None
        ts = self.bar["time"]
        if self._last_emitted_ts is not None and ts <= self._last_emitted_ts:
            ts = self._last_emitted_ts + 1
        return {
            "time":   int(ts),
            "open":   round(self.bar["open"],  self.pd),
            "high":   round(self.bar["high"],  self.pd),
            "low":    round(self.bar["low"],   self.pd),
            "close":  round(self.bar["close"], self.pd),
            "volume": round(self.bar["volume"], 2),
        }

    def process_tick(self, tick):
        p  = tick["price"]
        v  = tick["volume"]
        rs = self.rs

        if self.bar is None:
            self.level = self._snap(p)
            self.bar = {
                "time": tick["ts"], "open": self.level, "high": self.level,
                "low": self.level, "close": self.level, "volume": v,
            }
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
                else:
                    break

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
                        b_open  = lvl
                        b_close = rev_t
                        b_high  = self.bar["high"]
                        b_low   = min(self.bar["low"], b_close)
                    self._emit(self._make_bar(b_open, b_high, b_low, b_close))
                    self.trend = -1; self.level = rev_t; self._reset_bar(tick); continue
                else:
                    break

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
                        b_open  = lvl
                        b_close = rev_t
                        b_high  = max(self.bar["high"], b_close)
                        b_low   = self.bar["low"]
                    self._emit(self._make_bar(b_open, b_high, b_low, b_close))
                    self.trend = 1; self.level = rev_t; self._reset_bar(tick); continue
                else:
                    break


# ============================ HELPERS ============================

def candle_dir(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0


def tick_to_price(t):
    """Convert MT5 named-tuple or struct to our {ts, price, volume} format."""
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


# ============================ STRATEGY ============================

class StrategyEngine:
    def __init__(self):
        self.positions     = []
        self.trades        = []
        self.pending_validations = []
        self.validations   = []
        self.markers       = []
        self.warmup_done   = False
        self.equity        = 0.0
        self._trade_id_seq = 0
        self._validate_fn  = None

    def _next_trade_id(self):
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
        return max(MIN_LOTS, min(MAX_LOTS, round(raw, 2)))

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

    def _open_position(self, direction, signal_bar):
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
            "dir":           direction,
            "entry_price":   round(entry_price, PRICE_DECIMALS),
            "sl_price":      round(sl_price,    PRICE_DECIMALS),
            "lots":          lots,
            "entry_time":    signal_bar["time"],
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

        if LIVE_TRADING:
            self._send_mt5_order(pos)

        print(f"[ENTRY #{tid}] {'LONG' if direction == 1 else 'SHORT'} @ {entry_price:.2f}  "
              f"SL={sl_price:.2f}  lots={lots}  risk=${pos['risk_used']:.2f}  (open={len(self.positions)})")

        tg.notify_entry(
            trade_id=tid, direction=direction, entry=entry_price, sl=sl_price,
            lots=lots, bar_time=signal_bar["time"], position_count=len(self.positions),
        )

    def _close_position(self, pos, exit_price, reason, exit_time):
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
        }
        self.trades.append(trade)

        self.markers.append({
            "time":     exit_time,
            "position": "aboveBar" if pos["dir"] == 1 else "belowBar",
            "color":    "#26c6da" if reason == "TP" else "#ff9800",
            "shape":    "circle",
            "text":     f"#{pos['id']} {reason} {net:+.2f}",
        })

        if LIVE_TRADING and pos["broker_ticket"] is not None:
            self._close_mt5_order(pos)

        if pos in self.positions:
            self.positions.remove(pos)

        print(f"[EXIT/{reason} #{pos['id']}] @ {exit_price:.2f}  "
              f"pnl={net:+.2f}$  bars_held={pos['bars_held']}  equity=${self.equity:.2f}  "
              f"(open={len(self.positions)})")

        tg.notify_exit(
            trade_id=pos["id"], direction=pos["dir"], entry=pos["entry_price"],
            exit_px=exit_price, net_pnl=net, reason=reason,
            bars_held=pos["bars_held"], equity=self.equity,
            position_count=len(self.positions),
        )

        if self._validate_fn is not None:
            self.pending_validations.append(trade)
            self._run_pending_validations()
    def _run_pending_validations(self):
        """Try to validate any closed trades whose backtest window now exists."""
        still_pending = []
        for trade in self.pending_validations:
            try:
                v = self._validate_fn(trade)
                if v["status"] == "SKIP" and any("insufficient post-signal" in i for i in v.get("issues", [])):
                    # bars not built up yet; retry on next bar close
                    still_pending.append(trade)
                    continue
                self.validations.append(v)
                print(validator.format_report(v))
                tg.notify_validation(v)
            except Exception as e:
                print(f"[VALIDATOR-ERR] {e}")
                still_pending.append(trade)
        self.pending_validations = still_pending
    def on_tick(self, tick):
        if not self.positions:
            return
        price = tick["price"]
        for pos in list(self.positions):
            if pos["dir"] == 1:
                fav = price - pos["entry_price"]
                adv = pos["entry_price"] - price
            else:
                fav = pos["entry_price"] - price
                adv = price - pos["entry_price"]
            if fav > pos["mfe_points"]: pos["mfe_points"] = fav
            if adv > pos["mae_points"]: pos["mae_points"] = adv

            if pos["dir"] == 1 and price <= pos["sl_price"]:
                self._close_position(pos, pos["sl_price"] - SLIPPAGE_POINTS, "SL", tick["ts"])
            elif pos["dir"] == -1 and price >= pos["sl_price"]:
                self._close_position(pos, pos["sl_price"] + SLIPPAGE_POINTS, "SL", tick["ts"])

    def on_bar_close(self, bar, all_bars):
        # 1) age existing positions, TP-exit any that hit horizon
        for pos in list(self.positions):
            pos["bars_held"] += 1
            if pos["bars_held"] >= TP_CLOSE_AFTER + 1:
                if pos["dir"] == 1:
                    exit_price = bar["close"] - SLIPPAGE_POINTS
                else:
                    exit_price = bar["close"] + SLIPPAGE_POINTS
                self._close_position(pos, exit_price, "TP", bar["time"])

        # 2) scan for new signal
        if not self.warmup_done:
            return
        sig = self._check_signal(all_bars)
        if sig != 0:
            self._open_position(sig, bar)

        # 3) retry any deferred validations now that we have more bars
        if self.pending_validations:
            self._run_pending_validations()
    # ── MT5 EXECUTION ──────────────────────────────────────────

    def _send_mt5_order(self, pos):
        t = mt5.symbol_info_tick(SYMBOL)
        if t is None:
            print("[MT5] no tick, order aborted")
            return
        price = t.ask if pos["dir"] == 1 else t.bid
        order_type = mt5.ORDER_TYPE_BUY if pos["dir"] == 1 else mt5.ORDER_TYPE_SELL

        req = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       SYMBOL,
            "volume":       pos["lots"],
            "type":         order_type,
            "price":        price,
            "sl":           pos["sl_price"],
            "deviation":    DEVIATION_POINTS,
            "magic":        MAGIC_NUMBER,
            "comment":      f"koko_#{pos['id']}",
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(req)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"[MT5] order failed: {result}")
            return
        pos["broker_ticket"] = result.order
        print(f"[MT5] #{pos['id']} filled ticket={result.order}")

    def _close_mt5_order(self, pos):
        t = mt5.symbol_info_tick(SYMBOL)
        if t is None:
            return
        close_type = mt5.ORDER_TYPE_SELL if pos["dir"] == 1 else mt5.ORDER_TYPE_BUY
        price = t.bid if pos["dir"] == 1 else t.ask
        req = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       SYMBOL,
            "volume":       pos["lots"],
            "type":         close_type,
            "position":     pos["broker_ticket"],
            "price":        price,
            "deviation":    DEVIATION_POINTS,
            "magic":        MAGIC_NUMBER,
            "comment":      f"koko_close_#{pos['id']}",
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(req)
        print(f"[MT5] close #{pos['id']} result: {result}")


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
        """Feed a chronological array of MT5 ticks through streamer + strategy."""
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
        """Process recent ticks until we have enough bars built up to NOW.
        After: self.bars has the most recent N bars, self.last_price is current."""
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

            # reset state so re-attempts don't double-process
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

        # trim self.bars to last (WARMUP_BARS * 2) so memory + payload stay sane
        keep = max(WARMUP_BARS, 200)
        if len(self.bars) > keep:
            self.bars = self.bars[-keep:]

        # GAP FILL: catch any ticks that arrived during warmup processing
        gap_start = datetime.fromtimestamp(self.last_tick_time, tz=timezone.utc)
        gap_end   = datetime.now(timezone.utc)
        if (gap_end - gap_start).total_seconds() > 1:
            print(f"[WARMUP] gap-filling from {gap_start.isoformat()} → now")
            gap = mt5.copy_ticks_range(SYMBOL, gap_start, gap_end, mt5.COPY_TICKS_ALL)
            if gap is not None and len(gap) > 0:
                print(f"[WARMUP] {len(gap):,} gap ticks → feeding")
                self._feed_ticks(gap)

        self.strategy.warmup_done = True
        print(f"\n{'='*60}\n  WARMUP COMPLETE — {len(self.bars)} bars\n"
              f"  Last price: {self.last_price}\n"
              f"  STRATEGY IS NOW LIVE\n{'='*60}\n")
        tg.notify_warmup_done(len(self.bars), self.last_price)

    def live_loop(self):
        print("[LIVE] tick poll started")
        last_heartbeat = time.time()
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

            with self.lock:
                self.strategy.on_tick(t)
                self.streamer.process_tick(t)

            self.last_tick_time = ts
            self.last_price     = price
            self.last_bid       = t["bid"]
            self.last_ask       = t["ask"]
            self.tick_count    += 1

            now = time.time()
            if now - last_heartbeat >= 10.0:
                print(f"[HEARTBEAT] ticks={self.tick_count}  price={price}  "
                      f"bars={len(self.bars)}  pos={len(self.strategy.positions)}  "
                      f"equity=${self.strategy.equity:.2f}")
                last_heartbeat = now

            time.sleep(TICK_POLL_MS / 1000.0)

    def snapshot(self):
        with self.lock:
            working = self.streamer.get_working_bar()
            chart_bars = self.bars[-CHART_MAX_BARS:] if len(self.bars) > CHART_MAX_BARS else self.bars
            return {
                "bars":             list(chart_bars),
                "working_bar":      working,
                "markers":          list(self.strategy.markers),
                "pending_validations": len(self.strategy.pending_validations),
                "trades":           list(self.strategy.trades),
                "validations":      list(self.strategy.validations),
                "positions":        [dict(p) for p in self.strategy.positions],
                "warmup_done":      self.strategy.warmup_done,
                "bar_count":        len(self.bars),
                "equity":           round(self.strategy.equity, 2),
                "balance":          round(STARTING_BALANCE + self.strategy.equity, 2),
                "starting_balance": STARTING_BALANCE,
                "last_price":       self.last_price,
                "last_bid":         self.last_bid,
                "last_ask":         self.last_ask,
                "tick_count":       self.tick_count,
                "symbol":           SYMBOL,
                "live_trading":     LIVE_TRADING,
                "config": {
                    "range":        RANGE_SIZE,
                    "rev":          REV_BRICKS,
                    "clean":        CLEAN_MODE,
                    "streak":       STREAK_SIZE,
                    "tp":           TP_CLOSE_AFTER,
                    "sl":           FIXED_SL_POINTS,
                    "risk_per_100": RISK_PER_100,
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
    return jsonify({
        "running":          engine.running,
        "warmup_done":      engine.strategy.warmup_done,
        "bar_count":        len(engine.bars),
        "tick_count":       engine.tick_count,
        "last_tick_time":   engine.last_tick_time,
        "last_tick_dt_utc": datetime.fromtimestamp(engine.last_tick_time, tz=timezone.utc).isoformat() if engine.last_tick_time else None,
        "last_price":       engine.last_price,
        "now_utc":          datetime.now(timezone.utc).isoformat(),
    })


# ============================ MAIN ============================

def main():
    print("=" * 60)
    print(f"  KOKO LIVE ENGINE — {SYMBOL} {RANGE_SIZE}pt  |  LIVE_TRADING={LIVE_TRADING}")
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
        mt5.shutdown()


if __name__ == "__main__":
    main()