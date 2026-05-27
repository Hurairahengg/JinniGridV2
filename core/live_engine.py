"""
Koko Live Engine — USTEC 8pt streaming + 2-streak/3-tp strategy
=================================================================
- MT5 tick stream  →  Koko candles (8pt grid, 2x rev, clean ON)
- Warmup: pulls historical ticks until 100 bars generated, THEN goes live
- Strategy (1:1 backtest):
    * 2 same-color bricks (streak)
    * 1 opposite brick (reversal candle)
    * enter immediately at market in reversal direction
    * SL = entry ∓ 16pt (intrabar tick-level kill)
    * exit on close of bar (entry + 3)   ← TP_CLOSE_AFTER bars after entry bar
- LIVE_TRADING = False by default → sim mode, no MT5 orders fired
- Flask serves chart.html on :5000 with all bars + entry/exit markers

Run:  python live_engine.py
"""
import validator
import json
import time
import threading
from datetime import datetime, timedelta, timezone

import MetaTrader5 as mt5
import telegram_bot as tg
from flask import Flask, jsonify, send_from_directory

# ============================ CONFIG ============================

SYMBOL                = "USTEC"          # adjust to your broker's symbol (NAS100, USTEC.cash, etc)
RANGE_SIZE            = 8.0
REV_BRICKS            = 2.0
CLEAN_MODE            = True
PRICE_DECIMALS        = 1
WARMUP_BARS           = 100
WARMUP_LOOKBACK_DAYS  = 7

# Strategy (matches backtest)
STREAK_SIZE           = 2
TP_CLOSE_AFTER        = 3
FIXED_SL_POINTS       = 16.0
SLIPPAGE_POINTS       = 0.3
COMMISSION_PER_LOT    = 0.8
FLAT_RISK_DOLLARS     = 10.0

# Execution
LIVE_TRADING          = False            # ← flip to True to actually order_send
MAGIC_NUMBER          = 770808
DEVIATION_POINTS      = 20
TICK_POLL_MS          = 50

# Web
HTTP_PORT             = 5000

# =================== KOKO STREAMER (1:1 range_bars.py) ===================

class KokoCandleStreamer:
    """
    Identical brick math to range_bars.py KokoCandleStreamer.
    Difference: emits via callback (no file IO), and exposes get_working_bar()
    so the live chart can render the forming brick in real time.
    """

    def __init__(self, range_size, price_decimals, rev_bricks, clean_mode, on_bar_close):
        self.rs         = float(range_size)
        self.pd         = price_decimals
        self.rev_bricks = rev_bricks
        self.clean_mode = clean_mode
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
        p = tick["price"]
        v = tick["volume"]
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

            # ── STARTUP ──────────────────────────────────────
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

            # ── BULL ─────────────────────────────────────────
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

            # ── BEAR ─────────────────────────────────────────
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


# ============================ STRATEGY ============================

def candle_dir(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0


class StrategyEngine:
    """
    1:1 backtest logic with OVERLAPPING positions.
    - last STREAK_SIZE bars same color, last bar opposite → signal fires every time
    - each position lives independently: own entry, own SL, own bars_held counter
    - SL checked per-tick on every open position
    - TP: exit on close of the bar where bars_held == TP_CLOSE_AFTER + 1
    """

    def __init__(self):
        self.positions     = []
        self.trades        = []
        self.validations   = []   # list of validator results, one per closed trade
        self.markers       = []
        self.warmup_done   = False
        self.equity        = 0.0
        self._trade_id_seq = 0
        self._validate_fn  = None   # injected by LiveEngine so we can pass bars

    def _next_trade_id(self):
        self._trade_id_seq += 1
        return self._trade_id_seq

    def _calc_lots(self):
        raw = FLAT_RISK_DOLLARS / FIXED_SL_POINTS
        return max(0.01, min(600.0, round(raw, 2)))

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

    # ── OPEN ─────────────────────────────────────────────────────

    def _open_position(self, direction, signal_bar):
        theoretical_entry = signal_bar["close"]
        if direction == 1:
            entry_price = theoretical_entry + SLIPPAGE_POINTS
            sl_price    = entry_price - FIXED_SL_POINTS
        else:
            entry_price = theoretical_entry - SLIPPAGE_POINTS
            sl_price    = entry_price + FIXED_SL_POINTS

        lots = self._calc_lots()
        tid  = self._next_trade_id()

        pos = {
            "id":            tid,
            "dir":           direction,
            "entry_price":   round(entry_price, PRICE_DECIMALS),
            "sl_price":      round(sl_price,    PRICE_DECIMALS),
            "lots":          lots,
            "entry_time":    signal_bar["time"],
            "bars_held":     1,    # signal bar = entry bar = bar 1
            "mfe_points":    0.0,
            "mae_points":    0.0,
            "broker_ticket": None,
        }
        self.positions.append(pos)

        self.markers.append({
            "time":     signal_bar["time"],
            "position": "belowBar" if direction == 1 else "aboveBar",
            "color":    "#4caf50" if direction == 1 else "#ef5350",
            "shape":    "arrowUp" if direction == 1 else "arrowDown",
            "text":     f"#{tid} {'LONG' if direction == 1 else 'SHORT'} @ {entry_price:.1f}",
        })

        if LIVE_TRADING:
            self._send_mt5_order(pos)

        print(f"[ENTRY #{tid}] {'LONG' if direction == 1 else 'SHORT'} @ {entry_price:.1f}  SL={sl_price:.1f}  lots={lots}  (open={len(self.positions)})")

        tg.notify_entry(
            trade_id=tid,
            direction=direction,
            entry=entry_price,
            sl=sl_price,
            lots=lots,
            bar_time=signal_bar["time"],
            position_count=len(self.positions),
        )

    # ── CLOSE ────────────────────────────────────────────────────

    def _close_position(self, pos, exit_price, reason, exit_time):
        if pos["dir"] == 1:
            pnl_points = exit_price - pos["entry_price"]
        else:
            pnl_points = pos["entry_price"] - exit_price

        gross      = pnl_points * pos["lots"]
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
            "pnl_points": round(pnl_points, 2),
            "net_pnl":    round(net, 2),
            "reason":     reason,
            "entry_time": pos["entry_time"],
            "exit_time":  exit_time,
            "bars_held":  pos["bars_held"],
            "mfe_points": round(pos["mfe_points"], 2),
            "mae_points": round(pos["mae_points"], 2),
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

        print(f"[EXIT/{reason} #{pos['id']}] @ {exit_price:.1f}  pnl={net:+.2f}$  bars_held={pos['bars_held']}  equity=${self.equity:.2f}  (open={len(self.positions)})")

        tg.notify_exit(
            trade_id=pos["id"],
            direction=pos["dir"],
            entry=pos["entry_price"],
            exit_px=exit_price,
            net_pnl=net,
            reason=reason,
            bars_held=pos["bars_held"],
            equity=self.equity,
            position_count=len(self.positions),
        )
        # ── run the validator on this closed trade ────────────
        if self._validate_fn is not None:
            try:
                v_result = self._validate_fn(trade)
                self.validations.append(v_result)
                print(validator.format_report(v_result))
                tg.notify_validation(v_result)
            except Exception as e:
                print(f"[VALIDATOR-ERR] {e}")

    # ── TICK HOOK (per-position SL check) ────────────────────────

    def on_tick(self, tick):
        if not self.positions:
            return
        price = tick["price"]
        # iterate over copy so we can close mid-loop
        for pos in list(self.positions):
            # MFE/MAE
            if pos["dir"] == 1:
                fav = price - pos["entry_price"]
                adv = pos["entry_price"] - price
            else:
                fav = pos["entry_price"] - price
                adv = price - pos["entry_price"]
            if fav > pos["mfe_points"]: pos["mfe_points"] = fav
            if adv > pos["mae_points"]: pos["mae_points"] = adv

            # SL
            if pos["dir"] == 1 and price <= pos["sl_price"]:
                exit_price = pos["sl_price"] - SLIPPAGE_POINTS
                self._close_position(pos, exit_price, "SL", tick["ts"])
            elif pos["dir"] == -1 and price >= pos["sl_price"]:
                exit_price = pos["sl_price"] + SLIPPAGE_POINTS
                self._close_position(pos, exit_price, "SL", tick["ts"])

    # ── BAR CLOSE HOOK ───────────────────────────────────────────

    def on_bar_close(self, bar, all_bars):
        # 1) age existing positions and exit any that reach TP horizon
        for pos in list(self.positions):
            pos["bars_held"] += 1
            if pos["bars_held"] >= TP_CLOSE_AFTER + 1:
                if pos["dir"] == 1:
                    exit_price = bar["close"] - SLIPPAGE_POINTS
                else:
                    exit_price = bar["close"] + SLIPPAGE_POINTS
                self._close_position(pos, exit_price, "TP", bar["time"])

        # 2) signal scan — ALWAYS, regardless of open positions (overlap)
        if not self.warmup_done:
            return
        if len(all_bars) < STREAK_SIZE + 1:
            return

        sig = self._check_signal(all_bars)
        if sig == 0:
            return

        self._open_position(sig, bar)

    # ── MT5 EXECUTION ────────────────────────────────────────────

    def _send_mt5_order(self, pos):
        tick = mt5.symbol_info_tick(SYMBOL)
        if tick is None:
            print("[MT5] no tick, order aborted")
            return
        price = tick.ask if pos["dir"] == 1 else tick.bid
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
        tick = mt5.symbol_info_tick(SYMBOL)
        if tick is None:
            return
        close_type = mt5.ORDER_TYPE_SELL if pos["dir"] == 1 else mt5.ORDER_TYPE_BUY
        price = tick.bid if pos["dir"] == 1 else tick.ask
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
            range_size=RANGE_SIZE,
            price_decimals=PRICE_DECIMALS,
            rev_bricks=REV_BRICKS,
            clean_mode=CLEAN_MODE,
            on_bar_close=self._on_bar_close,
        )
        self.last_tick_time = 0
        self.last_price     = None
        self.lock           = threading.Lock()
        self.running        = True

    def _on_bar_close(self, bar):
        with self.lock:
            self.bars.append(bar)
            self.strategy.on_bar_close(bar, self.bars)
            if not self.strategy.warmup_done and len(self.bars) >= WARMUP_BARS:
                self.strategy.warmup_done = True
                print(f"\n{'='*60}\n  WARMUP COMPLETE — {len(self.bars)} bars built\n  STRATEGY IS NOW LIVE\n{'='*60}\n")
                tg.notify_warmup_done(len(self.bars), self.last_price)

    # ── MT5 INIT ─────────────────────────────────────────────────

    def init_mt5(self):
        if not mt5.initialize():
            raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")
        info = mt5.symbol_info(SYMBOL)
        if info is None:
            raise RuntimeError(f"Symbol {SYMBOL} not found")
        if not info.visible:
            mt5.symbol_select(SYMBOL, True)
        print(f"[MT5] connected. symbol={SYMBOL} digits={info.digits} point={info.point}")

    # ── WARMUP ───────────────────────────────────────────────────

    def warmup(self):
        print(f"[WARMUP] fetching last {WARMUP_LOOKBACK_DAYS}d of ticks...")
        end   = datetime.now(timezone.utc)
        start = end - timedelta(days=WARMUP_LOOKBACK_DAYS)

        ticks = mt5.copy_ticks_range(SYMBOL, start, end, mt5.COPY_TICKS_ALL)
        if ticks is None or len(ticks) == 0:
            raise RuntimeError("no warmup ticks returned")

        print(f"[WARMUP] {len(ticks):,} ticks pulled, streaming through bar builder...")

        for t in ticks:
            bid = float(t["bid"])
            ask = float(t["ask"])
            if bid > 0 and ask > 0:
                price = (bid + ask) / 2.0
            elif "last" in t.dtype.names and float(t["last"]) > 0:
                price = float(t["last"])
            elif bid > 0:
                price = bid
            elif ask > 0:
                price = ask
            else:
                continue

            vol = float(t["volume"]) if "volume" in t.dtype.names else 0.0
            ts  = int(t["time"])

            self.streamer.process_tick({"ts": ts, "price": price, "volume": vol})
            self.last_tick_time = ts
            self.last_price = price

            if len(self.bars) >= WARMUP_BARS and self.strategy.warmup_done:
                break

        if not self.strategy.warmup_done:
            print(f"[WARMUP] only {len(self.bars)} bars built from {WARMUP_LOOKBACK_DAYS}d — extend lookback or wait for live")
        else:
            print(f"[WARMUP] done. {len(self.bars)} bars, last price={self.last_price}")

    # ── LIVE LOOP ────────────────────────────────────────────────

    def live_loop(self):
        print("[LIVE] tick poll started")
        while self.running:
            tick = mt5.symbol_info_tick(SYMBOL)
            if tick is None:
                time.sleep(TICK_POLL_MS / 1000.0)
                continue

            ts = int(tick.time)
            bid, ask = float(tick.bid), float(tick.ask)
            if bid > 0 and ask > 0:
                price = (bid + ask) / 2.0
            elif bid > 0:
                price = bid
            elif ask > 0:
                price = ask
            else:
                time.sleep(TICK_POLL_MS / 1000.0)
                continue
            vol = float(tick.volume) if hasattr(tick, "volume") else 0.0

            # dedupe: only feed if time advanced OR price changed
            if ts < self.last_tick_time:
                ts = self.last_tick_time
            if ts == self.last_tick_time and price == self.last_price:
                time.sleep(TICK_POLL_MS / 1000.0)
                continue

            t_obj = {"ts": ts, "price": price, "volume": vol}
            with self.lock:
                self.strategy.on_tick(t_obj)           # SL check at tick resolution
                self.streamer.process_tick(t_obj)      # may fire on_bar_close

            self.last_tick_time = ts
            self.last_price     = price

            time.sleep(TICK_POLL_MS / 1000.0)

    def snapshot(self):
        with self.lock:
            working = self.streamer.get_working_bar()
            return {
                "bars":         list(self.bars),
                "working_bar":  working,
                "markers":      list(self.strategy.markers),
                "trades":       list(self.strategy.trades),
                "positions":    [dict(p) for p in self.strategy.positions],
                "warmup_done":  self.strategy.warmup_done,
                "validations":  list(self.strategy.validations),
                "bar_count":    len(self.bars),
                "equity":       round(self.strategy.equity, 2),
                "last_price":   self.last_price,
                "symbol":       SYMBOL,
                "live_trading": LIVE_TRADING,
                "config": {
                    "range":   RANGE_SIZE,
                    "rev":     REV_BRICKS,
                    "clean":   CLEAN_MODE,
                    "streak":  STREAK_SIZE,
                    "tp":      TP_CLOSE_AFTER,
                    "sl":      FIXED_SL_POINTS,
                    "risk":    FLAT_RISK_DOLLARS,
                },
            }


# ============================ FLASK ============================

engine = LiveEngine()
app = Flask(__name__, static_folder=".", static_url_path="")

@app.route("/")
def index():
    return send_from_directory(".", "chart.html")

@app.route("/api/state")
def state():
    return jsonify(engine.snapshot())


# ============================ MAIN ============================

def main():
    print("="*60)
    print(f"  KOKO LIVE ENGINE — {SYMBOL} {RANGE_SIZE}pt  |  LIVE_TRADING={LIVE_TRADING}")
    print("="*60)

    engine.init_mt5()
    tg.notify_start(
        symbol=SYMBOL,
        range_size=RANGE_SIZE,
        rev=REV_BRICKS,
        clean=CLEAN_MODE,
        streak=STREAK_SIZE,
        tp=TP_CLOSE_AFTER,
        sl=FIXED_SL_POINTS,
        risk=FLAT_RISK_DOLLARS,
        live_trading=LIVE_TRADING,
    )
    engine.warmup()

    t = threading.Thread(target=engine.live_loop, daemon=True)
    t.start()

    print(f"[WEB] chart → http://localhost:{HTTP_PORT}")
    try:
        app.run(host="0.0.0.0", port=HTTP_PORT, debug=False, use_reloader=False)
    finally:
        engine.running = False
        mt5.shutdown()


if __name__ == "__main__":
    main()