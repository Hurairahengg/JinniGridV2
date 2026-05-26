"""
live_engine.py — Live execution for the 3-streak / 3-TP / 16pt-SL strategy.

- Symbol:   USTEC
- Bars:     8pt Koko (grid-anchored range bars), rev_bricks=2, clean_mode=OFF
- Strategy: streak_size=3, tp_close_after=3, FIXED_SL=16pt
- Warmup:   100 bars from MT5 tick history
- Memory:   max 100 bars in RAM at all times
- Match:    bar generator & strategy port the original Python 1:1
"""

import os
import json
import time
import math
import sys
from collections import deque
from datetime import datetime, timedelta, timezone

import MetaTrader5 as mt5

import telegram_bot
import validator

# ═════════════════════════════════════════════════════════════════
# CONFIG
# ═════════════════════════════════════════════════════════════════
SYMBOL              = "USTEC"

# Bar engine
BRICK_SIZE          = 8.0
REV_BRICKS          = 2.0
CLEAN_MODE          = True
PRICE_DECIMALS      = 2
MAX_BARS_IN_MEM     = 100
WARMUP_BARS         = 100

# Strategy
STREAK_SIZE         = 2
TP_CLOSE_AFTER      = 3
FIXED_SL_POINTS     = 16.0

# Costs (theoretical — for parity with backtester)
SLIPPAGE_POINTS     = 0.3
COMMISSION_PER_LOT  = 0.8

# Risk — matches backtester scaling model
RISK_PER_100        = 1.0     # $ risked per $100 of balance (1.0 = 1%)
SCALING_ENABLED     = True   # True = check live balance each trade
                              # False = lock to STARTING_BALANCE for all trades
STARTING_BALANCE    = 3100.0  # used when SCALING_ENABLED = False
POINT_VALUE_PER_LOT = 1.0     # $/pt/lot for USTEC. VERIFY YOUR BROKER.
MIN_LOTS            = 0.01
MAX_LOTS            = 600.0

# MT5 order
MAGIC_NUMBER        = 778899
ORDER_COMMENT       = "JinniSnapback"
DEVIATION_POINTS    = 30     # max slippage MT5 will accept on market order

# Loop
TICK_POLL_INTERVAL  = 0.05  # seconds between tick polls
HISTORY_LOOKBACK_DAYS = 14   # how far back to scan ticks for warmup

# Files
TRADE_LOG_FILE      = "trades_log.json"

# ═════════════════════════════════════════════════════════════════
# LIVE KOKO BAR STREAMER  (in-memory mirror of range_bars.py)
# ═════════════════════════════════════════════════════════════════
class LiveKokoCandleStreamer:
    """
    Same state machine as KokoCandleStreamer in range_bars.py.
    No file I/O. Calls `on_bar_emit(bar, bar_index)` for each emitted bar.
    Holds a rolling window of MAX_BARS_IN_MEM bars in self.bars.
    """

    def __init__(self, range_size, rev_bricks, clean_mode, price_decimals,
                 max_bars=MAX_BARS_IN_MEM, on_bar_emit=None):
        self.rs = float(range_size)
        self.rev_bricks = float(rev_bricks)
        self.clean_mode = clean_mode
        self.pd = price_decimals
        self.bars = deque(maxlen=max_bars)
        self.on_bar_emit = on_bar_emit

        self.trend = 0
        self.level = None
        self.bar = None
        self.global_bar_count = 0   # never resets, even when deque rolls
        self._last_written_ts = None

    def _snap(self, price):
        return round(round(price / self.rs) * self.rs, self.pd)

    def _emit(self, open_, high_, low_, close_, ts, volume):
        # de-dup timestamps (mirror original)
        if self._last_written_ts is not None and ts <= self._last_written_ts:
            ts = self._last_written_ts + 1
        self._last_written_ts = ts

        bar = {
            "time":   int(ts),
            "open":   round(open_,  self.pd),
            "high":   round(high_,  self.pd),
            "low":    round(low_,   self.pd),
            "close":  round(close_, self.pd),
            "volume": round(volume, 2),
        }
        self.bars.append(bar)
        self.global_bar_count += 1
        if self.on_bar_emit:
            self.on_bar_emit(bar, self.global_bar_count - 1)

    def _reset_working(self, ts):
        self.bar = {
            "time": ts,
            "open": self.level, "high": self.level,
            "low":  self.level, "close": self.level,
            "volume": 0.0,
        }

    def process_tick(self, ts, price, volume):
        rs = self.rs

        if self.bar is None:
            self.level = self._snap(price)
            self.bar = {
                "time": ts,
                "open": self.level, "high": self.level,
                "low":  self.level, "close": self.level,
                "volume": volume,
            }
            return

        self.bar["volume"] += volume
        if price > self.bar["high"]: self.bar["high"] = price
        if price < self.bar["low"]:  self.bar["low"]  = price

        while True:
            lvl = self.level

            if self.trend == 0:
                up_t   = round(lvl + rs, self.pd)
                down_t = round(lvl - rs, self.pd)
                if price >= up_t:
                    bh = max(self.bar["high"], up_t)
                    self._emit(lvl, bh, self.bar["low"], up_t, self.bar["time"], self.bar["volume"])
                    self.trend = 1; self.level = up_t; self._reset_working(ts); continue
                elif price <= down_t:
                    bl = min(self.bar["low"], down_t)
                    self._emit(lvl, self.bar["high"], bl, down_t, self.bar["time"], self.bar["volume"])
                    self.trend = -1; self.level = down_t; self._reset_working(ts); continue
                else:
                    break

            elif self.trend == 1:
                cont = round(lvl + rs, self.pd)
                rev  = round(lvl - self.rev_bricks * rs, self.pd)
                if price >= cont:
                    bh = max(self.bar["high"], cont)
                    self._emit(lvl, bh, self.bar["low"], cont, self.bar["time"], self.bar["volume"])
                    self.level = cont; self._reset_working(ts); continue
                elif price <= rev:
                    if self.clean_mode:
                        bc = rev
                        bo = round(rev + rs, self.pd)
                        bh = max(self.bar["high"], lvl)
                        bl = min(self.bar["low"], bc)
                    else:
                        bo = lvl; bc = rev
                        bh = self.bar["high"]
                        bl = min(self.bar["low"], bc)
                    self._emit(bo, bh, bl, bc, self.bar["time"], self.bar["volume"])
                    self.trend = -1; self.level = rev; self._reset_working(ts); continue
                else:
                    break

            else:   # trend == -1
                cont = round(lvl - rs, self.pd)
                rev  = round(lvl + self.rev_bricks * rs, self.pd)
                if price <= cont:
                    bl = min(self.bar["low"], cont)
                    self._emit(lvl, self.bar["high"], bl, cont, self.bar["time"], self.bar["volume"])
                    self.level = cont; self._reset_working(ts); continue
                elif price >= rev:
                    if self.clean_mode:
                        bc = rev
                        bo = round(rev - rs, self.pd)
                        bh = max(self.bar["high"], bc)
                        bl = min(self.bar["low"], lvl)
                    else:
                        bo = lvl; bc = rev
                        bh = max(self.bar["high"], bc)
                        bl = self.bar["low"]
                    self._emit(bo, bh, bl, bc, self.bar["time"], self.bar["volume"])
                    self.trend = 1; self.level = rev; self._reset_working(ts); continue
                else:
                    break


# ═════════════════════════════════════════════════════════════════
# TRADE LOG (json file, append on close)
# ═════════════════════════════════════════════════════════════════
def load_log():
    if not os.path.exists(TRADE_LOG_FILE):
        return []
    try:
        with open(TRADE_LOG_FILE) as f:
            return json.load(f)
    except Exception:
        return []

def save_log(log):
    with open(TRADE_LOG_FILE, "w") as f:
        json.dump(log, f, indent=2, default=str)


# ═════════════════════════════════════════════════════════════════
# STRATEGY ENGINE
# ═════════════════════════════════════════════════════════════════
def direction(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0


def get_current_risk():
    """
    Mirrors backtester:
      scaling ON  → risk = (live_balance / 100) * RISK_PER_100
      scaling OFF → risk = (STARTING_BALANCE / 100) * RISK_PER_100  (fixed)
    """
    if SCALING_ENABLED:
        acct = mt5.account_info()
        if acct is None:
            telegram_bot.send_error("account_info() returned None, using STARTING_BALANCE")
            balance = STARTING_BALANCE
        else:
            balance = acct.balance
    else:
        balance = STARTING_BALANCE
    return (balance / 100.0) * RISK_PER_100, balance


class StrategyEngine:
    """
    State machine:
        IDLE  →  signal found on emitted bar  →  place market order  →  IN_TRADE
        IN_TRADE  →  bars_since_entry counter ticks each new bar
                  →  exit on bar_count == TP_CLOSE_AFTER + 1 (TP), or MT5 SL trigger
                  →  log + telegram + validate  →  IDLE
    """

    STATE_IDLE     = "IDLE"
    STATE_IN_TRADE = "IN_TRADE"

    def __init__(self, streamer):
        self.streamer = streamer
        self.state = self.STATE_IDLE
        self.open_trade = None         # dict for currently-open trade
        self.bars_since_entry = 0      # increments on every NEW bar emit after entry
        self.live_bars_seen = 0        # post-warmup bars only — signals gated on this

    # ─── called by streamer for EVERY new bar (live + warmup) ───
    def on_bar(self, bar, global_idx, is_warmup):
        if is_warmup:
            return   # don't trade during warmup, only build history

        self.live_bars_seen += 1

        if self.state == self.STATE_IN_TRADE:
            self.bars_since_entry += 1
            # NEW: append this bar to the trade's window in real-time
            if self.open_trade is not None:
                self.open_trade["bars_window"].append(bar)
            # exit on TP if we've held through (TP_CLOSE_AFTER + 1) bars
            if self.bars_since_entry >= TP_CLOSE_AFTER + 1:
                self._close_trade_tp(bar)
                return
            return

        # IDLE — only allow signals once we have enough LIVE bars (no warmup contamination)
        required_live_bars = STREAK_SIZE + 1   # streak + reversal bar
        if self.live_bars_seen < required_live_bars:
            print(f"[live] bar #{self.live_bars_seen}/{required_live_bars} — building live history, no signals yet")
            return

        self._check_signal()

    def _check_signal(self):
        bars = list(self.streamer.bars)
        if len(bars) < STREAK_SIZE + 1:
            return

        signal_bar     = bars[-1]
        reversal_dir   = direction(signal_bar)
        if reversal_dir == 0:
            return

        # streak: bars[-1-STREAK_SIZE .. -2] all same direction = -reversal_dir
        streak_dir = -reversal_dir
        streak_slice = bars[-(STREAK_SIZE + 1):-1]
        for b in streak_slice:
            if direction(b) != streak_dir:
                return

        # ─── SIGNAL VALID — fire order ───
        self._open_trade(reversal_dir, bars, streak_slice, signal_bar)

    # ─── ORDER EXECUTION ────────────────────────────────────────
    def _open_trade(self, reversal_dir, bars_snapshot, streak_slice, signal_bar):
        # theoretical entry = next bar's open = current grid level
        theo_entry = self.streamer.level
        if reversal_dir == 1:
            theo_entry_priced = theo_entry + SLIPPAGE_POINTS
            theo_sl           = theo_entry_priced - FIXED_SL_POINTS
        else:
            theo_entry_priced = theo_entry - SLIPPAGE_POINTS
            theo_sl           = theo_entry_priced + FIXED_SL_POINTS

        # ─── dynamic risk + lot sizing (mirror backtester) ───
        risk, balance_used = get_current_risk()
        if risk <= 0:
            telegram_bot.send_error(f"non-positive risk (balance=${balance_used:.2f}), skipping trade")
            return
        raw_lots = risk / (FIXED_SL_POINTS * POINT_VALUE_PER_LOT)
        lots = max(MIN_LOTS, min(MAX_LOTS, raw_lots))
        # round to broker's volume step
        info = mt5.symbol_info(SYMBOL)
        if info is None:
            telegram_bot.send_error(f"symbol_info({SYMBOL}) returned None")
            return
        step = info.volume_step or 0.01
        lots = math.floor(lots / step) * step
        lots = max(info.volume_min, min(info.volume_max, lots))
        lots = round(lots, 2)

        # ─── place MT5 market order with attached SL ───
        order_type = mt5.ORDER_TYPE_BUY if reversal_dir == 1 else mt5.ORDER_TYPE_SELL
        tick = mt5.symbol_info_tick(SYMBOL)
        price = tick.ask if reversal_dir == 1 else tick.bid

        # compute live SL price based on actual ask/bid
        if reversal_dir == 1:
            live_sl = round(price - FIXED_SL_POINTS, PRICE_DECIMALS)
        else:
            live_sl = round(price + FIXED_SL_POINTS, PRICE_DECIMALS)

        request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       SYMBOL,
            "volume":       lots,
            "type":         order_type,
            "price":        price,
            "sl":           live_sl,
            "deviation":    DEVIATION_POINTS,
            "magic":        MAGIC_NUMBER,
            "comment":      ORDER_COMMENT,
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            err = result.comment if result else mt5.last_error()
            telegram_bot.send_error(f"order_send failed: {err}")
            return

        actual_entry = result.price
        ticket = result.order

        # build bars window for validator (streak + reversal + entry-bar-placeholder slots)
        # we'll backfill walk-forward bars as they emit, but store snapshot now
        bars_window = list(streak_slice) + [signal_bar]
        signal_idx_in_window = len(bars_window) - 1   # reversal bar position

        trade = {
            "status":            "open",
            "symbol":            SYMBOL,
            "ticket":            ticket,
            "dir":               reversal_dir,
            "lots":              lots,
            "risk_used":         risk,
            "balance_at_entry":  balance_used,
            "scaling_enabled":   SCALING_ENABLED,
            "entry_time":        datetime.now(timezone.utc).isoformat(),
            "theoretical_entry": theo_entry_priced,
            "actual_entry":      actual_entry,
            "theoretical_sl":    theo_sl,
            "actual_sl":         live_sl,
            "sl_price":          live_sl,
            "sl_pts":            FIXED_SL_POINTS,
            "streak_summary":    " ".join(f"{'+' if direction(b)==1 else '-'}" for b in streak_slice),
            "reversal_summary":  f"{'+' if reversal_dir==1 else '-'} O={signal_bar['open']} C={signal_bar['close']}",
            # validator needs these:
            "bars_window":            bars_window,    # will append walk-forward bars on close
            "signal_idx_in_window":   signal_idx_in_window,
            "entry_global_idx":       self.streamer.global_bar_count,  # next bar will be entry bar
        }

        self.open_trade = trade
        self.bars_since_entry = 0
        self.state = self.STATE_IN_TRADE

        telegram_bot.send_signal(trade)
        print(f"[trade] OPEN  ticket={ticket} dir={reversal_dir} entry={actual_entry} sl={live_sl} lots={lots}")

    def _close_trade_tp(self, exit_bar):
        """Send market close at the moment the TP bar emits."""
        if self.open_trade is None: return
        ticket = self.open_trade["ticket"]
        positions = mt5.positions_get(ticket=ticket)
        if not positions:
            # position already closed (SL hit before we got here) — handle via reconcile
            self._reconcile_closed_position(ticket, exit_bar, exit_reason="sl_or_external")
            return

        pos = positions[0]
        tick = mt5.symbol_info_tick(SYMBOL)
        is_long = (pos.type == mt5.POSITION_TYPE_BUY)
        close_type  = mt5.ORDER_TYPE_SELL if is_long else mt5.ORDER_TYPE_BUY
        close_price = tick.bid if is_long else tick.ask

        request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       SYMBOL,
            "volume":       pos.volume,
            "type":         close_type,
            "position":     ticket,
            "price":        close_price,
            "deviation":    DEVIATION_POINTS,
            "magic":        MAGIC_NUMBER,
            "comment":      "tp_close",
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            err = result.comment if result else mt5.last_error()
            telegram_bot.send_error(f"TP close failed: {err}")
            return

        self._finalize_trade(exit_bar, result.price, hit_sl=False)

    def _reconcile_closed_position(self, ticket, exit_bar, exit_reason):
        """If MT5 already closed the position (SL trigger), pull deal info and finalize."""
        # find the closing deal
        from_time = datetime.now(timezone.utc) - timedelta(hours=24)
        deals = mt5.history_deals_get(from_time, datetime.now(timezone.utc), position=ticket)
        if not deals:
            telegram_bot.send_error(f"could not find closing deal for ticket {ticket}")
            return
        close_deal = max(deals, key=lambda d: d.time)
        self._finalize_trade(exit_bar, close_deal.price, hit_sl=True)

    def _finalize_trade(self, exit_bar, actual_exit, hit_sl):
        t = self.open_trade
        actual_entry = t["actual_entry"]
        is_long = t["dir"] == 1
        pnl_points = (actual_exit - actual_entry) if is_long else (actual_entry - actual_exit)
        gross_pnl  = pnl_points * t["lots"] * POINT_VALUE_PER_LOT
        commission = t["lots"] * COMMISSION_PER_LOT
        net_pnl    = gross_pnl - commission

        # bars_window has been collected live during the trade (see on_bar).
        # Sanity check it:
        expected_total = (STREAK_SIZE + 1) + self.bars_since_entry
        actual_total   = len(t["bars_window"])
        if actual_total != expected_total:
            print(f"[validator] ⚠️ bars_window size mismatch: "
                  f"expected={expected_total} actual={actual_total} "
                  f"bars_since_entry={self.bars_since_entry}")

        t.update({
            "status":      "closed",
            "exit_time":   datetime.now(timezone.utc).isoformat(),
            "actual_exit": actual_exit,
            "hit_sl":      hit_sl,
            "pnl_points":  pnl_points,
            "gross_pnl":   gross_pnl,
            "commission":  commission,
            "net_pnl":     net_pnl,
            "bars_held":   self.bars_since_entry,
            "r_multiple":  net_pnl / t["risk_used"] if t["risk_used"] > 0 else 0.0,
        })

        # validate
        verdict = validator.validate_trade(t)
        t["validator_verdict"] = verdict

        # log
        log = load_log()
        log.append(t)
        save_log(log)

        # notify
        telegram_bot.send_close(t, verdict)
        match_tag = "MATCH" if verdict["match"] else "MISMATCH"
        print(f"[trade] CLOSE ticket={t['ticket']} pnl=${net_pnl:+.2f} "
              f"reason={'SL' if hit_sl else 'TP'} validator={match_tag}")

        self.open_trade = None
        self.state = self.STATE_IDLE
        self.bars_since_entry = 0

    # ─── poll MT5 each loop to detect broker-side SL closure ────
    def poll_position_status(self):
        if self.state != self.STATE_IN_TRADE: return
        if self.open_trade is None: return
        ticket = self.open_trade["ticket"]
        positions = mt5.positions_get(ticket=ticket)
        if not positions:
            # position closed by SL (or manual). reconcile.
            last_bar = self.streamer.bars[-1] if self.streamer.bars else None
            self._reconcile_closed_position(ticket, last_bar, exit_reason="sl")


# ═════════════════════════════════════════════════════════════════
# WARMUP — pull historical ticks, build first 100 bars
# ═════════════════════════════════════════════════════════════════
def warmup(streamer):
    print(f"[warmup] fetching historical ticks for {SYMBOL}…")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=HISTORY_LOOKBACK_DAYS)

    ticks = mt5.copy_ticks_range(SYMBOL, start, end, mt5.COPY_TICKS_ALL)
    if ticks is None or len(ticks) == 0:
        telegram_bot.send_error(f"warmup: no ticks from copy_ticks_range")
        return False

    print(f"[warmup] got {len(ticks):,} ticks, building bars…")
    last_ts = None
    for t in ticks:
        ts = int(t["time"])
        bid = float(t["bid"]); ask = float(t["ask"]); last = float(t["last"])
        if bid > 0 and ask > 0: price = (bid + ask) / 2.0
        elif last > 0:          price = last
        elif bid > 0:           price = bid
        elif ask > 0:           price = ask
        else: continue
        vol = float(t["volume"])
        if last_ts is not None and ts < last_ts: ts = last_ts
        last_ts = ts
        streamer.process_tick(ts, price, vol)

        if streamer.global_bar_count >= WARMUP_BARS:
            # keep going a bit to ensure deque is filled but we have enough
            if len(streamer.bars) >= MAX_BARS_IN_MEM:
                break

    print(f"[warmup] built {streamer.global_bar_count} bars total, "
          f"{len(streamer.bars)} in memory")
    return True


# ═════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═════════════════════════════════════════════════════════════════
def main():
    if not mt5.initialize():
        print(f"mt5.initialize() failed: {mt5.last_error()}")
        sys.exit(1)

    if not mt5.symbol_select(SYMBOL, True):
        telegram_bot.send_error(f"symbol_select({SYMBOL}) failed")
        sys.exit(1)

    info = mt5.symbol_info(SYMBOL)
    if info is None:
        telegram_bot.send_error(f"symbol_info({SYMBOL}) None")
        sys.exit(1)

    telegram_bot.send_boot({
        "boot_time":        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "symbol":           SYMBOL,
        "brick":            BRICK_SIZE,
        "rev":              REV_BRICKS,
        "max_bars":         MAX_BARS_IN_MEM,
        "streak":           STREAK_SIZE,
        "tp":               TP_CLOSE_AFTER,
        "sl":               FIXED_SL_POINTS,
        "scaling_enabled":  SCALING_ENABLED,
        "starting_balance": STARTING_BALANCE,
        "rp100":            RISK_PER_100,
        "pt_value":         POINT_VALUE_PER_LOT,
        "commission":       COMMISSION_PER_LOT,
        "slippage":         SLIPPAGE_POINTS,
    })

    is_warmup = [True]   # mutable holder so closure can flip it

    streamer = LiveKokoCandleStreamer(
        range_size=BRICK_SIZE,
        rev_bricks=REV_BRICKS,
        clean_mode=CLEAN_MODE,
        price_decimals=PRICE_DECIMALS,
        max_bars=MAX_BARS_IN_MEM,
    )
    engine = StrategyEngine(streamer)

    def on_bar(bar, gidx):
        engine.on_bar(bar, gidx, is_warmup=is_warmup[0])

    streamer.on_bar_emit = on_bar

    # ─── warmup ───
    ok = warmup(streamer)
    if not ok:
        print("warmup failed, aborting"); sys.exit(1)

    is_warmup[0] = False
    telegram_bot.send_warmup_done(streamer.global_bar_count, len(streamer.bars))

    # ─── live loop ───
    last_tick_ts = mt5.symbol_info_tick(SYMBOL).time_msc
    print(f"[live] entering main loop. last_tick_msc={last_tick_ts}")

    try:
        while True:
            try:
                # fetch new ticks since last seen
                now_dt = datetime.now(timezone.utc)
                from_ts = last_tick_ts + 1  # ms
                new_ticks = mt5.copy_ticks_from(
                    SYMBOL,
                    datetime.fromtimestamp(from_ts / 1000.0, tz=timezone.utc),
                    1000,
                    mt5.COPY_TICKS_ALL,
                )

                if new_ticks is not None and len(new_ticks) > 0:
                    for t in new_ticks:
                        ts_msc = int(t["time_msc"])
                        if ts_msc <= last_tick_ts:
                            continue
                        last_tick_ts = ts_msc

                        ts = int(t["time"])
                        bid = float(t["bid"]); ask = float(t["ask"]); last = float(t["last"])
                        if bid > 0 and ask > 0: price = (bid + ask) / 2.0
                        elif last > 0:          price = last
                        elif bid > 0:           price = bid
                        elif ask > 0:           price = ask
                        else: continue
                        vol = float(t["volume"])
                        streamer.process_tick(ts, price, vol)

                # check if MT5 closed our position out-of-band (SL hit)
                engine.poll_position_status()

            except KeyboardInterrupt:
                raise
            except Exception as e:
                telegram_bot.send_error(f"loop error: {e}")
                print(f"[live] loop error: {e}")

            time.sleep(TICK_POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n[live] shutting down…")
        telegram_bot.send_status("Live engine stopped (manual).")
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    main()