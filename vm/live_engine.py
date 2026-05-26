"""
live_engine.py — Worker-side strategy engine.

Strategy state machine, bar generation, signal detection, order routing,
SL/TP exit, and trade-record shape are BIT-IDENTICAL to the original.

Edges:
  • config dict in via run(config, on_event, stop_event)
  • every state change goes through on_event(type, payload)
  • verbose logging at every step so you can see exactly what's happening live
"""

import math
import time
import threading
from collections import deque
from datetime import datetime, timedelta, timezone

import MetaTrader5 as mt5


# ═════════════════════════════════════════════════════════════════
# LIVE KOKO BAR STREAMER  (logic unchanged)
# ═════════════════════════════════════════════════════════════════
class LiveKokoCandleStreamer:
    def __init__(self, range_size, rev_bricks, clean_mode, price_decimals,
                 max_bars=100, on_bar_emit=None):
        self.rs = float(range_size)
        self.rev_bricks = float(rev_bricks)
        self.clean_mode = clean_mode
        self.pd = price_decimals
        self.bars = deque(maxlen=max_bars)
        self.on_bar_emit = on_bar_emit

        self.trend = 0
        self.level = None
        self.bar = None
        self.global_bar_count = 0
        self._last_written_ts = None

    def _snap(self, price):
        return round(round(price / self.rs) * self.rs, self.pd)

    def _emit(self, open_, high_, low_, close_, ts, volume):
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
                        bc = rev; bo = round(rev + rs, self.pd)
                        bh = max(self.bar["high"], lvl); bl = min(self.bar["low"], bc)
                    else:
                        bo = lvl; bc = rev
                        bh = self.bar["high"]; bl = min(self.bar["low"], bc)
                    self._emit(bo, bh, bl, bc, self.bar["time"], self.bar["volume"])
                    self.trend = -1; self.level = rev; self._reset_working(ts); continue
                else:
                    break
            else:
                cont = round(lvl - rs, self.pd)
                rev  = round(lvl + self.rev_bricks * rs, self.pd)
                if price <= cont:
                    bl = min(self.bar["low"], cont)
                    self._emit(lvl, self.bar["high"], bl, cont, self.bar["time"], self.bar["volume"])
                    self.level = cont; self._reset_working(ts); continue
                elif price >= rev:
                    if self.clean_mode:
                        bc = rev; bo = round(rev - rs, self.pd)
                        bh = max(self.bar["high"], bc); bl = min(self.bar["low"], lvl)
                    else:
                        bo = lvl; bc = rev
                        bh = max(self.bar["high"], bc); bl = self.bar["low"]
                    self._emit(bo, bh, bl, bc, self.bar["time"], self.bar["volume"])
                    self.trend = 1; self.level = rev; self._reset_working(ts); continue
                else:
                    break


# ═════════════════════════════════════════════════════════════════
# STRATEGY ENGINE
# ═════════════════════════════════════════════════════════════════
def direction(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0


class StrategyEngine:
    STATE_IDLE     = "IDLE"
    STATE_IN_TRADE = "IN_TRADE"

    def __init__(self, streamer, config, on_event):
        self.streamer = streamer
        self.cfg = config
        self.on_event = on_event
        self.state = self.STATE_IDLE
        self.open_trade = None
        self.bars_since_entry = 0
        self.live_bars_seen = 0

    def _log(self, level, msg):
        self.on_event("log", {"level": level, "message": msg})

    def _err(self, msg, ctx=None):
        self.on_event("error", {"message": msg, "context": ctx or {}})

    def _get_current_risk(self):
        c = self.cfg
        if c["scaling_enabled"]:
            acct = mt5.account_info()
            if acct is None:
                self._err("account_info() returned None, using starting_balance")
                balance = c["starting_balance"]
            else:
                balance = acct.balance
        else:
            balance = c["starting_balance"]
        return (balance / 100.0) * c["risk_per_100"], balance

    def on_bar(self, bar, global_idx, is_warmup):
        if is_warmup:
            return
        self.live_bars_seen += 1

        # emit structured bar for the dashboard chart
        self.on_event("bar", {
            "bar": bar,
            "live_bars_seen": self.live_bars_seen,
            "engine_state": self.state,
        })
        # short log line
        self._log("INFO", f"bar #{self.live_bars_seen} "
                          f"O={bar['open']} C={bar['close']} dir={direction(bar):+d}")

        if self.state == self.STATE_IN_TRADE:
            self.bars_since_entry += 1
            if self.open_trade is not None:
                self.open_trade["bars_window"].append(bar)
            self._log("INFO", f"IN_TRADE -- bars_since_entry={self.bars_since_entry}/"
                              f"{self.cfg['tp_close_after'] + 1}")
            if self.bars_since_entry >= self.cfg["tp_close_after"] + 1:
                self._log("INFO", "TP bar reached -- closing trade")
                self._close_trade_tp(bar)
                return
            return

        required_live_bars = self.cfg["streak_size"] + 1
        if self.live_bars_seen < required_live_bars:
            self._log("INFO", f"building live history {self.live_bars_seen}/{required_live_bars} "
                              f"-- no signals yet")
            return

        self._check_signal()

    def _check_signal(self):
        bars = list(self.streamer.bars)
        streak = self.cfg["streak_size"]
        if len(bars) < streak + 1:
            return
        signal_bar   = bars[-1]
        reversal_dir = direction(signal_bar)
        if reversal_dir == 0:
            self._log("INFO", "signal check: latest bar is doji -- skip")
            return
        streak_dir   = -reversal_dir
        streak_slice = bars[-(streak + 1):-1]
        streak_dirs  = [direction(b) for b in streak_slice]

        for b in streak_slice:
            if direction(b) != streak_dir:
                self._log("INFO", f"signal check: streak broken (need {streak} x dir={streak_dir:+d}, "
                                  f"got {streak_dirs})")
                return

        self._log("INFO", f"SIGNAL -- streak={streak_dirs} reversal={reversal_dir:+d} -- firing order")
        self._open_trade(reversal_dir, bars, streak_slice, signal_bar)

    def _open_trade(self, reversal_dir, bars_snapshot, streak_slice, signal_bar):
        c = self.cfg
        theo_entry = self.streamer.level
        if reversal_dir == 1:
            theo_entry_priced = theo_entry + c["slippage_points"]
            theo_sl           = theo_entry_priced - c["fixed_sl_points"]
        else:
            theo_entry_priced = theo_entry - c["slippage_points"]
            theo_sl           = theo_entry_priced + c["fixed_sl_points"]

        risk, balance_used = self._get_current_risk()
        if risk <= 0:
            self._err(f"non-positive risk (balance=${balance_used:.2f}), skipping trade")
            return
        raw_lots = risk / (c["fixed_sl_points"] * c["point_value_per_lot"])
        lots = max(c["min_lots"], min(c["max_lots"], raw_lots))

        info = mt5.symbol_info(c["symbol"])
        if info is None:
            self._err(f"symbol_info({c['symbol']}) returned None"); return
        step = info.volume_step or 0.01
        lots = math.floor(lots / step) * step
        lots = max(info.volume_min, min(info.volume_max, lots))
        lots = round(lots, 2)

        order_type = mt5.ORDER_TYPE_BUY if reversal_dir == 1 else mt5.ORDER_TYPE_SELL
        tick = mt5.symbol_info_tick(c["symbol"])
        price = tick.ask if reversal_dir == 1 else tick.bid
        if reversal_dir == 1:
            live_sl = round(price - c["fixed_sl_points"], c["price_decimals"])
        else:
            live_sl = round(price + c["fixed_sl_points"], c["price_decimals"])

        self._log("INFO", f"order_send -- dir={reversal_dir:+d} price={price} sl={live_sl} "
                          f"lots={lots} risk=${risk:.2f} balance=${balance_used:.2f}")

        request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       c["symbol"],
            "volume":       lots,
            "type":         order_type,
            "price":        price,
            "sl":           live_sl,
            "deviation":    c["deviation_points"],
            "magic":        c["magic_number"],
            "comment":      c["order_comment"],
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            err = result.comment if result else mt5.last_error()
            self._err(f"order_send FAILED: retcode={getattr(result,'retcode',None)} err={err}")
            return

        actual_entry = result.price
        ticket = result.order
        bars_window = list(streak_slice) + [signal_bar]
        signal_idx_in_window = len(bars_window) - 1

        trade = {
            "status":            "open",
            "symbol":            c["symbol"],
            "ticket":            ticket,
            "dir":               reversal_dir,
            "lots":              lots,
            "risk_used":         risk,
            "balance_at_entry":  balance_used,
            "scaling_enabled":   c["scaling_enabled"],
            "entry_time":        datetime.now(timezone.utc).isoformat(),
            "theoretical_entry": theo_entry_priced,
            "actual_entry":      actual_entry,
            "theoretical_sl":    theo_sl,
            "actual_sl":         live_sl,
            "sl_price":          live_sl,
            "sl_pts":            c["fixed_sl_points"],
            "streak_summary":    " ".join(f"{'+' if direction(b)==1 else '-'}" for b in streak_slice),
            "reversal_summary":  f"{'+' if reversal_dir==1 else '-'} O={signal_bar['open']} C={signal_bar['close']}",
            "bars_window":            bars_window,
            "signal_idx_in_window":   signal_idx_in_window,
            "entry_global_idx":       self.streamer.global_bar_count,
        }

        self.open_trade = trade
        self.bars_since_entry = 0
        self.state = self.STATE_IN_TRADE
        self.on_event("trade.opened", trade)
        self._log("INFO", f"OPEN ticket={ticket} dir={reversal_dir:+d} entry={actual_entry} "
                          f"sl={live_sl} lots={lots}")

    def _close_trade_tp(self, exit_bar):
        if self.open_trade is None:
            return
        c = self.cfg
        ticket = self.open_trade["ticket"]
        positions = mt5.positions_get(ticket=ticket)
        if not positions:
            self._log("INFO", f"position {ticket} not found -- assuming SL already hit, reconciling")
            self._reconcile_closed_position(ticket, exit_bar)
            return

        pos = positions[0]
        tick = mt5.symbol_info_tick(c["symbol"])
        is_long = (pos.type == mt5.POSITION_TYPE_BUY)
        close_type  = mt5.ORDER_TYPE_SELL if is_long else mt5.ORDER_TYPE_BUY
        close_price = tick.bid if is_long else tick.ask

        request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       c["symbol"],
            "volume":       pos.volume,
            "type":         close_type,
            "position":     ticket,
            "price":        close_price,
            "deviation":    c["deviation_points"],
            "magic":        c["magic_number"],
            "comment":      "tp_close",
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            err = result.comment if result else mt5.last_error()
            self._err(f"TP close FAILED: {err}"); return
        self._finalize_trade(exit_bar, result.price, hit_sl=False)

    def _reconcile_closed_position(self, ticket, exit_bar):
        from_time = datetime.now(timezone.utc) - timedelta(hours=24)
        deals = mt5.history_deals_get(from_time, datetime.now(timezone.utc), position=ticket)
        if not deals:
            self._err(f"could not find closing deal for ticket {ticket}"); return
        close_deal = max(deals, key=lambda d: d.time)
        self._finalize_trade(exit_bar, close_deal.price, hit_sl=True)

    def _finalize_trade(self, exit_bar, actual_exit, hit_sl):
        c = self.cfg
        t = self.open_trade
        actual_entry = t["actual_entry"]
        is_long = t["dir"] == 1
        pnl_points = (actual_exit - actual_entry) if is_long else (actual_entry - actual_exit)
        gross_pnl  = pnl_points * t["lots"] * c["point_value_per_lot"]
        commission = t["lots"] * c["commission_per_lot"]
        net_pnl    = gross_pnl - commission

        expected_total = (c["streak_size"] + 1) + self.bars_since_entry
        actual_total   = len(t["bars_window"])
        if actual_total != expected_total:
            self._log("WARN", f"bars_window size mismatch: expected={expected_total} actual={actual_total}")

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

        self.on_event("trade.closed", t)
        self._log("INFO", f"CLOSE ticket={t['ticket']} pnl=${net_pnl:+.2f} "
                          f"reason={'SL' if hit_sl else 'TP'} bars_held={self.bars_since_entry}")

        self.open_trade = None
        self.state = self.STATE_IDLE
        self.bars_since_entry = 0

    def poll_position_status(self):
        if self.state != self.STATE_IN_TRADE or self.open_trade is None:
            return
        ticket = self.open_trade["ticket"]
        positions = mt5.positions_get(ticket=ticket)
        if not positions:
            last_bar = self.streamer.bars[-1] if self.streamer.bars else None
            self._log("INFO", f"broker-side SL detected on ticket {ticket} -- reconciling")
            self._reconcile_closed_position(ticket, last_bar)

    def status_snapshot(self):
        return {
            "engine_state":     self.state,
            "live_bars_seen":   self.live_bars_seen,
            "mem_bars":         len(self.streamer.bars) if self.streamer else 0,
            "last_bar_ts":      self.streamer.bars[-1]["time"] if self.streamer and self.streamer.bars else None,
            "open_ticket":      self.open_trade["ticket"] if self.open_trade else None,
            "bars_since_entry": self.bars_since_entry,
        }


# ═════════════════════════════════════════════════════════════════
# WARMUP (verbose)
# ═════════════════════════════════════════════════════════════════
def warmup(streamer, config, on_event):
    def _log(level, msg): on_event("log", {"level": level, "message": msg})
    def _err(msg):        on_event("error", {"message": msg})

    c = config
    _log("INFO", f"WARMUP start -- fetching ticks for {c['symbol']} "
                 f"(lookback={c['history_lookback_days']} days)")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=c["history_lookback_days"])

    ticks = mt5.copy_ticks_range(c["symbol"], start, end, mt5.COPY_TICKS_ALL)
    if ticks is None or len(ticks) == 0:
        _err(f"WARMUP FAILED: no ticks returned. mt5_error={mt5.last_error()}")
        return False

    _log("INFO", f"WARMUP got {len(ticks):,} ticks -- building bars...")

    last_ts = None
    processed = 0
    next_report = 50_000
    last_report_time = time.time()
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
        processed += 1

        # progress report every 50k ticks OR every 3 seconds
        if processed >= next_report or (time.time() - last_report_time) > 3:
            _log("INFO", f"WARMUP progress -- ticks={processed:,}/{len(ticks):,} "
                         f"bars_built={streamer.global_bar_count}")
            next_report = processed + 50_000
            last_report_time = time.time()

        if streamer.global_bar_count >= c["warmup_bars"] and len(streamer.bars) >= c["max_bars_in_mem"]:
            _log("INFO", f"WARMUP early-exit -- enough bars built")
            break

    _log("INFO", f"WARMUP done -- {streamer.global_bar_count} bars built, "
                 f"{len(streamer.bars)} in memory, {processed:,} ticks processed")
    return True


# ═════════════════════════════════════════════════════════════════
# RUN — worker calls this in a thread
# ═════════════════════════════════════════════════════════════════
def run(config, on_event, stop_event):
    def _log(level, msg): on_event("log", {"level": level, "message": msg})
    def _err(msg, ctx=None): on_event("error", {"message": msg, "context": ctx or {}})

    c = config
    _log("INFO", f"engine.run() -- symbol={c['symbol']} brick={c['brick_size']} "
                 f"streak={c['streak_size']} sl={c['fixed_sl_points']}")

    # ─── MT5 init ──────────────────────────────────────────────
    _log("INFO", "MT5: initialize()...")
    if not mt5.initialize():
        _err(f"mt5.initialize() FAILED: {mt5.last_error()}"); return

    term = mt5.terminal_info()
    if term:
        _log("INFO", f"MT5 terminal: name={term.name} build={term.build} "
                     f"connected={term.connected} trade_allowed={term.trade_allowed}")
    else:
        _log("WARN", "MT5 terminal_info() returned None")

    acct = mt5.account_info()
    if acct:
        _log("INFO", f"MT5 account: login={acct.login} company={acct.company} "
                     f"balance=${acct.balance:.2f} equity=${acct.equity:.2f} "
                     f"currency={acct.currency} leverage={acct.leverage} "
                     f"trade_allowed={acct.trade_allowed}")
        if not acct.trade_allowed:
            _err("ACCOUNT trade_allowed=False -- algo trading is disabled in MT5 terminal!")
    else:
        _err(f"mt5.account_info() returned None -- {mt5.last_error()}")

    _log("INFO", f"MT5: selecting symbol {c['symbol']}...")
    if not mt5.symbol_select(c["symbol"], True):
        _err(f"symbol_select({c['symbol']}) FAILED: {mt5.last_error()}")
        mt5.shutdown(); return

    info = mt5.symbol_info(c["symbol"])
    if info is None:
        _err(f"symbol_info({c['symbol']}) returned None"); mt5.shutdown(); return
    _log("INFO", f"symbol {c['symbol']}: visible={info.visible} bid={info.bid} ask={info.ask} "
                 f"point={info.point} digits={info.digits} "
                 f"volume_min={info.volume_min} volume_step={info.volume_step} "
                 f"trade_mode={info.trade_mode}")

    # ─── build streamer + engine ───────────────────────────────
    is_warmup = [True]
    streamer = LiveKokoCandleStreamer(
        range_size=c["brick_size"],
        rev_bricks=c["rev_bricks"],
        clean_mode=c["clean_mode"],
        price_decimals=c["price_decimals"],
        max_bars=c["max_bars_in_mem"],
    )
    engine = StrategyEngine(streamer, c, on_event)
    streamer.on_bar_emit = lambda bar, gidx: engine.on_bar(bar, gidx, is_warmup=is_warmup[0])

    on_event("engine.ready", {"engine": engine})

    if stop_event.is_set():
        _log("INFO", "stop_event already set, skipping warmup"); mt5.shutdown(); return
    if not warmup(streamer, c, on_event):
        _err("warmup failed, engine exiting"); mt5.shutdown(); return
    is_warmup[0] = False
    on_event("warmup.done", {"bars": streamer.global_bar_count, "in_memory": len(streamer.bars)})

    # ─── live loop ────────────────────────────────────────────
    last_tick = mt5.symbol_info_tick(c["symbol"])
    last_tick_ts = last_tick.time_msc if last_tick else 0
    _log("INFO", f"LIVE LOOP entering -- last_tick_msc={last_tick_ts} "
                 f"poll_interval={c['tick_poll_interval']}s")

    consecutive_errors = 0

    try:
        while not stop_event.is_set():
            try:
                from_ts = last_tick_ts + 1
                new_ticks = mt5.copy_ticks_from(
                    c["symbol"],
                    datetime.fromtimestamp(from_ts / 1000.0, tz=timezone.utc),
                    1000, mt5.COPY_TICKS_ALL,
                )
                if new_ticks is not None and len(new_ticks) > 0:
                    for t in new_ticks:
                        ts_msc = int(t["time_msc"])
                        if ts_msc <= last_tick_ts: continue
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

                engine.poll_position_status()
                consecutive_errors = 0


            except Exception as e:
                consecutive_errors += 1
                _err(f"live loop error: {e}", ctx={"consecutive": consecutive_errors})
                if consecutive_errors > 20:
                    _err("too many consecutive errors, attempting MT5 reconnect")
                    try:
                        mt5.shutdown(); time.sleep(2)
                        mt5.initialize(); mt5.symbol_select(c["symbol"], True)
                        consecutive_errors = 0
                    except Exception as ee:
                        _err(f"MT5 reconnect failed: {ee}"); time.sleep(5)
            time.sleep(c["tick_poll_interval"])
    finally:
        _log("INFO", "engine shutting down, calling mt5.shutdown()")
        mt5.shutdown()


# ═════════════════════════════════════════════════════════════════
# ACCOUNT SNAPSHOT for heartbeats
# ═════════════════════════════════════════════════════════════════
def get_account_snapshot():
    try:
        acct = mt5.account_info()
        if acct is None:
            return {"balance": None, "equity": None, "open_positions": None}
        positions = mt5.positions_get() or []
        return {
            "balance": acct.balance,
            "equity":  acct.equity,
            "open_positions": len(positions),
            "broker": acct.company,
            "account": str(acct.login),
        }
    except Exception:
        return {"balance": None, "equity": None, "open_positions": None}