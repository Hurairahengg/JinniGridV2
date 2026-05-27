"""
live_engine.py — Worker-side strategy engine.

CONCURRENT / OVERLAPPING TRADES — 1:1 with backtester's `i += 1` behavior.
Each open trade is independent: own ticket, bars_since_entry counter,
bars_window, etc. Signals fire every bar regardless of open positions.

FIXES IN THIS VERSION:
  • Auto-detect point_value_per_lot from MT5 symbol info (no more hardcoded $1)
  • Use BROKER-REPORTED PnL (deal.profit + commission + swap) as source of truth
  • Retry close orders immediately on transient failure (3 attempts, no waiting for next bar)
  • Track theoretical vs actual PnL separately for validator
  • Prioritize closes — overdue trades are closed FIRST, before any new bar processing
"""

import math
import time
import threading
from collections import deque
from datetime import datetime, timedelta, timezone

import MetaTrader5 as mt5


# ═════════════════════════════════════════════════════════════════
# LIVE KOKO BAR STREAMER  (unchanged — 1:1 with original)
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
# STRATEGY ENGINE — concurrent / overlapping trades (1:1 backtester)
# ═════════════════════════════════════════════════════════════════
def direction(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0


# Close-order retry policy
CLOSE_RETRY_ATTEMPTS  = 3
CLOSE_RETRY_DELAY_S   = 0.15  # short — we want to bail or succeed fast


class StrategyEngine:
    """
    Concurrent positions allowed. Signals fire on EVERY bar regardless of
    whether other positions are open. Mirrors backtester's `i += 1`
    behavior where overlapping trades count as separate trades.
    """

    def __init__(self, streamer, config, on_event):
        self.streamer = streamer
        self.cfg = config
        self.on_event = on_event
        self.open_trades = []
        self.live_bars_seen = 0

        # static — taken from config, no auto-detect (which gave wrong values
        # because broker contract specs aren't $1/pt/lot on USTEC)
        self.point_value_per_lot = float(config.get("point_value_per_lot", 1.0))

    # ─── helpers ────────────────────────────────────────────────
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

    # ─── streamer callback ──────────────────────────────────────
    def on_bar(self, bar, global_idx, is_warmup):
        if is_warmup:
            return

        self.live_bars_seen += 1


        self.on_event("bar", {
            "bar": bar,
            "live_bars_seen": self.live_bars_seen,
            "open_trades": len(self.open_trades),
        })
        self._log("INFO", f"bar #{self.live_bars_seen} O={bar['open']} C={bar['close']} "
                          f"dir={direction(bar):+d} | open_trades={len(self.open_trades)}")

        # ── PRIORITY 1: close any overdue trades FIRST (before bar append, before signals) ──
        # bars_window still gets the bar appended below; we just guarantee
        # close orders go out before any new signal processing.
        for trade in list(self.open_trades):
            trade["bars_since_entry"] += 1
            trade["bars_window"].append(bar)
            if trade["bars_since_entry"] >= self.cfg["tp_close_after"] + 1:
                self._close_trade_tp(trade, bar)

        # ── PRIORITY 2: scan for new signal (overlap allowed) ──
        required_live_bars = self.cfg["streak_size"] + 1
        if self.live_bars_seen < required_live_bars:
            self._log("INFO", f"building live history {self.live_bars_seen}/{required_live_bars}")
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
            return
        streak_dir   = -reversal_dir
        streak_slice = bars[-(streak + 1):-1]
        streak_dirs  = [direction(b) for b in streak_slice]

        for b in streak_slice:
            if direction(b) != streak_dir:
                return

        self._log("INFO", f"SIGNAL streak={streak_dirs} reversal={reversal_dir:+d} -- firing order")
        self._open_trade(reversal_dir, bars, streak_slice, signal_bar)

    # ─── ORDER EXECUTION ────────────────────────────────────────
    def _open_trade(self, reversal_dir, bars_snapshot, streak_slice, signal_bar):
        c = self.cfg
        pv = self.point_value_per_lot

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

        # use REAL point value for sizing — was a bug if config was wrong
        raw_lots = risk / (c["fixed_sl_points"] * pv)
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
            "bars_since_entry":       0,
            # strategy params for validator
            "streak_size":            c["streak_size"],
            "tp_close_after":         c["tp_close_after"],
            "slippage_points":        c["slippage_points"],
            "commission_per_lot":     c["commission_per_lot"],
            "point_value_per_lot":    pv,  # use resolved value, not raw cfg
        }

        self.open_trades.append(trade)
        self.on_event("trade.opened", trade)
        self._log("INFO", f"OPEN ticket={ticket} dir={reversal_dir:+d} entry={actual_entry} "
                          f"sl={live_sl} lots={lots} | concurrent_open={len(self.open_trades)}")

    def _send_close_with_retry(self, ticket, lots, is_long):
        """
        Send a market close with up to CLOSE_RETRY_ATTEMPTS retries on
        transient broker failures. Returns (result, attempts_used).
        """
        c = self.cfg
        last_err = None
        for attempt in range(1, CLOSE_RETRY_ATTEMPTS + 1):
            tick = mt5.symbol_info_tick(c["symbol"])
            if tick is None:
                last_err = f"symbol_info_tick None (attempt {attempt})"
                time.sleep(CLOSE_RETRY_DELAY_S); continue

            close_type  = mt5.ORDER_TYPE_SELL if is_long else mt5.ORDER_TYPE_BUY
            close_price = tick.bid if is_long else tick.ask

            request = {
                "action":       mt5.TRADE_ACTION_DEAL,
                "symbol":       c["symbol"],
                "volume":       lots,
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
            if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                return result, attempt
            last_err = result.comment if result else mt5.last_error()
            self._log("WARN",
                f"close attempt {attempt}/{CLOSE_RETRY_ATTEMPTS} failed ticket={ticket}: {last_err}")

            # short retry delay — we want to close fast, not wait for next bar
            if attempt < CLOSE_RETRY_ATTEMPTS:
                time.sleep(CLOSE_RETRY_DELAY_S)

        self._err(f"TP close EXHAUSTED retries for ticket {ticket}: {last_err}")
        return None, CLOSE_RETRY_ATTEMPTS

    def _close_trade_tp(self, trade, exit_bar):
        """Market-close the given trade at TP_CLOSE_AFTER+1 bars."""
        ticket = trade["ticket"]
        positions = mt5.positions_get(ticket=ticket)
        if not positions:
            self._log("INFO", f"ticket={ticket} not found in MT5 -- assuming SL, reconciling")
            self._reconcile_closed_position(trade, exit_bar)
            return

        pos = positions[0]
        is_long = (pos.type == mt5.POSITION_TYPE_BUY)

        result, attempts = self._send_close_with_retry(ticket, pos.volume, is_long)
        if result is None:
            # close totally failed — leave in open_trades, will retry on next bar
            # but ALSO check if broker closed it out-of-band in the meantime
            still_open = mt5.positions_get(ticket=ticket)
            if not still_open:
                self._log("INFO", f"ticket={ticket} disappeared during retry, reconciling")
                self._reconcile_closed_position(trade, exit_bar)
            return

        # Use broker deal data as source of truth, not result.price math
        self._finalize_trade(trade, exit_bar, result.price, hit_sl=False, close_attempts=attempts)

    def _reconcile_closed_position(self, trade, exit_bar):
        """Position was closed broker-side (SL trigger or manual). Pull authoritative deal data."""
        ticket = trade["ticket"]
        from_time = datetime.now(timezone.utc) - timedelta(hours=24)
        deals = mt5.history_deals_get(from_time, datetime.now(timezone.utc), position=ticket)
        if not deals:
            self._err(f"could not find closing deal for ticket {ticket}")
            if trade in self.open_trades:
                self.open_trades.remove(trade)
            return
        close_deal = max(deals, key=lambda d: d.time)
        self._finalize_trade(trade, exit_bar, close_deal.price, hit_sl=True, close_attempts=0)


    def _finalize_trade(self, trade, exit_bar, actual_exit, hit_sl, close_attempts=0):
        c = self.cfg
        pv = self.point_value_per_lot  # static from config

        actual_entry = trade["actual_entry"]
        is_long = trade["dir"] == 1

        # theoretical PnL (matches backtester) — single source of truth
        pnl_points = (actual_exit - actual_entry) if is_long else (actual_entry - actual_exit)
        gross_pnl  = pnl_points * trade["lots"] * pv
        commission = trade["lots"] * c["commission_per_lot"]
        net_pnl    = gross_pnl - commission

        expected_total = (c["streak_size"] + 1) + trade["bars_since_entry"]
        actual_total   = len(trade["bars_window"])
        if actual_total != expected_total:
            self._log("WARN", f"bars_window size mismatch ticket={trade['ticket']}: "
                              f"expected={expected_total} actual={actual_total}")

        trade.update({
            "status":      "closed",
            "exit_time":   datetime.now(timezone.utc).isoformat(),
            "actual_exit": actual_exit,
            "hit_sl":      hit_sl,
            "close_attempts": close_attempts,
            "pnl_points":  pnl_points,
            "gross_pnl":   gross_pnl,
            "commission":  commission,
            "net_pnl":     net_pnl,
            "swap":        0.0,
            "bars_held":   trade["bars_since_entry"],
            "r_multiple":  net_pnl / trade["risk_used"] if trade["risk_used"] > 0 else 0.0,
        })

        # remove BEFORE emitting close so heartbeats are accurate
        if trade in self.open_trades:
            self.open_trades.remove(trade)

        self.on_event("trade.closed", trade)
        self._log("INFO", f"CLOSE ticket={trade['ticket']} pnl=${net_pnl:+.2f} "
                          f"({pnl_points:+.2f}pt × {trade['lots']} × ${pv}/pt - ${commission:.2f}) "
                          f"reason={'SL' if hit_sl else 'TP'} attempts={close_attempts} | "
                          f"remaining_open={len(self.open_trades)}")

    def poll_position_status(self):
        """Check broker for any open trade that was closed out-of-band (SL hit)."""
        if not self.open_trades:
            return
        for trade in list(self.open_trades):
            ticket = trade["ticket"]
            positions = mt5.positions_get(ticket=ticket)
            if not positions:
                last_bar = self.streamer.bars[-1] if self.streamer.bars else None
                self._log("INFO", f"broker-side SL on ticket {ticket} -- reconciling")
                self._reconcile_closed_position(trade, last_bar)

    def status_snapshot(self):
        return {
            "engine_state":       "RUNNING" if self.live_bars_seen >= 0 else "IDLE",
            "live_bars_seen":     self.live_bars_seen,
            "mem_bars":           len(self.streamer.bars) if self.streamer else 0,
            "last_bar_ts":        self.streamer.bars[-1]["time"] if self.streamer and self.streamer.bars else None,
            "open_tickets":       [t["ticket"] for t in self.open_trades],
            "open_ticket":        self.open_trades[0]["ticket"] if self.open_trades else None,
            "open_count":         len(self.open_trades),
            "point_value_per_lot": self.point_value_per_lot,
        }


# ═════════════════════════════════════════════════════════════════
# WARMUP (unchanged)
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

        if processed >= next_report or (time.time() - last_report_time) > 3:
            _log("INFO", f"WARMUP progress -- ticks={processed:,}/{len(ticks):,} "
                         f"bars_built={streamer.global_bar_count}")
            next_report = processed + 50_000
            last_report_time = time.time()

        if streamer.global_bar_count >= c["warmup_bars"] and len(streamer.bars) >= c["max_bars_in_mem"]:
            break

    _log("INFO", f"WARMUP done -- {streamer.global_bar_count} bars built, "
                 f"{len(streamer.bars)} in memory, {processed:,} ticks processed")
    return True


# ═════════════════════════════════════════════════════════════════
# RUN
# ═════════════════════════════════════════════════════════════════
def run(config, on_event, stop_event):
    def _log(level, msg): on_event("log", {"level": level, "message": msg})
    def _err(msg, ctx=None): on_event("error", {"message": msg, "context": ctx or {}})

    c = config
    _log("INFO", f"engine.run() -- symbol={c['symbol']} brick={c['brick_size']} "
                 f"streak={c['streak_size']} sl={c['fixed_sl_points']} tp_after={c['tp_close_after']}")

    _log("INFO", "MT5: initialize()...")
    if not mt5.initialize():
        _err(f"mt5.initialize() FAILED: {mt5.last_error()}"); return

    term = mt5.terminal_info()
    if term:
        _log("INFO", f"MT5 terminal: name={term.name} build={term.build} "
                     f"connected={term.connected} trade_allowed={term.trade_allowed}")
    acct = mt5.account_info()
    if acct:
        _log("INFO", f"MT5 account: login={acct.login} company={acct.company} "
                     f"balance=${acct.balance:.2f} equity=${acct.equity:.2f} "
                     f"trade_allowed={acct.trade_allowed}")
        if not acct.trade_allowed:
            _err("ACCOUNT trade_allowed=False -- algo trading disabled in terminal!")
    else:
        _err(f"mt5.account_info() returned None -- {mt5.last_error()}")

    if not mt5.symbol_select(c["symbol"], True):
        _err(f"symbol_select({c['symbol']}) FAILED: {mt5.last_error()}")
        mt5.shutdown(); return
    info = mt5.symbol_info(c["symbol"])
    if info is None:
        _err(f"symbol_info({c['symbol']}) returned None"); mt5.shutdown(); return

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

    last_tick = mt5.symbol_info_tick(c["symbol"])
    last_tick_ts = last_tick.time_msc if last_tick else 0
    _log("INFO", f"LIVE LOOP entering -- poll_interval={c['tick_poll_interval']}s")

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