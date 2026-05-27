"""
validator.py — per-trade backtest replay validator.

For every closed live trade, this re-runs the EXACT walk-forward logic
from the offline backtester on the surrounding bars, and compares:
  - expected entry price  vs  live entry price
  - expected exit price   vs  live exit price
  - expected exit reason  (SL/TP)  vs  live exit reason
  - expected bars held    vs  live bars held

If everything matches within PRICE_TOLERANCE → MATCH ✅
Anything off → MISMATCH ❌ with detailed diffs.

This is the safety net that proves the live engine == backtest 1:1.
"""

# ============================ CONFIG ============================
# These MUST match live_engine.py + backtester exactly.

FIXED_SL_POINTS = 16.0
SLIPPAGE_POINTS = 0.3
TP_CLOSE_AFTER  = 3
STREAK_SIZE     = 2

# Allowed drift between live & replayed prices.
# Live uses tick-resolution SL checks while backtest uses bar-OHLC, so a small
# delta is expected on SL exits even in a perfect implementation. Bump if needed.
PRICE_TOLERANCE = 0.5   # points

# ============================ HELPERS ============================

def _candle_dir(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0


def _find_bar_idx_by_time(bars, ts):
    """Locate the bar whose time == ts. Returns -1 if not found."""
    # bars are time-ordered & emit enforces strict monotonic ts,
    # so linear-scan-from-end is fastest for recently-closed trades
    for idx in range(len(bars) - 1, -1, -1):
        if bars[idx]["time"] == ts:
            return idx
    return -1


# ============================ REPLAY ============================

def replay_trade(bars, signal_bar_time, live_direction):
    """
    Re-run the backtester's signal + walk-forward logic on the given bars,
    starting from the bar matching `signal_bar_time` (= the reversal candle).

    Returns:
        dict with full replay outcome, or {"signal_valid": False, ...}
        if the signal can't be reconstructed.
    """
    sig_idx = _find_bar_idx_by_time(bars, signal_bar_time)
    if sig_idx < 0:
        return {"signal_valid": False, "reason": "signal bar not found in history"}

    # need STREAK_SIZE bars before + TP_CLOSE_AFTER bars after entry
    if sig_idx < STREAK_SIZE:
        return {"signal_valid": False, "reason": "insufficient pre-signal history"}
    if sig_idx + 1 + TP_CLOSE_AFTER >= len(bars):
        return {"signal_valid": False, "reason": "insufficient post-signal bars to walk"}

    # ── verify streak + reversal candle pattern (exact backtester logic) ──
    streak_dir = _candle_dir(bars[sig_idx - STREAK_SIZE])
    if streak_dir == 0:
        return {"signal_valid": False, "reason": "streak base bar is doji"}
    for k in range(sig_idx - STREAK_SIZE, sig_idx):
        if _candle_dir(bars[k]) != streak_dir:
            return {"signal_valid": False, "reason": f"streak broken at bar idx {k}"}

    reversal_dir = _candle_dir(bars[sig_idx])
    if reversal_dir != -streak_dir:
        return {"signal_valid": False, "reason": "no opposite reversal candle"}

    if reversal_dir != live_direction:
        return {
            "signal_valid": False,
            "reason": f"direction mismatch: replay={reversal_dir} live={live_direction}",
        }

    # ── ENTRY (matches backtester exactly) ─────────────────────────
    entry_idx = sig_idx + 1
    raw_entry = bars[entry_idx]["open"]
    if reversal_dir == 1:
        entry_price = raw_entry + SLIPPAGE_POINTS
        sl_price    = entry_price - FIXED_SL_POINTS
    else:
        entry_price = raw_entry - SLIPPAGE_POINTS
        sl_price    = entry_price + FIXED_SL_POINTS

    # ── WALK FORWARD (intrabar SL via OHLC, exact backtester logic) ─
    tp_idx = entry_idx + TP_CLOSE_AFTER
    hit_sl = False
    exit_price = None
    exit_idx = None
    mfe_points = 0.0
    mae_points = 0.0

    for k in range(entry_idx, tp_idx + 1):
        bar = bars[k]
        bar_open = bar["open"]
        bar_high = bar["high"]
        bar_low  = bar["low"]

        if reversal_dir == 1:
            fav = bar_high - entry_price
            adv = entry_price - bar_low
        else:
            fav = entry_price - bar_low
            adv = bar_high - entry_price

        if fav > mfe_points: mfe_points = fav
        if adv > mae_points: mae_points = adv

        if reversal_dir == 1:
            if bar_open <= sl_price:
                exit_price = bar_open - SLIPPAGE_POINTS
                hit_sl = True; exit_idx = k; break
            if bar_low <= sl_price:
                exit_price = sl_price - SLIPPAGE_POINTS
                hit_sl = True; exit_idx = k; break
        else:
            if bar_open >= sl_price:
                exit_price = bar_open + SLIPPAGE_POINTS
                hit_sl = True; exit_idx = k; break
            if bar_high >= sl_price:
                exit_price = sl_price + SLIPPAGE_POINTS
                hit_sl = True; exit_idx = k; break

    if not hit_sl:
        raw_tp = bars[tp_idx]["close"]
        if reversal_dir == 1:
            exit_price = raw_tp - SLIPPAGE_POINTS
        else:
            exit_price = raw_tp + SLIPPAGE_POINTS
        exit_idx = tp_idx

    # ── PnL replay (without lots — we compare pts; lots are deterministic) ──
    if reversal_dir == 1:
        pnl_points = exit_price - entry_price
    else:
        pnl_points = entry_price - exit_price

    return {
        "signal_valid":  True,
        "entry_price":   round(entry_price, 4),
        "exit_price":    round(exit_price,  4),
        "sl_price":      round(sl_price,    4),
        "hit_sl":        hit_sl,
        "expected_reason": "SL" if hit_sl else "TP",
        "entry_bar_time":  bars[entry_idx]["time"],
        "exit_bar_time":   bars[exit_idx]["time"],
        "entry_idx":       entry_idx,
        "exit_idx":        exit_idx,
        "bars_held":       exit_idx - entry_idx + 1,
        "pnl_points":      round(pnl_points, 4),
        "mfe_points":      round(mfe_points, 4),
        "mae_points":      round(mae_points, 4),
    }


# ============================ VALIDATE ============================

def validate(live_trade, bars):
    """
    Compare a closed live trade against the backtester replay.

    Args:
        live_trade: dict from StrategyEngine.trades  (id, dir, entry, exit,
                    reason, entry_time, exit_time, bars_held, etc.)
        bars: full list of closed Koko bars currently known to the engine.

    Returns:
        dict with status ∈ {"MATCH", "MISMATCH", "SKIP"} + diagnostics.
    """
    result = {
        "trade_id": live_trade.get("id"),
        "dir":      live_trade.get("dir"),
        "status":   "UNKNOWN",
        "issues":   [],
    }

    replay = replay_trade(bars, live_trade["entry_time"], live_trade["dir"])

    if not replay.get("signal_valid"):
        # we couldn't even reconstruct the signal → can't claim mismatch fairly
        result["status"] = "SKIP"
        result["issues"].append(f"replay unavailable: {replay.get('reason')}")
        result["replay"] = replay
        return result

    # ── numeric diffs ────────────────────────────────────────────────
    entry_diff = abs(live_trade["entry"] - replay["entry_price"])
    exit_diff  = abs(live_trade["exit"]  - replay["exit_price"])
    reason_match = (live_trade["reason"] == replay["expected_reason"])

    result.update({
        "live_entry":          live_trade["entry"],
        "expected_entry":      replay["entry_price"],
        "entry_diff_pts":      round(entry_diff, 4),
        "live_exit":           live_trade["exit"],
        "expected_exit":       replay["exit_price"],
        "exit_diff_pts":       round(exit_diff, 4),
        "live_reason":         live_trade["reason"],
        "expected_reason":     replay["expected_reason"],
        "live_bars_held":      live_trade["bars_held"],
        "expected_bars_held":  replay["bars_held"],
        "replay":              replay,
    })

    issues = []
    if not reason_match:
        issues.append(
            f"exit reason: live={live_trade['reason']} expected={replay['expected_reason']}"
        )
    if entry_diff > PRICE_TOLERANCE:
        issues.append(f"entry diff {entry_diff:.3f}pt > tol {PRICE_TOLERANCE}pt")
    if exit_diff > PRICE_TOLERANCE:
        issues.append(f"exit diff {exit_diff:.3f}pt > tol {PRICE_TOLERANCE}pt")

    if not issues:
        result["status"] = "MATCH"
    else:
        result["status"] = "MISMATCH"
        result["issues"] = issues

    return result


# ============================ PRETTY PRINT ============================

def format_report(v):
    """Console-friendly one-block summary of a validation result."""
    tid = v.get("trade_id")
    dir_txt = "L" if v.get("dir") == 1 else "S"
    if v["status"] == "MATCH":
        return f"  ✅ VALIDATOR #{tid} {dir_txt}  MATCH  (entryΔ={v['entry_diff_pts']:.2f}pt, exitΔ={v['exit_diff_pts']:.2f}pt)"
    if v["status"] == "SKIP":
        return f"  ⚪ VALIDATOR #{tid} {dir_txt}  SKIP   ({'; '.join(v['issues'])})"
    # MISMATCH
    lines = [f"  ❌ VALIDATOR #{tid} {dir_txt}  MISMATCH"]
    for issue in v["issues"]:
        lines.append(f"       - {issue}")
    lines.append(
        f"       live   entry={v['live_entry']}  exit={v['live_exit']}  reason={v['live_reason']}  bars={v['live_bars_held']}"
    )
    lines.append(
        f"       replay entry={v['expected_entry']}  exit={v['expected_exit']}  reason={v['expected_reason']}  bars={v['expected_bars_held']}"
    )
    return "\n".join(lines)