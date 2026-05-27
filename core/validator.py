"""Per-trade backtest replay validator."""

FIXED_SL_POINTS = 16.0
SLIPPAGE_POINTS = 0.3
TP_CLOSE_AFTER  = 3
STREAK_SIZE     = 2
PRICE_TOLERANCE = 0.5

def _candle_dir(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0

def _find_bar_idx_by_time(bars, ts):
    for idx in range(len(bars) - 1, -1, -1):
        if bars[idx]["time"] == ts: return idx
    return -1

def replay_trade(bars, signal_bar_time, live_direction):
    sig_idx = _find_bar_idx_by_time(bars, signal_bar_time)
    if sig_idx < 0:
        return {"signal_valid": False, "reason": "signal bar not found"}
    if sig_idx < STREAK_SIZE:
        return {"signal_valid": False, "reason": "insufficient pre-signal history"}
    if sig_idx + 1 + TP_CLOSE_AFTER >= len(bars):
        return {"signal_valid": False, "reason": "insufficient post-signal bars"}

    streak_dir = _candle_dir(bars[sig_idx - STREAK_SIZE])
    if streak_dir == 0:
        return {"signal_valid": False, "reason": "streak base is doji"}
    for k in range(sig_idx - STREAK_SIZE, sig_idx):
        if _candle_dir(bars[k]) != streak_dir:
            return {"signal_valid": False, "reason": f"streak broken at idx {k}"}

    reversal_dir = _candle_dir(bars[sig_idx])
    if reversal_dir != -streak_dir:
        return {"signal_valid": False, "reason": "no opposite reversal"}
    if reversal_dir != live_direction:
        return {"signal_valid": False, "reason": f"dir mismatch: replay={reversal_dir} live={live_direction}"}

    entry_idx = sig_idx + 1
    raw_entry = bars[entry_idx]["open"]
    if reversal_dir == 1:
        entry_price = raw_entry + SLIPPAGE_POINTS
        sl_price    = entry_price - FIXED_SL_POINTS
    else:
        entry_price = raw_entry - SLIPPAGE_POINTS
        sl_price    = entry_price + FIXED_SL_POINTS

    tp_idx = entry_idx + TP_CLOSE_AFTER
    hit_sl = False
    exit_price = None
    exit_idx = None
    mfe_points = 0.0
    mae_points = 0.0

    for k in range(entry_idx, tp_idx + 1):
        bar = bars[k]
        bar_open, bar_high, bar_low = bar["open"], bar["high"], bar["low"]
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
                exit_price = bar_open - SLIPPAGE_POINTS; hit_sl = True; exit_idx = k; break
            if bar_low <= sl_price:
                exit_price = sl_price - SLIPPAGE_POINTS; hit_sl = True; exit_idx = k; break
        else:
            if bar_open >= sl_price:
                exit_price = bar_open + SLIPPAGE_POINTS; hit_sl = True; exit_idx = k; break
            if bar_high >= sl_price:
                exit_price = sl_price + SLIPPAGE_POINTS; hit_sl = True; exit_idx = k; break

    if not hit_sl:
        raw_tp = bars[tp_idx]["close"]
        exit_price = raw_tp - SLIPPAGE_POINTS if reversal_dir == 1 else raw_tp + SLIPPAGE_POINTS
        exit_idx = tp_idx

    pnl_points = (exit_price - entry_price) if reversal_dir == 1 else (entry_price - exit_price)

    return {
        "signal_valid": True,
        "entry_price":  round(entry_price, 4),
        "exit_price":   round(exit_price,  4),
        "sl_price":     round(sl_price,    4),
        "hit_sl":       hit_sl,
        "expected_reason": "SL" if hit_sl else "TP",
        "entry_bar_time":  bars[entry_idx]["time"],
        "exit_bar_time":   bars[exit_idx]["time"],
        "bars_held":       exit_idx - entry_idx + 1,
        "pnl_points":      round(pnl_points, 4),
        "mfe_points":      round(mfe_points, 4),
        "mae_points":      round(mae_points, 4),
    }

def validate(live_trade, bars):
    result = {
        "trade_id": live_trade.get("id"),
        "dir":      live_trade.get("dir"),
        "status":   "UNKNOWN", "issues": [],
    }
    replay = replay_trade(bars, live_trade["entry_time"], live_trade["dir"])
    if not replay.get("signal_valid"):
        result["status"] = "SKIP"
        result["issues"].append(f"replay unavailable: {replay.get('reason')}")
        result["replay"] = replay
        return result

    entry_diff = abs(live_trade["entry"] - replay["entry_price"])
    exit_diff  = abs(live_trade["exit"]  - replay["exit_price"])
    reason_match = (live_trade["reason"] == replay["expected_reason"])

    result.update({
        "live_entry":         live_trade["entry"],
        "expected_entry":     replay["entry_price"],
        "entry_diff_pts":     round(entry_diff, 4),
        "live_exit":          live_trade["exit"],
        "expected_exit":      replay["exit_price"],
        "exit_diff_pts":      round(exit_diff, 4),
        "live_reason":        live_trade["reason"],
        "expected_reason":    replay["expected_reason"],
        "live_bars_held":     live_trade["bars_held"],
        "expected_bars_held": replay["bars_held"],
        "replay":             replay,
    })

    issues = []
    if not reason_match:
        issues.append(f"exit reason: live={live_trade['reason']} expected={replay['expected_reason']}")
    if entry_diff > PRICE_TOLERANCE:
        issues.append(f"entry diff {entry_diff:.3f}pt > tol {PRICE_TOLERANCE}pt")
    if exit_diff > PRICE_TOLERANCE:
        issues.append(f"exit diff {exit_diff:.3f}pt > tol {PRICE_TOLERANCE}pt")

    result["status"] = "MATCH" if not issues else "MISMATCH"
    if issues: result["issues"] = issues
    return result

def format_report(v):
    tid = v.get("trade_id")
    dir_txt = "L" if v.get("dir") == 1 else "S"
    if v["status"] == "MATCH":
        return f"  ✅ VALIDATOR #{tid} {dir_txt}  MATCH  (entryΔ={v['entry_diff_pts']:.2f}pt, exitΔ={v['exit_diff_pts']:.2f}pt)"
    if v["status"] == "SKIP":
        return f"  ⚪ VALIDATOR #{tid} {dir_txt}  SKIP  ({'; '.join(v['issues'])})"
    lines = [f"  ❌ VALIDATOR #{tid} {dir_txt}  MISMATCH"]
    for issue in v["issues"]: lines.append(f"       - {issue}")
    lines.append(f"       live   entry={v['live_entry']} exit={v['live_exit']} {v['live_reason']} bars={v['live_bars_held']}")
    lines.append(f"       replay entry={v['expected_entry']} exit={v['expected_exit']} {v['expected_reason']} bars={v['expected_bars_held']}")
    return "\n".join(lines)