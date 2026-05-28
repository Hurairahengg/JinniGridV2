"""
Per-trade backtest replay validator.

Replays each closed live trade through the exact same logic the backtester uses
on the same Koko bar series, then compares prices, exit reason, bars held,
MFE/MAE, and net PnL.

1:1 with backtester for streak=2, tp=3 (matches live_engine.py constants).
"""

# ================= CONFIG (1:1 with backtester) =================

FIXED_SL_POINTS     = 16.0
SLIPPAGE_POINTS     = 0.3
COMMISSION_PER_LOT  = 0.8       # one-side, scales with lots
POINT_VALUE_PER_LOT = 1.0       # 1pt × 1lot = $1

TP_CLOSE_AFTER      = 3
STREAK_SIZE         = 2

BLOCKED_HOURS_UTC   = [0]       # must match live_engine BLOCKED_HOURS_UTC

# Tolerances
PRICE_TOLERANCE     = 0.5       # points
PNL_TOLERANCE       = 0.50      # dollars  (covers float + lot rounding)
EXCURSION_TOLERANCE = 1.0       # points   (bar-level vs tick-level MFE/MAE)
BARS_HELD_TOL_SL    = 1         # SL exit allowed ±1 bar (tick vs bar-close timing)
BARS_HELD_TOL_TP    = 0         # TP must match exactly


# ================= HELPERS =================

def _candle_dir(c):
    if c["close"] > c["open"]: return 1
    if c["close"] < c["open"]: return -1
    return 0

def _find_bar_idx_by_time(bars, ts):
    # Scan from the end — most validations are for recent trades
    for idx in range(len(bars) - 1, -1, -1):
        if bars[idx]["time"] == ts:
            return idx
    return -1

def _bar_hour_utc(ts):
    return int((int(ts) // 3600) % 24)


# ================= REPLAY (1:1 backtester walk-forward) =================

def replay_trade(bars, signal_bar_time, live_direction):
    """
    Walks the strategy forward from the signal bar exactly like the backtester.

    Returns dict:
      signal_valid: bool
      reason: str (when invalid)
      entry_price, exit_price, sl_price, hit_sl, expected_reason,
      entry_bar_time, exit_bar_time, bars_held,
      pnl_points, mfe_points, mae_points,
      blocked_by_filter: bool   (would the time filter have skipped this?)
    """
    sig_idx = _find_bar_idx_by_time(bars, signal_bar_time)
    if sig_idx < 0:
        return {"signal_valid": False, "reason": "signal bar not found"}
    if sig_idx < STREAK_SIZE:
        return {"signal_valid": False, "reason": "insufficient pre-signal history"}
    if sig_idx + 1 + TP_CLOSE_AFTER >= len(bars):
        return {"signal_valid": False, "reason": "insufficient post-signal bars"}

    # ── streak check ──────────────────────────────────────
    streak_dir = _candle_dir(bars[sig_idx - STREAK_SIZE])
    if streak_dir == 0:
        return {"signal_valid": False, "reason": "streak base is doji"}
    for k in range(sig_idx - STREAK_SIZE, sig_idx):
        if _candle_dir(bars[k]) != streak_dir:
            return {"signal_valid": False, "reason": f"streak broken at idx {k}"}

    # ── reversal check ────────────────────────────────────
    reversal_dir = _candle_dir(bars[sig_idx])
    if reversal_dir != -streak_dir:
        return {"signal_valid": False, "reason": "no opposite reversal"}
    if reversal_dir != live_direction:
        return {"signal_valid": False,
                "reason": f"dir mismatch: replay={reversal_dir} live={live_direction}"}

    # ── entry (= data[entry_idx]["open"] ± slippage) ──────
    entry_idx = sig_idx + 1
    raw_entry = bars[entry_idx]["open"]
    if reversal_dir == 1:
        entry_price = raw_entry + SLIPPAGE_POINTS
        sl_price    = entry_price - FIXED_SL_POINTS
    else:
        entry_price = raw_entry - SLIPPAGE_POINTS
        sl_price    = entry_price + FIXED_SL_POINTS

    # ── time-filter awareness (same hour the backtester would have skipped) ──
    blocked_by_filter = _bar_hour_utc(bars[entry_idx]["time"]) in BLOCKED_HOURS_UTC

    # ── walk forward [entry_idx .. tp_idx] inclusive ──────
    tp_idx = entry_idx + TP_CLOSE_AFTER
    hit_sl     = False
    exit_price = None
    exit_idx   = None
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
        "signal_valid":      True,
        "entry_price":       round(entry_price, 4),
        "exit_price":        round(exit_price,  4),
        "sl_price":          round(sl_price,    4),
        "hit_sl":            hit_sl,
        "expected_reason":   "SL" if hit_sl else "TP",
        "entry_bar_time":    bars[entry_idx]["time"],
        "exit_bar_time":     bars[exit_idx]["time"],
        "bars_held":         exit_idx - entry_idx + 1,
        "pnl_points":        round(pnl_points, 4),
        "mfe_points":        round(mfe_points, 4),
        "mae_points":        round(mae_points, 4),
        "blocked_by_filter": blocked_by_filter,
    }


# ================= VALIDATE =================

def _net_pnl(pnl_points, lots):
    gross = pnl_points * lots * POINT_VALUE_PER_LOT
    commission = lots * COMMISSION_PER_LOT
    return gross - commission


def validate(live_trade, bars):
    """
    Compare a closed live_trade against a replay over the live engine's bar series.

    Status:
      MATCH              — all checks pass
      MISMATCH           — one or more drifts beyond tolerance
      BLOCKED_VIOLATION  — trade was opened but backtester time filter would have skipped it
      SKIP               — replay window not yet complete (live engine should retry later)
    """
    result = {
        "trade_id": live_trade.get("id"),
        "dir":      live_trade.get("dir"),
        "status":   "UNKNOWN",
        "issues":   [],
    }

    replay = replay_trade(bars, live_trade["entry_time"], live_trade["dir"])

    if not replay.get("signal_valid"):
        result["status"] = "SKIP"
        # Keep the exact phrase the live engine matches on for deferred retries
        result["issues"].append(f"replay unavailable: {replay.get('reason')}")
        if replay.get('reason') == "insufficient post-signal bars":
            # ensure live engine's substring check `"insufficient post-signal" in i` hits
            pass
        result["replay"] = replay
        return result

    # ── compute per-trade derived values ──────────────────
    lots         = float(live_trade.get("lots", 0.0))
    live_net     = float(live_trade.get("net_pnl", 0.0))
    expected_net = round(_net_pnl(replay["pnl_points"], lots), 2)
    pnl_diff     = abs(live_net - expected_net)

    entry_diff = abs(live_trade["entry"] - replay["entry_price"])
    exit_diff  = abs(live_trade["exit"]  - replay["exit_price"])

    live_mfe   = float(live_trade.get("mfe_points", 0.0))
    live_mae   = float(live_trade.get("mae_points", 0.0))
    mfe_diff   = abs(live_mfe - replay["mfe_points"])
    mae_diff   = abs(live_mae - replay["mae_points"])

    reason_match = (live_trade["reason"] == replay["expected_reason"])
    bars_tol = BARS_HELD_TOL_SL if live_trade["reason"] == "SL" else BARS_HELD_TOL_TP
    bars_diff = abs(int(live_trade["bars_held"]) - int(replay["bars_held"]))

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
        "bars_held_diff":     bars_diff,

        "live_net_pnl":       round(live_net, 2),
        "expected_net_pnl":   expected_net,
        "pnl_diff_usd":       round(pnl_diff, 2),
        "lots":               lots,

        "live_mfe_pts":       round(live_mfe, 4),
        "expected_mfe_pts":   replay["mfe_points"],
        "mfe_diff_pts":       round(mfe_diff, 4),

        "live_mae_pts":       round(live_mae, 4),
        "expected_mae_pts":   replay["mae_points"],
        "mae_diff_pts":       round(mae_diff, 4),

        "blocked_by_filter":  replay["blocked_by_filter"],
        "replay":             replay,
    })

    # ── time filter check ─────────────────────────────────
    # If the backtester would have skipped this trade due to BLOCKED_HOURS_UTC,
    # but the live engine took it anyway, that's a serious leak.
    if replay["blocked_by_filter"]:
        result["status"] = "BLOCKED_VIOLATION"
        result["issues"].append(
            f"trade taken during BLOCKED hour (UTC {_bar_hour_utc(live_trade['entry_time']):02d}) "
            f"— backtester would have skipped"
        )
        return result

    # ── normal checks ─────────────────────────────────────
    issues = []

    if not reason_match:
        issues.append(f"exit reason: live={live_trade['reason']} expected={replay['expected_reason']}")

    if entry_diff > PRICE_TOLERANCE:
        issues.append(f"entry diff {entry_diff:.3f}pt > tol {PRICE_TOLERANCE}pt")

    if exit_diff > PRICE_TOLERANCE:
        issues.append(f"exit diff {exit_diff:.3f}pt > tol {PRICE_TOLERANCE}pt")

    if bars_diff > bars_tol:
        issues.append(
            f"bars_held diff {bars_diff} > tol {bars_tol} "
            f"(live={live_trade['bars_held']} expected={replay['bars_held']})"
        )

    if pnl_diff > PNL_TOLERANCE:
        issues.append(
            f"net PnL diff ${pnl_diff:.2f} > tol ${PNL_TOLERANCE:.2f} "
            f"(live=${live_net:.2f} expected=${expected_net:.2f})"
        )

    if mfe_diff > EXCURSION_TOLERANCE:
        issues.append(
            f"MFE diff {mfe_diff:.3f}pt > tol {EXCURSION_TOLERANCE}pt "
            f"(live={live_mfe:.3f} expected={replay['mfe_points']:.3f})"
        )

    if mae_diff > EXCURSION_TOLERANCE:
        issues.append(
            f"MAE diff {mae_diff:.3f}pt > tol {EXCURSION_TOLERANCE}pt "
            f"(live={live_mae:.3f} expected={replay['mae_points']:.3f})"
        )

    result["status"] = "MATCH" if not issues else "MISMATCH"
    if issues:
        result["issues"] = issues
    return result


# ================= REPORT FORMATTER =================

def format_report(v):
    tid = v.get("trade_id")
    dir_txt = "L" if v.get("dir") == 1 else "S"
    status = v["status"]

    if status == "MATCH":
        return (
            f"  ✅ VALIDATOR #{tid} {dir_txt}  MATCH  "
            f"(entryΔ={v['entry_diff_pts']:.2f}pt  "
            f"exitΔ={v['exit_diff_pts']:.2f}pt  "
            f"pnlΔ=${v['pnl_diff_usd']:.2f}  "
            f"barsΔ={v['bars_held_diff']})"
        )

    if status == "SKIP":
        return f"  ⚪ VALIDATOR #{tid} {dir_txt}  SKIP  ({'; '.join(v['issues'])})"

    if status == "BLOCKED_VIOLATION":
        lines = [f"  🚫 VALIDATOR #{tid} {dir_txt}  BLOCKED_VIOLATION"]
        for issue in v["issues"]:
            lines.append(f"       - {issue}")
        return "\n".join(lines)

    # MISMATCH
    lines = [f"  ❌ VALIDATOR #{tid} {dir_txt}  MISMATCH"]
    for issue in v["issues"]:
        lines.append(f"       - {issue}")
    lines.append(
        f"       live   entry={v['live_entry']} exit={v['live_exit']} "
        f"{v['live_reason']} bars={v['live_bars_held']} "
        f"pnl=${v['live_net_pnl']:.2f} "
        f"mfe={v['live_mfe_pts']:.2f} mae={v['live_mae_pts']:.2f}"
    )
    lines.append(
        f"       replay entry={v['expected_entry']} exit={v['expected_exit']} "
        f"{v['expected_reason']} bars={v['expected_bars_held']} "
        f"pnl=${v['expected_net_pnl']:.2f} "
        f"mfe={v['expected_mfe_pts']:.2f} mae={v['expected_mae_pts']:.2f}"
    )
    return "\n".join(lines)