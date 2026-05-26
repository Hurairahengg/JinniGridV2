"""
validator.py — Backtest re-runner / trade validator.

Reads strategy params from the trade record itself (embedded by live_engine
at order open). No more module-level constants drifting out of sync.
"""

import json
import os

# ─── default fallbacks if a (legacy) trade has no embedded params ────
DEFAULT_PARAMS = {
    "streak_size":        2,
    "tp_close_after":     3,
    "fixed_sl_points":    16.0,
    "slippage_points":    0.3,
    "commission_per_lot": 0.8,
}

# tolerances for "match"
PRICE_TOL_PTS   = 2.0
PNL_TOL_DOLLARS = 2.0
PNL_TOL_PCT     = 10.0

TRADE_LOG = "trades_log.json"


def direction(candle):
    if candle["close"] > candle["open"]:
        return 1
    if candle["close"] < candle["open"]:
        return -1
    return 0


def _params_from(record):
    """Pull strategy params from the trade record, with safe fallbacks."""
    return {
        "streak_size":        record.get("streak_size",        DEFAULT_PARAMS["streak_size"]),
        "tp_close_after":     record.get("tp_close_after",     DEFAULT_PARAMS["tp_close_after"]),
        "fixed_sl_points":    record.get("sl_pts",             DEFAULT_PARAMS["fixed_sl_points"]),
        "slippage_points":    record.get("slippage_points",    DEFAULT_PARAMS["slippage_points"]),
        "commission_per_lot": record.get("commission_per_lot", DEFAULT_PARAMS["commission_per_lot"]),
    }


def simulate_trade(bars, signal_idx, lots, params):
    """
    Re-runs backtester logic on the trade's captured bar window.
    Uses params from the trade record (not module globals).
    """
    streak_size        = params["streak_size"]
    tp_close_after     = params["tp_close_after"]
    fixed_sl_points    = params["fixed_sl_points"]
    slippage_points    = params["slippage_points"]
    commission_per_lot = params["commission_per_lot"]

    diag = (f"len(bars)={len(bars)} signal_idx={signal_idx} "
            f"STREAK_SIZE={streak_size} TP_AFTER={tp_close_after}")

    # ─── streak validation ───
    if signal_idx < streak_size:
        return {"ok": False, "reason": f"not enough streak bars ({diag})"}

    streak_dir = direction(bars[signal_idx - streak_size])
    if streak_dir == 0:
        return {"ok": False, "reason": f"streak base bar is doji ({diag})"}
    for j in range(signal_idx - streak_size, signal_idx):
        if direction(bars[j]) != streak_dir:
            return {"ok": False, "reason": f"streak invalid at j={j} ({diag})"}

    reversal_dir = direction(bars[signal_idx])
    if reversal_dir != -streak_dir:
        return {"ok": False, "reason": f"reversal direction wrong ({diag})"}

    entry_idx = signal_idx + 1
    if entry_idx >= len(bars):
        return {"ok": False, "reason": f"no entry bar ({diag})"}

    tp_idx_ideal = entry_idx + tp_close_after
    tp_idx = min(tp_idx_ideal, len(bars) - 1)
    walk_truncated = tp_idx < tp_idx_ideal

    # ─── entry ───
    raw_entry = bars[entry_idx]["open"]
    if reversal_dir == 1:
        entry_price = raw_entry + slippage_points
        sl_price    = entry_price - fixed_sl_points
    else:
        entry_price = raw_entry - slippage_points
        sl_price    = entry_price + fixed_sl_points

    # ─── walk forward ───
    hit_sl = False
    exit_price = None
    exit_idx = None
    for k in range(entry_idx, tp_idx + 1):
        bar = bars[k]
        if reversal_dir == 1:
            if bar["open"] <= sl_price:
                exit_price = bar["open"] - slippage_points
                hit_sl = True; exit_idx = k; break
            if bar["low"] <= sl_price:
                exit_price = sl_price - slippage_points
                hit_sl = True; exit_idx = k; break
        else:
            if bar["open"] >= sl_price:
                exit_price = bar["open"] + slippage_points
                hit_sl = True; exit_idx = k; break
            if bar["high"] >= sl_price:
                exit_price = sl_price + slippage_points
                hit_sl = True; exit_idx = k; break

    if not hit_sl:
        if walk_truncated:
            return {"ok": False, "reason": f"walk-forward truncated, can't simulate TP ({diag})"}
        raw_tp = bars[tp_idx]["close"]
        exit_price = raw_tp - slippage_points if reversal_dir == 1 else raw_tp + slippage_points
        exit_idx = tp_idx

    pnl_points = (exit_price - entry_price) if reversal_dir == 1 else (entry_price - exit_price)
    gross_pnl  = pnl_points * lots
    commission = lots * commission_per_lot
    net_pnl    = gross_pnl - commission

    return {
        "ok": True,
        "dir": reversal_dir,
        "entry_price": entry_price,
        "sl_price": sl_price,
        "exit_price": exit_price,
        "exit_idx": exit_idx,
        "hit_sl": hit_sl,
        "pnl_points": pnl_points,
        "gross_pnl": gross_pnl,
        "commission": commission,
        "net_pnl": net_pnl,
    }


def validate_trade(record):
    bars       = record["bars_window"]
    signal_idx = record["signal_idx_in_window"]
    lots       = record["lots"]
    params     = _params_from(record)

    sim = simulate_trade(bars, signal_idx, lots, params)
    if not sim["ok"]:
        return {
            "match": False,
            "reason": f"sim_failed: {sim['reason']}",
            "expected_pnl": 0.0,
            "pnl_diff": 0.0,
            "pnl_diff_pct": 0.0,
        }

    actual_entry = record["actual_entry"]
    actual_exit  = record["actual_exit"]
    actual_pnl   = record["net_pnl"]

    entry_diff = abs(actual_entry - sim["entry_price"])
    exit_diff  = abs(actual_exit  - sim["exit_price"])
    pnl_diff   = actual_pnl - sim["net_pnl"]
    base_pnl   = max(abs(sim["net_pnl"]), 1.0)
    pnl_diff_pct = (pnl_diff / base_pnl) * 100.0

    reasons = []
    if entry_diff > PRICE_TOL_PTS:
        reasons.append(f"entry_off_{entry_diff:.2f}pt")
    if exit_diff > PRICE_TOL_PTS:
        reasons.append(f"exit_off_{exit_diff:.2f}pt")
    if abs(pnl_diff) > PNL_TOL_DOLLARS and abs(pnl_diff_pct) > PNL_TOL_PCT:
        reasons.append(f"pnl_off_${pnl_diff:+.2f}")
    if record["hit_sl"] != sim["hit_sl"]:
        reasons.append(f"exit_reason_mismatch (live={'SL' if record['hit_sl'] else 'TP'}, "
                       f"sim={'SL' if sim['hit_sl'] else 'TP'})")

    return {
        "match": len(reasons) == 0,
        "reason": ",".join(reasons) if reasons else "ok",
        "expected_entry": sim["entry_price"],
        "expected_exit":  sim["exit_price"],
        "expected_pnl":   sim["net_pnl"],
        "actual_pnl":     actual_pnl,
        "pnl_diff":       pnl_diff,
        "pnl_diff_pct":   pnl_diff_pct,
        "expected_hit_sl": sim["hit_sl"],
        "params_used":    params,
    }


# ─── standalone runner ──────────────────────────────────────────
def _run_standalone():
    if not os.path.exists(TRADE_LOG):
        print(f"no log file: {TRADE_LOG}"); return
    with open(TRADE_LOG) as f:
        log = json.load(f)
    matches = mismatches = 0
    for i, rec in enumerate(log):
        if rec.get("status") != "closed":
            continue
        v = validate_trade(rec)
        tag = "✅ MATCH" if v["match"] else "⚠️ MISMATCH"
        print(f"#{i+1:>3} {tag}  actual=${rec['net_pnl']:+.2f}  "
              f"expected=${v['expected_pnl']:+.2f}  diff=${v['pnl_diff']:+.2f}  ({v['reason']})")
        if v["match"]: matches += 1
        else:          mismatches += 1
    print(f"\nTotal: {matches} match / {mismatches} mismatch")


if __name__ == "__main__":
    _run_standalone()