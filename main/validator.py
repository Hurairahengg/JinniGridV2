"""
validator.py — Backtest re-runner / trade validator.

For each closed live trade, takes the captured bars and re-runs the EXACT
same strategy logic (matching the original Python backtester). Compares
expected vs actual entry/exit/PnL and returns a verdict.

Can also be run standalone:  python validator.py
"""

import json
import os

# ─── MUST MATCH live_engine.py ───────────────────────────────────
STREAK_SIZE        = 3
TP_CLOSE_AFTER     = 3
FIXED_SL_POINTS    = 16.0
SLIPPAGE_POINTS    = 0.3
COMMISSION_PER_LOT = 0.8

# tolerances for "match"
PRICE_TOL_PTS      = 2.0     # entry/exit price tolerance
PNL_TOL_DOLLARS    = 2.0     # absolute $ tolerance
PNL_TOL_PCT        = 10.0    # OR % tolerance, whichever bigger
# ─────────────────────────────────────────────────────────────────

TRADE_LOG = "trades_log.json"


def direction(candle):
    if candle["close"] > candle["open"]:
        return 1
    if candle["close"] < candle["open"]:
        return -1
    return 0


def simulate_trade(bars, signal_idx, lots):
    """
    Re-runs the backtester logic on a window of bars where:
      bars[signal_idx - STREAK_SIZE : signal_idx]   = streak
      bars[signal_idx]                              = reversal
      bars[signal_idx + 1]                          = entry bar
      bars[signal_idx + 1 .. signal_idx + 1 + TP_CLOSE_AFTER] = walk-forward

    Tolerates partial walk-forward (e.g., SL hit before all TP bars elapsed).
    """
    # ─── diagnostic ───
    diag = f"len(bars)={len(bars)} signal_idx={signal_idx} STREAK_SIZE={STREAK_SIZE} TP_AFTER={TP_CLOSE_AFTER}"

    # ─── validate streak ───
    if signal_idx < STREAK_SIZE:
        return {"ok": False, "reason": f"not enough streak bars ({diag})"}

    streak_dir = direction(bars[signal_idx - STREAK_SIZE])
    if streak_dir == 0:
        return {"ok": False, "reason": f"streak base bar is doji ({diag})"}
    for j in range(signal_idx - STREAK_SIZE, signal_idx):
        if direction(bars[j]) != streak_dir:
            return {"ok": False, "reason": f"streak invalid at j={j} ({diag})"}

    reversal_dir = direction(bars[signal_idx])
    if reversal_dir != -streak_dir:
        return {"ok": False, "reason": f"reversal direction wrong ({diag})"}

    entry_idx = signal_idx + 1
    if entry_idx >= len(bars):
        return {"ok": False, "reason": f"no entry bar ({diag})"}

    # tp_idx might exceed available walk-forward bars if SL hit early — that's fine, we cap it
    tp_idx_ideal = entry_idx + TP_CLOSE_AFTER
    tp_idx = min(tp_idx_ideal, len(bars) - 1)
    walk_truncated = tp_idx < tp_idx_ideal

    # ─── entry ───
    raw_entry = bars[entry_idx]["open"]
    if reversal_dir == 1:
        entry_price = raw_entry + SLIPPAGE_POINTS
        sl_price    = entry_price - FIXED_SL_POINTS
    else:
        entry_price = raw_entry - SLIPPAGE_POINTS
        sl_price    = entry_price + FIXED_SL_POINTS

    # ─── walk forward ───
    hit_sl = False
    exit_price = None
    exit_idx = None
    for k in range(entry_idx, tp_idx + 1):
        bar = bars[k]
        if reversal_dir == 1:
            if bar["open"] <= sl_price:
                exit_price = bar["open"] - SLIPPAGE_POINTS
                hit_sl = True; exit_idx = k; break
            if bar["low"]  <= sl_price:
                exit_price = sl_price - SLIPPAGE_POINTS
                hit_sl = True; exit_idx = k; break
        else:
            if bar["open"] >= sl_price:
                exit_price = bar["open"] + SLIPPAGE_POINTS
                hit_sl = True; exit_idx = k; break
            if bar["high"] >= sl_price:
                exit_price = sl_price + SLIPPAGE_POINTS
                hit_sl = True; exit_idx = k; break

    if not hit_sl:
        if walk_truncated:
            # walk-forward was cut short (live SL fired before full window).
            # Can't simulate a TP exit — mark as inconclusive.
            return {"ok": False, "reason": f"walk-forward truncated, can't simulate TP ({diag})"}
        raw_tp = bars[tp_idx]["close"]
        exit_price = raw_tp - SLIPPAGE_POINTS if reversal_dir == 1 else raw_tp + SLIPPAGE_POINTS
        exit_idx = tp_idx

    # ─── pnl ───
    pnl_points    = (exit_price - entry_price) if reversal_dir == 1 else (entry_price - exit_price)
    gross_pnl     = pnl_points * lots
    commission    = lots * COMMISSION_PER_LOT
    net_pnl       = gross_pnl - commission

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
    """
    record = single trade dict written by live_engine.py
    contains the bars window + actual fills.
    Returns verdict dict.
    """
    bars       = record["bars_window"]
    signal_idx = record["signal_idx_in_window"]
    lots       = record["lots"]

    sim = simulate_trade(bars, signal_idx, lots)
    if not sim["ok"]:
        return {
            "match": False,
            "reason": f"sim_failed: {sim['reason']}",
            "expected_pnl": 0.0,
            "pnl_diff": 0.0,
            "pnl_diff_pct": 0.0,
        }

    # compare
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
        reasons.append(f"exit_reason_mismatch (live={'SL' if record['hit_sl'] else 'TP'}, sim={'SL' if sim['hit_sl'] else 'TP'})")

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
    }


# ─── standalone runner ───────────────────────────────────────────
def _run_standalone():
    if not os.path.exists(TRADE_LOG):
        print(f"no log file: {TRADE_LOG}")
        return
    with open(TRADE_LOG) as f:
        log = json.load(f)

    matches = 0
    mismatches = 0
    for i, rec in enumerate(log):
        if rec.get("status") != "closed":
            continue
        v = validate_trade(rec)
        tag = "✅ MATCH" if v["match"] else "⚠️ MISMATCH"
        print(f"#{i+1:>3} {tag}  actual=${rec['net_pnl']:+.2f}  "
              f"expected=${v['expected_pnl']:+.2f}  diff=${v['pnl_diff']:+.2f}  "
              f"({v['reason']})")
        if v["match"]: matches += 1
        else:          mismatches += 1

    print(f"\nTotal: {matches} match / {mismatches} mismatch")


if __name__ == "__main__":
    _run_standalone()