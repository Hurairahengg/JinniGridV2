"""Fire-and-forget telegram notifier for Koko Live Engine."""

import threading
import requests
from datetime import datetime, timezone

# ============================ CONFIG ============================

BOT_TOKEN = '7320016249:AAH9wV_QttEVNnzlWw5wiqIvjWNgC1TQ4ow'  
CHAT_ID = '-5124585879'
ENABLED   = True
TIMEOUT   = 5

# ============================ CORE ============================

def _send(text):
    if not ENABLED: return
    if BOT_TOKEN.startswith("PASTE") or CHAT_ID.startswith("PASTE"):
        print(f"[TG-SKIP] {text.splitlines()[0]}")
        return

    def _do():
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            r = requests.post(url, json={
                "chat_id": CHAT_ID, "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }, timeout=TIMEOUT)
            if r.status_code != 200:
                print(f"[TG-ERR] {r.status_code} {r.text[:200]}")
        except Exception as e:
            print(f"[TG-ERR] {e}")

    threading.Thread(target=_do, daemon=True).start()

def _ts_fmt(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

# ============================ TEMPLATES ============================

def notify_start(symbol, range_size, rev, clean, streak, tp, sl,
                 starting_balance, risk_per_100, live_trading):
    mode = "🔴 *LIVE TRADING*" if live_trading else "🟡 *SIM MODE*"
    initial_risk = (starting_balance / 100.0) * risk_per_100
    text = (
        f"🚀 *KOKO ENGINE STARTED*\n"
        f"{mode}\n\n"
        f"*Symbol:* `{symbol}`\n"
        f"*Bars:* `{range_size}pt | rev {rev}x | clean {'ON' if clean else 'OFF'}`\n"
        f"*Strategy:* `streak={streak}, tp={tp}, SL={sl}pt`\n"
        f"*Balance:* `${starting_balance:.2f}`\n"
        f"*Risk:* `${risk_per_100}/100 → ${initial_risk:.2f}/trade`\n"
        f"_started {_ts_fmt(int(datetime.now(timezone.utc).timestamp()))}_"
    )
    _send(text)

def notify_warmup_done(bar_count, last_price):
    _send(
        f"✅ *WARMUP COMPLETE*\n"
        f"Built `{bar_count}` bars\n"
        f"Last price: `{last_price}`\n"
        f"*Strategy is now LIVE.*"
    )

def notify_entry(trade_id, direction, entry, sl, lots, bar_time, position_count):
    side = "🟢 LONG" if direction == 1 else "🔴 SHORT"
    text = (
        f"*{side} OPENED* `#{trade_id}`\n"
        f"*Entry:* `{entry:.2f}`\n"
        f"*SL:* `{sl:.2f}` ({abs(entry-sl):.1f}pt)\n"
        f"*Lots:* `{lots}`\n"
        f"*Open positions:* `{position_count}`\n"
        f"_{_ts_fmt(bar_time)}_"
    )
    _send(text)

def notify_exit(trade_id, direction, entry, exit_px, net_pnl, reason, bars_held, equity, position_count):
    side_txt = "LONG" if direction == 1 else "SHORT"
    result = "*TAKE PROFIT* ✅" if reason == "TP" else "*STOP LOSS* ❌"
    pnl_sign = "+" if net_pnl >= 0 else ""
    text = (
        f"{result} `#{trade_id}`\n"
        f"*{side_txt}* `{entry:.2f}` → `{exit_px:.2f}`\n"
        f"*PnL:* `{pnl_sign}${net_pnl:.2f}`\n"
        f"*Bars held:* `{bars_held}`\n"
        f"*Equity:* `${equity:.2f}`\n"
        f"*Open positions:* `{position_count}`"
    )
    _send(text)

def notify_validation(v):
    tid = v.get("trade_id")
    dir_txt = "LONG" if v.get("dir") == 1 else "SHORT"
    if v["status"] == "MATCH":
        text = (
            f"✅ *VALIDATOR #{tid}* — MATCH\n"
            f"`{dir_txt}  entryΔ={v['entry_diff_pts']:.2f}pt  exitΔ={v['exit_diff_pts']:.2f}pt`"
        )
    elif v["status"] == "SKIP":
        text = (
            f"⚪ *VALIDATOR #{tid}* — SKIP\n"
            f"_{'; '.join(v['issues'])}_"
        )
    else:
        issues = "\n".join(f"• {i}" for i in v["issues"])
        text = (
            f"❌ *VALIDATOR #{tid}* — MISMATCH\n{issues}\n\n"
            f"*Live:*   `entry={v['live_entry']}  exit={v['live_exit']}  {v['live_reason']}  bars={v['live_bars_held']}`\n"
            f"*Replay:* `entry={v['expected_entry']}  exit={v['expected_exit']}  {v['expected_reason']}  bars={v['expected_bars_held']}`"
        )
    _send(text)

def notify_error(msg):
    _send(f"⚠️ *ENGINE ERROR*\n```\n{msg[:500]}\n```")