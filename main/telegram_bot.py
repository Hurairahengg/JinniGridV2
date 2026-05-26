"""
telegram_bot.py — Notifications module.

This file is intentionally a no-op until you wire it up.
All send_* functions exist but do nothing, so fleet.py can call them safely.

To enable real notifications later, fill in BOT_TOKEN/CHAT_ID and set ENABLED=True.
"""
import os
from dotenv import load_dotenv
load_dotenv()

import threading

# ─── FILL THESE IN to enable Telegram notifications ──────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
ENABLED   = True
# ─────────────────────────────────────────────────────────────────


def _send(text):
    if not ENABLED or not BOT_TOKEN or not CHAT_ID:
        return
    def _worker():
        try:
            import requests
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            r = requests.post(url, json={
                "chat_id": CHAT_ID, "text": text,
                "parse_mode": "HTML", "disable_web_page_preview": True,
            }, timeout=8)
            if r.status_code != 200:
                print(f"[telegram] HTTP {r.status_code}: {r.text}")
        except Exception as e:
            print(f"[telegram] send failed: {e}")
    threading.Thread(target=_worker, daemon=True).start()


def send_status(msg):              _send(f"ℹ️ STATUS\n{msg}")
def send_error(msg):                _send(f"🚨 ERROR\n{msg}")
def send_boot(config):              _send(f"🤖 JINNI LIVE — ONLINE\n{config.get('symbol','?')}")
def send_warmup_done(bars, in_mem): _send(f"✅ WARMUP COMPLETE\nbars={bars} in_mem={in_mem}")

def send_signal(trade):
    d = "🟢 LONG" if trade.get("dir") == 1 else "🔴 SHORT"
    _send(f"⚡️ OPEN — {d}\n{trade.get('symbol')} @ {trade.get('actual_entry')}  "
          f"sl={trade.get('sl_price')}  lots={trade.get('lots')}")

def send_close(trade, verdict):
    pnl = trade.get("net_pnl", 0)
    emoji = "✅" if pnl > 0 else "❌"
    reason = "🎯 TP" if not trade.get("hit_sl") else "🛑 SL"
    match = "✅ ok" if verdict.get("match") else f"⚠️ {verdict.get('reason','?')}"
    _send(f"{emoji} CLOSE — {reason}\n{trade.get('symbol')}  "
          f"pnl=${pnl:+.2f}  R={trade.get('r_multiple',0):.2f}\n"
          f"validator: {match}")