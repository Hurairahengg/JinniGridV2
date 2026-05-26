"""
telegram_bot.py — Notifications module
Imported by live_engine.py and validator.py
"""

import requests
from datetime import datetime

# ─── FILL THESE IN ────────────────────────────────────────────────
BOT_TOKEN = '7320016249:AAH9wV_QttEVNnzlWw5wiqIvjWNgC1TQ4ow'  
CHAT_ID = '-5124585879'
# ──────────────────────────────────────────────────────────────────

ENABLED = True   # set False to silence


def _send(text):
    if not ENABLED:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        r = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=8)
        if r.status_code != 200:
            print(f"[telegram] HTTP {r.status_code}: {r.text}")
    except Exception as e:
        print(f"[telegram] send failed: {e}")


def send_status(msg):
    _send(f"ℹ️ <b>STATUS</b>\n<code>{msg}</code>")


def send_boot(config):
    """Fancy boot banner sent once when live engine starts."""
    risk_line = (
        f"SCALING 🔄 (live balance)"
        if config["scaling_enabled"]
        else f"FIXED 🔒 (base ${config['starting_balance']:.2f})"
    )
    msg = (
        f"╔═══════════════════════════╗\n"
        f"   🤖 <b>JINNI LIVE — ONLINE</b>\n"
        f"╚═══════════════════════════╝\n"
        f"\n"
        f"⚡️ <b>System booted successfully</b>\n"
        f"<i>{config['boot_time']}</i>\n"
        f"\n"
        f"━━━━━━━━ <b>MARKET</b> ━━━━━━━━\n"
        f"📊 Symbol:      <code>{config['symbol']}</code>\n"
        f"🧱 Bars:        <code>{config['brick']}pt Koko</code>  (rev={config['rev']}, clean=OFF)\n"
        f"💾 Memory:      <code>max {config['max_bars']} bars</code>\n"
        f"\n"
        f"━━━━━━━ <b>STRATEGY</b> ━━━━━━━\n"
        f"🎯 Streak:      <code>{config['streak']}</code>\n"
        f"⏱  TP after:    <code>{config['tp']} bars</code>\n"
        f"🛡  Stop loss:   <code>{config['sl']}pt</code>\n"
        f"\n"
        f"━━━━━━━━━ <b>RISK</b> ━━━━━━━━━\n"
        f"💰 Model:       {risk_line}\n"
        f"📈 Risk/$100:   <code>${config['rp100']:.2f}</code>  ({config['rp100']}%)\n"
        f"💵 Pt value:    <code>${config['pt_value']}/pt/lot</code>\n"
        f"\n"
        f"━━━━━━━━ <b>COSTS</b> ━━━━━━━━━\n"
        f"💸 Commission:  <code>${config['commission']}/lot</code>\n"
        f"🌀 Slippage:    <code>{config['slippage']}pt</code>\n"
        f"\n"
        f"🟢 <b>Status:</b> waiting for warmup…\n"
        f"📡 <b>Mode:</b> tick stream from MT5\n"
        f"\n"
    )
    _send(msg)


def send_warmup_done(bar_count, in_memory):
    msg = (
        f"✅ <b>WARMUP COMPLETE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Bars built:    <code>{bar_count}</code>\n"
        f"💾 In memory:     <code>{in_memory}</code>\n"
        f"\n"
        f"🚀 <b>Engine is LIVE</b>"
    )
    _send(msg)
def send_error(msg):
    _send(f"🚨 <b>ERROR</b>\n<code>{msg}</code>")


def send_signal(trade):
    """Called the moment a trade is opened."""
    dir_emoji = "🟢 LONG" if trade["dir"] == 1 else "🔴 SHORT"
    msg = (
        f"⚡️ <b>TRADE OPENED — {dir_emoji}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Symbol:</b> <code>{trade['symbol']}</code>\n"
        f"<b>Entry:</b>  <code>{trade['actual_entry']:.2f}</code>\n"
        f"<b>SL:</b>     <code>{trade['sl_price']:.2f}</code>  ({trade['sl_pts']:.1f}pt)\n"
        f"<b>Lots:</b>   <code>{trade['lots']:.2f}</code>\n"
        f"<b>Risk:</b>   <code>${trade['risk_used']:.2f}</code>\n"
        f"<b>Time:</b>   <code>{trade['entry_time']}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Streak bars: {trade.get('streak_summary','')}\n"
        f"Reversal:    {trade.get('reversal_summary','')}"
    )
    _send(msg)


def send_close(trade, verdict):
    """Called when a trade closes (TP/SL). Includes validator verdict."""
    pnl = trade["net_pnl"]
    pnl_emoji = "✅" if pnl > 0 else "❌"
    reason = "🎯 TP" if not trade["hit_sl"] else "🛑 SL"

    if verdict["match"]:
        v_emoji = "✅"
        v_line = (
            f"<b>Backtest match:</b> ✅ <code>OK</code>\n"
            f"Expected PnL: <code>${verdict['expected_pnl']:.2f}</code>  "
            f"(diff <code>${verdict['pnl_diff']:+.2f}</code> / "
            f"<code>{verdict['pnl_diff_pct']:+.2f}%</code>)"
        )
    else:
        v_emoji = "⚠️"
        v_line = (
            f"<b>Backtest match:</b> ⚠️ <code>MISMATCH</code>\n"
            f"Expected PnL: <code>${verdict['expected_pnl']:.2f}</code>  "
            f"Got: <code>${pnl:.2f}</code>\n"
            f"Reason: <code>{verdict['reason']}</code>"
        )

    msg = (
        f"{pnl_emoji} <b>TRADE CLOSED — {reason}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Symbol:</b>  <code>{trade['symbol']}</code>\n"
        f"<b>Dir:</b>     <code>{'LONG' if trade['dir']==1 else 'SHORT'}</code>\n"
        f"<b>Entry:</b>   <code>{trade['actual_entry']:.2f}</code>\n"
        f"<b>Exit:</b>    <code>{trade['actual_exit']:.2f}</code>\n"
        f"<b>Lots:</b>    <code>{trade['lots']:.2f}</code>\n"
        f"<b>Points:</b>  <code>{trade['pnl_points']:+.2f}</code>\n"
        f"<b>Gross:</b>   <code>${trade['gross_pnl']:+.2f}</code>\n"
        f"<b>Comm:</b>    <code>-${trade['commission']:.2f}</code>\n"
        f"<b>Net PnL:</b> <code>${pnl:+.2f}</code>\n"
        f"<b>Bars:</b>    <code>{trade['bars_held']}</code>\n"
        f"<b>R-mult:</b>  <code>{trade['r_multiple']:+.2f}R</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{v_line}"
    )
    _send(msg)


if __name__ == "__main__":
    # quick test
    send_status("telegram_bot.py self-test")