"""
╔══════════════════════════════════════════════════════════════════════════╗
║   GOLD (XAUTUSD) LIQUIDITY HUNT ENGINE — V7 (ISOLATED: gold_bot.py)      ║
║   Namespace: GOLD:*  |  Logger: [GOLD]  |  Delta Exchange XAUTUSD        ║
║                                                                          ║
║   CORE ARCHITECTURE:                                                     ║
║   1. Strict Isolation: Uses 'GOLD:' Redis/RAM prefix and its own lock    ║
║      so changes in Gold never affect Nifty 50 or Bitcoin.                ║
║   2. Proven Setups:                                                      ║
║      - S2: Previous Day High/Low (PDH/PDL) Sweep + Reclaim.              ║
║      - S1: London/NY Session & Morning-to-Afternoon Memory Box Sweep.    ║
║      - Trend Pullback: 15M Trend + 5M/1M Liquidity Sweep Reclaim.        ║
║   3. 1-Minute Zone Proximity Polling: Polls 1m candles every minute      ║
║      when Gold is within ±$10 of an active liquidity zone.               ║
║   4. Dual AI Router: Gemini Free Tier Primary -> Claude Backup.          ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import time
import html
import logging
import threading
import requests
import pandas as pd
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")
log = logging.getLogger("GOLD")

GEMINI_API_KEYS     = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip()]
ANTHROPIC_KEY       = os.getenv("ANTHROPIC_API_KEY", "")
TG_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID          = os.getenv("GOLD_TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", ""))

DELTA_BASE_URL      = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange")
GOLD_SYMBOL         = os.getenv("GOLD_SYMBOL", "XAUTUSD")
GOLD_RANGE_WINDOW   = float(os.getenv("GOLD_PRICE_RANGE_PTS", "12.0"))  # ±$12 selective memory window
WAIT_COOLDOWN_MIN   = int(os.getenv("GOLD_WAIT_COOLDOWN_MIN", "15"))
SIGNAL_COOLDOWN_MIN = int(os.getenv("GOLD_SIGNAL_COOLDOWN_MIN", "30"))

GEMINI_MODEL        = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
CLAUDE_BACKUP_MODEL = os.getenv("CLAUDE_BACKUP_MODEL", "claude-haiku-4-5-20251001")

_gold_lock = threading.Lock()
_gemini_idx = 0


def esc(text):
    return html.escape(str(text), quote=False) if text is not None else ""


class GoldState:
    """Strictly prefixes every Redis/RAM key with 'GOLD:' so Nifty and BTC never collide."""
    PREFIX = "GOLD:"

    def __init__(self, shared_store):
        self.store = shared_store

    def set(self, key, value, ttl=86400):
        self.store.set(f"{self.PREFIX}{key}", value, ttl=ttl)

    def get(self, key):
        return self.store.get(f"{self.PREFIX}{key}")


def call_ai_json(system_prompt, user_prompt, max_tokens=700):
    global _gemini_idx
    if GEMINI_API_KEYS:
        for _ in range(len(GEMINI_API_KEYS)):
            api_key = GEMINI_API_KEYS[_gemini_idx % len(GEMINI_API_KEYS)]
            _gemini_idx += 1
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={api_key}"
            payload = {
                "systemInstruction": {"parts": [{"text": system_prompt}]},
                "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                "generationConfig": {"temperature": 0.0, "maxOutputTokens": max_tokens, "responseMimeType": "application/json"},
            }
            try:
                r = requests.post(url, json=payload, timeout=20)
                if r.status_code == 200:
                    candidates = r.json().get("candidates", [])
                    if candidates:
                        raw = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                        clean = raw.strip().replace("```json", "").replace("```", "").strip()
                        s, e = clean.find("{"), clean.rfind("}") + 1
                        if s != -1:
                            return json.loads(clean[s:e]), f"GEMINI ({GEMINI_MODEL})"
            except Exception as e:
                log.warning(f"[GOLD] Gemini error: {e}")

    if ANTHROPIC_KEY:
        hdrs = {"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
        body = {"model": CLAUDE_BACKUP_MODEL, "max_tokens": max_tokens, "temperature": 0, "system": system_prompt, "messages": [{"role": "user", "content": user_prompt}]}
        try:
            r = requests.post("https://api.anthropic.com/v1/messages", headers=hdrs, json=body, timeout=25)
            if r.status_code == 200:
                raw = r.json()["content"][0]["text"]
                clean = raw.strip().replace("```json", "").replace("```", "").strip()
                s, e = clean.find("{"), clean.rfind("}") + 1
                if s != -1:
                    return json.loads(clean[s:e]), f"CLAUDE_BACKUP ({CLAUDE_BACKUP_MODEL})"
        except Exception as e:
            log.error(f"[GOLD] Claude backup failed: {e}")

    return None, "PYTHON_DETERMINISTIC"


def fetch_delta_candles(symbol: str, resolution: str, hours_back: int):
    end_ts = int(time.time())
    start_ts = end_ts - (hours_back * 3600)
    url = f"{DELTA_BASE_URL}/v2/history/candles"
    params = {"symbol": symbol, "resolution": resolution, "start": start_ts, "end": end_ts}
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            res = r.json().get("result", [])
            if not res:
                return pd.DataFrame()
            df = pd.DataFrame(res)
            df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(IST)
            df = df.rename(columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
            df[["o", "h", "l", "c"]] = df[["o", "h", "l", "c"]].astype(float).round(2)
            return df.sort_values("ts").reset_index(drop=True)
    except Exception as e:
        log.error(f"[GOLD] Delta candle fetch error ({resolution}): {e}")
    return pd.DataFrame()


def tg_send(text):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info(f"[GOLD-TG-DRY] {text[:120]}...")
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        log.error(f"[GOLD] Telegram error: {e}")


def build_gold_zones(state: GoldState, df_15m: pd.DataFrame, ltp: float):
    zones = []
    if df_15m.empty or len(df_15m) < 20:
        return zones

    df_15m["date_str"] = df_15m["ts"].dt.strftime("%Y-%m-%d")
    dates = sorted(df_15m["date_str"].unique())
    if len(dates) >= 2:
        prev_df = df_15m[df_15m["date_str"] == dates[-2]]
        pdh, pdl = float(prev_df["h"].max()), float(prev_df["l"].min())
        zones.append({"id": "GOLD_PDH", "type": "RESISTANCE", "pivot": pdh, "low": pdh - 1.5, "high": pdh + 1.5, "why": f"Previous Day High (${pdh:.2f})"})
        zones.append({"id": "GOLD_PDL", "type": "SUPPORT", "pivot": pdl, "low": pdl - 1.5, "high": pdl + 1.5, "why": f"Previous Day Low (${pdl:.2f})"})

    # Selective Historical Swing & Morning Session Memory within ±$12 of LTP
    for i in range(2, len(df_15m) - 3):
        h_v, l_v = float(df_15m.loc[i, "h"]), float(df_15m.loc[i, "l"])
        if abs(h_v - ltp) <= GOLD_RANGE_WINDOW and h_v >= df_15m.loc[i - 2:i + 2, "h"].max():
            zones.append({"id": f"GOLD_SWH_{h_v:.1f}", "type": "RESISTANCE", "pivot": h_v, "low": h_v - 1.2, "high": h_v + 1.2, "why": f"15M Swing High Liquidity (${h_v:.2f})"})
        if abs(l_v - ltp) <= GOLD_RANGE_WINDOW and l_v <= df_15m.loc[i - 2:i + 2, "l"].min():
            zones.append({"id": f"GOLD_SWL_{l_v:.1f}", "type": "SUPPORT", "pivot": l_v, "low": l_v - 1.2, "high": l_v + 1.2, "why": f"15M Swing Low Liquidity (${l_v:.2f})"})

    zones.sort(key=lambda z: abs(z["pivot"] - ltp))
    dedup = []
    for z in zones:
        if not any(abs(z["pivot"] - ez["pivot"]) <= 2.5 for ez in dedup):
            dedup.append(z)
    state.set("active_zones", dedup[:8], ttl=1800)
    return dedup[:8]


def run_gold_1m_tick(shared_store):
    """Called every 1 minute by main.py scheduler."""
    if not _gold_lock.acquire(blocking=False):
        return
    try:
        state = GoldState(shared_store)
        now = datetime.now(IST)
        df_5m = fetch_delta_candles(GOLD_SYMBOL, "5m", hours_back=12)
        if df_5m.empty or len(df_5m) < 10:
            return
        ltp = float(df_5m.iloc[-1]["c"])

        zones = state.get("active_zones")
        if not zones or (now.minute % 15 == 0):
            df_15m = fetch_delta_candles(GOLD_SYMBOL, "15m", hours_back=96)
            zones = build_gold_zones(state, df_15m, ltp)

        if not zones:
            return

        # Poll 1-Minute candles when Gold is near any liquidity zone (±$12)
        near = [z for z in zones if abs(ltp - z["pivot"]) <= GOLD_RANGE_WINDOW]
        if not near:
            return

        df_1m = fetch_delta_candles(GOLD_SYMBOL, "1m", hours_back=3)
        d5 = df_5m.iloc[:-1].tail(8).reset_index(drop=True)
        ema20 = d5["c"].ewm(span=20, adjust=False).mean().iloc[-1]
        trend = "UPTREND" if ltp > ema20 else "DOWNTREND"

        c1 = d5.iloc[-1]
        c2 = d5.iloc[-2]
        o1, h1, l1, cl1 = float(c1["o"]), float(c1["h"]), float(c1["l"]), float(c1["c"])

        for z in near:
            zid = z["id"]
            cd = state.get(f"cd_{zid}")
            if cd and time.time() < cd:
                continue

            piv = z["pivot"]
            sig_payload = None

            if z["type"] == "SUPPORT" and min(l1, float(c2["l"])) < piv and cl1 > piv and cl1 > o1:
                sl = round(min(l1, float(c2["l"])) - 0.80, 2)
                risk = round(max(1.20, cl1 - sl), 2)
                if risk <= 6.50:
                    sig_payload = {"action": "BUY_GOLD", "entry": cl1, "sl": sl, "tp": round(cl1 + risk * 2.5, 2), "be": round(cl1 + risk * 1.1, 2), "detail": f"Swept Support ${piv:.2f} (low ${min(l1, float(c2['l'])):.2f}) & closed Green at ${cl1:.2f}"}

            elif z["type"] == "RESISTANCE" and max(h1, float(c2["h"])) > piv and cl1 < piv and cl1 < o1:
                sl = round(max(h1, float(c2["h"])) + 0.80, 2)
                risk = round(max(1.20, sl - cl1), 2)
                if risk <= 6.50:
                    sig_payload = {"action": "SELL_GOLD", "entry": cl1, "sl": sl, "tp": round(cl1 - risk * 2.5, 2), "be": round(cl1 - risk * 1.1, 2), "detail": f"Swept Resistance ${piv:.2f} (high ${max(h1, float(c2['h'])):.2f}) & closed Red at ${cl1:.2f}"}

            if not sig_payload:
                continue

            sys_p = "You are a Gold (XAUTUSD) Institutional Liquidity Hunt Supervisor. Respond ONLY in JSON: {\"approved\":true/false,\"confidence\":\"HIGH/MED/LOW\",\"view\":\"1 line\"}"
            usr_p = f"GOLD LTP: ${ltp:.2f} | Trend: {trend} | Zone: {zid} (${piv:.2f}) | Trigger: {sig_payload['detail']}"
            ai_res, provider = call_ai_json(sys_p, usr_p)
            if ai_res and not ai_res.get("approved", True):
                state.set(f"cd_{zid}", time.time() + WAIT_COOLDOWN_MIN * 60, ttl=900)
                continue

            state.set(f"cd_{zid}", time.time() + SIGNAL_COOLDOWN_MIN * 60, ttl=1800)
            em = "🟢" if sig_payload["action"] == "BUY_GOLD" else "🔴"
            tg_send(f"""{em} <b>[GOLD - XAUTUSD] {sig_payload['action']} | {now.strftime('%H:%M IST')}</b>

<b>Zone    :</b> {esc(zid)} — {esc(z['why'])}
<b>Trend   :</b> {trend} | <b>Entry:</b> ${sig_payload['entry']:.2f}
<b>Small SL:</b> <b>${sig_payload['sl']:.2f}</b> | <b>1.1x BE:</b> <b>${sig_payload['be']:.2f}</b>
<b>Target  :</b> <b>${sig_payload['tp']:.2f}</b> (1:2.5 R:R)
<b>Detail  :</b> {esc(sig_payload['detail'])}
🧠 <i>{esc((ai_res or {}).get('view', 'Gold Liquidity Sweep confirmed.'))} ({esc(provider)})</i>""")
            log.info(f"[GOLD] 🔥 Fired {sig_payload['action']} at {zid}")
            break

    except Exception:
        log.exception("[GOLD] Error in 1m tick")
    finally:
        try:
            _gold_lock.release()
        except RuntimeError:
            pass