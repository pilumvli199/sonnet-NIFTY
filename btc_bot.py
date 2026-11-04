"""
╔══════════════════════════════════════════════════════════════════════════╗
║   BITCOIN (BTCUSD) INSTITUTIONAL TRADING ENGINE — V8.5 (MULTI-TF)        ║
║   Namespace: BTC:*  |  Delta Exchange Public API (5M Execution Engine)   ║
║                                                                          ║
║   TOP 6 PROVEN SETUPS INTEGRATED:                                        ║
║   1. 5M_VOLUME_EXPANSION_BLAST (1:3 RR - Tight Range + Vol Anomaly)      ║
║   2. S3_BREAKOUT_LIQUIDITY_HUNT (1:3 RR - Wyckoff Pre-Sweep Blast)       ║
║   3. ASIAN_OPEN_SWEEP (1:2 RR - Morning 07:30 to 11:30 IST Judas Trap)   ║
║   4. USER_3SWING_INDUCEMENT_TRAP (1:2 RR - SMC Inducement Sweep)         ║
║   5. S3_BREAKOUT_OPT (1:3 RR - Trend Aligned Compression Breakout)       ║
║   6. S2_PDH_PDL_SWEEP (1:2 RR - 0.7 ATR Buffer Daily Level Sweep)        ║
║                                                                          ║
║   CORE SECURITY & STABILITY ENHANCEMENTS:                                ║
║   • Multi-TF Sync: 15M Macro Structure + 5M Precision Execution          ║
║   • 503 Retry Guard: 2.5s Sleep & Retry for Gemini Server Overload       ║
║   • Fail-Closed AI Guard: If AI fails or times out, trade is BLOCKED     ║
║   • Built-in HTTP Health Server (Port 8080 / $PORT) for UptimeRobot      ║
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
from http.server import HTTPServer, BaseHTTPRequestHandler

IST = pytz.timezone("Asia/Kolkata")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("BTC")

# ==============================================================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# ==============================================================================
GEMINI_API_KEYS     = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip()]
ANTHROPIC_KEY       = os.getenv("ANTHROPIC_API_KEY", "")
TG_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID          = os.getenv("BTC_TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", ""))

DELTA_BASE_URL      = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange")
DELTA_API_KEY       = os.getenv("DELTA_API_KEY", "").strip()
DELTA_API_SECRET    = os.getenv("DELTA_API_SECRET", "").strip()
BTC_SYMBOL          = os.getenv("BTC_SYMBOL", "BTCUSD")
PORT                = int(os.getenv("PORT", "8080"))
WAIT_COOLDOWN_MIN   = int(os.getenv("BTC_WAIT_COOLDOWN_MIN", "15"))
SIGNAL_COOLDOWN_MIN = int(os.getenv("BTC_SIGNAL_COOLDOWN_MIN", "240"))  # 4 Hours Cooldown
GEMINI_MODEL        = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite")
CLAUDE_BACKUP_MODEL = os.getenv("CLAUDE_BACKUP_MODEL", "claude-haiku-4-5-20251001")

_btc_lock = threading.Lock()
_gemini_idx = 0
_last_gemini_call = 0.0
_daily_ai_call_count = 0
_last_daily_report_date = None
_last_macro_reanalysis_slot = None


def esc(text):
    return html.escape(str(text), quote=False) if text is not None else ""


# ==============================================================================
# IN-MEMORY STATE STORE (WITH REDIS/SHARED_STORE COMPATIBILITY)
# ==============================================================================
class DefaultStore:
    def __init__(self):
        self._data = {}
        self._exp = {}

    def set(self, key, value, ttl=86400):
        self._data[key] = value
        self._exp[key] = time.time() + ttl

    def get(self, key):
        if key in self._exp and time.time() > self._exp[key]:
            self._data.pop(key, None)
            self._exp.pop(key, None)
            return None
        return self._data.get(key)


_global_store = DefaultStore()


class BtcState:
    PREFIX = "BTC:"

    def __init__(self, shared_store=None):
        self.store = shared_store if shared_store is not None else _global_store

    def set(self, key, value, ttl=86400):
        self.store.set(f"{self.PREFIX}{key}", value, ttl=ttl)

    def get(self, key):
        return self.store.get(f"{self.PREFIX}{key}")


# ==============================================================================
# KOYEB HTTP HEALTH CHECK SERVER (FOR UPTIMEROBOT 24/7 PING)
# ==============================================================================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - BTC Institutional Liquidity Engine v8.5 Active\n")

    def log_message(self, format, *args):
        return


def start_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
        log.info(f"[BTC] Health Check HTTP server listening on port {PORT} for UptimeRobot")
        server.serve_forever()
    except Exception as e:
        log.error(f"[BTC] HTTP health server error: {e}")


# ==============================================================================
# DUAL AI SUPERVISOR (FAIL-CLOSED + 503 RETRY GUARD)
# ==============================================================================
def call_ai_json(system_prompt, user_prompt, max_tokens=700):
    global _gemini_idx, _last_gemini_call, _daily_ai_call_count

    if GEMINI_API_KEYS:
        now = time.time()
        elapsed = now - _last_gemini_call
        if elapsed < 5.0:
            time.sleep(5.0 - elapsed)
        _last_gemini_call = time.time()

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
                _daily_ai_call_count += 1
                r = requests.post(url, json=payload, timeout=20)
                
                # 429 Rate Limit Retry
                if r.status_code == 429:
                    log.warning("[BTC] Gemini 429 rate limit hit. Pausing 8s before retry...")
                    time.sleep(8.0)
                    _last_gemini_call = time.time()
                    r = requests.post(url, json=payload, timeout=20)
                # 500/503 Server Overload Retry
                elif r.status_code in (500, 503):
                    log.warning(f"[BTC] Gemini HTTP {r.status_code} on key #{_gemini_idx}. Pausing 2.5s and retrying...")
                    time.sleep(2.5)
                    _last_gemini_call = time.time()
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
                log.warning(f"[BTC] Gemini request error: {e}")

    # Fallback to Claude Backup if configured
    if ANTHROPIC_KEY:
        hdrs = {"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
        body = {"model": CLAUDE_BACKUP_MODEL, "max_tokens": max_tokens, "temperature": 0, "system": system_prompt, "messages": [{"role": "user", "content": user_prompt}]}
        try:
            _daily_ai_call_count += 1
            r = requests.post("https://api.anthropic.com/v1/messages", headers=hdrs, json=body, timeout=25)
            if r.status_code == 200:
                raw = r.json()["content"][0]["text"]
                clean = raw.strip().replace("```json", "").replace("```", "").strip()
                s, e = clean.find("{"), clean.rfind("}") + 1
                if s != -1:
                    return json.loads(clean[s:e]), f"CLAUDE_BACKUP ({CLAUDE_BACKUP_MODEL})"
        except Exception as e:
            log.error(f"[BTC] Claude backup error: {e}")

    return None, "AI_UNREACHABLE"


# ==============================================================================
# DELTA EXCHANGE PUBLIC/PRIVATE API CANDLES
# ==============================================================================
def fetch_delta_candles(symbol: str, resolution: str, hours_back: int, closed_only: bool = True):
    end_ts = int(time.time())
    start_ts = end_ts - (hours_back * 3600)
    url = f"{DELTA_BASE_URL}/v2/history/candles"
    params = {"symbol": symbol, "resolution": resolution, "start": start_ts, "end": end_ts}
    res_sec = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}.get(resolution, 1800)

    headers = {}
    if DELTA_API_KEY:
        headers["api-key"] = DELTA_API_KEY

    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        if r.status_code == 200:
            res = r.json().get("result", [])
            if not res:
                return pd.DataFrame()
            df = pd.DataFrame(res)
            df["time"] = df["time"].astype(int)
            df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(IST)
            df = df.rename(columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
            df[["o", "h", "l", "c"]] = df[["o", "h", "l", "c"]].astype(float).round(1)
            df = df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)

            if closed_only and not df.empty:
                last_open_ts = int(df.iloc[-1]["time"])
                if end_ts < (last_open_ts + res_sec):
                    df = df.iloc[:-1].reset_index(drop=True)
            return df
    except Exception as e:
        log.error(f"[BTC] Delta candle fetch error ({resolution}): {e}")
    return pd.DataFrame()


def tg_send(text):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info(f"[BTC-TG-DRY]\n{text}\n")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"[BTC] Telegram error: {e}")
        return False


# ==============================================================================
# TECHNICAL INDICATORS, ZONES & FRACTAL SWINGS
# ==============================================================================
def compute_indicators(df: pd.DataFrame):
    df_new = df.copy()
    high, low, close = df_new["h"], df_new["l"], df_new["c"]
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    df_new["atr14"] = tr.rolling(14).mean().bfill()
    df_new["ema50"] = close.ewm(span=50, adjust=False).mean()
    df_new["ema50_slope"] = df_new["ema50"] - df_new["ema50"].shift(12)

    df_new["roll_h96"] = high.rolling(96).max()
    df_new["roll_l96"] = low.rolling(96).min()
    df_new["range_pct"] = (close - df_new["roll_l96"]) / (df_new["roll_h96"] - df_new["roll_l96"] + 1e-6) * 100

    df_new["utc_date"] = df_new["ts"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d")
    daily = df_new.groupby("utc_date").agg({"h": "max", "l": "min"}).shift(1)
    daily.columns = ["pdh", "pdl"]
    daily = daily.reset_index()
    df_new = df_new.merge(daily, on="utc_date", how="left")
    return df_new


def find_swings(df: pd.DataFrame, w=3):
    sh, sl = [], []
    for idx in range(w, len(df) - w):
        h_val, l_val = float(df["h"].iloc[idx]), float(df["l"].iloc[idx])
        if all(h_val >= float(df["h"].iloc[idx - k]) for k in range(1, w + 1)) and all(h_val >= float(df["h"].iloc[idx + k]) for k in range(1, w + 1)):
            sh.append({"idx": idx, "price": h_val, "ts": df["ts"].iloc[idx]})
        if all(l_val <= float(df["l"].iloc[idx - k]) for k in range(1, w + 1)) and all(l_val <= float(df["l"].iloc[idx + k]) for k in range(1, w + 1)):
            sl.append({"idx": idx, "price": l_val, "ts": df["ts"].iloc[idx]})
    return sh, sl


def build_btc_zones(state: BtcState, df_macro: pd.DataFrame, ltp: float):
    zones = []
    if df_macro.empty or len(df_macro) < 20:
        return zones

    atr = float(df_macro["atr14"].iloc[-1]) if "atr14" in df_macro.columns else 350.0
    dynamic_window = max(600.0, 3.0 * atr)

    pdh = df_macro["pdh"].iloc[-1] if "pdh" in df_macro.columns else None
    pdl = df_macro["pdl"].iloc[-1] if "pdl" in df_macro.columns else None
    if pd.notna(pdh) and pd.notna(pdl):
        pdh, pdl = float(pdh), float(pdl)
        zones.append({"id": "BTC_PDH", "type": "RESISTANCE", "pivot": pdh, "low": pdh - 60, "high": pdh + 60, "why": f"Previous Day High (${pdh})"})
        zones.append({"id": "BTC_PDL", "type": "SUPPORT", "pivot": pdl, "low": pdl - 60, "high": pdl + 60, "why": f"Previous Day Low (${pdl})"})

    for i in range(3, len(df_macro) - 3):
        h_v, l_v = float(df_macro.loc[i, "h"]), float(df_macro.loc[i, "l"])
        if abs(h_v - ltp) <= dynamic_window and h_v >= df_macro.loc[i - 3:i + 3, "h"].max():
            zones.append({"id": f"BTC_SWH_{int(h_v)}", "type": "RESISTANCE", "pivot": h_v, "low": h_v - 50, "high": h_v + 50, "why": f"15M Swing High Liquidity (${h_v})"})
        if abs(l_v - ltp) <= dynamic_window and l_v <= df_macro.loc[i - 3:i + 3, "l"].min():
            zones.append({"id": f"BTC_SWL_{int(l_v)}", "type": "SUPPORT", "pivot": l_v, "low": l_v - 50, "high": l_v + 50, "why": f"15M Swing Low Liquidity (${l_v})"})

    zones.sort(key=lambda z: abs(z["pivot"] - ltp))
    dedup = []
    for z in zones:
        if not any(abs(z["pivot"] - ez["pivot"]) <= 120 for ez in dedup):
            dedup.append(z)
    state.set("active_zones", dedup[:8], ttl=1800)
    return dedup[:8]


# ==============================================================================
# TOP 6 INSTITUTIONAL PATTERN DETECTORS (ON CLOSED 5M EXECUTION BAR)
# ==============================================================================

# SETUP 1: 5M Volume Expansion Blast (Trend Aligned Breakout)
def check_5m_volume_expansion(df_5m: pd.DataFrame, df_15m: pd.DataFrame):
    if len(df_5m) < 20 or len(df_15m) < 10:
        return None
        
    i = len(df_5m) - 1
    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    v_curr = float(df_5m["v"].iloc[i])
    
    v_avg = df_5m["v"].iloc[i-14:i].mean()
    atr_5m = float(df_5m["atr14"].iloc[i])
    ema50_15m = float(df_15m["ema50"].iloc[-1])
    
    box = df_5m.iloc[i - 6:i]
    box_h, box_l = float(box["h"].max()), float(box["l"].min())
    box_range = box_h - box_l
    
    if box_range <= 1.5 * atr_5m and v_curr > 3.5 * v_avg:
        if c > box_h and c > ema50_15m and (c - o) > 0.8 * atr_5m:
            sl = round(box_l, 1)
            risk = max(60.0, round(c - sl, 1))
            return {
                "name": "5M_VOLUME_EXPANSION_BLAST",
                "action": "BUY_LONG",
                "tier": "A++ Institutional Breakout (+Volume Anomaly)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c + 3.0 * risk, 1),
                "be": round(c + 1.0 * risk, 1),
                "rr": "1:3.0",
                "why": f"Tight box (${box_l}-${box_h}) exploded UP with 3.5x Volume Anomaly ({int(v_curr)} vol) aligned with 15M Trend."
            }
            
        if c < box_l and c < ema50_15m and (o - c) > 0.8 * atr_5m:
            sl = round(box_h, 1)
            risk = max(60.0, round(sl - c, 1))
            return {
                "name": "5M_VOLUME_EXPANSION_BLAST",
                "action": "SELL_SHORT",
                "tier": "A++ Institutional Breakout (+Volume Anomaly)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c - 3.0 * risk, 1),
                "be": round(c - 1.0 * risk, 1),
                "rr": "1:3.0",
                "why": f"Tight box (${box_l}-${box_h}) exploded DOWN with 3.5x Volume Anomaly ({int(v_curr)} vol) aligned with 15M Trend."
            }
            
    return None

# SETUP 2: S3 Breakout + Pre-Breakout Liquidity Hunt (1:3 RR)
def check_s3_breakout_liq_hunt(df: pd.DataFrame):
    if len(df) < 15:
        return None
    i = len(df) - 1
    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    prev_c = float(df["c"].iloc[i - 1])
    atr = float(df["atr14"].iloc[i])
    ema50 = float(df["ema50"].iloc[i])
    slope = float(df["ema50_slope"].iloc[i])

    box = df.iloc[i - 6:i]
    box_h, box_l = float(box["h"].max()), float(box["l"].min())
    box_range = box_h - box_l

    if box_range > 1.35 * atr:
        return None

    recent_box = df.iloc[i - 4:i]
    lower_grab = any((float(row["l"]) <= box_l + 0.1 * atr and (min(float(row["o"]), float(row["c"])) - float(row["l"])) >= 0.3 * max(float(row["h"]) - float(row["l"]), 1.0)) for _, row in recent_box.iterrows())
    upper_grab = any((float(row["h"]) >= box_h - 0.1 * atr and (float(row["h"]) - max(float(row["o"]), float(row["c"]))) >= 0.3 * max(float(row["h"]) - float(row["l"]), 1.0)) for _, row in recent_box.iterrows())

    if c > box_h and prev_c <= box_h and c > ema50 and slope > -0.05 * atr and lower_grab:
        sl = round(box_l, 1)
        risk = max(60.0, round(c - sl, 1))
        return {
            "name": "S3_BREAKOUT_LIQUIDITY_HUNT",
            "action": "BUY_LONG",
            "tier": "A++ Institutional Pre-Sweep (+33R Engine)",
            "entry": c,
            "sl": sl,
            "risk": risk,
            "tp": round(c + 3.0 * risk, 1),
            "be": round(c + 1.0 * risk, 1),
            "rr": "1:3.0",
            "why": f"[Wyckoff Spring] Sellers trapped at ${box_l} before breaking UP above ${box_h}"
        }

    if c < box_l and prev_c >= box_l and c < ema50 and slope < 0.05 * atr and upper_grab:
        sl = round(box_h, 1)
        risk = max(60.0, round(sl - c, 1))
        return {
            "name": "S3_BREAKOUT_LIQUIDITY_HUNT",
            "action": "SELL_SHORT",
            "tier": "A++ Institutional Pre-Sweep (+33R Engine)",
            "entry": c,
            "sl": sl,
            "risk": risk,
            "tp": round(c - 3.0 * risk, 1),
            "be": round(c - 1.0 * risk, 1),
            "rr": "1:3.0",
            "why": f"[Upthrust Trap] Buyers trapped at ${box_h} before breaking DOWN below ${box_l}"
        }

    return None

# SETUP 3: Asian Open Range (AOR) Sweep / Judas Swing (07:30 to 11:30 IST) (1:2 RR)
def check_asian_open_sweep(df: pd.DataFrame):
    if len(df) < 15:
        return None
    i = len(df) - 1
    bar_ts = df["ts"].iloc[i]
    t_min = bar_ts.hour * 60 + bar_ts.minute

    if not (450 <= t_min <= 690):
        return None

    today_date = bar_ts.date()
    asian_candles = df[
        (df["ts"].dt.date == today_date)
        & (df["ts"].dt.hour >= 5)
        & ((df["ts"].dt.hour < 7) | ((df["ts"].dt.hour == 7) & (df["ts"].dt.minute < 30)))
    ]
    if len(asian_candles) < 4:
        return None

    aor_h = float(asian_candles["h"].max())
    aor_l = float(asian_candles["l"].min())

    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    atr = float(df["atr14"].iloc[i])
    c_range = max(h - l, 1.0)
    upper_wick = (h - max(o, c)) / c_range
    lower_wick = (min(o, c) - l) / c_range

    if h > aor_h and c < aor_h and upper_wick >= 0.35 and c <= o:
        sl = round(h + 0.5 * atr, 1)
        risk = max(60.0, round(sl - c, 1))
        return {
            "name": "ASIAN_OPEN_SWEEP",
            "action": "SELL_SHORT",
            "tier": "A++ Morning Judas Swing Reversal",
            "entry": c,
            "sl": sl,
            "risk": risk,
            "tp": round(c - 2.0 * risk, 1),
            "be": round(c - 1.0 * risk, 1),
            "rr": "1:2.0",
            "why": f"Swept Asian Open High (${aor_h}) with {int(upper_wick*100)}% rejection wick & closed RED"
        }

    if l < aor_l and c > aor_l and lower_wick >= 0.35 and c >= o:
        sl = round(l - 0.5 * atr, 1)
        risk = max(60.0, round(c - sl, 1))
        return {
            "name": "ASIAN_OPEN_SWEEP",
            "action": "BUY_LONG",
            "tier": "A++ Morning Judas Swing Reversal",
            "entry": c,
            "sl": sl,
            "risk": risk,
            "tp": round(c + 2.0 * risk, 1),
            "be": round(c + 1.0 * risk, 1),
            "rr": "1:2.0",
            "why": f"Swept Asian Open Low (${aor_l}) with {int(lower_wick*100)}% absorption wick & closed GREEN"
        }

    return None

# SETUP 4: User 3-Swing Inducement Trap (1:2 RR)
def check_user_3swing_inducement(df: pd.DataFrame, sw_h, sw_l):
    if len(df) < 30:
        return None
    i = len(df) - 1
    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    atr = float(df["atr14"].iloc[i])
    c_range = max(h - l, 1.0)
    upper_wick = (h - max(o, c)) / c_range
    lower_wick = (min(o, c) - l) / c_range
    range_pct = float(df["range_pct"].iloc[i])
    slope = float(df["ema50_slope"].iloc[i])

    if upper_wick >= 0.38 and c <= o and range_pct >= 65.0 and slope <= 0.25 * atr:
        recent_sh = [s for s in sw_h if s["idx"] <= i - 3]
        if len(recent_sh) >= 2:
            for idx_2 in range(len(recent_sh) - 1, max(-1, len(recent_sh) - 8), -1):
                s2 = recent_sh[idx_2]
                gap2 = i - s2["idx"]
                if not (6 <= gap2 <= 55) or not (h > s2["price"] and c < s2["price"]):
                    continue
                for idx_1 in range(idx_2 - 1, max(-1, idx_2 - 8), -1):
                    s1 = recent_sh[idx_1]
                    gap1 = s2["idx"] - s1["idx"]
                    if not (8 <= gap1 <= 60):
                        continue
                    if s2["price"] < s1["price"] and h < (s1["price"] + 0.3 * atr):
                        sl = round(h + 0.7 * atr, 1)
                        risk = max(60.0, round(sl - c, 1))
                        return {
                            "name": "USER_3SWING_INDUCEMENT_TRAP",
                            "action": "SELL_SHORT",
                            "tier": "A+ Structural Inducement Trap (50% Win Rate)",
                            "entry": c,
                            "sl": sl,
                            "risk": risk,
                            "tp": round(c - 2.0 * risk, 1),
                            "be": round(c - 1.0 * risk, 1),
                            "rr": "1:2.0",
                            "why": f"Swept Inducement High (${s2['price']}) below Major High (${s1['price']}) with {int(upper_wick*100)}% Red wick"
                        }

    if lower_wick >= 0.38 and c >= o and range_pct <= 35.0 and slope >= -0.25 * atr:
        recent_sl = [s for s in sw_l if s["idx"] <= i - 3]
        if len(recent_sl) >= 2:
            for idx_2 in range(len(recent_sl) - 1, max(-1, len(recent_sl) - 8), -1):
                s2 = recent_sl[idx_2]
                gap2 = i - s2["idx"]
                if not (6 <= gap2 <= 55) or not (l < s2["price"] and c > s2["price"]):
                    continue
                for idx_1 in range(idx_2 - 1, max(-1, idx_2 - 8), -1):
                    s1 = recent_sl[idx_1]
                    gap1 = s2["idx"] - s1["idx"]
                    if not (8 <= gap1 <= 60):
                        continue
                    if s2["price"] > s1["price"] and l > (s1["price"] - 0.3 * atr):
                        sl = round(l - 0.7 * atr, 1)
                        risk = max(60.0, round(c - sl, 1))
                        return {
                            "name": "USER_3SWING_INDUCEMENT_TRAP",
                            "action": "BUY_LONG",
                            "tier": "A+ Structural Inducement Trap (50% Win Rate)",
                            "entry": c,
                            "sl": sl,
                            "risk": risk,
                            "tp": round(c + 2.0 * risk, 1),
                            "be": round(c + 1.0 * risk, 1),
                            "rr": "1:2.0",
                            "why": f"Swept Inducement Low (${s2['price']}) above Major Low (${s1['price']}) with {int(lower_wick*100)}% Green wick"
                        }

    return None

# SETUP 5: S3 Breakout Trend Aligned (Clean Momentum) (1:3 RR)
def check_s3_breakout_opt(df: pd.DataFrame):
    if len(df) < 15:
        return None
    i = len(df) - 1
    c = float(df["c"].iloc[i])
    prev_c = float(df["c"].iloc[i - 1])
    atr = float(df["atr14"].iloc[i])
    ema50 = float(df["ema50"].iloc[i])
    slope = float(df["ema50_slope"].iloc[i])

    box = df.iloc[i - 6:i]
    box_h, box_l = float(box["h"].max()), float(box["l"].min())
    box_range = box_h - box_l

    if box_range > 1.35 * atr:
        return None

    if c > box_h and prev_c <= box_h and c > ema50 and slope > -0.05 * atr:
        sl = round(box_l, 1)
        risk = max(60.0, round(c - sl, 1))
        return {
            "name": "S3_BREAKOUT_OPT",
            "action": "BUY_LONG",
            "tier": "A Trend Aligned Compression Breakout",
            "entry": c,
            "sl": sl,
            "risk": risk,
            "tp": round(c + 3.0 * risk, 1),
            "be": round(c + 1.0 * risk, 1),
            "rr": "1:3.0",
            "why": f"6-Bar tight box broke UP above ${box_h} aligned with 50 EMA trend"
        }

    if c < box_l and prev_c >= box_l and c < ema50 and slope < 0.05 * atr:
        sl = round(box_h, 1)
        risk = max(60.0, round(sl - c, 1))
        return {
            "name": "S3_BREAKOUT_OPT",
            "action": "SELL_SHORT",
            "tier": "A Trend Aligned Compression Breakdown",
            "entry": c,
            "sl": sl,
            "risk": risk,
            "tp": round(c - 3.0 * risk, 1),
            "be": round(c - 1.0 * risk, 1),
            "rr": "1:3.0",
            "why": f"6-Bar tight box broke DOWN below ${box_l} aligned with 50 EMA trend"
        }

    return None

# SETUP 6: S2 PDH / PDL Sweep (With 0.7 ATR Buffer) (1:2 RR)
def check_s2_pdh_pdl_sweep(df: pd.DataFrame, state: BtcState):
    if len(df) < 15:
        return None
    i = len(df) - 1
    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    atr = float(df["atr14"].iloc[i])
    slope = float(df["ema50_slope"].iloc[i])
    pdh, pdl = df["pdh"].iloc[i], df["pdl"].iloc[i]
    d_key = str(df["utc_date"].iloc[i])

    if pd.isna(pdh) or pd.isna(pdl):
        return None
    pdh, pdl = float(pdh), float(pdl)

    c_range = max(h - l, 1.0)
    upper_wick = (h - max(o, c)) / c_range
    lower_wick = (min(o, c) - l) / c_range

    if not state.get(f"s2_taken_{d_key}_PDH"):
        if h > pdh and c < pdh and upper_wick >= 0.35 and c <= o and slope <= 0.25 * atr:
            sl = round(h + 0.7 * atr, 1)
            risk = max(60.0, round(sl - c, 1))
            return {
                "name": "S2_PDH_SWEEP",
                "action": "SELL_SHORT",
                "tier": "Tier-1 PDH Sweep (0.7 ATR Safe Buffer)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c - 2.0 * risk, 1),
                "be": round(c - 1.0 * risk, 1),
                "rr": "1:2.0",
                "s2_daily_key": f"s2_taken_{d_key}_PDH",
                "why": f"Swept Previous Day High (${pdh}) with {int(upper_wick*100)}% rejection wick and closed RED"
            }

    if not state.get(f"s2_taken_{d_key}_PDL"):
        if l < pdl and c > pdl and lower_wick >= 0.35 and c >= o and slope >= -0.25 * atr:
            sl = round(l - 0.7 * atr, 1)
            risk = max(60.0, round(c - sl, 1))
            return {
                "name": "S2_PDL_SWEEP",
                "action": "BUY_LONG",
                "tier": "Tier-1 PDL Sweep (0.7 ATR Safe Buffer)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c + 2.0 * risk, 1),
                "be": round(c + 1.0 * risk, 1),
                "rr": "1:2.0",
                "s2_daily_key": f"s2_taken_{d_key}_PDL",
                "why": f"Swept Previous Day Low (${pdl}) with {int(lower_wick*100)}% absorption wick and closed GREEN"
            }

    return None


# ==============================================================================
# DETERMINISTIC RISK, FOMO & EMA50 HEADROOM AUDIT
# ==============================================================================
def run_deterministic_filters(df: pd.DataFrame, setup: dict):
    i = len(df) - 1
    c = float(df["c"].iloc[i])
    atr = float(df["atr14"].iloc[i])
    ema50 = float(df["ema50"].iloc[i])
    risk = float(setup["risk"])

    if risk > 3.0 * atr:
        log.info(f"[BTC] Blocked {setup['name']}: Bloated Risk (${risk} > 3.0x ATR ${3.0*atr:.1f})")
        return False, "Bloated Risk"

    if i >= 3:
        open_3 = float(df["o"].iloc[i - 3])
        run_up = (c - open_3) if setup["action"] == "BUY_LONG" else (open_3 - c)
        if run_up > 3.5 * atr:
            log.info(f"[BTC] Blocked {setup['name']}: 3-Bar FOMO Chase (${run_up:.1f} > 3.5x ATR)")
            return False, "FOMO Chase"

    ema_note = f"Clear (50 EMA at ${ema50:.1f})"
    if setup["action"] == "BUY_LONG" and c < ema50 and (ema50 - c) < risk:
        ema_note = f"⚠️ WALL AHEAD: 50 EMA at ${ema50:.1f} (${ema50 - c:.0f} headroom < ${risk:.0f} risk)"
    elif setup["action"] == "SELL_SHORT" and c > ema50 and (c - ema50) < risk:
        ema_note = f"⚠️ WALL BELOW: 50 EMA at ${ema50:.1f} (${c - ema50:.0f} headroom < ${risk:.0f} risk)"

    setup["ema_note"] = ema_note
    return True, ema_note


# ==============================================================================
# DIAGNOSTIC STARTUP CHECK & ROUTINE HEALTH AUDITS
# ==============================================================================
def run_startup_diagnostics(state: BtcState):
    log.info("[BTC] 🔍 Running Startup Diagnostic Suite...")
    now = datetime.now(IST)

    t0 = time.time()
    df_test = fetch_delta_candles(BTC_SYMBOL, "5m", hours_back=3, closed_only=False)
    delta_lat = int((time.time() - t0) * 1000)
    delta_status = "ONLINE" if not df_test.empty else "OFFLINE"
    ltp_test = float(df_test.iloc[-1]["c"]) if not df_test.empty else 0.0

    t0 = time.time()
    ai_test_res, ai_provider = call_ai_json('Respond strictly in JSON: {"status":"ok"}', "Diagnostic ping. Return status ok.")
    ai_lat = int((time.time() - t0) * 1000)
    ai_status = f"ONLINE ({ai_lat}ms)" if ai_test_res else "OFFLINE / UNREACHABLE"

    state.set("diag_test", True, ttl=60)
    state_status = "OK" if state.get("diag_test") else "ERROR"

    startup_msg = f"""🚀 <b>[SYSTEM INITIALIZATION COMPLETE]</b>
📅 <b>Time:</b> <code>{now.strftime('%Y-%m-%d %H:%M:%S IST')}</code> | <b>Server:</b> <code>Koyeb</code>

🟢 <b>Delta Public API:</b> <code>{delta_status} (LTP: ${ltp_test:,.1f} | {delta_lat}ms)</code>
🟢 <b>AI Supervisor   :</b> <code>{ai_status} [{esc(ai_provider)}]</code>
🟢 <b>State Cache     :</b> <code>{state_status}</code>
🟢 <b>HTTP Health Check:</b> <code>RUNNING (Port {PORT} - UptimeRobot Ready)</code>

🎯 <b>Active Core Setups (Top 6 Multi-TF):</b>
- <code>5M_VOLUME_EXPANSION_BLAST</code> (Tight Range + Vol Spike)
- <code>S3_BREAKOUT_LIQUIDITY_HUNT</code> (1:3 RR)
- <code>ASIAN_OPEN_SWEEP</code> (07:30 - 11:30 IST)
- <code>USER_3SWING_INDUCEMENT_TRAP</code> (1:2 RR)
- <code>S3_BREAKOUT_OPT</code> (1:3 RR)
- <code>S2_PDH_PDL_SWEEP</code> (1:2 RR)

🛡️ <i>Fail-Closed AI Guard & 503 Retry active.</i>"""

    tg_send(startup_msg)
    log.info("[BTC] ✅ Startup diagnostics complete and report pushed to Telegram.")


def check_daily_0915_health_report(state: BtcState, df_15m: pd.DataFrame):
    global _last_daily_report_date
    now = datetime.now(IST)
    today_str = now.strftime("%Y-%m-%d")

    if now.hour == 9 and now.minute >= 15 and _last_daily_report_date != today_str:
        _last_daily_report_date = today_str

        ltp = float(df_15m.iloc[-1]["c"]) if not df_15m.empty else 0.0
        ema50 = float(df_15m["ema50"].iloc[-1]) if "ema50" in df_15m.columns else 0.0
        pdh = float(df_15m["pdh"].iloc[-1]) if "pdh" in df_15m.columns and pd.notna(df_15m["pdh"].iloc[-1]) else 0.0
        pdl = float(df_15m["pdl"].iloc[-1]) if "pdl" in df_15m.columns and pd.notna(df_15m["pdl"].iloc[-1]) else 0.0

        daily_msg = f"""☀️ <b>[DAILY 09:15 AM SYSTEM HEALTH AUDIT]</b>
📅 <code>{now.strftime('%d %b %Y | %H:%M IST')}</code>

📊 <b>Engine Status:</b>
- AI Calls Today   : <code>{_daily_ai_call_count} / 500 Calls</code>
- 5M Surveillance  : <code>ACTIVE (Closed-bar execution)</code>
- Uptime Server    : <code>Port {PORT} (200 OK)</code>

🗺️ <b>Macro Context (15M):</b>
- BTC LTP : <b>${ltp:,.1f}</b>
- 50 EMA  : <b>${ema50:,.1f}</b> ({'Bullish' if ltp > ema50 else 'Bearish'})
- PDH / PDL: <b>${pdh:,.1f} / ${pdl:,.1f}</b>

✅ <i>All 6 Multi-TF setups active.</i>"""
        tg_send(daily_msg)


def check_6h_macro_reanalysis(state: BtcState, df_15m: pd.DataFrame):
    global _last_macro_reanalysis_slot
    now = datetime.now(IST)

    slot_hours = [5, 11, 17, 23]
    cur_slot = None
    for h in slot_hours:
        if now.hour == h and 30 <= now.minute < 59:
            cur_slot = f"{now.strftime('%Y-%m-%d')}_{h}"
            break

    if cur_slot and cur_slot != _last_macro_reanalysis_slot:
        _last_macro_reanalysis_slot = cur_slot
        ltp = float(df_15m.iloc[-1]["c"]) if not df_15m.empty else 0.0
        ema50 = float(df_15m["ema50"].iloc[-1]) if "ema50" in df_15m.columns else 0.0
        r_pct = float(df_15m["range_pct"].iloc[-1]) if "range_pct" in df_15m.columns else 50.0

        log.info(f"[BTC] ═════════ [6-HOUR MACRO RE-ANALYSIS: {now.strftime('%H:%M IST')}] ═════════")
        log.info(f"[BTC] LTP: ${ltp:,.1f} | 50 EMA: ${ema50:,.1f} | 48h Range Percentile: {r_pct:.1f}%")
        log.info("[BTC] Active Setups Refreshed. Next re-analysis in 6 Hours.")

        zones = build_btc_zones(state, df_15m, ltp)
        slope = float(df_15m["ema50_slope"].iloc[-1])
        r_high = float(df_15m["roll_h96"].iloc[-1])
        r_low = float(df_15m["roll_l96"].iloc[-1])
        trend_bias = "BULLISH" if ltp > ema50 and slope > 0 else ("BEARISH" if ltp < ema50 and slope < 0 else "RANGING / CONSOLIDATION")

        sys_p = (
            "You are the Lead Institutional Crypto Trading Supervisor. "
            "Provide a concise 1-2 sentence macro structure assessment for BTCUSD. "
            "Respond strictly in JSON: {\"macro_bias\": \"BULLISH/BEARISH/NEUTRAL\", \"assessment\": \"1-2 sentences\"}"
        )
        usr_p = (
            f"Asset: BTCUSD | Time: {now.strftime('%Y-%m-%d %H:%M IST')} | Price: ${ltp}\n"
            f"50 EMA: ${ema50:.1f} (Slope: {slope:+.1f}) | 48h Range: ${r_low:.0f} - ${r_high:.0f} (Pos: {r_pct:.1f}%)\n"
            f"Key Zones: {', '.join([z['why'] for z in zones[:3]])}"
        )
        ai_res, provider = call_ai_json(sys_p, usr_p, max_tokens=300)
        ai_text = (ai_res or {}).get("assessment", "Consolidation within macro range. Monitor S3 box breakout or PDH/PDL sweeps.")
        ai_bias = (ai_res or {}).get("macro_bias", trend_bias)

        z_lines = [f"• <b>{esc(z['type'])}:</b> <code>${z['pivot']:,.1f}</code> ({esc(z['why'])})" for z in zones[:3]]

        msg = f"""🏛️ <b>[BITCOIN] 6-HOUR MACRO RE-ANALYSIS | {now.strftime('%H:%M IST')}</b>

📊 <b>LTP     :</b> <b>${ltp:,.1f}</b> | <b>Trend:</b> <b>{trend_bias}</b>
📐 <b>50 EMA :</b> <code>${ema50:,.1f}</code> (Slope: <code>{slope:+.1f}</code>)
📏 <b>48h Rng :</b> <code>${r_low:,.0f} — ${r_high:,.0f}</code> (Pos: <code>{r_pct:.1f}%</code>)

🗺️ <b>Active 15M Macro Zones:</b>
{chr(10).join(z_lines)}

🧠 <b>AI Macro Audit ({esc(provider)}):</b> [{esc(ai_bias)}]
<i>{esc(ai_text)}</i>"""
        tg_send(msg)


# ==============================================================================
# MAIN CYCLE RUNNER (EVALUATES CLOSED 5M CANDLES MULTI-TF)
# ==============================================================================
def run_btc_cycle(shared_store=None):
    if not _btc_lock.acquire(blocking=False):
        return

    try:
        state = BtcState(shared_store)
        state.set("last_tick", time.time(), ttl=86400)

        # Multi-TF Data Sync: 15M (Macro/Zones) + 5M (Execution)
        df_15m = fetch_delta_candles(BTC_SYMBOL, "15m", hours_back=60, closed_only=True)
        df_5m = fetch_delta_candles(BTC_SYMBOL, "5m", hours_back=24, closed_only=True)
        
        if df_5m.empty or len(df_5m) < 35 or df_15m.empty:
            return

        df_15m = compute_indicators(df_15m)
        df_5m = compute_indicators(df_5m)
        
        ltp = float(df_5m["c"].iloc[-1])
        last_bar_time = int(df_5m.iloc[-1]["time"])

        # Routine checks on 15M Macro context
        check_6h_macro_reanalysis(state, df_15m)
        check_daily_0915_health_report(state, df_15m)

        # Refresh active zones in state based on 15M structure
        zones = build_btc_zones(state, df_15m, ltp)

        # Ensure we only evaluate each closed 5M candle once
        if state.get("last_evaluated_5m_ts") == last_bar_time:
            return
        state.set("last_evaluated_5m_ts", last_bar_time, ttl=300)

        closed_ts_str = df_5m["ts"].iloc[-1].strftime("%H:%M IST")
        ema50 = float(df_15m["ema50"].iloc[-1])
        slope = float(df_15m["ema50_slope"].iloc[-1])
        trend_label = "BULLISH" if ltp > ema50 and slope > 0 else ("BEARISH" if ltp < ema50 and slope < 0 else "RANGING/CHOP")
        nearest_str = f"{zones[0]['id']}: ${zones[0]['pivot']}" if zones else "None"

        log.info(f"[BTC] ═════════ 5M EXECUTION AUDIT: {closed_ts_str} ═════════")
        log.info(f"[BTC] Close: ${ltp:,.1f} | 15M Trend: {trend_label} (50 EMA ${ema50:.1f})")
        log.info(f"[BTC] Nearest 15M Zone: {nearest_str} | AI Calls Today: {_daily_ai_call_count}/500")

        sw_h, sw_l = find_swings(df_5m, w=3)

        # Evaluate Top 6 Setups on 5M Timeframe
        setup = (
            check_5m_volume_expansion(df_5m, df_15m)
            or check_s3_breakout_liq_hunt(df_5m)
            or check_asian_open_sweep(df_5m)
            or check_user_3swing_inducement(df_5m, sw_h, sw_l)
            or check_s3_breakout_opt(df_5m)
            or check_s2_pdh_pdl_sweep(df_5m, state)
        )

        if not setup:
            log.info("[BTC] Setups Evaluated: Vol_Blast=NO | S3_Liq=NO | Asian_Sweep=NO | 3Swing_Trap=NO | S3_Opt=NO | S2=NO -> WAITING")
            return

        passed, filter_msg = run_deterministic_filters(df_5m, setup)
        if not passed:
            log.warning(f"[BTC] Setup {setup['name']} rejected by deterministic check: {filter_msg}")
            return

        cd_key = f"cd_{setup['name']}_{setup['action']}"
        cd_until = state.get(cd_key)
        if cd_until and time.time() < cd_until:
            rem_m = int((cd_until - time.time()) / 60)
            log.info(f"[BTC] Cooldown active for {setup['name']} ({rem_m}m remaining). Skipped.")
            return

        log.info(f"[BTC] 🧠 Calling AI Supervisor for {setup['name']} ({setup['action']})...")
        sys_p = (
            "You are the Lead Institutional Crypto Trading Supervisor. "
            "Validate Python's detected setup against Master Rules, EMA50 headroom wall, and candle quality. "
            "Respond ONLY in strictly valid JSON: "
            '{"approved": true/false, "confidence": "HIGH/MEDIUM/LOW", "view": "1 concise sentence"}'
        )
        usr_p = (
            f"Asset: BTCUSD | Closed Bar Time: {closed_ts_str} | Close: ${ltp}\n"
            f"Detected Setup: {setup['name']} ({setup['tier']})\n"
            f"Action: {setup['action']} | Entry: ${setup['entry']} | SL: ${setup['sl']} | TP: ${setup['tp']} ({setup['rr']} R:R)\n"
            f"Trigger Logic: {setup['why']}\n"
            f"EMA50 Check: {setup['ema_note']} | ATR(14): ${df_5m['atr14'].iloc[-1]:.1f}"
        )

        ai_res, provider = call_ai_json(sys_p, usr_p)

        if not ai_res or not ai_res.get("approved", False):
            reason = ai_res.get("view", "AI Disapproved") if ai_res else f"AI Offline/503 Failed ({provider})"
            log.warning(f"[BTC] 🛑 Trade Blocked by Fail-Closed Safety Guard: {reason}")
            state.set(cd_key, time.time() + WAIT_COOLDOWN_MIN * 60, ttl=WAIT_COOLDOWN_MIN * 60)
            return

        state.set(cd_key, time.time() + (SIGNAL_COOLDOWN_MIN * 60), ttl=SIGNAL_COOLDOWN_MIN * 60)
        if "s2_daily_key" in setup:
            state.set(setup["s2_daily_key"], True, ttl=86400)

        em = "🟢" if setup["action"] == "BUY_LONG" else "🔴"
        action_title = "BUY (LONG)" if setup["action"] == "BUY_LONG" else "SELL (SHORT)"
        ai_view = ai_res.get("view", "Institutional order flow alignment confirmed.")
        ai_conf = ai_res.get("confidence", "HIGH")

        msg = f"""{em} <b>[BITCOIN - BTCUSD] {action_title} | {datetime.now(IST).strftime('%H:%M IST')}</b>

⚡ <b>Setup    :</b> <code>{esc(setup['name'])}</code>
🏆 <b>Tier     :</b> {esc(setup['tier'])}
📊 <b>Risk:Rew :</b> <b>{setup['rr']} Target</b> (Risk: ${setup['risk']})

🎯 <b>Entry    :</b> <b>${setup['entry']}</b>
🛑 <b>StopLoss :</b> <b>${setup['sl']}</b>
✅ <b>1:1 BE   :</b> <b>${setup['be']}</b> (Move SL to Cost)
🚀 <b>Target   :</b> <b>${setup['tp']}</b>

📐 <b>EMA50    :</b> {esc(setup['ema_note'])}
📝 <b>Logic    :</b> {esc(setup['why'])}
🧠 <b>AI ({esc(provider)}):</b> [{esc(ai_conf)}] <i>{esc(ai_view)}</i>"""

        tg_send(msg)
        log.info(f"[BTC] 🔥 FIRED {setup['name']} ({setup['action']}) at ${setup['entry']} | AI: {ai_conf}")

    except Exception:
        log.exception("[BTC] Error in run_btc_cycle")
    finally:
        try:
            _btc_lock.release()
        except RuntimeError:
            pass


def run_6h_analysis(shared_store=None):
    """
    6-Hour Macro Re-analysis for Bitcoin (BTCUSD).
    Called by main.py at 05:31, 11:31, 17:31, 23:31 IST (UTC-aligned closes).
    """
    if not _btc_lock.acquire(blocking=False):
        return

    try:
        state = BtcState(shared_store)
        now = datetime.now(IST)

        df_15m = fetch_delta_candles(BTC_SYMBOL, "15m", hours_back=120, closed_only=True)
        if df_15m.empty or len(df_15m) < 35:
            return

        df_15m = compute_indicators(df_15m)
        ltp = float(df_15m["c"].iloc[-1])
        ema50 = float(df_15m["ema50"].iloc[-1])
        slope = float(df_15m["ema50_slope"].iloc[-1])
        r_pct = float(df_15m["range_pct"].iloc[-1])
        r_high = float(df_15m["roll_h96"].iloc[-1])
        r_low = float(df_15m["roll_l96"].iloc[-1])
        atr = float(df_15m["atr14"].iloc[-1])
        pdh = float(df_15m["pdh"].iloc[-1]) if pd.notna(df_15m["pdh"].iloc[-1]) else 0.0
        pdl = float(df_15m["pdl"].iloc[-1]) if pd.notna(df_15m["pdl"].iloc[-1]) else 0.0

        zones = build_btc_zones(state, df_15m, ltp)
        trend_bias = "BULLISH" if ltp > ema50 and slope > 0 else ("BEARISH" if ltp < ema50 and slope < 0 else "RANGING / CONSOLIDATION")

        log.info(f"[BTC] ═════════ 6-HOUR MACRO RE-ANALYSIS ({now.strftime('%H:%M IST')}) ═════════")
        log.info(f"[BTC] LTP: ${ltp:,.1f} | 50 EMA: ${ema50:,.1f} (Slope: {slope:+.1f}) | Trend: {trend_bias}")
        log.info(f"[BTC] Active Zones ({len(zones)}): {', '.join([z['id'] for z in zones[:4]])}")

        sys_p = (
            "You are the Lead Institutional Crypto Trading Supervisor. "
            "Provide a concise 1-2 sentence macro structure assessment for BTCUSD. "
            "Respond strictly in JSON: {\"macro_bias\": \"BULLISH/BEARISH/NEUTRAL\", \"assessment\": \"1-2 sentences\"}"
        )
        usr_p = (
            f"Asset: BTCUSD | Time: {now.strftime('%Y-%m-%d %H:%M IST')} | Price: ${ltp}\n"
            f"50 EMA: ${ema50:.1f} (Slope: {slope:+.1f}) | 48h Range: ${r_low:.0f} - ${r_high:.0f} (Pos: {r_pct:.1f}%)\n"
            f"PDH: ${pdh:.1f} | PDL: ${pdl:.1f} | Key Zones: {', '.join([z['why'] for z in zones[:3]])}"
        )

        ai_res, provider = call_ai_json(sys_p, usr_p, max_tokens=300)
        ai_text = (ai_res or {}).get("assessment", "Consolidation within macro range. Monitor 5M S3 box breakout or PDH/PDL sweeps.")
        ai_bias = (ai_res or {}).get("macro_bias", trend_bias)

        z_lines = [f"• <b>{esc(z['type'])}:</b> <code>${z['pivot']:,.1f}</code> ({esc(z['why'])})" for z in zones[:3]]

        msg = f"""🏛️ <b>[BITCOIN] 6-HOUR MACRO RE-ANALYSIS | {now.strftime('%H:%M IST')}</b>

📊 <b>LTP     :</b> <b>${ltp:,.1f}</b> | <b>Trend:</b> <b>{trend_bias}</b>
📐 <b>50 EMA :</b> <code>${ema50:,.1f}</code> (Slope: <code>{slope:+.1f}</code>)
📏 <b>48h Rng :</b> <code>${r_low:,.0f} — ${r_high:,.0f}</code> (Pos: <code>{r_pct:.1f}%</code>)

🗺️ <b>Active 15M Macro Zones:</b>
{chr(10).join(z_lines)}

🧠 <b>AI Macro Audit ({esc(provider)}):</b> [{esc(ai_bias)}]
<i>{esc(ai_text)}</i>"""

        tg_send(msg)
        log.info(f"[BTC] 🏛️ 6-Hour Macro Re-analysis published to Telegram.")

    except Exception:
        log.exception("[BTC] Error in run_6h_analysis")
    finally:
        try:
            _btc_lock.release()
        except RuntimeError:
            pass


def run_btc_30m_tick(shared_store=None):
    run_btc_cycle(shared_store)

def run_btc_1m_tick(shared_store=None):
    run_btc_cycle(shared_store)

if __name__ == "__main__":
    log.info("[BTC] Starting Bitcoin Institutional Engine v8.5 on Koyeb...")
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    state_init = BtcState()
    run_startup_diagnostics(state_init)

    log.info("[BTC] Starting 30-Second Continuous Polling Loop...")
    while True:
        try:
            run_btc_cycle()
        except Exception as e:
            log.error(f"[BTC] Loop iteration error: {e}")
        time.sleep(30)
