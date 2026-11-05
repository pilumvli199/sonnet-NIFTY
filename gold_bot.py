"""
╔══════════════════════════════════════════════════════════════════════════╗
║   GOLD (XAUTUSD) INSTITUTIONAL TRADING ENGINE — V9.0 PURE PRICE ACTION   ║
║   Namespace: GOLD:* | Logger: [GOLD] | Delta Exchange Public API         ║
║   Timeframe: 5M Execution Engine | Multi-Asset Isolation Enabled         ║
║                                                                          ║
║   100% PURE PRICE ACTION ARCHITECTURE (ZERO ATR DEPENDENCY):             ║
║   1. 5M_VOLUME_EXPANSION_BLAST (Relative Body >= 2x + Fixed Box <= $9)   ║
║   2. TREND_PULLBACK_RETEST (Catches Retests after Giant Breakouts)       ║
║   3. PDH_PDL_MOMENTUM_BREAKOUT (Clean Daily Expansion with Vol >= 1.8x)  ║
║   4. S2_PDH_PDL_SWEEP (30% Rejection Wick Trap Reversal — No Day Lock)   ║
║   5. SESSION_RANGE_SWEEP (Asian/London Range Sweep Reversals)            ║
║   6. USER_3SWING_INDUCEMENT_TRAP (SMC Structure — No 35-65% Deadzone)    ║
║   7. S3_COMPRESSION_BREAKOUT (6-Bar Box Compression <= $9.00 Breakout)   ║
║                                                                          ║
║   RELAXED GUARDS & BOT STABILITY FIXES:                                  ║
║   • No FOMO Block (3-Bar Chase Block Removed for Real Breakouts)         ║
║   • Pullback Router: Giant candles (> $7.50) NOT skipped, routed to PB!  ║
║   • Cooldown reduced from 240m to 35m for fast market continuation       ║
║   • Honest AI Fallback: Never broadcasts fake default text if AI fails   ║
║   • 24h Range Label Fixed (96 bars of 15m = 24 Hours)                    ║
║   • Delta Error Logging uses {type(e).__name__} (Zero API Key Leak)      ║
║   • Fail-Closed AI Guard + 503 Retry Guard (2.5s Sleep & Retry)          ║
║   • Built-in HTTP Health Server (Port 8080 / $PORT) for Koyeb / Uptime   ║
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
log = logging.getLogger("GOLD")

# ==============================================================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# ==============================================================================
GEMINI_API_KEYS     = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip() and "your_" not in k.lower()]
ANTHROPIC_KEY       = os.getenv("ANTHROPIC_API_KEY", "").strip()
if "your_" in ANTHROPIC_KEY.lower():
    ANTHROPIC_KEY = ""
TG_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID          = os.getenv("GOLD_TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", "")).strip()

DELTA_BASE_URL      = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange")
DELTA_API_KEY       = os.getenv("DELTA_API_KEY", "").strip()
if "your_" in DELTA_API_KEY.lower():
    DELTA_API_KEY = ""
GOLD_SYMBOL         = os.getenv("GOLD_SYMBOL", "XAUTUSD")
PORT                = int(os.getenv("PORT", "8080"))
WAIT_COOLDOWN_MIN   = int(os.getenv("GOLD_WAIT_COOLDOWN_MIN", "10"))
SIGNAL_COOLDOWN_MIN = int(os.getenv("GOLD_SIGNAL_COOLDOWN_MIN", "35"))  # Relaxed from 240m to 35m
GEMINI_MODEL        = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite").strip()
CLAUDE_BACKUP_MODEL = os.getenv("CLAUDE_BACKUP_MODEL", "claude-haiku-4-5-20251001").strip()

_gold_lock = threading.Lock()
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


class GoldState:
    PREFIX = "GOLD:"

    def __init__(self, shared_store=None):
        self.store = shared_store if shared_store is not None else _global_store

    def set(self, key, value, ttl=86400):
        self.store.set(f"{self.PREFIX}{key}", value, ttl=ttl)

    def get(self, key):
        return self.store.get(f"{self.PREFIX}{key}")


# ==============================================================================
# KOYEB HTTP HEALTH CHECK SERVER
# ==============================================================================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Gold (XAUTUSD) Institutional Engine v9.0 Active\n")

    def log_message(self, format, *args):
        return


def start_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
        log.info(f"[GOLD] Health Check HTTP server listening on port {PORT} for UptimeRobot")
        server.serve_forever()
    except Exception as e:
        log.error(f"[GOLD] HTTP health server error: {type(e).__name__}")


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
                    log.warning("[GOLD] Gemini 429 rate limit hit. Pausing 8s before retry...")
                    time.sleep(8.0)
                    _last_gemini_call = time.time()
                    r = requests.post(url, json=payload, timeout=20)
                # 500/503 Server Overload Retry
                elif r.status_code in (500, 503):
                    log.warning(f"[GOLD] Gemini HTTP {r.status_code} on key #{_gemini_idx}. Pausing 2.5s and retrying...")
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
                log.warning(f"[GOLD] Gemini request error: {type(e).__name__}")

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
            log.error(f"[GOLD] Claude backup error: {type(e).__name__}")

    return None, "AI_UNREACHABLE"


# ==============================================================================
# DELTA EXCHANGE PUBLIC API CANDLES (WITH UNCLOSED CANDLE GUARD)
# ==============================================================================
def fetch_delta_candles(symbol: str, resolution: str, hours_back: int, closed_only: bool = True):
    end_ts = int(time.time())
    start_ts = end_ts - (hours_back * 3600)
    url = f"{DELTA_BASE_URL}/v2/history/candles"
    params = {"symbol": symbol, "resolution": resolution, "start": start_ts, "end": end_ts}
    res_sec = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}.get(resolution, 1800)

    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            res = r.json().get("result", [])
            if not res:
                return pd.DataFrame()
            df = pd.DataFrame(res)
            df["time"] = df["time"].astype(int)
            df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(IST)
            df = df.rename(columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
            df[["o", "h", "l", "c"]] = df[["o", "h", "l", "c"]].astype(float).round(2)
            df = df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)

            if closed_only and not df.empty:
                last_open_ts = int(df.iloc[-1]["time"])
                if end_ts < (last_open_ts + res_sec):
                    df = df.iloc[:-1].reset_index(drop=True)
            return df
    except Exception as e:
        log.error(f"[GOLD] Delta candle fetch error ({resolution}): {type(e).__name__}")
    return pd.DataFrame()


def tg_send(text):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info(f"[GOLD-TG-DRY]\n{text}\n")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"[GOLD] Telegram error: {type(e).__name__}")
        return False


# ==============================================================================
# PURE PRICE ACTION INDICATORS & STRUCTURE
# ==============================================================================
def compute_indicators(df: pd.DataFrame):
    """Zero-ATR Indicator engine: EMA50, 24h Rolling Extremes (96 bars of 15M), and Daily PDH/PDL."""
    df_new = df.copy()
    high, low, close = df_new["h"], df_new["l"], df_new["c"]

    df_new["ema50"] = close.ewm(span=50, adjust=False).mean()
    df_new["roll_h96"] = high.rolling(96).max()
    df_new["roll_l96"] = low.rolling(96).min()

    # Daily PDH / PDL (UTC Date aligned for Delta Exchange)
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


def build_gold_zones(state: GoldState, df_macro: pd.DataFrame, ltp: float):
    """Maintains active_zones in GoldState using pure structural S/R ($25 window, $1.50 buffer)."""
    zones = []
    if df_macro.empty or len(df_macro) < 20:
        return zones

    dynamic_window = 25.0  # Clean $25 window around LTP

    pdh = df_macro["pdh"].iloc[-1] if "pdh" in df_macro.columns else None
    pdl = df_macro["pdl"].iloc[-1] if "pdl" in df_macro.columns else None
    if pd.notna(pdh) and pd.notna(pdl):
        pdh, pdl = float(pdh), float(pdl)
        zones.append({"id": "GOLD_PDH", "type": "RESISTANCE", "pivot": pdh, "low": pdh - 1.5, "high": pdh + 1.5, "why": f"Previous Day High (${pdh:.2f})"})
        zones.append({"id": "GOLD_PDL", "type": "SUPPORT", "pivot": pdl, "low": pdl - 1.5, "high": pdl + 1.5, "why": f"Previous Day Low (${pdl:.2f})"})

    for i in range(3, len(df_macro) - 3):
        h_v, l_v = float(df_macro.loc[i, "h"]), float(df_macro.loc[i, "l"])
        if abs(h_v - ltp) <= dynamic_window and h_v >= df_macro.loc[i - 3:i + 3, "h"].max():
            zones.append({"id": f"GOLD_SWH_{h_v:.1f}", "type": "RESISTANCE", "pivot": h_v, "low": h_v - 1.2, "high": h_v + 1.2, "why": f"15M Swing High Liquidity (${h_v:.2f})"})
        if abs(l_v - ltp) <= dynamic_window and l_v <= df_macro.loc[i - 3:i + 3, "l"].min():
            zones.append({"id": f"GOLD_SWL_{l_v:.1f}", "type": "SUPPORT", "pivot": l_v, "low": l_v - 1.2, "high": l_v + 1.2, "why": f"15M Swing Low Liquidity (${l_v:.2f})"})

    zones.sort(key=lambda z: abs(z["pivot"] - ltp))
    dedup = []
    for z in zones:
        if not any(abs(z["pivot"] - ez["pivot"]) <= 2.5 for ez in dedup):
            dedup.append(z)
    state.set("active_zones", dedup[:8], ttl=1800)
    return dedup[:8]


# ==============================================================================
# TOP 7 PURE PRICE ACTION DETECTORS (ZERO ATR)
# ==============================================================================

# SETUP 1: 5M Volume Expansion Blast (Relative Momentum + Fixed Box <= $9)
def check_5m_volume_expansion(df_5m: pd.DataFrame, df_15m: pd.DataFrame, state: GoldState):
    if len(df_5m) < 20 or len(df_15m) < 10:
        return None
        
    i = len(df_5m) - 1
    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    v_curr = float(df_5m["v"].iloc[i])
    v_avg = df_5m["v"].iloc[i-14:i].mean()
    ema50_15m = float(df_15m["ema50"].iloc[-1])
    
    # 1. Relative Candle Body: Current Body >= 2x Avg of past 3 bodies
    curr_body = abs(c - o)
    prev_bodies = [abs(float(df_5m["c"].iloc[k]) - float(df_5m["o"].iloc[k])) for k in range(i-3, i)]
    avg_prev_body = max(0.50, sum(prev_bodies) / 3.0)
    is_momentum_body = (curr_body >= 2.0 * avg_prev_body)
    
    # 2. Fixed Compression Box: Past 6 bars <= $9.00 range
    box = df_5m.iloc[i-6:i]
    box_h, box_l = float(box["h"].max()), float(box["l"].min())
    box_range = box_h - box_l
    is_tight_box = (box_range <= 9.0)
    
    # 3. Volume Anomaly: >= 2.0x 14-period SMA
    is_vol_anomaly = (v_curr >= 2.0 * v_avg)
    
    if is_tight_box and is_vol_anomaly and is_momentum_body:
        # Bullish Breakout
        if c > box_h and c > ema50_15m:
            candle_risk = c - l
            # If overextended (> $7.50), route to Trend Pullback watch!
            if candle_risk > 7.50:
                state.set("pullback_watch", {
                    "action": "BUY_GOLD",
                    "level": box_h,
                    "breakout_high": h,
                    "expires": time.time() + 1800
                }, ttl=1800)
                log.info(f"[GOLD] 📢 Giant Breakout (${candle_risk:.2f} > $7.50). Watching for TREND PULLBACK near ${box_h:.2f}!")
                return None
            
            sl = round(min(l, box_l) - 0.50, 2)
            risk = max(1.50, round(c - sl, 2))
            return {
                "name": "5M_VOLUME_EXPANSION_BLAST",
                "action": "BUY_GOLD",
                "tier": "A++ Pure Momentum Breakout (Relative Body + Vol 2x)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c + 2.0 * risk, 2),
                "be": round(c + 1.0 * risk, 2),
                "rr": "1:2.0",
                "why": f"Fixed Box (${box_l:.2f}-${box_h:.2f}, rng ${box_range:.2f}) broke UP with body ${curr_body:.2f} ({curr_body/avg_prev_body:.1f}x) & {v_curr/v_avg:.1f}x Vol"
            }
            
        # Bearish Breakdown
        elif c < box_l and c < ema50_15m:
            candle_risk = h - c
            if candle_risk > 7.50:
                state.set("pullback_watch", {
                    "action": "SELL_GOLD",
                    "level": box_l,
                    "breakout_low": l,
                    "expires": time.time() + 1800
                }, ttl=1800)
                log.info(f"[GOLD] 📢 Giant Breakdown (${candle_risk:.2f} > $7.50). Watching for TREND PULLBACK near ${box_l:.2f}!")
                return None
                
            sl = round(max(h, box_h) + 0.50, 2)
            risk = max(1.50, round(sl - c, 2))
            return {
                "name": "5M_VOLUME_EXPANSION_BLAST",
                "action": "SELL_GOLD",
                "tier": "A++ Pure Momentum Breakout (Relative Body + Vol 2x)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c - 2.0 * risk, 2),
                "be": round(c - 1.0 * risk, 2),
                "rr": "1:2.0",
                "why": f"Fixed Box (${box_l:.2f}-${box_h:.2f}, rng ${box_range:.2f}) broke DOWN with body ${curr_body:.2f} ({curr_body/avg_prev_body:.1f}x) & {v_curr/v_avg:.1f}x Vol"
            }
            
    return None

# SETUP 2: TREND PULLBACK RETEST (Never Skip Overextended Moves!)
def check_trend_pullback(df_5m: pd.DataFrame, df_15m: pd.DataFrame, state: GoldState):
    if len(df_5m) < 10:
        return None
        
    pb_watch = state.get("pullback_watch")
    if not pb_watch:
        return None
        
    if time.time() > pb_watch.get("expires", 0):
        state.set("pullback_watch", None)
        return None
        
    i = len(df_5m) - 1
    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    prev_l = float(df_5m["l"].iloc[i-1])
    prev_h = float(df_5m["h"].iloc[i-1])
    level = float(pb_watch["level"])
    action = pb_watch["action"]
    
    # Bullish Pullback: Retest of breakout level / previous low holds and closes green
    if action == "BUY_GOLD":
        if l <= pb_watch.get("breakout_high", level + 3.0) and (l <= level + 3.0 or l <= prev_l + 0.50) and c > o and (c - l) >= 1.0:
            sl = round(min(l, prev_l) - 0.50, 2)
            risk = max(1.50, round(c - sl, 2))
            if risk <= 7.50:
                state.set("pullback_watch", None)  # Retest complete!
                return {
                    "name": "TREND_PULLBACK_RETEST",
                    "action": "BUY_GOLD",
                    "tier": "A++ Institutional Retest Entry (Tight SL)",
                    "entry": c,
                    "sl": sl,
                    "risk": risk,
                    "tp": round(c + 2.0 * risk, 2),
                    "be": round(c + 1.0 * risk, 2),
                    "rr": "1:2.0",
                    "why": f"Retest of breakout level ${level:.2f} held! Rejection green close at ${c:.2f} with tight ${risk:.2f} risk"
                }

    # Bearish Pullback: Retest of breakdown level / previous high holds and closes red
    elif action == "SELL_GOLD":
        if h >= pb_watch.get("breakout_low", level - 3.0) and (h >= level - 3.0 or h >= prev_h - 0.50) and c < o and (h - c) >= 1.0:
            sl = round(max(h, prev_h) + 0.50, 2)
            risk = max(1.50, round(sl - c, 2))
            if risk <= 7.50:
                state.set("pullback_watch", None)
                return {
                    "name": "TREND_PULLBACK_RETEST",
                    "action": "SELL_GOLD",
                    "tier": "A++ Institutional Retest Entry (Tight SL)",
                    "entry": c,
                    "sl": sl,
                    "risk": risk,
                    "tp": round(c - 2.0 * risk, 2),
                    "be": round(c - 1.0 * risk, 2),
                    "rr": "1:2.0",
                    "why": f"Retest of breakdown level ${level:.2f} rejected! Red continuation close at ${c:.2f} with tight ${risk:.2f} risk"
                }

    return None

# SETUP 3: PDH / PDL MOMENTUM BREAKOUT (Catches Clean Daily Level Expansions)
def check_pdh_pdl_breakout(df_5m: pd.DataFrame, df_15m: pd.DataFrame, state: GoldState):
    if len(df_5m) < 15:
        return None
        
    i = len(df_5m) - 1
    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    prev_c = float(df_5m["c"].iloc[i-1])
    pdh, pdl = df_5m["pdh"].iloc[i], df_5m["pdl"].iloc[i]
    if pd.isna(pdh) or pd.isna(pdl):
        return None
    pdh, pdl = float(pdh), float(pdl)
    
    v_curr = float(df_5m["v"].iloc[i])
    v_avg = df_5m["v"].iloc[i-14:i].mean()
    ema50_15m = float(df_15m["ema50"].iloc[-1])
    
    curr_body = abs(c - o)
    prev_bodies = [abs(float(df_5m["c"].iloc[k]) - float(df_5m["o"].iloc[k])) for k in range(i-3, i)]
    avg_prev_body = max(0.50, sum(prev_bodies) / 3.0)
    
    # PDH Breakout (BUY)
    if prev_c <= pdh and c > pdh and c > ema50_15m and curr_body >= 1.5 * avg_prev_body and v_curr >= 1.8 * v_avg:
        candle_risk = c - l
        if candle_risk > 7.50:
            state.set("pullback_watch", {"action": "BUY_GOLD", "level": pdh, "breakout_high": h, "expires": time.time() + 1800}, ttl=1800)
            log.info(f"[GOLD] 📢 Giant PDH Breakout (${candle_risk:.2f} > $7.50). Watching for TREND PULLBACK at PDH ${pdh:.2f}!")
            return None
            
        sl = round(max(l - 0.50, pdh - 1.00), 2)
        risk = max(1.50, round(c - sl, 2))
        return {
            "name": "PDH_MOMENTUM_BREAKOUT",
            "action": "BUY_GOLD",
            "tier": "A+ Daily High Expansion (Pure Momentum)",
            "entry": c,
            "sl": sl,
            "risk": risk,
            "tp": round(c + 2.0 * risk, 2),
            "be": round(c + 1.0 * risk, 2),
            "rr": "1:2.0",
            "why": f"Clean close above Previous Day High (${pdh:.2f}) with {v_curr/v_avg:.1f}x Vol & ${curr_body:.2f} body"
        }

    # PDL Breakdown (SELL)
    if prev_c >= pdl and c < pdl and c < ema50_15m and curr_body >= 1.5 * avg_prev_body and v_curr >= 1.8 * v_avg:
        candle_risk = h - c
        if candle_risk > 7.50:
            state.set("pullback_watch", {"action": "SELL_GOLD", "level": pdl, "breakout_low": l, "expires": time.time() + 1800}, ttl=1800)
            log.info(f"[GOLD] 📢 Giant PDL Breakdown (${candle_risk:.2f} > $7.50). Watching for TREND PULLBACK at PDL ${pdl:.2f}!")
            return None
            
        sl = round(min(h + 0.50, pdl + 1.00), 2)
        risk = max(1.50, round(sl - c, 2))
        return {
            "name": "PDL_MOMENTUM_BREAKOUT",
            "action": "SELL_GOLD",
            "tier": "A+ Daily Low Expansion (Pure Momentum)",
            "entry": c,
            "sl": sl,
            "risk": risk,
            "tp": round(c - 2.0 * risk, 2),
            "be": round(c - 1.0 * risk, 2),
            "rr": "1:2.0",
            "why": f"Clean close below Previous Day Low (${pdl:.2f}) with {v_curr/v_avg:.1f}x Vol & ${curr_body:.2f} body"
        }

    return None

# SETUP 4: S2 PDH / PDL Sweep (Trap Reversals — 30% Wick, No 1-per-day restriction)
def check_s2_pdh_pdl_sweep(df: pd.DataFrame, state: GoldState):
    if len(df) < 15:
        return None
    i = len(df) - 1
    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    pdh, pdl = df["pdh"].iloc[i], df["pdl"].iloc[i]

    if pd.isna(pdh) or pd.isna(pdl):
        return None
    pdh, pdl = float(pdh), float(pdl)

    c_range = max(h - l, 0.1)
    upper_wick = (h - max(o, c)) / c_range
    lower_wick = (min(o, c) - l) / c_range

    # PDH Sweep Reversal (Short)
    if h > pdh and c < pdh and upper_wick >= 0.30 and c <= o:
        sl = round(h + 0.50, 2)
        risk = max(1.50, round(sl - c, 2))
        if risk <= 7.50:
            return {
                "name": "S2_PDH_SWEEP",
                "action": "SELL_GOLD",
                "tier": "Tier-1 PDH Sweep Trap (30% Rejection Wick)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c - 2.0 * risk, 2),
                "be": round(c - 1.0 * risk, 2),
                "rr": "1:2.0",
                "why": f"Swept Previous Day High (${pdh:.2f}) with {int(upper_wick*100)}% rejection wick and closed RED"
            }

    # PDL Sweep Reversal (Long)
    if l < pdl and c > pdl and lower_wick >= 0.30 and c >= o:
        sl = round(l - 0.50, 2)
        risk = max(1.50, round(c - sl, 2))
        if risk <= 7.50:
            return {
                "name": "S2_PDL_SWEEP",
                "action": "BUY_GOLD",
                "tier": "Tier-1 PDL Sweep Trap (30% Absorption Wick)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c + 2.0 * risk, 2),
                "be": round(c + 1.0 * risk, 2),
                "rr": "1:2.0",
                "why": f"Swept Previous Day Low (${pdl:.2f}) with {int(lower_wick*100)}% absorption wick and closed GREEN"
            }

    return None

# SETUP 5: SESSION RANGE SWEEP (Judas Swing Reversal across Sessions)
def check_session_range_sweep(df: pd.DataFrame):
    if len(df) < 20:
        return None
    i = len(df) - 1
    bar_ts = df["ts"].iloc[i]
    today_date = bar_ts.date()

    # Lookback past session bars from today
    prior_session_candles = df[(df["ts"].dt.date == today_date) & (df["ts"] < bar_ts - pd.Timedelta(hours=1))]
    if len(prior_session_candles) < 6:
        return None

    sess_h = float(prior_session_candles["h"].max())
    sess_l = float(prior_session_candles["l"].min())

    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    c_range = max(h - l, 0.1)
    upper_wick = (h - max(o, c)) / c_range
    lower_wick = (min(o, c) - l) / c_range

    if h > sess_h and c < sess_h and upper_wick >= 0.30 and c <= o:
        sl = round(h + 0.50, 2)
        risk = max(1.50, round(sl - c, 2))
        if risk <= 7.50:
            return {
                "name": "SESSION_RANGE_SWEEP",
                "action": "SELL_GOLD",
                "tier": "A++ Session Range Sweep Reversal",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c - 2.0 * risk, 2),
                "be": round(c - 1.0 * risk, 2),
                "rr": "1:2.0",
                "why": f"Swept Prior Session High (${sess_h:.2f}) with {int(upper_wick*100)}% rejection wick & closed RED"
            }

    if l < sess_l and c > sess_l and lower_wick >= 0.30 and c >= o:
        sl = round(l - 0.50, 2)
        risk = max(1.50, round(c - sl, 2))
        if risk <= 7.50:
            return {
                "name": "SESSION_RANGE_SWEEP",
                "action": "BUY_GOLD",
                "tier": "A++ Session Range Sweep Reversal",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c + 2.0 * risk, 2),
                "be": round(c + 1.0 * risk, 2),
                "rr": "1:2.0",
                "why": f"Swept Prior Session Low (${sess_l:.2f}) with {int(lower_wick*100)}% absorption wick & closed GREEN"
            }

    return None

# SETUP 6: USER 3-SWING INDUCEMENT TRAP (SMC Structure — Relaxed Gaps, No Range% Deadzone)
def check_user_3swing_inducement(df: pd.DataFrame, sw_h, sw_l):
    if len(df) < 30:
        return None
    i = len(df) - 1
    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    c_range = max(h - l, 0.1)
    upper_wick = (h - max(o, c)) / c_range
    lower_wick = (min(o, c) - l) / c_range

    if upper_wick >= 0.30 and c <= o:
        recent_sh = [s for s in sw_h if s["idx"] <= i - 2]
        if len(recent_sh) >= 2:
            for idx_2 in range(len(recent_sh) - 1, max(-1, len(recent_sh) - 8), -1):
                s2 = recent_sh[idx_2]
                gap2 = i - s2["idx"]
                if not (3 <= gap2 <= 80) or not (h > s2["price"] and c < s2["price"]):
                    continue
                for idx_1 in range(idx_2 - 1, max(-1, idx_2 - 8), -1):
                    s1 = recent_sh[idx_1]
                    gap1 = s2["idx"] - s1["idx"]
                    if not (3 <= gap1 <= 80):
                        continue
                    if s2["price"] < s1["price"] and h < (s1["price"] + 2.0):
                        sl = round(h + 0.50, 2)
                        risk = max(1.50, round(sl - c, 2))
                        if risk <= 7.50:
                            return {
                                "name": "USER_3SWING_INDUCEMENT_TRAP",
                                "action": "SELL_GOLD",
                                "tier": "A+ Structural Inducement Trap (SMC)",
                                "entry": c,
                                "sl": sl,
                                "risk": risk,
                                "tp": round(c - 2.0 * risk, 2),
                                "be": round(c - 1.0 * risk, 2),
                                "rr": "1:2.0",
                                "why": f"Swept Inducement High (${s2['price']:.2f}) below Major High (${s1['price']:.2f}) with Red rejection close"
                            }

    if lower_wick >= 0.30 and c >= o:
        recent_sl = [s for s in sw_l if s["idx"] <= i - 2]
        if len(recent_sl) >= 2:
            for idx_2 in range(len(recent_sl) - 1, max(-1, len(recent_sl) - 8), -1):
                s2 = recent_sl[idx_2]
                gap2 = i - s2["idx"]
                if not (3 <= gap2 <= 80) or not (l < s2["price"] and c > s2["price"]):
                    continue
                for idx_1 in range(idx_2 - 1, max(-1, idx_2 - 8), -1):
                    s1 = recent_sl[idx_1]
                    gap1 = s2["idx"] - s1["idx"]
                    if not (3 <= gap1 <= 80):
                        continue
                    if s2["price"] > s1["price"] and l > (s1["price"] - 2.0):
                        sl = round(l - 0.50, 2)
                        risk = max(1.50, round(c - sl, 2))
                        if risk <= 7.50:
                            return {
                                "name": "USER_3SWING_INDUCEMENT_TRAP",
                                "action": "BUY_GOLD",
                                "tier": "A+ Structural Inducement Trap (SMC)",
                                "entry": c,
                                "sl": sl,
                                "risk": risk,
                                "tp": round(c + 2.0 * risk, 2),
                                "be": round(c + 1.0 * risk, 2),
                                "rr": "1:2.0",
                                "why": f"Swept Inducement Low (${s2['price']:.2f}) above Major Low (${s1['price']:.2f}) with Green absorption close"
                            }

    return None

# SETUP 7: S3 COMPRESSION BREAKOUT (Fixed Box <= $9.00 Breakout)
def check_s3_breakout_opt(df: pd.DataFrame, df_15m: pd.DataFrame):
    if len(df) < 15:
        return None
    i = len(df) - 1
    c = float(df["c"].iloc[i])
    prev_c = float(df["c"].iloc[i - 1])
    ema50 = float(df_15m["ema50"].iloc[-1])

    box = df.iloc[i - 6:i]
    box_h, box_l = float(box["h"].max()), float(box["l"].min())
    box_range = box_h - box_l

    if box_range > 9.00:
        return None

    if c > box_h and prev_c <= box_h and c > ema50:
        sl = round(box_l - 0.50, 2)
        risk = max(1.50, round(c - sl, 2))
        if risk <= 7.50:
            return {
                "name": "S3_BREAKOUT_OPT",
                "action": "BUY_GOLD",
                "tier": "A Trend Aligned Compression Breakout",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c + 2.0 * risk, 2),
                "be": round(c + 1.0 * risk, 2),
                "rr": "1:2.0",
                "why": f"6-Bar box (${box_l:.2f}-${box_h:.2f}, rng ${box_range:.2f}) broke UP above ${box_h:.2f} with trend"
            }

    if c < box_l and prev_c >= box_l and c < ema50:
        sl = round(box_h + 0.50, 2)
        risk = max(1.50, round(sl - c, 2))
        if risk <= 7.50:
            return {
                "name": "S3_BREAKOUT_OPT",
                "action": "SELL_GOLD",
                "tier": "A Trend Aligned Compression Breakdown",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "tp": round(c - 2.0 * risk, 2),
                "be": round(c - 1.0 * risk, 2),
                "rr": "1:2.0",
                "why": f"6-Bar box (${box_l:.2f}-${box_h:.2f}, rng ${box_range:.2f}) broke DOWN below ${box_l:.2f} with trend"
            }

    return None


# ==============================================================================
# PURE PRICE ACTION RISK AUDIT (NO ATR CHOKES, NO 3-BAR FOMO BLOCK)
# ==============================================================================
def run_deterministic_filters(df: pd.DataFrame, setup: dict):
    i = len(df) - 1
    c = float(df["c"].iloc[i])
    ema50 = float(df["ema50"].iloc[i])
    risk = float(setup["risk"])

    # Hard Cap: Max Risk Cap $8.50 (Prevents runaway bloated SL)
    if risk > 8.50:
        log.info(f"[GOLD] Blocked {setup['name']}: Bloated Risk (${risk:.2f} > $8.50 cap)")
        return False, "Bloated Risk"

    ema_note = f"Clear (50 EMA at ${ema50:.2f})"
    if setup["action"] == "BUY_GOLD" and c < ema50 and (ema50 - c) < risk:
        ema_note = f"⚠️ WALL AHEAD: 50 EMA at ${ema50:.2f} (${ema50 - c:.2f} headroom < ${risk:.2f} risk)"
    elif setup["action"] == "SELL_GOLD" and c > ema50 and (c - ema50) < risk:
        ema_note = f"⚠️ WALL BELOW: 50 EMA at ${ema50:.2f} (${c - ema50:.2f} headroom < ${risk:.2f} risk)"

    setup["ema_note"] = ema_note
    return True, ema_note


# ==============================================================================
# DIAGNOSTIC STARTUP CHECK & ROUTINE HEALTH AUDITS
# ==============================================================================
def run_startup_diagnostics(state: GoldState):
    log.info("[GOLD] 🔍 Running Startup Diagnostic Suite...")
    now = datetime.now(IST)

    t0 = time.time()
    df_test = fetch_delta_candles(GOLD_SYMBOL, "5m", hours_back=3, closed_only=False)
    delta_lat = int((time.time() - t0) * 1000)
    delta_status = "ONLINE" if not df_test.empty else "OFFLINE"
    ltp_test = float(df_test.iloc[-1]["c"]) if not df_test.empty else 0.0

    t0 = time.time()
    ai_test_res, ai_provider = call_ai_json('Respond strictly in JSON: {"status":"ok"}', "Diagnostic ping. Return status ok.")
    ai_lat = int((time.time() - t0) * 1000)
    ai_status = f"ONLINE ({ai_lat}ms)" if ai_test_res else "OFFLINE / UNREACHABLE"

    state.set("diag_test", True, ttl=60)
    state_status = "OK" if state.get("diag_test") else "ERROR"

    startup_msg = f"""🚀 <b>[GOLD PURE PRICE ACTION V9.0 ONLINE]</b>
📅 <b>Time:</b> <code>{now.strftime('%Y-%m-%d %H:%M:%S IST')}</code> | <b>Server:</b> <code>Koyeb</code>

🟢 <b>Delta Public API:</b> <code>{delta_status} (LTP: ${ltp_test:,.2f} | {delta_lat}ms)</code>
🟢 <b>AI Supervisor   :</b> <code>{ai_status} [{esc(ai_provider)}]</code>
🟢 <b>State Cache     :</b> <code>{state_status}</code>
🟢 <b>HTTP Health Check:</b> <code>RUNNING (Port {PORT} - UptimeRobot Ready)</code>

🎯 <b>Active Pure Price Action Setups (Zero ATR):</b>
- <code>5M_VOLUME_EXPANSION_BLAST</code> (Relative Body >= 2x + Box <= $9)
- <code>TREND_PULLBACK_RETEST</code> (Catches Retests after Giant Moves)
- <code>PDH_PDL_MOMENTUM_BREAKOUT</code> (Clean Daily Level Expansion)
- <code>S2_PDH_PDL_SWEEP</code> (30% Rejection Wick Trap)
- <code>SESSION_RANGE_SWEEP</code> (Session Judas Swing Reversals)
- <code>USER_3SWING_INDUCEMENT_TRAP</code> (SMC Inducement Sweep)
- <code>S3_BREAKOUT_OPT</code> (6-Bar Box Compression <= $9.00)

🛡️ <i>Fail-Closed AI Guard + Pullback Router Active (No Skipped Moves).</i>"""

    tg_send(startup_msg)
    log.info("[GOLD] ✅ Startup diagnostics complete and report pushed to Telegram.")


def check_daily_0915_health_report(state: GoldState, df_15m: pd.DataFrame):
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
- 5M Surveillance  : <code>ACTIVE (Pure Price Action Zero ATR)</code>
- Uptime Server    : <code>Port {PORT} (200 OK)</code>

🗺️ <b>Macro Context (15M):</b>
- GOLD LTP : <b>${ltp:,.2f}</b>
- 50 EMA   : <b>${ema50:,.2f}</b> ({'Bullish' if ltp > ema50 else 'Bearish'})
- PDH / PDL: <b>${pdh:,.2f} / ${pdl:,.2f}</b>

✅ <i>All 7 Pure Price Action setups active.</i>"""
        tg_send(daily_msg)


def check_6h_macro_reanalysis(state: GoldState, df_15m: pd.DataFrame):
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
        r_high = float(df_15m["roll_h96"].iloc[-1])
        r_low = float(df_15m["roll_l96"].iloc[-1])
        trend_bias = "BULLISH" if ltp > ema50 else "BEARISH"

        log.info(f"[GOLD] ═════════ [6-HOUR MACRO RE-ANALYSIS: {now.strftime('%H:%M IST')}] ═════════")
        log.info(f"[GOLD] LTP: ${ltp:.2f} | 50 EMA: ${ema50:.2f} | 24h Range: ${r_low:.2f} - ${r_high:.2f}")

        zones = build_gold_zones(state, df_15m, ltp)

        sys_p = (
            "You are the Lead Institutional Gold (XAUUSD) Trading Supervisor. "
            "Provide a concise 1-2 sentence macro structure assessment for Gold. "
            'Respond strictly in JSON: {"macro_bias": "BULLISH/BEARISH/NEUTRAL", "assessment": "1-2 sentences"}'
        )
        usr_p = (
            f"Asset: GOLD (XAUTUSD) | Time: {now.strftime('%Y-%m-%d %H:%M IST')} | Price: ${ltp:.2f}\n"
            f"50 EMA: ${ema50:.2f} (Trend: {trend_bias}) | 24h Range: ${r_low:.2f} - ${r_high:.2f}\n"
            f"Key Zones: {', '.join([z['why'] for z in zones[:3]])}"
        )
        ai_res, provider = call_ai_json(sys_p, usr_p, max_tokens=300)

        # Honest AI Fallback: Do not send fake broadcast if AI fails
        if not ai_res:
            log.warning(f"[GOLD] ⚠️ 6-Hour Macro AI failed ({provider}) — skipping Telegram broadcast to avoid misleading fallback.")
            return

        ai_text = ai_res.get("assessment", "Gold consolidating within key macro liquidity boundaries.")
        ai_bias = ai_res.get("macro_bias", trend_bias)
        state.set("macro_bias", ai_bias, ttl=21600)

        z_lines = [f"• <b>{esc(z['type'])}:</b> <code>${z['pivot']:,.2f}</code> ({esc(z['why'])})" for z in zones[:3]]

        msg = f"""🏛️ <b>[GOLD] 6-HOUR MACRO RE-ANALYSIS | {now.strftime('%H:%M IST')}</b>

📊 <b>LTP     :</b> <b>${ltp:,.2f}</b> | <b>Trend:</b> <b>{trend_bias}</b>
📐 <b>50 EMA :</b> <code>${ema50:.2f}</code>
📏 <b>24h Rng :</b> <code>${r_low:,.1f} — ${r_high:,.1f}</code>

🗺️ <b>Active 15M Macro Zones:</b>
{chr(10).join(z_lines)}

🧠 <b>AI Macro Audit ({esc(provider)}):</b> [{esc(ai_bias)}]
<i>{esc(ai_text)}</i>"""
        tg_send(msg)


# ==============================================================================
# MAIN CYCLE RUNNER (EVALUATES CLOSED 5M CANDLES MULTI-TF)
# ==============================================================================
def run_gold_cycle(shared_store=None):
    if not _gold_lock.acquire(blocking=False):
        log.warning("[GOLD] Execution lock busy, skipping cycle")
        return

    try:
        state = GoldState(shared_store)
        state.set("last_tick", time.time(), ttl=86400)

        # Multi-TF Data Sync: 15M (Macro/Zones) + 5M (Execution)
        df_15m = fetch_delta_candles(GOLD_SYMBOL, "15m", hours_back=60, closed_only=True)
        df_5m = fetch_delta_candles(GOLD_SYMBOL, "5m", hours_back=24, closed_only=True)
        
        if df_5m.empty or len(df_5m) < 35 or df_15m.empty:
            log.warning("[GOLD] Insufficient candle data from Delta Exchange")
            return

        df_15m = compute_indicators(df_15m)
        df_5m = compute_indicators(df_5m)
        
        ltp = float(df_5m["c"].iloc[-1])
        last_bar_time = int(df_5m.iloc[-1]["time"])

        check_6h_macro_reanalysis(state, df_15m)
        check_daily_0915_health_report(state, df_15m)

        # Refresh active zones in state based on 15M structure
        zones = build_gold_zones(state, df_15m, ltp)

        # Ensure we only evaluate each closed 5M candle once
        if state.get("last_evaluated_5m_ts") == last_bar_time:
            return
        state.set("last_evaluated_5m_ts", last_bar_time, ttl=300)

        closed_ts_str = df_5m["ts"].iloc[-1].strftime("%H:%M IST")
        ema50 = float(df_15m["ema50"].iloc[-1])
        trend_label = "BULLISH" if ltp > ema50 else "BEARISH"
        nearest_str = f"{zones[0]['id']}: ${zones[0]['pivot']:.2f}" if zones else "None"

        log.info(f"[GOLD] ═════════ 5M EXECUTION AUDIT: {closed_ts_str} ═════════")
        log.info(f"[GOLD] Close: ${ltp:.2f} | 15M Trend: {trend_label} (50 EMA ${ema50:.2f})")
        log.info(f"[GOLD] Nearest 15M Zone: {nearest_str} | AI Calls Today: {_daily_ai_call_count}/500")

        sw_h, sw_l = find_swings(df_5m, w=3)

        # Evaluate Top 7 Pure Price Action Setups (Pullback Retest checks FIRST!)
        setup = (
            check_trend_pullback(df_5m, df_15m, state)
            or check_5m_volume_expansion(df_5m, df_15m, state)
            or check_pdh_pdl_breakout(df_5m, df_15m, state)
            or check_s2_pdh_pdl_sweep(df_5m, state)
            or check_session_range_sweep(df_5m)
            or check_user_3swing_inducement(df_5m, sw_h, sw_l)
            or check_s3_breakout_opt(df_5m, df_15m)
        )

        if not setup:
            log.info("[GOLD] Setups Evaluated: PB_Retest=NO | Vol_Blast=NO | PDH_BO=NO | S2_Sweep=NO | Sess_Sweep=NO | 3Swing_Trap=NO | S3_Opt=NO -> WAITING")
            return

        passed, filter_msg = run_deterministic_filters(df_5m, setup)
        if not passed:
            log.warning(f"[GOLD] Setup {setup['name']} rejected by deterministic check: {filter_msg}")
            return

        cd_key = f"cd_{setup['name']}_{setup['action']}"
        cd_until = state.get(cd_key)
        if cd_until and time.time() < cd_until:
            rem_m = int((cd_until - time.time()) / 60)
            log.info(f"[GOLD] Cooldown active for {setup['name']} ({rem_m}m remaining). Skipped.")
            return

        log.info(f"[GOLD] 🧠 Calling AI Supervisor for {setup['name']} ({setup['action']})...")
        sys_p = (
            "You are the Lead Institutional Gold (XAUUSD) Trading Supervisor. "
            "Validate Python's detected setup against Master Rules, EMA50 headroom wall, and candle quality. "
            "Respond ONLY in strictly valid JSON: "
            '{"approved": true/false, "confidence": "HIGH/MEDIUM/LOW", "view": "1 concise sentence"}'
        )
        usr_p = (
            f"Asset: GOLD (XAUTUSD) | Closed Bar Time: {closed_ts_str} | Close: ${ltp:.2f}\n"
            f"Detected Setup: {setup['name']} ({setup['tier']})\n"
            f"Action: {setup['action']} | Entry: ${setup['entry']:.2f} | SL: ${setup['sl']:.2f} | TP: ${setup['tp']:.2f} ({setup['rr']} R:R)\n"
            f"Trigger Logic: {setup['why']}\n"
            f"EMA50 Check: {setup['ema_note']}"
        )

        ai_res, provider = call_ai_json(sys_p, usr_p)

        if not ai_res or not ai_res.get("approved", False):
            reason = ai_res.get("view", "AI Disapproved") if ai_res else f"AI Offline/503 Failed ({provider})"
            log.warning(f"[GOLD] 🛑 Trade Blocked by Fail-Closed Safety Guard: {reason}")
            state.set(cd_key, time.time() + WAIT_COOLDOWN_MIN * 60, ttl=WAIT_COOLDOWN_MIN * 60)
            return

        # Cooldown applied (relaxed 35 min)
        state.set(cd_key, time.time() + (SIGNAL_COOLDOWN_MIN * 60), ttl=SIGNAL_COOLDOWN_MIN * 60)

        em = "🟢" if "BUY" in setup["action"] else "🔴"
        action_title = "BUY (LONG)" if "BUY" in setup["action"] else "SELL (SHORT)"
        ai_view = ai_res.get("view", "Institutional Gold order flow alignment confirmed.")
        ai_conf = ai_res.get("confidence", "HIGH")

        msg = f"""{em} <b>[GOLD - XAUTUSD] {action_title} | {datetime.now(IST).strftime('%H:%M IST')}</b>

⚡ <b>Setup    :</b> <code>{esc(setup['name'])}</code>
🏆 <b>Tier     :</b> {esc(setup['tier'])}
📊 <b>Risk:Rew :</b> <b>{setup['rr']} Target</b> (Risk: ${setup['risk']:.2f})

🎯 <b>Entry    :</b> <b>${setup['entry']:.2f}</b>
🛑 <b>StopLoss :</b> <b>${setup['sl']:.2f}</b>
✅ <b>1:1 BE   :</b> <b>${setup['be']:.2f}</b> (Move SL to Cost)
🚀 <b>Target   :</b> <b>${setup['tp']:.2f}</b>

📐 <b>EMA50    :</b> {esc(setup['ema_note'])}
📝 <b>Logic    :</b> {esc(setup['why'])}
🧠 <b>AI ({esc(provider)}):</b> [{esc(ai_conf)}] <i>{esc(ai_view)}</i>"""

        tg_send(msg)
        log.info(f"[GOLD] 🔥 FIRED {setup['name']} ({setup['action']}) at ${setup['entry']:.2f} | AI: {ai_conf}")

    except Exception as e:
        log.exception(f"[GOLD] Error in run_gold_cycle: {type(e).__name__}")
    finally:
        try:
            _gold_lock.release()
        except RuntimeError:
            pass


def run_6h_analysis(shared_store=None):
    """
    6-Hour Macro Re-analysis for Gold (XAUTUSD).
    Called by main.py at 05:32, 11:32, 17:32, 23:32 IST (UTC-aligned closes).
    """
    if not _gold_lock.acquire(blocking=False):
        log.warning("[GOLD] 6H Macro analysis skipped: Execution lock busy")
        return

    try:
        state = GoldState(shared_store)
        now = datetime.now(IST)

        df_15m = fetch_delta_candles(GOLD_SYMBOL, "15m", hours_back=120, closed_only=True)
        if df_15m.empty or len(df_15m) < 35:
            log.warning("[GOLD] 6H Macro skipped: Insufficient 15M candles from Delta")
            return

        df_15m = compute_indicators(df_15m)
        ltp = float(df_15m["c"].iloc[-1])
        ema50 = float(df_15m["ema50"].iloc[-1])
        r_high = float(df_15m["roll_h96"].iloc[-1])
        r_low = float(df_15m["roll_l96"].iloc[-1])

        zones = build_gold_zones(state, df_15m, ltp)
        trend_bias = "BULLISH" if ltp > ema50 else "BEARISH"

        log.info(f"[GOLD] ═════════ 6-HOUR MACRO RE-ANALYSIS ({now.strftime('%H:%M IST')}) ═════════")
        log.info(f"[GOLD] LTP: ${ltp:.2f} | 50 EMA: ${ema50:.2f} | Trend: {trend_bias}")
        log.info(f"[GOLD] Active Zones ({len(zones)}): {', '.join([z['id'] for z in zones[:4]])}")

        sys_p = (
            "You are the Lead Institutional Gold (XAUUSD) Trading Supervisor. "
            "Provide a concise 1-2 sentence macro structure assessment for Gold. "
            'Respond strictly in JSON: {"macro_bias": "BULLISH/BEARISH/NEUTRAL", "assessment": "1-2 sentences"}'
        )
        usr_p = (
            f"Asset: GOLD (XAUTUSD) | Time: {now.strftime('%Y-%m-%d %H:%M IST')} | Price: ${ltp:.2f}\n"
            f"50 EMA: ${ema50:.2f} (Trend: {trend_bias}) | 24h Range: ${r_low:.2f} - ${r_high:.2f}\n"
            f"Key Zones: {', '.join([z['why'] for z in zones[:3]])}"
        )

        ai_res, provider = call_ai_json(sys_p, usr_p, max_tokens=300)
        
        # Honest AI Fallback
        if not ai_res:
            log.warning(f"[GOLD] ⚠️ 6H Macro AI failed ({provider}) — skipping Telegram broadcast to avoid misleading fallback.")
            return

        ai_text = ai_res.get("assessment", "Gold consolidating within key macro liquidity boundaries.")
        ai_bias = ai_res.get("macro_bias", trend_bias)
        state.set("macro_bias", ai_bias, ttl=21600)

        z_lines = [f"• <b>{esc(z['type'])}:</b> <code>${z['pivot']:,.2f}</code> ({esc(z['why'])})" for z in zones[:3]]

        msg = f"""🏛️ <b>[GOLD] 6-HOUR MACRO RE-ANALYSIS | {now.strftime('%H:%M IST')}</b>

📊 <b>LTP     :</b> <b>${ltp:,.2f}</b> | <b>Trend:</b> <b>{trend_bias}</b>
📐 <b>50 EMA :</b> <code>${ema50:.2f}</code>
📏 <b>24h Rng :</b> <code>${r_low:,.1f} — ${r_high:,.1f}</code>

🗺️ <b>Active 15M Macro Zones:</b>
{chr(10).join(z_lines)}

🧠 <b>AI Macro Audit ({esc(provider)}):</b> [{esc(ai_bias)}]
<i>{esc(ai_text)}</i>"""

        tg_send(msg)
        log.info(f"[GOLD] 🏛️ 6-Hour Macro Re-analysis published to Telegram.")

    except Exception as e:
        log.exception(f"[GOLD] Error in run_6h_analysis: {type(e).__name__}")
    finally:
        try:
            _gold_lock.release()
        except RuntimeError:
            pass


def run_gold_30m_tick(shared_store=None):
    run_gold_cycle(shared_store)

def run_gold_1m_tick(shared_store=None):
    run_gold_cycle(shared_store)


if __name__ == "__main__":
    log.info("[GOLD] Starting Gold Institutional Engine v9.0 on Koyeb...")
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    state_init = GoldState()
    run_startup_diagnostics(state_init)

    log.info("[GOLD] Starting 30-Second Continuous Polling Loop...")
    while True:
        try:
            run_gold_cycle()
        except Exception as e:
            log.error(f"[GOLD] Loop error: {type(e).__name__}")
        time.sleep(30)
