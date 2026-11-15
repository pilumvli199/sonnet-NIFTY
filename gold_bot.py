"""
╔══════════════════════════════════════════════════════════════════════════╗
║   GOLD (XAUTUSD) INSTITUTIONAL LIQUIDITY ENGINE — V9.5 HYBRID MTF        ║
║   Namespace: GOLD:* | Logger: [GOLD] | Delta Exchange Public API         ║
║   Timeframe: 1D + 4H + 1H Liquidity Mapping -> 5M Sniper Execution       ║
║                                                                          ║
║   V9.5 DATA-PROVEN UPGRADES (60,530 Candles / 6-Month Backtest):         ║
║   1. BIG_SR_SWEEP [1D / 4H / 1H] (Fresh Unswept HTF Liquidity Hunts)     ║
║   2. BIG_SR_MSS_SWEEP (HTF Zone Touch + 5M EMA20/50 Shift + Sweep)       ║
║   3. TREND_PULLBACK_RETEST (All Volume/PDH/PDL Blasts Routed Here)       ║
║   4. SESSION_RANGE_SWEEP (Wick >= 35%, Risk <= $5.50, Spent Lock)        ║
║   5. S2_PDH_SWEEP (Wick >= 35%, Risk <= $5.50, Spent PDH Lock)           ║
║   6. USER_3SWING_INDUCEMENT_TRAP (4H Aligned, <=60m Fresh, Wick >= 38%)  ║
║                                                                          ║
║   REMOVED NOISE SETUPS:                                                  ║
║   • S3_BREAKOUT_OPT Removed (-278R 6M loss / chop fakeouts eliminated)   ║
║   • S2_PDL_SWEEP Removed (counter-trend bottom trap loss eliminated)     ║
║   • 1:1 Early BE Cut Removed (Entry-only alerts; manual SL/Target)       ║
║                                                                          ║
║   MULTI-TIER AI CASCADE & BACKGROUND THREADS:                            ║
║   • Tier-1 Macro: gemini-4-hydrogen (HIGH) -> gemini-3.8-flash (HIGH)    ║
║                   -> gemini-3.6-flash (OFF) -> gemini-3.5-flash-lite(HIGH)║
║   • Background Hydrogen Confirmation Thread if Lite fallback is used     ║
║   • 23:30 IST Daily 1D+4H+1H Background Thread (2-Min Auto-Retry)        ║
║   • Tier-2 5M Gatekeeper: gemini-3.5-flash-lite (OFF)                    ║
║                           -> gemini-3.1-flash-lite (OFF) -> Claude       ║
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
WAIT_COOLDOWN_MIN   = int(os.getenv("GOLD_WAIT_COOLDOWN_MIN", "15"))
SIGNAL_COOLDOWN_MIN = int(os.getenv("GOLD_SIGNAL_COOLDOWN_MIN", "35"))
SEND_AI_WAIT_TO_TG  = os.getenv("GOLD_SEND_AI_WAIT_TO_TG", "true").lower() == "true"

# AI Model Chains (Benchmark-Verified Thinking Levels & Generous Timeouts)
MACRO_MODEL_CHAIN = [
    {"model": os.getenv("GOLD_MACRO_PRIMARY", "gemini-4-hydrogen").strip(),   "thinking": "HIGH", "timeout": 75},
    {"model": os.getenv("GOLD_MACRO_FB1", "gemini-3.8-flash").strip(),        "thinking": "HIGH", "timeout": 60},
    {"model": os.getenv("GOLD_MACRO_FB2", "gemini-3.6-flash").strip(),        "thinking": "OFF",  "timeout": 45},
    {"model": os.getenv("GOLD_MACRO_FB3", "gemini-3.5-flash-lite").strip(),   "thinking": "HIGH", "timeout": 35},
]

FAST_5M_MODEL_CHAIN = [
    {"model": os.getenv("GOLD_5M_PRIMARY", "gemini-3.5-flash-lite").strip(),  "thinking": "OFF",  "timeout": 25},
    {"model": os.getenv("GOLD_5M_FB1", "gemini-3.1-flash-lite").strip(),      "thinking": "OFF",  "timeout": 25},
]

HYDROGEN_MODEL      = os.getenv("GOLD_HYDROGEN_MODEL", "gemini-4-hydrogen").strip()
CLAUDE_BACKUP_MODEL = os.getenv("CLAUDE_BACKUP_MODEL", "claude-haiku-5-5").strip()

# V9.5 Data-Tuned Pure Price Action Constants
SL_BUFFER_USD       = 0.85   # Tuned from $0.50 -> prevents Gold wick stop-outs
SWEEP_RISK_CAP_USD  = 5.50   # Tuned cap for Session / PDH / 3-Swing SMC sweeps
HTF_RISK_CAP_USD    = 8.00   # Max structural invalidation distance for 1D/4H/1H sweeps
PB_RISK_CAP_USD     = 7.50   # Max structural invalidation distance for Pullback Retests
MIN_RISK_USD        = 1.50   # Minimum structural invalidation distance
BOX_MAX_USD         = 9.00   # 6-bar compression box max range for Volume Expansion

_gold_lock = threading.Lock()
_ai_call_lock = threading.Lock()
_gemini_idx = 0
_last_gemini_call = 0.0
_daily_ai_call_count = 0
_daily_ai_count_date = None
_last_daily_report_date = None
_hydrogen_bg_active = False
_scheduler_started = False


def esc(text):
    return html.escape(str(text), quote=False) if text is not None else ""


def _bump_ai_counter():
    global _daily_ai_call_count, _daily_ai_count_date
    today = datetime.now(IST).strftime("%Y-%m-%d")
    if _daily_ai_count_date != today:
        _daily_ai_count_date = today
        _daily_ai_call_count = 0
    _daily_ai_call_count += 1


# ==============================================================================
# IN-MEMORY STATE STORE (WITH REDIS / SHARED_STORE COMPATIBILITY)
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
        self.wfile.write(b"OK - Gold (XAUTUSD) Institutional Liquidity Engine v9.5 Active\n")

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
# MULTI-TIER AI SUPERVISOR (CASCADE FALLBACK + THINKING LEVEL CONTROL)
# ==============================================================================
def _build_gemini_payload(system_prompt, user_prompt, max_tokens, thinking_mode, include_thinking_cfg=True):
    gen_cfg = {
        "temperature": 0.0,
        "maxOutputTokens": max_tokens,
        "responseMimeType": "application/json",
    }
    if include_thinking_cfg:
        if thinking_mode == "HIGH":
            gen_cfg["thinkingConfig"] = {"thinkingLevel": "HIGH"}
        elif thinking_mode == "OFF":
            gen_cfg["thinkingConfig"] = {"thinkingBudget": 0}
    return {
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
        "generationConfig": gen_cfg,
    }


def _call_single_gemini_model(model_name, thinking_mode, timeout_sec, system_prompt, user_prompt, max_tokens):
    global _gemini_idx, _last_gemini_call
    if not GEMINI_API_KEYS:
        return None

    num_keys = len(GEMINI_API_KEYS)
    for attempt in range(num_keys):
        with _ai_call_lock:
            now = time.time()
            elapsed = now - _last_gemini_call
            if elapsed < 2.0:
                time.sleep(2.0 - elapsed)
            _last_gemini_call = time.time()
            api_key = GEMINI_API_KEYS[_gemini_idx % num_keys]
            _gemini_idx += 1
            _bump_ai_counter()

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        payload = _build_gemini_payload(system_prompt, user_prompt, max_tokens, thinking_mode, include_thinking_cfg=True)

        try:
            r = requests.post(url, json=payload, timeout=timeout_sec)

            # Schema compatibility guard: if API rejects thinkingConfig on a specific model, retry without it
            if r.status_code == 400 and "thinking" in r.text.lower():
                log.warning(f"[GOLD] {model_name} rejected thinkingConfig (400). Retrying with default config...")
                payload_fallback = _build_gemini_payload(system_prompt, user_prompt, max_tokens, thinking_mode, include_thinking_cfg=False)
                r = requests.post(url, json=payload_fallback, timeout=timeout_sec)

            if r.status_code == 200:
                candidates = r.json().get("candidates", [])
                if candidates:
                    parts = candidates[0].get("content", {}).get("parts", [])
                    raw = "".join(p.get("text", "") for p in parts if "text" in p)
                    clean = raw.strip().replace("```json", "").replace("```", "").strip()
                    s, e = clean.find("{"), clean.rfind("}") + 1
                    if s != -1 and e > s:
                        return json.loads(clean[s:e])
            elif r.status_code in (429, 500, 502, 503, 504):
                log.warning(f"[GOLD] {model_name} HTTP {r.status_code} (server busy/rate-limited) -> cascading to next model...")
                if attempt < num_keys - 1:
                    time.sleep(1.5)
                    continue
                return None
            else:
                log.warning(f"[GOLD] {model_name} HTTP {r.status_code} -> cascading...")
                return None
        except requests.Timeout:
            log.warning(f"[GOLD] {model_name} timed out after {timeout_sec}s -> cascading to next model...")
            return None
        except Exception as e:
            log.warning(f"[GOLD] {model_name} error ({type(e).__name__}) -> cascading...")
            return None

    return None


def call_ai_json(system_prompt, user_prompt, max_tokens=700, mode="fast_5m"):
    """
    Executes the benchmark-verified model fallback chain:
      - mode == 'macro': gemini-4-hydrogen (HIGH) -> gemini-3.8-flash (HIGH) -> gemini-3.6-flash (OFF) -> gemini-3.5-flash-lite (HIGH)
      - mode == 'fast_5m': gemini-3.5-flash-lite (OFF) -> gemini-3.1-flash-lite (OFF)
      - mode == 'hydrogen_only': gemini-4-hydrogen (HIGH) only (used by background confirmation worker)
    """
    if mode == "hydrogen_only":
        chain = [{"model": HYDROGEN_MODEL, "thinking": "HIGH", "timeout": 85}]
    elif mode == "macro":
        chain = MACRO_MODEL_CHAIN
    else:
        chain = FAST_5M_MODEL_CHAIN

    for step in chain:
        m_name = step["model"]
        t_mode = step["thinking"]
        t_out = step["timeout"]
        res = _call_single_gemini_model(m_name, t_mode, t_out, system_prompt, user_prompt, max_tokens)
        if res is not None:
            tag = f"GEMINI ({m_name} | {t_mode})"
            return res, tag

    # Emergency Claude Backup (only if GEMINI chain fails completely)
    if ANTHROPIC_KEY and mode != "hydrogen_only":
        hdrs = {
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        body = {
            "model": CLAUDE_BACKUP_MODEL,
            "max_tokens": max_tokens,
            "temperature": 0,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        try:
            with _ai_call_lock:
                _bump_ai_counter()
            r = requests.post("https://api.anthropic.com/v1/messages", headers=hdrs, json=body, timeout=40)
            if r.status_code == 200:
                raw = r.json()["content"][0]["text"]
                clean = raw.strip().replace("```json", "").replace("```", "").strip()
                s, e = clean.find("{"), clean.rfind("}") + 1
                if s != -1 and e > s:
                    return json.loads(clean[s:e]), f"CLAUDE_BACKUP ({CLAUDE_BACKUP_MODEL})"
        except Exception as e:
            log.error(f"[GOLD] Claude backup error: {type(e).__name__}")

    return None, "AI_UNREACHABLE"


# ==============================================================================
# DELTA EXCHANGE PUBLIC API CANDLES (1M, 5M, 15M, 1H, 4H, 1D + UNCLOSED GUARD)
# ==============================================================================
def fetch_delta_candles(symbol: str, resolution: str, hours_back: int, closed_only: bool = True):
    end_ts = int(time.time())
    start_ts = end_ts - (hours_back * 3600)
    url = f"{DELTA_BASE_URL}/v2/history/candles"
    params = {"symbol": symbol, "resolution": resolution, "start": start_ts, "end": end_ts}
    res_sec = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }.get(resolution, 1800)

    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            res = r.json().get("result", [])
            if not res:
                return pd.DataFrame()
            df = pd.DataFrame(res)
            df["time"] = df["time"].astype(int)
            df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(IST)
            df = df.rename(columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
            df[["o", "h", "l", "c"]] = df[["o", "h", "l", "c"]].astype(float).round(2)
            if "v" in df.columns:
                df["v"] = df["v"].astype(float)
            else:
                df["v"] = 0.0
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
            timeout=12,
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"[GOLD] Telegram error: {type(e).__name__}")
        return False


# ==============================================================================
# PURE PRICE ACTION INDICATORS, SWINGS & MULTI-TF LIQUIDITY POOL ENGINE
# ==============================================================================
def compute_indicators(df: pd.DataFrame):
    """Zero-ATR Indicator engine: EMA20, EMA50, 24h Rolling Extremes, and UTC Daily PDH/PDL."""
    df_new = df.copy()
    high, low, close = df_new["h"], df_new["l"], df_new["c"]

    df_new["ema20"] = close.ewm(span=20, adjust=False).mean()
    df_new["ema50"] = close.ewm(span=50, adjust=False).mean()
    df_new["roll_h96"] = high.rolling(96).max()
    df_new["roll_l96"] = low.rolling(96).min()

    df_new["utc_date"] = df_new["ts"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d")
    daily = df_new.groupby("utc_date").agg({"h": "max", "l": "min"}).shift(1)
    daily.columns = ["pdh", "pdl"]
    daily = daily.reset_index()
    df_new = df_new.merge(daily, on="utc_date", how="left")
    return df_new


def find_swings(df: pd.DataFrame, w=3):
    sh, sl = [], []
    if len(df) < (2 * w + 1):
        return sh, sl
    for idx in range(w, len(df) - w):
        h_val, l_val = float(df["h"].iloc[idx]), float(df["l"].iloc[idx])
        if all(h_val >= float(df["h"].iloc[idx - k]) for k in range(1, w + 1)) and all(h_val >= float(df["h"].iloc[idx + k]) for k in range(1, w + 1)):
            sh.append({"idx": idx, "price": h_val, "ts": df["ts"].iloc[idx]})
        if all(l_val <= float(df["l"].iloc[idx - k]) for k in range(1, w + 1)) and all(l_val <= float(df["l"].iloc[idx + k]) for k in range(1, w + 1)):
            sl.append({"idx": idx, "price": l_val, "ts": df["ts"].iloc[idx]})
    return sh, sl


def detect_liquidity_pools(df: pd.DataFrame, tolerance_usd: float = 1.50):
    """Counts Equal Highs (BSL magnet) and Equal Lows (SSL magnet) over recent structure."""
    if df.empty or len(df) < 15:
        return {"bsl_eqh_count": 0, "ssl_eql_count": 0, "dominant_pool": "BALANCED", "summary": "Balanced liquidity"}

    tail = df.tail(48)
    highs = tail["h"].astype(float).tolist()
    lows = tail["l"].astype(float).tolist()

    eqh_clusters = 0
    eql_clusters = 0
    for i in range(len(highs)):
        for j in range(i + 3, len(highs)):
            if abs(highs[i] - highs[j]) <= tolerance_usd:
                eqh_clusters += 1
            if abs(lows[i] - lows[j]) <= tolerance_usd:
                eql_clusters += 1

    if eqh_clusters > eql_clusters + 1:
        dom = "BUY_SIDE_LIQUIDITY_HEAVY (EQH Magnet Above)"
    elif eql_clusters > eqh_clusters + 1:
        dom = "SELL_SIDE_LIQUIDITY_HEAVY (EQL Magnet Below)"
    else:
        dom = "BALANCED_LIQUIDITY"

    return {
        "bsl_eqh_count": eqh_clusters,
        "ssl_eql_count": eql_clusters,
        "dominant_pool": dom,
        "summary": f"{dom} [EQH={eqh_clusters}, EQL={eql_clusters}]",
    }


def _extract_fresh_unswept_zones(df_tf: pd.DataFrame, tf_tag: str, w: int, ltp: float, window_usd: float):
    """Extracts fractal swings on 1D/4H/1H that have NOT been cleanly broken/swept since formation."""
    zones = []
    if df_tf.empty or len(df_tf) < (2 * w + 2):
        return zones

    highs = df_tf["h"].astype(float).values
    lows = df_tf["l"].astype(float).values
    closes = df_tf["c"].astype(float).values
    n = len(df_tf)

    for k in range(w, n - w):
        h_v = highs[k]
        l_v = lows[k]

        if abs(h_v - ltp) <= window_usd and h_v >= max(highs[k - w:k]) and h_v >= max(highs[k + 1:k + w + 1]):
            subsequent_highs = highs[k + w + 1:]
            subsequent_closes = closes[k + w + 1:]
            if len(subsequent_highs) == 0 or (max(subsequent_highs) <= h_v and max(subsequent_closes) <= h_v):
                zones.append({
                    "id": f"{tf_tag}_BSL_{h_v:.1f}",
                    "tf": tf_tag,
                    "type": "RESISTANCE",
                    "side": "BSL",
                    "pivot": round(h_v, 2),
                    "why": f"[{tf_tag}_ZONE] Fresh Unswept High (${h_v:.2f})",
                })

        if abs(l_v - ltp) <= window_usd and l_v <= min(lows[k - w:k]) and l_v <= min(lows[k + 1:k + w + 1]):
            subsequent_lows = lows[k + w + 1:]
            subsequent_closes = closes[k + w + 1:]
            if len(subsequent_lows) == 0 or (min(subsequent_lows) >= l_v and min(subsequent_closes) >= l_v):
                zones.append({
                    "id": f"{tf_tag}_SSL_{l_v:.1f}",
                    "tf": tf_tag,
                    "type": "SUPPORT",
                    "side": "SSL",
                    "pivot": round(l_v, 2),
                    "why": f"[{tf_tag}_ZONE] Fresh Unswept Low (${l_v:.2f})",
                })

    return zones


def build_mtf_liquidity_zones(state: GoldState, ltp: float, df_1d=None, df_4h=None, df_1h=None):
    """
    Builds and deduplicates 3-Tier Multi-Timeframe Unswept Liquidity Zones:
      Priority Hierarchy: [1D_ZONE] > [4H_ZONE] > [1H_ZONE]
    Preserves previously cached 1D zones if only 4H/1H are refreshed during intraday syncs.
    """
    existing_zones = state.get("mtf_liquidity_zones") or []
    spent_ids = set(state.get("spent_zone_ids") or [])

    zones_1d = _extract_fresh_unswept_zones(df_1d, "1D", w=2, ltp=ltp, window_usd=120.0) if (df_1d is not None and not df_1d.empty) else [z for z in existing_zones if z.get("tf") == "1D"]
    zones_4h = _extract_fresh_unswept_zones(df_4h, "4H", w=2, ltp=ltp, window_usd=65.0)  if (df_4h is not None and not df_4h.empty) else [z for z in existing_zones if z.get("tf") == "4H"]
    zones_1h = _extract_fresh_unswept_zones(df_1h, "1H", w=3, ltp=ltp, window_usd=35.0)  if (df_1h is not None and not df_1h.empty) else [z for z in existing_zones if z.get("tf") == "1H"]

    tf_rank = {"1D": 0, "4H": 1, "1H": 2}
    combined = [z for z in (zones_1d + zones_4h + zones_1h) if z["id"] not in spent_ids]
    combined.sort(key=lambda z: (tf_rank.get(z["tf"], 3), abs(z["pivot"] - ltp)))

    dedup = []
    for z in combined:
        if not any(abs(z["pivot"] - ez["pivot"]) <= 3.00 and z["side"] == ez["side"] for ez in dedup):
            dedup.append(z)

    dedup.sort(key=lambda z: abs(z["pivot"] - ltp))
    final_zones = dedup[:12]
    state.set("mtf_liquidity_zones", final_zones, ttl=86400)
    return final_zones


def _mark_zone_spent(state: GoldState, zone_id: str):
    spent = state.get("spent_zone_ids") or []
    if zone_id not in spent:
        spent.append(zone_id)
        state.set("spent_zone_ids", spent[-150:], ttl=86400)
    zones = state.get("mtf_liquidity_zones") or []
    zones = [z for z in zones if z["id"] != zone_id]
    state.set("mtf_liquidity_zones", zones, ttl=86400)


# ==============================================================================
# V9.5 DATA-PROVEN SNIPER ENTRY DETECTORS (ZERO ATR, NO S3/PDL NOISE)
# ==============================================================================

# SETUP 1: TREND PULLBACK RETEST (All Volume Expansion & PDH/PDL Blasts Route Here!)
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
    curr_bar_ts = int(df_5m["time"].iloc[i])
    if curr_bar_ts <= int(pb_watch.get("armed_bar_ts", 0)):
        return None

    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    prev_l = float(df_5m["l"].iloc[i - 1])
    prev_h = float(df_5m["h"].iloc[i - 1])
    level = float(pb_watch["level"])
    action = pb_watch["action"]
    origin = pb_watch.get("origin", "Momentum Breakout")

    if action == "BUY_GOLD":
        if l <= pb_watch.get("breakout_high", level + 3.0) and (l <= level + 3.0 or l <= prev_l + SL_BUFFER_USD) and c > o and (c - l) >= 1.0:
            sl = round(min(l, prev_l) - SL_BUFFER_USD, 2)
            risk = max(MIN_RISK_USD, round(c - sl, 2))
            if risk <= PB_RISK_CAP_USD:
                state.set("pullback_watch", None)
                return {
                    "name": "TREND_PULLBACK_RETEST",
                    "action": "BUY_GOLD",
                    "tier": "A++ Institutional Retest Entry (De-Noised Pullback)",
                    "entry": c,
                    "sl": sl,
                    "risk": risk,
                    "trail_15r": round(c + 1.5 * risk, 2),
                    "rally_25r": round(c + 2.5 * risk, 2),
                    "why": f"Retest of {origin} level ${level:.2f} held with bullish close at ${c:.2f} (Invalidation Ref: ${sl:.2f})"
                }

    elif action == "SELL_GOLD":
        if h >= pb_watch.get("breakout_low", level - 3.0) and (h >= level - 3.0 or h >= prev_h - SL_BUFFER_USD) and c < o and (h - c) >= 1.0:
            sl = round(max(h, prev_h) + SL_BUFFER_USD, 2)
            risk = max(MIN_RISK_USD, round(sl - c, 2))
            if risk <= PB_RISK_CAP_USD:
                state.set("pullback_watch", None)
                return {
                    "name": "TREND_PULLBACK_RETEST",
                    "action": "SELL_GOLD",
                    "tier": "A++ Institutional Retest Entry (De-Noised Pullback)",
                    "entry": c,
                    "sl": sl,
                    "risk": risk,
                    "trail_15r": round(c - 1.5 * risk, 2),
                    "rally_25r": round(c - 2.5 * risk, 2),
                    "why": f"Retest of {origin} level ${level:.2f} rejected with bearish close at ${c:.2f} (Invalidation Ref: ${sl:.2f})"
                }

    return None


def arm_pullback_watch_from_expansions(df_5m: pd.DataFrame, df_15m: pd.DataFrame, state: GoldState):
    """
    Replaces direct chasing on 5M_VOLUME_EXPANSION_BLAST (-112R loss in 6M backtest)
    and PDH_PDL_MOMENTUM_BREAKOUT by arming a 30-min TREND_PULLBACK_RETEST watch!
    """
    if len(df_5m) < 20 or len(df_15m) < 10:
        return

    i = len(df_5m) - 1
    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    prev_c = float(df_5m["c"].iloc[i - 1])
    v_curr = float(df_5m["v"].iloc[i])
    v_avg = float(df_5m["v"].iloc[i - 14:i].mean())
    ema50_15m = float(df_15m["ema50"].iloc[-1])

    curr_body = abs(c - o)
    prev_bodies = [abs(float(df_5m["c"].iloc[k]) - float(df_5m["o"].iloc[k])) for k in range(i - 3, i)]
    avg_prev_body = max(0.50, sum(prev_bodies) / 3.0)

    box = df_5m.iloc[i - 6:i]
    box_h, box_l = float(box["h"].max()), float(box["l"].min())
    box_range = box_h - box_l
    if box_range <= BOX_MAX_USD and v_curr >= 2.0 * v_avg and curr_body >= 2.0 * avg_prev_body:
        if c > box_h and c > ema50_15m:
            state.set("pullback_watch", {
                "action": "BUY_GOLD",
                "level": box_h,
                "breakout_high": h,
                "origin": "5M Volume Blast",
                "armed_bar_ts": int(df_5m["time"].iloc[i]),
                "expires": time.time() + 1800,
            }, ttl=1800)
            log.info(f"[GOLD] 📢 5M Volume Blast UP above ${box_h:.2f} -> Armed TREND_PULLBACK_RETEST watch (No FOMO Chase).")
            return
        elif c < box_l and c < ema50_15m:
            state.set("pullback_watch", {
                "action": "SELL_GOLD",
                "level": box_l,
                "breakout_low": l,
                "origin": "5M Volume Blast",
                "armed_bar_ts": int(df_5m["time"].iloc[i]),
                "expires": time.time() + 1800,
            }, ttl=1800)
            log.info(f"[GOLD] 📢 5M Volume Blast DOWN below ${box_l:.2f} -> Armed TREND_PULLBACK_RETEST watch (No FOMO Chase).")
            return

    pdh, pdl = df_5m["pdh"].iloc[i], df_5m["pdl"].iloc[i]
    if pd.notna(pdh) and pd.notna(pdl):
        pdh, pdl = float(pdh), float(pdl)
        if prev_c <= pdh and c > pdh and c > ema50_15m and curr_body >= 1.5 * avg_prev_body and v_curr >= 1.8 * v_avg:
            state.set("pullback_watch", {
                "action": "BUY_GOLD",
                "level": pdh,
                "breakout_high": h,
                "origin": "PDH Expansion",
                "armed_bar_ts": int(df_5m["time"].iloc[i]),
                "expires": time.time() + 1800,
            }, ttl=1800)
            log.info(f"[GOLD] 📢 PDH Breakout above ${pdh:.2f} -> Armed TREND_PULLBACK_RETEST watch.")
        elif prev_c >= pdl and c < pdl and c < ema50_15m and curr_body >= 1.5 * avg_prev_body and v_curr >= 1.8 * v_avg:
            state.set("pullback_watch", {
                "action": "SELL_GOLD",
                "level": pdl,
                "breakout_low": l,
                "origin": "PDL Expansion",
                "armed_bar_ts": int(df_5m["time"].iloc[i]),
                "expires": time.time() + 1800,
            }, ttl=1800)
            log.info(f"[GOLD] 📢 PDL Breakdown below ${pdl:.2f} -> Armed TREND_PULLBACK_RETEST watch.")


# SETUP 2 & 3: BIG S/R LIQUIDITY SWEEP [1D / 4H / 1H] + HTF MSS SWEEP
def check_big_sr_liquidity_hunts(df_5m: pd.DataFrame, state: GoldState):
    """
    Evaluates 5M sweep & rejection on fresh unswept [1D_ZONE], [4H_ZONE], and [1H_ZONE] levels,
    plus Market Structure Shift (MSS) sweeps after touching a [1D/4H] zone.
    """
    if len(df_5m) < 20:
        return None

    zones = state.get("mtf_liquidity_zones") or []
    if not zones:
        return None

    i = len(df_5m) - 1
    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    c_range = max(h - l, 0.1)
    upper_wick = (h - max(o, c)) / c_range
    lower_wick = (min(o, c) - l) / c_range

    tf_rank = {"1D": 0, "4H": 1, "1H": 2}
    ordered_zones = sorted(zones, key=lambda z: tf_rank.get(z.get("tf", "1H"), 3))

    for z in ordered_zones:
        zp = float(z["pivot"])
        tf_tag = z.get("tf", "1H")
        zid = z["id"]

        if z["side"] == "BSL":
            if c > zp + 3.00:
                _mark_zone_spent(state, zid)
                continue
            if h >= zp - 2.00 and c < zp + 2.00 and tf_tag in ("1D", "4H"):
                state.set("recent_htf_touch", {"dir": "SELL_GOLD", "tf": tf_tag, "pivot": zp, "ts": time.time()}, ttl=5400)
            if h > zp and c < zp and upper_wick >= 0.35 and c <= o:
                _mark_zone_spent(state, zid)
                sl = round(h + SL_BUFFER_USD, 2)
                risk = max(MIN_RISK_USD, round(sl - c, 2))
                if risk <= HTF_RISK_CAP_USD:
                    tier_str = "A+++ 1D Institutional Liquidity Sweep" if tf_tag == "1D" else f"A++ {tf_tag} Major Liquidity Sweep"
                    return {
                        "name": f"BIG_SR_SWEEP_{tf_tag}",
                        "action": "SELL_GOLD",
                        "tier": tier_str,
                        "entry": c,
                        "sl": sl,
                        "risk": risk,
                        "trail_15r": round(c - 1.5 * risk, 2),
                        "rally_25r": round(c - 2.5 * risk, 2),
                        "why": f"Swept [{tf_tag}_ZONE] High (${zp:.2f}) with {int(upper_wick*100)}% rejection wick & closed RED"
                    }

        elif z["side"] == "SSL":
            if c < zp - 3.00:
                _mark_zone_spent(state, zid)
                continue
            if l <= zp + 2.00 and c > zp - 2.00 and tf_tag in ("1D", "4H"):
                state.set("recent_htf_touch", {"dir": "BUY_GOLD", "tf": tf_tag, "pivot": zp, "ts": time.time()}, ttl=5400)
            if l < zp and c > zp and lower_wick >= 0.35 and c >= o:
                _mark_zone_spent(state, zid)
                sl = round(l - SL_BUFFER_USD, 2)
                risk = max(MIN_RISK_USD, round(c - sl, 2))
                if risk <= HTF_RISK_CAP_USD:
                    tier_str = "A+++ 1D Institutional Liquidity Sweep" if tf_tag == "1D" else f"A++ {tf_tag} Major Liquidity Sweep"
                    return {
                        "name": f"BIG_SR_SWEEP_{tf_tag}",
                        "action": "BUY_GOLD",
                        "tier": tier_str,
                        "entry": c,
                        "sl": sl,
                        "risk": risk,
                        "trail_15r": round(c + 1.5 * risk, 2),
                        "rally_25r": round(c + 2.5 * risk, 2),
                        "why": f"Swept [{tf_tag}_ZONE] Low (${zp:.2f}) with {int(lower_wick*100)}% absorption wick & closed GREEN"
                    }

    # SETUP 3: BIG_SR_MSS_SWEEP (HTF Zone Touch + 5M EMA20/50 Trend Shift + Fresh <=30m Swing Sweep)
    touch = state.get("recent_htf_touch")
    if touch and (time.time() - touch.get("ts", 0)) <= 5400:
        ema20_5m = float(df_5m["ema20"].iloc[i])
        ema50_5m = float(df_5m["ema50"].iloc[i])
        recent_6 = df_5m.iloc[i - 6:i - 1]
        if not recent_6.empty:
            if touch["dir"] == "SELL_GOLD" and c < ema20_5m and c < ema50_5m:
                micro_h = float(recent_6["h"].max())
                if h > micro_h and c < micro_h and upper_wick >= 0.35 and c <= o:
                    sl = round(h + SL_BUFFER_USD, 2)
                    risk = max(MIN_RISK_USD, round(sl - c, 2))
                    if risk <= HTF_RISK_CAP_USD:
                        state.set("recent_htf_touch", None)
                        return {
                            "name": "BIG_SR_MSS_SWEEP",
                            "action": "SELL_GOLD",
                            "tier": f"A++ {touch['tf']} Zone MSS + Liquidity Sweep",
                            "entry": c,
                            "sl": sl,
                            "risk": risk,
                            "trail_15r": round(c - 1.5 * risk, 2),
                            "rally_25r": round(c - 2.5 * risk, 2),
                            "why": f"After tapping [{touch['tf']}_ZONE] ${touch['pivot']:.2f}, shifted below 5M EMA20/50 & swept ${micro_h:.2f}"
                        }
            elif touch["dir"] == "BUY_GOLD" and c > ema20_5m and c > ema50_5m:
                micro_l = float(recent_6["l"].min())
                if l < micro_l and c > micro_l and lower_wick >= 0.35 and c >= o:
                    sl = round(l - SL_BUFFER_USD, 2)
                    risk = max(MIN_RISK_USD, round(c - sl, 2))
                    if risk <= HTF_RISK_CAP_USD:
                        state.set("recent_htf_touch", None)
                        return {
                            "name": "BIG_SR_MSS_SWEEP",
                            "action": "BUY_GOLD",
                            "tier": f"A++ {touch['tf']} Zone MSS + Liquidity Sweep",
                            "entry": c,
                            "sl": sl,
                            "risk": risk,
                            "trail_15r": round(c + 1.5 * risk, 2),
                            "rally_25r": round(c + 2.5 * risk, 2),
                            "why": f"After tapping [{touch['tf']}_ZONE] ${touch['pivot']:.2f}, shifted above 5M EMA20/50 & swept ${micro_l:.2f}"
                        }

    return None


# SETUP 4: S2 PDH SWEEP (Wick >= 35%, Risk <= $5.50, Spent PDH Lock — No Counter-Trend PDL!)
def check_s2_pdh_sweep(df: pd.DataFrame, state: GoldState):
    if len(df) < 15:
        return None
    i = len(df) - 1
    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    pdh = df["pdh"].iloc[i]
    if pd.isna(pdh):
        return None
    pdh = float(pdh)

    utc_day = str(df["utc_date"].iloc[i])
    spent_key = f"pdh_spent_{utc_day}_{pdh:.2f}"
    if state.get(spent_key):
        return None

    c_range = max(h - l, 0.1)
    upper_wick = (h - max(o, c)) / c_range

    if c > pdh + 2.00:
        state.set(spent_key, True, ttl=86400)
        return None

    if h > pdh and c < pdh and upper_wick >= 0.35 and c <= o:
        state.set(spent_key, True, ttl=86400)
        sl = round(h + SL_BUFFER_USD, 2)
        risk = max(MIN_RISK_USD, round(sl - c, 2))
        if risk <= SWEEP_RISK_CAP_USD:
            return {
                "name": "S2_PDH_SWEEP",
                "action": "SELL_GOLD",
                "tier": "A+ PDH Liquidity Sweep Trap (Wick >= 35%)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "trail_15r": round(c - 1.5 * risk, 2),
                "rally_25r": round(c - 2.5 * risk, 2),
                "why": f"Swept Previous Day High (${pdh:.2f}) with {int(upper_wick*100)}% rejection wick & closed RED"
            }

    return None


# SETUP 5: SESSION RANGE SWEEP (Wick >= 35%, Risk <= $5.50, Spent Session Lock)
def check_session_range_sweep(df: pd.DataFrame, state: GoldState):
    if len(df) < 20:
        return None
    i = len(df) - 1
    bar_ts = df["ts"].iloc[i]
    today_date = bar_ts.date()

    prior_session_candles = df[(df["ts"].dt.date == today_date) & (df["ts"] < bar_ts - pd.Timedelta(hours=1))]
    if len(prior_session_candles) < 6:
        return None

    sess_h = float(prior_session_candles["h"].max())
    sess_l = float(prior_session_candles["l"].min())

    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    c_range = max(h - l, 0.1)
    upper_wick = (h - max(o, c)) / c_range
    lower_wick = (min(o, c) - l) / c_range

    spent_h_key = f"sess_h_spent_{today_date}_{sess_h:.2f}"
    spent_l_key = f"sess_l_spent_{today_date}_{sess_l:.2f}"

    if h > sess_h and c < sess_h and upper_wick >= 0.35 and c <= o and not state.get(spent_h_key):
        state.set(spent_h_key, True, ttl=86400)
        sl = round(h + SL_BUFFER_USD, 2)
        risk = max(MIN_RISK_USD, round(sl - c, 2))
        if risk <= SWEEP_RISK_CAP_USD:
            return {
                "name": "SESSION_RANGE_SWEEP",
                "action": "SELL_GOLD",
                "tier": "A++ Session High Sweep Reversal (Tight Risk)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "trail_15r": round(c - 1.5 * risk, 2),
                "rally_25r": round(c - 2.5 * risk, 2),
                "why": f"Swept Prior Session High (${sess_h:.2f}) with {int(upper_wick*100)}% rejection wick & closed RED"
            }

    if l < sess_l and c > sess_l and lower_wick >= 0.35 and c >= o and not state.get(spent_l_key):
        state.set(spent_l_key, True, ttl=86400)
        sl = round(l - SL_BUFFER_USD, 2)
        risk = max(MIN_RISK_USD, round(c - sl, 2))
        if risk <= SWEEP_RISK_CAP_USD:
            return {
                "name": "SESSION_RANGE_SWEEP",
                "action": "BUY_GOLD",
                "tier": "A++ Session Low Sweep Reversal (Tight Risk)",
                "entry": c,
                "sl": sl,
                "risk": risk,
                "trail_15r": round(c + 1.5 * risk, 2),
                "rally_25r": round(c + 2.5 * risk, 2),
                "why": f"Swept Prior Session Low (${sess_l:.2f}) with {int(lower_wick*100)}% absorption wick & closed GREEN"
            }

    return None


# SETUP 6: USER 3-SWING INDUCEMENT TRAP (4H Aligned + <=60m Freshness + Wick >= 38% + Spent Lock)
def check_user_3swing_inducement(df: pd.DataFrame, sw_h, sw_l, is_4h_bull: bool, is_4h_bear: bool, state: GoldState):
    if len(df) < 30:
        return None
    i = len(df) - 1
    c, o, h, l = float(df["c"].iloc[i]), float(df["o"].iloc[i]), float(df["h"].iloc[i]), float(df["l"].iloc[i])
    c_range = max(h - l, 0.1)
    upper_wick = (h - max(o, c)) / c_range
    lower_wick = (min(o, c) - l) / c_range

    spent_smc = set(state.get("spent_smc_levels") or [])

    if upper_wick >= 0.38 and c <= o and is_4h_bear:
        recent_sh = [s for s in sw_h if s["idx"] <= i - 2]
        if len(recent_sh) >= 2:
            for idx_2 in range(len(recent_sh) - 1, max(-1, len(recent_sh) - 8), -1):
                s2 = recent_sh[idx_2]
                gap2 = i - s2["idx"]
                lvl_key = f"SH_{s2['price']:.2f}"
                if not (3 <= gap2 <= 12) or lvl_key in spent_smc or not (h > s2["price"] and c < s2["price"]):
                    continue
                for idx_1 in range(idx_2 - 1, max(-1, idx_2 - 8), -1):
                    s1 = recent_sh[idx_1]
                    gap1 = s2["idx"] - s1["idx"]
                    if not (3 <= gap1 <= 60):
                        continue
                    if s2["price"] < s1["price"] and h < (s1["price"] + 2.0):
                        sl = round(h + SL_BUFFER_USD, 2)
                        risk = max(MIN_RISK_USD, round(sl - c, 2))
                        if risk <= SWEEP_RISK_CAP_USD:
                            spent_smc.add(lvl_key)
                            state.set("spent_smc_levels", list(spent_smc)[-100:], ttl=43200)
                            return {
                                "name": "USER_3SWING_INDUCEMENT_TRAP",
                                "action": "SELL_GOLD",
                                "tier": "A+ 4H-Aligned SMC Inducement Trap",
                                "entry": c,
                                "sl": sl,
                                "risk": risk,
                                "trail_15r": round(c - 1.5 * risk, 2),
                                "rally_25r": round(c - 2.5 * risk, 2),
                                "why": f"4H Bearish + Swept fresh ({gap2*5}m) Inducement High (${s2['price']:.2f}) below Major High (${s1['price']:.2f}) with {int(upper_wick*100)}% wick"
                            }

    if lower_wick >= 0.38 and c >= o and is_4h_bull:
        recent_sl = [s for s in sw_l if s["idx"] <= i - 2]
        if len(recent_sl) >= 2:
            for idx_2 in range(len(recent_sl) - 1, max(-1, len(recent_sl) - 8), -1):
                s2 = recent_sl[idx_2]
                gap2 = i - s2["idx"]
                lvl_key = f"SL_{s2['price']:.2f}"
                if not (3 <= gap2 <= 12) or lvl_key in spent_smc or not (l < s2["price"] and c > s2["price"]):
                    continue
                for idx_1 in range(idx_2 - 1, max(-1, idx_2 - 8), -1):
                    s1 = recent_sl[idx_1]
                    gap1 = s2["idx"] - s1["idx"]
                    if not (3 <= gap1 <= 60):
                        continue
                    if s2["price"] > s1["price"] and l > (s1["price"] - 2.0):
                        sl = round(l - SL_BUFFER_USD, 2)
                        risk = max(MIN_RISK_USD, round(c - sl, 2))
                        if risk <= SWEEP_RISK_CAP_USD:
                            spent_smc.add(lvl_key)
                            state.set("spent_smc_levels", list(spent_smc)[-100:], ttl=43200)
                            return {
                                "name": "USER_3SWING_INDUCEMENT_TRAP",
                                "action": "BUY_GOLD",
                                "tier": "A+ 4H-Aligned SMC Inducement Trap",
                                "entry": c,
                                "sl": sl,
                                "risk": risk,
                                "trail_15r": round(c + 1.5 * risk, 2),
                                "rally_25r": round(c + 2.5 * risk, 2),
                                "why": f"4H Bullish + Swept fresh ({gap2*5}m) Inducement Low (${s2['price']:.2f}) above Major Low (${s1['price']:.2f}) with {int(lower_wick*100)}% wick"
                            }

    return None


# ==============================================================================
# PURE PRICE ACTION HEADROOM & HTF WALL AUDIT
# ==============================================================================
def run_deterministic_filters(df: pd.DataFrame, setup: dict, state: GoldState):
    i = len(df) - 1
    c = float(df["c"].iloc[i])
    ema50 = float(df["ema50"].iloc[i])
    risk = float(setup["risk"])

    if risk > HTF_RISK_CAP_USD:
        log.info(f"[GOLD] Blocked {setup['name']}: Bloated Invalidation (${risk:.2f} > ${HTF_RISK_CAP_USD:.2f})")
        return False, "Bloated Risk"

    ema_note = f"Clear (5M EMA50 at ${ema50:.2f})"
    if setup["action"] == "BUY_GOLD" and c < ema50 and (ema50 - c) < risk:
        ema_note = f"⚠️ WALL AHEAD: 5M EMA50 at ${ema50:.2f} (${ema50 - c:.2f} headroom < ${risk:.2f} risk)"
    elif setup["action"] == "SELL_GOLD" and c > ema50 and (c - ema50) < risk:
        ema_note = f"⚠️ WALL BELOW: 5M EMA50 at ${ema50:.2f} (${c - ema50:.2f} headroom < ${risk:.2f} risk)"

    zones = state.get("mtf_liquidity_zones") or []
    htf_wall_note = "Clear (No immediate opposing 1D/4H wall)"
    for z in zones:
        if z.get("tf") not in ("1D", "4H"):
            continue
        zp = float(z["pivot"])
        if setup["action"] == "BUY_GOLD" and z["side"] == "BSL" and zp > c and (zp - c) < (1.5 * risk):
            htf_wall_note = f"⚠️ HTF WALL: [{z['tf']}_ZONE] Resistance at ${zp:.2f} (${zp - c:.2f} away)"
            break
        elif setup["action"] == "SELL_GOLD" and z["side"] == "SSL" and zp < c and (c - zp) < (1.5 * risk):
            htf_wall_note = f"⚠️ HTF WALL: [{z['tf']}_ZONE] Support at ${zp:.2f} (${c - zp:.2f} away)"
            break

    setup["ema_note"] = ema_note
    setup["htf_wall_note"] = htf_wall_note
    return True, ema_note


# ==============================================================================
# MULTI-TF MACRO EXECUTION ENGINE + BACKGROUND HYDROGEN CONFIRMATION WORKER
# ==============================================================================
def _background_hydrogen_confirm_macro(shared_store, sys_p, usr_p, fallback_provider, fallback_bias, ltp, ema20_4h, z_lines, pool_summary):
    """
    When Tier-1 Macro falls back to Lite because gemini-4-hydrogen / 3.8 / 3.6 were busy,
    this daemon thread retries gemini-4-hydrogen (HIGH thinking) every 2 minutes in the background
    (up to 15 tries = 30 mins) without blocking the 5M live trading loop!
    """
    global _hydrogen_bg_active
    if _hydrogen_bg_active:
        return
    _hydrogen_bg_active = True
    state = GoldState(shared_store)

    try:
        for attempt in range(1, 16):
            time.sleep(120)
            log.info(f"[GOLD-BG] 🧠 Background Hydrogen Confirmation Attempt #{attempt}/15...")
            ai_res, provider = call_ai_json(sys_p, usr_p, max_tokens=450, mode="hydrogen_only")
            if ai_res:
                h_bias = ai_res.get("macro_bias", fallback_bias)
                h_regime = ai_res.get("regime", "TRENDING")
                h_magnet = ai_res.get("primary_magnet", "BALANCED")
                h_text = ai_res.get("assessment", "Hydrogen institutional liquidity audit confirmed.")

                state.set("macro_bias", h_bias, ttl=86400)
                state.set("macro_regime", h_regime, ttl=86400)
                state.set("macro_magnet", h_magnet, ttl=86400)

                msg = f"""🧬 <b>[GOLD] GEMINI-4-HYDROGEN BACKGROUND CONFIRMATION</b>
📅 <code>{datetime.now(IST).strftime('%d %b %Y | %H:%M IST')}</code>

✅ <b>Hydrogen Audit Confirmed</b> (Upgraded from <code>{esc(fallback_provider)}</code>)
📊 <b>LTP:</b> <b>${ltp:,.2f}</b> | <b>4H EMA20:</b> <code>${ema20_4h:,.2f}</code>
🧭 <b>Bias:</b> <b>{esc(h_bias)}</b> | <b>Regime:</b> <code>{esc(h_regime)}</code> | <b>Magnet:</b> <code>{esc(h_magnet)}</code>
💧 <b>Pools:</b> <code>{esc(pool_summary)}</code>

🗺️ <b>Active MTF Liquidity Zones:</b>
{chr(10).join(z_lines) if z_lines else '• No immediate unswept zones in window'}

🧠 <b>Hydrogen View ({esc(provider)}):</b>
<i>{esc(h_text)}</i>"""
                tg_send(msg)
                log.info(f"[GOLD-BG] ✅ Hydrogen macro confirmation succeeded on attempt #{attempt}!")
                return
    except Exception as e:
        log.error(f"[GOLD-BG] Hydrogen background worker error: {type(e).__name__}")
    finally:
        _hydrogen_bg_active = False


def execute_mtf_macro_analysis(shared_store=None, include_1d: bool = False, retry_until_success: bool = False, slot_tag: str = ""):
    """
    Executes 3-Tier Multi-Timeframe Liquidity Mapping (1D + 4H + 1H) + AI Macro Audit.
    - At 23:30 IST (include_1d=True, retry_until_success=True): fetches 180-day 1D + 30-day 4H + 7-day 1H
      and auto-retries every 2 minutes in the background until successful!
    - At 05:30 / 11:30 / 17:30 IST (include_1d=False): refreshes 4H + 1H intraday liquidity zones.
    """
    state = GoldState(shared_store)
    if slot_tag:
        if state.get(slot_tag):
            log.info(f"[GOLD] Macro slot {slot_tag} already completed. Skipping duplicate.")
            return True
        state.set(slot_tag, True, ttl=7200)

    while True:
        try:
            now = datetime.now(IST)
            df_1d = fetch_delta_candles(GOLD_SYMBOL, "1d", hours_back=24 * 180, closed_only=True) if include_1d else None
            df_4h = fetch_delta_candles(GOLD_SYMBOL, "4h", hours_back=24 * 30, closed_only=True)
            df_1h = fetch_delta_candles(GOLD_SYMBOL, "1h", hours_back=24 * 7, closed_only=True)

            if df_4h.empty or df_1h.empty:
                log.warning("[GOLD] Macro analysis: Insufficient 4H/1H data from Delta.")
                if retry_until_success:
                    time.sleep(120)
                    continue
                return False

            df_4h = compute_indicators(df_4h)
            df_1h = compute_indicators(df_1h)
            if df_1d is not None and not df_1d.empty:
                df_1d = compute_indicators(df_1d)

            ltp = float(df_1h["c"].iloc[-1])
            ema20_4h = float(df_4h["ema20"].iloc[-1])
            ema50_1h = float(df_1h["ema50"].iloc[-1])
            r_high_24h = float(df_1h["h"].tail(24).max())
            r_low_24h = float(df_1h["l"].tail(24).min())
            trend_bias = "BULLISH" if ltp > ema20_4h else "BEARISH"

            state.set("4h_ema20", ema20_4h, ttl=86400)
            state.set("1h_ema50", ema50_1h, ttl=86400)

            zones = build_mtf_liquidity_zones(state, ltp, df_1d=df_1d, df_4h=df_4h, df_1h=df_1h)
            pools = detect_liquidity_pools(df_1h, tolerance_usd=2.50)
            state.set("htf_pool_summary", pools["summary"], ttl=86400)

            top_zones_txt = ", ".join([z["why"] for z in zones[:5]]) if zones else "None nearby"
            mode_label = "DAILY 1D (6-MONTH) + 4H + 1H MACRO" if include_1d else "6-HOUR INTRADAY (4H + 1H) MACRO"

            sys_p = (
                "You are the Lead Institutional Gold (XAUTUSD) Macro Liquidity Strategist. "
                "Analyze Python's pre-computed [1D_ZONE], [4H_ZONE], [1H_ZONE] fresh unswept liquidity levels, "
                "4H EMA20 trend, and EQH/EQL liquidity pool asymmetry. Identify the single dominant liquidity magnet "
                "(BUY_SIDE_LIQUIDITY above Equal Highs/resistance OR SELL_SIDE_LIQUIDITY below Equal Lows/support) "
                "and classify regime strictly as TRENDING_BULL, TRENDING_BEAR, or CHOP_RANGE. "
                'Respond ONLY in valid JSON: {"macro_bias": "BULLISH/BEARISH/NEUTRAL", '
                '"regime": "TRENDING_BULL/TRENDING_BEAR/CHOP_RANGE", '
                '"primary_magnet": "BSL/SSL", "assessment": "1-2 concise sentences"}'
            )
            usr_p = (
                f"Scope: {mode_label} | Time: {now.strftime('%Y-%m-%d %H:%M IST')} | Gold LTP: ${ltp:.2f}\n"
                f"4H EMA20: ${ema20_4h:.2f} ({trend_bias}) | 1H EMA50: ${ema50_1h:.2f} | 24h Range: ${r_low_24h:.2f} - ${r_high_24h:.2f}\n"
                f"Liquidity Pool Asymmetry: {pools['summary']}\n"
                f"Fresh Unswept MTF Zones (1D > 4H > 1H): {top_zones_txt}"
            )

            ai_res, provider = call_ai_json(sys_p, usr_p, max_tokens=450, mode="macro")

            if not ai_res:
                log.warning(f"[GOLD] ⚠️ Macro AI cascade failed ({provider}).")
                if retry_until_success:
                    log.info("[GOLD] 🔄 23:30 Daily Macro will auto-retry in 2 minutes until successful...")
                    time.sleep(120)
                    continue
                return False

            ai_bias = ai_res.get("macro_bias", trend_bias)
            ai_regime = ai_res.get("regime", "TRENDING_BULL" if trend_bias == "BULLISH" else "TRENDING_BEAR")
            ai_magnet = ai_res.get("primary_magnet", "BSL" if pools["bsl_eqh_count"] >= pools["ssl_eql_count"] else "SSL")
            ai_text = ai_res.get("assessment", "Gold liquidity structure mapped across higher timeframes.")

            state.set("macro_bias", ai_bias, ttl=86400)
            state.set("macro_regime", ai_regime, ttl=86400)
            state.set("macro_magnet", ai_magnet, ttl=86400)

            z_lines = [f"• <b>[{esc(z['tf'])}] {esc(z['type'])}:</b> <code>${z['pivot']:,.2f}</code>" for z in zones[:5]]

            msg = f"""🏛️ <b>[GOLD] {mode_label} | {now.strftime('%H:%M IST')}</b>

📊 <b>LTP      :</b> <b>${ltp:,.2f}</b> | <b>4H Trend:</b> <b>{trend_bias}</b>
📐 <b>4H EMA20 :</b> <code>${ema20_4h:,.2f}</code> | <b>1H EMA50:</b> <code>${ema50_1h:,.2f}</code>
📏 <b>24h Rng  :</b> <code>${r_low_24h:,.1f} — ${r_high_24h:,.1f}</code>
💧 <b>Pools    :</b> <code>{esc(pools['summary'])}</code>

🗺️ <b>Active Unswept MTF Liquidity Zones:</b>
{chr(10).join(z_lines) if z_lines else '• No unswept zones within range'}

🧠 <b>AI Macro Audit ({esc(provider)}):</b> [{esc(ai_bias)} | {esc(ai_regime)} | Magnet: {esc(ai_magnet)}]
<i>{esc(ai_text)}</i>"""
            tg_send(msg)
            log.info(f"[GOLD] 🏛️ {mode_label} published via {provider}.")

            if HYDROGEN_MODEL not in provider and GEMINI_API_KEYS:
                log.info(f"[GOLD] 🧬 Spawning background {HYDROGEN_MODEL} confirmation worker...")
                threading.Thread(
                    target=_background_hydrogen_confirm_macro,
                    args=(shared_store, sys_p, usr_p, provider, ai_bias, ltp, ema20_4h, z_lines, pools["summary"]),
                    daemon=True,
                ).start()

            return True

        except Exception as e:
            log.error(f"[GOLD] Macro execution error: {type(e).__name__}")
            if retry_until_success:
                time.sleep(120)
                continue
            return False


def start_macro_background_scheduler(shared_store=None):
    """
    Dedicated Background Daemon Thread:
      • 23:30 IST: Daily 1D (6-Month) + 4H + 1H Liquidity Audit (with 2-min auto-retry until success)
      • 05:30, 11:30, 17:30 IST: Intraday 4H + 1H Liquidity Refresh
    Runs completely independently so the 5-minute execution engine is NEVER blocked.
    """
    def _scheduler_loop():
        log.info("[GOLD-SCHED] 🕰️ Background MTF Macro Scheduler started (23:30 1D + 05:30/11:30/17:30 4H-1H).")
        while True:
            try:
                now = datetime.now(IST)
                date_str = now.strftime("%Y-%m-%d")

                if now.hour == 23 and now.minute == 30:
                    slot = f"macro_slot_{date_str}_23"
                    execute_mtf_macro_analysis(shared_store, include_1d=True, retry_until_success=True, slot_tag=slot)
                    time.sleep(65)
                elif now.hour in (5, 11, 17) and now.minute == 30:
                    slot = f"macro_slot_{date_str}_{now.hour}"
                    execute_mtf_macro_analysis(shared_store, include_1d=False, retry_until_success=False, slot_tag=slot)
                    time.sleep(65)
                else:
                    time.sleep(20)
            except Exception as e:
                log.error(f"[GOLD-SCHED] Scheduler loop error: {type(e).__name__}")
                time.sleep(30)

    t = threading.Thread(target=_scheduler_loop, daemon=True)
    t.start()
    return t


# ==============================================================================
# DIAGNOSTIC STARTUP CHECK & DAILY 09:15 IST SYSTEM AUDIT
# ==============================================================================
def run_startup_diagnostics(state: GoldState):
    log.info("[GOLD] 🔍 Running Startup Diagnostic Suite (V9.5)...")
    now = datetime.now(IST)

    t0 = time.time()
    df_test = fetch_delta_candles(GOLD_SYMBOL, "5m", hours_back=3, closed_only=False)
    delta_lat = int((time.time() - t0) * 1000)
    delta_status = "ONLINE" if not df_test.empty else "OFFLINE"
    ltp_test = float(df_test.iloc[-1]["c"]) if not df_test.empty else 0.0

    t0 = time.time()
    ai_test_res, ai_provider = call_ai_json(
        'Respond strictly in JSON: {"status":"ok"}',
        "Diagnostic ping. Return status ok.",
        max_tokens=100,
        mode="fast_5m",
    )
    ai_lat = int((time.time() - t0) * 1000)
    ai_status = f"ONLINE ({ai_lat}ms)" if ai_test_res else "OFFLINE / UNREACHABLE"

    threading.Thread(
        target=execute_mtf_macro_analysis,
        args=(state.store, True, False, f"startup_{now.strftime('%Y%m%d_%H%M')}"),
        daemon=True,
    ).start()

    startup_msg = f"""🚀 <b>[GOLD INSTITUTIONAL LIQUIDITY ENGINE V9.5 ONLINE]</b>
📅 <b>Time:</b> <code>{now.strftime('%Y-%m-%d %H:%M:%S IST')}</code> | <b>Server:</b> <code>Koyeb</code>

🟢 <b>Delta Public API :</b> <code>{delta_status} (LTP: ${ltp_test:,.2f} | {delta_lat}ms)</code>
🟢 <b>5M AI Supervisor :</b> <code>{ai_status} [{esc(ai_provider)}]</code>
🟢 <b>Macro AI Cascade :</b> <code>{esc(HYDROGEN_MODEL)} -> 3.8-flash -> 3.6-flash -> 3.5-flash-lite</code>
🟢 <b>HTTP Health Check:</b> <code>RUNNING (Port {PORT} - UptimeRobot Ready)</code>

🎯 <b>V9.5 De-Noised Sniper Setups (Entry Identification Only):</b>
- <code>BIG_SR_SWEEP [1D / 4H / 1H]</code> (Fresh Unswept HTF Liquidity Sweeps)
- <code>BIG_SR_MSS_SWEEP</code> (1D/4H Touch + 5M EMA20/50 Shift + Sweep)
- <code>TREND_PULLBACK_RETEST</code> (All 5M Volume/PDH/PDL Blasts Routed Here)
- <code>SESSION_RANGE_SWEEP</code> (Wick >= 35%, Risk <= $5.50, Spent Lock)
- <code>S2_PDH_SWEEP</code> (Wick >= 35%, Risk <= $5.50, Spent PDH Lock)
- <code>USER_3SWING_INDUCEMENT_TRAP</code> (4H Aligned, <=60m Fresh, Wick >= 38%)

🛡️ <i>S3 Box & Counter-Trend PDL Removed | Manual SL/Target Mode Active.</i>"""

    tg_send(startup_msg)
    log.info("[GOLD] ✅ Startup diagnostics complete and V9.5 report pushed to Telegram.")


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
        zones = state.get("mtf_liquidity_zones") or []
        regime = state.get("macro_regime") or "TRENDING"

        daily_msg = f"""☀️ <b>[DAILY 09:15 AM SYSTEM HEALTH AUDIT — V9.5]</b>
📅 <code>{now.strftime('%d %b %Y | %H:%M IST')}</code>

📊 <b>Engine Status:</b>
- AI Calls Today   : <code>{_daily_ai_call_count} / 1,000 Lite Pool</code>
- 5M Surveillance  : <code>ACTIVE (V9.5 Entry-Only Liquidity Engine)</code>
- Active MTF Zones : <code>{len(zones)} Unswept [1D/4H/1H] Levels</code>
- Macro Regime     : <code>{esc(regime)}</code>

🗺️ <b>Market Context:</b>
- GOLD LTP : <b>${ltp:,.2f}</b>
- 15M EMA50: <b>${ema50:,.2f}</b> ({'Bullish' if ltp > ema50 else 'Bearish'})
- PDH / PDL: <b>${pdh:,.2f} / ${pdl:,.2f}</b>

✅ <i>All 6 V9.5 Sniper Setups active (S3 & PDL noise removed).</i>"""
        tg_send(daily_msg)


# ==============================================================================
# MAIN CYCLE RUNNER (EVALUATES CLOSED 5M CANDLES MULTI-TF)
# ==============================================================================
def run_gold_cycle(shared_store=None):
    global _scheduler_started
    if not _scheduler_started:
        _scheduler_started = True
        start_macro_background_scheduler(shared_store)

    if not _gold_lock.acquire(blocking=False):
        log.warning("[GOLD] Execution lock busy, skipping cycle")
        return

    try:
        state = GoldState(shared_store)
        state.set("last_tick", time.time(), ttl=86400)

        df_15m = fetch_delta_candles(GOLD_SYMBOL, "15m", hours_back=60, closed_only=True)
        df_5m = fetch_delta_candles(GOLD_SYMBOL, "5m", hours_back=24, closed_only=True)

        if df_5m.empty or len(df_5m) < 35 or df_15m.empty:
            log.warning("[GOLD] Insufficient candle data from Delta Exchange")
            return

        df_15m = compute_indicators(df_15m)
        df_5m = compute_indicators(df_5m)

        ltp = float(df_5m["c"].iloc[-1])
        last_bar_time = int(df_5m.iloc[-1]["time"])

        check_daily_0915_health_report(state, df_15m)

        if state.get("last_evaluated_5m_ts") == last_bar_time:
            return
        state.set("last_evaluated_5m_ts", last_bar_time, ttl=300)

        closed_ts_str = df_5m["ts"].iloc[-1].strftime("%H:%M IST")
        ema50_15m = float(df_15m["ema50"].iloc[-1])
        ema20_4h = state.get("4h_ema20")
        if ema20_4h is None:
            df_4h_boot = fetch_delta_candles(GOLD_SYMBOL, "4h", hours_back=24 * 15, closed_only=True)
            if not df_4h_boot.empty:
                df_4h_boot = compute_indicators(df_4h_boot)
                ema20_4h = float(df_4h_boot["ema20"].iloc[-1])
                state.set("4h_ema20", ema20_4h, ttl=86400)
            else:
                ema20_4h = ema50_15m

        is_4h_bull = ltp > float(ema20_4h)
        is_4h_bear = ltp < float(ema20_4h)
        trend_label = "BULLISH" if ltp > ema50_15m else "BEARISH"
        trend_4h_label = "BULLISH" if is_4h_bull else "BEARISH"

        mtf_zones = state.get("mtf_liquidity_zones") or []
        nearest_str = f"{mtf_zones[0]['id']}: ${mtf_zones[0]['pivot']:.2f}" if mtf_zones else "Syncing MTF"

        log.info(f"[GOLD] ═════════ 5M EXECUTION AUDIT (V9.5): {closed_ts_str} ═════════")
        log.info(f"[GOLD] Close: ${ltp:.2f} | 4H Trend: {trend_4h_label} (${float(ema20_4h):.2f}) | 15M Trend: {trend_label}")
        log.info(f"[GOLD] Nearest MTF Zone: {nearest_str} | AI Calls Today: {_daily_ai_call_count}")

        sw_h, sw_l = find_swings(df_5m, w=3)

        # Evaluate V9.5 Data-Proven Sniper Setups (Check active Pullback Retest BEFORE arming new watches!)
        setup = (
            check_big_sr_liquidity_hunts(df_5m, state)
            or check_trend_pullback(df_5m, df_15m, state)
            or check_s2_pdh_sweep(df_5m, state)
            or check_session_range_sweep(df_5m, state)
            or check_user_3swing_inducement(df_5m, sw_h, sw_l, is_4h_bull, is_4h_bear, state)
        )

        # Arm Pullback Watch if a 5M Volume Blast or PDH/PDL Expansion just closed
        arm_pullback_watch_from_expansions(df_5m, df_15m, state)

        if not setup:
            log.info("[GOLD] Setups Evaluated: HTF_Sweep=NO | PB_Retest=NO | S2_PDH=NO | Sess_Sweep=NO | 3Swing_SMC=NO -> WAITING")
            return

        passed, filter_msg = run_deterministic_filters(df_5m, setup, state)
        if not passed:
            log.warning(f"[GOLD] Setup {setup['name']} rejected by deterministic check: {filter_msg}")
            return

        cd_key = f"cd_{setup['name']}_{setup['action']}"
        cd_until = state.get(cd_key)
        if cd_until and time.time() < cd_until:
            rem_m = int((cd_until - time.time()) / 60)
            log.info(f"[GOLD] Cooldown active for {setup['name']} ({rem_m}m remaining). Skipped.")
            return

        pools_5m = detect_liquidity_pools(df_5m, tolerance_usd=1.50)
        macro_bias = state.get("macro_bias") or trend_4h_label
        macro_regime = state.get("macro_regime") or "TRENDING"
        macro_magnet = state.get("macro_magnet") or "BALANCED"

        log.info(f"[GOLD] 🧠 Calling 5M AI Gatekeeper for {setup['name']} ({setup['action']})...")
        sys_p = (
            "You are a high-precision Gold (XAUTUSD) 5M Entry Gatekeeper. "
            "Validate Python's triggered entry using these strict institutional rules: "
            "1. For Liquidity Sweeps (BIG_SR_SWEEP, BIG_SR_MSS_SWEEP, SESSION_RANGE_SWEEP, S2_PDH_SWEEP, USER_3SWING_INDUCEMENT_TRAP): "
            "confirm clean wick rejection (>=35%) back inside the swept level and sufficient headroom before any opposing [1D/4H] wall. "
            "2. For Pullback Retests (TREND_PULLBACK_RETEST): confirm price retested the broken expansion level in alignment with trend. "
            "3. Reject mid-range chop entries or entries directly blocked by an opposing [1D/4H] wall. "
            'Respond ONLY in strictly valid JSON: {"approved": true/false, "confidence": "HIGH/MEDIUM/LOW", "view": "1 concise sentence"}'
        )
        usr_p = (
            f"Asset: GOLD (XAUTUSD) | Closed Bar: {closed_ts_str} | Entry Close: ${ltp:.2f}\n"
            f"Setup: {setup['name']} ({setup['tier']})\n"
            f"Action: {setup['action']} | Structural Invalidation Ref: ${setup['sl']:.2f} (Width: ${setup['risk']:.2f})\n"
            f"Trigger Logic: {setup['why']}\n"
            f"4H Trend: {trend_4h_label} (EMA20 ${float(ema20_4h):.2f}) | Macro Bias: {macro_bias} ({macro_regime} | Magnet: {macro_magnet})\n"
            f"5M Liquidity Pools: {pools_5m['summary']}\n"
            f"EMA50 Check: {setup['ema_note']}\n"
            f"HTF Wall Check: {setup['htf_wall_note']}"
        )

        ai_res, provider = call_ai_json(sys_p, usr_p, max_tokens=300, mode="fast_5m")

        if not ai_res or not ai_res.get("approved", False):
            reason = ai_res.get("view", "AI Disapproved") if ai_res else f"AI Unreachable ({provider})"
            log.warning(f"[GOLD] 🛑 Entry Blocked by AI Gatekeeper: {reason}")
            state.set(cd_key, time.time() + WAIT_COOLDOWN_MIN * 60, ttl=WAIT_COOLDOWN_MIN * 60)
            if SEND_AI_WAIT_TO_TG:
                wait_msg = f"""⏸️ <b>[GOLD - AI WAIT / FILTERED] | {datetime.now(IST).strftime('%H:%M IST')}</b>
⚡ <b>Candidate:</b> <code>{esc(setup['name'])}</code> ({esc(setup['action'])}) @ <b>${setup['entry']:.2f}</b>
🧠 <b>AI ({esc(provider)}):</b> <i>{esc(reason)}</i>"""
                tg_send(wait_msg)
            return

        state.set(cd_key, time.time() + (SIGNAL_COOLDOWN_MIN * 60), ttl=SIGNAL_COOLDOWN_MIN * 60)

        em = "🟢" if "BUY" in setup["action"] else "🔴"
        action_title = "BUY (LONG)" if "BUY" in setup["action"] else "SELL (SHORT)"
        ai_view = ai_res.get("view", "Institutional Gold liquidity sweep & order flow confirmed.")
        ai_conf = ai_res.get("confidence", "HIGH")

        msg = f"""{em} <b>[GOLD - XAUTUSD] {action_title} ENTRY | {datetime.now(IST).strftime('%H:%M IST')}</b>

⚡ <b>Setup     :</b> <code>{esc(setup['name'])}</code>
🏆 <b>Tier      :</b> {esc(setup['tier'])}
🎯 <b>Entry     :</b> <b>${setup['entry']:.2f}</b>

📐 <b>Structure Reference (Manual SL / Target):</b>
  • Sweep / Retest Invalidation: <code>${setup['sl']:.2f}</code> (Width: <code>${setup['risk']:.2f}</code>)
  • Safe Trail Guide (1.5R)   : <code>${setup['trail_15r']:.2f}</code>
  • Rally Trail Guide (2.5R)  : <code>${setup['rally_25r']:.2f}</code>

🗺️ <b>Context   :</b> 4H <b>{trend_4h_label}</b> | Macro: <b>{esc(macro_bias)}</b> ({esc(macro_regime)})
🧱 <b>Headroom  :</b> {esc(setup['htf_wall_note'])}
📝 <b>Logic     :</b> {esc(setup['why'])}
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
    Backward-compatible 6-Hour Macro entrypoint (used if main.py invokes it directly).
    Deduplicated via state slot lock so it never double-fires alongside the background scheduler.
    """
    now = datetime.now(IST)
    slot = f"macro_slot_{now.strftime('%Y-%m-%d')}_{now.hour}"
    is_2330 = (now.hour == 23)
    execute_mtf_macro_analysis(
        shared_store=shared_store,
        include_1d=is_2330,
        retry_until_success=is_2330,
        slot_tag=slot,
    )


def run_gold_30m_tick(shared_store=None):
    run_gold_cycle(shared_store)


def run_gold_1m_tick(shared_store=None):
    run_gold_cycle(shared_store)


if __name__ == "__main__":
    log.info("[GOLD] Starting Gold Institutional Liquidity Engine v9.5 on Koyeb...")
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()

    state_init = GoldState()
    start_macro_background_scheduler(state_init.store)
    run_startup_diagnostics(state_init)

    log.info("[GOLD] Starting 30-Second Continuous Polling Loop...")
    while True:
        try:
            run_gold_cycle()
        except Exception as e:
            log.error(f"[GOLD] Loop error: {type(e).__name__}")
        time.sleep(30)
