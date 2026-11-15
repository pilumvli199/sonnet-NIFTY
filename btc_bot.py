"""
╔══════════════════════════════════════════════════════════════════════════╗
║   BITCOIN (BTCUSD) INSTITUTIONAL LIQUIDITY ENGINE — V9.5                 ║
║   Namespace: BTC:*  |  Delta Exchange Public API (5M Sniper Execution)   ║
║                                                                          ║
║   100% PURE FORMAL LIQUIDITY ARCHITECTURE (ENTRY-ONLY IDENTIFICATION):   ║
║   1. BIG_SR_SWEEP [1W, 1D, 4H, 1H] (First-Touch Un-swept Pool Sweep)     ║
║      - 1W (PWH/PWL + Swings), 1D (PDH/PDL + Swings), 4H, 1H (With-Trend) ║
║   2. TUNED_3SWING_INDUCEMENT_TRAP (w=4 Fractal + Trend/Big S/R + <=$160) ║
║   3. FRESH_30M_EQH_EQL_HUNT (Fresh <=30m Equal Highs/Lows Sweep)         ║
║                                                                          ║
║   REMOVED NOISE SETUPS:                                                  ║
║   • S3_BREAKOUT_OPT, 5M_VOLUME_EXPANSION_BLAST, PDH_PDL_BREAKOUT,        ║
║     TREND_PULLBACK_RETEST, IST-Midnight SESSION_RANGE_SWEEP              ║
║                                                                          ║
║   DUAL-TIER AI + NON-BLOCKING HYDROGEN CONFIRMATION:                     ║
║   • Tier-1 (Heavy Macro & Post-Entry Confirmation):                      ║
║     gemini-4-hydrogen (HIGH) -> gemini-3.8-flash (HIGH) ->               ║
║     gemini-3.6-flash (OFF) -> gemini-3.5-flash-lite (HIGH) -> Claude     ║
║   • Tier-2 (5M Live Entry Gatekeeper — Zero Latency):                    ║
║     gemini-3.5-flash-lite (OFF) -> gemini-3.1-flash-lite (OFF) -> Claude ║
║   • Generous Timeouts (No premature cutoff; fallback only on true fail)  ║
║   • 23:30 IST Dedicated Macro Thread (2-Min Auto-Retry for Hydrogen)     ║
║   • 3x Intraday Sync (05:30, 11:30, 17:30 IST) + Shared Duplicate Lock   ║
║   • Spent Zone Lock + 60-Min Opposite-Direction Lock + Manual SL/TP Ref  ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import time
import html
import logging
import threading
import requests
import numpy as np
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
GEMINI_API_KEYS     = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip() and "your_" not in k.lower()]
ANTHROPIC_KEY       = os.getenv("ANTHROPIC_API_KEY", "").strip()
if "your_" in ANTHROPIC_KEY.lower():
    ANTHROPIC_KEY = ""

TG_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID          = os.getenv("BTC_TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", "")).strip()

DELTA_BASE_URL      = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange")
DELTA_API_KEY       = os.getenv("DELTA_API_KEY", "").strip()
if "your_" in DELTA_API_KEY.lower():
    DELTA_API_KEY = ""
DELTA_API_SECRET    = os.getenv("DELTA_API_SECRET", "").strip()

BTC_SYMBOL          = os.getenv("BTC_SYMBOL", "BTCUSD")
PORT                = int(os.getenv("PORT", "8080"))

# Cooldowns & Locks
WAIT_COOLDOWN_MIN   = int(os.getenv("BTC_WAIT_COOLDOWN_MIN", "15"))
SETUP_COOLDOWN_MIN  = int(os.getenv("BTC_SETUP_COOLDOWN_MIN", "90"))
OPPOSITE_LOCK_MIN   = int(os.getenv("BTC_OPPOSITE_LOCK_MIN", "60"))

# AI Models (Configurable via Env Vars with Benchmark-Validated Defaults)
HYDROGEN_MODEL      = os.getenv("GEMINI_HYDROGEN_MODEL", "gemini-4-hydrogen").strip()
FLASH_38_MODEL      = os.getenv("GEMINI_38_MODEL", "gemini-3.8-flash").strip()
FLASH_36_MODEL      = os.getenv("GEMINI_36_MODEL", "gemini-3.6-flash").strip()
LITE_35_MODEL       = os.getenv("GEMINI_35_LITE_MODEL", "gemini-3.5-flash-lite").strip()
LITE_31_MODEL       = os.getenv("GEMINI_31_LITE_MODEL", "gemini-3.1-flash-lite").strip()
CLAUDE_BACKUP_MODEL = os.getenv("CLAUDE_BACKUP_MODEL", "claude-haiku-5-5").strip()

# Tier-1 Heavy Chain: (model_name, thinking_level, generous_timeout_sec)
HEAVY_AI_CHAIN = [
    (HYDROGEN_MODEL, "HIGH", 300),
    (FLASH_38_MODEL, "HIGH", 300),
    (FLASH_36_MODEL, "OFF",  240),
    (LITE_35_MODEL,  "HIGH", 90),
]

# Tier-2 Fast 5M Gatekeeper Chain: (model_name, thinking_level, timeout_sec)
FAST_5M_AI_CHAIN = [
    (LITE_35_MODEL, "OFF", 35),
    (LITE_31_MODEL, "OFF", 35),
]

_btc_lock = threading.Lock()
_ai_rate_lock = threading.Lock()
_gemini_idx = 0
_last_gemini_call = 0.0
_daily_ai_call_count = 0
_daily_ai_date = None
_last_daily_report_date = None


def esc(text):
    return html.escape(str(text), quote=False) if text is not None else ""


# ==============================================================================
# IN-MEMORY / SHARED STATE STORE (WITH REDIS COMPATIBILITY)
# ==============================================================================
class DefaultStore:
    def __init__(self):
        self._data = {}
        self._exp = {}
        self._lock = threading.Lock()

    def set(self, key, value, ttl=86400):
        with self._lock:
            self._data[key] = value
            self._exp[key] = time.time() + ttl

    def get(self, key):
        with self._lock:
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

    def mark_spent(self, bucket: str, item_id: str, ttl=172800):
        spent = self.get(bucket) or []
        if item_id not in spent:
            spent.append(item_id)
            if len(spent) > 300:
                spent = spent[-300:]
            self.set(bucket, spent, ttl=ttl)

    def is_spent(self, bucket: str, item_id: str) -> bool:
        spent = self.get(bucket) or []
        return item_id in spent


# ==============================================================================
# KOYEB HTTP HEALTH CHECK SERVER (FOR UPTIMEROBOT 24/7 PING)
# ==============================================================================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - BTC Institutional Liquidity Engine v9.5 Active\n")

    def log_message(self, format, *args):
        return


def start_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
        log.info(f"[BTC] Health Check HTTP server listening on port {PORT} for UptimeRobot")
        server.serve_forever()
    except Exception as e:
        log.error(f"[BTC] HTTP health server error: {type(e).__name__}")


# ==============================================================================
# DUAL-TIER AI ENGINE (GENEROUS TIMEOUTS + THINKING CONTROL + CLAUDE BACKUP)
# ==============================================================================
def _bump_ai_counter():
    global _daily_ai_call_count, _daily_ai_date
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    if _daily_ai_date != today_str:
        _daily_ai_date = today_str
        _daily_ai_call_count = 0
    _daily_ai_call_count += 1


def _parse_json_response(raw_text: str):
    if not raw_text:
        return None
    clean = raw_text.strip().replace("```json", "").replace("```", "").strip()
    s, e = clean.find("{"), clean.rfind("}") + 1
    if s != -1 and e > s:
        return json.loads(clean[s:e])
    return None


def _call_single_gemini_model(model_name: str, thinking_level: str, timeout_sec: int, system_prompt: str, user_prompt: str, max_tokens: int):
    global _gemini_idx, _last_gemini_call
    if not GEMINI_API_KEYS:
        return None

    for _ in range(len(GEMINI_API_KEYS)):
        with _ai_rate_lock:
            now = time.time()
            elapsed = now - _last_gemini_call
            if elapsed < 4.2:
                time.sleep(4.2 - elapsed)
            _last_gemini_call = time.time()
            api_key = GEMINI_API_KEYS[_gemini_idx % len(GEMINI_API_KEYS)]
            _gemini_idx += 1
            _bump_ai_counter()

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        gen_cfg = {
            "temperature": 0.0,
            "maxOutputTokens": max_tokens,
            "responseMimeType": "application/json",
        }
        if thinking_level:
            gen_cfg["thinkingConfig"] = {"thinkingLevel": thinking_level}

        payload = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
            "generationConfig": gen_cfg,
        }

        try:
            r = requests.post(url, json=payload, timeout=timeout_sec)

            # Schema fallback: if a model variant rejects thinkingConfig with HTTP 400, retry once without thinkingConfig
            if r.status_code == 400 and "thinking" in r.text.lower() and "thinkingConfig" in gen_cfg:
                gen_cfg.pop("thinkingConfig", None)
                r = requests.post(url, json=payload, timeout=timeout_sec)

            if r.status_code == 429:
                log.warning(f"[BTC] {model_name} HTTP 429 rate limit — switching key/model...")
                continue
            if r.status_code in (500, 502, 503, 504):
                log.warning(f"[BTC] {model_name} HTTP {r.status_code} server busy — falling back to next model...")
                break
            if r.status_code == 200:
                candidates = r.json().get("candidates", [])
                if candidates:
                    parts = candidates[0].get("content", {}).get("parts", [])
                    # Skip thought parts if returned separately and pick text output
                    text_parts = [p.get("text", "") for p in parts if not p.get("thought", False) and p.get("text")]
                    raw = "\n".join(text_parts) if text_parts else (parts[0].get("text", "") if parts else "")
                    parsed = _parse_json_response(raw)
                    if parsed is not None:
                        return parsed
            else:
                log.warning(f"[BTC] {model_name} returned HTTP {r.status_code}")
                break
        except requests.Timeout:
            log.warning(f"[BTC] {model_name} timed out after {timeout_sec}s — falling back...")
            break
        except Exception as e:
            log.warning(f"[BTC] {model_name} error ({type(e).__name__}) — falling back...")
            break

    return None


def _call_claude_backup(system_prompt: str, user_prompt: str, max_tokens: int):
    if not ANTHROPIC_KEY:
        return None, None
    models_to_try = [CLAUDE_BACKUP_MODEL]
    if CLAUDE_BACKUP_MODEL != "claude-haiku-4-5-20251001":
        models_to_try.append("claude-haiku-4-5-20251001")

    for c_model in models_to_try:
        hdrs = {
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        body = {
            "model": c_model,
            "max_tokens": max_tokens,
            "temperature": 0,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        try:
            _bump_ai_counter()
            r = requests.post("https://api.anthropic.com/v1/messages", headers=hdrs, json=body, timeout=60)
            if r.status_code == 200:
                raw = r.json()["content"][0]["text"]
                parsed = _parse_json_response(raw)
                if parsed is not None:
                    return parsed, f"CLAUDE ({c_model})"
        except Exception as e:
            log.warning(f"[BTC] Claude backup ({c_model}) error: {type(e).__name__}")
    return None, None


def call_ai_chain(chain: list, system_prompt: str, user_prompt: str, max_tokens: int = 750):
    """Executes the ordered AI model chain with configured thinking levels and Claude failover."""
    for model_name, thinking_lvl, timeout_sec in chain:
        res = _call_single_gemini_model(model_name, thinking_lvl, timeout_sec, system_prompt, user_prompt, max_tokens)
        if res is not None:
            tag = f"{model_name} [{thinking_lvl}]"
            return res, tag

    claude_res, claude_tag = _call_claude_backup(system_prompt, user_prompt, max_tokens)
    if claude_res is not None:
        return claude_res, claude_tag

    return None, "ALL_MODELS_UNREACHABLE"


# ==============================================================================
# DELTA EXCHANGE CANDLES FETCHER (WITH UNCLOSED CANDLE GUARD FOR ALL TFs)
# ==============================================================================
def fetch_delta_candles(symbol: str, resolution: str, hours_back: int, closed_only: bool = True):
    end_ts = int(time.time())
    start_ts = end_ts - (hours_back * 3600)
    url = f"{DELTA_BASE_URL}/v2/history/candles"
    params = {"symbol": symbol, "resolution": resolution, "start": start_ts, "end": end_ts}
    res_sec = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "1d": 86400,
        "1w": 604800,
    }.get(resolution, 300)

    headers = {}
    if DELTA_API_KEY:
        headers["api-key"] = DELTA_API_KEY

    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        if r.status_code == 200:
            res = r.json().get("result", [])
            if not res:
                return pd.DataFrame()
            df = pd.DataFrame(res)
            df["time"] = df["time"].astype(int)
            df["ts_utc"] = pd.to_datetime(df["time"], unit="s", utc=True)
            df["ts"] = df["ts_utc"].dt.tz_convert(IST)
            df = df.rename(columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
            df[["o", "h", "l", "c"]] = df[["o", "h", "l", "c"]].astype(float).round(1)
            df["v"] = df["v"].astype(float)
            df = df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)

            if closed_only and not df.empty:
                last_open_ts = int(df.iloc[-1]["time"])
                if end_ts < (last_open_ts + res_sec):
                    df = df.iloc[:-1].reset_index(drop=True)
            return df
    except Exception as e:
        log.error(f"[BTC] Delta candle fetch error ({resolution}): {type(e).__name__}")
    return pd.DataFrame()


def tg_send(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info(f"[BTC-TG-DRY]\n{text}\n")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=15,
        )
        if r.status_code != 200:
            log.warning(f"[BTC] Telegram HTTP {r.status_code}")
            return False
        return True
    except Exception as e:
        log.error(f"[BTC] Telegram error: {type(e).__name__}")
        return False


# ==============================================================================
# MULTI-TF FORMAL LIQUIDITY POOL & EQH/EQL ENGINE (1W, 1D, 4H, 1H)
# ==============================================================================
def compute_indicators(df: pd.DataFrame):
    df_new = df.copy()
    df_new["ema20"] = df_new["c"].ewm(span=20, adjust=False).mean()
    df_new["ema50"] = df_new["c"].ewm(span=50, adjust=False).mean()
    return df_new


def _extract_tf_pools(df_tf: pd.DataFrame, tf_name: str, w: int, eq_tol: float, max_bars_back: int):
    pools = []
    if df_tf.empty or len(df_tf) < (2 * w + 3):
        return pools

    sub = df_tf.tail(max_bars_back).reset_index(drop=True)
    hh = sub["h"].values
    ll = sub["l"].values
    tt = sub["time"].values
    m = len(sub)

    recent_highs = []
    recent_lows = []

    for idx in range(w, m - w):
        bar_t = int(tt[idx])
        # Confirmed Fractal Swing High (Buy-Side Liquidity - BSL)
        if hh[idx] >= np.max(hh[idx - w:idx]) and hh[idx] >= np.max(hh[idx + 1:idx + w + 1]):
            lvl = float(hh[idx])
            # Ensure no subsequent closed bar in this TF already swept/broke it
            if np.max(hh[idx + 1:]) <= lvl:
                is_eq = any(abs(lvl - p["pivot"]) <= eq_tol for p in recent_highs[-10:])
                pool = {
                    "id": f"{tf_name}_BSL_{int(lvl)}_{bar_t}",
                    "tf": tf_name,
                    "side": "BSL",  # Above price -> Sweep triggers SELL (SHORT)
                    "pivot": lvl,
                    "is_eq": is_eq,
                    "why": f"{tf_name} {'EQH' if is_eq else 'Swing High'} BSL (${lvl:,.1f})",
                }
                pools.append(pool)
                recent_highs.append(pool)

        # Confirmed Fractal Swing Low (Sell-Side Liquidity - SSL)
        if ll[idx] <= np.min(ll[idx - w:idx]) and ll[idx] <= np.min(ll[idx + 1:idx + w + 1]):
            lvl = float(ll[idx])
            if np.min(ll[idx + 1:]) >= lvl:
                is_eq = any(abs(lvl - p["pivot"]) <= eq_tol for p in recent_lows[-10:])
                pool = {
                    "id": f"{tf_name}_SSL_{int(lvl)}_{bar_t}",
                    "tf": tf_name,
                    "side": "SSL",  # Below price -> Sweep triggers BUY (LONG)
                    "pivot": lvl,
                    "is_eq": is_eq,
                    "why": f"{tf_name} {'EQL' if is_eq else 'Swing Low'} SSL (${lvl:,.1f})",
                }
                pools.append(pool)
                recent_lows.append(pool)

    return pools


def sync_htf_liquidity_pools(state: BtcState, include_1d_1w: bool = True):
    """
    Syncs formal un-swept liquidity pools across 1W, 1D (180d), 4H (30d), and 1H (7d),
    calculates 6H/4H trend alignment, and counts EQH (BSL) vs EQL (SSL) pools.
    """
    df_1h = fetch_delta_candles(BTC_SYMBOL, "1h", hours_back=240, closed_only=True)
    df_4h = fetch_delta_candles(BTC_SYMBOL, "4h", hours_back=720, closed_only=True)

    if df_1h.empty or df_4h.empty:
        log.warning("[BTC] HTF sync skipped: 1H or 4H candles empty")
        return None

    df_1h = compute_indicators(df_1h)
    df_4h = compute_indicators(df_4h)

    # Build 6H closed structure from 1H closed bars (aligned to 00:00, 06:00, 12:00, 18:00 UTC)
    df_6h = (
        df_1h.set_index("ts_utc")
        .resample("6h")
        .agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"})
        .dropna()
        .reset_index()
    )
    df_6h["ema20"] = df_6h["c"].ewm(span=20, adjust=False).mean()

    ltp = float(df_1h["c"].iloc[-1])
    ema20_6h = float(df_6h["ema20"].iloc[-1]) if not df_6h.empty else ltp
    ema50_4h = float(df_4h["ema50"].iloc[-1])
    t6 = "BULLISH" if ltp > ema20_6h else "BEARISH"
    t4 = "BULLISH" if ltp > ema50_4h else "BEARISH"

    if t6 == "BULLISH" and t4 == "BULLISH":
        struct_bias = "BULLISH"
    elif t6 == "BEARISH" and t4 == "BEARISH":
        struct_bias = "BEARISH"
    else:
        struct_bias = "RANGE"

    # Extract 1H & 4H formal pools
    pools_1h = _extract_tf_pools(df_1h, "1H", w=2, eq_tol=50.0, max_bars_back=72)
    pools_4h = _extract_tf_pools(df_4h, "4H", w=2, eq_tol=90.0, max_bars_back=180)

    # Preserve existing 1D/1W pools if intraday sync, or refresh full 180d 1D + 1W
    existing_pools = state.get("htf_pools") or []
    pools_1d_1w = [p for p in existing_pools if p.get("tf") in ("1D", "1W")]

    pdh, pdl, pwh, pwl = 0.0, 0.0, 0.0, 0.0
    if include_1d_1w or not pools_1d_1w:
        df_1d = fetch_delta_candles(BTC_SYMBOL, "1d", hours_back=4320, closed_only=True)  # 180 days
        if not df_1d.empty and len(df_1d) >= 5:
            pools_1d_1w = _extract_tf_pools(df_1d, "1D", w=1, eq_tol=150.0, max_bars_back=180)
            pdh = float(df_1d["h"].iloc[-1])
            pdl = float(df_1d["l"].iloc[-1])
            pd_t = int(df_1d["time"].iloc[-1])
            pools_1d_1w.append({"id": f"1D_PDH_{int(pdh)}_{pd_t}", "tf": "1D", "side": "BSL", "pivot": pdh, "is_eq": False, "why": f"Previous Day High PDH (${pdh:,.1f})"})
            pools_1d_1w.append({"id": f"1D_PDL_{int(pdl)}_{pd_t}", "tf": "1D", "side": "SSL", "pivot": pdl, "is_eq": False, "why": f"Previous Day Low PDL (${pdl:,.1f})"})

            # Resample Weekly (1W) from 1D
            df_1w = (
                df_1d.set_index("ts_utc")
                .resample("1W")
                .agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"})
                .dropna()
                .reset_index()
            )
            if len(df_1w) >= 3:
                df_1w["time"] = (df_1w["ts_utc"].astype("int64") // 10**9).astype(int)
                pools_1w = _extract_tf_pools(df_1w, "1W", w=1, eq_tol=250.0, max_bars_back=26)
                pwh = float(df_1w["h"].iloc[-2]) if len(df_1w) >= 2 else float(df_1w["h"].iloc[-1])
                pwl = float(df_1w["l"].iloc[-2]) if len(df_1w) >= 2 else float(df_1w["l"].iloc[-1])
                pw_t = int(df_1w["time"].iloc[-1])
                pools_1w.append({"id": f"1W_PWH_{int(pwh)}_{pw_t}", "tf": "1W", "side": "BSL", "pivot": pwh, "is_eq": False, "why": f"Previous Week High PWH (${pwh:,.1f})"})
                pools_1w.append({"id": f"1W_PWL_{int(pwl)}_{pw_t}", "tf": "1W", "side": "SSL", "pivot": pwl, "is_eq": False, "why": f"Previous Week Low PWL (${pwl:,.1f})"})
                pools_1d_1w.extend(pools_1w)

    # Combine & deduplicate with priority: 1W > 1D > 4H > 1H
    tf_prio = {"1W": 4, "1D": 3, "4H": 2, "1H": 1}
    raw_all = pools_1d_1w + pools_4h + pools_1h
    raw_all.sort(key=lambda z: (tf_prio.get(z["tf"], 0), -abs(z["pivot"] - ltp)), reverse=True)

    dedup = []
    for z in raw_all:
        if state.is_spent("spent_pools", z["id"]):
            continue
        # Keep only pools within $4,500 of LTP
        if abs(z["pivot"] - ltp) > 4500.0:
            continue
        # Deduplicate clusters within $80 of a higher-timeframe pool
        if not any(abs(z["pivot"] - ez["pivot"]) <= 80.0 and z["side"] == ez["side"] for ez in dedup):
            dedup.append(z)

    # Count EQH (BSL above LTP) and EQL (SSL below LTP)
    bsl_pools = sorted([z for z in dedup if z["side"] == "BSL" and z["pivot"] >= ltp], key=lambda x: x["pivot"])
    ssl_pools = sorted([z for z in dedup if z["side"] == "SSL" and z["pivot"] <= ltp], key=lambda x: x["pivot"], reverse=True)
    eqh_count = sum(1 for z in bsl_pools if z.get("is_eq"))
    eql_count = sum(1 for z in ssl_pools if z.get("is_eq"))

    high_24h = float(df_1h.tail(24)["h"].max())
    low_24h = float(df_1h.tail(24)["l"].min())

    macro_snapshot = {
        "ltp": ltp,
        "t6": t6,
        "ema20_6h": round(ema20_6h, 1),
        "t4": t4,
        "ema50_4h": round(ema50_4h, 1),
        "struct_bias": struct_bias,
        "pdh": pdh,
        "pdl": pdl,
        "high_24h": high_24h,
        "low_24h": low_24h,
        "bsl_count": len(bsl_pools),
        "ssl_count": len(ssl_pools),
        "eqh_count": eqh_count,
        "eql_count": eql_count,
        "nearest_bsl": bsl_pools[:4],
        "nearest_ssl": ssl_pools[:4],
    }

    state.set("htf_pools", dedup[:25], ttl=86400)
    state.set("macro_snapshot", macro_snapshot, ttl=86400)
    return macro_snapshot


# ==============================================================================
# TIER-1 HEAVY MACRO AUDIT + 23:30 IST DEDICATED BACKGROUND THREAD
# ==============================================================================
MACRO_SYS_PROMPT = (
    "You are the Lead Institutional BTCUSD Liquidity & Market Structure Analyst.\n"
    "Analyze the 6H/4H trend alignment and un-swept 1W/1D/4H/1H liquidity pools (BSL above vs SSL below) plus EQH/EQL clusters.\n"
    "Determine macro directional bias and which 1H liquidity sweeps are safe for the next 6 hours.\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"macro_bias": "BULLISH|BEARISH|RANGE", "allowed_1h_sweep": "LONG_ONLY|SHORT_ONLY|BOTH", '
    '"liquidity_magnet": "Nearest major un-swept pool price & side", "assessment": "1-2 crisp sentences"}'
)


def _build_macro_user_prompt(snap: dict, now_ist: datetime) -> str:
    bsl_str = ", ".join([f"{z['why']}" for z in snap.get("nearest_bsl", [])[:3]]) or "None nearby"
    ssl_str = ", ".join([f"{z['why']}" for z in snap.get("nearest_ssl", [])[:3]]) or "None nearby"
    return (
        f"Asset: BTCUSD | Time: {now_ist.strftime('%Y-%m-%d %H:%M IST')} | LTP: ${snap['ltp']:,.1f}\n"
        f"6H Trend (EMA20): {snap['t6']} (${snap['ema20_6h']:,.1f}) | 4H Trend (EMA50): {snap['t4']} (${snap['ema50_4h']:,.1f})\n"
        f"Structural Regime: {snap['struct_bias']} | 24h Range: ${snap['low_24h']:,.1f} - ${snap['high_24h']:,.1f}\n"
        f"Liquidity Pool Counter: BSL Above = {snap['bsl_count']} (EQH: {snap['eqh_count']}) | SSL Below = {snap['ssl_count']} (EQL: {snap['eql_count']})\n"
        f"Nearest Overhead BSL Pools: {bsl_str}\n"
        f"Nearest Below SSL Pools: {ssl_str}"
    )


def _hydrogen_macro_retry_worker(shared_store, slot_key: str, snap: dict):
    """Background retry worker every 2 mins until gemini-4-hydrogen confirms the 23:30 IST macro map."""
    for attempt in range(1, 16):
        time.sleep(120)
        state = BtcState(shared_store)
        if state.get(f"{slot_key}_hydrogen_ok"):
            return
        now = datetime.now(IST)
        usr_p = _build_macro_user_prompt(snap, now)
        res = _call_single_gemini_model(HYDROGEN_MODEL, "HIGH", 300, MACRO_SYS_PROMPT, usr_p, 500)
        if res is not None:
            state.set(f"{slot_key}_hydrogen_ok", True, ttl=43200)
            state.set("macro_bias", res.get("macro_bias", snap["struct_bias"]), ttl=25200)
            state.set("allowed_1h_sweep", res.get("allowed_1h_sweep", "BOTH"), ttl=25200)
            msg = (
                f"🧠 <b>[HYDROGEN MACRO CONFIRMATION | {now.strftime('%H:%M IST')}]</b>\n\n"
                f"🏛️ <b>Bias:</b> <b>{esc(res.get('macro_bias'))}</b> | <b>1H Rule:</b> <code>{esc(res.get('allowed_1h_sweep'))}</code>\n"
                f"🧲 <b>Liquidity Magnet:</b> <code>{esc(res.get('liquidity_magnet'))}</code>\n"
                f"<i>{esc(res.get('assessment'))}</i>"
            )
            tg_send(msg)
            log.info(f"[BTC] ✅ Hydrogen background macro confirmation succeeded on retry #{attempt}")
            return


def _execute_macro_sync_worker(shared_store, is_daily_2330: bool, slot_key: str):
    state = BtcState(shared_store)
    now = datetime.now(IST)
    snap = sync_htf_liquidity_pools(state, include_1d_1w=is_daily_2330)
    if not snap:
        return

    usr_p = _build_macro_user_prompt(snap, now)
    ai_res, provider = call_ai_chain(HEAVY_AI_CHAIN, MACRO_SYS_PROMPT, usr_p, max_tokens=500)

    if not ai_res:
        # Deterministic fallback to pure 6H/4H structure so live engine never stalls
        ai_bias = snap["struct_bias"]
        allowed_1h = "LONG_ONLY" if ai_bias == "BULLISH" else ("SHORT_ONLY" if ai_bias == "BEARISH" else "BOTH")
        ai_text = f"6H ({snap['t6']}) & 4H ({snap['t4']}) structure active; AI heavy chain unreachable."
        magnet = snap["nearest_bsl"][0]["why"] if ai_bias == "BULLISH" and snap["nearest_bsl"] else (snap["nearest_ssl"][0]["why"] if snap["nearest_ssl"] else "N/A")
        provider = "STRUCTURE_FALLBACK"
    else:
        ai_bias = ai_res.get("macro_bias", snap["struct_bias"])
        allowed_1h = ai_res.get("allowed_1h_sweep", "BOTH")
        ai_text = ai_res.get("assessment", "")
        magnet = ai_res.get("liquidity_magnet", "N/A")

    state.set("macro_bias", ai_bias, ttl=25200)
    state.set("allowed_1h_sweep", allowed_1h, ttl=25200)

    title = "23:30 IST DAILY 1D+4H+1H LIQUIDITY MAP" if is_daily_2330 else f"INTRADAY 4H-1H LIQUIDITY SYNC ({now.strftime('%H:%M IST')})"
    bsl_lines = [f"• <b>BSL:</b> {esc(z['why'])}" for z in snap.get("nearest_bsl", [])[:2]]
    ssl_lines = [f"• <b>SSL:</b> {esc(z['why'])}" for z in snap.get("nearest_ssl", [])[:2]]

    msg = f"""🏛️ <b>[BITCOIN] {title}</b>

📊 <b>LTP      :</b> <b>${snap['ltp']:,.1f}</b> | <b>Bias:</b> <b>{esc(ai_bias)}</b>
🧭 <b>Structure:</b> <code>6H: {snap['t6']} | 4H: {snap['t4']} | 1H Rule: {esc(allowed_1h)}</code>
💧 <b>Pool Map :</b> <code>BSL Above: {snap['bsl_count']} (EQH:{snap['eqh_count']}) | SSL Below: {snap['ssl_count']} (EQL:{snap['eql_count']})</code>

🗺️ <b>Nearest Un-Swept HTF Liquidity:</b>
{chr(10).join(bsl_lines + ssl_lines) if (bsl_lines or ssl_lines) else '• None within $4,500'}

🧲 <b>Magnet   :</b> <code>{esc(magnet)}</code>
🧠 <b>AI ({esc(provider)}):</b> <i>{esc(ai_text)}</i>"""

    tg_send(msg)
    log.info(f"[BTC] 🏛️ Macro sync published via {provider} (Bias: {ai_bias})")

    # If 23:30 IST daily job did NOT get gemini-4-hydrogen on first pass, auto-retry Hydrogen every 2 mins in background
    if is_daily_2330:
        if HYDROGEN_MODEL in provider:
            state.set(f"{slot_key}_hydrogen_ok", True, ttl=43200)
        else:
            log.info(f"[BTC] Scheduling 2-min background auto-retry for {HYDROGEN_MODEL}...")
            t_retry = threading.Thread(
                target=_hydrogen_macro_retry_worker,
                args=(shared_store, slot_key, snap),
                daemon=True,
            )
            t_retry.start()


def trigger_macro_sync_if_due(shared_store=None, force: bool = False):
    """
    Runs 23:30 IST 1D+4H+1H job and 3x Intraday 4H-1H sync (05:30, 11:30, 17:30 IST)
    in a non-blocking background thread with shared deduplication lock.
    """
    state = BtcState(shared_store)
    now = datetime.now(IST)
    slot_hours = [5, 11, 17, 23]

    cur_hour = None
    if force:
        cur_hour = now.hour
    else:
        for h in slot_hours:
            if now.hour == h and 30 <= now.minute < 55:
                cur_hour = h
                break

    if cur_hour is None:
        return

    slot_key = f"macro_slot_done_{now.strftime('%Y-%m-%d')}_{cur_hour}"
    if not force and state.get(slot_key):
        return

    state.set(slot_key, True, ttl=21600)
    is_daily_2330 = (cur_hour == 23) or force
    t = threading.Thread(
        target=_execute_macro_sync_worker,
        args=(shared_store, is_daily_2330, slot_key),
        daemon=True,
    )
    t.start()


# ==============================================================================
# 3 VALIDATED PURE FORMAL LIQUIDITY SETUPS (ENTRY-ONLY IDENTIFICATION)
# ==============================================================================

# SETUP 1: BIG_SR_SWEEP [1W, 1D, 4H, 1H] (First-Touch Un-swept Pool Sweep)
def check_big_sr_and_1h_sweep(df_5m: pd.DataFrame, state: BtcState, macro_bias: str, allowed_1h: str):
    if len(df_5m) < 10:
        return None

    pools = state.get("htf_pools") or []
    if not pools:
        return None

    i = len(df_5m) - 1
    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    rng = max(h - l, 1.0)
    upper_wick = (h - max(o, c)) / rng
    lower_wick = (min(o, c) - l) / rng

    tf_prio = {"1W": 4, "1D": 3, "4H": 2, "1H": 1}
    candidates = []
    pools_updated = False

    for p in pools:
        pid = p["id"]
        if state.is_spent("spent_pools", pid):
            continue

        lvl = float(p["pivot"])
        tf = p["tf"]

        # Buy-Side Liquidity (BSL / High) pierced -> First-touch consumes pool
        if p["side"] == "BSL" and h > lvl:
            state.mark_spent("spent_pools", pid)
            pools_updated = True
            if c < lvl and upper_wick >= 0.30 and c <= o:
                # 1H sweep allowed only when aligned with macro trend or RANGE
                if tf == "1H" and (macro_bias == "BULLISH" or allowed_1h == "LONG_ONLY"):
                    continue
                sweep_depth = round((h + 25.0) - c, 1)
                if 60.0 <= sweep_depth <= 380.0:
                    candidates.append((
                        tf_prio.get(tf, 1),
                        {
                            "name": f"BIG_SR_SWEEP [{tf}]",
                            "action": "SELL_SHORT",
                            "tier": f"Tier-1 {tf} Liquidity Pool Sweep (First-Touch)",
                            "entry": c,
                            "wick_extreme": h,
                            "invalidation_ref": round(h + 25.0, 1),
                            "sweep_depth": sweep_depth,
                            "wick_pct": int(upper_wick * 100),
                            "swept_tf": tf,
                            "swept_level": lvl,
                            "why": f"Swept {p['why']} with {int(upper_wick * 100)}% rejection wick & closed inside at ${c:,.1f}",
                        },
                    ))

        # Sell-Side Liquidity (SSL / Low) pierced -> First-touch consumes pool
        elif p["side"] == "SSL" and l < lvl:
            state.mark_spent("spent_pools", pid)
            pools_updated = True
            if c > lvl and lower_wick >= 0.30 and c >= o:
                if tf == "1H" and (macro_bias == "BEARISH" or allowed_1h == "SHORT_ONLY"):
                    continue
                sweep_depth = round(c - (l - 25.0), 1)
                if 60.0 <= sweep_depth <= 380.0:
                    candidates.append((
                        tf_prio.get(tf, 1),
                        {
                            "name": f"BIG_SR_SWEEP [{tf}]",
                            "action": "BUY_LONG",
                            "tier": f"Tier-1 {tf} Liquidity Pool Sweep (First-Touch)",
                            "entry": c,
                            "wick_extreme": l,
                            "invalidation_ref": round(l - 25.0, 1),
                            "sweep_depth": sweep_depth,
                            "wick_pct": int(lower_wick * 100),
                            "swept_tf": tf,
                            "swept_level": lvl,
                            "why": f"Swept {p['why']} with {int(lower_wick * 100)}% absorption wick & closed inside at ${c:,.1f}",
                        },
                    ))

    if pools_updated:
        active_remaining = [p for p in pools if not state.is_spent("spent_pools", p["id"])]
        state.set("htf_pools", active_remaining, ttl=86400)

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


# SETUP 2: TUNED_3SWING_INDUCEMENT_TRAP (w=4 Fractal + Trend/Big S/R + <=$160 + Spent Lock)
def _find_5m_swings(df: pd.DataFrame, w: int = 4):
    sh, sl = [], []
    hh = df["h"].values
    ll = df["l"].values
    tt = df["time"].values
    for idx in range(w, len(df) - w):
        hv, lv = float(hh[idx]), float(ll[idx])
        if hv >= np.max(hh[idx - w:idx]) and hv >= np.max(hh[idx + 1:idx + w + 1]):
            sh.append({"idx": idx, "price": hv, "time": int(tt[idx])})
        if lv <= np.min(ll[idx - w:idx]) and lv <= np.min(ll[idx + 1:idx + w + 1]):
            sl.append({"idx": idx, "price": lv, "time": int(tt[idx])})
    return sh, sl


def check_tuned_3swing_inducement(df_5m: pd.DataFrame, state: BtcState, macro_bias: str):
    if len(df_5m) < 35:
        return None

    i = len(df_5m) - 1
    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    rng = max(h - l, 1.0)
    upper_wick = (h - max(o, c)) / rng
    lower_wick = (min(o, c) - l) / rng

    pools = state.get("htf_pools") or []
    near_big_sr_high = any(p["tf"] in ("4H", "1D", "1W") and p["side"] == "BSL" and abs(h - float(p["pivot"])) <= 150.0 for p in pools)
    near_big_sr_low = any(p["tf"] in ("4H", "1D", "1W") and p["side"] == "SSL" and abs(l - float(p["pivot"])) <= 150.0 for p in pools)

    sw_h, sw_l = _find_5m_swings(df_5m, w=4)

    # Bearish Inducement Sweep (SELL_SHORT): Allowed in BEARISH macro bias OR near Big 4H/1D/1W BSL
    if upper_wick >= 0.35 and c < o and (macro_bias == "BEARISH" or near_big_sr_high):
        valid_sh = [s for s in sw_h if s["idx"] <= i - 6]
        if len(valid_sh) >= 2:
            rec = valid_sh[-6:]
            for idx2 in range(len(rec) - 1, -1, -1):
                s2 = rec[idx2]
                sw_id = f"3SW_H_{int(s2['price'])}_{s2['time']}"
                if state.is_spent("spent_3swing", sw_id):
                    continue
                if not (4 <= i - s2["idx"] <= 60) or not (h > s2["price"] and c < s2["price"]):
                    continue
                for idx1 in range(idx2 - 1, -1, -1):
                    s1 = rec[idx1]
                    if not (4 <= s2["idx"] - s1["idx"] <= 60):
                        continue
                    if s2["price"] < s1["price"] and h < (s1["price"] + 80.0):
                        sweep_depth = max(60.0, round((h + 25.0) - c, 1))
                        if sweep_depth <= 160.0:
                            state.mark_spent("spent_3swing", sw_id)
                            conf_note = "With 6H/4H Bearish Trend" if macro_bias == "BEARISH" else "At Big 4H/1D BSL Zone"
                            return {
                                "name": "TUNED_3SWING_INDUCEMENT_TRAP",
                                "action": "SELL_SHORT",
                                "tier": f"A+ SMC Inducement Sweep ({conf_note})",
                                "entry": c,
                                "wick_extreme": h,
                                "invalidation_ref": round(h + 25.0, 1),
                                "sweep_depth": sweep_depth,
                                "wick_pct": int(upper_wick * 100),
                                "swept_tf": "5M_SMC",
                                "swept_level": s2["price"],
                                "why": f"Swept Inducement High (${s2['price']:,.1f}) below Major High (${s1['price']:,.1f}) [{conf_note}]",
                            }

    # Bullish Inducement Sweep (BUY_LONG): Allowed in BULLISH macro bias OR near Big 4H/1D/1W SSL
    if lower_wick >= 0.35 and c > o and (macro_bias == "BULLISH" or near_big_sr_low):
        valid_sl = [s for s in sw_l if s["idx"] <= i - 6]
        if len(valid_sl) >= 2:
            rec = valid_sl[-6:]
            for idx2 in range(len(rec) - 1, -1, -1):
                s2 = rec[idx2]
                sw_id = f"3SW_L_{int(s2['price'])}_{s2['time']}"
                if state.is_spent("spent_3swing", sw_id):
                    continue
                if not (4 <= i - s2["idx"] <= 60) or not (l < s2["price"] and c > s2["price"]):
                    continue
                for idx1 in range(idx2 - 1, -1, -1):
                    s1 = rec[idx1]
                    if not (4 <= s2["idx"] - s1["idx"] <= 60):
                        continue
                    if s2["price"] > s1["price"] and l > (s1["price"] - 80.0):
                        sweep_depth = max(60.0, round(c - (l - 25.0), 1))
                        if sweep_depth <= 160.0:
                            state.mark_spent("spent_3swing", sw_id)
                            conf_note = "With 6H/4H Bullish Trend" if macro_bias == "BULLISH" else "At Big 4H/1D SSL Zone"
                            return {
                                "name": "TUNED_3SWING_INDUCEMENT_TRAP",
                                "action": "BUY_LONG",
                                "tier": f"A+ SMC Inducement Sweep ({conf_note})",
                                "entry": c,
                                "wick_extreme": l,
                                "invalidation_ref": round(l - 25.0, 1),
                                "sweep_depth": sweep_depth,
                                "wick_pct": int(lower_wick * 100),
                                "swept_tf": "5M_SMC",
                                "swept_level": s2["price"],
                                "why": f"Swept Inducement Low (${s2['price']:,.1f}) above Major Low (${s1['price']:,.1f}) [{conf_note}]",
                            }

    return None


# SETUP 3: FRESH_30M_EQH_EQL_HUNT (Fresh <=30m Equal Highs/Lows Sweep)
def check_fresh_30m_eqh_eql_hunt(df_5m: pd.DataFrame, state: BtcState, macro_bias: str):
    if len(df_5m) < 12:
        return None

    i = len(df_5m) - 1
    c, o, h, l = float(df_5m["c"].iloc[i]), float(df_5m["o"].iloc[i]), float(df_5m["h"].iloc[i]), float(df_5m["l"].iloc[i])
    rng = max(h - l, 1.0)
    upper_wick = (h - max(o, c)) / rng
    lower_wick = (min(o, c) - l) / rng

    win = df_5m.iloc[i - 6:i - 1]
    win_h = win["h"].values.astype(float)
    win_l = win["l"].values.astype(float)
    prev_h = float(df_5m["h"].iloc[i - 1])
    prev_l = float(df_5m["l"].iloc[i - 1])

    pools = state.get("htf_pools") or []
    near_big_sr_high = any(p["tf"] in ("4H", "1D", "1W") and p["side"] == "BSL" and abs(h - float(p["pivot"])) <= 150.0 for p in pools)
    near_big_sr_low = any(p["tf"] in ("4H", "1D", "1W") and p["side"] == "SSL" and abs(l - float(p["pivot"])) <= 150.0 for p in pools)

    piv_h = float(np.max(win_h))
    piv_l = float(np.min(win_l))

    # Fresh Equal Highs (EQH) Hunt -> SELL_SHORT
    if prev_h <= piv_h and h > piv_h and c < piv_h and upper_wick >= 0.35 and c < o:
        if (macro_bias == "RANGE" or near_big_sr_high) and np.sum(np.abs(win_h - piv_h) <= 30.0) >= 2:
            sweep_depth = round((h + 25.0) - c, 1)
            if 60.0 <= sweep_depth <= 250.0:
                return {
                    "name": "FRESH_30M_EQH_EQL_HUNT",
                    "action": "SELL_SHORT",
                    "tier": "A+ Fresh <=30m Equal Highs (EQH) Sweep",
                    "entry": c,
                    "wick_extreme": h,
                    "invalidation_ref": round(h + 25.0, 1),
                    "sweep_depth": sweep_depth,
                    "wick_pct": int(upper_wick * 100),
                    "swept_tf": "30M_EQH",
                    "swept_level": piv_h,
                    "why": f"Swept Fresh <=30m Equal Highs (${piv_h:,.1f}) with {int(upper_wick * 100)}% rejection wick",
                }

    # Fresh Equal Lows (EQL) Hunt -> BUY_LONG
    if prev_l >= piv_l and l < piv_l and c > piv_l and lower_wick >= 0.35 and c > o:
        if (macro_bias == "RANGE" or near_big_sr_low) and np.sum(np.abs(win_l - piv_l) <= 30.0) >= 2:
            sweep_depth = round(c - (l - 25.0), 1)
            if 60.0 <= sweep_depth <= 250.0:
                return {
                    "name": "FRESH_30M_EQH_EQL_HUNT",
                    "action": "BUY_LONG",
                    "tier": "A+ Fresh <=30m Equal Lows (EQL) Sweep",
                    "entry": c,
                    "wick_extreme": l,
                    "invalidation_ref": round(l - 25.0, 1),
                    "sweep_depth": sweep_depth,
                    "wick_pct": int(lower_wick * 100),
                    "swept_tf": "30M_EQL",
                    "swept_level": piv_l,
                    "why": f"Swept Fresh <=30m Equal Lows (${piv_l:,.1f}) with {int(lower_wick * 100)}% absorption wick",
                }

    return None


# ==============================================================================
# TIER-2 FAST 5M GATEKEEPER + PARALLEL BACKGROUND HYDROGEN CONFIRMATION
# ==============================================================================
ENTRY_GATEKEEPER_SYS_PROMPT = (
    "You are a fast Institutional BTCUSD Liquidity Execution Gatekeeper.\n"
    "Python detected a 5M liquidity sweep setup. Approve ONLY clean sweeps aligned with market structure.\n"
    "RULES:\n"
    "1. For BIG_SR_SWEEP [1H] or TUNED_3SWING_INDUCEMENT_TRAP: Direction must align with Macro Bias unless sweeping a major 4H/1D/1W S/R zone.\n"
    "2. For BIG_SR_SWEEP [4H/1D/1W] or FRESH_30M_EQH_EQL_HUNT: Approve if sweep wick >= 30% and body closed cleanly back inside.\n"
    "3. Reject if entry is running directly into an opposing 4H/1D liquidity wall less than $150 away.\n"
    "Respond ONLY in strict JSON:\n"
    '{"approved": true/false, "confidence": "HIGH|MEDIUM|REJECT", "view": "1 crisp sentence"}'
)

HYDROGEN_CONFIRM_SYS_PROMPT = (
    "You are the Senior Institutional BTCUSD Chief Strategist (gemini-4-hydrogen).\n"
    "An intraday 5M liquidity sweep entry alert was just triggered. Perform a deep secondary liquidity and order-flow audit.\n"
    "Respond ONLY in strict JSON:\n"
    '{"verdict": "CONFIRMED|CAUTION|INVALIDATE_WARNING", "confidence": "HIGH|MEDIUM|LOW", "note": "1-2 crisp sentences for manual trade management"}'
)


def _background_hydrogen_entry_audit(setup: dict, usr_p: str, fired_time_str: str):
    """Non-blocking background worker calling gemini-4-hydrogen (with heavy fallback) after entry alert fires."""
    ai_res, provider = call_ai_chain(HEAVY_AI_CHAIN, HYDROGEN_CONFIRM_SYS_PROMPT, usr_p, max_tokens=400)
    if not ai_res:
        log.warning(f"[BTC] Background heavy confirmation unreachable ({provider})")
        return

    verdict = ai_res.get("verdict", "CONFIRMED")
    conf = ai_res.get("confidence", "MEDIUM")
    note = ai_res.get("note", "Secondary liquidity audit completed.")
    badge = "✅" if verdict == "CONFIRMED" else ("⚠️" if verdict == "CAUTION" else "🚨")
    action_title = "BUY (LONG)" if setup["action"] == "BUY_LONG" else "SELL (SHORT)"

    msg = f"""{badge} <b>[HEAVY AI CONFIRMATION] {action_title} @ ${setup['entry']:,.1f}</b>
⏱️ <b>Signal Time:</b> <code>{fired_time_str}</code> | <b>Setup:</b> <code>{esc(setup['name'])}</code>
🧠 <b>Model      :</b> <code>{esc(provider)}</code>
🏛️ <b>Verdict    :</b> <b>[{esc(verdict)} | {esc(conf)}]</b>
<i>{esc(note)}</i>"""

    tg_send(msg)
    log.info(f"[BTC] 🧠 Background Heavy Confirmation sent ({provider}: {verdict})")


# ==============================================================================
# DIAGNOSTIC STARTUP CHECK & DAILY HEALTH REPORT
# ==============================================================================
def run_startup_diagnostics(state: BtcState):
    log.info("[BTC] 🔍 Running Startup Diagnostic Suite (V9.5)...")
    now = datetime.now(IST)

    t0 = time.time()
    df_test = fetch_delta_candles(BTC_SYMBOL, "5m", hours_back=3, closed_only=False)
    delta_lat = int((time.time() - t0) * 1000)
    delta_status = "ONLINE" if not df_test.empty else "OFFLINE"
    ltp_test = float(df_test.iloc[-1]["c"]) if not df_test.empty else 0.0

    t0 = time.time()
    ai_test_res, ai_provider = call_ai_chain(
        FAST_5M_AI_CHAIN,
        'Respond strictly in JSON: {"status":"ok"}',
        "Diagnostic ping. Return status ok.",
        max_tokens=100,
    )
    ai_lat = int((time.time() - t0) * 1000)
    ai_status = f"ONLINE ({ai_lat}ms)" if ai_test_res else "OFFLINE / UNREACHABLE"

    # Trigger initial background HTF liquidity pool sync so pools are ready immediately
    trigger_macro_sync_if_due(state.store, force=True)

    startup_msg = f"""🚀 <b>[BTC INSTITUTIONAL LIQUIDITY ENGINE V9.5 ONLINE]</b>
📅 <b>Time:</b> <code>{now.strftime('%Y-%m-%d %H:%M:%S IST')}</code> | <b>Server:</b> <code>Koyeb</code>

🟢 <b>Delta Public API :</b> <code>{delta_status} (LTP: ${ltp_test:,.1f} | {delta_lat}ms)</code>
🟢 <b>5M Gatekeeper AI :</b> <code>{ai_status} [{esc(ai_provider)}]</code>
🟢 <b>Heavy Confirm AI :</b> <code>{esc(HYDROGEN_MODEL)} -> {esc(FLASH_38_MODEL)} -> {esc(FLASH_36_MODEL)}</code>
🟢 <b>HTTP Health Check:</b> <code>RUNNING (Port {PORT} - UptimeRobot Ready)</code>

🎯 <b>Active Pure Formal Liquidity Setups (Entry-Only Mode):</b>
- <code>BIG_SR_SWEEP [1W, 1D, 4H, 1H]</code> (First-Touch Pool Sweep)
- <code>TUNED_3SWING_INDUCEMENT_TRAP</code> (w=4 Fractal + Trend/Big S/R + &lt;=$160)
- <code>FRESH_30M_EQH_EQL_HUNT</code> (Fresh &lt;=30m Equal Highs/Lows Sweep)

🛡️ <i>Spent Zone Lock + 60m Opposite-Direction Lock + Background Hydrogen Audit Active.</i>"""

    tg_send(startup_msg)
    log.info("[BTC] ✅ Startup diagnostics complete.")


def check_daily_0915_health_report(state: BtcState, ltp: float):
    global _last_daily_report_date
    now = datetime.now(IST)
    today_str = now.strftime("%Y-%m-%d")

    if now.hour == 9 and now.minute >= 15 and _last_daily_report_date != today_str:
        _last_daily_report_date = today_str
        snap = state.get("macro_snapshot") or {}
        pools = state.get("htf_pools") or []
        bias = state.get("macro_bias", snap.get("struct_bias", "N/A"))

        daily_msg = f"""☀️ <b>[DAILY 09:15 AM BTC LIQUIDITY HEALTH AUDIT]</b>
📅 <code>{now.strftime('%d %b %Y | %H:%M IST')}</code>

📊 <b>Engine Status:</b>
- AI Calls Today   : <code>{_daily_ai_call_count} Calls</code>
- Active HTF Pools : <code>{len(pools)} Un-swept (BSL:{snap.get('bsl_count', 0)} | SSL:{snap.get('ssl_count', 0)})</code>
- Macro Bias       : <b>{esc(bias)}</b> (6H: {esc(snap.get('t6', 'N/A'))} | 4H: {esc(snap.get('t4', 'N/A'))})
- BTC LTP          : <b>${ltp:,.1f}</b>

✅ <i>Entry-Only Pure Liquidity Engine V9.5 active (Manual SL/TP mode).</i>"""
        tg_send(daily_msg)


# ==============================================================================
# MAIN 5M EXECUTION CYCLE (ENTRY-ONLY IDENTIFICATION)
# ==============================================================================
def run_btc_cycle(shared_store=None):
    if not _btc_lock.acquire(blocking=False):
        log.warning("[BTC] Execution lock busy, skipping cycle")
        return

    try:
        state = BtcState(shared_store)
        state.set("last_tick", time.time(), ttl=86400)

        # Check scheduled 23:30 IST 1D+4H+1H job & 3x Intraday 4H-1H sync (05:30, 11:30, 17:30 IST)
        trigger_macro_sync_if_due(shared_store, force=False)

        # Fetch closed 5M candles for execution
        df_5m = fetch_delta_candles(BTC_SYMBOL, "5m", hours_back=24, closed_only=True)
        if df_5m.empty or len(df_5m) < 40:
            log.warning("[BTC] Insufficient 5M candle data from Delta Exchange")
            return

        ltp = float(df_5m["c"].iloc[-1])
        last_bar_time = int(df_5m["time"].iloc[-1])

        check_daily_0915_health_report(state, ltp)

        # Evaluate each closed 5M candle strictly once
        if state.get("last_evaluated_5m_ts") == last_bar_time:
            return
        state.set("last_evaluated_5m_ts", last_bar_time, ttl=300)

        # Ensure HTF pools exist if container just booted without startup hook
        if state.get("htf_pools") is None:
            trigger_macro_sync_if_due(shared_store, force=True)

        snap = state.get("macro_snapshot") or {}
        macro_bias = state.get("macro_bias", snap.get("struct_bias", "RANGE"))
        allowed_1h = state.get("allowed_1h_sweep", "BOTH")
        closed_ts_str = df_5m["ts"].iloc[-1].strftime("%H:%M IST")

        log.info(f"[BTC] ═════════ 5M LIQUIDITY AUDIT: {closed_ts_str} ═════════")
        log.info(f"[BTC] Close: ${ltp:,.1f} | Macro Bias: {macro_bias} (1H Rule: {allowed_1h}) | AI Calls: {_daily_ai_call_count}")

        # Evaluate the 3 Validated Pure Formal Liquidity Setups
        setup = (
            check_big_sr_and_1h_sweep(df_5m, state, macro_bias, allowed_1h)
            or check_tuned_3swing_inducement(df_5m, state, macro_bias)
            or check_fresh_30m_eqh_eql_hunt(df_5m, state, macro_bias)
        )

        if not setup:
            log.info("[BTC] Setups Evaluated: BIG_SR_1W_1D_4H_1H=NO | 3SWING_TRAP=NO | 30M_EQH_EQL=NO -> WAITING")
            return

        # 60-Minute Global Opposite-Direction Lock (Prevents flip-flop collisions)
        last_dir = state.get("last_signal_dir")
        last_dir_ts = state.get("last_signal_ts") or 0
        if last_dir and last_dir != setup["action"] and (time.time() - last_dir_ts) < (OPPOSITE_LOCK_MIN * 60):
            rem_opp = int(((OPPOSITE_LOCK_MIN * 60) - (time.time() - last_dir_ts)) / 60)
            log.info(f"[BTC] Opposite-direction lock active ({last_dir} fired recently; {rem_opp}m left). Blocked {setup['action']}.")
            return

        # 90-Minute Same-Setup Cooldown
        cd_key = f"cd_{setup['name']}_{setup['action']}"
        cd_until = state.get(cd_key)
        if cd_until and time.time() < cd_until:
            rem_m = int((cd_until - time.time()) / 60)
            log.info(f"[BTC] Cooldown active for {setup['name']} ({rem_m}m remaining). Skipped.")
            return

        # Build rich liquidity context for Tier-2 Gatekeeper & Tier-1 Background Hydrogen Audit
        bsl_str = ", ".join([z["why"] for z in snap.get("nearest_bsl", [])[:2]]) or "None"
        ssl_str = ", ".join([z["why"] for z in snap.get("nearest_ssl", [])[:2]]) or "None"
        usr_p = (
            f"Asset: BTCUSD | Closed 5M Bar: {closed_ts_str} | Entry Close: ${setup['entry']:,.1f}\n"
            f"6H/4H Macro Bias: {macro_bias} (Allowed 1H Sweep: {allowed_1h})\n"
            f"Detected Setup: {setup['name']} ({setup['tier']})\n"
            f"Action: {setup['action']} | Sweep Wick Extreme: ${setup['wick_extreme']:,.1f} | Sweep Depth: ${setup['sweep_depth']:.1f}\n"
            f"Trigger Details: {setup['why']}\n"
            f"EQH/EQL Counter: BSL Above={snap.get('bsl_count', 0)} (EQH:{snap.get('eqh_count', 0)}) | SSL Below={snap.get('ssl_count', 0)} (EQL:{snap.get('eql_count', 0)})\n"
            f"Nearest BSL: {bsl_str} | Nearest SSL: {ssl_str}"
        )

        log.info(f"[BTC] ⚡ Calling Tier-2 Fast Gatekeeper for {setup['name']} ({setup['action']})...")
        ai_res, provider = call_ai_chain(FAST_5M_AI_CHAIN, ENTRY_GATEKEEPER_SYS_PROMPT, usr_p, max_tokens=300)

        if not ai_res or not ai_res.get("approved", False):
            reason = ai_res.get("view", "Rejected by Gatekeeper") if ai_res else f"AI Unreachable ({provider})"
            log.warning(f"[BTC] 🛑 Entry Blocked by Gatekeeper: {reason}")
            state.set(cd_key, time.time() + WAIT_COOLDOWN_MIN * 60, ttl=WAIT_COOLDOWN_MIN * 60)
            return

        # Lock direction & setup cooldown
        state.set("last_signal_dir", setup["action"], ttl=OPPOSITE_LOCK_MIN * 60)
        state.set("last_signal_ts", time.time(), ttl=OPPOSITE_LOCK_MIN * 60)
        state.set(cd_key, time.time() + (SETUP_COOLDOWN_MIN * 60), ttl=SETUP_COOLDOWN_MIN * 60)

        em = "🟢" if setup["action"] == "BUY_LONG" else "🔴"
        action_title = "BUY (LONG)" if setup["action"] == "BUY_LONG" else "SELL (SHORT)"
        ai_view = ai_res.get("view", "Clean institutional liquidity sweep confirmed.")
        ai_conf = ai_res.get("confidence", "HIGH")
        now_str = datetime.now(IST).strftime("%H:%M IST")

        msg = f"""{em} <b>[BITCOIN - BTCUSD] {action_title} | {now_str}</b>

⚡ <b>Setup        :</b> <code>{esc(setup['name'])}</code>
🏆 <b>Category     :</b> {esc(setup['tier'])}
🎯 <b>Entry Price  :</b> <b>${setup['entry']:,.1f}</b>
🛑 <b>Wick Extreme :</b> <b>${setup['wick_extreme']:,.1f}</b> <i>(Invalidation Ref: ${setup['invalidation_ref']:,.1f} | Depth: ${setup['sweep_depth']:.1f})</i>

🧭 <b>Macro Bias   :</b> <b>{esc(macro_bias)}</b> <i>(6H: {esc(snap.get('t6', 'N/A'))} | 4H: {esc(snap.get('t4', 'N/A'))})</i>
💧 <b>EQH/EQL Pool :</b> <code>BSL Above: {snap.get('bsl_count', 0)} (EQH:{snap.get('eqh_count', 0)}) | SSL Below: {snap.get('ssl_count', 0)} (EQL:{snap.get('eql_count', 0)})</code>
📝 <b>Sweep Logic  :</b> {esc(setup['why'])}
⚡ <b>Gatekeeper ({esc(provider)}):</b> [{esc(ai_conf)}] <i>{esc(ai_view)}</i>

⏳ <i>Running {esc(HYDROGEN_MODEL)} background confirmation... (Manual SL &amp; Target Mode)</i>"""

        tg_send(msg)
        log.info(f"[BTC] 🔥 ENTRY ALERT FIRED: {setup['name']} ({setup['action']}) @ ${setup['entry']:,.1f}")

        # Launch non-blocking background thread for gemini-4-hydrogen confirmation
        t_hydro = threading.Thread(
            target=_background_hydrogen_entry_audit,
            args=(setup, usr_p, now_str),
            daemon=True,
        )
        t_hydro.start()

    except Exception as e:
        log.exception(f"[BTC] Error in run_btc_cycle: {type(e).__name__}")
    finally:
        try:
            _btc_lock.release()
        except RuntimeError:
            pass


def run_6h_analysis(shared_store=None):
    """Orchestrator hook for 6-hour macro sync (uses shared slot lock so zero duplicate alerts)."""
    trigger_macro_sync_if_due(shared_store, force=False)


def run_btc_30m_tick(shared_store=None):
    run_btc_cycle(shared_store)


def run_btc_1m_tick(shared_store=None):
    run_btc_cycle(shared_store)


if __name__ == "__main__":
    log.info("[BTC] Starting Bitcoin Institutional Liquidity Engine v9.5 on Koyeb...")
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()

    state_init = BtcState()
    run_startup_diagnostics(state_init)

    log.info("[BTC] Starting 30-Second Continuous Polling Loop...")
    while True:
        try:
            run_btc_cycle()
        except Exception as e:
            log.error(f"[BTC] Loop iteration error: {type(e).__name__}")
        time.sleep(30)
