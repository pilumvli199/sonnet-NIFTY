"""
================================================================================
NIFTY 50 PURE LIQUIDITY ENGINE — PRODUCTION V10.0
Namespace: NIFTY:* | Upstox V3 API | 100% Price Action (Zero EMA / RSI / ATR)
================================================================================
ARCHITECTURE:
  • Schedule (IST):
    - 09:20 IST: Morning BIG Multi-TF Analysis (1D + 1H + 15M + 5M)
    - 10:20, 11:20, 12:20, 13:20, 14:20 IST: Hourly Intraday Zone Sync
    - Every 5M Candle Close (:05s): Zero-Lag Python Liquidity Hunt Scanner
  • Dynamic Historical Price-Band Slice (Zone Vacuum Protection):
    - Triggers if < 2 untouched zones exist within LTP ± 300 pts.
    - Fetches 3-Year 1D/1W history, filters ONLY candles inside LTP ± 600 pts
      + Today's intraday candles (60m band cooldown).
    - On New All-Time High (0 historical candles in band), runs seamlessly on
      Current-Day Data + Real-Time Dynamic Swing Zones.
  • Pure Liquidity Setups (Entry-Only Mode — Manual SL & Target):
    1. OLD_HTF_BLUE_BOX (1D / 1H / 15M untouched up to 85d + AI-marked zones)
    2. MULTI_TOUCH_SHELF (2+ untouched swings within 12 pts)
    3. 1ST_15M_LIQ_POOL (Today's 09:15-09:30 High/Low liquidity sweep)
    4. EXHAUSTION_TRAP_PT10 (3+ same-color candles / big bar into Blue Box)
    5. TREND_PULLBACK_LIQ_HUNT (HH+HL / LH+LL impulse pullback sweep)
    6. BREAKOUT_FLIP_HUNT (Broken 1D/1H Blue Box retest with trend)
    7. DUAL_SIDE_HUNT_PT7 (Consolidation sweeps Side A first, then Side B)
    8. CURRENT_DAY_DYNAMIC_ZONE (Real-time swing Blue Box formed today)
  • AI Model Chain (No 3.8/3.6):
    - Heavy / Hourly: gemini-4-hydrogen (HIGH) -> gemini-3.5-flash-lite (HIGH) -> gemini-3.1-flash-lite (OFF)
    - Live Confirm  : gemini-3.5-flash-lite (HIGH) -> gemini-3.1-flash-lite (OFF)
================================================================================
"""

import os
import json
import math
import time
import html
import logging
import threading
from datetime import datetime, timedelta
from urllib.parse import quote
import requests
import numpy as np
import pandas as pd
import pytz

IST = pytz.timezone("Asia/Kolkata")
log = logging.getLogger("NIFTY")

# ==============================================================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# ==============================================================================
GEMINI_API_KEYS     = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip() and "your_" not in k.lower()]
UPSTOX_TOKEN        = os.getenv("UPSTOX_ANALYTICS_TOKEN", os.getenv("UPSTOX_ACCESS_TOKEN", "")).strip()
TG_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID          = os.getenv("NIFTY_TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", "")).strip()

NIFTY_SYMBOL_KEY    = "NSE_INDEX|Nifty 50"
NIFTY_KEY_ENC       = quote(NIFTY_SYMBOL_KEY)
UPSTOX_V3_BASE      = "https://api.upstox.com/v3"

# AI Models
HYDROGEN_MODEL      = os.getenv("GEMINI_HYDROGEN_MODEL", "gemini-4-hydrogen").strip()
LITE_35_MODEL       = os.getenv("GEMINI_35_LITE_MODEL", "gemini-3.5-flash-lite").strip()
LITE_31_MODEL       = os.getenv("GEMINI_31_LITE_MODEL", "gemini-3.1-flash-lite").strip()

HEAVY_AI_CHAIN = [
    (HYDROGEN_MODEL, "HIGH", 240, 4096),
    (LITE_35_MODEL,  "HIGH",  90, 4096),
    (LITE_31_MODEL,  "OFF",   45, 2048),
]

LIVE_CONFIRM_CHAIN = [
    (LITE_35_MODEL,  "HIGH",  45, 2048),
    (LITE_31_MODEL,  "OFF",   30, 2048),
]

# Pure Liquidity Constants (NIFTY Index Points)
CANDLE_BUFFER_SEC   = 2
ZONE_BUFFER_PTS     = 3.0
SHELF_MERGE_PTS     = 12.0
MAX_ZONE_AGE_DAYS   = 85
VACUUM_RADIUS_PTS   = 300.0   # User-specified: trigger deep band if < 2 zones in LTP ± 300 pts
DEEP_BAND_PTS       = 600.0   # Wide rally/crash historical slice: LTP ± 600 pts
MIN_RISK_PTS        = 22.0
SETUP_COOLDOWN_MIN  = int(os.getenv("NIFTY_SETUP_COOLDOWN_MIN", "45"))
OPPOSITE_LOCK_MIN   = int(os.getenv("NIFTY_OPPOSITE_LOCK_MIN", "30"))

_nifty_lock = threading.Lock()
_ai_rate_lock = threading.Lock()
_gemini_idx = 0
_last_gemini_call = 0.0
_daily_ai_calls = 0
_daily_ai_date = None


def esc(val) -> str:
    return html.escape(str(val), quote=False) if val is not None else ""


# ==============================================================================
# NAMESPACE STATE STORE (NIFTY:*)
# ==============================================================================
class _LocalStore:
    def __init__(self):
        self._d = {}
        self._e = {}
        self._lk = threading.Lock()

    def set(self, k, v, ttl=86400):
        with self._lk:
            self._d[k] = v
            self._e[k] = time.time() + ttl

    def get(self, k):
        with self._lk:
            if k in self._e and time.time() > self._e[k]:
                self._d.pop(k, None)
                self._e.pop(k, None)
                return None
            return self._d.get(k)


_fallback_store = _LocalStore()


class NiftyState:
    PREFIX = "NIFTY:"

    def __init__(self, shared_store=None):
        self.store = shared_store if shared_store is not None else _fallback_store

    def set(self, key: str, value, ttl: int = 86400):
        self.store.set(f"{self.PREFIX}{key}", value, ttl=ttl)

    def get(self, key: str):
        return self.store.get(f"{self.PREFIX}{key}")

    def mark_spent(self, zone_id: str, ttl: int = 172800):
        spent = self.get("spent_zones") or []
        if zone_id not in spent:
            spent.append(zone_id)
            if len(spent) > 400:
                spent = spent[-400:]
            self.set("spent_zones", spent, ttl=ttl)

    def is_spent(self, zone_id: str) -> bool:
        return zone_id in (self.get("spent_zones") or [])


# ==============================================================================
# TELEGRAM HELPER
# ==============================================================================
def tg_send(text: str) -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info(f"[NIFTY-TG-DRY]\n{text}\n")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=12,
        )
        if r.status_code != 200:
            # Plain-text fallback if HTML tag fails
            plain = text.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "").replace("<i>", "").replace("</i>", "")
            r2 = requests.post(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT_ID, "text": plain},
                timeout=12,
            )
            return r2.status_code == 200
        return True
    except Exception as e:
        log.error(f"[NIFTY] Telegram send error: {type(e).__name__}")
        return False


# ==============================================================================
# GEMINI AI ENGINE (NO thinkingBudget:0 BUG + thought PART FILTERING)
# ==============================================================================
def _bump_ai_counter():
    global _daily_ai_calls, _daily_ai_date
    today = datetime.now(IST).strftime("%Y-%m-%d")
    if _daily_ai_date != today:
        _daily_ai_date = today
        _daily_ai_calls = 0
    _daily_ai_calls += 1


def _parse_json_safe(raw: str):
    if not raw:
        return None
    clean = raw.strip().replace("```json", "").replace("```", "").strip()
    s, e = clean.find("{"), clean.rfind("}") + 1
    if s != -1 and e > s:
        try:
            return json.loads(clean[s:e])
        except Exception:
            return None
    return None


def call_gemini_single(model_name: str, thinking_lvl: str, timeout_sec: int, max_tokens: int, sys_prompt: str, user_prompt: str):
    global _gemini_idx, _last_gemini_call
    if not GEMINI_API_KEYS:
        return None

    for _ in range(len(GEMINI_API_KEYS)):
        with _ai_rate_lock:
            wait = 4.1 - (time.time() - _last_gemini_call)
            if wait > 0:
                time.sleep(wait)
            _last_gemini_call = time.time()
            key = GEMINI_API_KEYS[_gemini_idx % len(GEMINI_API_KEYS)]
            _gemini_idx += 1
            _bump_ai_counter()

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={key}"
        gen_cfg = {
            "temperature": 0.0,
            "maxOutputTokens": max_tokens,
            "responseMimeType": "application/json",
        }
        # Only attach thinkingConfig when thinking is ON (never send thinkingBudget: 0)
        if thinking_lvl and thinking_lvl.upper() != "OFF":
            if "4-hydrogen" in model_name:
                gen_cfg["thinkingConfig"] = {"thinkingLevel": thinking_lvl.upper()}
            else:
                b_map = {"LOW": 2048, "MEDIUM": 4096, "HIGH": 8192}
                gen_cfg["thinkingConfig"] = {"thinkingBudget": b_map.get(thinking_lvl.upper(), 4096)}

        payload = {
            "systemInstruction": {"parts": [{"text": sys_prompt}]},
            "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
            "generationConfig": gen_cfg,
        }

        try:
            r = requests.post(url, json=payload, timeout=timeout_sec)
            if r.status_code == 400 and "thinking" in r.text.lower() and "thinkingConfig" in gen_cfg:
                gen_cfg.pop("thinkingConfig", None)
                r = requests.post(url, json=payload, timeout=timeout_sec)

            if r.status_code == 429:
                log.warning(f"[NIFTY] {model_name} HTTP 429 rate limit — rotating key...")
                continue
            if r.status_code in (500, 502, 503, 504):
                log.warning(f"[NIFTY] {model_name} HTTP {r.status_code} — falling back...")
                break
            if r.status_code == 200:
                cands = r.json().get("candidates", [])
                if cands:
                    parts = cands[0].get("content", {}).get("parts", [])
                    text_parts = [p.get("text", "") for p in parts if not p.get("thought", False) and p.get("text")]
                    raw = "\n".join(text_parts) if text_parts else ""
                    parsed = _parse_json_safe(raw)
                    if parsed is not None:
                        return parsed
            else:
                log.warning(f"[NIFTY] {model_name} HTTP {r.status_code}")
                break
        except requests.Timeout:
            log.warning(f"[NIFTY] {model_name} timeout ({timeout_sec}s) — falling back...")
            break
        except Exception as e:
            log.warning(f"[NIFTY] {model_name} error ({type(e).__name__}) — falling back...")
            break
    return None


def call_gemini_chain(chain: list, sys_prompt: str, user_prompt: str):
    for model_name, t_lvl, t_sec, m_tok in chain:
        res = call_gemini_single(model_name, t_lvl, t_sec, m_tok, sys_prompt, user_prompt)
        if res is not None:
            return res, f"{model_name} [{t_lvl}]"
    return None, "AI_UNREACHABLE"


# ==============================================================================
# UPSTOX V3 CANDLE FETCHER + CLOSED-CANDLE BUFFER GUARD
# ==============================================================================
def _ensure_ist(ts_val) -> datetime:
    dt = pd.Timestamp(ts_val).to_pydatetime()
    return IST.localize(dt) if dt.tzinfo is None else dt.astimezone(IST)


def _normalize_upstox_candles(candles_raw: list) -> pd.DataFrame:
    if not candles_raw:
        return pd.DataFrame()
    cols = ["ts", "o", "h", "l", "c", "v", "oi"][:len(candles_raw[0])]
    df = pd.DataFrame(candles_raw, columns=cols)
    df["ts"] = pd.to_datetime(df["ts"])
    df["ts"] = df["ts"].dt.tz_localize(IST) if df["ts"].dt.tz is None else df["ts"].dt.tz_convert(IST)
    for col in ["o", "h", "l", "c"]:
        df[col] = df[col].astype(float).round(2)
    df["v"] = df["v"].astype(float) if "v" in df.columns else 0.0
    df = df.drop_duplicates(subset=["ts"], keep="last").sort_values("ts").reset_index(drop=True)
    return df[["ts", "o", "h", "l", "c", "v"]]


def drop_incomplete_candle(df: pd.DataFrame, interval_minutes: int, buffer_sec: int = CANDLE_BUFFER_SEC) -> pd.DataFrame:
    if df is None or df.empty or len(df) < 2:
        return pd.DataFrame() if df is None else df
    last_start = _ensure_ist(df.iloc[-1]["ts"])
    if datetime.now(IST) < last_start + timedelta(minutes=interval_minutes, seconds=buffer_sec):
        return df.iloc[:-1].reset_index(drop=True)
    return df


def fetch_upstox_candles(unit: str, interval: int, days_back: int, include_intraday: bool = True, closed_only: bool = True) -> pd.DataFrame:
    if not UPSTOX_TOKEN:
        return pd.DataFrame()
    headers = {"Authorization": f"Bearer {UPSTOX_TOKEN}", "Accept": "application/json"}
    now = datetime.now(IST)
    to_str = now.strftime("%Y-%m-%d")
    from_str = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")

    hist_rows, intra_rows = [], []
    try:
        r1 = requests.get(
            f"{UPSTOX_V3_BASE}/historical-candle/{NIFTY_KEY_ENC}/{unit}/{interval}/{to_str}/{from_str}",
            headers=headers,
            timeout=15,
        )
        if r1.status_code == 200 and r1.json().get("status") == "success":
            hist_rows = r1.json().get("data", {}).get("candles", [])
    except Exception as e:
        log.warning(f"[NIFTY] Upstox hist fetch error ({unit}/{interval}): {type(e).__name__}")

    if include_intraday and unit in ("minutes", "hours"):
        try:
            r2 = requests.get(
                f"{UPSTOX_V3_BASE}/historical-candle/intraday/{NIFTY_KEY_ENC}/{unit}/{interval}",
                headers=headers,
                timeout=12,
            )
            if r2.status_code == 200 and r2.json().get("status") == "success":
                intra_rows = r2.json().get("data", {}).get("candles", [])
        except Exception as e:
            log.warning(f"[NIFTY] Upstox intra fetch error ({unit}/{interval}): {type(e).__name__}")

    df_h = _normalize_upstox_candles(hist_rows)
    df_i = _normalize_upstox_candles(intra_rows)
    if df_h.empty:
        df = df_i
    elif df_i.empty:
        df = df_h
    else:
        df = pd.concat([df_h, df_i], ignore_index=True).drop_duplicates(subset=["ts"], keep="last").sort_values("ts").reset_index(drop=True)

    if closed_only and not df.empty:
        iv_mins = interval if unit == "minutes" else (interval * 60 if unit == "hours" else 375)
        df = drop_incomplete_candle(df, iv_mins)
    return df


# ==============================================================================
# COMPACT DELTA ENCODER (SAVES 70% TOKENS)
# ==============================================================================
def encode_delta_candles(df: pd.DataFrame, limit: int, base_price: int, fmt: str = "%m-%d %H:%M") -> list:
    if df is None or df.empty:
        return []
    sub = df.tail(limit)
    out = []
    for _, r in sub.iterrows():
        t_s = _ensure_ist(r["ts"]).strftime(fmt)
        o = int(round(float(r["o"]) - base_price))
        h = int(round(float(r["h"]) - base_price))
        l = int(round(float(r["l"]) - base_price))
        c = int(round(float(r["c"]) - base_price))
        out.append(f"{t_s}: O{o:+d} H{h:+d} L{l:+d} C{c:+d}")
    return out


# ==============================================================================
# PURE PRICE-ACTION REGIME & PERSISTENT BLUE BOX ENGINE (ZERO INDICATORS)
# ==============================================================================
def _median_range(df: pd.DataFrame, n: int = 20) -> float:
    if df is None or df.empty:
        return 15.0
    v = float((df["h"] - df["l"]).tail(n).median())
    return v if v > 0 else 15.0


def get_pure_pa_regime(h1_df: pd.DataFrame) -> str:
    """Pure HH+HL vs LH+LL structure regime on completed 1H bars (NO EMAs)."""
    if h1_df is None or len(h1_df) < 10:
        return "SIDEWAYS"
    sub = h1_df.tail(10)
    hh = sub["h"].values
    ll = sub["l"].values
    cc = sub["c"].values
    sc = float(np.median(hh - ll))
    ph, pl = np.max(hh[:5]), np.min(ll[:5])
    ch, cl = np.max(hh[5:]), np.min(ll[5:])
    net_mv = float(cc[-1] - cc[4])
    if (ch > ph and cl > pl) or net_mv > 1.7 * sc:
        return "TRENDING_BULL"
    if (ch < ph and cl < pl) or net_mv < -1.7 * sc:
        return "TRENDING_BEAR"
    return "SIDEWAYS"


def extract_pure_blue_boxes(df_1d: pd.DataFrame, df_1h: pd.DataFrame, df_15m: pd.DataFrame, df_5m: pd.DataFrame, ltp: float, now_ist: datetime, state: NiftyState) -> list:
    """
    Extracts UNTOUCHED Blue Boxes across 1D, 1H, 15M (up to 85 days old),
    merges nearby levels into MULTI_TOUCH_SHELF, and adds CURRENT_DAY_DYNAMIC_ZONE
    from today's intraday swings so rallies/new highs never run out of zones.
    """
    specs = [
        (df_1d,  "1D_BLUE_BOX",  3, 2, 1.3, 1),
        (df_1h,  "1H_BLUE_BOX",  6, 3, 1.8, 2),
        (df_15m, "15M_BLUE_BOX", 8, 4, 2.0, 3),
    ]
    raw_boxes = []
    for df, tf_lbl, lb, rb, rev_mult, prio in specs:
        if df is None or df.empty or len(df) < lb + rb + 2:
            continue
        sc = _median_range(df, 20)
        hh, ll, cc = df["h"].values, df["l"].values, df["c"].values
        for k in range(lb, len(df) - rb):
            formed_t = _ensure_ist(df["ts"].iloc[k])
            age_d = max(0, (now_ist - formed_t).days)
            if age_d > MAX_ZONE_AGE_DAYS:
                continue

            if hh[k] == np.max(hh[k - lb : k + rb + 1]):
                rev = float(hh[k] - np.min(ll[k + 1 : k + rb + 1]))
                sub_h, sub_c = hh[k + rb + 1 : -1], cc[k + rb + 1 : -1]
                if rev >= rev_mult * sc and (len(sub_h) == 0 or (np.max(sub_h) <= hh[k] and np.max(sub_c) <= hh[k])):
                    zid = f"{tf_lbl}_H_{int(hh[k])}_{formed_t.strftime('%Y%m%d%H%M')}"
                    if not state.is_spent(zid) and abs(hh[k] - ltp) <= DEEP_BAND_PTS:
                        raw_boxes.append({
                            "id": zid, "type": "RESISTANCE", "tf": tf_lbl, "prio": prio,
                            "pivot": round(float(hh[k]), 2), "age_days": age_d, "touches": 1,
                            "source": "PYTHON_HTF", "why": f"{tf_lbl} ({age_d}d old) @ {hh[k]:.2f}"
                        })

            if ll[k] == np.min(ll[k - lb : k + rb + 1]):
                rev = float(np.max(hh[k + 1 : k + rb + 1]) - ll[k])
                sub_l, sub_c = ll[k + rb + 1 : -1], cc[k + rb + 1 : -1]
                if rev >= rev_mult * sc and (len(sub_l) == 0 or (np.min(sub_l) >= ll[k] and np.min(sub_c) >= ll[k])):
                    zid = f"{tf_lbl}_L_{int(ll[k])}_{formed_t.strftime('%Y%m%d%H%M')}"
                    if not state.is_spent(zid) and abs(ll[k] - ltp) <= DEEP_BAND_PTS:
                        raw_boxes.append({
                            "id": zid, "type": "SUPPORT", "tf": tf_lbl, "prio": prio,
                            "pivot": round(float(ll[k]), 2), "age_days": age_d, "touches": 1,
                            "source": "PYTHON_HTF", "why": f"{tf_lbl} ({age_d}d old) @ {ll[k]:.2f}"
                        })

    # Add Current-Day Real-Time Dynamic Zones (5M/15M untouched swings formed TODAY)
    if df_5m is not None and len(df_5m) >= 12:
        today_str = now_ist.strftime("%Y-%m-%d")
        df_today = df_5m[df_5m["ts"].dt.strftime("%Y-%m-%d") == today_str].reset_index(drop=True)
        if len(df_today) >= 8:
            sc5 = _median_range(df_today, 15)
            hh5, ll5, cc5 = df_today["h"].values, df_today["l"].values, df_today["c"].values
            for k in range(3, len(df_today) - 2):
                ft = _ensure_ist(df_today["ts"].iloc[k])
                if hh5[k] == np.max(hh5[k - 3 : k + 3]) and (hh5[k] - np.min(ll5[k + 1 : k + 3])) >= 1.2 * sc5:
                    if len(hh5[k + 3 : -1]) == 0 or (np.max(hh5[k + 3 : -1]) <= hh5[k] and np.max(cc5[k + 3 : -1]) <= hh5[k]):
                        zid = f"CURR_DAY_H_{int(hh5[k])}_{ft.strftime('%H%M')}"
                        if not state.is_spent(zid):
                            raw_boxes.append({
                                "id": zid, "type": "RESISTANCE", "tf": "CURRENT_DAY_5M", "prio": 4,
                                "pivot": round(float(hh5[k]), 2), "age_days": 0, "touches": 1,
                                "source": "CURRENT_DAY_DYNAMIC", "why": f"Today {ft.strftime('%H:%M')} Swing High @ {hh5[k]:.2f}"
                            })
                if ll5[k] == np.min(ll5[k - 3 : k + 3]) and (np.max(hh5[k + 1 : k + 3]) - ll5[k]) >= 1.2 * sc5:
                    if len(ll5[k + 3 : -1]) == 0 or (np.min(ll5[k + 3 : -1]) >= ll5[k] and np.min(cc5[k + 3 : -1]) >= ll5[k]):
                        zid = f"CURR_DAY_L_{int(ll5[k])}_{ft.strftime('%H%M')}"
                        if not state.is_spent(zid):
                            raw_boxes.append({
                                "id": zid, "type": "SUPPORT", "tf": "CURRENT_DAY_5M", "prio": 4,
                                "pivot": round(float(ll5[k]), 2), "age_days": 0, "touches": 1,
                                "source": "CURRENT_DAY_DYNAMIC", "why": f"Today {ft.strftime('%H:%M')} Swing Low @ {ll5[k]:.2f}"
                            })

    # Merge AI-saved zones from state
    ai_zones = state.get("ai_zones") or []
    for az in ai_zones:
        if not state.is_spent(az["id"]) and abs(float(az["pivot"]) - ltp) <= DEEP_BAND_PTS:
            raw_boxes.append(az)

    # Deduplicate & merge nearby untouched swings (within 12 pts) into MULTI_TOUCH_SHELF
    raw_boxes.sort(key=lambda x: (x.get("prio", 3), abs(float(x["pivot"]) - ltp)))
    merged = []
    for cand in raw_boxes:
        m = next((x for x in merged if x["type"] == cand["type"] and abs(float(x["pivot"]) - float(cand["pivot"])) <= SHELF_MERGE_PTS), None)
        if m:
            m["touches"] = m.get("touches", 1) + 1
            m["age_days"] = max(m.get("age_days", 0), cand.get("age_days", 0))
            m["pivot"] = round(max(float(m["pivot"]), float(cand["pivot"])) if cand["type"] == "RESISTANCE" else min(float(m["pivot"]), float(cand["pivot"])), 2)
        else:
            merged.append(cand)

    merged.sort(key=lambda z: (z.get("prio", 3), -z.get("touches", 1), abs(float(z["pivot"]) - ltp)))
    return merged[:20]


# ==============================================================================
# DYNAMIC HISTORICAL PRICE-BAND SLICE (ZONE VACUUM < 2 ZONES IN LTP ± 300 PTS)
# ==============================================================================
BAND_SLICE_SYS_PROMPT = (
    "You are the NIFTY 50 Historical Liquidity Archaeologist (100% Pure Price Action — No Indicators).\n"
    "The market entered a price zone with fewer than 2 nearby untouched zones.\n"
    "Analyze ONLY the supplied historical candles inside LTP ± 600 pts + today's intraday candles.\n"
    "Identify untouched historical Blue Boxes (Swing Highs/Lows) and current-day dynamic zones.\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"regime_update": "TRENDING_BULL|TRENDING_BEAR|SIDEWAYS", '
    '"zones": [{"type": "RESISTANCE|SUPPORT", "tf": "HIST_1D|HIST_1W|CURR_DAY", "pivot": 0.0, "why": "short reason"}]}'
)


def _run_historical_band_slice_worker(shared_store, ltp: float, band_key: str, df_5m: pd.DataFrame):
    state = NiftyState(shared_store)
    now = datetime.now(IST)
    low_b, high_b = round(ltp - DEEP_BAND_PTS, 1), round(ltp + DEEP_BAND_PTS, 1)

    # Fetch 3-year 1D & 1W historical candles
    df_1d_3y = fetch_upstox_candles("days", 1, days_back=1095, include_intraday=False, closed_only=True)
    df_1w_3y = fetch_upstox_candles("weeks", 1, days_back=1095, include_intraday=False, closed_only=True)

    hist_hits = []
    for df_h, lbl in [(df_1w_3y, "1W"), (df_1d_3y, "1D")]:
        if df_h is None or df_h.empty:
            continue
        in_band = df_h[(df_h["h"] >= low_b) & (df_h["l"] <= high_b)].tail(35)
        for _, r in in_band.iterrows():
            dt_s = _ensure_ist(r["ts"]).strftime("%Y-%m-%d")
            hist_hits.append(f"{lbl} {dt_s}: O={r['o']} H={r['h']} L={r['l']} C={r['c']}")

    # All-Time High / Uncharted Price Fallback: 0 historical candles in band -> run purely on Current Day data!
    if not hist_hits:
        log.info(f"[NIFTY] 🚀 Uncharted Price / New High @ {ltp:.2f} (0 historical bars in {low_b}-{high_b}). Running Current-Day Dynamic Mode.")
        tg_send(
            f"🚀 <b>[NIFTY 50 NEW PRICE TERRITORY] {ltp:.2f}</b>\n"
            f"• 3-Year historical data has 0 candles in <code>{low_b}–{high_b}</code>.\n"
            f"• Switched automatically to <b>Current-Day Dynamic Zones + Trend Pullback Liquidity Hunts</b>."
        )
        return

    base_p = int(math.floor(low_b / 500.0) * 500)
    today_str = now.strftime("%Y-%m-%d")
    df_today = df_5m[df_5m["ts"].dt.strftime("%Y-%m-%d") == today_str] if (df_5m is not None and not df_5m.empty) else pd.DataFrame()
    today_delta = encode_delta_candles(df_today, 60, base_p, "%H:%M")

    usr_p = (
        f"Asset: NIFTY 50 | LTP: {ltp:.2f} | Band: {low_b} to {high_b} (±{int(DEEP_BAND_PTS)} pts)\n"
        f"Historical Candles Inside Band ({len(hist_hits)} bars):\n" + "\n".join(hist_hits[:45]) + "\n\n"
        f"Current Day 5M Candles (Base={base_p}):\n" + "\n".join(today_delta)
    )

    ai_res, provider = call_gemini_chain(HEAVY_AI_CHAIN, BAND_SLICE_SYS_PROMPT, usr_p)
    if not ai_res or not isinstance(ai_res.get("zones"), list):
        return

    existing_ai = state.get("ai_zones") or []
    added = []
    for idx, z in enumerate(ai_res["zones"][:10]):
        try:
            piv = round(float(z["pivot"]), 2)
            ztype = str(z.get("type", "RESISTANCE")).upper()
            if ztype not in ("RESISTANCE", "SUPPORT") or abs(piv - ltp) > DEEP_BAND_PTS:
                continue
            zid = f"AI_BAND_{ztype[:3]}_{int(piv)}_{now.strftime('%H%M')}_{idx}"
            item = {
                "id": zid, "type": ztype, "tf": str(z.get("tf", "HIST_1D")), "prio": 2,
                "pivot": piv, "age_days": 30, "touches": 1,
                "source": "AI_HISTORICAL_BAND", "why": str(z.get("why", "Historical Band Blue Box"))
            }
            existing_ai.append(item)
            added.append(item)
        except Exception:
            continue

    if added:
        state.set("ai_zones", existing_ai[-30:], ttl=86400)
        lines = [f"• <b>{z['type']}</b> @ <code>{z['pivot']}</code> ({esc(z['why'])})" for z in added[:5]]
        tg_send(
            f"🧭 <b>[NIFTY 50 DEEP BAND SYNC | {esc(provider)}]</b>\n"
            f"• Zone Vacuum (&lt;2 zones in ±300 pts) triggered 3-Yr Band Slice (<code>{low_b}–{high_b}</code>)\n"
            f"• Added <b>{len(added)}</b> Historical + Current-Day Blue Boxes for Python tracking:\n" + "\n".join(lines)
        )


def check_zone_vacuum_and_trigger_band(state: NiftyState, zones: list, ltp: float, df_5m: pd.DataFrame):
    nearby = [z for z in zones if abs(float(z["pivot"]) - ltp) <= VACUUM_RADIUS_PTS]
    if len(nearby) >= 2:
        return
    band_bucket = int(round(ltp / 300.0) * 300)
    lock_key = f"band_slice_lock_{band_bucket}"
    if state.get(lock_key):
        return
    state.set(lock_key, True, ttl=3600)  # 60-minute band cooldown
    log.info(f"[NIFTY] Zone Vacuum detected (<2 zones in {ltp:.1f} ± {VACUUM_RADIUS_PTS}). Launching Historical Band Slice...")
    threading.Thread(
        target=_run_historical_band_slice_worker,
        args=(state.store, ltp, lock_key, df_5m),
        daemon=True,
    ).start()


# ==============================================================================
# SCHEDULED TOP-DOWN ANALYSIS: 09:20 BIG ANALYSIS + HOURLY (10:20 - 14:20 IST)
# ==============================================================================
TOP_DOWN_SYS_PROMPT = (
    "You are the NIFTY 50 Top-Down Pure Liquidity Architect (1D -> 1H -> 15M -> 5M).\n"
    "Zero EMA/RSI/ATR. Analyze market structure and mark untouched Blue Boxes (Swing Highs/Lows, "
    "Multi-Touch Shelves, and Current-Day newly formed liquidity zones) for Python to track.\n"
    "All candle values are delta-encoded from BASE (real price = delta + BASE).\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"regime": "TRENDING_BULL|TRENDING_BEAR|SIDEWAYS", '
    '"if_then_plan": "1 concise sentence IF-THEN plan", '
    '"zones": [{"type": "RESISTANCE|SUPPORT", "tf": "1D|1H|15M|CURR_DAY", "pivot": 0.0, "why": "short reason"}]}'
)


def _execute_topdown_sync_worker(shared_store, is_morning_big: bool, slot_key: str):
    state = NiftyState(shared_store)
    now = datetime.now(IST)

    if is_morning_big:
        df_1d = fetch_upstox_candles("days", 1, days_back=120, include_intraday=False, closed_only=True)
        df_1h = fetch_upstox_candles("hours", 1, days_back=45, include_intraday=True, closed_only=True)
        df_15m = fetch_upstox_candles("minutes", 15, days_back=20, include_intraday=True, closed_only=True)
        df_5m = fetch_upstox_candles("minutes", 5, days_back=5, include_intraday=True, closed_only=True)
    else:
        df_1d = None
        df_1h = fetch_upstox_candles("hours", 1, days_back=15, include_intraday=True, closed_only=True)
        df_15m = fetch_upstox_candles("minutes", 15, days_back=10, include_intraday=True, closed_only=True)
        df_5m = fetch_upstox_candles("minutes", 5, days_back=3, include_intraday=True, closed_only=True)

    if df_5m is None or df_5m.empty:
        return

    ltp = float(df_5m["c"].iloc[-1])
    pa_regime = get_pure_pa_regime(df_1h)
    py_zones = extract_pure_blue_boxes(df_1d, df_1h, df_15m, df_5m, ltp, now, state)
    state.set("active_zones", py_zones, ttl=86400)
    state.set("regime", pa_regime, ttl=14400)

    # Publish Python Distance-Ring Map immediately (0-lag)
    r1 = [z for z in py_zones if abs(z["pivot"] - ltp) <= 75]
    r2 = [z for z in py_zones if 75 < abs(z["pivot"] - ltp) <= 150]
    r3 = [z for z in py_zones if 150 < abs(z["pivot"] - ltp) <= 300]
    title = "09:20 IST MORNING BIG MULTI-TF MAP" if is_morning_big else f"HOURLY LIQUIDITY SYNC ({now.strftime('%H:%M IST')})"

    r1_s = ", ".join([f"{z['type'][:3]} {z['pivot']}" for z in r1[:3]]) or "None"
    r2_s = ", ".join([f"{z['type'][:3]} {z['pivot']}" for z in r2[:3]]) or "None"
    r3_s = ", ".join([f"{z['type'][:3]} {z['pivot']}" for z in r3[:3]]) or "None"
    ring_txt = (
        f"• <b>Ring 1 (±75 pts):</b> {r1_s}\n"
        f"• <b>Ring 2 (±75–150):</b> {r2_s}\n"
        f"• <b>Ring 3 (±150–300):</b> {r3_s}"
    )
    tg_send(
        f"🏛️ <b>[NIFTY 50] {title}</b>\n"
        f"📊 <b>LTP:</b> <b>{ltp:.2f}</b> | <b>PA Regime:</b> <code>{pa_regime}</code> | <b>Zones:</b> <code>{len(py_zones)}</code>\n"
        f"{ring_txt}\n⏳ <i>Running {esc(HYDROGEN_MODEL)} background zone refinement...</i>"
    )

    # Build Compact Delta Payload for AI
    base_p = int(math.floor(ltp / 500.0) * 500)
    today_str = now.strftime("%Y-%m-%d")
    prev_5m = df_5m[df_5m["ts"].dt.strftime("%Y-%m-%d") < today_str].tail(45)
    curr_5m = df_5m[df_5m["ts"].dt.strftime("%Y-%m-%d") == today_str]
    bars_5m = encode_delta_candles(pd.concat([prev_5m, curr_5m]), 90, base_p, "%m-%d %H:%M")

    usr_p = (
        f"Asset: NIFTY 50 | Mode: {title} | Time: {now.strftime('%Y-%m-%d %H:%M IST')} | LTP: {ltp:.2f}\n"
        f"Base Price for Delta Decoding: {base_p}\n"
        f"Python Pre-Detected Zones: {json.dumps([{'type': z['type'], 'tf': z['tf'], 'pivot': z['pivot'], 'age_d': z['age_days']} for z in py_zones[:10]])}\n"
        + (f"1D Candles (30): {encode_delta_candles(df_1d, 30, base_p, '%Y-%m-%d')}\n" if df_1d is not None and not df_1d.empty else "")
        + f"1H Candles: {encode_delta_candles(df_1h, 35 if is_morning_big else 21, base_p)}\n"
        + f"15M Candles: {encode_delta_candles(df_15m, 35 if is_morning_big else 28, base_p)}\n"
        + f"5M Candles (Prev 45 + Today): {bars_5m}"
    )

    ai_res, provider = call_gemini_chain(HEAVY_AI_CHAIN, TOP_DOWN_SYS_PROMPT, usr_p)
    if not ai_res:
        log.warning(f"[NIFTY] Top-down AI unreachable ({provider}); Python zones remain active.")
        return

    ai_regime = ai_res.get("regime", pa_regime)
    state.set("regime", ai_regime, ttl=14400)
    ai_saved = state.get("ai_zones") or []
    new_cnt = 0
    for idx, z in enumerate((ai_res.get("zones") or [])[:12]):
        try:
            piv = round(float(z["pivot"]), 2)
            ztype = str(z.get("type", "RESISTANCE")).upper()
            if ztype not in ("RESISTANCE", "SUPPORT") or abs(piv - ltp) > DEEP_BAND_PTS:
                continue
            zid = f"AI_TD_{ztype[:3]}_{int(piv)}_{now.strftime('%Y%m%d_%H')}_{idx}"
            ai_saved.append({
                "id": zid, "type": ztype, "tf": str(z.get("tf", "CURR_DAY")), "prio": 2,
                "pivot": piv, "age_days": 0, "touches": 1,
                "source": "AI_TOPDOWN", "why": str(z.get("why", "AI Marked Blue Box"))
            })
            new_cnt += 1
        except Exception:
            continue

    if new_cnt:
        state.set("ai_zones", ai_saved[-35:], ttl=86400)
        # Refresh combined zone list for Python tracker
        refreshed = extract_pure_blue_boxes(df_1d, df_1h, df_15m, df_5m, ltp, now, state)
        state.set("active_zones", refreshed, ttl=86400)

    tg_send(
        f"🧠 <b>[NIFTY 50 AI ZONE SYNC | {esc(provider)}]</b>\n"
        f"🧭 <b>AI Regime:</b> <b>{esc(ai_regime)}</b> | <b>Saved Zones:</b> <code>{new_cnt}</code>\n"
        f"📋 <b>Plan:</b> <i>{esc(ai_res.get('if_then_plan', 'Tracking Blue Box -> 5M Yellow Box hunts.'))}</i>"
    )


def trigger_topdown_if_due(shared_store=None, force: bool = False):
    state = NiftyState(shared_store)
    now = datetime.now(IST)
    if now.weekday() >= 5 and not force:
        return
    valid_hours = [9, 10, 11, 12, 13, 14]
    cur_h = now.hour if force else (now.hour if (now.hour in valid_hours and 20 <= now.minute <= 32) else None)
    if cur_h is None:
        return

    slot_key = f"td_done_{now.strftime('%Y-%m-%d')}_{cur_h}"
    if not force and state.get(slot_key):
        return
    state.set(slot_key, True, ttl=14400)
    is_morning_big = (cur_h == 9) or force
    threading.Thread(
        target=_execute_topdown_sync_worker,
        args=(shared_store, is_morning_big, slot_key),
        daemon=True,
    ).start()


# ==============================================================================
# PURE LIQUIDITY SETUP DETECTOR (5M YELLOW BOX SWEEP & RECLAIM)
# ==============================================================================
def _check_yellow_box_reclaim(m5: pd.DataFrame, pivot: float, action: str, tol: float = ZONE_BUFFER_PTS):
    if len(m5) < 3:
        return False, 0.0, ""
    c1, c2 = m5.iloc[-1], m5.iloc[-2]
    o1, h1, l1, cl1 = float(c1["o"]), float(c1["h"]), float(c1["l"]), float(c1["c"])
    o2, h2, l2, cl2 = float(c2["o"]), float(c2["h"]), float(c2["l"]), float(c2["c"])
    rng1 = max(h1 - l1, 1.0)
    up_w1 = (h1 - max(o1, cl1)) / rng1
    dn_w1 = (min(o1, cl1) - l1) / rng1

    if action == "BUY_CE":
        if up_w1 > 0.38 and cl1 < h1 - rng1 * 0.35:
            return False, 0.0, ""
        if l1 <= pivot + tol and cl1 > pivot and (cl1 > o1 or dn_w1 >= 0.45):
            return True, l1, f"1-Bar Yellow Box Sweep ({l1:.2f} <= {pivot:.2f}) & Reclaim ({cl1:.2f})"
        low_2c = min(l1, l2)
        if low_2c <= pivot + tol and cl1 > o1 and cl1 > pivot and cl1 >= max(o2, cl2) - 3.0:
            return True, low_2c, f"2-Bar Yellow Box Sweep ({low_2c:.2f} <= {pivot:.2f}) & Reclaim ({cl1:.2f})"

    elif action == "BUY_PE":
        if dn_w1 > 0.38 and cl1 > l1 + rng1 * 0.35:
            return False, 0.0, ""
        if h1 >= pivot - tol and cl1 < pivot and (cl1 < o1 or up_w1 >= 0.45):
            return True, h1, f"1-Bar Yellow Box Sweep ({h1:.2f} >= {pivot:.2f}) & Reject ({cl1:.2f})"
        high_2c = max(h1, h2)
        if high_2c >= pivot - tol and cl1 < o1 and cl1 < pivot and cl1 <= min(o2, cl2) + 3.0:
            return True, high_2c, f"2-Bar Yellow Box Sweep ({high_2c:.2f} >= {pivot:.2f}) & Reject ({cl1:.2f})"

    return False, 0.0, ""


def detect_nifty_pure_liquidity_setup(m5: pd.DataFrame, m15: pd.DataFrame, zones: list, regime: str, state: NiftyState, close_ts: datetime):
    ltp = float(m5["c"].iloc[-1])
    sc = _median_range(m5, 20)
    o1, h1, l1, cl1 = float(m5["o"].iloc[-1]), float(m5["h"].iloc[-1]), float(m5["l"].iloc[-1]), ltp
    rng1 = max(h1 - l1, 1.0)
    up_w1 = (h1 - max(o1, cl1)) / rng1
    dn_w1 = (min(o1, cl1) - l1) / rng1

    # Point #10 Exhaustion Momentum Check
    c_tail = m5.iloc[-4:-1]
    prev_bar = m5.iloc[-2]
    big_up_in = (int((c_tail["c"] > c_tail["o"]).sum()) == 3) or ((float(prev_bar["c"]) - float(prev_bar["o"])) >= 2.2 * sc)
    big_dn_in = (int((c_tail["c"] < c_tail["o"]).sum()) == 3) or ((float(prev_bar["o"]) - float(prev_bar["c"])) >= 2.2 * sc)

    # 1. Persistent Old / HTF Blue Boxes + Multi-Touch Shelf + Point #10 Exhaustion Trap + Current-Day Dynamic Zones
    for b in zones:
        zid = b["id"]
        if state.is_spent(zid):
            continue
        lvl = float(b["pivot"])
        if abs(lvl - ltp) > 4.0 * sc:
            continue
        action = "BUY_PE" if b["type"] == "RESISTANCE" else "BUY_CE"
        is_major = (b.get("tf") in ("1D_BLUE_BOX", "1H_BLUE_BOX", "HIST_1D", "HIST_1W")) or (b.get("age_days", 0) >= 5) or (b.get("touches", 1) >= 2)
        if not is_major and ((action == "BUY_PE" and regime == "TRENDING_BULL") or (action == "BUY_CE" and regime == "TRENDING_BEAR")):
            continue

        ok, ext, desc = _check_yellow_box_reclaim(m5, lvl, action, tol=ZONE_BUFFER_PTS)
        if ok:
            state.mark_spent(zid)
            if (action == "BUY_PE" and big_up_in) or (action == "BUY_CE" and big_dn_in):
                sname = "EXHAUSTION_TRAP_PT10"
            elif b.get("touches", 1) >= 2:
                sname = "MULTI_TOUCH_SHELF"
            elif b.get("source") == "CURRENT_DAY_DYNAMIC" or b.get("tf") in ("CURRENT_DAY_5M", "CURR_DAY"):
                sname = "CURRENT_DAY_DYNAMIC_ZONE"
            else:
                sname = f"OLD_{b['tf']}" if b.get("age_days", 0) >= 5 else b["tf"]

            return {
                "name": sname, "action": action, "zone_id": zid, "blue_box": lvl,
                "wick_extreme": round(ext, 2), "entry": ltp,
                "reason": f"{desc} | {b.get('why', '')} [Touches:{b.get('touches', 1)}, Age:{b.get('age_days', 0)}d]"
            }

    # 2. Today's 1ST_15M Liquidity Pool (09:15-09:30 High/Low)
    mn = close_ts.hour * 60 + close_ts.minute
    if mn >= 9 * 60 + 35 and m15 is not None and not m15.empty:
        today_str = close_ts.strftime("%Y-%m-%d")
        f15 = m15[(m15["ts"].dt.strftime("%Y-%m-%d") == today_str) & (m15["ts"].dt.hour == 9) & (m15["ts"].dt.minute == 15)]
        if not f15.empty:
            fh, fl = float(f15.iloc[0]["h"]), float(f15.iloc[0]["l"])
            for lvl, act, tag in [(fh, "BUY_PE", "F15_H"), (fl, "BUY_CE", "F15_L")]:
                zid = f"1ST15M_{today_str}_{tag}"
                if state.is_spent(zid):
                    continue
                if (act == "BUY_PE" and regime == "TRENDING_BULL") or (act == "BUY_CE" and regime == "TRENDING_BEAR"):
                    continue
                ok, ext, desc = _check_yellow_box_reclaim(m5, lvl, act, tol=2.0)
                if ok:
                    state.mark_spent(zid)
                    return {
                        "name": "1ST_15M_LIQ_POOL", "action": act, "zone_id": zid, "blue_box": lvl,
                        "wick_extreme": round(ext, 2), "entry": ltp,
                        "reason": f"Day's 1st 15M ({lvl:.2f}) — {desc}"
                    }

    # 3. Trend Pullback Liquidity Hunt (Pure HH+HL / LH+LL Structure — NO EMAs)
    if len(m5) >= 16:
        hh5 = m5["h"].values
        ll5 = m5["l"].values
        i = len(m5) - 1
        if regime == "TRENDING_BULL":
            pb_idx = i - 6 + int(np.argmin(ll5[i - 6 : i - 1]))
            pb_low = float(ll5[pb_idx])
            imp_h = float(np.max(hh5[max(0, pb_idx - 8) : pb_idx]))
            if (imp_h - pb_low) >= 1.2 * sc and pb_low > float(np.min(ll5[max(0, pb_idx - 12) : pb_idx])):
                if l1 < pb_low and cl1 > pb_low and cl1 > o1 and (dn_w1 >= 0.25 or (cl1 - o1) >= 0.4 * sc):
                    return {
                        "name": "TREND_PULLBACK_LIQ_HUNT", "action": "BUY_CE", "zone_id": f"TPB_L_{int(pb_low)}_{i}",
                        "blue_box": pb_low, "wick_extreme": round(l1, 2), "entry": ltp,
                        "reason": f"Bullish Impulse Pullback Sweep below {pb_low:.2f} (Wick {l1:.2f}) & Green Reclaim ({cl1:.2f})"
                    }
        elif regime == "TRENDING_BEAR":
            pb_idx = i - 6 + int(np.argmax(hh5[i - 6 : i - 1]))
            pb_high = float(hh5[pb_idx])
            imp_l = float(np.min(ll5[max(0, pb_idx - 8) : pb_idx]))
            if (pb_high - imp_l) >= 1.2 * sc and pb_high < float(np.max(hh5[max(0, pb_idx - 12) : pb_idx])):
                if h1 > pb_high and cl1 < pb_high and cl1 < o1 and (up_w1 >= 0.25 or (o1 - cl1) >= 0.4 * sc):
                    return {
                        "name": "TREND_PULLBACK_LIQ_HUNT", "action": "BUY_PE", "zone_id": f"TPB_H_{int(pb_high)}_{i}",
                        "blue_box": pb_high, "wick_extreme": round(h1, 2), "entry": ltp,
                        "reason": f"Bearish Impulse Pullback Sweep above {pb_high:.2f} (Wick {h1:.2f}) & Red Reclaim ({cl1:.2f})"
                    }

    # 4. Dual-Side Liquidity Hunt (Point #7 & #8)
    today_str = close_ts.strftime("%Y-%m-%d")
    same_day = m5[m5["ts"].dt.strftime("%Y-%m-%d") == today_str]
    if len(same_day) >= 17:
        box, mid_bars = same_day.iloc[-17:-5], same_day.iloc[-5:-1]
        bh, bl = float(box["h"].max()), float(box["l"].min())
        if 18.0 <= (bh - bl) <= 65.0:
            if float(mid_bars["h"].max()) > bh and float(mid_bars["l"].min()) >= bl and regime != "TRENDING_BEAR":
                ok, ext, desc = _check_yellow_box_reclaim(m5, bl, "BUY_CE", tol=2.0)
                if ok:
                    return {
                        "name": "DUAL_SIDE_HUNT_PT7", "action": "BUY_CE", "zone_id": f"DUAL_L_{int(bl)}",
                        "blue_box": bl, "wick_extreme": round(ext, 2), "entry": ltp,
                        "reason": f"Dual-Side Hunt: Swept High ({bh:.2f}) first, then Low ({bl:.2f}) -> {desc}"
                    }
            elif float(mid_bars["l"].min()) < bl and float(mid_bars["h"].max()) <= bh and regime != "TRENDING_BULL":
                ok, ext, desc = _check_yellow_box_reclaim(m5, bh, "BUY_PE", tol=2.0)
                if ok:
                    return {
                        "name": "DUAL_SIDE_HUNT_PT7", "action": "BUY_PE", "zone_id": f"DUAL_H_{int(bh)}",
                        "blue_box": bh, "wick_extreme": round(ext, 2), "entry": ltp,
                        "reason": f"Dual-Side Hunt: Swept Low ({bl:.2f}) first, then High ({bh:.2f}) -> {desc}"
                    }

    return None


# ==============================================================================
# PARALLEL NON-BLOCKING BACKGROUND AI CONFIRMATION
# ==============================================================================
LIVE_CONFIRM_SYS_PROMPT = (
    "You are the NIFTY 50 Institutional Pure Liquidity Supervisor (Zero Indicators).\n"
    "Python already fired a fast 5M Yellow Box sweep alert. Perform an independent price-action audit.\n"
    "Decode delta candles using Base Price. Check wick quality, zone authenticity, and opposing walls.\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"verdict": "APPROVED|WAIT_CAUTION|REJECT_TRAP", "confidence": "HIGH|MEDIUM|LOW", "note": "1-2 concise sentences"}'
)


def _background_ai_confirm_worker(setup: dict, usr_p: str, signal_time: str):
    ai_res, provider = call_gemini_chain(LIVE_CONFIRM_CHAIN, LIVE_CONFIRM_SYS_PROMPT, usr_p)
    if not ai_res:
        log.warning(f"[NIFTY] Background AI confirm unreachable ({provider})")
        return
    verdict = ai_res.get("verdict", "APPROVED")
    conf = ai_res.get("confidence", "MEDIUM")
    note = ai_res.get("note", "")
    badge = "✅" if verdict == "APPROVED" else ("⚠️" if verdict == "WAIT_CAUTION" else "🛑")
    tg_send(
        f"{badge} <b>[NIFTY 50 AI CONFIRMATION | {signal_time}]</b>\n"
        f"⚡ <b>Setup:</b> <code>{esc(setup['name'])}</code> ({esc(setup['action'])}) @ <b>{setup['entry']:.2f}</b>\n"
        f"🧠 <b>Model:</b> <code>{esc(provider)}</code> | <b>Verdict:</b> <b>[{esc(verdict)} | {esc(conf)}]</b>\n"
        f"<i>{esc(note)}</i>"
    )


# ==============================================================================
# DIAGNOSTICS & DAILY 09:03-09:10 IST HEALTH AUDIT
# ==============================================================================
def run_startup_diagnostics(shared_store=None) -> dict:
    log.info("[NIFTY] Running Startup Diagnostics...")
    t0 = time.time()
    df_t = fetch_upstox_candles("minutes", 5, days_back=2, include_intraday=True, closed_only=False)
    api_ms = int((time.time() - t0) * 1000)
    upstox_ok = not df_t.empty
    ltp = float(df_t["c"].iloc[-1]) if upstox_ok else 0.0
    if upstox_ok:
        trigger_topdown_if_due(shared_store, force=True)
    return {"asset": "NIFTY50", "api_ok": upstox_ok, "api_ms": api_ms, "ltp": ltp}


def check_daily_morning_health(state: NiftyState, ltp: float):
    now = datetime.now(IST)
    today_str = now.strftime("%Y-%m-%d")
    if now.hour == 9 and 3 <= now.minute <= 10 and state.get("health_sent_date") != today_str:
        state.set("health_sent_date", today_str, ttl=86400)
        zones = state.get("active_zones") or []
        regime = state.get("regime", "SIDEWAYS")
        tg_send(
            f"☀️ <b>[NIFTY 50 DAILY HEALTH | {now.strftime('%d %b %Y %H:%M IST')}]</b>\n"
            f"• <b>Upstox V3:</b> <code>ONLINE</code> | <b>Last Close:</b> <code>{ltp:.2f}</code>\n"
            f"• <b>Active Blue Boxes:</b> <code>{len(zones)}</code> | <b>Regime:</b> <code>{regime}</code>\n"
            f"• <b>AI Calls Today:</b> <code>{_daily_ai_calls}</code>"
        )


# ==============================================================================
# MAIN 5M CYCLE (PYTHON-FIRST INSTANT ALERT -> PARALLEL AI)
# ==============================================================================
def run_nifty_cycle(shared_store=None):
    if not _nifty_lock.acquire(blocking=False):
        return
    try:
        state = NiftyState(shared_store)
        now = datetime.now(IST)

        # 1. Trigger 09:20 BIG Analysis or Hourly Sync (10:20 - 14:20 IST)
        trigger_topdown_if_due(shared_store, force=False)

        # 2. Fetch closed 5M & 15M candles
        df_5m = fetch_upstox_candles("minutes", 5, days_back=4, include_intraday=True, closed_only=True)
        if df_5m is None or len(df_5m) < 25:
            return

        ltp = float(df_5m["c"].iloc[-1])
        check_daily_morning_health(state, ltp)

        last_bar_ts = _ensure_ist(df_5m["ts"].iloc[-1])
        close_ts = last_bar_ts + timedelta(minutes=5)
        bar_key = close_ts.strftime("%Y-%m-%d_%H:%M")

        if state.get("last_eval_bar") == bar_key:
            return
        state.set("last_eval_bar", bar_key, ttl=600)

        mn = close_ts.hour * 60 + close_ts.minute
        if mn < 9 * 60 + 25 or mn > 14 * 60 + 30:
            return

        df_15m = fetch_upstox_candles("minutes", 15, days_back=6, include_intraday=True, closed_only=True)
        df_1h = fetch_upstox_candles("hours", 1, days_back=15, include_intraday=True, closed_only=True)

        regime = state.get("regime") or get_pure_pa_regime(df_1h)
        zones = state.get("active_zones")
        if zones is None:
            zones = extract_pure_blue_boxes(None, df_1h, df_15m, df_5m, ltp, close_ts, state)
            state.set("active_zones", zones, ttl=14400)

        # 3. Check Zone Vacuum (< 2 zones in LTP ± 300 pts) -> Dynamic 3-Yr Band Slice / New High Fallback
        check_zone_vacuum_and_trigger_band(state, zones, ltp, df_5m)

        # 4. Evaluate Pure Liquidity Setups (0-Lag Python)
        setup = detect_nifty_pure_liquidity_setup(df_5m, df_15m, zones, regime, state, close_ts)
        if not setup:
            log.info(f"[NIFTY] {close_ts.strftime('%H:%M IST')} | LTP:{ltp:.2f} | Regime:{regime} | Zones:{len(zones)} -> WAITING")
            return

        # Cooldown & opposite-direction collision guard
        last_dir = state.get("last_sig_dir")
        last_dir_t = state.get("last_sig_ts") or 0
        if last_dir and last_dir != setup["action"] and (time.time() - last_dir_t) < OPPOSITE_LOCK_MIN * 60:
            log.info(f"[NIFTY] Opposite lock active ({last_dir}); skipped {setup['action']}.")
            return

        cd_key = f"cd_{setup['name']}_{setup['action']}"
        if state.get(cd_key):
            return

        state.set("last_sig_dir", setup["action"], ttl=OPPOSITE_LOCK_MIN * 60)
        state.set("last_sig_ts", time.time(), ttl=OPPOSITE_LOCK_MIN * 60)
        state.set(cd_key, True, ttl=SETUP_COOLDOWN_MIN * 60)

        # 5. PYTHON-FIRST INSTANT TELEGRAM ALERT (Entry-Only, Manual SL/Target)
        em = "🟢" if setup["action"] == "BUY_CE" else "🔴"
        sig_time_str = close_ts.strftime("%H:%M IST")
        msg = (
            f"{em} <b>[NIFTY 50 PURE LIQUIDITY] {setup['action']} | {sig_time_str}</b>\n\n"
            f"⚡ <b>Setup        :</b> <code>{esc(setup['name'])}</code>\n"
            f"🎯 <b>Index Entry  :</b> <b>{setup['entry']:.2f}</b>\n"
            f"🟦 <b>Blue Box Lvl :</b> <b>{setup['blue_box']:.2f}</b> | <b>Yellow Wick Ext:</b> <b>{setup['wick_extreme']:.2f}</b>\n"
            f"🧭 <b>PA Regime    :</b> <code>{esc(regime)}</code>\n"
            f"📝 <b>Sweep Logic  :</b> {esc(setup['reason'])}\n\n"
            f"⏳ <i>Chart baghun manual confirm kar (Manual SL &amp; Target mode). Running parallel AI audit...</i>"
        )
        tg_send(msg)
        log.info(f"[NIFTY] 🔥 INSTANT ALERT: {setup['name']} ({setup['action']}) @ {setup['entry']:.2f}")

        # 6. Parallel Non-Blocking Background AI Audit
        base_p = int(math.floor(ltp / 500.0) * 500)
        delta_bars = encode_delta_candles(df_5m, 45, base_p, "%m-%d %H:%M")
        usr_p = (
            f"Asset: NIFTY 50 | Time: {sig_time_str} | Entry: {setup['entry']:.2f}\n"
            f"Setup: {setup['name']} ({setup['action']}) | Blue Box: {setup['blue_box']:.2f} | Wick Ext: {setup['wick_extreme']:.2f}\n"
            f"Regime: {regime} | Reason: {setup['reason']}\n"
            f"Base Price: {base_p} | Recent 45 5M Delta Candles: {delta_bars}"
        )
        threading.Thread(
            target=_background_ai_confirm_worker,
            args=(setup, usr_p, sig_time_str),
            daemon=True,
        ).start()

    except Exception as e:
        log.exception(f"[NIFTY] Cycle error: {type(e).__name__}")
    finally:
        try:
            _nifty_lock.release()
        except RuntimeError:
            pass
