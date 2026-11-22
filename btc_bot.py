"""
================================================================================
GOLD (XAUTUSD) PURE LIQUIDITY ENGINE — PRODUCTION V10.0
Namespace: GOLD:* | Delta Exchange India Public API | 100% Price Action
================================================================================
ARCHITECTURE:
  • Schedule (IST):
    - 23:30 IST: Daily BIG Multi-TF Analysis (1D + 4H + 1H + 15M + 5M)
    - 05:30, 11:30, 17:30 IST: Every 6-Hour Intraday Zone Sync (Current-Day Zones)
    - Every 5M Candle Close (:05s): Zero-Lag Python Liquidity Hunt Scanner
  • Dynamic Historical Price-Band Slice (Zone Vacuum Protection):
    - Triggers if < 2 untouched zones exist within LTP ± $100.
    - Fetches 3-Year 1D + 11-Month 4H history, filters ONLY candles inside
      LTP ± $200 + Today's intraday candles (60m band cooldown).
    - On New All-Time High (0 historical candles in band), runs seamlessly on
      Current-Day Data + Real-Time Dynamic Swing Zones.
  • Pure Liquidity Setups (Entry-Only Mode — Manual SL & Target):
    1. OLD_HTF_BLUE_BOX (1D / 4H / 1H untouched up to 85d + AI-marked zones)
    2. MULTI_TOUCH_SHELF (2+ untouched swings within $2.50)
    3. EXHAUSTION_TRAP_PT10 (3+ same-color candles / big bar into Blue Box)
    4. TREND_PULLBACK_LIQ_HUNT (15M clean impulse pullback sweep — +32.5R proven)
    5. BREAKOUT_FLIP_HUNT (Broken 1D/4H/1H Blue Box retest with trend)
    6. DUAL_SIDE_HUNT_PT7 (Consolidation sweeps Side A first, then Side B)
    7. CURRENT_DAY_DYNAMIC_ZONE (Real-time swing Blue Box formed today)
  • AI Model Chain (No 3.8/3.6):
    - Heavy / 6H Sync: gemini-4-hydrogen (HIGH) -> gemini-3.5-flash-lite (HIGH) -> gemini-3.1-flash-lite (OFF)
    - Live Confirm     : gemini-3.5-flash-lite (HIGH) -> gemini-3.1-flash-lite (OFF)
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
import requests
import numpy as np
import pandas as pd
import pytz

IST = pytz.timezone("Asia/Kolkata")
log = logging.getLogger("GOLD")

# ==============================================================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# ==============================================================================
GEMINI_API_KEYS     = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip() and "your_" not in k.lower()]
TG_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID          = os.getenv("GOLD_TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", "")).strip()

DELTA_BASE_URL      = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange").strip()
DELTA_API_KEY       = os.getenv("DELTA_API_KEY", "").strip()
if "your_" in DELTA_API_KEY.lower():
    DELTA_API_KEY = ""
GOLD_SYMBOL         = os.getenv("GOLD_SYMBOL", "XAUTUSD").strip()

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

# Pure Liquidity Constants (Gold USD)
CANDLE_BUFFER_SEC   = 2
ZONE_BUFFER_USD     = 1.80
SHELF_MERGE_USD     = 2.50
FLIP_BREAK_USD      = 2.50
MAX_ZONE_AGE_DAYS   = 85
VACUUM_RADIUS_USD   = 100.0   # User-specified: trigger deep band if < 2 zones in LTP ± $100
DEEP_BAND_USD       = 200.0   # Wide rally/crash historical slice: LTP ± $200
SETUP_COOLDOWN_MIN  = int(os.getenv("GOLD_SETUP_COOLDOWN_MIN", "60"))
OPPOSITE_LOCK_MIN   = int(os.getenv("GOLD_OPPOSITE_LOCK_MIN", "45"))

_gold_lock = threading.Lock()
_ai_rate_lock = threading.Lock()
_gemini_idx = 0
_last_gemini_call = 0.0
_daily_ai_calls = 0
_daily_ai_date = None


def esc(val) -> str:
    return html.escape(str(val), quote=False) if val is not None else ""


# ==============================================================================
# NAMESPACE STATE STORE (GOLD:*)
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


class GoldState:
    PREFIX = "GOLD:"

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
        log.info(f"[GOLD-TG-DRY]\n{text}\n")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=12,
        )
        if r.status_code != 200:
            plain = text.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "").replace("<i>", "").replace("</i>", "")
            r2 = requests.post(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT_ID, "text": plain},
                timeout=12,
            )
            return r2.status_code == 200
        return True
    except Exception as e:
        log.error(f"[GOLD] Telegram send error: {type(e).__name__}")
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
                log.warning(f"[GOLD] {model_name} HTTP 429 — rotating key...")
                continue
            if r.status_code in (500, 502, 503, 504):
                log.warning(f"[GOLD] {model_name} HTTP {r.status_code} — falling back...")
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
                log.warning(f"[GOLD] {model_name} HTTP {r.status_code}")
                break
        except requests.Timeout:
            log.warning(f"[GOLD] {model_name} timeout ({timeout_sec}s) — falling back...")
            break
        except Exception as e:
            log.warning(f"[GOLD] {model_name} error ({type(e).__name__}) — falling back...")
            break
    return None


def call_gemini_chain(chain: list, sys_prompt: str, user_prompt: str):
    for model_name, t_lvl, t_sec, m_tok in chain:
        res = call_gemini_single(model_name, t_lvl, t_sec, m_tok, sys_prompt, user_prompt)
        if res is not None:
            return res, f"{model_name} [{t_lvl}]"
    return None, "AI_UNREACHABLE"


# ==============================================================================
# DELTA EXCHANGE CANDLES FETCHER + CLOSED-CANDLE BUFFER GUARD
# ==============================================================================
def _ensure_ist(ts_val) -> datetime:
    dt = pd.Timestamp(ts_val).to_pydatetime()
    return IST.localize(dt) if dt.tzinfo is None else dt.astimezone(IST)


def fetch_delta_candles(resolution: str, hours_back: int, closed_only: bool = True) -> pd.DataFrame:
    end_ts = int(time.time())
    start_ts = end_ts - (hours_back * 3600)
    res_sec = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}.get(resolution, 300)
    headers = {"api-key": DELTA_API_KEY} if DELTA_API_KEY else {}

    try:
        r = requests.get(
            f"{DELTA_BASE_URL}/v2/history/candles",
            params={"symbol": GOLD_SYMBOL, "resolution": resolution, "start": start_ts, "end": end_ts},
            headers=headers,
            timeout=18,
        )
        if r.status_code == 200:
            rows = r.json().get("result", [])
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows)
            df["time"] = df["time"].astype(int)
            df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(IST)
            df = df.rename(columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
            for col in ["o", "h", "l", "c"]:
                df[col] = df[col].astype(float).round(2)
            df["v"] = df["v"].astype(float)
            df = df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)

            if closed_only and not df.empty:
                last_open = int(df.iloc[-1]["time"])
                if end_ts < (last_open + res_sec + CANDLE_BUFFER_SEC):
                    df = df.iloc[:-1].reset_index(drop=True)
            return df[["time", "ts", "o", "h", "l", "c", "v"]]
    except Exception as e:
        log.error(f"[GOLD] Delta fetch error ({resolution}): {type(e).__name__}")
    return pd.DataFrame()


def encode_delta_candles(df: pd.DataFrame, limit: int, base_price: float, fmt: str = "%m-%d %H:%M") -> list:
    if df is None or df.empty:
        return []
    sub = df.tail(limit)
    out = []
    for _, r in sub.iterrows():
        t_s = _ensure_ist(r["ts"]).strftime(fmt)
        o = round(float(r["o"]) - base_price, 2)
        h = round(float(r["h"]) - base_price, 2)
        l = round(float(r["l"]) - base_price, 2)
        c = round(float(r["c"]) - base_price, 2)
        out.append(f"{t_s}: O{o:+.2f} H{h:+.2f} L{l:+.2f} C{c:+.2f}")
    return out


# ==============================================================================
# PURE PRICE-ACTION REGIME & PERSISTENT BLUE BOX ENGINE (ZERO INDICATORS)
# ==============================================================================
def _median_range(df: pd.DataFrame, n: int = 20) -> float:
    if df is None or df.empty:
        return 3.0
    v = float((df["h"] - df["l"]).tail(n).median())
    return v if v > 0 else 3.0


def get_pure_pa_regime(df_4h: pd.DataFrame, df_1h: pd.DataFrame) -> str:
    def _eval(hd):
        if hd is None or len(hd) < 10:
            return "SIDEWAYS"
        sub = hd.tail(10)
        hh, ll, cc = sub["h"].values, sub["l"].values, sub["c"].values
        sc = float(np.median(hh - ll))
        if (np.max(hh[5:]) > np.max(hh[:5]) and np.min(ll[5:]) > np.min(ll[:5])) or (cc[-1] - cc[4]) > 1.7 * sc:
            return "TRENDING_BULL"
        if (np.max(hh[5:]) < np.max(hh[:5]) and np.min(ll[5:]) < np.min(ll[:5])) or (cc[-1] - cc[4]) < -1.7 * sc:
            return "TRENDING_BEAR"
        return "SIDEWAYS"

    r4 = _eval(df_4h)
    r1 = _eval(df_1h)
    if r4 == "TRENDING_BULL" or (r4 == "SIDEWAYS" and r1 == "TRENDING_BULL"):
        return "TRENDING_BULL"
    if r4 == "TRENDING_BEAR" or (r4 == "SIDEWAYS" and r1 == "TRENDING_BEAR"):
        return "TRENDING_BEAR"
    return "SIDEWAYS"


def extract_pure_blue_boxes(df_1d: pd.DataFrame, df_4h: pd.DataFrame, df_1h: pd.DataFrame, df_5m: pd.DataFrame, ltp: float, now_ist: datetime, state: GoldState) -> list:
    specs = [
        (df_1d, "1D_BLUE_BOX", 3, 2, 1.3, 1),
        (df_4h, "4H_BLUE_BOX", 5, 2, 1.7, 2),
        (df_1h, "1H_BLUE_BOX", 6, 3, 1.9, 3),
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
                    zid = f"{tf_lbl}_H_{int(hh[k]*100)}_{formed_t.strftime('%Y%m%d%H')}"
                    if not state.is_spent(zid) and abs(hh[k] - ltp) <= DEEP_BAND_USD:
                        raw_boxes.append({
                            "id": zid, "type": "RESISTANCE", "tf": tf_lbl, "prio": prio,
                            "pivot": round(float(hh[k]), 2), "age_days": age_d, "touches": 1,
                            "flipped": False, "source": "PYTHON_HTF", "why": f"{tf_lbl} ({age_d}d old) @ ${hh[k]:.2f}"
                        })

            if ll[k] == np.min(ll[k - lb : k + rb + 1]):
                rev = float(np.max(hh[k + 1 : k + rb + 1]) - ll[k])
                sub_l, sub_c = ll[k + rb + 1 : -1], cc[k + rb + 1 : -1]
                if rev >= rev_mult * sc and (len(sub_l) == 0 or (np.min(sub_l) >= ll[k] and np.min(sub_c) >= ll[k])):
                    zid = f"{tf_lbl}_L_{int(ll[k]*100)}_{formed_t.strftime('%Y%m%d%H')}"
                    if not state.is_spent(zid) and abs(ll[k] - ltp) <= DEEP_BAND_USD:
                        raw_boxes.append({
                            "id": zid, "type": "SUPPORT", "tf": tf_lbl, "prio": prio,
                            "pivot": round(float(ll[k]), 2), "age_days": age_d, "touches": 1,
                            "flipped": False, "source": "PYTHON_HTF", "why": f"{tf_lbl} ({age_d}d old) @ ${ll[k]:.2f}"
                        })

    # Current-Day Real-Time Dynamic Zones (5M untouched swings formed TODAY)
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
                        zid = f"CURR_DAY_H_{int(hh5[k]*100)}_{ft.strftime('%H%M')}"
                        if not state.is_spent(zid):
                            raw_boxes.append({
                                "id": zid, "type": "RESISTANCE", "tf": "CURRENT_DAY_5M", "prio": 4,
                                "pivot": round(float(hh5[k]), 2), "age_days": 0, "touches": 1,
                                "flipped": False, "source": "CURRENT_DAY_DYNAMIC", "why": f"Today {ft.strftime('%H:%M')} Swing High @ ${hh5[k]:.2f}"
                            })
                if ll5[k] == np.min(ll5[k - 3 : k + 3]) and (np.max(hh5[k + 1 : k + 3]) - ll5[k]) >= 1.2 * sc5:
                    if len(ll5[k + 3 : -1]) == 0 or (np.min(ll5[k + 3 : -1]) >= ll5[k] and np.min(cc5[k + 3 : -1]) >= ll5[k]):
                        zid = f"CURR_DAY_L_{int(ll5[k]*100)}_{ft.strftime('%H%M')}"
                        if not state.is_spent(zid):
                            raw_boxes.append({
                                "id": zid, "type": "SUPPORT", "tf": "CURRENT_DAY_5M", "prio": 4,
                                "pivot": round(float(ll5[k]), 2), "age_days": 0, "touches": 1,
                                "flipped": False, "source": "CURRENT_DAY_DYNAMIC", "why": f"Today {ft.strftime('%H:%M')} Swing Low @ ${ll5[k]:.2f}"
                            })

    # Merge AI-saved zones
    for az in (state.get("ai_zones") or []):
        if not state.is_spent(az["id"]) and abs(float(az["pivot"]) - ltp) <= DEEP_BAND_USD:
            raw_boxes.append(az)

    raw_boxes.sort(key=lambda x: (x.get("prio", 3), abs(float(x["pivot"]) - ltp)))
    merged = []
    for cand in raw_boxes:
        m = next((x for x in merged if x["type"] == cand["type"] and not x.get("flipped") and abs(float(x["pivot"]) - float(cand["pivot"])) <= SHELF_MERGE_USD), None)
        if m:
            m["touches"] = m.get("touches", 1) + 1
            m["age_days"] = max(m.get("age_days", 0), cand.get("age_days", 0))
            m["pivot"] = round(max(float(m["pivot"]), float(cand["pivot"])) if cand["type"] == "RESISTANCE" else min(float(m["pivot"]), float(cand["pivot"])), 2)
        else:
            merged.append(cand)

    merged.sort(key=lambda z: (z.get("prio", 3), -z.get("touches", 1), abs(float(z["pivot"]) - ltp)))
    return merged[:22]


# ==============================================================================
# DYNAMIC HISTORICAL PRICE-BAND SLICE (ZONE VACUUM < 2 ZONES IN LTP ± $100)
# ==============================================================================
BAND_SLICE_SYS_PROMPT = (
    "You are the Gold (XAUTUSD) Historical Liquidity Archaeologist (100% Pure Price Action — No Indicators).\n"
    "Gold moved into an area with fewer than 2 nearby untouched zones.\n"
    "Analyze ONLY the supplied historical candles inside LTP ± $200 + today's intraday candles.\n"
    "Identify untouched historical Blue Boxes (Swing Highs/Lows) and current-day dynamic zones.\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"regime_update": "TRENDING_BULL|TRENDING_BEAR|SIDEWAYS", '
    '"zones": [{"type": "RESISTANCE|SUPPORT", "tf": "HIST_1D|HIST_4H|CURR_DAY", "pivot": 0.0, "why": "short reason"}]}'
)


def _run_historical_band_slice_worker(shared_store, ltp: float, df_5m: pd.DataFrame):
    state = GoldState(shared_store)
    now = datetime.now(IST)
    low_b, high_b = round(ltp - DEEP_BAND_USD, 2), round(ltp + DEEP_BAND_USD, 2)

    df_1d_3y = fetch_delta_candles("1d", hours_back=24 * 1095, closed_only=True)
    df_4h_11m = fetch_delta_candles("4h", hours_back=24 * 330, closed_only=True)

    hist_hits = []
    for df_h, lbl in [(df_1d_3y, "1D"), (df_4h_11m, "4H")]:
        if df_h is None or df_h.empty:
            continue
        in_band = df_h[(df_h["h"] >= low_b) & (df_h["l"] <= high_b)].tail(35)
        for _, r in in_band.iterrows():
            dt_s = _ensure_ist(r["ts"]).strftime("%Y-%m-%d %H:%M")
            hist_hits.append(f"{lbl} {dt_s}: O={r['o']} H={r['h']} L={r['l']} C={r['c']}")

    if not hist_hits:
        log.info(f"[GOLD] 🚀 Uncharted Price / New High @ ${ltp:.2f} (0 historical bars in ${low_b}-${high_b}). Running Current-Day Dynamic Mode.")
        tg_send(
            f"🚀 <b>[GOLD NEW PRICE TERRITORY] ${ltp:.2f}</b>\n"
            f"• 3-Year historical data has 0 candles in <code>${low_b}–${high_b}</code>.\n"
            f"• Switched automatically to <b>Current-Day Dynamic Zones + 15M Trend Pullback Liquidity Hunts</b>."
        )
        return

    base_p = round(math.floor(low_b / 50.0) * 50.0, 2)
    today_str = now.strftime("%Y-%m-%d")
    df_today = df_5m[df_5m["ts"].dt.strftime("%Y-%m-%d") == today_str] if (df_5m is not None and not df_5m.empty) else pd.DataFrame()
    today_delta = encode_delta_candles(df_today, 60, base_p, "%H:%M")

    usr_p = (
        f"Asset: GOLD (XAUTUSD) | LTP: ${ltp:.2f} | Band: ${low_b} to ${high_b} (±${int(DEEP_BAND_USD)})\n"
        f"Historical Candles Inside Band ({len(hist_hits)} bars):\n" + "\n".join(hist_hits[:45]) + "\n\n"
        f"Current Day 5M Candles (Base=${base_p}):\n" + "\n".join(today_delta)
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
            if ztype not in ("RESISTANCE", "SUPPORT") or abs(piv - ltp) > DEEP_BAND_USD:
                continue
            zid = f"AI_BAND_{ztype[:3]}_{int(piv*100)}_{now.strftime('%H%M')}_{idx}"
            item = {
                "id": zid, "type": ztype, "tf": str(z.get("tf", "HIST_1D")), "prio": 2,
                "pivot": piv, "age_days": 30, "touches": 1, "flipped": False,
                "source": "AI_HISTORICAL_BAND", "why": str(z.get("why", "Historical Band Blue Box"))
            }
            existing_ai.append(item)
            added.append(item)
        except Exception:
            continue

    if added:
        state.set("ai_zones", existing_ai[-30:], ttl=86400)
        lines = [f"• <b>{z['type']}</b> @ <code>${z['pivot']:.2f}</code> ({esc(z['why'])})" for z in added[:5]]
        tg_send(
            f"🧭 <b>[GOLD DEEP BAND SYNC | {esc(provider)}]</b>\n"
            f"• Zone Vacuum (&lt;2 zones in ±$100) triggered 3-Yr Band Slice (<code>${low_b}–${high_b}</code>)\n"
            f"• Added <b>{len(added)}</b> Historical + Current-Day Blue Boxes for Python tracking:\n" + "\n".join(lines)
        )


def check_zone_vacuum_and_trigger_band(state: GoldState, zones: list, ltp: float, df_5m: pd.DataFrame):
    nearby = [z for z in zones if abs(float(z["pivot"]) - ltp) <= VACUUM_RADIUS_USD]
    if len(nearby) >= 2:
        return
    band_bucket = int(round(ltp / 100.0) * 100)
    lock_key = f"band_slice_lock_{band_bucket}"
    if state.get(lock_key):
        return
    state.set(lock_key, True, ttl=3600)
    log.info(f"[GOLD] Zone Vacuum detected (<2 zones in ${ltp:.2f} ± ${VACUUM_RADIUS_USD}). Launching Historical Band Slice...")
    threading.Thread(
        target=_run_historical_band_slice_worker,
        args=(state.store, ltp, df_5m),
        daemon=True,
    ).start()


# ==============================================================================
# SCHEDULED 23:30 BIG ANALYSIS + EVERY 6 HOURS SYNC (05:30, 11:30, 17:30 IST)
# ==============================================================================
MACRO_6H_SYS_PROMPT = (
    "You are the Gold (XAUTUSD) Institutional Pure Liquidity Architect (1D -> 4H -> 1H -> 15M -> 5M).\n"
    "Zero EMA/RSI/ATR. Analyze market structure, determine regime, and mark untouched Blue Boxes "
    "(including current-day newly formed liquidity zones in the last 6 hours) for Python to track.\n"
    "All candle values are delta-encoded from BASE (real price = delta + BASE).\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"regime": "TRENDING_BULL|TRENDING_BEAR|SIDEWAYS", '
    '"if_then_plan": "1 concise sentence IF-THEN liquidity plan", '
    '"zones": [{"type": "RESISTANCE|SUPPORT", "tf": "1D|4H|1H|CURR_DAY", "pivot": 0.0, "why": "short reason"}]}'
)


def _execute_6h_macro_worker(shared_store, is_daily_2330: bool):
    state = GoldState(shared_store)
    now = datetime.now(IST)

    df_1d = fetch_delta_candles("1d", hours_back=24 * 90, closed_only=True) if is_daily_2330 else None
    df_4h = fetch_delta_candles("4h", hours_back=24 * (90 if is_daily_2330 else 15), closed_only=True)
    df_1h = fetch_delta_candles("1h", hours_back=24 * (30 if is_daily_2330 else 7), closed_only=True)
    df_15m = fetch_delta_candles("15m", hours_back=24 * (7 if is_daily_2330 else 3), closed_only=True)
    df_5m = fetch_delta_candles("5m", hours_back=48, closed_only=True)

    if df_5m.empty or df_1h.empty:
        return

    ltp = float(df_5m["c"].iloc[-1])
    pa_regime = get_pure_pa_regime(df_4h, df_1h)
    py_zones = extract_pure_blue_boxes(df_1d, df_4h, df_1h, df_5m, ltp, now, state)
    state.set("active_zones", py_zones, ttl=86400)
    state.set("regime", pa_regime, ttl=25200)

    title = "23:30 IST DAILY BIG MULTI-TF ANALYSIS" if is_daily_2330 else f"6-HOUR INTRADAY LIQUIDITY SYNC ({now.strftime('%H:%M IST')})"
    near_txt = "\n".join([f"• <b>{z['type']}</b> ({z['tf']}): <code>${z['pivot']:.2f}</code>" for z in py_zones[:4]]) or "• None within $200"

    tg_send(
        f"🏛️ <b>[GOLD - XAUTUSD] {title}</b>\n"
        f"📊 <b>LTP:</b> <b>${ltp:.2f}</b> | <b>PA Regime:</b> <code>{pa_regime}</code> | <b>Zones:</b> <code>{len(py_zones)}</code>\n"
        f"{near_txt}\n⏳ <i>Running {esc(HYDROGEN_MODEL)} background zone sync...</i>"
    )

    base_p = round(math.floor(ltp / 50.0) * 50.0, 2)
    usr_p = (
        f"Asset: GOLD (XAUTUSD) | Mode: {title} | Time: {now.strftime('%Y-%m-%d %H:%M IST')} | LTP: ${ltp:.2f}\n"
        f"Base Price for Delta Decoding: ${base_p}\n"
        f"Python Pre-Detected Zones: {json.dumps([{'type': z['type'], 'tf': z['tf'], 'pivot': z['pivot'], 'age_d': z['age_days']} for z in py_zones[:10]])}\n"
        + (f"1D Candles (30): {encode_delta_candles(df_1d, 30, base_p, '%Y-%m-%d')}\n" if df_1d is not None and not df_1d.empty else "")
        + f"4H Candles: {encode_delta_candles(df_4h, 45 if is_daily_2330 else 18, base_p)}\n"
        + f"1H Candles: {encode_delta_candles(df_1h, 48 if is_daily_2330 else 36, base_p)}\n"
        + f"15M Candles: {encode_delta_candles(df_15m, 32, base_p)}\n"
        + f"5M Candles (Last 6H=72 bars): {encode_delta_candles(df_5m, 48 if is_daily_2330 else 72, base_p)}"
    )

    ai_res, provider = call_gemini_chain(HEAVY_AI_CHAIN, MACRO_6H_SYS_PROMPT, usr_p)
    if not ai_res:
        log.warning(f"[GOLD] 6H/Macro AI unreachable ({provider}); Python zones remain active.")
        return

    ai_regime = ai_res.get("regime", pa_regime)
    state.set("regime", ai_regime, ttl=25200)
    ai_saved = state.get("ai_zones") or []
    new_cnt = 0
    for idx, z in enumerate((ai_res.get("zones") or [])[:12]):
        try:
            piv = round(float(z["pivot"]), 2)
            ztype = str(z.get("type", "RESISTANCE")).upper()
            if ztype not in ("RESISTANCE", "SUPPORT") or abs(piv - ltp) > DEEP_BAND_USD:
                continue
            zid = f"AI_6H_{ztype[:3]}_{int(piv*100)}_{now.strftime('%Y%m%d_%H')}_{idx}"
            ai_saved.append({
                "id": zid, "type": ztype, "tf": str(z.get("tf", "CURR_DAY")), "prio": 2,
                "pivot": piv, "age_days": 0, "touches": 1, "flipped": False,
                "source": "AI_6H_SYNC", "why": str(z.get("why", "AI Marked Blue Box"))
            })
            new_cnt += 1
        except Exception:
            continue

    if new_cnt:
        state.set("ai_zones", ai_saved[-35:], ttl=86400)
        refreshed = extract_pure_blue_boxes(df_1d, df_4h, df_1h, df_5m, ltp, now, state)
        state.set("active_zones", refreshed, ttl=86400)

    tg_send(
        f"🧠 <b>[GOLD AI ZONE SYNC | {esc(provider)}]</b>\n"
        f"🧭 <b>AI Regime:</b> <b>{esc(ai_regime)}</b> | <b>Saved Zones:</b> <code>{new_cnt}</code>\n"
        f"📋 <b>Plan:</b> <i>{esc(ai_res.get('if_then_plan', 'Tracking Blue Box -> Yellow Box hunts.'))}</i>"
    )


def trigger_6h_macro_if_due(shared_store=None, force: bool = False):
    state = GoldState(shared_store)
    now = datetime.now(IST)
    slot_hours = [5, 11, 17, 23]
    cur_h = now.hour if force else (now.hour if (now.hour in slot_hours and 30 <= now.minute <= 50) else None)
    if cur_h is None:
        return

    slot_key = f"macro_6h_done_{now.strftime('%Y-%m-%d')}_{cur_h}"
    if not force and state.get(slot_key):
        return
    state.set(slot_key, True, ttl=21600)
    is_daily_2330 = (cur_h == 23) or force
    threading.Thread(
        target=_execute_6h_macro_worker,
        args=(shared_store, is_daily_2330),
        daemon=True,
    ).start()


# ==============================================================================
# PURE LIQUIDITY SETUP DETECTOR (5M HTF/SHELF/PT10/FLIP/DUAL + 15M PULLBACK)
# ==============================================================================
def _check_yellow_box_reclaim(df: pd.DataFrame, pivot: float, action: str, tol: float = ZONE_BUFFER_USD):
    if len(df) < 3:
        return False, 0.0, ""
    c1, c2 = df.iloc[-1], df.iloc[-2]
    o1, h1, l1, cl1 = float(c1["o"]), float(c1["h"]), float(c1["l"]), float(c1["c"])
    o2, h2, l2, cl2 = float(c2["o"]), float(c2["h"]), float(c2["l"]), float(c2["c"])
    rng1 = max(h1 - l1, 0.05)
    up_w1, dn_w1 = (h1 - max(o1, cl1)) / rng1, (min(o1, cl1) - l1) / rng1

    if action == "BUY_LONG":
        if up_w1 > 0.38 and cl1 < h1 - rng1 * 0.35:
            return False, 0.0, ""
        if l1 <= pivot + tol and cl1 > pivot and (cl1 > o1 or dn_w1 >= 0.45):
            return True, l1, f"1-Bar Yellow Box Sweep (${l1:.2f} <= ${pivot:.2f}) & Reclaim (${cl1:.2f})"
        low_2c = min(l1, l2)
        if low_2c <= pivot + tol and cl1 > o1 and cl1 > pivot and cl1 >= max(o2, cl2) - 0.50:
            return True, low_2c, f"2-Bar Yellow Box Sweep (${low_2c:.2f} <= ${pivot:.2f}) & Reclaim (${cl1:.2f})"

    elif action == "SELL_SHORT":
        if dn_w1 > 0.38 and cl1 > l1 + rng1 * 0.35:
            return False, 0.0, ""
        if h1 >= pivot - tol and cl1 < pivot and (cl1 < o1 or up_w1 >= 0.45):
            return True, h1, f"1-Bar Yellow Box Sweep (${h1:.2f} >= ${pivot:.2f}) & Reject (${cl1:.2f})"
        high_2c = max(h1, h2)
        if high_2c >= pivot - tol and cl1 < o1 and cl1 < pivot and cl1 <= min(o2, cl2) + 0.50:
            return True, high_2c, f"2-Bar Yellow Box Sweep (${high_2c:.2f} >= ${pivot:.2f}) & Reject (${cl1:.2f})"

    return False, 0.0, ""


def detect_gold_pure_liquidity_setup(m5: pd.DataFrame, m15: pd.DataFrame, zones: list, regime: str, state: GoldState):
    ltp = float(m5["c"].iloc[-1])
    sc5 = _median_range(m5, 20)
    o1, h1, l1, cl1 = float(m5["o"].iloc[-1]), float(m5["h"].iloc[-1]), float(m5["l"].iloc[-1]), ltp
    rng1 = max(h1 - l1, 0.05)
    up_w1, dn_w1 = (h1 - max(o1, cl1)) / rng1, (min(o1, cl1) - l1) / rng1

    # Update polarity flips on broken 1D/4H/1H zones
    zones_changed = False
    for b in zones:
        if not b.get("flipped") and b.get("tf") in ("1D_BLUE_BOX", "4H_BLUE_BOX", "1H_BLUE_BOX"):
            piv = float(b["pivot"])
            if b["type"] == "RESISTANCE" and cl1 > piv + FLIP_BREAK_USD and float(m5["c"].iloc[-2]) > piv + 1.0:
                b["type"] = "SUPPORT"
                b["flipped"] = True
                zones_changed = True
            elif b["type"] == "SUPPORT" and cl1 < piv - FLIP_BREAK_USD and float(m5["c"].iloc[-2]) < piv - 1.0:
                b["type"] = "RESISTANCE"
                b["flipped"] = True
                zones_changed = True
    if zones_changed:
        state.set("active_zones", zones, ttl=86400)

    # Point #10 Exhaustion Momentum Check
    c_tail = m5.iloc[-4:-1]
    prev_bar = m5.iloc[-2]
    big_up_in = (int((c_tail["c"] > c_tail["o"]).sum()) == 3) or ((float(prev_bar["c"]) - float(prev_bar["o"])) >= 2.2 * sc5)
    big_dn_in = (int((c_tail["c"] < c_tail["o"]).sum()) == 3) or ((float(prev_bar["o"]) - float(prev_bar["c"])) >= 2.2 * sc5)

    # 1. Persistent Old / HTF Blue Boxes + Multi-Touch Shelf + Point #10 Exhaustion + Breakout Flip + Current-Day Dynamic Zones
    for b in zones:
        zid = b["id"]
        if state.is_spent(zid):
            continue
        lvl = float(b["pivot"])
        if abs(lvl - ltp) > 12.0:
            continue
        action = "SELL_SHORT" if b["type"] == "RESISTANCE" else "BUY_LONG"

        if b.get("flipped"):
            if (action == "BUY_LONG" and regime != "TRENDING_BULL") or (action == "SELL_SHORT" and regime != "TRENDING_BEAR"):
                continue
            ok, ext, desc = _check_yellow_box_reclaim(m5, lvl, action, tol=ZONE_BUFFER_USD)
            if ok:
                state.mark_spent(zid)
                return {
                    "name": "BREAKOUT_FLIP_HUNT", "action": action, "zone_id": zid, "blue_box": lvl,
                    "wick_extreme": round(ext, 2), "entry": ltp,
                    "reason": f"Broken {b['tf']} Polarity Flip Retest — {desc}"
                }
        else:
            is_major = (b.get("tf") in ("1D_BLUE_BOX", "4H_BLUE_BOX", "HIST_1D", "HIST_4H")) or (b.get("age_days", 0) >= 5) or (b.get("touches", 1) >= 2)
            if not is_major and ((action == "SELL_SHORT" and regime == "TRENDING_BULL") or (action == "BUY_LONG" and regime == "TRENDING_BEAR")):
                continue

            ok, ext, desc = _check_yellow_box_reclaim(m5, lvl, action, tol=ZONE_BUFFER_USD)
            if ok:
                state.mark_spent(zid)
                if (action == "SELL_SHORT" and big_up_in) or (action == "BUY_LONG" and big_dn_in):
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

    # 2. 15M Trend Pullback Liquidity Hunt (Gold's +32.5R Proven Trend Engine — evaluated on closed 15M candle)
    if m15 is not None and len(m15) >= 16:
        last_15m_t = int(m15["time"].iloc[-1])
        if state.get("last_eval_15m") != last_15m_t:
            state.set("last_eval_15m", last_15m_t, ttl=1200)
            sc15 = _median_range(m15, 20)
            hh15, ll15 = m15["h"].values, m15["l"].values
            k = len(m15) - 1
            o15, h15, l15, c15 = float(m15["o"].iloc[k]), float(hh15[k]), float(ll15[k]), float(m15["c"].iloc[k])
            rng15 = max(h15 - l15, 0.05)
            up_w15, dn_w15 = (h15 - max(o15, c15)) / rng15, (min(o15, c15) - l15) / rng15

            if regime == "TRENDING_BULL":
                pb_idx = k - 6 + int(np.argmin(ll15[k - 6 : k - 1]))
                pb_low = float(ll15[pb_idx])
                imp_h = float(np.max(hh15[max(0, pb_idx - 8) : pb_idx]))
                if (imp_h - pb_low) >= 1.2 * sc15 and pb_low > float(np.min(ll15[max(0, pb_idx - 12) : pb_idx])):
                    if l15 < pb_low and c15 > pb_low and c15 > o15 and (dn_w15 >= 0.25 or (c15 - o15) >= 0.4 * sc15):
                        return {
                            "name": "TREND_PULLBACK_LIQ_HUNT", "action": "BUY_LONG", "zone_id": f"G15_PB_L_{last_15m_t}",
                            "blue_box": pb_low, "wick_extreme": round(l15, 2), "entry": c15,
                            "reason": f"15M Bullish Impulse Pullback Sweep below ${pb_low:.2f} (Wick ${l15:.2f}) & Green Reclaim (${c15:.2f})"
                        }
            elif regime == "TRENDING_BEAR":
                pb_idx = k - 6 + int(np.argmax(hh15[k - 6 : k - 1]))
                pb_high = float(hh15[pb_idx])
                imp_l = float(np.min(ll15[max(0, pb_idx - 8) : pb_idx]))
                if (pb_high - imp_l) >= 1.2 * sc15 and pb_high < float(np.max(hh15[max(0, pb_idx - 12) : pb_idx])):
                    if h15 > pb_high and c15 < pb_high and c15 < o15 and (up_w15 >= 0.25 or (o15 - c15) >= 0.4 * sc15):
                        return {
                            "name": "TREND_PULLBACK_LIQ_HUNT", "action": "SELL_SHORT", "zone_id": f"G15_PB_H_{last_15m_t}",
                            "blue_box": pb_high, "wick_extreme": round(h15, 2), "entry": c15,
                            "reason": f"15M Bearish Impulse Pullback Sweep above ${pb_high:.2f} (Wick ${h15:.2f}) & Red Reclaim (${c15:.2f})"
                        }

    # 3. Dual-Side Liquidity Hunt (Point #7 & #8)
    if len(m5) >= 20:
        bh = float(m5["h"].iloc[-18:-5].max())
        bl = float(m5["l"].iloc[-18:-5].min())
        if 3.5 <= (bh - bl) <= 12.0:
            mid_h = float(m5["h"].iloc[-5:-1].max())
            mid_l = float(m5["l"].iloc[-5:-1].min())
            if mid_h > bh and mid_l >= bl and regime != "TRENDING_BEAR":
                ok, ext, desc = _check_yellow_box_reclaim(m5, bl, "BUY_LONG", tol=ZONE_BUFFER_USD)
                if ok:
                    return {
                        "name": "DUAL_SIDE_HUNT_PT7", "action": "BUY_LONG", "zone_id": f"GDUAL_L_{int(bl*100)}",
                        "blue_box": bl, "wick_extreme": round(ext, 2), "entry": ltp,
                        "reason": f"Dual-Side Hunt: Swept High (${bh:.2f}) first, then Low (${bl:.2f}) -> {desc}"
                    }
            elif mid_l < bl and mid_h <= bh and regime != "TRENDING_BULL":
                ok, ext, desc = _check_yellow_box_reclaim(m5, bh, "SELL_SHORT", tol=ZONE_BUFFER_USD)
                if ok:
                    return {
                        "name": "DUAL_SIDE_HUNT_PT7", "action": "SELL_SHORT", "zone_id": f"GDUAL_H_{int(bh*100)}",
                        "blue_box": bh, "wick_extreme": round(ext, 2), "entry": ltp,
                        "reason": f"Dual-Side Hunt: Swept Low (${bl:.2f}) first, then High (${bh:.2f}) -> {desc}"
                    }

    return None


# ==============================================================================
# PARALLEL NON-BLOCKING BACKGROUND AI CONFIRMATION
# ==============================================================================
LIVE_CONFIRM_SYS_PROMPT = (
    "You are the Gold (XAUTUSD) Institutional Pure Liquidity Supervisor (Zero Indicators).\n"
    "Python already fired a fast Yellow Box sweep alert. Perform an independent price-action audit.\n"
    "Decode delta candles using Base Price. Check wick quality, zone authenticity, and opposing walls.\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"verdict": "APPROVED|WAIT_CAUTION|REJECT_TRAP", "confidence": "HIGH|MEDIUM|LOW", "note": "1-2 concise sentences"}'
)


def _background_ai_confirm_worker(setup: dict, usr_p: str, signal_time: str):
    ai_res, provider = call_gemini_chain(LIVE_CONFIRM_CHAIN, LIVE_CONFIRM_SYS_PROMPT, usr_p)
    if not ai_res:
        log.warning(f"[GOLD] Background AI confirm unreachable ({provider})")
        return
    verdict = ai_res.get("verdict", "APPROVED")
    conf = ai_res.get("confidence", "MEDIUM")
    note = ai_res.get("note", "")
    badge = "✅" if verdict == "APPROVED" else ("⚠️" if verdict == "WAIT_CAUTION" else "🛑")
    tg_send(
        f"{badge} <b>[GOLD AI CONFIRMATION | {signal_time}]</b>\n"
        f"⚡ <b>Setup:</b> <code>{esc(setup['name'])}</code> ({esc(setup['action'])}) @ <b>${setup['entry']:.2f}</b>\n"
        f"🧠 <b>Model:</b> <code>{esc(provider)}</code> | <b>Verdict:</b> <b>[{esc(verdict)} | {esc(conf)}]</b>\n"
        f"<i>{esc(note)}</i>"
    )


def run_startup_diagnostics(shared_store=None) -> dict:
    log.info("[GOLD] Running Startup Diagnostics...")
    t0 = time.time()
    df_t = fetch_delta_candles("5m", hours_back=3, closed_only=False)
    api_ms = int((time.time() - t0) * 1000)
    ok = not df_t.empty
    ltp = float(df_t["c"].iloc[-1]) if ok else 0.0
    if ok:
        trigger_6h_macro_if_due(shared_store, force=True)
    return {"asset": "GOLD", "api_ok": ok, "api_ms": api_ms, "ltp": ltp}


# ==============================================================================
# MAIN 5M CYCLE (PYTHON-FIRST INSTANT ALERT -> PARALLEL AI)
# ==============================================================================
def run_gold_cycle(shared_store=None):
    if not _gold_lock.acquire(blocking=False):
        return
    try:
        state = GoldState(shared_store)

        # 1. Check 23:30 BIG Analysis & Every 6-Hour Sync (05:30, 11:30, 17:30 IST)
        trigger_6h_macro_if_due(shared_store, force=False)

        # 2. Fetch closed 5M candles
        df_5m = fetch_delta_candles("5m", hours_back=36, closed_only=True)
        if df_5m.empty or len(df_5m) < 30:
            return

        last_bar_t = int(df_5m["time"].iloc[-1])
        if state.get("last_eval_5m") == last_bar_t:
            return
        state.set("last_eval_5m", last_bar_t, ttl=300)

        ltp = float(df_5m["c"].iloc[-1])
        close_ts = _ensure_ist(df_5m["ts"].iloc[-1]) + timedelta(minutes=5)
        df_15m = fetch_delta_candles("15m", hours_back=48, closed_only=True)

        regime = state.get("regime") or "SIDEWAYS"
        zones = state.get("active_zones")
        if zones is None:
            df_1h = fetch_delta_candles("1h", hours_back=24 * 7, closed_only=True)
            df_4h = fetch_delta_candles("4h", hours_back=24 * 15, closed_only=True)
            regime = get_pure_pa_regime(df_4h, df_1h)
            zones = extract_pure_blue_boxes(None, df_4h, df_1h, df_5m, ltp, close_ts, state)
            state.set("regime", regime, ttl=14400)
            state.set("active_zones", zones, ttl=21600)

        # 3. Check Zone Vacuum (< 2 zones in LTP ± $100) -> Dynamic 3-Yr Band Slice / New High Fallback
        check_zone_vacuum_and_trigger_band(state, zones, ltp, df_5m)

        # 4. Evaluate Pure Liquidity Setups (0-Lag Python)
        setup = detect_gold_pure_liquidity_setup(df_5m, df_15m, zones, regime, state)
        if not setup:
            log.info(f"[GOLD] {close_ts.strftime('%H:%M IST')} | LTP:${ltp:.2f} | Regime:{regime} | Zones:{len(zones)} -> WAITING")
            return

        last_dir = state.get("last_sig_dir")
        last_dir_t = state.get("last_sig_ts") or 0
        if last_dir and last_dir != setup["action"] and (time.time() - last_dir_t) < OPPOSITE_LOCK_MIN * 60:
            return

        cd_key = f"cd_{setup['name']}_{setup['action']}"
        if state.get(cd_key):
            return

        state.set("last_sig_dir", setup["action"], ttl=OPPOSITE_LOCK_MIN * 60)
        state.set("last_sig_ts", time.time(), ttl=OPPOSITE_LOCK_MIN * 60)
        state.set(cd_key, True, ttl=SETUP_COOLDOWN_MIN * 60)

        # 5. PYTHON-FIRST INSTANT TELEGRAM ALERT (Entry-Only, Manual SL/Target)
        em = "🟢" if setup["action"] == "BUY_LONG" else "🔴"
        title_act = "BUY (LONG)" if setup["action"] == "BUY_LONG" else "SELL (SHORT)"
        sig_time_str = close_ts.strftime("%H:%M IST")
        msg = (
            f"{em} <b>[GOLD - XAUTUSD] {title_act} | {sig_time_str}</b>\n\n"
            f"⚡ <b>Setup        :</b> <code>{esc(setup['name'])}</code>\n"
            f"🎯 <b>Entry Price  :</b> <b>${setup['entry']:.2f}</b>\n"
            f"🟦 <b>Blue Box Lvl :</b> <b>${setup['blue_box']:.2f}</b> | <b>Yellow Wick Ext:</b> <b>${setup['wick_extreme']:.2f}</b>\n"
            f"🧭 <b>PA Regime    :</b> <code>{esc(regime)}</code>\n"
            f"📝 <b>Sweep Logic  :</b> {esc(setup['reason'])}\n\n"
            f"⏳ <i>Chart baghun manual confirm kar (Manual SL &amp; Target mode). Running parallel AI audit...</i>"
        )
        tg_send(msg)
        log.info(f"[GOLD] 🔥 INSTANT ALERT: {setup['name']} ({setup['action']}) @ ${setup['entry']:.2f}")

        # 6. Parallel Non-Blocking Background AI Audit
        base_p = round(math.floor(ltp / 50.0) * 50.0, 2)
        delta_bars = encode_delta_candles(df_5m, 45, base_p, "%m-%d %H:%M")
        usr_p = (
            f"Asset: GOLD (XAUTUSD) | Time: {sig_time_str} | Entry: ${setup['entry']:.2f}\n"
            f"Setup: {setup['name']} ({setup['action']}) | Blue Box: ${setup['blue_box']:.2f} | Wick Ext: ${setup['wick_extreme']:.2f}\n"
            f"Regime: {regime} | Reason: {setup['reason']}\n"
            f"Base Price: ${base_p} | Recent 45 5M Delta Candles: {delta_bars}"
        )
        threading.Thread(
            target=_background_ai_confirm_worker,
            args=(setup, usr_p, sig_time_str),
            daemon=True,
        ).start()

    except Exception as e:
        log.exception(f"[GOLD] Cycle error: {type(e).__name__}")
    finally:
        try:
            _gold_lock.release()
        except RuntimeError:
            pass
