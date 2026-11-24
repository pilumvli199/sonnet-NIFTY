"""
================================================================================
BITCOIN (BTCUSD) PURE LIQUIDITY ENGINE — PRODUCTION V10.6 (HUNT-ALERT EDITION)
Namespace: BTC:* | Delta Exchange India Public API | 100% Price Action
================================================================================
V10.6 CHANGES (on top of V10.5):
  1. 1H-AGREEMENT FILTER: TREND_PULLBACK setups are skipped when the 1H regime is
     opposite to the 4H-led regime (all 3 losing shorts in the 7-day replay were
     taken while 1H was TRENDING_BULL).
  2. CLIMAX_SWEEP_RECLAIM (EXPERIMENTAL): capitulation wick that sweeps the last
     8h extreme and closes back inside. Runs BEFORE the range gate (the gate used
     to kill exactly these candles). Alert carries a large-risk warning.
  3. EARLY HUNT HEADS-UP: every 5M tick, a CLOSED 5M candle that sweeps a valid
     zone and closes back inside sends a "hunt detected" heads-up (NOT an entry)
     while the 15M candle is still forming. Full 15M signal follows at 15M close.
     Disable with BTC_EARLY_HUNT=false.
  4. AI PROMPTS V2: Python facts block (risk, targets, wicks, 1H/4H regime, nearest
     opposing zone), 5-check rubric, verdict enforced in Python (missing verdict
     no longer defaults to APPROVED), AI zone pivots must snap to REAL candle
     highs/lows (hallucinated levels are dropped).
  5. GEMINI DAILY CAP (GEMINI_DAILY_CAP, default 200 for BTC): Python alerts keep
     working when AI budget is exhausted.
  6. main.py COMPAT: run_btc_1m_tick / run_6h_analysis entry points added.
  7. Alert shows SL (wick extreme) + 1:2 / 1:3 reference levels and risk in USD.
================================================================================
"""

import os
import json
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
log = logging.getLogger("BTC")

# ==============================================================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# ==============================================================================
GEMINI_API_KEYS     = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip() and "your_" not in k.lower()]
TG_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID          = os.getenv("BTC_TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", "")).strip()

DELTA_BASE_URL      = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange").strip()
DELTA_API_KEY       = os.getenv("DELTA_API_KEY", "").strip()
if "your_" in DELTA_API_KEY.lower():
    DELTA_API_KEY = ""
BTC_SYMBOL          = os.getenv("BTC_SYMBOL", "BTCUSD").strip()

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

# Pure Liquidity Constants (BTC USD — V10.5 Backtest-Locked)
CANDLE_BUFFER_SEC   = 2
SHELF_MERGE_USD     = 95.0
ZOMBIE_BREAK_USD    = 140.0
MIN_PIERCE_USD      = 20.0
MAX_OVERSHOOT_USD   = 420.0
MIN_RNG_15M_USD     = 140.0
MAX_RNG_MULT        = 3.5
SCAN_RADIUS_USD     = 600.0
MAX_ZONE_AGE_DAYS   = 85
VACUUM_RADIUS_USD   = 2500.0
DEEP_BAND_USD       = 5000.0
SETUP_COOLDOWN_MIN  = int(os.getenv("BTC_SETUP_COOLDOWN_MIN", "60"))
OPPOSITE_LOCK_MIN   = int(os.getenv("BTC_OPPOSITE_LOCK_MIN", "45"))

# V10.6 additions
CLIMAX_LOOKBACK      = 32      # 8h of 15M bars
CLIMAX_MIN_RNG_MULT  = 2.5
CLIMAX_MAX_RNG_MULT  = 7.0
MIN_RNG_5M_USD       = 60.0    # early-hunt 5M candle minimum range
EARLY_HUNT_ENABLED   = os.getenv("BTC_EARLY_HUNT", "true").lower() == "true"
EARLY_HUNT_COOLDOWN  = 3600    # seconds, per zone
GEMINI_DAILY_CAP     = int(os.getenv("GEMINI_DAILY_CAP", "200"))

_btc_lock = threading.Lock()
_ai_rate_lock = threading.Lock()
_gemini_idx = 0
_last_gemini_call = 0.0
_daily_ai_calls = 0
_daily_ai_date = None
_cap_warned_date = None


def esc(val) -> str:
    return html.escape(str(val), quote=False) if val is not None else ""


# ==============================================================================
# NAMESPACE STATE STORE (BTC:*)
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


class BtcState:
    PREFIX = "BTC:"

    def __init__(self, shared_store=None):
        self.store = shared_store if shared_store is not None else _fallback_store

    def set(self, key: str, value, ttl: int = 86400):
        self.store.set(f"{self.PREFIX}{key}", value, ttl=ttl)

    def get(self, key: str, default=None):
        val = self.store.get(f"{self.PREFIX}{key}")
        return default if val is None else val

    def mark_spent(self, zone_id: str, ttl: int = 172800):
        if not zone_id:
            return
        spent = self.get("spent_zones") or []
        if zone_id not in spent:
            spent.append(zone_id)
            if len(spent) > 500:
                spent = spent[-500:]
            self.set("spent_zones", spent, ttl=ttl)

    def is_spent(self, zone_id: str) -> bool:
        return zone_id in (self.get("spent_zones") or [])


# ==============================================================================
# TELEGRAM HELPER
# ==============================================================================
def tg_send(text: str) -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info(f"[BTC-TG-DRY]\n{text}\n")
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
        log.error(f"[BTC] Telegram send error: {type(e).__name__}")
        return False


# ==============================================================================
# GEMINI AI ENGINE (SMART 429/503 BACKOFF + NO thinkingBudget:0 BUG + DAILY CAP)
# ==============================================================================
def _bump_ai_counter():
    global _daily_ai_calls, _daily_ai_date
    today = datetime.now(IST).strftime("%Y-%m-%d")
    if _daily_ai_date != today:
        _daily_ai_date = today
        _daily_ai_calls = 0
    _daily_ai_calls += 1


def _ai_budget_left() -> bool:
    """False once today's BTC Gemini calls reach GEMINI_DAILY_CAP (Python alerts keep working)."""
    global _daily_ai_calls, _daily_ai_date, _cap_warned_date
    today = datetime.now(IST).strftime("%Y-%m-%d")
    if _daily_ai_date != today:
        _daily_ai_date = today
        _daily_ai_calls = 0
    if _daily_ai_calls >= GEMINI_DAILY_CAP:
        if _cap_warned_date != today:
            _cap_warned_date = today
            log.warning(f"[BTC] Gemini daily cap reached ({_daily_ai_calls}/{GEMINI_DAILY_CAP}) — AI skipped, Python alerts continue")
        return False
    return True


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

    attempts = max(2, len(GEMINI_API_KEYS))
    srv_503_hits = 0
    for attempt in range(attempts):
        if not _ai_budget_left():
            return None
        with _ai_rate_lock:
            wait = 2.2 - (time.time() - _last_gemini_call)
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
                backoff = min(6.0, 1.5 + attempt * 1.2)
                log.warning(f"[BTC] {model_name} HTTP 429 — rotating key after {backoff:.1f}s...")
                time.sleep(backoff)
                continue
            if r.status_code in (500, 502, 503, 504):
                srv_503_hits += 1
                if srv_503_hits <= 1:
                    log.warning(f"[BTC] {model_name} HTTP {r.status_code} — quick 1.5s retry...")
                    time.sleep(1.5)
                    continue
                log.warning(f"[BTC] {model_name} HTTP {r.status_code} — falling back to next model...")
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
                log.warning(f"[BTC] {model_name} HTTP {r.status_code}")
                break
        except requests.Timeout:
            log.warning(f"[BTC] {model_name} timeout ({timeout_sec}s) — retrying once...")
            time.sleep(1.5)
            continue
        except Exception as e:
            log.warning(f"[BTC] {model_name} error ({type(e).__name__}) — retrying once...")
            time.sleep(1.5)
            continue
    return None


def call_gemini_chain(chain: list, sys_prompt: str, user_prompt: str):
    for model_name, t_lvl, t_sec, m_tok in chain:
        res = call_gemini_single(model_name, t_lvl, t_sec, m_tok, sys_prompt, user_prompt)
        if res is not None:
            return res, f"{model_name} [{t_lvl}]"
    return None, "AI_UNREACHABLE"


# ==============================================================================
# DELTA EXCHANGE CANDLES FETCHER + REAL OHLC FORMATTER (ZERO "DELTA" WORD)
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
            params={"symbol": BTC_SYMBOL, "resolution": resolution, "start": start_ts, "end": end_ts},
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
                df[col] = df[col].astype(float).round(1)
            df["v"] = df["v"].astype(float)
            df = df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)

            if closed_only and not df.empty:
                last_open = int(df.iloc[-1]["time"])
                if end_ts < (last_open + res_sec + CANDLE_BUFFER_SEC):
                    df = df.iloc[:-1].reset_index(drop=True)
            return df[["time", "ts", "o", "h", "l", "c", "v"]]
    except Exception as e:
        log.error(f"[BTC] Market feed error ({resolution}): {type(e).__name__}")
    return pd.DataFrame()


def format_ohlc_candles(df: pd.DataFrame, limit: int, fmt: str = "%m-%d %H:%M") -> list:
    if df is None or df.empty:
        return []
    sub = df.tail(limit)
    out = []
    for _, r in sub.iterrows():
        t_s = _ensure_ist(r["ts"]).strftime(fmt)
        out.append(f"{t_s} O:{float(r['o']):.1f} H:{float(r['h']):.1f} L:{float(r['l']):.1f} C:{float(r['c']):.1f}")
    return out


# ==============================================================================
# AI SAFETY HELPERS (V10.6): REAL-PIVOT SNAP + PYTHON FACTS + ENFORCED VERDICT
# ==============================================================================
def _snap_pivot_to_real_extreme(piv: float, ztype: str, frames: list, tol: float = 60.0):
    """AI pivot must match a REAL candle High (RESISTANCE) / Low (SUPPORT) in the supplied data
    within tol USD. Returns the snapped real price, or None (hallucinated level -> rejected)."""
    col = "h" if ztype == "RESISTANCE" else "l"
    best = None
    for df in frames:
        if df is None or df.empty:
            continue
        vals = df[col].values
        j = int(np.argmin(np.abs(vals - piv)))
        if abs(vals[j] - piv) <= tol and (best is None or abs(vals[j] - piv) < abs(best - piv)):
            best = float(vals[j])
    return best


def build_confirm_facts(setup: dict, df_15m: pd.DataFrame, zones: list, regime: str, reg_1h: str) -> str:
    """Python-computed facts so the AI audits instead of re-deriving numbers."""
    long_ = setup["action"] == "BUY_LONG"
    entry, ext = float(setup["entry"]), float(setup["wick_extreme"])
    risk = max(abs(entry - ext), 1e-9)
    tp2 = entry + 2 * risk if long_ else entry - 2 * risk
    b = df_15m.iloc[-1]
    rng = float(b["h"] - b["l"])
    dn_w = (min(b["o"], b["c"]) - b["l"]) / rng * 100 if rng else 0.0
    up_w = (b["h"] - max(b["o"], b["c"])) / rng * 100 if rng else 0.0
    last3 = float(df_15m["c"].iloc[-4:].diff().dropna().sum())
    opp = [z for z in zones if z["type"] == ("RESISTANCE" if long_ else "SUPPORT")
           and ((float(z["pivot"]) > entry) if long_ else (float(z["pivot"]) < entry))]
    opp.sort(key=lambda z: abs(float(z["pivot"]) - entry))
    nxt = opp[0] if opp else None
    nxt_txt = (f"{nxt['tf']} @ {float(nxt['pivot']):,.1f} ({abs(float(nxt['pivot']) - entry) / risk:.1f}R away)"
               if nxt else "none within scan band")
    return (
        "PYTHON FACTS (authoritative - do not recompute):\n"
        f"- Direction: {setup['action']} | Entry: {entry:,.1f} | Invalidation (wick extreme): {ext:,.1f} | Risk: {risk:,.0f} USD\n"
        f"- 1:2 price target: {tp2:,.1f}\n"
        f"- Signal candle: range {rng:,.0f} USD | lower wick {dn_w:.0f}% | upper wick {up_w:.0f}% | last-3-bar net move {last3:+,.0f}\n"
        f"- Regime 4H-led: {regime} | Regime 1H: {reg_1h}\n"
        f"- Nearest opposing zone: {nxt_txt}"
    )


def verdict_from_checks(res: dict) -> str:
    """Python enforces the rubric even if the model's own verdict disagrees.
    Missing/invalid output NEVER defaults to APPROVED."""
    keys = ["pool", "reclaim", "no_displacement", "trend_ok", "room"]
    c = (res or {}).get("checks")
    if not isinstance(c, dict) or not all(k in c for k in keys):
        v = str((res or {}).get("verdict", "WAIT_CAUTION")).upper()
        return "WAIT_CAUTION" if v == "APPROVED" else (v if v in ("WAIT_CAUTION", "REJECT_TRAP") else "WAIT_CAUTION")
    bad = [k for k in keys if c[k] is False]
    if not bad:
        return "APPROVED"
    if len(bad) == 1 and bad[0] != "reclaim":
        return "WAIT_CAUTION"
    return "REJECT_TRAP"


# ==============================================================================
# LIVE DYNAMIC PA REGIME & ZOMBIE-FREE BLUE BOX ENGINE (ZERO INDICATORS)
# ==============================================================================
def _median_range(df: pd.DataFrame, n: int = 20) -> float:
    if df is None or df.empty:
        return 150.0
    v = float((df["h"] - df["l"]).tail(n).median())
    return v if v > 0 else 150.0


def get_live_pa_regime(df_4h: pd.DataFrame, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> tuple:
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
    disp_15m = "NONE"
    if df_15m is not None and len(df_15m) >= 10:
        sc15 = _median_range(df_15m, 20)
        mv = float(df_15m["c"].iloc[-1] - df_15m["c"].iloc[-5])
        if mv > 3.0 * sc15:
            disp_15m = "TRENDING_BULL"
        elif mv < -3.0 * sc15:
            disp_15m = "TRENDING_BEAR"

    if r4 == "TRENDING_BULL":
        reg = "TRENDING_BULL"
    elif r4 == "TRENDING_BEAR":
        reg = "TRENDING_BEAR"
    elif r1 == "TRENDING_BULL":
        reg = "TRENDING_BULL"
    elif r1 == "TRENDING_BEAR":
        reg = "TRENDING_BEAR"
    else:
        reg = "SIDEWAYS"
    return reg, disp_15m


def _is_zone_broken_or_zombie(z: dict, df_15m: pd.DataFrame, ltp: float) -> bool:
    piv = float(z["pivot"])
    ztype = z["type"]
    if ztype == "RESISTANCE":
        if ltp > piv + ZOMBIE_BREAK_USD:
            return True
        if df_15m is not None and len(df_15m) >= 3 and float(df_15m["c"].iloc[-2]) > piv + ZOMBIE_BREAK_USD:
            return True
    elif ztype == "SUPPORT":
        if ltp < piv - ZOMBIE_BREAK_USD:
            return True
        if df_15m is not None and len(df_15m) >= 3 and float(df_15m["c"].iloc[-2]) < piv - ZOMBIE_BREAK_USD:
            return True
    return False


def extract_pure_blue_boxes(df_1d: pd.DataFrame, df_4h: pd.DataFrame, df_1h: pd.DataFrame, df_15m: pd.DataFrame, ltp: float, now_ist: datetime, state: BtcState) -> list:
    specs = [
        (df_1d, "1D_BLUE_BOX", 3, 2, 1.3, 1),
        (df_4h, "4H_BLUE_BOX", 5, 2, 1.7, 2),
        (df_1h, "1H_BLUE_BOX", 6, 3, 1.8, 3),
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
                    zid = f"{tf_lbl}_H_{int(hh[k])}_{formed_t.strftime('%Y%m%d%H')}"
                    if not state.is_spent(zid) and abs(hh[k] - ltp) <= DEEP_BAND_USD:
                        raw_boxes.append({
                            "id": zid, "type": "RESISTANCE", "tf": tf_lbl, "prio": prio,
                            "pivot": round(float(hh[k]), 1), "age_days": age_d, "touches": 1,
                            "source": "PYTHON_HTF", "why": f"{tf_lbl} ({age_d}d old) @ ${hh[k]:,.1f}"
                        })

            if ll[k] == np.min(ll[k - lb : k + rb + 1]):
                rev = float(np.max(hh[k + 1 : k + rb + 1]) - ll[k])
                sub_l, sub_c = ll[k + rb + 1 : -1], cc[k + rb + 1 : -1]
                if rev >= rev_mult * sc and (len(sub_l) == 0 or (np.min(sub_l) >= ll[k] and np.min(sub_c) >= ll[k])):
                    zid = f"{tf_lbl}_L_{int(ll[k])}_{formed_t.strftime('%Y%m%d%H')}"
                    if not state.is_spent(zid) and abs(ll[k] - ltp) <= DEEP_BAND_USD:
                        raw_boxes.append({
                            "id": zid, "type": "SUPPORT", "tf": tf_lbl, "prio": prio,
                            "pivot": round(float(ll[k]), 1), "age_days": age_d, "touches": 1,
                            "source": "PYTHON_HTF", "why": f"{tf_lbl} ({age_d}d old) @ ${ll[k]:,.1f}"
                        })

    for az in (state.get("ai_zones") or []):
        if not state.is_spent(az["id"]) and abs(float(az["pivot"]) - ltp) <= DEEP_BAND_USD:
            raw_boxes.append(az)

    clean_boxes = []
    for z in raw_boxes:
        if _is_zone_broken_or_zombie(z, df_15m, ltp):
            state.mark_spent(z["id"])
        else:
            clean_boxes.append(z)

    clean_boxes.sort(key=lambda x: (x.get("prio", 3), abs(float(x["pivot"]) - ltp)))
    merged = []
    for cand in clean_boxes:
        m = next((x for x in merged if x["type"] == cand["type"] and abs(float(x["pivot"]) - float(cand["pivot"])) <= SHELF_MERGE_USD), None)
        if m:
            if not str(m.get("source", "")).startswith("AI_") and not str(cand.get("source", "")).startswith("AI_"):
                m["touches"] = m.get("touches", 1) + 1
            m["age_days"] = max(m.get("age_days", 0), cand.get("age_days", 0))
            m["pivot"] = round(max(float(m["pivot"]), float(cand["pivot"])) if cand["type"] == "RESISTANCE" else min(float(m["pivot"]), float(cand["pivot"])), 1)
        else:
            merged.append(cand)

    merged.sort(key=lambda z: (z.get("prio", 3), -z.get("touches", 1), abs(float(z["pivot"]) - ltp)))
    return merged[:20]


# ==============================================================================
# DYNAMIC HISTORICAL PRICE-BAND SLICE (ZONE VACUUM < 2 ZONES IN LTP ± $2,500)
# ==============================================================================
AI_PIVOT_RULE = (
    "\nSTRICT: every zone pivot MUST equal an exact High (RESISTANCE) or Low (SUPPORT) printed in the supplied candles - "
    "never a round/psychological number. Add \"evidence\": \"<candle time you took it from>\". "
    "If no genuine untouched level exists, return an empty zones list. Max 6 zones."
)

BAND_SLICE_SYS_PROMPT = (
    "You are the Bitcoin (BTCUSD) Historical Liquidity Archaeologist (100% Pure Price Action — No Indicators).\n"
    "BTC entered an area with fewer than 2 nearby untouched zones.\n"
    "Analyze ONLY the supplied real OHLC historical candles inside LTP ± $5,000 + recent 15M candles.\n"
    "Mark ONLY genuine untouched RESISTANCE strictly ABOVE current LTP and SUPPORT strictly BELOW current LTP.\n"
    "A level is untouched only if no later supplied candle traded through it.\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"regime_update": "TRENDING_BULL|TRENDING_BEAR|SIDEWAYS", '
    '"zones": [{"type": "RESISTANCE|SUPPORT", "tf": "HIST_1D|HIST_4H", "pivot": 0.0, "evidence": "candle time", "why": "short reason"}]}'
    + AI_PIVOT_RULE
)


def _run_historical_band_slice_worker(shared_store, ltp: float, df_15m: pd.DataFrame):
    state = BtcState(shared_store)
    now = datetime.now(IST)
    low_b, high_b = round(ltp - DEEP_BAND_USD, 1), round(ltp + DEEP_BAND_USD, 1)

    df_1d_3y = fetch_delta_candles("1d", hours_back=24 * 1095, closed_only=True)
    df_4h_10m = fetch_delta_candles("4h", hours_back=24 * 300, closed_only=True)

    hist_hits = []
    for df_h, lbl in [(df_1d_3y, "1D"), (df_4h_10m, "4H")]:
        if df_h is None or df_h.empty:
            continue
        in_band = df_h[(df_h["h"] >= low_b) & (df_h["l"] <= high_b)].tail(30)
        for _, r in in_band.iterrows():
            dt_s = _ensure_ist(r["ts"]).strftime("%Y-%m-%d %H:%M")
            hist_hits.append(f"{lbl} {dt_s} O:{r['o']:.1f} H:{r['h']:.1f} L:{r['l']:.1f} C:{r['c']:.1f}")

    if not hist_hits:
        log.info(f"[BTC] 🚀 Uncharted Price / New High @ ${ltp:,.1f} (0 historical bars in ${low_b:,.0f}-${high_b:,.0f}). Running 15M Trend Pullback Mode.")
        tg_send(
            f"🚀 <b>[BITCOIN NEW PRICE TERRITORY] ${ltp:,.1f}</b>\n"
            f"• 3-Year history has 0 candles in <code>${low_b:,.0f}–${high_b:,.0f}</code>.\n"
            f"• Tracking <b>15M Trend Pullback Liquidity Hunts</b>."
        )
        return

    usr_p = (
        f"Asset: BITCOIN (BTCUSD) | Current LTP: ${ltp:,.1f} | Band: ${low_b:,.1f} to ${high_b:,.1f}\n"
        f"Historical Real OHLC Inside Band ({len(hist_hits)} bars):\n" + "\n".join(hist_hits[:40]) + "\n\n"
        f"Recent 15M Real OHLC Candles:\n" + "\n".join(format_ohlc_candles(df_15m, 30))
    )

    ai_res, provider = call_gemini_chain(HEAVY_AI_CHAIN, BAND_SLICE_SYS_PROMPT, usr_p)
    if not ai_res or not isinstance(ai_res.get("zones"), list):
        return

    # Real-candle frames the AI could legitimately have used (in-band only)
    snap_frames = []
    for df_h in (df_1d_3y, df_4h_10m):
        if df_h is not None and not df_h.empty:
            snap_frames.append(df_h[(df_h["h"] >= low_b) & (df_h["l"] <= high_b)])

    existing_ai = state.get("ai_zones") or []
    added = []
    for z in ai_res["zones"][:10]:
        try:
            ztype = str(z.get("type", "RESISTANCE")).upper()
            piv = _snap_pivot_to_real_extreme(round(float(z["pivot"]), 1), ztype, snap_frames)
            if piv is None:
                continue
            if ztype not in ("RESISTANCE", "SUPPORT") or abs(piv - ltp) > DEEP_BAND_USD:
                continue
            if (ztype == "RESISTANCE" and piv <= ltp) or (ztype == "SUPPORT" and piv >= ltp):
                continue
            zid = f"AI_BAND_{ztype[:3]}_{int(piv)}_{now.strftime('%Y%m%d')}"
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
        lines = [f"• <b>{z['type']}</b> @ <code>${z['pivot']:,.1f}</code> ({esc(z['why'])})" for z in added[:5]]
        tg_send(
            f"🧭 <b>[BTC DEEP BAND SYNC | {esc(provider)}]</b>\n"
            f"• Zone Vacuum (&lt;2 zones in ±$2,500) triggered 3-Yr Band Slice (<code>${low_b:,.0f}–${high_b:,.0f}</code>)\n"
            f"• Added <b>{len(added)}</b> Historical Blue Boxes:\n" + "\n".join(lines)
        )


def check_zone_vacuum_and_trigger_band(state: BtcState, zones: list, ltp: float, df_15m: pd.DataFrame):
    nearby = [z for z in zones if abs(float(z["pivot"]) - ltp) <= VACUUM_RADIUS_USD]
    if len(nearby) >= 2:
        return
    band_bucket = int(round(ltp / 2500.0) * 2500)
    lock_key = f"band_slice_lock_{band_bucket}"
    if state.get(lock_key):
        return
    state.set(lock_key, True, ttl=3600)
    threading.Thread(target=_run_historical_band_slice_worker, args=(state.store, ltp, df_15m), daemon=True).start()


# ==============================================================================
# SCHEDULED 23:30 BIG ANALYSIS + EVERY 6 HOURS SYNC (05:30, 11:30, 17:30 IST)
# ==============================================================================
MACRO_6H_SYS_PROMPT = (
    "You are the Bitcoin (BTCUSD) Institutional Pure Liquidity Architect (1D -> 4H -> 1H -> 15M).\n"
    "Zero indicators (No EMA/RSI/ATR). All prices provided are REAL OHLC candle prices.\n"
    "Determine market regime and mark ONLY major untouched Blue Boxes (1D/4H/Multi-Touch Shelf): "
    "RESISTANCE must be strictly ABOVE LTP, SUPPORT must be strictly BELOW LTP (never mark broken levels).\n"
    "A level is untouched only if no later supplied candle traded through it. Do not repeat levels already in the "
    "Python Pre-Detected list (within $95); add only genuinely new ones.\n"
    "if_then_plan must name concrete levels: IF price sweeps <level> and reclaims THEN <direction> toward <next level>.\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"regime": "TRENDING_BULL|TRENDING_BEAR|SIDEWAYS", '
    '"if_then_plan": "1 concise sentence IF-THEN liquidity plan", '
    '"zones": [{"type": "RESISTANCE|SUPPORT", "tf": "1D|4H|1H", "pivot": 0.0, "evidence": "candle time", "why": "short reason"}]}'
    + AI_PIVOT_RULE
)


def _execute_6h_macro_worker(shared_store, is_daily_2330: bool):
    state = BtcState(shared_store)
    now = datetime.now(IST)

    df_1d = fetch_delta_candles("1d", hours_back=24 * 90, closed_only=True) if is_daily_2330 else None
    df_4h = fetch_delta_candles("4h", hours_back=24 * (90 if is_daily_2330 else 15), closed_only=True)
    df_1h = fetch_delta_candles("1h", hours_back=24 * (30 if is_daily_2330 else 7), closed_only=True)
    df_15m = fetch_delta_candles("15m", hours_back=24 * (7 if is_daily_2330 else 3), closed_only=True)

    if df_15m.empty or df_1h.empty:
        return

    ltp = float(df_15m["c"].iloc[-1])
    pa_regime, _ = get_live_pa_regime(df_4h, df_1h, df_15m)
    py_zones = extract_pure_blue_boxes(df_1d, df_4h, df_1h, df_15m, ltp, now, state)
    state.set("active_zones", py_zones, ttl=86400)
    state.set("ai_regime", pa_regime, ttl=21600)

    title = "23:30 IST DAILY BIG MULTI-TF ANALYSIS" if is_daily_2330 else f"6-HOUR INTRADAY LIQUIDITY SYNC ({now.strftime('%H:%M IST')})"
    near_txt = "\n".join([f"• <b>{z['type']}</b> ({z['tf']}): <code>${z['pivot']:,.1f}</code>" for z in py_zones[:4]]) or "• None within $5,000"

    tg_send(
        f"🏛️ <b>[BITCOIN - BTCUSD] {title}</b>\n"
        f"📊 <b>LTP:</b> <b>${ltp:,.1f}</b> | <b>Live PA Regime:</b> <code>{pa_regime}</code> | <b>Valid Zones:</b> <code>{len(py_zones)}</code>\n"
        f"{near_txt}\n⏳ <i>Running {esc(HYDROGEN_MODEL)} background zone sync...</i>"
    )

    usr_p = (
        f"Asset: BITCOIN (BTCUSD) | Mode: {title} | Time: {now.strftime('%Y-%m-%d %H:%M IST')} | Current LTP: ${ltp:,.1f}\n"
        f"Python Pre-Detected Valid Zones: {json.dumps([{'type': z['type'], 'tf': z['tf'], 'pivot': z['pivot'], 'age_d': z['age_days']} for z in py_zones[:10]])}\n"
        + (f"1D Real OHLC (25): {format_ohlc_candles(df_1d, 25, '%Y-%m-%d')}\n" if df_1d is not None and not df_1d.empty else "")
        + f"4H Real OHLC: {format_ohlc_candles(df_4h, 30 if is_daily_2330 else 16)}\n"
        + f"1H Real OHLC: {format_ohlc_candles(df_1h, 36 if is_daily_2330 else 28)}\n"
        + f"15M Real OHLC (Last 6H=24 bars): {format_ohlc_candles(df_15m, 28)}"
    )

    ai_res, provider = call_gemini_chain(HEAVY_AI_CHAIN, MACRO_6H_SYS_PROMPT, usr_p)
    if not ai_res:
        log.warning(f"[BTC] 6H/Macro AI unreachable ({provider}); Python zones remain active.")
        return

    ai_regime = ai_res.get("regime", pa_regime)
    state.set("ai_regime", ai_regime, ttl=21600)
    ai_saved = state.get("ai_zones") or []
    new_cnt = 0
    snap_frames = [df_1d, df_4h, df_1h]
    for z in (ai_res.get("zones") or [])[:10]:
        try:
            ztype = str(z.get("type", "RESISTANCE")).upper()
            piv = _snap_pivot_to_real_extreme(round(float(z["pivot"]), 1), ztype, snap_frames)
            if piv is None:
                continue
            if ztype not in ("RESISTANCE", "SUPPORT") or abs(piv - ltp) > DEEP_BAND_USD:
                continue
            if (ztype == "RESISTANCE" and piv <= ltp) or (ztype == "SUPPORT" and piv >= ltp):
                continue
            tf_lbl = str(z.get("tf", "4H_BLUE_BOX")).upper()
            if tf_lbl in ("1D", "4H", "1H"):
                tf_lbl = f"{tf_lbl}_BLUE_BOX"
            zid = f"AI_6H_{ztype[:3]}_{int(piv)}_{now.strftime('%Y%m%d')}"
            ai_saved.append({
                "id": zid, "type": ztype, "tf": tf_lbl, "prio": 2,
                "pivot": piv, "age_days": 0, "touches": 1,
                "source": "AI_6H_SYNC", "why": str(z.get("why", "AI Marked Major Blue Box"))
            })
            new_cnt += 1
        except Exception:
            continue

    if new_cnt:
        state.set("ai_zones", ai_saved[-30:], ttl=86400)
        refreshed = extract_pure_blue_boxes(df_1d, df_4h, df_1h, df_15m, ltp, now, state)
        state.set("active_zones", refreshed, ttl=86400)

    tg_send(
        f"🧠 <b>[BTC AI ZONE SYNC | {esc(provider)}]</b>\n"
        f"🧭 <b>AI Regime:</b> <b>{esc(ai_regime)}</b> | <b>Saved Valid Zones:</b> <code>{new_cnt}</code>\n"
        f"📋 <b>Plan:</b> <i>{esc(ai_res.get('if_then_plan', 'Tracking 15M Trend Pullback & Mega Blue Box hunts.'))}</i>"
    )


def trigger_6h_macro_if_due(shared_store=None, force: bool = False):
    state = BtcState(shared_store)
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
    threading.Thread(target=_execute_6h_macro_worker, args=(shared_store, is_daily_2330), daemon=True).start()


# ==============================================================================
# V10.6 5-GATE PURE LIQUIDITY DETECTOR (15M CLEAN EXECUTION — ZERO 5M CHOP)
# ==============================================================================
def _detect_climax_sweep(m15: pd.DataFrame, sc15: float):
    """EXPERIMENTAL: capitulation wick that sweeps the last-8h extreme and closes back inside.
    Runs BEFORE the normal range gate (which would reject exactly these big candles)."""
    k = len(m15) - 1
    if k < CLIMAX_LOOKBACK + 1:
        return None
    b = m15.iloc[k]
    op, hi, lo, cl = float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])
    rng = hi - lo
    if rng < CLIMAX_MIN_RNG_MULT * sc15 or rng > CLIMAX_MAX_RNG_MULT * sc15:
        return None
    prior = m15.iloc[k - CLIMAX_LOOKBACK:k]
    p_low, p_high = float(prior["l"].min()), float(prior["h"].max())
    dn_w, up_w = (min(op, cl) - lo) / rng, (hi - max(op, cl)) / rng
    if lo < p_low - MIN_PIERCE_USD and cl > p_low and dn_w >= 0.5 and (cl - lo) / rng >= 0.75:
        return {"name": "CLIMAX_SWEEP_RECLAIM", "action": "BUY_LONG", "zone_id": "", "blue_box": round(p_low, 1),
                "wick_extreme": round(lo, 1), "entry": cl,
                "reason": f"Climax wick swept 8h low ${p_low:,.1f} by ${p_low - lo:,.1f} & closed back inside ({dn_w * 100:.0f}% wick)"}
    if hi > p_high + MIN_PIERCE_USD and cl < p_high and up_w >= 0.5 and (hi - cl) / rng >= 0.75:
        return {"name": "CLIMAX_SWEEP_RECLAIM", "action": "SELL_SHORT", "zone_id": "", "blue_box": round(p_high, 1),
                "wick_extreme": round(hi, 1), "entry": cl,
                "reason": f"Climax wick swept 8h high ${p_high:,.1f} by ${hi - p_high:,.1f} & closed back inside ({up_w * 100:.0f}% wick)"}
    return None


def detect_btc_pure_liquidity_setup(m15: pd.DataFrame, zones: list, regime: str, disp_15m: str, state: BtcState, reg_1h=None):
    """
    Evaluates completed 15M candle against strict quality gates.
    Setups: 1D/4H Mega Blue Boxes & Multi-Touch Shelves (>=35% wick), 15M Trend Pullback Liquidity Hunt
    (skipped when 1H regime opposes the 4H-led regime), CLIMAX_SWEEP_RECLAIM (experimental).
    """
    if m15 is None or len(m15) < 16:
        return None

    k = len(m15) - 1
    c_bar = m15.iloc[k]
    op, hi, lo, cl = float(c_bar["o"]), float(c_bar["h"]), float(c_bar["l"]), float(c_bar["c"])
    sc15 = _median_range(m15, 20)
    rng = hi - lo

    cx = _detect_climax_sweep(m15, sc15)
    if cx:
        return cx

    if rng < MIN_RNG_15M_USD or rng > MAX_RNG_MULT * sc15:
        return None

    up_w = (hi - max(op, cl)) / rng
    dn_w = (min(op, cl) - lo) / rng
    body_r = abs(cl - op) / rng
    max_ov = min(MAX_OVERSHOOT_USD, 2.2 * sc15)
    prev4_c = m15["c"].iloc[k - 4 : k].values

    # 1. Mega Blue Boxes (1D / 4H / AI Major) & Multi-Touch Shelves (skip weak single-touch 1H)
    for b in zones:
        zid = b["id"]
        if state.is_spent(zid):
            continue
        if b.get("tf") == "1H_BLUE_BOX" and b.get("touches", 1) < 2 and not str(b.get("source", "")).startswith("AI_"):
            continue
        lvl = float(b["pivot"])
        if abs(lvl - cl) > SCAN_RADIUS_USD:
            continue
        action = "SELL_SHORT" if b["type"] == "RESISTANCE" else "BUY_LONG"

        if (action == "SELL_SHORT" and disp_15m == "TRENDING_BULL") or (action == "BUY_LONG" and disp_15m == "TRENDING_BEAR"):
            continue
        if action == "BUY_LONG" and int(np.sum(prev4_c > lvl)) < 2:
            continue
        if action == "SELL_SHORT" and int(np.sum(prev4_c < lvl)) < 2:
            continue

        is_mega = b.get("tf") in ("1D_BLUE_BOX", "4H_BLUE_BOX", "HIST_1D", "HIST_4H")
        if not is_mega and ((action == "SELL_SHORT" and regime == "TRENDING_BULL") or (action == "BUY_LONG" and regime == "TRENDING_BEAR")):
            continue

        sname = "MULTI_TOUCH_SHELF" if b.get("touches", 1) >= 2 else (f"OLD_{b['tf']}" if b.get("age_days", 0) >= 5 else b["tf"])
        if action == "BUY_LONG":
            pierce = lvl - lo
            if MIN_PIERCE_USD <= pierce <= max_ov and cl > lvl and up_w <= 0.35 and dn_w >= 0.35:
                return {
                    "name": sname, "action": "BUY_LONG", "zone_id": zid, "blue_box": lvl,
                    "wick_extreme": round(lo, 1), "entry": cl,
                    "reason": f"15M Yellow Box Sweep (${lo:,.1f} < ${lvl:,.1f}, -${pierce:,.1f}) & Strong Wick Reclaim ({dn_w*100:.0f}%) | {b.get('why', '')}"
                }
        else:
            pierce = hi - lvl
            if MIN_PIERCE_USD <= pierce <= max_ov and cl < lvl and dn_w <= 0.35 and up_w >= 0.35:
                return {
                    "name": sname, "action": "SELL_SHORT", "zone_id": zid, "blue_box": lvl,
                    "wick_extreme": round(hi, 1), "entry": cl,
                    "reason": f"15M Yellow Box Sweep (${hi:,.1f} > ${lvl:,.1f}, +${pierce:,.1f}) & Strong Wick Reject ({up_w*100:.0f}%) | {b.get('why', '')}"
                }

    # 2. 15M Trend Pullback Liquidity Hunt (BTC's #1 Proven Edge)
    hh15, ll15 = m15["h"].values, m15["l"].values
    if reg_1h is not None and ((regime == "TRENDING_BULL" and reg_1h == "TRENDING_BEAR") or (regime == "TRENDING_BEAR" and reg_1h == "TRENDING_BULL")):
        return None   # pullback sweep against the 1H trend is not a real pullback
    if regime == "TRENDING_BULL":
        pb_idx = k - 6 + int(np.argmin(ll15[k - 6 : k - 1]))
        pb_low = float(ll15[pb_idx])
        imp_h = float(np.max(hh15[max(0, pb_idx - 8) : pb_idx]))
        pierce = pb_low - lo
        if (imp_h - pb_low) >= 1.2 * sc15 and pb_low > float(np.min(ll15[max(0, pb_idx - 12) : pb_idx])):
            if MIN_PIERCE_USD <= pierce <= max_ov and cl > pb_low and cl > op and up_w <= 0.35 and (dn_w >= 0.25 or body_r >= 0.40):
                return {
                    "name": "TREND_PULLBACK_LIQ_HUNT", "action": "BUY_LONG", "zone_id": "",
                    "blue_box": round(pb_low, 1), "wick_extreme": round(lo, 1), "entry": cl,
                    "reason": f"15M Bullish Pullback Sweep below ${pb_low:,.1f} (Wick ${lo:,.1f}, -${pierce:,.1f}) & Green Reclaim (${cl:,.1f})"
                }
    elif regime == "TRENDING_BEAR":
        pb_idx = k - 6 + int(np.argmax(hh15[k - 6 : k - 1]))
        pb_high = float(hh15[pb_idx])
        imp_l = float(np.min(ll15[max(0, pb_idx - 8) : pb_idx]))
        pierce = hi - pb_high
        if (pb_high - imp_l) >= 1.2 * sc15 and pb_high < float(np.max(hh15[max(0, pb_idx - 12) : pb_idx])):
            if MIN_PIERCE_USD <= pierce <= max_ov and cl < pb_high and cl < op and dn_w <= 0.35 and (up_w >= 0.25 or body_r >= 0.40):
                return {
                    "name": "TREND_PULLBACK_LIQ_HUNT", "action": "SELL_SHORT", "zone_id": "",
                    "blue_box": round(pb_high, 1), "wick_extreme": round(hi, 1), "entry": cl,
                    "reason": f"15M Bearish Pullback Sweep above ${pb_high:,.1f} (Wick ${hi:,.1f}, +${pierce:,.1f}) & Red Reclaim (${cl:,.1f})"
                }

    return None


# ==============================================================================
# V10.6 EARLY HUNT HEADS-UP (CLOSED 5M CANDLE INSIDE THE STILL-FORMING 15M CANDLE)
# ==============================================================================
def detect_early_hunt(m5: pd.DataFrame, df_15m: pd.DataFrame, zones: list, regime: str, disp_15m: str, state: BtcState):
    """Heads-up only (NOT an entry). A closed 5M candle that swept a valid zone and closed back inside,
    while the 15M candle is still forming. Never marks a zone spent; own per-zone cooldown."""
    if m5 is None or len(m5) < 3 or df_15m is None or len(df_15m) < 6 or not zones:
        return None
    forming_start = int(df_15m["time"].iloc[-1]) + 900
    b = m5.iloc[-1]
    if int(b["time"]) < forming_start:
        return None   # belongs to an already-closed 15M candle -> the 15M cycle handles it
    op, hi, lo, cl = float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])
    rng = hi - lo
    if rng < MIN_RNG_5M_USD:
        return None
    up_w, dn_w = (hi - max(op, cl)) / rng, (min(op, cl) - lo) / rng
    sc15 = _median_range(df_15m, 20)
    max_ov = min(MAX_OVERSHOOT_USD, 2.2 * sc15)
    prev4_c = df_15m["c"].iloc[-4:].values

    for z in zones:
        zid = z["id"]
        if state.is_spent(zid) or state.get(f"early_{zid}"):
            continue
        if z.get("tf") == "1H_BLUE_BOX" and z.get("touches", 1) < 2 and not str(z.get("source", "")).startswith("AI_"):
            continue
        lvl = float(z["pivot"])
        if abs(lvl - cl) > SCAN_RADIUS_USD:
            continue
        action = "SELL_SHORT" if z["type"] == "RESISTANCE" else "BUY_LONG"
        if (action == "SELL_SHORT" and disp_15m == "TRENDING_BULL") or (action == "BUY_LONG" and disp_15m == "TRENDING_BEAR"):
            continue
        if action == "BUY_LONG" and int(np.sum(prev4_c > lvl)) < 2:
            continue
        if action == "SELL_SHORT" and int(np.sum(prev4_c < lvl)) < 2:
            continue
        is_mega = z.get("tf") in ("1D_BLUE_BOX", "4H_BLUE_BOX", "HIST_1D", "HIST_4H")
        if not is_mega and ((action == "SELL_SHORT" and regime == "TRENDING_BULL") or (action == "BUY_LONG" and regime == "TRENDING_BEAR")):
            continue
        if action == "BUY_LONG":
            pierce = lvl - lo
            if MIN_PIERCE_USD <= pierce <= max_ov and cl > lvl and dn_w >= 0.35:
                return {"zone_id": zid, "action": action, "level": lvl, "wick_extreme": round(lo, 1), "close": cl,
                        "pierce": pierce, "tf": z.get("tf", ""), "bar_time": int(b["time"]), "confirm_at": forming_start + 900}
        else:
            pierce = hi - lvl
            if MIN_PIERCE_USD <= pierce <= max_ov and cl < lvl and up_w >= 0.35:
                return {"zone_id": zid, "action": action, "level": lvl, "wick_extreme": round(hi, 1), "close": cl,
                        "pierce": pierce, "tf": z.get("tf", ""), "bar_time": int(b["time"]), "confirm_at": forming_start + 900}
    return None


def _early_hunt_check(state: BtcState, df_15m: pd.DataFrame):
    """Runs on every 5M tick using CACHED zones (no extra heavy fetches). Never raises."""
    try:
        cached = state.get("active_zones") or []
        if not cached:
            return
        ltp = float(df_15m["c"].iloc[-1])
        zones = [z for z in cached if not state.is_spent(z["id"]) and not _is_zone_broken_or_zombie(z, df_15m, ltp)]
        if not zones:
            return
        m5 = fetch_delta_candles("5m", hours_back=2, closed_only=True)
        regime = state.get("regime") or "SIDEWAYS"
        disp_15m = get_live_pa_regime(None, None, df_15m)[1]
        hunt = detect_early_hunt(m5, df_15m, zones, regime, disp_15m, state)
        if not hunt:
            return
        state.set(f"early_{hunt['zone_id']}", True, ttl=EARLY_HUNT_COOLDOWN)
        em = "🟢" if hunt["action"] == "BUY_LONG" else "🔴"
        side = "BULLISH (long bias)" if hunt["action"] == "BUY_LONG" else "BEARISH (short bias)"
        t_now = _ensure_ist(pd.Timestamp(hunt["bar_time"] + 300, unit="s", tz="UTC")).strftime("%H:%M IST")
        t_cf = _ensure_ist(pd.Timestamp(hunt["confirm_at"], unit="s", tz="UTC")).strftime("%H:%M IST")
        tg_send(
            f"👀{em} <b>[BITCOIN] LIQUIDITY HUNT DETECTED (5M) | {t_now}</b>\n\n"
            f"🟦 <b>Zone:</b> {esc(hunt['tf'])} @ <b>${hunt['level']:,.1f}</b> swept by <b>${hunt['pierce']:,.1f}</b>\n"
            f"🕯️ <b>5M close:</b> <b>${hunt['close']:,.1f}</b> (back inside) | <b>Wick ext:</b> ${hunt['wick_extreme']:,.1f}\n"
            f"🧭 <b>Bias:</b> {side}\n\n"
            f"⚠️ <i>HEADS-UP, entry nahi. 15M candle ajun band zhali nahi — {t_cf} la confirm alert yeil (zhalyas).</i>"
        )
        log.info(f"[BTC] 👀 EARLY HUNT: {hunt['action']} zone {hunt['zone_id']} lvl ${hunt['level']:,.1f}")
    except Exception as e:
        log.warning(f"[BTC] Early hunt check error: {type(e).__name__}")


# ==============================================================================
# PARALLEL NON-BLOCKING BACKGROUND AI CONFIRMATION (REAL OHLC — ZERO HALLUCINATION)
# ==============================================================================
LIVE_CONFIRM_SYS_PROMPT = (
    "You are a strict Bitcoin (BTCUSD) price-action auditor. Zero indicators, no orderflow/volume delta. "
    "Candle prices are real chart OHLC in USD.\n"
    "The user message has a PYTHON FACTS block: treat its numbers as authoritative; never recompute or invent levels.\n"
    "Judge each check true/false using ONLY the facts and candles supplied:\n"
    "1. pool: the sweep took out a visible cluster/extreme of prior highs or lows in the supplied candles.\n"
    "2. reclaim: the signal candle closed back inside the level with real rejection (no acceptance beyond it).\n"
    "3. no_displacement: the last 3 candles do not show strong momentum AGAINST the trade.\n"
    "4. trend_ok: the trade is not against BOTH the 1H and 4H regime.\n"
    "5. room: the nearest opposing zone is at least 1.5R away (or none).\n"
    "Verdict: APPROVED = all 5 true. WAIT_CAUTION = exactly 1 false. REJECT_TRAP = 2+ false, or check 2 false. "
    "When unsure choose the more cautious verdict. No praise, no filler.\n"
    "Respond ONLY with valid JSON: "
    '{"checks": {"pool": true, "reclaim": true, "no_displacement": true, "trend_ok": true, "room": true}, '
    '"verdict": "APPROVED|WAIT_CAUTION|REJECT_TRAP", "confidence": "HIGH|MEDIUM|LOW", '
    '"note": "max 25 words, cite the candle time of the key evidence"}'
)


def _background_ai_confirm_worker(setup: dict, usr_p: str, signal_time: str):
    ai_res, provider = call_gemini_chain(LIVE_CONFIRM_CHAIN, LIVE_CONFIRM_SYS_PROMPT, usr_p)
    if not ai_res:
        log.warning(f"[BTC] Background AI confirm unreachable ({provider})")
        return
    verdict = verdict_from_checks(ai_res)
    conf = ai_res.get("confidence", "MEDIUM")
    note = ai_res.get("note", "")
    checks = ai_res.get("checks") if isinstance(ai_res.get("checks"), dict) else {}
    failed = [k for k, v in checks.items() if v is False]
    badge = "✅" if verdict == "APPROVED" else ("⚠️" if verdict == "WAIT_CAUTION" else "🛑")
    log.info(f"AI_VERDICT|{setup['name']}|{setup['action']}|{setup['entry']}|{verdict}|{conf}|failed={','.join(failed) or '-'}")
    fail_line = f"\n❌ <b>Failed checks:</b> <code>{esc(', '.join(failed))}</code>" if failed else ""
    tg_send(
        f"{badge} <b>[BTC AI CONFIRMATION | {signal_time}]</b>\n"
        f"⚡ <b>Setup:</b> <code>{esc(setup['name'])}</code> ({esc(setup['action'])}) @ <b>${setup['entry']:,.1f}</b>\n"
        f"🧠 <b>Model:</b> <code>{esc(provider)}</code> | <b>Verdict:</b> <b>[{esc(verdict)} | {esc(conf)}]</b>"
        f"{fail_line}\n"
        f"<i>{esc(note)}</i>"
    )


def run_startup_diagnostics(shared_store=None) -> dict:
    log.info("[BTC] Running Startup Diagnostics...")
    t0 = time.time()
    df_t = fetch_delta_candles("15m", hours_back=6, closed_only=False)
    api_ms = int((time.time() - t0) * 1000)
    ok = not df_t.empty
    ltp = float(df_t["c"].iloc[-1]) if ok else 0.0
    if ok:
        trigger_6h_macro_if_due(shared_store, force=True)
    return {"asset": "BTCUSD", "api_ok": ok, "api_ms": api_ms, "ltp": ltp}


# ==============================================================================
# MAIN CYCLE (EVALUATES EVERY CLOSED 15M CANDLE + 5M EARLY-HUNT HEADS-UP)
# ==============================================================================
def run_btc_cycle(shared_store=None):
    if not _btc_lock.acquire(blocking=False):
        log.warning("[BTC] Previous cycle still running — tick skipped")
        return
    try:
        state = BtcState(shared_store)

        # 1. Check 23:30 BIG Analysis & Every 6-Hour Sync (05:30, 11:30, 17:30 IST)
        trigger_6h_macro_if_due(shared_store, force=False)

        # 2. Fetch closed 15M candles
        df_15m = fetch_delta_candles("15m", hours_back=24 * 3, closed_only=True)
        if df_15m.empty or len(df_15m) < 25:
            log.warning("[BTC] 15M feed empty/short — cycle skipped")
            return

        # 2b. 5M early-hunt heads-up (uses cached zones; runs every tick, before the 15M de-dup return)
        if EARLY_HUNT_ENABLED:
            _early_hunt_check(state, df_15m)

        last_15m_t = int(df_15m["time"].iloc[-1])
        if state.get("last_eval_15m") == last_15m_t:
            return
        state.set("last_eval_15m", last_15m_t, ttl=1200)

        ltp = float(df_15m["c"].iloc[-1])
        close_ts = _ensure_ist(df_15m["ts"].iloc[-1]) + timedelta(minutes=15)

        df_1h = fetch_delta_candles("1h", hours_back=24 * 7, closed_only=True)
        df_4h = fetch_delta_candles("4h", hours_back=24 * 15, closed_only=True)

        # Live Dynamic PA Regime (never stale)
        live_regime, disp_15m = get_live_pa_regime(df_4h, df_1h, df_15m)
        reg_1h = get_live_pa_regime(None, df_1h, df_15m)[0]
        ai_reg = state.get("ai_regime")
        regime = live_regime if (live_regime != "SIDEWAYS" or not ai_reg) else ai_reg
        state.set("regime", regime, ttl=7200)

        # Refresh valid zones & purge broken zombie zones
        cached_htf = state.get("active_zones") or []
        live_zones = extract_pure_blue_boxes(None, df_4h, df_1h, df_15m, ltp, close_ts, state)
        zones = list(live_zones)
        for cz in cached_htf:
            if not state.is_spent(cz["id"]) and not _is_zone_broken_or_zombie(cz, df_15m, ltp):
                if not any(x["type"] == cz["type"] and abs(float(x["pivot"]) - float(cz["pivot"])) <= SHELF_MERGE_USD for x in zones):
                    zones.append(cz)
        state.set("active_zones", zones[:20], ttl=21600)

        # 3. Zone Vacuum Check (< 2 zones in LTP ± $2,500) -> 3-Yr Band Slice
        check_zone_vacuum_and_trigger_band(state, zones, ltp, df_15m)

        # 4. Evaluate Pure Liquidity Setups
        setup = detect_btc_pure_liquidity_setup(df_15m, zones, regime, disp_15m, state, reg_1h=reg_1h)
        if not setup:
            log.info(f"[BTC] {close_ts.strftime('%H:%M IST')} | LTP:${ltp:,.1f} | Regime:{regime} (1H:{reg_1h}) | ValidZones:{len(zones)} -> WAITING")
            return

        # Gate 5: Check cooldowns & opposite lock BEFORE marking zone spent!
        last_dir = state.get("last_sig_dir")
        last_dir_t = state.get("last_sig_ts") or 0
        if last_dir and last_dir != setup["action"] and (time.time() - last_dir_t) < OPPOSITE_LOCK_MIN * 60:
            log.info(f"[BTC] Opposite lock active ({last_dir}); skipped {setup['action']} without spending zone.")
            return

        cd_key = f"cd_{setup['name']}_{setup['action']}"
        if state.get(cd_key):
            return

        if setup.get("zone_id"):
            state.mark_spent(setup["zone_id"])
        state.set("last_sig_dir", setup["action"], ttl=OPPOSITE_LOCK_MIN * 60)
        state.set("last_sig_ts", time.time(), ttl=OPPOSITE_LOCK_MIN * 60)
        state.set(cd_key, True, ttl=SETUP_COOLDOWN_MIN * 60)

        # 5. PYTHON-FIRST INSTANT TELEGRAM ALERT (fires ~15s after the 15M candle that did the hunt closes)
        em = "🟢" if setup["action"] == "BUY_LONG" else "🔴"
        title_act = "BUY (LONG)" if setup["action"] == "BUY_LONG" else "SELL (SHORT)"
        sig_time_str = close_ts.strftime("%H:%M IST")
        long_ = setup["action"] == "BUY_LONG"
        risk = abs(setup["entry"] - setup["wick_extreme"])
        tp2 = setup["entry"] + 2 * risk if long_ else setup["entry"] - 2 * risk
        tp3 = setup["entry"] + 3 * risk if long_ else setup["entry"] - 3 * risk
        warn = ""
        if setup["name"] == "CLIMAX_SWEEP_RECLAIM":
            warn = f"⚠️ <b>EXPERIMENTAL</b> | Risk to wick: <b>${risk:,.0f}</b> (bada risk, size kami thev)\n"
        msg = (
            f"{em} <b>[BITCOIN - BTCUSD] {title_act} | {sig_time_str} (15M)</b>\n\n"
            f"⚡ <b>Setup        :</b> <code>{esc(setup['name'])}</code>\n"
            f"🎯 <b>Entry Price  :</b> <b>${setup['entry']:,.1f}</b>\n"
            f"🟦 <b>Blue Box Lvl :</b> <b>${setup['blue_box']:,.1f}</b> | <b>Yellow Wick Ext:</b> <b>${setup['wick_extreme']:,.1f}</b>\n"
            f"🛑 <b>SL ref (wick):</b> <code>${setup['wick_extreme']:,.1f}</code> | Risk <code>${risk:,.0f}</code>\n"
            f"🏁 <b>Ref targets  :</b> 1:2 <code>${tp2:,.1f}</code> | 1:3 <code>${tp3:,.1f}</code>\n"
            f"🧭 <b>Live Regime  :</b> <code>{esc(regime)}</code> (1H: <code>{esc(reg_1h)}</code>)\n"
            f"📝 <b>Sweep Logic  :</b> {esc(setup['reason'])}\n"
            f"{warn}\n"
            f"⏳ <i>Chart baghun manual confirm kar (Manual SL &amp; Target mode). Running parallel AI audit...</i>"
        )
        tg_send(msg)
        log.info(f"[BTC] 🔥 QUALITY ALERT: {setup['name']} ({setup['action']}) @ ${setup['entry']:,.1f}")

        # 6. Parallel Background AI Audit with REAL OHLC + PYTHON FACTS (No "Delta" word)
        facts = build_confirm_facts(setup, df_15m, zones, regime, reg_1h)
        df_5m = fetch_delta_candles("5m", hours_back=8, closed_only=True)
        usr_p = (
            f"Asset: BITCOIN (BTCUSD) | Time: {sig_time_str}\n{facts}\n"
            f"Setup: {setup['name']} | Blue Box: ${setup['blue_box']:,.1f}\n"
            f"Python reason: {setup['reason']}\n"
            f"Recent 20 15M Real OHLC Candles:\n" + "\n".join(format_ohlc_candles(df_15m, 20)) + "\n"
            f"Recent 24 5M Real OHLC Candles:\n" + "\n".join(format_ohlc_candles(df_5m, 24))
        )
        threading.Thread(target=_background_ai_confirm_worker, args=(setup, usr_p, sig_time_str), daemon=True).start()

    except Exception as e:
        log.exception(f"[BTC] Cycle error: {type(e).__name__}")
        try:
            BtcState(shared_store).set("last_eval_15m", 0, ttl=60)   # let the next tick retry this candle
        except Exception:
            pass
    finally:
        try:
            _btc_lock.release()
        except RuntimeError:
            pass


# ==============================================================================
# main.py ENTRY POINTS (main.py V8.5 looks these names up with getattr)
# ==============================================================================
def run_btc_1m_tick(shared_store=None):
    """5-minute scheduler entry: 15M cycle + 5M early-hunt heads-up."""
    run_btc_cycle(shared_store)


def run_6h_analysis(shared_store=None):
    """6-hour scheduler entry: de-duplicated against the in-cycle trigger via the slot key."""
    trigger_6h_macro_if_due(shared_store, force=False)
