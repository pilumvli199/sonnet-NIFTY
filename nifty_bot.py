"""
================================================================================
NIFTY 50 PURE LIQUIDITY ENGINE — PRODUCTION V10.6 (EARLY WATCH EDITION)
Namespace: NIFTY:* | Upstox V3 API | 100% Price Action (Zero EMA / RSI / ATR)
================================================================================
UPGRADES IN V10.6 (backtested on 15 real trading days, 02-20 Nov 2026 1-min NIFTY data):
  1. NEW: "Early Watch" layer — scans 1-min candles of the forming 5M bar for a
     sweep+2-bar-hold against the same Blue Box zones, and sends a separate, non-blocking
     "👀 heads up" Telegram alert 1-4 min before the real 5-Gate signal could fire.
     It NEVER spends a zone, starts a cooldown, or substitutes for the 5-Gate signal —
     backtesting a single-bar early trigger as a REPLACEMENT for the 5-Gate entry actually
     hurt 30-min win-rate (35% vs 42.9%) on this sample, so the quality-vetted 5-Gate path
     is kept fully intact and this ships as an optional, clearly-labeled early heads-up only.
     Needs run_nifty_cycle() invoked every ~1 min (not 5) during market hours — see its docstring.
  2. Model names unchanged, per Pravesh — stays on "gemini-4-hydrogen" / "gemini-3.5-flash-lite" /
     "gemini-3.1-flash-lite" (gemini-4-argon is Stable but not on his free tier).
  3. LIVE_CONFIRM_SYS_PROMPT now explicitly asks the AI audit to flag stale/3rd-touch
     reclaims and first-10-min / last-30-min session timing, both common sources of
     early-but-low-quality entries.
  4. FIX: Early Watch's 1-min scan used to give up on the whole forming bar the moment one
     sweep attempt failed to hold (e.g. a minute-1 sweep that didn't hold would block a real
     minute-3 sweep+hold from ever firing) — it now keeps scanning every remaining minute.
================================================================================
PRIOR V10.5 UPGRADES:
  1. Removes noisy CURRENT_DAY_5M micro-ripples & negative-edge DUAL_SIDE_HUNT_PT7.
  2. Adds genuine BREAKOUT_FLIP_HUNT on broken 1D/1H Blue Boxes (with-trend only).
  3. Live Dynamic PA Regime (1H structure + 15M rapid displacement guard).
  4. Zombie / Broken Zone Purge (Resistance strictly above LTP, Support strictly below).
  5. Strict 5-Gate Filter: Clean Approach (>=2/4 prior closes inside), Strict Piercing
     (>=1.5 pts & <=2.2x scale), Dead-Session (>=10 pts) & Anti-Spike (<=3.5x scale).
  6. Real OHLC Sent to AI (ZERO "Delta" word hallucination in AI verdicts).
  7. Weekend-Safe Startup Check (days_back=5 fixes Monday false OFFLINE alert).
  8. Zone marked spent ONLY AFTER cooldown/lock checks approve the alert.
================================================================================
"""

import os
import json
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

# Model check (24 Nov 2026 live availability sweep): "gemini-4-hydrogen" itself didn't turn up
# on the public model list — "gemini-4-argon" is the one confirmed Stable there — but Pravesh
# confirmed gemini-4-hydrogen is what's actually wired to his key/tier, so it stays as-is.
# "gemini-3.5-flash-lite" and "gemini-3.1-flash-lite" are both confirmed Stable, unchanged.
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

# Pure Liquidity Constants (NIFTY Index Points — V10.5)
CANDLE_BUFFER_SEC   = 2
SHELF_MERGE_PTS     = 12.0
FLIP_BREAK_PTS      = 15.0
MIN_PIERCE_PTS      = 1.5
MAX_OVERSHOOT_PTS   = 32.0
MIN_RNG_5M_PTS      = 10.0
MAX_RNG_MULT        = 3.5
MAX_ZONE_AGE_DAYS   = 85
VACUUM_RADIUS_PTS   = 300.0
DEEP_BAND_PTS       = 600.0
SETUP_COOLDOWN_MIN  = int(os.getenv("NIFTY_SETUP_COOLDOWN_MIN", "45"))
OPPOSITE_LOCK_MIN   = int(os.getenv("NIFTY_OPPOSITE_LOCK_MIN", "30"))

# --- EARLY WATCH (V10.6): 1-minute pre-alert layer on top of the unchanged 5-Gate system ---
# Backtested on 15 trading days (02-20 Nov 2026) of real 1-min NIFTY data: a single-1-minute-bar
# reclaim trigger pulled signals 2-4 min earlier but *lowered* 30-min win-rate (35% vs 42.9% for
# the stock 5-gate system) on this small sample -- too noisy to replace the confirmed signal.
# A 2-bar hold confirmation (sweep bar + 1 more 1-min close holding past the pivot) was safer but
# STILL not reliably better than the 5-gate close on this sample size (15 days / ~15 signals is too
# small to prove a timing edge). So Early Watch ships as a SEPARATE, non-blocking "heads up" alert:
# it never spends a zone, never starts a cooldown, and never substitutes for the 5-Gate signal --
# it only gives Pravesh a few minutes' notice to pull up the chart before the real alert can fire.
# Requires run_nifty_cycle() to be invoked every ~1 min during market hours (the 5-Gate path is
# already safe to call that often -- it only acts once a NEW closed 5M candle actually appears).
EARLY_WATCH_ENABLED     = os.getenv("NIFTY_EARLY_WATCH", "1") != "0"
EARLY_WATCH_MIN_RNG_1M  = 3.0   # min 1-min bar range (pts) to even look at a reclaim
EARLY_WATCH_WINDOW_MIN  = 4     # only watch minutes 1-4 of the forming 5M bar (never its closing minute)
EARLY_WATCH_COOLDOWN_MIN = SETUP_COOLDOWN_MIN

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
        log.info(f"[NIFTY-TG-DRY]\n{text}\n")
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
        log.error(f"[NIFTY] Telegram send error: {type(e).__name__}")
        return False


# ==============================================================================
# GEMINI AI ENGINE (SMART 429/503 BACKOFF + NO thinkingBudget:0 BUG)
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

    attempts = max(2, len(GEMINI_API_KEYS))
    srv_503_hits = 0
    for attempt in range(attempts):
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
                log.warning(f"[NIFTY] {model_name} HTTP 429 — rotating key after {backoff:.1f}s...")
                time.sleep(backoff)
                continue
            if r.status_code in (500, 502, 503, 504):
                srv_503_hits += 1
                if srv_503_hits <= 1:
                    log.warning(f"[NIFTY] {model_name} HTTP {r.status_code} — quick 1.5s retry...")
                    time.sleep(1.5)
                    continue
                log.warning(f"[NIFTY] {model_name} HTTP {r.status_code} — falling back to next model...")
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
            log.warning(f"[NIFTY] {model_name} timeout ({timeout_sec}s) — retrying once...")
            time.sleep(1.5)
            continue
        except Exception as e:
            log.warning(f"[NIFTY] {model_name} error ({type(e).__name__}) — retrying once...")
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
# UPSTOX V3 CANDLE FETCHER + REAL OHLC FORMATTER (ZERO "DELTA" WORD)
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
# LIVE DYNAMIC PA REGIME & ZOMBIE-FREE BLUE BOX ENGINE (ZERO INDICATORS)
# ==============================================================================
def _median_range(df: pd.DataFrame, n: int = 20) -> float:
    if df is None or df.empty:
        return 15.0
    v = float((df["h"] - df["l"]).tail(n).median())
    return v if v > 0 else 15.0


def get_live_pa_regime(h1_df: pd.DataFrame, m15_df: pd.DataFrame) -> tuple:
    reg = "SIDEWAYS"
    if h1_df is not None and len(h1_df) >= 10:
        sub = h1_df.tail(10)
        hh, ll, cc = sub["h"].values, sub["l"].values, sub["c"].values
        sc = float(np.median(hh - ll))
        ph, pl = np.max(hh[:5]), np.min(ll[:5])
        ch, cl = np.max(hh[5:]), np.min(ll[5:])
        net_mv = float(cc[-1] - cc[4])
        if (ch > ph and cl > pl) or net_mv > 1.7 * sc:
            reg = "TRENDING_BULL"
        elif (ch < ph and cl < pl) or net_mv < -1.7 * sc:
            reg = "TRENDING_BEAR"

    disp_15m = "NONE"
    if m15_df is not None and len(m15_df) >= 10:
        sc15 = _median_range(m15_df, 20)
        mv = float(m15_df["c"].iloc[-1] - m15_df["c"].iloc[-5])
        if mv > 2.8 * sc15:
            disp_15m = "TRENDING_BULL"
        elif mv < -2.8 * sc15:
            disp_15m = "TRENDING_BEAR"

    if reg == "SIDEWAYS" and disp_15m != "NONE":
        reg = disp_15m
    return reg, disp_15m


def _update_or_purge_zone(z: dict, df_5m: pd.DataFrame, ltp: float) -> str:
    """
    Returns:
      'KEEP'  -> valid untouched (or flipped) zone
      'FLIP'  -> major 1D/1H zone cleanly broken -> flip polarity for BREAKOUT_FLIP_HUNT
      'PURGE' -> broken zombie zone -> remove & mark spent
    """
    piv = float(z["pivot"])
    ztype = z["type"]
    c_prev1 = float(df_5m["c"].iloc[-2]) if (df_5m is not None and len(df_5m) >= 3) else ltp
    c_prev2 = float(df_5m["c"].iloc[-3]) if (df_5m is not None and len(df_5m) >= 4) else ltp

    is_broken = False
    if ztype == "RESISTANCE":
        if (c_prev1 > piv + FLIP_BREAK_PTS and c_prev2 > piv + FLIP_BREAK_PTS * 0.4) or ltp > piv + FLIP_BREAK_PTS * 2.5:
            is_broken = True
    elif ztype == "SUPPORT":
        if (c_prev1 < piv - FLIP_BREAK_PTS and c_prev2 < piv - FLIP_BREAK_PTS * 0.4) or ltp < piv - FLIP_BREAK_PTS * 2.5:
            is_broken = True

    if not is_broken:
        return "KEEP"
    if not z.get("flipped") and z.get("tf") in ("1D_BLUE_BOX", "1H_BLUE_BOX"):
        return "FLIP"
    return "PURGE"


def extract_pure_blue_boxes(df_1d: pd.DataFrame, df_1h: pd.DataFrame, df_15m: pd.DataFrame, df_5m: pd.DataFrame, ltp: float, now_ist: datetime, state: NiftyState) -> list:
    specs = [
        (df_1d,  "1D_BLUE_BOX",  3, 2, 1.3, 1),
        (df_1h,  "1H_BLUE_BOX",  6, 3, 1.8, 2),
        (df_15m, "15M_BLUE_BOX", 6, 3, 1.8, 3),
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
                            "flipped": False, "source": "PYTHON_HTF", "why": f"{tf_lbl} ({age_d}d old) @ {hh[k]:.2f}"
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
                            "flipped": False, "source": "PYTHON_HTF", "why": f"{tf_lbl} ({age_d}d old) @ {ll[k]:.2f}"
                        })

    for az in (state.get("ai_zones") or []):
        if not state.is_spent(az["id"]) and abs(float(az["pivot"]) - ltp) <= DEEP_BAND_PTS:
            raw_boxes.append(az)

    clean_boxes = []
    for z in raw_boxes:
        status = _update_or_purge_zone(z, df_5m, ltp)
        if status == "PURGE":
            state.mark_spent(z["id"])
        elif status == "FLIP":
            zf = dict(z)
            zf["type"] = "SUPPORT" if z["type"] == "RESISTANCE" else "RESISTANCE"
            zf["flipped"] = True
            clean_boxes.append(zf)
        else:
            clean_boxes.append(z)

    clean_boxes.sort(key=lambda x: (x.get("prio", 3), abs(float(x["pivot"]) - ltp)))
    merged = []
    for cand in clean_boxes:
        m = next((x for x in merged if x["type"] == cand["type"] and x.get("flipped") == cand.get("flipped") and abs(float(x["pivot"]) - float(cand["pivot"])) <= SHELF_MERGE_PTS), None)
        if m:
            if not str(m.get("source", "")).startswith("AI_") and not str(cand.get("source", "")).startswith("AI_"):
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
    "NIFTY entered a price zone with fewer than 2 nearby untouched zones.\n"
    "Analyze ONLY the supplied REAL OHLC historical candles inside LTP ± 600 pts + today's intraday candles.\n"
    "Mark ONLY genuine untouched RESISTANCE strictly ABOVE LTP and SUPPORT strictly BELOW LTP.\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"regime_update": "TRENDING_BULL|TRENDING_BEAR|SIDEWAYS", '
    '"zones": [{"type": "RESISTANCE|SUPPORT", "tf": "HIST_1D|HIST_1W|15M", "pivot": 0.0, "why": "short reason"}]}'
)


def _run_historical_band_slice_worker(shared_store, ltp: float, df_15m: pd.DataFrame):
    state = NiftyState(shared_store)
    now = datetime.now(IST)
    low_b, high_b = round(ltp - DEEP_BAND_PTS, 1), round(ltp + DEEP_BAND_PTS, 1)

    df_1d_3y = fetch_upstox_candles("days", 1, days_back=1095, include_intraday=False, closed_only=True)
    df_1w_3y = pd.DataFrame()
    if df_1d_3y is not None and not df_1d_3y.empty:
        df_1w_3y = (
            df_1d_3y.set_index("ts")
            .resample("1W")
            .agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"})
            .dropna()
            .reset_index()
        )

    hist_hits = []
    for df_h, lbl in [(df_1w_3y, "1W"), (df_1d_3y, "1D")]:
        if df_h is None or df_h.empty:
            continue
        in_band = df_h[(df_h["h"] >= low_b) & (df_h["l"] <= high_b)].tail(30)
        for _, r in in_band.iterrows():
            dt_s = _ensure_ist(r["ts"]).strftime("%Y-%m-%d")
            hist_hits.append(f"{lbl} {dt_s} O:{r['o']:.1f} H:{r['h']:.1f} L:{r['l']:.1f} C:{r['c']:.1f}")

    if not hist_hits:
        log.info(f"[NIFTY] 🚀 Uncharted Price / New High @ {ltp:.2f} (0 historical bars in {low_b}-{high_b}). Running Intraday 15M/Trend Mode.")
        tg_send(
            f"🚀 <b>[NIFTY 50 NEW PRICE TERRITORY] {ltp:.2f}</b>\n"
            f"• 3-Year history has 0 candles in <code>{low_b}–{high_b}</code>.\n"
            f"• Tracking <b>15M Blue Boxes + 1st 15M Pool + Trend Pullback Liquidity Hunts</b>."
        )
        return

    usr_p = (
        f"Asset: NIFTY 50 | Current LTP: {ltp:.2f} | Band: {low_b} to {high_b}\n"
        f"Historical Real OHLC Inside Band ({len(hist_hits)} bars):\n" + "\n".join(hist_hits[:40]) + "\n\n"
        f"Recent 15M Real OHLC Candles:\n" + "\n".join(format_ohlc_candles(df_15m, 25))
    )

    ai_res, provider = call_gemini_chain(HEAVY_AI_CHAIN, BAND_SLICE_SYS_PROMPT, usr_p)
    if not ai_res or not isinstance(ai_res.get("zones"), list):
        return

    existing_ai = state.get("ai_zones") or []
    added = []
    for z in ai_res["zones"][:10]:
        try:
            piv = round(float(z["pivot"]), 2)
            ztype = str(z.get("type", "RESISTANCE")).upper()
            if ztype not in ("RESISTANCE", "SUPPORT") or abs(piv - ltp) > DEEP_BAND_PTS:
                continue
            if (ztype == "RESISTANCE" and piv <= ltp) or (ztype == "SUPPORT" and piv >= ltp):
                continue
            zid = f"AI_BAND_{ztype[:3]}_{int(piv)}_{now.strftime('%Y%m%d')}"
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
        lines = [f"• <b>{z['type']}</b> @ <code>{z['pivot']}</code> ({esc(z['why'])})" for z in added[:5]]
        tg_send(
            f"🧭 <b>[NIFTY 50 DEEP BAND SYNC | {esc(provider)}]</b>\n"
            f"• Zone Vacuum (&lt;2 zones in ±300 pts) triggered 3-Yr Band Slice (<code>{low_b}–{high_b}</code>)\n"
            f"• Added <b>{len(added)}</b> Historical Blue Boxes:\n" + "\n".join(lines)
        )


def check_zone_vacuum_and_trigger_band(state: NiftyState, zones: list, ltp: float, df_15m: pd.DataFrame):
    nearby = [z for z in zones if abs(float(z["pivot"]) - ltp) <= VACUUM_RADIUS_PTS]
    if len(nearby) >= 2:
        return
    band_bucket = int(round(ltp / 300.0) * 300)
    lock_key = f"band_slice_lock_{band_bucket}"
    if state.get(lock_key):
        return
    state.set(lock_key, True, ttl=3600)
    threading.Thread(target=_run_historical_band_slice_worker, args=(state.store, ltp, df_15m), daemon=True).start()


# ==============================================================================
# SCHEDULED TOP-DOWN ANALYSIS: 09:20 BIG ANALYSIS + HOURLY (10:20 - 14:20 IST)
# ==============================================================================
TOP_DOWN_SYS_PROMPT = (
    "You are the NIFTY 50 Top-Down Pure Liquidity Architect (1D -> 1H -> 15M -> 5M).\n"
    "Zero indicators (No EMA/RSI/ATR). All prices provided are REAL OHLC Index prices.\n"
    "Determine regime and mark ONLY genuine untouched Blue Boxes: RESISTANCE strictly ABOVE LTP, "
    "SUPPORT strictly BELOW LTP (never mark already-broken levels).\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"regime": "TRENDING_BULL|TRENDING_BEAR|SIDEWAYS", '
    '"if_then_plan": "1 concise sentence IF-THEN plan", '
    '"zones": [{"type": "RESISTANCE|SUPPORT", "tf": "1D|1H|15M", "pivot": 0.0, "why": "short reason"}]}'
)


def _execute_topdown_sync_worker(shared_store, is_morning_big: bool):
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
        df_5m = fetch_upstox_candles("minutes", 5, days_back=5, include_intraday=True, closed_only=True)

    if df_5m is None or df_5m.empty:
        return

    ltp = float(df_5m["c"].iloc[-1])
    pa_regime, _ = get_live_pa_regime(df_1h, df_15m)
    py_zones = extract_pure_blue_boxes(df_1d, df_1h, df_15m, df_5m, ltp, now, state)
    state.set("active_zones", py_zones, ttl=86400)
    state.set("ai_regime", pa_regime, ttl=14400)

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
        f"📊 <b>LTP:</b> <b>{ltp:.2f}</b> | <b>Live PA Regime:</b> <code>{pa_regime}</code> | <b>Valid Zones:</b> <code>{len(py_zones)}</code>\n"
        f"{ring_txt}\n⏳ <i>Running {esc(HYDROGEN_MODEL)} background zone refinement...</i>"
    )

    usr_p = (
        f"Asset: NIFTY 50 | Mode: {title} | Time: {now.strftime('%Y-%m-%d %H:%M IST')} | Current LTP: {ltp:.2f}\n"
        f"Python Pre-Detected Valid Zones: {json.dumps([{'type': z['type'], 'tf': z['tf'], 'pivot': z['pivot'], 'age_d': z['age_days']} for z in py_zones[:10]])}\n"
        + (f"1D Real OHLC (25): {format_ohlc_candles(df_1d, 25, '%Y-%m-%d')}\n" if df_1d is not None and not df_1d.empty else "")
        + f"1H Real OHLC: {format_ohlc_candles(df_1h, 28 if is_morning_big else 20)}\n"
        + f"15M Real OHLC: {format_ohlc_candles(df_15m, 30 if is_morning_big else 24)}\n"
        + f"5M Real OHLC (Recent 36): {format_ohlc_candles(df_5m, 36)}"
    )

    ai_res, provider = call_gemini_chain(HEAVY_AI_CHAIN, TOP_DOWN_SYS_PROMPT, usr_p)
    if not ai_res:
        log.warning(f"[NIFTY] Top-down AI unreachable ({provider}); Python zones remain active.")
        return

    ai_regime = ai_res.get("regime", pa_regime)
    state.set("ai_regime", ai_regime, ttl=14400)
    ai_saved = state.get("ai_zones") or []
    new_cnt = 0
    for z in (ai_res.get("zones") or [])[:10]:
        try:
            piv = round(float(z["pivot"]), 2)
            ztype = str(z.get("type", "RESISTANCE")).upper()
            if ztype not in ("RESISTANCE", "SUPPORT") or abs(piv - ltp) > DEEP_BAND_PTS:
                continue
            if (ztype == "RESISTANCE" and piv <= ltp) or (ztype == "SUPPORT" and piv >= ltp):
                continue
            tf_lbl = str(z.get("tf", "15M_BLUE_BOX")).upper()
            if tf_lbl in ("1D", "1H", "15M"):
                tf_lbl = f"{tf_lbl}_BLUE_BOX"
            zid = f"AI_TD_{ztype[:3]}_{int(piv)}_{now.strftime('%Y%m%d')}"
            ai_saved.append({
                "id": zid, "type": ztype, "tf": tf_lbl, "prio": 2,
                "pivot": piv, "age_days": 0, "touches": 1, "flipped": False,
                "source": "AI_TOPDOWN", "why": str(z.get("why", "AI Marked Blue Box"))
            })
            new_cnt += 1
        except Exception:
            continue

    if new_cnt:
        state.set("ai_zones", ai_saved[-30:], ttl=86400)
        refreshed = extract_pure_blue_boxes(df_1d, df_1h, df_15m, df_5m, ltp, now, state)
        state.set("active_zones", refreshed, ttl=86400)

    tg_send(
        f"🧠 <b>[NIFTY 50 AI ZONE SYNC | {esc(provider)}]</b>\n"
        f"🧭 <b>AI Regime:</b> <b>{esc(ai_regime)}</b> | <b>Saved Valid Zones:</b> <code>{new_cnt}</code>\n"
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
    threading.Thread(target=_execute_topdown_sync_worker, args=(shared_store, is_morning_big), daemon=True).start()


# ==============================================================================
# V10.5 5-GATE PURE LIQUIDITY DETECTOR (5M CLEAN EXECUTION — NO MICRO-NOISE)
# ==============================================================================
def _check_strict_yellow_box(m5: pd.DataFrame, pivot: float, action: str, max_overshoot: float):
    """Strict 5-Gate check: Clean Approach (>=2/4 prior closes inside) + Genuine Pierce (>=1.5 pts) + Crisp Reclaim."""
    if len(m5) < 6:
        return False, 0.0, ""
    c1 = m5.iloc[-1]
    o1, h1, l1, cl1 = float(c1["o"]), float(c1["h"]), float(c1["l"]), float(c1["c"])
    rng1 = h1 - l1
    if rng1 < MIN_RNG_5M_PTS:
        return False, 0.0, ""
    up_w1 = (h1 - max(o1, cl1)) / rng1
    dn_w1 = (min(o1, cl1) - l1) / rng1
    body_r = abs(cl1 - o1) / rng1
    prev4_c = m5["c"].iloc[-5:-1].values

    if action == "BUY_CE":
        if int(np.sum(prev4_c > pivot)) < 2:
            return False, 0.0, ""
        pierce = pivot - l1
        if MIN_PIERCE_PTS <= pierce <= max_overshoot and cl1 > pivot and up_w1 <= 0.35 and (dn_w1 >= 0.32 or (cl1 > o1 and body_r >= 0.35)):
            return True, l1, f"Yellow Box Sweep ({l1:.2f} < {pivot:.2f}, -{pierce:.1f} pts) & Reclaim ({cl1:.2f})"
    elif action == "BUY_PE":
        if int(np.sum(prev4_c < pivot)) < 2:
            return False, 0.0, ""
        pierce = h1 - pivot
        if MIN_PIERCE_PTS <= pierce <= max_overshoot and cl1 < pivot and dn_w1 <= 0.35 and (up_w1 >= 0.32 or (cl1 < o1 and body_r >= 0.35)):
            return True, h1, f"Yellow Box Sweep ({h1:.2f} > {pivot:.2f}, +{pierce:.1f} pts) & Reject ({cl1:.2f})"
    return False, 0.0, ""


def _check_early_watch_candidate(df_5m_closed: pd.DataFrame, m1_bar, pivot: float, action: str, max_overshoot: float):
    """Same sweep+reclaim shape as _check_strict_yellow_box, but on ONE 1-minute candle
    (scaled-down range gate) and using the last 4 CLOSED 5M candles for the clean-approach
    context (never the still-forming bar). Returns True only for the FIRST ("sweep") bar;
    the actual alert fires one bar later, once _check_early_watch_hold confirms it stuck."""
    o1, h1, l1, cl1 = float(m1_bar["o"]), float(m1_bar["h"]), float(m1_bar["l"]), float(m1_bar["c"])
    rng1 = h1 - l1
    if rng1 < EARLY_WATCH_MIN_RNG_1M:
        return False
    up_w1 = (h1 - max(o1, cl1)) / rng1
    dn_w1 = (min(o1, cl1) - l1) / rng1
    body_r = abs(cl1 - o1) / rng1
    prev4_c = df_5m_closed["c"].iloc[-4:].values if len(df_5m_closed) >= 4 else df_5m_closed["c"].values
    if len(prev4_c) < 2:
        return False

    if action == "BUY_CE":
        if int(np.sum(prev4_c > pivot)) < 2:
            return False
        pierce = pivot - l1
        return MIN_PIERCE_PTS <= pierce <= max_overshoot and cl1 > pivot and up_w1 <= 0.35 and (dn_w1 >= 0.32 or (cl1 > o1 and body_r >= 0.35))
    if action == "BUY_PE":
        if int(np.sum(prev4_c < pivot)) < 2:
            return False
        pierce = h1 - pivot
        return MIN_PIERCE_PTS <= pierce <= max_overshoot and cl1 < pivot and dn_w1 <= 0.35 and (up_w1 >= 0.32 or (cl1 < o1 and body_r >= 0.35))
    return False


def _check_early_watch_hold(m1_bar, pivot: float, action: str) -> bool:
    """2nd confirmation bar: the reclaim has to still be standing one minute later."""
    cl1 = float(m1_bar["c"])
    return (cl1 > pivot) if action == "BUY_CE" else (cl1 < pivot)


def detect_early_watch(df_1m: pd.DataFrame, df_5m: pd.DataFrame, zones: list, regime: str, disp_15m: str,
                        state: NiftyState, bar_start: datetime, bar_end: datetime):
    """Scans the still-forming 5M bar (1-min candles between bar_start and bar_end) for an
    early sweep+hold against the SAME Blue Box zones the 5-Gate system watches. Pure heads-up:
    never touches zone-spent / cooldown / opposite-lock state used by the real signal path."""
    if not EARLY_WATCH_ENABLED or df_1m is None or df_1m.empty or len(df_5m) < 4:
        return None
    sub = df_1m[(df_1m["ts"] >= bar_start) & (df_1m["ts"] < bar_end)].reset_index(drop=True)
    if sub.empty:
        return None
    sub = sub.iloc[:EARLY_WATCH_WINDOW_MIN]
    sc5 = _median_range(df_5m, 20)
    max_ov = min(MAX_OVERSHOOT_PTS, 2.2 * sc5)
    ltp = float(df_5m["c"].iloc[-1])

    for b in zones:
        zid = b["id"]
        if state.is_spent(zid):
            continue
        lvl = float(b["pivot"])
        if abs(lvl - ltp) > 4.0 * sc5:
            continue
        action = "BUY_PE" if b["type"] == "RESISTANCE" else "BUY_CE"
        if (action == "BUY_PE" and disp_15m == "TRENDING_BULL") or (action == "BUY_CE" and disp_15m == "TRENDING_BEAR"):
            continue
        if b.get("flipped"):
            if (action == "BUY_CE" and regime != "TRENDING_BULL") or (action == "BUY_PE" and regime != "TRENDING_BEAR"):
                continue
        else:
            is_mega = b.get("tf") == "1D_BLUE_BOX"
            if not is_mega and ((action == "BUY_PE" and regime == "TRENDING_BULL") or (action == "BUY_CE" and regime == "TRENDING_BEAR")):
                continue

        ew_key = f"ew_cd_{zid}_{action}"
        if state.get(ew_key):
            continue

        # Scan every minute for a sweep; if one fails to hold, keep looking at later minutes
        # instead of giving up on the whole bar (a minute-1 sweep that fails must not block a
        # genuine minute-3 sweep+hold from firing).
        for idx in range(len(sub) - 1):
            m1 = sub.iloc[idx]
            if not _check_early_watch_candidate(df_5m, m1, lvl, action, max_ov):
                continue
            hold_bar = sub.iloc[idx + 1]
            if _check_early_watch_hold(hold_bar, lvl, action):
                state.set(ew_key, True, ttl=EARLY_WATCH_COOLDOWN_MIN * 60)
                return {
                    "action": action, "zone_id": zid, "blue_box": lvl, "tf": b.get("tf", ""),
                    "entry_preview": float(hold_bar["c"]),
                    "sweep_ts": _ensure_ist(m1["ts"]), "confirm_ts": _ensure_ist(hold_bar["ts"]),
                    "lead_min": round((bar_end - _ensure_ist(hold_bar["ts"])).total_seconds() / 60.0, 1),
                    "why": b.get("why", ""),
                }
    return None


def detect_nifty_pure_liquidity_setup(m5: pd.DataFrame, m15: pd.DataFrame, zones: list, regime: str, disp_15m: str, state: NiftyState, close_ts: datetime):
    ltp = float(m5["c"].iloc[-1])
    sc = _median_range(m5, 20)
    o1, h1, l1, cl1 = float(m5["o"].iloc[-1]), float(m5["h"].iloc[-1]), float(m5["l"].iloc[-1]), ltp
    rng1 = h1 - l1
    if rng1 < MIN_RNG_5M_PTS or rng1 > MAX_RNG_MULT * sc:
        return None

    up_w1 = (h1 - max(o1, cl1)) / rng1
    dn_w1 = (min(o1, cl1) - l1) / rng1
    body_r = abs(cl1 - o1) / rng1
    max_ov = min(MAX_OVERSHOOT_PTS, 2.2 * sc)

    c_tail = m5.iloc[-4:-1]
    prev_bar = m5.iloc[-2]
    big_up_in = (int((c_tail["c"] > c_tail["o"]).sum()) == 3) or ((float(prev_bar["c"]) - float(prev_bar["o"])) >= 2.0 * sc)
    big_dn_in = (int((c_tail["c"] < c_tail["o"]).sum()) == 3) or ((float(prev_bar["o"]) - float(prev_bar["c"])) >= 2.0 * sc)

    # 1. Persistent HTF / 15M Blue Boxes + Multi-Touch Shelf + Point #10 (Major only) + Breakout Flip Hunt
    for b in zones:
        zid = b["id"]
        if state.is_spent(zid):
            continue
        lvl = float(b["pivot"])
        if abs(lvl - ltp) > 4.0 * sc:
            continue
        action = "BUY_PE" if b["type"] == "RESISTANCE" else "BUY_CE"

        if (action == "BUY_PE" and disp_15m == "TRENDING_BULL") or (action == "BUY_CE" and disp_15m == "TRENDING_BEAR"):
            continue

        if b.get("flipped"):
            if (action == "BUY_CE" and regime != "TRENDING_BULL") or (action == "BUY_PE" and regime != "TRENDING_BEAR"):
                continue
            ok, ext, desc = _check_strict_yellow_box(m5, lvl, action, max_ov)
            if ok:
                return {
                    "name": "BREAKOUT_FLIP_HUNT", "action": action, "zone_id": zid, "blue_box": lvl,
                    "wick_extreme": round(ext, 2), "entry": ltp,
                    "reason": f"Broken {b['tf']} Polarity Flip Retest — {desc}"
                }
        else:
            is_mega = b.get("tf") in ("1D_BLUE_BOX", "HIST_1D", "HIST_1W")
            if not is_mega and ((action == "BUY_PE" and regime == "TRENDING_BULL") or (action == "BUY_CE" and regime == "TRENDING_BEAR")):
                continue

            ok, ext, desc = _check_strict_yellow_box(m5, lvl, action, max_ov)
            if ok:
                is_major_pt10 = (b.get("tf") in ("1D_BLUE_BOX", "1H_BLUE_BOX")) or (b.get("touches", 1) >= 2)
                if is_major_pt10 and ((action == "BUY_PE" and big_up_in and up_w1 >= 0.35) or (action == "BUY_CE" and big_dn_in and dn_w1 >= 0.35)):
                    sname = "EXHAUSTION_TRAP_PT10"
                elif b.get("touches", 1) >= 2:
                    sname = "MULTI_TOUCH_SHELF"
                else:
                    sname = f"OLD_{b['tf']}" if b.get("age_days", 0) >= 5 else b["tf"]

                return {
                    "name": sname, "action": action, "zone_id": zid, "blue_box": lvl,
                    "wick_extreme": round(ext, 2), "entry": ltp,
                    "reason": f"{desc} | {b.get('why', '')} [Touches:{b.get('touches', 1)}, Age:{b.get('age_days', 0)}d]"
                }

    # 2. Today's 1ST_15M Liquidity Pool (09:15-09:30 High/Low — NIFTY #1 Winner)
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
                if (act == "BUY_PE" and (regime == "TRENDING_BULL" or disp_15m == "TRENDING_BULL")) or (act == "BUY_CE" and (regime == "TRENDING_BEAR" or disp_15m == "TRENDING_BEAR")):
                    continue
                ok, ext, desc = _check_strict_yellow_box(m5, lvl, act, max_ov)
                if ok:
                    return {
                        "name": "1ST_15M_LIQ_POOL", "action": act, "zone_id": zid, "blue_box": lvl,
                        "wick_extreme": round(ext, 2), "entry": ltp,
                        "reason": f"Day's 1st 15M ({lvl:.2f}) — {desc}"
                    }

    # 3. 5M Trend Pullback Liquidity Hunt (Strict Pierce + Clean Impulse)
    if len(m5) >= 16:
        hh5, ll5 = m5["h"].values, m5["l"].values
        i = len(m5) - 1
        if regime == "TRENDING_BULL":
            pb_idx = i - 6 + int(np.argmin(ll5[i - 6 : i - 1]))
            pb_low = float(ll5[pb_idx])
            imp_h = float(np.max(hh5[max(0, pb_idx - 8) : pb_idx]))
            pierce = pb_low - l1
            if (imp_h - pb_low) >= 1.2 * sc and pb_low > float(np.min(ll5[max(0, pb_idx - 12) : pb_idx])):
                if MIN_PIERCE_PTS <= pierce <= max_ov and cl1 > pb_low and cl1 > o1 and up_w1 <= 0.35 and dn_w1 >= 0.30:
                    return {
                        "name": "TREND_PULLBACK_LIQ_HUNT", "action": "BUY_CE", "zone_id": "",
                        "blue_box": round(pb_low, 2), "wick_extreme": round(l1, 2), "entry": ltp,
                        "reason": f"Bullish Pullback Sweep below {pb_low:.2f} (Wick {l1:.2f}, -{pierce:.1f} pts) & Green Reclaim ({cl1:.2f})"
                    }
        elif regime == "TRENDING_BEAR":
            pb_idx = i - 6 + int(np.argmax(hh5[i - 6 : i - 1]))
            pb_high = float(hh5[pb_idx])
            imp_l = float(np.min(ll5[max(0, pb_idx - 8) : pb_idx]))
            pierce = h1 - pb_high
            if (pb_high - imp_l) >= 1.2 * sc and pb_high < float(np.max(hh5[max(0, pb_idx - 12) : pb_idx])):
                if MIN_PIERCE_PTS <= pierce <= max_ov and cl1 < pb_high and cl1 < o1 and dn_w1 <= 0.35 and up_w1 >= 0.30:
                    return {
                        "name": "TREND_PULLBACK_LIQ_HUNT", "action": "BUY_PE", "zone_id": "",
                        "blue_box": round(pb_high, 2), "wick_extreme": round(h1, 2), "entry": ltp,
                        "reason": f"Bearish Pullback Sweep above {pb_high:.2f} (Wick {h1:.2f}, +{pierce:.1f} pts) & Red Reclaim ({cl1:.2f})"
                    }

    return None


# ==============================================================================
# PARALLEL NON-BLOCKING BACKGROUND AI CONFIRMATION (REAL OHLC — ZERO HALLUCINATION)
# ==============================================================================
LIVE_CONFIRM_SYS_PROMPT = (
    "You are the NIFTY 50 Institutional Pure Price-Action Supervisor (Zero Indicators, No Orderflow/Volume Delta).\n"
    "All candle prices provided below are ACTUAL OHLC Index prices.\n"
    "Audit the 5M liquidity sweep & reclaim quality, wick rejection strength, trend alignment, and nearby opposing walls.\n"
    "Specifically flag (in the note) if this is a STALE zone (3rd+ touch, reclaim looks tired / shrinking "
    "range into the level) or a FRESH first-touch reclaim with real displacement — stale reclaims are the "
    "most common source of early-but-low-quality entries and should push the verdict toward WAIT_CAUTION.\n"
    "Also flag if the entry sits in the first 10 minutes of the session or the last 30 minutes before close, "
    "where follow-through is historically less reliable.\n"
    "Respond ONLY in strictly valid JSON:\n"
    '{"verdict": "APPROVED|WAIT_CAUTION|REJECT_TRAP", "confidence": "HIGH|MEDIUM|LOW", "note": "1-2 concise pure price-action sentences"}'
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
# DIAGNOSTICS & DAILY 09:03-09:10 IST HEALTH AUDIT (WEEKEND-SAFE days_back=5)
# ==============================================================================
def run_startup_diagnostics(shared_store=None) -> dict:
    log.info("[NIFTY] Running Startup Diagnostics...")
    t0 = time.time()
    df_t = fetch_upstox_candles("minutes", 5, days_back=5, include_intraday=True, closed_only=False)
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
            f"• <b>Valid Blue Boxes:</b> <code>{len(zones)}</code> | <b>Regime:</b> <code>{regime}</code>\n"
            f"• <b>AI Calls Today:</b> <code>{_daily_ai_calls}</code>"
        )


# ==============================================================================
# MAIN 5M CYCLE (PYTHON-FIRST INSTANT ALERT -> PARALLEL AI)
# ==============================================================================
def run_nifty_cycle(shared_store=None):
    """
    Safe to invoke every ~1 minute during market hours (recommended, for Early Watch to have
    any effect) or every 5 minutes (old cadence, Early Watch then effectively stays off since
    the forming-bar window it scans will already be closed by the next call). The heavy 5-Gate
    block below only ever runs once per NEWLY closed 5M candle either way, via `new_5m_bar`.
    """
    if not _nifty_lock.acquire(blocking=False):
        return
    try:
        state = NiftyState(shared_store)

        # 1. Trigger 09:20 BIG Analysis or Hourly Sync (10:20 - 14:20 IST)
        trigger_topdown_if_due(shared_store, force=False)

        # 2. Fetch closed 5M candles (days_back=5 handles weekends/holidays cleanly)
        df_5m = fetch_upstox_candles("minutes", 5, days_back=5, include_intraday=True, closed_only=True)
        if df_5m is None or len(df_5m) < 25:
            return

        ltp = float(df_5m["c"].iloc[-1])
        check_daily_morning_health(state, ltp)

        last_bar_ts = _ensure_ist(df_5m["ts"].iloc[-1])
        close_ts = last_bar_ts + timedelta(minutes=5)
        bar_key = close_ts.strftime("%Y-%m-%d_%H:%M")
        new_5m_bar = state.get("last_eval_bar") != bar_key

        mn = close_ts.hour * 60 + close_ts.minute
        if mn < 9 * 60 + 25 or mn > 14 * 60 + 30:
            return

        if new_5m_bar:
            state.set("last_eval_bar", bar_key, ttl=600)

            df_15m = fetch_upstox_candles("minutes", 15, days_back=10, include_intraday=True, closed_only=True)
            df_1h = fetch_upstox_candles("hours", 1, days_back=20, include_intraday=True, closed_only=True)

            # Live Dynamic PA Regime (never stale)
            live_regime, disp_15m = get_live_pa_regime(df_1h, df_15m)
            ai_reg = state.get("ai_regime")
            regime = live_regime if (live_regime != "SIDEWAYS" or not ai_reg) else ai_reg
            state.set("regime", regime, ttl=7200)
            state.set("disp_15m", disp_15m, ttl=7200)

            # Refresh valid zones & purge/flip broken zones every 5M bar
            cached_htf = state.get("active_zones") or []
            live_zones = extract_pure_blue_boxes(None, df_1h, df_15m, df_5m, ltp, close_ts, state)
            zones = list(live_zones)
            for cz in cached_htf:
                if not state.is_spent(cz["id"]):
                    st = _update_or_purge_zone(cz, df_5m, ltp)
                    if st == "PURGE":
                        state.mark_spent(cz["id"])
                        continue
                    if st == "FLIP":
                        cz = dict(cz)
                        cz["type"] = "SUPPORT" if cz["type"] == "RESISTANCE" else "RESISTANCE"
                        cz["flipped"] = True
                    if not any(x["type"] == cz["type"] and x.get("flipped") == cz.get("flipped") and abs(float(x["pivot"]) - float(cz["pivot"])) <= SHELF_MERGE_PTS for x in zones):
                        zones.append(cz)
            state.set("active_zones", zones[:20], ttl=21600)

            # 3. Zone Vacuum Check (< 2 zones in LTP ± 300 pts) -> 3-Yr Band Slice
            check_zone_vacuum_and_trigger_band(state, zones, ltp, df_15m)
        else:
            # In-between 1-min tick (same 5M bar still forming): reuse the last computed
            # regime/zones rather than re-hitting Upstox for 15M/1H data every minute.
            regime = state.get("regime", "SIDEWAYS")
            disp_15m = state.get("disp_15m", "NONE")
            zones = state.get("active_zones") or []
            df_15m = None

        # 3b. EARLY WATCH: scan the 1-min bars of the bar now forming (close_ts -> +5m) for an
        # early sweep+hold against these same zones. Non-blocking "heads up" only -- see the
        # EARLY_WATCH_* constants/comment above for why this stays separate from the 5-Gate path.
        if EARLY_WATCH_ENABLED and zones:
            try:
                df_1m = fetch_upstox_candles("minutes", 1, days_back=1, include_intraday=True, closed_only=True)
                ew = detect_early_watch(df_1m, df_5m, zones, regime, disp_15m, state, close_ts, close_ts + timedelta(minutes=5))
                if ew:
                    em = "🟢" if ew["action"] == "BUY_CE" else "🔴"
                    tg_send(
                        f"👀 {em} <b>[NIFTY 50 EARLY WATCH] {ew['action']} | {ew['confirm_ts'].strftime('%H:%M IST')}</b>\n\n"
                        f"⚡ <b>Zone         :</b> <code>{esc(ew['tf'])}</code> @ <b>{ew['blue_box']:.2f}</b>\n"
                        f"🎯 <b>Forming Entry:</b> <b>{ew['entry_preview']:.2f}</b> (~<b>{ew['lead_min']:.0f} min</b> ahead of the 5M close)\n"
                        f"📝 1-Min reclaim holding past the zone — <b>NOT a confirmed signal yet.</b>\n"
                        f"<i>Chart var nazar thev; asli alert feela 5M candle clean close zaalya nantarach.</i>"
                    )
            except Exception:
                log.exception("[NIFTY] Early Watch scan error")

        if not new_5m_bar:
            return

        # 4. Evaluate 5-Gate Pure Liquidity Setups (unchanged — this is still the ONLY path
        #    that actually trades; Early Watch above never feeds into it)
        setup = detect_nifty_pure_liquidity_setup(df_5m, df_15m, zones, regime, disp_15m, state, close_ts)
        if not setup:
            log.info(f"[NIFTY] {close_ts.strftime('%H:%M IST')} | LTP:{ltp:.2f} | Regime:{regime} | ValidZones:{len(zones)} -> WAITING")
            return

        # Gate 5: Check cooldowns & opposite lock BEFORE marking zone spent!
        last_dir = state.get("last_sig_dir")
        last_dir_t = state.get("last_sig_ts") or 0
        if last_dir and last_dir != setup["action"] and (time.time() - last_dir_t) < OPPOSITE_LOCK_MIN * 60:
            log.info(f"[NIFTY] Opposite lock active ({last_dir}); skipped {setup['action']} without spending zone.")
            return

        cd_key = f"cd_{setup['name']}_{setup['action']}"
        if state.get(cd_key):
            return

        if setup.get("zone_id"):
            state.mark_spent(setup["zone_id"])
        state.set("last_sig_dir", setup["action"], ttl=OPPOSITE_LOCK_MIN * 60)
        state.set("last_sig_ts", time.time(), ttl=OPPOSITE_LOCK_MIN * 60)
        state.set(cd_key, True, ttl=SETUP_COOLDOWN_MIN * 60)

        # 5. PYTHON-FIRST INSTANT TELEGRAM ALERT
        em = "🟢" if setup["action"] == "BUY_CE" else "🔴"
        sig_time_str = close_ts.strftime("%H:%M IST")
        msg = (
            f"{em} <b>[NIFTY 50 PURE LIQUIDITY] {setup['action']} | {sig_time_str}</b>\n\n"
            f"⚡ <b>Setup        :</b> <code>{esc(setup['name'])}</code>\n"
            f"🎯 <b>Index Entry  :</b> <b>{setup['entry']:.2f}</b>\n"
            f"🟦 <b>Blue Box Lvl :</b> <b>{setup['blue_box']:.2f}</b> | <b>Yellow Wick Ext:</b> <b>{setup['wick_extreme']:.2f}</b>\n"
            f"🧭 <b>Live Regime  :</b> <code>{esc(regime)}</code>\n"
            f"📝 <b>Sweep Logic  :</b> {esc(setup['reason'])}\n\n"
            f"⏳ <i>Chart baghun manual confirm kar (Manual SL &amp; Target mode). Running parallel AI audit...</i>"
        )
        tg_send(msg)
        log.info(f"[NIFTY] 🔥 QUALITY ALERT: {setup['name']} ({setup['action']}) @ {setup['entry']:.2f}")

        # 6. Parallel Background AI Audit with REAL OHLC (No "Delta" word)
        usr_p = (
            f"Asset: NIFTY 50 | Time: {sig_time_str} | Entry: {setup['entry']:.2f}\n"
            f"Setup: {setup['name']} ({setup['action']}) | Blue Box: {setup['blue_box']:.2f} | Wick Ext: {setup['wick_extreme']:.2f}\n"
            f"Live Regime: {regime} | Reason: {setup['reason']}\n"
            f"Recent 16 15M Real OHLC Candles:\n" + "\n".join(format_ohlc_candles(df_15m, 16)) + "\n"
            f"Recent 28 5M Real OHLC Candles:\n" + "\n".join(format_ohlc_candles(df_5m, 28))
        )
        threading.Thread(target=_background_ai_confirm_worker, args=(setup, usr_p, sig_time_str), daemon=True).start()

    except Exception as e:
        log.exception(f"[NIFTY] Cycle error: {type(e).__name__}")
    finally:
        try:
            _nifty_lock.release()
        except RuntimeError:
            pass
