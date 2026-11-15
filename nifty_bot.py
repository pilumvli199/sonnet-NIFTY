"""
╔══════════════════════════════════════════════════════════════════════════╗
║   NIFTY 50 LIQUIDITY HUNT BOT — V8 INDEX-ONLY EDITION (nifty_bot.py)     ║
║   Namespace: NIFTY:*  |  Logger: [NIFTY]  |  Upstox V3 Index Feed        ║
║                                                                          ║
║   V8 ARCHITECTURE & TUNED 6-SETUP ENGINE:                                ║
║   1. PURE INDEX SETUP IDENTIFIER (NO SL/TARGET, NO OPTION CHAIN, NO CAP):║
║      - Completely removes Option chain/candle fetching, Option SL/Tgt,   ║
║        Index SL/Tgt calculation, and the `idx_risk > 45` cutoff.         ║
║      - Bot strictly identifies clean Index liquidity setups and fires    ║
║        zero-lag alerts on 5M candle close; user manages SL/Tgt manually. ║
║   2. DUAL-CHAIN AI ROUTING + DAILY RPD QUOTA TRACKER + HYDROGEN THREAD:  ║
║      - Heavy / Top-Down Chain (09:20 Morning, Hourly Sync, & Background  ║
║        Deep Confirmation):                                               ║
║          1) gemini-4-hydrogen      (Thinking: HIGH, Timeout: 25s, 20 RPD)║
║          2) gemini-3.8-flash       (Thinking: HIGH, Timeout: 15s, 20 RPD)║
║          3) gemini-3.6-flash       (Thinking: OFF,  Timeout: 20s, 20 RPD)║
║          4) gemini-3.5-flash-lite  (Thinking: HIGH, Timeout: 20s, 500RPD)║
║      - Normal / 5M Live Supervisor Chain (Zero-Lag Entry Filter):        ║
║          1) gemini-3.5-flash-lite  (Thinking: OFF,  Timeout: 15s, 500RPD)║
║          2) gemini-3.1-flash-lite  (Thinking: OFF,  Timeout: 15s, 500RPD)║
║      - Non-Blocking Background Hydrogen Thread: Sends instant Lite alert ║
║        first, while `gemini-4-hydrogen` (HIGH) verifies in background    ║
║        and posts a follow-up Telegram deep confirmation!                 ║
║   3. FINAL 6 TUNED SETUPS (2026 BACKTEST VERIFIED):                      ║
║      - S1_TREND_LIQ : 15M + 1H Trend-Aligned Fresh Liquidity Pool Sweep. ║
║      - PB_MTF       : 15M + 1H Trend-Aligned EMA21 (<=45pt) Pullback Hunt.║
║      - S2_WEEKLY    : Previous Week High/Low (PWH/PWL) Sweep & Reclaim.  ║
║      - S2_PD_2ND    : Prev Day (PDH/PDL) 1st-Touch Blue Box -> 2nd Hunt. ║
║      - S2_ORIGIN60  : Institutional Origin (>=60pt move, ±600pt) Sweep.  ║
║      - S3_FAKEOUT   : Tight Sideways Box (12–32pt) Fakeout Reclaim.      ║
║   4. CHASE -> ENTRY MODE TAG & TRANSPARENT AI WAIT ALERTS:               ║
║      - Sweep distance <= 28 pts -> `MARKET_NOW`; > 28 pts -> exact       ║
║        `PULLBACK_50PCT` midpoint limit entry level (never blocks!).      ║
║      - AI `WAIT` / rejection reasons are sent directly to Telegram.      ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import math
import time
import html
import logging
import threading
import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
import pytz

IST = pytz.timezone("Asia/Kolkata")
log = logging.getLogger("NIFTY")

# ── Environment Configuration ────────────────────────────────────────
UPSTOX_TOKEN     = os.getenv("UPSTOX_ANALYTICS_TOKEN", "")
GEMINI_API_KEYS  = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip()]
ANTHROPIC_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
TG_BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID       = os.getenv("NIFTY_TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", ""))

NIFTY_KEY        = "NSE_INDEX|Nifty 50"
NIFTY_KEY_ENC    = quote(NIFTY_KEY)
UPSTOX_V3        = "https://api.upstox.com/v3"

# ── Bot & Cooldown Settings ──────────────────────────────────────────
PRICE_RANGE_WINDOW_PTS = int(os.getenv("NIFTY_PRICE_RANGE_PTS", "50"))      # ±50 pts Historical sync window
ZONE_SCAN_RADIUS_PTS   = int(os.getenv("NIFTY_ZONE_SCAN_RADIUS_PTS", "600")) # ±600 pts zone radius (handles big gaps)
WAIT_COOLDOWN_MIN      = int(os.getenv("NIFTY_WAIT_COOLDOWN_MIN", "10"))
SIGNAL_COOLDOWN_MIN    = int(os.getenv("NIFTY_SIGNAL_COOLDOWN_MIN", "20"))
PULLBACK_COOLDOWN_MIN  = int(os.getenv("NIFTY_PULLBACK_COOLDOWN_MIN", "12"))
MAX_ZONES              = int(os.getenv("NIFTY_MAX_ZONES", "14"))
MAX_AI_RAW_LOG         = int(os.getenv("NIFTY_MAX_AI_RAW_LOG", "100"))
MIN_CONFIRMATIONS      = int(os.getenv("NIFTY_MIN_CONFIRMATIONS", "2"))

# ── Candle & Setup Engine Thresholds ─────────────────────────────────
SIDEWAYS_MIN_CANDLES   = int(os.getenv("NIFTY_SIDEWAYS_MIN_CANDLES", "5"))
SIDEWAYS_MAX_RANGE     = int(os.getenv("NIFTY_SIDEWAYS_MAX_RANGE", "32"))
ORIGIN_MIN_MOVE_PTS    = int(os.getenv("NIFTY_ORIGIN_MIN_MOVE_PTS", "60"))    # Tuned >=60pt institutional displacement
HIST_MIN_DISPLACEMENT  = int(os.getenv("NIFTY_HIST_MIN_DISPLACEMENT", "120"))
TREND_LIQ_MIN_BOUNCE   = int(os.getenv("NIFTY_TREND_LIQ_MIN_BOUNCE", "8"))
PB_MAX_EMA21_DIST_PTS  = int(os.getenv("NIFTY_PB_MAX_EMA21_DIST_PTS", "45"))
MARKET_ENTRY_MAX_DIST  = int(os.getenv("NIFTY_MARKET_ENTRY_MAX_DIST", "28"))  # <=28pt MARKET_NOW, >28pt PULLBACK_50PCT
LAST_AI_CALL_MINUTE    = int(os.getenv("NIFTY_LAST_AI_CALL_MINUTE", "10"))
REJECTION_WICK_RATIO   = float(os.getenv("NIFTY_REJECTION_WICK_RATIO", "0.30"))
DOMINANCE_MIN_RANGE    = int(os.getenv("NIFTY_DOMINANCE_MIN_RANGE", "18"))
DOMINANCE_LOOKBACK     = int(os.getenv("NIFTY_DOMINANCE_LOOKBACK", "15"))

# ── Watchdog & Holidays ──────────────────────────────────────────────
FEED_FAIL_ALERT_AFTER  = int(os.getenv("FEED_FAIL_ALERT_AFTER", "3"))
NSE_HOLIDAYS           = {d.strip() for d in os.getenv("NSE_HOLIDAYS", "").split(",") if d.strip()}

# ── AI Model Routing Chains (Free-Tier RPD & Benchmark Tuned) ────────
HEAVY_AI_CHAIN = [
    {"model": "gemini-4-hydrogen",     "thinking": "HIGH", "timeout": 25, "rpd": 20},
    {"model": "gemini-3.8-flash",      "thinking": "HIGH", "timeout": 15, "rpd": 20},
    {"model": "gemini-3.6-flash",      "thinking": "OFF",  "timeout": 20, "rpd": 20},
    {"model": "gemini-3.5-flash-lite", "thinking": "HIGH", "timeout": 20, "rpd": 500},
]

NORMAL_5M_AI_CHAIN = [
    {"model": "gemini-3.5-flash-lite", "thinking": "OFF",  "timeout": 15, "rpd": 500},
    {"model": "gemini-3.1-flash-lite", "thinking": "OFF",  "timeout": 15, "rpd": 500},
]

ANALYSIS_CLAUDE_MODEL     = os.getenv("ANALYSIS_MODEL", "claude-sonnet-5-5")
DECISION_CLAUDE_MODEL     = os.getenv("CLAUDE_BACKUP_MODEL", "claude-haiku-5-5")
FIRST_ANALYSIS_MAX_TOKENS = int(os.getenv("FIRST_ANALYSIS_MAX_TOKENS", "2600"))
ZONE_DECISION_MAX_TOKENS  = int(os.getenv("ZONE_DECISION_MAX_TOKENS", "900"))

_nifty_lock   = threading.Lock()
_gemini_idx   = 0
_feed_fails   = 0
_feed_alerted = False


def esc(text):
    return html.escape(str(text), quote=False) if text is not None else ""


# ═══════════════════════════════════════════════════════════════════════
# SECTION 1: NAMESPACED STATE ENGINE & RPD QUOTA TRACKER (PREFIX: "NIFTY:")
# ═══════════════════════════════════════════════════════════════════════

class NiftyState:
    """Strictly prefixes every Redis/RAM key with 'NIFTY:' so assets never collide."""
    PREFIX = "NIFTY:"

    def __init__(self, shared_store):
        self.store = shared_store

    def set(self, key, value, ttl=86400):
        self.store.set(f"{self.PREFIX}{key}", value, ttl=ttl)

    def get(self, key):
        return self.store.get(f"{self.PREFIX}{key}")

    def delete(self, key):
        self.store.delete(f"{self.PREFIX}{key}")

    def get_rpd_used(self, model_name: str) -> int:
        today = datetime.now(IST).strftime("%Y-%m-%d")
        val = self.get(f"rpd_{model_name}_{today}")
        try:
            return int(val) if val is not None else 0
        except (TypeError, ValueError):
            return 0

    def inc_rpd_used(self, model_name: str) -> int:
        today = datetime.now(IST).strftime("%Y-%m-%d")
        cur = self.get_rpd_used(model_name) + 1
        self.set(f"rpd_{model_name}_{today}", cur, ttl=90000)
        return cur


def save_zones(state: NiftyState, zones):
    state.set("zones", zones[:MAX_ZONES])
    state.set("zones_date", datetime.now(IST).strftime("%Y-%m-%d"))
    log.info(f"[NIFTY] ✅ {len(zones[:MAX_ZONES])} zones saved")


def get_zones(state: NiftyState):
    return state.get("zones") or []


def update_single_zone_state(state: NiftyState, zone_id, touch_count=None, blue_box_extreme=None,
                             flipped_type=None, preferred_action=None):
    zones = get_zones(state)
    changed = False
    for z in zones:
        if z.get("id") == zone_id:
            if touch_count is not None:
                z["touch_count"] = touch_count
            if blue_box_extreme is not None:
                z["blue_box_extreme"] = blue_box_extreme
            if flipped_type:
                z["type"] = flipped_type
            if preferred_action:
                z["preferred_action"] = preferred_action
            changed = True
            break
    if changed:
        save_zones(state, zones)


def save_morning_context(state: NiftyState, ctx):
    state.set("morning_context", ctx)
    state.set("morning_context_date", datetime.now(IST).strftime("%Y-%m-%d"))


def get_morning_context(state: NiftyState):
    return state.get("morning_context") or {}


def is_today_data_valid(state: NiftyState):
    today = datetime.now(IST).strftime("%Y-%m-%d")
    return bool(get_zones(state) and get_morning_context(state)
                and state.get("zones_date") == today and state.get("morning_context_date") == today)


def get_signal_history(state: NiftyState):
    return state.get("signal_history") or []


def save_signal_log(state: NiftyState, entry):
    h = get_signal_history(state)
    h.append(entry)
    state.set("signal_history", h)


def save_ai_raw_log(state: NiftyState, entry):
    logs = state.get("ai_raw_log") or []
    logs.append(entry)
    state.set("ai_raw_log", logs[-MAX_AI_RAW_LOG:])


def save_flip_event(state: NiftyState, zone_id, direction):
    events = state.get("flip_log") or []
    events.append({"time": datetime.now(IST).strftime("%H:%M"), "zone_id": zone_id, "direction": direction})
    state.set("flip_log", events[-20:])


def get_structure_shift(state: NiftyState, window_minutes=60, min_flips=2):
    events = state.get("flip_log") or []
    if not events:
        return None
    now = datetime.now(IST)
    recent = []
    for e in events:
        try:
            t = IST.localize(datetime.strptime(e["time"], "%H:%M").replace(
                year=now.year, month=now.month, day=now.day))
            if 0 <= (now - t).total_seconds() / 60 <= window_minutes:
                recent.append(e)
        except Exception:
            continue
    down = [e for e in recent if e["direction"] == "down"]
    up = [e for e in recent if e["direction"] == "up"]
    if len(down) >= min_flips and len(down) > len(up):
        return {"direction": "down", "count": len(down), "since": down[0]["time"]}
    if len(up) >= min_flips and len(up) > len(down):
        return {"direction": "up", "count": len(up), "since": up[0]["time"]}
    return None


def zone_cooldown_ok(state: NiftyState, zone_id):
    cd = state.get(f"cooldown_{zone_id}")
    if cd:
        try:
            until = datetime.fromisoformat(cd["until"])
            if until.tzinfo is None:
                until = IST.localize(until)
            if datetime.now(IST) < until:
                return False
        except Exception:
            pass
    return True


def mark_zone_cooldown(state: NiftyState, zone_id, signal_type, custom_mins=None):
    if custom_mins is not None:
        mins = custom_mins
    else:
        mins = SIGNAL_COOLDOWN_MIN if signal_type in ("BUY_CE", "BUY_PE") else WAIT_COOLDOWN_MIN
    until = datetime.now(IST) + timedelta(minutes=mins)
    state.set(f"cooldown_{zone_id}", {"until": until.isoformat()}, ttl=mins * 60 + 120)
    log.info(f"[NIFTY] ⏳ Cooldown set on {zone_id} for {mins} min ({signal_type})")


def flush_day(state: NiftyState):
    for k in ["zones", "zones_date", "morning_context", "morning_context_date",
              "signal_history", "ai_raw_log", "flip_log", "active_s3_box", "hist_memory_bank"]:
        state.delete(k)
    log.info("[NIFTY] 🧹 Day data flushed")


# ═══════════════════════════════════════════════════════════════════════
# SECTION 2: UPSTOX INDEX-ONLY FETCHERS (1D, 1H, 30M, 15M, 5M, 1M)
# ═══════════════════════════════════════════════════════════════════════

def _feed_ok():
    global _feed_fails, _feed_alerted
    if _feed_alerted:
        tg_send("✅ <b>[NIFTY] DATA FEED BACK</b> — Upstox responding again.")
    _feed_fails = 0
    _feed_alerted = False


def _feed_fail(detail):
    global _feed_fails, _feed_alerted
    _feed_fails += 1
    log.error(f"[NIFTY] Feed failure #{_feed_fails}: {detail}")
    if _feed_fails >= FEED_FAIL_ALERT_AFTER and not _feed_alerted:
        _feed_alerted = True
        tg_send(f"🔴 <b>[NIFTY] DATA FEED DOWN</b>\n{_feed_fails} consecutive Upstox failures.\n"
                f"Last error: {esc(str(detail)[:120])}\n\n"
                f"Check Upstox token or network status!")


def upstox_get(url, retries=3):
    hdrs = {"Authorization": f"Bearer {UPSTOX_TOKEN}", "Accept": "application/json"}
    last_detail = "unknown"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=hdrs, timeout=15)
            if r.status_code == 200:
                d = r.json()
                if d.get("status") == "success":
                    _feed_ok()
                    return d.get("data", {})
                last_detail = f"status={d.get('status')}"
            elif r.status_code in (401, 403):
                last_detail = f"HTTP {r.status_code} — token expired/invalid"
                break
            elif r.status_code == 429:
                last_detail = "HTTP 429 rate limited"
                time.sleep(2 ** attempt)
            else:
                last_detail = f"HTTP {r.status_code}"
        except Exception as e:
            last_detail = str(e)
        time.sleep(1)
    _feed_fail(last_detail)
    return None


def get_ltp():
    data = upstox_get(f"{UPSTOX_V3}/market-quote/ltp?instrument_key={NIFTY_KEY_ENC}")
    if data:
        first = next(iter(data.values()), None)
        if first and "last_price" in first:
            return float(first["last_price"])
    return None


def _to_df(data):
    if not data or "candles" not in data:
        return pd.DataFrame()
    df = pd.DataFrame(data["candles"], columns=["ts", "o", "h", "l", "c", "v", "oi"])
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    df[["o", "h", "l", "c"]] = df[["o", "h", "l", "c"]].round(0).astype(int)
    return df


def _merge_hist_and_intraday(hist_df, intra_df):
    if hist_df is None or hist_df.empty:
        return intra_df if intra_df is not None else pd.DataFrame()
    if intra_df is None or intra_df.empty:
        return hist_df
    combined = pd.concat([hist_df, intra_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ts"], keep="last").sort_values("ts").reset_index(drop=True)
    return combined


def fetch_historical(unit, interval, candles_needed, days_override=None):
    now = datetime.now(IST)
    to_date = now.strftime("%Y-%m-%d")
    if days_override is not None:
        from_date = (now - timedelta(days=days_override)).strftime("%Y-%m-%d")
    elif unit == "days":
        from_date = (now - timedelta(days=candles_needed + 20)).strftime("%Y-%m-%d")
    elif unit == "hours":
        from_date = (now - timedelta(days=18)).strftime("%Y-%m-%d")
    else:
        from_date = (now - timedelta(days=6)).strftime("%Y-%m-%d")
    data = upstox_get(f"{UPSTOX_V3}/historical-candle/{NIFTY_KEY_ENC}/{unit}/{interval}/{to_date}/{from_date}")
    df = _to_df(data)
    return df.tail(candles_needed) if (not df.empty and candles_needed) else df


def fetch_intraday(unit, interval):
    return _to_df(upstox_get(f"{UPSTOX_V3}/historical-candle/intraday/{NIFTY_KEY_ENC}/{unit}/{interval}"))


def fetch_top_down_data():
    """Fetches full multi-timeframe Index hierarchy (1D, 1H, 30M, 15M, 5M)."""
    log.info("[NIFTY] Fetching Top-Down Index TF data (1D, 1H, 30M, 15M, 5M)...")
    daily = fetch_historical("days", 1, 25, days_override=45)
    h1    = _merge_hist_and_intraday(fetch_historical("hours", 1, 35), fetch_intraday("hours", 1))
    m30   = _merge_hist_and_intraday(fetch_historical("minutes", 30, 45), fetch_intraday("minutes", 30))
    m15   = _merge_hist_and_intraday(fetch_historical("minutes", 15, 65), fetch_intraday("minutes", 15))
    m5    = _merge_hist_and_intraday(fetch_historical("minutes", 5, 80), fetch_intraday("minutes", 5))
    return {"daily": daily, "h1": h1, "m30": m30, "m15": m15, "m5": m5}


def fetch_live_monitor_data(include_1m=True):
    """Fetches merged historical + intraday Index data for 1H, 15M, 5M, and 1M."""
    h1  = _merge_hist_and_intraday(fetch_historical("hours", 1, 30), fetch_intraday("hours", 1))
    m15 = _merge_hist_and_intraday(fetch_historical("minutes", 15, 50), fetch_intraday("minutes", 15))
    m5  = _merge_hist_and_intraday(fetch_historical("minutes", 5, 65), fetch_intraday("minutes", 5))

    m1 = pd.DataFrame()
    if include_1m:
        m1 = fetch_intraday("minutes", 1)
        if m1.empty or len(m1) < 15:
            m1 = _merge_hist_and_intraday(fetch_historical("minutes", 1, 45), m1)

    return {"h1": h1, "m15": m15, "m5": m5, "m1": m1}


# ═══════════════════════════════════════════════════════════════════════
# SECTION 3: TIMESTAMP CANDLE FILTER & 15M + 1H MULTI-TF TREND ENGINE
# ═══════════════════════════════════════════════════════════════════════

def drop_incomplete_candle(df, interval_minutes=5, buffer_sec=8):
    """Drops the last row ONLY if `now < candle_start_ts + interval_minutes + buffer_sec`."""
    if df is None or df.empty or len(df) < 2:
        return pd.DataFrame() if df is None else df
    try:
        last_ts = pd.Timestamp(df.iloc[-1]["ts"])
        last_ts = (IST.localize(last_ts.to_pydatetime()) if last_ts.tzinfo is None
                   else last_ts.tz_convert(IST).to_pydatetime())
        if datetime.now(IST) < last_ts + timedelta(minutes=interval_minutes, seconds=buffer_sec):
            return df.iloc[:-1].copy()
    except Exception as e:
        log.error(f"[NIFTY] drop_incomplete_candle error: {e}")
    return df


def compute_1h_trend(h1_df):
    """Computes 1H HTF Trend (UP / DOWN / SIDEWAYS) using EMA20 + 1H swing structure."""
    df = drop_incomplete_candle(h1_df, 60)
    if df.empty or len(df) < 8:
        return {"trend_1h": "SIDEWAYS", "ema20_1h": 0, "last_close_1h": 0}
    ema20 = round(float(df["c"].ewm(span=20, adjust=False).mean().iloc[-1]), 1)
    last_c = int(df.iloc[-1]["c"])
    prev_c = int(df.iloc[-3]["c"])
    if last_c > ema20 + 10 and last_c >= prev_c - 15:
        t1h = "UP"
    elif last_c < ema20 - 10 and last_c <= prev_c + 15:
        t1h = "DOWN"
    else:
        t1h = "SIDEWAYS"
    return {"trend_1h": t1h, "ema20_1h": ema20, "last_close_1h": last_c}


def compute_15m_trend(m15_df, h1_df=None):
    """Computes 15M Trend Structure (HH_HL = UPTREND, LH_LL = DOWNTREND, else SIDEWAYS) + 1H alignment."""
    h1_info = compute_1h_trend(h1_df) if h1_df is not None else {"trend_1h": "SIDEWAYS", "ema20_1h": 0}
    df = drop_incomplete_candle(m15_df, 15)
    if df.empty or len(df) < 6:
        return {
            "trend": "SIDEWAYS", "structure": "UNCLEAR",
            "trend_1h": h1_info["trend_1h"], "ema20_1h": h1_info["ema20_1h"],
            "last_swing_high": 0, "last_swing_low": 0, "ema9": 0, "ema21": 0,
        }

    ema9_val = round(float(df["c"].ewm(span=9, adjust=False).mean().iloc[-1]), 1)
    ema21_val = round(float(df["c"].ewm(span=21, adjust=False).mean().iloc[-1]), 1)

    recent = df.tail(20).reset_index(drop=True)
    swing_highs, swing_lows = [], []
    for i in range(1, len(recent) - 1):
        if recent.loc[i, "h"] >= recent.loc[i - 1, "h"] and recent.loc[i, "h"] >= recent.loc[i + 1, "h"]:
            swing_highs.append(int(recent.loc[i, "h"]))
        if recent.loc[i, "l"] <= recent.loc[i - 1, "l"] and recent.loc[i, "l"] <= recent.loc[i + 1, "l"]:
            swing_lows.append(int(recent.loc[i, "l"]))

    last_close = int(recent.iloc[-1]["c"])
    prev_close = int(recent.iloc[-4]["c"]) if len(recent) >= 4 else int(recent.iloc[0]["c"])
    net_move = last_close - prev_close

    hh = len(swing_highs) >= 2 and swing_highs[-1] > swing_highs[-2]
    hl = len(swing_lows) >= 2 and swing_lows[-1] > swing_lows[-2]
    lh = len(swing_highs) >= 2 and swing_highs[-1] < swing_highs[-2]
    ll = len(swing_lows) >= 2 and swing_lows[-1] < swing_lows[-2]

    ema_bull = (last_close > ema9_val > ema21_val) and (ema9_val - ema21_val >= 8)
    ema_bear = (last_close < ema9_val < ema21_val) and (ema21_val - ema9_val >= 8)

    if (hh and hl) or (hl and net_move > 30) or (ema_bull and net_move > 25) or net_move > 60:
        trend, structure = "UPTREND", "HH_HL"
    elif (lh and ll) or (lh and net_move < -30) or (ema_bear and net_move < -25) or net_move < -60:
        trend, structure = "DOWNTREND", "LH_LL"
    else:
        trend, structure = "SIDEWAYS", "SIDEWAYS"

    return {
        "trend": trend,
        "structure": structure,
        "trend_1h": h1_info["trend_1h"],
        "ema20_1h": h1_info["ema20_1h"],
        "last_swing_high": swing_highs[-1] if swing_highs else int(recent["h"].max()),
        "last_swing_low": swing_lows[-1] if swing_lows else int(recent["l"].min()),
        "net_move_1hr": net_move,
        "ema9": ema9_val,
        "ema21": ema21_val,
    }


def is_setup_trend_side(action, trend_15m):
    if trend_15m == "UPTREND":
        return "TREND_SIDE" if action == "BUY_CE" else "COUNTER_TREND"
    if trend_15m == "DOWNTREND":
        return "TREND_SIDE" if action == "BUY_PE" else "COUNTER_TREND"
    return "NEUTRAL_SIDE"


# ═══════════════════════════════════════════════════════════════════════
# SECTION 4: ZONE EXTRACTOR (S2_WEEKLY, S2_PD_2ND, S2_ORIGIN60, 6M MEMORY)
# ═══════════════════════════════════════════════════════════════════════

def _overlap_ratio(a_low, a_high, b_low, b_high):
    ol = max(0, min(a_high, b_high) - max(a_low, b_low))
    return ol / max(min(a_high - a_low, b_high - b_low), 1)


def extract_auto_math_zones(tf_data, ltp):
    """
    Extracts Tuned S2 & Structural Liquidity Zones within ±600 pts of LTP:
      1. S2_WEEKLY   : Previous Week High (PWH) & Previous Week Low (PWL)
      2. S2_PD_2ND   : Previous Trading Day High (PDH) & Low (PDL) — 2nd-Hunt rule enforced!
      3. S2_ORIGIN60 : 15M Institutional Origin Zones where >= 60 pt move originated
    """
    zones = []
    daily = drop_incomplete_candle(tf_data.get("daily", pd.DataFrame()), 375)
    m15   = drop_incomplete_candle(tf_data.get("m15", pd.DataFrame()), 15)

    if not daily.empty:
        d_df = daily.copy()
        d_df["ts_ist"] = pd.to_datetime(d_df["ts"])
        if d_df["ts_ist"].dt.tz is None:
            d_df["ts_ist"] = d_df["ts_ist"].dt.tz_localize(IST)
        else:
            d_df["ts_ist"] = d_df["ts_ist"].dt.tz_convert(IST)

        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        completed_days = d_df[d_df["ts_ist"].dt.strftime("%Y-%m-%d") < today_str].reset_index(drop=True)
        if completed_days.empty:
            completed_days = d_df.reset_index(drop=True)

        # 1. Previous Week High (PWH) & Previous Week Low (PWL) -> S2_WEEKLY
        iso_now = datetime.now(IST).isocalendar()
        cur_week = iso_now[0] * 100 + iso_now[1]
        completed_days["yw"] = [x.isocalendar()[0] * 100 + x.isocalendar()[1] for x in completed_days["ts_ist"]]
        prev_weeks = completed_days[completed_days["yw"] < cur_week]
        if not prev_weeks.empty:
            last_yw = prev_weeks["yw"].max()
            pw_slice = prev_weeks[prev_weeks["yw"] == last_yw]
            pwh = int(pw_slice["h"].max())
            pwl = int(pw_slice["l"].min())
            if abs(pwh - ltp) <= ZONE_SCAN_RADIUS_PTS:
                zones.append({
                    "id": "PWH", "type": "RESISTANCE", "low": pwh - 8, "high": pwh + 8,
                    "pivot": pwh, "strength": "STRONG",
                    "tags": ["WEEKLY_LEVEL", "S2_LEVEL", "S2_WEEKLY"],
                    "preferred_action": "BUY_PE",
                    "why": f"Previous Week High ({pwh}) major liquidity pool (S2_WEEKLY)",
                    "touch_count": 0, "blue_box_extreme": None, "source": "PYTHON_MATH"
                })
            if abs(pwl - ltp) <= ZONE_SCAN_RADIUS_PTS:
                zones.append({
                    "id": "PWL", "type": "SUPPORT", "low": pwl - 8, "high": pwl + 8,
                    "pivot": pwl, "strength": "STRONG",
                    "tags": ["WEEKLY_LEVEL", "S2_LEVEL", "S2_WEEKLY"],
                    "preferred_action": "BUY_CE",
                    "why": f"Previous Week Low ({pwl}) major liquidity pool (S2_WEEKLY)",
                    "touch_count": 0, "blue_box_extreme": None, "source": "PYTHON_MATH"
                })

        # 2. Previous Day High (PDH) & Low (PDL) -> S2_PD_2ND (1st touch = Blue Box, 2nd Hunt = Entry)
        pdc_row = completed_days.iloc[-1]
        pdh, pdl = int(pdc_row["h"]), int(pdc_row["l"])
        if abs(pdh - ltp) <= ZONE_SCAN_RADIUS_PTS:
            zones.append({
                "id": "PDH", "type": "RESISTANCE", "low": pdh - 8, "high": pdh + 8,
                "pivot": pdh, "strength": "STRONG",
                "tags": ["PREV_DAY_HIGH", "S2_LEVEL", "S2_PD_2ND"],
                "preferred_action": "BUY_PE",
                "why": f"Previous Day High ({pdh}) — 1st touch Blue Box, 2nd Hunt entry (S2_PD_2ND)",
                "touch_count": 0, "blue_box_extreme": None, "source": "PYTHON_MATH"
            })
        if abs(pdl - ltp) <= ZONE_SCAN_RADIUS_PTS:
            zones.append({
                "id": "PDL", "type": "SUPPORT", "low": pdl - 8, "high": pdl + 8,
                "pivot": pdl, "strength": "STRONG",
                "tags": ["PREV_DAY_LOW", "S2_LEVEL", "S2_PD_2ND"],
                "preferred_action": "BUY_CE",
                "why": f"Previous Day Low ({pdl}) — 1st touch Blue Box, 2nd Hunt entry (S2_PD_2ND)",
                "touch_count": 0, "blue_box_extreme": None, "source": "PYTHON_MATH"
            })

    # 3. Clean Institutional Origin Zones (>= 60 pt move in 3x15M bars) -> S2_ORIGIN60
    if not m15.empty and len(m15) >= 10:
        df15 = m15.tail(55).reset_index(drop=True)
        orig_idx = 1
        for i in range(1, len(df15) - 3):
            move_3 = int(df15.loc[i + 2, "c"]) - int(df15.loc[i, "o"])
            if move_3 <= -ORIGIN_MIN_MOVE_PTS:
                base_lvl = int(df15.loc[i, "o"])
                if abs(base_lvl - ltp) <= ZONE_SCAN_RADIUS_PTS:
                    zones.append({
                        "id": f"ORIG_PE{orig_idx}", "type": "RESISTANCE",
                        "low": base_lvl - 9, "high": base_lvl + 9, "pivot": base_lvl,
                        "strength": "STRONG",
                        "tags": ["CLEAN_ORIGIN", "S2_LEVEL", "S2_ORIGIN60"],
                        "preferred_action": "BUY_PE",
                        "why": f"Institutional Breakdown Origin ({base_lvl}) where -{abs(move_3)}pt drop started",
                        "touch_count": 0, "blue_box_extreme": None, "source": "PYTHON_MATH"
                    })
                    orig_idx += 1
            elif move_3 >= ORIGIN_MIN_MOVE_PTS:
                base_lvl = int(df15.loc[i, "o"])
                if abs(base_lvl - ltp) <= ZONE_SCAN_RADIUS_PTS:
                    zones.append({
                        "id": f"ORIG_CE{orig_idx}", "type": "SUPPORT",
                        "low": base_lvl - 9, "high": base_lvl + 9, "pivot": base_lvl,
                        "strength": "STRONG",
                        "tags": ["CLEAN_ORIGIN", "S2_LEVEL", "S2_ORIGIN60"],
                        "preferred_action": "BUY_CE",
                        "why": f"Institutional Rally Origin ({base_lvl}) where +{move_3}pt rally started",
                        "touch_count": 0, "blue_box_extreme": None, "source": "PYTHON_MATH"
                    })
                    orig_idx += 1

    return deduplicate_and_merge_zones(zones, [], ltp)


def fetch_selective_historical_memory_zones(state: NiftyState, ltp: float):
    """6-Month Selective Institutional Memory Zones (±50 pts Window, >=120pt reversal)."""
    cached = state.get("hist_memory_bank")
    if cached and abs(cached.get("center_ltp", 0) - ltp) <= 25:
        return cached.get("zones", [])

    daily_6m = drop_incomplete_candle(fetch_historical("days", 1, 180, days_override=185), 375)
    selected_zones = []
    low_bound = ltp - PRICE_RANGE_WINDOW_PTS
    high_bound = ltp + PRICE_RANGE_WINDOW_PTS

    if not daily_6m.empty and len(daily_6m) >= 12:
        for i in range(5, len(daily_6m) - 3):
            row = daily_6m.iloc[i]
            d_str = pd.Timestamp(row["ts"]).strftime("%Y-%m-%d")
            h_val, l_val = int(row["h"]), int(row["l"])

            if low_bound <= l_val <= high_bound:
                window_min = int(daily_6m.iloc[i - 5:i + 4]["l"].min())
                if l_val == window_min:
                    next_bounce = int(daily_6m.iloc[i:min(len(daily_6m), i + 4)]["h"].max()) - l_val
                    if next_bounce >= HIST_MIN_DISPLACEMENT:
                        selected_zones.append({
                            "id": f"HIST_SUP_{l_val}", "type": "SUPPORT",
                            "low": l_val - 7, "high": l_val + 7, "pivot": l_val,
                            "strength": "STRONG",
                            "tags": ["HISTORICAL_MEMORY_ZONE", "S2_LEVEL", "S2_ORIGIN60"],
                            "preferred_action": "BUY_CE",
                            "why": f"6M Memory Support ({d_str} @ {l_val}): +{next_bounce}pt reversal",
                            "touch_count": 0, "blue_box_extreme": None, "source": "HIST_6M_MEMORY"
                        })

            if low_bound <= h_val <= high_bound:
                window_max = int(daily_6m.iloc[i - 5:i + 4]["h"].max())
                if h_val == window_max:
                    next_drop = h_val - int(daily_6m.iloc[i:min(len(daily_6m), i + 4)]["l"].min())
                    if next_drop >= HIST_MIN_DISPLACEMENT:
                        selected_zones.append({
                            "id": f"HIST_RES_{h_val}", "type": "RESISTANCE",
                            "low": h_val - 7, "high": h_val + 7, "pivot": h_val,
                            "strength": "STRONG",
                            "tags": ["HISTORICAL_MEMORY_ZONE", "S2_LEVEL", "S2_ORIGIN60"],
                            "preferred_action": "BUY_PE",
                            "why": f"6M Memory Resistance ({d_str} @ {h_val}): -{next_drop}pt drop",
                            "touch_count": 0, "blue_box_extreme": None, "source": "HIST_6M_MEMORY"
                        })

    state.set("hist_memory_bank", {"center_ltp": ltp, "zones": selected_zones[:4]}, ttl=3600)
    return selected_zones[:4]


def update_live_intraday_zones(state: NiftyState, existing_zones, tf_data, ltp):
    """Runs on 5M boundaries (0 AI calls): updates 6M Memory zones + Live S3 Compression Box (12-32 pts)."""
    m5 = drop_incomplete_candle(tf_data.get("m5", pd.DataFrame()), 5)
    if m5.empty or len(m5) < 8:
        return existing_zones

    recent = m5.tail(20).reset_index(drop=True)
    new_live = fetch_selective_historical_memory_zones(state, ltp)

    # S3 Sideways Compression Box Detection (6 completed 5M candles within 12–32 pts range)
    if len(recent) >= SIDEWAYS_MIN_CANDLES + 1:
        box_slice = recent.iloc[-(SIDEWAYS_MIN_CANDLES + 1):-1]
        box_h, box_l = int(box_slice["h"].max()), int(box_slice["l"].min())
        box_rng = box_h - box_l
        if 12 <= box_rng <= SIDEWAYS_MAX_RANGE:
            state.set("active_s3_box", {
                "low": box_l, "high": box_h, "range": box_rng,
                "updated_at": datetime.now(IST).strftime("%H:%M")
            }, ttl=1800)

    if not new_live:
        return existing_zones

    merged = deduplicate_and_merge_zones(existing_zones, new_live, ltp)
    save_zones(state, merged)
    return merged


def normalize_zone_types(zones, ltp):
    out = []
    for z in zones:
        z = dict(z)
        try:
            z["low"], z["high"] = int(float(z.get("low", 0))), int(float(z.get("high", 0)))
        except (TypeError, ValueError):
            continue
        if z["high"] < z["low"]:
            continue
        if z["high"] == z["low"]:
            z["low"] -= 6
            z["high"] += 6
        z.setdefault("pivot", round((z["low"] + z["high"]) / 2))
        z.setdefault("touch_count", 0)
        z.setdefault("blue_box_extreme", None)
        z.setdefault("tags", [])
        if z.get("type") == "FLIP":
            z["type"] = "FLIP_SUPPORT" if ltp >= z["low"] else "FLIP_RESISTANCE"
        out.append(z)
    return out


def deduplicate_and_merge_zones(base_zones, extra_zones, ltp):
    combined = normalize_zone_types(base_zones, ltp) + normalize_zone_types(extra_zones, ltp)
    merged = []
    for cand in combined:
        if abs(cand["pivot"] - ltp) > ZONE_SCAN_RADIUS_PTS and "S2_WEEKLY" not in cand.get("tags", []):
            continue
        found_overlap = False
        for existing in merged:
            if _overlap_ratio(existing["low"], existing["high"], cand["low"], cand["high"]) >= 0.45 or abs(existing["pivot"] - cand["pivot"]) <= 12:
                found_overlap = True
                existing["touch_count"] = max(existing.get("touch_count", 0), cand.get("touch_count", 0))
                if cand.get("blue_box_extreme") and not existing.get("blue_box_extreme"):
                    existing["blue_box_extreme"] = cand["blue_box_extreme"]
                existing["tags"] = list(set(existing.get("tags", []) + cand.get("tags", [])))
                if cand.get("strength") == "STRONG":
                    existing["strength"] = "STRONG"
                break
        if not found_overlap:
            merged.append(cand)

    merged.sort(key=lambda z: abs(z["pivot"] - ltp))
    return merged[:MAX_ZONES]


# ═══════════════════════════════════════════════════════════════════════
# SECTION 5: FINAL 6 SETUPS ENGINE (ZERO-LAG 5M CLOSE, INDEX ONLY)
# ═══════════════════════════════════════════════════════════════════════

def zone_buffer(zone):
    return max(8, round((zone.get("high", 0) - zone.get("low", 0)) * 0.20))


def get_touched_zone(ltp, zones, extra_proximity=0):
    tradable, no_trade = [], []
    for z in zones:
        low, high = z.get("low", 0), z.get("high", 0)
        buf = zone_buffer(z) + extra_proximity
        if not (low - buf <= ltp <= high + buf):
            continue
        dist = abs(z.get("pivot", (low + high) / 2) - ltp)
        (no_trade if z.get("type") in ("NO_TRADE", "SIDEWAYS") else tradable).append((dist, z))
    pool = tradable or no_trade
    return min(pool, key=lambda x: x[0])[1] if pool else None


def compute_entry_mode(ltp: float, sweep_extreme: float, sweep_candle_close: float):
    """
    Replaces old `MAX_INDEX_CHASE_PTS = 38` hard blocker:
      - If distance from sweep extreme <= 28 pts -> `MARKET_NOW`
      - If distance > 28 pts (big displacement sweep candle) -> `PULLBACK_50PCT` with exact 50% midpoint!
    """
    dist = round(abs(ltp - sweep_extreme), 1)
    midpoint = round((sweep_extreme + sweep_candle_close) / 2.0, 1)
    if dist <= MARKET_ENTRY_MAX_DIST:
        return "MARKET_NOW", dist, midpoint
    return "PULLBACK_50PCT", dist, midpoint


def detect_synthetic_or_single_sweep(df, level_low, level_high, side, pivot=None):
    """Strict inequality sweep + reclaim/rejection detection on completed candles."""
    if df is None or df.empty or len(df) < 3:
        return False, 0, 0, None, "no data"

    c1 = df.iloc[-1]
    c2 = df.iloc[-2]
    o1, h1, l1, cl1 = int(c1["o"]), int(c1["h"]), int(c1["l"]), int(c1["c"])
    o2, h2, l2, cl2 = int(c2["o"]), int(c2["h"]), int(c2["l"]), int(c2["c"])

    rng1 = max(h1 - l1, 1)
    dn_wick1 = min(o1, cl1) - l1
    up_wick1 = h1 - max(o1, cl1)

    sup_ref = int(pivot) if pivot is not None else int(level_low)
    res_ref = int(pivot) if pivot is not None else int(level_high)

    if side == "BUY_CE":
        if l1 < sup_ref - 1 and cl1 > sup_ref and (cl1 > o1 or dn_wick1 >= rng1 * REJECTION_WICK_RATIO):
            return True, l1, cl1, "SINGLE_CANDLE_SWEEP", f"Wick swept {l1} below {sup_ref} & closed {cl1}"
        low_2c = min(l1, l2)
        if low_2c < sup_ref - 1 and cl1 > o1 and cl1 > sup_ref and (cl1 - o1) >= 4:
            return True, low_2c, cl1, "MARUBOZU_2C_SYNTHETIC_SWEEP", f"2-Candle Synthetic Sweep {low_2c} below {sup_ref} & Green close {cl1}"

    elif side == "BUY_PE":
        if h1 > res_ref + 1 and cl1 < res_ref and (cl1 < o1 or up_wick1 >= rng1 * REJECTION_WICK_RATIO):
            return True, h1, cl1, "SINGLE_CANDLE_SWEEP", f"Wick swept {h1} above {res_ref} & closed {cl1}"
        high_2c = max(h1, h2)
        if high_2c > res_ref + 1 and cl1 < o1 and cl1 < res_ref and (o1 - cl1) >= 4:
            return True, high_2c, cl1, "MARUBOZU_2C_SYNTHETIC_SWEEP", f"2-Candle Synthetic Sweep {high_2c} above {res_ref} & Red close {cl1}"

    return False, 0, 0, None, "no strict level break yet"


def evaluate_zone_setup(state: NiftyState, tf_data, ltp, zone, trend_info):
    """
    Evaluates S2 Setups at a Touched Structural Zone (Zero-Lag 5M Close):
      - S2_WEEKLY   : Previous Week High/Low (PWH/PWL) — 1st clean sweep triggers immediately.
      - S2_PD_2ND   : Previous Day High/Low (PDH/PDL) — 1st touch records Blue Box & WAITS;
                      2nd sweep beyond 1st-touch extreme triggers immediately!
      - S2_ORIGIN60 : Institutional Origin (>=60pt displacement) — clean sweep triggers immediately.
    """
    m5 = drop_incomplete_candle(tf_data.get("m5", pd.DataFrame()), 5)
    z_id = zone.get("id", "?")
    z_low, z_high = zone.get("low", 0), zone.get("high", 0)
    z_pivot = zone.get("pivot", round((z_low + z_high) / 2))
    z_type = zone.get("type", "")
    tags = zone.get("tags", [])
    touch_count = zone.get("touch_count", 0)
    blue_extreme = zone.get("blue_box_extreme")

    if z_type in ("SUPPORT", "FLIP_SUPPORT"):
        action = "BUY_CE"
    elif z_type in ("RESISTANCE", "FLIP_RESISTANCE"):
        action = "BUY_PE"
    else:
        action = zone.get("preferred_action", "WAIT")
    if action not in ("BUY_CE", "BUY_PE"):
        return {"status": "NO_SETUP", "reason": "zone has no directional action"}

    is_weekly = "S2_WEEKLY" in tags or z_id in ("PWH", "PWL")
    is_pd_2nd = "S2_PD_2ND" in tags or z_id in ("PDH", "PDL")
    is_origin = "S2_ORIGIN60" in tags or z_id.startswith("ORIG_") or z_id.startswith("HIST_")

    if not (is_weekly or is_pd_2nd or is_origin):
        return {"status": "NO_SETUP", "reason": "non-S2 zone skipped (random S1 disabled)"}

    swept_5m, sweep_ext, sweep_close, pat_name, sweep_detail = detect_synthetic_or_single_sweep(
        m5, z_low, z_high, action, pivot=z_pivot
    )
    if not swept_5m:
        return {"status": "NO_SETUP", "reason": "5M candle has not strictly swept & reclaimed zone"}

    trend_15m = trend_info.get("trend", "SIDEWAYS")
    hunt_side = is_setup_trend_side(action, trend_15m)
    entry_mode, chase_dist, pullback_mid = compute_entry_mode(ltp, sweep_ext, sweep_close)

    # ── RULE FOR PDH / PDL (S2_PD_2ND): 1st Touch = Blue Box, 2nd Hunt = Entry ──
    if is_pd_2nd:
        if touch_count == 0:
            update_single_zone_state(state, z_id, touch_count=1, blue_box_extreme=sweep_ext)
            mark_zone_cooldown(state, z_id, "BLUE_BOX_CREATED", custom_mins=10)
            send_blue_box_alert(zone, ltp, sweep_ext, action, hunt_side)
            return {"status": "BLUE_BOX_RECORDED", "reason": f"PDH/PDL 1st touch Blue Box recorded at {sweep_ext}; waiting for 2nd Hunt"}

        deeper_or_equal = (
            (sweep_ext <= blue_extreme + 4) if (action == "BUY_CE" and blue_extreme)
            else (sweep_ext >= blue_extreme - 4) if (action == "BUY_PE" and blue_extreme)
            else True
        )
        if not deeper_or_equal:
            return {"status": "NO_SETUP", "reason": f"PDH/PDL 2nd touch ({sweep_ext}) did not sweep 1st-touch extreme ({blue_extreme})"}

        update_single_zone_state(state, z_id, touch_count=touch_count + 1, blue_box_extreme=sweep_ext)
        return {
            "status": "TRIGGERED",
            "setup_code": "S2_PD_2ND",
            "setup_name": "S2_PD_2ND (Prev Day High/Low 2nd-Hunt Sweep)",
            "action": action,
            "hunt_side": hunt_side,
            "trigger_tf": "5M",
            "pattern": pat_name,
            "sweep_extreme": sweep_ext,
            "entry_mode": entry_mode,
            "pullback_midpoint": pullback_mid,
            "chase_distance_pts": chase_dist,
            "sweep_detail": f"2nd Hunt past Blue Box ({blue_extreme} → {sweep_ext}) | {sweep_detail}",
        }

    # ── RULE FOR S2_WEEKLY & S2_ORIGIN60: Immediate Trigger on Clean 5M Sweep ──
    setup_code = "S2_WEEKLY" if is_weekly else "S2_ORIGIN60"
    setup_name = (
        "S2_WEEKLY (Previous Week High/Low Liquidity Sweep)"
        if is_weekly else "S2_ORIGIN60 (Institutional >=60pt Origin Liquidity Sweep)"
    )
    update_single_zone_state(state, z_id, touch_count=touch_count + 1, blue_box_extreme=sweep_ext)
    return {
        "status": "TRIGGERED",
        "setup_code": setup_code,
        "setup_name": setup_name,
        "action": action,
        "hunt_side": hunt_side,
        "trigger_tf": "5M",
        "pattern": pat_name,
        "sweep_extreme": sweep_ext,
        "entry_mode": entry_mode,
        "pullback_midpoint": pullback_mid,
        "chase_distance_pts": chase_dist,
        "sweep_detail": sweep_detail,
    }


def evaluate_trend_liq_hunt(state: NiftyState, tf_data, ltp, trend_info):
    """
    Setup #1: `S1_TREND_LIQ` (Trend-Side Fresh Liquidity Pool Hunt):
      - Requires 15M Trend AND 1H Trend aligned (`UPTREND` + `UP`, or `DOWNTREND` + `DOWN`).
      - Detects a fresh intraday 5M swing low / EQL (in Uptrend) or swing high / EQH (in Downtrend)
        that bounced/dropped >= 8 pts (accumulating retail trailing SLs).
      - When a 5M candle strictly sweeps beyond that fresh liquidity pool and closes back inside
        in the trend direction -> IMMEDIATE ENTRY ALERT on 1st hunt!
    """
    t15 = trend_info.get("trend", "SIDEWAYS")
    t1h = trend_info.get("trend_1h", "SIDEWAYS")
    if not ((t15 == "UPTREND" and t1h == "UP") or (t15 == "DOWNTREND" and t1h == "DOWN")):
        return None
    if not zone_cooldown_ok(state, "S1_TREND_LIQ"):
        return None

    m5 = drop_incomplete_candle(tf_data.get("m5", pd.DataFrame()), 5)
    if m5.empty or len(m5) < 8:
        return None

    df = m5.tail(32).copy()
    df["ist_date"] = df["ts"].dt.tz_convert(IST).dt.strftime("%Y-%m-%d")
    today_str = df["ist_date"].iloc[-1]
    today_df = df[df["ist_date"] == today_str].reset_index(drop=True)
    if len(today_df) < 6:
        return None

    c1 = today_df.iloc[-1]
    o1, h1, l1, cl1 = int(c1["o"]), int(c1["h"]), int(c1["l"]), int(c1["c"])

    if t15 == "UPTREND" and t1h == "UP" and cl1 > o1:
        for idx in range(len(today_df) - 3, 0, -1):
            lvl = int(today_df.loc[idx, "l"])
            if lvl <= int(today_df.loc[idx - 1, "l"]) and lvl <= int(today_df.loc[idx + 1, "l"]):
                between = today_df.iloc[idx + 1:len(today_df) - 1]
                if between.empty or int(between["l"].min()) < lvl:
                    continue
                bounce = int(between["h"].max()) - lvl
                if bounce >= TREND_LIQ_MIN_BOUNCE and l1 < lvl - 1 and cl1 > lvl:
                    entry_mode, chase_dist, pullback_mid = compute_entry_mode(ltp, l1, cl1)
                    return {
                        "status": "TRIGGERED",
                        "setup_code": "S1_TREND_LIQ",
                        "setup_name": "S1_TREND_LIQ (15M+1H Trend-Side Fresh Liquidity Hunt)",
                        "action": "BUY_CE",
                        "hunt_side": "TREND_SIDE",
                        "trigger_tf": "5M",
                        "pattern": "TREND_SSL_SWEEP_RECLAIM",
                        "sweep_extreme": l1,
                        "entry_mode": entry_mode,
                        "pullback_midpoint": pullback_mid,
                        "chase_distance_pts": chase_dist,
                        "sweep_detail": f"Uptrend (15M+1H) fresh SSL {lvl} (bounced +{bounce}pts) swept to {l1} & reclaimed {cl1}",
                        "zone_obj": {"id": "S1_TREND_LIQ", "type": "TREND_LIQ_POOL", "low": l1, "high": lvl, "pivot": lvl, "strength": "STRONG"},
                    }

    if t15 == "DOWNTREND" and t1h == "DOWN" and cl1 < o1:
        for idx in range(len(today_df) - 3, 0, -1):
            lvl = int(today_df.loc[idx, "h"])
            if lvl >= int(today_df.loc[idx - 1, "h"]) and lvl >= int(today_df.loc[idx + 1, "h"]):
                between = today_df.iloc[idx + 1:len(today_df) - 1]
                if between.empty or int(between["h"].max()) > lvl:
                    continue
                drop = lvl - int(between["l"].min())
                if drop >= TREND_LIQ_MIN_BOUNCE and h1 > lvl + 1 and cl1 < lvl:
                    entry_mode, chase_dist, pullback_mid = compute_entry_mode(ltp, h1, cl1)
                    return {
                        "status": "TRIGGERED",
                        "setup_code": "S1_TREND_LIQ",
                        "setup_name": "S1_TREND_LIQ (15M+1H Trend-Side Fresh Liquidity Hunt)",
                        "action": "BUY_PE",
                        "hunt_side": "TREND_SIDE",
                        "trigger_tf": "5M",
                        "pattern": "TREND_BSL_SWEEP_REJECT",
                        "sweep_extreme": h1,
                        "entry_mode": entry_mode,
                        "pullback_midpoint": pullback_mid,
                        "chase_distance_pts": chase_dist,
                        "sweep_detail": f"Downtrend (15M+1H) fresh BSL {lvl} (dropped -{drop}pts) swept to {h1} & rejected {cl1}",
                        "zone_obj": {"id": "S1_TREND_LIQ", "type": "TREND_LIQ_POOL", "low": lvl, "high": h1, "pivot": lvl, "strength": "STRONG"},
                    }

    return None


def evaluate_trend_pullback_hunt(state: NiftyState, tf_data, ltp, trend_info):
    """
    Setup #2: `PB_MTF` (Multi-TF EMA21 Trend Pullback Hunt):
      - Requires 15M Trend AND 1H Trend aligned (`UPTREND` + `UP`, or `DOWNTREND` + `DOWN`).
      - Price pullback must be within <= 45 pts of 15M EMA21 (no chasing overextended trends).
      - 5M candle strictly sweeps previous 2-bar low/high and closes back in trend direction.
    """
    t15 = trend_info.get("trend", "SIDEWAYS")
    t1h = trend_info.get("trend_1h", "SIDEWAYS")
    ema21 = float(trend_info.get("ema21", 0))
    if not ((t15 == "UPTREND" and t1h == "UP") or (t15 == "DOWNTREND" and t1h == "DOWN")):
        return None
    if not zone_cooldown_ok(state, "PB_MTF"):
        return None

    m5 = drop_incomplete_candle(tf_data.get("m5", pd.DataFrame()), 5)
    if m5.empty or len(m5) < 5:
        return None

    c1, c2, c3 = m5.iloc[-1], m5.iloc[-2], m5.iloc[-3]
    o1, h1, l1, cl1 = int(c1["o"]), int(c1["h"]), int(c1["l"]), int(c1["c"])
    prev2_low = min(int(c2["l"]), int(c3["l"]))
    prev2_high = max(int(c2["h"]), int(c3["h"]))

    if t15 == "UPTREND" and t1h == "UP":
        if l1 < prev2_low - 1 and cl1 > o1 and cl1 > prev2_low:
            if ema21 > 0 and abs(l1 - ema21) > PB_MAX_EMA21_DIST_PTS:
                return None
            entry_mode, chase_dist, pullback_mid = compute_entry_mode(ltp, l1, cl1)
            return {
                "status": "TRIGGERED",
                "setup_code": "PB_MTF",
                "setup_name": "PB_MTF (15M+1H Trend Pullback Sweep near EMA21)",
                "action": "BUY_CE",
                "hunt_side": "TREND_SIDE",
                "trigger_tf": "5M",
                "pattern": "MTF_PULLBACK_LOW_SWEEP",
                "sweep_extreme": l1,
                "entry_mode": entry_mode,
                "pullback_midpoint": pullback_mid,
                "chase_distance_pts": chase_dist,
                "sweep_detail": f"15M+1H Uptrend pullback near EMA21 ({ema21}) swept 2-bar low ({prev2_low} → {l1}) & closed Green {cl1}",
                "zone_obj": {"id": "PB_MTF", "type": "PULLBACK_SWEEP", "low": l1, "high": cl1, "pivot": prev2_low, "strength": "STRONG"},
            }

    if t15 == "DOWNTREND" and t1h == "DOWN":
        if h1 > prev2_high + 1 and cl1 < o1 and cl1 < prev2_high:
            if ema21 > 0 and abs(h1 - ema21) > PB_MAX_EMA21_DIST_PTS:
                return None
            entry_mode, chase_dist, pullback_mid = compute_entry_mode(ltp, h1, cl1)
            return {
                "status": "TRIGGERED",
                "setup_code": "PB_MTF",
                "setup_name": "PB_MTF (15M+1H Trend Pullback Sweep near EMA21)",
                "action": "BUY_PE",
                "hunt_side": "TREND_SIDE",
                "trigger_tf": "5M",
                "pattern": "MTF_PULLBACK_HIGH_SWEEP",
                "sweep_extreme": h1,
                "entry_mode": entry_mode,
                "pullback_midpoint": pullback_mid,
                "chase_distance_pts": chase_dist,
                "sweep_detail": f"15M+1H Downtrend pullback near EMA21 ({ema21}) swept 2-bar high ({prev2_high} → {h1}) & closed Red {cl1}",
                "zone_obj": {"id": "PB_MTF", "type": "PULLBACK_SWEEP", "low": cl1, "high": h1, "pivot": prev2_high, "strength": "STRONG"},
            }

    return None


def evaluate_s3_sideways_setup(state: NiftyState, tf_data, ltp, trend_info):
    """
    Setup #6: `S3_FAKEOUT` (Sideways Compression Box Fakeout Reclaim — `S3_MICRO_ABS` removed!):
      - Tight 12–32 pt sideways box formed by 5+ completed 5M candles.
      - Fake breakout above box high -> closes Red back inside box (`BUY_PE`), or
        fake breakdown below box low -> closes Green back inside box (`BUY_CE`).
    """
    s3_box = state.get("active_s3_box")
    if not s3_box or not zone_cooldown_ok(state, "S3_FAKEOUT"):
        return None

    m5 = drop_incomplete_candle(tf_data.get("m5", pd.DataFrame()), 5)
    if m5.empty or len(m5) < 6:
        return None

    box_l, box_h = s3_box["low"], s3_box["high"]
    trend_15m = trend_info.get("trend", "SIDEWAYS")

    swept_dn, ext_dn, cl_dn, _, det_dn = detect_synthetic_or_single_sweep(m5, box_l, box_h, "BUY_CE", pivot=box_l)
    if swept_dn and trend_15m != "DOWNTREND":
        entry_mode, chase_dist, pullback_mid = compute_entry_mode(ltp, ext_dn, cl_dn)
        return {
            "status": "TRIGGERED",
            "setup_code": "S3_FAKEOUT",
            "setup_name": "S3_FAKEOUT (Sideways Box Fake Breakdown Reclaim)",
            "action": "BUY_CE",
            "hunt_side": is_setup_trend_side("BUY_CE", trend_15m),
            "trigger_tf": "5M",
            "pattern": "S3_FAKEOUT_RECLAIM",
            "sweep_extreme": ext_dn,
            "entry_mode": entry_mode,
            "pullback_midpoint": pullback_mid,
            "chase_distance_pts": chase_dist,
            "sweep_detail": f"Sideways Box ({box_l}–{box_h}) fake breakdown reclaimed: {det_dn}",
            "zone_obj": {"id": "S3_FAKEOUT", "type": "SIDEWAYS_BOX", "low": box_l, "high": box_h, "pivot": box_l, "strength": "STRONG"},
        }

    swept_up, ext_up, cl_up, _, det_up = detect_synthetic_or_single_sweep(m5, box_l, box_h, "BUY_PE", pivot=box_h)
    if swept_up and trend_15m != "UPTREND":
        entry_mode, chase_dist, pullback_mid = compute_entry_mode(ltp, ext_up, cl_up)
        return {
            "status": "TRIGGERED",
            "setup_code": "S3_FAKEOUT",
            "setup_name": "S3_FAKEOUT (Sideways Box Fake Breakout Reject)",
            "action": "BUY_PE",
            "hunt_side": is_setup_trend_side("BUY_PE", trend_15m),
            "trigger_tf": "5M",
            "pattern": "S3_FAKEOUT_REJECT",
            "sweep_extreme": ext_up,
            "entry_mode": entry_mode,
            "pullback_midpoint": pullback_mid,
            "chase_distance_pts": chase_dist,
            "sweep_detail": f"Sideways Box ({box_l}–{box_h}) fake breakout rejected: {det_up}",
            "zone_obj": {"id": "S3_FAKEOUT", "type": "SIDEWAYS_BOX", "low": box_l, "high": box_h, "pivot": box_h, "strength": "STRONG"},
        }

    return None


def detect_dominance_candle(df, ltp, lookback=DOMINANCE_LOOKBACK):
    if df is None or df.empty or len(df) < 3:
        return None
    d = df.tail(lookback).copy()
    d["rng"] = d["h"] - d["l"]
    row = d.loc[d["rng"].idxmax()]
    rng = int(row["rng"])
    if rng < DOMINANCE_MIN_RANGE:
        return None
    o, c, h, l = int(row["o"]), int(row["c"]), int(row["h"]), int(row["l"])
    mid = round((h + l) / 2)
    side = "SELLER" if c < o else "BUYER"
    return {"low": l, "high": h, "mid": mid, "side": side, "rng": rng,
            "reclaimed": (ltp > mid) if side == "SELLER" else (ltp < mid)}


def get_zone_history(state: NiftyState, zone, threshold=0.5):
    zl, zh = zone.get("low", 0), zone.get("high", 0)
    if zh <= zl:
        return []
    out = []
    for e in get_signal_history(state):
        if e.get("signal") not in ("BUY_CE", "BUY_PE"):
            continue
        el, eh = e.get("zone_low"), e.get("zone_high")
        if el is None or eh is None:
            continue
        if _overlap_ratio(zl, zh, el, eh) >= threshold:
            out.append(e)
    return out


# ═══════════════════════════════════════════════════════════════════════
# SECTION 6: DUAL-CHAIN AI ROUTER + BACKGROUND HYDROGEN THREAD
# ═══════════════════════════════════════════════════════════════════════

def shared_base(*dfs):
    lows = [int(d["l"].min()) for d in dfs if d is not None and not d.empty]
    if not lows:
        return 24000
    return math.floor(min(lows) / 500) * 500


def compress_ohlc(df, base, max_candles=None):
    if df is None or df.empty:
        return "N/A"
    d = df.tail(max_candles) if max_candles else df
    return "\n".join(f"{int(r['o'])-base} {int(r['h'])-base} {int(r['l'])-base} {int(r['c'])-base}"
                     for _, r in d.iterrows())


def compress_hl(df, base, max_candles=None):
    if df is None or df.empty:
        return "N/A"
    d = df.tail(max_candles) if max_candles else df
    return "\n".join(f"{int(r['h'])-base} {int(r['l'])-base}" for _, r in d.iterrows())


FIRST_ANALYSIS_SYSTEM = """You are the NIFTY 50 Top-Down Liquidity & Zone Architect (1D -> 1H -> 30M -> 15M -> 5M).
The user manually manages SL & Target; your sole job is accurate Index liquidity zone & HTF bias identification.

RULES:
- Identify major CALL-side (Support / Demand Origin / PWL / PDL) and PUT-side (Resistance / Supply Origin / PWH / PDH) liquidity pools.
- Determine 1D + 1H + 15M macro bias without predicting gap opens.
- All candle numbers are delta-encoded with ONE shared BASE. Convert back to REAL Index prices (value + BASE) in JSON.
Respond ONLY with valid JSON."""

ZONE_DECISION_SYSTEM = """You are a NIFTY 50 Index Liquidity Setup Supervisor.
The user manually sets SL and Target; your ONLY job is verifying whether the Index candle data proves a valid liquidity sweep and reclaim/rejection.
Default verdict is WAIT unless the raw candles confirm a clean setup.

VERIFICATION RULES:
1. Inspect the delta-encoded 1H, 15M, 5M, and 1M Index candles (delta + BASE).
2. Direction Lock: Output ONLY the exact proposed direction (`BUY_CE` or `BUY_PE`) or `WAIT`. Never flip direction.
3. Setup Rules:
   - S1_TREND_LIQ: 15M + 1H aligned trend-side fresh swing/pool swept and reclaimed on 5M.
   - PB_MTF: 15M + 1H aligned pullback near 15M EMA21 swept prev 2-bar high/low and closed in trend direction.
   - S2_WEEKLY: Previous Week High/Low (PWH/PWL) swept and reclaimed on 5M.
   - S2_PD_2ND: Previous Day High/Low (PDH/PDL) 2nd-hunt sweep past 1st-touch Blue Box extreme.
   - S2_ORIGIN60: Institutional origin (>=60pt move) swept and reclaimed on 5M.
   - S3_FAKEOUT: Tight sideways box (12-32pt) fakeout swept and closed back inside box.
4. Confirmations: Cite 2 to 4 concrete observations with exact REAL Index prices (delta + BASE).

Respond ONLY with valid JSON."""


def build_top_down_prompt_string(tf_data, auto_zones, trend_15m):
    daily = drop_incomplete_candle(tf_data.get("daily", pd.DataFrame()), 375)
    h1    = drop_incomplete_candle(tf_data.get("h1", pd.DataFrame()), 60)
    m30   = drop_incomplete_candle(tf_data.get("m30", pd.DataFrame()), 30)
    m15   = drop_incomplete_candle(tf_data.get("m15", pd.DataFrame()), 15)
    m5    = drop_incomplete_candle(tf_data.get("m5", pd.DataFrame()), 5)
    if daily.empty:
        return None

    base = shared_base(daily, h1, m30, m15, m5)
    pdc_row = daily.iloc[-1]
    pdh, pdl, pdc = int(pdc_row["h"]), int(pdc_row["l"]), int(pdc_row["c"])
    ltp_approx = int(m5.iloc[-1]["c"]) if not m5.empty else 0

    auto_str = "\n".join(
        f"  {z['id']}: {z['type']} {z['low']}-{z['high']} ({z['preferred_action']}) — {z['why']}"
        for z in auto_zones[:10]
    )

    return f"""=== NIFTY 50 INDEX TOP-DOWN LIQUIDITY ANALYSIS | {datetime.now(IST).strftime('%d-%m-%Y %H:%M')} ===
ALL candle blocks use ONE base: BASE={base}. Real price = value + {base}.

[KEY INDEX LEVELS — REAL PRICES]
PDH:{pdh} | PDL:{pdl} | PDC:{pdc} | LTP:{ltp_approx}
1H Trend: {trend_15m.get('trend_1h')} (EMA20:{trend_15m.get('ema20_1h')})
15M Trend: {trend_15m.get('trend')} ({trend_15m.get('structure')}) | EMA9:{trend_15m.get('ema9')} EMA21:{trend_15m.get('ema21')}

[PYTHON AUTO-DETECTED ZONES (Verify & Refine)]
{auto_str}

[1D - 14 candles] (H L)
{compress_hl(daily, base, 14)}

[1H - 22 candles] (O H L C)
{compress_ohlc(h1, base, 22)}

[30M - 24 candles] (O H L C)
{compress_ohlc(m30, base, 24)}

[15M - 32 candles] (O H L C)
{compress_ohlc(m15, base, 32)}

[5M - 45 candles] (O H L C)
{compress_ohlc(m5, base, 45)}"""


def build_supervisor_prompt_string(state: NiftyState, tf_data, ltp, zone, all_zones, setup_payload, trend_info):
    h1  = drop_incomplete_candle(tf_data.get("h1", pd.DataFrame()), 60)
    m15 = drop_incomplete_candle(tf_data.get("m15", pd.DataFrame()), 15)
    m5  = drop_incomplete_candle(tf_data.get("m5", pd.DataFrame()), 5)
    m1  = drop_incomplete_candle(tf_data.get("m1", pd.DataFrame()), 1, buffer_sec=4)

    base = shared_base(h1, m15, m5, m1)
    dominance = detect_dominance_candle(m5, ltp)
    shift = get_structure_shift(state)
    history = get_zone_history(state, zone)

    other_zones = [
        f"  {z['id']}: {z['type']} {z['low']}-{z['high']} [{z.get('strength','?')}]"
        for z in all_zones if z.get("id") != zone.get("id")
    ]
    other_str = "\n".join(other_zones[:6]) if other_zones else "None"

    extra_ctx = ""
    if dominance:
        extra_ctx += (f"\n[DOMINANCE CANDLE] {dominance['low']}-{dominance['high']} ({dominance['rng']}pts) "
                      f"Side:{dominance['side']} 50%:{dominance['mid']} Reclaimed:{'YES' if dominance['reclaimed'] else 'NO'}")
    if shift:
        extra_ctx += f"\n[STRUCTURE SHIFT] {shift['count']} '{shift['direction']}' zone flips since {shift['since']}"
    if history:
        h_lines = "; ".join(f"{m.get('time','?')} {m.get('signal','?')}→{m.get('result','?')}" for m in history)
        extra_ctx += f"\n[ZONE HISTORY TODAY] {h_lines}"

    return f"""=== NIFTY 50 INDEX SETUP VERIFICATION | {datetime.now(IST).strftime('%H:%M:%S')} ===
ALL candle blocks below use ONE base: BASE={base}. Real price = value + {base}.

[CANDIDATE SETUP — INDEX ONLY]
Setup Code : {setup_payload.get('setup_code')} — {setup_payload.get('setup_name')}
Proposed   : {setup_payload.get('action')} (Output ONLY '{setup_payload.get('action')}' or 'WAIT')
Hunt Side  : {setup_payload.get('hunt_side')} | 1H Trend: {trend_info.get('trend_1h')} | 15M Trend: {trend_info.get('trend')} (EMA21:{trend_info.get('ema21')})
Sweep Info : {setup_payload.get('sweep_detail')}
Sweep Ext  : {setup_payload.get('sweep_extreme')} | Current LTP: {ltp} | Dist: {setup_payload.get('chase_distance_pts')} pts
Entry Mode : {setup_payload.get('entry_mode')} (50% Pullback Midpoint: {setup_payload.get('pullback_midpoint')})

[TOUCHED ZONE & NEARBY ZONES — REAL INDEX PRICES]
Touched    : {zone.get('id')} ({zone.get('type')}) {zone.get('low')}-{zone.get('high')} — {zone.get('why','')}
Other Zones:
{other_str}{extra_ctx}

[1H - 10 candles] (O H L C | oldest→newest)
{compress_ohlc(h1, base, 10)}

[15M - 16 candles] (O H L C | oldest→newest)
{compress_ohlc(m15, base, 16)}

[5M - 24 candles] (O H L C | oldest→newest)
{compress_ohlc(m5, base, 24)}

[1M - 20 candles] (O H L C | oldest→newest)
{compress_ohlc(m1, base, 20)}"""


def _parse_json_safe(raw):
    if not raw:
        return None
    try:
        clean = raw.strip().replace("```json", "").replace("```", "").strip()
        s, e = clean.find("{"), clean.rfind("}") + 1
        return json.loads(clean[s:e]) if s != -1 else None
    except Exception as ex:
        log.error(f"[NIFTY] JSON parse error: {ex}")
        return None


def _build_thinking_cfg(thinking_mode: str, alt_format: bool = False):
    if thinking_mode == "HIGH":
        return {"thinkingBudget": -1} if alt_format else {"thinkingLevel": "HIGH"}
    return {"thinkingLevel": "NONE"} if alt_format else {"thinkingBudget": 0}


def call_ai_chain(state: NiftyState, chain_spec: list, system_prompt: str, user_prompt: str,
                  max_tokens: int, claude_model: str = DECISION_CLAUDE_MODEL):
    """
    Executes the ordered Gemini Fallback Chain with exact thinking levels, strict per-model
    timeouts, and daily RPD quota enforcement, falling back to Claude if configured.
    """
    global _gemini_idx
    last_raw = None

    if GEMINI_API_KEYS:
        for step in chain_spec:
            model_name = step["model"]
            thinking_mode = step["thinking"]
            timeout_sec = step["timeout"]
            rpd_limit = step["rpd"]

            if state is not None and state.get_rpd_used(model_name) >= rpd_limit:
                log.info(f"[NIFTY] ⏭️ Skipping {model_name} (daily RPD cap {rpd_limit} reached)")
                continue

            api_key = GEMINI_API_KEYS[_gemini_idx % len(GEMINI_API_KEYS)]
            _gemini_idx += 1
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"

            for alt_fmt in (False, True):
                payload = {
                    "systemInstruction": {"parts": [{"text": system_prompt}]},
                    "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                    "generationConfig": {
                        "temperature": 0.0,
                        "maxOutputTokens": max_tokens,
                        "responseMimeType": "application/json",
                        "thinkingConfig": _build_thinking_cfg(thinking_mode, alt_format=alt_fmt),
                    },
                }
                try:
                    t0 = time.time()
                    r = requests.post(url, json=payload, timeout=timeout_sec)
                    elapsed = round(time.time() - t0, 1)
                    if r.status_code == 200:
                        if state is not None:
                            state.inc_rpd_used(model_name)
                        data = r.json()
                        candidates = data.get("candidates", [])
                        if candidates:
                            raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                            last_raw = raw_text
                            parsed = _parse_json_safe(raw_text)
                            if parsed:
                                tag = f"{model_name} [think={thinking_mode}, {elapsed}s]"
                                log.info(f"[NIFTY] ✅ AI OK: {tag}")
                                return parsed, raw_text, tag
                    elif r.status_code == 400 and not alt_fmt and "thinking" in r.text.lower():
                        log.warning(f"[NIFTY] {model_name} HTTP 400 on thinkingConfig, retrying alt format...")
                        continue
                    else:
                        log.warning(f"[NIFTY] {model_name} failed (HTTP {r.status_code}, {elapsed}s) -> jumping next model")
                        break
                except requests.Timeout:
                    log.warning(f"[NIFTY] ⏱️ {model_name} timed out after {timeout_sec}s -> jumping next model")
                    break
                except Exception as e:
                    log.warning(f"[NIFTY] {model_name} error ({e}) -> jumping next model")
                    break

    if ANTHROPIC_KEY:
        log.info(f"[NIFTY] Switching to Backup Claude ({claude_model})...")
        hdrs = {
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        body = {
            "model": claude_model,
            "max_tokens": max_tokens,
            "temperature": 0,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        try:
            r = requests.post("https://api.anthropic.com/v1/messages", headers=hdrs, json=body, timeout=30)
            if r.status_code == 200:
                raw_text = r.json()["content"][0]["text"]
                last_raw = raw_text
                parsed = _parse_json_safe(raw_text)
                if parsed:
                    return parsed, raw_text, f"CLAUDE ({claude_model})"
        except Exception as e:
            log.error(f"[NIFTY] Claude error: {e}")

    return None, last_raw, "AI_UNAVAILABLE"


def run_top_down_analysis(state: NiftyState, tf_data, auto_zones, trend_15m, mode="MORNING"):
    """Uses Heavy Chain: gemini-4-hydrogen (HIGH) -> 3.8-flash (HIGH, 15s) -> 3.6-flash (OFF) -> 3.5-flash-lite (HIGH)."""
    log.info(f"[NIFTY] 🌅 Running {mode} Top-Down Analysis (Heavy Chain)...")
    prompt_str = build_top_down_prompt_string(tf_data, auto_zones, trend_15m)
    if not prompt_str:
        return None

    user_prompt = prompt_str + """

RETURN ONLY this JSON (use REAL Index prices = delta + BASE):
{"bias":"UPTREND/DOWNTREND/SIDEWAYS","structure":"HH_HL/LH_LL/SIDEWAYS","day_type":"TRENDING/RANGING/VOLATILE","if_then_plan":"1 line IF-THEN plan for CALL and PUT sides","zones":[{"id":"Z1","type":"SUPPORT/RESISTANCE/FLIP","low":0,"high":0,"strength":"STRONG/MED","tags":["S2_WEEKLY/S2_PD_2ND/S2_ORIGIN60"],"preferred_action":"BUY_CE/BUY_PE","why":"short reason"}]}"""

    result, raw, provider = call_ai_chain(
        state, HEAVY_AI_CHAIN, FIRST_ANALYSIS_SYSTEM, user_prompt,
        max_tokens=FIRST_ANALYSIS_MAX_TOKENS, claude_model=ANALYSIS_CLAUDE_MODEL
    )
    save_ai_raw_log(state, {
        "time": datetime.now(IST).strftime("%H:%M"), "mode": mode,
        "provider": provider, "raw": raw[:400] if raw else None, "parsed": bool(result)
    })
    return result


def run_ai_supervisor(state: NiftyState, tf_data, ltp, zone, all_zones, setup_payload, trend_info):
    """Uses Fast 5M Chain: gemini-3.5-flash-lite (OFF) -> gemini-3.1-flash-lite (OFF) for instant alert."""
    prompt_str = build_supervisor_prompt_string(state, tf_data, ltp, zone, all_zones, setup_payload, trend_info)
    user_prompt = prompt_str + """

RETURN ONLY this JSON:
{"signal":"BUY_CE/BUY_PE/WAIT","confidence":"HIGH/MED/LOW","setup_tag":"S1_TREND_LIQ/PB_MTF/S2_WEEKLY/S2_PD_2ND/S2_ORIGIN60/S3_FAKEOUT","confirmations":["concrete fact 1 citing exact real price","concrete fact 2 citing exact real price"],"ai_view":"1 honest line on why this setup works or why to wait","message":"1 short Marathi/Hinglish line for Telegram"}"""

    result, raw, provider = call_ai_chain(
        state, NORMAL_5M_AI_CHAIN, ZONE_DECISION_SYSTEM, user_prompt,
        max_tokens=ZONE_DECISION_MAX_TOKENS, claude_model=DECISION_CLAUDE_MODEL
    )
    save_ai_raw_log(state, {
        "time": datetime.now(IST).strftime("%H:%M:%S"),
        "mode": "SUPERVISOR_FAST",
        "provider": provider,
        "setup": setup_payload.get("setup_code"),
        "zone_id": zone.get("id"),
        "ltp": ltp,
        "raw": raw[:400] if raw else None,
        "parsed": bool(result),
    })
    return result, provider, user_prompt


def run_hydrogen_background_check(shared_store, user_prompt: str, setup_payload: dict, zone: dict, ltp: float):
    """
    Daemon background worker: runs Heavy Chain (`gemini-4-hydrogen` HIGH -> `3.8-flash` -> `3.6-flash`
    -> `3.5-flash-lite` HIGH) while Lite alert is already delivered, then posts a follow-up confirmation!
    """
    try:
        state = NiftyState(shared_store)
        res, raw, provider = call_ai_chain(
            state, HEAVY_AI_CHAIN, ZONE_DECISION_SYSTEM, user_prompt,
            max_tokens=ZONE_DECISION_MAX_TOKENS, claude_model=ANALYSIS_CLAUDE_MODEL
        )
        save_ai_raw_log(state, {
            "time": datetime.now(IST).strftime("%H:%M:%S"),
            "mode": "HYDROGEN_BG_CONFIRM",
            "provider": provider,
            "setup": setup_payload.get("setup_code"),
            "zone_id": zone.get("id"),
            "ltp": ltp,
            "raw": raw[:400] if raw else None,
            "parsed": bool(res),
        })
        if not res:
            return
        verdict = res.get("signal", "WAIT")
        conf = res.get("confidence", "LOW")
        ai_view = res.get("ai_view", "")
        expected = setup_payload.get("action")
        badge = "✅ DEEP CONFIRMED" if (verdict == expected and conf in ("HIGH", "MED")) else "⚠️ DEEP CAUTION / WAIT"

        tg_send(f"""🧠 <b>[HYDROGEN DEEP CHECK] {esc(badge)} | {datetime.now(IST).strftime('%H:%M:%S')}</b>

<b>Setup   :</b> {esc(setup_payload.get('setup_code'))} ({esc(expected)}) @ {ltp}
<b>Model   :</b> {esc(provider)}
<b>Verdict :</b> {esc(verdict)} ({esc(conf)})
<b>View    :</b> {esc(ai_view)}""")
    except Exception as e:
        log.error(f"[NIFTY] Background Hydrogen thread error: {e}")


# ═══════════════════════════════════════════════════════════════════════
# SECTION 7: VALIDATION (NO SL/RISK CAP) & TELEGRAM WAIT ALERTS
# ═══════════════════════════════════════════════════════════════════════

FILLER = {"none", "na", "n/a", "null", "", "looks good", "strong setup", "good entry", "bullish", "bearish",
          "price looking good", "momentum", "trend is good", "confirmed", "valid setup"}


def _real_confirmations(confs):
    real = [
        str(c).strip() for c in (confs or [])
        if len(str(c).strip()) >= 12 and str(c).strip().lower() not in FILLER
    ]
    has_num = any(any(ch.isdigit() for ch in s) for s in real)
    return real, has_num


def validate_supervisor_decision(ai_res, expected_action):
    """Validates AI direction & price-citing confirmations (no SL/Target or risk cap checks)."""
    if not ai_res:
        log.warning("[NIFTY] Rejected: AI Supervisor unavailable")
        return False, "AI_UNAVAILABLE"

    sig = ai_res.get("signal", "WAIT")
    conf = ai_res.get("confidence", "LOW")

    if sig == "WAIT":
        return False, "AI_WAIT"

    if sig != expected_action:
        log.warning(f"[NIFTY] Rejected: AI direction ({sig}) != Python direction ({expected_action})")
        return False, f"DIRECTION_MISMATCH ({sig} != {expected_action})"

    if conf not in ("HIGH", "MED"):
        return False, f"LOW_CONFIDENCE ({conf})"

    real_confs, has_num = _real_confirmations(ai_res.get("confirmations", []))
    if len(real_confs) < MIN_CONFIRMATIONS or not has_num:
        return False, "WEAK_CONFIRMATIONS"

    return True, "APPROVED"


def process_triggered_setup(shared_store, state: NiftyState, tf_data, ltp, zone, all_zones, trend_info, setup_payload):
    """No `idx_risk > 45` filter, no SL/Target calc. Sends WAIT reasons to Telegram + spawns Hydrogen thread."""
    zone_id = zone.get("id", "?")
    action = setup_payload["action"]
    hunt_side = setup_payload["hunt_side"]
    setup_code = setup_payload["setup_code"]

    ai_res, provider, supervisor_prompt = run_ai_supervisor(
        state, tf_data, ltp, zone, all_zones, setup_payload, trend_info
    )
    is_valid, reason = validate_supervisor_decision(ai_res, action)

    if not is_valid:
        ai_view = (ai_res or {}).get("ai_view", "No AI view returned")
        log.info(f"[NIFTY] ⏭️ Filtered {zone_id} ({setup_code}): {reason} | {ai_view}")
        save_signal_log(state, {
            "time": datetime.now(IST).strftime("%H:%M"),
            "zone_id": zone_id,
            "zone_low": zone.get("low"),
            "zone_high": zone.get("high"),
            "setup_code": setup_code,
            "hunt_side": hunt_side,
            "signal": "WAIT" if reason == "AI_WAIT" else "REJECTED",
            "confidence": (ai_res or {}).get("confidence", "NONE"),
            "reason": reason,
            "ai_view": ai_view,
            "result": "FILTERED",
        })
        mark_zone_cooldown(state, zone_id, "WAIT", custom_mins=WAIT_COOLDOWN_MIN)
        send_filtered_setup_alert(zone, setup_payload, ltp, reason, ai_view, provider)
        return

    cd_mins = PULLBACK_COOLDOWN_MIN if setup_code in ("PB_MTF", "S1_TREND_LIQ") else SIGNAL_COOLDOWN_MIN
    mark_zone_cooldown(state, zone_id, action, custom_mins=cd_mins)

    save_signal_log(state, {
        "time": datetime.now(IST).strftime("%H:%M"),
        "zone_id": zone_id,
        "zone_low": zone.get("low"),
        "zone_high": zone.get("high"),
        "setup_code": setup_code,
        "setup_name": setup_payload.get("setup_name"),
        "hunt_side": hunt_side,
        "signal": action,
        "confidence": ai_res.get("confidence"),
        "provider": provider,
        "ltp": ltp,
        "sweep_extreme": setup_payload.get("sweep_extreme"),
        "entry_mode": setup_payload.get("entry_mode"),
        "pullback_midpoint": setup_payload.get("pullback_midpoint"),
        "result": "TRIGGERED",
    })

    # 1. Immediate Fast Alert via Lite Chain
    send_trade_signal_alert(ai_res, provider, ltp, zone, setup_payload, trend_info)

    # 2. Background Deep Confirmation via `gemini-4-hydrogen` (HIGH)
    bg_thread = threading.Thread(
        target=run_hydrogen_background_check,
        args=(shared_store, supervisor_prompt, dict(setup_payload), dict(zone), ltp),
        daemon=True,
    )
    bg_thread.start()


# ═══════════════════════════════════════════════════════════════════════
# SECTION 8: TELEGRAM ALERTS (INDEX-ONLY + AI WAIT REASONS + EOD LOG)
# ═══════════════════════════════════════════════════════════════════════

def tg_send(text):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info(f"[NIFTY-TG-DRY] {text[:140]}...")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if r.status_code != 200:
            log.error(f"[NIFTY] Telegram send failed {r.status_code}: {r.text[:250]}")
    except Exception as e:
        log.error(f"[NIFTY] Telegram error: {e}")


def send_zone_brief(ctx, zones, trend_15m):
    today = datetime.now(IST).strftime("%d %b %Y %H:%M")
    call_zones = [z for z in zones if z.get("preferred_action") == "BUY_CE"]
    put_zones  = [z for z in zones if z.get("preferred_action") == "BUY_PE"]

    call_str = "\n".join(
        f"  🟢 <b>{esc(z['id'])}</b>: {z['low']}–{z['high']} — {esc(z.get('why',''))}"
        for z in call_zones[:6]
    ) or "  None within ±600 pts"

    put_str = "\n".join(
        f"  🔴 <b>{esc(z['id'])}</b>: {z['low']}–{z['high']} — {esc(z.get('why',''))}"
        for z in put_zones[:6]
    ) or "  None within ±600 pts"

    tg_send(f"""⚡ <b>[NIFTY 50] V8 INDEX LIQUIDITY ZONES | {today}</b>

<b>1H Trend :</b> {esc(trend_15m.get('trend_1h','?'))} (EMA20:{trend_15m.get('ema20_1h')})
<b>15M Trend:</b> {esc(trend_15m.get('trend','?'))} ({esc(trend_15m.get('structure','?'))} | EMA9:{trend_15m.get('ema9')} / EMA21:{trend_15m.get('ema21')})
<b>Day Type :</b> {esc(ctx.get('day_type','?'))}
<b>IF-THEN  :</b> {esc(ctx.get('if_then_plan','React on PWH/PWL, PDH/PDL 2nd Hunt & Trend sweeps'))}

<b>🔴 PUT SIDE ZONES (Sweep + Reject = BUY PE):</b>
{put_str}

<b>🟢 CALL SIDE ZONES (Sweep + Reclaim = BUY CE):</b>
{call_str}""")


def send_blue_box_alert(zone, ltp, sweep_ext, action, hunt_side):
    side_lbl = "CALL (CE)" if action == "BUY_CE" else "PUT (PE)"
    tg_send(f"""🔵 <b>[NIFTY 50] 1ST TOUCH BLUE BOX (PDH/PDL) | {datetime.now(IST).strftime('%H:%M')}</b>

<b>Zone      :</b> {esc(zone.get('id'))} ({zone.get('low')}–{zone.get('high')})
<b>Watch Side:</b> {esc(hunt_side)} ({side_lbl})
<b>1st Sweep :</b> {sweep_ext} (LTP: {ltp})

⚠️ <b>S2_PD_2ND Rule:</b> Ha 1st touch (Blue Box) ahe. Parat <b>2nd Hunt</b> past {sweep_ext} zalyavar entry alert yeil.""")


def send_filtered_setup_alert(zone, setup_payload, ltp, reason, ai_view, provider):
    """Sends transparent AI WAIT / Filter reasons directly to Telegram."""
    tg_send(f"""⏸️ <b>[NIFTY 50] SETUP FILTERED / AI WAIT | {datetime.now(IST).strftime('%H:%M:%S')}</b>

<b>Setup   :</b> {esc(setup_payload.get('setup_code'))} ({esc(setup_payload.get('action'))})
<b>Zone    :</b> {esc(zone.get('id'))} @ LTP {ltp} (Sweep: {setup_payload.get('sweep_extreme')})
<b>Reason  :</b> <code>{esc(reason)}</code> (Cooldown: {WAIT_COOLDOWN_MIN}m)
<b>AI View :</b> {esc(ai_view)}
<b>Model   :</b> {esc(provider)}""")


def send_trade_signal_alert(ai_res, provider, ltp, zone, setup_payload, trend_info):
    """Clean Index-Only Trade Identification Alert (SL & Target managed manually by user)."""
    sig = ai_res.get("signal", "WAIT")
    emoji = "🟢" if sig == "BUY_CE" else "🔴"
    entry_mode = setup_payload.get("entry_mode", "MARKET_NOW")
    mid_pt = setup_payload.get("pullback_midpoint", ltp)
    entry_hint = (
        f"⚡ <b>MARKET_NOW</b> (Dist from sweep: {setup_payload.get('chase_distance_pts')} pts)"
        if entry_mode == "MARKET_NOW"
        else f"🎯 <b>PULLBACK_50PCT Limit Zone: ~{mid_pt}</b> (Big sweep: {setup_payload.get('chase_distance_pts')} pts)"
    )
    confs = "\n".join(f"  ✔ {esc(c)}" for c in ai_res.get("confirmations", [])[:4] if c)

    tg_send(f"""{emoji} <b>[NIFTY 50] {esc(sig)} — {esc(setup_payload.get('setup_code'))} | {datetime.now(IST).strftime('%H:%M:%S')}</b>

<b>Setup     :</b> {esc(setup_payload.get('setup_name'))}
<b>Trend     :</b> 1H: {esc(trend_info.get('trend_1h'))} | 15M: {esc(trend_info.get('trend'))} ({esc(setup_payload.get('hunt_side'))})
<b>Zone      :</b> {esc(zone.get('id'))} ({zone.get('low')}–{zone.get('high')}) | TF: 5M
<b>Index LTP :</b> <b>{ltp}</b> | <b>Sweep Ext:</b> <b>{setup_payload.get('sweep_extreme')}</b>
<b>Entry Mode:</b> {entry_hint}
<b>Fast AI   :</b> {esc(ai_res.get('confidence'))} ({esc(provider)})

<b>Why this Setup:</b>
  • {esc(setup_payload.get('sweep_detail'))}
{confs}

🧠 <b>AI View:</b> {esc(ai_res.get('ai_view',''))}
💬 {esc(ai_res.get('message',''))}
⏳ <i>Hydrogen (HIGH) deep check running in background...</i>""")
    log.info(f"[NIFTY] 🔥 Alert sent: {sig} ({setup_payload.get('setup_code')}) @ {ltp}")


def run_nifty_eod_summary(shared_store):
    """Clean EOD Journal Summary of Triggered & Filtered Setups (no SL/Target proxy outcomes)."""
    state = NiftyState(shared_store)
    all_hist = get_signal_history(state)
    trades = [s for s in all_hist if s.get("signal") in ("BUY_CE", "BUY_PE")]
    filtered = [s for s in all_hist if s.get("result") == "FILTERED"]
    today = datetime.now(IST).strftime("%d %b %Y")

    ce = [s for s in trades if s.get("signal") == "BUY_CE"]
    pe = [s for s in trades if s.get("signal") == "BUY_PE"]

    setup_stats = {}
    for s in trades:
        sc = s.get("setup_code", "OTHER")
        setup_stats[sc] = setup_stats.get(sc, 0) + 1

    setup_lines = "\n".join(f"  • <b>{esc(k)}</b>: {v} alerts" for k, v in setup_stats.items()) or "  • None today"
    detail = "".join(
        f"\n• {s['time']} <b>{esc(s.get('setup_code',''))}</b> {esc(s['signal'])} @ {s.get('ltp')} "
        f"(Sweep:{s.get('sweep_extreme')} | {esc(s.get('entry_mode',''))})"
        for s in trades
    )

    tg_send(f"""📈 <b>[NIFTY 50] V8 EOD SETUP LOG | {today}</b>

<b>Triggered Alerts:</b> {len(trades)} (🟢 CE: {len(ce)} | 🔴 PE: {len(pe)})
<b>Filtered / AI WAIT:</b> {len(filtered)}

<b>Setup Breakdown:</b>
{setup_lines}
{detail}""")
    flush_day(state)


# ═══════════════════════════════════════════════════════════════════════
# SECTION 9: ORCHESTRATOR JOBS (CALLED BY main.py SCHEDULER)
# ═══════════════════════════════════════════════════════════════════════

def is_nifty_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS:
        return False
    from datetime import time as dtime
    return dtime(9, 15) <= now.time() <= dtime(15, 30)


def morning_job(shared_store):
    if not is_nifty_market_open() or not UPSTOX_TOKEN:
        return
    log.info("[NIFTY] 🌅 MORNING TOP-DOWN ANALYSIS JOB (09:20 IST)")
    with _nifty_lock:
        try:
            state = NiftyState(shared_store)
            ltp = get_ltp() or 0
            tf_data = fetch_top_down_data()
            trend_15m = compute_15m_trend(tf_data.get("m15", pd.DataFrame()), tf_data.get("h1", pd.DataFrame()))
            auto_zones = extract_auto_math_zones(tf_data, ltp)
            hist_zones = fetch_selective_historical_memory_zones(state, ltp)

            ai_res = run_top_down_analysis(state, tf_data, auto_zones + hist_zones, trend_15m, mode="MORNING")
            ai_zones = ai_res.get("zones", []) if ai_res else []
            merged_zones = deduplicate_and_merge_zones(auto_zones + hist_zones, ai_zones, ltp)

            ctx = {
                "bias": trend_15m.get("trend"),
                "structure": trend_15m.get("structure"),
                "day_type": (ai_res or {}).get("day_type", "UNCLEAR"),
                "if_then_plan": (ai_res or {}).get("if_then_plan", "React on PWH/PWL, PDH/PDL 2nd Hunt & Trend sweeps"),
            }
            save_morning_context(state, ctx)
            save_zones(state, merged_zones)
            send_zone_brief(ctx, merged_zones, trend_15m)
        except Exception as e:
            log.exception("[NIFTY] Morning job error")
            tg_send(f"⚠️ <b>[NIFTY] Morning job error:</b> {esc(str(e)[:100])}")


def hourly_job(shared_store):
    if not is_nifty_market_open() or not UPSTOX_TOKEN:
        return
    log.info("[NIFTY] 🔄 HOURLY TOP-DOWN SYNC (Heavy Chain)")
    with _nifty_lock:
        try:
            state = NiftyState(shared_store)
            ltp = get_ltp()
            if not ltp:
                return
            tf_data = fetch_top_down_data()
            trend_15m = compute_15m_trend(tf_data.get("m15", pd.DataFrame()), tf_data.get("h1", pd.DataFrame()))
            auto_zones = extract_auto_math_zones(tf_data, ltp)
            hist_zones = fetch_selective_historical_memory_zones(state, ltp)

            ai_res = run_top_down_analysis(state, tf_data, auto_zones + hist_zones, trend_15m, mode="HOURLY")
            ai_zones = ai_res.get("zones", []) if ai_res else []

            existing = get_zones(state)
            merged = deduplicate_and_merge_zones(existing + auto_zones + hist_zones, ai_zones, ltp)
            ctx = get_morning_context(state)
            ctx.update({
                "bias": trend_15m.get("trend"),
                "structure": trend_15m.get("structure"),
                "day_type": (ai_res or {}).get("day_type", ctx.get("day_type", "UNCLEAR")),
                "if_then_plan": (ai_res or {}).get("if_then_plan", ctx.get("if_then_plan", "")),
            })
            save_morning_context(state, ctx)
            save_zones(state, merged)
            tg_send(f"🔄 <b>[NIFTY 50] Hourly Zone Sync | {datetime.now(IST).strftime('%H:%M')}</b>\n"
                    f"1H: <b>{esc(trend_15m.get('trend_1h'))}</b> | 15M: <b>{esc(trend_15m.get('trend'))}</b> "
                    f"(EMA21:{trend_15m.get('ema21')}) | Active Zones: {len(merged)}")
        except Exception:
            log.exception("[NIFTY] Hourly job error")


def run_nifty_1m_tick(shared_store):
    """
    Zero-Lag 5-Minute Setup Scanner (called by `main.py` scheduler):
      - 09:20 AM: runs `morning_job` (Heavy Chain).
      - 10:02–14:02: runs `hourly_job` (Heavy Chain).
      - Every 5M boundary (:00, :05, :10...): evaluates all 6 final setups on completed 5M candles,
        fires instant Lite Chain alert + spawns background `gemini-4-hydrogen` confirmation thread!
      - 15:28 PM: runs `run_nifty_eod_summary`.
    """
    if not is_nifty_market_open() or not UPSTOX_TOKEN:
        return
    now = datetime.now(IST)
    if now.hour == 15 and now.minute >= 28:
        state = NiftyState(shared_store)
        if state.get("zones"):
            run_nifty_eod_summary(shared_store)
        return
    if now.hour == 9 and now.minute < 20:
        return
    if now.hour == 15 and now.minute > LAST_AI_CALL_MINUTE:
        return

    if not _nifty_lock.acquire(blocking=False):
        return
    try:
        state = NiftyState(shared_store)
        ltp = get_ltp()
        if not ltp:
            return

        if not is_today_data_valid(state):
            _nifty_lock.release()
            morning_job(shared_store)
            _nifty_lock.acquire()

        zones = get_zones(state)
        if not zones:
            return

        # Hourly sync trigger at 10:02, 11:02, 12:02, 13:02, 14:02
        if now.minute == 2 and now.hour in (10, 11, 12, 13, 14):
            last_hr = state.get("last_hourly_sync_hr")
            if last_hr != now.hour:
                state.set("last_hourly_sync_hr", now.hour, ttl=3600)
                _nifty_lock.release()
                hourly_job(shared_store)
                _nifty_lock.acquire()
                zones = get_zones(state)

        # Execute all 6 setups strictly on 5M boundaries (:00, :05, :10...)
        if now.minute % 5 != 0:
            return

        tf_data = fetch_live_monitor_data(include_1m=True)
        trend_info = compute_15m_trend(tf_data.get("m15", pd.DataFrame()), tf_data.get("h1", pd.DataFrame()))
        zones = update_live_intraday_zones(state, zones, tf_data, ltp)

        # 1. Evaluate S2 Structural Zones (S2_WEEKLY, S2_PD_2ND, S2_ORIGIN60)
        touched = get_touched_zone(ltp, zones)
        if touched and touched.get("type") not in ("NO_TRADE", "SIDEWAYS"):
            zid = touched.get("id", "?")
            if zone_cooldown_ok(state, zid):
                eval_res = evaluate_zone_setup(state, tf_data, ltp, touched, trend_info)
                if eval_res.get("status") == "TRIGGERED":
                    process_triggered_setup(shared_store, state, tf_data, ltp, touched, zones, trend_info, eval_res)
                    return

        # 2. Evaluate S1_TREND_LIQ (15M + 1H Trend-Side Fresh Liquidity Pool Hunt)
        tliq_res = evaluate_trend_liq_hunt(state, tf_data, ltp, trend_info)
        if tliq_res and tliq_res.get("status") == "TRIGGERED":
            process_triggered_setup(shared_store, state, tf_data, ltp, tliq_res["zone_obj"], zones, trend_info, tliq_res)
            return

        # 3. Evaluate PB_MTF (15M + 1H Aligned EMA21 Trend Pullback Sweep)
        pb_res = evaluate_trend_pullback_hunt(state, tf_data, ltp, trend_info)
        if pb_res and pb_res.get("status") == "TRIGGERED":
            process_triggered_setup(shared_store, state, tf_data, ltp, pb_res["zone_obj"], zones, trend_info, pb_res)
            return

        # 4. Evaluate S3_FAKEOUT (Tight Sideways Box 12–32pt Fakeout Reclaim)
        s3_res = evaluate_s3_sideways_setup(state, tf_data, ltp, trend_info)
        if s3_res and s3_res.get("status") == "TRIGGERED":
            process_triggered_setup(shared_store, state, tf_data, ltp, s3_res["zone_obj"], zones, trend_info, s3_res)
            return

    except Exception:
        log.exception("[NIFTY] run_nifty_1m_tick error")
    finally:
        try:
            _nifty_lock.release()
        except RuntimeError:
            pass
