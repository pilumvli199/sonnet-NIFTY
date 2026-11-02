"""
MASTER ORCHESTRATOR (main.py) — NIFTY 50 + BITCOIN + GOLD V8.1
Multi-Asset Cloud Launcher for Koyeb / Docker

Schedule (IST):
  NIFTY : Mon-Fri 09:20-15:30 (5-min scan) + hourly AI re-analysis
  BTC   : 24x7 (30-min closed-bar scan) + 6-hourly AI re-analysis
  GOLD  : 24x7 by default (GOLD_24X7=true). Set GOLD_24X7=false to use
          COMEX-style hours (DST-safe, computed in New York time).

Notes:
  - Bots run inside the scheduler's own worker threads. A per-job lock skips
    a new tick if the previous tick of the same job is still running.
  - All bots share ONE process and ONE store. NIFTY:* / BTC:* / GOLD:* key
    prefixes are a convention the bots must follow; this file does not enforce it.
"""

import os
import json
import time
import requests
import logging
import threading
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

IST = pytz.timezone("Asia/Kolkata")
NY = pytz.timezone("America/New_York")


class TaggedISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=pytz.utc).astimezone(IST)
        return dt.strftime(datefmt) if datefmt else dt.strftime("%Y-%m-%d %H:%M:%S")


_handler = logging.StreamHandler()
_handler.setFormatter(TaggedISTFormatter("%(asctime)s IST | [%(name)s] | %(levelname)s | %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[_handler])
log = logging.getLogger("MAIN")

logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

ENABLE_NIFTY = os.getenv("ENABLE_NIFTY", "true").lower() == "true"
ENABLE_BTC   = os.getenv("ENABLE_BTC",   "true").lower() == "true"
ENABLE_GOLD  = os.getenv("ENABLE_GOLD",  "true").lower() == "true"
GOLD_24X7    = os.getenv("GOLD_24X7",    "true").lower() == "true"
REDIS_URL    = os.getenv("REDIS_URL", "")
NSE_HOLIDAYS = {d.strip() for d in os.getenv("NSE_HOLIDAYS", "").split(",") if d.strip()}  # optional

# Job IDs that were actually added to the scheduler (used by heartbeat + /health)
REGISTERED = set()


# ═══════════════════════════════════════════════════════════════════════
# MARKET-HOUR GATES
# ═══════════════════════════════════════════════════════════════════════

def is_nifty_open(now=None) -> bool:
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS:
        return False
    return (9, 20) <= (now.hour, now.minute) <= (15, 30)


def is_gold_open(now=None) -> bool:
    if GOLD_24X7:
        return True
    # COMEX-style hours in New York time, so US DST is handled automatically:
    # Sun opens 18:00 ET, Mon-Thu break 17:00-18:00 ET, Fri closes 17:00 ET, Sat closed.
    now = (now or datetime.now(IST)).astimezone(NY)
    wd = now.weekday()
    t = now.hour * 60 + now.minute
    if wd == 5:
        return False
    if wd == 6:
        return t >= 18 * 60
    if wd == 4:
        return t < 17 * 60
    return not (17 * 60 <= t < 18 * 60)


def _gold_gate() -> bool:
    # Look 1 minute back so the last closed bar before a break/close is still processed
    return is_gold_open(datetime.now(IST) - timedelta(minutes=1))


def is_btc_open(now=None) -> bool:
    return True


# ═══════════════════════════════════════════════════════════════════════
# SHARED REDIS + BOUNDED RAM STORE
# ═══════════════════════════════════════════════════════════════════════

class SharedNamespacedStore:
    """
    Redis if REDIS_URL is set and reachable, otherwise a RAM dict capped at
    MAX_RAM_KEYS keys. The cap is on key COUNT; keep stored values small.
    """
    MAX_RAM_KEYS = 500

    def __init__(self, redis_url=""):
        self._ram = {}
        self._lock = threading.Lock()
        self._r = None
        self._redis_ok = False
        if redis_url:
            try:
                import redis
                r = redis.from_url(redis_url, decode_responses=True)
                r.ping()
                self._r = r
                self._redis_ok = True
                log.info("Connected to Redis successfully")
            except Exception as e:
                log.warning(f"Redis unavailable ({e}) — running in bounded RAM mode")
        else:
            log.info("Running in bounded RAM mode")

    def set(self, namespaced_key: str, value, ttl: int = 86400):
        raw = json.dumps(value)
        if self._r:
            try:
                self._r.setex(namespaced_key, ttl, raw)
                self._redis_ok = True
                return
            except Exception as e:
                self._redis_ok = False
                log.error(f"Redis set error ({namespaced_key}): {e}")
        with self._lock:
            if namespaced_key in self._ram:
                self._ram.pop(namespaced_key)  # refresh recency
            elif len(self._ram) >= self.MAX_RAM_KEYS:
                now_ts = time.time()
                for k in [k for k, (_, exp) in self._ram.items() if now_ts > exp]:
                    self._ram.pop(k, None)
                if len(self._ram) >= self.MAX_RAM_KEYS:
                    self._ram.pop(next(iter(self._ram)), None)
            self._ram[namespaced_key] = (raw, time.time() + ttl)

    def get(self, namespaced_key: str):
        if self._r:
            try:
                raw = self._r.get(namespaced_key)
                self._redis_ok = True
                if raw:
                    return json.loads(raw)
            except Exception as e:
                self._redis_ok = False
                log.error(f"Redis get error ({namespaced_key}): {e}")
        with self._lock:
            item = self._ram.get(namespaced_key)
            if not item:
                return None
            raw, exp = item
            if time.time() > exp:
                self._ram.pop(namespaced_key, None)
                return None
            return json.loads(raw)

    def delete(self, namespaced_key: str):
        if self._r:
            try:
                self._r.delete(namespaced_key)
            except Exception as e:
                log.error(f"Redis delete error ({namespaced_key}): {e}")
        with self._lock:
            self._ram.pop(namespaced_key, None)

    def stats(self):
        with self._lock:
            return {
                "storage_mode": "redis" if (self._r and self._redis_ok) else "bounded_ram",
                "ram_keys_count": len(self._ram),
                "ram_bytes_approx": sum(len(raw) for raw, _ in self._ram.values()),
            }


# ═══════════════════════════════════════════════════════════════════════
# FAULT-TOLERANT BOT IMPORTS
# ═══════════════════════════════════════════════════════════════════════

try:
    import nifty_bot
except Exception as e:
    nifty_bot = None
    log.error(f"Failed to import nifty_bot: {e}")

try:
    import btc_bot
except Exception as e:
    btc_bot = None
    log.error(f"Failed to import btc_bot: {e}")

try:
    import gold_bot
except Exception as e:
    gold_bot = None
    log.error(f"Failed to import gold_bot: {e}")


# ═══════════════════════════════════════════════════════════════════════
# JOB WRAPPER + REGISTRATION
# ═══════════════════════════════════════════════════════════════════════

_locks = {}


def _guarded(tag: str, fn, gate):
    """Skip outside market hours, skip if the previous run of this job is still going,
    and log failures under the bot's tag."""
    bot_log = logging.getLogger(tag)
    lock = _locks.setdefault(f"{tag}:{getattr(fn, '__name__', 'fn')}", threading.Lock())

    def runner(store):
        if not gate():
            return
        if not lock.acquire(blocking=False):
            bot_log.warning(f"[{tag}] previous run still active — skipping this tick")
            return
        try:
            fn(store)
        except Exception:
            bot_log.exception(f"[{tag}] Task execution failed")
        finally:
            lock.release()

    runner.__name__ = f"{tag.lower()}_guarded_runner"
    return runner


def _register(scheduler, store, *, enabled, module, func_name, tag, job_id, gate, label, **cron):
    if not enabled:
        log.info(f"[{tag}] disabled via .env — {job_id} not scheduled")
        return
    if module is None:
        log.error(f"[{tag}] module failed to import — {job_id} not scheduled")
        return
    fn = getattr(module, func_name, None)
    if not callable(fn):
        log.warning(f"[{tag}] Function '{func_name}' not found in {module.__name__}. {job_id} not scheduled.")
        return
    scheduler.add_job(_guarded(tag, fn, gate), "cron", args=[store], id=job_id, **cron)
    REGISTERED.add(job_id)
    log.info(f"Scheduled [{tag}] {label}")


# ═══════════════════════════════════════════════════════════════════════
# 5-MINUTE LIVE MULTI-ASSET RADAR MONITOR
# ═══════════════════════════════════════════════════════════════════════

def run_asset_radar_heartbeat(store):
    """Logs real-time market price fetching & status every 5 minutes."""
    now = datetime.now(IST)
    now_str = now.strftime("%H:%M:%S IST")
    h_log = logging.getLogger("HEARTBEAT")

    # 1. BTC
    btc_status = "Disabled"
    if ENABLE_BTC:
        try:
            r = requests.get("https://api.india.delta.exchange/v2/tickers/BTCUSD", timeout=8)
            if r.status_code == 200:
                data = r.json().get("result", {})
                close_p = float(data.get("close", 0.0))
                next_min = 30 if now.minute < 30 else 0
                next_h = now.hour if now.minute < 30 else (now.hour + 1) % 24
                btc_status = f"LIVE OK (${close_p:,.1f}) | 30M Bar Scanning (Next: {next_h:02d}:{next_min:02d}:15)"
            else:
                btc_status = f"Delta HTTP {r.status_code}"
        except Exception as e:
            btc_status = f"Fetch Error ({e})"
        if "btc_30m_engine" not in REGISTERED:
            btc_status = f"NOT SCHEDULED (check startup logs) | {btc_status}"

    # 2. Nifty
    nifty_status = "Disabled"
    if ENABLE_NIFTY:
        if "nifty_5m_engine" not in REGISTERED:
            nifty_status = "NOT SCHEDULED (check startup logs)"
        elif is_nifty_open(now):
            nifty_status = "ACTIVE (5-Min Scanner Running)"
        elif now.weekday() >= 5:
            nifty_status = "Closed (Weekend) -> Opens Monday 09:15 IST"
        elif (now.hour, now.minute) < (9, 20):
            nifty_status = "Pre-Market -> Opens Today 09:15 IST"
        else:
            nifty_status = "Market Closed -> Opens Next Trading Day 09:15 IST"

    # 3. Gold
    gold_status = "Disabled"
    if ENABLE_GOLD:
        if "gold_30m_engine" not in REGISTERED:
            gold_status = "NOT SCHEDULED (check startup logs)"
        elif is_gold_open(now):
            gold_status = "ACTIVE (30-Min Scanner Running" + (", 24x7)" if GOLD_24X7 else ")")
        else:
            gold_status = "Closed / Daily Break (COMEX hours)"

    stats = store.stats() if hasattr(store, "stats") else {}
    h_log.info(f"═════════ MULTI-ASSET RADAR ({now_str}) ═════════")
    h_log.info(f"BTCUSD   : {btc_status}")
    h_log.info(f"NIFTY 50 : {nifty_status}")
    h_log.info(f"GOLD     : {gold_status}")
    h_log.info(f"STATUS   : Storage: {stats.get('storage_mode', 'RAM')} ({stats.get('ram_keys_count', 0)} keys) | Server: Healthy")
    h_log.info("══════════════════════════════════════════════════════")


# ═══════════════════════════════════════════════════════════════════════
# SYSTEM HEALTH CHECK ENGINE
# ═══════════════════════════════════════════════════════════════════════

def run_health_check():
    log.info("[SYSTEM] Running Health Check & API Validations...")
    res = {"Delta": "FAIL", "Upstox": "Not Set", "Gemini": "FAIL", "Claude": "Not Set (Optional)"}

    # 1. Delta Exchange
    try:
        d_key = os.getenv("DELTA_API_KEY", "").strip()
        if "your_" in d_key.lower():
            d_key = ""
        d_headers = {"api-key": d_key} if d_key else {}
        r_delta = requests.get("https://api.india.delta.exchange/v2/tickers/BTCUSD", headers=d_headers, timeout=10)
        if r_delta.status_code == 200 and r_delta.json().get("success"):
            res["Delta"] = "OK"
        else:
            r_fb = requests.get("https://api.india.delta.exchange/v2/products/BTCUSD", headers=d_headers, timeout=10)
            if r_fb.status_code == 200:
                res["Delta"] = "OK"
            else:
                res["Delta"] = f"FAIL (HTTP {r_delta.status_code})"
    except Exception as e:
        log.warning(f"[HEALTH] Delta check failed: {e}")
        res["Delta"] = "FAIL (Connection Error)"

    # 2. Upstox
    try:
        u_token = os.getenv("UPSTOX_ANALYTICS_TOKEN", "").strip()
        if u_token and "your_" not in u_token.lower():
            hdrs = {"Authorization": f"Bearer {u_token}", "Accept": "application/json"}
            r_upstox = requests.get("https://api.upstox.com/v3/market-quote/ltp?instrument_key=NSE_INDEX|Nifty 50", headers=hdrs, timeout=10)
            if r_upstox.status_code == 200:
                res["Upstox"] = "OK"
            else:
                res["Upstox"] = f"FAIL (HTTP {r_upstox.status_code})"
    except Exception as e:
        log.warning(f"[HEALTH] Upstox check failed: {e}")
        res["Upstox"] = "FAIL (Connection Error)"

    # 3. Gemini (primary from GEMINI_MODEL, optional GEMINI_FALLBACK_MODEL; key sent in header, never in URL)
    try:
        raw_keys = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip()]
        g_keys = [k for k in raw_keys if "your_" not in k.lower()]
        models_to_try = [os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite").strip(),
                         os.getenv("GEMINI_FALLBACK_MODEL", "").strip()]
        models_to_try = [m for m in dict.fromkeys(models_to_try) if m]

        if not g_keys:
            res["Gemini"] = "FAIL (No Key Provided)"
        else:
            errs, codes = [], []
            for m in models_to_try:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent"
                payload = {"contents": [{"role": "user", "parts": [{"text": "Hi"}]}]}
                for attempt in range(2):
                    r_gem = requests.post(url, json=payload, headers={"x-goog-api-key": g_keys[0]}, timeout=12)
                    if r_gem.status_code != 503 or attempt == 1:
                        break
                    time.sleep(2)
                if r_gem.status_code == 200:
                    res["Gemini"] = f"OK ({m})"
                    break
                codes.append(r_gem.status_code)
                errs.append(f"{m}: {r_gem.status_code}")
                log.warning(f"[HEALTH] Gemini test with {m} failed: {r_gem.status_code} - {r_gem.text[:100]}")
            else:
                temp = all(c in (429, 503) for c in codes)
                res["Gemini"] = ("BUSY (temporary: " if temp else "FAIL (") + " | ".join(errs) + ")"
    except Exception as e:
        log.warning(f"[HEALTH] Gemini check exception: {e}")
        res["Gemini"] = "FAIL (Connection Error)"

    # 4. Claude (optional)
    try:
        a_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if a_key and "your_" not in a_key.lower():
            c_model = os.getenv("CLAUDE_BACKUP_MODEL", "claude-haiku-4-5-20251001").strip()
            hdrs = {"x-api-key": a_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}
            r_claude = requests.post("https://api.anthropic.com/v1/messages", headers=hdrs, json={"model": c_model, "max_tokens": 10, "messages": [{"role": "user", "content": "Hi"}]}, timeout=10)
            if r_claude.status_code == 200:
                res["Claude"] = "OK"
            else:
                res["Claude"] = f"FAIL (HTTP {r_claude.status_code})"
        else:
            res["Claude"] = "Not Set (Optional)"
    except Exception as e:
        log.warning(f"[HEALTH] Claude check failed: {e}")
        res["Claude"] = "FAIL (Connection Error)"

    has_fail = any(v.startswith("FAIL") for v in res.values())
    title = "SYSTEM HEALTH CHECK: ATTENTION" if has_fail else "SYSTEM HEALTH CHECK: OK"
    msg = (f"<b>[{title}]</b>\n\n"
           f"<b>Upstox Data :</b> {res['Upstox']}\n"
           f"<b>Delta Data  :</b> {res['Delta']}\n"
           f"<b>Gemini AI   :</b> {res['Gemini']}\n"
           f"<b>Claude AI   :</b> {res['Claude']}\n\n"
           f"<i>Multi-Asset Orchestrator active and running. {datetime.now(IST).strftime('%H:%M IST')}</i>")

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("BTC_TELEGRAM_CHAT_ID")
    if tg_token and tg_chat:
        try:
            requests.post(f"https://api.telegram.org/bot{tg_token}/sendMessage", json={"chat_id": tg_chat, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except Exception as e:
            log.error(f"[HEALTH] Telegram alert failed: {e}")


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    log.info("Starting Multi-Asset Liquidity Hunt Orchestrator V8.1...")

    store = SharedNamespacedStore(REDIS_URL)

    # Startup health check (runs once in background, does not block startup)
    threading.Thread(target=run_health_check, daemon=True).start()

    scheduler = BackgroundScheduler(timezone=IST, job_defaults={"max_instances": 3, "coalesce": True, "misfire_grace_time": 40})

    # 0. Daily Morning Health Check (09:15 AM IST)
    scheduler.add_job(run_health_check, "cron", hour=9, minute=15, id="daily_health_check")
    log.info("Scheduled [SYSTEM] Daily Health Check (09:15 AM)")

    # 1. 5-Minute Multi-Asset Live Radar Heartbeat
    scheduler.add_job(run_asset_radar_heartbeat, "cron", args=[store], minute="*/5", second=0, id="asset_radar_heartbeat")
    log.info("Scheduled [SYSTEM] 5-Minute Multi-Asset Radar Heartbeat")

    # 2. NIFTY 50: 5-Minute Setup Scan + 1-Hour Macro Reanalysis
    _register(scheduler, store, enabled=ENABLE_NIFTY, module=nifty_bot, func_name="run_nifty_1m_tick", tag="NIFTY", job_id="nifty_5m_engine", gate=is_nifty_open, label="5-Minute Setup Scan (Mon-Fri 09:20-15:30)", day_of_week="mon-fri", hour="9-15", minute="*/5", second=5)
    _register(scheduler, store, enabled=ENABLE_NIFTY, module=nifty_bot, func_name="run_1h_analysis", tag="NIFTY", job_id="nifty_1h_ai", gate=is_nifty_open, label="1-Hour Macro Reanalysis (10:15-15:15)", day_of_week="mon-fri", hour="10,11,12,13,14,15", minute=15, second=10)

    # 3. BITCOIN: 30-Minute Closed Bar Scan (24/7) + 6-Hour Macro Reanalysis
    _register(scheduler, store, enabled=ENABLE_BTC, module=btc_bot, func_name="run_btc_30m_tick", tag="BTC", job_id="btc_30m_engine", gate=is_btc_open, label="30-Minute Closed Bar Scan (24/7)", minute="0,30", second=15)
    _register(scheduler, store, enabled=ENABLE_BTC, module=btc_bot, func_name="run_6h_analysis", tag="BTC", job_id="btc_6h_ai", gate=is_btc_open, label="6-Hour Macro Re-analysis (00,06,12,18)", hour="0,6,12,18", minute=1, second=0)

    # 4. GOLD: 30-Minute Closed Bar Scan + 6-Hour Macro Reanalysis (24x7 unless GOLD_24X7=false)
    _register(scheduler, store, enabled=ENABLE_GOLD, module=gold_bot, func_name="run_gold_1m_tick", tag="GOLD", job_id="gold_30m_engine", gate=_gold_gate, label="30-Minute Closed Bar Scan", minute="0,30", second=35)
    _register(scheduler, store, enabled=ENABLE_GOLD, module=gold_bot, func_name="run_6h_analysis", tag="GOLD", job_id="gold_6h_ai", gate=_gold_gate, label="6-Hour Macro Re-analysis (00,06,12,18)", hour="0,6,12,18", minute=2, second=0)

    scheduler.start()

    port = int(os.getenv("PORT", 8000))
    app = Flask(__name__)

    @app.route("/")
    def index():
        return "Multi-Asset Liquidity Hunt V8.1 Running OK", 200

    @app.route("/health")
    def health():
        now = datetime.now(IST)
        return {
            "status": "ok",
            "time_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
            "scheduler_running": scheduler.running,
            "jobs_registered": sorted(REGISTERED),
            "market_open_now": {"nifty": is_nifty_open(now), "btc": True, "gold": is_gold_open(now)},
            "memory": store.stats(),
        }, 200

    log.info(f"Health server listening on port {port}")
    try:
        app.run(host="0.0.0.0", port=port, use_reloader=False)
    finally:
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
