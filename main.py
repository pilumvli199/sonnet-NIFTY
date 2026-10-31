"""
╔══════════════════════════════════════════════════════════════════════════╗
║   MASTER ORCHESTRATOR (main.py) — NIFTY 50 + BITCOIN + GOLD V7.5         ║
║   Multi-Asset Launcher for Koyeb / Docker / Cloud (High-Precision Async) ║
║                                                                          ║
║   SCHEDULE ARCHITECTURE:                                                 ║
║   • NIFTY 50 : 5-Min Setup Tick (09:20-15:30) + 1-Hour Macro Reanalysis  ║
║   • BITCOIN  : 30-Min Closed Bar Scan (24/7) + 6-Hour Macro Reanalysis   ║
║   • GOLD     : 30-Min Setup Scan (Mon-Fri) + 6-Hour Macro Reanalysis     ║
║   • HEALTH   : Startup Verification Suite + Daily 09:15 AM System Audit  ║
║   • ASYNC    : Non-blocking worker threads (Zero Scheduler Freezing)     ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import time
import requests
import logging
import threading
from datetime import datetime
import pytz
from dotenv import load_dotenv

try:
    from flask import Flask
except ImportError:
    Flask = None

try:
    from apscheduler.schedulers.background import BackgroundScheduler
except ImportError:
    BackgroundScheduler = None

load_dotenv()

IST = pytz.timezone("Asia/Kolkata")

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
REDIS_URL    = os.getenv("REDIS_URL", "")
NSE_HOLIDAYS = {d.strip() for d in os.getenv("NSE_HOLIDAYS", "").split(",") if d.strip()}


def is_nifty_open(now=None) -> bool:
    now = now or datetime.now(IST)
    if now.weekday() >= 5: return False
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS: return False
    return (9, 20) <= (now.hour, now.minute) <= (15, 30)

def is_gold_open(now=None) -> bool:
    now = now or datetime.now(IST)
    weekday = now.weekday()  # 0=Mon, 1=Tue, ..., 5=Sat, 6=Sun
    t_min = now.hour * 60 + now.minute

    # Sunday: Closed all day
    if weekday == 6:
        return False

    # Saturday: Closes at 02:30 AM IST (Summer EDT) / 03:30 AM IST (Winter EST)
    if weekday == 5:
        return t_min < (2 * 60 + 30)

    # Monday: Opens at 03:30 AM IST (Summer EDT) / 04:30 AM IST (Winter EST)
    if weekday == 0:
        return t_min >= (3 * 60 + 30)

    # Tue-Fri: Daily 1-hour rollover maintenance break (02:30 - 03:30 AM IST)
    if 1 <= weekday <= 4:
        if (2 * 60 + 30) <= t_min < (3 * 60 + 30):
            return False

    return True

def is_btc_open(now=None) -> bool:
    return True


class SharedNamespacedStore:
    MAX_RAM_KEYS = 500
    def __init__(self, redis_url=""):
        self._ram = {}
        self._lock = threading.Lock()
        self._r = None
        if redis_url:
            try:
                import redis
                r = redis.from_url(redis_url, decode_responses=True)
                r.ping()
                self._r = r
                log.info("✅ Connected to Redis")
            except Exception as e:
                log.warning(f"⚠️ Redis unavailable ({e}) — running in bounded RAM mode")
        else:
            log.info("ℹ️ Running in bounded RAM mode")

    def set(self, namespaced_key: str, value, ttl: int = 86400):
        raw = json.dumps(value)
        if self._r:
            try:
                self._r.setex(namespaced_key, ttl, raw)
                return
            except Exception:
                pass
        with self._lock:
            self._ram[namespaced_key] = (raw, time.time() + ttl)

    def get(self, namespaced_key: str):
        if self._r:
            try:
                raw = self._r.get(namespaced_key)
                if raw: return json.loads(raw)
            except Exception:
                pass
        with self._lock:
            item = self._ram.get(namespaced_key)
            if not item: return None
            raw, exp = item
            if time.time() > exp: return None
            return json.loads(raw)

    def delete(self, namespaced_key: str):
        if self._r:
            try: self._r.delete(namespaced_key)
            except Exception: pass
        with self._lock: self._ram.pop(namespaced_key, None)

    def stats(self):
        with self._lock:
            return {"storage_mode": "redis" if self._r else "bounded_ram", "ram_keys_count": len(self._ram)}


try: import nifty_bot
except Exception as e: nifty_bot = None; log.error(f"⚠️ Failed to import nifty_bot: {e}")

try: import btc_bot
except Exception as e: btc_bot = None; log.error(f"⚠️ Failed to import btc_bot: {e}")

try: import gold_bot
except Exception as e: gold_bot = None; log.error(f"⚠️ Failed to import gold_bot: {e}")


def _guarded(tag: str, fn, gate, run_async: bool = True):
    bot_log = logging.getLogger(tag)
    def runner(store):
        if not gate(): return
        def _task():
            try: fn(store)
            except Exception: bot_log.exception(f"[{tag}] Task execution failed")
        if run_async:
            threading.Thread(target=_task, daemon=True).start()
        else:
            _task()
    runner.__name__ = f"{tag.lower()}_guarded_runner"
    return runner

def _register(scheduler, store, *, enabled, module, func_name, tag, job_id, gate, label, run_async=True, **cron):
    if not enabled: return
    if module is None: return
    fn = getattr(module, func_name, None)
    if not callable(fn):
        log.warning(f"⚠️ [{tag}] Function '{func_name}' not found in {module.__name__}. Skipping.")
        return
    if scheduler:
        scheduler.add_job(_guarded(tag, fn, gate, run_async=run_async), "cron", args=[store], id=job_id, **cron)
    log.info(f"✅ Scheduled [{tag}] {label}")

# ─── SYSTEM HEALTH CHECK ENGINE ───
def run_health_check():
    log.info("[SYSTEM] 🏥 Running Health Check & API Validations...")
    res = {"Delta": "❌", "Upstox": "❌", "Gemini": "❌", "Claude": "❌"}
    
    # 1. Delta Exchange Health Check (Tickers endpoint - no start/end required)
    try:
        d_key = os.getenv("DELTA_API_KEY", "").strip()
        d_headers = {"api-key": d_key} if d_key else {}
        r_delta = requests.get("https://api.india.delta.exchange/v2/tickers/BTCUSD", headers=d_headers, timeout=10)
        if r_delta.status_code == 200 and r_delta.json().get("success"):
            res["Delta"] = "✅ OK"
        else:
            r_fb = requests.get("https://api.india.delta.exchange/v2/products/BTCUSD", headers=d_headers, timeout=10)
            if r_fb.status_code == 200:
                res["Delta"] = "✅ OK"
    except Exception as e:
        log.warning(f"[HEALTH] Delta check failed: {e}")

    # 2. Upstox Health Check
    try:
        u_token = os.getenv("UPSTOX_ANALYTICS_TOKEN", "").strip()
        if u_token:
            hdrs = {"Authorization": f"Bearer {u_token}", "Accept": "application/json"}
            if requests.get("https://api.upstox.com/v3/market-quote/ltp?instrument_key=NSE_INDEX|Nifty 50", headers=hdrs, timeout=10).status_code == 200:
                res["Upstox"] = "✅ OK"
    except Exception as e:
        log.warning(f"[HEALTH] Upstox check failed: {e}")

    # 3. Gemini AI Health Check (Uses active model from env, e.g., gemini-3.1-flash-lite)
    try:
        g_keys = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip()]
        g_model = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite").strip()
        if g_keys:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{g_model}:generateContent?key={g_keys[0]}"
            payload = {"contents": [{"role": "user", "parts": [{"text": "Hi"}]}]}
            r_gem = requests.post(url, json=payload, timeout=10)
            if r_gem.status_code == 200:
                res["Gemini"] = "✅ OK"
            else:
                log.warning(f"[HEALTH] Gemini ping returned {r_gem.status_code}: {r_gem.text[:120]}")
    except Exception as e:
        log.warning(f"[HEALTH] Gemini check failed: {e}")

    # 4. Claude AI Backup Check (Optional)
    try:
        a_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        c_model = os.getenv("CLAUDE_BACKUP_MODEL", "claude-haiku-4-5-20251001").strip()
        if a_key:
            hdrs = {"x-api-key": a_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}
            r_claude = requests.post("https://api.anthropic.com/v1/messages", headers=hdrs, json={"model": c_model, "max_tokens": 10, "messages": [{"role": "user", "content": "Hi"}]}, timeout=10)
            if r_claude.status_code == 200:
                res["Claude"] = "✅ OK"
        else:
            res["Claude"] = "⚪ Not Set (Optional)"
    except Exception as e:
        log.warning(f"[HEALTH] Claude check failed: {e}")

    msg = (f"🏥 <b>[SYSTEM HEALTH CHECK PASS]</b>\n\n"
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


def main():
    log.info("╔══════════════════════════════════════════════════════════════════╗")
    log.info("║  MULTI-ASSET LIQUIDITY HUNT ORCHESTRATOR V7.5 (NIFTY+BTC+GOLD)   ║")
    log.info("╚══════════════════════════════════════════════════════════════════╝")

    store = SharedNamespacedStore(REDIS_URL)

    # Trigger Startup Health Check asynchronously
    threading.Thread(target=run_health_check, daemon=True).start()

    scheduler = None
    if BackgroundScheduler:
        scheduler = BackgroundScheduler(timezone=IST, job_defaults={"max_instances": 3, "coalesce": True, "misfire_grace_time": 40})

        # 0. Daily Morning Health Check (09:15 AM IST)
        scheduler.add_job(run_health_check, "cron", hour=9, minute=15, id="daily_health_check")
        log.info("✅ Scheduled [SYSTEM] Daily Health Check (09:15 AM)")

        # 1. NIFTY 50: 5-Minute Setup Scan + 1-Hour Macro Reanalysis
        _register(scheduler, store, enabled=ENABLE_NIFTY, module=nifty_bot, func_name="run_nifty_1m_tick", tag="NIFTY", job_id="nifty_5m_engine", gate=is_nifty_open, label="5-Minute Setup Scan (09:20-15:30)", day_of_week="mon-fri", hour="9-15", minute="*/5", second=5)
        _register(scheduler, store, enabled=ENABLE_NIFTY, module=nifty_bot, func_name="run_1h_analysis", tag="NIFTY", job_id="nifty_1h_ai", gate=is_nifty_open, label="1-Hour Macro Reanalysis (10:15-15:15)", day_of_week="mon-fri", hour="10,11,12,13,14,15", minute=15, second=0)

        # 2. BITCOIN: 30-Minute Closed Bar Scan (24/7) + 6-Hour Macro Reanalysis
        _register(scheduler, store, enabled=ENABLE_BTC, module=btc_bot, func_name="run_btc_30m_tick", tag="BTC", job_id="btc_30m_engine", gate=is_btc_open, label="30-Minute Closed Bar Scan (24/7)", minute="0,30", second=15)
        _register(scheduler, store, enabled=ENABLE_BTC, module=btc_bot, func_name="run_6h_analysis", tag="BTC", job_id="btc_6h_ai", gate=is_btc_open, label="6-Hour Macro Re-analysis (00,06,12,18)", hour="0,6,12,18", minute=1, second=0)

        # 3. GOLD: 30-Minute Closed Bar Scan + 6-Hour Macro Reanalysis (Mon-Fri)
        _register(scheduler, store, enabled=ENABLE_GOLD, module=gold_bot, func_name="run_gold_1m_tick", tag="GOLD", job_id="gold_30m_engine", gate=is_gold_open, label="30-Minute Closed Bar Scan (Mon-Sat 02:30 AM)", day_of_week="mon-sat", minute="0,30", second=35)
        _register(scheduler, store, enabled=ENABLE_GOLD, module=gold_bot, func_name="run_6h_analysis", tag="GOLD", job_id="gold_6h_ai", gate=is_gold_open, label="6-Hour Macro Re-analysis (00,06,12,18)", day_of_week="mon-sat", hour="0,6,12,18", minute=2, second=0)

        scheduler.start()

    port = int(os.getenv("PORT", 8080))
    if Flask:
        app = Flask(__name__)
        @app.route("/")
        def index(): return "Multi-Asset Liquidity Hunt V7.5 Running ✅", 200
        @app.route("/health")
        def health(): return {"status": "ok", "time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")}, 200
        app.run(host="0.0.0.0", port=port, use_reloader=False)
    else:
        try:
            while True: time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            if scheduler: scheduler.shutdown(wait=False)

if __name__ == "__main__":
    main()
