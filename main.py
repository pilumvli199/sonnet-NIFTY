"""
MASTER ORCHESTRATOR (main.py) — NIFTY 50 + BITCOIN + GOLD V8.5 PRODUCTION
Multi-Asset Cloud Launcher for Koyeb / Docker
- Non-blocking Asynchronous Scheduler with strict Lock per Job
- Automatic Last-Tick Vitality Writer & Reader in Heartbeat
- 6-Hour Macro Alignment: 05:31, 11:31, 17:31, 23:31 IST (UTC-Aligned Candle Closes)
- Morning Bias Initialization: Nifty 09:21 IST
- Bounded RAM Auto-Cleanup & Secure API Exception Logging
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
    if now.weekday() >= 5:
        return False
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS:
        return False
    return (9, 20) <= (now.hour, now.minute) <= (15, 30)


def is_nifty_morning_bias(now=None) -> bool:
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS:
        return False
    return (now.hour, now.minute) == (9, 21)


def is_gold_open(now=None) -> bool:
    now = now or datetime.now(IST)
    weekday = now.weekday()
    t_min = now.hour * 60 + now.minute

    if weekday == 6:
        return False
    if weekday == 5:
        return t_min < (2 * 60 + 30)
    if weekday == 0:
        return t_min >= (3 * 60 + 30)
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
                log.info("Connected to Redis successfully")
            except Exception as e:
                log.warning(f"Redis unavailable ({type(e).__name__}) — running in bounded RAM mode")
        else:
            log.info("Running in bounded RAM mode")

    def _cleanup_ram(self):
        """Evicts expired keys if RAM exceeds MAX limit to prevent memory creep."""
        if len(self._ram) > self.MAX_RAM_KEYS:
            now = time.time()
            expired = [k for k, (_, exp) in self._ram.items() if now > exp]
            for k in expired:
                self._ram.pop(k, None)
            
            if len(self._ram) > self.MAX_RAM_KEYS:
                excess = len(self._ram) - self.MAX_RAM_KEYS
                for k in list(self._ram.keys())[:excess]:
                    self._ram.pop(k, None)

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
            self._cleanup_ram()

    def get(self, namespaced_key: str):
        if self._r:
            try:
                raw = self._r.get(namespaced_key)
                if raw:
                    return json.loads(raw)
            except Exception:
                pass
        with self._lock:
            item = self._ram.get(namespaced_key)
            if not item:
                return None
            raw, exp = item
            if time.time() > exp:
                return None
            return json.loads(raw)

    def delete(self, namespaced_key: str):
        if self._r:
            try:
                self._r.delete(namespaced_key)
            except Exception:
                pass
        with self._lock:
            self._ram.pop(namespaced_key, None)

    def stats(self):
        with self._lock:
            return {"storage_mode": "redis" if self._r else "bounded_ram", "ram_keys_count": len(self._ram)}


try:
    import nifty_bot
except Exception as e:
    nifty_bot = None
    log.error(f"Failed to import nifty_bot: {type(e).__name__}")

try:
    import btc_bot
except Exception as e:
    btc_bot = None
    log.error(f"Failed to import btc_bot: {type(e).__name__}")

try:
    import gold_bot
except Exception as e:
    gold_bot = None
    log.error(f"Failed to import gold_bot: {type(e).__name__}")


def _guarded(tag: str, fn, gate, run_async: bool = True, tick_key: str = None):
    bot_log = logging.getLogger(tag)
    lock = threading.Lock()

    def runner(store):
        if not gate():
            return

        def _task():
            if not lock.acquire(blocking=False):
                bot_log.warning(f"[{tag}] Previous run still active, skipping duplicate tick")
                return
            try:
                fn(store)
                if tick_key:
                    store.set(tick_key, time.time(), ttl=86400)
            except Exception as e:
                bot_log.exception(f"[{tag}] Task execution failed: {type(e).__name__}")
            finally:
                lock.release()

        if run_async:
            threading.Thread(target=_task, daemon=True).start()
        else:
            _task()

    runner.__name__ = f"{tag.lower()}_guarded_runner"
    return runner


def _register(scheduler, store, *, enabled, module, func_name, tag, job_id, gate, label, run_async=True, tick_key=None, **cron):
    if not enabled:
        return
    if module is None:
        return
    fn = getattr(module, func_name, None)
    if not callable(fn):
        log.warning(f"[{tag}] Function '{func_name}' not found in {module.__name__}. Skipping.")
        return
    if scheduler:
        scheduler.add_job(_guarded(tag, fn, gate, run_async=run_async, tick_key=tick_key), "cron", args=[store], id=job_id, **cron)
    log.info(f"Scheduled [{tag}] {label}")


def _format_tick_vitality(store, key):
    last_tick = store.get(key)
    if not last_tick:
        return "ACTIVE (Waiting 1st tick)"
    
    elapsed = int((time.time() - last_tick) / 60)
    if elapsed > 10:
        return f"⚠️ WARN: STALLED (Last tick {elapsed}m ago)"
    return f"ACTIVE (Tick {elapsed}m ago)"


# ─── 5-MINUTE LIVE MULTI-ASSET RADAR MONITOR ───
def run_asset_radar_heartbeat(store):
    """Logs real-time market price fetching & vitality status every 5 minutes."""
    now = datetime.now(IST)
    now_str = now.strftime("%H:%M:%S IST")
    h_log = logging.getLogger("HEARTBEAT")

    # 1. BTC Live Status
    btc_status = "Disabled"
    if ENABLE_BTC:
        try:
            r = requests.get("https://api.india.delta.exchange/v2/tickers/BTCUSD", timeout=8)
            if r.status_code == 200:
                data = r.json().get("result", {})
                close_p = float(data.get("close", 0.0))
                vitality = _format_tick_vitality(store, "BTC:last_tick")
                btc_status = f"LIVE OK (${close_p:,.1f}) | {vitality}"
            else:
                btc_status = f"Delta HTTP {r.status_code}"
        except Exception as e:
            btc_status = f"Fetch Error ({type(e).__name__})"

    # 2. Nifty Status
    nifty_status = "Disabled"
    if ENABLE_NIFTY:
        if is_nifty_open(now):
            vitality = _format_tick_vitality(store, "NIFTY:last_tick")
            nifty_status = vitality
        else:
            if now.weekday() >= 5:
                nifty_status = "Closed (Weekend) -> Opens Monday 09:20 IST"
            elif (now.hour, now.minute) < (9, 20):
                nifty_status = "Pre-Market -> Opens Today 09:20 IST"
            else:
                nifty_status = "Market Closed -> Opens Next Trading Day 09:20 IST"

    # 3. Gold Status
    gold_status = "Disabled"
    if ENABLE_GOLD:
        if is_gold_open(now):
            vitality = _format_tick_vitality(store, "GOLD:last_tick")
            gold_status = vitality
        else:
            if now.weekday() == 6:
                gold_status = "Closed (Sunday) -> Opens Monday 03:30 IST"
            elif now.weekday() == 5:
                gold_status = "Closed (Weekend) -> Opens Monday 03:30 IST"
            else:
                gold_status = "Daily Rollover Break (02:30-03:30 AM IST)"

    stats = store.stats() if hasattr(store, "stats") else {}
    h_log.info(f"═════════ MULTI-ASSET RADAR ({now_str}) ═════════")
    h_log.info(f"BTCUSD   : {btc_status}")
    h_log.info(f"NIFTY 50 : {nifty_status}")
    h_log.info(f"GOLD     : {gold_status}")
    h_log.info(f"STATUS   : Storage: {stats.get('storage_mode', 'RAM')} ({stats.get('ram_keys_count', 0)} keys) | Server: Healthy")
    h_log.info("══════════════════════════════════════════════════════")


# ─── SYSTEM HEALTH CHECK ENGINE ───
def run_health_check():
    log.info("[SYSTEM] Running Health Check & API Validations...")
    res = {"Delta": "FAIL", "Upstox": "FAIL", "Gemini": "FAIL", "Claude": "Not Set (Optional)"}

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
        log.warning(f"[HEALTH] Delta check failed: {type(e).__name__}")
        res["Delta"] = "FAIL (Connection Error)"

    try:
        u_token = os.getenv("UPSTOX_ANALYTICS_TOKEN", "").strip()
        if u_token and "your_" not in u_token.lower():
            hdrs = {"Authorization": f"Bearer {u_token}", "Accept": "application/json"}
            r_upstox = requests.get("https://api.upstox.com/v3/market-quote/ltp?instrument_key=NSE_INDEX|Nifty 50", headers=hdrs, timeout=10)
            if r_upstox.status_code == 200:
                res["Upstox"] = "OK"
            else:
                res["Upstox"] = f"FAIL (HTTP {r_upstox.status_code})"
        else:
            res["Upstox"] = "FAIL (Token Not Set)"
    except Exception as e:
        log.warning(f"[HEALTH] Upstox check failed: {type(e).__name__}")
        res["Upstox"] = "FAIL (Connection Error)"

    try:
        raw_keys = [k.strip() for k in os.getenv("GEMINI_API_KEYS", os.getenv("GEMINI_API_KEY", "")).split(",") if k.strip()]
        g_keys = [k for k in raw_keys if "your_" not in k.lower()]
        models_to_try = [
            os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite").strip(),
            "gemini-2.5-flash",
            "gemini-1.5-flash"
        ]

        if not g_keys:
            res["Gemini"] = "FAIL (No Key Provided)"
        else:
            success = False
            last_err = ""
            for m in dict.fromkeys(models_to_try):
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={g_keys[0]}"
                payload = {"contents": [{"role": "user", "parts": [{"text": "Hi"}]}]}
                r_gem = requests.post(url, json=payload, timeout=10)
                if r_gem.status_code == 200:
                    res["Gemini"] = f"OK ({m})"
                    success = True
                    break
                else:
                    last_err = f"HTTP {r_gem.status_code}"
                    log.warning(f"[HEALTH] Gemini test with {m} failed: {r_gem.status_code}")

            if not success:
                res["Gemini"] = f"FAIL ({last_err})"
    except Exception as e:
        log.warning(f"[HEALTH] Gemini check exception: {type(e).__name__}")
        res["Gemini"] = "FAIL (Connection Error)"

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
        log.warning(f"[HEALTH] Claude check failed: {type(e).__name__}")
        
    overall_status = "PASS" if res["Delta"] == "OK" and res["Gemini"].startswith("OK") else "WARNING/FAIL"

    msg = (f"<b>[SYSTEM HEALTH CHECK {overall_status}]</b>\n\n"
           f"<b>Upstox Data :</b> {res['Upstox']}\n"
           f"<b>Delta Data  :</b> {res['Delta']}\n"
           f"<b>Gemini AI   :</b> {res['Gemini']}\n"
           f"<b>Claude AI   :</b> {res['Claude']}\n\n"
           f"<i>Multi-Asset Orchestrator check complete. {datetime.now(IST).strftime('%H:%M IST')}</i>")

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("BTC_TELEGRAM_CHAT_ID")
    if tg_token and tg_chat:
        try:
            requests.post(f"https://api.telegram.org/bot{tg_token}/sendMessage", json={"chat_id": tg_chat, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except Exception as e:
            log.error(f"[HEALTH] Telegram alert failed: {type(e).__name__}")


def main():
    log.info("Starting Multi-Asset Liquidity Hunt Orchestrator V8.5...")

    store = SharedNamespacedStore(REDIS_URL)

    # Trigger Startup Health Check asynchronously
    threading.Thread(target=run_health_check, daemon=True).start()

    scheduler = None
    if BackgroundScheduler:
        scheduler = BackgroundScheduler(timezone=IST, job_defaults={"max_instances": 1, "coalesce": True, "misfire_grace_time": 40})

        # 0. Daily Morning Health Check (09:15 AM IST)
        scheduler.add_job(run_health_check, "cron", hour=9, minute=15, id="daily_health_check")
        log.info("Scheduled [SYSTEM] Daily Health Check (09:15 AM)")

        # 1. 5-Minute Multi-Asset Live Radar Heartbeat
        scheduler.add_job(run_asset_radar_heartbeat, "cron", args=[store], minute="*/5", second=0, id="asset_radar_heartbeat")
        log.info("Scheduled [SYSTEM] 5-Minute Multi-Asset Radar Heartbeat")

        # 2. NIFTY 50: Morning Bias (09:21) + 5-Minute Setup Scan + 1-Hour Macro Reanalysis
        _register(scheduler, store, enabled=ENABLE_NIFTY, module=nifty_bot, func_name="run_1h_analysis", tag="NIFTY", job_id="nifty_morning_bias", gate=is_nifty_morning_bias, label="Morning Bias Initialization (09:21)", day_of_week="mon-fri", hour=9, minute=21, second=0)
        _register(scheduler, store, enabled=ENABLE_NIFTY, module=nifty_bot, func_name="run_nifty_1m_tick", tag="NIFTY", job_id="nifty_5m_engine", gate=is_nifty_open, label="5-Minute Setup Scan (09:20-15:30)", tick_key="NIFTY:last_tick", day_of_week="mon-fri", hour="9-15", minute="*/5", second=5)
        _register(scheduler, store, enabled=ENABLE_NIFTY, module=nifty_bot, func_name="run_1h_analysis", tag="NIFTY", job_id="nifty_1h_ai", gate=is_nifty_open, label="1-Hour Macro Reanalysis (10:15-15:15)", day_of_week="mon-fri", hour="10-15", minute=15, second=0)

        # 3. BITCOIN: 5-Minute Execution Scan (24/7) + 6-Hour Macro Reanalysis (UTC-aligned closes: 05:31, 11:31, 17:31, 23:31 IST)
        _register(scheduler, store, enabled=ENABLE_BTC, module=btc_bot, func_name="run_btc_1m_tick", tag="BTC", job_id="btc_5m_engine", gate=is_btc_open, label="5-Minute Execution Scan (24/7)", tick_key="BTC:last_tick", minute="*/5", second=15)
        _register(scheduler, store, enabled=ENABLE_BTC, module=btc_bot, func_name="run_6h_analysis", tag="BTC", job_id="btc_6h_ai", gate=is_btc_open, label="6-Hour Macro Re-analysis (05:31, 11:31, 17:31, 23:31 IST)", hour="5,11,17,23", minute=31, second=0)

        # 4. GOLD: 5-Minute Execution Scan + 6-Hour Macro Reanalysis (Mon-Sat, UTC-aligned closes)
        _register(scheduler, store, enabled=ENABLE_GOLD, module=gold_bot, func_name="run_gold_1m_tick", tag="GOLD", job_id="gold_5m_engine", gate=is_gold_open, label="5-Minute Execution Scan (Mon-Sat)", tick_key="GOLD:last_tick", day_of_week="mon-sat", minute="*/5", second=35)
        _register(scheduler, store, enabled=ENABLE_GOLD, module=gold_bot, func_name="run_6h_analysis", tag="GOLD", job_id="gold_6h_ai", gate=is_gold_open, label="6-Hour Macro Re-analysis (05:32, 11:32, 17:32, 23:32 IST)", day_of_week="mon-sat", hour="5,11,17,23", minute=32, second=0)

        scheduler.start()

    port = int(os.getenv("PORT", 8080))
    if Flask:
        app = Flask(__name__)

        @app.route("/")
        def index():
            return "Multi-Asset Liquidity Hunt V8.5 Running OK", 200

        @app.route("/health")
        def health():
            return {"status": "ok", "time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")}, 200

        app.run(host="0.0.0.0", port=port, use_reloader=False)
    else:
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            if scheduler:
                scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
