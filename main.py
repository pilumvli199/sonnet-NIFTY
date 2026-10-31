"""
╔══════════════════════════════════════════════════════════════════════════╗
║   MASTER ORCHESTRATOR (main.py) — NIFTY 50 + BITCOIN + GOLD V7.1         ║
║   Multi-Asset Launcher for Koyeb / Docker / Cloud                        ║
║                                                                          ║
║   SCHEDULE (all times IST):                                              ║
║     NIFTY -> Mon-Fri, 09:20 to 15:30 (NSE holidays skipped via env)      ║
║     BTC   -> 24x7                                                        ║
║     GOLD  -> Mon-Fri                                                     ║
║                                                                          ║
║   ISOLATION (what is and is NOT isolated):                               ║
║     - Import errors and runtime exceptions in one bot do not stop        ║
║       the other two.                                                     ║
║     - All 3 bots still share ONE process and ONE store (RAM / Redis).    ║
║       Key prefixes NIFTY:* / BTC:* / GOLD:* are a convention the bots    ║
║       must follow - this file does not enforce them.                     ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import time
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

# Silence per-request / per-job noise (Koyeb health checks + 3 jobs/min)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

# Enable/Disable flags per asset via .env
ENABLE_NIFTY = os.getenv("ENABLE_NIFTY", "true").lower() == "true"
ENABLE_BTC   = os.getenv("ENABLE_BTC",   "true").lower() == "true"
ENABLE_GOLD  = os.getenv("ENABLE_GOLD",  "true").lower() == "true"
REDIS_URL    = os.getenv("REDIS_URL", "")

# Optional: comma-separated NSE holidays, e.g. NSE_HOLIDAYS=2026-11-10,2026-11-24,2026-12-25
NSE_HOLIDAYS = {d.strip() for d in os.getenv("NSE_HOLIDAYS", "").split(",") if d.strip()}


# ═══════════════════════════════════════════════════════════════════════
# MARKET-HOUR GATES
# ═══════════════════════════════════════════════════════════════════════

def is_nifty_open(now=None) -> bool:
    """Mon-Fri, 09:20 <= time <= 15:30 IST, excluding NSE_HOLIDAYS."""
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS:
        return False
    return (9, 20) <= (now.hour, now.minute) <= (15, 30)


def is_gold_open(now=None) -> bool:
    """Mon-Fri (IST calendar day). Intraday open/close handling is left to gold_bot."""
    now = now or datetime.now(IST)
    return now.weekday() < 5


def is_btc_open(now=None) -> bool:
    return True


# ═══════════════════════════════════════════════════════════════════════
# SHARED REDIS + BOUNDED RAM STORE
# ═══════════════════════════════════════════════════════════════════════

class SharedNamespacedStore:
    """
    Redis (if REDIS_URL is set and reachable) with a bounded in-memory fallback.
    RAM fallback is capped by KEY COUNT (500). Value size is not capped, so
    keep values small (JSON of a few KB each).
    """
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
                log.info("✅ Connected to Redis (namespaced mode: NIFTY:*, BTC:*, GOLD:*)")
            except Exception as e:
                log.warning(f"⚠️ Redis unavailable ({e}) — running in bounded RAM mode")
        else:
            log.info("ℹ️ Running in bounded RAM mode (NIFTY:*, BTC:*, GOLD:*)")

    def set(self, namespaced_key: str, value, ttl: int = 86400):
        raw = json.dumps(value)
        if self._r:
            try:
                self._r.setex(namespaced_key, ttl, raw)
                return
            except Exception as e:
                log.error(f"Redis set error ({namespaced_key}): {e}")
        with self._lock:
            if namespaced_key in self._ram:
                self._ram.pop(namespaced_key)
            elif len(self._ram) >= self.MAX_RAM_KEYS:
                now_ts = time.time()
                expired = [k for k, (_, exp) in self._ram.items() if now_ts > exp]
                for k in expired:
                    self._ram.pop(k, None)
                if len(self._ram) >= self.MAX_RAM_KEYS:
                    oldest_key = next(iter(self._ram))
                    self._ram.pop(oldest_key, None)
            self._ram[namespaced_key] = (raw, time.time() + ttl)

    def get(self, namespaced_key: str):
        if self._r:
            try:
                raw = self._r.get(namespaced_key)
                if raw:
                    return json.loads(raw)
            except Exception as e:
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
            except Exception:
                pass
        with self._lock:
            self._ram.pop(namespaced_key, None)

    def stats(self):
        with self._lock:
            return {
                "storage_mode": "redis" if self._r else "bounded_ram",
                "ram_keys_count": len(self._ram),
                "ram_bytes_approx": sum(len(raw) for raw, _ in self._ram.values()),
            }


# ═══════════════════════════════════════════════════════════════════════
# FAULT-TOLERANT IMPORTS
# ═══════════════════════════════════════════════════════════════════════

try:
    import nifty_bot
except Exception as e:
    nifty_bot = None
    log.error(f"⚠️ Failed to import nifty_bot.py: {e}")

try:
    import btc_bot
except Exception as e:
    btc_bot = None
    log.error(f"⚠️ Failed to import btc_bot.py: {e}")

try:
    import gold_bot
except Exception as e:
    gold_bot = None
    log.error(f"⚠️ Failed to import gold_bot.py: {e}")


# ═══════════════════════════════════════════════════════════════════════
# JOB REGISTRATION HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _guarded(tag: str, fn, gate):
    """Wrap a bot tick: skip outside market hours, and log failures under the bot's tag."""
    bot_log = logging.getLogger(tag)

    def runner(store):
        if not gate():
            return
        try:
            fn(store)
        except Exception:
            bot_log.exception("tick failed")

    runner.__name__ = f"{tag.lower()}_guarded_tick"
    return runner


def _register(scheduler, store, *, enabled, module, func_name, tag, job_id, gate, label, **cron):
    if not enabled:
        log.info(f"⏸️ [{tag}] disabled via .env")
        return
    if module is None:
        log.error(f"❌ [{tag}] module failed to import — not scheduled")
        return
    fn = getattr(module, func_name, None)
    if not callable(fn):
        log.error(f"❌ [{tag}] {func_name}() not found in module — not scheduled")
        return
    if scheduler:
        scheduler.add_job(
            _guarded(tag, fn, gate),
            "cron",
            args=[store],
            id=job_id,
            **cron,
        )
    log.info(f"✅ Scheduled [{tag}] {label}")


def main():
    log.info("╔══════════════════════════════════════════════════════════════════╗")
    log.info("║  MULTI-ASSET LIQUIDITY HUNT ORCHESTRATOR V7.1 (NIFTY+BTC+GOLD)   ║")
    log.info("╚══════════════════════════════════════════════════════════════════╝")

    store = SharedNamespacedStore(REDIS_URL)

    scheduler = None
    if BackgroundScheduler:
        scheduler = BackgroundScheduler(
            timezone=IST,
            job_defaults={"max_instances": 1, "coalesce": True, "misfire_grace_time": 40},
        )

        # 1. NIFTY 50 — Mon-Fri, cron wakes 09:00-15:59, gate allows only 09:20-15:30
        _register(
            scheduler, store,
            enabled=ENABLE_NIFTY, module=nifty_bot, func_name="run_nifty_1m_tick",
            tag="NIFTY", job_id="nifty_1m_engine", gate=is_nifty_open,
            label="1-Minute Zone & Sniper Engine (Mon-Fri 09:20-15:30 IST)",
            day_of_week="mon-fri", hour="9-15", minute="*", second=5,
        )

        # 2. BITCOIN — 24/7
        _register(
            scheduler, store,
            enabled=ENABLE_BTC, module=btc_bot, func_name="run_btc_1m_tick",
            tag="BTC", job_id="btc_1m_engine", gate=is_btc_open,
            label="1-Minute Liquidity Hunt Engine (24/7)",
            minute="*", second=20,
        )

        # 3. GOLD — Mon-Fri
        _register(
            scheduler, store,
            enabled=ENABLE_GOLD, module=gold_bot, func_name="run_gold_1m_tick",
            tag="GOLD", job_id="gold_1m_engine", gate=is_gold_open,
            label="1-Minute Liquidity Hunt Engine (Mon-Fri)",
            day_of_week="mon-fri", minute="*", second=35,
        )

        scheduler.start()

    # Flask Health & Status Server for Koyeb
    port = int(os.getenv("PORT", 8000))
    if Flask:
        app = Flask(__name__)

        @app.route("/")
        def index():
            return "Multi-Asset Liquidity Hunt V7.1 (NIFTY + BTC + GOLD) Running ✅", 200

        @app.route("/health")
        def health():
            now = datetime.now(IST)
            next_runs_map = {j.id: str(j.next_run_time) for j in scheduler.get_jobs()} if scheduler else {}
            return {
                "status": "ok",
                "time_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
                "engines": {
                    "nifty_enabled": bool(ENABLE_NIFTY and nifty_bot),
                    "btc_enabled": bool(ENABLE_BTC and btc_bot),
                    "gold_enabled": bool(ENABLE_GOLD and gold_bot),
                },
                "market_open_now": {
                    "nifty": is_nifty_open(now),
                    "btc": is_btc_open(now),
                    "gold": is_gold_open(now),
                },
                "next_runs": next_runs_map,
                "memory": store.stats(),
            }, 200

        log.info(f"🌐 Koyeb Health Server listening on port {port}")
        try:
            app.run(host="0.0.0.0", port=port, use_reloader=False)
        finally:
            if scheduler:
                scheduler.shutdown(wait=False)
    else:
        log.warning("Flask not installed in local environment — running scheduler loop")
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            if scheduler:
                scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
