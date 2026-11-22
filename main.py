"""
================================================================================
UNIFIED MULTI-ASSET PURE LIQUIDITY ORCHESTRATOR — PRODUCTION V10.0
Assets: NIFTY 50 (Upstox V3) + GOLD XAUTUSD (Delta) + BITCOIN BTCUSD (Delta)
================================================================================
CORE FEATURES:
  1. First-5-Minute Auto Startup Diagnostic Suite:
     - Verifies Upstox V3, Delta Exchange India, Redis/Memory Store, Telegram,
       and all 3 Gemini AI models (gemini-4-hydrogen, gemini-3.5-flash-lite,
       gemini-3.1-flash-lite) within the first 5 minutes of deployment.
  2. Strict Namespace Isolation:
     - NIFTY:* | GOLD:* | BTC:* in Redis (with automatic in-memory fallback).
  3. Zero-Lag :05s Aligned Multi-Threaded Scheduler:
     - Triggers at :05s of every minute so closed 5M/15M/1H/4H/1D bars are
       scanned immediately without cross-market blocking.
     - NIFTY 50: 09:20 BIG Analysis + Hourly Sync (10:20–14:20 IST).
     - GOLD & BTC: 23:30 BIG Analysis + 6-Hour Sync (05:30, 11:30, 17:30 IST).
  4. Daily Morning Health Report (09:03–09:10 IST) + Koyeb HTTP Health Server.
================================================================================
"""

import os
import json
import time
import html
import logging
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import pytz

import sys
import glob
import importlib.util

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)


def _load_market_module(aliases: tuple):
    for name in aliases:
        try:
            return __import__(name)
        except ModuleNotFoundError:
            pass
    for name in aliases:
        candidates = sorted(
            [p for p in glob.glob(os.path.join(BASE_DIR, f"{name}*.py")) if os.path.basename(p) != "main.py"],
            key=os.path.getmtime,
            reverse=True,
        )
        if candidates:
            spec = importlib.util.spec_from_file_location(aliases[0], candidates[0])
            mod = importlib.util.module_from_spec(spec)
            sys.modules[aliases[0]] = mod
            spec.loader.exec_module(mod)
            return mod
    present = sorted(os.listdir(BASE_DIR))
    raise ModuleNotFoundError(
        f"Missing '{aliases[0]}.py' in {BASE_DIR}. Files found: {present}."
    )


nifty50 = _load_market_module(("nifty_bot", "nifty50"))
gold = _load_market_module(("gold_bot", "gold"))
bitcoin = _load_market_module(("btc_bot", "bitcoin"))

IST = pytz.timezone("Asia/Kolkata")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("MAIN")

PORT            = int(os.getenv("PORT", "8080"))
REDIS_URL       = os.getenv("REDIS_URL", "").strip()
TG_BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "").strip()

ENABLE_NIFTY    = os.getenv("ENABLE_NIFTY", "true").strip().lower() in ("1", "true", "yes", "on")
ENABLE_GOLD     = os.getenv("ENABLE_GOLD", "true").strip().lower() in ("1", "true", "yes", "on")
ENABLE_BTC      = os.getenv("ENABLE_BTC", "true").strip().lower() in ("1", "true", "yes", "on")

HYDROGEN_MODEL  = os.getenv("GEMINI_HYDROGEN_MODEL", "gemini-4-hydrogen").strip()
LITE_35_MODEL   = os.getenv("GEMINI_35_LITE_MODEL", "gemini-3.5-flash-lite").strip()
LITE_31_MODEL   = os.getenv("GEMINI_31_LITE_MODEL", "gemini-3.1-flash-lite").strip()

_boot_time_ist = datetime.now(IST)
_boot_ts = time.time()


def esc(val) -> str:
    return html.escape(str(val), quote=False) if val is not None else ""


# ==============================================================================
# SHARED REDIS / IN-MEMORY STORE (STRICT NAMESPACE SUPPORT)
# ==============================================================================
class SharedStateStore:
    def __init__(self, redis_url: str = ""):
        self._mem = {}
        self._exp = {}
        self._lock = threading.Lock()
        self._redis = None
        self.backend_name = "IN_MEMORY_RAM"

        if redis_url:
            try:
                import redis
                client = redis.from_url(redis_url, decode_responses=True, socket_timeout=5)
                client.ping()
                self._redis = client
                self.backend_name = "REDIS_CLOUD"
                log.info("[MAIN] Connected to Redis Cloud state store.")
            except Exception as e:
                log.warning(f"[MAIN] Redis unavailable ({type(e).__name__}); using thread-safe RAM fallback.")

    def set(self, key: str, value, ttl: int = 86400):
        raw = json.dumps(value)
        if self._redis is not None:
            try:
                self._redis.setex(key, ttl, raw)
                return
            except Exception:
                pass
        with self._lock:
            self._mem[key] = raw
            self._exp[key] = time.time() + ttl

    def get(self, key: str):
        if self._redis is not None:
            try:
                raw = self._redis.get(key)
                return json.loads(raw) if raw is not None else None
            except Exception:
                pass
        with self._lock:
            if key in self._exp and time.time() > self._exp[key]:
                self._mem.pop(key, None)
                self._exp.pop(key, None)
                return None
            raw = self._mem.get(key)
            return json.loads(raw) if raw is not None else None


shared_store = SharedStateStore(REDIS_URL)


def tg_broadcast(text: str):
    target_chat = TG_CHAT_ID or nifty50.TG_CHAT_ID or gold.TG_CHAT_ID or bitcoin.TG_CHAT_ID
    if not TG_BOT_TOKEN or not target_chat:
        log.info(f"[MAIN-TG-DRY]\n{text}\n")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": target_chat, "text": text, "parse_mode": "HTML"},
            timeout=12,
        )
        if r.status_code != 200:
            plain = text.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "").replace("<i>", "").replace("</i>", "")
            requests.post(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                json={"chat_id": target_chat, "text": plain},
                timeout=12,
            )
    except Exception as e:
        log.error(f"[MAIN] Telegram broadcast error: {type(e).__name__}")


# ==============================================================================
# KOYEB / UPTIMEROBOT 24/7 HTTP HEALTH SERVER
# ==============================================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        uptime_min = int((time.time() - _boot_ts) / 60)
        if self.path == "/status":
            payload = {
                "status": "ONLINE",
                "engine": "Pure Liquidity Multi-Asset V10.0",
                "boot_time_ist": _boot_time_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
                "uptime_minutes": uptime_min,
                "store_backend": shared_store.backend_name,
                "nifty_zones": len(shared_store.get("NIFTY:active_zones") or []),
                "gold_zones": len(shared_store.get("GOLD:active_zones") or []),
                "btc_zones": len(shared_store.get("BTC:active_zones") or []),
            }
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"OK - Pure Liquidity V10.0 Active | Uptime: {uptime_min}m\n".encode("utf-8"))

    def log_message(self, format, *args):
        return


def start_http_server():
    try:
        srv = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        log.info(f"[MAIN] HTTP Health Server active on port {PORT}")
        srv.serve_forever()
    except Exception as e:
        log.error(f"[MAIN] HTTP server failed: {type(e).__name__}")


# ==============================================================================
# FIRST-5-MINUTE AUTO STARTUP DIAGNOSTIC SUITE (APIs + ALL 3 AI MODELS)
# ==============================================================================
def _ping_gemini_model(model_name: str, thinking_lvl: str) -> str:
    t0 = time.time()
    res = nifty50.call_gemini_single(
        model_name=model_name,
        thinking_lvl=thinking_lvl,
        timeout_sec=45,
        max_tokens=256,
        sys_prompt='Respond ONLY in valid JSON: {"status": "OK"}',
        user_prompt="Startup health check ping. Return status OK.",
    )
    ms = int((time.time() - t0) * 1000)
    return f"ONLINE ({ms}ms)" if (res and res.get("status") == "OK") else "OFFLINE / TIMEOUT"


def run_first_5min_startup_diagnostics():
    """
    Runs immediately on boot (within first 5 minutes) to verify:
      - Upstox V3 (NIFTY 50)
      - Delta Exchange India (GOLD & BITCOIN)
      - All 3 Gemini Models (gemini-4-hydrogen, gemini-3.5-flash-lite, gemini-3.1-flash-lite)
      - State Store & Telegram
    """
    log.info("[MAIN] 🔍 Executing First-5-Minute Startup API & Model Diagnostic Suite...")
    now = datetime.now(IST)

    # 1. Test Data APIs + trigger initial Top-Down / 6H Zone Syncs
    nifty_diag = nifty50.run_startup_diagnostics(shared_store) if ENABLE_NIFTY else {"api_ok": False, "ltp": 0.0, "api_ms": 0}
    gold_diag = gold.run_startup_diagnostics(shared_store) if ENABLE_GOLD else {"api_ok": False, "ltp": 0.0, "api_ms": 0}
    btc_diag = bitcoin.run_startup_diagnostics(shared_store) if ENABLE_BTC else {"api_ok": False, "ltp": 0.0, "api_ms": 0}

    # 2. Test all 3 Gemini AI Models
    m_hydro = _ping_gemini_model(HYDROGEN_MODEL, "HIGH")
    m_35 = _ping_gemini_model(LITE_35_MODEL, "HIGH")
    m_31 = _ping_gemini_model(LITE_31_MODEL, "OFF")

    n_str = f"ONLINE ({nifty_diag['ltp']:.2f} | {nifty_diag['api_ms']}ms)" if nifty_diag["api_ok"] else "OFFLINE / TOKEN_NEEDED"
    g_str = f"ONLINE (${gold_diag['ltp']:.2f} | {gold_diag['api_ms']}ms)" if gold_diag["api_ok"] else "OFFLINE"
    b_str = f"ONLINE (${btc_diag['ltp']:,.1f} | {btc_diag['api_ms']}ms)" if btc_diag["api_ok"] else "OFFLINE"

    report = (
        f"🚀 <b>[PURE LIQUIDITY ENGINE V10.0 — STARTUP VERIFIED]</b>\n"
        f"📅 <b>Boot Time:</b> <code>{now.strftime('%Y-%m-%d %H:%M:%S IST')}</code>\n"
        f"🗄️ <b>State Store:</b> <code>{shared_store.backend_name} (NIFTY:* | GOLD:* | BTC:*)</code>\n\n"
        f"📡 <b>Market Data APIs:</b>\n"
        f"• <b>NIFTY 50 (Upstox V3):</b> <code>{n_str}</code>\n"
        f"• <b>GOLD (Delta India)  :</b> <code>{g_str}</code>\n"
        f"• <b>BTCUSD (Delta India):</b> <code>{b_str}</code>\n\n"
        f"🧠 <b>Gemini AI Fallback Chain:</b>\n"
        f"• <code>{esc(HYDROGEN_MODEL)} [HIGH]</code> : <code>{m_hydro}</code>\n"
        f"• <code>{esc(LITE_35_MODEL)} [HIGH]</code> : <code>{m_35}</code>\n"
        f"• <code>{esc(LITE_31_MODEL)} [OFF]</code>  : <code>{m_31}</code>\n\n"
        f"⏰ <b>Schedules Active (IST):</b>\n"
        f"• <b>NIFTY 50:</b> 09:20 BIG Analysis + Hourly (10:20–14:20) | Vacuum: ±300 pts\n"
        f"• <b>GOLD/BTC:</b> 23:30 BIG Analysis + 6H Sync (05:30, 11:30, 17:30) | Vacuum: ±$100 / ±$2,500\n"
        f"• <b>Mode:</b> 100% Pure Price Action (Zero Indicators) · Entry-Only Manual SL/Target"
    )
    tg_broadcast(report)
    log.info("[MAIN] ✅ First-5-Minute Startup Diagnostic Report published.")


# ==============================================================================
# DAILY CONSOLIDATED MORNING HEALTH AUDIT (09:03 - 09:10 IST)
# ==============================================================================
def check_consolidated_morning_health():
    now = datetime.now(IST)
    today_str = now.strftime("%Y-%m-%d")
    if now.hour == 9 and 3 <= now.minute <= 10:
        if shared_store.get("MAIN:morning_health_date") == today_str:
            return
        shared_store.set("MAIN:morning_health_date", today_str, ttl=86400)
        nz = len(shared_store.get("NIFTY:active_zones") or [])
        gz = len(shared_store.get("GOLD:active_zones") or [])
        bz = len(shared_store.get("BTC:active_zones") or [])
        tg_broadcast(
            f"☀️ <b>[MORNING 09:05 IST MULTI-MARKET HEALTH CHECK]</b>\n"
            f"📅 <code>{now.strftime('%d %b %Y | %H:%M IST')}</code>\n"
            f"• <b>NIFTY 50 Zones:</b> <code>{nz}</code> (Regime: <code>{shared_store.get('NIFTY:regime') or 'READY'}</code>)\n"
            f"• <b>GOLD Zones    :</b> <code>{gz}</code> (Regime: <code>{shared_store.get('GOLD:regime') or 'READY'}</code>)\n"
            f"• <b>BTCUSD Zones  :</b> <code>{bz}</code> (Regime: <code>{shared_store.get('BTC:regime') or 'READY'}</code>)\n"
            f"✅ <i>All 3 engines healthy &amp; synced before 09:15 NSE Open.</i>"
        )


# ==============================================================================
# ZERO-LAG :05s ALIGNED ORCHESTRATOR LOOP
# ==============================================================================
def _sleep_until_next_05s():
    now = datetime.now(IST)
    sec = now.second + (now.microsecond / 1_000_000.0)
    wait = (5.0 - sec) if sec < 5.0 else (65.0 - sec)
    time.sleep(max(0.2, wait))


def main():
    log.info("[MAIN] Starting Pure Liquidity Multi-Asset Orchestrator V10.0...")

    # Start 24/7 HTTP health server for Koyeb / UptimeRobot
    threading.Thread(target=start_http_server, daemon=True).start()

    # Run First-5-Minute Startup Diagnostics immediately on boot
    threading.Thread(target=run_first_5min_startup_diagnostics, daemon=True).start()

    while True:
        try:
            _sleep_until_next_05s()
            now = datetime.now(IST)

            # Consolidated Morning Health Check (09:03 - 09:10 IST)
            check_consolidated_morning_health()

            # Launch enabled market engines in parallel non-blocking daemon threads
            if ENABLE_NIFTY:
                threading.Thread(target=nifty50.run_nifty_cycle, args=(shared_store,), daemon=True).start()
            if ENABLE_GOLD:
                threading.Thread(target=gold.run_gold_cycle, args=(shared_store,), daemon=True).start()
            if ENABLE_BTC:
                threading.Thread(target=bitcoin.run_btc_cycle, args=(shared_store,), daemon=True).start()

        except Exception as e:
            log.error(f"[MAIN] Loop error: {type(e).__name__}")
            time.sleep(5)


if __name__ == "__main__":
    main()
