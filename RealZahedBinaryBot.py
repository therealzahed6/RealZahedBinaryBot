import time, threading, traceback, requests, math, json, os, re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple
import pandas as pd
import numpy as np
import telebot
from telebot import types
import warnings
warnings.filterwarnings('ignore')

# ============================== POSTGRESQL MEMORY BACKEND ==============================
# Requires: pip install psycopg2-binary
# Set DATABASE_URL env var in Railway → Add PostgreSQL plugin → copy DATABASE_URL
try:
    import psycopg2
    from psycopg2.extras import Json
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    print("[WARNING] psycopg2 not found. Falling back to file-based memory (NOT persistent on Railway).")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://neondb_owner:npg_imP5Mp6XqlsS@ep-purple-tree-ao9mh2wn-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

try:
    import ta
    HAS_TA = True
except ImportError:
    HAS_TA = False
    print("[WARNING] 'ta' library not found. Using simplified indicators.")

# ============================== CONFIG ==============================
IST = timezone(timedelta(hours=5, minutes=30))

TELEGRAM_BOT_TOKEN = "8985761250:AAEsZyAHDw0AiNjUJWDXxZb_e2hpDVFGxTw"

# AUTHORIZED USER - Your Chat ID
AUTHORIZED_CHAT_ID = 7963544891

# Admin contact for payment verification
ADMIN_USERNAME = "@therealzahed6"

# UPI ID for payments
UPI_ID = "zahedfxtrade@okicici"

# Payment plans
PAYMENT_PLANS = {
    "1_day": {"name": "1 Day Plan", "price": 199, "days": 1},
    "1_week": {"name": "1 Week Plan", "price": 999, "days": 7},
    "1_month": {"name": "1 Month Plan", "price": 3999, "days": 30},
}

TWELVE_KEYS = [
    "bc3f718fb5c2431c932ad77c5ec637fc",
    "70cb53f97a314aff980b64a85cdaaa46",
    "868ba693832a48d7b05dcd3aa041c109",
    "df9a5c2262e5416388f2e9dfd677c117",
    "2dbc3ae4903b4927a9d60561758485c4",
    "bbd69dec783948f88db1f8519bb69587",
]

NEWSDATA_API_KEY = "pub_e0795437a14e48a89e2174c0cd880ac0"  # newsdata.io API key

ALL_PAIRS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "EUR/GBP",
    "CAD/JPY", "AUD/USD", "GBP/JPY", "EUR/JPY"
]

MIN_SCORE = 7.0  # Balanced accuracy with reasonable signal frequency
MIN_CONFIRMATIONS = 3  # Require at least 3 confirmations for strong signals

# How many seconds before signal auto-rejects if no user response
SIGNAL_AUTO_REJECT_SECONDS = 60

# News warning: minutes before event to send the alert
NEWS_WARN_MINUTES_BEFORE = 30

# ============================== PERSISTENT MEMORY (PostgreSQL) ==============================
# Single key-value table: key TEXT PRIMARY KEY, value JSONB
# All existing code calls load_memory() / save_memory() — no other changes needed.

MEMORY_FILE = "bot_memory.json"   # fallback only
memory_lock = threading.Lock()
_db_conn = None
_db_conn_lock = threading.Lock()

def _get_db_conn():
    """Return a live psycopg2 connection, reconnecting if needed."""
    global _db_conn
    if not HAS_PSYCOPG2 or not DATABASE_URL:
        return None
    with _db_conn_lock:
        try:
            if _db_conn is None or _db_conn.closed:
                _db_conn = psycopg2.connect(DATABASE_URL, sslmode="require")
                _db_conn.autocommit = True
                # Create table if it doesn't exist
                with _db_conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS bot_kv (
                            key TEXT PRIMARY KEY,
                            value JSONB NOT NULL
                        );
                    """)
                print("[MEMORY] ✅ Connected to PostgreSQL")
        except Exception as e:
            print(f"[MEMORY] DB connect error: {e}")
            _db_conn = None
    return _db_conn

def _db_load() -> dict:
    """Load the single bot memory blob from PostgreSQL."""
    conn = _get_db_conn()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM bot_kv WHERE key = 'main';")
            row = cur.fetchone()
            if row:
                return dict(row[0]) if isinstance(row[0], dict) else json.loads(row[0])
            return {}
    except Exception as e:
        print(f"[MEMORY] DB load error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None

def _db_save(data: dict) -> bool:
    """Save the bot memory blob to PostgreSQL (upsert)."""
    conn = _get_db_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bot_kv (key, value) VALUES ('main', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
            """, (json.dumps(data),))
        return True
    except Exception as e:
        print(f"[MEMORY] DB save error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False

def load_memory() -> dict:
    """Load persistent memory — PostgreSQL first, file fallback."""
    # Try PostgreSQL
    if HAS_PSYCOPG2 and DATABASE_URL:
        result = _db_load()
        if result is not None:
            return result
        print("[MEMORY] ⚠️ DB load failed, trying file fallback...")

    # File fallback
    try:
        if os.path.exists(MEMORY_FILE):
            with open(MEMORY_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        print(f"[MEMORY] File load error: {e}")
    return {}

def save_memory(data: dict):
    """Save persistent memory — PostgreSQL first, file fallback."""
    # Try PostgreSQL
    if HAS_PSYCOPG2 and DATABASE_URL:
        if _db_save(data):
            return   # Success — no need for file write
        print("[MEMORY] ⚠️ DB save failed, falling back to file...")

    # File fallback (atomic write)
    try:
        tmp = MEMORY_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, MEMORY_FILE)
    except Exception as e:
        print(f"[MEMORY] File save error: {e}")

def save_capital(capital: float):
    """Save current capital to persistent memory (global + today's starting capital)."""
    with memory_lock:
        mem = load_memory()
        mem["capital"] = capital
        # Record as starting capital for today if not already set
        today = get_today_key()
        day = mem.setdefault("days", {}).setdefault(today, {})
        if "starting_capital" not in day:
            day["starting_capital"] = capital
        save_memory(mem)

def set_todays_starting_capital(capital: float):
    """Explicitly set today's starting capital (called when user enters capital at day start)."""
    with memory_lock:
        mem = load_memory()
        mem["capital"] = capital
        today = get_today_key()
        day = mem.setdefault("days", {}).setdefault(today, {})
        # Always overwrite — this is the explicit day-start capital
        day["starting_capital"] = capital
        save_memory(mem)

def update_day_ending_capital(capital: float, date_key: str = None):
    """Update the ending capital for a given day (called after /done)."""
    with memory_lock:
        mem = load_memory()
        today = date_key or get_today_key()
        day = mem.setdefault("days", {}).setdefault(today, {})
        day["ending_capital"] = capital
        # Also update global capital
        mem["capital"] = capital
        save_memory(mem)

def record_withdrawal(amount: float, capital_before: float, capital_after: float, date_key: str = None):
    """Save a withdrawal entry for today."""
    with memory_lock:
        mem = load_memory()
        today = date_key or get_today_key()
        day = mem.setdefault("days", {}).setdefault(today, {})
        withdrawals = day.setdefault("withdrawals", [])
        withdrawals.append({
            "amount": amount,
            "capital_before": capital_before,
            "capital_after": capital_after,
            "time_ist": now_ist().strftime("%H:%M"),
        })
        # Also update ending capital to capital_after
        day["ending_capital"] = capital_after
        mem["capital"] = capital_after
        save_memory(mem)

def get_withdrawals_for_date(date_key: str) -> list:
    with memory_lock:
        mem = load_memory()
        return mem.get("days", {}).get(date_key, {}).get("withdrawals", [])

def get_day_data_for_range(start_key: str, end_key: str) -> list:
    """Return list of per-day dicts sorted by date, within the given range."""
    with memory_lock:
        mem = load_memory()
        days = mem.get("days", {})
    start_d = datetime.strptime(start_key, "%Y-%m-%d").date()
    end_d   = datetime.strptime(end_key,   "%Y-%m-%d").date()
    result = []
    for k in sorted(days.keys()):
        try:
            d = datetime.strptime(k, "%Y-%m-%d").date()
            if start_d <= d <= end_d:
                result.append({"date_key": k, "date": d, **days[k]})
        except ValueError:
            pass
    return result

def load_capital() -> Optional[float]:
    """Load last saved capital from persistent memory."""
    with memory_lock:
        mem = load_memory()
        val = mem.get("capital")
        if val is not None:
            try:
                return float(val)
            except Exception:
                return None
    return None

def get_capital_for_date(date_key: str) -> dict:
    """Return starting/ending capital recorded for a specific date."""
    with memory_lock:
        mem = load_memory()
        day = mem.get("days", {}).get(date_key, {})
        return {
            "starting": day.get("starting_capital"),
            "ending": day.get("ending_capital"),
        }

def save_martingale_state(consecutive_losses: int, stoploss_hit: bool):
    """Persist martingale consecutive_losses and stoploss_hit so /stop + /pairs doesn't wipe them."""
    with memory_lock:
        mem = load_memory()
        today = get_today_key()
        day = mem.setdefault("days", {}).setdefault(today, {})
        day["consecutive_losses"] = consecutive_losses
        day["stoploss_hit"] = stoploss_hit
        save_memory(mem)

def load_martingale_state() -> dict:
    """Load persisted martingale state for today. Returns defaults if not set."""
    with memory_lock:
        mem = load_memory()
        today = get_today_key()
        day = mem.get("days", {}).get(today, {})
        return {
            "consecutive_losses": day.get("consecutive_losses", 0),
            "stoploss_hit": day.get("stoploss_hit", False),
        }

def save_pair_losses(pair_losses: dict):
    """Persist per-pair consecutive loss counters to memory."""
    with memory_lock:
        mem = load_memory()
        today = get_today_key()
        day = mem.setdefault("days", {}).setdefault(today, {})
        day["pair_consecutive_losses"] = pair_losses
        save_memory(mem)

def load_pair_losses() -> dict:
    """Load per-pair consecutive loss counters for today."""
    with memory_lock:
        mem = load_memory()
        today = get_today_key()
        day = mem.get("days", {}).get(today, {})
        return dict(day.get("pair_consecutive_losses", {}))

def get_capital_for_range(start_key: str, end_key: str) -> dict:
    """Return earliest starting capital and latest ending capital across a date range."""
    with memory_lock:
        mem = load_memory()
        days = mem.get("days", {})
    from datetime import date as _date
    start_d = datetime.strptime(start_key, "%Y-%m-%d").date()
    end_d   = datetime.strptime(end_key,   "%Y-%m-%d").date()
    sorted_keys = sorted(
        [k for k in days if start_d <= datetime.strptime(k, "%Y-%m-%d").date() <= end_d]
    )
    starting = None
    ending   = None
    for k in sorted_keys:
        d = days[k]
        if starting is None and d.get("starting_capital") is not None:
            starting = float(d["starting_capital"])
        if d.get("ending_capital") is not None:
            ending = float(d["ending_capital"])
    return {"starting": starting, "ending": ending}

def get_today_key() -> str:
    """Return today's date string in IST as YYYY-MM-DD."""
    return now_ist().strftime("%Y-%m-%d")

def get_signal_no_for_today() -> int:
    with memory_lock:
        mem = load_memory()
        today = get_today_key()
        day_data = mem.get("days", {}).get(today, {})
        return day_data.get("next_signal_no", 1)

def save_signal_no_for_today(next_no: int):
    """Persist the next signal number for today."""
    with memory_lock:
        mem = load_memory()
        today = get_today_key()
        mem.setdefault("days", {}).setdefault(today, {})
        mem["days"][today]["next_signal_no"] = next_no
        save_memory(mem)

def record_signal(signal_no: int, pair: str, direction: str, entry_ist: str,
                  expiry_ist: str, expiry_minutes: int, score: float, date_key: str = None):
    """Save a new accepted signal to memory."""
    with memory_lock:
        mem = load_memory()
        today = date_key or get_today_key()
        day = mem.setdefault("days", {}).setdefault(today, {})
        signals = day.setdefault("signals", {})
        signals[str(signal_no)] = {
            "no": signal_no,
            "pair": pair,
            "direction": direction,
            "entry_ist": entry_ist,
            "expiry_ist": expiry_ist,
            "expiry_minutes": expiry_minutes,
            "score": score,
            "result": None,
            "accepted": True,
        }
        save_memory(mem)

def update_signal_result(signal_no: int, result: str, date_key: str = None):
    """Update a signal's result (profit/loss) in memory."""
    with memory_lock:
        mem = load_memory()
        today = date_key or get_today_key()
        try:
            mem["days"][today]["signals"][str(signal_no)]["result"] = result
            save_memory(mem)
            return True
        except (KeyError, TypeError):
            return False

def get_signals_for_date(date_key: str) -> dict:
    with memory_lock:
        mem = load_memory()
        day = mem.get("days", {}).get(date_key, {})
        return day.get("signals", {})

def get_signals_for_range(start_date: str, end_date: str) -> dict:
    with memory_lock:
        mem = load_memory()
        days = mem.get("days", {})
    result = {}
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    for key, val in days.items():
        try:
            d = datetime.strptime(key, "%Y-%m-%d").date()
            if start <= d <= end:
                for sno, sdata in val.get("signals", {}).items():
                    result[f"{key}_{sno}"] = sdata
        except ValueError:
            pass
    return result


# ============================== TELEGRAM BOT ==================================
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN, parse_mode="HTML")

# ============================== SIMPLE STATE ==================================
# Threading Locks for Safety
state_lock = threading.Lock()

STATE = {
    "signal_no": 1,
    "selected_pairs": [],
    "bot_running": False,
    "active_signal": None,
    "telegram_chat_id": AUTHORIZED_CHAT_ID,
    # News filter state
    "news_events_today": [],
    "news_warned_ids": set(),
    "news_signal_sent_ids": set(),
    "news_cache_date": None,
    # Pending result queries: signal_no -> True
    "pending_results": {},
    # Pending signals awaiting Accept/Reject
    "pending_signals": {},
    # Waiting for result after expiry
    "waiting_for_result": False,
    # Internal flags
    "_counter_date": "",
    # Watchdog
    "_scheduler_last_heartbeat": time.time(),
    "_news_monitor_last_heartbeat": time.time(),
    # Strategy: no martingale, user manages money themselves
    "strategy": "manual",
    "awaiting_strategy": False,
    "awaiting_capital": False, # True when bot is waiting for user to input capital
    "awaiting_done_capital": False,  # True when /done was used and bot wants new capital
    "_next_scan_after": 0.0,          # Epoch timestamp — scheduler waits until this time before next scan
    # Per-pair consecutive loss tracking (for pair blocking after 2 losses in a row)
    "pair_consecutive_losses": {},  # {pair: int} — resets to 0 on profit for that pair
    
    # === NEW AUTO-LEARNING STORAGE ===
    "pair_performance": {},  # Format: { pair: {"wins": 0, "losses": 0} }
    "hour_performance": {},  # Format: { hour: {"wins": 0, "losses": 0} }
}


# ============================== SESSION STATS ==============================
_session_stats_lock = threading.Lock()
_session_stats = {
    "wins": 0,
    "losses": 0,
    "consecutive_losses": 0,
    "total_signals": 0,
    "total_pnl": 0.0,
}

def update_session_stats(result: str, trade_amount: float = 0.0):
    """Update in-memory session stats after each trade result."""
    with _session_stats_lock:
        if result == "profit":
            _session_stats["wins"] += 1
            _session_stats["consecutive_losses"] = 0
            _session_stats["total_pnl"] += trade_amount * 0.85
        elif result == "loss":
            _session_stats["losses"] += 1
            _session_stats["consecutive_losses"] += 1
            _session_stats["total_pnl"] -= trade_amount
        if result in ("profit", "loss"):
            _session_stats["total_signals"] += 1

def reset_session_stats():
    """Reset session stats — called at start of new trading day."""
    with _session_stats_lock:
        _session_stats["wins"] = 0
        _session_stats["losses"] = 0
        _session_stats["consecutive_losses"] = 0
        _session_stats["total_signals"] = 0
        _session_stats["total_pnl"] = 0.0


# ============================== AUTO-LEARNING & VOICE EXTENSIONS ==============================
def update_auto_learning(pair: str, result: str, entry_ist: str):
    """Saves wins/losses per currency pair and per trading hour."""
    with state_lock:
        try:
            hour = int(entry_ist.split(":")[0])
        except Exception:
            hour = now_ist().hour

        hour_key = str(hour)
        pair_data = STATE.setdefault("pair_performance", {}).setdefault(pair, {"wins": 0, "losses": 0})
        hour_data = STATE.setdefault("hour_performance", {}).setdefault(hour_key, {"wins": 0, "losses": 0})

        if result == "profit":
            pair_data["wins"] += 1
            hour_data["wins"] += 1
        elif result == "loss":
            pair_data["losses"] += 1
            hour_data["losses"] += 1

def apply_auto_learning_bonus(pair: str, base_score: float) -> float:
    """Adjusts signal score dynamically using historical win-rate stats."""
    with state_lock:
        current_hour = str(now_ist().hour)
        pair_data = STATE.get("pair_performance", {}).get(pair, {"wins": 0, "losses": 0})
        hour_data = STATE.get("hour_performance", {}).get(current_hour, {"wins": 0, "losses": 0})
        
        bonus = 0.0
        
        # Adjust score for pair performance
        total_pair = pair_data["wins"] + pair_data["losses"]
        if total_pair >= 3:
            win_rate_pair = pair_data["wins"] / total_pair
            if win_rate_pair >= 0.70:
                bonus += 0.5
            elif win_rate_pair <= 0.40:
                bonus -= 0.5
                
        # Adjust score for time performance
        total_hour = hour_data["wins"] + hour_data["losses"]
        if total_hour >= 3:
            win_rate_hour = hour_data["wins"] / total_hour
            if win_rate_hour >= 0.70:
                bonus += 0.3
            elif win_rate_hour <= 0.40:
                bonus -= 0.3
                
        return round(min(10.0, base_score + bonus), 2)



# ============================== WATCHDOG ==============================
WATCHDOG_TIMEOUT = 180  # seconds — restart thread if no heartbeat

def watchdog_loop():
    """Monitors scheduler and news threads. Restarts them if they hang."""
    global scheduler_thread, news_thread
    print("[WATCHDOG] Started")
    time.sleep(60)  # Give threads time to start
    while True:
        try:
            now_ts = time.time()
            with state_lock:
                sched_hb = STATE.get("_scheduler_last_heartbeat", now_ts)
                news_hb = STATE.get("_news_monitor_last_heartbeat", now_ts)

            if now_ts - sched_hb > WATCHDOG_TIMEOUT:
                print(f"[WATCHDOG] ⚠️ Scheduler thread appears frozen! Restarting...")
                try:
                    new_thread = threading.Thread(target=scheduler_loop, daemon=True, name="scheduler")
                    new_thread.start()
                    with state_lock:
                        STATE["_scheduler_last_heartbeat"] = time.time()
                    print("[WATCHDOG] ✅ Scheduler thread restarted")
                    try:
                        bot.send_message(
                            AUTHORIZED_CHAT_ID,
                            "⚠️ <b>Bot Auto-Recovery</b>\n\nScheduler was frozen and has been automatically restarted.\nBot is now back to scanning."
                        )
                    except Exception:
                        pass
                except Exception as e:
                    print(f"[WATCHDOG] Failed to restart scheduler: {e}")

            if now_ts - news_hb > WATCHDOG_TIMEOUT * 2:
                print(f"[WATCHDOG] ⚠️ News monitor thread appears frozen! Restarting...")
                try:
                    new_thread = threading.Thread(target=news_monitor_loop, daemon=True, name="news_monitor")
                    new_thread.start()
                    with state_lock:
                        STATE["_news_monitor_last_heartbeat"] = time.time()
                    print("[WATCHDOG] ✅ News monitor thread restarted")
                except Exception as e:
                    print(f"[WATCHDOG] Failed to restart news monitor: {e}")

        except Exception as e:
            print(f"[WATCHDOG] Error: {e}")

        time.sleep(30)

# ============================== KEY ROTATION + RATE LIMITER ==================================
KEY_CREDITS_PER_MIN = 7
key_lock = threading.Lock()
key_usage = {k: {"used": 0, "window_start": time.time()} for k in TWELVE_KEYS}

def get_next_key():
    while True:
        with key_lock:
            now_ts = time.time()
            earliest_reset = float('inf')
            for key in TWELVE_KEYS:
                info = key_usage[key]
                if now_ts - info["window_start"] >= 60:
                    info["used"] = 0
                    info["window_start"] = now_ts
                if info["used"] < KEY_CREDITS_PER_MIN:
                    info["used"] += 1
                    return key
                # Track earliest window reset for waiting
                secs_until_reset = 60 - (now_ts - info["window_start"])
                if secs_until_reset < earliest_reset:
                    earliest_reset = secs_until_reset

        wait = max(1, earliest_reset + 1)
        print(f"[API] All keys rate-limited. Waiting {wait:.0f}s for reset...")
        time.sleep(wait)

# ============================== DATA CACHE ==================================
_data_cache: Dict[str, Dict] = {}
_cache_lock = threading.Lock()
CACHE_TTL_SECONDS = 50  # Must be less than the 55s minimum scan interval so fresh candles are always fetched

def get_cached_or_fetch(symbol: str, interval: str, outputsize: int = 200):
    cache_key = f"{symbol}:{interval}"
    now_ts = time.time()
    with _cache_lock:
        entry = _data_cache.get(cache_key)
        if entry and (now_ts - entry["ts"]) < CACHE_TTL_SECONDS:
            age = now_ts - entry["ts"]
            if age < 5:  # Only print cache hit if very recent (suppress spam)
                print(f"[CACHE] {symbol} {interval} reused (age {age:.0f}s)")
            return entry["df"]
    df = td_time_series(symbol, interval, outputsize)
    if df is not None:
        with _cache_lock:
            _data_cache[cache_key] = {"df": df, "ts": time.time()}
    return df

# ============================== TIME UTILS ==================================
def now_utc():
    return datetime.now(timezone.utc)

def now_ist():
    return now_utc().astimezone(IST)

def ist_hhmm(dt_utc: datetime) -> str:
    return dt_utc.astimezone(IST).strftime("%H:%M")

def ist_weekend(dt_utc: Optional[datetime] = None) -> bool:
    return (dt_utc or now_utc()).astimezone(IST).weekday() >= 5

def get_next_candle_open() -> datetime:
    now = now_utc()
    next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return next_minute

def is_trading_hours() -> bool:
    current_hour = now_ist().hour
    if 2 <= current_hour < 6:
        return False
    return True

# ============================== MARTINGALE MONEY MANAGEMENT ==============================

MARTINGALE_PLAN = [
    (8,      79,     [1,   2,   4,   9]),   # Low-capital tier: $8–$79
    (10,     207,    [1,   2,   4,   9]),
    (208,    319,    [2,   4,   8,   18]),
    (320,    415,    [3,   6,   12,  27]),
    (416,    543,    [4,   8,   16,  36]),
    (544,    623,    [5,   10,  20,  45]),
    (624,    767,    [6,   12,  24,  54]),
    (768,    823,    [7,   14,  28,  63]),
    (824,    1079,   [8,   16,  32,  72]),
    (1080,   1559,   [10,  20,  40,  90]),
    (1560,   2159,   [15,  30,  60,  135]),
    (2160,   2639,   [20,  40,  80,  180]),
    (2640,   3239,   [25,  50,  100, 225]),
    (3240,   4199,   [30,  60,  120, 270]),
    (4200,   5159,   [40,  80,  160, 360]),
    (5160,   6359,   [50,  100, 200, 450]),
    (6360,   7319,   [60,  120, 240, 540]),
    (7320,   8439,   [70,  140, 280, 630]),
    (8440,   10999,  [80,  160, 320, 720]),
    (11000,  15599,  [100, 200, 400, 900]),
    (15600,  21599,  [150, 300, 600, 1350]),
    (21600,  26399,  [200, 400, 800, 1800]),
    (26400,  32399,  [250, 500, 1000, 2250]),
    (32400,  41999,  [300, 600, 1200, 2700]),
]

# ============================== DAILY MONEY MANAGEMENT PLAN ==============================
# Each entry: (capital, initial_trade, stoploss_capital, target_capital)
# Steps increase by the initial_trade amount each row within a tier.

def _build_mm_table() -> list:
    """
    Build the full money management table as a list of tuples:
    (capital, trade_amount, stoploss_capital, target_capital)

    Formula per row:
      stoploss = capital - trade * 16   (worst-case 4-step martingale: 1+2+4+9 = 16 units)
      target   = capital + trade * 8    (~80% binary payout on initial trade)
    Step between rows within a tier = trade * 8
    (each row's target becomes the next row's starting capital)
    """
    tiers = [
        # (start_capital, trade_amount, last_capital_in_tier)
        (8,     1,   72),    # $1  tier (low): 8, 16, 24 ... 72      -> target 80
        (80,    1,   200),   # $1  tier: 80, 88, 96 ... 200    -> target 208
        (208,   2,   304),   # $2  tier: 208, 224 ... 304      -> target 320
        (320,   3,   392),   # $3  tier: 320, 344 ... 392      -> target 416
        (416,   4,   512),   # $4  tier: 416, 448 ... 512      -> target 544
        (544,   5,   584),   # $5  tier: 544, 584              -> target 624
        (624,   6,   720),   # $6  tier: 624, 672, 720         -> target 768
        (768,   7,   768),   # $7  tier: 768 only              -> target 824
        (824,   8,   1016),  # $8  tier: 824, 888 ... 1016     -> target 1080
        (1080,  10,  1480),  # $10 tier: 1080, 1160 ... 1480   -> target 1560
        (1560,  15,  2040),  # $15 tier: 1560, 1680 ... 2040   -> target 2160
        (2160,  20,  2480),  # $20 tier: 2160, 2320, 2480      -> target 2640
        (2640,  25,  3040),  # $25 tier: 2640, 2840, 3040      -> target 3240
        (3240,  30,  3960),  # $30 tier: 3240, 3480 ... 3960   -> target 4200
        (4200,  40,  4840),  # $40 tier: 4200, 4520, 4840      -> target 5160
        (5160,  50,  5960),  # $50 tier: 5160, 5560, 5960      -> target 6360
        (6360,  60,  6840),  # $60 tier: 6360, 6840            -> target 7320
        (7320,  70,  7880),  # $70 tier: 7320, 7880            -> target 8440
        (8440,  80,  10360), # $80 tier: 8440, 8600 ... 10360  -> target 11000
        (11000, 100, 15000), # $100 tier: 11000 ... 15000      -> target 15800
        (15800, 150, 20600), # $150 tier: 15800 ... 20600      -> target 21800
        (21800, 200, 31400), # $200 tier: 21800 ... 31400      -> target 33000
        (33000, 300, 42600), # $300 tier: 33000 ... 42600      -> target 45000
    ]

    rows = []
    seen = set()
    for (start, trade, last_cap) in tiers:
        cap = float(start)
        step = float(trade * 8)
        while True:
            key = round(cap, 2)
            if key not in seen:
                seen.add(key)
                rows.append((key, trade,
                              round(cap - trade * 16, 2),
                              round(cap + trade * 8,  2)))
            if abs(cap - last_cap) < 0.01:
                break
            cap = round(cap + step, 2)

    rows.sort(key=lambda x: x[0])
    return rows

_MM_TABLE = _build_mm_table()

def get_mm_for_capital(capital: float):
    """
    Return (trade_amount, stoploss_capital, target_capital) for an exact capital match.
    Returns None if capital is not a valid MM entry.
    """
    cap_rounded = round(float(capital), 2)
    for (c, trade, sl, tp) in _MM_TABLE:
        if abs(c - cap_rounded) < 0.01:
            return {"trade": trade, "stoploss": sl, "target": tp, "capital": c}
    return None

def get_nearest_valid_capital(capital: float):
    """
    Find the nearest valid MM capital at or below the given amount.
    Used for STARTING capital validation.
    Returns (valid_capital, withdraw_amount) tuple.
    If capital is already valid, withdraw_amount == 0.
    """
    cap = float(capital)
    best = None
    for (c, trade, sl, tp) in _MM_TABLE:
        if c <= cap + 0.01:
            best = (c, trade, sl, tp)
    if best is None:
        return None, None
    withdraw = round(cap - best[0], 2)
    return best[0], withdraw

def get_nearest_valid_ending_capital(ending_cap: float, mm_plan: dict):
    """Work out what the user should do with their ending session capital."""
    cap = float(ending_cap)
    sl  = float(mm_plan["stoploss"])
    tp  = float(mm_plan["target"])

    if cap >= tp:
        withdraw = round(cap - tp, 2)
        return {
            "status": "above_target",
            "withdraw": withdraw,
            "next_valid_cap": tp,
            "extra_above_target": round(cap - tp, 2),
            "extra_below_sl": 0.0,
        }
    elif cap <= sl:
        nearest, withdraw_amt = get_nearest_valid_capital(cap)
        if nearest is None:
            nearest = sl
            withdraw_amt = 0.0
        return {
            "status": "below_sl",
            "withdraw": max(0.0, round(withdraw_amt, 2)),
            "next_valid_cap": nearest,
            "extra_above_target": 0.0,
            "extra_below_sl": round(sl - cap, 2),
        }
    else:
        nearest, withdraw_amt = get_nearest_valid_capital(cap)
        if nearest is None:
            nearest = cap
            withdraw_amt = 0.0
        return {
            "status": "between",
            "withdraw": max(0.0, round(withdraw_amt, 2)),
            "next_valid_cap": nearest,
            "extra_above_target": 0.0,
            "extra_below_sl": 0.0,
        }

def format_mm_info(mm: dict) -> str:
    """Format the money management info for display in Telegram."""
    return (
        f"💰 <b>Today's Money Management Plan</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 <b>Starting Capital:</b> ${mm['capital']:,.0f}\n"
        f"🎯 <b>Initial Trade Size:</b> ${mm['trade']}\n"
        f"🟢 <b>Target:</b> ${mm['target']:,.0f} (+${mm['target']-mm['capital']:,.0f})\n"
        f"🔴 <b>Stop Loss:</b> ${mm['stoploss']:,.0f} (-${mm['capital']-mm['stoploss']:,.0f})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Stop trading when you hit target or stoploss.\nUse /done to record your ending capital.</i>"
    )

def _save_todays_mm(mm: dict):
    """Persist today's MM plan (SL/TP) for use in /done comparison."""
    with memory_lock:
        mem = load_memory()
        today = get_today_key()
        day = mem.setdefault("days", {}).setdefault(today, {})
        day["mm_plan"] = {
            "capital": mm["capital"],
            "trade": mm["trade"],
            "stoploss": mm["stoploss"],
            "target": mm["target"],
        }
        save_memory(mem)

def _load_todays_mm() -> dict:
    """Load today's MM plan from memory. Returns None if not set."""
    with memory_lock:
        mem = load_memory()
        today = get_today_key()
        return mem.get("days", {}).get(today, {}).get("mm_plan")

def get_martingale_amount(capital: float, consecutive_losses: int) -> float:
    """Return the trade amount based on capital and consecutive losses (0-3)."""
    step = min(consecutive_losses, 3)  # max 4 steps (index 0-3)
    for low, high, amounts in MARTINGALE_PLAN:
        if low <= capital <= high:
            return float(amounts[step])
    return float(MARTINGALE_PLAN[-1][2][step])

def get_martingale_sequence(capital: float) -> list:
    """Return the full [step1, step2, step3, step4] sequence for the given capital."""
    for low, high, amounts in MARTINGALE_PLAN:
        if low <= capital <= high:
            return amounts
    return MARTINGALE_PLAN[-1][2]

def check_market_health_and_notify(chat_id: int, capital: float = None, mm_plan: dict = None) -> bool:
    """Check session accuracy and notify if low. Returns False (no forced pause)."""
    should_pause = False
    with _session_stats_lock:
        wins = _session_stats["wins"]
        losses = _session_stats["losses"]
        total_sigs = _session_stats["total_signals"]

    if total_sigs >= 5 and (wins + losses) > 0:
        acc = wins / (wins + losses)
        if acc < 0.40:
            safe_send(chat_id,
                "📉 <b>Low Accuracy Warning</b>\n\n"
                "Session accuracy is only <b>%d%%</b> (%dW/%dL).\n\n"
                "Market may not be syncing with signals right now.\n"
                "⚠️ <b>Consider pausing 30 minutes</b> before next trade.\n"
                "🧘 Sometimes the best trade is no trade." % (int(acc*100), wins, losses))
    return should_pause

# ============================== QUOTEX LIVE/OTC MARKET CHECKER ==============================

def check_quotex_market_status(pairs: list) -> dict:
    now = now_utc()
    weekday = now.weekday()
    day_name = now.strftime("%A")
    is_weekend = weekday >= 5
    all_otc = is_weekend
    message = ""
    if all_otc:
        reason = f"{day_name} (Weekend)"
        detail = (
            "Forex markets are closed on weekends.\n"
            "All pairs are currently <b>OTC only</b> on Quotex — "
            "prices are broker-simulated, not real market data.\n\n"
            "🔁 Live market resumes on <b>Monday</b>."
        )
        message = (
            f"🟥 <b>Market Closed — {reason}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📅 <b>Today:</b> {now.astimezone(IST).strftime('%A, %d %B %Y')}\n\n"
            f"⚠️ {detail}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 <i>Signals are generated using real TwelveData market feeds. "
            f"Running the bot on OTC pairs will produce mismatched signals. "
            f"It is recommended to wait for live market hours.</i>\n\n"
            f"⏹️ Bot has been stopped. Use /pairs when live market opens."
        )
    return {
        "all_otc": all_otc,
        "is_weekend": is_weekend,
        "day_name": day_name,
        "message": message,
    }


# ============================== TWELVE DATA API ===============================
TD_BASE = "https://api.twelvedata.com"

def td_time_series(symbol: str, interval: str, outputsize: int = 200):
    max_attempts = 2
    for attempt in range(max_attempts):
        key = get_next_key()
        time.sleep(1.5)
        try:
            params = {
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "dp": 5,
                "apikey": key
            }
            r = requests.get(f"{TD_BASE}/time_series", params=params, timeout=10)
            if r.status_code != 200:
                print(f"[API] {symbol} HTTP {r.status_code}")
                continue
            data = r.json()
            if "status" in data and data["status"] == "error":
                print(f"[API] {symbol} Error: {data.get('message', 'Unknown')}")
                continue
            if "values" not in data or not data["values"]:
                print(f"[API] {symbol} No values in response")
                continue
            df = pd.DataFrame(data["values"])
            for col in ["open", "high", "low", "close"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["open", "high", "low", "close"])
            if len(df) == 0:
                print(f"[API] {symbol} All data invalid after cleaning")
                continue
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
                df = df.dropna(subset=["datetime"])
            df = df.sort_values("datetime").reset_index(drop=True)
            if len(df) < 20:
                print(f"[API] {symbol} Insufficient data: {len(df)} rows")
                continue
            print(f"[API] ✓ {symbol} fetched: {len(df)} candles")
            return df
        except Exception as e:
            print(f"[API] {symbol} attempt {attempt+1} failed: {e}")
            continue
    print(f"[API] ✗ {symbol} failed all attempts")
    return None

# ============================== ENHANCED TECHNICALS ====================================
def safe_ema(series: pd.Series, span: int) -> pd.Series:
    try:
        if len(series) < span:
            span = max(2, len(series) // 2)
        return series.ewm(span=span, adjust=False).mean().bfill()
    except Exception:
        return series.fillna(series.mean())

def safe_sma(series: pd.Series, period: int) -> pd.Series:
    try:
        if len(series) < period:
            period = max(2, len(series) // 2)
        return series.rolling(window=period).mean().bfill()
    except Exception:
        return series.fillna(series.mean())

def enhanced_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    try:
        if HAS_TA:
            return ta.momentum.RSIIndicator(series, window=period).rsi().fillna(50)
        else:
            delta = series.diff()
            gain = (delta.where(delta > 0, 0.0)).rolling(period).mean()
            loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
            rs = gain / (loss.replace(0, np.nan))
            rsi = 100 - (100 / (1 + rs))
            return rsi.fillna(50.0)
    except Exception:
        return pd.Series([50] * len(series), index=series.index)

def enhanced_macd(series: pd.Series, fast=12, slow=26, signal=9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    try:
        if HAS_TA:
            macd_indicator = ta.trend.MACD(series, window_fast=fast, window_slow=slow, window_sign=signal)
            macd_line = macd_indicator.macd().fillna(0)
            signal_line = macd_indicator.macd_signal().fillna(0)
            histogram = macd_indicator.macd_diff().fillna(0)
            return macd_line, signal_line, histogram
        else:
            ema_fast = safe_ema(series, fast)
            ema_slow = safe_ema(series, slow)
            macd_line = ema_fast - ema_slow
            signal_line = safe_ema(macd_line, signal)
            histogram = macd_line - signal_line
            return macd_line.fillna(0), signal_line.fillna(0), histogram.fillna(0)
    except Exception:
        zeros = pd.Series([0] * len(series), index=series.index)
        return zeros, zeros, zeros

def enhanced_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    try:
        if HAS_TA:
            return ta.volatility.AverageTrueRange(df["high"], df["low"], df["close"], window=period).average_true_range().fillna(0.0001)
        else:
            high = df["high"].astype(float)
            low = df["low"].astype(float)
            close = df["close"].astype(float)
            prev_close = close.shift(1)
            tr1 = high - low
            tr2 = (high - prev_close).abs()
            tr3 = (low - prev_close).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(period).mean()
            return atr.fillna(0.0001)
    except Exception:
        return pd.Series([0.0001] * len(df), index=df.index)

# ============================== NEW ACCURACY FILTERS ==============================

def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Average Directional Index — measures trend STRENGTH (not direction).
    ADX > 25 = strong trend (good for signals)
    ADX < 20 = weak/ranging market (avoid signals)
    """
    try:
        if HAS_TA:
            adx = ta.trend.ADXIndicator(df["high"], df["low"], df["close"], window=period)
            return adx.adx().fillna(0)
        else:
            high = df["high"].astype(float)
            low  = df["low"].astype(float)
            close = df["close"].astype(float)
            prev_high  = high.shift(1)
            prev_low   = low.shift(1)
            prev_close = close.shift(1)
            dm_plus  = (high - prev_high).clip(lower=0)
            dm_minus = (prev_low - low).clip(lower=0)
            dm_plus  = dm_plus.where(dm_plus > dm_minus, 0)
            dm_minus = dm_minus.where(dm_minus > dm_plus, 0)
            tr1 = high - low
            tr2 = (high - prev_close).abs()
            tr3 = (low - prev_close).abs()
            tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr_s   = tr.rolling(period).mean()
            di_plus  = 100 * (dm_plus.rolling(period).mean()  / atr_s.replace(0, np.nan))
            di_minus = 100 * (dm_minus.rolling(period).mean() / atr_s.replace(0, np.nan))
            dx = (100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan))
            adx_series = dx.rolling(period).mean()
            return adx_series.fillna(0)
    except Exception as e:
        print(f"[ADX] Calculation error: {e}")
        return pd.Series([0] * len(df), index=df.index)


def check_adx_filter(df: pd.DataFrame, direction: str) -> dict:
    """
    Returns whether ADX confirms the signal direction.
    Also returns DI+ vs DI- to confirm directional alignment.
    """
    try:
        adx_series = calculate_adx(df, 14)
        adx_val = adx_series.iloc[-1]

        if HAS_TA:
            adx_ind = ta.trend.ADXIndicator(df["high"], df["low"], df["close"], window=14)
            di_plus  = adx_ind.adx_pos().iloc[-1]
            di_minus = adx_ind.adx_neg().iloc[-1]
        else:
            high = df["high"].astype(float)
            low  = df["low"].astype(float)
            close = df["close"].astype(float)
            prev_high  = high.shift(1)
            prev_low   = low.shift(1)
            prev_close = close.shift(1)
            dm_plus  = (high - prev_high).clip(lower=0)
            dm_minus = (prev_low - low).clip(lower=0)
            dm_plus  = dm_plus.where(dm_plus > dm_minus, 0)
            dm_minus = dm_minus.where(dm_minus > dm_plus, 0)
            tr  = pd.concat([high-low, (high-prev_close).abs(), (low-prev_close).abs()], axis=1).max(axis=1)
            atr_s   = tr.rolling(14).mean()
            di_plus  = (100 * dm_plus.rolling(14).mean()  / atr_s.replace(0, np.nan)).iloc[-1]
            di_minus = (100 * dm_minus.rolling(14).mean() / atr_s.replace(0, np.nan)).iloc[-1]

        trend_strong   = adx_val >= 25
        trend_moderate = adx_val >= 18
        directional_ok = (direction == "CALL" and di_plus > di_minus) or \
                         (direction == "PUT"  and di_minus > di_plus)

        if trend_strong and directional_ok:
            score_add = 1.2
            label = f"ADX Strong Trend ({adx_val:.1f})"
        elif trend_moderate and directional_ok:
            score_add = 0.7
            label = f"ADX Moderate Trend ({adx_val:.1f})"
        elif adx_val < 18:
            score_add = -1.0   # penalise weak/choppy trend
            label = f"ADX Weak ({adx_val:.1f}) — ranging market"
        else:
            score_add = 0.0
            label = f"ADX Neutral ({adx_val:.1f})"

        return {
            "adx": round(adx_val, 1),
            "di_plus": round(float(di_plus), 1),
            "di_minus": round(float(di_minus), 1),
            "trend_strong": trend_strong,
            "trend_moderate": trend_moderate,
            "directional_ok": directional_ok,
            "score_add": score_add,
            "label": label,
            "pass_filter": adx_val >= 18,   # hard reject below 18
        }
    except Exception as e:
        print(f"[ADX] Filter error: {e}")
        return {"adx": 0, "trend_strong": False, "trend_moderate": False,
                "directional_ok": False, "score_add": 0, "label": "ADX Error",
                "pass_filter": True}   # don't block on error


def check_volume_confirmation(df: pd.DataFrame, direction: str) -> dict:
    """
    Volume analysis — confirms whether the move has real strength.
    Compares current candle volume to the recent average.
    Higher volume on signal candle = stronger confirmation.
    """
    try:
        if "volume" not in df.columns:
            return {"confirmed": False, "score_add": 0, "label": "No Volume Data", "ratio": 0}

        vol = df["volume"].astype(float)
        if vol.sum() == 0:
            return {"confirmed": False, "score_add": 0, "label": "No Volume Data", "ratio": 0}

        avg_vol   = vol.iloc[-20:].mean()
        curr_vol  = vol.iloc[-1]
        prev_vol  = vol.iloc[-2]
        ratio     = curr_vol / avg_vol if avg_vol > 0 else 1.0

        # Check if price moved in signal direction with volume
        curr_close = df["close"].iloc[-1]
        curr_open  = df["open"].iloc[-1]
        bullish_candle = curr_close > curr_open
        bearish_candle = curr_close < curr_open

        direction_matches = (direction == "CALL" and bullish_candle) or \
                            (direction == "PUT"  and bearish_candle)

        if ratio >= 1.5 and direction_matches:
            score_add = 1.0
            label = f"Strong Volume ({ratio:.1f}x avg) ✅"
            confirmed = True
        elif ratio >= 1.1 and direction_matches:
            score_add = 0.5
            label = f"Good Volume ({ratio:.1f}x avg)"
            confirmed = True
        elif ratio < 0.7:
            score_add = -0.5
            label = f"Low Volume ({ratio:.1f}x avg) ⚠️"
            confirmed = False
        else:
            score_add = 0.0
            label = f"Normal Volume ({ratio:.1f}x avg)"
            confirmed = True

        return {
            "confirmed": confirmed,
            "score_add": score_add,
            "label": label,
            "ratio": round(ratio, 2),
        }
    except Exception as e:
        print(f"[VOLUME] Error: {e}")
        return {"confirmed": True, "score_add": 0, "label": "Volume Error", "ratio": 0}


def check_ema_slope(df: pd.DataFrame, direction: str) -> dict:
    """
    EMA Slope filter — ensures EMAs are genuinely sloping in signal direction,
    not just crossed but flat (which produces many false signals).
    """
    try:
        ema9  = df["ema_9"].values  if "ema_9"  in df.columns else safe_ema(df["close"], 9).values
        ema21 = df["ema_21"].values if "ema_21" in df.columns else safe_ema(df["close"], 21).values

        # Slope = change over last 3 candles (normalised by price)
        price = df["close"].iloc[-1]
        slope9  = (ema9[-1]  - ema9[-4])  / price if len(ema9)  >= 4 else 0
        slope21 = (ema21[-1] - ema21[-4]) / price if len(ema21) >= 4 else 0

        # Threshold: slope must be meaningful (not flat)
        SLOPE_MIN = 0.00005   # 0.005% per candle minimum

        call_ok = slope9 > SLOPE_MIN and slope21 > SLOPE_MIN
        put_ok  = slope9 < -SLOPE_MIN and slope21 < -SLOPE_MIN

        if direction == "CALL":
            aligned = call_ok
            strong  = slope9 > SLOPE_MIN * 3
        else:
            aligned = put_ok
            strong  = slope9 < -SLOPE_MIN * 3

        if strong and aligned:
            score_add = 0.8
            label = "EMA Slope Strong ✅"
        elif aligned:
            score_add = 0.4
            label = "EMA Slope Aligned"
        else:
            score_add = -0.6
            label = "EMA Slope Flat/Against ⚠️"

        return {
            "aligned": aligned,
            "strong": strong,
            "slope9": round(slope9 * 10000, 3),
            "slope21": round(slope21 * 10000, 3),
            "score_add": score_add,
            "label": label,
        }
    except Exception as e:
        print(f"[EMA SLOPE] Error: {e}")
        return {"aligned": True, "strong": False, "score_add": 0, "label": "Slope Error"}


# Per-pair normal ATR ranges (as % of price) — used for pair-specific volatility check
PAIR_NORMAL_ATR = {
    "EUR/USD": (0.0003, 0.0035),
    "GBP/USD": (0.0004, 0.0045),
    "USD/JPY": (0.0003, 0.0040),
    "EUR/GBP": (0.0002, 0.0030),
    "CAD/JPY": (0.0003, 0.0040),
    "AUD/USD": (0.0003, 0.0038),
    "GBP/JPY": (0.0005, 0.0055),
    "EUR/JPY": (0.0004, 0.0048),
}

def check_pair_volatility(df: pd.DataFrame, pair: str) -> dict:
    """
    Pair-specific volatility check using per-pair normal ATR ranges.
    Rejects signals when volatility is abnormally high or low for that pair.
    """
    try:
        atr_val  = enhanced_atr(df, 14).iloc[-1]
        avg_price = df["close"].iloc[-20:].mean()
        atr_pct  = atr_val / avg_price if avg_price > 0 else 0

        low_norm, high_norm = PAIR_NORMAL_ATR.get(pair, (0.0002, 0.005))

        if atr_pct < low_norm:
            score_add = -0.3
            label = f"Volatility Too Low for {pair}"
            suitable = False
        elif atr_pct > high_norm:
            score_add = -0.8
            label = f"Volatility Too High for {pair} ⚠️"
            suitable = False
        else:
            score_add = 0.3
            label = f"Volatility Normal for {pair} ✅"
            suitable = True

        return {
            "suitable": suitable,
            "atr_pct": round(atr_pct * 10000, 1),
            "score_add": score_add,
            "label": label,
        }
    except Exception as e:
        print(f"[PAIR VOL] Error: {e}")
        return {"suitable": True, "score_add": 0, "label": "Volatility Error"}


# Per-pair cooldown — DISABLED (no cooldown between same-pair signals)
_pair_last_signal_time: Dict[str, float] = {}
_pair_last_signal_result: Dict[str, str] = {}
_pair_cooldown_lock = threading.Lock()

def check_pair_cooldown(pair: str, df1=None) -> bool:
    """Cooldown fully removed — every candle is eligible for a signal on any pair."""
    return True

def set_pair_cooldown(pair: str, result: str = "pending"):
    """No-op — cooldown removed."""
    with _pair_cooldown_lock:
        _pair_last_signal_time[pair]   = time.time()
        _pair_last_signal_result[pair] = result

def update_pair_cooldown_result(pair: str, result: str):
    """Update result tracking for auto-learning (cooldown itself is off)."""
    with _pair_cooldown_lock:
        if pair in _pair_last_signal_time:
            _pair_last_signal_result[pair] = result

# Candle close confirmation tracking
_last_confirmed_candle: Dict[str, str] = {}   # pair -> last candle datetime string
_candle_confirm_lock = threading.Lock()

def is_candle_closed(df: pd.DataFrame, pair: str) -> bool:
    """
    Checks if the most recent candle in df is a NEW closed candle
    (i.e. different from the last one we already analyzed for this pair).
    This prevents re-analyzing the same candle twice.
    """
    try:
        if "datetime" not in df.columns or len(df) < 2:
            return True   # can't check — allow through
        # The most recent row is the just-closed candle
        latest_dt = str(df["datetime"].iloc[-1])
        with _candle_confirm_lock:
            last_seen = _last_confirmed_candle.get(pair, "")
            if latest_dt == last_seen:
                return False   # same candle, not yet closed/new
            _last_confirmed_candle[pair] = latest_dt
        return True
    except Exception:
        return True


# Noisy hour score boost threshold (require higher score during volatile windows)
def get_noisy_hour_min_score() -> float:
    """
    During historically noisier market windows, require a higher minimum score.
    London open (1:30–2:30 PM IST) and NY open (6:30–7:30 PM IST) are trickier.
    """
    ist_hour = now_ist().hour
    ist_minute = now_ist().minute
    ist_time = ist_hour * 60 + ist_minute   # minutes since midnight IST

    # London open zone: 13:30–14:30 IST
    if 810 <= ist_time <= 870:
        return float(MIN_SCORE) + 1.0   # require 8.0 during London open

    # NY open zone: 18:30–19:30 IST
    if 1110 <= ist_time <= 1170:
        return float(MIN_SCORE) + 1.0   # require 8.0 during NY open

    return float(MIN_SCORE)   # normal threshold otherwise

# ============================== END NEW ACCURACY FILTERS ==============================

def bollinger_bands(series: pd.Series, window: int = 20, num_std: int = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
    try:
        if HAS_TA:
            bb = ta.volatility.BollingerBands(series, window=window, window_dev=num_std)
            upper = bb.bollinger_hband().bfill()
            middle = bb.bollinger_mavg().bfill()
            lower = bb.bollinger_lband().bfill()
            return upper, middle, lower
        else:
            rolling_mean = series.rolling(window=window).mean()
            rolling_std = series.rolling(window=window).std()
            upper_band = rolling_mean + (rolling_std * num_std)
            lower_band = rolling_mean - (rolling_std * num_std)
            return upper_band.bfill(), rolling_mean.bfill(), lower_band.bfill()
    except Exception:
        middle = series.bfill()
        return middle, middle, middle

def stochastic_oscillator(df: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
    try:
        if HAS_TA:
            stoch = ta.momentum.StochasticOscillator(df["high"], df["low"], df["close"],
                                                   window=k_period, smooth_window=d_period)
            k_percent = stoch.stoch().fillna(50)
            d_percent = stoch.stoch_signal().fillna(50)
            return k_percent, d_percent
        else:
            high = df["high"]
            low = df["low"]
            close = df["close"]
            lowest_low = low.rolling(window=k_period).min()
            highest_high = high.rolling(window=k_period).max()
            k_percent = 100 * ((close - lowest_low) / (highest_high - lowest_low))
            d_percent = k_percent.rolling(window=d_period).mean()
            return k_percent.fillna(50), d_percent.fillna(50)
    except Exception:
        default = pd.Series([50] * len(df), index=df.index)
        return default, default

# ============================== ENHANCED PATTERN RECOGNITION ==============================

def detect_enhanced_candlesticks(df: pd.DataFrame) -> Dict[str,any]:
    if len(df) < 5:
        return {"patterns": [], "bullish_score": 0, "bearish_score": 0}
    patterns = []
    bullish_score = 0
    bearish_score = 0
    o = df["open"].values[-3:]
    h = df["high"].values[-3:]
    l = df["low"].values[-3:]
    c = df["close"].values[-3:]
    curr_o, curr_h, curr_l, curr_c = o[-1], h[-1], l[-1], c[-1]
    prev_o, prev_h, prev_l, prev_c = o[-2], h[-2], l[-2], c[-2]
    curr_body = abs(curr_c - curr_o)
    curr_range = curr_h - curr_l if curr_h != curr_l else 0.0001
    upper_shadow = curr_h - max(curr_c, curr_o)
    lower_shadow = min(curr_c, curr_o) - curr_l
    prev_body = abs(prev_c - prev_o)
    prev_range = prev_h - prev_l if prev_h != prev_l else 0.0001
    if lower_shadow > 2 * curr_body and upper_shadow < curr_body * 0.3:
        if curr_c > curr_o:
            patterns.append("Bullish Hammer")
            bullish_score += 3
        else:
            patterns.append("Hammer")
            bullish_score += 1
    if upper_shadow > 2 * curr_body and lower_shadow < curr_body * 0.3:
        if curr_c < curr_o:
            patterns.append("Bearish Shooting Star")
            bearish_score += 3
        else:
            patterns.append("Inverted Hammer")
            bullish_score += 1
    if curr_body <= 0.1 * curr_range:
        if upper_shadow > 2 * curr_body and lower_shadow > 2 * curr_body:
            patterns.append("Long-Legged Doji")
        elif upper_shadow > 3 * curr_body:
            patterns.append("Dragonfly Doji")
            bullish_score += 2
        elif lower_shadow > 3 * curr_body:
            patterns.append("Gravestone Doji")
            bearish_score += 2
        else:
            patterns.append("Doji")
    if curr_body > 0.8 * curr_range:
        if curr_c > curr_o:
            patterns.append("Bullish Marubozu")
            bullish_score += 2
        else:
            patterns.append("Bearish Marubozu")
            bearish_score += 2
    if len(o) >= 2:
        if (curr_c > curr_o and prev_c < prev_o and
            curr_c >= prev_o and curr_o <= prev_c and
            curr_body > prev_body):
            patterns.append("Bullish Engulfing")
            bullish_score += 4
        if (curr_c < curr_o and prev_c > prev_o and
            curr_c <= prev_o and curr_o >= prev_c and
            curr_body > prev_body):
            patterns.append("Bearish Engulfing")
            bearish_score += 4
        if (curr_c > curr_o and prev_c < prev_o and
            curr_o < prev_l and curr_c > prev_o + (prev_o - prev_c) * 0.5):
            patterns.append("Piercing Line")
            bullish_score += 3
        if (curr_c < curr_o and prev_c > prev_o and
            curr_o > prev_h and curr_c < prev_c - (prev_c - prev_o) * 0.5):
            patterns.append("Dark Cloud Cover")
            bearish_score += 3
    if len(o) >= 3:
        third_o, third_c = o[-3], c[-3]
        if (third_c < third_o and prev_body < curr_body * 0.3 and
            curr_c > curr_o and curr_c > (third_o + third_c) / 2):
            patterns.append("Morning Star")
            bullish_score += 4
        if (third_c > third_o and prev_body < curr_body * 0.3 and
            curr_c < curr_o and curr_c < (third_o + third_c) / 2):
            patterns.append("Evening Star")
            bearish_score += 4
    return {
        "patterns": patterns,
        "bullish_score": bullish_score,
        "bearish_score": bearish_score,
        "net_sentiment": bullish_score - bearish_score
    }

def detect_advanced_chart_patterns(df: pd.DataFrame) -> Dict[str,any]:
    if len(df) < 30:
        return {"patterns": [], "bullish_patterns": [], "bearish_patterns": [], "score": 0}
    patterns = []
    bullish_patterns = []
    bearish_patterns = []
    pattern_score = 0
    closes = df["close"].values[-30:]
    highs = df["high"].values[-30:]
    lows = df["low"].values[-30:]
    if len(highs) >= 20:
        double_top = detect_double_top(highs)
        double_bottom = detect_double_bottom(lows)
        if double_top["detected"]:
            patterns.append("Double Top")
            bearish_patterns.append("Double Top")
            pattern_score -= 3
        if double_bottom["detected"]:
            patterns.append("Double Bottom")
            bullish_patterns.append("Double Bottom")
            pattern_score += 3
    triangle_result = detect_triangle_patterns(highs, lows)
    if triangle_result["pattern"]:
        patterns.append(triangle_result["pattern"])
        if "Ascending" in triangle_result["pattern"]:
            bullish_patterns.append(triangle_result["pattern"])
            pattern_score += 2
        elif "Descending" in triangle_result["pattern"]:
            bearish_patterns.append(triangle_result["pattern"])
            pattern_score -= 2
    flag_result = detect_flag_patterns(closes)
    if flag_result["pattern"]:
        patterns.append(flag_result["pattern"])
        if "Bull" in flag_result["pattern"]:
            bullish_patterns.append(flag_result["pattern"])
            pattern_score += 2
        else:
            bearish_patterns.append(flag_result["pattern"])
            pattern_score -= 2
    return {
        "patterns": patterns,
        "bullish_patterns": bullish_patterns,
        "bearish_patterns": bearish_patterns,
        "score": pattern_score
    }

def detect_double_top(highs: np.ndarray) -> Dict[str,any]:
    if len(highs) < 10:
        return {"detected": False, "confidence": 0}
    peak_indices = []
    for i in range(2, len(highs) - 2):
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
            peak_indices.append(i)
    if len(peak_indices) >= 2:
        peak1_height = highs[peak_indices[-2]]
        peak2_height = highs[peak_indices[-1]]
        height_diff = abs(peak1_height - peak2_height) / max(peak1_height, peak2_height)
        if height_diff < 0.02:
            return {"detected": True, "confidence": 0.8}
    return {"detected": False, "confidence": 0}

def detect_double_bottom(lows: np.ndarray) -> Dict[str,any]:
    if len(lows) < 10:
        return {"detected": False, "confidence": 0}
    trough_indices = []
    for i in range(2, len(lows) - 2):
        if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
            trough_indices.append(i)
    if len(trough_indices) >= 2:
        trough1_depth = lows[trough_indices[-2]]
        trough2_depth = lows[trough_indices[-1]]
        depth_diff = abs(trough1_depth - trough2_depth) / min(trough1_depth, trough2_depth)
        if depth_diff < 0.02:
            return {"detected": True, "confidence": 0.8}
    return {"detected": False, "confidence": 0}

def detect_triangle_patterns(highs: np.ndarray, lows: np.ndarray) -> Dict[str,any]:
    if len(highs) < 15:
        return {"pattern": None, "confidence": 0}
    try:
        recent_highs = highs[-15:]
        recent_lows = lows[-15:]
        x = np.arange(len(recent_highs))
        high_slope = np.polyfit(x, recent_highs, 1)[0]
        low_slope = np.polyfit(x, recent_lows, 1)[0]
        if abs(high_slope) < 0.0001 and low_slope > 0.0001:
            return {"pattern": "Ascending Triangle", "confidence": 0.7}
        elif high_slope < -0.0001 and abs(low_slope) < 0.0001:
            return {"pattern": "Descending Triangle", "confidence": 0.7}
        elif high_slope < -0.0001 and low_slope > 0.0001:
            return {"pattern": "Symmetrical Triangle", "confidence": 0.6}
    except Exception:
        pass
    return {"pattern": None, "confidence": 0}

def detect_flag_patterns(closes: np.ndarray) -> Dict[str, any]:
    if len(closes) < 20:
        return {"pattern": None, "confidence": 0}
    try:
        trend_phase = closes[-20:-10]
        consolidation_phase = closes[-10:]
        trend_slope = np.polyfit(np.arange(len(trend_phase)), trend_phase, 1)[0]
        consolidation_slope = np.polyfit(np.arange(len(consolidation_phase)), consolidation_phase, 1)[0]
        if abs(trend_slope) > 0.001 and abs(consolidation_slope) < 0.0005:
            if trend_slope > 0:
                return {"pattern": "Bull Flag", "confidence": 0.7}
            else:
                return {"pattern": "Bear Flag", "confidence": 0.7}
    except Exception:
        pass
    return {"pattern": None, "confidence": 0}

# ============================== MARKET STRUCTURE ANALYSIS ==============================

def detect_market_structure(df: pd.DataFrame) -> Dict[str,any]:
    if len(df) < 50:
        return {"trend": "Unknown", "structure": "Insufficient Data", "strength": 0}
    closes = df["close"].values[-50:]
    highs = df["high"].values[-50:]
    lows = df["low"].values[-50:]
    ema_9 = safe_ema(df["close"].tail(50), 9).values
    ema_21 = safe_ema(df["close"].tail(50), 21).values
    ema_50 = safe_ema(df["close"].tail(50), 50).values
    current_price = closes[-1]
    if ema_9[-1] > ema_21[-1] > ema_50[-1] and current_price > ema_9[-1]:
        primary_trend = "Strong Uptrend"
        trend_strength = 0.9
    elif ema_9[-1] > ema_21[-1] and current_price > ema_21[-1]:
        primary_trend = "Uptrend"
        trend_strength = 0.7
    elif ema_9[-1] < ema_21[-1] < ema_50[-1] and current_price < ema_9[-1]:
        primary_trend = "Strong Downtrend"
        trend_strength = 0.9
    elif ema_9[-1] < ema_21[-1] and current_price < ema_21[-1]:
        primary_trend = "Downtrend"
        trend_strength = 0.7
    else:
        primary_trend = "Sideways"
        trend_strength = 0.3
    support_resistance = find_support_resistance_levels(df)
    structure = classify_market_structure(closes, highs, lows)
    return {
        "trend": primary_trend,
        "structure": structure,
        "strength": trend_strength,
        "support_levels": support_resistance["support"],
        "resistance_levels": support_resistance["resistance"],
        "key_level_distance": support_resistance["distance_to_key_level"]
    }

def find_support_resistance_levels(df: pd.DataFrame, lookback: int = 30) -> Dict[str,any]:
    if len(df) < lookback:
        lookback = len(df)
    recent_data = df.tail(lookback)
    highs = recent_data["high"].values
    lows = recent_data["low"].values
    closes = recent_data["close"].values
    current_price = closes[-1]
    resistance_levels = []
    support_levels = []
    for i in range(2, len(highs) - 2):
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
            resistance_levels.append(highs[i])
        if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
            support_levels.append(lows[i])
    resistance_levels = cluster_levels(resistance_levels, current_price * 0.0005)
    support_levels = cluster_levels(support_levels, current_price * 0.0005)
    all_levels = resistance_levels + support_levels
    if all_levels:
        distances = [abs(level - current_price) / current_price for level in all_levels]
        min_distance = min(distances)
    else:
        min_distance = 1.0
    return {
        "resistance": resistance_levels,
        "support": support_levels,
        "distance_to_key_level": min_distance
    }

def cluster_levels(levels: List[float], tolerance: float) -> List[float]:
    if not levels:
        return []
    levels = sorted(levels)
    clusters = []
    current_cluster = [levels[0]]
    for level in levels[1:]:
        if abs(level - current_cluster[-1]) <= tolerance:
            current_cluster.append(level)
        else:
            clusters.append(np.mean(current_cluster))
            current_cluster = [level]
    if current_cluster:
        clusters.append(np.mean(current_cluster))
    return clusters

def classify_market_structure(closes: np.ndarray, highs: np.ndarray, lows: np.ndarray) -> str:
    if len(closes) < 20:
        return "Unknown"
    recent_closes = closes[-20:]
    recent_highs = highs[-20:]
    recent_lows = lows[-20:]
    volatility = np.std(recent_closes)
    price_range = np.max(recent_highs) - np.min(recent_lows)
    avg_price = np.mean(recent_closes)
    linear_trend = np.polyfit(range(len(recent_closes)), recent_closes, 1)[0]
    trend_strength = abs(linear_trend) / (volatility + 0.0001)
    if trend_strength > 0.5:
        if linear_trend > 0:
            return "Strong Bullish Trend"
        else:
            return "Strong Bearish Trend"
    elif trend_strength > 0.2:
        if linear_trend > 0:
            return "Weak Bullish Trend"
        else:
            return "Weak Bearish Trend"
    else:
        upper_bound = avg_price + (price_range * 0.3)
        lower_bound = avg_price - (price_range * 0.3)
        price_in_range = np.sum((recent_closes >= lower_bound) & (recent_closes <= upper_bound))
        range_percentage = price_in_range / len(recent_closes)
        if range_percentage > 0.7:
            return "Range Bound"
        else:
            return "Choppy Market"

def detect_breakout_setup(df: pd.DataFrame, market_structure: Dict) -> Dict[str,any]:
    if len(df) < 20:
        return {"breakout_potential": False, "direction": None, "strength": 0}
    current_price = df["close"].iloc[-1]
    resistance_levels = market_structure.get("resistance_levels", [])
    support_levels = market_structure.get("support_levels", [])
    breakout_potential = False
    direction = None
    strength = 0
    tolerance = current_price * 0.0003
    for resistance in resistance_levels:
        if abs(current_price - resistance) <= tolerance:
            if current_price >= resistance:
                breakout_potential = True
                direction = "CALL"
                strength = 0.8
                break
    for support in support_levels:
        if abs(current_price - support) <= tolerance:
            if current_price <= support:
                breakout_potential = True
                direction = "PUT"
                strength = 0.8
                break
    return {
        "breakout_potential": breakout_potential,
        "direction": direction,
        "strength": strength
    }

def detect_range_bounce_setup(df: pd.DataFrame, market_structure: Dict) -> Dict[str,any]:
    if len(df) < 5:
        return {"bounce_potential": False, "direction": None, "strength": 0}
    current_price = df["close"].iloc[-1]
    prev_price = df["close"].iloc[-2]
    prev_low = df["low"].iloc[-2]
    prev_high = df["high"].iloc[-2]
    resistance_levels = market_structure.get("resistance_levels", [])
    support_levels = market_structure.get("support_levels", [])
    bounce_potential = False
    direction = None
    strength = 0
    for support in support_levels:
        distance_to_support = abs(current_price - support) / current_price
        if distance_to_support < 0.0015:
            price_rising = current_price >= prev_price
            candle_touched_support = prev_low <= support * 1.001
            if price_rising:
                bounce_potential = True
                direction = "CALL"
                strength = 0.8 if candle_touched_support else 0.6
                break
    if not bounce_potential:
        for resistance in resistance_levels:
            distance_to_resistance = abs(current_price - resistance) / current_price
            if distance_to_resistance < 0.0015:
                price_falling = current_price <= prev_price
                candle_touched_resistance = prev_high >= resistance * 0.999
                if price_falling:
                    bounce_potential = True
                    direction = "PUT"
                    strength = 0.8 if candle_touched_resistance else 0.6
                    break
    return {
        "bounce_potential": bounce_potential,
        "direction": direction,
        "strength": strength
    }


def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema_9"] = safe_ema(df["close"], 9)
    df["ema_21"] = safe_ema(df["close"], 21)
    df["ema_50"] = safe_ema(df["close"], 50)
    df["sma_20"] = safe_sma(df["close"], 20)
    df["rsi"] = enhanced_rsi(df["close"], 14)
    macd_line, macd_signal, macd_hist = enhanced_macd(df["close"])
    df["macd"] = macd_line
    df["macd_signal"] = macd_signal
    df["macd_hist"] = macd_hist
    df["atr"] = enhanced_atr(df, 14)
    bb_upper, bb_middle, bb_lower = bollinger_bands(df["close"])
    df["bb_upper"] = bb_upper
    df["bb_middle"] = bb_middle
    df["bb_lower"] = bb_lower
    stoch_k, stoch_d = stochastic_oscillator(df)
    df["stoch_k"] = stoch_k
    df["stoch_d"] = stoch_d
    return df

def analyze_multi_timeframe_trend(df1: pd.DataFrame, df5: pd.DataFrame, df15: pd.DataFrame) -> Dict[str,any]:
    m1_bullish = (df1["ema_9"].iloc[-1] > df1["ema_21"].iloc[-1] and
                  df1["close"].iloc[-1] > df1["ema_9"].iloc[-1])
    m5_bullish = (df5["ema_9"].iloc[-1] > df5["ema_21"].iloc[-1] and
                  df5["close"].iloc[-1] > df5["ema_9"].iloc[-1])
    m15_bullish = (df15["ema_21"].iloc[-1] > df15["ema_50"].iloc[-1] and
                   df15["close"].iloc[-1] > df15["ema_21"].iloc[-1])
    bullish_timeframes = sum([m1_bullish, m5_bullish, m15_bullish])
    bearish_timeframes = 3 - bullish_timeframes
    if bullish_timeframes >= 2:
        direction = "BULLISH"
        alignment_score = bullish_timeframes / 3
    else:
        direction = "BEARISH"
        alignment_score = bearish_timeframes / 3
    m1_strength = abs(df1["ema_9"].iloc[-1] - df1["ema_21"].iloc[-1]) / df1["close"].iloc[-1]
    m5_strength = abs(df5["ema_9"].iloc[-1] - df5["ema_21"].iloc[-1]) / df5["close"].iloc[-1]
    m15_strength = abs(df15["ema_21"].iloc[-1] - df15["ema_50"].iloc[-1]) / df15["close"].iloc[-1]
    avg_strength = (m1_strength + m5_strength + m15_strength) / 3
    return {
        "direction": direction,
        "alignment_score": alignment_score,
        "strength": min(1.0, avg_strength * 1000),
        "timeframe_agreement": {
            "1m": m1_bullish,
            "5m": m5_bullish,
            "15m": m15_bullish
        }
    }

def check_support_resistance_confluence(current_price: float, market_structure: Dict) -> Dict[str,any]:
    support_levels = market_structure.get("support_levels", [])
    resistance_levels = market_structure.get("resistance_levels", [])
    min_distance = float('inf')
    nearest_level = None
    level_type = None
    expected_direction = None
    for support in support_levels:
        distance = abs(current_price - support)
        if distance < min_distance:
            min_distance = distance
            nearest_level = support
            level_type = "Support"
            expected_direction = "CALL" if current_price >= support else None
    for resistance in resistance_levels:
        distance = abs(current_price - resistance)
        if distance < min_distance:
            min_distance = distance
            nearest_level = resistance
            level_type = "Resistance"
            expected_direction = "PUT" if current_price <= resistance else None
    distance_pips = min_distance * 10000 if nearest_level else float('inf')
    return {
        "near_level": distance_pips <= 5,
        "level_type": level_type,
        "distance_pips": distance_pips,
        "expected_direction": expected_direction,
        "nearest_level": nearest_level
    }

def analyze_momentum_convergence(df1: pd.DataFrame, df5: pd.DataFrame) -> Dict[str,any]:
    rsi_1m = df1["rsi"].iloc[-1]
    rsi_5m = df5["rsi"].iloc[-1]
    macd_hist_1m = df1["macd_hist"].iloc[-1]
    macd_hist_5m = df5["macd_hist"].iloc[-1]
    stoch_1m = df1["stoch_k"].iloc[-1]
    stoch_5m = df5["stoch_k"].iloc[-1]
    bullish_signals = 0
    bearish_signals = 0
    if rsi_1m > 50 and rsi_5m > 50:
        bullish_signals += 1
    elif rsi_1m < 50 and rsi_5m < 50:
        bearish_signals += 1
    if macd_hist_1m > 0 and macd_hist_5m > 0:
        bullish_signals += 1
    elif macd_hist_1m < 0 and macd_hist_5m < 0:
        bearish_signals += 1
    if stoch_1m > 50 and stoch_5m > 50:
        bullish_signals += 1
    elif stoch_1m < 50 and stoch_5m < 50:
        bearish_signals += 1
    if bullish_signals >= 2:
        direction = "BULLISH"
        strong_momentum = bullish_signals == 3
        moderate_momentum = bullish_signals == 2
    elif bearish_signals >= 2:
        direction = "BEARISH"
        strong_momentum = bearish_signals == 3
        moderate_momentum = bearish_signals == 2
    else:
        direction = "MIXED"
        strong_momentum = False
        moderate_momentum = False
    return {
        "direction": direction,
        "strong_momentum": strong_momentum,
        "moderate_momentum": moderate_momentum,
        "bullish_signals": bullish_signals,
        "bearish_signals": bearish_signals
    }

def check_rsi_divergence(df: pd.DataFrame) -> dict:
    try:
        closes = df["close"].values[-20:]
        rsi_vals = enhanced_rsi(df["close"], 14).values[-20:]
        if len(closes) < 5 or len(rsi_vals) < 5:
            return {"detected": False, "type": None, "direction": None}
        if closes[-1] < closes[-3] and rsi_vals[-1] > rsi_vals[-3]:
            return {"detected": True, "type": "Bullish", "direction": "BULLISH"}
        if closes[-1] > closes[-3] and rsi_vals[-1] < rsi_vals[-3]:
            return {"detected": True, "type": "Bearish", "direction": "BEARISH"}
        return {"detected": False, "type": None, "direction": None}
    except Exception as e:
        print(f"[ERROR] RSI divergence check failed: {e}")
        return {"detected": False, "type": None, "direction": None}

def analyze_volatility_conditions(df: pd.DataFrame) -> dict:
    try:
        atr = enhanced_atr(df, 14).iloc[-1]
        avg_price = df["close"].iloc[-50:].mean()
        atr_pct = atr / avg_price if avg_price else 0
        if atr_pct < 0.001:
            level = "LOW"
            suitable = False
        elif atr_pct > 0.008:
            level = "HIGH"
            suitable = False
        else:
            level = "OPTIMAL"
            suitable = True
        return {
            "atr_value": atr,
            "atr_pct": atr_pct,
            "level": level,
            "suitable_for_trading": suitable
        }
    except Exception as e:
        print(f"[ERROR] Volatility analysis failed: {e}")
        return {
            "atr_value": 0.0,
            "atr_pct": 0.0,
            "level": "UNKNOWN",
            "suitable_for_trading": False
        }

def check_macd_signal(df: pd.DataFrame) -> dict:
    try:
        macd_line, macd_signal, _ = enhanced_macd(df["close"])
        if len(macd_line) < 2 or len(macd_signal) < 2:
            return {"strong_signal": False, "direction": None}
        prev_macd, curr_macd = macd_line.iloc[-2], macd_line.iloc[-1]
        prev_signal, curr_signal = macd_signal.iloc[-2], macd_signal.iloc[-1]
        if prev_macd < prev_signal and curr_macd > curr_signal:
            return {"strong_signal": True, "direction": "BULLISH"}
        if prev_macd > prev_signal and curr_macd < curr_signal:
            return {"strong_signal": True, "direction": "BEARISH"}
        return {"strong_signal": False, "direction": None}
    except Exception as e:
        print(f"[ERROR] MACD signal check failed: {e}")
        return {"strong_signal": False, "direction": None}


def check_stochastic_conditions(df: pd.DataFrame) -> dict:
    try:
        stoch_k, stoch_d = stochastic_oscillator(df)
        if len(stoch_k) < 1 or len(stoch_d) < 1:
            return {"signal": False, "direction": None}
        k, d = stoch_k.iloc[-1], stoch_d.iloc[-1]
        if k < 20 and k > d:
            return {"signal": True, "direction": "BULLISH"}
        if k > 80 and k < d:
            return {"signal": True, "direction": "BEARISH"}
        return {"signal": False, "direction": None}
    except Exception as e:
        print(f"[ERROR] Stochastic condition check failed: {e}")
        return {"signal": False, "direction": None}

def measure_pip_speed(df: pd.DataFrame) -> float:
    """
    Idea 3 — Pip Momentum Speed.
    Measures average candle body size (pips) over last 5 candles.
    High speed = market moving fast = shorter expiry needed.
    Low speed  = market moving slow = longer expiry needed.
    Returns speed in price units (not scaled to pips).
    """
    try:
        last5 = df.tail(5)
        bodies = (last5["close"] - last5["open"]).abs()
        avg_body = bodies.mean()
        return float(avg_body)
    except Exception:
        return 0.0


def get_adx_for_expiry(df: pd.DataFrame) -> float:
    """
    Idea 5 — ADX-based expiry adjustment.
    Returns the current ADX value for use in expiry calculation.
    """
    try:
        adx_series = calculate_adx(df, 14)
        return float(adx_series.iloc[-1])
    except Exception:
        return 20.0  # neutral default


def get_candle_seconds_remaining() -> int:
    """
    Idea 1 — Candle Timing Offset.
    Returns how many seconds remain in the current 1-minute candle.
    Entry is always the NEXT candle open, so expiry should align
    to clean candle boundaries from that entry point.
    """
    now = now_utc()
    seconds_elapsed = now.second
    return 60 - seconds_elapsed


def calculate_smart_expiry(
    score: float,
    atr_value: float,
    trend_strength: float,
    structure: str,
    df: pd.DataFrame,
    candlestick_patterns: Dict,
    chart_patterns: Dict,
    support_resistance: Dict,
    momentum: Dict
) -> int:
    """
    Upgraded expiry engine:
    - Idea 1: candle timing alignment (always lands on clean candle boundary)
    - Idea 3: pip speed — fast market = shorter, slow market = longer
    - Idea 5: ADX strength — strong trend = shorter, weak = longer
    - Standard expiry range 1-5 min
    - Base expiry 1–5 min normal, 1–10 min during recovery
    """
    # ── Standard expiry ceiling ──────────────────────────────────────────
    max_expiry = 5
    # Score-based starting point
    if score >= 9.0:
        base_expiry = 3.0
    elif score >= 8.0:
        base_expiry = 2.0
    else:
        base_expiry = 2.0

    # ── Idea 3: Pip Speed Measurement ─────────────────────────────────────
    pip_speed = measure_pip_speed(df)
    avg_price = float(df["close"].iloc[-1]) if len(df) > 0 else 1.0
    # Normalise speed as % of price per candle
    speed_pct = (pip_speed / avg_price) * 100 if avg_price > 0 else 0

    if speed_pct > 0.08:        # very fast (>8 pips on 1.0000 scale)
        base_expiry -= 1.0
        speed_label = f"Very Fast ({speed_pct:.3f}%) -1.0"
    elif speed_pct > 0.04:      # fast (>4 pips)
        base_expiry -= 0.5
        speed_label = f"Fast ({speed_pct:.3f}%) -0.5"
    elif speed_pct < 0.01:      # very slow (<1 pip)
        base_expiry += 1.5
        speed_label = f"Very Slow ({speed_pct:.3f}%) +1.5"
    elif speed_pct < 0.02:      # slow (<2 pips)
        base_expiry += 0.5
        speed_label = f"Slow ({speed_pct:.3f}%) +0.5"
    else:
        speed_label = f"Normal ({speed_pct:.3f}%) 0"

    # ── Idea 5: ADX Strength ──────────────────────────────────────────────
    adx_val = get_adx_for_expiry(df)
    if adx_val >= 35:
        base_expiry -= 1.0
        adx_label = f"Strong ADX ({adx_val:.1f}) -1.0"
    elif adx_val >= 28:
        base_expiry -= 0.5
        adx_label = f"Moderate-Strong ADX ({adx_val:.1f}) -0.5"
    elif adx_val >= 22:
        adx_label = f"Moderate ADX ({adx_val:.1f}) 0"
    elif adx_val >= 18:
        base_expiry += 0.5
        adx_label = f"Weak ADX ({adx_val:.1f}) +0.5"
    else:
        base_expiry += 1.0
        adx_label = f"Very Weak ADX ({adx_val:.1f}) +1.0"

    # ── ATR volatility check (kept from original) ─────────────────────────
    if atr_value > 0.0025:
        base_expiry -= 1.0
    elif atr_value > 0.0015:
        base_expiry -= 0.5
    elif atr_value < 0.0003:
        base_expiry += 1.0
    elif atr_value < 0.0008:
        base_expiry += 0.5

    # ── Momentum direction (kept) ─────────────────────────────────────────
    if momentum.get("strong_momentum"):
        base_expiry -= 0.5 if atr_value > 0.0015 else 0
    elif not momentum.get("moderate_momentum"):
        base_expiry += 0.5

    # ── Market structure ──────────────────────────────────────────────────
    if "Strong" in structure and "Trend" in structure:
        base_expiry += 0.5
    elif "Choppy" in structure:
        base_expiry -= 0.5
    elif "Range" in structure:
        base_expiry -= 0.5

    # ── Candlestick patterns ──────────────────────────────────────────────
    if candlestick_patterns.get("patterns"):
        patterns = candlestick_patterns["patterns"]
        if any(p in patterns for p in ["Bullish Marubozu", "Bearish Marubozu"]):
            base_expiry += 0.5
        if any(p in patterns for p in ["Bullish Engulfing", "Bearish Engulfing",
                                        "Morning Star", "Evening Star"]):
            if support_resistance.get("distance_to_key_level", 1.0) < 0.001:
                base_expiry -= 0.5

    # ── S/R distance ──────────────────────────────────────────────────────
    dist = support_resistance.get("distance_to_key_level", 1.0)
    if dist < 0.0003:
        base_expiry -= 0.5
    elif dist > 0.003:
        base_expiry += 0.5

    # ── Clamp to valid range ──────────────────────────────────────────────
    expiry = round(base_expiry)
    expiry = max(1, min(max_expiry, expiry))

    # ── Idea 1: Candle Timing Alignment ──────────────────────────────────
    # Entry is always at the next candle open (already aligned to minute boundary).
    # So expiry naturally lands on minute boundaries — no extra offset needed
    # unless we're within 5s of candle close (edge case: add 1 min buffer).
    secs_remaining = get_candle_seconds_remaining()
    if secs_remaining <= 5 and expiry < max_expiry:
        expiry += 1
        print(f"[EXPIRY] ⏰ Candle boundary buffer: +1min (only {secs_remaining}s left in candle)")

    print(f"[EXPIRY] Score:{score:.1f} | "
          f"Speed:{speed_label} | {adx_label} | ATR:{atr_value:.5f} → Final:{expiry}min (max:{max_expiry})")
    return expiry


def calculate_enhanced_signal_score(
    df1: pd.DataFrame,
    df5: pd.DataFrame,
    df15: pd.DataFrame
) -> Dict[str,any]:

    if len(df1) < 50 or len(df5) < 50 or len(df15) < 50:
        return {"score": 0.0, "direction": None, "confirmations": [], "expiry_minutes": 1, "logic": {}}

    confirmations = []
    total_score = 0.0
    bullish_evidence = 0.0
    bearish_evidence = 0.0
    bullish_ratio = 0.0
    bearish_ratio = 0.0

    df1 = add_all_indicators(df1)
    df5 = add_all_indicators(df5)
    df15 = add_all_indicators(df15)

    current_price = df1["close"].iloc[-1]
    tagged_confirmations = []

    # 1. MULTI-TIMEFRAME TREND (3.0 points)
    trend_analysis = analyze_multi_timeframe_trend(df1, df5, df15)
    trend_dir = "CALL" if trend_analysis["direction"] == "BULLISH" else "PUT"
    if trend_analysis["alignment_score"] >= 0.95:
        trend_points = 3.0
        tagged_confirmations.append((f"Perfect Trend ({trend_analysis['direction']})", trend_dir))
    elif trend_analysis["alignment_score"] >= 0.60:
        trend_points = 2.0
        tagged_confirmations.append((f"Strong Trend ({trend_analysis['direction']})", trend_dir))
    else:
        trend_points = 1.0
        tagged_confirmations.append((f"Weak Trend ({trend_analysis['direction']})", trend_dir))
    if trend_analysis["direction"] == "BULLISH":
        bullish_evidence += trend_points
    else:
        bearish_evidence += trend_points
    total_score += trend_points
    tf = trend_analysis.get("timeframe_agreement", {})
    print(f"  ├─ 📈 Trend: {trend_analysis['direction']} ({trend_points:.1f}pts) | 1m:{'✅' if tf.get('1m') else '❌'} 5m:{'✅' if tf.get('5m') else '❌'} 15m:{'✅' if tf.get('15m') else '❌'}")

    # 2. MOMENTUM CONVERGENCE (3.0 points)
    momentum_signals = analyze_momentum_convergence(df1, df5)
    mom_dir = "CALL" if momentum_signals["direction"] == "BULLISH" else ("PUT" if momentum_signals["direction"] == "BEARISH" else None)
    if momentum_signals["strong_momentum"]:
        momentum_points = 3.0
        if mom_dir:
            tagged_confirmations.append((f"Strong {momentum_signals['direction']} Momentum", mom_dir))
    elif momentum_signals["moderate_momentum"]:
        momentum_points = 2.0
        if mom_dir:
            tagged_confirmations.append((f"Moderate {momentum_signals['direction']} Momentum", mom_dir))
    elif momentum_signals["direction"] != "MIXED":
        momentum_points = 1.0
        if mom_dir:
            tagged_confirmations.append((f"Weak {momentum_signals['direction']} Momentum", mom_dir))
    else:
        momentum_points = 0.0
    if momentum_signals["direction"] == "BULLISH":
        bullish_evidence += momentum_points
    elif momentum_signals["direction"] == "BEARISH":
        bearish_evidence += momentum_points
    total_score += momentum_points
    print(f"  ├─ ⚡ Momentum: {momentum_signals['direction']} ({momentum_points:.1f}pts) | RSI/MACD/Stoch on 1m+5m")
    candle_patterns = detect_enhanced_candlesticks(df1)
    net_sentiment = candle_patterns.get("net_sentiment", 0)
    candle_dir = "CALL" if net_sentiment > 0 else ("PUT" if net_sentiment < 0 else None)
    if abs(net_sentiment) >= 4:
        pattern_points = 2.0
        if candle_dir:
            tagged_confirmations.append((f"Strong {'Bullish' if net_sentiment > 0 else 'Bearish'} Pattern", candle_dir))
    elif abs(net_sentiment) >= 2:
        pattern_points = 1.0
        if candle_dir:
            tagged_confirmations.append((f"{'Bullish' if net_sentiment > 0 else 'Bearish'} Pattern", candle_dir))
    elif abs(net_sentiment) >= 1:
        pattern_points = 0.5
        if candle_dir:
            tagged_confirmations.append((f"Weak {'Bullish' if net_sentiment > 0 else 'Bearish'} Pattern", candle_dir))
    else:
        pattern_points = 0.0
    if net_sentiment > 0:
        bullish_evidence += pattern_points
    elif net_sentiment < 0:
        bearish_evidence += pattern_points
    total_score += pattern_points
    candle_names = ", ".join(candle_patterns.get("patterns", [])) or "None"
    print(f"  ├─ 🕯  Candles: {candle_names} ({pattern_points:.1f}pts)")

    # 4. VOLATILITY (1.0 points)
    volatility_analysis = analyze_volatility_conditions(df1)
    if volatility_analysis["level"] == "OPTIMAL":
        volatility_points = 1.0
        tagged_confirmations.append(("Optimal Volatility", "NEUTRAL"))
    elif volatility_analysis["level"] == "LOW":
        volatility_points = 0.5
        tagged_confirmations.append(("Low Volatility", "NEUTRAL"))
    else:
        volatility_points = 0.0
    total_score += volatility_points
    print(f"  ├─ 🌊 Volatility: {volatility_analysis['level']} | ATR: {volatility_analysis.get('atr_value', 0):.5f} ({volatility_points:.1f}pts)")

    # 5. ADVANCED CONFLUENCES (1.0 points)
    advanced_points = 0.0
    rsi_divergence = check_rsi_divergence(df1)
    if rsi_divergence["detected"]:
        advanced_points += 0.4
        rsi_dir = "CALL" if rsi_divergence["direction"] == "BULLISH" else "PUT"
        tagged_confirmations.append((f"RSI {rsi_divergence['type']} Divergence", rsi_dir))
        if rsi_divergence["direction"] == "BULLISH":
            bullish_evidence += 0.4
        else:
            bearish_evidence += 0.4
    macd_signal = check_macd_signal(df1)
    if macd_signal["strong_signal"]:
        advanced_points += 0.3
        macd_dir = "CALL" if macd_signal["direction"] == "BULLISH" else "PUT"
        tagged_confirmations.append((f"MACD {macd_signal['direction']}", macd_dir))
        if macd_signal["direction"] == "BULLISH":
            bullish_evidence += 0.3
        else:
            bearish_evidence += 0.3
    stoch_signal = check_stochastic_conditions(df1)
    if stoch_signal["signal"]:
        advanced_points += 0.3
        stoch_dir = "CALL" if stoch_signal["direction"] == "BULLISH" else "PUT"
        tagged_confirmations.append((f"Stochastic {stoch_signal['direction']}", stoch_dir))
        if stoch_signal["direction"] == "BULLISH":
            bullish_evidence += 0.3
        else:
            bearish_evidence += 0.3
    total_score += advanced_points

    # CHART PATTERNS (bonus evidence)
    chart_patterns = detect_advanced_chart_patterns(df1)
    market_structure = detect_market_structure(df1)
    breakout_setup = detect_breakout_setup(df1, market_structure)
    bounce_setup = detect_range_bounce_setup(df1, market_structure)
    print(f"  ├─ 🏗  Market Structure: {market_structure.get('structure','?')} | Trend: {market_structure.get('trend','?')}")
    chart_names = ", ".join(chart_patterns.get("patterns", [])) or "None"
    print(f"  ├─ 📐 Chart Patterns: {chart_names}")

    if breakout_setup["breakout_potential"]:
        bk_dir = breakout_setup["direction"]
        if bk_dir == "CALL":
            bullish_evidence += 0.5
        else:
            bearish_evidence += 0.5
        tagged_confirmations.append((f"Breakout Setup ({bk_dir})", bk_dir))
    elif bounce_setup["bounce_potential"]:
        bounce_dir = bounce_setup["direction"]
        if bounce_dir == "CALL":
            bullish_evidence += 0.5
            bearish_evidence -= 0.3
            tagged_confirmations.append(("Support Bounce → CALL ⚠️", "CALL"))
        else:
            bearish_evidence += 0.5
            bullish_evidence -= 0.3
            tagged_confirmations.append(("Resistance Bounce → PUT ⚠️", "PUT"))

    # DIRECTIONAL BIAS
    total_evidence = bullish_evidence + bearish_evidence
    if total_evidence > 0:
        bullish_ratio = bullish_evidence / total_evidence
        bearish_ratio = bearish_evidence / total_evidence
        if bullish_ratio >= 0.65 and bullish_evidence >= 1.5:
            final_direction = "CALL"
        elif bearish_ratio >= 0.65 and bearish_evidence >= 1.5:
            final_direction = "PUT"
        else:
            final_direction = None
            total_score = 0.0
    else:
        final_direction = None
        total_score = 0.0

    # CHOPPY MARKET REJECT
    structure_type = market_structure.get("structure", "")
    if "Choppy" in structure_type:
        print("[SCORE] ❌ Choppy market detected — signal rejected")
        final_direction = None

    # RSI EXTREME FILTER — avoid entering already-exhausted moves
    # Don't CALL when RSI > 75 (overbought), don't PUT when RSI < 25 (oversold)
    if final_direction:
        rsi_1m_now = df1["rsi"].iloc[-1]
        if final_direction == "CALL" and rsi_1m_now > 75:
            print(f"[SCORE] ❌ RSI overbought ({rsi_1m_now:.1f}) — CALL rejected to avoid exhausted move")
            final_direction = None
        elif final_direction == "PUT" and rsi_1m_now < 25:
            print(f"[SCORE] ❌ RSI oversold ({rsi_1m_now:.1f}) — PUT rejected to avoid exhausted move")
            final_direction = None

    # 15M TREND GATE — never trade directly against the 15m trend unless score is exceptional
    # A signal going against the 15m trend is a counter-trend trade and statistically worse
    if final_direction:
        m15_bull = trend_analysis.get("timeframe_agreement", {}).get("15m", True)
        signal_is_call = final_direction == "CALL"
        trading_against_15m = (signal_is_call and not m15_bull) or (not signal_is_call and m15_bull)
        if trading_against_15m and total_score < 9.0:
            print(f"[SCORE] ❌ Signal fights 15m trend and score {total_score:.1f} < 9.0 — rejected")
            final_direction = None
        elif trading_against_15m:
            print(f"[SCORE] ⚠️ Counter-trend signal allowed — exceptional score {total_score:.1f}/10")

    # MTF FULL AGREEMENT BONUS
    tf_agree = trend_analysis.get("timeframe_agreement", {})
    all_bullish = tf_agree.get("1m") and tf_agree.get("5m") and tf_agree.get("15m")
    all_bearish = (not tf_agree.get("1m")) and (not tf_agree.get("5m")) and (not tf_agree.get("15m"))
    if all_bullish or all_bearish:
        total_score = min(10.0, total_score + 0.5)
        mtf_dir = "CALL" if all_bullish else "PUT"
        tagged_confirmations.append(("✅ Full MTF Alignment (1m+5m+15m)", mtf_dir))

    # FILTER CONFIRMATIONS
    if final_direction:
        confirmations = [
            text for text, d in tagged_confirmations
            if d == final_direction or d == "NEUTRAL"
        ]
        contradicting = [text for text, d in tagged_confirmations if d not in (final_direction, "NEUTRAL")]
        if contradicting:
            print(f"[SCORE] ⚠️ Contradicting indicators excluded: {contradicting}")
    else:
        confirmations = []

    # ── NEW ACCURACY FILTERS ────────────────────────────────────────────────

    # ADX Filter — trend strength check
    if final_direction:
        adx_result = check_adx_filter(df1, final_direction)
        adx_score_add = adx_result["score_add"]
        total_score = max(0.0, total_score + adx_score_add)
        if adx_result["label"] not in ("ADX Error",):
            tagged_confirmations.append((adx_result["label"],
                final_direction if adx_result["directional_ok"] else "NEUTRAL"))
        print(f"  ├─ 📐 ADX: {adx_result['adx']} | DI+: {adx_result['di_plus']} DI-: {adx_result['di_minus']} | Score adj: {adx_score_add:+.1f}")
        # Hard reject if ADX below 18 (truly ranging market)
        if not adx_result["pass_filter"]:
            print(f"  └─ ❌ ADX HARD REJECT — market too choppy (ADX={adx_result['adx']})")
            final_direction = None

    # EMA Slope Filter — EMAs must be genuinely sloping, not flat
    if final_direction:
        slope_result = check_ema_slope(df1, final_direction)
        total_score = max(0.0, total_score + slope_result["score_add"])
        tagged_confirmations.append((slope_result["label"],
            final_direction if slope_result["aligned"] else "NEUTRAL"))
        print(f"  ├─ 📈 EMA Slope: EMA9={slope_result['slope9']} EMA21={slope_result['slope21']} | Score adj: {slope_result['score_add']:+.1f}")

    # Volume Confirmation — real strength behind the move
    if final_direction:
        vol_result = check_volume_confirmation(df1, final_direction)
        total_score = max(0.0, total_score + vol_result["score_add"])
        if vol_result["label"] != "No Volume Data":
            tagged_confirmations.append((vol_result["label"],
                final_direction if vol_result["confirmed"] else "NEUTRAL"))
        print(f"  ├─ 📊 Volume: {vol_result['ratio']}x avg | Score adj: {vol_result['score_add']:+.1f}")

    # Noisy hour score threshold
    noisy_min_score = get_noisy_hour_min_score()
    effective_min_score = max(float(get_current_min_score()), noisy_min_score)
    if noisy_min_score > float(MIN_SCORE):
        print(f"  ├─ 🕐 Noisy hour detected — min score raised to {noisy_min_score}")

    # Re-filter confirmations after new filters
    if final_direction:
        confirmations = [
            text for text, d in tagged_confirmations
            if d == final_direction or d == "NEUTRAL"
        ]

    # QUALITY GATE
    if total_score < effective_min_score or len(confirmations) < MIN_CONFIRMATIONS:
        print(f"  ├─ Score: {total_score:.1f}/10 | Confirmations: {len(confirmations)} | Direction: {final_direction or 'NONE'}")
        print(f"  ├─ Bullish evidence: {bullish_evidence:.1f} | Bearish evidence: {bearish_evidence:.1f}")
        if total_score < effective_min_score:
            print(f"  └─ ❌ REJECTED: Score {total_score:.1f} < Min {effective_min_score}")
        else:
            print(f"  └─ ❌ REJECTED: Only {len(confirmations)} confirmations (need {MIN_CONFIRMATIONS})")
        final_direction = None
        confirmations = []
    else:
        print(f"  ├─ Score: {total_score:.1f}/10 | Confirmations: {len(confirmations)} | Direction: {final_direction}")
        print(f"  ├─ Bullish evidence: {bullish_evidence:.1f} | Bearish evidence: {bearish_evidence:.1f}")
        print(f"  └─ ✅ PASSED quality gate")

    total_score = min(10.0, max(0.0, total_score))

    # EXPIRY CALCULATION
    expiry_minutes = calculate_smart_expiry(
        score=total_score,
        atr_value=volatility_analysis.get("atr_value", 0.0),
        trend_strength=trend_analysis.get("strength", 0.5),
        structure=market_structure.get("structure", "Unknown"),
        df=df1,
        candlestick_patterns=candle_patterns,
        chart_patterns=chart_patterns,
        support_resistance=market_structure,
        momentum=momentum_signals
    )

    return {
        "score": round(total_score, 1),
        "direction": final_direction,
        "confirmations": confirmations,
        "expiry_minutes": expiry_minutes,
        "current_min_score_required": float(MIN_SCORE),
        "logic": {
            "trend_analysis": trend_analysis,
            "candle_patterns": candle_patterns,
            "chart_patterns": chart_patterns,
            "market_structure": market_structure,
            "momentum": momentum_signals,
            "volatility": volatility_analysis,
            "bullish_evidence": round(bullish_evidence, 1),
            "bearish_evidence": round(bearish_evidence, 1),
            "directional_bias": f"{max(bullish_ratio, bearish_ratio)*100:.0f}%" if total_evidence > 0 else "0%"
        }
    }

# ============================== TELEGRAM HELPERS ==============================
def safe_send(chat_id: int, text: str, reply_markup=None, retries: int = 3) -> Optional[types.Message]:
    """Send message with retry logic to handle connection issues."""
    for attempt in range(retries):
        try:
            if reply_markup:
                return bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode="HTML")
            else:
                return bot.send_message(chat_id, text, parse_mode="HTML")
        except Exception as e:
            print(f"[TELEGRAM] Send attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    print(f"[TELEGRAM] All {retries} send attempts failed for chat {chat_id}")
    return None

def safe_edit_text(chat_id: int, message_id: int, text: str, reply_markup=None, retries: int = 3) -> bool:
    """Edit message with retry logic."""
    for attempt in range(retries):
        try:
            if reply_markup:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup, parse_mode="HTML")
            else:
                bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML")
            return True
        except Exception as e:
            err_str = str(e)
            if "message is not modified" in err_str.lower():
                return True  # Already correct content
            if "message to edit not found" in err_str.lower():
                return False
            print(f"[TELEGRAM] Edit attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    return False

def safe_delete(chat_id: int, message_id: int) -> bool:
    """Delete message safely."""
    try:
        bot.delete_message(chat_id, message_id)
        return True
    except Exception as e:
        print(f"[TELEGRAM] Delete failed: {e}")
        return False

def load_approved_users() -> set:
    """Load the set of approved chat IDs from persistent memory."""
    with memory_lock:
        mem = load_memory()
        return set(int(x) for x in mem.get("approved_users", []))

def save_approved_users(users: set):
    """Save the set of approved chat IDs to persistent memory."""
    with memory_lock:
        mem = load_memory()
        mem["approved_users"] = list(users)
        save_memory(mem)

def add_approved_user(chat_id: int):
    """Add a chat ID to the approved users list."""
    users = load_approved_users()
    users.add(chat_id)
    save_approved_users(users)

def remove_approved_user(chat_id: int):
    """Remove a chat ID from the approved users list."""
    users = load_approved_users()
    users.discard(chat_id)
    save_approved_users(users)

# In-memory cache of approved users (loaded at startup, updated on grant/deny)
_approved_users: set = set()
_approved_users_lock = threading.Lock()

def _load_approved_users_cache():
    global _approved_users
    with _approved_users_lock:
        _approved_users = load_approved_users()

# ============================== TELEGRAM HANDLERS ==============================
def is_authorized(chat_id: int) -> bool:
    """Returns True if the chat_id is the owner or has been approved by the owner."""
    if chat_id == AUTHORIZED_CHAT_ID:
        return True
    with _approved_users_lock:
        return chat_id in _approved_users


def send_payment_info(chat_id: int):
    """Send payment/subscription info to an unauthorized user."""
    payment_msg = (
        "💳 <b>Zahed Binary Bot — Subscription Required</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔒 This bot requires a subscription to access signals.\n\n"
        "💰 <b>PAYMENT PLANS:</b>\n\n"
        "📅 <b>1 Day Plan:</b>   ₹199\n"
        "📅 <b>1 Week Plan:</b>  ₹999\n"
        "📅 <b>1 Month Plan:</b> ₹3,999\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📩 <b>Contact {ADMIN_USERNAME} to get the UPI ID for payment</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📸 <b>HOW TO GET ACCESS:</b>\n"
        "1️⃣ Contact admin to get UPI ID, then make payment\n"
        "2️⃣ Screenshot the successful payment\n"
        f"3️⃣ Send screenshot to {ADMIN_USERNAME}\n"
        "4️⃣ Mention your Telegram username\n"
        "5️⃣ Wait for approval ✅\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📱 <b>Contact:</b> {ADMIN_USERNAME}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    safe_send(chat_id, payment_msg)


# ============================== PER-USER MEMORY ==============================
# Each user gets their own isolated memory key in bot_memory.json:
#   user_data:<chat_id> -> { capital, days: {...}, ... }

def _user_key(chat_id: int) -> str:
    return f"user_data:{chat_id}"

def load_user_memory(chat_id: int) -> dict:
    with memory_lock:
        mem = load_memory()
        return mem.get(_user_key(chat_id), {})

def save_user_memory(chat_id: int, user_data: dict):
    with memory_lock:
        mem = load_memory()
        mem[_user_key(chat_id)] = user_data
        save_memory(mem)

def get_user_today_key() -> str:
    return now_ist().strftime("%Y-%m-%d")

def user_save_capital(chat_id: int, capital: float):
    ud = load_user_memory(chat_id)
    ud["capital"] = capital
    today = get_user_today_key()
    day = ud.setdefault("days", {}).setdefault(today, {})
    if "starting_capital" not in day:
        day["starting_capital"] = capital
    save_user_memory(chat_id, ud)

def user_set_starting_capital(chat_id: int, capital: float):
    ud = load_user_memory(chat_id)
    ud["capital"] = capital
    today = get_user_today_key()
    day = ud.setdefault("days", {}).setdefault(today, {})
    day["starting_capital"] = capital
    # Also persist the current strategy for this day
    with state_lock:
        current_strategy = STATE.get("strategy")
    if current_strategy:
        day["strategy"] = current_strategy
    save_user_memory(chat_id, ud)

def user_update_ending_capital(chat_id: int, capital: float):
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    day = ud.setdefault("days", {}).setdefault(today, {})
    day["ending_capital"] = capital
    ud["capital"] = capital
    save_user_memory(chat_id, ud)

def user_record_withdrawal(chat_id: int, amount: float, capital_before: float, capital_after: float):
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    day = ud.setdefault("days", {}).setdefault(today, {})
    wds = day.setdefault("withdrawals", [])
    wds.append({
        "amount": amount,
        "capital_before": capital_before,
        "capital_after": capital_after,
        "time_ist": now_ist().strftime("%H:%M"),
    })
    day["ending_capital"] = capital_after
    ud["capital"] = capital_after
    save_user_memory(chat_id, ud)

def user_load_capital(chat_id: int) -> Optional[float]:
    ud = load_user_memory(chat_id)
    val = ud.get("capital")
    if val is not None:
        try:
            return float(val)
        except Exception:
            return None
    return None

def user_get_capital_for_date(chat_id: int, date_key: str) -> dict:
    ud = load_user_memory(chat_id)
    day = ud.get("days", {}).get(date_key, {})
    return {"starting": day.get("starting_capital"), "ending": day.get("ending_capital")}

def user_get_today_has_capital(chat_id: int) -> bool:
    return user_get_capital_for_date(chat_id, get_user_today_key())["starting"] is not None

def user_get_signal_no(chat_id: int) -> int:
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    return ud.get("days", {}).get(today, {}).get("next_signal_no", 1)

def user_save_signal_no(chat_id: int, next_no: int):
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    ud.setdefault("days", {}).setdefault(today, {})["next_signal_no"] = next_no
    save_user_memory(chat_id, ud)

def user_record_signal(chat_id: int, signal_no: int, pair: str, direction: str,
                       entry_ist: str, expiry_ist: str, expiry_minutes: int, score: float):
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    day = ud.setdefault("days", {}).setdefault(today, {})
    sigs = day.setdefault("signals", {})
    sigs[str(signal_no)] = {
        "no": signal_no, "pair": pair, "direction": direction,
        "entry_ist": entry_ist, "expiry_ist": expiry_ist,
        "expiry_minutes": expiry_minutes, "score": score,
        "result": None, "accepted": True,
    }
    save_user_memory(chat_id, ud)

def user_update_signal_result(chat_id: int, signal_no: int, result: str) -> bool:
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    try:
        ud["days"][today]["signals"][str(signal_no)]["result"] = result
        save_user_memory(chat_id, ud)
        return True
    except (KeyError, TypeError):
        return False

def user_get_signals_for_date(chat_id: int, date_key: str) -> dict:
    ud = load_user_memory(chat_id)
    return ud.get("days", {}).get(date_key, {}).get("signals", {})

def user_save_martingale(chat_id: int, consecutive_losses: int, stoploss_hit: bool):
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    day = ud.setdefault("days", {}).setdefault(today, {})
    day["consecutive_losses"] = consecutive_losses
    day["stoploss_hit"] = stoploss_hit
    save_user_memory(chat_id, ud)

def user_load_martingale(chat_id: int) -> dict:
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    day = ud.get("days", {}).get(today, {})
    return {
        "consecutive_losses": day.get("consecutive_losses", 0),
        "stoploss_hit": day.get("stoploss_hit", False),
    }

def user_save_pair_losses(chat_id: int, pair_losses: dict):
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    ud.setdefault("days", {}).setdefault(today, {})["pair_consecutive_losses"] = pair_losses
    save_user_memory(chat_id, ud)

def user_load_pair_losses(chat_id: int) -> dict:
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    return dict(ud.get("days", {}).get(today, {}).get("pair_consecutive_losses", {}))

def user_get_withdrawals(chat_id: int, date_key: str) -> list:
    ud = load_user_memory(chat_id)
    return ud.get("days", {}).get(date_key, {}).get("withdrawals", [])

def user_record_deposit(chat_id: int, amount: float):
    """Record a deposit for today."""
    ud = load_user_memory(chat_id)
    today = get_user_today_key()
    day = ud.setdefault("days", {}).setdefault(today, {})
    deposits = day.setdefault("deposits", [])
    deposits.append({
        "amount": amount,
        "time_ist": now_ist().strftime("%H:%M"),
    })
    save_user_memory(chat_id, ud)

def user_get_deposits(chat_id: int, date_key: str) -> list:
    ud = load_user_memory(chat_id)
    return ud.get("days", {}).get(date_key, {}).get("deposits", [])

def user_get_day_data_range(chat_id: int, start_key: str, end_key: str) -> list:
    ud = load_user_memory(chat_id)
    days = ud.get("days", {})
    start_d = datetime.strptime(start_key, "%Y-%m-%d").date()
    end_d   = datetime.strptime(end_key,   "%Y-%m-%d").date()
    result = []
    for k in sorted(days.keys()):
        try:
            d = datetime.strptime(k, "%Y-%m-%d").date()
            if start_d <= d <= end_d:
                result.append({"date_key": k, "date": d, **days[k]})
        except ValueError:
            pass
    return result

# ============================== ADMIN COMMANDS (owner only) ==============================

@bot.message_handler(commands=["add"])
def cmd_add(msg):
    """Owner only: /add <chat_id> — grant access to a user."""
    if msg.chat.id != AUTHORIZED_CHAT_ID:
        safe_send(msg.chat.id, "❌ This command is for the admin only.")
        return
    parts = msg.text.strip().split()
    if len(parts) < 2:
        safe_send(msg.chat.id, "⚠️ Usage: <code>/add 123456789</code>")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        safe_send(msg.chat.id, "⚠️ Invalid chat ID. Must be a number.")
        return
    add_approved_user(target_id)
    with _approved_users_lock:
        _approved_users.add(target_id)
    safe_send(msg.chat.id,
        f"✅ <b>Access Granted</b>\n\n"
        f"🆔 Chat ID <code>{target_id}</code> has been added.\n"
        f"They can now use the bot."
    )
    safe_send(target_id,
        f"✅ <b>Access Approved!</b>\n\n"
        f"You have been granted access by {ADMIN_USERNAME}.\n"
        f"Use /start to begin. 🚀"
    )
    print(f"[ACCESS] Admin added Chat ID: {target_id}")

@bot.message_handler(commands=["remove"])
def cmd_remove(msg):
    """Owner only: /remove <chat_id> — revoke access from a user."""
    if msg.chat.id != AUTHORIZED_CHAT_ID:
        safe_send(msg.chat.id, "❌ This command is for the admin only.")
        return
    parts = msg.text.strip().split()
    if len(parts) < 2:
        safe_send(msg.chat.id, "⚠️ Usage: <code>/remove 123456789</code>")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        safe_send(msg.chat.id, "⚠️ Invalid chat ID. Must be a number.")
        return
    remove_approved_user(target_id)
    with _approved_users_lock:
        _approved_users.discard(target_id)
    safe_send(msg.chat.id,
        f"🗑 <b>Access Removed</b>\n\n"
        f"🆔 Chat ID <code>{target_id}</code> has been removed.\n"
        f"They can no longer use the bot."
    )
    try:
        safe_send(target_id,
            f"❌ <b>Access Revoked</b>\n\n"
            f"Your access has been removed by {ADMIN_USERNAME}.\n"
            f"Contact {ADMIN_USERNAME} if you believe this is a mistake."
        )
    except Exception:
        pass
    print(f"[ACCESS] Admin removed Chat ID: {target_id}")

@bot.message_handler(commands=["authorisers"])
def cmd_authorisers(msg):
    """Owner only: list all currently authorised chat IDs."""
    if msg.chat.id != AUTHORIZED_CHAT_ID:
        safe_send(msg.chat.id, "❌ This command is for the admin only.")
        return
    users = load_approved_users()
    if not users:
        safe_send(msg.chat.id, "📋 <b>Authorised Users</b>\n\nNo users approved yet.")
        return
    lines = ["📋 <b>Authorised Users</b>", "━━━━━━━━━━━━━━━━━━━━━━━━", ""]
    for idx, uid in enumerate(sorted(users), 1):
        lines.append(f"{idx}. <code>{uid}</code>")
    lines += ["", f"Total: <b>{len(users)}</b> user(s)"]
    safe_send(msg.chat.id, "\n".join(lines))

@bot.message_handler(commands=["userreport"])
def cmd_userreport(msg):
    """Owner only: /userreport <chat_id> [dd/mm/yyyy] — view any user's report."""
    if msg.chat.id != AUTHORIZED_CHAT_ID:
        safe_send(msg.chat.id, "❌ This command is for the admin only.")
        return
    parts = msg.text.strip().split(maxsplit=2)
    if len(parts) < 2:
        safe_send(msg.chat.id,
            "⚠️ Usage:\n"
            "<code>/userreport 123456789</code> — today's report\n"
            "<code>/userreport 123456789 dd/mm/yyyy</code> — specific date"
        )
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        safe_send(msg.chat.id, "⚠️ Invalid chat ID.")
        return
    arg = parts[2].strip() if len(parts) > 2 else ""
    try:
        if not arg:
            date_key = get_today_key()
            display = now_ist().strftime("%d/%m/%Y")
        else:
            date_key = parse_date_arg(arg)
            display = arg
        signals = user_get_signals_for_date(target_id, date_key)
        cap = user_get_capital_for_date(target_id, date_key)
        wds = user_get_withdrawals(target_id, date_key)
        ud_target = load_user_memory(target_id)
        target_day_strategy = ud_target.get("days", {}).get(date_key, {}).get("strategy")
        deps = user_get_deposits(target_id, date_key)
        report = build_report(signals, display,
                              starting_capital=cap["starting"],
                              ending_capital=cap["ending"],
                              withdrawals=wds,
                              deposits=deps)
        safe_send(msg.chat.id, f"👤 <b>Report for user <code>{target_id}</code></b>\n\n" + report)
    except Exception as e:
        safe_send(msg.chat.id, f"⚠️ Error: {e}")

@bot.message_handler(commands=["userinfo"])
def cmd_userinfo(msg):
    """Owner only: /userinfo <chat_id> — view full capital & martingale state of any user."""
    if msg.chat.id != AUTHORIZED_CHAT_ID:
        safe_send(msg.chat.id, "❌ This command is for the admin only.")
        return
    parts = msg.text.strip().split()
    if len(parts) < 2:
        safe_send(msg.chat.id, "⚠️ Usage: <code>/userinfo 123456789</code>")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        safe_send(msg.chat.id, "⚠️ Invalid chat ID.")
        return
    ud = load_user_memory(target_id)
    capital = ud.get("capital")
    today = get_today_key()
    day = ud.get("days", {}).get(today, {})
    start_cap = day.get("starting_capital")
    end_cap = day.get("ending_capital")
    cons_losses = day.get("consecutive_losses", 0)
    stoploss = day.get("stoploss_hit", False)
    pair_losses = day.get("pair_consecutive_losses", {})
    sigs = day.get("signals", {})
    profit = sum(1 for s in sigs.values() if s.get("result") == "profit")
    loss = sum(1 for s in sigs.values() if s.get("result") == "loss")
    refund = sum(1 for s in sigs.values() if s.get("result") == "refund")
    pending = len(sigs) - profit - loss - refund
    acc = round((profit / (profit + loss)) * 100) if (profit + loss) > 0 else 0
    blocked = [p for p, v in pair_losses.items() if v >= 2]
    lines = [
        f"👤 <b>User Info: <code>{target_id}</code></b>",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 Date: <b>{now_ist().strftime('%d/%m/%Y')}</b>",
        f"💰 Current Capital: <b>{'$' + f'{capital:.2f}' if capital else 'Not set'}</b>",
        f"🟢 Today Start: <b>{'$' + f'{start_cap:.2f}' if start_cap else 'N/A'}</b>",
        f"💵 Today End:   <b>{'$' + f'{end_cap:.2f}' if end_cap else 'N/A'}</b>",
        f"",
        f"📊 Signals: {len(sigs)} total | 🟢 {profit} | 🔴 {loss}" + (f" | ⚪ {refund}" if refund > 0 else "") + f" | ⏳ {pending} | 🎯 {acc}%",
        f"🔄 Consecutive Losses: <b>{cons_losses}</b>",
        f"🚨 Stoploss Hit: <b>{'Yes' if stoploss else 'No'}</b>",
        f"🚫 Blocked Pairs: <b>{', '.join(blocked) if blocked else 'None'}</b>",
    ]
    safe_send(msg.chat.id, "\n".join(lines))


# ============================== MEMORY CLEAR HELPERS ==============================

def _clear_day_data(day: dict) -> dict:
    """
    Wipe trading data from a single day dict, keeping structure intact.
    Clears: signals, capital, martingale state, pair losses, withdrawals, mm_plan.
    Does NOT touch approved_users (handled at top-level memory).
    """
    return {}   # Return empty — same as a fresh day


def _safe_preserved_keys(mem: dict) -> dict:
    """Return a copy of mem with ONLY the keys that must never be wiped."""
    preserved = {}
    if "approved_users" in mem:
        preserved["approved_users"] = mem["approved_users"]
    # Preserve per-user memory blobs (approved users' own trading data is theirs to clear separately)
    for k, v in mem.items():
        if k.startswith("user_data:"):
            preserved[k] = v
    return preserved


def _clear_memory_for_dates(date_keys: list):
    """
    Clear all trading data for the given list of date keys (YYYY-MM-DD strings)
    from both global memory and all user_data: blobs.
    Authorised users list is always preserved.
    Also resets the global 'capital' and 'days' entries for cleared dates.
    """
    with memory_lock:
        mem = load_memory()

        # --- Global days ---
        days = mem.get("days", {})
        for dk in date_keys:
            if dk in days:
                days[dk] = {}
        mem["days"] = days

        # If today is being cleared, also reset global capital + martingale in STATE
        today = get_today_key()
        if today in date_keys:
            mem.pop("capital", None)

        # --- Per-user data ---
        for k in list(mem.keys()):
            if k.startswith("user_data:"):
                ud = mem[k]
                user_days = ud.get("days", {})
                for dk in date_keys:
                    if dk in user_days:
                        user_days[dk] = {}
                ud["days"] = user_days
                if today in date_keys:
                    ud.pop("capital", None)
                mem[k] = ud

        save_memory(mem)

    # Also reset live STATE if today is being cleared
    if today in date_keys:
        with state_lock:
            STATE["capital"] = None
            STATE["awaiting_capital"] = False
            STATE["awaiting_done_capital"] = False
            STATE["consecutive_losses"] = 0
            STATE["stoploss_hit"] = False
            STATE["pair_consecutive_losses"] = {}
            STATE["signal_no"] = 1
            STATE["active_signal"] = None
            STATE["pending_signals"] = {}
            STATE["pending_results"] = {}
            STATE["waiting_for_result"] = False


def _clear_all_except_users():
    """Wipe ALL trading data — all days for all users. Preserve only approved_users."""
    with memory_lock:
        mem = load_memory()
        preserved = _safe_preserved_keys(mem)
        # Keep approved_users, but clear per-user trading days/capital
        for k in list(preserved.keys()):
            if k.startswith("user_data:"):
                # Keep the user_data key but clear their trading history
                preserved[k] = {}
        save_memory(preserved)

    # Reset live STATE
    with state_lock:
        STATE["capital"] = None
        STATE["awaiting_capital"] = False
        STATE["awaiting_done_capital"] = False
        STATE["consecutive_losses"] = 0
        STATE["stoploss_hit"] = False
        STATE["pair_consecutive_losses"] = {}
        STATE["signal_no"] = 1
        STATE["active_signal"] = None
        STATE["pending_signals"] = {}
        STATE["pending_results"] = {}
        STATE["waiting_for_result"] = False


def _clear_older_than(keep_days: int):
    """
    Delete all day entries older than `keep_days` days from today.
    Preserves: approved_users, current capital, and all dates within the keep window.
    """
    cutoff = (now_ist() - timedelta(days=keep_days)).date()
    with memory_lock:
        mem = load_memory()

        # --- Global days ---
        days = mem.get("days", {})
        to_delete = []
        for dk in list(days.keys()):
            try:
                d = datetime.strptime(dk, "%Y-%m-%d").date()
                if d < cutoff:
                    to_delete.append(dk)
            except ValueError:
                pass
        for dk in to_delete:
            del days[dk]
        mem["days"] = days

        # --- Per-user data ---
        for k in list(mem.keys()):
            if k.startswith("user_data:"):
                ud = mem[k]
                user_days = ud.get("days", {})
                for dk in list(user_days.keys()):
                    try:
                        d = datetime.strptime(dk, "%Y-%m-%d").date()
                        if d < cutoff:
                            del user_days[dk]
                    except ValueError:
                        pass
                ud["days"] = user_days
                mem[k] = ud

        save_memory(mem)
    return len(to_delete)


# ============================== CLEAR COMMANDS ==============================

def _wipe_date_for_user(chat_id: int, date_key: str):
    """Delete all data for a specific date from both user memory and global memory."""
    # User memory
    ud = load_user_memory(chat_id)
    if date_key in ud.get("days", {}):
        ud["days"][date_key] = {}
        save_user_memory(chat_id, ud)
    # Global memory
    with memory_lock:
        mem = load_memory()
        if date_key in mem.get("days", {}):
            mem["days"][date_key] = {}
        save_memory(mem)


@bot.message_handler(commands=["clear"])
def cmd_clear(msg):
    """
    /clear          — wipe today's data (signals, capital, deposits, withdrawals)
    /clear DD-MM-YYYY — wipe a specific date's data
    """
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    parts = (msg.text or "").strip().split(maxsplit=1)
    arg   = parts[1].strip() if len(parts) > 1 else ""

    if arg:
        # Parse specific date — accept DD-MM-YYYY or DD/MM/YYYY
        arg_clean = arg.replace("/", "-")
        try:
            parsed = datetime.strptime(arg_clean, "%d-%m-%Y")
            date_key     = parsed.strftime("%Y-%m-%d")
            display_date = parsed.strftime("%d/%m/%Y")
        except ValueError:
            safe_send(msg.chat.id,
                "⚠️ <b>Invalid date format.</b>\n\n"
                "Use: <code>/clear DD-MM-YYYY</code>\n"
                "Example: <code>/clear 02-05-2026</code>"
            )
            return
    else:
        date_key     = get_today_key()
        display_date = now_ist().strftime("%d/%m/%Y") + " (today)"

    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton("✅ Yes, clear it", callback_data=f"clrdate:confirm:{date_key}:{display_date}"),
        types.InlineKeyboardButton("❌ Cancel",         callback_data="clrdate:cancel"),
    )
    safe_send(
        msg.chat.id,
        f"⚠️ <b>Clear data for {display_date}?</b>\n\n"
        f"This will delete:\n"
        f"• All signals & results\n"
        f"• Starting & ending capital\n"
        f"• Deposits & withdrawals\n\n"
        f"<b>This cannot be undone.</b> Are you sure?",
        reply_markup=kb
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("clrdate:"))
def cb_clrdate(cq):
    if not is_authorized(cq.message.chat.id):
        bot.answer_callback_query(cq.id, "❌ Not authorized")
        return

    parts  = cq.data.split(":", 3)
    action = parts[1]

    if action == "cancel":
        safe_edit_text(cq.message.chat.id, cq.message.message_id,
            "❌ <b>Cancelled.</b> Data is unchanged.")
        bot.answer_callback_query(cq.id, "Cancelled")
        return

    date_key     = parts[2]
    display_date = parts[3] if len(parts) > 3 else date_key
    chat_id      = cq.message.chat.id

    _wipe_date_for_user(chat_id, date_key)

    # If cleared date is today — also reset STATE
    if date_key == get_today_key():
        with state_lock:
            STATE["signal_no"]             = 1
            STATE["capital"]               = None
            STATE["active_signal"]         = None
            STATE["waiting_for_result"]    = False
            STATE["awaiting_capital"]      = True
            STATE["awaiting_done_capital"] = False
            STATE["bot_running"]           = False

        safe_edit_text(cq.message.chat.id, cq.message.message_id,
            f"🗑 <b>Cleared: {display_date}</b>\n\n"
            f"✅ Signals, capital, deposits & withdrawals wiped.\n"
            f"✅ Signal counter reset to #01.\n\n"
            f"💰 What is your <b>new starting capital</b> for today?\n"
            f"Reply with the amount, e.g. <code>$110</code>"
        )
    else:
        safe_edit_text(cq.message.chat.id, cq.message.message_id,
            f"🗑 <b>Cleared: {display_date}</b>\n\n"
            f"✅ All data for that date has been deleted."
        )

    bot.answer_callback_query(cq.id, f"✅ {display_date} cleared")
    print(f"[CLEAR] Date {date_key} wiped by user {chat_id}")


@bot.message_handler(commands=["clearall"])
def cmd_clearall(msg):
    """Delete ALL data — every day, every signal, all capital history."""
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton("✅ Yes, delete everything", callback_data="clearall:confirm"),
        types.InlineKeyboardButton("❌ Cancel",                 callback_data="clearall:cancel"),
    )
    safe_send(
        msg.chat.id,
        "⚠️ <b>Clear ALL Data?</b>\n\n"
        "This will permanently delete:\n"
        "• All signals from every day\n"
        "• All capital records\n"
        "• All deposits & withdrawals\n"
        "• Complete trade history\n\n"
        "🚨 <b>This cannot be undone!</b>\n\n"
        "Are you sure?",
        reply_markup=kb
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("clearall:"))
def cb_clearall(cq):
    if not is_authorized(cq.message.chat.id):
        bot.answer_callback_query(cq.id, "❌ Not authorized")
        return

    action  = cq.data.split(":")[1]
    chat_id = cq.message.chat.id

    if action == "cancel":
        safe_edit_text(chat_id, cq.message.message_id,
            "❌ <b>Cancelled.</b> Your data is safe.")
        bot.answer_callback_query(cq.id, "Cancelled")
        return

    # Wipe all user data
    with memory_lock:
        mem      = load_memory()
        user_key = _user_key(chat_id)
        if user_key in mem:
            del mem[user_key]
        mem["days"] = {}
        save_memory(mem)

    with state_lock:
        STATE["signal_no"]             = 1
        STATE["capital"]               = None
        STATE["active_signal"]         = None
        STATE["waiting_for_result"]    = False
        STATE["awaiting_capital"]      = True
        STATE["awaiting_done_capital"] = False
        STATE["bot_running"]           = False
        STATE["selected_pairs"]        = []
        STATE["pair_consecutive_losses"] = {}

    safe_edit_text(
        chat_id, cq.message.message_id,
        "🗑 <b>All data cleared!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ All signals deleted\n"
        "✅ All capital records deleted\n"
        "✅ All deposits & withdrawals deleted\n"
        "✅ Complete history wiped\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Fresh start! Use /start to begin."
    )
    bot.answer_callback_query(cq.id, "✅ All data cleared")
    print(f"[CLEARALL] All data wiped by user {chat_id}")


@bot.message_handler(commands=["clear1"])
def cmd_clear1(msg):
    """Owner only: /clear1 — delete all memory OLDER than 1 month."""
    if msg.chat.id != AUTHORIZED_CHAT_ID:
        safe_send(msg.chat.id, "❌ This command is for the admin only.")
        return

    cutoff_display = (now_ist() - timedelta(days=30)).strftime("%d/%m/%Y")
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Yes, Clear Old Data", callback_data="memclear:old1m:confirm"),
        types.InlineKeyboardButton("❌ Cancel",               callback_data="memclear:cancel"),
    )
    safe_send(
        msg.chat.id,
        f"🗂 <b>Clear Memory Older Than 1 Month?</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Will delete all data <b>before {cutoff_display}</b>.\n\n"
        f"Last 30 days are kept. ✅\n\n"
        f"Are you sure?",
        reply_markup=markup
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("memclear:"))
def handle_memclear_callback(call):
    """Handle old1m clear confirmation."""
    if call.message.chat.id != AUTHORIZED_CHAT_ID:
        bot.answer_callback_query(call.id, "❌ Not authorized")
        return

    parts  = call.data.split(":")
    action = parts[1]

    if action == "cancel":
        try:
            bot.edit_message_text("❎ <b>Clear cancelled.</b> Memory is unchanged.",
                call.message.chat.id, call.message.message_id, parse_mode="HTML")
        except Exception:
            pass
        bot.answer_callback_query(call.id, "Cancelled")
        return

    if action == "old1m":
        try:
            deleted_count  = _clear_older_than(30)
            cutoff_display = (now_ist() - timedelta(days=30)).strftime("%d/%m/%Y")
            text = (
                f"🗂 <b>Old Memory Cleared</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Removed <b>{deleted_count}</b> day record(s) older than <b>{cutoff_display}</b>.\n\n"
                f"Last 30 days kept. ✅"
            )
            print(f"[CLEAR] Deleted {deleted_count} old day entries")
        except Exception as e:
            text = f"⚠️ <b>Error:</b> {e}"
    else:
        bot.answer_callback_query(call.id, "Unknown action")
        return

    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML")
    except Exception:
        safe_send(call.message.chat.id, text)
    bot.answer_callback_query(call.id, "Done ✅")


@bot.message_handler(commands=["start"])
def cmd_start(msg):
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        username = msg.from_user.username or "No username"
        first_name = msg.from_user.first_name or "Unknown"
        markup = types.InlineKeyboardMarkup()
        # Use ":" as separator — safe for all numeric chat IDs
        btn_yes = types.InlineKeyboardButton("✅ Approve", callback_data=f"useraccess:grant:{msg.chat.id}")
        btn_no  = types.InlineKeyboardButton("❌ Reject",  callback_data=f"useraccess:deny:{msg.chat.id}")
        markup.add(btn_yes, btn_no)
        try:
            safe_send(
                AUTHORIZED_CHAT_ID,
                f"🔔 <b>New Access Request</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 <b>Name:</b> {first_name}\n"
                f"📱 <b>Username:</b> @{username}\n"
                f"🆔 <b>Chat ID:</b> <code>{msg.chat.id}</code>\n\n"
                f"Tap a button to approve or reject this user:",
                reply_markup=markup
            )
        except Exception as e:
            print(f"[ERROR] Could not notify owner: {e}")
        return

    chat_id = msg.chat.id

    with state_lock:
        STATE["bot_running"] = False
        STATE["selected_pairs"] = []
        STATE["telegram_chat_id"] = chat_id
        STATE["signal_no"] = user_get_signal_no(chat_id)
        STATE["strategy"] = "manual"
        STATE["awaiting_strategy"] = False
        STATE["awaiting_capital"] = False
        STATE["awaiting_done_capital"] = False

    today_key  = get_today_key()
    cap_data   = user_get_capital_for_date(chat_id, today_key)
    today_cap  = cap_data["starting"]

    # ── Check if YESTERDAY's ending capital was never recorded ──────────
    from datetime import timedelta
    yesterday_key    = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday_data   = user_get_capital_for_date(chat_id, yesterday_key)
    yesterday_start  = yesterday_data["starting"]
    yesterday_end    = yesterday_data["ending"]

    if yesterday_start is not None and yesterday_end is None:
        # Previous day had a session but no /done was recorded
        with state_lock:
            STATE["awaiting_done_capital"] = True
        safe_send(
            chat_id,
            "⚠️ <b>Yesterday's session was not closed!</b>\n\n"
            f"📅 You started yesterday with <b>${yesterday_start:.2f}</b> but never recorded your ending capital.\n\n"
            "Please enter your <b>ending capital for yesterday</b> now so your report stays accurate.\n\n"
            "Reply with the amount, e.g. <code>$95</code>"
        )
        return

    # ── Today's capital already set → go straight to pair selector ──────
    if today_cap and today_cap > 0:
        with state_lock:
            STATE["capital"] = today_cap
        kb = types.InlineKeyboardMarkup(row_width=2)
        for p in ALL_PAIRS:
            kb.add(types.InlineKeyboardButton(p, callback_data=f"pair:{p}"))
        kb.add(types.InlineKeyboardButton("✅ START BOT", callback_data="pairs:start"))
        safe_send(
            chat_id,
            f"👋 <b>Welcome back!</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 Today's capital: <b>${today_cap:.2f}</b>\n\n"
            f"📊 <b>Select your trading pairs:</b>\n"
            f"Tap pairs to select/deselect, then press START BOT",
            reply_markup=kb
        )
        return

    # ── No capital for today yet → ask for it ───────────────────────────
    prev_cap = yesterday_data["ending"] or user_load_capital(chat_id)
    prev_hint = f"\n💡 Yesterday you ended with <b>${prev_cap:.2f}</b>." if prev_cap else ""

    safe_send(
        chat_id,
        "👋 <b>Welcome to RealZahedBinaryBot!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"What is your <b>starting capital</b> for today?{prev_hint}\n\n"
        "Reply with just the amount, e.g. <code>$100</code> or <code>$50</code>\n\n"
        "<i>After you enter your capital I'll show you the pair selector.</i>"
    )
    with state_lock:
        STATE["awaiting_capital"] = True

@bot.callback_query_handler(func=lambda call: call.data.startswith("useraccess:"))
def handle_approval_callback(call):
    """Handle approve/deny buttons sent to the owner for new access requests."""
    # Only owner can approve/deny
    if call.message.chat.id != AUTHORIZED_CHAT_ID:
        bot.answer_callback_query(call.id, "❌ Not authorized")
        return

    try:
        # Format: useraccess:grant:CHATID or useraccess:deny:CHATID
        parts = call.data.split(":", 2)
        action       = parts[1]           # "grant" or "deny"
        user_chat_id = int(parts[2])      # the requesting user's chat ID
    except (IndexError, ValueError):
        bot.answer_callback_query(call.id, "❌ Invalid data")
        return

    if action == "grant":
        # Add to approved users in memory and in-memory cache
        add_approved_user(user_chat_id)
        with _approved_users_lock:
            _approved_users.add(user_chat_id)

        # Update the owner's message
        try:
            bot.edit_message_text(
                f"✅ <b>Access Approved</b>\n\n"
                f"🆔 Chat ID <code>{user_chat_id}</code> has been granted access.\n"
                f"They can now use the bot.",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="HTML"
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id, "✅ Access granted!")

        # Notify the user
        safe_send(
            user_chat_id,
            f"✅ <b>Access Approved!</b>\n\n"
            f"Your request has been approved by {ADMIN_USERNAME}.\n\n"
            f"Use /start to begin using the bot. 🚀"
        )
        print(f"[ACCESS] Approved Chat ID: {user_chat_id}")

    elif action == "deny":
        # Remove from approved users in case they were previously approved
        remove_approved_user(user_chat_id)
        with _approved_users_lock:
            _approved_users.discard(user_chat_id)

        # Update the owner's message
        try:
            bot.edit_message_text(
                f"❌ <b>Access Rejected</b>\n\n"
                f"🆔 Chat ID <code>{user_chat_id}</code> has been denied access.",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="HTML"
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id, "❌ Access rejected")

        # Notify the user
        safe_send(
            user_chat_id,
            f"❌ <b>Access Rejected</b>\n\n"
            f"{ADMIN_USERNAME} has rejected your access request.\n\n"
            f"If you believe this is a mistake or have made a payment, "
            f"please contact {ADMIN_USERNAME} directly."
        )
        print(f"[ACCESS] Rejected Chat ID: {user_chat_id}")

    else:
        bot.answer_callback_query(call.id, "❌ Unknown action")


@bot.message_handler(commands=["pairs"])
def cmd_pairs(msg):
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    with state_lock:
        current_capital = STATE.get("capital")
        already_selected = STATE.get("selected_pairs", [])[:]
        was_running = STATE.get("bot_running", False)
        chat_id = msg.chat.id
        STATE["strategy"] = "manual"

    # Check if today's starting capital is already recorded for this user
    today_has_capital = user_get_today_has_capital(chat_id)

    # If no starting capital for today yet — always ask
    if not today_has_capital:
        with state_lock:
            STATE["awaiting_capital"] = True
            STATE["awaiting_done_capital"] = False
        prev_cap = user_load_capital(chat_id)
        prev_hint = f"\n💰 Yesterday's ending capital was <b>${prev_cap:.2f}</b>.\nEnter today's starting capital:" if prev_cap else ""
        prompt = (
            f"💰 <b>What's your starting capital for today?</b>{prev_hint}\n\n"
            "Reply with just the amount, e.g. <code>$100</code> or <code>$50</code>\n\n"
            "<i>Capital must match the Money Management plan. After you reply I'll show you the pair selector.</i>"
        )
        safe_send(chat_id, prompt)
        return

    # Today's capital is set — use it
    display_capital = user_get_capital_for_date(chat_id, get_today_key())["starting"]
    with state_lock:
        STATE["capital"] = display_capital

    kb = types.InlineKeyboardMarkup(row_width=2)
    selected_set = set(already_selected)
    for p in ALL_PAIRS:
        mark = "✅ " if p in selected_set else ""
        kb.add(types.InlineKeyboardButton(f"{mark}{p}", callback_data=f"pair:{p}"))
    kb.add(types.InlineKeyboardButton("✅ START BOT", callback_data="pairs:start:nonnews" if was_running else "pairs:start"))
    hint = "\n<i>Already-selected pairs shown with ✅ — tap to deselect or add more.</i>" if already_selected else ""
    cap_str = f"${display_capital:.2f}" if display_capital else "Not set"
    safe_send(
        chat_id,
        f"📊 <b>Select Trading Pairs</b>\n\n"
        f"💰 Today's Capital: <b>{cap_str}</b>\n{hint}\n"
        f"Tap pairs to select/deselect, then press START BOT",
        reply_markup=kb
    )

@bot.message_handler(commands=["status"])
def cmd_status(msg):
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return
    with state_lock:
        running = STATE["bot_running"]
        pairs = STATE["selected_pairs"]
        active = STATE.get("active_signal")
        signal_no = STATE["signal_no"]
        waiting_result = STATE.get("waiting_for_result", False)
        capital = STATE.get("capital")

    status_emoji = "🟢 ACTIVE" if running else "🔴 STOPPED"
    pair_display = ", ".join(pairs[:4]) if pairs else "None"
    if len(pairs) > 4:
        pair_display += f" +{len(pairs)-4} more"

    if active:
        if waiting_result:
            activity = f"⏳ Waiting for result — Signal #{active['no']:02d} ({active['pair']})"
        else:
            activity = f"📊 Signal #{active['no']:02d} active ({active['pair']})"
    elif running:
        activity = "🔍 Scanning for signals"
    else:
        activity = "⏸ Stopped"

    # Min score in effect
    min_score_now = get_current_min_score()

    msg_text = (
        f"🤖 <b>Bot Status</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔋 <b>Status:</b> {status_emoji}\n"
        f"💹 <b>Pairs:</b> {pair_display}\n"
        f"📊 <b>Activity:</b> {activity}\n"
        f"🔢 <b>Next Signal:</b> #{signal_no:02d}\n"
        f"💰 <b>Capital:</b> {'$' + f'{capital:.2f}' if capital else 'Not set'}\n"
        f"🎯 <b>Min Score:</b> {min_score_now}/10\n"
        f"⏳ <b>Expiry Range:</b> 1–5 min\n"
        f"⚡ <b>Signal Advance:</b> ~30s before entry\n"
        f"⏰ <b>Time:</b> {now_ist().strftime('%H:%M IST')}\n"
    )
    safe_send(msg.chat.id, msg_text)

@bot.message_handler(commands=["stop"])
def cmd_stop(msg):
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return
    with state_lock:
        was_running = STATE["bot_running"]
        active = STATE.get("active_signal")
        waiting = STATE.get("waiting_for_result", False)
        STATE["bot_running"] = False
        STATE["waiting_for_result"] = False
        # Do NOT clear active_signal here — preserve it so on /pairs resume
        # the scheduler can detect an unresolved trade and ask for its result.
        # Only clear if nothing was live.
        if not active:
            STATE["active_signal"] = None

    if was_running:
        print(f"[{now_ist().strftime('%H:%M:%S')}] ⏹  Bot STOPPED by user command")
        if active and not waiting:
            # Signal was live or expired but result not recorded yet
            safe_send(
                msg.chat.id,
                "⏹️ <b>Bot Stopped</b>\n\n"
                f"⚠️ You had an active signal on <b>{active.get('pair','?')}</b> "
                f"(expiry {active.get('expiry_ist','?')} IST).\n\n"
                "When you restart with /pairs, the bot will ask for that trade result first "
                "so your martingale amount stays correct.\n\n"
                "Use /pairs to restart."
            )
        else:
            safe_send(
                msg.chat.id,
                "⏹️ <b>Bot Stopped</b>\n\nUse /pairs to select pairs and restart"
            )
    else:
        safe_send(msg.chat.id, "ℹ️ Bot is already stopped")

@bot.message_handler(commands=['api'])
def cmd_api(msg):
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return
    safe_send(msg.chat.id, "🔍 Checking API keys...")
    api_status = run_api_check()
    safe_send(msg.chat.id, api_status)
    
@bot.message_handler(commands=["test_alert"])
def handle_test_alert(msg):
    if not is_authorized(msg.chat.id):
        return
    safe_send(
        msg.chat.id,
        "🚩 <b>SIGNAL #99 (LIVE)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔤 <b>Asset Pair:</b> EUR/USD-OTC\n"
        "↕️ <b>Action:</b> CALL (BUY)\n"
        "⏳ <b>Duration:</b> 1 Minute\n"
        "🕐 <b>Entry Time:</b> NOW IST\n"
        "⚖️ <b>Confidence:</b> 88% (High)\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🎯 <i>This is a manual alert test. Good luck!</i>"
    )
    
@bot.message_handler(commands=['about'])
def cmd_about(msg):
    about_msg = (
        "🤖 <b>RealZahedBinaryBot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "RealZahedBinaryBot is a Binary Trading Bot designed for the <b>Quotex</b> platform.\n\n"
        "👨‍💻 Created by <b>Mohd Zahed</b>\n"
        f"📱 Telegram: {ADMIN_USERNAME}\n"
        "📸 Instagram: @therealzahed6\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✨ <b>Advantages</b>\n\n"
        "📰 <b>Built-in News Filter</b>\n"
        "  Automatically detects high-impact forex news and pauses signals to protect your trades.\n\n"
        "💰 <b>Full Capital Control</b>\n"
        "  You manage your own money — the bot focuses purely on signal quality.\n\n"
        "📊 <b>Daily Tracking</b>\n"
        "  Track deposits, withdrawals, starting and ending capital with /deposit, /withdraw, and /done.\n\n"
        "📈 <b>Detailed Reports</b>\n"
        "  View profit/loss, signal accuracy, and full session history with /report.\n\n"
        "📈 <b>Multi-Timeframe Analysis</b>\n"
        "  Analyses 1m, 5m, and 15m charts simultaneously for high-confluence signals.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📋 Use /commands to view all available commands.\n\n"
        "⚠️ <i>Trading involves risk. Use responsibly.</i>"
    )
    safe_send(msg.chat.id, about_msg)
    if not is_authorized(msg.chat.id):
        def _send_payment_delayed():
            time.sleep(2)
            send_payment_info(msg.chat.id)
        threading.Thread(target=_send_payment_delayed, daemon=True).start()

@bot.message_handler(commands=['commands'])
def cmd_commands(msg):
    is_admin = (msg.chat.id == AUTHORIZED_CHAT_ID)
    user_commands = (
        "📋 <b>RealZahedBinaryBot Commands</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🚀 <b>Trading</b>\n"
        "/pairs — Select pairs & start the bot\n"
        "/stop — Stop the bot\n"
        "/status — View bot status and active signal\n\n"
        "💰 <b>Capital & Transactions</b>\n"
        "/done — Record ending capital for the day\n"
        "/deposit — Record a deposit (e.g. /deposit $50)\n"
        "/withdraw — Record a withdrawal (e.g. /withdraw $20)\n"
        "/clear — Reset today's capital & signals (fresh start today)\n"
        "/clearall — Delete all data completely\n\n"
        "📊 <b>Reports & Stats</b>\n"
        "/accuracy — Live win rate for today\n"
        "/besthour — Best & worst trading hours from your history\n"
        "/report — Full signal accuracy + PnL report\n\n"
        "📰 <b>News</b>\n"
        "/news — Show today's high-impact forex events\n\n"
        "ℹ️ <b>Info</b>\n"
        "/about — About ZahedBinaryBot\n"
        "/commands — Show this commands list\n"
    )
    admin_commands = (
        "\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔐 <b>Admin Commands</b>\n"
        "/api — Check API keys status\n"
        "/reset — Reset today's signal counter\n"
        "/deletereport — Delete report data by date\n"
        "/userinfo — View a user's trading info\n"
    )
    msg_text = user_commands + (admin_commands if is_admin else "")
    safe_send(msg.chat.id, msg_text)

@bot.message_handler(commands=['accuracy'])
def cmd_accuracy(msg):
    """Quick live accuracy + martingale state overview for today."""
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    today = get_today_key()
    signals = user_get_signals_for_date(msg.chat.id, today)
    with state_lock:
        capital = STATE.get("capital")

    total   = len(signals)
    profit  = sum(1 for s in signals.values() if s.get("result") == "profit")
    loss    = sum(1 for s in signals.values() if s.get("result") == "loss")
    refund  = sum(1 for s in signals.values() if s.get("result") == "refund")
    pending = total - profit - loss - refund
    accuracy = round((profit / (profit + loss)) * 100) if (profit + loss) > 0 else 0

    min_score_now = get_current_min_score()

    safe_send(msg.chat.id,
        f"📊 <b>Today's Live Stats</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 <b>Date:</b> {now_ist().strftime('%d/%m/%Y')}\n\n"
        f"🟢 Profits:  <b>{profit}</b>\n"
        f"🔴 Losses:   <b>{loss}</b>\n"
        + (f"⚪ Refunds:  <b>{refund}</b>\n" if refund > 0 else "")
        + f"⏳ Pending:  <b>{pending}</b>\n"
        f"🎯 Accuracy: <b>{accuracy}%</b> ({profit + loss} trades)\n\n"
        f"💰 <b>Capital:</b> {'$' + f'{capital:.2f}' if capital else 'Not set'}\n"
        f"🎯 <b>Min Score Now:</b> {min_score_now}/10\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Use /report for full detailed report.</i>"
    )

@bot.message_handler(commands=['besthour'])
def cmd_besthour(msg):
    """Show best and worst trading hours based on historical win/loss data."""
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    with state_lock:
        hour_performance = dict(STATE.get("hour_performance", {}))
        pair_performance = dict(STATE.get("pair_performance", {}))

    # --- Build hour stats table ---
    hour_rows = []
    for hour_key, data in hour_performance.items():
        wins = data.get("wins", 0)
        losses = data.get("losses", 0)
        total = wins + losses
        if total == 0:
            continue
        wr = round((wins / total) * 100)
        hour_int = int(hour_key)
        # Format IST label e.g. "14:00 – 14:59"
        label = f"{hour_int:02d}:00 – {hour_int:02d}:59 IST"
        hour_rows.append((hour_int, label, wins, losses, total, wr))

    # Sort by win rate descending
    hour_rows.sort(key=lambda x: x[5], reverse=True)

    # --- Build pair stats table ---
    pair_rows = []
    for pair, data in pair_performance.items():
        wins = data.get("wins", 0)
        losses = data.get("losses", 0)
        total = wins + losses
        if total == 0:
            continue
        wr = round((wins / total) * 100)
        pair_rows.append((pair, wins, losses, total, wr))

    pair_rows.sort(key=lambda x: x[4], reverse=True)

    # --- Compose message ---
    if not hour_rows and not pair_rows:
        safe_send(msg.chat.id,
            "📊 <b>Best Trading Hours</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ No trading data yet.\n\n"
            "Accept signals and mark results (Profit/Loss) to build your history. "
            "After 5–10 trades the bot will show your best hours and pairs."
        )
        return

    lines = [
        "⏰ <b>Best Trading Hours Report</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # Hour performance section
    if hour_rows:
        lines.append("🕐 <b>Hour-by-Hour Performance (IST)</b>")
        lines.append("")
        for i, (hour_int, label, wins, losses, total, wr) in enumerate(hour_rows):
            if wr >= 70:
                badge = "🟢"
                tip = "Best"
            elif wr >= 55:
                badge = "🟡"
                tip = "Good"
            elif wr >= 45:
                badge = "🟠"
                tip = "Average"
            else:
                badge = "🔴"
                tip = "Avoid"

            lines.append(
                f"{badge} <b>{label}</b>\n"
                f"   {tip} — {wr}% ({wins}W / {losses}L / {total} trades)"
            )
            lines.append("")

    # Best window summary
    if len(hour_rows) >= 2:
        best_hours = [r for r in hour_rows if r[5] >= 60 and r[3] >= 2]
        if best_hours:
            lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
            lines.append("✅ <b>Your Best Windows to Trade:</b>")
            lines.append("")
            seen_windows = set()
            for hour_int, label, wins, losses, total, wr in best_hours[:5]:
                h = hour_int
                window = f"{h:02d}:00 – {h+1:02d}:59 IST"
                if window not in seen_windows:
                    seen_windows.add(window)
                    lines.append(f"  🎯 {window} — {wr}% win rate")
            lines.append("")

        avoid_hours = [r for r in hour_rows if r[5] < 45 and r[3] >= 2]
        if avoid_hours:
            lines.append("⛔ <b>Hours to Avoid:</b>")
            lines.append("")
            for hour_int, label, wins, losses, total, wr in avoid_hours[:3]:
                lines.append(f"  ❌ {hour_int:02d}:00 – {hour_int:02d}:59 IST — {wr}% win rate")
            lines.append("")

    # Pair performance section
    if pair_rows:
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("💹 <b>Pair Performance</b>")
        lines.append("")
        for pair, wins, losses, total, wr in pair_rows:
            if wr >= 70:
                badge = "🟢"
            elif wr >= 55:
                badge = "🟡"
            elif wr >= 45:
                badge = "🟠"
            else:
                badge = "🔴"
            lines.append(f"{badge} <b>{pair}</b> — {wr}% ({wins}W / {losses}L)")
        lines.append("")

    total_signals = sum(r[4] for r in hour_rows)
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📈 Based on <b>{total_signals}</b> total trades in history.")
    lines.append("<i>More trades = more accurate recommendations.</i>")

    safe_send(msg.chat.id, "\n".join(lines))


@bot.message_handler(commands=['reset'])
def cmd_reset(msg):
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return
    with state_lock:
        current_signal = STATE["signal_no"]
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✅ Yes, Reset", callback_data="reset:confirm"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="reset:cancel")
    )
    confirm_msg = (
        f"⚠️ <b>Reset Signal Counter</b>\n\n"
        f"Current signal number: <b>#{current_signal:02d}</b>\n\n"
        f"This will reset today's counter to #01.\n"
        f"Are you sure?"
    )
    safe_send(msg.chat.id, confirm_msg, reply_markup=kb)

# ============================== NEWS COMMAND ==============================
@bot.message_handler(commands=['news'])
def cmd_news(msg):
    """Show today's news events on demand."""
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    with state_lock:
        events = STATE.get("news_events_today", [])
        pairs = STATE.get("selected_pairs", [])

    if not pairs:
        safe_send(msg.chat.id,
            "⚠️ No pairs selected.\n\nUse /pairs to select pairs first, then the bot will load today's news."
        )
        return

    safe_send(msg.chat.id, "📰 Fetching today's news events...")

    def _fetch_and_reply():
        try:
            fresh_events = fetch_todays_high_impact_news(pairs)
            with state_lock:
                STATE["news_events_today"] = fresh_events
                STATE["news_cache_date"] = now_ist().date()
            if fresh_events:
                brief = format_news_daily_brief(fresh_events)
                safe_send(msg.chat.id, brief)
            else:
                safe_send(msg.chat.id,
                    "✅ <b>No High/Medium Impact News Today</b>\n\n"
                    "No major forex events found for your selected pairs.\n"
                    "Market conditions should be relatively calm."
                )
        except Exception as e:
            safe_send(msg.chat.id, f"⚠️ Could not fetch news: {str(e)[:100]}")

    threading.Thread(target=_fetch_and_reply, daemon=True).start()

# ============================== REPORT COMMAND ==============================
@bot.message_handler(commands=['report'])
def cmd_report(msg):
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    parts = msg.text.strip().split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else ""

    try:
        if not arg:
            # Today — single day report
            date_key = get_today_key()
            signals  = user_get_signals_for_date(msg.chat.id, date_key)
            display  = now_ist().strftime("%d/%m/%Y")
            cap      = user_get_capital_for_date(msg.chat.id, date_key)
            wds      = user_get_withdrawals(msg.chat.id, date_key)
            deps     = user_get_deposits(msg.chat.id, date_key)
            report   = build_report(signals, display,
                                    starting_capital=cap["starting"],
                                    ending_capital=cap["ending"],
                                    withdrawals=wds,
                                    deposits=deps)

        elif " to " in arg:
            raw_start, raw_end = arg.split(" to ", 1)
            start_key = parse_date_arg(raw_start.strip())
            end_key   = parse_date_arg(raw_end.strip())
            display   = f"{raw_start.strip()} to {raw_end.strip()}"
            day_list = user_get_day_data_range(msg.chat.id, start_key, end_key)
            report = _build_range_report_from_daylist(day_list, display)

        elif "/" in arg:
            parts2 = arg.split("/")
            if len(parts2) == 2:
                month, year = int(parts2[0]), int(parts2[1])
                start_key = f"{year}-{month:02d}-01"
                if month == 12:
                    end_key = f"{year+1}-01-01"
                else:
                    end_key = f"{year}-{month+1:02d}-01"
                end_dt  = datetime.strptime(end_key, "%Y-%m-%d").date() - timedelta(days=1)
                end_key = end_dt.strftime("%Y-%m-%d")
                display = f"{parts2[0]}/{parts2[1]}"
                day_list = user_get_day_data_range(msg.chat.id, start_key, end_key)
                report = _build_range_report_from_daylist(day_list, display)
            elif len(parts2) == 3:
                date_key = parse_date_arg(arg)
                signals  = user_get_signals_for_date(msg.chat.id, date_key)
                display  = arg
                cap      = user_get_capital_for_date(msg.chat.id, date_key)
                wds      = user_get_withdrawals(msg.chat.id, date_key)
                deps     = user_get_deposits(msg.chat.id, date_key)
                report   = build_report(signals, display,
                                        starting_capital=cap["starting"],
                                        ending_capital=cap["ending"],
                                        withdrawals=wds,
                                        deposits=deps)
            else:
                report = "⚠️ Invalid format. Use: /report, /report dd/mm/yyyy, /report mm/yyyy, /report dd/mm/yyyy to dd/mm/yyyy"
        else:
            report = "⚠️ Invalid format. Use: /report, /report dd/mm/yyyy, /report mm/yyyy, /report dd/mm/yyyy to dd/mm/yyyy"

    except Exception as e:
        report = f"⚠️ Error generating report: {e}"

    safe_send(msg.chat.id, report)

# ============================== /done COMMAND ==============================
@bot.message_handler(commands=["done"])
def cmd_done(msg):
    """User hit target or stop loss. Ask for new capital to calculate PnL."""
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    with state_lock:
        prev_capital = STATE.get("capital")

    if prev_capital is None:
        # No capital on record — just ask what the new capital is
        with state_lock:
            STATE["awaiting_done_capital"] = True
            STATE["awaiting_capital"] = False
        safe_send(
            msg.chat.id,
            "💰 <b>/done noted!</b>\n\n"
            "I don't have your previous capital on record.\n"
            "What is your <b>current capital</b> now?\n\n"
            "Reply with just the amount, e.g. <code>$50</code> or <code>50</code>"
        )
    else:
        with state_lock:
            STATE["awaiting_done_capital"] = True
            STATE["awaiting_capital"] = False
        safe_send(
            msg.chat.id,
            f"✅ <b>Trade Done!</b>\n\n"
            f"📊 Previous Capital: <b>${prev_capital:.2f}</b>\n\n"
            f"What is your <b>current capital</b> now (after this trade)?\n"
            f"Reply with just the amount, e.g. <code>$30</code> or <code>$50</code>"
        )

def parse_capital_amount(text: str) -> Optional[float]:
    """Parse a capital reply like '$30', '$30.50', '$1,000', '30', '30.5' into a float."""
    text = text.strip()
    # Match optional $, then digits with optional thousands commas, then optional decimal part
    match = re.search(r'\$?\s*([\d]{1,3}(?:,[\d]{3})*(?:\.\d+)?|[\d]+(?:\.\d+)?)', text)
    if match:
        raw = match.group(1).replace(",", "")  # strip thousands separators
        try:
            return float(raw)
        except ValueError:
            return None
    return None

# ============================== /withdraw COMMAND ==============================
@bot.message_handler(commands=["withdraw"])
def cmd_withdraw(msg):
    """Record a withdrawal: /withdraw $20 or /withdraw 20"""
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    parts = msg.text.strip().split(maxsplit=1)
    arg   = parts[1].strip() if len(parts) > 1 else ""

    amount = parse_capital_amount(arg)
    if amount is None or amount <= 0:
        safe_send(msg.chat.id,
            "⚠️ <b>Usage:</b> <code>/withdraw $20</code> or <code>/withdraw 20</code>\n\n"
            "Please include the amount you are withdrawing."
        )
        return

    with state_lock:
        current_capital = STATE.get("capital")

    if current_capital is None:
        current_capital = load_capital()

    if current_capital is None:
        safe_send(msg.chat.id,
            "⚠️ No capital on record yet.\n"
            "Use /pairs to set your starting capital first."
        )
        return

    if amount > current_capital:
        safe_send(msg.chat.id,
            f"⚠️ Withdrawal amount <b>${amount:.2f}</b> exceeds current capital <b>${current_capital:.2f}</b>.\n"
            f"Please enter a valid amount."
        )
        return

    capital_after = round(current_capital - amount, 2)

    record_withdrawal(
        amount=amount,
        capital_before=current_capital,
        capital_after=capital_after,
    )
    user_record_withdrawal(msg.chat.id, amount, current_capital, capital_after)
    with state_lock:
        STATE["capital"] = capital_after

    safe_send(msg.chat.id,
        f"💸 <b>Withdrawal Recorded</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Capital Before: <b>${current_capital:.2f}</b>\n"
        f"➖ Withdrawn:      <b>-${amount:.2f}</b>\n"
        f"💵 Capital After:  <b>${capital_after:.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ This withdrawal will appear in your /report."
    )

@bot.message_handler(commands=["deposit"])
def cmd_deposit(msg):
    """Record a deposit: /deposit $50 or /deposit 50"""
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    parts = msg.text.strip().split(maxsplit=1)
    arg   = parts[1].strip() if len(parts) > 1 else ""

    amount = parse_capital_amount(arg)
    if amount is None or amount <= 0:
        safe_send(msg.chat.id,
            "⚠️ <b>Usage:</b> <code>/deposit $50</code> or <code>/deposit 50</code>\n\n"
            "Please include the amount you are depositing."
        )
        return

    user_record_deposit(msg.chat.id, amount)

    safe_send(msg.chat.id,
        f"💰 <b>Deposit Recorded</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"➕ Deposited:      <b>+${amount:.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ This deposit will appear in your /report.\n"
        f"<i>Note: Deposits are not counted as profit.</i>"
    )



def delete_report_for_date(date_key: str) -> bool:
    """Delete all signal data (and capital) for a specific date. Returns True if data existed."""
    with memory_lock:
        mem = load_memory()
        days = mem.get("days", {})
        if date_key in days:
            del days[date_key]
            save_memory(mem)
            return True
        return False

def delete_report_for_range(start_key: str, end_key: str) -> int:
    """Delete all day data in the given date range. Returns number of days deleted."""
    from datetime import date as _date
    start_d = datetime.strptime(start_key, "%Y-%m-%d").date()
    end_d   = datetime.strptime(end_key,   "%Y-%m-%d").date()
    with memory_lock:
        mem  = load_memory()
        days = mem.get("days", {})
        keys_to_delete = [
            k for k in list(days.keys())
            if start_d <= datetime.strptime(k, "%Y-%m-%d").date() <= end_d
        ]
        for k in keys_to_delete:
            del days[k]
        if keys_to_delete:
            save_memory(mem)
        return len(keys_to_delete)

# ============================== /deletereport COMMAND ==============================
@bot.message_handler(commands=["deletereport"])
def cmd_deletereport(msg):
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        return

    parts = msg.text.strip().split(maxsplit=1)
    arg   = parts[1].strip() if len(parts) > 1 else ""

    try:
        if not arg:
            # Delete today
            date_key     = get_today_key()
            display      = now_ist().strftime("%d/%m/%Y")
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("✅ Yes, Delete", callback_data=f"delrep:date:{date_key}"),
                types.InlineKeyboardButton("❌ Cancel",      callback_data="delrep:cancel")
            )
            safe_send(msg.chat.id,
                f"🗑 <b>Delete Report: {display}?</b>\n\n"
                f"This will permanently delete all signal data and capital records for today.\n"
                f"Are you sure?",
                reply_markup=kb
            )

        elif " to " in arg:
            raw_start, raw_end = arg.split(" to ", 1)
            start_key = parse_date_arg(raw_start.strip())
            end_key   = parse_date_arg(raw_end.strip())
            display   = f"{raw_start.strip()} to {raw_end.strip()}"
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("✅ Yes, Delete", callback_data=f"delrep:range:{start_key}:{end_key}"),
                types.InlineKeyboardButton("❌ Cancel",      callback_data="delrep:cancel")
            )
            safe_send(msg.chat.id,
                f"🗑 <b>Delete Report: {display}?</b>\n\n"
                f"This will permanently delete all signal data and capital records for this date range.\n"
                f"Are you sure?",
                reply_markup=kb
            )

        elif "/" in arg:
            parts2 = arg.split("/")
            if len(parts2) == 3:
                date_key = parse_date_arg(arg)
                display  = arg
                kb = types.InlineKeyboardMarkup()
                kb.row(
                    types.InlineKeyboardButton("✅ Yes, Delete", callback_data=f"delrep:date:{date_key}"),
                    types.InlineKeyboardButton("❌ Cancel",      callback_data="delrep:cancel")
                )
                safe_send(msg.chat.id,
                    f"🗑 <b>Delete Report: {display}?</b>\n\n"
                    f"This will permanently delete all signal data and capital records for this date.\n"
                    f"Are you sure?",
                    reply_markup=kb
                )
            else:
                safe_send(msg.chat.id,
                    "⚠️ Invalid format.\n\nUse:\n"
                    "• /deletereport\n"
                    "• /deletereport dd/mm/yyyy\n"
                    "• /deletereport dd/mm/yyyy to dd/mm/yyyy"
                )
        else:
            safe_send(msg.chat.id,
                "⚠️ Invalid format.\n\nUse:\n"
                "• /deletereport\n"
                "• /deletereport dd/mm/yyyy\n"
                "• /deletereport dd/mm/yyyy to dd/mm/yyyy"
            )

    except Exception as e:
        safe_send(msg.chat.id, f"⚠️ Error: {e}")

@bot.callback_query_handler(func=lambda cq: cq.data.startswith("delrep:"))
def cb_deletereport(cq):
    if not is_authorized(cq.message.chat.id):
        bot.answer_callback_query(cq.id, "❌ Unauthorized")
        return

    parts = cq.data.split(":", 2)  # ["delrep", action, ...]
    action = parts[1] if len(parts) > 1 else ""

    if action == "cancel":
        try:
            bot.edit_message_text("❌ Delete cancelled.", cq.message.chat.id, cq.message.message_id)
        except Exception:
            pass
        bot.answer_callback_query(cq.id, "Cancelled")
        return

    if action == "date":
        date_key = parts[2] if len(parts) > 2 else ""
        existed  = delete_report_for_date(date_key)
        display  = datetime.strptime(date_key, "%Y-%m-%d").strftime("%d/%m/%Y")
        if existed:
            result_text = f"🗑 <b>Deleted:</b> All data for <b>{display}</b> has been removed."
            bot.answer_callback_query(cq.id, "✅ Data deleted")
        else:
            result_text = f"ℹ️ No data found for <b>{display}</b> — nothing to delete."
            bot.answer_callback_query(cq.id, "No data found")
        try:
            bot.edit_message_text(result_text, cq.message.chat.id, cq.message.message_id)
        except Exception:
            safe_send(cq.message.chat.id, result_text)
        return

    if action == "range":
        # data is "delrep:range:start_key:end_key" — rejoin to split correctly
        remainder = cq.data[len("delrep:range:"):]
        colon_idx = remainder.index(":")
        start_key = remainder[:colon_idx]
        end_key   = remainder[colon_idx+1:]
        count     = delete_report_for_range(start_key, end_key)
        start_disp = datetime.strptime(start_key, "%Y-%m-%d").strftime("%d/%m/%Y")
        end_disp   = datetime.strptime(end_key,   "%Y-%m-%d").strftime("%d/%m/%Y")
        if count > 0:
            result_text = (
                f"🗑 <b>Deleted:</b> Data for <b>{count} day(s)</b> "
                f"({start_disp} to {end_disp}) has been removed."
            )
            bot.answer_callback_query(cq.id, f"✅ {count} day(s) deleted")
        else:
            result_text = f"ℹ️ No data found for {start_disp} to {end_disp} — nothing to delete."
            bot.answer_callback_query(cq.id, "No data found")
        try:
            bot.edit_message_text(result_text, cq.message.chat.id, cq.message.message_id)
        except Exception:
            safe_send(cq.message.chat.id, result_text)
        return

    bot.answer_callback_query(cq.id, "❌ Unknown action")

def parse_date_arg(s: str) -> str:
    """Convert dd/mm/yyyy to YYYY-MM-DD."""
    parts = s.strip().split("/")
    if len(parts) == 3:
        dd, mm, yyyy = int(parts[0]), int(parts[1]), int(parts[2])
        return f"{yyyy}-{mm:02d}-{dd:02d}"
    raise ValueError(f"Cannot parse date: {s}")

def _build_range_report_from_daylist(day_data_list: list, display: str) -> str:
    """Build a per-day range report from a pre-fetched list of day dicts (per-user aware)."""
    lines = [
        f"📊 <b>REPORT: {display}</b>",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"",
    ]
    if not day_data_list:
        lines.append("No data found for this period.")
        return "\n".join(lines)

    grand_total = grand_profit = grand_loss = grand_pending = grand_refund = 0
    grand_withdrawn = 0.0
    grand_deposited = 0.0
    first_starting = None
    last_ending = None

    for day in day_data_list:
        dk        = day["date_key"]
        disp_date = datetime.strptime(dk, "%Y-%m-%d").strftime("%d/%m/%Y")
        signals   = day.get("signals", {})
        start_cap = day.get("starting_capital")
        end_cap   = day.get("ending_capital")
        wds       = day.get("withdrawals", [])
        deps      = day.get("deposits", [])
        w_total   = sum(w["amount"] for w in wds)
        dep_total = sum(d["amount"] for d in deps)
        d_total   = len(signals)
        d_profit  = sum(1 for s in signals.values() if s.get("result") == "profit")
        d_loss    = sum(1 for s in signals.values() if s.get("result") == "loss")
        d_refund  = sum(1 for s in signals.values() if s.get("result") == "refund")
        d_pending = d_total - d_profit - d_loss - d_refund
        d_acc     = round((d_profit / (d_profit + d_loss)) * 100) if (d_profit + d_loss) > 0 else 0
        grand_total   += d_total
        grand_profit  += d_profit
        grand_loss    += d_loss
        grand_refund  += d_refund
        grand_pending += d_pending
        grand_withdrawn += w_total
        grand_deposited += dep_total
        if first_starting is None and start_cap is not None:
            first_starting = float(start_cap)
        if end_cap is not None:
            last_ending = float(end_cap)
        lines.append(f"📅 <b>{disp_date}</b>")
        if start_cap is not None:
            lines.append(f"  💰 Start: <b>${float(start_cap):.2f}</b>")
        if dep_total > 0:
            lines.append(f"  💳 Deposited: <b>+${dep_total:.2f}</b>")
        if w_total > 0:
            lines.append(f"  💸 Withdrawn: <b>-${w_total:.2f}</b>")
        if end_cap is not None:
            diff = float(end_cap) - (float(start_cap) if start_cap else float(end_cap))
            pct  = (diff / float(start_cap)) * 100 if start_cap and float(start_cap) > 0 else 0
            pnl_icon  = "🟢" if diff >= 0 else "🔴"
            pnl_label = f"+${diff:.2f} (+{pct:.1f}%)" if diff >= 0 else f"-${abs(diff):.2f} ({pct:.1f}%)"
            lines.append(f"  💵 End:   <b>${float(end_cap):.2f}</b>  {pnl_icon} {pnl_label}")
        elif start_cap is not None:
            lines.append(f"  💵 End:   <i>not recorded</i>")
        if d_total > 0:
            sig_line = f"  📊 Signals: {d_total}  🟢 {d_profit}  🔴 {d_loss}"
            if d_refund > 0:
                sig_line += f"  ⚪ {d_refund}"
            if d_pending > 0:
                sig_line += f"  ⏳ {d_pending}"
            sig_line += f"  🎯 {d_acc}%"
            lines.append(sig_line)
        else:
            lines.append(f"  📊 No signals this day")
        lines.append("")

    grand_acc = round((grand_profit / (grand_profit + grand_loss)) * 100) if (grand_profit + grand_loss) > 0 else 0
    lines.append(f"━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📈 <b>TOTAL SUMMARY</b>")
    lines.append(f"")
    if first_starting is not None:
        lines.append(f"💰 Starting Capital: <b>${first_starting:.2f}</b>")
    if grand_deposited > 0:
        lines.append(f"💳 Total Deposited:  <b>+${grand_deposited:.2f}</b>")
    if grand_withdrawn > 0:
        lines.append(f"💸 Total Withdrawn:  <b>-${grand_withdrawn:.2f}</b>")
    if last_ending is not None:
        lines.append(f"💵 Ending Capital:   <b>${last_ending:.2f}</b>")
        if first_starting is not None:
            net = last_ending - first_starting
            pct = (net / first_starting) * 100 if first_starting > 0 else 0
            pnl_icon  = "🟢" if net >= 0 else "🔴"
            pnl_label = f"+${net:.2f} (+{pct:.1f}%)" if net >= 0 else f"-${abs(net):.2f} ({pct:.1f}%)"
            lines.append(f"{pnl_icon} Net PnL:          <b>{pnl_label}</b>")
    lines.append(f"")
    lines.append(f"⚫ Total Signals:         {grand_total}")
    lines.append(f"🟢 Total Profit Signals: {grand_profit}")
    lines.append(f"🔴 Total Loss Signals:   {grand_loss}")
    if grand_refund > 0:
        lines.append(f"⚪ Total Refunds:         {grand_refund}")
    if grand_pending > 0:
        lines.append(f"⏳ Pending Result:        {grand_pending}")
    lines.append(f"")
    lines.append(f"🎯 Overall Accuracy:     {grand_acc}%")
    lines.append(f"━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


def build_report(signals: dict, display_period: str,
                 starting_capital: float = None, ending_capital: float = None,
                 withdrawals: list = None, deposits: list = None, strategy: str = None) -> str:
    """Single-day or aggregated report (no per-day breakdown)."""
    w_total = sum(w["amount"] for w in (withdrawals or []))
    d_total = sum(d["amount"] for d in (deposits or []))

    if not signals:
        cap_line = ""
        if starting_capital is not None:
            cap_line = f"💰 Starting Capital: <b>${starting_capital:.2f}</b>\n"
            if d_total > 0:
                cap_line += f"💳 Total Deposited:  <b>+${d_total:.2f}</b>\n"
            if w_total > 0:
                cap_line += f"💸 Total Withdrawn:  <b>-${w_total:.2f}</b>\n"
            cap_line += "\n"
        return (
            f"📊 <b>REPORT: {display_period}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{cap_line}"
            f"No accepted signals found for this period."
        )

    total    = len(signals)
    profit   = sum(1 for s in signals.values() if s.get("result") == "profit")
    loss     = sum(1 for s in signals.values() if s.get("result") == "loss")
    refund   = sum(1 for s in signals.values() if s.get("result") == "refund")
    pending  = total - profit - loss - refund
    accuracy = round((profit / (profit + loss)) * 100) if (profit + loss) > 0 else 0

    lines = [
        f"📊 <b>REPORT: {display_period}</b>",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"",
    ]

    if starting_capital is not None:
        lines.append(f"💰 Starting Capital: <b>${starting_capital:.2f}</b>")
        if d_total > 0:
            lines.append(f"💳 Total Deposited:  <b>+${d_total:.2f}</b>")
        if w_total > 0:
            lines.append(f"💸 Total Withdrawn:  <b>-${w_total:.2f}</b>")
        if ending_capital is not None:
            diff = ending_capital - starting_capital
            pct  = (diff / starting_capital) * 100 if starting_capital > 0 else 0
            lines.append(f"💵 Ending Capital:   <b>${ending_capital:.2f}</b>")
            pnl_label = f"+${diff:.2f} (+{pct:.1f}%)" if diff >= 0 else f"-${abs(diff):.2f} ({pct:.1f}%)"
            pnl_icon  = "🟢" if diff >= 0 else "🔴"
            lines.append(f"{pnl_icon} Net PnL:          <b>{pnl_label}</b>")
        else:
            lines.append(f"💵 Ending Capital:   <i>not recorded yet — use /done</i>")
        lines.append("")

    lines += [
        f"⚫ Total Signals:         {total}",
        f"🟢 Total Profit Signals: {profit}",
        f"🔴 Total Loss Signals:   {loss}",
    ]
    if refund > 0:
        lines.append(f"⚪ Total Refunds:         {refund}")
    if pending > 0:
        lines.append(f"⏳ Pending Result:        {pending}")
    lines += [
        f"",
        f"🎯 Overall Accuracy:     {accuracy}%",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(lines)


def build_range_report(start_key: str, end_key: str, display: str) -> str:
    """
    Full per-day breakdown report for a date range.
    Shows each day's: starting capital, ending capital, PnL, withdrawals,
    total signals, profit signals, loss signals, accuracy.
    Then a grand summary at the bottom.
    """
    day_data_list = get_day_data_for_range(start_key, end_key)

    lines = [
        f"📊 <b>REPORT: {display}</b>",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"",
    ]

    if not day_data_list:
        lines.append("No data found for this period.")
        return "\n".join(lines)

    grand_total = grand_profit = grand_loss = grand_pending = grand_refund = 0
    grand_withdrawn = 0.0
    first_starting = None
    last_ending     = None

    for day in day_data_list:
        dk        = day["date_key"]
        disp_date = datetime.strptime(dk, "%Y-%m-%d").strftime("%d/%m/%Y")
        signals   = day.get("signals", {})
        start_cap = day.get("starting_capital")
        end_cap   = day.get("ending_capital")
        wds       = day.get("withdrawals", [])
        w_total   = sum(w["amount"] for w in wds)

        d_total   = len(signals)
        d_profit  = sum(1 for s in signals.values() if s.get("result") == "profit")
        d_loss    = sum(1 for s in signals.values() if s.get("result") == "loss")
        d_refund  = sum(1 for s in signals.values() if s.get("result") == "refund")
        d_pending = d_total - d_profit - d_loss - d_refund
        d_acc     = round((d_profit / (d_profit + d_loss)) * 100) if (d_profit + d_loss) > 0 else 0

        grand_total   += d_total
        grand_profit  += d_profit
        grand_loss    += d_loss
        grand_refund  += d_refund
        grand_pending += d_pending
        grand_withdrawn += w_total

        if first_starting is None and start_cap is not None:
            first_starting = float(start_cap)
        if end_cap is not None:
            last_ending = float(end_cap)

        lines.append(f"📅 <b>{disp_date}</b>")

        if start_cap is not None:
            lines.append(f"  💰 Start: <b>${float(start_cap):.2f}</b>")
        if w_total > 0:
            lines.append(f"  💸 Withdrawn: <b>-${w_total:.2f}</b>")
            for w in wds:
                lines.append(
                    f"    ↳ ${w['amount']:.2f} at {w.get('time_ist','—')} "
                    f"(${w['capital_before']:.2f} → ${w['capital_after']:.2f})"
                )
        if end_cap is not None:
            diff = float(end_cap) - (float(start_cap) if start_cap is not None else float(end_cap))
            pct  = (diff / float(start_cap)) * 100 if start_cap and float(start_cap) > 0 else 0
            pnl_icon  = "🟢" if diff >= 0 else "🔴"
            pnl_label = f"+${diff:.2f} (+{pct:.1f}%)" if diff >= 0 else f"-${abs(diff):.2f} ({pct:.1f}%)"
            lines.append(f"  💵 End:   <b>${float(end_cap):.2f}</b>  {pnl_icon} {pnl_label}")
        elif start_cap is not None:
            lines.append(f"  💵 End:   <i>not recorded</i>")

        if d_total > 0:
            sig_line = f"  📊 Signals: {d_total}  🟢 {d_profit}  🔴 {d_loss}"
            if d_refund > 0:
                sig_line += f"  ⚪ {d_refund}"
            if d_pending > 0:
                sig_line += f"  ⏳ {d_pending}"
            sig_line += f"  🎯 {d_acc}%"
            lines.append(sig_line)
        else:
            lines.append(f"  📊 No signals this day")

        lines.append("")  # spacer between days

    # ── Grand summary ────────────────────────────────────────────────────────
    grand_acc = round((grand_profit / (grand_profit + grand_loss)) * 100) if (grand_profit + grand_loss) > 0 else 0
    lines.append(f"━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📈 <b>TOTAL SUMMARY</b>")
    lines.append(f"")
    if first_starting is not None:
        lines.append(f"💰 Starting Capital: <b>${first_starting:.2f}</b>")
    if grand_withdrawn > 0:
        lines.append(f"💸 Total Withdrawn:  <b>-${grand_withdrawn:.2f}</b>")
    if last_ending is not None:
        lines.append(f"💵 Ending Capital:   <b>${last_ending:.2f}</b>")
        if first_starting is not None:
            net = last_ending - first_starting
            pct = (net / first_starting) * 100 if first_starting > 0 else 0
            pnl_icon  = "🟢" if net >= 0 else "🔴"
            pnl_label = f"+${net:.2f} (+{pct:.1f}%)" if net >= 0 else f"-${abs(net):.2f} ({pct:.1f}%)"
            lines.append(f"{pnl_icon} Net PnL:          <b>{pnl_label}</b>")
    lines.append(f"")
    lines.append(f"⚫ Total Signals:         {grand_total}")
    lines.append(f"🟢 Total Profit Signals: {grand_profit}")
    lines.append(f"🔴 Total Loss Signals:   {grand_loss}")
    if grand_refund > 0:
        lines.append(f"⚪ Total Refunds:         {grand_refund}")
    if grand_pending > 0:
        lines.append(f"⏳ Pending Result:        {grand_pending}")
    lines.append(f"")
    lines.append(f"🎯 Overall Accuracy:     {grand_acc}%")
    lines.append(f"━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)

# ============================== CALLBACK HANDLERS ==============================
@bot.callback_query_handler(func=lambda cq: cq.data.startswith("reset:"))
def cb_reset(cq):
    if not is_authorized(cq.message.chat.id):
        bot.answer_callback_query(cq.id, "❌ Unauthorized")
        return
    action = cq.data.split(":", 1)[1]
    if action == "confirm":
        with state_lock:
            old_signal = STATE["signal_no"]
            STATE["signal_no"] = 1
        save_signal_no_for_today(1)
        success_msg = (
            f"✅ <b>Signal Counter Reset</b>\n\n"
            f"Previous: #{old_signal:02d}\n"
            f"Current: #01\n\n"
            f"Next signal will be #01"
        )
        try:
            bot.edit_message_text(success_msg, cq.message.chat.id, cq.message.message_id)
            bot.answer_callback_query(cq.id, "✅ Counter reset to #01")
        except Exception:
            bot.answer_callback_query(cq.id, "Reset successful!")
            safe_send(cq.message.chat.id, success_msg)
    elif action == "cancel":
        try:
            bot.edit_message_text("❌ Reset cancelled", cq.message.chat.id, cq.message.message_id)
            bot.answer_callback_query(cq.id, "Cancelled")
        except Exception:
            bot.answer_callback_query(cq.id, "Cancelled")

@bot.callback_query_handler(func=lambda cq: cq.data.startswith("pair:"))
def cb_pairs(cq):
    if not is_authorized(cq.message.chat.id):
        bot.answer_callback_query(cq.id, "❌ Unauthorized")
        return
    pair = cq.data.split(":", 1)[1]
    with state_lock:
        if pair in STATE["selected_pairs"]:
            STATE["selected_pairs"].remove(pair)
        else:
            STATE["selected_pairs"].append(pair)
        selected = set(STATE["selected_pairs"])
    kb = types.InlineKeyboardMarkup(row_width=2)
    for p in ALL_PAIRS:
        mark = "✅ " if p in selected else ""
        kb.add(types.InlineKeyboardButton(f"{mark}{p}", callback_data=f"pair:{p}"))
    kb.add(types.InlineKeyboardButton("✅ START BOT", callback_data="pairs:start"))
    try:
        bot.edit_message_reply_markup(cq.message.chat.id, cq.message.message_id, reply_markup=kb)
        bot.answer_callback_query(cq.id)
    except Exception:
        bot.answer_callback_query(cq.id, "Selection updated")

@bot.callback_query_handler(func=lambda cq: cq.data.startswith("pairs:start"))
def cb_pairs_start(cq):
    if not is_authorized(cq.message.chat.id):
        bot.answer_callback_query(cq.id, "❌ Unauthorized")
        return
    # Determine if we should skip sending news (mid-session re-select)
    skip_news = cq.data == "pairs:start:nonnews"

    with state_lock:
        pairs = STATE["selected_pairs"][:]
    if not pairs:
        bot.answer_callback_query(cq.id, "❌ No pairs selected!")
        safe_send(cq.message.chat.id, "⚠️ Please select at least one pair")
        return
    try:
        bot.answer_callback_query(cq.id)
    except Exception:
        pass
    market = check_quotex_market_status(pairs)
    if market["all_otc"]:
        with state_lock:
            STATE["bot_running"] = False
        try:
            bot.edit_message_text(market["message"], cq.message.chat.id, cq.message.message_id)
        except Exception:
            safe_send(cq.message.chat.id, market["message"])
        return
    with state_lock:
        STATE["bot_running"] = True
        STATE["waiting_for_result"] = False
        STATE["signal_no"] = get_signal_no_for_today()
        # Restore per-pair loss counters
        STATE["pair_consecutive_losses"] = load_pair_losses()
        # Check if there's an unresolved signal left over from before /stop
        preserved_signal = STATE.get("active_signal")

    # If there's a preserved active signal, ask for its result before scanning
    if preserved_signal:
        sig_no = preserved_signal.get("no")
        today = get_today_key()
        with memory_lock:
            mem = load_memory()
            recorded = mem.get("days", {}).get(today, {}).get("signals", {}).get(str(sig_no))
        if recorded and recorded.get("accepted") and recorded.get("result") is None:
            with state_lock:
                STATE["waiting_for_result"] = True
                STATE["active_signal"] = preserved_signal
            def _ask_pending_result():
                time.sleep(2)  # small delay so the "Bot Started" message sends first
                ask_signal_result(
                    cq.message.chat.id,
                    sig_no,
                    preserved_signal.get("pair", "?"),
                    preserved_signal.get("expiry_ist", "?")
                )
            threading.Thread(target=_ask_pending_result, daemon=True).start()
        else:
            with state_lock:
                STATE["active_signal"] = None

    try:
        selected_text = ", ".join(pairs[:3])
        if len(pairs) > 3:
            selected_text += f" +{len(pairs)-3} more"
        confirm_msg = (
            f"✅ <b>Bot Started!</b>\n\n"
            f"📊 <b>Selected Pairs:</b> {selected_text}\n"
            f"📡 <b>Market:</b> 🟢 LIVE (Real Forex)\n"
            f"🔢 <b>Resuming from Signal:</b> #{STATE['signal_no']:02d}\n"
            f"🔍 Scanning for signals...\n"
        )
        bot.edit_message_text(confirm_msg, cq.message.chat.id, cq.message.message_id)
    except Exception:
        safe_send(cq.message.chat.id, "✅ Bot is now running!")

    if skip_news:
        # Mid-session re-select — no news resend
        print("[NEWS] Skipping news brief — mid-session pairs update")
        return

    # First start of the day — fetch and show today's news brief
    def _send_news_brief():
        try:
            events = fetch_todays_high_impact_news(pairs)
            with state_lock:
                STATE["news_events_today"] = events
                STATE["news_warned_ids"] = set()
                STATE["news_signal_sent_ids"] = set()
                STATE["news_cache_date"] = now_ist().date()
            if events:
                brief = format_news_daily_brief(events)
                safe_send(cq.message.chat.id, brief)
                print(f"[NEWS] Daily brief sent — {len(events)} events today")
            else:
                safe_send(
                    cq.message.chat.id,
                    "📰 <b>News Filter Active</b>\n\n"
                    "✅ No high or medium impact forex events affecting your "
                    "selected pairs found for today. Market conditions should be normal.\n\n"
                    "<i>Use /news anytime to refresh news data.</i>"
                )
                print("[NEWS] No impactful news today for selected pairs")
        except Exception as e:
            print(f"[NEWS] Error sending daily brief: {e}")
            safe_send(
                cq.message.chat.id,
                "📰 <b>News Filter</b>\n\n"
                "⚠️ Could not fetch news data automatically.\n"
                "Use /news command to retry fetching news.\n"
                f"Error: {str(e)[:100]}"
            )

    threading.Thread(target=_send_news_brief, daemon=True).start()

# ============================== ACCEPT / REJECT SIGNAL CALLBACKS ==============================

@bot.callback_query_handler(func=lambda cq: cq.data.startswith("sig_accept:"))
def cb_signal_accept(cq):
    """User accepted the signal — save it and remove buttons."""
    if not is_authorized(cq.message.chat.id):
        bot.answer_callback_query(cq.id, "❌ Unauthorized")
        return

    try:
        _, signal_no_str = cq.data.split(":", 1)
        signal_no = int(signal_no_str)
    except (ValueError, IndexError):
        bot.answer_callback_query(cq.id, "❌ Invalid signal data")
        return

    with state_lock:
        pending = STATE.get("pending_signals", {})
        sig_data = pending.get(signal_no)

    if not sig_data:
        bot.answer_callback_query(cq.id, "⚠️ Signal expired or already processed")
        return

    # Mark as no longer pending (auto-reject timer should not fire)
    with state_lock:
        STATE.get("pending_signals", {}).pop(signal_no, None)

    # Save to per-user memory
    user_record_signal(
        chat_id=cq.message.chat.id,
        signal_no=signal_no,
        pair=sig_data.get("pair", ""),
        direction=sig_data.get("direction", ""),
        entry_ist=sig_data.get("entry_ist", ""),
        expiry_ist=sig_data.get("expiry_ist", ""),
        expiry_minutes=sig_data.get("expiry_min", 1),
        score=sig_data.get("score", 0),
    )
    # Also record to global memory for backward-compat
    record_signal(
        signal_no=signal_no,
        pair=sig_data.get("pair", ""),
        direction=sig_data.get("direction", ""),
        entry_ist=sig_data.get("entry_ist", ""),
        expiry_ist=sig_data.get("expiry_ist", ""),
        expiry_minutes=sig_data.get("expiry_min", 1),
        score=sig_data.get("score", 0),
    )
    # Bump next signal counter (per-user)
    next_no = signal_no + 1
    user_save_signal_no(cq.message.chat.id, next_no)
    save_signal_no_for_today(next_no)
    with state_lock:
        STATE["signal_no"] = next_no

    # Edit message to show accepted state
    original_text = ""
    try:
        original_text = cq.message.text or ""
    except Exception:
        pass
    new_text = original_text + "\n\n✅ <b>Signal Accepted — Trade is Live!</b>"
    safe_edit_text(cq.message.chat.id, cq.message.message_id, new_text)
    bot.answer_callback_query(cq.id, f"✅ Signal #{signal_no:02d} Accepted!")
    print(f"[SIGNAL] #{signal_no} accepted by user")

@bot.callback_query_handler(func=lambda cq: cq.data.startswith("sig_reject:"))
def cb_signal_reject(cq):
    """User rejected the signal — delete the message and DO NOT count it."""
    if not is_authorized(cq.message.chat.id):
        bot.answer_callback_query(cq.id, "❌ Unauthorized")
        return

    try:
        _, signal_no_str = cq.data.split(":", 1)
        signal_no = int(signal_no_str)
    except (ValueError, IndexError):
        bot.answer_callback_query(cq.id, "❌ Invalid data")
        return

    with state_lock:
        STATE.get("pending_signals", {}).pop(signal_no, None)
        # Clear active signal so scheduler can proceed
        active = STATE.get("active_signal")
        if active and active.get("no") == signal_no:
            STATE["active_signal"] = None
        # Do NOT bump signal_no — rejected signal is not counted,
        # so the next signal reuses this same number

    # Clear dedup so the same signal number can be sent again next scan
    _clear_signal_dedup(signal_no)

    safe_delete(cq.message.chat.id, cq.message.message_id)
    bot.answer_callback_query(cq.id, f"❌ Signal #{signal_no:02d} Rejected")
    print(f"[SIGNAL] #{signal_no} rejected by user — reusing #{signal_no} for next signal")

# ============================== PROFIT / LOSS RESULT CALLBACKS ==============================

@bot.callback_query_handler(func=lambda cq: cq.data.startswith("result_profit:") or cq.data.startswith("result_loss:") or cq.data.startswith("result_refund:"))
def cb_signal_result(cq):
    """Handle Profit/Loss/Refund result from user."""
    if not is_authorized(cq.message.chat.id):
        bot.answer_callback_query(cq.id, "❌ Unauthorized")
        return

    try:
        action, signal_no_str = cq.data.split(":", 1)
        signal_no = int(signal_no_str)
        if action == "result_profit":
            result = "profit"
        elif action == "result_loss":
            result = "loss"
        else:
            result = "refund"
    except (ValueError, IndexError):
        bot.answer_callback_query(cq.id, "❌ Invalid data")
        return

    success = update_signal_result(signal_no, result)
    # Also update per-user memory
    user_update_signal_result(cq.message.chat.id, signal_no, result)
    if result == "profit":
        emoji = "🟢"
        result_label = "PROFIT"
    elif result == "loss":
        emoji = "🔴"
        result_label = "LOSS"
    else:
        emoji = "⚪"
        result_label = "REFUND"

    if success:
        # ── Update auto-learning stats ──────────────────────────────────────
        with state_lock:
            capital = STATE.get("capital")
            today = get_today_key()
        with memory_lock:
            mem = load_memory()
            sig_data = mem.get("days", {}).get(today, {}).get("signals", {}).get(str(signal_no), {})
        signal_pair = sig_data.get("pair", "")
        entry_ist = sig_data.get("entry_ist", "")

        # Auto-learning feature update
        if signal_pair and result in ["profit", "loss"]:
            update_auto_learning(signal_pair, result, entry_ist)

        # Update smart cooldown result for this pair
        cooldown_result = "win" if result == "profit" else ("loss" if result == "loss" else "pending")
        update_pair_cooldown_result(signal_pair, cooldown_result)

        safe_edit_text(
            cq.message.chat.id,
            cq.message.message_id,
            f"📊 <b>Signal #{signal_no:02d} — Result Recorded</b>\n\n"
            f"{emoji} Marked as <b>{result_label}</b>\n\n"
            f"<i>Saved to memory. Use /report to view your stats.</i>"
        )
        bot.answer_callback_query(cq.id, f"{emoji} Signal #{signal_no:02d} marked as {result_label}!")
        print(f"[RESULT] Signal #{signal_no} marked as {result}")

        # Signal result received — allow scheduler to proceed
        with state_lock:
            STATE["waiting_for_result"] = False
            # Block scanner for one full candle so fresh data is available
            STATE["_next_scan_after"] = time.time() + max(10, (get_next_candle_open() - now_utc()).total_seconds() - 5)

    else:
        bot.answer_callback_query(cq.id, "⚠️ Could not save result. Try /report to check.")

# Track which unauthorized users we've already notified the owner about (session only)
_notified_unauthorized: set = set()
_notified_lock = threading.Lock()


# ============================== CATCH-ALL HANDLER ==============================
@bot.message_handler(func=lambda msg: True)
def handle_all_messages(msg):
    if not is_authorized(msg.chat.id):
        send_payment_info(msg.chat.id)
        # Notify owner — but only once per user per session to avoid spam
        with _notified_lock:
            already_notified = msg.chat.id in _notified_unauthorized
            if not already_notified:
                _notified_unauthorized.add(msg.chat.id)
        if not already_notified:
            username  = msg.from_user.username or "No username"
            first_name = msg.from_user.first_name or "Unknown"
            text_preview = (msg.text or "")[:60]
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("✅ Approve", callback_data=f"useraccess:grant:{msg.chat.id}"),
                types.InlineKeyboardButton("❌ Reject",  callback_data=f"useraccess:deny:{msg.chat.id}")
            )
            try:
                safe_send(
                    AUTHORIZED_CHAT_ID,
                    f"🔔 <b>New Access Request</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"👤 <b>Name:</b> {first_name}\n"
                    f"📱 <b>Username:</b> @{username}\n"
                    f"🆔 <b>Chat ID:</b> <code>{msg.chat.id}</code>\n"
                    f"💬 <b>Message:</b> {text_preview}\n\n"
                    f"Tap a button to approve or reject this user:",
                    reply_markup=markup
                )
            except Exception as e:
                print(f"[ACCESS] Could not notify owner: {e}")
        return

    with state_lock:
        awaiting_done = STATE.get("awaiting_done_capital", False)
        awaiting_start = STATE.get("awaiting_capital", False)
        prev_capital_state = STATE.get("capital")

    # For done handler: prev_capital is today's starting cap if set, else yesterday's starting cap
    if awaiting_done:
        from datetime import timedelta
        _dk_today = get_today_key()
        _dk_yest  = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
        _today_start = user_get_capital_for_date(msg.chat.id, _dk_today)["starting"]
        _yest_start  = user_get_capital_for_date(msg.chat.id, _dk_yest)["starting"]
        # If today has no capital yet, this is a yesterday-close triggered by /start
        if _today_start is None and _yest_start is not None:
            prev_capital = _yest_start
        else:
            prev_capital = _today_start or prev_capital_state
    else:
        prev_capital = prev_capital_state

    # ── Handle /done capital reply ─────────────────────────────────────────
    if awaiting_done:
        new_cap = parse_capital_amount(msg.text or "")
        if new_cap is None or new_cap <= 0:
            safe_send(msg.chat.id,
                "⚠️ Couldn't read that amount. Please reply with your capital, e.g. <code>$50</code>"
            )
            return
        with state_lock:
            STATE["awaiting_done_capital"] = False

        # Detect if this was triggered by /start for yesterday's missing close
        from datetime import timedelta
        today_key_now     = get_today_key()
        yesterday_key_now = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
        today_cap_data    = user_get_capital_for_date(msg.chat.id, today_key_now)
        is_yesterday_close = (
            today_cap_data["starting"] is None
            and prev_capital is not None
            and user_get_capital_for_date(msg.chat.id, yesterday_key_now)["starting"] is not None
        )

        if is_yesterday_close:
            # Save as yesterday's ending capital
            _yest_ud = load_user_memory(msg.chat.id)
            _yest_day = _yest_ud.setdefault("days", {}).setdefault(yesterday_key_now, {})
            _yest_day["ending_capital"] = new_cap
            save_user_memory(msg.chat.id, _yest_ud)

            # Show brief yesterday summary
            _yest_start = prev_capital
            _diff = new_cap - _yest_start
            _pnl = f"🟢 +${_diff:.2f}" if _diff >= 0 else f"🔴 -${abs(_diff):.2f}"
            safe_send(msg.chat.id,
                f"✅ <b>Yesterday closed: {_pnl}</b>\n"
                f"Started ${_yest_start:.2f} → Ended ${new_cap:.2f}\n\n"
                f"💰 Now, what is your <b>starting capital for today</b>?\n"
                f"Reply with the amount, e.g. <code>$100</code>"
            )
            with state_lock:
                STATE["awaiting_capital"] = True
            return

        # Normal /done flow — save today's ending capital
        if prev_capital is not None and prev_capital > 0:
            diff = new_cap - prev_capital
            pct = (diff / prev_capital) * 100
            if diff >= 0:
                pnl_line = f"🟢 In Profit: <b>+${diff:,.2f}</b> (+{pct:.1f}%)"
            else:
                pnl_line = f"🔴 In Loss: <b>-${abs(diff):,.2f}</b> ({pct:.1f}%)"

            _sigs = user_get_signals_for_date(msg.chat.id, today_key_now)
            _deposits = user_get_deposits(msg.chat.id, today_key_now)
            _wds = user_get_withdrawals(msg.chat.id, today_key_now)
            _total_dep = sum(d["amount"] for d in _deposits)
            _total_wd = sum(w["amount"] for w in _wds)
            _profit_count = sum(1 for s in _sigs.values() if s.get("result") == "profit")
            _loss_count = sum(1 for s in _sigs.values() if s.get("result") == "loss")
            _refund_count = sum(1 for s in _sigs.values() if s.get("result") == "refund")
            _total_count = len(_sigs)
            _acc = round((_profit_count / (_profit_count + _loss_count)) * 100) if (_profit_count + _loss_count) > 0 else 0

            dep_line = f"\n💰 Deposits:          +${_total_dep:.2f}" if _total_dep > 0 else ""
            wd_line = f"\n💸 Withdrawals:       -${_total_wd:.2f}" if _total_wd > 0 else ""

            summary = (
                f"✅ <b>Session Done!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📉 Starting Capital: <b>${prev_capital:,.2f}</b>\n"
                f"📈 Ending Capital:   <b>${new_cap:,.2f}</b>\n"
                f"{pnl_line}"
                f"{dep_line}"
                f"{wd_line}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚫ Total Signals:    {_total_count}\n"
                f"🟢 Profit Signals:  {_profit_count}\n"
                f"🔴 Loss Signals:    {_loss_count}\n"
                + (f"⚪ Refunds:         {_refund_count}\n" if _refund_count > 0 else "")
                + f"🎯 Accuracy:        {_acc}%\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Use /report for full report.</i>"
            )
        else:
            summary = (
                f"✅ <b>Capital Set: ${new_cap:,.2f}</b>\n\n"
                f"Use /pairs to start trading and I'll track your PnL from here."
            )

        user_save_capital(msg.chat.id, new_cap)
        user_update_ending_capital(msg.chat.id, new_cap)
        with state_lock:
            STATE["capital"] = new_cap
        safe_send(msg.chat.id, summary)
        return

    # ── Handle /pairs capital reply (new day / first start) ──────────────
    if awaiting_start:
        new_cap = parse_capital_amount(msg.text or "")
        if new_cap is None or new_cap <= 0:
            safe_send(msg.chat.id,
                "⚠️ Couldn't read that. Please reply with your starting capital, e.g. <code>$100</code>"
            )
            return

        # Accept any positive capital amount — no MM validation
        user_set_starting_capital(msg.chat.id, new_cap)
        with state_lock:
            STATE["capital"] = new_cap
            STATE["awaiting_capital"] = False
        kb = types.InlineKeyboardMarkup(row_width=2)
        for p in ALL_PAIRS:
            kb.add(types.InlineKeyboardButton(p, callback_data=f"pair:{p}"))
        kb.add(types.InlineKeyboardButton("✅ START BOT", callback_data="pairs:start"))
        safe_send(
            msg.chat.id,
            f"✅ <b>Capital Set: ${new_cap:,.2f}</b>\n\n"
            f"📊 <b>Now select your trading pairs:</b>\n"
            f"Tap pairs to select/deselect, then press START BOT",
            reply_markup=kb
        )
        return

# ============================== HELPER FUNCTIONS ==============================
def run_api_check():
    API_KEYS = TWELVE_KEYS
    all_messages = ["📊 <b>API Keys Status</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
    for idx, key in enumerate(API_KEYS, 1):
        try:
            r = requests.get(f"https://api.twelvedata.com/api_usage?apikey={key}", timeout=5)
            data = r.json()
            if "plan_daily_limit" in data:
                used = data.get("daily_usage", "N/A")
                limit = data.get("plan_daily_limit", "N/A")
                percentage = int((used/limit)*100) if isinstance(used, int) and isinstance(limit, int) else 0
                status_emoji = "🟢" if percentage < 70 else "🟡" if percentage < 90 else "🔴"
                all_messages.append(f"{status_emoji} <b>Key {idx}:</b> {used}/{limit} ({percentage}%)")
            elif "status" in data and data["status"] == "error":
                all_messages.append(f"❌ <b>Key {idx}:</b> {data.get('message', 'Error')}")
            else:
                all_messages.append(f"⚠️ <b>Key {idx}:</b> Unexpected response")
        except Exception:
            all_messages.append(f"💥 <b>Key {idx}:</b> Connection failed")
    return "\n".join(all_messages)

# ============================== SIGNAL BUILDER ==============================
def build_signal(pairs: List[str]) -> Optional[Dict]:
    # ── Filter out pairs blocked due to 2+ consecutive losses ──────────────
    with state_lock:
        pair_losses = dict(STATE.get("pair_consecutive_losses", {}))
    blocked_pairs = [p for p, v in pair_losses.items() if v >= 2]
    active_pairs = [p for p in pairs if p not in blocked_pairs]

    if blocked_pairs:
        print(f"[PAIR BLOCK] Skipping blocked pairs: {', '.join(blocked_pairs)}")

    if not active_pairs:
        # All selected pairs are blocked — allow all to avoid full lockout
        print(f"[PAIR BLOCK] ⚠️ ALL pairs blocked — allowing all pairs this scan to avoid lockout")
        active_pairs = pairs

    # ── Clear candle dedup so a fresh scan always reads latest candles ──────
    with _candle_confirm_lock:
        _last_confirmed_candle.clear()

    candidates = []
    all_data_failed = True
    now_str = now_ist().strftime("%H:%M:%S")
    print(f"\n{'='*55}")
    print(f"🔍 [{now_str} IST] SCANNING {len(active_pairs)} PAIRS | Min Score: {get_current_min_score()}")
    if blocked_pairs:
        print(f"🚫 Blocked pairs (2+ losses): {', '.join(blocked_pairs)}")
    print(f"{'='*55}")
    for pair in active_pairs:
        try:
            print(f"\n📊 [{pair}] ─────────────────────────────")
            print(f"  ├─ Fetching 1m candles...")
            df1 = get_cached_or_fetch(pair, "1min", 200)
            print(f"  ├─ Fetching 5m candles...")
            df5 = get_cached_or_fetch(pair, "5min", 200)
            print(f"  ├─ Fetching 15m candles...")
            df15 = get_cached_or_fetch(pair, "15min", 200)
            if df1 is None or len(df1) < 50:
                print(f"  └─ ⚠️ Insufficient 1m data — skipping")
                continue
            all_data_failed = False
            if df5 is None or len(df5) < 50:
                print(f"  ├─ ⚠️ No 5m data — using 1m fallback")
                df5 = df1.copy()
            if df15 is None or len(df15) < 50:
                print(f"  ├─ ⚠️ No 15m data — using 5m fallback")
                df15 = df5.copy()

            # ── Candle close confirmation ──────────────────────────────
            if not is_candle_closed(df1, pair):
                print(f"  ├─ ⏳ [{pair}] Same candle as last scan — waiting for close")
                continue

            # ── Pair-specific volatility check ─────────────────────────
            pair_vol = check_pair_volatility(df1, pair)
            if not pair_vol["suitable"]:
                print(f"  ├─ 🌊 [{pair}] {pair_vol['label']} — skipping")
                # Don't hard-skip on volatility, just penalise in score (handled inside scoring)

            print(f"  ├─ ✅ Data OK ({len(df1)} candles) — running analysis...")
            analysis = calculate_enhanced_signal_score(df1, df5, df15)
            if not analysis["direction"]:
                print(f"  └─ ❌ [{pair}] No signal (Score: {analysis['score']:.1f}/10)")
                continue
            if analysis["score"] < get_current_min_score():
                print(f"  └─ ❌ [{pair}] Score too low: {analysis['score']:.1f}/10 (Need {get_current_min_score()})")
                continue
            if len(analysis["confirmations"]) < MIN_CONFIRMATIONS:
                print(f"  └─ ❌ [{pair}] Too few confirmations: {len(analysis['confirmations'])}/{MIN_CONFIRMATIONS}")
                continue
            print(f"  └─ 🎯 [{pair}] QUALIFIED! {analysis['direction']} | Score: {analysis['score']:.1f}/10 | {len(analysis['confirmations'])} confirmations")
            candidates.append({
                "pair": pair,
                "score": analysis["score"],
                "direction": analysis["direction"],
                "confirmations": analysis["confirmations"],
                "expiry_min": analysis["expiry_minutes"],
                "entry_ist": "—",
                "expiry_ist": "—",
            })
        except Exception as e:
            print(f"  └─ 💥 [{pair}] Error: {e}")
            traceback.print_exc()
            continue
    print(f"\n{'─'*55}")
    if all_data_failed:
        print("⚠️  Could not fetch data for ANY pairs — markets may be closed")
        print(f"{'─'*55}")
        return None
    if not candidates:
        print(f"❌  No pairs passed the signal criteria this scan")
        print(f"{'─'*55}")
        return None
    best = max(candidates, key=lambda x: x["score"])
    print(f"🏆  BEST SIGNAL → {best['pair']} {best['direction']} | Score: {best['score']:.1f}/10")
    print(f"{'='*55}")
    return best


def get_current_min_score() -> float:
    """Return the minimum signal score required."""
    return float(MIN_SCORE)

# ============================== NEWS FILTER ENGINE ==============================
CURRENCY_TO_PAIRS = {
    "USD": ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD"],
    "EUR": ["EUR/USD", "EUR/GBP", "EUR/JPY"],
    "GBP": ["GBP/USD", "EUR/GBP", "GBP/JPY"],
    "JPY": ["USD/JPY", "CAD/JPY", "GBP/JPY", "EUR/JPY"],
    "AUD": ["AUD/USD"],
    "CAD": ["CAD/JPY"],
    "CHF": [],
    "NZD": [],
}

HIGH_IMPACT = {"High"}
MEDIUM_IMPACT = {"Medium"}

_news_fetch_lock = threading.Lock()
_news_raw_cache: Dict = {"data": None, "fetched_at": 0.0, "source_used": ""}
NEWS_CACHE_TTL = 3600


def _fetch_ff_json_direct() -> Optional[list]:
    """Try fetching FF calendar JSON directly (works on unrestricted servers)."""
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ForexBot/1.0)",
        "Accept": "application/json",
    }
    r = requests.get(url, headers=headers, timeout=12)
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list) and len(data) > 0:
            return data
    return None


def _fetch_ff_via_jsonp_proxy() -> Optional[list]:
    """
    Use jsonp.afeld.me — a simple open CORS/proxy that PythonAnywhere allows.
    Returns the FF JSON wrapped in a JSONP callback; we strip the callback.
    """
    import re as _re
    url = "https://jsonp.afeld.me/?url=https%3A%2F%2Fnfs.faireconomy.media%2Fff_calendar_thisweek.json"
    r = requests.get(url, timeout=15)
    if r.status_code == 200:
        text = r.text.strip()
        # Strip JSONP wrapper if present: callback([...])
        m = re.match(r'^[^(]*\((.*)\)\s*;?\s*$', text, re.DOTALL)
        raw = m.group(1) if m else text
        data = json.loads(raw)
        if isinstance(data, list) and len(data) > 0:
            return data
    return None


def _fetch_ff_via_thingproxy() -> Optional[list]:
    """
    Use thingproxy.freeboard.io — a reliable server-side HTTP proxy.
    Works from PythonAnywhere without CORS restrictions.
    """
    url = "https://thingproxy.freeboard.io/fetch/https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ForexBot/1.0)"}
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list) and len(data) > 0:
            return data
    return None


def _fetch_ff_via_allorigins_v2() -> Optional[list]:
    """
    Use allorigins.win v2 endpoint with GET (not raw) — more reliable than the raw path.
    Returns JSON wrapped in {"contents": "..."}.
    """
    import urllib.parse
    target = urllib.parse.quote("https://nfs.faireconomy.media/ff_calendar_thisweek.json", safe="")
    url = f"https://api.allorigins.win/get?url={target}"
    r = requests.get(url, timeout=20)
    if r.status_code == 200:
        wrapper = r.json()
        contents = wrapper.get("contents", "")
        if contents:
            data = json.loads(contents)
            if isinstance(data, list) and len(data) > 0:
                return data
    return None


def _fetch_ff_via_corsproxy_dev() -> Optional[list]:
    """
    Use cors-proxy.htmldriven.com — PythonAnywhere-compatible server-side proxy.
    """
    url = "https://cors-proxy.htmldriven.com/?url=https%3A%2F%2Fnfs.faireconomy.media%2Fff_calendar_thisweek.json"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ForexBot/1.0)"}
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code == 200:
        # Some proxies return {"body": "..."}, others return raw JSON
        try:
            wrapper = r.json()
            if isinstance(wrapper, list):
                return wrapper
            body = wrapper.get("body") or wrapper.get("contents") or wrapper.get("data")
            if body:
                data = json.loads(body) if isinstance(body, str) else body
                if isinstance(data, list) and len(data) > 0:
                    return data
        except Exception:
            pass
    return None


# ── newsdata.io integration ────────────────────────────────────────────────────
# newsdata.io provides forex news headlines. We fetch them, detect which
# currencies are mentioned, and convert them into the same event-dict format
# the rest of the bot uses (impact="High", time=pub_date).

_NEWSDATA_CURRENCY_KEYWORDS = {
    "USD": ["dollar", "usd", "fed", "federal reserve", "fomc", "us economy",
            "us gdp", "us inflation", "us jobs", "nonfarm", "nfp", "us cpi", "us ppi",
            "us retail", "powell", "treasury"],
    "EUR": ["euro", "eur", "ecb", "european central bank", "eurozone",
            "eu economy", "eu inflation", "eu gdp", "lagarde", "germany", "france"],
    "GBP": ["pound", "gbp", "sterling", "boe", "bank of england",
            "uk economy", "uk gdp", "uk inflation", "uk cpi", "bailey"],
    "JPY": ["yen", "jpy", "boj", "bank of japan", "japan economy",
            "japan gdp", "japan inflation", "ueda", "kuroda"],
    "AUD": ["aud", "australian dollar", "rba", "reserve bank of australia",
            "australia gdp", "australia inflation"],
    "CAD": ["cad", "canadian dollar", "boc", "bank of canada",
            "canada gdp", "canada inflation", "oil price", "crude oil"],
}

_NEWSDATA_FOREX_QUERIES = [
    "forex trading central bank interest rate",
    "USD EUR GBP JPY economic news",
    "forex market inflation GDP jobs",
]

_newsdata_cache: Dict = {"data": None, "fetched_at": 0.0}
_NEWSDATA_CACHE_TTL = 3600  # 1 hour


def _detect_currencies_in_text(text: str) -> List[str]:
    """Return list of currency codes mentioned in the given text."""
    lower = text.lower()
    found = []
    for currency, keywords in _NEWSDATA_CURRENCY_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            found.append(currency)
    return found


def fetch_newsdata_forex_news() -> Optional[list]:
    """
    Fetch forex-relevant news from newsdata.io.
    Returns a list of FF-compatible event dicts, or None on failure.
    Caches results for 1 hour.
    """
    now_ts = time.time()
    if _newsdata_cache["data"] is not None and now_ts - _newsdata_cache["fetched_at"] < _NEWSDATA_CACHE_TTL:
        return _newsdata_cache["data"]

    all_articles = []
    seen_ids = set()

    for query in _NEWSDATA_FOREX_QUERIES:
        try:
            params = {
                "apikey": NEWSDATA_API_KEY,
                "q": query,
                "language": "en",
                "category": "business",
                "size": 10,
            }
            r = requests.get(
                "https://newsdata.io/api/1/news",
                params=params,
                timeout=15
            )
            if r.status_code != 200:
                print(f"[NEWSDATA] HTTP {r.status_code} for query: {query[:40]}")
                continue
            data = r.json()
            if data.get("status") != "success":
                print(f"[NEWSDATA] API error: {data.get('message', 'unknown')}")
                continue
            for article in data.get("results", []):
                art_id = article.get("article_id") or article.get("link", "")
                if art_id in seen_ids:
                    continue
                seen_ids.add(art_id)
                all_articles.append(article)
        except Exception as e:
            print(f"[NEWSDATA] Query error ({query[:30]}): {e}")
            continue

    if not all_articles:
        print("[NEWSDATA] No articles returned")
        return None

    # Convert articles to FF-compatible event dicts
    events = []
    for art in all_articles:
        try:
            title = art.get("title", "") or ""
            description = art.get("description", "") or ""
            combined_text = f"{title} {description}"

            currencies = _detect_currencies_in_text(combined_text)
            if not currencies:
                continue

            # Parse publication date
            pub_date_str = art.get("pubDate", "")
            if pub_date_str:
                try:
                    pub_dt = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
                    pub_dt = pub_dt.astimezone(timezone.utc)
                except Exception:
                    pub_dt = now_utc()
            else:
                pub_dt = now_utc()

            art_id = art.get("article_id") or art.get("link", f"{title}_{pub_date_str}")

            for currency in currencies:
                events.append({
                    "id": f"nd_{art_id}_{currency}",
                    "impact": "High",
                    "currency": currency,
                    "title": title[:80] if title else "Forex News",
                    "date": pub_dt.isoformat(),
                    # Keep extra metadata for display
                    "_source": "newsdata.io",
                    "_link": art.get("link", ""),
                })
        except Exception as e:
            print(f"[NEWSDATA] Parse error: {e}")
            continue

    if not events:
        print("[NEWSDATA] No forex-relevant articles after filtering")
        return None

    _newsdata_cache["data"] = events
    _newsdata_cache["fetched_at"] = now_ts
    print(f"[NEWSDATA] ✓ Fetched {len(all_articles)} articles → {len(events)} forex events")
    return events


# Ordered list of fetcher functions — fastest/most reliable first
_NEWS_FETCHERS = [
    ("direct",          _fetch_ff_json_direct),
    ("allorigins-v2",   _fetch_ff_via_allorigins_v2),
    ("thingproxy",      _fetch_ff_via_thingproxy),
    ("jsonp-proxy",     _fetch_ff_via_jsonp_proxy),
    ("cors-htmldriven", _fetch_ff_via_corsproxy_dev),
]


def fetch_ff_news_raw() -> Optional[list]:
    """
    Fetch this week's Forex Factory news JSON.
    Tries multiple server-side-compatible sources in order.
    Falls back to stale cache if all fail.
    Returns parsed list or None on complete failure.
    """
    with _news_fetch_lock:
        now_ts = time.time()
        if (
            _news_raw_cache["data"] is not None
            and now_ts - _news_raw_cache["fetched_at"] < NEWS_CACHE_TTL
        ):
            return _news_raw_cache["data"]

        last_error = None
        for name, fetcher in _NEWS_FETCHERS:
            try:
                data = fetcher()
                if data:
                    _news_raw_cache["data"] = data
                    _news_raw_cache["fetched_at"] = now_ts
                    _news_raw_cache["source_used"] = name
                    print(f"[NEWS] ✓ Fetched {len(data)} events via [{name}]")
                    return data
                else:
                    print(f"[NEWS] [{name}] returned empty/invalid data")
            except requests.exceptions.ConnectionError as ce:
                print(f"[NEWS] [{name}] Connection blocked: {str(ce)[:80]}")
                last_error = str(ce)
            except requests.exceptions.Timeout:
                print(f"[NEWS] [{name}] Timeout")
                last_error = "timeout"
            except Exception as e:
                print(f"[NEWS] [{name}] Error: {str(e)[:80]}")
                last_error = str(e)

        print(f"[NEWS] ⚠️ All news sources failed. Last error: {last_error}")
        if _news_raw_cache["data"] is not None:
            print(f"[NEWS] Using stale cache (age: {(now_ts - _news_raw_cache['fetched_at'])/3600:.1f}h)")
            return _news_raw_cache["data"]
        print(f"[NEWS] No raw news data available — skipping filter")
        return None


def parse_news_event_time(date_str: str) -> Optional[datetime]:
    """Parse FF date string like '2025-04-04T09:30:00-0400' to UTC datetime."""
    try:
        normalised = date_str.strip().replace("Z", "+00:00")
        normalised = re.sub(r'([+-])(\d{2})(\d{2})$', r'\1\2:\3', normalised)
        dt_obj = datetime.fromisoformat(normalised)
        return dt_obj.astimezone(timezone.utc)
    except Exception:
        return None


def get_affected_pairs(currency: str, selected_pairs: list) -> List[str]:
    all_affected = CURRENCY_TO_PAIRS.get(currency.upper(), [])
    return [p for p in all_affected if p in selected_pairs]


def _parse_raw_event_list(raw: list, selected_pairs: list, today_ist) -> List[Dict]:
    """Parse a list of FF-format event dicts and return normalised event list for today."""
    results = []
    for event in raw:
        try:
            impact = event.get("impact", "")
            if impact not in HIGH_IMPACT and impact not in MEDIUM_IMPACT:
                continue

            date_str = event.get("date", "")
            if not date_str:
                continue
            event_time_utc = parse_news_event_time(date_str)
            if not event_time_utc:
                continue

            event_date_ist = event_time_utc.astimezone(IST).date()
            if event_date_ist != today_ist:
                continue

            currency = event.get("currency", "") or event.get("country", "")
            if not currency:
                continue
            affected = get_affected_pairs(currency, selected_pairs)
            if not affected:
                continue

            event_time_ist = event_time_utc.astimezone(IST).strftime("%I:%M %p IST")
            results.append({
                "id": event.get("id", f"{event.get('title','')}_{date_str}"),
                "title": event.get("title", "Unknown Event"),
                "currency": currency,
                "impact": impact,
                "event_time_utc": event_time_utc,
                "event_time_ist": event_time_ist,
                "affected_pairs": affected,
                "forecast": event.get("forecast", ""),
                "previous": event.get("previous", ""),
                "_source": event.get("_source", "forexfactory"),
            })
        except Exception as e:
            print(f"[NEWS] Error parsing event: {e}")
            continue
    return results


def fetch_todays_high_impact_news(selected_pairs: list) -> List[Dict]:
    """
    Fetch today's high/medium impact UPCOMING scheduled events from Forex Factory.
    Only returns events that haven't happened yet (future events in IST).
    newsdata.io headlines are excluded — they report past events.
    """
    today_ist = now_ist().date()
    results = []
    seen_ids = set()

    # ── Source: Forex Factory calendar (scheduled economic events only) ─────
    raw_ff = fetch_ff_news_raw()
    if raw_ff:
        ff_events = _parse_raw_event_list(raw_ff, selected_pairs, today_ist)
        now_utc_ts = now_utc()
        for ev in ff_events:
            if ev["id"] not in seen_ids:
                # Only include events that are still in the future
                if ev["event_time_utc"] > now_utc_ts:
                    seen_ids.add(ev["id"])
                    results.append(ev)
        print(f"[NEWS] FF calendar: {len(ff_events)} events today, {len(results)} still upcoming")
    else:
        print("[NEWS] FF calendar unavailable — no news data")

    if not results:
        print("[NEWS] No upcoming events found")
        return []

    results.sort(key=lambda x: x["event_time_utc"])
    print(f"[NEWS] Total: {len(results)} upcoming events for today ({today_ist})")
    return results


def format_news_daily_brief(events: List[Dict]) -> str:
    if not events:
        return ""
    today_str = now_ist().strftime("%A, %d %B %Y")
    # All events are now upcoming FF calendar events only
    high_events = [e for e in events if e["impact"] == "High"]
    med_events  = [e for e in events if e["impact"] == "Medium"]
    lines = [
        f"📅 <b>UPCOMING FOREX EVENTS — {today_str}</b>",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"⏳ All times shown in <b>IST</b>",
        f"",
    ]
    if high_events:
        lines.append(f"🔴 <b>HIGH IMPACT ({len(high_events)} events)</b>")
        for e in high_events:
            pairs_str = ", ".join(e["affected_pairs"])
            forecast = f" | Forecast: {e['forecast']}" if e.get("forecast") else ""
            previous = f" | Prev: {e['previous']}" if e.get("previous") else ""
            lines.append(
                f"  🕐 <b>{e['event_time_ist']}</b>\n"
                f"  📌 {e['title']} ({e['currency']}){forecast}{previous}\n"
                f"  💹 Affects: <b>{pairs_str}</b>"
            )
            lines.append("")
    if med_events:
        lines.append(f"🟡 <b>MEDIUM IMPACT ({len(med_events)} events)</b>")
        for e in med_events:
            pairs_str = ", ".join(e["affected_pairs"])
            lines.append(
                f"  🕐 <b>{e['event_time_ist']}</b>\n"
                f"  📌 {e['title']} ({e['currency']})\n"
                f"  💹 Affects: <b>{pairs_str}</b>"
            )
            lines.append("")
    if not high_events and not med_events:
        lines.append("✅ No upcoming high or medium impact events found today.")
        lines.append("")
    lines += [
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"⚠️ <i>Bot will alert you {NEWS_WARN_MINUTES_BEFORE} minutes before each event.\n"
        f"Avoid trading affected pairs around news times.</i>",
        f"",
        f"💡 Use /news to refresh news data anytime.",
    ]
    return "\n".join(lines)


def format_news_warning(event: Dict) -> str:
    pairs_str = ", ".join(event["affected_pairs"])
    impact_icon = "🔴" if event["impact"] == "High" else "🟡"
    return (
        f"⚠️ <b>NEWS ALERT — {NEWS_WARN_MINUTES_BEFORE} MIN WARNING</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{impact_icon} <b>Event:</b> {event['title']}\n"
        f"🏦 <b>Currency:</b> {event['currency']} ({event['impact']} Impact)\n"
        f"🕐 <b>Release Time:</b> {event['event_time_ist']}\n"
        f"💹 <b>Pairs Affected:</b> <b>{pairs_str}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🚨 <i>These pairs may be extremely volatile around the release time.\n"
        f"Avoid opening new positions 15 minutes before and after the event.</i>"
    )


def format_news_signal_message(signal: Dict, event: Dict, advance_seconds: int = 0) -> str:
    confirmations_text = "\n".join(
        [f"  {i}. {c}" for i, c in enumerate(signal["confirmations"], 1)]
    )
    impact_icon = "🔴" if event["impact"] == "High" else "🟡"
    if advance_seconds >= 40:
        advance_line = f"⚡ <b>Act Now!</b> Entry in <b>{advance_seconds}s</b> — open chart immediately!\n"
    elif advance_seconds > 0:
        advance_line = f"⏱️ Entry in ~{advance_seconds}s\n"
    else:
        advance_line = ""
    return (
        f"{impact_icon} <b>NEWS SIGNAL</b>\n"
        f"📰 <b>{event['title']}</b> ({event['currency']})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{advance_line}"
        f"💹 <b>Pair:</b> {signal['pair']}\n"
        f"🕐 <b>Entry:</b> {signal['entry_ist']} IST\n"
        f"🕐 <b>Expiry:</b> {signal['expiry_ist']} IST ({signal['expiry_min']}min)\n"
        f"\n"
        f"🎯 <b>Direction:</b> {'🟢 CALL ▲' if signal['direction'] == 'CALL' else '🔴 PUT ▼'}\n"
        f"📊 <b>Quality Score:</b> {signal['score']:.1f}/10.0 ⭐\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <b>CONFIRMATIONS ({len(signal['confirmations'])}):</b>\n"
        f"{confirmations_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 <i>News-driven setup — enter only if spread is normal</i>\n"
    )


def news_monitor_loop():
    print("[NEWS] Monitor started")
    warn_window_seconds = NEWS_WARN_MINUTES_BEFORE * 60
    warn_window_lower = warn_window_seconds - 90   # 30 min ± 90s
    warn_window_upper = warn_window_seconds + 90

    while True:
        try:
            with state_lock:
                STATE["_news_monitor_last_heartbeat"] = time.time()
                running  = STATE["bot_running"]
                pairs    = STATE["selected_pairs"][:]
                chat_id  = STATE["telegram_chat_id"]
                events   = STATE["news_events_today"][:]

            if not running or not pairs or not events:
                time.sleep(30)
                continue

            now = now_utc()

            for event in events:
                eid = event["id"]
                event_time = event["event_time_utc"]
                seconds_to_event = (event_time - now).total_seconds()

                # 30-minute warning
                with state_lock:
                    already_warned = eid in STATE["news_warned_ids"]
                if not already_warned and warn_window_lower <= seconds_to_event <= warn_window_upper:
                    try:
                        safe_send(chat_id, format_news_warning(event))
                        with state_lock:
                            STATE["news_warned_ids"].add(eid)
                        print(f"[NEWS] ⚠️ {NEWS_WARN_MINUTES_BEFORE}-min warning sent for: {event['title']}")
                    except Exception as e:
                        print(f"[NEWS] Error sending warning: {e}")

                # News signal: 3-8 minutes before event
                with state_lock:
                    already_sig_sent = eid in STATE["news_signal_sent_ids"]
                if not already_sig_sent and 180 <= seconds_to_event <= 480:
                    try:
                        # Respect pair blocks — don't generate news signals for blocked pairs
                        with state_lock:
                            pair_losses = dict(STATE.get("pair_consecutive_losses", {}))
                        blocked = [p for p, v in pair_losses.items() if v >= 2]
                        news_pairs = [p for p in event["affected_pairs"] if p in pairs and p not in blocked]
                        if news_pairs:
                            signal = build_signal(news_pairs)
                            if signal:
                                entry_time  = get_next_candle_open()
                                expiry_time = entry_time + timedelta(minutes=signal["expiry_min"])
                                advance     = (entry_time - now_utc()).total_seconds()
                                signal["entry_time"]  = entry_time
                                signal["expiry_time"] = expiry_time
                                signal["entry_ist"]   = entry_time.astimezone(IST).strftime("%H:%M")
                                signal["expiry_ist"]  = expiry_time.astimezone(IST).strftime("%H:%M")
                                msg = format_news_signal_message(signal, event, advance_seconds=int(advance))
                                safe_send(chat_id, msg)
                    except Exception as e:
                        print(f"[NEWS] Error generating news signal: {e}")
                    finally:
                        with state_lock:
                            STATE["news_signal_sent_ids"].add(eid)

            time.sleep(30)

        except Exception as e:
            print(f"[NEWS] Error in monitor loop: {e}")
            traceback.print_exc()
            time.sleep(60)


# ============================== TELEGRAM SIGNAL MESSAGES ==============================
def format_signal_message(signal: Dict, signal_no: int, advance_seconds: int = 0) -> str:
    num_confirmations = len(signal["confirmations"])

    if advance_seconds >= 25:
        advance_line = f"⚡  Entry in {advance_seconds}s — open chart now!\n\n"
    elif advance_seconds > 0:
        advance_line = f"⚡  Entry in ~{advance_seconds}s\n\n"
    else:
        advance_line = ""

    amount_line = ""
    recovery_note = ""

    direction_text = "🟢 CALL ▲" if signal["direction"] == "CALL" else "🔴 PUT ▼"

    return (
        f"🔔 <b>SIGNAL #{signal_no:02d}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{advance_line}"
        f"💹 Pair: <b>{signal['pair']}</b>\n\n"
        f"🕐 Entry: <b>{signal['entry_ist']} IST</b>\n\n"
        f"🕐 Expiry: <b>{signal['expiry_ist']} IST</b>  ({signal['expiry_min']} min)\n\n"
        f"🎯 Direction: <b>{direction_text}</b>\n\n"
        f"{amount_line}"
        f"{recovery_note}"
        f"📊 Quality Score: <b>{signal['score']:.1f}/10.0</b> ⭐\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <b>CONFIRMATIONS ({num_confirmations}):</b>\n"
        f"  💡 High-accuracy setup — {num_confirmations} confluences aligned\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⏳ <i>Auto-rejects in {SIGNAL_AUTO_REJECT_SECONDS}s if no response</i>"
    )


_sent_signals_lock = threading.Lock()
_sent_signal_nos: set = set()   # track signal numbers already sent this session
_asked_result_nos: set = set()  # track result-asks already sent

def _clear_signal_dedup(signal_no: int):
    """Remove a signal number from dedup sets so it can be re-sent if needed."""
    with _sent_signals_lock:
        _sent_signal_nos.discard(signal_no)
        _asked_result_nos.discard(signal_no)

def send_signal_with_buttons(chat_id: int, signal: Dict, signal_no: int, advance_seconds: int = 0) -> Optional[types.Message]:
    """Send signal message with Accept/Reject inline buttons. Returns sent Message or None."""
    with _sent_signals_lock:
        if signal_no in _sent_signal_nos:
            # Clear and allow resend — previous send must have failed
            _sent_signal_nos.discard(signal_no)
        _sent_signal_nos.add(signal_no)

    # 2. Dynamic Performance Analytics — Add Auto-Learning to the signal text
    with state_lock:
        current_hour = str(now_ist().hour)
        pair_data = STATE.get("pair_performance", {}).get(signal["pair"], {"wins": 0, "losses": 0})
        hour_data = STATE.get("hour_performance", {}).get(current_hour, {"wins": 0, "losses": 0})

    p_total = pair_data["wins"] + pair_data["losses"]
    h_total = hour_data["wins"] + hour_data["losses"]
    p_wr = (pair_data["wins"] / p_total * 100) if p_total > 0 else 0.0
    h_wr = (hour_data["wins"] / h_total * 100) if h_total > 0 else 0.0

    # Format normal signal message and append dynamic auto-learning stats
    msg_text = format_signal_message(signal, signal_no, advance_seconds)
    msg_text += (
        f"\n\n📈 <b>Auto-Learning Analytics:</b>\n"
        f"└ Pair WR: {p_wr:.1f}% ({pair_data['wins']}/{p_total})\n"
        f"└ Hour WR: {h_wr:.1f}% ({hour_data['wins']}/{h_total})"
    )

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(f"Accept ✅", callback_data=f"sig_accept:{signal_no}"),
        types.InlineKeyboardButton(f"Reject ❌", callback_data=f"sig_reject:{signal_no}")
    )
    result = safe_send(chat_id, msg_text, reply_markup=kb)
    if result is None:
        # Send failed — remove from dedup so next scan can retry
        with _sent_signals_lock:
            _sent_signal_nos.discard(signal_no)
    return result


def ask_signal_result(chat_id: int, signal_no: int, pair: str, expiry_ist: str):
    """Ask for the trade result after expiry. Dedup: only ask once per signal."""
    with _sent_signals_lock:
        if signal_no in _asked_result_nos:
            print(f"[DEDUP] Result ask for signal #{signal_no} already sent — skipping")
            return
        _asked_result_nos.add(signal_no)
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("Profit 🟢", callback_data=f"result_profit:{signal_no}"),
        types.InlineKeyboardButton("Loss 🔴", callback_data=f"result_loss:{signal_no}"),
        types.InlineKeyboardButton("Refund ⚪", callback_data=f"result_refund:{signal_no}")
    )
    safe_send(
        chat_id,
        f"⏰ <b>Signal #{signal_no:02d} Expired</b>\n\n"
        f"💹 <b>Pair:</b> {pair}\n"
        f"🕐 <b>Expiry Time:</b> {expiry_ist} IST\n\n"
        f"What was the result of this trade?",
        reply_markup=kb
    )

# ============================== SCHEDULER ==============================
def interruptible_sleep(seconds: float, interval: float = 2.0) -> bool:
    """Sleep interruptibly. Returns False if bot was stopped during sleep."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        with state_lock:
            if not STATE["bot_running"]:
                return False
        remaining = deadline - time.time()
        time.sleep(min(interval, max(0, remaining)))
    return True

def auto_reject_signal(signal_no: int, msg_id: int, chat_id: int):
    """
    Called in a background thread after SIGNAL_AUTO_REJECT_SECONDS.
    If signal is still pending (not yet accepted/rejected), auto-reject it.
    """
    time.sleep(SIGNAL_AUTO_REJECT_SECONDS)
    with state_lock:
        pending = STATE.get("pending_signals", {})
        if signal_no not in pending:
            # Already handled by user
            return
        # Remove from pending
        pending.pop(signal_no, None)
        active = STATE.get("active_signal")
        if active and active.get("no") == signal_no:
            STATE["active_signal"] = None
        # Do NOT bump signal_no — auto-rejected signal is not counted,
        # so the next signal reuses this same number
        print(f"[AUTO-REJECT] Signal #{signal_no} auto-rejected after {SIGNAL_AUTO_REJECT_SECONDS}s — reusing #{signal_no} for next signal")

    # Clear dedup so the same number can be sent again next scan
    _clear_signal_dedup(signal_no)

    # Delete the message from Telegram
    safe_delete(chat_id, msg_id)
    safe_send(chat_id,
        f"⏱️ <b>Signal #{signal_no:02d} Auto-Rejected</b>\n\n"
        f"No response received within {SIGNAL_AUTO_REJECT_SECONDS} seconds.\n"
        f"Signal was automatically rejected and not counted.\n\n"
        f"🔍 Scanning for next signal..."
    )

_scan_lock = threading.Lock()  # prevents two simultaneous scans sending duplicate signals/messages

def scheduler_loop():
    print("[SCHEDULER] Started (v4 - Fixed + Auto-reject + Result-wait)")

    last_no_signal_msg_time = 0
    last_market_closed_msg_time = 0
    last_heartbeat_print = 0
    last_stopped_print = 0
    ADVANCE_SECONDS = 30   # scan 30s before candle open → signal arrives ~25s early

    with state_lock:
        if "pending_signals" not in STATE:
            STATE["pending_signals"] = {}

    while True:
        try:
            # Update watchdog heartbeat
            with state_lock:
                STATE["_scheduler_last_heartbeat"] = time.time()

            with state_lock:
                running = STATE["bot_running"]
                pairs = STATE["selected_pairs"][:]
                active = STATE.get("active_signal")
                chat_id = STATE.get("telegram_chat_id")
                waiting_result = STATE.get("waiting_for_result", False)

            # ── Live heartbeat every 15 seconds ──────────────────────────────
            now_ts = time.time()
            if now_ts - last_heartbeat_print >= 15:
                ist_now = now_ist().strftime("%H:%M:%S")
                if not running:
                    # Only print "stopped" once every 5 minutes to avoid spam
                    if now_ts - last_stopped_print >= 300:
                        print(f"[{ist_now}] ⏸  Bot is STOPPED — send /pairs to start")
                        last_stopped_print = now_ts
                elif not pairs:
                    print(f"[{ist_now}] ⚠️  No pairs selected — send /pairs")
                elif waiting_result:
                    sig_no = active["no"] if active else "?"
                    print(f"[{ist_now}] ⏳  Waiting for trade result on Signal #{sig_no} — tap Profit/Loss in Telegram")
                elif active:
                    expiry_str = active.get("expiry_ist", "?")
                    print(f"[{ist_now}] 📊  Active signal #{active['no']:02d} ({active['pair']}) — expires at {expiry_str} IST")
                else:
                    next_open = get_next_candle_open()
                    secs = max(0, int((next_open - now_utc()).total_seconds()))
                    print(f"[{ist_now}] 🔍  Scanning... next candle opens in {secs}s | Pairs: {', '.join(pairs)}")
                last_heartbeat_print = now_ts

            if not running or not pairs:
                time.sleep(5)
                continue

            # ── If waiting for result, poll until user clicks ─────────────
            if waiting_result:
                time.sleep(5)
                continue

            # ── Post-result delay: wait for next candle boundary ──────────
            with state_lock:
                next_scan_after = STATE.get("_next_scan_after", 0.0)
            if time.time() < next_scan_after:
                remaining = next_scan_after - time.time()
                print(f"[SCHEDULER] ⏳ Post-result cooldown — waiting {remaining:.0f}s for fresh candle data...")
                if not interruptible_sleep(remaining):
                    print("[SCHEDULER] /stop received — aborting post-result wait")
                continue

            # ── Reset signal counter at midnight IST ─────────────────────
            today_key = get_today_key()
            with state_lock:
                current_counter_date = STATE.get("_counter_date", "")
            if current_counter_date != today_key:
                today_no = get_signal_no_for_today()
                with state_lock:
                    STATE["signal_no"] = today_no
                    STATE["_counter_date"] = today_key
                    # New day — reset state
                    STATE["pair_consecutive_losses"] = {}
                print(f"[SCHEDULER] 🗓 New day detected ({today_key}) — signal counter set to #{today_no}")

            # ── Refresh news cache if the date has changed ─────────────
            with state_lock:
                cached_date = STATE.get("news_cache_date")
            today_ist_date = now_ist().date()
            if cached_date != today_ist_date:
                def _refresh_news():
                    try:
                        events = fetch_todays_high_impact_news(pairs)
                        with state_lock:
                            STATE["news_events_today"] = events
                            STATE["news_warned_ids"] = set()
                            STATE["news_signal_sent_ids"] = set()
                            STATE["news_cache_date"] = today_ist_date
                        print(f"[NEWS] Daily refresh — {len(events)} events for {today_ist_date}")
                    except Exception as ex:
                        print(f"[NEWS] Refresh error: {ex}")
                threading.Thread(target=_refresh_news, daemon=True).start()

            # ── Check if active signal has expired ──────────────────────
            # Re-read active signal fresh here to avoid stale value from top
            # of loop (auto_reject thread may have cleared it during sleep)
            with state_lock:
                active = STATE.get("active_signal")

            if active:
                expiry_time = active.get("expiry_time")
                if expiry_time and now_utc() >= expiry_time:
                    sig_no = active["no"]
                    print(f"[SCHEDULER] Signal #{sig_no} expired — asking for result...")
                    with state_lock:
                        STATE["active_signal"] = None
                        # Check if it was accepted (recorded in memory)
                        pending = STATE.get("pending_signals", {})

                    # Only ask result if signal was actually accepted (not auto-rejected)
                    today = get_today_key()
                    with memory_lock:
                        mem = load_memory()
                        recorded = mem.get("days", {}).get(today, {}).get("signals", {}).get(str(sig_no))

                    if recorded and recorded.get("accepted"):
                        # Set waiting_for_result flag — scheduler pauses until user clicks
                        with state_lock:
                            STATE["waiting_for_result"] = True
                        ask_signal_result(chat_id, sig_no, active["pair"], active["expiry_ist"])
                        print(f"[SCHEDULER] Waiting for result on Signal #{sig_no}...")
                        # Poll until result received (or bot stopped)
                        timeout = time.time() + 300  # 5 min max wait
                        while time.time() < timeout:
                            with state_lock:
                                still_waiting = STATE.get("waiting_for_result", False)
                                still_running = STATE["bot_running"]
                            if not still_waiting or not still_running:
                                break
                            time.sleep(3)
                        # Check if user actually responded within timeout
                        with state_lock:
                            still_waiting = STATE.get("waiting_for_result", False)
                        if still_waiting:
                            # Timed out — user never responded. Keep waiting_for_result=True
                            # and remind the user. Bot will NOT continue until they respond.
                            with state_lock:
                                STATE["waiting_for_result"] = True  # keep blocking
                            safe_send(
                                chat_id,
                                f"⚠️ <b>Result Pending — Signal #{sig_no:02d}</b>\n\n"
                                f"You haven't marked the result for <b>{active['pair']}</b> yet.\n\n"
                                f"⏸ <b>Bot is paused</b> until you tap Profit or Loss above.\n"
                                f"The martingale amount for your next trade depends on this result.\n\n"
                                f"<i>Scroll up to find the Profit 🟢 / Loss 🔴 buttons.</i>"
                            )
                            print(f"[SCHEDULER] ⚠️ Result timed out for Signal #{sig_no} — bot paused, user reminded")
                            # Continue polling indefinitely until user responds or stops bot
                            while True:
                                with state_lock:
                                    still_waiting = STATE.get("waiting_for_result", False)
                                    still_running = STATE["bot_running"]
                                if not still_waiting or not still_running:
                                    break
                                time.sleep(5)
                    # Now scan for next signal
                    continue
                elif expiry_time:
                    # Signal still active, wait
                    time.sleep(5)
                    continue

            # ── Block new signals while accepted signal is live ───────────
            with state_lock:
                active_now = STATE.get("active_signal")
                waiting_now = STATE.get("waiting_for_result", False)
            if active_now or waiting_now:
                time.sleep(5)
                continue

            # ── Market checks ────────────────────────────────────────────
            market = check_quotex_market_status(pairs)
            if market["all_otc"]:
                current_time = time.time()
                if current_time - last_market_closed_msg_time > 1800:
                    safe_send(chat_id, market["message"])
                    last_market_closed_msg_time = current_time
                    with state_lock:
                        STATE["bot_running"] = False
                time.sleep(30)
                continue

            if not is_trading_hours():
                current_time = time.time()
                if current_time - last_market_closed_msg_time > 1800:
                    safe_send(
                        chat_id,
                        "🟥 <b>Market Closed - Off Hours</b>\n\n"
                        "Trading is paused during off-hours (2 AM - 6 AM IST).\n"
                        "Bot will resume scanning soon."
                    )
                    last_market_closed_msg_time = current_time
                time.sleep(30)
                continue

            # ── Calculate next candle open ────────────────────────────────
            next_open = get_next_candle_open()
            seconds_to_open = (next_open - now_utc()).total_seconds()

            if seconds_to_open > 65:
                wait_for = seconds_to_open - ADVANCE_SECONDS - 3
                if wait_for > 2:
                    print(f"[SCHEDULER] Next candle in {seconds_to_open:.0f}s — waiting {wait_for:.0f}s")
                    if not interruptible_sleep(min(wait_for, 15)):
                        print("[SCHEDULER] /stop received — aborting pre-scan wait")
                    continue

            print(f"\n[SCHEDULER] 🔍 Scanning {len(pairs)} pairs — next candle in {seconds_to_open:.0f}s | signal will arrive ~30s early")

            # ── Build signal (scan lock prevents duplicate concurrent scans) ─
            if not _scan_lock.acquire(blocking=False):
                print("[SCHEDULER] ⚠️ Scan already in progress — skipping this cycle")
                time.sleep(5)
                continue

            try:
                signal = build_signal(pairs)
            finally:
                _scan_lock.release()

            if signal:
                # === AUTO-LEARNING SCORE MODIFIER ===
                base_score = float(signal.get("score", 0.0))
                final_score = apply_auto_learning_bonus(signal["pair"], base_score)
                signal["score"] = final_score

                # Enforce minimum score threshold
                min_allowed_score = float(MIN_SCORE)
                if final_score < min_allowed_score:
                    print(f"[SCHEDULER] Signal rejected by learning penalty filter. Score: {final_score} < Minimum: {min_allowed_score}")
                    time.sleep(5)
                    continue

                with state_lock:
                    signal_no = STATE["signal_no"]

                entry_time = get_next_candle_open()
                expiry_time = entry_time + timedelta(minutes=signal["expiry_min"])
                entry_ist = entry_time.astimezone(IST).strftime("%H:%M")
                expiry_ist = expiry_time.astimezone(IST).strftime("%H:%M")

                signal["no"] = signal_no
                signal["entry_time"] = entry_time
                signal["expiry_time"] = expiry_time
                signal["entry_ist"] = entry_ist
                signal["expiry_ist"] = expiry_ist

                # Store in pending signals
                with state_lock:
                    STATE.setdefault("pending_signals", {})[signal_no] = signal
                    STATE["active_signal"] = signal

                advance = (entry_time - now_utc()).total_seconds()

                sent_msg = send_signal_with_buttons(chat_id, signal, signal_no, advance_seconds=int(advance))
                if sent_msg:
                    print(f"[SCHEDULER] ✅ Sent signal #{signal_no} — entry at {entry_ist} IST")
                    last_no_signal_msg_time = 0

                    # Start auto-reject timer in background
                    threading.Thread(
                        target=auto_reject_signal,
                        args=(signal_no, sent_msg.message_id, chat_id),
                        daemon=True
                    ).start()

                    # Wait until signal expiry (interruptible)
                    wait_until_expiry = max(0, (expiry_time - now_utc()).total_seconds() - 2)
                    print(f"[SCHEDULER] Waiting {wait_until_expiry:.0f}s until signal expires...")
                    if not interruptible_sleep(wait_until_expiry):
                        print("[SCHEDULER] /stop received — aborting wait after signal")
                        continue
                    # Sleep finished — loop back immediately to trigger expiry/result check
                    continue
                else:
                    # Failed to send — clear active signal, bump signal number, retry next scan
                    next_no = signal_no + 1
                    save_signal_no_for_today(next_no)
                    with state_lock:
                        STATE["active_signal"] = None
                        STATE["signal_no"] = next_no
                        STATE.get("pending_signals", {}).pop(signal_no, None)
                    _clear_signal_dedup(signal_no)
                    print(f"[SCHEDULER] ⚠️ Failed to send signal #{signal_no} — bumping to #{next_no} and retrying")
                    time.sleep(10)

            else:
                # No signal found
                current_time = time.time()
                if current_time - last_no_signal_msg_time > 600:  # 10 min cooldown
                    with state_lock:
                        still_running = STATE["bot_running"]
                    if still_running:
                        try:
                            next_scan_ist = (now_utc() + timedelta(seconds=60)).astimezone(IST).strftime("%H:%M:%S")
                            safe_send(
                                chat_id,
                                "❌ <b>No Signal Found</b>\n\n"
                                "No setup meets our high-accuracy criteria.\n"
                                f"🔎 Scanning again at: {next_scan_ist} IST"
                            )
                            last_no_signal_msg_time = current_time
                        except Exception as e:
                            print(f"[SCHEDULER] Error sending no-signal message: {e}")

                # Wait until next candle open — minimum 55s so candle data refreshes
                seconds_to_next = (get_next_candle_open() - now_utc()).total_seconds()
                wait_secs = max(55, seconds_to_next - ADVANCE_SECONDS + 1)
                print(f"[SCHEDULER] Waiting {wait_secs:.0f}s for next candle before re-scanning...")
                if not interruptible_sleep(wait_secs):
                    print("[SCHEDULER] /stop received — aborting no-signal wait")
                    continue

        except Exception as e:
            print(f"[SCHEDULER] Error in main loop: {e}")
            traceback.print_exc()
            time.sleep(30)


# ============================== MAIN ==============================
scheduler_thread = None
news_thread = None

def start_bot():
    global scheduler_thread, news_thread

    print("🚀 Starting RealZahedBinaryBot v4")
    print(f"✅ Authorized Chat ID: {AUTHORIZED_CHAT_ID}")
    print(f"💾 Memory file: {MEMORY_FILE}")
    print(f"⏱️ Signal auto-reject: {SIGNAL_AUTO_REJECT_SECONDS}s")
    print(f"📰 News warning: {NEWS_WARN_MINUTES_BEFORE} min before event")

    # Load today's signal counter at startup (owner's counter)
    today_no = user_get_signal_no(AUTHORIZED_CHAT_ID)
    if today_no == 1:
        today_no = get_signal_no_for_today()  # fallback to global if user not yet set
    with state_lock:
        STATE["signal_no"] = today_no
        STATE["_counter_date"] = get_today_key()
        STATE["pending_signals"] = {}
        STATE["_scheduler_last_heartbeat"] = time.time()
        STATE["_news_monitor_last_heartbeat"] = time.time()
        # Restore persisted capital
        persisted_capital = load_capital()
        if persisted_capital:
            STATE["capital"] = persisted_capital
            print(f"💰 Restored capital: ${persisted_capital:.2f}")
        # Restore per-pair loss counters
        STATE["pair_consecutive_losses"] = load_pair_losses()
        blocked = [p for p, v in STATE["pair_consecutive_losses"].items() if v >= 2]
        if blocked:
            print(f"🚫 Pairs currently blocked: {', '.join(blocked)}")

    # Load approved users into memory cache
    _load_approved_users_cache()
    approved_count = len(_approved_users)
    if approved_count:
        print(f"👥 Loaded {approved_count} approved user(s) from memory")
    print(f"🔢 Resuming from signal #{today_no} (today: {get_today_key()})")

    try:
        bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        print(f"[STARTUP] Webhook deletion failed (non-critical): {e}")

    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True, name="scheduler")
    scheduler_thread.start()

    news_thread = threading.Thread(target=news_monitor_loop, daemon=True, name="news_monitor")
    news_thread.start()

    threading.Thread(target=watchdog_loop, daemon=True, name="watchdog").start()

    print("[BOT] Starting polling...")
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=25)
        except Exception as e:
            print(f"[ERROR] Polling crashed: {e}")
            time.sleep(10)

if __name__ == "__main__":
    try:
        start_bot()
    except KeyboardInterrupt:
        print("\n[INFO] Bot stopped by user")
    except Exception as e:
        print(f"\n[CRITICAL] Fatal error: {e}")
        traceback.print_exc()
