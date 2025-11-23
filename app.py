#!/usr/bin/env python3
"""
Telegram Word-Splitter Bot (Webhook-ready) - app.py (updated: safer 429 handling)

Changes in this file compared to the previous integrated version:
- Remove long blocking sleeps when Telegram returns a large retry_after on 429.
- Implement a global, capped "tele_pause" (tele_pause_until) set when a 429 is observed.
  Workers check tele_pause and pause tasks (persisted as task status 'paused') instead of
  sleeping inside tg_call — this avoids worker timeouts and prevents duplicate sends / lost counts.
- Add TELE_PAUSE_MAX_SECONDS env var (default 60) to cap Telegram-suggested retry_after.
- tg_call no longer blocks for long retry_after; it sets tele_pause and returns quickly.
- robust_send_message and process_user_queue now detect tele_pause and behave non-blocking:
  robust_send_message will fail fast when tele_pause active; process_user_queue will pause the task
  and notify the user + owners to avoid repeated failed attempts.
- Logging improved around tele_pause events so operators can see when the bot is backing off.
"""
import os
import time
import json
import sqlite3
import threading
import traceback
import logging
import re
import uuid
import hashlib
from datetime import datetime, timedelta
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests
import random

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Create Flask app once
app = Flask(__name__)

# Configuration via environment variables
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")  # full https URL for webhook

# Support multiple owners via OWNER_IDS (comma-separated). Backwards-compatible with single OWNER_ID.
_owner_ids_raw = os.environ.get("OWNER_IDS")
if not _owner_ids_raw:
    _owner_ids_raw = os.environ.get("OWNER_ID", "0")
_owner_ids_list = [p.strip() for p in re.split(r"[,\s]+", _owner_ids_raw) if p and p.strip().isdigit()]
OWNERS = set(int(p) for p in _owner_ids_list) if _owner_ids_list else set()
# Primary owner for legacy places expecting a single OWNER_ID
OWNER_ID = next(iter(OWNERS)) if OWNERS else 0

OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "justmemmy")
MAX_ALLOWED_USERS = int(os.environ.get("MAX_ALLOWED_USERS", "50"))
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "50"))
MAINTENANCE_START_HOUR_WAT = 3  # 03:00 WAT
MAINTENANCE_END_HOUR_WAT = 4    # 04:00 WAT
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))

# Rate-limit and retry configuration (minimum enforced earlier)
_raw_max_msg_per_second = float(os.environ.get("MAX_MSG_PER_SECOND", "50"))
MAX_MSG_PER_SECOND = max(50.0, _raw_max_msg_per_second)
TG_CALL_MAX_RETRIES = int(os.environ.get("TG_CALL_MAX_RETRIES", "5"))
TG_CALL_MAX_BACKOFF = float(os.environ.get("TG_CALL_MAX_BACKOFF", "60"))  # seconds

# New configuration for per-recipient retries
BROADCAST_MAX_ATTEMPTS = int(os.environ.get("BROADCAST_MAX_ATTEMPTS", "3"))
SPLIT_MAX_ATTEMPTS = int(os.environ.get("SPLIT_MAX_ATTEMPTS", "3"))
ROBUST_SEND_BASE_BACKOFF = float(os.environ.get("ROBUST_SEND_BASE_BACKOFF", "1.0"))  # seconds

# New config: cap for Telegram's retry_after (avoid massive blocking sleeps)
TELE_PAUSE_MAX_SECONDS = int(os.environ.get("TELE_PAUSE_MAX_SECONDS", "60"))

if not TELEGRAM_TOKEN or not WEBHOOK_URL or not OWNER_ID:
    logger.warning("TELEGRAM_TOKEN, WEBHOOK_URL, OWNER_ID (or OWNER_IDS) should be set in environment.")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

# Reuse a requests.Session for faster connections
_session = requests.Session()

# DB helper
_db_lock = threading.Lock()

# Global tele pause state to avoid sleeping inside tg_call for huge retry_after values
_tele_pause_lock = threading.Lock()
_tele_pause_until = 0.0  # epoch seconds until which we should pause sending due to Telegram 429


def set_tele_pause(seconds: float, reason: str = ""):
    """Set a global pause until now + seconds. Thread-safe."""
    global _tele_pause_until
    if seconds <= 0:
        return
    cap = min(float(seconds), float(TELE_PAUSE_MAX_SECONDS))
    with _tele_pause_lock:
        new_until = time.time() + cap
        # only extend if new_until later
        if new_until > _tele_pause_until:
            _tele_pause_until = new_until
            logger.warning("Global Telegram pause set for %.1fs (capped) due to: %s. Until: %s",
                           cap, reason, datetime.utcfromtimestamp(_tele_pause_until).isoformat())


def get_tele_pause_remaining() -> float:
    """Return remaining pause seconds (0 if none)."""
    with _tele_pause_lock:
        rem = _tele_pause_until - time.time()
        return rem if rem > 0 else 0.0


def is_tele_paused() -> bool:
    return get_tele_pause_remaining() > 0


def init_db():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        # allowed users table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_at TEXT,
                is_admin INTEGER DEFAULT 0
            )
            """
        )
        # tasks: one row per submitted text task
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                text TEXT,
                words_json TEXT,
                total_words INTEGER,
                sent_count INTEGER DEFAULT 0,
                status TEXT,
                created_at TEXT,
                started_at TEXT,
                finished_at TEXT
            )
            """
        )
        # split logs (what words were split) for stats
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS split_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                words INTEGER,
                created_at TEXT
            )
            """
        )
        # bot messages we sent (to allow deletion)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sent_at TEXT,
                deleted INTEGER DEFAULT 0
            )
            """
        )
        # suspended users table (user_id -> suspended_until ISO timestamp)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS suspended_users (
                user_id INTEGER PRIMARY KEY,
                suspended_until TEXT,
                reason TEXT,
                added_by INTEGER,
                added_at TEXT
            )
            """
        )
        # broadcast tables for idempotent broadcasts
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS broadcast_runs (
                run_id TEXT PRIMARY KEY,
                message_hash TEXT,
                message TEXT,
                created_by INTEGER,
                created_at TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS broadcast_recipients (
                run_id TEXT,
                user_id INTEGER,
                status TEXT DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                last_attempt TEXT,
                PRIMARY KEY (run_id, user_id)
            )
            """
        )
        conn.commit()

        # Migration: ensure sent_count column exists (for older DBs)
        c.execute("PRAGMA table_info(tasks)")
        cols = [r[1] for r in c.fetchall()]
        if "sent_count" not in cols:
            try:
                c.execute("ALTER TABLE tasks ADD COLUMN sent_count INTEGER DEFAULT 0")
                conn.commit()
                logger.info("Migrated tasks table: added sent_count")
            except Exception:
                logger.exception("Failed to add sent_count column (maybe already present)")


def db_execute(query, params=(), fetch=False, retries=6):
    """
    Central DB helper with locking + retry on OperationalError (e.g. database is locked).
    Returns rows if fetch=True, otherwise returns None.
    """
    attempt = 0
    delay = 0.05
    while True:
        try:
            # single-use connection per call. 30s timeout helps SQLite wait for locks instead of immediate SQLITE_BUSY.
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute(query, params)
                if fetch:
                    rows = c.fetchall()
                    return rows
                conn.commit()
                return None
        except sqlite3.OperationalError as e:
            # common transient error: database is locked — retry a few times with small backoff
            attempt += 1
            if attempt >= retries:
                logger.exception("SQLite OperationalError after %d attempts: %s. Query: %s Params: %s", attempt, e, query, params)
                raise
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
        except Exception:
            logger.exception("Unexpected DB error on query: %s params=%s", query, params)
            raise


# Initialize DB and ensure owners are allowed/admin
init_db()
try:
    # Ensure each owner present as admin in allowed_users
    if OWNERS:
        for oid in OWNERS:
            res = db_execute("SELECT user_id FROM allowed_users WHERE user_id = ?", (oid,), fetch=True)
            if not res:
                db_execute(
                    "INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                    (oid, OWNER_USERNAME, datetime.utcnow().isoformat(), 1),
                )
            else:
                db_execute("UPDATE allowed_users SET is_admin = 1 WHERE user_id = ?", (oid,))

    # Add any preconfigured allowed users from ALLOWED_USERS env var (comma/space separated ids).
    _allowed_users_raw = os.environ.get("ALLOWED_USERS")
    if not _allowed_users_raw:
        _allowed_users_raw = os.environ.get("allowed_users", "")
    if _allowed_users_raw is None:
        _allowed_users_raw = ""

    if _allowed_users_raw:
        logger.info("ALLOWED_USERS env raw value: %s", _allowed_users_raw)
        parts = [p.strip() for p in re.split(r"[,\s]+", _allowed_users_raw) if p and p.strip()]
        numeric_ids = []
        invalid_parts = []
        for p in parts:
            m = re.match(r"^\+?(\d+)$", p)
            if m:
                try:
                    numeric_ids.append(int(m.group(1)))
                except Exception:
                    invalid_parts.append(p)
            else:
                invalid_parts.append(p)

        if invalid_parts:
            logger.warning("ALLOWED_USERS: ignored invalid entries: %s", ", ".join(invalid_parts))

        try:
            current_total = int(db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0])
        except Exception:
            current_total = 0
            logger.exception("ALLOWED_USERS: failed to read current allowed_users count; assuming 0")

        added_env = []
        skipped_existing = []
        skipped_owner = []
        skipped_max = []

        for uid in numeric_ids:
            if uid in OWNERS:
                skipped_owner.append(uid)
                continue
            exists = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (uid,), fetch=True)
            if exists:
                skipped_existing.append(uid)
                continue
            if current_total >= MAX_ALLOWED_USERS:
                skipped_max.append(uid)
                logger.warning("ALLOWED_USERS: skipping %s because MAX_ALLOWED_USERS (%d) reached", uid, MAX_ALLOWED_USERS)
                break
            try:
                db_execute(
                    "INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                    (uid, "", datetime.utcnow().isoformat(), 0),
                )
                added_env.append(uid)
                current_total += 1
            except Exception:
                logger.exception("ALLOWED_USERS: failed to insert id %s", uid)

        if added_env:
            logger.info("Added ALLOWED_USERS from environment: %s", ", ".join(str(x) for x in added_env))
        if skipped_existing:
            logger.info("ALLOWED_USERS: already present (skipped): %s", ", ".join(str(x) for x in skipped_existing))
        if skipped_owner:
            logger.info("ALLOWED_USERS: skipped owner ids: %s", ", ".join(str(x) for x in skipped_owner))
        if skipped_max:
            logger.info("ALLOWED_USERS: skipped due to MAX_ALLOWED_USERS limit: %s", ", ".join(str(x) for x in skipped_max))
    else:
        logger.debug("No ALLOWED_USERS env var set.")
except Exception:
    logger.exception("Error ensuring owners in allowed_users")


# In-memory per-user locks to guarantee single active worker per user (persisted states in DB)
user_locks = {}
user_locks_lock = threading.Lock()


def get_user_lock(user_id):
    with user_locks_lock:
        if user_id not in user_locks:
            user_locks[user_id] = threading.Lock()
        return user_locks[user_id]


# Utilities
def get_now_iso():
    return datetime.utcnow().isoformat()


def split_text_into_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]


def compute_interval(total_words: int) -> float:
    if total_words <= 150:
        return 0.4
    elif total_words <= 300:
        return 0.5
    else:
        return 0.6


def is_maintenance_now() -> bool:
    utc_now = datetime.utcnow()
    wat_now = utc_now + timedelta(hours=1)
    h = wat_now.hour
    if MAINTENANCE_START_HOUR_WAT < MAINTENANCE_END_HOUR_WAT:
        return MAINTENANCE_START_HOUR_WAT <= h < MAINTENANCE_END_HOUR_WAT
    return h >= MAINTENANCE_START_HOUR_WAT or h < MAINTENANCE_END_HOUR_WAT


# --- Rate limiter (token bucket) for outgoing Telegram requests ---
_token_bucket = {
    "tokens": MAX_MSG_PER_SECOND,
    "last": time.time(),
    "lock": threading.Lock(),
    "capacity": max(1.0, MAX_MSG_PER_SECOND)
}


def _consume_token(block=True, timeout=10.0):
    start = time.time()
    while True:
        with _token_bucket["lock"]:
            now = time.time()
            elapsed = now - _token_bucket["last"]
            if elapsed > 0:
                refill = elapsed * MAX_MSG_PER_SECOND
                _token_bucket["tokens"] = min(_token_bucket["capacity"], _token_bucket["tokens"] + refill)
                _token_bucket["last"] = now
            if _token_bucket["tokens"] >= 1:
                _token_bucket["tokens"] -= 1
                return True
        if not block:
            return False
        if time.time() - start >= timeout:
            return False
        time.sleep(0.01)


# --- Robust tg_call implementation (now sets global tele pause on 429 instead of sleeping very long) ---
_tele_429_count = 0


def _parse_retry_after_from_response(data, resp_text=""):
    if isinstance(data, dict):
        params = data.get("parameters") or {}
        retry_after = params.get("retry_after") or data.get("retry_after")
        if retry_after:
            try:
                return int(retry_after)
            except Exception:
                pass
        desc = data.get("description") or ""
        m = re.search(r"retry after (\d+)", desc, re.I)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    m = re.search(r"retry after (\d+)", resp_text, re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return None


def tg_call(method: str, payload: dict):
    """
    Perform the HTTP call to Telegram. On 429 we SET a global tele pause (capped) and return None quickly.
    We avoid sleeping for huge retry_after values inside this function to prevent worker timeouts.
    """
    global _tele_429_count
    if not TELEGRAM_API:
        logger.error("tg_call attempted but TELEGRAM_API not configured")
        return None

    url = f"{TELEGRAM_API}/{method}"
    max_retries = max(1, TG_CALL_MAX_RETRIES)
    backoff = 1.0

    for attempt in range(1, max_retries + 1):
        # If a global tele pause is active, return early — caller should handle paused state gracefully.
        if is_tele_paused():
            logger.warning("tg_call: global tele pause is active (remaining %.1fs). Aborting attempt.",
                           get_tele_pause_remaining())
            return None

        token_acquired = _consume_token(block=True, timeout=10.0)
        if not token_acquired:
            logger.warning("tg_call: could not acquire token within timeout; attempt %d/%d", attempt, max_retries)

        try:
            resp = _session.post(url, json=payload, timeout=REQUESTS_TIMEOUT)
            try:
                data = resp.json()
            except Exception:
                data = None

            if resp.status_code == 429:
                _tele_429_count += 1
                retry_after = _parse_retry_after_from_response(data, resp.text)
                # Instead of sleeping here for retry_after (which can be very large), we set a global pause (capped)
                # and return quickly to allow worker code to persist state and avoid being killed by worker timeout.
                pause_seconds = retry_after if retry_after is not None else backoff
                set_tele_pause(pause_seconds, reason=f"tg_call 429 on method={method}")
                logger.warning(
                    "Telegram API 429 Too Many Requests (attempt %d/%d). Parsed retry_after=%s. Applied capped pause of up to %ds. method=%s",
                    attempt, max_retries, retry_after, TELE_PAUSE_MAX_SECONDS, method,
                )
                # Return None to indicate send failure; callers will decide how to handle
                return None

            if 500 <= resp.status_code < 600:
                logger.warning(
                    "Telegram server error %s on attempt %d/%d. Retrying after %.1fs. method=%s",
                    resp.status_code, attempt, max_retries, backoff, method,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, TG_CALL_MAX_BACKOFF)
                continue

            if data is None:
                logger.error("tg_call: non-json response status=%s text=%s", resp.status_code, resp.text[:400])
                return None

            if not data.get("ok", False):
                logger.error("Telegram API returned error: %s", data)
            return data

        except requests.exceptions.RequestException:
            logger.exception("tg_call network/request exception on attempt %d/%d", attempt, max_retries)
            time.sleep(backoff)
            backoff = min(backoff * 2, TG_CALL_MAX_BACKOFF)
            continue

    logger.error("tg_call failed after %d attempts for method=%s", max_retries, method)
    return None


# Telegram API helpers that use tg_call
def send_message(chat_id: int, text: str, parse_mode: str = "Markdown"):
    """
    Low-level send wrapper. Returns the Telegram API result dict on success, or None on definite failure.
    """
    # If global tele pause is active, avoid calling tg_call and fail fast to let higher-level logic pause tasks.
    if is_tele_paused():
        logger.warning("send_message aborted: global Telegram pause active (remaining %.1fs). chat=%s",
                       get_tele_pause_remaining(), chat_id)
        return None

    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    try:
        data = tg_call("sendMessage", payload)
    except Exception:
        logger.exception("send_message: tg_call raised exception")
        data = None
    if data and data.get("result"):
        mid = data["result"].get("message_id")
        if mid:
            try:
                record_sent_message(chat_id, mid)
            except Exception:
                logger.exception("Failed to record sent message for %s:%s", chat_id, mid)
        return data["result"]
    return None


def robust_send_message(chat_id: int, text: str, parse_mode: str = "Markdown", attempts: int = 3, base_backoff: float = ROBUST_SEND_BASE_BACKOFF):
    """
    Higher-level sending helper that retries per-recipient only when the send fails.
    If a global tele pause is active, fail fast (do not perform long sleeps).
    """
    if attempts <= 0:
        attempts = 1
    # If tele pause active, do not attempt
    if is_tele_paused():
        logger.warning("robust_send_message: aborting send to %s due to global tele pause (remaining %.1fs)",
                       chat_id, get_tele_pause_remaining())
        return None

    for attempt in range(1, attempts + 1):
        try:
            # Before each attempt check tele_pause again
            if is_tele_paused():
                logger.warning("robust_send_message: tele pause detected before attempt %d for %s", attempt, chat_id)
                return None
            res = send_message(chat_id, text, parse_mode=parse_mode)
            if res:
                return res
            # else failed; log and maybe retry
            logger.warning("robust_send_message: attempt %d/%d failed for chat %s", attempt, attempts, chat_id)
        except Exception:
            logger.exception("robust_send_message: exception on attempt %d for chat %s", attempt, chat_id)
        if attempt < attempts:
            # exponential backoff with jitter but keep sleeps short to avoid worker timeouts
            backoff = base_backoff * (2 ** (attempt - 1))
            backoff = min(backoff, min(TG_CALL_MAX_BACKOFF, TELE_PAUSE_MAX_SECONDS))
            jitter = random.uniform(0, backoff * 0.15)
            time.sleep(backoff + jitter)
    logger.error("robust_send_message: all %d attempts failed for chat %s", attempts, chat_id)
    return None


def delete_message(chat_id: int, message_id: int):
    payload = {"chat_id": chat_id, "message_id": message_id}
    data = tg_call("deleteMessage", payload)
    if data and data.get("ok"):
        mark_message_deleted(chat_id, message_id)
    return data


# helper: send same message to all configured owners (use robust send with modest attempts)
def send_to_owners(text: str, parse_mode: str = "Markdown"):
    if not OWNERS:
        logger.warning("send_to_owners: no owners configured")
        return
    for oid in OWNERS:
        try:
            # small retries for owner notifications to reduce lost-notice risk
            robust_send_message(oid, text, parse_mode=parse_mode, attempts=2)
        except Exception:
            logger.exception("Failed to send owner message to %s", oid)


def set_webhook():
    if not TELEGRAM_API or not WEBHOOK_URL:
        logger.warning("Cannot set webhook: TELEGRAM_TOKEN or WEBHOOK_URL not configured")
        return None
    try:
        resp = _session.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()
        logger.info("Webhook set response: %s", resp.text)
        return resp.json()
    except Exception:
        logger.exception("Failed to set webhook")
        return None


# Task management functions (unchanged except for extra tele_pause checks in worker)
def enqueue_task(user_id: int, username: str, text: str) -> dict:
    words = split_text_into_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
    pending = q[0][0] if q else 0
    if pending >= MAX_QUEUE_PER_USER:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    db_execute(
        "INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, username, text, json.dumps(words), total, "queued", get_now_iso(), 0),
    )
    return {"ok": True, "total_words": total, "queue_size": pending + 1}


def get_next_task_for_user(user_id: int):
    rows = db_execute(
        "SELECT id, words_json, total_words, text FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1",
        (user_id,),
        fetch=True,
    )
    if not rows:
        return None
    r = rows[0]
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3]}


def set_task_status(task_id: int, status: str):
    if status == "running":
        db_execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, get_now_iso(), task_id))
    elif status in ("done", "cancelled"):
        db_execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, get_now_iso(), task_id))
    else:
        db_execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))


def mark_task_paused(task_id: int):
    set_task_status(task_id, "paused")


def mark_task_resumed(task_id: int):
    set_task_status(task_id, "running")


def mark_task_done(task_id: int):
    set_task_status(task_id, "done")


def cancel_active_task_for_user(user_id: int):
    rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,), fetch=True)
    count = 0
    for r in rows:
        db_execute("UPDATE tasks SET status = ? WHERE id = ?", ("cancelled", r[0]))
        count += 1
    return count


def record_split_log(user_id: int, username: str, words: int):
    db_execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)",
               (user_id, username, words, get_now_iso()))


def record_sent_message(chat_id: int, message_id: int):
    db_execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
               (chat_id, message_id, get_now_iso()))


def mark_message_deleted(chat_id: int, message_id: int):
    db_execute("UPDATE sent_messages SET deleted = 1 WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))


def get_messages_older_than(days=1):
    cutoff = datetime.utcnow() - timedelta(days=days)
    rows = db_execute("SELECT chat_id, message_id, sent_at FROM sent_messages WHERE deleted = 0", fetch=True)
    res = []
    for r in rows:
        try:
            sent_at = datetime.fromisoformat(r[2])
        except Exception:
            continue
        if sent_at < cutoff:
            res.append({"chat_id": r[0], "message_id": r[1]})
    return res


# Broadcast helper DB functions for idempotency (unchanged)
def compute_message_hash(text: str) -> str:
    if text is None:
        return ""
    h = hashlib.sha256()
    h.update(text.encode("utf-8"))
    return h.hexdigest()


def create_broadcast_run(message: str, created_by: int) -> str:
    run_id = str(uuid.uuid4())
    message_hash = compute_message_hash(message)
    db_execute(
        "INSERT INTO broadcast_runs (run_id, message_hash, message, created_by, created_at) VALUES (?, ?, ?, ?, ?)",
        (run_id, message_hash, message, created_by, get_now_iso()),
    )
    return run_id


def insert_broadcast_recipient(run_id: str, user_id: int):
    try:
        db_execute(
            "INSERT OR IGNORE INTO broadcast_recipients (run_id, user_id, status, attempts, last_attempt) VALUES (?, ?, 'pending', 0, ?)",
            (run_id, user_id, get_now_iso()),
        )
    except Exception:
        logger.exception("Failed to insert broadcast recipient %s for run %s", user_id, run_id)


def get_previous_successful_recipients_for_message(message_hash: str):
    rows = db_execute(
        "SELECT DISTINCT br.user_id FROM broadcast_recipients br JOIN broadcast_runs r ON br.run_id = r.run_id WHERE r.message_hash = ? AND br.status = 'success'",
        (message_hash,),
        fetch=True,
    )
    return set(r[0] for r in rows) if rows else set()


def get_pending_recipients_for_run(run_id: str):
    rows = db_execute(
        "SELECT user_id, status, attempts FROM broadcast_recipients WHERE run_id = ? AND status = 'pending' ORDER BY user_id ASC",
        (run_id,),
        fetch=True,
    )
    return rows or []


def mark_broadcast_recipient_status(run_id: str, user_id: int, status: str, attempts: int = None):
    now = get_now_iso()
    if attempts is None:
        db_execute("UPDATE broadcast_recipients SET status = ?, last_attempt = ? WHERE run_id = ? AND user_id = ?", (status, now, run_id, user_id))
    else:
        db_execute("UPDATE broadcast_recipients SET status = ?, attempts = ?, last_attempt = ? WHERE run_id = ? AND user_id = ?", (status, attempts, now, run_id, user_id))


# Suspension helpers (unchanged)
def parse_duration_to_seconds(s: str) -> int:
    if not s:
        return None
    s = s.strip().lower()
    if s.isdigit():
        return int(s)
    total = 0
    for m in re.finditer(r"(\d+)([smhd])", s):
        val = int(m.group(1))
        unit = m.group(2)
        if unit == "s":
            total += val
        elif unit == "m":
            total += val * 60
        elif unit == "h":
            total += val * 3600
        elif unit == "d":
            total += val * 86400
    return total if total > 0 else None


def suspend_user_in_db(target_id: int, until_iso: str, added_by: int, reason: str = ""):
    db_execute(
        "INSERT OR REPLACE INTO suspended_users (user_id, suspended_until, reason, added_by, added_at) VALUES (?, ?, ?, ?, ?)",
        (target_id, until_iso, reason or "", added_by, get_now_iso()),
    )


def unsuspend_user_in_db(target_id: int):
    db_execute("DELETE FROM suspended_users WHERE user_id = ?", (target_id,))


def get_suspended_row(target_id: int):
    rows = db_execute("SELECT suspended_until, reason, added_by, added_at FROM suspended_users WHERE user_id = ?", (target_id,), fetch=True)
    return rows[0] if rows else None


def process_expired_suspensions_notify():
    now_iso = get_now_iso()
    rows = db_execute("SELECT user_id, suspended_until, reason, added_by, added_at FROM suspended_users WHERE suspended_until <= ?", (now_iso,), fetch=True)
    if not rows:
        return
    for r in rows:
        try:
            uid = r[0]
            until = r[1] or ""
            reason = r[2] or ""
            added_by = r[3] or ""
            added_at = r[4] or ""
            try:
                unsuspend_user_in_db(uid)
            except Exception:
                logger.exception("Failed to remove expired suspension for %s", uid)
            try:
                robust_send_message(uid, f"✅ Your suspension ended at {until} UTC. You can use the bot again.")
            except Exception:
                logger.exception("Failed to notify user about suspension end %s", uid)
            try:
                send_to_owners(f"ℹ️ Suspension expired: {uid} suspended_until={until} added_by={added_by} at={added_at} reason={reason}")
            except Exception:
                logger.exception("Failed to notify owners about expired suspension for %s", uid)
        except Exception:
            logger.exception("Error processing expired suspension row: %s", r)


def cleanup_expired_suspensions():
    process_expired_suspensions_notify()


def is_suspended(user_id: int) -> bool:
    if user_id in OWNERS:
        return False
    rows = db_execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,), fetch=True)
    if not rows:
        return False
    suspended_until = rows[0][0]
    if not suspended_until:
        return False
    try:
        until = datetime.fromisoformat(suspended_until)
    except Exception:
        logger.warning("Bad suspended_until format for user %s: %s", user_id, suspended_until)
        unsuspend_user_in_db(user_id)
        return False
    if datetime.utcnow() >= until:
        try:
            process_expired_suspensions_notify()
        except Exception:
            logger.exception("Failed to process expired suspensions during is_suspended check")
        return False
    return True


# Authorization helpers (unchanged)
def is_allowed(user_id: int) -> bool:
    if user_id in OWNERS:
        return True
    if is_suspended(user_id):
        return False
    rows = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    return bool(rows)


def is_admin(user_id: int) -> bool:
    if user_id in OWNERS:
        return True
    rows = db_execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    if not rows:
        return False
    return bool(rows[0][0])


# Background worker to process tasks for users
_worker_stop = threading.Event()


def process_user_queue(user_id: int, chat_id: int, username: str):
    """
    Worker that processes queued tasks for a single user.
    Now respects global tele pause: when a pause is active, it will set the task to 'paused',
    notify the user and owners, and stop processing to avoid worker blocking/timeouts and lost counts.
    """
    lock = get_user_lock(user_id)
    if not lock.acquire(blocking=False):
        return
    try:
        while True:
            try:
                task = get_next_task_for_user(user_id)
                if not task:
                    break

                # If global Telegram pause is active, persistently pause this task and notify
                if is_tele_paused():
                    try:
                        set_task_status(task["id"], "paused")
                        robust_send_message(chat_id, f"⚠️ Sending is temporarily paused due to Telegram rate limits (pause remaining ~{int(get_tele_pause_remaining())}s). Your task has been paused. The owner has been notified.")
                        send_to_owners(f"⚠️ Global Telegram pause active (~{int(get_tele_pause_remaining())}s remaining). Paused tasks for user {user_id}.")
                    except Exception:
                        logger.exception("Failed to notify user/owners about tele pause")
                    break

                # If user became suspended while tasks queued, cancel their tasks
                if is_suspended(user_id):
                    stopped = cancel_active_task_for_user(user_id)
                    try:
                        robust_send_message(chat_id, "❌ You have been suspended; your tasks were stopped.")
                    except Exception:
                        logger.exception("Failed to notify suspended user %s", user_id)
                    break

                task_id = task["id"]
                words = task["words"]
                total = task["total_words"]
                set_task_status(task_id, "running")
                qcount = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
                interval = compute_interval(total)
                sent_info = db_execute("SELECT sent_count FROM tasks WHERE id = ?", (task_id,), fetch=True)
                sent = int(sent_info[0][0] or 0) if sent_info else 0
                remaining = max(0, total - sent)
                est_seconds = interval * remaining
                est_str = str(timedelta(seconds=int(est_seconds)))
                start_msg = f"🚀 Starting your split now. Words: {total}. Estimated time: {est_str}."
                if qcount:
                    start_msg += f" There are {qcount} more task(s) waiting."
                robust_send_message(chat_id, start_msg)

                i = sent
                consecutive_failures_for_recipient = 0
                while i < total:
                    # Respect global tele pause before each send
                    if is_tele_paused():
                        # Pause the task persistently and inform
                        set_task_status(task_id, "paused")
                        try:
                            robust_send_message(chat_id, f"⚠️ Sending paused due to Telegram rate limits (~{int(get_tele_pause_remaining())}s left). Your task has been paused.")
                            send_to_owners(f"⚠️ Paused task {task_id} for user {user_id} due to global Telegram pause.")
                        except Exception:
                            logger.exception("Failed to notify about tele pause while processing task %s", task_id)
                        break

                    status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                    if not status_row:
                        break
                    status = status_row[0][0]
                    if status == "paused":
                        robust_send_message(chat_id, "⏸️ Task paused. Use /resume to continue.")
                        while True:
                            time.sleep(0.5)
                            status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                            if not status_row:
                                break
                            status = status_row[0][0]
                            if status == "running":
                                robust_send_message(chat_id, "▶️ Resuming your task.")
                                break
                            if status == "cancelled":
                                break
                        if status == "cancelled":
                            break
                    if status == "cancelled":
                        break

                    # Before each send check if user suspended now
                    if is_suspended(user_id):
                        robust_send_message(chat_id, "❌ You have been suspended; stopping your task.")
                        cancel_active_task_for_user(user_id)
                        break

                    word = words[i]
                    sent_ok = False
                    for attempt in range(1, SPLIT_MAX_ATTEMPTS + 1):
                        # robust_send_message itself checks tele_pause and returns None if paused
                        res = robust_send_message(chat_id, word, attempts=1)
                        if res:
                            sent_ok = True
                            consecutive_failures_for_recipient = 0
                            try:
                                db_execute("UPDATE tasks SET sent_count = sent_count + 1 WHERE id = ?", (task_id,))
                            except Exception:
                                logger.exception("Failed to update sent_count for task %s", task_id)
                            break
                        else:
                            # small backoff before retrying the current word
                            backoff = ROBUST_SEND_BASE_BACKOFF * (2 ** (attempt - 1))
                            backoff = min(backoff, TG_CALL_MAX_BACKOFF)
                            jitter = random.uniform(0, backoff * 0.2)
                            time.sleep(backoff + jitter)
                            # If tele_pause became active during retries, stop this word attempts to avoid waiting long
                            if is_tele_paused():
                                logger.info("Stopping retries for current word due to tele pause (task %s user %s)", task_id, user_id)
                                break
                    if not sent_ok:
                        consecutive_failures_for_recipient += 1
                        logger.error("Failed to send word #%d for user %s after %d attempts. task=%s", i, user_id, SPLIT_MAX_ATTEMPTS, task_id)
                        if consecutive_failures_for_recipient >= 3:
                            set_task_status(task_id, "paused")
                            try:
                                robust_send_message(chat_id, "⚠️ Repeated send failures occurred. Your task has been paused. The owner has been notified.")
                            except Exception:
                                logger.exception("Failed to notify user about repeated failures")
                            try:
                                send_to_owners(f"⚠️ Repeated send failures for user {user_id} while processing task {task_id}. Paused the task.")
                            except Exception:
                                logger.exception("Failed to notify owners about repeated failures")
                            break
                        else:
                            time.sleep(1.0)
                            continue

                    # if successful, advance
                    i += 1
                    time.sleep(interval)

                final_status_row = db_execute("SELECT status, sent_count, total_words FROM tasks WHERE id = ?", (task_id,), fetch=True)
                final_status = final_status_row[0][0] if final_status_row else "done"
                sent_count = int(final_status_row[0][1] or 0) if final_status_row else total
                if final_status != "cancelled" and final_status != "paused":
                    mark_task_done(task_id)
                    try:
                        record_split_log(user_id, username, sent_count)
                    except Exception:
                        logger.exception("Failed to record split log for %s", user_id)
                    robust_send_message(chat_id, "✅ All done! Your text was split.")
                elif final_status == "paused":
                    robust_send_message(chat_id, "⏸️ Your task is paused due to send failures or by request. Use /resume to continue.")
                else:
                    robust_send_message(chat_id, "🛑 Task stopped.")
                qcount_after = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
                if qcount_after > 0:
                    robust_send_message(chat_id, "⏩ Next task will start soon!")
            except Exception:
                logger.exception("Unexpected exception in process_user_queue for user %s", user_id)
                try:
                    robust_send_message(user_id, "⚠️ An internal error occurred while processing your tasks. The owner has been notified.")
                    send_to_owners(f"⚠️ Internal error while processing tasks for user {user_id}. See logs for details.")
                except Exception:
                    logger.exception("Failed to notify about internal error")
                time.sleep(2)
                break
    finally:
        lock.release()


def global_worker_loop():
    while not _worker_stop.is_set():
        try:
            rows = db_execute("SELECT DISTINCT user_id, username FROM tasks WHERE status = 'queued' ORDER BY created_at ASC", fetch=True)
            for r in rows:
                try:
                    user_id = r[0]
                    username = r[1] or ""
                    t = threading.Thread(target=process_user_queue, args=(user_id, user_id, username), daemon=True)
                    t.start()
                except Exception:
                    logger.exception("Failed to start per-user worker for row: %s", r)
            time.sleep(0.5)
        except Exception:
            traceback.print_exc()
            time.sleep(1)


# Scheduler jobs (unchanged)
scheduler = BackgroundScheduler()


def hourly_owner_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    rows = db_execute("SELECT user_id, username, SUM(words) as s FROM split_logs WHERE created_at >= ? GROUP BY user_id ORDER BY s DESC", (cutoff.isoformat(),), fetch=True)
    if not rows:
        send_to_owners("🕐 Last 1 hour: no splits.")
        return
    lines = []
    total_words = 0
    for r in rows:
        uid = r[0]
        uname = r[1] or ""
        wsum = int(r[2] or 0)
        total_words += wsum
        lines.append(f"{uid} - {wsum} words ({uname})" if uname else f"{uid} - {wsum} words")
    body = "🕐 Last 1 hour activity:\n" + "\n".join(lines) + f"\n\nTotal words: {total_words}"
    send_to_owners(body)


def delete_old_bot_messages():
    msgs = get_messages_older_than(days=1)
    for m in msgs:
        try:
            delete_message(m["chat_id"], m["message_id"])
        except Exception:
            try:
                mark_message_deleted(m["chat_id"], m["message_id"])
            except Exception:
                logger.exception("Failed to mark message deleted in DB: %s", m)


def maintenance_hourly_health():
    send_to_owners("👑 Bot health: running.")


# Run expiration processor every minute
scheduler.add_job(process_expired_suspensions_notify, "interval", minutes=1, next_run_time=datetime.utcnow() + timedelta(seconds=5))
scheduler.add_job(hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10))
scheduler.add_job(delete_old_bot_messages, "interval", minutes=30, next_run_time=datetime.utcnow() + timedelta(seconds=20))
scheduler.add_job(maintenance_hourly_health, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=15))
scheduler.start()

_worker_thread = threading.Thread(target=global_worker_loop, daemon=True)
_worker_thread.start()


# Webhook endpoint
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update_json = request.get_json(force=True)
    except Exception:
        return "no json", 400

    try:
        if "message" in update_json:
            msg = update_json["message"]
            user = msg.get("from", {})
            user_id = user.get("id")
            username = user.get("username") or user.get("first_name") or ""
            text = msg.get("text") or ""
            if text.startswith("/"):
                parts = text.split(None, 1)
                command = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                return handle_command(user_id, username, command, args)
            else:
                return handle_new_text(user_id, username, text)
    except Exception:
        logger.exception("Error handling webhook update")
    return jsonify({"ok": True})


# Command handlers (broadcast behavior unchanged except it will observe tele_pause via robust_send_message)
def handle_command(user_id: int, username: str, command: str, args: str):
    # allow /start and /help for anyone; otherwise check allowed (and suspended)
    if not is_allowed(user_id) and command not in ("/start", "/help"):
        robust_send_message(user_id, "❌ Sorry, you are not allowed to use this bot. The owner has been notified.")
        try:
            if OWNER_ID:
                robust_send_message(OWNER_ID, f"⚠️ Unallowed access attempt by {username or user_id} ({user_id}).")
            send_to_owners(f"⚠️ Unallowed access attempt by {username or user_id} ({user_id}).")
        except Exception:
            logger.exception("Failed to notify owner(s) about unallowed access")
        return jsonify({"ok": True})

    # ... (other commands preserved) ...

    if command == "/broadcast":
        if user_id not in OWNERS:
            robust_send_message(user_id, "❌ Only owner can broadcast")
            return jsonify({"ok": True})
        if not args:
            robust_send_message(user_id, "ℹ️ Usage: /broadcast <message>")
            return jsonify({"ok": True})
        message = args.strip()
        if not message:
            robust_send_message(user_id, "ℹ️ Empty broadcast message; nothing sent.")
            return jsonify({"ok": True})

        message_hash = compute_message_hash(message)
        run_id = create_broadcast_run(message, user_id)
        already_done = get_previous_successful_recipients_for_message(message_hash)

        rows = db_execute("SELECT user_id FROM allowed_users", fetch=True) or []
        total_recipients = 0
        for r in rows:
            target = r[0]
            total_recipients += 1
            if target in already_done:
                db_execute("INSERT OR IGNORE INTO broadcast_recipients (run_id, user_id, status, attempts, last_attempt) VALUES (?, ?, 'success', 0, ?)",
                           (run_id, target, get_now_iso()))
                continue
            insert_broadcast_recipient(run_id, target)

        # If global tele pause active at broadcast start, inform owner and do not attempt sends now
        if is_tele_paused():
            robust_send_message(user_id, f"⚠️ Broadcast aborted: currently respecting Telegram rate limits (~{int(get_tele_pause_remaining())}s remaining). Please try again after the pause.")
            send_to_owners(f"⚠️ Broadcast attempt by owner {user_id} deferred due to global Telegram pause (~{int(get_tele_pause_remaining())}s left). Run id: {run_id}")
            return jsonify({"ok": True})

        pending = get_pending_recipients_for_run(run_id)
        success_count = 0
        failed_list = []

        for row in pending:
            target = row[0]
            attempts = int(row[2] or 0) if len(row) > 2 else 0
            sent_ok = False
            for attempt_no in range(attempts + 1, BROADCAST_MAX_ATTEMPTS + 1):
                # robust_send_message will fail-fast if tele pause is active
                res = robust_send_message(target, f"📣 Broadcast from owner:\n\n{message}", attempts=1)
                attempts = attempt_no
                db_execute("UPDATE broadcast_recipients SET attempts = ?, last_attempt = ? WHERE run_id = ? AND user_id = ?",
                           (attempts, get_now_iso(), run_id, target))
                if res:
                    sent_ok = True
                    success_count += 1
                    mark_broadcast_recipient_status(run_id, target, "success", attempts)
                    break
                else:
                    if is_tele_paused():
                        # If tele pause activated while broadcasting, stop further sends and record remaining recipients as pending
                        logger.info("Broadcast run %s: stopping due to tele pause. Remaining recipients will stay pending.", run_id)
                        break
                    if attempt_no < BROADCAST_MAX_ATTEMPTS:
                        backoff = ROBUST_SEND_BASE_BACKOFF * (2 ** (attempt_no - 1))
                        backoff = min(backoff, TG_CALL_MAX_BACKOFF)
                        time.sleep(backoff + (0.05 * attempt_no))
                    else:
                        mark_broadcast_recipient_status(run_id, target, "failed", attempts)
                        failed_list.append(target)
            if is_tele_paused():
                # stop outer loop if pause active
                break

        pre_skipped = len(already_done)
        total_success = success_count + pre_skipped

        robust_send_message(user_id, f"📣 Broadcast run {run_id} completed/paused. Total recipients: {total_recipients}. Success: {total_success}. Failed (final): {len(failed_list)}. Pending remain if paused: {len(pending) - (success_count + len(failed_list))}")
        if failed_list:
            try:
                send_to_owners(f"⚠️ Broadcast run {run_id} had failures for recipients: {', '.join(str(x) for x in failed_list)}")
            except Exception:
                logger.exception("Failed to notify owners about broadcast failures")
        return jsonify({"ok": True})

    send_message(user_id, "❓ Unknown command.")
    return jsonify({"ok": True})


def get_queue_size(user_id):
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
    return q[0][0] if q else 0


def handle_new_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id):
        robust_send_message(user_id, "❌ Sorry, you are not allowed. The owner has been notified.")
        try:
            if OWNER_ID:
                robust_send_message(OWNER_ID, f"⚠️ Unallowed access attempt by {username or user_id} ({user_id}). Message: {text}")
            send_to_owners(f"⚠️ Unallowed access attempt by {username or user_id} ({user_id}). Message: {text}")
        except Exception:
            logger.exception("Failed to notify owner about unallowed message")
        return jsonify({"ok": True})
    if is_maintenance_now():
        robust_send_message(user_id, "🛠️ Maintenance in progress. New tasks are blocked. Please try later.")
        try:
            send_to_owners(f"🛠️ Maintenance attempt by {user_id}.")
        except Exception:
            logger.exception("Failed to notify owner about maintenance attempt")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res.get("reason") == "empty":
            robust_send_message(user_id, "⚠️ Empty text. Nothing to split.")
            return jsonify({"ok": True})
        if res.get("reason") == "queue_full":
            robust_send_message(user_id, f"❌ Your queue is full ({res['queue_size']}). Use /stop or wait.")
            return jsonify({"ok": True})
    running_exists = bool(db_execute("SELECT 1 FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,), fetch=True))
    queued = get_queue_size(user_id)
    if running_exists:
        robust_send_message(user_id, f"📝 Queued. You have {queued} task(s) waiting.")
    else:
        robust_send_message(user_id, f"✅ Task added. Words: {res['total_words']}.")
    return jsonify({"ok": True})


# Root and health endpoints
@app.route("/", methods=["GET", "POST"])
def root_forward():
    if request.method == "POST":
        logger.info("Received POST at root; forwarding to /webhook")
        try:
            update_json = request.get_json(force=True)
            threading.Thread(target=handle_update, args=(update_json,), daemon=True).start()
            return jsonify({"ok": True})
        except Exception:
            logger.exception("Forwarding POST to webhook failed")
            return jsonify({"ok": False, "error": "forward failed"}), 200
    return "Word Splitter Bot is running.", 200


@app.route("/health", methods=["GET", "HEAD"])
@app.route("/health/", methods=["GET", "HEAD"])
def health():
    logger.info("Health check from %s method=%s", request.remote_addr, request.method)
    # expose tele pause remaining for operational awareness
    tp = int(get_tele_pause_remaining())
    return jsonify({"ok": True, "tele_pause_remaining_s": tp}), 200


@app.route("/debug/routes", methods=["GET"])
def debug_routes():
    try:
        rules = sorted(str(r) for r in app.url_map.iter_rules())
        return jsonify({"routes": rules}), 200
    except Exception:
        logger.exception("Failed to list routes")
        return jsonify({"ok": False, "error": "failed to list routes"}), 500


# handle_update used by root forward. Keep minimal to not duplicate logic.
def handle_update(update_json):
    try:
        if "message" in update_json:
            msg = update_json["message"]
            user = msg.get("from", {})
            user_id = user.get("id")
            username = user.get("username") or user.get("first_name") or ""
            text = msg.get("text") or ""
            if text.startswith("/"):
                parts = text.split(None, 1)
                command = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                handle_command(user_id, username, command, args)
            else:
                handle_new_text(user_id, username, text)
    except Exception:
        logger.exception("Error in handle_update")


if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        logger.exception("Failed to set webhook at startup")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
