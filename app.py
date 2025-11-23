#!/usr/bin/env python3
"""
Telegram Word-Splitter Bot (Webhook-ready) - app.py (final updated)

Key fixes in this version:
- Broadcast uses a one-shot low-level sender (no retries) to avoid duplicate deliveries.
- /suspend and /unsuspend always respond, validate usage, give examples, and enforce suspensions immediately.
- Heavily emojified user-facing messages and friendlier wording.
- Keeps previous improvements (ALLOWED_USERS, OWNER_IDS, suspended_users table, etc).
"""

import os
import time
import json
import sqlite3
import threading
import traceback
import logging
import re
import random
from datetime import datetime, timedelta
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests

# -------------------------
# Basic logger
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# -------------------------
# Flask app
# -------------------------
app = Flask(__name__)

# -------------------------
# Environment / Config
# -------------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
OWNER_IDS_RAW = os.environ.get("OWNER_IDS", "")
ALLOWED_USERS_RAW = os.environ.get("ALLOWED_USERS", "")
OWNER_USERNAMES_RAW = os.environ.get("OWNER_USERNAMES", "")
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
MAX_ALLOWED_USERS = int(os.environ.get("MAX_ALLOWED_USERS", "500"))
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "50"))
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))

# Rate-limiter / token bucket
_raw_max_msg_per_second = float(os.environ.get("MAX_MSG_PER_SECOND", "50"))
MAX_MSG_PER_SECOND = max(1.0, _raw_max_msg_per_second)

if not TELEGRAM_TOKEN:
    logger.warning("TELEGRAM_TOKEN not set - outgoing messages will fail.")
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None
_request_session = requests.Session()

# -------------------------
# Helpers - parsing env lists
# -------------------------
def parse_id_list(raw: str) -> List[int]:
    if not raw:
        return []
    parts = re.split(r"[,\s]+", raw.strip())
    ids = []
    for p in parts:
        if not p:
            continue
        try:
            ids.append(int(p))
        except Exception:
            # ignore non-numeric tokens
            continue
    return ids

OWNER_IDS = parse_id_list(OWNER_IDS_RAW)
OWNER_USERNAMES = [s for s in (OWNER_USERNAMES_RAW.split(",") if OWNER_USERNAMES_RAW else []) if s]
PRIMARY_OWNER = OWNER_IDS[0] if OWNER_IDS else None

def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def owners_str() -> str:
    if not OWNER_IDS:
        return "(none)"
    return ", ".join(str(x) for x in OWNER_IDS)

# -------------------------
# Database helpers
# -------------------------
_db_lock = threading.Lock()

def init_db():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_at TEXT,
                is_admin INTEGER DEFAULT 0
            )
        """)
        c.execute("""
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
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS split_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                words INTEGER,
                created_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sent_at TEXT,
                deleted INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS suspended_users (
                user_id INTEGER PRIMARY KEY,
                suspended_until TEXT,
                reason TEXT,
                added_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS send_failures (
                user_id INTEGER PRIMARY KEY,
                failures INTEGER,
                last_failure_at TEXT
            )
        """)
        conn.commit()

init_db()

def db_execute(query: str, params: tuple = (), fetch: bool = False):
    attempt = 0
    while True:
        try:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute(query, params)
                if fetch:
                    return c.fetchall()
                conn.commit()
                return None
        except sqlite3.OperationalError as e:
            attempt += 1
            if attempt > 6:
                logger.exception("DB OperationalError giving up: %s ; query=%s params=%s", e, query, params)
                raise
            time.sleep(0.05 * attempt)
        except Exception:
            logger.exception("DB error: %s ; query=%s params=%s", traceback.format_exc(), query, params)
            raise

# Ensure owners present in allowed_users as admin
for idx, oid in enumerate(OWNER_IDS):
    uname = OWNER_USERNAMES[idx] if idx < len(OWNER_USERNAMES) else ""
    try:
        exists = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (oid,), fetch=True)
        if not exists:
            db_execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)", (oid, uname, now_ts(), 1))
        else:
            db_execute("UPDATE allowed_users SET is_admin = 1 WHERE user_id = ?", (oid,))
    except Exception:
        logger.exception("Error ensuring owner in allowed_users")

# Auto-add ALLOWED_USERS env var
for uid in parse_id_list(ALLOWED_USERS_RAW):
    try:
        exists = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (uid,), fetch=True)
        if not exists:
            db_execute("INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)", (uid, "", now_ts(), 0))
            # best-effort notification (non-critical)
            if TELEGRAM_API_BASE:
                try:
                    _request_session.post(f"{TELEGRAM_API_BASE}/sendMessage", json={"chat_id": uid, "text": "✨ You were added via ALLOWED_USERS. Hello! Send any text to start splitting."}, timeout=3)
                except Exception:
                    pass
    except Exception:
        logger.exception("Auto-add ALLOWED_USERS error for %s", uid)

# -------------------------
# Token bucket rate limiter
# -------------------------
_token_bucket = {
    "tokens": MAX_MSG_PER_SECOND,
    "last": time.time(),
    "capacity": max(1.0, MAX_MSG_PER_SECOND),
    "lock": threading.Lock()
}

def _acquire_token(timeout: float = 10.0) -> bool:
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
        if time.time() - start >= timeout:
            return False
        time.sleep(0.01)

# -------------------------
# Low-level senders
# -------------------------
def _parse_telegram_response(resp):
    try:
        data = resp.json()
    except Exception:
        return False, f"non-json response: {resp.text[:300]}"
    ok = data.get("ok", False)
    return ok, data

def send_message(chat_id: int, text: str, parse_mode: str = "Markdown"):
    """
    Normal send function used across the bot. Uses token bucket + lightweight retry/backoff for transient errors.
    Keeps a send_failures counter for repeated failures.
    """
    if not TELEGRAM_API_BASE:
        logger.error("send_message attempted but TELEGRAM token missing.")
        return None

    # rate limit
    if not _acquire_token(timeout=5.0):
        logger.warning("send_message: rate token acquisition failed for chat %s", chat_id)

    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    try:
        resp = _request_session.post(f"{TELEGRAM_API_BASE}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception:
        logger.exception("Network error sending message to %s", chat_id)
        _increment_failure(chat_id)
        return None

    ok, data = _parse_telegram_response(resp)
    if ok:
        try:
            msg_id = data.get("result", {}).get("message_id")
            if msg_id:
                db_execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)", (chat_id, msg_id, now_ts()))
        except Exception:
            logger.exception("Failed recording sent message")
        _reset_failures(chat_id)
        return data.get("result")
    else:
        # log and increment failure; we do not retry indefinitely here
        logger.info("send_message failed for %s: %s", chat_id, data)
        _increment_failure(chat_id)
        return None

def _increment_failure(user_id: int):
    try:
        rows = db_execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,), fetch=True)
        if not rows:
            db_execute("INSERT INTO send_failures (user_id, failures, last_failure_at) VALUES (?, ?, ?)", (user_id, 1, now_ts()))
            failures = 1
        else:
            failures = int(rows[0][0] or 0) + 1
            db_execute("UPDATE send_failures SET failures = ?, last_failure_at = ? WHERE user_id = ?", (failures, now_ts(), user_id))

        if failures >= 6:
            # notify owners and cancel user's tasks
            _notify_all_owners(f"⚠️ Repeated send failures for user {user_id} ({failures} consecutive). Stopping their queue and alerting owner.")
            cancel_active_task_for_user(user_id)
    except Exception:
        logger.exception("Error updating send_failures for %s", user_id)

def _reset_failures(user_id: int):
    try:
        db_execute("DELETE FROM send_failures WHERE user_id = ?", (user_id,))
    except Exception:
        pass

# -------------------------
# Broadcast: NO RETRY low-level sender
# -------------------------
def broadcast_send_raw(chat_id: int, text: str):
    """
    One-shot send for broadcast. This will NOT retry automatically.
    It uses one HTTP attempt and returns True/False.
    This prevents double-sends when we attempted a retry that succeeded later.
    """
    if not TELEGRAM_API_BASE:
        logger.error("broadcast_send_raw called without TELEGRAM token.")
        return False, "no_token"

    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True}
    try:
        resp = _request_session.post(f"{TELEGRAM_API_BASE}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception as e:
        logger.info("broadcast_send_raw network error for %s: %s", chat_id, e)
        return False, f"network_error:{e}"

    ok, data = _parse_telegram_response(resp)
    if ok:
        # record sent message for cleanup if needed
        try:
            msg_id = data.get("result", {}).get("message_id")
            if msg_id:
                db_execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)", (chat_id, msg_id, now_ts()))
        except Exception:
            pass
        return True, "ok"
    else:
        # no retry, just return failure string
        reason = data.get("description") if isinstance(data, dict) else str(data)
        logger.info("broadcast_send_raw failure for %s: %s", chat_id, reason)
        return False, reason

# -------------------------
# Basic task / queue operations
# -------------------------
def split_text_to_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

def enqueue_task(user_id: int, username: str, text: str):
    words = split_text_to_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    pending_rows = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
    pending = pending_rows[0][0] if pending_rows else 0
    if pending >= MAX_QUEUE_PER_USER:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    db_execute("INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
               (user_id, username, text, json.dumps(words), total, "queued", now_ts(), 0))
    return {"ok": True, "total_words": total, "queue_size": pending + 1}

def get_next_task_for_user(user_id: int):
    rows = db_execute("SELECT id, words_json, total_words, text FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1", (user_id,), fetch=True)
    if not rows:
        return None
    r = rows[0]
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3]}

def set_task_status(task_id: int, status: str):
    if status == "running":
        db_execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, now_ts(), task_id))
    elif status in ("done", "cancelled"):
        db_execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, now_ts(), task_id))
    else:
        db_execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))

def cancel_active_task_for_user(user_id: int):
    rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,), fetch=True) or []
    count = 0
    for r in rows:
        try:
            db_execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", ("cancelled", now_ts(), r[0]))
            count += 1
        except Exception:
            logger.exception("Error cancelling task %s", r[0])
    return count

def record_split_log(user_id: int, username: str, words: int):
    db_execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)", (user_id, username, words, now_ts()))

# -------------------------
# Allowed / Admin / Suspended checks
# -------------------------
def is_allowed(user_id: int) -> bool:
    rows = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    return bool(rows)

def is_admin(user_id: int) -> bool:
    rows = db_execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    if not rows:
        return False
    return bool(rows[0][0])

def suspend_user(user_id: int, seconds: int, reason: str = ""):
    until = (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    db_execute("INSERT OR REPLACE INTO suspended_users (user_id, suspended_until, reason, added_at) VALUES (?, ?, ?, ?)", (user_id, until, reason, now_ts()))
    stopped = cancel_active_task_for_user(user_id)
    # notify user + owners
    try:
        send_message(user_id, f"⛔ You were suspended until *{until} UTC*.\nReason: {reason or 'No reason provided.'}\nIf you believe this is a mistake, contact the owner. 🙏")
    except Exception:
        logger.exception("Failed to notify suspended user %s", user_id)
    _notify_all_owners(f"⛔ User *{user_id}* suspended until *{until} UTC*.\nStopped {stopped} active task(s). Reason: {reason or '(none)'}")

def unsuspend_user(user_id: int) -> bool:
    rows = db_execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,), fetch=True)
    if not rows:
        return False
    db_execute("DELETE FROM suspended_users WHERE user_id = ?", (user_id,))
    try:
        send_message(user_id, "✅ Your suspension has been lifted. You may now use the bot again. Welcome back! 🎉")
    except Exception:
        logger.exception("Failed to notify unsuspended user %s", user_id)
    _notify_all_owners(f"✅ User *{user_id}* unsuspended by an admin/owner.")
    return True

def list_suspended():
    rows = db_execute("SELECT user_id, suspended_until, reason, added_at FROM suspended_users ORDER BY suspended_until ASC", fetch=True)
    return rows

def is_suspended(user_id: int) -> bool:
    rows = db_execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,), fetch=True)
    if not rows:
        return False
    try:
        until = datetime.strptime(rows[0][0], "%Y-%m-%d %H:%M:%S")
        return until > datetime.utcnow()
    except Exception:
        return False

def check_and_lift_suspensions():
    rows = db_execute("SELECT user_id, suspended_until FROM suspended_users", fetch=True) or []
    now = datetime.utcnow()
    for r in rows:
        try:
            until = datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S")
            if until <= now:
                uid = r[0]
                unsuspend_user(uid)
        except Exception:
            logger.exception("Invalid suspended_until: %s for user %s", r[1], r[0])

# -------------------------
# Worker loop to process queued tasks
# -------------------------
_user_locks = {}
_user_locks_lock = threading.Lock()

def get_user_lock(user_id: int):
    with _user_locks_lock:
        if user_id not in _user_locks:
            _user_locks[user_id] = threading.Lock()
        return _user_locks[user_id]

_worker_stop = threading.Event()

def process_user_queue(user_id: int, chat_id: int, username: str):
    lock = get_user_lock(user_id)
    if not lock.acquire(blocking=False):
        return
    try:
        if is_suspended(user_id):
            # friendly notify to user (they may be suspended)
            try:
                send_message(user_id, "⛔ You are suspended — your queued tasks will not run until suspension ends. ❗")
            except Exception:
                pass
            return

        while True:
            task = get_next_task_for_user(user_id)
            if not task:
                break
            # double-check suspension
            if is_suspended(user_id):
                send_message(user_id, "⛔ You are suspended. Your tasks were paused/cancelled.")
                break

            task_id = task["id"]
            words = task["words"]
            total = task["total_words"]
            set_task_status(task_id, "running")
            sent_info = db_execute("SELECT sent_count FROM tasks WHERE id = ?", (task_id,), fetch=True)
            sent = int(sent_info[0][0] or 0) if sent_info else 0
            remaining = max(0, total - sent)

            interval = 0.4 if total <= 150 else (0.5 if total <= 300 else 0.6)
            est_seconds = int(remaining * interval)
            est_str = str(timedelta(seconds=est_seconds))
            header = f"🚀 Starting your split — *{total}* words.\nEstimated time: *{est_str}*.\n✨ I'm on it!"
            send_message(chat_id, header)

            i = sent
            consecutive_failures = 0
            while i < total:
                # check cancellation/suspension
                row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                if not row:
                    break
                status = row[0][0]
                if status == "paused":
                    send_message(chat_id, "⏸️ Task paused. Use /resume to continue. 🌴")
                    # wait until resumed/cancelled
                    while True:
                        time.sleep(0.5)
                        row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                        if not row:
                            break
                        status = row[0][0]
                        if status == "running":
                            send_message(chat_id, "▶️ Resuming your task. Let's go! 💪")
                            break
                        if status == "cancelled":
                            break
                    if status == "cancelled":
                        break
                if status == "cancelled":
                    break

                res = send_message(chat_id, words[i])
                if res is None:
                    consecutive_failures += 1
                    logger.info("Send failure for user %s task %s index %s (consecutive=%s)", chat_id, task_id, i, consecutive_failures)
                    if consecutive_failures >= 4:
                        _notify_all_owners(f"⚠️ Repeated send failures while processing tasks for user *{user_id}*. Stopping user's tasks.")
                        cancel_active_task_for_user(user_id)
                        break
                else:
                    consecutive_failures = 0

                db_execute("UPDATE tasks SET sent_count = sent_count + 1 WHERE id = ?", (task_id,))
                i += 1
                time.sleep(interval)

            # finalize
            status_row = db_execute("SELECT status, sent_count, total_words FROM tasks WHERE id = ?", (task_id,), fetch=True)
            if status_row:
                final_status = status_row[0][0]
                sent_count = int(status_row[0][1] or 0)
                total_words = int(status_row[0][2] or 0)
            else:
                final_status = "done"
                sent_count = total
                total_words = total

            if final_status != "cancelled":
                set_task_status(task_id, "done")
                record_split_log(user_id, username, sent_count)
                send_message(chat_id, "✅ All done! Your text has been fully split. ✨")
            else:
                send_message(chat_id, "🛑 Task cancelled / stopped. If you need help, contact the owner. 🙏")
    finally:
        lock.release()

def global_worker():
    while not _worker_stop.is_set():
        try:
            rows = db_execute("SELECT DISTINCT user_id, username FROM tasks WHERE status = 'queued' ORDER BY created_at ASC", fetch=True) or []
            for r in rows:
                user_id = r[0]
                username = r[1] or ""
                if is_suspended(user_id):
                    continue
                t = threading.Thread(target=process_user_queue, args=(user_id, user_id, username), daemon=True)
                t.start()
            time.sleep(0.6)
        except Exception:
            logger.exception("global_worker error")
            time.sleep(1.0)

_worker_thread = threading.Thread(target=global_worker, daemon=True)
_worker_thread.start()

# -------------------------
# Scheduler: suspension lifting & housekeeping
# -------------------------
scheduler = BackgroundScheduler()
scheduler.add_job(check_and_lift_suspensions, "interval", minutes=1, next_run_time=datetime.utcnow() + timedelta(seconds=5))
scheduler.start()

# -------------------------
# Notifications to owners
# -------------------------
def _notify_all_owners(msg: str):
    for oid in OWNER_IDS:
        try:
            send_message(oid, f"👑 Owner notice:\n{msg}")
        except Exception:
            logger.exception("Failed notifying owner %s", oid)

# -------------------------
# Webhook handling
# -------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True)
        if not update:
            return jsonify({"ok": False, "reason": "no_json"}), 400
    except Exception:
        return jsonify({"ok": False, "reason": "invalid_json"}), 400

    # handle messages synchronously (safe, light)
    try:
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            user_id = user.get("id")
            username = user.get("username") or (user.get("first_name") or "")
            text = msg.get("text") or ""
            if text.startswith("/"):
                parts = text.split(None, 1)
                command = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                return handle_command(user_id, username, command, args)
            else:
                return handle_user_text(user_id, username, text)
    except Exception:
        logger.exception("Error processing update")
    return jsonify({"ok": True})

# Root and health endpoints
@app.route("/", methods=["GET", "POST"])
def root_handler():
    if request.method == "POST":
        try:
            update = request.get_json(force=True)
            threading.Thread(target=_async_handle_update, args=(update,), daemon=True).start()
            return jsonify({"ok": True})
        except Exception:
            logger.exception("Forwarding update failed")
            return jsonify({"ok": False}), 200
    return "Word Splitter Bot is running. ✨", 200

@app.route("/health", methods=["GET", "HEAD"])
def health():
    return jsonify({"ok": True, "time": now_ts()}), 200

def _async_handle_update(update):
    try:
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            user_id = user.get("id")
            username = user.get("username") or (user.get("first_name") or "")
            text = msg.get("text") or ""
            if text.startswith("/"):
                parts = text.split(None, 1)
                command = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                handle_command(user_id, username, command, args)
            else:
                handle_user_text(user_id, username, text)
    except Exception:
        logger.exception("Async update handler error")

# -------------------------
# Command handlers
# -------------------------
def handle_command(user_id: int, username: str, command: str, args: str):
    """
    Handles commands and always returns a Flask response (jsonified).
    Commands are heavily emojified and user-friendly.
    """
    # /start and /help allowed for everyone
    if command in ("/start", "/help"):
        send_message(user_id,
            f"👋 Hello *{username or user_id}*! Welcome to *WordSplitter* — I send one word per message. ✂️\n\n"
            "Just send any text and I'll split it for you. ✨\n\n"
            "Common commands:\n"
            "• /start /help — show this message\n"
            "• /example — run a tiny demo split\n"
            "• /pause — pause active split\n"
            "• /resume — resume paused split\n"
            "• /stop — stop active & clear queued tasks\n            "
            "• /status — show your current status\n\n"
            "If you need admin actions, ask an owner. 👑\n"
            f"Owner(s): {owners_str()}",
        )
        return jsonify({"ok": True})

    # Other commands require allowed users
    if not is_allowed(user_id):
        send_message(user_id, "❌ Sorry — you are not allowed to use this bot. I've informed the owner. 🙇")
        _notify_all_owners(f"⚠️ Unallowed access attempt by *{username or user_id}* ({user_id}).")
        return jsonify({"ok": True})

    # Admin-only commands (admins include owners)
    if command == "/example":
        sample = "This is a demo split!"
        res = enqueue_task(user_id, username, sample)
        if not res["ok"]:
            send_message(user_id, "Hmm, I couldn't enqueue a demo. Try again later. 🤖")
            return jsonify({"ok": True})
        send_message(user_id, f"✨ Demo added — will split *{res['total_words']}* words. Sit back and watch the magic! 🎩")
        return jsonify({"ok": True})

    if command == "/pause":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "ℹ️ You have no active task to pause. Try /status to see your tasks. 🤝")
            return jsonify({"ok": True})
        task_id = rows[0][0]
        set_task_status(task_id, "paused")
        send_message(user_id, "⏸️ Paused. Use /resume to continue. Take a break! ☕")
        return jsonify({"ok": True})

    if command == "/resume":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "ℹ️ You have no paused task to resume. Maybe try /status. 🔍")
            return jsonify({"ok": True})
        task_id = rows[0][0]
        set_task_status(task_id, "running")
        send_message(user_id, "▶️ Resumed — continuing where we left off. 💪")
        return jsonify({"ok": True})

    if command == "/status":
        if is_suspended(user_id):
            rows = db_execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,), fetch=True)
            until = rows[0][0] if rows else "unknown"
            send_message(user_id, f"⛔ You are suspended until *{until} UTC*. Contact the owner for more info.")
            return jsonify({"ok": True})
        active = db_execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        queued_count = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
        if active:
            aid, status, total_words, sent_count = active[0]
            remaining = int(total_words or 0) - int(sent_count or 0)
            send_message(user_id, f"📊 Status: *{status}*\nRemaining words: *{remaining}* ✨\nQueue size: *{queued_count}*")
        else:
            send_message(user_id, f"✅ You have no active split. Queue size: *{queued_count}* — send text to add one! 💬")
        return jsonify({"ok": True})

    if command == "/stop":
        queued_rows = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
        queued_count = queued_rows[0][0] if queued_rows else 0
        stopped = cancel_active_task_for_user(user_id)
        if stopped > 0 or queued_count > 0:
            send_message(user_id, f"🛑 Stopped active tasks and cleared queued tasks. Stopped: *{stopped}*, cleared: *{queued_count}* ✅")
        else:
            send_message(user_id, "ℹ️ You had no active or queued tasks. Nothing to stop. 👍")
        return jsonify({"ok": True})

    if command == "/stats":
        cutoff = datetime.utcnow() - timedelta(hours=12)
        rows = db_execute("SELECT SUM(words) FROM split_logs WHERE user_id = ? AND created_at >= ?", (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")), fetch=True)
        words = int(rows[0][0] or 0) if rows else 0
        send_message(user_id, f"📈 Last 12 hours: *{words}* words split. Nice! 🎉")
        return jsonify({"ok": True})

    if command == "/about":
        send_message(user_id,
            "🤖 *About WordSplitter*\nI split your messages one word at a time.\n\n"
            f"Owners: {owners_str()}\nCommands: /start /example /pause /resume /stop /status /stats\n\nHave fun! ✨"
        )
        return jsonify({"ok": True})

    # Admin: adduser, listusers
    if command == "/adduser":
        if not is_admin(user_id):
            send_message(user_id, "❌ Only admins can use /adduser. Ask an owner if you need help. 👑")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /adduser <telegram_user_id> [username]\nExamples:\n• /adduser 12345678\n• /adduser 12345678 someuser")
            return jsonify({"ok": True})
        parts = re.split(r"[,\s]+", args.strip())
        added = []
        already = []
        invalid = []
        for p in parts:
            if not p:
                continue
            try:
                tid = int(p)
            except Exception:
                invalid.append(p)
                continue
            exists = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (tid,), fetch=True)
            if exists:
                already.append(tid)
                continue
            db_execute("INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)", (tid, "", now_ts(), 0))
            added.append(tid)
            try:
                send_message(tid, "✅ You have been added and can now use the bot. Just send me any text! 🎉")
            except Exception:
                pass
        parts_msgs = []
        if added:
            parts_msgs.append(f"Added: {', '.join(str(x) for x in added)}")
        if already:
            parts_msgs.append(f"Already present: {', '.join(str(x) for x in already)}")
        if invalid:
            parts_msgs.append(f"Invalid ids: {', '.join(invalid)}")
        send_message(user_id, "✅ /adduser result:\n" + ("\n".join(parts_msgs) if parts_msgs else "No changes."))
        return jsonify({"ok": True})

    if command == "/listusers":
        if not is_admin(user_id):
            send_message(user_id, "❌ Only admins can use /listusers.")
            return jsonify({"ok": True})
        rows = db_execute("SELECT user_id, username, is_admin, added_at FROM allowed_users ORDER BY added_at DESC", fetch=True) or []
        lines = []
        for r in rows:
            uid, uname, isadm, added_at = r[0], (r[1] or ""), ("admin" if r[2] else "user"), r[3]
            name_part = f" ({uname})" if uname else ""
            lines.append(f"{uid}{name_part} — {isadm} — added={added_at}")
        body = "📋 Allowed users:\n" + ("\n".join(lines) if lines else "No allowed users yet.")
        send_message(user_id, body)
        return jsonify({"ok": True})

    # Owner-only: botinfo, broadcast
    if command == "/botinfo":
        if user_id not in OWNER_IDS:
            send_message(user_id, "❌ Only an owner may use /botinfo.")
            return jsonify({"ok": True})
        total_allowed = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0]
        active_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status IN ('running','paused')", fetch=True)[0][0]
        queued_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'", fetch=True)[0][0]
        send_message(user_id, f"🤖 Bot info — Owners: {owners_str()}\nAllowed users: {total_allowed}\nActive tasks: {active_tasks}\nQueued tasks: {queued_tasks}")
        return jsonify({"ok": True})

    if command == "/broadcast":
        if user_id not in OWNER_IDS:
            send_message(user_id, "❌ Only owner(s) can broadcast.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /broadcast <message>\nExample:\n/broadcast Hello everyone! ✨")
            return jsonify({"ok": True})

        rows = db_execute("SELECT user_id FROM allowed_users", fetch=True) or []
        succeeded = []
        failed = []
        # Build a friendly broadcast header
        header = f"📣 *Broadcast from owner*\n\n{args}"
        for r in rows:
            tid = r[0]
            try:
                ok, reason = broadcast_send_raw(tid, header)
                if ok:
                    succeeded.append(tid)
                else:
                    failed.append((tid, str(reason)))
            except Exception:
                logger.exception("Broadcast low-level send error for %s", tid)
                failed.append((tid, "exception"))

        # Summarize to owner (single send via normal send_message)
        msg = (
            f"📣 Broadcast complete ✅\n\n"
            f"Delivered to: *{len(succeeded)}* users 🎯\n"
            f"Failed: *{len(failed)}* users ❌\n"
            f"{'Failed IDs: ' + ', '.join(str(x[0]) for x in failed) if failed else ''}"
        )
        send_message(user_id, msg)
        if failed:
            # inform owners with details but DO NOT retry
            details = ", ".join(f"{x[0]}({x[1]})" for x in failed)
            _notify_all_owners(f"⚠️ Broadcast failures: {details}")
        return jsonify({"ok": True})

    # New commands: /suspend, /unsuspend, /listsuspended
    if command == "/suspend":
        if not is_admin(user_id):
            send_message(user_id, "❌ Only admins can suspend users. Ask an owner if needed. 👑")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id,
                "Usage: /suspend <user_id> <duration> [reason]\n"
                "Examples:\n• /suspend 12345678 30s Spam\n• /suspend 9876543 5m Too many messages\nDuration format: 30s | 5m | 3h | 2d"
            )
            return jsonify({"ok": True})
        parts = args.split(None, 2)
        try:
            target_id = int(parts[0])
        except Exception:
            send_message(user_id, "❌ Invalid user id. It must be numeric. Example: /suspend 12345678 5m spamming 🙏")
            return jsonify({"ok": True})
        duration = parts[1] if len(parts) > 1 else ""
        reason = parts[2] if len(parts) > 2 else ""
        m = re.match(r"^(\d+)(s|m|h|d)?$", duration)
        if not m:
            send_message(user_id, "❌ Invalid duration. Examples: 30s, 5m, 3h, 2d\nTry: /suspend 12345678 5m spamming")
            return jsonify({"ok": True})
        val, unit = int(m.group(1)), (m.group(2) or "s")
        multiplier = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 1)
        seconds = val * multiplier
        suspend_user(target_id, seconds, reason)
        send_message(user_id, f"⛔ User *{target_id}* suspended for *{val}{unit}* ✅")
        return jsonify({"ok": True})

    if command == "/unsuspend":
        if not is_admin(user_id):
            send_message(user_id, "❌ Only admins can unsuspend users. Ask an owner if needed. 👑")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /unsuspend <user_id>\nExample: /unsuspend 12345678")
            return jsonify({"ok": True})
        try:
            target_id = int(args.split()[0])
        except Exception:
            send_message(user_id, "❌ Invalid user id. Example: /unsuspend 12345678")
            return jsonify({"ok": True})
        ok = unsuspend_user(target_id)
        if ok:
            send_message(user_id, f"✅ User *{target_id}* unsuspended. Welcome back! 🎉")
        else:
            send_message(user_id, f"ℹ️ User *{target_id}* was not suspended.")
        return jsonify({"ok": True})

    if command == "/listsuspended":
        if not is_admin(user_id):
            send_message(user_id, "❌ Only admins can list suspended users.")
            return jsonify({"ok": True})
        rows = list_suspended()
        if not rows:
            send_message(user_id, "ℹ️ No suspended users at the moment. ✅")
            return jsonify({"ok": True})
        lines = []
        for r in rows:
            uid, until, reason, added_at = r[0], r[1], (r[2] or ""), r[3]
            lines.append(f"{uid} — until={until} UTC — reason={reason or '(none)'} — added={added_at}")
        send_message(user_id, "⛓️ Suspended users:\n" + "\n".join(lines))
        return jsonify({"ok": True})

    # Unknown command fallback
    send_message(user_id, "🤔 Unknown command. Try /help for available commands.")
    return jsonify({"ok": True})

# -------------------------
# Handle plain user text (non-command)
# -------------------------
def handle_user_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id):
        send_message(user_id, "❌ You're not allowed to use this bot. The owner has been informed. 🙇")
        _notify_all_owners(f"⚠️ Unallowed user {user_id} attempted to send a message: {text[:200]}")
        return jsonify({"ok": True})
    if is_suspended(user_id):
        rows = db_execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,), fetch=True)
        until = rows[0][0] if rows else "unknown"
        send_message(user_id, f"⛔ You're suspended until *{until} UTC*. You cannot submit tasks now.")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res["reason"] == "empty":
            send_message(user_id, "ℹ️ I couldn't find any words to split. Try sending a longer text. ✍️")
            return jsonify({"ok": True})
        if res["reason"] == "queue_full":
            send_message(user_id, f"⚠️ Your queue is full ({res['queue_size']}). Use /stop to clear it or wait. ⏳")
            return jsonify({"ok": True})
    running = db_execute("SELECT 1 FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,), fetch=True)
    queued_count = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
    if running:
        send_message(user_id, f"✅ Task queued! You're number *{queued_count}* in the queue. I'll notify you when it starts. ✨")
    else:
        send_message(user_id, f"🎉 Task accepted — will split *{res['total_words']}* words. Sit tight and enjoy! 🚀")
    return jsonify({"ok": True})

# -------------------------
# Start webhook setup when run directly
# -------------------------
def set_webhook():
    if not TELEGRAM_API_BASE or not WEBHOOK_URL:
        logger.warning("Cannot set webhook — TELEGRAM_TOKEN or WEBHOOK_URL missing.")
        return
    try:
        resp = _request_session.post(f"{TELEGRAM_API_BASE}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
        logger.info("setWebhook response: %s", resp.text[:400])
    except Exception:
        logger.exception("set_webhook failed")

if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        pass
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
