#!/usr/bin/env python3
"""
Telegram Word-Splitter Bot (Webhook-ready) - app.py (updated)

Changes in this version:
- /stats now shows only the requesting user's words split in the last 12 hours.
- /addadmin command added (owner-only) to grant admin rights to a user.
- Admins are able to use /adduser and /listusers (unchanged behavior; preserved).
- /removeuser can remove any user (including admins) from allowed_users.
- All other behavior kept as in the previous app.py (tg_call rate-limiter, botinfo showing active users and last-1h stats, etc.)

Run with:
    gunicorn app:app --bind 0.0.0.0:$PORT
"""
import os
import time
import json
import sqlite3
import threading
import traceback
import logging
import re
from datetime import datetime, timedelta
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Create Flask app once
app = Flask(__name__)

# Configuration via environment variables
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")  # full https URL for webhook
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))  # numeric Telegram user id
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

if not TELEGRAM_TOKEN or not WEBHOOK_URL or not OWNER_ID:
    logger.warning("TELEGRAM_TOKEN, WEBHOOK_URL, OWNER_ID should be set in environment.")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

# Reuse a requests.Session for faster connections
_session = requests.Session()

# DB helper
_db_lock = threading.Lock()


def init_db():
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
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


def db_execute(query, params=(), fetch=False):
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(query, params)
        if fetch:
            rows = c.fetchall()
            return rows
        conn.commit()


# Initialize DB and ensure owner is allowed/admin
init_db()
try:
    res = db_execute("SELECT user_id FROM allowed_users WHERE user_id = ?", (OWNER_ID,), fetch=True)
    if not res:
        db_execute(
            "INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
            (OWNER_ID, OWNER_USERNAME, datetime.utcnow().isoformat(), 1),
        )
except Exception:
    logger.exception("Error ensuring owner in allowed_users")


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


# --- Robust tg_call implementation ---
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
    global _tele_429_count
    if not TELEGRAM_API:
        logger.error("tg_call attempted but TELEGRAM_API not configured")
        return None

    url = f"{TELEGRAM_API}/{method}"
    max_retries = max(1, TG_CALL_MAX_RETRIES)
    backoff = 1.0

    for attempt in range(1, max_retries + 1):
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
                if retry_after is None:
                    retry_after = backoff
                logger.warning(
                    "Telegram API 429 Too Many Requests (attempt %d/%d). retry_after=%s secs. method=%s",
                    attempt, max_retries, retry_after, method,
                )
                sleep_time = min(max(0.5, float(retry_after)), TG_CALL_MAX_BACKOFF)
                time.sleep(sleep_time)
                backoff = min(backoff * 2, TG_CALL_MAX_BACKOFF)
                continue

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
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    data = tg_call("sendMessage", payload)
    if data and data.get("result"):
        mid = data["result"].get("message_id")
        if mid:
            record_sent_message(chat_id, mid)
        return data["result"]
    return None


def delete_message(chat_id: int, message_id: int):
    payload = {"chat_id": chat_id, "message_id": message_id}
    data = tg_call("deleteMessage", payload)
    if data and data.get("ok"):
        mark_message_deleted(chat_id, message_id)
    return data


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


# Task management functions (unchanged)
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


# Authorization helpers
def is_allowed(user_id: int) -> bool:
    rows = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    return bool(rows)


def is_admin(user_id: int) -> bool:
    rows = db_execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    if not rows:
        return False
    return bool(rows[0][0])


# Background worker to process tasks for users
_worker_stop = threading.Event()


def process_user_queue(user_id: int, chat_id: int, username: str):
    lock = get_user_lock(user_id)
    if not lock.acquire(blocking=False):
        return
    try:
        while True:
            task = get_next_task_for_user(user_id)
            if not task:
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
            send_message(chat_id, start_msg)
            i = sent
            while i < total:
                status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                if not status_row:
                    break
                status = status_row[0][0]
                if status == "paused":
                    send_message(chat_id, "⏸️ Task paused. Use /resume to continue.")
                    while True:
                        time.sleep(0.5)
                        status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                        if not status_row:
                            break
                        status = status_row[0][0]
                        if status == "running":
                            send_message(chat_id, "▶️ Resuming your task.")
                            break
                        if status == "cancelled":
                            break
                    if status == "cancelled":
                        break
                if status == "cancelled":
                    break
                send_message(chat_id, words[i])
                db_execute("UPDATE tasks SET sent_count = sent_count + 1 WHERE id = ?", (task_id,))
                i += 1
                time.sleep(interval)
            final_status_row = db_execute("SELECT status, sent_count, total_words FROM tasks WHERE id = ?", (task_id,), fetch=True)
            final_status = final_status_row[0][0] if final_status_row else "done"
            sent_count = int(final_status_row[0][1] or 0) if final_status_row else total
            if final_status != "cancelled":
                mark_task_done(task_id)
                record_split_log(user_id, username, sent_count)
                send_message(chat_id, "✅ All done! Your text was split.")
            else:
                send_message(chat_id, "🛑 Task stopped.")
            qcount_after = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
            if qcount_after > 0:
                send_message(chat_id, "⏩ Next task will start soon!")
    finally:
        lock.release()


def global_worker_loop():
    while not _worker_stop.is_set():
        try:
            rows = db_execute("SELECT DISTINCT user_id, username FROM tasks WHERE status = 'queued' ORDER BY created_at ASC", fetch=True)
            for r in rows:
                user_id = r[0]
                username = r[1] or ""
                t = threading.Thread(target=process_user_queue, args=(user_id, user_id, username), daemon=True)
                t.start()
            time.sleep(0.5)
        except Exception:
            traceback.print_exc()
            time.sleep(1)


# Scheduler jobs
scheduler = BackgroundScheduler()


def hourly_owner_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    rows = db_execute("SELECT user_id, username, SUM(words) as s FROM split_logs WHERE created_at >= ? GROUP BY user_id ORDER BY s DESC", (cutoff.isoformat(),), fetch=True)
    if not rows:
        send_message(OWNER_ID, "🕐 Last 1 hour: no splits.")
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
    send_message(OWNER_ID, body)


def delete_old_bot_messages():
    msgs = get_messages_older_than(days=1)
    for m in msgs:
        try:
            delete_message(m["chat_id"], m["message_id"])
        except Exception:
            mark_message_deleted(m["chat_id"], m["message_id"])


def maintenance_hourly_health():
    send_message(OWNER_ID, "👑 Bot health: running.")


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


# Command handlers
def handle_command(user_id: int, username: str, command: str, args: str):
    if not is_allowed(user_id) and command not in ("/start", "/help"):
        send_message(user_id, "❌ Sorry, you are not allowed to use this bot. The owner has been notified.")
        send_message(OWNER_ID, f"⚠️ Unallowed access attempt by {username or user_id} ({user_id}).")
        return jsonify({"ok": True})

    if command == "/start":
        body = (
            f"👋 Hi {username or user_id}!\n"
            "I split your text into individual word messages.\n"
            "Admin commands (admins + owner): /adduser /removeuser /listusers\n"
            "Owner-only: /botinfo /broadcast /addadmin /removeadmin\n"
            "User commands: /start /example /pause /resume /status /stop /stats /about\n"
            "Just send any text and I'll split it for you."
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/example":
        sample = "This is a demo split"
        send_message(user_id, "Running a short example...")
        res = enqueue_task(user_id, username, sample)
        running_exists = bool(db_execute("SELECT 1 FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,), fetch=True))
        queued = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
        if running_exists:
            send_message(user_id, f"📝 Queued. You have {queued} task(s) waiting.")
        else:
            send_message(user_id, f"✅ Task added. Words: {res['total_words']}.")
        return jsonify({"ok": True})

    if command == "/pause":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "❌ No active task to pause.")
            return jsonify({"ok": True})
        task_id = rows[0][0]
        mark_task_paused(task_id)
        send_message(user_id, "⏸️ Paused. Use /resume to continue.")
        return jsonify({"ok": True})

    if command == "/resume":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "❌ No paused task to resume.")
            return jsonify({"ok": True})
        task_id = rows[0][0]
        mark_task_resumed(task_id)
        send_message(user_id, "▶️ Resuming your task now.")
        return jsonify({"ok": True})

    if command == "/status":
        active = db_execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        queued = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
        if active:
            aid, status, total_words, sent_count = active[0]
            remaining = int(total_words or 0) - int(sent_count or 0)
            send_message(user_id, f"📊 Status: {status}\nRemaining words: {remaining}\nQueue size: {queued}")
        else:
            if queued > 0:
                send_message(user_id, f"📝 Waiting. Your first task is in line. Queue size: {queued}")
            else:
                send_message(user_id, "📊 You have no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stop":
        queued_rows = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
        queued_count = queued_rows[0][0] if queued_rows else 0
        stopped = cancel_active_task_for_user(user_id)
        if stopped > 0:
            send_message(user_id, "🛑 Active task stopped. Your queued tasks have been cleared too.")
        elif queued_count > 0:
            db_execute("UPDATE tasks SET status = 'cancelled' WHERE user_id = ? AND status = 'queued'", (user_id,))
            send_message(user_id, f"🛑 Cleared {queued_count} queued task(s).")
        else:
            send_message(user_id, "ℹ️ You had no active or queued tasks.")
        return jsonify({"ok": True})

    # /stats now shows only the requesting user's words split in the last 12 hours
    if command == "/stats":
        cutoff = datetime.utcnow() - timedelta(hours=12)
        rows = db_execute("SELECT SUM(words) FROM split_logs WHERE user_id = ? AND created_at >= ?", (user_id, cutoff.isoformat()), fetch=True)
        words = int(rows[0][0] or 0) if rows else 0
        send_message(user_id, f"🕰️ Your last 12 hours: {words} words split. Nice!")
        return jsonify({"ok": True})

    if command == "/about":
        body = (
            "About:\nI split texts into single-word messages. Features: queueing, pause/resume, hourly owner stats, auto-delete old bot messages.\n"
            f"Developer: @{OWNER_USERNAME}"
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    # Admin commands (admins + owner)
    if command == "/adduser":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /adduser <telegram_user_id> [username]")
            return jsonify({"ok": True})
        parts = args.split()
        try:
            target_id = int(parts[0])
        except Exception:
            send_message(user_id, "Invalid user id. Must be numeric.")
            return jsonify({"ok": True})
        uname = parts[1] if len(parts) > 1 else ""
        count = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0]
        if count >= MAX_ALLOWED_USERS:
            send_message(user_id, f"Cannot add more users. Max: {MAX_ALLOWED_USERS}")
            return jsonify({"ok": True})
        db_execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                   (target_id, uname, get_now_iso(), 0))
        send_message(user_id, f"✅ User {target_id} added.")
        send_message(target_id, "✅ You have been added. Send any text to start.")
        return jsonify({"ok": True})

    if command == "/removeuser":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /removeuser <telegram_user_id>")
            return jsonify({"ok": True})
        try:
            target_id = int(args.split()[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return jsonify({"ok": True})
        # Remove target user regardless of their is_admin flag (admins or normal users)
        db_execute("DELETE FROM allowed_users WHERE user_id = ?", (target_id,))
        send_message(user_id, f"✅ User {target_id} removed.")
        send_message(target_id, "❌ You have been removed. Contact the owner to regain access.")
        return jsonify({"ok": True})

    if command == "/listusers":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this.")
            return jsonify({"ok": True})
        rows = db_execute("SELECT user_id, username, is_admin, added_at FROM allowed_users", fetch=True)
        lines = []
        for r in rows:
            lines.append(f"{r[0]} {r[1] or ''} admin={bool(r[2])} added={r[3]}")
        body = "Allowed users:\n" + ("\n".join(lines) if lines else "(none)")
        send_message(user_id, body)
        return jsonify({"ok": True})

    # owner-only addadmin/removeadmin
    if command == "/addadmin":
        if user_id != OWNER_ID:
            send_message(user_id, "❌ Only the owner can add admins.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /addadmin <telegram_user_id>")
            return jsonify({"ok": True})
        try:
            target_id = int(args.split()[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return jsonify({"ok": True})
        rows = db_execute("SELECT user_id FROM allowed_users WHERE user_id = ?", (target_id,), fetch=True)
        if not rows:
            db_execute("INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                       (target_id, "", get_now_iso(), 1))
        else:
            db_execute("UPDATE allowed_users SET is_admin = 1 WHERE user_id = ?", (target_id,))
        send_message(user_id, f"✅ {target_id} is now an admin.")
        send_message(target_id, "✅ You have been granted admin privileges.")
        return jsonify({"ok": True})

    if command == "/removeadmin":
        if user_id != OWNER_ID:
            send_message(user_id, "❌ Only the owner can remove admins.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /removeadmin <telegram_user_id>")
            return jsonify({"ok": True})
        try:
            target_id = int(args.split()[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return jsonify({"ok": True})
        db_execute("UPDATE allowed_users SET is_admin = 0 WHERE user_id = ?", (target_id,))
        send_message(user_id, f"✅ Admin removed: {target_id}")
        send_message(target_id, "❌ Your admin privileges have been revoked.")
        return jsonify({"ok": True})

    if command == "/botinfo":
        if user_id != OWNER_ID:
            send_message(user_id, "❌ Only the bot owner can use /botinfo")
            return jsonify({"ok": True})
        total_allowed = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0]
        active_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status IN ('running','paused')", fetch=True)[0][0]
        queued_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'", fetch=True)[0][0]

        # Active users with remaining words and counts
        active_rows = db_execute(
            "SELECT user_id, username, SUM(total_words - IFNULL(sent_count,0)) as remaining_words, COUNT(*) as active_count "
            "FROM tasks WHERE status IN ('running','paused') GROUP BY user_id ORDER BY remaining_words DESC",
            fetch=True,
        )
        queued_rows = db_execute(
            "SELECT user_id, COUNT(*) as queued_count FROM tasks WHERE status = 'queued' GROUP BY user_id",
            fetch=True,
        )
        queued_map = {r[0]: int(r[1]) for r in queued_rows}

        peruser_active_lines = []
        for r in active_rows:
            uid = r[0]
            uname = r[1] or ""
            remaining_words = int(r[2] or 0)
            active_count = int(r[3] or 0)
            queued_count = queued_map.get(uid, 0)
            name_part = f" ({uname})" if uname else ""
            peruser_active_lines.append(f"{uid}{name_part} - {remaining_words} remaining - {active_count} active - {queued_count} queued")

        # User stats for last 1 hour
        cutoff = datetime.utcnow() - timedelta(hours=1)
        stats_rows = db_execute(
            "SELECT user_id, username, SUM(words) as s FROM split_logs WHERE created_at >= ? GROUP BY user_id ORDER BY s DESC",
            (cutoff.isoformat(),),
            fetch=True,
        )
        stats_lines = []
        for r in stats_rows:
            uid = r[0]
            uname = r[1] or ""
            wsum = int(r[2] or 0)
            stats_lines.append(f"{uid}{(' (' + uname + ')') if uname else ''} - {wsum} words")

        body_parts = [
            "Bot status: Online",
            f"Allowed users: {total_allowed}",
            f"Active tasks: {active_tasks}",
            f"Queued tasks: {queued_tasks}",
            "",
            "Users with active tasks:",
            "\n".join(peruser_active_lines) if peruser_active_lines else "No active users",
            "",
            "User stats (last 1h):",
            "\n".join(stats_lines) if stats_lines else "No activity in the last 1h",
        ]
        body = "\n".join(body_parts)
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/broadcast":
        if user_id != OWNER_ID:
            send_message(user_id, "❌ Only owner can broadcast")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /broadcast <message>")
            return jsonify({"ok": True})
        rows = db_execute("SELECT user_id FROM allowed_users", fetch=True)
        count = 0
        fails = 0
        for r in rows:
            try:
                send_message(r[0], f"📣 Broadcast from owner:\n\n{args}")
                count += 1
            except Exception:
                fails += 1
        send_message(user_id, f"Broadcast done. Success: {count}, Failed: {fails}")
        return jsonify({"ok": True})

    send_message(user_id, "Unknown command.")
    return jsonify({"ok": True})


def get_queue_size(user_id):
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
    return q[0][0] if q else 0


def handle_new_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id):
        send_message(user_id, "❌ Sorry, you are not allowed. The owner has been notified.")
        send_message(OWNER_ID, f"⚠️ Unallowed access attempt by {username or user_id} ({user_id}). Message: {text}")
        return jsonify({"ok": True})
    if is_maintenance_now():
        send_message(user_id, "🛠️ Maintenance in progress. New tasks are blocked. Please try later.")
        send_message(OWNER_ID, f"🛠️ Maintenance attempt by {user_id}.")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res.get("reason") == "empty":
            send_message(user_id, "Empty text. Nothing to split.")
            return jsonify({"ok": True})
        if res.get("reason") == "queue_full":
            send_message(user_id, f"❌ Your queue is full ({res['queue_size']}). Use /stop or wait.")
            return jsonify({"ok": True})
    running_exists = bool(db_execute("SELECT 1 FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,), fetch=True))
    queued = get_queue_size(user_id)
    if running_exists:
        send_message(user_id, f"📝 Queued. You have {queued} task(s) waiting.")
    else:
        send_message(user_id, f"✅ Task added. Words: {res['total_words']}.")
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
    return jsonify({"ok": True}), 200


@app.route("/debug/routes", methods=["GET"])
def debug_routes():
    try:
        rules = sorted(str(r) for r in app.url_map.iter_rules())
        return jsonify({"routes": rules}), 200
    except Exception:
        logger.exception("Failed to list routes")
        return jsonify({"ok": False, "error": "failed to list routes"}), 500


if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        logger.exception("Failed to set webhook at startup")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
