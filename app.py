#!/usr/bin/env python3
"""
Telegram Word-Splitter Bot (Webhook-ready) without python-telegram-bot.

This is your original script with:
- a single Flask app instance (no duplicates),
- a robust /health (GET, HEAD) endpoint and /health/ alternate,
- a /debug/routes endpoint to list registered routes,
- root (/) accepts GET for health and forwards POST to webhook() as a fallback,
- logging to stdout for Render,
- minimal other edits only to integrate health endpoints safely.

Run with: gunicorn app:app --bind 0.0.0.0:$PORT
"""
import os
import time
import json
import sqlite3
import threading
import traceback
import logging
from datetime import datetime, timedelta
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, abort, jsonify
import requests

# Configure logging to stdout (Render will show these logs)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Create Flask app once
app = Flask(__name__)

# Configuration via environment variables
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")  # full https URL for webhook, e.g. https://your-render-service.onrender.com/webhook
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))  # numeric Telegram user id
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "justmemmy")  # clickable @justmemmy
MAX_ALLOWED_USERS = int(os.environ.get("MAX_ALLOWED_USERS", "50"))
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "50"))
MAINTENANCE_START_HOUR_WAT = 3  # 03:00 WAT
MAINTENANCE_END_HOUR_WAT = 4    # 04:00 WAT
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))

if not TELEGRAM_TOKEN or not WEBHOOK_URL or not OWNER_ID:
    logger.warning("TELEGRAM_TOKEN, WEBHOOK_URL, OWNER_ID should be set in environment.")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

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


# Telegram API helpers (requests)
def tg_call(method: str, payload: dict):
    if not TELEGRAM_API:
        logger.error("tg_call attempted but TELEGRAM_API not configured")
        return None
    url = f"{TELEGRAM_API}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            logger.error("Telegram API error: %s", data)
        return data
    except Exception:
        logger.exception("tg_call failed")
        return None


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
        resp = requests.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()
        logger.info("Webhook set response: %s", resp.text)
        return resp.json()
    except Exception:
        logger.exception("Failed to set webhook")
        return None


# Task management
def enqueue_task(user_id: int, username: str, text: str) -> dict:
    words = split_text_into_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,), fetch=True)
    pending = q[0][0] if q else 0
    if pending >= MAX_QUEUE_PER_USER:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    db_execute(
        "INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (user_id, username, text, json.dumps(words), total, "queued", get_now_iso()),
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
            est_seconds = interval * total
            est_str = str(timedelta(seconds=int(est_seconds)))
            start_msg = f"🚀 Task Starting! (Waiting: {qcount})\nWords: {total}. Time per word: **{interval}s**\n⏳ Estimated total time: **{est_str}**"
            send_message(chat_id, start_msg)
            i = 0
            while i < total:
                status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                if not status_row:
                    break
                status = status_row[0][0]
                if status == "paused":
                    send_message(chat_id, "⏸️ **Task Paused!** Waiting for /resume to continue.")
                    while True:
                        time.sleep(1)
                        status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                        if not status_row:
                            break
                        status = status_row[0][0]
                        if status == "running":
                            send_message(chat_id, "▶️ **Welcome Back!** Resuming word transmission now.")
                            break
                        if status == "cancelled":
                            break
                    if status == "cancelled":
                        break
                if status == "cancelled":
                    break
                send_message(chat_id, words[i])
                i += 1
                time.sleep(interval)
            final_status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
            final_status = final_status_row[0][0] if final_status_row else "done"
            if final_status != "cancelled":
                mark_task_done(task_id)
                record_split_log(user_id, username, total)
                send_message(chat_id, "✅ **All Done!** Split finished successfully.")
            else:
                send_message(chat_id, "🛑 **Active Task Stopped!** The current word transmission has been terminated.")
            qcount_after = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
            if qcount_after > 0:
                send_message(chat_id, "⏩ **Next Task Starting Soon!** Your next queued task is ready for processing.")
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
            time.sleep(2)
        except Exception:
            traceback.print_exc()
            time.sleep(5)


# Scheduler jobs
scheduler = BackgroundScheduler()


def hourly_owner_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    rows = db_execute("SELECT user_id, username, SUM(words) FROM split_logs WHERE created_at >= ? GROUP BY user_id", (cutoff.isoformat(),), fetch=True)
    if not rows:
        send_message(OWNER_ID, f"🕐 **Last 1 Hour Activity**\nNo splits in the last 1 hour.")
        return
    lines = []
    total_words = 0
    for r in rows:
        uid = r[0]
        wsum = int(r[2] or 0)
        total_words += wsum
        lines.append(f"{uid} - {wsum} words")
    body = "🕐 **Last 1 Hour Activity**\n" + "\n".join(lines) + f"\n\nTotal words: {total_words}"
    send_message(OWNER_ID, body)


def delete_old_bot_messages():
    msgs = get_messages_older_than(days=1)
    for m in msgs:
        try:
            delete_message(m["chat_id"], m["message_id"])
        except Exception:
            mark_message_deleted(m["chat_id"], m["message_id"])


def maintenance_hourly_health():
    send_message(OWNER_ID, "👑 **Bot Health Check**\nBot is running. Time: " + get_now_iso())


scheduler.add_job(hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10))
scheduler.add_job(delete_old_bot_messages, "interval", minutes=30, next_run_time=datetime.utcnow() + timedelta(seconds=20))
scheduler.add_job(maintenance_hourly_health, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=15))
scheduler.start()

_worker_thread = threading.Thread(target=global_worker_loop, daemon=True)
_worker_thread.start()


# Flask webhook endpoint (POST only)
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


# Command handlers (same semantics)
def handle_command(user_id: int, username: str, command: str, args: str):
    if not is_allowed(user_id) and command not in ("/start", "/help"):
        send_message(user_id, f"❌ **Sorry!** You are not allowed to use this bot. A request has been sent to the owner.")
        send_message(OWNER_ID, f"⚠️ Unallowed access attempt by @{username or user_id} ({user_id}). Message: {args or '[no text]'}")
        return jsonify({"ok": True})
    if command == "/start":
        body = (
            f"👋 **Welcome, @{username or user_id}!**\n"
            "I split your text into individual word messages. Commands:\n"
            "/start /example /pause /resume /status /stop /stats /about\n"
            "Owner-only: /adduser /removeuser /listusers /botinfo /broadcast\n"
            "How to use: send any text to split it into words."
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/example":
        sample = "This is a demo split"
        send_message(user_id, "Running example split...")
        enqueue_task(user_id, username, sample)
        send_message(user_id, f"📝 **Queued!** You currently have **{get_queue_size(user_id)}** task(s) waiting in line. Your current task must finish first.")
        return jsonify({"ok": True})

    if command == "/pause":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "❌ **Sorry!** No active task found for you to pause.")
            return jsonify({"ok": True})
        task_id = rows[0][0]
        mark_task_paused(task_id)
        send_message(user_id, "⏸️ **Task Paused!** Waiting for /resume to continue.")
        return jsonify({"ok": True})

    if command == "/resume":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "❌ **Sorry!** No paused task found for you to resume.")
            return jsonify({"ok": True})
        task_id = rows[0][0]
        mark_task_resumed(task_id)
        send_message(user_id, "▶️ **Welcome Back!** Resuming word transmission now.")
        return jsonify({"ok": True})

    if command == "/status":
        active = db_execute("SELECT id, status, total_words, created_at FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        queued = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
        if active:
            aid, status, total_words, created_at = active[0]
            send_message(user_id, f"📊 **Current Status Check**\nStatus: {status}\nRemaining words: {total_words}\nQueue size: {queued}")
        else:
            if queued > 0:
                send_message(user_id, f"📝 **Status: Waiting.** Your first task is waiting in line. Queue size: {queued}")
            else:
                send_message(user_id, "📊 You have no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stop":
        queued_rows = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
        queued_count = queued_rows[0][0] if queued_rows else 0
        stopped = cancel_active_task_for_user(user_id)
        if stopped > 0:
            send_message(user_id, f"🛑 **Active Task Stopped!** The current word transmission has been terminated. All your queued tasks have also been cleared.")
        elif queued_count > 0:
            db_execute("UPDATE tasks SET status = 'cancelled' WHERE user_id = ? AND status = 'queued'", (user_id,))
            send_message(user_id, f"🛑 **Queued Tasks Cleared!** You had **{queued_count}** task(s) waiting that have been removed.")
        else:
            send_message(user_id, "ℹ️ You had no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stats":
        cutoff = datetime.utcnow() - timedelta(hours=12)
        rows = db_execute("SELECT SUM(words) FROM split_logs WHERE created_at >= ?", (cutoff.isoformat(),), fetch=True)
        words = int(rows[0][0] or 0)
        send_message(user_id, f"🕰️ **Your Recent Activity (Last 12 Hours)**\nYou have split **{words}** words! Keep up the great work! 💪 📝")
        return jsonify({"ok": True})

    if command == "/about":
        body = (
            "*About This Bot*\n"
            "I split texts into individual word messages. Speed control is dynamic based on total words.\n"
            "Features: queueing, pause/resume, maintenance window, hourly owner stats, auto-delete bot messages older than 24h.\n"
            f"Developer: @{OWNER_USERNAME}"
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/adduser":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this command.")
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
            send_message(user_id, f"Cannot add more users. Max allowed users: {MAX_ALLOWED_USERS}")
            return jsonify({"ok": True})
        db_execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                   (target_id, uname, get_now_iso(), 0))
        send_message(user_id, f"✅ User {target_id} added to allowed list.")
        send_message(target_id, "✅ You have been added to the Word Splitter bot. Send any text to start.")
        return jsonify({"ok": True})

    if command == "/removeuser":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this command.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /removeuser <telegram_user_id>")
            return jsonify({"ok": True})
        try:
            target_id = int(args.split()[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return jsonify({"ok": True})
        db_execute("DELETE FROM allowed_users WHERE user_id = ?", (target_id,))
        send_message(user_id, f"✅ User {target_id} removed from allowed list.")
        send_message(target_id, "❌ You have been removed from the Word Splitter bot. Contact owner to regain access.")
        return jsonify({"ok": True})

    if command == "/listusers":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this command.")
            return jsonify({"ok": True})
        rows = db_execute("SELECT user_id, username, is_admin, added_at FROM allowed_users", fetch=True)
        lines = []
        for r in rows:
            lines.append(f"{r[0]} @{r[1] or ''} admin={bool(r[2])} added={r[3]}")
        body = "Allowed users:\n" + ("\n".join(lines) if lines else "(none)")
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/botinfo":
        if user_id != OWNER_ID:
            send_message(user_id, "❌ Only the bot owner can use /botinfo")
            return jsonify({"ok": True})
        total_allowed = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0]
        active_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status IN ('running','paused')", fetch=True)[0][0]
        queued_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'", fetch=True)[0][0]
        rows = db_execute("SELECT user_id, SUM(words) FROM split_logs WHERE created_at >= ?", ((datetime.utcnow() - timedelta(hours=2)).isoformat(),), fetch=True)
        peruser_lines = []
        for r in rows:
            peruser_lines.append(f"{r[0]} - {r[1] or 0}")
        body = (
            f"Bot status: Online\n"
            f"Allowed users: {total_allowed}\n"
            f"Active tasks: {active_tasks}\n"
            f"Queued tasks: {queued_tasks}\n"
            f"User stats (last 2h):\n" + ("\n".join(peruser_lines) if peruser_lines else "No activity")
        )
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
        send_message(user_id, f"Broadcast complete. Success: {count}, Failed: {fails}")
        return jsonify({"ok": True})

    send_message(user_id, "Unknown command.")
    return jsonify({"ok": True})


def get_queue_size(user_id):
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
    return q[0][0] if q else 0


def handle_new_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id):
        send_message(user_id, "❌ **Sorry!** You are not allowed to use this bot. A request has been sent to the owner.")
        send_message(OWNER_ID, f"⚠️ Unallowed access attempt by @{username or user_id} ({user_id}). Message: {text}")
        return jsonify({"ok": True})
    if is_maintenance_now():
        send_message(user_id, "🛠️ **Maintenance in Progress!** New tasks are blocked. Please try again after 4 AM WAT.")
        send_message(OWNER_ID, f"🛠️ Maintenance attempted during window by {user_id}.")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res.get("reason") == "empty":
            send_message(user_id, "Empty or whitespace-only text. Nothing to split.")
            return jsonify({"ok": True})
        if res.get("reason") == "queue_full":
            send_message(user_id, f"❌ Your queue is full ({res['queue_size']}). Please /stop or wait for tasks to finish.")
            return jsonify({"ok": True})
    qsize = get_queue_size(user_id)
    if qsize > 1:
        send_message(user_id, f"📝 **Queued!** You currently have **{qsize}** task(s) waiting in line. Your current task must finish first.")
    else:
        send_message(user_id, f"🚀 Task queued and will start shortly. Words: {res['total_words']}. Time per word: **{compute_interval(res['total_words'])}s**")
    return jsonify({"ok": True})


# Ensure the root GET exists and POST forwarded to webhook() fallback
@app.route("/", methods=["GET", "POST"])
def root_forward():
    if request.method == "POST":
        logger.info("Received POST at root; forwarding to /webhook")
        try:
            return webhook()
        except Exception:
            logger.exception("Forwarding POST to webhook failed")
            return jsonify({"ok": False, "error": "forward failed"}), 200
    return "Word Splitter Bot is running.", 200


# Health endpoint for UptimeRobot (accept HEAD and GET, and both /health and /health/)
@app.route("/health", methods=["GET", "HEAD"])
@app.route("/health/", methods=["GET", "HEAD"])
def health():
    logger.info("Health check from %s method=%s", request.remote_addr, request.method)
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat()}), 200


# Debug: list all registered routes (useful to verify /health exists)
@app.route("/debug/routes", methods=["GET"])
def debug_routes():
    try:
        rules = sorted(str(r) for r in app.url_map.iter_rules())
        return jsonify({"routes": rules}), 200
    except Exception:
        logger.exception("Failed to list routes")
        return jsonify({"ok": False, "error": "failed to list routes"}), 500


if __name__ == "__main__":
    # Optionally set webhook when running directly (local debug). On Render, gunicorn will import app and this block won't run.
    try:
        set_webhook()
    except Exception:
        logger.exception("Failed to set webhook at startup")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
