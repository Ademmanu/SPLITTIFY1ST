#!/usr/bin/env python3
"""
Final integrated Telegram Word-Splitter Bot (app.py)

Features implemented per user request:
- OWNER_IDS and ALLOWED_USERS env vars
- /adduser, /listusers (admin)
- /suspend, /unsuspend, /listsuspended (admin) with usage examples when input is invalid
- Broadcast uses one-shot sender (no retries) to avoid duplicate deliveries
- Hourly owner stats (last 1 hour) sent automatically and included in /botinfo, without "UTC time" reference.
- /help removed completely
- Bot-health hourly reports removed
- Suspensions enforce immediately and cancel running/queued tasks
- Timestamps formatted as "YYYY-MM-DD HH:MM:SS" (in UTC for database)
- Time displays converted to WAT (Nigeria Time) for user-facing messages
- Added Scheduled Maintenance (3:00 AM - 4:00 AM WAT)
- Added /mem and /sysmem endpoints for memory usage checking (psutil)
- Fixed database inefficiency by reverting to per-word updates to satisfy /status requirement.
- Maintenance now stops/cancels running tasks, not pauses them.
- Fixed redundant maintenance notifications for suspended users.
- Owner tag (@justmemmy) and emojis are consistently applied.
- Removed "UTC time" references from status/reports.
- Simplified /stats output to remove the explicit 12-hour timestamp detail.
- Fixed /botinfo inefficiency by getting all queued task counts in one query.
"""

import os
import time
import json
import sqlite3
import threading
import logging
import re
import random
from datetime import datetime, timedelta
from typing import List, Dict
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests
import traceback
import psutil

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wordsplitter")

# App
app = Flask(__name__)

# Free Tier container memory limit (in MB)
CONTAINER_MAX_RAM_MB = 512

@app.route("/mem")
def mem_usage():
    process = psutil.Process()
    mem_used_mb = process.memory_info().rss / (1024 * 1024)
    return f"Memory used by app: {mem_used_mb:.2f} MB\n"

@app.route("/sysmem")
def system_memory():
    process = psutil.Process()
    mem_used_mb = process.memory_info().rss / (1024 * 1024)
    available_mb = max(CONTAINER_MAX_RAM_MB - mem_used_mb, 0)
    percent = (mem_used_mb / CONTAINER_MAX_RAM_MB) * 100

    return (
        f"Total container RAM (Free Tier): {CONTAINER_MAX_RAM_MB:.2f} MB\n"
        f"Available RAM: {available_mb:.2f} MB\n"
        f"Used RAM: {mem_used_mb:.2f} MB\n"
        f"Usage percent: {percent:.1f}%\n"
    )

# Config from env
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
OWNER_IDS_RAW = os.environ.get("OWNER_IDS", "")      # comma/space separated IDs
ALLOWED_USERS_RAW = os.environ.get("ALLOWED_USERS", "")  # auto-allowed IDs
OWNER_USERNAMES_RAW = os.environ.get("OWNER_USERNAMES", "")
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
MAX_ALLOWED_USERS = int(os.environ.get("MAX_ALLOWED_USERS", "500"))
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "50"))
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))
MAX_MSG_PER_SECOND = float(os.environ.get("MAX_MSG_PER_SECOND", "50"))

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None
_session = requests.Session()

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
            continue
    return ids

OWNER_IDS = parse_id_list(OWNER_IDS_RAW)
OWNER_USERNAMES = [s for s in (OWNER_USERNAMES_RAW.split(",") if OWNER_USERNAMES_RAW else []) if s]
PRIMARY_OWNER = OWNER_IDS[0] if OWNER_IDS else None

# Timezone and Time Helpers
# WAT (Nigeria Time) is UTC + 1 hour
NIGERIA_TZ_OFFSET = timedelta(hours=1)

def now_ts() -> str:
    # Keeps DB timestamps in UTC (server standard)
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def utc_to_wat_ts(utc_ts: str) -> str:
    # Converts a UTC DB timestamp string to a WAT display string
    try:
        utc_dt = datetime.strptime(utc_ts, "%Y-%m-%d %H:%M:%S")
        wat_dt = utc_dt + NIGERIA_TZ_OFFSET
        return wat_dt.strftime("%Y-%m-%d %H:%M:%S WAT")
    except Exception:
        # Fallback if DB data is corrupted
        return f"{utc_ts} (UTC error)"

# Maintenance State
_is_maintenance = False
_maintenance_lock = threading.Lock()

def is_maintenance_time() -> bool:
    with _maintenance_lock:
        return _is_maintenance

# DB helpers
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
        )""")
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
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS split_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            words INTEGER,
            created_at TEXT
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS sent_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            message_id INTEGER,
            sent_at TEXT,
            deleted INTEGER DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS suspended_users (
            user_id INTEGER PRIMARY KEY,
            suspended_until TEXT,
            reason TEXT,
            added_at TEXT
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS send_failures (
            user_id INTEGER PRIMARY KEY,
            failures INTEGER,
            last_failure_at TEXT
        )""")
        conn.commit()

init_db()

# Ensure owners are admins in allowed_users
for idx, oid in enumerate(OWNER_IDS):
    uname = OWNER_USERNAMES[idx] if idx < len(OWNER_USERNAMES) else ""
    try:
        exists = None
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (oid,))
            exists = c.fetchone()
        if not exists:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                          (oid, uname, now_ts(), 1))
                conn.commit()
        else:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("UPDATE allowed_users SET is_admin = 1 WHERE user_id = ?", (oid,))
                conn.commit()
    except Exception:
        logger.exception("Error ensuring owner in allowed_users")

# Auto-add ALLOWED_USERS env var
for uid in parse_id_list(ALLOWED_USERS_RAW):
    try:
        rows = None
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (uid,))
            rows = c.fetchone()
        if not rows:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                          (uid, "", now_ts(), 0))
                conn.commit()
            # best-effort notify
            try:
                if TELEGRAM_API:
                    _session.post(f"{TELEGRAM_API}/sendMessage", json={"chat_id": uid, "text": "You were added via ALLOWED_USERS. Hello! 🥳"}, timeout=3)
            except Exception:
                pass
    except Exception:
        logger.exception("Auto-add allowed user error")

# Token bucket for normal sends
_token_bucket = {"tokens": MAX_MSG_PER_SECOND, "last": time.time(), "capacity": max(1.0, MAX_MSG_PER_SECOND), "lock": threading.Lock()}

def acquire_token(timeout=10.0):
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

def parse_telegram_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

# Normal send with token bucket and limited retry/backoff
def send_message(chat_id: int, text: str, parse_mode: str = "Markdown"):
    if not TELEGRAM_API:
        logger.error("No TELEGRAM_TOKEN; cannot send message.")
        return None
    # acquire token (best-effort)
    acquire_token(timeout=5.0)
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    try:
        resp = _session.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception as e:
        logger.exception("Network send error")
        increment_failure(chat_id)
        return None
    data = parse_telegram_json(resp)
    if data and data.get("ok"):
        # record sent
        try:
            mid = data["result"].get("message_id")
            if mid:
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
                              (chat_id, mid, now_ts()))
                    conn.commit()
        except Exception:
            logger.exception("record sent message failed")
        reset_failures(chat_id)
        return data["result"]
    else:
        increment_failure(chat_id)
        return None

def increment_failure(user_id: int):
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            if not row:
                c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at) VALUES (?, ?, ?)",
                          (user_id, 1, now_ts()))
                failures = 1
            else:
                failures = int(row[0] or 0) + 1
                c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ? WHERE user_id = ?",
                          (failures, now_ts(), user_id))
            conn.commit()
        if failures >= 6:
            notify_owners(f"⚠️ Repeated send failures for {user_id} ({failures}). Stopping their tasks. 🛑")
            cancel_active_task_for_user(user_id)
    except Exception:
        logger.exception("increment_failure error")

def reset_failures(user_id: int):
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM send_failures WHERE user_id = ?", (user_id,))
            conn.commit()
    except Exception:
        pass

# Broadcast one-shot sender (no retries)
def broadcast_send_raw(chat_id: int, text: str):
    if not TELEGRAM_API:
        return False, "no_token"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True}
    try:
        resp = _session.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception as e:
        logger.info("Broadcast network error to %s: %s", chat_id, e)
        return False, str(e)
    data = parse_telegram_json(resp)
    if data and data.get("ok"):
        try:
            mid = data["result"].get("message_id")
            if mid:
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
                              (chat_id, mid, now_ts()))
                    conn.commit()
        except Exception:
            pass
        return True, "ok"
    reason = data.get("description") if isinstance(data, dict) else "error"
    logger.info("Broadcast failed to %s: %s", chat_id, reason)
    return False, reason

# Maintenance Helpers
def broadcast_to_all_allowed(text: str):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM allowed_users")
        rows = c.fetchall()
    for r in rows:
        tid = r[0]
        # Check suspension to avoid duplicate maintenance notification
        if not is_suspended(tid):
            broadcast_send_raw(tid, text) # Use broadcast_send_raw (one-shot, no retry/failure tracking)

def cancel_all_tasks():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        # Cancel running, paused, and queued tasks
        c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE status IN ('queued','running','paused')", ("cancelled", now_ts()))
        conn.commit()
        count = c.rowcount
    return count

def start_maintenance():
    global _is_maintenance
    with _maintenance_lock:
        _is_maintenance = True
    stopped = cancel_all_tasks()
    msg = f"⚠️ Maintenance Started! 🛠️\n\nThe WordSplitter bot is undergoing scheduled maintenance and will be unavailable from *3:00 AM to 4:00 AM WAT*.\n\nWe *stopped* {stopped} pending tasks. Please try again after 4:00 AM WAT. Thank you for your patience! 🙏"
    broadcast_to_all_allowed(msg)
    logger.info("Maintenance started. All tasks cancelled.")

def end_maintenance():
    global _is_maintenance
    with _maintenance_lock:
        _is_maintenance = False
    msg = "✅ Maintenance Complete! 🥳\n\nThe WordSplitter bot is back online and ready to split your words. Thank you for your patience! 🎉"
    broadcast_to_all_allowed(msg)
    logger.info("Maintenance ended.")


# Task queue management
def split_text_to_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

def enqueue_task(user_id: int, username: str, text: str):
    words = split_text_to_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        pending = c.fetchone()[0]
        if pending >= MAX_QUEUE_PER_USER:
            return {"ok": False, "reason": "queue_full", "queue_size": pending}
        c.execute("INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                  (user_id, username, text, json.dumps(words), total, "queued", now_ts(), 0))
        conn.commit()
    return {"ok": True, "total_words": total, "queue_size": pending + 1}

def get_next_task_for_user(user_id: int):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        # The global worker only pulls 'queued' tasks
        c.execute("SELECT id, words_json, total_words, text FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1", (user_id,))
        r = c.fetchone()
    if not r:
        return None
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3]}

def set_task_status(task_id: int, status: str):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        if status == "running":
            c.execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, now_ts(), task_id))
        elif status in ("done", "cancelled"):
            c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, now_ts(), task_id))
        else:
            c.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
        conn.commit()

def cancel_active_task_for_user(user_id: int):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,))
        rows = c.fetchall()
        count = 0
        for r in rows:
            tid = r[0]
            c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", ("cancelled", now_ts(), tid))
            count += 1
        conn.commit()
    return count

def record_split_log(user_id: int, username: str, words: int):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)", (user_id, username, words, now_ts()))
        conn.commit()

# Allowed/admin/suspended checks
def is_allowed(user_id: int) -> bool:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
        return bool(c.fetchone())

def is_admin(user_id: int) -> bool:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return bool(r and r[0])

def suspend_user(target_id: int, seconds: int, reason: str = ""):
    until_utc_str = (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    until_wat_str = utc_to_wat_ts(until_utc_str)
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO suspended_users (user_id, suspended_until, reason, added_at) VALUES (?, ?, ?, ?)",
                  (target_id, until_utc_str, reason, now_ts()))
        conn.commit()
    stopped = cancel_active_task_for_user(target_id)
    try:
        send_message(target_id, f"⛔ You were suspended until *{until_wat_str}*.\nReason: {reason or '(none)'} 😔")
    except Exception:
        logger.exception("notify suspended user failed")
    notify_owners(f"⛔ User {target_id} suspended until {until_wat_str}. Stopped {stopped} tasks. Reason: {reason or '(none)'} 🛑")

def unsuspend_user(target_id: int) -> bool:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (target_id,))
        r = c.fetchone()
        if not r:
            return False
        c.execute("DELETE FROM suspended_users WHERE user_id = ?", (target_id,))
        conn.commit()
    try:
        send_message(target_id, "✅ Your suspension has been lifted. You may use the bot again. 🙂")
    except Exception:
        logger.exception("notify unsuspended failed")
    notify_owners(f"✅ User {target_id} unsuspended. 🎉")
    return True

def list_suspended():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, suspended_until, reason, added_at FROM suspended_users ORDER BY suspended_until ASC")
        return c.fetchall()

def is_suspended(user_id: int) -> bool:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        if not r:
            return False
        try:
            until = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
            return until > datetime.utcnow()
        except Exception:
            return False

# Worker to process user queues
_user_locks: Dict[int, threading.Lock] = {}
_user_locks_lock = threading.Lock()
_worker_stop = threading.Event()

def get_user_lock(uid: int):
    with _user_locks_lock:
        if uid not in _user_locks:
            _user_locks[uid] = threading.Lock()
        return _user_locks[uid]

def process_user_queue(user_id: int, chat_id: int, username: str):
    lock = get_user_lock(user_id)
    if not lock.acquire(blocking=False):
        return
    try:
        # Check suspension/maintenance immediately upon starting task thread
        if is_suspended(user_id):
            try:
                send_message(user_id, "⛔ You are suspended — tasks won't run until suspension ends. 😔")
            except Exception:
                pass
            return
        
        if is_maintenance_time():
            # If maintenance starts just as the task is about to run (after global worker pulled it)
            try:
                 send_message(user_id, "🚧 Maintenance in Progress! 🚧\n\nTask cannot start now (3:00 AM - 4:00 AM WAT). Please try again after 4:00 AM WAT. 🙏")
            except Exception:
                pass
            return

        while True:
            task = get_next_task_for_user(user_id)
            if not task:
                break
                
            # Re-check just in case suspension/maintenance was activated between task pull and lock acquire
            if is_suspended(user_id):
                send_message(user_id, "⛔ You are suspended. Tasks paused/cancelled. 🛑")
                set_task_status(task["id"], "cancelled") 
                break
            
            if is_maintenance_time():
                send_message(user_id, "🛑 Task stopped due to maintenance (3:00 AM - 4:00 AM WAT). Please try again after 4:00 AM WAT. 🙏")
                set_task_status(task["id"], "cancelled")
                break
            
            task_id = task["id"]
            words = task["words"]
            total = task["total_words"]
            set_task_status(task_id, "running")
            
            # Retrieve current sent count
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT sent_count FROM tasks WHERE id = ?", (task_id,))
                sent_info = c.fetchone()
            sent = int(sent_info[0] or 0) if sent_info else 0
            
            remaining = max(0, total - sent)
            interval = 0.4 if total <= 150 else (0.5 if total <= 300 else 0.6)
            est_seconds = int(remaining * interval)
            est_str = str(timedelta(seconds=est_seconds))
            send_message(chat_id, f"🚀 Starting split: *{total}* words. Est: *{est_str}*.")
            
            i = sent
            consecutive_failures = 0
            
            while i < total:
                
                # Check task status for external pause/cancel/suspension/maintenance
                row = None
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                    row = c.fetchone()
                if not row:
                    break
                status = row[0]
                
                # IMPORTANT: Priority Check for immediate termination
                if status == "cancelled":
                    break
                
                if is_suspended(user_id):
                    # Only notify if the task was running and is being forcibly stopped by suspension status change
                    send_message(chat_id, "⛔ Your task was stopped because your account was suspended. 🛑")
                    set_task_status(task_id, "cancelled")
                    break
                
                if is_maintenance_time():
                    # Only notify if the user is NOT suspended (Fix #3)
                    if not is_suspended(user_id):
                         send_message(chat_id, "🛑 Task stopped due to maintenance (3:00 AM - 4:00 AM WAT). Please try again after 4:00 AM WAT. 🙏")
                    set_task_status(task_id, "cancelled")
                    break # Break sending loop (Fix #2)

                # Pause handling (allows external /resume)
                if status == "paused":
                    send_message(chat_id, "⏸️ Paused. Use /resume to continue.")
                    
                    # Wait loop
                    while True:
                        time.sleep(0.5)
                        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn_check:
                            c_check = conn_check.cursor()
                            c_check.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                            row2 = c_check.fetchone()
                        
                        if not row2: break
                        new_status = row2[0]

                        # Handle termination events during pause
                        if new_status == "cancelled" or is_suspended(user_id) or is_maintenance_time():
                            if is_suspended(user_id):
                                set_task_status(task_id, "cancelled")
                                send_message(chat_id, "⛔ Task stopped because your account was suspended. 🛑")
                            elif is_maintenance_time():
                                set_task_status(task_id, "cancelled")
                                # Only notify if not suspended
                                if not is_suspended(user_id):
                                    send_message(chat_id, "🛑 Task stopped due to maintenance (3:00 AM - 4:00 AM WAT). 🙏")
                            break # Break inner wait loop and send loop

                        if new_status == "running":
                            send_message(chat_id, "▶️ Resuming.")
                            break

                    if new_status in ("cancelled", "paused"): # If it's still paused or terminated, break
                        break
                        
                # Send the word
                res = send_message(chat_id, words[i])
                if res is None:
                    consecutive_failures += 1
                    if consecutive_failures >= 4:
                        notify_owners(f"⚠️ Repeated send failures for {user_id}. Stopping tasks. 🛑")
                        cancel_active_task_for_user(user_id)
                        break
                else:
                    consecutive_failures = 0
                
                i += 1
                
                # IMMEDIATE UPDATE: Commit sent_count for immediate /status accuracy (Fix #1)
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("UPDATE tasks SET sent_count = ? WHERE id = ?", (i, task_id))
                    conn.commit()
                    
                time.sleep(interval)
            
            # finalize outside inner loop
            
            # Check status again to see if it was cancelled/stopped
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT status, sent_count FROM tasks WHERE id = ?", (task_id,))
                r = c.fetchone()
            
            final_status = r[0] if r else "done" 
            
            if final_status not in ("cancelled", "paused"): # If not manually stopped or paused, it's done.
                set_task_status(task_id, "done")
                record_split_log(user_id, username, i) # Record total words sent (i)
                send_message(chat_id, "✅ Done! Your text was split. ✨")
            elif final_status == "cancelled":
                send_message(chat_id, "🛑 Task stopped.")
            elif final_status == "paused":
                # Do nothing, message was already sent
                pass
            
            continue # Process next task
            
    finally:
        lock.release()

def global_worker():
    while not _worker_stop.is_set():
        if is_maintenance_time():
            time.sleep(1.0)
            continue
        try:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                # Only pull 'queued' tasks (the 'paused' tasks will wait until a user manually resumes them)
                c.execute("SELECT DISTINCT user_id, username FROM tasks WHERE status IN ('queued') ORDER BY created_at ASC")
                rows = c.fetchall()
            for r in rows:
                uid = r[0]
                uname = r[1] or ""
                if is_suspended(uid):
                    continue
                t = threading.Thread(target=process_user_queue, args=(uid, uid, uname), daemon=True)
                t.start()
            time.sleep(0.6)
        except Exception:
            logger.exception("global worker error")
            time.sleep(1.0)

threading.Thread(target=global_worker, daemon=True).start()

# Scheduler for hourly stats and suspension lifting
scheduler = BackgroundScheduler()

def compute_last_hour_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, username, SUM(words) as s FROM split_logs WHERE created_at >= ? GROUP BY user_id ORDER BY s DESC", (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
        rows = c.fetchall()
    return rows

def send_hourly_owner_stats():
    rows = compute_last_hour_stats()
    if not rows:
        # Removed "(based on UTC time)"
        msg = "⏰ Hourly Report: no splits in the last hour. 😴"
        for oid in OWNER_IDS:
            try:
                send_message(oid, msg)
            except Exception:
                pass
        return
    lines = []
    for r in rows:
        uid = r[0]
        uname = r[1] or ""
        w = int(r[2] or 0)
        part = f"{uid} ({uname}) - {w} words" if uname else f"{uid} - {w} words"
        lines.append(part)
    # Removed "(based on UTC time)"
    body = "⏰ Hourly Report (Last 1h): 📝\n" + "\n".join(lines)
    for oid in OWNER_IDS:
        try:
            send_message(oid, body)
        except Exception:
            pass

# Lift suspensions when due
def check_and_lift():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, suspended_until FROM suspended_users")
        rows = c.fetchall()
    now = datetime.utcnow()
    for r in rows:
        try:
            until = datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S")
            if until <= now:
                uid = r[0]
                unsuspend_user(uid)
        except Exception:
            logger.exception("suspend parse error for %s", r)

# Job registration (3:00 AM WAT is 2:00 AM UTC, 4:00 AM WAT is 3:00 AM UTC)
scheduler.add_job(send_hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10), timezone='UTC')
scheduler.add_job(check_and_lift, "interval", minutes=1, next_run_time=datetime.utcnow() + timedelta(seconds=15), timezone='UTC')
scheduler.add_job(start_maintenance, 'cron', hour=2, minute=0, timezone='UTC')
scheduler.add_job(end_maintenance, 'cron', hour=3, minute=0, timezone='UTC')
scheduler.start()

# Notify owners helper
def notify_owners(text: str):
    for oid in OWNER_IDS:
        try:
            send_message(oid, f"👑 (@justmemmy) {text}")
        except Exception:
            logger.exception("notify owner failed for %s", oid)

# Webhook endpoints
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False}), 400
    try:
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            uid = user.get("id")
            username = user.get("username") or (user.get("first_name") or "")
            text = msg.get("text") or ""
            if text.startswith("/"):
                parts = text.split(None, 1)
                cmd = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                return handle_command(uid, username, cmd, args)
            else:
                return handle_user_text(uid, username, text)
    except Exception:
        logger.exception("webhook handling error")
    return jsonify({"ok": True})

@app.route("/", methods=["GET"])
def root():
    return "WordSplitter running. 💚", 200

@app.route("/health", methods=["GET", "HEAD"])
def health():
    return jsonify({"ok": True, "ts": now_ts()}), 200

# Helper function
def get_queued_for_user(user_id: int) -> int:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        return c.fetchone()[0]

def handle_command(user_id: int, username: str, command: str, args: str):
    # /start allowed for anyone
    if command == "/start":
        msg = (
            f"👋 Hello {username or user_id}! 🤖\n\n"
            "I am WordSplitter Bot — I can split any text you send into single-word messages, "
            "making it easier to read or process. ✨\n\n"
            "Just send me a message, and I'll start splitting it word by word. 🚀\n"
            "You can also try /example to see a demo. 💡"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})
        
    # Check for maintenance mode
    if command != "/start" and is_maintenance_time():
        if user_id in OWNER_IDS: # Allow owners to use commands during maintenance
            send_message(user_id, "👑 You are in maintenance mode (3:00 AM - 4:00 AM WAT), but since you are the owner, commands are allowed. 🛠️")
            pass
        else:
            send_message(user_id, "🚧 Maintenance in Progress! 🚧\n\nThe bot is currently undergoing maintenance (3:00 AM - 4:00 AM WAT). Please try again after 4:00 AM WAT. 🙏")
            return jsonify({"ok": True})

    # require allowed for other commands
    if not is_allowed(user_id):
        # Added ❌ and (@justmemmy) prefix
        send_message(user_id, "❌ You are not allowed to use this bot. Owner (@justmemmy) has been notified. 🥺")
        notify_owners(f"Unallowed access attempt by {username or user_id} ({user_id}).")
        return jsonify({"ok": True})

    # User commands
    if command == "/example":
        # Updated sample text
        sample = ("447781515818\n447781515819\n447781515820\n447781515821\n"
                  "447781515822\n447781515823\n447781515824\n447781515825\n"
                  "447781515826\n447781515827")
        res = enqueue_task(user_id, username, sample)
        if not res["ok"]:
            send_message(user_id, "😔 Could not queue demo. Try later.")
            return jsonify({"ok": True})
        send_message(user_id, f"Demo queued — will split *{res['total_words']}* words. ✨")
        return jsonify({"ok": True})

    if command == "/pause":
        rows = None
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            send_message(user_id, "⏸️ No active task to pause.")
            return jsonify({"ok": True})
        set_task_status(rows[0], "paused")
        send_message(user_id, "⏸️ Paused. Use /resume to continue.")
        return jsonify({"ok": True})

    if command == "/resume":
        rows = None
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            send_message(user_id, "▶️ No paused task to resume.")
            return jsonify({"ok": True})
        set_task_status(rows[0], "running")
        send_message(user_id, "▶️ Resumed.")
        return jsonify({"ok": True})

    if command == "/status":
        if is_suspended(user_id):
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
                r = c.fetchone()
                until_utc = r[0] if r else "unknown"
                until_wat = utc_to_wat_ts(until_utc)
            send_message(user_id, f"⛔ Suspended until *{until_wat}*.")
            return jsonify({"ok": True})
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,))
            active = c.fetchone()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        if active:
            aid, status, total, sent = active
            remaining = int(total or 0) - int(sent or 0)
            # This remaining count is now immediate (Fix #1)
            send_message(user_id, f"⚙️ Status: {status.capitalize()}. Remaining: *{remaining}* words. 📝 Queue: {queued}")
        else:
            send_message(user_id, f"✅ No active tasks. 📝 Queue: {queued}")
        return jsonify({"ok": True})

    if command == "/stop":
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        stopped = cancel_active_task_for_user(user_id)
        if stopped > 0 or queued > 0:
            send_message(user_id, f"🛑 Stopped {stopped} active and cleared {queued} queued tasks.")
        else:
            send_message(user_id, "No active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stats":
        cutoff_utc = datetime.utcnow() - timedelta(hours=12)
        # Removed cutoff_wat variable and reference from message
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT SUM(words) FROM split_logs WHERE user_id = ? AND created_at >= ?", (user_id, cutoff_utc.strftime("%Y-%m-%d %H:%M:%S")))
            r = c.fetchone()
            words = int(r[0] or 0) if r else 0
        # Simplified message as requested
        send_message(user_id, f"📊 Last 12 hours: *{words}* words split. 🚀")
        return jsonify({"ok": True})

    if command == "/about":
        msg = (
            "🤖 WordSplitter Bot\n\n"
            "I take any text you send and split it into individual word messages, "
            "with controlled pacing so you can follow easily. ✨\n\n"
            "Useful for breaking down long messages for readability or analysis. 💡\n\n"
            "Admins can manage allowed users, suspend or resume users, and owners (@justmemmy) can broadcast messages. 👑"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})

    # Admin commands
    if command == "/adduser":
        if not is_admin(user_id):
            send_message(user_id, "❌ Admin only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "ℹ️ Usage: /adduser <id> [username]\nExample: /adduser 12345678")
            return jsonify({"ok": True})
        parts = re.split(r"[,\s]+", args.strip())
        added, already, invalid = [], [], []
        for p in parts:
            if not p:
                continue
            try:
                tid = int(p)
            except Exception:
                invalid.append(p)
                continue
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (tid,))
                if c.fetchone():
                    already.append(tid)
                    continue
                c.execute("INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)", (tid, "", now_ts(), 0))
                conn.commit()
            added.append(tid)
            try:
                send_message(tid, "You were added. Send text to start. ✨")
            except Exception:
                pass
        parts_msgs = []
        if added: parts_msgs.append("✅ Added: " + ", ".join(str(x) for x in added))
        if already: parts_msgs.append("ℹ️ Already allowed: " + ", ".join(str(x) for x in already))
        if invalid: parts_msgs.append("❌ Invalid: " + ", ".join(invalid))
        send_message(user_id, "Result: " + ("; ".join(parts_msgs) if parts_msgs else "No changes"))
        return jsonify({"ok": True})

    if command == "/listusers":
        if not is_admin(user_id):
            send_message(user_id, "❌ Admin only.")
            return jsonify({"ok": True})
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT user_id, username, is_admin, added_at FROM allowed_users ORDER BY added_at DESC")
            rows = c.fetchall()
        lines = []
        for r in rows:
            uid, uname, isadm, added_at_utc = r
            added_at_wat = utc_to_wat_ts(added_at_utc)
            lines.append(f"👥 {uid} ({uname or 'no-name'}) - {'👑 Admin' if isadm else '👤 User'} - added={added_at_wat}")
        send_message(user_id, "👥 Allowed users:\n" + ("\n".join(lines) if lines else "(none)"))
        return jsonify({"ok": True})

    # Owner-only commands
    if command == "/botinfo":
        if user_id not in OWNER_IDS:
            send_message(user_id, "👑 Owner only.")
            return jsonify({"ok": True})
        # gather info
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM allowed_users")
            total_allowed = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM suspended_users")
            total_suspended = c.fetchone()[0]
            c.execute("SELECT user_id, username, SUM(total_words - IFNULL(sent_count,0)) as remaining, COUNT(*) as active_count FROM tasks WHERE status IN ('running','paused') GROUP BY user_id ORDER BY remaining DESC")
            active_rows = c.fetchall()
            c.execute("SELECT user_id, COUNT(*) FROM tasks WHERE status = 'queued' GROUP BY user_id")
            queued_counts = {row[0]: row[1] for row in c.fetchall()} # Get all queued counts once (Fix: inefficiency)
            c.execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'")
            queued_tasks = c.fetchone()[0]
            # last 1h stats
            cutoff = datetime.utcnow() - timedelta(hours=1)
            c.execute("SELECT user_id, username, SUM(words) as s FROM split_logs WHERE created_at >= ? GROUP BY user_id ORDER BY s DESC", (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
            stats_rows = c.fetchall()
        lines_active = []
        for r in active_rows:
            uid, uname, rem, ac = r
            name = f" ({uname})" if uname else ""
            # Use the pre-fetched queued count map
            queued_for_user = queued_counts.get(uid, 0)
            lines_active.append(f"⚙️ {uid}{name} - {int(rem)} remaining - {int(ac)} active - {queued_for_user} queued")
        lines_stats = []
        for r in stats_rows:
            uid, uname, s = r
            name = f" ({uname})" if uname else ""
            lines_stats.append(f"📈 {uid}{name} - {int(s)} words")
        
        maintenance_status = "ON 🚧" if is_maintenance_time() else "OFF ✅"
        
        body = (
            "🟢 Bot Status\n"
            f"🛠️ Maintenance Mode: {maintenance_status} (3:00 AM - 4:00 AM WAT)\n"
            f"👥 Allowed users: {total_allowed}\n"
            f"⛔ Suspended users: {total_suspended}\n"
            f"⚙️ Active tasks: {len(active_rows)}\n" # Use len(active_rows) or total active count from separate query if needed
            f"📝 Queued tasks: {queued_tasks}\n\n"
            "👤 Users with active tasks:\n" + ("\n".join(lines_active) if lines_active else "(none)") + "\n\n"
            # Removed "UTC time"
            "📊 User Stats (Last 1h):\n" + ("\n".join(lines_stats) if lines_stats else "(none)")
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    if command == "/broadcast":
        if user_id not in OWNER_IDS:
            send_message(user_id, "👑 Owner only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "ℹ️ Usage: /broadcast <message>\nExample: /broadcast Hello everyone")
            return jsonify({"ok": True})
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM allowed_users")
            rows = c.fetchall()
        succeeded, failed = [], []
        header = f"📣 Broadcast from owner (@justmemmy):\n\n{args}"
        for r in rows:
            tid = r[0]
            ok, reason = broadcast_send_raw(tid, header)
            if ok:
                succeeded.append(tid)
            else:
                failed.append((tid, reason))
        summary = f"Broadcast done. ✅ Delivered: {len(succeeded)}. ❌ Failed: {len(failed)}."
        send_message(user_id, summary)
        if failed:
            notify_owners("Broadcast failures: " + ", ".join(f"{x[0]}({x[1]})" for x in failed))
        return jsonify({"ok": True})

    # Suspend / Unsuspend / Listsuspended
    if command == "/suspend":
        if not is_admin(user_id):
            send_message(user_id, "❌ Admin only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id,
                         "ℹ️ Usage: /suspend <user_id> <duration> [reason]\n"
                         "Examples:\n"
                         "/suspend 12345678 30s Spam\n"
                         "/suspend 9876543 5m Too many messages")
            return jsonify({"ok": True})
        parts = args.split(None, 2)
        try:
            target = int(parts[0])
        except Exception:
            send_message(user_id, "❌ Invalid user id.\nExample: /suspend 12345678 5m Spamming")
            return jsonify({"ok": True})
        if len(parts) < 2:
            send_message(user_id, "❌ Missing duration.\nExample: /suspend 12345678 5m Spamming")
            return jsonify({"ok": True})
        dur = parts[1]
        reason = parts[2] if len(parts) > 2 else ""
        m = re.match(r"^(\d+)(s|m|h|d)?$", dur)
        if not m:
            send_message(user_id, "❌ Invalid duration format.\nUse 30s, 5m, 3h, 2d.\nExample: /suspend 12345678 5m")
            return jsonify({"ok": True})
        val, unit = int(m.group(1)), (m.group(2) or "s")
        mul = {"s":1, "m":60, "h":3600, "d":86400}.get(unit,1)
        seconds = val * mul
        suspend_user(target, seconds, reason)
        send_message(user_id, f"✅ Suspended {target} for {val}{unit}.")
        return jsonify({"ok": True})

    if command == "/unsuspend":
        if not is_admin(user_id):
            send_message(user_id, "❌ Admin only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id,
                         "ℹ️ Usage: /unsuspend <user_id>\n"
                         "Example: /unsuspend 12345678")
            return jsonify({"ok": True})
        try:
            target = int(args.split()[0])
        except Exception:
            send_message(user_id, "❌ Invalid user id.\nExample: /unsuspend 12345678")
            return jsonify({"ok": True})
        ok = unsuspend_user(target)
        if ok:
            send_message(user_id, f"✅ Unsuspended {target}.")
        else:
            send_message(user_id, f"ℹ️ User {target} was not suspended.")
        return jsonify({"ok": True})

    if command == "/listsuspended":
        if not is_admin(user_id):
            send_message(user_id, "❌ Admin only.")
            return jsonify({"ok": True})
        rows = list_suspended()
        if not rows:
            send_message(user_id, "✅ No suspended users.")
            return jsonify({"ok": True})
        lines = []
        for r in rows:
            uid, until_utc, reason, added_at_utc = r
            until_wat = utc_to_wat_ts(until_utc)
            added_wat = utc_to_wat_ts(added_at_utc)
            lines.append(f"⛔ {uid} until={until_wat} reason={reason or '(none)'} added={added_wat}")
        send_message(user_id, "⛔ Suspended:\n" + "\n".join(lines))
        return jsonify({"ok": True})

    # Unknown
    send_message(user_id, "🤔 Unknown command.")
    return jsonify({"ok": True})

def handle_user_text(user_id: int, username: str, text: str):
    
    if is_maintenance_time():
        send_message(user_id, "🚧 Maintenance in Progress! 🚧\n\nThe bot is currently undergoing maintenance (3:00 AM - 4:00 AM WAT). Please try again after 4:00 AM WAT. 🙏")
        return jsonify({"ok": True})
        
    if not is_allowed(user_id):
        send_message(user_id, "❌ You are not allowed. Owner (@justmemmy) notified. 🥺")
        notify_owners(f"Unallowed access by {user_id}.")
        return jsonify({"ok": True})
    if is_suspended(user_id):
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
            r = c.fetchone()
            until_utc = r[0] if r else "unknown"
            until_wat = utc_to_wat_ts(until_utc)
        send_message(user_id, f"⛔ Suspended until *{until_wat}*.")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res["reason"] == "empty":
            send_message(user_id, "🧐 No words found. Send longer text.")
            return jsonify({"ok": True})
        if res["reason"] == "queue_full":
            send_message(user_id, f"🛑 Queue full ({res['queue_size']}). Use /stop.")
            return jsonify({"ok": True})
    running = None
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,))
        running = c.fetchone()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        queued = c.fetchone()[0]
    if running:
        send_message(user_id, f"📝 Queued. Position in queue: {queued}.")
    else:
        # Added star formatting to total_words and a nice emoji
        send_message(user_id, f"🚀 Task accepted — will split *{res['total_words']}* words. ✨")
    return jsonify({"ok": True})

# Webhook setup helper
def set_webhook():
    if not TELEGRAM_API or not WEBHOOK_URL:
        logger.info("Webhook not configured.")
        return
    try:
        _session.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
    except Exception:
        logger.exception("set_webhook failed")

if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        pass
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
