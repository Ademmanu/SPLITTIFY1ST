#!/usr/bin/env python3
"""
WordSplitter Telegram Bot - Optimized Complete Script

Features / notes:
- Per-user worker threads and queued word splitting.
- Max queue: 5 per user.
- Scheduled daily maintenance Nigeria time (2–3 AM WAT).
- Owners auto-added; ALLOWED_USERS env processed.
- Numeric IDs in messages are sent as code entities (monospace) to be copyable.
- Usernames are stored/displayed without a leading '@' (per last requested behavior).
- Owner tag: "Owner (@justmemmy)" left unchanged.
- Optimizations:
  - Thread-local SQLite connections with WAL, synchronous=NORMAL.
  - TokenBucket uses Condition for precise rate limiting.
  - Batch DB writes for split logs and sent_count updates.
  - Precise dynamic-send cadence using time.monotonic() and Event.wait(timeout).
  - Compiled regexes and fewer DB lock hold times.
- Includes /suspend <id> <duration> [reason] support.

This file is a complete standalone app.py.
"""

import os
import time
import json
import sqlite3
import threading
import logging
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wordsplitter")

app = Flask(__name__)

# Config from env
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
OWNER_IDS_RAW = os.environ.get("OWNER_IDS", "")      # comma/space separated IDs
ALLOWED_USERS_RAW = os.environ.get("ALLOWED_USERS", "")  # comma/space separated IDs
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "5"))
MAX_MSG_PER_SECOND = float(os.environ.get("MAX_MSG_PER_SECOND", "50"))
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None
_session = requests.Session()

# Constants & precompiled regexes
FLUSH_BATCH = 5
NUM_RE = re.compile(r"\b\d+\b")
SPLIT_WS_RE = re.compile(r"\s+")
DURATION_RE = re.compile(r"^(\d+)(s|m|h|d)?$")

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
PRIMARY_OWNER = OWNER_IDS[0] if OWNER_IDS else None
ALLOWED_USERS = parse_id_list(ALLOWED_USERS_RAW)

NIGERIA_TZ_OFFSET = timedelta(hours=1)
def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
def utc_to_wat_ts(utc_ts: str) -> str:
    try:
        utc_dt = datetime.strptime(utc_ts, "%Y-%m-%d %H:%M:%S")
        wat_dt = utc_dt + NIGERIA_TZ_OFFSET
        return wat_dt.strftime("%Y-%m-%d %H:%M:%S WAT")
    except Exception:
        return f"{utc_ts} (UTC error)"

# Formatting helpers (usernames displayed without '@' per current behaviour)
def at_username(u: Optional[str]) -> str:
    if not u:
        return ""
    return u.lstrip("@")

def label_for_self(viewer_id: int, username: Optional[str]) -> str:
    if username:
        if viewer_id in OWNER_IDS:
            return f"{at_username(username)} (ID: {viewer_id})"
        return f"{at_username(username)}"
    return f"(ID: {viewer_id})" if viewer_id in OWNER_IDS else ""

def label_for_owner_view(target_id: int, target_username: Optional[str]) -> str:
    if target_username:
        return f"{at_username(target_username)} (ID: {target_id})"
    return str(target_id)

OWNER_TAG = "Owner (@justmemmy)"

# Database: thread-local connections for performance
_db_lock = threading.Lock()
_thread_local = threading.local()

def get_db_conn():
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store = MEMORY;")
        except Exception:
            pass
        _thread_local.conn = conn
    return conn

def init_db():
    parent = os.path.dirname(os.path.abspath(DB_PATH))
    try:
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
    except Exception as e:
        logger.warning("Could not create DB parent directory %s: %s", parent, e)
    with _db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS allowed_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            added_at TEXT
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
        conn.close()
    logger.info("DB initialized at %s", DB_PATH)

init_db()

# Ensure owners auto-added
with _db_lock:
    conn0 = get_db_conn()
    c0 = conn0.cursor()
    for oid in OWNER_IDS:
        c0.execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", (oid, "", now_ts()))
    conn0.commit()

# Efficient token bucket using Condition to avoid busy waiting
class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0001, float(rate_per_sec))
        self.capacity = capacity if capacity is not None else max(1.0, float(rate_per_sec))
        self.tokens = float(self.capacity)
        self.last = time.monotonic()
        self.cond = threading.Condition()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last
        if elapsed <= 0:
            return
        add = elapsed * self.rate
        self.tokens = min(self.capacity, self.tokens + add)
        self.last = now

    def acquire(self, timeout=10.0) -> bool:
        deadline = time.monotonic() + timeout
        with self.cond:
            while True:
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                need = (1.0 - self.tokens) / self.rate
                wait = min(remaining, max(need, 0.01))
                self.cond.wait(wait)

_token_bucket = TokenBucket(MAX_MSG_PER_SECOND, max(1.0, MAX_MSG_PER_SECOND))

# Entity helpers - numeric tokens as code (UTF-16 offsets)
def _utf16_len(s: str) -> int:
    if not s:
        return 0
    return len(s.encode("utf-16-le")) // 2

def _build_entities_for_text(text: str):
    if not text:
        return None
    entities = []
    for m in NUM_RE.finditer(text):
        py_start = m.start()
        py_end = m.end()
        utf16_offset = _utf16_len(text[:py_start])
        utf16_length = _utf16_len(text[py_start:py_end])
        entities.append({"type": "code", "offset": utf16_offset, "length": utf16_length})
    return entities if entities else None

def parse_telegram_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

# Failure accounting
def increment_failure(user_id: int):
    try:
        conn = get_db_conn()
        c = conn.cursor()
        if user_id in OWNER_IDS:
            logger.warning("Send failure for owner %s; recording but not notifying owners to avoid recursion.", user_id)
            c.execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            if not row:
                c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at) VALUES (?, ?, ?)", (user_id, 1, now_ts()))
            else:
                failures = int(row[0] or 0) + 1
                c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ? WHERE user_id = ?", (failures, now_ts(), user_id))
            conn.commit()
            return
        c.execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if not row:
            c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at) VALUES (?, ?, ?)", (user_id, 1, now_ts()))
            failures = 1
        else:
            failures = int(row[0] or 0) + 1
            c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ? WHERE user_id = ?", (failures, now_ts(), user_id))
        conn.commit()
        if failures >= 6:
            notify_owners(f"⚠️ Repeated send failures for {user_id} ({failures}). Stopping their tasks. 🛑")
            cancel_active_task_for_user(user_id)
    except Exception:
        logger.exception("increment_failure error")

def reset_failures(user_id: int):
    try:
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("DELETE FROM send_failures WHERE user_id = ?", (user_id,))
        conn.commit()
    except Exception:
        pass

# Send message wrapper
def send_message(chat_id: int, text: str):
    if not TELEGRAM_API:
        logger.error("No TELEGRAM_TOKEN; cannot send message.")
        return None
    if not _token_bucket.acquire(timeout=5.0):
        logger.warning("Token bucket timeout while sending to %s", chat_id)
        increment_failure(chat_id)
        return None
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_entities_for_text(text)
    if entities:
        payload["entities"] = entities
    try:
        resp = _session.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception:
        logger.exception("Network send error")
        increment_failure(chat_id)
        return None
    data = parse_telegram_json(resp)
    if data and data.get("ok"):
        try:
            mid = data["result"].get("message_id")
            if mid:
                conn = get_db_conn()
                c = conn.cursor()
                c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)", (chat_id, mid, now_ts()))
                conn.commit()
        except Exception:
            logger.exception("record sent message failed")
        reset_failures(chat_id)
        return data["result"]
    else:
        increment_failure(chat_id)
        return None

# Broadcast helpers (admin)
def broadcast_send_raw(chat_id: int, text: str) -> Tuple[bool, str]:
    if not TELEGRAM_API:
        return False, "no_token"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_entities_for_text(text)
    if entities:
        payload["entities"] = entities
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
                conn = get_db_conn()
                c = conn.cursor()
                c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)", (chat_id, mid, now_ts()))
                conn.commit()
        except Exception:
            pass
        return True, "ok"
    reason = data.get("description") if isinstance(data, dict) else "error"
    logger.info("Broadcast failed to %s: %s", chat_id, reason)
    return False, reason

def broadcast_to_all_allowed(text: str):
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT user_id FROM allowed_users")
    rows = c.fetchall()
    for r in rows:
        tid = r[0]
        if not is_suspended(tid):
            broadcast_send_raw(tid, text)

# Task management
def cancel_all_tasks() -> int:
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE status IN ('queued','running','paused')", ("cancelled", now_ts()))
    conn.commit()
    return c.rowcount

_is_maintenance = False
_maintenance_lock = threading.Lock()
def is_maintenance_time() -> bool:
    with _maintenance_lock:
        return _is_maintenance

def start_maintenance():
    global _is_maintenance
    with _maintenance_lock:
        _is_maintenance = True
    stopped = cancel_all_tasks()
    broadcast_to_all_allowed("🛠️ Scheduled maintenance started (2:00 AM–3:00 AM WAT). Tasks are temporarily blocked. Please try later.")
    notify_owners(f"🔧 Automatic maintenance started. Bot tasks were blocked by {OWNER_TAG}.")
    logger.info("Maintenance started. All tasks cancelled: %s", stopped)

def end_maintenance():
    global _is_maintenance
    with _maintenance_lock:
        _is_maintenance = False
    broadcast_to_all_allowed("✅ Scheduled maintenance ended. Bot is now available for tasks.")
    notify_owners(f"✅ Automatic maintenance ended. Bot resumed under {OWNER_TAG}.")
    logger.info("Maintenance ended.")

def split_text_to_words(text: str) -> List[str]:
    return [w for w in SPLIT_WS_RE.split(text.strip()) if w]

def enqueue_task(user_id: int, username: str, text: str):
    if is_maintenance_time():
        return {"ok": False, "reason": "maintenance"}
    words = split_text_to_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
    pending = c.fetchone()[0]
    if pending >= MAX_QUEUE_PER_USER:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    try:
        c.execute("INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                  (user_id, username, text, json.dumps(words), total, "queued", now_ts(), 0))
        conn.commit()
    except Exception:
        logger.exception("enqueue_task db error")
        return {"ok": False, "reason": "db_error"}
    return {"ok": True, "total_words": total, "queue_size": pending + 1}

def get_next_task_for_user(user_id: int):
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT id, words_json, total_words, text FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1", (user_id,))
    r = c.fetchone()
    if not r:
        return None
    return {"id": r[0], "words": json.loads(r[1]) if r[1] else split_text_to_words(r[3]), "total_words": r[2], "text": r[3]}

def set_task_status(task_id: int, status: str):
    conn = get_db_conn()
    c = conn.cursor()
    if status == "running":
        c.execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, now_ts(), task_id))
    elif status in ("done", "cancelled"):
        c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, now_ts(), task_id))
    else:
        c.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
    conn.commit()

def cancel_active_task_for_user(user_id: int):
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,))
    rows = c.fetchall()
    count = 0
    for r in rows:
        tid = r[0]
        c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", ("cancelled", now_ts(), tid))
        count += 1
    conn.commit()
    notify_user_worker(user_id)
    return count

def record_split_log(user_id: int, username: str, count: int = 1):
    try:
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)", (user_id, username, int(count), now_ts()))
        conn.commit()
    except Exception:
        logger.exception("record_split_log error")

def is_allowed(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
    return bool(c.fetchone())

def suspend_user(target_id: int, seconds: int, reason: str = ""):
    until_utc_str = (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    until_wat_str = utc_to_wat_ts(until_utc_str)
    try:
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO suspended_users (user_id, suspended_until, reason, added_at) VALUES (?, ?, ?, ?)",
                  (target_id, until_utc_str, reason, now_ts()))
        conn.commit()
    except Exception:
        logger.exception("suspend_user db error")
    stopped = cancel_active_task_for_user(target_id)
    try:
        reason_text = f"\nReason: {reason}" if reason else ""
        send_message(target_id, f"⛔ You have been suspended until {until_wat_str} by {OWNER_TAG}.{reason_text}")
    except Exception:
        logger.exception("notify suspended user failed")
    notify_owners(f"🔒 User suspended: {label_for_owner_view(target_id, fetch_display_username(target_id))} suspended_until={until_wat_str} by {OWNER_TAG} reason={reason}")

def unsuspend_user(target_id: int) -> bool:
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (target_id,))
    r = c.fetchone()
    if not r:
        return False
    c.execute("DELETE FROM suspended_users WHERE user_id = ?", (target_id,))
    conn.commit()
    try:
        send_message(target_id, f"✅ You have been unsuspended by {OWNER_TAG}.")
    except Exception:
        logger.exception("notify unsuspended failed")
    notify_owners(f"🔓 Manual unsuspend: {label_for_owner_view(target_id, fetch_display_username(target_id))} by {OWNER_TAG}.")
    return True

def list_suspended():
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT user_id, suspended_until, reason, added_at FROM suspended_users ORDER BY suspended_until ASC")
    return c.fetchall()

def is_suspended(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return False
    conn = get_db_conn()
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

def notify_owners(text: str):
    # simple notify; avoid leaking tokens in future if needed
    for oid in OWNER_IDS:
        try:
            send_message(oid, text)
        except Exception:
            logger.exception("notify owner failed for %s", oid)

# Worker control
_user_workers_lock = threading.Lock()
_user_workers: Dict[int, Dict[str, object]] = {}

def notify_user_worker(user_id: int):
    with _user_workers_lock:
        info = _user_workers.get(user_id)
        if info and "wake" in info:
            try:
                info["wake"].set()
            except Exception:
                pass

def start_user_worker_if_needed(user_id: int):
    with _user_workers_lock:
        info = _user_workers.get(user_id)
        if info:
            thr = info.get("thread")
            if thr and thr.is_alive():
                return
        wake = threading.Event()
        stop = threading.Event()
        thr = threading.Thread(target=per_user_worker_loop, args=(user_id, wake, stop), daemon=True)
        _user_workers[user_id] = {"thread": thr, "wake": wake, "stop": stop}
        thr.start()
        logger.info("Started worker for user %s", user_id)

def stop_user_worker(user_id: int, join_timeout: float = 0.5):
    with _user_workers_lock:
        info = _user_workers.get(user_id)
        if not info:
            return
        try:
            info["stop"].set()
            info["wake"].set()
            thr = info.get("thread")
            if thr and thr.is_alive():
                thr.join(join_timeout)
        except Exception:
            logger.exception("Error stopping worker for %s", user_id)
        finally:
            _user_workers.pop(user_id, None)
            logger.info("Stopped worker for user %s", user_id)

def per_user_worker_loop(user_id: int, wake_event: threading.Event, stop_event: threading.Event):
    logger.info("Worker loop starting for user %s", user_id)
    try:
        while not stop_event.is_set():
            if is_maintenance_time():
                cancel_active_task_for_user(user_id)
                while is_maintenance_time() and not stop_event.is_set():
                    wake_event.wait(timeout=3.0)
                    wake_event.clear()
                continue
            if is_suspended(user_id):
                cancel_active_task_for_user(user_id)
                try:
                    send_message(user_id, "⛔ You have been suspended; stopping your task.")
                except Exception:
                    pass
                while is_suspended(user_id) and not stop_event.is_set():
                    wake_event.wait(timeout=5.0)
                    wake_event.clear()
                continue

            task = get_next_task_for_user(user_id)
            if not task:
                wake_event.wait(timeout=1.0)
                wake_event.clear()
                continue

            task_id = task["id"]
            words = task["words"]
            total = int(task["total_words"] or len(words))
            set_task_status(task_id, "running")
            conn = get_db_conn()
            c = conn.cursor()
            c.execute("SELECT sent_count FROM tasks WHERE id = ?", (task_id,))
            sent_info = c.fetchone()
            sent = int(sent_info[0] or 0) if sent_info else 0

            interval = 0.4 if total <= 150 else (0.5 if total <= 300 else 0.6)
            est_seconds = int((total - sent) * interval)
            est_str = str(timedelta(seconds=est_seconds))
            try:
                send_message(user_id, f"🚀 Starting your split now. Words: {total}. Estimated time: {est_str}")
            except Exception:
                pass

            i = sent
            batch_count = 0
            uname_for_stat = ""
            with _db_lock:
                cur = get_db_conn().cursor()
                cur.execute("SELECT username FROM allowed_users WHERE user_id = ?", (user_id,))
                r = cur.fetchone()
                uname_for_stat = r[0] if r and r[0] else ""

            last_send_time = time.monotonic()
            while i < total and not stop_event.is_set():
                # status check
                with _db_lock:
                    chk = get_db_conn().cursor()
                    chk.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                    st = chk.fetchone()
                if not st:
                    break
                status = st[0]
                if status == "cancelled":
                    break
                if is_suspended(user_id):
                    try:
                        send_message(user_id, "⛔ You have been suspended; stopping your task.")
                    except Exception:
                        pass
                    set_task_status(task_id, "cancelled")
                    break
                if is_maintenance_time():
                    try:
                        send_message(user_id, "🛠️ Your task stopped due to scheduled maintenance.")
                    except Exception:
                        pass
                    set_task_status(task_id, "cancelled")
                    break
                if status == "paused":
                    try:
                        send_message(user_id, "⏸️ Task paused…")
                    except Exception:
                        pass
                    while True:
                        wake_event.wait(timeout=0.7)
                        wake_event.clear()
                        if stop_event.is_set():
                            break
                        with _db_lock:
                            chk2 = get_db_conn().cursor()
                            chk2.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                            row2 = chk2.fetchone()
                        if not row2:
                            break
                        new_status = row2[0]
                        if new_status in ("cancelled",) or is_suspended(user_id) or is_maintenance_time():
                            break
                        if new_status == "running":
                            try:
                                send_message(user_id, "▶️ Resuming your task now.")
                            except Exception:
                                pass
                            break
                    with _db_lock:
                        chk3 = get_db_conn().cursor()
                        chk3.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                        rr = chk3.fetchone()
                    if not rr or rr[0] in ("cancelled", "paused"):
                        break

                # precise cadence
                now = time.monotonic()
                target_time = last_send_time + interval
                wait_time = target_time - now
                if wait_time > 0:
                    woke = wake_event.wait(timeout=wait_time)
                    wake_event.clear()
                    if woke:
                        last_send_time = time.monotonic()
                        continue

                try:
                    send_message(user_id, words[i])
                except Exception:
                    pass

                batch_count += 1
                i += 1
                last_send_time = time.monotonic()

                if batch_count >= FLUSH_BATCH:
                    try:
                        record_split_log(user_id, uname_for_stat or str(user_id), batch_count)
                        conn = get_db_conn()
                        curu = conn.cursor()
                        curu.execute("UPDATE tasks SET sent_count = ? WHERE id = ?", (i, task_id))
                        conn.commit()
                    except Exception:
                        logger.exception("Failed to flush batch for task %s", task_id)
                    batch_count = 0

                if is_maintenance_time() or is_suspended(user_id):
                    break

            # flush remainder
            if batch_count > 0:
                try:
                    record_split_log(user_id, uname_for_stat or str(user_id), batch_count)
                    conn = get_db_conn()
                    curu = conn.cursor()
                    curu.execute("UPDATE tasks SET sent_count = ? WHERE id = ?", (i, task_id))
                    conn.commit()
                except Exception:
                    logger.exception("Failed to flush final batch for task %s", task_id)

            with _db_lock:
                cfin = get_db_conn().cursor()
                cfin.execute("SELECT status, sent_count FROM tasks WHERE id = ?", (task_id,))
                rf = cfin.fetchone()
            final_status = rf[0] if rf else "done"
            if final_status not in ("cancelled", "paused"):
                set_task_status(task_id, "done")
                try:
                    send_message(user_id, "✅ All done!")
                except Exception:
                    pass
            elif final_status == "cancelled":
                try:
                    send_message(user_id, "🛑 Task stopped.")
                except Exception:
                    pass

    except Exception:
        logger.exception("Worker error for user %s", user_id)
    finally:
        with _user_workers_lock:
            _user_workers.pop(user_id, None)
        logger.info("Worker loop exiting for user %s", user_id)

def fetch_display_username(user_id: int) -> str:
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT username FROM split_logs WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
    r = c.fetchone()
    if r and r[0]:
        return r[0]
    c.execute("SELECT username FROM allowed_users WHERE user_id = ?", (user_id,))
    r2 = c.fetchone()
    if r2 and r2[0]:
        return r2[0]
    return ""

def compute_last_hour_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("""
        SELECT user_id, username, SUM(words) as s
        FROM split_logs
        WHERE created_at >= ?
        GROUP BY user_id, username
        ORDER BY s DESC
    """, (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
    rows = c.fetchall()
    stat_map = {}
    for uid, uname, s in rows:
        stat_map[uid] = {"uname": uname, "words": int(s or 0)}
    return [(k, v["uname"], v["words"]) for k, v in stat_map.items()]

def compute_last_12h_stats(user_id: int):
    cutoff = datetime.utcnow() - timedelta(hours=12)
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT SUM(words) FROM split_logs WHERE user_id = ? AND created_at >= ?", (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
    r = c.fetchone()
    return int(r[0] or 0)

def send_hourly_owner_stats():
    rows = compute_last_hour_stats()
    if not rows:
        msg = "📊 Hourly Report: no splits in the last hour."
        for oid in OWNER_IDS:
            try:
                send_message(oid, msg)
            except Exception:
                pass
        return
    lines = []
    for uid, uname, w in rows:
        uname_for_stat = at_username(uname) if uname else fetch_display_username(uid)
        lines.append(f"{uid} ({uname_for_stat}) - {w} words sent")
    body = "📊 Report - last 1h:\n" + "\n".join(lines)
    for oid in OWNER_IDS:
        try:
            send_message(oid, body)
        except Exception:
            pass

def check_and_lift():
    conn = get_db_conn()
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

# Scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(send_hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10), timezone='UTC')
scheduler.add_job(check_and_lift, "interval", minutes=1, next_run_time=datetime.utcnow() + timedelta(seconds=15), timezone='UTC')
scheduler.add_job(start_maintenance, 'cron', hour=1, minute=0, timezone='UTC')
scheduler.add_job(end_maintenance, 'cron', hour=2, minute=0, timezone='UTC')
scheduler.start()

# Command handlers
def handle_command(user_id: int, username: str, command: str, args: str):
    def is_owner(u): return u in OWNER_IDS

    # /start
    if command == "/start":
        who = label_for_self(user_id, username) or "there"
        msg = (
            f"👋 Hi {who}!\n\n"
            "I split your text into individual word messages. ✂️📤\n\n"
            f"{OWNER_TAG} commands:\n"
            " /adduser /listusers /listsuspended /botinfo /broadcast /suspend /unsuspend\n\n"
            "User commands:\n"
            " /start /example /pause /resume /status /stop /stats /about\n\n"
            "Just send any text and I'll split it for you. 🚀"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})

    # /about
    if command == "/about":
        msg = (
            "ℹ️ About:\n"
            "I split texts into single words. ✂️\n\n"
            "Features:\n"
            "queueing, pause/resume, scheduled maintenance (2AM–3AM),\n"
            "hourly owner stats, rate-limited sending. ⚖️"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})

    # maintenance block
    if command != "/start" and is_maintenance_time() and not is_owner(user_id):
        send_message(user_id, "🛠️ Scheduled maintenance in progress. Tasks are temporarily blocked. Please try later.")
        return jsonify({"ok": True})

    # allowed check
    if user_id not in OWNER_IDS and not is_allowed(user_id):
        # user sees their numeric ID here (per prior behavior)
        send_message(user_id, f"🚫 Sorry, you are not allowed. {OWNER_TAG} notified.\nYour ID: {user_id}")
        notify_owners(f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})

    # /example
    if command == "/example":
        sample = "\n".join([
            "996770061141", "996770064514", "996770071665", "996770073284",
            "996770075145", "996770075627", "996770075973", "996770076350",
            "996770076869", "996770077101"
        ])
        res = enqueue_task(user_id, username, sample)
        if not res["ok"]:
            if res.get("reason") == "maintenance":
                send_message(user_id, "🛠️ Scheduled maintenance in progress. Try later.")
                return jsonify({"ok": True})
            send_message(user_id, "❗ Could not queue demo. Try later.")
            return jsonify({"ok": True})
        start_user_worker_if_needed(user_id)
        notify_user_worker(user_id)
        active, queued = get_user_task_counts(user_id)
        if active:
            send_message(user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
        else:
            send_message(user_id, f"✅ Task added. Words: {res['total_words']}.")
        return jsonify({"ok": True})

    # /pause
    if command == "/pause":
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,))
        rows = c.fetchone()
        if not rows:
            send_message(user_id, "ℹ️ No active task to pause.")
            return jsonify({"ok": True})
        set_task_status(rows[0], "paused")
        notify_user_worker(user_id)
        send_message(user_id, "⏸️ Paused. Use /resume to continue.")
        return jsonify({"ok": True})

    # /resume
    if command == "/resume":
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,))
        rows = c.fetchone()
        if not rows:
            send_message(user_id, "ℹ️ No paused task to resume.")
            return jsonify({"ok": True})
        set_task_status(rows[0], "running")
        notify_user_worker(user_id)
        send_message(user_id, "▶️ Resuming your task now.")
        return jsonify({"ok": True})

    # /status
    if command == "/status":
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,))
        active = c.fetchone()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        queued = c.fetchone()[0]
        if active:
            aid, status, total, sent = active
            remaining = int(total or 0) - int(sent or 0)
            send_message(user_id, f"ℹ️ Status: {status}\nRemaining words: {remaining}\nQueue size: {queued}")
        elif queued > 0:
            send_message(user_id, f"⏳ Waiting. Queue size: {queued}")
        else:
            send_message(user_id, "✅ You have no active or queued tasks.")
        return jsonify({"ok": True})

    # /stop
    if command == "/stop":
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        queued = c.fetchone()[0]
        stopped = cancel_active_task_for_user(user_id)
        stop_user_worker(user_id)
        if stopped > 0 or queued > 0:
            send_message(user_id, "🛑 Active task stopped. Your queued tasks were cleared too.")
        else:
            send_message(user_id, "ℹ️ You had no active or queued tasks.")
        return jsonify({"ok": True})

    # /stats
    if command == "/stats":
        words = compute_last_12h_stats(user_id)
        send_message(user_id, f"📊 Your last 12 hours: {words} words split")
        return jsonify({"ok": True})

    # /adduser
    if command == "/adduser":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /adduser <user_id> [username]")
            return jsonify({"ok": True})
        parts = re.split(r"[,\s]+", args.strip())
        added, already, invalid = [], [], []
        conn = get_db_conn()
        c = conn.cursor()
        for p in parts:
            if not p:
                continue
            try:
                tid = int(p)
            except Exception:
                invalid.append(p)
                continue
            c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (tid,))
            if c.fetchone():
                already.append(tid)
                continue
            c.execute("INSERT INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", (tid, "", now_ts()))
            conn.commit()
            added.append(tid)
            try:
                send_message(tid, f"✅ You have been added. Send any text to start.")
            except Exception:
                pass
        parts_msgs = []
        if added: parts_msgs.append("Added: " + ", ".join(str(x) for x in added))
        if already: parts_msgs.append("Already present: " + ", ".join(str(x) for x in already))
        if invalid: parts_msgs.append("Invalid: " + ", ".join(invalid))
        send_message(user_id, "✅ " + ("; ".join(parts_msgs) if parts_msgs else "No changes"))
        return jsonify({"ok": True})

    # /listusers
    if command == "/listusers":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC")
        rows = c.fetchall()
        lines = []
        for r in rows:
            uid, uname, added_at_utc = r
            uname_s = f"({at_username(uname)})" if uname else "(no username)"
            added_at_wat = utc_to_wat_ts(added_at_utc)
            lines.append(f"{uid} {uname_s} added={added_at_wat}")
        send_message(user_id, "👥 Allowed users:\n" + ("\n".join(lines) if lines else "(none)"))
        return jsonify({"ok": True})

    # /listsuspended
    if command == "/listsuspended":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        # auto-lift expired
        for row in list_suspended()[:]:
            uid, until_utc, reason, added_at_utc = row
            until_dt = datetime.strptime(until_utc, "%Y-%m-%d %H:%M:%S")
            if until_dt <= datetime.utcnow():
                unsuspend_user(uid)
        rows = list_suspended()
        if not rows:
            send_message(user_id, "✅ No suspended users.")
            return jsonify({"ok": True})
        lines = []
        for r in rows:
            uid, until_utc, reason, added_at_utc = r
            until_wat = utc_to_wat_ts(until_utc)
            added_wat = utc_to_wat_ts(added_at_utc)
            uname = fetch_display_username(uid)
            uname_s = f"({at_username(uname)})" if uname else ""
            lines.append(f"{uid} {uname_s} suspended_until={until_wat} by={OWNER_TAG} reason={reason}")
        send_message(user_id, "🚫 Suspended users:\n" + "\n".join(lines))
        return jsonify({"ok": True})

    # /botinfo
    if command == "/botinfo":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT user_id, username, SUM(total_words - IFNULL(sent_count,0)) as remaining, COUNT(*) as active_count FROM tasks WHERE status IN ('running','paused') GROUP BY user_id")
        active_rows = c.fetchall()
        c.execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'")
        queued_tasks = c.fetchone()[0]
        queued_counts = {}
        c.execute("SELECT user_id, COUNT(*) FROM tasks WHERE status = 'queued' GROUP BY user_id")
        for row in c.fetchall():
            queued_counts[row[0]] = row[1]
        stats_rows = compute_last_hour_stats()
        lines_active = []
        for r in active_rows:
            uid, uname, rem, ac = r
            if not uname:
                uname = fetch_display_username(uid)
            name = f" ({at_username(uname)})" if uname else ""
            queued_for_user = queued_counts.get(uid, 0)
            lines_active.append(f"{uid}{name} - {int(rem)} remaining - {int(ac)} active - {queued_for_user} queued")
        lines_stats = []
        for uid, uname, s in stats_rows:
            uname_final = at_username(uname) if uname else fetch_display_username(uid)
            lines_stats.append(f"{uid} ({uname_final}) - {int(s)} words sent")
        c.execute("SELECT COUNT(*) FROM allowed_users")
        total_allowed = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM suspended_users")
        total_suspended = c.fetchone()[0]
        maintenance_status = "ON" if is_maintenance_time() else "OFF"
        body = (
            f"🤖 Bot status: Online\n"
            f"🛠️ Maintenance: {maintenance_status}\n"
            f"👥 Allowed users: {total_allowed}\n"
            f"🚫 Suspended users: {total_suspended}\n"
            f"⚙️ Active tasks: {len(active_rows)}\n"
            f"📨 Queued tasks: {queued_tasks}\n\n"
            "Users with active tasks:\n" + ("\n".join(lines_active) if lines_active else "(none)") + "\n\n"
            "User stats (last 1h):\n" + ("\n".join(lines_stats) if lines_stats else "(none)")
        )
        send_message(user_id, body)
        return jsonify({"ok": True})

    # /broadcast
    if command == "/broadcast":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /broadcast <message>")
            return jsonify({"ok": True})
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT user_id FROM allowed_users")
        rows = c.fetchall()
        succeeded, failed = [], []
        header = f"📣 Broadcast from {OWNER_TAG}:\n\n{args}"
        for r in rows:
            tid = r[0]
            ok, reason = broadcast_send_raw(tid, header)
            if ok:
                succeeded.append(tid)
            else:
                failed.append((tid, reason))
        summary = f"📨 Broadcast done. Success: {len(succeeded)}, Failed: {len(failed)}"
        send_message(user_id, summary)
        if failed:
            notify_owners("⚠️ Broadcast failures: " + ", ".join(f"{x[0]}({x[1]})" for x in failed))
        return jsonify({"ok": True})

    # /suspend with reason parsing
    if command == "/suspend":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /suspend <telegram_user_id> <duration> [reason]\nExample: /suspend 8282747479 30s Too many requests")
            return jsonify({"ok": True})
        parts = args.split()
        try:
            target = int(parts[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return jsonify({"ok": True})
        if len(parts) < 2:
            send_message(user_id, "Missing duration.")
            return jsonify({"ok": True})
        dur = parts[1]
        reason = " ".join(parts[2:]) if len(parts) > 2 else ""
        m = DURATION_RE.match(dur)
        if not m:
            send_message(user_id, "Invalid duration format. Examples: 30s 10m 2h 1d")
            return jsonify({"ok": True})
        val, unit = int(m.group(1)), (m.group(2) or "s")
        mul = {"s":1, "m":60, "h":3600, "d":86400}.get(unit,1)
        seconds = val * mul
        suspend_user(target, seconds, reason)
        reason_part = f" Reason: {reason}" if reason else ""
        send_message(user_id, f"🔒 User suspended until {utc_to_wat_ts((datetime.utcnow() + timedelta(seconds=seconds)).strftime('%Y-%m-%d %H:%M:%S'))}.{reason_part}")
        return jsonify({"ok": True})

    # /unsuspend
    if command == "/unsuspend":
        if not is_owner(user_id):
            send_message(user_id, f"🔒 {OWNER_TAG} only.")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, "Usage: /unsuspend <telegram_user_id>")
            return jsonify({"ok": True})
        try:
            target = int(args.split()[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return jsonify({"ok": True})
        ok = unsuspend_user(target)
        if ok:
            send_message(user_id, f"✅ User unsuspended.")
        else:
            send_message(user_id, f"ℹ️ User {target} is not suspended.")
        return jsonify({"ok": True})

    # fallback
    send_message(user_id, "❓ Unknown command.")
    return jsonify({"ok": True})

def handle_user_text(user_id: int, username: str, text: str):
    if is_maintenance_time():
        send_message(user_id, "🛠️ Scheduled maintenance in progress. Tasks are temporarily blocked. Please try later.")
        return jsonify({"ok": True})
    if user_id not in OWNER_IDS and not is_allowed(user_id):
        send_message(user_id, f"🚫 Sorry, you are not allowed. {OWNER_TAG} notified.\nYour ID: {user_id}")
        notify_owners(f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})
    if is_suspended(user_id):
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        until_utc = r[0] if r else "unknown"
        until_wat = utc_to_wat_ts(until_utc)
        send_message(user_id, f"⛔ You have been suspended until {until_wat} by {OWNER_TAG}.")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res.get("reason") == "maintenance":
            send_message(user_id, "🛠️ Scheduled maintenance in progress. Try later.")
            return jsonify({"ok": True})
        if res["reason"] == "empty":
            send_message(user_id, "⚠️ Empty text. Nothing to split.")
            return jsonify({"ok": True})
        if res["reason"] == "queue_full":
            send_message(user_id, f"⏳ Your queue is full ({res['queue_size']}). Use /stop or wait.")
            return jsonify({"ok": True})
        send_message(user_id, "❗ Could not queue task. Try later.")
        return jsonify({"ok": True})
    start_user_worker_if_needed(user_id)
    notify_user_worker(user_id)
    active, queued = get_user_task_counts(user_id)
    if active:
        send_message(user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
    else:
        send_message(user_id, f"✅ Task added. Words: {res['total_words']}.")
    return jsonify({"ok": True})

# Webhook, health endpoints
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
            # Ensure allowed_users row exists and update username quickly
            try:
                conn = get_db_conn()
                c = conn.cursor()
                c.execute("INSERT OR IGNORE INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", (uid, username or "", now_ts()))
                c.execute("UPDATE allowed_users SET username = ? WHERE user_id = ?", (username or "", uid))
                conn.commit()
            except Exception:
                logger.exception("webhook: ensure allowed_users row failed")
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
    return "WordSplitter running.", 200

@app.route("/health", methods=["GET", "HEAD"])
def health():
    return jsonify({"ok": True, "ts": now_ts()}), 200

def get_user_task_counts(user_id: int) -> Tuple[int, int]:
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,))
    active = int(c.fetchone()[0] or 0)
    c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
    queued = int(c.fetchone()[0] or 0)
    return active, queued

if __name__ == "__main__":
    try:
        # set webhook if present
        if TELEGRAM_API and WEBHOOK_URL:
            try:
                _session.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
            except Exception:
                logger.exception("set_webhook failed")
    except Exception:
        pass
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
