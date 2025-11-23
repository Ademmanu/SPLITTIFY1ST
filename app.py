#!/usr/bin/env python3
"""
Simplified Telegram Word-Splitter Bot - app.py (trimmed)

This version keeps the core functionality and the recent improvements:
- Controlled handling of Telegram 429 (global capped tele-pause, non-blocking).
- robust_send_message wrapper (configurable attempts, short backoff/jitter).
- Broadcast is idempotent per-run and attempts each recipient exactly once;
  failures are recorded and reported to the initiating owner and owners.
- Per-user queue worker that sends words and updates sent_count only on confirmed sends.
The file is intentionally trimmed: non-essential helpers, extra logs,
and advanced metrics were removed for clarity.
"""
import os
import time
import json
import sqlite3
import threading
import logging
import re
import uuid
import hashlib
from datetime import datetime, timedelta
from typing import List
from flask import Flask, request, jsonify
import requests
import random

# Basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# App and config
app = Flask(__name__)
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
_owner_ids_raw = os.environ.get("OWNER_IDS") or os.environ.get("OWNER_ID", "0")
_owner_ids_list = [p.strip() for p in re.split(r"[,\s]+", _owner_ids_raw) if p and p.strip().isdigit()]
OWNERS = set(int(p) for p in _owner_ids_list) if _owner_ids_list else set()
OWNER_ID = next(iter(OWNERS)) if OWNERS else 0
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))

# rate & retry defaults
_raw_max_msg_per_second = float(os.environ.get("MAX_MSG_PER_SECOND", "50"))
MAX_MSG_PER_SECOND = max(50.0, _raw_max_msg_per_second)
TG_CALL_MAX_RETRIES = int(os.environ.get("TG_CALL_MAX_RETRIES", "5"))
TG_CALL_MAX_BACKOFF = float(os.environ.get("TG_CALL_MAX_BACKOFF", "60"))
BROADCAST_MAX_ATTEMPTS = int(os.environ.get("BROADCAST_MAX_ATTEMPTS", "1"))  # we will attempt each recipient once in broadcast
SPLIT_MAX_ATTEMPTS = int(os.environ.get("SPLIT_MAX_ATTEMPTS", "3"))
ROBUST_SEND_BASE_BACKOFF = float(os.environ.get("ROBUST_SEND_BASE_BACKOFF", "1.0"))
TELE_PAUSE_MAX_SECONDS = int(os.environ.get("TELE_PAUSE_MAX_SECONDS", "60"))

if not TELEGRAM_TOKEN or not WEBHOOK_URL or not OWNER_ID:
    logger.warning("TELEGRAM_TOKEN, WEBHOOK_URL, OWNER_ID should be set")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None
_session = requests.Session()

# DB lock
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
            CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sent_at TEXT,
                deleted INTEGER DEFAULT 0
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
            CREATE TABLE IF NOT EXISTS suspended_users (
                user_id INTEGER PRIMARY KEY,
                suspended_until TEXT,
                reason TEXT,
                added_by INTEGER,
                added_at TEXT
            )""")
        c.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_runs (
                run_id TEXT PRIMARY KEY,
                message_hash TEXT,
                message TEXT,
                created_by INTEGER,
                created_at TEXT
            )""")
        c.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_recipients (
                run_id TEXT,
                user_id INTEGER,
                status TEXT DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                last_attempt TEXT,
                PRIMARY KEY (run_id, user_id)
            )""")
        conn.commit()

init_db()

def db_execute(query, params=(), fetch=False, retries=6):
    attempt = 0
    delay = 0.05
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
            if attempt >= retries:
                logger.exception("DB locked after retries: %s; query=%s params=%s", e, query, params)
                raise
            time.sleep(delay)
            delay = min(delay * 2, 1.0)

# Token bucket limiter
_token_bucket = {"tokens": MAX_MSG_PER_SECOND, "last": time.time(), "lock": threading.Lock(), "capacity": max(1.0, MAX_MSG_PER_SECOND)}

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

# Global tele-pause state
_tele_pause_lock = threading.Lock()
_tele_pause_until = 0.0

def set_tele_pause(seconds: float, reason: str = ""):
    global _tele_pause_until
    if not seconds or seconds <= 0:
        return
    cap = min(float(seconds), float(TELE_PAUSE_MAX_SECONDS))
    with _tele_pause_lock:
        new_until = time.time() + cap
        if new_until > _tele_pause_until:
            _tele_pause_until = new_until
            logger.warning("Tele-pause set for %.1fs due to: %s", cap, reason)

def get_tele_pause_remaining() -> float:
    with _tele_pause_lock:
        rem = _tele_pause_until - time.time()
        return rem if rem > 0 else 0.0

def is_tele_paused() -> bool:
    return get_tele_pause_remaining() > 0

# tg_call: minimal, non-blocking on long retry_after
def _parse_retry_after_from_response(data, resp_text=""):
    if isinstance(data, dict):
        params = data.get("parameters") or {}
        ra = params.get("retry_after") or data.get("retry_after")
        if ra:
            try:
                return int(ra)
            except Exception:
                pass
        desc = data.get("description") or ""
        m = re.search(r"retry\W*after\W*(\d+)", desc, re.I)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    m = re.search(r"retry\W*after\W*(\d+)", resp_text, re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return None

def tg_call(method: str, payload: dict):
    if not TELEGRAM_API:
        logger.error("tg_call: TELEGRAM_API not configured")
        return None
    # early fail if tele pause active
    if is_tele_paused():
        logger.info("tg_call: tele-pause active, aborting %s", method)
        return None
    url = f"{TELEGRAM_API}/{method}"
    max_retries = max(1, TG_CALL_MAX_RETRIES)
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        if is_tele_paused():
            return None
        _consume_token(block=True, timeout=10.0)
        try:
            resp = _session.post(url, json=payload, timeout=REQUESTS_TIMEOUT)
            try:
                data = resp.json()
            except Exception:
                data = None
            if resp.status_code == 429:
                retry_after = _parse_retry_after_from_response(data, resp.text)
                pause_seconds = retry_after if retry_after is not None else backoff
                set_tele_pause(pause_seconds, reason=f"429 {method}")
                logger.warning("tg_call: 429 received, applied tele-pause (parsed=%s)", retry_after)
                return None
            if 500 <= resp.status_code < 600:
                time.sleep(backoff)
                backoff = min(backoff * 2, TG_CALL_MAX_BACKOFF)
                continue
            if data is None:
                logger.error("tg_call: non-json response for %s status=%s", method, resp.status_code)
                return None
            return data
        except requests.exceptions.RequestException:
            logger.exception("tg_call: request exception on attempt %d", attempt)
            time.sleep(backoff)
            backoff = min(backoff * 2, TG_CALL_MAX_BACKOFF)
            continue
    logger.error("tg_call: failed after attempts for %s", method)
    return None

# Messaging helpers
def record_sent_message(chat_id: int, message_id: int):
    try:
        db_execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)", (chat_id, message_id, datetime.utcnow().isoformat()))
    except Exception:
        logger.exception("record_sent_message failed")

def send_message(chat_id: int, text: str, parse_mode: str = "Markdown"):
    if is_tele_paused():
        logger.debug("send_message: tele pause active aborting send to %s", chat_id)
        return None
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    data = tg_call("sendMessage", payload)
    if data and data.get("result"):
        mid = data["result"].get("message_id")
        if mid:
            record_sent_message(chat_id, mid)
        return data["result"]
    return None

def robust_send_message(chat_id: int, text: str, attempts: int = 1, base_backoff: float = ROBUST_SEND_BASE_BACKOFF):
    if attempts <= 0:
        attempts = 1
    if is_tele_paused():
        return None
    for attempt in range(1, attempts + 1):
        if is_tele_paused():
            return None
        res = send_message(chat_id, text)
        if res:
            return res
        if attempt < attempts:
            backoff = base_backoff * (2 ** (attempt - 1))
            backoff = min(backoff, TG_CALL_MAX_BACKOFF)
            time.sleep(backoff + random.uniform(0, 0.1 * backoff))
    return None

# Broadcast helpers (idempotent run + single attempt per recipient)
def compute_message_hash(text: str) -> str:
    h = hashlib.sha256()
    h.update((text or "").encode("utf-8"))
    return h.hexdigest()

def create_broadcast_run(message: str, created_by: int) -> str:
    run_id = str(uuid.uuid4())
    db_execute("INSERT INTO broadcast_runs (run_id, message_hash, message, created_by, created_at) VALUES (?, ?, ?, ?, ?)",
               (run_id, compute_message_hash(message), message, created_by, datetime.utcnow().isoformat()))
    return run_id

def insert_broadcast_recipient(run_id: str, user_id: int):
    db_execute("INSERT OR IGNORE INTO broadcast_recipients (run_id, user_id, status, attempts, last_attempt) VALUES (?, ?, 'pending', 0, ?)",
               (run_id, user_id, datetime.utcnow().isoformat()))

def get_previous_successful_recipients_for_message(message_hash: str):
    rows = db_execute("SELECT DISTINCT br.user_id FROM broadcast_recipients br JOIN broadcast_runs r ON br.run_id = r.run_id WHERE r.message_hash = ? AND br.status = 'success'",
                      (message_hash,), fetch=True)
    return set(r[0] for r in rows) if rows else set()

def get_pending_recipients_for_run(run_id: str):
    rows = db_execute("SELECT user_id FROM broadcast_recipients WHERE run_id = ? AND status = 'pending' ORDER BY user_id ASC", (run_id,), fetch=True)
    return [r[0] for r in rows] if rows else []

def mark_broadcast_recipient_status(run_id: str, user_id: int, status: str):
    db_execute("UPDATE broadcast_recipients SET status = ?, last_attempt = ? WHERE run_id = ? AND user_id = ?",
               (status, datetime.utcnow().isoformat(), run_id, user_id))

# Task helpers (minimal)
def split_text_into_words(text: str) -> List[str]:
    return [w for w in (text or "").strip().split() if w]

def enqueue_task(user_id: int, username: str, text: str):
    words = split_text_into_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    pending = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
    if pending >= 10:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    db_execute("INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count) VALUES (?, ?, ?, ?, ?, 'queued', ?, 0)",
               (user_id, username, text, json.dumps(words), total, datetime.utcnow().isoformat()))
    return {"ok": True, "total_words": total}

def get_next_task_for_user(user_id: int):
    rows = db_execute("SELECT id, words_json, total_words FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1", (user_id,), fetch=True)
    if not rows:
        return None
    r = rows[0]
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2]}

def set_task_status(task_id: int, status: str):
    if status == "running":
        db_execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, datetime.utcnow().isoformat(), task_id))
    elif status in ("done", "cancelled", "paused"):
        db_execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, datetime.utcnow().isoformat(), task_id))
    else:
        db_execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))

def record_split_log(user_id: int, username: str, words: int):
    db_execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)",
               (user_id, username, words, datetime.utcnow().isoformat()))

# Simple per-user worker (keeps behavior but trimmed)
_user_locks = {}
_user_locks_lock = threading.Lock()
_worker_stop = threading.Event()

def get_user_lock(user_id):
    with _user_locks_lock:
        if user_id not in _user_locks:
            _user_locks[user_id] = threading.Lock()
        return _user_locks[user_id]

def process_user_queue(user_id: int, chat_id: int, username: str):
    lock = get_user_lock(user_id)
    if not lock.acquire(blocking=False):
        return
    try:
        while True:
            if is_tele_paused():
                # pause behavior: notify user and yield
                robust_send_message(user_id, f"⚠️ Sending paused due to Telegram rate limits (~{int(get_tele_pause_remaining())}s).")
                break
            task = get_next_task_for_user(user_id)
            if not task:
                break
            task_id = task["id"]
            words = task["words"]
            total = task["total_words"]
            set_task_status(task_id, "running")
            sent_info = db_execute("SELECT sent_count FROM tasks WHERE id = ?", (task_id,), fetch=True)
            sent = int(sent_info[0][0] or 0) if sent_info else 0
            i = sent
            interval = 0.5 if total <= 150 else 0.6
            consecutive_failures = 0
            while i < total:
                if is_tele_paused():
                    set_task_status(task_id, "paused")
                    break
                word = words[i]
                ok = False
                for attempt in range(1, SPLIT_MAX_ATTEMPTS + 1):
                    if is_tele_paused():
                        break
                    res = robust_send_message(chat_id, word, attempts=1)
                    if res:
                        ok = True
                        db_execute("UPDATE tasks SET sent_count = sent_count + 1 WHERE id = ?", (task_id,))
                        break
                    time.sleep(0.2)
                if not ok:
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        set_task_status(task_id, "paused")
                        robust_send_message(chat_id, "⚠️ Repeated send failures — task paused.")
                        break
                    time.sleep(1.0)
                    continue
                i += 1
                time.sleep(interval)
            final = db_execute("SELECT status, sent_count FROM tasks WHERE id = ?", (task_id,), fetch=True)[0]
            if final[0] != "paused" and final[0] != "cancelled":
                set_task_status(task_id, "done")
                try:
                    record_split_log(user_id, username, int(final[1] or 0))
                    robust_send_message(chat_id, "✅ All done! Your text was split.")
                except Exception:
                    logger.exception("post-task actions failed")
    finally:
        lock.release()

def global_worker_loop():
    while not _worker_stop.is_set():
        try:
            rows = db_execute("SELECT DISTINCT user_id, username FROM tasks WHERE status = 'queued' ORDER BY created_at ASC", fetch=True)
            for r in rows:
                t = threading.Thread(target=process_user_queue, args=(r[0], r[0], r[1] or ""), daemon=True)
                t.start()
            time.sleep(1.0)
        except Exception:
            logger.exception("global_worker_loop error")
            time.sleep(1.0)

_worker_thread = threading.Thread(target=global_worker_loop, daemon=True)
_worker_thread.start()

# Webhook and command handlers (trimmed for clarity but full commands retained)
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
                cmd = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                return handle_command(user_id, username, cmd, args)
            else:
                return handle_new_text(user_id, username, text)
    except Exception:
        logger.exception("webhook handler failed")
    return jsonify({"ok": True})

def handle_command(user_id: int, username: str, command: str, args: str):
    if not is_allowed(user_id) and command not in ("/start", "/help"):
        robust_send_message(user_id, "❌ You are not allowed. The owner has been notified.")
        if OWNER_ID:
            robust_send_message(OWNER_ID, f"Unallowed access attempt by {username or user_id} ({user_id}).")
        return jsonify({"ok": True})

    if command == "/start":
        robust_send_message(user_id, "👋 Send any text and I'll split it into words.")
        return jsonify({"ok": True})

    if command == "/example":
        robust_send_message(user_id, "🎯 Running a short example...")
        enqueue_task(user_id, username, "This is a demo split")
        return jsonify({"ok": True})

    if command == "/pause":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            robust_send_message(user_id, "❌ No active task to pause.")
            return jsonify({"ok": True})
        set_task_status(rows[0][0], "paused")
        robust_send_message(user_id, "⏸️ Paused.")
        return jsonify({"ok": True})

    if command == "/resume":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            robust_send_message(user_id, "❌ No paused task to resume.")
            return jsonify({"ok": True})
        set_task_status(rows[0][0], "running")
        robust_send_message(user_id, "▶️ Resuming.")
        return jsonify({"ok": True})

    if command == "/status":
        active = db_execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        queued = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
        if active:
            aid, status, total, sent = active[0]
            remaining = int(total or 0) - int(sent or 0)
            robust_send_message(user_id, f"Status: {status}. Remaining words: {remaining}. Queue: {queued}")
        else:
            robust_send_message(user_id, f"No active tasks. Queue size: {queued}")
        return jsonify({"ok": True})

    if command == "/stop":
        cancelled = db_execute("UPDATE tasks SET status = 'cancelled' WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,))
        robust_send_message(user_id, "🛑 Stopped your tasks.")
        return jsonify({"ok": True})

    if command == "/adduser":
        if not is_admin(user_id):
            robust_send_message(user_id, "❌ Not allowed.")
            return jsonify({"ok": True})
        parts = re.split(r"[,\s]+", args.strip())
        added = []
        for p in parts:
            if not p:
                continue
            try:
                uid = int(p)
                db_execute("INSERT OR IGNORE INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, 0)", (uid, "", datetime.utcnow().isoformat()))
                added.append(uid)
            except Exception:
                continue
        robust_send_message(user_id, f"Added users: {', '.join(str(x) for x in added)}")
        return jsonify({"ok": True})

    if command == "/listusers":
        if not is_admin(user_id):
            robust_send_message(user_id, "❌ Not allowed.")
            return jsonify({"ok": True})
        rows = db_execute("SELECT user_id, username FROM allowed_users", fetch=True)
        lines = [f"{r[0]} {r[1] or ''}" for r in rows]
        robust_send_message(user_id, "Allowed:\n" + ("\n".join(lines) if lines else "(none)"))
        return jsonify({"ok": True})

    if command == "/suspend":
        if user_id not in OWNERS:
            robust_send_message(user_id, "❌ Only owner.")
            return jsonify({"ok": True})
        parts = args.split(None, 1)
        try:
            target = int(parts[0])
        except Exception:
            robust_send_message(user_id, "❌ Invalid id.")
            return jsonify({"ok": True})
        duration = 3600
        if len(parts) > 1:
            d = parse_duration_to_seconds(parts[1].strip())
            if d: duration = d
        until = datetime.utcnow() + timedelta(seconds=duration)
        db_execute("INSERT OR REPLACE INTO suspended_users (user_id, suspended_until, reason, added_by, added_at) VALUES (?, ?, ?, ?, ?)",
                   (target, until.isoformat(), "", user_id, datetime.utcnow().isoformat()))
        robust_send_message(user_id, f"Suspended {target} until {until.isoformat()}")
        try:
            robust_send_message(target, f"You have been suspended until {until.isoformat()} UTC.")
        except Exception:
            logger.exception("notify suspended failed")
        send_to_owners(f"User suspended: {target} by {user_id}")
        return jsonify({"ok": True})

    if command == "/unsuspend":
        if user_id not in OWNERS:
            robust_send_message(user_id, "❌ Only owner.")
            return jsonify({"ok": True})
        try:
            target = int(args.split()[0])
        except Exception:
            robust_send_message(user_id, "❌ Invalid id.")
            return jsonify({"ok": True})
        db_execute("DELETE FROM suspended_users WHERE user_id = ?", (target,))
        robust_send_message(user_id, f"Unsuspended {target}")
        try:
            robust_send_message(target, "You have been unsuspended.")
        except Exception:
            logger.exception("notify unsuspend failed")
        send_to_owners(f"User unsuspended: {target} by {user_id}")
        return jsonify({"ok": True})

    if command == "/listsuspended":
        if not is_admin(user_id):
            robust_send_message(user_id, "❌ Not allowed.")
            return jsonify({"ok": True})
        rows = db_execute("SELECT user_id, suspended_until FROM suspended_users", fetch=True)
        lines = [f"{r[0]} until {r[1]}" for r in rows]
        robust_send_message(user_id, "Suspended:\n" + ("\n".join(lines) if lines else "(none)"))
        return jsonify({"ok": True})

    if command == "/botinfo":
        if user_id not in OWNERS:
            robust_send_message(user_id, "❌ Only owner.")
            return jsonify({"ok": True})
        total_allowed = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0]
        queued = db_execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'", fetch=True)[0][0]
        robust_send_message(user_id, f"Allowed: {total_allowed}. Queued tasks: {queued}")
        return jsonify({"ok": True})

    if command == "/broadcast":
        if user_id not in OWNERS:
            robust_send_message(user_id, "❌ Only owner.")
            return jsonify({"ok": True})
        message = (args or "").strip()
        if not message:
            robust_send_message(user_id, "❌ Empty message.")
            return jsonify({"ok": True})
        # create run
        run_id = create_broadcast_run(message, user_id)
        already_done = get_previous_successful_recipients_for_message(compute_message_hash(message))
        rows = db_execute("SELECT user_id FROM allowed_users", fetch=True) or []
        total = 0
        for r in rows:
            uid = r[0]
            total += 1
            if uid in already_done:
                db_execute("INSERT OR IGNORE INTO broadcast_recipients (run_id, user_id, status, attempts, last_attempt) VALUES (?, ?, 'success', 0, ?)", (run_id, uid, datetime.utcnow().isoformat()))
                continue
            insert_broadcast_recipient(run_id, uid)
        pending = get_pending_recipients_for_run(run_id)
        failed = []
        success = 0
        # Single attempt per recipient (no repeated retries)
        for uid in pending:
            res = robust_send_message(uid, f"📣 Broadcast:\n\n{message}", attempts=1)
            db_execute("UPDATE broadcast_recipients SET attempts = attempts + 1, last_attempt = ? WHERE run_id = ? AND user_id = ?",
                       (datetime.utcnow().isoformat(), run_id, uid))
            if res:
                mark_broadcast_recipient_status(run_id, uid, "success")
                success += 1
            else:
                mark_broadcast_recipient_status(run_id, uid, "failed")
                failed.append(uid)
        # report
        if failed:
            failed_summary = ", ".join(str(x) for x in failed)
            robust_send_message(user_id, f"Broadcast {run_id} completed with failures: {len(failed)} failed. Failed user IDs: {failed_summary}")
            send_to_owners(f"Broadcast {run_id} failures: {failed_summary}")
        else:
            robust_send_message(user_id, f"Broadcast {run_id} completed. Success: {success + len(already_done)} / {total}")
        return jsonify({"ok": True})

    # unknown
    send_message(user_id, "❓ Unknown command.")
    return jsonify({"ok": True})

@app.route("/", methods=["GET", "POST"])
def root():
    if request.method == "POST":
        try:
            update_json = request.get_json(force=True)
            threading.Thread(target=handle_update, args=(update_json,), daemon=True).start()
            return jsonify({"ok": True})
        except Exception:
            logger.exception("root forward failed")
            return jsonify({"ok": False}), 200
    return "Bot running", 200

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "tele_pause_remaining_s": int(get_tele_pause_remaining())}), 200

def handle_update(update_json):
    try:
        if "message" in update_json:
            msg = update_json["message"]
            user = msg.get("from", {})
            uid = user.get("id")
            name = user.get("username") or user.get("first_name") or ""
            text = msg.get("text") or ""
            if text.startswith("/"):
                parts = text.split(None, 1)
                cmd = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                handle_command(uid, name, cmd, args)
            else:
                handle_new_text(uid, name, text)
    except Exception:
        logger.exception("handle_update failed")

def handle_new_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id):
        robust_send_message(user_id, "❌ You are not allowed.")
        if OWNER_ID:
            robust_send_message(OWNER_ID, f"Unallowed attempt by {user_id}")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res.get("reason") == "empty":
            robust_send_message(user_id, "⚠️ Empty text.")
        else:
            robust_send_message(user_id, "⚠️ Queue full.")
        return jsonify({"ok": True})
    robust_send_message(user_id, f"✅ Task added. Words: {res['total_words']}.")
    return jsonify({"ok": True})

# Simple auth helpers
def is_allowed(user_id: int) -> bool:
    if user_id in OWNERS:
        return True
    rows = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    return bool(rows)

def is_admin(user_id: int) -> bool:
    if user_id in OWNERS:
        return True
    rows = db_execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    return bool(rows and rows[0][0])

if __name__ == "__main__":
    try:
        # try to set webhook (best-effort)
        if TELEGRAM_API and WEBHOOK_URL:
            _session.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
    except Exception:
        logger.exception("set_webhook failed")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
