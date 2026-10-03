#!/usr/bin/env python3
"""
Genz Cheats Hosting Bot
Single-file Telegram hosting panel.

Python 3.11+
Required env:
  BOT_TOKEN
  OWNER_ID
  UPI_ID
  ADMIN_USERNAME
  SUPPORT_USERNAME

Important security note:
This application executes user-supplied code. A normal Python process is NOT a
security sandbox. For untrusted users, run the bot and hosted workloads inside
separate containers/VMs with OS-level quotas, AppArmor/SELinux, and a dedicated
service account. The code below provides application-level isolation,
safe archive extraction, process groups, timeouts, quotas and environment
sanitization, but cannot replace a real sandbox.
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import io
import json
import logging
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import traceback
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters
)

APP_NAME = "Genz Cheats Hosting Bot"
BASE_DIR = Path(os.getenv("HOSTING_DATA_DIR", "./genz_hosting_data")).resolve()
PROJECTS_DIR = BASE_DIR / "projects"
LOGS_DIR = BASE_DIR / "logs"
BACKUPS_DIR = BASE_DIR / "backups"
TMP_DIR = BASE_DIR / "tmp"
DB_PATH = BASE_DIR / "hosting.db"

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
UPI_ID = os.getenv("UPI_ID", "").strip()
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "").strip().lstrip("@")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "").strip().lstrip("@")

# Non-secret defaults; change them with env vars or from the admin settings
DEFAULT_MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "50"))
DEFAULT_LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(2 * 1024 * 1024)))
DEFAULT_INSTALL_TIMEOUT = int(os.getenv("INSTALL_TIMEOUT", "300"))
DEFAULT_START_TIMEOUT = int(os.getenv("START_TIMEOUT", "20"))
DEFAULT_MAX_RESTARTS = int(os.getenv("MAX_RESTARTS", "3"))
DEFAULT_CRASH_WINDOW = int(os.getenv("CRASH_WINDOW", "300"))
DEFAULT_TRIAL_HOURS = int(os.getenv("TRIAL_HOURS", "24"))
DEFAULT_TRIAL_RAM_MB = int(os.getenv("TRIAL_RAM_MB", "128"))
DEFAULT_TRIAL_STORAGE_MB = int(os.getenv("TRIAL_STORAGE_MB", "100"))
DEFAULT_TRIAL_DAILY_DEPLOYMENTS = int(os.getenv("TRIAL_DAILY_DEPLOYMENTS", "3"))

ENTRY_FILES_PY = ["main.py", "bot.py", "app.py", "index.py", "server.py", "run.py"]
ENTRY_FILES_NODE = ["main.js", "bot.js", "app.js", "index.js", "server.js", "index.mjs", "index.cjs"]
ALLOWED_EXTENSIONS = {".py", ".js", ".zip"}
PROJECT_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")

for d in (BASE_DIR, PROJECTS_DIR, LOGS_DIR, BACKUPS_DIR, TMP_DIR):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger(APP_NAME)

db_lock = asyncio.Lock()
processes: dict[str, asyncio.subprocess.Process] = {}
process_tasks: dict[str, asyncio.Task] = {}
restart_history: dict[str, list[float]] = {}

DB_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    balance REAL NOT NULL DEFAULT 0,
    banned INTEGER NOT NULL DEFAULT 0,
    trial_started TEXT,
    trial_expires TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    custom_project_limit INTEGER,
    custom_ram_mb INTEGER,
    custom_storage_mb INTEGER,
    custom_daily_deployments INTEGER
);

CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    price REAL NOT NULL DEFAULT 0,
    duration_days INTEGER NOT NULL DEFAULT 30,
    max_projects INTEGER NOT NULL DEFAULT 1,
    ram_mb INTEGER NOT NULL DEFAULT 128,
    storage_mb INTEGER NOT NULL DEFAULT 100,
    daily_deployments INTEGER NOT NULL DEFAULT 3,
    max_running INTEGER NOT NULL DEFAULT 1,
    max_file_mb INTEGER NOT NULL DEFAULT 50,
    runtime_hours INTEGER NOT NULL DEFAULT 24,
    auto_restart INTEGER NOT NULL DEFAULT 0,
    priority_support INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    plan_id INTEGER NOT NULL,
    starts_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    source TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(plan_id) REFERENCES plans(id)
);

CREATE TABLE IF NOT EXISTS payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    plan_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    utr TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TEXT NOT NULL,
    reviewed_at TEXT,
    reviewed_by INTEGER,
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(plan_id) REFERENCES plans(id)
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    amount REAL NOT NULL,
    balance_after REAL NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    runtime TEXT,
    entry_file TEXT,
    path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'STOPPED',
    pid INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    auto_restart INTEGER NOT NULL DEFAULT 0,
    restart_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_started TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS project_settings (
    project_id TEXT PRIMARY KEY,
    auto_start INTEGER NOT NULL DEFAULT 0,
    env_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS usage (
    user_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    deployments INTEGER NOT NULL DEFAULT 0,
    uploads INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(user_id, date)
);

CREATE TABLE IF NOT EXISTS admin_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    target TEXT,
    details TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

def now() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None

def esc(value) -> str:
    return html.escape(str(value if value is not None else ""))

def safe_name(name: str, default="project") -> str:
    name = PROJECT_NAME_RE.sub("-", Path(name).stem).strip(".-")
    return (name[:50] or default)

def owner(uid: int) -> bool:
    return OWNER_ID > 0 and uid == OWNER_ID

def utc_date() -> str:
    return now().date().isoformat()

def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    return con

def init_db():
    with db_connect() as con:
        con.executescript(DB_SCHEMA)
        defaults = [
            ("maintenance", "0"),
            ("block_new_deployments", "0"),
            ("max_upload_mb", str(DEFAULT_MAX_UPLOAD_MB)),
            ("log_max_bytes", str(DEFAULT_LOG_MAX_BYTES)),
            ("install_timeout", str(DEFAULT_INSTALL_TIMEOUT)),
            ("max_restarts", str(DEFAULT_MAX_RESTARTS)),
        ]
        con.executemany("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", defaults)
        plans = [
            ("FREE TRIAL", 0, DEFAULT_TRIAL_HOURS / 24, 1, DEFAULT_TRIAL_RAM_MB,
             DEFAULT_TRIAL_STORAGE_MB, DEFAULT_TRIAL_DAILY_DEPLOYMENTS, 1,
             DEFAULT_MAX_UPLOAD_MB, DEFAULT_TRIAL_HOURS, 0, 0),
            ("BASIC", 49, 30, 2, 256, 500, 10, 1, 50, 720, 1, 0),
            ("PREMIUM", 99, 30, 5, 512, 1500, 25, 2, 75, 720, 1, 1),
            ("PRO", 199, 30, 10, 1024, 5000, 50, 4, 100, 720, 1, 1),
            ("LIFETIME", 499, 36500, 20, 1024, 10000, 100, 5, 100, 876000, 1, 1),
        ]
        for p in plans:
            con.execute("""
                INSERT OR IGNORE INTO plans
                (name,price,duration_days,max_projects,ram_mb,storage_mb,daily_deployments,
                 max_running,max_file_mb,runtime_hours,auto_restart,priority_support)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """, p)
        con.commit()

def setting(key: str, default=None):
    with db_connect() as con:
        row = con.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default

def setting_int(key: str, default: int) -> int:
    try:
        return int(setting(key, default))
    except (TypeError, ValueError):
        return default

def get_user(uid: int):
    with db_connect() as con:
        return con.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()

def ensure_user(tg_user):
    uid = tg_user.id
    t = iso(now())
    with db_connect() as con:
        row = con.execute("SELECT user_id FROM users WHERE user_id=?", (uid,)).fetchone()
        if not row:
            trial_start = now()
            trial_end = trial_start + timedelta(hours=DEFAULT_TRIAL_HOURS)
            con.execute("""
                INSERT INTO users(user_id,username,first_name,trial_started,trial_expires,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?)
            """, (uid, tg_user.username or "", tg_user.first_name or "",
                  iso(trial_start), iso(trial_end), t, t))
        else:
            con.execute("""
                UPDATE users SET username=?,first_name=?,updated_at=? WHERE user_id=?
            """, (tg_user.username or "", tg_user.first_name or "", t, uid))
        con.commit()
    return get_user(uid)

def active_subscription(uid: int):
    with db_connect() as con:
        return con.execute("""
            SELECT s.*, p.* FROM subscriptions s
            JOIN plans p ON p.id=s.plan_id
            WHERE s.user_id=? AND s.active=1 AND s.expires_at>?
            ORDER BY s.expires_at DESC LIMIT 1
        """, (uid, iso(now()))).fetchone()

def effective_plan(uid: int):
    sub = active_subscription(uid)
    if sub:
        return sub
    u = get_user(uid)
    if not u:
        return None
    exp = parse_iso(u["trial_expires"])
    if exp and exp > now():
        with db_connect() as con:
            return con.execute("SELECT * FROM plans WHERE name='FREE TRIAL'").fetchone()
    return None

def limits(uid: int):
    u = get_user(uid)
    p = effective_plan(uid)
    if not u or not p:
        return dict(projects=0, ram=0, storage=0, daily=0, running=0, file_mb=0, runtime=0, auto_restart=0)
    return dict(
        projects=u["custom_project_limit"] if u["custom_project_limit"] is not None else p["max_projects"],
        ram=u["custom_ram_mb"] if u["custom_ram_mb"] is not None else p["ram_mb"],
        storage=u["custom_storage_mb"] if u["custom_storage_mb"] is not None else p["storage_mb"],
        daily=u["custom_daily_deployments"] if u["custom_daily_deployments"] is not None else p["daily_deployments"],
        running=p["max_running"], file_mb=p["max_file_mb"], runtime=p["runtime_hours"],
        auto_restart=p["auto_restart"]
    )

def user_project_count(uid: int) -> int:
    with db_connect() as con:
        return con.execute("SELECT COUNT(*) c FROM projects WHERE user_id=?", (uid,)).fetchone()["c"]

def running_count(uid: int) -> int:
    with db_connect() as con:
        return con.execute("SELECT COUNT(*) c FROM projects WHERE user_id=? AND status='ONLINE'", (uid,)).fetchone()["c"]

def directory_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for p in path.rglob("*"):
        try:
            if p.is_file() and not p.is_symlink():
                total += p.stat().st_size
        except OSError:
            pass
    return total

def user_storage(uid: int) -> int:
    total = 0
    root = PROJECTS_DIR / str(uid)
    if root.exists():
        for p in root.iterdir():
            total += directory_size(p)
    return total

def project_row(uid: int, pid: str):
    with db_connect() as con:
        return con.execute("SELECT * FROM projects WHERE id=? AND user_id=?", (pid, uid)).fetchone()

def all_project_rows():
    with db_connect() as con:
        return con.execute("SELECT * FROM projects").fetchall()

def log_project(pid: str, level: str, message: str):
    clean = str(message)[-10000:]
    with db_connect() as con:
        con.execute("INSERT INTO logs(project_id,level,message,created_at) VALUES(?,?,?,?)",
                    (pid, level, clean, iso(now())))
        con.commit()
    f = LOGS_DIR / f"{pid}.log"
    try:
        with f.open("a", encoding="utf-8", errors="replace") as out:
            out.write(f"[{iso(now())}] [{level}] {clean}\n")
        maxb = setting_int("log_max_bytes", DEFAULT_LOG_MAX_BYTES)
        if f.stat().st_size > maxb:
            data = f.read_bytes()[-maxb:]
            f.write_bytes(data)
    except OSError:
        pass

def audit(action: str, target="", details="", admin_id=OWNER_ID):
    with db_connect() as con:
        con.execute("INSERT INTO admin_actions(admin_id,action,target,details,created_at) VALUES(?,?,?,?,?)",
                    (admin_id, action, str(target), str(details)[:2000], iso(now())))
        con.commit()

def record_usage(uid: int, deployments=0, uploads=0):
    with db_connect() as con:
        con.execute("""
            INSERT INTO usage(user_id,date,deployments,uploads) VALUES(?,?,?,?)
            ON CONFLICT(user_id,date) DO UPDATE SET
              deployments=deployments+excluded.deployments,
              uploads=uploads+excluded.uploads
        """, (uid, utc_date(), deployments, uploads))
        con.commit()

def daily_deployments(uid: int) -> int:
    with db_connect() as con:
        r = con.execute("SELECT deployments FROM usage WHERE user_id=? AND date=?",
                        (uid, utc_date())).fetchone()
    return r["deployments"] if r else 0

def plan_text(p) -> str:
    if not p:
        return "No active plan"
    return (
        f"💎 <b>{esc(p['name'])}</b>\n"
        f"💰 Price: ₹{p['price']:.2f}\n"
        f"📦 Projects: {p['max_projects']}\n"
        f"🧠 RAM: {p['ram_mb']} MB\n"
        f"💾 Storage: {p['storage_mb']} MB\n"
        f"🚀 Daily deployments: {p['daily_deployments']}\n"
        f"🔄 Auto restart: {'Yes' if p['auto_restart'] else 'No'}\n"
    )

def maintenance_blocked() -> bool:
    return setting("maintenance", "0") == "1" and setting("block_new_deployments", "0") == "1"

def main_menu(uid: int):
    rows = [
        [("🚀 Host Project", "host"), ("📂 My Projects", "projects")],
        [("💎 Plans", "plans"), ("💳 Wallet", "wallet")],
        [("📊 Account", "account"), ("🧾 Usage", "usage")],
        [("🆘 Support", "support")],
    ]
    if owner(uid):
        rows.append([("👑 Admin Panel", "admin")])
    return InlineKeyboardMarkup([[InlineKeyboardButton(a, callback_data=b) for a,b in row] for row in rows])

def back_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="home")]])

def project_keyboard(pid: str, uid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start", callback_data=f"p:start:{pid}"),
         InlineKeyboardButton("🛑 Stop", callback_data=f"p:stop:{pid}")],
        [InlineKeyboardButton("🔁 Restart", callback_data=f"p:restart:{pid}"),
         InlineKeyboardButton("🧾 Logs", callback_data=f"p:logs:{pid}")],
        [InlineKeyboardButton("📊 Stats", callback_data=f"p:stats:{pid}"),
         InlineKeyboardButton("✏️ Rename", callback_data=f"p:rename:{pid}")],
        [InlineKeyboardButton("📥 Download", callback_data=f"p:download:{pid}"),
         InlineKeyboardButton("🗑 Delete", callback_data=f"p:delete:{pid}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="projects")]
    ])

async def send_home(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = ensure_user(update.effective_user)
    p = effective_plan(uid)
    active = "None"
    expiry = "—"
    if p:
        sub = active_subscription(uid)
        if sub:
            expiry = sub["expires_at"][:19].replace("T", " ")
        else:
            expiry = (u["trial_expires"] or "")[:19].replace("T", " ")
        active = p["name"]
    text = (
        f"╭━━〔 ⚡ {APP_NAME.upper()} 〕━━╮\n"
        f"┃\n┃ 🚀 Premium Telegram Hosting\n┃\n"
        f"┃ 👤 User: <code>{uid}</code>\n"
        f"┃ 💎 Plan: <b>{esc(active)}</b>\n"
        f"┃ 🚀 Running: {running_count(uid)}\n"
        f"┃ 💾 Storage: {user_storage(uid)/1024/1024:.1f} MB\n"
        f"┃ ⏳ Expiry: {esc(expiry)}\n┃\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━━━━━╯"
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu(uid))
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu(uid))

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update.effective_user)
    await send_home(update, context)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        f"<b>{APP_NAME}</b>\n\n"
        "🚀 Upload .py, .js or .zip projects.\n"
        "📂 Manage your own projects.\n"
        "💎 Purchase plans using manual UPI.\n\n"
        "<b>User commands</b>\n"
        "/start /projects /plans /wallet /account /usage\n"
        "/logs PROJECT /restart PROJECT /stop PROJECT /delete PROJECT"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu(update.effective_user.id))

async def plans_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with db_connect() as con:
        plans = con.execute("SELECT * FROM plans WHERE active=1 ORDER BY price,id").fetchall()
    lines = [f"╭━━〔 💎 {APP_NAME.upper()} — PLANS 〕━━╮", ""]
    buttons = []
    for p in plans:
        if p["name"] == "FREE TRIAL":
            continue
        lines.append(f"💎 <b>{esc(p['name'])}</b> — ₹{p['price']:.2f} / {p['duration_days']} days")
        lines.append(f"   📦 {p['max_projects']} projects • 🧠 {p['ram_mb']} MB • 💾 {p['storage_mb']} MB")
        buttons.append([InlineKeyboardButton(f"🛒 Buy {p['name']}", callback_data=f"buy:{p['id']}")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="home")])
    target = update.callback_query
    markup = InlineKeyboardMarkup(buttons)
    if target:
        await target.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=markup)
    else:
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=markup)

async def wallet_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = get_user(uid)
    with db_connect() as con:
        tx = con.execute("SELECT * FROM transactions WHERE user_id=? ORDER BY id DESC LIMIT 10", (uid,)).fetchall()
    history = "\n".join(f"• {esc(r['kind'])}: ₹{r['amount']:.2f} — {esc(r['created_at'][:19])}" for r in tx) or "No transactions"
    text = f"💳 <b>Wallet</b>\n\n💰 Balance: ₹{u['balance']:.2f}\n\n<b>Recent transactions</b>\n{history}"
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=back_menu())

async def account_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = get_user(uid)
    p = effective_plan(uid)
    sub = active_subscription(uid)
    exp = sub["expires_at"] if sub else u["trial_expires"]
    text = (
        f"📊 <b>Account</b>\n\n"
        f"🆔 User ID: <code>{uid}</code>\n"
        f"👤 Username: @{esc(u['username']) if u['username'] else '—'}\n"
        f"💎 Plan: <b>{esc(p['name']) if p else 'Expired'}</b>\n"
        f"⏳ Expiry: {esc(exp or '—')}\n"
        f"🚀 Projects: {user_project_count(uid)}\n"
        f"💾 Storage: {user_storage(uid)/1024/1024:.2f} MB\n"
        f"💰 Wallet: ₹{u['balance']:.2f}"
    )
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=back_menu())

async def usage_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lim = limits(uid)
    text = (
        f"🧾 <b>Usage</b>\n\n"
        f"🚀 Projects: {user_project_count(uid)}/{lim['projects']}\n"
        f"🟢 Running: {running_count(uid)}/{lim['running']}\n"
        f"💾 Storage: {user_storage(uid)/1024/1024:.2f}/{lim['storage']} MB\n"
        f"📤 Deployments today: {daily_deployments(uid)}/{lim['daily']}\n"
        f"📦 Max file: {lim['file_mb']} MB\n"
        f"🧠 RAM plan: {lim['ram']} MB"
    )
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=back_menu())

async def projects_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    with db_connect() as con:
        rows = con.execute("SELECT * FROM projects WHERE user_id=? ORDER BY created_at DESC", (uid,)).fetchall()
    if not rows:
        text = "📂 <b>My Projects</b>\n\nNo projects yet. Press 🚀 Host Project to deploy one."
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Host Project", callback_data="host")],
            [InlineKeyboardButton("⬅️ Back", callback_data="home")]
        ])
    else:
        text = "📂 <b>My Projects</b>\n\n"
        buttons = []
        for r in rows:
            status_icon = {"ONLINE":"🟢","OFFLINE":"🔴","STARTING":"🟡","ERROR":"⚠️","STOPPED":"⏸"}.get(r["status"], "❔")
            text += f"{status_icon} <b>{esc(r['name'])}</b> — {esc(r['runtime'] or '?')} — {esc(r['status'])}\n"
            buttons.append([InlineKeyboardButton(f"{status_icon} {r['name']}", callback_data=f"show:{r['id']}")])
        buttons.append([InlineKeyboardButton("🚀 Host Project", callback_data="host")])
        buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="home")])
        markup = InlineKeyboardMarkup(buttons)
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

def safe_extract_zip(zip_path: Path, destination: Path):
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            name = info.filename.replace("\\", "/")
            if not name or name.endswith("/"):
                continue
            if name.startswith("/") or "\x00" in name:
                raise ValueError("Unsafe ZIP path")
            parts = Path(name).parts
            if ".." in parts:
                raise ValueError("ZIP path traversal blocked")
            mode = (info.external_attr >> 16) & 0o170000
            if mode == 0o120000:
                raise ValueError("Symlink in ZIP is not allowed")
            target = (destination / name).resolve()
            if destination.resolve() not in target.parents and target != destination.resolve():
                raise ValueError("ZIP path traversal blocked")
            target.parent.mkdir(parents=True, exist_ok=True)
            with z.open(info) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=1024*1024)

def find_entry(root: Path, runtime_hint: Optional[str] = None):
    # Prefer files at project root, then shallow nested directories.
    candidates = ENTRY_FILES_PY if runtime_hint == "python" else ENTRY_FILES_NODE if runtime_hint == "node" else ENTRY_FILES_PY + ENTRY_FILES_NODE
    for candidate in candidates:
        p = root / candidate
        if p.is_file():
            runtime = "python" if candidate in ENTRY_FILES_PY else "node"
            return runtime, candidate
    for p in sorted(root.rglob("*")):
        if not p.is_file() or len(p.relative_to(root).parts) > 3:
            continue
        if p.name in ENTRY_FILES_PY:
            return "python", str(p.relative_to(root))
        if p.name in ENTRY_FILES_NODE:
            return "node", str(p.relative_to(root))
    return None, None

def safe_project_env():
    keep = {"PATH", "LANG", "LC_ALL", "HOME", "TMPDIR", "SYSTEMROOT", "COMSPEC"}
    return {k: v for k, v in os.environ.items() if k in keep}

async def run_capture(cmd, cwd: Path, timeout: int, log_pid: str):
    env = safe_project_env()
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=str(cwd), env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        start_new_session=True
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        raise TimeoutError(f"Command timed out after {timeout}s")
    text_out = out.decode("utf-8", "replace")[-20000:]
    log_project(log_pid, "INSTALL", text_out)
    return proc.returncode, text_out

async def prepare_project(pid: str, root: Path, runtime: str):
    timeout = setting_int("install_timeout", DEFAULT_INSTALL_TIMEOUT)
    if runtime == "python":
        req = root / "requirements.txt"
        if req.exists():
            venv = root / ".venv"
            code, out = await run_capture([sys.executable, "-m", "venv", str(venv)], root, timeout, pid)
            if code != 0:
                raise RuntimeError("Virtual environment creation failed")
            pip = venv / "bin" / "pip"
            if not pip.exists():
                pip = venv / "Scripts" / "pip.exe"
            code, out = await run_capture([str(pip), "install", "--disable-pip-version-check", "-r", "requirements.txt"],
                                          root, timeout, pid)
            if code != 0:
                raise RuntimeError("Python dependency installation failed")
    else:
        pkg = root / "package.json"
        if pkg.exists():
            npm = shutil.which("npm")
            if not npm:
                raise RuntimeError("npm is not installed on this host")
            cmd = [npm, "ci", "--ignore-scripts"] if (root / "package-lock.json").exists() else [npm, "install", "--ignore-scripts"]
            code, out = await run_capture(cmd, root, timeout, pid)
            if code != 0:
                raise RuntimeError("Node.js dependency installation failed")

async def start_project(uid: int, pid: str, auto=False) -> tuple[bool, str]:
    row = project_row(uid, pid)
    if not row:
        return False, "Project not found."
    if pid in processes and processes[pid].returncode is None:
        return False, "Project is already running."
    lim = limits(uid)
    if running_count(uid) >= lim["running"]:
        return False, "Running-project limit reached."
    root = Path(row["path"]).resolve()
    if not root.exists():
        return False, "Project files are missing."
    runtime, entry = find_entry(root, row["runtime"])
    if not runtime:
        return False, "No supported entry file found."
    with db_connect() as con:
        con.execute("UPDATE projects SET runtime=?,entry_file=?,status='STARTING',updated_at=? WHERE id=? AND user_id=?",
                    (runtime, entry, iso(now()), pid, uid))
        con.commit()
    try:
        await prepare_project(pid, root, runtime)
        if runtime == "python":
            venv_py = root / ".venv" / "bin" / "python"
            if not venv_py.exists():
                venv_py = root / ".venv" / "Scripts" / "python.exe"
            executable = str(venv_py) if venv_py.exists() else sys.executable
            cmd = [executable, entry]
        else:
            node = shutil.which("node")
            if not node:
                raise RuntimeError("Node.js is not installed on this host")
            cmd = [node, entry]
        env = safe_project_env()
        env["PYTHONUNBUFFERED"] = "1"
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=str(root), env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            start_new_session=True
        )
        processes[pid] = proc
        with db_connect() as con:
            con.execute("UPDATE projects SET status='ONLINE',pid=?,last_started=?,last_error=NULL,updated_at=? WHERE id=?",
                        (proc.pid, iso(now()), iso(now()), pid))
            con.commit()
        log_project(pid, "INFO", f"Started PID={proc.pid}: {' '.join(cmd)}")
        process_tasks[pid] = asyncio.create_task(monitor_project(uid, pid, proc))
        return True, "Project started."
    except Exception as e:
        msg = str(e)
        log_project(pid, "ERROR", traceback.format_exc())
        with db_connect() as con:
            con.execute("UPDATE projects SET status='ERROR',last_error=?,updated_at=? WHERE id=?",
                        (msg[:1000], iso(now()), pid))
            con.commit()
        return False, msg

async def stop_project(uid: int, pid: str):
    row = project_row(uid, pid)
    if not row:
        return False, "Project not found."
    proc = processes.get(pid)
    if proc and proc.returncode is None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            with contextlib.suppress(Exception):
                await proc.wait()
    processes.pop(pid, None)
    task = process_tasks.pop(pid, None)
    if task and task is not asyncio.current_task():
        task.cancel()
    with db_connect() as con:
        con.execute("UPDATE projects SET status='STOPPED',pid=NULL,updated_at=? WHERE id=? AND user_id=?",
                    (iso(now()), pid, uid))
        con.commit()
    log_project(pid, "INFO", "Project stopped.")
    return True, "Project stopped."

async def monitor_project(uid: int, pid: str, proc):
    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            log_project(pid, "OUT", line.decode("utf-8", "replace").rstrip())
        rc = await proc.wait()
        processes.pop(pid, None)
        with db_connect() as con:
            row = con.execute("SELECT * FROM projects WHERE id=? AND user_id=?", (pid, uid)).fetchone()
            if not row:
                return
            if row["status"] not in ("STOPPED",):
                con.execute("UPDATE projects SET status=?,pid=NULL,last_error=?,updated_at=? WHERE id=?",
                            ("ERROR" if rc else "STOPPED", f"Process exited with code {rc}", iso(now()), pid))
            con.commit()
        log_project(pid, "INFO" if rc == 0 else "ERROR", f"Process exited with code {rc}")
        if rc != 0:
            await maybe_auto_restart(uid, pid)
    except asyncio.CancelledError:
        return
    except Exception:
        log_project(pid, "ERROR", traceback.format_exc())

async def maybe_auto_restart(uid: int, pid: str):
    row = project_row(uid, pid)
    if not row:
        return
    lim = limits(uid)
    if not lim["auto_restart"] or not row["auto_restart"]:
        return
    t = time.time()
    history = [x for x in restart_history.get(pid, []) if t - x < DEFAULT_CRASH_WINDOW]
    maxr = setting_int("max_restarts", DEFAULT_MAX_RESTARTS)
    if len(history) >= maxr:
        log_project(pid, "ERROR", "Auto-restart disabled for this crash window.")
        return
    history.append(t)
    restart_history[pid] = history
    with db_connect() as con:
        con.execute("UPDATE projects SET restart_count=restart_count+1,status='STARTING',updated_at=? WHERE id=?",
                    (iso(now()), pid))
        con.commit()
    await asyncio.sleep(min(5 * len(history), 30))
    await start_project(uid, pid, auto=True)

async def deploy_from_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = ensure_user(update.effective_user)
    if u["banned"]:
        await update.message.reply_text("🚫 Your account is banned.")
        return
    if maintenance_blocked() and not owner(uid):
        await update.message.reply_text("🛠 Maintenance mode is active. New deployments are temporarily disabled.")
        return
    lim = limits(uid)
    if not lim["projects"]:
        await update.message.reply_text("💎 Your trial/plan has expired. Open Plans to continue.")
        return
    if user_project_count(uid) >= lim["projects"]:
        await update.message.reply_text("📦 Project limit reached.")
        return
    if daily_deployments(uid) >= lim["daily"]:
        await update.message.reply_text("⏳ Daily deployment limit reached.")
        return

    doc = update.message.document
    ext = Path(doc.file_name or "").suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        await update.message.reply_text("❌ Only .py, .js and .zip files are supported.")
        return
    max_bytes = lim["file_mb"] * 1024 * 1024
    if doc.file_size and doc.file_size > max_bytes:
        await update.message.reply_text(f"❌ File is too large. Maximum is {lim['file_mb']} MB.")
        return
    if user_storage(uid) + (doc.file_size or 0) > lim["storage"] * 1024 * 1024:
        await update.message.reply_text("💾 Storage quota exceeded.")
        return

    project_id = uuid.uuid4().hex[:12]
    name = safe_name(doc.file_name)
    root = PROJECTS_DIR / str(uid) / project_id
    tmp = TMP_DIR / f"{uid}_{project_id}{ext}"
    root.parent.mkdir(parents=True, exist_ok=True)
    try:
        await update.message.reply_text("📥 Downloading project...")
        tg_file = await context.bot.get_file(doc.file_id)
        await tg_file.download_to_drive(custom_path=str(tmp))
        root.mkdir(parents=True, exist_ok=False)
        if ext == ".zip":
            safe_extract_zip(tmp, root)
        else:
            shutil.copy2(tmp, root / Path(doc.file_name).name)
        runtime, entry = find_entry(root)
        if not runtime:
            shutil.rmtree(root, ignore_errors=True)
            await update.message.reply_text("❌ No supported entry file found. Include one of the supported Python/Node entry files.")
            return
        with db_connect() as con:
            con.execute("""
                INSERT INTO projects(id,user_id,name,runtime,entry_file,path,status,created_at,updated_at,auto_restart)
                VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (project_id, uid, name, runtime, entry, str(root), "STARTING", iso(now()), iso(now()), 0))
            con.commit()
        record_usage(uid, deployments=1, uploads=1)
        ok, msg = await start_project(uid, project_id)
        row = project_row(uid, project_id)
        status = row["status"] if row else "ERROR"
        text = (
            f"╭━━〔 🚀 DEPLOYMENT 〕━━╮\n┃\n"
            f"┃ Project: <b>{esc(name)}</b>\n"
            f"┃ Runtime: {esc(runtime)}\n"
            f"┃ Entry: <code>{esc(entry)}</code>\n┃\n"
            f"┃ Status: {'🟢 ONLINE' if status=='ONLINE' else '⚠️ ERROR'}\n"
            f"┃ {esc(msg)}\n┃\n╰━━━━━━━━━━━━━━━━━━━━━━╯"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                        reply_markup=project_keyboard(project_id, uid))
    except Exception as e:
        log.error("Deployment error: %s", traceback.format_exc())
        shutil.rmtree(root, ignore_errors=True)
        await update.message.reply_text(f"❌ Deployment failed: {esc(e)}", parse_mode=ParseMode.HTML)
    finally:
        with contextlib.suppress(OSError):
            tmp.unlink()

async def host_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "📤 <b>Send your project</b>\n\nUpload a <code>.py</code>, <code>.js</code> or <code>.zip</code> file.",
            parse_mode=ParseMode.HTML, reply_markup=back_menu()
        )

async def show_project(update: Update, pid: str):
    uid = update.effective_user.id
    row = project_row(uid, pid)
    if not row:
        await update.callback_query.answer("Project not found.", show_alert=True)
        return
    status_icon = {"ONLINE":"🟢","ERROR":"⚠️","STARTING":"🟡","STOPPED":"⏸","OFFLINE":"🔴"}.get(row["status"], "❔")
    root = Path(row["path"])
    text = (
        f"🚀 <b>{esc(row['name'])}</b>\n\n"
        f"{status_icon} Status: <b>{esc(row['status'])}</b>\n"
        f"⚙️ Runtime: {esc(row['runtime'] or '—')}\n"
        f"📄 Entry: <code>{esc(row['entry_file'] or '—')}</code>\n"
        f"💾 Disk: {directory_size(root)/1024/1024:.2f} MB\n"
        f"🔄 Auto restart: {'ON' if row['auto_restart'] else 'OFF'}\n"
        f"🕐 Started: {esc(row['last_started'] or '—')}\n"
        f"⚠️ Error: {esc(row['last_error'] or '—')}"
    )
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=project_keyboard(pid, uid))

async def project_logs(update: Update, pid: str):
    uid = update.effective_user.id
    row = project_row(uid, pid)
    if not row:
        return
    f = LOGS_DIR / f"{pid}.log"
    data = f.read_text(encoding="utf-8", errors="replace")[-12000:] if f.exists() else "No logs."
    await update.callback_query.edit_message_text(
        f"🧾 <b>Logs — {esc(row['name'])}</b>\n<pre>{esc(data)}</pre>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧹 Clear Logs", callback_data=f"p:clearlogs:{pid}")],
            [InlineKeyboardButton("📥 Download Logs", callback_data=f"p:downloadlogs:{pid}")],
            [InlineKeyboardButton("⬅️ Back", callback_data=f"show:{pid}")]
        ])
    )

async def send_project_download(query, uid, pid):
    row = project_row(uid, pid)
    if not row:
        await query.answer("Not found", show_alert=True); return
    root = Path(row["path"])
    archive = TMP_DIR / f"{pid}.zip"
    try:
        shutil.make_archive(str(archive.with_suffix("")), "zip", root)
        await query.message.reply_document(open(archive, "rb"), filename=f"{row['name']}.zip")
    finally:
        with contextlib.suppress(OSError):
            archive.unlink()

async def payment_buy(update: Update, plan_id: int):
    uid = update.effective_user.id
    with db_connect() as con:
        p = con.execute("SELECT * FROM plans WHERE id=? AND active=1", (plan_id,)).fetchone()
    if not p:
        await update.callback_query.answer("Plan unavailable.", show_alert=True); return
    context_data = update.callback_query.data
    text = (
        f"💳 <b>Buy {esc(p['name'])}</b>\n\n"
        f"💰 Amount: <b>₹{p['price']:.2f}</b>\n"
        f"📱 UPI ID: <code>{esc(UPI_ID or 'Not configured')}</code>\n\n"
        "1. Make the UPI payment.\n"
        "2. Press Submit UTR.\n"
        "3. Enter the transaction/UTR ID.\n"
        "4. Wait for owner approval.\n\n"
        "⚠️ Your plan is NOT activated automatically."
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🧾 Submit UTR", callback_data=f"utr:{p['id']}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="plans")]
    ])
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

async def utr_start(update: Update, context: ContextTypes.DEFAULT_TYPE, plan_id: int):
    context.user_data["payment_plan_id"] = plan_id
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("🧾 Send your UTR / Transaction ID as a message.\n\nSend /cancel to stop.")

async def utr_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    plan_id = context.user_data.pop("payment_plan_id", None)
    if not plan_id:
        return False
    utr = (update.message.text or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]{6,80}", utr):
        await update.message.reply_text("❌ Invalid UTR format.")
        context.user_data["payment_plan_id"] = plan_id
        return True
    with db_connect() as con:
        duplicate = con.execute("SELECT id FROM payments WHERE UTR=? COLLATE NOCASE AND status!='REJECTED'", (utr,)).fetchone()
        p = con.execute("SELECT * FROM plans WHERE id=? AND active=1", (plan_id,)).fetchone()
        if duplicate or not p:
            await update.message.reply_text("❌ Duplicate UTR or unavailable plan.")
            return True
        con.execute("""
            INSERT INTO payments(user_id,plan_id,amount,utr,status,created_at)
            VALUES(?,?,?,?,?,?)
        """, (uid, plan_id, p["price"], utr, "PENDING", iso(now())))
        payment_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
        con.commit()
    await update.message.reply_text("✅ Payment request submitted. The owner will review it.")
    if OWNER_ID:
        try:
            await context.bot.send_message(
                OWNER_ID,
                f"💳 <b>Pending Payment #{payment_id}</b>\n\n"
                f"User: <code>{uid}</code>\n"
                f"Username: @{esc(update.effective_user.username or '—')}\n"
                f"Plan: <b>{esc(p['name'])}</b>\nAmount: ₹{p['price']:.2f}\nUTR: <code>{esc(utr)}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Approve", callback_data=f"pay:approve:{payment_id}"),
                     InlineKeyboardButton("❌ Reject", callback_data=f"pay:reject:{payment_id}")]
                ])
            )
        except TelegramError:
            pass
    return True

async def approve_payment(update: Update, payment_id: int, approve: bool):
    if not owner(update.effective_user.id):
        await update.callback_query.answer("Owner only.", show_alert=True); return
    with db_connect() as con:
        payment = con.execute("SELECT * FROM payments WHERE id=?", (payment_id,)).fetchone()
        if not payment or payment["status"] != "PENDING":
            await update.callback_query.answer("Already reviewed.", show_alert=True); return
        p = con.execute("SELECT * FROM plans WHERE id=?", (payment["plan_id"],)).fetchone()
        if not approve:
            con.execute("UPDATE payments SET status='REJECTED',reviewed_at=?,reviewed_by=? WHERE id=?",
                        (iso(now()), OWNER_ID, payment_id))
            con.commit()
            audit("payment_rejected", payment["user_id"], f"payment={payment_id}")
            await update.callback_query.edit_message_reply_markup(reply_markup=None)
            await update.callback_query.answer("Rejected")
            with contextlib.suppress(TelegramError):
                await context.bot.send_message(payment["user_id"], f"❌ Payment #{payment_id} was rejected.")
            return
        start = now()
        expiry = start + timedelta(days=int(p["duration_days"]))
        # Atomic subscription activation + payment status.
        con.execute("UPDATE subscriptions SET active=0 WHERE user_id=? AND active=1", (payment["user_id"],))
        con.execute("""
            INSERT INTO subscriptions(user_id,plan_id,starts_at,expires_at,active,source)
            VALUES(?,?,?,?,1,'UPI_APPROVED')
        """, (payment["user_id"], p["id"], iso(start), iso(expiry)))
        con.execute("UPDATE payments SET status='APPROVED',reviewed_at=?,reviewed_by=? WHERE id=?",
                    (iso(start), OWNER_ID, payment_id))
        con.execute("UPDATE users SET updated_at=? WHERE user_id=?", (iso(start), payment["user_id"]))
        con.commit()
    audit("payment_approved", payment["user_id"], f"payment={payment_id},plan={p['name']}")
    await update.callback_query.edit_message_reply_markup(reply_markup=None)
    await update.callback_query.answer("Approved")
    with contextlib.suppress(TelegramError):
        await context.bot.send_message(payment["user_id"],
            f"✅ <b>Payment approved!</b>\n\n💎 Plan: {esc(p['name'])}\n⏳ Expires: {esc(iso(expiry))}",
            parse_mode=ParseMode.HTML)

async def admin_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not owner(uid):
        await update.callback_query.answer("Owner only.", show_alert=True); return
    with db_connect() as con:
        users = con.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        projects = con.execute("SELECT COUNT(*) c FROM projects").fetchone()["c"]
        pending = con.execute("SELECT COUNT(*) c FROM payments WHERE status='PENDING'").fetchone()["c"]
    text = (
        f"👑 <b>Admin Dashboard</b>\n\n"
        f"👥 Users: {users}\n🚀 Projects: {projects}\n💳 Pending payments: {pending}\n"
        f"🛠 Maintenance: {'ON' if setting('maintenance','0')=='1' else 'OFF'}"
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Users", callback_data="a:users"),
         InlineKeyboardButton("🚀 Projects", callback_data="a:projects")],
        [InlineKeyboardButton("💎 Plans", callback_data="a:plans"),
         InlineKeyboardButton("💳 Payments", callback_data="a:payments")],
        [InlineKeyboardButton("💰 Wallet", callback_data="a:wallet"),
         InlineKeyboardButton("📢 Broadcast", callback_data="a:broadcast")],
        [InlineKeyboardButton("🛠 Maintenance", callback_data="a:maintenance"),
         InlineKeyboardButton("📊 Statistics", callback_data="a:stats")],
        [InlineKeyboardButton("💾 Backup", callback_data="a:backup"),
         InlineKeyboardButton("⚙️ Settings", callback_data="a:settings")],
        [InlineKeyboardButton("⬅️ Back", callback_data="home")]
    ])
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id): return
    with db_connect() as con:
        users = con.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        projects = con.execute("SELECT COUNT(*) c FROM projects").fetchone()["c"]
        online = con.execute("SELECT COUNT(*) c FROM projects WHERE status='ONLINE'").fetchone()["c"]
        payments = con.execute("SELECT COUNT(*) c FROM payments WHERE status='APPROVED'").fetchone()["c"]
    text = f"📊 <b>Statistics</b>\n\n👥 Users: {users}\n🚀 Projects: {projects}\n🟢 Online: {online}\n💳 Approved payments: {payments}"
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=back_admin())

def back_admin():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin", callback_data="admin")]])

async def admin_payments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with db_connect() as con:
        rows = con.execute("""
            SELECT p.*,pl.name plan FROM payments p JOIN plans pl ON pl.id=p.plan_id
            WHERE p.status='PENDING' ORDER BY p.id DESC LIMIT 20
        """).fetchall()
    if not rows:
        text = "💳 <b>Pending Payments</b>\n\nNo pending payments."
        markup = back_admin()
    else:
        text = "💳 <b>Pending Payments</b>\n\n"
        buttons = []
        for r in rows:
            text += f"#{r['id']} • User <code>{r['user_id']}</code> • {esc(r['plan'])} • ₹{r['amount']:.2f} • <code>{esc(r['utr'])}</code>\n"
            buttons.append([
                InlineKeyboardButton(f"✅ #{r['id']}", callback_data=f"pay:approve:{r['id']}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"pay:reject:{r['id']}")
            ])
        buttons.append([InlineKeyboardButton("⬅️ Admin", callback_data="admin")])
        markup = InlineKeyboardMarkup(buttons)
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with db_connect() as con:
        rows = con.execute("SELECT user_id,username,banned,balance FROM users ORDER BY created_at DESC LIMIT 25").fetchall()
    text = "👥 <b>Users</b>\n\n" + "\n".join(
        f"• <code>{r['user_id']}</code> @{esc(r['username'] or '—')} | ₹{r['balance']:.2f} | {'BANNED' if r['banned'] else 'OK'}"
        for r in rows
    )
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=back_admin())

async def admin_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    current = setting("maintenance", "0") == "1"
    new = "0" if current else "1"
    with db_connect() as con:
        con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('maintenance',?)", (new,))
        con.commit()
    audit("maintenance", "", f"set={new}")
    await update.callback_query.answer(f"Maintenance {'OFF' if new=='0' else 'ON'}")
    await admin_page(update, context)

async def create_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id): return
    stamp = now().strftime("%Y%m%d_%H%M%S")
    out = BACKUPS_DIR / f"backup_{stamp}.zip"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(DB_PATH, arcname="hosting.db")
        for p in (PROJECTS_DIR).rglob("*"):
            if p.is_file() and not p.is_symlink():
                z.write(p, arcname=str(Path("projects") / p.relative_to(PROJECTS_DIR)))
    audit("backup", "", out.name)
    await update.callback_query.message.reply_document(open(out, "rb"), filename=out.name)
    await update.callback_query.answer("Backup created.")

async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id): return
    context.user_data["broadcast"] = True
    await update.callback_query.edit_message_text("📢 Send the message to broadcast.\n\nSend /cancel to abort.", reply_markup=back_admin())

async def do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id) or not context.user_data.pop("broadcast", False):
        return False
    with db_connect() as con:
        users = [r["user_id"] for r in con.execute("SELECT user_id FROM users WHERE banned=0")]
    ok = fail = 0
    status = await update.message.reply_text(f"📢 Broadcasting to {len(users)} users...")
    for uid in users:
        try:
            await update.message.copy(chat_id=uid)
            ok += 1
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            try:
                await update.message.copy(chat_id=uid); ok += 1
            except TelegramError:
                fail += 1
        except (Forbidden, TelegramError):
            fail += 1
        await asyncio.sleep(0.05)
    await status.edit_text(f"📢 <b>Broadcast complete</b>\n\n✅ Sent: {ok}\n❌ Failed: {fail}", parse_mode=ParseMode.HTML)
    audit("broadcast", "", f"sent={ok},failed={fail}")
    return True

async def command_project_action(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    uid = update.effective_user.id
    if not context.args:
        await update.message.reply_text(f"Usage: /{action} PROJECT_ID")
        return
    pid = context.args[0]
    row = project_row(uid, pid)
    if not row:
        await update.message.reply_text("❌ Project not found.")
        return
    if action == "restart":
        await stop_project(uid, pid)
        ok, msg = await start_project(uid, pid)
    elif action == "stop":
        ok, msg = await stop_project(uid, pid)
    else:
        root = Path(row["path"])
        await stop_project(uid, pid)
        with db_connect() as con:
            con.execute("DELETE FROM projects WHERE id=? AND user_id=?", (pid, uid))
            con.commit()
        shutil.rmtree(root, ignore_errors=True)
        ok, msg = True, "Project deleted."
    await update.message.reply_text(("✅ " if ok else "❌ ") + msg)

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ""
    uid = q.from_user.id
    ensure_user(q.from_user)
    if data == "home":
        await q.answer(); await send_home(update, context); return
    if data == "host":
        await host_prompt(update, context); return
    if data == "projects":
        await q.answer(); await projects_page(update, context); return
    if data == "plans":
        await q.answer(); await plans_page(update, context); return
    if data == "wallet":
        await q.answer(); await wallet_page(update, context); return
    if data == "account":
        await q.answer(); await account_page(update, context); return
    if data == "usage":
        await q.answer(); await usage_page(update, context); return
    if data == "support":
        await q.answer()
        target = f"https://t.me/{SUPPORT_USERNAME}" if SUPPORT_USERNAME else None
        kb = [[InlineKeyboardButton("🆘 Contact Support", url=target)]] if target else []
        kb.append([InlineKeyboardButton("⬅️ Back", callback_data="home")])
        await q.edit_message_text("🆘 <b>Support</b>\n\nContact the configured support account.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        return
    if data == "admin":
        await q.answer(); await admin_page(update, context); return
    if data.startswith("buy:"):
        await q.answer(); await payment_buy(update, int(data.split(":")[1])); return
    if data.startswith("utr:"):
        await utr_start(update, context, int(data.split(":")[1])); return
    if data.startswith("pay:"):
        _, act, ident = data.split(":")
        await approve_payment(update, int(ident), act == "approve"); return
    if data.startswith("show:"):
        await q.answer(); await show_project(update, data.split(":",1)[1]); return
    if data.startswith("p:"):
        _, action, pid = data.split(":", 2)
        row = project_row(uid, pid)
        if not row:
            await q.answer("Not your project.", show_alert=True); return
        if action == "start":
            ok, msg = await start_project(uid, pid)
            await q.answer(msg[:180], show_alert=not ok); await show_project(update, pid)
        elif action == "stop":
            ok, msg = await stop_project(uid, pid)
            await q.answer(msg[:180]); await show_project(update, pid)
        elif action == "restart":
            await stop_project(uid, pid); ok, msg = await start_project(uid, pid)
            await q.answer(msg[:180], show_alert=not ok); await show_project(update, pid)
        elif action == "logs":
            await q.answer(); await project_logs(update, pid)
        elif action == "clearlogs":
            with contextlib.suppress(OSError):
                (LOGS_DIR / f"{pid}.log").unlink()
            await q.answer("Logs cleared"); await project_logs(update, pid)
        elif action == "downloadlogs":
            f = LOGS_DIR / f"{pid}.log"
            if f.exists():
                await q.message.reply_document(open(f, "rb"), filename=f"{row['name']}_logs.txt")
            await q.answer()
        elif action == "download":
            await q.answer("Preparing archive...")
            await send_project_download(q, uid, pid)
        elif action == "stats":
            await q.answer()
            root = Path(row["path"])
            await q.edit_message_text(
                f"📊 <b>{esc(row['name'])} Stats</b>\n\n"
                f"Status: {esc(row['status'])}\nPID: {row['pid'] or '—'}\n"
                f"Disk: {directory_size(root)/1024/1024:.2f} MB",
                parse_mode=ParseMode.HTML, reply_markup=back_admin() if False else project_keyboard(pid, uid)
            )
        elif action == "delete":
            await q.answer()
            await stop_project(uid, pid)
            root = Path(row["path"])
            with db_connect() as con:
                con.execute("DELETE FROM projects WHERE id=? AND user_id=?", (pid, uid)); con.commit()
            shutil.rmtree(root, ignore_errors=True)
            await q.edit_message_text("🗑 Project deleted.", reply_markup=back_menu())
        else:
            await q.answer("This action is not available in this build.", show_alert=True)
        return
    if data == "a:users":
        await q.answer(); await admin_users(update, context); return
    if data == "a:payments":
        await q.answer(); await admin_payments(update, context); return
    if data == "a:stats":
        await q.answer(); await admin_stats(update, context); return
    if data == "a:maintenance":
        await admin_maintenance(update, context); return
    if data == "a:backup":
        await create_backup(update, context); return
    if data == "a:broadcast":
        await q.answer(); await broadcast_start(update, context); return
    if data in ("a:projects", "a:plans", "a:wallet", "a:settings"):
        await q.answer("Use the corresponding user section or owner commands in this build.", show_alert=True); return
    await q.answer("Unknown action.", show_alert=True)

async def generic_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await do_broadcast(update, context):
        return
    if await utr_message(update, context):
        return

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("payment_plan_id", None)
    context.user_data.pop("broadcast", None)
    await update.message.reply_text("❌ Cancelled.", reply_markup=main_menu(update.effective_user.id))

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id):
        await update.message.reply_text("🚫 Owner only."); return
    await update.message.reply_text("👑 Admin Panel", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Open Dashboard", callback_data="admin")]]))

async def giveplan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id) or len(context.args) < 2:
        await update.message.reply_text("Usage: /giveplan USER_ID PLAN"); return
    try: uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID."); return
    plan_name = " ".join(context.args[1:]).upper()
    with db_connect() as con:
        p = con.execute("SELECT * FROM plans WHERE name=?", (plan_name,)).fetchone()
        if not p: await update.message.reply_text("Plan not found."); return
        start = now(); end = start + timedelta(days=int(p["duration_days"]))
        con.execute("UPDATE subscriptions SET active=0 WHERE user_id=? AND active=1", (uid,))
        con.execute("INSERT INTO subscriptions(user_id,plan_id,starts_at,expires_at,active,source) VALUES(?,?,?,?,1,'ADMIN')",
                    (uid,p["id"],iso(start),iso(end)))
        con.commit()
    audit("giveplan", uid, plan_name)
    await update.message.reply_text("✅ Plan activated.")

async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, kind: str):
    if not owner(update.effective_user.id) or len(context.args) < 2:
        await update.message.reply_text(f"Usage: /{kind} USER_ID AMOUNT"); return
    try: uid, amount = int(context.args[0]), float(context.args[1])
    except ValueError:
        await update.message.reply_text("Invalid values."); return
    with db_connect() as con:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute("SELECT balance FROM users WHERE user_id=?", (uid,)).fetchone()
        if not row: con.rollback(); await update.message.reply_text("User not found."); return
        old = row["balance"]
        if kind == "addbalance": new = old + amount
        elif kind == "removebalance": new = old - amount
        else: new = amount
        new = max(0, round(new, 2))
        con.execute("UPDATE users SET balance=?,updated_at=? WHERE user_id=?", (new,iso(now()),uid))
        con.execute("INSERT INTO transactions(user_id,kind,amount,balance_after,note,created_at) VALUES(?,?,?,?,?,?)",
                    (uid,kind,amount,new,"Admin wallet adjustment",iso(now())))
        con.commit()
    audit(kind, uid, str(amount))
    await update.message.reply_text(f"✅ Balance updated: ₹{new:.2f}")

async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id) or not context.args: return
    uid = int(context.args[0])
    with db_connect() as con:
        con.execute("UPDATE users SET banned=1,updated_at=? WHERE user_id=?", (iso(now()),uid)); con.commit()
    audit("ban", uid)
    await update.message.reply_text("✅ Banned.")

async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id) or not context.args: return
    uid = int(context.args[0])
    with db_connect() as con:
        con.execute("UPDATE users SET banned=0,updated_at=? WHERE user_id=?", (iso(now()),uid)); con.commit()
    audit("unban", uid)
    await update.message.reply_text("✅ Unbanned.")

async def maintenance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id) or not context.args or context.args[0] not in ("on","off"):
        await update.message.reply_text("Usage: /maintenance on|off"); return
    val = "1" if context.args[0] == "on" else "0"
    with db_connect() as con:
        con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('maintenance',?)",(val,)); con.commit()
    audit("maintenance", "", val)
    await update.message.reply_text(f"🛠 Maintenance {'ON' if val=='1' else 'OFF'}")

async def allprojects_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id): return
    rows = all_project_rows()
    text = "\n".join(f"{r['id']} | {r['user_id']} | {r['name']} | {r['status']}" for r in rows) or "No projects."
    await update.message.reply_text(f"<pre>{esc(text[-12000:])}</pre>", parse_mode=ParseMode.HTML)

async def stopall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id): return
    for r in all_project_rows():
        await stop_project(r["user_id"], r["id"])
    await update.message.reply_text("🛑 All projects stopped.")

async def restartall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id): return
    results = []
    for r in all_project_rows():
        await stop_project(r["user_id"], r["id"])
        ok, msg = await start_project(r["user_id"], r["id"])
        results.append(f"{r['name']}: {'OK' if ok else msg}")
    await update.message.reply_text("🔁 Restart all complete.\n" + "\n".join(results[-50:]))

async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not owner(update.effective_user.id): return
    stamp = now().strftime("%Y%m%d_%H%M%S")
    out = BACKUPS_DIR / f"backup_{stamp}.zip"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(DB_PATH, "hosting.db")
        for p in PROJECTS_DIR.rglob("*"):
            if p.is_file() and not p.is_symlink():
                z.write(p, str(Path("projects") / p.relative_to(PROJECTS_DIR)))
    await update.message.reply_document(open(out, "rb"), filename=out.name)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled Telegram error: %s\n%s", context.error, traceback.format_exc())

async def post_init(app: Application):
    init_db()
    # Reconcile stale metadata after a bot restart.
    with db_connect() as con:
        con.execute("UPDATE projects SET pid=NULL,status='STOPPED',updated_at=? WHERE status IN ('ONLINE','STARTING')",
                    (iso(now()),))
        con.commit()
    log.info("%s started. Data directory: %s", APP_NAME, BASE_DIR)

async def post_shutdown(app: Application):
    for pid, proc in list(processes.items()):
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGTERM)
    await asyncio.sleep(1)

def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured.")
    if OWNER_ID <= 0:
        raise RuntimeError("OWNER_ID is not configured.")
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("projects", lambda u,c: projects_page(u,c)))
    app.add_handler(CommandHandler("plans", lambda u,c: plans_page(u,c)))
    app.add_handler(CommandHandler("wallet", lambda u,c: wallet_page(u,c)))
    app.add_handler(CommandHandler("account", lambda u,c: account_page(u,c)))
    app.add_handler(CommandHandler("usage", lambda u,c: usage_page(u,c)))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("giveplan", giveplan_cmd))
    app.add_handler(CommandHandler("addbalance", lambda u,c: balance_cmd(u,c,"addbalance")))
    app.add_handler(CommandHandler("removebalance", lambda u,c: balance_cmd(u,c,"removebalance")))
    app.add_handler(CommandHandler("setbalance", lambda u,c: balance_cmd(u,c,"setbalance")))
    app.add_handler(CommandHandler("ban", ban_cmd))
    app.add_handler(CommandHandler("unban", unban_cmd))
    app.add_handler(CommandHandler("maintenance", maintenance_cmd))
    app.add_handler(CommandHandler("allprojects", allprojects_cmd))
    app.add_handler(CommandHandler("stopall", stopall_cmd))
    app.add_handler(CommandHandler("restartall", restartall_cmd))
    app.add_handler(CommandHandler("backup", backup_cmd))
    app.add_handler(CommandHandler("restart", lambda u,c: command_project_action(u,c,"restart")))
    app.add_handler(CommandHandler("stop", lambda u,c: command_project_action(u,c,"stop")))
    app.add_handler(CommandHandler("delete", lambda u,c: command_project_action(u,c,"delete")))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.Document.ALL, deploy_from_file))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, generic_text))
    app.add_error_handler(error_handler)
    return app

if __name__ == "__main__":
    application = build_app()
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
