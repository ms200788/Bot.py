#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Upload Bot - single-file deployment
- Fixed: health server binds inside on_startup so Render detects port (no port-scan errors).
- Fixed: /upload -> /d flow reliably finalizes and saves sessions.
- Persistent jobstore for APScheduler so auto-delete jobs survive restarts.
- Vault channel storage for permanent Telegram-native file retention.
- All outgoing user messages are sent without HTML parsing to avoid CantParseEntities.
- Requirements and Dockerfile notes at bottom.
"""

# =============================================================
# Standard library imports
# =============================================================
import os
import sys
import time
import logging
import sqlite3
import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

# =============================================================
# Third-party imports
# =============================================================
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.deep_linking import get_start_link, decode_payload
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from aiohttp import web

# =============================================================
# Configuration (read env)
# =============================================================
def required_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"ERROR: {name} environment variable is required", file=sys.stderr)
        sys.exit(1)
    return val

BOT_TOKEN = required_env("BOT_TOKEN")
try:
    OWNER_ID = int(required_env("OWNER_ID"))
except ValueError:
    print("ERROR: OWNER_ID must be integer", file=sys.stderr)
    sys.exit(1)

try:
    VAULT_CHANNEL_ID = int(required_env("VAULT_CHANNEL_ID"))
except ValueError:
    print("ERROR: VAULT_CHANNEL_ID must be integer", file=sys.stderr)
    sys.exit(1)

DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", "10000"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))

# =============================================================
# Logging
# =============================================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
)
logger = logging.getLogger("telegram-upload-bot")

# =============================================================
# SQLite Database wrapper
# =============================================================
class Database:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        # users
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_active INTEGER
        );
        """)
        # start message
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS start_message (
            id INTEGER PRIMARY KEY,
            content TEXT,
            file_id TEXT
        );
        """)
        # sessions
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER,
            created_at INTEGER,
            protect INTEGER,
            auto_delete INTEGER,
            header_vault_msg_id INTEGER,
            link_vault_msg_id INTEGER,
            revoked INTEGER DEFAULT 0
        );
        """)
        # files
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            vault_msg_id INTEGER,
            file_id TEXT,
            file_type TEXT,
            caption TEXT,
            position INTEGER
        );
        """)
        # access log
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS access_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            user_id INTEGER,
            accessed_at INTEGER
        );
        """)
        self.conn.commit()

    # --- users ---
    def add_or_update_user(self, user_id: int, username: Optional[str], first_name: Optional[str]):
        now = int(time.time())
        self.cur.execute("""
        INSERT INTO users (user_id, username, first_name, last_active)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_active=excluded.last_active
        ;
        """, (user_id, username or "", first_name or "", now))
        self.conn.commit()

    def touch_user(self, user_id: int):
        now = int(time.time())
        self.cur.execute("UPDATE users SET last_active=? WHERE user_id=?", (now, user_id))
        self.conn.commit()

    def get_all_user_ids(self) -> List[int]:
        rows = self.cur.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]

    def count_users(self) -> int:
        return self.cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    def count_active_2days(self) -> int:
        cutoff = int(time.time()) - 2 * 86400
        return self.cur.execute("SELECT COUNT(*) FROM users WHERE last_active >= ?", (cutoff,)).fetchone()[0]

    # --- start message ---
    def set_start_message(self, content: str, file_id: Optional[str]):
        self.cur.execute("DELETE FROM start_message")
        self.cur.execute("INSERT INTO start_message (id, content, file_id) VALUES (1, ?, ?)", (content, file_id))
        self.conn.commit()

    def get_start_message(self) -> Tuple[str, Optional[str]]:
        row = self.cur.execute("SELECT content, file_id FROM start_message WHERE id=1").fetchone()
        if row:
            return row["content"], row["file_id"]
        return "Welcome, {username}!", None

    # --- sessions & files ---
    def create_session(self, owner_id: int, protect: int, auto_delete: int, header_vault_msg_id: int, link_vault_msg_id: int) -> int:
        now = int(time.time())
        self.cur.execute("""
        INSERT INTO sessions (owner_id, created_at, protect, auto_delete, header_vault_msg_id, link_vault_msg_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (owner_id, now, protect, auto_delete, header_vault_msg_id, link_vault_msg_id))
        sid = self.cur.lastrowid
        self.conn.commit()
        return sid

    def add_file(self, session_id: int, vault_msg_id: int, file_id: str, file_type: str, caption: str, position: int):
        self.cur.execute("""
        INSERT INTO files (session_id, vault_msg_id, file_id, file_type, caption, position)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (session_id, vault_msg_id, file_id, file_type, caption, position))
        self.conn.commit()

    def get_session(self, session_id: int) -> Optional[sqlite3.Row]:
        return self.cur.execute("SELECT * FROM sessions WHERE id=? LIMIT 1", (session_id,)).fetchone()

    def get_files_for_session(self, session_id: int) -> List[sqlite3.Row]:
        return self.cur.execute("SELECT * FROM files WHERE session_id=? ORDER BY position ASC", (session_id,)).fetchall()

    def log_access(self, session_id: int, user_id: int):
        now = int(time.time())
        self.cur.execute("INSERT INTO access_log (session_id, user_id, accessed_at) VALUES (?, ?, ?)", (session_id, user_id, now))
        self.conn.commit()

    def revoke_session(self, session_id: int):
        self.cur.execute("UPDATE sessions SET revoked=1 WHERE id=?", (session_id,))
        self.conn.commit()

    def is_revoked(self, session_id: int) -> bool:
        row = self.cur.execute("SELECT revoked FROM sessions WHERE id=?", (session_id,)).fetchone()
        return bool(row["revoked"]) if row else True

    def count_files(self) -> int:
        return self.cur.execute("SELECT COUNT(*) FROM files").fetchone()[0]

    def count_sessions(self) -> int:
        return self.cur.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]

    def list_sessions(self) -> List[sqlite3.Row]:
        return self.cur.execute("SELECT * FROM sessions ORDER BY created_at DESC").fetchall()

    def close(self):
        try:
            self.conn.commit()
            self.conn.close()
        except Exception:
            pass

# instantiate DB
db = Database(DB_PATH)

# =============================================================
# APScheduler with persistent jobstore (SQLAlchemy)
# =============================================================
os.makedirs(os.path.dirname(JOB_DB_PATH) or ".", exist_ok=True)
jobstore_url = f"sqlite:///{os.path.abspath(JOB_DB_PATH)}"
jobstores = {'default': SQLAlchemyJobStore(url=jobstore_url)}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.start()

# =============================================================
# Bot setup
# - Use parse_mode=None by default when sending messages to users to avoid CantParseEntities.
# =============================================================
bot = Bot(token=BOT_TOKEN, parse_mode=None)
dp = Dispatcher(bot, storage=MemoryStorage())
broadcast_semaphore = asyncio.Semaphore(BROADCAST_CONCURRENCY)

# =============================================================
# aiohttp health app (we will start it in on_startup)
# =============================================================
async def health_handler(request):
    return web.Response(text="ok")

health_app = web.Application()
health_app.router.add_get("/health", health_handler)

# =============================================================
# FSM states
# =============================================================
class UploadStates(StatesGroup):
    waiting_files = State()
    choosing_protect = State()
    choosing_timer = State()

# =============================================================
# In-memory staging for uploads (owner only)
# =============================================================
# Structure: { owner_id: {"items":[{"from_chat_id":..., "message_id":...}, ...], "protect": None, "auto_delete": None } }
upload_sessions: Dict[int, Dict[str, Any]] = {}

# =============================================================
# Helpers
# =============================================================
def is_owner(uid: int) -> bool:
    return int(uid) == int(OWNER_ID)

def extract_media_info_from_message(msg: types.Message) -> Tuple[str, Optional[str], Optional[str]]:
    if msg.photo:
        return "photo", msg.photo[-1].file_id, msg.caption or ""
    if msg.video:
        return "video", msg.video.file_id, msg.caption or ""
    if msg.document:
        return "document", msg.document.file_id, msg.caption or ""
    if msg.audio:
        return "audio", msg.audio.file_id, msg.caption or ""
    if msg.voice:
        return "voice", msg.voice.file_id, msg.caption or ""
    if msg.sticker:
        return "sticker", msg.sticker.file_id, ""
    # fallback to text
    return "text", msg.text or msg.caption or "", msg.text or msg.caption or ""

async def send_welcome(user: types.User, chat_id: int):
    content, file_id = db.get_start_message()
    username = user.username or user.first_name or "there"
    rendered = content.replace("{username}", username)
    try:
        if file_id:
            await bot.send_photo(chat_id, file_id, caption=rendered)
        else:
            await bot.send_message(chat_id, rendered)
    except Exception:
        try:
            await bot.send_message(chat_id, rendered)
        except Exception:
            pass

# =============================================================
# Persistent deletion worker functions
# - scheduler will store jobs that call _delete_messages_job_runner
# - this runner schedules the actual async delete coroutine on the bot's event loop
# =============================================================
def _delete_messages_job_runner(chat_id: int, message_ids: List[int]):
    """
    Synchronous function called by APScheduler from the jobstore.
    It schedules the asynchronous deletion on the bot's event loop.
    Stored in jobstore by function path so it must be top-level in this module.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    # schedule coroutine thread-safely
    asyncio.run_coroutine_threadsafe(_delete_messages_async(chat_id, message_ids), loop)

async def _delete_messages_async(chat_id: int, message_ids: List[int]):
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id, mid)
        except Exception as exc:
            logger.debug("Failed to delete message %s in chat %s: %s", mid, chat_id, exc)

def schedule_persistent_deletion(chat_id: int, message_ids: List[int], seconds: int):
    if seconds <= 0 or not message_ids:
        return
    run_date = datetime.utcnow() + timedelta(seconds=seconds)
    # schedule the synchronous runner so jobstore can serialize its path
    scheduler.add_job(_delete_messages_job_runner, "date", run_date=run_date, args=[chat_id, message_ids])
    logger.info("Scheduled persistent deletion for chat %s at %s (messages: %s)", chat_id, run_date.isoformat(), message_ids)

# =============================================================
# Bot handlers
# =============================================================
@dp.message_handler(commands=["start"])
async def handler_start(message: types.Message):
    db.add_or_update_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name)
    args = message.get_args()
    if args:
        # decode payload (aiogram's get_start_link encodes payload)
        try:
            payload = decode_payload(args)
            session_id = int(payload)
        except Exception as exc:
            logger.warning("Invalid deep-link payload from %s: %s", message.from_user.id, exc)
            await message.reply("Invalid link.", parse_mode=None)
            return

        session = db.get_session(session_id)
        if not session:
            await message.reply("Session not found.", parse_mode=None)
            return

        if db.is_revoked(session_id):
            await message.reply("This session has been revoked by the owner.", parse_mode=None)
            return

        files = db.get_files_for_session(session_id)
        if not files:
            await message.reply("No files in this session.", parse_mode=None)
            return

        # log access & update last active
        db.log_access(session_id, message.from_user.id)
        db.touch_user(message.from_user.id)

        protect_flag = bool(session["protect"]) and (not is_owner(message.from_user.id))
        auto_delete_seconds = int(session["auto_delete"]) if session["auto_delete"] else 0

        delivered_message_ids: List[int] = []
        for f in files:
            ftype = f["file_type"]
            fid = f["file_id"]
            caption = f["caption"] or ""
            try:
                if ftype == "photo":
                    msg = await bot.send_photo(message.chat.id, fid, caption=caption, protect_content=protect_flag)
                elif ftype == "video":
                    msg = await bot.send_video(message.chat.id, fid, caption=caption, protect_content=protect_flag)
                elif ftype == "document":
                    msg = await bot.send_document(message.chat.id, fid, caption=caption, protect_content=protect_flag)
                elif ftype == "audio":
                    msg = await bot.send_audio(message.chat.id, fid, caption=caption, protect_content=protect_flag)
                elif ftype == "voice":
                    msg = await bot.send_voice(message.chat.id, fid, caption=caption, protect_content=protect_flag)
                elif ftype == "sticker":
                    msg = await bot.send_sticker(message.chat.id, fid)
                elif ftype == "text":
                    msg = await bot.send_message(message.chat.id, caption or fid)
                else:
                    msg = await bot.send_message(message.chat.id, caption or fid)
                if msg:
                    delivered_message_ids.append(msg.message_id)
            except Exception as exc:
                logger.exception("Error delivering file %s: %s", fid, exc)
                # continue with next file

        if auto_delete_seconds > 0 and delivered_message_ids:
            schedule_persistent_deletion(message.chat.id, delivered_message_ids, auto_delete_seconds)
        return

    # plain start
    await send_welcome(message.from_user, message.chat.id)

@dp.message_handler(commands=["help"])
async def handler_help(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    text = (
        "Owner Commands:\n"
        "/setimage - Reply to message (text/photo) to set welcome message (use {username}).\n"
        "/upload - Start multi-file upload session (owner only).\n"
        "/d - Finish upload and save session (after sending files).\n"
        "/e - Cancel current upload session.\n"
        "/broadcast - Reply to a message to broadcast to all users.\n"
        "/stats - Show usage stats.\n"
        "/list_sessions - List saved sessions.\n"
        "/revoke <id> - Revoke a session.\n"
        "/help - Show this help.\n\n"
        "Public:\n"
        "/start - Show welcome or open a session via deep link."
    )
    await message.reply(text, parse_mode=None)

@dp.message_handler(commands=["setimage"])
async def handler_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    if not message.reply_to_message:
        await message.reply("Reply to a text or photo to set the welcome message.", parse_mode=None)
        return
    reply = message.reply_to_message
    content = reply.caption or reply.text or ""
    file_id = None
    if reply.photo:
        file_id = reply.photo[-1].file_id
    db.set_start_message(content, file_id)
    await message.reply("Start message updated.", parse_mode=None)

# =============================================================
# Upload flow
# =============================================================
@dp.message_handler(commands=["upload"])
async def handler_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    upload_sessions[message.from_user.id] = {"items": [], "protect": None, "auto_delete": None}
    await UploadStates.waiting_files.set()
    await message.reply("Upload session started. Send files now (photos, videos, docs, text). When done, send /d to finish or /e to cancel.", parse_mode=None)

@dp.message_handler(commands=["e"], state=UploadStates.waiting_files)
async def handler_cancel_upload(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    upload_sessions.pop(message.from_user.id, None)
    await state.finish()
    await message.reply("Upload session canceled.", parse_mode=None)

@dp.message_handler(content_types=types.ContentType.ANY, state=UploadStates.waiting_files)
async def handler_collect_files(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    owner = message.from_user.id
    sess = upload_sessions.get(owner)
    if sess is None:
        return
    # store original message identifiers
    sess["items"].append({"from_chat_id": message.chat.id, "message_id": message.message_id})
    await message.reply("File added to session.", parse_mode=None)

@dp.message_handler(commands=["d"], state=UploadStates.waiting_files)
async def handler_finish_prompt_protect(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    owner = message.from_user.id
    sess = upload_sessions.get(owner)
    if not sess or not sess.get("items"):
        await message.reply("No files uploaded in this session. Use /upload then send files.", parse_mode=None)
        return
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Protect ON", callback_data="protect_on"))
    kb.add(InlineKeyboardButton("Protect OFF", callback_data="protect_off"))
    await message.reply("Protect content? (prevents forwarding/downloading for non-owner). Choose an option.", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("protect_"))
async def handler_protect_choice(callback: types.CallbackQuery):
    owner = callback.from_user.id
    if owner not in upload_sessions:
        await callback.answer("Session not found.", show_alert=True)
        return
    upload_sessions[owner]["protect"] = 1 if callback.data == "protect_on" else 0
    await UploadStates.choosing_timer.set()
    await callback.message.edit_text("Enter auto-delete timer in hours (0 for none). Range 0 - 168. Example: 10")

@dp.message_handler(lambda m: m.from_user.id in upload_sessions and upload_sessions[m.from_user.id]["protect"] is not None, state=UploadStates.choosing_timer)
async def handler_timer_input(message: types.Message, state: FSMContext):
    owner = message.from_user.id
    if owner not in upload_sessions:
        await message.reply("Session not found.", parse_mode=None)
        await state.finish()
        return
    info = upload_sessions[owner]
    try:
        hours = float(message.text.strip())
    except Exception:
        await message.reply("Invalid hours. Send a number between 0 and 168.", parse_mode=None)
        return
    if hours < 0 or hours > 168:
        await message.reply("Hours out of range (0-168).", parse_mode=None)
        return
    auto_delete_seconds = int(hours * 3600)
    protect_flag = int(info.get("protect", 0))
    items = info.get("items", [])

    # create header placeholder in vault channel
    try:
        header_msg = await bot.send_message(VAULT_CHANNEL_ID, "Preparing session...")
    except Exception as exc:
        logger.exception("Failed to send header to vault: %s", exc)
        await message.reply("Failed to write to vault channel. Check VAULT_CHANNEL_ID and bot admin rights.", parse_mode=None)
        await state.finish()
        return

    # create link placeholder
    try:
        link_placeholder = await bot.send_message(VAULT_CHANNEL_ID, "Preparing session link...")
    except Exception as exc:
        logger.exception("Failed to create link placeholder: %s", exc)
        await message.reply("Failed to write to vault channel.", parse_mode=None)
        await state.finish()
        return

    # copy items into vault preserving original captions
    copied_info = []
    pos = 0
    for it in items:
        from_chat = it["from_chat_id"]
        mid = it["message_id"]
        try:
            copied = await bot.copy_message(chat_id=VAULT_CHANNEL_ID, from_chat_id=from_chat, message_id=mid)
            ftype, fid, caption = extract_media_info_from_message(copied)
            copied_info.append({
                "vault_msg_id": copied.message_id,
                "file_id": fid,
                "file_type": ftype,
                "caption": caption or "",
                "position": pos
            })
            pos += 1
            await asyncio.sleep(0.06)
        except Exception as exc:
            logger.exception("Failed to copy message %s from %s into vault: %s", mid, from_chat, exc)
            # continue copying others

    # persist session in DB
    try:
        session_id = db.create_session(owner, protect_flag, auto_delete_seconds, header_msg.message_id, link_placeholder.message_id)
    except Exception as exc:
        logger.exception("Failed to create session in DB: %s", exc)
        await message.reply("Failed to create session record.", parse_mode=None)
        await state.finish()
        return

    # edit header to include session id
    try:
        await bot.edit_message_text(f"📦 Session {session_id}", chat_id=VAULT_CHANNEL_ID, message_id=header_msg.message_id)
    except Exception as exc:
        logger.exception("Failed to edit header: %s", exc)

    # save files metadata
    for ci in copied_info:
        try:
            db.add_file(session_id, ci["vault_msg_id"], ci["file_id"], ci["file_type"], ci["caption"], ci["position"])
        except Exception as exc:
            logger.exception("Failed to add file metadata for session %s: %s", session_id, exc)

    # build deep link
    try:
        start_link = await get_start_link(str(session_id), encode=True)
    except Exception as exc:
        logger.exception("Failed to build start link: %s", exc)
        me = await bot.get_me()
        start_link = f"https://t.me/{me.username}?start={session_id}"

    # update link placeholder with start link
    try:
        await bot.edit_message_text(f"🔗 Files saved in Session {session_id}: {start_link}", chat_id=VAULT_CHANNEL_ID, message_id=link_placeholder.message_id)
    except Exception as exc:
        logger.exception("Failed to update link placeholder: %s", exc)

    # notify owner
    try:
        await message.reply(f"Upload complete. Session {session_id} link:\n{start_link}", parse_mode=None)
    except Exception:
        pass

    # clear in-memory session and finish FSM
    upload_sessions.pop(owner, None)
    await state.finish()

# =============================================================
# Broadcast / stats / list / revoke
# =============================================================
@dp.message_handler(commands=["broadcast"])
async def handler_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message to broadcast.", parse_mode=None)
        return
    users = db.get_all_user_ids()
    if not users:
        await message.reply("No users to broadcast to.", parse_mode=None)
        return
    await message.reply(f"Starting broadcast to {len(users)} users...", parse_mode=None)
    sent = 0
    failed = 0
    async def send_to(uid:int):
        nonlocal sent, failed
        async with broadcast_semaphore:
            try:
                await message.reply_to_message.copy_to(uid)
                sent += 1
            except (BotBlocked, ChatNotFound):
                failed += 1
            except RetryAfter as e:
                await asyncio.sleep(e.timeout + 1)
                try:
                    await message.reply_to_message.copy_to(uid)
                    sent += 1
                except Exception:
                    failed += 1
            except Exception:
                failed += 1
    tasks = [asyncio.create_task(send_to(uid)) for uid in users]
    await asyncio.gather(*tasks)
    await message.reply(f"Broadcast finished. Sent: {sent}, Failed: {failed}", parse_mode=None)

@dp.message_handler(commands=["stats"])
async def handler_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    total_users = db.count_users()
    active_2d = db.count_active_2days()
    total_files = db.count_files()
    total_sessions = db.count_sessions()
    await message.reply(f"Users active (2d): {active_2d}\nTotal users: {total_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}", parse_mode=None)

@dp.message_handler(commands=["list_sessions"])
async def handler_list_sessions(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    rows = db.list_sessions()
    if not rows:
        await message.reply("No sessions found.", parse_mode=None)
        return
    parts = []
    for r in rows:
        created = datetime.utcfromtimestamp(r["created_at"]).isoformat() + "Z"
        parts.append(f"ID:{r['id']} owner:{r['owner_id']} created:{created} protect:{r['protect']} auto_delete:{r['auto_delete']} revoked:{r['revoked']}")
    chunk = ""
    for line in parts:
        if len(chunk) + len(line) + 1 > 3500:
            await message.reply(chunk, parse_mode=None)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        await message.reply(chunk, parse_mode=None)

@dp.message_handler(commands=["revoke"])
async def handler_revoke(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /revoke <session_id>", parse_mode=None)
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid session id.", parse_mode=None)
        return
    if not db.get_session(sid):
        await message.reply("Session not found.", parse_mode=None)
        return
    db.revoke_session(sid)
    await message.reply(f"Session {sid} revoked.", parse_mode=None)

# =============================================================
# Fallback collector (owner can send files outside FSM; we still collect)
# =============================================================
@dp.message_handler(content_types=types.ContentType.ANY)
async def fallback_collector(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    sess = upload_sessions.get(message.from_user.id)
    if sess is None:
        return
    sess["items"].append({"from_chat_id": message.chat.id, "message_id": message.message_id})

# =============================================================
# Startup / Shutdown
# - Start aiohttp health app inside on_startup so Render sees binding.
# =============================================================
async def on_startup_handler(dispatcher: Dispatcher):
    # start scheduler if not running
    if not scheduler.running:
        scheduler.start()
    # start aiohttp health server using AppRunner bound to 0.0.0.0:PORT
    try:
        runner = web.AppRunner(health_app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
        await site.start()
        logger.info("Health endpoint running at 0.0.0.0:%s/health", PORT)
    except Exception as exc:
        logger.exception("Failed to start health server on port %s: %s", PORT, exc)
    logger.info("Bot startup complete. Owner=%s Vault=%s DB=%s JOB_DB=%s", OWNER_ID, VAULT_CHANNEL_ID, DB_PATH, JOB_DB_PATH)

async def on_shutdown_handler(dispatcher: Dispatcher):
    logger.info("Shutting down...")
    try:
        await dispatcher.storage.close()
        await dispatcher.storage.wait_closed()
    except Exception:
        pass
    try:
        db.close()
    except Exception:
        pass
    try:
        await bot.close()
    except Exception:
        pass
    logger.info("Shutdown complete.")

# =============================================================
# Run
# =============================================================
if __name__ == "__main__":
    from aiogram import executor
    try:
        executor.start_polling(dp, on_startup=on_startup_handler, on_shutdown=on_shutdown_handler)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Exit signal received.")
    except Exception as exc:
        logger.exception("Run error: %s", exc)