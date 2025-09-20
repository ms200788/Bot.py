#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Upload Bot - Single-file with Vault Channel Storage
- Owner-only uploads to create persistent sessions stored in a private vault channel.
- Session sequence in vault:
    1) Header: "📦 Session {id}"
    2) All files copied into the vault preserving original captions
    3) Final message containing deep link for session
- When a user opens the deep link, the bot sends only the files with original captions.
- If session.protect == ON, delivered messages use protect_content=True for non-owner users.
- If session.auto_delete > 0, delivered messages in the user's chat are scheduled for deletion.
- All session/file references (vault_message_id, file_id, file_type, caption) are stored in SQLite (DB_PATH).
- Health check endpoint /health using aiohttp for uptime monitors.
- Environment variables required:
    - BOT_TOKEN (required)
    - OWNER_ID (required, integer)
    - VAULT_CHANNEL_ID (required, integer like -1001234567890)
    - DB_PATH (optional, default: /data/database.sqlite3)
    - PORT (optional, default: 10000)
- Requirements: aiogram 2.x, APScheduler, aiohttp
"""

# =============================================================
# Standard library imports
# =============================================================
import os
import sys
import time
import logging
import sqlite3
import threading
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

# =============================================================
# Third-party imports
# =============================================================
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.deep_linking import get_start_link, decode_payload
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter, CantInitiateConversation
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web

# =============================================================
# Read configuration from environment variables (no defaults for secrets)
# =============================================================
try:
    BOT_TOKEN = os.environ["BOT_TOKEN"]
except KeyError:
    print("ERROR: BOT_TOKEN environment variable is required", file=sys.stderr)
    sys.exit(1)

try:
    OWNER_ID = int(os.environ["OWNER_ID"])
except KeyError:
    print("ERROR: OWNER_ID environment variable is required", file=sys.stderr)
    sys.exit(1)
except ValueError:
    print("ERROR: OWNER_ID must be an integer", file=sys.stderr)
    sys.exit(1)

try:
    VAULT_CHANNEL_ID = int(os.environ["VAULT_CHANNEL_ID"])
except KeyError:
    print("ERROR: VAULT_CHANNEL_ID environment variable is required (private channel id like -1001234567890)", file=sys.stderr)
    sys.exit(1)
except ValueError:
    print("ERROR: VAULT_CHANNEL_ID must be an integer", file=sys.stderr)
    sys.exit(1)

DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
PORT = int(os.environ.get("PORT", "10000"))

# Broadcast concurrency — limit parallel broadcast copy to avoid flooding
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))

# Logging configuration
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
)
logger = logging.getLogger("upload-vault-bot")

# =============================================================
# Database wrapper (SQLite)
# =============================================================
class Database:
    def __init__(self, path: str):
        self.path = path
        # Ensure directory exists
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
        # start message displayed on /start (owner-settable)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS start_message (
            id INTEGER PRIMARY KEY,
            content TEXT,
            file_id TEXT
        );
        """)
        # sessions: one per upload finalize
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
        # files: each file entry references a vault message and also stores file_id/file_type/caption
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
        # access log: when user accesses a session
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS access_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            user_id INTEGER,
            accessed_at INTEGER
        );
        """)
        self.conn.commit()

    # --- users
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

    # --- start message
    def set_start_message(self, content: str, file_id: Optional[str]):
        # keep a single row (id=1)
        self.cur.execute("DELETE FROM start_message")
        self.cur.execute("INSERT INTO start_message (id, content, file_id) VALUES (1, ?, ?)", (content, file_id))
        self.conn.commit()

    def get_start_message(self) -> Tuple[str, Optional[str]]:
        row = self.cur.execute("SELECT content, file_id FROM start_message WHERE id=1").fetchone()
        if row:
            return row["content"], row["file_id"]
        return "Welcome, {username}!", None

    # --- sessions & files
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

# Instantiate DB
db = Database(DB_PATH)

# =============================================================
# Scheduler (APScheduler) for auto-delete scheduling
# - Note: Jobs are in-memory only. If bot restarts, already-scheduled deletions may be lost.
# - The files remain permanently in the vault DB.
# =============================================================
scheduler = AsyncIOScheduler()
scheduler.start()

# =============================================================
# Aiogram bot setup
# =============================================================
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())

# Semaphore for broadcast concurrency control
broadcast_semaphore = asyncio.Semaphore(BROADCAST_CONCURRENCY)

# =============================================================
# Healthcheck server using aiohttp (runs on a daemon thread)
# =============================================================
async def health_handler(request):
    return web.Response(text="ok")

def run_health_server():
    app = web.Application()
    app.router.add_get("/health", health_handler)
    logger.info("Starting health server on port %s", PORT)
    web.run_app(app, port=PORT)

health_thread = threading.Thread(target=run_health_server, daemon=True)

# =============================================================
# FSM States for Upload Flow
# =============================================================
class UploadStates(StatesGroup):
    waiting_files = State()
    choosing_protect = State()
    choosing_timer = State()

# =============================================================
# In-memory staging area for owner uploads before finalizing:
# sessions_in_memory[owner_id] = {
#    "items": [ {"chat_id": from_chat_id, "message_id": message_id, "type": 'photo'/'video'/... } ... ],
#    "created": epoch,
# }
# We store original owner message identifiers so we can copy them to the vault.
# =============================================================
sessions_in_memory: Dict[int, Dict[str, Any]] = {}

# =============================================================
# Helper utilities
# =============================================================
def is_owner(uid: int) -> bool:
    return int(uid) == int(OWNER_ID)

def safe_username(u: Optional[str], first: Optional[str]) -> str:
    if u and u.strip():
        return u
    return first or "there"

async def schedule_deletion(chat_id: int, message_ids: List[int], seconds: int):
    """
    Schedule deletion of the message ids from a chat after seconds.
    Jobs are added in-memory. If the process restarts, scheduled deletions may be lost.
    """
    if seconds <= 0 or not message_ids:
        return

    async def delete_task():
        try:
            for mid in message_ids:
                try:
                    await bot.delete_message(chat_id, mid)
                except Exception as e:
                    logger.debug("Could not delete message %s in chat %s: %s", mid, chat_id, e)
        except Exception as exc:
            logger.exception("Scheduled delete task error: %s", exc)

    run_date = datetime.utcnow() + timedelta(seconds=seconds)
    scheduler.add_job(lambda: asyncio.ensure_future(delete_task()), trigger='date', run_date=run_date)
    logger.info("Scheduled deletion of %d messages in chat %s in %s seconds", len(message_ids), chat_id, seconds)

# Helper to detect file type and safely extract file_id and caption from a Message object
def extract_media_info_from_message(msg: types.Message) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Return (file_type, file_id, caption_text)
    file_type: 'photo', 'video', 'document', 'audio', 'voice', 'sticker', 'text'
    file_id: Telegram file_id or text string for text
    caption_text: caption or text content
    """
    if msg.photo:
        file_id = msg.photo[-1].file_id
        caption = msg.caption or ""
        return "photo", file_id, caption
    if msg.video:
        file_id = msg.video.file_id
        caption = msg.caption or ""
        return "video", file_id, caption
    if msg.document:
        file_id = msg.document.file_id
        caption = msg.caption or ""
        return "document", file_id, caption
    if msg.audio:
        file_id = msg.audio.file_id
        caption = msg.caption or ""
        return "audio", file_id, caption
    if msg.voice:
        file_id = msg.voice.file_id
        caption = msg.caption or ""
        return "voice", file_id, caption
    if msg.sticker:
        file_id = msg.sticker.file_id
        caption = msg.caption or ""
        return "sticker", file_id, caption
    # fallback: plain text
    text = msg.text or msg.caption or ""
    return "text", text, text

# =============================================================
# Command Handlers
# =============================================================

@dp.message_handler(commands=["start"])
async def handle_start(message: types.Message):
    """
    Public. If args present (deep link), decode payload and deliver session files.
    Otherwise, send configured start message.
    """
    user = message.from_user
    db.add_or_update_user(user.id, user.username, user.first_name)
    args = message.get_args()
    if args:
        # decode deep link payload to session id
        try:
            payload = decode_payload(args)
            session_id = int(payload)
        except Exception as e:
            logger.warning("Invalid deep link payload from %s: %s", user.id, e)
            await message.reply("Invalid link.")
            return

        session_row = db.get_session(session_id)
        if not session_row:
            await message.reply("Session not found.")
            return

        if db.is_revoked(session_id):
            await message.reply("This session has been revoked by the owner.")
            return

        # Retrieve files for session
        files = db.get_files_for_session(session_id)
        if not files:
            await message.reply("No files found in this session.")
            return

        # Log access
        db.log_access(session_id, user.id)
        db.touch_user(user.id)

        # Determine protect flag for this user
        protect_flag = bool(session_row["protect"]) and (not is_owner(user.id))
        auto_delete_seconds = int(session_row["auto_delete"]) if session_row["auto_delete"] else 0

        delivered_message_ids: List[int] = []

        # Deliver files one by one using saved file_id and caption (preserves original captions)
        for f in files:
            ftype = f["file_type"]
            file_id = f["file_id"]
            caption = f["caption"] or ""

            try:
                if ftype == "photo":
                    msg = await bot.send_photo(message.chat.id, file_id, caption=caption, protect_content=protect_flag)
                elif ftype == "video":
                    msg = await bot.send_video(message.chat.id, file_id, caption=caption, protect_content=protect_flag)
                elif ftype == "document":
                    msg = await bot.send_document(message.chat.id, file_id, caption=caption, protect_content=protect_flag)
                elif ftype == "audio":
                    msg = await bot.send_audio(message.chat.id, file_id, caption=caption, protect_content=protect_flag)
                elif ftype == "voice":
                    msg = await bot.send_voice(message.chat.id, file_id, caption=caption, protect_content=protect_flag)
                elif ftype == "sticker":
                    # stickers can't have captions; send as sticker
                    msg = await bot.send_sticker(message.chat.id, file_id)
                elif ftype == "text":
                    msg = await bot.send_message(message.chat.id, caption or file_id)
                else:
                    # fallback
                    msg = await bot.send_message(message.chat.id, caption or file_id)
                if msg:
                    delivered_message_ids.append(msg.message_id)
            except Exception as exc:
                logger.exception("Error delivering file id %s of type %s to user %s: %s", file_id, ftype, user.id, exc)
                # continue attempting next files

        # schedule deletion if needed
        if auto_delete_seconds > 0 and delivered_message_ids:
            await schedule_deletion(message.chat.id, delivered_message_ids, auto_delete_seconds)

        # no header or extra messages per user's request — we only sent files
        return

    # Normal /start without args -> welcome message (owner-settable)
    content, file_id = db.get_start_message()
    rendered = content.replace("{username}", safe_username(user.username, user.first_name))
    try:
        if file_id:
            await bot.send_photo(message.chat.id, file_id, caption=rendered)
        else:
            await bot.send_message(message.chat.id, rendered)
    except Exception as exc:
        logger.exception("Failed to send start message: %s", exc)
        try:
            await bot.send_message(message.chat.id, rendered)
        except Exception:
            pass

@dp.message_handler(commands=["help"])
async def handle_help(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    help_text = (
        "<b>Owner Commands</b>\n"
        "/setimage - Reply to a message (photo or text) to set the start message (use {username}).\n"
        "/upload - Start a new upload session (owner-only). Upload files to the bot, then /d to finish or /e to cancel.\n"
        "/d - Finalize upload session. Bot will ask protect (yes/no) and auto-delete hours, then save to vault and return a deep link.\n"
        "/e - Cancel active upload session.\n"
        "/broadcast - Reply to a message to broadcast it to all users (copy, preserves inline buttons).\n"
        "/stats - Show usage stats.\n"
        "/list_sessions - List saved sessions.\n"
        "/revoke <session_id> - Revoke a session link.\n"
        "/help - Show this help.\n\n"
        "<b>Public</b>\n"
        "/start - Show welcome or open a session with deep link.\n"
    )
    await message.reply(help_text)

@dp.message_handler(commands=["setimage"])
async def handle_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    if not message.reply_to_message:
        await message.reply("Reply to a text or photo message to set the welcome /start message.")
        return
    reply = message.reply_to_message
    content = reply.caption or reply.text or ""
    file_id = None
    if reply.photo:
        file_id = reply.photo[-1].file_id
    db.set_start_message(content, file_id)
    await message.reply("Start message updated successfully.")

# =============================================================
# UPLOAD FLOW (owner-only)
# /upload -> collect messages from owner into in-memory staging
# /d -> finalize: copy header, copy each file into vault (preserve original captions), save file info in DB, send final vault message with deep link
# /e -> cancel
# =============================================================

@dp.message_handler(commands=["upload"])
async def start_upload(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    owner_id = message.from_user.id
    sessions_in_memory[owner_id] = {
        "items": [],  # each item = {"chat_id": from_chat_id, "message_id": message_id}
        "created": int(time.time())
    }
    await UploadStates.waiting_files.set()
    await message.reply("Upload session started. Send files (photos, videos, documents, text). When done, send /d to finish or /e to cancel.")

@dp.message_handler(commands=["e"], state=UploadStates.waiting_files)
async def cancel_upload(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    owner_id = message.from_user.id
    sessions_in_memory.pop(owner_id, None)
    await state.finish()
    await message.reply("Upload session cancelled and cleared.")

@dp.message_handler(content_types=types.ContentType.ANY, state=UploadStates.waiting_files)
async def collect_file_messages(message: types.Message, state: FSMContext):
    """
    During the upload session, the owner sends files/messages to the bot.
    We record their chat_id and message_id to be copied into the vault on finalize.
    """
    if not is_owner(message.from_user.id):
        return
    owner_id = message.from_user.id
    if owner_id not in sessions_in_memory:
        await message.reply("No active upload session. Start with /upload.")
        return

    # Store the original chat id (should be owner's private chat with bot) and message_id
    sessions_in_memory[owner_id]["items"].append({
        "from_chat_id": message.chat.id,
        "message_id": message.message_id
    })
    await message.reply("Added to session. Send more files or /d to finalize, /e to cancel.")

@dp.message_handler(commands=["d"], state=UploadStates.waiting_files)
async def finalize_upload_step1(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    owner_id = message.from_user.id
    info = sessions_in_memory.get(owner_id)
    if not info or not info.get("items"):
        await message.reply("No files were added to the session. Use /upload and send files first.")
        return
    await UploadStates.choosing_protect.set()
    await message.reply("Enable protect content? Reply with 'yes' to enable, or 'no' to disable. (Protect prevents forwarding/downloading for non-owner users.)")

@dp.message_handler(state=UploadStates.choosing_protect)
async def finalize_upload_protect(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    choice = (message.text or "").strip().lower()
    protect = 1 if choice in ("yes", "y", "true", "1") else 0
    owner_id = message.from_user.id
    if owner_id not in sessions_in_memory:
        await message.reply("Session not found. Start with /upload.")
        await state.finish()
        return
    sessions_in_memory[owner_id]["protect"] = protect
    await UploadStates.choosing_timer.set()
    await message.reply("Set auto-delete timer in hours (0 for no auto-delete). Range 0 to 168. Example: 10 for 10 hours.")

@dp.message_handler(state=UploadStates.choosing_timer)
async def finalize_upload_timer(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    owner_id = message.from_user.id
    info = sessions_in_memory.get(owner_id)
    if not info:
        await message.reply("Session not found. Start with /upload.")
        await state.finish()
        return

    text = (message.text or "").strip()
    try:
        hours = float(text)
    except Exception:
        await message.reply("Invalid input. Enter number of hours between 0 and 168.")
        return

    if hours < 0 or hours > 168:
        await message.reply("Invalid range. Hours must be between 0 and 168.")
        return

    auto_delete_seconds = int(hours * 3600)

    # Now perform vault operations: create header, copy messages into vault, store file metadata in DB, final link message
    items = info.get("items", [])
    protect_flag = int(info.get("protect", 0))

    # Prepare header in vault channel
    # We'll create header first, then copy items preserving original captions, then create final link message
    try:
        header_text = f"\ud83d\udce6 Session storing files"  # temporary; we will include session id later after creation
        header_msg = await bot.send_message(VAULT_CHANNEL_ID, "Preparing session storage...")
    except Exception as exc:
        logger.exception("Failed to send header to vault: %s", exc)
        await message.reply("Failed to write to vault channel. Make sure the bot is admin in the channel and VAULT_CHANNEL_ID is correct.")
        await state.finish()
        return

    # Copy owner messages into vault, capture returned Message objects for metadata
    copied_messages_info = []
    position_counter = 0
    for it in items:
        from_chat = it["from_chat_id"]
        mid = it["message_id"]
        try:
            # Use copy_message to preserve original media and caption (copy_message returns Message object)
            copied = await bot.copy_message(chat_id=VAULT_CHANNEL_ID, from_chat_id=from_chat, message_id=mid)
            # Extract file info from copied message:
            ftype, fid, caption = extract_media_info_from_message(copied)
            # store vault message id, file_id, type, caption and position (order)
            copied_messages_info.append({
                "vault_msg_id": copied.message_id,
                "file_id": fid,
                "file_type": ftype,
                "caption": caption or "",
                "position": position_counter
            })
            position_counter += 1
            # Slight pause to avoid hitting request limits if many files
            await asyncio.sleep(0.12)
        except Exception as exc:
            logger.exception("Failed to copy message %s from chat %s into vault: %s", mid, from_chat, exc)
            # continue copying others

    # Create session row in DB with header_vault_msg_id initially (we will update header text after session id known)
    # For now, we will create session with temporary header id, then update header and send final link message.
    created_header_vault_msg_id = header_msg.message_id

    # Create a placeholder link message in vault (will be updated afterwards)
    link_placeholder = await bot.send_message(VAULT_CHANNEL_ID, "Preparing session link...")

    # Persist session in DB
    try:
        session_id = db.create_session(owner_id, protect_flag, auto_delete_seconds, created_header_vault_msg_id, link_placeholder.message_id)
    except Exception as exc:
        logger.exception("Failed to create session in DB: %s", exc)
        await message.reply("Failed to create session in DB.")
        await state.finish()
        return

    # Now update header to include session number: edit header message in vault
    try:
        header_text = f"\ud83d\udce6 Session {session_id}"
        await bot.edit_message_text(header_text, chat_id=VAULT_CHANNEL_ID, message_id=created_header_vault_msg_id)
    except Exception as exc:
        logger.exception("Failed to edit header message in vault: %s", exc)
        # Not fatal — continue

    # Save each copied file's metadata into DB, set position ordering
    for info_item in copied_messages_info:
        try:
            db.add_file(session_id=session_id,
                        vault_msg_id=info_item["vault_msg_id"],
                        file_id=info_item["file_id"],
                        file_type=info_item["file_type"],
                        caption=info_item["caption"],
                        position=info_item["position"])
        except Exception as exc:
            logger.exception("Failed to save file metadata to DB: %s", exc)

    # Build deep link for the session
    try:
        session_link = await get_start_link(str(session_id), encode=True)
    except Exception as exc:
        logger.exception("Failed to generate start link via aiogram: %s", exc)
        # fallback to tg://deep_link
        session_link = f"https://t.me/{(await bot.get_me()).username}?start={session_id}"

    # Edit the vault link placeholder message to include the session link
    try:
        link_text = f"🔗 Files saved in Session {session_id}: {session_link}"
        await bot.edit_message_text(link_text, chat_id=VAULT_CHANNEL_ID, message_id=link_placeholder.message_id)
        # Update session row with link_vault_msg_id and header_vault_msg_id (already set)
    except Exception as exc:
        logger.exception("Failed to update link message in vault: %s", exc)

    # Finally, send a simple confirmation back to the owner with the deep link
    try:
        await message.reply(f"Session {session_id} saved to vault. Link:\n{session_link}")
    except Exception:
        pass

    # Clear in-memory session
    sessions_in_memory.pop(owner_id, None)
    await state.finish()

# =============================================================
# BROADCAST (owner-only) - reply to a message to broadcast with copy_to (preserves inline buttons)
# =============================================================
@dp.message_handler(commands=["broadcast"])
async def handle_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    if not message.reply_to_message:
        await message.reply("Reply to the message you want to broadcast, then run /broadcast.")
        return
    users = db.get_all_user_ids()
    if not users:
        await message.reply("No users to broadcast to.")
        return
    target = message.reply_to_message
    await message.reply(f"Starting broadcast to {len(users)} users.")
    sent = 0
    failed = 0

    async def send_to(uid: int):
        nonlocal sent, failed
        async with broadcast_semaphore:
            try:
                await target.copy_to(uid)
                sent += 1
            except (BotBlocked, ChatNotFound):
                failed += 1
            except RetryAfter as e:
                logger.warning("Broadcast RetryAfter %s for uid %s", e.timeout, uid)
                await asyncio.sleep(e.timeout + 1)
                try:
                    await target.copy_to(uid)
                    sent += 1
                except Exception:
                    failed += 1
            except Exception:
                failed += 1

    tasks = [asyncio.create_task(send_to(uid)) for uid in users]
    await asyncio.gather(*tasks)
    await message.reply(f"Broadcast completed. Sent: {sent}, Failed: {failed}")

# =============================================================
# STATS / SESSION MANAGEMENT (owner-only)
# =============================================================
@dp.message_handler(commands=["stats"])
async def handle_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    total_users = db.count_users()
    active_2d = db.count_active_2days()
    total_files = db.count_files()
    total_sessions = db.count_sessions()
    text = (
        f"Stats:\n"
        f"Users active in last 2 days: {active_2d}\n"
        f"Total users: {total_users}\n"
        f"Total files stored: {total_files}\n"
        f"Total sessions: {total_sessions}\n"
    )
    await message.reply(text)

@dp.message_handler(commands=["list_sessions"])
async def handle_list_sessions(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    rows = db.list_sessions()
    if not rows:
        await message.reply("No sessions found.")
        return
    out_lines = []
    for r in rows:
        created = datetime.utcfromtimestamp(r["created_at"]).isoformat() + "Z"
        out_lines.append(f"ID:{r['id']} owner:{r['owner_id']} created:{created} protect:{r['protect']} auto_delete:{r['auto_delete']} revoked:{r['revoked']}")
    # Split into several messages if too long
    chunk = ""
    for line in out_lines:
        if len(chunk) + len(line) + 1 > 3500:
            await message.reply(chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        await message.reply(chunk)

@dp.message_handler(commands=["revoke"])
async def handle_revoke(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /revoke <session_id>")
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid session id.")
        return
    if not db.get_session(sid):
        await message.reply("Session not found.")
        return
    db.revoke_session(sid)
    await message.reply(f"Session {sid} revoked successfully.")

# =============================================================
# Error handling and graceful shutdown
# =============================================================
@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Unhandled exception: %s", exception)
    return True  # prevents default handler from running

async def shutdown(dispatcher: Dispatcher):
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
    logger.info("Shutdown completed.")

# =============================================================
# MAIN
# =============================================================
async def main():
    logger.info("Bot starting up. Owner ID: %s, Vault Channel: %s, DB: %s", OWNER_ID, VAULT_CHANNEL_ID, DB_PATH)
    try:
        await dp.start_polling()
    finally:
        await shutdown(dp)

if __name__ == "__main__":
    # Start health server thread for uptime pings
    health_thread.start()
    # Run bot
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Exiting...")

# =============================================================
# Requirements (save into requirements.txt)
# =============================================================
# aiogram==2.25.1
# APScheduler==3.10.4
# aiohttp==3.9.4

# =============================================================
# Dockerfile (example)
# =============================================================
# FROM python:3.11-slim
# WORKDIR /app
# COPY bot.py /app/bot.py
# COPY requirements.txt /app/requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt
# ENV PORT=10000
# CMD ["python", "bot.py"]
#
# When deploying on Render:
# - Add environment variables:
#     BOT_TOKEN, OWNER_ID, VAULT_CHANNEL_ID, DB_PATH=/data/database.sqlite3, PORT=10000
# - Add a persistent disk mounted to /data
# - Configure health check to /health (Render will use the thread that runs aiohttp)
#
# =============================================================
# Notes & caveats
# =============================================================
# - The bot copies owner-uploaded messages into the vault channel. The copy returns a message object
#   which provides stable file_ids for the media stored in the vault. The DB stores both vault_msg_id
#   and the file_id and caption so delivering later is done by sending using those file_ids.
# - copy_message preserves the original caption. The bot stores that caption for later delivery.
# - Auto-delete timers are scheduled after delivering files to a user. The scheduler is in-memory; if the
#   process restarts between delivery and scheduled deletion, the scheduled deletion will not run.
#   However, the files themselves remain in the vault channel and DB for permanent access.
# - If you require guaranteed scheduled deletion even across restarts, we can add a persistent scheduler
#   jobstore (APScheduler supports DB-backed jobstores) and load pending deletions on startup.
# - The vault channel must be accessible to the bot (add the bot as admin). Use a private channel for safety.
# - Use a persistent disk (like Render's /data) for DB_PATH to ensure data survives restarts/deploys.
