#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kino_bot.py — final, to'liq, 3-part
Features:
- TTL subscription validation
- Multi-line movie meta (first non-empty line = title)
- Admin Add/Remove Group, Add/Remove JoinRequest, Add/Remove Movie
- Join-request monitoring with invite URL normalized, inline "Qo'shilish" buttons
- downloads counter per movie
- DB migrations for older schemas
"""

import asyncio
import os
import re
import datetime
import random
import html
import logging
import urllib.parse
from typing import Optional, Dict, Any, List, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.enums import ParseMode, ChatType
from aiogram.types import (
    Message, CallbackQuery, ChatJoinRequest,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.client.default import DefaultBotProperties

# ---------------- CONFIG & LOGGING ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN",  "7395067120:AAETmYWyvKui08NUwM9i01wGn29Xdjt40cs")
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN kerak. export BOT_TOKEN")
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "7794986117"))
except Exception:
    ADMIN_ID = 0

DB_FILE = os.getenv("DB_FILE", "kinobot.db")
VALIDATION_TTL = int(os.getenv("VALIDATION_TTL", "3600"))  # seconds; default 1 hour

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# in-memory admin flow state
admin_states: Dict[int, Dict[str, Any]] = {}

# ---------------- Helpers: normalize invite/url ----------------
def make_tg_url(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    v = val.strip()
    # if starts with t.me/ or telegram.me/
    if v.startswith("t.me/") or v.startswith("telegram.me/"):
        if not v.startswith("http"):
            return "https://" + v
        return v
    # if full url
    if v.startswith("http://") or v.startswith("https://"):
        return v
    # @username or plain username
    if v.startswith("@") or re.fullmatch(r"[A-Za-z0-9_]{3,}", v):
        uname = v.lstrip("@")
        return "https://t.me/" + uname
    # joinchat token or plus-code (may start with +)
    if v.startswith("+") or "joinchat" in v:
        if not v.startswith("http"):
            return "https://t.me/" + v
        return v
    # fallback
    return None

def normalize_invite_for_compare(invite: Optional[str]) -> Optional[str]:
    """Return invite token suitable for substring compare: strip protocol and trailing slashes."""
    if not invite:
        return None
    u = invite.strip()
    # remove https://, http://
    u = re.sub(r"^https?://(www\.)?", "", u, flags=re.I)
    # remove trailing slash
    u = u.rstrip("/")
    return u.lower()

# ---------------- DB INIT & MIGRATION ----------------
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        # create base tables
        await db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                chat_id TEXT PRIMARY KEY,
                username TEXT,
                title TEXT,
                invite TEXT
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS join_monitored (
                chat_id TEXT PRIMARY KEY,
                invite TEXT
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pending_join_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                user_id INTEGER,
                username TEXT,
                full_name TEXT,
                requested_at TEXT
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                code TEXT PRIMARY KEY,
                title TEXT,
                file_id TEXT,
                file_type TEXT,
                year TEXT,
                genre TEXT,
                language TEXT,
                description TEXT,
                downloads INTEGER DEFAULT 0
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                subscribed INTEGER DEFAULT 0,
                last_validated_at TEXT
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        await db.commit()

        # do migrations if older schemas (best-effort)
        try:
            cur = await db.execute("PRAGMA table_info(movies)")
            cols = await cur.fetchall()
            col_names = [c[1] for c in cols]
            if "downloads" not in col_names:
                await db.execute("ALTER TABLE movies ADD COLUMN downloads INTEGER DEFAULT 0")
                await db.commit()
                logger.info("Migration: added movies.downloads column")
        except Exception:
            logger.exception("Migration: movies.downloads failed (continuing)")

        try:
            cur = await db.execute("PRAGMA table_info(users)")
            cols = await cur.fetchall()
            col_names = [c[1] for c in cols]
            if "last_validated_at" not in col_names:
                await db.execute("ALTER TABLE users ADD COLUMN last_validated_at TEXT")
                await db.commit()
                logger.info("Migration: added users.last_validated_at column")
        except Exception:
            logger.exception("Migration: users.last_validated_at failed (continuing)")
# ---------------- DB HELPERS ----------------
# Users
async def add_user_db(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, subscribed, last_validated_at) VALUES (?, 0, NULL)",
            (int(user_id),)
        )
        await db.commit()

async def set_user_subscribed_db(user_id: int, val: int, validated_at: Optional[datetime.datetime] = None):
    ts = validated_at.isoformat() if validated_at else None
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR REPLACE INTO users(user_id, subscribed, last_validated_at) VALUES (?, ?, ?)",
            (int(user_id), int(val), ts)
        )
        await db.commit()

async def update_user_last_validated(user_id: int, validated_at: datetime.datetime):
    ts = validated_at.isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET last_validated_at = ?, subscribed = 1 WHERE user_id = ?", (ts, int(user_id)))
        await db.commit()

async def invalidate_user_subscription(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET subscribed = 0 WHERE user_id = ?", (int(user_id),))
        await db.commit()

async def get_user_record_db(user_id: int) -> Tuple[int, Optional[datetime.datetime]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT subscribed, last_validated_at FROM users WHERE user_id = ?", (int(user_id),))
        r = await cur.fetchone()
    if not r:
        return 0, None
    subscribed = int(r[0]) if r[0] is not None else 0
    last_validated_at = None
    if r[1]:
        try:
            last_validated_at = datetime.datetime.fromisoformat(r[1])
        except Exception:
            last_validated_at = None
    return subscribed, last_validated_at

# Settings
async def settings_get(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        r = await cur.fetchone()
        return r[0] if r else None

async def settings_set(key: str, value: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)", (key, value))
        await db.commit()

# Groups / join monitored / pending
async def add_group_db(chat_id: str, username: Optional[str], title: Optional[str], invite: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO groups(chat_id, username, title, invite) VALUES (?, ?, ?, ?)",
                         (str(chat_id), username, title, invite))
        await db.commit()

async def remove_group_db(chat_id: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM groups WHERE chat_id = ?", (str(chat_id),))
        await db.commit()

async def list_groups_db() -> List[Tuple[str, Optional[str], Optional[str], Optional[str]]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT chat_id, username, title, invite FROM groups ORDER BY chat_id")
        rows = await cur.fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]

async def add_join_monitored_db(chat_id: str, invite: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO join_monitored(chat_id, invite) VALUES (?, ?)", (str(chat_id), invite))
        await db.commit()

async def remove_join_monitored_db(chat_id: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM join_monitored WHERE chat_id = ?", (str(chat_id),))
        await db.commit()

async def list_join_monitored_db() -> List[Tuple[str, Optional[str]]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT chat_id, invite FROM join_monitored ORDER BY chat_id")
        rows = await cur.fetchall()
        return [(r[0], r[1]) for r in rows]

async def is_join_monitored_db(chat_id: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT 1 FROM join_monitored WHERE chat_id = ? LIMIT 1", (str(chat_id),))
        r = await cur.fetchone()
        return bool(r)

async def add_pending_join_request_db(chat_id: str, user_id: int, username: Optional[str], full_name: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT INTO pending_join_requests(chat_id, user_id, username, full_name, requested_at) VALUES (?, ?, ?, ?, ?)",
                         (str(chat_id), int(user_id), username, full_name, datetime.datetime.utcnow().isoformat()))
        await db.commit()

async def list_pending_for_user_db(user_id: int) -> List[Tuple[int, str, int, Optional[str], Optional[str]]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT id, chat_id, user_id, username, full_name FROM pending_join_requests WHERE user_id = ?", (int(user_id),))
        rows = await cur.fetchall()
        return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

# Movies
async def add_movie_db(code: str, title: str, file_id: str, file_type: str,
                       year: Optional[str]=None, genre: Optional[str]=None,
                       language: Optional[str]=None, description: Optional[str]=None):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT downloads FROM movies WHERE code = ?", (code,))
        r = await cur.fetchone()
        downloads = int(r[0]) if r and r[0] is not None else 0
        await db.execute("""
            INSERT OR REPLACE INTO movies(code, title, file_id, file_type, year, genre, language, description, downloads)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code, title, file_id, file_type, year, genre, language, description, downloads))
        await db.commit()

async def remove_movie_db(code: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("DELETE FROM movies WHERE code = ?", (code,))
        await db.commit()
        return cur.rowcount > 0

async def get_movie_db(code: str) -> Optional[Tuple[str, str, str, Optional[str], Optional[str], Optional[str], Optional[str], int]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT title, file_id, file_type, year, genre, language, description, COALESCE(downloads,0) FROM movies WHERE code = ?", (code,))
        r = await cur.fetchone()
    if not r:
        return None
    return (r[0], r[1], r[2], r[3], r[4], r[5], r[6], int(r[7]))

async def increment_movie_downloads(code: str) -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE movies SET downloads = COALESCE(downloads,0) + 1 WHERE code = ?", (code,))
        await db.commit()
        cur = await db.execute("SELECT downloads FROM movies WHERE code = ?", (code,))
        r = await cur.fetchone()
    return int(r[0]) if r else 0

# ---------------- UI helpers (button labels fixed to "Qo'shilish") ----------------
async def resolve_display_for_inline(chat_id_or_invite: str, invite: Optional[str]) -> Tuple[str, Optional[str]]:
    # tries to return friendly label and url if possible (not used for button label)
    try:
        ch = await bot.get_chat(chat_id_or_invite)
        username = getattr(ch, "username", None)
        title = getattr(ch, "title", None)
        if username:
            url = make_tg_url(username)
            return ("@" + username.lstrip("@"), url)
        if title:
            return (title, None)
    except Exception:
        pass
    if invite:
        url = make_tg_url(invite)
        if url:
            return ("Join via invite", url)
        return (invite, None)
    return (str(chat_id_or_invite), None)

async def groups_inline_kb(missing: List[Tuple[str, Optional[str]]]) -> InlineKeyboardMarkup:
    """Create inline keyboard for missing join requirements.
    Buttons will be labeled exactly 'Qo'shilish'. If an invite URL exists, it will be used as the button URL.
    Otherwise a dummy callback button will be shown to alert the user.
    """
    rows = []
    for cid, invite in missing:
        invite_url = make_tg_url(invite)
        if invite_url:
            rows.append([InlineKeyboardButton(text="Qo'shilish", url=invite_url)])
        else:
            rows.append([InlineKeyboardButton(text="Qo'shilish", callback_data=f"dummy:{cid}")])
    rows.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def movie_inline_kb(code: str, title: str) -> InlineKeyboardMarkup:
    codes_link_raw = await settings_get("codes_link") or ""
    codes_link_url = make_tg_url(codes_link_raw)
    try:
        bot_username = (await bot.get_me()).username or ""
    except Exception:
        bot_username = ""
    share_text = f"Kodni yuboring: {code} - {title}\nKodni olish: {codes_link_raw}\nBot: @{bot_username}"
    share_url = "https://t.me/share/url?url=&text=" + urllib.parse.quote_plus(share_text)
    rows = []
    if codes_link_url:
        rows.append([InlineKeyboardButton(text="ssilka", url=codes_link_url)])
    rows.append([InlineKeyboardButton(text="🔁 Ulashish", url=share_url),
                 InlineKeyboardButton(text="❌ Yashirish", callback_data=f"movie:hide:{code}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# safe send
async def safe_send(user_id: int, text: str, reply_markup=None) -> bool:
    try:
        await bot.send_message(user_id, text, reply_markup=reply_markup)
        return True
    except Exception as e:
        logger.warning("safe_send failed to %s: %s", user_id, e)
        return False

# ---------------- CORE: check_user_all ----------------
async def check_user_all(user_id: int) -> Tuple[bool, List[Tuple[str, Optional[str]]]]:
    missing: List[Tuple[str, Optional[str]]] = []
    monitored = await list_join_monitored_db()
    pendings = await list_pending_for_user_db(user_id)
    # monitored join requests
    for chat_id, invite in monitored:
        target = chat_id
        if isinstance(chat_id, str) and (chat_id.startswith("http") or chat_id.startswith("+") or "joinchat" in chat_id):
            try:
                resolved = await bot.get_chat(chat_id)
                if resolved:
                    target = str(resolved.id)
            except Exception:
                target = chat_id
        found_pending = False
        for p in pendings:
            _, p_chat_id, _, _, _ = p
            if str(p_chat_id) == str(chat_id) or str(p_chat_id) == str(target):
                found_pending = True
                break
        if found_pending:
            continue
        try:
            member = await bot.get_chat_member(target, user_id)
            status = getattr(member, "status", None)
            if status in ("member", "administrator", "creator"):
                continue
            else:
                missing.append((chat_id, invite))
        except Exception:
            missing.append((chat_id, invite))
    # regular groups
    groups = await list_groups_db()
    for chat_id, username, title, invite in groups:
        target = chat_id
        if isinstance(chat_id, str) and (chat_id.startswith("http") or chat_id.startswith("+") or "joinchat" in chat_id):
            try:
                resolved = await bot.get_chat(chat_id)
                if resolved:
                    target = str(resolved.id)
            except Exception:
                target = chat_id
        try:
            member = await bot.get_chat_member(target, user_id)
            status = getattr(member, "status", None)
            if status in ("member", "administrator", "creator"):
                continue
            else:
                missing.append((chat_id, invite))
        except Exception:
            missing.append((chat_id, invite))
    return (len(missing) == 0), missing

# ---------------- Keyboards ----------------
def admin_main_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Add Group"), KeyboardButton(text="Remove Group")],
            [KeyboardButton(text="Add JoinRequest"), KeyboardButton(text="Remove JoinRequest")],
            [KeyboardButton(text="List Groups"), KeyboardButton(text="List Monitored")],
            [KeyboardButton(text="Add Movie"), KeyboardButton(text="Remove Movie")],
            [KeyboardButton(text="Set Share Link"), KeyboardButton(text="Remove Share Link")],
            [KeyboardButton(text="Users")]
        ],
        resize_keyboard=True
    )
    return kb

def admin_flow_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Cancel")]], resize_keyboard=True)

# ---------------- Handlers (part start) ----------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await add_user_db(message.from_user.id)
    if message.from_user.id == ADMIN_ID:
        if message.chat.type != ChatType.PRIVATE:
            await safe_send(ADMIN_ID, "Admin panelni private ga yubordim.", reply_markup=admin_main_kb())
            try:
                await message.reply("Admin panelni private ga yubordim.")
            except Exception:
                pass
            return
        await safe_send(ADMIN_ID, "Admin panel.", reply_markup=admin_main_kb())
        return
@dp.callback_query(lambda c: c.data == "check_sub")
async def cb_check_sub(cq: CallbackQuery):
    user_id = cq.from_user.id
    ok, missing = await check_user_all(user_id)
    if ok:
        await update_user_last_validated(user_id, datetime.datetime.utcnow())
        try:
            await cq.message.edit_text("✅ Tekshiruv muvaffaqiyatli.", reply_markup=None)
        except Exception:
            pass
        await safe_send(user_id, "✅ Tekshiruv muvaffaqiyatli. Kino kodini yuboring.")
        try:
            await cq.answer()
        except Exception:
            pass
        return
    kb = await groups_inline_kb(missing)
    try:
        await cq.message.edit_text("❌ Siz hali quyidagilarga a'zo emassiz yoki join-request yubormagansiz:", reply_markup=kb)
    except Exception:
        await safe_send(user_id, "❌ Siz hali quyidagilarga a'zo emassiz yoki join-request yubormagansiz:", reply_markup=kb)
    try:
        await cq.answer()
    except Exception:
        pass

@dp.callback_query(lambda c: c.data and c.data.startswith("movie:hide:"))
async def cb_movie_hide(cq: CallbackQuery):
    try:
        await bot.delete_message(cq.message.chat.id, cq.message.message_id)
    except Exception:
        pass
    try:
        await cq.answer()
    except Exception:
        pass

@dp.callback_query(lambda c: c.data and c.data.startswith("dummy:"))
async def cb_dummy(cq: CallbackQuery):
    try:
        await cq.answer("Iltimos havola orqali yoki guruh admini bilan bog'laning.", show_alert=True)
    except Exception:
        pass

# Admin handlers
@dp.message(lambda m: m.from_user is not None and m.from_user.id == ADMIN_ID)
async def admin_text_handler(message: Message):
    text = (message.text or "").strip()
    st = admin_states.get(ADMIN_ID)

    if text == "Cancel":
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, "Operatsiya bekor qilindi.", reply_markup=admin_main_kb())
        return

    if st:
        action = st.get("action")
        step = st.get("step")

        # Add Group flow
        if action == "add_group" and step == "wait_link":
            ident = text
            parsed = None
            parsed = {"type": "invite", "value": ident} if ident else None
            if not parsed:
                admin_states.pop(ADMIN_ID, None)
                await safe_send(ADMIN_ID, "SSilka/identifierni tushunmadim.", reply_markup=admin_main_kb())
                return
            invite_raw = parsed["value"]
            invite_norm = make_tg_url(invite_raw) or invite_raw
            admin_states[ADMIN_ID] = {"action": "add_group", "step": "wait_chatid", "invite": invite_norm}
            await safe_send(ADMIN_ID, f"Invite qabul qilindi: {invite_norm}\nEndi chat_id yuboring (masalan -1001234567890) yoki Cancel.", reply_markup=admin_flow_kb())
            return

        if action == "add_group" and step == "wait_chatid":
            chat_id_text = text
            st = admin_states.pop(ADMIN_ID, None)
            invite = st.get("invite") if st else None
            try:
                chat_id_to_save = str(int(chat_id_text))
            except Exception:
                parsed = None
                m = re.fullmatch(r"-?\d{5,}", chat_id_text)
                if m:
                    chat_id_to_save = chat_id_text
                else:
                    await safe_send(ADMIN_ID, "Chat id noto'g'ri. Iltimos -100... formatida yuboring.", reply_markup=admin_main_kb())
                    return
            try:
                try:
                    ch = await bot.get_chat(chat_id_to_save)
                    await add_group_db(str(ch.id), getattr(ch, "username", None), getattr(ch, "title", None), invite)
                    await safe_send(ADMIN_ID, f"Guruh qo'shildi: {ch.id}", reply_markup=admin_main_kb())
                except Exception:
                    await add_group_db(chat_id_to_save, None, None, invite)
                    await safe_send(ADMIN_ID, f"Guruh qo'shildi (chat_id saqlandi): {chat_id_to_save}", reply_markup=admin_main_kb())
            except Exception as e:
                logger.exception("add_group error")
                await safe_send(ADMIN_ID, f"DB xatolik: {e}", reply_markup=admin_main_kb())
            return

        # Remove Group flow
        if action == "remove_group" and step == "wait_link":
            ident = text
            admin_states.pop(ADMIN_ID, None)
            parsed = None
            m = re.search(r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(.+)", ident or "")
            if m:
                # try resolve
                try:
                    ch = await bot.get_chat(ident)
                    await remove_group_db(str(ch.id))
                    await safe_send(ADMIN_ID, f"Guruh {ch.id} dan olib tashlandi.", reply_markup=admin_main_kb())
                except Exception:
                    await safe_send(ADMIN_ID, "Username resolve bo'lmadi; iltimos chat_id yuboring.", reply_markup=admin_main_kb())
            else:
                # maybe id
                try:
                    await remove_group_db(ident)
                    await safe_send(ADMIN_ID, f"Guruh {ident} dan olib tashlandi.", reply_markup=admin_main_kb())
                except Exception:
                    await safe_send(ADMIN_ID, "O'chirishda muammo bo'ldi.", reply_markup=admin_main_kb())
            return

        # Add JoinRequest: first wait_link, then wait_chatid
        if action == "add_join" and step == "wait_link":
            ident = text
            if not ident:
                admin_states.pop(ADMIN_ID, None)
                await safe_send(ADMIN_ID, "Iltimos invite/link yuboring.", reply_markup=admin_main_kb())
                return
            invite_norm = make_tg_url(ident) or ident
            # validate that invite looks like t.me/+ or joinchat
            if not invite_norm or (("t.me/+" not in invite_norm.lower()) and ("joinchat" not in invite_norm.lower() and not re.search(r"/[A-Za-z0-9_]{3,}", invite_norm))):
                # still allow public username invites but warn
                await safe_send(ADMIN_ID, f"Iltimos private invite (t.me/+) ni yuboring. Siz yuborgan: {invite_norm}", reply_markup=admin_main_kb())
                admin_states.pop(ADMIN_ID, None)
                return
            admin_states[ADMIN_ID] = {"action": "add_join", "step": "wait_chatid", "invite": invite_norm}
            await safe_send(ADMIN_ID, f"JoinRequest invite qabul qilindi: {invite_norm}\nEndi chat_id yuboring (masalan -1001234567890) yoki Cancel.", reply_markup=admin_flow_kb())
            return

        if action == "add_join" and step == "wait_chatid":
            chat_id_text = text
            st = admin_states.pop(ADMIN_ID, None)
            invite = st.get("invite") if st else None
            # try parse id
            try:
                chat_id_to_save = str(int(chat_id_text))
            except Exception:
                m = re.fullmatch(r"-?\d{5,}", chat_id_text)
                if m:
                    chat_id_to_save = chat_id_text
                else:
                    await safe_send(ADMIN_ID, "Chat id noto'g'ri. Iltimos -100... formatida yuboring.", reply_markup=admin_main_kb())
                    return
            # store normalized invite and chat_id
            try:
                try:
                    ch = await bot.get_chat(chat_id_to_save)
                    chat_id_to_store = str(ch.id)
                except Exception:
                    chat_id_to_store = chat_id_to_save
                invite_to_store = invite
                await add_join_monitored_db(chat_id_to_store, invite_to_store)
                await safe_send(ADMIN_ID, f"JoinRequest monitoring qoʻshildi: chat_id={chat_id_to_store}, invite={invite_to_store or '-'}", reply_markup=admin_main_kb())
            except Exception as e:
                logger.exception("add_join error")
                await safe_send(ADMIN_ID, f"DB xatolik: {e}", reply_markup=admin_main_kb())
            return

        # Remove JoinRequest
        if action == "remove_join" and step == "wait_link":
            ident = text
            admin_states.pop(ADMIN_ID, None)
            parsed = None
            m = re.search(r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(.+)", ident or "")
            if m:
                try:
                    ch = await bot.get_chat(ident)
                    await remove_join_monitored_db(str(ch.id))
                    await safe_send(ADMIN_ID, f"JoinRequest monitoring {ch.id} dan olib tashlandi.", reply_markup=admin_main_kb())
                except Exception:
                    if re.fullmatch(r"-?\d{5,}", ident):
                        await remove_join_monitored_db(ident)
                        await safe_send(ADMIN_ID, f"JoinRequest monitoring {ident} dan olib tashlandi.", reply_markup=admin_main_kb())
                    else:
                        await safe_send(ADMIN_ID, "Username resolve bo'lmadi; iltimos chat_id yuboring.", reply_markup=admin_main_kb())
            else:
                try:
                    await remove_join_monitored_db(ident)
                    await safe_send(ADMIN_ID, "JoinRequest monitoring olib tashlandi.", reply_markup=admin_main_kb())
                except Exception:
                    await safe_send(ADMIN_ID, "O'chirishda muammo.", reply_markup=admin_main_kb())
            return

        # Add Movie flow
        if action == "add_movie" and step == "wait_media":
            file_id = None; ftype = None
            if message.video:
                file_id = message.video.file_id; ftype = "video"
            elif message.document:
                file_id = message.document.file_id; ftype = "document"
            elif message.animation:
                file_id = message.animation.file_id; ftype = "animation"
            else:
                admin_states.pop(ADMIN_ID, None)
                await safe_send(ADMIN_ID, "Iltimos video yoki fayl yuboring.", reply_markup=admin_main_kb())
                return
            admin_states[ADMIN_ID] = {"action": "add_movie", "step": "wait_meta", "file_id": file_id, "file_type": ftype}
            await safe_send(ADMIN_ID, "Endi kinoning nomi va (ixtiyoriy) ma'lumot yuboring (sarlavha birinchi non-empty qator). Bir nechta qator bo'lishi mumkin.", reply_markup=admin_flow_kb())
            return

        if action == "add_movie" and step == "wait_meta":
            meta = (message.text or "").strip()
            st = admin_states.pop(ADMIN_ID, None)
            file_id = st.get("file_id"); ftype = st.get("file_type")
            title = None
            for ln in meta.splitlines():
                ln = ln.strip()
                if ln:
                    title = ln
                    break
            if not title:
                title = f"Kino {random.randint(1,999)}"
            description = meta if meta else None
            nxt = await settings_get("next_code")
            try:
                ni = int(nxt) if nxt else 1
            except Exception:
                ni = 1
            code = str(ni)
            await settings_set("next_code", str(ni + 1))
            await add_movie_db(code, title, file_id, ftype, None, None, None, description)
            await safe_send(ADMIN_ID, f"🎬 Kino saqlandi. Kod: {code}", reply_markup=admin_main_kb())
            return

        # Remove Movie
        if action == "remove_movie" and step == "wait_code":
            code = text
            admin_states.pop(ADMIN_ID, None)
            ok = await remove_movie_db(code)
            if ok:
                await safe_send(ADMIN_ID, f"🗑️ Kino {code} o'chirildi.", reply_markup=admin_main_kb())
            else:
                await safe_send(ADMIN_ID, "❌ Bunday kod topilmadi.", reply_markup=admin_main_kb())
            return

        # Set Share Link
        if action == "set_codes_link" and step == "wait_link":
            link = text.strip()
            admin_states.pop(ADMIN_ID, None)
            if link.startswith("t.me/") or link.startswith("telegram.me/"):
                link = "https://" + link
            if not (link.startswith("http://") or link.startswith("https://")):
                await safe_send(ADMIN_ID, "Noto'g'ri format. Iltimos https:// yoki t.me/ bilan boshlang.", reply_markup=admin_main_kb())
                return
            await settings_set("codes_link", link)
            await safe_send(ADMIN_ID, f"Codes link saqlandi: {link}", reply_markup=admin_main_kb())
            return

        # Remove Share Link
        if action == "remove_codes_link" and step == "confirm":
            admin_states.pop(ADMIN_ID, None)
            await settings_set("codes_link", "")
            await safe_send(ADMIN_ID, "Codes link o'chirildi.", reply_markup=admin_main_kb())
            return

    # no active state -> admin keyboard
    if text == "Add Group":
        admin_states[ADMIN_ID] = {"action": "add_group", "step": "wait_link"}
        await safe_send(ADMIN_ID, "Guruh yoki kanal ssilkasi, @username yoki invite yuboring (keyin chat_id so'raladi):", reply_markup=admin_flow_kb())
        return
    if text == "Remove Group":
        admin_states[ADMIN_ID] = {"action": "remove_group", "step": "wait_link"}
        await safe_send(ADMIN_ID, "O'chirish uchun ssilka yoki chat_id yuboring:", reply_markup=admin_flow_kb())
        return
    if text == "Add JoinRequest":
        admin_states[ADMIN_ID] = {"action": "add_join", "step": "wait_link"}
        await safe_send(ADMIN_ID, "JoinRequest monitoring uchun invite/link yuboring (https://t.me/+) — keyin chat_id so'raladi:", reply_markup=admin_flow_kb())
        return
    if text == "Remove JoinRequest":
        admin_states[ADMIN_ID] = {"action": "remove_join", "step": "wait_link"}
        await safe_send(ADMIN_ID, "JoinRequest monitoringni o'chirish uchun ssilka yoki chat_id yuboring:", reply_markup=admin_flow_kb())
        return
    if text == "List Groups":
        groups = await list_groups_db()
        lines = [f"- {c} ({u or t or 'no title'}) invite:{inv or '-'}" for c, u, t, inv in groups]
        await safe_send(ADMIN_ID, "Groups:\n" + ("\n".join(lines) if lines else "Hech narsa topilmadi."), reply_markup=admin_main_kb())
        return
    if text == "List Monitored":
        monitored = await list_join_monitored_db()
        lines = [f"- {c} invite:{inv or '-'}" for c, inv in monitored]
        await safe_send(ADMIN_ID, "JoinRequest monitored:\n" + ("\n".join(lines) if lines else "Hech narsa topilmadi."), reply_markup=admin_main_kb())
        return
    if text == "Add Movie":
        admin_states[ADMIN_ID] = {"action": "add_movie", "step": "wait_media"}
        await safe_send(ADMIN_ID, "Iltimos video yoki fayl yuboring:", reply_markup=admin_flow_kb())
        return
    if text == "Remove Movie":
        admin_states[ADMIN_ID] = {"action": "remove_movie", "step": "wait_code"}
        await safe_send(ADMIN_ID, "O'chirish uchun kino kodini yuboring:", reply_markup=admin_flow_kb())
        return
    if text == "Set Share Link":
        admin_states[ADMIN_ID] = {"action": "set_codes_link", "step": "wait_link"}
        await safe_send(ADMIN_ID, "Iltimos kodni olish uchun ssilkani yuboring (https://... yoki t.me/...):", reply_markup=admin_flow_kb())
        return
    if text == "Remove Share Link":
        admin_states[ADMIN_ID] = {"action": "remove_codes_link", "step": "confirm"}
        await safe_send(ADMIN_ID, "Codes linkni o'chirishni tasdiqlaysizmi? (Cancel bilan bekor qilishingiz mumkin)", reply_markup=admin_flow_kb())
        return
    if text == "Users":
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute("SELECT user_id FROM users ORDER BY user_id LIMIT 100")
            rows = await cur.fetchall()
            cur2 = await db.execute("SELECT COUNT(*) FROM users")
            total = (await cur2.fetchone())[0]
        users = [str(r[0]) for r in rows]
        await safe_send(ADMIN_ID, f"Foydalanuvchilar soni: {total}\nBirinchi {len(users)} ID:\n" + ("\n".join(users) if users else "Hech narsa topilmadi."), reply_markup=admin_main_kb())
        return

# ---------------- USER HANDLER (kod yuborilganda TTL tekshiruv) ----------------
@dp.message(lambda m: m.from_user is not None and m.from_user.id != ADMIN_ID)
async def user_handler(message: Message):
    await add_user_db(message.from_user.id)

    txt = (message.text or "").strip()
    if re.fullmatch(r"\d{1,4}", txt):
        code = txt
        subscribed, last_validated_at = await get_user_record_db(message.from_user.id)
        now = datetime.datetime.utcnow()

        # TTL check: agar hali amal qilsa, skip real API check
        if subscribed and last_validated_at:
            elapsed = (now - last_validated_at).total_seconds()
            if elapsed < VALIDATION_TTL:
                mv = await get_movie_db(code)
                if not mv:
                    await safe_send(message.from_user.id, "Bunday kod topilmadi.")
                    return
                title, file_id, file_type, year, genre, language, desc, downloads = mv
                caption_parts = [f"{html.escape(title or 'Film')}"]
                if desc:
                    caption_parts.append(html.escape(desc))
                # two blank lines then Kod
                initial_caption = "\n".join(caption_parts) + "\n\n\n" + f"Kod: {code}"
                kb = await movie_inline_kb(code, title or "Film")
                try:
                    if file_type == "video":
                        sent = await bot.send_video(message.from_user.id, file_id, caption=initial_caption, reply_markup=kb)
                    else:
                        sent = await bot.send_document(message.from_user.id, file_id, caption=initial_caption, reply_markup=kb)
                except Exception:
                    logger.exception("Failed to send media to user")
                    return
                # increment counter after successful send
                try:
                    new_count = await increment_movie_downloads(code)
                    new_caption = "\n".join(caption_parts) + "\n\n\n" + f"Kod: {code}\nYuklashlar: ({new_count})"
                    try:
                        await bot.edit_message_caption(chat_id=sent.chat.id, message_id=sent.message_id, caption=new_caption, reply_markup=kb)
                    except Exception:
                        pass
                except Exception:
                    logger.exception("Failed to increment downloads")
                return

        # TTL expired or not subscribed -> perform real check
        ok, missing = await check_user_all(message.from_user.id)
        if not ok:
            kb = await groups_inline_kb(missing)
            await safe_send(message.from_user.id, "Kodni yuborishdan oldin quyidagi talablarni bajaring (guruh/kanalga a'zo bo'ling yoki join-request yuboring), so‘ng ✅ Tekshirishni bosing:", reply_markup=kb)
            await invalidate_user_subscription(message.from_user.id)
            return

        # validated -> update last_validated_at
        await update_user_last_validated(message.from_user.id, now)

        # deliver movie and increment downloads
        mv = await get_movie_db(code)
        if not mv:
            await safe_send(message.from_user.id, "Bunday kod topilmadi.")
            return
        title, file_id, file_type, year, genre, language, desc, downloads = mv
        caption_parts = [f"{html.escape(title or 'Film')}"]
        if desc:
            caption_parts.append(html.escape(desc))
        initial_caption = "\n".join(caption_parts) + "\n\n\n" + f"Kod: {code}"
        kb = await movie_inline_kb(code, title or "Film")
        try:
            if file_type == "video":
                sent = await bot.send_video(message.from_user.id, file_id, caption=initial_caption, reply_markup=kb)
            else:
                sent = await bot.send_document(message.from_user.id, file_id, caption=initial_caption, reply_markup=kb)
        except Exception:
            logger.exception("Failed to send media to user")
            return
        try:
            new_count = await increment_movie_downloads(code)
            new_caption = "\n".join(caption_parts) + "\n\n\n" + f"Kod: {code}\nYuklashlar: ({new_count})"
            try:
                await bot.edit_message_caption(chat_id=sent.chat.id, message_id=sent.message_id, caption=new_caption, reply_markup=kb)
            except Exception:
                pass
        except Exception:
            logger.exception("Failed to increment downloads after send")
        return

    # other messages
    await safe_send(message.from_user.id, "Iltimos kino kodi yuboring. Kod yuborilganda obuna/join holatingiz avtomatik tekshiriladi.")

# ---------------- Chat join request handler ----------------
@dp.chat_join_request()
async def on_chat_join_request(chat_join_request: ChatJoinRequest):
    try:
        chat = chat_join_request.chat
        user = chat_join_request.from_user
        # Try to identify monitored record by chat.id first
        monitored = False
        stored_invite = None
        try:
            if await is_join_monitored_db(str(chat.id)):
                monitored = True
                async with aiosqlite.connect(DB_FILE) as db:
                    cur = await db.execute("SELECT invite FROM join_monitored WHERE chat_id = ?", (str(chat.id),))
                    r = await cur.fetchone()
                    stored_invite = r[0] if r else None
        except Exception:
            monitored = False

        # If not found by id, try matching invite link token
        if not monitored:
            inv_link = chat_join_request.invite_link or ""
            inv_link_norm = normalize_invite_for_compare(inv_link)
            async with aiosqlite.connect(DB_FILE) as db:
                cur = await db.execute("SELECT chat_id, invite FROM join_monitored")
                rows = await cur.fetchall()
            for cid, inv in rows:
                if not inv:
                    continue
                inv_norm = normalize_invite_for_compare(inv)
                try:
                    if inv_norm and inv_norm in (inv_link_norm or ""):
                        monitored = True
                        stored_invite = inv
                        break
                except Exception:
                    continue

        if not monitored:
            logger.info("Join-request ignored for unmonitored chat %s (invite_link=%s)", getattr(chat, "id", None), getattr(chat_join_request, "invite_link", None))
            return

        username = getattr(user, "username", None)
        full_name = getattr(user, "full_name", None)
        await add_pending_join_request_db(str(chat.id), int(user.id), username, full_name)

        admin_msg = (
            f"🔔 Join-request (monitored):\n"
            f"Chat: {chat.title or getattr(chat, 'username', None) or chat.id} (id: {chat.id})\n"
            f"User: {full_name} (id: {user.id})\n"
        )
        if username:
            admin_msg += f"Username: @{username}\nLink: https://t.me/{username}\n"
        if stored_invite:
            admin_msg += f"Invite used: {stored_invite}\n"
        admin_msg += "\nEslatma: BOT tasdiqlamaydi — adminlar kanal/guruhda qo'lda tasdiqlasin."
        await safe_send(ADMIN_ID, admin_msg)
        try:
            await safe_send(user.id, f"Siz {chat.title or 'kanal/guruh'} ga qo'shilish uchun ariza yubordingiz. Adminlar arizangizni ko'rib chiqadi.")
        except Exception:
            pass
    except Exception:
        logger.exception("join_request handler error")

# ---------------- Admin pending commands ----------------
@dp.message(Command("pending"))
async def cmd_pending(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT id, chat_id, user_id, username, full_name, requested_at FROM pending_join_requests ORDER BY requested_at DESC")
        rows = await cur.fetchall()
    lines = []
    for r in rows:
        lines.append(f"- id:{r[0]} chat:{r[1]} user:{r[2]} uname:{r[3] or '-'} name:{r[4] or '-'} at:{r[5]}")
    await safe_send(ADMIN_ID, "Pending join requests:\n" + ("\n".join(lines) if lines else "Hech narsa topilmadi."))

@dp.message(Command("remove_pending"))
async def cmd_remove_pending(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await safe_send(ADMIN_ID, "Iltimos: /remove_pending <id>"); return
    try:
        pid = int(parts[1])
    except Exception:
        await safe_send(ADMIN_ID, "ID raqam bo'lishi kerak."); return
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM pending_join_requests WHERE id = ?", (pid,))
        await db.commit()
    await safe_send(ADMIN_ID, f"Pending id {pid} o'chirildi (agar mavjud bo'lsa).")

# ---------------- START UP ----------------
async def main():
    await init_db()
    try:
        logger.info("Bot ishga tushmoqda (VALIDATION_TTL=%s seconds)...", VALIDATION_TTL)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
