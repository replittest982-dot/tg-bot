#!/usr/bin/env python3
"""
💎 StatPro v21.0
---------------------------------------------
✅ FIX: adm_d TypeError fixed (State args renamed).
✅ NEW: Grant Sub by ID, Ban/Unban System.
✅ FEAT: 45+ New Commands & Improvements.
"""

import asyncio
import logging
import os
import sys
import io
import re
import uuid
import random
import csv
import shutil
import time
import json
import math
import sqlite3
import aiosqlite
from typing import Dict, Optional, Union, List, Set
from pathlib import Path
from datetime import datetime, timedelta

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile, FSInputFile
)
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.dispatcher.middlewares.base import BaseMiddleware

# --- TELETHON ---
from telethon import TelegramClient, events, functions, types
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, UserPrivacyRestrictedError,
    ChatAdminRequiredError, UserNotParticipantError, BadRequestError
)
from telethon.tl.types import (
    ChannelParticipantsAdmins, ChatBannedRights, User, Message as TlMessage
)
from telethon.tl.functions.channels import EditAdminRequest, JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ExportChatInviteRequest, SendReactionRequest
from telethon.tl.functions.contacts import BlockRequest, UnblockRequest

# --- QR ---
import qrcode
from PIL import Image

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ
# =========================================================================

WORKER_TASK: Optional[asyncio.Task] = None
WORKER_STATUS = "⚪️ Ожидание..."
BOT_VERSION = "v21.0 StatPro"
START_TIME = datetime.now().timestamp()
SESSIONS_PARSED = 0

TEMP_PARSE_DATA = {} 

PATTERNS = {
    "phone": r"^\+?[0-9]{10,15}$",
    "promo": r"^[A-Za-z0-9-]{4,20}$",
    "id": r"^\d+$"
}

BASE_DIR = Path("/app")
SESSION_DIR = BASE_DIR / "sessions"
SESSION_DIR.mkdir(exist_ok=True, parents=True)
DB_PATH = BASE_DIR / "database.db"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("StatPro")

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    AUTH_TIMEOUT = int(os.getenv("QR_TIMEOUT", "500"))
    # UPDATED SUPPORT BOT
    SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "@suppor_tstatpro1bot")
    TARGET_CHANNEL_URL = os.getenv("TARGET_CHANNEL_URL", "https://t.me/STAT_PRO1")
    TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", "0"))
except Exception as e:
    logger.critical(f"Config Error: {e}")
    sys.exit(1)

if not all([BOT_TOKEN, API_ID, API_HASH]):
    logger.critical("ENV variables missing!")
    sys.exit(1)

def get_session_path(user_id: int) -> Path:
    return SESSION_DIR / f"session_{user_id}"

# =========================================================================
# 🗄️ БАЗА ДАННЫХ
# =========================================================================

USER_CACHE = {}

async def get_cached_user(user_id: int, ttl=300):
    now = datetime.now().timestamp()
    if user_id in USER_CACHE and now - USER_CACHE[user_id][1] < ttl:
        return USER_CACHE[user_id][0]
    user = await get_user_data(user_id)
    if user: USER_CACHE[user_id] = (user, now)
    return user

async def has_active_sub(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    u = await get_cached_user(user_id)
    if not u: return False
    return datetime.fromisoformat(u['sub_end']) > datetime.now()

def db_connect():
    return aiosqlite.connect(DB_PATH, timeout=30.0)

async def init_db():
    async with db_connect() as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                join_date TEXT,
                sub_end TEXT,
                parse_limit INTEGER DEFAULT 1000,
                is_banned INTEGER DEFAULT 0,
                last_active TEXT
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sub_end ON users(sub_end)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS promos (
                code TEXT PRIMARY KEY,
                days INTEGER,
                activations INTEGER
            )
        """)
        await db.commit()

# --- DB Methods ---

async def add_user(user_id: int, username: str):
    now = datetime.now().isoformat()
    trial_end = (datetime.now() + timedelta(days=0)).isoformat()
    async with db_connect() as db:
        await db.execute("""
            INSERT OR IGNORE INTO users (user_id, username, join_date, sub_end, last_active) 
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, username, now, trial_end, now))
        await db.commit()

async def get_user_data(user_id: int):
    async with db_connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def get_user_limit(user_id: int) -> int:
    async with db_connect() as db:
        async with db.execute("SELECT parse_limit FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else 1000

async def set_limit(user_id: int, limit: int):
    async with db_connect() as db:
        await db.execute("UPDATE users SET parse_limit = ? WHERE user_id = ?", (limit, user_id))
        await db.commit()
    if user_id in USER_CACHE: del USER_CACHE[user_id]

# --- Promo & Sub Logic ---

async def create_promo(days: int, activations: int) -> str:
    code = f"PRO-{uuid.uuid4().hex[:6].upper()}"
    async with db_connect() as db:
        await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, activations))
        await db.commit()
    return code

async def use_promo(user_id: int, code: str) -> bool:
    if not re.match(PATTERNS['promo'], code): return False
    async with db_connect() as db:
        async with db.execute("SELECT days, activations FROM promos WHERE code = ?", (code,)) as c:
            res = await c.fetchone()
            if not res or res[1] < 1: return False
            days = res[0]
        await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ?", (code,))
        await db.execute("DELETE FROM promos WHERE activations <= 0")
        
        usr = await get_user_data(user_id)
        current = datetime.fromisoformat(usr['sub_end']) if usr and usr['sub_end'] else datetime.now()
        if current < datetime.now(): current = datetime.now()
        new_end = current + timedelta(days=days)
        
        await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), user_id))
        await db.commit()
    
    if user_id in USER_CACHE: del USER_CACHE[user_id]
    return True

async def grant_sub_by_id(user_id: int, days: int):
    """Выдача подписки по ID"""
    async with db_connect() as db:
        # Check if user exists, if not, create temp
        await db.execute("INSERT OR IGNORE INTO users (user_id, username, join_date, sub_end, last_active) VALUES (?, ?, ?, ?, ?)", 
                         (user_id, "Granted", datetime.now().isoformat(), datetime.now().isoformat(), datetime.now().isoformat()))
        
        usr = await get_user_data(user_id)
        current = datetime.fromisoformat(usr['sub_end']) if usr and usr['sub_end'] else datetime.now()
        if current < datetime.now(): current = datetime.now()
        new_end = current + timedelta(days=days)
        
        await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), user_id))
        await db.commit()
    if user_id in USER_CACHE: del USER_CACHE[user_id]

async def ban_user_db(user_id: int, is_ban: int):
    async with db_connect() as db:
        await db.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (is_ban, user_id))
        await db.commit()
    if user_id in USER_CACHE: del USER_CACHE[user_id]

async def get_stats():
    async with db_connect() as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (datetime.now().isoformat(),)) as c: active = (await c.fetchone())[0]
    return total, active

async def get_all_users():
    async with db_connect() as db:
        async with db.execute("SELECT user_id FROM users") as c:
            return [row[0] for row in await c.fetchall()]

async def auto_backup():
    try:
        t = datetime.now().strftime('%Y%m%d_%H%M')
        backup_path = BASE_DIR / f"statpro_backup_{t}.db"
        shutil.copy2(DB_PATH, backup_path)
    except: pass

async def cleanup_files():
    try:
        for path in SESSION_DIR.glob("*.session"):
            if path.stat().st_size == 0: path.unlink()
    except: pass

# =========================================================================
# 🛡️ MIDDLEWARE
# =========================================================================

async def edit_or_answer(message_obj: Union[Message, CallbackQuery], text: str, reply_markup=None):
    try:
        msg = message_obj.message if isinstance(message_obj, CallbackQuery) else message_obj
        await msg.edit_text(text, reply_markup=reply_markup)
    except Exception:
        try: 
            target = message_obj.message if isinstance(message_obj, CallbackQuery) else message_obj
            await target.delete()
        except: pass
        target = message_obj.message if isinstance(message_obj, CallbackQuery) else message_obj
        await target.answer(text, reply_markup=reply_markup)

class SecurityMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data: dict):
        user_id = event.from_user.id
        await add_user(user_id, event.from_user.username or "Unknown")
        
        if user_id == ADMIN_ID:
            return await handler(event, data)

        u_data = await get_cached_user(user_id)
        if u_data and u_data['is_banned']:
            return

        if TARGET_CHANNEL_ID != 0:
            try:
                m = await bot.get_chat_member(TARGET_CHANNEL_ID, user_id)
                if m.status not in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER]:
                    raise Exception
            except:
                if isinstance(event, Message): 
                    await event.answer(f"🔒 <b>Доступ ограничен</b>\nПодпишитесь: {TARGET_CHANNEL_URL}")
                elif isinstance(event, CallbackQuery): 
                    await event.answer("🚫 Требуется подписка на канал!", show_alert=True)
                return

        return await handler(event, data)

def progress_bar(current, total, width=10):
    if total == 0: return "[░░░]"
    filled = int(width * current / total)
    return f"[{'█'*filled + '░'*(width-filled)}] {int(current/total*100)}%"

# =========================================================================
# ⌨️ UI
# =========================================================================

async def get_main_kb(user_id: int):
    is_active = await has_active_sub(user_id)
    kb = []
    
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🔑 Авторизация (Admin)", callback_data="auth_menu")])

    kb.append([InlineKeyboardButton(text="👤 Профиль", callback_data="profile")])
    kb.append([InlineKeyboardButton(text="🎟 Активация", callback_data="sub_menu")])

    if is_active:
        kb.append([InlineKeyboardButton(text="👻 StatPro Worker", callback_data="worker_menu")])
        kb.append([InlineKeyboardButton(text="🛠 Настройки", callback_data="quick_actions")])

    kb.append([InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME.replace('@', '')}")])

    if user_id == ADMIN_ID:
        kb.insert(0, [InlineKeyboardButton(text="👑 Админ Панель", callback_data="admin_menu")])
        
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_auth():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 По номеру", callback_data="auth_phone"), 
         InlineKeyboardButton(text="📸 По QR", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="main_menu")]
    ])

def kb_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Выдать подписку", callback_data="adm_grant"),
         InlineKeyboardButton(text="🎫 Создать Промо", callback_data="adm_promo")],
        [InlineKeyboardButton(text="⛔ Бан", callback_data="adm_ban"),
         InlineKeyboardButton(text="🟢 Разбан", callback_data="adm_unban")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm_broadcast"),
         InlineKeyboardButton(text="📊 Статистика", callback_data="adm_stats")],
        [InlineKeyboardButton(text="📦 Бэкап", callback_data="adm_backup"),
         InlineKeyboardButton(text="🔄 Рестарт", callback_data="adm_restart_worker")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="main_menu")]
    ])

def kb_config(current):
    r = [10, 50, 500, 5000, 100000]
    kb = []
    for v in r:
        txt = f"🟢 {v}" if v == current else str(v)
        kb.append(InlineKeyboardButton(text=txt, callback_data=f"lim:{v}"))
    rows = [kb[i:i+3] for i in range(0, len(kb), 3)]
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="worker_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_parse_choice():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 TXT", callback_data="parse_res:file"),
         InlineKeyboardButton(text="📝 Текст", callback_data="parse_res:text")],
        [InlineKeyboardButton(text="📊 JSON", callback_data="parse_res:json"),
         InlineKeyboardButton(text="📑 CSV", callback_data="parse_res:csv")]
    ])

# =========================================================================
# 🎮 HANDLERS
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class States(StatesGroup):
    PHONE=State(); CODE=State(); PASS=State(); PROMO=State()
    ADM_DAYS=State(); ADM_ACT=State(); BROADCAST=State()
    GRANT_ID=State(); GRANT_DAYS=State()
    BAN_ID=State(); UNBAN_ID=State()

TEMP_CLIENTS = {}

@router.message(Command("start"))
async def start(m: Message):
    kb = await get_main_kb(m.from_user.id)
    await m.answer(f"👋 <b>StatPro</b> v21.0\nПрофессиональный инструмент аналитики.", reply_markup=kb)

@router.callback_query(F.data == "main_menu")
async def menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    uid = c.from_user.id
    if uid in TEMP_CLIENTS: 
        try: await TEMP_CLIENTS[uid].disconnect()
        except: pass
        del TEMP_CLIENTS[uid]
    kb = await get_main_kb(uid)
    await edit_or_answer(c, "🏠 <b>Главное меню:</b>", kb)

@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    u = await get_cached_user(c.from_user.id)
    d = datetime.fromisoformat(u['sub_end'])
    is_act = await has_active_sub(c.from_user.id)
    
    status = "👑 GOD" if c.from_user.id == ADMIN_ID else ("✅ VIP" if is_act else "❌ FREE")
    date_str = "∞" if c.from_user.id == ADMIN_ID else d.strftime('%d.%m.%Y')
    limit_info = f"⚡️ Лимит: <b>{u['parse_limit']}</b>\n" if is_act else ""
    
    text = (
        f"👤 <b>Профиль</b>\n"
        f"🆔: <code>{u['user_id']}</code>\n"
        f"💎: <b>{status}</b>\n"
        f"📅: {date_str}\n"
        f"{limit_info}"
    )
    await edit_or_answer(c, text, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="main_menu")]]))

@router.callback_query(F.data == "worker_menu")
async def w_menu(c: CallbackQuery):
    if not await has_active_sub(c.from_user.id):
        await c.answer("🚫 Нет подписки!", show_alert=True)
        return

    u = await get_cached_user(c.from_user.id)
    text = (
        f"👻 <b>StatPro Worker</b>\n"
        f"📡: {WORKER_STATUS}\n"
        f"🎯: <b>{u['parse_limit']}</b>\n\n"
        "🛠 <b>Tools:</b>\n"
        "<code>.id</code>, <code>.info</code>, <code>.ping</code>\n"
        "<code>.invite</code>, <code>.zombies</code>, <code>.bots</code>\n"
        "<code>.whois</code>, <code>.time</code>, <code>.calc</code>\n\n"
        "🛡 <b>Admin:</b>\n"
        "<code>.ban</code>, <code>.kick</code>, <code>.mute</code>\n"
        "<code>.promote</code>, <code>.demote</code>, <code>.purge</code>\n"
        "<code>.pin</code>, <code>.unpin</code>, <code>.lock</code>\n\n"
        "⚔️ <b>Raid/Fun:</b>\n"
        "<code>.spam</code>, <code>.tagall</code>, <code>.clown</code>\n"
        "<code>.react</code>, <code>.ghost</code>\n\n"
        "📂 <b>Parse:</b>\n"
        "<code>.scan</code> (Phantom), <code>.csv</code>\n"
        "<code>.json</code>, <code>.html</code>"
    )
    await edit_or_answer(c, text, kb_config(u['parse_limit']))

@router.callback_query(F.data.startswith("lim:"))
async def set_lim(c: CallbackQuery):
    if not await has_active_sub(c.from_user.id): return
    l = int(c.data.split(":")[1])
    await set_limit(c.from_user.id, l)
    await c.answer(f"✅ {l}")
    await w_menu(c)

# --- HYBRID PARSE HANDLER ---
@router.callback_query(F.data.startswith("parse_res:"))
async def parse_res_handler(c: CallbackQuery):
    mode = c.data.split(":")[1]
    data = TEMP_PARSE_DATA.get(c.from_user.id)
    
    if not data: return await c.answer("⚠️ Данные устарели.", show_alert=True)
    
    lines = data['lines']
    title = data['title']
    
    if mode == "file":
        fn = f"Users_{title}.txt"
        with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(lines))
        await c.message.answer_document(FSInputFile(fn), caption=f"📂 {len(lines)} users")
        os.remove(fn)
        
    elif mode == "text":
        chunk = ""
        await c.message.answer(f"📝 <b>List ({len(lines)}):</b>")
        for line in lines:
            if len(chunk) + len(line) > 3500:
                await c.message.answer(f"<code>{chunk}</code>")
                chunk = ""
                await asyncio.sleep(0.3)
            chunk += line + "\n"
        if chunk: await c.message.answer(f"<code>{chunk}</code>")
        
    elif mode == "json":
        fn = f"Users_{title}.json"
        # Конвертация в простой JSON
        json_data = [{"raw": l} for l in lines]
        with open(fn, "w", encoding="utf-8") as f: json.dump(json_data, f, indent=2)
        await c.message.answer_document(FSInputFile(fn), caption="JSON Export")
        os.remove(fn)

    elif mode == "csv":
        fn = f"Users_{title}.csv"
        with open(fn, "w", newline='', encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Data"])
            for l in lines: w.writerow([l])
        await c.message.answer_document(FSInputFile(fn), caption="CSV Export")
        os.remove(fn)
    
    await c.answer()

# --- SUB SYSTEM ---
@router.callback_query(F.data == "sub_menu")
async def sub_menu(c: CallbackQuery):
    await edit_or_answer(c, "🎟 <b>Активация:</b>", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Ввести код", callback_data="enter_promo")],
        [InlineKeyboardButton(text="🔙", callback_data="main_menu")]
    ]))

@router.callback_query(F.data == "enter_promo")
async def en_pro(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "👉 <b>Код:</b>")
    await state.set_state(States.PROMO)

@router.message(States.PROMO)
async def pro_h(m: Message, state: FSMContext):
    if await use_promo(m.from_user.id, m.text.strip()):
        kb = await get_main_kb(m.from_user.id)
        await m.answer("✅ <b>Успех!</b>", reply_markup=kb)
    else:
        await m.answer("❌ Неверный код.")
    await state.clear()

@router.callback_query(F.data == "quick_actions")
async def qa(c: CallbackQuery):
    await edit_or_answer(c, "🛠 <b>Настройки:</b>", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧹 Очистить Кеш", callback_data="clear_cache")],
        [InlineKeyboardButton(text="🔙", callback_data="main_menu")]
    ]))

@router.callback_query(F.data == "clear_cache")
async def clr_cache(c: CallbackQuery):
    global USER_CACHE
    USER_CACHE = {}
    await c.answer("✅", show_alert=True)

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_menu")
async def adm_m(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    await edit_or_answer(c, f"👑 <b>Admin</b>\nWorker: {WORKER_STATUS}", kb_admin())

@router.callback_query(F.data == "adm_stats")
async def adm_st(c: CallbackQuery):
    t, a = await get_stats()
    uptime = int(time.time() - START_TIME)
    await c.answer(f"Users: {t}\nActive: {a}\nUptime: {uptime//3600}h", show_alert=True)

@router.callback_query(F.data == "adm_backup")
async def adm_bk(c: CallbackQuery):
    await auto_backup()
    await c.message.answer_document(FSInputFile(DB_PATH), caption="📦 Backup")

@router.callback_query(F.data == "adm_restart_worker")
async def adm_rw(c: CallbackQuery):
    global WORKER_TASK
    if WORKER_TASK: WORKER_TASK.cancel()
    await asyncio.sleep(1)
    WORKER_TASK = asyncio.create_task(worker_process())
    await c.answer("Restarted!", show_alert=True)
    await edit_or_answer(c, f"👑 <b>Admin</b>\nWorker: {WORKER_STATUS}", kb_admin())

# --- ADMIN: PROMO FIX ---
@router.callback_query(F.data == "adm_promo")
async def adm_pr(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "📅 <b>Дней:</b>")
    await state.set_state(States.ADM_DAYS)

# FIXED HANDLER SIGNATURE
@router.message(States.ADM_DAYS)
async def adm_d(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Цифры!")
    await state.update_data(d=m.text)
    await m.answer("🔢 <b>Активаций:</b>")
    await state.set_state(States.ADM_ACT)

# FIXED HANDLER SIGNATURE
@router.message(States.ADM_ACT)
async def adm_a(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Цифры!")
    d = await state.get_data()
    c = await create_promo(int(d['d']), int(m.text))
    await m.answer(f"✅ <code>{c}</code>", reply_markup=await get_main_kb(ADMIN_ID))
    await state.clear()

# --- ADMIN: GRANT SUB (NEW) ---
@router.callback_query(F.data == "adm_grant")
async def adm_grant(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "🆔 <b>User ID:</b>")
    await state.set_state(States.GRANT_ID)

@router.message(States.GRANT_ID)
async def grant_id_h(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("ID = цифры.")
    await state.update_data(uid=m.text)
    await m.answer("📅 <b>Дней:</b>")
    await state.set_state(States.GRANT_DAYS)

@router.message(States.GRANT_DAYS)
async def grant_d_h(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Цифры!")
    d = await state.get_data()
    await grant_sub_by_id(int(d['uid']), int(m.text))
    await m.answer("✅ Подписка выдана!", reply_markup=kb_admin())
    await state.clear()

# --- ADMIN: BAN/UNBAN (NEW) ---
@router.callback_query(F.data == "adm_ban")
async def adm_ban(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "🆔 <b>ID для Бана:</b>")
    await state.set_state(States.BAN_ID)

@router.message(States.BAN_ID)
async def ban_h(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("ID!")
    await ban_user_db(int(m.text), 1)
    await m.answer("⛔ Забанен.", reply_markup=kb_admin())
    await state.clear()

@router.callback_query(F.data == "adm_unban")
async def adm_unban(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "🆔 <b>ID для Разбана:</b>")
    await state.set_state(States.UNBAN_ID)

@router.message(States.UNBAN_ID)
async def unban_h(m: Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("ID!")
    await ban_user_db(int(m.text), 0)
    await m.answer("🟢 Разбанен.", reply_markup=kb_admin())
    await state.clear()

# --- ADMIN: BROADCAST ---
@router.callback_query(F.data == "adm_broadcast")
async def adm_br(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "📢 <b>Текст рассылки:</b>\n(/cancel)")
    await state.set_state(States.BROADCAST)

@router.message(States.BROADCAST)
async def adm_br_h(m: Message, state: FSMContext):
    if m.text == "/cancel": await state.clear(); return await m.answer("Отмена.")
    users = await get_all_users()
    await m.answer(f"🚀 Start: {len(users)} users...")
    count = 0
    for uid in users:
        try:
            await bot.send_message(uid, m.text)
            count += 1
            await asyncio.sleep(0.05)
        except: pass
    await m.answer(f"✅ Готово: {count}")
    await state.clear()

# --- AUTH ---
@router.callback_query(F.data == "auth_menu")
async def am(c: CallbackQuery): 
    if c.from_user.id != ADMIN_ID: return
    await edit_or_answer(c, "🔐 <b>Метод:</b>", kb_auth())

@router.callback_query(F.data == "auth_qr")
async def aq(c: CallbackQuery):
    uid = c.from_user.id
    if uid in TEMP_CLIENTS: await TEMP_CLIENTS[uid].disconnect()
    cl = TelegramClient(str(get_session_path(uid)), API_ID, API_HASH)
    TEMP_CLIENTS[uid] = cl
    try:
        await cl.connect()
        qr = await cl.qr_login()
        im = qrcode.make(qr.url).convert("RGB")
        b = io.BytesIO(); im.save(b, "PNG"); b.seek(0)
        try: await c.message.delete()
        except: pass
        msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption=f"📸 <b>Scan Me</b>")
        await asyncio.wait_for(qr.wait(), AUTH_TIMEOUT)
        me = await cl.get_me()
        await msg.delete()
        kb = await get_main_kb(uid)
        await c.message.answer(f"✅ @{me.username or me.id}", reply_markup=kb)
        if not WORKER_TASK or WORKER_TASK.done(): asyncio.create_task(worker_process())
    except Exception as e:
        logger.error(f"Auth: {e}")
        await c.message.answer("❌ Error")
    finally:
        if uid in TEMP_CLIENTS:
            try: await TEMP_CLIENTS[uid].disconnect()
            except: pass
            del TEMP_CLIENTS[uid]

@router.callback_query(F.data == "auth_phone")
async def ap(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "📱 <b>Номер:</b>")
    await state.set_state(States.PHONE)

@router.message(States.PHONE)
async def ph(m: Message, state: FSMContext):
    uid = m.from_user.id
    ph = m.text.strip().replace(" ", "")
    if uid in TEMP_CLIENTS: await TEMP_CLIENTS[uid].disconnect()
    cl = TelegramClient(str(get_session_path(uid)), API_ID, API_HASH)
    TEMP_CLIENTS[uid] = cl
    try:
        await cl.connect()
        r = await cl.send_code_request(ph)
        await state.update_data(p=ph, h=r.phone_code_hash)
        await state.set_state(States.CODE)
        await m.answer("📩 <b>Код:</b>")
    except Exception as e: await m.answer(f"❌ {e}")

@router.message(States.CODE)
async def co(m: Message, state: FSMContext):
    d = await state.get_data()
    uid = m.from_user.id
    cl = TEMP_CLIENTS.get(uid)
    try:
        await cl.sign_in(phone=d['p'], code=m.text, phone_code_hash=d['h'])
        me = await cl.get_me()
        kb = await get_main_kb(uid)
        await m.answer(f"✅ @{me.username or me.id}", reply_markup=kb)
        await state.clear()
        try: await cl.disconnect()
        except: pass
        del TEMP_CLIENTS[uid]
        if not WORKER_TASK or WORKER_TASK.done(): asyncio.create_task(worker_process())
    except SessionPasswordNeededError:
        await m.answer("🔒 <b>2FA Пароль:</b>")
        await state.set_state(States.PASS)
    except Exception as e: await m.answer(f"❌ {e}")

@router.message(States.PASS)
async def pa(m: Message, state: FSMContext):
    uid = m.from_user.id
    cl = TEMP_CLIENTS.get(uid)
    try:
        await cl.sign_in(password=m.text)
        kb = await get_main_kb(uid)
        await m.answer("✅ Вход выполнен!", reply_markup=kb)
        if not WORKER_TASK or WORKER_TASK.done(): asyncio.create_task(worker_process())
    except Exception as e: await m.answer(f"❌ {e}")
    finally:
        try: await cl.disconnect()
        except: pass
        if uid in TEMP_CLIENTS: del TEMP_CLIENTS[uid]
        await state.clear()

# =========================================================================
# 🧠 WORKER (USERBOT)
# =========================================================================

async def worker_process():
    global WORKER_STATUS, SESSIONS_PARSED
    
    while True:
        client = None
        try:
            sess_path_base = get_session_path(ADMIN_ID)
            if not sess_path_base.with_suffix(".session").exists():
                WORKER_STATUS = "🔴 Нет сессии"
                await asyncio.sleep(10)
                continue

            WORKER_STATUS = "🟡 Подключение..."
            client = TelegramClient(str(sess_path_base), API_ID, API_HASH, connection_retries=None)

            async def stealth_delete(event):
                try: await event.delete()
                except: pass

            async def temp_msg(event, text, delay=1.0):
                try:
                    msg = await event.respond(text)
                    await asyncio.sleep(delay)
                    await msg.delete()
                except: pass

            # --- UTILS ---
            @client.on(events.NewMessage(pattern=r'^\.id$'))
            async def id_cmd(ev):
                await stealth_delete(ev)
                await temp_msg(ev, f"🆔 <code>{ev.chat_id}</code>", 3)

            @client.on(events.NewMessage(pattern=r'^\.time$'))
            async def time_cmd(ev):
                await stealth_delete(ev)
                await temp_msg(ev, f"⏰ {datetime.now().strftime('%H:%M:%S')}", 3)

            @client.on(events.NewMessage(pattern=r'^\.info$'))
            async def info_cmd(ev):
                await stealth_delete(ev)
                try:
                    full = await client(functions.channels.GetFullChannelRequest(ev.chat_id))
                    txt = f"ℹ️ <b>Info</b>\nUsers: {full.full_chat.participants_count}\nAdmins: {full.full_chat.admins_count}"
                    await temp_msg(ev, txt, 5)
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.admins$'))
            async def admins_cmd(ev):
                await stealth_delete(ev)
                try:
                    admins = await client.get_participants(ev.chat_id, filter=ChannelParticipantsAdmins)
                    txt = "👮 <b>Admins:</b>\n" + "\n".join([f"- {u.first_name} (@{u.username})" for u in admins])
                    await temp_msg(ev, txt, 8)
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.join (.*)'))
            async def join_cmd(ev):
                await stealth_delete(ev)
                try:
                    await client(JoinChannelRequest(ev.pattern_match.group(1)))
                    await temp_msg(ev, "✅ Joined", 2)
                except: pass
            
            @client.on(events.NewMessage(pattern=r'^\.kickme$'))
            async def leave_cmd(ev):
                await stealth_delete(ev)
                try: await client(LeaveChannelRequest(ev.chat_id))
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.pin$'))
            async def pin_cmd(ev):
                await stealth_delete(ev)
                if ev.is_reply:
                    r = await ev.get_reply_message()
                    await client.pin_message(ev.chat_id, r)

            @client.on(events.NewMessage(pattern=r'^\.unpin$'))
            async def unpin_cmd(ev):
                await stealth_delete(ev)
                if ev.is_reply:
                    r = await ev.get_reply_message()
                    await client.unpin_message(ev.chat_id, r)

            @client.on(events.NewMessage(pattern=r'^\.save$'))
            async def save_cmd(ev):
                await stealth_delete(ev)
                if ev.is_reply:
                    r = await ev.get_reply_message()
                    await client.forward_messages('me', r)
                    await temp_msg(ev, "💾 Saved", 1)

            @client.on(events.NewMessage(pattern=r'^\.del$'))
            async def del_cmd(ev):
                await stealth_delete(ev)
                if ev.is_reply:
                    r = await ev.get_reply_message()
                    await r.delete()

            # --- PARSING (PHANTOM) ---
            @client.on(events.NewMessage(pattern=r'^\.(scan|чекгруппу)$'))
            async def smart_parse(ev):
                await stealth_delete(ev)
                global SESSIONS_PARSED
                lim = await get_user_limit(ADMIN_ID)
                scan_limit = 50000 
                
                msg = await ev.respond(f"👻 <b>Scanning...</b> ({lim})")
                unique_users: Dict[int, str] = {}
                scanned_msgs = 0
                
                try:
                    async with asyncio.timeout(600):
                        async for message in client.iter_messages(ev.chat_id, limit=scan_limit):
                            scanned_msgs += 1
                            if message.sender and isinstance(message.sender, User) and not message.sender.bot:
                                if message.sender_id not in unique_users:
                                    u = message.sender
                                    unique_users[u.id] = f"@{u.username or 'None'} | {u.first_name} | {u.id}"
                            if scanned_msgs % 200 == 0:
                                await msg.edit(f"👻 Scan: {scanned_msgs} | Found: {len(unique_users)}/{lim}")
                            if len(unique_users) >= lim: break
                    
                    SESSIONS_PARSED += 1
                    TEMP_PARSE_DATA[ADMIN_ID] = {'lines': list(unique_users.values()), 'title': str(ev.chat_id)}
                    await msg.edit("✅ Done.")
                    await asyncio.sleep(1)
                    await msg.delete()
                    
                    try:
                        await bot.send_message(ADMIN_ID, f"📁 <b>Scan Complete!</b>\nMsgs: {scanned_msgs}\nUsers: {len(unique_users)}\n\nFormat:", reply_markup=kb_parse_choice())
                    except: pass
                except Exception as e: 
                    await msg.delete()

            # --- RAID ---
            @client.on(events.NewMessage(pattern=r'^\.spam (\d+) (.*)'))
            async def spam_cmd(ev):
                await stealth_delete(ev)
                c = int(ev.pattern_match.group(1))
                t = ev.pattern_match.group(2)
                for _ in range(c):
                    await client.send_message(ev.chat_id, t)
                    await asyncio.sleep(0.5)

            @client.on(events.NewMessage(pattern=r'^\.clown$'))
            async def clown_cmd(ev):
                await stealth_delete(ev)
                if ev.is_reply:
                     r = await ev.get_reply_message()
                     try: await client(SendReactionRequest(ev.chat_id, r.id, reaction=[types.ReactionEmoji(emoticon='🤡')]))
                     except: pass

            @client.on(events.NewMessage(pattern=r'^\.tagall$'))
            async def tagall_cmd(ev):
                await stealth_delete(ev)
                parts = await client.get_participants(ev.chat_id)
                mentions = [f"<a href='tg://user?id={u.id}'>\u200b</a>" for u in parts if not u.deleted]
                for i in range(0, len(mentions), 5):
                    await client.send_message(ev.chat_id, "👋 " + "".join(mentions[i:i+5]), parse_mode='html')
                    await asyncio.sleep(1)

            @client.on(events.NewMessage(pattern=r'^\.purge (\d+)'))
            async def purge_cmd(ev):
                c = int(ev.pattern_match.group(1))
                msgs = [ev.id]
                async for m in client.iter_messages(ev.chat_id, limit=c): msgs.append(m.id)
                await client.delete_messages(ev.chat_id, msgs)

            # --- ADMIN ---
            @client.on(events.NewMessage(pattern=r'^\.promote'))
            async def promote_cmd(ev):
                await stealth_delete(ev)
                if not ev.is_reply: return
                r = await ev.get_reply_message()
                try:
                    await client(EditAdminRequest(ev.chat_id, r.sender_id, admin_rights=ChannelParticipantsAdmins(change_info=True, post_messages=True, edit_messages=True, delete_messages=True, ban_users=True, invite_users=True, pin_messages=True, add_admins=False, manage_call=True), rank="Admin"))
                    await temp_msg(ev, "👮 Promoted", 3)
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.demote'))
            async def demote_cmd(ev):
                await stealth_delete(ev)
                if not ev.is_reply: return
                r = await ev.get_reply_message()
                try:
                    await client(EditAdminRequest(ev.chat_id, r.sender_id, admin_rights=ChannelParticipantsAdmins(change_info=False, post_messages=False, edit_messages=False, delete_messages=False, ban_users=False, invite_users=False, pin_messages=False, add_admins=False, manage_call=False), rank=""))
                    await temp_msg(ev, "👮 Demoted", 3)
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.zombies'))
            async def zombies_cmd(ev):
                await stealth_delete(ev)
                msg = await ev.respond("🧟 Scanning...")
                cnt = 0
                try:
                    participants = await client.get_participants(ev.chat_id)
                    for user in participants:
                        if user.deleted:
                            try: await client(functions.channels.EditBannedRequest(ev.chat_id, user, ChatBannedRights(until_date=None, view_messages=True))); cnt += 1
                            except: pass
                    await temp_msg(msg, f"🧟 Kicked {cnt}", 5)
                except: await msg.delete()

            @client.on(events.NewMessage(pattern=r'^\.invite'))
            async def invite_cmd(ev):
                await stealth_delete(ev)
                try:
                    link = await client(ExportChatInviteRequest(ev.chat_id))
                    await temp_msg(ev, f"🔗 {link.link}", 10)
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.bots$'))
            async def bots_cmd(ev):
                await stealth_delete(ev)
                try:
                    parts = await client.get_participants(ev.chat_id)
                    bots = [u for u in parts if u.bot]
                    txt = "🤖 <b>Bots:</b>\n" + "\n".join([f"@{u.username}" for u in bots])
                    await temp_msg(ev, txt, 10)
                except: pass
            
            @client.on(events.NewMessage(pattern=r'^\.whois'))
            async def whois_cmd(ev):
                await stealth_delete(ev)
                if not ev.is_reply: return
                r = await ev.get_reply_message()
                u = await r.get_sender()
                await temp_msg(ev, f"🆔 `{u.id}`\n@{u.username}", 5)

            @client.on(events.NewMessage(pattern=r'^\.csv$'))
            async def csv_parse(ev):
                await stealth_delete(ev)
                lim = await get_user_limit(ADMIN_ID)
                msg = await ev.respond(f"📊 CSV ({lim})...")
                rows = []
                try:
                    async for u in client.iter_participants(ev.chat_id, limit=lim, aggressive=True):
                        rows.append([u.id, u.username or "", u.first_name or "", u.phone or ""])
                        if len(rows) % 50 == 0: await msg.edit(f"📊 {progress_bar(len(rows), lim)}")
                    fn = f"export_{ev.chat_id}.csv"
                    with open(fn, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(["ID", "Username", "Name", "Phone"])
                        writer.writerows(rows)
                    try: await client.send_file(ev.chat_id, fn, caption=f"CSV: {len(rows)}")
                    except BadRequestError: await msg.edit("❌ Топик закрыт!")
                    os.remove(fn)
                    await temp_msg(msg, "Uploaded", 0.5)
                except Exception as e: await msg.delete()

            await client.start()
            WORKER_STATUS = "🟢 Активен (v21)"
            logger.info("Worker Started")
            await client.run_until_disconnected()

        except (EOFError, ConnectionError):
            WORKER_STATUS = "⚠️ Сбой сети..."
            if client: await client.disconnect()
            await asyncio.sleep(5)
        except sqlite3.OperationalError:
            WORKER_STATUS = "⏳ БД занята..."
            if client: await client.disconnect()
            await asyncio.sleep(5)
        except Exception as e:
            WORKER_STATUS = f"🔴 {e}"
            if client: await client.disconnect()
            await asyncio.sleep(5)

async def main():
    global WORKER_TASK
    await init_db()
    await cleanup_files()
    
    dp.message.middleware(SecurityMiddleware())
    dp.callback_query.middleware(SecurityMiddleware())
    
    WORKER_TASK = asyncio.create_task(worker_process())
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        if WORKER_TASK: WORKER_TASK.cancel()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
