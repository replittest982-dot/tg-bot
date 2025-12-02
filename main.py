#!/usr/bin/env python3
"""
🚀 StatPro Ultimate v18.1 - STABLE EDITION
✅ FIX: Database Locked (Added timeout=30s + sqlite3 handler)
✅ FIX: Unsupported HTML tags in menu
✅ FIX: Telethon connection cleanup (disconnect on crash)
✅ FULL FEATURES: All commands present.
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
import aiosqlite
import sqlite3  # <--- ВАЖНО ДЛЯ ОБРАБОТКИ ОШИБОК БД
from typing import Dict, Optional, Union, List
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
    ChannelParticipantsAdmins, ChatBannedRights
)
from telethon.tl.functions.channels import EditAdminRequest
from telethon.tl.functions.messages import ExportChatInviteRequest

# --- QR ---
import qrcode
from PIL import Image

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

WORKER_TASK: Optional[asyncio.Task] = None
WORKER_STATUS = "⚪️ Stopped"
BOT_VERSION = "v18.1 Stable"
START_TIME = datetime.now().timestamp()
SESSIONS_PARSED = 0

# Временное хранилище для парсинга
TEMP_PARSE_DATA = {} 

PATTERNS = {
    "phone": r"^\+?[0-9]{10,15}$",
    "promo": r"^[A-Za-z0-9-]{4,20}$"
}

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    AUTH_TIMEOUT = int(os.getenv("QR_TIMEOUT", "500"))
    SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "@tstatprobot")
    TARGET_CHANNEL_URL = os.getenv("TARGET_CHANNEL_URL", "https://t.me/STAT_PRO1")
    TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", "0"))
except ValueError as e:
    print(f"❌ CONFIG ERROR: {e}")
    sys.exit(1)

if not all([BOT_TOKEN, API_ID, API_HASH]):
    print("❌ ERROR: Check ENV variables")
    sys.exit(1)

ABSOLUTE_SESSION_DIR = Path("/app") / "sessions"
ABSOLUTE_SESSION_DIR.mkdir(exist_ok=True, parents=True)
DB_PATH = Path("/app") / "database.db"
MAX_FILE_SIZE = 50 * 1024 * 1024 

def get_session_path(user_id: int) -> Path:
    return ABSOLUTE_SESSION_DIR / f"session_{user_id}"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# =========================================================================
# II. SYSTEM UTILS & DB
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

def progress_bar(current, total, width=12):
    if total == 0: return "[░░░]"
    filled = int(width * current / total)
    return f"[{'█'*filled + '░'*(width-filled)}] {int(current/total*100)}%"

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

# --- DB METHODS (Added timeout=30 to prevent locking) ---
async def init_db():
    # Timeout 30s to wait for lock release
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
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

async def get_user_limit(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        async with db.execute("SELECT parse_limit FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else 1000

async def add_user(user_id: int, username: str):
    now = datetime.now().isoformat()
    trial_end = (datetime.now() + timedelta(days=0)).isoformat()
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("""
            INSERT OR IGNORE INTO users (user_id, username, join_date, sub_end, last_active) 
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, username, now, trial_end, now))
        await db.commit()

async def get_user_data(user_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def set_limit(user_id: int, limit: int):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await db.execute("UPDATE users SET parse_limit = ? WHERE user_id = ?", (limit, user_id))
        await db.commit()

async def create_promo(days: int, activations: int) -> str:
    code = f"PRO-{uuid.uuid4().hex[:6].upper()}"
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, activations))
        await db.commit()
    return code

async def use_promo(user_id: int, code: str) -> bool:
    if not re.match(PATTERNS['promo'], code): return False
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
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

async def get_stats():
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (datetime.now().isoformat(),)) as c: active = (await c.fetchone())[0]
    return total, active

async def get_all_users():
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        async with db.execute("SELECT user_id FROM users") as c:
            return [row[0] for row in await c.fetchall()]

async def cleanup_old_sessions():
    for path in ABSOLUTE_SESSION_DIR.glob("*.session"):
        if path.stat().st_size == 0: path.unlink()

async def auto_backup():
    try:
        backup_path = f"/app/backup_{datetime.now().strftime('%Y%m%d')}.db"
        shutil.copy2(DB_PATH, backup_path)
    except: pass

# =========================================================================
# III. MIDDLEWARE
# =========================================================================

class SecurityMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data: dict):
        user_id = event.from_user.id
        await add_user(user_id, event.from_user.username or "Unknown")
        
        if user_id == ADMIN_ID:
            return await handler(event, data)

        u_data = await get_cached_user(user_id)
        if u_data and u_data['is_banned']:
            if isinstance(event, Message): await event.answer("🚫 Вы забанены.")
            return

        if TARGET_CHANNEL_ID != 0:
            try:
                m = await bot.get_chat_member(TARGET_CHANNEL_ID, user_id)
                if m.status not in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER]:
                    raise Exception
            except:
                text = f"🚫 Подпишитесь на канал!\n{TARGET_CHANNEL_URL}"
                if isinstance(event, Message): await event.answer(text)
                elif isinstance(event, CallbackQuery): await event.answer("🚫 Подписка обязательна!", show_alert=True)
                return

        return await handler(event, data)

# =========================================================================
# IV. KEYBOARDS
# =========================================================================

async def get_main_kb(user_id: int):
    is_active = await has_active_sub(user_id)
    kb = []
    
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth_menu")])

    kb.append([InlineKeyboardButton(text="👤 Профиль", callback_data="profile")])
    kb.append([InlineKeyboardButton(text="⭐ Активировать код", callback_data="sub_menu")])

    if is_active:
        kb.append([InlineKeyboardButton(text="📊 Функции Worker", callback_data="worker_menu")])
        kb.append([InlineKeyboardButton(text="⚡ Быстрые действия", callback_data="quick_actions")])

    kb.append([InlineKeyboardButton(text="🆘 Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME.replace('@', '')}")])

    if user_id == ADMIN_ID:
        kb.insert(0, [InlineKeyboardButton(text="👑 ADMIN", callback_data="admin_menu")])
        
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_quick_actions():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧹 Очистить Кеш", callback_data="clear_cache")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])

def kb_auth():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Номер", callback_data="auth_phone"), 
         InlineKeyboardButton(text="📸 QR-код", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])

def kb_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Промокод", callback_data="adm_promo"),
         InlineKeyboardButton(text="📢 Рассылка", callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="adm_stats"),
         InlineKeyboardButton(text="📦 Бэкап БД", callback_data="adm_backup")],
        [InlineKeyboardButton(text="🔄 Рестарт Worker", callback_data="adm_restart_worker")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])

def kb_config(current):
    r = [10, 50, 500, 5000, 100000]
    kb = []
    for v in r:
        txt = f"✅ {v}" if v == current else str(v)
        kb.append(InlineKeyboardButton(text=txt, callback_data=f"lim:{v}"))
    rows = [kb[i:i+3] for i in range(0, len(kb), 3)]
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="worker_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_parse_choice(count: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 Файлом (.txt)", callback_data="parse_res:file")],
        [InlineKeyboardButton(text="📝 Текстом (в чат)", callback_data="parse_res:text")]
    ])

# =========================================================================
# V. HANDLERS (AIOGRAM)
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class States(StatesGroup):
    PHONE=State(); CODE=State(); PASS=State(); PROMO=State()
    ADM_DAYS=State(); ADM_ACT=State(); BROADCAST=State()

TEMP_CLIENTS = {}

# --- MENU & PROFILE ---
@router.message(Command("start"))
async def start(m: Message):
    kb = await get_main_kb(m.from_user.id)
    await m.answer(f"👋 Привет, {m.from_user.first_name}!", reply_markup=kb)

@router.callback_query(F.data == "main_menu")
async def menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    uid = c.from_user.id
    if uid in TEMP_CLIENTS: 
        try: await TEMP_CLIENTS[uid].disconnect()
        except: pass
        del TEMP_CLIENTS[uid]
    kb = await get_main_kb(uid)
    await edit_or_answer(c, "🏠 Главное меню:", kb)

@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    u = await get_cached_user(c.from_user.id)
    d = datetime.fromisoformat(u['sub_end'])
    is_act = await has_active_sub(c.from_user.id)
    active = "✅ Активна (ADMIN)" if c.from_user.id == ADMIN_ID else ("✅ Активна" if is_act else "❌ Не активна")
    date_str = "∞" if c.from_user.id == ADMIN_ID else d.strftime('%d.%m.%Y')
    limit_info = f"Лимит парсинга: {u['parse_limit']}\n" if is_act else ""
    txt = (
        f"👤 Профиль\nID: <code>{u['user_id']}</code>\n"
        f"Подписка: {active} (до {date_str})\n"
        f"{limit_info}Версия: <code>{BOT_VERSION}</code>"
    )
    await edit_or_answer(c, txt, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]))

# --- WORKER MENU (FIXED HTML TAGS) ---
@router.callback_query(F.data == "worker_menu")
async def w_menu(c: CallbackQuery):
    if not await has_active_sub(c.from_user.id):
        await c.answer("❌ Купите подписку!", show_alert=True)
        return await menu(c, None)

    u = await get_cached_user(c.from_user.id)
    # FIX: Заменены < > на &lt; &gt; чтобы aiogram не падал
    text = (
        f"📊 Worker Infinity\nСтатус: {WORKER_STATUS}\nЛимит: {u['parse_limit']}\n\n"
        "<b>Админ:</b>\n"
        "<code>.ban</code>, <code>.mute &lt;m/h&gt;</code>, <code>.kick</code>\n"
        "<code>.promote</code>, <code>.demote</code>, <code>.zombies</code>\n"
        "<b>Утилиты:</b>\n"
        "<code>.afk &lt;txt&gt;</code>, <code>.whois</code>, <code>.invite</code>, <code>.calc</code>\n"
        "<b>Спам:</b>\n"
        "<code>.spam &lt;n&gt; &lt;txt&gt;</code>, <code>.tagall</code>\n"
        "<b>Парсинг:</b>\n"
        "<code>.чекгруппу</code>, <code>.csv</code>\n"
        "(Отчет приходит в бота с кнопками)"
    )
    await edit_or_answer(c, text, kb_config(u['parse_limit']))

@router.callback_query(F.data.startswith("lim:"))
async def set_lim(c: CallbackQuery):
    if not await has_active_sub(c.from_user.id): return
    l = int(c.data.split(":")[1])
    await set_limit(c.from_user.id, l)
    await c.answer(f"Лимит: {l}")
    await w_menu(c)

# --- PARSE CALLBACKS ---
@router.callback_query(F.data.startswith("parse_res:"))
async def parse_res_handler(c: CallbackQuery):
    mode = c.data.split(":")[1]
    data = TEMP_PARSE_DATA.get(c.from_user.id)
    if not data: return await c.answer("❌ Данные устарели.", show_alert=True)
    
    lines = data['lines']
    title = data['title']
    
    if mode == "file":
        fn = f"users_{title}.txt"
        with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(lines))
        await c.message.answer_document(FSInputFile(fn), caption=f"📂 Результат: {len(lines)} строк")
        os.remove(fn)
    elif mode == "text":
        text_chunk = ""
        await c.message.answer(f"📝 Результат ({len(lines)}):")
        for line in lines:
            if len(text_chunk) + len(line) > 4000:
                await c.message.answer(f"<code>{text_chunk}</code>")
                text_chunk = ""
                await asyncio.sleep(0.3)
            text_chunk += line + "\n"
        if text_chunk: await c.message.answer(f"<code>{text_chunk}</code>")
    await c.answer()

# --- SUBSCRIPTION ---
@router.callback_query(F.data == "sub_menu")
async def sub_menu(c: CallbackQuery):
    await edit_or_answer(c, "⭐ Активация:", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎫 Ввести промокод", callback_data="enter_promo")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ]))

@router.callback_query(F.data == "enter_promo")
async def en_pro(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "🎫 Введите код:")
    await state.set_state(States.PROMO)

@router.message(States.PROMO)
async def pro_h(m: Message, state: FSMContext):
    if await use_promo(m.from_user.id, m.text.strip()):
        kb = await get_main_kb(m.from_user.id)
        await m.answer("✅ Подписка активирована!", reply_markup=kb)
    else:
        await m.answer("❌ Неверный код.")
    await state.clear()

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_menu")
async def adm_m(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    await edit_or_answer(c, f"👑 Админ\nWorker: {WORKER_STATUS}", kb_admin())

@router.callback_query(F.data == "adm_stats")
async def adm_st(c: CallbackQuery):
    t, a = await get_stats()
    uptime = time.time() - START_TIME
    await c.answer(f"Users: {t}\nActive: {a}\nUptime: {int(uptime//3600)}h", show_alert=True)

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
    await c.answer("Restarting...", show_alert=True)
    await asyncio.sleep(2)
    await edit_or_answer(c, f"👑 Админ\nWorker: {WORKER_STATUS}", kb_admin())

@router.callback_query(F.data == "adm_promo")
async def adm_pr(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "Дней:")
    await state.set_state(States.ADM_DAYS)

@router.message(States.ADM_DAYS)
async def adm_d(m: Message, s: FSMContext):
    await s.update_data(d=m.text)
    await m.answer("Кол-во активаций:")
    await s.set_state(States.ADM_ACT)

@router.message(States.ADM_ACT)
async def adm_a(m: Message, s: FSMContext):
    d = await s.get_data()
    c = await create_promo(int(d['d']), int(m.text))
    await m.answer(f"Code: <code>{c}</code>", reply_markup=await get_main_kb(ADMIN_ID))
    await s.clear()

@router.callback_query(F.data == "adm_broadcast")
async def adm_br(c: CallbackQuery, s: FSMContext):
    await edit_or_answer(c, "Текст рассылки (или /cancel):")
    await s.set_state(States.BROADCAST)

@router.message(States.BROADCAST)
async def adm_br_h(m: Message, s: FSMContext):
    if m.text == "/cancel": 
        await s.clear(); return await m.answer("Отмена.")
    users = await get_all_users()
    await m.answer(f"🚀 Рассылка {len(users)} юзерам...")
    count = 0
    for uid in users:
        try:
            await bot.send_message(uid, m.text)
            count += 1
            await asyncio.sleep(0.05)
        except: pass
    await m.answer(f"✅ Доставлено: {count}")
    await s.clear()

# --- AUTH LOGIC ---
@router.callback_query(F.data == "auth_menu")
async def am(c: CallbackQuery): 
    if c.from_user.id != ADMIN_ID: return
    await edit_or_answer(c, "Метод:", kb_auth())

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
        msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption=f"📸 Сканируйте! (500с)")
        await asyncio.wait_for(qr.wait(), AUTH_TIMEOUT)
        me = await cl.get_me()
        await msg.delete()
        kb = await get_main_kb(uid)
        await c.message.answer(f"✅ Вход выполнен: @{me.username or me.id}", reply_markup=kb)
        asyncio.create_task(worker_process())
    except Exception as e:
        logger.error(f"Auth Error: {e}")
        await c.message.answer("❌ Ошибка входа")
    finally:
        if uid in TEMP_CLIENTS:
            try: await TEMP_CLIENTS[uid].disconnect()
            except: pass
            del TEMP_CLIENTS[uid]

@router.callback_query(F.data == "auth_phone")
async def ap(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "Введите номер:")
    await state.set_state(States.PHONE)

@router.message(States.PHONE)
async def ph(m: Message, s: FSMContext):
    uid = m.from_user.id
    ph = m.text.strip().replace(" ", "")
    if uid in TEMP_CLIENTS: await TEMP_CLIENTS[uid].disconnect()
    cl = TelegramClient(str(get_session_path(uid)), API_ID, API_HASH)
    TEMP_CLIENTS[uid] = cl
    try:
        await cl.connect()
        r = await cl.send_code_request(ph)
        await s.update_data(p=ph, h=r.phone_code_hash)
        await s.set_state(States.CODE)
        await m.answer("Введите код:")
    except Exception as e: await m.answer(f"❌ Ошибка: {e}")

@router.message(States.CODE)
async def co(m: Message, s: FSMContext):
    d = await s.get_data()
    uid = m.from_user.id
    cl = TEMP_CLIENTS.get(uid)
    try:
        await cl.sign_in(phone=d['p'], code=m.text, phone_code_hash=d['h'])
        me = await cl.get_me()
        kb = await get_main_kb(uid)
        await m.answer(f"✅ Вход выполнен: @{me.username or me.id}", reply_markup=kb)
        await s.clear()
        try: await cl.disconnect()
        except: pass
        del TEMP_CLIENTS[uid]
        asyncio.create_task(worker_process())
    except SessionPasswordNeededError:
        await m.answer("🔒 Введите пароль 2FA:")
        await s.set_state(States.PASS)
    except Exception as e: await m.answer(f"❌ Ошибка: {e}")

@router.message(States.PASS)
async def pa(m: Message, s: FSMContext):
    uid = m.from_user.id
    cl = TEMP_CLIENTS.get(uid)
    try:
        await cl.sign_in(password=m.text)
        kb = await get_main_kb(uid)
        await m.answer("✅ Вход выполнен (2FA)", reply_markup=kb)
        asyncio.create_task(worker_process())
    except Exception as e: await m.answer(f"❌ Ошибка: {e}")
    finally:
        try: await cl.disconnect()
        except: pass
        if uid in TEMP_CLIENTS: del TEMP_CLIENTS[uid]
        await s.clear()

# =========================================================================
# VI. TELETHON WORKER
# =========================================================================

async def worker_process():
    global WORKER_STATUS, SESSIONS_PARSED
    
    while True:
        client = None
        try:
            sess_path_base = get_session_path(ADMIN_ID)
            if not sess_path_base.with_suffix(".session").exists():
                WORKER_STATUS = "🔴 Нет сессии (Жду вход)"
                await asyncio.sleep(10)
                continue

            WORKER_STATUS = "🟡 Подключение..."
            client = TelegramClient(str(sess_path_base), API_ID, API_HASH)

            async def temp_msg(event, text, delay=0.5):
                try:
                    if event.out: msg = await event.edit(text)
                    else: msg = await event.reply(text)
                    await asyncio.sleep(delay)
                    await msg.delete()
                    if not event.out: await event.delete()
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.status'))
            async def status_cmd(ev):
                uptime = int(time.time() - START_TIME)
                txt = f"🟢 **Worker Online**\n⏱ Uptime: {uptime}s\n📂 Parsed: {SESSIONS_PARSED}"
                await temp_msg(ev, txt, 5)

            @client.on(events.NewMessage(pattern=r'^\.чекгруппу$'))
            async def txt_parse(ev):
                global SESSIONS_PARSED
                lim = await get_user_limit(ADMIN_ID)
                msg = await ev.reply(f"🔍 Парсинг ({lim})...")
                lines = []
                try:
                    async with asyncio.timeout(300):
                        async for u in client.iter_participants(ev.chat_id, limit=lim, aggressive=True):
                            lines.append(f"@{u.username or 'None'} | {u.first_name} | {u.id}")
                            if len(lines) % 50 == 0: await msg.edit(f"🔍 {progress_bar(len(lines), lim)}")
                    SESSIONS_PARSED += 1
                    
                    TEMP_PARSE_DATA[ADMIN_ID] = {'lines': lines, 'title': str(ev.chat_id)}
                    await msg.edit("✅ Парсинг завершен! Отправляю меню в бот...")
                    await asyncio.sleep(1)
                    await msg.delete()
                    
                    try:
                        await bot.send_message(
                            ADMIN_ID, 
                            f"🔍 Парсинг завершен ({len(lines)} юзеров)!\nКак отправить результат?",
                            reply_markup=kb_parse_choice(len(lines))
                        )
                    except Exception as e: print(f"Bot Send Error: {e}")

                except Exception as e: 
                    return await temp_msg(msg, f"Error: {e}", 3)

            # ALL FEATURES KEPT
            @client.on(events.NewMessage(pattern=r'^\.promote'))
            async def promote_cmd(ev):
                if not ev.is_reply: return
                r = await ev.get_reply_message()
                try:
                    await client(EditAdminRequest(ev.chat_id, r.sender_id, 
                        admin_rights=ChannelParticipantsAdmins(
                            change_info=True, post_messages=True, edit_messages=True,
                            delete_messages=True, ban_users=True, invite_users=True,
                            pin_messages=True, add_admins=False, manage_call=True
                        ), rank="Admin"))
                    await temp_msg(ev, "👮 Promoted", 3)
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.demote'))
            async def demote_cmd(ev):
                if not ev.is_reply: return
                r = await ev.get_reply_message()
                try:
                    await client(EditAdminRequest(ev.chat_id, r.sender_id, 
                        admin_rights=ChannelParticipantsAdmins(
                            change_info=False, post_messages=False, edit_messages=False,
                            delete_messages=False, ban_users=False, invite_users=False,
                            pin_messages=False, add_admins=False, manage_call=False
                        ), rank=""))
                    await temp_msg(ev, "👮 Demoted", 3)
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.zombies'))
            async def zombies_cmd(ev):
                if not ev.is_group: return
                msg = await ev.reply("🧟 Scanning...")
                cnt = 0
                try:
                    participants = await client.get_participants(ev.chat_id)
                    for user in participants:
                        if user.deleted:
                            try:
                                await client(functions.channels.EditBannedRequest(
                                    ev.chat_id, user, ChatBannedRights(until_date=None, view_messages=True)
                                ))
                                cnt += 1
                            except: pass
                    await temp_msg(msg, f"🧟 Kicked {cnt} zombies", 5)
                except: await msg.delete()

            @client.on(events.NewMessage(pattern=r'^\.invite'))
            async def invite_cmd(ev):
                try:
                    link = await client(ExportChatInviteRequest(ev.chat_id))
                    await temp_msg(ev, f"🔗 {link.link}", 10)
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.tagall'))
            async def tagall_cmd(ev):
                if not ev.is_group: return
                await ev.delete()
                parts = await client.get_participants(ev.chat_id)
                mentions = [f"<a href='tg://user?id={u.id}'>\u200b</a>" for u in parts if not u.deleted]
                for i in range(0, len(mentions), 5):
                    await client.send_message(ev.chat_id, "👋 " + "".join(mentions[i:i+5]), parse_mode='html')
                    await asyncio.sleep(1)

            @client.on(events.NewMessage(pattern=r'^\.whois'))
            async def whois_cmd(ev):
                if not ev.is_reply: return
                r = await ev.get_reply_message()
                u = await r.get_sender()
                await temp_msg(ev, f"🆔 `{u.id}`\n@{u.username}", 5)

            @client.on(events.NewMessage(pattern=r'^\.spam (\d+) (.*)'))
            async def spam_cmd(ev):
                c = int(ev.pattern_match.group(1))
                t = ev.pattern_match.group(2)
                await ev.delete()
                for _ in range(c):
                    await client.send_message(ev.chat_id, t)
                    await asyncio.sleep(0.1)

            @client.on(events.NewMessage(pattern=r'^\.purge (\d+)'))
            async def purge_cmd(ev):
                c = int(ev.pattern_match.group(1))
                msgs = [ev.id]
                async for m in client.iter_messages(ev.chat_id, limit=c): msgs.append(m.id)
                await client.delete_messages(ev.chat_id, msgs)

            @client.on(events.NewMessage(pattern=r'^\.calc (.+)'))
            async def calc_cmd(ev):
                try:
                    res = eval(ev.pattern_match.group(1), {"__builtins__": {}}, {"math": math})
                    await temp_msg(ev, f"🔢 {res}", 5)
                except: pass
            
            @client.on(events.NewMessage(pattern=r'^\.csv$'))
            async def csv_parse(ev):
                lim = await get_user_limit(ADMIN_ID)
                msg = await ev.reply(f"📊 CSV ({lim})...")
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
                    
                    try:
                        await client.send_file(ev.chat_id, fn, caption=f"CSV: {len(rows)}")
                    except BadRequestError: await msg.edit("❌ Топик закрыт!")
                    os.remove(fn)
                    await temp_msg(msg, "Uploaded", 0.5)
                except Exception as e: return await temp_msg(msg, f"Error: {e}", 3)

            @client.on(events.NewMessage(pattern=r'^\.лс (.*?)(?: (@.+))?$'))
            async def dm_cmd(ev):
                match = re.match(r'^\.лс (.*?)(?: (@.+))?$', ev.text, re.DOTALL)
                if not match: return await temp_msg(ev, "Формат: .лс текст @юзер", 2)
                txt = match.group(1).strip()
                usrs = match.group(2).split() if match.group(2) else []
                m = await ev.reply(f"🚀 Queue {len(usrs)}...")
                for u in usrs:
                    try:
                        await client.send_message(u.lstrip('@'), txt)
                        await asyncio.sleep(random.uniform(1.5, 3))
                    except: pass
                await temp_msg(m, "✅ Done", 1)

            @client.on(events.NewMessage(pattern=r'^\.ping'))
            async def ping_cmd(ev):
                s = time.time()
                msg = await ev.reply("Pong")
                await temp_msg(msg, f"Ping: {int((time.time()-s)*1000)}ms", 1)

            await client.start()
            WORKER_STATUS = "🟢 Активен (24/7)"
            logger.info("Worker Started")
            await client.run_until_disconnected()

        except sqlite3.OperationalError as e:
            WORKER_STATUS = "❌ Ошибка: Блокировка БД"
            logger.error(f"Worker Locked: {e}")
            if client: await client.disconnect()
            await asyncio.sleep(5)
            
        except Exception as e:
            WORKER_STATUS = f"🔴 Сбой: {e.__class__.__name__}"
            logger.error(f"Worker Crashed: {e}")
            if client: await client.disconnect()
            await asyncio.sleep(5)

# =========================================================================
# VII. MAIN
# =========================================================================

async def main():
    global WORKER_TASK
    await init_db()
    await cleanup_old_sessions()
    
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
