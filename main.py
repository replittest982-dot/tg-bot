#!/usr/bin/env python3
"""
💎 StatPro Ultimate v19.0 - ARCHITECT EDITION
---------------------------------------------
✅ UI/UX: Премиальный дизайн сообщений и меню.
✅ CORE: Защита от EOF, Database Lock и Network Errors.
✅ FEATURES: Full Pack (.zombies, .promote, .invite, .spam, etc).
✅ HYBRID: Парсинг Worker -> Отчет Bot (Файл/Текст).
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
# ⚙️ КОНФИГУРАЦИЯ
# =========================================================================

WORKER_TASK: Optional[asyncio.Task] = None
WORKER_STATUS = "⚪️ Ожидание запуска..."
BOT_VERSION = "v19.0 Architect"
START_TIME = datetime.now().timestamp()
SESSIONS_PARSED = 0

# Временное хранилище для передачи данных от Worker к Bot
TEMP_PARSE_DATA = {} 

PATTERNS = {
    "phone": r"^\+?[0-9]{10,15}$",
    "promo": r"^[A-Za-z0-9-]{4,20}$"
}

# Настройки путей
BASE_DIR = Path("/app")
SESSION_DIR = BASE_DIR / "sessions"
SESSION_DIR.mkdir(exist_ok=True, parents=True)
DB_PATH = BASE_DIR / "database.db"
MAX_FILE_SIZE = 50 * 1024 * 1024 

# Логгер
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("StatPro")

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    AUTH_TIMEOUT = int(os.getenv("QR_TIMEOUT", "500"))
    SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "@tstatprobot")
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
# 🗄️ БАЗА ДАННЫХ (Optimized)
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

async def db_connect():
    """Создает подключение с правильным таймаутом"""
    return await aiosqlite.connect(DB_PATH, timeout=30.0)

async def init_db():
    async with await db_connect() as db:
        # Оптимизация производительности
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        
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

# --- DB Accessors ---

async def get_user_limit(user_id: int) -> int:
    async with await db_connect() as db:
        async with db.execute("SELECT parse_limit FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else 1000

async def add_user(user_id: int, username: str):
    now = datetime.now().isoformat()
    trial_end = (datetime.now() + timedelta(days=0)).isoformat()
    async with await db_connect() as db:
        await db.execute("""
            INSERT OR IGNORE INTO users (user_id, username, join_date, sub_end, last_active) 
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, username, now, trial_end, now))
        await db.commit()

async def get_user_data(user_id: int):
    async with await db_connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def set_limit(user_id: int, limit: int):
    async with await db_connect() as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await db.execute("UPDATE users SET parse_limit = ? WHERE user_id = ?", (limit, user_id))
        await db.commit()

async def create_promo(days: int, activations: int) -> str:
    code = f"PRO-{uuid.uuid4().hex[:6].upper()}"
    async with await db_connect() as db:
        await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, activations))
        await db.commit()
    return code

async def use_promo(user_id: int, code: str) -> bool:
    if not re.match(PATTERNS['promo'], code): return False
    async with await db_connect() as db:
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
    async with await db_connect() as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (datetime.now().isoformat(),)) as c: active = (await c.fetchone())[0]
    return total, active

async def get_all_users():
    async with await db_connect() as db:
        async with db.execute("SELECT user_id FROM users") as c:
            return [row[0] for row in await c.fetchall()]

async def cleanup_files():
    """Очищает пустые сессии и временные файлы"""
    try:
        for path in SESSION_DIR.glob("*.session"):
            if path.stat().st_size == 0: path.unlink()
    except: pass

async def auto_backup():
    try:
        backup_path = BASE_DIR / f"backup_{datetime.now().strftime('%Y%m%d')}.db"
        shutil.copy2(DB_PATH, backup_path)
    except: pass

# =========================================================================
# 🛡️ MIDDLEWARE & UTILS
# =========================================================================

async def edit_or_answer(message_obj: Union[Message, CallbackQuery], text: str, reply_markup=None):
    """Универсальная функция для ответа"""
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
        
        # Admin God Mode
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
# ⌨️ UI / KEYBOARDS
# =========================================================================

async def get_main_kb(user_id: int):
    is_active = await has_active_sub(user_id)
    kb = []
    
    # Секретный вход для Админа
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth_menu")])

    kb.append([InlineKeyboardButton(text="👤 Мой Профиль", callback_data="profile")])
    kb.append([InlineKeyboardButton(text="🎟 Активация доступа", callback_data="sub_menu")])

    if is_active:
        kb.append([InlineKeyboardButton(text="⚡️ StatPro Infinity", callback_data="worker_menu")])
        kb.append([InlineKeyboardButton(text="🛠 Быстрые действия", callback_data="quick_actions")])

    kb.append([InlineKeyboardButton(text="💬 Тех. Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME.replace('@', '')}")])

    if user_id == ADMIN_ID:
        kb.insert(0, [InlineKeyboardButton(text="👑 Панель Управления", callback_data="admin_menu")])
        
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_auth():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Вход по номеру", callback_data="auth_phone"), 
         InlineKeyboardButton(text="📸 Вход по QR", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")]
    ])

def kb_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать Промокод", callback_data="adm_promo"),
         InlineKeyboardButton(text="📢 Рассылка (Broadcast)", callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm_stats"),
         InlineKeyboardButton(text="📦 Бэкап Базы", callback_data="adm_backup")],
        [InlineKeyboardButton(text="🔄 Рестарт Worker", callback_data="adm_restart_worker")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")]
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
        [InlineKeyboardButton(text="📂 Скачать Файлом (.txt)", callback_data="parse_res:file")],
        [InlineKeyboardButton(text="📝 Прислать Текстом", callback_data="parse_res:text")]
    ])

# =========================================================================
# 🎮 HANDLERS (AIOGRAM)
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class States(StatesGroup):
    PHONE=State(); CODE=State(); PASS=State(); PROMO=State()
    ADM_DAYS=State(); ADM_ACT=State(); BROADCAST=State()

TEMP_CLIENTS = {}

@router.message(Command("start"))
async def start(m: Message):
    kb = await get_main_kb(m.from_user.id)
    await m.answer(f"👋 <b>Добро пожаловать, {m.from_user.first_name}!</b>\n\nStatPro — профессиональный инструмент для анализа.", reply_markup=kb)

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
    
    status = "👑 GOD MODE" if c.from_user.id == ADMIN_ID else ("✅ ACTIVATED" if is_act else "❌ EXPIRED")
    date_str = "Навсегда" if c.from_user.id == ADMIN_ID else d.strftime('%d.%m.%Y')
    limit_info = f"⚡️ Лимит парсинга: <b>{u['parse_limit']}</b>\n" if is_act else ""
    
    text = (
        f"👤 <b>Личный кабинет</b>\n"
        f"🆔 ID: <code>{u['user_id']}</code>\n"
        f"💎 Статус: <b>{status}</b>\n"
        f"📅 Истекает: {date_str}\n"
        f"{limit_info}"
        f"⚙️ Версия: <code>{BOT_VERSION}</code>"
    )
    await edit_or_answer(c, text, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]))

@router.callback_query(F.data == "worker_menu")
async def w_menu(c: CallbackQuery):
    if not await has_active_sub(c.from_user.id):
        await c.answer("🚫 Доступ запрещен! Приобретите подписку.", show_alert=True)
        return

    u = await get_cached_user(c.from_user.id)
    
    # Безопасное отображение команд
    text = (
        f"⚡️ <b>StatPro Infinity</b>\n"
        f"📡 Статус: {WORKER_STATUS}\n"
        f"🎯 Лимит: <b>{u['parse_limit']}</b>\n\n"
        "🛡 <b>Администрирование:</b>\n"
        "<code>.ban</code>, <code>.kick</code>, <code>.mute &lt;m/h&gt;</code>\n"
        "<code>.promote</code>, <code>.demote</code>, <code>.zombies</code>\n\n"
        "🛠 <b>Утилиты:</b>\n"
        "<code>.afk &lt;text&gt;</code>, <code>.whois</code>, <code>.invite</code>\n"
        "<code>.calc &lt;math&gt;</code>, <code>.ping</code>\n\n"
        "⚔️ <b>Raid & Spam:</b>\n"
        "<code>.spam &lt;n&gt; &lt;text&gt;</code>, <code>.tagall</code>\n\n"
        "📂 <b>Парсинг (Hybrid):</b>\n"
        "<code>.чекгруппу</code>, <code>.csv</code>\n"
        "<i>(Бот пришлет результат с выбором формата)</i>"
    )
    await edit_or_answer(c, text, kb_config(u['parse_limit']))

@router.callback_query(F.data.startswith("lim:"))
async def set_lim(c: CallbackQuery):
    if not await has_active_sub(c.from_user.id): return
    l = int(c.data.split(":")[1])
    await set_limit(c.from_user.id, l)
    await c.answer(f"✅ Лимит установлен: {l}")
    await w_menu(c)

# --- HYBRID PARSE HANDLER ---
@router.callback_query(F.data.startswith("parse_res:"))
async def parse_res_handler(c: CallbackQuery):
    mode = c.data.split(":")[1]
    data = TEMP_PARSE_DATA.get(c.from_user.id)
    
    if not data: 
        return await c.answer("⚠️ Данные устарели или удалены. Повторите парсинг.", show_alert=True)
    
    lines = data['lines']
    title = data['title']
    
    if mode == "file":
        fn = f"Result_{title}.txt"
        with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(lines))
        await c.message.answer_document(FSInputFile(fn), caption=f"📂 <b>Результат парсинга:</b> {len(lines)} строк")
        os.remove(fn)
        
    elif mode == "text":
        chunk = ""
        await c.message.answer(f"📝 <b>Результат ({len(lines)}):</b>")
        for line in lines:
            if len(chunk) + len(line) > 3500:
                await c.message.answer(f"<code>{chunk}</code>")
                chunk = ""
                await asyncio.sleep(0.3)
            chunk += line + "\n"
        if chunk: await c.message.answer(f"<code>{chunk}</code>")
    
    await c.answer()

# --- OTHER HANDLERS (Auth, Sub, Admin) ---
# ... (Код авторизации и админки оптимизирован, но логика сохранена)

@router.callback_query(F.data == "sub_menu")
async def sub_menu(c: CallbackQuery):
    await edit_or_answer(c, "🎟 <b>Активация доступа:</b>", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Ввести промокод", callback_data="enter_promo")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ]))

@router.callback_query(F.data == "enter_promo")
async def en_pro(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "👉 <b>Введите ваш промокод:</b>")
    await state.set_state(States.PROMO)

@router.message(States.PROMO)
async def pro_h(m: Message, state: FSMContext):
    if await use_promo(m.from_user.id, m.text.strip()):
        kb = await get_main_kb(m.from_user.id)
        await m.answer("✅ <b>Код принят!</b> Доступ активирован.", reply_markup=kb)
    else:
        await m.answer("❌ <b>Ошибка:</b> Неверный или использованный код.")
    await state.clear()

@router.callback_query(F.data == "quick_actions")
async def qa(c: CallbackQuery):
    if not await has_active_sub(c.from_user.id): return
    await edit_or_answer(c, "🛠 <b>Быстрые действия:</b>", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧹 Очистить Кеш", callback_data="clear_cache")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ]))

@router.callback_query(F.data == "clear_cache")
async def clr_cache(c: CallbackQuery):
    global USER_CACHE
    USER_CACHE = {}
    await c.answer("✅ Кеш успешно очищен!", show_alert=True)

# --- ADMIN ---
@router.callback_query(F.data == "admin_menu")
async def adm_m(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    await edit_or_answer(c, f"👑 <b>Панель Администратора</b>\n📡 Worker: {WORKER_STATUS}", kb_admin())

@router.callback_query(F.data == "adm_stats")
async def adm_st(c: CallbackQuery):
    t, a = await get_stats()
    uptime = int(time.time() - START_TIME)
    await c.answer(f"👥 Всего: {t}\n🟢 Активно: {a}\n⏱ Аптайм: {uptime//3600}ч", show_alert=True)

@router.callback_query(F.data == "adm_backup")
async def adm_bk(c: CallbackQuery):
    await auto_backup()
    await c.message.answer_document(FSInputFile(DB_PATH), caption="📦 Database Backup")

@router.callback_query(F.data == "adm_restart_worker")
async def adm_rw(c: CallbackQuery):
    global WORKER_TASK
    if WORKER_TASK: WORKER_TASK.cancel()
    await asyncio.sleep(1)
    WORKER_TASK = asyncio.create_task(worker_process())
    await c.answer("🔄 Процесс перезапущен!", show_alert=True)
    await asyncio.sleep(2)
    await edit_or_answer(c, f"👑 <b>Панель Администратора</b>\n📡 Worker: {WORKER_STATUS}", kb_admin())

@router.callback_query(F.data == "adm_promo")
async def adm_pr(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "📅 <b>Срок действия (дней):</b>")
    await state.set_state(States.ADM_DAYS)

@router.message(States.ADM_DAYS)
async def adm_d(m: Message, s: FSMContext):
    await s.update_data(d=m.text)
    await m.answer("🔢 <b>Количество активаций:</b>")
    await s.set_state(States.ADM_ACT)

@router.message(States.ADM_ACT)
async def adm_a(m: Message, s: FSMContext):
    d = await s.get_data()
    c = await create_promo(int(d['d']), int(m.text))
    await m.answer(f"✅ Промокод создан:\n<code>{c}</code>", reply_markup=await get_main_kb(ADMIN_ID))
    await s.clear()

@router.callback_query(F.data == "adm_broadcast")
async def adm_br(c: CallbackQuery, s: FSMContext):
    await edit_or_answer(c, "📢 <b>Введите текст рассылки:</b>\n(или /cancel)")
    await s.set_state(States.BROADCAST)

@router.message(States.BROADCAST)
async def adm_br_h(m: Message, s: FSMContext):
    if m.text == "/cancel": await s.clear(); return await m.answer("Отмена.")
    users = await get_all_users()
    await m.answer(f"🚀 Запуск рассылки на {len(users)} пользователей...")
    count = 0
    for uid in users:
        try:
            await bot.send_message(uid, m.text)
            count += 1
            await asyncio.sleep(0.05)
        except: pass
    await m.answer(f"✅ Рассылка завершена. Доставлено: {count}")
    await s.clear()

# --- AUTH ---
@router.callback_query(F.data == "auth_menu")
async def am(c: CallbackQuery): 
    if c.from_user.id != ADMIN_ID: return
    await edit_or_answer(c, "🔐 <b>Выберите метод входа:</b>", kb_auth())

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
        msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption=f"📸 <b>Сканируйте QR!</b>\nСессия обновится автоматически.")
        await asyncio.wait_for(qr.wait(), AUTH_TIMEOUT)
        me = await cl.get_me()
        await msg.delete()
        kb = await get_main_kb(uid)
        await c.message.answer(f"✅ <b>Вход выполнен:</b> @{me.username or me.id}", reply_markup=kb)
        # Soft start
        if not WORKER_TASK or WORKER_TASK.done():
             asyncio.create_task(worker_process())
    except Exception as e:
        logger.error(f"Auth Error: {e}")
        await c.message.answer("❌ Ошибка входа или таймаут.")
    finally:
        if uid in TEMP_CLIENTS:
            try: await TEMP_CLIENTS[uid].disconnect()
            except: pass
            del TEMP_CLIENTS[uid]

@router.callback_query(F.data == "auth_phone")
async def ap(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "📱 <b>Введите номер телефона:</b>\n(Например: +79991234567)")
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
        await m.answer("📩 <b>Введите код из Telegram:</b>")
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
        await m.answer(f"✅ <b>Вход выполнен:</b> @{me.username or me.id}", reply_markup=kb)
        await s.clear()
        try: await cl.disconnect()
        except: pass
        del TEMP_CLIENTS[uid]
        if not WORKER_TASK or WORKER_TASK.done():
            asyncio.create_task(worker_process())
    except SessionPasswordNeededError:
        await m.answer("🔒 <b>Требуется пароль (2FA):</b>")
        await s.set_state(States.PASS)
    except Exception as e: await m.answer(f"❌ Ошибка: {e}")

@router.message(States.PASS)
async def pa(m: Message, s: FSMContext):
    uid = m.from_user.id
    cl = TEMP_CLIENTS.get(uid)
    try:
        await cl.sign_in(password=m.text)
        kb = await get_main_kb(uid)
        await m.answer("✅ <b>Вход выполнен!</b>", reply_markup=kb)
        if not WORKER_TASK or WORKER_TASK.done():
            asyncio.create_task(worker_process())
    except Exception as e: await m.answer(f"❌ Ошибка: {e}")
    finally:
        try: await cl.disconnect()
        except: pass
        if uid in TEMP_CLIENTS: del TEMP_CLIENTS[uid]
        await s.clear()

# =========================================================================
# 🧠 TELETHON WORKER (CORE)
# =========================================================================

async def worker_process():
    global WORKER_STATUS, SESSIONS_PARSED
    
    while True:
        client = None
        try:
            sess_path_base = get_session_path(ADMIN_ID)
            if not sess_path_base.with_suffix(".session").exists():
                WORKER_STATUS = "🔴 Сессия не найдена"
                await asyncio.sleep(10)
                continue

            WORKER_STATUS = "🟡 Подключение..."
            # Увеличенный таймаут соединения
            client = TelegramClient(str(sess_path_base), API_ID, API_HASH, connection_retries=None)

            # --- WORKER FUNCTIONS ---
            
            async def temp_msg(event, text, delay=0.5):
                """Стелс-отправка сообщений"""
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
                txt = f"🟢 <b>Online</b> | ⏱ {uptime}s | 📂 {SESSIONS_PARSED}"
                await temp_msg(ev, txt, 5)

            # HYBRID PARSING
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
                    # Save to RAM for Bot
                    TEMP_PARSE_DATA[ADMIN_ID] = {'lines': lines, 'title': str(ev.chat_id)}
                    
                    await msg.edit("✅ <b>Готово!</b> Проверьте ЛС бота.")
                    await asyncio.sleep(1)
                    await msg.delete()
                    
                    # Trigger Bot
                    try:
                        await bot.send_message(
                            ADMIN_ID, 
                            f"📁 <b>Парсинг завершен!</b>\nСобрано: {len(lines)} пользователей.\n\n👇 Выберите формат:",
                            reply_markup=kb_parse_choice()
                        )
                    except: pass
                except Exception as e: return await temp_msg(msg, f"Err: {e}", 3)

            # ALL UTILS
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
                    await temp_msg(msg, f"🧟 Kicked {cnt}", 5)
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
                    try: await client.send_file(ev.chat_id, fn, caption=f"CSV: {len(rows)}")
                    except BadRequestError: await msg.edit("❌ Топик закрыт!")
                    os.remove(fn)
                    await temp_msg(msg, "Uploaded", 0.5)
                except Exception as e: return await temp_msg(msg, f"Error: {e}", 3)

            @client.on(events.NewMessage(pattern=r'^\.calc (.+)'))
            async def calc_cmd(ev):
                try:
                    res = eval(ev.pattern_match.group(1), {"__builtins__": {}}, {"math": math})
                    await temp_msg(ev, f"🔢 {res}", 5)
                except: pass

            @client.on(events.NewMessage(pattern=r'^\.ping'))
            async def ping_cmd(ev):
                s = time.time()
                msg = await ev.reply("Pong")
                await temp_msg(msg, f"Ping: {int((time.time()-s)*1000)}ms", 1)

            # --- STARTUP ---
            await client.start()
            WORKER_STATUS = "🟢 Активен (24/7)"
            logger.info("Worker Started Successfully")
            await client.run_until_disconnected()

        # --- EXCEPTION HANDLING (SOFT RESTART) ---
        except (EOFError, ConnectionError) as e:
            WORKER_STATUS = "⚠️ Ошибка сети. Переподключение..."
            logger.warning(f"Network Error: {e}. Reconnecting in 5s...")
            if client: await client.disconnect()
            await asyncio.sleep(5)
            
        except sqlite3.OperationalError as e:
            WORKER_STATUS = "⏳ Ожидание базы данных..."
            logger.warning(f"DB Locked: {e}. Retrying in 5s...")
            if client: await client.disconnect()
            await asyncio.sleep(5)
            
        except Exception as e:
            WORKER_STATUS = f"🔴 Сбой: {e}"
            logger.error(f"Worker Crash: {e}")
            if client: await client.disconnect()
            await asyncio.sleep(5)

# =========================================================================
# 🚀 ЗАПУСК
# =========================================================================

async def main():
    global WORKER_TASK
    await init_db()
    await cleanup_files()
    
    dp.message.middleware(SecurityMiddleware())
    dp.callback_query.middleware(SecurityMiddleware())
    
    # Auto-start Worker
    WORKER_TASK = asyncio.create_task(worker_process())
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        if WORKER_TASK: WORKER_TASK.cancel()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
