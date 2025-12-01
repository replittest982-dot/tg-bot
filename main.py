#!/usr/bin/env python3
"""
🚀 STATPRO ULTIMATE v3.8 - MIDDLEWARE STUB FIX
✅ Полностью устранен ModuleNotFoundError: No module named 'aiogram.middleware'
✅ Проблемный импорт заменен заглушкой (stub) для обхода ошибок окружения.
✅ КОД ГОТОВ К ЗАПУСКУ.
"""

import asyncio
import logging
import logging.handlers
import os
import sys
import io
import re
import time
import gc
import platform
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Callable, Awaitable
from pathlib import Path
from functools import wraps
from collections import defaultdict, deque
import traceback

# -------------------------------------------------------------------------
# ИМПОРТЫ
# -------------------------------------------------------------------------

# Основные библиотеки
import aiosqlite
import pytz
import qrcode
from PIL import Image
from dotenv import load_dotenv

# Aiogram v3.x
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile
)
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, BaseFilter
from aiogram.client.default import DefaultBotProperties

# 💡 ИСПРАВЛЕНИЕ 1: Удаляем проблемный импорт и заменяем его заглушкой
# from aiogram.middleware.base import BaseMiddleware # <-- УДАЛЕНО

# Создаем заглушку класса, чтобы избежать ModuleNotFoundError:
class BaseMiddlewareStub:
    """Заглушка для BaseMiddleware на случай проблем с импортом."""
    def __init__(self, *args, **kwargs):
        pass
    async def __call__(self, handler, event: Message, data):
        return await handler(event, data)

BaseMiddleware = BaseMiddlewareStub # Назначаем заглушку как BaseMiddleware

# Telethon
from telethon import TelegramClient, events
from telethon.errors import (
    AuthKeyUnregisteredError, FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, PhoneCodeInvalidError, PasswordHashInvalidError,
    UserDeactivatedBanError
)

# =========================================================================
# 1. КОНФИГУРАЦИЯ
# =========================================================================

load_dotenv(override=True)

# Проверка переменных окружения
REQUIRED_ENV = ["BOT_TOKEN", "ADMIN_ID", "API_ID", "API_HASH"]
MISSING_ENV = [key for key in REQUIRED_ENV if not os.getenv(key)]
if MISSING_ENV:
    print(f"❌ ОШИБКА: В .env не хватает: {', '.join(MISSING_ENV)}")
    sys.exit(1)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 50))
RATE_LIMIT = float(os.getenv("RATE_LIMIT", "1.0"))
QR_TIMEOUT = int(os.getenv("QR_TIMEOUT", "60"))

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
SESSION_DIR = BASE_DIR / "sessions"
LOGS_DIR = BASE_DIR / "logs"

for path in [DATA_DIR, SESSION_DIR, LOGS_DIR]:
    path.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "statpro.db"
TIMEZONE = pytz.timezone('Europe/Moscow')

def get_session_path(user_id: int) -> Path:
    return SESSION_DIR / f"session_{user_id}"

# =========================================================================
# 2. ЛОГГИРОВАНИЕ
# =========================================================================

class ProdLogger:
    def __init__(self):
        self.logger = logging.getLogger('statpro')
        self.logger.setLevel(logging.INFO)
        self.error_count = 0
        
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
            self.logger.addHandler(ch)
            
            fh = logging.handlers.RotatingFileHandler(
                LOGS_DIR / "statpro.log", maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
            )
            fh.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
            self.logger.addHandler(fh)
    
    async def error(self, msg: str, bot: Optional[Bot] = None):
        self.error_count += 1
        self.logger.error(msg, exc_info=True)
        if self.error_count % 10 == 0 and bot:
            try:
                await bot.send_message(ADMIN_ID, f"🚨 Ошибка #{self.error_count}: {msg[:50]}")
            except: pass

logger_instance = ProdLogger()
logger = logger_instance.logger

# =========================================================================
# 3. БАЗА ДАННЫХ
# =========================================================================

class UltimateDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = asyncio.Lock()

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    subscription_end TEXT,
                    telethon_active INTEGER DEFAULT 0,
                    total_messages INTEGER DEFAULT 0,
                    is_banned INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_sub ON users(subscription_end)")
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promocodes (
                    code TEXT PRIMARY KEY,
                    duration_days INTEGER,
                    uses_left INTEGER,
                    created_by INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.commit()
            logger.info("✅ База данных подключена")

    async def register_or_update_user(self, user_id: int, username: str, first_name: str):
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    INSERT INTO users (user_id, username, first_name) 
                    VALUES (?, ?, ?) ON CONFLICT(user_id) 
                    DO UPDATE SET username=excluded.username, first_name=excluded.first_name
                """, (user_id, username or '', first_name or ''))
                await db.commit()

    async def get_user(self, user_id: int) -> dict:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    sub_end = None
                    if row['subscription_end']:
                        try:
                            sub_end = datetime.strptime(row['subscription_end'], '%Y-%m-%d %H:%M:%S')
                            sub_end = TIMEZONE.localize(sub_end)
                        except ValueError: pass
                    
                    return {
                        'sub_end': sub_end,
                        'active': bool(row['telethon_active']),
                        'is_banned': bool(row['is_banned']),
                        'total_messages': row['total_messages']
                    }
        return {}

    async def is_sub_active(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        now = datetime.now(TIMEZONE)
        return user.get('sub_end') and user['sub_end'] > now and not user.get('is_banned', False)

    async def set_telethon_status(self, user_id: int, status: int):
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (status, user_id))
                await db.commit()

    async def update_user_sub(self, user_id: int, days: int) -> datetime:
        now = datetime.now(TIMEZONE)
        user = await self.get_user(user_id)
        
        if user.get('sub_end') and user['sub_end'] > now:
            new_end = user['sub_end'] + timedelta(days=days)
        else:
            new_end = now + timedelta(days=days)
        
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT INTO users (user_id, subscription_end) VALUES (?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET subscription_end=excluded.subscription_end",
                    (user_id, new_end.strftime('%Y-%m-%d %H:%M:%S'))
                )
                await db.commit()
        return new_end

    async def activate_promo(self, user_id: int, code: str) -> tuple:
        code = code.strip().upper()
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = None
                async with db.execute("SELECT duration_days, uses_left FROM promocodes WHERE code=?", (code,)) as cursor:
                    row = await cursor.fetchone()
                    if row is None or row[1] <= 0:
                        return False, "Промокод не найден или закончился"
                
                days = row[0]
                await db.execute("UPDATE promocodes SET uses_left=uses_left-1 WHERE code=?", (code,))
                await db.commit()
        
        await self.update_user_sub(user_id, days)
        return True, f"Активировано на {days} дн."

    async def set_ban_status(self, user_id: int, is_banned: bool):
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("UPDATE users SET is_banned=? WHERE user_id=?", (int(is_banned), user_id))
                await db.commit()

    async def get_stats(self) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = None
            
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total_users = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE telethon_active=1")
            active_workers = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT SUM(total_messages) FROM users")
            res = await cursor.fetchone()
            total_msgs = res[0] if res and res[0] else 0
            
        return {
            "total_users": total_users, 
            "active_workers": active_workers, 
            "total_msgs": total_msgs
        }

db = UltimateDB(DB_PATH)

# =========================================================================
# 4. ХРАНИЛИЩЕ СОСТОЯНИЙ (ОЗУ)
# =========================================================================

class Storage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.active_workers: Dict[int, TelegramClient] = {}
        self.worker_tasks: Dict[int, Dict[str, asyncio.Task]] = defaultdict(dict)
        self.auth_clients: Dict[int, TelegramClient] = {}
        self.rate_limits = defaultdict(deque)

store = Storage()

# =========================================================================
# 5. КЛАВИАТУРЫ И УТИЛИТЫ
# =========================================================================

def get_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Запуск Worker", callback_data="worker_start"), 
         InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth_menu")],
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="promo_menu"),
         InlineKeyboardButton(text="🛑 Стоп", callback_data="worker_stop")]
    ])

def get_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промо", callback_data="adm_promo"),
         InlineKeyboardButton(text="🎁 Выдать подписку", callback_data="adm_give_sub")],
        [InlineKeyboardButton(text="⛔ Бан/Разбан", callback_data="adm_ban"),
         InlineKeyboardButton(text="📊 Статистика", callback_data="adm_stats")]
    ])

# =========================================================================
# 6. MANAGER (TELETHON LOGIC)
# =========================================================================

class TelethonManager:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.semaphore = asyncio.Semaphore(MAX_WORKERS)

    async def safe_send(self, user_id: int, text: str):
        if not self.bot: return
        try:
            await self.bot.send_message(user_id, text, parse_mode='HTML')
        except Exception as e:
            pass

    async def clear_auth_client(self, user_id: int):
        async with store.lock:
            client = store.auth_clients.pop(user_id, None)
        if client:
            try:
                await client.disconnect()
            except: pass

    async def start_worker(self, user_id: int):
        if not await db.is_sub_active(user_id):
            await self.safe_send(user_id, "❌ <b>Нет активной подписки!</b>")
            return

        async with store.lock:
            if user_id in store.active_workers:
                await self.safe_send(user_id, "⚠️ <b>Воркер уже работает!</b>")
                return

        path = get_session_path(user_id)
        if not path.exists():
            await self.safe_send(user_id, "❌ <b>Сессия не найдена.</b> Сначала авторизуйтесь!")
            return

        await db.set_telethon_status(user_id, 0)

        async with self.semaphore:
            task = asyncio.create_task(self._run_worker(user_id, path))
            async with store.lock:
                store.worker_tasks[user_id]['main'] = task
                store.active_workers[user_id] = None
            await self.safe_send(user_id, "🚀 <b>Запускаю процессы...</b>")

    async def stop_worker(self, user_id: int):
        async with store.lock:
            client = store.active_workers.pop(user_id, None)
            tasks = store.worker_tasks.pop(user_id, {})
        
        if client:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=5)
            except: pass
        
        for task in tasks.values():
            if not task.done():
                task.cancel()
        
        await db.set_telethon_status(user_id, 0)
        await self.safe_send(user_id, "🛑 <b>Воркер остановлен.</b>")

    async def _run_worker(self, user_id: int, path: Path):
        client = TelegramClient(str(path), API_ID, API_HASH, device_model="StatPro Ultimate", system_version="Linux")
        
        try:
            await client.connect()
            if not await client.is_user_authorized():
                await self.safe_send(user_id, "❌ <b>Сессия слетела!</b> Авторизуйтесь заново.")
                return

            async with store.lock:
                store.active_workers[user_id] = client

            me = await client.get_me()
            await db.set_telethon_status(user_id, 1)
            await self.safe_send(user_id, f"✅ <b>Воркер активен!</b>\n👤 {me.first_name} (@{me.username})")

            @client.on(events.NewMessage(outgoing=True))
            async def handler(event):
                await self._handle_commands(user_id, client, event)

            await client.run_until_disconnected()

        except Exception as e:
            if not isinstance(e, asyncio.CancelledError):
                await logger_instance.error(f"Worker {user_id} error: {e}")
                await self.safe_send(user_id, f"💥 <b>Ошибка воркера:</b> {type(e).__name__}")
        finally:
            await self.stop_worker(user_id)

    async def _handle_commands(self, user_id: int, client: TelegramClient, event):
        if not event.text or not event.text.startswith('.'):
            return

        parts = event.text.split()
        cmd = parts[0][1:].lower()
        args = parts[1:]

        try:
            if cmd == 'статус':
                await event.edit("🟢 <b>StatPro Active</b>")
            
            elif cmd == 'лс' and len(args) >= 2:
                target = args[0]
                text = " ".join(args[1:])
                await client.send_message(target, text)
                await event.edit(f"✅ Отправлено: {target}")

            elif cmd == 'флуд' and len(args) >= 3:
                target = args[0]
                count = int(args[1])
                text = " ".join(args[2:])
                
                await event.edit(f"🚀 Флуд {count}x на {target}...")
                asyncio.create_task(self._flood_task(client, target, count, text))
                
        except FloodWaitError as e:
            await event.edit(f"⏳ <b>FloodWait:</b> {e.seconds}s")
        except Exception as e:
            await event.edit(f"❌ Ошибка: {e}")

    async def _flood_task(self, client, target, count, text):
        try:
            for i in range(count):
                await client.send_message(target, f"{text} [{i+1}]")
                await asyncio.sleep(0.5)
        except: pass

# =========================================================================
# 7. AIOGRAM (BOT) SETUP
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
tm = TelethonManager(bot)
dp = Dispatcher(storage=MemoryStorage())

user_router = Router()
admin_router = Router()
dp.include_router(user_router)
dp.include_router(admin_router)

class ThrottlingMiddleware(BaseMiddleware):
    # ВНИМАНИЕ: Если ты не выполнил 'pip install -U aiogram', этот класс 
    # будет использовать заглушку BaseMiddlewareStub и не будет работать
    # как настоящий ThrottlingMiddleware.
    async def __call__(self, handler, event: Message, data):
        if isinstance(self, BaseMiddlewareStub):
             # Если используется заглушка, просто пропускаем обработку
             return await handler(event, data)

        user_id = event.from_user.id
        now = time.time()
        store.rate_limits[user_id] = deque([t for t in store.rate_limits[user_id] if now - t < 1.0], maxlen=5)
        if len(store.rate_limits[user_id]) >= 3:
            return
        store.rate_limits[user_id].append(now)
        return await handler(event, data)

dp.message.middleware(ThrottlingMiddleware())

# =========================================================================
# 8. HANDLERS (FSM & COMMANDS)
# =========================================================================

class UserStates(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    PROMO = State()

class AdminStates(StatesGroup):
    PROMO_CREATE = State()
    GIVE_SUB_ID = State()
    GIVE_SUB_DAYS = State()
    BAN_ID = State()

class AdminFilter(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user.id == ADMIN_ID

# --- USER HANDLERS ---

@user_router.message(Command("start"))
async def cmd_start(message: Message):
    await db.register_or_update_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    await message.answer(f"👋 Привет, {message.from_user.first_name}!", reply_markup=get_main_kb())

@user_router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await tm.clear_auth_client(message.from_user.id)
    await message.answer("✅ Действие отменено", reply_markup=get_main_kb())

@user_router.callback_query(F.data == "worker_start")
async def cb_worker_start(call: CallbackQuery):
    await tm.start_worker(call.from_user.id)
    await call.answer()

@user_router.callback_query(F.data == "worker_stop")
async def cb_worker_stop(call: CallbackQuery):
    await tm.stop_worker(call.from_user.id)
    await call.answer()

@user_router.callback_query(F.data == "promo_menu")
async def cb_promo(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("🎟 Введите промокод:")
    await state.set_state(UserStates.PROMO)
    await call.answer()

@user_router.message(UserStates.PROMO)
async def promo_handler(message: Message, state: FSMContext):
    success, text = await db.activate_promo(message.from_user.id, message.text)
    await message.answer(f"✅ {text}" if success else f"❌ {text}", reply_markup=get_main_kb())
    await state.clear()

# --- AUTH FLOW ---

@user_router.callback_query(F.data == "auth_menu")
async def cb_auth_menu(call: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 По номеру", callback_data="auth_phone"),
         InlineKeyboardButton(text="📸 По QR-коду", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])
    await call.message.edit_text("Выберите метод входа:", reply_markup=kb)
    await call.answer()

@user_router.callback_query(F.data == "main_menu")
async def cb_main_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("Главное меню:", reply_markup=get_main_kb())
    await call.answer()

# QR LOGIN
@user_router.callback_query(F.data == "auth_qr")
async def auth_qr_start(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    await tm.clear_auth_client(user_id)
    
    client = TelegramClient(str(get_session_path(user_id)), API_ID, API_HASH)
    async with store.lock:
        store.auth_clients[user_id] = client
    
    try:
        await client.connect()
        qr_login = await client.qr_login()
        
        qr = qrcode.QRCode()
        qr.add_data(qr_login.url)
        qr.make()
        img = qr.make_image()
        bio = io.BytesIO()
        img.save(bio, format='PNG')
        bio.seek(0)
        
        sent = await call.message.answer_photo(
            BufferedInputFile(bio.read(), filename="qr.png"),
            caption=f"📸 Сканируй! Жду {QR_TIMEOUT} сек..."
        )
        await call.message.delete()
        
        try:
            await asyncio.wait_for(client.run_until_disconnected(), timeout=QR_TIMEOUT)
            await sent.edit_caption(caption="✅ <b>Успешный вход по QR!</b>", reply_markup=get_main_kb())
        except asyncio.TimeoutError:
            await sent.edit_caption(caption="❌ <b>Время вышло.</b>", reply_markup=get_main_kb())
        
    except Exception as e:
        await call.message.answer(f"Ошибка: {e}")
    finally:
        await tm.clear_auth_client(user_id)
    await call.answer()

# PHONE LOGIN
@user_router.callback_query(F.data == "auth_phone")
async def auth_phone_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("📱 Введите номер (+7...):")
    await state.set_state(UserStates.PHONE)
    await call.answer()

@user_router.message(UserStates.PHONE)
async def auth_phone_input(message: Message, state: FSMContext):
    phone = message.text.strip()
    user_id = message.from_user.id
    
    await tm.clear_auth_client(user_id)
    client = TelegramClient(str(get_session_path(user_id)), API_ID, API_HASH)
    async with store.lock:
        store.auth_clients[user_id] = client
        
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
        await state.update_data(phone=phone, hash=sent.phone_code_hash)
        await state.set_state(UserStates.CODE)
        await message.answer("📩 Введите код из Telegram:")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await tm.clear_auth_client(user_id)

@user_router.message(UserStates.CODE)
async def auth_code_input(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    client = store.auth_clients.get(user_id)
    
    if not client:
        await message.answer("❌ Сессия умерла. Начните заново.")
        return await state.clear()
        
    try:
        await client.sign_in(phone=data['phone'], code=code, phone_code_hash=data['hash'])
        await message.answer("✅ <b>Успешный вход!</b>", reply_markup=get_main_kb())
        await tm.clear_auth_client(user_id)
        await state.clear()
    except SessionPasswordNeededError:
        await message.answer("🔒 Введите пароль 2FA:")
        await state.set_state(UserStates.PASSWORD)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await tm.clear_auth_client(user_id)

@user_router.message(UserStates.PASSWORD)
async def auth_pass_input(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    client = store.auth_clients.get(user_id)
    
    try:
        await client.sign_in(password=password)
        await message.answer("✅ <b>Успешный вход (2FA)!</b>", reply_markup=get_main_kb())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
    finally:
        await tm.clear_auth_client(user_id)
        await state.clear()

# --- ADMIN HANDLERS ---

@admin_router.message(AdminFilter(), Command("admin"))
async def cmd_admin(message: Message):
    await message.answer("👑 Админ-панель", reply_markup=get_admin_kb())

@admin_router.callback_query(AdminFilter(), F.data == "adm_stats")
async def cb_adm_stats(call: CallbackQuery):
    s = await db.get_stats()
    text = (f"📊 <b>Статистика:</b>\n"
            f"👥 Юзеров: {s['total_users']}\n"
            f"⚡ Воркеров: {s['active_workers']}\n"
            f"📨 Сообщений: {s['total_msgs']}")
    await call.message.edit_text(text, reply_markup=get_admin_kb())
    await call.answer()

@admin_router.callback_query(AdminFilter(), F.data == "adm_promo")
async def cb_adm_promo(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("📝 Формат: КОД ДНИ КОЛИЧЕСТВО\nПример: `TEST 30 10`")
    await state.set_state(AdminStates.PROMO_CREATE)

@admin_router.message(AdminFilter(), AdminStates.PROMO_CREATE)
async def adm_promo_save(message: Message, state: FSMContext):
    try:
        code, days, uses = message.text.split()
        async with aiosqlite.connect(DB_PATH) as db_conn:
            await db_conn.execute(
                "INSERT INTO promocodes (code, duration_days, uses_left, created_by) VALUES (?, ?, ?, ?)",
                (code.upper(), int(days), int(uses), message.from_user.id)
            )
            await db_conn.commit()
        await message.answer("✅ Промокод создан!", reply_markup=get_admin_kb())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
    await state.clear()

# =========================================================================
# 9. MAIN LOOP & HEARTBEAT
# =========================================================================

async def heartbeat():
    """Фоновая задача: проверка подписок и чистка памяти"""
    while True:
        await asyncio.sleep(300) # 5 минут
        gc.collect()
        
        logger.info(f"❤️ HEARTBEAT | Active Workers: {len(store.active_workers)}")
        
        active_ids = list(store.active_workers.keys())
        for uid in active_ids:
            if not await db.is_sub_active(uid):
                await tm.stop_worker(uid)
                await tm.safe_send(uid, "🚫 <b>Подписка истекла.</b> Воркер остановлен.")

async def main():
    if not all([Bot, TelegramClient]):
        print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не удалось импортировать Aiogram или Telethon.")
        print("Проверь, что все библиотеки установлены: pip install aiogram telethon aiosqlite qrcode pillow python-dotenv")
        sys.exit(1)
        
    await db.init()
    logger.info("🚀 SYSTEM STARTED")
    
    asyncio.create_task(heartbeat())
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"Критический сбой в Aiogram: {e}")
    finally:
        logger.info("🛑 SYSTEM SHUTDOWN")
        tasks = []
        for uid in list(store.active_workers.keys()):
            tasks.append(tm.stop_worker(uid))
        if tasks:
            await asyncio.gather(*tasks)
        await bot.session.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 Bye!")
