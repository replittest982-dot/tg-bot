import asyncio
import logging
import os
import re
import random
import string
import traceback
import sys
import aiosqlite
import pytz
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Union
from functools import wraps

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties 

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, AuthKeyUnregisteredError,
    UserIsBlockedError
)

# =========================================================================
# I. КОНФИГУРАЦИЯ (ХАРДКОД - BotHost.ru НЕ НУЖЕН!)
# =========================================================================

# ТВОИ ТОКЕНЫ (замени на свои если нужно)
BOT_TOKEN = "7868097991:AAEWx2koF8jM-gsNuLlvDpax-tfJUj6lhqw"
ADMIN_ID = 6256576302
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
SUPPORT_BOT_USERNAME = "suppor_tstatpro1bot"

DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow') 
RATE_LIMIT_TIME = 1.0
SESSION_DIR = 'sessions'

# Автоматическое создание папок
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs('data', exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Bot и Dispatcher
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())
user_router = Router()
drops_router = Router()

# =========================================================================
# II. ГЛОБАЛЬНОЕ ХРАНИЛИЩЕ (БЕЗ ИЗМЕНЕНИЙ)
# =========================================================================

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {}
        self.process_progress: Dict[int, Dict] = {}
        self.last_user_request: Dict[int, datetime] = {}
        self.pc_monitoring: Dict[int, str] = {}
        self.active_workers: Dict[int, TelegramClient] = {} 
        self.worker_tasks: Dict[int, List[asyncio.Task]] = {}

store = GlobalStorage()

class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()

class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_phone_change = State()

# =========================================================================
# III. УТИЛИТЫ (БЕЗ ИЗМЕНЕНИЙ)
# =========================================================================

def get_session_path(user_id: int, is_temp: bool = False) -> str:
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}.session')

def get_current_time_msk() -> datetime:
    return datetime.now(TIMEZONE_MSK)

def to_msk_aware(dt_str: str) -> datetime:
    naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    return TIMEZONE_MSK.localize(naive_dt)

def get_topic_name_from_message(message: types.Message) -> Optional[str]:
    return store.pc_monitoring.get(message.chat.id)

def rate_limit(limit: float):
    def decorator(func):
        @wraps(func)
        async def wrapper(message: types.Message, *args, **kwargs):
            user_id = message.from_user.id
            now = get_current_time_msk()
            async with store.lock:
                last = store.last_user_request.get(user_id)
                if last and (now - last).total_seconds() < limit:
                    return
                store.last_user_request[user_id] = now
            return await func(message, *args, **kwargs)
        return wrapper
    return decorator

# =========================================================================
# IV. БАЗА ДАННЫХ (БЕЗ ИЗМЕНЕНИЙ)
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA foreign_keys=ON;")
            await db.execute("""CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    subscription_active BOOLEAN DEFAULT 0,
                    subscription_end_date TEXT,
                    telethon_active BOOLEAN DEFAULT 0
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS drop_sessions (
                    phone TEXT PRIMARY KEY,
                    pc_name TEXT,
                    drop_id INTEGER,
                    status TEXT,
                    start_time TEXT,
                    last_status_time TEXT,
                    prosto_seconds INTEGER DEFAULT 0
            )""")
            await db.commit()

    async def get_user(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            await db.commit()
            async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def check_subscription(self, user_id: int) -> bool:
        if user_id == ADMIN_ID: return True
        user = await self.get_user(user_id)
        if not user or not user.get('subscription_active'): return False
        end_date_str = user['subscription_end_date']
        if not end_date_str: return False
        try:
            end = to_msk_aware(end_date_str)
            return end > get_current_time_msk()
        except:
            return False

    async def set_telethon_status(self, user_id: int, status: bool):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            await db.commit()

    async def get_active_telethon_users(self) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def get_drop_session(self, phone: str):
        async with aiosqlite.connect(self.db_path) as
