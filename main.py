import asyncio
import logging
import os
import re
import random
import traceback
import sys
import aiosqlite
import pytz
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from functools import wraps

from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties 

from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, AuthKeyUnregisteredError,
    UserIsBlockedError
)

BOT_TOKEN = "7868097991:AAEWx2koF8jM-gsNuLlvDpax-tfJUj6lhqw"
ADMIN_ID = 6256576302
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
SUPPORT_BOT_USERNAME = "suppor_tstatpro1bot"

DB_NAME = 'bot_database.db'
SESSION_DIR = 'sessions'
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs('data', exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())
user_router = Router()
drops_router = Router()

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

def get_session_path(user_id: int, is_temp: bool = False) -> str:
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def get_current_time_msk() -> datetime:
    return datetime.now(pytz.timezone('Europe/Moscow'))

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

class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = os.path.join('data', db_path)

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:  # ✅ ФИКС!
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("""CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
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
        async with aiosqlite.connect(self.db_path) as db:  # ✅ ФИКС!
            db.row_factory = aiosqlite.Row
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            await db.commit()
            async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def set_telethon_status(self, user_id: int, status: bool):
        async with aiosqlite.connect(self.db_path) as db:  # ✅ ФИКС!
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            await db.commit()

    async def get_active_telethon_users(self) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:  # ✅ ФИКС!
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def get_drop_session(self, phone: str):
        async with aiosqlite.connect(self.db_path) as db:  # ✅ ФИКС!
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions WHERE phone=?", (phone,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_drop_session_by_drop_id(self, drop_id: int):
        async with aiosqlite.connect(self.db_path) as db:  # ✅ ФИКС!
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM drop_sessions WHERE drop_id=? AND status NOT IN ('closed', 'deleted', 'замена_закрыт') ORDER BY start_time DESC LIMIT 1", 
                (drop_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def create_drop_session(self, phone: str, pc_name: str, drop_id: int, status: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:  # ✅ ФИКС!
            now_str = get_current_time_msk().strftime('%Y-%m-%d %H:%M:%S')
            try:
                await db.execute(
                    "INSERT INTO drop_sessions (phone, pc_name, drop_id, status, start_time, last_status_time) VALUES (?, ?, ?, ?, ?, ?)",
                    (phone, pc_name, drop_id, status, now_str, now_str)
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def update_drop_status(self, phone: str, new_status: str, new_phone: Optional[str] = None):
        async with aiosqlite.connect(self.db_path) as db:  # ✅ ФИКС!
            now_str = get_current_time_msk().strftime('%Y-%m-%d %H:%M:%S')
            await db.execute(
                "UPDATE drop_sessions SET status=?, last_status_time=? WHERE phone=?",
                (new_status, now_str, phone)
            )
            await db.commit()
        return True

db = AsyncDatabase(DB_NAME)

@user_router.message(Command('start'))
@rate_limit(1.0)
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await db.get_user(message.from_user.id) 
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Вход (Номер)", callback_data="auth_phone")],
        [InlineKeyboardButton(text="❓ Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}")]
    ])
    await message.answer("👋 Добро пожаловать в STATPRO Worker!", reply_markup=kb)

dp.include_router(user_router)
dp.include_router(drops_router)

async def main():
    await db.init()
    await bot.delete_webhook(drop_pending_updates=True) 
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
