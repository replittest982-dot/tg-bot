#!/usr/bin/env python3
"""
🚀 StatPro Telegram Bot с FSM-авторизацией Telethon (телефон, код, 2FA)
Полный код с интеграцией FSM, менеджером воркеров, безопасностью и логированием.
"""

import asyncio
import logging
import logging.handlers
import os
import re
import sys
import html
import shutil
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Set, Union, Callable, Awaitable
from pathlib import Path
from functools import wraps

import aiosqlite
import pytz

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F, types, BaseMiddleware
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, TelegramObject
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from telethon import TelegramClient, events
from telethon.errors import (
    AuthKeyUnregisteredError, FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, FloodWaitError as TLFloodWaitError
)

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# =========================================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")

if not all([BOT_TOKEN, ADMIN_ID, API_ID, API_HASH]):
    print("❌ Критические переменные окружения отсутствуют.")
    sys.exit(1)

TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
DB_PATH = Path('data/bot_database.db')
SESSION_DIR = Path('sessions')
for d in [DB_PATH.parent, SESSION_DIR]:
    d.mkdir(exist_ok=True)

# --- Логирование ---

logger = logging.getLogger('statpro')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# --- FSM-состояния ---

class UserStates(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    PROMO_CODE = State()

class AdminStates(StatesGroup):
    WAITING_PROMO = State()

# --- Временное хранилище клиентов Telethon для FSM-авторизации ---

class AuthClients:
    def __init__(self):
        self.clients: Dict[int, TelegramClient] = {} # {user_id: TelethonClient}

auth_clients = AuthClients()

# --- Глобальные хранилища ---

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.active_workers: Dict[int, TelegramClient] = {} # {user_id: TelethonClient}
        self.worker_tasks: Dict[int, Dict[str, Any]] = {}   # {user_id: {"main": Task}}
        self.premium_users: Set[int] = set()

store = GlobalStorage()

# --- Инициализация бота и диспетчера ---

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())

user_router = Router()
admin_router = Router()

dp.include_routers(user_router, admin_router)

# =========================================================================
# II. БАЗА ДАННЫХ И УТИЛИТЫ
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    telethon_active BOOLEAN DEFAULT 0,
                    subscription_end TEXT,
                    is_banned BOOLEAN DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promocodes (
                    code TEXT PRIMARY KEY,
                    duration_days INTEGER,
                    uses_left INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.commit()
            logger.info("✅ Инициализация базы данных завершена")

    async def get_subscription_status(self, user_id: int) -> Optional[datetime]:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            await db.commit()
            async with db.execute("SELECT subscription_end FROM users WHERE user_id=?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                if row and row[0]:
                    try:
                        dt = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                        dt_aware = TIMEZONE_MSK.localize(dt)
                        if dt_aware > datetime.now(TIMEZONE_MSK):
                            return dt_aware
                    except ValueError:
                        pass
                return None

    async def set_telethon_status(self, user_id: int, active: bool):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if active else 0, user_id))
            await db.commit()
            logger.info(f"DB: Telethon status for {user_id} set to {active}")

    async def update_subscription(self, user_id: int, days: int) -> datetime:
        current_end = await self.get_subscription_status(user_id)
        now = datetime.now(TIMEZONE_MSK)
        # Если подписка не активна или истекла, отсчет идет с текущего момента
        new_end = (current_end + timedelta(days=days)) if current_end and current_end > now else now + timedelta(days=days)
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET subscription_end=? WHERE user_id=?",
                (new_end.strftime('%Y-%m-%d %H:%M:%S'), user_id)
            )
            await db.commit()
        return new_end

    # NOTE: Промокод не реализован, заглушка для совместимости
    async def use_promocode(self, code: str, user_id: int) -> bool:
        if code.upper() == "TEST30":
            await self.update_subscription(user_id, 30)
            return True
        return False

db = AsyncDatabase(DB_PATH)

# --- Утилиты ---

def get_session_path(user_id: int) -> Path:
    return SESSION_DIR / f"session_{user_id}"

# =========================================================================
# III. TELETHON MANAGER
# =========================================================================

class TelethonManager:
    def __init__(self, bot_instance: Bot):
        self.bot = bot_instance
        self.semaphore = asyncio.Semaphore(50) # Max 50 workers
        self.subscription_checker: Optional[asyncio.Task] = None

    async def send_to_user(self, user_id: int, message: str, admin_notify: bool = False):
        try:
            await self.bot.send_message(user_id, message, parse_mode='HTML')
        except (TelegramBadRequest, TelegramForbiddenError):
            logger.warning(f"Send failed -> {user_id}. Stopping worker.")
            await self.stop_worker(user_id)
        
        if admin_notify and user_id != ADMIN_ID:
            try:
                await self.bot.send_message(ADMIN_ID, f"🚨 USER {user_id}: {message}")
            except:
                pass

    async def start_worker(self, user_id: int):
        if user_id in store.active_workers:
            await self.send_to_user(user_id, "⚠️ Worker уже запущен.")
            return

        async with self.semaphore:
            await self.stop_worker(user_id)
            
            path = get_session_path(user_id)
            if not path.exists():
                await self.send_to_user(user_id, "❌ **Сессия не найдена.** Начните авторизацию.")
                return

            task = asyncio.create_task(self._run_worker(user_id))
            async with store.lock:
                store.worker_tasks.setdefault(user_id, {})["main"] = {"task": task}
                store.premium_users.add(user_id)
            
            await self.send_to_user(user_id, "🚀 **Worker запущен!**")

    async def stop_worker(self, user_id: Optional[int] = None):
        user_ids = [user_id] if user_id is not None else list(store.active_workers.keys())
        
        for uid in user_ids:
            client = store.active_workers.pop(uid, None)
            tasks_dict = store.worker_tasks.pop(uid, {})
            store.premium_users.discard(uid)
            
            if client:
                try:
                    await asyncio.wait_for(client.disconnect(), timeout=5.0)
                except Exception as e:
                    logger.warning(f"Worker {uid} client disconnect failed: {e}")
            
            main_task = tasks_dict.get("main", {}).get("task")
            if main_task and not main_task.done():
                main_task.cancel()
                try:
                    await asyncio.wait_for(main_task, timeout=5.0)
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(f"Worker {uid} task cancel failed: {e}")
            
            await db.set_telethon_status(uid, False)
            if user_id is not None:
                logger.info(f"Worker {uid} остановлен.")


    async def _handle_worker_command(self, user_id: int, client: TelegramClient, event):
        if not event.is_private:
            await event.delete()
            return
            
        cmd = event.text.strip().lower().split()[0]
        
        if cmd == '.статус':
            me = await client.get_me()
            await client.send_message(
                event.chat_id,
                f"⚙️ **Статус**\n👤 @{me.username or 'No username'}\n🟢 **Активен**",
                parse_mode='HTML'
            )
        elif cmd == '.стоп':
            await client.send_message(event.chat_id, "🛑 **Останавливаю...**", parse_mode='HTML')
            await self.stop_worker(user_id)
        else:
            await client.send_message(event.chat_id, "❓ **Неизвестная команда**", parse_mode='HTML')

    async def _run_worker(self, user_id: int):
        path = get_session_path(user_id)
        
        async with TelegramClient(
            str(path), API_ID, API_HASH,
            device_model="StatPro Worker"
        ) as client:
            
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    await self.send_to_user(user_id, "🔑 **Сессия недействительна!**", True)
                    return

                async with store.lock:
                    store.active_workers[user_id] = client

                me = await client.get_me()
                sub_end = await db.get_subscription_status(user_id)

                if not sub_end:
                    await self.send_to_user(user_id, "⚠️ **Подписка истекла!** Worker остановлен.", True)
                    return

                await db.set_telethon_status(user_id, True)
                await self.send_to_user(
                    user_id,
                    f"✅ **Worker активен!**\n👤 **@{me.username or 'Без username'}**\n📅 **{sub_end.strftime('%d.%m.%Y %H:%M')}**"
                )

                @client.on(events.NewMessage(outgoing=True))
                async def handler(event):
                    await asyncio.wait_for(self._handle_worker_command(user_id, client, event), timeout=30.0)

                await asyncio.Future() # Бесконечный цикл ожидания
                
            except TLFloodWaitError as e:
                await self.send_to_user(user_id, f"⏳ **FloodWait: {e.seconds}s**. Worker остановлен.", True)
            except AuthKeyUnregisteredError:
                path.unlink(missing_ok=True)
                await self.send_to_user(user_id, "🔑 **Сессия удалена!** Повторите авторизацию.", True)
            except SessionPasswordNeededError:
                await self.send_to_user(user_id, "🔐 **Требуется 2FA пароль.**", True)
            except asyncio.CancelledError:
                logger.info(f"Worker {user_id} task was cancelled.")
            except Exception as e:
                logger.error(f"Worker {user_id} fatal error: {e}", exc_info=True)
                await self.send_to_user(user_id, f"💥 **Критическая ошибка**: {type(e).__name__}", True)
            finally:
                await self.stop_worker(user_id)

tm = TelethonManager(bot)

# =========================================================================
# IV. ХЕНДЛЕРЫ
# =========================================================================

# --- Общие команды ---

@user_router.message(Command("start"))
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    sub_end = await db.get_subscription_status(user_id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Запустить Worker", callback_data="start_worker")],
        [InlineKeyboardButton(text="🔑 Подключить сессию", callback_data="auth")],
        [InlineKeyboardButton(text="🎁 Промокод", callback_data="promo")]
    ])
    
    status_text = f"📅 **Подписка до:** {sub_end.strftime('%d.%m.%Y %H:%M')}" if sub_end else "❌ **Подписка не активна**"
    
    text = f"""👋 **StatPro Bot**
ID: `{user_id}`
{status_text}
"""
    await message.answer(text, reply_markup=kb)

@user_router.message(Command("stop"))
async def stop_handler(message: types.Message):
    await tm.stop_worker(message.from_user.id)
    await message.answer("🛑 **Worker остановлен.**")

@user_router.callback_query(F.data == "start_worker")
async def start_worker_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not await db.get_subscription_status(user_id):
        await callback.answer("❌ Нет активной подписки!", show_alert=True)
        return
    
    await tm.start_worker(user_id)
    await callback.answer("🚀 Запуск Worker'а...")

# --- FSM Авторизация ---

@user_router.callback_query(F.data == "auth")
async def start_auth_cb(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📱 Введите ваш номер телефона в международном формате (например, `+79001234567`):"
    )
    await state.set_state(UserStates.PHONE)
    await callback.answer()

@user_router.message(UserStates.PHONE)
async def process_phone(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    phone = message.text.strip()
    
    if not re.match(r'^\+\d{10,15}$', phone):
        return await message.answer("❌ **Неверный формат.** Введите номер, начиная с +, без пробелов.")

    client = TelegramClient(str(get_session_path(user_id)), API_ID, API_HASH)
    auth_clients.clients[user_id] = client

    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        
        await state.update_data(phone=phone, sent_code=sent_code)
        await state.set_state(UserStates.CODE)
        await message.answer(f"✅ Код отправлен на номер `{phone}`. Введите его:")
    
    except PhoneNumberInvalidError:
        await message.answer("❌ **Неверный номер телефона.** Попробуйте снова.")
        await state.clear()
        client.disconnect()
        del auth_clients.clients[user_id]
    except Exception as e:
        logger.error(f"Auth error (phone) {user_id}: {e}")
        await message.answer(f"❌ Критическая ошибка: {type(e).__name__}.")
        await state.clear()
        client.disconnect()
        del auth_clients.clients[user_id]


@user_router.message(UserStates.CODE)
async def process_code(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    
    code = message.text.strip()
    client = auth_clients.clients.get(user_id)
    
    if not client:
        await state.clear()
        return await message.answer("❌ **Сессия авторизации истекла.** Начните сначала.")

    try:
        await client.sign_in(data['phone'], code, phone_code_hash=data['sent_code'].phone_code_hash)
        
        # Успешный вход
        await client.disconnect() 
        await message.answer("🎉 **Авторизация успешна!**\nТеперь нажмите 🚀 **Запустить Worker**.")
        await state.clear()
        del auth_clients.clients[user_id]
        
    except SessionPasswordNeededError:
        # Нужен 2FA пароль
        await state.set_state(UserStates.PASSWORD)
        await message.answer("🔐 **Требуется двухфакторная аутентификация.** Введите ваш облачный пароль:")
        
    except Exception as e:
        await message.answer("❌ **Неверный код** или ошибка: " + str(type(e).__name__) + ". Попробуйте снова.")
        # Остаемся в этом состоянии


@user_router.message(UserStates.PASSWORD)
async def process_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    password = message.text.strip()
    client = auth_clients.clients.get(user_id)
    
    if not client:
        await state.clear()
        return await message.answer("❌ **Сессия авторизации истекла.** Начните сначала.")
        
    try:
        await client.sign_in(password=password)
        
        # Успешный вход
        await client.disconnect() 
        await message.answer("🎉 **2FA подтверждена! Сессия сохранена.**\nТеперь нажмите 🚀 **Запустить Worker**.")
        await state.clear()
        del auth_clients.clients[user_id]
        
    except Exception as e:
        await message.answer("❌ **Неверный пароль** или ошибка: " + str(type(e).__name__) + ". Попробуйте снова.")
        # Остаемся в этом состоянии


# --- Промокод (заглушка) ---

@user_router.callback_query(F.data == "promo")
async def promo_cb(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🎁 **Введите промокод (например, `TEST30`):**")
    await state.set_state(UserStates.PROMO_CODE)
    await callback.answer()

@user_router.message(UserStates.PROMO_CODE)
async def process_promo(message: types.Message, state: FSMContext):
    code = message.text.strip()
    success = await db.use_promocode(code, message.from_user.id)
    
    if success:
        end_date = await db.get_subscription_status(message.from_user.id)
        await message.answer(f"✅ **Активировано!**\n📅 **До:** {end_date.strftime('%d.%m.%Y %H:%M')}")
        await tm.start_worker(message.from_user.id) # Запускаем после успешной активации
    else:
        await message.answer("❌ **Неверный промокод!**")
    await state.clear()


# =========================================================================
# V. ЗАПУСК И SHUTDOWN
# =========================================================================

async def main():
    await db.init()
    logger.info("🚀 StatPro Bot запущен.")
    
    try:
        await dp.start_polling(bot)
    finally:
        await tm.stop_worker() # Остановка всех воркеров при выключении
        logger.info("✅ Graceful shutdown завершен.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
