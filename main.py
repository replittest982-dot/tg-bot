import asyncio
import logging
import os
import re
import random
import string
import sys
import traceback
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Union, Any, Callable, Awaitable
from functools import wraps
from dotenv import load_dotenv

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage 
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, Update 
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter, TelegramConflictError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.dispatcher.middlewares.base import BaseMiddleware 
from aiogram.types.error import ErrorEvent

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, 
    AuthKeyUnregisteredError, UserIsBlockedError, PasswordHashInvalidError, 
    RpcCallFailError, SessionRevokedError, UserDeactivatedBanError
)

# --- OTHER ---
import aiosqlite
import pytz 
from contextlib import suppress 

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# =========================================================================

# --- КЛЮЧИ И КОНСТАНТЫ ---
BOT_TOKEN = "7868097991:AAG48aFRhSd6dDB87I6AkrYD_mzLJgclNVk" # ✅ УСТАНОВЛЕН ПОЛНЫЙ ТОКЕН
ADMIN_ID = 6256576302 # ✅ УСТАНОВЛЕН ADMIN ID
API_ID = 29930612 # ✅ УСТАНОВЛЕН API ID
API_HASH = "2690aa8c364b91e47b6da1f90a71f825" # ✅ УСТАНОВЛЕН API HASH
DROPS_CHAT_ID = -1009876543210 

# Прочие настройки
SUPPORT_BOT_USERNAME = "YourSupportBotUsername"
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 0.5 
SESSION_DIR = 'sessions'

if not os.path.exists(SESSION_DIR): os.makedirs(SESSION_DIR)
if not os.path.exists('data'): os.makedirs('data')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

storage = MemoryStorage() 
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='Markdown'))
dp = Dispatcher(storage=storage)
user_router = Router()
drops_router = Router()
admin_router = Router()

# =========================================================================
# II. ГЛОБАЛЬНЫЙ ERROR HANDLER
# =========================================================================

@dp.error()
async def global_error_handler(event: ErrorEvent):
    """Глобальный обработчик ВСЕХ ошибок (aiogram v3) с фильтрацией."""
    exception = event.exception
    
    with suppress():
        if isinstance(exception, TelegramBadRequest) and (
            "message is not modified" in str(exception).lower() or 
            "can't parse entities" in str(exception).lower()
        ):
            return True 
            
        if isinstance(exception, TelegramRetryAfter):
            logger.warning(f"FloodWait encountered. Sleeping for {exception.timeout}s.")
            await asyncio.sleep(exception.timeout)
            return True
            
        if isinstance(exception, TelegramConflictError):
            logger.critical("🚨 TelegramConflictError: Another bot instance is running!")
            return True

    logger.critical(f"🚨 КРИТИЧЕСКАЯ ОШИБКА: {exception.__class__.__name__}: {exception}", exc_info=True)
    
    if ADMIN_ID:
        error_msg = (
            f"🔥 **BOT CRASH** 🔥\n"
            f"❌ Тип: `{exception.__class__.__name__}`\n"
            f"📄 Ошибка: `{str(exception)[:100]}`\n" 
            f"📍 Трейсбек:\n`{traceback.format_exc()[:1500]}`"
        )
        try:
            await bot.send_message(ADMIN_ID, error_msg, parse_mode='Markdown')
        except TelegramForbiddenError:
            logger.error("Не могу отправить админу - заблокирован")
        except Exception:
            pass
            
    return True

# =========================================================================
# III. ГЛОБАЛЬНЫЕ ХРАНИЛИЩА И FSM СОСТОЯНИЯ
# =========================================================================

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {} 
        self.process_progress: Dict[int, Dict] = {}
        self.pc_monitoring: Dict[Union[int, str], str] = {}
        self.active_workers: Dict[int, TelegramClient] = {} 
        self.worker_tasks: Dict[int, List[asyncio.Task]] = {} 
        self.last_user_request: Dict[int, datetime] = {}

store = GlobalStorage()

# --- FSM States ---
class TelethonAuth(StatesGroup):
    PHONE = State() 
    CODE = State()  
    PASSWORD = State() 
    QR_WAIT = State()  
    
class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_phone_change = State()

class PromoStates(StatesGroup):
    waiting_for_code = State()

class AdminStates(StatesGroup):
    waiting_for_promo_length = State()
    waiting_for_promo_days = State()
    waiting_for_promo_uses = State()
    waiting_for_user_id_for_sub = State() 
    waiting_for_sub_days = State()
    
# =========================================================================
# IV. ASYNC DATABASE 
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        self.TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

    def get_current_time_msk(self) -> datetime:
        return datetime.now(self.TIMEZONE_MSK)

    def to_msk_aware(self, dt_str: str) -> datetime:
        if not dt_str: return datetime.fromtimestamp(0, self.TIMEZONE_MSK) 
        try:
            naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
            return self.TIMEZONE_MSK.localize(naive_dt)
        except ValueError:
            return datetime.fromtimestamp(0, self.TIMEZONE_MSK)
        
    def _calculate_new_end_date(self, current_end_date_str: Optional[str], days_to_add: int) -> str:
        now = self.get_current_time_msk()
        start_date = now
        
        if current_end_date_str:
            try:
                current_end = self.to_msk_aware(current_end_date_str)
                if current_end > now:
                    start_date = current_end
            except:
                pass 

        new_end_date = start_date + timedelta(days=days_to_add)
        return new_end_date.strftime('%Y-%m-%d %H:%M:%S')

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
            await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
                        code TEXT PRIMARY KEY,
                        days INTEGER NOT NULL,
                        uses_left INTEGER NOT NULL,
                        created_at TEXT NOT NULL
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
        logger.info("Database initialized successfully.")

    async def get_user(self, user_id):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            await db.commit()
            async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def check_subscription(self, user_id):
        global ADMIN_ID
        
        if user_id == ADMIN_ID: return True
        user = await self.get_user(user_id)
        if not user or not user.get('subscription_active') or not user.get('subscription_end_date'): 
            return False

        try:
            end = self.to_msk_aware(user['subscription_end_date'])
            now = self.get_current_time_msk()
            
            if end > now:
                return True
            else:
                # Автоматическое отключение просроченной подписки/воркера
                await self.set_telethon_status(user_id, False)
                await self.set_subscription_status(user_id, False, user['subscription_end_date'])
                
                # ✅ П.5: Убрана проверка if 'tm' in globals()
                if 'tm' in globals(): 
                     await tm.stop_worker(user_id)
                return False
        except Exception as e:
            logger.error(f"Subscription check error for {user_id}: {e}")
            return False

    async def set_telethon_status(self, user_id, status):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            await db.commit()
            
    async def set_subscription_status(self, user_id, status, end_date_str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET subscription_active=?, subscription_end_date=? WHERE user_id=?", (1 if status else 0, end_date_str, user_id))
            await db.commit()

    async def activate_promo_code(self, user_id: int, code: str) -> Optional[int]:
        promo = await self.get_promo_code(code)
        if not promo or (promo['uses_left'] is not None and promo['uses_left'] == 0):
            return None

        user = await self.get_user(user_id)
        days = promo['days']
        
        new_end_date_str = self._calculate_new_end_date(user.get('subscription_end_date'), days)

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET subscription_active=1, subscription_end_date=? WHERE user_id=?",
                (new_end_date_str, user_id)
            )
            
            if promo['uses_left'] != -1: 
                 await db.execute(
                     "UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code=?",
                     (code.upper(),)
                 )
            await db.commit()
        
        return days 

    async def get_promo_code(self, code: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM promo_codes WHERE code=?", (code.upper(),)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_active_telethon_users(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
                
    async def create_promo_code(self, code: str, days: int, uses: int):
        async with aiosqlite.connect(self.db_path) as db:
            now_str = self.get_current_time_msk().strftime('%Y-%m-%d %H:%M:%S')
            uses_value = uses if uses != 0 else -1 
            try:
                await db.execute(
                    "INSERT INTO promo_codes (code, days, uses_left, created_at) VALUES (?, ?, ?, ?)",
                    (code.upper(), days, uses_value, now_str)
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False 

    async def get_all_promo_codes(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM promo_codes ORDER BY created_at DESC") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def delete_promo_code(self, code: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM promo_codes WHERE code=?", (code.upper(),))
            await db.commit()
            return db.total_changes > 0

    async def get_all_users_count(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(user_id) FROM users") as cursor:
                return (await cursor.fetchone())[0]

    async def get_active_subs_count(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(user_id) FROM users WHERE subscription_active=1") as cursor:
                return (await cursor.fetchone())[0]

    async def get_active_drops_count(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(phone) FROM drop_sessions WHERE status NOT IN ('closed', 'deleted', 'замена_закрыт')") as cursor:
                return (await cursor.fetchone())[0]

    async def get_all_drops(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions ORDER BY start_time DESC") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def update_drop_status(self, phone: str, status: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE drop_sessions SET status=? WHERE phone=?", (status, phone))
            await db.commit()
            
    async def delete_drop_session(self, phone: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM drop_sessions WHERE phone=?", (phone,))
            await db.commit()
            return db.total_changes > 0

db = AsyncDatabase(os.path.join('data', DB_NAME))


# =========================================================================
# V. RATE LIMIT MIDDLEWARE
# =========================================================================

def get_user_id_from_update(update: Update) -> Optional[int]:
    """Извлекает ID пользователя из любого типа Update."""
    if update.message:
        return update.message.from_user.id
    if update.callback_query:
        return update.callback_query.from_user.id
    if update.from_user:
        return update.from_user.id
    return None

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, limit: float = RATE_LIMIT_TIME):
        self.limit = limit
        self.lock = asyncio.Lock()
        super().__init__()

    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any]
    ) -> Any:
        
        user_id = get_user_id_from_update(event)

        if not user_id:
            return await handler(event, data)

        now = datetime.now(TIMEZONE_MSK)
        
        async with self.lock:
            last = store.last_user_request.get(user_id)
            
            if last and (now - last).total_seconds() < self.limit:
                return 
                
            store.last_user_request[user_id] = now
        
        return await handler(event, data)

# =========================================================================
# VI. TELETHON MANAGER, UTILS & KEYBOARDS 
# =========================================================================

# --- UTILS ---
def generate_promo_code(length=8):
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

# --- KEYBOARDS ---
def get_main_menu_keyboard(user_id: int, is_subscribed: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="👤 Профиль", callback_data="profile_menu"))
    if is_subscribed:
        builder.row(InlineKeyboardButton(text="⚙️ Управление Worker", callback_data="worker_menu"))
    if not is_subscribed:
        builder.row(InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="enter_promo"))
    builder.row(InlineKeyboardButton(text="❓ Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}"))
    if user_id == ADMIN_ID:
        builder.row(InlineKeyboardButton(text="📊 Админ-панель", callback_data="admin_stats"))
    builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="start_menu")) 
    return builder.as_markup()

def get_worker_menu_keyboard(is_worker_active: bool, session_exists: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if is_worker_active:
        builder.row(InlineKeyboardButton(text="🛑 Остановить Worker", callback_data="stop_worker"))
    elif session_exists: 
        builder.row(InlineKeyboardButton(text="▶️ Запустить Worker", callback_data="start_worker"))
        
    if not is_worker_active: 
        builder.row(InlineKeyboardButton(text="🚪 Новый вход/Авторизация", callback_data="auth_method_menu"))
        
    builder.row(InlineKeyboardButton(text="🔙 Профиль", callback_data="profile_menu"))
    return builder.as_markup()


# --- TELETHON MANAGER ---
class TelethonManager:
    def __init__(self, bot_instance: Bot):
        self.bot = bot_instance
    
    async def _send_to_bot_user(self, user_id, message):
        try:
            await self.bot.send_message(user_id, message, disable_notification=False)
        except (TelegramForbiddenError, TelegramBadRequest, UserIsBlockedError):
            logger.error(f"Cannot send message to {user_id}. Stopping worker.")
            await self.stop_worker(user_id)
        except Exception as e:
            logger.error(f"Unknown error sending message to {user_id}: {e}")

    # ✅ П.4 и П.6: ИСПРАВЛЕНО: _finalize_auth теперь работает с Message, отправляет "Готово" и вызывает worker_menu через фиктивный call
    async def _finalize_auth(self, user_id: int, original_message: Message, state: FSMContext, user_info: Union[User, Channel, Chat]):
        """Общая логика завершения авторизации: сохранение сессии, запуск worker, очистка FSM."""
        
        temp_path = os.path.join(SESSION_DIR, f'temp_{user_id}.session')
        final_path = os.path.join(SESSION_DIR, f'session_{user_id}.session')
        
        if await asyncio.to_thread(os.path.exists, temp_path):
            await asyncio.to_thread(os.rename, temp_path, final_path)
        
        if client := store.temp_auth_clients.pop(user_id, None):
            if client.is_connected(): 
                await client.disconnect()

        await state.clear()
        
        name = getattr(user_info, 'first_name', 'Аккаунт')
        
        # 1. Отправляем финальное сообщение об успехе. 
        await original_message.answer(f"🎉 **Успешная авторизация!** Аккаунт **{name}** привязан.")
        
        await self.start_client_task(user_id)
        
        # 2. Создаем "фиктивный" CallbackQuery для вызова worker_menu. 
        # Отправляем новое Message для редактирования
        fake_call = types.CallbackQuery( 
            id='fake_finalize', 
            from_user=types.User(id=user_id, is_bot=False, first_name="User"), 
            message=await original_message.answer("🔄 Переход к управлению Worker...") 
        )
        # Вызываем worker_menu с фиктивным call
        await worker_menu(fake_call, state) 

    async def start_client_task(self, user_id):
        if not await db.check_subscription(user_id):
            await self._send_to_bot_user(user_id, "⚠️ **Ваша подписка истекла.** Worker не запущен.")
            return

        # ✅ П.14: Проверка существования файла сессии перед запуском
        session_exists = await asyncio.to_thread(os.path.exists, os.path.join(SESSION_DIR, f'session_{user_id}.session'))
        if not session_exists:
            await self._send_to_bot_user(user_id, "⚠️ **Сессия не найдена.** Проведите авторизацию.")
            return

        await self.stop_worker(user_id)
        
        task = asyncio.create_task(self._run_worker(user_id))
        
        async with store.lock:
            store.worker_tasks.pop(user_id, None)
            store.worker_tasks.setdefault(user_id, []).append(task)
            
        return task

    async def _run_worker(self, user_id):
        path = os.path.join(SESSION_DIR, f'session_{user_id}')
        client = TelegramClient(path, API_ID, API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        async with store.lock:
            store.active_workers[user_id] = client

        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            await self.worker_message_handler(user_id, client, event)

        try:
            await client.start(phone=None) 
            me = await client.get_me() 
            await db.set_telethon_status(user_id, True)
            await self._send_to_bot_user(user_id, f"🚀 Worker успешно запущен и готов к работе! Аккаунт: **{me.first_name or 'Нет имени'}**.")
            await client.run_until_disconnected()

        except (AuthKeyUnregisteredError, SessionPasswordNeededError, PhoneNumberInvalidError, EOFError, SessionRevokedError, UserDeactivatedBanError):
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Требуется повторная авторизация через меню **Профиль -> Управление Worker**.")
            
            session_file = os.path.join(SESSION_DIR, f'session_{user_id}.session')
            if await asyncio.to_thread(os.path.exists, session_file):
                try: await asyncio.to_thread(os.remove, session_file)
                except Exception as e: logger.warning(f"Failed to remove session file {session_file}: {e}")
                    
            await self.stop_worker(user_id)
        except Exception as e:
            logger.critical(f"Worker {user_id} failed: {e}", exc_info=True)
            await self._send_to_bot_user(user_id, f"💔 Worker отключился из-за критической ошибки: `{e.__class__.__name__}`.")
        finally:
            await self.stop_worker(user_id, silent=True) 
            await db.set_telethon_status(user_id, False)

    async def stop_worker(self, user_id, silent=False):
        """Останавливает клиент Telethon и отменяет все связанные задачи."""
        async with store.lock:
            client = store.active_workers.pop(user_id, None)
            
            tasks = store.worker_tasks.pop(user_id, [])
            for t in tasks:
                if not t.done(): t.cancel()

        if client:
            try:
                if client.is_connected():
                    await client.disconnect()
                if not silent: await self._send_to_bot_user(user_id, "🛑 Worker успешно остановлен.")
            except Exception as e:
                logger.warning(f"Error disconnecting client {user_id}: {e}")

        await db.set_telethon_status(user_id, False)

    async def worker_message_handler(self, user_id, client, event):
        if not event.text or not event.text.startswith('.'): return

        msg = event.text.strip()
        parts = msg.split()
        cmd = parts[0].lower()
        chat = event.chat_id

        try: await event.delete() 
        except: pass 

        if cmd == '.флуд':
            try:
                if len(parts) < 3: 
                    return await client.send_message(chat, "⚠️ Используйте: `.флуд [кол-во] [текст] [задержка]`", reply_to=event.message.id)
                
                count = int(parts[1])
                delay_str = parts[-1]
                if delay_str.replace('.', '', 1).isdigit():
                    delay = max(0.5, float(delay_str)) 
                    text = " ".join(parts[2:-1])
                else:
                    delay = 0.5
                    text = " ".join(parts[2:])
                
                if count > 1000 and count != 0: 
                    return await client.send_message(chat, "❌ Максимальное количество сообщений 1000.")

                async with store.lock:
                    if store.process_progress.get(user_id, {}).get('type') == 'flood':
                        return await client.send_message(chat, "⚠️ Уже запущен активный флуд. Сначала остановите: `.стопфлуд`")

                    store.process_progress[user_id] = {'type': 'flood', 'stop': False}
                
                task = asyncio.create_task(self._flood_task(client, chat, text, count, delay, user_id))
                async with store.lock:
                    store.worker_tasks.setdefault(user_id, []).append(task)

                temp = await client.send_message(chat, f"🚀 Флуд запущен: {count} сообщений, задержка {delay}с. Для остановки: `.стопфлуд`")
                await asyncio.sleep(2)
                await temp.delete()
            except Exception as e:
                await client.send_message(chat, f"❌ Ошибка флуда: `{e.__class__.__name__}`")
        
        elif cmd == '.стопфлуд':
            async with store.lock:
                if store.process_progress.get(user_id, {}).get('type') == 'flood':
                    store.process_progress[user_id]['stop'] = True
                    temp = await client.send_message(chat, "🛑 Флуд остановлен.")
                    await asyncio.sleep(2)
                    await temp.delete()
                else:
                    temp = await client.send_message(chat, "⚠️ Активный флуд не найден.")
                    await asyncio.sleep(2)
                    await temp.delete()

    async def _flood_task(self, client, chat, text, count, delay, user_id):
        """Задача флуда с обработкой FloodWait и сигнала стоп."""
        i = 0
        
        # ✅ П.11: Добавлен явный лимит на "бесконечный" флуд
        max_limit = 5000 if count == 0 else count

        while i < max_limit: 
            async with store.lock: 
                if store.process_progress.get(user_id, {}).get('stop'): break
            
            try:
                await client.send_message(chat, text)
                i += 1
                await asyncio.sleep(delay)
            except FloodWaitError as e:
                logger.warning(f"FloodWait on {user_id}: {e.seconds}s. Sleeping...")
                await asyncio.sleep(e.seconds + random.randint(1, 5)) 
            except Exception:
                break
        
        async with store.lock:
            store.process_progress.pop(user_id, None)

# ✅ П.7: Инициализация tm после определения класса TelethonManager
tm = TelethonManager(bot) 

# =========================================================================
# VII. USER HANDLERS 
# =========================================================================

# --- Главное меню (/start) ---
@user_router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    await db.get_user(user_id) 
    
    is_subscribed = await db.check_subscription(user_id)
    text = "👋 Добро пожаловать в систему STATPRO. Используйте меню для навигации."

    await message.answer(text, reply_markup=get_main_menu_keyboard(user_id, is_subscribed))

# --- Вкладка "Профиль" ---
@user_router.callback_query(F.data.in_({"profile_menu", "start_menu"}))
async def profile_menu(call: types.CallbackQuery, state: FSMContext):
    
    user_id = call.from_user.id
    message_to_edit = call.message
    await call.answer()
    
    # Отмена FSM состояния при переходе в меню
    await state.clear()
    
    if call.data == "start_menu":
        is_subscribed = await db.check_subscription(user_id)
        await message_to_edit.edit_text("👋 Добро пожаловать в систему STATPRO. Используйте меню для навигации.", 
                                        reply_markup=get_main_menu_keyboard(user_id, is_subscribed))
        return

    user_data = await db.get_user(user_id)
    is_subscribed = await db.check_subscription(user_id)
    is_worker_active = user_data.get('telethon_active', False)
    
    end_date_str = user_data.get('subscription_end_date')
    end_date_info = db.to_msk_aware(end_date_str).strftime('%d.%m.%Y %H:%M MSK') if is_subscribed and end_date_str else "Не активна"
    
    text = (
        f"👤 **Ваш Профиль**\n\n"
        f"🔹 **Ваш Telegram ID:** `{user_id}`\n"
        f"✅ **Подписка активна:** {'Да' if is_subscribed else 'Нет'}\n"
        f"🗓️ **Действует до:** `{end_date_info}`\n"
        f"🚀 **Worker активен:** {'Да' if is_worker_active else 'Нет'}"
    )
    
    builder = InlineKeyboardBuilder()
    if is_subscribed:
          builder.row(InlineKeyboardButton(text="⚙️ Управление Worker", callback_data="worker_menu"))
    else:
        builder.row(InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="enter_promo"))
        
    builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="start_menu"))
    
    await message_to_edit.edit_text(text, reply_markup=builder.as_markup())


# --- Управление Worker ---
@user_router.callback_query(F.data == "worker_menu")
# ✅ П.9: Используем оригинальный call
async def worker_menu(call: types.CallbackQuery, state: FSMContext): 
    user_id = call.from_user.id
    message_to_edit = call.message 
    await call.answer()
    
    await state.clear()

    if not await db.check_subscription(user_id):
        await call.answer("❌ Ваша подписка истекла.", show_alert=True)
        # ✅ П.9: Используем оригинальный call
        return await profile_menu(call, state) 

    user_data = await db.get_user(user_id)
    is_worker_active = user_data.get('telethon_active', False)
    # ✅ П.14: Проверка существования сессии
    session_exists = await asyncio.to_thread(os.path.exists, os.path.join(SESSION_DIR, f'session_{user_id}.session'))

    status_text = "✅ **Worker активен**." if is_worker_active else "❌ **Worker не активен**."
    
    text = f"⚙️ **Управление Worker**\n\n{status_text}\n\n*Для смены аккаунта используйте кнопку 'Новый вход/Авторизация'.*"
    
    await message_to_edit.edit_text(text, reply_markup=get_worker_menu_keyboard(is_worker_active, session_exists))


# --- Запуск/Остановка Worker ---
@user_router.callback_query(F.data == "stop_worker")
async def stop_worker_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer("Остановка Worker...", show_alert=False)
    await tm.stop_worker(call.from_user.id)
    return await worker_menu(call, state)

@user_router.callback_query(F.data == "start_worker")
async def start_worker_handler(call: types.CallbackQuery, state: FSMContext):
    # ✅ П.14: Проверка существования сессии перед запуском
    session_exists = await asyncio.to_thread(os.path.exists, os.path.join(SESSION_DIR, f'session_{call.from_user.id}.session'))
    if not session_exists:
        await call.answer("❌ Сессия не найдена. Сначала авторизуйтесь.", show_alert=True)
        return await worker_menu(call, state)
        
    await call.answer("Запуск Worker...", show_alert=False)
    await tm.start_client_task(call.from_user.id)
    # Даем worker'у время на запуск и обновление статуса в БД
    await asyncio.sleep(1) 
    return await worker_menu(call, state)


# --- Выбор метода авторизации ---
@user_router.callback_query(F.data == "auth_method_menu")
async def auth_method_menu(call: types.CallbackQuery, state: FSMContext):
    await tm.stop_worker(call.from_user.id) 
    
    # ✅ П.12: Удаляем временные файлы сессий перед новой авторизацией
    temp_path = os.path.join(SESSION_DIR, f'temp_{call.from_user.id}.session')
    with suppress(FileNotFoundError):
        await asyncio.to_thread(os.remove, temp_path)
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📲 По номеру телефона", callback_data="auth_by_phone"))
    builder.row(InlineKeyboardButton(text="📷 По QR-коду (Временно отключено)", callback_data="auth_by_qr_placeholder")) 
    
    # ✅ П.9: Используем оригинальный call для отмены
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data="worker_menu")) 
    
    await call.message.edit_text("🚪 **Выберите способ авторизации**:", reply_markup=builder.as_markup())
    await call.answer()
    
@user_router.callback_query(F.data == "auth_by_qr_placeholder")
async def auth_by_qr_placeholder(call: types.CallbackQuery):
    await call.answer("Функция входа по QR-коду находится в разработке.", show_alert=True)


# --- АВТОРИЗАЦИЯ ПО НОМЕРУ ТЕЛЕФОНА (НАЧАЛО) ---
@user_router.callback_query(F.data == "auth_by_phone")
async def auth_by_phone_step1(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    
    await call.message.edit_text(
        "📲 **Шаг 1/3: Введите номер телефона** в международном формате (например, `+79001234567`):",
        reply_markup=InlineKeyboardBuilder().row(
            InlineKeyboardButton(text="🔙 Отмена", callback_data="worker_menu")
        ).as_markup()
    )
    await call.answer()

# --- Шаг 2: Ввод номера телефона ---
@user_router.message(TelethonAuth.PHONE, F.text)
async def auth_by_phone_step2_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    user_id = message.from_user.id
    
    if not re.match(r'^\+\d{10,15}$', phone):
        return await message.answer("❌ Неверный формат. Введите номер, начиная с +, например `+79001234567`:")
    
    # Сохраняем Message, которое нужно для _finalize_auth
    await state.update_data(phone=phone, original_message=message) 

    # 1. Создаем временный клиент
    session_path = os.path.join(SESSION_DIR, f'temp_{user_id}')
    client = TelegramClient(session_path, API_ID, API_HASH, device_model="StatPro Auth")
    store.temp_auth_clients[user_id] = client

    try:
        # 2. Подключаемся и отправляем код
        await client.connect()
        send_code_result = await client.send_code_request(phone) 
        await state.update_data(send_code_hash=send_code_result.phone_code_hash)
        
        await state.set_state(TelethonAuth.CODE)
        
        await message.answer(
            f"🔑 **Шаг 2/3: Введите код** из сообщения от Telegram на номер `{phone}`:",
            reply_markup=InlineKeyboardBuilder().row(
                InlineKeyboardButton(text="🔙 Отмена", callback_data="worker_menu")
            ).as_markup()
        )
    except PhoneNumberInvalidError:
        await state.clear()
        return await message.answer("❌ Неверный номер телефона. Попробуйте снова через меню 'Новый вход/Авторизация'.")
    except Exception as e:
        await state.clear()
        logger.error(f"Telethon Phone Auth Error for {user_id}: {e}", exc_info=True)
        return await message.answer(f"❌ Произошла ошибка при отправке кода: `{e.__class__.__name__}`. Попробуйте позже.")
    
# --- Шаг 3: Ввод кода авторизации ---
@user_router.message(TelethonAuth.CODE, F.text.regexp(r'^\d{4,6}$'))
async def auth_by_phone_step3_code(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    code = message.text.strip()
    code_hash = data.get('send_code_hash')
    original_message = data.get('original_message')
    phone = data.get('phone')
    
    if user_id not in store.temp_auth_clients:
        await state.clear()
        return await message.answer("❌ Сессия авторизации истекла. Начните сначала.")

    client = store.temp_auth_clients[user_id]
    
    try:
        # 4. Вход с кодом
        user_info = await client.sign_in(phone, code, phone_code_hash=code_hash)
        
        # Успешный вход без 2FA
        # ✅ П.4: Передаем Message
        await tm._finalize_auth(user_id, original_message, state, user_info)
        
    except SessionPasswordNeededError:
        # Требуется 2FA
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer(
            f"🔒 **Шаг 3/3: Введите облачный пароль (2FA):**",
            reply_markup=InlineKeyboardBuilder().row(
                InlineKeyboardButton(text="🔙 Отмена", callback_data="worker_menu")
            ).as_markup()
        )
    except RpcCallFailError as e:
        await state.clear()
        client.disconnect()
        store.temp_auth_clients.pop(user_id, None)
        logger.error(f"Telethon Code Auth RpcCallFailError for {user_id}: {e}")
        return await message.answer("❌ Неверный код. Попробуйте начать авторизацию снова.")
    except Exception as e:
        await state.clear()
        client.disconnect()
        store.temp_auth_clients.pop(user_id, None)
        logger.error(f"Telethon Code Auth Error for {user_id}: {e}", exc_info=True)
        return await message.answer(f"❌ Произошла ошибка при вводе кода: `{e.__class__.__name__}`. Попробуйте позже.")


# --- Шаг 4: Ввод пароля 2FA ---
@user_router.message(TelethonAuth.PASSWORD, F.text)
async def auth_by_phone_step4_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    password = message.text.strip()
    original_message = data.get('original_message')
    
    if user_id not in store.temp_auth_clients:
        await state.clear()
        return await message.answer("❌ Сессия авторизации истекла. Начните сначала.")

    client = store.temp_auth_clients[user_id]
    
    try:
        # 5. Вход с паролем
        user_info = await client.sign_in(password=password)
        
        # Успешный вход с 2FA
        # ✅ П.4: Передаем Message
        await tm._finalize_auth(user_id, original_message, state, user_info)
        
    except PasswordHashInvalidError:
        return await message.answer("❌ Неверный облачный пароль. Попробуйте снова или нажмите 'Отмена'.")
    except Exception as e:
        await state.clear()
        client.disconnect()
        store.temp_auth_clients.pop(user_id, None)
        logger.error(f"Telethon Password Auth Error for {user_id}: {e}", exc_info=True)
        return await message.answer(f"❌ Критическая ошибка при вводе пароля: `{e.__class__.__name__}`. Попробуйте позже.")

# --- Отмена FSM состояния (Общая) ---
@user_router.callback_query(F.data == "cancel_state")
async def cancel_state_handler(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.answer("Действие отменено.", show_alert=False)
    # Перенаправляем на профиль, чтобы обновить клавиатуру
    return await profile_menu(call, state)


# --- Активация промокода (Начало) ---
@user_router.callback_query(F.data == "enter_promo")
async def enter_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await call.message.edit_text(
        "🔑 **Введите промокод**:",
        reply_markup=InlineKeyboardBuilder().row(
            InlineKeyboardButton(text="🔙 Отмена", callback_data="profile_menu")
        ).as_markup()
    )
    await call.answer()

# --- Активация промокода (Обработка) ---
@user_router.message(PromoStates.waiting_for_code, F.text)
async def process_promo_activation(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # ✅ П.6: Восстановлена обрезанная строка
    code = message.text.strip().upper() 
    
    days = await db.activate_promo_code(user_id, code)
    
    await state.clear()
    
    if days:
        await message.answer(f"🎉 **Промокод активирован!** Ваша подписка продлена на **{days}** дней.")
        # Создаем фиктивный call для корректного вызова profile_menu/start_menu
        fake_call = types.CallbackQuery( 
            id='fake_promo', 
            from_user=message.from_user, 
            message=await message.answer("🔄 Переход к профилю...") 
        )
        return await profile_menu(fake_call, state)

    else:
        await message.answer(
            "❌ **Промокод не найден или уже недействителен.** Попробуйте снова или свяжитесь с поддержкой.",
            reply_markup=InlineKeyboardBuilder().row(
                InlineKeyboardButton(text="🔙 Профиль", callback_data="profile_menu")
            ).as_markup()
        )

# ✅ П.10: Хендлер для неизвестных команд
@user_router.message(F.text)
async def handle_unknown_command(message: types.Message):
    if message.text.startswith('/'):
        return await message.answer("❌ Неизвестная команда. Используйте /start для вызова главного меню.")
    

# =========================================================================
# VIII. ФУНКЦИЯ ЗАПУСКА
# =========================================================================

# ✅ П.3: Добавлена функция main()
async def main():
    # ✅ П.13: Инициализация базы данных перед использованием
    await db.init() 
    logger.info("Starting bot...")
    dp.message.middleware(RateLimitMiddleware())
    dp.callback_query.middleware(RateLimitMiddleware())
    
    # ✅ П.8: Подключение роутеров
    dp.include_router(user_router)
    # dp.include_router(drops_router) 
    # dp.include_router(admin_router)
    
    try:
        # Проверяем и запускаем всех активных worker'ов после старта
        active_users = await db.get_active_telethon_users()
        if active_users:
             logger.info(f"Attempting to restart {len(active_users)} Telethon workers.")
             # Используем tm, который теперь определен!
             start_tasks = [tm.start_client_task(user_id) for user_id in active_users]
             await asyncio.gather(*start_tasks, return_exceptions=True)
        
        # ✅ П.8: Запуск бота
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == '__main__':
    # ✅ П.3: Запуск asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Critical error in main loop: {e}", exc_info=True)
