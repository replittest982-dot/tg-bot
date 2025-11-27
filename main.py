import asyncio
import logging
import logging.handlers
import os
import re
import random
import sys
import aiosqlite
import pytz
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Union, Set, Any
from functools import wraps
from io import BytesIO

# --- ENV ---
from dotenv import load_dotenv

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types, BaseMiddleware
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, FSInputFile, CallbackQuery
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, AuthKeyUnregisteredError,
    ChatSendForbiddenError
)
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# =========================================================================

# Загрузка переменных окружения
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "7868097991:AAGfHdp175nUyy0ah6PLw5sSY2wCy0V2_XI")
ADMIN_ID = int(os.getenv("ADMIN_ID", 6256576302)) 
API_ID = int(os.getenv("API_ID", 35775411))
API_HASH = os.getenv("API_HASH", "4f8220840326cb5f74e1771c0c4248f2")

DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 1.0 
SESSION_DIR = 'sessions'
BACKUP_DIR = 'backups'
RETRY_DELAY = 5 
PROMOCODE_MAX_LENGTH = 30 # Ограничение длины промокода

# Инициализация папок
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs('data', exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# --- Настройка логирования (Исправление 8: Гарантируем однократную настройку) ---
_logging_setup_done = False
def setup_logging(log_file='bot.log', level=logging.INFO):
    """Настраивает логирование в консоль и файл с ротацией."""
    global _logging_setup_done
    if _logging_setup_done: return
    
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)
    
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    _logging_setup_done = True

setup_logging()
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())
user_router = Router(name='user_router')
drops_router = Router(name='drops_router')
admin_router = Router(name='admin_router')

# =========================================================================
# II. ГЛОБАЛЬНОЕ ХРАНИЛИЩЕ, УТИЛИТЫ И FSM STATES
# =========================================================================

class WorkerTask:
    # ... (Определение WorkerTask - без изменений) ...
    def __init__(self, task_type: str, task_id: str, creator_id: int, target: Union[int, str], args: tuple = ()):
        self.task_type = task_type
        self.task_id = task_id
        self.creator_id = creator_id
        self.target = target
        self.args = args
        self.task: Optional[asyncio.Task] = None
        self.start_time: datetime = datetime.now(TIMEZONE_MSK)

    def __str__(self) -> str:
        elapsed = int((datetime.now(TIMEZONE_MSK) - self.start_time).total_seconds())
        return f"[{self.task_type.upper()}] T:{self.target} ID:{self.task_id} Время: {elapsed} сек."

class GlobalStorage:
    # ... (Определение GlobalStorage - без изменений) ...
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {}
        self.pc_monitoring: Dict[Union[int, str], str] = {} 
        self.active_workers: Dict[int, TelegramClient] = {} 
        self.worker_tasks: Dict[int, Dict[str, WorkerTask]] = {} 
        self.premium_users: Set[int] = set()
        self.admin_jobs: Dict[str, asyncio.Task] = {} 

store = GlobalStorage()

# --- FSM States ---

class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    PROMO_CODE = State()

class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_phone_change = State()

class AdminStates(StatesGroup):
    waiting_for_promo_data = State()

# --- Utilities ---

def get_session_path(user_id: int, is_temp: bool = False) -> str:
    """Получает путь к файлу сессии Telethon."""
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def to_msk_aware(dt_str: str) -> Optional[datetime]:
    # ... (to_msk_aware - без изменений) ...
    if not dt_str: return None
    try:
        naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        return TIMEZONE_MSK.localize(naive_dt)
    except ValueError:
        return None

def get_topic_name_from_message(message: types.Message) -> Optional[str]:
    # ... (get_topic_name_from_message - без изменений) ...
    topic_key = message.message_thread_id if message.message_thread_id else message.chat.id
    return store.pc_monitoring.get(topic_key)

def is_valid_phone(phone: str) -> bool:
    # ... (is_valid_phone - без изменений) ...
    return re.match(r'^\+\d{10,15}$', phone) is not None

def is_valid_username(username: str) -> bool:
    """Проверяет, является ли строка действительным @username."""
    return username.startswith('@') and len(username) > 1

# =========================================================================
# III. БАЗА ДАННЫХ
# =========================================================================

class AsyncDatabase:
    """Асинхронный менеджер базы данных SQLite."""
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    async def init(self):
        # ... (init - без изменений) ...
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    telethon_active BOOLEAN DEFAULT 0,
                    subscription_end TEXT,
                    is_banned BOOLEAN DEFAULT 0,
                    password_2fa TEXT
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS drop_sessions (
                    phone TEXT PRIMARY KEY, 
                    pc_name TEXT, 
                    drop_id INTEGER, 
                    status TEXT, 
                    start_time TEXT, 
                    last_status_time TEXT, 
                    prosto_seconds INTEGER DEFAULT 0
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promocodes (
                    code TEXT PRIMARY KEY,
                    duration_days INTEGER,
                    uses_left INTEGER
                )
            """)
            await db.commit()
            logger.info("Database initialized successfully.")

    # ... (get_user, get_subscription_status, update_subscription - без изменений) ...
    async def get_user(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            await db.commit()
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
                result = await cursor.fetchone() 
                return dict(result) if result else None
    
    async def get_subscription_status(self, user_id: int) -> Optional[datetime]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT subscription_end FROM users WHERE user_id=?", (user_id,)) as cursor:
                result = await cursor.fetchone() 
                if result and result[0]:
                    return to_msk_aware(result[0])
                return None
                
    async def update_subscription(self, user_id: int, days: int):
        async with aiosqlite.connect(self.db_path) as db:
            current_end = await self.get_subscription_status(user_id)
            now = datetime.now(TIMEZONE_MSK)
            
            if current_end and current_end > now:
                new_end = current_end + timedelta(days=days)
            else:
                new_end = now + timedelta(days=days)
            
            await db.execute("UPDATE users SET subscription_end=? WHERE user_id=?", (new_end.strftime('%Y-%m-%d %H:%M:%S'), user_id))
            await db.commit()
            return new_end

    async def get_promocode(self, code: str):
        # ... (get_promocode - без изменений) ...
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM promocodes WHERE code=?", (code.upper(),)) as cursor:
                result = await cursor.fetchone() 
                return dict(result) if result else None

    async def add_promocode(self, code: str, days: int, uses: int) -> bool:
        """Добавляет новый промокод (Исправление 18: Валидация длины)."""
        if len(code) > PROMOCODE_MAX_LENGTH:
             logger.warning(f"Promocode too long: {code}")
             return False
             
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("INSERT INTO promocodes (code, duration_days, uses_left) VALUES (?, ?, ?)", 
                                (code.upper(), days, uses))
                await db.commit()
                return True
        except aiosqlite.IntegrityError:
            return False 
    
    # ... (get_stats, use_promocode - без изменений) ...
    async def get_stats(self) -> Dict[str, Union[int, float]]:
        async with aiosqlite.connect(self.db_path) as db:
            stats = {}
            async with db.execute("SELECT COUNT(user_id) FROM users") as cursor:
                stats['total_users'] = (await cursor.fetchone())[0]
                
            now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
            async with db.execute("SELECT COUNT(user_id) FROM users WHERE telethon_active=1 AND subscription_end > ?", (now_str,)) as cursor:
                stats['active_workers'] = (await cursor.fetchone())[0]

            async with db.execute("SELECT COUNT(phone) FROM drop_sessions WHERE status NOT IN ('closed', 'deleted')") as cursor:
                stats['active_drops'] = (await cursor.fetchone())[0]

            async with db.execute("SELECT SUM(duration_days) FROM promocodes") as cursor:
                stats['total_promo_days'] = (await cursor.fetchone())[0] or 0

            return stats
    
    async def use_promocode(self, code: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            promocode = await self.get_promocode(code)
            if not promocode or promocode['uses_left'] <= 0:
                return False

            new_uses = promocode['uses_left'] - 1
            await db.execute("UPDATE promocodes SET uses_left=? WHERE code=?", (new_uses, code.upper()))
            await db.commit()
            return True
            
    async def cleanup_old_sessions(self, days: int = 30):
        # ... (cleanup_old_sessions - без изменений) ...
        async with aiosqlite.connect(self.db_path) as db:
            cutoff = (datetime.now(TIMEZONE_MSK) - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            
            await db.execute("UPDATE drop_sessions SET status='deleted' WHERE last_status_time < ? AND status IN ('closed', 'slet', 'error')", (cutoff,))
            
            now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
            await db.execute("DELETE FROM users WHERE subscription_end IS NOT NULL AND subscription_end < ? AND telethon_active=0", (now_str,))
            
            await db.commit()
            logger.info("Database cleanup completed.")

    # ... (set_telethon_status, get_active_telethon_users, set_password_2fa - без изменений) ...
    async def set_telethon_status(self, user_id: int, status: bool):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            await db.commit()
    
    async def get_active_telethon_users(self) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1 AND is_banned=0 AND (subscription_end IS NULL OR subscription_end > ?)", (now_str,)) as cursor:
                return [row[0] for row in await cursor.fetchall()]
    
    async def set_password_2fa(self, user_id: int, password: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET password_2fa=? WHERE user_id=?", (password, user_id))
            await db.commit()

    async def get_drop_session_by_drop_id(self, drop_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions WHERE drop_id=? AND status NOT IN ('closed', 'deleted') ORDER BY start_time DESC LIMIT 1", (drop_id,)) as cursor:
                result = await cursor.fetchone() 
                return dict(result) if result else None

    async def get_drop_session_by_phone(self, phone: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions WHERE phone=? AND status NOT IN ('closed', 'deleted') ORDER BY start_time DESC LIMIT 1", (phone,)) as cursor:
                result = await cursor.fetchone() 
                return dict(result) if result else None
    
    async def create_drop_session(self, phone: str, pc_name: str, drop_id: int, status: str) -> bool:
        """Создает новую drop-сессию (Исправление 7: Ловля IntegrityError)."""
        now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        current = await self.get_drop_session_by_phone(phone)
        if current:
            return False 

        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("INSERT INTO drop_sessions (phone, pc_name, drop_id, status, start_time, last_status_time) VALUES (?, ?, ?, ?, ?, ?)", 
                                (phone, pc_name, drop_id, status, now_str, now_str))
                await db.commit()
                return True
        except aiosqlite.IntegrityError as e:
            logger.error(f"Race condition in create_drop_session for {phone}: {e}")
            return False

    async def update_drop_status(self, old_phone: str, new_status: str, new_phone: Optional[str] = None) -> bool:
        # ... (update_drop_status - без изменений) ...
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.now(TIMEZONE_MSK)
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            
            current_session = await self.get_drop_session_by_phone(old_phone)
            
            # Исправление 11: Проверка на None перед доступом к ключам
            if not current_session: 
                logger.warning(f"Session not found for phone {old_phone} during status update.")
                return False
                
            old_time = to_msk_aware(current_session.get('last_status_time')) or now
            time_diff = int((now - old_time).total_seconds())
            prosto_seconds = current_session.get('prosto_seconds', 0)

            is_prosto_status = current_session['status'] in ('дайте номер', 'error', 'slet')

            if is_prosto_status:
                prosto_seconds += time_diff
            
            if new_phone and new_phone != old_phone:
                await db.execute("UPDATE drop_sessions SET status='closed', last_status_time=? WHERE phone=?", (now_str, old_phone))
                
                success = await self.create_drop_session(new_phone, current_session['pc_name'], current_session['drop_id'], 'замена')
                if not success: 
                    return False
                
                await db.execute("UPDATE drop_sessions SET prosto_seconds=?, last_status_time=? WHERE phone=?", (prosto_seconds, now_str, new_phone))

            else:
                query = "UPDATE drop_sessions SET status=?, last_status_time=?, prosto_seconds=? WHERE phone=?"
                await db.execute(query, (new_status, now_str, prosto_seconds, old_phone))
            
            await db.commit()
            return True

db = AsyncDatabase(os.path.join('data', DB_NAME))

# =========================================================================
# IV. MIDDLEWARE (RATE LIMIT)
# =========================================================================

class SimpleRateLimitMiddleware(BaseMiddleware):
    # ... (SimpleRateLimitMiddleware - без изменений) ...
    def __init__(self, limit: float = 1.0) -> None:
        self.limit = limit
        self.user_timestamps: Dict[int, datetime] = {}
        super().__init__()

    async def __call__(
        self,
        handler: Any,
        event: types.Message,
        data: Dict[str, Any]
    ) -> Any:
        user_id = event.from_user.id
        now = datetime.now()
        
        last_time = self.user_timestamps.get(user_id)
        
        if last_time and (now - last_time).total_seconds() < self.limit:
            wait_time = round(self.limit - (now - last_time).total_seconds(), 2)
            await event.reply(f"🚫 **Rate Limit**. Слишком быстро. Подождите {wait_time} сек.")
            return

        self.user_timestamps[user_id] = now
        return await handler(event, data)

dp.message.middleware(SimpleRateLimitMiddleware(limit=RATE_LIMIT_TIME))

# =========================================================================
# V. TELETHON MANAGER
# =========================================================================

class TelethonManager:
    """Менеджер для запуска, остановки и управления Telethon-воркерами и их задачами."""
    def __init__(self, bot_instance: Bot, db_instance: AsyncDatabase):
        self.bot = bot_instance
        self.db = db_instance
        self.API_ID = API_ID
        self.API_HASH = API_HASH

    async def _send_to_bot_user(self, user_id: int, message: str):
        # ... (_send_to_bot_user - без изменений) ...
        try:
            await self.bot.send_message(user_id, message)
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            logger.warning(f"Failed to send message to user {user_id}. Stopping worker. Error: {e.__class__.__name__}: {e}")
            await self.stop_worker(user_id)
        except Exception as e:
            logger.error(f"Unknown error sending message to {user_id}: {e.__class__.__name__}: {e}")
            # Не останавливаем worker, если это не проблема с доступом к пользователю

    async def _notify_admin(self, message: str):
        # ... (_notify_admin - без изменений) ...
        try:
            await self.bot.send_message(ADMIN_ID, f"🚨 **ADMIN ALERT**: {message}")
        except Exception as e:
            logger.error(f"Failed to notify admin {ADMIN_ID}: {e.__class__.__name__}: {e}")

    async def start_client_task(self, user_id: int):
        """Запускает главный асинхронный таск для Telethon-клиента (Исправление 15: Добавлен try/except для старта)."""
        await self.stop_worker(user_id)
        
        try:
            task = asyncio.create_task(self._run_worker(user_id), name=f"main-worker-{user_id}")
            
            task_id = f"main-{user_id}"
            async with store.lock: 
                worker_task = WorkerTask(task_type="main", task_id=task_id, creator_id=user_id, target="worker")
                worker_task.task = task
                store.worker_tasks.setdefault(user_id, {})[task_id] = worker_task
                store.premium_users.add(user_id)

            logger.info(f"Main worker task started for user {user_id}")
            return task
        except Exception as e:
            # Если ошибка произошла до регистрации таска (например, неверные API ID/Hash), сбрасываем статус
            logger.error(f"Critical error during start_client_task for {user_id}: {e.__class__.__name__}: {e}")
            await self.db.set_telethon_status(user_id, False)
            await self._send_to_bot_user(user_id, f"❌ Критическая ошибка при запуске worker'а. Проверьте данные сессии.")


    async def _run_worker(self, user_id: int):
        """Основная функция Telethon-воркера."""
        path = get_session_path(user_id)
        client = TelegramClient(path, self.API_ID, self.API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        async with store.lock: store.active_workers[user_id] = client

        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            await self.worker_message_handler(user_id, client, event)

        try:
            # Исправление 13: Проверка доступности Telegram API
            if not await client.is_user_authorized():
                await client.connect()
                
            if not await client.is_user_authorized():
                 await client.start()

            sub_end = await self.db.get_subscription_status(user_id)
            if not sub_end or sub_end <= datetime.now(TIMEZONE_MSK):
                await self._send_to_bot_user(user_id, "⚠️ Ваша подписка истекла. Worker будет отключен.")
                return 
            
            await self.db.set_telethon_status(user_id, True)
            await self._send_to_bot_user(user_id, f"🚀 Worker запущен. Подписка до: **{sub_end.strftime('%d.%m.%Y')}**.")
            
            await asyncio.sleep(float('inf'))
            
        except AuthKeyUnregisteredError:
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Пожалуйста, выполните повторный вход.")
        except asyncio.CancelledError:
            logger.info(f"Worker {user_id} task cancelled.")
        except Exception as e:
            error_msg = f"{e.__class__.__name__}: {e}"
            logger.error(f"Worker {user_id} crashed: {error_msg}")
            await self._send_to_bot_user(user_id, f"💔 Worker отключился: {error_msg}.")
        finally:
            # stop_worker вызывает set_telethon_status(False)
            await self.stop_worker(user_id)

    async def stop_worker(self, user_id: int):
        # ... (stop_worker - без изменений) ...
        async with store.lock:
            client = store.active_workers.pop(user_id, None)
            tasks_to_cancel = store.worker_tasks.pop(user_id, {})
            store.premium_users.discard(user_id)

            for task_id, worker_task in tasks_to_cancel.items():
                if worker_task.task and not worker_task.task.done():
                    worker_task.task.cancel()
                    logger.info(f"Task {task_id} for user {user_id} cancelled.")

        if client:
            try:
                await client.disconnect()
            except Exception:
                pass 

        await self.db.set_telethon_status(user_id, False)

    async def worker_message_handler(self, user_id: int, client: TelegramClient, event: events.NewMessage.Event):
        """Обработчик исходящих сообщений Telethon-клиента для команд (Исправление 14: Дебаунс на .флуд)."""
        if not event.text or not event.text.startswith('.'):
            return

        msg = event.text.strip().lower()
        parts = msg.split()
        cmd = parts[0]
        
        # Удаляем сообщение, если это не команда
        if cmd not in ('.пкворк', '.флуд', '.стопфлуд', '.лс', '.чекгруппу', '.статус'):
             await event.delete()
             return
        
        chat_id = event.chat_id
        
        if cmd == '.флуд':
            # Исправление 14: Проверка, что нет уже активных задач флуда для этого пользователя
            async with store.lock:
                active_flood = any(t.task_type == "flood" for t in store.worker_tasks.get(user_id, {}).values())
            
            if active_flood:
                await client.send_message(chat_id, "⚠️ **Флуд уже запущен!** Используйте `.стопфлуд` перед запуском новой задачи.")
                return 

            try:
                count = int(parts[1]); delay = float(parts[2])
                target = parts[3] if len(parts) > 4 else event.chat_id
                text = " ".join(parts[4:])
                if not text: raise ValueError("Нет текста для флуда.")
                
                await self._start_flood_task(user_id, client, chat_id, target, count, delay, text)
                
            except (IndexError, ValueError) as e:
                await client.send_message(chat_id, f"❌ Неверный формат. Использование: `.флуд <кол-во> <задержка> <цель/чат_id> <текст>`. Ошибка: {e.__class__.__name__}")
            except Exception as e:
                await client.send_message(chat_id, f"❌ Ошибка при запуске флуда: {e.__class__.__name__}: {e}")
        
        # ... (Остальные команды .пкворк, .стопфлуд, .лс, .чекгруппу, .статус - без изменений в логике) ...
        elif cmd == '.пкворк':
            pc_name = parts[1] if len(parts) > 1 else 'PC'
            topic_key = event.message.reply_to_msg_id if event.message.reply_to_msg_id else chat_id
            async with store.lock: 
                store.pc_monitoring[topic_key] = pc_name
            temp = await client.send_message(chat_id, f"✅ ПК для топика **{topic_key}** установлен как **{pc_name}**.", reply_to=event.message.id)
            await asyncio.sleep(2); await temp.delete()
        
        elif cmd == '.стопфлуд':
            stopped = await self._stop_tasks_by_type(user_id, "flood")
            await client.send_message(chat_id, f"✅ Остановлено {stopped} задач флуда.")
            
        elif cmd == '.лс':
            try:
                lines = event.text.split('\n')
                if len(lines) < 2:
                    return await client.send_message(chat_id, "❌ Неверный формат. Первая строка - `.лс`, вторая - текст, далее построчно @username.")
                
                content = lines[1] 
                # Исправление 17: Фильтрация по валидности username
                usernames = [line.strip() for line in lines[2:] if is_valid_username(line.strip())] 
                
                if not usernames: return await client.send_message(chat_id, "❌ Цели для рассылки не указаны или неверный формат @username.")
                
                await self._start_mass_dm_task(user_id, client, chat_id, content, usernames)

            except Exception as e:
                await client.send_message(chat_id, f"❌ Ошибка при запуске рассылки: {e.__class__.__name__}: {e}")
                
        elif cmd == '.чекгруппу':
            try:
                target = parts[1] if len(parts) > 1 else chat_id
                await self._start_check_group_task(user_id, client, chat_id, target)
            except IndexError:
                await client.send_message(chat_id, "❌ Использование: `.чекгруппу <чат_id/@username>`")
            except Exception as e:
                await client.send_message(chat_id, f"❌ Ошибка при запуске сканирования: {e.__class__.__name__}: {e}")

        elif cmd == '.статус':
            await self._report_status(user_id, client, chat_id)
            
    # --- Task Executors (Flood, Mass DM, Check Group - без изменений в логике) ---
    def _flood_executor_factory(self, user_id: int, client: TelegramClient, task_id: str, target: Union[int, str], count: int, delay: float, text: str):
        # ... (_flood_executor_factory - без изменений) ...
        async def executor():
            logger.info(f"Starting flood task {task_id} on {target}")
            try:
                for i in range(1, count + 1):
                    try:
                        await client.send_message(target, text)
                        await asyncio.sleep(delay)
                    except FloodWaitError as e:
                        await self._send_to_bot_user(user_id, f"⏳ **{target}**: FloodWait на {e.seconds} сек. Флуд остановлен.")
                        break
                    except ChatSendForbiddenError:
                        await self._send_to_bot_user(user_id, f"❌ **{target}**: Запрет на отправку. Флуд остановлен.")
                        break
                    except asyncio.CancelledError:
                        raise 
                    except Exception as e:
                        await self._send_to_bot_user(user_id, f"❌ **{target}**: Ошибка отправки: {e.__class__.__name__}: {e}")
                        break
            except asyncio.CancelledError:
                 pass 
            finally:
                await self._send_to_bot_user(user_id, f"✅ Задача флуда **{task_id}** завершена (цель: **{target}**).")
                await self._remove_task(user_id, task_id)
        return executor
        
    def _mass_dm_executor_factory(self, user_id: int, client: TelegramClient, task_id: str, content: str, usernames: List[str]):
        # ... (логика mass_dm - без изменений) ...
        async def executor():
            success_count = 0; fail_report = []
            
            for username in usernames:
                try:
                    await client.send_message(username, content)
                    success_count += 1
                    await asyncio.sleep(1) 
                except FloodWaitError as e:
                    await self._send_to_bot_user(user_id, f"⏳ **{username}**: FloodWait на {e.seconds} сек. Рассылка остановлена.")
                    fail_report.append(f"FloodWait на {e.seconds} сек. на {username}")
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    fail_report.append(f"❌ {username}: Ошибка ({e.__class__.__name__}: {e}).") 

            report_message = [f"✅ **Отчет по рассылке (Задача {task_id}):**"]
            report_message.append(f"  * **Успешно отправлено:** {success_count} из {len(usernames)}")
            if fail_report:
                report_message.append("\n**⚠️ Ошибки:**")
                report_message.extend(fail_report)

            await self._send_to_bot_user(user_id, "\n".join(report_message))
            await self._remove_task(user_id, task_id)
        return executor

    def _check_group_executor_factory(self, user_id: int, client: TelegramClient, task_id: str, target: Union[int, str]):
        async def executor():
            users_list = []
            limit = 200; offset = 0; total_participants = 0
            buffer = None
            
            try:
                entity = await client.get_entity(target)
                
                if not isinstance(entity, (Channel, Chat)):
                    await self._send_to_bot_user(user_id, f"❌ **{target}**: Цель не является группой или каналом.")
                    return

                while True:
                    if isinstance(entity, Channel):
                        participants = await client(GetParticipantsRequest(
                            entity, ChannelParticipantsSearch(''), offset, limit, hash=0
                        ))
                        
                        if not participants.participants: break
                            
                        total_participants = participants.count
                        users = participants.users
                        
                        for user_obj in users:
                            if isinstance(user_obj, User):
                                username = user_obj.username if user_obj.username else 'N/A'
                                name = f"{user_obj.first_name or ''} {user_obj.last_name or ''}".strip()
                                status = user_obj.status.__class__.__name__.replace('UserStatus', '')
                                users_list.append(f"ID: {user_obj.id}, Username: @{username}, Name: {name}, Status: {status}")
                        
                        offset += len(participants.participants)
                        if len(participants.participants) < limit or offset >= total_participants: break
                    else: 
                        await self._send_to_bot_user(user_id, f"❌ **{target}**: Сканирование обычных чатов не поддерживается.")
                        break
                         
                    await asyncio.sleep(RETRY_DELAY)
                
                report_content = f"Отчет по сканированию чата: {target}\nВсего участников: {total_participants}\n\n"
                report_content += "\n".join(users_list)
                
                buffer = BytesIO(report_content.encode('utf-8'))
                buffer.name = f"scan_report_{target}_{datetime.now().strftime('%Y%m%d%H%M')}.txt"
                buffer.seek(0)
                
                # Исправление 19: Добавлена обработка ошибок отправки документов
                try:
                    await self.bot.send_document(user_id, FSInputFile(buffer, filename=buffer.name))
                    await self._send_to_bot_user(user_id, f"✅ **{target}**: Сканирование завершено. Отчет в файле.")
                except Exception as e:
                    await self._send_to_bot_user(user_id, f"❌ **{target}**: Отчет не отправлен. Ошибка Telegram: {e.__class__.__name__}: {e}")

            except FloodWaitError as e:
                await self._send_to_bot_user(user_id, f"⏳ **{target}**: FloodWait на {e.seconds} сек. Сканирование остановлено.")
            except Exception as e:
                await self._send_to_bot_user(user_id, f"❌ **{target}**: Ошибка сканирования: {e.__class__.__name__}: {e}")
            finally:
                if buffer:
                    buffer.close() # Исправление 1: Закрытие буфера
                await self._remove_task(user_id, task_id)
        return executor
        
    # ... (Остальные методы _start_flood_task, _start_mass_dm_task, _start_check_group_task - без изменений) ...

tm = TelethonManager(bot, db)

# =========================================================================
# VI. HANDLERS (USERS, DROPS, ADMIN)
# =========================================================================

# --- Users Handler (Исправление 16: Очистка FSM state при ошибках) ---

@user_router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    # ... (логика start - без изменений) ...
    await db.get_user(message.from_user.id)
    
    sub_end = await db.get_subscription_status(message.from_user.id)
    status_text = f"✅ **Подписка активна** до: **{sub_end.strftime('%d.%m.%Y')}**." if sub_end and sub_end > datetime.now(TIMEZONE_MSK) else "❌ **Подписка не активна.**"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Вход Telethon", callback_data="auth_phone")],
        [InlineKeyboardButton(text="🔑 Активировать промокод", callback_data="activate_promo")],
        [InlineKeyboardButton(text="❓ Помощь / Команды", callback_data="show_help")]
    ])
    await message.answer(f"👋 Добро пожаловать в STATPRO Worker.\n\n{status_text}", reply_markup=kb)

# ... (cmd_help, activate_promo_start, activate_promo_input - без изменений) ...

@user_router.message(TelethonAuth.CODE)
async def auth_code_input(message: types.Message, state: FSMContext):
    # ... (логика code input) ...
    user_id = message.from_user.id; data = await state.get_data()
    async with store.lock: client = store.temp_auth_clients.get(user_id)
    
    if not client: 
        await state.clear()
        return await message.answer("Сессия истекла. Начните заново.")
        
    try:
        await client.sign_in(data['phone'], message.text.strip(), phone_code_hash=data['hash'])
        # ... (успешный вход) ...
        
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔐 Включена двухфакторная аутентификация. Введите ваш облачный пароль:")
        
    except Exception as e: 
        # Исправление 16: Очистка FSM state при ошибке
        await state.clear()
        await message.answer(f"❌ Ошибка входа: {e.__class__.__name__}: {e}")

@user_router.message(TelethonAuth.PASSWORD)
async def auth_password_input(message: types.Message, state: FSMContext):
    # ... (логика password input) ...
    user_id = message.from_user.id; data = await state.get_data()
    async with store.lock: client = store.temp_auth_clients.pop(user_id, None)
    
    if not client: 
        await state.clear()
        return await message.answer("Сессия истекла. Начните заново.")
        
    password = message.text.strip()
    
    try:
        await client.sign_in(password=password)
        # ... (успешный вход) ...
        
    except Exception as e:
        # Исправление 16: Очистка FSM state при ошибке
        await state.clear()
        await message.answer(f"❌ Неверный пароль. Ошибка: {e.__class__.__name__}: {e}")

# --- Admin Handlers (Исправление 4, 9: Использование F.from_user.id в фильтре) ---

@admin_router.message(Command("admin"), F.from_user.id == ADMIN_ID)
async def cmd_admin(message: types.Message):
    """Главное меню администратора."""
    # ... (логика admin - без изменений) ...
    stats = await db.get_stats()
    
    text = (
        "👑 **Админ-Панель STATPRO**\n\n"
        f"👥 **Пользователи:** {stats['total_users']}\n"
        f"🚀 **Активные воркеры:** {stats['active_workers']}\n"
        f"📞 **Активные дропы:** {stats['active_drops']}\n"
        f"🔑 **Дни промокодов:** {stats['total_promo_days']}"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Рестарт всех воркеров", callback_data="admin_restart_workers")],
        [InlineKeyboardButton(text="🔑 Добавить промокод", callback_data="admin_add_promo")],
        [InlineKeyboardButton(text="💾 Бэкап базы", callback_data="admin_backup_db")],
        [InlineKeyboardButton(text="🧹 Очистка БД и файлов", callback_data="admin_cleanup_db")],
        [InlineKeyboardButton(text="❌ Остановка бота", callback_data="admin_shutdown")],
    ])
    await message.answer(text, reply_markup=kb)

@admin_router.callback_query(F.data == "admin_shutdown", F.from_user.id == ADMIN_ID)
async def admin_shutdown(call: CallbackQuery):
    await call.answer("Отключаю бота...")
    await call.message.edit_text("❌ **Бот отключен.**")
    
    # Исправление 10: Используем loop.stop() или sys.exit для гарантированной остановки
    await dp.stop_polling()
    asyncio.get_event_loop().stop()
    sys.exit(0) 

@admin_router.callback_query(F.data == "admin_backup_db", F.from_user.id == ADMIN_ID)
async def admin_backup_db(call: CallbackQuery):
    """Создание и отправка бэкапа (Исправление 1, 3)."""
    await call.answer("Создаю бэкап...")
    now_str = datetime.now(TIMEZONE_MSK).strftime('%Y%m%d_%H%M%S')
    backup_path = os.path.join(BACKUP_DIR, f"backup_{now_str}.db")
    buffer = None
    dst_db = None
    
    try:
        # Исправление 3: Асинхронное подключение к целевой БД
        dst_db = await aiosqlite.connect(backup_path)
        async with aiosqlite.connect(db.db_path) as src_db:
            await src_db.backup(dst_db)
            
        buffer = open(backup_path, 'rb')
        await call.message.answer_document(FSInputFile(buffer, filename=os.path.basename(backup_path)), caption="✅ **Бэкап базы данных**")
    except Exception as e:
        logger.error(f"Error during manual backup: {e.__class__.__name__}: {e}")
        await call.message.answer(f"❌ Ошибка создания/отправки бэкапа: {e.__class__.__name__}: {e}")
    finally:
        # Исправление 1: Закрытие всех ресурсов
        if buffer: buffer.close() 
        if dst_db: await dst_db.close()

@admin_router.callback_query(F.data == "admin_cleanup_db", F.from_user.id == ADMIN_ID)
async def admin_cleanup_db_full(call: CallbackQuery):
    await call.answer("Запускаю очистку БД и сессий...")
    await db.cleanup_old_sessions(days=60)
    # Исправление 12: Добавление очистки файлов сессий
    deleted_files = await _cleanup_sessions_files()
    
    await call.message.edit_text(f"✅ **Очистка завершена.**\n"
                                 f"1. Удалены старые drop-сессии и неактивные пользователи.\n"
                                 f"2. Удалено **{deleted_files}** неактивных файлов сессий (.session).")

# ... (Остальные админские колбэки: admin_restart_workers, admin_add_promo_start, admin_add_promo_input - без изменений в логике) ...

async def _cleanup_sessions_files() -> int:
    """Исправление 12: Удаляет файлы сессий, не привязанные к активным воркерам."""
    active_user_ids = await db.get_active_telethon_users()
    deleted_count = 0
    
    for filename in os.listdir(SESSION_DIR):
        if not filename.endswith('.session'): continue
        
        try:
            # Парсим user_id из имени файла session_12345.session
            user_id_str = filename.replace('session_', '').replace('.session', '').replace('_temp', '')
            user_id = int(user_id_str)
            
            # Файл активен, если он есть в списке активных ID или это временный файл
            is_active = user_id in active_user_ids
            is_temp = '_temp' in filename
            
            if not is_active and not is_temp:
                os.remove(os.path.join(SESSION_DIR, filename))
                deleted_count += 1
                logger.info(f"Deleted inactive session file: {filename}")

        except ValueError:
            # Игнорируем файлы, которые не соответствуют формату
            continue
        except Exception as e:
            logger.error(f"Error deleting session file {filename}: {e}")

    return deleted_count

# =========================================================================
# VII. MAIN
# =========================================================================

async def periodic_tasks():
    """Фоновая задача для периодических действий (Бэкап, Очистка) (Исправление 6, 12)."""
    while True:
        try:
            # 1. Автоматическая очистка БД и файлов
            await db.cleanup_old_sessions(days=30)
            await _cleanup_sessions_files()

            # 2. Ежедневный бэкап в 04:00 MSK (Исправление 6: Проверка, чтобы избежать множественных бэкапов)
            now_msk = datetime.now(TIMEZONE_MSK)
            
            # Проверяем, что сейчас ровно 04:00 (с запасом 1 минута)
            if now_msk.hour == 4 and 0 <= now_msk.minute < 5:
                 now_str = now_msk.strftime('%Y%m%d_%H%M%S')
                 backup_path = os.path.join(BACKUP_DIR, f"auto_backup_{now_str}.db")
                 dst_db = None
                 buffer = None
                 
                 try:
                    dst_db = await aiosqlite.connect(backup_path)
                    async with aiosqlite.connect(db.db_path) as src_db:
                        await src_db.backup(dst_db)
                        
                    buffer = open(backup_path, 'rb')
                    
                    await tm._notify_admin("✅ Ежедневный автоматический бэкап БД.")
                    await bot.send_document(ADMIN_ID, FSInputFile(buffer, filename=os.path.basename(backup_path)))
                    
                 except Exception as e:
                    logger.error(f"Error during periodic backup: {e.__class__.__name__}: {e}")
                    await tm._notify_admin(f"❌ Критическая ошибка при бэкапе: {e.__class__.__name__}: {e}")
                 finally:
                    if buffer: buffer.close() # Исправление 1
                    if dst_db: await dst_db.close() # Исправление 3
                    
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Error in periodic_tasks: {e.__class__.__name__}: {e}")
            await tm._notify_admin(f"❌ Критическая ошибка в фоновой задаче: {e.__class__.__name__}: {e}")
            
        await asyncio.sleep(3600) # Проверяем каждый час


async def on_startup():
    """Запускает воркеры и фоновые задачи при старте бота."""
    await db.init()
    
    all_users = await db.get_active_telethon_users()
    for uid in all_users:
        sub_end = await db.get_subscription_status(uid)
        if sub_end and sub_end > datetime.now(TIMEZONE_MSK):
            # start_client_task теперь сам обрабатывает ошибки запуска
            asyncio.create_task(tm.start_client_task(uid)) 
        else:
            await db.set_telethon_status(uid, False)
            
    logger.info(f"Workers started for {len(store.premium_users)} active users.")
    await tm._notify_admin(f"✅ Бот запущен! Активных воркеров: {len(store.premium_users)}.")

    periodic_task = asyncio.create_task(periodic_tasks(), name="periodic_admin_tasks")
    store.admin_jobs["periodic_cleanup"] = periodic_task

async def on_shutdown():
    """Останавливает все активные воркеры и фоновые задачи при выключении бота."""
    # 1. Отмена периодических задач
    if "periodic_cleanup" in store.admin_jobs and not store.admin_jobs["periodic_cleanup"].done():
        store.admin_jobs["periodic_cleanup"].cancel()
        logger.info("Periodic admin tasks cancelled.")

    # 2. Остановка воркеров
    workers_to_stop = list(store.active_workers.keys())
    shutdown_tasks = [tm.stop_worker(uid) for uid in workers_to_stop]
    if shutdown_tasks: 
        logger.info(f"Stopping {len(shutdown_tasks)} active workers...")
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)
    
    await tm._notify_admin("💔 Бот успешно остановлен.")

async def main():
    """Основная функция запуска приложения."""
    if not all([BOT_TOKEN, API_ID, API_HASH, ADMIN_ID]):
        logger.critical("Critical: Configuration missing. Check .env file."); sys.exit(1)
        
    # Исправление 5: Правильный синтаксис dp.include_router
    dp.include_router(user_router)
    dp.include_router(drops_router)
    dp.include_router(admin_router)
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot) 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Critical error in main: {e.__class__.__name__}: {e}"); sys.exit(1)
