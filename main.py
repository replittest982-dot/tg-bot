import asyncio
import logging
import logging.handlers
import os
import re
import random
import sys
import aiosqlite
import pytz
import shutil
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Union, Set, Any
from functools import wraps
from io import BytesIO

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types, BaseMiddleware
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, FSInputFile, CallbackQuery, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, AuthKeyUnregisteredError,
    ChatSendForbiddenError, LoginTokenExpiredError
)
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# --- QR/IMAGE ---
import qrcode
from PIL import Image

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# =========================================================================

# --- Ваши данные (прописаны напрямую) ---
BOT_TOKEN = "7868097991:AAGYbOOjiOeKXZoh7-W7zwU_zYG5P3pOCy4"
ADMIN_ID = 123456789  # Замените на реальный ID
API_ID = 29930612
API_HASH = "2690aa8c364b91e47b6da1f90a71f825"
# CHANNEL_ID = -100123456789 # Идентификатор обязательного канала (если нужен)
# --------------------

DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 1.0 
SESSION_DIR = 'sessions'
BACKUP_DIR = 'backups'
RETRY_DELAY = 5 
PROMOCODE_MAX_LENGTH = 30 

# Инициализация папок
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs('data', exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# --- Настройка логирования ---
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
    WAITING_FOR_METHOD = State()
    WAITING_FOR_QR_SCAN = State()
    PHONE = State()
    CODE = State()
    PASSWORD = State()

class PromoStates(StatesGroup):
    WAITING_CODE = State()

class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_phone_change = State()

class AdminStates(StatesGroup):
    waiting_for_promo_data = State()

# --- Utilities ---

def get_session_path(user_id: int, is_temp: bool = False) -> str:
    """Получает путь к файлу сессии Telethon (без расширения)."""
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def to_msk_aware(dt_str: str) -> Optional[datetime]:
    if not dt_str: return None
    try:
        # Учитываем, что формат может быть с долями секунды, но SQLite обычно хранит без
        naive_dt = datetime.strptime(dt_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        return TIMEZONE_MSK.localize(naive_dt)
    except ValueError:
        return None

def get_topic_name_from_message(message: types.Message) -> Optional[str]:
    topic_key = message.message_thread_id if message.message_thread_id else message.chat.id
    return store.pc_monitoring.get(topic_key)

def is_valid_phone(phone: str) -> bool:
    """Проверяет формат номера телефона: +79XXXXXXXXX."""
    return re.match(r'^\+?\d{7,15}$', phone) is not None

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
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM promocodes WHERE code=?", (code.upper(),)) as cursor:
                result = await cursor.fetchone() 
                return dict(result) if result else None

    async def add_promocode(self, code: str, days: int, uses: int) -> bool:
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
        async with aiosqlite.connect(self.db_path) as db:
            cutoff = (datetime.now(TIMEZONE_MSK) - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            
            await db.execute("UPDATE drop_sessions SET status='deleted' WHERE last_status_time < ? AND status IN ('closed', 'slet', 'error')", (cutoff,))
            
            now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
            await db.execute("DELETE FROM users WHERE subscription_end IS NOT NULL AND subscription_end < ? AND telethon_active=0", (now_str,))
            
            await db.commit()
            logger.info("Database cleanup completed.")

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

    async def get_drop_session_by_phone(self, phone: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions WHERE phone=? AND status NOT IN ('closed', 'deleted') ORDER BY start_time DESC LIMIT 1", (phone,)) as cursor:
                result = await cursor.fetchone() 
                return dict(result) if result else None
    
    async def create_drop_session(self, phone: str, pc_name: str, drop_id: int, status: str) -> bool:
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
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.now(TIMEZONE_MSK)
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            
            current_session = await self.get_drop_session_by_phone(old_phone)
            
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
                # Закрываем старую сессию
                await db.execute("UPDATE drop_sessions SET status='closed', last_status_time=? WHERE phone=?", (now_str, old_phone))
                
                # Создаем новую
                success = await self.create_drop_session(new_phone, current_session['pc_name'], current_session['drop_id'], 'замена')
                if not success: 
                    return False
                
                # Обновляем статистику простоя для новой (учитывая старую)
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
            if wait_time > (self.limit / 2):
                # NOTE: answer() только для Message. Для Callback нужно call.answer()
                if isinstance(event, types.Message):
                    await event.answer(f"🚫 **Rate Limit**. Слишком быстро. Подождите {wait_time} сек.")
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

    async def _send_to_bot_user(self, user_id: int, message: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
        try:
            await self.bot.send_message(user_id, message, reply_markup=reply_markup)
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            logger.warning(f"Failed to send message to user {user_id}. Stopping worker. Error: {e.__class__.__name__}: {e}")
            await self.stop_worker(user_id)
        except Exception as e:
            logger.error(f"Unknown error sending message to {user_id}: {e.__class__.__name__}: {e}")

    async def _notify_admin(self, message: str):
        try:
            await self.bot.send_message(ADMIN_ID, f"🚨 **ADMIN ALERT**: {message}")
        except Exception as e:
            logger.error(f"Failed to notify admin {ADMIN_ID}: {e.__class__.__name__}: {e}")
            
    async def start_worker_session(self, user_id: int, client: TelegramClient):
        """Переименовывает временную сессию и запускает постоянный worker."""
        path_perm_base = get_session_path(user_id)
        path_temp_base = get_session_path(user_id, is_temp=True)
        path_perm = path_perm_base + '.session'
        path_temp = path_temp_base + '.session'

        async with store.lock:
            store.temp_auth_clients.pop(user_id, None)

        # 1. Закрываем temp-клиента
        if client:
            try: 
                await client.disconnect()
            except Exception: 
                logger.warning(f"Failed to disconnect temp client {user_id} before rename.")
        
        # 2. Переименовываем
        if os.path.exists(path_temp):
            try:
                if os.path.exists(path_perm):
                    os.remove(path_perm) # Удаляем старую постоянную сессию перед заменой
                os.rename(path_temp, path_perm)
                await self.start_client_task(user_id) 
            except OSError as e:
                logger.error(f"File operation error during session rename for {user_id}: {e}")
                await self._send_to_bot_user(user_id, "❌ Ошибка: Не удалось переименовать файл сессии. Повторите вход.")
            except Exception as e:
                logger.error(f"Unexpected error during start_worker_session for {user_id}: {e}")
                await self._send_to_bot_user(user_id, "❌ Критическая ошибка при запуске. Повторите вход.")
        else:
            await self._send_to_bot_user(user_id, "❌ Ошибка: Временный файл сессии не найден. Повторите вход.")

    async def start_client_task(self, user_id: int):
        """Запускает главный асинхронный таск для Telethon-клиента."""
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
            logger.error(f"Critical error during start_client_task for {user_id}: {e.__class__.__name__}: {e}")
            await self.db.set_telethon_status(user_id, False)
            await self._send_to_bot_user(user_id, f"❌ Критическая ошибка при запуске worker'а. Проверьте данные сессии.")


    async def _run_worker(self, user_id: int):
        """Основная функция Telethon-воркера."""
        path = get_session_path(user_id)
        # Telethon сам добавит .session, если его нет
        client = TelegramClient(path, self.API_ID, self.API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        async with store.lock: store.active_workers[user_id] = client

        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            await self.worker_message_handler(user_id, client, event) # <-- ТЕПЕРЬ ОН ЕСТЬ

        try:
            await client.connect()
            
            if not await client.is_user_authorized():
                raise AuthKeyUnregisteredError('Session expired/invalid.')

            sub_end = await self.db.get_subscription_status(user_id)
            if not sub_end or sub_end <= datetime.now(TIMEZONE_MSK):
                await self._send_to_bot_user(user_id, "⚠️ Ваша подписка истекла. Worker будет отключен.")
                return 
            
            await self.db.set_telethon_status(user_id, True)
            me = await client.get_me()
            await self._send_to_bot_user(user_id, f"🚀 Worker запущен (**@{me.username}**). Подписка до: **{sub_end.strftime('%d.%m.%Y')}**.")
            
            await asyncio.sleep(float('inf'))
            
        except AuthKeyUnregisteredError:
            session_path = path + '.session'
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Пожалуйста, выполните повторный вход.")
            try:
                if os.path.exists(session_path):
                    os.remove(session_path)
            except OSError as e:
                logger.error(f"Failed to remove bad session file for {user_id}: {e}")
        except asyncio.CancelledError:
            logger.info(f"Worker {user_id} task cancelled.")
        except Exception as e:
            error_msg = f"{e.__class__.__name__}: {e}"
            logger.error(f"Worker {user_id} crashed: {error_msg}")
            await self._send_to_bot_user(user_id, f"💔 Worker отключился: {error_msg}.")
        finally:
            await self.stop_worker(user_id)

    async def stop_worker(self, user_id: int):
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

    # --- Worker Message Handler ---
    async def worker_message_handler(self, user_id: int, client: TelegramClient, event: events.NewMessage.Event):
        """Обрабатывает исходящие сообщения (команды .флуд, .пкворк и т.д.)"""
        if not event.text or not event.text.startswith('.'):
            return

        msg = event.text.strip().lower()
        parts = msg.split()
        cmd = parts[0]
        
        # Проверяем, что это одна из разрешенных команд
        allowed_cmds = ('.пкворк', '.флуд', '.стопфлуд', '.лс', '.чекгруппу', '.статус')
        if cmd not in allowed_cmds:
             await event.delete()
             return
        
        chat_id = event.chat_id
        
        if cmd == '.флуд':
            async with store.lock:
                active_flood = any(t.task_type == "flood" for t in store.worker_tasks.get(user_id, {}).values())
            
            if active_flood:
                await client.send_message(chat_id, "⚠️ **Флуд уже запущен!** Используйте `.стопфлуд` перед запуском новой задачи.")
                return 

            try:
                # .флуд 10 0.5 -100123456789 Текст
                count = int(parts[1]); delay = float(parts[2])
                target = parts[3] if len(parts) > 4 else event.chat_id
                text = " ".join(parts[4:])
                if not text: raise ValueError("Нет текста для флуда.")
                
                await self._start_flood_task(user_id, client, chat_id, target, count, delay, text)
                
            except (IndexError, ValueError) as e:
                await client.send_message(chat_id, f"❌ Неверный формат. Использование: `.флуд <кол-во> <задержка> <цель/чат_id> <текст>`. Ошибка: {e.__class__.__name__}")
            except Exception as e:
                await client.send_message(chat_id, f"❌ Ошибка при запуске флуда: {e.__class__.__name__}: {e}")
        
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
            await self._report_status(user_id, client, chat_id) # <-- ДОБАВЛЕН

    # --- Executors (Фабрики для запуска задач) ---
    
    # 1. НЕДОПИСАННАЯ ФАБРИКА-МЕТОД (Исправлено)
    def _flood_executor_factory(self, user_id: int, client: TelegramClient, task_id: str, target: Union[int, str], count: int, delay: float, text: str):
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
                            channel=entity, 
                            filter=ChannelParticipantsSearch(''), 
                            offset=offset, 
                            limit=limit, 
                            hash=0
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
                        if len(participants.participants) < limit and offset >= total_participants: break
                    else: 
                        await self._send_to_bot_user(user_id, f"❌ **{target}**: Сканирование обычных чатов не поддерживается.")
                        break
                         
                    await asyncio.sleep(RETRY_DELAY)
                
                report_content = f"Отчет по сканированию чата: {target}\nВсего участников: {total_participants}\n\n"
                report_content += "\n".join(users_list)
                
                buffer = BytesIO(report_content.encode('utf-8'))
                buffer.name = f"scan_report_{target}_{datetime.now().strftime('%Y%m%d%H%M')}.txt"
                buffer.seek(0)
                
                try:
                    await self.bot.send_document(user_id, BufferedInputFile(buffer.read(), filename=buffer.name))
                    await self._send_to_bot_user(user_id, f"✅ **{target}**: Сканирование завершено. Отчет в файле.")
                except Exception as e:
                    await self._send_to_bot_user(user_id, f"❌ **{target}**: Отчет не отправлен. Ошибка Telegram: {e.__class__.__name__}: {e}")

            except FloodWaitError as e:
                await self._send_to_bot_user(user_id, f"⏳ **{target}**: FloodWait на {e.seconds} сек. Сканирование остановлено.")
            except Exception as e:
                await self._send_to_bot_user(user_id, f"❌ **{target}**: Ошибка сканирования: {e.__class__.__name__}: {e}")
            finally:
                if buffer:
                    buffer.close()
                await self._remove_task(user_id, task_id)
        return executor
    
    # --- Task Runners ---
    
    # 3. ОТСУТСТВУЮЩИЕ _start_* методы (Исправлено)
    def _start_flood_task(self, user_id: int, client: TelegramClient, chat_id: int, target: Union[int, str], count: int, delay: float, text: str):
        task_id = f"fld-{random.randint(1000, 9999)}"
        executor = self._flood_executor_factory(user_id, client, task_id, target, count, delay, text)
        task = asyncio.create_task(executor(), name=f"{task_id}-user-{user_id}")
        async with store.lock:
            store.worker_tasks.setdefault(user_id, {})[task_id] = WorkerTask("flood", task_id, user_id, target, (count, delay, text))
            store.worker_tasks[user_id][task_id].task = task
        asyncio.create_task(client.send_message(chat_id, f"✅ Задача флуда **{task_id}** запущена на **{target}**."))

    def _start_mass_dm_task(self, user_id: int, client: TelegramClient, chat_id: int, content: str, usernames: List[str]):
        task_id = f"dm-{random.randint(1000, 9999)}"
        executor = self._mass_dm_executor_factory(user_id, client, task_id, content, usernames)
        task = asyncio.create_task(executor(), name=f"{task_id}-user-{user_id}")
        async with store.lock:
            store.worker_tasks.setdefault(user_id, {})[task_id] = WorkerTask("mass_dm", task_id, user_id, f"{len(usernames)} users", (content, usernames))
            store.worker_tasks[user_id][task_id].task = task
        asyncio.create_task(client.send_message(chat_id, f"✅ Задача рассылки **{task_id}** запущена ({len(usernames)} целей)."))
        
    def _start_check_group_task(self, user_id: int, client: TelegramClient, chat_id: int, target: Union[int, str]):
        task_id = f"chk-{random.randint(1000, 9999)}"
        executor = self._check_group_executor_factory(user_id, client, task_id, target)
        task = asyncio.create_task(executor(), name=f"{task_id}-user-{user_id}")
        async with store.lock:
            store.worker_tasks.setdefault(user_id, {})[task_id] = WorkerTask("check_group", task_id, user_id, target)
            store.worker_tasks[user_id][task_id].task = task
        asyncio.create_task(client.send_message(chat_id, f"✅ Задача сканирования **{task_id}** запущена на **{target}**."))


    async def _stop_tasks_by_type(self, user_id: int, task_type: str) -> int:
        stopped_count = 0
        tasks_to_cancel = {}

        async with store.lock:
            if user_id in store.worker_tasks:
                for task_id, worker_task in list(store.worker_tasks[user_id].items()):
                    if worker_task.task_type == task_type:
                        if worker_task.task and not worker_task.task.done():
                            worker_task.task.cancel()
                            stopped_count += 1
                        tasks_to_cancel[task_id] = worker_task

                for task_id in tasks_to_cancel:
                    store.worker_tasks[user_id].pop(task_id, None)

        return stopped_count
        
    async def _remove_task(self, user_id: int, task_id: str):
        async with store.lock:
            if user_id in store.worker_tasks and task_id in store.worker_tasks[user_id]:
                store.worker_tasks[user_id].pop(task_id)
                logger.info(f"Task {task_id} for user {user_id} removed from storage.")
                
    # 3. ОТСУТСТВУЮЩИЙ _report_status метод (Исправлено)
    async def _report_status(self, user_id: int, client: TelegramClient, chat_id: int):
        task_list = []
        async with store.lock:
            tasks = store.worker_tasks.get(user_id, {})
            task_list = [str(t) for t in tasks.values()]

        status_text = f"⚙️ **Worker Status** ⚙️\n"
        try:
            me = await client.get_me()
            status_text += f"**Аккаунт:** @{me.username}\n"
        except Exception:
            status_text += "**Аккаунт:** Не авторизован (ошибка).\n"
            
        status_text += f"**Активные задачи:** {len(task_list)}\n\n"

        if task_list:
            status_text += "--- **Задачи** ---\n" + "\n".join(task_list)
        else:
            status_text += "Нет активных задач."

        await client.send_message(chat_id, status_text)


tm = TelethonManager(bot, db)

# =========================================================================
# VI. HANDLERS (USERS, DROPS, ADMIN)
# =========================================================================

# --- CODE INPUT UTILITY (Inline-клавиатура для ввода кода) ---

def get_code_keyboard(current_code: str) -> InlineKeyboardMarkup:
    """Генерирует Inline-клавиатуру для ввода кода подтверждения."""
    digits_map = {
        '1': "1️⃣", '2': "2️⃣", '3': "3️⃣", '4': "4️⃣", '5': "5️⃣", 
        '6': "6️⃣", '7': "7️⃣", '8': "8️⃣", '9': "9️⃣", '0': "0️⃣"
    }
    
    kb_rows = []
    
    # 7 8 9
    kb_rows.append([
        InlineKeyboardButton(text=digits_map['7'], callback_data="code_input_7"),
        InlineKeyboardButton(text=digits_map['8'], callback_data="code_input_8"),
        InlineKeyboardButton(text=digits_map['9'], callback_data="code_input_9"),
    ])
    # 4 5 6
    kb_rows.append([
        InlineKeyboardButton(text=digits_map['4'], callback_data="code_input_4"),
        InlineKeyboardButton(text=digits_map['5'], callback_data="code_input_5"),
        InlineKeyboardButton(text=digits_map['6'], callback_data="code_input_6"),
    ])
    # 1 2 3
    kb_rows.append([
        InlineKeyboardButton(text=digits_map['1'], callback_data="code_input_1"),
        InlineKeyboardButton(text=digits_map['2'], callback_data="code_input_2"),
        InlineKeyboardButton(text=digits_map['3'], callback_data="code_input_3"),
    ])
    
    # <- 0 -> (Back, 0, Send)
    kb_rows.append([
        InlineKeyboardButton(text="⬅️ Удалить", callback_data="code_input_del"),
        InlineKeyboardButton(text=digits_map['0'], callback_data="code_input_0"),
        InlineKeyboardButton(text="✅ Отправить", callback_data="code_input_send"),
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=kb_rows)

@user_router.callback_query(F.data.startswith("code_input_"), TelethonAuth.CODE)
async def code_input_callback(call: CallbackQuery, state: FSMContext):
    """
    Обрабатывает ввод цифр и команд через Inline-клавиатуру 
    в состоянии TelethonAuth.CODE.
    """
    user_id = call.from_user.id
    action = call.data.split("_")[-1]
    
    data = await state.get_data()
    current_code = data.get('current_code_input', "")
    new_code = current_code 
    
    # Обработчик цифр
    if action.isdigit():
        if len(current_code) < 10: 
            new_code = current_code + action
        else:
            await call.answer("Код слишком длинный.", show_alert=True)
            return
            
    # Обработчик удаления
    elif action == "del":
        new_code = current_code[:-1]
        
    # Обработчик отправки
    elif action == "send":
        code = current_code
        if not code:
            await call.answer("Введите код.", show_alert=True)
            return

        # Редактируем сообщение перед вызовом основного обработчика
        await call.message.edit_text(f"⏳ **Отправка кода: `{code}`...**")
        await call.answer()
        
        # Обновляем состояние перед вызовом
        await state.update_data(current_code_input="") # Очищаем ввод
        
        # Вызов логики авторизации
        await auth_code_input_from_callback(call, code, state)
        return # Выход, так как дальнейшее обновление не нужно

    # Обновляем состояние FSM
    await state.update_data(current_code_input=new_code)

    # Обновляем сообщение с текущим кодом (только для цифр и удаления)
    try:
        current_code_display = f"`{new_code}`" if new_code else "(введите код)"
        await call.message.edit_text(
            f"🔑 **Введите код подтверждения (Telegram/SMS):**\n\n{current_code_display}",
            reply_markup=get_code_keyboard(new_code)
        )
    except TelegramBadRequest:
        # Если текст не изменился, Aiogram может выдать ошибку, это нормально
        pass
        
    await call.answer()


# Дополнительный асинхронный обработчик, вызываемый из callback
async def auth_code_input_from_callback(call: CallbackQuery, code: str, state: FSMContext):
    """Специальный обработчик для кода, введенного через Inline-клавиатуру."""
    user_id = call.from_user.id
    data = await state.get_data()
    phone = data.get('phone')
    code_hash = data.get('hash')

    async with store.lock:
        client = store.temp_auth_clients.get(user_id)
        if not client:
            await bot.send_message(user_id, "❌ Ошибка клиента. Пожалуйста, начните вход заново (/start).")
            await state.clear()
            return

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=code_hash)
        await call.message.edit_text("✅ **Вход успешен!** Запускаю Worker...")
        await finalize_auth(user_id, client, state)
        await state.clear()

    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await call.message.edit_text("🔒 **Включена двухфакторная аутентификация (2FA).** Введите облачный пароль:")
        
    except FloodWaitError as e:
        await call.message.edit_text(f"⏳ **Telegram Flood Wait.** Слишком много попыток. Повторите попытку через {e.seconds} секунд.")
        await state.set_state(TelethonAuth.PHONE)
    except Exception as e:
        await call.message.edit_text(f"❌ **Неверный код.** Пожалуйста, проверьте код и попробуйте снова.\nОшибка: {e.__class__.__name__}\n\n"
                                  f"🔑 **Введите код подтверждения (Telegram/SMS):**",
                                  reply_markup=get_code_keyboard(""))
        await state.update_data(current_code_input="") # Сброс ввода

# --- Core Telethon Auth (TEXT INPUT) ---

async def finalize_auth(user_id: int, client: TelegramClient, state: FSMContext, password: Optional[str] = None):
    """Переименование temp-сессии и запуск worker"""
    
    if password:
        await db.set_password_2fa(user_id, password)
        
    # Вся логика disconnect/rename/start_worker_task перенесена в TelethonManager.start_worker_session
    await tm.start_worker_session(user_id, client)
    
@user_router.message(TelethonAuth.CODE)
async def auth_code_input(message: types.Message, state: FSMContext): 
    """Получает код, завершает вход или запрашивает 2FA пароль (TEXT INPUT)."""
    user_id = message.from_user.id
    data = await state.get_data()
    
    async with store.lock:
        client = store.temp_auth_clients.get(user_id)
        if not client:
             return await message.answer("❌ Сессия истекла. Начните заново.")
    
    try:
        await client.sign_in(phone=data.get('phone'), code=message.text.strip(), phone_code_hash=data.get('hash'))
        await message.answer("✅ **Вход успешен!** Запускаю Worker...")
        await finalize_auth(user_id, client, state) 
        await state.clear()
        
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔒 **Включена двухфакторная аутентификация (2FA).** Введите облачный пароль:")
    except FloodWaitError as e:
        await message.answer(f"⏳ **Telegram Flood Wait.** Слишком много попыток. Повторите попытку через {e.seconds} секунд.")
        await state.set_state(TelethonAuth.PHONE)
    except Exception as e:
        await message.answer(f"❌ **Неверный код.** Пожалуйста, проверьте код и попробуйте снова.\nОшибка: {e.__class__.__name__}\n\n"
                             f"🔑 **Введите код подтверждения (Telegram/SMS):**")

@user_router.message(TelethonAuth.PASSWORD)
async def auth_password_input(message: types.Message, state: FSMContext):
    """Получает 2FA пароль и завершает вход (TEXT INPUT)."""
    user_id = message.from_user.id
    password = message.text.strip()
    
    async with store.lock:
        client = store.temp_auth_clients.get(user_id)
        if not client: return await message.answer("❌ Сессия истекла.")
    
    try:
        await client.sign_in(password=password)
        await message.answer("✅ **2FA-пароль принят.** Вход успешен! Запускаю Worker...")
        await finalize_auth(user_id, client, state, password=password)
        await state.clear()
        
    except Exception as e:
        await message.answer(f"❌ **Неверный 2FA-пароль.** Пожалуйста, проверьте и попробуйте снова.\nОшибка: {e.__class__.__name__}")
        # Остаемся в стейте PASSWORD
        

# --- Users Handler (3. ОТСУТСТВУЮЩИЙ Aiogram handler) ---

@user_router.callback_query(F.data == "cmd_start")
@user_router.message(Command('start'))
async def cmd_start(message: Union[types.Message, CallbackQuery], state: FSMContext):
    is_callback = isinstance(message, CallbackQuery)
    chat = message.message if is_callback else message
    user_id = message.from_user.id
    
    await state.clear()
    await db.get_user(user_id)
    
    # NOTE: Здесь была бы логика проверки подписки на канал, если бы был определен CHANNEL_ID.
    
    sub_end = await db.get_subscription_status(user_id)
    now = datetime.now(TIMEZONE_MSK)
    status_text = f"✅ **Подписка активна** до: **{sub_end.strftime('%d.%m.%Y')}**." if sub_end and sub_end > now else "❌ **Подписка не активна.**"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Вход Telethon (Выбор метода)", callback_data="auth_method_select")],
        [InlineKeyboardButton(text="🔑 Активировать промокод", callback_data="activate_promo")],
        [InlineKeyboardButton(text="❓ Помощь / Команды", callback_data="show_help")]
    ])
    
    text = f"👋 Добро пожаловать в STATPRO Worker.\n\n{status_text}"
    
    if is_callback:
        # Если это CallbackQuery, редактируем сообщение
        await chat.edit_text(text, reply_markup=kb)
        await message.answer()
    else:
        # Если это Message (команда /start), просто отвечаем
        await message.answer(text, reply_markup=kb)

# --- Вход Telethon: Выбор метода, QR, Phone ---

@user_router.callback_query(F.data == "auth_method_select")
async def auth_method_select(call: CallbackQuery, state: FSMContext):
    """Предлагает выбор между QR и SMS/Паролем."""
    await state.set_state(TelethonAuth.WAITING_FOR_METHOD)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 QR-код (Рекомендуется)", callback_data="auth_qr_start")],
        [InlineKeyboardButton(text="📱 Номер / Пароль (Запасной)", callback_data="auth_phone_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="cmd_start")]
    ])
    await call.message.edit_text("Выберите метод входа в аккаунт Telegram:", reply_markup=kb)
    await call.answer()


@user_router.callback_query(F.data == "auth_qr_start")
async def auth_qr_start(call: CallbackQuery, state: FSMContext):
    """Начинает процесс входа по QR-коду."""
    await call.answer("Генерирую QR-код...")
    user_id = call.from_user.id
    path_temp_base = get_session_path(user_id, is_temp=True)
    
    async with store.lock:
        old_client = store.temp_auth_clients.pop(user_id, None)
    if old_client:
        try: await old_client.disconnect()
        except Exception: pass
        
    try:
        temp_session_file = path_temp_base + '.session'
        if os.path.exists(temp_session_file):
            os.remove(temp_session_file)
    except OSError as e:
        logger.error(f"Failed to remove old temp session file for {user_id}: {e}")

    client = TelegramClient(path_temp_base, API_ID, API_HASH)
    async with store.lock:
        store.temp_auth_clients[user_id] = client

    qr_image_bytes = None
    qr_login_obj = None
    
    await call.message.edit_text("⏳ **Генерация QR...**")


    try:
        await client.connect()
        
        qr_login_obj = await client.qr_login()
        
        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
        qr.add_data(qr_login_obj.url)
        qr.make(fit=True)

        img = qr.make_image(fill_color="black", back_color="white").convert('RGB')
        
        qr_image_bytes = BytesIO()
        img.save(qr_image_bytes, format='PNG')
        qr_image_bytes.seek(0)
        
        caption = (
            "📸 **Вход по QR-коду**\n\n"
            "Пожалуйста, **отсканируйте** этот QR-код с помощью вашего основного приложения Telegram:\n"
            "Настройки → Устройства → Привязать Desktop/Устройство.\n\n"
            "⚠️ Код истечет через **60 секунд**. Если истечет, нажмите 'Получить новый'."
        )
        
        sent_photo = await call.message.answer_photo(
            photo=BufferedInputFile(qr_image_bytes.read(), filename="qr_code.png"),
            caption=caption,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Получить новый QR-код", callback_data="auth_qr_start")],
                [InlineKeyboardButton(text="📱 Войти по номеру", callback_data="auth_phone_start")]
            ])
        )
        
        try:
             await call.message.delete()
        except:
            pass 
        
        await state.set_state(TelethonAuth.WAITING_FOR_QR_SCAN)
        
        await qr_login_obj.wait(timeout=60)

        await bot.send_message(user_id, "✅ **QR-код успешно отсканирован!**\n\nЗапускаю Worker...")
        await tm.start_worker_session(user_id, client)
        await state.clear()
        
    except LoginTokenExpiredError:
        await bot.send_message(user_id, "❌ **Время действия QR-кода истекло.** Пожалуйста, нажмите 'Получить новый QR-код'.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Получить новый QR-код", callback_data="auth_qr_start")],
            [InlineKeyboardButton(text="📱 Войти по номеру", callback_data="auth_phone_start")]
        ]))
        await state.clear()
    except SessionPasswordNeededError:
        await bot.send_message(user_id, "⚠️ **Включена двухфакторная аутентификация (2FA).** Вход по QR-коду невозможен.\n"
                                  "Пожалуйста, используйте 'Войти по номеру'.")
        await state.clear()
    except Exception as e:
        logger.error(f"Error during QR login for {user_id}: {e.__class__.__name__}: {e}")
        await bot.send_message(user_id, f"❌ Критическая ошибка при входе по QR-коду: {e.__class__.__name__}", reply_markup=None)
        await state.clear()
    finally:
        if qr_image_bytes:
            qr_image_bytes.close()
        
        if qr_login_obj is None or not await client.is_user_authorized():
            async with store.lock:
                store.temp_auth_clients.pop(user_id, None)
            if client:
                try: await client.disconnect()
                except: pass
            
            temp_session_file = path_temp_base + '.session'
            if os.path.exists(temp_session_file):
                try: 
                    os.remove(temp_session_file)
                except OSError as e:
                    logger.error(f"Failed to remove temp session file after failure for {user_id}: {e}")

@user_router.callback_query(F.data == "auth_phone_start")
async def auth_phone_start(call: CallbackQuery, state: FSMContext):
    """Запрашивает номер телефона."""
    await state.set_state(TelethonAuth.PHONE)
    user_id = call.from_user.id
    path_temp_base = get_session_path(user_id, is_temp=True)
    
    async with store.lock:
        old_client = store.temp_auth_clients.pop(user_id, None)
    if old_client:
        try: await old_client.disconnect()
        except Exception: pass
    
    try:
        temp_session_file = path_temp_base + '.session'
        if os.path.exists(temp_session_file):
            os.remove(temp_session_file)
    except OSError as e:
        logger.error(f"Failed to remove old temp session file for {user_id}: {e}")
            
    await call.message.edit_text(
        "📞 **Введите номер телефона** (в международном формате, например: **+79001234567**):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="auth_method_select")]
        ])
    )
    await call.answer()

@user_router.message(TelethonAuth.PHONE)
async def auth_phone_input(message: types.Message, state: FSMContext):
    """Получает номер, отправляет код."""
    phone = message.text.strip()
    user_id = message.from_user.id
    
    if not is_valid_phone(phone):
        await message.answer("❌ Неверный формат номера. Пожалуйста, введите номер в формате **+79001234567**.")
        return

    path_temp_base = get_session_path(user_id, is_temp=True)
    client = TelegramClient(path_temp_base, API_ID, API_HASH)
    async with store.lock:
        store.temp_auth_clients[user_id] = client

    try:
        await client.connect()
        if await client.is_user_authorized():
            await message.answer("✅ Вы уже авторизованы! Запускаю worker...")
            await tm.start_worker_session(user_id, client)
            await state.clear()
            return

        sent_code = await client.send_code_request(phone)
        
        # Обновляем состояние FSM
        await state.update_data(phone=phone, hash=sent_code.phone_code_hash, current_code_input="")
        await state.set_state(TelethonAuth.CODE)
        
        await message.answer(
            f"🔑 **Код отправлен** на номер **`{phone}`** (проверьте Telegram или SMS).\n\n"
            "**Введите код подтверждения:**",
            reply_markup=get_code_keyboard("")
        )

    except PhoneNumberInvalidError:
        await message.answer("❌ **Неверный номер телефона.** Пожалуйста, проверьте и попробуйте снова.")
        await state.set_state(TelethonAuth.PHONE)
    except FloodWaitError as e:
        await message.answer(f"⏳ **Telegram Flood Wait.** Повторите попытку через {e.seconds} секунд.")
        await state.set_state(TelethonAuth.PHONE)
    except Exception as e:
        logger.error(f"Error during phone auth for {user_id}: {e.__class__.__name__}: {e}")
        await message.answer(f"❌ Критическая ошибка: {e.__class__.__name__}")
        await state.clear()


# --- Promo Code Handlers ---

@user_router.callback_query(F.data == "activate_promo")
async def activate_promo_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.WAITING_CODE)
    await call.message.edit_text("🔑 Введите **промокод**:")
    await call.answer()

@user_router.message(PromoStates.WAITING_CODE)
async def process_promo_code(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    user_id = message.from_user.id
    
    promocode = await db.get_promocode(code)
    
    if not promocode:
        await message.answer("❌ **Промокод не найден.** Пожалуйста, проверьте ввод.")
        return
        
    if promocode['uses_left'] <= 0:
        await message.answer("❌ **Промокод истек** (нет доступных использований).")
        return

    if not await db.use_promocode(code):
         await message.answer("❌ **Произошла ошибка при использовании промокода.** Попробуйте позже.")
         return

    new_end = await db.update_subscription(user_id, promocode['duration_days'])
    
    await message.answer(
        f"🎉 **Промокод `{code}` успешно активирован!**\n"
        f"➕ Добавлено {promocode['duration_days']} дней подписки.\n"
        f"✅ Новая дата окончания: **{new_end.strftime('%d.%m.%Y')}**."
    )
    
    await state.clear()
    await cmd_start(message, state)


# --- Drop Handlers (Заглушки) ---

@drops_router.message(Command('povt'))
async def cmd_povt(message: types.Message):
    """
    /povt <старый_номер> <новый_номер>
    Изменение номера дропа (повторное использование сессии).
    """
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("❌ **Формат:** `/povt +79001112233 +79004445566`")
    
    old_phone = parts[1]
    new_phone = parts[2]
    
    if not is_valid_phone(old_phone) or not is_valid_phone(new_phone):
        return await message.answer("❌ Оба номера должны быть в формате **+7XXXXXXXXXX**.")
        
    # Проверка существования активной сессии для старого номера
    session = await db.get_drop_session_by_phone(old_phone)
    if not session:
        return await message.answer(f"❌ Активная сессия для номера **{old_phone}** не найдена.")
        
    # Проверка на дубликат нового номера
    new_session_check = await db.get_drop_session_by_phone(new_phone)
    if new_session_check:
        return await message.answer(f"❌ Номер **{new_phone}** уже используется в активной сессии (Статус: {new_session_check['status']}).")
        
    # Обновление в БД
    if await db.update_drop_status(old_phone, 'замена', new_phone):
        await message.answer(f"✅ **Успешно!** Номер **{old_phone}** заменен на **{new_phone}**.\n"
                             f"Статус старой сессии: `closed`.\n"
                             f"Создана новая сессия: `замена`.")
    else:
        await message.answer(f"❌ **Ошибка при обновлении** базы данных.")

@drops_router.message(Command('slet'))
async def cmd_slet(message: types.Message):
    """
    /slet <номер>
    Отметка сессии как "слет" (блокировка).
    """
    parts = message.text.split()
    if len(parts) != 2:
        return await message.answer("❌ **Формат:** `/slet +79001112233`")
    
    phone = parts[1]
    
    if not is_valid_phone(phone):
        return await message.answer("❌ Номер должен быть в формате **+7XXXXXXXXXX**.")
        
    session = await db.get_drop_session_by_phone(phone)
    if not session:
        return await message.answer(f"❌ Активная сессия для номера **{phone}** не найдена.")

    # Обновление в БД
    if await db.update_drop_status(phone, 'slet'):
        await message.answer(f"✅ **Успешно!** Статус сессии для **{phone}** изменен на `slet`.")
    else:
        await message.answer(f"❌ **Ошибка при обновлении** базы данных.")

# --- Admin Handlers ---

def admin_only(handler):
    """Декоратор для проверки прав администратора."""
    @wraps(handler)
    async def wrapper(message_or_call: Union[Message, CallbackQuery], *args, **kwargs):
        if message_or_call.from_user.id != ADMIN_ID:
            if isinstance(message_or_call, Message):
                await message_or_call.answer("❌ У вас нет прав администратора.")
            else:
                await message_or_call.answer("❌ У вас нет прав администратора.")
            return
        return await handler(message_or_call, *args, **kwargs)
    return wrapper

@admin_router.message(Command('admin'))
@admin_only
async def cmd_admin(message: types.Message):
    stats = await db.get_stats()
    stats_text = (
        f"👑 **Админ-панель**\n\n"
        f"**📊 Статистика:**\n"
        f"👤 Всего пользователей: {stats['total_users']}\n"
        f"⚙️ Активных воркеров: {stats['active_workers']}\n"
        f"💧 Активных дропов (записи): {stats['active_drops']}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="🧹 Очистить старые сессии", callback_data="admin_cleanup")]
    ])
    await message.answer(stats_text, reply_markup=kb)

@admin_router.callback_query(F.data == "admin_create_promo")
@admin_only
async def admin_create_promo_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_promo_data)
    await call.message.edit_text("🎁 Введите данные промокода в формате:\n\n`КОД_ДНЕЙ_ИСПОЛЬЗОВАНИЙ`\n\nПример: `TESTPROMO_30_10`")
    await call.answer()

@admin_router.message(AdminStates.waiting_for_promo_data)
@admin_only
async def admin_create_promo_process(message: types.Message, state: FSMContext):
    try:
        parts = message.text.strip().split('_')
        if len(parts) != 3:
            raise ValueError("Неверное количество параметров.")
            
        code = parts[0].upper()
        days = int(parts[1])
        uses = int(parts[2])
        
        if not code or days <= 0 or uses <= 0:
            raise ValueError("Неверные значения.")

        if await db.add_promocode(code, days, uses):
            await message.answer(f"✅ **Промокод `{code}` успешно создан:**\n"
                                 f"  * Длительность: {days} дней\n"
                                 f"  * Использований: {uses} раз")
        else:
            await message.answer(f"❌ **Промокод `{code}` уже существует.**")

    except ValueError as e:
        await message.answer(f"❌ **Ошибка формата.** Используйте: `КОД_ДНЕЙ_ИСПОЛЬЗОВАНИЙ`. Дни/использования должны быть числами > 0. {e}")
    except Exception as e:
        await message.answer(f"❌ Критическая ошибка: {e.__class__.__name__}")
        logger.error(f"Admin promo creation error: {e}")
        
    await state.clear()
    await cmd_admin(message)
    
@admin_router.callback_query(F.data == "admin_cleanup")
@admin_only
async def admin_cleanup(call: CallbackQuery):
    try:
        await db.cleanup_old_sessions(days=30)
        await call.answer("✅ Очистка старых сессий завершена (30+ дней).", show_alert=True)
        await cmd_admin(call)
    except Exception as e:
        await call.answer(f"❌ Ошибка при очистке: {e.__class__.__name__}", show_alert=True)
        logger.error(f"Admin cleanup error: {e}")


# =========================================================================
# VII. MAIN LOOP И ОЧИСТКА
# =========================================================================

async def periodic_tasks():
    """Фоновая задача для очистки БД и запуска воркеров."""
    await asyncio.sleep(5) 
    await db.init()
    
    while True:
        try:
            await db.cleanup_old_sessions(days=30)
        except Exception as e:
            logger.error(f"Error during periodic cleanup: {e}")
            
        active_users = await db.get_active_telethon_users()
        for user_id in active_users:
            if user_id not in store.active_workers:
                logger.info(f"Restarting worker for user {user_id}...")
                session_path = get_session_path(user_id) + '.session'
                if os.path.exists(session_path):
                     asyncio.create_task(tm.start_client_task(user_id))
                else:
                    logger.warning(f"Session file not found for user {user_id}. Skipping restart.")
                    await db.set_telethon_status(user_id, False)

        await asyncio.sleep(3600)
    
async def main():
    """Основная точка входа в приложение."""
    dp.include_routers(admin_router, user_router, drops_router)
    
    # Запуск фоновых задач
    asyncio.create_task(periodic_tasks())
    
    logger.info("🚀 Bot запущен!")
    # Запуск бота
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        # Простая проверка, что ID/TOKEN не остались дефолтными
        if ADMIN_ID == 123456789:
            print("WARNING: ADMIN_ID is set to default (123456789). Please change it in the code.")
            
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutdown via KeyboardInterrupt.")
    except Exception as e:
        logger.critical(f"Unhandled error in main execution: {e}")
