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
# ИСПРАВЛЕНИЕ ИМПОРТОВ: Заменены устаревшие ошибки на актуальные
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, AuthKeyUnregisteredError,
    ChatWriteForbiddenError, # Вместо ChatSendForbiddenError
    UserIsBlockedError, PeerIdInvalidError, UsernameInvalidError,
    UserNotMutualContactError
)
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# --- QR/IMAGE ---
import qrcode
from PIL import Image

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# =========================================================================

# --- ВАШИ ДАННЫЕ (ОБНОВЛЕНО) ---
BOT_TOKEN = "7868097991:AAHGGLFnzEiL4h9aS2mkULvMvdIw8yLi9vE" # <-- НОВЫЙ ТОКЕН
ADMIN_ID = 6256576302
API_ID = 29930612
API_HASH = "2690aa8c364b91e47b6da1f90a71f825"
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
        self.progress: Dict[str, Any] = {'sent': 0, 'total': 0, 'processed_count': 0} # Инициализация прогресса

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
        self.code_input_state: Dict[int, str] = {} 

store = GlobalStorage()

# --- FSM States ---

class TelethonAuth(StatesGroup):
    WAITING_FOR_METHOD = State()
    WAITING_FOR_QR_SCAN = State()
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    QR_PASSWORD = State()

class PromoStates(StatesGroup):
    WAITING_CODE = State()

class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_report_phone = State() # Добавлен стейт для отчета

class AdminStates(StatesGroup):
    waiting_for_promo_data = State()
    waiting_for_sub_user_id = State()
    waiting_for_sub_days = State()

# --- Utilities ---

def get_session_path(user_id: int, is_temp: bool = False) -> str:
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def to_msk_aware(dt_str: str) -> Optional[datetime]:
    if not dt_str: return None
    try:
        naive_dt = datetime.strptime(dt_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        return TIMEZONE_MSK.localize(naive_dt)
    except ValueError:
        return None

def get_topic_name_from_message(message: types.Message) -> Optional[str]:
    # ИСПРАВЛЕНИЕ: Используем глобальное хранилище для получения имени ПК
    topic_key = message.message_thread_id if message.message_thread_id else message.chat.id
    return store.pc_monitoring.get(topic_key)

def is_valid_phone(phone: str) -> bool:
    return re.match(r'^\+?\d{7,15}$', phone) is not None

def is_valid_username(username: str) -> bool:
    return username.startswith('@') and len(username) > 1

# =========================================================================
# III. БАЗА ДАННЫХ
# =========================================================================

class AsyncDatabase:
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
                    subscription_active BOOLEAN DEFAULT 0, -- Добавлено поле
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
            
            # Таблица promo_codes (старое название, для совместимости если была)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    days INTEGER,
                    is_active BOOLEAN DEFAULT 1,
                    max_uses INTEGER,
                    current_uses INTEGER DEFAULT 0
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
    
    async def get_subscription_details(self, user_id: int) -> tuple[bool, Optional[datetime]]:
        # Функция возвращает (активна_ли_подписка, дата_окончания)
        user = await self.get_user(user_id)
        if user_id == ADMIN_ID: return True, None
        
        if not user: return False, None
        
        end_str = user['subscription_end']
        if not end_str: return False, None
        
        end_date = to_msk_aware(end_str)
        if not end_date: return False, None
        
        is_active = end_date > datetime.now(TIMEZONE_MSK)
        return is_active, end_date

    async def get_subscription_status(self, user_id: int) -> Optional[datetime]:
        # Для совместимости
        active, end = await self.get_subscription_details(user_id)
        return end

    async def check_subscription(self, user_id: int) -> bool:
        active, _ = await self.get_subscription_details(user_id)
        return active

    async def update_subscription(self, user_id: int, days: int):
        async with aiosqlite.connect(self.db_path) as db:
            active, current_end = await self.get_subscription_details(user_id)
            now = datetime.now(TIMEZONE_MSK)
            
            if active and current_end:
                new_end = current_end + timedelta(days=days)
            else:
                new_end = now + timedelta(days=days)
            
            await db.execute("UPDATE users SET subscription_end=?, subscription_active=1 WHERE user_id=?", (new_end.strftime('%Y-%m-%d %H:%M:%S'), user_id))
            await db.commit()
            return new_end
    
    async def set_subscription_status(self, user_id: int, status: bool, end_date: Optional[datetime]):
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S') if end_date else None
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET subscription_active=?, subscription_end=? WHERE user_id=?", (1 if status else 0, end_str, user_id))
            await db.commit()

    async def get_promocode(self, code: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            # Проверяем обе таблицы для надежности
            async with db.execute("SELECT * FROM promocodes WHERE code=?", (code.upper(),)) as cursor:
                res = await cursor.fetchone()
                if res: return dict(res)
            
            # Fallback к promo_codes
            async with db.execute("SELECT * FROM promo_codes WHERE code=?", (code.upper(),)) as cursor:
                res = await cursor.fetchone()
                if res:
                    d = dict(res)
                    # Нормализуем ключи
                    return {'code': d['code'], 'duration_days': d['days'], 'uses_left': d['max_uses'] - d['current_uses']}
                return None

    async def add_promocode(self, code: str, days: int, uses: int) -> bool:
        if len(code) > PROMOCODE_MAX_LENGTH: return False
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("INSERT INTO promocodes (code, duration_days, uses_left) VALUES (?, ?, ?)", (code.upper(), days, uses))
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
            async with db.execute("SELECT COUNT(user_id) FROM users WHERE telethon_active=1",) as cursor:
                stats['active_workers'] = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(phone) FROM drop_sessions WHERE status NOT IN ('closed', 'deleted')") as cursor:
                stats['active_drops'] = (await cursor.fetchone())[0]
            return stats
    
    async def use_promocode(self, code: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            # Сначала пробуем таблицу promocodes
            promo = await self.get_promocode(code)
            if not promo: return False
            
            # Проверяем какую таблицу обновлять
            try:
                # Попытка обновления promocodes
                await db.execute("UPDATE promocodes SET uses_left=uses_left-1 WHERE code=? AND uses_left>0", (code.upper(),))
                if db.total_changes > 0:
                    await db.commit()
                    return True
            except: pass
            
            # Попытка обновления promo_codes
            try:
                await db.execute("UPDATE promo_codes SET current_uses=current_uses+1 WHERE code=? AND current_uses < max_uses", (code.upper(),))
                if db.total_changes > 0:
                    await db.commit()
                    return True
            except: pass
            
            return False
            
    async def cleanup_old_sessions(self, days: int = 30):
        async with aiosqlite.connect(self.db_path) as db:
            cutoff = (datetime.now(TIMEZONE_MSK) - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            await db.execute("UPDATE drop_sessions SET status='deleted' WHERE last_status_time < ? AND status IN ('closed', 'slet', 'error')", (cutoff,))
            await db.commit()

    async def set_telethon_status(self, user_id: int, status: bool):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            await db.commit()
    
    async def get_active_telethon_users(self) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            # Получаем всех, у кого telethon_active=1
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1") as cursor:
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
    
    async def get_last_drop_session(self, drop_id: int, pc_name: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions WHERE drop_id=? AND pc_name=? ORDER BY start_time DESC LIMIT 1", (drop_id, pc_name)) as cursor:
                result = await cursor.fetchone() 
                return dict(result) if result else None

    async def create_drop_session(self, phone: str, pc_name: str, drop_id: int, status: str) -> bool:
        now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        current = await self.get_drop_session_by_phone(phone)
        if current and phone != 'N/A': # Разрешаем N/A дубликаты (они временные)
            return False 
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("INSERT INTO drop_sessions (phone, pc_name, drop_id, status, start_time, last_status_time) VALUES (?, ?, ?, ?, ?, ?)", 
                                (phone, pc_name, drop_id, status, now_str, now_str))
                await db.commit()
                return True
        except aiosqlite.IntegrityError:
            return False

    async def update_drop_status_by_phone(self, phone: str, new_status: str, new_phone: Optional[str] = None) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.now(TIMEZONE_MSK)
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            
            current_session = await self.get_drop_session_by_phone(phone)
            if not current_session: return False
                
            old_time = to_msk_aware(current_session.get('last_status_time')) or now
            time_diff = int((now - old_time).total_seconds())
            prosto_seconds = current_session.get('prosto_seconds', 0)

            is_prosto_status = current_session['status'] in ('дайте номер', 'error', 'slet', 'повтор')
            if is_prosto_status:
                prosto_seconds += time_diff
            
            if new_phone and new_phone != phone:
                await db.execute("UPDATE drop_sessions SET status='closed', last_status_time=? WHERE phone=?", (now_str, phone))
                success = await self.create_drop_session(new_phone, current_session['pc_name'], current_session['drop_id'], 'замена')
                if not success: return False
                await db.execute("UPDATE drop_sessions SET prosto_seconds=?, last_status_time=? WHERE phone=?", (prosto_seconds, now_str, new_phone))
            else:
                query = "UPDATE drop_sessions SET status=?, last_status_time=?, prosto_seconds=? WHERE phone=?"
                await db.execute(query, (new_status, now_str, prosto_seconds, phone))
            
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

    async def __call__(self, handler: Any, event: types.Message, data: Dict[str, Any]) -> Any:
        user_id = event.from_user.id 
        now = datetime.now()
        last_time = self.user_timestamps.get(user_id)
        if last_time and (now - last_time).total_seconds() < self.limit:
            wait_time = round(self.limit - (now - last_time).total_seconds(), 2)
            if wait_time > (self.limit / 2) and isinstance(event, types.Message):
                await event.answer(f"🚫 Слишком быстро. Подождите {wait_time} сек.")
            return
        self.user_timestamps[user_id] = now
        return await handler(event, data)

dp.message.middleware(SimpleRateLimitMiddleware(limit=RATE_LIMIT_TIME))

# =========================================================================
# V. TELETHON MANAGER
# =========================================================================

class TelethonManager:
    def __init__(self, bot_instance: Bot, db_instance: AsyncDatabase):
        self.bot = bot_instance
        self.db = db_instance
        self.API_ID = API_ID
        self.API_HASH = API_HASH

    async def _send_to_bot_user(self, user_id: int, message: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
        try:
            await self.bot.send_message(user_id, message, reply_markup=reply_markup)
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            logger.warning(f"Failed to send to {user_id}. Stopping worker. Error: {e}")
            await self.stop_worker(user_id)
        except Exception as e:
            logger.error(f"Error sending message to {user_id}: {e}")

    async def start_worker_session(self, user_id: int, client: TelegramClient):
        path_perm_base = get_session_path(user_id)
        path_temp_base = get_session_path(user_id, is_temp=True)
        path_perm = path_perm_base + '.session'
        path_temp = path_temp_base + '.session'

        async with store.lock:
            store.temp_auth_clients.pop(user_id, None)

        if client:
            try: await client.disconnect()
            except Exception: pass
        
        if os.path.exists(path_temp):
            try:
                if os.path.exists(path_perm):
                    os.remove(path_perm)
                os.rename(path_temp, path_perm)
                await self.start_client_task(user_id) 
            except OSError as e:
                logger.error(f"File error renaming session for {user_id}: {e}")
                await self._send_to_bot_user(user_id, "❌ Ошибка файла сессии. Повторите вход.")
        else:
            await self._send_to_bot_user(user_id, "❌ Временный файл сессии не найден.")

    async def start_client_task(self, user_id: int):
        await self.stop_worker(user_id)
        try:
            task = asyncio.create_task(self._run_worker(user_id), name=f"main-worker-{user_id}")
            task_id = f"main-{user_id}"
            async with store.lock: 
                worker_task = WorkerTask(task_type="main", task_id=task_id, creator_id=user_id, target="worker")
                worker_task.task = task
                store.worker_tasks.setdefault(user_id, {})[task_id] = worker_task
                store.premium_users.add(user_id)
            logger.info(f"Main worker started for user {user_id}")
            return task
        except Exception as e:
            logger.error(f"Critical error start_client_task {user_id}: {e}")
            await self.db.set_telethon_status(user_id, False)

    async def _run_worker(self, user_id: int):
        path = get_session_path(user_id)
        client = TelegramClient(path, self.API_ID, self.API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        async with store.lock: store.active_workers[user_id] = client

        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            await self.worker_message_handler(user_id, client, event)

        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise AuthKeyUnregisteredError('Session expired')

            active, sub_end = await self.db.get_subscription_details(user_id)
            if not active:
                await self._send_to_bot_user(user_id, "⚠️ Подписка истекла. Worker отключен.")
                return 
            
            await self.db.set_telethon_status(user_id, True)
            me = await client.get_me()
            await self._send_to_bot_user(user_id, f"🚀 Worker запущен (**@{me.username}**). До: **{sub_end.strftime('%d.%m.%Y')}**.")
            await asyncio.sleep(float('inf'))
            
        except AuthKeyUnregisteredError:
            path_s = path + '.session'
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Выполните вход заново.")
            try: 
                if os.path.exists(path_s): os.remove(path_s)
            except: pass
        except asyncio.CancelledError:
            logger.info(f"Worker {user_id} cancelled.")
        except Exception as e:
            logger.error(f"Worker {user_id} crashed: {e}")
            await self._send_to_bot_user(user_id, f"💔 Worker отключился: {e}.")
        finally:
            await self.stop_worker(user_id)

    async def stop_worker(self, user_id: int):
        async with store.lock:
            client = store.active_workers.pop(user_id, None)
            tasks_to_cancel = store.worker_tasks.pop(user_id, {})
            store.premium_users.discard(user_id)
            for t in tasks_to_cancel.values():
                if t.task and not t.task.done(): t.task.cancel()

        if client:
            try: await client.disconnect()
            except: pass 
        await self.db.set_telethon_status(user_id, False)

    async def worker_message_handler(self, user_id: int, client: TelegramClient, event: events.NewMessage.Event):
        if not event.text or not event.text.startswith('.'): return
        msg = event.text.strip().lower(); parts = msg.split(); cmd = parts[0]
        allowed = ('.пкворк', '.флуд', '.стопфлуд', '.лс', '.чекгруппу', '.статус')
        if cmd not in allowed: 
            await event.delete()
            return
        
        chat_id = event.chat_id
        
        if cmd == '.флуд':
            async with store.lock:
                active = any(t.task_type == "flood" for t in store.worker_tasks.get(user_id, {}).values())
            if active:
                await client.send_message(chat_id, "⚠️ Флуд уже запущен! Используйте .стопфлуд")
                return
            try:
                count = int(parts[1]); delay = float(parts[2]); target = parts[3] if len(parts) > 4 else event.chat_id
                text = " ".join(parts[4:])
                if not text: raise ValueError
                await self._start_flood_task(user_id, client, chat_id, target, count, delay, text)
            except: await client.send_message(chat_id, "❌ Формат: .флуд <кол> <сек> <цель> <текст>")
        
        elif cmd == '.пкворк':
            pc = parts[1] if len(parts) > 1 else 'PC'
            key = event.message.reply_to_msg_id or chat_id
            async with store.lock: store.pc_monitoring[key] = pc
            m = await client.send_message(chat_id, f"✅ ПК для топика установлен: **{pc}**")
            await asyncio.sleep(2); await m.delete()
        
        elif cmd == '.стопфлуд':
            n = await self._stop_tasks_by_type(user_id, "flood")
            await client.send_message(chat_id, f"✅ Остановлено {n} задач.")
            
        elif cmd == '.лс':
            try:
                lines = event.text.split('\n'); content = lines[1]
                users = [l.strip() for l in lines[2:] if is_valid_username(l.strip())]
                if not users: raise ValueError
                await self._start_mass_dm_task(user_id, client, chat_id, content, users)
            except: await client.send_message(chat_id, "❌ Ошибка. Формат:\n.лс\nТекст\n@user1\n@user2")
                
        elif cmd == '.чекгруппу':
            try:
                target = parts[1] if len(parts) > 1 else chat_id
                await self._start_check_group_task(user_id, client, chat_id, target)
            except: await client.send_message(chat_id, "❌ Ошибка. .чекгруппу <цель>")

        elif cmd == '.статус':
            await self._report_status(user_id, client, chat_id)

    def _flood_executor_factory(self, user_id: int, client: TelegramClient, task_id: str, target: Union[int, str], count: int, delay: float, text: str):
        async def executor():
            try:
                for i in range(count):
                    try:
                        await client.send_message(target, text)
                        await asyncio.sleep(delay)
                    except FloodWaitError as e:
                        await self._send_to_bot_user(user_id, f"⏳ FloodWait {e.seconds}s.")
                        await asyncio.sleep(e.seconds)
                    except ChatWriteForbiddenError:
                        await self._send_to_bot_user(user_id, "❌ Запрет отправки.")
                        break
                    except Exception: break
            finally:
                await self._remove_task(user_id, task_id)
        return executor
        
    def _mass_dm_executor_factory(self, user_id: int, client: TelegramClient, task_id: str, content: str, usernames: List[str]):
        async def executor():
            succ = 0
            for u in usernames:
                try:
                    await client.send_message(u, content)
                    succ += 1
                    await asyncio.sleep(1)
                except FloodWaitError as e: await asyncio.sleep(e.seconds)
                except: pass
            await self._send_to_bot_user(user_id, f"✅ Рассылка завершена. Успешно: {succ}")
            await self._remove_task(user_id, task_id)
        return executor

    def _check_group_executor_factory(self, user_id: int, client: TelegramClient, task_id: str, target: Union[int, str]):
        async def executor():
            try:
                entity = await client.get_entity(target)
                if not isinstance(entity, (Channel, Chat)): return
                users = []
                async for u in client.iter_participants(entity):
                    if isinstance(u, User): users.append(f"@{u.username or 'NoUser'} | {u.id} | {u.first_name}")
                
                buf = BytesIO(("\n".join(users)).encode('utf-8'))
                buf.name = f"users_{target}.txt"
                await self.bot.send_document(user_id, BufferedInputFile(buf.read(), filename=buf.name))
            except Exception as e:
                await self._send_to_bot_user(user_id, f"❌ Ошибка скана: {e}")
            finally:
                await self._remove_task(user_id, task_id)
        return executor
    
    async def _start_flood_task(self, uid, cl, cid, tgt, cnt, dly, txt):
        tid = f"fld-{random.randint(1000,9999)}"
        tsk = asyncio.create_task(self._flood_executor_factory(uid, cl, tid, tgt, cnt, dly, txt)())
        async with store.lock: 
            store.worker_tasks.setdefault(uid, {})[tid] = WorkerTask("flood", tid, uid, tgt)
            store.worker_tasks[uid][tid].task = tsk
        asyncio.create_task(cl.send_message(cid, f"✅ Флуд запущен."))

    async def _start_mass_dm_task(self, uid, cl, cid, cnt, usrs):
        tid = f"dm-{random.randint(1000,9999)}"
        tsk = asyncio.create_task(self._mass_dm_executor_factory(uid, cl, tid, cnt, usrs)())
        async with store.lock: 
            store.worker_tasks.setdefault(uid, {})[tid] = WorkerTask("dm", tid, uid, "list")
            store.worker_tasks[uid][tid].task = tsk
        asyncio.create_task(cl.send_message(cid, f"✅ Рассылка запущена."))
        
    async def _start_check_group_task(self, uid, cl, cid, tgt):
        tid = f"chk-{random.randint(1000,9999)}"
        tsk = asyncio.create_task(self._check_group_executor_factory(uid, cl, tid, tgt)())
        async with store.lock: 
            store.worker_tasks.setdefault(uid, {})[tid] = WorkerTask("scan", tid, uid, tgt)
            store.worker_tasks[uid][tid].task = tsk
        asyncio.create_task(cl.send_message(cid, f"✅ Скан запущен."))

    async def _stop_tasks_by_type(self, uid, type):
        c = 0
        async with store.lock:
            for tid, t in list(store.worker_tasks.get(uid, {}).items()):
                if t.task_type == type:
                    if t.task: t.task.cancel()
                    store.worker_tasks[uid].pop(tid)
                    c += 1
        return c
        
    async def _remove_task(self, uid, tid):
        async with store.lock:
            if uid in store.worker_tasks: store.worker_tasks[uid].pop(tid, None)
                
    async def _report_status(self, uid, cl, cid):
        async with store.lock: c = len(store.worker_tasks.get(uid, {}))
        await cl.send_message(cid, f"⚙️ Активных задач: {c}")

tm = TelethonManager(bot, db)

# =========================================================================
# VI. HANDLERS
# =========================================================================

def get_code_keyboard(current_code: str) -> InlineKeyboardMarkup:
    # 1 2 3 ...
    digits = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "0️⃣"]
    rows = []
    rows.append([InlineKeyboardButton(text=d, callback_data=f"code_input_{i+1}") for i, d in enumerate(digits[:3])])
    rows.append([InlineKeyboardButton(text=d, callback_data=f"code_input_{i+4}") for i, d in enumerate(digits[3:6])])
    rows.append([InlineKeyboardButton(text=d, callback_data=f"code_input_{i+7}") for i, d in enumerate(digits[6:9])])
    rows.append([InlineKeyboardButton(text="⬅️", callback_data="code_input_del"), 
                 InlineKeyboardButton(text=digits[9], callback_data="code_input_0"),
                 InlineKeyboardButton(text="✅", callback_data="code_input_send")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@user_router.callback_query(F.data.startswith("code_input_"), TelethonAuth.CODE)
async def code_input_callback(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    action = call.data.split("_")[-1]
    
    async with store.lock:
        current_code = store.code_input_state.get(user_id, "")
        
        if action.isdigit():
            if len(current_code) < 5: 
                store.code_input_state[user_id] = current_code + action
        elif action == "del":
            store.code_input_state[user_id] = current_code[:-1]
        elif action == "send":
            pass 

    # Обновление UI
    new_val = store.code_input_state.get(user_id, "")
    if action == "send":
        if not new_val: 
            await call.answer("Введите код!")
            return
        await call.message.edit_text("⏳ Проверка...")
        await auth_code_input(Message(text=new_val, chat=call.message.chat, from_user=call.from_user, message_id=0, date=datetime.now()), state)
    else:
        try: await call.message.edit_text(f"🔑 Код: `{new_val}`", reply_markup=get_code_keyboard(new_val))
        except: pass
        await call.answer()

@user_router.callback_query(F.data == "cmd_start")
@user_router.message(Command('start'))
async def cmd_start(message: Union[types.Message, CallbackQuery], state: FSMContext):
    chat = message.message if isinstance(message, CallbackQuery) else message
    user_id = message.from_user.id
    await state.clear()
    await db.get_user(user_id)
    
    active, sub_end = await db.get_subscription_details(user_id)
    now = datetime.now(TIMEZONE_MSK)
    st = f"✅ Подписка до: {sub_end.strftime('%d.%m')}" if active and sub_end else "❌ Нет подписки"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Вход", callback_data="auth_method_select")],
        [InlineKeyboardButton(text="🔑 Промокод", callback_data="activate_promo")]
    ])
    
    txt = f"👋 StatPro Bot\n{st}"
    if isinstance(message, CallbackQuery):
        await chat.edit_text(txt, reply_markup=kb)
    else:
        await chat.answer(txt, reply_markup=kb)

@user_router.callback_query(F.data == "auth_method_select")
async def auth_method_select(call: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 QR", callback_data="auth_qr_start")],
        [InlineKeyboardButton(text="📱 SMS", callback_data="auth_phone_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="cmd_start")]
    ])
    await call.message.edit_text("Выберите метод:", reply_markup=kb)

# --- QR AUTH ---
@user_router.callback_query(F.data == "auth_qr_start")
async def auth_qr_start(call: CallbackQuery, state: FSMContext):
    await call.answer("Генерация QR...")
    uid = call.from_user.id
    path = get_session_path(uid, is_temp=True)
    
    client = TelegramClient(path, API_ID, API_HASH)
    async with store.lock: store.temp_auth_clients[uid] = client
    
    try:
        await client.connect()
        qr_login = await client.qr_login()
        qr = qrcode.QRCode(); qr.add_data(qr_login.url); qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        bio = BytesIO(); img.save(bio, format='PNG'); bio.seek(0)
        
        msg = await call.message.answer_photo(
            BufferedInputFile(bio.read(), filename="qr.png"), 
            caption="📸 Сканируйте QR в Telegram > Устройства.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cmd_start")]])
        )
        await call.message.delete()
        
        await qr_login.wait(60)
        await msg.delete()
        await bot.send_message(uid, "✅ Успех! Запуск...")
        await tm.start_worker_session(uid, client)
        
    except Exception as e:
        await bot.send_message(uid, f"Ошибка QR: {e}")
        try: await client.disconnect()
        except: pass

# --- PHONE AUTH ---
@user_router.callback_query(F.data == "auth_phone_start")
async def auth_phone_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("📞 Введите номер (+7...):")

@user_router.message(TelethonAuth.PHONE)
async def auth_phone_input(msg: Message, state: FSMContext):
    phone = msg.text.strip()
    uid = msg.from_user.id
    path = get_session_path(uid, is_temp=True)
    client = TelegramClient(path, API_ID, API_HASH)
    async with store.lock: store.temp_auth_clients[uid] = client
    
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
        await state.update_data(phone=phone, hash=sent.phone_code_hash)
        await state.set_state(TelethonAuth.CODE)
        async with store.lock: store.code_input_state[uid] = ""
        await msg.answer("🔑 Введите код:", reply_markup=get_code_keyboard(""))
    except Exception as e:
        await msg.answer(f"Ошибка: {e}")

@user_router.message(TelethonAuth.CODE)
async def auth_code_input(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    data = await state.get_data()
    async with store.lock: client = store.temp_auth_clients.get(uid)
    if not client: return await msg.answer("Сессия истекла.")
    
    try:
        await client.sign_in(data['phone'], msg.text.strip(), phone_code_hash=data['hash'])
        await msg.answer("✅ Вход выполнен.")
        await tm.start_worker_session(uid, client)
        await state.clear()
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await msg.answer("🔐 Введите 2FA пароль:")
    except Exception as e:
        await msg.answer(f"Ошибка кода: {e}")

@user_router.message(TelethonAuth.PASSWORD)
async def auth_pass(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    async with store.lock: client = store.temp_auth_clients.get(uid)
    try:
        await client.sign_in(password=msg.text.strip())
        await msg.answer("✅ 2FA Принят.")
        await tm.start_worker_session(uid, client)
        await state.clear()
    except Exception as e: await msg.answer(f"Ошибка пароля: {e}")

# --- PROMO ---
@user_router.callback_query(F.data == "activate_promo")
async def promo_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.WAITING_CODE)
    await call.message.edit_text("📝 Введите промокод:")

@user_router.message(PromoStates.WAITING_CODE)
async def promo_proc(msg: Message, state: FSMContext):
    code = msg.text.upper().strip()
    p = await db.get_promocode(code)
    if not p or p['uses_left'] <= 0: return await msg.answer("Неверный/истекший код.")
    if await db.use_promocode(code):
        await db.update_subscription(msg.from_user.id, p['duration_days'])
        await msg.answer(f"✅ Активировано: +{p['duration_days']} дн.")
        await state.clear()

# --- ADMIN ---
def admin_only(func):
    @wraps(func)
    async def wrapper(message, *args, **kwargs):
        uid = message.from_user.id
        if uid != ADMIN_ID: return
        return await func(message, *args, **kwargs)
    return wrapper

@admin_router.message(Command("admin"))
@admin_only
async def admin_panel(msg: Message):
    stats = await db.get_stats()
    t = f"📊 Users: {stats['total_users']} | Workers: {stats['active_workers']}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎁 Add Promo", callback_data="adm_promo")]])
    await msg.answer(t, reply_markup=kb)

@admin_router.callback_query(F.data == "adm_promo")
@admin_only
async def adm_promo_ask(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_promo_data)
    await call.message.edit_text("CODE_DAYS_USES")

@admin_router.message(AdminStates.waiting_for_promo_data)
@admin_only
async def adm_promo_save(msg: Message, state: FSMContext):
    try:
        c, d, u = msg.text.split('_')
        if await db.add_promocode(c.upper(), int(d), int(u)):
            await msg.answer("✅ Created")
        else: await msg.answer("Error")
    except: await msg.answer("Format error")
    await state.clear()

# --- DROPS (IMPLEMENTED) ---
async def get_drop_context(message: Message) -> Optional[Dict]:
    topic_key = get_topic_name_from_message(message)
    if not topic_key:
        await message.answer("❌ .пкворк")
        return None
    return {'drop_id': message.from_user.id, 'pc_name': topic_key}

def format_prosto(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}ч {m}м"

@drops_router.message(Command("numb"))
async def drop_numb(msg: Message, state: FSMContext): 
    ctx = await get_drop_context(msg)
    if not ctx: return
    await state.update_data(**ctx)
    await state.set_state(DropStates.waiting_for_phone_and_pc)
    await msg.answer("📝 Введите номер:")

@drops_router.message(DropStates.waiting_for_phone_and_pc)
async def drop_numb_input(msg: Message, state: FSMContext):
    ph = msg.text.strip()
    data = await state.get_data()
    if await db.create_drop_session(ph, data['pc_name'], data['drop_id'], 'номер'):
        await msg.answer("✅ ОК")
    else: await msg.answer("❌ Занят")
    await state.clear()

async def handle_status(msg: Message, status: str, zm=False):
    try:
        if zm:
            parts = msg.text.split()
            if len(parts) < 3: return await msg.answer("/zm OLD NEW")
            old, new = parts[1], parts[2]
            if await db.update_drop_status_by_phone(old, status, new):
                await msg.answer("✅ Замена ОК")
        else:
            if not msg.reply_to_message: return await msg.answer("Реплай на номер")
            ph = msg.reply_to_message.text.split()[0]
            if await db.update_drop_status_by_phone(ph, status):
                await msg.answer(f"✅ {status}")
    except: pass

@drops_router.message(Command("vstal"))
async def dv(m: Message): await handle_status(m, "в работе")
@drops_router.message(Command("slet"))
async def ds(m: Message): await handle_status(m, "слет")
@drops_router.message(Command("error"))
async def de(m: Message): await handle_status(m, "ошибка")
@drops_router.message(Command("povt"))
async def dp(m: Message): await handle_status(m, "повтор")
@drops_router.message(Command("zm"))
async def dz(m: Message): await handle_status(m, "замена", True)

@drops_router.message(Command("report_last"))
async def dr(msg: Message):
    ctx = await get_drop_context(msg)
    if not ctx: return
    sess = await db.get_last_drop_session(ctx['drop_id'], ctx['pc_name'])
    if sess:
        await msg.answer(f"📊 {sess['phone']} | {sess['status']} | Простой: {format_prosto(sess['prosto_seconds'])}")
    else: await msg.answer("Нет сессий")

# --- MAIN ---
async def periodic_tasks():
    await db.init()
    for uid in await db.get_active_telethon_users():
        asyncio.create_task(tm.start_client_task(uid))
    while True:
        await asyncio.sleep(3600)
        await db.cleanup_old_sessions()

async def main():
    dp.include_routers(admin_router, user_router, drops_router)
    asyncio.create_task(periodic_tasks())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except: pass
