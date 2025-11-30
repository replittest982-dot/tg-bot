import asyncio
import logging
import logging.handlers
import os
import re
import random
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Union, Set, Any, Tuple
from io import BytesIO

# Third-party Imports
import aiosqlite
import pytz
import qrcode
from PIL import Image
from dotenv import load_dotenv

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types, BaseMiddleware
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile, CallbackQuery, ErrorEvent
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramAPIError

# --- TELETHON ---
# ИСПРАВЛЕНО: Убран InputClientQRLogin
from telethon import TelegramClient, events, errors, functions, utils
from telethon.tl.types import User, Channel, Chat # <- ИСПРАВЛЕННАЯ СТРОКА
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, AuthKeyUnregisteredError, ChatForwardsRestrictedError, PasswordHashInvalidError

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# =========================================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 6256576302)) 
API_ID = int(os.getenv("API_ID", 37185453))
API_HASH = os.getenv("API_HASH")

if not BOT_TOKEN or not API_HASH:
    # Эта ошибка сработает, если BOT_TOKEN или API_HASH не заданы в .env
    raise ValueError("BOT_TOKEN или API_HASH не найдены в .env файле.")

DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 0.5  
SESSION_DIR = 'sessions'
DATA_DIR = 'data'
QR_TIMEOUT = 120  
TASK_LIMIT_PER_USER = 5 

os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# --- Настройка логирования ---
def setup_logging(log_file=os.path.join(DATA_DIR, 'bot.log'), level=logging.INFO):
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)
    
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

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
    def __init__(self, task_type: str, task_id: str, creator_id: int, target: Union[int, str]): 
        self.task_type = task_type
        self.task_id = task_id
        self.creator_id = creator_id
        self.target = target
        self.task: Optional[asyncio.Task] = None
        self.start_time: datetime = datetime.now(TIMEZONE_MSK)

    def __str__(self) -> str:
        elapsed = int((datetime.now(TIMEZONE_MSK) - self.start_time).total_seconds())
        return f"[{self.task_type.upper()}] Цель:{self.target} Время: {elapsed} сек."

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {} 
        self.qr_login_future: Dict[int, asyncio.Future] = {} 
        self.pc_monitoring: Dict[Union[int, str], str] = {} 
        self.active_workers: Dict[int, TelegramClient] = {} 
        self.worker_tasks: Dict[int, Dict[str, WorkerTask]] = {} 
        self.premium_users: Set[int] = set() 
        self.code_input_state: Dict[int, str] = {} 

store = GlobalStorage()

# --- FSM States ---
class TelethonAuth(StatesGroup):
    WAITING_FOR_QR_SCAN = State()
    PHONE = State()
    CODE = State()
    PASSWORD = State()

class PromoStates(StatesGroup):
    WAITING_CODE = State()

class AdminStates(StatesGroup):
    waiting_for_promo_data = State()
    waiting_for_broadcast_message = State()

class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_new_phone = State()

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

def is_valid_phone(phone: str) -> bool:
    return re.match(r'^\+?\d{7,15}$', phone) is not None

# =========================================================================
# III. БАЗА ДАННЫХ (Полная реализация методов)
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db_pool: Optional[aiosqlite.Connection] = None
        self.db_lock = asyncio.Lock() 

    async def init(self):
        self.db_pool = await aiosqlite.connect(self.db_path, isolation_level=None, timeout=30.0) 
        await self.db_pool.execute("PRAGMA journal_mode=WAL;")
        await self.db_pool.execute("PRAGMA synchronous=OFF;") 
        await self.db_pool.execute("PRAGMA foreign_keys=ON;")

        await self.db_pool.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, 
                telethon_active BOOLEAN DEFAULT 0,
                subscription_end TEXT,
                is_banned BOOLEAN DEFAULT 0,
                password_2fa TEXT
            )
        """)
        await self.db_pool.execute("INSERT OR IGNORE INTO users (user_id, is_banned) VALUES (?, ?)", (ADMIN_ID, 0))
        await self.db_pool.execute("CREATE INDEX IF NOT EXISTS idx_users_sub_active ON users (subscription_end, telethon_active);")

        await self.db_pool.execute("""
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
        await self.db_pool.execute("CREATE INDEX IF NOT EXISTS idx_drops_status ON drop_sessions (status);")

        await self.db_pool.execute("""
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                duration_days INTEGER,
                uses_left INTEGER
            )
        """)
        await self.db_pool.commit()
        logger.info("Database initialized successfully.")

    async def get_user(self, user_id: int):
        if not self.db_pool: return None
        await self.db_pool.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await self.db_pool.commit()
        self.db_pool.row_factory = aiosqlite.Row
        async with self.db_pool.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
            result = await cursor.fetchone() 
            self.db_pool.row_factory = None
            return dict(result) if result else None
            
    async def get_subscription_status(self, user_id: int) -> Optional[datetime]:
        if not self.db_pool: return None
        async with self.db_pool.execute("SELECT subscription_end FROM users WHERE user_id=?", (user_id,)) as cursor:
            result = await cursor.fetchone() 
            if result and result[0]:
                return to_msk_aware(result[0])
            return None

    async def update_subscription(self, user_id: int, days: int):
        if not self.db_pool: return
        current_end = await self.get_subscription_status(user_id)
        now = datetime.now(TIMEZONE_MSK)
        
        if current_end and current_end > now:
            new_end = current_end + timedelta(days=days)
        else:
            new_end = now + timedelta(days=days)
        
        await self.db_pool.execute("UPDATE users SET subscription_end=? WHERE user_id=?", (new_end.strftime('%Y-%m-%d %H:%M:%S'), user_id))
        await self.db_pool.commit()
        return new_end

    async def get_promocode(self, code: str):
        if not self.db_pool: return None
        self.db_pool.row_factory = aiosqlite.Row
        async with self.db_pool.execute("SELECT * FROM promocodes WHERE code=?", (code.upper(),)) as cursor:
            result = await cursor.fetchone() 
            self.db_pool.row_factory = None
            return dict(result) if result else None
            
    async def use_promocode(self, code: str) -> bool:
        if not self.db_pool: return False
        promocode = await self.get_promocode(code)
        if not promocode or promocode['uses_left'] <= 0:
            return False
        new_uses = promocode['uses_left'] - 1
        await self.db_pool.execute("UPDATE promocodes SET uses_left=? WHERE code=?", (new_uses, code.upper()))
        await self.db_pool.commit()
        return True

    async def set_telethon_status(self, user_id: int, status: bool):
        if not self.db_pool: return
        await self.db_pool.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
        await self.db_pool.commit()
        
    async def ban_user(self, user_id: int, is_banned: bool):
        if not self.db_pool: return
        await self.db_pool.execute("UPDATE users SET is_banned=? WHERE user_id=?", (1 if is_banned else 0, user_id))
        await self.db_pool.commit()

    async def get_active_telethon_users(self) -> List[int]: 
        if not self.db_pool: return []
        now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        async with self.db_pool.execute("SELECT user_id FROM users WHERE telethon_active=1 AND is_banned=0 AND (subscription_end IS NULL OR subscription_end > ?)", (now_str,)) as cursor:
            return [row[0] for row in await cursor.fetchall()]

    async def set_password_2fa(self, user_id: int, password: str):
        if not self.db_pool: return
        await self.db_pool.execute("UPDATE users SET password_2fa=? WHERE user_id=?", (password, user_id))
        await self.db_pool.commit()

    async def get_drop_session_by_phone(self, phone: str):
        if not self.db_pool: return None
        self.db_pool.row_factory = aiosqlite.Row
        async with self.db_pool.execute("SELECT * FROM drop_sessions WHERE phone=? AND status NOT IN ('closed', 'deleted') ORDER BY start_time DESC LIMIT 1", (phone,)) as cursor:
            result = await cursor.fetchone() 
            self.db_pool.row_factory = None
            return dict(result) if result else None

    async def update_drop_status(self, old_phone: str, new_status: str, new_phone: Optional[str] = None) -> bool:
        if not self.db_pool: return False
        now = datetime.now(TIMEZONE_MSK)
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        current_session = await self.get_drop_session_by_phone(old_phone)
        if not current_session: return False
            
        old_time = to_msk_aware(current_session.get('last_status_time')) or now
        time_diff = int((now - old_time).total_seconds())
        prosto_seconds = current_session.get('prosto_seconds', 0) 

        is_prosto_status = current_session['status'] in ('дайте номер', 'error', 'slet')
        if is_prosto_status:
            prosto_seconds += time_diff
        
        query = "UPDATE drop_sessions SET status=?, last_status_time=?, prosto_seconds=? WHERE phone=?"
        await self.db_pool.execute(query, (new_status, now_str, prosto_seconds, old_phone))
        
        await self.db_pool.commit()
        return True

    async def cleanup_old_sessions(self, days: int = 30):
        if not self.db_pool: return
        logger.info(f"Running database cleanup (sessions older than {days} days)...")
        cutoff = (datetime.now(TIMEZONE_MSK) - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        await self.db_pool.execute("UPDATE drop_sessions SET status='deleted' WHERE last_status_time < ? AND status IN ('closed', 'slet', 'error')", (cutoff,))
        now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        await self.db_pool.execute("DELETE FROM users WHERE subscription_end IS NOT NULL AND subscription_end < ? AND telethon_active=0 AND is_banned=0", (now_str,))
        await self.db_pool.commit()
        logger.info("Database cleanup finished.")
        
    async def get_stats(self) -> Dict[str, Any]:
        if not self.db_pool: return {}
        
        async with self.db_pool.execute("SELECT COUNT(user_id) FROM users") as cursor:
            total_users = (await cursor.fetchone())[0]
            
        async with self.db_pool.execute("SELECT COUNT(user_id) FROM users WHERE telethon_active=1 AND is_banned=0") as cursor:
            active_workers_db = (await cursor.fetchone())[0]
            
        async with self.db_pool.execute("SELECT COUNT(phone) FROM drop_sessions WHERE status='active'") as cursor:
            active_drops = (await cursor.fetchone())[0]
            
        async with self.db_pool.execute("SELECT COUNT(phone) FROM drop_sessions") as cursor:
            total_drops = (await cursor.fetchone())[0]

        return {
            'total_users': total_users,
            'active_workers_db': active_workers_db,
            'active_workers_ram': len(store.active_workers), 
            'active_drops': active_drops,
            'total_drops': total_drops,
            'premium_users_ram': len(store.premium_users)
        }


db = AsyncDatabase(os.path.join(DATA_DIR, DB_NAME))

# =========================================================================
# IV. MIDDLEWARE
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
        
        user_data = await db.get_user(user_id)
        if user_data and user_data.get('is_banned', 0):
            if isinstance(event, types.Message):
                await event.answer("🚫 Вы заблокированы администратором.")
            return

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
        self.tasks_lock = asyncio.Lock() 

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
            try:
                if not await client.is_user_authorized():
                    raise AuthKeyUnregisteredError("Client not authorized after session swap.")
                await client.disconnect()
            except Exception as e:
                logger.error(f"Session validation failed for {user_id}: {e}")
                await self._send_to_bot_user(user_id, "❌ Критическая ошибка сессии. Начните вход заново.")
                if os.path.exists(path_temp): os.remove(path_temp)
                return

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
            async with self.tasks_lock:
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
        async with self.tasks_lock: store.active_workers[user_id] = client

        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            await self.worker_message_handler(user_id, client, event)
        
        try:
            await client.connect()
            if not await client.is_user_authorized():
                 raise AuthKeyUnregisteredError('Session expired or not authorized after connect')

            sub_end = await self.db.get_subscription_status(user_id)
            if not sub_end or sub_end <= datetime.now(TIMEZONE_MSK):
                await self._send_to_bot_user(user_id, "⚠️ Подписка истекла. Worker отключен.")
                return 
            
            await self.db.set_telethon_status(user_id, True)
            me = await client.get_me()
            await self._send_to_bot_user(user_id, f"🚀 Worker запущен (**@{me.username}**). До: **{sub_end.strftime('%d.%m.%Y')}**.")
            
            await asyncio.Future() 
            
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
        async with self.tasks_lock:
            client = store.active_workers.pop(user_id, None)
            tasks_to_cancel = store.worker_tasks.pop(user_id, {})
            store.premium_users.discard(user_id)
            for t in tasks_to_cancel.values():
                if t.task and not t.task.done(): 
                    t.task.cancel()
                    logger.info(f"Task {t.task_id} for user {user_id} cancelled.")

        if client:
            try: await client.disconnect()
            except: pass 
        await self.db.set_telethon_status(user_id, False)
        
    async def worker_message_handler(self, user_id: int, client: TelegramClient, event: events.NewMessage.Event): 
        if not event.text or not event.text.startswith('.'): return
        msg = event.text.strip().lower(); parts = msg.split(); cmd = parts[0]
        
        if cmd == '.пкворк':
            pc = parts[1] if len(parts) > 1 else 'PC'
            key = event.message.reply_to_msg_id or event.chat_id
            async with store.lock: store.pc_monitoring[key] = pc 
            m = await client.send_message(event.chat_id, f"✅ ПК для топика установлен: **{pc}**")
            await asyncio.sleep(2); await m.delete()

    async def _get_tasks_list(self, user_id: int) -> List[WorkerTask]: 
        async with self.tasks_lock:
            return list(store.worker_tasks.get(user_id, {}).values())

tm = TelethonManager(bot, db)

# =========================================================================
# VI. HANDLERS (AIOGRAM)
# =========================================================================

# --- Global Error Handler ---
@dp.errors()
async def errors_handler(event: ErrorEvent):
    exc = event.exception
    user_id_obj = getattr(getattr(event.update, 'message', None), 'from_user', None) or \
                  getattr(getattr(event.update, 'callback_query', None), 'from_user', None)
    
    user_info = f"UID:{user_id_obj.id if user_id_obj else 'N/A'}"
    
    if isinstance(exc, TelegramForbiddenError):
        logger.warning(f"🚫 Forbidden {user_info}: Bot was blocked by user.")
    elif isinstance(exc, TelegramBadRequest):
        logger.info(f"⚠️ BadRequest {user_info}: {exc}")
    elif isinstance(exc, TelegramAPIError):
        logger.error(f"🌐 API {user_info}: {exc}")
    else:
        logger.error(f"💥 UNKNOWN ERROR {user_info}: {exc}", exc_info=True)
        
    return True

# --- Utilities ---
def get_code_keyboard(current_code: str) -> InlineKeyboardMarkup:
    digits = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "0️⃣"]
    rows = []
    rows.append([InlineKeyboardButton(text=d, callback_data=f"code_input_{i+1}") for i, d in enumerate(digits[:3])])
    rows.append([InlineKeyboardButton(text=d, callback_data=f"code_input_{i+4}") for i, d in enumerate(digits[3:6])])
    rows.append([InlineKeyboardButton(text=d, callback_data=f"code_input_{i+7}") for i, d in enumerate(digits[6:9])])
    rows.append([InlineKeyboardButton(text="⬅️", callback_data="code_input_del"), 
                 InlineKeyboardButton(text=digits[9], callback_data="code_input_0"),
                 InlineKeyboardButton(text="✅", callback_data="code_input_send")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def send_main_menu(user_id: int, state: FSMContext, message: types.Message = None, call: CallbackQuery = None):
    await state.clear()
    
    user_data = await db.get_user(user_id)
    sub = await db.get_subscription_status(user_id)
    is_active = user_id in store.premium_users
    
    status_text = "🔥 Активен" if is_active else "😴 Неактивен"
    sub_text = f"✅ Подписка до: **{sub.strftime('%d.%m.%Y')}**" if sub and sub > datetime.now(TIMEZONE_MSK) else "❌ Нет подписки"
    
    text = f"⚙️ **Главное меню**\n\nСтатус Worker: **{status_text}**\n{sub_text}"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Вход / Перезапуск Worker", callback_data="auth_start_menu")],
        [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")],
        [InlineKeyboardButton(text="➕ Ввести промокод", callback_data="use_promocode_start")],
        [InlineKeyboardButton(text="❌ Остановить Worker", callback_data="cmd_stop")] if is_active else [InlineKeyboardButton(text="🟢 Запустить Worker", callback_data="cmd_restart")],
        [InlineKeyboardButton(text="🔧 Админ-панель", callback_data="admin_stats")] if user_id == ADMIN_ID else []
    ])

    if call:
        try:
            await call.message.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
             pass
        await call.answer()
    elif message:
        await message.answer(text, reply_markup=kb)

# --- Commands ---

@user_router.message(Command('stop'))
@user_router.callback_query(F.data == "cmd_stop")
async def cmd_stop(update: Union[Message, CallbackQuery], state: FSMContext):
    user_id = update.from_user.id
    if user_id not in store.premium_users:
        text = "⚠️ Worker не запущен."
    else:
        await tm.stop_worker(user_id)
        text = "✅ Worker остановлен."
    
    if isinstance(update, CallbackQuery):
        await update.answer(text, show_alert=True)
        await send_main_menu(user_id, state, call=update)
    else:
        await update.answer(text)

@user_router.message(Command('restart'))
@user_router.callback_query(F.data == "cmd_restart")
async def cmd_restart(update: Union[Message, CallbackQuery], state: FSMContext):
    user_id = update.from_user.id
    path = get_session_path(user_id) + '.session'
    
    if not os.path.exists(path):
        text = "❌ Сессия не найдена. Требуется повторный вход."
    else:
        if isinstance(update, CallbackQuery):
            await update.answer("⏳ Запускаю Worker...")
        await tm.start_client_task(user_id)
        text = "🚀 Worker перезапущен!"

    if isinstance(update, CallbackQuery):
        await asyncio.sleep(1) 
        await send_main_menu(user_id, state, call=update)
    else:
        await update.answer(text)

@user_router.message(Command('start', 'menu'))
@user_router.callback_query(F.data == "cmd_start")
async def cmd_start(update: Union[Message, CallbackQuery], state: FSMContext): 
    user_id = update.from_user.id
    if isinstance(update, CallbackQuery):
        await send_main_menu(user_id, state, call=update)
    else:
        await send_main_menu(user_id, state, message=update)

# --- Drops Router Handlers ---

@drops_router.message(Command('numb'))
async def cmd_numb(message: Message, state: FSMContext):
    if not message.text or len(message.text.split()) < 2:
        return await message.answer("❌ Формат: `/numb +79991234567`")
    
    phone = message.text.split()[1]
    if not is_valid_phone(phone):
        return await message.answer("❌ Неверный формат номера.")
        
    session_data = await db.get_drop_session_by_phone(phone)
    if not session_data:
        return await message.answer(f"❌ Активный дроп сессия для **{phone}** не найдена.")

    success = await db.update_drop_status(phone, 'завершено')
    if success:
        await message.answer(f"✅ Статус дропа для **{phone}** установлен как `завершено`.")
    else:
        await message.answer(f"❌ Не удалось обновить статус дропа для **{phone}**.")

# --- Admin Router Handlers ---

@admin_router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return await call.answer()
    await call.answer("⏳ Загрузка статистики...", show_alert=False)

    stats = await db.get_stats()
    
    active_worker_ids = list(store.active_workers.keys())
    tasks_running = sum(len(tasks) for tasks in store.worker_tasks.values())
    
    text = (
        "📊 **Административная Статистика**\n\n"
        "👥 **Пользователи**\n"
        f" • Всего в DB: **{stats['total_users']}**\n"
        f" • Премиум (RAM): **{stats['premium_users_ram']}**\n"
        f" • Активных Worker'ов (DB): **{stats['active_workers_db']}**\n"
        
        "\n🤖 **Worker'ы и Задачи (RAM)**\n"
        f" • Worker'ов (RAM): **{stats['active_workers_ram']}**\n"
        f" • Активных задач: **{tasks_running}**\n\n"
        
        "💧 **Дропы (Sessions)**\n"
        f" • Всего: **{stats['total_drops']}**\n"
        f" • Активных: **{stats['active_drops']}**\n\n"
        
        "🛠️ **Детали активных Worker'ов (ID):**\n"
        f" `{', '.join(map(str, active_worker_ids)) or 'Нет'}`"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Сгенерировать Промокод", callback_data="cmd_genpromo_start")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="cmd_broadcast_start")],
        [InlineKeyboardButton(text="⬅️ Назад в Меню", callback_data="cmd_start")]
    ])
    
    await call.message.edit_text(text, reply_markup=kb)

@admin_router.message(Command('genpromo'))
@admin_router.callback_query(F.data == "cmd_genpromo_start")
async def cmd_genpromo_start(update: Union[Message, CallbackQuery], state: FSMContext):
    if update.from_user.id != ADMIN_ID: return 
    
    if isinstance(update, CallbackQuery):
        await update.answer()
        await update.message.delete()
    
    await state.set_state(AdminStates.waiting_for_promo_data)
    text = "🔑 Введите данные для промокода в формате: `КОД 7 10` (Код, Дни, Использований)\n\n"
    text += "Пример: `TESTPROMO 30 1`"
    
    await bot.send_message(update.from_user.id, text)

@admin_router.message(AdminStates.waiting_for_promo_data)
async def cmd_genpromo_process(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    await state.clear()
    
    parts = message.text.split()
    if len(parts) != 3:
        return await message.answer("❌ Неверный формат. Нужно: `КОД 7 10`")
        
    code, days_str, uses_str = parts
    code = code.upper()
    try:
        days = int(days_str)
        uses = int(uses_str)
    except ValueError:
        return await message.answer("❌ Дни и Использования должны быть числами.")
        
    if days <= 0 or uses <= 0:
        return await message.answer("❌ Дни и Использования должны быть положительными.")
        
    try:
        await db.db_pool.execute("INSERT OR REPLACE INTO promocodes (code, duration_days, uses_left) VALUES (?, ?, ?)", (code, days, uses))
        await db.db_pool.commit()
        await message.answer(f"✅ Промокод **{code}** создан.\nСрок: **{days}** дней. Использований: **{uses}**.")
    except Exception as e:
        logger.error(f"Error creating promocode: {e}")
        await message.answer("❌ Ошибка при создании промокода.")


# --- FSM Handlers for Auth ---

async def auth_success(user_id: int, client: TelegramClient, state: FSMContext, message: Message):
    await tm.start_worker_session(user_id, client)
    await state.clear()
    await message.answer("✅ Авторизация успешна! Worker запущен.")
    await send_main_menu(user_id, state, message=message)

@user_router.callback_query(F.data == "auth_start_menu")
async def cb_auth_start_menu(call: CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.WAITING_FOR_QR_SCAN)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 QR Code (рекомендуется)", callback_data="auth_qr_start")],
        [InlineKeyboardButton(text="📞 По номеру телефона", callback_data="auth_phone_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="cmd_start")]
    ])
    await call.message.edit_text("Выберите метод авторизации:", reply_markup=kb)
    await call.answer()

@user_router.callback_query(F.data == "auth_qr_start", TelethonAuth.WAITING_FOR_QR_SCAN)
async def cb_auth_qr_start(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    
    path_temp = get_session_path(user_id, is_temp=True)
    client = TelegramClient(path_temp, API_ID, API_HASH)
    
    async with store.lock:
        store.temp_auth_clients[user_id] = client

    try:
        await client.connect()
        # Запрос токена для QR-логина
        login_token_response = await client(functions.auth.ExportLoginTokenRequest(
            api_id=API_ID,
            api_hash=API_HASH,
            except_ids=[] # Тут обычно указывается ID, но можно оставить пустым, если не нужно исключать
        ))
        
        url = login_token_response.url
        qr_img = qrcode.make(url)
        buf = BytesIO()
        qr_img.save(buf, format='JPEG')
        qr_data = BufferedInputFile(buf.getvalue(), filename='qr_code.jpg')
        
        await call.message.delete()
        
        future = asyncio.Future()
        async with store.lock:
             store.qr_login_future[user_id] = future
             
        qr_message = await bot.send_photo(user_id, qr_data, caption="📸 Отсканируйте QR-код в официальном клиенте Telegram. Таймер: **120 секунд**.")

        # Ожидание QR-логина
        await asyncio.wait_for(future, timeout=QR_TIMEOUT)
        
        await auth_success(user_id, client, state, qr_message)

    except asyncio.TimeoutError:
        await bot.send_message(user_id, "❌ Время ожидания QR-кода истекло (120с).")
    except Exception as e:
        logger.error(f"QR Auth error for {user_id}: {e}")
        await bot.send_message(user_id, f"❌ Ошибка авторизации: {e}")
    finally:
        async with store.lock:
            store.qr_login_future.pop(user_id, None)
            store.temp_auth_clients.pop(user_id, None)
        await state.clear()
        try:
            # Пытаемся удалить сообщение с QR-кодом, если оно еще есть
            if 'qr_message' in locals():
                 await qr_message.delete()
        except Exception:
            pass
        # Возвращаемся в главное меню, если это не было сделано через auth_success
        if not state.get_state():
             await send_main_menu(user_id, state, message=call.message)


# --- FSM Handlers for Promo ---

@user_router.callback_query(F.data == "use_promocode_start")
async def cb_use_promo_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.WAITING_CODE)
    await call.message.edit_text("🔑 Введите промокод:")
    await call.answer()

@user_router.message(PromoStates.WAITING_CODE)
async def promo_code_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip().upper()
    await state.clear()
    
    promocode_data = await db.get_promocode(code)
    
    if not promocode_data:
        await message.answer("❌ Промокод не найден.")
        return await send_main_menu(user_id, state, message=message)
        
    if promocode_data['uses_left'] <= 0:
        await message.answer("❌ Промокод закончился.")
        return await send_main_menu(user_id, state, message=message)

    success = await db.use_promocode(code)
    if success:
        days = promocode_data['duration_days']
        new_end = await db.update_subscription(user_id, days)
        await message.answer(f"✅ Подписка успешно активирована на **{days}** дней. Действует до **{new_end.strftime('%d.%m.%Y')}**.")
    else:
        await message.answer("❌ Ошибка при активации промокода. Попробуйте позже.")
        
    await send_main_menu(user_id, state, message=message)

# =========================================================================
# VII. ЗАПУСК БОТА (ФИНАЛИЗАЦИЯ)
# =========================================================================

async def on_startup(dispatcher: Dispatcher, bot: Bot): 
    logger.info("Initializing system...")
    await db.init()
    
    active_users = await db.get_active_telethon_users()
    tasks = []
    for user_id in active_users:
        tasks.append(asyncio.create_task(tm.start_client_task(user_id), name=f"restore-task-{user_id}"))
        logger.info(f"Attempting to restore worker for user {user_id}")

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Bot system is ready.")

async def on_shutdown(dispatcher: Dispatcher, bot: Bot): 
    logger.info("Shutting down bot system...")
    
    # 1. Отключение временных клиентов (Auth)
    async with store.lock:
        temp_clients = list(store.temp_auth_clients.values())
        store.temp_auth_clients.clear() 

    for client in temp_clients:
        try: await client.disconnect()
        except: pass
        
    # 2. Остановка всех активных Worker'ов
    for user_id in list(store.active_workers.keys()):
        await tm.stop_worker(user_id)
        
    # 3. Закрытие DB
    if db.db_pool:
        await db.db_pool.close()
        
    # 4. Закрытие сессии бота
    await bot.session.close()
    logger.info("Bot stopped and resources released.")
    
async def main_run():
    dp.include_router(user_router)
    dp.include_router(admin_router)
    dp.include_router(drops_router) 
    
    dp["db"] = db
    dp["tm"] = tm
    dp["admin_id"] = ADMIN_ID

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    # Запуск фоновой задачи для очистки DB
    # Запускается как fire-and-forget, так как неблокирующая
    cleanup_task = asyncio.create_task(db.cleanup_old_sessions(days=30), name="db-cleanup-task")

    logger.info("Starting bot polling...")
    try:
        await dp.start_polling(bot)
    finally:
        if not cleanup_task.done():
            cleanup_task.cancel()

if __name__ == '__main__':
    try:
        asyncio.run(main_run())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(f"Critical error in main runtime: {e}", exc_info=True)
