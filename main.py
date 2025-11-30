import asyncio
import logging
import logging.handlers
import os
import re
import random
import string
import base64  # Добавлено для фикса QR
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Union, Set, Any
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
from telethon import TelegramClient, events, errors, functions, utils
from telethon.tl.types import User, Channel, Chat
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, AuthKeyUnregisteredError, PasswordHashInvalidError

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 6256576302)) 
API_ID = int(os.getenv("API_ID", 37185453))
API_HASH = os.getenv("API_HASH")

if not BOT_TOKEN or not API_HASH:
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

# --- Логирование ---
def setup_logging(log_file=os.path.join(DATA_DIR, 'bot.log'), level=logging.INFO):
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=2, encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

setup_logging() 
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML')) 
dp = Dispatcher(storage=MemoryStorage())
user_router = Router(name='user_router')
drops_router = Router(name='drops_router') 
admin_router = Router(name='admin_router')

# =========================================================================
# II. ХРАНИЛИЩЕ И СОСТОЯНИЯ
# =========================================================================

class WorkerTask:
    def __init__(self, task_type: str, task_id: str, creator_id: int, target: Union[int, str]): 
        self.task_type = task_type
        self.task_id = task_id
        self.creator_id = creator_id
        self.target = target
        self.task: Optional[asyncio.Task] = None
        self.start_time: datetime = datetime.now(TIMEZONE_MSK)

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
    PASSWORD = State() # 2FA

class PromoStates(StatesGroup):
    WAITING_CODE = State()

class AdminStates(StatesGroup):
    # Новые состояния для инлайн-создания промокода
    SELECT_DAYS = State()
    SELECT_USES = State()
    waiting_for_broadcast_message = State()

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

def generate_promocode(length: int = 8) -> str:
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

# =========================================================================
# III. БАЗА ДАННЫХ
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db_pool: Optional[aiosqlite.Connection] = None

    async def init(self):
        self.db_pool = await aiosqlite.connect(self.db_path, isolation_level=None, timeout=30.0) 
        await self.db_pool.execute("PRAGMA journal_mode=WAL;")
        await self.db_pool.execute("PRAGMA synchronous=OFF;") 
        
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
        
        await self.db_pool.execute("""
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                duration_days INTEGER,
                uses_left INTEGER
            )
        """)
        await self.db_pool.commit()

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
        new_end = (current_end if current_end and current_end > now else now) + timedelta(days=days)
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
        if not promocode or promocode['uses_left'] <= 0: return False
        await self.db_pool.execute("UPDATE promocodes SET uses_left=? WHERE code=?", (promocode['uses_left'] - 1, code.upper()))
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

    async def update_drop_status(self, old_phone: str, new_status: str) -> bool:
        if not self.db_pool: return False
        now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        current_session = await self.get_drop_session_by_phone(old_phone)
        if not current_session: return False
        await self.db_pool.execute("UPDATE drop_sessions SET status=?, last_status_time=? WHERE phone=?", (new_status, now_str, old_phone))
        await self.db_pool.commit()
        return True

    async def cleanup_old_sessions(self, days: int = 30):
        if not self.db_pool: return
        cutoff = (datetime.now(TIMEZONE_MSK) - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        await self.db_pool.execute("UPDATE drop_sessions SET status='deleted' WHERE last_status_time < ?", (cutoff,))
        await self.db_pool.commit()
        
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
            'total_users': total_users, 'active_workers_db': active_workers_db,
            'active_workers_ram': len(store.active_workers), 'active_drops': active_drops,
            'total_drops': total_drops, 'premium_users_ram': len(store.premium_users)
        }

db = AsyncDatabase(os.path.join(DATA_DIR, DB_NAME))

# =========================================================================
# IV. TELETHON MANAGER
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
        except Exception as e:
            logger.error(f"Error sending message to {user_id}: {e}")
            if "blocked" in str(e).lower(): await self.stop_worker(user_id)
    
    async def start_worker_session(self, user_id: int, client: TelegramClient):
        path_perm_base = get_session_path(user_id)
        path_temp_base = get_session_path(user_id, is_temp=True)
        path_perm = path_perm_base + '.session'
        path_temp = path_temp_base + '.session'

        async with store.lock:
            store.temp_auth_clients.pop(user_id, None)

        if client:
            try:
                if not await client.is_user_authorized(): raise AuthKeyUnregisteredError("Not authorized")
                await client.disconnect()
            except Exception:
                if os.path.exists(path_temp): os.remove(path_temp)
                return await self._send_to_bot_user(user_id, "❌ Ошибка сессии. Повторите вход.")

        if os.path.exists(path_temp):
            if os.path.exists(path_perm): os.remove(path_perm)
            os.rename(path_temp, path_perm)
            await self.start_client_task(user_id) 
        else:
            await self._send_to_bot_user(user_id, "❌ Файл сессии не найден.")

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
            # Тут будет логика обработчика сообщений воркера (флуд, пк и т.д.)
            pass
        
        try:
            await client.connect()
            if not await client.is_user_authorized(): raise AuthKeyUnregisteredError('Session expired')

            sub_end = await self.db.get_subscription_status(user_id)
            if not sub_end or sub_end <= datetime.now(TIMEZONE_MSK):
                await self._send_to_bot_user(user_id, "⚠️ Подписка истекла. Worker отключен.")
                return 
            
            await self.db.set_telethon_status(user_id, True)
            me = await client.get_me()
            await self._send_to_bot_user(user_id, f"🚀 Worker запущен (<b>@{me.username or 'NoUser'}</b>).")
            
            await asyncio.Future() 
            
        except AuthKeyUnregisteredError:
            path_s = path + '.session'
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Выполните вход заново.")
            if os.path.exists(path_s): os.remove(path_s)
        except Exception as e:
            logger.error(f"Worker {user_id} crashed: {e}")
        finally:
            await self.stop_worker(user_id)

    async def stop_worker(self, user_id: int):
        async with self.tasks_lock:
            client = store.active_workers.pop(user_id, None)
            store.premium_users.discard(user_id)
            if user_id in store.worker_tasks: store.worker_tasks.pop(user_id)

        if client:
            try: await client.disconnect()
            except: pass 
        await self.db.set_telethon_status(user_id, False)

tm = TelethonManager(bot, db)

# =========================================================================
# V. ХЕНДЛЕРЫ
# =========================================================================

# --- Global Error Handler ---
@dp.errors()
async def errors_handler(event: ErrorEvent):
    logger.error(f"Error: {event.exception}", exc_info=True)
    return True

# --- General Menus ---
async def send_main_menu(user_id: int, state: FSMContext, message: types.Message = None, call: CallbackQuery = None):
    await state.clear()
    
    sub = await db.get_subscription_status(user_id)
    is_active = user_id in store.premium_users
    
    status_text = "🟢 <b>Активен</b>" if is_active else "🔴 <b>Неактивен</b>"
    sub_text = f"✅ Подписка до: <b>{sub.strftime('%d.%m.%Y')}</b>" if sub and sub > datetime.now(TIMEZONE_MSK) else "❌ <b>Нет подписки</b>"
    
    text = (f"👋 <b>Привет! ID: {user_id}</b>\n\n"
            f"🤖 Статус Worker: {status_text}\n"
            f"{sub_text}\n\n"
            "👇 Выберите действие:")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Вход / Смена Аккаунта", callback_data="auth_start_menu")],
        [InlineKeyboardButton(text="🎟 Ввести Промокод", callback_data="use_promocode_start")],
        [InlineKeyboardButton(text="🔴 Остановить" if is_active else "🟢 Запустить", callback_data="cmd_stop" if is_active else "cmd_restart")],
        [InlineKeyboardButton(text="🔧 Админ-Панель", callback_data="admin_stats")] if user_id == ADMIN_ID else []
    ])

    if call:
        try: await call.message.edit_text(text, reply_markup=kb)
        except: await call.message.answer(text, reply_markup=kb)
        await call.answer()
    elif message:
        await message.answer(text, reply_markup=kb)

@user_router.message(Command('start'))
@user_router.callback_query(F.data == "cmd_start")
async def cmd_start(update: Union[Message, CallbackQuery], state: FSMContext): 
    user_id = update.from_user.id
    if isinstance(update, CallbackQuery):
        await send_main_menu(user_id, state, call=update)
    else:
        await send_main_menu(user_id, state, message=update)

@user_router.callback_query(F.data == "cmd_stop")
async def cmd_stop(call: CallbackQuery, state: FSMContext):
    await tm.stop_worker(call.from_user.id)
    await call.answer("✅ Worker остановлен", show_alert=True)
    await send_main_menu(call.from_user.id, state, call=call)

@user_router.callback_query(F.data == "cmd_restart")
async def cmd_restart(call: CallbackQuery, state: FSMContext):
    path = get_session_path(call.from_user.id) + '.session'
    if not os.path.exists(path):
        return await call.answer("❌ Нет сессии. Сначала войдите.", show_alert=True)
    
    await call.answer("⏳ Запуск...", show_alert=False)
    await tm.start_client_task(call.from_user.id)
    await asyncio.sleep(1)
    await send_main_menu(call.from_user.id, state, call=call)

# --- AUTH (QR & PHONE) ---

@user_router.callback_query(F.data == "auth_start_menu")
async def cb_auth_start_menu(call: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 QR-Код", callback_data="auth_qr_start"),
         InlineKeyboardButton(text="📱 Номер Телефона", callback_data="auth_phone_start")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="cmd_start")]
    ])
    await call.message.edit_text("<b>🔑 Выберите способ входа:</b>", reply_markup=kb)

# --- QR ---
@user_router.callback_query(F.data == "auth_qr_start")
async def cb_auth_qr_start(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    path_temp = get_session_path(user_id, is_temp=True)
    client = TelegramClient(path_temp, API_ID, API_HASH)
    async with store.lock: store.temp_auth_clients[user_id] = client

    try:
        await client.connect()
        # ✅ ИСПРАВЛЕННАЯ ЛОГИКА ГЕНЕРАЦИИ URL
        login_token_response = await client(functions.auth.ExportLoginTokenRequest(api_id=API_ID, api_hash=API_HASH, except_ids=[]))
        token_base64 = base64.urlsafe_b64encode(login_token_response.token).decode('utf-8').rstrip('=')
        url = f"tg://login?token={token_base64}"
        
        qr = qrcode.make(url)
        buf = BytesIO()
        qr.save(buf, format='JPEG')
        qr_data = BufferedInputFile(buf.getvalue(), filename='qr.jpg')
        
        await call.message.delete()
        
        future = asyncio.Future()
        async with store.lock: store.qr_login_future[user_id] = future
        
        msg = await bot.send_photo(user_id, qr_data, caption="📸 <b>Отсканируйте QR в Telegram</b>\n\nНастройки -> Устройства -> Подключить.\nТаймер: 120 сек.", 
                                   reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cmd_start")]]))

        await asyncio.wait_for(future, timeout=QR_TIMEOUT)
        await auth_success(user_id, client, state, msg)

    except asyncio.TimeoutError:
        await bot.send_message(user_id, "❌ Время вышло.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="cmd_start")]]))
    except Exception as e:
        logger.error(f"QR Error: {e}")
        await bot.send_message(user_id, "❌ Ошибка генерации QR.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="cmd_start")]]))
    finally:
        async with store.lock:
            store.qr_login_future.pop(user_id, None)
            store.temp_auth_clients.pop(user_id, None)

async def auth_success(user_id: int, client: TelegramClient, state: FSMContext, message: Message):
    await tm.start_worker_session(user_id, client)
    await state.clear()
    await message.answer("✅ <b>Успешный вход!</b> Worker запущен.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="cmd_start")]]))

# --- PHONE ---
@user_router.callback_query(F.data == "auth_phone_start")
async def cb_auth_phone_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("📱 <b>Введите номер телефона:</b>\n(Пример: +79001234567)", 
                                 reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cmd_start")]]))

@user_router.message(TelethonAuth.PHONE)
async def auth_phone_input(message: Message, state: FSMContext):
    phone = message.text.strip()
    if not is_valid_phone(phone): return await message.answer("❌ Неверный формат.")
    
    path_temp = get_session_path(message.from_user.id, is_temp=True)
    if os.path.exists(path_temp+'.session'): os.remove(path_temp+'.session')
    
    client = TelegramClient(path_temp, API_ID, API_HASH)
    try:
        await client.connect()
        result = await client.send_code_request(phone)
        async with store.lock: store.temp_auth_clients[message.from_user.id] = client
        
        await state.update_data(phone=phone, phone_code_hash=result.phone_code_hash)
        await state.set_state(TelethonAuth.CODE)
        await message.answer("📩 <b>Введите код из Telegram (5 цифр):</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cmd_start")]]))
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@user_router.message(TelethonAuth.CODE)
async def auth_code_input(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    client = store.temp_auth_clients.get(message.from_user.id)
    
    if not client: return await message.answer("❌ Сессия истекла.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="cmd_start")]]))

    try:
        await client.sign_in(data['phone'], code, phone_code_hash=data['phone_code_hash'])
        await auth_success(message.from_user.id, client, state, message)
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔒 <b>Введите пароль 2FA:</b>")
    except Exception as e:
        await message.answer(f"❌ Ошибка кода: {e}")

@user_router.message(TelethonAuth.PASSWORD)
async def auth_password_input(message: Message, state: FSMContext):
    password = message.text.strip()
    client = store.temp_auth_clients.get(message.from_user.id)
    try:
        await client.sign_in(password=password)
        # 2FA хранение в БД
        await db.set_password_2fa(message.from_user.id, password)
        await auth_success(message.from_user.id, client, state, message)
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: {e}")

# --- PROMOCODE ---
@user_router.callback_query(F.data == "use_promocode_start")
async def cb_use_promo_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.WAITING_CODE)
    await call.message.edit_text("🎟 <b>Введите промокод:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cmd_start")]]))

@user_router.message(PromoStates.WAITING_CODE)
async def promo_code_input(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    if await db.use_promocode(code):
        promo = await db.get_promocode(code)
        new_end = await db.update_subscription(message.from_user.id, promo['duration_days'])
        await message.answer(f"✅ Промокод активирован!\nПодписка до: <b>{new_end.strftime('%d.%m.%Y')}</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="cmd_start")]]))
    else:
        await message.answer("❌ Неверный или истекший промокод.")

# --- ADMIN PANEL (INLINE CREATION) ---
@admin_router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    stats = await db.get_stats()
    text = (f"📊 <b>Статистика</b>\n\n"
            f"Всего юзеров: {stats['total_users']}\n"
            f"Активных Worker: {stats['active_workers_db']}\n"
            f"Активных Drops: {stats['active_drops']}")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✨ Создать Промокод", callback_data="admin_create_promo_init")],
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="cmd_start")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

# Шаг 1: Выбор дней
@admin_router.callback_query(F.data == "admin_create_promo_init")
async def admin_promo_days(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.SELECT_DAYS)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 День", callback_data="pday_1"), InlineKeyboardButton(text="3 Дня", callback_data="pday_3")],
        [InlineKeyboardButton(text="7 Дней", callback_data="pday_7"), InlineKeyboardButton(text="30 Дней", callback_data="pday_30")],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="admin_stats")]
    ])
    await call.message.edit_text("⏳ <b>Выберите срок действия:</b>", reply_markup=kb)

# Шаг 2: Выбор кол-ва использований
@admin_router.callback_query(F.data.startswith("pday_"), AdminStates.SELECT_DAYS)
async def admin_promo_uses(call: CallbackQuery, state: FSMContext):
    days = int(call.data.split("_")[1])
    await state.update_data(days=days)
    await state.set_state(AdminStates.SELECT_USES)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 Активация", callback_data="puse_1"), InlineKeyboardButton(text="5 Активаций", callback_data="puse_5")],
        [InlineKeyboardButton(text="10 Активаций", callback_data="puse_10"), InlineKeyboardButton(text="50 Активаций", callback_data="puse_50")],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="admin_stats")]
    ])
    await call.message.edit_text(f"⏳ Срок: <b>{days} дн.</b>\nВыберите кол-во активаций:", reply_markup=kb)

# Шаг 3: Генерация
@admin_router.callback_query(F.data.startswith("puse_"), AdminStates.SELECT_USES)
async def admin_promo_finish(call: CallbackQuery, state: FSMContext):
    uses = int(call.data.split("_")[1])
    data = await state.get_data()
    days = data['days']
    
    code = generate_promocode()
    await db.db_pool.execute("INSERT INTO promocodes (code, duration_days, uses_left) VALUES (?, ?, ?)", (code, days, uses))
    await db.db_pool.commit()
    
    await call.message.edit_text(
        f"✅ <b>Промокод создан!</b>\n\n"
        f"<code>{code}</code>\n\n"
        f"Срок: {days} дн.\nАктиваций: {uses}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_stats")]])
    )
    await state.clear()

# =========================================================================
# VI. RUN
# =========================================================================

async def main():
    await db.init()
    dp.include_router(user_router)
    dp.include_router(admin_router)
    
    # Авторестарт воркеров
    active_ids = await db.get_active_telethon_users()
    for uid in active_ids:
        asyncio.create_task(tm.start_client_task(uid))
        
    logger.info("Bot started!")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        if db.db_pool: await db.db_pool.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stop.")
