import asyncio
import logging
import logging.handlers
import os
import re
import random
import string
import base64
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Set, Any
from io import BytesIO
import concurrent.futures

# Third-party Imports
import aiosqlite
import pytz
import qrcode
from PIL import Image
from dotenv import load_dotenv

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile, CallbackQuery
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# --- TELETHON ---
from telethon import TelegramClient, events, functions, utils
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, 
    AuthKeyUnregisteredError, PasswordHashInvalidError, PhoneCodeInvalidError, 
    PhoneCodeExpiredError, RpcCallFailError 
    # LogOutError удален для совместимости
)

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0)) 
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")

if not BOT_TOKEN or not API_HASH or API_ID == 0:
    print("❌ ОШИБКА: Проверьте .env файл! Не найдены BOT_TOKEN, API_ID или API_HASH.")
    sys.exit(1)

DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
SESSION_DIR = 'sessions'
DATA_DIR = 'data'
QR_TIMEOUT = 120  

os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# --- Логирование ---
def setup_logging():
    log_file = os.path.join(DATA_DIR, 'bot.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

setup_logging() 
logger = logging.getLogger(__name__)

# Executor для синхронных, блокирующих задач (QR-код)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML')) 
dp = Dispatcher(storage=MemoryStorage())
user_router = Router(name='user_router')
admin_router = Router(name='admin_router')

# =========================================================================
# II. ХРАНИЛИЩЕ И СОСТОЯНИЯ
# =========================================================================

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {} 
        self.qr_login_future: Dict[int, asyncio.Future] = {} 
        self.active_workers: Dict[int, TelegramClient] = {} 
        self.premium_users: Set[int] = set() 

store = GlobalStorage()

# --- FSM States ---
class TelethonAuth(StatesGroup):
    WAITING_FOR_QR_SCAN = State()
    PHONE = State()
    CODE = State()
    PASSWORD = State() 

class PromoStates(StatesGroup):
    WAITING_CODE = State()

class AdminPromo(StatesGroup):
    WAITING_DAYS = State() 
    WAITING_USES = State() 

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

# Оптимизированная генерация QR (вынесена в ThreadPoolExecutor)
def make_qr_image_sync(url: str) -> bytes:
    qr = qrcode.make(url)
    buf = BytesIO()
    qr.save(buf, format='JPEG')
    return buf.getvalue()

async def make_qr_image(url: str) -> bytes:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, make_qr_image_sync, url)

def generate_promocode(length=8) -> str:
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(characters) for i in range(length))

# =========================================================================
# III. БАЗА ДАННЫХ
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db_pool: Optional[aiosqlite.Connection] = None

    async def init(self):
        self.db_pool = await aiosqlite.connect(self.db_path, isolation_level=None) 
        await self.db_pool.execute("PRAGMA journal_mode=WAL;")
        await self.db_pool.execute("PRAGMA synchronous=NORMAL;")
        
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
            return dict(result) if result else None
            
    async def use_promocode(self, code: str) -> bool:
        if not self.db_pool: return False
        promocode = await self.get_promocode(code)
        if not promocode or promocode['uses_left'] == 0: return False # uses_left = 0 means infinite
        
        if promocode['uses_left'] > 0:
            await self.db_pool.execute("UPDATE promocodes SET uses_left=? WHERE code=?", (promocode['uses_left'] - 1, code.upper()))
            await self.db_pool.commit()
        return True

    async def set_telethon_status(self, user_id: int, status: bool):
        if not self.db_pool: return
        await self.db_pool.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
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
        
    async def get_stats(self) -> Dict[str, Any]:
        if not self.db_pool: return {}
        async with self.db_pool.execute("SELECT COUNT(user_id) FROM users") as cursor:
            total_users = (await cursor.fetchone())[0]
        async with self.db_pool.execute("SELECT COUNT(user_id) FROM users WHERE telethon_active=1 AND is_banned=0") as cursor:
            active_workers_db = (await cursor.fetchone())[0]
        return {
            'total_users': total_users, 'active_workers_db': active_workers_db,
            'active_workers_ram': len(store.active_workers), 'premium_users_ram': len(store.premium_users)
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

        # Очистка временного клиента
        async with store.lock:
            store.temp_auth_clients.pop(user_id, None)

        if client:
            try:
                if not await client.is_user_authorized(): raise AuthKeyUnregisteredError("Not authorized")
                await client.disconnect() # Закрываем временное соединение
            except Exception:
                pass

        # Переименовываем временный файл сессии в постоянный
        if os.path.exists(path_temp):
            if os.path.exists(path_perm): os.remove(path_perm)
            os.rename(path_temp, path_perm)
            await self.start_client_task(user_id) 
            # Удаляем временные файлы, которые могут остаться
            if os.path.exists(path_temp_base): os.remove(path_temp_base)
        else:
            await self._send_to_bot_user(user_id, "❌ Файл сессии не найден.")
            # Удаляем временные файлы, которые могут остаться
            if os.path.exists(path_temp_base): os.remove(path_temp_base)


    async def start_client_task(self, user_id: int):
        await self.stop_worker(user_id) # Остановка предыдущего
        try:
            task = asyncio.create_task(self._run_worker(user_id), name=f"main-worker-{user_id}")
            async with self.tasks_lock:
                pass 
            logger.info(f"Main worker task created for user {user_id}")
            return task
        except Exception as e:
            logger.error(f"Critical error start_client_task {user_id}: {e}")
            await self.db.set_telethon_status(user_id, False)

    async def _run_worker(self, user_id: int): 
        path = get_session_path(user_id)
        client = TelegramClient(path, self.API_ID, self.API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        async with self.tasks_lock: store.active_workers[user_id] = client # Храним клиента
        
        try:
            await client.connect()
            if not await client.is_user_authorized(): raise AuthKeyUnregisteredError('Session expired')

            sub_end = await self.db.get_subscription_status(user_id)
            if not sub_end or sub_end <= datetime.now(TIMEZONE_MSK):
                await self._send_to_bot_user(user_id, "⚠️ Подписка истекла. Worker отключен.")
                return 
            
            await self.db.set_telethon_status(user_id, True)
            me = await client.get_me()
            await self._send_to_bot_user(user_id, f"✅ Worker запущен! Аккаунт: **{utils.get_display_name(me)}**\nСтатистика активна. Время подписки до: {sub_end.strftime('%d.%m.%Y %H:%M')}")
            
            # --- ОСНОВНАЯ ЛОГИКА WORKER (Здесь будет ваша логика парсинга, если она есть) ---
            
            await client.run_until_disconnected() 
            
        except AuthKeyUnregisteredError:
            await self._send_to_bot_user(user_id, "❌ Сессия истекла/отозвана. Требуется повторный вход.")
            if os.path.exists(path + '.session'): os.remove(path + '.session')
            await self.db.set_telethon_status(user_id, False)
        except FloodWaitError as e:
            await self._send_to_bot_user(user_id, f"⚠️ FloodWait. Worker будет остановлен на {e.seconds} секунд.")
            await self.db.set_telethon_status(user_id, False)
        except Exception as e:
            # Сюда попадет и LogOutError
            logger.error(f"Worker {user_id} error: {e}")
            if client.is_connected(): await client.disconnect()
        finally:
            await self.db.set_telethon_status(user_id, False)
            async with self.tasks_lock:
                store.active_workers.pop(user_id, None)
                store.premium_users.discard(user_id)
            logger.info(f"Worker stopped for user {user_id}")


    async def stop_worker(self, user_id: int, silent=False):
        async with self.tasks_lock:
            client = store.active_workers.pop(user_id, None)
            store.premium_users.discard(user_id)
        
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
            await self.db.set_telethon_status(user_id, False)
            if not silent:
                await self._send_to_bot_user(user_id, "🛑 Worker успешно остановлен.")

manager = TelethonManager(bot, db)

# =========================================================================
# V. USER HANDLERS (АВТОРИЗАЦИЯ)
# =========================================================================

# --- START MENU ---
async def get_main_menu_markup(user_id: int) -> InlineKeyboardMarkup:
    user_data = await db.get_user(user_id)
    is_admin = user_id == ADMIN_ID
    
    is_active = user_id in store.active_workers

    status_text = "🟢 Активен" if is_active else "🔴 Не активен"
    
    auth_button_text = "🔑 Сменить Аккаунт" if user_data and user_data['telethon_active'] else "🔑 Войти"
    
    buttons = [
        [InlineKeyboardButton(text=auth_button_text, callback_data="cb_auth_menu")],
        [InlineKeyboardButton(text=f"📊 Статус Worker: {status_text}", callback_data="cb_worker_status")],
        [InlineKeyboardButton(text="🎁 Активировать Промокод", callback_data="cb_activate_promo")],
    ]
    
    if is_admin:
        buttons.append([InlineKeyboardButton(text="🔧 Админ-Панель", callback_data="admin_stats")])
        
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def send_main_menu(chat_id: int, message_id: Optional[int] = None):
    markup = await get_main_menu_markup(chat_id)
    user_data = await db.get_user(chat_id)
    
    sub_end = await db.get_subscription_status(chat_id)
    
    now_msk = datetime.now(TIMEZONE_MSK)
    
    if sub_end and sub_end > now_msk:
        sub_text = f"✅ Подписка до: {sub_end.strftime('%d.%m.%Y %H:%M')}"
    else:
        sub_text = "❌ Подписка не активна."
        if user_data and user_data['telethon_active']:
             await manager.stop_worker(chat_id, silent=True)
             
    status_worker = "🟢 Активен" if user_data and user_data['telethon_active'] else "🔴 Не активен"

    text = (
        f"👋 **Добро пожаловать в STATPRO!**\n\n"
        f"⚙️ Ваш Worker: {status_worker}\n"
        f"📅 Статус подписки: {sub_text}"
    )
    
    try:
        if message_id:
            await bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
        else:
            await bot.send_message(chat_id, text, reply_markup=markup)
    except TelegramBadRequest:
        pass

@user_router.message(Command(commands=['start']))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await send_main_menu(message.chat.id)

# --- AUTH MENU ---
@user_router.callback_query(F.data == "cb_auth_menu")
async def cb_auth_menu(call: CallbackQuery):
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Номер Телефона", callback_data="auth_phone_start")],
        [InlineKeyboardButton(text="📸 QR-Код", callback_data="auth_qr_start")],
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="cmd_start")]
    ])
    await call.message.edit_text("Выберите способ авторизации:", reply_markup=markup)
    await call.answer()

# --- Shared Auth Success Handler ---
async def auth_success(user_id: int, client: TelegramClient, state: FSMContext, msg_to_delete: Message):
    # Успешно авторизовались. Запускаем постоянный worker.
    await manager.start_worker_session(user_id, client)
    await state.clear()
    
    try:
        await msg_to_delete.delete()
    except TelegramBadRequest:
        pass
    
    # Отправляем новое сообщение с основным меню
    await send_main_menu(user_id)

# --- CANCEL Handler ---
@user_router.callback_query(F.data.in_({'cmd_start', 'cancel_auth'}))
@admin_router.callback_query(F.data.in_({'cmd_start', 'cancel_auth', 'admin_panel'}))
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    
    # 1. Очистка FSM
    current_state = await state.get_state()
    if current_state:
        await state.clear()
    
    # 2. Очистка временных клиентов Telethon
    async with store.lock:
        client = store.temp_auth_clients.pop(user_id, None)
        future = store.qr_login_future.pop(user_id, None)
        
    if client:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass
    
    # 3. Отмена Future (если ожидался QR)
    if future and not future.done():
        future.cancel()
        
    # 4. Возврат в главное меню
    if call.data == 'admin_panel' and user_id == ADMIN_ID:
        # Пытаемся вызвать cb_admin_stats, чтобы вернуться в админ-панель
        await call.answer()
        return await cb_admin_stats(call, state)
        
    await send_main_menu(user_id, call.message.message_id)
    await call.answer()

# =========================================================================
# V-A. QR AUTH FLOW (Оптимизирован для предотвращения лагов)
# =========================================================================

# --- QR START ---
@user_router.callback_query(F.data == "auth_qr_start")
async def cb_auth_qr_start(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    path_temp = get_session_path(user_id, is_temp=True)
    client = TelegramClient(path_temp, API_ID, API_HASH)
    
    # Отправляем сообщение о загрузке
    msg = await call.message.edit_text("⏳ Генерируем QR-код...")
    
    async with store.lock: 
        store.temp_auth_clients[user_id] = client

    try:
        await client.connect()
        
        # 1. Генерация URL
        login_token_response = await client(functions.auth.ExportLoginTokenRequest(api_id=API_ID, api_hash=API_HASH, except_ids=[]))
        token_base64 = base64.urlsafe_b64encode(login_token_response.token).decode('utf-8').rstrip('=')
        url = f"tg://login?token={token_base64}"
        
        # 2. Асинхронная генерация QR-изображения (ОПТИМИЗАЦИЯ!)
        qr_bytes = await make_qr_image(url)
        qr_data = BufferedInputFile(qr_bytes, filename='qr.jpg')
        
        # 3. Отправка QR-кода
        await msg.delete()
        
        future = asyncio.Future()
        async with store.lock: store.qr_login_future[user_id] = future
        
        # Отправка нового сообщения с QR
        msg_qr = await bot.send_photo(user_id, qr_data, caption="📸 <b>Отсканируйте QR в Telegram</b>\n\nНастройки -> Устройства -> Подключить.\nТаймер: 120 сек. <b>Если у вас 2FA, введите его на телефоне!</b>", 
                                   reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_auth")]]))
        
        # 4. НОВАЯ ЛОГИКА: ПЕРИОДИЧЕСКАЯ ПРОВЕРКА СТАТУСА
        async def check_auth_status(client, future, interval=1):
            for _ in range(QR_TIMEOUT // interval):
                try:
                    if await client.is_user_authorized():
                        if not future.done():
                            future.set_result(True)
                        return
                except Exception:
                    pass
                await asyncio.sleep(interval)
            if not future.done():
                future.set_exception(asyncio.TimeoutError)

        check_task = asyncio.create_task(check_auth_status(client, future))
        
        await asyncio.wait_for(future, timeout=QR_TIMEOUT) 
        check_task.cancel()
        
        await auth_success(user_id, client, state, msg_qr)

    except asyncio.TimeoutError:
        try: check_task.cancel()
        except: pass
        await bot.send_message(user_id, "❌ Время сканирования QR вышло.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="cmd_start")]]))
    except Exception as e:
        logger.error(f"QR Error: {e}")
        await bot.send_message(user_id, "❌ Ошибка генерации QR.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="cmd_start")]]))
    finally:
        async with store.lock:
            store.qr_login_future.pop(user_id, None)
            store.temp_auth_clients.pop(user_id, None)
        if os.path.exists(path_temp): os.remove(path_temp)

# =========================================================================
# V-B. PHONE AUTH FLOW (Улучшенная обработка 2FA и ошибок)
# =========================================================================

# --- PHONE START ---
@user_router.callback_query(F.data == "auth_phone_start")
async def cb_auth_phone_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text(
        "✍️ **Введите номер телефона** в международном формате (например, `+79001234567`):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_auth")]])
    )
    await call.answer()

# --- PHONE INPUT ---
@user_router.message(TelethonAuth.PHONE)
async def msg_auth_phone(message: Message, state: FSMContext):
    phone_number = message.text.strip()
    if not is_valid_phone(phone_number):
        return await message.reply("❌ Неверный формат. Введите номер, включая `+`.")
    
    user_id = message.from_user.id
    path_temp = get_session_path(user_id, is_temp=True)
    client = TelegramClient(path_temp, API_ID, API_HASH)
    
    async with store.lock: 
        store.temp_auth_clients[user_id] = client

    try:
        await client.connect()
        result = await client.send_code_request(phone_number)
        
        await state.update_data(phone_number=phone_number, phone_code_hash=result.phone_code_hash)
        await state.set_state(TelethonAuth.CODE)
        
        await message.reply(
            f"✅ Код подтверждения отправлен на **{phone_number}** (или в приложение Telegram).\n\n"
            f"✍️ **Введите 5-значный код** из сообщения:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_auth")]])
        )
        
    except PhoneNumberInvalidError:
        await state.clear()
        await message.reply("❌ Неверный номер телефона. Начните с `/start`.")
    except Exception as e:
        logger.error(f"Send Code Error: {e}")
        await state.clear()
        await message.reply("❌ Произошла ошибка при отправке кода. Повторите попытку.")
    finally:
        if client and client.is_connected():
            await client.disconnect()

# --- CODE INPUT ---
@user_router.message(TelethonAuth.CODE, F.text.regexp(r'^\d{4,5}$'))
async def msg_auth_code(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    if 'phone_number' not in data or 'phone_code_hash' not in data:
        return await message.reply("❌ Сессия авторизации утеряна. Начните с `/start`.")

    phone_number = data['phone_number']
    phone_code_hash = data['phone_code_hash']
    
    path_temp = get_session_path(user_id, is_temp=True)
    client = TelegramClient(path_temp, API_ID, API_HASH)
    
    msg_wait = await message.reply("⏳ Проверяю код...")
    
    try:
        await client.connect()
        
        # Попытка входа
        await client(functions.auth.SignInRequest(
            phone_code=code,
            phone_number=phone_number,
            phone_code_hash=phone_code_hash
        ))
        
        # Успешный вход без 2FA
        await auth_success(user_id, client, state, msg_wait)

    except SessionPasswordNeededError:
        # 2FA требуется
        await state.set_state(TelethonAuth.PASSWORD)
        await msg_wait.delete()
        await message.reply(
            "⚠️ **Требуется облачный пароль (2FA)!**\n\n"
            "✍️ Введите ваш пароль:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_auth")]])
        )
    except (PhoneCodeInvalidError, PhoneCodeExpiredError, RpcCallFailError):
        await msg_wait.delete()
        await message.reply("❌ Неверный или просроченный код. Повторите, начиная с `/start`.")
        await state.clear()
        if client and client.is_connected(): await client.disconnect()
    except Exception as e:
        logger.error(f"SignIn Error: {e}")
        await msg_wait.delete()
        await state.clear()
        await message.reply(f"❌ Неизвестная ошибка входа: {e}. Повторите с `/start`.")
        if client and client.is_connected(): await client.disconnect()

# --- PASSWORD INPUT ---
@user_router.message(TelethonAuth.PASSWORD)
async def msg_auth_password(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    path_temp = get_session_path(user_id, is_temp=True)
    client = TelegramClient(path_temp, API_ID, API_HASH)

    msg_wait = await message.reply("⏳ Проверяю пароль...")

    try:
        await client.connect()
        
        # Проверяем 2FA
        await client(functions.auth.CheckPasswordRequest(password=password)) 
        
        # Успешный вход с 2FA
        await db.set_password_2fa(user_id, password) 
        await auth_success(user_id, client, state, msg_wait)
        
    except PasswordHashInvalidError:
        await msg_wait.delete()
        await message.reply("❌ Неверный облачный пароль (2FA). Повторите попытку.")
    except Exception as e:
        logger.error(f"Password Check Error: {e}")
        await msg_wait.delete()
        await state.clear()
        await message.reply(f"❌ Неизвестная ошибка 2FA: {e}. Начните с `/start`.")
        if client and client.is_connected(): await client.disconnect()

# =========================================================================
# VI. USER HANDLERS (ПРОЧЕЕ)
# =========================================================================

@user_router.callback_query(F.data == "cb_worker_status")
async def cb_worker_status(call: CallbackQuery):
    user_id = call.from_user.id
    user_data = await db.get_user(user_id)
    
    if not user_data or not user_data['telethon_active']:
        return await call.answer("Worker не активен.", show_alert=True)

    sub_end = await db.get_subscription_status(user_id)
    status_text = "🟢 Активен" if user_id in store.active_workers else "🔴 Не активен (Запускается...)"

    await call.message.answer(
        f"**Информация о Worker'e:**\n"
        f"Статус: {status_text}\n"
        f"Подписка до: {sub_end.strftime('%d.%m.%Y %H:%M')}\n"
        f"Чтобы остановить, смените аккаунт или дождитесь окончания подписки."
    )
    await call.answer()

# --- PROMO CODE ACTIVATION ---
@user_router.callback_query(F.data == "cb_activate_promo")
async def cb_activate_promo(call: CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.WAITING_CODE)
    await call.message.edit_text(
        "✍️ **Введите промокод:**",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cmd_start")]])
    )
    await call.answer()

@user_router.message(PromoStates.WAITING_CODE)
async def msg_activate_promo(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    user_id = message.from_user.id
    
    promocode = await db.get_promocode(code)
    
    if not promocode:
        return await message.reply("❌ Промокод не найден.")

    uses_left = promocode['uses_left']
    
    if uses_left == 0 and promocode['duration_days'] == 0:
        return await message.reply("❌ Промокод недействителен (0 дней и 0 использований).")
        
    # Активация
    success = await db.use_promocode(code)
    
    if success:
        new_end = await db.update_subscription(user_id, promocode['duration_days'])
        
        await state.clear()
        
        uses_display = 'Бесконечно' if promocode['uses_left'] == 0 else promocode['uses_left'] - 1
        
        await message.reply(
            f"🎉 **Промокод активирован!**\n"
            f"Добавлено {promocode['duration_days']} дней к подписке.\n"
            f"Осталось активаций: {uses_display}\n"
            f"Новый срок подписки: {new_end.strftime('%d.%m.%Y %H:%M')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ В меню", callback_data="cmd_start")]])
        )
    else:
        await message.reply("❌ Промокод уже использован или произошла ошибка базы данных.")


# =========================================================================
# VII. ADMIN HANDLERS
# =========================================================================

# --- ADMIN PANEL START ---
@admin_router.callback_query(F.data.in_({"admin_stats", "admin_panel"}))
async def cb_admin_stats(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("🛑 Доступ запрещен.", show_alert=True)
    
    await state.clear()
    stats = await db.get_stats()
    
    text = (
        "**🔧 АДМИН-ПАНЕЛЬ**\n\n"
        f"👤 Всего пользователей: {stats.get('total_users', 0)}\n"
        f"⚙️ Активные воркеры (DB): {stats.get('active_workers_db', 0)}\n"
        f"⚡ Активные воркеры (RAM): {stats.get('active_workers_ram', 0)}"
    )
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✨ Создать Промокод", callback_data="admin_create_promo_init")],
        [InlineKeyboardButton(text="🗑 Удалить Промокод", callback_data="admin_delete_promo_init")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="cmd_start")]
    ])
    
    try:
        await call.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest:
        pass 
    await call.answer()


# --- PROMO CREATE (STEP 1: GENERATE CODE + ASK DAYS) ---
@admin_router.callback_query(F.data == "admin_create_promo_init")
async def cb_admin_create_promo_init(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    
    promo_code = generate_promocode()
    await state.update_data(promo_code=promo_code)
    
    await state.set_state(AdminPromo.WAITING_DAYS)

    # Удобный для копирования вывод кода
    text = (f"✅ Промокод сгенерирован!\n"
            f"Код: <code>{promo_code}</code> (Нажмите, чтобы скопировать)\n\n"
            f"✍️ **Шаг 1/2:** Введите **срок действия** (в днях, только число, 0 = 0 дней):")
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="admin_panel")]
    ])
    
    await call.message.edit_text(text, reply_markup=markup)
    await call.answer()


# --- PROMO CREATE (STEP 2: DAYS INPUT) ---
@admin_router.message(AdminPromo.WAITING_DAYS, F.text.regexp(r'^\d+$'))
async def msg_admin_promo_days(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        days = int(message.text.strip())
        if days < 0: raise ValueError("Non-negative days only")
    except ValueError:
        return await message.reply("❌ Неверный формат. Введите только положительное число дней или 0.")
    
    await state.update_data(days=days)
    await state.set_state(AdminPromo.WAITING_USES)
    
    data = await state.get_data()
    text = (f"✅ Код <code>{data['promo_code']}</code>. Срок: {days} д.\n\n"
            f"✍️ **Шаг 2/2:** Введите **количество активаций** (только число, 0 = бесконечно):")
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="admin_panel")]
    ])
    
    await message.reply(text, reply_markup=markup)

@admin_router.message(AdminPromo.WAITING_DAYS)
async def msg_admin_promo_days_invalid(message: Message):
    if message.from_user.id != ADMIN_ID: return
    await message.reply("❌ Неверный формат. Введите только положительное число дней или 0.")


# --- PROMO CREATE (STEP 3: USES INPUT) ---
@admin_router.message(AdminPromo.WAITING_USES, F.text.regexp(r'^\d+$'))
async def msg_admin_promo_uses(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        uses = int(message.text.strip())
        if uses < 0: raise ValueError("Non-negative uses only")
    except ValueError:
        return await message.reply("❌ Неверный формат. Введите только положительное число или 0 (для бесконечных активаций).")

    data = await state.get_data()
    promo_code = data['promo_code']
    days = data['days']
    
    # Сохранение в БД
    try:
        await db.db_pool.execute(
            "INSERT INTO promocodes (code, duration_days, uses_left) VALUES (?, ?, ?)",
            (promo_code, days, uses)
        )
        await db.db_pool.commit()
    except aiosqlite.IntegrityError:
        # Это не должно случиться, так как код генерируется, но на всякий случай
        await state.clear()
        return await message.reply("❌ Ошибка: Промокод с таким кодом уже существует. Повторите создание.")
    
    await state.clear()
    
    await message.reply(
        f"🎉 **Промокод создан!**\n\n"
        f"Код: <code>{promo_code}</code>\n"
        f"Срок: {days} д.\n"
        f"Использований: {'Бесконечно' if uses == 0 else uses}\n",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ В админ-панель", callback_data="admin_stats")]
        ])
    )

@admin_router.message(AdminPromo.WAITING_USES)
async def msg_admin_promo_uses_invalid(message: Message):
    if message.from_user.id != ADMIN_ID: return
    await message.reply("❌ Неверный формат. Введите только положительное число или 0 (для бесконечных активаций).")


# --- PROMO DELETE ---
@admin_router.callback_query(F.data == "admin_delete_promo_init")
async def cb_admin_delete_promo_init(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return

    await state.set_state(PromoStates.WAITING_CODE)
    await call.message.edit_text(
        "✍️ **Введите промокод, который нужно удалить:**",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="admin_stats")]])
    )
    await call.answer()

@admin_router.message(PromoStates.WAITING_CODE)
async def msg_admin_delete_promo(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return

    code = message.text.strip().upper()

    async with db.db_pool.execute("DELETE FROM promocodes WHERE code=?", (code,)) as cursor:
        rows_deleted = cursor.rowcount
    await db.db_pool.commit()
    
    await state.clear()
    
    if rows_deleted > 0:
        await message.reply(
            f"🗑 **Промокод <code>{code}</code> успешно удален.**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ В админ-панель", callback_data="admin_stats")]])
        )
    else:
        await message.reply(
            f"❌ Промокод <code>{code}</code> не найден в базе данных.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ В админ-панель", callback_data="admin_stats")]])
        )

# =========================================================================
# VIII. LAUNCH
# =========================================================================

async def on_startup(dispatcher: Dispatcher, bot: Bot):
    logger.info("Bot starting up...")
    await db.init()
    
    # Восстановление активных воркеров из БД
    active_users = await db.get_active_telethon_users()
    logger.info(f"Restoring {len(active_users)} active workers...")
    for user_id in active_users:
        try:
            await manager.start_client_task(user_id)
        except Exception as e:
            logger.error(f"Failed to restore worker {user_id}: {e}")
            await db.set_telethon_status(user_id, False)

    # Регистрация роутеров
    dispatcher.include_router(user_router)
    dispatcher.include_router(admin_router)
    
    logger.info("Bot ready and polling started!")

async def main():
    await on_startup(dp, bot)
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by KeyboardInterrupt.")
    except Exception as e:
        logger.critical(f"Fatal error in main loop: {e}")
