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
from aiogram.enums import ParseMode # Для явного указания режима парсинга

# --- TELETHON ---
from telethon import TelegramClient, events, functions, utils
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, 
    AuthKeyUnregisteredError, PasswordHashInvalidError, PhoneCodeInvalidError, 
    PhoneCodeExpiredError, RpcCallFailError 
    # LogOutError удален
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

# Устанавливаем ParseMode.HTML по умолчанию
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)) 
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
    # ... (Весь класс AsyncDatabase остается без изменений, кроме метода get_all_promocodes) ...
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
            
    async def get_all_promocodes(self) -> List[Dict[str, Any]]:
        """Возвращает список всех промокодов."""
        if not self.db_pool: return []
        self.db_pool.row_factory = aiosqlite.Row
        async with self.db_pool.execute("SELECT * FROM promocodes ORDER BY code") as cursor:
            results = await cursor.fetchall()
            return [dict(row) for row in results]

    async def use_promocode(self, code: str) -> bool:
        if not self.db_pool: return False
        promocode = await self.get_promocode(code)
        if not promocode or promocode['uses_left'] == 0: return False
        
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
# IV. TELETHON MANAGER (Исправлен запуск Worker'а)
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
            # Преобразуем chat_id в str для Aiogram 3/Pydantic, как обсуждалось ранее
            await self.bot.send_message(user_id, message, reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"Error sending message to {user_id}: {e}")
            if "blocked" in str(e).lower(): await self.stop_worker(user_id, silent=True)
    
    async def start_worker_session(self, user_id: int, client: TelegramClient):
        path_perm_base = get_session_path(user_id)
        path_temp_base = get_session_path(user_id, is_temp=True)
        path_perm = path_perm_base + '.session'
        path_temp = path_temp_base + '.session'

        # Очистка временного клиента
        async with store.lock:
            store.temp_auth_clients.pop(user_id, None)

        # 1. Завершаем временное соединение, если оно еще активно
        if client:
            try:
                if await client.is_connected(): await client.disconnect()
            except Exception:
                pass

        # 2. Переименовываем временный файл сессии в постоянный
        if os.path.exists(path_temp):
            if os.path.exists(path_perm): os.remove(path_perm)
            os.rename(path_temp, path_perm)
            
            # 3. Запуск Worker'а как отдельной задачи
            await self.start_client_task(user_id) 
            
            # 4. Удаляем временные файлы
            if os.path.exists(path_temp_base): os.remove(path_temp_base)
        else:
            await self._send_to_bot_user(user_id, "❌ Файл сессии не найден. Авторизация не завершена.")
            if os.path.exists(path_temp_base): os.remove(path_temp_base)


    async def start_client_task(self, user_id: int):
        await self.stop_worker(user_id) # Остановка предыдущего
        try:
            # ГАРАНТИРУЕМ, что задача создается и запускается асинхронно
            task = asyncio.create_task(self._run_worker(user_id), name=f"main-worker-{user_id}")
            logger.info(f"Main worker task created for user {user_id}")
            return task
        except Exception as e:
            logger.error(f"Critical error start_client_task {user_id}: {e}")
            await self.db.set_telethon_status(user_id, False)

    async def _run_worker(self, user_id: int): 
        path = get_session_path(user_id)
        client = TelegramClient(path, self.API_ID, self.API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        async with self.tasks_lock: 
            if user_id in store.active_workers: # Двойная проверка
                await client.disconnect()
                return 
            store.active_workers[user_id] = client 
        
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
            
            # --- ОСНОВНАЯ ЛОГИКА WORKER (Цикл ожидания) ---
            await client.run_until_disconnected() 
            
        except AuthKeyUnregisteredError:
            await self._send_to_bot_user(user_id, "❌ Сессия истекла/отозвана. Требуется повторный вход.")
            if os.path.exists(path + '.session'): os.remove(path + '.session')
            await self.db.set_telethon_status(user_id, False)
        except FloodWaitError as e:
            await self._send_to_bot_user(user_id, f"⚠️ FloodWait. Worker будет остановлен на {e.seconds} секунд.")
            await self.db.set_telethon_status(user_id, False)
        except Exception as e:
            logger.error(f"Worker {user_id} error: {e}")
            if client.is_connected(): 
                try: await client.disconnect()
                except: pass
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
                # Отключение клиента Telethon
                await client.disconnect()
            except Exception:
                pass
            await self.db.set_telethon_status(user_id, False)
            if not silent:
                await self._send_to_bot_user(user_id, "🛑 Worker успешно остановлен.")

manager = TelethonManager(bot, db)


# =========================================================================
# V. USER HANDLERS (АВТОРИЗАЦИЯ И МЕНЮ)
# =========================================================================

# --- START MENU (УЛУЧШЕННОЕ ПРИВЕТСТВИЕ) ---
async def get_main_menu_markup(user_id: int) -> InlineKeyboardMarkup:
    # ... (Остается как было) ...
    user_data = await db.get_user(user_id)
    is_admin = user_id == ADMIN_ID
    
    is_active = user_id in store.active_workers

    status_text = "🟢 Активен" if is_active else "🔴 Не активен"
    
    auth_button_text = "🔑 Сменить Аккаунт" if user_data and user_data['telethon_active'] else "🔑 Войти в Telegram"
    
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
        sub_text = f"✅ Подписка до: **{sub_end.strftime('%d.%m.%Y %H:%M')}**"
    else:
        sub_text = "❌ Подписка не активна. Активируйте промокод или оплатите."
        if user_data and user_data['telethon_active']:
             await manager.stop_worker(chat_id, silent=True)
             
    status_worker = "🟢 Активен" if user_data and user_data['telethon_active'] else "🔴 Не активен"

    # УЛУЧШЕННЫЙ ТЕКСТ ПРИВЕТСТВИЯ
    text = (
        f"👋 **Добро пожаловать в StatPro!**\n"
        f"Это ваш личный Worker для сбора и анализа статистики.\n\n"
        f"⚙️ Статус Worker'а: **{status_worker}**\n"
        f"📅 Статус подписки: {sub_text}\n\n"
        f"Нажмите **'🔑 Войти в Telegram'**, чтобы подключить аккаунт и начать работу."
    )
    
    try:
        if message_id:
            # Исправленный вызов, используя str(chat_id) для надежности Aiogram/Pydantic
            await bot.edit_message_text(text, str(chat_id), message_id, reply_markup=markup)
        else:
            await bot.send_message(chat_id, text, reply_markup=markup)
    except TelegramBadRequest as e:
        # Если не удалось отредактировать (например, сообщение старое), просто отправляем новое
        if "message can't be edited" in str(e):
             await bot.send_message(chat_id, text, reply_markup=markup)
        else:
            logger.warning(f"TelegramBadRequest in send_main_menu: {e}")


@user_router.message(Command(commands=['start']))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await send_main_menu(message.chat.id)

# --- Shared Auth Success Handler (ИСПРАВЛЕННЫЙ) ---
async def auth_success(user_id: int, client: TelegramClient, state: FSMContext, msg_to_delete: Message):
    # 1. Запускаем постоянный worker (это сохранит сессию и запустит фоновую задачу)
    # Этот вызов теперь гарантированно не блокирует!
    await manager.start_worker_session(user_id, client) 
    
    await state.clear()
    
    # 2. Удаление сообщения об ожидании/QR
    try:
        await msg_to_delete.delete()
    except TelegramBadRequest:
        pass
    
    # 3. Отправка нового сообщения с основным меню
    await send_main_menu(user_id)


# --- CANCEL Handler ---
@user_router.callback_query(F.data.in_({'cmd_start', 'cancel_auth'}))
@admin_router.callback_query(F.data.in_({'cmd_start', 'cancel_auth', 'admin_panel'}))
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    
    # ... (логика очистки Temp Clients и Future остается без изменений) ...
    current_state = await state.get_state()
    if current_state:
        await state.clear()
    
    async with store.lock:
        client = store.temp_auth_clients.pop(user_id, None)
        future = store.qr_login_future.pop(user_id, None)
        
    if client:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass
    
    if future and not future.done():
        future.cancel()
        
    # 4. Возврат в главное меню
    if call.data == 'admin_panel' and user_id == ADMIN_ID:
        await call.answer()
        return await cb_admin_stats(call, state)
        
    # Используем call.message.message_id для редактирования
    await send_main_menu(user_id, call.message.message_id) 
    await call.answer()

# ... (Остальные хэндлеры для QR-Auth и Phone-Auth остаются без изменений, кроме этого одного блока) ...

# --- PASSWORD INPUT (УЛУЧШЕННАЯ ПРОСЬБА ПАРОЛЯ) ---
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
        
        await client(functions.auth.SignInRequest(
            phone_code=code,
            phone_number=phone_number,
            phone_code_hash=phone_code_hash
        ))
        
        await auth_success(user_id, client, state, msg_wait)

    except SessionPasswordNeededError:
        # 2FA требуется - УЛУЧШЕННОЕ СООБЩЕНИЕ
        await state.set_state(TelethonAuth.PASSWORD)
        await msg_wait.delete()
        await message.reply(
            "⚠️ **Включен Облачный Пароль (2FA)!**\n\n"
            "Telegram защищает ваш аккаунт дополнительным паролем. "
            "✍️ **Введите ваш Облачный Пароль:**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_auth")]])
        )
    # ... (Остальные except блоки остаются) ...
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


# ... (Хэндлер для TelethonAuth.PASSWORD остается без изменений) ...


# =========================================================================
# VII. ADMIN HANDLERS (УЛУЧШЕННАЯ ПАНЕЛЬ: ПРОСМОТР ПРОМОКОДОВ)
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
        [InlineKeyboardButton(text="📋 Показать Промокоды", callback_data="admin_view_promos")], # НОВАЯ КНОПКА
        [InlineKeyboardButton(text="🗑 Удалить Промокод", callback_data="admin_delete_promo_init")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="cmd_start")]
    ])
    
    try:
        await call.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest:
        pass 
    await call.answer()


# --- НОВЫЙ ХЕНДЛЕР: ПРОСМОТР ПРОМОКОДОВ ---
@admin_router.callback_query(F.data == "admin_view_promos")
async def cb_admin_view_promos(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    
    promocodes = await db.get_all_promocodes()
    
    if not promocodes:
        text = "🤷‍♂️ В базе данных нет активных промокодов."
    else:
        promo_list = []
        for p in promocodes:
            uses = '∞' if p['uses_left'] == 0 else p['uses_left']
            promo_list.append(
                f"• `{p['code']}` | {p['duration_days']} д. | {uses} исп."
            )
        
        text = (
            "📋 **СПИСОК АКТИВНЫХ ПРОМОКОДОВ**\n\n"
            "Код | Срок | Использований\n"
            "-----------------------------\n"
            f"{'\n'.join(promo_list)}\n\n"
            "Нажмите на код, чтобы скопировать его."
        )

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в Админ-панель", callback_data="admin_panel")]
    ])
    
    await call.message.edit_text(text, reply_markup=markup, parse_mode=ParseMode.MARKDOWN_V2) # Используем Markdown_V2 для `
    await call.answer()


# ... (Остальные хэндлеры для создания и удаления промокодов остаются без изменений) ...

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
            # Используем manager.start_client_task, который создает асинхронную задачу
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
