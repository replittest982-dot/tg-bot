import asyncio
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from io import BytesIO
import sqlite3 

# Third-party Imports
import aiosqlite
import pytz
import qrcode
from PIL import Image
from dotenv import load_dotenv

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, 
    BufferedInputFile
)
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError
from aiogram.enums import ParseMode 

# --- TELETHON ---
from telethon import TelegramClient, functions, utils
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError, 
    AuthKeyUnregisteredError, PhoneCodeInvalidError, 
    PhoneCodeExpiredError, PhoneNumberInvalidError, 
    PasswordHashInvalidError
)

# =========================================================================
# I. КОНФИГУРАЦИЯ И НАСТРОЙКА
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

os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# --- Настройка Логирования ---
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

# --- Инициализация Aiogram Роутеров ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)) 
dp = Dispatcher(storage=MemoryStorage())

user_router = Router(name='user_router')

# =========================================================================
# II. ХРАНИЛИЩЕ И СОСТОЯНИЯ (FSM)
# =========================================================================

class GlobalStorage:
    """Хранение данных в оперативной памяти."""
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {} 
        self.qr_login_future: Dict[int, asyncio.Future] = {} 
        self.active_workers: Dict[int, TelegramClient] = {} 

store = GlobalStorage()

# --- FSM States ---
class TelethonAuth(StatesGroup):
    WAITING_FOR_QR_SCAN = State()
    PHONE = State()
    CODE = State()
    PASSWORD = State() 

# --- УТИЛИТЫ ---
def get_session_path(user_id: int, is_temp: bool = False) -> str:
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def to_msk_aware(dt_str: str) -> Optional[datetime]:
    if not dt_str: return None
    try:
        naive_dt = datetime.strptime(dt_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        return TIMEZONE_MSK.localize(naive_dt)
    except Exception as e:
        logger.error(f"Failed to parse datetime string: {dt_str} ({e})")
        return None

async def safe_edit_or_send(
    chat_id: int, 
    text: str, 
    reply_markup: Optional[InlineKeyboardMarkup] = None, 
    message_id: Optional[int] = None, 
    bot_instance: Bot = bot
):
    """
    Централизованная функция для ОТПРАВКИ/РЕДАКТИРОВАНИЯ сообщений. 
    Использует delete+send для избежания Bad Request ошибок Aiogram.
    """
    
    if isinstance(reply_markup, int): reply_markup = None
        
    if message_id:
        try:
            # Пытаемся удалить старое сообщение
            await bot_instance.delete_message(chat_id, message_id)
        except TelegramAPIError:
            pass 
        except Exception as e:
            logger.warning(f"Unexpected error during delete for {chat_id}: {e}")

    # Отправляем новое сообщение.
    try:
        await bot_instance.send_message(chat_id, text, reply_markup=reply_markup)
    except Exception as e_send:
        logger.error(f"FATAL: Failed to send message to {chat_id}: {e_send}")

def normalize_phone(phone: str) -> str:
    phone = phone.strip()
    cleaned = re.sub(r'[^\d+]', '', phone)
    if not cleaned: return ""
    if cleaned.startswith('+'):
        return cleaned
    return '+' + cleaned

async def _new_telethon_client(user_id: int, is_temp: bool = False) -> TelegramClient:
    session_path = get_session_path(user_id, is_temp=is_temp)
    client = TelegramClient(
        session_path, API_ID, API_HASH, 
        device_model="Worker StatPro", 
        flood_sleep_threshold=15
    )
    return client

# =========================================================================
# III. БАЗА ДАННЫХ (AsyncDatabase)
# =========================================================================

class AsyncDatabase:
    """Управление асинхронной базой данных SQLite."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db_pool: Optional[aiosqlite.Connection] = None

    async def init(self):
        try:
            self.db_pool = await aiosqlite.connect(self.db_path, isolation_level=None) 
            await self.db_pool.execute("PRAGMA journal_mode=WAL;")
            await self.db_pool.execute("PRAGMA synchronous=NORMAL;")
            self.db_pool.row_factory = aiosqlite.Row
            
            await self.db_pool.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    telethon_active BOOLEAN DEFAULT 0,
                    subscription_end TEXT,
                    is_banned BOOLEAN DEFAULT 0
                )
            """)
            if ADMIN_ID != 0:
                await self.db_pool.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (ADMIN_ID,))
            
            await self.db_pool.commit()
            logger.info("Database initialized successfully.")
        except sqlite3.OperationalError as e:
            logger.critical(f"FATAL DB ERROR: Cannot open database file {self.db_path}. Error: {e}")
            sys.exit(1) 

    async def get_user(self, user_id: int):
        if not self.db_pool: return None
        await self.db_pool.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await self.db_pool.commit()
        
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

    async def set_telethon_status(self, user_id: int, status: bool):
        if not self.db_pool: return
        await self.db_pool.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
        await self.db_pool.commit()
        
    async def get_active_telethon_users(self) -> List[int]: 
        if not self.db_pool: return []
        now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        async with self.db_pool.execute("SELECT user_id FROM users WHERE telethon_active=1 AND is_banned=0 AND (subscription_end IS NULL OR subscription_end > ?)", (now_str,)) as cursor:
            return [row[0] for row in await cursor.fetchall()]

db = AsyncDatabase(os.path.join(DATA_DIR, DB_NAME))


# =========================================================================
# IV. TELETHON MANAGER 
# =========================================================================

class TelethonManager:
    """Управление сессиями Telethon и авторизацией."""
    def __init__(self, bot_instance: Bot, db_instance: AsyncDatabase):
        self.bot = bot_instance
        self.db = db_instance
        self.API_ID = API_ID
        self.API_HASH = API_HASH
        self.tasks_lock = asyncio.Lock() 

    async def _send_to_bot_user(self, user_id: int, message: str, reply_markup: Optional[InlineKeyboardMarkup] = None, message_id: Optional[int] = None):
        """Безопасная отправка/редактирование сообщения пользователю бота."""
        await safe_edit_or_send(user_id, message, reply_markup, message_id, bot_instance=self.bot)
    
    async def _cleanup_temp_session(self, user_id: int):
        """Отключает временный клиент и удаляет временный файл сессии."""
        async with store.lock:
            client = store.temp_auth_clients.pop(user_id, None)
            qr_future = store.qr_login_future.pop(user_id, None)
            
            if qr_future and not qr_future.done():
                qr_future.cancel()
        
        if client:
            try:
                if hasattr(client, "is_connected") and await client.is_connected(): await client.disconnect() 
            except Exception:
                pass
                
        path_temp = get_session_path(user_id, is_temp=True) 
        try:
             # Удаляем все файлы, связанные с временной сессией
             for ext in ['.session', '.session-journal', '.session-shm', '.session-wal']:
                 file_path = path_temp + ext
                 if os.path.exists(file_path):
                     os.remove(file_path)
             logger.info(f"Worker {user_id}: Temporary session files cleaned up.")
        except OSError as e: 
             logger.error(f"Worker {user_id}: Failed to delete temporary session file: {e}")
            
    async def handle_telethon_error(self, user_id: int, error_type: str, e: Exception, message: str):
        """Обработчик ошибок Telethon/SQLite."""
        logger.error(f"Worker {user_id}: Critical {error_type} error: {type(e).__name__} - {e}")
        await self._send_to_bot_user(user_id, message)
        await self._cleanup_temp_session(user_id) 

    async def start_worker_session(self, user_id: int, client_temp: TelegramClient):
        """Сохраняет сессию в постоянный файл, удаляет временный и запускает Worker."""
        path_perm = get_session_path(user_id)
        await self.stop_worker(user_id, silent=True)
        client_perm = None
        
        try:
            if not await client_temp.is_connected():
                await client_temp.connect() 
                
            client_perm = await _new_telethon_client(user_id, is_temp=False) 
            client_perm._copy_session_from(client_temp) 
            client_perm.session.save()
            logger.info(f"Worker {user_id}: Session successfully copied and saved to permanent path.")

        except (sqlite3.OperationalError, Exception) as e:
            await self.handle_telethon_error(user_id, "Session Save", e, "❌ Критическая ошибка при сохранении сессии. Повторите вход.")
            if client_perm and hasattr(client_perm, "is_connected") and await client_perm.is_connected(): 
                try: await client_perm.disconnect() 
                except: pass
            await self._cleanup_temp_session(user_id) 
            return

        await self._cleanup_temp_session(user_id) 
        if client_perm and hasattr(client_perm, "is_connected") and await client_perm.is_connected():
            try: await client_perm.disconnect() 
            except: pass
        
        if os.path.exists(path_perm + '.session'): 
            await self.start_client_task(user_id) 
            await self._send_to_bot_user(user_id, "✅ **Авторизация прошла успешно!** Worker запускается...")
        else:
             logger.error(f"Worker {user_id}: Failed to find permanent session after save operation.")
             await self._send_to_bot_user(user_id, "❌ Критическая ошибка: Файл постоянной сессии не найден. Повторите вход.")


    async def start_client_task(self, user_id: int):
        """Запускает Worker в фоновой задаче."""
        await self.stop_worker(user_id, silent=True)
        session_path = get_session_path(user_id) + '.session'
        
        if not os.path.exists(session_path):
             await self.db.set_telethon_status(user_id, False)
             return
             
        try:
            async with self.tasks_lock:
                 if user_id in store.active_workers: return

            task = asyncio.create_task(self._run_worker(user_id), name=f"main-worker-{user_id}")
            logger.info(f"Worker {user_id}: Main worker task created and scheduled.")
            return task
        except Exception as e:
            logger.critical(f"Worker {user_id}: Critical error starting client task: {e}")
            await self.db.set_telethon_status(user_id, False)


    async def _run_worker(self, user_id: int): 
        """Основная логика Worker'а - просто поддержание активности и обработка."""
        path = get_session_path(user_id)
        client = TelegramClient(path, self.API_ID, self.API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        async with self.tasks_lock: 
            if user_id in store.active_workers: return 
            store.active_workers[user_id] = client 
        
        try:
            await client.connect()
            if not await client.is_user_authorized(): 
                raise AuthKeyUnregisteredError('Session expired or unauthorized')

            sub_end = await self.db.get_subscription_status(user_id)
            now_msk = datetime.now(TIMEZONE_MSK)

            if not sub_end or sub_end <= now_msk:
                # В случае истечения подписки, отключаем и обновляем статус
                await self._send_to_bot_user(user_id, "⚠️ **Срок вашей подписки истек!** Worker отключен. Продлите подписку, чтобы продолжить работу.")
                await client.disconnect() 
                return 
            
            await self.db.set_telethon_status(user_id, True)
            me = await client.get_me()
            logger.info(f"Worker {user_id} ({utils.get_display_name(me)}) started successfully.")
            
            # Отправка стартового сообщения (чтобы юзер знал, что запущен)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Остановить Worker", callback_data="stop_worker")]
            ])
            await self._send_to_bot_user(user_id, 
                f"✅ **Worker запущен!** Аккаунт: <b>{utils.get_display_name(me)}</b>.\n"
                f"Ожидаю команды /start для перехода в главное меню.",
                reply_markup=kb
            )

            # Основной цикл Worker'а: просто ждем, пока его не остановят или не истечет сессия
            # В реальном приложении здесь будет логика обработки апдейтов (client.run_until_disconnected())
            await client.run_until_disconnected()

        except AuthKeyUnregisteredError:
            await self.handle_telethon_error(user_id, "Auth Key", Exception("AuthKeyUnregisteredError"), "❌ Сессия истекла! Пожалуйста, авторизуйтесь снова.")
        except FloodWaitError as e:
            await self.handle_telethon_error(user_id, "Flood Wait", e, f"⚠️ **Flood Wait.** Worker получил блокировку на {e.seconds} секунд.")
        except Exception as e:
            logger.error(f"Worker {user_id}: Unhandled error in _run_worker: {type(e).__name__} - {e}")
        finally:
            await self.db.set_telethon_status(user_id, False)
            try:
                if await client.is_connected(): await client.disconnect()
            except Exception:
                pass 
                
            async with self.tasks_lock:
                store.active_workers.pop(user_id, None)
            logger.info(f"Worker {user_id}: Task finished and client disconnected.")


    async def stop_worker(self, user_id: int, silent: bool = False):
        """Останавливает Worker."""
        async with self.tasks_lock:
            client = store.active_workers.pop(user_id, None)
            
        if client:
            try:
                await client.disconnect()
                await self.db.set_telethon_status(user_id, False)
                logger.info(f"Worker {user_id}: Explicitly stopped and disconnected.")
                if not silent:
                    await self._send_to_bot_user(user_id, "🛑 **Worker остановлен.**")
                return True
            except Exception as e:
                logger.error(f"Worker {user_id}: Error during graceful disconnect: {e}")
        return False
        
    async def get_worker_status(self, user_id: int) -> bool:
        """Проверка, активен ли Worker в памяти."""
        return user_id in store.active_workers


manager = TelethonManager(bot, db)

# =========================================================================
# V. ХЕНДЛЕРЫ AIOGRAM
# =========================================================================

# --- КЛАВИАТУРЫ ---

async def main_menu_keyboard(user_id: int, db_user_data: Dict[str, Any], is_worker_active: bool) -> InlineKeyboardMarkup:
    kb_content = []
    
    # --- СТАТУС WORKER'А ---
    if is_worker_active:
        kb_content.append([InlineKeyboardButton(text="❌ Остановить Worker", callback_data="stop_worker")])
    elif db_user_data.get('telethon_active'):
        # Если в базе активен, но в памяти нет (например, после перезапуска бота)
        kb_content.append([InlineKeyboardButton(text="🔄 Перезапустить Worker", callback_data="restart_worker")])
    else:
        kb_content.append([InlineKeyboardButton(text="🔑 Войти (QR/Phone)", callback_data="auth_init")])
        
    # --- ДЕЙСТВИЯ ---
    kb_content.append([
        InlineKeyboardButton(text="💰 Промокоды (Заглушка)", callback_data="promos"),
        InlineKeyboardButton(text="📝 Инструкция", url="https://telegra.ph/instructions-01-01") # Пример ссылки
    ])
    
    # --- ПОДПИСКА и ПОДДЕРЖКА ---
    sub_end = to_msk_aware(db_user_data.get('subscription_end'))
    if sub_end and sub_end > datetime.now(TIMEZONE_MSK):
        days_left = (sub_end - datetime.now(TIMEZONE_MSK)).days
        kb_content.append([InlineKeyboardButton(text=f"🟢 Подписка: {days_left} дн.", callback_data="sub_status")])
    else:
        kb_content.append([InlineKeyboardButton(text="🔴 Подписка не активна", callback_data="sub_status")])
        
    kb_content.append([InlineKeyboardButton(text="💬 Поддержка (Заглушка)", callback_data="support")])
    
    # --- АДМИН-ПАНЕЛЬ (Рабочая заглушка) ---
    if user_id == ADMIN_ID:
        kb_content.append([InlineKeyboardButton(text="⚙️ Админ-панель (Заглушка)", callback_data="admin_panel")])
        
    return InlineKeyboardMarkup(inline_keyboard=kb_content)


async def main_menu_text(db_user_data: Dict[str, Any], is_worker_active: bool) -> str:
    status = "🟢 Активен" if is_worker_active else "🔴 Отключен"
    
    sub_end = to_msk_aware(db_user_data.get('subscription_end'))
    sub_text = "Не активна"
    if sub_end and sub_end > datetime.now(TIMEZONE_MSK):
        sub_text = f"До {sub_end.strftime('%d.%m.%Y %H:%M')}"
        
    return (
        "🤖 **Главное Меню Worker Bot**\n"
        "➖➖➖➖➖➖➖➖➖➖\n"
        f"Статус Worker'а: **{status}**\n"
        f"Статус Подписки: **{sub_text}**\n"
        "Выберите действие:"
    )

# --- БАЗОВЫЕ ХЕНДЛЕРЫ ---

@user_router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    
    db_user = await db.get_user(user_id)
    is_worker_active = await manager.get_worker_status(user_id)
    
    kb = await main_menu_keyboard(user_id, db_user, is_worker_active)
    text = await main_menu_text(db_user, is_worker_active)
    
    await message.answer(text, reply_markup=kb)

@user_router.callback_query(F.data == "back_to_menu")
async def cb_back_to_menu(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await state.clear()
    
    db_user = await db.get_user(user_id)
    is_worker_active = await manager.get_worker_status(user_id)
    
    kb = await main_menu_keyboard(user_id, db_user, is_worker_active)
    text = await main_menu_text(db_user, is_worker_active)
    
    try:
        await callback.message.edit_text(text, reply_markup=kb)
        await callback.answer()
    except TelegramAPIError as e:
        if 'message is not modified' in str(e):
            await callback.answer("Вы уже в главном меню.")
        else:
            # Если не удалось отредактировать (например, из-за старого сообщения), отправляем новое
            await safe_edit_or_send(user_id, text, reply_markup=kb, message_id=callback.message.message_id)


# --- ХЕНДЛЕРЫ WORKER'А ---

@user_router.callback_query(F.data == "stop_worker")
async def cb_stop_worker(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    success = await manager.stop_worker(user_id)
    
    if success:
        await callback.answer("Worker остановлен.", show_alert=True)
    else:
        await callback.answer("Worker не был активен.", show_alert=True)
        
    await state.clear()
    db_user = await db.get_user(user_id)
    is_worker_active = await manager.get_worker_status(user_id)
    kb = await main_menu_keyboard(user_id, db_user, is_worker_active)
    text = await main_menu_text(db_user, is_worker_active)
    
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except TelegramAPIError:
        pass


@user_router.callback_query(F.data == "restart_worker")
async def cb_restart_worker(callback: CallbackQuery):
    user_id = callback.from_user.id
    # Пробуем запустить Worker
    task = await manager.start_client_task(user_id)
    
    if task:
        await callback.answer("Попытка запуска Worker...", show_alert=True)
    else:
        await callback.answer("Файл сессии не найден. Требуется авторизация.", show_alert=True)
        
    db_user = await db.get_user(user_id)
    is_worker_active = await manager.get_worker_status(user_id)
    kb = await main_menu_keyboard(user_id, db_user, is_worker_active)
    text = await main_menu_text(db_user, is_worker_active)
    
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except TelegramAPIError:
        pass


# --- ХЕНДЛЕРЫ АВТОРИЗАЦИИ (QR & PHONE) ---

@user_router.callback_query(F.data == "cancel_auth")
async def cb_cancel_auth(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await manager._cleanup_temp_session(user_id)
    await state.clear()
    
    await callback.answer("Авторизация отменена.")
    # Возвращаемся в меню
    db_user = await db.get_user(user_id)
    is_worker_active = await manager.get_worker_status(user_id)
    kb = await main_menu_keyboard(user_id, db_user, is_worker_active)
    text = await main_menu_text(db_user, is_worker_active)
    
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except TelegramAPIError:
        pass


@user_router.callback_query(F.data == "auth_init")
async def cb_auth_init(callback: CallbackQuery, state: FSMContext):
    """Начало авторизации - попытка QR-кода."""
    user_id = callback.from_user.id
    
    # 1. Очистка старых/временных сессий
    await manager._cleanup_temp_session(user_id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Войти по номеру", callback_data="auth_phone_init")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_auth")]
    ])
    
    await callback.message.edit_text("⏳ **Получаю ссылку для QR-кода...**", reply_markup=kb)
    
    try:
        # 2. Создание временного клиента и запрос QR-ссылки
        client = await _new_telethon_client(user_id, is_temp=True)
        await client.connect()

        # Запрос токена для QR-кода
        qr_login = await client(functions.auth.ExportLoginTokenRequest(
            api_id=API_ID,
            api_hash=API_HASH,
            except_ids=[user_id] 
        ))

        # 3. Обработка токена
        qr_url = getattr(qr_login, 'url', None)

        if not qr_url:
            # Обработка ошибки: URL не получен (часто из-за миграции DC)
            raise AttributeError("LoginToken has no usable 'url'.")

        # 4. Сохранение клиента и Future в Глобальном хранилище
        async with store.lock:
            store.temp_auth_clients[user_id] = client
            # Создаем Future для ожидания сканирования
            login_future = asyncio.get_event_loop().create_future()
            store.qr_login_future[user_id] = login_future

        # 5. Генерация QR-кода
        qr = qrcode.QRCode(version=1, box_size=10, border=4, error_correction=qrcode.constants.ERROR_CORRECT_L)
        qr.add_data(qr_url)
        qr.make(fit=True)
        img: Image.Image = qr.make_image(fill_color="black", back_color="white")
        
        bio = BytesIO()
        img.save(bio, 'PNG')
        bio.seek(0)
        
        # 6. Отправка QR-кода и ожидание сканирования
        message_to_delete = await callback.message.edit_text("⏳ Отправляю QR-код...")
        
        await bot.send_photo(
            chat_id=user_id,
            photo=BufferedInputFile(bio.read(), filename="qr_code.png"),
            caption="📸 **QR-авторизация**\n"
                    "Отсканируйте этот код с помощью официального приложения Telegram (Настройки -> Устройства -> Привязать устройство).\n"
                    "Код активен **2 минуты**.",
            reply_markup=kb
        )
        await state.set_state(TelethonAuth.WAITING_FOR_QR_SCAN)
        
        # Удаляем временное сообщение "Отправляю QR-код..."
        try: await bot.delete_message(user_id, message_to_delete.message_id) 
        except: pass
        
        # 7. Запуск таска ожидания логина
        asyncio.create_task(manager._wait_for_qr_login(user_id, client, login_future))

    except AttributeError as e:
        logger.error(f"QR Auth Error for {user_id}: Returned object {e}. Falling back to phone auth suggestion.")
        await manager._cleanup_temp_session(user_id)
        kb_phone = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📞 Войти по номеру", callback_data="auth_phone_init")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="auth_init")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_auth")]
        ])
        await callback.message.edit_text(
            "⚠️ **Ошибка QR-авторизации:** Не удалось получить ссылку для QR-кода (возможно, из-за миграции DC или настроек сессии).\n"
            "Пожалуйста, воспользуйтесь **входом по номеру телефона**.",
            reply_markup=kb_phone
        )
    except Exception as e:
        await manager.handle_telethon_error(user_id, "QR Auth Init", e, f"❌ Непредвиденная ошибка при запросе QR-кода: {type(e).__name__}.")

    await callback.answer()


@user_router.callback_query(F.data == "auth_phone_init")
async def cb_auth_phone_init(callback: CallbackQuery, state: FSMContext):
    """Начало авторизации по номеру телефона."""
    user_id = callback.from_user.id
    
    await manager._cleanup_temp_session(user_id) 
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_auth")]
    ])
    
    await callback.message.edit_text(
        "📞 **Введите номер телефона**\n"
        "Пожалуйста, введите номер в международном формате (например, `+79XXXXXXXXX`):",
        reply_markup=kb
    )
    await state.set_state(TelethonAuth.PHONE)
    await callback.answer()

@user_router.message(TelethonAuth.PHONE)
async def process_phone(message: Message, state: FSMContext):
    user_id = message.from_user.id
    phone = normalize_phone(message.text)
    
    if not phone or len(phone) < 10:
        await message.reply("❌ Неверный формат. Пожалуйста, введите номер телефона в формате `+79XXXXXXXXX`.")
        return
        
    await safe_edit_or_send(user_id, "⏳ Отправляю запрос на код...", message_id=message.message_id)

    try:
        client = await _new_telethon_client(user_id, is_temp=True)
        await client.connect()
        
        phone_hash = await client.send_code_request(phone)
        
        await state.update_data(phone=phone, phone_hash=phone_hash.phone_code_hash)
        async with store.lock:
            store.temp_auth_clients[user_id] = client 
            
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_auth")]
        ])

        await safe_edit_or_send(user_id, 
            "🔢 **Введите код подтверждения**\n"
            "Код был отправлен в официальное приложение Telegram.", 
            reply_markup=kb, message_id=message.message_id
        )
        await state.set_state(TelethonAuth.CODE)

    except PhoneNumberInvalidError:
        await manager.handle_telethon_error(user_id, "Phone Auth", Exception("PhoneNumberInvalidError"), "❌ Неверный номер телефона. Пожалуйста, проверьте формат и повторите ввод.")
        await state.set_state(TelethonAuth.PHONE) 
    except FloodWaitError as e:
        await manager.handle_telethon_error(user_id, "Phone Auth", e, f"⚠️ FloodWait: Пожалуйста, подождите {e.seconds} секунд, прежде чем снова запрашивать код.")
        await state.clear()
    except Exception as e:
        await manager.handle_telethon_error(user_id, "Phone Auth", e, f"❌ Непредвиденная ошибка при запросе кода: {type(e).__name__}.")
        await state.clear()


@user_router.message(TelethonAuth.CODE)
async def process_code(message: Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip()
    
    if not code.isdigit():
        await message.reply("❌ Код должен состоять только из цифр.")
        return

    data = await state.get_data()
    phone = data.get('phone')
    phone_hash = data.get('phone_hash')
    
    if phone is None or phone_hash is None:
        await message.reply("❌ Ошибка сессии. Пожалуйста, начните авторизацию заново.", 
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔑 Войти", callback_data="auth_init")]]))
        await state.clear()
        return

    client = store.temp_auth_clients.get(user_id)
    if client is None:
        await message.reply("❌ Ошибка клиента. Пожалуйста, начните авторизацию заново.")
        await state.clear()
        return

    await safe_edit_or_send(user_id, "⏳ Проверяю код...", message_id=message.message_id)

    try:
        await client.sign_in(phone, code, phone_code_hash=phone_hash)
        
        # --- УСПЕШНЫЙ ВХОД БЕЗ 2FA ---
        await manager.start_worker_session(user_id, client)
        await state.clear()
        
    except SessionPasswordNeededError:
        # --- ТРЕБУЕТСЯ ПАРОЛЬ (2FA) ---
        await safe_edit_or_send(user_id, 
            "🔒 <b>ТРЕБУЕТСЯ ПАРОЛЬ (2FA)</b>\n"
            "Ваш аккаунт защищен двухфакторной аутентификацией. Введите ваш облачный пароль:", 
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_auth")]])
        )
        await state.set_state(TelethonAuth.PASSWORD)
        
    except PhoneCodeInvalidError:
        await safe_edit_or_send(user_id, "❌ Неверный код. Пожалуйста, попробуйте снова:", message_id=message.message_id)
    except PhoneCodeExpiredError:
        await manager.handle_telethon_error(user_id, "Code Auth", Exception("PhoneCodeExpiredError"), "❌ Срок действия кода истек. Пожалуйста, начните авторизацию заново.")
        await state.clear()
    except Exception as e:
        await manager.handle_telethon_error(user_id, "Code Auth", e, f"❌ Непредвиденная ошибка при проверке кода: {type(e).__name__}.")
        await state.clear()
        
        
@user_router.message(TelethonAuth.PASSWORD)
async def process_password(message: Message, state: FSMContext):
    user_id = message.from_user.id
    password = message.text.strip()
    
    client = store.temp_auth_clients.get(user_id)
    if client is None:
        await message.reply("❌ Ошибка клиента. Пожалуйста, начните авторизацию заново.")
        await state.clear()
        return
        
    await safe_edit_or_send(user_id, "⏳ Проверяю пароль...", message_id=message.message_id)

    try:
        await client.sign_in(password=password)
        
        # --- УСПЕШНЫЙ ВХОД С 2FA ---
        await manager.start_worker_session(user_id, client)
        await state.clear()
        
    except PasswordHashInvalidError:
        await safe_edit_or_send(user_id, "❌ Неверный пароль. Пожалуйста, попробуйте снова:", message_id=message.message_id)
    except Exception as e:
        await manager.handle_telethon_error(user_id, "Password Auth", e, f"❌ Непредвиденная ошибка при проверке пароля: {type(e).__name__}.")
        await state.clear()

# --- ХЕНДЛЕРЫ ЗАГЛУШЕК (СТУБЫ) ---

@user_router.callback_query(F.data == "promos")
async def cb_promos(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")]
    ])
    await callback.message.edit_text(
        "💰 **Промокоды (Заглушка)**\n"
        "Эта функция будет добавлена позже. Сейчас здесь должна быть форма для ввода промокода.",
        reply_markup=kb
    )
    await callback.answer()

@user_router.callback_query(F.data == "support")
async def cb_support(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")]
    ])
    await callback.message.edit_text(
        "💬 **Поддержка (Заглушка)**\n"
        "По всем вопросам обращайтесь к менеджеру:\n"
        "[@YourSupportManagerUsername](https://t.me/YourSupportManagerUsername)\n"
        "*Не забудьте заменить эту ссылку в реальном проекте!*",
        reply_markup=kb
    )
    await callback.answer()

@user_router.callback_query(F.data == "sub_status")
async def cb_sub_status(callback: CallbackQuery):
    user_id = callback.from_user.id
    db_user = await db.get_user(user_id)
    sub_end = to_msk_aware(db_user.get('subscription_end'))
    
    now_msk = datetime.now(TIMEZONE_MSK)
    
    if sub_end and sub_end > now_msk:
        days_left = (sub_end - now_msk).days
        hours_left = (sub_end - now_msk).seconds // 3600
        message = (
            "✅ **Ваша подписка активна!**\n"
            f"Осталось: **{days_left} дней и {hours_left} часов**.\n"
            f"Дата окончания: {sub_end.strftime('%d.%m.%Y в %H:%M:%S')} МСК."
        )
    else:
        message = (
            "🔴 **Подписка не активна.**\n"
            "Пожалуйста, приобретите или активируйте подписку, чтобы запустить Worker.\n"
            "*(Примечание: Добавьте здесь ссылку на оплату)*"
        )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")]
    ])
    await callback.message.edit_text(message, reply_markup=kb)
    await callback.answer()

# --- ХЕНДЛЕР АДМИН-ПАНЕЛИ (Рабочая заглушка) ---

@user_router.callback_query(F.data == "admin_panel")
async def cb_admin_panel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("У вас нет доступа к этому разделу.", show_alert=True)
        return
        
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Управление юзерами", callback_data="admin_users")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")]
    ])
    await callback.message.edit_text(
        "⚙️ **Админ-панель (Заглушка)**\n"
        "Здесь вы сможете управлять пользователями, выдавать подписки и т.д.\n"
        "*(Эта функция пока не реализована полностью)*",
        reply_markup=kb
    )
    await callback.answer()

@user_router.callback_query(F.data == "admin_users")
async def cb_admin_users(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("У вас нет доступа к этому разделу.", show_alert=True)
        return
        
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_panel")]
    ])
    
    # Пример функциональности: получение статистики
    active_workers = await db.get_active_telethon_users()
    
    await callback.message.edit_text(
        "👥 **Управление юзерами (Заглушка)**\n"
        "Статистика:\n"
        f"Активных Worker'ов: **{len(active_workers)}**\n"
        "*(Здесь будет логика выдачи подписки)*",
        reply_markup=kb
    )
    await callback.answer()


# =========================================================================
# VI. ЗАПУСК БОТА
# =========================================================================

async def on_startup(dispatcher: Dispatcher, bot: Bot):
    """Вызывается при запуске бота."""
    logger.info("Starting up and attempting to restore active workers...")
    await db.init()
    
    # 1. Запуск всех активных Worker'ов из базы данных (после перезагрузки)
    active_users = await db.get_active_telethon_users()
    for user_id in active_users:
        # Проверяем наличие файла сессии
        session_path = get_session_path(user_id) + '.session'
        if os.path.exists(session_path):
            await manager.start_client_task(user_id)
        else:
            await db.set_telethon_status(user_id, False)

    # 2. Отправка уведомления админу (если ID указан)
    if ADMIN_ID != 0:
        try:
            await bot.send_message(ADMIN_ID, "🟢 Бот успешно перезапущен и готов к работе.")
        except Exception:
            logger.warning(f"Failed to send startup message to Admin ID {ADMIN_ID}. Check the ID and access.")
            
    logger.info("Starting Aiogram Bot polling...")


async def main():
    dp.include_router(user_router)
    # Запускаем on_startup перед началом polling
    dp.startup.register(on_startup) 
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.critical(f"FATAL: Aiogram polling failed: {e}")

if __name__ == "__main__":
    # Если вы хотите, чтобы ваш единственный пользователь (вы) имел подписку для тестирования
    async def create_initial_subscription():
        if ADMIN_ID != 0:
            await db.init()
            # Устанавливаем подписку на 30 дней для админа (для тестирования Worker'а)
            sub_end = datetime.now(TIMEZONE_MSK) + timedelta(days=30)
            await db.db_pool.execute("UPDATE users SET subscription_end=? WHERE user_id=?", (sub_end.strftime('%Y-%m-%d %H:%M:%S'), ADMIN_ID))
            await db.db_pool.commit()
            logger.info(f"Admin ID {ADMIN_ID} subscription set to {sub_end.strftime('%Y-%m-%d %H:%M:%S')}")
            
    asyncio.run(create_initial_subscription())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by KeyboardInterrupt.")
