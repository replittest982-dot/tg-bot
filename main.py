import asyncio
import logging
import logging.handlers
import os
import re
import random
import string
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Set, Any
import concurrent.futures

# Third-party Imports
import aiosqlite
import pytz
from dotenv import load_dotenv

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError
from aiogram.enums import ParseMode 

# --- TELETHON ---
from telethon import TelegramClient, utils
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError, 
    AuthKeyUnregisteredError, PhoneCodeInvalidError, 
    PhoneCodeExpiredError, RpcCallFailError 
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

# Executor для синхронных задач (заглушка)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# --- Инициализация Aiogram Роутеров ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)) 
dp = Dispatcher(storage=MemoryStorage())

user_router = Router(name='user_router')
# Строгая фильтрация по ID администратора
admin_router = Router(name='admin_router', filters=[F.from_user.id == ADMIN_ID])

# =========================================================================
# II. ХРАНИЛИЩЕ И СОСТОЯНИЯ (FSM)
# =========================================================================

class GlobalStorage:
    """Глобальное хранилище данных в RAM для управления воркерами и авторизацией."""
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {} 
        self.qr_login_future: Dict[int, asyncio.Future] = {} 
        self.active_workers: Dict[int, TelegramClient] = {} # {user_id: TelethonClient}
        self.premium_users: Set[int] = set() 

store = GlobalStorage()

# --- FSM States ---
class TelethonAuth(StatesGroup):
    """Состояния для процесса авторизации Telethon."""
    WAITING_FOR_QR_SCAN = State()
    PHONE = State()
    CODE = State()
    PASSWORD = State() 

class PromoStates(StatesGroup):
    """Общее состояние для ввода промокода (User: активация, Admin: удаление)."""
    WAITING_CODE = State()

class AdminPromo(StatesGroup):
    """Состояния для создания промокода."""
    WAITING_DAYS = State() 
    WAITING_USES = State() 

# --- УТИЛИТЫ ---
def get_session_path(user_id: int, is_temp: bool = False) -> str:
    """Возвращает базовый путь сессии (без расширения .session)."""
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def to_msk_aware(dt_str: str) -> Optional[datetime]:
    """Преобразует строку даты/времени из БД в объект datetime с часовым поясом MSK."""
    if not dt_str: return None
    try:
        naive_dt = datetime.strptime(dt_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        return TIMEZONE_MSK.localize(naive_dt)
    except ValueError:
        logger.error(f"Failed to parse datetime string: {dt_str}")
        return None

async def safe_edit_or_send(
    chat_id: int, 
    text: str, 
    reply_markup: Optional[InlineKeyboardMarkup] = None, 
    message_id: Optional[int] = None, 
    bot_instance: Bot = bot
):
    """Централизованная функция для отказоустойчивой отправки/редактирования сообщений."""
    if message_id:
        try:
            await bot_instance.edit_message_text(
                text, str(chat_id), message_id, reply_markup=reply_markup
            )
            return
        except TelegramAPIError as e:
            logger.warning(f"Failed to edit message {message_id} for {chat_id}: {e}. Sending new message.")
    
    # Отправляем новое сообщение (Failover или первоначальная отправка)
    try:
        await bot_instance.send_message(chat_id, text, reply_markup=reply_markup)
    except Exception as e_send:
        logger.error(f"FATAL: Failed to send message to {chat_id}: {e_send}")

def generate_promocode(length=8) -> str:
    """Генерирует случайный промокод."""
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(characters) for i in range(length))


# =========================================================================
# III. БАЗА ДАННЫХ (AsyncDatabase)
# =========================================================================

class AsyncDatabase:
    """Управление асинхронной базой данных SQLite."""
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
    """Управление жизненным циклом Telethon Worker'ов (запуск, остановка, авторизация)."""
    def __init__(self, bot_instance: Bot, db_instance: AsyncDatabase):
        self.bot = bot_instance
        self.db = db_instance
        self.API_ID = API_ID
        self.API_HASH = API_HASH
        self.tasks_lock = asyncio.Lock() 

    async def _send_to_bot_user(self, user_id: int, message: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
        """Хелпер для отправки сообщений пользователю через Aiogram."""
        await safe_edit_or_send(user_id, message, reply_markup, bot_instance=self.bot)
    
    async def start_worker_session(self, user_id: int, client: TelegramClient):
        """
        Завершает авторизацию: переносит временную сессию в постоянную, 
        удаляет временные файлы и запускает воркер.
        """
        path_perm_base = get_session_path(user_id)
        path_temp_base = get_session_path(user_id, is_temp=True)
        path_perm = path_perm_base + '.session'
        path_temp = path_temp_base + '.session'

        # Очистка временных клиентов из RAM
        async with store.lock:
            store.temp_auth_clients.pop(user_id, None)

        # Отключение клиента перед переименованием
        if client:
            try:
                if await client.is_connected(): await client.disconnect()
            except Exception:
                pass

        if os.path.exists(path_temp):
            logger.info(f"Worker {user_id}: Found temp session. Moving to permanent.")
            
            # Удаление старой постоянной сессии, если существует
            if os.path.exists(path_perm): 
                os.remove(path_perm)
                logger.warning(f"Worker {user_id}: Overwrote existing permanent session.")
            
            os.rename(path_temp, path_perm)
            
            # Проверка и запуск
            if os.path.exists(path_perm): 
                logger.info(f"Worker {user_id}: Session moved successfully. Starting task.")
                await self.start_client_task(user_id) 
            else:
                 logger.error(f"Worker {user_id}: Failed to find permanent session after rename operation.")
                 await self._send_to_bot_user(user_id, "❌ Критическая ошибка при сохранении сессии. Повторите вход.")
                 
            # Очистка временного файла
            if os.path.exists(path_temp): 
                try: os.remove(path_temp)
                except OSError as e: logger.error(f"Worker {user_id}: Failed to delete temporary session file: {e}") 
        else:
            logger.error(f"Worker {user_id}: Temp session file not found during session finish.")
            await self._send_to_bot_user(user_id, "❌ Файл сессии не найден. Авторизация не завершена.")


    async def start_client_task(self, user_id: int):
        """Создает и запускает асинхронную задачу для основного Telethon Worker."""
        await self.stop_worker(user_id)
        
        session_path = get_session_path(user_id) + '.session'
        if not os.path.exists(session_path):
             logger.warning(f"Worker {user_id}: Attempted to start, but permanent session file not found.")
             await self.db.set_telethon_status(user_id, False)
             return
             
        try:
            task = asyncio.create_task(self._run_worker(user_id), name=f"main-worker-{user_id}")
            logger.info(f"Worker {user_id}: Main worker task created and scheduled.")
            return task
        except Exception as e:
            logger.critical(f"Worker {user_id}: Critical error starting client task: {e}")
            await self.db.set_telethon_status(user_id, False)

    async def _run_worker(self, user_id: int): 
        """Основной цикл работы Telethon клиента."""
        path = get_session_path(user_id)
        client = TelegramClient(path, self.API_ID, self.API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        async with self.tasks_lock: 
            if user_id in store.active_workers:
                logger.warning(f"Worker {user_id}: Duplicate task detected. Disconnecting new client.")
                await client.disconnect()
                return 
            store.active_workers[user_id] = client 
        
        try:
            await client.connect()
            if not await client.is_user_authorized(): 
                logger.error(f"Worker {user_id}: Client is not authorized after connection attempt.")
                raise AuthKeyUnregisteredError('Session expired or unauthorized')

            sub_end = await self.db.get_subscription_status(user_id)
            if not sub_end or sub_end <= datetime.now(TIMEZONE_MSK):
                logger.info(f"Worker {user_id}: Subscription expired. Stopping worker.")
                await self._send_to_bot_user(user_id, "⚠️ Подписка истекла. Worker отключен.")
                await client.disconnect() 
                return 
            
            await self.db.set_telethon_status(user_id, True)
            me = await client.get_me()
            logger.info(f"Worker {user_id} ({utils.get_display_name(me)}) started successfully.")
            await self._send_to_bot_user(user_id, f"✅ Worker запущен! Аккаунт: <b>{utils.get_display_name(me)}</b>\nСтатистика активна. Время подписки до: {sub_end.strftime('%d.%m.%Y %H:%M')}")
            
            await client.run_until_disconnected() 
            
        except AuthKeyUnregisteredError:
            logger.error(f"Worker {user_id}: Session expired (AuthKeyUnregisteredError). Deleting session file.")
            await self._send_to_bot_user(user_id, "❌ Сессия истекла/отозвана. Требуется повторный вход.")
            session_file = path + '.session'
            if os.path.exists(session_file): os.remove(session_file)
            await self.db.set_telethon_status(user_id, False)
        except FloodWaitError as e:
            logger.warning(f"Worker {user_id}: FloodWait detected for {e.seconds}s.")
            await self._send_to_bot_user(user_id, f"⚠️ FloodWait. Worker будет остановлен на {e.seconds} секунд.")
            await self.db.set_telethon_status(user_id, False)
        except Exception as e:
            logger.error(f"Worker {user_id} unhandled error: {type(e).__name__} - {e}")
            if client and client.is_connected(): 
                try: await client.disconnect()
                except: pass
        finally:
            # Очистка состояния в БД и RAM при остановке
            await self.db.set_telethon_status(user_id, False)
            async with self.tasks_lock:
                store.active_workers.pop(user_id, None)
                store.premium_users.discard(user_id)
            logger.info(f"Worker {user_id}: Task execution gracefully finished/stopped.")


    async def stop_worker(self, user_id: int, silent=False):
        """Останавливает Telethon Worker, если он активен."""
        async with self.tasks_lock:
            client = store.active_workers.pop(user_id, None)
            store.premium_users.discard(user_id)
        
        if client:
            logger.info(f"Worker {user_id}: Stopping active worker instance.")
            try:
                await client.disconnect()
            except Exception as e:
                logger.error(f"Worker {user_id}: Error during client disconnect: {e}")
            await self.db.set_telethon_status(user_id, False)
            if not silent:
                await self._send_to_bot_user(user_id, "🛑 Worker успешно остановлен.")

manager = TelethonManager(bot, db)


# =========================================================================
# V. USER HANDLERS (МЕНЮ, АВТОРИЗАЦИЯ, АКТИВАЦИЯ)
# =========================================================================

# --- START MENU (Опущено для краткости) ---
async def get_main_menu_markup(user_id: int) -> InlineKeyboardMarkup:
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
        sub_text = f"✅ Подписка до: <b>{sub_end.strftime('%d.%m.%Y %H:%M')}</b>"
    else:
        sub_text = "❌ Подписка не активна. Активируйте промокод или оплатите."
        if user_data and user_data['telethon_active']:
             await manager.stop_worker(chat_id, silent=True)
             
    status_worker = "🟢 Активен" if user_data and user_data['telethon_active'] else "🔴 Не активен"

    text = (
        f"👋 <b>Добро пожаловать в StatPro!</b>\n"
        f"Это ваш личный Worker для сбора и анализа статистики.\n\n"
        f"⚙️ Статус Worker'а: <b>{status_worker}</b>\n"
        f"📅 Статус подписки: {sub_text}\n\n"
        f"Нажмите <b>'🔑 Войти в Telegram'</b>, чтобы подключить аккаунт и начать работу."
    )
    
    await safe_edit_or_send(chat_id, text, markup, message_id)


@user_router.message(Command(commands=['start']))
async def cmd_start(message: Message, state: FSMContext):
    """USER_ROUTER: Начальная точка входа."""
    await state.clear()
    await send_main_menu(message.chat.id)

# --- CANCEL Handler (Общий) ---
@user_router.callback_query(F.data.in_({'cmd_start', 'cancel_auth'}))
@admin_router.callback_query(F.data.in_({'cmd_start', 'cancel_auth', 'admin_panel'})) 
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    """Общий хендлер отмены для User и Admin."""
    user_id = call.from_user.id
    current_state = await state.get_state()
    if current_state:
        await state.clear()
    
    # Очистка временных данных Telethon
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
        
    if call.data == 'admin_panel' and user_id == ADMIN_ID:
        await call.answer()
        return await cb_admin_stats(call, state)
        
    await call.answer() 
    await send_main_menu(user_id, call.message.message_id) 

# --- USER: АКТИВАЦИЯ ПРОМОКОДА (Инициация) ---
@user_router.callback_query(F.data == "cb_activate_promo")
async def cb_activate_promo(call: CallbackQuery, state: FSMContext):
    """USER_ROUTER: Инициация ввода промокода для активации."""
    await state.set_state(PromoStates.WAITING_CODE)
    
    text = "🎁 <b>Введите промокод для активации:</b>"
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="cancel_auth")]
    ])
    
    await call.answer()
    await safe_edit_or_send(call.from_user.id, text, markup, call.message.message_id)

# --- USER: ОБРАБОТЧИК ВВОДА ПРОМОКОДА (Активация) ---
@user_router.message(PromoStates.WAITING_CODE)
async def msg_activate_promo(message: Message, state: FSMContext):
    """USER_ROUTER: Обрабатывает ввод промокода и активирует подписку."""
    code = message.text.strip().upper()
    user_id = message.from_user.id
    
    promo_data = await db.get_promocode(code)
    
    if not promo_data or promo_data['uses_left'] == 0:
        await message.reply("❌ Неверный или использованный промокод.")
    else:
        await db.use_promocode(code)
        new_end = await db.update_subscription(user_id, promo_data['duration_days'])
        
        await message.reply(
            f"🎉 <b>Промокод активирован!</b>\n"
            f"Вам добавлено {promo_data['duration_days']} дней подписки.\n"
            f"Новый срок окончания: <b>{new_end.strftime('%d.%m.%Y %H:%M')}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="cancel_auth")]])
        )
        
        # Проверка сессии перед запуском Worker
        session_path = get_session_path(user_id) + '.session'
        if not user_id in store.active_workers and os.path.exists(session_path):
            await manager.start_client_task(user_id)
        elif not os.path.exists(session_path):
             await message.answer("⚠️ Подписка активирована, но для запуска Worker'а требуется войти в Telegram (нет файла сессии).")
            
    await state.clear()
    
# --- FALLBACK (USER) ---
@user_router.callback_query()
async def cb_fallback_handler_user(call: CallbackQuery, state: FSMContext):
    """USER_ROUTER: Отказоустойчивый хендлер для необработанных колбэков."""
    logger.warning(f"User {call.from_user.id}: Unhandled CallbackQuery: {call.data}")
    await call.answer("🔄 Обновляю меню...", show_alert=False)
    await state.clear()
    await send_main_menu(call.from_user.id, call.message.message_id) 

# --- Заглушки для Auth ---
@user_router.callback_query(F.data == "cb_auth_menu")
async def cb_auth_menu(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    text = "Выберите способ авторизации: (Не реализовано в этой версии)"
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="cancel_auth")]
    ])
    await safe_edit_or_send(call.from_user.id, text, markup, call.message.message_id)
    
@user_router.callback_query(F.data == "cb_worker_stop")
async def cb_worker_stop(call: CallbackQuery):
    await manager.stop_worker(call.from_user.id)
    await call.answer("🛑 Worker остановлен.", show_alert=True)
    await send_main_menu(call.from_user.id, call.message.message_id)

@user_router.callback_query(F.data == "cb_worker_status")
async def cb_worker_status(call: CallbackQuery, state: FSMContext):
    is_active = call.from_user.id in store.active_workers
    sub_end = await db.get_subscription_status(call.from_user.id)
    
    if not is_active:
        text = "🔴 Worker не активен. Подключите аккаунт через '🔑 Войти в Telegram'."
    elif not sub_end or sub_end <= datetime.now(TIMEZONE_MSK):
        text = "⚠️ Worker не активен. Срок подписки истек."
    else:
        text = f"🟢 Worker активен и работает.\nАккаунт подключен.\nПодписка до: <b>{sub_end.strftime('%d.%m.%Y %H:%M')}</b>"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛑 Остановить Worker", callback_data="cb_worker_stop")] if is_active else [],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="cancel_auth")]
    ])
    
    await call.answer()
    await safe_edit_or_send(call.from_user.id, text, markup, call.message.message_id)


# =========================================================================
# VI. ADMIN HANDLERS 
# =========================================================================

# --- ADMIN PANEL START ---
@admin_router.callback_query(F.data.in_({"admin_stats", "admin_panel"}))
async def cb_admin_stats(call: CallbackQuery, state: FSMContext):
    """ADMIN_ROUTER: Отображает статистику и главное меню админки."""
    await state.clear()
    stats = await db.get_stats()
    
    text = (
        "<b>🔧 АДМИН-ПАНЕЛЬ</b>\n\n"
        f"👤 Всего пользователей: {stats.get('total_users', 0)}\n"
        f"⚙️ Активные воркеры (DB): {stats.get('active_workers_db', 0)}\n"
        f"⚡ Активные воркеры (RAM): {stats.get('active_workers_ram', 0)}"
    )
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✨ Создать Промокод", callback_data="admin_create_promo_init")],
        [InlineKeyboardButton(text="📋 Показать Промокоды", callback_data="admin_view_promos")],
        [InlineKeyboardButton(text="🗑 Удалить Промокод", callback_data="admin_delete_promo_init")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="cmd_start")]
    ])
    
    await call.answer()
    await safe_edit_or_send(call.from_user.id, text, markup, call.message.message_id)
    
# --- ПРОСМОТР ПРОМОКОДОВ (Опущено для краткости) ---
@admin_router.callback_query(F.data == "admin_view_promos")
async def cb_admin_view_promos(call: CallbackQuery):
    promocodes = await db.get_all_promocodes()
    
    if not promocodes:
        text = "🤷‍♂️ В базе данных нет активных промокодов."
    else:
        promo_list = []
        for p in promocodes:
            uses = '∞' if p['uses_left'] == 0 else p['uses_left']
            
            promo_line = "• <code>{}</code> | {} д. | {} исп.".format(
                p['code'], p['duration_days'], uses
            )
            promo_list.append(promo_line)
        
        text = (
            "📋 <b>СПИСОК АКТИВНЫХ ПРОМОКОДОВ</b>\n\n"
            "<pre>"
            "КОД       | СРОК | ИСПОЛЬЗОВАНИЙ\n"
            "----------------------------------\n"
            "{}\n"
            "</pre>\n"
            "\nНажмите на код, чтобы скопировать его.".format('\n'.join(promo_list))
        )

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в Админ-панель", callback_data="admin_panel")]
    ])
    
    await call.answer()
    await safe_edit_or_send(call.from_user.id, text, markup, call.message.message_id)

# --- PROMO CREATE (STEP 1: GENERATE CODE + ASK DAYS) ---
@admin_router.callback_query(F.data == "admin_create_promo_init")
async def cb_admin_create_promo_init(call: CallbackQuery, state: FSMContext):
    promo_code = generate_promocode()
    await state.update_data(promo_code=promo_code)
    
    await state.set_state(AdminPromo.WAITING_DAYS)

    text = (f"✅ Промокод сгенерирован!\n"
            f"Код: <code>{promo_code}</code> (Нажмите, чтобы скопировать)\n\n"
            f"✍️ <b>Шаг 1/2:</b> Введите <b>срок действия</b> (в днях, только число, 0 = 0 дней):")
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="admin_panel")]
    ])
    
    await call.answer()
    await safe_edit_or_send(call.from_user.id, text, markup, call.message.message_id)


# --- PROMO CREATE (STEP 2: DAYS INPUT) ---
@admin_router.message(AdminPromo.WAITING_DAYS, F.text.regexp(r'^\d+$'))
async def msg_admin_promo_days(message: Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        if days < 0: raise ValueError("Non-negative days only")
    except ValueError:
        return await message.reply("❌ Неверный формат. Введите только положительное число дней или 0.")
    
    await state.update_data(days=days)
    await state.set_state(AdminPromo.WAITING_USES)
    
    data = await state.get_data()
    text = (f"✅ Код <code>{data['promo_code']}</code>. Срок: {days} д.\n\n"
            f"✍️ <b>Шаг 2/2:</b> Введите <b>количество активаций</b> (только число, 0 = бесконечно):")
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="admin_panel")]
    ])
    
    await message.reply(text, reply_markup=markup)

@admin_router.message(AdminPromo.WAITING_DAYS)
async def msg_admin_promo_days_invalid(message: Message):
    await message.reply("❌ Неверный формат. Введите только положительное число дней или 0.")


# --- PROMO CREATE (STEP 3: USES INPUT) ---
@admin_router.message(AdminPromo.WAITING_USES, F.text.regexp(r'^\d+$'))
async def msg_admin_promo_uses(message: Message, state: FSMContext):
    try:
        uses = int(message.text.strip())
        if uses < 0: raise ValueError("Non-negative uses only")
    except ValueError:
        return await message.reply("❌ Неверный формат. Введите только положительное число или 0 (для бесконечных активаций).")

    data = await state.get_data()
    promo_code = data['promo_code']
    days = data['days']
    
    try:
        await db.db_pool.execute(
            "INSERT INTO promocodes (code, duration_days, uses_left) VALUES (?, ?, ?)",
            (promo_code, days, uses)
        )
        await db.db_pool.commit()
    except aiosqlite.IntegrityError:
        await state.clear()
        return await message.reply("❌ Ошибка: Промокод с таким кодом уже существует. Повторите создание.")
    
    await state.clear()
    
    await message.reply(
        f"🎉 <b>Промокод создан!</b>\n\n"
        f"Код: <code>{promo_code}</code>\n"
        f"Срок: {days} д.\n"
        f"Использований: {'Бесконечно' if uses == 0 else uses}\n",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ В админ-панель", callback_data="admin_stats")]
        ])
    )

@admin_router.message(AdminPromo.WAITING_USES)
async def msg_admin_promo_uses_invalid(message: Message):
    await message.reply("❌ Неверный формат. Введите только положительное число или 0 (для бесконечных активаций).")


# --- PROMO DELETE (Инициация) ---
@admin_router.callback_query(F.data == "admin_delete_promo_init")
async def cb_admin_delete_promo_init(call: CallbackQuery, state: FSMContext):
    """ADMIN_ROUTER: Инициация ввода промокода для удаления."""
    await state.set_state(PromoStates.WAITING_CODE)
    
    text = "✍️ <b>Введите промокод, который нужно удалить:</b>"
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="admin_stats")]])
    
    await call.answer()
    await safe_edit_or_send(call.from_user.id, text, markup, call.message.message_id)


# --- PROMO DELETE (Обработка ввода) ---
@admin_router.message(PromoStates.WAITING_CODE)
async def msg_admin_delete_promo(message: Message, state: FSMContext):
    """ADMIN_ROUTER: Обрабатывает ввод промокода и удаляет его из БД."""
    code = message.text.strip().upper()

    try:
        async with db.db_pool.execute("DELETE FROM promocodes WHERE code=?", (code,)) as cursor:
            rows_deleted = cursor.rowcount
        await db.db_pool.commit()
    except Exception as e:
        logger.error(f"Admin {message.from_user.id}: Failed to delete promocode {code}: {e}")
        await message.reply("❌ Произошла ошибка базы данных при удалении промокода.")
        await state.clear()
        return

    await state.clear()
    
    if rows_deleted > 0:
        await message.reply(
            f"🗑 <b>Промокод <code>{code}</code> успешно удален.</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ В админ-панель", callback_data="admin_stats")]])
        )
    else:
        await message.reply(
            f"❌ Промокод <code>{code}</code> не найден в базе данных.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ В админ-панель", callback_data="admin_stats")]])
        )

# --- FALLBACK (ADMIN) ---
@admin_router.callback_query()
async def cb_fallback_handler_admin(call: CallbackQuery, state: FSMContext):
    """ADMIN_ROUTER: Отказоустойчивый хендлер для необработанных колбэков админа."""
    logger.warning(f"Admin {call.from_user.id}: Unhandled CallbackQuery: {call.data}")
    await call.answer("🔄 Обновляю меню...", show_alert=False)
    await state.clear()
    await cb_admin_stats(call, state)


# =========================================================================
# VII. ЗАПУСК БОТА
# =========================================================================

async def on_startup(dispatcher: Dispatcher, bot: Bot):
    logger.info("Bot starting up...")
    await db.init()
    
    active_users = await db.get_active_telethon_users()
    logger.info(f"Restoring {len(active_users)} workers from database for re-check and startup.")
    
    for user_id in active_users:
        try:
            # Запуск worker-задачи (внутри start_client_task есть проверка файла сессии)
            await manager.start_client_task(user_id) 
        except Exception as e:
            logger.error(f"Failed to restore worker {user_id} on startup: {e}")
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
