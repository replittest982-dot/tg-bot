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
from io import BytesIO
import sqlite3 # Импортируем для явной обработки ошибки

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
from telethon import TelegramClient, events, utils
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError, 
    AuthKeyUnregisteredError, PhoneCodeInvalidError, 
    PhoneCodeExpiredError, RpcCallFailError, 
    PhoneNumberInvalidError, PasswordHashInvalidError,
    UsernameInvalidError, PeerIdInvalidError, 
    UserNotMutualContactError
)
from telethon.tl.types import ChannelParticipantAdmin, ChannelParticipantCreator

# =========================================================================
# I. КОНФИГУРАЦИЯ И НАСТРОЙКА
# =========================================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
# Измените на ваш фактический ID
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

# Убедимся, что директории существуют
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
admin_router = Router(name='admin_router')

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

class PromoStates(StatesGroup):
    WAITING_CODE = State()

class AdminPromo(StatesGroup):
    WAITING_DAYS = State() 
    WAITING_USES = State() 

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
    
    # Предотвращение ошибки Pydantic/int в reply_markup
    if isinstance(reply_markup, int):
        logger.error(f"CORRECTION: Received int {reply_markup} as reply_markup for {chat_id}. Setting to None.")
        reply_markup = None
        
    # 1. Если передан message_id, пытаемся удалить старое сообщение.
    if message_id:
        try:
            # Пытаемся удалить сообщение
            await bot_instance.delete_message(chat_id, message_id)
        except TelegramAPIError as e:
            # logger.warning(f"Failed to delete old message {message_id} for {chat_id}: {e}. Sending new message.")
            pass # Игнорируем ошибки удаления, просто отправляем новое
        except Exception as e:
            logger.warning(f"Unexpected error during delete for {chat_id}: {e}")

    # 2. Отправляем новое сообщение.
    try:
        await bot_instance.send_message(chat_id, text, reply_markup=reply_markup)
    except Exception as e_send:
        logger.error(f"FATAL: Failed to send message to {chat_id}: {e_send}")

def generate_promocode(length=8) -> str:
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def normalize_phone(phone: str) -> str:
    phone = phone.strip()
    cleaned = re.sub(r'[^\d+]', '', phone)

    if not cleaned: return ""
    
    if cleaned.startswith('+'):
        return cleaned
        
    if cleaned.startswith('7') or cleaned.startswith('8'):
        return '+7' + cleaned[1:]
        
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
            # Добавляем админа, если его нет
            if ADMIN_ID != 0:
                await self.db_pool.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (ADMIN_ID,))
            
            await self.db_pool.execute("""
                CREATE TABLE IF NOT EXISTS promocodes (
                    code TEXT PRIMARY KEY,
                    duration_days INTEGER,
                    uses_left INTEGER
                )
            """)
            await self.db_pool.commit()
            logger.info("Database initialized successfully.")
        except sqlite3.OperationalError as e:
            logger.critical(f"FATAL DB ERROR: Cannot open database file {self.db_path}. Check permissions! Error: {e}")
            sys.exit(1) # Выход, если не удалось открыть основную базу данных

    async def get_user(self, user_id: int):
        if not self.db_pool: return None
        # Обеспечиваем, что пользователь существует в базе
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

    async def update_subscription(self, user_id: int, days: int):
        if not self.db_pool: return
        current_end = await self.get_subscription_status(user_id)
        now = datetime.now(TIMEZONE_MSK)
        # Если подписка не активна (None или меньше текущего времени), начинаем отсчет с текущего момента
        new_end = (current_end if current_end and current_end > now else now) + timedelta(days=days)
        await self.db_pool.execute("UPDATE users SET subscription_end=? WHERE user_id=?", (new_end.strftime('%Y-%m-%d %H:%M:%S'), user_id))
        await self.db_pool.commit()
        return new_end

    async def get_promocode(self, code: str):
        if not self.db_pool: return None
        async with self.db_pool.execute("SELECT * FROM promocodes WHERE code=?", (code.upper(),)) as cursor:
            result = await cursor.fetchone() 
            return dict(result) if result else None
            
    async def get_all_promocodes(self) -> List[Dict[str, Any]]:
        if not self.db_pool: return []
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
        # Ищем активных пользователей с активным статусом и действующей подпиской
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
            'active_workers_ram': len(store.active_workers), 
        }
        
    async def delete_promocode(self, code: str) -> int:
        if not self.db_pool: return 0
        cursor = await self.db_pool.execute("DELETE FROM promocodes WHERE code=?", (code.upper(),))
        count = cursor.rowcount or 0
        await self.db_pool.commit()
        return count

db = AsyncDatabase(os.path.join(DATA_DIR, DB_NAME))


# =========================================================================
# IV. TELETHON MANAGER 
# =========================================================================

class TelethonManager:
    """Управление сессиями Telethon, Worker'ами и авторизацией."""
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
            qr_future = store.qr_login_future.pop(user_id, None) # Чистим QR future
            
            # Если QR-future еще не завершен, отменяем его
            if qr_future and not qr_future.done():
                qr_future.cancel()
        
        if client:
            try:
                # Отключаем, только если он был подключен (дополнительная защита)
                if hasattr(client, "is_connected") and await client.is_connected(): await client.disconnect() 
            except Exception:
                pass
                
        # Удаляем ТОЛЬКО временный файл, если он существует. 
        path_temp = get_session_path(user_id, is_temp=True) + '.session'
        if os.path.exists(path_temp):
            try: 
                os.remove(path_temp)
                logger.info(f"Worker {user_id}: Temporary session file cleaned up.")
            except OSError as e: 
                logger.error(f"Worker {user_id}: Failed to delete temporary session file: {e}")
                
    async def handle_telethon_error(self, user_id: int, error_type: str, e: Exception, message: str):
        """Обработчик критических ошибок Telethon/SQLite."""
        logger.error(f"Worker {user_id}: Critical {error_type} error: {type(e).__name__} - {e}")
        
        # 1. Специфический обработчик для ошибки с правами доступа
        if isinstance(e, sqlite3.OperationalError):
            if 'unable to open database file' in str(e):
                message = "❌ **Критическая ошибка:** Не удалось открыть/создать файл сессии. Это, скорее всего, **проблема с правами доступа** на сервере. Пожалуйста, выполните команду `chmod -R 777 sessions data` на вашем хостинге."
            elif 'attempt to write a readonly database' in str(e):
                message = "❌ **Критическая ошибка:** База данных сессий доступна только для чтения. **Проверьте права доступа** к папке `sessions` (требуются права на запись)."

        await self._send_to_bot_user(user_id, message)
        await self._cleanup_temp_session(user_id) 


    async def start_worker_session(self, user_id: int, client_temp: TelegramClient):
        """
        Сохраняет сессию в постоянный файл, удаляет временный и запускает Worker.
        Устранены TypeError и добавлены обработчики sqlite3.
        """
        path_perm = get_session_path(user_id)
        
        # 1. Очистка старого состояния и остановка worker'а
        await self.stop_worker(user_id, silent=True)
        
        client_perm = None
        
        try:
            # Убеждаемся, что временный клиент подключен и содержит данные
            if not await client_temp.is_connected():
                # Подключаем его, чтобы убедиться, что данные сессии загружены
                await client_temp.connect() 
                
            # Создаем новый клиент с постоянным путем сессии
            client_perm = await _new_telethon_client(user_id, is_temp=False) 
            # Важно: НЕ вызываем client_perm.start() здесь, только после копирования!
            
            # Копируем авторизационные данные из временного клиента в постоянный
            # *** ИСПРАВЛЕНО: УБРАН 'await' для _copy_session_from ***
            client_perm._copy_session_from(client_temp) 
            
            # Принудительное сохранение сессии в постоянный файл
            client_perm.session.save()
            logger.info(f"Worker {user_id}: Session successfully copied and saved to permanent path.")

        except (sqlite3.OperationalError, Exception) as e:
            # Обработка всех критических ошибок сохранения сессии, включая SQLite
            await self.handle_telethon_error(user_id, "Session Save", e, "❌ Критическая ошибка при сохранении сессии. Повторите вход.")
            
            # Отключаем, если создали и подключили
            if client_perm and hasattr(client_perm, "is_connected") and await client_perm.is_connected(): 
                try: await client_perm.disconnect() 
                except: pass
            await self._cleanup_temp_session(user_id) 
            return

        # 3. Очистка временного клиента и временного файла сессии
        await self._cleanup_temp_session(user_id) 
        # Постоянный клиент нужно запускать из start_client_task, поэтому его просто чистим
        if client_perm and hasattr(client_perm, "is_connected") and await client_perm.is_connected():
            try: await client_perm.disconnect() 
            except: pass
        
        # 4. Запуск Worker'а
        if os.path.exists(path_perm + '.session'): 
            logger.info(f"Worker {user_id}: Permanent session found. Starting task.")
            # Теперь запускаем основную задачу, которая подключит клиент
            await self.start_client_task(user_id) 
        else:
             logger.error(f"Worker {user_id}: Failed to find permanent session after save operation.")
             await self._send_to_bot_user(user_id, "❌ Критическая ошибка: Файл постоянной сессии не найден. Повторите вход.")


    async def start_client_task(self, user_id: int):
        """Запускает Worker в фоновой задаче."""
        await self.stop_worker(user_id, silent=True)
        
        session_path = get_session_path(user_id) + '.session'
        if not os.path.exists(session_path):
             logger.warning(f"Worker {user_id}: Attempted to start, but permanent session file not found.")
             await self.db.set_telethon_status(user_id, False)
             return
             
        try:
            # Проверяем, существует ли уже задача. Это защита от race condition.
            async with self.tasks_lock:
                 if user_id in store.active_workers: return

            task = asyncio.create_task(self._run_worker(user_id), name=f"main-worker-{user_id}")
            logger.info(f"Worker {user_id}: Main worker task created and scheduled.")
            return task
        except Exception as e:
            logger.critical(f"Worker {user_id}: Critical error starting client task: {e}")
            await self.db.set_telethon_status(user_id, False)

    # =====================================================================
    # ЛОГИКА WORKER'А И КОМАНД (Не изменена, т.к. это ваша бизнес-логика)
    # =====================================================================
    
    async def _handle_ls_command(self, event):
        """Обработка команды .лс [юзернейм/ID] [сообщение]"""
        # ... (Ваша логика) ...
        text = event.message.message
        parts = text.split(maxsplit=2)
        
        if len(parts) < 3:
            await event.reply("❌ **.лс [юзернейм/ID] [сообщение]**: Недостаточно аргументов.")
            return
            
        target = parts[1]
        message_to_send = parts[2]
        
        client: TelegramClient = event.client
        
        try:
            # Пытаемся получить объект пользователя/чата
            entity = await client.get_entity(target)
            
            # Отправляем сообщение
            await client.send_message(entity, message_to_send)
            
            # Отправляем ответ в чат, откуда пришла команда
            await event.reply(f"✅ **Успешно:** Сообщение отправлено пользователю/чату <code>{target}</code>.")
            
        except UsernameInvalidError:
            await event.reply(f"❌ **Ошибка:** Неверный юзернейм или ID: <code>{target}</code>.")
        except PeerIdInvalidError:
            await event.reply(f"❌ **Ошибка:** Неверный ID или недоступный чат: <code>{target}</code>.")
        except UserNotMutualContactError:
            await event.reply(f"❌ **Ошибка:** Невозможно отправить ЛС пользователю <code>{target}</code>. Он не в контактах.")
        except Exception as e:
            logger.error(f"Worker {client.session.user_id} .лс error: {type(e).__name__} - {e}")
            await event.reply(f"❌ **Критическая ошибка при отправке ЛС:** {type(e).__name__}.")


    async def _handle_checkgroup_command(self, event):
        """Обработка команды .чекгруппу [юзернейм группы] [юзернейм/ID пользователя]"""
        # ... (Ваша логика) ...
        text = event.message.message
        parts = text.split(maxsplit=2)
        
        if len(parts) < 3:
            await event.reply("❌ **.чекгруппу [юзернейм группы] [юзернейм/ID пользователя]**: Недостаточно аргументов.")
            return
            
        group_username = parts[1]
        user_target = parts[2]
        client: TelegramClient = event.client

        try:
            # 1. Получаем объект группы
            group_entity = await client.get_entity(group_username)
            
            # 2. Получаем объект пользователя
            user_entity = await client.get_entity(user_target)

            # 3. Запрашиваем информацию об участии
            participant = await client(GetParticipantRequest(group_entity, user_entity))
            
            status_text = "Участник"
            
            # Проверка статуса (для админов/создателей)
            if isinstance(participant.participant, ChannelParticipantAdmin):
                status_text = "Админ"
            elif isinstance(participant.participant, ChannelParticipantCreator):
                status_text = "Создатель"

            # 4. Отправляем результат
            await event.reply(
                f"✅ **Проверка статуса в группе <code>{group_username}</code>:**\n"
                f"Пользователь <code>{user_target}</code> является **{status_text}**."
            )

        except FloodWaitError as e:
             await event.reply(f"❌ **FloodWait:** Пожалуйста, подождите {e.seconds} секунд перед повторной проверкой.")
        except UsernameInvalidError:
            await event.reply(f"❌ **Ошибка:** Неверный юзернейм группы или пользователя.")
        except PeerIdInvalidError:
            await event.reply(f"❌ **Ошибка:** Неверный ID или недоступный чат.")
        except ValueError as e:
            if 'The specified user is not a participant' in str(e):
                 await event.reply(f"✅ **Проверка статуса в группе <code>{group_username}</code>:**\nПользователь <code>{user_target}</code> **НЕ** является участником.")
            else:
                 logger.error(f"Worker {client.session.user_id} .чекгруппу error: {type(e).__name__} - {e}")
                 await event.reply(f"❌ **Критическая ошибка:** {type(e).__name__}.")
        except Exception as e:
            logger.error(f"Worker {client.session.user_id} .чекгруппу unhandled error: {type(e).__name__} - {e}")
            await event.reply(f"❌ **Неизвестная ошибка:** {type(e).__name__}.")


    async def _run_worker(self, user_id: int): 
        """Основная логика Worker'а."""
        path = get_session_path(user_id)
        client = TelegramClient(path, self.API_ID, self.API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        # Защита от дублирования
        async with self.tasks_lock: 
            if user_id in store.active_workers:
                logger.warning(f"Worker {user_id}: Duplicate task detected. Disconnecting new client.")
                if hasattr(client, "is_connected") and await client.is_connected(): await client.disconnect()
                return 
            store.active_workers[user_id] = client 
        
        try:
            await client.connect()
            if not await client.is_user_authorized(): 
                logger.error(f"Worker {user_id}: Client is not authorized after connection attempt.")
                raise AuthKeyUnregisteredError('Session expired or unauthorized')

            sub_end = await self.db.get_subscription_status(user_id)
            now_msk = datetime.now(TIMEZONE_MSK)

            if not sub_end or sub_end <= now_msk:
                logger.info(f"Worker {user_id}: Subscription expired. Stopping worker.")
                await self._send_to_bot_user(user_id, "⚠️ Подписка истекла. Worker отключен.")
                await client.disconnect() 
                return 
            
            await self.db.set_telethon_status(user_id, True)
            me = await client.get_me()
            logger.info(f"Worker {user_id} ({utils.get_display_name(me)}) started successfully.")
            await self._send_to_bot_user(user_id, f"✅ Worker запущен! Аккаунт: <b>{utils.get_display_name(me)}</b>\nСтатистика активна. Время подписки до: {sub_end.strftime('%d.%m.%Y %H:%M')}")
            
            
            # =================================================================
            # ИНТЕГРАЦИЯ КАСТОМНЫХ КОМАНД TELETHON
            # =================================================================
            
            # Хендлер для команды .лс
            client.add_event_handler(
                self._handle_ls_command, 
                events.NewMessage(pattern=r'^\.лс\s', incoming=True, chats=[user_id]) 
            )
            
            # Хендлер для команды .чекгруппу
            client.add_event_handler(
                self._handle_checkgroup_command, 
                events.NewMessage(pattern=r'^\.чекгруппу\s', incoming=True, chats=[user_id])
            )
            
            # =================================================================
            
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
            # Отключаем клиента, если он еще подключен
            if client and hasattr(client, "is_connected") and await client.is_connected(): 
                try: await client.disconnect()
                except: pass
        finally:
            await self.db.set_telethon_status(user_id, False)
            async with self.tasks_lock:
                store.active_workers.pop(user_id, None)
            logger.info(f"Worker {user_id}: Task execution gracefully finished/stopped.")


    async def stop_worker(self, user_id: int, silent=False):
        """Останавливает Worker и отключает клиент."""
        async with self.tasks_lock:
            client = store.active_workers.pop(user_id, None)
        
        if client:
            logger.info(f"Worker {user_id}: Stopping active worker instance.")
            try:
                # Отключаем, только если он был подключен (дополнительная защита)
                if hasattr(client, "is_connected") and await client.is_connected(): await client.disconnect()
            except Exception as e:
                logger.error(f"Worker {user_id}: Error during client disconnect: {e}")
            await self.db.set_telethon_status(user_id, False)
            if not silent:
                await self._send_to_bot_user(user_id, "🛑 Worker успешно остановлен.")
                
    async def wait_for_qr_scan(self, user_id: int, client: TelegramClient, qr_login: Any, qr_future: asyncio.Future):
        """Фоновая задача, ожидающая сканирования QR-кода."""
        try:
            # client уже должен быть подключен в cb_auth_qr_init
            
            # Ждем сканирования
            await qr_login.wait(timeout=65) 
            
            # --- АВТОРИЗАЦИЯ УСПЕШНА (QR-код СКАНИРОВАН) ---
            if not qr_future.done():
                qr_future.set_result(True)
                
            # Проверяем, авторизован ли пользователь (пропуск 2FA)
            if await client.is_user_authorized():
                # Явное сообщение об успешном входе перед запуском Worker
                await self._send_to_bot_user(user_id, "✅ **Успешный вход!** Инициализирую Worker...")
                logger.info(f"Worker {user_id}: QR login successful. Starting session.")
                await self.start_worker_session(user_id, client)
                return # Успешный выход

        except TimeoutError:
            if not qr_future.done():
                qr_future.set_result(False)
            await self._send_to_bot_user(user_id, "❌ Время ожидания QR-кода истекло (60 сек). Повторите попытку.", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="cancel_auth")]]))
            await self._cleanup_temp_session(user_id) 
            return

        except asyncio.CancelledError:
             logger.info(f"QR wait task for {user_id} was cancelled.")
             await self._cleanup_temp_session(user_id) 
             return
             
        except SessionPasswordNeededError:
            # Обработка 2FA - это ожидаемое поведение, если аккаунт защищен.
            logger.info(f"Worker {user_id}: QR login successful, but 2FA password required.")
            pass # Продолжаем к блоку 2FA ниже
            
        except Exception as e:
            logger.error(f"QR wait error for {user_id}: {type(e).__name__} - {e}")
            if not qr_future.done():
                qr_future.set_result(False)
            await self._send_to_bot_user(user_id, "❌ Произошла ошибка. Попробуйте войти по номеру телефона.", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="cancel_auth")]]))
            await self._cleanup_temp_session(user_id) 
            return
        
        # ЕСЛИ ДОШЛИ СЮДА, ЗНАЧИТ SessionPasswordNeededError ИЛИ НЕУДАЧНЫЙ QR
        
        # Отправляем сообщение о необходимости ввода пароля через номер
        await self._send_to_bot_user(user_id, 
            "🔒 <b>ТРЕБУЕТСЯ ПАРОЛЬ (2FA)</b>\n"
            "Ваш аккаунт защищен двухфакторной аутентификацией. QR-код не может завершить вход. \n"
            "Пожалуйста, используйте кнопку **'Вход по номеру телефона'** для ввода пароля.", 
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📞 Войти по номеру", callback_data="cb_auth_phone")],[InlineKeyboardButton(text="⬅️ В меню", callback_data="cancel_auth")]])
        )
        
        # Очистка временной сессии после неудачного входа/перехода на 2FA
        await self._cleanup_temp_session(user_id) 


manager = TelethonManager(bot, db)


# =========================================================================
# V. USER HANDLERS (МЕНЮ, АВТОРИЗАЦИЯ, АКТИВАЦИЯ)
# =========================================================================
# ... (Остальной код хендлеров AIOGram) ...
# (Этот раздел не был изменен, так как он касается FSM и меню бота, а не критической логики Telethon)
# =========================================================================

# --- AIOGRAM ХЕНДЛЕРЫ ---

@user_router.message(Command("start", "help"))
async def command_start_handler(message: Message, state: FSMContext):
    # Если пользователь находится в процессе аутентификации, сначала отменяем его
    await state.clear()
    
    # ... (Ваша логика команды /start) ...
    await message.reply("Привет! Я бот для управления сессиями Telethon.\n"
                        "Используйте команду /login для авторизации вашего аккаунта.")

@user_router.callback_query(F.data == "cancel_auth")
async def cb_cancel_auth(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await state.clear()
    await manager._cleanup_temp_session(user_id)
    await safe_edit_or_send(
        user_id, 
        "🚪 Процесс авторизации отменен. Вы можете начать снова, используя команду /login.",
        message_id=callback.message.message_id
    )
    await callback.answer("Процесс отменен")


@user_router.message(Command("login"))
async def command_login_handler(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id in store.active_workers:
        await message.reply("✅ Ваша сессия уже активна. Используйте /status для проверки.")
        return
        
    await state.set_state(TelethonAuth.WAITING_FOR_QR_SCAN)
    await message.reply("Начинаю процесс авторизации...")
    # Запускаем логин в фоновом режиме
    asyncio.create_task(manager.start_login_process(user_id))

@user_router.message(Command("status"))
async def command_status_handler(message: Message):
    user_id = message.from_user.id
    
    # Проверка статуса в памяти
    is_active_ram = user_id in store.active_workers and await store.active_workers[user_id].is_connected()
    
    # Проверка статуса в БД
    user_data = await db.get_user(user_id)
    is_active_db = user_data and user_data['telethon_active']
    sub_end = await db.get_subscription_status(user_id)
    
    if is_active_ram:
        status_text = "🟢 **ОНЛАЙН** (Worker активен)"
    elif is_active_db:
        status_text = "🟠 **ОЖИДАЕТ** (Активен в БД, но Worker не запущен. Попробуйте /restart)"
    else:
        status_text = "🔴 **ОФФЛАЙН** (Сессия не активна. Используйте /login)"

    sub_info = f"Подписка: {'Активна до ' + sub_end.strftime('%d.%m.%Y %H:%M') if sub_end and sub_end > datetime.now(TIMEZONE_MSK) else '**Истекла/Нет**'}"
    
    await message.reply(f"🤖 Статус вашего воркера:\n{status_text}\n{sub_info}", parse_mode="Markdown")

@user_router.message(Command("stop"))
async def command_stop_handler(message: Message):
    user_id = message.from_user.id
    await manager.stop_worker(user_id)
    
@user_router.message(Command("restart"))
async def command_restart_handler(message: Message):
    user_id = message.from_user.id
    await manager.stop_worker(user_id, silent=True)
    await message.reply("🔄 Перезапуск Worker'а...")
    await manager.start_client_task(user_id)


# (Ваш остальной код AIOGRAM хендлеров: cb_auth_qr_init, cb_auth_phone, cb_auth_phone_submit, 
# message_phone_handler, message_code_handler, message_password_handler, и т.д.
# Я не включаю их сюда, чтобы не повторять 1000+ строк, но они должны быть в вашем файле!)


# =========================================================================
# VI. СТАРТ И ВОССТАНОВЛЕНИЕ
# =========================================================================

async def restore_workers_on_boot():
    """Восстановление сессий из БД при старте."""
    online_workers = await db.get_active_telethon_users()
    
    logger.info(f"Restoring {len(online_workers)} workers from database for re-check and startup.")

    for user_id in online_workers:
        # Запуск задачи для каждого Worker'а
        asyncio.create_task(manager.start_client_task(user_id))

async def on_startup(dp):
    logger.info("Bot starting up...")
    await db.init() # Асинхронная инициализация БД
    
    # Проверка, что папки существуют (уже сделано в начале, но для гарантии)
    os.makedirs(SESSION_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Попытка восстановить рабочие сессии
    await restore_workers_on_boot()
    
    # Регистрация роутеров
    dp.include_router(user_router)
    dp.include_router(admin_router) # Предполагая, что у вас есть админ-роутер

    logger.info("Bot ready and polling started!")
    
async def on_shutdown(dp):
    # Останавливаем все активные Worker'ы при выключении
    logger.info("Stopping all active Telethon workers...")
    workers_to_stop = list(store.active_workers.keys())
    await asyncio.gather(*[manager.stop_worker(uid, silent=True) for uid in workers_to_stop])
    
    if db.db_pool:
        await db.db_pool.close()
    logger.info("Database connection closed.")
    logger.info("Bot polling stopped.")


if __name__ == '__main__':
    # Настройка логирования для Aiogram (фильтр reply_markup)
    aiogram_logger = logging.getLogger('aiogram.event')
    original_info = aiogram_logger.info
    
    def patched_info(msg, *args, **kwargs):
        if 'reply_markup=' in msg:
            filtered_msg = msg.split('reply_markup=')[0].strip() + ' ...'
            return original_info(filtered_msg, *args, **kwargs)
        return original_info(msg, *args, **kwargs)
        
    aiogram_logger.info = patched_info

    asyncio.run(dp.start_polling(bot))
