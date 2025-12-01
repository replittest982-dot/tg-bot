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
from telethon import TelegramClient, events, utils, functions
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
    """ИСПРАВЛЕНА: Проверена на синтаксис '_in'."""
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
        # Учитываем, что Telethon может создавать несколько файлов (session, session-journal)
        path_temp = get_session_path(user_id, is_temp=True) 
        try:
             for ext in ['.session', '.session-journal', '.session-shm', '.session-wal']:
                 file_path = path_temp + ext
                 if os.path.exists(file_path):
                     os.remove(file_path)
             logger.info(f"Worker {user_id}: Temporary session files cleaned up.")
        except OSError as e: 
             logger.error(f"Worker {user_id}: Failed to delete temporary session file: {e}")
            
    async def handle_telethon_error(self, user_id: int, error_type: str, e: Exception, message: str):
        """Обработчик критических ошибок Telethon/SQLite."""
        logger.error(f"Worker {user_id}: Critical {error_type} error: {type(e).__name__} - {e}")
        
        # 1. Специфический обработчик для ошибки с правами доступа
        if isinstance(e, sqlite3.OperationalError):
            if 'unable to open database file' in str(e) or 'attempt to write a readonly database' in str(e):
                message = "❌ **Критическая ошибка:** Не удалось открыть/создать/записать файл сессии. Это, скорее всего, **проблема с правами доступа** на сервере. Пожалуйста, выполните команду `chmod -R 777 sessions data` на вашем хостинге."
        
        await self._send_to_bot_user(user_id, message)
        await self._cleanup_temp_session(user_id) 


    async def start_worker_session(self, user_id: int, client_temp: TelegramClient):
        """
        Сохраняет сессию в постоянный файл, удаляет временный и запускает Worker.
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
            
            # Копируем авторизационные данные из временного клиента в постоянный
            # ИСПРАВЛЕНИЕ: _copy_session_from - это синхронный метод.
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
    # ЛОГИКА WORKER'А И КОМАНД
    # =====================================================================
    
    async def _handle_ls_command(self, event):
        """Обработка команды .лс [юзернейм/ID] [сообщение]"""
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
        
        # Отправляем сообщение о необходимости ввода пароля
        await self._send_to_bot_user(user_id, 
            "🔒 <b>ТРЕБУЕТСЯ ПАРОЛЬ (2FA)</b>\n"
            "Ваш аккаунт защищен двухфакторной аутентификацией. QR-код не может завершить вход. \n"
            "Пожалуйста, используйте кнопку **'Вход по номеру телефона'** для ввода пароля.", 
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📞 Вход по номеру", callback_data="auth_phone_init")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_auth")]
            ])
        )


# =========================================================================
# V. ХЕНДЛЕРЫ AIOGRAM (Основная логика управления)
# =========================================================================

# --- ГЛАВНОЕ МЕНЮ И СТАТУС ---

def get_main_keyboard(user_id: int, sub_end: Optional[datetime]) -> InlineKeyboardMarkup:
    """Генерирует основное меню бота."""
    is_active = user_id in store.active_workers
    session_path = get_session_path(user_id) + '.session'
    session_exists = os.path.exists(session_path)
    
    # Проверка подписки
    now_msk = datetime.now(TIMEZONE_MSK)
    is_subscribed = sub_end is not None and sub_end > now_msk
    sub_text = f"Подписка до: {sub_end.strftime('%d.%m.%Y')}" if is_subscribed and sub_end else "❌ Подписка не активна"

    # Кнопки авторизации/статуса
    if not session_exists:
        auth_button = InlineKeyboardButton(text="🔑 Войти (QR/Телефон)", callback_data="auth_init")
    else:
        auth_button = InlineKeyboardButton(text="👤 Аккаунт активен" if is_active else "🔓 Сессия сохранена", callback_data="status_wa")
        
    # Кнопки управления
    control_text = "🛑 Остановить Worker" if is_active else "▶️ Запустить Worker"
    control_callback = "stop_worker" if is_active else "start_worker"

    keyboard = [
        [InlineKeyboardButton(text=sub_text, callback_data="sub_status")],
        [auth_button],
    ]
    
    if session_exists:
        keyboard.append([InlineKeyboardButton(text=control_text, callback_data=control_callback)])
    
    keyboard.append([InlineKeyboardButton(text="🎁 Активировать промокод", callback_data="promo_init")])

    if user_id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton(text="🛠️ Админ-панель", callback_data="admin_menu")])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


@user_router.message(Command("start", "menu"))
async def command_start_handler(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    sub_end = await db.get_subscription_status(user_id)
    
    await message.reply(
        "👋 **Добро пожаловать в панель управления Worker'ом!**\n\n"
        "1. Проверьте статус **Подписки**.\n"
        "2. Нажмите **'🔑 Войти'** для авторизации аккаунта Telegram.",
        reply_markup=get_main_keyboard(user_id, sub_end)
    )

# --- АВТОРИЗАЦИЯ: ВЫБОР МЕТОДА (QR/PHONE) ---

@user_router.callback_query(F.data == "auth_init")
async def cb_auth_init(callback: CallbackQuery):
    user_id = callback.from_user.id
    await manager.stop_worker(user_id, silent=True)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 Войти через QR-код", callback_data="auth_qr_init")],
        [InlineKeyboardButton(text="📞 Войти по номеру телефона", callback_data="auth_phone_init")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")]
    ])
    
    await callback.message.edit_text("⚙️ **Выберите способ авторизации:**", reply_markup=kb)
    await callback.answer()

# --- АВТОРИЗАЦИЯ: QR-код ---

@user_router.callback_query(F.data == "auth_qr_init")
async def cb_auth_qr_init(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    await manager._cleanup_temp_session(user_id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_auth")]
    ])
    
    await callback.message.edit_text("⏳ Инициализирую QR-код...", reply_markup=kb)
    
    try:
        client = await _new_telethon_client(user_id, is_temp=True)
        await client.connect()
        
        # Получаем объект для QR-логина
        qr_login = await client(functions.auth.ExportLoginTokenRequest(api_id=API_ID, api_hash=API_HASH, except_ids=[user_id]))
        
        # Генерируем QR-код
        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
        qr.add_data(qr_login.url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Сохраняем QR-код в буфер
        buffer = BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)
        
        qr_file = BufferedInputFile(buffer.read(), filename="qr_code.png")
        
        # Сохраняем временный клиент и создаем Future для ожидания
        async with store.lock:
            store.temp_auth_clients[user_id] = client
            qr_future = asyncio.Future()
            store.qr_login_future[user_id] = qr_future
            
        # Запускаем фоновую задачу для ожидания сканирования
        asyncio.create_task(manager.wait_for_qr_scan(user_id, client, qr_login, qr_future))
        
        await state.set_state(TelethonAuth.WAITING_FOR_QR_SCAN)
        
        # Отправляем QR-код
        await bot.send_photo(
            chat_id=user_id,
            photo=qr_file,
            caption="📸 **Сканируйте QR-код** через официальное приложение Telegram (Настройки -> Устройства -> Привязать рабочий стол). Код действует **60 секунд**.",
            reply_markup=kb
        )
        await callback.message.delete()
        
    except Exception as e:
        logger.error(f"QR Auth Error for {user_id}: {e}")
        await manager._send_to_bot_user(user_id, f"❌ Не удалось сгенерировать QR-код. Ошибка: {type(e).__name__}", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")]]))
        await manager._cleanup_temp_session(user_id)
    
    await callback.answer()

# --- ОТМЕНА ЛЮБОЙ АВТОРИЗАЦИИ ---

@user_router.callback_query(F.data == "cancel_auth")
async def cb_cancel_auth(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    await manager._cleanup_temp_session(user_id)
    await state.clear()
    
    sub_end = await db.get_subscription_status(user_id)
    await callback.message.edit_text("✅ Авторизация отменена. Возврат в главное меню.", reply_markup=get_main_keyboard(user_id, sub_end))
    await callback.answer()
    
# --- ВОЗВРАТ В МЕНЮ ---

@user_router.callback_query(F.data == "back_to_menu")
async def cb_back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    sub_end = await db.get_subscription_status(user_id)
    
    await callback.message.edit_text(
        "📝 Главное меню панели управления:",
        reply_markup=get_main_keyboard(user_id, sub_end)
    )
    await callback.answer()


# --- ЗАПУСК/ОСТАНОВКА WORKER'А ---

@user_router.callback_query(F.data == "start_worker")
async def cb_start_worker(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    session_path = get_session_path(user_id) + '.session'
    sub_end = await db.get_subscription_status(user_id)
    now_msk = datetime.now(TIMEZONE_MSK)

    if not os.path.exists(session_path):
        await callback.answer("❌ Сессия не найдена. Пожалуйста, войдите в аккаунт.", show_alert=True)
        return
        
    if sub_end is None or sub_end <= now_msk:
         await callback.answer("❌ Подписка истекла. Активируйте промокод или оплатите подписку.", show_alert=True)
         return
        
    task = await manager.start_client_task(user_id)
    
    if task:
        await callback.message.edit_text("⏳ **Worker запускается...** (Проверка сессии и подписки)", reply_markup=get_main_keyboard(user_id, sub_end))
    else:
        await callback.message.edit_text("⚠️ **Worker уже запущен** или произошла ошибка при запуске.", reply_markup=get_main_keyboard(user_id, sub_end))

    await callback.answer()

@user_router.callback_query(F.data == "stop_worker")
async def cb_stop_worker(callback: CallbackQuery):
    user_id = callback.from_user.id
    sub_end = await db.get_subscription_status(user_id)
    
    await manager.stop_worker(user_id, silent=False) # Отправляет сообщение об остановке
    
    await callback.message.edit_text("🛑 Worker остановлен. Возврат в меню.", reply_markup=get_main_keyboard(user_id, sub_end))
    await callback.answer()

# =========================================================================
# VI. ЗАПУСК БОТА
# =========================================================================

async def on_startup(dp: Dispatcher):
    """Выполняется при запуске бота."""
    await db.init()
    logger.info("Starting up and attempting to restore active workers...")
    
    # Запуск worker'ов, которые были активны до перезапуска
    active_users = await db.get_active_telethon_users()
    for user_id in active_users:
        if os.path.exists(get_session_path(user_id) + '.session'):
            await manager.start_client_task(user_id)
            logger.info(f"Restored worker for user: {user_id}")
        else:
             await db.set_telethon_status(user_id, False)


async def main():
    dp.startup.register(on_startup)
    dp.include_router(user_router)
    # Здесь можно добавить admin_router и другие
    logger.info("Starting Aiogram Bot polling...")
    await dp.start_polling(bot)

if __name__ == '__main__':
    # Инициализация менеджера
    manager = TelethonManager(bot, db)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    except Exception as e:
        logger.critical(f"FATAL BOT ERROR: {e}")
