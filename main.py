import asyncio
import logging
import os
import re
import random
import sys
import aiosqlite
import pytz
import qrcode
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Union, Set
from functools import wraps
from io import BytesIO
from PIL import Image

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, FSInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums.chat_member_status import ChatMemberStatus

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat, InputPeerUser, InputPeerChannel
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, AuthKeyUnregisteredError,
    UserIsBlockedError, PeerIdInvalidError, UsernameInvalidError,
    ChatWriteForbiddenError, # <-- ИСПРАВЛЕНО
    UserNotMutualContactError
)
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch, UserStatusRecently, UserStatusOnline, UserStatusOffline, UserStatusLastWeek, UserStatusLastMonth

# =========================================================================
# I. КОНФИГУРАЦИЯ (ХАРДКОД)
# =========================================================================

BOT_TOKEN = "7868097991:AAEWx2koF8jM-gsNu2lvDpax-tfJUj6lhqw" # ВАШ_ТОКЕН_ОТ_BOTFATHER
ADMIN_ID = 6256576302 
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1"
TARGET_CHANNEL_ID = -1001234567890 # Нужно будет подставить ID канала
SUPPORT_BOT_USERNAME = "suppor_tstatpro1bot"

DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 1.0 # Заглушка для декоратора
SESSION_DIR = 'sessions'
DATA_DIR = 'data'
RETRY_DELAY = 5 # Задержка между запросами для Telethon
QR_TIMEOUT = 180 # Время ожидания скана QR

# Инициализация папок
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())
user_router = Router()
drops_router = Router()

# =========================================================================
# II. ГЛОБАЛЬНОЕ ХРАНИЛИЩЕ И УТИЛИТЫ
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
        self.progress: Dict[str, Union[int, str, List]] = {'sent': 0, 'total': 0}

    def __str__(self):
        elapsed = int((datetime.now(TIMEZONE_MSK) - self.start_time).total_seconds())
        progress_str = ""
        if self.task_type == 'flood':
            total = self.args[0]
            progress_str = f"Отправлено: {self.progress['sent']}{f' из {total}' if total > 0 else ' (∞)'}"
        elif self.task_type == 'check_group':
            progress_str = f"Обработано: {self.progress.get('processed_count', 0)} элементов"
        elif self.task_type == 'mass_dm':
            progress_str = f"Отправлено: {self.progress['sent']} из {self.progress['total']}"
            
        return f"[{self.task_type.upper()}] T:{self.target} ID:{self.task_id[:4]}... [{progress_str}] Время: {elapsed} сек."

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {} # Для FSM авторизации
        self.active_workers: Dict[int, TelegramClient] = {} # {user_id: TelethonClient}
        self.worker_tasks: Dict[int, Dict[str, WorkerTask]] = {} # {user_id: {task_id: WorkerTask}}
        self.pc_monitoring: Dict[Union[int, str], str] = {} # {topic_id / chat_id: pc_name}
        self.qr_login_tasks: Dict[int, asyncio.Task] = {} # Для отслеживания процесса QR-логина

store = GlobalStorage()

class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State() # 2FA при входе по номеру
    WAITING_FOR_QR_LOGIN = State() # Ожидание скана QR
    QR_PASSWORD = State() # 2FA после QR

class PromoCodeStates(StatesGroup):
    waiting_for_code = State()
    
class AdminStates(StatesGroup):
    waiting_for_promo_details = State()
    waiting_for_sub_user_id = State()
    waiting_for_sub_days = State()

class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_report_phone = State()
    
# --- Утилиты для работы с сессиями и временем ---

def get_session_path(user_id: int, is_temp: bool = False) -> str:
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def to_msk_aware(dt_str: str) -> Optional[datetime]:
    if not dt_str: return None
    try:
        naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        return TIMEZONE_MSK.localize(naive_dt)
    except ValueError:
        return None

def get_topic_key(message: types.Message) -> Union[int, str]:
    # Используем message_thread_id для топиков или chat_id для общего чата
    return message.message_thread_id if message.message_thread_id else message.chat.id

def rate_limit(limit: float):
    def decorator(func):
        @wraps(func)
        async def wrapper(message: types.Message, *args, **kwargs):
            # В реальном приложении здесь будет логика проверки времени последнего вызова
            # Для упрощения ТЗ: просто выполним функцию
            return await func(message, *args, **kwargs)
        return wrapper
    return decorator

# --- Утилита для удаления сообщений ---

async def delete_messages_safely(chat_id: Union[int, str], message_ids: List[int], delay: int = 5):
    await asyncio.sleep(delay)
    for msg_id in message_ids:
        try:
            await bot.delete_message(chat_id, msg_id)
        except (TelegramBadRequest, TelegramForbiddenError):
            pass # Игнорируем ошибки, если сообщение уже удалено или бот не имеет прав

# =========================================================================
# III. БАЗА ДАННЫХ
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            
            # Таблица пользователей
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    subscription_active BOOLEAN DEFAULT 0,
                    subscription_end_date TEXT,
                    telethon_active BOOLEAN DEFAULT 0
                )
            """)
            
            # Таблица промокодов
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    days INTEGER,
                    is_active BOOLEAN DEFAULT 1,
                    max_uses INTEGER,
                    current_uses INTEGER DEFAULT 0
                )
            """)
            
            # Таблица сессий дропов
            await db.execute("""
                CREATE TABLE IF NOT EXISTS drop_sessions (
                    phone TEXT, 
                    pc_name TEXT, 
                    drop_id INTEGER, 
                    status TEXT, 
                    start_time TEXT, 
                    last_status_time TEXT, 
                    prosto_seconds INTEGER DEFAULT 0,
                    PRIMARY KEY (phone, pc_name, start_time) -- Сложный ключ для уникальности
                )
            """)
            
            await db.commit()

    # --- Users ---

    async def get_user(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            await db.commit()
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def set_telethon_status(self, user_id: int, status: bool):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            await db.commit()

    async def get_active_telethon_users(self) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
            # Выбираем только тех, у кого активна подписка ИЛИ это админ
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1 AND (subscription_active=1 OR user_id=?)", (ADMIN_ID,)) as cursor:
                return [row[0] for row in await cursor.fetchall()]
    
    async def check_subscription(self, user_id: int) -> bool:
        if user_id == ADMIN_ID: return True

        user = await self.get_user(user_id)
        if not user or not user['subscription_active']:
            return False

        end_date = to_msk_aware(user['subscription_end_date'])
        now = datetime.now(TIMEZONE_MSK)

        if end_date and end_date <= now:
            await self.set_subscription_status(user_id, False, None)
            return False
        
        return True
    
    async def get_subscription_details(self, user_id: int) -> tuple[bool, Optional[datetime]]:
        user = await self.get_user(user_id) # Обеспечивает наличие записи
        if user_id == ADMIN_ID:
            return True, None
            
        if not user or not user['subscription_active']:
            return False, None
            
        end_date = to_msk_aware(user['subscription_end_date'])
        if end_date and end_date <= datetime.now(TIMEZONE_MSK):
            await self.set_subscription_status(user_id, False, None)
            return False, None
        
        return True, end_date

    async def set_subscription_status(self, user_id: int, status: bool, end_date: Optional[datetime]):
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S') if end_date else None
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET subscription_active=?, subscription_end_date=? WHERE user_id=?", 
                             (1 if status else 0, end_date_str, user_id))
            await db.commit()

    async def update_subscription(self, user_id: int, days: int) -> datetime:
        async with aiosqlite.connect(self.db_path) as db:
            active, current_end = await self.get_subscription_details(user_id)
            now = datetime.now(TIMEZONE_MSK)
            
            if active and current_end and current_end > now:
                new_end = current_end + timedelta(days=days)
            else:
                new_end = now + timedelta(days=days)
                
            await self.set_subscription_status(user_id, True, new_end)
            return new_end

    # --- Promocodes ---

    async def get_promocode(self, code: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM promo_codes WHERE code=?", (code.upper(),)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def use_promocode(self, code: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            promocode = await self.get_promocode(code)
            if not promocode or not promocode['is_active'] or promocode['current_uses'] >= promocode['max_uses']:
                return False

            new_uses = promocode['current_uses'] + 1
            is_active = 1 if new_uses < promocode['max_uses'] else 0
            
            await db.execute("UPDATE promo_codes SET current_uses=?, is_active=? WHERE code=?", 
                             (new_uses, is_active, code.upper()))
            await db.commit()
            return True

    # --- Drop Sessions ---
    
    async def get_last_drop_session(self, drop_id: int, pc_name: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM drop_sessions 
                WHERE drop_id=? AND pc_name=?
                ORDER BY start_time DESC LIMIT 1
            """, (drop_id, pc_name)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
                
    async def get_drop_session_by_phone(self, phone: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM drop_sessions 
                WHERE phone=? 
                ORDER BY start_time DESC LIMIT 1
            """, (phone,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def create_drop_session(self, phone: str, pc_name: str, drop_id: int, status: str):
        async with aiosqlite.connect(self.db_path) as db:
            now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
            try:
                await db.execute("""
                    INSERT INTO drop_sessions (phone, pc_name, drop_id, status, start_time, last_status_time) 
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (phone, pc_name, drop_id, status, now_str, now_str))
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def update_drop_status_by_phone(self, phone: str, new_status: str, new_phone: Optional[str] = None):
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.now(TIMEZONE_MSK)
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            
            current_session = await self.get_drop_session_by_phone(phone)
            if not current_session: return False

            old_time = to_msk_aware(current_session['last_status_time'])
            time_diff = int((now - old_time).total_seconds())
            prosto_seconds = current_session['prosto_seconds']

            # Если предыдущий статус был "проблемным", прибавляем время простоя
            if current_session['status'] in ('дайте номер', 'error', 'slet', 'повтор', 'замена'):
                prosto_seconds += time_diff 

            if new_phone and new_phone != phone:
                # 1. Замена номера: закрываем старую сессию
                await db.execute("UPDATE drop_sessions SET status='закрыта (замена)', last_status_time=? WHERE phone=?", (now_str, phone))
                
                # 2. Создаем новую сессию для нового номера
                success = await self.create_drop_session(
                    new_phone, 
                    current_session['pc_name'], 
                    current_session['drop_id'], 
                    'в работе' # Новая сессия сразу в работе после замены
                )
                
                if success:
                    # Копируем накопленный простой в новую сессию
                    await db.execute("UPDATE drop_sessions SET prosto_seconds=? WHERE phone=?", (prosto_seconds, new_phone))
                
                await db.commit()
                return success

            # Просто обновление статуса (для /vstal, /error, /povt, /slet, /num)
            query = """
                UPDATE drop_sessions 
                SET status=?, last_status_time=?, prosto_seconds=? 
                WHERE phone=? AND start_time=?
            """
            await db.execute(query, (new_status, now_str, prosto_seconds, phone, current_session['start_time']))
            await db.commit()
            return True

db = AsyncDatabase(os.path.join(DATA_DIR, DB_NAME))

# =========================================================================
# IV. TELETHON MANAGER
# =========================================================================

class TelethonManager:
    def __init__(self, bot_instance: Bot):
        self.bot = bot_instance

    async def _send_to_bot_user(self, user_id: int, message: str):
        try:
            await self.bot.send_message(user_id, message)
        except (TelegramBadRequest, TelegramForbiddenError):
            logger.warning(f"Failed to send message to user {user_id}. Stopping worker.")
            await self.stop_worker(user_id)
        except Exception:
            await self.stop_worker(user_id)

    # --- Login/Logout Logic ---

    async def finalize_login(self, user_id: int, client: TelegramClient):
        # Переименование временной сессии в постоянную
        temp_path = get_session_path(user_id, is_temp=True)
        perm_path = get_session_path(user_id)
        
        try:
            # Убедиться, что клиент дисконнектен для безопасного перемещения файла сессии
            if await client.is_connected():
                await client.disconnect()
        except:
             pass # Игнорируем ошибки дисконнекта
        
        # Переименование файла сессии
        if os.path.exists(temp_path + '.session'):
            os.rename(temp_path + '.session', perm_path + '.session')
        
        await db.set_telethon_status(user_id, True)
        
        # Запуск Worker-Task
        await self.start_client_task(user_id)

    async def start_client_task(self, user_id: int):
        await self.stop_worker(user_id) # Остановка старого worker'а
        task = asyncio.create_task(self._run_worker(user_id))
        
        async with store.lock: 
            # Сохраняем главный таск, чтобы управлять им
            worker_task = WorkerTask(task_type="main", task_id=f"main-{user_id}", creator_id=user_id, target="worker")
            worker_task.task = task
            store.worker_tasks.setdefault(user_id, {})[worker_task.task_id] = worker_task

    async def _run_worker(self, user_id: int):
        path = get_session_path(user_id)
        client = TelegramClient(path, API_ID, API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        try:
            await client.connect()
        except Exception as e:
            logger.error(f"Worker {user_id} failed to connect: {e}")
            await self._send_to_bot_user(user_id, f"💔 Не удалось подключить Worker: {e.__class__.__name__}. Попробуйте `/logout` и `/login`.")
            return

        async with store.lock: store.active_workers[user_id] = client

        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            await self.worker_message_handler(user_id, client, event)

        try:
            await db.set_telethon_status(user_id, True)
            
            active, sub_end = await db.get_subscription_details(user_id)
            
            if not active and user_id != ADMIN_ID:
                await self._send_to_bot_user(user_id, "⚠️ Ваша подписка истекла или не активна. Worker будет отключен.")
                return 
                
            sub_info = f"Подписка до: **{sub_end.strftime('%d.%m.%Y')}**." if sub_end else "Админ-режим."
            await self._send_to_bot_user(user_id, f"🚀 Worker запущен. {sub_info}")
            
            # Должен работать, пока не будет остановлен или не возникнет ошибка
            await client.run_until_disconnected()
            
        except AuthKeyUnregisteredError:
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Пожалуйста, выполните повторный вход.")
        except asyncio.CancelledError:
            logger.info(f"Worker {user_id} task cancelled.")
        except Exception as e:
            await self._send_to_bot_user(user_id, f"💔 Worker отключился: {e.__class__.__name__}.")
        finally:
            await self.stop_worker(user_id)

    async def stop_worker(self, user_id: int):
        async with store.lock:
            client = store.active_workers.pop(user_id, None)
            
            tasks_to_cancel = store.worker_tasks.pop(user_id, {})
            for task_id, worker_task in tasks_to_cancel.items():
                if worker_task.task and not worker_task.task.done():
                    worker_task.task.cancel()
                    logger.info(f"Task {task_id} for user {user_id} cancelled.")

        if client:
            try:
                await client.disconnect()
            except Exception:
                pass 

        await db.set_telethon_status(user_id, False)

    # --- Telethon Message Handler & Commands ---

    async def worker_message_handler(self, user_id: int, client: TelegramClient, event: events.NewMessage.Event):
        if not event.text or not event.text.startswith('.'):
            return

        msg = event.text.strip().lower()
        parts = msg.split()
        cmd = parts[0]
        chat_id = event.chat_id
        
        # 1. Проверка подписки (кроме админа)
        if user_id != ADMIN_ID:
            active_sub = await db.check_subscription(user_id)
            if not active_sub:
                temp_msg = await client.send_message(chat_id, "❌ Нет активной подписки. Команда проигнорирована.")
                asyncio.create_task(delete_messages_safely(chat_id, [event.message.id, temp_msg.id], delay=3))
                return

        # 2. Автоудаление команды (для большинства)
        if cmd not in ('.пкворк',):
             asyncio.create_task(delete_messages_safely(chat_id, [event.message.id]))
        
        # --- .ПКВОРК ---
        if cmd == '.пкворк':
            pc_name = parts[1] if len(parts) > 1 else 'PC'
            topic_key = get_topic_key(event.message)
            async with store.lock: 
                store.pc_monitoring[topic_key] = pc_name
            temp_msg = await client.send_message(chat_id, f"✅ ПК для топика **{topic_key}** установлен как **{pc_name}**.", reply_to=event.message.id)
            asyncio.create_task(delete_messages_safely(chat_id, [event.message.id, temp_msg.id], delay=3))

        # --- .ФЛУД ---
        elif cmd == '.флуд':
            try:
                if len(parts) < 3: raise IndexError("Недостаточно аргументов.")
                
                # Формат: .флуд <кол-во> <текст> <задержка> [<цель>]
                # Текст может содержать пробелы, поэтому парсинг сложнее
                
                count_str = parts[1]
                delay_str = parts[-1]
                target_str = None
                
                # Если 5+ аргументов, последний - задержка, предпоследний - цель
                if len(parts) >= 5:
                    target_str = parts[-2]
                    # Текст - все между count и target
                    text = " ".join(parts[2:-2])
                else:
                    # Текст - все между count и delay
                    text = " ".join(parts[2:-1])
                    target_str = chat_id # Текущий чат
                
                count = int(count_str)
                delay = float(delay_str)
                target = target_str # Telethon поймет ID или username
                
                if not text: raise ValueError("Нет текста.")
                
                await self._start_flood_task(user_id, client, chat_id, target, count, delay, text)
                
            except (IndexError, ValueError) as e:
                temp_msg = await client.send_message(chat_id, f"❌ Неверный формат. Использование: `.флуд <кол-во> <текст> <задержка> [<цель>]`.")
                asyncio.create_task(delete_messages_safely(chat_id, [temp_msg.id], delay=5))
            except Exception as e:
                temp_msg = await client.send_message(chat_id, f"❌ Ошибка при запуске флуда: {e.__class__.__name__}")
                asyncio.create_task(delete_messages_safely(chat_id, [temp_msg.id], delay=5))

        # --- .СТОПФЛУД ---
        elif cmd == '.стопфлуд':
            await self._stop_tasks_by_type(user_id, "flood", chat_id)
            temp_msg = await client.send_message(chat_id, "✅ Все запущенные задачи флуда остановлены.")
            asyncio.create_task(delete_messages_safely(chat_id, [temp_msg.id], delay=3))
            
        # --- .ЛС ---
        elif cmd == '.лс':
            try:
                lines = event.text.split('\n')
                if len(lines) < 2:
                    temp_msg = await client.send_message(chat_id, "❌ Неверный формат. 1 строка: `.лс <текст>`, далее построчно `@username`.")
                    asyncio.create_task(delete_messages_safely(chat_id, [temp_msg.id], delay=5))
                    return
                
                content = lines[0][len(cmd)+1:].strip() # Текст сообщения
                usernames = [line.strip() for line in lines[1:] if line.strip().startswith('@')] # Список целей
                
                if not usernames: 
                    temp_msg = await client.send_message(chat_id, "❌ Цели для рассылки не указаны.")
                    asyncio.create_task(delete_messages_safely(chat_id, [temp_msg.id], delay=5))
                    return
                
                await self._start_mass_dm_task(user_id, client, chat_id, content, usernames)

            except Exception as e:
                temp_msg = await client.send_message(chat_id, f"❌ Ошибка при запуске рассылки: {e.__class__.__name__}")
                asyncio.create_task(delete_messages_safely(chat_id, [temp_msg.id], delay=5))
                
        # --- .ЧЕКГРУППУ ---
        elif cmd == '.чекгруппу':
            # Ограничим до одной задачи чекгруппу
            await self._stop_tasks_by_type(user_id, "check_group", chat_id, silent=True)
            
            try:
                target = parts[1] if len(parts) > 1 else chat_id
                
                temp_msg = await client.send_message(chat_id, "⏳ Сканирование запущено. Ожидайте отчета в ЛС бота.")
                asyncio.create_task(delete_messages_safely(chat_id, [temp_msg.id], delay=5))
                
                await self._start_check_group_task(user_id, client, target)
                
            except Exception as e:
                temp_msg = await client.send_message(chat_id, f"❌ Ошибка при запуске сканирования: {e.__class__.__name__}")
                asyncio.create_task(delete_messages_safely(chat_id, [temp_msg.id], delay=5))

        # --- .СТАТУС ---
        elif cmd == '.статус':
            await self._report_status(user_id, client, chat_id)


    # --- Telethon Task Management ---
    
    async def _add_task(self, user_id: int, task_type: str, task: asyncio.Task, target: Union[int, str], args: tuple = ()) -> str:
        async with store.lock:
            task_id = f"{task_type}-{random.randint(1000, 9999)}"
            worker_task = WorkerTask(task_type, task_id, user_id, target, args)
            worker_task.task = task
            store.worker_tasks.setdefault(user_id, {})[task_id] = worker_task
            return task_id
            
    async def _remove_task(self, user_id: int, task_id: str):
        async with store.lock:
            if user_id in store.worker_tasks and task_id in store.worker_tasks[user_id]:
                worker_task = store.worker_tasks[user_id].pop(task_id)
                if worker_task.task and not worker_task.task.done():
                    worker_task.task.cancel()
                    
    async def _stop_tasks_by_type(self, user_id: int, task_type: str, report_chat_id: int, silent: bool = False):
        tasks_to_stop = []
        async with store.lock:
            if user_id in store.worker_tasks:
                for task_id, worker_task in list(store.worker_tasks[user_id].items()):
                    if worker_task.task_type == task_type:
                        tasks_to_stop.append(task_id)
                        
        for task_id in tasks_to_stop:
            await self._remove_task(user_id, task_id)

    async def _report_status(self, user_id: int, client: TelegramClient, chat_id: int):
        status_report = [f"**📊 Активные задачи Worker {user_id}:**"]
        
        async with store.lock:
            tasks = store.worker_tasks.get(user_id, {})
            
            found = False
            for task_id, worker_task in tasks.items():
                if worker_task.task_type != "main":
                    status_report.append(f"  * {worker_task}")
                    found = True
                    
            if not found:
                status_report.append("  * Нет активных подзадач.")
                
        temp_msg = await client.send_message(chat_id, "\n".join(status_report))
        asyncio.create_task(delete_messages_safely(chat_id, [temp_msg.id], delay=7))

    # --- Worker Task Implementations ---

    async def _flood_task_executor(self, user_id: int, client: TelegramClient, worker_task: WorkerTask, count: int, delay: float, text: str):
        task_id = worker_task.task_id
        target = worker_task.target
        
        i = 1
        try:
            while count <= 0 or i <= count:
                try:
                    await client.send_message(target, text)
                    worker_task.progress['sent'] = i
                    await asyncio.sleep(delay)
                    i += 1
                except FloodWaitError as e:
                    await self._send_to_bot_user(user_id, f"⏳ **{target}**: FloodWait на {e.seconds} сек. Ожидание...")
                    await asyncio.sleep(e.seconds)
                except ChatWriteForbiddenError: # <-- ИСПРАВЛЕНО ЗДЕСЬ
                    await self._send_to_bot_user(user_id, f"❌ **{target}**: Запрет на отправку.")
                    break
                except Exception as e:
                    await self._send_to_bot_user(user_id, f"❌ **{target}**: Ошибка отправки: {e.__class__.__name__}")
                    break
        finally:
            await self._send_to_bot_user(user_id, f"✅ Задача флуда **{task_id[:4]}...** завершена (цель: **{target}**). Отправлено: {worker_task.progress['sent']}.")
            await self._remove_task(user_id, task_id)

    async def _start_flood_task(self, user_id: int, client: TelegramClient, report_chat_id: int, target: Union[int, str], count: int, delay: float, text: str):
        worker_task = WorkerTask("flood", "temp_id", user_id, target, (count, delay, text))
        task = asyncio.create_task(self._flood_task_executor(user_id, client, worker_task, count, delay, text))
        task_id = await self._add_task(user_id, "flood", task, target, (count, delay, text))
        
        # Обновляем task_id в объекте worker_task
        async with store.lock:
            store.worker_tasks[user_id][task_id].task_id = task_id
            store.worker_tasks[user_id][task_id].task = task
            
        temp_msg = await client.send_message(report_chat_id, f"🚀 Запущена задача флуда **{task_id[:4]}...** на **{count if count > 0 else '∞'}** сообщений в **{target}** с задержкой **{delay}** сек.")
        asyncio.create_task(delete_messages_safely(report_chat_id, [temp_msg.id], delay=5))

    async def _mass_dm_task_executor(self, user_id: int, client: TelegramClient, worker_task: WorkerTask, content: str, usernames: List[str]):
        task_id = worker_task.task_id
        worker_task.progress['total'] = len(usernames)
        success_count = 0
        fail_report = []
        
        for username in usernames:
            try:
                await client.send_message(username, content)
                success_count += 1
                worker_task.progress['sent'] = success_count
                await asyncio.sleep(0.5) # Небольшая задержка для DM
            except FloodWaitError as e:
                fail_report.append(f"❌ {username}: FloodWait на {e.seconds} сек. Рассылка остановлена.")
                break
            except UserNotMutualContactError:
                fail_report.append(f"❌ {username}: Невозможно отправить (неконтакт).")
            except (PeerIdInvalidError, UsernameInvalidError):
                fail_report.append(f"❌ {username}: Неверный ID/юзернейм.")
            except Exception as e:
                fail_report.append(f"❌ {username}: Ошибка ({e.__class__.__name__}).")

        report_message = [f"✅ **Отчет по рассылке (Задача {task_id[:4]}...):**"]
        report_message.append(f"  * **Успешно отправлено:** {success_count} из {len(usernames)}")
        if fail_report:
            report_message.append("\n**⚠️ Ошибки:**")
            report_message.extend(fail_report)

        await self._send_to_bot_user(user_id, "\n".join(report_message))
        await self._remove_task(user_id, task_id)

    async def _start_mass_dm_task(self, user_id: int, client: TelegramClient, report_chat_id: int, content: str, usernames: List[str]):
        worker_task = WorkerTask("mass_dm", "temp_id", user_id, "DM List", (content, usernames))
        task = asyncio.create_task(self._mass_dm_task_executor(user_id, client, worker_task, content, usernames))
        task_id = await self._add_task(user_id, "mass_dm", task, "DM List", (content, usernames))
        
        # Обновляем task_id в объекте worker_task
        async with store.lock:
            store.worker_tasks[user_id][task_id].task_id = task_id
            store.worker_tasks[user_id][task_id].task = task
            
        temp_msg = await client.send_message(report_chat_id, f"🚀 Запущена задача массовой рассылки **{task_id[:4]}...** на **{len(usernames)}** целей. Отчет придет в ЛС бота.")
        asyncio.create_task(delete_messages_safely(report_chat_id, [temp_msg.id], delay=5))

    async def _check_group_task_executor(self, user_id: int, client: TelegramClient, worker_task: WorkerTask, target: Union[int, str]):
        task_id = worker_task.task_id
        users_list = []
        
        try:
            entity = await client.get_entity(target)
            peer_name = getattr(entity, 'title', getattr(entity, 'username', 'N/A'))
            
            if not isinstance(entity, (Channel, Chat)):
                await self._send_to_bot_user(user_id, f"❌ **{target}**: Цель не является группой или каналом.")
                return

            if isinstance(entity, Channel):
                # Итерация по участникам для Channel/Supergroup
                async for participant in client.iter_participants(entity, limit=None):
                    if isinstance(participant, User):
                        username = participant.username if participant.username else 'N/A'
                        name = f"{participant.first_name or ''} {participant.last_name or ''}".strip()
                        status_cls = participant.status.__class__.__name__.replace('UserStatus', '')
                        
                        users_list.append(f"{name} | @{username} | {participant.id} | {status_cls}")
                        worker_task.progress['processed_count'] = len(users_list)
                    await asyncio.sleep(0.01) # Небольшая задержка
            else:
                 # Итерация по сообщениям для обычных чатов (собираем из сообщений)
                async for message in client.iter_messages(entity, limit=2000):
                    if message.sender and isinstance(message.sender, User):
                         if not any(user.id == message.sender.id for user in users_list):
                            username = message.sender.username if message.sender.username else 'N/A'
                            name = f"{message.sender.first_name or ''} {message.sender.last_name or ''}".strip()
                            # Статус в iter_messages может быть недоступен, ставим заглушку
                            users_list.append(f"{name} | @{username} | {message.sender.id} | N/A")
                            worker_task.progress['processed_count'] = len(users_list)
                    await asyncio.sleep(0.01)

            # Формирование отчета
            report_data = f"Имя | @username | ID | Status\n"
            report_data += "-" * 50 + "\n"
            report_data += "\n".join(users_list)
            
            # Сохраняем данные для отправки файлом
            worker_task.progress['report_data'] = report_data
            worker_task.progress['peer_name'] = peer_name

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📥 Файлом .txt", callback_data=f"send_report:{task_id}")],
                [InlineKeyboardButton(text="🗑️ Удалить отчёт", callback_data=f"delete_report:{task_id}")]
            ])
            
            await self._send_to_bot_user(user_id, 
                                        f"✅ **{peer_name}**: Сканирование завершено.\n"
                                        f"Найдено: **{len(users_list)}** уникальных пользователей.\n"
                                        f"Как отправить отчёт?",
                                        reply_markup=kb)

        except FloodWaitError as e:
            await self._send_to_bot_user(user_id, f"⏳ **{target}**: FloodWait на {e.seconds} сек. Сканирование остановлено.")
        except Exception as e:
            logger.error(f"Check group error: {e}")
            await self._send_to_bot_user(user_id, f"❌ **{target}**: Ошибка сканирования: {e.__class__.__name__}")
        finally:
            # Оставляем задачу до тех пор, пока пользователь не удалит отчет или не запросит файл
            pass 

    async def _start_check_group_task(self, user_id: int, client: TelegramClient, target: Union[int, str]):
        worker_task = WorkerTask("check_group", "temp_id", user_id, target)
        task = asyncio.create_task(self._check_group_task_executor(user_id, client, worker_task, target))
        task_id = await self._add_task(user_id, "check_group", task, target)
        
        # Обновляем task_id в объекте worker_task
        async with store.lock:
            store.worker_tasks[user_id][task_id].task_id = task_id
            store.worker_tasks[user_id][task_id].task = task

tm = TelethonManager(bot)

# =========================================================================
# V. AIOGRAM HANDLERS (DROPS)
# =========================================================================

# --- Drop Команды Логика ---

@drops_router.message(Command('пкворк'))
async def command_set_pc_name(message: Message):
    # Эта команда должна обрабатываться TelethonWorker, 
    # но оставим заглушку для информирования в случае использования в боте.
    await message.reply("⚙️ Команда `.пкворк` должна быть отправлена **Worker-аккаунтом** в чате дропов.")

@drops_router.message(Command('numb', 'vstal', 'slet', 'error', 'povt', 'zm', 'num'))
async def handle_drop_commands(message: Message, state: FSMContext):
    drop_id = message.from_user.id # Drop ID - это ID айтишника (пользователя, который ввел команду)
    topic_key = get_topic_key(message)
    pc_name = store.pc_monitoring.get(topic_key)
    
    if not pc_name:
        await message.reply("❌ Не удалось определить **ПК воркера** для этой темы/чата. Сначала выполните `.пкворк PC_NAME` Worker-аккаунтом.")
        return

    cmd = message.text.split()[0].replace('/', '')
    status_map = {
        'numb': 'дайте номер',
        'vstal': 'встал',
        'slet': 'слет',
        'error': 'error',
        'povt': 'повтор',
        'num': 'код пришел',
        'zm': 'замена'
    }
    
    new_status = status_map.get(cmd)
    
    await state.clear()
    await state.set_state(DropStates.waiting_for_phone_and_pc)
    await state.update_data(pc_name=pc_name, drop_id=drop_id, status=new_status, cmd=cmd)
    
    if cmd == 'numb':
        # Для /numb не нужен номер, просто создаем сессию (phone='N/A')
        success = await db.create_drop_session('N/A', pc_name, drop_id, 'дайте номер')
        if success:
            await message.reply(f"✅ Создана новая сессия для **{pc_name}**. Статус: **'дайте номер'**.")
        else:
            await message.reply(f"❌ Номер (N/A) для **{pc_name}** уже в работе. Закройте предыдущую сессию.")
        await state.clear()
        return
        
    if cmd == 'zm':
        await message.reply(f"🔄 **{pc_name}**: Смена номера. Пришлите **старый** и **новый** номер в формате `89xxxxxxxxxx 89yyyyyyyyyy`.")
    else:
        await message.reply(f"☎️ **{pc_name}** переведен в статус **'{new_status}'**. Пришлите номер телефона в формате `89xxxxxxxxxx`.")


@drops_router.message(DropStates.waiting_for_phone_and_pc)
async def process_drop_phone_input(message: Message, state: FSMContext):
    data = await state.get_data()
    pc_name = data['pc_name']
    drop_id = data['drop_id']
    new_status = data['status']
    cmd = data['cmd']
    
    phones = message.text.split()
    
    try:
        if cmd == 'zm':
            if len(phones) != 2:
                await message.reply("❌ Для команды **/zm** нужно 2 номера: `старый_номер новый_номер`")
                return
            
            old_phone, new_phone = phones[0], phones[1]
            success = await db.update_drop_status_by_phone(old_phone, new_status, new_phone=new_phone)
            
            if success:
                await message.reply(f"✅ Статус **{pc_name}** обновлен. Старый номер **{old_phone}** закрыт. Новый номер **{new_phone}** поставлен на **'в работе'**.")
            else:
                await message.reply(f"❌ Не удалось найти активную сессию для номера **{old_phone}**.")
                
        elif cmd == 'num':
            if len(phones) != 1:
                await message.reply("❌ Пришлите только один номер телефона.")
                return
            
            phone = phones[0]
            # /num - меняет статус, а также обновляет номер для N/A, если это была /numb
            current_session = await db.get_drop_session_by_phone('N/A')
            
            if current_session and current_session['drop_id'] == drop_id and current_session['pc_name'] == pc_name:
                # Обновляем N/A на реальный номер
                async with aiosqlite.connect(db.db_path) as conn:
                    await conn.execute("DELETE FROM drop_sessions WHERE phone='N/A'")
                    await conn.commit()
                success = await db.create_drop_session(phone, pc_name, drop_id, 'код пришел')
            else:
                success = await db.update_drop_status_by_phone(phone, new_status)

            if success:
                await message.reply(f"✅ Статус номера **{phone}** обновлен на **'{new_status}'**.")
            else:
                await message.reply(f"❌ Не удалось найти активную сессию для номера **{phone}**.")
                
        else: # /vstal, /error, /povt, /slet
            if len(phones) != 1:
                await message.reply("❌ Пришлите только один номер телефона.")
                return
                
            phone = phones[0]
            success = await db.update_drop_status_by_phone(phone, new_status)

            if success:
                await message.reply(f"✅ Статус номера **{phone}** обновлен на **'{new_status}'**. Простой учтен.")
            else:
                await message.reply(f"❌ Не удалось найти активную сессию для номера **{phone}**.")

    except Exception as e:
        await message.reply(f"❌ Произошла ошибка: {e.__class__.__name__}")
        
    finally:
        await state.clear()


@drops_router.message(Command('report', 'report_last'))
async def handle_report_command(message: Message, state: FSMContext):
    cmd = message.text.split()[0].replace('/', '')
    
    if cmd == 'report_last':
        topic_key = get_topic_key(message)
        pc_name = store.pc_monitoring.get(topic_key)
        drop_id = message.from_user.id
        
        if not pc_name:
             await message.reply("❌ Не удалось определить **ПК воркера**.")
             return

        last_session = await db.get_last_drop_session(drop_id, pc_name)
        if last_session:
            report = await format_drop_report(last_session)
            await message.reply(report)
        else:
            await message.reply("❌ Активных сессий для этого ПК не найдено.")
            
    elif cmd == 'report':
        if len(message.text.split()) > 1:
            phone = message.text.split()[1]
            session = await db.get_drop_session_by_phone(phone)
            if session:
                report = await format_drop_report(session)
                await message.reply(report)
            else:
                await message.reply(f"❌ Сессия для номера **{phone}** не найдена.")
        else:
            await state.set_state(DropStates.waiting_for_report_phone)
            await message.reply("☎️ Введите номер телефона, по которому нужен отчет:")

@drops_router.message(DropStates.waiting_for_report_phone)
async def process_report_phone(message: Message, state: FSMContext):
    phone = message.text.split()[0]
    session = await db.get_drop_session_by_phone(phone)
    await state.clear()
    
    if session:
        report = await format_drop_report(session)
        await message.reply(report)
    else:
        await message.reply(f"❌ Сессия для номера **{phone}** не найдена.")

async def format_drop_report(session: dict) -> str:
    start_dt = to_msk_aware(session['start_time'])
    last_dt = to_msk_aware(session['last_status_time'])
    now = datetime.now(TIMEZONE_MSK)
    
    if not start_dt or not last_dt:
        return f"❌ Некорректные данные о времени в БД для номера {session['phone']}."

    total_seconds = int((now - start_dt).total_seconds())
    prosto_seconds = session['prosto_seconds']
    work_seconds = total_seconds - prosto_seconds
    
    # Конвертация секунд в читаемый формат Ч:М:С
    def format_time(seconds):
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    report = (
        f"**📊 Отчет по дроп-сессии**\n\n"
        f"**ПК / Дроп ID:** {session['pc_name']} / {session['drop_id']}\n"
        f"**Номер:** `{session['phone']}`\n"
        f"**Текущий статус:** `{session['status']}`\n"
        f"**Начало работы:** {start_dt.strftime('%d.%m %H:%M:%S')}\n"
        f"**Последний статус:** {last_dt.strftime('%d.%m %H:%M:%S')}\n\n"
        f"**Общее время:** {format_time(total_seconds)}\n"
        f"**Время в работе:** {format_time(work_seconds)}\n"
        f"**Время простоя:** {format_time(prosto_seconds)}"
    )
    return report

# =========================================================================
# VI. AIOGRAM HANDLERS (USERS, AUTH & SUBSCRIPTIONS)
# =========================================================================

# --- QR Code & Image Generation Utility ---

def generate_qr_image(url: str) -> BytesIO:
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=10,
        border=4,
    )
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").convert('RGB')
    
    buffer = BytesIO()
    img.save(buffer, format='PNG')
    buffer.seek(0)
    return buffer

async def get_menu_keyboard(user_id: int, sub_status: bool, sub_end: Optional[datetime], telethon_active: bool) -> InlineKeyboardMarkup:
    kb_list = []
    
    sub_text = "🟢 До " + sub_end.strftime('%d.%m.%Y') if sub_end else "🟢 Админ"
    if not sub_status and user_id != ADMIN_ID:
        sub_text = "🔴 Не активна / Истекла"
        
    # Строка 1: Статус подписки, Справка, Поддержка
    kb_list.append([
        InlineKeyboardButton(text=f"Подписка: {sub_text}", callback_data="sub_info"),
        InlineKeyboardButton(text="Справка", callback_data="show_help"),
        InlineKeyboardButton(text="Задать вопрос", url=f"https://t.me/{SUPPORT_BOT_USERNAME}")
    ])
    
    # Строка 2: Worker Status
    if telethon_active:
        if user_id in store.active_workers:
            # Worker запущен
            kb_list.append([
                InlineKeyboardButton(text="🟢 Worker Активен", callback_data="worker_status"),
                InlineKeyboardButton(text="⛔ Остановить", callback_data="logout_worker_confirm")
            ])
            # Прогресс задачи
            if any(t.task_type != 'main' for t in store.worker_tasks.get(user_id, {}).values()):
                kb_list.append([InlineKeyboardButton(text="🔍 Прогресс задачи", callback_data="show_worker_progress")])
        else:
            # Worker остановлен
            kb_list.append([
                InlineKeyboardButton(text="▶️ Запустить Worker", callback_data="login_worker_start_only"),
                InlineKeyboardButton(text="🔴 Worker Остановлен", callback_data="worker_status")
            ])
            
    elif sub_status:
        # Сессии нет, но подписка активна - предлагаем вход
        kb_list.append([
            InlineKeyboardButton(text="📷 Вход по QR-коду", callback_data="login_qr_start"),
            InlineKeyboardButton(text="🔑 Вход по Номеру", callback_data="login_phone_start")
        ])

    # Строка 3: Общие действия
    
    general_actions = []
    general_actions.append(InlineKeyboardButton(text="🎁 Активировать Промокод", callback_data="activate_promo"))
    
    if telethon_active:
         general_actions.append(InlineKeyboardButton(text="🗑️ Выход (Удалить сессию)", callback_data="delete_session_confirm"))
         
    kb_list.append(general_actions)
        
    if user_id == ADMIN_ID:
        kb_list.append([InlineKeyboardButton(text="👑 Админ-Панель", callback_data="go_admin")])
        
    return InlineKeyboardMarkup(inline_keyboard=kb_list)


async def check_target_channel(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    
    try:
        member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
        return member.status in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}
    except Exception:
        # Если не удалось проверить (нет доступа к ID канала), предполагаем успех, но это риск. 
        # Лучше заранее найти ID и подставить в константы.
        return True # Временное решение

async def show_subscription_check(message: Message, force_check: bool = False):
    await db.get_user(message.from_user.id) # Создаем/обновляем запись
    
    if await check_target_channel(message.from_user.id):
        sub_active, sub_end = await db.get_subscription_details(message.from_user.id)
        if sub_active or message.from_user.id == ADMIN_ID:
            telethon_active = os.path.exists(get_session_path(message.from_user.id) + '.session')
            kb = await get_menu_keyboard(message.from_user.id, sub_active, sub_end, telethon_active)
            status_text = f"**🤖 Главное меню**"
            await message.answer(status_text, reply_markup=kb)
        else:
            # Подписан, но подписка истекла
            await message.answer("⚠️ Ваша подписка истекла. Используйте промокод или обратитесь в поддержку.")
    else:
        # Не подписан на целевой канал
        channel_name = TARGET_CHANNEL_URL.replace('@', '')
        kb_check = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➡️ Подписаться", url=f"https://t.me/{channel_name}")],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_subscription_again")]
        ])
        await message.answer(
            "🔒 **Доступ к боту закрыт.**\n\n"
            f"Для работы необходимо подписаться на наш канал {TARGET_CHANNEL_URL}.",
            reply_markup=kb_check
        )

# --- Главное меню и подписка ---

@user_router.message(Command('start'))
async def command_start(message: Message, state: FSMContext):
    await state.clear()
    await show_subscription_check(message)
    
@user_router.callback_query(F.data == "check_subscription_again")
async def callback_check_subscription_again(query: types.CallbackQuery, state: FSMContext):
    await query.answer("Проверяю подписку...")
    await state.clear()
    await show_subscription_check(query.message)
    
@user_router.callback_query(F.data == "sub_info")
async def callback_sub_info(query: types.CallbackQuery):
    active, end = await db.get_subscription_details(query.from_user.id)
    if active:
        msg = f"✅ Ваша подписка активна до: **{end.strftime('%d.%m.%Y %H:%M:%S')}**."
    else:
        msg = "🔴 Ваша подписка не активна. Активируйте промокод или обратитесь в поддержку."
    await query.answer(msg, show_alert=True)
    
@user_router.callback_query(F.data == "worker_status")
async def callback_worker_status(query: types.CallbackQuery):
    if query.from_user.id in store.active_workers:
        await query.answer("🟢 Worker активен и обрабатывает события Telethon.", show_alert=True)
    else:
        await query.answer("🔴 Worker остановлен. Нажмите 'Запустить Worker'.", show_alert=True)

# --- Активация промокода ---

@user_router.callback_query(F.data == "activate_promo")
async def callback_activate_promo(query: types.CallbackQuery, state: FSMContext):
    await query.answer()
    await state.set_state(PromoCodeStates.waiting_for_code)
    await query.message.answer("🔑 Введите промокод:")

@user_router.message(PromoCodeStates.waiting_for_code)
async def process_promo_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    user_id = message.from_user.id
    await state.clear()
    
    promo = await db.get_promocode(code)
    
    if not promo:
        await message.answer("❌ Промокод не найден.")
        return
        
    if not promo['is_active'] or promo['current_uses'] >= promo['max_uses']:
        await message.answer("❌ Промокод недействителен или все его использования исчерпаны.")
        return

    if await db.use_promocode(code):
        new_end_date = await db.update_subscription(user_id, promo['days'])
        
        await message.answer(
            f"🎉 **Промокод активирован!**\n"
            f"Подписка продлена на **{promo['days']} дней**.\n"
            f"Новая дата окончания: **{new_end_date.strftime('%d.%m.%Y %H:%M:%S')}**."
        )
    else:
         await message.answer("❌ Не удалось использовать промокод (возможно, он только что закончился).")
         
    await show_subscription_check(message)


# --- Вход/Выход Worker ---

@user_router.callback_query(F.data.in_({"login_phone_start", "login_worker_start_only"}))
async def callback_login_start(query: types.CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    
    if not await db.check_subscription(user_id):
        await query.answer("❌ Доступ к Worker закрыт. Активируйте подписку.", show_alert=True)
        return
        
    telethon_active = os.path.exists(get_session_path(user_id) + '.session')
    
    if query.data == "login_worker_start_only":
        if not telethon_active:
            await query.answer("❌ Сначала нужно выполнить вход по номеру или QR.", show_alert=True)
            return
        if user_id in store.active_workers:
            await query.answer("⚠️ Worker уже запущен.", show_alert=True)
            return
        
        await query.answer("Запуск Worker...")
        await tm.start_client_task(user_id)
        await query.message.answer("🚀 Worker запущен.")
        await show_subscription_check(query.message)
        return

    # login_phone_start (Вход по Номеру)
    if telethon_active:
        await query.answer("⚠️ Сессия уже существует. Удалите ее ('Выход'), чтобы авторизоваться заново.", show_alert=True)
        return
        
    await query.answer()
    await state.set_state(TelethonAuth.PHONE)
    await query.message.answer("📲 Введите **номер телефона** Worker-аккаунта (формат: `+79xxxxxxxxx` или `89xxxxxxxxx`):")


@user_router.callback_query(F.data == "logout_worker_confirm")
async def callback_logout_confirm(query: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, остановить", callback_data="logout_worker")],
        [InlineKeyboardButton(text="Нет, отмена", callback_data="start")]
    ])
    await query.message.answer("⛔ Вы уверены, что хотите остановить Worker?", reply_markup=kb)

@user_router.callback_query(F.data == "logout_worker")
async def callback_logout_worker(query: types.CallbackQuery):
    await query.answer("Остановка Worker...")
    await tm.stop_worker(query.from_user.id)
    await query.message.answer("✅ Worker-аккаунт отключен. Сессия сохранена.")
    await show_subscription_check(query.message)
    
@user_router.callback_query(F.data == "delete_session_confirm")
async def callback_delete_session_confirm(query: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, удалить навсегда", callback_data="delete_session")],
        [InlineKeyboardButton(text="Нет, отмена", callback_data="start")]
    ])
    await query.message.answer("🗑️ Вы уверены, что хотите удалить сессию? Worker будет отключен, и потребуется новая авторизация.", reply_markup=kb)

@user_router.callback_query(F.data == "delete_session")
async def callback_delete_session(query: types.CallbackQuery):
    user_id = query.from_user.id
    
    await query.answer("Удаление сессии...")
    await tm.stop_worker(user_id) # Остановка и дисконнект
    
    # Удаление файла сессии
    perm_path = get_session_path(user_id) + '.session'
    if os.path.exists(perm_path):
        os.remove(perm_path)
        
    await db.set_telethon_status(user_id, False)
    await query.message.answer("✅ Сессия Worker-аккаунта удалена. Требуется повторный вход.")
    await show_subscription_check(query.message)

# --- FSM Авторизация (Телефон) ---

@user_router.message(TelethonAuth.PHONE)
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip().replace(' ', '')
    user_id = message.from_user.id
    
    if not re.match(r'^\+?\d{10,15}$', phone):
        await message.reply("❌ Неверный формат номера. Введите, пожалуйста, еще раз.")
        return
        
    temp_path = get_session_path(user_id, is_temp=True)
    client = TelegramClient(temp_path, API_ID, API_HASH)

    async with store.lock:
        store.temp_auth_clients[user_id] = client

    await message.answer("⏳ Отправка кода...")
    
    try:
        await client.connect()
        if await client.is_user_authorized():
            await tm.finalize_login(user_id, client)
            await state.clear()
            await message.answer("✅ Успешный вход по сохраненной сессии!")
            return
            
        await client.send_code_request(phone)
        await state.update_data(phone=phone)
        await state.set_state(TelethonAuth.CODE)
        await message.answer(f"🔢 Код отправлен на номер **{phone}**. Введите его:")
        
    except PhoneNumberInvalidError:
        await message.reply("❌ Неверный номер телефона. Попробуйте снова.")
        await state.set_state(TelethonAuth.PHONE)
    except FloodWaitError as e:
        await message.reply(f"❌ FloodWait: Слишком много попыток. Повторите через {e.seconds} секунд.")
        await client.disconnect()
        await state.clear()
    except Exception as e:
        logger.error(f"Auth error (phone): {e}")
        await message.reply(f"❌ Неизвестная ошибка: {e.__class__.__name__}. Попробуйте снова.")
        await client.disconnect()
        await state.clear()


@user_router.message(TelethonAuth.CODE)
async def process_code(message: Message, state: FSMContext):
    data = await state.get_data()
    phone = data['phone']
    code = message.text.strip()
    user_id = message.from_user.id
    client = store.temp_auth_clients.get(user_id)
    
    if not client or not await client.is_connected():
        await message.answer("❌ Сессия авторизации прервана. Начните заново командой `/start`.")
        await state.clear()
        return
    
    try:
        await client.sign_in(phone, code)
        
        if await client.is_user_authorized():
            await tm.finalize_login(user_id, client)
            await state.clear()
            await message.answer("✅ Успешный вход! Worker запускается...")
            await show_subscription_check(message)
            return

    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔐 Требуется **облачный пароль (2FA)**. Введите его:")
        return

    except Exception as e:
        error_msg = "❌ Неверный код. Попробуйте еще раз."
        if 'invalid code' not in str(e).lower():
            error_msg = f"❌ Ошибка: {e.__class__.__name__}. Попробуйте снова."
        
        await message.reply(error_msg)
        return
        

@user_router.message(TelethonAuth.PASSWORD)
async def process_password(message: Message, state: FSMContext):
    data = await state.get_data()
    password = message.text.strip()
    user_id = message.from_user.id
    client = store.temp_auth_clients.get(user_id)
    
    if not client or not await client.is_connected():
        await message.answer("❌ Сессия авторизации прервана. Начните заново командой `/start`.")
        await state.clear()
        return

    try:
        await client.sign_in(password=password)
        
        if await client.is_user_authorized():
            await tm.finalize_login(user_id, client)
            await state.clear()
            await message.answer("✅ Успешный вход (2FA)! Worker запускается...")
            await show_subscription_check(message)
            return
            
    except Exception as e:
        error_msg = "❌ Неверный пароль. Попробуйте еще раз."
        if 'password invalid' not in str(e).lower():
             error_msg = f"❌ Ошибка: {e.__class__.__name__}. Попробуйте снова."
        await message.reply(error_msg)
        return
        
# --- FSM Авторизация (QR Code) ---

async def handle_qr_login_timeout(user_id: int):
    # Эта задача будет отменена при успешном скане
    await asyncio.sleep(QR_TIMEOUT)
    client = store.temp_auth_clients.get(user_id)
    if client and await client.is_connected():
        await client.disconnect()
        
    await bot.send_message(user_id, "⌛ Время ожидания сканирования QR-кода истекло. Попробуйте снова.")
    async with store.lock:
        store.temp_auth_clients.pop(user_id, None)
        store.qr_login_tasks.pop(user_id, None)


async def check_qr_login_status(user_id: int, client: TelegramClient):
    try:
        while True:
            await asyncio.sleep(1) 
            if await client.is_user_authorized():
                await tm.finalize_login(user_id, client)
                await bot.send_message(user_id, "✅ QR-код успешно отсканирован! Worker запускается...")
                return
            
    except SessionPasswordNeededError:
        await bot.send_message(user_id, "🔐 QR-код отсканирован, но требуется **облачный пароль (2FA)**. Введите его:")
        return # Перевод в QR_PASSWORD должен быть сделан вне этого цикла, через try/except
        
    except asyncio.CancelledError:
        pass # Нормальное завершение по таймауту или успешному входу
    except Exception as e:
        await bot.send_message(user_id, f"❌ Ошибка при ожидании QR-кода: {e.__class__.__name__}")
    finally:
        async with store.lock:
            store.qr_login_tasks.pop(user_id, None)


@user_router.callback_query(F.data == "login_qr_start")
async def callback_login_qr_start(query: types.CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    
    if not await db.check_subscription(user_id):
        await query.answer("❌ Доступ к Worker закрыт. Активируйте подписку.", show_alert=True)
        return
        
    if os.path.exists(get_session_path(user_id) + '.session'):
        await query.answer("⚠️ Сессия уже существует. Удалите ее ('Выход'), чтобы авторизоваться заново.", show_alert=True)
        return

    await query.answer("Генерация QR-кода...")
    await state.clear()
    await state.set_state(TelethonAuth.WAITING_FOR_QR_LOGIN)
    
    temp_path = get_session_path(user_id, is_temp=True)
    client = TelegramClient(temp_path, API_ID, API_HASH)

    async with store.lock:
        store.temp_auth_clients[user_id] = client

    try:
        await client.connect()
        qr_login = await client.qr_login()
        
        # Запуск таска для отслеживания таймаута
        timeout_task = asyncio.create_task(handle_qr_login_timeout(user_id))
        async with store.lock:
            store.qr_login_tasks[user_id] = timeout_task
        
        # Если Telegram предоставил изображение QR
        if hasattr(qr_login, 'image') and qr_login.image:
            await bot.send_photo(user_id, FSInputFile(BytesIO(qr_login.image)))
        else:
            # Генерация QR из URL
            qr_image = generate_qr_image(qr_login.url)
            await bot.send_photo(user_id, FSInputFile(qr_image, filename='qr_code.png'), caption="📷 Отсканируйте этот QR-код для входа. Ждем 180 секунд.")
        
        # В отличие от входа по номеру, здесь нужно активно проверять статус
        # Простая реализация: просто ждем, пока is_user_authorized() станет True
        while not await client.is_user_authorized():
            await asyncio.sleep(1)
            # Внутри цикла нужно проверять 2FA
            try:
                await client.is_user_authorized() 
            except SessionPasswordNeededError:
                timeout_task.cancel()
                await state.set_state(TelethonAuth.QR_PASSWORD)
                await query.message.answer("🔐 QR-код отсканирован, но требуется **облачный пароль (2FA)**. Введите его:")
                return
            except asyncio.CancelledError:
                return # Таймаут или успешный вход
        
        # Успешный вход
        await tm.finalize_login(user_id, client)
        timeout_task.cancel()
        await state.clear()
        await show_subscription_check(query.message)
        
    except FloodWaitError as e:
        await query.message.answer(f"❌ FloodWait: Слишком много попыток. Повторите через {e.seconds} секунд.")
    except Exception as e:
        logger.error(f"QR Auth error: {e}")
        await query.message.answer(f"❌ Ошибка QR-авторизации: {e.__class__.__name__}. Попробуйте снова.")
    finally:
        if user_id in store.temp_auth_clients:
            async with store.lock:
                store.temp_auth_clients.pop(user_id, None)


@user_router.message(TelethonAuth.QR_PASSWORD)
async def process_qr_password(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    client = store.temp_auth_clients.get(user_id)
    
    if not client or not await client.is_connected():
        await message.answer("❌ Сессия авторизации прервана. Начните заново командой `/start`.")
        await state.clear()
        return

    try:
        await client.sign_in(password=password)
        
        if await client.is_user_authorized():
            await tm.finalize_login(user_id, client)
            await state.clear()
            await message.answer("✅ Успешный вход (2FA после QR)! Worker запускается...")
            await show_subscription_check(message)
            return
            
    except Exception:
        await message.reply("❌ Неверный пароль. Попробуйте еще раз.")
        return
        
# --- CheckGroup Report Handlers ---

@user_router.callback_query(F.data.startswith("send_report:"))
async def callback_send_report(query: types.CallbackQuery):
    user_id = query.from_user.id
    task_id = query.data.split(':')[1]
    
    async with store.lock:
        worker_task = store.worker_tasks.get(user_id, {}).get(task_id)

    if not worker_task or 'report_data' not in worker_task.progress:
        await query.answer("❌ Отчет не найден или устарел.", show_alert=True)
        return
        
    report_content = worker_task.progress['report_data']
    peer_name = worker_task.progress.get('peer_name', 'chat')
    
    buffer = BytesIO(report_content.encode('utf-8'))
    buffer.name = f"scan_report_{peer_name}_{datetime.now().strftime('%Y%m%d%H%M')}.txt"
    
    await query.answer("Отправка файла...")
    await bot.send_document(user_id, FSInputFile(buffer, filename=buffer.name), caption=f"Отчет по сканированию чата **{peer_name}**.")

@user_router.callback_query(F.data.startswith("delete_report:"))
async def callback_delete_report(query: types.CallbackQuery):
    user_id = query.from_user.id
    task_id = query.data.split(':')[1]
    
    await tm._remove_task(user_id, task_id)
    
    await query.answer("✅ Отчет удален из памяти.", show_alert=True)
    await query.message.edit_text("✅ Отчет удален из памяти.")

# =========================================================================
# VII. AIOGRAM HANDLERS (ADMIN PANEL)
# =========================================================================

def is_admin(func):
    """Декоратор для проверки прав администратора."""
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id != ADMIN_ID:
            await message.reply("❌ Доступ запрещен.")
            return
        return await func(message, *args, **kwargs)
    return wrapper


@user_router.callback_query(F.data == "go_admin")
@user_router.message(Command('admin'))
@is_admin
async def command_admin(query_or_message: Union[types.CallbackQuery, types.Message], state: FSMContext):
    if isinstance(query_or_message, types.CallbackQuery):
        message = query_or_message.message
        await query_or_message.answer()
    else:
        message = query_or_message
        
    await state.clear()
    
    menu_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Создать Промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_add_sub")],
        [InlineKeyboardButton(text="➖ Снять подписку", callback_data="admin_remove_sub")],
    ])
    
    active_workers_count = len(store.active_workers)
    
    await message.answer(
        f"**👑 АДМИН ПАНЕЛЬ**\n\n"
        f"**Активных Worker'ов:** {active_workers_count}\n"
        f"**ID:** `{ADMIN_ID}`",
        reply_markup=menu_kb
    )

# --- Promocode Creation ---

@user_router.callback_query(F.data == "admin_create_promo")
@is_admin
async def admin_create_promo(query: types.CallbackQuery, state: FSMContext):
    await query.answer()
    await state.set_state(AdminStates.waiting_for_promo_details)
    await query.message.answer(
        "📝 Введите детали промокода в формате:\n"
        "**`<кол-во дней> <кол-во использований>`**\n\n"
        "Пример: `30 10` (30 дней, 10 раз)"
    )


@user_router.message(AdminStates.waiting_for_promo_details)
@is_admin
async def process_promo_details(message: Message, state: FSMContext):
    try:
        parts = message.text.split()
        if len(parts) != 2:
            raise ValueError("Неверное количество параметров.")
        
        duration_days = int(parts[0])
        uses_left = int(parts[1])
        
        if duration_days <= 0 or uses_left <= 0:
            raise ValueError("Дни и использования должны быть положительными числами.")
            
        # Генерация рандомного кода (8 символов)
        promo_code = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))
        
        async with aiosqlite.connect(db.db_path) as conn:
            await conn.execute(
                "INSERT INTO promo_codes (code, days, is_active, max_uses, current_uses) VALUES (?, ?, ?, ?, ?)",
                (promo_code, duration_days, 1, uses_left, 0)
            )
            await conn.commit()

        await state.clear()
        await message.answer(
            f"✅ **Промокод успешно создан!**\n"
            f"**КОД:** `{promo_code}`\n"
            f"**Дней:** {duration_days}\n"
            f"**Использований:** {uses_left}"
        )
        
    except ValueError as e:
        await message.reply(f"❌ Ошибка формата: {e}. Повторите ввод: `<кол-во дней> <кол-во использований>`")
    except Exception as e:
        await message.reply(f"❌ Непредвиденная ошибка БД: {e.__class__.__name__}")
        await state.clear()

# --- Subscription Management ---

@user_router.callback_query(F.data.in_({"admin_add_sub", "admin_remove_sub"}))
@is_admin
async def admin_start_sub_management(query: types.CallbackQuery, state: FSMContext):
    action = query.data.split('_')[-2] # 'add' or 'remove'
    
    await query.answer()
    await state.set_state(AdminStates.waiting_for_sub_user_id)
    await state.update_data(action=action)
    
    verb = "выдать" if action == "add" else "снять"
    await query.message.answer(f"👤 Введите **ID пользователя**, которому нужно {verb} подписку:")


@user_router.message(AdminStates.waiting_for_sub_user_id)
@is_admin
async def process_sub_user_id(message: Message, state: FSMContext):
    try:
        target_user_id = int(message.text.strip())
        data = await state.get_data()
        action = data['action']
        
        if action == 'remove':
            # Снятие подписки
            await db.set_subscription_status(target_user_id, False, None)
            
            # Если Worker активен, оповещаем и отключаем его
            if target_user_id in store.active_workers:
                await tm.stop_worker(target_user_id)
                await tm._send_to_bot_user(target_user_id, "⚠️ Ваша подписка была снята администратором. Worker отключен.")
            
            await message.answer(f"✅ Подписка пользователя `{target_user_id}` успешно **снята**.")
            await state.clear()
            
        elif action == 'add':
            # Добавление подписки
            await state.update_data(target_user_id=target_user_id)
            await state.set_state(AdminStates.waiting_for_sub_days)
            await message.answer(f"📅 Введите **количество дней** для пользователя `{target_user_id}`:")
            
    except ValueError:
        await message.reply("❌ ID пользователя должен быть числом.")
    except Exception as e:
        await message.reply(f"❌ Непредвиденная ошибка: {e.__class__.__name__}")
        await state.clear()


@user_router.message(AdminStates.waiting_for_sub_days)
@is_admin
async def process_sub_days(message: Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError("Дни должны быть положительным числом.")
            
        data = await state.get_data()
        target_user_id = data['target_user_id']
        
        new_end_date = await db.update_subscription(target_user_id, days)
        
        # Если Worker отключен, оповещаем о новой подписке
        if target_user_id not in store.active_workers:
            await tm._send_to_bot_user(target_user_id, 
                                       f"🎉 Администратор выдал вам подписку на **{days} дней**!\n"
                                       f"Теперь подписка действительна до: **{new_end_date.strftime('%d.%m.%Y')}**.")
        
        await message.answer(f"✅ Подписка для пользователя `{target_user_id}` успешно **добавлена** на {days} дней. Итого до: **{new_end_date.strftime('%d.%m.%Y')}**.")
        await state.clear()
        
    except ValueError as e:
        await message.reply(f"❌ Ошибка: {e}. Введите корректное количество дней.")
    except Exception as e:
        await message.reply(f"❌ Непредвиденная ошибка: {e.__class__.__name__}")
        await state.clear()

# =========================================================================
# VIII. ЗАПУСК БОТА
# =========================================================================

async def on_startup():
    logger.info("Initializing database...")
    await db.init()
    
    logger.info("Starting Telethon workers for active users...")
    active_users = await db.get_active_telethon_users()
    for user_id in active_users:
        # Проверяем подписку еще раз перед запуском
        if await db.check_subscription(user_id):
            asyncio.create_task(tm.start_client_task(user_id))
        else:
            await db.set_telethon_status(user_id, False)

async def main():
    # Регистрация роутеров
    dp.include_router(user_router)
    dp.include_router(drops_router)
    
    # Запуск
    dp.startup.register(on_startup)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shut down manually.")
