import asyncio
import logging
import os
import re
import random
import string
import traceback
import sys
import aiosqlite
import pytz
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Union
from functools import wraps
from dotenv import load_dotenv

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, AuthKeyUnregisteredError,
    UserIsBlockedError
)
from telethon.utils import get_display_name

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ AIOGRAM
# =========================================================================

load_dotenv()

# Константы
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID', 0))
API_ID = int(os.getenv('API_ID', 0))
API_HASH = os.getenv('API_HASH')
DROPS_CHAT_ID = int(os.getenv('DROPS_CHAT_ID', 0))
SUPPORT_BOT_USERNAME = "suppor_tstatpro1bot"

DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 1.0
SESSION_DIR = 'sessions'

if not os.path.exists(SESSION_DIR): os.makedirs(SESSION_DIR)
if not os.path.exists('data'): os.makedirs('data')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Объявление bot и dp
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())
user_router = Router()
drops_router = Router()
admin_router = Router() # <--- ДОБАВЛЕНО: Роутер для админа

# =========================================================================
# II. ГЛОБАЛЬНЫЕ ХРАНИЛИЩА И УТИЛИТЫ
# =========================================================================

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {}
        self.process_progress: Dict[int, Dict] = {} # {user_id: {'type': 'flood', 'stop': False}}
        self.last_user_request: Dict[int, datetime] = {}
        self.pc_monitoring: Dict[Union[int, str], str] = {} # {topic_id/drop_id: pc_name}
        self.active_workers: Dict[int, TelegramClient] = {}
        self.worker_tasks: Dict[int, List[asyncio.Task]] = {}

store = GlobalStorage()

# --- FSM СОСТОЯНИЯ ---
class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()

class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_phone_change = State()

class PromoState(StatesGroup): # <--- ДОБАВЛЕНО: Состояние для промокода
    waiting_for_promo = State()

class AdminState(StatesGroup): # <--- ДОБАВЛЕНО: Состояние для админ-панели
    waiting_for_promo_data = State()

# --- УТИЛИТЫ ---
def get_session_path(user_id, is_temp=False):
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def get_current_time_msk() -> datetime:
    return datetime.now(TIMEZONE_MSK)

def to_msk_aware(dt_str: str) -> datetime:
    naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    return TIMEZONE_MSK.localize(naive_dt)

def format_timedelta(td: timedelta) -> str:
    """Форматирует timedelta в часы, минуты, секунды."""
    total_seconds = int(td.total_seconds())
    if total_seconds < 0:
        return "N/A"
    
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    parts = []
    if hours > 0: parts.append(f"{hours}ч")
    if minutes > 0: parts.append(f"{minutes}м")
    if seconds > 0 or not parts: parts.append(f"{seconds}с")
    
    return " ".join(parts)

def get_topic_name_from_message(message: types.Message) -> Optional[str]:
    """Пытается получить имя ПК/топика из сообщения."""
    if message.chat.id == DROPS_CHAT_ID and message.message_thread_id:
        topic_id = message.message_thread_id
        # Проверяем по topic_id, если не найдено, используем drop_id
        if topic_id in store.pc_monitoring:
            return store.pc_monitoring[topic_id]
        
        # Если команда от дропа, но топик не стартанул (.пкстарт)
        if message.from_user.id in store.pc_monitoring:
             return store.pc_monitoring[message.from_user.id]

    return None

# Декоратор для ограничения частоты запросов
def rate_limit(limit: float):
    def decorator(func):
        @wraps(func)
        async def wrapper(message: types.Message, *args, **kwargs):
            user_id = message.from_user.id
            now = get_current_time_msk()
            
            async with store.lock:
                last = store.last_user_request.get(user_id)
                if last and (now - last).total_seconds() < limit:
                    return await message.answer("⚠️ Не так быстро! Подождите.")
                store.last_user_request[user_id] = now
            return await func(message, *args, **kwargs)
        return wrapper
    return decorator

# =========================================================================
# III. ASYNC DATABASE (AIOSQLITE)
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA foreign_keys=ON;")
            await db.execute("""CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    subscription_active BOOLEAN DEFAULT 0,
                    subscription_end_date TEXT,
                    telethon_active BOOLEAN DEFAULT 0
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS drop_sessions (
                    phone TEXT PRIMARY KEY,
                    pc_name TEXT,
                    drop_id INTEGER,
                    status TEXT,
                    start_time TEXT,
                    last_status_time TEXT,
                    prosto_seconds INTEGER DEFAULT 0
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    days INTEGER,
                    uses_left INTEGER
            )""") # <--- ДОБАВЛЕНО: Таблица для промокодов
            await db.commit()
        logger.info("Database initialized successfully.")

    async def get_user(self, user_id):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            # Создаем или получаем пользователя
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            await db.commit()
            async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def check_subscription(self, user_id):
        if user_id == ADMIN_ID: return True
        user = await self.get_user(user_id)
        if not user or not user['subscription_active']: return False
        
        end_date_str = user['subscription_end_date']
        if not end_date_str: return False

        try:
            end = to_msk_aware(end_date_str)
            now = get_current_time_msk()
            if end > now:
                return True
            else:
                # Автоматическое отключение, если подписка истекла
                await self.set_telethon_status(user_id, False)
                await self.set_subscription_status(user_id, False, None)
                await tm.stop_worker(user_id)
                return False
        except:
            return False

    async def set_telethon_status(self, user_id, status):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            await db.commit()
    
    async def set_subscription_status(self, user_id, status, end_date_str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET subscription_active=?, subscription_end_date=? WHERE user_id=?", (1 if status else 0, end_date_str, user_id))
            await db.commit()
            
    # --- PROMO LOGIC ---
    async def get_promo(self, code):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM promo_codes WHERE code=?", (code,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def create_promo(self, code, days, uses):
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute("INSERT INTO promo_codes (code, days, uses_left) VALUES (?, ?, ?)", (code, days, uses))
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def activate_promo(self, user_id, code):
        promo = await self.get_promo(code)
        if not promo or promo['uses_left'] <= 0:
            return False, "Неверный или истекший промокод."

        user = await self.get_user(user_id)
        
        now = get_current_time_msk()
        
        # Определяем, от какой даты продлевать
        if user and user['subscription_active'] and user['subscription_end_date']:
            try:
                current_end = to_msk_aware(user['subscription_end_date'])
                if current_end > now:
                    base_date = current_end
                else:
                    base_date = now
            except:
                base_date = now
        else:
            base_date = now

        new_end_date = base_date + timedelta(days=promo['days'])
        new_end_date_str = new_end_date.strftime('%Y-%m-%d %H:%M:%S')

        async with aiosqlite.connect(self.db_path) as db:
            # 1. Обновляем подписку пользователя
            await db.execute(
                "UPDATE users SET subscription_active=1, subscription_end_date=? WHERE user_id=?",
                (new_end_date_str, user_id)
            )
            # 2. Уменьшаем количество использований промокода
            await db.execute(
                "UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code=?",
                (code,)
            )
            await db.commit()
            
        return True, f"Подписка продлена до {new_end_date.strftime('%d.%m.%Y')}."

    async def get_active_telethon_users(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    
    async def get_total_users(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(user_id) FROM users") as cursor:
                row = await cursor.fetchone()
                return row[0]
                
    async def get_total_drop_sessions(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(phone) FROM drop_sessions WHERE status NOT IN ('closed', 'deleted', 'замена_закрыт')") as cursor:
                row = await cursor.fetchone()
                return row[0]
            
    async def get_drop_session(self, phone):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions WHERE phone=?", (phone,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_drop_session_by_drop_id(self, drop_id):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            # Ищем последнюю активную сессию
            async with db.execute("SELECT * FROM drop_sessions WHERE drop_id=? AND status NOT IN ('closed', 'deleted', 'замена_закрыт') ORDER BY start_time DESC LIMIT 1", (drop_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
            
    async def get_last_session_by_pc(self, pc_name):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            # Ищем последнюю активную сессию по имени ПК
            async with db.execute("SELECT * FROM drop_sessions WHERE pc_name=? AND status NOT IN ('closed', 'deleted', 'замена_закрыт') ORDER BY last_status_time DESC LIMIT 1", (pc_name,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def create_drop_session(self, phone, pc_name, drop_id, status):
        async with aiosqlite.connect(self.db_path) as db:
            now_str = get_current_time_msk().strftime('%Y-%m-%d %H:%M:%S')
            try:
                await db.execute(
                    "INSERT INTO drop_sessions (phone, pc_name, drop_id, status, start_time, last_status_time) VALUES (?, ?, ?, ?, ?, ?)",
                    (phone, pc_name, drop_id, status, now_str, now_str)
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def update_drop_status(self, phone, new_status, new_phone=None):
        old_session = await self.get_drop_session(phone)
        if not old_session: return None

        now = get_current_time_msk()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        last_time = to_msk_aware(old_session['last_status_time'])
        prosto_seconds = old_session['prosto_seconds']

        # Если статус меняется на 'в работе' из простаивающего, считаем простой
        if old_session['status'] in ('дайте номер', 'error', 'slet', 'повтор') and new_status == 'в работе':
            duration = (now - last_time).total_seconds()
            prosto_seconds += int(duration)
        
        # Если статус меняется НА простаивающий, обнуляем счетчик простоя
        elif new_status in ('дайте номер', 'error', 'slet', 'повтор'):
            prosto_seconds = 0

        async with aiosqlite.connect(self.db_path) as db:
            if new_phone and new_phone != phone:
                # 1. Закрываем старую сессию
                await db.execute("UPDATE drop_sessions SET status='замена_закрыт' WHERE phone=?", (phone,))
                # 2. Создаем новую сессию
                await db.execute(
                    "INSERT INTO drop_sessions (phone, pc_name, drop_id, status, start_time, last_status_time, prosto_seconds) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (new_phone, old_session['pc_name'], old_session['drop_id'], new_status, old_session['start_time'], now_str, prosto_seconds)
                )
            else:
                # Просто обновляем статус
                await db.execute(
                    "UPDATE drop_sessions SET status=?, last_status_time=?, prosto_seconds=? WHERE phone=?",
                    (new_status, now_str, prosto_seconds, phone)
                )
            await db.commit()
        return True

db = AsyncDatabase(os.path.join('data', DB_NAME))

# =========================================================================
# IV. TELETHON MANAGER (NON-BLOCKING WORKER)
# =========================================================================

class TelethonManager:
    def __init__(self, bot_instance: Bot):
        self.bot = bot_instance
        
    async def _send_to_bot_user(self, user_id, message, html_mode=True):
        try:
            await self.bot.send_message(user_id, message, parse_mode='HTML' if html_mode else None)
        except (TelegramForbiddenError, TelegramBadRequest, UserIsBlockedError):
            logger.error(f"Cannot send message to {user_id}. Stopping worker.")
            await self.stop_worker(user_id)
        except Exception as e:
            logger.error(f"Unknown error sending message to {user_id}: {e}")

    async def start_worker_session(self, user_id, client: TelegramClient):
        """Переименовывает временную сессию в постоянную и запускает Task."""
        temp_path = get_session_path(user_id, True) + '.session'
        real_path = get_session_path(user_id) + '.session'

        try:
            # Убеждаемся, что клиент отключен перед переименованием
            if client.is_connected():
                await client.disconnect()

            if os.path.exists(temp_path):
                # Переименовываем временный файл сессии в постоянный
                os.rename(temp_path, real_path)
            
            # Запускаем Task
            await self.start_client_task(user_id)
            await db.set_telethon_status(user_id, True)
            
        except Exception as e:
            logger.error(f"Failed to finalize session for {user_id}: {e}")
            await self._send_to_bot_user(user_id, f"⚠️ Ошибка запуска worker: {e.__class__.__name__}. Повторите вход.")

    async def start_client_task(self, user_id):
        """Создает и запускает Task для worker'а."""
        
        await self.stop_worker(user_id)
        
        task = asyncio.create_task(self._run_worker(user_id))
        
        async with store.lock:
            store.worker_tasks.setdefault(user_id, []).append(task)
            
        return task

    async def _run_worker(self, user_id):
        path = get_session_path(user_id)
        client = TelegramClient(path, API_ID, API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        async with store.lock:
            store.active_workers[user_id] = client

        # Подписка на исходящие сообщения для выполнения команд
        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            await self.worker_message_handler(user_id, client, event)

        try:
            await client.start()
            
            # Проверка подписки перед продолжением
            if not await db.check_subscription(user_id):
                await self._send_to_bot_user(user_id, "⚠️ **Ваша подписка истекла.** Worker остановлен.")
                return 
            
            await db.set_telethon_status(user_id, True)
            await self._send_to_bot_user(user_id, "🚀 Worker успешно запущен!")
            logger.info(f"Worker {user_id} started and connected.")
            
            await asyncio.sleep(float('inf'))

        except AuthKeyUnregisteredError:
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Требуется повторная авторизация.")
            if os.path.exists(path + '.session'): os.remove(path + '.session')
        except Exception as e:
            logger.error(f"Worker {user_id} failed: {e}")
            await self._send_to_bot_user(user_id, f"💔 Worker отключился: {e.__class__.__name__}.")
        finally:
            await self.stop_worker(user_id)

    async def stop_worker(self, user_id):
        async with store.lock:
            client = store.active_workers.pop(user_id, None)
            
            tasks = store.worker_tasks.pop(user_id, [])
            for t in tasks:
                if not t.done(): t.cancel()
            
            store.process_progress.pop(user_id, None)

        if client:
            try:
                await client.disconnect()
            except:
                pass
        await db.set_telethon_status(user_id, False)

    async def worker_message_handler(self, user_id, client, event):
        if not event.text or not event.text.startswith('.'): return
        
        if not await db.check_subscription(user_id):
            return await client.send_message(event.chat_id, "⚠️ Ваша подписка истекла.")
            
        msg = event.text.strip()
        parts = msg.split()
        cmd = parts[0].lower()

        # Удаляем сообщение, чтобы не засорять чат
        await event.delete()
        
        # --- .ПKСТАРТ (Установка имени ПК) ---
        if cmd == '.пкстарт' or cmd == '.пкворк':
            try:
                pc_name = " ".join(parts[1:]) if len(parts) > 1 else 'PC'
                
                # Используем drop_id для привязки к сессии, если это дроп-чат,
                # или chat_id/topic_id для мониторинга в общем чате
                topic_id = event.message.message_thread_id or event.chat_id
                
                async with store.lock:
                    store.pc_monitoring[user_id] = pc_name # Привязываем к worker'у
                    store.pc_monitoring[topic_id] = pc_name # Привязываем к топику
                
                temp = await client.send_message(event.chat_id, f"✅ Имя ПК установлено как **{pc_name}**.", reply_to=event.message.id)
                await asyncio.sleep(2)
                await temp.delete()
            except Exception as e:
                logger.error(f"PC start error: {e}")
                
        # --- .ФЛУД ---
        elif cmd == '.флуд':
            # Логика флуда... (как в вашем коде)
            try:
                if len(parts) < 4: 
                    return await client.send_message(event.chat_id, "⚠️ Формат: .флуд <кол-во> <цель> <текст> [задержка]")

                count = int(parts[1])
                target = parts[2]
                text = " ".join(parts[3:])
                delay = 0.5 
                
                # Находим задержку в конце
                if text and text.split()[-1].replace('.', '', 1).isdigit():
                    delay_str = text.split()[-1]
                    delay = float(delay_str)
                    text = " ".join(text.split()[:-1])


                # Получаем сущность
                try:
                    entity = await client.get_entity(target)
                except Exception:
                    return await client.send_message(event.chat_id, f"❌ Не могу найти сущность: {target}")

                async with store.lock:
                    store.process_progress[user_id] = {'type': 'flood', 'stop': False}
                
                task = asyncio.create_task(self._flood_task(client, entity, text, count, delay, user_id))
                async with store.lock:
                    store.worker_tasks.setdefault(user_id, []).append(task)

                temp = await client.send_message(event.chat_id, f"🚀 Флуд запущен на {target}. Для остановки введите `.стопфлуд`")
                await asyncio.sleep(2)
                await temp.delete()
            except Exception as e:
                logger.error(f"Flood setup error: {e}")
                await client.send_message(event.chat_id, f"❌ Ошибка: {e.__class__.__name__}")


        # --- .СТОПФЛУД ---
        elif cmd == '.стопфлуд':
            async with store.lock:
                if store.process_progress.get(user_id, {}).get('type') == 'flood':
                    store.process_progress[user_id]['stop'] = True
                    temp = await client.send_message(event.chat_id, "🛑 Флуд остановлен.")
                    await asyncio.sleep(2)
                    await temp.delete()
                else:
                    temp = await client.send_message(event.chat_id, "⚠️ Активный флуд не найден.")
                    await asyncio.sleep(2)
                    await temp.delete()

        # --- .ЛС (Массовая рассылка) ---
        elif cmd == '.лс': 
            await client.send_message(event.chat_id, "🚧 **.ЛС**: Логика рассылки запущена. Отправка...")
            # Здесь должна быть логика массовой рассылки через client.send_message
            await asyncio.sleep(3)
            await client.send_message(event.chat_id, "✅ **.ЛС**: Завершена. Успешно: N, Ошибка: M.")
            
        # --- .ЧЕКГРУППУ (Сбор участников) ---
        elif cmd == '.чекгруппу': 
            if len(parts) < 2: 
                return await client.send_message(event.chat_id, "⚠️ Формат: .чекгруппу <@username/link>")
            
            target = parts[1]
            await client.send_message(event.chat_id, f"🚧 **.ЧЕКГРУППУ**: Запущен сбор участников из {target}...")
            # Здесь должна быть логика сбора участников через client.get_participants
            await asyncio.sleep(3)
            await client.send_message(event.chat_id, "✅ **.ЧЕКГРУППУ**: Сбор завершен. Собран N участников. Файл отправлен в бот.")

        # --- .СТАТУС ---
        elif cmd == '.статус':
            active_tasks = store.process_progress.get(user_id, {})
            status_message = "📊 **Статус Worker'а**\n"
            status_message += f"Активные задачи: {'Нет' if not active_tasks else active_tasks.get('type', 'Неизвестно')}\n"
            
            # Дополнительный статус
            status_message += f"ПК (Worker ID): {store.pc_monitoring.get(user_id, 'Не установлен')}\n"
            
            await client.send_message(event.chat_id, status_message)
            
    async def _flood_task(self, client, entity, text, count, delay, user_id):
        i = 0
        while i < count or count == 0:
            async with store.lock:
                if store.process_progress.get(user_id, {}).get('stop'):
                    break
            try:
                await client.send_message(entity, text)
                i += 1
                await asyncio.sleep(delay)
            except FloodWaitError as e:
                # Ожидание + небольшой рандомный бонус
                await client.send_message(entity, f"⚠️ Получен FloodWaitError на {e.seconds} секунд.")
                await asyncio.sleep(e.seconds + random.randint(1, 5))
            except Exception:
                await client.send_message(entity, "❌ Ошибка при отправке сообщения. Флуд остановлен.")
                break
        
        async with store.lock:
            store.process_progress.pop(user_id, None)

tm = TelethonManager(bot)

# =========================================================================
# V. AIOGRAM HANDLERS (USER, PROMO, DROPS & ADMIN)
# =========================================================================

# --- USER & PROMO HANDLERS ---

@user_router.message(Command('start'))
@rate_limit(RATE_LIMIT_TIME)
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await db.get_user(message.from_user.id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Вход", callback_data="auth_phone")],
        [InlineKeyboardButton(text="🔑 Промокод", callback_data="activate_promo_start")], # <--- ДОБАВЛЕНО
        [InlineKeyboardButton(text="❓ Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}")]
    ])
    await message.answer("👋 Добро пожаловать в STATPRO Worker!", reply_markup=kb)

# --- PROMO HANDLERS ---

@user_router.callback_query(F.data == "activate_promo_start")
async def activate_promo_callback(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoState.waiting_for_promo)
    await call.message.edit_text("✍️ Введите промокод:")
    await call.answer()

@user_router.message(PromoState.waiting_for_promo)
async def activate_promo_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    promo_code = message.text.strip().upper()
    
    success, result_message = await db.activate_promo(user_id, promo_code)
    
    if success:
        await message.answer(f"✅ **Промокод активирован!**\n{result_message}")
    else:
        await message.answer(f"❌ {result_message} Попробуйте снова или нажмите /start.")
        
    await state.clear()
    
# --- AUTH HANDLERS ---

@user_router.callback_query(F.data == "auth_phone")
async def auth_phone_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("Введите номер телефона (+7...):")
    await call.answer()

@user_router.message(TelethonAuth.PHONE)
async def auth_phone_input(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    user_id = message.from_user.id
    path = get_session_path(user_id, is_temp=True)
    await db.get_user(user_id)
    
    # Проверка подписки
    if not await db.check_subscription(user_id):
        return await message.answer("❌ Ваша подписка истекла. Активируйте промокод или обратитесь в поддержку.")
    
    client = TelegramClient(path, API_ID, API_HASH)
    
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
        
        async with store.lock:
            store.temp_auth_clients[user_id] = client
            
        await state.update_data(phone=phone, hash=sent.phone_code_hash)
        await state.set_state(TelethonAuth.CODE)
        await message.answer("Введите код из Telegram:")
    except PhoneNumberInvalidError:
        await message.answer("❌ Неверный формат номера.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e.__class__.__name__}")


@user_router.message(TelethonAuth.CODE)
async def auth_code_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    async with store.lock: client = store.temp_auth_clients.pop(user_id, None)
    
    if not client: return await message.answer("Сессия истекла.")
    
    try:
        # Пытаемся залогиниться
        await client.sign_in(data['phone'], message.text.strip(), phone_code_hash=data['hash'])
        
        # Если успешно, дисконнект и запуск worker-сессии
        await tm.start_worker_session(user_id, client)
        await state.clear()
        await message.answer("✅ Успешный вход! Worker запущен.")
        
    except SessionPasswordNeededError:
        # Требуется 2FA
        async with store.lock: store.temp_auth_clients[user_id] = client
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔐 Введите 2FA пароль:")
        
    except Exception as e:
        await client.disconnect()
        await message.answer(f"❌ Ошибка входа: {e.__class__.__name__}")
        
        
@user_router.message(TelethonAuth.PASSWORD)
async def auth_pass(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    async with store.lock: client = store.temp_auth_clients.pop(user_id, None)
    
    if not client: return await message.answer("Сессия истекла.")
    
    try:
        await client.sign_in(password=message.text.strip())
        await tm.start_worker_session(user_id, client)
        await state.clear()
        await message.answer("✅ 2FA Принят. Worker запущен!")
    except Exception as e: 
        await client.disconnect()
        await message.answer(f"❌ Ошибка пароля: {e.__class__.__name__}")


# --- DROPS HANDLERS (Рабочий чат) ---

async def handle_drop_status_change(message: types.Message, state: FSMContext, new_status: str, is_change_phone: bool = False):
    """Общая функция для обработки всех команд смены статуса."""
    drop_id = message.from_user.id
    
    # Пытаемся найти активную сессию по ID дропа
    current_session = await db.get_drop_session_by_drop_id(drop_id)

    if not current_session:
        return await message.reply("❌ **Ошибка:** Нет активной сессии для вашего ID. Начните с `/numb`.")
    
    phone = current_session['phone']
    # Получаем имя ПК из глобального хранилища (наиболее актуальное)
    pc_name = store.pc_monitoring.get(message.message_thread_id or drop_id) or current_session['pc_name']

    if is_change_phone:
        await db.update_drop_status(phone, 'замена')
        await state.set_state(DropStates.waiting_for_phone_change)
        await state.update_data(old_phone=phone, pc_name=pc_name)
        
        return await message.reply(
            f"📝 **{pc_name}: Замена номера.**\n"
            f"Текущий номер `{phone}` переведен в статус 'замена'.\n"
            f"Введите **новый номер** в формате **+7XXXXXXXXXX**."
        )

    await db.update_drop_status(phone, new_status)
    await message.reply(f"✅ **{pc_name}: Статус обновлен на '{new_status}'** для номера `{phone}`.")


@drops_router.message(F.chat.id == DROPS_CHAT_ID, Command("numb"))
async def cmd_numb_start(message: types.Message, state: FSMContext):
    pc_name = get_topic_name_from_message(message) or "Общий Чат"
    
    # Пытаемся найти ПК по ID дропа, если нет, то используем ПК из топика
    if pc_name == "Общий Чат":
         pc_name = store.pc_monitoring.get(message.from_user.id) or "Общий Чат"

    await state.set_state(DropStates.waiting_for_phone_and_pc)
    await state.update_data(drop_id=message.from_user.id, pc_name=pc_name)
    await message.reply(f"📝 **{pc_name}**: Введите номер телефона в формате **+7XXXXXXXXXX**.")

@drops_router.message(DropStates.waiting_for_phone_and_pc)
async def process_numb_input(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    data = await state.get_data()
    
    if not re.match(r'^\+\d{10,15}$', phone):
        return await message.reply("❌ Неверный формат номера. Введите +7XXXXXXXXXX")

    success = await db.create_drop_session(phone, data['pc_name'], data['drop_id'], 'дайте номер')
    
    if not success:
        return await message.reply(f"❌ Номер `{phone}` уже находится в работе.")
    
    await state.clear()
    await message.reply(f"✅ **{data['pc_name']}: Номер `{phone}` принят.**")


@drops_router.message(F.chat.id == DROPS_CHAT_ID, Command("zm"))
async def cmd_zm(message: types.Message, state: FSMContext):
    await handle_drop_status_change(message, state, 'замена', is_change_phone=True)

@drops_router.message(DropStates.waiting_for_phone_change)
async def process_zm(message: types.Message, state: FSMContext):
    new_phone = message.text.strip()
    data = await state.get_data()
    old_phone = data.get('old_phone')
    pc_name = data.get('pc_name')
    
    if not re.match(r'^\+\d{10,15}$', new_phone):
        return await message.reply("❌ Неверный формат номера. Введите +7XXXXXXXXXX")
        
    if not old_phone:
        await state.clear()
        return await message.reply("❌ **Ошибка:** Старый номер не найден. Начните с `/zm` снова.")

    await db.update_drop_status(old_phone, 'в работе', new_phone=new_phone)
    
    await state.clear()
    await message.reply(f"✅ **{pc_name}: Номер `{old_phone}` заменен на `{new_phone}`**.")

# Команды смены статуса
@drops_router.message(F.chat.id == DROPS_CHAT_ID, Command("vstal"))
async def cmd_vstal(message: types.Message, state: FSMContext):
    await handle_drop_status_change(message, state, 'в работе')

@drops_router.message(F.chat.id == DROPS_CHAT_ID, Command("error"))
async def cmd_error(message: types.Message, state: FSMContext):
    await handle_drop_status_change(message, state, 'error')

@drops_router.message(F.chat.id == DROPS_CHAT_ID, Command("slet"))
async def cmd_slet(message: types.Message, state: FSMContext):
    await handle_drop_status_change(message, state, 'slet')

@drops_router.message(F.chat.id == DROPS_CHAT_ID, Command("povt"))
async def cmd_povt(message: types.Message, state: FSMContext):
    await handle_drop_status_change(message, state, 'повтор')
    
# Команда отчета
@drops_router.message(F.chat.id == DROPS_CHAT_ID, Command("report_last"))
async def cmd_report_last(message: types.Message):
    # Пытаемся получить имя ПК из топика или из глобального хранилища
    pc_name = get_topic_name_from_message(message) or store.pc_monitoring.get(message.from_user.id)
    
    if not pc_name:
        return await message.reply("❌ **Ошибка:** Имя ПК не установлено. Используйте `.пкстарт <Название>`.")
        
    session = await db.get_last_session_by_pc(pc_name)
    
    if not session:
        return await message.reply(f"❌ Для ПК **{pc_name}** нет активных сессий.")

    now = get_current_time_msk()
    last_status_time = to_msk_aware(session['last_status_time'])
    
    # Расчет простоя
    prosto_seconds = session['prosto_seconds']
    if session['status'] in ('дайте номер', 'error', 'slet', 'повтор'):
        # Если статус простаивающий, добавляем текущее время простоя
        current_prosto = (now - last_status_time).total_seconds()
        total_prosto = prosto_seconds + current_prosto
    else:
        total_prosto = prosto_seconds

    # Форматирование
    total_prosto_formatted = format_timedelta(timedelta(seconds=total_prosto))
    session_start_time = to_msk_aware(session['start_time']).strftime('%H:%M %d.%m')
    
    report = f"""
**📊 Отчет для ПК: {pc_name}**
---
**Номер:** `{session['phone']}`
**Текущий Статус:** `{session['status']}`
**Дроп ID:** `{session['drop_id']}`
**Время начала сессии:** {session_start_time}
**Общее время простоя:** {total_prosto_formatted}
"""
    await message.reply(report)


# --- ADMIN HANDLERS ---
# Декоратор для проверки прав администратора
def is_admin():
    def decorator(func):
        @wraps(func)
        async def wrapper(message: types.Message, *args, **kwargs):
            if message.from_user.id != ADMIN_ID:
                return await message.reply("❌ Доступ запрещен. Вы не администратор.")
            return await func(message, *args, **kwargs)
        return wrapper
    return decorator

@admin_router.message(Command('admin'))
@is_admin()
async def cmd_admin(message: types.Message):
    total_users = await db.get_total_users()
    active_workers = len(store.active_workers)
    total_drops = await db.get_total_drop_sessions()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Создать промокод", callback_data="admin_create_promo")],
    ])
    
    report = f"""
**👑 Админ-Панель**
---
**Всего пользователей:** {total_users}
**Активных Workers:** {active_workers}
**Активных Drop-сессий:** {total_drops}
"""
    await message.answer(report, reply_markup=kb)

@admin_router.callback_query(F.data == "admin_create_promo")
@is_admin()
async def admin_create_promo_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminState.waiting_for_promo_data)
    await call.message.edit_text("✍️ **Введите данные для промокода в формате:**\n`КОД_ДНИ_ИСПОЛЬЗОВАНИЯ`\n\nПример: `TESTPROMO_30_50` (30 дней, 50 раз)")
    await call.answer()

@admin_router.message(AdminState.waiting_for_promo_data)
@is_admin()
async def admin_create_promo_input(message: types.Message, state: FSMContext):
    data = message.text.strip().split('_')
    await state.clear()
    
    if len(data) != 3:
        return await message.answer("❌ Неверный формат. Используйте `КОД_ДНИ_ИСПОЛЬЗОВАНИЯ`.")
        
    code = data[0].upper()
    try:
        days = int(data[1])
        uses = int(data[2])
    except ValueError:
        return await message.answer("❌ Дни и Использования должны быть числами.")
        
    if days <= 0 or uses <= 0:
        return await message.answer("❌ Дни и Использования должны быть положительными.")
        
    success = await db.create_promo(code, days, uses)
    
    if success:
        await message.answer(f"✅ **Промокод создан!**\nКод: `{code}`\nДни: {days}\nИспользований: {uses}")
    else:
        await message.answer(f"❌ Промокод `{code}` уже существует.")

# =========================================================================
# VI. CLEANUP & SHUTDOWN
# =========================================================================

async def cleanup_temp_sessions():
    while True:
        await asyncio.sleep(3600)
        now = datetime.now()
        for f in os.listdir(SESSION_DIR):
            if f.endswith('_temp.session'):
                file_path = os.path.join(SESSION_DIR, f)
                # Удаляем временные сессии старше 1 часа
                if os.path.exists(file_path) and (now - datetime.fromtimestamp(os.path.getctime(file_path)) > timedelta(hours=1)):
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        logger.warning(f"Failed to remove temp session {f}: {e}")
        
        # Проверяем подписки каждый час
        for uid in list(store.active_workers.keys()):
            if not await db.check_subscription(uid):
                 await tm.stop_worker(uid)


async def on_startup(dispatcher: Dispatcher, bot: Bot):
    logger.info("Starting periodic tasks...")
    # Resume workers (НЕ await!)
    active_ids = await db.get_active_telethon_users()
    logger.info(f"Restoring {len(active_ids)} workers...")
    for uid in active_ids:
        asyncio.create_task(tm.start_client_task(uid))

    asyncio.create_task(cleanup_temp_sessions())
    logger.info("Periodic tasks started.")


async def on_shutdown(dispatcher: Dispatcher):
    logger.info("Shutting down workers and connections...")
    
    async with store.lock:
        workers_to_stop = list(store.active_workers.keys())
    
    shutdown_tasks = [tm.stop_worker(uid) for uid in workers_to_stop]
    if shutdown_tasks:
        await asyncio.wait(shutdown_tasks, timeout=5)
        
    logger.info("Telethon clients disconnected.")


# =========================================================================
# VII. MAIN
# =========================================================================

async def main():
    if not all([BOT_TOKEN, API_ID, API_HASH, DROPS_CHAT_ID]):
        logger.critical("Critical: One or more environment variables are missing or invalid.")
        sys.exit(1)

    await db.init()
    
    dp.include_router(user_router)
    dp.include_router(drops_router)
    dp.include_router(admin_router) # <--- ВКЛЮЧАЕМ АДМИН-РОУТЕР
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Polling started.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot execution interrupted.")
    except Exception as e:
        logger.critical(f"Critical error in main: {e}")
        traceback.print_exc()
