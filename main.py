import asyncio
import logging
import os
import re
import random
import string
import sys
import traceback
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Union, Any, Callable, Awaitable
from functools import wraps
from dotenv import load_dotenv

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage 
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, Update
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.dispatcher.middlewares.base import BaseMiddleware 

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, AuthKeyUnregisteredError, UserIsBlockedError, PasswordHashInvalidError, RpcCallFailError, SessionRevokedError, UserDeactivatedBanError

# --- OTHER ---
import aiosqlite
import pytz

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# =========================================================================

# --- КЛЮЧИ И КОНСТАНТЫ (ПРОВЕРЬТЕ И ЗАМЕНИТЕ DROPS_CHAT_ID) ---
BOT_TOKEN = "7868097991:AAHIHM32o9MeluAeWgBwC9WKHydiedWUrQY" 
ADMIN_ID = 6256576302                                        
API_ID = 29930612                                            
API_HASH = "2690aa8c364b91e47b6da1f90a71f825"                
DROPS_CHAT_ID = -100 # !!! ЗАМЕНИТЕ ЭТО ЗНАЧЕНИЕ НА РЕАЛЬНЫЙ ID ЧАТА !!!

# Прочие настройки
SUPPORT_BOT_USERNAME = "suppor_tstatpro1bot"
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 1.0 
SESSION_DIR = 'sessions'

if not os.path.exists(SESSION_DIR): os.makedirs(SESSION_DIR)
if not os.path.exists('data'): os.makedirs('data')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

storage = MemoryStorage() 
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='Markdown'))
dp = Dispatcher(storage=storage)
user_router = Router()
drops_router = Router()
admin_router = Router()

# =========================================================================
# II. ГЛОБАЛЬНЫЙ ERROR HANDLER
# =========================================================================

@dp.errors()
async def global_error_handler(event: Update, exception: Exception):
    """Глобальный обработчик ВСЕХ ошибок"""
    logger.critical(f"🚨 КРИТИЧЕСКАЯ ОШИБКА: {exception.__class__.__name__}: {exception}", exc_info=True)
    
    if ADMIN_ID:
        # Улучшенное отображение Traceback
        error_msg = (
            f"🔥 **BOT CRASH** 🔥\n"
            f"❌ Тип: `{exception.__class__.__name__}`\n"
            f"📄 Update ID: `{event.update_id}`\n"
            f"📍 Трейсбек:\n`{traceback.format_exc()[:1500]}`"
        )
        try:
            await bot.send_message(ADMIN_ID, error_msg, parse_mode='Markdown')
        except:
            pass
            
    return True

# =========================================================================
# III. ГЛОБАЛЬНЫЕ ХРАНИЛИЩА И FSM СОСТОЯНИЯ
# =========================================================================

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {} 
        self.process_progress: Dict[int, Dict] = {}
        self.pc_monitoring: Dict[Union[int, str], str] = {}
        self.active_workers: Dict[int, TelegramClient] = {} 
        self.worker_tasks: Dict[int, List[asyncio.Task]] = {} 

store = GlobalStorage()

class TelethonAuth(StatesGroup):
    PHONE = State() 
    CODE = State()  
    PASSWORD = State() 
    QR_WAIT = State()  
    
class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_phone_change = State()

class PromoStates(StatesGroup):
    waiting_for_code = State()

class AdminStates(StatesGroup):
    waiting_for_promo_length = State()
    waiting_for_promo_days = State()
    waiting_for_promo_uses = State()
    waiting_for_user_id_for_sub = State() 
    waiting_for_sub_days = State()
    
# =========================================================================
# IV. ASYNC DATABASE (ПОЛНЫЙ КОД)
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        self.TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

    def get_current_time_msk(self) -> datetime:
        return datetime.now(self.TIMEZONE_MSK)

    def to_msk_aware(self, dt_str: str) -> datetime:
        naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        return self.TIMEZONE_MSK.localize(naive_dt)

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
            await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    days INTEGER NOT NULL,
                    uses_left INTEGER NOT NULL,
                    created_at TEXT NOT NULL
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
            await db.commit()
        logger.info("Database initialized successfully.")

    async def get_user(self, user_id):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            await db.commit()
            async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def check_subscription(self, user_id):
        global ADMIN_ID
        global tm 
        
        if user_id == ADMIN_ID: return True
        user = await self.get_user(user_id)
        if not user or not user['subscription_active']: return False
        
        end_date_str = user['subscription_end_date']
        if not end_date_str: return False

        try:
            end = self.to_msk_aware(end_date_str)
            now = self.get_current_time_msk()
            if end > now:
                return True
            else:
                await self.set_telethon_status(user_id, False)
                await self.set_subscription_status(user_id, False, None)
                if 'tm' in globals():
                    await tm.stop_worker(user_id)
                return False
        except Exception:
            return False

    async def set_telethon_status(self, user_id, status):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            await db.commit()
            
    async def set_subscription_status(self, user_id, status, end_date_str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET subscription_active=?, subscription_end_date=? WHERE user_id=?", (1 if status else 0, end_date_str, user_id))
            await db.commit()
            
    async def get_active_telethon_users(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def create_promo_code(self, code: str, days: int, uses: int):
        async with aiosqlite.connect(self.db_path) as db:
            now_str = self.get_current_time_msk().strftime('%Y-%m-%d %H:%M:%S')
            try:
                await db.execute(
                    "INSERT INTO promo_codes (code, days, uses_left, created_at) VALUES (?, ?, ?, ?)",
                    (code.upper(), days, uses, now_str)
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False 

    async def get_promo_code(self, code: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM promo_codes WHERE code=?", (code.upper(),)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def activate_promo_code(self, user_id: int, code: str) -> bool:
        promo = await self.get_promo_code(code)
        if not promo or promo['uses_left'] <= 0:
            return False

        user = await self.get_user(user_id)
        current_end_date_str = user.get('subscription_end_date')
        
        now = self.get_current_time_msk()

        is_active = await self.check_subscription(user_id)
        if is_active and current_end_date_str:
            start_date = self.to_msk_aware(current_end_date_str)
        else:
            start_date = now

        new_end_date = start_date + timedelta(days=promo['days'])
        new_end_date_str = new_end_date.strftime('%Y-%m-%d %H:%M:%S')

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET subscription_active=1, subscription_end_date=? WHERE user_id=?",
                (new_end_date_str, user_id)
            )
            await db.execute(
                "UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code=?",
                (code.upper(),)
            )
            await db.commit()
        
        return True
        
    async def get_all_promo_codes(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM promo_codes ORDER BY created_at DESC") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def delete_promo_code(self, code: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM promo_codes WHERE code=?", (code.upper(),))
            await db.commit()
            return db.total_changes > 0

    async def get_all_users_count(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(user_id) FROM users") as cursor:
                return (await cursor.fetchone())[0]

    async def get_active_subs_count(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(user_id) FROM users WHERE subscription_active=1") as cursor:
                return (await cursor.fetchone())[0]

    async def get_active_drops_count(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(phone) FROM drop_sessions WHERE status NOT IN ('closed', 'deleted', 'замена_закрыт')") as cursor:
                return (await cursor.fetchone())[0]

    async def get_all_drops(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions ORDER BY start_time DESC") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def update_drop_status(self, phone: str, status: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE drop_sessions SET status=? WHERE phone=?", (status, phone))
            await db.commit()
            
    async def delete_drop_session(self, phone: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM drop_sessions WHERE phone=?", (phone,))
            await db.commit()
            return db.total_changes > 0


db = AsyncDatabase(os.path.join('data', DB_NAME))

# =========================================================================
# V. RATE LIMIT MIDDLEWARE (ИСПРАВЛЕН)
# =========================================================================

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, limit: float = RATE_LIMIT_TIME):
        self.limit = limit
        self.last_user_request: Dict[int, datetime] = {} 
        self.lock = asyncio.Lock()
        super().__init__()

    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any] # <-- ИСПРАВЛЕННЫЙ СИНТАКСИС
    ) -> Any:
        
        user_id = (event.message.from_user.id if event.message
                   else event.callback_query.from_user.id if event.callback_query 
                   else None)

        if not user_id:
            return await handler(event, data)

        now = db.get_current_time_msk()
        
        async with self.lock:
            last = self.last_user_request.get(user_id)
            
            if last and (now - last).total_seconds() < self.limit:
                return 
                
            self.last_user_request[user_id] = now
        
        return await handler(event, data)

# =========================================================================
# VI. TELETHON MANAGER И USER HANDLERS (ВСТАВИТЬ ПОЛНОСТЬЮ)
# =========================================================================

def generate_promo_code(length=8):
    """Генерирует случайную буквенно-цифровую строку."""
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

# --- ВСТАВЬТЕ СЮДА ВЕСЬ КОД КЛАССА TelethonManager и ВСЕХ ХЭНДЛЕРОВ ---

class TelethonManager:
    # ... (Весь код TelethonManager)
    def __init__(self, bot_instance: Bot):
        self.bot = bot_instance
    
    async def _send_to_bot_user(self, user_id, message):
        try:
            await self.bot.send_message(user_id, message, disable_notification=False)
        except (TelegramForbiddenError, TelegramBadRequest, UserIsBlockedError):
            logger.error(f"Cannot send message to {user_id}. Stopping worker.")
            await self.stop_worker(user_id)
        except Exception as e:
            logger.error(f"Unknown error sending message to {user_id}: {e}")

    async def start_client_task(self, user_id):
        if not await db.check_subscription(user_id):
            await self._send_to_bot_user(user_id, "⚠️ **Ваша подписка истекла.** Worker не запущен.")
            return

        await self.stop_worker(user_id)
        
        task = asyncio.create_task(self._run_worker(user_id))
        
        async with store.lock:
            store.worker_tasks.setdefault(user_id, []).append(task)
            
        return task

    async def _run_worker(self, user_id):
        path = os.path.join(SESSION_DIR, f'session_{user_id}')
        client = TelegramClient(path, API_ID, API_HASH, device_model="StatPro Worker", flood_sleep_threshold=15)
        
        async with store.lock:
            store.active_workers[user_id] = client

        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            await self.worker_message_handler(user_id, client, event)

        try:
            await client.start(phone=None) 
            await db.set_telethon_status(user_id, True)
            await self._send_to_bot_user(user_id, "🚀 Worker успешно запущен и готов к работе!")
            await client.run_until_disconnected()

        except (AuthKeyUnregisteredError, SessionPasswordNeededError, PhoneNumberInvalidError, EOFError, SessionRevokedError, UserDeactivatedBanError):
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Требуется повторная авторизация через меню **Профиль -> Управление Worker**.")
            
            session_file = os.path.join(SESSION_DIR, f'session_{user_id}.session')
            if os.path.exists(session_file):
                try:
                    await asyncio.to_thread(os.remove, session_file)
                    logger.info(f"Removed invalid session file for {user_id}.")
                except Exception as e:
                    logger.warning(f"Failed to remove session file {session_file}: {e}")
                    
            await self.stop_worker(user_id)
        except Exception as e:
            logger.error(f"Worker {user_id} failed: {e}", exc_info=True)
            await self._send_to_bot_user(user_id, f"💔 Worker отключился из-за критической ошибки: `{e.__class__.__name__}`.")
        finally:
            if user_id in store.active_workers:
                 await self.stop_worker(user_id)
            await db.set_telethon_status(user_id, False)

    async def stop_worker(self, user_id):
        async with store.lock:
            client = store.active_workers.pop(user_id, None)
            
            tasks = store.worker_tasks.pop(user_id, [])
            for t in tasks:
                if not t.done(): t.cancel()

        if client:
            try:
                await client.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting client {user_id}: {e}")

        await db.set_telethon_status(user_id, False)

    async def worker_message_handler(self, user_id, client, event):
        if not event.text or not event.text.startswith('.'): return

        msg = event.text.strip()
        parts = msg.split()
        cmd = parts[0].lower()
        chat = event.chat_id

        try: await event.delete() 
        except: pass 

        if cmd == '.флуд':
            try:
                if len(parts) < 3: 
                    return await client.send_message(chat, "⚠️ Используйте: `.флуд [кол-во] [текст] [задержка]`", reply_to=event.message.id)
                
                count = int(parts[1])
                delay_str = parts[-1]
                
                if delay_str.replace('.', '', 1).isdigit():
                    delay = max(0.5, float(delay_str)) 
                    text = " ".join(parts[2:-1])
                else:
                    delay = 0.5
                    text = " ".join(parts[2:])
                
                if count > 1000 and count != 0: 
                    return await client.send_message(chat, "❌ Максимальное количество сообщений 1000.")

                async with store.lock:
                    if store.process_progress.get(user_id, {}).get('type') == 'flood':
                        return await client.send_message(chat, "⚠️ Уже запущен активный флуд. Сначала остановите: `.стопфлуд`")

                    store.process_progress[user_id] = {'type': 'flood', 'stop': False}
                
                task = asyncio.create_task(self._flood_task(client, chat, text, count, delay, user_id))
                async with store.lock:
                    store.worker_tasks.setdefault(user_id, []).append(task)

                temp = await client.send_message(chat, f"🚀 Флуд запущен: {count} сообщений, задержка {delay}с. Для остановки: `.стопфлуд`")
                await asyncio.sleep(2)
                await temp.delete()
            except Exception as e:
                await client.send_message(chat, f"❌ Ошибка флуда: `{e.__class__.__name__}`")
        
        elif cmd == '.стопфлуд':
            async with store.lock:
                if store.process_progress.get(user_id, {}).get('type') == 'flood':
                    store.process_progress[user_id]['stop'] = True
                    temp = await client.send_message(chat, "🛑 Флуд остановлен.")
                    await asyncio.sleep(2)
                    await temp.delete()
                else:
                    temp = await client.send_message(chat, "⚠️ Активный флуд не найден.")
                    await asyncio.sleep(2)
                    await temp.delete()

    async def _flood_task(self, client, chat, text, count, delay, user_id):
        i = 0
        while i < count or count == 0:
            async with store.lock:
                if store.process_progress.get(user_id, {}).get('stop'): break
            try:
                await client.send_message(chat, text)
                i += 1
                await asyncio.sleep(delay)
            except FloodWaitError as e:
                logger.warning(f"FloodWait on {user_id}: {e.seconds}s. Sleeping...")
                await asyncio.sleep(e.seconds + random.randint(1, 5))
            except Exception:
                break
        
        async with store.lock:
            store.process_progress.pop(user_id, None)

tm = TelethonManager(bot) 

# --- Главное меню (/start) ---
@user_router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    await db.get_user(user_id) 
    
    is_subscribed = await db.check_subscription(user_id)

    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="👤 Профиль", callback_data="profile_menu"))
    
    if is_subscribed:
        builder.row(InlineKeyboardButton(text="⚙️ Управление Worker", callback_data="worker_menu"))
    
    if not is_subscribed:
        builder.row(InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="enter_promo"))
        
    builder.row(InlineKeyboardButton(text="❓ Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}"))

    if user_id == ADMIN_ID:
        builder.row(InlineKeyboardButton(text="📊 Админ-панель", callback_data="admin_stats"))

    text = "👋 Добро пожаловать в систему STATPRO. Используйте меню для навигации."

    await message.answer(text, reply_markup=builder.as_markup())

# --- Вкладка "Профиль" ---
@user_router.callback_query(F.data.in_({"profile_menu", "start_menu"}))
async def profile_menu(call: Union[types.CallbackQuery, types.Message], state: FSMContext):
    
    if isinstance(call, types.CallbackQuery):
        user_id = call.from_user.id
        message_to_edit = call.message
        await call.answer()
        if call.data == "start_menu":
            await cmd_start(message_to_edit, state)
            return 
    else:
        user_id = call.from_user.id
        message_to_edit = call

        
    user_data = await db.get_user(user_id)
    is_subscribed = await db.check_subscription(user_id)
    is_worker_active = user_data.get('telethon_active', False)
    
    end_date_str = user_data.get('subscription_end_date')
    end_date_info = db.to_msk_aware(end_date_str).strftime('%d.%m.%Y %H:%M MSK') if is_subscribed and end_date_str else "Не активна"
    
    text = (
        f"👤 **Ваш Профиль**\n\n"
        f"🔹 **Ваш Telegram ID:** `{user_id}`\n"
        f"✅ **Подписка активна:** {'Да' if is_subscribed else 'Нет'}\n"
        f"🗓️ **Действует до:** `{end_date_info}`\n"
        f"🚀 **Worker активен:** {'Да' if is_worker_active else 'Нет'}"
    )
    
    builder = InlineKeyboardBuilder()
    if is_subscribed:
         builder.row(InlineKeyboardButton(text="⚙️ Управление Worker", callback_data="worker_menu"))
    else:
        builder.row(InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="enter_promo"))
        
    builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="start_menu"))
    
    if isinstance(call, types.CallbackQuery):
        await message_to_edit.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message_to_edit.answer(text, reply_markup=builder.as_markup())


# --- Управление Worker ---
@user_router.callback_query(F.data == "worker_menu")
async def worker_menu(call: Union[types.CallbackQuery, types.Message], state: FSMContext):
    if isinstance(call, types.CallbackQuery):
        user_id = call.from_user.id
        message_to_edit = call.message
        await call.answer()
    else:
        user_id = call.from_user.id
        message_to_edit = call
        
    is_subscribed = await db.check_subscription(user_id)
    
    if not is_subscribed:
        if isinstance(call, types.CallbackQuery):
             await call.answer("❌ Ваша подписка истекла.", show_alert=True)
             return await profile_menu(call, state)
        return await profile_menu(call, state)


    user_data = await db.get_user(user_id)
    is_worker_active = user_data.get('telethon_active', False)
    
    builder = InlineKeyboardBuilder()
    
    session_exists = os.path.exists(os.path.join(SESSION_DIR, f'session_{user_id}.session'))

    if is_worker_active:
        builder.row(InlineKeyboardButton(text="🛑 Остановить Worker", callback_data="stop_worker"))
    elif session_exists:
        builder.row(InlineKeyboardButton(text="▶️ Запустить Worker", callback_data="start_worker"))
        
    if not is_worker_active or not session_exists:
        builder.row(InlineKeyboardButton(text="🚪 Новый вход/Авторизация", callback_data="auth_method_menu"))
        
    builder.row(InlineKeyboardButton(text="🔙 Профиль", callback_data="profile_menu"))

    status_text = "✅ **Worker активен**." if is_worker_active else "❌ **Worker не активен**."
    
    text = f"⚙️ **Управление Worker**\n\n{status_text}\n\n*Для смены аккаунта используйте кнопку 'Новый вход/Авторизация'.*"
    
    if isinstance(call, types.CallbackQuery):
        await message_to_edit.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message_to_edit.answer(text, reply_markup=builder.as_markup())


# --- Выбор метода авторизации ---
@user_router.callback_query(F.data == "auth_method_menu")
async def auth_method_menu(call: types.CallbackQuery, state: FSMContext):
    await tm.stop_worker(call.from_user.id) 
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📲 По номеру телефона", callback_data="auth_by_phone"))
    builder.row(InlineKeyboardButton(text="📷 По QR-коду (Временно отключено)", callback_data="auth_by_qr_placeholder")) 
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data="worker_menu")) 
    
    await call.message.edit_text("🚪 **Выберите способ авторизации**:", reply_markup=builder.as_markup())
    await call.answer()
    
@user_router.callback_query(F.data == "auth_by_qr_placeholder")
async def auth_by_qr_placeholder(call: types.CallbackQuery):
    await call.answer("Функция входа по QR-коду находится в разработке.", show_alert=True)


# --- АВТОРИЗАЦИЯ ПО НОМЕРУ ТЕЛЕФОНА (НАЧАЛО) ---
@user_router.callback_query(F.data == "auth_by_phone")
async def auth_by_phone_step1(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("📲 Введите **номер телефона** (включая код страны, например, `+79xxxxxxxxx`):")
    await call.answer()

@user_router.message(TelethonAuth.PHONE)
async def auth_by_phone_step2_send_code(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    phone = message.text.strip()
    
    if not re.match(r'^\+\d{10,15}$', phone):
        return await message.answer("❌ Неверный формат номера. Используйте формат `+79xxxxxxxxx`:")
        
    path = os.path.join(SESSION_DIR, f'temp_{user_id}')
    client = TelegramClient(path, API_ID, API_HASH)
    
    if user_id in store.temp_auth_clients:
        try: await store.temp_auth_clients[user_id].disconnect()
        except: pass

    async with store.lock:
        store.temp_auth_clients[user_id] = client
        
    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        
        await state.update_data(phone=phone, sent_code=sent_code)
        await state.set_state(TelethonAuth.CODE)
        await message.answer(f"✅ Код подтверждения отправлен на номер **{phone}**.\nВведите полученный код:")

    except PhoneNumberInvalidError:
        await state.clear()
        await message.answer("❌ Неверный номер телефона. Начните заново: /start")
    except Exception as e:
        logger.error(f"Telethon Auth Error: {e}")
        await state.clear()
        await message.answer(f"❌ Критическая ошибка Telethon: `{e.__class__.__name__}`. Начните заново: /start")
    finally:
        if not client.is_connected():
            await client.disconnect()

@user_router.message(TelethonAuth.CODE)
async def auth_by_phone_step3_sign_in(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip()
    
    data = await state.get_data()
    phone = data.get('phone')
    sent_code = data.get('sent_code')

    client = store.temp_auth_clients.get(user_id)
    if not client:
        await state.clear()
        return await message.answer("❌ Сессия авторизации потеряна. Начните заново: /start")
        
    try:
        if not client.is_connected(): await client.connect()
        
        user_info = await client.sign_in(phone, code, password=None, phone_code_hash=sent_code.phone_code_hash)
        
        temp_path = os.path.join(SESSION_DIR, f'temp_{user_id}.session')
        final_path = os.path.join(SESSION_DIR, f'session_{user_id}.session')
        
        await asyncio.to_thread(os.rename, temp_path, final_path)
        
        await client.disconnect()
        async with store.lock:
            store.temp_auth_clients.pop(user_id, None)

        await state.clear()
        await message.answer(f"🎉 **Успешная авторизация!** Аккаунт **{user_info.first_name}** привязан.")
        
        await tm.start_client_task(user_id)
        
        await worker_menu(message, state)

    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("⚠️ **Требуется облачный пароль (2FA)**. Введите ваш пароль:")
        
    except RpcCallFailError as e:
        if 'phone_code_hash expired' in str(e):
             await state.clear()
             await message.answer("❌ Код просрочен. Начните авторизацию заново: /start")
        else:
             await message.answer("❌ Неверный код. Попробуйте снова:")

    except Exception as e:
        logger.error(f"Telethon Sign-in Error: {e}")
        await client.disconnect()
        async with store.lock:
            store.temp_auth_clients.pop(user_id, None)
        await state.clear()
        await message.answer(f"❌ Критическая ошибка: `{e.__class__.__name__}`. Начните заново: /start")

@user_router.message(TelethonAuth.PASSWORD)
async def auth_by_phone_step4_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    password = message.text.strip()
    
    data = await state.get_data()
    phone = data.get('phone')
    sent_code = data.get('sent_code')

    client = store.temp_auth_clients.get(user_id)
    if not client:
        await state.clear()
        return await message.answer("❌ Сессия авторизации потеряна. Начните заново: /start")
        
    try:
        if not client.is_connected(): await client.connect()
        
        user_info = await client.sign_in(phone, code=None, password=password, phone_code_hash=sent_code.phone_code_hash if sent_code else None) 
        
        temp_path = os.path.join(SESSION_DIR, f'temp_{user_id}.session')
        final_path = os.path.join(SESSION_DIR, f'session_{user_id}.session')
        
        await asyncio.to_thread(os.rename, temp_path, final_path)
        
        await state.clear()
        await message.answer(f"🎉 **Успешная авторизация!** Аккаунт **{user_info.first_name}** привязан.")
        
        await tm.start_client_task(user_id)
        await worker_menu(message, state)

    except PasswordHashInvalidError:
        await message.answer("❌ Неверный облачный пароль. Попробуйте снова:")
    except Exception as e:
        logger.error(f"Telethon Password Error: {e}")
        await state.clear()
        await message.answer(f"❌ Критическая ошибка: `{e.__class__.__name__}`. Начните заново: /start")
    finally:
        if client and client.is_connected(): await client.disconnect()
        async with store.lock:
            store.temp_auth_clients.pop(user_id, None)


# --- ВВОД ПРОМОКОДА ---
@user_router.callback_query(F.data == "enter_promo")
async def ask_for_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code) 
    await call.message.edit_text("🔑 Введите ваш промокод для активации подписки:")
    await call.answer()
    
@user_router.message(PromoStates.waiting_for_code)
async def process_promo_activation(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    user_id = message.from_user.id
    
    if not re.match(r'^[A-Z0-9]+$', code) or len(code) < 4:
        await message.answer("❌ Неверный формат промокода. Код должен содержать только буквы (A-Z) и цифры, длиной от 4 символов.")
        return

    success = await db.activate_promo_code(user_id, code)

    if success:
        await state.clear()
        
        user_data = await db.get_user(user_id)
        end_date_str = user_data.get('subscription_end_date', 'Неизвестно')
        end_date_msk = db.to_msk_aware(end_date_str).strftime('%d.%m.%Y %H:%M MSK')
        
        await message.answer(
            f"🎉 **Промокод '{code}' успешно активирован!**\n"
            f"Ваша подписка активна до: **{end_date_msk}**."
        )
        await profile_menu(message, state) 
    else:
        await message.answer("❌ **Промокод недействителен** (истек, использован или не найден). Попробуйте еще раз или вернитесь в главное меню /start.")

# --- Worker Controls ---
@user_router.callback_query(F.data == "start_worker")
async def start_worker_handler(call: types.CallbackQuery, state: FSMContext):
    await tm.start_client_task(call.from_user.id)
    await worker_menu(call, state) 
    await call.answer("🚀 Worker запускается...", show_alert=False)

@user_router.callback_query(F.data == "stop_worker")
async def stop_worker_handler(call: types.CallbackQuery, state: FSMContext):
    await tm.stop_worker(call.from_user.id)
    await worker_menu(call, state)
    await call.answer("🛑 Worker остановлен.", show_alert=False)

# =========================================================================
# VII. ADMIN PANEL
# =========================================================================

@admin_router.callback_query(F.data == "admin_stats")
async def admin_main_menu(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    await state.clear()
    
    total_users = await db.get_all_users_count()
    active_subs = await db.get_active_subs_count()
    active_drops = await db.get_active_drops_count()
    active_workers_count = len(store.active_workers)

    text = (
        "📊 **Админ-панель**\n\n"
        f"👤 **Всего пользователей в БД:** {total_users}\n"
        f"✅ **Активных подписок:** {active_subs}\n"
        f"🚀 **Активных Worker:** {active_workers_count}\n"
        f"💼 **Дропов в работе:** {active_drops}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔑 Управление промокодами", callback_data="admin_promo_menu"))
    builder.row(InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_give_sub"))
    builder.row(InlineKeyboardButton(text="🔍 Управление дропами", callback_data="admin_drops_menu"))
    builder.row(InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="start_menu"))
    
    await call.message.edit_text(text, reply_markup=builder.as_markup())
    await call.answer()
    
@admin_router.callback_query(F.data == "admin_promo_menu")
async def admin_promo_menu(call: Union[types.CallbackQuery, types.Message], state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    await state.clear()
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Создать промокод", callback_data="admin_create_promo"))
    builder.row(InlineKeyboardButton(text="📜 Показать все коды", callback_data="admin_view_promo"))
    builder.row(InlineKeyboardButton(text="🔙 В админ-меню", callback_data="admin_stats"))
    
    if isinstance(call, types.CallbackQuery):
        await call.message.edit_text("🔑 **Управление промокодами**", reply_markup=builder.as_markup())
        await call.answer()
    else:
        await call.answer("🔑 **Управление промокодами**", reply_markup=builder.as_markup())


# --- ГЕНЕРАЦИЯ ПРОМОКОДА ---

@admin_router.callback_query(F.data == "admin_create_promo")
async def admin_create_promo_step1(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    
    await state.set_state(AdminStates.waiting_for_promo_length)
    await call.message.edit_text("🔢 Введите **длину** промокода (от 4 до 16 символов):")
    await call.answer()

@admin_router.message(AdminStates.waiting_for_promo_length)
async def admin_create_promo_step2_generate(message: types.Message, state: FSMContext):
    try:
        length = int(message.text.strip())
        if not (4 <= length <= 16): raise ValueError
    except ValueError:
        return await message.answer("❌ Неверное число. Длина должна быть от 4 до 16 символов. Попробуйте снова:")
    
    code = None
    for _ in range(10): 
        generated_code = generate_promo_code(length)
        if not await db.get_promo_code(generated_code):
            code = generated_code
            break
    
    if not code:
        await state.clear()
        return await message.answer("❌ Не удалось сгенерировать уникальный промокод. Начните заново: /start")
        
    await state.update_data(new_promo_code=code)
    
    await state.set_state(AdminStates.waiting_for_promo_days)
    await message.answer(f"✅ Код сгенерирован: `{code}`.\n\n🗓️ Введите количество **дней** подписки (целое число):")

@admin_router.message(AdminStates.waiting_for_promo_days)
async def admin_create_promo_step3(message: types.Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        if days <= 0 or days > 3650: raise ValueError
    except ValueError:
        return await message.answer("❌ Неверное число. Введите целое число дней (от 1 до 3650):")
        
    await state.update_data(new_promo_days=days)
    
    await state.set_state(AdminStates.waiting_for_promo_uses)
    await message.answer("🔢 Введите количество **доступных использований** (целое число, 0 - бесконечно):")

@admin_router.message(AdminStates.waiting_for_promo_uses)
async def admin_create_promo_step4(message: types.Message, state: FSMContext):
    try:
        uses = int(message.text.strip())
        if uses < 0: raise ValueError
    except ValueError:
        return await message.answer("❌ Неверное число. Введите целое число использований (0 или больше):")

    data = await state.get_data()
    code = data['new_promo_code']
    days = data['new_promo_days']

    success = await db.create_promo_code(code, days, uses)
    await state.clear()
    
    if success:
        text = (
            f"🎉 **Промокод успешно создан!**\n"
            f"🔑 **Код для копирования:** `{code}`\n" 
            f"Длительность: **{days}** дней.\n"
            f"Использований: **{'Бесконечно' if uses == 0 else uses}**."
        )
        await message.answer(text) 
    else:
        await message.answer(f"❌ Ошибка. Промокод **'{code}'** уже существует (крайне маловероятно).")
        
    await admin_promo_menu(message, state) 

# --- УДАЛЕНИЕ ПРОМОКОДА И ПРОСМОТР ---

@admin_router.callback_query(F.data == "admin_view_promo")
async def admin_view_promo(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    
    promos = await db.get_all_promo_codes()
    
    if not promos:
        builder = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 В меню промокодов", callback_data="admin_promo_menu"))
        await call.message.edit_text("📜 **Промокоды:**\n\nНет активных промокодов.", reply_markup=builder.as_markup())
        return await call.answer()

    text = "📜 **Активные промокоды (последние 10)**:\n\n"
    
    for promo in promos[:10]:
        uses_str = '∞' if promo['uses_left'] == 0 else promo['uses_left']
        text += f"`{promo['code']}` ({promo['days']}д.) — Осталось: {uses_str}\n"

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🗑️ Удалить код", callback_data="admin_delete_promo_menu"))
    builder.row(InlineKeyboardButton(text="🔙 В меню промокодов", callback_data="admin_promo_menu"))
    
    await call.message.edit_text(text, reply_markup=builder.as_markup())
    await call.answer()

@admin_router.callback_query(F.data == "admin_delete_promo_menu")
async def admin_delete_promo_menu(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    
    promos = await db.get_all_promo_codes()
    builder = InlineKeyboardBuilder()

    if not promos:
        await call.answer("Нет промокодов для удаления.", show_alert=True)
        return await admin_promo_menu(call, state)

    for promo in promos[:10]:
        builder.row(InlineKeyboardButton(text=f"🗑️ {promo['code']}", callback_data=f"delete_{promo['code']}"))
        
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_view_promo"))
    
    await call.message.edit_text("⬇️ **Выберите код для удаления**:", reply_markup=builder.as_markup())
    await call.answer()
    
@admin_router.callback_query(F.data.startswith("delete_"))
async def admin_delete_promo(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    
    code_to_delete = call.data.split('_')[1]
    
    deleted = await db.delete_promo_code(code_to_delete)
    
    if deleted:
        await call.answer(f"✅ Код {code_to_delete} удален.", show_alert=True)
    else:
        await call.answer(f"❌ Ошибка удаления кода {code_to_delete}.", show_alert=True)
        
    await admin_delete_promo_menu(call, state) 

# --- ВЫДАЧА ПОДПИСКИ АДМИНОМ ---

@admin_router.callback_query(F.data == "admin_give_sub")
async def admin_give_sub_step1(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    await state.set_state(AdminStates.waiting_for_user_id_for_sub)
    await call.message.edit_text("👤 Введите **ID пользователя Telegram** для выдачи подписки:")
    await call.answer()

@admin_router.message(AdminStates.waiting_for_user_id_for_sub)
async def admin_give_sub_step2(message: types.Message, state: FSMContext):
    try:
        user_id_to_sub = int(message.text.strip())
        if user_id_to_sub <= 0: raise ValueError
    except ValueError:
        return await message.answer("❌ Неверный ID. Введите целое положительное число (ID пользователя):")

    await state.update_data(target_user_id=user_id_to_sub)
    await state.set_state(AdminStates.waiting_for_sub_days)
    await message.answer(f"🗓️ Введите количество **дней** подписки для ID `{user_id_to_sub}`:")

@admin_router.message(AdminStates.waiting_for_sub_days)
async def admin_give_sub_step3(message: types.Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        if days <= 0 or days > 3650: raise ValueError
    except ValueError:
        return await message.answer("❌ Неверное число. Введите целое число дней (от 1 до 3650):")

    data = await state.get_data()
    user_id_to_sub = data['target_user_id']
    await state.clear()
    
    user_data = await db.get_user(user_id_to_sub)
    current_end_date_str = user_data.get('subscription_end_date')
    
    now = db.get_current_time_msk()

    is_active = await db.check_subscription(user_id_to_sub)
    if is_active and current_end_date_str:
        start_date = db.to_msk_aware(current_end_date_str)
    else:
        start_date = now

    new_end_date = start_date + timedelta(days=days)
    new_end_date_str = new_end_date.strftime('%Y-%m-%d %H:%M:%S')

    await db.set_subscription_status(user_id_to_sub, True, new_end_date_str)
    
    await message.answer(
        f"✅ **Подписка успешно выдана** пользователю `{user_id_to_sub}` на **{days} дней**.\n"
        f"Новая дата окончания: **{new_end_date.strftime('%d.%m.%Y %H:%M MSK')}**."
    )
    
    try:
        # Улучшенная обратная связь пользователю
        await bot.send_message(user_id_to_sub, f"🎉 **Администратор продлил Вашу подписку!** На {days} дней. Проверьте статус в разделе Профиль.")
    except Exception:
        logger.warning(f"Failed to notify user {user_id_to_sub} about sub extension.")
        
    await admin_main_menu(message, state)

# --- DROPS HANDLERS (Заглушки) ---

@admin_router.callback_query(F.data == "admin_drops_menu")
async def admin_drops_menu(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    
    all_drops = await db.get_all_drops()
    
    if not all_drops:
        text = "💼 **Управление дропами**\n\nНет активных или недавних сессий дропов."
        builder = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 В админ-меню", callback_data="admin_stats"))
        await call.message.edit_text(text, reply_markup=builder.as_markup())
        return await call.answer()
        
    text = "💼 **Управление дропами (Последние 10)**:\n\n"
    
    for drop in all_drops[:10]:
        status_emoji = "✅" if drop['status'] == 'active' else "⏳"
        text += f"{status_emoji} {drop['pc_name']} ({drop['phone']})\n"
        
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔍 Показать/Сменить статус (TBD)", callback_data="drops_view_status"))
    builder.row(InlineKeyboardButton(text="🔙 В админ-меню", callback_data="admin_stats"))
    
    await call.message.edit_text(text, reply_markup=builder.as_markup())
    await call.answer()

@drops_router.callback_query(F.data == "drops_view_status")
async def drops_view_status(call: types.CallbackQuery):
    await call.answer("🛠️ Детальное управление дропами пока не реализовано.", show_alert=True)


# =========================================================================
# VIII. CLEANUP & SHUTDOWN (ДОБАВЛЕНО)
# =========================================================================

async def cleanup_temp_sessions():
    while True:
        await asyncio.sleep(3600)
        now = db.get_current_time_msk()
        try:
            file_list = await asyncio.to_thread(os.listdir, SESSION_DIR)
        except Exception as e:
            logger.error(f"Error reading session directory: {e}")
            file_list = []

        for f in file_list:
            if f.startswith('temp_') and f.endswith('.session'): 
                file_path = os.path.join(SESSION_DIR, f)
                try:
                    if await asyncio.to_thread(os.path.exists, file_path):
                       file_creation_time = datetime.fromtimestamp(await asyncio.to_thread(os.path.getctime, file_path))
                       if (now.replace(tzinfo=None) - file_creation_time) > timedelta(hours=1):
                           await asyncio.to_thread(os.remove, file_path)
                           logger.info(f"Removed old temp session: {f}")
                except Exception as e:
                    logger.warning(f"Failed to remove temp session {f}: {e}")


async def on_startup(dispatcher: Dispatcher):
    global tm 
    
    if not BOT_TOKEN or API_ID == 0 or not API_HASH:
        logger.critical("❌ КРИТИЧЕСКАЯ ОШИБКА КОНФИГУРАЦИИ: Проверьте BOT_TOKEN, API_ID, API_HASH.")
        raise SystemExit(1)
        
    logger.info("Bot started and configuration validated.")

    # 🚀 ВОЗОБНОВЛЕНИЕ РАБОТЫ WORKER
    active_ids = await db.get_active_telethon_users()
    for uid in active_ids:
        if await db.check_subscription(uid):
            asyncio.create_task(tm.start_client_task(uid)) 

    asyncio.create_task(cleanup_temp_sessions())

async def on_shutdown(dispatcher: Dispatcher):
    global tm
    logger.info("Shutting down workers and connections...")
    
    async with store.lock:
        workers_to_stop = list(store.active_workers.keys())
    
    shutdown_tasks = [tm.stop_worker(uid) for uid in workers_to_stop]
    if shutdown_tasks:
        await asyncio.wait(shutdown_tasks, timeout=5)
        
    logger.info("Telethon clients disconnected.")

# =========================================================================
# IX. MAIN (ИСПРАВЛЕН)
# =========================================================================

async def main():
    await db.init()
    
    # 🚨 ИСПРАВЛЕНО: Правильное подключение Middleware
    rate_limit_middleware = RateLimitMiddleware()
    dp.message.middleware(rate_limit_middleware)
    dp.callback_query.middleware(rate_limit_middleware)
    
    dp.include_router(user_router)
    dp.include_router(admin_router)
    dp.include_router(drops_router)
    
    # Регистрация хуков
    dp.startup.register(on_startup) 
    dp.shutdown.register(on_shutdown)

    await bot.delete_webhook(drop_pending_updates=True) 
    await dp.start_polling(bot)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot execution interrupted.")
    except Exception as e:
        logger.critical(f"Critical error in main: {e}", exc_info=True)
