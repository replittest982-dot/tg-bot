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
from contextlib import suppress

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage 
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, Update 
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter, TelegramConflictError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.dispatcher.middlewares.base import BaseMiddleware 

# 🛠️ ИСПРАВЛЕНИЕ ИМПОРТА ErrorEvent
try:
    from aiogram.types import ErrorEvent
except ImportError:
    from aiogram.types.error_event import ErrorEvent

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, 
    AuthKeyUnregisteredError, UserIsBlockedError, PasswordHashInvalidError, 
    RpcCallFailError, SessionRevokedError, UserDeactivatedBanError
)

# --- OTHER ---
import aiosqlite
import pytz 

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# =========================================================================

# ✅ ВАШИ ДАННЫЕ (ВШИТЫ)
BOT_TOKEN = "7868097991:AAG48aFRhSd6dDB7dI6AkrYD_mzLJgclNVk"
ADMIN_ID = 6256576302
API_ID = 29930612
API_HASH = "2690aa8c364b91e47b6da1f90a71f825"
DROPS_CHAT_ID = -100 # ⚠️ ЗАМЕНИТЕ НА ID ВАШЕГО ЧАТА ДЛЯ ДРОПОВ, если он используется.

# Настройки
SUPPORT_BOT_USERNAME = "suppor_tstatpro1bot"
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 0.5 
SESSION_DIR = 'sessions'

# Создание папок
if not os.path.exists(SESSION_DIR): os.makedirs(SESSION_DIR)
if not os.path.exists('data'): os.makedirs('data')

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация бота
storage = MemoryStorage() 
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='Markdown'))
dp = Dispatcher(storage=storage)

# Роутеры
user_router = Router()
drops_router = Router()
admin_router = Router()

# =========================================================================
# II. ГЛОБАЛЬНОЕ ХРАНИЛИЩЕ И FSM СОСТОЯНИЯ
# =========================================================================

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {} 
        self.process_progress: Dict[int, Dict] = {}
        self.pc_monitoring: Dict[Union[int, str], str] = {}
        self.active_workers: Dict[int, TelegramClient] = {} 
        self.worker_tasks: Dict[int, List[asyncio.Task]] = {} 
        self.last_user_request: Dict[int, datetime] = {}

store = GlobalStorage()

# --- FSM States ---
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
    # Управление подпиской
    waiting_for_user_id_for_sub = State() 
    waiting_for_sub_days = State()
    # Создание промокода
    waiting_for_promo_days = State()
    waiting_for_promo_uses = State()
    
# =========================================================================
# III. DATABASE (AIOSQLITE)
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        self.TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

    def get_current_time_msk(self) -> datetime:
        return datetime.now(self.TIMEZONE_MSK)

    def to_msk_aware(self, dt_str: str) -> datetime:
        if not dt_str: return datetime.fromtimestamp(0, self.TIMEZONE_MSK) 
        try:
            naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
            return self.TIMEZONE_MSK.localize(naive_dt)
        except ValueError:
            return datetime.fromtimestamp(0, self.TIMEZONE_MSK)
        
    def _calculate_new_end_date(self, current_end_date_str: Optional[str], days_to_add: int) -> str:
        now = self.get_current_time_msk()
        start_date = now
        
        if current_end_date_str:
            try:
                current_end = self.to_msk_aware(current_end_date_str)
                if current_end > now:
                    start_date = current_end
            except:
                pass 

        new_end_date = start_date + timedelta(days=days_to_add)
        return new_end_date.strftime('%Y-%m-%d %H:%M:%S')

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
        global ADMIN_ID, tm
        if user_id == ADMIN_ID: return True
        
        user = await self.get_user(user_id)
        if not user or not user.get('subscription_active') or not user.get('subscription_end_date'): 
            return False

        try:
            end = self.to_msk_aware(user['subscription_end_date'])
            now = self.get_current_time_msk()
            
            if end > now:
                return True
            else:
                await self.set_telethon_status(user_id, False)
                await self.set_subscription_status(user_id, False, None)
                if 'tm' in globals() and tm:
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

    async def activate_promo_code(self, user_id: int, code: str) -> Optional[int]:
        promo = await self.get_promo_code(code)
        if not promo or (promo['uses_left'] is not None and promo['uses_left'] == 0):
            return None

        user = await self.get_user(user_id)
        days = promo['days']
        new_end_date_str = self._calculate_new_end_date(user.get('subscription_end_date'), days)

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET subscription_active=1, subscription_end_date=? WHERE user_id=?", (new_end_date_str, user_id))
            if promo['uses_left'] != -1: 
                 await db.execute("UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code=?", (code.upper(),))
            await db.commit()
        return days 

    async def get_promo_code(self, code: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM promo_codes WHERE code=?", (code.upper(),)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_active_telethon_users(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
                
    async def create_promo_code(self, code: str, days: int, uses: int):
        async with aiosqlite.connect(self.db_path) as db:
            now_str = self.get_current_time_msk().strftime('%Y-%m-%d %H:%M:%S')
            uses_value = uses if uses != 0 else -1 
            try:
                await db.execute("INSERT INTO promo_codes (code, days, uses_left, created_at) VALUES (?, ?, ?, ?)", (code.upper(), days, uses_value, now_str))
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False 

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

db = AsyncDatabase(os.path.join('data', DB_NAME))

# =========================================================================
# IV. MIDDLEWARE & UTILS
# =========================================================================

def get_user_id_from_update(update: Update) -> Optional[int]:
    if update.message: return update.message.from_user.id
    if update.callback_query: return update.callback_query.from_user.id
    if update.from_user: return update.from_user.id
    return None

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, limit: float = RATE_LIMIT_TIME):
        self.limit = limit
        self.lock = asyncio.Lock()
        super().__init__()

    async def __call__(self, handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]], event: Update, data: Dict[str, Any]) -> Any:
        user_id = get_user_id_from_update(event)
        if not user_id: return await handler(event, data)

        now = datetime.now(TIMEZONE_MSK)
        async with self.lock:
            last = store.last_user_request.get(user_id)
            if last and (now - last).total_seconds() < self.limit:
                return 
            store.last_user_request[user_id] = now
        
        return await handler(event, data)

def generate_promo_code(length=8):
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

# --- KEYBOARDS ---
def get_main_menu_keyboard(user_id: int, is_subscribed: bool, session_exists: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="👤 Профиль", callback_data="profile_menu"))
    
    if is_subscribed:
        # 1. Изменение кнопки для управления аккаунтом
        text = "⚙️ Управление аккаунтом" if session_exists else "🚪 Войти в аккаунт"
        builder.row(InlineKeyboardButton(text=text, callback_data="worker_menu"))
    
    if not is_subscribed:
        builder.row(InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="enter_promo"))
    
    builder.row(InlineKeyboardButton(text="❓ Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}"))
    
    if user_id == ADMIN_ID:
        builder.row(InlineKeyboardButton(text="📊 Админ-панель", callback_data="admin_stats"))
        
    return builder.as_markup()

def get_account_menu_keyboard(is_worker_active: bool, session_exists: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    if is_worker_active:
        builder.row(InlineKeyboardButton(text="🛑 Остановить Аккаунт", callback_data="stop_worker"))
    elif session_exists: 
        builder.row(InlineKeyboardButton(text="▶️ Запустить Аккаунт", callback_data="start_worker"))
        
    builder.row(InlineKeyboardButton(text="🚪 Сменить аккаунт/Авторизация", callback_data="auth_method_menu"))
        
    builder.row(InlineKeyboardButton(text="🔙 Профиль", callback_data="profile_menu"))
    return builder.as_markup()

def get_admin_promo_menu_keyboard(codes_list: List[Dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Создать промокод", callback_data="admin_create_promo"))
    
    # Добавление кнопок для каждого промокода
    for promo in codes_list:
        code = promo['code']
        uses = promo['uses_left']
        days = promo['days']
        uses_display = "∞" if uses == -1 else uses
        
        text = f"🔑 {code} ({days} дн. | {uses_display} исп.)"
        builder.row(
            InlineKeyboardButton(text=text, callback_data=f"admin_view_promo_{code}"),
        )
        
    builder.row(InlineKeyboardButton(text="🔙 Админ-панель", callback_data="admin_stats"))
    return builder.as_markup()

# =========================================================================
# V. TELETHON MANAGER
# =========================================================================

class TelethonManager:
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

    async def _finalize_auth(self, user_id: int, original_message: Message, state: FSMContext, user_info: Union[User, Channel, Chat]):
        """Завершение авторизации."""
        temp_path = os.path.join(SESSION_DIR, f'temp_{user_id}.session')
        final_path = os.path.join(SESSION_DIR, f'session_{user_id}.session')
        
        if await asyncio.to_thread(os.path.exists, temp_path):
            await asyncio.to_thread(os.rename, temp_path, final_path)
        
        if client := store.temp_auth_clients.pop(user_id, None):
            if client.is_connected(): await client.disconnect()

        await state.clear()
        
        name = getattr(user_info, 'first_name', 'Аккаунт')
        await original_message.answer(f"🎉 **Успешная авторизация!** Аккаунт **{name}** привязан.")
        
        await self.start_client_task(user_id)
        
        # Фиктивный Call для обновления меню
        fake_call = types.CallbackQuery( 
            id='fake_finalize', 
            from_user=types.User(id=user_id, is_bot=False, first_name="User"), 
            message=await original_message.answer("🔄 Переход к управлению аккаунтом...") 
        )
        # Вызываем worker_menu, который теперь называется account_menu
        await account_menu(fake_call, state) 

    async def start_client_task(self, user_id):
        if not await db.check_subscription(user_id):
            await self._send_to_bot_user(user_id, "⚠️ **Ваша подписка истекла.** Аккаунт не запущен.")
            return

        session_exists = await asyncio.to_thread(os.path.exists, os.path.join(SESSION_DIR, f'session_{user_id}.session'))
        if not session_exists:
            await self._send_to_bot_user(user_id, "⚠️ **Сессия не найдена.** Проведите авторизацию.")
            return

        await self.stop_worker(user_id)
        
        task = asyncio.create_task(self._run_worker(user_id))
        async with store.lock:
            store.worker_tasks.pop(user_id, None)
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
            me = await client.get_me() 
            await db.set_telethon_status(user_id, True)
            await self._send_to_bot_user(user_id, f"🚀 Аккаунт **{me.first_name}** запущен и готов к работе.")
            await client.run_until_disconnected()

        except (AuthKeyUnregisteredError, SessionPasswordNeededError, PhoneNumberInvalidError, EOFError, SessionRevokedError, UserDeactivatedBanError):
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Требуется повторная авторизация.")
            session_file = os.path.join(SESSION_DIR, f'session_{user_id}.session')
            if await asyncio.to_thread(os.path.exists, session_file):
                try: await asyncio.to_thread(os.remove, session_file)
                except: pass
            await self.stop_worker(user_id)
        except Exception as e:
            logger.critical(f"Worker {user_id} failed: {e}", exc_info=True)
            await self._send_to_bot_user(user_id, f"💔 Аккаунт отключился: `{e.__class__.__name__}`.")
        finally:
            await self.stop_worker(user_id, silent=True) 
            await db.set_telethon_status(user_id, False)

    async def stop_worker(self, user_id, silent=False):
        async with store.lock:
            client = store.active_workers.pop(user_id, None)
            tasks = store.worker_tasks.pop(user_id, [])
            for t in tasks:
                if not t.done(): t.cancel()

        if client:
            try:
                if client.is_connected(): await client.disconnect()
                if not silent: await self._send_to_bot_user(user_id, "🛑 Аккаунт остановлен.")
            except Exception as e:
                logger.warning(f"Error disconnecting {user_id}: {e}")

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
                    return await client.send_message(chat, "⚠️ Формат: `.флуд [кол-во] [текст] [задержка]`", reply_to=event.message.id)
                
                count = int(parts[1])
                delay_str = parts[-1]
                if delay_str.replace('.', '', 1).isdigit():
                    delay = max(0.5, float(delay_str)) 
                    text = " ".join(parts[2:-1])
                else:
                    delay = 0.5
                    text = " ".join(parts[2:])
                
                async with store.lock:
                    if store.process_progress.get(user_id, {}).get('type') == 'flood':
                        return await client.send_message(chat, "⚠️ Флуд уже активен. `.стопфлуд`")
                    store.process_progress[user_id] = {'type': 'flood', 'stop': False}
                
                task = asyncio.create_task(self._flood_task(client, chat, text, count, delay, user_id))
                async with store.lock:
                    store.worker_tasks.setdefault(user_id, []).append(task)

                temp = await client.send_message(chat, f"🚀 Флуд запущен: {count} шт, {delay}с.")
                await asyncio.sleep(2)
                await temp.delete()
            except Exception as e:
                await client.send_message(chat, f"❌ Ошибка: `{e.__class__.__name__}`")
        
        elif cmd == '.стопфлуд':
            async with store.lock:
                if store.process_progress.get(user_id, {}).get('type') == 'flood':
                    store.process_progress[user_id]['stop'] = True
                    temp = await client.send_message(chat, "🛑 Флуд остановлен.")
                    await asyncio.sleep(2)
                    await temp.delete()
                else:
                    temp = await client.send_message(chat, "⚠️ Нет активного флуда.")
                    await asyncio.sleep(2)
                    await temp.delete()

    async def _flood_task(self, client, chat, text, count, delay, user_id):
        i = 0
        max_limit = 5000 if count == 0 else count
        while i < max_limit: 
            async with store.lock: 
                if store.process_progress.get(user_id, {}).get('stop'): break
            try:
                await client.send_message(chat, text)
                i += 1
                await asyncio.sleep(delay)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + random.randint(1, 5)) 
            except Exception:
                break
        async with store.lock:
            store.process_progress.pop(user_id, None)

tm = TelethonManager(bot) 

# =========================================================================
# VI. HANDLERS (USER)
# =========================================================================

# --- START ---
@user_router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    await db.get_user(user_id) 
    is_subscribed = await db.check_subscription(user_id)
    session_exists = await asyncio.to_thread(os.path.exists, os.path.join(SESSION_DIR, f'session_{user_id}.session'))

    await message.answer("👋 Добро пожаловать в систему STATPRO.", reply_markup=get_main_menu_keyboard(user_id, is_subscribed, session_exists))

# --- PROFILE MENU ---
@user_router.callback_query(F.data.in_({"profile_menu", "start_menu"}))
async def profile_menu(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    message_to_edit = call.message
    await call.answer()
    
    await state.clear()
    
    user_data = await db.get_user(user_id)
    is_subscribed = await db.check_subscription(user_id)
    is_worker_active = user_data.get('telethon_active', False)
    session_exists = await asyncio.to_thread(os.path.exists, os.path.join(SESSION_DIR, f'session_{user_id}.session'))
    
    if call.data == "start_menu":
        await message_to_edit.edit_text("👋 Добро пожаловать в систему STATPRO.", reply_markup=get_main_menu_keyboard(user_id, is_subscribed, session_exists))
        return

    end_date_str = user_data.get('subscription_end_date')
    end_date_info = db.to_msk_aware(end_date_str).strftime('%d.%m.%Y %H:%M MSK') if is_subscribed and end_date_str else "Не активна"
    
    # 2. Изменение слова "Worker"
    auth_status = "✅ Авторизован" if session_exists else "❌ Не авторизован"
    active_status = "Да" if is_worker_active else "Нет"
    
    text = (
        f"👤 **Ваш Профиль**\n\n"
        f"🔹 **ID:** `{user_id}`\n"
        f"✅ **Подписка:** {'Да' if is_subscribed else 'Нет'}\n"
        f"🗓️ **До:** `{end_date_info}`\n"
        f"🔗 **Статус авторизации:** {auth_status}\n"
        f"🚀 **Аккаунт запущен:** {active_status}"
    )
    
    builder = InlineKeyboardBuilder()
    if is_subscribed: 
        text_btn = "⚙️ Управление аккаунтом" if session_exists else "🚪 Войти в аккаунт"
        builder.row(InlineKeyboardButton(text=text_btn, callback_data="worker_menu"))
    else: 
        builder.row(InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="enter_promo"))
        
    builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="start_menu"))
    
    await message_to_edit.edit_text(text, reply_markup=builder.as_markup())

# --- УПРАВЛЕНИЕ АККАУНТОМ (бывший Worker Menu) ---
@user_router.callback_query(F.data == "worker_menu")
async def account_menu(call: types.CallbackQuery, state: FSMContext): 
    user_id = call.from_user.id
    message_to_edit = call.message 
    await call.answer()
    await state.clear()

    if not await db.check_subscription(user_id):
        await call.answer("❌ Ваша подписка истекла.", show_alert=True)
        return await profile_menu(call, state) 

    user_data = await db.get_user(user_id)
    is_worker_active = user_data.get('telethon_active', False)
    session_exists = await asyncio.to_thread(os.path.exists, os.path.join(SESSION_DIR, f'session_{user_id}.session'))

    status_text = "✅ **Аккаунт запущен**." if is_worker_active else "❌ **Аккаунт остановлен**."
    
    text = f"⚙️ **Управление аккаунтом**\n\n{status_text}\n\n*Для смены аккаунта используйте кнопку 'Сменить аккаунт/Авторизация'.*"
    
    await message_to_edit.edit_text(text, reply_markup=get_account_menu_keyboard(is_worker_active, session_exists))


# --- WORKER ACTIONS (Переименованы) ---
@user_router.callback_query(F.data == "stop_worker")
async def stop_worker_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer("Остановка аккаунта...", show_alert=False)
    await tm.stop_worker(call.from_user.id)
    return await account_menu(call, state)

@user_router.callback_query(F.data == "start_worker")
async def start_worker_handler(call: types.CallbackQuery, state: FSMContext):
    session_exists = await asyncio.to_thread(os.path.exists, os.path.join(SESSION_DIR, f'session_{call.from_user.id}.session'))
    if not session_exists:
        await call.answer("❌ Сессия не найдена. Сначала авторизуйтесь.", show_alert=True)
        return await account_menu(call, state)
        
    await call.answer("Запуск аккаунта...", show_alert=False)
    await tm.start_client_task(call.from_user.id)
    await asyncio.sleep(1) 
    return await account_menu(call, state)

# --- AUTH FLOW ---
@user_router.callback_query(F.data == "auth_method_menu")
async def auth_method_menu(call: types.CallbackQuery, state: FSMContext):
    await tm.stop_worker(call.from_user.id) 
    temp_path = os.path.join(SESSION_DIR, f'temp_{call.from_user.id}.session')
    with suppress(FileNotFoundError): await asyncio.to_thread(os.remove, temp_path)
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📲 По номеру телефона", callback_data="auth_by_phone"))
    builder.row(InlineKeyboardButton(text="🔙 Управление аккаунтом", callback_data="worker_menu")) 
    
    await call.message.edit_text("🚪 **Выберите способ авторизации**:", reply_markup=builder.as_markup())
    await call.answer()

# Хендлеры авторизации (PHONE, CODE, PASSWORD) остаются без изменений...
@user_router.callback_query(F.data == "auth_by_phone")
async def auth_by_phone_step1(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("📲 Введите **номер телефона** (например, `+79001234567`):", reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Отмена", callback_data="worker_menu")).as_markup())
    await call.answer()

@user_router.message(TelethonAuth.PHONE, F.text)
async def auth_by_phone_step2_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    user_id = message.from_user.id
    if not re.match(r'^\+\d{10,15}$', phone):
        return await message.answer("❌ Неверный формат. Пример: `+79001234567`")
    
    await state.update_data(phone=phone, original_message=message) 
    path = os.path.join(SESSION_DIR, f'temp_{user_id}')
    client = TelegramClient(path, API_ID, API_HASH)
    
    if user_id in store.temp_auth_clients:
        try: await store.temp_auth_clients[user_id].disconnect()
        except: pass
    async with store.lock: store.temp_auth_clients[user_id] = client
        
    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        await state.update_data(phone=phone, sent_code=sent_code)
        await state.set_state(TelethonAuth.CODE)
        await message.answer(f"✅ Код отправлен на **{phone}**. Введите код:")
    except PhoneNumberInvalidError:
        await client.disconnect()
        await state.clear()
        await message.answer("❌ Неверный номер.")
    except Exception as e:
        logger.error(f"Auth Error: {e}")
        await client.disconnect()
        await state.clear()
        await message.answer("❌ Ошибка отправки кода.")

@user_router.message(TelethonAuth.CODE)
async def auth_by_phone_step3_sign_in(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip()
    data = await state.get_data()
    client = store.temp_auth_clients.get(user_id)
    if not client:
        await state.clear()
        return await message.answer("❌ Сессия истекла.")
        
    try:
        if not client.is_connected(): await client.connect()
        user_info = await client.sign_in(data['phone'], code, password=None, phone_code_hash=data['sent_code'].phone_code_hash)
        await tm._finalize_auth(user_id, data['original_message'], state, user_info)
    except SessionPasswordNeededError:
        await state.update_data(code_hash=data['sent_code'].phone_code_hash) 
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("⚠️ **Введите облачный пароль (2FA):**")
    except Exception as e:
        logger.error(f"Sign-in Error: {e}")
        await message.answer("❌ Ошибка кода. Попробуйте снова.")

@user_router.message(TelethonAuth.PASSWORD)
async def auth_by_phone_step4_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    password = message.text.strip()
    data = await state.get_data()
    client = store.temp_auth_clients.get(user_id)
    
    try:
        if not client.is_connected(): await client.connect()
        user_info = await client.sign_in(password=password)
        await tm._finalize_auth(user_id, data['original_message'], state, user_info)
    except PasswordHashInvalidError:
        await message.answer("❌ Неверный пароль.")
    except Exception as e:
        await state.clear()
        if client: await client.disconnect()
        await message.answer("❌ Ошибка входа.")

# --- PROMO ACTIVATION ---
@user_router.callback_query(F.data == "enter_promo")
async def enter_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await call.message.edit_text("🔑 **Введите промокод**:", reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Отмена", callback_data="profile_menu")).as_markup())
    await call.answer()

@user_router.message(PromoStates.waiting_for_code, F.text)
async def process_promo_activation(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    days = await db.activate_promo_code(message.from_user.id, code)
    await state.clear()
    
    if days:
        await message.answer(f"🎉 **Промокод активирован!** +{days} дней.")
        session_exists = await asyncio.to_thread(os.path.exists, os.path.join(SESSION_DIR, f'session_{message.from_user.id}.session'))
        fake_call = types.CallbackQuery(id='fake', from_user=message.from_user, message=await message.answer("🔄 Обновление..."))
        return await profile_menu(fake_call, state)
    else:
        await message.answer("❌ **Неверный код.**", reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Профиль", callback_data="profile_menu")).as_markup())


# =========================================================================
# VII. HANDLERS (ADMIN)
# =========================================================================

# --- ADMIN MAIN MENU ---
@admin_router.callback_query(F.data == "admin_stats")
async def admin_main_menu(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.clear()
    total_users = await db.get_all_users_count()
    active_subs = await db.get_active_subs_count()
    
    text = (
        f"📊 **Админ-панель**\n\n"
        f"👤 Всего пользователей: {total_users}\n"
        f"✅ Активных подписок: {active_subs}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔑 Промокоды", callback_data="admin_promo_menu"))
    builder.row(InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_give_sub"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="start_menu"))
    
    await call.message.edit_text(text, reply_markup=builder.as_markup())

# --- ADMIN SUB MENU (PROMO) ---
@admin_router.callback_query(F.data == "admin_promo_menu")
async def admin_promo_menu(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.clear()
    
    codes = await db.get_all_promo_codes()
    
    text = f"🔑 **Управление промокодами**\n\nВсего активных кодов: {len(codes)}"
    
    await call.message.edit_text(text, reply_markup=get_admin_promo_menu_keyboard(codes))

# --- ADMIN CREATE PROMO (STEP 1: DAYS) ---
@admin_router.callback_query(F.data == "admin_create_promo")
async def admin_create_promo_step1(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(AdminStates.waiting_for_promo_days)
    
    builder = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_promo_menu"))
    await call.message.edit_text("🗓️ **Введите количество дней** подписки для этого промокода (число):", reply_markup=builder.as_markup())

@admin_router.message(AdminStates.waiting_for_promo_days)
async def admin_create_promo_step2(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError
        await state.update_data(promo_days=days)
        await state.set_state(AdminStates.waiting_for_promo_uses)
        
        await message.answer(
            "🧮 **Введите количество использований** (число):\n\n"
            "0 - для **бесконечного** использования.",
            reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_promo_menu")).as_markup()
        )
    except ValueError:
        await message.answer("❌ Введите корректное число дней (больше 0).")

@admin_router.message(AdminStates.waiting_for_promo_uses)
async def admin_create_promo_step3(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        uses = int(message.text.strip())
        if uses < 0: raise ValueError
        
        data = await state.get_data()
        days = data['promo_days']
        
        # ГЕНЕРАЦИЯ ПРОМОКОДА
        new_code = generate_promo_code(10) # 10 символов
        
        success = await db.create_promo_code(new_code, days, uses)
        
        if success:
            await state.clear()
            uses_display = "бесконечно" if uses == 0 else f"{uses} раз"
            
            text = (
                f"🎉 **Промокод создан!**\n\n"
                f"🔑 Код: `{new_code}`\n"
                f"🗓️ Дней: **{days}**\n"
                f"🧮 Использований: **{uses_display}**"
            )
            await message.answer(text, reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 К промокодам", callback_data="admin_promo_menu")).as_markup())
        else:
            # Маловероятно, но если код совпал (коллайдер), попробуем снова
            await message.answer("❌ Ошибка БД. Попробуйте снова.") 
            await state.clear()

    except ValueError:
        await message.answer("❌ Введите корректное число использований (0 или больше).")

# --- ADMIN VIEW/DELETE PROMO ---
@admin_router.callback_query(F.data.startswith("admin_view_promo_"))
async def admin_view_promo(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.answer()
    
    code = call.data.split('_')[-1]
    promo = await db.get_promo_code(code)
    
    if not promo:
        await call.message.edit_text("❌ Промокод не найден.", reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 К промокодам", callback_data="admin_promo_menu")).as_markup())
        return

    uses_display = "Бесконечно" if promo['uses_left'] == -1 else promo['uses_left']
    
    text = (
        f"🔑 **Информация о промокоде: {promo['code']}**\n\n"
        f"🗓️ Дней подписки: **{promo['days']}**\n"
        f"🧮 Осталось использований: **{uses_display}**\n"
        f"⏳ Создан: {promo['created_at']} MSK"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"admin_delete_promo_{code}"))
    builder.row(InlineKeyboardButton(text="🔙 К промокодам", callback_data="admin_promo_menu"))
    
    await call.message.edit_text(text, reply_markup=builder.as_markup())

@admin_router.callback_query(F.data.startswith("admin_delete_promo_"))
async def admin_delete_promo(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    code = call.data.split('_')[-1]
    await call.answer(f"Промокод {code} удален.", show_alert=True)
    
    await db.delete_promo_code(code)
    
    # Возврат в меню промокодов
    await admin_promo_menu(call, state)


# --- ADMIN GIVE SUB ---
@admin_router.callback_query(F.data == "admin_give_sub")
async def admin_give_sub(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(AdminStates.waiting_for_user_id_for_sub)
    await call.message.edit_text("👤 **Введите ID пользователя**, которому нужно выдать подписку (число):", reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_stats")).as_markup())

@admin_router.message(AdminStates.waiting_for_user_id_for_sub)
async def admin_give_sub_id(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        uid = int(message.text.strip())
        await db.get_user(uid) # Создаст запись, если нет
        await state.update_data(target_uid=uid)
        await state.set_state(AdminStates.waiting_for_sub_days)
        await message.answer(f"🗓️ Пользователь `{uid}` выбран. Введите **количество дней** подписки (число):")
    except: await message.answer("❌ ID должен быть числом. Попробуйте снова.")

@admin_router.message(AdminStates.waiting_for_sub_days)
async def admin_give_sub_days(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError
        
        data = await state.get_data()
        uid = data['target_uid']
        
        user = await db.get_user(uid)
        end = db._calculate_new_end_date(user.get('subscription_end_date') if user else None, days)
        await db.set_subscription_status(uid, True, end)
        
        await message.answer(
            f"✅ **Успешно!** Выдано **{days}** дней пользователю `{uid}`. Действует до {end} MSK."
        )
        await state.clear()
        
    except ValueError: 
        await message.answer("❌ Количество дней должно быть положительным числом.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e.__class__.__name__}. Попробуйте снова.")
        await state.clear()


# =========================================================================
# VIII. MAIN
# =========================================================================

# --- GLOBAL ERROR HANDLER ---
@dp.error()
async def global_error_handler(event: ErrorEvent):
    exception = event.exception
    
    with suppress():
        if isinstance(exception, TelegramBadRequest) and ("message is not modified" in str(exception).lower() or "can't parse entities" in str(exception).lower()): return True 
        if isinstance(exception, TelegramRetryAfter):
            logger.warning(f"FloodWait encountered. Sleeping for {exception.timeout}s.")
            await asyncio.sleep(exception.timeout)
            return True
        if isinstance(exception, TelegramConflictError):
            logger.critical("🚨 TelegramConflictError: Another bot instance is running!")
            return True

    logger.critical(f"🚨 КРИТИЧЕСКАЯ ОШИБКА: {exception.__class__.__name__}: {exception}", exc_info=True)
    
    if ADMIN_ID:
        error_msg = (
            f"🔥 **BOT CRASH** 🔥\n"
            f"❌ Тип: `{exception.__class__.__name__}`\n"
            f"📄 Ошибка: `{str(exception)[:100]}`\n" 
            f"📍 Трейсбек:\n`{traceback.format_exc()[:1500]}`"
        )
        try:
            await bot.send_message(ADMIN_ID, error_msg, parse_mode='Markdown')
        except: pass
            
    return True

# --- STARTUP ---
async def on_startup(dispatcher: Dispatcher):
    logger.info("Bot starting...")
    await db.init() 
    
    # Запуск активных воркеров
    active_users = await db.get_active_telethon_users()
    for uid in active_users:
        # Проверяем подписку перед запуском
        if await db.check_subscription(uid): asyncio.create_task(tm.start_client_task(uid))

async def main():
    dp.message.middleware(RateLimitMiddleware())
    dp.callback_query.middleware(RateLimitMiddleware())
    
    dp.include_router(user_router)
    dp.include_router(admin_router)
    dp.include_router(drops_router)
    
    dp.startup.register(on_startup)
    
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Polling started...")
    await dp.start_polling(bot)

if __name__ == '__main__':
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Critical error in main loop: {e}", exc_info=True)
