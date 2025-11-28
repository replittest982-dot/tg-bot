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
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, AuthKeyUnregisteredError, UserIsBlockedError

# --- OTHER ---
import aiosqlite
import pytz

# =========================================================================
# I. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ (ОБНОВЛЕНО)
# =========================================================================

# --- КЛЮЧИ И КОНСТАНТЫ (ВШИТЫ В КОД по запросу) ---
# ВНИМАНИЕ: ХРАНЕНИЕ СЕКРЕТНЫХ ДАННЫХ В КОДЕ - ОПАСНО!
BOT_TOKEN = "7868097991:AAHIHM32o9MeluAeWgBwC9WKHydiedWUrQY" 
ADMIN_ID = 6256576302                                        
API_ID = 29930612                                            
API_HASH = "2690aa8c364b91e47b6da1f90a71f825"                
DROPS_CHAT_ID = -100                                         # !!! ЗАМЕНИТЕ ЭТО ЗНАЧЕНИЕ НА РЕАЛЬНЫЙ ID ВАШЕГО ЧАТА ДЛЯ ДРОПОВ !!!

# Прочие настройки
SUPPORT_BOT_USERNAME = "suppor_tstatpro1bot"
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 1.0 
SESSION_DIR = 'sessions'

# Автоматическое создание папок
if not os.path.exists(SESSION_DIR): os.makedirs(SESSION_DIR)
if not os.path.exists('data'): os.makedirs('data')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация FSM Storage
storage = MemoryStorage() 

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=storage)
user_router = Router()
drops_router = Router()
admin_router = Router()

# =========================================================================
# II. ГЛОБАЛЬНЫЕ ХРАНИЛИЩА И FSM СОСТОЯНИЯ
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

# --- FSM СОСТОЯНИЯ ---
class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    
class DropStates(StatesGroup):
    waiting_for_phone_and_pc = State()
    waiting_for_phone_change = State()

class PromoStates(StatesGroup):
    waiting_for_code = State()

# --- FSM СОСТОЯНИЯ АДМИНА ---
class AdminStates(StatesGroup):
    waiting_for_promo_code_creation = State()
    waiting_for_promo_days = State()
    waiting_for_promo_uses = State()
    waiting_for_user_id_for_sub = State() # Для выдачи подписки
    
# =========================================================================
# III. ASYNC DATABASE
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
        global tm # Важно для доступа к TelethonManager
        
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

    # --- МЕТОДЫ ПРОМОКОДОВ (Остаются прежними) ---

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

    # Статистика
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
# IV. RATE LIMIT MIDDLEWARE
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
        data: Dict[str, Any]
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
            await client.start()
            await db.set_telethon_status(user_id, True)
            await self._send_to_bot_user(user_id, "🚀 Worker успешно запущен и готов к работе!")
            await client.run_until_disconnected()

        except AuthKeyUnregisteredError:
            await self._send_to_bot_user(user_id, "⚠️ Сессия недействительна. Требуется повторная авторизация.")
        except Exception as e:
            logger.error(f"Worker {user_id} failed: {e}", exc_info=True)
            await self._send_to_bot_user(user_id, f"💔 Worker отключился из-за критической ошибки: {e.__class__.__name__}.")
        finally:
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
                await client.send_message(chat, f"❌ Ошибка флуда: {e.__class__.__name__}")
        
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

# =========================================================================
# VI. USER HANDLERS (С worker_menu)
# =========================================================================

@user_router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    await db.get_user(user_id) 
    
    is_subscribed = await db.check_subscription(user_id)

    builder = InlineKeyboardBuilder()

    if is_subscribed:
        builder.row(InlineKeyboardButton(text="📱 Управление Worker", callback_data="worker_menu"))
    else:
        builder.row(InlineKeyboardButton(text="✅ Активировать подписку", callback_data="enter_promo"))
        
    builder.row(InlineKeyboardButton(text="❓ Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}"))

    if user_id == ADMIN_ID:
        builder.row(InlineKeyboardButton(text="📊 Админ-панель", callback_data="admin_stats"))

    end_date_str = (await db.get_user(user_id)).get('subscription_end_date', 'N/A')
    end_date_info = f" до: **{db.to_msk_aware(end_date_str).strftime('%d.%m.%Y %H:%M MSK')}**" if is_subscribed and end_date_str else ""
    
    status_text = f"✅ **Подписка активна**{end_date_info}." if is_subscribed else "⚠️ **Подписка неактивна**. Используйте промокод."

    await message.answer(f"👋 Добро пожаловать в STATPRO Worker!\n\n{status_text}", reply_markup=builder.as_markup())

# --- worker_menu (Добавленный хэндлер) ---
@user_router.callback_query(F.data == "worker_menu")
async def worker_menu(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    is_subscribed = await db.check_subscription(user_id)
    
    if not is_subscribed:
        await call.answer("❌ Ваша подписка истекла.", show_alert=True)
        return await cmd_start(call.message, state)

    user_data = await db.get_user(user_id)
    is_worker_active = user_data.get('telethon_active', False)
    
    builder = InlineKeyboardBuilder()
    
    if is_worker_active:
        builder.row(InlineKeyboardButton(text="🛑 Остановить Worker", callback_data="stop_worker"))
    else:
        builder.row(InlineKeyboardButton(text="▶️ Запустить Worker", callback_data="start_worker"))
        
    builder.row(InlineKeyboardButton(text="⚙️ Сменить аккаунт", callback_data="change_account"))
    builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="start_menu"))

    status_text = "✅ **Worker активен**." if is_worker_active else "❌ **Worker не активен**."
    
    await call.message.edit_text(f"📱 **Управление Worker**\n\n{status_text}\n\nЗдесь вы можете контролировать состояние вашего аккаунта Telegram, привязанного к боту.", reply_markup=builder.as_markup())
    await call.answer()
    
@user_router.callback_query(F.data == "start_worker")
async def start_worker_handler(call: types.CallbackQuery):
    await tm.start_client_task(call.from_user.id)
    await worker_menu(call, FSMContext(storage=storage, key=call.from_user.id, bot=bot))
    await call.answer("🚀 Worker запускается...", show_alert=False)

@user_router.callback_query(F.data == "stop_worker")
async def stop_worker_handler(call: types.CallbackQuery):
    await tm.stop_worker(call.from_user.id)
    await worker_menu(call, FSMContext(storage=storage, key=call.from_user.id, bot=bot))
    await call.answer("🛑 Worker остановлен.", show_alert=False)
    
@user_router.callback_query(F.data == "change_account")
async def change_account_handler(call: types.CallbackQuery, state: FSMContext):
    # Логика смены аккаунта
    await tm.stop_worker(call.from_user.id)
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("📲 **Смена аккаунта**\n\nДля повторной авторизации введите **номер телефона** (формат +79xxxxxxxx):")
    await call.answer()

# --- Ввод промокода ---
@user_router.callback_query(F.data == "enter_promo")
async def ask_for_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code) 
    await call.message.edit_text("🔑 Введите ваш промокод для активации подписки:")
    await call.answer()
    
@user_router.message(PromoStates.waiting_for_code)
async def process_promo_activation(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    user_id = message.from_user.id
    
    if not re.match(r'^[A-Z0-9]{4,20}$', code):
        await message.answer("❌ Неверный формат промокода. Код должен содержать от 4 до 20 букв (A-Z) и цифр.")
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
        await cmd_start(message, state)
    else:
        await message.answer("❌ **Промокод недействителен** (истек, использован или не найден). Попробуйте еще раз или вернитесь в главное меню /start.")

# --- Возврат в главное меню ---
@user_router.callback_query(F.data == "start_menu")
async def back_to_start_menu(call: types.CallbackQuery, state: FSMContext):
    await cmd_start(call.message, state)
    await call.answer()

# =========================================================================
# VII. ADMIN PANEL (Полный набор хэндлеров)
# =========================================================================

# --- Админ-меню (admin_stats) ---
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
    
# --- МЕНЮ ПРОМОКОДОВ (admin_promo_menu, admin_create_promo, view, delete) ---

@admin_router.callback_query(F.data == "admin_promo_menu")
async def admin_promo_menu(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    await state.clear()
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Создать промокод", callback_data="admin_create_promo"))
    builder.row(InlineKeyboardButton(text="📜 Показать все коды", callback_data="admin_view_promo"))
    builder.row(InlineKeyboardButton(text="🔙 В админ-меню", callback_data="admin_stats"))
    
    await call.message.edit_text("🔑 **Управление промокодами**", reply_markup=builder.as_markup())
    await call.answer()

@admin_router.callback_query(F.data == "admin_create_promo")
async def admin_create_promo_step1(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    
    await state.set_state(AdminStates.waiting_for_promo_code_creation)
    await call.message.edit_text("📝 Введите **уникальный** промокод (только A-Z, 0-9):")
    await call.answer()

@admin_router.message(AdminStates.waiting_for_promo_code_creation)
async def admin_create_promo_step2(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    
    if not re.match(r'^[A-Z0-9]{4,20}$', code):
        return await message.answer("❌ Код должен содержать от 4 до 20 букв (A-Z) и цифр. Попробуйте снова:")
        
    await state.update_data(new_promo_code=code)
    await state.set_state(AdminStates.waiting_for_promo_days)
    await message.answer("🗓️ Введите количество **дней** подписки (целое число):")

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
            f"✅ **Промокод '{code}' успешно создан!**\n"
            f"Длительность: **{days}** дней.\n"
            f"Использований: **{'Бесконечно' if uses == 0 else uses}**."
        )
        await message.answer(text)
    else:
        await message.answer(f"❌ Ошибка. Промокод **'{code}'** уже существует.")
        
    # Возврат в меню промокодов
    await admin_promo_menu(message, state)

@admin_router.callback_query(F.data == "admin_view_promo")
async def admin_view_promo(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Доступ запрещен", show_alert=True)
    
    promos = await db.get_all_promo_codes()
    
    if not promos:
        await call.message.edit_text("📜 **Промокоды:**\n\nНет активных промокодов.")
        return await call.answer()

    text = "📜 **Активные промокоды (последние 10)**:\n\n"
    
    for promo in promos[:10]:
        uses_str = '∞' if promo['uses_left'] == 0 else promo['uses_left']
        text += f"**{promo['code']}** ({promo['days']}д.) — Осталось: {uses_str}\n"

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
        await call.message.answer("Нет промокодов для удаления.")
        return await admin_promo_menu(call, state)

    for promo in promos:
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

# --- ВЫДАЧА ПОДПИСКИ ВРУЧНУЮ ---

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

    await state.clear()
    
    # 30 дней по умолчанию для ручной выдачи
    days = 30
    
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
    
    # Пытаемся уведомить пользователя
    try:
        await bot.send_message(user_id_to_sub, "🎉 **Администратор продлил Вашу подписку!**")
    except Exception:
        pass
        
    await admin_main_menu(message, state)
    
# =========================================================================
# VIII. DROPS HANDLERS (Добавленный роутер drops_router)
# =========================================================================

# --- Меню управления дропами ---
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
    # Здесь могут быть кнопки для детального просмотра или смены статуса
    builder.row(InlineKeyboardButton(text="🔍 Показать/Сменить статус", callback_data="drops_view_status"))
    builder.row(InlineKeyboardButton(text="🔙 В админ-меню", callback_data="admin_stats"))
    
    await call.message.edit_text(text, reply_markup=builder.as_markup())
    await call.answer()

# --- Хэндлер для детального просмотра / смены статуса (Заглушка) ---
@drops_router.callback_query(F.data == "drops_view_status")
async def drops_view_status(call: types.CallbackQuery):
    # Эта логика будет сложной и требует отдельного FSM для ввода номера телефона дропа
    await call.answer("🛠️ Детальное управление дропами пока не реализовано.", show_alert=True)

# =========================================================================
# IX. CLEANUP & SHUTDOWN
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
            if f.endswith('_temp.session'): 
                file_path = os.path.join(SESSION_DIR, f)
                try:
                    if await asyncio.to_thread(os.path.exists, file_path) and \
                       (now - datetime.fromtimestamp(await asyncio.to_thread(os.path.getctime, file_path))) > timedelta(hours=1):
                        await asyncio.to_thread(os.remove, file_path)
                        logger.info(f"Removed old temp session: {f}")
                except Exception as e:
                    logger.warning(f"Failed to remove temp session {f}: {e}")

async def on_startup(dispatcher: Dispatcher):
    if not BOT_TOKEN or API_ID == 0 or not API_HASH:
        logger.critical("❌ КРИТИЧЕСКАЯ ОШИБКА КОНФИГУРАЦИИ: Проверьте BOT_TOKEN, API_ID, API_HASH.")
        raise SystemExit(1)
        
    logger.info("Bot started and configuration validated.")

async def on_shutdown(dispatcher: Dispatcher):
    logger.info("Shutting down workers and connections...")
    
    async with store.lock:
        workers_to_stop = list(store.active_workers.keys())
    
    shutdown_tasks = [tm.stop_worker(uid) for uid in workers_to_stop]
    if shutdown_tasks:
        await asyncio.wait(shutdown_tasks, timeout=5)
        
    logger.info("Telethon clients disconnected.")

@dp.errors()
async def errors_handler(exception: Exception, event: types.Update, data: dict):
    logger.error(f"Global Error Catch: {exception.__class__.__name__}: {exception}", exc_info=True)
    
    if isinstance(exception, TelegramForbiddenError):
        return True
    
    if ADMIN_ID and ADMIN_ID != 0:
        error_msg = f"🔥 **КРИТИЧЕСКИЙ КРАШ BOT**\n\n**Тип:** `{exception.__class__.__name__}`\n**Update:** `{event.update_id}`\n\n**Traceback:**\n`{traceback.format_exc()[:1500]}...`"
        try:
            await bot.send_message(ADMIN_ID, error_msg, parse_mode='Markdown')
        except:
            pass
            
    return True 

# =========================================================================
# X. MAIN
# =========================================================================

async def main():
    await db.init()
    
    dp.update.middleware(RateLimitMiddleware())
    
    dp.include_router(user_router)
    dp.include_router(drops_router)
    dp.include_router(admin_router)
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Resume workers
    active_ids = await db.get_active_telethon_users()
    for uid in active_ids:
        if await db.check_subscription(uid):
            asyncio.create_task(tm.start_client_task(uid)) 

    asyncio.create_task(cleanup_temp_sessions())

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
        traceback.print_exc()
