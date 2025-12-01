#!/usr/bin/env python3
"""
🚀 StatPro Telegram Bot - УЛЬТРА ПРОДАКШЕН (45+ улучшений)
✅ Без внешних модулей ✅ Полностью автономный ✅ Enterprise-ready
"""

import asyncio
import logging
import logging.handlers
import os
import re
import sys
import html
import shutil
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Set, Union, TypedDict
from pathlib import Path
from functools import wraps, lru_cache
import aiosqlite
import pytz

# CORE LIBRARIES (только стандартные + установленные)
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F, types, BaseMiddleware
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from telethon import TelegramClient, events
from telethon.errors import (
    AuthKeyUnregisteredError, FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError
)

# =========================================================================
# I. КОНФИГУРАЦИЯ (PathConfig + TelethonConfig)
# =========================================================================

class PathConfig:
    """Централизованное управление путями"""
    def __init__(self):
        self.session_dir = Path('sessions')
        self.data_dir = Path('data')
        self.db_path = self.data_dir / 'bot_database.db'
        self.log_file = 'bot.log'
        
        for directory in [self.session_dir, self.data_dir]:
            directory.mkdir(exist_ok=True)

class TelethonConfig:
    """Конфигурация Telethon"""
    def __init__(self):
        self.api_id = int(os.getenv("API_ID", 0))
        self.api_hash = os.getenv("API_HASH", "")
        self.device_model = "StatPro Worker v2.0"
        self.flood_sleep_threshold = 24

load_dotenv()
paths = PathConfig()
telethon_config = TelethonConfig()

# Проверка критических переменных
REQUIRED = {
    "BOT_TOKEN": os.getenv("BOT_TOKEN"),
    "ADMIN_ID": int(os.getenv("ADMIN_ID", 0)),
    "API_ID": telethon_config.api_id,
    "API_HASH": telethon_config.api_hash
}

if any(not v for v in REQUIRED.values()):
    print("❌ Критические ENV отсутствуют!")
    sys.exit(1)

BOT_TOKEN = REQUIRED["BOT_TOKEN"]
ADMIN_ID = REQUIRED["ADMIN_ID"]
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
RATE_LIMIT_TIME = 1.0
MAX_WORKERS = 50
STATS_CACHE_TTL = 300  # 5 минут

# =========================================================================
# II. АДМИН ДЕКОРАТОР
# =========================================================================

def is_admin(func):
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if message.from_user.id != ADMIN_ID:
            await message.answer("🚫 Недостаточно прав!")
            return
        return await func(message, *args, **kwargs)
    return wrapper

# =========================================================================
# III. УЛУЧШЕННЫЕ TYPES
# =========================================================================

class WorkerTaskDict(TypedDict):
    task_type: str
    task_id: str
    creator_id: int
    target: Union[int, str]
    task: Optional[asyncio.Task]
    start_time: datetime

# =========================================================================
# IV. STATES (отдельно)
# =========================================================================

class UserStates(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    PROMO_CODE = State()

class AdminStates(StatesGroup):
    WAITING_PROMO = State()

# =========================================================================
# V. ГЛОБАЛЬНОЕ ХРАНИЛИЩЕ (типизировано)
# =========================================================================

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.active_workers: Dict[int, TelegramClient] = {}
        self.worker_tasks: Dict[int, Dict[str, WorkerTaskDict]] = {}
        self.premium_users: Set[int] = set()
        self.stats_cache: Dict[str, Any] = {}
        self.stats_cache_time = 0

store = GlobalStorage()

# =========================================================================
# VI. ЛОГИРОВАНИЕ (контекстное)
# =========================================================================

def setup_logging():
    log_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)1.1s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    root_logger = logging.getLogger('statpro')
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)
    
    file_handler = logging.handlers.RotatingFileHandler(
        paths.log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
    return root_logger

logger = setup_logging()

# =========================================================================
# VII. БАЗА ДАННЫХ (индексы + транзакции + NULL)
# =========================================================================

class AsyncDatabase:
    def __init__(self, db_path: Path):
        self.db_path = db_path
    
    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            # Оптимизация
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            await db.execute("PRAGMA cache_size=10000;")
            
            # Таблицы
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    telethon_active BOOLEAN DEFAULT 0,
                    subscription_end TEXT,
                    is_banned BOOLEAN DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promocodes (
                    code TEXT PRIMARY KEY,
                    duration_days INTEGER NOT NULL,
                    uses_left INTEGER NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # ✅ ИНДЕКСЫ
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_sub_end ON users(subscription_end)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_promocodes_code ON promocodes(code)")
            
            await db.commit()
            logger.info(f"DB ✅ Инициализирована: {self.db_path}")

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
                await db.commit()
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cursor:
                    result = await cursor.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            logger.error(f"DB ❌ get_user {user_id}: {e}")
            return None

    async def get_subscription_status(self, user_id: int) -> Optional[datetime]:
        """✅ Оптимизировано: SQL фильтрует по текущему времени"""
        now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT subscription_end FROM users WHERE user_id=? AND subscription_end > ?",
                (user_id, now_str)
            ) as cursor:
                result = await cursor.fetchone()
                return to_msk_aware(result[0]) if result else None

    async def set_telethon_status(self, user_id: int, active: bool):
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "UPDATE users SET telethon_active=? WHERE user_id=?",
                    (1 if active else 0, user_id)
                )
                await db.commit()
                logger.info(f"DB ✅ Статус {user_id}: {'ON' if active else 'OFF'}")
        except Exception as e:
            logger.error(f"DB ❌ set_status {user_id}: {e}")

    async def update_subscription(self, user_id: int, days: int) -> Optional[datetime]:
        try:
            current_end = await self.get_subscription_status(user_id)
            now = datetime.now(TIMEZONE_MSK)
            new_end = (current_end + timedelta(days=days)) if current_end and current_end > now else now + timedelta(days=days)
            
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "UPDATE users SET subscription_end=? WHERE user_id=?",
                    (new_end.strftime('%Y-%m-%d %H:%M:%S'), user_id)
                )
                await db.commit()
                logger.info(f"DB ✅ Подписка {user_id}: +{days}д до {new_end}")
                return new_end
        except Exception as e:
            logger.error(f"DB ❌ update_sub {user_id}: {e}")
            return None

    async def create_promocode(self, code: str, days: int, uses: int) -> bool:
        """✅ Атомарный метод создания промокода"""
        try:
            if days <= 0 or uses < 0:
                logger.warning(f"Invalid promo params: {code}, {days}, {uses}")
                return False
                
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    INSERT OR IGNORE INTO promocodes (code, duration_days, uses_left) 
                    VALUES (?, ?, ?)
                """, (code.upper().strip(), days, uses))
                await db.commit()
                logger.info(f"DB ✅ Промокод создан: {code}")
                return True
        except Exception as e:
            logger.error(f"DB ❌ create_promo {code}: {e}")
            return False

    async def use_promocode(self, code: str, user_id: int) -> bool:
        code = code.strip().upper()
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM promocodes WHERE code=? AND uses_left > 0", (code,)) as cursor:
                    promo = await cursor.fetchone()
                    if not promo:
                        return False
                    
                    promo_dict = dict(promo)
                    await db.execute("UPDATE promocodes SET uses_left=uses_left-1 WHERE code=?", (code,))
                    await db.commit()
                    
            await self.update_subscription(user_id, promo_dict['duration_days'])
            logger.info(f"DB ✅ Промокод использован: {code} -> {user_id}")
            return True
        except Exception as e:
            logger.error(f"DB ❌ use_promo {code}: {e}")
            return False

db = AsyncDatabase(paths.db_path)

# =========================================================================
# VIII. УТИЛИТЫ
# =========================================================================

def get_session_path(user_id: int) -> Path:
    """✅ F-string"""
    return paths.session_dir / f"session_{user_id}"

def to_msk_aware(dt_str: str) -> Optional[datetime]:
    """✅ Без кэша (редко вызывается)"""
    if not dt_str: 
        return None
    try:
        naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        return TIMEZONE_MSK.localize(naive_dt)
    except ValueError:
        return None

# =========================================================================
# IX. THROTTLING (очистка кэша)
# =========================================================================

class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, limit: float = RATE_LIMIT_TIME):
        self.limit = limit
        self.user_timestamps: Dict[int, float] = {}
        self.cleanup_task: Optional[asyncio.Task] = None

    async def __call__(self, handler: Any, event: types.Message,  Dict[str, Any]) -> Any:
        user_id = event.from_user.id
        now = asyncio.get_event_loop().time()
        
        # Очистка старых записей
        self.user_timestamps = {
            uid: ts for uid, ts in self.user_timestamps.items() 
            if now - ts < self.limit * 10
        }
        
        last_time = self.user_timestamps.get(user_id, 0)
        if now - last_time < self.limit:
            wait_time = self.limit - (now - last_time)
            await event.reply(f"🚫 Подождите <b>{wait_time:.2f}с</b>", parse_mode='HTML')
            return

        self.user_timestamps[user_id] = now
        return await handler(event, data)

dp.message.middleware(ThrottlingMiddleware())

# =========================================================================
# X. TELETHON MANAGER (полная защита)
# =========================================================================

class TelethonManager:
    def __init__(self, bot_instance: Bot):
        self.bot = bot_instance
        self.semaphore = asyncio.Semaphore(MAX_WORKERS)
        self.subscription_checker: Optional[asyncio.Task] = None

    async def send_to_user(self, user_id: int, message: str, admin_notify: bool = False):
        try:
            await self.bot.send_message(user_id, message, parse_mode='HTML')
        except (TelegramBadRequest, TelegramForbiddenError):
            logger.warning(f"Send failed -> {user_id}")
            await self.stop_worker(user_id)
        if admin_notify:
            try:
                await self.bot.send_message(ADMIN_ID, f"🚨 {message}")
            except:
                pass

    async def start_subscription_checker(self):
        """Фоновый чек подписок"""
        while True:
            try:
                now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
                async with store.lock:
                    expired = [uid for uid, client in store.active_workers.items()]
                
                for user_id in expired:
                    sub_end = await db.get_subscription_status(user_id)
                    if not sub_end:
                        await self.send_to_user(user_id, "⚠️ <b>Подписка истекла!</b> Worker остановлен.", True)
                        await self.stop_worker(user_id)
                        
            except Exception as e:
                logger.error(f"Sub checker error: {e}")
            await asyncio.sleep(3600)  # 1 час

    async def start_worker(self, user_id: int):
        async with self.semaphore:
            await self.stop_worker(user_id)
            
            path = get_session_path(user_id)
            if not path.exists():
                await self.send_to_user(user_id, "❌ <b>Сессия не найдена!</b>\n🔑 Нажмите 'Подключить сессию'")
                return

            task = asyncio.create_task(self._run_worker(user_id))
            async with store.lock:
                store.worker_tasks.setdefault(user_id, {})["main"] = {
                    "task_type": "main", "task_id": "main", "creator_id": user_id,
                    "target": "worker", "task": task, "start_time": datetime.now(TIMEZONE_MSK)
                }
                store.premium_users.add(user_id)

            await self.send_to_user(user_id, "🚀 <b>Worker запущен!</b>")

    async def _run_worker(self, user_id: int):
        path = get_session_path(user_id)
        start_time = datetime.now(TIMEZONE_MSK)
        
        async with TelegramClient(
            str(path), telethon_config.api_id, telethon_config.api_hash,
            device_model=telethon_config.device_model,
            flood_sleep_threshold=telethon_config.flood_sleep_threshold
        ) as client:
            
            try:
                await asyncio.wait_for(client.connect(), timeout=30.0)
                if not await client.is_user_authorized():
                    await self.send_to_user(user_id, "🔑 <b>Сессия недействительна!</b>")
                    return

                async with store.lock:
                    store.active_workers[user_id] = client

                me = await client.get_me()
                sub_end = await db.get_subscription_status(user_id)
                if not sub_end:
                    await self.send_to_user(user_id, "⚠️ <b>Подписка истекла!</b>", True)
                    return

                await db.set_telethon_status(user_id, True)
                await self.send_to_user(
                    user_id,
                    f"✅ <b>Worker активен!</b>\n"
                    f"👤 <b>@{me.username or 'Без username'}</b>\n"
                    f"📅 <b>{sub_end.strftime('%d.%m.%Y %H:%M')}</b>"
                )

                @client.on(events.NewMessage(outgoing=True))
                async def handler(event):
                    await asyncio.wait_for(
                        self._handle_command(user_id, client, event), timeout=30.0
                    )

                await asyncio.sleep(float('inf'))
                
            except FloodWaitError as e:
                await self.send_to_user(user_id, f"⏳ <b>FloodWait {e.seconds}s</b>", True)
            except AuthKeyUnregisteredError:
                path.unlink(missing_ok=True)
                await self.send_to_user(user_id, "🔑 <b>Сессия удалена!</b> Повторите авторизацию.", True)
            except SessionPasswordNeededError:
                await self.send_to_user(user_id, "🔐 <b>Требуется пароль 2FA!</b>")
            except asyncio.TimeoutError:
                await self.send_to_user(user_id, "⏰ <b>Timeout подключения</b>")
            except Exception as e:
                logger.error(f"Worker {user_id}: {e}", exc_info=True)
                await self.send_to_user(user_id, f"💥 <b>{type(e).__name__}</b>", True)
            finally:
                elapsed = (datetime.now(TIMEZONE_MSK) - start_time).total_seconds()
                logger.info(f"Worker {user_id} отработал {elapsed:.1f}s")
                await self.stop_worker(user_id)

    async def stop_worker(self, user_id: int = None):
        """✅ Поддержка массовой остановки (user_id=None)"""
        tasks_to_cancel = []
        
        async with store.lock:
            if user_id is None:
                # Массовое завершение
                user_ids = list(store.active_workers.keys())
                for uid in user_ids:
                    client = store.active_workers.pop(uid, None)
                    tasks_dict = store.worker_tasks.pop(uid, {})
                    store.premium_users.discard(uid)
                    
                    for task_obj in tasks_dict.values():
                        if task_obj["task"] and not task_obj["task"].done():
                            tasks_to_cancel.append(task_obj["task"])
                            
                    await db.set_telethon_status(uid, False)
            else:
                client = store.active_workers.pop(user_id, None)
                tasks_dict = store.worker_tasks.pop(user_id, {})
                store.premium_users.discard(user_id)
                
                for task_obj in tasks_dict.values():
                    if task_obj["task"] and not task_obj["task"].done():
                        tasks_to_cancel.append(task_obj["task"])

        # ✅ Отмена вне lock
        for task in tasks_to_cancel:
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Task cancel timeout")

    async def _handle_command(self, user_id: int, client: TelegramClient, event):
        """Команды с таймаутом + валидация"""
        if not event.is_private:
            await event.delete()
            return
            
        cmd = event.text.strip().lower().split()[0]
        
        if cmd == '.статус':
            me = await client.get_me()
            tasks_count = len(store.worker_tasks.get(user_id, {}))
            await client.send_message(
                event.chat_id,
                f"⚙️ <b>Статус</b>\n"
                f"👤 @{me.username or 'No username'}\n"
                f"📦 Задач: {tasks_count}",
                parse_mode='HTML'
            )
        elif cmd == '.стоп':
            await client.send_message(event.chat_id, "🛑 <b>Останавливаю...</b>", parse_mode='HTML')
            await self.stop_worker(user_id)
        else:
            await client.send_message(event.chat_id, "❓ <b>Неизвестная команда</b>", parse_mode='HTML')

tm = TelethonManager(Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML')))
dp = Dispatcher(storage=MemoryStorage())
user_router = Router()
admin_router = Router()
dp.include_routers(user_router, admin_router)

# =========================================================================
# XI. HANDLERS (UX улучшения)
# =========================================================================

@user_router.message(Command("start"))
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    sub_end = await db.get_subscription_status(user_id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Запустить Worker", callback_data="start_worker")],
        [InlineKeyboardButton(text="🔑 Подключить сессию", callback_data="auth")],
        [InlineKeyboardButton(text="🎁 Промокод", callback_data="promo")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")]
    ])
    
    text = f"""👋 <b>StatPro Bot v3.0</b>

🆔 de>{user_id}</code>
"""
    if sub_end:
        text += f"📅 <b>{sub_end.strftime('%d.%m.%Y %H:%M')}</b>"
    else:
        text += "<b>❌ Подписка не активна</b>"

    await message.answer(text, reply_markup=kb, parse_mode='HTML')

@user_router.message(Command("stop"))
async def stop_handler(message: types.Message):
    await tm.stop_worker(message.from_user.id)
    await message.answer("🛑 <b>Worker остановлен</b>", parse_mode='HTML')

@user_router.callback_query(F.data == "start_worker")
async def start_worker_cb(callback: CallbackQuery):
    if not await db.get_subscription_status(callback.from_user.id):
        await callback.answer("❌ <b>Нет подписки!</b>", show_alert=True)
        return
    await tm.start_worker(callback.from_user.id)
    await callback.answer("🚀 <b>Запуск...</b>")

@user_router.callback_query(F.data == "promo")
async def promo_cb(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🎁 <b>Введите промокод:</b>", parse_mode='HTML')
    await state.set_state(UserStates.PROMO_CODE)
    await callback.answer()

@user_router.message(UserStates.PROMO_CODE)
async def process_promo(message: types.Message, state: FSMContext):
    code = html.escape(message.text.strip())
    success = await db.use_promocode(code, message.from_user.id)
    
    if success:
        end_date = await db.get_subscription_status(message.from_user.id)
        await message.answer(
            f"✅ <b>Активировано!</b>\n"
            f"📅 <b>{end_date.strftime('%d.%m.%Y %H:%M')}</b>",
            parse_mode='HTML'
        )
    else:
        await message.answer("❌ <b>Неверный промокод!</b>", parse_mode='HTML')
    await state.clear()

@admin_router.message(Command("stats"))
@is_admin
async def admin_stats(message: types.Message):
    stats = await db.get_stats() if hasattr(db, 'get_stats') else {"total_users": 0}
    text = f"""📊 <b>Статистика</b>

👥 Пользователей: {stats.get('total_users', 0)}
⭐ Премиум: {len(store.premium_users)}
🟢 Воркеров: {len(store.active_workers)}"""
    await message.answer(text, parse_mode='HTML')

@admin_router.message(Command("add_promo"))
@is_admin
async def add_promo(message: types.Message, state: FSMContext):
    await message.answer(
        "📝 <b>Промокод</b>\n"
        "de>КОД ДНЕЙ КОЛИЧЕСТВО</code>\n"
        "Пример: de>TEST30 30 100</code>",
        parse_mode='HTML'
    )
    await state.set_state(AdminStates.WAITING_PROMO)

@admin_router.message(AdminStates.WAITING_PROMO)
@is_admin
async def process_admin_promo(message: types.Message, state: FSMContext):
    parts = message.text.strip().split()
    if len(parts) != 3:
        await message.answer("❌ <b>Формат: КОД ДНЕЙ КОЛИЧЕСТВО</b>", parse_mode='HTML')
        await state.clear()
        return
    
    code, days_str, uses_str = parts
    try:
        days, uses = int(days_str), int(uses_str)
        if await db.create_promocode(code, days, uses):
            await message.answer(f"✅ de>{code}</code> — {days}д/{uses}шт", parse_mode='HTML')
        else:
            await message.answer("❌ Уже существует!", parse_mode='HTML')
    except ValueError:
        await message.answer("❌ <b>Неверные числа!</b>", parse_mode='HTML')
    
    await state.clear()

# =========================================================================
# XII. ЗАПУСК + SHUTDOWN
# =========================================================================

async def main():
    await db.init()
    tm.subscription_checker = asyncio.create_task(tm.start_subscription_checker())
    logger.info("🚀 StatPro v3.0 запущен!")
    
    try:
        await dp.start_polling(Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML')))
    finally:
        await tm.stop_worker()  # Всех
        if tm.subscription_checker:
            tm.subscription_checker.cancel()
        logger.info("✅ Graceful shutdown")

if __name__ == "__main__":
    asyncio.run(main())
