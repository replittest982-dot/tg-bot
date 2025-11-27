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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Объявление bot и dp
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())
user_router = Router()
drops_router = Router()

# =========================================================================
# II. ГЛОБАЛЬНЫЕ ХРАНИЛИЩА И УТИЛИТЫ
# =========================================================================

class GlobalStorage:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.temp_auth_clients: Dict[int, TelegramClient] = {}
        self.process_progress: Dict[int, Dict] = {} # {user_id: {'type': 'flood', 'stop': False}}
        self.last_user_request: Dict[int, datetime] = {}
        self.pc_monitoring: Dict[int, str] = {} # {topic_id: pc_name}
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

# --- УТИЛИТЫ ---
def get_session_path(user_id, is_temp=False):
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def get_current_time_msk() -> datetime:
    return datetime.now(TIMEZONE_MSK)

def to_msk_aware(dt_str: str) -> datetime:
    naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    return TIMEZONE_MSK.localize(naive_dt)

def get_topic_name_from_message(message: types.Message) -> Optional[str]:
    """Пытается получить имя ПК/топика из сообщения."""
    if message.chat.id == DROPS_CHAT_ID and message.message_thread_id:
        topic_id = message.message_thread_id
        return store.pc_monitoring.get(topic_id)
    return None

def rate_limit(limit: float):
    def decorator(func):
        @wraps(func)
        async def wrapper(message: types.Message, *args, **kwargs):
            user_id = message.from_user.id
            now = get_current_time_msk()
            
            async with store.lock:
                last = store.last_user_request.get(user_id)
                if last and (now - last).total_seconds() < limit:
                    return
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
            await db.commit()

    async def get_user(self, user_id):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
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
            return end > now
        except:
            return False

    async def set_telethon_status(self, user_id, status):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            await db.commit()

    async def get_active_telethon_users(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM users WHERE telethon_active=1") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
                
    async def get_drop_session(self, phone):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions WHERE phone=?", (phone,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_drop_session_by_drop_id(self, drop_id):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM drop_sessions WHERE drop_id=? AND status NOT IN ('closed', 'deleted', 'замена_закрыт') ORDER BY start_time DESC LIMIT 1", (drop_id,)) as cursor:
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

        if old_session['status'] in ('дайте номер', 'error', 'slet', 'замена', 'повтор') and new_status == 'в работе':
            duration = (now - last_time).total_seconds()
            prosto_seconds += int(duration)

        async with aiosqlite.connect(self.db_path) as db:
            if new_phone and new_phone != phone:
                await db.execute("UPDATE drop_sessions SET status='замена_закрыт' WHERE phone=?", (phone,))
                await db.execute(
                    "INSERT INTO drop_sessions (phone, pc_name, drop_id, status, start_time, last_status_time, prosto_seconds) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (new_phone, old_session['pc_name'], old_session['drop_id'], new_status, old_session['start_time'], now_str, prosto_seconds)
                )
            else:
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
        
    async def _send_to_bot_user(self, user_id, message):
        try:
            await self.bot.send_message(user_id, message) 
        except (TelegramForbiddenError, TelegramBadRequest, UserIsBlockedError):
            logger.error(f"Cannot send message to {user_id}. Stopping worker.")
            await self.stop_worker(user_id)
        except Exception as e:
            logger.error(f"Unknown error sending message to {user_id}: {e}")

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

        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            await self.worker_message_handler(user_id, client, event)

        try:
            await client.start()
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

        msg = event.text.strip()
        parts = msg.split()
        cmd = parts[0].lower()

        await event.delete() 
        
        # --- .ПKСТАРТ ---
        if cmd == '.пкстарт':
            try:
                pc_name = parts[1] if len(parts) > 1 else 'PC'
                # Исправлено: используем message_thread_id, если есть, иначе chat_id
                topic_id = event.message.message_thread_id or event.chat_id
                
                async with store.lock:
                    store.pc_monitoring[topic_id] = pc_name
                
                temp = await client.send_message(event.chat_id, f"✅ Имя ПК для топика **{topic_id}** установлено как **{pc_name}**.", reply_to=event.message.id)
                await asyncio.sleep(2)
                await temp.delete()
            except Exception as e:
                logger.error(f"PC start error: {e}")
                
        # --- .ФЛУД ---
        elif cmd == '.флуд':
            try:
                if len(parts) < 3: return
                count = int(parts[1])
                # Проверяем, является ли последний элемент числом (delay)
                delay_str = parts[-1]
                if delay_str.replace('.', '', 1).isdigit():
                    delay = float(delay_str)
                    text = " ".join(parts[2:-1])
                else:
                    delay = 0.5
                    text = " ".join(parts[2:])

                chat = event.chat_id
                
                async with store.lock:
                    store.process_progress[user_id] = {'type': 'flood', 'stop': False}
                
                task = asyncio.create_task(self._flood_task(client, chat, text, count, delay, user_id))
                async with store.lock:
                    store.worker_tasks.setdefault(user_id, []).append(task)

                temp = await client.send_message(chat, "🚀 Флуд запущен. Для остановки введите `.стопфлуд`")
                await asyncio.sleep(2)
                await temp.delete()
            except Exception as e:
                logger.error(f"Flood setup error: {e}")

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

        # --- .ЛС, .ЧЕКГРУППУ (Заглушки) ---
        elif cmd == '.лс': await client.send_message(event.chat_id, "🚧 Команда `.лс` в разработке.")
        elif cmd == '.чекгруппу': await client.send_message(event.chat_id, "🚧 Команда `.чекгруппу` в разработке.")

    async def _flood_task(self, client, chat, text, count, delay, user_id):
        i = 0
        while i < count or count == 0:
            async with store.lock:
                if store.process_progress.get(user_id, {}).get('stop'):
                    break
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
# V. AIOGRAM HANDLERS (USER & DROPS)
# =========================================================================

# --- USER HANDLERS ---
@user_router.message(Command('start'))
@rate_limit(RATE_LIMIT_TIME)
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await db.get_user(message.from_user.id) 
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Вход (Номер)", callback_data="auth_phone")],
        [InlineKeyboardButton(text="❓ Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}")]
    ])
    await message.answer("👋 Добро пожаловать в STATPRO Worker!", reply_markup=kb)

@user_router.callback_query(F.data == "auth_phone")
async def auth_phone_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("Введите номер телефона (+7...):")

@user_router.message(TelethonAuth.PHONE)
async def auth_phone_input(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    user_id = message.from_user.id
    path = get_session_path(user_id, is_temp=True)
    await db.get_user(user_id) 
    
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
    temp_path = get_session_path(user_id, True) + '.session'

    try:
        await client.sign_in(data['phone'], message.text.strip(), phone_code_hash=data['hash'])
        
        await client.disconnect() # Исправлено: дисконнект перед переименованием
        
        real_path = get_session_path(user_id) + '.session'
        if os.path.exists(temp_path):
            os.rename(temp_path, real_path)
            
        await db.set_telethon_status(user_id, True)
        await tm.start_client_task(user_id)
        await state.clear()
        await message.answer("✅ Успешный вход!")
    except PermissionError:
        await message.answer("⚠️ Ошибка: Клиент не смог закрыть соединение. Попробуйте снова.")
    except Exception as e:
        await message.answer(f"❌ Ошибка входа: {e.__class__.__name__}")


# --- DROPS HANDLERS (Полностью восстановлена логика) ---

async def handle_drop_status_change(message: types.Message, state: FSMContext, new_status: str, is_change_phone: bool = False):
    """Общая функция для обработки всех команд смены статуса."""
    drop_id = message.from_user.id
    current_session = await db.get_drop_session_by_drop_id(drop_id)

    if not current_session:
        return await message.reply("❌ **Ошибка:** Нет активной сессии для вашего ID. Начните с `/numb`.")
    
    phone = current_session['phone']
    pc_name = get_topic_name_from_message(message) or current_session['pc_name']

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
                if os.path.exists(file_path) and (now - datetime.fromtimestamp(os.path.getctime(file_path)) > timedelta(hours=1)):
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        logger.warning(f"Failed to remove temp session {f}: {e}")

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
    dp.shutdown.register(on_shutdown)

    # Resume workers (Исправлено: НЕ await!)
    active_ids = await db.get_active_telethon_users()
    for uid in active_ids:
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
        logger.critical(f"Critical error in main: {e}")
        traceback.print_exc()
