import asyncio
import logging
import os
import sqlite3
import pytz
import re
import io
import random
import string
from datetime import datetime, timedelta
from typing import Dict, Union, Optional
from functools import wraps

# =========================================================================
# I. КОНФИГУРАЦИЯ И НАСТРОЙКА
# =========================================================================

# --- ВАШИ КЛЮЧИ ---
BOT_TOKEN = "7868097991:AAFJb7pNRfr_FPDxigk7GqlCr1AryXTrcYY"
ADMIN_ID = 6256576302
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1"

# --- НАСТРОЙКИ ---
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
DB_TIMEOUT = 10
RATE_LIMIT_TIME = 1  # Задержка между командами от одного пользователя, сек

# --- ПУТИ ---
DATA_DIR = 'data'
SESSION_DIR = 'sessions'

if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
if not os.path.exists(SESSION_DIR): os.makedirs(SESSION_DIR)
DB_PATH = os.path.join(DATA_DIR, DB_NAME)

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- ИМПОРТЫ ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, StateFilter

from telethon import TelegramClient, events
from telethon.tl.types import User
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneNumberInvalidError, PhoneCodeExpiredError,
    PasswordHashInvalidError, AuthKeyUnregisteredError, RpcCallFailError
)
from telethon.utils import get_display_name

# --- ИНИЦИАЛИЗАЦИЯ AIOGRAM ---
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, parse_mode='HTML')
dp = Dispatcher(storage=storage)
user_router = Router()

# =========================================================================
# II. ХРАНИЛИЩА И УТИЛИТЫ
# =========================================================================

# --- ГЛОБАЛЬНЫЕ ХРАНИЛИЩА ---
# (Перенесены из глобальной области в класс TelethonManager, кроме FSM)
TEMP_AUTH_CLIENTS: Dict[int, 'TelegramClient'] = {}
PROCESS_PROGRESS: Dict[int, Dict] = {}
LAST_USER_REQUEST: Dict[int, datetime] = {}

# --- УТИЛИТЫ FSM ---
class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    WAITING_FOR_QR_LOGIN = State()

class PromoStates(StatesGroup):
    waiting_for_code = State()

class AdminStates(StatesGroup):
    main_menu = State()
    promo_days_input = State()
    promo_uses_input = State()
    sub_user_id_input = State()
    sub_days_input = State()

def get_session_path(user_id, is_temp=False):
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def generate_promo_code(length=10):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

# --- Rate Limiting Decorator ---
def rate_limit(time_limit: float):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            user_id = args[0].from_user.id
            now = datetime.now(TIMEZONE_MSK)
            last_request = LAST_USER_REQUEST.get(user_id)
            
            if last_request and (now - last_request).total_seconds() < time_limit:
                await bot.send_message(user_id, "⚠️ Слишком частые запросы. Подождите секунду.")
                return 
            
            LAST_USER_REQUEST[user_id] = now
            return await func(*args, **kwargs)
        return wrapper
    return decorator

# =========================================================================
# III. КЛАСС: DATABASE
# =========================================================================

class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path, timeout=DB_TIMEOUT)

    def _init_db(self):
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    subscription_active BOOLEAN DEFAULT 0,
                    subscription_end_date TEXT,
                    telethon_active BOOLEAN DEFAULT 0
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    days INTEGER,
                    is_active BOOLEAN DEFAULT 1,
                    max_uses INTEGER,
                    current_uses INTEGER DEFAULT 0
            )""")
            conn.commit()

    def get_user(self, user_id):
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            conn.commit()
            cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
            row = cur.fetchone()
            if row:
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))
            return None

    def check_subscription(self, user_id):
        user = self.get_user(user_id)
        if not user or not user.get('subscription_active'): return False

        end_date_str = user.get('subscription_end_date')
        if not end_date_str: return False

        try:
            end = TIMEZONE_MSK.localize(datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S'))
            # Автоматическое сброс активности, если дата прошла (для кэша)
            if end <= datetime.now(TIMEZONE_MSK):
                self.set_subscription_status(user_id, False)
                return False
            return True
        except ValueError:
            return False

    def set_subscription_status(self, user_id, status):
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE users SET subscription_active=? WHERE user_id=?", (1 if status else 0, user_id))
            conn.commit()

    def update_subscription(self, user_id, days):
        with self._get_conn() as conn:
            cur = conn.cursor()
            user = self.get_user(user_id)
            now = datetime.now(TIMEZONE_MSK)
            current_end = user.get('subscription_end_date')
            start_date = now

            if current_end:
                try:
                    ce = TIMEZONE_MSK.localize(datetime.strptime(current_end, '%Y-%m-%d %H:%M:%S'))
                    if ce > now: start_date = ce
                except: pass
                
            new_end = (start_date + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            cur.execute("UPDATE users SET subscription_active=1, subscription_end_date=? WHERE user_id=?", (new_end, user_id))
            conn.commit()
            return new_end

    def set_telethon_status(self, user_id, status):
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
            conn.commit()

    def get_promo(self, code):
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM promo_codes WHERE code=?", (code,))
            row = cur.fetchone()
            if row:
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))
            return None

    def use_promo(self, code):
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code=?", (code,))
            conn.commit()

    def add_promo(self, code, days, max_uses):
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR REPLACE INTO promo_codes (code, days, max_uses) VALUES (?, ?, ?)", (code, days, max_uses))
            conn.commit()

    def get_active_telethon_users(self):
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
            return [row[0] for row in cur.fetchall()]

db = Database(DB_PATH)

# =========================================================================
# IV. КЛАСС: TELETHON MANAGER (Worker)
# =========================================================================

class TelethonManager:
    def __init__(self, api_id, api_hash):
        self.API_ID = api_id
        self.API_HASH = api_hash
        self.ACTIVE_CLIENTS: Dict[int, TelegramClient] = {}
        self.ACTIVE_WORKERS: Dict[int, asyncio.Task] = {}
        self.FLOOD_TASKS: Dict[int, Dict[int, asyncio.Task]] = {}

    def _get_client(self, user_id):
        return self.ACTIVE_CLIENTS.get(user_id)

    async def _send_to_bot_user(self, user_id, message, reply_markup=None):
        try:
            await bot.send_message(user_id, message, reply_markup=reply_markup)
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.error(f"Cannot send message to {user_id}: {e}")
            if user_id in self.ACTIVE_WORKERS:
                await self.stop_worker(user_id)

    async def stop_flood(self, user_id, chat_id=None):
        if user_id in self.FLOOD_TASKS:
            tasks_to_cancel = self.FLOOD_TASKS[user_id].items() if chat_id is None else [(cid, t) for cid, t in self.FLOOD_TASKS[user_id].items() if cid == chat_id]
            for cid, t in tasks_to_cancel:
                if not t.done(): t.cancel()
                del self.FLOOD_TASKS[user_id][cid]
            if not self.FLOOD_TASKS[user_id]: del self.FLOOD_TASKS[user_id]
        if user_id in PROCESS_PROGRESS: del PROCESS_PROGRESS[user_id]

    async def stop_worker(self, user_id, force_disconnect=True):
        await self.stop_flood(user_id)
        
        if user_id in self.ACTIVE_WORKERS:
            t = self.ACTIVE_WORKERS[user_id]
            if not t.done(): t.cancel()
            del self.ACTIVE_WORKERS[user_id]
            
        if user_id in self.ACTIVE_CLIENTS:
            c = self.ACTIVE_CLIENTS[user_id]
            if force_disconnect and c.is_connected():
                try: await c.disconnect()
                except: pass
            del self.ACTIVE_CLIENTS[user_id]
            
        db.set_telethon_status(user_id, False)
        logger.info(f"Worker {user_id} stopped.")

    async def start_workers_on_startup(self):
        for uid in db.get_active_telethon_users():
            self.ACTIVE_WORKERS[uid] = asyncio.create_task(self.run_worker(uid))

    async def run_worker(self, user_id):
        await self.stop_worker(user_id, force_disconnect=True)
        path = get_session_path(user_id)
        client = TelegramClient(path, self.API_ID, self.API_HASH, device_model="Android Client")
        self.ACTIVE_CLIENTS[user_id] = client

        try:
            # 1. Проверка сессии перед стартом
            if not os.path.exists(path + '.session'):
                db.set_telethon_status(user_id, False)
                await self._send_to_bot_user(user_id, "⚠️ Сессия не найдена. Авторизуйтесь снова.", reply_markup=get_main_kb(user_id))
                return

            # 2. Попытка подключения
            await client.start()
            db.set_telethon_status(user_id, True)
            await self._send_to_bot_user(user_id, "🚀 Worker успешно запущен!")
            logger.info(f"Worker {user_id} started and connected.")
            
            # 3. Внутренняя логика команд (Tasks)
            async def flood_task(peer, message, count, delay, chat_id):
                try:
                    is_unl = count <= 0
                    mx = count if not is_unl else 999999999
                    if user_id not in self.FLOOD_TASKS: self.FLOOD_TASKS[user_id] = {}
                    self.FLOOD_TASKS[user_id][chat_id] = asyncio.current_task()
                    PROCESS_PROGRESS[user_id] = {'type': 'flood', 'total': count, 'done': 0, 'peer': peer}
                    
                    for i in range(mx):
                        # Проверка отмены задачи
                        if user_id not in self.FLOOD_TASKS or chat_id not in self.FLOOD_TASKS[user_id]: break
                        
                        await client.send_message(peer, message)
                        PROCESS_PROGRESS[user_id]['done'] = i + 1
                        await asyncio.sleep(delay)
                        
                    await self._send_to_bot_user(user_id, "✅ Флуд завершен.")
                except asyncio.CancelledError:
                    await self._send_to_bot_user(user_id, "🛑 Флуд остановлен по запросу.")
                except FloodWaitError as e:
                    await self._send_to_bot_user(user_id, f"❌ Ограничение API: Слишком много запросов. Ждите {e.seconds} секунд.")
                except Exception as e: 
                    logger.error(f"Flood error for {user_id}: {e}")
                    await self._send_to_bot_user(user_id, f"❌ Критическая ошибка флуда: {e}")
                finally:
                    if user_id in self.FLOOD_TASKS and chat_id in self.FLOOD_TASKS[user_id]: del self.FLOOD_TASKS[user_id][chat_id]
                    if user_id in PROCESS_PROGRESS: del PROCESS_PROGRESS[user_id]

            async def check_group_task(event, target, mn, mx):
                try:
                    ent = await client.get_entity(target) if target else await client.get_entity(event.chat_id)
                    name = get_display_name(ent)
                    await client.send_message(user_id, f"⏳ Сканирую `{name}`...")
                    
                    users = {}
                    PROCESS_PROGRESS[user_id] = {'type': 'checkgroup', 'peer': ent, 'done_msg': 0}
                    
                    # Оптимизация: используем более быстрый метод iter_participants, если это возможно, 
                    # или iter_messages для истории (как сейчас).
                    async for msg in client.iter_messages(ent, limit=None):
                        if user_id not in PROCESS_PROGRESS: return
                        PROCESS_PROGRESS[user_id]['done_msg'] += 1
                        if msg.sender and isinstance(msg.sender, User) and msg.sender_id not in users:
                            uid = msg.sender.id
                            if (mn is None or uid >= mn) and (mx is None or uid <= mx):
                                users[uid] = msg.sender
                    
                    res = []
                    for u in users.values():
                        res.append(f"👤 {get_display_name(u)} | @{u.username if u.username else 'Нет'} | ID: {u.id}")
                    
                    full_text = f"📊 Отчет: {name}\nНайдено: {len(users)}\n\n" + "\n".join(res)
                    PROCESS_PROGRESS[user_id]['report_data'] = full_text
                    PROCESS_PROGRESS[user_id]['peer_name'] = name
                    
                    await self._send_to_bot_user(user_id, f"✅ Готово! Найдено: {len(users)}", reply_markup=get_report_choice_kb())

                except asyncio.CancelledError:
                    await self._send_to_bot_user(user_id, "🛑 Сканирование отменено.")
                except Exception as e: 
                    logger.error(f"Checkgroup error for {user_id}: {e}")
                    await client.send_message(user_id, f"❌ Критическая ошибка сканирования: {e}")
                finally:
                    if user_id in PROCESS_PROGRESS and PROCESS_PROGRESS[user_id].get('type') == 'checkgroup':
                        del PROCESS_PROGRESS[user_id]
                    
            # 4. Обработчик сообщений Telethon
            @client.on(events.NewMessage(outgoing=True))
            async def worker_handler(event):
                # Проверка подписки (для Worker'а)
                if not db.check_subscription(user_id) and user_id != ADMIN_ID: return await event.reply("❌ Подписка истекла.")
                
                msg = event.text.strip()
                parts = msg.split()
                if not parts: return
                cmd = parts[0].lower()

                if cmd == '.лс':
                    if len(event.text.split('\n')) < 2: return await event.reply("❌ Формат: `.лс [текст]\n[@юзер1]\n[@юзер2]`")
                    txt = event.text.split('\n')[0][len(cmd):].strip()
                    targets = [l.strip() for l in event.text.split('\n')[1:] if l.strip()]
                    res = []
                    for t in targets:
                        try:
                            await client.send_message(t, txt)
                            res.append(f"✅ {t}")
                        except Exception as e: res.append(f"❌ {t} ({e.__class__.__name__})")
                    await event.reply("\n".join(res))

                elif cmd == '.флуд' and len(parts) >= 4:
                    if user_id in self.FLOOD_TASKS: return await event.reply("⚠️ Уже идет.")
                    try:
                        cnt = int(parts[1])
                        dly = float(parts[-1])
                        trg = parts[2]
                        msg_txt = " ".join(parts[3:-1])
                        
                        # Парсинг сущности
                        ent = await client.get_input_entity(trg)
                        cid = (await client.get_entity(trg)).id
                        
                        # Создание и запуск задачи
                        asyncio.create_task(flood_task(ent, msg_txt, cnt, dly, cid))
                        await event.reply("🔥 Флуд запущен!")
                    except ValueError: await event.reply("❌ Неверный формат чисел/задержки.")
                    except Exception as e: await event.reply(f"❌ Ошибка: {e.__class__.__name__}")

                elif cmd == '.стопфлуд':
                    if user_id in self.FLOOD_TASKS:
                        await self.stop_flood(user_id)
                        await event.reply("🛑 Остановлено.")
                    else: await event.reply("⚠️ Нет задач.")

                elif cmd == '.чекгруппу':
                    if user_id in PROCESS_PROGRESS: return await event.reply("⚠️ Занято.")
                    trg = parts[1] if len(parts) > 1 else None
                    mn = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
                    mx = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None
                    asyncio.create_task(check_group_task(event, trg, mn, mx))
                    await event.reply("⏳ Сканирование запущено.")
                    
                elif cmd == '.статус':
                    if user_id in PROCESS_PROGRESS:
                        p = PROCESS_PROGRESS[user_id]
                        status = f"⚙️ {p['type']}: {p.get('done', 0)} / {p.get('total', 'inf')}" if p['type'] == 'flood' else f"⚙️ {p['type']}: Обработано {p.get('done_msg', 0)} сообщений."
                        await event.reply(status)
                    else: await event.reply("✨ Worker активен, задач нет.")

            # 5. Основной цикл Telethon
            await client.run_until_disconnected()

        # 6. Обработка критических ошибок Telethon
        except AuthKeyUnregisteredError:
            await self._send_to_bot_user(user_id, "⚠️ **Сессия недействительна.** Требуется повторная авторизация.", reply_markup=get_main_kb(user_id))
        except FloodWaitError as e:
            await self._send_to_bot_user(user_id, f"❌ **Критическая ошибка FloodWait:** Бот получил глобальный таймаут на {e.seconds} секунд.", reply_markup=get_main_kb(user_id))
        except Exception as e:
            logger.error(f"Worker {user_id} disconnected unexpectedly: {e}")
            await self._send_to_bot_user(user_id, f"💔 **Worker отключился:** {e.__class__.__name__}. Попробуйте перезапустить.", reply_markup=get_main_kb(user_id))
        finally:
            await self.stop_worker(user_id, force_disconnect=False)

manager = TelethonManager(API_ID, API_HASH)

# =========================================================================
# V. КЛАВИАТУРЫ И UI/UX
# =========================================================================

def format_sub_info(user):
    end_date_str = user.get('subscription_end_date')
    if not user.get('subscription_active') or not end_date_str:
        return "🔴 Не активна"
    try:
        end = TIMEZONE_MSK.localize(datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S'))
        now = datetime.now(TIMEZONE_MSK)
        remaining = end - now
        days_left = remaining.days
        
        if days_left <= 0: return "🔴 Истекла"
        
        end_display = end.strftime('%d.%m.%Y')
        return f"🟢 До {end_display} ({days_left} дн.)"
    except:
        return "🔴 Недействительна"

def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

def get_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать Промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="👤 Выдать Подписку", callback_data="admin_grant_sub")],
        [InlineKeyboardButton(text="📊 Статистика (TODO)", callback_data="admin_stats")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_main_kb(user_id):
    user = db.get_user(user_id)
    active = user.get('telethon_active')
    running = user_id in manager.ACTIVE_WORKERS
    
    sub_info = format_sub_info(user)
    
    kb = []
    kb.append([InlineKeyboardButton(text=f"Подписка: {sub_info}", callback_data="show_sub_info")])
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    kb.append([InlineKeyboardButton(text="❓ Справка / Команды", callback_data="show_help")])

    if not active:
        kb.append([InlineKeyboardButton(text="📲 Вход по QR-коду", callback_data="telethon_auth_qr_start"),
                   InlineKeyboardButton(text="🔐 Вход по Номеру", callback_data="telethon_auth_phone_start")])
    else:
        status_text = "🟢 Worker Активен" if running else "🔴 Worker Остановлен"
        kb.append([InlineKeyboardButton(text=status_text, callback_data="telethon_check_status")])
        
        if running:
            kb.append([InlineKeyboardButton(text="🚀 Остановить Worker", callback_data="confirm_stop_session")])
            if user_id in PROCESS_PROGRESS:
                 kb.append([InlineKeyboardButton(text="⚡️ Прогресс", callback_data="show_progress")])
        else:
            kb.append([InlineKeyboardButton(text="🟢 Запустить Worker", callback_data="telethon_start_session")])
        
        kb.append([InlineKeyboardButton(text="❌ Выход (Удалить сессию)", callback_data="confirm_logout")])
        
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
        
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_confirm_kb(action_data):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=action_data)],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_main")]
    ])

def get_report_choice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Файлом", callback_data="send_checkgroup_file")],
        [InlineKeyboardButton(text="💬 Сообщениями", callback_data="send_checkgroup_messages")],
        [InlineKeyboardButton(text="❌ Удалить", callback_data="send_checkgroup_delete")]
    ])

# =========================================================================
# VI. AIOGRAM ХЭНДЛЕРЫ
# =========================================================================

@user_router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    db.get_user(user_id)
    
    has_access = db.check_subscription(user_id) or user_id == ADMIN_ID
    
    if not has_access:
        # Упрощенная проверка подписки и канала
        try:
            member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
            if member.status not in ('member', 'administrator', 'creator'):
                 return await message.answer(f"❌ Подпишитесь на канал: {TARGET_CHANNEL_URL} и активируйте подписку.", 
                                            reply_markup=get_main_kb(user_id))
        except Exception:
            # Если канал не найден или API ошибка
            pass
            
    await message.answer("👋 Привет! Добро пожаловать.", reply_markup=get_main_kb(user_id))

@user_router.callback_query(F.data == "back_to_main")
@user_router.callback_query(F.data == "cancel_action", StateFilter('*'))
async def back_home(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    # Очистка временного клиента
    client = TEMP_AUTH_CLIENTS.pop(user_id, None)
    if client:
        try: await client.disconnect()
        except: pass
    
    # Очистка временной сессии
    temp_session_path = get_session_path(user_id, True) + '.session'
    if os.path.exists(temp_session_path):
        os.remove(temp_session_path)
        
    await state.clear()
    
    try: await call.message.edit_text("🏠 Главное меню", reply_markup=get_main_kb(user_id))
    except TelegramBadRequest: await call.message.answer("🏠 Главное меню", reply_markup=get_main_kb(user_id))

# --- АВТОРИЗАЦИЯ FSM ---

@user_router.callback_query(F.data == "telethon_auth_phone_start", StateFilter(None))
@rate_limit(RATE_LIMIT_TIME)
async def auth_phone_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id in TEMP_AUTH_CLIENTS: return await call.answer("Процесс уже запущен.")
    user_id = call.from_user.id
    await state.set_state(TelethonAuth.PHONE)
    
    client = TelegramClient(get_session_path(user_id, True), manager.API_ID, manager.API_HASH, device_model="Android Client")
    TEMP_AUTH_CLIENTS[user_id] = client
    await call.message.edit_text("📞 **Ввод номера:**\nВведите ваш номер телефона в международном формате (+7...):", reply_markup=get_cancel_kb())
    await state.set_data({'message_id': call.message.message_id})

@user_router.message(TelethonAuth.PHONE)
async def auth_phone_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    client = TEMP_AUTH_CLIENTS.get(user_id)
    phone = message.text.strip()
    
    try:
        if not re.match(r'^\+?[0-9\s-]{7,15}$', phone): raise ValueError("Неверный формат номера.")
        
        await client.connect()
        hash_code = await client.send_code_request(phone)
        
        await state.update_data(phone=phone, hash=hash_code)
        await state.set_state(TelethonAuth.CODE)
        
        await message.answer("🔑 **Ввод кода:**\nВведите код, который пришел в ваш аккаунт Telegram:", reply_markup=get_cancel_kb())
        
    except PhoneNumberInvalidError: await message.answer("❌ **Ошибка:** Неверный номер телефона. Попробуйте снова.", reply_markup=get_cancel_kb())
    except RpcCallFailError as e: await message.answer(f"❌ **Ошибка API:** Сбой вызова RPC. {e}.", reply_markup=get_cancel_kb())
    except Exception as e: await message.answer(f"❌ **Ошибка:** {e.__class__.__name__}. Попробуйте снова.", reply_markup=get_cancel_kb())

@user_router.message(TelethonAuth.CODE)
async def auth_code_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    client = TEMP_AUTH_CLIENTS.get(user_id)
    
    try:
        await client.sign_in(data['phone'], message.text.strip(), phone_code_hash=data['hash'].phone_code_hash)
        await finalize_login(user_id, client, message, state)
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔒 **Ввод 2FA:**\nВведите ваш облачный пароль (2FA):", reply_markup=get_cancel_kb())
    except PhoneCodeExpiredError: await message.answer("❌ **Ошибка:** Код истек. Начните заново.", reply_markup=get_cancel_kb())
    except Exception as e: await message.answer(f"❌ **Ошибка:** {e.__class__.__name__}. Попробуйте снова.", reply_markup=get_cancel_kb())

@user_router.message(TelethonAuth.PASSWORD)
async def auth_password_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    client = TEMP_AUTH_CLIENTS.get(user_id)
    
    try:
        await client.sign_in(password=message.text.strip())
        await finalize_login(user_id, client, message, state)
    except PasswordHashInvalidError: await message.answer("❌ **Ошибка:** Неверный пароль. Попробуйте снова.", reply_markup=get_cancel_kb())
    except Exception as e: await message.answer(f"❌ **Ошибка:** {e.__class__.__name__}. Попробуйте снова.", reply_markup=get_cancel_kb())

@user_router.callback_query(F.data == "telethon_auth_qr_start", StateFilter(None))
@rate_limit(RATE_LIMIT_TIME)
async def auth_qr_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id in TEMP_AUTH_CLIENTS: return await call.answer("Процесс уже запущен.")
    user_id = call.from_user.id
    await state.set_state(TelethonAuth.WAITING_FOR_QR_LOGIN)
    
    client = TelegramClient(get_session_path(user_id, True), manager.API_ID, manager.API_HASH, device_model="Android Client")
    TEMP_AUTH_CLIENTS[user_id] = client
    
    try:
        await client.connect()
        qr_login = await client.qr_login()
        
        img = io.BytesIO(qr_login.qr_code)
        
        await call.message.answer_photo(
            BufferedInputFile(img.getvalue(), 'qr.png'), 
            caption="📲 **Скан QR:** Отсканируйте код на своём основном устройстве (Настройки -> Устройства/Сессии -> Сканировать QR).\n\n*Таймаут: 3 минуты.*", 
            reply_markup=get_cancel_kb()
        )
        
        await qr_login.wait(180) # Таймаут 3 минуты
        
        await finalize_login(user_id, client, call.message, state)
    
    except asyncio.exceptions.TimeoutError: 
        await call.message.answer("❌ **Таймаут:** Время для сканирования QR-кода истекло.", reply_markup=get_main_kb(user_id))
    except Exception as e: 
        await call.message.answer(f"❌ **Ошибка:** {e.__class__.__name__}. Попробуйте снова.", reply_markup=get_main_kb(user_id))
    finally:
        if user_id in TEMP_AUTH_CLIENTS: TEMP_AUTH_CLIENTS.pop(user_id)

async def finalize_login(user_id, client, message, state):
    await client.disconnect()
    
    src = get_session_path(user_id, True) + '.session'
    dst = get_session_path(user_id) + '.session'
    
    if os.path.exists(src):
        if os.path.exists(dst): os.remove(dst)
        os.rename(src, dst)
        
    db.set_telethon_status(user_id, True)
    await state.clear()
    await message.answer("✅ **Авторизация успешна!** Запускаю Worker...", reply_markup=get_main_kb(user_id))
    asyncio.create_task(manager.run_worker(user_id))

# --- WORKER CONTROL ---

@user_router.callback_query(F.data == "telethon_start_session")
@rate_limit(RATE_LIMIT_TIME)
async def worker_start(call: types.CallbackQuery):
    asyncio.create_task(manager.run_worker(call.from_user.id))
    await call.answer("Запуск Worker'а...")
    await call.message.edit_reply_markup(reply_markup=get_main_kb(call.from_user.id))

@user_router.callback_query(F.data == "confirm_stop_session")
async def confirm_worker_stop(call: types.CallbackQuery):
    await call.message.edit_text("Вы уверены, что хотите **остановить** Worker? Задачи будут отменены.", reply_markup=get_confirm_kb("telethon_stop_session_confirmed"))

@user_router.callback_query(F.data == "telethon_stop_session_confirmed")
async def worker_stop_confirmed(call: types.CallbackQuery):
    await manager.stop_worker(call.from_user.id)
    await call.answer("Worker остановлен.", show_alert=True)
    await call.message.edit_text("Worker остановлен.", reply_markup=get_main_kb(call.from_user.id))

@user_router.callback_query(F.data == "confirm_logout")
async def confirm_worker_logout(call: types.CallbackQuery):
    await call.message.edit_text("Вы уверены, что хотите **удалить сессию**? Потребуется новая авторизация!", reply_markup=get_confirm_kb("telethon_logout_confirmed"))

@user_router.callback_query(F.data == "telethon_logout_confirmed")
async def worker_logout_confirmed(call: types.CallbackQuery):
    user_id = call.from_user.id
    await manager.stop_worker(user_id)
    
    session_path = get_session_path(user_id) + '.session'
    if os.path.exists(session_path): os.remove(session_path)
    
    db.set_telethon_status(user_id, False)
    await call.message.edit_text("❌ Сессия удалена. Требуется авторизация.", reply_markup=get_main_kb(user_id))

@user_router.callback_query(F.data == "telethon_check_status")
async def worker_status(call: types.CallbackQuery):
    running = call.from_user.id in manager.ACTIVE_WORKERS
    await call.answer(f"Worker: {'🟢 Активен' if running else '🔴 Остановлен'}", show_alert=True)

@user_router.callback_query(F.data == "show_progress")
async def show_progress(call: types.CallbackQuery):
    user_id = call.from_user.id
    if user_id not in PROCESS_PROGRESS: return await call.answer("Нет активных задач.", show_alert=True)
    
    p = PROCESS_PROGRESS[user_id]
    if p['type'] == 'flood':
        text = f"⚙️ **Флуд:**\nОтправлено: {p.get('done', 0)} из {p.get('total', '∞')}"
    elif p['type'] == 'checkgroup':
        text = f"⚙️ **Сканирование:**\nОбработано: {p.get('done_msg', 0)} сообщений."
    else:
        text = "Прогресс неизвестной задачи."
        
    await call.answer(text, show_alert=True)

@user_router.callback_query(F.data == "show_help")
async def help_msg(call: types.CallbackQuery):
    help_text = (
        "📚 **Справка по командам Worker'а (в чате вашего аккаунта):**\n\n"
        "1. **`.лс [текст]`**\n   Отправка ЛС по списку юзернеймов/ID.\n   Пример:\n   `.лс Привет!\n@user1\n12345678`\n\n"
        "2. **`.флуд [кол-во] [цель] [текст] [задержка]`**\n   Массовая отправка сообщений.\n   `[кол-во]`: число (0 - без лимита)\n   `[цель]`: юзернейм или ID\n   `[задержка]`: число (секунды)\n   Пример: `.флуд 100 @target_chat Привет, тест 0.5`\n\n"
        "3. **`.стопфлуд`**\n   Останавливает запущенный флуд.\n\n"
        "4. **`.чекгруппу [цель] [минID] [максID]`**\n   Сканирует сообщения в чате для сбора списка участников.\n   Пример: `.чекгруппу @target_chat 1000000 9000000000`\n\n"
        "5. **`.статус`**\n   Проверить прогресс запущенной задачи."
    )
    await call.message.edit_text(help_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]]))

@user_router.callback_query(F.data == "show_sub_info")
async def sub_info_msg(call: types.CallbackQuery):
    user = db.get_user(call.from_user.id)
    end_date_str = user.get('subscription_end_date')
    
    if not db.check_subscription(call.from_user.id):
        text = "❌ **Подписка не активна.**\nАктивируйте ее промокодом или обратитесь к администратору."
    else:
        end = TIMEZONE_MSK.localize(datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S'))
        now = datetime.now(TIMEZONE_MSK)
        days_left = (end - now).days
        
        text = (
            "✅ **Ваша подписка активна!**\n"
            f"Осталось дней: **{days_left}**\n"
            f"Дата окончания: **{end.strftime('%d.%m.%Y %H:%M:%S')} (МСК)**"
        )
        
    await call.answer(text, show_alert=True)

# --- ПРОМОКОДЫ И АДМИНКА (без изменений в логике FSM, но с улучшенной БД) ---

@user_router.callback_query(F.data == "start_promo_fsm")
async def promo_start(call: types.CallbackQuery, state: FSMContext):
    if db.check_subscription(call.from_user.id):
         return await call.answer("У вас уже есть активная подписка.", show_alert=True)
    await state.set_state(PromoStates.waiting_for_code)
    await call.message.edit_text("🔑 **Ввод промокода:**\nВведите ваш промокод:", reply_markup=get_cancel_kb())

@user_router.message(PromoStates.waiting_for_code)
async def promo_input(message: types.Message, state: FSMContext):
    code = message.text.strip()
    promo = db.get_promo(code)
    
    if not promo or not promo['is_active'] or (promo['max_uses'] > 0 and promo['current_uses'] >= promo['max_uses']):
        await state.clear()
        return await message.answer("❌ Промокод не найден или недействителен.", reply_markup=get_main_kb(message.from_user.id))
    
    # Активация
    db.use_promo(code)
    new_end = db.update_subscription(message.from_user.id, promo['days'])
    
    await state.clear()
    await message.answer(f"🎉 **Успех!**\nДобавлено: {promo['days']} дней.\nПодписка до: {new_end}", reply_markup=get_main_kb(message.from_user.id))

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(AdminStates.main_menu)
    await call.message.edit_text("🛠 **Админ-панель**", reply_markup=get_admin_kb())

@user_router.callback_query(F.data == "admin_create_promo")
async def admin_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.promo_days_input)
    await call.message.edit_text("Дней подписки (число):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_days_input)
async def admin_promo_days(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return await message.answer("Введите число!")
    await state.update_data(days=int(message.text))
    await state.set_state(AdminStates.promo_uses_input)
    await message.answer("Лимит использований (0 - безлимит):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_uses_input)
async def admin_promo_fin(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return await message.answer("Введите число!")
    data = await state.get_data()
    code = generate_promo_code()
    db.add_promo(code, data['days'], int(message.text))
    
    await state.clear()
    await message.answer(f"✅ **Промокод создан!**\nКод: `{code}`", reply_markup=get_admin_kb())

@user_router.callback_query(F.data == "admin_grant_sub")
async def admin_grant(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.sub_user_id_input)
    await call.message.edit_text("ID пользователя:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_user_id_input)
async def admin_grant_id(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return await message.answer("ID должен быть числом!")
    await state.update_data(uid=int(message.text))
    await state.set_state(AdminStates.sub_days_input)
    await message.answer("Количество дней:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_days_input)
async def admin_grant_fin(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return await message.answer("Введите число!")
    data = await state.get_data()
    
    new_end = db.update_subscription(data['uid'], int(message.text))
    
    await state.clear()
    await message.answer(f"✅ Подписка выдана ID {data['uid']}.\nДо: {new_end}", reply_markup=get_admin_kb())

# =========================================================================
# VII. ЗАПУСК И ПЕРИОДИЧЕСКИЕ ЗАДАЧИ
# =========================================================================

async def cleanup_temp_sessions():
    """Периодическая очистка устаревших временных сессий."""
    while True:
        await asyncio.sleep(3600) # Проверка раз в час
        for filename in os.listdir(SESSION_DIR):
            if filename.endswith('_temp.session'):
                path = os.path.join(SESSION_DIR, filename)
                # Простейшая проверка: удалить, если файл старше 1 часа
                if (datetime.now() - datetime.fromtimestamp(os.path.getmtime(path))).total_seconds() > 3600:
                    try:
                        os.remove(path)
                        logger.info(f"Cleaned up old temp session: {filename}")
                    except Exception as e:
                        logger.error(f"Error cleaning up {filename}: {e}")

async def main():
    db._init_db()
    
    # Запуск фоновых задач
    asyncio.create_task(manager.start_workers_on_startup())
    asyncio.create_task(cleanup_temp_sessions())
    
    dp.include_router(user_router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
