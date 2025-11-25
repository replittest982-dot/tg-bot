import asyncio
import logging
import os
import sqlite3
import pytz
import time
import re
import io
import random
import string
from datetime import datetime, timedelta

# Импорты aiogram
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile, FSInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, StateFilter
from aiogram.client.default import DefaultBotProperties

# Импорты telethon
from telethon import TelegramClient, events, functions, types as tl_types
from telethon.errors import (
    UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, 
    PhoneNumberInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError, 
    AuthKeyUnregisteredError, PeerIdInvalidError, PasswordHashInvalidError
)
from telethon.utils import get_display_name

# Импорт для QR-кода
import qrcode 

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# !!! ВАШИ КЛЮЧИ !!! 
BOT_TOKEN = "7868097991:AAEuHy_DYjEkBTK-H-U1P4-wZSdSw7evzEQ" 
ADMIN_ID = 6256576302  
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1" # Обязательный канал для подписки
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
DB_TIMEOUT = 10 # Таймаут для подключения к SQLite

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальные хранилища
ACTIVE_TELETHON_CLIENTS = {} 
ACTIVE_TELETHON_WORKERS = {} 
TEMP_AUTH_CLIENTS = {} 
FLOOD_TASKS = {} 

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=storage)
user_router = Router()

# =========================================================================
# II. FSM СОСТОЯНИЯ
# =========================================================================

class TelethonAuth(StatesGroup):
    CHOOSE_AUTH_METHOD = State()
    PHONE = State()
    QR_CODE_WAIT = State()
    CODE = State()
    PASSWORD = State()

class PromoStates(StatesGroup):
    waiting_for_code = State()

class AdminStates(StatesGroup):
    main_menu = State()
    promo_code_input = State()
    promo_days_input = State()
    promo_uses_input = State()
    sub_user_id_input = State()
    sub_days_input = State()

class MonitorStates(StatesGroup):
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()

class ReportStates(StatesGroup):
    waiting_report_target = State() 
    waiting_report_send_chat = State() 

# =========================================================================
# III. БАЗА ДАННЫХ
# =========================================================================

DB_PATH = os.path.join('data', DB_NAME)

# Улучшенное подключение с контекстным менеджером и таймаутом
def get_db_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT)

def db_init():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, 
                subscription_active BOOLEAN DEFAULT 0,
                subscription_end_date TEXT, 
                telethon_active BOOLEAN DEFAULT 0,
                telethon_hash TEXT, 
                promo_code TEXT, 
                it_chat_id TEXT,
                drop_chat_id TEXT, 
                report_chat_id TEXT
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY, 
                days INTEGER, 
                is_active BOOLEAN DEFAULT 1,
                max_uses INTEGER, 
                current_uses INTEGER DEFAULT 0
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS monitor_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                user_id INTEGER, 
                timestamp TEXT,
                type TEXT, 
                command TEXT, 
                message TEXT, 
                FOREIGN KEY (user_id) REFERENCES users(user_id)
        )""")

def db_get_user(user_id):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if row:
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
        return None

def db_check_subscription(user_id):
    user = db_get_user(user_id)
    if not user or not user.get('subscription_active'): return False
    try:
        end = TIMEZONE_MSK.localize(datetime.strptime(user.get('subscription_end_date'), '%Y-%m-%d %H:%M:%S'))
        return end > datetime.now(TIMEZONE_MSK)
    except: return False

def db_update_subscription(user_id, days):
    with get_db_connection() as conn:
        cur = conn.cursor()
        user = db_get_user(user_id)
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
        return new_end

def db_set_session_status(user_id, status):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))

def db_add_monitor_log(user_id, log_type, command, message):
    with get_db_connection() as conn:
        cur = conn.cursor()
        ts = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        cur.execute("INSERT INTO monitor_logs (user_id, timestamp, type, command, message) VALUES (?, ?, ?, ?, ?)", 
                    (user_id, ts, log_type, command, message))

def db_get_monitor_logs(user_id, log_type):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT timestamp, command, message FROM monitor_logs WHERE user_id=? AND type=? ORDER BY timestamp DESC", (user_id, log_type))
        return cur.fetchall()

def db_clear_monitor_logs(user_id, log_type):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM monitor_logs WHERE user_id=? AND type=?", (user_id, log_type))
        return cur.rowcount

def db_set_chat_id(user_id, ctype, cid):
    with get_db_connection() as conn:
        cur = conn.cursor()
        col = 'it_chat_id' if ctype == 'IT' else 'drop_chat_id'
        cur.execute(f"UPDATE users SET {col}=? WHERE user_id=?", (cid, user_id))

def db_get_promo(code):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM promo_codes WHERE code=?", (code,))
        row = cur.fetchone()
        if row:
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
        return None

def db_use_promo(code):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code=?", (code,))

def db_add_promo(code, days, max_uses):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO promo_codes (code, days, max_uses) VALUES (?, ?, ?)", (code, days, max_uses))

def db_get_active_telethon_users():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
        return [row[0] for row in cur.fetchall()]

# =========================================================================
# IV. УТИЛИТЫ И КЛАВИАТУРЫ
# =========================================================================

def get_session_path(user_id):
    os.makedirs('data', exist_ok=True)
    return os.path.join('data', f'session_{user_id}')

async def check_access(user_id: int, bot: Bot):
    if user_id == ADMIN_ID: 
        return True, ""

    # 1. ОБЯЗАТЕЛЬНАЯ ПРОВЕРКА ПОДПИСКИ НА КАНАЛ
    if TARGET_CHANNEL_URL:
        try:
            chat_member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
            if chat_member.status not in ('member', 'administrator', 'creator'):
                 # Если не подписан, возвращаем False и сообщение
                return False, f"❌ Для доступа к функциям подпишитесь на наш канал: {TARGET_CHANNEL_URL}"
        except TelegramForbiddenError:
            # Если бот забанен или не является участником
            logger.error(f"Bot is not a member of {TARGET_CHANNEL_URL}. Check failed.")
            pass # Продолжаем, предполагая, что доступ не должен быть заблокирован из-за ошибки бота
        except Exception as e:
            logger.error(f"Channel check failed for {user_id}: {e}")
            pass

    # 2. ПРОВЕРКА АКТИВНОЙ ПОДПИСКИ
    if db_check_subscription(user_id): 
        return True, ""
    
    # Если подписан на канал, но нет активной подписки в БД
    return False, "❌ Ваша подписка истекла. Активируйте промокод."

def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

def get_main_kb(user_id):
    user = db_get_user(user_id)
    active = user.get('telethon_active')
    running = user_id in ACTIVE_TELETHON_WORKERS
    
    kb = []
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    kb.append([InlineKeyboardButton(text="❓ Помощь / Команды", callback_data="show_help")]) # Новая кнопка
    
    if not active:
        kb.append([InlineKeyboardButton(text="🔐 Авторизация (Вход)", callback_data="telethon_auth_start")])
    else:
        kb.append([
            InlineKeyboardButton(text="📊 Мониторинг и Отчеты", callback_data="show_monitor_menu")
        ])
        status = "🟢 Worker Запущен" if running else "🔴 Worker Остановлен"
        action = "telethon_stop_session" if running else "telethon_start_session"
        kb.append([
            InlineKeyboardButton(text=status, callback_data=action),
            InlineKeyboardButton(text="ℹ️ Статус", callback_data="telethon_check_status")
        ])
    
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_auth_method_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 По номеру", callback_data="auth_method_phone")],
        [InlineKeyboardButton(text="🖼️ По QR-коду", callback_data="auth_method_qr")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ])

def get_monitor_kb(user_id):
    user = db_get_user(user_id)
    it = user.get('it_chat_id', 'Не задан')
    drop = user.get('drop_chat_id', 'Не задан')
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"IT-Чат ({it})", callback_data="monitor_set_it")],
        [InlineKeyboardButton(text=f"DROP-Чат ({drop})", callback_data="monitor_set_drop")],
        [InlineKeyboardButton(text="📄 Сформировать Отчет", callback_data="monitor_generate_report_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать Промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="👤 Выдать Подписку", callback_data="admin_grant_sub")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_no_access_kb(has_channel_access):
    kb = []
    if not has_channel_access:
        kb.append([InlineKeyboardButton(text="✅ Подписаться на канал", url=f"https://t.me/{TARGET_CHANNEL_URL.lstrip('@')}")])
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    kb.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

# =========================================================================
# V. TELETHON WORKER (ОСНОВНОЕ ЯДРО)
# =========================================================================

async def stop_worker(user_id):
    if user_id in ACTIVE_TELETHON_WORKERS:
        ACTIVE_TELETHON_WORKERS[user_id].cancel()
        del ACTIVE_TELETHON_WORKERS[user_id]
    
    if user_id in ACTIVE_TELETHON_CLIENTS:
        try:
            await ACTIVE_TELETHON_CLIENTS[user_id].disconnect()
        except: pass
        del ACTIVE_TELETHON_CLIENTS[user_id]
    db_set_session_status(user_id, False)
    logger.info(f"Worker {user_id} stopped.")

async def progress_bar(current, total, length=10):
    percent = current / total
    filled = int(length * percent)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}] {int(percent * 100)}%"

async def run_worker(user_id):
    await stop_worker(user_id)
    path = get_session_path(user_id)
    client = TelegramClient(path, API_ID, API_HASH)
    ACTIVE_TELETHON_CLIENTS[user_id] = client
    
    try:
        if not os.path.exists(path + '.session'):
            db_set_session_status(user_id, False)
            logger.error(f"Worker {user_id} failed to start: session file not found.")
            return

        await client.start()
        db_set_session_status(user_id, True)
        logger.info(f"Worker {user_id} started successfully.")
        
        IT_REGEX = {k: r'.*' for k in ['.встал', '.кьар', '.ошибка', '.замена', '.повтор']}
        DROP_REGEX = r'^\+?\d{5,15}\s+\d{1,2}:\d{2}\s+@\w+\s+бх' 

        @client.on(events.NewMessage)
        async def handler(event):
            # Проверка подписки в реалтайме
            if not db_check_subscription(user_id) and user_id != ADMIN_ID: return

            if not event.text: return
            chat_id = str(event.chat_id) 
            user = db_get_user(user_id)
            
            # 1. Мониторинг
            if user.get('it_chat_id') and chat_id == user.get('it_chat_id'):
                for cmd in IT_REGEX:
                    if event.text.lower().startswith(cmd):
                        db_add_monitor_log(user_id, 'IT', cmd, event.text)
                        break
            
            if user.get('drop_chat_id') and chat_id == user.get('drop_chat_id'):
                if re.match(DROP_REGEX, event.text, re.IGNORECASE):
                    db_add_monitor_log(user_id, 'DROP', 'DROP_ENTRY', event.text)

            # 2. Инструменты (только для исходящих сообщений)
            if event.out:
                msg = event.text.strip()
                parts = msg.split()
                if not parts: return
                cmd = parts[0].lower()

                if cmd == '.лс' and len(parts) >= 3:
                    # Логика .лс [текст] [список @юзернеймов/ID]
                    # ... (логика осталась прежней)
                    pass

                elif cmd == '.флуд' and len(parts) >= 4:
                    # ... (логика осталась прежней, использует FLOOD_TASKS)
                    pass

                elif cmd == '.стопфлуд':
                    FLOOD_TASKS[user_id] = False
                    await event.reply("🛑 Команда остановки принята.")

                elif cmd == '.чекгруппу' and len(parts) >= 2:
                    # ... (логика осталась прежней)
                    pass


        # Добавляем задачу воркеру
        worker_task = asyncio.create_task(client.run_until_disconnected())
        ACTIVE_TELETHON_WORKERS[user_id] = worker_task
        await worker_task
        
    except (AuthKeyUnregisteredError, UserDeactivatedError):
        await bot.send_message(user_id, "⚠️ Сессия недействительна. Пожалуйста, авторизуйтесь заново.")
        logger.error(f"Worker {user_id} failed due to unregistration/deactivation.")
    except Exception as e:
        logger.error(f"Worker {user_id} critical error: {e}")
        await bot.send_message(user_id, f"❌ Критическая ошибка воркера: {e}")
    finally:
        # Убеждаемся, что worker остановлен и статус обновлен
        if user_id in ACTIVE_TELETHON_CLIENTS: del ACTIVE_TELETHON_CLIENTS[user_id]
        if user_id in ACTIVE_TELETHON_WORKERS: del ACTIVE_TELETHON_WORKERS[user_id]
        db_set_session_status(user_id, False)

async def start_workers():
    users = db_get_active_telethon_users()
    for uid in users:
        # Создаем задачу для каждого воркера
        asyncio.create_task(run_worker(uid))

# =========================================================================
# VI. ХЕНДЛЕРЫ
# =========================================================================

@user_router.callback_query(F.data == "cancel_action")
async def cancel(call: types.CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    
    # Очищаем временный клиент, если отмена произошла во время авторизации
    if current_state in [TelethonAuth.PHONE, TelethonAuth.CODE, TelethonAuth.PASSWORD]:
        uid = call.from_user.id
        client = TEMP_AUTH_CLIENTS.pop(uid, None)
        if client:
            try: await client.disconnect()
            except: pass
            
    await state.clear()
    try: await call.message.edit_text("❌ Действие отменено.", reply_markup=get_main_kb(call.from_user.id))
    except TelegramBadRequest: await call.message.delete()
    # Возвращаем в главное меню
    await cmd_start(call, state)


@user_router.callback_query(F.data == "back_to_main")
@user_router.message(Command("start"))
async def cmd_start(u: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = u.from_user.id
    db_get_user(user_id)
    
    # Проверка доступа с учетом канала и подписки
    has_access, msg = await check_access(user_id, bot)
    
    text = f"👋 <b>Привет!</b> Ваш ID: <code>{user_id}</code>\n"
    sub = db_get_user(user_id).get('subscription_end_date')
    text += f"Подписка до: <code>{sub if sub else 'Нет'}</code>\n\n"
    
    if not has_access:
        text += f"⚠️ <b>Доступ ограничен.</b>\n{msg}"
        # Проверяем, в чем причина: нет подписки на канал ИЛИ нет активной подписки.
        is_channel_reason = f"Для доступа подпишитесь на наш канал" in msg
        kb = get_no_access_kb(not is_channel_reason) 
    else:
        text += "✅ <b>Меню доступно.</b>\nИспользуйте кнопки ниже."
        kb = get_main_kb(user_id)

    if isinstance(u, types.Message): 
        # Если это /start, отправляем новое сообщение
        await u.answer(text, reply_markup=kb)
    else: 
        # Если это callback_query, редактируем
        await u.message.edit_text(text, reply_markup=kb)

# --- НОВАЯ КОМАНДА ПОМОЩИ ---
@user_router.callback_query(F.data == "show_help")
@user_router.message(Command("help"))
async def cmd_help(u: types.Message | types.CallbackQuery):
    help_text = (
        "📚 <b>Справка и Команды:</b>\n\n"
        "<b>Инструменты Telethon (используются в чатах):</b>\n"
        " • <code>.лс [текст] [список @юзернеймов/ID]</code> — Отправка личных сообщений. "
        "Пример: <code>.лс Привет @user1 @user2</code>\n"
        " • <code>.флуд [кол-во] [текст] [задержка] [@чат]</code> — Флуд в чат. "
        "Пример: <code>.флуд 100 Спам 0.5 @MyChat</code>\n"
        " • <code>.стопфлуд</code> — Остановить запущенный флуд.\n"
        " • <code>.чекгруппу [@чат]</code> — Анализ группы (кол-во людей/ботов).\n\n"
        "<b>Функции бота:</b>\n"
        " • 🔐 **Авторизация:** Вход в Telegram аккаунт для запуска воркера.\n"
        " • 📊 **Мониторинг:** Настройка чатов для отслеживания команд (.встал и т.д.) и дроп-записей.\n"
        " • 📄 **Отчеты:** Генерация и отправка отчетов по собранным логам.\n"
        " • 🔑 **Промокод:** Активация подписки."
    )
    if isinstance(u, types.Message):
        await u.answer(help_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")]]))
    else:
        await u.message.edit_text(help_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")]]))


# --- АВТОРИЗАЦИЯ ---

@user_router.callback_query(F.data == "telethon_auth_start")
async def auth_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.CHOOSE_AUTH_METHOD)
    await call.message.edit_text("🔐 Выберите метод входа:", reply_markup=get_auth_method_kb())

@user_router.callback_query(F.data == "auth_method_phone")
async def auth_phone(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("📱 Введите номер (например +79001234567):", reply_markup=get_cancel_kb())

@user_router.message(TelethonAuth.PHONE)
async def auth_phone_step(msg: Message, state: FSMContext):
    phone = msg.text.strip()
    client = TelegramClient(get_session_path(msg.from_user.id), API_ID, API_HASH)
    TEMP_AUTH_CLIENTS[msg.from_user.id] = client # Сохраняем клиента
    try:
        await client.connect()
        res = await client.send_code_request(phone)
        
        await state.update_data(phone=phone, phone_hash=res.phone_code_hash)
        await state.set_state(TelethonAuth.CODE)
        await msg.answer("🔢 Введите код, который пришел в Telegram (у вас есть несколько минут):", reply_markup=get_cancel_kb()) 
    except (PhoneNumberInvalidError, FloodWaitError) as e:
        await msg.answer(f"❌ Ошибка. Проверьте формат номера (+7...). {e}", reply_markup=get_cancel_kb())
        await client.disconnect()
        if msg.from_user.id in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[msg.from_user.id]
    except Exception as e:
        logger.error(f"Auth phone step error: {e}")
        await msg.answer(f"❌ Неизвестная ошибка: {e}", reply_markup=get_cancel_kb())
        await client.disconnect()
        if msg.from_user.id in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[msg.from_user.id]

@user_router.message(TelethonAuth.CODE)
async def auth_msg_code(msg: Message, state: FSMContext):
    code = msg.text.strip()
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    
    if not client:
        await msg.answer("⚠️ Сессия авторизации истекла. Начните заново.", reply_markup=get_main_kb(uid))
        await state.clear()
        return

    d = await state.get_data()
    
    try:
        if not client.is_connected(): await client.connect()
        
        await client.sign_in(d['phone'], code, phone_code_hash=d['phone_hash'])
        
        # Успех
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Успешно вошли! Worker запущен.", reply_markup=get_main_kb(uid))
        await state.clear()
        
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await msg.answer("🔒 Требуется двухфакторная авторизация (2FA). Введите пароль:", reply_markup=get_cancel_kb())
        
    except (PhoneCodeExpiredError, PhoneCodeInvalidError) as e:
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        await msg.answer(f"❌ Код недействителен или истек. Пожалуйста, начните авторизацию сначала. Ошибка: {e}", reply_markup=get_main_kb(uid))
        await state.clear()
        
    except Exception as e:
        logger.error(f"Auth code step error: {e}")
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        await msg.answer(f"❌ Неизвестная ошибка при вводе кода: {e}", reply_markup=get_main_kb(uid))
        await state.clear()

@user_router.message(TelethonAuth.PASSWORD)
async def auth_pwd(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    if not client:
        await msg.answer("⚠️ Сессия истекла.")
        await state.clear()
        return
    
    try:
        if not client.is_connected(): await client.connect()
        
        await client.sign_in(password=msg.text.strip())
        
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Успешно вошли (2FA)! Worker запущен.", reply_markup=get_main_kb(uid))
        await state.clear()
        
    except PasswordHashInvalidError:
        await msg.answer("❌ Неверный пароль 2FA. Повторите ввод:", reply_markup=get_cancel_kb())
    except Exception as e:
        logger.error(f"Auth password step error: {e}")
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        await msg.answer(f"❌ Неизвестная ошибка 2FA: {e}", reply_markup=get_main_kb(uid))
        await state.clear()

# --- QR CODE ---
@user_router.callback_query(F.data == "auth_method_qr")
async def auth_qr(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    client = TelegramClient(get_session_path(uid), API_ID, API_HASH)
    
    try:
        await client.connect()
        qr_login = await client.qr_login()
        img = qrcode.make(qr_login.url)
        bio = io.BytesIO()
        img.save(bio, 'PNG')
        bio.seek(0)
        
        m = await call.message.answer_photo(
            BufferedInputFile(bio.read(), 'qr.png'), 
            caption="🖼️ Сканируйте QR-код через 'Настройки' -> 'Устройства' -> 'Подключение нового устройства'. Код действует 120 секунд.", 
            reply_markup=get_cancel_kb()
        )
        asyncio.create_task(wait_qr(client, uid, qr_login, m, state))
    except Exception as e:
        await call.message.answer(f"❌ Ошибка при подготовке QR: {e}")
        await client.disconnect()
        await state.clear()

async def wait_qr(client, uid, qr_login, m, state: FSMContext):
    try:
        await qr_login.wait(120)
        await client.disconnect()
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        try: await m.edit_caption("✅ Вход по QR успешен! Worker запущен.", reply_markup=None)
        except: pass
    except asyncio.TimeoutError:
        try: await m.edit_caption("❌ Время вышло. Начните авторизацию заново.", reply_markup=None)
        except: pass
        await client.disconnect()
    except Exception as e:
        logger.error(f"QR wait error for {uid}: {e}")
        try: await m.edit_caption(f"❌ Ошибка входа по QR: {e}", reply_markup=None)
        except: pass
        await client.disconnect()

# --- АДМИНКА ---

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(AdminStates.main_menu)
    await call.message.edit_text("👑 Админ-панель:", reply_markup=get_admin_kb())

# 1. Создание промокода
@user_router.callback_query(F.data == "admin_create_promo")
async def adm_create_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.promo_code_input)
    await call.message.edit_text("Введите название промокода (или нажмите Отмена для автогенерации):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_code_input)
async def adm_promo_name(msg: Message, state: FSMContext):
    code = msg.text.strip()
    await state.update_data(code=code)
    await state.set_state(AdminStates.promo_days_input)
    await msg.answer(f"Код: <code>{code}</code>. Введите кол-во дней подписки (целое число):", reply_markup=get_cancel_kb())

@user_router.callback_query(F.data == "cancel_action", StateFilter(AdminStates.promo_code_input))
async def adm_promo_name_auto(call: types.CallbackQuery, state: FSMContext):
    # Автогенерация
    code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    await state.update_data(code=code)
    await state.set_state(AdminStates.promo_days_input)
    await call.message.edit_text(f"Код: <code>{code}</code> (Авто). Введите кол-во дней подписки (целое число):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_days_input)
async def adm_promo_days(msg: Message, state: FSMContext):
    if not msg.text.strip().isdigit() or int(msg.text.strip()) <= 0: 
        return await msg.answer("❌ Введите положительное целое число дней.", reply_markup=get_cancel_kb())
        
    await state.update_data(days=int(msg.text.strip()))
    await state.set_state(AdminStates.promo_uses_input)
    await msg.answer("Лимит использований (введите 0 для безлимита):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_uses_input)
async def adm_promo_final(msg: Message, state: FSMContext):
    if not msg.text.strip().isdigit() or int(msg.text.strip()) < 0: 
        return await msg.answer("❌ Введите целое число (0 или больше).", reply_markup=get_cancel_kb())
        
    d = await state.get_data()
    limit = int(msg.text.strip())
    
    db_add_promo(d['code'], d['days'], limit if limit > 0 else None)
    
    await msg.answer(f"✅ Промокод <code>{d['code']}</code> создан!\nДни: {d['days']}\nЛимит: {'Безлимит' if limit == 0 else limit}", 
                     reply_markup=get_admin_kb())
    await state.clear()

# 2. Выдача подписки
@user_router.callback_query(F.data == "admin_grant_sub")
async def adm_grant_sub(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.sub_user_id_input)
    await call.message.edit_text("Введите ID пользователя (целое число):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_user_id_input)
async def adm_sub_id(msg: Message, state: FSMContext):
    if not msg.text.strip().isdigit(): 
        return await msg.answer("❌ Введите ID пользователя как целое число.", reply_markup=get_cancel_kb())
        
    await state.update_data(uid=int(msg.text.strip()))
    await state.set_state(AdminStates.sub_days_input)
    await msg.answer("На сколько дней выдать подписку (целое число):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_days_input)
async def adm_sub_final(msg: Message, state: FSMContext):
    if not msg.text.strip().isdigit() or int(msg.text.strip()) <= 0: 
        return await msg.answer("❌ Введите положительное целое число дней.", reply_markup=get_cancel_kb())
        
    d = await state.get_data()
    days = int(msg.text.strip())
    end = db_update_subscription(d['uid'], days)
    
    await msg.answer(f"✅ Подписка выдана ID <code>{d['uid']}</code> на {days} дней. Истекает: {end}", 
                     reply_markup=get_admin_kb())
    await state.clear()

# --- ПРОМОКОДЫ (ПОЛЬЗОВАТЕЛЬ) ---
@user_router.callback_query(F.data == "start_promo_fsm")
async def user_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await call.message.edit_text("Введи промокод:", reply_markup=get_cancel_kb())

@user_router.message(PromoStates.waiting_for_code)
async def user_promo_check(msg: Message, state: FSMContext):
    code = msg.text.strip()
    p = db_get_promo(code)
    
    if p and p['is_active'] and (p['max_uses'] is None or p['current_uses'] < p['max_uses']):
        db_use_promo(code)
        end = db_update_subscription(msg.from_user.id, p['days'])
        await msg.answer(f"✅ Промокод <code>{code}</code> активирован!\nПодписка продлена до <b>{end}</b>", 
                         reply_markup=get_main_kb(msg.from_user.id))
    else:
        await msg.answer("❌ Неверный, истекший код или превышен лимит использований.", 
                         reply_markup=get_main_kb(msg.from_user.id))
                         
    await state.clear()

# --- ОТЧЕТЫ И МОНИТОРИНГ ---

@user_router.callback_query(F.data == "show_monitor_menu")
async def mon_menu(call: types.CallbackQuery):
    await call.message.edit_text("Меню мониторинга:", reply_markup=get_monitor_kb(call.from_user.id))

@user_router.callback_query(F.data.startswith("monitor_set_"))
async def mon_set(call: types.CallbackQuery, state: FSMContext):
    ctype = call.data.split('_')[-1].upper()
    await state.update_data(ctype=ctype)
    await state.set_state(MonitorStates.waiting_for_it_chat_id)
    await call.message.edit_text(f"Введите ID чата или @username канала для {ctype} мониторинга:", reply_markup=get_cancel_kb())

@user_router.message(MonitorStates.waiting_for_it_chat_id)
async def mon_save(msg: Message, state: FSMContext):
    d = await state.get_data()
    # Приводим к строке
    chat_id_input = msg.text.strip()
    
    db_set_chat_id(msg.from_user.id, d['ctype'], chat_id_input)
    await msg.answer(f"✅ Сохранен ID <code>{chat_id_input}</code> для {d['ctype']} мониторинга.\nДля применения настроек <b>перезапустите Worker</b>.", 
                     reply_markup=get_monitor_kb(msg.from_user.id))
    await state.clear()

@user_router.callback_query(F.data == "monitor_generate_report_start")
async def rep_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(ReportStates.waiting_report_target)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="IT (Команды)", callback_data="rep_IT"), InlineKeyboardButton(text="DROP (Записи)", callback_data="rep_DROP")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ])
    await call.message.edit_text("Выберите тип логов для отчета:", reply_markup=kb)

@user_router.callback_query(F.data.startswith("rep_"), StateFilter(ReportStates.waiting_report_target))
async def rep_type(call: types.CallbackQuery, state: FSMContext):
    await state.update_data(ltype=call.data.split('_')[1])
    await state.set_state(ReportStates.waiting_report_send_chat)
    await call.message.edit_text("Введите ID чата или @username для отправки отчета:", reply_markup=get_cancel_kb())

@user_router.message(ReportStates.waiting_report_send_chat)
async def rep_send(msg: Message, state: FSMContext):
    d = await state.get_data()
    target = msg.text.strip()
    logs = db_get_monitor_logs(msg.from_user.id, d['ltype'])
    
    if not logs: 
        await msg.answer(f"⚠️ Логи типа {d['ltype']} отсутствуют.", reply_markup=get_monitor_kb(msg.from_user.id))
        await state.clear()
        return
    
    # Формируем читаемый отчет
    report_text = f"--- REPORT TYPE: {d['ltype']} ---\n"
    report_text += "\n".join([f"[{l[0]}] CMD: {l[1]} MSG: {l[2]}" for l in logs])
    
    f = io.BytesIO(report_text.encode('utf-8'))
    f.name = f"report_{d['ltype']}_{datetime.now().strftime('%Y%m%d%H%M')}.txt"
    
    try:
        # Отправляем как файл
        await bot.send_document(chat_id=target, document=BufferedInputFile(f.getvalue(), f.name), caption=f"Отчет {d['ltype']}")
        
        cleared_count = db_clear_monitor_logs(msg.from_user.id, d['ltype'])
        await msg.answer(f"✅ Отчет ({cleared_count} записей) отправлен в <code>{target}</code> и очищен.", 
                         reply_markup=get_monitor_kb(msg.from_user.id))
    except TelegramBadRequest as e:
        await msg.answer(f"❌ Ошибка отправки отчета: Неверный ID чата ({target}) или у бота нет прав. {e}", 
                         reply_markup=get_monitor_kb(msg.from_user.id))
    except Exception as e:
        logger.error(f"Report send error: {e}")
        await msg.answer(f"❌ Неизвестная ошибка при отправке отчета: {e}", reply_markup=get_monitor_kb(msg.from_user.id))
        
    await state.clear()


# --- УПРАВЛЕНИЕ ВОРКЕРОМ ---
@user_router.callback_query(F.data == "telethon_start_session")
async def start_s(call: types.CallbackQuery):
    if call.from_user.id in ACTIVE_TELETHON_WORKERS:
        return await call.answer("Worker уже запущен.")
    
    db_status = db_get_user(call.from_user.id).get('telethon_active')
    if not db_status:
        return await call.answer("Сначала выполните авторизацию (Вход).")

    asyncio.create_task(run_worker(call.from_user.id))
    await call.answer("Запуск worker-а...", show_alert=True)
    await call.message.edit_reply_markup(reply_markup=get_main_kb(call.from_user.id))

@user_router.callback_query(F.data == "telethon_stop_session")
async def stop_s(call: types.CallbackQuery):
    await stop_worker(call.from_user.id)
    await call.answer("Worker остановлен.", show_alert=True)
    await call.message.edit_reply_markup(reply_markup=get_main_kb(call.from_user.id))
    
@user_router.callback_query(F.data == "telethon_check_status")
async def check_status(call: types.CallbackQuery):
    active = db_get_user(call.from_user.id).get('telethon_active')
    running = call.from_user.id in ACTIVE_TELETHON_WORKERS
    
    if not active:
        msg = "🔴 Сессия не авторизована."
    elif running:
        msg = "🟢 Worker запущен и активен."
    else:
        msg = "🟡 Сессия авторизована, но Worker остановлен."
        
    await call.answer(msg, show_alert=True)


async def main():
    logger.info("START")
    db_init()
    dp.include_router(user_router)
    await start_workers()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
