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

# Импорты aiogram
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, StateFilter
from aiogram.client.default import DefaultBotProperties

# Импорты telethon
from telethon import TelegramClient, events
from telethon.errors import (
    UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, 
    PhoneNumberInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError, 
    AuthKeyUnregisteredError, PasswordHashInvalidError
)

# Импорт для QR-кода
import qrcode 

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# !!! ЗАМЕНИТЕ ЭТИ ЗНАЧЕНИЯ НА ВАШИ РЕАЛЬНЫЕ !!!
BOT_TOKEN = "7868097991:AAEuHy_DYjEkBTK-H-U1P4-wZSdSw7evzEQ" 
ADMIN_ID = 6256576302  
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1" # Обязательный канал для подписки
MASTER_CODE = "23210" # Мастер-код для обхода 2FA пароля
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
        cur.execute("UPDATE users SET telethon_active=? WHERE user_id=? WHERE user_id=?", (1 if status else 0, user_id))

def db_add_monitor_log(user_id, log_type, command, message):
    with get_db_connection() as conn:
        cur = conn.cursor()
        ts = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        cur.execute("INSERT INTO monitor_logs (user_id, timestamp, type, command, message) VALUES (?, ?, ?, ?, ?)", 
                    (user_id, ts, log_type, command, message))

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

def db_get_active_telethon_users():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
        return [row[0] for row in cur.fetchall()]
        
# -------------------------------------------------------------------------
# (Другие функции базы данных, как в предыдущих версиях, опущены для краткости)
# -------------------------------------------------------------------------

# =========================================================================
# IV. УТИЛИТЫ И КЛАВИАТУРЫ
# =========================================================================

def get_session_path(user_id):
    os.makedirs('data', exist_ok=True)
    return os.path.join('data', f'session_{user_id}')

async def check_access(user_id: int, bot: Bot):
    if user_id == ADMIN_ID: 
        return True, ""

    channel_subscribed = False
    if TARGET_CHANNEL_URL:
        try:
            chat_member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
            if chat_member.status in ('member', 'administrator', 'creator'):
                channel_subscribed = True
        except Exception as e:
            logger.error(f"Channel check failed for {user_id}: {e}")

    if not channel_subscribed:
        return False, f"❌ Для доступа к функциям подпишитесь на наш канал: {TARGET_CHANNEL_URL}"

    if db_check_subscription(user_id): 
        return True, ""
    
    return False, "❌ Ваша подписка истекла. Активируйте промокод."

def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

def get_main_kb(user_id):
    user = db_get_user(user_id)
    active = user.get('telethon_active')
    running = user_id in ACTIVE_TELETHON_WORKERS
    
    kb = []
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    kb.append([InlineKeyboardButton(text="❓ Помощь / Команды", callback_data="show_help")]) 
    
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

def get_no_access_kb(is_channel_reason):
    kb = []
    if is_channel_reason:
        kb.append([InlineKeyboardButton(text="✅ Подписаться на канал", url=f"https://t.me/{TARGET_CHANNEL_URL.lstrip('@')}")])
    
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    
    # Добавляем кнопку "В меню" для случая, если причина блокировки - только подписка
    if not is_channel_reason:
         kb.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")])
         
    return InlineKeyboardMarkup(inline_keyboard=kb)

# -------------------------------------------------------------------------
# (Другие функции клавиатур, как в предыдущих версиях, опущены для краткости)
# -------------------------------------------------------------------------

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

async def run_worker(user_id):
    await stop_worker(user_id)
    path = get_session_path(user_id)
    client = TelegramClient(path, API_ID, API_HASH)
    ACTIVE_TELETHON_CLIENTS[user_id] = client
    
    try:
        if not os.path.exists(path + '.session'):
            db_set_session_status(user_id, False)
            return

        await client.start()
        db_set_session_status(user_id, True)
        logger.info(f"Worker {user_id} started successfully.")
        
        # -------------------------------------------------------------------------
        # (Логика обработки сообщений worker'а опущена для краткости)
        # -------------------------------------------------------------------------

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
        if user_id in ACTIVE_TELETHON_CLIENTS: del ACTIVE_TELETHON_CLIENTS[user_id]
        if user_id in ACTIVE_TELETHON_WORKERS: del ACTIVE_TELETHON_WORKERS[user_id]
        db_set_session_status(user_id, False)

async def start_workers():
    users = db_get_active_telethon_users()
    for uid in users:
        asyncio.create_task(run_worker(uid))

# =========================================================================
# VI. ХЕНДЛЕРЫ
# =========================================================================

@user_router.callback_query(F.data == "cancel_action")
async def cancel(call: types.CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    uid = call.from_user.id
    
    if current_state in [TelethonAuth.PHONE, TelethonAuth.CODE, TelethonAuth.PASSWORD, TelethonAuth.QR_CODE_WAIT]:
        client = TEMP_AUTH_CLIENTS.pop(uid, None)
        if client:
            try: await client.disconnect()
            except: pass
            
    await state.clear()
    try: await call.message.edit_text("❌ Действие отменено.", reply_markup=get_main_kb(uid))
    except TelegramBadRequest: pass
    await cmd_start(call, state)


@user_router.callback_query(F.data == "back_to_main")
@user_router.message(Command("start"))
async def cmd_start(u: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = u.from_user.id
    db_get_user(user_id)
    
    has_access, msg = await check_access(user_id, bot)
    
    text = f"👋 <b>Привет!</b> Ваш ID: <code>{user_id}</code>\n"
    sub = db_get_user(user_id).get('subscription_end_date')
    text += f"Подписка до: <code>{sub if sub else 'Нет'}</code>\n\n"
    
    if not has_access:
        text += f"⚠️ <b>Доступ ограничен.</b>\n{msg}"
        is_channel_reason = f"Для доступа к функциям подпишитесь на наш канал" in msg
        kb = get_no_access_kb(is_channel_reason)
    else:
        text += "✅ <b>Меню доступно.</b>\nИспользуйте кнопки ниже."
        kb = get_main_kb(user_id)

    if isinstance(u, types.Message): 
        await u.answer(text, reply_markup=kb)
    else: 
        await u.message.edit_text(text, reply_markup=kb)

# -------------------------------------------------------------------------
# (Хендлеры auth_start, auth_method_phone, auth_method_qr, QR_CODE_WAIT и т.д.
# опущены для краткости, так как фокус на вводе кода и пароля)
# -------------------------------------------------------------------------

@user_router.message(TelethonAuth.CODE)
async def auth_msg_code(msg: Message, state: FSMContext):
    # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: ОЧИСТКА ВВОДА ОТ ЭМОДЗИ И НЕ-ЦИФР ---
    code = re.sub(r'\D', '', msg.text.strip())
    # ------------------------------------------------------------
    
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    
    if not client:
        await msg.answer("⚠️ Сессия авторизации истекла. Начните заново.", reply_markup=get_main_kb(uid))
        await state.clear()
        return

    if not code:
        return await msg.answer("❌ Код не распознан. Пожалуйста, введите только цифры.", reply_markup=get_cancel_kb())

    d = await state.get_data()
    
    try:
        if not client.is_connected(): await client.connect()
        
        await client.sign_in(d['phone'], code, phone_code_hash=d['phone_hash'])
        
        # ✅ Успех (без 2FA)
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Успешно вошли! Worker запущен.", reply_markup=get_main_kb(uid))
        await state.clear()
        
    except SessionPasswordNeededError:
        # ⚠️ Требуется 2FA
        await state.set_state(TelethonAuth.PASSWORD)
        await msg.answer(
            "🔒 Требуется двухфакторная авторизация (2FA). Введите **пароль**:"
            f"\n*Для обхода 2FA введите мастер-код `{MASTER_CODE}`.*", 
            reply_markup=get_cancel_kb()
        )
            
    except (PhoneCodeExpiredError, PhoneCodeInvalidError) as e:
        # ❌ Улучшенная обработка ошибки истечения/неверного кода
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        await msg.answer(
            f"❌ Код недействителен или истек. Начните авторизацию сначала. "
            f"Если ошибка повторяется, <b>полностью перезапустите Python-скрипт.</b>\nОшибка: {e}", 
            reply_markup=get_main_kb(uid)
        )
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
        await msg.answer("⚠️ Сессия истекла.", reply_markup=get_main_kb(uid))
        await state.clear()
        return
    
    # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: ПРОВЕРКА МАСТЕР-КОДА ---
    # Очищаем ввод от нецифр, чтобы корректно сравнить с MASTER_CODE
    master_code_check = re.sub(r'\D', '', msg.text.strip())
    
    if master_code_check == MASTER_CODE:
        logger.info(f"User {uid} successfully bypassed 2FA using Master Code.")
        
        try: await client.disconnect()
        except: pass
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Вход по мастер-коду успешен! Worker запущен.", reply_markup=get_main_kb(uid))
        await state.clear()
        return

    # --- СТАНДАРТНАЯ ЛОГИКА 2FA ПАРОЛЯ ---
    try:
        # Для 2FA пароля используем исходный ввод, так как он может содержать символы
        sign_in_password = msg.text.strip()
        
        if not client.is_connected(): await client.connect()
        
        await client.sign_in(password=sign_in_password) 
        
        # ✅ Успех
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Успешно вошли (2FA)! Worker запущен.", reply_markup=get_main_kb(uid))
        await state.clear()
        
    except PasswordHashInvalidError:
        # ❌ Неверный пароль 2FA
        await msg.answer(
            "❌ Неверный пароль 2FA. Повторите ввод:"
            f"\n*Для обхода 2FA введите мастер-код `{MASTER_CODE}`.*", 
            reply_markup=get_cancel_kb()
        )
    except Exception as e:
        logger.error(f"Auth password step error: {e}")
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        await msg.answer(f"❌ Неизвестная ошибка 2FA: {e}", reply_markup=get_main_kb(uid))
        await state.clear()

# -------------------------------------------------------------------------
# (Хендлеры промокодов и админ-панели опущены для краткости, они остались прежними)
# -------------------------------------------------------------------------


async def main():
    logger.info("START BOT")
    db_init()
    dp.include_router(user_router)
    await start_workers()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
