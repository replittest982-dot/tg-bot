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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.filters.state import StateFilter 

# Импорты telethon
from telethon import TelegramClient, events
from telethon.errors import (
    UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, 
    PhoneNumberInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError, 
    AuthKeyUnregisteredError
)
from telethon.utils import get_display_name

# Импорт для QR-кода
import qrcode 

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# ВАШ ТОКЕН (ЖЕСТКО ПРОПИСАН)
BOT_TOKEN = "7868097991:AAEuHy_DYjEkBTK-H-U1P4-wZSdSw7evzEQ" 
ADMIN_ID = 6256576302  
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1" 
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальные хранилища
ACTIVE_TELETHON_CLIENTS = {} # Для запущенных воркеров (мониторинг)
ACTIVE_TELETHON_WORKERS = {} # Задачи asyncio
TEMP_AUTH_CLIENTS = {}       # ВАЖНО: Для хранения клиентов во время входа (чтобы код не сгорал)

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=storage)
user_router = Router()

# =========================================================================
# II. FSM И БД
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
    creating_promo_code = State()
    creating_promo_days = State()
    creating_promo_uses = State()
    sub_target_user_id = State()
    sub_duration_days = State()

class MonitorStates(StatesGroup):
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()

class ReportStates(StatesGroup):
    waiting_report_target = State()
    waiting_report_topic = State() 
    waiting_report_send_chat = State() 

class TelethonCommands(StatesGroup):
    waiting_ls_params = State()
    waiting_flood_params = State()
    waiting_check_params = State()

DB_PATH = os.path.join('data', DB_NAME)

def get_db_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def db_init():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, subscription_active BOOLEAN DEFAULT 0,
            subscription_end_date TEXT, telethon_active BOOLEAN DEFAULT 0,
            telethon_hash TEXT, promo_code TEXT, it_chat_id TEXT,
            drop_chat_id TEXT, report_chat_id TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY, days INTEGER, is_active BOOLEAN DEFAULT 1,
            max_uses INTEGER, current_uses INTEGER DEFAULT 0)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS monitor_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, timestamp TEXT,
            type TEXT, command TEXT, target TEXT, FOREIGN KEY (user_id) REFERENCES users(user_id))""")
    conn.commit()

def db_get_user(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    conn.commit()
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
    conn = get_db_connection()
    cur = conn.cursor()
    user = db_get_user(user_id)
    now = datetime.now(TIMEZONE_MSK)
    current_end = user.get('subscription_end_date')
    
    start_date = now
    if current_end:
        try:
            ce = TIMEZONE_MSK.localize(datetime.strptime(current_end, '%Y-%m-%d %H:%M:%S'))
            start_date = max(now, ce)
        except: pass
        
    new_end = (start_date + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("UPDATE users SET subscription_active=1, subscription_end_date=? WHERE user_id=?", (new_end, user_id))
    conn.commit()
    return new_end

def db_set_session_status(user_id, status):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
    conn.commit()

def db_add_monitor_log(user_id, log_type, command, target):
    conn = get_db_connection()
    cur = conn.cursor()
    ts = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("INSERT INTO monitor_logs (user_id, timestamp, type, command, target) VALUES (?, ?, ?, ?, ?)", 
                (user_id, ts, log_type, command, target))
    conn.commit()

def db_get_monitor_logs(user_id, log_type, days):
    conn = get_db_connection()
    cur = conn.cursor()
    query = "SELECT timestamp, command, target FROM monitor_logs WHERE user_id=? AND type=? "
    params = [user_id, log_type]
    if days > 0:
        cut = (datetime.now(TIMEZONE_MSK) - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        query += "AND timestamp >= ?"
        params.append(cut)
    query += " ORDER BY timestamp DESC"
    cur.execute(query, params)
    return cur.fetchall()

def db_clear_monitor_logs(user_id, log_type):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM monitor_logs WHERE user_id=? AND type=?", (user_id, log_type))
    conn.commit()
    return cur.rowcount

def db_set_chat_id(user_id, ctype, cid):
    conn = get_db_connection()
    cur = conn.cursor()
    col = 'it_chat_id' if ctype == 'IT' else 'drop_chat_id'
    cur.execute(f"UPDATE users SET {col}=? WHERE user_id=?", (cid, user_id))
    conn.commit()

def db_get_promo(code):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM promo_codes WHERE code=?", (code,))
    row = cur.fetchone()
    if row:
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))
    return None

def db_use_promo(code):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code=?", (code,))
    conn.commit()

def db_add_promo(code, days, max_uses):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO promo_codes (code, days, max_uses) VALUES (?, ?, ?)", (code, days, max_uses))
    conn.commit()

# =========================================================================
# IV. УТИЛИТЫ И КЛАВИАТУРЫ
# =========================================================================

def get_session_path(user_id):
    os.makedirs('data', exist_ok=True)
    return os.path.join('data', f'session_{user_id}')

async def check_access(user_id: int, bot: Bot):
    if user_id == ADMIN_ID: return True, ""
    if db_check_subscription(user_id): return True, ""
    try:
        m = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
        if m.status in ["member", "administrator", "creator"]: return True, ""
    except: pass
    return False, f"❌ Нет доступа. Подпишитесь на {TARGET_CHANNEL_URL} или введите промокод."

def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

def get_numeric_kb():
    kb = []
    for i in range(1, 10, 3):
        kb.append([InlineKeyboardButton(text=str(j), callback_data=f"auth_digit_{j}") for j in range(i, i+3)])
    kb.append([
        InlineKeyboardButton(text="⬅️", callback_data="auth_delete_digit"),
        InlineKeyboardButton(text="0", callback_data="auth_digit_0"),
        InlineKeyboardButton(text="✅ Ввод", callback_data="auth_submit_code")
    ])
    kb.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_main_kb(user_id):
    user = db_get_user(user_id)
    active = user.get('telethon_active')
    running = user_id in ACTIVE_TELETHON_WORKERS
    
    kb = []
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    
    if not active:
        kb.append([InlineKeyboardButton(text="🔐 Авторизация", callback_data="telethon_auth_start")])
    else:
        kb.append([
            InlineKeyboardButton(text="🔥 Инструменты", callback_data="show_telethon_tools"),
            InlineKeyboardButton(text="📊 Мониторинг", callback_data="show_monitor_menu")
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
        [InlineKeyboardButton(text="📄 Отчет", callback_data="monitor_generate_report_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_tools_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 .лс", callback_data="cmd_ls_start"),
         InlineKeyboardButton(text="💥 .флуд", callback_data="cmd_flood_start")],
        [InlineKeyboardButton(text="🛑 Стоп Флуд", callback_data="cmd_stop_flood"),
         InlineKeyboardButton(text="🔬 .чекгруппу", callback_data="cmd_check_group_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Промокод", callback_data="admin_create_promo_start"),
         InlineKeyboardButton(text="➡️ Подписка", callback_data="admin_issue_sub_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

# =========================================================================
# V. TELETHON WORKER
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
        
        user = db_get_user(user_id)
        it_chat = user.get('it_chat_id')
        drop_chat = user.get('drop_chat_id')
        
        IT_REGEX = {k: r'.*' for k in ['.встал', '.кьар', '.ошибка']} # Упрощенный regex
        DROP_REGEX = r'^\+?\d{5,15}.*' # Упрощенный regex

        @client.on(events.NewMessage)
        async def handler(event):
            if not event.text: return
            chat = await event.get_chat()
            cid = str(chat.id)
            
            if it_chat and cid in it_chat:
                for cmd in IT_REGEX:
                    if event.text.lower().startswith(cmd):
                        db_add_monitor_log(user_id, 'IT', cmd, event.text)
            
            if drop_chat and cid in drop_chat:
                if re.match(DROP_REGEX, event.text):
                    db_add_monitor_log(user_id, 'DROP', 'DROP', event.text)

        await client.run_until_disconnected()
        
    except Exception as e:
        logger.error(f"Worker {user_id} error: {e}")
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS: del ACTIVE_TELETHON_CLIENTS[user_id]
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
    await state.clear()
    await call.message.delete()
    await cmd_start(call.message, state)

@user_router.callback_query(F.data == "back_to_main")
@user_router.message(Command("start"))
async def cmd_start(u: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = u.from_user.id
    db_get_user(user_id)
    
    has, msg = await check_access(user_id, bot)
    kb = get_main_kb(user_id)
    
    text = f"👋 <b>Привет!</b> Ваш ID: <code>{user_id}</code>\n"
    sub = db_get_user(user_id).get('subscription_end_date')
    text += f"Подписка до: <code>{sub if sub else 'Нет'}</code>\n\n"
    
    if not has:
        text += f"⚠️ <b>Доступ закрыт.</b>\nПодпишитесь на {TARGET_CHANNEL_URL} или введите Промокод."
    else:
        text += "✅ <b>Меню доступно.</b>"

    if isinstance(u, types.Message): await u.answer(text, reply_markup=kb)
    else: await u.message.edit_text(text, reply_markup=kb)

# --- АВТОРИЗАЦИЯ (ИСПРАВЛЕННАЯ) ---

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
    try:
        await client.connect()
        res = await client.send_code_request(phone)
        # ВАЖНО: НЕ ОТКЛЮЧАЕМ КЛИЕНТ, СОХРАНЯЕМ В ГЛОБАЛЬНЫЙ СЛОВАРЬ
        TEMP_AUTH_CLIENTS[msg.from_user.id] = client 
        
        await state.update_data(phone=phone, phone_hash=res.phone_code_hash)
        await state.set_state(TelethonAuth.CODE)
        await msg.answer("🔢 Введите код:", reply_markup=get_numeric_kb())
    except Exception as e:
        await msg.answer(f"Ошибка: {e}")
        await client.disconnect()

@user_router.callback_query(F.data.startswith("auth_digit_") | (F.data == "auth_delete_digit"))
async def auth_ui_digit(call: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get('code_tmp', '')
    if "digit_" in call.data: code += call.data.split("_")[-1]
    elif "delete" in call.data: code = code[:-1]
    await state.update_data(code_tmp=code)
    try: await call.message.edit_text(f"Код: {code}", reply_markup=get_numeric_kb())
    except: pass

@user_router.callback_query(F.data == "auth_submit_code")
async def auth_ui_submit(call: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get('code_tmp', '')
    await auth_code_logic(call.message, state, code, call.from_user.id)

@user_router.message(TelethonAuth.CODE)
async def auth_msg_code(msg: Message, state: FSMContext):
    await auth_code_logic(msg, state, msg.text.strip(), msg.from_user.id)

async def auth_code_logic(msg, state, code, uid):
    # БЕРЕМ КЛИЕНТ ИЗ ПАМЯТИ, А НЕ СОЗДАЕМ НОВЫЙ
    client = TEMP_AUTH_CLIENTS.get(uid)
    if not client:
        await msg.answer("⚠️ Сессия истекла. Начните заново.")
        return

    d = await state.get_data()
    try:
        await client.sign_in(d['phone'], code, phone_code_hash=d['phone_hash'])
        # Если успешно
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Успешно вошли!", reply_markup=get_main_kb(uid))
        await state.clear()
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await msg.answer("🔒 Введите пароль 2FA:")
    except Exception as e:
        await msg.answer(f"Ошибка: {e}")
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]

@user_router.message(TelethonAuth.PASSWORD)
async def auth_pwd(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    if not client:
        await msg.answer("⚠️ Сессия истекла.")
        return
    try:
        await client.sign_in(password=msg.text.strip())
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Успешно вошли (2FA)!", reply_markup=get_main_kb(uid))
        await state.clear()
    except Exception as e:
        await msg.answer(f"Ошибка: {e}")

# --- QR CODE ---
@user_router.callback_query(F.data == "auth_method_qr")
async def auth_qr(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    client = TelegramClient(get_session_path(uid), API_ID, API_HASH)
    await client.connect()
    
    qr_login = await client.qr_login()
    img = qrcode.make(qr_login.url)
    bio = io.BytesIO()
    img.save(bio, 'PNG')
    bio.seek(0)
    
    m = await call.message.answer_photo(BufferedInputFile(bio.read(), 'qr.png'), caption="Сканируйте QR", reply_markup=get_cancel_kb())
    asyncio.create_task(wait_qr(client, uid, qr_login, m))

async def wait_qr(client, uid, qr_login, m):
    try:
        await qr_login.wait(120)
        await client.disconnect()
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await m.answer("✅ Вход по QR успешен!")
    except:
        await m.answer("❌ Время вышло.")
        await client.disconnect()

# --- УПРАВЛЕНИЕ ---
@user_router.callback_query(F.data == "telethon_start_session")
async def start_s(call: types.CallbackQuery):
    asyncio.create_task(run_worker(call.from_user.id))
    await call.answer("Запуск...", show_alert=True)
    await call.message.edit_reply_markup(reply_markup=get_main_kb(call.from_user.id))

@user_router.callback_query(F.data == "telethon_stop_session")
async def stop_s(call: types.CallbackQuery):
    await stop_worker(call.from_user.id)
    await call.answer("Остановлено.", show_alert=True)
    await call.message.edit_reply_markup(reply_markup=get_main_kb(call.from_user.id))

@user_router.callback_query(F.data == "telethon_check_status")
async def check_s(call: types.CallbackQuery):
    active = call.from_user.id in ACTIVE_TELETHON_WORKERS
    await call.answer(f"Статус: {'🟢 Работает' if active else '🔴 Стоит'}", show_alert=True)

# --- ПРОМОКОДЫ ---
@user_router.callback_query(F.data == "start_promo_fsm")
async def promo_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await call.message.edit_text("Введите код:", reply_markup=get_cancel_kb())

@user_router.message(PromoStates.waiting_for_code)
async def promo_check(msg: Message, state: FSMContext):
    code = msg.text.strip()
    promo = db_get_promo(code)
    if promo and promo['is_active'] and (not promo['max_uses'] or promo['current_uses'] < promo['max_uses']):
        db_use_promo(code)
        end = db_update_subscription(msg.from_user.id, promo['days'])
        await msg.answer(f"✅ Успех! Подписка до {end}", reply_markup=get_main_kb(msg.from_user.id))
    else:
        await msg.answer("❌ Неверный код.")
    await state.clear()

# --- АДМИНКА ---
@user_router.callback_query(F.data == "admin_panel_start")
async def admin_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("Админка:", reply_markup=get_admin_kb())

@user_router.callback_query(F.data == "admin_create_promo_start")
async def admin_promo(call: types.CallbackQuery, state: FSMContext):
    code = generate_promo_code()
    await state.update_data(code=code)
    await state.set_state(AdminStates.creating_promo_days)
    await call.message.edit_text(f"Код: {code}\nВведите дни:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.creating_promo_days)
async def admin_days(msg: Message, state: FSMContext):
    await state.update_data(days=int(msg.text))
    await state.set_state(AdminStates.creating_promo_uses)
    await msg.answer("Лимит (0 - безлимит):")

@user_router.message(AdminStates.creating_promo_uses)
async def admin_uses(msg: Message, state: FSMContext):
    d = await state.get_data()
    limit = int(msg.text)
    db_add_promo(d['code'], d['days'], limit if limit > 0 else None)
    await msg.answer("✅ Промокод создан!", reply_markup=get_admin_kb())
    await state.clear()

# --- МОНИТОРИНГ ---
@user_router.callback_query(F.data == "show_monitor_menu")
async def mon_menu(call: types.CallbackQuery):
    await call.message.edit_text("Меню мониторинга:", reply_markup=get_monitor_kb(call.from_user.id))

@user_router.callback_query(F.data.startswith("monitor_set_"))
async def mon_set(call: types.CallbackQuery, state: FSMContext):
    ctype = call.data.split('_')[-1].upper()
    await state.update_data(ctype=ctype)
    await state.set_state(MonitorStates.waiting_for_it_chat_id) # Используем одно состояние для простоты
    await call.message.edit_text(f"Введите ID для {ctype}:", reply_markup=get_cancel_kb())

@user_router.message(MonitorStates.waiting_for_it_chat_id)
async def mon_save(msg: Message, state: FSMContext):
    d = await state.get_data()
    db_set_chat_id(msg.from_user.id, d['ctype'], msg.text.strip())
    await msg.answer("✅ Сохранено. Перезапустите воркер.", reply_markup=get_monitor_kb(msg.from_user.id))
    await state.clear()

async def main():
    logger.info("START")
    db_init()
    dp.include_router(user_router)
    await start_workers()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
