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
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, StateFilter
from aiogram.client.default import DefaultBotProperties

# Импорты telethon
from telethon import TelegramClient, events, functions, types as tl_types
from telethon.errors import (
    UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, 
    PhoneNumberInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError, 
    AuthKeyUnregisteredError, PeerIdInvalidError
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
TARGET_CHANNEL_URL = "@STAT_PRO1" 
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальные хранилища
ACTIVE_TELETHON_CLIENTS = {} 
ACTIVE_TELETHON_WORKERS = {} 
TEMP_AUTH_CLIENTS = {} # Для процесса входа
FLOOD_TASKS = {} # Для остановки флуда {user_id: bool_run_flag}

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
    # Создание промокода
    promo_code_input = State()
    promo_days_input = State()
    promo_uses_input = State()
    # Выдача подписки
    sub_user_id_input = State()
    sub_days_input = State()

class MonitorStates(StatesGroup):
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()

class ReportStates(StatesGroup):
    waiting_report_target = State() # Выбор типа (IT/DROP)
    waiting_report_send_chat = State() # Куда отправить

# =========================================================================
# III. БАЗА ДАННЫХ
# =========================================================================

DB_PATH = os.path.join('data', DB_NAME)

def get_db_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def db_init():
    conn = get_db_connection()
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
            if ce > now: start_date = ce
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

def db_add_monitor_log(user_id, log_type, command, message):
    conn = get_db_connection()
    cur = conn.cursor()
    ts = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("INSERT INTO monitor_logs (user_id, timestamp, type, command, message) VALUES (?, ?, ?, ?, ?)", 
                (user_id, ts, log_type, command, message))
    conn.commit()

def db_get_monitor_logs(user_id, log_type):
    conn = get_db_connection()
    cur = conn.cursor()
    # Получаем все логи выбранного типа
    cur.execute("SELECT timestamp, command, message FROM monitor_logs WHERE user_id=? AND type=? ORDER BY timestamp DESC", (user_id, log_type))
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
    cur.execute("INSERT OR REPLACE INTO promo_codes (code, days, max_uses) VALUES (?, ?, ?)", (code, days, max_uses))
    conn.commit()

def db_get_active_telethon_users():
    conn = get_db_connection()
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
    if user_id == ADMIN_ID: return True, ""
    
    # 1. Подписка на канал
    try:
        m = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
        if m.status not in ["member", "administrator", "creator"]:
             return False, f"❌ Подпишитесь на канал {TARGET_CHANNEL_URL}"
    except Exception as e:
        logger.error(f"Check channel error: {e}")
        # Если ошибка проверки, лучше пустить или попросить админа проверить
        pass 

    # 2. Активная подписка в боте
    if db_check_subscription(user_id): return True, ""
    
    return False, "❌ Ваша подписка истекла. Активируйте промокод."

def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

def get_main_kb(user_id):
    user = db_get_user(user_id)
    active = user.get('telethon_active')
    running = user_id in ACTIVE_TELETHON_WORKERS
    
    kb = []
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    
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

def get_channel_sub_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Подписаться", url=f"https://t.me/{TARGET_CHANNEL_URL.replace('@', '')}")],
        [InlineKeyboardButton(text="✅ Я подписался", callback_data="back_to_main")]
    ])

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
            return

        await client.start()
        db_set_session_status(user_id, True)
        
        # Регулярки
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
            if user['it_chat_id'] and chat_id == user['it_chat_id']:
                for cmd in IT_REGEX:
                    if event.text.lower().startswith(cmd):
                        db_add_monitor_log(user_id, 'IT', cmd, event.text)
                        break
            
            if user['drop_chat_id'] and chat_id == user['drop_chat_id']:
                if re.match(DROP_REGEX, event.text, re.IGNORECASE):
                    db_add_monitor_log(user_id, 'DROP', 'DROP_ENTRY', event.text)

            # 2. Инструменты (работают ВЕЗДЕ, если отправлены самим юзером)
            if event.out:
                msg = event.text.strip()
                parts = msg.split()
                cmd = parts[0].lower()

                if cmd == '.лс' and len(parts) >= 3:
                    # .лс Привет @user1 @user2
                    text_to_send = parts[1] # Упрощенно (одно слово), лучше использовать msg.split(maxsplit=...)
                    # Сложный парсинг для текста с пробелами:
                    # Находим, где начинаются юзернеймы (обычно с @ или цифры)
                    # Для простоты берем последнее слово как юзернейм, остальное текст
                    # Но по ТЗ: .лс [текст] [список]
                    targets = [p for p in parts if p.startswith('@') or p.lstrip('-').isdigit()]
                    text_content = msg.replace(cmd, '').replace(' '.join(targets), '').strip()
                    
                    status_msg = await event.reply(f"🚀 Рассылка: {len(targets)} шт.")
                    for i, target in enumerate(targets):
                        try:
                            await client.send_message(target, text_content)
                            await asyncio.sleep(1)
                        except: pass
                    await status_msg.edit("✅ Рассылка завершена.")

                elif cmd == '.флуд' and len(parts) >= 4:
                    # .флуд 10 Текст 0.5 chat_id
                    try:
                        count = int(parts[1])
                        delay = float(parts[3])
                        target_chat = parts[4]
                        flood_text = parts[2] # Упрощенно
                        
                        FLOOD_TASKS[user_id] = True
                        status = await event.reply(f"🌊 Флуд запущен: 0/{count}")
                        
                        for i in range(count):
                            if not FLOOD_TASKS.get(user_id): 
                                await status.edit("🛑 Флуд остановлен пользователем.")
                                break
                            await client.send_message(target_chat, flood_text)
                            if i % 5 == 0: # Обновляем статус каждые 5 сообщений
                                bar = await progress_bar(i, count)
                                try: await status.edit(f"🌊 {bar} {i}/{count}\nДля стопа: .стопфлуд")
                                except: pass
                            await asyncio.sleep(delay)
                        if FLOOD_TASKS.get(user_id): await status.edit("✅ Флуд завершен.")
                    except Exception as e:
                        await event.reply(f"Ошибка флуда: {e}")

                elif cmd == '.стопфлуд':
                    FLOOD_TASKS[user_id] = False
                    await event.reply("🛑 Команда остановки принята.")

                elif cmd == '.чекгруппу' and len(parts) >= 2:
                    target_chat = parts[1]
                    status = await event.reply("🔬 Анализ группы...")
                    try:
                        entity = await client.get_entity(target_chat)
                        participants = await client.get_participants(entity)
                        total = len(participants)
                        bots = len([p for p in participants if p.bot])
                        real = total - bots
                        await status.edit(
                            f"📊 <b>Анализ {get_display_name(entity)}</b>\n"
                            f"Всего: {total}\n"
                            f"Людей: {real}\n"
                            f"Ботов: {bots}"
                        )
                    except Exception as e:
                        await status.edit(f"Ошибка: {e}")

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
        text += f"⚠️ <b>Доступ ограничен.</b>\n{msg}"
        # Если нет доступа - даем кнопку подписки
        kb = get_channel_sub_kb()
        # Но добавляем кнопку промокода и админку
        rows = []
        rows.append([InlineKeyboardButton(text="➕ Подписаться", url=f"https://t.me/{TARGET_CHANNEL_URL.replace('@', '')}")])
        rows.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="back_to_main")])
        rows.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
        if user_id == ADMIN_ID: rows.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
    else:
        text += "✅ <b>Меню доступно.</b>\nИспользуйте кнопки ниже."

    if isinstance(u, types.Message): await u.answer(text, reply_markup=kb)
    else: await u.message.edit_text(text, reply_markup=kb)

# --- АВТОРИЗАЦИЯ (С фиксом expired code) ---

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
        # ВАЖНО: Храним клиент, чтобы код не сгорел
        TEMP_AUTH_CLIENTS[msg.from_user.id] = client 
        
        await state.update_data(phone=phone, phone_hash=res.phone_code_hash)
        await state.set_state(TelethonAuth.CODE)
        await msg.answer("🔢 Введите код:", reply_markup=get_cancel_keyboard()) # Можно использовать get_numeric_kb()
    except Exception as e:
        await msg.answer(f"Ошибка: {e}")
        await client.disconnect()

@user_router.message(TelethonAuth.CODE)
async def auth_msg_code(msg: Message, state: FSMContext):
    code = msg.text.strip()
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    
    if not client:
        await msg.answer("⚠️ Сессия истекла. Начните заново.")
        return

    d = await state.get_data()
    try:
        await client.sign_in(d['phone'], code, phone_code_hash=d['phone_hash'])
        # Успех
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Успешно вошли!", reply_markup=get_main_kb(uid))
        await state.clear()
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await msg.answer("🔒 Введите пароль 2FA:", reply_markup=get_cancel_kb())
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

# --- QR CODE (ВОССТАНОВЛЕН) ---
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
        await m.edit_caption("✅ Вход по QR успешен!")
    except:
        try: await m.edit_caption("❌ Время вышло.")
        except: pass
        await client.disconnect()

# --- АДМИНКА (ИСПРАВЛЕНА И РАБОТАЕТ) ---

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(AdminStates.main_menu)
    await call.message.edit_text("👑 Админ-панель:", reply_markup=get_admin_kb())

# 1. Создание промокода
@user_router.callback_query(F.data == "admin_create_promo")
async def adm_create_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.promo_code_input)
    await call.message.edit_text("Введите название промокода (или 'auto' для случайного):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_code_input)
async def adm_promo_name(msg: Message, state: FSMContext):
    code = msg.text.strip()
    if code.lower() == 'auto':
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    await state.update_data(code=code)
    await state.set_state(AdminStates.promo_days_input)
    await msg.answer(f"Код: {code}. Введите кол-во дней:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_days_input)
async def adm_promo_days(msg: Message, state: FSMContext):
    if not msg.text.isdigit(): return await msg.answer("Число!")
    await state.update_data(days=int(msg.text))
    await state.set_state(AdminStates.promo_uses_input)
    await msg.answer("Введите лимит использований (0 - безлимит):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_uses_input)
async def adm_promo_final(msg: Message, state: FSMContext):
    if not msg.text.isdigit(): return await msg.answer("Число!")
    d = await state.get_data()
    limit = int(msg.text)
    db_add_promo(d['code'], d['days'], limit if limit > 0 else None)
    await msg.answer(f"✅ Промокод <code>{d['code']}</code> создан!", reply_markup=get_admin_kb())
    await state.clear()

# 2. Выдача подписки
@user_router.callback_query(F.data == "admin_grant_sub")
async def adm_grant_sub(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.sub_user_id_input)
    await call.message.edit_text("Введите ID пользователя:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_user_id_input)
async def adm_sub_id(msg: Message, state: FSMContext):
    if not msg.text.isdigit(): return await msg.answer("Число!")
    await state.update_data(uid=int(msg.text))
    await state.set_state(AdminStates.sub_days_input)
    await msg.answer("На сколько дней выдать:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_days_input)
async def adm_sub_final(msg: Message, state: FSMContext):
    if not msg.text.isdigit(): return await msg.answer("Число!")
    d = await state.get_data()
    end = db_update_subscription(d['uid'], int(msg.text))
    await msg.answer(f"✅ Подписка выдана ID {d['uid']} до {end}", reply_markup=get_admin_kb())
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
        await msg.answer(f"✅ Активировано! Подписка до {end}", reply_markup=get_main_kb(msg.from_user.id))
    else:
        await msg.answer("❌ Неверный или истекший код.")
    await state.clear()

# --- ОТЧЕТЫ И МОНИТОРИНГ (ПОЛНАЯ) ---

@user_router.callback_query(F.data == "show_monitor_menu")
async def mon_menu(call: types.CallbackQuery):
    await call.message.edit_text("Меню мониторинга:", reply_markup=get_monitor_kb(call.from_user.id))

@user_router.callback_query(F.data.startswith("monitor_set_"))
async def mon_set(call: types.CallbackQuery, state: FSMContext):
    ctype = call.data.split('_')[-1].upper()
    await state.update_data(ctype=ctype)
    await state.set_state(MonitorStates.waiting_for_it_chat_id)
    await call.message.edit_text(f"Введите ID для {ctype}:", reply_markup=get_cancel_kb())

@user_router.message(MonitorStates.waiting_for_it_chat_id)
async def mon_save(msg: Message, state: FSMContext):
    d = await state.get_data()
    db_set_chat_id(msg.from_user.id, d['ctype'], msg.text.strip())
    await msg.answer("✅ Сохранено. Перезапустите воркер.", reply_markup=get_monitor_kb(msg.from_user.id))
    await state.clear()

@user_router.callback_query(F.data == "monitor_generate_report_start")
async def rep_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(ReportStates.waiting_report_target)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="IT", callback_data="rep_IT"), InlineKeyboardButton(text="DROP", callback_data="rep_DROP")]
    ])
    await call.message.edit_text("Выберите тип логов:", reply_markup=kb)

@user_router.callback_query(F.data.startswith("rep_"))
async def rep_type(call: types.CallbackQuery, state: FSMContext):
    await state.update_data(ltype=call.data.split('_')[1])
    await state.set_state(ReportStates.waiting_report_send_chat)
    await call.message.edit_text("Введите ID чата для отправки отчета:", reply_markup=get_cancel_kb())

@user_router.message(ReportStates.waiting_report_send_chat)
async def rep_send(msg: Message, state: FSMContext):
    d = await state.get_data()
    target = msg.text.strip()
    logs = db_get_monitor_logs(msg.from_user.id, d['ltype'])
    
    if not logs: return await msg.answer("Пусто.")
    
    text = "\n".join([f"[{l[0]}] {l[1]}: {l[2]}" for l in logs])
    f = io.BytesIO(text.encode('utf-8'))
    f.name = "report.txt"
    
    try:
        # Пробуем отправить как файл
        await bot.send_document(chat_id=target, document=BufferedInputFile(f.getvalue(), "report.txt"), caption="Отчет", message_thread_id=1)
        db_clear_monitor_logs(msg.from_user.id, d['ltype'])
        await msg.answer("✅ Отправлено и очищено.")
    except Exception as e:
        await msg.answer(f"Ошибка отправки: {e}")
    await state.clear()

# --- УПРАВЛЕНИЕ ВОРКЕРОМ ---
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

async def main():
    logger.info("START")
    db_init()
    dp.include_router(user_router)
    await start_workers()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
