import asyncio
import logging
import os
import sqlite3
import pytz
import re
import tempfile
import io
import random
import string
from datetime import datetime, timedelta
from typing import Dict, Union, Optional

# =========================================================================
# I. КОНФИГУРАЦИЯ И НАСТРОЙКА
# =========================================================================

# --- ВАШИ КЛЮЧИ ---
BOT_TOKEN = "7868097991:AAFWAAw1357IWkGXr9cOpqY11xBtnB0xJSg"
ADMIN_ID = 6256576302
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1"
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
DB_TIMEOUT = 10

# --- ПУТИ ---
DATA_DIR = 'data'
SESSION_DIR = 'sessions'

if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
if not os.path.exists(SESSION_DIR): os.makedirs(SESSION_DIR)

DB_PATH = os.path.join(DATA_DIR, DB_NAME)
PROXY_CONFIG = None

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- ГЛОБАЛЬНЫЕ ХРАНИЛИЩА ---
ACTIVE_TELETHON_CLIENTS: Dict[int, 'TelegramClient'] = {}
ACTIVE_TELETHON_WORKERS: Dict[int, asyncio.Task] = {}
TEMP_AUTH_CLIENTS: Dict[int, 'TelegramClient'] = {}
FLOOD_TASKS: Dict[int, Dict[int, asyncio.Task]] = {}
PROCESS_PROGRESS: Dict[int, Dict] = {}

# --- ИМПОРТЫ ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, FSInputFile, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, StateFilter
from aiogram.client.default import DefaultBotProperties

from telethon import TelegramClient, events
from telethon.tl.types import User
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneCodeExpiredError, PhoneCodeInvalidError,
    PasswordHashInvalidError, UsernameInvalidError, PeerIdInvalidError,
    RpcCallFailError, ApiIdInvalidError, PhoneNumberInvalidError, AuthKeyUnregisteredError
)
from telethon.utils import get_display_name
from telethon.tl.custom import Button

# --- ИНИЦИАЛИЗАЦИЯ ---
storage = MemoryStorage()
default_properties = DefaultBotProperties(parse_mode='HTML')
bot = Bot(token=BOT_TOKEN, default=default_properties)
dp = Dispatcher(storage=storage)
user_router = Router()

# =========================================================================
# II. БАЗА ДАННЫХ
# =========================================================================

def get_db_connection():
    return sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT)

def db_init():
    with get_db_connection() as conn:
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

def db_get_user(user_id):
    with get_db_connection() as conn:
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
        conn.commit()
        return new_end

def db_set_session_status(user_id, status):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
        conn.commit()

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
        conn.commit()

def db_add_promo(code, days, max_uses):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO promo_codes (code, days, max_uses) VALUES (?, ?, ?)", (code, days, max_uses))
        conn.commit()

def db_get_active_telethon_users():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
        return [row[0] for row in cur.fetchall()]

# =========================================================================
# III. FSM И УТИЛИТЫ
# =========================================================================

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

async def check_access(user_id: int):
    if user_id == ADMIN_ID: return True, ""
    channel_subscribed = False
    if TARGET_CHANNEL_URL:
        try:
            chat_member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
            if chat_member.status in ('member', 'administrator', 'creator'):
                channel_subscribed = True
        except: pass
    if not channel_subscribed:
        return False, f"❌ Подпишитесь на канал: {TARGET_CHANNEL_URL}"
    if db_check_subscription(user_id): return True, ""
    return False, "❌ Подписка истекла. Активируйте код."

# --- КЛАВИАТУРЫ ---
def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

def get_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать Промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="👤 Выдать Подписку", callback_data="admin_grant_sub")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_main_kb(user_id):
    user = db_get_user(user_id)
    active = user.get('telethon_active')
    running = user_id in ACTIVE_TELETHON_WORKERS
    has_progress = user_id in PROCESS_PROGRESS
    kb = []
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    kb.append([InlineKeyboardButton(text="❓ Помощь / Команды", callback_data="show_help")])
    if not active:
        kb.append([InlineKeyboardButton(text="📲 Вход по QR-коду", callback_data="telethon_auth_qr_start")])
        kb.append([InlineKeyboardButton(text="🔐 Вход по Номеру", callback_data="telethon_auth_phone_start")])
    else:
        if has_progress: kb.append([InlineKeyboardButton(text="⚡️ Прогресс", callback_data="show_progress")])
        kb.append([InlineKeyboardButton(text="🚀 Остановить Worker" if running else "🟢 Запустить Worker", callback_data="telethon_stop_session" if running else "telethon_start_session")])
        kb.append([InlineKeyboardButton(text="ℹ️ Статус Сессии", callback_data="telethon_check_status")])
        kb.append([InlineKeyboardButton(text="❌ Выход", callback_data="telethon_logout")])
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_report_choice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Файлом", callback_data="send_checkgroup_file")],
        [InlineKeyboardButton(text="💬 Сообщениями", callback_data="send_checkgroup_messages")],
        [InlineKeyboardButton(text="❌ Удалить", callback_data="send_checkgroup_delete")]
    ])

# =========================================================================
# IV. TELETHON WORKER
# =========================================================================

async def send_long_message(client, user_id, text, parse_mode='HTML', max_len=4096):
    if len(text) <= max_len: return await client.send_message(user_id, text, parse_mode=parse_mode)
    parts, current_part = [], ""
    for line in text.splitlines(True):
        if len(current_part) + len(line) > max_len:
            parts.append(current_part.strip())
            current_part = line
        else: current_part += line
    if current_part.strip(): parts.append(current_part.strip())
    for i, part in enumerate(parts):
        await client.send_message(user_id, part, parse_mode=parse_mode)
        await asyncio.sleep(0.5)

async def stop_worker(user_id, force_disconnect=True):
    if user_id in FLOOD_TASKS:
        for t in FLOOD_TASKS[user_id].values():
            if not t.done(): t.cancel()
        del FLOOD_TASKS[user_id]
    if user_id in ACTIVE_TELETHON_WORKERS:
        t = ACTIVE_TELETHON_WORKERS[user_id]
        if not t.done(): t.cancel()
        del ACTIVE_TELETHON_WORKERS[user_id]
    if user_id in ACTIVE_TELETHON_CLIENTS:
        c = ACTIVE_TELETHON_CLIENTS[user_id]
        if force_disconnect and c.is_connected():
            try: await c.disconnect()
            except: pass
        del ACTIVE_TELETHON_CLIENTS[user_id]
    if user_id in PROCESS_PROGRESS: del PROCESS_PROGRESS[user_id]
    db_set_session_status(user_id, False)
    logger.info(f"Worker {user_id} stopped.")

async def start_workers():
    for uid in db_get_active_telethon_users():
        ACTIVE_TELETHON_WORKERS[uid] = asyncio.create_task(run_worker(uid))

async def run_worker(user_id):
    await stop_worker(user_id, force_disconnect=True)
    path = get_session_path(user_id)
    client = TelegramClient(path, API_ID, API_HASH, proxy=PROXY_CONFIG, device_model="Android Client")
    ACTIVE_TELETHON_CLIENTS[user_id] = client
    
    try:
        if not os.path.exists(path + '.session'):
            db_set_session_status(user_id, False)
            await bot.send_message(user_id, "⚠️ Сессия не найдена. Авторизуйтесь снова.", reply_markup=get_main_kb(user_id))
            return
        await client.start()
        db_set_session_status(user_id, True)
        logger.info(f"Worker {user_id} started.")
        
        # --- ЗАДАЧИ ---
        async def flood_task(peer, message, count, delay, chat_id):
            try:
                is_unl = count <= 0
                mx = count if not is_unl else 999999999
                if user_id not in FLOOD_TASKS: FLOOD_TASKS[user_id] = {}
                PROCESS_PROGRESS[user_id] = {'type': 'flood', 'total': count, 'done': 0, 'peer': peer}
                for i in range(mx):
                    if user_id not in FLOOD_TASKS or chat_id not in FLOOD_TASKS[user_id]: 
                        await client.send_message(user_id, "🛑 Флуд остановлен.")
                        break
                    await client.send_message(peer, message)
                    PROCESS_PROGRESS[user_id]['done'] = i + 1
                    await asyncio.sleep(delay)
                await client.send_message(user_id, "✅ Флуд завершен.")
            except Exception as e: await client.send_message(user_id, f"❌ Ошибка флуда: {e}")
            finally:
                if user_id in FLOOD_TASKS and chat_id in FLOOD_TASKS[user_id]: del FLOOD_TASKS[user_id][chat_id]
                if user_id in PROCESS_PROGRESS: del PROCESS_PROGRESS[user_id]

        async def check_group_task(event, target, mn, mx):
            try:
                ent = await client.get_entity(target) if target else await client.get_entity(event.chat_id)
                name = get_display_name(ent)
                await client.send_message(user_id, f"⏳ Сканирую `{name}`...")
                
                users = {}
                PROCESS_PROGRESS[user_id] = {'type': 'checkgroup', 'peer': ent, 'done_msg': 0}
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
                await bot.send_message(user_id, f"✅ Готово! Найдено: {len(users)}", reply_markup=get_report_choice_kb())
            except Exception as e: await client.send_message(user_id, f"❌ Ошибка: {e}")

        # --- КОМАНДЫ ---
        @client.on(events.NewMessage(outgoing=True))
        async def worker_handler(event):
            if not db_check_subscription(user_id) and user_id != ADMIN_ID: return
            msg = event.text.strip()
            parts = msg.split()
            if not parts: return
            cmd = parts[0].lower()

            if cmd == '.лс':
                lines = event.text.split('\n')
                if len(lines) < 2: return await event.reply("❌ `.лс [текст]`\n`[@юзер]`")
                txt = lines[0][len(cmd):].strip()
                targets = [l.strip() for l in lines[1:] if l.strip()]
                res = []
                for t in targets:
                    try:
                        await client.send_message(t, txt)
                        res.append(f"✅ {t}")
                    except: res.append(f"❌ {t}")
                await event.reply("\n".join(res))

            elif cmd == '.флуд' and len(parts) >= 4:
                if user_id in FLOOD_TASKS: return await event.reply("⚠️ Уже идет.")
                try:
                    cnt = int(parts[1])
                    dly = float(parts[-1])
                    trg = parts[2] if len(parts) > 3 else None
                    msg_txt = " ".join(parts[2:-1]) if trg else parts[2]
                    if not trg: trg = event.chat_id
                    
                    ent = await client.get_input_entity(trg)
                    cid = (await client.get_entity(trg)).id
                    
                    task = asyncio.create_task(flood_task(ent, msg_txt, cnt, dly, cid))
                    if user_id not in FLOOD_TASKS: FLOOD_TASKS[user_id] = {}
                    FLOOD_TASKS[user_id][cid] = task
                    await event.reply("🔥 Запущено!")
                except Exception as e: await event.reply(f"❌ {e}")

            elif cmd == '.стопфлуд':
                if user_id in FLOOD_TASKS:
                    for t in FLOOD_TASKS[user_id].values(): t.cancel()
                    del FLOOD_TASKS[user_id]
                    await event.reply("🛑 Остановлено.")
                else: await event.reply("⚠️ Нет задач.")

            elif cmd == '.чекгруппу':
                if user_id in PROCESS_PROGRESS: return await event.reply("⚠️ Занято.")
                trg = parts[1] if len(parts) > 1 else None
                mn = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
                mx = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None
                asyncio.create_task(check_group_task(event, trg, mn, mx))
                await event.reply("⏳ Запущено.")
                
            elif cmd == '.статус':
                if user_id in PROCESS_PROGRESS:
                    p = PROCESS_PROGRESS[user_id]
                    await event.reply(f"⚙️ {p['type']}: {p.get('done', p.get('done_msg'))}")
                else: await event.reply("✨ Пусто.")

        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Worker error {user_id}: {e}")
    finally:
        await stop_worker(user_id, force_disconnect=False)

# =========================================================================
# V. AIOGRAM ХЭНДЛЕРЫ
# =========================================================================

@user_router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    db_get_user(user_id)
    has_access, reason = await check_access(user_id)
    if not has_access and "подпишитесь" in reason:
        return await message.answer(reason, reply_markup=get_no_access_kb(True))
    await message.answer("👋 Привет!", reply_markup=get_main_kb(user_id))

@user_router.callback_query(F.data == "back_to_main")
@user_router.callback_query(F.data == "cancel_action", StateFilter('*'))
async def back_home(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    client = TEMP_AUTH_CLIENTS.pop(user_id, None)
    if client:
        try: await client.disconnect()
        except: pass
    if os.path.exists(get_session_path(user_id, True) + '.session'):
        os.remove(get_session_path(user_id, True) + '.session')
    await state.clear()
    try: await call.message.delete()
    except: pass
    await call.message.answer("🏠 Главное меню", reply_markup=get_main_kb(user_id))

# --- АВТОРИЗАЦИЯ ---
@user_router.callback_query(F.data == "telethon_auth_phone_start")
async def auth_phone_start(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    await state.set_state(TelethonAuth.PHONE)
    client = TelegramClient(get_session_path(user_id, True), API_ID, API_HASH, proxy=PROXY_CONFIG, device_model="Android Client")
    TEMP_AUTH_CLIENTS[user_id] = client
    await call.message.edit_text("📞 Номер:", reply_markup=get_cancel_kb())

@user_router.message(TelethonAuth.PHONE)
async def auth_phone_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    client = TEMP_AUTH_CLIENTS.get(user_id)
    if not client: return await message.answer("❌ Ошибка.", reply_markup=get_main_kb(user_id))
    try:
        await client.connect()
        hash_code = await client.send_code_request(message.text.strip())
        await state.update_data(phone=message.text.strip(), hash=hash_code)
        await state.set_state(TelethonAuth.CODE)
        await message.answer("🔑 Код:", reply_markup=get_cancel_kb())
    except Exception as e: await message.answer(f"❌ {e}", reply_markup=get_cancel_kb())

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
        await message.answer("🔒 2FA Пароль:", reply_markup=get_cancel_kb())
    except Exception as e: await message.answer(f"❌ {e}", reply_markup=get_cancel_kb())

@user_router.message(TelethonAuth.PASSWORD)
async def auth_password_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    client = TEMP_AUTH_CLIENTS.get(user_id)
    try:
        await client.sign_in(password=message.text.strip())
        await finalize_login(user_id, client, message, state)
    except Exception as e: await message.answer(f"❌ {e}", reply_markup=get_cancel_kb())

@user_router.callback_query(F.data == "telethon_auth_qr_start")
async def auth_qr_start(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    await state.set_state(TelethonAuth.WAITING_FOR_QR_LOGIN)
    client = TelegramClient(get_session_path(user_id, True), API_ID, API_HASH, proxy=PROXY_CONFIG, device_model="Android Client")
    TEMP_AUTH_CLIENTS[user_id] = client
    await client.connect()
    qr = await client.qr_login()
    img = io.BytesIO(qr.qr_code)
    await call.message.answer_photo(BufferedInputFile(img.getvalue(), 'qr.png'), caption="📲 Скан QR (3 мин)", reply_markup=get_cancel_kb())
    try:
        await qr.wait(180)
        await finalize_login(user_id, client, call.message, state)
    except: await call.message.answer("❌ Время вышло.", reply_markup=get_main_kb(user_id))

async def finalize_login(user_id, client, message, state):
    await client.disconnect()
    del TEMP_AUTH_CLIENTS[user_id]
    src = get_session_path(user_id, True) + '.session'
    dst = get_session_path(user_id) + '.session'
    if os.path.exists(src):
        if os.path.exists(dst): os.remove(dst)
        os.rename(src, dst)
    db_set_session_status(user_id, True)
    await state.clear()
    await message.answer("✅ Успешно! Запускаю...", reply_markup=get_main_kb(user_id))
    asyncio.create_task(run_worker(user_id))

# --- WORKER CONTROL ---
@user_router.callback_query(F.data == "telethon_start_session")
async def worker_start(call: types.CallbackQuery):
    asyncio.create_task(run_worker(call.from_user.id))
    await call.answer("Запуск...")
    await call.message.edit_reply_markup(reply_markup=get_main_kb(call.from_user.id))

@user_router.callback_query(F.data == "telethon_stop_session")
async def worker_stop(call: types.CallbackQuery):
    await stop_worker(call.from_user.id)
    await call.answer("Стоп...")
    await call.message.edit_reply_markup(reply_markup=get_main_kb(call.from_user.id))

@user_router.callback_query(F.data == "telethon_logout")
async def worker_logout(call: types.CallbackQuery):
    user_id = call.from_user.id
    await stop_worker(user_id)
    if os.path.exists(get_session_path(user_id) + '.session'): os.remove(get_session_path(user_id) + '.session')
    db_set_session_status(user_id, False)
    await call.message.edit_text("❌ Сессия удалена.", reply_markup=get_main_kb(user_id))

@user_router.callback_query(F.data == "telethon_check_status")
async def worker_status(call: types.CallbackQuery):
    active = call.from_user.id in ACTIVE_TELETHON_WORKERS
    await call.answer(f"Worker: {'🟢 ON' if active else '🔴 OFF'}", show_alert=True)

# --- ОТЧЕТЫ ---
@user_router.callback_query(F.data.startswith("send_checkgroup_"))
async def report_handler(call: types.CallbackQuery):
    user_id = call.from_user.id
    action = call.data.split('_')[2]
    if user_id not in PROCESS_PROGRESS: return await call.answer("Нет данных", show_alert=True)
    data = PROCESS_PROGRESS[user_id]['report_data']
    name = PROCESS_PROGRESS[user_id]['peer_name']
    
    if action == 'file':
        f = io.BytesIO(data.encode('utf-8'))
        await call.message.answer_document(BufferedInputFile(f.getvalue(), f"report_{name}.txt"))
    elif action == 'messages':
        for i in range(0, len(data), 4000): await call.message.answer(f"<pre>{data[i:i+4000]}</pre>")
    del PROCESS_PROGRESS[user_id]
    await call.message.delete()

# --- АДМИНКА И ПРОМО ---
@user_router.callback_query(F.data == "start_promo_fsm")
async def promo_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await call.message.edit_text("Введи код:", reply_markup=get_cancel_kb())

@user_router.message(PromoStates.waiting_for_code)
async def promo_input(message: types.Message, state: FSMContext):
    promo = db_get_promo(message.text.strip())
    if promo and promo['is_active']:
        db_use_promo(message.text.strip())
        db_update_subscription(message.from_user.id, promo['days'])
        await message.answer(f"✅ +{promo['days']} дней.", reply_markup=get_main_kb(message.from_user.id))
    else: await message.answer("❌ Неверно.")
    await state.clear()

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(AdminStates.main_menu)
    await call.message.edit_text("🛠 Админка", reply_markup=get_admin_kb())

@user_router.callback_query(F.data == "admin_create_promo")
async def admin_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.promo_days_input)
    await call.message.edit_text("Дней:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_days_input)
async def admin_promo_days(message: types.Message, state: FSMContext):
    await state.update_data(days=int(message.text))
    await state.set_state(AdminStates.promo_uses_input)
    await message.answer("Лимит:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_uses_input)
async def admin_promo_fin(message: types.Message, state: FSMContext):
    data = await state.get_data()
    code = generate_promo_code()
    db_add_promo(code, data['days'], int(message.text))
    await state.clear()
    await message.answer(f"✅ Код: `{code}`", reply_markup=get_admin_kb())

@user_router.callback_query(F.data == "admin_grant_sub")
async def admin_grant(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.sub_user_id_input)
    await call.message.edit_text("ID юзера:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_user_id_input)
async def admin_grant_id(message: types.Message, state: FSMContext):
    await state.update_data(uid=int(message.text))
    await state.set_state(AdminStates.sub_days_input)
    await message.answer("Дней:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_days_input)
async def admin_grant_fin(message: types.Message, state: FSMContext):
    data = await state.get_data()
    db_update_subscription(data['uid'], int(message.text))
    await state.clear()
    await message.answer("✅ Выдано.", reply_markup=get_admin_kb())

@user_router.callback_query(F.data == "show_help")
async def help_msg(call: types.CallbackQuery):
    await call.message.edit_text("1. .лс\n2. .флуд\n3. .чекгруппу", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="back_to_main")]]))

# =========================================================================
# VI. ЗАПУСК
# =========================================================================

async def main():
    db_init()
    await start_workers()
    dp.include_router(user_router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
