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

# --- ИМПОРТЫ AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, FSInputFile, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, StateFilter
from aiogram.client.default import DefaultBotProperties

# --- ИМПОРТЫ TELETHON ---
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

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# ВАШИ ДАННЫЕ
API_ID = 38735310
API_HASH = "8d303ae71a002e7cc69c6b1d1bf14a9c"
BOT_TOKEN = "7868097991:AAHbVy_1SLrsVcxKEjmLz_QijdaA3OsdMBI"
ADMIN_ID = 6256576302
TARGET_CHANNEL_URL = "@STAT_PRO1"
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
DB_TIMEOUT = 10

# --- ПУТИ ---
DATA_DIR = 'data'
SESSION_DIR = 'sessions'

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

DB_PATH = os.path.join(DATA_DIR, DB_NAME)
PROXY_CONFIG = None

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- ГЛОБАЛЬНЫЕ ХРАНИЛИЩА ---
ACTIVE_TELETHON_CLIENTS: Dict[int, TelegramClient] = {}
ACTIVE_TELETHON_WORKERS: Dict[int, asyncio.Task] = {}
TEMP_AUTH_CLIENTS: Dict[int, TelegramClient] = {}
FLOOD_TASKS: Dict[int, Dict[int, asyncio.Task]] = {}
PROCESS_PROGRESS: Dict[int, Dict] = {}

# --- ИНИЦИАЛИЗАЦИЯ БОТА ---
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
# III. FSM СОСТОЯНИЯ
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

# =========================================================================
# IV. УТИЛИТЫ И КЛАВИАТУРЫ
# =========================================================================

def get_session_path(user_id, is_temp=False):
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')

def generate_promo_code(length=10):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

async def check_access(user_id: int):
    if user_id == ADMIN_ID:
        return True, ""

    channel_subscribed = False
    if TARGET_CHANNEL_URL:
        try:
            chat_member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
            if chat_member.status in ('member', 'administrator', 'creator'):
                channel_subscribed = True
        except Exception:
            pass

    if not channel_subscribed:
        return False, f"❌ Для доступа к функциям подпишитесь на наш канал: {TARGET_CHANNEL_URL}"

    if db_check_subscription(user_id):
        return True, ""
    
    return False, "❌ Ваша подписка истекла. Активируйте промокод."

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
        kb.append([InlineKeyboardButton(text="📲 Вход по QR-коду (Рекоменд.)", callback_data="telethon_auth_qr_start")])
        kb.append([InlineKeyboardButton(text="🔐 Вход по Номеру/Коду (Старый)", callback_data="telethon_auth_phone_start")])
    else:
        if has_progress:
             kb.append([InlineKeyboardButton(text="⚡️ Активный Прогресс", callback_data="show_progress")])
             
        kb.append([InlineKeyboardButton(text="🚀 Остановить Worker" if running else "🟢 Запустить Worker", callback_data="telethon_stop_session" if running else "telethon_start_session")])
        kb.append([InlineKeyboardButton(text="ℹ️ Статус Сессии", callback_data="telethon_check_status")])
        kb.append([InlineKeyboardButton(text="❌ Выход (Удалить Сессию)", callback_data="telethon_logout")])

    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_report_choice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Отправить файлом (.txt)", callback_data="send_checkgroup_file")],
        [InlineKeyboardButton(text="💬 Отправить сообщениями", callback_data="send_checkgroup_messages")],
        [InlineKeyboardButton(text="❌ Удалить отчет", callback_data="send_checkgroup_delete")]
    ])

# =========================================================================
# V. TELETHON WORKER (ЛОГИКА)
# =========================================================================

async def send_long_message(client, user_id, text, parse_mode='HTML', max_len=4096):
    if len(text) <= max_len:
        return await client.send_message(user_id, text, parse_mode=parse_mode)
    
    parts = []
    current_part = ""
    lines = text.splitlines(True)
    
    for line in lines:
        if len(current_part) + len(line) > max_len:
            parts.append(current_part.strip())
            current_part = line
        else:
            current_part += line
    
    if current_part.strip():
        parts.append(current_part.strip())
        
    for i, part in enumerate(parts):
        header = f"📊 **Часть {i+1}/{len(parts)}**\n"
        if len(part) < max_len - len(header):
             message_to_send = header + part
        else:
             message_to_send = part
        
        await client.send_message(user_id, message_to_send, parse_mode=parse_mode)
        await asyncio.sleep(0.5)

async def stop_worker(user_id, force_disconnect=True):
    # 1. Отмена задач флуда
    if user_id in FLOOD_TASKS:
        for chat_id, task in FLOOD_TASKS[user_id].items():
            if task and not task.done():
                task.cancel()
        del FLOOD_TASKS[user_id]
    
    # 2. Отмена основной задачи Worker'а
    if user_id in ACTIVE_TELETHON_WORKERS:
        task = ACTIVE_TELETHON_WORKERS[user_id]
        if not task.done():
             task.cancel()
        del ACTIVE_TELETHON_WORKERS[user_id]
    
    # 3. Отключение клиента
    if user_id in ACTIVE_TELETHON_CLIENTS:
        client = ACTIVE_TELETHON_CLIENTS[user_id]
        if force_disconnect and client.is_connected():
            try:
                await client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting client {user_id}: {e}")
        del ACTIVE_TELETHON_CLIENTS[user_id]
            
    if user_id in PROCESS_PROGRESS:
        del PROCESS_PROGRESS[user_id]
        
    db_set_session_status(user_id, False)
    logger.info(f"Worker {user_id} stopped.")

async def start_workers():
    users = db_get_active_telethon_users()
    for uid in users:
        task = asyncio.create_task(run_worker(uid))
        ACTIVE_TELETHON_WORKERS[uid] = task

async def run_worker(user_id):
    await stop_worker(user_id, force_disconnect=True)
    path = get_session_path(user_id)
    
    # Используем Android модель для стабильности
    client = TelegramClient(path, API_ID, API_HASH, proxy=PROXY_CONFIG, device_model='Android Client')
    ACTIVE_TELETHON_CLIENTS[user_id] = client
    
    try:
        if not os.path.exists(path + '.session'):
            db_set_session_status(user_id, False)
            await bot.send_message(user_id, "⚠️ Файл сессии не найден. Требуется повторная авторизация.", reply_markup=get_main_kb(user_id))
            return

        await client.start()
        db_set_session_status(user_id, True)
        logger.info(f"Worker {user_id} started successfully.")
        
        # --- ФЛУД ЗАДАЧА ---
        async def flood_task(peer, message, count, delay, chat_id):
            try:
                is_unlimited = count <= 0
                max_iterations = count if not is_unlimited else 999999999
                
                if user_id not in FLOOD_TASKS: FLOOD_TASKS[user_id] = {}
                PROCESS_PROGRESS[user_id] = {'type': 'flood', 'total': count, 'done': 0, 'peer': peer, 'chat_id': chat_id}
                
                for i in range(max_iterations):
                    if user_id not in FLOOD_TASKS or chat_id not in FLOOD_TASKS[user_id]:
                        await client.send_message(user_id, f"🛑 Флуд остановлен по команде .стопфлуд.")
                        break
                        
                    if not is_unlimited and i >= count: break
                        
                    await client.send_message(peer, message)
                    PROCESS_PROGRESS[user_id]['done'] = i + 1
                    await asyncio.sleep(delay)
                    
                await client.send_message(user_id, "✅ Флуд завершен.")
            except asyncio.CancelledError:
                pass
            except Exception as e:
                await client.send_message(user_id, f"❌ Ошибка при флуде: {e}")
            finally:
                if user_id in FLOOD_TASKS and chat_id in FLOOD_TASKS[user_id]:
                    del FLOOD_TASKS[user_id][chat_id]
                    if not FLOOD_TASKS[user_id]: del FLOOD_TASKS[user_id]
                if user_id in PROCESS_PROGRESS and PROCESS_PROGRESS[user_id].get('chat_id') == chat_id:
                    del PROCESS_PROGRESS[user_id]

        # --- ЧЕК ГРУППУ ЗАДАЧА ---
        async def check_group_task(event, target_chat_str, min_id, max_id):
             chat_id = event.chat_id
             if chat_id is None and not target_chat_str:
                  return await client.send_message(user_id, "❌ `.чекгруппу` должен быть вызван из группы/канала или с аргументом.")
                  
             try:
                 if target_chat_str:
                    chat_entity = await client.get_entity(target_chat_str)
                 elif chat_id is not None:
                     chat_entity = await client.get_entity(chat_id)
                 else:
                     return
                     
                 unique_users = {}
                 limit = 1000000
                 chat_name = get_display_name(chat_entity)
                 
                 await client.send_message(user_id, f"⏳ Начинаю сканирование `{chat_name}`...")
                 
                 if user_id in PROCESS_PROGRESS: del PROCESS_PROGRESS[user_id]
                 PROCESS_PROGRESS[user_id] = {'type': 'checkgroup', 'peer': chat_entity, 'done_msg': 0}
                 
                 async for message in client.iter_messages(chat_entity, limit=limit):
                     if user_id not in PROCESS_PROGRESS or PROCESS_PROGRESS[user_id].get('type') != 'checkgroup': return
                     PROCESS_PROGRESS[user_id]['done_msg'] += 1
                     
                     if message.sender and isinstance(message.sender, User) and message.sender_id not in unique_users:
                         user_id_int = message.sender.id
                         if (min_id is None or user_id_int >= min_id) and (max_id is None or user_id_int <= max_id):
                              unique_users[user_id_int] = message.sender
                         
                 total_found = len(unique_users)
                 if total_found > 0:
                     report_data_raw = []
                     range_info = f" ({min_id or 'Все'}-{max_id or 'Все'})" if min_id or max_id else ""
                     
                     for uid, p in unique_users.items():
                         full_name = ' '.join(filter(None, [p.first_name, p.last_name]))
                         report_data_raw.append(f"👤 Имя: {full_name}\n🔗 Юзернейм: @{p.username if p.username else 'Нет'}\n🆔 ID: {uid}")
                         
                     header_text = f"📊 Отчет .ЧЕКГРУППУ {range_info}\nЧат: {chat_name}\nПросканировано: {PROCESS_PROGRESS[user_id]['done_msg']}\nНайдено: {total_found}\n\n"
                     full_report_text = header_text + "\n".join(report_data_raw)
                     
                     PROCESS_PROGRESS[user_id]['report_data'] = full_report_text
                     PROCESS_PROGRESS[user_id]['peer_name'] = chat_name

                     await bot.send_message(
                         user_id, 
                         f"✅ Сбор завершен! Найдено: **{total_found}**.\nЧат: `{chat_name}`",
                         reply_markup=get_report_choice_kb()
                     )
                 else:
                     await client.send_message(user_id, "✅ Пользователи не найдены.")
             except Exception as e:
                 await client.send_message(user_id, f"❌ Ошибка .чекгруппу: {e}")
             finally:
                 if user_id in PROCESS_PROGRESS and 'report_data' not in PROCESS_PROGRESS[user_id]:
                     del PROCESS_PROGRESS[user_id]

        # --- ОБРАБОТКА КОМАНД (SELF-BOT) ---
        @client.on(events.NewMessage(outgoing=True))
        async def worker_handler(event):
            if not db_check_subscription(user_id) and user_id != ADMIN_ID: return
            
            msg = event.text.strip()
            parts = msg.split()
            if not parts: return
            cmd = parts[0].lower()
            current_chat_id = event.chat_id

            # .ЛС
            if cmd == '.лс':
                 try:
                    lines = event.text.split('\n')
                    if len(lines) < 2:
                        return await event.reply("❌ Формат: `.лс [текст]`\n`[@юзер]`")
                    text = lines[0][len(cmd):].strip()
                    recipients = [line.strip() for line in lines[1:] if line.strip()]
                    
                    if not text or not recipients:
                        return await event.reply("❌ Текст или адресаты не указаны.")
                    
                    results = []
                    for target in recipients:
                        try:
                            if not (target.startswith('@') or target.isdigit() or re.match(r'^-?\d+$', target)):
                                results.append(f"❌ {target}: Неверный формат")
                                continue
                            await client.send_message(target, text)
                            results.append(f"✅ {target}: Отправлено")
                        except Exception as e:
                            results.append(f"❌ {target}: Ошибка")
                            
                    await event.reply("<b>Результат:</b>\n" + "\n".join(results), parse_mode='HTML')
                 except Exception as e:
                    await event.reply(f"❌ Ошибка: {e}")

            # .ФЛУД
            elif cmd == '.флуд' and len(parts) >= 4:
                if user_id in FLOOD_TASKS and current_chat_id in FLOOD_TASKS[user_id]:
                    return await event.reply("⚠️ Флуд уже идет.")
                try:
                    count = int(parts[1])
                    delay = float(parts[-1])
                    target_chat_str = None
                    message_parts = parts[2:-1]
                    
                    if message_parts and (message_parts[-1].startswith('@') or re.match(r'^-?\d+$', message_parts[-1])):
                        target_chat_str = message_parts.pop()
                    
                    message = ' '.join(message_parts)
                    if target_chat_str:
                        peer = await client.get_input_entity(target_chat_str)
                        flood_chat_id = (await client.get_entity(target_chat_str)).id
                    else:
                        if not current_chat_id: return await event.reply("❌ Чат не определен.")
                        peer = await client.get_input_entity(current_chat_id)
                        flood_chat_id = current_chat_id

                    if delay < 0.5: return await event.reply("❌ Мин. задержка 0.5 сек.")
                    
                    task = asyncio.create_task(flood_task(peer, message, count, delay, flood_chat_id))
                    if user_id not in FLOOD_TASKS: FLOOD_TASKS[user_id] = {}
                    FLOOD_TASKS[user_id][flood_chat_id] = task
                    
                    await event.reply(f"🔥 Флуд запущен! Задержка: {delay}с")
                except Exception as e:
                    await event.reply(f"❌ Ошибка: {e}")

            # .СТОПФЛУД
            elif cmd == '.стопфлуд':
                if user_id in FLOOD_TASKS and current_chat_id in FLOOD_TASKS[user_id]:
                    task = FLOOD_TASKS[user_id][current_chat_id]
                    if not task.done(): task.cancel()
                    await event.reply("🛑 Флуд остановлен.")
                else:
                    await event.reply("⚠️ Нет активного флуда.")

            # .СТАТУС
            elif cmd == '.статус':
                if user_id in PROCESS_PROGRESS:
                    p = PROCESS_PROGRESS[user_id]
                    if p['type'] == 'flood':
                        done, total = p['done'], p['total']
                        await event.reply(f"⚡️ Флуд: {done}/{'∞' if total<=0 else total}")
                    elif p['type'] == 'checkgroup':
                        await event.reply(f"🔎 Чекгруппы: {p['done_msg']} сообщений...")
                else:
                    await event.reply("✨ Нет задач.")

            # .ЧЕКГРУППУ
            elif cmd == '.чекгруппу':
                if user_id in PROCESS_PROGRESS: return await event.reply("⚠️ Задача уже идет.")
                target, mn, mx = None, None, None
                if len(parts) >= 2:
                    if parts[1].startswith('@') or re.match(r'^-?\d+$', parts[1]): target = parts[1]
                    if len(parts) >= 3 and parts[2].isdigit(): mn = int(parts[2])
                    if len(parts) >= 4 and parts[3].isdigit(): mx = int(parts[3])
                if not target and current_chat_id: target = current_chat_id
                
                asyncio.create_task(check_group_task(event, target, mn, mx))
                await event.reply("⏳ Задача запущена.")

        await client.run_until_disconnected()

    except FloodWaitError as e:
        logger.error(f"FloodWait {user_id}: {e}")
    except Exception as e:
        logger.error(f"Worker error {user_id}: {e}")
    finally:
        await stop_worker(user_id, force_disconnect=False)

# =========================================================================
# VI. AIOGRAM ХЭНДЛЕРЫ
# =========================================================================

@user_router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    db_get_user(user_id) # Создаем юзера в БД
    
    has_access, reason = await check_access(user_id)
    if not has_access and "подпишитесь" in reason:
        return await message.answer(reason, reply_markup=get_no_access_kb(True))
        
    await message.answer("👋 Добро пожаловать!", reply_markup=get_main_kb(user_id))

@user_router.callback_query(F.data == "back_to_main")
@user_router.callback_query(F.data == "cancel_action", StateFilter('*'))
async def back_home(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    
    # Очистка временных клиентов при отмене
    client = TEMP_AUTH_CLIENTS.pop(user_id, None)
    if client:
        try: await client.disconnect()
        except: pass
    if os.path.exists(get_session_path(user_id, True) + '.session'):
        os.remove(get_session_path(user_id, True) + '.session')
        
    await state.clear()
    try:
        await call.message.delete()
    except: pass
    await call.message.answer("🏠 Главное меню", reply_markup=get_main_kb(user_id))

# --- АВТОРИЗАЦИЯ (ТЕЛЕФОН) ---
@user_router.callback_query(F.data == "telethon_auth_phone_start")
async def auth_phone_start(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    await state.set_state(TelethonAuth.PHONE)
    
    # Создаем временный клиент
    path = get_session_path(user_id, True)
    client = TelegramClient(path, API_ID, API_HASH, proxy=PROXY_CONFIG, device_model='Android Client')
    TEMP_AUTH_CLIENTS[user_id] = client
    
    await call.message.edit_text("📞 Введите номер телефона:", reply_markup=get_cancel_kb())

@user_router.message(TelethonAuth.PHONE)
async def auth_phone_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    phone = message.text.strip()
    client = TEMP_AUTH_CLIENTS.get(user_id)
    
    if not client: return await message.answer("❌ Ошибка сессии.", reply_markup=get_main_kb(user_id))
    
    try:
        await client.connect()
        hash_code = await client.send_code_request(phone)
        await state.update_data(phone=phone, hash=hash_code)
        await state.set_state(TelethonAuth.CODE)
        await message.answer("🔑 Введите код:", reply_markup=get_cancel_kb())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}", reply_markup=get_cancel_kb())

@user_router.message(TelethonAuth.CODE)
async def auth_code_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip()
    data = await state.get_data()
    client = TEMP_AUTH_CLIENTS.get(user_id)
    
    try:
        await client.sign_in(data['phone'], code, phone_code_hash=data['hash'].phone_code_hash)
        await finalize_login(user_id, client, message, state)
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔒 Введите 2FA пароль:", reply_markup=get_cancel_kb())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}", reply_markup=get_cancel_kb())

@user_router.message(TelethonAuth.PASSWORD)
async def auth_password_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    password = message.text.strip()
    client = TEMP_AUTH_CLIENTS.get(user_id)
    
    try:
        await client.sign_in(password=password)
        await finalize_login(user_id, client, message, state)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}", reply_markup=get_cancel_kb())

# --- АВТОРИЗАЦИЯ (QR) ---
@user_router.callback_query(F.data == "telethon_auth_qr_start")
async def auth_qr_start(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    await state.set_state(TelethonAuth.WAITING_FOR_QR_LOGIN)
    
    path = get_session_path(user_id, True)
    client = TelegramClient(path, API_ID, API_HASH, proxy=PROXY_CONFIG, device_model='Android Client')
    TEMP_AUTH_CLIENTS[user_id] = client
    await client.connect()
    
    qr = await client.qr_login()
    img = io.BytesIO(qr.qr_code)
    await call.message.answer_photo(BufferedInputFile(img.getvalue(), 'qr.png'), caption="📲 Сканируйте QR (3 мин)", reply_markup=get_cancel_kb())
    
    try:
        await qr.wait(180)
        await finalize_login(user_id, client, call.message, state)
    except Exception:
        await call.message.answer("❌ Время вышло.", reply_markup=get_main_kb(user_id))

async def finalize_login(user_id, client, message, state):
    await client.disconnect()
    del TEMP_AUTH_CLIENTS[user_id]
    
    # Перенос сессии
    src = get_session_path(user_id, True) + '.session'
    dst = get_session_path(user_id) + '.session'
    if os.path.exists(src):
        if os.path.exists(dst): os.remove(dst)
        os.rename(src, dst)
        
    db_set_session_status(user_id, True)
    await state.clear()
    await message.answer("✅ Авторизация успешна! Worker запускается...", reply_markup=get_main_kb(user_id))
    asyncio.create_task(run_worker(user_id))

# --- УПРАВЛЕНИЕ WORKER ---
@user_router.callback_query(F.data == "telethon_start_session")
async def worker_start(call: types.CallbackQuery):
    asyncio.create_task(run_worker(call.from_user.id))
    await call.answer("Запуск...")
    await call.message.edit_reply_markup(reply_markup=get_main_kb(call.from_user.id))

@user_router.callback_query(F.data == "telethon_stop_session")
async def worker_stop(call: types.CallbackQuery):
    await stop_worker(call.from_user.id)
    await call.answer("Остановка...")
    await call.message.edit_reply_markup(reply_markup=get_main_kb(call.from_user.id))

@user_router.callback_query(F.data == "telethon_logout")
async def worker_logout(call: types.CallbackQuery):
    user_id = call.from_user.id
    await stop_worker(user_id)
    path = get_session_path(user_id) + '.session'
    if os.path.exists(path): os.remove(path)
    db_set_session_status(user_id, False)
    await call.message.edit_text("❌ Сессия удалена.", reply_markup=get_main_kb(user_id))

@user_router.callback_query(F.data == "telethon_check_status")
async def worker_status(call: types.CallbackQuery):
    user_id = call.from_user.id
    active = user_id in ACTIVE_TELETHON_WORKERS
    await call.answer(f"Worker: {'🟢 Работает' if active else '🔴 Остановлен'}", show_alert=True)

# --- ОТЧЕТЫ ---
@user_router.callback_query(F.data.startswith("send_checkgroup_"))
async def report_handler(call: types.CallbackQuery):
    user_id = call.from_user.id
    action = call.data.split('_')[2]
    
    if user_id not in PROCESS_PROGRESS: return await call.answer("Нет данных.", show_alert=True)
    
    data = PROCESS_PROGRESS[user_id]['report_data']
    name = PROCESS_PROGRESS[user_id]['peer_name']
    
    if action == 'file':
        f = io.BytesIO(data.encode('utf-8'))
        await call.message.answer_document(BufferedInputFile(f.getvalue(), f"report_{name}.txt"))
    elif action == 'messages':
        # Отправка через бота (aiogram), т.к. это результат
        for i in range(0, len(data), 4000):
            await call.message.answer(f"<pre>{data[i:i+4000]}</pre>")
    elif action == 'delete':
        del PROCESS_PROGRESS[user_id]
        await call.message.delete()

# --- АДМИНКА И ПРОМОКОДЫ ---
# (Добавьте сюда хэндлеры PromoStates и AdminStates из предыдущих версий, они стандартные)
# Для краткости я включил основные. Добавьте start_promo_fsm и admin_panel_start по аналогии с предыдущими кодами.

@user_router.callback_query(F.data == "start_promo_fsm")
async def promo_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await call.message.edit_text("Введите код:", reply_markup=get_cancel_kb())

@user_router.message(PromoStates.waiting_for_code)
async def promo_process(message: types.Message, state: FSMContext):
    code = message.text.strip()
    promo = db_get_promo(code)
    if promo and promo['is_active']:
        db_use_promo(code)
        db_update_subscription(message.from_user.id, promo['days'])
        await message.answer(f"✅ Активировано {promo['days']} дней.", reply_markup=get_main_kb(message.from_user.id))
    else:
        await message.answer("❌ Неверный код.")
    await state.clear()

@user_router.callback_query(F.data == "show_help")
async def show_help(call: types.CallbackQuery):
    txt = (
        "📖 **Команды Worker'а (отправлять в чаты):**\n"
        "1. `.лс [текст]` + `[@юзер]` (с новой строки) - Рассылка\n"
        "2. `.флуд [кол-во] [текст] [задержка]` - Флуд\n"
        "3. `.стопфлуд` - Стоп флуда\n"
        "4. `.чекгруппу` - Парсинг юзеров\n"
        "5. `.тхт` (реплаем на файл) - Чтение файла\n"
        "6. `.статус` - Статус задач"
    )
    await call.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="back_to_main")]]))

# =========================================================================
# VII. ЗАПУСК
# =========================================================================

async def main():
    db_init()
    await start_workers()
    dp.include_router(user_router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt: pass
