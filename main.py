import asyncio
import logging
import os
import sqlite3
import pytz 
import qrcode 
import time
import re
from io import BytesIO 
from datetime import datetime, timedelta 
from typing import Optional, Set, Dict, Any

# --- Aiogram и FSM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
# ВОЗВРАТ INLINE КНОПОК
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile, FSInputFile
from aiogram.client.default import DefaultBotProperties 

# --- Telethon ---
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, RPCError, UserDeactivatedError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.utils import get_display_name, get_peer_id

# =========================================================================
# I. КОНФИГ И ЛОГИРОВАНИЕ
# =========================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0")) 
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")

TARGET_CHANNEL_URL = "@STAT_PRO1" 
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# --- ГЛОБАЛЬНОЕ СОСТОЯНИЕ ДЛЯ TELETHON СЕССИЙ ---
ACTIVE_TELETHON_CLIENTS: Dict[int, TelegramClient] = {} 
ACTIVE_TELETHON_WORKERS: Dict[int, asyncio.Task] = {} 
ACTIVE_FLOOD_TASKS: Dict[int, list] = {} 
# Хранение QR-логина для ожидания
QR_LOGIN_WAITS: Dict[int, asyncio.Task] = {}


# FSM Состояния
class AdminStates(StatesGroup):
    waiting_for_promo_user_id = State()
    waiting_for_new_promo_code = State()
    waiting_for_new_promo_days = State()
    waiting_for_new_promo_max_uses = State()
    
class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    QR_LOGIN = State() # НОВОЕ СОСТОЯНИЕ ДЛЯ ВХОДА ПО QR

class PromoStates(StatesGroup):
    waiting_for_code = State()
    
class MonitorStates(StatesGroup):
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()
    
# Роутеры
auth_router = Router(name="auth")
user_router = Router(name="user")

# =========================================================================
# II. БАЗА ДАННЫХ (DB) - Функции остаются прежними
# =========================================================================

DB_PATH = os.path.join('data', DB_NAME) 

def get_db_connection():
    return sqlite3.connect(DB_PATH) 

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            subscription_active BOOLEAN DEFAULT 0,
            subscription_end_date TIMESTAMP,
            role TEXT DEFAULT 'user',
            promo_code TEXT,       
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telethon_sessions (
            user_id INTEGER PRIMARY KEY,
            session_file TEXT NOT NULL UNIQUE,
            is_active BOOLEAN DEFAULT 0,
            phone_code_hash TEXT,
            it_chat_id TEXT,    
            drop_chat_id TEXT,  
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            days INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            max_uses INTEGER,
            current_uses INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monitor_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            type TEXT, -- 'IT' or 'DROP'
            command TEXT,
            target TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

# --- DB-функции (db_get_user, db_add_or_update_user, db_check_subscription, db_activate_subscription, db_get_session_data, db_set_session_status, db_set_monitor_chat_id, db_check_and_use_promo, db_create_promo_code, db_add_monitor_log, db_get_monitor_logs, db_clear_monitor_logs) ---
# ... (Остаются прежними) ...

def db_get_user(user_id: int) -> Optional[dict]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    row = cursor.fetchone()
    if row:
        cols = [desc[0] for desc in cursor.description]
        return dict(zip(cols, row))
    return None

def db_add_or_update_user(user_id: int, username: str, first_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (user_id, username, first_name) 
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            username=excluded.username, 
            first_name=excluded.first_name;
    """, (user_id, username, first_name))
    conn.commit()
    
def get_session_file_path(user_id: int) -> str:
    return os.path.join('data', f'session_{user_id}.session')


# =========================================================================
# III. TELETHON WORKER (Добавление проверки команд)
# =========================================================================

# ... (flood_task_worker, check_channel_membership остаются прежними) ...

async def run_telethon_worker_for_user(user_id: int, bot: Bot):
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        await client.start()
        user_info = await client.get_me()
        logger.info(f"✅ Telethon Worker [{user_id}] запущен как: {get_display_name(user_info)}")
        db_set_session_status(user_id, True)

        # --- TELETHON ХЕНДЛЕРЫ ДЛЯ НОВЫХ КОМАНД (.лс, .флуд, .стопфлуд, .чекгруппу) ---
        
        @client.on(events.NewMessage(pattern=r'^\.(лс|флуд|стопфлуд|чекгруппу).*'))
        async def handle_telethon_control_commands(event: events.NewMessage.Event):
            
            if event.sender_id != user_id:
                return
            
            # ВСЕ КОМАНДЫ ПРИВОДИМ К НИЖНЕМУ РЕГИСТРУ
            msg_text = event.message.message.lower().strip()
            parts = msg_text.split()
            command = parts[0]
            
            chat_id = event.chat_id
            
            # --- 1. КОМАНДА .лс ---
            if command == '.лс':
                # ... (логика .ЛС остается прежней, но используем .лс) ...
                try:
                    if len(parts) < 3:
                        await event.reply("❌ **Ошибка .лс:** Неверный формат. Используйте: `.лс [текст сообщения] [список @юзернеймов или ID]`")
                        return

                    user_targets = [p.strip() for p in parts if p.startswith('@') or p.isdigit() or p.startswith('-100')]
                    
                    if not user_targets:
                         await event.reply("❌ **Ошибка .лс:** Не найден список юзернеймов или ID для отправки.")
                         return
                    
                    text = msg_text[len('.лс'):].replace(' '.join(user_targets), '').strip()
                    
                    if not text:
                         await event.reply("❌ **Ошибка .лс:** Текст сообщения не может быть пустым.")
                         return
                        
                    sent_count = 0
                    
                    for target in user_targets:
                        try:
                            target_entity = await client.get_entity(target)
                            await client.send_message(target_entity, text)
                            sent_count += 1
                        except Exception as e:
                            logger.error(f"Ошибка отправки ЛС на {target}: {e}")
                            await client.send_message(chat_id, f"❌ Не удалось отправить в ЛС: {target}. Ошибка: {e}")
                            
                    await event.reply(f"✅ **.лс:** Сообщение отправлено **{sent_count}** пользователям из {len(user_targets)}.")

                except Exception as e:
                    logger.error(f"Общая ошибка .лс: {e}")
                    await event.reply(f"❌ Общая ошибка .лс: {e}")
                    
            # --- 2. КОМАНДА .флуд ---
            elif command == '.флуд':
                # ... (логика .ФЛУД остается прежней, но используем .флуд) ...
                try:
                    if len(parts) < 5 or not parts[1].isdigit() or not parts[3].replace('.', '', 1).isdigit():
                        await event.reply("❌ **Ошибка .флуд:** Неверный формат. Используйте: `.флуд [кол-во] [текст] [задержка_сек] [чат @юзернейм/ID]`")
                        return

                    count = int(parts[1])
                    delay = float(parts[3])
                    target = parts[-1]
                    
                    text = " ".join(parts[2:-2]) 

                    if user_id in ACTIVE_FLOOD_TASKS:
                        await event.reply("❌ **Ошибка:** Флуд уже запущен. Используйте `.стопфлуд` сначала.")
                        return

                    target_entity = await client.get_entity(target)
                    
                    flood_task = asyncio.create_task(flood_task_worker(client, target_entity, text, count, delay, user_id, bot))
                    ACTIVE_FLOOD_TASKS[user_id] = [flood_task, target_entity]

                    await event.reply(f"🚀 **Флуд запущен!** Будет отправлено {count} сообщений с задержкой {delay}с в чат `{get_display_name(target_entity)}`. Используйте `.стопфлуд` для остановки.")
                
                except Exception as e:
                    logger.error(f"Общая ошибка .флуд: {e}")
                    await event.reply(f"❌ Общая ошибка .флуд: {e}")

            # --- 3. КОМАНДА .стопфлуд ---
            elif command == '.стопфлуд':
                # ... (логика .СТОПФЛУД остается прежней, но используем .стопфлуд) ...
                if user_id in ACTIVE_FLOOD_TASKS:
                    ACTIVE_FLOOD_TASKS[user_id][0].cancel()
                    await event.reply("⏳ Попытка остановить флуд-рассылку...")
                else:
                    await event.reply("❌ **Ошибка:** Флуд-рассылка неактивна.")
            
            # --- 4. КОМАНДА .чекгруппу ---
            elif command == '.чекгруппу':
                # ... (логика .ЧЕКГРУППУ остается прежней, но используем .чекгруппу) ...
                try:
                    if len(parts) < 2:
                        await event.reply("❌ **Ошибка .чекгруппу:** Неверный формат. Используйте: `.чекгруппу [чат @юзернейм/ID]`")
                        return

                    target = parts[1]
                    await event.reply("⏳ **Анализ активности запущен.** Это может занять время для больших чатов...")

                    target_entity = await client.get_entity(target)
                    
                    all_messages = []
                    limit = 50000 
                    offset_id = 0
                    total_messages = 0
                    
                    while total_messages < limit:
                        history = await client(GetHistoryRequest(
                            peer=target_entity,
                            offset_id=offset_id,
                            offset_date=None,
                            add_offset=0,
                            limit=100,
                            max_id=0,
                            min_id=0,
                            hash=0
                        ))
                        if not history.messages:
                            break
                            
                        all_messages.extend(history.messages)
                        total_messages += len(history.messages)
                        offset_id = history.messages[-1].id
                        
                        if len(history.messages) < 100: break

                    user_activity = {}
                    for msg in all_messages:
                        sender = msg.sender
                        if sender and msg.message:
                            sender_id = get_peer_id(sender)
                            if sender_id not in user_activity:
                                user_activity[sender_id] = {'count': 0, 'username': 'N/A', 'last_msg': msg.date}
                            user_activity[sender_id]['count'] += 1
                            user_activity[sender_id]['last_msg'] = max(user_activity[sender_id]['last_msg'], msg.date)
                            
                            if not isinstance(sender, PeerChannel):
                                try:
                                    sender_user = await client.get_entity(sender)
                                    user_activity[sender_id]['username'] = get_display_name(sender_user)
                                except Exception:
                                    pass

                    sorted_activity = sorted(user_activity.items(), key=lambda item: item[1]['count'], reverse=True)
                    
                    report_content = f"--- АКТИВНОСТЬ В ЧАТЕ {get_display_name(target_entity)} ---\n"
                    report_content += f"Всего проанализировано сообщений: {total_messages}\n"
                    report_content += f"Всего уникальных участников, писавших: {len(user_activity)}\n\n"
                    report_content += "ТОП-20 САМЫХ АКТИВНЫХ:\n"
                    
                    for i, (uid, data) in enumerate(sorted_activity[:20]):
                        last_msg_msk = data['last_msg'].astimezone(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M')
                        report_content += f"{i+1}. {data['username']} (ID: {uid}): {data['count']} сообщений. (Последнее: {last_msg_msk})\n"
                        
                    file_path = os.path.join('data', f"Group_Check_{time.time()}.txt")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(report_content)
                        
                    report_file = FSInputFile(file_path, filename=f"Group_Activity_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
                    
                    await client.send_file(chat_id, report_file, caption=f"✅ **Анализ активности завершен** в чате `{get_display_name(target_entity)}`!")
                    os.remove(file_path)

                except Exception as e:
                    logger.error(f"Ошибка .чекгруппу: {e}")
                    await event.reply(f"❌ Критическая ошибка .чекгруппу: {e}")


        # --- TELETHON ХЕНДЛЕРЫ ДЛЯ МОНИТОРИНГА (Остаются прежними) ---
        @client.on(events.NewMessage(pattern=r'^\.(встал|кьар|ошибка|замена|повтор).*'))
        async def handle_it_commands(event: events.NewMessage.Event):
             # ... (логика IT команд) ...
             # Обработка команд IT-ворка (.встал, .ошибка и т.д.)
            session_data = db_get_session_data(user_id)
            if not session_data or not session_data.get('it_chat_id'): return
                
            it_chat = session_data['it_chat_id']
            try:
                it_chat_entity = await client.get_entity(it_chat)
            except Exception:
                return 
            
            if event.chat_id != it_chat_entity.id: return

            msg_text = event.message.message.lower()
            
            if msg_text.startswith('.встал'):
                target = ""
                if event.is_reply and event.reply_to_msg_id:
                    original_message = await client.get_messages(event.chat_id, ids=event.reply_to_msg_id)
                    if original_message and original_message[0].message:
                        target = original_message[0].message.split()[0]
                elif len(msg_text.split()) > 1:
                    target = msg_text.split()[1]
                    
                if target:
                    db_add_monitor_log(user_id, 'IT', '.встал', target)
                    await client.send_message(it_chat_entity, f"✅ Лог: .встал ({target}) добавлен.", reply_to=event.id)
                    return
            
            commands_map = {
                '.кьар': 'QR',
                '.ошибка': 'ERROR',
                '.замена': 'REPLACE',
            }
            if msg_text.split()[0] in commands_map:
                target = event.reply_to_msg_id if event.is_reply else 'N/A'
                db_add_monitor_log(user_id, 'IT', msg_text.split()[0], str(target))
                await client.send_message(it_chat_entity, f"✅ Лог: {msg_text.split()[0]} добавлен.", reply_to=event.id)
                return


        @client.on(events.NewMessage(func=lambda e: e.message and len(e.message.split()) >= 4 and e.message.split()[-1].lower() == 'бх'))
        async def handle_drop_commands(event: events.NewMessage.Event):
             # ... (логика DROP команд) ...
             # Обработка команд Дроп-ворка
            session_data = db_get_session_data(user_id)
            if not session_data or not session_data.get('drop_chat_id'): return
                
            drop_chat = session_data['drop_chat_id']
            try:
                drop_chat_entity = await client.get_entity(drop_chat)
            except Exception: return

            if event.chat_id != drop_chat_entity.id: return

            msg_text = event.message.message.strip()
            parts = msg_text.split()
            if len(parts) >= 4 and parts[-1].lower() == 'бх':
                target_info = msg_text
                db_add_monitor_log(user_id, 'DROP', 'Новая заявка', target_info)
                await client.send_message(drop_chat_entity, f"✅ Лог: Заявка Дропа добавлена.", reply_to=event.id)
                return
        
        await client.run_until_disconnected()

    except UserDeactivatedError:
        logger.warning(f"❌ Telethon Worker [{user_id}]: Аккаунт деактивирован.")
        db_set_session_status(user_id, False)
        session_path = get_session_file_path(user_id)
        if os.path.exists(session_path + '.session'): os.remove(session_path + '.session')
    except Exception as e:
        logger.error(f"❌ Критическая ошибка Telethon Worker [{user_id}]: {e}")
        db_set_session_status(user_id, False)
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS: del ACTIVE_TELETHON_CLIENTS[user_id]
        if client.is_connected(): await client.disconnect()
        logger.info(f"Telethon Worker [{user_id}] остановлен.")


async def start_all_telethon_workers(bot: Bot):
    # ... (логика запуска всех воркеров) ...
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM telethon_sessions WHERE is_active=1")
    
    for row in cursor.fetchall():
        user_id = row[0]
        if user_id not in ACTIVE_TELETHON_WORKERS or ACTIVE_TELETHON_WORKERS[user_id].done():
            task = asyncio.create_task(run_telethon_worker_for_user(user_id, bot))
            ACTIVE_TELETHON_WORKERS[user_id] = task
            logger.info(f"🚀 Запуск Telethon Worker для пользователя ID: {user_id}")


# =========================================================================
# IV. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И КЛАВИАТУРЫ (INLINE)
# =========================================================================

def kb_back_to_main(user_id: int) -> InlineKeyboardMarkup:
    # Возвращение в главное меню или в Админ-панель
    callback_data = "admin_panel" if user_id == ADMIN_ID else "back_to_main"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data=callback_data)]
    ])

def get_main_inline_kb(user_id: int) -> InlineKeyboardMarkup:
    is_admin = user_id == ADMIN_ID
    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    
    kb = [
        [InlineKeyboardButton(text="💳 Подписка", callback_data="show_subscription"),
         InlineKeyboardButton(text="🔑 Промокод", callback_data="activate_promo")],
        [InlineKeyboardButton(text="📊 Отчеты и Мониторинг", callback_data="show_monitor_menu")],
    ]
    
    if is_admin:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel")])

    # Кнопка авторизации
    auth_text = "🟢 Сессия активна" if session_active else "🔐 Авторизовать Telethon"
    auth_callback = "telethon_auth_status" if session_active else "telethon_auth_start"
    kb.append([InlineKeyboardButton(text=auth_text, callback_data=auth_callback)])

    return InlineKeyboardMarkup(inline_keyboard=kb)
    
async def generate_qr_code(data: str) -> BufferedInputFile:
    # ... (логика QR)
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    
    return BufferedInputFile(buffer.read(), filename="qr_code.png")

def format_monitor_logs_to_file(logs: list[tuple], log_type: str) -> FSInputFile:
    # ... (логика форматирования отчета) ...
    if not logs: return None
        
    header = f"--- ОТЧЕТ {log_type} (Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S MSK')}) ---\n"
    content = ""
    
    for timestamp, command, target in logs:
        timestamp_msk = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').astimezone(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        
        if log_type == 'IT':
            content += f"[{timestamp_msk}] {command.upper()}: {target}\n"
        elif log_type == 'DROP':
            content += f"[{timestamp_msk}] {command.upper()}: {target}\n"
            
    file_path = os.path.join('data', f"{log_type}_Report_{time.time()}.txt")
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(header + content)
        
    return FSInputFile(file_path, filename=f"{log_type}_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")


# =========================================================================
# V. ХЕНДЛЕРЫ
# =========================================================================

# --- СТАРТ И ВОЗВРАТ В ГЛАВНОЕ МЕНЮ ---
@auth_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(query_or_message: types.CallbackQuery | types.Message, state: FSMContext) -> None:
    await state.clear()
    
    is_callback = isinstance(query_or_message, types.CallbackQuery)
    message = query_or_message.message if is_callback else query_or_message
    user = message.from_user
    
    db_add_or_update_user(user.id, user.username or '', user.first_name or '')
    
    text = (
        f"👋 **STATPRO | Панель управления.**\n\n"
        f"Ваш ID: `{user.id}`. Здесь вы можете управлять подпиской, авторизовать аккаунт для мониторинга и доступа к инструментам."
    )
    
    if is_callback:
        await message.edit_text(text, reply_markup=get_main_inline_kb(user.id))
        await query_or_message.answer()
    else:
        await message.answer(text, reply_markup=get_main_inline_kb(user.id))

# --- МЕНЮ АВТОРИЗАЦИИ (ОБНОВЛЕНО С QR) ---
@user_router.callback_query(F.data.in_({"telethon_auth_start", "telethon_auth_status"}))
async def telethon_auth_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("✅ Сессия Telethon уже активна.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Авторизация по номеру", callback_data="auth_by_phone")],
        [InlineKeyboardButton(text="2️⃣ Вход по QR-коду", callback_data="auth_by_qr")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        "🔐 **Авторизация Telethon.**\n\n"
        "Выберите удобный способ входа для запуска вашего рабочего аккаунта:",
        reply_markup=kb
    )
    await callback.answer()

# --- АВТОРИЗАЦИЯ: ШАГ 1 (ВВОД ТЕЛЕФОНА) ---
@user_router.callback_query(F.data == "auth_by_phone")
async def telethon_auth_start(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if not API_ID or not API_HASH:
        await callback.answer("❌ API_ID и API_HASH не настроены.", show_alert=True)
        return

    await state.set_state(TelethonAuth.PHONE)
    await callback.message.edit_text(
        "1️⃣ **Шаг 1: Ввод телефона**\n\n"
        "Введите номер телефона для авторизации вашего аккаунта (например, `+79001234567`):",
        reply_markup=kb_back_to_main(user_id)
    )
    await callback.answer()

@user_router.message(TelethonAuth.PHONE)
async def telethon_auth_step_phone(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    phone_number = message.text.strip()
    session_path = get_session_file_path(user_id)
    
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect() # Подключаемся
        if await client.is_user_authorized():
            await message.answer("⚠️ Аккаунт уже авторизован. Запуск Telethon Worker...")
            task = asyncio.create_task(run_telethon_worker_for_user(user_id, bot))
            ACTIVE_TELETHON_WORKERS[user_id] = task
            await state.clear()
            await message.answer("✅ Telethon Worker запущен!", reply_markup=get_main_inline_kb(user_id))
            return

        result = await client.send_code_request(phone_number)
        
        await state.update_data(phone=phone_number, phone_code_hash=result.phone_code_hash) 
        db_set_session_status(user_id, False, hash_code=result.phone_code_hash)
        
        await state.set_state(TelethonAuth.CODE)
        await message.answer("2️⃣ **Шаг 2: Ввод кода**\n\nВведите код, который пришел в Telegram:")
            
    except RPCError as e:
        logger.error(f"Telethon Auth Error: {e}")
        await message.answer(f"❌ Ошибка Telethon: {e}", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect() # Отключаемся сразу после запроса кода, чтобы избежать зависаний

# --- АВТОРИЗАЦИЯ: ШАГ 2 (ВВОД КОДА) ---
@user_router.message(TelethonAuth.CODE)
async def telethon_auth_step_code(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    data = await state.get_data()
    phone_number = data['phone']
    code = message.text.strip()
    phone_code_hash = data['phone_code_hash'] 
    session_path = get_session_file_path(user_id)
    
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        
        try:
            user_info = await client.sign_in(phone_number, code, phone_code_hash=phone_code_hash)
            
        except SessionPasswordNeededError:
            await state.set_state(TelethonAuth.PASSWORD)
            await message.answer("3️⃣ **Шаг 3: Ввод пароля**\n\nТребуется двухфакторная аутентификация. Введите пароль:")
            return
        except Exception as e:
            await message.answer(f"❌ Ошибка входа: {e}", reply_markup=get_main_inline_kb(user_id))
            await state.clear()
            return

        await message.answer(f"✅ Аккаунт **{get_display_name(user_info)}** успешно авторизован! Запуск Worker...")
        
        task = asyncio.create_task(run_telethon_worker_for_user(user_id, bot))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await state.clear()
        await message.answer("✅ Telethon Worker запущен!", reply_markup=get_main_inline_kb(user_id))
            
    except Exception as e:
        await message.answer(f"❌ Критическая ошибка: {e}", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    finally:
        if client.is_connected(): await client.disconnect()

# --- АВТОРИЗАЦИЯ: ШАГ 3 (ВВОД ПАРОЛЯ) ---
@user_router.message(TelethonAuth.PASSWORD)
async def telethon_auth_step_password(message: Message, state: FSMContext, bot: Bot):
    # ... (логика ввода пароля остается прежней) ...
    user_id = message.from_user.id
    password = message.text.strip()
    session_path = get_session_file_path(user_id)
    
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        user_info = await client.sign_in(password=password)

        await message.answer(f"✅ Аккаунт **{get_display_name(user_info)}** успешно авторизован! Запуск Worker...")
        task = asyncio.create_task(run_telethon_worker_for_user(user_id, bot))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        await state.clear()
        await message.answer("✅ Telethon Worker запущен!", reply_markup=get_main_inline_kb(user_id))

    except Exception as e:
        await message.answer(f"❌ Ошибка входа с паролем: {e}", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    finally:
        if client.is_connected(): await client.disconnect()

# --- АВТОРИЗАЦИЯ: QR-код (НОВЫЙ ФУНКЦИОНАЛ) ---

async def qr_waiter(client: TelegramClient, user_id: int, message_id: int, bot: Bot):
    """Ожидает сканирования QR-кода."""
    try:
        await client.connect()
        await client.qr_login()
        user_info = await client.get_me()
        
        # Успех
        await bot.edit_message_text(
            f"🎉 **Авторизация успешна!**\n\nАккаунт **{get_display_name(user_info)}** авторизован.",
            chat_id=user_id,
            message_id=message_id,
            reply_markup=get_main_inline_kb(user_id)
        )
        task = asyncio.create_task(run_telethon_worker_for_user(user_id, bot))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
    except asyncio.CancelledError:
        # Задача отменена (например, по таймауту или нажатию кнопки "Отмена")
        await bot.edit_message_text(
            "🛑 **Авторизация по QR отменена.**",
            chat_id=user_id,
            message_id=message_id,
            reply_markup=get_main_inline_kb(user_id)
        )
    except Exception as e:
        logger.error(f"Ошибка QR-логина для {user_id}: {e}")
        await bot.edit_message_text(
            f"❌ **Ошибка QR-логина:** {e}",
            chat_id=user_id,
            message_id=message_id,
            reply_markup=get_main_inline_kb(user_id)
        )
    finally:
        if user_id in QR_LOGIN_WAITS:
            del QR_LOGIN_WAITS[user_id]
        if client.is_connected():
            await client.disconnect()
        
@user_router.callback_query(F.data == "auth_by_qr")
async def telethon_auth_by_qr_start(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("✅ Сессия Telethon уже активна.", show_alert=True)
        return

    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        
        # Получаем QR-код в виде URL
        qr_login = await client.qr_login()
        qr_url = qr_login.url 

        # Генерируем изображение QR-кода
        qr_img = await generate_qr_code(qr_url)
        
        kb_cancel = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛑 Отменить QR-вход", callback_data="cancel_qr_auth")]
        ])

        # Отправляем QR-код
        sent_message = await callback.message.answer_photo(
            photo=qr_img,
            caption="2️⃣ **Вход по QR-коду.**\n\n"
                    "Откройте **Настройки** -> **Устройства** -> **Связать настольное устройство**.\n"
                    "**Отсканируйте** этот QR-код вашим рабочим аккаунтом.\n\n"
                    "Ожидание сканирования... (2 минуты)",
            reply_markup=kb_cancel
        )
        await state.set_state(TelethonAuth.QR_LOGIN)
        await callback.answer()
        
        # Запускаем таск ожидания
        wait_task = asyncio.create_task(qr_waiter(client, user_id, sent_message.message_id, callback.bot))
        QR_LOGIN_WAITS[user_id] = wait_task
        
        # Добавляем таймаут на 120 секунд
        await asyncio.sleep(120)
        if user_id in QR_LOGIN_WAITS and not QR_LOGIN_WAITS[user_id].done():
            QR_LOGIN_WAITS[user_id].cancel()

    except Exception as e:
        logger.error(f"Ошибка при генерации QR: {e}")
        await callback.message.answer(f"❌ Ошибка при генерации QR-кода: {e}", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    finally:
        if client.is_connected(): await client.disconnect()
        
@user_router.callback_query(F.data == "cancel_qr_auth")
async def cancel_qr_auth(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id in QR_LOGIN_WAITS:
        QR_LOGIN_WAITS[user_id].cancel()
        await callback.answer("QR-вход отменен.", show_alert=True)
        await state.clear()
        
        # Обновляем сообщение
        await callback.message.edit_caption(
            caption="🛑 **Авторизация по QR отменена.**",
            reply_markup=None
        )
        await callback.message.edit_text(
            "🛑 **Авторизация по QR отменена.**", 
            reply_markup=get_main_inline_kb(user_id)
        )
    else:
        await callback.answer("QR-вход неактивен или уже завершен.", show_alert=True)

# --- МЕНЮ ОТЧЕТОВ И МОНИТОРИНГА (ОБНОВЛЕНО) ---
@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu_start(callback: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📈 IT-Отчеты", callback_data="monitor_IT")],
        [InlineKeyboardButton(text="📉 Дроп-Отчеты", callback_data="monitor_DROP")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(
        "📊 **Отчеты и Мониторинг.**\n\n"
        "Выберите тип отчета для настройки чата и получения статистики:",
        reply_markup=kb
    )
    await callback.answer()
    
@user_router.callback_query(F.data.startswith("monitor_"))
async def handle_monitor_menu(callback: types.CallbackQuery) -> None:
    # ... (логика мониторинга остается прежней, но используем Inline-кнопки)
    user_id = callback.from_user.id
    monitor_type = callback.data.split('_')[-1].upper()
    
    if not db_check_subscription(user_id) or not await check_channel_membership(user_id, callback.bot):
        await callback.answer("❌ Нет доступа. Требуется подписка и членство в канале.", show_alert=True)
        return
        
    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    
    status_text = f"📈 **Мониторинг {monitor_type}**\n\n"
    
    if not session_active:
        status_text += "🔴 Сессия Telethon неактивна. Запустите ее через '🔐 Авторизовать Telethon'."
        kb_monitor = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔐 Авторизовать Telethon", callback_data="telethon_auth_start")],
            [InlineKeyboardButton(text="⬅️ В меню отчетов", callback_data="show_monitor_menu")]
        ])
    else:
        session_data = db_get_session_data(user_id)
        chat_id = session_data.get(f'{monitor_type.lower()}_chat_id') if session_data else None
        
        if chat_id:
            status_text += f"🟢 **Чат для команд:** `{chat_id}`\n"
            status_text += f"💬 **Ожидаемые команды:**\n"
            if monitor_type == 'IT':
                status_text += "`.встал`, `.кьар`, `.ошибка`, `.замена`, `.повтор`\n"
            else:
                status_text += "Заявки: `номер время @user бх`\n"
            status_text += "\n"
            
            kb_monitor = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"⚙️ Настроить чат {monitor_type}", callback_data=f"config_chat_{monitor_type}"),
                 InlineKeyboardButton(text=f"📊 Получить Отчет", callback_data=f"get_report_{monitor_type}")],
                [InlineKeyboardButton(text="⬅️ В меню отчетов", callback_data="show_monitor_menu")]
            ])
        else:
            status_text += f"🔴 Чат для мониторинга не настроен.\n"
            kb_monitor = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"⚙️ Настроить чат {monitor_type}", callback_data=f"config_chat_{monitor_type}")],
                [InlineKeyboardButton(text="⬅️ В меню отчетов", callback_data="show_monitor_menu")]
            ])

    await callback.message.edit_text(status_text, reply_markup=kb_monitor)
    await callback.answer()

# ... (Остальные хендлеры: config_chat_start, process_config_chat_id, get_monitor_report, admin_panel, promo и т.д. остаются привязанными к Inline)
