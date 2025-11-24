import asyncio
import logging
import os
import sqlite3
import pytz 
import qrcode 
import time
from io import BytesIO 
from datetime import datetime, timedelta 
from typing import Optional, Set, Dict, Any

# --- Aiogram и FSM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile, FSInputFile
from aiogram.client.default import DefaultBotProperties 

# --- Telethon ---
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, RPCError, UserDeactivatedError, ChatForwardsRestrictedError
from telethon.tl.types import PeerChannel
from telethon.utils import get_display_name

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

# FSM Состояния
class AdminStates(StatesGroup):
    waiting_for_promo_user_id = State()
    
    waiting_for_new_promo_code = State()
    waiting_for_new_promo_days = State()
    waiting_for_new_promo_max_uses = State()
    
# FSM для авторизации Telethon
class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()

# FSM для активации промокода
class PromoStates(StatesGroup):
    waiting_for_code = State()
    
# НОВЫЕ СОСТОЯНИЯ ДЛЯ НАСТРОЙКИ МОНИТОРИНГА
class MonitorStates(StatesGroup):
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()
    
# Роутеры
auth_router = Router(name="auth")
user_router = Router(name="user")


# =========================================================================
# II. БАЗА ДАННЫХ (DB)
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
            it_chat_id TEXT,    -- Чат для IT-ворка
            drop_chat_id TEXT,  -- Чат для Дроп-ворка
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
    # НОВАЯ ТАБЛИЦА ДЛЯ ХРАНЕНИЯ ЛОГОВ (ОТЧЕТОВ)
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

# --- DB-функции для пользователей и подписок ---
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

def db_check_subscription(user_id: int) -> bool:
    user = db_get_user(user_id)
    if not user or not user.get('subscription_active'):
        return False
        
    end_date_str = user.get('subscription_end_date')
    if not end_date_str:
        return False

    try:
        end_date_utc = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        logger.error(f"Неверный формат даты подписки: {end_date_str}")
        return False
        
    now_utc = datetime.now()

    if end_date_utc > now_utc:
        return True
    else:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET subscription_active=0, subscription_end_date=NULL WHERE user_id=?", (user_id,))
        conn.commit()
        return False

def db_activate_subscription(user_id: int, days: int = 30) -> datetime:
    end_date_utc = datetime.now() + timedelta(days=days) 
    end_date_str = end_date_utc.strftime('%Y-%m-%d %H:%M:%S')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (user_id, subscription_active, subscription_end_date) 
        VALUES (?, 1, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            subscription_active=1, 
            subscription_end_date=?;
    """, (user_id, end_date_str, end_date_str))
    conn.commit()
    
    end_date_msk = pytz.utc.localize(end_date_utc).astimezone(TIMEZONE_MSK)
    return end_date_msk

# --- DB-функции для Telethon сессий (ОБНОВЛЕНЫ) ---

def get_session_file_path(user_id: int) -> str:
    return os.path.join('data', f'session_{user_id}.session')

def db_get_session_data(user_id: int) -> Optional[dict]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM telethon_sessions WHERE user_id=?", (user_id,))
    row = cursor.fetchone()
    if row:
        cols = [desc[0] for desc in cursor.description]
        return dict(zip(cols, row))
    return None

def db_set_session_status(user_id: int, is_active: bool, hash_code: Optional[str] = None):
    conn = get_db_connection()
    cursor = conn.cursor()
    session_file = get_session_file_path(user_id)
    
    cursor.execute("""
        INSERT INTO telethon_sessions (user_id, session_file, is_active, phone_code_hash) 
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            is_active=excluded.is_active, 
            phone_code_hash=COALESCE(excluded.phone_code_hash, telethon_sessions.phone_code_hash)
    """, (user_id, session_file, is_active, hash_code))
    conn.commit()

def db_set_monitor_chat_id(user_id: int, monitor_type: str, chat_id_str: str):
    """Сохраняет ID чата для мониторинга ('it' или 'drop')."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    column = f'{monitor_type}_chat_id'
    session_file = get_session_file_path(user_id)
    
    cursor.execute(f"""
        INSERT INTO telethon_sessions (user_id, session_file, {column}) 
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            {column}=excluded.{column}
    """, (user_id, session_file, chat_id_str))
    conn.commit()

# --- DB-функции для промокодов и логов (добавление логов) ---

def db_check_and_use_promo(code: str) -> Optional[int]:
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT days, is_active, max_uses, current_uses FROM promo_codes WHERE code=?", (code,))
    promo = cursor.fetchone()
    
    if not promo: return None
    days, is_active, max_uses, current_uses = promo
    
    if not is_active: return None
    if max_uses is not None and current_uses >= max_uses:
        cursor.execute("UPDATE promo_codes SET is_active=0 WHERE code=?", (code,))
        conn.commit()
        return None
        
    cursor.execute("UPDATE promo_codes SET current_uses=current_uses + 1 WHERE code=?", (code,))
    conn.commit()
    
    return days

def db_create_promo_code(code: str, days: int, max_uses: Optional[int] = None):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if max_uses is not None and max_uses <= 0: max_uses = None 
            
        cursor.execute("""
            INSERT INTO promo_codes (code, days, max_uses)
            VALUES (?, ?, ?)
        """, (code.upper(), days, max_uses))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False 

def db_add_monitor_log(user_id: int, log_type: str, command: str, target: str):
    """Добавляет запись в лог мониторинга."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO monitor_logs (user_id, type, command, target)
        VALUES (?, ?, ?, ?)
    """, (user_id, log_type, command, target))
    conn.commit()

def db_get_monitor_logs(user_id: int, log_type: str) -> list[tuple]:
    """Извлекает логи для отчета."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT timestamp, command, target 
        FROM monitor_logs 
        WHERE user_id=? AND type=? 
        ORDER BY timestamp
    """, (user_id, log_type))
    return cursor.fetchall()
    
def db_clear_monitor_logs(user_id: int, log_type: str):
    """Очищает логи после генерации отчета."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM monitor_logs WHERE user_id=? AND type=?", (user_id, log_type))
    conn.commit()


# =========================================================================
# III. TELETHON WORKER (МУЛЬТИСЕССИИ И ЛОГИКА МОНИТОРИНГА)
# =========================================================================

async def check_channel_membership(user_id: int, bot: Bot) -> bool:
    # ... (логика проверки членства)
    try:
        chat_member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
        return chat_member.status in ["member", "administrator", "creator"]
    except Exception as e:
        if 'user not found' in str(e).lower() or 'not a member' in str(e).lower():
             return False
        logger.error(f"Ошибка проверки членства в канале для {user_id}: {e}")
        return False

async def run_telethon_worker_for_user(user_id: int, bot: Bot):
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        await client.start()
        user_info = await client.get_me()
        logger.info(f"✅ Telethon Worker [{user_id}] запущен как: {get_display_name(user_info)}")

        # Обновляем статус в БД на Активен
        db_set_session_status(user_id, True)

        # --- TELETHON ХЕНДЛЕРЫ ДЛЯ КОМАНД ---
        
        async def handle_it_commands(event: events.NewMessage.Event):
            """Обработка команд IT-ворка (.встал, .ошибка и т.д.)"""
            
            # Получаем настройки чатов для этого пользователя
            session_data = db_get_session_data(user_id)
            if not session_data or not session_data.get('it_chat_id'):
                return # Мониторинг не настроен
                
            it_chat = session_data['it_chat_id']
            try:
                it_chat_entity = await client.get_entity(it_chat)
            except Exception:
                return # Чат не найден или не доступен
            
            # Проверяем, что сообщение пришло из нужного чата
            if event.chat_id != it_chat_entity.id:
                return

            msg_text = event.message.message.lower()
            
            # 1. КОМАНДА .ВСТАЛ
            if msg_text.startswith('.встал'):
                target = ""
                # Если ответ на сообщение
                if event.is_reply and event.reply_to_msg_id:
                    original_message = await client.get_messages(event.chat_id, ids=event.reply_to_msg_id)
                    if original_message and original_message[0].message:
                        target = original_message[0].message.split()[0] # Берем первое слово из исходного (обычно номер)
                
                # Если с аргументом
                elif len(msg_text.split()) > 1:
                    target = msg_text.split()[1] # Берем второй аргумент (номер)
                    
                if target:
                    db_add_monitor_log(user_id, 'IT', '.встал', target)
                    await client.send_message(it_chat_entity, f"✅ Лог: .встал ({target}) добавлен.", reply_to=event.id)
                    return
            
            # 2. КОМАНДЫ БЕЗ АРГУМЕНТОВ
            commands_map = {
                '.кьар': 'QR',
                '.ошибка': 'ERROR',
                '.замена': 'REPLACE',
            }
            if msg_text.split()[0] in commands_map:
                target = event.reply_to_msg_id if event.is_reply else 'N/A' # Логируем ID сообщения
                db_add_monitor_log(user_id, 'IT', msg_text.split()[0], str(target))
                await client.send_message(it_chat_entity, f"✅ Лог: {msg_text.split()[0]} добавлен.", reply_to=event.id)
                return

        client.add_event_handler(handle_it_commands, events.NewMessage(pattern=r'^\.(встал|кьар|ошибка|замена|повтор).*'))


        async def handle_drop_commands(event: events.NewMessage.Event):
            """Обработка команд Дроп-ворка (.дропворк)"""
            
            session_data = db_get_session_data(user_id)
            if not session_data or not session_data.get('drop_chat_id'):
                return
                
            drop_chat = session_data['drop_chat_id']
            try:
                drop_chat_entity = await client.get_entity(drop_chat)
            except Exception:
                return

            if event.chat_id != drop_chat_entity.id:
                return

            msg_text = event.message.message.strip()
            # Условие: номер время свой юзернейм и подпись бх (например: 1234 10:30 @user_name бх)
            parts = msg_text.split()
            if len(parts) >= 4 and parts[-1].lower() == 'бх':
                target_info = msg_text
                db_add_monitor_log(user_id, 'DROP', 'Новая заявка', target_info)
                await client.send_message(drop_chat_entity, f"✅ Лог: Заявка Дропа добавлена.", reply_to=event.id)
                return

        client.add_event_handler(handle_drop_commands, events.NewMessage(func=lambda e: e.message and len(e.message.split()) >= 4 and e.message.split()[-1].lower() == 'бх'))


        await client.run_until_disconnected()

    except UserDeactivatedError:
        logger.warning(f"❌ Telethon Worker [{user_id}]: Аккаунт деактивирован.")
        db_set_session_status(user_id, False)
        if os.path.exists(session_path + '.session'):
            os.remove(session_path + '.session')
    except Exception as e:
        logger.error(f"❌ Критическая ошибка Telethon Worker [{user_id}]: {e}")
        db_set_session_status(user_id, False)
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS:
            del ACTIVE_TELETHON_CLIENTS[user_id]
        if client.is_connected():
            await client.disconnect()
        logger.info(f"Telethon Worker [{user_id}] остановлен.")


async def start_all_telethon_workers(bot: Bot):
    # ... (логика запуска всех воркеров)
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
# IV. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И КЛАВИАТУРЫ
# =========================================================================

def kb_back_to_main(user_id: int) -> InlineKeyboardMarkup:
    callback_data = "admin_panel" if user_id == ADMIN_ID else "back_to_main"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data=callback_data)]
    ])

def get_main_inline_kb(user_id: int) -> InlineKeyboardMarkup:
    is_admin = user_id == ADMIN_ID
    
    kb = [
        [InlineKeyboardButton(text="💳 Подписка", callback_data="show_subscription"),
         InlineKeyboardButton(text="🔑 Промокод", callback_data="activate_promo")],
        [InlineKeyboardButton(text="ℹ️ Помощь", callback_data="show_help")],
        [InlineKeyboardButton(text="IT-Отчеты", callback_data="monitor_it"), 
         InlineKeyboardButton(text="Дроп-Отчеты", callback_data="monitor_drop")],
    ]
    
    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    
    if is_admin:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel")])

    if not session_active:
        kb.append([InlineKeyboardButton(text="🔐 Авторизация Telethon", callback_data="telethon_auth_start")])
    else:
         kb.append([InlineKeyboardButton(text="🟢 Сессия Telethon активна", callback_data="telethon_auth_status")])

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
    """Форматирует логи в текстовый файл для отчета."""
    if not logs:
        return None
        
    header = f"--- ОТЧЕТ {log_type} (Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S MSK')}) ---\n"
    content = ""
    
    for timestamp, command, target in logs:
        timestamp_msk = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').astimezone(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        
        if log_type == 'IT':
            # TIMESTAMP | КОМАНДА | ЦЕЛЬ (НОМЕР/ID)
            content += f"[{timestamp_msk}] {command.upper()}: {target}\n"
        elif log_type == 'DROP':
            # TIMESTAMP | НОВАЯ ЗАЯВКА | ВСЯ ИНФОРМАЦИЯ
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
        f"🤖 Добро пожаловать, **{user.first_name}**!\n\n"
        f"Ваш ID: `{user.id}`. Выберите действие в Inline-меню ниже."
    )
    
    if is_callback:
        await message.edit_text(text, reply_markup=get_main_inline_kb(user.id))
        await query_or_message.answer()
    else:
        await message.answer(text, reply_markup=get_main_inline_kb(user.id))

# --- АВТОРИЗАЦИЯ TELETHON (Шаги остаются прежними) ---
# ... (telethon_auth_start, telethon_auth_step_phone, telethon_auth_step_code, telethon_auth_step_password) ...

@user_router.callback_query(F.data.in_({"telethon_auth_start", "telethon_auth_status"}))
async def telethon_auth_start(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("✅ Ваша сессия Telethon уже активна.", show_alert=True)
        return

    if not API_ID or not API_HASH:
        await callback.answer("❌ API_ID и API_HASH не настроены администратором.", show_alert=True)
        return

    await state.set_state(TelethonAuth.PHONE)
    await callback.message.edit_text(
        "🔐 **Шаг 1: Ввод телефона**\n\n"
        "Введите номер телефона для авторизации вашего аккаунта Telethon (например, +79001234567):",
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
        await client.connect()
        if not await client.is_user_authorized():
            
            result = await client.send_code_request(phone_number)
            
            await state.update_data(phone=phone_number, phone_code_hash=result.phone_code_hash) 
            db_set_session_status(user_id, False, hash_code=result.phone_code_hash)
            
            await state.set_state(TelethonAuth.CODE)
            await message.answer("🔐 **Шаг 2: Ввод кода**\n\nВведите код, который пришел в Telegram:")
        else:
            await message.answer("⚠️ Аккаунт уже авторизован. Запуск Telethon Worker...")
            task = asyncio.create_task(run_telethon_worker_for_user(user_id, bot))
            ACTIVE_TELETHON_WORKERS[user_id] = task
            
            await state.clear()
            await message.answer("✅ Telethon Worker запущен!", reply_markup=get_main_inline_kb(user_id))
            
    except RPCError as e:
        logger.error(f"Telethon Auth Error: {e}")
        await message.answer(f"❌ Ошибка Telethon: {e}")
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()

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
            await message.answer("🔒 **Шаг 3: Ввод пароля**\n\nТребуется двухфакторная аутентификация. Введите пароль:")
            return
        except Exception as e:
            await message.answer(f"❌ Ошибка входа: {e}")
            await state.clear()
            return

        await message.answer(f"✅ Аккаунт @{user_info.username or user_info.first_name} успешно авторизован! Запуск Worker...")
        
        task = asyncio.create_task(run_telethon_worker_for_user(user_id, bot))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await state.clear()
        await message.answer("✅ Telethon Worker запущен!", reply_markup=get_main_inline_kb(user_id))
            
    except Exception as e:
        await message.answer(f"❌ Критическая ошибка: {e}")
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()

@user_router.message(TelethonAuth.PASSWORD)
async def telethon_auth_step_password(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    password = message.text.strip()
    session_path = get_session_file_path(user_id)
    
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        user_info = await client.sign_in(password=password)

        await message.answer(f"✅ Аккаунт @{user_info.username or user_info.first_name} успешно авторизован! Запуск Worker...")
        
        task = asyncio.create_task(run_telethon_worker_for_user(user_id, bot))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await state.clear()
        await message.answer("✅ Telethon Worker запущен!", reply_markup=get_main_inline_kb(user_id))

    except Exception as e:
        await message.answer(f"❌ Ошибка входа с паролем: {e}")
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()


# --- НАСТРОЙКА МОНИТОРИНГА (ОБНОВЛЕНО) ---
@user_router.callback_query(F.data.startswith("monitor_"))
async def handle_monitor_menu(callback: types.CallbackQuery) -> None:
    user_id = callback.from_user.id
    monitor_type = callback.data.split('_')[-1].upper()
    
    if not db_check_subscription(user_id) or not await check_channel_membership(user_id, callback.bot):
        await callback.answer("❌ Нет доступа. Требуется подписка и членство в канале.", show_alert=True)
        return
        
    if user_id not in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("❌ Сначала авторизуйте аккаунт через '🔐 Авторизация Telethon'.", show_alert=True)
        return
    
    session_data = db_get_session_data(user_id)
    chat_id = session_data.get(f'{monitor_type.lower()}_chat_id') if session_data else None
    
    status_text = f"**Статус мониторинга {monitor_type}:**\n"
    if chat_id:
        status_text += f"🟢 Активен в чате: `{chat_id}`\n"
    else:
        status_text += f"🔴 Не настроен.\n"

    kb_monitor = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⚙️ Настроить чат {monitor_type}", callback_data=f"config_chat_{monitor_type}")],
        [InlineKeyboardButton(text=f"📊 Получить Отчет {monitor_type}", callback_data=f"get_report_{monitor_type}")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])

    await callback.message.edit_text(status_text, reply_markup=kb_monitor)
    await callback.answer()

# --- ШАГ 1: НАСТРОЙКА ЧАТА ---
@user_router.callback_query(F.data.startswith("config_chat_"))
async def config_chat_start(callback: types.CallbackQuery, state: FSMContext):
    monitor_type = callback.data.split('_')[-1].upper()
    
    await state.set_state(MonitorStates.waiting_for_it_chat_id if monitor_type == 'IT' else MonitorStates.waiting_for_drop_chat_id)
    
    await callback.message.edit_text(
        f"⚙️ **Настройка чата {monitor_type}**\n\n"
        f"Введите **ID чата** (например, `-10012345678`) или **Username** (например, `@chat_name`), в котором будет работать команда `.{monitor_type.lower()}ворк`.\n"
        f"**Важно:** Ваш авторизованный аккаунт должен быть администратором в этом чате.",
        reply_markup=kb_back_to_main(callback.from_user.id)
    )
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_it_chat_id, F.text)
@user_router.message(MonitorStates.waiting_for_drop_chat_id, F.text)
async def process_config_chat_id(message: Message, state: FSMContext):
    user_id = message.from_user.id
    chat_id_str = message.text.strip()
    
    current_state = await state.get_state()
    monitor_type = 'IT' if current_state == MonitorStates.waiting_for_it_chat_id else 'DROP'
    
    # 1. Проверка доступности чата через Telethon
    client = ACTIVE_TELETHON_CLIENTS.get(user_id)
    if not client:
        await message.answer("❌ Сессия Telethon неактивна. Запустите ее сначала.")
        await state.clear()
        return

    try:
        # Пытаемся получить Entity для проверки доступности
        entity = await client.get_entity(chat_id_str)
        
        # 2. Сохранение ID
        db_set_monitor_chat_id(user_id, monitor_type.lower(), str(entity.id))
        
        await message.answer(
            f"✅ **Чат для {monitor_type} успешно настроен!**\n"
            f"ID чата: `{entity.id}`.\n"
            f"Теперь ваш аккаунт будет отслеживать команды в этом чате.",
            reply_markup=get_main_inline_kb(user_id)
        )
    except RPCError as e:
        await message.answer(
            f"❌ Ошибка Telethon: Не удалось получить доступ к чату `{chat_id_str}`.\n"
            f"Проверьте, что аккаунт в нем состоит и имеет нужные права. Ошибка: {e}",
            reply_markup=kb_back_to_main(user_id)
        )
    except Exception as e:
        await message.answer(f"❌ Критическая ошибка: {e}", reply_markup=kb_back_to_main(user_id))
        
    await state.clear()


# --- ГЕНЕРАЦИЯ ОТЧЕТА ---
@user_router.callback_query(F.data.startswith("get_report_"))
async def get_monitor_report(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    monitor_type = callback.data.split('_')[-1].upper()
    
    logs = db_get_monitor_logs(user_id, monitor_type)
    
    if not logs:
        await callback.answer(f"❌ Логи {monitor_type} пусты. Начните работу в настроенном чате.", show_alert=True)
        return
        
    report_file = format_monitor_logs_to_file(logs, monitor_type)
    
    if report_file:
        await callback.message.answer_document(
            document=report_file,
            caption=f"📊 **Отчет {monitor_type}**\n\n"
                    f"Сгенерировано {len(logs)} записей.\n"
                    f"Логи очищены.",
            reply_markup=kb_back_to_main(user_id)
        )
        db_clear_monitor_logs(user_id, monitor_type)
        
        # Удаляем временный файл
        if os.path.exists(report_file.path):
            os.remove(report_file.path)
            
    await callback.answer()


# --- ПРОМОКОДЫ И АДМИН ПАНЕЛЬ (Остаются прежними) ---
# ... (show_subscription, generate_qr_payment, cmd_activate_promo_start, process_activate_promo) ...
# ... (show_admin_panel, cmd_admin_issue_promo, process_admin_issued_promo) ...
# ... (cmd_admin_create_promo_start, process_admin_create_promo_code, process_admin_create_promo_days, process_admin_create_promo_max_uses) ...


# =========================================================================
# VI. ГЛАВНАЯ ТОЧКА ЗАПУСКА
# =========================================================================

async def main():
    if not BOT_TOKEN or not API_ID or not API_HASH:
        logger.critical("КРИТИЧЕСКАЯ ОШИБКА: Один или несколько API ключей/токенов не найдены.")
        return

    logger.info("Инициализация базы данных и проверка таблиц...")
    os.makedirs('data', exist_ok=True) 
    create_tables()
    
    storage = MemoryStorage() 
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='Markdown')) 
    dp = Dispatcher(storage=storage)
    
    dp.include_router(auth_router)
    dp.include_router(user_router)

    startup_task = asyncio.create_task(start_all_telethon_workers(bot))
    await startup_task

    logger.info("Бот запущен. Ожидание сообщений...")
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске Aiogram: {e}")
    finally:
        for task in ACTIVE_TELETHON_WORKERS.values():
            task.cancel()
        logger.info("Бот остановлен.")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Принудительная остановка бота (KeyboardInterrupt).")
    except Exception as e:
        logger.error(f"Критическая ошибка вне цикла: {e}")
