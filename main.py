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
from aiogram.filters.state import StateFilter # Явный импорт для фильтрации

# Импорты telethon
from telethon import TelegramClient, events
from telethon.errors import UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError
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
TARGET_CHANNEL_URL = "@STAT_PRO1" # Канал для обязательной подписки
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальные хранилища для активных сессий Telethon и долгих задач
ACTIVE_TELETHON_CLIENTS = {}
ACTIVE_TELETHON_WORKERS = {}
ACTIVE_TELETHON_TASKS = {} 

storage = MemoryStorage()
# Инициализация бота с новым токеном
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='Markdown'))
dp = Dispatcher(storage=storage)
user_router = Router()

# =========================================================================
# II. FSM-СОСТОЯНИЯ
# =========================================================================

class TelethonAuth(StatesGroup):
    """Состояния для процесса авторизации Telethon."""
    CHOOSE_AUTH_METHOD = State()
    PHONE = State()
    QR_CODE_WAIT = State()
    CODE = State()
    PASSWORD = State()

class PromoStates(StatesGroup):
    """Состояния для активации промокода пользователем."""
    waiting_for_code = State()

class AdminStates(StatesGroup):
    """Состояния для Админ-панели."""
    main_menu = State()
    creating_promo_code = State()
    creating_promo_days = State()
    creating_promo_uses = State()
    sub_target_user_id = State()
    sub_duration_days = State()

class MonitorStates(StatesGroup):
    """Состояния для настройки мониторинга."""
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()

class ReportStates(StatesGroup):
    """Состояния для настройки и отправки отчета."""
    waiting_report_target = State()
    waiting_report_topic = State() 
    waiting_report_send_chat = State() 

class TelethonCommands(StatesGroup):
    """Состояния для сложных Telethon команд (.флуд, .лс)"""
    waiting_ls_params = State()
    waiting_flood_params = State()
    waiting_check_params = State()


# =========================================================================
# III. РАБОТА С БАЗОЙ ДАННЫХ (SQLite)
# =========================================================================

DB_PATH = os.path.join('data', DB_NAME)

def get_db_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def db_init():
    conn = get_db_connection()
    cur = conn.cursor()
    # Таблица для пользователей
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            subscription_active BOOLEAN NOT NULL DEFAULT 0,
            subscription_end_date TEXT,
            telethon_active BOOLEAN NOT NULL DEFAULT 0,
            telethon_hash TEXT,
            promo_code TEXT,
            it_chat_id TEXT,
            drop_chat_id TEXT,
            report_chat_id TEXT
        )
    """)
    # Таблица для промокодов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            days INTEGER NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT 1,
            max_uses INTEGER,
            current_uses INTEGER NOT NULL DEFAULT 0
        )
    """)
    # Таблица для логов мониторинга
    cur.execute("""
        CREATE TABLE IF NOT EXISTS monitor_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            type TEXT NOT NULL, 
            command TEXT,
            target TEXT,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    """)
    conn.commit()

def db_update_subscription(user_id: int, days: int):
    conn = get_db_connection()
    cur = conn.cursor()
    user = db_get_user(user_id)
    
    current_end_date_str = user.get('subscription_end_date')
    now_msk = datetime.now(TIMEZONE_MSK)
    
    # Определяем, от какой даты отсчитывать новые дни
    if current_end_date_str:
        try:
            current_end_date = TIMEZONE_MSK.localize(datetime.strptime(current_end_date_str, '%Y-%m-%d %H:%M:%S'))
            start_date = max(now_msk, current_end_date)
        except ValueError:
            start_date = now_msk
    else:
        start_date = now_msk

    new_end_date = start_date + timedelta(days=days)
    new_end_date_str = new_end_date.strftime('%Y-%m-%d %H:%M:%S')

    cur.execute("""
        UPDATE users SET subscription_active=?, subscription_end_date=? WHERE user_id=?
    """, (1, new_end_date_str, user_id))
    conn.commit()
    return new_end_date_str

def db_get_user(user_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO users (user_id) VALUES (?)
    """, (user_id,))
    conn.commit()
    
    cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    if row:
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))
    return None

def db_check_subscription(user_id: int) -> bool:
    user = db_get_user(user_id)
    if not user or not user.get('subscription_active'):
        return False
    try:
        end_date_str = user.get('subscription_end_date')
        if not end_date_str: return False
        
        end_date = TIMEZONE_MSK.localize(datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S'))
        now_msk = datetime.now(TIMEZONE_MSK)
    except Exception:
        return False
    return end_date > now_msk

def db_set_session_status(user_id: int, is_active: bool, hash_code: str = None):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Гарантируем, что пользователь существует
    db_get_user(user_id) 
    
    cur.execute("""
        UPDATE users SET telethon_active=?, telethon_hash=? WHERE user_id=?
    """, (1 if is_active else 0, hash_code, user_id))
    conn.commit()

def db_clear_monitor_logs(user_id: int, log_type: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM monitor_logs WHERE user_id=? AND type=?", (user_id, log_type))
    conn.commit()
    return cur.rowcount

def db_get_monitor_logs(user_id, log_type, since_days: int = None):
    conn = get_db_connection()
    cur = conn.cursor()
    query = "SELECT timestamp, command, target FROM monitor_logs WHERE user_id=? AND type=? "
    params = [user_id, log_type]
    
    if since_days is not None and since_days > 0:
        cutoff_date = (datetime.now(TIMEZONE_MSK) - timedelta(days=since_days)).strftime('%Y-%m-%d %H:%M:%S')
        query += "AND timestamp >= ? "
        params.append(cutoff_date)

    query += "ORDER BY timestamp"
    cur.execute(query, params)
    return cur.fetchall()

def db_add_monitor_log(user_id, log_type, command, target):
    conn = get_db_connection()
    cur = conn.cursor()
    timestamp = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("INSERT INTO monitor_logs (user_id, timestamp, type, command, target) VALUES (?, ?, ?, ?, ?)",
                (user_id, timestamp, log_type, command, target))
    conn.commit()

def db_get_active_telethon_users():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
    return [row[0] for row in cur.fetchall()]

def db_set_chat_id(user_id: int, chat_type: str, chat_id: str):
    conn = get_db_connection()
    cur = conn.cursor()
    
    if chat_type == 'IT':
        col = 'it_chat_id'
    elif chat_type == 'DROP':
        col = 'drop_chat_id'
    else:
        return
    
    cur.execute(f"UPDATE users SET {col}=? WHERE user_id=?", (chat_id, user_id))
    conn.commit()
    
def db_add_promo_code(code: str, days: int, max_uses: int | None = None):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO promo_codes (code, days, max_uses) VALUES (?, ?, ?)
    """, (code, days, max_uses))
    conn.commit()

def db_get_promo_code(code: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM promo_codes WHERE code=?", (code,))
    row = cur.fetchone()
    if row:
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))
    return None

def db_use_promo_code(code: str):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Увеличиваем счетчик использования
    cur.execute("""
        UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code=?
    """, (code,))
    
    # 2. Проверяем, нужно ли деактивировать (если current_uses >= max_uses)
    cur.execute("""
        UPDATE promo_codes SET is_active = 0 
        WHERE code=? AND max_uses IS NOT NULL AND current_uses >= max_uses
    """, (code,))
    
    conn.commit()

# =========================================================================
# IV. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И КЛАВИАТУРА
# =========================================================================

def generate_promo_code(length=10):
    """Генерирует случайный промокод."""
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def get_cancel_task_kb(task_name: str) -> InlineKeyboardMarkup:
    """Клавиатура для остановки активной Telethon-задачи."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛑 Отмена", callback_data=f"cancel_telethon_task_{task_name}")]
    ])

def get_session_file_path(user_id: int):
    os.makedirs('data', exist_ok=True)
    return os.path.join('data', f'session_{user_id}')

async def check_channel_subscription(user_id: int, bot: Bot) -> bool:
    """Проверяет подписку на обязательный канал."""
    try:
        member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id) 
        if member.status in ["member", "administrator", "creator"]:
             return True
    except Exception as e:
        # Ошибка, возможно, потому что канал не существует или бот не админ
        logger.error(f"Ошибка проверки подписки на канал для {user_id}: {e}")
        
    return False

async def check_access(user_id: int, bot: Bot) -> tuple[bool, str]:
    """Проверяет доступ пользователя (админ, подписка, канал)."""
    if user_id == ADMIN_ID:
        return True, ""
    
    user = db_get_user(user_id)

    # 1. Проверка активной подписки по сроку
    subscribed_by_time = db_check_subscription(user_id)
    if subscribed_by_time:
        return True, ""
    
    # 2. Проверка подписки на канал (TARGET_CHANNEL_URL)
    if await check_channel_subscription(user_id, bot):
        return True, ""
        
    return False, f"❌ Для использования бота необходима активная подписка или подписка на канал **{TARGET_CHANNEL_URL}**."

def get_cancel_keyboard():
    """Возвращает клавиатуру с кнопкой 'Отмена'."""
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
        ]
    )

def get_channel_check_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➡️ Канал для подписки", url=f"https://t.me/{TARGET_CHANNEL_URL.lstrip('@')}")] ,
        [InlineKeyboardButton(text="✅ Я подписался!", callback_data="back_to_main")] 
    ])


def get_numeric_code_keyboard(current_code=""):
    kb = [
        [
            InlineKeyboardButton(text="1️⃣", callback_data="auth_digit_1"),
            InlineKeyboardButton(text="2️⃣", callback_data="auth_digit_2"),
            InlineKeyboardButton(text="3️⃣", callback_data="auth_digit_3"),
        ],
        [
            InlineKeyboardButton(text="4️⃣", callback_data="auth_digit_4"),
            InlineKeyboardButton(text="5️⃣", callback_data="auth_digit_5"),
            InlineKeyboardButton(text="6️⃣", callback_data="auth_digit_6"),
        ],
        [
            InlineKeyboardButton(text="7️⃣", callback_data="auth_digit_7"),
            InlineKeyboardButton(text="8️⃣", callback_data="auth_digit_8"),
            InlineKeyboardButton(text="9️⃣", callback_data="auth_digit_9"),
        ],
        [
            InlineKeyboardButton(text="⬅️ Удалить", callback_data="auth_delete_digit"),
            InlineKeyboardButton(text="0️⃣", callback_data="auth_digit_0"),
            InlineKeyboardButton(text="✅ Ввод", callback_data="auth_submit_code"), 
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


def get_main_inline_kb(user_id: int, check_sub: bool) -> InlineKeyboardMarkup:
    """Генерирует ГЛАВНУЮ клавиатуру."""
    user_data = db_get_user(user_id)
    is_telethon_active = user_data.get('telethon_active', 0) if user_data else 0
    worker_running = user_id in ACTIVE_TELETHON_WORKERS
    
    keyboard = []

    if not check_sub and user_id != ADMIN_ID:
        # Если нет подписки и не админ, показываем только проверку и промокод
        keyboard.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
        keyboard.append([InlineKeyboardButton(text="🌐 Проверить подписку", callback_data="back_to_main")])
    else:
        # Если подписка есть (по времени или по каналу)
        if not is_telethon_active:
            keyboard.append([InlineKeyboardButton(text="🔐 Авторизация", callback_data="telethon_auth_start")])
        else:
            keyboard.append([
                InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm"),
                InlineKeyboardButton(text="🔥 Расширенные Инструменты", callback_data="show_telethon_tools") 
            ])
            
            keyboard.append([
                InlineKeyboardButton(text="📊 Отчеты и Мониторинг", callback_data="show_monitor_menu")
            ])
            
            worker_text = "🟢 Worker запущен" if worker_running else "🔴 Worker остановлен"
            worker_callback = "telethon_stop_session" if worker_running else "telethon_start_session"
            
            keyboard.append([
                InlineKeyboardButton(text=worker_text, callback_data=worker_callback),
                InlineKeyboardButton(text="ℹ️ Статус", callback_data="telethon_check_status")
            ])
    
    if user_id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
        
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_auth_method_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Вход по Номеру", callback_data="auth_method_phone")],
        # [InlineKeyboardButton(text="🖼️ Вход по QR-коду", callback_data="auth_method_qr")], # Отключаем QR-код для простоты
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ])

def get_monitor_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настроить IT-Чат", callback_data="monitor_set_it")],
        [InlineKeyboardButton(text="⚙️ Настроить DROP-Чат", callback_data="monitor_set_drop")],
        [InlineKeyboardButton(text="📋 Сгенерировать Отчет", callback_data="monitor_generate_report_start")], 
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_telethon_tools_kb() -> InlineKeyboardMarkup:
    """Клавиатура для расширенных Telethon команд."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Массовая рассылка .лс", callback_data="cmd_ls_start")],
        [InlineKeyboardButton(text="💥 Запуск флуда .флуд", callback_data="cmd_flood_start")],
        [InlineKeyboardButton(text="🛑 Остановить флуд .стопфлуд", callback_data="cmd_stop_flood")],
        [InlineKeyboardButton(text="🔬 Анализ группы .чекгруппу", callback_data="cmd_check_group_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_admin_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать Промокод", callback_data="admin_create_promo_start")],
        [InlineKeyboardButton(text="➡️ Выдать Подписку", callback_data="admin_issue_sub_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])


# =========================================================================
# V. TELETHON WORKER И КОМАНДЫ
# =========================================================================

async def stop_telethon_worker_for_user(user_id: int):
    if user_id in ACTIVE_TELETHON_WORKERS and ACTIVE_TELETHON_WORKERS[user_id]:
        ACTIVE_TELETHON_WORKERS[user_id].cancel()
        del ACTIVE_TELETHON_WORKERS[user_id]
        logger.info(f"Telethon Worker [{user_id}] отменен.")
    
    if user_id in ACTIVE_TELETHON_CLIENTS:
        client = ACTIVE_TELETHON_CLIENTS[user_id]
        if client.is_connected():
            await client.disconnect()
        del ACTIVE_TELETHON_CLIENTS[user_id]
        logger.info(f"Telethon Client [{user_id}] отключен.")
        
    db_set_session_status(user_id, False)


async def run_telethon_worker_for_user(user_id: int):
    """Запускает Telethon worker для конкретного пользователя."""
    
    # Сначала всегда останавливаем старые
    await stop_telethon_worker_for_user(user_id) 
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        # Проверяем наличие файла сессии
        if not os.path.exists(session_path + '.session'):
            logger.warning(f"Файл сессии не найден для {user_id}. Требуется повторная авторизация.")
            db_set_session_status(user_id, False)
            await bot.send_message(user_id, "❌ Файл сессии не найден. Требуется повторная авторизация.", reply_markup=get_main_inline_kb(user_id, True))
            return

        await client.start()
        user_info = await client.get_me()
        logger.info(f"Telethon [{user_id}] запущен как: {get_display_name(user_info)}")
        
        db_set_session_status(user_id, True)
        
        await bot.send_message(user_id, "⚙️ **Telethon Worker запущен и готов к работе!**", reply_markup=get_main_inline_kb(user_id, True))

        user_db = db_get_user(user_id)
        it_chat_id_str = user_db.get('it_chat_id')
        drop_chat_id_str = user_db.get('drop_chat_id')

        # --- ХЕНДЛЕРЫ ДЛЯ МОНИТОРИНГА И ЛОГИРОВАНИЯ ---
        IT_PATTERNS = {
            ".встал": r'^\.встал.*', ".кьар": r'^\.кьар.*',
            ".ошибка": r'^\.ошибка.*', ".замена": r'^\.замена.*',
            ".повтор": r'^\.повтор.*',
        }
        # Паттерн для DROP-лога: Телефон Пробел Время Пробел @ник Пробел бх [Пробел Время]
        DROP_PATTERN_REGEX = r'^\+?\d{5,15}\s+\d{1,2}:\d{2}\s+@\w+\s+бх(?:\s+.*)?' 

        @client.on(events.NewMessage)
        async def monitor_listener(event):
            # Проверка доступа на уровне Worker
            has_access, _ = await check_access(user_id, bot)
            if not has_access and user_id != ADMIN_ID:
                # Если подписка кончилась, не обрабатываем
                return
            
            if not event.is_group and not event.is_channel or not event.message.text: return
            
            try:
                # Получаем chat_id в формате строки (Telethon использует ID, а не username)
                chat_entity = await event.get_chat()
                chat_id_str = str(chat_entity.id) if chat_entity else None
                if not chat_id_str: return

                message_text = event.message.text.strip()
                
                # IT Логирование
                if it_chat_id_str and chat_id_str == it_chat_id_str.lstrip('@'): # Учитываем, что в БД может быть username
                    for command, regex in IT_PATTERNS.items():
                        if re.match(regex, message_text, re.IGNORECASE | re.DOTALL):
                            db_add_monitor_log(user_id, 'IT', command, message_text)
                            break
                
                # DROP Логирование
                if drop_chat_id_str and chat_id_str == drop_chat_id_str.lstrip('@'):
                    if re.match(DROP_PATTERN_REGEX, message_text, re.IGNORECASE | re.DOTALL):
                         db_add_monitor_log(user_id, 'DROP', 'DROP_ENTRY', message_text)

            except Exception as e:
                logger.error(f"Ошибка в мониторинге Telethon для {user_id}: {e}")
                
        
        # --- ХЕНДЛЕРЫ ДЛЯ КОМАНД В ЛС TELETHON-АККАУНТА (Скелет) ---
        @client.on(events.NewMessage(pattern=r'^\.(лс|флуд|стопфлуд|чекгруппу).*'))
        async def telethon_command_handler(event):
            
            me = await client.get_me()
            # Обрабатываем только команды, которые пользователь отправил сам себе в ЛС (или их Worker)
            if event.sender_id != me.id: return
            if not event.is_private: return
            
            # Проверка доступа
            has_access, error_msg = await check_access(user_id, bot)
            if not has_access:
                await event.reply(f"❌ **Отказано в доступе.** {error_msg}")
                return
            
            command = event.text.split()[0].lower()
            
            # Логика выполнения команд... (заглушка)
            await event.reply(f"✅ Команда **{command}** принята к выполнению (Скелет).")
            
        await client.run_until_disconnected()
        
    except asyncio.CancelledError:
        logger.info(f"Telethon Worker [{user_id}] отменен по запросу.")
    except UserDeactivatedError:
        logger.warning(f"Аккаунт Telethon {user_id} деактивирован.")
        await bot.send_message(user_id, "⚠️ Аккаунт деактивирован. Требуется переавторизация.", reply_markup=get_main_inline_kb(user_id, True))
    except FloodWaitError as e:
         error_text = f"❌ Ошибка лимитов Telegram: Необходимо подождать {e.seconds} секунд."
         await bot.send_message(user_id, error_text, reply_markup=get_main_inline_kb(user_id, True))
    except Exception as e:
        logger.error(f"Критическая ошибка Telethon Worker [{user_id}]: {e}")
        error_text = f"❌ Критическая ошибка Telethon Worker: `{type(e).__name__}`. Требуется переавторизация."
        if "AuthorizationKeyUnregistered" in str(e):
             error_text = "❌ Ключ авторизации недействителен. Сессия завершена. Требуется переавторизация."
             
        await bot.send_message(user_id, error_text, reply_markup=get_main_inline_kb(user_id, True))
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS:
            del ACTIVE_TELETHON_CLIENTS[user_id]
        if user_id in ACTIVE_TELETHON_WORKERS:
            del ACTIVE_TELETHON_WORKERS[user_id]
        db_set_session_status(user_id, False)


async def start_all_active_telethon_workers():
    active_users = db_get_active_telethon_users()
    logger.info(f"Найдено {len(active_users)} активных Telethon-сессий в БД.")
    
    for user_id in active_users:
        if user_id not in ACTIVE_TELETHON_WORKERS:
            task = asyncio.create_task(run_telethon_worker_for_user(user_id))
            ACTIVE_TELETHON_WORKERS[user_id] = task
            logger.info(f"Worker запущен для пользователя {user_id}.")

# =========================================================================
# VI. ХЕНДЛЕРЫ AIOGRAM
# =========================================================================

# --- Обработчик Отмены и Назад ---
@user_router.callback_query(F.data == "cancel_action")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.edit_text("❌ Действие отменено.", reply_markup=None)
    except TelegramBadRequest:
        await callback.message.answer("❌ Действие отменено.", reply_markup=None)
    # Используем union, чтобы обработать как Message, так и CallbackQuery
    await cmd_start_or_back(callback, state)
    await callback.answer("Отменено.")

# --- Главное меню (/start) ---
@user_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = union.from_user.id
    
    db_get_user(user_id) # Гарантируем, что пользователь создан в БД
    
    # 1. Проверяем подписку по времени
    is_subscribed_by_time = db_check_subscription(user_id)
    
    # 2. Проверяем подписку на канал
    is_subscribed_to_channel = await check_channel_subscription(user_id, bot)
    
    has_access = is_subscribed_by_time or is_subscribed_to_channel or user_id == ADMIN_ID
    
    await state.clear()
    
    user_data = db_get_user(user_id)
    end_date_str = user_data.get('subscription_end_date')
    sub_info = f"Подписка до: `{end_date_str}`" if is_subscribed_by_time else "Подписка: `Нет`"
        
    text = (
        "👋 **Привет!**\n\n"
        f"Ваш ID: `{user_id}`\n"
        f"{sub_info}\n\n"
    )
    
    if not has_access and user_id != ADMIN_ID:
        text += (
            "⚠️ **Доступ ограничен.**\n"
            f"Для использования функционала необходимо подписаться на наш канал: **{TARGET_CHANNEL_URL}** или активировать Промокод.\n"
        )
        keyboard = get_channel_check_kb()
    else:
        text += (
            "✅ **Доступ открыт!**\n"
            "Это бот для мониторинга команд в Telegram-чатах с помощью вашей личной Telethon-сессии.\n\n"
            "Выберите опцию ниже."
        )
        keyboard = get_main_inline_kb(user_id, has_access)
        

    if isinstance(union, types.Message):
        await union.answer(text, reply_markup=keyboard)
    else:
        try:
            await union.message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest:
            pass 
        await union.answer()

# --- Telethon Авторизация ---

@user_router.callback_query(F.data == "telethon_auth_start")
async def telethon_auth_choose_method_handler(callback: types.CallbackQuery, state: FSMContext):
    # Дополнительная проверка доступа перед началом авторизации
    has_access, _ = await check_access(callback.from_user.id, bot)
    if not has_access:
        await callback.answer("❌ Доступ ограничен. Сначала получите подписку или подпишитесь на канал.", show_alert=True)
        return
        
    await state.set_state(TelethonAuth.CHOOSE_AUTH_METHOD)
    await callback.message.edit_text(
        "🔐 **Авторизация Telethon**\n\nВыберите удобный способ входа:",
        reply_markup=get_auth_method_kb()
    )
    await callback.answer()

@user_router.callback_query(F.data == "auth_method_phone", TelethonAuth.CHOOSE_AUTH_METHOD)
async def telethon_auth_start_phone(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await callback.message.edit_text(
        "📱 Введите **номер телефона** в формате: `+79001234567` (обязательно с международным кодом).",
        parse_mode="Markdown",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()

@user_router.message(TelethonAuth.PHONE)
async def telethon_auth_step_phone(message: Message, state: FSMContext):
    user_id = message.from_user.id
    phone_number = message.text.strip()
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        if not re.match(r'^\+\d{10,15}$', phone_number):
            raise PhoneNumberInvalidError("Неверный формат номера телефона.")
        
        await client.connect()
        result = await client.send_code_request(phone_number)
            
        await state.update_data(phone_number=phone_number, phone_code_hash=result.phone_code_hash, auth_code_temp="")
        
        await state.set_state(TelethonAuth.CODE)
        await message.answer(
            f"🔢 **Код подтверждения отправлен.**\n\n"
            f"Введите **код** с помощью кнопок ниже или сообщением.\n"
            f"Текущий ввод: `_`",
            reply_markup=get_numeric_code_keyboard() 
        )
        
    except PhoneNumberInvalidError:
        await message.answer("❌ **Ошибка:** Неверный формат номера телефона. Используйте `+79001234567`.", reply_markup=get_cancel_keyboard())
    except Exception as e:
        error_text = f"❌ **Критическая ошибка авторизации:** Не удалось отправить код. `{type(e).__name__}`"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()

# --- Исправленный UI для ввода кода (Обработка CallbackQuery и сообщений) ---
@user_router.callback_query(
    (F.data.startswith("auth_digit_")) | (F.data == "auth_submit_code") | (F.data == "auth_delete_digit"), 
    TelethonAuth.CODE
)
async def process_code_input_ui(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    temp_code = data.get('auth_code_temp', "")
    action = callback.data

    if action.startswith("auth_digit_"):
        digit = action.split('_')[2]
        if len(temp_code) < 6: 
            temp_code += digit
            
    elif action == "auth_delete_digit":
        temp_code = temp_code[:-1]

    await state.update_data(auth_code_temp=temp_code)
        
    if action == "auth_submit_code":
        if not temp_code.isdigit() or len(temp_code) < 4:
            await callback.answer("❌ Код слишком короткий. Введите минимум 4 цифры.", show_alert=True)
            return

        await callback.message.edit_text(f"⏳ Проверка кода: `{temp_code}`...", reply_markup=None)
        # Передаем управление основной логике проверки кода
        await telethon_auth_step_code_logic(callback.message, state, temp_code)
        await callback.answer("Код отправлен.")
        return

    current_display = f"`{temp_code}_`" if len(temp_code) < 6 else f"`{temp_code}`"
    try:
        await callback.message.edit_text(
            f"🔢 Введите **код** с помощью кнопок ниже или сообщением.\n"
            f"Текущий ввод: {current_display}",
            reply_markup=get_numeric_code_keyboard(temp_code)
        )
    except TelegramBadRequest:
        pass 
    
    await callback.answer()
        
@user_router.message(TelethonAuth.CODE)
async def process_code_input_message(message: Message, state: FSMContext):
    code = message.text.strip()
    if not code.isdigit() or len(code) < 4:
        await message.answer("❌ Неверный формат кода. Введите только цифры (минимум 4).", reply_markup=get_numeric_code_keyboard())
        return

    await message.answer(f"⏳ Проверка кода: `{code}`...", reply_markup=None)
    await telethon_auth_step_code_logic(message, state, code)

async def telethon_auth_step_code_logic(message: Message, state: FSMContext, code: str):
    user_id = message.from_user.id
    data = await state.get_data()
    phone_number = data['phone_number']
    phone_code_hash = data['phone_code_hash']
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        # Попытка авторизации
        user = await client.sign_in(phone_number, code, phone_code_hash=phone_code_hash)
        
        # Успешно
        await client.disconnect()
        db_set_session_status(user_id, True)
        
        # Запускаем Worker в фоновом режиме
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
        
    except SessionPasswordNeededError:
        # Требуется 2FA
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔒 **Включена двухфакторная аутентификация.**\nВведите пароль 2FA.", reply_markup=get_cancel_keyboard())
    except PhoneCodeExpiredError:
        # Ошибка, которую вы спрашивали
        await message.answer(
            "❌ **Ошибка:** Срок действия кода истек. Попробуйте снова.", 
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📱 Ввести номер заново", callback_data="auth_method_phone")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
            ])
        )
        await state.set_state(TelethonAuth.CHOOSE_AUTH_METHOD) # Возвращаем на выбор
    except PhoneCodeInvalidError:
        await message.answer("❌ **Ошибка:** Введен неверный код.", reply_markup=get_numeric_code_keyboard(data.get('auth_code_temp', "")))
    except Exception as e:
        error_text = f"❌ **Критическая ошибка:** Ошибка сервера. `{type(e).__name__}`"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()

@user_router.message(TelethonAuth.PASSWORD)
async def telethon_auth_step_password(message: Message, state: FSMContext):
    user_id = message.from_user.id
    password = message.text.strip()
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        await client.sign_in(password=password)

        await client.disconnect()

        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
        
    except Exception as e:
        error_text = f"❌ **Критическая ошибка:** Неверный пароль или ошибка сервера. `{type(e).__name__}`"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()
            
# --- Расширенные Инструменты Telethon (Скелеты) ---

@user_router.callback_query(F.data == "show_telethon_tools")
async def show_telethon_tools_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    # Проверка доступа 
    has_access, _ = await check_access(user_id, bot)
    if not has_access:
        await callback.answer("❌ Доступ ограничен. Сначала получите подписку или подпишитесь на канал.", show_alert=True)
        return
    
    if user_id not in ACTIVE_TELETHON_CLIENTS or not db_get_user(user_id).get('telethon_active'):
        await callback.answer("❌ Сначала авторизуйтесь и запустите Worker.", show_alert=True)
        return

    await callback.message.edit_text("🔥 **Расширенные Инструменты Telethon**\n\nВыберите команду для запуска:", 
                                     reply_markup=get_telethon_tools_kb())
    await callback.answer()

# --- FSM для Telethon команд (ИСПРАВЛЕНИЕ: StateFilter для объединения состояний) ---

@user_router.callback_query(F.data == "cmd_ls_start")
async def cmd_ls_start_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonCommands.waiting_ls_params)
    prompt = "💬 Введите параметры для **.лс** в формате:\n`[текст]` `[список @юзернеймов/ID через пробел]`\n\nНапример: `Привет @user1 -10012345678`"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.callback_query(F.data == "cmd_flood_start")
async def cmd_flood_start_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonCommands.waiting_flood_params)
    prompt = "💥 Введите параметры для **.флуд** в формате:\n`[кол-во]` `[текст]` `[задержка_сек]` `[чат @юзернейм/ID]`\n\nНапример: `100 Флуд-текст 0.5 @чат_для_флуда`"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.callback_query(F.data == "cmd_check_group_start")
async def cmd_check_group_start_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonCommands.waiting_check_params)
    prompt = "🔬 Введите параметр для **.чекгруппу**:\n`[чат @юзернейм/ID]`\n\nНапример: `@проверяемая_группа`"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.callback_query(F.data == "cmd_stop_flood")
async def cmd_stop_flood_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id in ACTIVE_TELETHON_TASKS:
        task_name = "flood" # Условное имя задачи
        if user_id in ACTIVE_TELETHON_TASKS and ACTIVE_TELETHON_TASKS[user_id]:
             ACTIVE_TELETHON_TASKS[user_id].cancel()
             del ACTIVE_TELETHON_TASKS[user_id]
             await callback.answer("🛑 Задача успешно отменена.", show_alert=True)
             await callback.message.edit_reply_markup(reply_markup=get_main_inline_kb(user_id, True))
        else:
            await callback.answer("⚠️ Нет активной задачи для остановки.", show_alert=True)
    else:
        await callback.answer("⚠️ Нет активной задачи для остановки.", show_alert=True)


@user_router.message(
    StateFilter(
        TelethonCommands.waiting_ls_params, 
        TelethonCommands.waiting_flood_params, 
        TelethonCommands.waiting_check_params
    )
)
async def process_telethon_command(message: Message, state: FSMContext):
    user_id = message.from_user.id
    current_state = await state.get_state()
    params = message.text.strip()
    
    cmd_map = {
        TelethonCommands.waiting_ls_params.state: ".лс",
        TelethonCommands.waiting_flood_params.state: ".флуд",
        TelethonCommands.waiting_check_params.state: ".чекгруппу"
    }
    
    cmd_name = cmd_map.get(current_state)

    if not cmd_name:
        await message.answer("Неизвестное состояние.")
        await state.clear()
        return

    client = ACTIVE_TELETHON_CLIENTS.get(user_id)
    if client and client.is_connected():
        # Отправка команды самому себе в ЛС (имитация ввода в Telethon-аккаунте)
        try:
            me = await client.get_me()
            # Отправляем команду самому себе в ЛС
            await client.send_message(me, f"{cmd_name} {params}")
            
            await message.answer(f"✅ Команда **{cmd_name}** с параметрами `{params}` отправлена Worker'у. Ожидайте выполнения.", 
                                 reply_markup=get_cancel_task_kb(cmd_name))
        except Exception as e:
            await message.answer(f"❌ Не удалось отправить команду Worker'у. Ошибка: `{type(e).__name__}`", 
                                 reply_markup=get_main_inline_kb(user_id, True))
    else:
        await message.answer("❌ Telethon Worker не запущен.", reply_markup=get_main_inline_kb(user_id, True))

    await state.clear()
    
    

### 8. Функционал Мониторинга и Отчетов 

@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    has_access, _ = await check_access(user_id, bot)
    if not has_access:
        await callback.answer("❌ Доступ ограничен. Сначала получите подписку или подпишитесь на канал.", show_alert=True)
        return

    user_data = db_get_user(user_id)
    it_chat = user_data.get('it_chat_id') or "Не установлен"
    drop_chat = user_data.get('drop_chat_id') or "Не установлен"
    
    text = (
        "📊 **Настройка Мониторинга**\n\n"
        f"Текущие чаты:\n"
        f"• IT-чат: `{it_chat}`\n"
        f"• DROP-чат: `{drop_chat}`\n\n"
        "Выберите, что настроить или какой отчет сгенерировать."
    )
    await callback.message.edit_text(text, reply_markup=get_monitor_menu_kb())
    await callback.answer()

@user_router.callback_query(F.data == "monitor_set_it")
async def monitor_set_it_start(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MonitorStates.waiting_for_it_chat_id)
    prompt = "⚙️ Введите **ID или Username** IT-чата, в котором бот должен слушать команды (например, `@IT_chat` или `-10012345678`):"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_it_chat_id)
async def monitor_set_it_end(message: Message, state: FSMContext):
    chat_id = message.text.strip()
    db_set_chat_id(message.from_user.id, 'IT', chat_id)
    await message.answer(f"✅ IT-чат установлен: `{chat_id}`.", reply_markup=get_main_inline_kb(message.from_user.id, True))
    await state.clear()

@user_router.callback_query(F.data == "monitor_set_drop")
async def monitor_set_drop_start(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MonitorStates.waiting_for_drop_chat_id)
    prompt = "⚙️ Введите **ID или Username** DROP-чата, в котором бот должен слушать заявки (например, `@DROP_chat` или `-10012345678`):"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_drop_chat_id)
async def monitor_set_drop_end(message: Message, state: FSMContext):
    chat_id = message.text.strip()
    db_set_chat_id(message.from_user.id, 'DROP', chat_id)
    await message.answer(f"✅ DROP-чат установлен: `{chat_id}`.", reply_markup=get_main_inline_kb(message.from_user.id, True))
    await state.clear()


@user_router.callback_query(F.data == "monitor_generate_report_start")
async def report_select_target(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(ReportStates.waiting_report_target)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="IT (Команды)", callback_data="report_target_IT")],
        [InlineKeyboardButton(text="DROP (Входы)", callback_data="report_target_DROP")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ])
    
    await callback.message.edit_text("📋 Выберите **тип** отчета:", reply_markup=kb)
    await callback.answer()


@user_router.callback_query(F.data.startswith("report_target_"), ReportStates.waiting_report_target)
async def report_select_days(callback: types.CallbackQuery, state: FSMContext):
    log_type = callback.data.split('_')[-1]
    await state.update_data(log_type=log_type)
    await state.set_state(ReportStates.waiting_report_topic) 
    
    prompt = "Введите **количество дней** (напр., `7`) для отчета или `0` для всех логов:"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()


@user_router.message(ReportStates.waiting_report_topic)
async def report_select_send_chat(message: Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        if days < 0: raise ValueError
    except ValueError:
        await message.answer("❌ Неверный формат. Введите целое число дней (>= 0).", reply_markup=get_cancel_keyboard())
        return
        
    await state.update_data(days=days)
    await state.set_state(ReportStates.waiting_report_send_chat)
    
    prompt = (
        "✉️ Введите **ID** или **Username** чата/пользователя, куда отправить отчет.\n"
        "Например: `@my_channel` или `6256576302` (ваш ID для ЛС)."
    )
    await message.answer(prompt, reply_markup=get_cancel_keyboard())


@user_router.message(ReportStates.waiting_report_send_chat)
async def report_process_send_chat_and_send(message: Message, state: FSMContext):
    user_id = message.from_user.id
    target_chat_input = message.text.strip()
    data = await state.get_data()
    log_type = data['log_type']
    days = data['days']
    
    status_msg = await message.answer(f"⏳ Генерирую отчет типа **{log_type}** за {days} дней...")
    
    logs = db_get_monitor_logs(user_id, log_type, days)
    
    if not logs:
        await status_msg.edit_text(f"⚠️ Логи типа **{log_type}** за последние {days} дней не найдены.", reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
        return

    # --- Формирование Отчета ---
    report_text = f"**📊 Отчет [{log_type}]**\n"
    report_text += f"**Пользователь:** `{user_id}`\n"
    report_text += f"**Период:** {'Последние ' + str(days) + ' дней' if days > 0 else 'Все доступные логи'}\n"
    report_text += f"**Всего записей:** {len(logs)}\n\n"
    
    for timestamp, command, target in logs:
        # Улучшенное форматирование для читаемости
        report_text += f"`[{timestamp}]` **{command}** ({target or 'N/A'})\n"
        
    chunks = [report_text[i:i + 4096] for i in range(0, len(report_text), 4096)]
    
    try:
        # Пытаемся отправить с учетом топиков (ID 1 - General)
        for chunk in chunks:
            try:
                await bot.send_message(target_chat_input, chunk, message_thread_id=1, parse_mode='Markdown')
            except TelegramBadRequest:
                # Если не сработало с топиком, отправляем как обычное сообщение
                await bot.send_message(target_chat_input, chunk, parse_mode='Markdown')

        # Очистка логов после успешной отправки
        cleared_count = db_clear_monitor_logs(user_id, log_type)
        
        await status_msg.edit_text(
            f"✅ Отчет типа **{log_type}** ({len(logs)} записей) отправлен в чат `{target_chat_input}`.\n"
            f"База данных очищена: **{cleared_count}** записей удалено.", 
            reply_markup=get_main_inline_kb(user_id, True)
        )
    except Exception as e:
        logger.error(f"Ошибка отправки отчета в {target_chat_input}: {e}")
        await status_msg.edit_text(
            f"❌ Не удалось отправить отчет в чат `{target_chat_input}` (Ошибка: `{type(e).__name__}`). Отчет отправлен вам в ЛС (без очистки БД).", 
            reply_markup=get_main_inline_kb(user_id, True)
        )
        for chunk in chunks:
            await message.answer(chunk, parse_mode='Markdown')

    await state.clear()


### 9. Функционал Промокодов и Админки 

@user_router.callback_query(F.data == "start_promo_fsm")
async def start_promo_fsm_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await callback.message.edit_text("🔑 Введите **Промокод**:", reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.message(PromoStates.waiting_for_code)
async def process_promo_code(message: Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip().upper()
    
    promo_data = db_get_promo_code(code)
    
    if not promo_data or not promo_data['is_active']:
        await message.answer(f"❌ Промокод `{code}` не найден или недействителен.", reply_markup=get_main_inline_kb(user_id, False))
    elif promo_data['max_uses'] is not None and promo_data['current_uses'] >= promo_data['max_uses']:
        await message.answer(f"❌ Промокод `{code}` исчерпал лимит использований.", reply_markup=get_main_inline_kb(user_id, False))
    else:
        # Активация
        db_update_subscription(user_id, promo_data['days'])
        db_use_promo_code(code)
        
        days = promo_data['days']
        end_date = db_get_user(user_id)['subscription_end_date']
        
        await message.answer(
            f"🎉 **Промокод `{code}` активирован!**\n"
            f"Получено **{days}** дней подписки.\n"
            f"Новый срок действия: `{end_date}`.", 
            reply_markup=get_main_inline_kb(user_id, True)
        )

    await state.clear()


# --- Админ-Панель ---

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_panel_start_handler(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен.", show_alert=True)
        return
    
    await state.set_state(AdminStates.main_menu)
    await callback.message.edit_text("👑 **Админ-Панель**\n\nВыберите действие:", reply_markup=get_admin_main_kb())
    await callback.answer()
    
# --- Создание Промокода (FSM) ---

@user_router.callback_query(F.data == "admin_create_promo_start", AdminStates.main_menu)
async def admin_create_promo_start(callback: types.CallbackQuery, state: FSMContext):
    code = generate_promo_code()
    await state.update_data(promo_code=code)
    await state.set_state(AdminStates.creating_promo_days)
    
    await callback.message.edit_text(
        f"🗝️ Генерируем промокод: `{code}`\n\n"
        "1/3: Введите **количество дней** подписки, которое даст этот промокод (целое число > 0):", 
        reply_markup=get_cancel_keyboard()
    )

@user_router.message(AdminStates.creating_promo_days)
async def admin_create_promo_get_days(message: Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError
        
        await state.update_data(promo_days=days)
        await state.set_state(AdminStates.creating_promo_uses)
        await message.answer(
            "2/3: Введите **лимит использований** (целое число > 0) или `0` для неограниченного использования:", 
            reply_markup=get_cancel_keyboard()
        )
    except ValueError:
        await message.answer("❌ Неверный формат. Введите положительное целое число дней.", reply_markup=get_cancel_keyboard())

@user_router.message(AdminStates.creating_promo_uses)
async def admin_create_promo_complete(message: Message, state: FSMContext):
    try:
        max_uses_input = message.text.strip()
        max_uses = int(max_uses_input) if max_uses_input != '0' else None
        
        if max_uses is not None and max_uses <= 0: raise ValueError
        
        data = await state.get_data()
        code = data['promo_code']
        days = data['promo_days']
        
        db_add_promo_code(code, days, max_uses)
        
        uses_text = f"Лимит: **{max_uses}** раз" if max_uses is not None else "**Неограниченно**"
        
        final_message = (
            "🎉 **Промокод УСПЕШНО создан!**\n\n"
            f"Код: `{code}`\n"
            f"Дает: **{days}** дней подписки\n"
            f"{uses_text}"
        )
        
        await message.answer(final_message, reply_markup=get_admin_main_kb())
        await state.set_state(AdminStates.main_menu)
        
    except ValueError:
        await message.answer("❌ Неверный формат. Введите положительное целое число для лимита или `0`.", reply_markup=get_cancel_keyboard())
    except sqlite3.IntegrityError:
        await message.answer("❌ Ошибка: Промокод с таким именем уже существует (редко). Повторите попытку.", reply_markup=get_cancel_keyboard())
    except Exception as e:
        await message.answer(f"❌ Критическая ошибка при сохранении: `{type(e).__name__}`", reply_markup=get_cancel_keyboard())


# --- Выдача Подписки ---

@user_router.callback_query(F.data == "admin_issue_sub_start", AdminStates.main_menu)
async def admin_issue_sub_start(callback: types.CallbackQuery, state: FSMContext):
     await state.set_state(AdminStates.sub_target_user_id)
     await callback.message.edit_text("👤 Введите **ID пользователя**, которому выдать подписку:", reply_markup=get_cancel_keyboard())

@user_router.message(AdminStates.sub_target_user_id)
async def admin_issue_sub_get_id(message: Message, state: FSMContext):
    try:
        target_id = int(message.text.strip())
        await state.update_data(target_id=target_id)
        await state.set_state(AdminStates.sub_duration_days)
        await message.answer(f"⏳ Пользователь `{target_id}`. Введите **количество дней** подписки:", reply_markup=get_cancel_keyboard())
    except ValueError:
        await message.answer("❌ Неверный ID. Введите только цифры.", reply_markup=get_cancel_keyboard())

@user_router.message(AdminStates.sub_duration_days)
async def admin_issue_sub_complete(message: Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError
        
        data = await state.get_data()
        target_id = data['target_id']
        
        new_end_date = db_update_subscription(target_id, days)
        
        await message.answer(f"✅ Подписка на {days} дней для ID `{target_id}` успешно выдана.\nСрок действия: `{new_end_date}`", 
                             reply_markup=get_admin_main_kb())
        
        # Уведомление пользователя
        try:
             await bot.send_message(target_id, f"🎁 Ваша подписка продлена администратором на {days} дней! Срок действия: `{new_end_date}`")
        except:
             pass 

        await state.set_state(AdminStates.main_menu)
        
    except ValueError:
        await message.answer("❌ Неверное количество дней. Введите положительное число.", reply_markup=get_cancel_keyboard())
    except Exception as e:
        await message.answer(f"❌ Произошла ошибка при выдаче подписки: `{type(e).__name__}`", reply_markup=get_cancel_keyboard())


### 10. Финальный Запуск

async def main():
    logger.info("Запуск бота...")
    
    db_init()
    logger.info("База данных инициализирована.")

    dp.include_router(user_router)
    
    # Запуск Worker'ов, которые должны работать
    await start_all_active_telethon_workers()

    # Запуск polling Aiogram
    try:
        await dp.start_polling(bot)
    except Exception as e:
         # Ловим ошибку, если она произошла в start_polling (чаще всего это Unauthorized)
         logger.critical(f"Критическая ошибка в start_polling: {e}")
         if "Unauthorized" in str(e):
             logger.critical("Проблема: Unauthorized. Проверьте токен в main.py!")
             
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
    except Exception as e:
        # Ловим ошибку, если она произошла до start_polling
        logger.critical(f"Критическая ошибка в main: {e}")
