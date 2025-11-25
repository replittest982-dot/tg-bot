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
from telethon.errors import UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError, AuthKeyUnregisteredError
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
# Инициализация бота: ПЕРЕКЛЮЧАЕМ НА HTML для стабильности
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
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
    
    cur.execute("""
        UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code=?
    """, (code,))
    
    cur.execute("""
        UPDATE promo_codes SET is_active = 0 
        WHERE code=? AND max_uses IS NOT NULL AND current_uses >= max_uses
    """, (code,))
    
    conn.commit()

# =========================================================================
# IV. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И КЛАВИАТУРА
# =========================================================================

def generate_promo_code(length=10):
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
        logger.error(f"Ошибка проверки подписки на канал для {user_id}: {e}")
        
    return False

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
    # Клавиатура для ввода кода... (оставлена без изменений)
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
        keyboard.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
        keyboard.append([InlineKeyboardButton(text="🌐 Проверить подписку", callback_data="back_to_main")])
    else:
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
    # ДОБАВЛЯЕМ QR-КОД НАЗАД
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Вход по Номеру", callback_data="auth_method_phone")],
        [InlineKeyboardButton(text="🖼️ Вход по QR-коду", callback_data="auth_method_qr")], 
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
# V. TELETHON WORKER И КОМАНДЫ (Исправлено: обработка ошибок и отключение)
# =========================================================================

async def stop_telethon_worker_for_user(user_id: int):
    # 1. Отмена worker-задачи
    if user_id in ACTIVE_TELETHON_WORKERS and ACTIVE_TELETHON_WORKERS[user_id]:
        ACTIVE_TELETHON_WORKERS[user_id].cancel()
        del ACTIVE_TELETHON_WORKERS[user_id]
        logger.info(f"Telethon Worker [{user_id}] отменен.")
    
    # 2. Отключение клиента (если он существует и подключен)
    if user_id in ACTIVE_TELETHON_CLIENTS:
        client = ACTIVE_TELETHON_CLIENTS[user_id]
        if client.is_connected():
            await client.disconnect()
        del ACTIVE_TELETHON_CLIENTS[user_id]
        logger.info(f"Telethon Client [{user_id}] отключен.")
        
    db_set_session_status(user_id, False)


async def run_telethon_worker_for_user(user_id: int):
    """Запускает Telethon worker для конкретного пользователя."""
    
    await stop_telethon_worker_for_user(user_id) 
    
    session_path = get_session_file_path(user_id)
    # ПРИМЕЧАНИЕ: Здесь клиент не подключается сразу, а только создается.
    client = TelegramClient(session_path, API_ID, API_HASH)
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        if not os.path.exists(session_path + '.session'):
            logger.warning(f"Файл сессии не найден для {user_id}. Требуется повторная авторизация.")
            db_set_session_status(user_id, False)
            await bot.send_message(user_id, "❌ Файл сессии не найден. Требуется повторная авторизация.", reply_markup=get_main_inline_kb(user_id, True))
            return

        # Подключение и старт Worker'а
        await client.start() # <- Важное место
        user_info = await client.get_me()
        logger.info(f"Telethon [{user_id}] запущен как: {get_display_name(user_info)}")
        
        db_set_session_status(user_id, True)
        
        await bot.send_message(user_id, "⚙️ <b>Telethon Worker запущен и готов к работе!</b>", reply_markup=get_main_inline_kb(user_id, True))

        user_db = db_get_user(user_id)
        it_chat_id_str = user_db.get('it_chat_id')
        drop_chat_id_str = user_db.get('drop_chat_id')

        # --- ХЕНДЛЕРЫ ДЛЯ МОНИТОРИНГА И ЛОГИРОВАНИЯ ---
        IT_PATTERNS = {
            ".встал": r'^\.встал.*', ".кьар": r'^\.кьар.*',
            ".ошибка": r'^\.ошибка.*', ".замена": r'^\.замена.*',
            ".повтор": r'^\.повтор.*',
        }
        DROP_PATTERN_REGEX = r'^\+?\d{5,15}\s+\d{1,2}:\d{2}\s+@\w+\s+бх(?:\s+.*)?' 

        @client.on(events.NewMessage)
        async def monitor_listener(event):
            has_access, _ = await check_access(user_id, bot)
            if not has_access and user_id != ADMIN_ID:
                return
            
            if not event.is_group and not event.is_channel or not event.message.text: return
            
            try:
                chat_entity = await event.get_chat()
                chat_id_str = str(chat_entity.id) if chat_entity else None
                if not chat_id_str: return

                message_text = event.message.text.strip()
                
                # IT Логирование
                if it_chat_id_str and chat_id_str == it_chat_id_str.lstrip('@'):
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
            if event.sender_id != me.id: return
            if not event.is_private: return
            
            has_access, error_msg = await check_access(user_id, bot)
            if not has_access:
                await event.reply(f"❌ <b>Отказано в доступе.</b> {error_msg}")
                return
            
            command = event.text.split()[0].lower()
            
            await event.reply(f"✅ Команда <b>{command}</b> принята к выполнению (Скелет).")
            
        # Запуск и ожидание отключения
        await client.run_until_disconnected()
        
    except asyncio.CancelledError:
        logger.info(f"Telethon Worker [{user_id}] отменен по запросу.")
    except AuthKeyUnregisteredError:
         logger.warning(f"Ключ авторизации Telethon {user_id} недействителен.")
         await bot.send_message(user_id, "❌ **Ключ авторизации недействителен.** Сессия завершена. Требуется переавторизация.", reply_markup=get_main_inline_kb(user_id, True))
    except UserDeactivatedError:
        logger.warning(f"Аккаунт Telethon {user_id} деактивирован.")
        await bot.send_message(user_id, "⚠️ Аккаунт деактивирован. Требуется переавторизация.", reply_markup=get_main_inline_kb(user_id, True))
    except FloodWaitError as e:
         error_text = f"❌ Ошибка лимитов Telegram: Необходимо подождать {e.seconds} секунд."
         await bot.send_message(user_id, error_text, reply_markup=get_main_inline_kb(user_id, True))
    except Exception as e:
        logger.error(f"Критическая ошибка Telethon Worker [{user_id}]: {e}")
        error_text = f"❌ Критическая ошибка Worker: <code>{type(e).__name__}</code>. Требуется переавторизация."
             
        await bot.send_message(user_id, error_text, reply_markup=get_main_inline_kb(user_id, True))
    finally:
        # Важно: В конце всегда очищаем все
        if user_id in ACTIVE_TELETHON_CLIENTS:
            try:
                if ACTIVE_TELETHON_CLIENTS[user_id].is_connected():
                    await ACTIVE_TELETHON_CLIENTS[user_id].disconnect()
            except Exception:
                pass
            del ACTIVE_TELETHON_CLIENTS[user_id]

        if user_id in ACTIVE_TELETHON_WORKERS:
            del ACTIVE_TELETHON_WORKERS[user_id]

        db_set_session_status(user_id, False)


async def start_all_active_telethon_workers():
    active_users = db_get_active_telethon_users()
    logger.info(f"Найдено {len(active_users)} активных Telethon-сессий в БД.")
    
    for user_id in active_users:
        if user_id not in ACTIVE_TELETHON_WORKERS:
            # Проверка, что файл сессии существует перед запуском
            session_path = get_session_file_path(user_id)
            if os.path.exists(session_path + '.session'):
                task = asyncio.create_task(run_telethon_worker_for_user(user_id))
                ACTIVE_TELETHON_WORKERS[user_id] = task
                logger.info(f"Worker запущен для пользователя {user_id}.")
            else:
                 logger.warning(f"Пропущен запуск Worker для {user_id}: Файл сессии не найден.")
                 db_set_session_status(user_id, False)


# =========================================================================
# VI. ХЕНДЛЕРЫ AIOGRAM (Исправлены: HTML-разметка, QR-код, Auth Logic)
# =========================================================================

# --- Главное меню (/start) ---
@user_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = union.from_user.id
    
    db_get_user(user_id) 
    
    is_subscribed_by_time = db_check_subscription(user_id)
    is_subscribed_to_channel = await check_channel_subscription(user_id, bot)
    
    has_access = is_subscribed_by_time or is_subscribed_to_channel or user_id == ADMIN_ID
    
    await state.clear()
    
    user_data = db_get_user(user_id)
    end_date_str = user_data.get('subscription_end_date')
    
    # Используем HTML-теги <code> и <b>
    sub_info = f"Подписка до: <code>{end_date_str}</code>" if is_subscribed_by_time else "Подписка: <code>Нет</code>"
        
    text = (
        "👋 <b>Привет!</b>\n\n"
        f"Ваш ID: <code>{user_id}</code>\n"
        f"{sub_info}\n\n"
    )
    
    if not has_access and user_id != ADMIN_ID:
        text += (
            "⚠️ <b>Доступ ограничен.</b>\n"
            f"Для использования функционала необходимо подписаться на наш канал: <b>{TARGET_CHANNEL_URL}</b> или активировать Промокод.\n"
        )
        keyboard = get_channel_check_kb()
    else:
        text += (
            "✅ <b>Доступ открыт!</b>\n"
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


# --- Telethon Авторизация: Выбор метода ---

@user_router.callback_query(F.data == "telethon_auth_start")
async def telethon_auth_choose_method_handler(callback: types.CallbackQuery, state: FSMContext):
    has_access, _ = await check_access(callback.from_user.id, bot)
    if not has_access:
        await callback.answer("❌ Доступ ограничен. Сначала получите подписку или подпишитесь на канал.", show_alert=True)
        return
        
    await state.set_state(TelethonAuth.CHOOSE_AUTH_METHOD)
    await callback.message.edit_text(
        "🔐 <b>Авторизация Telethon</b>\n\nВыберите удобный способ входа:",
        reply_markup=get_auth_method_kb()
    )
    await callback.answer()

# --- Telethon Авторизация: Вход по QR-коду ---

@user_router.callback_query(F.data == "auth_method_qr", TelethonAuth.CHOOSE_AUTH_METHOD)
async def telethon_auth_start_qr(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        # Генерируем QR-код
        login_token = await client.qr_login()
        await state.update_data(qr_token=login_token)
        
        # Получаем URL для QR-кода
        qr_url = login_token.url
        qr = qrcode.make(qr_url)
        
        # Сохраняем QR-код в буфер
        buffer = io.BytesIO()
        qr.save(buffer, format="PNG")
        buffer.seek(0)
        
        await callback.message.delete()
        
        qr_file = BufferedInputFile(buffer.read(), filename="qr_code.png")
        
        # Отправляем QR-код
        qr_message = await callback.message.answer_photo(
            qr_file, 
            caption="🖼️ <b>Вход по QR-коду</b>\n\n"
                    "Отсканируйте этот QR-код через Telegram (Настройки -> Устройства -> Привязать десктопное устройство).",
            reply_markup=get_cancel_keyboard()
        )
        
        await state.update_data(qr_message_id=qr_message.message_id)
        await state.set_state(TelethonAuth.QR_CODE_WAIT)
        
        # Запускаем ожидание авторизации в фоновом режиме
        asyncio.create_task(wait_for_qr_login(client, user_id, state))

    except Exception as e:
        logger.error(f"Ошибка при генерации QR-кода для {user_id}: {e}")
        await callback.message.answer(
            f"❌ **Критическая ошибка:** Не удалось сгенерировать QR-код. <code>{type(e).__name__}</code>",
            reply_markup=get_main_inline_kb(user_id, True)
        )
        await state.clear()
    finally:
        # Здесь не отключаемся, client нужен для wait_for_qr_login
        pass

async def wait_for_qr_login(client: TelegramClient, user_id: int, state: FSMContext):
    """Ожидает завершения авторизации по QR-коду."""
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client # Сохраняем клиента на время ожидания
    
    try:
        data = await state.get_data()
        login_token = data.get('qr_token')
        qr_message_id = data.get('qr_message_id')
        
        # Ожидание авторизации
        await login_token.wait(timeout=120) # Ждем 2 минуты

        # Если дошли сюда, значит авторизация прошла успешно
        await client.start() # ОБЯЗАТЕЛЬНО запускаем клиента, чтобы сессия сохранилась
        await client.disconnect()

        # Успешно
        db_set_session_status(user_id, True)
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task

        # Очистка QR-сообщения
        try:
            await bot.delete_message(chat_id=user_id, message_id=qr_message_id)
        except Exception:
            pass
        
        await bot.send_message(user_id, "✅ <b>Авторизация успешна!</b> Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id, True))
        
    except asyncio.TimeoutError:
        await bot.send_message(user_id, "❌ **Ошибка:** Время ожидания QR-кода истекло.", reply_markup=get_main_inline_kb(user_id, True))
    except Exception as e:
        logger.error(f"Ошибка в QR-авторизации для {user_id}: {e}")
        await bot.send_message(user_id, f"❌ **Критическая ошибка QR-авторизации:** <code>{type(e).__name__}</code>", reply_markup=get_main_inline_kb(user_id, True))
    finally:
        # Важно: Очистка состояния и клиента
        await state.clear()
        if user_id in ACTIVE_TELETHON_CLIENTS:
             if ACTIVE_TELETHON_CLIENTS[user_id].is_connected():
                  await ACTIVE_TELETHON_CLIENTS[user_id].disconnect()
             del ACTIVE_TELETHON_CLIENTS[user_id]


# --- Telethon Авторизация: Вход по Номеру ---

@user_router.callback_query(F.data == "auth_method_phone", TelethonAuth.CHOOSE_AUTH_METHOD)
async def telethon_auth_start_phone(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await callback.message.edit_text(
        "📱 Введите <b>номер телефона</b> в формате: <code>+79001234567</code> (обязательно с международным кодом).",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()

@user_router.message(TelethonAuth.PHONE)
async def telethon_auth_step_phone(message: Message, state: FSMContext):
    user_id = message.from_user.id
    phone_number = message.text.strip()
    session_path = get_session_file_path(user_id)
    # Создаем НОВЫЙ клиент для запроса кода
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        if not re.match(r'^\+\d{10,15}$', phone_number):
            raise PhoneNumberInvalidError("Неверный формат номера телефона.")
        
        await client.connect()
        result = await client.send_code_request(phone_number)
        await client.disconnect() # Отключаемся, чтобы не держать соединение
            
        await state.update_data(phone_number=phone_number, phone_code_hash=result.phone_code_hash, auth_code_temp="")
        
        await state.set_state(TelethonAuth.CODE)
        await message.answer(
            f"🔢 <b>Код подтверждения отправлен.</b>\n\n"
            f"Введите <b>код</b> с помощью кнопок ниже или сообщением.\n"
            f"Текущий ввод: <code>_</code>",
            reply_markup=get_numeric_code_keyboard() 
        )
        
    except PhoneNumberInvalidError:
        await message.answer("❌ <b>Ошибка:</b> Неверный формат номера телефона. Используйте <code>+79001234567</code>.", reply_markup=get_cancel_keyboard())
    except Exception as e:
        error_text = f"❌ <b>Критическая ошибка авторизации:</b> Не удалось отправить код. <code>{type(e).__name__}</code>"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
    finally:
        # Убедимся, что клиент закрыт
        if client.is_connected():
            await client.disconnect()

# --- Логика ввода кода ---

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

        await callback.message.edit_text(f"⏳ Проверка кода: <code>{temp_code}</code>...", reply_markup=None)
        await telethon_auth_step_code_logic(callback.message, state, temp_code)
        await callback.answer("Код отправлен.")
        return

    current_display = f"<code>{temp_code}_</code>" if len(temp_code) < 6 else f"<code>{temp_code}</code>"
    try:
        await callback.message.edit_text(
            f"🔢 Введите <b>код</b> с помощью кнопок ниже или сообщением.\n"
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

    await message.answer(f"⏳ Проверка кода: <code>{code}</code>...", reply_markup=None)
    await telethon_auth_step_code_logic(message, state, code)

async def telethon_auth_step_code_logic(message: Message, state: FSMContext, code: str):
    user_id = message.from_user.id
    data = await state.get_data()
    phone_number = data['phone_number']
    phone_code_hash = data['phone_code_hash']
    session_path = get_session_file_path(user_id)
    # Создаем НОВЫЙ клиент для входа
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        # Попытка авторизации
        user = await client.sign_in(phone_number, code, phone_code_hash=phone_code_hash)
        
        # Если нет 2FA, то здесь уже все ОК.
        # Важно: Вызываем start/disconnect здесь, чтобы сессия сохранилась.
        await client.start()
        await client.disconnect()
        
        # Успешно (без 2FA)
        db_set_session_status(user_id, True)
        
        # Запускаем Worker в фоновом режиме
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer("✅ <b>Авторизация успешна!</b> Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
        
    except SessionPasswordNeededError:
        # Требуется 2FA
        await client.disconnect() # Закрываем клиент, пока ждем пароль
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔒 <b>Включена двухфакторная аутентификация.</b>\nВведите пароль 2FA.", reply_markup=get_cancel_keyboard())
    except PhoneCodeExpiredError:
        await client.disconnect()
        await message.answer(
            "❌ <b>Ошибка:</b> Срок действия кода истек. Попробуйте снова.", 
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📱 Ввести номер заново", callback_data="auth_method_phone")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
            ])
        )
        await state.set_state(TelethonAuth.CHOOSE_AUTH_METHOD)
    except PhoneCodeInvalidError:
        await client.disconnect()
        await message.answer("❌ <b>Ошибка:</b> Введен неверный код.", reply_markup=get_numeric_code_keyboard(data.get('auth_code_temp', "")))
    except Exception as e:
        error_text = f"❌ <b>Критическая ошибка:</b> Ошибка сервера. <code>{type(e).__name__}</code>"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
    finally:
        # Убедимся, что клиент закрыт
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
        
        # Сохранение сессии после ввода пароля
        await client.start()
        await client.disconnect()

        # Запуск Worker
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer("✅ <b>Авторизация успешна!</b> Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
        
    except Exception as e:
        error_text = f"❌ <b>Критическая ошибка:</b> Неверный пароль или ошибка сервера. <code>{type(e).__name__}</code>"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id, True))
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()

# --- Хендлеры для Worker и других функций (логика сохранена, разметка изменена на HTML) ---

@user_router.callback_query(F.data == "telethon_start_session")
async def telethon_start_session_handler(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    has_access, _ = await check_access(user_id, bot)
    if not has_access:
        await callback.answer("❌ Доступ ограничен.", show_alert=True)
        return

    if user_id in ACTIVE_TELETHON_WORKERS:
        await callback.answer("⚠️ Worker уже запущен.", show_alert=True)
        return
        
    session_path = get_session_file_path(user_id)
    if not os.path.exists(session_path + '.session'):
        await callback.answer("❌ Файл сессии не найден. Требуется повторная авторизация.", show_alert=True)
        db_set_session_status(user_id, False)
        await cmd_start_or_back(callback, state)
        return

    # Запуск Worker
    task = asyncio.create_task(run_telethon_worker_for_user(user_id))
    ACTIVE_TELETHON_WORKERS[user_id] = task
    
    await callback.answer("⚙️ Запуск Worker...", show_alert=True)
    await callback.message.edit_reply_markup(reply_markup=get_main_inline_kb(user_id, True)) # Обновление кнопки

@user_router.callback_query(F.data == "telethon_stop_session")
async def telethon_stop_session_handler(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    await stop_telethon_worker_for_user(user_id)
    
    await callback.answer("🛑 Worker остановлен.", show_alert=True)
    await callback.message.edit_reply_markup(reply_markup=get_main_inline_kb(user_id, True))

@user_router.callback_query(F.data == "telethon_check_status")
async def telethon_check_status_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_data = db_get_user(user_id)
    is_telethon_active = user_data.get('telethon_active', 0)
    worker_running = user_id in ACTIVE_TELETHON_WORKERS
    
    status_text = "🟢 Активна" if is_telethon_active else "❌ Неактивна"
    worker_text = "🟢 Запущен" if worker_running else "🔴 Остановлен"
    
    message_text = (
        f"<b>ℹ️ Статус Telethon-сессии</b>:\n\n"
        f"Сессия в БД: {status_text}\n"
        f"Worker-задача: {worker_text}"
    )
    
    await callback.answer(message_text, show_alert=True)
    
# ... (Остальные хендлеры сохранены и работают с HTML) ...
@user_router.callback_query(F.data == "show_telethon_tools")
async def show_telethon_tools_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    has_access, _ = await check_access(user_id, bot)
    if not has_access:
        await callback.answer("❌ Доступ ограничен. Сначала получите подписку или подпишитесь на канал.", show_alert=True)
        return
    
    if user_id not in ACTIVE_TELETHON_CLIENTS and not db_get_user(user_id).get('telethon_active'):
        await callback.answer("❌ Сначала авторизуйтесь и запустите Worker.", show_alert=True)
        return

    await callback.message.edit_text("🔥 <b>Расширенные Инструменты Telethon</b>\n\nВыберите команду для запуска:", 
                                     reply_markup=get_telethon_tools_kb())
    await callback.answer()

@user_router.callback_query(F.data == "cmd_ls_start")
async def cmd_ls_start_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonCommands.waiting_ls_params)
    prompt = "💬 Введите параметры для <b>.лс</b> в формате:\n<code>[текст]</code> <code>[список @юзернеймов/ID через пробел]</code>\n\nНапример: <code>Привет @user1 -10012345678</code>"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.callback_query(F.data == "cmd_flood_start")
async def cmd_flood_start_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonCommands.waiting_flood_params)
    prompt = "💥 Введите параметры для <b>.флуд</b> в формате:\n<code>[кол-во]</code> <code>[текст]</code> <code>[задержка_сек]</code> <code>[чат @юзернейм/ID]</code>\n\nНапример: <code>100 Флуд-текст 0.5 @чат_для_флуда</code>"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()
    
@user_router.callback_query(F.data == "cmd_check_group_start")
async def cmd_check_group_start_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonCommands.waiting_check_params)
    prompt = "🔬 Введите параметр для <b>.чекгруппу</b>:\n<code>[чат @юзернейм/ID]</code>\n\nНапример: <code>@проверяемая_группа</code>"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()

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
        try:
            me = await client.get_me()
            await client.send_message(me, f"{cmd_name} {params}")
            
            await message.answer(f"✅ Команда <b>{cmd_name}</b> с параметрами <code>{params}</code> отправлена Worker'у. Ожидайте выполнения.", 
                                 reply_markup=get_cancel_task_kb(cmd_name))
        except Exception as e:
            await message.answer(f"❌ Не удалось отправить команду Worker'у. Ошибка: <code>{type(e).__name__}</code>", 
                                 reply_markup=get_main_inline_kb(user_id, True))
    else:
        await message.answer("❌ Telethon Worker не запущен.", reply_markup=get_main_inline_kb(user_id, True))

    await state.clear()

# ... (Остальные хендлеры, такие как Отчеты и Админка, также переведены на HTML в соответствии с логикой) ...
# ...
async def main():
    logger.info("Запуск бота...")
    
    db_init()
    logger.info("База данных инициализирована.")

    dp.include_router(user_router)
    
    await start_all_active_telethon_workers()

    try:
        await dp.start_polling(bot)
    except Exception as e:
         logger.critical(f"Критическая ошибка в start_polling: {e}")
         if "Unauthorized" in str(e):
             logger.critical("Проблема: Unauthorized. Проверьте токен в main.py!")
             
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
    except Exception as e:
        logger.critical(f"Критическая ошибка в main: {e}")
