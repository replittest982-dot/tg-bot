import asyncio
import logging
import os
import sqlite3
import pytz
import time
import re
import secrets
import io 
from datetime import datetime, timedelta

# Импорты aiogram
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, Message, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
import qrcode # Для QR-кода

# Импорты telethon
from telethon import TelegramClient, events
from telethon.errors import UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError
from telethon.utils import get_display_name

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# !!! ОБЯЗАТЕЛЬНО ЗАМЕНИТЕ ВАШИ КЛЮЧИ !!!
BOT_TOKEN = "7868097991:AAFQtLSv6nlS5PmGH4TMsgV03dxs_X7iZf8"
ADMIN_ID = 6256576302 # Ваш ID для доступа к Админ-Панели
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
ACTIVE_LONG_TASKS = {} # Формат: {user_id: {task_id: {'task': asyncio.Task, 'message_id': int}}} 

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='Markdown')) 
dp = Dispatcher(storage=storage)
user_router = Router()

# =========================================================================
# II. FSM-СОСТОЯНИЯ
# =========================================================================

class TelethonAuth(StatesGroup):
    """Состояния для процесса авторизации Telethon, включая QR-код."""
    CHOOSE_AUTH_METHOD = State()
    PHONE = State()
    QR_CODE_WAIT = State()
    CODE = State()
    PASSWORD = State()

class PromoStates(StatesGroup):
    """Состояния для активации промокода пользователем."""
    waiting_for_code = State()
    processing_code = State()

class AdminStates(StatesGroup):
    """Состояния для Админ-панели."""
    main_menu = State()
    # Реализация промокодов
    creating_promo_days = State()
    creating_promo_uses = State()
    # Скелет для выдачи подписки
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

def db_get_user(user_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
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
        if not end_date_str:
             return False
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
    except Exception:
        return False
    return end_date > datetime.now()

def db_set_session_status(user_id: int, is_active: bool, hash_code: str = None):
    """Устанавливает статус Telethon-сессии и ГАРАНТИРУЕТ СУЩЕСТВОВАНИЕ записи о пользователе."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Гарантируем, что пользователь существует в таблице users
    cur.execute("""
        INSERT OR IGNORE INTO users (user_id, subscription_active, telethon_active) 
        VALUES (?, 0, 0)
    """, (user_id,))
    
    cur.execute("""
        UPDATE users SET telethon_active=?, telethon_hash=? WHERE user_id=?
    """, (1 if is_active else 0, hash_code, user_id))
    conn.commit()

def db_clear_monitor_logs(user_id, log_type):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM monitor_logs WHERE user_id=? AND type=?", (user_id, log_type))
    conn.commit()

def db_get_monitor_logs(user_id, log_type, since_days: int = None):
    conn = get_db_connection()
    cur = conn.cursor()
    query = "SELECT timestamp, command, target FROM monitor_logs WHERE user_id=? AND type=? "
    params = [user_id, log_type]
    
    if since_days is not None and since_days > 0:
        cutoff_date = (datetime.now() - timedelta(days=since_days)).strftime('%Y-%m-%d %H:%M:%S')
        query += "AND timestamp >= ? "
        params.append(cutoff_date)

    query += "ORDER BY timestamp"
    cur.execute(query, params)
    return cur.fetchall()

def db_add_monitor_log(user_id, log_type, command, target):
    conn = get_db_connection()
    cur = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("INSERT INTO monitor_logs (user_id, timestamp, type, command, target) VALUES (?, ?, ?, ?, ?)",
                (user_id, timestamp, log_type, command, target))
    conn.commit()

def db_get_active_telethon_users():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
    return [row[0] for row in cur.fetchall()]

def db_check_and_deactivate_subscriptions():
    conn = get_db_connection()
    cur = conn.cursor()
    
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    cur.execute("""
        SELECT user_id FROM users 
        WHERE subscription_active=1 AND subscription_end_date < ?
    """, (now_str,))
    
    expired_users = [row[0] for row in cur.fetchall()]
    
    if expired_users:
        cur.execute("""
            UPDATE users SET subscription_active=0, subscription_end_date=NULL
            WHERE subscription_active=1 AND subscription_end_date < ?
        """, (now_str,))
        conn.commit()
        logger.info(f"Деактивировано {len(expired_users)} просроченных подписок.")
        return expired_users
    return []


# =========================================================================
# IV. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И КЛАВИАТУРА
# =========================================================================

def get_session_file_path(user_id: int):
    os.makedirs('data', exist_ok=True)
    return os.path.join('data', f'session_{user_id}')

async def check_access(user_id: int, bot: Bot) -> tuple[bool, str]:
    """Проверяет доступ пользователя (админ, подписка, канал)."""
    if user_id == ADMIN_ID:
        return True, ""
    
    user = db_get_user(user_id)
    if not user:
        db_set_session_status(user_id, False) 
        user = db_get_user(user_id)

    # 1. Проверка активной подписки по сроку
    subscribed_by_time = db_check_subscription(user_id)
    if subscribed_by_time:
        return True, ""
    
    # 2. Проверка подписки на канал (TARGET_CHANNEL_URL)
    try:
        member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id) 
        if member.status in ["member", "administrator", "creator"]:
             return True, ""
    except Exception as e:
        logger.error(f"Ошибка проверки подписки на канал для {user_id}: {e}")
        
    return False, f"❌ Для использования бота необходима активная подписка или подписка на канал {TARGET_CHANNEL_URL}. Подпишитесь и нажмите /start снова."

def get_cancel_keyboard():
    """Возвращает клавиатуру с кнопкой 'Отмена'."""
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
        ]
    )

def get_progress_keyboard(task_id):
    """Клавиатура с кнопкой отмены для долгой задачи."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛑 Остановить Задачу", callback_data=f"stop_long_task_{task_id}")]
    ])

def get_numeric_code_keyboard(current_code=""):
    """Возвращает Inline-клавиатуру для удобного ввода 4/5-значного кода (1️⃣2️⃣3️⃣)."""
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
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action"),
            InlineKeyboardButton(text="0️⃣", callback_data="auth_digit_0"),
            InlineKeyboardButton(text="✅ Ввод", callback_data="auth_submit_code"), 
        ],
        [InlineKeyboardButton(text="⬅️ Удалить", callback_data="auth_delete_digit")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


def get_main_inline_kb(user_id: int) -> InlineKeyboardMarkup:
    """Генерирует главную инлайн-клавиатуру с обновленным названием кнопки авторизации."""
    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    
    kb = [
        [InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")],
        [InlineKeyboardButton(text="📊 Отчеты и Мониторинг", callback_data="show_monitor_menu")],
    ]
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")]) 
        
    auth_text = "🟢 Сессия активна" if session_active else "🔐 Авторизация"
    auth_callback = "telethon_auth_status" if session_active else "telethon_auth_start"
    
    if session_active:
        kb.append([
            InlineKeyboardButton(text="🛑 Остановить Сессию", callback_data="telethon_stop_session"),
            InlineKeyboardButton(text="ℹ️ Статус Аккаунта", callback_data="telethon_check_status")
        ])
    else:
         kb.append([InlineKeyboardButton(text=auth_text, callback_data=auth_callback)])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)

# Клавиатура для выбора метода авторизации
def get_auth_method_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Вход по Номеру", callback_data="auth_method_phone")],
        [InlineKeyboardButton(text="🖼️ Вход по QR-коду", callback_data="auth_method_qr")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ])

def get_monitor_menu_kb() -> InlineKeyboardMarkup:
    """Клавиатура меню мониторинга."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настроить IT-Чат", callback_data="monitor_set_it")],
        [InlineKeyboardButton(text="⚙️ Настроить DROP-Чат", callback_data="monitor_set_drop")],
        [InlineKeyboardButton(text="📋 Сгенерировать Отчет", callback_data="monitor_generate_report_start")], 
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_admin_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать Промокод", callback_data="admin_create_promo_start")],
        [InlineKeyboardButton(text="➡️ Выдать Подписку (Скелет)", callback_data="admin_issue_sub_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])


# =========================================================================
# V. TELETHON WORKER И КОМАНДЫ
# =========================================================================

async def stop_telethon_worker_for_user(user_id: int):
    """Останавливает Telethon worker и очищает ресурсы."""
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
    
    await stop_telethon_worker_for_user(user_id) 
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        if not os.path.exists(session_path + '.session'):
            logger.warning(f"Файл сессии не найден для {user_id}. Требуется повторная авторизация.")
            db_set_session_status(user_id, False)
            await bot.send_message(user_id, "❌ Файл сессии не найден. Требуется повторная авторизация.", reply_markup=get_main_inline_kb(user_id))
            return

        await client.start()
        user_info = await client.get_me()
        logger.info(f"Telethon [{user_id}] запущен как: {get_display_name(user_info)}")
        
        db_set_session_status(user_id, True)
        
        await bot.send_message(user_id, "⚙️ **Telethon Worker запущен и готов к работе!**", reply_markup=get_main_inline_kb(user_id))

        user_db = db_get_user(user_id)
        it_chat_id_str = user_db.get('it_chat_id')
        drop_chat_id_str = user_db.get('drop_chat_id')

        # --- ХЕНДЛЕРЫ ДЛЯ МОНИТОРИНГА И ЛОГИРОВАНИЯ ---
        IT_PATTERNS = {
            ".встал": r'^\.встал.*', ".кьар": r'^\.кьар.*',
            ".ошибка": r'^\.ошибка.*', ".замена": r'^\.замена.*',
            ".повтор": r'^\.повтор.*',
        }
        DROP_PATTERN_REGEX = r'^\+?\d{10,15}\s+\d{1,2}:\d{2}\s+@\w+\s+бх(?:\s+\d{1,2}:\d{2})?.*'

        @client.on(events.NewMessage)
        async def monitor_listener(event):
            # Проверяем, есть ли доступ у пользователя для обработки команд
            has_access, _ = await check_access(user_id, bot)
            if not has_access and user_id != ADMIN_ID:
                # Если доступа нет, пропускаем обработку команд и мониторинг
                return
            
            if not event.is_group and not event.is_channel or not event.message.text: return
            
            try:
                chat_id_str = str(event.chat_id) 
                message_text = event.message.text.strip()
                
                # IT Логирование
                if it_chat_id_str and chat_id_str == it_chat_id_str:
                    for command, regex in IT_PATTERNS.items():
                        if re.match(regex, message_text, re.IGNORECASE | re.DOTALL):
                            db_add_monitor_log(user_id, 'IT', command, message_text)
                            break
                
                # DROP Логирование
                if drop_chat_id_str and chat_id_str == drop_chat_id_str:
                    if re.match(DROP_PATTERN_REGEX, message_text, re.IGNORECASE | re.DOTALL):
                         db_add_monitor_log(user_id, 'DROP', 'DROP_ENTRY', message_text)

            except Exception as e:
                logger.error(f"Ошибка в мониторинге Telethon для {user_id}: {e}")
                
        
        # --- ХЕНДЛЕРЫ ДЛЯ КОМАНД В ЛС TELETHON-АККАУНТА ---
        @client.on(events.NewMessage(pattern=r'^\.(лс|флуд|стопфлуд|чекгруппу).*'))
        async def telethon_command_handler(event):
            
            me = await client.get_me()
            if event.sender_id != me.id: return
            if not event.is_private: return
            
            # Проверяем доступ для выполнения команд
            has_access, error_msg = await check_access(user_id, bot)
            if not has_access:
                await event.reply(f"❌ **Отказано в доступе.** {error_msg}")
                return
            
            command = event.text.split()[0].lower()
            
            # ... (логика команд .флуд, .чекгруппу, .стопфлуд, .лс - без изменений) ...
            # Оставлю только заглушку, чтобы код был короче
            await event.reply(f"✅ Команда **{command}** принята к выполнению (Скелет).")
            
        await client.run_until_disconnected()
        
    except asyncio.CancelledError:
        logger.info(f"Telethon Worker [{user_id}] отменен по запросу.")
    except UserDeactivatedError:
        logger.warning(f"Аккаунт Telethon {user_id} деактивирован.")
        await bot.send_message(user_id, "⚠️ Аккаунт деактивирован. Требуется переавторизация.", reply_markup=get_main_inline_kb(user_id))
    except FloodWaitError as e:
         error_text = f"❌ Ошибка лимитов Telegram: Необходимо подождать {e.seconds} секунд."
         await bot.send_message(user_id, error_text, reply_markup=get_main_inline_kb(user_id))
    except Exception as e:
        logger.error(f"Критическая ошибка Telethon Worker [{user_id}]: {e}")
        error_text = f"❌ Критическая ошибка Telethon Worker: `{type(e).__name__}`. Требуется переавторизация."
        if "AuthorizationKeyUnregistered" in str(e):
             error_text = "❌ Ключ авторизации недействителен. Сессия завершена. Требуется переавторизация."
             
        await bot.send_message(user_id, error_text, reply_markup=get_main_inline_kb(user_id))
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS:
            del ACTIVE_TELETHON_CLIENTS[user_id]
        if user_id in ACTIVE_TELETHON_WORKERS:
            del ACTIVE_TELETHON_WORKERS[user_id]
        db_set_session_status(user_id, False)


async def start_all_active_telethon_workers():
    """Запускает worker для всех пользователей, отмеченных как 'active' в БД."""
    active_users = db_get_active_telethon_users()
    logger.info(f"Найдено {len(active_users)} активных Telethon-сессий в БД.")
    
    for user_id in active_users:
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        logger.info(f"Worker запущен для пользователя {user_id}.")

# =========================================================================
# VI. ХЕНДЛЕРЫ AIOGRAM
# =========================================================================

# --- Обработчик Отмены и Долгих Задач ---
@user_router.callback_query(F.data == "cancel_action")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.edit_text("❌ Действие отменено.", reply_markup=None)
    except TelegramBadRequest:
        await callback.message.answer("❌ Действие отменено.", reply_markup=None)
    await cmd_start_or_back(callback, state)
    await callback.answer("Отменено.")

# --- Главное меню ---
@user_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = union.from_user.id
    
    db_set_session_status(user_id, False) 
    has_access, error_msg = await check_access(user_id, bot)
    
    keyboard = get_main_inline_kb(user_id)
    
    if has_access or user_id == ADMIN_ID:
        text = (
            "👋 **Добро пожаловать в STAT-PRO Bot!**\n\n"
            "Ваш ID: `{user_id}`\n"
            "Этот бот — ваш универсальный инструмент для автоматизации работы с Telegram-аккаунтом и сбора логов.\n\n"
            "Выберите опцию ниже для активации подписки, авторизации аккаунта Telethon или настройки мониторинга."
        ).format(user_id=user_id)
    else:
        text = error_msg + f"\n\nВаш ID: `{user_id}`. Пожалуйста, подпишитесь на канал для продолжения работы или введите **Промокод**."

    await state.clear()
    
    if isinstance(union, types.Message):
        await union.answer(text, reply_markup=keyboard)
    else:
        try:
            await union.message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest:
            pass 
        await union.answer()

# --- Telethon Авторизация (Стартовый хендлер) ---

@user_router.callback_query(F.data == "telethon_auth_start")
async def telethon_auth_choose_method_handler(callback: types.CallbackQuery, state: FSMContext):
    """Шаг 0: Предлагаем выбрать метод авторизации."""
    await state.set_state(TelethonAuth.CHOOSE_AUTH_METHOD)
    await callback.message.edit_text(
        "🔐 **Авторизация Telethon**\n\nВыберите удобный способ входа:",
        reply_markup=get_auth_method_kb()
    )
    await callback.answer()

@user_router.callback_query(F.data == "auth_method_phone", TelethonAuth.CHOOSE_AUTH_METHOD)
async def telethon_auth_start_phone(callback: types.CallbackQuery, state: FSMContext):
    """Шаг 1.1: Запрос номера телефона."""
    await state.set_state(TelethonAuth.PHONE)
    await callback.message.edit_text(
        "📱 Введите **номер телефона** в формате: `+79001234567` (обязательно с международным кодом).",
        parse_mode="Markdown",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()

@user_router.message(TelethonAuth.PHONE)
async def telethon_auth_step_phone(message: Message, state: FSMContext):
    """Шаг 1.2: Отправка кода по номеру."""
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
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()

# --- Логика QR-кода ---

@user_router.callback_query(F.data == "auth_method_qr", TelethonAuth.CHOOSE_AUTH_METHOD)
async def telethon_auth_start_qr(callback: types.CallbackQuery, state: FSMContext):
    """Шаг 1.3: Генерация QR-кода."""
    user_id = callback.from_user.id
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        login_token = await client.qr_login()
        
        # Генерируем QR-код
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(login_token.url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Сохраняем в буфер
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)
        
        await state.set_state(TelethonAuth.QR_CODE_WAIT)
        
        await callback.message.delete()
        
        message_qr = await bot.send_photo(
            chat_id=user_id,
            photo=BufferedInputFile(buffer.getvalue(), filename="qr_code.png"),
            caption="🖼️ **QR-код для входа сгенерирован.**\n\n"
                    "Откройте Telegram, перейдите в **Настройки -> Устройства -> Привязать рабочий стол** и отсканируйте код.\n\n"
                    "Ожидание авторизации... (Обычно 2 минуты)",
            reply_markup=get_cancel_keyboard()
        )
        
        await client.disconnect()
        
        # Запускаем ожидание входа
        asyncio.create_task(wait_for_qr_login(user_id, login_token, state, message_qr))
        
    except Exception as e:
        logger.error(f"Ошибка при генерации QR-кода: {e}")
        await callback.message.answer(f"❌ Не удалось сгенерировать QR-код. Ошибка: `{type(e).__name__}`", 
                                     reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    finally:
         if client.is_connected():
             await client.disconnect()

async def wait_for_qr_login(user_id: int, login_token, state: FSMContext, message_qr: Message):
    """Функция, которая ждет, пока пользователь авторизуется по QR-коду."""
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        await client.loop.run_in_executor(None, login_token.wait) # Блокирующая операция ожидания
        
        # Проверяем, авторизовались ли успешно
        if login_token.signed_in:
            await client.disconnect()
            
            task = asyncio.create_task(run_telethon_worker_for_user(user_id))
            ACTIVE_TELETHON_WORKERS[user_id] = task
            
            await message_qr.edit_caption("✅ **Авторизация по QR-коду успешна!** Telethon-сессия активна.", reply_markup=None)
            await message_qr.answer("Возврат в главное меню.", reply_markup=get_main_inline_kb(user_id))
        else:
            await message_qr.edit_caption("❌ **Время авторизации по QR-коду истекло** или сессия была отменена.", reply_markup=None)
            await message_qr.answer("Пожалуйста, попробуйте еще раз.", reply_markup=get_main_inline_kb(user_id))

    except asyncio.CancelledError:
        # Если пользователь нажал Отмена, task будет отменен
        await message_qr.edit_caption("❌ **Авторизация по QR-коду отменена** пользователем.", reply_markup=None)
    except Exception as e:
        logger.error(f"Ошибка в процессе ожидания QR-кода: {e}")
        await message_qr.edit_caption(f"❌ Критическая ошибка при авторизации по QR-коду: `{type(e).__name__}`", reply_markup=None)
    finally:
        await state.clear()
        if client.is_connected():
            await client.disconnect()


# --- Логика Кода и Пароля (Обновлено) ---

async def telethon_auth_step_code_logic(source_message: Message, state: FSMContext, code: str):
    user_id = source_message.from_user.id
    data = await state.get_data()
    phone_number = data.get('phone_number')
    phone_code_hash = data.get('phone_code_hash')
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        await client.sign_in(phone=phone_number, code=code, phone_code_hash=phone_code_hash)
        
        await client.disconnect()

        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await source_message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except SessionPasswordNeededError:
        if client.is_connected():
            await client.disconnect()
        await state.set_state(TelethonAuth.PASSWORD)
        await source_message.answer("🔑 **Требуется двухфакторная аутентификация (2FA).**\n\nВведите ваш облачный пароль Telegram:", reply_markup=get_cancel_keyboard())
        
    except Exception as e:
        error_text = f"❌ **Критическая ошибка:** Не удалось войти. `{type(e).__name__}`. Проверьте правильность кода."
        await source_message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
# 1. Обработка цифровых кнопок (UI)
@user_router.callback_query(F.data.startswith("auth_digit_") | F.data == "auth_submit_code" | F.data == "auth_delete_digit", TelethonAuth.CODE)
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

        # Используем исходное сообщение колбэка для ответа и для получения chat_id/user_id
        await callback.message.edit_text(f"⏳ Проверка кода: `{temp_code}`...", reply_markup=None)
        await telethon_auth_step_code_logic(callback.message, state, temp_code)
        await callback.answer("Код отправлен.")
        return

    # Обновление сообщения с текущим вводом
    current_display = f"`{temp_code}_`" if len(temp_code) < 6 else f"`{temp_code}`"
    try:
        await callback.message.edit_text(
            f"🔢 Введите **код** с помощью кнопок ниже или сообщением.\n"
            f"Текущий ввод: {current_display}",
            reply_markup=get_numeric_code_keyboard()
        )
    except TelegramBadRequest:
        pass # Игнорируем, если текст не изменился
    
    await callback.answer()
        
# 2. Обработка обычного сообщения
@user_router.message(TelethonAuth.CODE)
async def telethon_auth_step_code_message(message: Message, state: FSMContext):
    code = message.text.strip()
    
    if not code.isdigit() or len(code) < 4:
         await message.reply("❌ Введите код подтверждения цифрами.", reply_markup=get_numeric_code_keyboard())
         return
    
    # Обычное сообщение используется для проверки кода
    await telethon_auth_step_code_logic(message, state, code)


@user_router.message(TelethonAuth.PASSWORD)
async def telethon_auth_step_password(message: Message, state: FSMContext):
    user_id = message.from_user.id
    password = message.text.strip()
    
    data = await state.get_data()
    phone_number = data.get('phone_number')
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        await client.sign_in(password=password)
        
        await client.disconnect()

        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except Exception as e:
        error_text = f"❌ **Критическая ошибка 2FA:** Неверный пароль или `{type(e).__name__}`"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()


# --- Активация Промокода (Для пользователей) ---

@user_router.callback_query(F.data == "start_promo_fsm")
async def start_promo_fsm(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    # Снимаем проверку доступа для возможности ввести промокод
    # has_access, error_msg = await check_access(user_id, callback.bot)
    # if not has_access and user_id != ADMIN_ID:
    #      await callback.answer(error_msg, show_alert=True)
    #      return
    
    await state.set_state(PromoStates.waiting_for_code)
    await callback.message.edit_text("🔑 Введите ваш **промокод**:", 
                                     reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.message(PromoStates.waiting_for_code)
async def process_promo_code(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip().upper()
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT days, is_active, max_uses, current_uses FROM promo_codes WHERE code=?", (code,))
    promo_data = cur.fetchone()
    
    if not promo_data:
        await message.reply("❌ Промокод не найден.", reply_markup=get_cancel_keyboard())
        return

    days, is_active, max_uses, current_uses = promo_data
    
    if not is_active:
        await message.reply("❌ Промокод неактивен.", reply_markup=get_cancel_keyboard())
        return
        
    if max_uses is not None and current_uses >= max_uses:
        await message.reply("❌ Промокод исчерпал лимит использований.", reply_markup=get_cancel_keyboard())
        return

    current_user_data = db_get_user(user_id)
    
    if current_user_data.get('subscription_end_date'):
        try:
            current_end = datetime.strptime(current_user_data['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
            if current_end < datetime.now():
                 current_end = datetime.now()
        except:
             current_end = datetime.now()
    else:
        current_end = datetime.now()
        
    new_end_date = current_end + timedelta(days=days)
    new_end_date_str = new_end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    cur.execute("""
        UPDATE users 
        SET subscription_active=1, subscription_end_date=?, promo_code=?
        WHERE user_id=?
    """, (new_end_date_str, code, user_id))

    if max_uses is not None:
        cur.execute("UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code=?", (code,))

    conn.commit()
    await state.clear()
    
    await message.reply(
        f"🎉 **Подписка активирована!**\n"
        f"Срок действия: **{days} дней**.\n"
        f"Новая дата окончания: `{new_end_date.strftime('%d.%m.%Y %H:%M')}` (MSK).",
        reply_markup=get_main_inline_kb(user_id)
    )

# --- Админ-Панель (Без изменений) ---

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_panel_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа к этой панели.", show_alert=True)
        return
    
    await state.set_state(AdminStates.main_menu)
    await callback.message.edit_text(
        "🛠️ **Админ-Панель**\n\nВыберите действие:",
        reply_markup=get_admin_main_kb()
    )
    await callback.answer()

@user_router.callback_query(F.data == "admin_create_promo_start", AdminStates.main_menu)
async def admin_create_promo_step1_auto(callback: types.CallbackQuery, state: FSMContext):
    promo_code = secrets.token_hex(6).upper() # Генерируем 12-символьный код
    
    await state.update_data(new_promo_code=promo_code)
    await state.set_state(AdminStates.creating_promo_days)
    
    await callback.message.edit_text(
        f"➕ **Создание Промокода**\n\n"
        f"Сгенерированный код: `{promo_code}`\n\n"
        f"Шаг 1/3: Введите **количество дней** подписки (целое число):",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()

@user_router.message(AdminStates.creating_promo_days)
async def admin_create_promo_step2_days(message: Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        if days <= 0:
            raise ValueError
    except ValueError:
        await message.reply("❌ Введите положительное **целое число** дней.", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(promo_days=days)
    await state.set_state(AdminStates.creating_promo_uses)
    
    await message.reply(
        f"Шаг 2/3: Введите **максимальное количество использований** (целое число). "
        f"Введите `0` или `любой текст`, если промокод должен быть **безлимитным**:",
        reply_markup=get_cancel_keyboard()
    )

@user_router.message(AdminStates.creating_promo_uses)
async def admin_create_promo_step3_uses(message: Message, state: FSMContext):
    data = await state.get_data()
    code = data['new_promo_code']
    days = data['promo_days']
    max_uses = None
    
    try:
        uses = int(message.text.strip())
        if uses > 0:
            max_uses = uses
    except ValueError:
        pass # Безлимитный, max_uses останется None

    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO promo_codes (code, days, max_uses, is_active)
            VALUES (?, ?, ?, 1)
        """, (code, days, max_uses))
        conn.commit()
        
        await state.clear()
        
        uses_str = f"**{max_uses}**" if max_uses else "**Безлимитный**"
        
        await message.reply(
            f"✅ **Промокод Успешно Создан!**\n\n"
            f"Код: `{code}`\n"
            f"Срок: **{days}** дней\n"
            f"Лимит активаций: {uses_str}",
            reply_markup=get_main_inline_kb(message.from_user.id)
        )
        
    except sqlite3.IntegrityError:
        # Это произойдет, только если secrets.token_hex выдал дубликат (крайне маловероятно)
        await message.reply("❌ Произошла ошибка. Попробуйте снова (дубликат кода).", 
                             reply_markup=get_admin_main_kb())
        await state.set_state(AdminStates.main_menu)
    
# --- Скелет Выдачи Подписки (Улучшенная маршрутизация) ---

@user_router.callback_query(F.data == "admin_issue_sub_start", AdminStates.main_menu)
async def admin_issue_sub_step1_user(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.sub_target_user_id)
    await callback.message.edit_text(
        "➡️ **Выдача Подписки**\n\n"
        "Шаг 1/2: Введите **ID пользователя** Telegram (число) для выдачи подписки:",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()

@user_router.message(AdminStates.sub_target_user_id)
async def admin_issue_sub_step2_duration(message: Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.reply("❌ ID пользователя должен быть **целым числом**.", reply_markup=get_cancel_keyboard())
        return
    
    # Здесь можно было бы проверить существование пользователя в БД, но для простоты пропустим
    await state.update_data(target_user_id=user_id)
    await state.set_state(AdminStates.sub_duration_days)
    
    await message.reply(
        "Шаг 2/2: Введите **количество дней** подписки (целое число):",
        reply_markup=get_cancel_keyboard()
    )

@user_router.message(AdminStates.sub_duration_days)
async def admin_issue_sub_step3_finish(message: Message, state: FSMContext):
    # СКЕЛЕТ РЕАЛИЗАЦИИ: Здесь должна быть полная логика обновления подписки в БД
    
    data = await state.get_data()
    target_id = data['target_user_id']
    
    try:
        days = int(message.text.strip())
        if days <= 0:
            raise ValueError
    except ValueError:
        await message.reply("❌ Введите положительное **целое число** дней.", reply_markup=get_cancel_keyboard())
        return

    # --- Эмуляция обновления БД и отправки уведомления ---
    
    # db_update_user_subscription(target_id, days) 
    
    await state.clear()
    await message.reply(
        f"✅ **Подписка Выдана (Скелет)!**\n\n"
        f"Пользователь ID: `{target_id}`\n"
        f"Выдано: **{days}** дней.",
        reply_markup=get_main_inline_kb(message.from_user.id)
    )
    # --- КОНЕЦ СКЕЛЕТА ---


# --- Мониторинг и Отчеты (Без изменений) ---

# (Хендлеры мониторинга и отчетов были опущены для краткости, так как они не менялись)
# ...

# =========================================================================
# VII. ЗАПУСК БОТА
# =========================================================================

async def main():
    logger.info("Запуск бота...")
    
    db_init()
    logger.info("База данных инициализирована.")

    expired_users = db_check_and_deactivate_subscriptions()
    if expired_users:
        logger.info(f"Уведомление {len(expired_users)} пользователей об истечении подписки.")
        for user_id in expired_users:
            try:
                await bot.send_message(user_id, "⚠️ **Ваша подписка истекла.** Для продолжения использования, пожалуйста, продлите её.", reply_markup=get_main_inline_kb(user_id))
            except Exception:
                pass 

    dp.include_router(user_router)
    
    await start_all_active_telethon_workers()

    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
    except Exception as e:
        logger.critical(f"Критическая ошибка в main: {e}")
