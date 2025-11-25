import asyncio
import logging
import os
import sqlite3
import pytz
import time
import re
import io
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

# Импорты telethon
from telethon import TelegramClient, events
from telethon.errors import UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError
from telethon.utils import get_display_name

# Импорт для QR-кода
import qrcode 

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

storage = MemoryStorage()
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
        
    return False, f"❌ Для использования бота необходима активная подписка или подписка на канал **{TARGET_CHANNEL_URL}**."

def get_cancel_keyboard():
    """Возвращает клавиатуру с кнопкой 'Отмена'."""
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
        ]
    )

def get_numeric_code_keyboard(current_code=""):
    """Возвращает Inline-клавиатуру для удобного ввода 4/5-значного кода."""
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
    """Генерирует ГЛАВНУЮ клавиатуру (по скриншоту пользователя)."""
    user_data = db_get_user(user_id)
    is_telethon_active = user_data.get('telethon_active', 0) if user_data else 0
    worker_running = user_id in ACTIVE_TELETHON_WORKERS
    
    keyboard = []

    # 1. Если не авторизован (нет сессии) -> Только Авторизация и Промокод
    if not is_telethon_active:
        keyboard.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
        keyboard.append([InlineKeyboardButton(text="🔐 Авторизация", callback_data="telethon_auth_start")])
    # 2. Если авторизован (есть сессия) -> Мониторинг и Worker
    else:
        # Кнопка Мониторинга доступна только при наличии сессии
        keyboard.append([InlineKeyboardButton(text="📊 Отчеты и Мониторинг", callback_data="show_monitor_menu")])
        
        # Кнопка Worker'а
        worker_text = "🟢 Worker запущен" if worker_running else "🔴 Worker остановлен"
        worker_callback = "telethon_stop_session" if worker_running else "telethon_start_session"
        
        keyboard.append([
            InlineKeyboardButton(text=worker_text, callback_data=worker_callback),
            InlineKeyboardButton(text="ℹ️ Статус", callback_data="telethon_check_status")
        ])
    
    # 3. Админ-Панель (независимо от статуса)
    if user_id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
        
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

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
        [InlineKeyboardButton(text="➕ Создать Промокод (Скелет)", callback_data="admin_create_promo_start")],
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
        # Паттерн для DROP-лога (Телефон Пробел Время Пробел @ник Пробел бх [Пробел Время])
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
                
        
        # --- ХЕНДЛЕРЫ ДЛЯ КОМАНД В ЛС TELETHON-АККАУНТА (Скелет) ---
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
            
            # Логика выполнения команд... (заглушка)
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

# --- Обработчик Отмены и Назад ---
@user_router.callback_query(F.data == "cancel_action")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.edit_text("❌ Действие отменено.", reply_markup=None)
    except TelegramBadRequest:
        await callback.message.answer("❌ Действие отменено.", reply_markup=None)
    await cmd_start_or_back(callback, state)
    await callback.answer("Отменено.")

# --- Главное меню (/start) ---
@user_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = union.from_user.id
    
    db_set_session_status(user_id, False) 
    has_access, error_msg = await check_access(user_id, bot)
    
    keyboard = get_main_inline_kb(user_id)
    
    # Обновленное приветствие (по скриншоту)
    if has_access or user_id == ADMIN_ID:
        text = (
            "👋 **Привет, юный!**\n\n"
            "Ваш ID: `{user_id}`\n"
            "Это бот для мониторинга команд в Telegram-чатах с помощью вашей личной Telethon-сессии.\n\n"
            "Выберите опцию ниже."
        ).format(user_id=user_id)
    else:
        text = error_msg + f"\n\nВаш ID: `{user_id}`. Пожалуйста, выполните условия доступа, чтобы разблокировать функционал."

    await state.clear()
    
    if isinstance(union, types.Message):
        await union.answer(text, reply_markup=keyboard)
    else:
        try:
            await union.message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest:
            pass 
        await union.answer()

# --- Telethon Авторизация (Общий хендлер) ---

@user_router.callback_query(F.data == "telethon_auth_start")
async def telethon_auth_choose_method_handler(callback: types.CallbackQuery, state: FSMContext):
    """Шаг 0: Предлагаем выбрать метод авторизации."""
    await state.set_state(TelethonAuth.CHOOSE_AUTH_METHOD)
    await callback.message.edit_text(
        "🔐 **Авторизация Telethon**\n\nВыберите удобный способ входа:",
        reply_markup=get_auth_method_kb()
    )
    await callback.answer()

# --- Вход по Номеру ---
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
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()

# --- Вход по QR-коду ---
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
        # Максимальное время ожидания 120 секунд
        await client.loop.run_in_executor(None, lambda: login_token.wait(timeout=120)) 
        
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
        await message_qr.edit_caption("❌ **Авторизация по QR-коду отменена** пользователем.", reply_markup=None)
    except Exception as e:
        logger.error(f"Ошибка в процессе ожидания QR-кода: {e}")
        await message_qr.edit_caption(f"❌ Критическая ошибка при авторизации по QR-коду: `{type(e).__name__}`", reply_markup=None)
    finally:
        await state.clear()
        if client.is_connected():
            await client.disconnect()

# --- Логика Кода и Пароля ---

async def telethon_auth_step_code_logic(source_message: Message, state: FSMContext, code: str):
    """Основная логика проверки кода и перехода к паролю или завершению."""
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
        
# 2. Обработка кода, введенного сообщением
@user_router.message(TelethonAuth.CODE)
async def process_code_input_message(message: Message, state: FSMContext):
    code = message.text.strip()
    if not code.isdigit() or len(code) < 4:
        await message.answer("❌ Неверный формат кода. Введите только цифры (минимум 4).", reply_markup=get_numeric_code_keyboard())
        return

    # Используем исходное сообщение для ответа
    await message.answer(f"⏳ Проверка кода: `{code}`...", reply_markup=None)
    await telethon_auth_step_code_logic(message, state, code)


@user_router.message(TelethonAuth.PASSWORD)
async def telethon_auth_step_password(message: Message, state: FSMContext):
    """Шаг 3: Ввод 2FA-пароля."""
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
        
        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except Exception as e:
        error_text = f"❌ **Критическая ошибка:** Неверный пароль или ошибка сервера. `{type(e).__name__}`"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()
            
# --- Telethon Управление (Остановка/Статус/Запуск) ---

@user_router.callback_query(F.data == "telethon_stop_session")
async def telethon_stop_session_handler(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await stop_telethon_worker_for_user(user_id)
    
    await callback.message.edit_text("🛑 **Telethon-сессия остановлена.**", reply_markup=get_main_inline_kb(user_id))
    await callback.answer()

@user_router.callback_query(F.data == "telethon_start_session")
async def telethon_start_session_handler(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    task = asyncio.create_task(run_telethon_worker_for_user(user_id))
    ACTIVE_TELETHON_WORKERS[user_id] = task
    
    await callback.answer("Запуск Worker'а...", show_alert=True)
    await callback.message.edit_reply_markup(reply_markup=get_main_inline_kb(user_id))


@user_router.callback_query(F.data == "telethon_check_status")
async def telethon_check_status_handler(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    client = ACTIVE_TELETHON_CLIENTS.get(user_id)
    
    if client and client.is_connected():
        try:
            user_info = await client.get_me()
            session_file = get_session_file_path(user_id) + '.session'
            
            # Вычисляем время работы
            start_time = os.path.getmtime(session_file) if os.path.exists(session_file) else time.time()
            uptime_seconds = time.time() - start_time
            uptime_formatted = time.strftime('%H:%M:%S', time.gmtime(uptime_seconds))
            
            status_text = (
                "🟢 **Статус Сессии:** Активна\n"
                f"👤 **Имя Аккаунта:** {get_display_name(user_info)}\n"
                f"🆔 **ID Телефона:** `{user_info.id}`\n"
                f"⌚ **Время работы:** {uptime_formatted}"
            )
        except Exception:
            status_text = "⚠️ **Статус Сессии:** Не удалось получить данные аккаунта (ошибка связи)."
    else:
        status_text = "🔴 **Статус Сессии:** Не активна (Worker остановлен)."
        
    await callback.answer(status_text, show_alert=True)
    # Обновляем клавиатуру, чтобы кнопка статуса была актуальной
    await callback.message.edit_reply_markup(reply_markup=get_main_inline_kb(user_id))


### 4. Функционал Мониторинга и Отчетов

@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not db_check_subscription(user_id) and user_id != ADMIN_ID:
        await callback.answer("❌ Мониторинг доступен только при активной подписке.", show_alert=True)
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

async def process_chat_id_setting(message: Message, state: FSMContext, chat_field: str, chat_type: str):
    """Общий обработчик для установки IT/DROP чата."""
    user_id = message.from_user.id
    chat_input = message.text.strip()
    
    client = ACTIVE_TELETHON_CLIENTS.get(user_id)
    if not client:
        await message.answer("❌ Telethon Worker не запущен. Сначала запустите Worker.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        return

    await message.answer(f"⏳ Проверяю чат `{chat_input}`...")

    try:
        entity = await client.get_entity(chat_input)
        chat_id = str(entity.id)
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"UPDATE users SET {chat_field}=? WHERE user_id=?", (chat_id, user_id))
        conn.commit()
        conn.close()
        
        # Перезапускаем Worker для обновления хендлеров
        await stop_telethon_worker_for_user(user_id)
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer(f"✅ **{chat_type}** успешно установлен: `{chat_id}`.\nWorker будет перезапущен.", reply_markup=get_monitor_menu_kb())
        await state.clear()
        
    except Exception as e:
        logger.error(f"Ошибка получения Entity Telethon: {e}")
        await message.answer(f"❌ Не удалось найти чат `{chat_input}` или у аккаунта нет доступа. Проверьте ID/Username. Ошибка: `{type(e).__name__}`", reply_markup=get_cancel_keyboard())

# Хендлеры FSM для настройки чатов
@user_router.callback_query(F.data == "monitor_set_it")
async def monitor_set_it_start(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MonitorStates.waiting_for_it_chat_id)
    await callback.message.edit_text("💬 Введите **ID** или **Username** IT-чата (например, `-100123...` или `@mychat`).", reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_it_chat_id)
async def monitor_process_it_chat_id(message: Message, state: FSMContext):
    await process_chat_id_setting(message, state, 'it_chat_id', 'IT-чат')

@user_router.callback_query(F.data == "monitor_set_drop")
async def monitor_set_drop_start(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MonitorStates.waiting_for_drop_chat_id)
    await callback.message.edit_text("💬 Введите **ID** или **Username** DROP-чата (например, `-100123...` или `@mychat`).", reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_drop_chat_id)
async def monitor_process_drop_chat_id(message: Message, state: FSMContext):
    await process_chat_id_setting(message, state, 'drop_chat_id', 'DROP-чат')


# --- Генерация Отчета ---

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
async def report_select_topic(callback: types.CallbackQuery, state: FSMContext):
    log_type = callback.data.split('_')[-1]
    await state.update_data(log_type=log_type)
    await state.set_state(ReportStates.waiting_report_topic)
    
    prompt = "Введите **количество дней** (напр., `7`) для отчета или `0` для всех логов:"
    await callback.message.edit_text(prompt, reply_markup=get_cancel_keyboard())
    await callback.answer()


@user_router.message(ReportStates.waiting_report_topic)
async def report_process_days_and_send(message: Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        days = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Неверный формат. Введите целое число дней.", reply_markup=get_cancel_keyboard())
        return
        
    data = await state.get_data()
    log_type = data['log_type']
    
    logs = db_get_monitor_logs(user_id, log_type, days)
    
    report_chat_id = db_get_user(user_id).get('report_chat_id') or user_id
    
    if not logs:
        await message.answer(f"⚠️ Логи типа **{log_type}** за последние {days} дней не найдены.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        return

    # Формирование отчета
    report_text = f"**📊 Отчет [{log_type}]**\n\n"
    if days > 0:
        report_text += f"**Период:** Последние {days} дней\n"
    else:
        report_text += "**Период:** Все доступные логи\n"
        
    report_text += f"**Всего записей:** {len(logs)}\n\n"
    
    # Добавление логов (форматирование)
    for timestamp, command, target in logs:
        report_text += f"`[{timestamp}]` **{command}** (Target: {target or 'N/A'})\n"
        
    # Отправка отчета (разделение на части, если длинный)
    chunks = [report_text[i:i + 4096] for i in range(0, len(report_text), 4096)]
    
    try:
        for chunk in chunks:
            # Если report_chat_id не настроен, отправляем в ЛС
            target_chat = report_chat_id if str(report_chat_id).startswith('-') or str(report_chat_id).isdigit() else user_id
            await bot.send_message(target_chat, chunk, parse_mode='Markdown')
            
        await message.answer(f"✅ Отчет типа **{log_type}** ({len(logs)} записей) отправлен в чат `{report_chat_id}`.", reply_markup=get_main_inline_kb(user_id))
    except Exception as e:
        logger.error(f"Ошибка отправки отчета в {report_chat_id}: {e}")
        await message.answer(f"❌ Не удалось отправить отчет в чат `{report_chat_id}`. Отчет отправлен вам в ЛС (возможны ошибки форматирования).", reply_markup=get_main_inline_kb(user_id))
        for chunk in chunks:
            await message.answer(chunk, parse_mode='Markdown')

    await state.clear()

### 5. Функционал Промокодов (Скелет)

@user_router.callback_query(F.data == "start_promo_fsm")
async def start_promo_fsm_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await callback.message.edit_text("🔑 Введите **Промокод**:", reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.message(PromoStates.waiting_for_code)
async def process_promo_code(message: Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip().upper()
    
    # --- Скелет логики активации промокода ---
    # Здесь должна быть проверка в БД и обновление подписки
    
    await message.answer(f"❌ Промокод `{code}` не найден или недействителен (Скелет).", reply_markup=get_main_inline_kb(user_id))
    await state.clear()
    
### 6. Функционал Админ-Панели (Скелет)

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_panel_start_handler(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен.", show_alert=True)
        return
    
    await state.set_state(AdminStates.main_menu)
    await callback.message.edit_text("👑 **Админ-Панель**\n\nВыберите действие:", reply_markup=get_admin_main_kb())
    await callback.answer()
    
# --- Логика Админки (скелет) ---
# ... (Здесь должны быть хендлеры для создания промокодов и выдачи подписки) ...


### 7. Финальный Запуск

async def main():
    logger.info("Запуск бота...")
    
    db_init()
    logger.info("База данных инициализирована.")

    dp.include_router(user_router)
    
    # Запуск Worker'ов, которые должны работать
    await start_all_active_telethon_workers()

    # Запуск polling Aiogram
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
    except Exception as e:
        logger.critical(f"Критическая ошибка в main: {e}")
