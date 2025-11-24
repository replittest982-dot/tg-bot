import asyncio
import logging
import os
import sqlite3
import pytz
import time
import re
import secrets # Для генерации промокодов
import io # Для работы с QR-кодом
import qrcode # Для генерации QR-кокода
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
from telethon import TelegramClient, events
from telethon.errors import UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, RPCError, ButtonDataInvalidError
from telethon.tl.types import PeerChannel
from telethon.utils import get_display_name

# =========================================================================
# I. КОНФИГУРАЦИЯ (ОБЯЗАТЕЛЬНО ПРОВЕРЬТЕ КЛЮЧИ)
# =========================================================================

# ВАШИ КЛЮЧИ АВТОРИЗАЦИИ
BOT_TOKEN = "7868097991:AAFQtLSv6nlS5PmGH4TMsgV03dxs_X7iZf8"
ADMIN_ID = 6256576302
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1"
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальные хранилища для активных сессий Telethon и долгих задач
ACTIVE_TELETHON_CLIENTS = {}
ACTIVE_TELETHON_WORKERS = {}
ACTIVE_LONG_TASKS = {} 

storage = MemoryStorage()

# ИСПРАВЛЕНИЕ OШИБКИ TypeError: parse_mode
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

class AdminStates(StatesGroup):
    """Состояния для Админ-панели."""
    main_menu = State()
    # Создание промокода
    creating_promo_days = State()
    creating_promo_uses = State()
    # Выдача подписки
    sub_target_user_id = State()
    sub_duration_days = State()

class MonitorStates(StatesGroup):
    """Состояния для настройки мониторинга и генерации отчетов."""
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()
    waiting_for_report_chat_id = State()

# =========================================================================
# III. РАБОТА С БАЗОЙ ДАННЫХ (SQLite)
# =========================================================================

DB_PATH = os.path.join('data', DB_NAME)

def get_db_connection():
    """Получает соединение с базой данных."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def db_init():
    """Инициализация базы данных и создание таблиц, если они не существуют."""
    conn = get_db_connection()
    cur = conn.cursor()
    # ... (код db_init остается без изменений) ...
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
            type TEXT NOT NULL, -- 'IT' или 'DROP'
            command TEXT,
            target TEXT,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    """)
    conn.commit()

def db_get_user(user_id: int):
    """Получает данные пользователя."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    if row:
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))
    return None

def db_check_subscription(user_id: int) -> bool:
    """Проверяет активность подписки."""
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

def db_clear_monitor_logs(user_id, log_type):
    """Очищает логи мониторинга по типу."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM monitor_logs WHERE user_id=? AND type=?", (user_id, log_type))
    conn.commit()

def db_get_monitor_logs(user_id, log_type, since_days: int = None):
    """Получает логи мониторинга по типу с возможностью фильтрации по дате."""
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
    """Добавляет запись в логи мониторинга."""
    conn = get_db_connection()
    cur = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("INSERT INTO monitor_logs (user_id, timestamp, type, command, target) VALUES (?, ?, ?, ?, ?)",
                (user_id, timestamp, log_type, command, target))
    conn.commit()

def db_set_session_status(user_id: int, is_active: bool, hash_code: str = None):
    """Устанавливает статус Telethon-сессии в базе данных."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO users (user_id, subscription_active, telethon_active) 
        VALUES (?, 0, 0)
    """, (user_id,))
    
    cur.execute("""
        UPDATE users SET telethon_active=?, telethon_hash=? WHERE user_id=?
    """, (1 if is_active else 0, hash_code, user_id))
    conn.commit()

def db_get_active_telethon_users():
    """Получает список ID пользователей с активной сессией в БД."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
    return [row[0] for row in cur.fetchall()]

def db_check_and_deactivate_subscriptions():
    """НОВАЯ ФУНКЦИЯ: Проверяет и деактивирует просроченные подписки."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Находим пользователей с истекшей, но еще активной подпиской
    cur.execute("""
        SELECT user_id FROM users 
        WHERE subscription_active=1 AND subscription_end_date < ?
    """, (now_str,))
    
    expired_users = [row[0] for row in cur.fetchall()]
    
    if expired_users:
        # Деактивируем их
        cur.execute("""
            UPDATE users SET subscription_active=0, subscription_end_date=NULL
            WHERE subscription_active=1 AND subscription_end_date < ?
        """, (now_str,))
        conn.commit()
        logger.info(f"Деактивировано {len(expired_users)} просроченных подписок.")
        
        # Оповещаем пользователей (асинхронно, в основной функции main)
        return expired_users
    return []


# =========================================================================
# IV. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И КЛАВИАТУРА
# =========================================================================

def get_session_file_path(user_id: int):
    """Получает путь к файлу сессии Telethon."""
    os.makedirs('data', exist_ok=True)
    return os.path.join('data', f'session_{user_id}')

async def check_access(user_id: int, bot: Bot) -> tuple[bool, str]:
    """Проверяет доступ пользователя (админ, подписка, канал)."""
    # ... (код check_access остается без изменений) ...
    if user_id == ADMIN_ID:
        return True, ""
    
    user = db_get_user(user_id)
    if not user:
        db_set_session_status(user_id, False) 
        user = db_get_user(user_id)

    subscribed = db_check_subscription(user_id)

    if subscribed:
        return True, ""

    try:
        member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id) 
        if member.status in ["member", "administrator", "creator"]:
             return True, ""
    except Exception as e:
        logger.error(f"Ошибка проверки подписки на канал для {user_id}: {e}")
        
    return False, f"❌ Для использования бота необходима активная подписка или подписка на канал {TARGET_CHANNEL_URL}. Подпишитесь и нажмите /start снова."

def get_cancel_keyboard():
    """НОВАЯ УТИЛИТА: Возвращает клавиатуру с кнопкой 'Отмена'."""
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
        ]
    )

def get_main_inline_kb(user_id: int) -> InlineKeyboardMarkup:
    """Генерирует главную инлайн-клавиатуру."""
    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    
    kb = [
        [InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")],
        [InlineKeyboardButton(text="📊 Отчеты и Мониторинг", callback_data="show_monitor_menu")],
    ]
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])

    auth_text = "🟢 Сессия активна" if session_active else "🔐 Авторизовать Telethon"
    auth_callback = "telethon_auth_status" if session_active else "telethon_auth_start"
    
    # Добавление проверки статуса аккаунта, если сессия активна
    if session_active:
        kb.append([
            InlineKeyboardButton(text="🛑 Остановить Сессию", callback_data="telethon_stop_session"),
            InlineKeyboardButton(text="ℹ️ Статус Аккаунта", callback_data="telethon_check_status")
        ])
    else:
         kb.append([InlineKeyboardButton(text=auth_text, callback_data=auth_callback)])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_monitor_menu_kb() -> InlineKeyboardMarkup:
    """Клавиатура меню мониторинга."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настроить IT-Чат", callback_data="monitor_set_it")],
        [InlineKeyboardButton(text="⚙️ Настроить DROP-Чат", callback_data="monitor_set_drop")],
        [InlineKeyboardButton(text="📋 Сгенерировать Отчет", callback_data="monitor_generate_report_start")], # Изменено
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

# =========================================================================
# V. TELETHON WORKER И КОМАНДЫ
# =========================================================================

async def stop_telethon_worker_for_user(user_id: int):
    """Останавливает Telethon worker и очищает ресурсы."""
    # ... (код stop_telethon_worker_for_user остается без изменений) ...
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
    
    # ... (код run_telethon_worker_for_user остается без изменений) ...
    # Сначала пытаемся остановить старый, если он висит
    await stop_telethon_worker_for_user(user_id) 
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        # Проверяем, что файл сессии существует
        if not os.path.exists(session_path + '.session'):
            logger.warning(f"Файл сессии не найден для {user_id}. Невозможно запустить worker.")
            db_set_session_status(user_id, False)
            await bot.send_message(user_id, "❌ Файл сессии не найден. Требуется повторная авторизация.", reply_markup=get_main_inline_kb(user_id))
            return

        await client.start()
        user_info = await client.get_me()
        logger.info(f"Telethon [{user_id}] запущен как: {get_display_name(user_info)}")
        
        db_set_session_status(user_id, True)
        
        # Обновление клавиатуры
        await bot.send_message(user_id, "⚙️ **Telethon Worker запущен и готов к работе!**", reply_markup=get_main_inline_kb(user_id))

        # Получаем настроенные чаты для логирования
        user_db = db_get_user(user_id)
        it_chat_id_str = user_db.get('it_chat_id')
        drop_chat_id_str = user_db.get('drop_chat_id')

        # --- ХЕНДЛЕРЫ ДЛЯ МОНИТОРИНГА И ЛОГИРОВАНИЯ ---
        
        IT_PATTERNS = {
            ".встал": r'^\.встал.*',
            ".кьар": r'^\.кьар.*',
            ".ошибка": r'^\.ошибка.*',
            ".замена": r'^\.замена.*',
            ".повтор": r'^\.повтор.*',
        }
        DROP_PATTERN_REGEX = r'^\+?\d{10,15}\s+\d{1,2}:\d{2}\s+@\w+\s+бх(?:\s+\d{1,2}:\d{2})?.*'
        DROP_PATTERNS = {"DROP_ENTRY": DROP_PATTERN_REGEX}


        @client.on(events.NewMessage)
        async def monitor_listener(event):
            # Проверка, что это группа/канал 
            if not event.is_group and not event.is_channel:
                return

            if not event.message.text:
                 return 

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
            if event.sender_id != me.id:
                 return
            
            if not event.is_private:
                return
            
            command = event.text.split()[0].lower()
            
            response_msg = f"✅ Команда {command} принята в работу. (Скелет)"
            
            # ... (остальной скелет команд) ...
            if command == '.лс':
                 response_msg = "Массовая рассылка (.лс) — Скелет реализации. Используйте: `.лс [юзер/канал] | [сообщение]`"
            
            elif command == '.флуд':
                 response_msg = "Флуд (.флуд) — Скелет реализации. Используйте: `.флуд [юзер/канал] | [сообщение]`"
            
            elif command == '.стопфлуд':
                 response_msg = "Остановка флуда (.стопфлуд) — Скелет реализации."
                 
            elif command == '.чекгруппу':
                 response_msg = "Анализ группы (.чекгруппу) — Скелет реализации. Используйте: `.чекгруппу [юзер/канал]`"
            
            await event.reply(response_msg)

        await client.run_until_disconnected()
    except asyncio.CancelledError:
        logger.info(f"Telethon Worker [{user_id}] отменен по запросу.")
    # ... (остальные except блоки) ...
    except UserDeactivatedError:
        logger.warning(f"Аккаунт Telethon {user_id} деактивирован.")
        await bot.send_message(user_id, "⚠️ Аккаунт деактивирован. Переавторизуйтесь.", reply_markup=get_main_inline_kb(user_id))
    except Exception as e:
        logger.error(f"Критическая ошибка Telethon Worker [{user_id}]: {e}")
        error_text = f"❌ Критическая ошибка Telethon Worker: `{type(e).__name__}`. Требуется переавторизация."
        if isinstance(e, FloodWaitError):
             error_text = f"❌ Ошибка лимитов Telegram: Необходимо подождать {e.seconds} секунд."
        elif "AuthorizationKeyUnregistered" in str(e):
             error_text = "❌ Ключ авторизации недействителен. Возможно, сессия была завершена. Требуется переавторизация."
             
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

# --- Обработчик Отмены (НОВАЯ ФУНКЦИЯ) ---
@user_router.callback_query(F.data == "cancel_action")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик для кнопки отмены во всех FSM-сценариях."""
    await state.clear()
    
    # Пытаемся отредактировать сообщение с удалением клавиатуры
    try:
        await callback.message.edit_text("❌ Действие отменено.", reply_markup=None)
    except TelegramBadRequest:
        await callback.message.answer("❌ Действие отменено.", reply_markup=None)
    
    # Возвращаем пользователя в главное меню
    await cmd_start_or_back(callback, state)
    await callback.answer("Отменено.")

# --- Главное меню ---
@user_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    """Обработчик команды /start и кнопки 'Назад'."""
    # ... (код cmd_start_or_back остается без изменений) ...
    user_id = union.from_user.id
    
    has_access, error_msg = await check_access(user_id, bot)
    
    keyboard = get_main_inline_kb(user_id)
    
    text = f"Привет! Используйте меню ниже. Ваш ID: `{user_id}`"
    if not has_access and user_id != ADMIN_ID:
        text = error_msg + f"\n\nВаш ID: `{user_id}`"

    await state.clear()
    
    if isinstance(union, types.Message):
        await union.answer(text, reply_markup=keyboard)
    else:
        try:
            await union.message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest:
            pass 
        await union.answer()
        
# --- Остановка Telethon-сессии ---
@user_router.callback_query(F.data == "telethon_stop_session")
async def telethon_stop_session_handler(callback: types.CallbackQuery):
    """Останавливает Telethon worker по запросу пользователя."""
    # ... (код telethon_stop_session_handler остается без изменений) ...
    user_id = callback.from_user.id
    
    await stop_telethon_worker_for_user(user_id)
    
    # Обновляем клавиатуру, чтобы показать кнопку "Авторизовать"
    await callback.message.edit_text("🛑 **Telethon Worker остановлен.**\n\nДля возобновления работы нажмите 'Авторизовать Telethon'.", 
                                     reply_markup=get_main_inline_kb(user_id))
    await callback.answer("Сессия остановлена.")

# --- НОВАЯ ФУНКЦИЯ: Проверка статуса Telethon-аккаунта ---
@user_router.callback_query(F.data == "telethon_check_status")
async def telethon_check_status_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    client = ACTIVE_TELETHON_CLIENTS.get(user_id)
    
    if not client or not client.is_connected():
        await callback.answer("Сессия неактивна.", show_alert=True)
        return

    await callback.answer("Проверяю статус аккаунта...", show_alert=False)
    
    try:
        user_info = await client.get_me()
        status_text = (
            f"ℹ️ **Статус Telethon-аккаунта:**\n"
            f"Имя: `{get_display_name(user_info)}`\n"
            f"Username: `@{user_info.username}`\n"
            f"ID: `{user_info.id}`\n"
            f"Статус: ✅ Активен"
        )
        await callback.message.answer(status_text, reply_markup=get_main_inline_kb(user_id))
    except Exception as e:
        logger.error(f"Ошибка проверки статуса Telethon для {user_id}: {e}")
        await callback.message.answer("❌ Не удалось получить статус аккаунта. Возможно, требуется переавторизация.", reply_markup=get_main_inline_kb(user_id))


# --- Telethon Авторизация (НОВЫЕ ШАГИ QR-AUTH) ---

@user_router.callback_query(F.data == "telethon_auth_start")
async def telethon_auth_start_choice(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса авторизации Telethon - Выбор метода."""
    user_id = callback.from_user.id
    
    has_access, error_msg = await check_access(user_id, callback.bot)
    if not has_access:
        await callback.answer(error_msg, show_alert=True)
        return

    if user_id in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("Сессия уже активна.", show_alert=True)
        return

    await state.set_state(TelethonAuth.CHOOSE_AUTH_METHOD)
    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="📱 По номеру телефона", callback_data="auth_phone")],
            [types.InlineKeyboardButton(text="📷 По QR-коду (beta)", callback_data="auth_qr")],
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
        ]
    )
    await callback.message.edit_text("🔐 **Начало авторизации Telethon**\n\nВыберите удобный способ:", reply_markup=keyboard)
    await callback.answer()

@user_router.callback_query(F.data == "auth_phone", TelethonAuth.CHOOSE_AUTH_METHOD)
async def start_phone_auth(callback: types.CallbackQuery, state: FSMContext):
    """Переход к вводу номера телефона."""
    await state.set_state(TelethonAuth.PHONE)
    await callback.message.edit_text(
        "Введите **номер телефона** в формате: `+79001234567` (обязательно с международным кодом).",
        parse_mode="Markdown",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()


@user_router.message(TelethonAuth.PHONE)
async def telethon_auth_step_phone(message: Message, state: FSMContext):
    """Обработка ввода номера телефона."""
    user_id = message.from_user.id
    phone_number = message.text.strip()
    
    # ... (логика Telethon send_code_request) ...
    # ... (проверки, инициализация client, отправка кода) ...

    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        result = await client.send_code_request(phone_number)
            
        await state.update_data(phone_number=phone_number, phone_code_hash=result.phone_code_hash)
        
        await state.set_state(TelethonAuth.CODE)
        await message.answer(
            f"🔢 **Код подтверждения отправлен.**\n\n"
            f"Введите **код** (цифры), который пришел вам в Telegram на номер `{phone_number}`.",
            reply_markup=get_cancel_keyboard()
        )
        
    except Exception as e:
        error_text = f"❌ **Критическая ошибка авторизации:** Не удалось отправить код. `{str(e)}`"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()

@user_router.message(TelethonAuth.CODE)
async def telethon_auth_step_code(message: Message, state: FSMContext):
    """Обработка ввода кода подтверждения. (Улучшенный UI)"""
    user_id = message.from_user.id
    code = message.text.strip()
    
    if not code.isdigit():
        # Улучшенный UI с эмодзи для ошибки (1️⃣2️⃣3️⃣)
        visual_input = " ".join([f"{c}️" for c in code]) 
        await message.answer(
            f"❌ **Неверный формат:** Код должен состоять только из цифр.\n"
            f"**Ваш ввод:** {visual_input}\n\n"
            "Пожалуйста, введите код еще раз:",
            reply_markup=get_cancel_keyboard()
        )
        return
    # ... (логика Telethon sign_in с кодом) ...
    
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
        
        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except SessionPasswordNeededError:
        if client.is_connected():
            await client.disconnect()
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔑 **Требуется двухфакторная аутентификация (2FA).**\n\nВведите ваш облачный пароль Telegram:", reply_markup=get_cancel_keyboard())
        
    except Exception as e:
        error_text = f"❌ **Критическая ошибка:** Не удалось войти. `{type(e).__name__}`"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()

@user_router.message(TelethonAuth.PASSWORD)
async def telethon_auth_step_password(message: Message, state: FSMContext):
    """Обработка ввода облачного пароля (2FA). (С кнопкой отмены)"""
    # ... (логика Telethon sign_in с паролем) ...
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
        error_text = f"❌ **Критическая ошибка 2FA:** Не удалось войти. `{type(e).__name__}`"
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()


# --- НОВЫЙ ФЛОУ: АВТОРИЗАЦИЯ ПО QR-КОДУ ---

@user_router.callback_query(F.data == "auth_qr", TelethonAuth.CHOOSE_AUTH_METHOD)
async def start_qr_auth(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await callback.message.edit_text("⏳ Запускаю вход по QR-коду...")

    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        qr_login = await client.qr_login() # Инициация QR-входа Telethon

        # Используем библиотеку qrcode для отрисовки изображения
        img = qrcode.make(qr_login.url)
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)

        await callback.message.answer_photo(
            photo=BufferedInputFile(buffer.getvalue(), filename="qr_code.png"),
            caption=(
                "**Вход по QR-коду:**\n"
                "1. Откройте Telegram на телефоне.\n"
                "2. Перейдите в **Настройки** -> **Устройства** -> **Привязать рабочий стол**.\n"
                "3. **Отсканируйте** этот QR-код.\n\n"
                "Я жду сканирования до 90 секунд..."
            ),
            parse_mode="Markdown",
            reply_markup=get_cancel_keyboard()
        )
        
        await state.set_state(TelethonAuth.QR_CODE_WAIT)
        
        # Мониторинг завершения QR-сессии
        await monitor_qr_login(qr_login, user_id, callback.message.chat.id, state)

    except Exception as e:
        logger.error(f"Ошибка при QR-авторизации для {user_id}: {e}")
        await callback.message.answer(f"❌ Произошла ошибка при QR-авторизации: `{type(e).__name__}`", reply_markup=get_cancel_keyboard())
    finally:
        if client.is_connected():
            await client.disconnect()
            
async def monitor_qr_login(qr_login, user_id, chat_id, state: FSMContext):
    """Асинхронно мониторит статус QR-авторизации."""
    try:
        # Ждем завершения авторизации
        user_info = await qr_login.wait(timeout=90) 
        
        # Если успешно, запускаем worker
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await bot.send_message(chat_id, "✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except asyncio.TimeoutError:
        await bot.send_message(chat_id, "⏰ **Время ожидания истекло.** QR-код не был отсканирован.", reply_markup=get_main_inline_kb(user_id))
    except Exception as e:
         logger.error(f"Ошибка в мониторинге QR-входа для {user_id}: {e}")
         await bot.send_message(chat_id, f"❌ **Критическая ошибка QR-входа:** `{type(e).__name__}`. Попробуйте снова.", reply_markup=get_main_inline_kb(user_id))
    finally:
        await state.clear() # Очистка FSM в любом случае

# --- Админ-Панель ---

# НОВАЯ УТИЛИТА: Найти ID пользователя по пересланному сообщению
@user_router.message(F.forward_from, F.chat.id == ADMIN_ID)
async def admin_find_id_by_forward(message: types.Message):
    target_id = message.forward_from.id
    target_name = message.forward_from.full_name
    await message.reply(f"🔍 **ID пользователя по пересылке:**\n"
                        f"Имя: `{target_name}`\n"
                        f"ID: `{target_id}`", parse_mode="Markdown")

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_panel_start_handler(callback: types.CallbackQuery, state: FSMContext):
    """Главное меню Админ-панели."""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Доступ запрещен.", show_alert=True)
        return

    await state.set_state(AdminStates.main_menu)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать Промокод (Авто)", callback_data="admin_create_promo_auto")], # Изменено
        [InlineKeyboardButton(text="✍️ Выдать Подписку", callback_data="admin_manual_sub")],
        [InlineKeyboardButton(text="⬅️ В Главное Меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text("🛠️ **Админ-Панель**\n\n**Перешлите** мне сообщение от любого пользователя, чтобы узнать его ID. Выберите действие:", reply_markup=keyboard)
    await callback.answer()


@user_router.callback_query(F.data == "admin_create_promo_auto", AdminStates.main_menu)
async def admin_create_promo_step1_auto(callback: types.CallbackQuery, state: FSMContext):
    """Шаг 1: Автоматическая генерация кода и запрос дней."""
    # --- АВТОГЕНЕРАЦИЯ ПРОМОКОДА ---
    promo_code = secrets.token_urlsafe(8).upper()
    
    conn = get_db_connection()
    cur = conn.cursor()
    # Проверка на уникальность (хотя токен_urlsafe очень маловероятно сгенерирует дубликат)
    while cur.execute("SELECT code FROM promo_codes WHERE code=?", (promo_code,)).fetchone():
        promo_code = secrets.token_urlsafe(8).upper() 

    await state.update_data(new_promo_code=promo_code)
    await state.set_state(AdminStates.creating_promo_days)
    await callback.message.edit_text(f"🎁 **Промокод сгенерирован:** `{promo_code}`\n\n"
                                     f"Введите **количество дней** подписки (например, `7`):",
                                     reply_markup=get_cancel_keyboard())
    await callback.answer()

@user_router.message(AdminStates.creating_promo_days)
async def admin_create_promo_step2(message: types.Message, state: FSMContext):
    """Шаг 2: Запрос количества использований."""
    # ... (логика проверки дней, остается без изменений) ...
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError
    except ValueError:
        await message.reply("❌ Введите корректное число дней (больше 0).", reply_markup=get_cancel_keyboard())
        return

    await state.update_data(new_promo_days=days)
    await state.set_state(AdminStates.creating_promo_uses)
    await message.reply("Введите **максимальное количество использований** (например, `10`).\nДля **бесконечного** использования введите `0` или `all`:",
                         reply_markup=get_cancel_keyboard()) # Добавлена кнопка отмены

@user_router.message(AdminStates.creating_promo_uses)
async def admin_create_promo_final(message: types.Message, state: FSMContext):
    """Шаг 3: Финализация создания промокода."""
    # ... (логика финализации, остается без изменений) ...
    uses_input = message.text.strip().lower()
    max_uses = None
    if uses_input not in ('0', 'all'):
        try:
            max_uses = int(uses_input)
            if max_uses <= 0: raise ValueError
        except ValueError:
            await message.reply("❌ Введите корректное число использований, `0`, или `all`.", reply_markup=get_cancel_keyboard())
            return

    data = await state.get_data()
    code = data['new_promo_code']
    days = data['new_promo_days']
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO promo_codes (code, days, is_active, max_uses, current_uses) VALUES (?, ?, 1, ?, 0)",
                (code, days, max_uses))
    conn.commit()
    
    max_uses_display = max_uses if max_uses is not None else "Бесконечно"
    
    await message.reply(f"✅ **Промокод создан!**\n\n"
                        f"Код: `{code}`\n"
                        f"Дни: {days}\n"
                        f"Использований: {max_uses_display}",
                         reply_markup=get_main_inline_kb(message.from_user.id))
    await state.clear()


# --- Мониторинг и Отчеты (ДОРАБОТКА ИНТЕРВАЛА) ---

@user_router.callback_query(F.data == "monitor_generate_report_start")
async def monitor_generate_report_start(callback: types.CallbackQuery):
    """Запрос типа отчета."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="IT-Лог", callback_data="select_report_type_IT")],
        [InlineKeyboardButton(text="DROP-Лог", callback_data="select_report_type_DROP")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="show_monitor_menu")],
    ])
    await callback.message.edit_text("📋 Выберите, по какому типу логов сгенерировать отчет:", reply_markup=kb)
    await callback.answer()

@user_router.callback_query(F.data.startswith("select_report_type_"))
async def monitor_select_report_type(callback: types.CallbackQuery, state: FSMContext):
    """Запрос интервала отчета."""
    log_type = callback.data.split('_')[3]
    await state.update_data(report_log_type=log_type)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Последние 24 часа", callback_data=f"report_interval_{log_type}_1")],
        [InlineKeyboardButton(text="Последние 7 дней", callback_data=f"report_interval_{log_type}_7")],
        [InlineKeyboardButton(text="Полный лог", callback_data=f"report_interval_{log_type}_all")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")],
    ])
    await callback.message.edit_text(f"⏳ Выберите интервал для отчета по **{log_type}** логам:", reply_markup=kb)
    await callback.answer()

@user_router.callback_query(F.data.startswith("report_interval_"))
async def monitor_generate_report_final(callback: types.CallbackQuery):
    """Генерация и отправка отчета."""
    user_id = callback.from_user.id
    parts = callback.data.split('_')
    log_type = parts[2]
    interval = parts[3]
    
    since_days = None
    if interval.isdigit():
        since_days = int(interval)
    
    await callback.message.edit_text(f"⏳ Генерирую отчет по логам типа **{log_type}** ({'за ' + str(since_days) + ' дн.' if since_days else 'полный лог'})...")
    
    report_text = await generate_monitor_report(user_id, log_type, since_days)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗑️ Очистить {log_type}-Лог", callback_data=f"clear_log_{log_type}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="show_monitor_menu")],
    ])
    
    await callback.message.edit_text(report_text, reply_markup=kb)
    await callback.answer("Отчет сгенерирован.")

async def generate_monitor_report(user_id: int, log_type: str, since_days: int = None) -> str:
    """Генерирует форматированный отчет из логов с фильтром по дате."""
    logs = db_get_monitor_logs(user_id, log_type, since_days)
    
    # ... (остальной код generate_monitor_report) ...
    if not logs:
        return f"🤷‍♂️ Логи типа **{log_type}** {'за выбранный период' if since_days else ''} пусты."

    report_title = "📊 IT-Лог Отчет" if log_type == 'IT' else "📊 DROP-Лог Отчет"
    period = f" (за последние {since_days} дней)" if since_days else " (Полный лог)"
    report_text = f"**{report_title}**{period} ({len(logs)} записей):\n\n"
    
    command_counts = {}
    for _, command, _ in logs:
        command_counts[command] = command_counts.get(command, 0) + 1
        
    stats_text = ""
    for command, count in sorted(command_counts.items(), key=lambda item: item[1], reverse=True):
        stats_text += f" • `{command}`: {count}\n"
        
    report_text += "**Статистика по командам:**\n" + stats_text + "\n"
    
    report_text += "**Последние 5 записей:**\n"
    for timestamp, command, target in logs[-5:]:
        short_target = target[:70].replace('\n', ' ') + '...' if len(target) > 70 else target
        report_text += f"`{timestamp}` | `{command}`\n> {short_target}\n"
        
    return report_text

# ... (остальные хендлеры, такие как monitor_set_it и т.д.) ...
@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    """Показывает меню мониторинга."""
    # ... (код show_monitor_menu_handler остается без изменений) ...
    user_id = callback.from_user.id
    
    has_access, error_msg = await check_access(user_id, callback.bot)
    if not has_access:
         await callback.answer(error_msg, show_alert=True)
         return
    
    await state.clear()
    user = db_get_user(user_id)
    it_id = user.get('it_chat_id', 'Не установлен')
    drop_id = user.get('drop_chat_id', 'Не установлен')
    
    text = (f"📊 **Отчеты и Мониторинг**\n\n"
            f"Текущие настройки:\n"
            f"IT-Чат ID: `{it_id}`\n"
            f"DROP-Чат ID: `{drop_id}`\n\n"
            "**Важно:** ID чатов должны быть указаны в **числовом формате** (например, `-1001234567890`).")
    
    await callback.message.edit_text(text, reply_markup=get_monitor_menu_kb())
    await callback.answer()

@user_router.callback_query(F.data == "monitor_set_it")
async def monitor_set_it_start(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MonitorStates.waiting_for_it_chat_id)
    await callback.message.edit_text("⚙️ Введите **числовой ID** чата для IT-лога (начинается с `-100...`):",
                                     reply_markup=get_cancel_keyboard()) # Добавлена кнопка отмены
    await callback.answer()

@user_router.callback_query(F.data == "monitor_set_drop")
async def monitor_set_drop_start(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MonitorStates.waiting_for_drop_chat_id)
    await callback.message.edit_text("⚙️ Введите **числовой ID** чата для DROP-лога (начинается с `-100...`):",
                                     reply_markup=get_cancel_keyboard()) # Добавлена кнопка отмены
    await callback.answer()

# =========================================================================
# VII. ЗАПУСК БОТА
# =========================================================================

async def main():
    """Главная функция запуска бота."""
    logger.info("Запуск бота...")
    
    db_init()
    logger.info("База данных инициализирована.")

    # НОВАЯ ФУНКЦИЯ: Проверка и деактивация просроченных подписок
    expired_users = db_check_and_deactivate_subscriptions()
    if expired_users:
        logger.info(f"Уведомление {len(expired_users)} пользователей об истечении подписки.")
        for user_id in expired_users:
            try:
                await bot.send_message(user_id, "⚠️ **Ваша подписка истекла.** Для продолжения использования, пожалуйста, продлите её.", reply_markup=get_main_inline_kb(user_id))
            except Exception:
                pass # Игнорируем ошибки отправки, если пользователь заблокировал бота

    dp.include_router(user_router)
    
    # Запуск всех активных Telethon-воркеров при старте
    await start_all_active_telethon_workers()

    # Запуск поллинга Aiogram
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
    except Exception as e:
        logger.critical(f"Критическая ошибка в main: {e}")
