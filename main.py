import asyncio
import logging
import os
import sqlite3
import pytz
import time
import re
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, Message
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from telethon import TelegramClient, events
from telethon.errors import UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, RPCError, ButtonDataInvalidError
from telethon.tl.types import PeerChannel
from telethon.utils import get_display_name

# =========================================================================
# I. КОНФИГУРАЦИЯ (ОБЯЗАТЕЛЬНО ОБНОВИТЕ КЛЮЧИ)
# =========================================================================

# ВАШИ КЛЮЧИ АВТОРИЗАЦИИ
BOT_TOKEN = "7868097991:AAFQtLSv6nlS5PmGH4TMsgV03dxs_X7iZf8"  # Новый токен
ADMIN_ID = 6256576302  # Ваш ID для доступа к Админ-Панели
API_ID = 35775411  # Ваш API ID
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"  # Ваш API HASH
TARGET_CHANNEL_URL = "@STAT_PRO1"
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальные хранилища для активных сессий Telethon и долгих задач
ACTIVE_TELETHON_CLIENTS = {}
ACTIVE_TELETHON_WORKERS = {}
# Для отслеживания задач, которые можно отменить (например, .флуд или .чекгруппу)
ACTIVE_LONG_TASKS = {} 

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, parse_mode='Markdown')
dp = Dispatcher(storage=storage)
user_router = Router()

# =========================================================================
# II. FSM-СОСТОЯНИЯ
# =========================================================================

class TelethonAuth(StatesGroup):
    """Состояния для процесса авторизации Telethon."""
    PHONE = State()
    CODE = State()
    PASSWORD = State()

class PromoStates(StatesGroup):
    """Состояния для активации промокода пользователем."""
    waiting_for_code = State()

class AdminStates(StatesGroup):
    """Состояния для Админ-панели."""
    main_menu = State()
    # Создание промокода
    creating_promo_code = State()
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

def db_get_monitor_logs(user_id, log_type):
    """Получает логи мониторинга по типу."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT timestamp, command, target FROM monitor_logs WHERE user_id=? AND type=? ORDER BY timestamp", (user_id, log_type))
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


# =========================================================================
# IV. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И КЛАВИАТУРА
# =========================================================================

def get_session_file_path(user_id: int):
    """Получает путь к файлу сессии Telethon."""
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
    
    # Кнопка для остановки сессии (если активна)
    if session_active:
        kb.append([InlineKeyboardButton(text="🛑 Остановить Сессию", callback_data="telethon_stop_session")])
    
    kb.append([InlineKeyboardButton(text=auth_text, callback_data=auth_callback)])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_monitor_menu_kb() -> InlineKeyboardMarkup:
    """Клавиатура меню мониторинга."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настроить IT-Чат", callback_data="monitor_set_it")],
        [InlineKeyboardButton(text="⚙️ Настроить DROP-Чат", callback_data="monitor_set_drop")],
        [InlineKeyboardButton(text="📋 Сгенерировать Отчет", callback_data="monitor_generate_report")],
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
    
    # Сначала пытаемся остановить старый, если он висит
    await stop_telethon_worker_for_user(user_id) 
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        # Проверяем, что файл сессии существует, иначе авторизация не прошла до конца
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
        # ID чатов должны быть сохранены в БД как строки, например '-1001234567890'
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
                 return # Игнорируем медиа и стикеры

            try:
                # Telethon event.chat_id возвращает числовой ID (отрицательный для каналов/групп)
                chat_id_str = str(event.chat_id) 
                message_text = event.message.text.strip()
                
                # IT Логирование (сравнение строковых ID)
                if it_chat_id_str and chat_id_str == it_chat_id_str:
                    for command, regex in IT_PATTERNS.items():
                        if re.match(regex, message_text, re.IGNORECASE | re.DOTALL):
                            db_add_monitor_log(user_id, 'IT', command, message_text)
                            logger.info(f"Logged IT command {command} for user {user_id}")
                            break
                
                # DROP Логирование (сравнение строковых ID)
                if drop_chat_id_str and chat_id_str == drop_chat_id_str:
                    if re.match(DROP_PATTERN_REGEX, message_text, re.IGNORECASE | re.DOTALL):
                         db_add_monitor_log(user_id, 'DROP', 'DROP_ENTRY', message_text)
                         logger.info(f"Logged DROP_ENTRY for user {user_id}")

            except Exception as e:
                logger.error(f"Ошибка в мониторинге Telethon для {user_id}: {e}")
                
        
        # --- ХЕНДЛЕРЫ ДЛЯ КОМАНД В ЛС TELETHON-АККАУНТА (Скелет) ---
        # Обработка команд, отправленных ВЛАДЕЛЬЦЕМ в ЛС своего Telethon-аккаунта
        @client.on(events.NewMessage(pattern=r'^\.(лс|флуд|стопфлуд|чекгруппу).*'))
        async def telethon_command_handler(event):
            
            # Проверяем, что сообщение пришло от самого user_id (владельца аккаунта)
            me = await client.get_me()
            if event.sender_id != me.id:
                 # Если команда пришла не от владельца (например, из группы или от другого пользователя), игнорируем
                 return
            
            # Если команда пришла в ЛС (private chat)
            if not event.is_private:
                return
            
            command = event.text.split()[0].lower()
            
            response_msg = f"✅ Команда {command} принята в работу. (Скелет)"
            
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
    except UserDeactivatedError:
        logger.warning(f"Аккаунт Telethon {user_id} деактивирован.")
        await bot.send_message(user_id, "⚠️ Аккаунт деактивирован. Переавторизуйтесь.", reply_markup=get_main_inline_kb(user_id))
    except asyncio.CancelledError:
        logger.info(f"Telethon Worker [{user_id}] отменен по запросу.")
    except Exception as e:
        logger.error(f"Критическая ошибка Telethon Worker [{user_id}]: {e}")
        error_text = f"❌ Критическая ошибка Telethon Worker: `{type(e).__name__}`. Требуется переавторизация."
        if isinstance(e, FloodWaitError):
             error_text = f"❌ Ошибка лимитов Telegram: Необходимо подождать {e.seconds} секунд."
        elif "AuthorizationKeyUnregistered" in str(e):
             error_text = "❌ Ключ авторизации недействителен. Возможно, сессия была завершена. Требуется переавторизация."
             
        await bot.send_message(user_id, error_text, reply_markup=get_main_inline_kb(user_id))
    finally:
        # Убедимся, что сессия очищена в памяти, статус в БД будет сброшен функцией stop_telethon_worker_for_user
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

# --- Главное меню и Промокоды ---

@user_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    """Обработчик команды /start и кнопки 'Назад'."""
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
    user_id = callback.from_user.id
    
    await stop_telethon_worker_for_user(user_id)
    
    # Обновляем клавиатуру, чтобы показать кнопку "Авторизовать"
    await callback.message.edit_text("🛑 **Telethon Worker остановлен.**\n\nДля возобновления работы нажмите 'Авторизовать Telethon'.", 
                                     reply_markup=get_main_inline_kb(user_id))
    await callback.answer("Сессия остановлена.")


# (Хендлеры для Промокодов и Админ-Панели остаются без изменений)
@user_router.callback_query(F.data == "start_promo_fsm")
async def start_promo_fsm(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса активации промокода через кнопку."""
    user_id = callback.from_user.id
    
    if db_check_subscription(user_id):
         await callback.answer("У вас уже есть активная подписка!", show_alert=True)
         return

    await state.set_state(PromoStates.waiting_for_code)
    
    await callback.message.edit_text(
        "🔑 **Активация промокода**\n\n"
        "Введите ваш промокод:"
    )
    await callback.answer()

@user_router.message(PromoStates.waiting_for_code)
async def activate_promo_fsm(message: types.Message, state: FSMContext):
    """Обработка ввода промокода."""
    user_id = message.from_user.id
    promo_code = message.text.strip().upper()
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT days, is_active, max_uses, current_uses FROM promo_codes WHERE code=?", (promo_code,))
    promo = cur.fetchone()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

    if not promo:
        await message.reply("❌ Неверный промокод.", reply_markup=keyboard)
        await state.clear()
        return

    days, is_active, max_uses, current_uses = promo
    if not is_active:
        await message.reply("❌ Промокод не активен.", reply_markup=keyboard)
        await state.clear()
        return
    if max_uses is not None and current_uses >= max_uses:
        await message.reply("❌ Промокод исчерпан.", reply_markup=keyboard)
        await state.clear()
        return

    # Активация подписки
    end_date = datetime.now() + timedelta(days=days)
    cur.execute("""
        UPDATE users SET subscription_active=1, subscription_end_date=?, promo_code=?
        WHERE user_id=?
    """, (end_date.strftime('%Y-%m-%d %H:%M:%S'), promo_code, user_id))
    cur.execute("UPDATE promo_codes SET current_uses=current_uses+1 WHERE code=?", (promo_code,))
    conn.commit()
    
    await message.reply(f"✅ Промокод **{promo_code}** активирован! Подписка на **{days}** дней.", reply_markup=get_main_inline_kb(user_id))
    await state.clear()

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_panel_start_handler(callback: types.CallbackQuery, state: FSMContext):
    """Главное меню Админ-панели."""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Доступ запрещен.", show_alert=True)
        return

    await state.set_state(AdminStates.main_menu)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать Промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="✍️ Выдать Подписку", callback_data="admin_manual_sub")],
        [InlineKeyboardButton(text="⬅️ В Главное Меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text("🛠️ **Админ-Панель**\n\nВыберите действие:", reply_markup=keyboard)
    await callback.answer()


@user_router.callback_query(F.data == "admin_create_promo", AdminStates.main_menu)
async def admin_create_promo_step1(callback: types.CallbackQuery, state: FSMContext):
    """Шаг 1: Запрос кода промокода."""
    await state.set_state(AdminStates.creating_promo_code)
    await callback.message.edit_text("🎁 Введите **уникальный код** для промокода (например, `FREE30`):",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel_start")]
                                     ]))
    await callback.answer()

@user_router.message(AdminStates.creating_promo_code)
async def admin_create_promo_step2(message: types.Message, state: FSMContext):
    """Шаг 2: Запрос количества дней."""
    promo_code = message.text.strip().upper()
    if not re.match(r'^[A-Z0-9]+$', promo_code):
         await message.reply("❌ Промокод должен состоять только из латинских букв и цифр (без пробелов).")
         return
         
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT code FROM promo_codes WHERE code=?", (promo_code,))
    if cur.fetchone():
        await message.reply("❌ Промокод с таким кодом уже существует. Попробуйте другой.")
        return

    await state.update_data(new_promo_code=promo_code)
    await state.set_state(AdminStates.creating_promo_days)
    await message.reply(f"Промокод `{promo_code}`.\nВведите **количество дней** подписки (например, `7`):",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel_start")]
                         ]))

@user_router.message(AdminStates.creating_promo_days)
async def admin_create_promo_step3(message: types.Message, state: FSMContext):
    """Шаг 3: Запрос количества использований."""
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError
    except ValueError:
        await message.reply("❌ Введите корректное число дней (больше 0).")
        return

    await state.update_data(new_promo_days=days)
    await state.set_state(AdminStates.creating_promo_uses)
    await message.reply("Введите **максимальное количество использований** (например, `10`).\nДля **бесконечного** использования введите `0` или `all`:",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel_start")]
                         ]))

@user_router.message(AdminStates.creating_promo_uses)
async def admin_create_promo_final(message: types.Message, state: FSMContext):
    """Шаг 4: Финализация создания промокода."""
    uses_input = message.text.strip().lower()
    max_uses = None
    if uses_input not in ('0', 'all'):
        try:
            max_uses = int(uses_input)
            if max_uses <= 0: raise ValueError
        except ValueError:
            await message.reply("❌ Введите корректное число использований, `0`, или `all`.")
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


@user_router.callback_query(F.data == "admin_manual_sub", AdminStates.main_menu)
async def admin_manual_sub_step1(callback: types.CallbackQuery, state: FSMContext):
    """Шаг 1: Запрос ID пользователя для ручной подписки."""
    await state.set_state(AdminStates.sub_target_user_id)
    await callback.message.edit_text("✍️ Введите **ID пользователя**, которому нужно выдать подписку:",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel_start")]
                                     ]))
    await callback.answer()

@user_router.message(AdminStates.sub_target_user_id)
async def admin_manual_sub_step2(message: types.Message, state: FSMContext):
    """Шаг 2: Запрос длительности подписки."""
    try:
        target_id = int(message.text.strip())
    except ValueError:
        await message.reply("❌ Введите корректный числовой ID пользователя.")
        return

    await state.update_data(target_user_id=target_id)
    await state.set_state(AdminStates.sub_duration_days)
    await message.reply(f"Пользователь ID `{target_id}`.\nВведите **количество дней** подписки (например, `30`):",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                             [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel_start")]
                         ]))

@user_router.message(AdminStates.sub_duration_days)
async def admin_manual_sub_final(message: types.Message, state: FSMContext):
    """Шаг 3: Финализация ручной выдачи подписки."""
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError
    except ValueError:
        await message.reply("❌ Введите корректное число дней (больше 0).")
        return

    data = await state.get_data()
    target_id = data['target_user_id']
    
    end_date = datetime.now() + timedelta(days=days)
    end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO users (user_id, subscription_active) VALUES (?, 0)
    """, (target_id,))
    cur.execute("""
        UPDATE users SET subscription_active=1, subscription_end_date=?, promo_code=NULL 
        WHERE user_id=?
    """, (end_date_str, target_id))
    conn.commit()

    await message.reply(f"✅ **Подписка выдана!**\n\n"
                        f"Пользователь: `{target_id}`\n"
                        f"Действует до: {end_date_str}",
                         reply_markup=get_main_inline_kb(message.from_user.id))
    
    try:
        await bot.send_message(target_id, f"🎉 Вам выдана подписка на **{days}** дней! Срок действия истекает {end_date_str}.")
    except Exception as e:
        logger.warning(f"Не удалось отправить уведомление пользователю {target_id}: {e}")
        await message.answer(f"⚠️ Не удалось уведомить пользователя `{target_id}`.", disable_notification=True)

    await state.clear()

# --- Хендлеры Telethon Авторизации ---

@user_router.callback_query(F.data == "telethon_auth_status")
async def telethon_status_handler(callback: types.CallbackQuery):
    """Обработчик нажатия на кнопку '🟢 Сессия активна'."""
    user_id = callback.from_user.id
    if user_id in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("Сессия Telethon активна и работает.", show_alert=True)
    else:
        # Если сессия неактивна в памяти, но кнопка активна - обновляем клавиатуру
        await callback.message.edit_reply_markup(reply_markup=get_main_inline_kb(user_id))
        await callback.answer("Сессия неактивна, требуется авторизация.", show_alert=True)


@user_router.callback_query(F.data == "telethon_auth_start")
async def telethon_auth_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса авторизации Telethon."""
    user_id = callback.from_user.id
    
    has_access, error_msg = await check_access(user_id, callback.bot)
    if not has_access:
        await callback.answer(error_msg, show_alert=True)
        return

    if user_id in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("Сессия уже активна. Перезапуск не требуется.", show_alert=True)
        return

    await state.set_state(TelethonAuth.PHONE)
    
    await callback.message.edit_text(
        "🔐 **Начало авторизации Telethon**\n\n"
        "Введите **номер телефона**, который вы хотите авторизовать в формате: `+79001234567` (обязательно с международным кодом)."
    )
    await callback.answer()


@user_router.message(TelethonAuth.PHONE)
async def telethon_auth_step_phone(message: Message, state: FSMContext):
    """Обработка ввода номера телефона."""
    user_id = message.from_user.id
    phone_number = message.text.strip()
    
    if not re.match(r'^\+\d{10,15}$', phone_number):
        await message.answer("❌ **Ошибка:** Введите номер телефона в корректном формате (например, `+79001234567`).")
        return
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        result = await client.send_code_request(phone_number)
            
        await state.update_data(phone_number=phone_number, phone_code_hash=result.phone_code_hash)
        
        await state.set_state(TelethonAuth.CODE)
        await message.answer(
            f"🔢 **Код подтверждения отправлен.**\n\n"
            f"⚠️ **ВАЖНО:** Код действителен всего 2 минуты. Введите **код** (цифры), который пришел вам в Telegram на номер `{phone_number}`."
        )
        
    except FloodWaitError as e:
        await message.answer(f"❌ **Проблема с лимитами:** Telegram требует подождать {e.seconds} секунд. Попробуйте позже.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при запросе кода для {user_id}: {e}")
        await message.answer(f"❌ **Критическая ошибка авторизации:** Не удалось отправить код. `{str(e)}`", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    finally:
        if client.is_connected():
            await client.disconnect()


@user_router.message(TelethonAuth.CODE)
async def telethon_auth_step_code(message: Message, state: FSMContext):
    """Обработка ввода кода подтверждения."""
    user_id = message.from_user.id
    code = message.text.strip()
    
    if not code.isdigit():
        await message.answer("❌ **Неверный формат:** Код должен состоять только из цифр.")
        return

    data = await state.get_data()
    phone_number = data.get('phone_number')
    phone_code_hash = data.get('phone_code_hash')

    if not phone_number or not phone_code_hash:
        await message.answer("❌ Ошибка FSM: Пожалуйста, начните авторизацию сначала (/start).")
        await state.clear()
        return

    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        await client.sign_in(phone=phone_number, code=code, phone_code_hash=phone_code_hash)
        
        # Если авторизация прошла без 2FA
        await client.disconnect()

        # Запускаем worker асинхронно
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except SessionPasswordNeededError:
        # Если требуется 2FA
        if client.is_connected():
            await client.disconnect() # Закрываем, чтобы заново открыть на следующем шаге
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔑 **Требуется двухфакторная аутентификация (2FA).**\n\nВведите ваш облачный пароль Telegram:")
        
    except Exception as e:
        error_msg = str(e)
        if "The code is invalid" in error_msg:
             error_text = "❌ **Неверный код:** Введенный код недействителен."
        elif "You have tried logging in too many times" in error_msg:
             error_text = "❌ **Слишком много попыток:** Превышено допустимое количество попыток ввода кода."
        else:
             error_text = f"❌ **Критическая ошибка:** Не удалось войти. `{type(e).__name__}`"
             logger.error(f"Ошибка при вводе кода для {user_id}: {e}")
             
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()


@user_router.message(TelethonAuth.PASSWORD)
async def telethon_auth_step_password(message: Message, state: FSMContext):
    """Обработка ввода облачного пароля (2FA)."""
    user_id = message.from_user.id
    password = message.text.strip()
    
    data = await state.get_data()
    phone_number = data.get('phone_number')
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        await client.sign_in(password=password)
        
        # Финализация: 2FA пройдена
        await client.disconnect()

        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except Exception as e:
        error_msg = str(e)
        if "The password is invalid" in error_msg:
             error_text = "❌ **Неверный пароль.** Попробуйте еще раз."
        else:
             error_text = f"❌ **Критическая ошибка 2FA:** Не удалось войти. `{type(e).__name__}`"
             logger.error(f"Ошибка при вводе пароля для {user_id}: {e}")
             
        await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        await state.clear()


# --- Хендлеры Мониторинга (Настройка и Отчеты) ---

async def generate_monitor_report(user_id: int, log_type: str) -> str:
    """Генерирует форматированный отчет из логов."""
    logs = db_get_monitor_logs(user_id, log_type)
    
    if not logs:
        return f"🤷‍♂️ Логи типа **{log_type}** пусты."

    report_title = "📊 IT-Лог Отчет" if log_type == 'IT' else "📊 DROP-Лог Отчет"
    report_text = f"**{report_title}** ({len(logs)} записей):\n\n"
    
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

@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    """Показывает меню мониторинга."""
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

# --- Настройка IT-Чата ---
@user_router.callback_query(F.data == "monitor_set_it")
async def monitor_set_it_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало настройки IT-чата."""
    await state.set_state(MonitorStates.waiting_for_it_chat_id)
    await callback.message.edit_text("⚙️ Введите **числовой ID** чата для IT-лога (начинается с `-100...`):",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="❌ Отмена", callback_data="show_monitor_menu")]
                                     ]))
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_it_chat_id)
async def monitor_set_it_final(message: types.Message, state: FSMContext):
    """Финализация настройки IT-чата."""
    user_id = message.from_user.id
    chat_id = message.text.strip()
    
    # Проверка формата: начинается с -100 и остальное цифры
    if not (chat_id.startswith('-100') and chat_id[1:].isdigit()):
        await message.reply("❌ Некорректный ID. Должен быть в формате `-1001234567890`.")
        return
        
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET it_chat_id=? WHERE user_id=?", (chat_id, user_id))
    conn.commit()
    
    await state.clear()
    await message.reply(f"✅ ID IT-Чата установлен на `{chat_id}`.\n\n"
                        "Не забудьте **перезапустить Telethon Worker** (кнопка 'Остановить Сессию', затем 'Авторизовать'), чтобы изменения вступили в силу.", 
                        reply_markup=get_main_inline_kb(user_id))

# --- Настройка DROP-Чата ---
@user_router.callback_query(F.data == "monitor_set_drop")
async def monitor_set_drop_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало настройки DROP-чата."""
    await state.set_state(MonitorStates.waiting_for_drop_chat_id)
    await callback.message.edit_text("⚙️ Введите **числовой ID** чата для DROP-лога (начинается с `-100...`):",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="❌ Отмена", callback_data="show_monitor_menu")]
                                     ]))
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_drop_chat_id)
async def monitor_set_drop_final(message: types.Message, state: FSMContext):
    """Финализация настройки DROP-чата."""
    user_id = message.from_user.id
    chat_id = message.text.strip()
    
    if not (chat_id.startswith('-100') and chat_id[1:].isdigit()):
        await message.reply("❌ Некорректный ID. Должен быть в формате `-1001234567890`.")
        return
        
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET drop_chat_id=? WHERE user_id=?", (chat_id, user_id))
    conn.commit()
    
    await state.clear()
    await message.reply(f"✅ ID DROP-Чата установлен на `{chat_id}`.\n\n"
                        "Не забудьте **перезапустить Telethon Worker** (кнопка 'Остановить Сессию', затем 'Авторизовать'), чтобы изменения вступили в силу.", 
                        reply_markup=get_main_inline_kb(user_id))

# --- Генерация Отчета ---
@user_router.callback_query(F.data == "monitor_generate_report")
async def monitor_generate_report_start(callback: types.CallbackQuery):
    """Запрос типа отчета."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="IT-Лог", callback_data="report_type_IT")],
        [InlineKeyboardButton(text="DROP-Лог", callback_data="report_type_DROP")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="show_monitor_menu")],
    ])
    await callback.message.edit_text("📋 Выберите, по какому типу логов сгенерировать отчет:", reply_markup=kb)
    await callback.answer()

@user_router.callback_query(F.data.startswith("report_type_"))
async def monitor_generate_report_type(callback: types.CallbackQuery):
    """Генерация и отправка отчета."""
    user_id = callback.from_user.id
    log_type = callback.data.split('_')[2] # 'IT' или 'DROP'
    
    await callback.message.edit_text(f"⏳ Генерирую отчет по логам типа **{log_type}**...")
    
    report_text = await generate_monitor_report(user_id, log_type)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗑️ Очистить {log_type}-Лог", callback_data=f"clear_log_{log_type}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="show_monitor_menu")],
    ])
    
    await callback.message.edit_text(report_text, reply_markup=kb)
    await callback.answer("Отчет сгенерирован.")

@user_router.callback_query(F.data.startswith("clear_log_"))
async def monitor_clear_log(callback: types.CallbackQuery):
    """Очистка логов."""
    user_id = callback.from_user.id
    log_type = callback.data.split('_')[2]
    
    db_clear_monitor_logs(user_id, log_type)
    
    await callback.message.edit_text(f"✅ Логи типа **{log_type}** успешно очищены.", 
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="⬅️ Назад", callback_data="show_monitor_menu")]
                                     ]))
    await callback.answer(f"Логи {log_type} очищены.")


# =========================================================================
# VII. ЗАПУСК БОТА
# =========================================================================

async def main():
    """Главная функция запуска бота."""
    logger.info("Запуск бота...")
    
    # Инициализация базы данных
    db_init()
    logger.info("База данных инициализирована.")
    
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
