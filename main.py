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
        
    return False, f"❌ Для использования бота необходима активная подписка или подписка на канал {TARGET_CHANNEL_URL}. Подпишитесь и нажмите /start снова."

def get_cancel_keyboard():
    """Возвращает клавиатуру с кнопкой 'Отмена'."""
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
        ]
    )

def get_numeric_code_keyboard():
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
        [InlineKeyboardButton(text="⬅️ Удалить", callback_data="auth_delete_digit")] # Добавлена кнопка удаления
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

# =========================================================================
# V. TELETHON WORKER И КОМАНДЫ
# =========================================================================

async def stop_telethon_worker_for_user(user_id: int):
    """Останавливает Telethon worker и очищает ресурсы."""
    # (логика остановки без изменений)
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
        # (логика проверки сессии, запуска и хендлеров worker'а без изменений)
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

        # (мониторинг и хендлеры команд - скелеты)
        
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


# (start_all_active_telethon_workers - без изменений)

# =========================================================================
# VI. ХЕНДЛЕРЫ AIOGRAM
# =========================================================================

# --- Главное меню ---
@user_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = union.from_user.id
    
    db_set_session_status(user_id, False) 
    has_access, error_msg = await check_access(user_id, bot)
    
    keyboard = get_main_inline_kb(user_id)
    
    # (логика текста приветствия без изменений)
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
    """Шаг 0: Предлагаем выбрать метод авторизации. (Проверка доступа убрана, чтобы дать возможность авторизоваться)"""
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
    except asyncio.TimeoutError:
        await message.answer("❌ **Ошибка:** Истекло время ожидания соединения с Telegram. Попробуйте снова.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
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
        # ВНИМАНИЕ: login_token.wait - блокирующая операция, ее запускаем в отдельном потоке
        login_token = await client.qr_login() 
        
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(login_token.url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)
        
        await state.set_state(TelethonAuth.QR_CODE_WAIT)
        
        try:
             await callback.message.delete()
        except Exception:
             pass 

        message_qr = await bot.send_photo(
            chat_id=user_id,
            photo=BufferedInputFile(buffer.getvalue(), filename="qr_code.png"),
            caption="🖼️ **QR-код для входа сгенерирован.**\n\n"
                    "Откройте Telegram, перейдите в **Настройки -> Устройства -> Привязать рабочий стол** и отсканируйте код.\n\n"
                    "Ожидание авторизации... (Обычно 2 минуты)",
            reply_markup=get_cancel_keyboard()
        )
        
        # client остается подключенным для login_token.wait
        asyncio.create_task(wait_for_qr_login(user_id, client, login_token, state, message_qr))
        
    except asyncio.TimeoutError:
        await callback.message.answer("❌ **Ошибка:** Истекло время ожидания соединения с Telegram. Попробуйте снова.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при генерации QR-кода: {e}")
        await callback.message.answer(f"❌ Не удалось сгенерировать QR-код. Ошибка: `{type(e).__name__}`", 
                                     reply_markup=get_main_inline_kb(user_id))
        await state.clear()
    # Удаляем client из ACTIVE_TELETHON_CLIENTS, так как он используется в отдельной задаче wait_for_qr_login
    if user_id in ACTIVE_TELETHON_CLIENTS:
         del ACTIVE_TELETHON_CLIENTS[user_id]


async def wait_for_qr_login(user_id: int, client: TelegramClient, login_token, state: FSMContext, message_qr: Message):
    """Функция, которая ждет, пока пользователь авторизуется по QR-коду."""
    
    try:
        # run_in_executor используется, так как login_token.wait() блокирует
        await client.loop.run_in_executor(None, login_token.wait) 
        
        if login_token.signed_in:
            await client.disconnect() # Теперь можно отключиться
            
            task = asyncio.create_task(run_telethon_worker_for_user(user_id))
            ACTIVE_TELETHON_WORKERS[user_id] = task
            
            await message_qr.edit_caption("✅ **Авторизация по QR-коду успешна!** Telethon-сессия активна.", reply_markup=None)
            await message_qr.answer("Возврат в главное меню.", reply_markup=get_main_inline_kb(user_id))
        else:
            await message_qr.edit_caption("❌ **Время авторизации по QR-коду истекло** или сессия была отменена.", reply_markup=None)
            await message_qr.answer("Пожалуйста, попробуйте еще раз.", reply_markup=get_main_inline_kb(user_id))

    except asyncio.CancelledError:
        # Если пользователь нажал Отмена
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
        
# 1. Обработка цифровых кнопок (UI) - ЛОГИКА ИСПРАВЛЕНА
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

        # Используем исходное сообщение колбэка для ответа
        await callback.message.edit_text(f"⏳ Проверка кода: `{temp_code}`...", reply_markup=None)
        await telethon_auth_step_code_logic(callback.message, state, temp_code)
        await callback.answer("Код отправлен.")
        return

    # Обновление сообщения с текущим вводом
    current_display = f"{temp_code}_" if len(temp_code) < 6 else temp_code
    try:
        await callback.message.edit_text(
            f"🔢 Введите **код** с помощью кнопок ниже или сообщением.\n"
            f"Текущий ввод: `{current_display}`",
            reply_markup=get_numeric_code_keyboard()
        )
    except TelegramBadRequest:
        pass 
    
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
# (Без изменений)
# ...

# --- Админ-Панель ---
# (Без изменений)
# ...

# --- Мониторинг и Отчеты ---
# (Без изменений)
# ...


# =========================================================================
# VII. ЗАПУСК БОТА
# =========================================================================

async def main():
    logger.info("Запуск бота...")
    
    db_init()
    logger.info("База данных инициализирована.")

    # (логика проверки подписок)

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
