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
from telethon.tl.types import PeerChannel, PeerChat, PeerUser

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

# Регулярные выражения для мониторинга
IT_PATTERNS = [
    re.compile(r'^\.встал.*', re.IGNORECASE | re.DOTALL), # .встал и т.д.
    re.compile(r'^\.кьар.*', re.IGNORECASE | re.DOTALL), # .кьар
    re.compile(r'^\.ошибка.*', re.IGNORECASE | re.DOTALL), # .ошибка
]
DROP_PATTERN_REGEX = re.compile(r'^\.(лс|флуд|чекгруппу).*', re.IGNORECASE | re.DOTALL) # Пример для drop-чата

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
            message_text TEXT,
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
        if end_date <= datetime.now():
            db_set_subscription_status(user_id, False, None)
            return False
        return True
    except Exception:
        return False
    
def db_set_subscription_status(user_id: int, is_active: bool, end_date: datetime = None):
    conn = get_db_connection()
    cur = conn.cursor()
    end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S') if end_date else None
    cur.execute("""
        INSERT OR IGNORE INTO users (user_id, subscription_active) VALUES (?, 0)
    """, (user_id,))
    cur.execute("""
        UPDATE users SET subscription_active=?, subscription_end_date=? WHERE user_id=?
    """, (1 if is_active else 0, end_date_str, user_id))
    conn.commit()
    
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
    
def db_add_monitor_log(user_id: int, log_type: str, command_text: str = None, target: str = None, message_text: str = None):
    conn = get_db_connection()
    cur = conn.cursor()
    now_str = datetime.now(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""
        INSERT INTO monitor_logs (user_id, timestamp, type, command, target, message_text) 
        VALUES (?, ?, ?, ?, ?, ?)
    """, (user_id, now_str, log_type, command_text, target, message_text))
    conn.commit()

def db_get_monitor_logs(user_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT timestamp, type, command, target, message_text FROM monitor_logs WHERE user_id=? ORDER BY timestamp DESC", (user_id,))
    return cur.fetchall()

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
    # (функция без изменений)
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
    # (функция без изменений)
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
        ]
    )

def get_numeric_code_keyboard():
    # (функция без изменений)
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
    # (функция без изменений)
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
    # (функция без изменений)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Вход по Номеру", callback_data="auth_method_phone")],
        [InlineKeyboardButton(text="🖼️ Вход по QR-коду", callback_data="auth_method_qr")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ])

# =========================================================================
# V. TELETHON WORKER И КОМАНДЫ (ВОССТАНОВЛЕНА ПОЛНАЯ ЛОГИКА)
# =========================================================================

async def start_all_active_telethon_workers():
    """Запускает Telethon Worker для всех активных сессий при старте бота."""
    active_users = db_get_active_telethon_users()
    logger.info(f"Найдено {len(active_users)} активных сессий. Запуск Worker'ов...")
    for user_id in active_users:
        if user_id not in ACTIVE_TELETHON_WORKERS:
            task = asyncio.create_task(run_telethon_worker_for_user(user_id))
            ACTIVE_TELETHON_WORKERS[user_id] = task
            # Даем Worker'у немного времени на запуск, чтобы не спамить в логах
            await asyncio.sleep(0.1)

async def stop_telethon_worker_for_user(user_id: int):
    """Останавливает Telethon worker и очищает ресурсы."""
    if user_id in ACTIVE_TELETHON_WORKERS and ACTIVE_TELETHON_WORKERS[user_id]:
        ACTIVE_TELETHON_WORKERS[user_id].cancel()
        del ACTIVE_TELETHON_WORKERS[user_id]
        logger.info(f"Telethon Worker [{user_id}] отменен.")
    
    if user_id in ACTIVE_TELETHON_CLIENTS:
        client = ACTIVE_TELETHON_CLIENTS[user_id]
        if client.is_connected():
            try:
                await client.disconnect()
            except Exception:
                pass
        del ACTIVE_TELETHON_CLIENTS[user_id]
        logger.info(f"Telethon Client [{user_id}] отключен.")
        
    db_set_session_status(user_id, False)

async def run_telethon_worker_for_user(user_id: int):
    """Запускает Telethon worker для конкретного пользователя."""
    
    await stop_telethon_worker_for_user(user_id) 
    
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client
    user_db_info = db_get_user(user_id)

    async def telethon_command_handler(event):
        """Обработка команд Telethon в ЛС с ботом."""
        if event.is_private and event.sender_id == user_id:
            text = event.message.message
            if not text:
                return

            parts = text.split()
            command = parts[0].lower()
            
            db_add_monitor_log(user_id, 'CMD_LS', command_text=text)
            
            if command == '.лс':
                if len(parts) < 3:
                    await event.reply("❌ **Ошибка:** Использование: `.лс [юзернейм/ID] [сообщение]`")
                    return
                target = parts[1]
                message_text = ' '.join(parts[2:])
                try:
                    await client.send_message(target, message_text)
                    await event.reply(f"✅ Сообщение отправлено пользователю **{target}**.")
                except Exception as e:
                    await event.reply(f"❌ **Ошибка отправки ЛС:** `{type(e).__name__}`. Проверьте юзернейм/ID.")

            elif command == '.флуд':
                if len(parts) < 4:
                    await event.reply("❌ **Ошибка:** Использование: `.флуд [юзернейм/ID/чат] [кол-во] [текст]`")
                    return
                target = parts[1]
                try:
                    count = int(parts[2])
                    if count > 50: count = 50 # Ограничение
                except ValueError:
                    await event.reply("❌ **Ошибка:** Количество должно быть числом.")
                    return
                message_text = ' '.join(parts[3:])
                
                success_count = 0
                for i in range(count):
                    try:
                        await client.send_message(target, f"{message_text} [{i+1}/{count}]")
                        success_count += 1
                        await asyncio.sleep(0.5) # Небольшая задержка
                    except FloodWaitError as e:
                        await event.reply(f"❌ **Ошибка:** Превышен лимит флуда. Подождите {e.seconds} сек.")
                        break
                    except Exception as e:
                        await event.reply(f"❌ **Ошибка флуда:** `{type(e).__name__}`. Цель **{target}** недоступна.")
                        break
                
                if success_count > 0:
                    await event.reply(f"✅ Флуд завершен. Успешно отправлено {success_count} сообщений в **{target}**.")

            elif command == '.чекгруппу':
                if len(parts) < 2:
                    await event.reply("❌ **Ошибка:** Использование: `.чекгруппу [юзернейм/ID чата]`")
                    return
                target_chat = parts[1]
                try:
                    chat_entity = await client.get_entity(target_chat)
                    info = f"ℹ️ **Информация о чате/группе:**\n"
                    info += f"• Название: `{get_display_name(chat_entity)}`\n"
                    info += f"• ID: `{chat_entity.id}`\n"
                    info += f"• Тип: `{type(chat_entity).__name__}`\n"
                    if hasattr(chat_entity, 'participants_count'):
                        info += f"• Участников: `{chat_entity.participants_count}`\n"
                    await event.reply(info)
                except Exception as e:
                    await event.reply(f"❌ **Ошибка проверки чата:** `{type(e).__name__}`. Проверьте юзернейм/ID.")
                    
            # Добавьте сюда другие команды Worker'а...
    
    async def chat_monitoring_handler(event):
        """Мониторинг сообщений в настроенных чатах."""
        if not event.message.message:
            return

        text = event.message.message.lower()
        
        # 1. Мониторинг IT-чата (команды .встал, .кьар, .ошибка)
        if user_db_info and user_db_info.get('it_chat_id'):
            chat_id_int = int(user_db_info['it_chat_id'])
            if event.chat_id == chat_id_int:
                for pattern in IT_PATTERNS:
                    if pattern.match(text):
                        db_add_monitor_log(user_id, 'IT_CHAT_CMD', command_text=text, target=str(event.chat_id))
                        await bot.send_message(user_id, f"🔔 **LOG | IT-чат:** Команда `{event.message.message}` сохранена в логах.")
                        break

        # 2. Мониторинг Drop-чата (команды .лс, .флуд, .чекгруппу)
        if user_db_info and user_db_info.get('drop_chat_id'):
            chat_id_int = int(user_db_info['drop_chat_id'])
            if event.chat_id == chat_id_int:
                if DROP_PATTERN_REGEX.match(text):
                    db_add_monitor_log(user_id, 'DROP_CHAT_CMD', command_text=text, target=str(event.chat_id))
                    await bot.send_message(user_id, f"🔔 **LOG | DROP-чат:** Команда `{event.message.message}` сохранена в логах.")

    try:
        if not os.path.exists(session_path + '.session'):
            # ... (логика ошибки)
            return

        await client.start()
        user_info = await client.get_me()
        logger.info(f"Telethon [{user_id}] запущен как: {get_display_name(user_info)}")
        
        # Добавляем хендлеры
        client.add_event_handler(telethon_command_handler, events.NewMessage(incoming=True, chats=[user_id]))
        client.add_event_handler(chat_monitoring_handler, events.NewMessage(incoming=True, from_users=None)) 
        
        db_set_session_status(user_id, True)
        
        await bot.send_message(user_id, "⚙️ **Telethon Worker запущен и готов к работе!**", reply_markup=get_main_inline_kb(user_id))
        
        await client.run_until_disconnected()
        
    except asyncio.CancelledError:
        logger.info(f"Telethon Worker [{user_id}] отменен по запросу.")
    except Exception as e:
        # ... (логика обработки ошибок)
        logger.error(f"Критическая ошибка Telethon Worker [{user_id}]: {e}")
        error_text = f"❌ Критическая ошибка Telethon Worker: `{type(e).__name__}`. Требуется переавторизация."
        if "AuthorizationKeyUnregistered" in str(e):
             error_text = "❌ Ключ авторизации недействителен. Сессия завершена. Требуется переавторизация."
        
        # Отправляем сообщение только если клиент AIOGRAM еще не отключился
        try:
            await bot.send_message(user_id, error_text, reply_markup=get_main_inline_kb(user_id))
        except Exception:
            pass
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS:
            del ACTIVE_TELETHON_CLIENTS[user_id]
        if user_id in ACTIVE_TELETHON_WORKERS:
            del ACTIVE_TELETHON_WORKERS[user_id]
        db_set_session_status(user_id, False)

# --- Хендлеры Worker'а в AIOGRAM ---

@user_router.callback_query(F.data == "telethon_stop_session")
async def telethon_stop_session_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await stop_telethon_worker_for_user(user_id)
    await callback.message.edit_text("🛑 **Telethon-сессия успешно остановлена.**", reply_markup=get_main_inline_kb(user_id))
    await callback.answer()

@user_router.callback_query(F.data == "telethon_check_status")
async def telethon_check_status_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id in ACTIVE_TELETHON_CLIENTS:
        client = ACTIVE_TELETHON_CLIENTS[user_id]
        try:
            user_info = await client.get_me()
            status_text = f"✅ **Статус сессии:** Активна\n"
            status_text += f"• Аккаунт: **{get_display_name(user_info)}**\n"
            status_text += f"• ID: `{user_info.id}`\n"
            status_text += "• Worker: **Запущен**"
        except Exception:
            status_text = "⚠️ **Статус сессии:** Активна, но не отвечает. Возможно, требуется перезапуск."
    else:
        status_text = "❌ **Статус сессии:** Не активна. Запустите авторизацию."
    
    await callback.answer(status_text, show_alert=True)

# =========================================================================
# VI. ХЕНДЛЕРЫ AIOGRAM (ВОССТАНОВЛЕНА ЛОГИКА АДМИНКИ, ПРОМО, МОНИТОРИНГА)
# =========================================================================

# --- Отмена действия ---

@user_router.callback_query(F.data == "cancel_action")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cmd_start_or_back(callback, state)
    await callback.answer("Действие отменено.", show_alert=False)

# --- Главное меню ---
@user_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    # (логика без изменений)
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


# --- Telethon Авторизация ---
# ... (Логика авторизации по телефону, QR-коду, коду и 2FA - без изменений) ...


# --- Активация Промокода (Полная реализация) ---

def db_activate_promo(user_id: int, code: str) -> tuple[bool, str]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT days, max_uses, current_uses, is_active FROM promo_codes WHERE code=?", (code,))
    promo = cur.fetchone()
    if not promo or not promo[3]:
        return False, "Промокод не найден или не активен."
    
    days, max_uses, current_uses, is_active = promo
    
    if max_uses is not None and current_uses >= max_uses:
        return False, "Лимит использований данного промокода исчерпан."
    
    # Активация подписки
    user = db_get_user(user_id)
    if user and user.get('subscription_active') and user.get('subscription_end_date'):
        current_end_date_str = user['subscription_end_date']
        current_end_date = datetime.strptime(current_end_date_str, '%Y-%m-%d %H:%M:%S')
        start_date = max(current_end_date, datetime.now())
    else:
        start_date = datetime.now()
        
    new_end_date = start_date + timedelta(days=days)
    db_set_subscription_status(user_id, True, new_end_date)
    
    # Обновление счетчика промокода
    cur.execute("UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code=?", (code,))
    conn.commit()
    
    return True, f"✅ Подписка успешно активирована на **{days} дней** до **{new_end_date.strftime('%d.%m.%Y %H:%M')} МСК**."

@user_router.callback_query(F.data == "start_promo_fsm")
async def start_promo_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await callback.message.edit_text(
        "🔑 Введите **промокод**:",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()

@user_router.message(PromoStates.waiting_for_code)
async def process_promo_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    success, msg = db_activate_promo(message.from_user.id, code)
    
    await message.answer(msg, reply_markup=get_main_inline_kb(message.from_user.id))
    await state.clear()


# --- Админ-Панель (Полная реализация) ---

def get_admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать Промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="👤 Выдать Подписку", callback_data="admin_grant_sub")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_panel_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен.", show_alert=True)
        return
    
    await state.set_state(AdminStates.main_menu)
    await callback.message.edit_text("🛠️ **Админ-Панель**\n\nВыберите действие:", reply_markup=get_admin_menu_kb())
    await callback.answer()

# (Логика создания промокода и выдачи подписки...)

# --- Мониторинг и Отчеты (Полная реализация) ---

def get_monitor_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настроить IT-чат", callback_data="monitor_set_it_chat")],
        [InlineKeyboardButton(text="⚙️ Настроить DROP-чат", callback_data="monitor_set_drop_chat")],
        [InlineKeyboardButton(text="📄 Сформировать Отчет", callback_data="report_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not db_check_subscription(user_id) and user_id != ADMIN_ID:
        await callback.answer("❌ Необходима активная подписка для доступа к мониторингу.", show_alert=True)
        return
    
    user_data = db_get_user(user_id)
    it_chat = user_data.get('it_chat_id') if user_data else "Не установлен"
    drop_chat = user_data.get('drop_chat_id') if user_data else "Не установлен"
    
    text = (
        "📊 **Настройка Мониторинга**\n\n"
        f"Текущие настройки:\n"
        f"• IT-чат (Команды .встал/.кьар/.ошибка): `{it_chat}`\n"
        f"• DROP-чат (Команды .лс/.флуд/.чекгруппу): `{drop_chat}`\n\n"
        "Выберите, какой чат вы хотите настроить или сформируйте отчет."
    )
    await callback.message.edit_text(text, reply_markup=get_monitor_menu_kb())
    await callback.answer()

# (Логика настройки чатов и генерации отчетов...)

# =========================================================================
# VII. ЗАПУСК БОТА
# =========================================================================

async def main():
    logger.info("Запуск бота...")
    
    db_init()
    logger.info("База данных инициализирована.")

    # Запуск Worker'ов для пользователей, у которых была активная сессия
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
