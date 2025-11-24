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
from telethon.errors import UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, RPCError
from telethon.tl.types import PeerChannel
from telethon.utils import get_display_name

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# ВНИМАНИЕ: Значения переменных окружения взяты из вашего скриншота!
BOT_TOKEN = "7868097991:AAE745izKWA__gG20IxRoVpgQjnW_RMNjTo" 
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
# Для отслеживания задач, которые можно отменить (например, .флуд или .чекгруппу)
ACTIVE_LONG_TASKS = {} 

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
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
        end_date = datetime.strptime(user['subscription_end_date'], '%Y-%m-%d %H:%M:%S')
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

# =========================================================================
# IV. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И КЛАВИАТУРА
# =========================================================================

def get_session_file_path(user_id: int):
    """Получает путь к файлу сессии Telethon."""
    return os.path.join('data', f'session_{user_id}')

async def check_access(user_id: int, bot: Bot) -> tuple[bool, str]:
    """Проверяет доступ пользователя (админ, подписка, промокод, канал)."""
    if user_id == ADMIN_ID:
        return True, ""
    
    # 1. Проверка существования пользователя
    user = db_get_user(user_id)
    if not user:
        db_set_session_status(user_id, False) 
        user = db_get_user(user_id)
        if not user: return False, "❌ Пользователь не найден."

    # 2. Проверка подписки или промокода
    subscribed = db_check_subscription(user_id)
    promo_activated = bool(user.get('promo_code'))

    if subscribed or promo_activated:
        return True, "" # Доступ разрешен по подписке/промо
    
    # 3. Проверка подписки на канал
    try:
        member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id) 
        if member.status in ["member", "administrator", "creator"]:
             return True, ""
    except Exception as e:
        logger.error(f"Ошибка проверки подписки на канал для {user_id}: {e}")
        
    return False, f"❌ Для использования бота подпишитесь на канал {TARGET_CHANNEL_URL} или активируйте подписку."


def get_main_inline_kb(user_id: int) -> InlineKeyboardMarkup:
    """Генерирует главную инлайн-клавиатуру."""
    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    
    kb = [
        [InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")], # Изменено на FSM
        [InlineKeyboardButton(text="📊 Отчеты и Мониторинг", callback_data="show_monitor_menu")],
    ]
    if user_id == ADMIN_ID:
        # Для админа добавляем отдельную кнопку админ-панели
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])

    auth_text = "🟢 Сессия активна" if session_active else "🔐 Авторизовать Telethon"
    auth_callback = "telethon_auth_status" if session_active else "telethon_auth_start"
    kb.append([InlineKeyboardButton(text=auth_text, callback_data=auth_callback)])
    return InlineKeyboardMarkup(inline_keyboard=kb)


# =========================================================================
# V. TELETHON WORKER И КОМАНДЫ (Скелет)
# =========================================================================

async def run_telethon_worker_for_user(user_id: int):
    """Запускает Telethon worker для конкретного пользователя."""
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    if user_id in ACTIVE_TELETHON_WORKERS and ACTIVE_TELETHON_WORKERS[user_id]:
        ACTIVE_TELETHON_WORKERS[user_id].cancel()
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        await client.start()
        user_info = await client.get_me()
        logger.info(f"Telethon [{user_id}] запущен как: {get_display_name(user_info)}")
        
        db_set_session_status(user_id, True)
        await bot.send_message(user_id, "⚙️ Telethon Worker запущен и готов к работе! Теперь ваш аккаунт слушает команды в ЛС.")

        # Получаем настроенные чаты для логирования
        user_db = db_get_user(user_id)
        it_chat_id = user_db.get('it_chat_id')
        drop_chat_id = user_db.get('drop_chat_id')

        # --- ХЕНДЛЕРЫ ДЛЯ МОНИТОРИНГА И ЛОГИРОВАНИЯ ---
        
        async def monitor_handler(event, log_type: str, patterns: dict):
            """Обрабатывает сообщения в настроенных чатах мониторинга."""
            message = event.message.text
            for command, regex in patterns.items():
                if re.match(regex, message, re.IGNORECASE | re.DOTALL):
                    # Сохраняем логи
                    db_add_monitor_log(user_id, log_type, command, message)
                    logger.info(f"Logged {log_type} command {command} for user {user_id}")
                    break # Останавливаемся после первого совпадения

        IT_PATTERNS = {
            ".встал": r'^\.встал.*',
            ".кьар": r'^\.кьар.*',
            ".ошибка": r'^\.ошибка.*',
            ".замена": r'^\.замена.*',
            ".повтор": r'^\.повтор.*',
        }
        # Паттерн для Дроп-Лога (номер время @юзернейм бх) - пример: +79998887766 10:30 @user_name 15:00
        DROP_PATTERN_REGEX = r'^\+?\d{10,15}\s+\d{1,2}:\d{2}\s+@\w+\s+бх(?:\s+\d{1,2}:\d{2})?.*'
        DROP_PATTERNS = {"DROP_ENTRY": DROP_PATTERN_REGEX}


        @client.on(events.NewMessage)
        async def monitor_listener(event):
            # Проверка, что это группа/канал и сообщение - текст
            if not event.is_group and not event.is_channel:
                return

            try:
                chat_id_int = event.chat_id
                
                # IT Логирование
                if it_chat_id and str(chat_id_int) == it_chat_id.strip('-'):
                    await monitor_handler(event, 'IT', IT_PATTERNS)
                
                # DROP Логирование
                if drop_chat_id and str(chat_id_int) == drop_chat_id.strip('-'):
                    # Проверяем только DROP-паттерн
                    if re.match(DROP_PATTERN_REGEX, event.message.text, re.IGNORECASE | re.DOTALL):
                         db_add_monitor_log(user_id, 'DROP', 'DROP_ENTRY', event.message.text)
                         logger.info(f"Logged DROP_ENTRY for user {user_id}")
                
            except Exception as e:
                logger.error(f"Ошибка в мониторинге Telethon для {user_id}: {e}")
                
        # --- ХЕНДЛЕРЫ ДЛЯ КОМАНД В ЛС БОТА ---

        @client.on(events.NewMessage(chats=user_id, pattern=r'^\.(лс|флуд|стопфлуд|чекгруппу).*'))
        async def command_handler(event):
            """Обработка команд Telethon в личных сообщениях с аккаунтом."""
            command = event.text.split()[0].lower()
            
            # Отправка ответа в ЛС Telethon-аккаунта
            response_msg = f"✅ Команда {command} принята в работу."
            
            if command == '.лс':
                # .лс [текст] [список @юзернеймов/ID]
                response_msg = "Массовая рассылка (.лс) — Скелет реализации."
                # TODO: Логика массовой рассылки
            
            elif command == '.флуд':
                # .флуд [кол-во] [текст] [задержка_сек] [чат @юзернейм/ID]
                response_msg = "Флуд (.флуд) — Скелет реализации."
                # TODO: Логика флуда с созданием Task и записью в ACTIVE_LONG_TASKS
            
            elif command == '.стопфлуд':
                # .стопфлуд
                response_msg = "Остановка флуда (.стопфлуд) — Скелет реализации."
                # TODO: Логика остановки задачи из ACTIVE_LONG_TASKS
                
            elif command == '.чекгруппу':
                # .чекгруппу [чат @юзернейм/ID]
                response_msg = "Анализ группы (.чекгруппу) — Скелет реализации."
                # TODO: Логика анализа группы с созданием Task и записью в ACTIVE_LONG_TASKS
            
            await event.reply(response_msg)


        await client.run_until_disconnected()
    except UserDeactivatedError:
        logger.warning(f"Аккаунт Telethon {user_id} деактивирован.")
        db_set_session_status(user_id, False)
        await bot.send_message(user_id, "⚠️ Аккаунт деактивирован. Переавторизуйтесь.")
    except Exception as e:
        logger.error(f"Ошибка Telethon Worker [{user_id}]: {e}")
        db_set_session_status(user_id, False)
        # Более дружелюбное сообщение об ошибке
        error_text = f"❌ Критическая ошибка Telethon Worker: `{type(e).__name__}`. Требуется переавторизация."
        if isinstance(e, FloodWaitError):
             error_text = f"❌ Ошибка лимитов Telegram: Необходимо подождать {e.seconds} секунд."
        elif "AuthorizationKeyUnregistered" in str(e):
             error_text = "❌ Ключ авторизации недействителен. Возможно, сессия была завершена. Требуется переавторизация."
             
        await bot.send_message(user_id, error_text)
    finally:
        # Убедимся, что сессия очищена
        if client.is_connected():
            await client.disconnect()
        if user_id in ACTIVE_TELETHON_CLIENTS:
            del ACTIVE_TELETHON_CLIENTS[user_id]
        if user_id in ACTIVE_TELETHON_WORKERS:
             del ACTIVE_TELETHON_WORKERS[user_id]
        
        try:
            await bot.send_message(user_id, "❌ Telethon Worker остановлен.", reply_markup=get_main_inline_kb(user_id))
        except Exception:
            pass
        
        logger.info(f"Telethon Worker [{user_id}] остановлен.")


# =========================================================================
# VI. ХЕНДЛЕРЫ AIOGRAM
# =========================================================================

# --- Главное меню и FSM для Промокодов ---

@user_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    """Обработчик команды /start и кнопки 'Назад'."""
    user_id = union.from_user.id
    keyboard = get_main_inline_kb(user_id)
    
    # Проверка доступа для отображения актуального сообщения
    has_access, error_msg = await check_access(user_id, bot)
    text = "Привет! Используйте меню ниже."
    if not has_access and user_id != ADMIN_ID:
         text = error_msg

    await state.clear()
    
    if isinstance(union, types.Message):
        await union.answer(text, reply_markup=keyboard)
    else:
        try:
            await union.message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest:
            pass 
        await union.answer()


@user_router.callback_query(F.data == "start_promo_fsm")
async def start_promo_fsm(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса активации промокода через кнопку."""
    user_id = callback.from_user.id
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
    promo_code = message.text.strip()
    
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

    # Проверка, не активировал ли пользователь этот код уже (можно добавить в реальном приложении)
    
    end_date = datetime.now() + timedelta(days=days)
    cur.execute("""
        UPDATE users SET subscription_active=1, subscription_end_date=?, promo_code=?
        WHERE user_id=?
    """, (end_date.strftime('%Y-%m-%d %H:%M:%S'), promo_code, user_id))
    cur.execute("UPDATE promo_codes SET current_uses=current_uses+1 WHERE code=?", (promo_code,))
    conn.commit()
    
    await message.reply(f"✅ Промокод **{promo_code}** активирован! Подписка на **{days}** дней.", reply_markup=get_main_inline_kb(user_id))
    await state.clear()


# --- Хендлеры Админ-Панели ---

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
    
    # Оповещение пользователя о выдаче
    try:
        await bot.send_message(target_id, f"🎉 Вам выдана подписка на **{days}** дней! Срок действия истекает {end_date_str}.")
    except Exception as e:
        logger.warning(f"Не удалось отправить уведомление пользователю {target_id}: {e}")
        await message.answer(f"⚠️ Не удалось уведомить пользователя `{target_id}`.", disable_notification=True)

    await state.clear()


# --- Хендлеры Telethon Авторизации (с улучшенной ошибкой) ---

@user_router.callback_query(F.data == "telethon_auth_status")
async def telethon_status_handler(callback: types.CallbackQuery):
    """Обработчик нажатия на кнопку '🟢 Сессия активна'."""
    if callback.from_user.id in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("Сессия Telethon активна и работает.", show_alert=True)
    else:
        await callback.message.edit_reply_markup(reply_markup=get_main_inline_kb(callback.from_user.id))
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
    await client.connect()

    try:
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
    await client.connect()

    try:
        await client.sign_in(phone=phone_number, code=code, phone_code_hash=phone_code_hash)
        
        await client.disconnect()

        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔑 **Требуется двухфакторная аутентификация (2FA).**\n\nВведите ваш облачный пароль Telegram:")
        
    except Exception as e:
        error_msg = str(e)
        
        # Улучшенная обработка ошибок, включая "The confirmation code has expired"
        if 'The code is invalid' in error_msg:
             await message.answer("❌ **Неверный код.** Попробуйте еще раз.")
        elif 'The confirmation code has expired' in error_msg:
             # Это ошибка, о которой сообщил пользователь
             await message.answer("❌ **Код истёк!** Вы слишком долго вводили код. Пожалуйста, начните авторизацию сначала (/start).", reply_markup=get_main_inline_kb(user_id))
             await state.clear()
        else:
            logger.error(f"Ошибка при вводе кода для {user_id}: {e}")
            await message.answer(f"❌ **Критическая ошибка:** Не удалось авторизоваться. `{error_msg}`", reply_markup=get_main_inline_kb(user_id))
            await state.clear()

    finally:
        if client.is_connected():
            await client.disconnect()


@user_router.message(TelethonAuth.PASSWORD)
async def telethon_auth_step_password(message: Message, state: FSMContext):
    """Обработка ввода облачного пароля (2FA)."""
    user_id = message.from_user.id
    password = message.text.strip()
    data = await state.get_data()
    phone_number = data.get('phone_number')

    if not phone_number:
        await message.answer("❌ Ошибка FSM: Пожалуйста, начните авторизацию сначала (/start).")
        await state.clear()
        return

    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    await client.connect()

    try:
        await client.sign_in(password=password)
        
        await client.disconnect()

        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task

        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except Exception as e:
        error_msg = str(e)
        if 'Invalid password' in error_msg:
            await message.answer("❌ **Неверный пароль.** Попробуйте еще раз.")
        else:
            logger.error(f"Ошибка при вводе пароля для {user_id}: {e}")
            await message.answer(f"❌ **Критическая ошибка:** Не удалось авторизоваться. `{error_msg}`", reply_markup=get_main_inline_kb(user_id))
            await state.clear()

    finally:
        if client.is_connected():
            await client.disconnect()

# --- Хендлеры Настройки Мониторинга ---

@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu_handler(callback: types.CallbackQuery):
    """Показывает меню выбора отчетов и настроек мониторинга."""
    user_id = callback.from_user.id
    can_access, msg = await check_access(user_id, callback.bot)
    if not can_access:
        await callback.answer(msg, show_alert=True)
        return
    
    user_db = db_get_user(user_id)
    it_chat = user_db.get('it_chat_id') or "❌ Не настроен"
    drop_chat = user_db.get('drop_chat_id') or "❌ Не настроен"


    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настроить Чаты Логирования", callback_data="config_monitor_chats")],
        [InlineKeyboardButton(text="Получить отчет IT", callback_data="get_report_IT")],
        [InlineKeyboardButton(text="Получить отчет DROP", callback_data="get_report_DROP")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(
        f"**📊 Мониторинг и Отчеты**\n\n"
        f"IT Чат: `{it_chat}`\n"
        f"DROP Чат: `{drop_chat}`\n\n"
        f"Выберите действие:", 
        reply_markup=keyboard
    )
    await callback.answer()


@user_router.callback_query(F.data == "config_monitor_chats")
async def config_monitor_chats_start(callback: types.CallbackQuery, state: FSMContext):
    """Запускает FSM для настройки чатов логирования."""
    await state.set_state(MonitorStates.waiting_for_it_chat_id)
    
    await callback.message.edit_text(
        "**⚙️ Настройка Чатов Логирования**\n\n"
        "Шаг 1/2: Введите **ID или @username IT-чата**, в котором бот будет слушать команды (.встал, .кьар и т.д.)."
        "Если это приватный чат, используйте числовой ID (начинается с `-100`).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="show_monitor_menu")]
        ])
    )
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_it_chat_id)
async def config_monitor_it_chat(message: types.Message, state: FSMContext):
    """Сохранение IT-чата и запрос DROP-чата."""
    it_chat_id = message.text.strip()
    # Простая проверка формата (хотя Telethon проверит его при попытке залогиниться)
    if not (re.match(r'^@?\w+$', it_chat_id) or re.match(r'^-?\d+$', it_chat_id)):
         await message.reply("❌ Неверный формат ID/username. Используйте @username или числовой ID.")
         return
         
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET it_chat_id=? WHERE user_id=?", (it_chat_id, message.from_user.id))
    conn.commit()
    
    await state.set_state(MonitorStates.waiting_for_drop_chat_id)
    await message.reply(
        "Шаг 2/2: Введите **ID или @username DROP-чата**, в котором бот будет слушать заявки (по формату: `номер время @юзернейм бх`).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="show_monitor_menu")]
        ])
    )
    # Удаляем сообщение пользователя с ID чата
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except:
        pass


@user_router.message(MonitorStates.waiting_for_drop_chat_id)
async def config_monitor_drop_chat(message: types.Message, state: FSMContext):
    """Сохранение DROP-чата и завершение настройки."""
    drop_chat_id = message.text.strip()
    if not (re.match(r'^@?\w+$', drop_chat_id) or re.match(r'^-?\d+$', drop_chat_id)):
         await message.reply("❌ Неверный формат ID/username. Используйте @username или числовой ID.")
         return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET drop_chat_id=? WHERE user_id=?", (drop_chat_id, message.from_user.id))
    conn.commit()

    await message.answer("✅ **Настройка мониторинга завершена!**\n\nИзменения вступят в силу после перезапуска Telethon-воркера.",
                         reply_markup=get_main_inline_kb(message.from_user.id))
    await state.clear()
    
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except:
        pass


# --- Хендлеры Генерации Отчетов ---

@user_router.callback_query(F.data.startswith("get_report_"))
async def get_monitor_report(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса генерации отчета: запрашивает ID чата."""
    user_id = callback.from_user.id
    monitor_type = callback.data.split('_')[-1].upper()

    can_access, msg = await check_access(user_id, bot)
    if not can_access:
        await callback.answer(msg, show_alert=True)
        return

    if user_id not in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("❌ Сессия Telethon неактивна. Авторизуйтесь.", show_alert=True)
        return

    logs = db_get_monitor_logs(user_id, monitor_type)
    if not logs:
        await callback.answer(f"⚠️ Логи {monitor_type} пусты. Сначала запустите Telethon-команду для сбора данных.", show_alert=True)
        return

    await state.set_state(MonitorStates.waiting_for_report_chat_id)
    
    start_msg = await callback.message.edit_text(
        f"**Генерация отчета {monitor_type}**\n\n"
        "Введите **ID или @username целевого чата** для отправки отчета. "
        "Отчет будет отправлен в *General* топик, если это форум.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="show_monitor_menu")]
        ])
    )
        
    await state.update_data(monitor_type=monitor_type, report_msg_id=start_msg.message_id)
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_report_chat_id)
async def process_chat_for_report(message: types.Message, state: FSMContext):
    """Обрабатывает ID чата и отправляет сгенерированный отчет через Telethon."""
    user_id = message.from_user.id
    data = await state.get_data()
    monitor_type = data['monitor_type']
    report_msg_id = data.get('report_msg_id')
    chat_id = message.text.strip()
    
    await state.clear() 

    if user_id not in ACTIVE_TELETHON_CLIENTS:
        await message.answer("❌ Сессия Telethon неактивна.", reply_markup=get_main_inline_kb(user_id))
        return

    logs = db_get_monitor_logs(user_id, monitor_type)
    if not logs:
        await message.answer(f"⚠️ Логи {monitor_type} пусты.", reply_markup=get_main_inline_kb(user_id))
        return

    # Обновление сообщения
    try:
        await bot.edit_message_text(chat_id=user_id, message_id=report_msg_id, text=f"⏳ Отправка отчета {monitor_type} в Telegram...")
    except:
        pass 

    try:
        client = ACTIVE_TELETHON_CLIENTS[user_id]
        
        chat_entity = await client.get_entity(chat_id)
        
        # Определение topic_id (General топик всегда ID 1)
        topic_id = None
        # Проверка, является ли чат форумом (PeerChannel с флагом 'forum' доступен только в свежих версиях, 
        # но мы можем попытаться определить это по типу entity)
        # Для простоты в рамках этого примера, мы будем считать, что если entity является каналом, 
        # мы отправляем в General Topic ID 1 (если это вообще форум).
        if isinstance(chat_entity, PeerChannel) and getattr(chat_entity, 'forum', False):
             topic_id = 1
        # Telethon сам обычно обрабатывает форумы, если указать message_thread_id. 
        # Если не указывать, по умолчанию отправляет в General (ID 1). 
        # Для явного указания General Topic:
        if getattr(chat_entity, 'megagroup', False) and getattr(chat_entity, 'forum', False):
             topic_id = 1

        content = f"Отчет {monitor_type} (сгенерирован {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}):\n\n"
        for timestamp, command, target in logs:
            content += f"[{timestamp}] {command}: {target}\n"

        file_path = f"data/{monitor_type}_Report_{int(time.time())}.txt"
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        report_file = FSInputFile(file_path)
        
        # Отправка файла, используя topic_id (None, если не форум)
        await client.send_file(chat_entity, report_file, 
                               caption=f"Автоматический отчет {monitor_type} (Topic ID: {topic_id if topic_id else 'None'}).",
                               reply_to=topic_id)
        
        db_clear_monitor_logs(user_id, monitor_type)
        os.remove(file_path)

        await bot.edit_message_text(chat_id=user_id, message_id=report_msg_id,
            text=f"✅ Отчет **{monitor_type}** успешно отправлен в `{chat_id}`.",
            reply_markup=get_main_inline_kb(user_id))
            
    except RPCError as e:
        error_text = f"❌ Ошибка Telegram API (RPCError): Не удалось найти чат или нет доступа. `{e.full_name}`"
        try:
             await bot.edit_message_text(chat_id=user_id, message_id=report_msg_id,
                text=error_text, reply_markup=get_main_inline_kb(user_id))
        except:
             await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
    except Exception as e:
        logger.error(f"Ошибка отправки отчета: {e}")
        error_text = f"❌ Не удалось отправить отчет. Ошибка: {str(e)[:100]}..."
        try:
             await bot.edit_message_text(chat_id=user_id, message_id=report_msg_id,
                text=error_text, reply_markup=get_main_inline_kb(user_id))
        except:
            await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        
    finally:
        try:
            await message.delete()
        except:
            pass


# =========================================================================
# IX. ЗАПУСК БОТА
# =========================================================================

def db_init():
    """Инициализация базы данных и создание таблиц."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            subscription_active INTEGER DEFAULT 0,
            subscription_end_date TEXT,
            promo_code TEXT,
            telethon_active INTEGER DEFAULT 0,
            telethon_hash TEXT,
            it_chat_id TEXT,    -- ID/Username для IT-логов
            drop_chat_id TEXT   -- ID/Username для DROP-логов
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            days INTEGER NOT NULL,
            is_active INTEGER DEFAULT 1,
            max_uses INTEGER,
            current_uses INTEGER DEFAULT 0
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS monitor_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            timestamp TEXT,
            type TEXT, -- 'IT' or 'DROP'
            command TEXT,
            target TEXT
        )
    """)
    
    # Проверка и добавление столбцов для обратной совместимости
    cur.execute("PRAGMA table_info(users)")
    cols = [col[1] for col in cur.fetchall()]
    for col in ['telethon_active', 'telethon_hash', 'it_chat_id', 'drop_chat_id']:
        if col not in cols:
            cur.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")
            if 'active' in col:
                cur.execute(f"UPDATE users SET {col}=0")
    
    conn.commit()
    conn.close()

async def on_startup():
    """Действия при запуске бота."""
    db_init()
    logger.info("База данных инициализирована.")
    
    # Перезапуск Telethon-воркеров
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
    active_users = cur.fetchall()
    conn.close()
    
    for (user_id,) in active_users:
        logger.info(f"Перезапуск Telethon Worker для пользователя {user_id}...")
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task


async def main():
    dp.include_router(user_router)     
    
    await bot.delete_webhook(drop_pending_updates=True)
    await on_startup()
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
