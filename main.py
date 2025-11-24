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
from telethon import TelegramClient, events
from telethon.errors import UserDeactivatedError, FloodWaitError, SessionPasswordNeededError
from telethon.utils import get_display_name

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# Переменные окружения (замените на ваши данные)
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")
TARGET_CHANNEL_URL = "@STAT_PRO1"
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальные хранилища для активных сессий Telethon
ACTIVE_TELETHON_CLIENTS = {}
ACTIVE_TELETHON_WORKERS = {}
ACTIVE_LONG_TASKS = {} # Используется для задач, связанных с отчетами/мониторингом

storage = MemoryStorage()
# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)
user_router = Router() # Основной роутер

# =========================================================================
# II. FSM-СОСТОЯНИЯ
# =========================================================================

class MonitorStates(StatesGroup):
    """Состояния для процесса генерации отчетов."""
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()

class TelethonAuth(StatesGroup):
    """Состояния для процесса авторизации Telethon."""
    PHONE = State()
    CODE = State()
    PASSWORD = State()

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

def db_set_session_status(user_id: int, is_active: bool, hash_code: str = None):
    """Устанавливает статус Telethon-сессии в базе данных."""
    conn = get_db_connection()
    cur = conn.cursor()
    # Обновляем или вставляем пользователя, если он не существует
    cur.execute("""
        INSERT OR IGNORE INTO users (user_id, subscription_active, telethon_active) VALUES (?, 0, 0)
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
    """Проверяет доступ пользователя (админ, подписка, промокод)."""
    if user_id == ADMIN_ID:
        return True, ""
    
    # Пытаемся получить пользователя. Если не существует, создаем.
    user = db_get_user(user_id)
    if not user:
        db_set_session_status(user_id, False) # Вставит нового пользователя
        user = db_get_user(user_id) # Получаем свежие данные
        if not user: return False, "❌ Пользователь не найден."

    subscribed = db_check_subscription(user_id)
    promo_activated = bool(user.get('promo_code'))

    if not subscribed and not promo_activated:
        try:
            member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id) 
            if member.status not in ["member", "administrator", "creator"]:
                return False, f"❌ Подпишитесь на {TARGET_CHANNEL_URL} или активируйте промокод."
        except Exception:
            return False, f"❌ Подпишитесь на {TARGET_CHANNEL_URL} или активируйте промокод."
            
    return True, ""


def get_main_inline_kb(user_id: int) -> InlineKeyboardMarkup:
    """Генерирует главную инлайн-клавиатуру."""
    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    
    kb = [
        [InlineKeyboardButton(text="🔑 Промокод", callback_data="activate_promo")],
        [InlineKeyboardButton(text="📊 Отчеты и Мониторинг", callback_data="show_monitor_menu")],
    ]
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel")])

    auth_text = "🟢 Сессия активна" if session_active else "🔐 Авторизовать Telethon"
    auth_callback = "telethon_auth_status" if session_active else "telethon_auth_start"
    kb.append([InlineKeyboardButton(text=auth_text, callback_data=auth_callback)])
    return InlineKeyboardMarkup(inline_keyboard=kb)


# =========================================================================
# V. TELETHON WORKER
# =========================================================================

async def run_telethon_worker_for_user(user_id: int):
    """Запускает Telethon worker для конкретного пользователя."""
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    # Отмена старого воркера, если он есть
    if user_id in ACTIVE_TELETHON_WORKERS and ACTIVE_TELETHON_WORKERS[user_id]:
        ACTIVE_TELETHON_WORKERS[user_id].cancel()
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        await client.start()
        user_info = await client.get_me()
        logger.info(f"Telethon [{user_id}] запущен как: {get_display_name(user_info)}")
        
        db_set_session_status(user_id, True)
        await bot.send_message(user_id, "⚙️ Telethon Worker запущен и готов к работе!")

        # Пример хендлера Telethon (для команд в личных сообщениях с аккаунтом)
        @client.on(events.NewMessage(pattern=r'^\.(лс|флуд|стопфлуд|чекгруппу).*'))
        async def handler(event):
            # Тут будет логика команд Telethon.
            # Например, запись в monitor_logs:
            # db_add_log(user_id, 'IT', 'message', event.text)
            pass

        await client.run_until_disconnected()
    except UserDeactivatedError:
        logger.warning(f"Аккаунт Telethon {user_id} деактивирован.")
        db_set_session_status(user_id, False)
        await bot.send_message(user_id, "⚠️ Аккаунт деактивирован. Переавторизуйтесь.")
    except Exception as e:
        logger.error(f"Ошибка Telethon Worker [{user_id}]: {e}")
        db_set_session_status(user_id, False)
        await bot.send_message(user_id, f"❌ Ошибка Telethon Worker: `{e}`. Требуется переавторизация.")
    finally:
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
# VI. ХЕНДЛЕРЫ AIOGRAM (ГЛАВНОЕ МЕНЮ И ПРОМОКОДЫ)
# =========================================================================

@user_router.message(commands=["start"])
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(union: types.Message | types.CallbackQuery, state: FSMContext):
    """Обработчик команды /start и кнопки 'Назад'."""
    user_id = union.from_user.id
    keyboard = get_main_inline_kb(user_id)
    text = "Привет! Используйте меню ниже."
    
    await state.clear()
    
    if isinstance(union, types.Message):
        await union.answer(text, reply_markup=keyboard)
    else:
        try:
            await union.message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest:
            pass # Игнорируем, если текст не изменился
        await union.answer()


@user_router.callback_query(F.data == "activate_promo")
async def request_promo_code(callback: types.CallbackQuery):
    """Просто информационный ответ для кнопки 'Промокод'."""
    await callback.answer("Для активации промокода используйте команду: /promo КОД_ПРОМО", show_alert=True)


@user_router.message(F.text.startswith("/promo"))
async def activate_promo_command(message: types.Message):
    """Обработчик активации промокода."""
    user_id = message.from_user.id
    # Логика проверки доступа для активации промокода здесь не нужна, 
    # так как он предоставляет доступ.
    
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("❌ Используйте: /promo КОД_ПРОМО")
        return
    promo_code = parts[1]

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT days, is_active, max_uses, current_uses FROM promo_codes WHERE code=?", (promo_code,))
    promo = cur.fetchone()
    if not promo:
        await message.reply("❌ Неверный промокод.")
        return

    days, is_active, max_uses, current_uses = promo
    if not is_active:
        await message.reply("❌ Промокод не активен.")
        return
    if max_uses is not None and current_uses >= max_uses:
        await message.reply("❌ Промокод исчерпан.")
        return

    end_date = datetime.now() + timedelta(days=days)
    cur.execute("""
        UPDATE users SET subscription_active=1, subscription_end_date=?, promo_code=?
        WHERE user_id=?
    """, (end_date.strftime('%Y-%m-%d %H:%M:%S'), promo_code, user_id))
    cur.execute("UPDATE promo_codes SET current_uses=current_uses+1 WHERE code=?", (promo_code,))
    conn.commit()
    await message.reply(f"✅ Промокод активирован! Подписка на {days} дней.")
    
@user_router.callback_query(F.data == "admin_panel")
async def admin_panel_handler(callback: types.CallbackQuery):
    """Заглушка для Админ-панели."""
    if callback.from_user.id == ADMIN_ID:
        await callback.answer("🛠️ Админ-панель: Пока в разработке. Добавьте функционал здесь.", show_alert=True)
    else:
        await callback.answer("Доступ запрещен.", show_alert=True)


# =========================================================================
# VII. ХЕНДЛЕРЫ TELETHON АВТОРИЗАЦИИ
# =========================================================================

@user_router.callback_query(F.data == "telethon_auth_status")
async def telethon_status_handler(callback: types.CallbackQuery):
    """Обработчик нажатия на кнопку '🟢 Сессия активна'."""
    if callback.from_user.id in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("Сессия Telethon активна и работает.", show_alert=True)
    else:
        # Обновляем клавиатуру на случай, если статус изменился
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
            f"Введите **код** (цифры), который пришел вам в Telegram на номер `{phone_number}`."
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

        # Запускаем воркер в отдельной задаче
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
        await message.answer("✅ **Авторизация успешна!** Telethon-сессия активна.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔑 **Требуется двухфакторная аутентификация (2FA).**\n\nВведите ваш облачный пароль Telegram:")
        
    except Exception as e:
        error_msg = str(e)
        if 'The code is invalid' in error_msg:
             await message.answer("❌ **Неверный код.** Попробуйте еще раз.")
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

# =========================================================================
# VIII. ХЕНДЛЕРЫ ОТЧЕТОВ И МОНИТОРИНГА
# =========================================================================

@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu_handler(callback: types.CallbackQuery):
    """Показывает меню выбора отчетов."""
    user_id = callback.from_user.id
    can_access, msg = await check_access(user_id, callback.bot)
    if not can_access:
        await callback.answer(msg, show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Получить отчет IT", callback_data="get_report_IT")],
        [InlineKeyboardButton(text="Получить отчет DROP", callback_data="get_report_DROP")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])
    await callback.message.edit_text("Выберите тип отчета:", reply_markup=keyboard)
    await callback.answer()

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

    # Заглушка, чтобы не обрабатывать отчеты, пока не добавлена логика мониторинга
    logs = db_get_monitor_logs(user_id, monitor_type)
    if not logs:
        await callback.answer("⚠️ Логи пусты. Сначала запустите Telethon-команду для сбора данных.", show_alert=True)
        return

    start_msg = await callback.message.answer(f"Генерация отчета {monitor_type}... Введите ID или @username чата для отправки:")
    
    if monitor_type == 'IT':
        await state.set_state(MonitorStates.waiting_for_it_chat_id)
    else:
        await state.set_state(MonitorStates.waiting_for_drop_chat_id)
        
    await state.update_data(monitor_type=monitor_type, report_msg_id=start_msg.message_id)
    await callback.answer()

@user_router.message(MonitorStates.waiting_for_it_chat_id)
@user_router.message(MonitorStates.waiting_for_drop_chat_id)
async def process_chat_for_report(message: types.Message, state: FSMContext):
    """Обрабатывает ID чата и отправляет сгенерированный отчет через Telethon."""
    user_id = message.from_user.id
    data = await state.get_data()
    monitor_type = data['monitor_type']
    report_msg_id = data.get('report_msg_id')
    chat_id = message.text.strip()
    
    await state.clear() 

    if user_id not in ACTIVE_TELETHON_CLIENTS:
        try:
            await bot.edit_message_text(chat_id=user_id, message_id=report_msg_id,
                text="❌ Сессия Telethon неактивна.", reply_markup=get_main_inline_kb(user_id))
        except:
             await message.answer("❌ Сессия Telethon неактивна.", reply_markup=get_main_inline_kb(user_id))
        return

    logs = db_get_monitor_logs(user_id, monitor_type)
    if not logs:
        try:
            await bot.edit_message_text(chat_id=user_id, message_id=report_msg_id,
                text=f"⚠️ Логи {monitor_type} пусты.", reply_markup=get_main_inline_kb(user_id))
        except:
            await message.answer(f"⚠️ Логи {monitor_type} пусты.", reply_markup=get_main_inline_kb(user_id))
        return

    # Отправка временного сообщения о начале генерации (вместо редактирования)
    try:
        await bot.edit_message_text(chat_id=user_id, message_id=report_msg_id, text=f"⏳ Отправка отчета {monitor_type} в Telegram...")
    except:
        pass 

    try:
        client = ACTIVE_TELETHON_CLIENTS[user_id]
        
        chat_entity = await client.get_entity(chat_id)

        content = f"Отчет {monitor_type} (сгенерирован {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}):\n\n"
        for timestamp, command, target in logs:
            content += f"[{timestamp}] {command}: {target}\n"

        file_path = f"data/{monitor_type}_Report_{int(time.time())}.txt"
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        report_file = FSInputFile(file_path)
        
        await client.send_file(chat_entity, report_file, caption=f"Автоматический отчет {monitor_type}.")
        
        db_clear_monitor_logs(user_id, monitor_type)
        os.remove(file_path)

        await bot.edit_message_text(chat_id=user_id, message_id=report_msg_id,
            text=f"✅ Отчет {monitor_type} успешно отправлен в `{chat_id}`.",
            reply_markup=get_main_inline_kb(user_id))
            
    except Exception as e:
        logger.error(f"Ошибка отправки отчета: {e}")
        error_text = f"❌ Не удалось отправить отчет. Ошибка: {str(e)[:100]}..."
        try:
             await bot.edit_message_text(chat_id=user_id, message_id=report_msg_id,
                text=error_text, reply_markup=get_main_inline_kb(user_id))
        except:
            await message.answer(error_text, reply_markup=get_main_inline_kb(user_id))
        
    finally:
        # Удаляем сообщение пользователя с chat_id
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
    
    # Таблица users (добавлены telethon_active и telethon_hash)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            subscription_active INTEGER DEFAULT 0,
            subscription_end_date TEXT,
            promo_code TEXT,
            telethon_active INTEGER DEFAULT 0,
            telethon_hash TEXT 
        )
    """)
    
    # Таблица промокодов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            days INTEGER NOT NULL,
            is_active INTEGER DEFAULT 1,
            max_uses INTEGER,
            current_uses INTEGER DEFAULT 0
        )
    """)
    
    # Таблица логов мониторинга (для формирования отчетов)
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
    if 'telethon_active' not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN telethon_active INTEGER DEFAULT 0")
    if 'telethon_hash' not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN telethon_hash TEXT")
    
    conn.commit()
    conn.close()

async def on_startup():
    """Действия при запуске бота."""
    db_init()
    logger.info("База данных инициализирована.")
    
    # Логика для перезапуска Telethon-воркеров
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
    dp.include_router(user_router)     # Подключаем основной роутер
    
    await bot.delete_webhook(drop_pending_updates=True)
    await on_startup()
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
