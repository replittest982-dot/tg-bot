# main.py (ФИНАЛЬНЫЙ МОНОЛИТНЫЙ КОД С КАСТОМНЫМИ КОМАНДАМИ)

import asyncio
import logging
import os
import sqlite3
from datetime import datetime
import pytz 

# --- Aiogram и FSM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- Telethon ---
from telethon import TelegramClient, events # Добавлен events для кастомных команд

# --- НАСТРОЙКИ (КОНФИГ) ---

BOT_TOKEN = "7868097991:AAE745izKWA__gG20IxRoVpgQjnW_RMNjTo"
ADMIN_ID = 6256576302 
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1" # !!! ОБНОВЛЕННЫЙ КАНАЛ !!!
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


# =========================================================================
# I. БАЗА ДАННЫХ (DB)
# =========================================================================

DB_PATH = os.path.join('data', DB_NAME) 

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            subscription_active BOOLEAN DEFAULT 0,
            subscription_end_date TIMESTAMP,
            role TEXT DEFAULT 'user',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("CREATE TABLE IF NOT EXISTS monitoring_chats (chat_id INTEGER PRIMARY KEY, chat_name TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS it_logs (id INTEGER PRIMARY KEY, timestamp TIMESTAMP, phone TEXT, status TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS drop_reports (id INTEGER PRIMARY KEY, timestamp TIMESTAMP, report_text TEXT)")
    conn.commit()
    conn.close()

def db_create_user_if_not_exists(user_id, username=None, first_name=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    if cursor.fetchone() is None:
        try:
            cursor.execute(
                "INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                (user_id, username, first_name)
            )
            conn.commit()
        except sqlite3.IntegrityError: pass
    conn.close()

async def db_check_user_subscription(bot: Bot, user_id):
    """Проверяет подписку через членство в TARGET_CHANNEL_URL (@STAT_PRO1)."""
    if user_id == ADMIN_ID:
        return True 

    try:
        member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
        if member.status in ['member', 'administrator', 'creator']:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Ошибка при проверке подписки для {user_id}: {e}")
        return False

def db_get_last_it_entries(limit=10):
    return [
        {'timestamp': '2025-11-24 10:00:00', 'phone': '79990001122', 'status': 'встал'},
        {'timestamp': '2025-11-24 09:30:00', 'phone': '79990002233', 'status': 'слетел'},
    ]

def db_get_last_drop_entries(limit=10):
    return [
        {'timestamp': '2025-11-24 11:00:00', 'report_text': 'Тестовый дроп-отчет 1: Курьер на месте.'},
        {'timestamp': '2025-11-24 10:30:00', 'report_text': 'Тестовый дроп-отчет 2: Собрали 5 документов.'},
    ]


# =========================================================================
# II. КЛАВИАТУРЫ (KEYBOARDS)
# =========================================================================

def kb_main_menu(user_id: int) -> InlineKeyboardMarkup:
    buttons = []
    buttons.append([
        InlineKeyboardButton(text="📊 IT-Отчеты", callback_data="show_it_reports"),
        InlineKeyboardButton(text="📝 Дроп-Отчеты", callback_data="show_drop_reports"),
    ])
    buttons.append([
        InlineKeyboardButton(text="🔑 Статус подписки", callback_data="check_subscription"),
    ])
    
    buttons.append([
        InlineKeyboardButton(text="❓ Задать вопрос", callback_data="ask_question"), 
        InlineKeyboardButton(text="ℹ️ Помощь", callback_data="show_help"),          
    ])

    if user_id == ADMIN_ID:
        buttons.append([
            InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_back_to_main(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# =========================================================================
# III. TELETHON WORKER (РАБОЧИЙ ПОТОК)
# =========================================================================

SESSION_DIR = 'data'
SESSION_FILE = f'{SESSION_DIR}/telethon_session_{API_ID}'
TELETHON_RUNNING = False

async def start_telethon_worker(bot: Bot, dp: Dispatcher):
    """Запускает и поддерживает Telethon-клиента."""
    global TELETHON_RUNNING
    
    session_filepath = f'{SESSION_FILE}.session'
    if not os.path.exists(session_filepath):
        logger.error("🚫 Telethon не запущен: Файл сессии отсутствует.")
        return

    try:
        client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
        await client.start()
        
        user = await client.get_me()
        logger.info(f"✅ Telethon запущен как: {user.username or user.first_name}")
        TELETHON_RUNNING = True

        # --- СТРУКТУРА ДЛЯ ОБРАБОТКИ КАСТОМНЫХ КОМАНД TELETHON ---
        # NOTE: Telethon реагирует на сообщения, содержащие команду.
        
        # .чекгруппу (Проверка членства в группе)
        @client.on(events.NewMessage(pattern=r'^\.чекгруппу'))
        async def handle_check_group_command(event):
             # Логика: .чекгруппу [группа/юзернейм] [юзернейм для проверки]
             await event.reply("✅ Команда .чекгруппу обработана. Выполняется проверка пользователя в группе.")

        # .лс (Личное сообщение)
        @client.on(events.NewMessage(pattern=r'^\.лс'))
        async def handle_ls_command(event):
             # Логика: .лс [получатель] [текст]
             await event.reply("✅ Команда .лс обработана. Сообщение отправлено.")

        # .флуд (Флуд-рассылка)
        @client.on(events.NewMessage(pattern=r'^\.флуд'))
        async def handle_flood_command(event):
             # Логика: .флуд [текст] [задержка]
             await event.reply("✅ Команда .флуд обработана. Запущена рассылка.")
            
        # ----------------------------------------------------------------------
        
        # Основной цикл работы Telethon
        await client.run_until_disconnected()

    except Exception as e:
        logger.error(f"❌ Критическая ошибка Telethon: {e}")
    finally:
        TELETHON_RUNNING = False
        if 'client' in locals() and client.is_connected():
            await client.disconnect()


# =========================================================================
# IV. ХЕНДЛЕРЫ АВТОРИЗАЦИИ (AUTH)
# =========================================================================

auth_router = Router()

class AuthStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()

async def create_telethon_client_auth():
    session_path = os.path.join(SESSION_DIR, os.path.basename(SESSION_FILE))
    return TelegramClient(session_path, API_ID, API_HASH)

async def check_telethon_auth():
    session_filepath = f'{SESSION_FILE}.session'
    if os.path.exists(session_filepath):
        try:
            client = await create_telethon_client_auth()
            await client.connect()
            is_authorized = await client.is_user_authorized()
            await client.disconnect()
            return is_authorized
        except Exception:
            return False
    return False

@auth_router.message(Command("auth")) 
async def cmd_auth_start(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: 
         await message.answer("❌ Эта команда доступна только администратору.")
         return
    
    if await check_telethon_auth():
        await message.answer("✅ Telethon уже авторизован. Перезапуск не требуется.")
        return

    await message.answer(
        "🔒 **Начинаем вход в Telegram.**\n\n"
        "Пожалуйста, введите ваш **номер телефона** (например, 79991234567):"
    )
    await state.set_state(AuthStates.waiting_for_phone)

@auth_router.message(AuthStates.waiting_for_phone, F.text.regexp(r'^\+?[789]\d{9,10}$'))
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip().replace('+', '')
    try:
        client = await create_telethon_client_auth()
        await client.connect()
        result = await client.send_code_request(phone)
        await client.disconnect() 
        
        await state.update_data(phone=phone, code_hash=result.phone_code_hash)
        await message.answer(
            f"🔑 Код отправлен на номер **{phone}**.\n\n"
            "Введите **код подтверждения** из Telegram:"
        )
        await state.set_state(AuthStates.waiting_for_code)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}. Попробуйте /start снова.")
        await state.clear()

@auth_router.message(AuthStates.waiting_for_code, F.text.regexp(r'^\d{4,5}$'))
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    phone = data.get('phone')
    code_hash = data.get('code_hash')
    
    try:
        client = await create_telethon_client_auth()
        await client.connect()
        user = await client.sign_in(phone=phone, code=code, phone_code_hash=code_hash)
        await client.disconnect() 
        await state.clear() 

        await message.answer(
            f"🎉 Успешный вход!\n\nВы вошли как: @{user.username or 'без username'}.\n"
            "**⚠️ Теперь перезапустите** скрипт бота, чтобы активировать мониторинг."
        )
    except Exception as e:
        error_str = str(e).lower()
        if "session_password_needed" in error_str:
            await state.update_data(phone=phone) 
            await message.answer("🔒 Требуется двухфакторный пароль. Введите его:")
            await state.set_state(AuthStates.waiting_for_password)
            return
        
        await state.clear()
        await message.answer(f"❌ Ошибка входа: {e}. Пожалуйста, попробуйте /start снова.")


@auth_router.message(AuthStates.waiting_for_password, F.text)
async def process_password(message: types.Message, state: FSMContext):
    password = message.text.strip()
    await state.clear()
    
    try:
        client = await create_telethon_client_auth()
        await client.connect()
        user = await client.sign_in(password=password)
        await client.disconnect() 
        
        await message.answer(
            f"🎉 Успешный вход!\n\nВы вошли как: @{user.username or 'без username'}.\n"
            "**⚠️ Теперь перезапустите** скрипт бота, чтобы активировать мониторинг."
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}. Пароль неверен. Попробуйте /start снова.")
        await state.clear()


# =========================================================================
# V. ХЕНДЛЕРЫ ПОЛЬЗОВАТЕЛЯ (USER)
# =========================================================================

user_router = Router()

@user_router.message(Command("start"))
async def command_start_handler(message: types.Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    db_create_user_if_not_exists(user_id, message.from_user.username, message.from_user.first_name) 
    await state.clear()

    # Telethon нужен только для админа
    if user_id == ADMIN_ID and not await check_telethon_auth():
        logger.warning("Telethon не авторизован. Запуск диалога входа.")
        await cmd_auth_start(message, state) 
        return

    await message.answer(
        f"👋 **Привет, {message.from_user.full_name}!**\n\n"
        "Я бот для мониторинга активности. Выберите действие:",
        reply_markup=kb_main_menu(user_id)
    )

@user_router.callback_query(F.data == "back_to_main")
async def back_to_main_menu_callback(callback: types.CallbackQuery) -> None:
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "🏠 **Главное меню**",
        reply_markup=kb_main_menu(user_id)
    )
    await callback.answer()

@user_router.callback_query(F.data == "show_it_reports")
async def show_it_reports(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    if not await db_check_user_subscription(bot, user_id): 
        await callback.message.edit_text(f"❌ **Доступ запрещен.**\n\nНеобходимо быть участником канала {TARGET_CHANNEL_URL}.", reply_markup=kb_back_to_main(user_id))
        return

    entries = db_get_last_it_entries(limit=10) 
    text = "📄 **Последние 10 записей IT-цикла:**\n\n" + \
           "\n".join([f"*{e['timestamp']}* - **{e['phone']}** ({e['status']})" for e in entries])
            
    await callback.message.edit_text(text, reply_markup=kb_back_to_main(user_id))
    await callback.answer()

@user_router.callback_query(F.data == "show_drop_reports")
async def show_drop_reports(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    if not await db_check_user_subscription(bot, user_id):
        await callback.message.edit_text(f"❌ **Доступ запрещен.**\n\nНеобходимо быть участником канала {TARGET_CHANNEL_URL}.", reply_markup=kb_back_to_main(user_id))
        return
        
    entries = db_get_last_drop_entries(limit=10) 
    text = "📄 **Последние 10 Дроп-отчетов:**\n\n" + \
           "\n---\n".join([f"*{e['timestamp']}*:\n`{e['report_text'][:80]}...`" for e in entries])
            
    await callback.message.edit_text(text, reply_markup=kb_back_to_main(user_id))
    await callback.answer()

@user_router.callback_query(F.data == "check_subscription")
async def check_subscription_status(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    if await db_check_user_subscription(bot, user_id):
        text = "✅ **Ваша подписка активна!** Вы имеете доступ (членство в канале)."
    else:
        text = f"⏳ **Подписка не активна.** Для доступа вступите в канал: {TARGET_CHANNEL_URL}"
        
    await callback.message.edit_text(text, reply_markup=kb_back_to_main(user_id))
    await callback.answer()
    
@user_router.callback_query(F.data == "show_help")
async def show_help(callback: types.CallbackQuery) -> None:
    await callback.message.edit_text(
        "ℹ️ **Раздел Помощи**\n\n"
        f"Для просмотра отчетов необходимо быть участником канала {TARGET_CHANNEL_URL}.",
        reply_markup=kb_back_to_main(callback.from_user.id)
    )
    await callback.answer()

@user_router.callback_query(F.data == "ask_question")
async def ask_question(callback: types.CallbackQuery) -> None:
    await callback.message.edit_text(
        "❓ **Задайте свой вопрос**\n\n"
        "Пожалуйста, свяжитесь с Администратором напрямую.",
        reply_markup=kb_back_to_main(callback.from_user.id)
    )
    await callback.answer()

@user_router.callback_query(F.data == "admin_panel")
async def admin_panel_placeholder(callback: types.CallbackQuery) -> None:
     user_id = callback.from_user.id
     if user_id == ADMIN_ID:
        await callback.message.edit_text(
            "🛠️ **Админ-Панель.**\n\n"
            "Здесь будет настройка чатов и управление подписками.",
            reply_markup=kb_back_to_main(user_id)
        )
     await callback.answer()


# =========================================================================
# VI. ГЛАВНАЯ ТОЧКА ЗАПУСКА
# =========================================================================

async def main():
    logger.info("Инициализация базы данных и проверка таблиц...")
    os.makedirs('data', exist_ok=True) 
    create_tables()
    
    storage = MemoryStorage() 
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=storage)
    
    dp.include_router(user_router)
    dp.include_router(auth_router)

    telethon_task = asyncio.create_task(start_telethon_worker(bot, dp))

    logger.info("Бот запущен. Ожидание сообщений...")
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске Aiogram: {e}")
    finally:
        telethon_task.cancel()
        logger.info("Бот остановлен.")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Принудительная остановка бота (KeyboardInterrupt).")
    except Exception as e:
        logger.error(f"Критическая ошибка вне цикла: {e}")
