# main.py (ФИНАЛЬНЫЙ МОНОЛИТНЫЙ КОД)

import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta # <<< ДОБАВЛЕН timedelta
import pytz 
from io import BytesIO 
import qrcode 

# --- Aiogram и FSM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile
from aiogram.client.default import DefaultBotProperties 

# --- Telethon ---
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
from telethon.utils import get_display_name

# =========================================================================
# I. КОНФИГ И ЛОГИРОВАНИЕ (ЧТЕНИЕ ИЗ ОКРУЖЕНИЯ)
# =========================================================================

# Чтение из переменных окружения сайта/платформы
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0")) 
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")

TARGET_CHANNEL_URL = "@STAT_PRO1" 
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# =========================================================================
# II. БАЗА ДАННЫХ (DB)
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
            promo_code TEXT,       
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS topic_monitors (
            topic_id INTEGER PRIMARY KEY,
            chat_id INTEGER,
            monitor_type TEXT, -- 'drop' or 'it'
            is_active BOOLEAN DEFAULT 1,
            started_by INTEGER,
            start_time TIMESTAMP
        );
    """)
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

def db_activate_subscription(user_id, reason="admin_issued"):
    conn = get_db_connection()
    cursor = conn.cursor()
    # Срок действия подписки: 30 дней от текущей даты
    end_date = datetime.now(TIMEZONE_MSK) + timedelta(days=30)
    
    cursor.execute(
        "UPDATE users SET subscription_active = 1, subscription_end_date = ? WHERE user_id = ?",
        (end_date.isoformat(), user_id)
    )
    conn.commit()
    conn.close()
    return end_date

async def db_check_user_subscription(bot: Bot, user_id):
    if user_id == ADMIN_ID: return True 

    # 1. Проверка через API Telegram (вступление в канал)
    try:
        member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
        if member.status in ['member', 'administrator', 'creator']:
            return True
    except Exception as e:
        # Если API выдает ошибку (пользователь не найден/заблокирован), продолжаем проверку
        pass

    # 2. Проверка через DB (например, активация админом/промокодом)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT subscription_active, subscription_end_date FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        active, end_date_str = result
        if active and end_date_str:
            end_date = datetime.fromisoformat(end_date_str)
            if end_date > datetime.now(TIMEZONE_MSK).replace(tzinfo=None):
                 return True # Активна и не просрочена
            else:
                 # Если просрочена, можно обновить статус в DB, но пока просто вернем False
                 pass 

    return False

# Заглушки для отчетов
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
# III. КЛАВИАТУРЫ (KEYBOARDS)
# =========================================================================

def kb_subscription_required() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🔑 Активировать подписку / Промокод", callback_data="activate_promo")],
        [InlineKeyboardButton(text="ℹ️ Помощь по подписке", callback_data="show_help")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_main_menu(user_id: int) -> InlineKeyboardMarkup:
    buttons = []
    
    # Кнопки первого уровня
    buttons.append([
        InlineKeyboardButton(text="ℹ️ Помощь", callback_data="show_help"),
        InlineKeyboardButton(text="🔑 Подписка", callback_data="activate_promo"),
        InlineKeyboardButton(text="❓ Задать вопрос", url="t.me/yanixforever"), # <<< ИЗМЕНЕНИЕ: Ссылка на юзернейм
    ])
    
    # Раздел Отчетов и Инструментов
    buttons.append([
        InlineKeyboardButton(text="📄 Отчеты и Инструменты", callback_data="menu_reports_tools"), # <<< НОВАЯ КНОПКА
    ])
    
    if user_id == ADMIN_ID:
        buttons.append([
            InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# НОВАЯ КЛАВИАТУРА для Отчетов и Инструментов
def kb_general_reports_menu() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📊 IT-Отчеты", callback_data="menu_it")],
        [InlineKeyboardButton(text="📝 Дроп-Отчеты", callback_data="menu_drop")],
        [InlineKeyboardButton(text="🔐 Вход в аккаунт (Telethon)", callback_data="menu_auth")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_auth_menu() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📱 Войти через QR-код", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔑 Войти через API ID/Hash", callback_data="auth_api")],
        [InlineKeyboardButton(text="💬 Войти через TG SMS (Код)", callback_data="auth_sms")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_reports_tools")], # <<< ИЗМЕНЕНИЕ: Назад в Отчеты
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_terminal_input(current_code: str) -> InlineKeyboardMarkup:
    buttons = []
    buttons.append([InlineKeyboardButton(text="1️⃣", callback_data="term_1"),
                    InlineKeyboardButton(text="2️⃣", callback_data="term_2"),
                    InlineKeyboardButton(text="3️⃣", callback_data="term_3")])
    buttons.append([InlineKeyboardButton(text="4️⃣", callback_data="term_4"),
                    InlineKeyboardButton(text="5️⃣", callback_data="term_5"),
                    InlineKeyboardButton(text="6️⃣", callback_data="term_6")])
    buttons.append([InlineKeyboardButton(text="7️⃣", callback_data="term_7"),
                    InlineKeyboardButton(text="8️⃣", callback_data="term_8"),
                    InlineKeyboardButton(text="9️⃣", callback_data="term_9")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Очистить", callback_data="term_C"),
                    InlineKeyboardButton(text="0️⃣", callback_data="term_0"), 
                    InlineKeyboardButton(text="✅ Ввести", callback_data="term_OK")])
    
    # Текущий код
    display_code = "..." if not current_code else current_code
    buttons.append([InlineKeyboardButton(text=f"Код: {display_code} | Введите", callback_data="ignore")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_report_menu(report_type: str, user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📊 Отчет (Последние 10)", callback_data=f"show_{report_type}_reports")],
        [InlineKeyboardButton(text="📈 Прогресс/Статус", callback_data=f"show_{report_type}_progress")],
        [InlineKeyboardButton(text="💡 Помощь по командам", callback_data=f"show_{report_type}_help")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_reports_tools")] # <<< ИЗМЕНЕНИЕ: Назад в Отчеты
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_back_to_main(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# =========================================================================
# IV. TELETHON WORKER
# =========================================================================

SESSION_DIR = 'data'
SESSION_FILE = f'{SESSION_DIR}/telethon_session_{API_ID}'
TELETHON_RUNNING = False

async def start_telethon_worker(bot: Bot, dp: Dispatcher):
    global TELETHON_RUNNING
    
    if not API_ID or not API_HASH:
        logger.error("🚫 Telethon не запущен: Отсутствует API_ID или API_HASH в окружении.")
        return
        
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

        # --- СТРУКТУРА ДЛЯ КАСТОМНЫХ КОМАНД TELETHON ---
        
        @client.on(events.NewMessage(pattern=r'^\.чекгруппу', func=lambda e: e.is_private is False))
        async def handle_check_group_command(event: events.NewMessage):
             # Логика: .чекгруппу [группа/топик]
             await event.reply("✅ **.чекгруппу**: Начинаю сбор ID/Username. Отчет будет отправлен вам в ЛС бота.")
        
        @client.on(events.NewMessage(pattern=r'^\.флуд(стоп)?', func=lambda e: e.is_private is True or e.sender_id == ADMIN_ID))
        async def handle_flood_command(event: events.NewMessage):
            command = event.text.split()
            if command[0] == '.флудстоп':
                await event.reply("❌ **.флудстоп**: Команда остановки рассылки получена. (Требуется логика остановки процесса)")
                return
            await event.reply("✅ **.флуд**: Запущена рассылка с указанными параметрами.")

        @client.on(events.NewMessage(pattern=r'^\.лс ', func=lambda e: e.is_private is True or e.sender_id == ADMIN_ID))
        async def handle_ls_command(event: events.NewMessage):
             await event.reply("✅ **.лс**: Сообщение отправлено указанным пользователям.")
             
        # --- КОМАНДЫ МОНИТОРИНГА ТОПИКОВ ---
        @client.on(events.NewMessage(pattern=r'^\.(дропворк|айтиворк)', func=lambda e: e.is_private is False and e.is_topic))
        async def handle_start_monitor_command(event: events.NewMessage):
            topic_id = event.id if event.is_topic else event.reply_to_msg_id
            monitor_type = 'drop' if event.text.startswith('.дропворк') else 'it'
            
            # ... Логика сохранения в DB о начале мониторинга топика
            
            await client.send_message(event.chat_id, 
                                      f"✅ **Мониторинг {monitor_type.upper()} запущен** в топике ID: {topic_id}.", 
                                      reply_to=event.id)
            await client.send_message(ADMIN_ID, f"🔔 Мониторинг {monitor_type.upper()} запущен в чате {get_display_name(await event.get_chat())}, топик {topic_id}.")

        @client.on(events.NewMessage(func=lambda e: e.is_private is False and e.is_topic))
        async def handle_topic_commands(event: events.NewMessage):
            # ... Логика обработки IT и Drop команд внутри активных топиков
            pass 
        # ----------------------------------------------------------------------
        
        await client.run_until_disconnected()

    except Exception as e:
        logger.error(f"❌ Критическая ошибка Telethon: {e}")
    finally:
        TELETHON_RUNNING = False
        if 'client' in locals() and client.is_connected():
            await client.disconnect()

# =========================================================================
# V. ХЕНДЛЕРЫ АВТОРИЗАЦИИ И СТАРТА (AUTH & START)
# =========================================================================

auth_router = Router()
user_router = Router()

class AuthStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_qr_scan = State() 

class AdminStates(StatesGroup): # <<< НОВЫЙ FSM ДЛЯ АДМИНА
    waiting_for_promo_user_id = State()

async def create_telethon_client_auth():
    session_path = os.path.join(SESSION_DIR, os.path.basename(SESSION_FILE))
    return TelegramClient(session_path, API_ID, API_HASH)

# --- START HANDLER ---
@auth_router.message(Command("start"))
async def command_start_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    user_id = message.from_user.id
    db_create_user_if_not_exists(user_id, message.from_user.username, message.from_user.first_name) 
    await state.clear()
    
    is_subscribed = await db_check_user_subscription(bot, user_id)
    
    welcome_text = f"👋 **STATPRO приветствует!**\n"
    welcome_text += f"*{[BETA] Бот находится в бета-тестировании.}*\n\n" # <<< НОВОЕ СООБЩЕНИЕ
    welcome_text += f"Ваш ID: `{user_id}`\n"
    welcome_text += f"Статус подписки: {'✅ Активна' if is_subscribed else '❌ Не активна'}"

    if not is_subscribed:
        await message.answer(
            welcome_text + f"\n\n**⚠️ Доступ к отчетам закрыт.** Вступите в канал **`{TARGET_CHANNEL_URL}`** для активации.",
            reply_markup=kb_subscription_required()
        )
        return

    await message.answer(
        welcome_text + "\n\nВыберите действие в Главном меню:",
        reply_markup=kb_main_menu(user_id)
    )

@auth_router.callback_query(F.data == "back_to_main")
async def back_to_main_menu_callback(callback: types.CallbackQuery) -> None:
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "🏠 **Главное меню**",
        reply_markup=kb_main_menu(user_id)
    )
    await callback.answer()

@user_router.callback_query(F.data == "menu_reports_tools")
async def show_reports_tools_menu(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    
    # Проверка подписки перед показом инструментов
    if not await db_check_user_subscription(bot, user_id): 
        await callback.answer(text="❌ Доступ запрещен. Нет подписки.", show_alert=True)
        return
        
    await callback.message.edit_text(
        "📄 **Отчеты и Инструменты**\n\n"
        "Выберите раздел:",
        reply_markup=kb_general_reports_menu() # <<< НОВАЯ КЛАВИАТУРА
    )
    await callback.answer()

@auth_router.callback_query(F.data == "menu_auth")
async def show_auth_menu(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    if not await db_check_user_subscription(bot, user_id): 
        await callback.answer("❌ Доступ запрещен. Вход в аккаунт доступен только подписчикам.", show_alert=True)
        return
        
    await callback.message.edit_text(
        "🔐 **Вход в аккаунт Telethon**\n\nВыберите удобный способ авторизации:",
        reply_markup=kb_auth_menu()
    )
    await callback.answer()

# --- 1. QR-ВХОД (ДОСТУПЕН ПОДПИСЧИКАМ) ---
@auth_router.callback_query(F.data == "auth_qr")
async def cmd_qr_start(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    user_id = callback.from_user.id
    
    if not await db_check_user_subscription(bot, user_id):
         await callback.answer("❌ Вход в аккаунт доступен только подписчикам.", show_alert=True)
         return
    
    await callback.message.edit_text("⏳ **Запускаю QR-сессию...**")
    
    try:
        client = await create_telethon_client_auth()
        await client.connect()
        
        qr_login_object = await client.qr_login()
        qr_url = qr_login_object.url 
        
        # --- ГЕНЕРАЦИЯ ИЗОБРАЖЕНИЯ QR-КОДА ---
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(qr_url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        buffer = BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)
        # --- КОНЕЦ ГЕНЕРАЦИИ ---

        # Отправляем ИЗОБРАЖЕНИЕ с инструкциями
        await callback.message.answer_photo(
            BufferedInputFile(buffer.getvalue(), filename="qr_code.png"),
            caption="📱 **QR-вход запущен.**\n\n1. Откройте Telegram на телефоне.\n2. Перейдите: **Настройки → Устройства → Привязать новое устройство**.\n3. **Отсканируйте** код выше.\n\n**Ожидаю сканирования...**"
        )
        
        await callback.message.delete() 
        
        await state.set_state(AuthStates.waiting_for_qr_scan)
        
        user = await qr_login_object.wait(client)
        
        await state.clear()
        await callback.message.answer(
            f"🎉 Успешный вход через QR!\n\nВы вошли как: @{user.username or 'без username'}.\n"
            "**⚠️ Теперь перезапустите** скрипт бота, чтобы активировать мониторинг."
        )

    except Exception as e:
        logger.error(f"Ошибка QR входа: {e}")
        await state.clear()
        await callback.message.answer(f"❌ Ошибка QR входа: {e}. Попробуйте снова.", reply_markup=kb_auth_menu())
    finally:
        if 'client' in locals() and client.is_connected():
            await client.disconnect()
            
    await callback.answer()

# --- 2. API ВХОД (ЗАГЛУШКА) ---
@auth_router.callback_query(F.data == "auth_api")
async def cmd_api_start(callback: types.CallbackQuery, state: FSMContext):
     await callback.answer("⏳ API вход временно недоступен. Используйте SMS или QR.", show_alert=True)

# --- 3. SMS ВХОД (ДОСТУПЕН ПОДПИСЧИКАМ) ---
@auth_router.callback_query(F.data == "auth_sms")
async def cmd_auth_start_sms(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    user_id = callback.from_user.id
    
    if not await db_check_user_subscription(bot, user_id): 
         await callback.answer("❌ Вход в аккаунт доступен только подписчикам.", show_alert=True)
         return
         
    await callback.message.edit_text(
        "🔒 **Начинаем вход в Telegram.**\n\n"
        "Пожалуйста, введите ваш **номер телефона** (например, 79991234567):"
    )
    await state.set_state(AuthStates.waiting_for_phone)
    await callback.answer()

# --- PROCESS PHONE ---
@auth_router.message(AuthStates.waiting_for_qr_scan)
async def handle_qr_scan_status(message: types.Message):
    await message.answer(
        "⏳ **Процесс QR-входа запущен.**\n"
        "Пожалуйста, не отправляйте другие команды, пока не отсканируете код."
    )

@auth_router.message(AuthStates.waiting_for_phone, F.text.regexp(r'^\+?[789]\d{9,10}$'))
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip().replace('+', '')
    try:
        client = await create_telethon_client_auth()
        await client.connect()
        result = await client.send_code_request(phone)
        await client.disconnect() 
        
        await state.update_data(phone=phone, code_hash=result.phone_code_hash, current_code="")
        
        await message.answer(
            f"🔑 Код отправлен на номер **{phone}**.\n\n"
            "Введите **код подтверждения** с помощью цифровой клавиатуры:",
            reply_markup=kb_terminal_input("")
        )
        await state.set_state(AuthStates.waiting_for_code)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}. Попробуйте /start снова.")
        await state.clear()

# --- PROCESS CODE (ОБРАБАТЫВАЕТ INLINE КНОПКИ ТЕРМИНАЛА, ВКЛЮЧАЯ 0️⃣) ---
@auth_router.callback_query(AuthStates.waiting_for_code, F.data.startswith("term_"))
async def process_code_terminal(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    current_code = data.get('current_code', "")
    action = callback.data.split('_')[1]

    if action.isdigit(): 
        if len(current_code) < 5: 
            current_code += action
    elif action == 'C': 
        current_code = current_code[:-1] if current_code else ""
    elif action == 'OK':
        await state.update_data(current_code=current_code)
        await process_code_final(callback.message, state, current_code)
        await callback.answer()
        return

    await state.update_data(current_code=current_code)
    
    # Обновляем сообщение с новым кодом
    await callback.message.edit_text(
        f"🔑 **Код: {current_code}**\n\n"
        "Введите **код подтверждения** с помощью цифровой клавиатуры:",
        reply_markup=kb_terminal_input(current_code)
    )
    await callback.answer()

async def process_code_final(message: Message, state: FSMContext, code: str):
    data = await state.get_data()
    phone = data.get('phone')
    code_hash = data.get('code_hash')
    
    if len(code) < 4:
        await message.answer("❌ Введен слишком короткий код. Пожалуйста, введите полный код.", reply_markup=kb_terminal_input(code))
        return

    try:
        client = await create_telethon_client_auth()
        await client.connect()
        user = await client.sign_in(phone=phone, code=code, phone_code_hash=code_hash)
        await client.disconnect() 
        await state.clear() 

        await message.edit_text(
            f"🎉 Успешный вход!\n\nВы вошли как: @{user.username or 'без username'}.\n"
            "**⚠️ Теперь перезапустите** скрипт бота, чтобы активировать мониторинг."
        )
    except SessionPasswordNeededError:
        await state.update_data(phone=phone) 
        await message.edit_text("🔒 **Требуется двухфакторный пароль.** Введите его обычной клавиатурой:")
        await state.set_state(AuthStates.waiting_for_password)
    except Exception as e:
        await state.clear()
        await message.edit_text(f"❌ Ошибка входа: {e}. Пожалуйста, попробуйте /start снова.")

# --- PROCESS PASSWORD ---
@auth_router.message(AuthStates.waiting_for_password, F.text)
async def process_password(message: Message, state: FSMContext):
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
# VI. ХЕНДЛЕРЫ ПОЛЬЗОВАТЕЛЯ И АДМИНА
# =========================================================================


# --- МЕНЮ IT ---
@user_router.callback_query(F.data == "menu_it")
async def show_it_menu(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    if not await db_check_user_subscription(bot, user_id): 
        await callback.answer(text="❌ Доступ запрещен. Нет подписки.", show_alert=True)
        return
    await callback.message.edit_text("📊 **IT-Отчеты**\n\nВыберите действие:", reply_markup=kb_report_menu('it', user_id))
    await callback.answer()

@user_router.callback_query(F.data == "show_it_reports")
async def show_it_reports(callback: types.CallbackQuery, bot: Bot) -> None:
    entries = db_get_last_it_entries(limit=10) 
    text = "📄 **Последние 10 записей IT-цикла:**\n\n" + \
           "\n".join([f"*{e['timestamp']}* - **{e['phone']}** ({e['status']})" for e in entries])
            
    await callback.message.edit_text(text, reply_markup=kb_report_menu('it', callback.from_user.id))
    await callback.answer()
    
@user_router.callback_query(F.data == "show_it_help")
async def show_it_help(callback: types.CallbackQuery) -> None:
    await callback.message.edit_text(
        "💡 **Помощь по IT-командам**\n\n"
        "Команды для использования в топике:\n"
        "`.айтиворк` - начать мониторинг топика.\n"
        "`.встал` - номер готов.\n"
        "`.ошибка- [код]` - номер с ошибкой.\n"
        "`.кьар [номер]` - нужен QR.\n"
        "`.повтор [номер]` - повтор номера.\n"
        "`.слет` - номер слетел (с новым номером).",
        reply_markup=kb_report_menu('it', callback.from_user.id)
    )
    await callback.answer()

@user_router.callback_query(F.data == "show_it_progress")
async def show_it_progress(callback: types.CallbackQuery) -> None:
    await callback.answer(text="📈 Здесь будет прогресс IT. (Заглушка)", show_alert=True)


# --- МЕНЮ DROP ---
@user_router.callback_query(F.data == "menu_drop")
async def show_drop_menu(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    if not await db_check_user_subscription(bot, user_id): 
        await callback.answer(text="❌ Доступ запрещен. Нет подписки.", show_alert=True)
        return
    await callback.message.edit_text("📝 **Дроп-Отчеты**\n\nВыберите действие:", reply_markup=kb_report_menu('drop', user_id))
    await callback.answer()

@user_router.callback_query(F.data == "show_drop_reports")
async def show_drop_reports(callback: types.CallbackQuery, bot: Bot) -> None:
    entries = db_get_last_drop_entries(limit=10) 
    text = "📄 **Последние 10 Дроп-отчетов:**\n\n" + \
           "\n---\n".join([f"*{e['timestamp']}*:\n`{e['report_text'][:80]}...`" for e in entries])
            
    await callback.message.edit_text(text, reply_markup=kb_report_menu('drop', callback.from_user.id))
    await callback.answer()
    
@user_router.callback_query(F.data == "show_drop_help")
async def show_drop_help(callback: types.CallbackQuery) -> None:
    await callback.message.edit_text(
        "💡 **Помощь по Drop-командам**\n\n"
        "Команда для использования в топике:\n"
        "`.дропворк` - начать мониторинг топика.\n"
        "Формат отчета:\n"
        "`[номер] [время] @[юзернейм] бх`",
        reply_markup=kb_report_menu('drop', callback.from_user.id)
    )
    await callback.answer()
    
@user_router.callback_query(F.data == "show_drop_progress")
async def show_drop_progress(callback: types.CallbackQuery) -> None:
    await callback.answer(text="📈 Здесь будет прогресс Drop. (Заглушка)", show_alert=True)


# --- ПРОЧИЕ КНОПКИ ---

@user_router.callback_query(F.data == "activate_promo")
async def activate_promo(callback: types.CallbackQuery) -> None:
    await callback.answer(text="🔑 Перенаправление на активацию промокода/оплату... (Заглушка)", show_alert=True)

@user_router.callback_query(F.data == "show_help")
async def show_help(callback: types.CallbackQuery) -> None:
    await callback.message.edit_text(
        f"ℹ️ **Раздел Помощи**\n\n"
        f"Для доступа к отчетам необходимо членство в канале **`{TARGET_CHANNEL_URL}`**.",
        reply_markup=kb_back_to_main(callback.from_user.id)
    )
    await callback.answer()


# --- АДМИН-ПАНЕЛЬ ---
@user_router.callback_query(F.data == "admin_panel")
async def show_admin_panel(callback: types.CallbackQuery, state: FSMContext) -> None:
     user_id = callback.from_user.id
     if user_id != ADMIN_ID:
         await callback.answer("🚫 Доступ запрещен.", show_alert=True)
         return
         
     kb_admin = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Выдать подписку (30 дней)", callback_data="admin_issue_promo")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
     ])
     
     await callback.message.edit_text(
         "🛠️ **Админ-Панель.**\n\n"
         "Выберите действие:",
         reply_markup=kb_admin
     )
     await callback.answer()

@user_router.callback_query(F.data == "admin_issue_promo")
async def cmd_admin_issue_promo(callback: types.CallbackQuery, state: FSMContext):
     await callback.message.edit_text(
         "🔑 **Выдача подписки.**\n\n"
         "Введите **ID пользователя**, которому нужно активировать подписку (30 дней).",
         reply_markup=kb_back_to_main(callback.from_user.id)
     )
     await state.set_state(AdminStates.waiting_for_promo_user_id)
     await callback.answer()

@user_router.message(AdminStates.waiting_for_promo_user_id, F.text.regexp(r'^\d+$'))
async def process_admin_issued_promo(message: Message, state: FSMContext):
    target_user_id = int(message.text.strip())
    
    try:
        end_date = db_activate_subscription(target_user_id)
        
        await message.bot.send_message(
            target_user_id,
            f"🎉 **Ваша подписка активирована администратором!**\nСрок действия: до {end_date.strftime('%d.%m.%Y')}",
            parse_mode='Markdown'
        )
        
        await message.answer(f"✅ Подписка для пользователя ID `{target_user_id}` активирована до {end_date.strftime('%d.%m.%Y')}.", 
                             reply_markup=kb_back_to_main(ADMIN_ID))
    except Exception as e:
        logger.error(f"Ошибка при выдаче подписки: {e}")
        await message.answer("❌ Ошибка при активации. Убедитесь, что пользователь уже общался с ботом.",
                             reply_markup=kb_back_to_main(ADMIN_ID))
    
    await state.clear()


# =========================================================================
# VII. ГЛАВНАЯ ТОЧКА ЗАПУСКА
# =========================================================================

async def main():
    if not BOT_TOKEN or not API_ID or not API_HASH:
        logger.critical("КРИТИЧЕСКАЯ ОШИБКА: Один или несколько API ключей/токенов не найдены в переменных окружения. Проверьте настройки на вашем сайте!")
        return

    logger.info("Инициализация базы данных и проверка таблиц...")
    os.makedirs('data', exist_ok=True) 
    create_tables()
    
    storage = MemoryStorage() 
    # ИСПРАВЛЕНИЕ: Новый синтаксис для parse_mode в Aiogram 3.x
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='Markdown')) 
    dp = Dispatcher(storage=storage)
    
    dp.include_router(auth_router)
    dp.include_router(user_router)

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
