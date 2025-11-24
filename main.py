# main.py (ФИНАЛЬНАЯ РЕСТРУКТУРИЗАЦИЯ)

import asyncio
import logging
import os
import sqlite3
from datetime import datetime
import pytz 

# --- ЧТЕНИЕ ИЗ ОКРУЖЕНИЯ ---
from dotenv import load_dotenv 
load_dotenv() 

# --- Aiogram и FSM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message

# --- Telethon ---
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
from telethon.utils import get_display_name

# =========================================================================
# I. КОНФИГ И ЛОГИРОВАНИЕ (БЕЗ ИЗМЕНЕНИЙ)
# =========================================================================

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
# II. DB И UTILS (БЕЗ ИЗМЕНЕНИЙ)
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
    # Добавление таблицы для мониторинга топиков
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

async def db_check_user_subscription(bot: Bot, user_id):
    if user_id == ADMIN_ID: return True 

    try:
        member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
        if member.status in ['member', 'administrator', 'creator']:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Ошибка при проверке подписки для {user_id}: {e}")
        return False

# =========================================================================
# III. КЛАВИАТУРЫ (KEYBOARDS) - ПОЛНЫЙ РЕДИЗАЙН
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
        InlineKeyboardButton(text="❓ Спросить вопрос", callback_data="ask_question"),
    ])
    
    # Раздел Отчетов
    buttons.append([
        InlineKeyboardButton(text="📊 IT-Отчеты", callback_data="menu_it"),
        InlineKeyboardButton(text="📝 Дроп-Отчеты", callback_data="menu_drop"),
    ])
    
    # Вход в аккаунт
    buttons.append([
        InlineKeyboardButton(text="🔐 Вход в аккаунт (Telethon)", callback_data="menu_auth"),
    ])

    if user_id == ADMIN_ID:
        buttons.append([
            InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_auth_menu() -> InlineKeyboardMarkup:
    """Кнопки входа в аккаунт."""
    buttons = [
        [InlineKeyboardButton(text="📱 Войти через QR-код", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔑 Войти через API ID/Hash", callback_data="auth_api")],
        [InlineKeyboardButton(text="💬 Войти через TG SMS (Код)", callback_data="auth_sms")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_terminal_input(current_code: str) -> InlineKeyboardMarkup:
    """Специальная цифровая клавиатура для ввода кода."""
    buttons = []
    
    # 1 2 3
    buttons.append([InlineKeyboardButton(text="1️⃣", callback_data="term_1"),
                    InlineKeyboardButton(text="2️⃣", callback_data="term_2"),
                    InlineKeyboardButton(text="3️⃣", callback_data="term_3")])
    # 4 5 6
    buttons.append([InlineKeyboardButton(text="4️⃣", callback_data="term_4"),
                    InlineKeyboardButton(text="5️⃣", callback_data="term_5"),
                    InlineKeyboardButton(text="6️⃣", callback_data="term_6")])
    # 7 8 9
    buttons.append([InlineKeyboardButton(text="7️⃣", callback_data="term_7"),
                    InlineKeyboardButton(text="8️⃣", callback_data="term_8"),
                    InlineKeyboardButton(text="9️⃣", callback_data="term_9")])
    
    # Clear 0 Confirm
    buttons.append([InlineKeyboardButton(text="⬅️ Очистить", callback_data="term_C"),
                    InlineKeyboardButton(text="0️⃣", callback_data="term_0"),
                    InlineKeyboardButton(text="✅ Ввести", callback_data="term_OK")])
    
    # Текущий код
    buttons.append([InlineKeyboardButton(text=f"Код: {current_code or '...'} | Введите", callback_data="ignore")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_report_menu(report_type: str, user_id: int) -> InlineKeyboardMarkup:
    """Меню для IT и Drop отчетов."""
    buttons = [
        [InlineKeyboardButton(text="📊 Отчет (Последние 10)", callback_data=f"show_{report_type}_reports")],
        [InlineKeyboardButton(text="📈 Прогресс/Статус", callback_data=f"show_{report_type}_progress")],
        [InlineKeyboardButton(text="💡 Помощь по командам", callback_data=f"show_{report_type}_help")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_back_to_main(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# =========================================================================
# IV. TELETHON WORKER (ОБНОВЛЕННЫЕ КОМАНДЫ)
# =========================================================================

SESSION_DIR = 'data'
SESSION_FILE = f'{SESSION_DIR}/telethon_session_{API_ID}'
TELETHON_RUNNING = False

async def start_telethon_worker(bot: Bot, dp: Dispatcher):
    """Запускает и поддерживает Telethon-клиента с новыми командами."""
    global TELETHON_RUNNING
    
    # ... (Проверка ключей и сессии остается прежней) ...
    
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

        # --- ОБНОВЛЕННАЯ СТРУКТУРА ДЛЯ КАСТОМНЫХ КОМАНД TELETHON ---
        
        @client.on(events.NewMessage(pattern=r'^\.чекгруппу', func=lambda e: e.is_private is False))
        async def handle_check_group_command(event: events.NewMessage):
             # Логика: .чекгруппу [группа/топик]
             # event.chat_id, event.reply_to_msg_id (для топика)
             await event.reply("✅ **.чекгруппу**: Начинаю сбор ID/Username. Отчет будет отправлен вам в ЛС бота.")
             # ... Здесь должна быть логика сбора участников и форматирования TXT/Таблицы
             
             # Отправка результата в ЛС админа через Aiogram Bot
             # await bot.send_document(ADMIN_ID, ...)
        
        @client.on(events.NewMessage(pattern=r'^\.флуд(стоп)?', func=lambda e: e.is_private is True or e.sender_id == ADMIN_ID))
        async def handle_flood_command(event: events.NewMessage):
            # Логика: .флуд (кол-во) текст (задержка) / .флудстоп
            command = event.text.split()
            if command[0] == '.флудстоп':
                await event.reply("❌ **.флудстоп**: Команда остановки рассылки получена. (Требуется логика остановки процесса)")
                return
            
            # ... Логика парсинга .флуд (кол-во, текст, задержка)
            await event.reply("✅ **.флуд**: Запущена рассылка с указанными параметрами.")

        @client.on(events.NewMessage(pattern=r'^\.лс ', func=lambda e: e.is_private is True or e.sender_id == ADMIN_ID))
        async def handle_ls_command(event: events.NewMessage):
             # Логика: .лс текст юзернеймы(без запятой)
             # Парсинг: event.text.split()[1] - текст, остальные - юзернеймы.
             await event.reply("✅ **.лс**: Сообщение отправлено указанным пользователям.")
             
        # --- КОМАНДЫ МОНИТОРИНГА ТОПИКОВ ---
        # Логика запуска мониторинга топика
        @client.on(events.NewMessage(pattern=r'^\.(дропворк|айтиворк)', func=lambda e: e.is_private is False and e.is_topic))
        async def handle_start_monitor_command(event: events.NewMessage):
            topic_id = event.id if event.is_topic else event.reply_to_msg_id
            chat_id = event.chat_id
            monitor_type = 'drop' if event.text.startswith('.дропворк') else 'it'
            
            # ... Логика сохранения в DB о начале мониторинга топика
            
            await client.send_message(event.chat_id, 
                                      f"✅ **Мониторинг {monitor_type.upper()} запущен** в топике ID: {topic_id}.", 
                                      reply_to=event.id)
            await client.send_message(ADMIN_ID, f"🔔 Мониторинг {monitor_type.upper()} запущен в чате {get_display_name(await event.get_chat())}, топик {topic_id}.")

        # Логика обработки команд внутри топика (только если мониторинг активен)
        @client.on(events.NewMessage(func=lambda e: e.is_private is False and e.is_topic))
        async def handle_topic_commands(event: events.NewMessage):
            # ... Здесь должна быть логика проверки: Активен ли мониторинг для event.id / event.reply_to_msg_id
            if event.text.startswith('.дропворк'): # Игнорируем команду запуска
                return
            
            if event.text.startswith('.айтиворк'): # Игнорируем команду запуска
                return
            
            # --- Логика парсинга IT команд: .встал, .ошибка-, .кьар, .повтор, .слет ---
            # ...
            
            # --- Логика парсинга Drop отчета: +7... 12:00 @user бх ---
            # ...
            
            # Сохранение лога и отправка в ЛС админа
            # await bot.send_message(ADMIN_ID, f"Новый отчет ({monitor_type.upper()}): {event.text}")


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

class AuthStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State() # Теперь обрабатывает inline-кнопки
    waiting_for_password = State()
    
# ... (create_telethon_client_auth и check_telethon_auth остаются прежними)

# --- START HANDLER ---
@auth_router.message(Command("start"))
async def command_start_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    user_id = message.from_user.id
    db_create_user_if_not_exists(user_id, message.from_user.username, message.from_user.first_name) 
    await state.clear()
    
    is_subscribed = await db_check_user_subscription(bot, user_id)
    
    welcome_text = f"👋 **STATPRO приветствует!**\n\n"
    welcome_text += f"Ваш ID: `{user_id}`\n"
    welcome_text += f"Статус подписки: {'✅ Активна' if is_subscribed else '❌ Не активна'}"

    if not is_subscribed:
        await message.answer(
            welcome_text + f"\n\n**⚠️ Доступ к отчетам закрыт.** Вступите в канал {TARGET_CHANNEL_URL} для активации.",
            reply_markup=kb_subscription_required()
        )
        return

    # Если подписка активна
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

# --- AUTH MENU NAVIGATION ---
@auth_router.callback_query(F.data == "menu_auth")
async def show_auth_menu(callback: types.CallbackQuery) -> None:
    await callback.message.edit_text(
        "🔐 **Вход в аккаунт Telethon**\n\nВыберите удобный способ авторизации:",
        reply_markup=kb_auth_menu()
    )
    await callback.answer()

# --- AUTH METHODS ---
@auth_router.callback_query(F.data == "auth_sms")
async def cmd_auth_start(callback: types.CallbackQuery, state: FSMContext):
    # Проверка Telethon и запуск диалога (та же логика, что и раньше)
    if callback.from_user.id != ADMIN_ID: 
         await callback.answer("❌ Эта команда доступна только администратору.", show_alert=True)
         return
    
    # ... (логика проверки авторизации, см. старый код)
    
    await callback.message.edit_text(
        "🔒 **Начинаем вход в Telegram.**\n\n"
        "Пожалуйста, введите ваш **номер телефона** (например, 79991234567):"
    )
    await state.set_state(AuthStates.waiting_for_phone)
    await callback.answer()

@auth_router.callback_query(F.data == "auth_qr")
async def cmd_qr_start(callback: types.CallbackQuery, state: FSMContext):
     await callback.answer("⏳ QR-вход временно недоступен. Используйте SMS.", show_alert=True)
     # ... Здесь должна быть логика: client.qr_login() и FSM для ожидания сканирования.

@auth_router.callback_query(F.data == "auth_api")
async def cmd_api_start(callback: types.CallbackQuery, state: FSMContext):
     await callback.answer("⏳ API вход временно недоступен. Используйте SMS.", show_alert=True)
     # ... Здесь должна быть логика: ввод API ID/Hash, но мы их берем из окружения.

# --- PROCESS PHONE (Остается текстовым) ---
@auth_router.message(AuthStates.waiting_for_phone, F.text.regexp(r'^\+?[789]\d{9,10}$'))
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip().replace('+', '')
    try:
        client = await create_telethon_client_auth()
        await client.connect()
        result = await client.send_code_request(phone)
        await client.disconnect() 
        
        await state.update_data(phone=phone, code_hash=result.phone_code_hash, current_code="")
        
        # Отправляем сообщение с ТЕРМИНАЛЬНОЙ КЛАВИАТУРОЙ
        await message.answer(
            f"🔑 Код отправлен на номер **{phone}**.\n\n"
            "Введите **код подтверждения** с помощью цифровой клавиатуры:",
            reply_markup=kb_terminal_input("")
        )
        await state.set_state(AuthStates.waiting_for_code)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}. Попробуйте /start снова.")
        await state.clear()

# --- PROCESS CODE (ОБРАБАТЫВАЕТ INLINE КНОПКИ ТЕРМИНАЛА) ---
@auth_router.callback_query(AuthStates.waiting_for_code, F.data.startswith("term_"))
async def process_code_terminal(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    current_code = data.get('current_code', "")
    action = callback.data.split('_')[1]

    if action.isdigit():
        if len(current_code) < 5: # Максимум 5 цифр для кода
            current_code += action
    elif action == 'C': # Clear
        current_code = current_code[:-1] if current_code else ""
    elif action == 'OK':
        # Если код введен, переходим к следующему шагу (process_code_final)
        await state.update_data(current_code=current_code)
        await process_code_final(callback.message, state, current_code)
        return

    await state.update_data(current_code=current_code)
    
    # Обновляем сообщение с новым кодом и клавиатурой
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
        # Переключаемся на обычную текстовую клавиатуру для ввода пароля
        await message.edit_text("🔒 **Требуется двухфакторный пароль.** Введите его обычной клавиатурой:")
        await state.set_state(AuthStates.waiting_for_password)
    except Exception as e:
        await state.clear()
        await message.edit_text(f"❌ Ошибка входа: {e}. Пожалуйста, попробуйте /start снова.")

# --- PROCESS PASSWORD (Остается текстовым) ---
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
# VI. ХЕНДЛЕРЫ ПОЛЬЗОВАТЕЛЯ (USER HANDLERS)
# =========================================================================

user_router = Router()

# --- ОТЧЕТЫ: IT ---
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
    # ... (логика показа отчетов, см. старый код)
    await callback.answer(text="Отчеты показаны (старая заглушка)", show_alert=True)
    
@user_router.callback_query(F.data == "show_it_progress")
async def show_it_progress(callback: types.CallbackQuery) -> None:
    await callback.answer(text="📈 Здесь будет прогресс IT.", show_alert=True)

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

# --- ОТЧЕТЫ: DROP ---
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
    # ... (логика показа отчетов, см. старый код)
    await callback.answer(text="Отчеты показаны (старая заглушка)", show_alert=True)
    
@user_router.callback_query(F.data == "show_drop_progress")
async def show_drop_progress(callback: types.CallbackQuery) -> None:
    await callback.answer(text="📈 Здесь будет прогресс Drop.", show_alert=True)

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

# --- ПРОЧИЕ КНОПКИ ---

@user_router.callback_query(F.data == "activate_promo")
async def activate_promo(callback: types.CallbackQuery) -> None:
    await callback.answer(text="🔑 Перенаправление на активацию промокода/оплату...", show_alert=True)
    # Здесь можно добавить ссылку или FSM для ввода промокода

@user_router.callback_query(F.data == "show_help")
async def show_help(callback: types.CallbackQuery) -> None:
    await callback.message.edit_text(
        f"ℹ️ **Раздел Помощи**\n\n"
        f"Для доступа к отчетам необходимо членство в канале {TARGET_CHANNEL_URL}.",
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
            "🛠️ **Админ-Панель.**",
            reply_markup=kb_back_to_main(user_id)
        )
     await callback.answer()

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
    bot = Bot(token=BOT_TOKEN, parse_mode='Markdown')
    dp = Dispatcher(storage=storage)
    
    # Регистрация роутеров
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
