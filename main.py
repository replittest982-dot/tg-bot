# main.py (ФИНАЛЬНЫЙ МОНОЛИТНЫЙ КОД - Версия 6: Реализация .ЛС и Мониторинг Задач)

import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta 
import pytz 
from io import BytesIO 
import qrcode 
import random
import string

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
# II. БАЗА ДАННЫХ (DB) (ИСПРАВЛЕНА БЛОКИРОВКА)
# =========================================================================

DB_PATH = os.path.join('data', DB_NAME) 

def get_db_connection():
    # Установка таймаута на 5 секунд для ожидания снятия блокировки
    return sqlite3.connect(DB_PATH, timeout=5)

def create_tables():
    # Используем 'with' для гарантированного закрытия
    with get_db_connection() as conn:
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
            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                duration_days INTEGER NOT NULL,
                is_used BOOLEAN DEFAULT 0,
                used_by INTEGER,
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
    # Соединение закрывается автоматически

def db_create_user_if_not_exists(user_id, username=None, first_name=None):
    with get_db_connection() as conn:
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
    # Соединение закрывается автоматически

def db_activate_subscription(user_id, reason="admin_issued"):
    end_date = datetime.now(TIMEZONE_MSK) + timedelta(days=30)
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET subscription_active = 1, subscription_end_date = ? WHERE user_id = ?",
            (end_date.isoformat(), user_id)
        )
        conn.commit()
    return end_date

def db_use_promo_code(user_id, code):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT duration_days, is_used FROM promo_codes WHERE code = ?", (code,))
        promo = cursor.fetchone()
        
        if promo and promo[1] == 0: 
            duration = promo[0]
            end_date = datetime.now(TIMEZONE_MSK) + timedelta(days=duration)
            
            cursor.execute(
                "UPDATE users SET subscription_active = 1, subscription_end_date = ?, promo_code = ? WHERE user_id = ?",
                (end_date.isoformat(), code, user_id)
            )
            
            cursor.execute(
                "UPDATE promo_codes SET is_used = 1, used_by = ? WHERE code = ?",
                (user_id, code)
            )
            conn.commit()
            return end_date
    
    return None

def db_create_promo_code(duration_days):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        
        try:
            cursor.execute(
                "INSERT INTO promo_codes (code, duration_days) VALUES (?, ?)",
                (code, duration_days)
            )
            conn.commit()
            return code
        except sqlite3.IntegrityError:
            # Если код уже существует (редко), рекурсивно пытаемся создать новый
            return db_create_promo_code(duration_days) 

async def db_check_user_subscription(bot: Bot, user_id):
    if user_id == ADMIN_ID: return True 

    # 1. Проверка через API Telegram (вступление в канал)
    try:
        member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
        if member.status in ['member', 'administrator', 'creator']:
            return True
    except Exception:
        pass

    # 2. Проверка через DB (промокод/админ)
    # Используем 'with' для гарантированного закрытия
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT subscription_active, subscription_end_date FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
    
    if result:
        active, end_date_str = result
        if active and end_date_str:
            end_date = datetime.fromisoformat(end_date_str)
            if end_date.replace(tzinfo=None) > datetime.now(TIMEZONE_MSK).replace(tzinfo=None):
                 return True 

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
    
    buttons.append([
        InlineKeyboardButton(text="ℹ️ Помощь", callback_data="show_help"),
        InlineKeyboardButton(text="🔑 Подписка", callback_data="activate_promo"),
        InlineKeyboardButton(text="❓ Задать вопрос", url="t.me/yanixforever"), 
    ])
    
    buttons.append([
        InlineKeyboardButton(text="📄 Отчеты и Инструменты", callback_data="menu_reports_tools"), 
        # НОВАЯ КНОПКА: Мониторинг активных задач Telethon
        InlineKeyboardButton(text="⚙️ Мониторинг Задач", callback_data="menu_task_monitor"), 
    ])
    
    if user_id == ADMIN_ID:
        buttons.append([
            InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

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
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_reports_tools")], 
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
    
    display_code = "..." if not current_code else current_code
    buttons.append([InlineKeyboardButton(text=f"Код: {display_code} | Введите", callback_data="ignore")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_report_menu(report_type: str, user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📊 Отчет (Последние 10)", callback_data=f"show_{report_type}_reports")],
        [InlineKeyboardButton(text="📈 Прогресс/Статус", callback_data=f"show_{report_type}_progress")],
        [InlineKeyboardButton(text="💡 Помощь по командам", callback_data=f"show_{report_type}_help")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_reports_tools")] 
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_back_to_main(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_admin_panel() -> InlineKeyboardMarkup:
     buttons = [
        [InlineKeyboardButton(text="➕ Создать Промокод", callback_data="admin_create_promo")], 
        [InlineKeyboardButton(text="➕ Выдать подписку по ID (30 дней)", callback_data="admin_issue_promo")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
     ]
     return InlineKeyboardMarkup(inline_keyboard=buttons)


# =========================================================================
# IV. TELETHON WORKER (Добавлена логика .ЛС и Мониторинг Задач)
# =========================================================================

SESSION_DIR = 'data'
SESSION_FILE = f'{SESSION_DIR}/telethon_session_{API_ID}'
TELETHON_RUNNING = False
# Глобальный словарь для хранения активных задач Telethon
ACTIVE_TELETHON_TASKS = {} 


async def send_mass_pm(client, task_id, user_ids_or_usernames, message_text, started_by_id):
    global ACTIVE_TELETHON_TASKS
    
    task_data = ACTIVE_TELETHON_TASKS.get(task_id)
    if not task_data:
        logger.error(f"Задача {task_id} не найдена в ACTIVE_TELETHON_TASKS.")
        return

    total_recipients = len(user_ids_or_usernames)
    sent_count = 0
    
    task_data['total'] = total_recipients
    
    for recipient in user_ids_or_usernames:
        # Проверяем, была ли задача отменена
        if task_data['task'].cancelled():
            logger.warning(f"Задача {task_id} отменена.")
            break
            
        try:
            # Преобразование ID/Username в сущность Telegram
            entity = await client.get_input_entity(recipient) 
            await client.send_message(entity, message_text)
            sent_count += 1
            
            # Обновление прогресса
            task_data['progress'] = f"{sent_count}/{total_recipients}"
            
        except Exception as e:
            logger.error(f"Ошибка отправки ЛС на {recipient}: {e}")
            
        # Задержка 1 секунда для избежания флуда и блокировки
        await asyncio.sleep(1) 
        
    # Финальное обновление статуса
    task_data['progress'] = f"100% (Отправлено: {sent_count}/{total_recipients})"
    logger.info(f"Задача ЛС {task_id} завершена. Отправлено {sent_count}.")


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
        
        # .чек лс: Работает везде (private or group)
        @client.on(events.NewMessage(pattern=r'^\.чек лс'))
        async def handle_check_ls_command(event: events.NewMessage):
             await event.reply("✅ **.чек лс**: Начинаю сбор ID/Username. Отчет будет отправлен вам в ЛС бота. (Заглушка)")
        
        # .чекгруппу: Работает ТОЛЬКО в группах (is_private is False)
        @client.on(events.NewMessage(pattern=r'^\.чекгруппу', func=lambda e: e.is_private is False))
        async def handle_check_group_command(event: events.NewMessage):
             await event.reply("✅ **.чекгруппу**: Начинаю сбор ID/Username. Отчет будет отправлен вам в ЛС бота. (Заглушка)")
        
        @client.on(events.NewMessage(pattern=r'^\.чекгруппу', func=lambda e: e.is_private is True))
        async def handle_check_group_command_fail(event: events.NewMessage):
             await event.reply("❌ **.чекгруппу**: Эта команда работает **только в группах**.")

        # .флуд и .флудстоп: Работает везде (private or group)
        @client.on(events.NewMessage(pattern=r'^\.флуд(стоп)?'))
        async def handle_flood_command(event: events.NewMessage):
            command = event.text.split()
            if command[0] == '.флудстоп':
                # TODO: Добавить логику отмены флуд-задачи
                await event.reply("❌ **.флудстоп**: Команда остановки рассылки получена. (Требуется логика остановки процесса)")
                return
            await event.reply("✅ **.флуд**: Запущена рассылка с указанными параметрами. (Заглушка)")

        # .лс (ИСПРАВЛЕНО: Теперь запускает реальную рассылку)
        @client.on(events.NewMessage(pattern=r'^\.лс (.*)'))
        async def handle_ls_command(event: events.NewMessage):
            global ACTIVE_TELETHON_TASKS
            
            # 1. Парсинг команды
            # Пример: .лс @user1, 123456 Привет, это тест!
            parts = event.text.split(' ', 2)
            if len(parts) < 3:
                await event.reply("❌ **Неверный формат**. Используйте: `.лс [юзернейм1, ID2, ...] [Сообщение]`")
                return
            
            recipients_str = parts[1]
            message_text = parts[2]
            
            recipients = [r.strip().replace('@', '') for r in recipients_str.split(',')]
            task_id = ''.join(random.choices(string.hexdigits, k=10))
            
            # 2. Запуск асинхронной задачи
            loop = asyncio.get_event_loop()
            task = loop.create_task(send_mass_pm(client, task_id, recipients, message_text, event.sender_id))
            
            # 3. Сохранение статуса
            ACTIVE_TELETHON_TASKS[task_id] = {
                'task': task,
                'type': 'Mass PM (.лс)',
                'started_by': event.sender_id,
                'start_time': datetime.now(TIMEZONE_MSK),
                'progress': '0/0',
                'total': 0
            }

            # 4. Отправка ответа (БЕЗ ЗАГЛУШКИ "отправлено")
            await event.reply(
                f"✅ **Задача `.лс` запущена!**\n"
                f"ID задачи: `{task_id[:6]}`\n"
                f"Получателей: **{len(recipients)}**\n"
                f"Прогресс можно отслеживать в меню **⚙️ Мониторинг Задач**."
            )
             
        # --- КОМАНДЫ МОНИТОРИНГА ТОПИКОВ ---
        @client.on(events.NewMessage(pattern=r'^\.(дропворк|айтиворк)', func=lambda e: e.is_private is False))
        async def handle_start_monitor_command(event: events.NewMessage):
            topic_id = event.reply_to_msg_id if event.reply_to_msg_id else event.id 
            monitor_type = 'drop' if event.text.startswith('.дропворк') else 'it'
            
            await client.send_message(event.chat_id, 
                                      f"✅ **Мониторинг {monitor_type.upper()} запущен** в топике ID: {topic_id}. (Заглушка)", 
                                      reply_to=event.id)
            await client.send_message(ADMIN_ID, f"🔔 Мониторинг {monitor_type.upper()} запущен в чате {get_display_name(await event.get_chat())}, топик {topic_id}.")

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
    waiting_for_promo_code = State() 

class AdminStates(StatesGroup): 
    waiting_for_promo_user_id = State()
    waiting_for_promo_duration = State() 

async def create_telethon_client_auth():
    session_path = os.path.join(SESSION_DIR, os.path.basename(SESSION_FILE))
    return TelegramClient(session_path, API_ID, API_HASH)

# --- START HANDLER --- (Отправка нового сообщения)
@auth_router.message(Command("start"))
async def command_start_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    user_id = message.from_user.id
    db_create_user_if_not_exists(user_id, message.from_user.username, message.from_user.first_name) 
    await state.clear()
    
    is_subscribed = await db_check_user_subscription(bot, user_id)
    
    welcome_text = f"👋 **STATPRO приветствует!**\n"
    welcome_text += f"*Бот находится в бета-тестировании.*\n\n" 
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

# --- NAVIGATION HANDLERS (Редактирование сообщений) ---
@auth_router.callback_query(F.data == "back_to_main")
async def back_to_main_menu_callback(callback: types.CallbackQuery, state: FSMContext) -> None:
    user_id = callback.from_user.id
    await state.clear()
    await callback.message.edit_text(
        "🏠 **Главное меню**",
        reply_markup=kb_main_menu(user_id)
    )
    await callback.answer()

@user_router.callback_query(F.data == "menu_reports_tools")
async def show_reports_tools_menu(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    
    # СТРОГАЯ ПРОВЕРКА ПОДПИСКИ ЗДЕСЬ
    if not await db_check_user_subscription(bot, user_id): 
        await callback.answer(text="❌ Доступ запрещен. Требуется подписка.", show_alert=True)
        return
        
    await callback.message.edit_text(
        "📄 **Отчеты и Инструменты**\n\n"
        "Выберите раздел:",
        reply_markup=kb_general_reports_menu() 
    )
    await callback.answer()

# --- AUTH HANDLERS ---
@auth_router.callback_query(F.data == "menu_auth")
async def show_auth_menu(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    # СТРОГАЯ ПРОВЕРКА ПОДПИСКИ
    if not await db_check_user_subscription(bot, user_id): 
        await callback.answer("❌ Доступ запрещен. Вход в аккаунт доступен только подписчикам.", show_alert=True)
        return
        
    await callback.message.edit_text(
        "🔐 **Вход в аккаунт Telethon**\n\nВыберите удобный способ авторизации:",
        reply_markup=kb_auth_menu()
    )
    await callback.answer()

# --- 1. QR-ВХОД --- (Использует mix: edit_text, answer_photo, answer)
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
        
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(qr_url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        buffer = BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)

        # Отправляем ИЗОБРАЖЕНИЕ (нельзя редактировать)
        await callback.message.answer_photo(
            BufferedInputFile(buffer.getvalue(), filename="qr_code.png"),
            caption="📱 **QR-вход запущен.**\n\n1. Откройте Telegram на телефоне.\n2. Перейдите: **Настройки → Устройства → Привязать новое устройство**.\n3. **Отсканируйте** код выше.\n\n**Ожидаю сканирования...**"
        )
        
        # Удаляем предыдущее сообщение "Запускаю QR-сессию..."
        await callback.message.delete() 
        
        await state.set_state(AuthStates.waiting_for_qr_scan)
        
        user = await qr_login_object.wait(client)
        
        await state.clear()
        # Отправляем НОВОЕ сообщение об успехе (не редактируем, так как предыдущее - фото)
        await bot.send_message(user_id,
            f"🎉 Успешный вход через QR!\n\nВы вошли как: @{user.username or 'без username'}.\n"
            "**⚠️ Теперь перезапустите** скрипт бота, чтобы активировать мониторинг.",
            reply_markup=kb_main_menu(user_id)
        )

    except Exception as e:
        logger.error(f"Ошибка QR входа: {e}")
        await state.clear()
        # Отправляем НОВОЕ сообщение об ошибке
        await bot.send_message(user_id, f"❌ Ошибка QR входа: {e}. Попробуйте /start снова.", reply_markup=kb_auth_menu())
    finally:
        if 'client' in locals() and client.is_connected():
            await client.disconnect()
            
    await callback.answer()

# --- 2. API ВХОД (ЗАГЛУШКА) ---
@auth_router.callback_query(F.data == "auth_api")
async def cmd_api_start(callback: types.CallbackQuery, state: FSMContext):
     await callback.answer("⏳ API вход временно недоступен. Используйте SMS или QR.", show_alert=True)

# --- 3. SMS ВХОД (Редактирование сообщения) ---
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

# --- PROCESS PHONE (Отправка нового сообщения) ---
@auth_router.message(AuthStates.waiting_for_qr_scan)
async def handle_qr_scan_status(message: types.Message):
    # Отправляем НОВОЕ сообщение, так как предыдущее было фото
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
        
        # Отправляем НОВОЕ сообщение с клавиатурой терминала
        await message.answer(
            f"🔑 Код отправлен на номер **{phone}**.\n\n"
            "Введите **код подтверждения** с помощью цифровой клавиатуры:",
            reply_markup=kb_terminal_input("")
        )
        await state.set_state(AuthStates.waiting_for_code)
        # Удаляем сообщение с номером телефона для чистоты чата
        await message.delete() 
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}. Попробуйте /start снова.")
        await state.clear()

# --- PROCESS CODE (Редактирование сообщения) ---
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
    
    # Редактируем сообщение с новым кодом
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
        # Редактируем, чтобы не засорять чат
        await message.edit_text("❌ Введен слишком короткий код. Пожалуйста, введите полный код.", reply_markup=kb_terminal_input(code))
        return

    try:
        client = await create_telethon_client_auth()
        await client.connect()
        user = await client.sign_in(phone=phone, code=code, phone_code_hash=code_hash)
        await client.disconnect() 
        await state.clear() 

        # Редактируем сообщение об успешном входе
        await message.edit_text(
            f"🎉 Успешный вход!\n\nВы вошли как: @{user.username or 'без username'}.\n"
            "**⚠️ Теперь перезапустите** скрипт бота, чтобы активировать мониторинг.",
            reply_markup=kb_main_menu(message.chat.id)
        )
    except SessionPasswordNeededError:
        await state.update_data(phone=phone) 
        # Редактируем сообщение для ввода пароля
        await message.edit_text("🔒 **Требуется двухфакторный пароль.** Введите его обычной клавиатурой:")
        await state.set_state(AuthStates.waiting_for_password)
    except Exception as e:
        await state.clear()
        # Редактируем сообщение об ошибке
        await message.edit_text(f"❌ Ошибка входа: {e}. Пожалуйста, попробуйте /start снова.")

# --- PROCESS PASSWORD (Отправка нового сообщения) ---
@auth_router.message(AuthStates.waiting_for_password, F.text)
async def process_password(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.chat.id
    
    try:
        client = await create_telethon_client_auth()
        await client.connect()
        user = await client.sign_in(password=password)
        await client.disconnect() 
        
        # Отправляем НОВОЕ сообщение об успехе (так как вводили текст)
        await message.answer(
            f"🎉 Успешный вход!\n\nВы вошли как: @{user.username or 'без username'}.\n"
            "**⚠️ Теперь перезапустите** скрипт бота, чтобы активировать мониторинг.",
            reply_markup=kb_main_menu(user_id)
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}. Пароль неверен. Попробуйте /start снова.")
    finally:
        await state.clear()
        # Удаляем сообщение с паролем
        await message.delete()


# =========================================================================
# VI. ХЕНДЛЕРЫ ПОЛЬЗОВАТЕЛЯ И АДМИНА
# =========================================================================

# НОВЫЙ ХЕНДЛЕР: Мониторинг активных задач
@user_router.callback_query(F.data == "menu_task_monitor")
async def show_task_monitor_menu(callback: types.CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    if not await db_check_user_subscription(bot, user_id): 
        await callback.answer(text="❌ Доступ запрещен. Нет подписки.", show_alert=True)
        return
        
    tasks_list = [v for v in ACTIVE_TELETHON_TASKS.values() if v['started_by'] == user_id]
    
    if not tasks_list:
        text = "⚙️ **Мониторинг Задач**\n\n**Нет активных задач.**\n" \
               "Запустите рассылку или сбор, используя Telethon-команды."
        
    else:
        text = "⚙️ **Мониторинг Активных Задач:**\n\n"
        for task_id, task_data in ACTIVE_TELETHON_TASKS.items():
            if task_data['started_by'] != user_id: continue 
            
            status = "✅ Завершена" if task_data['task'].done() else "⏳ В процессе"
            
            text += f"**ID: `{task_id[:6]}` | Тип: {task_data['type']}**\n"
            text += f"Статус: {status}\n"
            text += f"Прогресс: {task_data.get('progress', '0/0')} ({task_data.get('total', '?')} получателей)\n"
            text += f"Время старта: {task_data['start_time'].strftime('%H:%M:%S')}\n\n"
            
    await callback.message.edit_text(
        text,
        reply_markup=kb_back_to_main(user_id)
    )
    await callback.answer()


# --- МЕНЮ IT / DROP (Редактирование сообщений) ---
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


# --- ПРОЧИЕ КНОПКИ (Редактирование сообщений) ---

# НОВЫЕ ХЕНДЛЕРЫ ДЛЯ АКТИВАЦИИ ПРОМОКОДА ПОЛЬЗОВАТЕЛЕМ
@user_router.callback_query(F.data == "activate_promo")
async def cmd_start_promo_activation(callback: types.CallbackQuery, state: FSMContext):
     await callback.message.edit_text(
         "🔑 **Активация Промокода**\n\n"
         "Пожалуйста, введите **промокод** для активации подписки:",
         reply_markup=kb_back_to_main(callback.from_user.id)
     )
     await state.set_state(AuthStates.waiting_for_promo_code)
     await callback.answer()

@auth_router.message(AuthStates.waiting_for_promo_code, F.text)
async def process_user_promo_code(message: Message, state: FSMContext, bot: Bot):
    code = message.text.strip().upper()
    user_id = message.from_user.id
    
    end_date = db_use_promo_code(user_id, code)
    
    if end_date:
        await message.answer(
            f"🎉 **Промокод активирован!**\nВаша подписка активна до **{end_date.strftime('%d.%m.%Y')}**.\n"
            "Перезапустите бота командой /start.",
            reply_markup=kb_main_menu(user_id)
        )
    else:
        await message.answer(
            "❌ **Ошибка активации.**\nПромокод не найден или уже был использован. Попробуйте снова или /start.",
            reply_markup=kb_main_menu(user_id)
        )
    
    await state.clear()
    await message.delete()

@user_router.callback_query(F.data == "show_help")
async def show_help(callback: types.CallbackQuery) -> None:
    await callback.message.edit_text(
        f"ℹ️ **Раздел Помощи и Инструкции**\n\n"
        f"**Шаг 1: Активация доступа**\n"
        f"Для использования функций бота необходимо членство в канале **`{TARGET_CHANNEL_URL}`** "
        f"или активация **Промокода** (Кнопка '🔑 Подписка').\n\n"
        f"**Шаг 2: Вход в аккаунт**\n"
        f"Перейдите в **📄 Отчеты и Инструменты → 🔐 Вход в аккаунт** "
        f"и авторизуйтесь через QR или SMS, чтобы запустить мониторинг.\n\n"
        f"**Шаг 3: Работа с отчетами**\n"
        f"После авторизации вам станут доступны **IT-Отчеты** и **Дроп-Отчеты**.\n"
        f"**Шаг 4: Мониторинг задач**\n"
        f"Прогресс запущенных команд (`.лс`, `.чек`) можно отслеживать в разделе **⚙️ Мониторинг Задач**.\n\n"
        f"Если у вас остались вопросы, воспользуйтесь кнопкой **❓ Задать вопрос** в Главном меню.",
        reply_markup=kb_back_to_main(callback.from_user.id)
    )
    await callback.answer()


# --- АДМИН-ПАНЕЛЬ (Редактирование сообщений) ---
@user_router.callback_query(F.data == "admin_panel")
async def show_admin_panel(callback: types.CallbackQuery, state: FSMContext) -> None:
     user_id = callback.from_user.id
     if user_id != ADMIN_ID:
         await callback.answer("🚫 Доступ запрещен.", show_alert=True)
         return
         
     await callback.message.edit_text(
         "🛠️ **Админ-Панель.**\n\n"
         "Выберите действие:",
         reply_markup=kb_admin_panel() 
     )
     await callback.answer()

@user_router.callback_query(F.data == "admin_issue_promo")
async def cmd_admin_issue_promo(callback: types.CallbackQuery, state: FSMContext):
     await callback.message.edit_text(
         "🔑 **Выдача подписки по ID.**\n\n"
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
                             reply_markup=kb_admin_panel())
    except Exception as e:
        logger.error(f"Ошибка при выдаче подписки: {e}")
        await message.answer("❌ Ошибка при активации. Убедитесь, что пользователь уже общался с ботом.",
                             reply_markup=kb_admin_panel())
    
    await state.clear()
    await message.delete()

@user_router.callback_query(F.data == "admin_create_promo")
async def cmd_admin_create_promo(callback: types.CallbackQuery, state: FSMContext):
     await callback.message.edit_text(
         "📅 **Создание Промокода.**\n\n"
         "Введите **срок действия** подписки в **днях** (например, 7, 30 или 365).",
         reply_markup=kb_back_to_main(callback.from_user.id)
     )
     await state.set_state(AdminStates.waiting_for_promo_duration)
     await callback.answer()

@user_router.message(AdminStates.waiting_for_promo_duration, F.text.regexp(r'^\d+$'))
async def process_admin_promo_duration(message: Message, state: FSMContext):
    try:
        duration = int(message.text.strip())
        if duration <= 0:
            raise ValueError
            
        new_code = db_create_promo_code(duration)
        
        await message.answer(
            f"✅ **Промокод успешно создан!**\n\n"
            f"Код: **`{new_code}`**\nСрок действия: **{duration}** дней.",
            reply_markup=kb_admin_panel()
        )
    except ValueError:
        await message.answer("❌ Введите корректное число дней (больше нуля).", reply_markup=kb_admin_panel())
    except Exception as e:
        logger.error(f"Ошибка при создании промокода: {e}")
        await message.answer("❌ Произошла ошибка при сохранении в базу данных.", reply_markup=kb_admin_panel())
    
    await state.clear()
    await message.delete()


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
