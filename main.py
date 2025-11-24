import os
import asyncio
import logging
import sqlite3
import random
import uuid
from datetime import datetime
from typing import Optional

# --- Телетон импорты ---
from telethon import TelegramClient, events, errors
from telethon.tl.types import PeerUser, Channel, Chat, InputPeerUser, User, InputPeerChannel
from telethon.errors.rpcerrorlist import SessionPasswordNeededError

# --- Aiogram импорты ---
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

# =========================================================================
# 0. НАСТРОЙКА ЛОГГИРОВАНИЯ
# =========================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =========================================================================
# I. GLOBAL CONFIG & INITIALIZATION
# =========================================================================

# --- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ ---
SESSION_DIR = 'data'
if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# --- TELETHON CONFIG (ВАШИ ДАННЫЕ) ---
# Убедитесь, что эти переменные установлены в вашем окружении (или замените 'os.getenv' на фактические значения)
API_ID = os.getenv('API_ID') 
API_HASH = os.getenv('API_HASH') 

# --- AIOGRAM CONFIG ---
TOKEN = os.getenv('BOT_TOKEN') 
bot = Bot(token=TOKEN, parse_mode=types.ParseMode.MARKDOWN)
dp = Dispatcher(bot, storage=MemoryStorage())


# =========================================================================
# II. IN-MEMORY STATE
# =========================================================================

# Список ID администраторов (должен быть заполнен)
ADMIN_IDS = {123456789} # <-- ЗАМЕНИТЕ НА СВОЙ ID!

# --- TELETHON SINGLE-SESSION STATE (УСТОЙЧИВАЯ ОДНА СЕССИЯ) ---
TELETHON_SESSION_NAME = f'{SESSION_DIR}/telethon_session_{API_ID}'
TELETHON_CLIENT: Optional[TelegramClient] = None
TELETHON_RUNNING: bool = False
ACTIVE_TELETHON_TASKS: dict = {} # Задачи .лс
FLOOD_TASK: Optional[asyncio.Task] = None # Задача .флуд
FLOOD_TARGET_CHAT: Optional[int] = None # Целевой чат для .флуд


# =========================================================================
# III. DATABASE FUNCTIONS
# =========================================================================

DATABASE_NAME = 'bot_db.db'

def get_db_connection():
    # Используем стандартный sqlite3, но стараемся закрывать соединение быстро.
    return sqlite3.connect(DATABASE_NAME)

def create_tables():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                is_admin BOOLEAN DEFAULT 0
            );
        """)
        # Удаляем telethon_sessions, так как мы вернулись к одной сессии
        conn.commit()

# --- Вспомогательные функции DB ---
def is_user_admin(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        return result is not None and result[0] == 1

# =========================================================================
# IV. TELETHON WORKER (ОДНА СЕССИЯ, ПОЛНАЯ РЕАЛИЗАЦИЯ)
# =========================================================================

# --- ЛОГИКА МАССОВОЙ РАССЫЛКИ (.лс) ---
async def send_mass_pm(client, task_id, user_ids_or_usernames, message_text, started_by_id):
    global ACTIVE_TELETHON_TASKS
    
    task_data = ACTIVE_TELETHON_TASKS.get(task_id)
    if not task_data:
        logger.error(f"Задача {task_id} не найдена в ACTIVE_TELETHON_TASKS.")
        return

    if not client.is_connected():
        await bot.send_message(started_by_id, f"❌ **Ошибка `.лс`**: Telethon-аккаунт отключен. Задача отменена.")
        ACTIVE_TELETHON_TASKS.pop(task_id, None)
        return
    
    total_recipients = len(user_ids_or_usernames)
    sent_count = 0
    
    # --- Основной цикл рассылки ---
    for recipient in user_ids_or_usernames:
        if task_data['status'] == 'Stopped': 
            break
            
        try:
            entity = await client.get_entity(recipient)
            await client.send_message(entity, message_text)
            sent_count += 1
            task_data['progress'] = sent_count
            
            # Пауза для избежания бана
            await asyncio.sleep(random.randint(5, 15)) 

        except Exception as e:
            logger.error(f"❌ Ошибка отправки ЛС на {recipient}: {e}")
            # Просто пропускаем, чтобы не останавливать всю рассылку
            continue 
            
    # --- Отчет по завершению ---
    status = task_data['status']
    if status == 'Stopped':
        report_message = f"🛑 **Задача ЛС ({task_id}) отменена** пользователем."
    else:
        report_message = f"✅ **Задача ЛС ({task_id}) завершена**.\n"
    
    report_message += f"➡️ Всего отправлено: **{sent_count}/{total_recipients}**\n"
    report_message += f"⏳ Время завершения: {datetime.now().strftime('%H:%M:%S')}"
        
    await bot.send_message(started_by_id, report_message)
    ACTIVE_TELETHON_TASKS.pop(task_id, None)


# --- ЛОГИКА ФЛУДА (.флуд) ---
async def send_flood_messages(client, chat_id, message_text, count, delay, started_by_id):
    """Реализация асинхронной рассылки по списку получателей с контролем количества и задержки."""
    global FLOOD_TASK, FLOOD_TARGET_CHAT
    
    if not client.is_connected():
        await bot.send_message(started_by_id, "❌ **Ошибка `.флуд`**: Telethon-аккаунт отключен.")
        FLOOD_TASK = None
        FLOOD_TARGET_CHAT = None
        return

    try:
        # Используем chat_id, который уже является числовым ID текущего чата
        target_entity = await client.get_entity(chat_id)
    except Exception as e:
        await bot.send_message(started_by_id, f"❌ Не удалось получить доступ к текущему чату (ID {chat_id}): {e}")
        FLOOD_TASK = None
        FLOOD_TARGET_CHAT = None
        return

    logger.info(f"🚀 Запуск флуда в чат {chat_id}: {count} сообщений с задержкой {delay}с.")

    sent_count = 0
    
    for i in range(count):
        # Проверяем на отмену
        if FLOOD_TASK is None or FLOOD_TASK.done(): 
             break
             
        try:
            await client.send_message(target_entity, message_text)
            sent_count += 1
            
            # Пауза
            if sent_count < count: 
                await asyncio.sleep(delay)

        except errors.FloodWaitError as e:
            wait_time = e.seconds + 5
            await bot.send_message(started_by_id, f"⚠️ **ОЖИДАНИЕ ФЛУДА**: Ждем **{wait_time}** секунд.")
            await asyncio.sleep(wait_time)
            # Продолжаем цикл с того же сообщения после ожидания
            continue 
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при флуде: {e}")
            await bot.send_message(started_by_id, f"❌ Критическая ошибка при флуде: {e}. Флуд остановлен.")
            break

    logger.info(f"✅ Флуд-задача завершена. Отправлено всего: {sent_count}/{count}.")
    await bot.send_message(started_by_id, f"✅ **Флуд завершен**. Всего отправлено: **{sent_count}/{count}**.")
    FLOOD_TASK = None
    FLOOD_TARGET_CHAT = None


async def start_telethon_worker(bot: Bot, dp: Dispatcher):
    global TELETHON_CLIENT, TELETHON_RUNNING
    
    if not API_ID or not API_HASH:
        logger.error("🚫 Telethon не запущен: Отсутствует API_ID или API_HASH.")
        return

    if TELETHON_RUNNING:
        logger.warning("🚫 Telethon Worker уже запущен.")
        return

    if not os.path.exists(f'{TELETHON_SESSION_NAME}.session'):
        logger.warning("⚠️ Файл сессии Telethon отсутствует. Запуск отложен до авторизации.")
        TELETHON_RUNNING = False
        return
        
    # --- ИНИЦИАЛИЗАЦИЯ КЛИЕНТА (УСТОЙЧИВОСТЬ К БЛОКИРОВКЕ) ---
    # reconnects=None предотвращает попытки Telethon сохранять сессию в фоне,
    # что минимизирует конфликт с SQLite3.
    TELETHON_CLIENT = TelegramClient(TELETHON_SESSION_NAME, API_ID, API_HASH, reconnects=None)
    client = TELETHON_CLIENT
    
    TELETHON_RUNNING = True
    
    # --- РЕГИСТРАЦИЯ ХЕНДЛЕРОВ ---
    
    # 1. .лс (Массовая рассылка)
    @client.on(events.NewMessage(pattern=r'^\.лс (.*)'))
    async def handle_ls_command(event: events.NewMessage):
        sender = await event.get_sender()
        if not is_user_admin(sender.id):
            return
        
        if not client.is_connected():
            await event.reply("❌ **Ошибка:** Telethon-аккаунт не подключен (Disconnected).")
            return

        parts = event.text.split(' ', 2)
        if len(parts) < 3:
            await event.reply("❌ Неверный формат. Используйте: `.лс [юзернейм/ID] [текст сообщения]`")
            return

        recipient_string = parts[1].strip()
        message_text = parts[2].strip()
        
        # Парсинг получателей (можно расширить для списка)
        if recipient_string.startswith('@') or recipient_string.isdigit():
            recipients = [recipient_string]
        else:
             await event.reply("❌ Неверный формат получателя. Используйте username или ID.")
             return
        
        task_id = str(uuid.uuid4())[:8] 
        
        ACTIVE_TELETHON_TASKS[task_id] = {
            'status': 'Running',
            'progress': 0,
            'total': len(recipients)
        }
        
        asyncio.create_task(send_mass_pm(client, task_id, recipients, message_text, sender.id))
        
        await event.reply(f"🚀 **Задача ЛС** запущена (ID: `{task_id}`). Получателей: {len(recipients)}")

    # 2. .чек лс (Проверка статуса)
    @client.on(events.NewMessage(pattern=r'^\.чек лс'))
    async def handle_check_ls_command(event: events.NewMessage):
        sender = await event.get_sender()
        if not is_user_admin(sender.id):
            return
        
        if not ACTIVE_TELETHON_TASKS:
            await event.reply("ℹ️ Активных задач рассылки `.лс` нет.")
            return

        msg = "📊 **Статус активных задач ЛС:**\n\n"
        for task_id, data in ACTIVE_TELETHON_TASKS.items():
            status_emoji = '🔄' if data['status'] == 'Running' else '🛑'
            msg += f"• **ID:** `{task_id}` {status_emoji}\n"
            msg += f"  **Статус:** {data['status']}\n"
            msg += f"  **Прогресс:** {data.get('progress', 0)}/{data.get('total', '??')}\n\n"
        
        await event.reply(msg)

    # 3. .лсстоп
    @client.on(events.NewMessage(pattern=r'^\.лсстоп (\w+)'))
    async def handle_ls_stop_command(event: events.NewMessage):
        sender = await event.get_sender()
        if not is_user_admin(sender.id):
            return
        
        task_id = event.pattern_match.group(1).strip()
        if task_id in ACTIVE_TELETHON_TASKS:
            ACTIVE_TELETHON_TASKS[task_id]['status'] = 'Stopped'
            await event.reply(f"🛑 Задача ЛС `{task_id}` помечена для остановки. Дождитесь завершения текущей отправки.")
        else:
            await event.reply(f"❌ Задача с ID `{task_id}` не найдена.")

    # 4. .чекгруппу (Сбор списка участников группы)
    @client.on(events.NewMessage(pattern=r'^\.чекгруппу ?(.*)'))
    async def handle_check_group_command(event: events.NewMessage):
        sender = await event.get_sender()
        if not is_user_admin(sender.id):
            return
        
        if not client.is_connected():
            await event.reply("❌ **Ошибка:** Telethon-аккаунт не подключен.")
            return

        chat_id_or_link = event.pattern_match.group(1).strip()
        
        # Если аргумент пуст, используем ID текущего чата
        if not chat_id_or_link and event.is_group or event.is_channel:
            chat_id_or_link = event.chat_id
        elif not chat_id_or_link:
            await event.reply("❌ **Ошибка:** Укажите ID/Link группы или вызовите команду в группе.")
            return

        await event.reply(f"🔎 Начинаю сбор участников из `{chat_id_or_link}`. Ожидайте...")
        
        try:
            entity = await client.get_entity(chat_id_or_link)
            
            if not isinstance(entity, (types.Channel, types.Chat)):
                await event.reply("❌ **Ошибка:** Указанный объект не является группой или каналом.")
                return

            participants_list = []
            async for user in client.iter_participants(entity):
                username = f"@{user.username}" if user.username else "нет username"
                participants_list.append(f"ID: {user.id}, Username: {username}")
                
            total_count = len(participants_list)
            
            output_file = f"participants_{entity.id}.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(participants_list))
                
            caption = f"✅ **Сбор участников завершен** из `{chat_id_or_link}`.\n➡️ Всего участников: **{total_count}**"
            
            # Отправка файла через Aiogram, так как он более надежен в ЛС
            await bot.send_document(sender.id, types.InputFile(output_file), caption=caption)
            os.remove(output_file) 

        except errors.ChatAdminRequiredError:
            await event.reply("❌ **Ошибка:** Для сбора участников требуются права администратора в этом чате/канале.")
        except Exception as e:
            logger.error(f"Ошибка при .чекгруппу: {e}")
            await event.reply(f"❌ **Критическая ошибка при сборе:** {e}")


    # 5. .флуд (Запуск рассылки в текущем чате с кол-вом и задержкой)
    @client.on(events.NewMessage(pattern=r'^\.флуд (.*)'))
    async def handle_flood_start_command(event: events.NewMessage):
        global FLOOD_TASK, FLOOD_TARGET_CHAT
        sender = await event.get_sender()
        if not is_user_admin(sender.id):
            return

        target_chat_id = event.chat_id
        
        if FLOOD_TASK and not FLOOD_TASK.done():
            if FLOOD_TARGET_CHAT == target_chat_id:
                await event.reply(f"⚠️ Флуд уже активен в этом чате. Сначала остановите его командой `.флудстоп`.")
                return
            else:
                 await event.reply(f"⚠️ Флуд уже активен в другом чате (`{FLOOD_TARGET_CHAT}`). Дождитесь завершения или остановите его.")
                 return

        # Формат: .флуд [кол-во] [задержка] [текст]
        parts = event.text.split(' ', 3)
        if len(parts) < 4:
            await event.reply("❌ Неверный формат.\nИспользуйте: `.флуд [кол-во] [задержка_сек] [текст]`\nПример: `.флуд 10 5 Привет!`")
            return
            
        try:
            count = int(parts[1].strip())
            delay = int(parts[2].strip())
            if count <= 0 or delay < 1:
                 await event.reply("❌ Количество должно быть > 0, задержка >= 1 секунды.")
                 return
        except ValueError:
            await event.reply("❌ Кол-во сообщений и задержка должны быть целыми числами.")
            return
            
        message_text = parts[3].strip()
        
        FLOOD_TARGET_CHAT = target_chat_id
        
        FLOOD_TASK = asyncio.create_task(send_flood_messages(
            client, target_chat_id, message_text, count, delay, sender.id
        ))
        
        await event.reply(f"🚀 Запуск флуда в **текущем чате**...")

    # 6. .флудстоп (Остановка рассылки)
    @client.on(events.NewMessage(pattern=r'^\.флудстоп'))
    async def handle_flood_stop_command(event: events.NewMessage):
        global FLOOD_TASK, FLOOD_TARGET_CHAT
        sender = await event.get_sender()
        if not is_user_admin(sender.id):
            return

        if FLOOD_TASK and not FLOOD_TASK.done():
            FLOOD_TASK.cancel() 
            await event.reply("🛑 Флуд-задача запросила остановку. Ожидайте завершения.")
        else:
            await event.reply("ℹ️ Активная флуд-задача не найдена.")
            
    # --- ЗАПУСК КЛИЕНТА ---
    try:
        await client.start()
        user = await client.get_me()
        logger.info(f"✅ Telethon запущен как: {user.username or user.first_name}")
        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка Telethon: {e}")
        
    TELETHON_RUNNING = False
    logger.info("Telethon Worker завершил работу.")


# =========================================================================
# V. HANDLERS (AIOGRAM)
# =========================================================================

# --- FSM для авторизации ---
class Auth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()

# --- Вспомогательные клавиатуры ---
def get_main_menu_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.row("📄 Отчеты и Инструменты", "📈 Мониторинг задач")
    return keyboard

def get_reports_menu_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        types.InlineKeyboardButton("🔐 Вход в аккаунт", callback_data="auth_start"),
        types.InlineKeyboardButton("⬅️ Назад в Главное меню", callback_data="main_menu")
    )
    return keyboard

# V.1. Обработка команды /start
@dp.message_handler(commands=['start'], state='*')
async def handle_start(message: types.Message):
    user_id = message.from_user.id
    if not is_user_admin(user_id):
        await message.reply("🛑 У вас нет прав доступа к боту.")
        return
        
    await message.reply("Привет! Выберите действие:", reply_markup=get_main_menu_keyboard())
    
    # Запускаем worker при /start, если он еще не запущен
    if not TELETHON_RUNNING:
        asyncio.create_task(start_telethon_worker(bot, dp))


# V.2. Меню Отчетов и Инструментов
@dp.message_handler(text="📄 Отчеты и Инструменты", state='*')
async def handle_reports_menu(message: types.Message):
    user_id = message.from_user.id
    if not is_user_admin(user_id):
        return
        
    status = "✅ Активен" if TELETHON_CLIENT and TELETHON_CLIENT.is_connected() else "❌ Неактивен"
    
    await message.reply(f"**Статус Telethon:** {status}\n\nВыберите действие:", reply_markup=get_reports_menu_keyboard())

# V.3. Обработка callback-ов меню
@dp.callback_query_handler(lambda c: c.data == 'auth_start' or c.data == 'main_menu', state='*')
async def handle_menu_callbacks(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    if not is_user_admin(user_id):
        await callback_query.answer("🛑 Нет прав доступа.")
        return
        
    if callback_query.data == 'auth_start':
        await handle_auth_step1(callback_query.message, state) # Переходим к авторизации
        
    elif callback_query.data == 'main_menu':
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=callback_query.message.message_id,
            text="Выберите действие:",
            reply_markup=None # Удаляем inline-клавиатуру
        )
        await callback_query.answer()
        
# V.4. Хендлеры авторизации (Шаг 1, 2, 3)
# Передаем управление команде /auth (которая невидима в меню)
async def handle_auth_step1(message: types.Message, state: FSMContext):
    await state.set_state(Auth.PHONE)
    await bot.send_message(message.chat.id, "Введите номер телефона для авторизации аккаунта Telethon (в формате +79001234567):")

@dp.message_handler(state=Auth.PHONE)
async def handle_auth_step_phone(message: types.Message, state: FSMContext):
    await state.update_data(phone=message.text.strip())
    await state.set_state(Auth.CODE)
    
    # --- Запуск клиента для получения кода ---
    client = TelegramClient(TELETHON_SESSION_NAME, API_ID, API_HASH)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            await client.send_code_request(message.text.strip())
            await message.reply("Введите код, который пришел на ваш телефон:")
        else:
            await message.reply("⚠️ Аккаунт уже авторизован. Для переавторизации удалите файл сессии.")
            await state.finish()
            
    except Exception as e:
        await message.reply(f"❌ Ошибка: {e}")
        await state.finish()
    finally:
        if client.is_connected():
            await client.disconnect()

@dp.message_handler(state=Auth.CODE)
async def handle_auth_step2(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    phone_number = data['phone']
    code = message.text.strip()
    
    client = TelegramClient(TELETHON_SESSION_NAME, API_ID, API_HASH)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            
            try:
                user = await client.sign_in(phone_number, code)
            except SessionPasswordNeededError:
                await state.set_state(Auth.PASSWORD)
                await message.reply("🔒 Требуется двухфакторная аутентификация. Введите пароль:")
                return
            except Exception as e:
                await message.reply(f"❌ Ошибка входа: {e}")
                await state.finish()
                return

            await message.reply(f"✅ Аккаунт @{user.username or user.first_name} успешно авторизован! **Перезапустите бота, чтобы активировать аккаунт.**")
            await state.finish()
            await client.disconnect() 
            
        else:
            await message.reply("⚠️ Аккаунт уже авторизован, попробуйте еще раз.")
            await state.finish()
            
    except Exception as e:
        await message.reply(f"❌ Критическая ошибка: {e}")
        await state.finish()
    finally:
        if client.is_connected():
            await client.disconnect()


@dp.message_handler(state=Auth.PASSWORD)
async def handle_auth_step3(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    password = message.text.strip()
    
    client = TelegramClient(TELETHON_SESSION_NAME, API_ID, API_HASH)

    try:
        await client.connect()
        user = await client.sign_in(password=password)

        await message.reply(f"✅ Аккаунт @{user.username or user.first_name} успешно авторизован! **Перезапустите бота, чтобы активировать аккаунт.**")
        await state.finish()
        await client.disconnect() 

    except Exception as e:
        await message.reply(f"❌ Ошибка входа с паролем: {e}")
        await state.finish()
    finally:
        if client.is_connected():
            await client.disconnect()


# V.5. Мониторинг задач (для кнопки)

def get_task_status_message():
    global ACTIVE_TELETHON_TASKS, FLOOD_TASK, FLOOD_TARGET_CHAT
    
    msg = "📊 **Мониторинг активных задач:**\n\n"
    
    # 1. Статус Флуд-задачи
    if FLOOD_TASK and not FLOOD_TASK.done():
        status_text = 'Работает' if not FLOOD_TASK.cancelling() else 'Запрос на остановку'
        msg += f"🟢 **Флуд** (Chat ID: `{FLOOD_TARGET_CHAT}`)\n"
        msg += f"  Статус: {status_text}\n\n"
    else:
        msg += "🔴 **Флуд** - Неактивен\n\n"
        
    # 2. Статус .лс задач
    if ACTIVE_TELETHON_TASKS:
        msg += "📈 **Массовая рассылка (.лс)**:\n"
        for task_id, data in ACTIVE_TELETHON_TASKS.items():
            progress = f"{data.get('progress', 0)}/{data.get('total', '??')}"
            status_emoji = '🔄' if data['status'] == 'Running' else '🛑'
            
            msg += f"• `{task_id}`: {status_emoji} **{data['status']}** ({progress})\n"
        msg += "\n"
    else:
        msg += "ℹ️ Активных задач `.лс` нет.\n\n"
        
    return msg

@dp.message_handler(text="📈 Мониторинг задач", state='*')
async def handle_monitoring_menu(message: types.Message):
    user_id = message.from_user.id
    if not is_user_admin(user_id):
        return
        
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    
    # Добавление кнопок отмены, если есть активные .лс задачи
    for task_id, data in ACTIVE_TELETHON_TASKS.items():
        if data['status'] == 'Running':
            keyboard.add(types.InlineKeyboardButton(f"🛑 Отменить .лс {task_id}", callback_data=f"cancel_ls_task_{task_id}"))
    
    # Добавление кнопки отмены флуда
    if FLOOD_TASK and not FLOOD_TASK.done():
        keyboard.add(types.InlineKeyboardButton("🛑 Отменить Флуд", callback_data="cancel_flood_task"))
        
    keyboard.add(types.InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status"))

    await message.reply(get_task_status_message(), reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data.startswith('cancel_ls_task_') or c.data == 'refresh_status' or c.data == 'cancel_flood_task', state='*')
async def handle_task_callbacks(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if not is_user_admin(user_id):
        await callback_query.answer("🛑 Нет прав доступа.")
        return
        
    data = callback_query.data
    
    if data == 'refresh_status':
        try:
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text=get_task_status_message(),
                reply_markup=callback_query.message.reply_markup 
            )
            await callback_query.answer("Статус обновлен.")
        except:
             await callback_query.answer("Статус не изменился.")

    elif data.startswith('cancel_ls_task_'):
        task_id = data.split('_')[-1]
        if task_id in ACTIVE_TELETHON_TASKS and ACTIVE_TELETHON_TASKS[task_id]['status'] == 'Running':
            ACTIVE_TELETHON_TASKS[task_id]['status'] = 'Stopped'
            await callback_query.answer(f"Задача .лс {task_id} помечена на остановку.")
        else:
            await callback_query.answer(f"Задача {task_id} не найдена или уже остановлена.")

    elif data == 'cancel_flood_task':
        global FLOOD_TASK
        if FLOOD_TASK and not FLOOD_TASK.done():
            FLOOD_TASK.cancel()
            await callback_query.answer("Флуд-задача запросила остановку.")
        else:
            await callback_query.answer("Активная флуд-задача не найдена.")
    
    await callback_query.answer()


# =========================================================================
# VI. MAIN EXECUTION
# =========================================================================

async def on_startup(dispatcher):
    create_tables()
    logger.info("Базы данных инициализированы.")
    
    # Запускаем worker при старте
    asyncio.create_task(start_telethon_worker(bot, dp))

if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
