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
from telethon.tl.types import PeerUser, Channel, Chat
from telethon.errors.rpcerrorlist import SessionPasswordNeededError

# --- Aiogram 3.x импорты (ОБНОВЛЕНО ДЛЯ 3.7.0+) ---
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from aiogram.client.default import DefaultBotProperties # <-- НОВЫЙ ИМПОРТ!


# =========================================================================
# 0. НАСТРОЙКА ЛОГГИРОВАНИЯ
# =========================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =========================================================================
# I. GLOBAL CONFIG & INITIALIZATION (КЛЮЧИ И ТОКЕНЫ)
# =========================================================================

# --- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ ---
SESSION_DIR = 'data'
if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# --- TELETHON CONFIG (ВАШИ ДАННЫЕ) ---
# ⚠️ ЗАМЕНИТЕ ЭТО НА ВАШИ API_ID и API_HASH
API_ID = 12345678 # Вставьте ваш API ID
API_HASH = 'ВАШ_API_HASH' # Вставьте ваш API Hash

# --- AIOGRAM CONFIG (ВАШ ТОКЕН) ---
TOKEN = '7868097991:AAE745izKWA__gG20IxRoVpgQjnW_RMNjTo' # Ваш токен

# Инициализация Aiogram 3.x (Диспетчер и Роутер)
router = Router() 
dp = Dispatcher(storage=MemoryStorage())


# =========================================================================
# II. IN-MEMORY STATE
# =========================================================================

# ⚠️ ЗАМЕНИТЕ ЭТО НА ВАШ ID (или список ID)
ADMIN_IDS = {123456789} 

# --- TELETHON SINGLE-SESSION STATE ---
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
    # Соединение с БД
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
        conn.commit()

def is_user_admin(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            return result is not None and result[0] == 1
    except sqlite3.OperationalError:
        return False


# =========================================================================
# IV. TELETHON WORKER (ОДНА СЕССИЯ, ПОЛНАЯ РЕАЛИЗАЦИЯ)
# =========================================================================

# --- ЛОГИКА МАССОВОЙ РАССЫЛКИ (.лс) ---
async def send_mass_pm(client, task_id, user_ids_or_usernames, message_text, started_by_id, bot_instance: Bot):
    global ACTIVE_TELETHON_TASKS
    
    task_data = ACTIVE_TELETHON_TASKS.get(task_id)
    if not task_data:
        return

    if not client.is_connected():
        await bot_instance.send_message(started_by_id, f"❌ **Ошибка `.лс`**: Telethon-аккаунт отключен. Задача отменена.")
        ACTIVE_TELETHON_TASKS.pop(task_id, None)
        return
    
    total_recipients = len(user_ids_or_usernames)
    sent_count = 0
    
    for recipient in user_ids_or_usernames:
        if task_data['status'] == 'Stopped': 
            break
            
        try:
            entity = await client.get_entity(recipient)
            await client.send_message(entity, message_text)
            sent_count += 1
            task_data['progress'] = sent_count
            await asyncio.sleep(random.randint(5, 15)) 

        except Exception as e:
            logger.error(f"❌ Ошибка отправки ЛС на {recipient}: {e}")
            continue 
            
    # Отчет по завершению
    status = task_data['status']
    if status == 'Stopped':
        report_message = f"🛑 **Задача ЛС ({task_id}) отменена** пользователем."
    else:
        report_message = f"✅ **Задача ЛС ({task_id}) завершена**.\n"
    
    report_message += f"➡️ Всего отправлено: **{sent_count}/{total_recipients}**\n"
        
    await bot_instance.send_message(started_by_id, report_message)
    ACTIVE_TELETHON_TASKS.pop(task_id, None)


# --- ЛОГИКА ФЛУДА (.флуд) ---
async def send_flood_messages(client, chat_id, message_text, count, delay, started_by_id, bot_instance: Bot):
    """Реализация асинхронной рассылки по списку получателей с контролем количества и задержки."""
    global FLOOD_TASK, FLOOD_TARGET_CHAT
    
    if not client.is_connected():
        await bot_instance.send_message(started_by_id, "❌ **Ошибка `.флуд`**: Telethon-аккаунт отключен.")
        FLOOD_TASK = None
        FLOOD_TARGET_CHAT = None
        return

    try:
        target_entity = await client.get_entity(chat_id)
    except Exception as e:
        await bot_instance.send_message(started_by_id, f"❌ Не удалось получить доступ к текущему чату (ID {chat_id}): {e}")
        FLOOD_TASK = None
        FLOOD_TARGET_CHAT = None
        return

    logger.info(f"🚀 Запуск флуда в чат {chat_id}: {count} сообщений с задержкой {delay}с.")

    sent_count = 0
    
    for i in range(count):
        if FLOOD_TASK is None or FLOOD_TASK.done(): 
             break
             
        try:
            await client.send_message(target_entity, message_text)
            sent_count += 1
            
            if sent_count < count: 
                await asyncio.sleep(delay)

        except errors.FloodWaitError as e:
            wait_time = e.seconds + 5
            await bot_instance.send_message(started_by_id, f"⚠️ **ОЖИДАНИЕ ФЛУДА**: Ждем **{wait_time}** секунд.")
            await asyncio.sleep(wait_time)
            continue 
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при флуде: {e}")
            await bot_instance.send_message(started_by_id, f"❌ Критическая ошибка при флуде: {e}. Флуд остановлен.")
            break

    logger.info(f"✅ Флуд-задача завершена. Отправлено всего: {sent_count}/{count}.")
    await bot_instance.send_message(started_by_id, f"✅ **Флуд завершен**. Всего отправлено: **{sent_count}/{count}**.")
    FLOOD_TASK = None
    FLOOD_TARGET_CHAT = None


async def start_telethon_worker(bot_instance: Bot):
    global TELETHON_CLIENT, TELETHON_RUNNING
    
    if not API_ID or not API_HASH or API_ID == 12345678:
        logger.error("🚫 Telethon не запущен: Отсутствует или не настроен API_ID/API_HASH.")
        return

    if TELETHON_RUNNING:
        logger.warning("🚫 Telethon Worker уже запущен.")
        return

    if not os.path.exists(f'{TELETHON_SESSION_NAME}.session'):
        logger.warning("⚠️ Файл сессии Telethon отсутствует. Запуск отложен до авторизации.")
        TELETHON_RUNNING = False
        return
        
    TELETHON_CLIENT = TelegramClient(TELETHON_SESSION_NAME, API_ID, API_HASH, reconnects=None)
    client = TELETHON_CLIENT
    
    TELETHON_RUNNING = True
    
    # --- РЕГИСТРАЦИЯ ХЕНДЛЕРОВ (ВНУТРИ TELETHON) ---
    
    # 1. .лс 
    @client.on(events.NewMessage(pattern=r'^\.лс (.*)'))
    async def handle_ls_command(event: events.NewMessage):
        sender = await event.get_sender()
        if not is_user_admin(sender.id):
            return
        
        if not client.is_connected():
            await event.reply("❌ **Ошибка:** Telethon-аккаунт не подключен.")
            return

        parts = event.text.split(' ', 2)
        if len(parts) < 3:
            await event.reply("❌ Неверный формат. Используйте: `.лс [юзернейм/ID] [текст сообщения]`")
            return

        recipient_string = parts[1].strip()
        message_text = parts[2].strip()
        
        if recipient_string.startswith('@') or recipient_string.isdigit():
            recipients = [recipient_string]
        else:
             await event.reply("❌ Неверный формат получателя.")
             return
        
        task_id = str(uuid.uuid4())[:8] 
        
        ACTIVE_TELETHON_TASKS[task_id] = {
            'status': 'Running',
            'progress': 0,
            'total': len(recipients)
        }
        
        # Передаем bot_instance для отправки отчетов
        asyncio.create_task(send_mass_pm(client, task_id, recipients, message_text, sender.id, bot_instance))
        
        await event.reply(f"🚀 **Задача ЛС** запущена (ID: `{task_id}`).")

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
        
        if not chat_id_or_link and (event.is_group or event.is_channel):
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
            
            # Используем bot_instance для отправки документа
            await bot_instance.send_document(sender.id, InputFile(output_file), caption=caption)
            os.remove(output_file) 

        except Exception as e:
            await event.reply(f"❌ **Критическая ошибка при сборе:** {e}")


    # 5. .флуд (Запуск рассылки в текущем чате)
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
                 await event.reply(f"⚠️ Флуд уже активен в другом чате (`{FLOOD_TARGET_CHAT}`).")
                 return

        parts = event.text.split(' ', 3)
        if len(parts) < 4:
            await event.reply("❌ Неверный формат.\nИспользуйте: `.флуд [кол-во] [задержка_сек] [текст]`")
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
        
        # Передаем bot_instance для отправки отчетов
        FLOOD_TASK = asyncio.create_task(send_flood_messages(
            client, target_chat_id, message_text, count, delay, sender.id, bot_instance
        ))
        
        await event.reply(f"🚀 Запуск флуда в **текущем чате**...")

    # 6. .флудстоп 
    @client.on(events.NewMessage(pattern=r'^\.флудстоп'))
    async def handle_flood_stop_command(event: events.NewMessage):
        global FLOOD_TASK
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
# V. HANDLERS (AIOGRAM 3.x)
# =========================================================================

# --- FSM для авторизации ---
class Auth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()

# --- Вспомогательные клавиатуры ---
def get_main_menu_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [types.KeyboardButton(text="📄 Отчеты и Инструменты"), types.KeyboardButton(text="📈 Мониторинг задач")]
    ])
    return keyboard

def get_reports_menu_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Вход в аккаунт", callback_data="auth_start")],
        [InlineKeyboardButton(text="⬅️ Назад в Главное меню", callback_data="main_menu")]
    ])
    return keyboard


# V.1. Обработка команды /start
@router.message(F.text == "/start")
async def handle_start(message: types.Message, bot: Bot):
    user_id = message.from_user.id
    if not is_user_admin(user_id):
        await message.answer("🛑 У вас нет прав доступа к боту.")
        return
        
    await message.answer("Привет! Выберите действие:", reply_markup=get_main_menu_keyboard())

# V.2. Меню Отчетов и Инструментов
@router.message(F.text == "📄 Отчеты и Инструменты")
async def handle_reports_menu(message: types.Message, bot: Bot):
    user_id = message.from_user.id
    if not is_user_admin(user_id):
        return
        
    status_text = "❌ Неактивен"
    if TELETHON_CLIENT:
        try:
            if await TELETHON_CLIENT.is_user_authorized():
                status_text = "✅ Авторизован"
            elif TELETHON_CLIENT.is_connected():
                 status_text = "⚠️ Подключен, но не авторизован"
        except Exception:
            pass 

    await message.answer(f"**Статус Telethon:** {status_text}\n\nВыберите действие:", reply_markup=get_reports_menu_keyboard())

# V.3. Обработка callback-ов меню
@router.callback_query(F.data.in_({"auth_start", "main_menu"}))
async def handle_menu_callbacks(callback_query: types.CallbackQuery, state: FSMContext, bot: Bot):
    user_id = callback_query.from_user.id
    if not is_user_admin(user_id):
        await callback_query.answer("🛑 Нет прав доступа.")
        return
        
    if callback_query.data == 'auth_start':
        await handle_auth_step1(callback_query.message, state, bot)
        
    elif callback_query.data == 'main_menu':
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=callback_query.message.message_id,
            text="Выберите действие:",
            reply_markup=None 
        )
    await callback_query.answer()
        
# V.4. Хендлеры авторизации (Шаг 1, 2, 3)
async def handle_auth_step1(message: types.Message, state: FSMContext, bot: Bot):
    await state.set_state(Auth.PHONE)
    await bot.send_message(message.chat.id, "Введите номер телефона для авторизации аккаунта Telethon (в формате +79001234567):")

@router.message(Auth.PHONE)
async def handle_auth_step_phone(message: types.Message, state: FSMContext, bot: Bot):
    await state.update_data(phone=message.text.strip())
    await state.set_state(Auth.CODE)
    
    client = TelegramClient(TELETHON_SESSION_NAME, API_ID, API_HASH)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            await client.send_code_request(message.text.strip())
            await message.answer("Введите код, который пришел на ваш телефон:")
        else:
            await message.answer("⚠️ Аккаунт уже авторизован. Для переавторизации удалите файл сессии.")
            await state.clear()
            
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()

@router.message(Auth.CODE)
async def handle_auth_step2(message: types.Message, state: FSMContext, bot: Bot):
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
                await message.answer("🔒 Требуется двухфакторная аутентификация. Введите пароль:")
                return
            except Exception as e:
                await message.answer(f"❌ Ошибка входа: {e}")
                await state.clear()
                return

            await message.answer(f"✅ Аккаунт @{user.username or user.first_name} успешно авторизован! **Перезапустите бота, чтобы активировать аккаунт.**")
            await state.clear()
            await client.disconnect() 
            
        else:
            await message.answer("⚠️ Аккаунт уже авторизован, попробуйте еще раз.")
            await state.clear()
            
    except Exception as e:
        await message.answer(f"❌ Критическая ошибка: {e}")
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()


@router.message(Auth.PASSWORD)
async def handle_auth_step3(message: types.Message, state: FSMContext, bot: Bot):
    password = message.text.strip()
    
    client = TelegramClient(TELETHON_SESSION_NAME, API_ID, API_HASH)

    try:
        await client.connect()
        user = await client.sign_in(password=password)

        await message.answer(f"✅ Аккаунт @{user.username or user.first_name} успешно авторизован! **Перезапустите бота, чтобы активировать аккаунт.**")
        await state.clear()
        await client.disconnect() 

    except Exception as e:
        await message.answer(f"❌ Ошибка входа с паролем: {e}")
        await state.clear()
    finally:
        if client.is_connected():
            await client.disconnect()


# V.5. Мониторинг задач (Aiogram 3.x)

def get_task_status_message():
    global ACTIVE_TELETHON_TASKS, FLOOD_TASK, FLOOD_TARGET_CHAT
    
    msg = "📊 **Мониторинг активных задач:**\n\n"
    
    if FLOOD_TASK and not FLOOD_TASK.done():
        status_text = 'Работает' if not FLOOD_TASK.cancelling() else 'Запрос на остановку'
        msg += f"🟢 **Флуд** (Chat ID: `{FLOOD_TARGET_CHAT}`)\n"
        msg += f"  Статус: {status_text}\n\n"
    else:
        msg += "🔴 **Флуд** - Неактивен\n\n"
        
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

def get_monitoring_keyboard():
    keyboard = []
    
    # Добавление кнопок отмены, если есть активные .лс задачи
    for task_id, data in ACTIVE_TELETHON_TASKS.items():
        if data['status'] == 'Running':
            keyboard.append([InlineKeyboardButton(text=f"🛑 Отменить .лс {task_id}", callback_data=f"cancel_ls_task_{task_id}")])
    
    # Добавление кнопки отмены флуда
    if FLOOD_TASK and not FLOOD_TASK.done():
        keyboard.append([InlineKeyboardButton(text="🛑 Отменить Флуд", callback_data="cancel_flood_task")])
        
    keyboard.append([InlineKeyboardButton(text="🔄 Обновить статус", callback_data="refresh_status")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


@router.message(F.text == "📈 Мониторинг задач")
async def handle_monitoring_menu(message: types.Message):
    user_id = message.from_user.id
    if not is_user_admin(user_id):
        return
        
    await message.answer(get_task_status_message(), reply_markup=get_monitoring_keyboard())


@router.callback_query(F.data.in_({"refresh_status", "cancel_flood_task"}) | F.data.startswith("cancel_ls_task_"))
async def handle_task_callbacks(callback_query: types.CallbackQuery, bot: Bot):
    user_id = callback_query.from_user.id
    if not is_user_admin(user_id):
        await callback_query.answer("🛑 Нет прав доступа.")
        return
        
    data = callback_query.data
    
    if data == 'refresh_status':
        try:
            # Используем edit_message_text, чтобы обновить сообщение
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text=get_task_status_message(),
                reply_markup=get_monitoring_keyboard()
            )
            await callback_query.answer("Статус обновлен.")
        except Exception:
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
# VI. MAIN EXECUTION (Aiogram 3.x Launch)
# =========================================================================

async def on_startup(bot: Bot):
    create_tables()
    logger.info("Базы данных инициализированы.")
    
    # Запускаем worker при старте
    asyncio.create_task(start_telethon_worker(bot))

async def main():
    # Создаем экземпляр бота (ИСПРАВЛЕНО ДЛЯ AIOGRAM 3.7.0+)
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode='Markdown')) 
    
    # Подключаем роутер к диспетчеру
    dp.include_router(router)
    
    # Запускаем инициализацию и Polling
    await on_startup(bot)
    await dp.start_polling(bot)

if __name__ == '__main__':
    logger.info("🤖 Бот запускается...")
    asyncio.run(main())
