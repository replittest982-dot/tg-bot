import asyncio
import logging
import sqlite3
import os
import time

# Aiogram
from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties

# Telethon
from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel, PeerChat

# --- КОНФИГУРАЦИЯ ---
# ВАЖНО: Обновите эти значения на ваши
BOT_TOKEN = "7868097991:AAH-IVyUWi9ghtRgeU6e8zO6r20xCeAK1P0" 
API_ID = 2623354  # Ваш API_ID для Telethon
API_HASH = 'c02be55627250682c3c6ef139b4d8d17'  # Ваш API_HASH для Telethon
ADMIN_ID = 123456789  # Ваш ID для админ-панели (замените)
DB_NAME = 'monitor_bot.db'

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Глобальные хранилища
ACTIVE_TELETHON_CLIENTS = {}
ACTIVE_TELETHON_WORKERS = {}

# Инициализация Aiogram
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='Markdown'))
dp = Dispatcher()
user_router = Router()

# --- FSM СОСТОЯНИЯ ---

class AuthStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()

class MonitorStates(StatesGroup):
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()

class ReportStates(StatesGroup):
    waiting_report_target = State()
    waiting_report_topic = State()

# --- БАЗА ДАННЫХ ---

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def db_init():
    conn = get_db_connection()
    cur = conn.cursor()
    # Таблица пользователей
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            phone_number TEXT,
            session_file TEXT,
            is_active INTEGER DEFAULT 0,
            it_chat_id TEXT,
            drop_chat_id TEXT,
            report_chat_id TEXT,
            subscription_end_date REAL DEFAULT 0,
            subscription_active INTEGER DEFAULT 0
        )
    """)
    # Таблица логов мониторинга
    cur.execute("""
        CREATE TABLE IF NOT EXISTS monitor_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            timestamp REAL,
            log_type TEXT, -- 'IT' or 'DROP'
            command TEXT,
            target TEXT, -- ID пользователя или чата, к которому относилась команда
            message_text TEXT
        )
    """)
    conn.commit()
    conn.close()

def db_get_user(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    columns = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    conn.close()
    return dict(zip(columns, row)) if row else None

def db_add_monitor_log(user_id, log_type, command, target, message_text):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO monitor_logs (user_id, timestamp, log_type, command, target, message_text) 
        VALUES (?, ?, ?, ?, ?, ?)
    """, (user_id, time.time(), log_type, command, target, message_text))
    conn.commit()
    conn.close()

def db_get_monitor_logs(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT timestamp, log_type, command, target, message_text FROM monitor_logs WHERE user_id=? ORDER BY timestamp DESC", (user_id,))
    logs = cur.fetchall()
    conn.close()
    return logs

def db_get_all_active_users():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id, session_file, it_chat_id, drop_chat_id FROM users WHERE is_active=1")
    rows = cur.fetchall()
    conn.close()
    return rows

def db_check_subscription(user_id):
    user = db_get_user(user_id)
    if not user:
        return False
    return user.get('subscription_active') == 1 and user.get('subscription_end_date', 0) > time.time()

# --- ВСПОМОГАТЕЛЬНЫЕ КЛАВИАТУРЫ ---

def get_cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="cancel_action")]
    ])

def get_monitor_menu_kb() -> InlineKeyboardMarkup:
    """Клавиатура для меню Мониторинга."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настроить IT-чат", callback_data="monitor_set_it_chat")],
        [InlineKeyboardButton(text="⚙️ Настроить DROP-чат", callback_data="monitor_set_drop_chat")],
        [InlineKeyboardButton(text="📨 Настроить Чат для Отчетов", callback_data="monitor_set_report_chat")],
        [InlineKeyboardButton(text="📄 Сформировать Отчет", callback_data="report_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])
    
def get_main_inline_kb(user_id: int) -> InlineKeyboardMarkup:
    user_data = db_get_user(user_id)
    is_active = user_data.get('is_active', 0) if user_data else 0
    worker_status_text = "🟢 Worker запущен" if user_id in ACTIVE_TELETHON_CLIENTS else "🔴 Worker остановлен"
    
    keyboard = [
        [InlineKeyboardButton(text="🔑 Авторизация", callback_data="show_auth_menu")],
        [InlineKeyboardButton(text=worker_status_text, callback_data="toggle_worker")]
    ]
    
    if is_active:
        keyboard.append([InlineKeyboardButton(text="📊 Мониторинг и Отчеты", callback_data="show_monitor_menu")])
    
    if user_id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton(text="👑 Админ-Панель", callback_data="admin_panel")])
        
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# --- TELETHON WORKER ЛОГИКА ---

async def monitor_it_commands(event, user_id, client, it_chat_id):
    """Слушатель для IT-команд: .встал, .кьар, .ошибка."""
    if str(event.chat_id) != it_chat_id:
        return
        
    message_text = event.message.message
    command = None
    target = None
    
    if message_text.lower().startswith('.встал'):
        command = '.встал'
    elif message_text.lower().startswith('.кьар'):
        command = '.кьар'
    elif message_text.lower().startswith('.ошибка'):
        command = '.ошибка'
        
    if command:
        # Пытаемся получить информацию о пользователе, к которому обращена команда
        try:
            if event.message.reply_to_msg_id:
                replied_message = await client.get_messages(it_chat_id, ids=event.message.reply_to_msg_id)
                if replied_message and replied_message.from_id:
                    target_entity = await client.get_entity(replied_message.from_id)
                    target = target_entity.username if hasattr(target_entity, 'username') else str(target_entity.id)
        except Exception:
            target = "Unknown/Self"
            
        db_add_monitor_log(user_id, 'IT', command, target, message_text)
        logger.info(f"[{user_id}] IT Log: {command} in {it_chat_id} (Target: {target})")


async def monitor_drop_commands(event, user_id, client, drop_chat_id):
    """Слушатель для DROP-команд: .лс, .флуд, .чекгруппу."""
    if str(event.chat_id) != drop_chat_id:
        return
        
    message_text = event.message.message
    command = None
    target = None
    
    if message_text.lower().startswith('.лс'):
        command = '.лс'
    elif message_text.lower().startswith('.флуд'):
        command = '.флуд'
    elif message_text.lower().startswith('.чекгруппу'):
        command = '.чекгруппу'
        
    if command:
        # Логика получения таргета аналогична IT-командам
        try:
            if event.message.reply_to_msg_id:
                replied_message = await client.get_messages(drop_chat_id, ids=event.message.reply_to_msg_id)
                if replied_message and replied_message.from_id:
                    target_entity = await client.get_entity(replied_message.from_id)
                    target = target_entity.username if hasattr(target_entity, 'username') else str(target_entity.id)
        except Exception:
            target = "Unknown/Self"
            
        db_add_monitor_log(user_id, 'DROP', command, target, message_text)
        logger.info(f"[{user_id}] DROP Log: {command} in {drop_chat_id} (Target: {target})")


async def run_telethon_worker_for_user(user_id):
    """Запускает Telethon Worker для конкретного пользователя."""
    user_data = db_get_user(user_id)
    if not user_data or not user_data.get('session_file'):
        logger.warning(f"Worker for user {user_id} cannot start: No session file.")
        return

    session_name = user_data['session_file']
    it_chat_id = user_data.get('it_chat_id')
    drop_chat_id = user_data.get('drop_chat_id')

    client = TelegramClient(session_name, API_ID, API_HASH)
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        await client.start()
        logger.info(f"Worker {user_id} started. Monitoring IT: {it_chat_id}, DROP: {drop_chat_id}")
        
        # Добавляем слушателей только для настроенных чатов
        if it_chat_id:
            # Lambda-функция для передачи user_id, client и it_chat_id в хендлер
            handler_it = lambda event: monitor_it_commands(event, user_id, client, it_chat_id)
            client.add_event_handler(handler_it, events.NewMessage(chats=[int(it_chat_id)]))
            
        if drop_chat_id:
            handler_drop = lambda event: monitor_drop_commands(event, user_id, client, drop_chat_id)
            client.add_event_handler(handler_drop, events.NewMessage(chats=[int(drop_chat_id)]))

        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Worker {user_id} crashed: {e}")
        # Очистка клиента после сбоя
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS:
            del ACTIVE_TELETHON_CLIENTS[user_id]
        logger.info(f"Worker {user_id} stopped.")


async def stop_telethon_worker_for_user(user_id):
    """Останавливает Worker для конкретного пользователя."""
    if user_id in ACTIVE_TELETHON_WORKERS:
        task = ACTIVE_TELETHON_WORKERS.pop(user_id)
        task.cancel()
        logger.info(f"Worker task for {user_id} cancelled.")
        
    if user_id in ACTIVE_TELETHON_CLIENTS:
        client = ACTIVE_TELETHON_CLIENTS.pop(user_id)
        if client.is_connected():
            await client.disconnect()
            logger.info(f"Telethon client for {user_id} disconnected.")


async def start_all_active_telethon_workers():
    """Запускает Worker'ы для всех активных пользователей при запуске бота."""
    active_users = db_get_all_active_users()
    for user_id, session_file, it_chat_id, drop_chat_id in active_users:
        if user_id not in ACTIVE_TELETHON_WORKERS:
            # Запускаем Worker в отдельной асинхронной задаче
            task = asyncio.create_task(run_telethon_worker_for_user(user_id))
            ACTIVE_TELETHON_WORKERS[user_id] = task
            logger.info(f"Worker started for {user_id} on startup.")


# --- ХЕНДЛЕРЫ AIOGRAM ---

@user_router.callback_query(F.data == "cancel_action")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Действие отменено.", reply_markup=get_main_inline_kb(callback.from_user.id))
    await callback.answer()

@user_router.callback_query(F.data == "back_to_main")
async def back_to_main_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await start_command(callback.message, state)


# --- 1. Главное меню и /start ---

@user_router.message(CommandStart())
async def start_command(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    db_get_user(user_id) or db_get_user(user_id) # Убедимся, что пользователь есть в БД
    await state.clear()
    
    text = (
        f"👋 **Привет, {message.from_user.full_name}!**\n\n"
        "Это бот для **мониторинга команд** в Telegram-чатах с помощью вашей личной Telethon-сессии.\n"
        "Начните с **Авторизации**."
    )
    await message.answer(text, reply_markup=get_main_inline_kb(user_id))

# --- 2. Управление Worker'ом ---

@user_router.callback_query(F.data == "toggle_worker")
async def toggle_worker_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id in ACTIVE_TELETHON_WORKERS:
        await stop_telethon_worker_for_user(user_id)
        await callback.answer("Worker остановлен.", show_alert=True)
    else:
        user_data = db_get_user(user_id)
        if not user_data or not user_data.get('session_file'):
            await callback.answer("❌ Сначала нужно пройти авторизацию.", show_alert=True)
            return

        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        await callback.answer("Worker запущен.", show_alert=True)
    
    await callback.message.edit_reply_markup(reply_markup=get_main_inline_kb(user_id))


# --- 3. Мониторинг и Отчеты (Настройка) ---

@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    # Проверка подписки (если есть)
    # if not db_check_subscription(user_id) and user_id != ADMIN_ID:
    #     await callback.answer("❌ Необходима активная подписка для доступа к мониторингу.", show_alert=True)
    #     return
    
    user_data = db_get_user(user_id)
    it_chat = user_data.get('it_chat_id') or "Не установлен"
    drop_chat = user_data.get('drop_chat_id') or "Не установлен"
    report_chat = user_data.get('report_chat_id') or "Не установлен"
    
    # Форматирование для отображения
    it_chat_display = f"ID: {it_chat}" if it_chat.startswith('-100') else it_chat
    drop_chat_display = f"ID: {drop_chat}" if drop_chat.startswith('-100') else drop_chat
    report_chat_display = report_chat
    
    text = (
        "📊 **Настройка Мониторинга**\n\n"
        f"Текущие настройки:\n"
        f"• IT-чат: `{it_chat_display}`\n"
        f"• DROP-чат: `{drop_chat_display}`\n"
        f"• Чат для Отчетов: `{report_chat_display}`\n\n"
        "Выберите, какой чат вы хотите настроить."
    )
    await callback.message.edit_text(text, reply_markup=get_monitor_menu_kb())
    await callback.answer()


async def request_chat_id(callback: types.CallbackQuery, state: FSMContext, chat_type: str, fsm_state: State, prompt: str):
    """Общий хендлер для запроса ID/Username чата."""
    user_id = callback.from_user.id
    
    # Для IT/DROP чатов необходим активный Worker
    if chat_type != "Чат для Отчетов" and user_id not in ACTIVE_TELETHON_CLIENTS:
        await callback.answer("❌ Сначала запустите Telethon-сессию (Worker).", show_alert=True)
        return
        
    await state.set_state(fsm_state)
    await state.update_data(chat_type=chat_type)

    await callback.message.edit_text(
        f"💬 **Настройка {chat_type}**\n\n"
        f"{prompt}\n"
        f"Введите: **Username** (напр., `@chat_name`) или **ID** (напр., `-1001234567890`).",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()

@user_router.callback_query(F.data == "monitor_set_it_chat")
async def monitor_set_it_chat_handler(callback: types.CallbackQuery, state: FSMContext):
    await request_chat_id(callback, state, "IT-чат", MonitorStates.waiting_for_it_chat_id, 
                          "IT-чат используется для мониторинга команд, связанных с IT-аккаунтами.")

@user_router.callback_query(F.data == "monitor_set_drop_chat")
async def monitor_set_drop_chat_handler(callback: types.CallbackQuery, state: FSMContext):
    await request_chat_id(callback, state, "DROP-чат", MonitorStates.waiting_for_drop_chat_id, 
                          "DROP-чат используется для мониторинга команд, связанных с дропами.")
                          
@user_router.callback_query(F.data == "monitor_set_report_chat")
async def monitor_set_report_chat_handler(callback: types.CallbackQuery, state: FSMContext):
    await request_chat_id(callback, state, "Чат для Отчетов", ReportStates.waiting_report_target, 
                          "Сюда бот будет отправлять сгенерированные отчеты.")


# --- 4. Обработка ID/Username и Сохранение ---

async def process_chat_id_input(message: Message, state: FSMContext, chat_field_name: str):
    """Общий хендлер для приема, проверки (через Telethon) и сохранения ID/Username чата."""
    user_id = message.from_user.id
    chat_input = message.text.strip()
    data = await state.get_data()
    chat_type = data.get('chat_type', 'Чат')
    
    # Для IT/DROP чатов
    if chat_field_name in ['it_chat_id', 'drop_chat_id']:
        client = ACTIVE_TELETHON_CLIENTS.get(user_id)
        if not client:
            await message.answer("❌ Telethon-сессия не активна. Перезапустите Worker.", reply_markup=get_main_inline_kb(user_id))
            await state.clear()
            return
            
        await message.answer("⌛️ Проверяю доступность чата...")

        # 1. Попытка получить Entity через Telethon
        try:
            # client.connect() не нужен, так как Worker уже запущен и подключен
            entity = await client.get_entity(chat_input)
            
            if not isinstance(entity.peer_id, (PeerChannel, PeerChat)):
                 await message.answer("❌ Введенный объект не является группой, супергруппой или каналом. Введите ID или Username чата.", reply_markup=get_cancel_keyboard())
                 return

            # Получаем канонический ID (Channel ID с префиксом -100)
            if hasattr(entity, 'channel_id'):
                chat_id = f"-100{entity.channel_id}"
            elif hasattr(entity, 'chat_id'):
                chat_id = f"-{entity.chat_id}"
            else:
                chat_id = str(entity.id)
                if not chat_id.startswith('-'):
                     chat_id = f"-{chat_id}"

        except Exception as e:
            logger.error(f"Telethon get_entity error for {user_id}: {e}")
            await message.answer(f"❌ **Ошибка:** Не удалось найти чат по вводу `{chat_input}`. Ошибка: `{type(e).__name__}`", 
                                 reply_markup=get_cancel_keyboard())
            return
    else:
        # Для чата отчетов (report_chat_id) - проверка через Telethon не нужна
        chat_id = chat_input 

    # 2. Сохранение в БД
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"""
        UPDATE users SET {chat_field_name}=? WHERE user_id=?
    """, (chat_id, user_id))
    conn.commit()

    # 3. Уведомление
    await message.answer(f"✅ **{chat_type}** успешно установлен!\n"
                         f"ID/Username: `{chat_id}`.",
                         reply_markup=get_main_inline_kb(user_id))
    
    # 4. Перезапуск Worker'а для обновления хендлеров (только для IT/DROP)
    if chat_field_name in ['it_chat_id', 'drop_chat_id']:
        await stop_telethon_worker_for_user(user_id)
        task = asyncio.create_task(run_telethon_worker_for_user(user_id))
        ACTIVE_TELETHON_WORKERS[user_id] = task
        
    await state.clear()


@user_router.message(MonitorStates.waiting_for_it_chat_id)
async def monitor_process_it_chat_id(message: Message, state: FSMContext):
    await process_chat_id_input(message, state, 'it_chat_id')

@user_router.message(MonitorStates.waiting_for_drop_chat_id)
async def monitor_process_drop_chat_id(message: Message, state: FSMContext):
    await process_chat_id_input(message, state, 'drop_chat_id')
    
@user_router.message(ReportStates.waiting_report_target)
async def monitor_process_report_chat_id(message: Message, state: FSMContext):
    await process_chat_id_input(message, state, 'report_chat_id')


# --- 5. Генерация Отчета ---

@user_router.callback_query(F.data == "report_start")
async def report_start_handler(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await state.set_state(ReportStates.waiting_report_topic)
    await callback.message.edit_text(
        "📄 **Генерация Отчета**\n\n"
        "Введите **тему отчета** (напр., `Отчет за 25.11`) или **команду для фильтрации** (напр., `.встал`).",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()

@user_router.message(ReportStates.waiting_report_topic)
async def report_process_topic_and_send(message: Message, state: FSMContext):
    user_id = message.from_user.id
    topic = message.text.strip()
    
    logs = db_get_monitor_logs(user_id)
    user_data = db_get_user(user_id)
    report_chat = user_data.get('report_chat_id')
    
    if not logs:
        await message.answer("⚠️ Логи мониторинга отсутствуют. Отчет не может быть сформирован.", reply_markup=get_main_inline_kb(user_id))
        await state.clear()
        return

    # 1. Формирование текста отчета
    report_text = f"**{topic}**\n\n"
    
    if topic.startswith('.'): # Фильтр по команде
        filtered_logs = [log for log in logs if log[2] and log[2].lower().startswith(topic.lower())]
        report_text += f"**Фильтр по команде:** `{topic}`\n"
    else:
        filtered_logs = logs
        report_text += f"**Общий отчет:**\n"
    
    report_text += "--- Logs ---\n"
    
    # Добавляем записи
    if filtered_logs:
        for timestamp, log_type, command, target, msg_text in filtered_logs[:50]: # Ограничимся 50 записями
            # Форматируем метку времени в читаемый вид
            dt_object = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            report_text += f"`[{dt_object}]` **{log_type}**: {command or 'N/A'} (Target: {target or 'N/A'})\n"
        
        report_text += f"\n--- Конец Отчета (показано {len(filtered_logs[:50])}/{len(filtered_logs)} записей) ---"
    else:
        report_text += "Нет записей, соответствующих фильтру."


    # 2. Отправка
    if report_chat:
        try:
            await bot.send_message(report_chat, report_text, disable_web_page_preview=True)
            await message.answer(f"✅ Отчет **'{topic}'** успешно отправлен в чат `{report_chat}`.", reply_markup=get_main_inline_kb(user_id))
        except Exception as e:
            await message.answer(f"❌ **Ошибка отправки отчета** в чат `{report_chat}`. Проверьте ID/Username и права бота. Ошибка: `{type(e).__name__}`. Текст отчета будет отправлен сюда.", reply_markup=get_main_inline_kb(user_id))
            await message.answer(report_text, disable_web_page_preview=True) # Отправляем в ЛС как запасной вариант
    else:
        await message.answer(f"⚠️ Чат для отчетов не настроен. Отчет отправлен вам в ЛС.\n\n" + report_text, reply_markup=get_main_inline_kb(user_id), disable_web_page_preview=True)

    await state.clear()

# --- 6. Авторизация (Скелет, должен быть реализован отдельно) ---

# ... Здесь должны быть хендлеры для AuthStates.waiting_for_phone, 
# AuthStates.waiting_for_code и AuthStates.waiting_for_password ...

# --- 7. Запуск Бота ---

async def main():
    logger.info("Запуск бота...")
    
    db_init()
    logger.info("База данных инициализирована.")

    dp.include_router(user_router)
    
    # Запуск Worker'ов, которые должны работать
    await start_all_active_telethon_workers()

    # Запуск polling Aiogram
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
    except Exception as e:
        logger.critical(f"Критическая ошибка в main: {e}")
