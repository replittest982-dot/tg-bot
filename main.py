import asyncio
import logging
import os
import sqlite3
import pytz
import re
import tempfile 
import io 
import random 
import string 
from datetime import datetime, timedelta

# Импорты aiogram
from aiogram import Bot, Dispatcher, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command 
from aiogram.client.default import DefaultBotProperties

# Импорты telethon
from telethon import TelegramClient, events
from telethon.tl.types import User
from telethon.errors import (
    UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, 
    PhoneNumberInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError, 
    AuthKeyUnregisteredError, PasswordHashInvalidError, UsernameInvalidError, 
    PeerIdInvalidError, RpcCallFailError
)
from telethon.utils import get_display_name 

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# !!! ВАШ НОВЫЙ BOT_TOKEN !!!
BOT_TOKEN = "7868097991:AAGdvAOa1-jxMaBnJHfbj6j1BC63AD1aE9I" 
ADMIN_ID = 6256576302  
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1" 
BOT_USERNAME = "@STATPBot" # Укажите юзернейм вашего бота для сообщений об отчетах
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
DB_TIMEOUT = 10 

# --- КОНФИГУРАЦИЯ ПРОКСИ ---
# Если вы на европейском сервере, оставьте None. Если ошибки (EOFError) повторятся, заполните.
PROXY_CONFIG = None 
# Пример для SOCKS5 прокси (раскомментировать, если понадобится):
# PROXY_CONFIG = (
#     'socks5',   
#     '12.34.56.78', 
#     1080,       
#     True,       
#     'ЛОГИН_ПРОКСИ', 
#     'ПАРОЛЬ_ПРОКСИ'
# )
# --------------------------------------------------------

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальные хранилища
ACTIVE_TELETHON_CLIENTS = {} 
ACTIVE_TELETHON_WORKERS = {} 
TEMP_AUTH_CLIENTS = {} 
FLOOD_TASKS = {} 
PROCESS_PROGRESS = {} 

storage = MemoryStorage()
# Инициализация бота
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=storage)

# =========================================================================
# II. FSM СОСТОЯНИЯ
# =========================================================================

class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State() 
    WAITING_FOR_QR_LOGIN = State()

class PromoStates(StatesGroup):
    waiting_for_code = State()

class AdminStates(StatesGroup):
    main_menu = State()
    promo_days_input = State()
    promo_uses_input = State()
    sub_user_id_input = State()
    sub_days_input = State()

# =========================================================================
# III. БАЗА ДАННЫХ
# =========================================================================

DB_PATH = os.path.join('data', DB_NAME)

def get_db_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT)

def db_init():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, 
                subscription_active BOOLEAN DEFAULT 0,
                subscription_end_date TEXT, 
                telethon_active BOOLEAN DEFAULT 0
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY, 
                days INTEGER, 
                is_active BOOLEAN DEFAULT 1,
                max_uses INTEGER, 
                current_uses INTEGER DEFAULT 0
        )""")

def db_get_user(user_id):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if row:
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
        return None

def db_check_subscription(user_id):
    user = db_get_user(user_id)
    if not user or not user.get('subscription_active'): return False
    try:
        end = TIMEZONE_MSK.localize(datetime.strptime(user.get('subscription_end_date'), '%Y-%m-%d %H:%M:%S'))
        return end > datetime.now(TIMEZONE_MSK)
    except: return False

def db_update_subscription(user_id, days):
    with get_db_connection() as conn:
        cur = conn.cursor()
        user = db_get_user(user_id)
        now = datetime.now(TIMEZONE_MSK)
        current_end = user.get('subscription_end_date')
        
        start_date = now
        if current_end:
            try:
                ce = TIMEZONE_MSK.localize(datetime.strptime(current_end, '%Y-%m-%d %H:%M:%S'))
                if ce > now: start_date = ce
            except: pass
            
        new_end = (start_date + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        cur.execute("UPDATE users SET subscription_active=1, subscription_end_date=? WHERE user_id=?", (new_end, user_id))
        return new_end

def db_set_session_status(user_id, status):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))

def db_get_promo(code):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM promo_codes WHERE code=?", (code,))
        row = cur.fetchone()
        if row:
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
        return None

def db_use_promo(code):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code=?", (code,))

def db_add_promo(code, days, max_uses):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO promo_codes (code, days, max_uses) VALUES (?, ?, ?)", (code, days, max_uses))

def db_get_active_telethon_users():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
        return [row[0] for row in cur.fetchall()]


# =========================================================================
# IV. УТИЛИТЫ И КЛАВИАТУРЫ
# =========================================================================

def get_session_path(user_id):
    os.makedirs('data', exist_ok=True)
    return os.path.join('data', f'session_{user_id}')
    
def generate_promo_code(length=10):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

async def check_access(user_id: int, bot: Bot):
    if user_id == ADMIN_ID: 
        return True, ""

    channel_subscribed = False
    if TARGET_CHANNEL_URL:
        try:
            chat_member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
            if chat_member.status in ('member', 'administrator', 'creator'):
                channel_subscribed = True
        except Exception:
            pass

    if not channel_subscribed:
        msg_text = "❌ Для доступа к функциям подпишитесь на наш канал: {}".format(TARGET_CHANNEL_URL)
        return False, msg_text

    if db_check_subscription(user_id): 
        return True, ""
    
    return False, "❌ Ваша подписка истекла. Активируйте промокод."

def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

def get_main_kb(user_id):
    user = db_get_user(user_id)
    active = user.get('telethon_active')
    running = user_id in ACTIVE_TELETHON_WORKERS
    has_progress = user_id in PROCESS_PROGRESS 
    
    kb = []
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    kb.append([InlineKeyboardButton(text="❓ Помощь / Команды", callback_data="show_help")]) 
    
    if not active:
        # Вход
        kb.append([InlineKeyboardButton(text="📲 Вход по QR-коду (Рекоменд.)", callback_data="telethon_auth_qr_start")])
        kb.append([InlineKeyboardButton(text="🔐 Вход по Номеру/Коду (Старый)", callback_data="telethon_auth_phone_start")])
    else:
        # Управление Worker'ом
        if has_progress:
             kb.append([InlineKeyboardButton(text="⚡️ Активный Прогресс", callback_data="show_progress")])
             
        kb.append([InlineKeyboardButton(text="🚀 Запустить / Остановить Worker", callback_data="telethon_stop_session" if running else "telethon_start_session")])
        kb.append([InlineKeyboardButton(text="ℹ️ Статус Сессии", callback_data="telethon_check_status")])
    
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_no_access_kb(is_channel_reason):
    kb = []
    if is_channel_reason:
        kb.append([InlineKeyboardButton(text="✅ Подписаться на канал", url="https://t.me/{}".format(TARGET_CHANNEL_URL.lstrip('@')))])
    
    # Кнопка промокода всегда тут
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    
    if not is_channel_reason:
         kb.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")])
         
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать Промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="👤 Выдать Подписку", callback_data="admin_grant_sub")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_report_choice_kb():
    """Кнопки выбора формата отчета для Aiogram-бота"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Отправить файлом (.txt)", callback_data="send_report_file")],
        [InlineKeyboardButton(text="💬 Отправить сообщениями (по частям)", callback_data="send_report_messages")],
        [InlineKeyboardButton(text="❌ Удалить отчет", callback_data="delete_report")]
    ])
    
# =========================================================================
# V. TELETHON WORKER (ОСНОВНОЕ ЯДРО)
# =========================================================================

async def send_long_message_aiogram(user_id, text, parse_mode='HTML', max_len=4000):
    """Делит длинное сообщение на части и отправляет их через Aiogram-бота."""
    
    if len(text) <= max_len:
        try:
            return await bot.send_message(user_id, text, parse_mode=parse_mode)
        except Exception as e:
            logger.error(f"Failed to send short message to {user_id}: {e}")
            return 
    
    parts = []
    current_part = ""
    lines = text.splitlines(True) 
    
    for line in lines:
        if len(current_part) + len(line) > max_len:
            parts.append(current_part.strip())
            current_part = line
        else:
            current_part += line
    
    if current_part.strip():
        parts.append(current_part.strip())
        
    for i, part in enumerate(parts):
        header = "📊 **Часть {}/{}**\n".format(i+1, len(parts))
        
        message_to_send = header + part
        
        if len(message_to_send) > max_len:
            message_to_send = part
        
        try:
            await bot.send_message(user_id, message_to_send, parse_mode=parse_mode)
            await asyncio.sleep(0.5)
        except Exception as e:
             logger.error(f"Failed to send part {i+1} to {user_id}: {e}")

async def stop_worker(user_id):
    """Останавливает Worker и очищает задачи."""
    
    # Очистка флуд-задач
    if user_id in FLOOD_TASKS:
        for task in FLOOD_TASKS[user_id].values():
            if task and not task.done():
                task.cancel()
        del FLOOD_TASKS[user_id]

    # Остановка основного таска Worker'а
    if user_id in ACTIVE_TELETHON_WORKERS:
        try:
            ACTIVE_TELETHON_WORKERS[user_id].cancel()
        except Exception as e:
             logger.error(f"Error canceling worker task {user_id}: {e}")
        del ACTIVE_TELETHON_WORKERS[user_id]
    
    # Отключение клиента
    if user_id in ACTIVE_TELETHON_CLIENTS:
        try:
            await ACTIVE_TELETHON_CLIENTS[user_id].disconnect()
        except: pass
        del ACTIVE_TELETHON_CLIENTS[user_id]
        
    db_set_session_status(user_id, False)
    if user_id in PROCESS_PROGRESS:
        del PROCESS_PROGRESS[user_id]
    logger.info("Worker {} stopped.".format(user_id))

async def start_workers():
    """Запускает worker'ы для всех пользователей, у которых активна сессия в БД."""
    users = db_get_active_telethon_users()
    for uid in users:
        asyncio.create_task(run_worker(uid))

async def check_group_task(client, event, target_chat_str, min_id, max_id, aiogram_chat_id):
    
    user_id = event.sender_id
    chat_id = event.chat_id
    if chat_id is None and not target_chat_str:
         await client.send_message(user_id, "❌ `.чекгруппу` должен быть вызван из группы/канала или с указанием его юзернейма/ID.")
         return
         
    try:
        # Определяем сущность чата
        try:
            chat_entity = await client.get_entity(target_chat_str)
        except Exception:
            chat_entity = await client.get_entity(chat_id)
            
        chat_name = get_display_name(chat_entity)

        unique_users = {} 
        limit = 1000000 
        
        # Уведомление через Aiogram-бот
        await bot.send_message(aiogram_chat_id, "⏳ Worker начинает сканирование **всех** сообщений в чате `{}` для сбора пользователей. Это может занять время. Отчет придет сюда, в чат с ботом {}.".format(chat_name, BOT_USERNAME), parse_mode='Markdown')
        
        PROCESS_PROGRESS[user_id] = {'type': 'checkgroup', 'peer_name': chat_name, 'done_msg': 0, 'aiogram_chat_id': aiogram_chat_id}
        
        async for message in client.iter_messages(chat_entity, limit=limit):
            if user_id in PROCESS_PROGRESS and PROCESS_PROGRESS[user_id].get('type') != 'checkgroup':
                return # Процесс был отменен
                
            PROCESS_PROGRESS[user_id]['done_msg'] += 1
            
            if message.sender and isinstance(message.sender, User) and message.sender_id not in unique_users:
                user_id_int = message.sender.id
                
                if (min_id is None or user_id_int >= min_id) and \
                   (max_id is None or user_id_int <= max_id):
                    
                    unique_users[user_id_int] = message.sender
            
            # Обновление прогресса каждые 1000 сообщений
            if PROCESS_PROGRESS[user_id]['done_msg'] % 1000 == 0:
                 await bot.send_message(aiogram_chat_id, "ℹ️ Просканировано: **{}** сообщений...".format(PROCESS_PROGRESS[user_id]['done_msg']), parse_mode='Markdown')

        # --- Формирование финального отчета ---
        total_found = len(unique_users)
        if total_found > 0:
            # ... (логика формирования отчета) ...
            report_data_raw = []
            range_info = " (Фильтр ID: {}-{})".format(min_id or 'Все', max_id or 'Все') if min_id is not None or max_id is not None else ""
            
            for uid, p in unique_users.items():
                full_name = ' '.join(filter(None, [p.first_name, p.last_name]))
                report_data_raw.append(
                     "👤 Имя: {}\n🔗 Юзернейм: @{}🆔 ID: {}".format(
                         full_name if full_name else 'Нет имени', 
                         p.username if p.username else 'Нет', 
                         uid
                     )
                )
                
            header_text = (
                "📊 Отчет .ЧЕКГРУППУ (по истории сообщений) {}\n"
                "Чат: {}\n"
                " • Просканировано сообщений: {}\n"
                " • Найдено уникальных пользователей: {}\n"
                "\nСписок пользователей (Имя, Юзернейм, ID):".format(
                    range_info,
                    chat_name,
                    PROCESS_PROGRESS[user_id]['done_msg'],
                    total_found
                )
            )
            
            full_report_text = header_text + "\n" + "\n".join(report_data_raw)
            
            # Сохраняем отчет во временное хранилище Aiogram-бота
            PROCESS_PROGRESS[user_id]['report_data'] = full_report_text
            PROCESS_PROGRESS[user_id]['peer_name'] = chat_name

            # Уведомление с кнопками выбора ЧЕРЕЗ AIOGRAM
            await bot.send_message(
                aiogram_chat_id, 
                "✅ **Сбор данных завершен!** Найдено **{}** пользователей.\nВыберите, как получить отчет по чату `{}`:".format(
                    total_found, chat_name
                ),
                reply_markup=get_report_choice_kb(),
                parse_mode='HTML'
            )
        else:
            response = "✅ **Отчет .ЧЕКГРУППУ:**\nПо указанным критериям (чат/диапазон ID) пользователи не найдены в истории сообщений."
            await bot.send_message(aiogram_chat_id, response, parse_mode='HTML')
        
    except RpcCallFailError:
         await bot.send_message(aiogram_chat_id, "❌ Ошибка RPC при .чекгруппу (чат недоступен, возможно, вы не являетесь участником).")
    except Exception as e:
        logger.error(f"Critical error in check_group_task for {user_id}: {e}")
        await bot.send_message(aiogram_chat_id, "❌ Критическая ошибка при .чекгруппу: {} - {}".format(type(e).__name__, e))
        
    finally:
        if user_id in PROCESS_PROGRESS and 'report_data' not in PROCESS_PROGRESS[user_id]:
            del PROCESS_PROGRESS[user_id]
        try:
            # Обновляем главное меню бота, если процесс был безрезультатным или с ошибкой
            await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
        except:
            pass

async def run_worker(user_id):
    # Агрессивная очистка перед запуском
    await stop_worker(user_id) 
    path = get_session_path(user_id)
    
    # --- Инициализация с Прокси ---
    client = TelegramClient(path, API_ID, API_HASH, proxy=PROXY_CONFIG)
    ACTIVE_TELETHON_CLIENTS[user_id] = client
    
    try:
        # --- ПРОВЕРКА СЕССИИ ---
        if not os.path.exists(path + '.session'):
            db_set_session_status(user_id, False)
            await bot.send_message(user_id, "⚠️ Файл сессии не найден. Требуется повторная авторизация.", reply_markup=get_main_kb(user_id))
            return

        await client.start()
        db_set_session_status(user_id, True)
        logger.info(f"Worker {user_id} started successfully.")

        # --- ЛОГИКА ФЛУДА И КОМАНД ---
        async def flood_task(peer, message, count, delay, chat_id):
            try:
                is_unlimited = count <= 0
                max_iterations = count if not is_unlimited else 999999999 
                
                peer_name = get_display_name(await client.get_entity(peer)) 
                PROCESS_PROGRESS[user_id] = {'type': 'flood', 'total': count, 'done': 0, 'peer': peer_name, 'chat_id': chat_id}
                
                for i in range(max_iterations):
                    if user_id in FLOOD_TASKS and chat_id not in FLOOD_TASKS[user_id]:
                        await client.send_message(user_id, "🛑 Флуд в чате `{}` остановлен по команде .стопфлуд.".format(peer_name))
                        break
                    
                    if not is_unlimited and i >= count: 
                        break 
                        
                    await client.send_message(peer, message)
                    PROCESS_PROGRESS[user_id]['done'] = i + 1
                    await asyncio.sleep(delay)
                    
                await client.send_message(user_id, "✅ Флуд завершен." if not is_unlimited else "✅ Бесконечный флуд остановлен вручную.")
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Flood task error for {user_id}: {e}")
                await client.send_message(user_id, "❌ Ошибка при флуде: {}".format(e))
            finally:
                if user_id in FLOOD_TASKS and chat_id in FLOOD_TASKS[user_id]:
                    del FLOOD_TASKS[user_id][chat_id]
                    if not FLOOD_TASKS[user_id]:
                        del FLOOD_TASKS[user_id]
                if user_id in PROCESS_PROGRESS and PROCESS_PROGRESS[user_id].get('chat_id') == chat_id:
                    del PROCESS_PROGRESS[user_id]
                try:
                    await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
                except:
                    pass

        # Обработчик сообщений Telethon
        @client.on(events.NewMessage)
        async def handler(event):
            if not db_check_subscription(user_id) and user_id != ADMIN_ID: return
            if not event.out: return # Только исходящие сообщения
            
            msg = event.text.strip()
            parts = msg.split()
            if not parts: return
            cmd = parts[0].lower()
            current_chat_id = event.chat_id

            # .ЛС
            if cmd == '.лс':
                try:
                    full_text = event.text
                    lines = full_text.split('\n')
                    
                    if len(lines) < 2:
                        return await event.reply("❌ Неверный формат .лс. Используйте:\n`.лс [сообщение]`\n`[@адресат1]`\n`[ID2]`\n\n**Адресаты должны быть с новой строки!**")

                    recipients = [line.strip() for line in lines[1:] if line.strip()]
                    
                    message_line = lines[0].strip()
                    text = message_line[len(cmd):].strip() 
                    
                    if not text or not recipients:
                        return await event.reply("❌ Неверный формат .лс. Убедитесь, что указаны и текст, и хотя бы один адресат с новой строки.")
                    
                    results = []
                    for target in recipients:
                        try:
                            if not (target.startswith('@') or target.isdigit() or re.match(r'^-?\d+$', target)):
                                results.append("❌ {}: Пропущен (Не похож на @юзернейм или ID)".format(target))
                                continue
                                
                            await client.send_message(target, text) 
                            results.append("✅ {}: Отправлено".format(target))
                        except ValueError: 
                            results.append("❌ {}: Ошибка (Некорректный ID/Юзернейм)".format(target))
                        except Exception as e:
                            results.append("❌ {}: Ошибка ({})".format(target, type(e).__name__))
                            
                    await event.reply("<b>Результаты .лс:</b>\n" + "\n".join(results), parse_mode='HTML')
                    
                except Exception as e:
                     await event.reply("❌ Критическая ошибка .лс: {}. Проверьте формат.".format(type(e).__name__))
            
            # .ТХТ (или .ТАБЛИЦА)
            elif cmd in ('.тхт', '.таблица'):
                if not event.is_reply:
                    return await event.reply("❌ Используйте `.тхт` или `.таблица` **ответом** на сообщение с текстовым файлом (.txt, .log, .csv).")

                reply_msg = await event.get_reply_message()
                
                if not reply_msg or not reply_msg.document:
                    return await event.reply("❌ В сообщении, на которое вы отвечаете, нет документа.")
                
                mime_type = reply_msg.document.mime_type
                filename = reply_msg.document.attributes[0].file_name if reply_msg.document.attributes else ""
                
                if not mime_type or not ('text' in mime_type or filename.endswith(('.txt', '.log', '.csv', '.ini', '.cfg'))):
                     return await event.reply("❌ Ожидается текстовый файл. Обнаружено: `{}`.".format(mime_type))
                
                
                try:
                    await event.reply("⏳ Начинаю скачивание и обработку файла...")
                    
                    with tempfile.TemporaryDirectory() as tmpdir:
                        safe_filename = re.sub(r'[^\w\-_\.]', '_', filename or 'temp_file')
                        downloaded_file_path = await client.download_media(reply_msg, file=os.path.join(tmpdir, safe_filename))
                        
                        with open(downloaded_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            file_content = f.read()
                        
                        formatted_content = "📖 **Содержимое файла** (`{}`):\n".format(filename)
                        
                        content_to_send = file_content.strip()[:4000] 
                        
                        formatted_content += "<pre>" + content_to_send + "</pre>"
                        
                        if len(file_content.strip()) > 4000:
                            formatted_content += "\n⚠️ Отображены только первые 4000 символов."
                            
                        await client.send_message(event.chat_id, formatted_content, parse_mode='HTML')
                        
                except Exception as e:
                    await event.reply("❌ Ошибка при обработке файла: {} - {}".format(type(e).__name__, e))
                finally:
                    pass
            
            # .ФЛУД
            elif cmd == '.флуд' and len(parts) >= 4:
                if user_id in FLOOD_TASKS and current_chat_id in FLOOD_TASKS[user_id]:
                    return await event.reply("⚠️ Флуд **уже запущен** в этом чате. Используйте `.стопфлуд` здесь.")
                
                try:
                    count = int(parts[1])
                    delay = float(parts[-1])
                    
                    target_chat_str = None
                    message_parts = parts[2:-1] 
                    
                    if message_parts and (message_parts[-1].startswith('@') or re.match(r'^-?\d+$', message_parts[-1])):
                        target_chat_str = message_parts.pop() 
                    
                    message = ' '.join(message_parts)

                    if target_chat_str is None:
                        if current_chat_id is None:
                            return await event.reply("❌ Укажите чат или запустите команду в группе/канале.")
                        peer = await client.get_input_entity(current_chat_id)
                        flood_chat_id = current_chat_id
                    else:
                        peer = await client.get_input_entity(target_chat_str)
                        flood_chat_id = (await client.get_entity(target_chat_str)).id 

                    if delay < 0.5:
                        return await event.reply("❌ Макс. кол-во: **безлимитно** (или 0). Мин. задержка: 0.5 сек.")
                    
                    if not message:
                         return await event.reply("❌ Сообщение для флуда не может быть пустым.")
                         
                    
                    task = asyncio.create_task(flood_task(peer, message, count, delay, flood_chat_id))
                    if user_id not in FLOOD_TASKS:
                        FLOOD_TASKS[user_id] = {}
                        
                    FLOOD_TASKS[user_id][flood_chat_id] = task
                    
                    await event.reply(
                        "🔥 **Флуд запущен!**\nЧат: `{}`\nСообщений: {}\nЗадержка: {} сек.".format(
                            get_display_name(await client.get_entity(peer)), 
                            'Безлимитно' if count <= 0 else count,
                            delay
                        ), 
                        parse_mode='HTML'
                    )
                    
                    try:
                        await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
                    except:
                        pass
                    
                except ValueError:
                    await event.reply("❌ Неверный формат чисел (кол-во/задержка).")
                except (UsernameInvalidError, PeerIdInvalidError, Exception) as e:
                    await event.reply("❌ Ошибка при подготовке флуда: Чат не найден или неверный формат. ({})".format(type(e).__name__))
            
            # .СТОПФЛУД 
            elif cmd == '.стопфлуд':
                if user_id in FLOOD_TASKS and current_chat_id in FLOOD_TASKS[user_id]:
                    task_to_cancel = FLOOD_TASKS[user_id].pop(current_chat_id)
                    if not FLOOD_TASKS[user_id]:
                        del FLOOD_TASKS[user_id]
                        
                    if task_to_cancel and not task_to_cancel.done():
                        task_to_cancel.cancel()
                        await event.reply("🛑 Флуд **в этом чате** остановлен.")
                        try:
                            await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
                        except:
                            pass
                    else:
                        await event.reply("⚠️ Флуд не был активен в этом чате, или задача уже завершилась.")
                else:
                    await event.reply("⚠️ Флуд **в этом чате** не запущен.")
            
            # .СТАТУС
            elif cmd == '.статус':
                if user_id in PROCESS_PROGRESS:
                    progress = PROCESS_PROGRESS[user_id]
                    p_type = progress['type']
                    
                    if p_type == 'flood':
                        total = progress['total']
                        done = progress['done']
                        peer_name = progress.get('peer', 'Неизвестно')
                        
                        status_text = (
                            "⚡️ **СТАТУС ФЛУДА:**\n"
                            " • Цель: `{}`\n"
                            " • Отправлено: **{}**\n"
                            " • Всего: **{}**\n"
                            " • Прогресс: **{}**".format(
                                peer_name,
                                done,
                                'Безлимитно' if total <= 0 else total,
                                '{:.2f}%'.format(done/total*100) if total > 0 else '—'
                            )
                        )
                    elif p_type == 'checkgroup':
                        peer_name = progress.get('peer_name', 'Неизвестно')
                        done_msg = progress['done_msg']
                        status_text = (
                            "🔎 **СТАТУС АНАЛИЗА ЧАТА:**\n"
                            " • Цель: `{}`\n"
                            " • Просканировано сообщений: **{}**\n"
                            " • Статус: **{}**".format(
                                peer_name,
                                done_msg,
                                'Сбор данных...' if 'report_data' not in progress else 'Сбор завершен! Ожидание выбора формата в Aiogram-боте.'
                            )
                        )
                    else:
                        status_text = "⚙️ Активный процесс: {}. Данные: {}".format(p_type, progress)
                else:
                    status_text = "✨ Активных процессов Worker'а нет."
                
                await client.send_message(current_chat_id, status_text, parse_mode='HTML')


            # .ЧЕКГРУППУ 
            elif cmd == '.чекгруппу':
                if user_id in PROCESS_PROGRESS and PROCESS_PROGRESS[user_id]['type'] == 'checkgroup':
                    return await event.reply("⚠️ Процесс сканирования чата уже запущен. Дождитесь его завершения.")
                    
                target_chat_str = None
                id_range_str = None

                if len(parts) == 2:
                    arg = parts[1]
                    if re.match(r'^\d+-\d+$', arg):
                        id_range_str = arg
                    else:
                        target_chat_str = arg
                elif len(parts) >= 3:
                    target_chat_str = parts[1]
                    id_range_str = parts[2]
                
                if not target_chat_str and current_chat_id:
                    target_chat_str = current_chat_id
                elif not target_chat_str:
                    return await event.reply("❌ Не удалось определить чат. Используйте: `.чекгруппу [@чат/ID] [мин_ID-макс_ID]` в ЛС.")

                min_id, max_id = None, None
                if id_range_str:
                    try:
                        min_id, max_id = map(int, id_range_str.split('-'))
                        if min_id >= max_id:
                             return await event.reply("❌ Неверный диапазон ID: минимальный ID должен быть меньше максимального.")
                    except ValueError:
                         return await event.reply("❌ Неверный формат диапазона ID. Используйте: `MIN_ID-MAX_ID`.")
                
                # Запускаем задачу
                aiogram_chat_id = user_id
                asyncio.create_task(check_group_task(client, event, target_chat_str, min_id, max_id, aiogram_chat_id))
                
                # Подтверждение от Worker'а в чат, где была вызвана команда
                await event.reply("⏳ **Начинаю анализ группы...** Ожидайте уведомления в чате с ботом `{}`.".format(BOT_USERNAME), parse_mode='HTML')
                
        # --- Запуск Worker'а ---
        worker_task = asyncio.create_task(client.run_until_disconnected())
        ACTIVE_TELETHON_WORKERS[user_id] = worker_task
        await worker_task
        
    except (AuthKeyUnregisteredError, UserDeactivatedError):
        # Ошибка невалидной сессии
        db_set_session_status(user_id, False)
        await bot.send_message(user_id, "⚠️ Сессия недействительна. Пожалуйста, авторизуйтесь заново.")
    except Exception as e:
        logger.error(f"Worker {user_id} critical error: {e}")
        error_msg = f"❌ Критическая ошибка воркера: {type(e).__name__}."
        if "ConnectionError" in str(e):
             error_msg += " **Проверьте настройки Прокси/Хостинга!**"
        await bot.send_message(user_id, error_msg)
    finally:
        # Убедимся, что все очищено
        await stop_worker(user_id) 


# =========================================================================
# VI. ХЕНДЛЕРЫ BOT (AIOGRAM)
# =========================================================================

@dp.callback_query(F.data == "cancel_action")
async def cancel_handler(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    # Агрессивная очистка временного клиента
    if uid in TEMP_AUTH_CLIENTS:
        try: 
            client = TEMP_AUTH_CLIENTS[uid]
            await client.disconnect()
        except: 
            pass
        del TEMP_AUTH_CLIENTS[uid]
        
    await state.clear()
    
    try:
        # Пытаемся отредактировать сообщение с отменой
        await call.message.edit_text("ℹ️ Действие отменено.", reply_markup=None)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
             # Если сообщение нельзя отредактировать (например, оно слишком старое), отправляем новое
            await call.message.answer("ℹ️ Действие отменено.", reply_markup=None)
        
    # Возврат в главное меню
    await cmd_start(call, state)


@dp.callback_query(F.data == "back_to_main")
@dp.message(Command("start"))
async def cmd_start(u: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = u.from_user.id
    db_get_user(user_id)
    
    # Сброс состояния, чтобы избежать зависаний (особенно перед отображением меню)
    await state.clear()
    
    has_access, msg = await check_access(user_id, bot)
    
    sub = db_get_user(user_id).get('subscription_end_date')
    text = "👋 <b>Привет!</b> Ваш ID: <code>{}</code>\nПодписка до: <code>{}</code>\n\n".format(user_id, sub if sub else 'Нет')
    
    if not has_access:
        text += "⚠️ <b>Доступ ограничен.</b>\n{}".format(msg)
        is_channel_reason = f"Для доступа к функциям подпишитесь на наш канал" in msg
        kb = get_no_access_kb(is_channel_reason)
    else:
        text += "✅ <b>Меню доступно.</b>\nИспользуйте кнопки ниже."
        kb = get_main_kb(user_id)

    if isinstance(u, types.Message): 
        await u.answer(text, reply_markup=kb)
    else: 
        try:
             await u.message.edit_text(text, reply_markup=kb)
        except TelegramBadRequest as e:
             if "message is not modified" not in str(e):
                 # Если сообщение слишком старое для редактирования
                 await u.answer("ℹ️ Возврат в меню.", show_alert=True)
                 await u.message.reply(text, reply_markup=kb)
             else:
                 await u.answer("ℹ️ Вы уже в главном меню.", show_alert=True)


# --- ХЕНДЛЕР АКТИВАЦИИ ПРОМОКОДА ---
@dp.callback_query(F.data == "start_promo_fsm")
async def start_promo_fsm(call: types.CallbackQuery, state: FSMContext):
    # Убеждаемся, что мы входим в состояние
    await state.set_state(PromoStates.waiting_for_code)
    try:
         await call.message.edit_text("🔑 Введите промокод:", reply_markup=get_cancel_kb())
    except TelegramBadRequest:
        # Если не удалось отредактировать, отправляем новое сообщение
        await call.message.answer("🔑 Введите промокод:", reply_markup=get_cancel_kb())

@dp.message(PromoStates.waiting_for_code)
async def process_promo_code(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    user_id = message.from_user.id
    
    # Сброс состояния сразу после получения данных
    await state.clear() 

    promo = db_get_promo(code)
    
    if not promo or promo.get('is_active') == 0:
        await message.answer("❌ Промокод не найден или недействителен.")
    elif promo.get('max_uses') > 0 and promo.get('current_uses', 0) >= promo.get('max_uses'):
        await message.answer("❌ Срок действия промокода (по количеству использований) истек.")
    else:
        days = promo['days']
        new_end_date = db_update_subscription(user_id, days)
        db_use_promo(code)
        
        status_text = (
            "🎉 **Промокод активирован!**\n"
            " • Дней добавлено: **{}**\n"
            " • Подписка до: <code>{}</code>".format(days, new_end_date)
        )
        await message.answer(status_text)
    
    # Возврат в главное меню
    await cmd_start(message, state)


# --- АВТОРИЗАЦИЯ TELETHON (С ИНТЕГРАЦИЕЙ ПРОКСИ И ПРИНУДИТЕЛЬНЫМ СТАРТОМ) ---

@dp.callback_query(F.data == "telethon_auth_phone_start")
async def telethon_auth_phone_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("📱 Введите номер телефона (в международном формате, например, `+79001234567`):", reply_markup=get_cancel_kb())

@dp.callback_query(F.data == "telethon_auth_qr_start")
async def telethon_auth_qr_start(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    await state.set_state(TelethonAuth.WAITING_FOR_QR_LOGIN)
    
    # Отправляем новое сообщение, если не можем редактировать
    try:
         msg = await call.message.edit_text("⏳ Генерирую QR-код и пытаюсь подключиться к серверам Telegram...", reply_markup=get_cancel_kb())
    except TelegramBadRequest:
         msg = await call.message.answer("⏳ Генерирую QR-код и пытаюсь подключиться к серверам Telegram...", reply_markup=get_cancel_kb())

    
    try:
        path = get_session_path(user_id)
        # --- Инициализация с Прокси ---
        client = TelegramClient(path, API_ID, API_HASH, proxy=PROXY_CONFIG)
        TEMP_AUTH_CLIENTS[user_id] = client
        
        # ПРИНУДИТЕЛЬНЫЙ СТАРТ
        await client.start() 
        
        login_token = await client.qr_login()
        qr_url = login_token.url

        text = (
            "🔗 **QR-КОД ДЛЯ ВХОДА**\n"
            "1. Откройте **основной аккаунт** Telegram.\n"
            "2. Перейдите в **Настройки > Устройства > Подключить устройство**.\n"
            "3. Отсканируйте код ниже."
        )
        await msg.edit_text(text, parse_mode='Markdown')
        # Отправляем фото с QR-кодом
        await bot.send_photo(user_id, qr_url, caption="Отсканируйте код в течение 3 минут.", reply_markup=get_cancel_kb()) 

        await login_token.wait(timeout=180) 
        
        db_set_session_status(user_id, True)
        await state.clear()
        
        asyncio.create_task(run_worker(user_id))
        
        await bot.send_message(user_id, "✅ **Авторизация прошла успешно!** Worker запущен.", reply_markup=get_main_kb(user_id))
        
    except asyncio.TimeoutError:
        await bot.send_message(user_id, "❌ Время ожидания QR-кода истекло (3 минуты).")
    except Exception as e:
        logger.error(f"QR Login Error for {user_id}: {e}")
        error_msg = f"❌ Ошибка авторизации: {type(e).__name__}."
        if "ConnectionError" in str(e) or "while disconnected" in str(e) or "EOF" in str(e):
             error_msg = "❌ Ошибка авторизации: **Проблема сетевого соединения (ConnectionError/EOF)**. Проверьте фаервол или смените хостинг/прокси."
        await bot.send_message(user_id, error_msg)
    finally:
        if user_id in TEMP_AUTH_CLIENTS:
            try: await TEMP_AUTH_CLIENTS[user_id].disconnect()
            except: pass
            del TEMP_AUTH_CLIENTS[user_id]
        await state.clear()
        
        try: await msg.edit_text("ℹ️ Возврат в меню.", reply_markup=get_main_kb(user_id))
        except: 
            await bot.send_message(user_id, "ℹ️ Возврат в меню.", reply_markup=get_main_kb(user_id))


@dp.message(TelethonAuth.PHONE)
async def auth_msg_phone(message: types.Message, state: FSMContext):
    phone_number = message.text.strip()
    user_id = message.from_user.id
    
    path = get_session_path(user_id)
    # --- Инициализация с Прокси ---
    client = TelegramClient(path, API_ID, API_HASH, proxy=PROXY_CONFIG)
    TEMP_AUTH_CLIENTS[user_id] = client
    
    try:
        # ПРИНУДИТЕЛЬНЫЙ СТАРТ
        await client.start()
        
        sent_code_hash = await client.send_code_request(phone_number)
        await state.update_data(phone_number=phone_number, sent_code_hash=sent_code_hash)
        await state.set_state(TelethonAuth.CODE)
        await message.answer("🔑 Код отправлен на ваш номер. Введите его (например, 12345):", reply_markup=get_cancel_kb())
    except PhoneNumberInvalidError:
        await message.answer("❌ Неверный формат номера телефона. Попробуйте снова.", reply_markup=get_cancel_kb())
    except Exception as e:
        logger.error(f"Send Code Error for {user_id}: {e}")
        error_msg = f"❌ Произошла ошибка при запросе кода: {type(e).__name__}."
        if "ConnectionError" in str(e) or "while disconnected" in str(e) or "EOF" in str(e):
             error_msg = "❌ Ошибка при запросе кода: **Проблема сетевого соединения (ConnectionError/EOF)**. Проверьте фаервол или смените хостинг/прокси."
        await message.answer(error_msg, reply_markup=get_cancel_kb())

@dp.message(TelethonAuth.CODE)
async def auth_msg_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    data = await state.get_data()
    client = TEMP_AUTH_CLIENTS.get(user_id)
    
    if not client:
        await state.clear()
        await message.answer("❌ Сессия авторизации утеряна. Начните заново.", reply_markup=get_main_kb(user_id))
        return

    try:
        await client.sign_in(
            phone=data['phone_number'],
            code=code,
            phone_code_hash=data['sent_code_hash']
        )
        
        db_set_session_status(user_id, True)
        await state.clear()
        
        asyncio.create_task(run_worker(user_id))
        
        await message.answer("✅ **Авторизация прошла успешно!** Worker запущен.", reply_markup=get_main_kb(user_id))

    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer("🔒 Требуется облачный пароль (2FA). Введите его:", reply_markup=get_cancel_kb())
    except (PhoneCodeExpiredError, PhoneCodeInvalidError) as e:
        await message.answer(f"❌ {type(e).__name__}. Начните авторизацию заново.", reply_markup=get_cancel_kb())
        
    except Exception as e:
        logger.error(f"Sign In Error for {user_id}: {e}")
        await message.answer(f"❌ Ошибка входа: {type(e).__name__}. Начните заново.", reply_markup=get_cancel_kb())
        

@dp.message(TelethonAuth.PASSWORD)
async def auth_msg_password(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    client = TEMP_AUTH_CLIENTS.get(user_id)

    if not client:
        await state.clear()
        await message.answer("❌ Сессия авторизации утеряна. Начните заново.", reply_markup=get_main_kb(user_id))
        return
        
    try:
        await client.sign_in(password=password)
        
        db_set_session_status(user_id, True)
        await state.clear()
        
        asyncio.create_task(run_worker(user_id))
        
        await message.answer("✅ **Авторизация прошла успешно!** Worker запущен.", reply_markup=get_main_kb(user_id))
        
    except PasswordHashInvalidError:
        await message.answer("❌ Неверный облачный пароль. Попробуйте еще раз.", reply_markup=get_cancel_kb())
    except Exception as e:
        logger.error(f"Password Error for {user_id}: {e}")
        await message.answer(f"❌ Ошибка пароля: {type(e).__name__}.", reply_markup=get_cancel_kb())
    finally:
        if user_id in TEMP_AUTH_CLIENTS:
            try: await TEMP_AUTH_CLIENTS[user_id].disconnect()
            except: pass
            del TEMP_AUTH_CLIENTS[user_id]


# --- УПРАВЛЕНИЕ WORKER'ОМ ---
@dp.callback_query(F.data == "telethon_start_session")
async def telethon_start_session(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    if user_id in ACTIVE_TELETHON_WORKERS:
        await call.answer("Worker уже запущен.", show_alert=True)
        return
    
    if not db_get_user(user_id).get('telethon_active'):
        await call.answer("❌ Сначала авторизуйтесь (Вход по QR/Номеру).", show_alert=True)
        return

    await call.answer("🚀 Запускаю Worker...")
    asyncio.create_task(run_worker(user_id))
    try:
         await call.message.edit_reply_markup(reply_markup=get_main_kb(user_id))
    except:
         pass


@dp.callback_query(F.data == "telethon_stop_session")
async def telethon_stop_session(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    if user_id not in ACTIVE_TELETHON_WORKERS:
        await call.answer("Worker не запущен.", show_alert=True)
        return
    
    await call.answer("🛑 Останавливаю Worker...")
    await stop_worker(user_id)
    try:
         await call.message.edit_reply_markup(reply_markup=get_main_kb(user_id))
    except:
         pass

@dp.callback_query(F.data == "telethon_check_status")
async def telethon_check_status(call: types.CallbackQuery):
    user_id = call.from_user.id
    user = db_get_user(user_id)
    
    is_session_active = user.get('telethon_active')
    is_worker_running = user_id in ACTIVE_TELETHON_WORKERS

    if is_session_active:
        status_text = (
            "✅ **СТАТУС СЕССИИ:**\n"
            " • Сессия: **Активна**\n"
            " • Worker: **{}**".format("Запущен (работает)" if is_worker_running else "Остановлен (готов к запуску)")
        )
    else:
        status_text = "❌ **СТАТУС СЕССИИ:**\nТребуется авторизация."
        
    await call.answer(status_text, show_alert=True)

@dp.callback_query(F.data == "show_progress")
async def show_progress(call: types.CallbackQuery):
    user_id = call.from_user.id
    
    if user_id not in PROCESS_PROGRESS:
        if user_id in FLOOD_TASKS:
            await call.answer("ℹ️ Активный процесс флуда, но прогресс не отображается. Используйте `.статус` в чате Worker'а.", show_alert=True)
            return

        await call.answer("✨ Активных процессов нет.", show_alert=True)
        return
        
    progress = PROCESS_PROGRESS[user_id]
    p_type = progress['type']
    
    status_text = "❌ Прогресс не может быть отображен."
    
    if p_type == 'flood':
        total = progress.get('total', 0)
        done = progress.get('done', 0)
        peer_name = progress.get('peer', 'Неизвестно')
        
        status_text = (
            "⚡️ **АКТИВНЫЙ ПРОЦЕСС:**\n"
            " • Тип: Флуд\n"
            " • Цель: `{}`\n"
            " • Отправлено: **{}**\n"
            " • Всего: **{}**\n"
            " • Прогресс: **{}**".format(
                peer_name,
                done,
                'Безлимитно' if total <= 0 else total,
                '{:.2f}%'.format(done/total*100) if total > 0 else '—'
            )
        )
    elif p_type == 'checkgroup':
        peer_name = progress.get('peer_name', 'Неизвестно')
        done_msg = progress.get('done_msg', 0)
        
        status = 'Сбор данных...'
        if 'report_data' in progress:
            status = 'Сбор завершен! (Ожидание выбора формата)'
        
        status_text = (
            "🔎 **АКТИВНЫЙ ПРОЦЕСС:**\n"
            " • Тип: Анализ Чата\n"
            " • Цель: `{}`\n"
            " • Просканировано сообщений: **{}**\n"
            " • Статус: **{}**".format(
                peer_name,
                done_msg,
                status
            )
        )
    
    await call.answer(status_text, show_alert=True)

@dp.callback_query(F.data.startswith('send_report_') | F.data == 'delete_report')
async def handle_report_choice(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    data = call.data
    
    if user_id not in PROCESS_PROGRESS or 'report_data' not in PROCESS_PROGRESS[user_id]:
        await call.answer("⚠️ Отчет устарел или был удален.", show_alert=True)
        try: await call.message.edit_text("ℹ️ Отчет по этой задаче более недоступен.", reply_markup=None)
        except: pass
        await cmd_start(call, state)
        return

    report_data = PROCESS_PROGRESS[user_id]['report_data']
    peer_name = PROCESS_PROGRESS[user_id]['peer_name']
    
    try:
        if data == 'send_report_file':
            await call.answer("⏳ Отправляю файл...")
            
            file_bytes = io.BytesIO(report_data.encode('utf-8'))
            file_bytes.name = "checkgroup_report_{}_{}.txt".format(peer_name.replace(' ', '_').replace('@', ''), datetime.now().strftime('%Y%m%d_%H%M%S'))
            
            await bot.send_document(user_id, BufferedInputFile(file_bytes.read(), filename=file_bytes.name), caption=f"📄 Отчет по чату `{peer_name}`")
            await call.message.edit_text(f"✅ Отчет по чату `{peer_name}` отправлен файлом.", reply_markup=None)
            
        elif data == 'send_report_messages':
            await call.answer("⏳ Отправляю сообщения...")
            
            start_index = report_data.find("Список пользователей (Имя, Юзернейм, ID):")
            if start_index != -1:
                start_of_list = start_index + len("Список пользователей (Имя, Юзернейм, ID):")
                
                report_html = (
                    report_data[:start_of_list] + "\n" +
                    "<pre>" + report_data[start_of_list:].strip() + "</pre>"
                )
            else:
                report_html = report_data 
            
            await send_long_message_aiogram(user_id, report_html, parse_mode='HTML')
            await call.message.edit_text(f"✅ Отчет по чату `{peer_name}` отправлен по частям.", reply_markup=None)

        elif data == 'delete_report':
            await call.answer("❌ Отчет удален.")
            await call.message.edit_text(f"❌ Отчет по чату `{peer_name}` удален.", reply_markup=None)

    except Exception as e:
        logger.error(f"Error sending report via Aiogram: {e}")
        await call.answer("❌ Произошла ошибка при отправке отчета.", show_alert=True)
        await call.message.edit_text("❌ Произошла ошибка: {}".format(type(e).__name__), reply_markup=None)
    
    finally:
        if user_id in PROCESS_PROGRESS and 'report_data' in PROCESS_PROGRESS[user_id]:
            del PROCESS_PROGRESS[user_id]
            
        await cmd_start(call, state)

@dp.callback_query(F.data == "show_help")
async def cmd_help(call: types.CallbackQuery):
    help_text = (
        "📖 **КОМАНДЫ WORKER'А (Telethon)**\n\n"
        "Эти команды нужно отправлять в **чат с Worker-аккаунтом** (ваша вторая учетная запись, которую вы авторизовали):\n\n"
        "1. **`.лс [сообщение]`**\n"
        "   `[@user1]`\n"
        "   `[12345678]`\n"
        "   *Отправляет личное сообщение указанным адресатам (юзернейм или ID) с новой строки.*\n\n"
        "2. **`.флуд [кол-во] [сообщение] [@чат/ID] [задержка]`**\n"
        "   *Пример: `.флуд 100 Привет всем! @my_channel 2.5`*\n"
        "   *Кол-во: 0 - безлимитно. Задержка: мин. 0.5 сек.*\n\n"
        "3. **`.стопфлуд`**\n"
        "   *Останавливает активный флуд в чате, где была вызвана команда.*\n\n"
        "4. **`.чекгруппу [@чат/ID] [мин_ID-макс_ID]`**\n"
        "   *Сканирует историю сообщений чата/канала для сбора уникальных пользователей. Отчет будет отправлен сюда, в чат с ботом `{}`.*\n\n"
        "5. **`.тхт` или `.таблица`**\n"
        "   *Используйте **ответом на прикрепленный файл** (.txt, .csv) в чате Worker'а. Отображает содержимое файла в форматированном виде.*\n\n"
        "6. **`.статус`**\n"
        "   *Показывает текущий прогресс активных задач Worker'а (флуд, анализ чата).*".format(BOT_USERNAME)
    )
    await call.message.edit_text(help_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")]]))
    
# --- АДМИН-ПАНЕЛЬ ---

@dp.callback_query(F.data == "admin_panel_start")
async def admin_panel_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("❌ Нет доступа")
    await state.set_state(AdminStates.main_menu)
    await call.message.edit_text("🛠️ **Админ-Панель**\nВыберите действие:", reply_markup=get_admin_kb())

@dp.callback_query(F.data == "admin_create_promo", AdminStates.main_menu)
async def admin_create_promo_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return 
    code = generate_promo_code()
    await state.update_data(promo_code=code)
    await state.set_state(AdminStates.promo_days_input)
    await call.message.edit_text(f"🎁 Создается промокод: <code>{code}</code>\nВведите количество **дней** подписки (число):", reply_markup=get_cancel_kb())

@dp.message(AdminStates.promo_days_input)
async def admin_promo_days_input(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return 
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError
        await state.update_data(days=days)
        await state.set_state(AdminStates.promo_uses_input)
        await message.answer("Введите количество **использований** (число, 0 - безлимитно):", reply_markup=get_cancel_kb())
    except ValueError:
        await message.answer("❌ Введите корректное положительное число дней.", reply_markup=get_cancel_kb())

@dp.message(AdminStates.promo_uses_input)
async def admin_promo_uses_input(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return 
    try:
        max_uses = int(message.text.strip())
        if max_uses < 0: raise ValueError
        data = await state.get_data()
        
        code = data['promo_code']
        days = data['days']
        
        db_add_promo(code, days, max_uses)
        
        await state.clear()
        
        status_text = (
            "✅ **Промокод создан!**\n"
            " • Код: <code>{}</code>\n"
            " • Дней: {}\n"
            " • Использований: {}".format(code, days, max_uses if max_uses > 0 else 'Безлимитно')
        )
        await message.answer(status_text, reply_markup=get_admin_kb())
        
    except ValueError:
        await message.answer("❌ Введите корректное число использований (0 или больше).", reply_markup=get_cancel_kb())
    
@dp.callback_query(F.data == "admin_grant_sub", AdminStates.main_menu)
async def admin_grant_sub_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(AdminStates.sub_user_id_input)
    await call.message.edit_text("👤 Введите **ID пользователя**, которому нужно выдать подписку:", reply_markup=get_cancel_kb())
    
@dp.message(AdminStates.sub_user_id_input)
async def admin_sub_user_id_input(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        target_id = int(message.text.strip())
        db_get_user(target_id)
        await state.update_data(target_id=target_id)
        await state.set_state(AdminStates.sub_days_input)
        await message.answer(f"✅ ID <code>{target_id}</code> найден.\nВведите количество **дней** подписки:", reply_markup=get_cancel_kb())
    except ValueError:
        await message.answer("❌ Введите корректный числовой ID пользователя.", reply_markup=get_cancel_kb())

@dp.message(AdminStates.sub_days_input)
async def admin_sub_days_input(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        days = int(message.text.strip())
        if days <= 0: raise ValueError
        data = await state.get_data()
        target_id = data['target_id']
        
        new_end_date = db_update_subscription(target_id, days)
        
        await state.clear()
        
        status_text = (
            "✅ **Подписка выдана!**\n"
            " • Пользователь ID: <code>{}</code>\n"
            " • Продлено до: <code>{}</code>".format(target_id, new_end_date)
        )
        await message.answer(status_text, reply_markup=get_admin_kb())
        
        try:
            await bot.send_message(target_id, f"🎉 Ваша подписка продлена до <code>{new_end_date}</code> нашими администраторами.", parse_mode='HTML')
        except (TelegramForbiddenError, TelegramBadRequest):
            pass 
            
    except ValueError:
        await message.answer("❌ Введите корректное положительное число дней.", reply_markup=get_cancel_kb())


# --- ЗАПУСК ---
async def main():
    logger.info("START BOT")
    db_init()
    
    # Агрессивная очистка временных клиентов при старте
    for uid in list(TEMP_AUTH_CLIENTS.keys()):
        if uid in TEMP_AUTH_CLIENTS:
             try: await TEMP_AUTH_CLIENTS[uid].disconnect()
             except: pass
             del TEMP_AUTH_CLIENTS[uid]

    # !!! ВАЖНО !!! Очистка вебхуков для предотвращения TelegramConflictError
    logger.info("Checking for and dropping pending updates/webhooks...")
    try:
        # Убедимся, что бот использует новый токен для этой операции
        current_bot = Bot(token=BOT_TOKEN) 
        await current_bot.delete_webhook(drop_pending_updates=True) 
        await current_bot.session.close() # Закрыть временную сессию
        logger.info("Webhooks and pending updates dropped successfully.")
    except Exception as e:
        logger.warning(f"Failed to drop webhook/updates: {e}")
        
    await start_workers()
    logger.info("Start polling")
    await dp.start_polling(bot)

if __name__ == "__main__":
    # Если запуск из скрипта, который не использует asyncio.run(), возможно, потребуется обернуть в try/except 
    # для корректной обработки KeyboardInterrupt или других сигналов остановки.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by KeyboardInterrupt")
    except Exception as e:
         logger.error(f"Critical error during bot runtime: {e}")
