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
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, FSInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, StateFilter 
from aiogram.client.default import DefaultBotProperties

# Импорты telethon
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat, UserStatusOnline, UserStatusRecently, User
from telethon.errors import (
    UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, 
    PhoneNumberInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError, 
    AuthKeyUnregisteredError, PasswordHashInvalidError, ChannelPrivateError, 
    UsernameInvalidError, PeerIdInvalidError, ChatAdminRequiredError, 
    RpcCallFailError
)
from telethon.utils import get_display_name 
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.functions.messages import GetMessagesViewsRequest
from telethon.tl.types import ChannelParticipantsRecent, InputChannel
from telethon.tl.custom import Button 

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# !!! ВАШ BOT_TOKEN !!!
BOT_TOKEN = "7868097991:AAFWAAw1357IWkGXr9cOpqY11xBtnB0xJSg" 
ADMIN_ID = 6256576302  
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1" 
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
DB_TIMEOUT = 10 

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
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=storage)
user_router = Router()

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
    """Генерирует случайный промокод из заглавных букв и цифр."""
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
        # Используем .format() для надежности
        msg_text = "❌ Для доступа к функциям подпишитесь на наш канал: {}".format(TARGET_CHANNEL_URL)
        return False, msg_text

    if db_check_subscription(user_id): 
        return True, ""
    
    return False, "❌ Ваша подписка истекла. Активируйте промокод."

def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

def get_code_kb(current_code_text=""):
    kb = []
    # Используем .format() для надежности
    kb.append([InlineKeyboardButton(text="Код: {} / Длина: {}".format(current_code_text if current_code_text else '...', len(current_code_text)), callback_data="ignore")])
    
    row1 = [InlineKeyboardButton(text=f"{i}️⃣", callback_data=f"code_input_{i}") for i in range(1, 4)]
    row2 = [InlineKeyboardButton(text=f"{i}️⃣", callback_data=f"code_input_{i}") for i in range(4, 7)]
    row3 = [InlineKeyboardButton(text=f"{i}️⃣", callback_data=f"code_input_{i}") for i in range(7, 10)]
    kb.extend([row1, row2, row3])
    
    row4 = [
        InlineKeyboardButton(text="⬅️ Удалить", callback_data="code_input_delete"),
        InlineKeyboardButton(text="0️⃣", callback_data="code_input_0"),
        InlineKeyboardButton(text="➡️ Отправить", callback_data="code_input_submit")
    ]
    kb.append(row4)
    
    kb.append([InlineKeyboardButton(text="❌ Отмена Авторизации", callback_data="cancel_action")])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)

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
        # Используем .format() для надежности
        kb.append([InlineKeyboardButton(text="✅ Подписаться на канал", url="https://t.me/{}".format(TARGET_CHANNEL_URL.lstrip('@')))])
    
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
    
# =========================================================================
# V. TELETHON WORKER (ОСНОВНОЕ ЯДРО)
# =========================================================================

async def send_long_message(client, user_id, text, parse_mode='HTML', max_len=4000):
    """Делит длинное сообщение на части и отправляет их."""
    
    if len(text) <= max_len:
        return await client.send_message(user_id, text, parse_mode=parse_mode)
    
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
        # Используем .format() для надежности
        header = "📊 **Часть {}/{}**\n".format(i+1, len(parts))
        
        message_to_send = header + part
        
        if len(message_to_send) > max_len:
            message_to_send = part
        
        await client.send_message(user_id, message_to_send, parse_mode=parse_mode)
        await asyncio.sleep(0.5) 

async def stop_worker(user_id):
    """Останавливает Worker и очищает задачи."""
    
    if user_id in FLOOD_TASKS:
        for chat_id, task in FLOOD_TASKS[user_id].items():
            if task and not task.done():
                task.cancel()
        del FLOOD_TASKS[user_id]

    if user_id in ACTIVE_TELETHON_WORKERS:
        ACTIVE_TELETHON_WORKERS[user_id].cancel()
        del ACTIVE_TELETHON_WORKERS[user_id]
    
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

async def run_worker(user_id):
    await stop_worker(user_id)
    path = get_session_path(user_id)
    client = TelegramClient(path, API_ID, API_HASH)
    ACTIVE_TELETHON_CLIENTS[user_id] = client
    
    try:
        # --- ПРОВЕРКА СЕССИИ (Для сохранения при перезапуске) ---
        if not os.path.exists(path + '.session'):
            db_set_session_status(user_id, False)
            # Отправка уведомления, если сессия пропала
            await bot.send_message(user_id, "⚠️ Файл сессии не найден. Требуется повторная авторизация.", reply_markup=get_main_kb(user_id))
            return

        await client.start()
        db_set_session_status(user_id, True)
        logger.info("Worker {} started successfully.".format(user_id))

        # --- ЛОГИКА ФЛУДА И КОМАНД ---
        async def flood_task(peer, message, count, delay, chat_id):
            try:
                is_unlimited = count <= 0
                max_iterations = count if not is_unlimited else 999999999 
                
                PROCESS_PROGRESS[user_id] = {'type': 'flood', 'total': count, 'done': 0, 'peer': peer, 'chat_id': chat_id}
                
                for i in range(max_iterations):
                    if user_id in FLOOD_TASKS and chat_id not in FLOOD_TASKS[user_id]:
                        # Используем .format() для надежности
                        await client.send_message(user_id, "🛑 Флуд в чате `{}` остановлен по команде .стопфлуд.".format(get_display_name(peer)))
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
                # Используем .format() для надежности
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


        # --- Парсинг .ЧЕКГРУППУ (ПЕРЕРАБОТАНО ДЛЯ ВЫБОРА ФОРМАТА) ---
        async def check_group_task(event, target_chat_str, min_id, max_id):
            
            chat_id = event.chat_id
            if chat_id is None:
                 await client.send_message(user_id, "❌ `.чекгруппу` должен быть вызван из группы/канала или с указанием его юзернейма/ID.")
                 return
                 
            try:
                try:
                    chat_entity = await client.get_entity(target_chat_str)
                except Exception:
                    chat_entity = await client.get_entity(chat_id)

                unique_users = {} 
                limit = 1000000 
                
                # Используем .format() для надежности
                await client.send_message(user_id, "⏳ Начинаю сканирование **всех** сообщений в чате `{}` для сбора пользователей. Это может занять время.".format(get_display_name(chat_entity)))
                
                PROCESS_PROGRESS[user_id] = {'type': 'checkgroup', 'peer': chat_entity, 'done_msg': 0}
                
                async for message in client.iter_messages(chat_entity, limit=limit):
                    if user_id in PROCESS_PROGRESS and PROCESS_PROGRESS[user_id].get('type') != 'checkgroup':
                        # Процесс был отменен или заменен
                        return
                        
                    PROCESS_PROGRESS[user_id]['done_msg'] += 1
                    
                    if message.sender and isinstance(message.sender, User) and message.sender_id not in unique_users:
                        user_id_int = message.sender.id
                        
                        if (min_id is None or user_id_int >= min_id) and \
                           (max_id is None or user_id_int <= max_id):
                            
                            unique_users[user_id_int] = message.sender
                        
                        # Небольшое ограничение для скорости
                        if len(unique_users) >= 1000 and min_id is None and max_id is None:
                            pass


                # --- Формирование финального отчета ---
                total_found = len(unique_users)
                if total_found > 0:
                    report_data_raw = []
                    # Используем .format() для надежности
                    range_info = " (Фильтр ID: {}-{})".format(min_id or 'Все', max_id or 'Все') if min_id is not None or max_id is not None else ""
                    
                    for uid, p in unique_users.items():
                        full_name = ' '.join(filter(None, [p.first_name, p.last_name]))
                        # Используем .format() для надежности
                        report_data_raw.append(
                             "👤 Имя: {}\n🔗 Юзернейм: @{}🆔 ID: {}".format(
                                 full_name if full_name else 'Нет имени', 
                                 p.username if p.username else 'Нет', 
                                 uid
                             )
                        )
                        
                    # Используем .format() для надежности
                    header_text = (
                        "📊 Отчет .ЧЕКГРУППУ (по истории сообщений) {}\n"
                        "Чат: {}\n"
                        " • Просканировано сообщений: {}\n"
                        " • Найдено уникальных пользователей: {}\n"
                        "\nСписок пользователей (Имя, Юзернейм, ID):".format(
                            range_info,
                            get_display_name(chat_entity),
                            PROCESS_PROGRESS[user_id]['done_msg'],
                            total_found
                        )
                    )
                    
                    # Полный отчет для сохранения. 
                    full_report_text = header_text + "\n" + "\n".join(report_data_raw)
                    
                    # Сохраняем отчет во временное хранилище
                    PROCESS_PROGRESS[user_id]['report_data'] = full_report_text
                    PROCESS_PROGRESS[user_id]['peer_name'] = get_display_name(chat_entity)

                    # Отправляем уведомление с кнопками выбора
                    # Используем .format() для надежности
                    await client.send_message(
                        user_id, 
                        "✅ **Сбор данных завершен!** Найдено **{}** пользователей.\nВыберите, как получить отчет по чату `{}`:".format(total_found, get_display_name(chat_entity)),
                        buttons=[
                            [Button.inline("📄 Отправить файлом (.txt)", data="send_checkgroup_file_worker")],
                            [Button.inline("💬 Отправить сообщениями (по частям)", data="send_checkgroup_messages_worker")],
                            [Button.inline("❌ Удалить отчет", data="delete_checkgroup_report_worker")]
                        ]
                    )
                else:
                    response = "✅ **Отчет .ЧЕКГРУППУ:**\nПо указанным критериям (чат/диапазон ID) пользователи не найдены в истории сообщений."
                    await send_long_message(client, user_id, response, parse_mode='HTML')
                
            except RpcCallFailError as e:
                 # Используем .format() для надежности
                 await client.send_message(user_id, "❌ Ошибка RPC при .чекгруппу (чат недоступен): {}".format(type(e).__name__))
            except Exception as e:
                # Используем .format() для надежности
                await client.send_message(user_id, "❌ Критическая ошибка при .чекгруппу: {} - {}".format(type(e).__name__, e))
                
            finally:
                # Очистка прогресса, если отчет не сохранен или отправлен сразу
                if user_id in PROCESS_PROGRESS and 'report_data' not in PROCESS_PROGRESS[user_id]:
                    del PROCESS_PROGRESS[user_id]
                try:
                    await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
                except:
                    pass

        @client.on(events.NewMessage)
        async def handler(event):
            if not db_check_subscription(user_id) and user_id != ADMIN_ID: return
            if not event.out: return
            
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
                                # Используем .format() для надежности
                                results.append("❌ {}: Пропущен (Не похож на @юзернейм или ID)".format(target))
                                continue
                                
                            await client.send_message(target, text) 
                            # Используем .format() для надежности
                            results.append("✅ {}: Отправлено".format(target))
                        except ValueError: 
                            # Используем .format() для надежности
                            results.append("❌ {}: Ошибка (Некорректный ID/Юзернейм)".format(target))
                        except Exception as e:
                            # Используем .format() для надежности
                            results.append("❌ {}: Ошибка ({})".format(target, type(e).__name__))
                            
                    await event.reply("<b>Результаты .лс:</b>\n" + "\n".join(results), parse_mode='HTML')
                    
                except Exception as e:
                     # Используем .format() для надежности
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
                     # Используем .format() для надежности
                     return await event.reply("❌ Ожидается текстовый файл. Обнаружено: `{}`.".format(mime_type))
                
                
                try:
                    await event.reply("⏳ Начинаю скачивание и обработку файла...")
                    
                    with tempfile.TemporaryDirectory() as tmpdir:
                        downloaded_file_path = await client.download_media(reply_msg, file=os.path.join(tmpdir, filename or 'temp_file'))
                        
                        with open(downloaded_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            file_content = f.read()
                        
                        # Используем .format() для надежности
                        formatted_content = "📖 **Содержимое файла** (`{}`):\n".format(filename)
                        # Оборачиваем в <pre> для сохранения форматирования (столбцов)
                        formatted_content += "<pre>" + file_content + "</pre>"
                        
                        await send_long_message(client, user_id, formatted_content, parse_mode='HTML')
                        
                except Exception as e:
                    # Используем .format() для надежности
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
                        peer = await client.get_entity(current_chat_id)
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
                    
                    # Используем .format() для надежности
                    await event.reply(
                        "🔥 **Флуд запущен!**\nЧат: `{}`\nСообщений: {}\nЗадержка: {} сек.".format(
                            get_display_name(peer),
                            'Безлимитно' if count <= 0 else count,
                            delay
                        ), 
                        parse_mode='HTML'
                    )
                    
                    # Обновление прогресса в боте
                    try:
                        await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
                    except:
                        pass
                    
                except ValueError:
                    await event.reply("❌ Неверный формат чисел (кол-во/задержка).")
                except (UsernameInvalidError, PeerIdInvalidError, Exception) as e:
                    # Используем .format() для надежности
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
                        # Обновление прогресса в боте
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
                        peer_name = get_display_name(await client.get_entity(progress['peer']))
                        
                        # Используем .format() для надежности
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
                        # Используем .format() для надежности
                        status_text = (
                            "🔎 **СТАТУС АНАЛИЗА ЧАТА:**\n"
                            " • Цель: `{}`\n"
                            " • Просканировано сообщений: **{}**\n"
                            " • Статус: **{}**".format(
                                peer_name,
                                done_msg,
                                'Сбор данных...' if 'report_data' not in progress else 'Сбор завершен! Ожидание выбора формата.'
                            )
                        )
                    else:
                        # Используем .format() для надежности
                        status_text = "⚙️ Активный процесс: {}. Данные: {}".format(p_type, progress)
                else:
                    status_text = "✨ Активных процессов Worker'а нет."
                
                await event.reply(status_text, parse_mode='HTML')


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
                
                asyncio.create_task(check_group_task(event, target_chat_str, min_id, max_id))
                await event.reply("⏳ **Начинаю анализ группы...** Отчет будет отправлен вам в ЛС.", parse_mode='HTML')


            # --- Обработка инлайн-кнопок отчета .чекгруппу (Telethon) ---
            if event.query and event.query.data:
                data = event.query.data.decode()
                
                if data.startswith('send_checkgroup_') or data.startswith('delete_checkgroup_'):
                    if user_id not in PROCESS_PROGRESS or 'report_data' not in PROCESS_PROGRESS[user_id]:
                        return await event.answer("⚠️ Отчет устарел или был удален.")

                    report_data = PROCESS_PROGRESS[user_id]['report_data']
                    peer_name = PROCESS_PROGRESS[user_id]['peer_name']
                    
                    if data == 'send_checkgroup_file_worker':
                        await event.answer("⏳ Отправляю файл...")
                        
                        file_bytes = io.BytesIO(report_data.encode('utf-8'))
                        # Используем .format() для надежности
                        file_bytes.name = "checkgroup_report_{}_{}.txt".format(peer_name.replace(' ', '_'), datetime.now().strftime('%Y%m%d_%H%M%S'))
                        
                        # Используем .format() для надежности
                        await client.send_file(user_id, file_bytes, caption="📄 Отчет по чату `{}`".format(peer_name))
                        await event.edit("✅ Отчет по чату `{}` отправлен файлом.".format(peer_name))
                        
                    elif data == 'send_checkgroup_messages_worker':
                        await event.answer("⏳ Отправляю сообщения...")
                        
                        # Оборачиваем список пользователей в <pre> для сохранения форматирования
                        start_index = report_data.find("Список пользователей (Имя, Юзернейм, ID):")
                        if start_index != -1:
                            start_of_list = start_index + len("Список пользователей (Имя, Юзернейм, ID):")
                            
                            report_html = (
                                report_data[:start_of_list] + "\n" +
                                "<pre>" + report_data[start_of_list:].strip() + "</pre>"
                            )
                        else:
                            report_html = report_data 
                        
                        await send_long_message(client, user_id, report_html, parse_mode='HTML')
                        # Используем .format() для надежности
                        await event.edit("✅ Отчет по чату `{}` отправлен по частям.".format(peer_name))

                    elif data == 'delete_checkgroup_report_worker':
                        await event.answer("❌ Отчет удален.")
                        # Используем .format() для надежности
                        await event.edit("❌ Отчет по чату `{}` удален.".format(peer_name))

                    # Очистка прогресса после отправки/удаления
                    if user_id in PROCESS_PROGRESS:
                        del PROCESS_PROGRESS[user_id]
                             
                    # Обновление главного меню бота
                    try:
                        await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
                    except:
                        pass
                    
                    return await event.answer()


        worker_task = asyncio.create_task(client.run_until_disconnected())
        ACTIVE_TELETHON_WORKERS[user_id] = worker_task
        await worker_task
        
    except (AuthKeyUnregisteredError, UserDeactivatedError):
        await bot.send_message(user_id, "⚠️ Сессия недействительна. Пожалуйста, авторизуйтесь заново.")
        db_set_session_status(user_id, False)
    except Exception as e:
        # Используем .format() для надежности
        logger.error("Worker {} critical error: {}".format(user_id, e))
        # Используем .format() для надежности
        await bot.send_message(user_id, "❌ Критическая ошибка воркера: {}".format(type(e).__name__))
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS: del ACTIVE_TELETHON_CLIENTS[user_id]
        if user_id in ACTIVE_TELETHON_WORKERS: del ACTIVE_TELETHON_WORKERS[user_id]
        db_set_session_status(user_id, False)
        # Уведомление бота о завершении
        try:
             await bot.send_message(user_id, "ℹ️ Worker остановлен.", reply_markup=get_main_kb(user_id))
        except:
            pass


# =========================================================================
# VI. ХЕНДЛЕРЫ BOT
# =========================================================================

# --- ОСНОВНОЕ МЕНЮ И СТАРТ ---
@user_router.callback_query(F.data == "cancel_action")
async def cancel_handler(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    if uid in TEMP_AUTH_CLIENTS:
        try: await TEMP_AUTH_CLIENTS[uid].disconnect()
        except: pass
        del TEMP_AUTH_CLIENTS[uid]
    await state.clear()
    await cmd_start(call, state)

@user_router.callback_query(F.data == "back_to_main")
@user_router.message(Command("start"))
async def cmd_start(u: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = u.from_user.id
    db_get_user(user_id)
    await state.clear()
    
    has_access, msg = await check_access(user_id, bot)
    
    # Используем .format() для надежности
    sub = db_get_user(user_id).get('subscription_end_date')
    text = "👋 <b>Привет!</b> Ваш ID: <code>{}</code>\nПодписка до: <code>{}</code>\n\n".format(user_id, sub if sub else 'Нет')
    
    if not has_access:
        # Используем .format() для надежности
        text += "⚠️ <b>Доступ ограничен.</b>\n{}".format(msg)
        is_channel_reason = f"Для доступа к функциям подпишитесь на наш канал" in msg
        kb = get_no_access_kb(is_channel_reason)
    else:
        text += "✅ <b>Меню доступно.</b>\nИспользуйте кнопки ниже."
        kb = get_main_kb(user_id)

    if isinstance(u, types.Message): 
        await u.answer(text, reply_markup=kb)
    else: 
        await u.message.edit_text(text, reply_markup=kb)

# --- АВТОРИЗАЦИЯ: ВХОД ПО QR-КОДУ ---

@user_router.callback_query(F.data == "telethon_auth_qr_start")
async def auth_qr_start(call: types.CallbackQuery, state: FSMContext):
    has_access, _ = await check_access(call.from_user.id, bot)
    if not has_access:
        return await call.answer("Доступ к авторизации ограничен. Активируйте подписку.", show_alert=True)
    
    uid = call.from_user.id
    path = get_session_path(uid)
    
    if uid in TEMP_AUTH_CLIENTS:
        try: await TEMP_AUTH_CLIENTS[uid].disconnect()
        except: pass
        del TEMP_AUTH_CLIENTS[uid]

    client = TelegramClient(path, API_ID, API_HASH)
    TEMP_AUTH_CLIENTS[uid] = client

    try:
        if not client.is_connected(): await client.connect()
        
        qr_login = await client.qr_login()
        await state.update_data(qr_login=qr_login)
        await state.set_state(TelethonAuth.WAITING_FOR_QR_LOGIN)

        # Используем .format() для надежности
        await call.message.edit_text(
            "📲 **Авторизация по QR-коду**\n"
            "1. Откройте **Настройки** -> **Устройства** -> **Привязать настольное устройство**.\n"
            "2. Нажмите на ссылку ниже, чтобы ваш Telegram-клиент отобразил QR-код для сканирования:\n\n"
            "🔗 [Нажмите для отображения QR-кода]({})\n\n"
            "Ожидаю сканирования (2 минуты)...".format(qr_login.url), 
            reply_markup=get_cancel_kb(),
            disable_web_page_preview=False
        )
        
        asyncio.create_task(wait_for_qr_login(uid, client, state, call.message.chat.id, call.message.message_id))

    except Exception as e:
        logger.error("QR auth start error: {}".format(e))
        # Используем .format() для надежности
        await call.message.edit_text("❌ Ошибка QR-авторизации: {}".format(type(e).__name__), reply_markup=get_main_kb(uid))
        await state.clear()

async def wait_for_qr_login(uid, client, state, chat_id, message_id):
    try:
        data = await state.get_data()
        qr_login = data.get('qr_login')

        await qr_login.wait(timeout=120) 
        
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        
        await bot.edit_message_text(
            chat_id=chat_id, 
            message_id=message_id, 
            text="✅ Успешно вошли по QR-коду! Worker запущен.", 
            reply_markup=get_main_kb(uid)
        )
        await state.clear()
        
    except asyncio.TimeoutError:
        await client.log_out() 
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        if await state.get_state() == TelethonAuth.WAITING_FOR_QR_LOGIN:
            await bot.edit_message_text(
                chat_id=chat_id, 
                message_id=message_id, 
                text="❌ Время ожидания QR-кода истекло. Начните заново.", 
                reply_markup=get_main_kb(uid)
            )
            await state.clear()
            
    except Exception as e:
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        if await state.get_state() == TelethonAuth.WAITING_FOR_QR_LOGIN:
             # Используем .format() для надежности
             await bot.edit_message_text(
                chat_id=chat_id, 
                message_id=message_id, 
                text="❌ Ошибка при входе по QR-коду: {}. Попробуйте Вход по Номеру/Коду.".format(type(e).__name__), 
                reply_markup=get_main_kb(uid)
            )
             await state.clear()
        logger.error("QR login wait error: {}".format(e))


# --- АВТОРИЗАЦИЯ: ВХОД ПО НОМЕРУ/КОДУ (Старый метод) ---

@user_router.callback_query(F.data == "telethon_auth_phone_start")
async def auth_phone_start(call: types.CallbackQuery, state: FSMContext):
    has_access, _ = await check_access(call.from_user.id, bot)
    if not has_access:
        return await call.answer("Доступ к авторизации ограничен. Активируйте подписку.", show_alert=True)
    
    uid = call.from_user.id
    path = get_session_path(uid)
    
    if uid in TEMP_AUTH_CLIENTS:
        try: await TEMP_AUTH_CLIENTS[uid].disconnect()
        except: pass
        del TEMP_AUTH_CLIENTS[uid]

    client = TelegramClient(path, API_ID, API_HASH)
    TEMP_AUTH_CLIENTS[uid] = client
    
    await state.set_state(TelethonAuth.PHONE)
    await call.message.edit_text("📞 Введите ваш номер телефона (в формате +79991234567):", reply_markup=get_cancel_kb())

@user_router.message(TelethonAuth.PHONE)
async def auth_msg_phone(msg: Message, state: FSMContext):
    phone = msg.text.strip()
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)

    if not client:
        await msg.answer("⚠️ Сессия авторизации истекла. Начните заново.", reply_markup=get_main_kb(uid))
        await state.clear()
        return

    try:
        if not client.is_connected(): await client.connect()
        result = await client.send_code_request(phone)
        
        await state.update_data(phone=phone, phone_hash=result.phone_code_hash, current_code="") 
        await state.set_state(TelethonAuth.CODE)
        
        await msg.answer("✉️ **Код подтверждения отправлен.**\nВведите его, используя кнопки ниже или отправьте текстом:", reply_markup=get_code_kb())

    except PhoneNumberInvalidError:
        await msg.answer("❌ Неверный формат номера телефона.", reply_markup=get_cancel_kb())
    except Exception as e:
        logger.error("Auth phone step error: {}".format(e))
        # Используем .format() для надежности
        await msg.answer("❌ Ошибка отправки кода: {}".format(e), reply_markup=get_main_kb(uid))
        await state.clear()

@user_router.message(TelethonAuth.CODE)
async def auth_msg_code(msg: Message, state: FSMContext):
    code = re.sub(r'\D', '', msg.text.strip())
    await process_code_submit(msg, state, code)

@user_router.callback_query(F.data.startswith("code_input_"), StateFilter(TelethonAuth.CODE))
async def code_kb_handler(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    current_code = data.get('current_code', '')
    action = call.data.split('_')[-1]
    
    uid = call.from_user.id

    if action.isdigit():
        if len(current_code) < 10: 
            new_code = current_code + action
            await state.update_data(current_code=new_code)
            await call.message.edit_reply_markup(reply_markup=get_code_kb(new_code))
        else:
            await call.answer("Максимальная длина кода.", show_alert=True)
            
    elif action == 'delete':
        new_code = current_code[:-1]
        await state.update_data(current_code=new_code)
        await call.message.edit_reply_markup(reply_markup=get_code_kb(new_code))
        
    elif action == 'submit':
        if len(current_code) >= 5: 
            await call.message.edit_text("⏳ Проверка кода...", reply_markup=None)
            await process_code_submit(call, state, current_code)
        else:
            await call.answer("Код слишком короткий.", show_alert=True)

async def process_code_submit(u: types.Message | types.CallbackQuery, state: FSMContext, code: str):
    uid = u.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    
    # Используем .format() для надежности
    if not client:
        await (u.message if isinstance(u, types.CallbackQuery) else u).answer("⚠️ Сессия авторизации истекла. Начните заново.", reply_markup=get_main_kb(uid))
        await state.clear()
        return

    if not code:
        # Используем .format() для надежности
        return await (u.message if isinstance(u, types.CallbackQuery) else u).answer("❌ Код не распознан. Пожалуйста, введите только цифры.", reply_markup=get_code_kb(code))

    d = await state.get_data()
    
    if isinstance(u, types.Message):
         await u.answer("⏳ Проверка кода...", reply_markup=types.ReplyKeyboardRemove())

    try:
        if not client.is_connected(): await client.connect()
        await client.sign_in(d['phone'], code, phone_code_hash=d['phone_hash'])
        
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await bot.send_message(uid, "✅ Успешно вошли! Worker запущен.", reply_markup=get_main_kb(uid))
        await state.clear()
        
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await bot.send_message(uid, "🔒 Требуется двухфакторная авторизация (2FA). Введите **пароль**:", reply_markup=get_cancel_kb())
            
    except (PhoneCodeExpiredError, PhoneCodeInvalidError) as e:
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        # Используем .format() для надежности
        await bot.send_message(uid, 
            "❌ Код недействителен или истек. Начните авторизацию сначала. Если ошибка повторяется, <b>полностью перезапустите Python-скрипт.</b>\nОшибка: {}".format(type(e).__name__), 
            reply_markup=get_main_kb(uid)
        )
        await state.clear()
        
    except Exception as e:
        logger.error("Auth code step error: {}".format(e))
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        # Используем .format() для надежности
        await bot.send_message(uid, "❌ Неизвестная ошибка: {}".format(e), reply_markup=get_main_kb(uid))
        await state.clear()

@user_router.message(TelethonAuth.PASSWORD)
async def auth_pwd(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    if not client:
        await msg.answer("⚠️ Сессия истекла.", reply_markup=get_main_kb(uid))
        await state.clear()
        return
    
    sign_in_password = msg.text.strip()
    
    try:
        if not client.is_connected(): await client.connect()
        await client.sign_in(password=sign_in_password) 
        
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Успешно вошли (2FA)! Worker запущен.", reply_markup=get_main_kb(uid))
        await state.clear()
        
    except PasswordHashInvalidError:
        await msg.answer(
            "❌ Неверный пароль 2FA. Повторите ввод:", 
            reply_markup=get_cancel_kb()
        )
    except Exception as e:
        logger.error("Auth password step error: {}".format(e))
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        # Используем .format() для надежности
        await msg.answer("❌ Неизвестная ошибка 2FA: {}".format(e), reply_markup=get_main_kb(uid))
        await state.clear()

# --- УПРАВЛЕНИЕ WORKER'ОМ ---
@user_router.callback_query(F.data.in_({'telethon_start_session', 'telethon_stop_session', 'telethon_check_status'}))
async def manage_worker(call: types.CallbackQuery):
    uid = call.from_user.id
    
    if call.data == 'telethon_start_session':
        # Используем .format() для надежности
        if not os.path.exists(get_session_path(uid) + '.session'):
            return await call.answer("⚠️ Файл сессии не найден. Требуется авторизация.", show_alert=True)
            
        asyncio.create_task(run_worker(uid))
        await call.answer("🚀 Worker запускается...", show_alert=True)
        await call.message.edit_reply_markup(reply_markup=get_main_kb(uid))
        
    elif call.data == 'telethon_stop_session':
        await stop_worker(uid)
        await call.answer("🛑 Worker остановлен.", show_alert=True)
        await call.message.edit_reply_markup(reply_markup=get_main_kb(uid))
        
    elif call.data == 'telethon_check_status':
        is_active = uid in ACTIVE_TELETHON_WORKERS
        status_text = "🟢 Активен и Запущен" if is_active else "🔴 Неактивен"
        # Используем .format() для надежности
        await call.answer("Статус Worker'а: {}".format(status_text), show_alert=True)
        
# --- ПРОГРЕСС АКТИВНЫХ ЗАДАЧ ---
@user_router.callback_query(F.data == "show_progress")
async def show_progress_handler(call: types.CallbackQuery):
    uid = call.from_user.id
    if uid not in PROCESS_PROGRESS:
        await call.answer("Нет активных процессов.", show_alert=True)
        return
        
    progress = PROCESS_PROGRESS[uid]
    p_type = progress['type']
    
    status_text = "✨ **АКТИВНЫЙ ПРОЦЕСС:**\n"
    
    if p_type == 'flood':
        total = progress['total']
        done = progress['done']
        
        # Используем .format() для надежности
        status_text += (
            " • **Тип:** Флуд\n"
            " • Отправлено: **{}**\n"
            " • Всего: **{}**\n"
            " • Прогресс: **{}**".format(
                done,
                'Безлимитно' if total <= 0 else total,
                '{:.2f}%'.format(done/total*100) if total > 0 else '—'
            )
        )
    elif p_type == 'checkgroup':
        peer_name = progress.get('peer_name', 'Неизвестно')
        done_msg = progress['done_msg']
        # Используем .format() для надежности
        status_text += (
            " • **Тип:** Анализ Чата\n"
            " • Цель: `{}`\n"
            " • Просканировано сообщений: **{}**\n"
            " • Статус: **{}**".format(
                peer_name,
                done_msg,
                'Сбор данных...' if 'report_data' not in progress else 'Сбор завершен! (Ожидание выбора формата в ЛС Worker\'а)'
            )
        )

    await call.message.edit_text(status_text, reply_markup=get_main_kb(uid))


# --- АКТИВАЦИЯ ПРОМОКОДА ---
@user_router.callback_query(F.data == "start_promo_fsm")
async def user_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_for_code)
    await call.message.edit_text("Введи промокод:", reply_markup=get_cancel_kb())

@user_router.message(PromoStates.waiting_for_code)
async def user_promo_check(msg: Message, state: FSMContext):
    code = msg.text.strip()
    p = db_get_promo(code)
    
    if p and p['is_active'] and (p['max_uses'] is None or p['current_uses'] < p['max_uses']):
        db_use_promo(code)
        days_granted = p['days']
        end = db_update_subscription(msg.from_user.id, days_granted)
        
        has_access, _ = await check_access(msg.from_user.id, bot)
        
        # Используем .format() для надежности
        message_text = "✅ Промокод <code>{0}</code> активирован!\nПодписка продлена до <b>{1}</b>. ".format(code, end)
        
        if not has_access:
            message_text += "Для полного доступа, пожалуйста, подпишитесь на наш канал: {}".format(TARGET_CHANNEL_URL)
        else:
            message_text += "Теперь вы можете использовать все функции."

        await msg.answer(message_text, reply_markup=get_main_kb(msg.from_user.id))
    else:
        await msg.answer("❌ Неверный, истекший код или превышен лимит использований.", 
                         reply_markup=get_main_kb(msg.from_user.id))
                         
    await state.clear()

# --- АДМИН ПАНЕЛЬ ---

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_panel_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("Недостаточно прав.")
    await state.set_state(AdminStates.main_menu)
    await call.message.edit_text("🛠️ **Админ-Панель**\nВыберите действие:", reply_markup=get_admin_kb())

@user_router.callback_query(F.data == "admin_create_promo", StateFilter(AdminStates.main_menu))
async def admin_create_promo(call: types.CallbackQuery, state: FSMContext):
    """Начинает создание промокода, автоматически генерируя код."""
    new_code = generate_promo_code()
    await state.update_data(code=new_code) # Сохраняем сгенерированный код
    await state.set_state(AdminStates.promo_days_input)
    # Используем .format() для надежности
    await call.message.edit_text("📝 **Генерация промокода:** `{}`\n\n📅 Сколько дней подписки дает промокод? (например, `7`):".format(new_code), reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_days_input)
async def admin_promo_days_input(msg: Message, state: FSMContext):
    try:
        days = int(msg.text.strip())
        await state.update_data(days=days)
        await state.set_state(AdminStates.promo_uses_input)
        await msg.answer("🔢 Сколько раз можно использовать промокод? (0 или -1 для бесконечно):", reply_markup=get_cancel_kb())
    except ValueError:
        await msg.answer("❌ Введите корректное число дней.", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_uses_input)
async def admin_promo_uses_input(msg: Message, state: FSMContext):
    try:
        max_uses = int(msg.text.strip())
        data = await state.get_data()
        
        db_add_promo(data['code'], data['days'], max_uses if max_uses > 0 else None)
        
        # Используем .format() для надежности
        await msg.answer(
            "✅ Промокод создан:\nКод: <code>{}</code>\nДни: {}\nЛимит: {}".format(
                data['code'], 
                data['days'], 
                max_uses if max_uses > 0 else 'Нет'
            ), 
            reply_markup=get_admin_kb()
        )
    except ValueError:
        await msg.answer("❌ Введите корректное число использований.", reply_markup=get_cancel_kb())
    finally:
        await state.set_state(AdminStates.main_menu)

@user_router.callback_query(F.data == "admin_grant_sub", StateFilter(AdminStates.main_menu))
async def admin_grant_sub_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.sub_user_id_input)
    await call.message.edit_text("👤 Введите ID пользователя, которому выдать подписку:", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_user_id_input)
async def admin_sub_user_id_input(msg: Message, state: FSMContext):
    try:
        target_id = int(msg.text.strip())
        await state.update_data(target_id=target_id)
        await state.set_state(AdminStates.sub_days_input)
        # Используем .format() для надежности
        await msg.answer("📅 ID {} принят. Сколько дней выдать?".format(target_id), reply_markup=get_cancel_kb())
    except ValueError:
        await msg.answer("❌ Введите корректный числовой ID.", reply_markup=get_cancel_kb())

# =========================================================================
# !!! ИСПРАВЛЕННЫЙ БЛОК ДЛЯ ОШИБКИ 1171 !!!
# =========================================================================
@user_router.message(AdminStates.sub_days_input)
async def admin_sub_days_input(msg: Message, state: FSMContext):
    try:
        days = int(msg.text.strip())
        data = await state.get_data()
        target_id = data['target_id']
        
        end = db_update_subscription(target_id, days)
        
        # БЛОК 1: ОТВЕТ АДМИНУ - ИСПОЛЬЗУЕМ .format() для гарантированной работы
        admin_message_text = "✅ Подписка выдана пользователю <code>{}</code> на {} дней.\nНовая дата окончания: <b>{}</b>".format(target_id, days, end)
        
        await msg.answer(
            admin_message_text, 
            reply_markup=get_admin_kb()
        )
        
        # БЛОК 2: ОТПРАВКА ПОЛЬЗОВАТЕЛЮ - ИСПОЛЬЗУЕМ .format()
        user_message_text = "🎉 Вам выдана подписка на {} дней до {}!".format(days, end)
        
        await bot.send_message(
            target_id, 
            user_message_text, 
            reply_markup=get_main_kb(target_id)
        )

    except ValueError:
        await msg.answer("❌ Введите корректное число дней.", reply_markup=get_cancel_kb())
    except TelegramForbiddenError:
        await msg.answer("⚠️ Не удалось уведомить пользователя (бот заблокирован).", reply_markup=get_admin_kb())
    finally:
        await state.set_state(AdminStates.main_menu)
# =========================================================================

# --- ПОМОЩЬ ---
@user_router.callback_query(F.data == "show_help")
@user_router.message(Command("help"))
async def cmd_help(u: types.Message | types.CallbackQuery):
    help_text = (
        "📚 <b>Справка и Команды (Worker):</b>\n\n"
        "Для работы инструментов сначала авторизуйтесь через одну из **🔐 Вход...** опций и запустите **Worker**.\n\n"
        "**Инструменты (вводятся в любом чате от вашего имени):**\n"
        " • <code>.лс [сообщение]</code>\n"
        "   <code>[@юзернейм1]</code>\n"
        "   <code>[ID2]</code> — Отправка **личных сообщений** по списку адресатов, указанных с новой строки.\n"
        " • <code>.флуд [кол-во] [текст] [задержка] [опц: @чат/ID]</code> — **Флуд**. Если чат не указан, флуд идет в текущий чат. (0/-1 для безлимита. Мин. задержка: 0.5 сек).\n"
        " • <code>.стопфлуд</code> — **Остановить** флуд **только в текущем чате**.\n"
        " • <code>.статус</code> — Показать **прогресс** активной задачи.\n"
        " • <code>.чекгруппу [опц: @чат/ID] [опц: мин_ID-макс_ID]</code> — **Анализ** **всех** пользователей, писавших в чат (по истории сообщений). После сбора **отчет будет предложено выбрать формат** (файл/сообщения) в ЛС Worker'а.\n"
        " • <code>.тхт</code> или <code>.таблица</code> — **Ответом** на файл (.txt, .log, .csv) преобразует его содержимое в форматированный текстовый блок в чате."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")]])
    if isinstance(u, types.Message):
        await u.answer(help_text, reply_markup=kb)
    else:
        await u.message.edit_text(help_text, reply_markup=kb)

# -------------------------------------------------------------------------

async def main():
    logger.info("START BOT")
    db_init()
    dp.include_router(user_router)
    await start_workers()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
