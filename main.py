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
# Удаляем импорт Button из telethon, так как кнопки будут обрабатываться Aiogram

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
# Инициализация бота
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=storage)
user_router = Router()

# =========================================================================
# II. FSM СОСТОЯНИЯ
# =========================================================================

# ... (Остальные состояния остаются без изменений)
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

# ... (Функции DB остаются без изменений)
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

# ... (Остальные утилиты остаются без изменений)
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
             # Изменяем callback для отправки отчета через Aiogram-бота
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

# ... (send_long_message, stop_worker, start_workers остаются без изменений, но send_long_message нужно перенести в секцию Aiogram)

async def send_long_message_aiogram(user_id, text, parse_mode='HTML', max_len=4000):
    """Делит длинное сообщение на части и отправляет их через Aiogram-бота."""
    
    if len(text) <= max_len:
        return await bot.send_message(user_id, text, parse_mode=parse_mode)
    
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
        
        await bot.send_message(user_id, message_to_send, parse_mode=parse_mode)
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

# --- ИЗМЕНЕННАЯ ФУНКЦИЯ ДЛЯ ОТПРАВКИ ОТЧЕТА ЧЕРЕЗ AIOGRAM-БОТ ---
async def check_group_task(client, event, target_chat_str, min_id, max_id, aiogram_chat_id):
    
    user_id = event.sender_id
    chat_id = event.chat_id
    if chat_id is None and not target_chat_str:
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
        # Отправляем уведомление через Aiogram-бот
        await bot.send_message(aiogram_chat_id, "⏳ Worker начинает сканирование **всех** сообщений в чате `{}` для сбора пользователей. Это может занять время.".format(get_display_name(chat_entity)), parse_mode='Markdown')
        
        PROCESS_PROGRESS[user_id] = {'type': 'checkgroup', 'peer_name': get_display_name(chat_entity), 'done_msg': 0, 'aiogram_chat_id': aiogram_chat_id}
        
        async for message in client.iter_messages(chat_entity, limit=limit):
            if user_id in PROCESS_PROGRESS and PROCESS_PROGRESS[user_id].get('type') != 'checkgroup':
                return # Процесс был отменен
                
            PROCESS_PROGRESS[user_id]['done_msg'] += 1
            
            if message.sender and isinstance(message.sender, User) and message.sender_id not in unique_users:
                user_id_int = message.sender.id
                
                if (min_id is None or user_id_int >= min_id) and \
                   (max_id is None or user_id_int <= max_id):
                    
                    unique_users[user_id_int] = message.sender
            
            # Обновление прогресса каждые 1000 сообщений (для наглядности)
            if PROCESS_PROGRESS[user_id]['done_msg'] % 1000 == 0:
                 # Используем .format() для надежности
                 await bot.send_message(aiogram_chat_id, "ℹ️ Просканировано: **{}** сообщений...".format(PROCESS_PROGRESS[user_id]['done_msg']), parse_mode='Markdown')

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
            
            # Сохраняем отчет во временное хранилище Aiogram-бота
            PROCESS_PROGRESS[user_id]['report_data'] = full_report_text
            PROCESS_PROGRESS[user_id]['peer_name'] = get_display_name(chat_entity)

            # Отправляем уведомление с кнопками выбора ЧЕРЕЗ AIOGRAM
            # Используем .format() для надежности
            await bot.send_message(
                aiogram_chat_id, 
                "✅ **Сбор данных завершен!** Найдено **{}** пользователей.\nВыберите, как получить отчет по чату `{}`:".format(
                    total_found, get_display_name(chat_entity)
                ),
                reply_markup=get_report_choice_kb(),
                parse_mode='HTML'
            )
        else:
            response = "✅ **Отчет .ЧЕКГРУППУ:**\nПо указанным критериям (чат/диапазон ID) пользователи не найдены в истории сообщений."
            await bot.send_message(aiogram_chat_id, response, parse_mode='HTML')
        
    except RpcCallFailError as e:
         # Используем .format() для надежности
         await bot.send_message(aiogram_chat_id, "❌ Ошибка RPC при .чекгруппу (чат недоступен): {}".format(type(e).__name__))
    except Exception as e:
        # Используем .format() для надежности
        await bot.send_message(aiogram_chat_id, "❌ Критическая ошибка при .чекгруппу: {} - {}".format(type(e).__name__, e))
        
    finally:
        # Очистка прогресса, если отчет не сохранен (в случае ошибки)
        if user_id in PROCESS_PROGRESS and 'report_data' not in PROCESS_PROGRESS[user_id]:
            del PROCESS_PROGRESS[user_id]
        
        # Обновляем главное меню бота, если нет других активных процессов
        if user_id not in PROCESS_PROGRESS and user_id not in FLOOD_TASKS:
            try:
                await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
            except:
                pass


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
        # (Flood task остается без изменений, но будет использовать client.send_message)
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

            # ... (ЛОГИКА ДРУГИХ КОМАНД .ЛС, .ТХТ, .ФЛУД, .СТОПФЛУД, .СТАТУС остается без изменений)

            # .ЛС
            if cmd == '.лс':
                # ... (Логика .лс)
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
                        
                        # Отправка через Telethon, так как команда была там вызвана
                        await client.send_message(event.chat_id, formatted_content, parse_mode='HTML')
                        
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
                                'Сбор данных...' if 'report_data' not in progress else 'Сбор завершен! Ожидание выбора формата в Aiogram-боте.'
                            )
                        )
                    else:
                        # Используем .format() для надежности
                        status_text = "⚙️ Активный процесс: {}. Данные: {}".format(p_type, progress)
                else:
                    status_text = "✨ Активных процессов Worker'а нет."
                
                # Отправка через Telethon (так как команда была вызвана Worker'ом)
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
                
                # Запускаем задачу, передавая ID чата Aiogram (т.к. Worker знает, что пользователь Aiogram-бота - это тот же ID)
                aiogram_chat_id = user_id
                asyncio.create_task(check_group_task(client, event, target_chat_str, min_id, max_id, aiogram_chat_id))
                
                # Подтверждение от Worker'а в чат, где была вызвана команда
                await event.reply("⏳ **Начинаю анализ группы...** Ожидайте уведомления в чате с ботом `@STATPBot`.", parse_mode='HTML')
                
        # --- Удаляем обработку инлайн-кнопок Telethon, так как теперь их обрабатывает Aiogram ---

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
# ... (Хендлеры до TelethonAuth.PASSWORD остаются без изменений)
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
        
# ... (Остальные хендлеры авторизации, промокодов и админки остаются без изменений)

# --- НОВЫЕ ХЕНДЛЕРЫ ДЛЯ ОБРАБОТКИ КНОПОК ОТЧЕТА (AIOGRAM) ---

@user_router.callback_query(F.data.startswith('send_report_') | F.data == 'delete_report')
async def handle_report_choice(call: types.CallbackQuery):
    user_id = call.from_user.id
    data = call.data
    
    if user_id not in PROCESS_PROGRESS or 'report_data' not in PROCESS_PROGRESS[user_id]:
        return await call.answer("⚠️ Отчет устарел или был удален.", show_alert=True)

    report_data = PROCESS_PROGRESS[user_id]['report_data']
    peer_name = PROCESS_PROGRESS[user_id]['peer_name']
    
    try:
        if data == 'send_report_file':
            await call.answer("⏳ Отправляю файл...")
            
            file_bytes = io.BytesIO(report_data.encode('utf-8'))
            # Используем .format() для надежности
            file_bytes.name = "checkgroup_report_{}_{}.txt".format(peer_name.replace(' ', '_'), datetime.now().strftime('%Y%m%d_%H%M%S'))
            
            await bot.send_document(user_id, FSInputFile(file_bytes, filename=file_bytes.name), caption=f"📄 Отчет по чату `{peer_name}`")
            await call.message.edit_text(f"✅ Отчет по чату `{peer_name}` отправлен файлом.", reply_markup=None)
            
        elif data == 'send_report_messages':
            await call.answer("⏳ Отправляю сообщения...")
            
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
            
            await send_long_message_aiogram(user_id, report_html, parse_mode='HTML')
            await call.message.edit_text(f"✅ Отчет по чату `{peer_name}` отправлен по частям.", reply_markup=None)

        elif data == 'delete_report':
            await call.answer("❌ Отчет удален.")
            await call.message.edit_text(f"❌ Отчет по чату `{peer_name}` удален.", reply_markup=None)

    except Exception as e:
        logger.error(f"Error sending report via Aiogram: {e}")
        await call.answer("❌ Произошла ошибка при отправке отчета.", show_alert=True)
        # Используем .format() для надежности
        await call.message.edit_text("❌ Произошла ошибка: {}".format(type(e).__name__), reply_markup=None)
    
    finally:
        # Очистка прогресса после отправки/удаления
        if user_id in PROCESS_PROGRESS and 'report_data' in PROCESS_PROGRESS[user_id]:
            del PROCESS_PROGRESS[user_id]
            
        await cmd_start(call, FSMContext(storage, user_id, user_id)) # Обновляем главное меню

# ... (Остальные хендлеры AdminStates.sub_days_input и cmd_help остаются без изменений)


# --- ЗАПУСК ---
async def main():
    logger.info("START BOT")
    db_init()
    dp.include_router(user_router)
    await start_workers()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
