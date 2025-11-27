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
from typing import Dict, Union, Optional

# =========================================================================
# I. КОНФИГУРАЦИЯ И НАСТРОЙКА
# =========================================================================

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = "7868097991:AAFWAAw1357IWkGXr9cOpqY11xBtnB0xJSg" 
ADMIN_ID = 6256576302  # ВАШ АДМИН ID
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1" 
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
DB_TIMEOUT = 10 

# --- ПУТИ ---
DATA_DIR = 'data'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
DB_PATH = os.path.join(DATA_DIR, DB_NAME)
SESSION_DIR = DATA_DIR

# --- НАСТРОЙКА ЛОГИРОВАНИЯ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- ГЛОБАЛЬНЫЕ ХРАНИЛИЩА ---
# Telethon Client objects (для Worker'а)
ACTIVE_TELETHON_CLIENTS: Dict[int, TelegramClient] = {}
# Asyncio Task objects (для Worker'а)
ACTIVE_TELETHON_WORKERS: Dict[int, asyncio.Task] = {}
# Telethon Client objects (для FSM авторизации)
TEMP_AUTH_CLIENTS: Dict[int, TelegramClient] = {}
# Задачи флуда
FLOOD_TASKS: Dict[int, Dict[int, asyncio.Task]] = {} 
# Прогресс задач (флуд, парсинг)
PROCESS_PROGRESS: Dict[int, Dict] = {} 

# --- ИМПОРТЫ AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, FSInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, StateFilter 
from aiogram.client.default import DefaultBotProperties

# --- ИМПОРТЫ TELETHON ---
from telethon import TelegramClient, events
from telethon.tl.types import User
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError, 
    PhoneCodeExpiredError, PhoneCodeInvalidError, 
    PasswordHashInvalidError, UsernameInvalidError, PeerIdInvalidError, 
    RpcCallFailError, ApiIdInvalidError
)
from telethon.utils import get_display_name 
from telethon.tl.custom import Button 

# --- ИНИЦИАЛИЗАЦИЯ AIOGRAM ---
storage = MemoryStorage()
# ИСПРАВЛЕНО: parse_mode='HTML'
default_properties = DefaultBotProperties(parse_mode='HTML')
bot = Bot(token=BOT_TOKEN, default=default_properties)
dp = Dispatcher(storage=storage)
user_router = Router()

# =========================================================================
# II. FSM СОСТОЯНИЯ
# =========================================================================

class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State() 
    # WAITING_FOR_QR_LOGIN не нужно, т.к. Telethon ждет сам

class PromoStates(StatesGroup):
    waiting_for_code = State()

class AdminStates(StatesGroup):
    promo_days_input = State()
    promo_uses_input = State()
    sub_user_id_input = State()
    sub_days_input = State()

# =========================================================================
# III. БАЗА ДАННЫХ (НЕ ИЗМЕНЕНА)
# =========================================================================

def get_db_connection():
    return sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT)

# ... (Остальные функции db_init, db_get_user, db_check_subscription и т.д. остаются без изменений) ...
# Добавляем все функции БД, чтобы код был полным
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
        conn.commit()
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
        conn.commit()
        return new_end

def db_set_session_status(user_id, status):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET telethon_active=? WHERE user_id=?", (1 if status else 0, user_id))
        conn.commit()

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
        conn.commit()

def db_add_promo(code, days, max_uses):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO promo_codes (code, days, max_uses) VALUES (?, ?, ?)", (code, days, max_uses))
        conn.commit()

def db_get_active_telethon_users():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE telethon_active=1")
        return [row[0] for row in cur.fetchall()]

# =========================================================================
# IV. УТИЛИТЫ И КЛАВИАТУРЫ
# =========================================================================

def get_session_path(user_id, is_temp=False):
    suffix = '_temp' if is_temp else ''
    return os.path.join(SESSION_DIR, f'session_{user_id}{suffix}')
    
def generate_promo_code(length=10):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

async def check_access(user_id: int):
    # Убираем 'bot: Bot' из аргументов, т.к. бот доступен глобально
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
        return False, f"❌ Для доступа к функциям подпишитесь на наш канал: {TARGET_CHANNEL_URL}"

    if db_check_subscription(user_id): 
        return True, ""
    
    # ⚠️ Исправлено: Возвращаем False, даже если подписка истекла, чтобы предложить промокод
    return False, "❌ Ваша подписка истекла. Активируйте промокод."

# Упрощенные клавиатуры
def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

def get_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Создать Промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="👤 Выдать Подписку", callback_data="admin_grant_sub")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])
    
def get_main_kb(user_id):
    user = db_get_user(user_id)
    active = user.get('telethon_active')
    running = user_id in ACTIVE_TELETHON_WORKERS
    has_progress = user_id in PROCESS_PROGRESS 
    
    kb = []
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    kb.append([InlineKeyboardButton(text="❓ Помощь / Команды", callback_data="show_help")]) 
    
    # --- БЛОК АВТОРИЗАЦИИ ---
    if not active:
        kb.append([InlineKeyboardButton(text="📲 Вход по QR-коду (Рекоменд.)", callback_data="telethon_auth_qr_start")])
        kb.append([InlineKeyboardButton(text="🔐 Вход по Номеру/Коду (Старый)", callback_data="telethon_auth_phone_start")])
    else:
        # --- БЛОК УПРАВЛЕНИЯ WORKER'ом ---
        if has_progress:
             kb.append([InlineKeyboardButton(text="⚡️ Активный Прогресс", callback_data="show_progress")])
             
        kb.append([InlineKeyboardButton(text="🚀 Остановить Worker" if running else "🟢 Запустить Worker", callback_data="telethon_stop_session" if running else "telethon_start_session")])
        kb.append([InlineKeyboardButton(text="ℹ️ Статус Сессии", callback_data="telethon_check_status")])
        kb.append([InlineKeyboardButton(text="❌ Выход (Удалить Сессию)", callback_data="telethon_logout")])

    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_no_access_kb(is_channel_reason):
    kb = []
    if is_channel_reason:
        kb.append([InlineKeyboardButton(text="✅ Подписаться на канал", url=f"https://t.me/{TARGET_CHANNEL_URL.lstrip('@')}")])
    
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    
    kb.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")])
        
    return InlineKeyboardMarkup(inline_keyboard=kb)


# =========================================================================
# V. TELETHON WORKER (ОСНОВНОЕ ЯДРО)
# =========================================================================

async def send_long_message(client, user_id, text, parse_mode='HTML', max_len=4096):
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
        # Добавляем заголовок к частям, но без превышения лимита
        header = f"📊 **Часть {i+1}/{len(parts)}**\n"
        if len(part) < max_len - len(header):
             message_to_send = header + part
        else:
             message_to_send = part # Слишком длинная часть, отправляем без заголовка
        
        await client.send_message(user_id, message_to_send, parse_mode=parse_mode)
        await asyncio.sleep(0.5) 

async def stop_worker(user_id, force_disconnect=True):
    """Останавливает Worker и очищает задачи."""
    
    # 1. Отмена задач флуда
    if user_id in FLOOD_TASKS:
        for chat_id, task in FLOOD_TASKS[user_id].items():
            if task and not task.done():
                task.cancel()
        del FLOOD_TASKS[user_id]
    
    # 2. Отмена основной задачи Worker'а
    if user_id in ACTIVE_TELETHON_WORKERS:
        task = ACTIVE_TELETHON_WORKERS[user_id]
        if not task.done():
             task.cancel()
        del ACTIVE_TELETHON_WORKERS[user_id]
    
    # 3. Отключение клиента Telethon
    if user_id in ACTIVE_TELETHON_CLIENTS:
        client = ACTIVE_TELETHON_CLIENTS[user_id]
        if force_disconnect and client.is_connected():
            try:
                # Disconnect Client
                await client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting client {user_id}: {e}")
        del ACTIVE_TELETHON_CLIENTS[user_id]
            
    # 4. Очистка прогресса
    if user_id in PROCESS_PROGRESS:
        del PROCESS_PROGRESS[user_id]
        
    db_set_session_status(user_id, False) # Сессия не активна
    logger.info(f"Worker {user_id} stopped.")

async def start_workers():
    """Запускает worker'ы для всех пользователей, у которых активна сессия в БД."""
    users = db_get_active_telethon_users()
    for uid in users:
        # Создаем задачу для запуска Worker'а
        task = asyncio.create_task(run_worker(uid))
        ACTIVE_TELETHON_WORKERS[uid] = task

# 💡 ИСПРАВЛЕНО: run_worker теперь содержит всю логику Telethon и использует client.run_until_disconnected()
async def run_worker(user_id):
    await stop_worker(user_id, force_disconnect=True)
    path = get_session_path(user_id)
    client = TelegramClient(path, API_ID, API_HASH)
    ACTIVE_TELETHON_CLIENTS[user_id] = client
    
    # Уведомление для Worker'а о запуске
    try:
        await bot.send_message(user_id, "ℹ️ Попытка запуска Worker'а...")
    except Exception:
        pass # Игнорируем, если бот не может отправить сообщение

    try:
        # --- ПРОВЕРКА СЕССИИ ---
        if not os.path.exists(path + '.session'):
            db_set_session_status(user_id, False)
            await bot.send_message(user_id, "⚠️ Файл сессии не найден. Требуется повторная авторизация.", reply_markup=get_main_kb(user_id))
            return

        # 1. Подключение к API
        await client.start()
        db_set_session_status(user_id, True)
        logger.info(f"Worker {user_id} started successfully.")
        await bot.send_message(user_id, "✅ Worker **успешно запущен!** Теперь можете использовать команды (`.лс`, `.флуд`).", reply_markup=get_main_kb(user_id))


        # --- ЛОГИКА АСИНХРОННЫХ ЗАДАЧ (ФЛУД) ---
        async def flood_task(peer, message, count, delay, chat_id):
            # (Логика flood_task остается без изменений, за исключением того, что client здесь неявно доступен через замыкание)
            try:
                is_unlimited = count <= 0
                max_iterations = count if not is_unlimited else 999999999 
                
                PROCESS_PROGRESS[user_id] = {'type': 'flood', 'total': count, 'done': 0, 'peer': peer, 'chat_id': chat_id}
                
                for i in range(max_iterations):
                    # Проверка на отмену
                    if user_id not in FLOOD_TASKS or chat_id not in FLOOD_TASKS[user_id]:
                        await client.send_message(user_id, f"🛑 Флуд в чате `{get_display_name(peer)}` остановлен по команде .стопфлуд.")
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
                await client.send_message(user_id, f"❌ Ошибка при флуде: {e}")
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

        # --- Парсинг .ЧЕКГРУППУ ---
        async def check_group_task(event, target_chat_str, min_id, max_id):
            # (Логика check_group_task остается без изменений, за исключением того, что client здесь неявно доступен через замыкание)
             chat_id = event.chat_id
             if chat_id is None and not target_chat_str:
                  return await client.send_message(user_id, "❌ `.чекгруппу` должен быть вызван из группы/канала или с указанием его юзернейма/ID.")
                  
             try:
                 # Поиск сущности чата
                 if target_chat_str:
                    chat_entity = await client.get_entity(target_chat_str)
                 elif chat_id is not None:
                     chat_entity = await client.get_entity(chat_id)
                 else:
                     return # Должно быть отловлено в начале
                     
                 unique_users = {} 
                 limit = 1000000 
                 
                 await client.send_message(user_id, f"⏳ Начинаю сканирование сообщений в чате `{get_display_name(chat_entity)}`. Это может занять время.")
                 
                 # Сброс прогресса, если был другой процесс
                 if user_id in PROCESS_PROGRESS:
                     del PROCESS_PROGRESS[user_id]
                 PROCESS_PROGRESS[user_id] = {'type': 'checkgroup', 'peer': chat_entity, 'done_msg': 0}
                 
                 # 💡 Итерация по сообщениям
                 async for message in client.iter_messages(chat_entity, limit=limit):
                     
                     if user_id not in PROCESS_PROGRESS or PROCESS_PROGRESS[user_id].get('type') != 'checkgroup':
                          return # Отменено
                          
                     PROCESS_PROGRESS[user_id]['done_msg'] += 1
                     
                     if message.sender and isinstance(message.sender, User) and message.sender_id not in unique_users:
                         user_id_int = message.sender.id
                         
                         if (min_id is None or user_id_int >= min_id) and \
                            (max_id is None or user_id_int <= max_id):
                             
                              unique_users[user_id_int] = message.sender
                         
                 # --- Формирование финального отчета ---
                 total_found = len(unique_users)
                 if total_found > 0:
                     report_data_raw = []
                     range_info = f" (Фильтр ID: {min_id or 'Все'}-{max_id or 'Все'})" if min_id is not None or max_id is not None else ""
                     
                     for uid, p in unique_users.items():
                         full_name = ' '.join(filter(None, [p.first_name, p.last_name]))
                         report_data_raw.append(
                              f"👤 Имя: {full_name if full_name else 'Нет имени'}\n"
                              f"🔗 Юзернейм: @{p.username}" if p.username else f"🔗 Юзернейм: Нет\n"
                              f"🆔 ID: {uid}"
                          )
                         
                     header_text = (
                         f"📊 Отчет .ЧЕКГРУППУ (по истории сообщений) {range_info}\n"
                         f"Чат: {get_display_name(chat_entity)}\n"
                         f" • Просканировано сообщений: {PROCESS_PROGRESS[user_id]['done_msg']}\n"
                         f" • Найдено уникальных пользователей: {total_found}\n"
                         f"\nСписок пользователей (Имя, Юзернейм, ID):"
                     )
                     
                     full_report_text = header_text + "\n" + "\n".join(report_data_raw)
                     
                     PROCESS_PROGRESS[user_id]['report_data'] = full_report_text
                     PROCESS_PROGRESS[user_id]['peer_name'] = get_display_name(chat_entity)

                     await client.send_message(
                         user_id, 
                         f"✅ **Сбор данных завершен!** Найдено **{total_found}** пользователей.\n"
                         f"Выберите, как получить отчет по чату `{get_display_name(chat_entity)}`:",
                         buttons=[
                             [Button.inline("📄 Отправить файлом (.txt)", data="send_checkgroup_file_worker")],
                             [Button.inline("💬 Отправить сообщениями (по частям)", data="send_checkgroup_messages_worker")],
                             [Button.inline("❌ Удалить отчет", data="delete_checkgroup_report_worker")]
                         ]
                     )
                 else:
                     response = "✅ **Отчет .ЧЕКГРУППУ:**\nПо указанным критериям пользователи не найдены в истории сообщений."
                     await client.send_message(user_id, response)
                     
             except RpcCallFailError as e:
                 await client.send_message(user_id, f"❌ Ошибка RPC при .чекгруппу (чат недоступен): {type(e).__name__}")
             except Exception as e:
                 await client.send_message(user_id, f"❌ Критическая ошибка при .чекгруппу: {type(e).__name__} - {e}")
                 
             finally:
                 if user_id in PROCESS_PROGRESS and 'report_data' not in PROCESS_PROGRESS[user_id]:
                     del PROCESS_PROGRESS[user_id]
                 try:
                     await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
                 except:
                     pass

        
        # --- ХЭНДЛЕРЫ КОМАНД WORKER'А (.лс, .флуд и т.д.) ---
        @client.on(events.NewMessage(outgoing=True))
        async def handler(event):
            # Проверка подписки должна быть здесь
            if not db_check_subscription(user_id) and user_id != ADMIN_ID: return
            
            msg = event.text.strip()
            parts = msg.split()
            if not parts: return
            cmd = parts[0].lower()
            current_chat_id = event.chat_id

            # .ЛС
            if cmd == '.лс':
                 try:
                    # 💡 Улучшенный парсинг: ожидаем, что адресаты будут отдельными строками
                    lines = event.text.split('\n')
                    if len(lines) < 2:
                        return await event.reply("❌ Неверный формат .лс. Используйте:\n`.лс [текст сообщения]`\n`[@адресат1]`\n`[ID2]`\n\n**Адресаты должны быть с новой строки!**")

                    message_line = lines[0].strip()
                    text = message_line[len(cmd):].strip() # Текст - это всё после .лс в первой строке
                    recipients = [line.strip() for line in lines[1:] if line.strip()] # Адресаты - в остальных строках
                    
                    if not text or not recipients:
                        return await event.reply("❌ Неверный формат. Убедитесь, что указаны и текст, и хотя бы один адресат с новой строки.")
                    
                    results = []
                    for target in recipients:
                        try:
                            # Проверка формата
                            if not (target.startswith('@') or target.isdigit() or re.match(r'^-?\d+$', target)):
                                results.append(f"❌ {target}: Пропущен (Не похож на @юзернейм или ID)")
                                continue
                                
                            await client.send_message(target, text) 
                            results.append(f"✅ {target}: Отправлено")
                        except ValueError: 
                            results.append(f"❌ {target}: Ошибка (Некорректный ID/Юзернейм)")
                        except Exception as e:
                            results.append(f"❌ {target}: Ошибка ({type(e).__name__})")
                            
                    await event.reply("<b>Результаты .лс:</b>\n" + "\n".join(results), parse_mode='HTML')
                    
                 except Exception as e:
                    await event.reply(f"❌ Критическая ошибка .лс: {type(e).__name__}. Проверьте формат.")

            # .ТХТ (или .ТАБЛИЦА)
            elif cmd in ('.тхт', '.таблица'):
                
                if not event.is_reply:
                    return await event.reply("❌ Используйте `.тхт` или `.таблица` **ответом** на сообщение с текстовым файлом.")

                reply_msg = await event.get_reply_message()
                
                if not reply_msg or not reply_msg.document:
                    return await event.reply("❌ В сообщении, на которое вы отвечаете, нет документа.")
                
                mime_type = reply_msg.document.mime_type
                filename = getattr(reply_msg.document.attributes[0], 'file_name', 'файл') if reply_msg.document.attributes else 'файл'
                
                if not mime_type or not ('text' in mime_type or filename.endswith(('.txt', '.log', '.csv', '.ini', '.cfg'))):
                     return await event.reply(f"❌ Ожидается текстовый файл. Обнаружено: `{mime_type}`.")
                
                try:
                    await event.reply("⏳ Начинаю скачивание и обработку файла...")
                    
                    # 💡 Используем io.BytesIO для скачивания в память, это безопаснее
                    with io.BytesIO() as buffer:
                        await client.download_media(reply_msg, buffer)
                        buffer.seek(0)
                        file_content = buffer.read().decode('utf-8', errors='ignore')
                    
                    formatted_content = f"📖 **Содержимое файла** (`{filename}`):\n"
                    # Оборачиваем в <pre> для сохранения форматирования (столбцов)
                    formatted_content += "<pre>" + file_content + "</pre>"
                    
                    # Отправляем длинное сообщение через отдельную функцию
                    await send_long_message(client, user_id, formatted_content, parse_mode='HTML')
                    
                except Exception as e:
                    await event.reply(f"❌ Ошибка при обработке файла: {type(e).__name__} - {e}")

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
                        # Дополнительный вызов для получения ID, т.к. get_input_entity возвращает InputPeer
                        flood_chat_id = (await client.get_entity(target_chat_str)).id

                    if delay < 0.5:
                        return await event.reply("❌ Макс. кол-во: **безлимитно** (или 0). Мин. задержка: 0.5 сек.")
                    
                    if not message:
                         return await event.reply("❌ Сообщение для флуда не может быть пустым.")
                         
                    # Запуск задачи флуда
                    task = asyncio.create_task(flood_task(peer, message, count, delay, flood_chat_id))
                    if user_id not in FLOOD_TASKS:
                        FLOOD_TASKS[user_id] = {}
                        
                    FLOOD_TASKS[user_id][flood_chat_id] = task
                    
                    await event.reply(
                        f"🔥 **Флуд запущен!**\n"
                        f"Чат: `{get_display_name(peer)}`\n"
                        f"Сообщений: {'Безлимитно' if count <= 0 else count}\n"
                        f"Задержка: {delay} сек.", 
                        parse_mode='HTML'
                    )
                    
                    try:
                        await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))
                    except:
                        pass
                        
                except ValueError:
                    await event.reply("❌ Неверный формат чисел (кол-во/задержка).")
                except (UsernameInvalidError, PeerIdInvalidError, Exception) as e:
                    await event.reply(f"❌ Ошибка при подготовке флуда: Чат не найден или неверный формат. ({type(e).__name__})")

            # .СТОПФЛУД 
            elif cmd == '.стопфлуд':
                if user_id in FLOOD_TASKS and current_chat_id in FLOOD_TASKS[user_id]:
                    # Отменяем задачу. Логика очистки в flood_task.
                    task_to_cancel = FLOOD_TASKS[user_id][current_chat_id] 
                    if task_to_cancel and not task_to_cancel.done():
                        task_to_cancel.cancel()
                        await event.reply("🛑 Флуд **в этом чате** остановлен.")
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
                        
                        # 💡 Улучшение: Получаем entity прямо здесь для отображения имени
                        try:
                             peer_entity = await client.get_entity(progress['peer'])
                             peer_name = get_display_name(peer_entity)
                        except:
                             peer_name = "Неизвестно"
                        
                        status_text = (
                            f"⚡️ **СТАТУС ФЛУДА:**\n"
                            f" • Цель: `{peer_name}`\n"
                            f" • Отправлено: **{done}**\n"
                            f" • Всего: **{'Безлимитно' if total <= 0 else total}**\n"
                            # ИСПРАВЛЕНО: форматирование процента
                            f" • Прогресс: **{'{:.2f}'.format(done/total*100) + '%' if total > 0 else '—'}**"
                        )
                    elif p_type == 'checkgroup':
                        peer_name = progress.get('peer_name', 'Неизвестно')
                        done_msg = progress['done_msg']
                        status_text = (
                            f"🔎 **СТАТУС АНАЛИЗА ЧАТА:**\n"
                            f" • Цель: `{peer_name}`\n"
                            f" • Просканировано сообщений: **{done_msg}**\n"
                            f" • Статус: **{'Сбор данных...' if 'report_data' not in progress else 'Сбор завершен! Ожидание выбора формата.'}**"
                        )
                    else:
                        status_text = f"⚙️ Активный процесс: {p_type}. Данные: {progress}"
                else:
                    status_text = "✨ Активных процессов Worker'а нет."
                    
                await event.reply(status_text, parse_mode='HTML')


            # .ЧЕКГРУППУ 
            elif cmd == '.чекгруппу':
                if user_id in PROCESS_PROGRESS:
                    return await event.reply("⚠️ **Внимание:** В данный момент уже выполняется задача (`.статус`). Дождитесь завершения или отмените её.")
                    
                target_chat_str = None
                min_id, max_id = None, None
                
                # Парсинг аргументов: .чекгруппу [чат] [мин_id] [макс_id]
                if len(parts) >= 2:
                    if parts[1].startswith('@') or re.match(r'^-?\d+$', parts[1]):
                        target_chat_str = parts[1]
                        
                    if len(parts) >= 3 and parts[2].isdigit():
                        min_id = int(parts[2])
                    
                    if len(parts) >= 4 and parts[3].isdigit():
                        max_id = int(parts[3])
                
                # Если чат не указан, берем текущий чат
                if not target_chat_str and current_chat_id:
                     target_chat_str = current_chat_id # Используем ID
                elif not target_chat_str:
                     return await event.reply("❌ Неверный формат. Используйте: `.чекгруппу [@чат/ID] [мин_ID, необязательно] [макс_ID, необязательно]`")


                # Запуск задачи парсинга
                asyncio.create_task(check_group_task(event, target_chat_str, min_id, max_id))
                await event.reply(f"⏳ **Задача .ЧЕКГРУППУ запущена!** Используйте `.статус` для отслеживания.")

        # 2. Удержание клиента в работе, пока не будет отменен
        await client.run_until_disconnected()

    except FloodWaitError as e:
        logger.error(f"FloodWaitError for {user_id}: {e}")
        await bot.send_message(user_id, f"❌ **Ограничение Telegram:** Worker получил Flood Wait. Повторите попытку через **{e.seconds}** секунд.", reply_markup=get_main_kb(user_id))
    except (AuthKeyUnregisteredError, SessionPasswordNeededError) as e:
        logger.error(f"AuthError for {user_id}: {e}")
        await bot.send_message(user_id, "❌ **Ошибка авторизации:** Сессия недействительна. Требуется повторная авторизация.", reply_markup=get_main_kb(user_id))
    except asyncio.CancelledError:
        logger.info(f"Worker {user_id} task was manually cancelled.")
    except Exception as e:
        logger.error(f"Unexpected error in run_worker {user_id}: {e}")
        await bot.send_message(user_id, f"❌ **Критическая ошибка Worker'а:** {type(e).__name__} - {e}.", reply_markup=get_main_kb(user_id))
    finally:
        # Убедимся, что все очищено и статус в БД сброшен
        await stop_worker(user_id, force_disconnect=False)


# =========================================================================
# VI. AIOGRAM ХЭНДЛЕРЫ
# =========================================================================

# --- ГЛОБАЛЬНАЯ ОТМЕНА ДЕЙСТВИЯ FSM ---
@user_router.callback_query(F.data == "cancel_action", StateFilter('*')) 
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("Действие отменено.")
        
    user_id = callback.from_user.id
    
    # 1. Очистка временного клиента Telethon (для авторизации)
    client_to_disconnect = TEMP_AUTH_CLIENTS.pop(user_id, None) 
    if client_to_disconnect:
        try:
            if client_to_disconnect.is_connected():
                await client_to_disconnect.disconnect()
        except:
            pass
        
    # 2. Удаление временного файла сессии
    temp_path = get_session_path(user_id, is_temp=True) + '.session'
    if os.path.exists(temp_path):
        os.remove(temp_path)
        
    # 3. Очистка FSM
    await state.clear()
    
    # 4. Возвращение в главное меню
    try:
        await callback.message.edit_text("🏠 Главное меню:", reply_markup=get_main_kb(user_id))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
             # Ошибка возникает, если пользователь уже в главном меню
            await callback.message.delete()
            await bot.send_message(user_id, "🏠 Главное меню:", reply_markup=get_main_kb(user_id))
        else:
            raise e


# --- ОБЩИЙ ХЭНДЛЕР /start И back_to_main ---
@user_router.message(Command('start'))
@user_router.callback_query(F.data == "back_to_main")
async def command_start_handler(call: Union[Message, types.CallbackQuery]):
    if isinstance(call, types.CallbackQuery):
        message = call.message
        user_id = call.from_user.id
        await call.answer()
    else:
        message = call
        user_id = call.from_user.id

    await bot.delete_message(chat_id=user_id, message_id=message.message_id)

    # 1. Проверка доступа (подписка + канал)
    has_access, reason = await check_access(user_id)
    
    if not has_access:
        # ⚠️ Нет доступа
        kb = get_no_access_kb(is_channel_reason="подпишитесь на наш канал" in reason)
        await bot.send_message(user_id, reason, reply_markup=kb)
        return
        
    # 2. Есть доступ - Главное меню
    kb = get_main_kb(user_id)
    
    user_info = db_get_user(user_id)
    sub_end_date_str = user_info.get('subscription_end_date')
    sub_end_date = "Нет"
    
    if sub_end_date_str:
        try:
            end_dt = TIMEZONE_MSK.localize(datetime.strptime(sub_end_date_str, '%Y-%m-%d %H:%M:%S'))
            sub_end_date = end_dt.strftime('%d.%m.%Y %H:%M:%S MSK')
        except:
            pass
            
    status_text = (
        "👋 **Добро пожаловать!**\n\n"
        f" • Статус подписки: {'✅ Активна' if db_check_subscription(user_id) else '❌ Истекла'}\n"
        f" • Окончание подписки: **{sub_end_date}**\n"
        f" • Статус Worker: **{'🟢 Запущен' if user_id in ACTIVE_TELETHON_WORKERS else '🔴 Остановлен' if user_id in ACTIVE_TELETHON_CLIENTS else '🚫 Не авторизован'}**"
    )
    
    await bot.send_message(user_id, status_text, reply_markup=kb)


# --- ХЭНДЛЕРЫ АВТОРИЗАЦИИ (TELETHON FSM) ---

# 💡 Добавьте здесь функции авторизации по QR и номеру, которые мы разрабатывали ранее.
# Они будут использовать TEMP_AUTH_CLIENTS и переходить в TelethonAuth FSM States.

async def finalize_telethon_login(user_id: int, client: TelegramClient, state: FSMContext, message_or_callback: Union[types.Message, types.CallbackQuery]):
    """Финальный этап после успешного sign_in/sign_up."""
    
    temp_path = get_session_path(user_id, is_temp=True) + '.session'
    final_path = get_session_path(user_id) + '.session'
    
    # 1. Завершение работы клиента авторизации
    client_to_disconnect = TEMP_AUTH_CLIENTS.pop(user_id, None)
    if client_to_disconnect and client_to_disconnect.is_connected():
        await client_to_disconnect.disconnect()
        
    # 2. Переименование временного файла в финальный
    try:
        if os.path.exists(temp_path):
            if os.path.exists(final_path):
                os.remove(final_path)
            os.rename(temp_path, final_path)
            
    except Exception as e:
        logger.warning(f"Error during file rename for {user_id}: {e}")
    
    # 3. Обновление статуса в БД
    db_set_session_status(user_id, True)
    await state.clear()
    
    # 4. Отправка результата
    try:
        me = await client.get_me()
        username = f"@{me.username}" if me.username else "Нет юзернейма"
        text = (
            "✅ **Авторизация успешна!**\n"
            f"Аккаунт: **{get_display_name(me)}** ({username})"
        )
    except Exception:
        text = "✅ **Авторизация успешна!**"

    
    if isinstance(message_or_callback, types.Message):
        await message_or_callback.answer(text, reply_markup=get_main_kb(user_id))
    else:
        await message_or_callback.message.edit_text(text, reply_markup=get_main_kb(user_id))

# ... (Здесь должны быть остальные хэндлеры FSM авторизации: start_telethon_auth_phone, process_phone, process_code, process_password, telethon_auth_qr_start) ...

# ⚠️ Поскольку эти FSM-функции были в предыдущем рабочем коде, я их не повторяю здесь, чтобы избежать слишком большого объема.
# Используйте их из предыдущего ответа, вставив в этот блок!

# --- ХЭНДЛЕРЫ УПРАВЛЕНИЯ WORKER'ом ---

@user_router.callback_query(F.data == "telethon_start_session")
async def start_session_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    await call.answer("Запускаю Worker...", show_alert=True)
    
    if not db_check_subscription(user_id) and user_id != ADMIN_ID:
        return await call.message.edit_text("❌ Нет доступа. Требуется активная подписка.", reply_markup=get_no_access_kb(False))
        
    if user_id in ACTIVE_TELETHON_WORKERS:
        return await call.message.edit_text("⚠️ Worker уже запущен.", reply_markup=get_main_kb(user_id))

    # Запуск worker'а
    task = asyncio.create_task(run_worker(user_id))
    ACTIVE_TELETHON_WORKERS[user_id] = task
    
    # Редактируем меню сразу
    await call.message.edit_text(call.message.text, reply_markup=get_main_kb(user_id))

@user_router.callback_query(F.data == "telethon_stop_session")
async def stop_session_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    await call.answer("Останавливаю Worker...", show_alert=True)

    if user_id not in ACTIVE_TELETHON_WORKERS:
        return await call.message.edit_text("⚠️ Worker не запущен.", reply_markup=get_main_kb(user_id))

    # Остановка worker'а
    await stop_worker(user_id)
    
    # Редактируем меню
    await call.message.edit_text(call.message.text, reply_markup=get_main_kb(user_id))

@user_router.callback_query(F.data == "telethon_logout")
async def logout_session_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    await call.answer("Удаляю сессию...", show_alert=True)
    
    # 1. Останавливаем worker, если запущен
    await stop_worker(user_id)
    
    # 2. Удаляем файл сессии
    session_path = get_session_path(user_id) + '.session'
    try:
        if os.path.exists(session_path):
            os.remove(session_path)
    except Exception as e:
        logger.error(f"Error removing session file for {user_id}: {e}")

    # 3. Обновляем статус в БД
    db_set_session_status(user_id, False)
    
    await call.message.edit_text("❌ Сессия **успешно удалена**. Требуется повторная авторизация.", reply_markup=get_main_kb(user_id))


# --- ХЭНДЛЕРЫ ОТЧЕТОВ WORKER'а (CallbackQuery) ---

@user_router.callback_query(F.data.startswith("send_checkgroup_"))
async def send_checkgroup_report_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    action = call.data.split('_')[-1] # file, messages, delete

    if user_id not in PROCESS_PROGRESS or PROCESS_PROGRESS[user_id].get('type') != 'checkgroup':
        return await call.answer("Отчет не найден или устарел.", show_alert=True)
        
    progress = PROCESS_PROGRESS[user_id]
    report_data = progress.get('report_data')
    peer_name = progress.get('peer_name', 'Report')
    
    if not report_data:
        return await call.answer("Отчет пуст или не готов.", show_alert=True)

    await call.answer("Обрабатываю запрос...")
    
    if action == 'file':
        # Отправка файлом
        try:
            filename = f"report_{peer_name}_{datetime.now(TIMEZONE_MSK).strftime('%Y%m%d_%H%M%S')}.txt"
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8') as tmp_file:
                 tmp_file.write(report_data)
                 tmp_path = tmp_file.name
            
            await call.message.answer_document(FSInputFile(tmp_path), caption=f"✅ Отчет по чату **{peer_name}**")
            os.remove(tmp_path)
            await call.message.delete()
        except Exception as e:
            await call.message.answer(f"❌ Ошибка при отправке файла: {e}")
            
    elif action == 'messages':
        # Отправка по частям (Используем Telethon-клиент для отправки, чтобы избежать конфликта с Aiogram)
        if user_id not in ACTIVE_TELETHON_CLIENTS:
             return await call.message.answer("❌ Worker не активен для отправки по частям.")
             
        client = ACTIVE_TELETHON_CLIENTS[user_id]
        await send_long_message(client, user_id, report_data, parse_mode='HTML')
        await call.message.delete()
        
    elif action == 'delete':
        await call.message.delete()
        await call.message.answer(f"🗑️ Отчет по чату **{peer_name}** удален.")
        
    del PROCESS_PROGRESS[user_id] # Очищаем прогресс после успешной отправки/удаления
    await bot.send_message(user_id, "ℹ️ Статус Worker обновлен.", reply_markup=get_main_kb(user_id))

# ... (Остальные хэндлеры для PromoStates, AdminStates, show_progress, show_help) ...

# =========================================================================
# VII. ЗАПУСК БОТА
# =========================================================================

async def main() -> None:
    db_init()
    # Запускаем worker'ы, которые были активны до перезагрузки
    await start_workers()
    
    dp.include_router(user_router)
    
    await bot.delete_webhook(drop_pending_updates=True) 
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by KeyboardInterrupt.")
    except Exception as e:
        logger.error(f"Fatal error during bot runtime: {e}")
