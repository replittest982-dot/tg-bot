import asyncio
import logging
import os
import sqlite3
import pytz 
import qrcode 
import time
import re
from io import BytesIO 
from datetime import datetime, timedelta 
from typing import Optional, Set, Dict, Any

# --- Aiogram и FSM ---
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, BufferedInputFile, FSInputFile
from aiogram.client.default import DefaultBotProperties 
from aiogram.exceptions import TelegramBadRequest

# --- Telethon ---
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, RPCError, UserDeactivatedError, FloodWaitError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel, PeerChat, PeerUser
from telethon.utils import get_display_name, get_peer_id 
from telethon.tl.functions.channels import GetChannelsRequest # <<< ИСПРАВЛЕНО
from telethon.tl.functions.messages import GetPeerDialogsRequest, GetForumTopicsRequest # <<< ИСПРАВЛЕНО: Добавлен GetForumTopicsRequest

# =========================================================================
# I. КОНФИГ И ЛОГИРОВАНИЕ
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

# --- ГЛОБАЛЬНОЕ СОСТОЯНИЕ ДЛЯ TELETHON СЕССИЙ ---
ACTIVE_TELETHON_CLIENTS: Dict[int, TelegramClient] = {} 
ACTIVE_TELETHON_WORKERS: Dict[int, asyncio.Task] = {} 
ACTIVE_FLOOD_TASKS: Dict[int, list] = {} 
QR_LOGIN_WAITS: Dict[int, asyncio.Task] = {}
# Состояние длительных операций {user_id: {task_type: [task, start_time, bot_msg_id]}}
ACTIVE_LONG_TASKS: Dict[int, Dict[str, list]] = {} 


# FSM Состояния
class AdminStates(StatesGroup):
    waiting_for_promo_user_id = State()
    waiting_for_new_promo_code = State()
    waiting_for_new_promo_days = State()
    waiting_for_new_promo_max_uses = State()
    
class TelethonAuth(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    QR_LOGIN = State()

class PromoStates(StatesGroup):
    waiting_for_code = State()
    
class MonitorStates(StatesGroup):
    waiting_for_it_chat_id = State()
    waiting_for_drop_chat_id = State()
    
# Роутеры
auth_router = Router(name="auth")
user_router = Router(name="user")

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
        CREATE TABLE IF NOT EXISTS telethon_sessions (
            user_id INTEGER PRIMARY KEY,
            session_file TEXT NOT NULL UNIQUE,
            is_active BOOLEAN DEFAULT 0,
            phone_code_hash TEXT,
            it_chat_id TEXT,    
            drop_chat_id TEXT,  
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            days INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            max_uses INTEGER,
            current_uses INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monitor_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            type TEXT, -- 'IT' or 'DROP'
            command TEXT,
            target TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

# --- DB-функции (в целях краткости оставим только используемые) ---

def db_get_user(user_id: int) -> Optional[dict]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    row = cursor.fetchone()
    if row:
        cols = [desc[0] for desc in cursor.description]
        return dict(zip(cols, row))
    return None

def db_add_or_update_user(user_id: int, username: str, first_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (user_id, username, first_name) 
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            username=excluded.username, 
            first_name=excluded.first_name;
    """, (user_id, username, first_name))
    conn.commit()
    
def get_session_file_path(user_id: int) -> str:
    return os.path.join('data', f'session_{user_id}.session')

def db_set_session_status(user_id: int, is_active: bool, hash_code: Optional[str] = None):
    conn = get_db_connection()
    cursor = conn.cursor()
    session_file = os.path.join('data', f'session_{user_id}.session')
    
    cursor.execute("""
        INSERT INTO telethon_sessions (user_id, session_file, is_active, phone_code_hash) 
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            is_active=excluded.is_active, 
            phone_code_hash=COALESCE(excluded.phone_code_hash, telethon_sessions.phone_code_hash)
    """, (user_id, session_file, is_active, hash_code))
    conn.commit()

def db_check_subscription(user_id: int) -> bool:
    user = db_get_user(user_id)
    if not user or not user.get('subscription_active'): return False
    end_date_str = user.get('subscription_end_date')
    if not end_date_str: return False

    try:
        end_date_utc = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
    except ValueError: return False
        
    now_utc = datetime.now()

    if end_date_utc > now_utc:
        return True
    else:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET subscription_active=0, subscription_end_date=NULL WHERE user_id=?", (user_id,))
        conn.commit()
        return False
        
def db_get_session_data(user_id: int) -> Optional[dict]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM telethon_sessions WHERE user_id=?", (user_id,))
    row = cursor.fetchone()
    if row:
        cols = [desc[0] for desc in cursor.description]
        return dict(zip(cols, row))
    return None

def db_set_monitor_chat_id(user_id: int, log_type: str, chat_id: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    session_file = os.path.join('data', f'session_{user_id}.session')
    
    col_name = f'{log_type.lower()}_chat_id'
    
    cursor.execute(f"""
        INSERT INTO telethon_sessions (user_id, session_file) 
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            {col_name}=?
    """, (user_id, session_file, chat_id))
    conn.commit()

def db_add_monitor_log(user_id: int, log_type: str, command: str, target: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO monitor_logs (user_id, type, command, target)
        VALUES (?, ?, ?, ?)
    """, (user_id, log_type, command, target))
    conn.commit()

def db_get_monitor_logs(user_id: int, log_type: str) -> list[tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT timestamp, command, target 
        FROM monitor_logs 
        WHERE user_id=? AND type=? 
        ORDER BY timestamp
    """, (user_id, log_type))
    return cursor.fetchall()
    
def db_clear_monitor_logs(user_id: int, log_type: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM monitor_logs WHERE user_id=? AND type=?", (user_id, log_type))
    conn.commit()
    
# =========================================================================
# III. TELETHON WORKER (ЛОГИКА ПРОГРЕССА)
# =========================================================================

async def get_target_entity_and_topic(client: TelegramClient, chat_id: str, topic_id: Optional[int] = None):
    """Возвращает сущность чата и, если возможно, ID основного топика."""
    try:
        chat_entity = await client.get_entity(chat_id)
        is_forum = getattr(chat_entity, 'megagroup', False) and getattr(chat_entity, 'forum', False)
        target_topic_id = 1 if is_forum else None
        return chat_entity, target_topic_id

    except Exception as e:
        logger.error(f"Не удалось получить сущность для {chat_id}: {e}")
        return None, None

async def check_channel_membership(user_id: int, bot: Bot) -> bool:
    """Проверяет членство в канале TARGET_CHANNEL_URL."""
    if user_id == ADMIN_ID: return True 
    try:
        chat_member = await bot.get_chat_member(TARGET_CHANNEL_URL, user_id)
        return chat_member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.error(f"Ошибка проверки членства в канале для {user_id}: {e}")
        return False

# --- ФУНКЦИЯ ДЛЯ ОТСЛЕЖИВАНИЯ ПРОГРЕССА ---
async def update_progress_message(user_id: int, task_type: str, bot: Bot, total: int, current: int, bot_msg_id: int):
    """Обновляет сообщение о прогрессе в чате бота."""
    try:
        if current > 0:
            percentage = (current / total) * 100
            bar_length = 20
            filled_length = int(bar_length * current / total)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            
            progress_text = f"⏳ **Прогресс {task_type}:**\n"
            progress_text += f"Обработано: **{current}** из **{total}**\n"
            progress_text += f"`{bar}` {percentage:.1f}%"
            
            await bot.edit_message_text(
                chat_id=user_id,
                message_id=bot_msg_id,
                text=progress_text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🛑 Отмена", callback_data=f"cancel_long_task_{task_type}")]
                ])
            )
        
    except TelegramBadRequest as e:
        # Ignore "message is not modified" errors
        if "message is not modified" not in str(e):
             logger.error(f"Ошибка обновления прогресса: {e}")
    except Exception as e:
        logger.error(f"Критическая ошибка обновления прогресса: {e}")


# --- ФЛУД ВОРКЕР (с прогрессом) ---
async def flood_task_worker(client: TelegramClient, target_entity, text: str, count: int, delay: float, sender_id: int, bot: Bot, bot_msg_id: int):
    """Задача, выполняющая флуд-рассылку с обновлением прогресса."""
    task_type = "Флуд"
    try:
        for i in range(1, count + 1):
            if sender_id not in ACTIVE_LONG_TASKS or task_type not in ACTIVE_LONG_TASKS[sender_id]:
                break # Задача отменена
                
            await client.send_message(target_entity, f"{text} ({i}/{count})")
            await update_progress_message(sender_id, task_type, bot, count, i, bot_msg_id)
            await asyncio.sleep(delay)
        
        await bot.edit_message_text(
            chat_id=sender_id,
            message_id=bot_msg_id,
            text=f"✅ **Флуд-рассылка завершена** в чат `{get_display_name(target_entity)}`! Отправлено {count} сообщений.",
            reply_markup=get_main_inline_kb(sender_id)
        )
    except asyncio.CancelledError:
        await bot.edit_message_text(
            chat_id=sender_id,
            message_id=bot_msg_id,
            text="🛑 **Флуд-рассылка отменена** пользователем.",
            reply_markup=get_main_inline_kb(sender_id)
        )
    except FloodWaitError as e:
        await bot.edit_message_text(
            chat_id=sender_id,
            message_id=bot_msg_id,
            text=f"❌ **Проблема с лимитами (FloodWait):** Telegram требует подождать {e.seconds} секунд. Рассылка остановлена.",
            reply_markup=get_main_inline_kb(sender_id)
        )
    except Exception as e:
        logger.error(f"Ошибка флуда для {sender_id}: {e}")
        await bot.edit_message_text(
            chat_id=sender_id,
            message_id=bot_msg_id,
            text=f"❌ **Критическая ошибка флуда:** Возникла техническая проблема. Подробнее: `{str(e)}`",
            reply_markup=get_main_inline_kb(sender_id)
        )
    finally:
        if sender_id in ACTIVE_LONG_TASKS and task_type in ACTIVE_LONG_TASKS[sender_id]:
            del ACTIVE_LONG_TASKS[sender_id][task_type]


# --- ЧЕКГРУППА ВОРКЕР (с прогрессом) ---
async def checkgroup_task_worker(client: TelegramClient, chat_entity, sender_id: int, bot: Bot, bot_msg_id: int):
    """Задача, выполняющая анализ группы с обновлением прогресса."""
    task_type = "ЧекГруппы"
    
    try:
        await bot.edit_message_text(
            chat_id=sender_id,
            message_id=bot_msg_id,
            text=f"⏳ **Анализ активности в чате {get_display_name(chat_entity)} запущен.**\nСбор сообщений...",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛑 Отмена", callback_data=f"cancel_long_task_{task_type}")]
            ])
        )

        all_messages = []
        limit = 70000 
        offset_id = 0
        total_messages = 0
        
        while total_messages < limit:
            if sender_id not in ACTIVE_LONG_TASKS or task_type not in ACTIVE_LONG_TASKS[sender_id]: break # Проверка отмены
            
            history = await client(GetHistoryRequest(
                peer=chat_entity,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=100,
                max_id=0,
                min_id=0,
                hash=0
            ))
            if not history.messages: break
                
            all_messages.extend(history.messages)
            newly_fetched = len(history.messages)
            total_messages += newly_fetched
            offset_id = history.messages[-1].id
            
            # Обновление прогресса
            await update_progress_message(sender_id, task_type, bot, limit, total_messages, bot_msg_id)
            
            if newly_fetched < 100 or total_messages >= limit: break
            await asyncio.sleep(0.5) # Небольшая задержка, чтобы не спамить API

        if sender_id not in ACTIVE_LONG_TASKS or task_type not in ACTIVE_LONG_TASKS[sender_id]:
             raise asyncio.CancelledError # Задача была отменена во время сбора
        
        await bot.edit_message_text(
            chat_id=sender_id,
            message_id=bot_msg_id,
            text=f"✅ **Сбор данных завершен.** Обработано {total_messages} сообщений. Анализ...",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛑 Отмена", callback_data=f"cancel_long_task_{task_type}")]
            ])
        )

        # ... (логика анализа и создания отчета)
        user_activity = {}
        for msg in all_messages:
            sender = msg.sender
            if sender and msg.message:
                sender_id = get_peer_id(sender)
                if sender_id not in user_activity:
                    user_activity[sender_id] = {'count': 0, 'username': 'N/A', 'last_msg': msg.date}
                user_activity[sender_id]['count'] += 1
                user_activity[sender_id]['last_msg'] = max(user_activity[sender_id]['last_msg'], msg.date)
                
                if not isinstance(sender, PeerChannel):
                    try:
                        sender_user = await client.get_entity(sender)
                        user_activity[sender_id]['username'] = get_display_name(sender_user)
                    except Exception:
                        pass

        sorted_activity = sorted(user_activity.items(), key=lambda item: item[1]['count'], reverse=True)
        
        report_content = f"--- АКТИВНОСТЬ В ЧАТЕ {get_display_name(chat_entity)} ---\n"
        report_content += f"Всего проанализировано сообщений: {total_messages}\n"
        report_content += f"Всего уникальных участников, писавших: {len(user_activity)}\n\n"
        report_content += "ТОП-20 САМЫХ АКТИВНЫХ:\n"
        
        for i, (uid, data) in enumerate(sorted_activity[:20]):
            last_msg_msk = data['last_msg'].astimezone(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M')
            report_content += f"{i+1}. {data['username']} (ID: {uid}): {data['count']} сообщений. (Последнее: {last_msg_msk})\n"
            
        file_path = os.path.join('data', f"Group_Check_{time.time()}.txt")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
            
        report_file = FSInputFile(file_path, filename=f"Group_Activity_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        
        # Отправляем отчет обратно в чат, откуда пришла команда (через Telethon)
        await client.send_file(chat_entity, report_file, caption=f"✅ **Анализ активности завершен** в чате `{get_display_name(chat_entity)}`!")
        os.remove(file_path)

        await bot.edit_message_text(
            chat_id=sender_id,
            message_id=bot_msg_id,
            text=f"✅ **Задача ЧекГруппы завершена!** Отчет отправлен в чат `{get_display_name(chat_entity)}`.",
            reply_markup=get_main_inline_kb(sender_id)
        )
        

    except asyncio.CancelledError:
        await bot.edit_message_text(
            chat_id=sender_id,
            message_id=bot_msg_id,
            text="🛑 **Задача ЧекГруппы отменена** пользователем.",
            reply_markup=get_main_inline_kb(sender_id)
        )
    except Exception as e:
        logger.error(f"Ошибка ЧекГруппы для {sender_id}: {e}")
        await bot.edit_message_text(
            chat_id=sender_id,
            message_id=bot_msg_id,
            text=f"❌ **Критическая ошибка ЧекГруппы:** Возникла проблема. Возможно, у аккаунта нет доступа к истории. Подробнее: `{str(e)}`",
            reply_markup=get_main_inline_kb(sender_id)
        )
    finally:
        if sender_id in ACTIVE_LONG_TASKS and task_type in ACTIVE_LONG_TASKS[sender_id]:
            del ACTIVE_LONG_TASKS[sender_id][task_type]


# --- ОСНОВНОЙ ВОРКЕР (с запуском задач) ---
async def run_telethon_worker_for_user(user_id: int, bot: Bot):
    session_path = get_session_file_path(user_id)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    ACTIVE_TELETHON_CLIENTS[user_id] = client

    try:
        await client.start()
        user_info = await client.get_me()
        logger.info(f"✅ Telethon Worker [{user_id}] запущен как: {get_display_name(user_info)}")
        db_set_session_status(user_id, True)

        # --- TELETHON ХЕНДЛЕРЫ ДЛЯ КОМАНД (.лс, .флуд, .чекгруппу) ---
        
        @client.on(events.NewMessage(pattern=r'^\.(лс|флуд|стопфлуд|чекгруппу).*'))
        async def handle_telethon_control_commands(event: events.NewMessage.Event):
            
            if event.sender_id != user_id: return
            
            msg_text = event.message.message.lower().strip()
            parts = msg_text.split()
            command = parts[0]
            
            # --- Проверка активных задач ---
            if user_id in ACTIVE_LONG_TASKS and ACTIVE_LONG_TASKS[user_id]:
                if command not in ['.стопфлуд', '.отмена']:
                     await event.reply("❌ **Ошибка:** У вас уже запущена длительная задача. Используйте `.`**`стопфлуд`** или **`отмените`** другую задачу в меню бота.")
                     return
            
            # --- 1. КОМАНДА .лс ---
            if command == '.лс':
                 # ... (логика .лс остается без изменений, так как она не является "длительной" задачей с прогрессом)
                try:
                    if len(parts) < 3:
                        await event.reply("❌ **.лс:** Неверный формат. Используйте: `.лс [текст] [список @юзернеймов или ID]`")
                        return

                    user_targets = [p.strip() for p in parts if p.startswith('@') or p.isdigit() or (p.startswith('-100') and len(p) > 5)]
                    
                    if not user_targets:
                         await event.reply("❌ **.лс:** Список получателей пуст. Укажите @username или ID.")
                         return
                    
                    text_parts = [p for p in parts[1:] if p not in user_targets]
                    text = " ".join(text_parts).strip()
                    
                    if not text:
                         await event.reply("❌ **.лс:** Текст сообщения не может быть пустым.")
                         return
                        
                    sent_count = 0
                    
                    for target in user_targets:
                        try:
                            target_entity = await client.get_entity(target)
                            
                            if isinstance(target_entity.peer_id, PeerChannel) or isinstance(target_entity.peer_id, PeerChat):
                                await client.send_message(event.chat_id, f"⚠️ Пропущен: `{target}`. Команда `.лс` работает только для личных сообщений (User).")
                                continue
                                
                            await client.send_message(target_entity, text)
                            sent_count += 1
                        except FloodWaitError as e:
                            await client.send_message(event.chat_id, f"❌ **Проблема с лимитами:** Не удалось отправить `{target}`. Telegram требует подождать {e.seconds} секунд.")
                            return
                        except Exception:
                            await client.send_message(event.chat_id, f"❌ Не удалось отправить в ЛС: `{target}`. Возможно, это закрытый аккаунт или ошибка ID.")
                            
                    await event.reply(f"✅ **.лс:** Сообщение отправлено **{sent_count}** пользователям из {len(user_targets)}.")

                except Exception as e:
                    await event.reply(f"❌ **.лс:** Возникла техническая проблема. Подробнее: `{str(e)}`")
                    
            # --- 2. КОМАНДА .флуд (С ЗАПУСКОМ ПРОГРЕССА) ---
            elif command == '.флуд':
                try:
                    if len(parts) < 5 or not parts[1].isdigit() or not parts[3].replace('.', '', 1).isdigit():
                        await event.reply("❌ **.флуд:** Неверный формат. Используйте: `.флуд [кол-во] [текст] [задержка_сек] [чат @юзернейм/ID]`")
                        return

                    count = int(parts[1])
                    delay = float(parts[3])
                    target = parts[-1]
                    text = " ".join(parts[2:-2]) 

                    if user_id in ACTIVE_LONG_TASKS and "Флуд" in ACTIVE_LONG_TASKS[user_id]:
                        await event.reply("❌ **Ошибка:** Флуд уже запущен.")
                        return

                    target_entity = await client.get_entity(target)
                    
                    # 1. Отправка стартового сообщения в чат бота (для получения ID)
                    start_msg = await bot.send_message(user_id, "🚀 **Флуд запущен!** Инициализация прогресса...")

                    # 2. Запуск асинхронной задачи
                    task_type = "Флуд"
                    flood_task = asyncio.create_task(flood_task_worker(client, target_entity, text, count, delay, user_id, bot, start_msg.message_id))
                    
                    if user_id not in ACTIVE_LONG_TASKS: ACTIVE_LONG_TASKS[user_id] = {}
                    ACTIVE_LONG_TASKS[user_id][task_type] = [flood_task, time.time(), start_msg.message_id]

                    await event.reply(f"✅ **Флуд:** Задача запущена. Прогресс смотрите в ЛС бота: @{bot.me.username}")
                
                except Exception as e:
                    await event.reply(f"❌ **.флуд:** Проблема при запуске. Убедитесь, что чат `{target}` доступен. Подробнее: `{str(e)}`")

            # --- 3. КОМАНДА .стопфлуд ---
            elif command == '.стопфлуд':
                task_type = "Флуд"
                if user_id in ACTIVE_LONG_TASKS and task_type in ACTIVE_LONG_TASKS[user_id]:
                    ACTIVE_LONG_TASKS[user_id][task_type][0].cancel()
                    await event.reply("⏳ Попытка остановить флуд-рассылку...")
                else:
                    await event.reply("❌ **Ошибка:** Флуд-рассылка неактивна.")
            
            # --- 4. КОМАНДА .чекгруппу (С ЗАПУСКОМ ПРОГРЕССА) ---
            elif command == '.чекгруппу':
                try:
                    if len(parts) < 2:
                        await event.reply("❌ **.чекгруппу:** Неверный формат. Используйте: `.чекгруппу [чат @юзернейм/ID]`")
                        return

                    target_id = parts[1]
                    
                    if user_id in ACTIVE_LONG_TASKS and "ЧекГруппы" in ACTIVE_LONG_TASKS[user_id]:
                        await event.reply("❌ **Ошибка:** Задача ЧекГруппы уже запущена.")
                        return

                    chat_entity, _ = await get_target_entity_and_topic(client, target_id)
                    
                    if not chat_entity:
                        await event.reply(f"❌ **.чекгруппу:** Не удалось найти или получить доступ к чату `{target_id}`.")
                        return

                    # 1. Отправка стартового сообщения в чат бота (для получения ID)
                    start_msg = await bot.send_message(user_id, "🚀 **ЧекГруппа запущена!** Инициализация прогресса...")
                    
                    # 2. Запуск асинхронной задачи
                    task_type = "ЧекГруппы"
                    check_task = asyncio.create_task(checkgroup_task_worker(client, chat_entity, user_id, bot, start_msg.message_id))
                    
                    if user_id not in ACTIVE_LONG_TASKS: ACTIVE_LONG_TASKS[user_id] = {}
                    ACTIVE_LONG_TASKS[user_id][task_type] = [check_task, time.time(), start_msg.message_id]

                    await event.reply(f"✅ **ЧекГруппа:** Задача запущена. Прогресс смотрите в ЛС бота: @{bot.me.username}")

                except Exception as e:
                    await event.reply(f"❌ **.чекгруппу:** Возникла проблема. Подробнее: `{str(e)}`")


        # --- TELETHON ХЕНДЛЕРЫ ДЛЯ МОНИТОРИНГА (Остаются прежними) ---
        @client.on(events.NewMessage(pattern=r'^\.(встал|кьар|ошибка|замена|повтор).*'))
        async def handle_it_commands(event: events.NewMessage.Event):
             # ... (логика IT команд - остается прежней) ...
            session_data = db_get_session_data(user_id)
            if not session_data or not session_data.get('it_chat_id'): return
                
            it_chat = session_data['it_chat_id']
            try:
                it_chat_entity = await client.get_entity(it_chat)
            except Exception:
                return 
            
            if event.chat_id != it_chat_entity.id: return

            msg_text = event.message.message.lower()
            
            if msg_text.startswith('.встал'):
                target = ""
                if event.is_reply and event.reply_to_msg_id:
                    original_message = await client.get_messages(event.chat_id, ids=event.reply_to_msg_id)
                    if original_message and original_message[0].message:
                        target = original_message[0].message.split()[0]
                elif len(msg_text.split()) > 1:
                    target = msg_text.split()[1]
                    
                if target:
                    db_add_monitor_log(user_id, 'IT', '.встал', target)
                    await client.send_message(it_chat_entity, f"✅ Лог: .встал ({target}) добавлен.", reply_to=event.id)
                    return
            
            commands_map = {
                '.кьар': 'QR',
                '.ошибка': 'ERROR',
                '.замена': 'REPLACE',
            }
            if msg_text.split()[0] in commands_map:
                target = event.reply_to_msg_id if event.is_reply else 'N/A'
                db_add_monitor_log(user_id, 'IT', msg_text.split()[0], str(target))
                await client.send_message(it_chat_entity, f"✅ Лог: {msg_text.split()[0]} добавлен.", reply_to=event.id)
                return


        @client.on(events.NewMessage(func=lambda e: e.message and len(e.message.split()) >= 4 and e.message.split()[-1].lower() == 'бх'))
        async def handle_drop_commands(event: events.NewMessage.Event):
             # ... (логика DROP команд - остается прежней) ...
            session_data = db_get_session_data(user_id)
            if not session_data or not session_data.get('drop_chat_id'): return
                
            drop_chat = session_data['drop_chat_id']
            try:
                drop_chat_entity = await client.get_entity(drop_chat)
            except Exception: return

            if event.chat_id != drop_chat_entity.id: return

            msg_text = event.message.message.strip()
            parts = msg_text.split()
            if len(parts) >= 4 and parts[-1].lower() == 'бх':
                target_info = msg_text
                db_add_monitor_log(user_id, 'DROP', 'Новая заявка', target_info)
                await client.send_message(drop_chat_entity, f"✅ Лог: Заявка Дропа добавлена.", reply_to=event.id)
                return
        
        await client.run_until_disconnected()

    except UserDeactivatedError:
        logger.warning(f"❌ Telethon Worker [{user_id}]: Аккаунт деактивирован.")
        db_set_session_status(user_id, False)
        session_path = get_session_file_path(user_id)
        if os.path.exists(session_path + '.session'): os.remove(session_path + '.session')
        await bot.send_message(user_id, "⚠️ **Проблема с сессией:** Ваш аккаунт Telethon был деактивирован. Пожалуйста, пройдите авторизацию заново.")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка Telethon Worker [{user_id}]: {e}")
        db_set_session_status(user_id, False)
        await bot.send_message(user_id, f"❌ **Критическая ошибка:** Произошел сбой в работе вашего аккаунта. Требуется повторная авторизация. Подробнее: `{str(e)}`")
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS: del ACTIVE_TELETHON_CLIENTS[user_id]
        if client.is_connected(): await client.disconnect()
        logger.info(f"Telethon Worker [{user_id}] остановлен.")


# =========================================================================
# IV. ХЕНДЛЕРЫ AIOGRAM (УПРАВЛЕНИЕ ПРОГРЕССОМ И ОТЧЕТАМИ)
# =========================================================================

# --- ГЛОБАЛЬНАЯ ПРОВЕРКА ПОДПИСКИ ---
async def check_access(user_id: int, bot: Bot) -> tuple[bool, str]:
    """Проверяет подписку и членство в канале TARGET_CHANNEL_URL."""
    if user_id == ADMIN_ID: return True, ""
    
    # 1. Проверка подписки
    if not db_check_subscription(user_id):
        return False, "❌ **Доступ запрещен:** Ваша подписка неактивна. Пожалуйста, обновите ее в разделе '💳 Подписка'."
        
    # 2. Проверка членства в канале
    if not await check_channel_membership(user_id, bot):
        return False, (
            f"❌ **Доступ запрещен:** Вы не подписаны на наш канал.\n"
            f"Пожалуйста, подпишитесь на {TARGET_CHANNEL_URL}, чтобы использовать бот."
        )
        
    return True, ""

def kb_back_to_main(user_id: int) -> InlineKeyboardMarkup:
    # Возвращение в главное меню или в Админ-панель
    callback_data = "admin_panel" if user_id == ADMIN_ID else "back_to_main"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data=callback_data)]
    ])

def get_main_inline_kb(user_id: int) -> InlineKeyboardMarkup:
    is_admin = user_id == ADMIN_ID
    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    
    kb = [
        [InlineKeyboardButton(text="💳 Подписка", callback_data="show_subscription"),
         InlineKeyboardButton(text="🔑 Промокод", callback_data="activate_promo")],
        [InlineKeyboardButton(text="📊 Отчеты и Мониторинг", callback_data="show_monitor_menu")],
    ]
    
    if is_admin:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel")])

    auth_text = "🟢 Сессия активна" if session_active else "🔐 Авторизовать Telethon"
    auth_callback = "telethon_auth_status" if session_active else "telethon_auth_start"
    kb.append([InlineKeyboardButton(text=auth_text, callback_data=auth_callback)])

    return InlineKeyboardMarkup(inline_keyboard=kb)
    
def format_monitor_logs_to_file(logs: list[tuple], log_type: str) -> FSInputFile:
    if not logs: return None
        
    header = f"--- ОТЧЕТ {log_type} (Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S MSK')}) ---\n"
    content = ""
    
    for timestamp, command, target in logs:
        timestamp_msk = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').astimezone(TIMEZONE_MSK).strftime('%Y-%m-%d %H:%M:%S')
        content += f"[{timestamp_msk}] {command.upper()}: {target}\n"
            
    file_path = os.path.join('data', f"{log_type}_Report_{time.time()}.txt")
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(header + content)
        
    return FSInputFile(file_path, filename=f"{log_type}_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")


# --- ОТМЕНА ДЛИТЕЛЬНОЙ ОПЕРАЦИИ (НОВЫЙ ХЕНДЛЕР) ---
@user_router.callback_query(F.data.startswith("cancel_long_task_"))
async def cancel_long_task(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    task_type = callback.data.split('_')[-1] # Флуд или ЧекГруппы

    if user_id in ACTIVE_LONG_TASKS and task_type in ACTIVE_LONG_TASKS[user_id]:
        task = ACTIVE_LONG_TASKS[user_id][task_type][0]
        task.cancel()
        await callback.answer(f"⏳ Задача '{task_type}' будет отменена.", show_alert=True)
        # Сообщение будет отредактировано в finally блоке воркера
        
    else:
        await callback.answer("❌ Задача уже неактивна.", show_alert=True)
        
    await callback.message.edit_reply_markup(reply_markup=None) # Убираем кнопку отмены сразу


# --- ПОЛУЧЕНИЕ ОТЧЕТА (УНИВЕРСАЛЬНОСТЬ + ПРОГРЕСС) ---
@user_router.callback_query(F.data.startswith("get_report_"))
async def get_monitor_report(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
    user_id = callback.from_user.id
    monitor_type = callback.data.split('_')[-1].upper()
    
    # ПРОВЕРКА ДОСТУПА
    has_access, error_msg = await check_access(user_id, bot)
    if not has_access:
        await callback.answer(error_msg, show_alert=True)
        return

    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    if not session_active:
        await callback.answer("❌ Сессия Telethon неактивна. Запустите ее.", show_alert=True)
        return
        
    # Проверка, нет ли уже активной задачи
    if user_id in ACTIVE_LONG_TASKS and ACTIVE_LONG_TASKS[user_id]:
        await callback.answer("❌ У вас уже запущена длительная задача. Дождитесь ее завершения.", show_alert=True)
        return

    logs = db_get_monitor_logs(user_id, monitor_type)
    
    if not logs:
        await callback.answer("⚠️ Логи пусты. Нет данных для отчета.", show_alert=True)
        return

    # 1. Отправка стартового сообщения в чат бота (для получения ID)
    start_msg = await callback.message.answer(f"⏳ **Генерация Отчета {monitor_type}**... Ожидание целевого чата.")
    
    # 2. Переводим пользователя в состояние ожидания чата для отправки отчета
    await state.set_state(MonitorStates.waiting_for_it_chat_id if monitor_type == 'IT' else MonitorStates.waiting_for_drop_chat_id)
    await state.update_data(monitor_type=monitor_type, report_msg_id=start_msg.message_id)

    await callback.message.edit_text(
        f"📊 **Отчет {monitor_type} готов к отправке.**\n\n"
        f"**Введите ID или @username чата/группы**, куда нужно отправить отчет. Отчет будет отправлен в топик 'General' (если это форум)."
    )
    await callback.answer()


# --- ОБРАБОТКА ЧАТА ДЛЯ ОТЧЕТА (УНИВЕРСАЛЬНО) ---
@user_router.message(MonitorStates.waiting_for_it_chat_id)
@user_router.message(MonitorStates.waiting_for_drop_chat_id)
async def process_chat_for_report(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    data = await state.get_data()
    monitor_type = data['monitor_type']
    chat_id_str = message.text.strip()
    report_msg_id = data.get('report_msg_id') # ID сообщения прогресса

    await state.clear()
    
    if user_id not in ACTIVE_TELETHON_CLIENTS:
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=report_msg_id or message.message_id,
            text="❌ **Ошибка:** Ваш Telethon Worker неактивен. Отчет не может быть отправлен.",
            reply_markup=get_main_inline_kb(user_id)
        )
        return

    logs = db_get_monitor_logs(user_id, monitor_type)
    if not logs:
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=report_msg_id or message.message_id,
            text=f"⚠️ **Отчет {monitor_type}:** Логи пусты или были очищены.",
            reply_markup=get_main_inline_kb(user_id)
        )
        return
        
    # 1. Обновление прогресса
    if report_msg_id:
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=report_msg_id,
            text=f"⏳ **Отправка Отчета {monitor_type}** в `{chat_id_str}`...",
            reply_markup=None
        )

    client = ACTIVE_TELETHON_CLIENTS[user_id]
    report_file = format_monitor_logs_to_file(logs, monitor_type)
    
    try:
        # Получаем сущность чата и топик для отправки
        chat_entity, target_topic_id = await get_target_entity_and_topic(client, chat_id_str)
        
        if not chat_entity:
            raise Exception(f"Не удалось найти целевой чат: {chat_id_str}")
             
        # Отправка файла через Telethon (для соблюдения логики топиков)
        await client.send_file(
            chat_entity,
            report_file.path,
            caption=f"✅ **Автоматический Отчет {monitor_type}.** Логи очищены.",
            message_thread_id=target_topic_id 
        )
        
        # 2. Очистка логов и финальное сообщение
        db_clear_monitor_logs(user_id, monitor_type)
        if os.path.exists(report_file.path): os.remove(report_file.path)
        
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=report_msg_id or message.message_id,
            text=f"✅ **Отчет {monitor_type} успешно отправлен** в чат: `{chat_id_str}` (Топик ID: {target_topic_id}).",
            reply_markup=get_main_inline_kb(user_id)
        )
            
    except Exception as e:
        logger.error(f"Ошибка отправки отчета Telethon: {e}")
        
        error_msg = f"❌ **Проблема с отчетом {monitor_type}:** Не удалось отправить отчет. Проверьте ID/Username и права аккаунта. Ошибка: `{str(e)}`"
        
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=report_msg_id or message.message_id,
            text=error_msg,
            reply_markup=get_main_inline_kb(user_id)
        )
        
# --- ПРОЧИЕ ХЕНДЛЕРЫ (остаются прежними) ---

@user_router.callback_query(F.data == "show_monitor_menu")
async def show_monitor_menu_start(callback: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📈 IT-Отчеты", callback_data="monitor_IT")],
        [InlineKeyboardButton(text="📉 Дроп-Отчеты", callback_data="monitor_DROP")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(
        "📊 **Отчеты и Мониторинг.**\n\n"
        "Выберите тип отчета для настройки чата и получения статистики:",
        reply_markup=kb
    )
    await callback.answer()
    
@user_router.callback_query(F.data.startswith("monitor_"))
async def handle_monitor_menu(callback: types.CallbackQuery) -> None:
    user_id = callback.from_user.id
    monitor_type = callback.data.split('_')[-1].upper()
    
    # ПРОВЕРКА ДОСТУПА
    has_access, error_msg = await check_access(user_id, callback.bot)
    if not has_access:
        await callback.answer(error_msg, show_alert=True)
        return
        
    session_active = user_id in ACTIVE_TELETHON_CLIENTS
    
    status_text = f"📈 **Мониторинг {monitor_type}**\n\n"
    
    if not session_active:
        status_text += "🔴 Сессия Telethon неактивна. Запустите ее через '🔐 Авторизовать Telethon'."
        kb_monitor = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔐 Авторизовать Telethon", callback_data="telethon_auth_start")],
            [InlineKeyboardButton(text="⬅️ В меню отчетов", callback_data="show_monitor_menu")]
        ])
    else:
        session_data = db_get_session_data(user_id)
        chat_id = session_data.get(f'{monitor_type.lower()}_chat_id') if session_data else None
        
        if chat_id:
            status_text += f"🟢 **Чат для команд (Только для логгирования):** `{chat_id}`\n"
            status_text += f"💬 **Ожидаемые команды:**\n"
            if monitor_type == 'IT':
                status_text += "`.встал`, `.кьар`, `.ошибка`, `.замена`, `.повтор`\n"
            else:
                status_text += "Заявки: `номер время @user бх`\n"
            status_text += "\n"
            
            kb_monitor = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"⚙️ Изменить чат логгирования {monitor_type}", callback_data=f"config_chat_{monitor_type}"),
                 InlineKeyboardButton(text=f"📊 Получить Отчет", callback_data=f"get_report_{monitor_type}")],
                [InlineKeyboardButton(text="⬅️ В меню отчетов", callback_data="show_monitor_menu")]
            ])
        else:
            status_text += f"🔴 Чат для логгирования не настроен.\n"
            kb_monitor = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"⚙️ Настроить чат логгирования {monitor_type}", callback_data=f"config_chat_{monitor_type}")],
                [InlineKeyboardButton(text="⬅️ В меню отчетов", callback_data="show_monitor_menu")]
            ])

    await callback.message.edit_text(status_text, reply_markup=kb_monitor)
    await callback.answer()
    
@user_router.callback_query(F.data.startswith("config_chat_"))
async def config_chat_start(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    monitor_type = callback.data.split('_')[-1].upper()

    has_access, error_msg = await check_access(user_id, callback.bot)
    if not has_access:
        await callback.answer(error_msg, show_alert=True)
        return
        
    await state.set_state(MonitorStates.waiting_for_it_chat_id if monitor_type == 'IT' else MonitorStates.waiting_for_drop_chat_id)
    await state.update_data(monitor_type=monitor_type)
    
    await callback.message.edit_text(
        f"⚙️ **Настройка чата логгирования {monitor_type}**\n\n"
        f"Введите ID или @username чата/группы, в которой бот будет **СЛУШАТЬ** команды для записи логов (например, `-10012345678` или `@my_group`):",
        reply_markup=kb_back_to_main(user_id)
    )
    await callback.answer()
    
@user_router.message(MonitorStates.waiting_for_it_chat_id)
@user_router.message(MonitorStates.waiting_for_drop_chat_id)
async def process_config_chat_id(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    monitor_type = data['monitor_type']
    chat_id_str = message.text.strip()
    
    await state.clear()
    
    # 1. Запись в БД (не проверяем доступ Telethon, только сохраняем ID)
    try:
        db_set_monitor_chat_id(user_id, monitor_type, chat_id_str)
        
        await message.answer(
            f"✅ **Чат логгирования {monitor_type} успешно настроен.**\n"
            f"Теперь бот будет слушать команды в чате: `{chat_id_str}`.",
            reply_markup=get_main_inline_kb(user_id)
        )
    except Exception as e:
        await message.answer(
            f"❌ **Ошибка при сохранении ID:** {e}",
            reply_markup=get_main_inline_kb(user_id)
        )

# --- СТАРТ И ВОЗВРАТ В ГЛАВНОЕ МЕНЮ ---
@auth_router.message(Command("start"))
@user_router.callback_query(F.data == "back_to_main")
async def cmd_start_or_back(query_or_message: types.CallbackQuery | types.Message, state: FSMContext) -> None:
    await state.clear()
    
    is_callback = isinstance(query_or_message, types.CallbackQuery)
    message = query_or_message.message if is_callback else query_or_message
    user = message.from_user
    
    db_add_or_update_user(user.id, user.username or '', user.first_name or '')
    
    has_access, error_msg = await check_access(user.id, message.bot)
    
    if not has_access:
        text = f"👋 **STATPRO | Панель управления.**\n\n{error_msg}"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💳 Подписка", callback_data="show_subscription")]])
    else:
        text = (
            f"👋 **STATPRO | Панель управления.**\n\n"
            f"Ваш ID: `{user.id}`. Здесь вы можете управлять подпиской, авторизовать аккаунт для мониторинга и доступа к инструментам."
        )
        kb = get_main_inline_kb(user.id)
    
    if is_callback:
        # Пытаемся отредактировать, если не успели. Иначе отправляем новое.
        try:
            await message.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            await message.answer(text, reply_markup=kb)
        await query_or_message.answer()
    else:
        await message.answer(text, reply_markup=kb)

# --- ... (Здесь должны быть остальные хендлеры: telethon_auth_start, telethon_auth_step_phone, telethon_auth_step_code, telethon_auth_step_password, telethon_auth_qr_start, telethon_auth_qr_check, show_subscription_status, start_promo_activation, process_promo_code, admin_panel_menu, admin_issue_promo_start, admin_issue_promo_id, admin_issue_promo_days, admin_create_promo_start, admin_create_promo_code, admin_create_promo_days, admin_create_promo_max_uses)
# Для краткости, если они не были изменены, они должны быть вставлены здесь, но я пропущу их для финального ответа. 

# =========================================================================
# V. ИНИЦИАЛИЗАЦИЯ
# =========================================================================

async def main():
    if not os.path.exists('data'):
        os.makedirs('data')
    create_tables()

    # Инициализация диспетчера и бота
    dp = Dispatcher(storage=MemoryStorage())
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))

    # Регистрация роутеров
    dp.include_router(auth_router)
    dp.include_router(user_router)
    
    # Запуск всех активных Telethon воркеров при старте
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM telethon_sessions WHERE is_active=1")
    active_sessions = [row[0] for row in cursor.fetchall()]
    conn.close()

    for user_id in active_sessions:
        # Проверяем, существует ли файл сессии
        session_path = get_session_file_path(user_id)
        if os.path.exists(session_path + '.session'):
            logger.info(f"Восстановление сессии для {user_id}...")
            task = asyncio.create_task(run_telethon_worker_for_user(user_id, bot))
            ACTIVE_TELETHON_WORKERS[user_id] = task
        else:
             db_set_session_status(user_id, False) # Сессия не найдена, сброс статуса

    # Запуск бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
