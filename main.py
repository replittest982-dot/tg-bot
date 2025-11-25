import asyncio
import logging
import os
import sqlite3
import pytz
import re
import random
import string
from datetime import datetime, timedelta

# Импорты aiogram
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, StateFilter # ✅ ИСПРАВЛЕНИЕ: StateFilter добавлен
from aiogram.client.default import DefaultBotProperties

# Импорты telethon
from telethon import TelegramClient, events
from telethon.errors import (
    UserDeactivatedError, FloodWaitError, SessionPasswordNeededError, 
    PhoneNumberInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError, 
    AuthKeyUnregisteredError, PasswordHashInvalidError, ChannelPrivateError, 
    UsernameInvalidError, PeerIdInvalidError, ChatAdminRequiredError
)
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.utils import get_display_name

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# !!! ЗАМЕНИТЕ ЭТИ ЗНАЧЕНИЯ НА ВАШИ РЕАЛЬНЫЕ !!!
BOT_TOKEN = "7868097991:AAEuHy_DYjEkBTK-H-U1P4-wZSdSw7evzEQ" 
ADMIN_ID = 6256576302  
API_ID = 35775411
API_HASH = "4f8220840326cb5f74e1771c0c4248f2"
TARGET_CHANNEL_URL = "@STAT_PRO1" # Обязательный канал для подписки
DB_NAME = 'bot_database.db'
TIMEZONE_MSK = pytz.timezone('Europe/Moscow')
DB_TIMEOUT = 10 # Таймаут для подключения к SQLite

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальные хранилища
ACTIVE_TELETHON_CLIENTS = {} 
ACTIVE_TELETHON_WORKERS = {} 
TEMP_AUTH_CLIENTS = {} 
FLOOD_TASKS = {} 

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
    PASSWORD = State() # Шаг для 2FA-пароля

class PromoStates(StatesGroup):
    waiting_for_code = State()

class AdminStates(StatesGroup):
    main_menu = State()
    promo_code_input = State()
    promo_days_input = State()
    promo_uses_input = State()
    sub_user_id_input = State()
    sub_days_input = State()

# =========================================================================
# III. БАЗА ДАННЫХ (без изменений)
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
        return False, f"❌ Для доступа к функциям подпишитесь на наш канал: {TARGET_CHANNEL_URL}"

    if db_check_subscription(user_id): 
        return True, ""
    
    return False, "❌ Ваша подписка истекла. Активируйте промокод."

def get_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]])

# --- НОВАЯ КЛАВИАТУРА ДЛЯ ВВОДА КОДА ---
def get_code_kb(current_code_text=""):
    kb = []
    # Отображаем текущий введенный код (если есть)
    kb.append([InlineKeyboardButton(text=f"Код: {current_code_text if current_code_text else '...'} / Длина: {len(current_code_text)}", callback_data="ignore")])
    
    # Цифры 1-9
    row1 = [InlineKeyboardButton(text=f"{i}️⃣", callback_data=f"code_input_{i}") for i in range(1, 4)]
    row2 = [InlineKeyboardButton(text=f"{i}️⃣", callback_data=f"code_input_{i}") for i in range(4, 7)]
    row3 = [InlineKeyboardButton(text=f"{i}️⃣", callback_data=f"code_input_{i}") for i in range(7, 10)]
    kb.extend([row1, row2, row3])
    
    # Удалить, 0, Отправить
    row4 = [
        InlineKeyboardButton(text="⬅️ Удалить", callback_data="code_input_delete"),
        InlineKeyboardButton(text="0️⃣", callback_data="code_input_0"),
        InlineKeyboardButton(text="➡️ Отправить", callback_data="code_input_submit")
    ]
    kb.append(row4)
    
    # Отмена
    kb.append([InlineKeyboardButton(text="❌ Отмена Авторизации", callback_data="cancel_action")])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)
# ----------------------------------------

def get_main_kb(user_id):
    user = db_get_user(user_id)
    active = user.get('telethon_active')
    running = user_id in ACTIVE_TELETHON_WORKERS
    
    kb = []
    kb.append([InlineKeyboardButton(text="🔑 Активировать Промокод", callback_data="start_promo_fsm")])
    kb.append([InlineKeyboardButton(text="❓ Помощь / Команды", callback_data="show_help")]) 
    
    if not active:
        kb.append([InlineKeyboardButton(text="🔐 Авторизация (Вход)", callback_data="telethon_auth_start")])
    else:
        kb.append([InlineKeyboardButton(text="🚀 Запустить / Остановить Worker", callback_data="telethon_stop_session" if running else "telethon_start_session")])
        kb.append([InlineKeyboardButton(text="ℹ️ Статус Сессии", callback_data="telethon_check_status")])
    
    if user_id == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🛠️ Админ-Панель", callback_data="admin_panel_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_no_access_kb(is_channel_reason):
    kb = []
    if is_channel_reason:
        kb.append([InlineKeyboardButton(text="✅ Подписаться на канал", url=f"https://t.me/{TARGET_CHANNEL_URL.lstrip('@')}")])
    
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
# V. TELETHON WORKER (ОСНОВНОЕ ЯДРО - без изменений)
# =========================================================================

async def stop_worker(user_id):
    # Логика worker'а без изменений
    if user_id in FLOOD_TASKS and FLOOD_TASKS[user_id] and not FLOOD_TASKS[user_id].done():
        FLOOD_TASKS[user_id].cancel()
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
    logger.info(f"Worker {user_id} stopped.")

async def run_worker(user_id):
    # Логика worker'а без изменений
    await stop_worker(user_id)
    path = get_session_path(user_id)
    client = TelegramClient(path, API_ID, API_HASH)
    ACTIVE_TELETHON_CLIENTS[user_id] = client
    
    try:
        if not os.path.exists(path + '.session'):
            db_set_session_status(user_id, False)
            return

        await client.start()
        db_set_session_status(user_id, True)
        logger.info(f"Worker {user_id} started successfully.")

        # --- ЛОГИКА ФЛУДА И КОМАНД ---
        async def flood_task(peer, message, count, delay):
            try:
                for i in range(count):
                    if user_id in FLOOD_TASKS and FLOOD_TASKS[user_id] is False:
                        await client.send_message(user_id, "🛑 Флуд остановлен по команде .стопфлуд.")
                        break
                    await client.send_message(peer, message)
                    await asyncio.sleep(delay)
                await client.send_message(user_id, "✅ Флуд завершен.")
            except asyncio.CancelledError:
                pass
            except Exception as e:
                await client.send_message(user_id, f"❌ Ошибка при флуде: {e}")
            finally:
                if user_id in FLOOD_TASKS:
                    del FLOOD_TASKS[user_id]
        
        @client.on(events.NewMessage)
        async def handler(event):
            if not db_check_subscription(user_id) and user_id != ADMIN_ID: return
            if not event.out: return
            
            msg = event.text.strip()
            parts = msg.split()
            if not parts: return
            cmd = parts[0].lower()

            # .ЛС [текст] [список @юзернеймов/ID]
            if cmd == '.лс' and len(parts) >= 3:
                text = parts[1]
                recipients = parts[2:]
                results = []
                for target in recipients:
                    try:
                        await client.send_message(target, text)
                        results.append(f"✅ {target}: Отправлено")
                    except Exception as e:
                        results.append(f"❌ {target}: Ошибка ({type(e).__name__})")
                await event.reply("<b>Результаты .лс:</b>\n" + "\n".join(results), parse_mode='HTML')

            # .ФЛУД [кол-во] [текст] [задержка] [@чат]
            elif cmd == '.флуд' and len(parts) >= 5:
                if user_id in FLOOD_TASKS:
                    return await event.reply("⚠️ Флуд уже запущен. Используйте .стопфлуд.")
                    
                try:
                    count = int(parts[1])
                    message = parts[2]
                    delay = float(parts[3])
                    target_chat = parts[4]
                    
                    if count > 50 or delay < 0.5:
                        return await event.reply("❌ Макс. кол-во: 50. Мин. задержка: 0.5 сек.")
                    
                    peer = await client.get_input_entity(target_chat)
                    
                    task = asyncio.create_task(flood_task(peer, message, count, delay))
                    FLOOD_TASKS[user_id] = task
                    await event.reply(f"🔥 **Флуд запущен!**\nЧат: {target_chat}\nСообщений: {count}\nЗадержка: {delay} сек.", parse_mode='HTML')
                    
                except ValueError:
                    await event.reply("❌ Неверный формат чисел (кол-во/задержка).")
                except (UsernameInvalidError, PeerIdInvalidError):
                    await event.reply("❌ Чат не найден или неверный формат.")
                except Exception as e:
                    await event.reply(f"❌ Ошибка при подготовке флуда: {type(e).__name__}")
            
            # .СТОПФЛУД
            elif cmd == '.стопфлуд':
                if user_id in FLOOD_TASKS:
                    FLOOD_TASKS[user_id] = False # Флаг для остановки внутри задачи
                    await event.reply("🛑 Запрос на остановку флуда принят. Ожидайте.")
                else:
                    await event.reply("⚠️ Флуд не запущен.")
            
            # .ЧЕКГРУППУ [@чат]
            elif cmd == '.чекгруппу' and len(parts) == 2:
                target_chat_str = parts[1]
                try:
                    chat_entity = await client.get_entity(target_chat_str)
                    
                    if not isinstance(chat_entity, (Channel, Chat)):
                         return await event.reply("❌ Цель должна быть чатом или каналом.")
                         
                    participants = []
                    if isinstance(chat_entity, Channel) and chat_entity.megagroup:
                        participants = await client.get_participants(chat_entity)
                    else:
                        return await event.reply("❌ Данная команда работает только в супергруппах/каналах.")
                    
                    total_users = len(participants)
                    online_users = sum(1 for p in participants if isinstance(p.status, (types.UserStatusOnline, types.UserStatusRecently)))
                    
                    response = (
                        f"📊 **Анализ чата {get_display_name(chat_entity)}**:\n"
                        f" • Всего участников: **{total_users}**\n"
                        f" • Онлайн / Недавно: **{online_users}**\n"
                    )
                    await event.reply(response, parse_mode='HTML')

                except (UsernameInvalidError, PeerIdInvalidError):
                    await event.reply("❌ Чат не найден или неверный формат.")
                except ChatAdminRequiredError:
                    await event.reply("❌ Бот не является администратором в этом чате или не имеет прав на получение списка участников.")
                except Exception as e:
                    await event.reply(f"❌ Критическая ошибка при чеке: {type(e).__name__}")


        worker_task = asyncio.create_task(client.run_until_disconnected())
        ACTIVE_TELETHON_WORKERS[user_id] = worker_task
        await worker_task
        
    except (AuthKeyUnregisteredError, UserDeactivatedError):
        await bot.send_message(user_id, "⚠️ Сессия недействительна. Пожалуйста, авторизуйтесь заново.")
        db_set_session_status(user_id, False)
    except Exception as e:
        logger.error(f"Worker {user_id} critical error: {e}")
        await bot.send_message(user_id, f"❌ Критическая ошибка воркера: {e}")
    finally:
        if user_id in ACTIVE_TELETHON_CLIENTS: del ACTIVE_TELETHON_CLIENTS[user_id]
        if user_id in ACTIVE_TELETHON_WORKERS: del ACTIVE_TELETHON_WORKERS[user_id]
        db_set_session_status(user_id, False)

async def start_workers():
    users = db_get_active_telethon_users()
    for uid in users:
        asyncio.create_task(run_worker(uid))

# =========================================================================
# VI. ХЕНДЛЕРЫ BOT
# =========================================================================

# --- ОСНОВНОЕ МЕНЮ И СТАРТ ---
@user_router.callback_query(F.data == "back_to_main")
@user_router.message(Command("start"))
async def cmd_start(u: types.Message | types.CallbackQuery, state: FSMContext):
    user_id = u.from_user.id
    db_get_user(user_id)
    await state.clear()
    
    has_access, msg = await check_access(user_id, bot)
    
    text = f"👋 <b>Привет!</b> Ваш ID: <code>{user_id}</code>\n"
    sub = db_get_user(user_id).get('subscription_end_date')
    text += f"Подписка до: <code>{sub if sub else 'Нет'}</code>\n\n"
    
    if not has_access:
        text += f"⚠️ <b>Доступ ограничен.</b>\n{msg}"
        is_channel_reason = f"Для доступа к функциям подпишитесь на наш канал" in msg
        kb = get_no_access_kb(is_channel_reason)
    else:
        text += "✅ <b>Меню доступно.</b>\nИспользуйте кнопки ниже."
        kb = get_main_kb(user_id)

    if isinstance(u, types.Message): 
        await u.answer(text, reply_markup=kb)
    else: 
        await u.message.edit_text(text, reply_markup=kb)

# --- АВТОРИЗАЦИЯ: ВХОД В АККАУНТ ---

@user_router.callback_query(F.data == "telethon_auth_start")
async def auth_start(call: types.CallbackQuery, state: FSMContext):
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
        
        await state.update_data(phone=phone, phone_hash=result.phone_code_hash, current_code="") # Добавлено поле для сборки кода
        await state.set_state(TelethonAuth.CODE)
        
        # --- ИСПОЛЬЗУЕМ НОВУЮ КЛАВИАТУРУ ---
        await msg.answer("✉️ **Код подтверждения отправлен.**\nВведите его, используя кнопки ниже или отправьте текстом:", reply_markup=get_code_kb())

    except PhoneNumberInvalidError:
        await msg.answer("❌ Неверный формат номера телефона.", reply_markup=get_cancel_kb())
    except Exception as e:
        logger.error(f"Auth phone step error: {e}")
        await msg.answer(f"❌ Ошибка отправки кода: {e}", reply_markup=get_main_kb(uid))
        await state.clear()

# --- ХЕНДЛЕР ВВОДА КОДА (ТЕКСТОВОЕ СООБЩЕНИЕ ИЛИ КНОПКА ОТПРАВИТЬ) ---
@user_router.message(TelethonAuth.CODE)
async def auth_msg_code(msg: Message, state: FSMContext):
    # --- ОЧИСТКА ВВОДА: Удаляем все, кроме цифр ---
    code = re.sub(r'\D', '', msg.text.strip())
    # -----------------------------------------------
    
    await process_code_submit(msg, state, code)

# --- ХЕНДЛЕР НАЖАТИЯ КНОПОК НА КЛАВИАТУРЕ КОДА ---
@user_router.callback_query(F.data.startswith("code_input_"), StateFilter(TelethonAuth.CODE))
async def code_kb_handler(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    current_code = data.get('current_code', '')
    action = call.data.split('_')[-1]
    
    uid = call.from_user.id

    if action.isdigit():
        # Добавление цифры
        if len(current_code) < 10: # Ограничиваем длину
            new_code = current_code + action
            await state.update_data(current_code=new_code)
            await call.message.edit_reply_markup(reply_markup=get_code_kb(new_code))
        else:
            await call.answer("Максимальная длина кода.", show_alert=True)
            
    elif action == 'delete':
        # Удаление последней цифры
        new_code = current_code[:-1]
        await state.update_data(current_code=new_code)
        await call.message.edit_reply_markup(reply_markup=get_code_kb(new_code))
        
    elif action == 'submit':
        # Отправка собранного кода
        if len(current_code) >= 5: # Обычно коды 5-6 цифр
            await call.message.edit_text("⏳ Проверка кода...", reply_markup=None)
            await process_code_submit(call, state, current_code)
        else:
            await call.answer("Код слишком короткий.", show_alert=True)

async def process_code_submit(u: types.Message | types.CallbackQuery, state: FSMContext, code: str):
    """Общая функция для обработки кода подтверждения."""
    uid = u.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    
    if not client:
        await (u.message if isinstance(u, types.CallbackQuery) else u).answer("⚠️ Сессия авторизации истекла. Начните заново.", reply_markup=get_main_kb(uid))
        await state.clear()
        return

    if not code:
        return await (u.message if isinstance(u, types.CallbackQuery) else u).answer("❌ Код не распознан. Пожалуйста, введите только цифры.", reply_markup=get_code_kb(code))

    d = await state.get_data()
    
    # Сообщения об ожидании, если обработка идет не через колбэк
    if isinstance(u, types.Message):
         await u.answer("⏳ Проверка кода...", reply_markup=types.ReplyKeyboardRemove())

    try:
        if not client.is_connected(): await client.connect()
        await client.sign_in(d['phone'], code, phone_code_hash=d['phone_hash'])
        
        # ✅ Успех (без 2FA)
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await bot.send_message(uid, "✅ Успешно вошли! Worker запущен.", reply_markup=get_main_kb(uid))
        await state.clear()
        
    except SessionPasswordNeededError:
        # ⚠️ Требуется 2FA
        await state.set_state(TelethonAuth.PASSWORD)
        await bot.send_message(uid, "🔒 Требуется двухфакторная авторизация (2FA). Введите **пароль**:", reply_markup=get_cancel_kb())
            
    except (PhoneCodeExpiredError, PhoneCodeInvalidError) as e:
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        await bot.send_message(uid, 
            f"❌ Код недействителен или истек. Начните авторизацию сначала. "
            f"Если ошибка повторяется, <b>полностью перезапустите Python-скрипт.</b>\nОшибка: {type(e).__name__}", 
            reply_markup=get_main_kb(uid)
        )
        await state.clear()
        
    except Exception as e:
        logger.error(f"Auth code step error: {e}")
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        await bot.send_message(uid, f"❌ Неизвестная ошибка: {e}", reply_markup=get_main_kb(uid))
        await state.clear()

@user_router.message(TelethonAuth.PASSWORD)
async def auth_pwd(msg: Message, state: FSMContext):
    # --- МАСТЕР-КОД УБРАН. ОЖИДАЕМ ТОЛЬКО ПАРОЛЬ ---
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    if not client:
        await msg.answer("⚠️ Сессия истекла.", reply_markup=get_main_kb(uid))
        await state.clear()
        return
    
    sign_in_password = msg.text.strip()
    
    # --- СТАНДАРТНАЯ ЛОГИКА 2FA ПАРОЛЯ ---
    try:
        if not client.is_connected(): await client.connect()
        await client.sign_in(password=sign_in_password) 
        
        # ✅ Успех
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        
        db_set_session_status(uid, True)
        asyncio.create_task(run_worker(uid))
        await msg.answer("✅ Успешно вошли (2FA)! Worker запущен.", reply_markup=get_main_kb(uid))
        await state.clear()
        
    except PasswordHashInvalidError:
        # ❌ Неверный пароль 2FA
        await msg.answer(
            "❌ Неверный пароль 2FA. Повторите ввод:", 
            reply_markup=get_cancel_kb()
        )
    except Exception as e:
        logger.error(f"Auth password step error: {e}")
        await client.disconnect()
        if uid in TEMP_AUTH_CLIENTS: del TEMP_AUTH_CLIENTS[uid]
        await msg.answer(f"❌ Неизвестная ошибка 2FA: {e}", reply_markup=get_main_kb(uid))
        await state.clear()

# --- УПРАВЛЕНИЕ WORKER'ОМ ---
@user_router.callback_query(F.data.in_({'telethon_start_session', 'telethon_stop_session', 'telethon_check_status'}))
async def manage_worker(call: types.CallbackQuery):
    uid = call.from_user.id
    
    if call.data == 'telethon_start_session':
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
        await call.answer(f"Статус Worker'а: {status_text}", show_alert=True)

# --- АКТИВАЦИЯ ПРОМОКОДА (без изменений) ---
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
        end = db_update_subscription(msg.from_user.id, p['days'])
        
        has_access, _ = await check_access(msg.from_user.id, bot)
        
        await msg.answer(
            f"✅ Промокод <code>{code}</code> активирован!\nПодписка продлена до <b>{end}</b>. "
            f"Теперь вы можете использовать все функции." if has_access else 
            f"✅ Промокод активирован, подписка продлена до <b>{end}</b>. "
            f"Для полного доступа, пожалуйста, подпишитесь на наш канал: {TARGET_CHANNEL_URL}", 
            reply_markup=get_main_kb(msg.from_user.id)
        )
    else:
        await msg.answer("❌ Неверный, истекший код или превышен лимит использований.", 
                         reply_markup=get_main_kb(msg.from_user.id))
                         
    await state.clear()

# --- АДМИН ПАНЕЛЬ (теперь с StateFilter) ---

@user_router.callback_query(F.data == "admin_panel_start")
async def admin_panel_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return await call.answer("Недостаточно прав.")
    await state.set_state(AdminStates.main_menu)
    await call.message.edit_text("🛠️ **Админ-Панель**\nВыберите действие:", reply_markup=get_admin_kb())

@user_router.callback_query(F.data == "admin_create_promo", StateFilter(AdminStates.main_menu))
async def admin_create_promo(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.promo_code_input)
    await call.message.edit_text("📝 Введите код промокода (например, `FREEWEEK`):", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.promo_code_input)
async def admin_promo_code_input(msg: Message, state: FSMContext):
    await state.update_data(code=msg.text.strip())
    await state.set_state(AdminStates.promo_days_input)
    await msg.answer("📅 Сколько дней подписки дает промокод? (например, `7`):", reply_markup=get_cancel_kb())

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
        
        await msg.answer(
            f"✅ Промокод создан:\n"
            f"Код: <code>{data['code']}</code>\n"
            f"Дни: {data['days']}\n"
            f"Лимит: {max_uses if max_uses > 0 else 'Нет'}", 
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
        await msg.answer(f"📅 ID {target_id} принят. Сколько дней выдать?", reply_markup=get_cancel_kb())
    except ValueError:
        await msg.answer("❌ Введите корректный числовой ID.", reply_markup=get_cancel_kb())

@user_router.message(AdminStates.sub_days_input)
async def admin_sub_days_input(msg: Message, state: FSMContext):
    try:
        days = int(msg.text.strip())
        data = await state.get_data()
        target_id = data['target_id']
        
        end = db_update_subscription(target_id, days)
        
        await msg.answer(
            f"✅ Подписка выдана пользователю <code>{target_id}</code> на {days} дней.\n"
            f"Новая дата окончания: <b>{end}</b>", 
            reply_markup=get_admin_kb()
        )
        
        await bot.send_message(target_id, f"🎉 Вам выдана подписка на {days} дней до {end}!", reply_markup=get_main_kb(target_id))

    except ValueError:
        await msg.answer("❌ Введите корректное число дней.", reply_markup=get_cancel_kb())
    except TelegramForbiddenError:
        await msg.answer("⚠️ Не удалось уведомить пользователя (бот заблокирован).", reply_markup=get_admin_kb())
    finally:
        await state.set_state(AdminStates.main_menu)

# --- ПОМОЩЬ (без изменений) ---
@user_router.callback_query(F.data == "show_help")
@user_router.message(Command("help"))
async def cmd_help(u: types.Message | types.CallbackQuery):
    help_text = (
        "📚 <b>Справка и Команды (Worker):</b>\n\n"
        "Для работы инструментов сначала авторизуйтесь через **🔐 Авторизация** и запустите **Worker**.\n\n"
        "**Инструменты (вводятся в любом чате от вашего имени):**\n"
        " • <code>.лс [текст] [список @юзернеймов/ID]</code> — Отправка **личных сообщений** по списку.\n"
        " • <code>.флуд [кол-во] [текст] [задержка] [@чат]</code> — **Флуд** в указанный чат (Макс: 50, Мин. задержка: 0.5 сек).\n"
        " • <code>.стопфлуд</code> — **Остановить** запущенный флуд.\n"
        " • <code>.чекгруппу [@чат]</code> — **Анализ** общего числа участников и онлайна в супергруппе/канале."
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
