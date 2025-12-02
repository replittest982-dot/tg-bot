#!/usr/bin/env python3
"""
🚀 StatPro Ultimate v9.0 - DATABASE EDITION
✅ Личные настройки для каждого юзера (БД).
✅ Рабочая система подписок и промокодов (БД).
✅ Кнопка перезапуска Worker'а (Решение проблемы "Неактивен").
✅ Исправлены все импорты.
"""

import asyncio
import logging
import os
import sys
import io
import re
import uuid
import random
import aiosqlite
from typing import Dict, Optional, Union
from pathlib import Path
from datetime import datetime, timedelta

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile
)
# ФИКС ИМПОРТА
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.dispatcher.middlewares.base import BaseMiddleware

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError, PhoneNumberInvalidError, PhoneCodeInvalidError,
    PasswordHashInvalidError, FloodWaitError
)

# --- QR ---
import qrcode
from PIL import Image

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# Глобальная переменная для задачи Worker'а
WORKER_TASK: Optional[asyncio.Task] = None
WORKER_STATUS = "⚪️ Не запущен"

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    
    # ТАЙМАУТ 500 СЕКУНД
    AUTH_TIMEOUT = int(os.getenv("QR_TIMEOUT", "500"))
    
    SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "@suppor_tstatpro1bot")
    TARGET_CHANNEL_URL = os.getenv("TARGET_CHANNEL_URL", "https://t.me/STAT_PRO1")
    TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", "0"))
    
except ValueError as e:
    print(f"❌ ОШИБКА КОНФИГУРАЦИИ: {e}")
    sys.exit(1)

REQUIRED_ENVS = {"BOT_TOKEN": BOT_TOKEN, "API_ID": API_ID, "API_HASH": API_HASH}
if not all(REQUIRED_ENVS.values()):
    sys.exit(1)

BASE_DIR = Path(__file__).parent
SESSION_DIR = BASE_DIR / "sessions"
SESSION_DIR.mkdir(exist_ok=True)
DB_PATH = BASE_DIR / "database.db"

def get_session_path(user_id: int) -> Path:
    return SESSION_DIR / f"session_{user_id}"

# =========================================================================
# II. ЛОГГИРОВАНИЕ И БД
# =========================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица пользователей (подписка + лимиты)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                sub_end TEXT,
                parse_limit INTEGER DEFAULT 1000
            )
        """)
        # Таблица промокодов
        await db.execute("""
            CREATE TABLE IF NOT EXISTS promos (
                code TEXT PRIMARY KEY,
                days INTEGER,
                activations INTEGER
            )
        """)
        await db.commit()

# --- ФУНКЦИИ БД ---

async def get_user_limit(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT parse_limit FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 1000

async def set_user_limit(user_id: int, limit: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, parse_limit) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET parse_limit = excluded.parse_limit
        """, (user_id, limit))
        await db.commit()

async def get_sub_date(user_id: int) -> Optional[datetime]:
    if user_id == ADMIN_ID:
        return datetime.now() + timedelta(days=3650) # Админ вечный
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                try:
                    return datetime.fromisoformat(row[0])
                except: return None
    return None

async def add_sub_days(user_id: int, days: int):
    current_end = await get_sub_date(user_id)
    now = datetime.now()
    if current_end and current_end > now:
        new_end = current_end + timedelta(days=days)
    else:
        new_end = now + timedelta(days=days)
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Сначала убедимся, что юзер есть, сохраняя его лимит
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), user_id))
        await db.commit()

async def create_promo_code_db(days: int, activations: int) -> str:
    code = f"STAT-{str(uuid.uuid4())[:6].upper()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO promos (code, days, activations) VALUES (?, ?, ?)", (code, days, activations))
        await db.commit()
    return code

async def activate_promo_db(user_id: int, code: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT days, activations FROM promos WHERE code = ?", (code,)) as cursor:
            row = await cursor.fetchone()
            if not row or row[1] <= 0:
                return False
            days = row[0]
            
        # Уменьшаем активации
        await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ?", (code,))
        await db.commit()
    
    await add_sub_days(user_id, days)
    return True

# =========================================================================
# III. MIDDLEWARE
# =========================================================================

class SubscriptionCheckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data: dict):
        user_id = event.from_user.id
        if user_id == ADMIN_ID or TARGET_CHANNEL_ID == 0:
            return await handler(event, data)

        try:
            member = await bot.get_chat_member(chat_id=TARGET_CHANNEL_ID, user_id=user_id)
            if member.status in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER]:
                return await handler(event, data)
        except Exception:
            pass 
            
        text = f"🚫 <b>Доступ закрыт!</b>\nПодпишитесь: {TARGET_CHANNEL_URL}\nЗатем жмите /start"
        if isinstance(event, Message):
            await event.answer(text)
        elif isinstance(event, CallbackQuery):
            await event.answer("🚫 Подпишитесь на канал!", show_alert=True)
        return

# =========================================================================
# IV. КЛАВИАТУРЫ
# =========================================================================

def get_main_kb(user_id: int) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="🔑 Авторизоваться", callback_data="auth_menu")],
        [InlineKeyboardButton(text="📊 Функции", callback_data="main_functions")],
        [InlineKeyboardButton(text="⭐ Подписка", callback_data="subscription_menu")],
        [InlineKeyboardButton(text="❓ Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME.replace('@', '')}")],
    ]
    if user_id == ADMIN_ID:
        kb.insert(1, [InlineKeyboardButton(text="👑 Админ Панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_auth_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 По номеру", callback_data="auth_phone"), 
         InlineKeyboardButton(text="📸 По QR-коду", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])

def get_admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промокод", callback_data="create_promo")],
        [InlineKeyboardButton(text="🔄 Перезапустить Worker", callback_data="restart_worker")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")],
    ])

def get_config_kb(current_limit: int) -> InlineKeyboardMarkup:
    ranges = {"10": 10, "50": 50, "500": 500, "5k": 5000, "МАКС": 900000}
    kb = []
    for t, v in ranges.items():
        txt = f"✅ {t}" if v == current_limit else t
        kb.append(InlineKeyboardButton(text=txt, callback_data=f"set_limit:{v}"))
    rows = [kb[i:i + 3] for i in range(0, len(kb), 3)]
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_functions")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_sub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎫 Ввести промокод", callback_data="enter_promo")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])

# =========================================================================
# V. HANDLERS
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthStates(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()
    PROMO_INPUT = State()
    ADMIN_PROMO_D = State()
    ADMIN_PROMO_A = State()

TEMP_AUTH_CLIENTS: Dict[int, TelegramClient] = {}

async def clear_temp_client(user_id: int):
    c = TEMP_AUTH_CLIENTS.pop(user_id, None)
    if c: 
        try: await c.disconnect()
        except: pass

# --- BASIC ---

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(f"👋 ID: <code>{message.from_user.id}</code>", reply_markup=get_main_kb(message.from_user.id))

@router.callback_query(F.data == "main_menu")
async def cb_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await clear_temp_client(call.from_user.id)
    await call.message.edit_text("Главное меню:", reply_markup=get_main_kb(call.from_user.id))

# --- SUBSCRIPTION ---

@router.callback_query(F.data == "subscription_menu")
async def cb_sub(call: CallbackQuery):
    end_date = await get_sub_date(call.from_user.id)
    if end_date and end_date > datetime.now():
        status = f"✅ Активна до {end_date.strftime('%d.%m.%Y')}"
    else:
        status = "❌ Не активна"
    
    await call.message.edit_text(f"⭐ <b>Подписка</b>\nСтатус: {status}", reply_markup=get_sub_kb())

@router.callback_query(F.data == "enter_promo")
async def cb_enter_promo(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("🎫 Введите промокод:")
    await state.set_state(AuthStates.PROMO_INPUT)

@router.message(AuthStates.PROMO_INPUT)
async def promo_handler(msg: Message, state: FSMContext):
    if await activate_promo_db(msg.from_user.id, msg.text.strip()):
        await msg.answer("✅ Промокод активирован! Подписка продлена.", reply_markup=get_main_kb(msg.from_user.id))
    else:
        await msg.answer("❌ Неверный код.")
    await state.clear()

# --- ADMIN ---

@router.callback_query(F.data == "admin_panel")
async def cb_admin(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text(f"👑 Админ\nWorker: {WORKER_STATUS}", reply_markup=get_admin_panel_kb())

@router.callback_query(F.data == "create_promo")
async def cb_cp(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("Дней:")
    await state.set_state(AuthStates.ADMIN_PROMO_D)

@router.message(AuthStates.ADMIN_PROMO_D)
async def admin_pd(msg: Message, state: FSMContext):
    await state.update_data(d=msg.text)
    await msg.answer("Активаций:")
    await state.set_state(AuthStates.ADMIN_PROMO_A)

@router.message(AuthStates.ADMIN_PROMO_A)
async def admin_pa(msg: Message, state: FSMContext):
    d = await state.get_data()
    code = await create_promo_code_db(int(d['d']), int(msg.text))
    await msg.answer(f"✅ Код: <code>{code}</code>", reply_markup=get_main_kb(msg.from_user.id))
    await state.clear()

@router.callback_query(F.data == "restart_worker")
async def cb_restart(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    await call.answer("🔄 Перезапуск...")
    global WORKER_TASK
    if WORKER_TASK: WORKER_TASK.cancel()
    await asyncio.sleep(2)
    WORKER_TASK = asyncio.create_task(start_telethon_worker())
    await call.message.edit_text("✅ Команда отправлена. Ждите статус 'Активен'.", reply_markup=get_admin_panel_kb())

# --- CONFIG & FUNCTIONS ---

@router.callback_query(F.data == "main_functions")
async def cb_funcs(call: CallbackQuery):
    limit = await get_user_limit(call.from_user.id)
    await call.message.edit_text(
        f"📊 <b>Настройки</b>\nЛимит парсинга: {limit}\n\n"
        f"Команды в чате:\n<code>.чекгруппу</code>\n<code>.лс текст @юзер</code>",
        reply_markup=get_config_kb(limit)
    )

@router.callback_query(F.data.startswith("set_limit:"))
async def cb_set_limit(call: CallbackQuery):
    limit = int(call.data.split(":")[1])
    await set_user_limit(call.from_user.id, limit)
    await call.answer(f"Лимит установлен: {limit}")
    await cb_funcs(call)

# --- AUTH (QR & PHONE) - Стандартная логика ---

@router.callback_query(F.data == "auth_menu")
async def cb_am(c: CallbackQuery): await c.message.edit_text("Метод:", reply_markup=get_auth_menu_kb())

@router.callback_query(F.data == "auth_qr")
async def auth_qr(call: CallbackQuery):
    uid = call.from_user.id
    await clear_temp_client(uid)
    client = TelegramClient(str(get_session_path(uid)), API_ID, API_HASH)
    TEMP_AUTH_CLIENTS[uid] = client
    try:
        await client.connect()
        qr = await client.qr_login()
        img = qrcode.make(qr.url).convert("RGB")
        bio = io.BytesIO()
        img.save(bio, format='PNG')
        bio.seek(0)
        sent = await call.message.answer_photo(BufferedInputFile(bio.read(), filename="qr.png"), caption=f"📸 Сканируйте! (500с)")
        await call.message.delete()
        await asyncio.wait_for(qr.wait(), timeout=AUTH_TIMEOUT)
        me = await client.get_me()
        await sent.edit_caption(caption=f"✅ Вход: @{me.username}", reply_markup=get_main_kb(uid))
    except Exception as e:
        await call.message.answer(f"❌ {e}")
    finally: await clear_temp_client(uid)

@router.callback_query(F.data == "auth_phone")
async def auth_ph(c: CallbackQuery, s: FSMContext):
    await c.message.edit_text("📱 Номер:")
    await s.set_state(AuthStates.PHONE)

@router.message(AuthStates.PHONE)
async def ph_in(m: Message, s: FSMContext):
    uid = m.from_user.id
    ph = m.text.strip()
    await clear_temp_client(uid)
    cl = TelegramClient(str(get_session_path(uid)), API_ID, API_HASH)
    TEMP_AUTH_CLIENTS[uid] = cl
    try:
        await cl.connect()
        res = await cl.send_code_request(ph)
        await s.update_data(ph=ph, h=res.phone_code_hash)
        await s.set_state(AuthStates.CODE)
        await m.answer("📩 Код:")
    except Exception as e: await m.answer(f"❌ {e}")

@router.message(AuthStates.CODE)
async def co_in(m: Message, s: FSMContext):
    d = await s.get_data()
    uid = m.from_user.id
    cl = TEMP_AUTH_CLIENTS.get(uid)
    if not cl: return await m.answer("Сбой.")
    try:
        await cl.sign_in(phone=d['ph'], code=m.text, phone_code_hash=d['h'])
        me = await cl.get_me()
        await m.answer(f"✅ Вход: @{me.username}", reply_markup=get_main_kb(uid))
        await clear_temp_client(uid)
        await s.clear()
    except SessionPasswordNeededError:
        await m.answer("🔒 Пароль:")
        await s.set_state(AuthStates.PASSWORD)
    except Exception as e: await m.answer(f"❌ {e}")

@router.message(AuthStates.PASSWORD)
async def pa_in(m: Message, s: FSMContext):
    uid = m.from_user.id
    cl = TEMP_AUTH_CLIENTS.get(uid)
    try:
        await cl.sign_in(password=m.text)
        await m.answer("✅ Вход (2FA)", reply_markup=get_main_kb(uid))
    except Exception as e: await m.answer(f"❌ {e}")
    finally:
        await clear_temp_client(uid)
        await s.clear()

# =========================================================================
# VI. WORKER
# =========================================================================

async def start_telethon_worker():
    global WORKER_STATUS
    sess = get_session_path(ADMIN_ID)
    if not sess.exists():
        WORKER_STATUS = "🔴 Нет сессии"
        return

    client = TelegramClient(str(sess), API_ID, API_HASH)
    
    @client.on(events.NewMessage(pattern=r'^\.чекгруппу$'))
    async def h_chk(ev):
        # Берем лимит ИЗ БАЗЫ ДАННЫХ для админа (владельца воркера)
        lim = await get_user_limit(ADMIN_ID)
        m = await ev.reply(f"🔍 Парсинг (Лимит БД: {lim})...")
        lines = []
        try:
            async for u in client.iter_participants(ev.chat_id, limit=lim, aggressive=True):
                lines.append(f"@{u.username or 'None'} | {u.first_name} | {u.id}")
                if len(lines) % 200 == 0: await m.edit(f"🔍 {len(lines)}...")
        except Exception as e: return await m.edit(f"❌ {e}")
            
        fn = f"u_{ev.chat_id}.txt"
        with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(lines))
        await client.send_file(ev.chat_id, fn, caption=f"✅ Готово: {len(lines)}")
        os.remove(fn)

    @client.on(events.NewMessage(pattern=r'^\.лс (.*?)(?: @(\S+))?$'))
    async def h_dm(ev):
        match = re.match(r'^\.лс (.*?)(?: @(\S+))?$', ev.text, re.DOTALL)
        if not match: return await ev.reply("❌ .лс текст @юзер")
        txt, usrs = match.group(1), match.group(2).split() if match.group(2) else []
        await ev.reply(f"🚀 {len(usrs)} юзерам...")
        for u in usrs:
            try:
                await client.send_message(u.lstrip('@'), txt)
                await asyncio.sleep(random.uniform(2, 5))
            except: pass
        await ev.reply("✅ Готово")

    await client.start()
    WORKER_STATUS = "🟢 Активен"
    logger.info("Worker ON")
    await client.run_until_disconnected()

# =========================================================================
# VII. MAIN
# =========================================================================

async def main():
    global WORKER_TASK
    await init_db()
    dp.message.middleware(SubscriptionCheckMiddleware())
    dp.callback_query.middleware(SubscriptionCheckMiddleware())
    
    WORKER_TASK = asyncio.create_task(start_telethon_worker())
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        if WORKER_TASK: WORKER_TASK.cancel()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
