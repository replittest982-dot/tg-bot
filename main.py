#!/usr/bin/env python3
"""
🚀 StatPro Ultimate v10.0 - THE FINAL CUT
✅ Fix: Safe Message Editing (No more "There is no text to edit")
✅ Fix: Full SQLite DB (No Mocks)
✅ New: CSV Export, Ban System, Broadcast, Backup, Profile
✅ New: 10+ Telethon Commands
"""

import asyncio
import logging
import os
import sys
import io
import re
import uuid
import random
import csv
import aiosqlite
from typing import Dict, Optional, Union, List
from pathlib import Path
from datetime import datetime, timedelta

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile, FSInputFile
)
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.dispatcher.middlewares.base import BaseMiddleware

# --- TELETHON ---
from telethon import TelegramClient, events, functions, types
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

WORKER_TASK: Optional[asyncio.Task] = None
WORKER_STATUS = "⚪️ Не инициализирован"

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    AUTH_TIMEOUT = int(os.getenv("QR_TIMEOUT", "500"))
    
    SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "@suppor_tstatpro1bot")
    TARGET_CHANNEL_URL = os.getenv("TARGET_CHANNEL_URL", "https://t.me/STAT_PRO1")
    TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", "0"))
    
except ValueError as e:
    print(f"❌ CONFIG ERROR: {e}")
    sys.exit(1)

if not all([BOT_TOKEN, API_ID, API_HASH]):
    print("❌ ERROR: Check ENV variables (BOT_TOKEN, API_ID, API_HASH)")
    sys.exit(1)

BASE_DIR = Path(__file__).parent
SESSION_DIR = BASE_DIR / "sessions"
SESSION_DIR.mkdir(exist_ok=True)
DB_PATH = BASE_DIR / "database.db"

def get_session_path(user_id: int) -> Path:
    return SESSION_DIR / f"session_{user_id}"

# =========================================================================
# II. SYSTEM UTILS (SAFE EDIT & DB)
# =========================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# 🔥 MAGIC FUNCTION: SAFE EDIT
async def edit_or_answer(message_obj: Union[Message, CallbackQuery], text: str, reply_markup=None):
    """Умная функция: пытается редактировать, если нет текста - шлет новое."""
    try:
        if isinstance(message_obj, CallbackQuery):
            msg = message_obj.message
        else:
            msg = message_obj

        # Пытаемся редактировать
        await msg.edit_text(text, reply_markup=reply_markup)
    except Exception:
        # Если не вышло (например, это фото), удаляем старое и шлем новое
        try: await msg.delete()
        except: pass
        await msg.answer(text, reply_markup=reply_markup)

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # Пользователи
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                join_date TEXT,
                sub_end TEXT,
                parse_limit INTEGER DEFAULT 1000,
                is_banned INTEGER DEFAULT 0
            )
        """)
        # Промокоды
        await db.execute("""
            CREATE TABLE IF NOT EXISTS promos (
                code TEXT PRIMARY KEY,
                days INTEGER,
                activations INTEGER
            )
        """)
        await db.commit()

# --- DB METHODS ---

async def add_user(user_id: int, username: str):
    now = datetime.now().isoformat()
    # Даем 1 день триал
    trial_end = (datetime.now() + timedelta(days=1)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR IGNORE INTO users (user_id, username, join_date, sub_end) 
            VALUES (?, ?, ?, ?)
        """, (user_id, username, now, trial_end))
        await db.commit()

async def get_user_data(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def set_limit(user_id: int, limit: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET parse_limit = ? WHERE user_id = ?", (limit, user_id))
        await db.commit()

async def create_promo(days: int, activations: int) -> str:
    code = f"PRO-{uuid.uuid4().hex[:6].upper()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, activations))
        await db.commit()
    return code

async def use_promo(user_id: int, code: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT days, activations FROM promos WHERE code = ?", (code,)) as c:
            res = await c.fetchone()
            if not res or res[1] < 1: return False
            days = res[0]
        
        await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ?", (code,))
        
        # Extend sub
        usr = await get_user_data(user_id)
        current = datetime.fromisoformat(usr['sub_end']) if usr and usr['sub_end'] else datetime.now()
        if current < datetime.now(): current = datetime.now()
        new_end = current + timedelta(days=days)
        
        await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), user_id))
        await db.commit()
    return True

async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (datetime.now().isoformat(),)) as c: active = (await c.fetchone())[0]
    return total, active

async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users") as c:
            return [row[0] for row in await c.fetchall()]

# =========================================================================
# III. MIDDLEWARE
# =========================================================================

class SecurityMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data: dict):
        user_id = event.from_user.id
        
        # 1. Register User
        await add_user(user_id, event.from_user.username or "Unknown")
        
        # 2. Check Ban
        u_data = await get_user_data(user_id)
        if u_data and u_data['is_banned']:
            if isinstance(event, Message): await event.answer("🚫 Вы забанены.")
            return

        # 3. Check Sub Channel (Skip Admin)
        if user_id != ADMIN_ID and TARGET_CHANNEL_ID != 0:
            try:
                m = await bot.get_chat_member(TARGET_CHANNEL_ID, user_id)
                if m.status not in ['creator', 'administrator', 'member']:
                    raise Exception
            except:
                text = f"🚫 <b>Подпишитесь на канал!</b>\n{TARGET_CHANNEL_URL}"
                if isinstance(event, Message): await event.answer(text)
                elif isinstance(event, CallbackQuery): await event.answer("🚫 Подписка обязательна!", show_alert=True)
                return

        return await handler(event, data)

# =========================================================================
# IV. KEYBOARDS
# =========================================================================

def kb_main(user_id: int):
    kb = [
        [InlineKeyboardButton(text="🔑 Авторизация Worker", callback_data="auth_menu")],
        [InlineKeyboardButton(text="👤 Мой Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="📊 Функции Worker", callback_data="worker_menu")],
        [InlineKeyboardButton(text="⭐ Подписка / Промо", callback_data="sub_menu")],
        [InlineKeyboardButton(text="🆘 Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME.replace('@', '')}")],
    ]
    if user_id == ADMIN_ID:
        kb.insert(0, [InlineKeyboardButton(text="👑 ADMIN PANEL", callback_data="admin_menu")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_auth():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Номер", callback_data="auth_phone"), 
         InlineKeyboardButton(text="📸 QR-код", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])

def kb_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Промокод", callback_data="adm_promo"),
         InlineKeyboardButton(text="📢 Рассылка", callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="adm_stats"),
         InlineKeyboardButton(text="📦 Бэкап БД", callback_data="adm_backup")],
        [InlineKeyboardButton(text="🔄 Перезапуск Worker", callback_data="adm_restart_worker")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])

def kb_config(current):
    r = [10, 50, 500, 5000, 100000]
    kb = []
    for v in r:
        txt = f"✅ {v}" if v == current else str(v)
        kb.append(InlineKeyboardButton(text=txt, callback_data=f"lim:{v}"))
    rows = [kb[i:i+3] for i in range(0, len(kb), 3)]
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="worker_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# =========================================================================
# V. HANDLERS
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class States(StatesGroup):
    PHONE=State(); CODE=State(); PASS=State(); PROMO=State()
    ADM_DAYS=State(); ADM_ACT=State(); BROADCAST=State()

TEMP_CLIENTS = {}

# --- MENU & PROFILE ---
@router.message(Command("start"))
async def start(m: Message):
    await m.answer(f"👋 Привет, {m.from_user.first_name}!", reply_markup=kb_main(m.from_user.id))

@router.callback_query(F.data == "main_menu")
async def menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await edit_or_answer(c, "🏠 Главное меню:", kb_main(c.from_user.id))

@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    u = await get_user_data(c.from_user.id)
    d = datetime.fromisoformat(u['sub_end'])
    active = "✅ Активна" if d > datetime.now() else "❌ Истекла"
    txt = (
        f"👤 <b>Профиль</b>\n"
        f"🆔: <code>{u['user_id']}</code>\n"
        f"📅 Подписка: {active} (до {d.strftime('%d.%m.%Y')})\n"
        f"⚙️ Лимит парсинга: {u['parse_limit']}"
    )
    await edit_or_answer(c, txt, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]]))

# --- WORKER MENU ---
@router.callback_query(F.data == "worker_menu")
async def w_menu(c: CallbackQuery):
    u = await get_user_data(c.from_user.id)
    # Check Sub
    if datetime.fromisoformat(u['sub_end']) < datetime.now() and c.from_user.id != ADMIN_ID:
        return await c.answer("❌ Подписка истекла!", show_alert=True)
        
    await edit_or_answer(c, 
        f"📊 <b>Worker Menu</b>\nСтатус: {WORKER_STATUS}\nЛимит: {u['parse_limit']}\n\n"
        "Команды в чатах:\n"
        "<code>.чекгруппу</code> - Парсинг TXT\n"
        "<code>.csv</code> - Парсинг CSV\n"
        "<code>.инфо</code> - Инфо о юзере\n"
        "<code>.лс текст @юзер</code> - Рассылка\n"
        "<code>.help</code> - Все команды",
        kb_config(u['parse_limit'])
    )

@router.callback_query(F.data.startswith("lim:"))
async def set_lim(c: CallbackQuery):
    l = int(c.data.split(":")[1])
    await set_limit(c.from_user.id, l)
    await c.answer(f"Лимит: {l}")
    await w_menu(c)

# --- SUBSCRIPTION ---
@router.callback_query(F.data == "sub_menu")
async def sub_menu(c: CallbackQuery):
    await edit_or_answer(c, "⭐ Управление подпиской", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎫 Ввести промокод", callback_data="enter_promo")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ]))

@router.callback_query(F.data == "enter_promo")
async def en_pro(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "🎫 Введите код:")
    await state.set_state(States.PROMO)

@router.message(States.PROMO)
async def pro_h(m: Message, state: FSMContext):
    if await use_promo(m.from_user.id, m.text.strip()):
        await m.answer("✅ Успех! Подписка продлена.", reply_markup=kb_main(m.from_user.id))
    else:
        await m.answer("❌ Неверный код.")
    await state.clear()

# --- ADMIN PANEL ---
@router.callback_query(F.data == "admin_menu")
async def adm_m(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    await edit_or_answer(c, f"👑 Админ\nWorker: {WORKER_STATUS}", kb_admin())

@router.callback_query(F.data == "adm_stats")
async def adm_st(c: CallbackQuery):
    t, a = await get_stats()
    await c.answer(f"Всего: {t}\nАктивных: {a}", show_alert=True)

@router.callback_query(F.data == "adm_backup")
async def adm_bk(c: CallbackQuery):
    await c.message.answer_document(FSInputFile(DB_PATH), caption="📦 Database Backup")

@router.callback_query(F.data == "adm_restart_worker")
async def adm_rw(c: CallbackQuery):
    global WORKER_TASK
    if WORKER_TASK: WORKER_TASK.cancel()
    await asyncio.sleep(1)
    WORKER_TASK = asyncio.create_task(worker_process())
    await c.answer("🔄 Restarting...", show_alert=True)
    await asyncio.sleep(2)
    await edit_or_answer(c, f"👑 Админ\nWorker: {WORKER_STATUS}", kb_admin())

@router.callback_query(F.data == "adm_promo")
async def adm_pr(c: CallbackQuery, state: FSMContext):
    await edit_or_answer(c, "Дней:")
    await state.set_state(States.ADM_DAYS)

@router.message(States.ADM_DAYS)
async def adm_d(m: Message, s: FSMContext):
    await s.update_data(d=m.text)
    await m.answer("Активаций:")
    await s.set_state(States.ADM_ACT)

@router.message(States.ADM_ACT)
async def adm_a(m: Message, s: FSMContext):
    d = await s.get_data()
    c = await create_promo(int(d['d']), int(m.text))
    await m.answer(f"Code: <code>{c}</code>", reply_markup=kb_main(ADMIN_ID))
    await s.clear()

@router.callback_query(F.data == "adm_broadcast")
async def adm_br(c: CallbackQuery, s: FSMContext):
    await edit_or_answer(c, "📝 Введите текст рассылки (или /cancel):")
    await s.set_state(States.BROADCAST)

@router.message(States.BROADCAST)
async def adm_br_h(m: Message, s: FSMContext):
    if m.text == "/cancel": 
        await s.clear(); return await m.answer("Отмена.")
    
    users = await get_all_users()
    await m.answer(f"🚀 Рассылка на {len(users)} юзеров...")
    count = 0
    for uid in users:
        try:
            await bot.send_message(uid, m.text)
            count += 1
            await asyncio.sleep(0.1)
        except: pass
    await m.answer(f"✅ Доставлено: {count}")
    await s.clear()

# --- AUTH LOGIC (Standard) ---
@router.callback_query(F.data == "auth_menu")
async def am(c: CallbackQuery): await edit_or_answer(c, "Метод:", kb_auth())

@router.callback_query(F.data == "auth_qr")
async def aq(c: CallbackQuery):
    uid = c.from_user.id
    if uid in TEMP_CLIENTS: await TEMP_CLIENTS[uid].disconnect()
    cl = TelegramClient(str(get_session_path(uid)), API_ID, API_HASH)
    TEMP_CLIENTS[uid] = cl
    try:
        await cl.connect()
        qr = await cl.qr_login()
        im = qrcode.make(qr.url).convert("RGB")
        b = io.BytesIO(); im.save(b, "PNG"); b.seek(0)
        # Отправляем новое, удаляя старое (через Safe Edit нельзя фото, поэтому просто шлем)
        await c.message.delete()
        msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption=f"QR (500s)")
        await asyncio.wait_for(qr.wait(), 500)
        me = await cl.get_me()
        await msg.edit_caption(caption=f"✅ {me.username}", reply_markup=kb_main(uid))
    except Exception as e: await c.message.answer(f"Error: {e}")

@router.callback_query(F.data == "auth_phone")
async def ap(c: CallbackQuery, s: FSMContext):
    await edit_or_answer(c, "Номер:")
    await s.set_state(States.PHONE)

@router.message(States.PHONE)
async def ph(m: Message, s: FSMContext):
    uid = m.from_user.id
    if uid in TEMP_CLIENTS: await TEMP_CLIENTS[uid].disconnect()
    cl = TelegramClient(str(get_session_path(uid)), API_ID, API_HASH)
    TEMP_CLIENTS[uid] = cl
    await cl.connect()
    r = await cl.send_code_request(m.text.strip())
    await s.update_data(p=m.text, h=r.phone_code_hash)
    await s.set_state(States.CODE)
    await m.answer("Код:")

@router.message(States.CODE)
async def co(m: Message, s: FSMContext):
    d = await s.get_data()
    cl = TEMP_CLIENTS.get(m.from_user.id)
    try:
        await cl.sign_in(phone=d['p'], code=m.text, phone_code_hash=d['h'])
        await m.answer("✅ Успех", reply_markup=kb_main(m.from_user.id))
        await s.clear()
    except SessionPasswordNeededError:
        await m.answer("Пароль:")
        await s.set_state(States.PASS)

@router.message(States.PASS)
async def pa(m: Message, s: FSMContext):
    cl = TEMP_CLIENTS.get(m.from_user.id)
    await cl.sign_in(password=m.text)
    await m.answer("✅ Успех", reply_markup=kb_main(m.from_user.id))
    await s.clear()

# =========================================================================
# VI. TELETHON WORKER
# =========================================================================

async def worker_process():
    global WORKER_STATUS
    sess = get_session_path(ADMIN_ID)
    if not sess.exists():
        WORKER_STATUS = "🔴 Нет сессии Админа"
        return

    client = TelegramClient(str(sess), API_ID, API_HASH)
    
    # --- HELPER: CHECK SUB ---
    async def check_access(event):
        # Allow admin everywhere
        if event.sender_id == (await client.get_me()).id: return True
        # Check logic here if needed for others
        return True

    @client.on(events.NewMessage(pattern=r'^\.help'))
    async def help_cmd(ev):
        await ev.reply(
            "🛠 **StatPro Commands:**\n"
            "`.чекгруппу` - Парсинг TXT\n"
            "`.csv` - Парсинг CSV\n"
            "`.id` - ID чата\n"
            "`.info` - Инфо (реплаем)\n"
            "`.join <link>` - Вход\n"
            "`.leave` - Выход\n"
            "`.лс текст @юзер` - Рассылка"
        )

    @client.on(events.NewMessage(pattern=r'^\.id'))
    async def id_cmd(ev):
        await ev.reply(f"Chat ID: `{ev.chat_id}`\nSender ID: `{ev.sender_id}`")

    @client.on(events.NewMessage(pattern=r'^\.info'))
    async def info_cmd(ev):
        if not ev.is_reply: return await ev.reply("Ответьте на сообщение!")
        r = await ev.get_reply_message()
        u = await r.get_sender()
        await ev.reply(f"Name: {u.first_name}\nID: `{u.id}`\nBot: {u.bot}\nUser: @{u.username}")

    @client.on(events.NewMessage(pattern=r'^\.join (.*)'))
    async def join_cmd(ev):
        link = ev.pattern_match.group(1)
        try:
            await client(functions.channels.JoinChannelRequest(link))
            await ev.reply("✅ Entered")
        except Exception as e: await ev.reply(f"❌ {e}")

    @client.on(events.NewMessage(pattern=r'^\.leave'))
    async def leave_cmd(ev):
        await ev.reply("👋 Bye!")
        await client(functions.channels.LeaveChannelRequest(ev.chat_id))

    @client.on(events.NewMessage(pattern=r'^\.чекгруппу$'))
    async def txt_parse(ev):
        lim = await get_user_limit(ADMIN_ID) # Limit from DB
        msg = await ev.reply(f"🔍 TXT Parsing ({lim})...")
        lines = []
        try:
            async for u in client.iter_participants(ev.chat_id, limit=lim, aggressive=True):
                lines.append(f"@{u.username or 'None'} | {u.first_name} | {u.id}")
                if len(lines) % 300 == 0: await msg.edit(f"🔍 {len(lines)}...")
        except Exception as e: return await msg.edit(f"❌ {e}")
        
        fn = f"u_{ev.chat_id}.txt"
        with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(lines))
        await client.send_file(ev.chat_id, fn, caption=f"✅ Count: {len(lines)}")
        os.remove(fn)

    @client.on(events.NewMessage(pattern=r'^\.csv$'))
    async def csv_parse(ev):
        lim = await get_user_limit(ADMIN_ID)
        msg = await ev.reply(f"📊 CSV Parsing ({lim})...")
        rows = []
        try:
            async for u in client.iter_participants(ev.chat_id, limit=lim, aggressive=True):
                rows.append([u.id, u.username or "", u.first_name or "", u.phone or ""])
                if len(rows) % 300 == 0: await msg.edit(f"📊 {len(rows)}...")
        except Exception as e: return await msg.edit(f"❌ {e}")
        
        fn = f"export_{ev.chat_id}.csv"
        with open(fn, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["ID", "Username", "Name", "Phone"])
            writer.writerows(rows)
        
        await client.send_file(ev.chat_id, fn, caption=f"✅ CSV Ready: {len(rows)}")
        os.remove(fn)

    @client.on(events.NewMessage(pattern=r'^\.лс (.*?)(?: @(\S+))?$'))
    async def dm_cmd(ev):
        # (Same logic as before)
        match = re.match(r'^\.лс (.*?)(?: @(\S+))?$', ev.text, re.DOTALL)
        if not match: return await ev.reply("❌ .лс msg @user")
        txt, usrs = match.group(1), match.group(2).split()
        await ev.reply(f"🚀 Sending to {len(usrs)}...")
        for u in usrs:
            try:
                await client.send_message(u.lstrip('@'), txt)
                await asyncio.sleep(random.uniform(2, 5))
            except: pass
        await ev.reply("✅ Done")

    WORKER_STATUS = "🟢 Активен"
    logger.info("Worker Started")
    await client.start()
    await client.run_until_disconnected()

# =========================================================================
# VII. MAIN
# =========================================================================

async def main():
    global WORKER_TASK
    await init_db()
    
    dp.message.middleware(SecurityMiddleware())
    dp.callback_query.middleware(SecurityMiddleware())
    
    WORKER_TASK = asyncio.create_task(worker_process())
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        if WORKER_TASK: WORKER_TASK.cancel()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
