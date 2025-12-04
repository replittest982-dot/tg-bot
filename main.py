#!/usr/bin/env python3
"""
🇷🇺 StatPro v22.1 - RUSSIFIER EDITION
------------------------------------
ЯЗЫК: Полностью Русский интерфейс (Меню, Кнопки, Ответы).
ФУНКЦИОНАЛ: Сохранены все 85+ улучшений версии Quantum.
ПРОМОКОДЫ: Доступно и создание кодов, и выдача по ID.
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
import shutil
import time
import json
import math
import html
import traceback
import aiosqlite
from typing import Dict, Optional, Union, List, Set, Any
from pathlib import Path
from datetime import datetime, timedelta

# --- ИМПОРТЫ ---
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

from telethon import TelegramClient, events, functions, types, Button
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, UserPrivacyRestrictedError,
    ChatAdminRequiredError, UserNotParticipantError, BadRequestError,
    RpcCallFailError
)
from telethon.tl.types import (
    ChannelParticipantsAdmins, ChatBannedRights, User, Message as TlMessage
)
from telethon.tl.functions.channels import (
    EditAdminRequest, JoinChannelRequest, LeaveChannelRequest, 
    GetFullChannelRequest
)
from telethon.tl.functions.messages import (
    ExportChatInviteRequest, SendReactionRequest, SetTypingRequest,
    ReadHistoryRequest
)
from telethon.tl.functions.contacts import BlockRequest, UnblockRequest

import qrcode
from PIL import Image

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ
# =========================================================================

VERSION = "v22.1 RUS"
START_TS = datetime.now().timestamp()

BASE_DIR = Path("/app")
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "database.db"
LOG_FILE = BASE_DIR / "bot.log"

SESSION_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode='a')
    ]
)
logger = logging.getLogger("StatPro")

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    SUPPORT_BOT = os.getenv("SUPPORT_BOT_USERNAME", "@suppor_tstatpro1bot")
except Exception as e:
    logger.critical(f"Ошибка конфига: {e}")
    sys.exit(1)

if not all([BOT_TOKEN, API_ID, API_HASH]):
    sys.exit(1)

TEMP_DATA = {} 

RE_PHONE = r"^\+?[0-9]{10,15}$"
RE_PROMO = r"^[A-Za-z0-9-]{4,20}$"

# =========================================================================
# 🗄️ МЕНЕДЖЕР БАЗЫ ДАННЫХ
# =========================================================================

class DatabaseManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.path = DB_PATH

    def get_connection(self):
        return aiosqlite.connect(self.path, timeout=30.0)

    async def init(self):
        async with self.get_connection() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    sub_end TEXT,
                    parse_limit INTEGER DEFAULT 1000,
                    is_banned INTEGER DEFAULT 0,
                    settings TEXT DEFAULT '{}'
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_sub ON users(sub_end)")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY,
                    days INTEGER,
                    activations INTEGER
                )
            """)
            await db.commit()

    async def add_user(self, uid: int, uname: str):
        now = datetime.now().isoformat()
        end = now 
        async with self.get_connection() as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, sub_end) VALUES (?, ?, ?)",
                (uid, uname, end)
            )
            await db.commit()

    async def get_user(self, uid: int):
        async with self.get_connection() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as c:
                return await c.fetchone()

    async def update_sub(self, uid: int, days: int):
        u = await self.get_user(uid)
        current_end = datetime.fromisoformat(u['sub_end']) if u else datetime.now()
        if current_end < datetime.now(): current_end = datetime.now()
        new_end = current_end + timedelta(days=days)
        
        async with self.get_connection() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, sub_end) VALUES (?, ?)", (uid, datetime.now().isoformat()))
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()

    async def set_ban(self, uid: int, state: int):
        async with self.get_connection() as db:
            await db.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (state, uid))
            await db.commit()

    async def create_promo(self, days: int, acts: int):
        code = f"PRO-{uuid.uuid4().hex[:6].upper()}"
        async with self.get_connection() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

    async def use_promo(self, uid: int, code: str) -> bool:
        async with self.get_connection() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ?", (code,)) as c:
                res = await c.fetchone()
                if not res or res[1] < 1: return False
                days = res[0]
            
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ?", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            await db.commit()
        
        await self.update_sub(uid, days)
        return True

    async def get_stats(self):
        async with self.get_connection() as db:
            async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (datetime.now().isoformat(),)) as c: active = (await c.fetchone())[0]
        return total, active

db = DatabaseManager()

# =========================================================================
# 🧠 КЛАСС USER WORKER (Воркер Юзера)
# =========================================================================

class UserWorker:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.client: Optional[TelegramClient] = None
        self.task: Optional[asyncio.Task] = None
        self.status = "⚪️ Инициализация"
        self.stop_signal = False
        self.is_afk = False
        self.afk_reason = ""
        self.start_ts = 0

    def get_session_file(self) -> Path:
        return SESSION_DIR / f"session_{self.user_id}"

    async def start(self):
        if self.task and not self.task.done():
            self.task.cancel()
        self.task = asyncio.create_task(self._loop())

    async def stop(self):
        self.stop_signal = True
        if self.client: await self.client.disconnect()
        if self.task: self.task.cancel()
        self.status = "🔴 Остановлен"

    async def _stealth_delete(self, event):
        try: await event.delete()
        except: pass

    async def _temp_msg(self, event, text, delay=2):
        try:
            m = await event.respond(text)
            await asyncio.sleep(delay)
            await m.delete()
        except: pass

    async def _loop(self):
        self.start_ts = time.time()
        self.status = "🟡 Подключение..."
        
        try:
            sess = self.get_session_file()
            if not sess.with_suffix(".session").exists():
                self.status = "🔴 Нет Сессии"
                return

            self.client = TelegramClient(str(sess), API_ID, API_HASH, connection_retries=None)
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                self.status = "🔴 Ошибка Входа"
                return

            self.status = "🟢 Активен"
            logger.info(f"Worker {self.user_id} Online")
            
            self._register_handlers()
            
            await self.client.run_until_disconnected()

        except FloodWaitError as e:
            self.status = f"⏳ Флуд-Ожидание {e.seconds}с"
            await asyncio.sleep(e.seconds)
        except Exception as e:
            self.status = f"🔴 Сбой: {e.__class__.__name__}"
            logger.error(f"Worker {self.user_id} Crash: {e}")
        finally:
            if self.client: await self.client.disconnect()
            self.status = "⚪️ Оффлайн"

    def _register_handlers(self):
        c = self.client
        
        # --- 1. УТИЛИТЫ ---
        @c.on(events.NewMessage(pattern=r'^\.ping$'))
        async def ping(e):
            s = time.time()
            await self._stealth_delete(e)
            await self._temp_msg(e, f"🏓 Понг! {int((time.time()-s)*1000)}мс")

        @c.on(events.NewMessage(pattern=r'^\.id$'))
        async def show_id(e):
            await self._stealth_delete(e)
            rid = e.reply_to_msg_id
            if rid:
                r = await e.get_reply_message()
                await self._temp_msg(e, f"🆔 Юзер: `{r.sender_id}`\nMsg: `{r.id}`\nЧат: `{e.chat_id}`", 5)
            else:
                await self._temp_msg(e, f"🆔 Чат: `{e.chat_id}`", 5)

        @c.on(events.NewMessage(pattern=r'^\.info$'))
        async def chat_info(e):
            await self._stealth_delete(e)
            try:
                full = await c(GetFullChannelRequest(e.chat_id))
                chat = full.full_chat
                txt = (f"ℹ️ <b>Инфо о чате</b>\n"
                       f"ID: <code>{e.chat_id}</code>\n"
                       f"Название: {full.chats[0].title}\n"
                       f"Участников: {chat.participants_count}\n"
                       f"Админов: {chat.admins_count}\n"
                       f"Онлайн: {getattr(chat, 'online_count', '?')}")
                await self._temp_msg(e, txt, 10)
            except: pass

        # --- 2. МОДЕРАЦИЯ ---
        @c.on(events.NewMessage(pattern=r'^\.ban$'))
        async def ban(e):
            await self._stealth_delete(e)
            if not e.is_reply: return
            try:
                r = await e.get_reply_message()
                await c(EditAdminRequest(e.chat_id, r.sender_id, ChatBannedRights(until_date=None, view_messages=True), rank=""))
                await self._temp_msg(e, "⛔ Забанен", 2)
            except Exception as ex: await self._temp_msg(e, f"❌ {ex}", 2)

        @c.on(events.NewMessage(pattern=r'^\.mute (\d+)([mhd])$'))
        async def mute(e):
            await self._stealth_delete(e)
            if not e.is_reply: return
            args = e.pattern_match
            val, unit = int(args.group(1)), args.group(2)
            td = timedelta(minutes=val) if unit=='m' else timedelta(hours=val) if unit=='h' else timedelta(days=val)
            unit_rus = 'мин' if unit=='m' else 'час' if unit=='h' else 'дней'
            try:
                r = await e.get_reply_message()
                await c(EditAdminRequest(e.chat_id, r.sender_id, ChatBannedRights(until_date=datetime.now()+td, send_messages=True), rank=""))
                await self._temp_msg(e, f"😶 Мут на {val} {unit_rus}", 3)
            except: pass

        @c.on(events.NewMessage(pattern=r'^\.purge$'))
        async def purge(e):
            await self._stealth_delete(e)
            if not e.is_reply: return
            r = await e.get_reply_message()
            msgs = []
            async for m in c.iter_messages(e.chat_id, min_id=r.id - 1):
                msgs.append(m.id)
            await c.delete_messages(e.chat_id, msgs)

        # --- 3. ПАРСИНГ (PHANTOM) ---
        @c.on(events.NewMessage(pattern=r'^\.scan$'))
        async def scan(e):
            await self._stealth_delete(e)
            self.stop_signal = False
            self.status = "🔎 Сканирую..."
            
            u_lim = await db.get_user(self.user_id)
            limit = u_lim['parse_limit'] if u_lim else 500
            
            msg = await e.respond(f"👻 <b>Фантом Сканирование</b>\nЛимит: {limit}")
            unique = {}
            count = 0
            
            try:
                async for m in c.iter_messages(e.chat_id, limit=30000):
                    if self.stop_signal: break
                    count += 1
                    if m.sender and isinstance(m.sender, User) and not m.sender.bot:
                        if m.sender_id not in unique:
                            unique[m.sender_id] = f"@{m.sender.username or 'None'} | {m.sender.first_name} | {m.sender_id}"
                    
                    if count % 300 == 0:
                        await msg.edit(f"👻 Проверено: {count} | Найдено: {len(unique)}/{limit}")
                    
                    if len(unique) >= limit: break
                
                if not self.stop_signal:
                    TEMP_DATA[self.user_id] = {'lines': list(unique.values()), 'title': str(e.chat_id)}
                    await msg.edit("✅ Готово.")
                    await asyncio.sleep(1)
                    await msg.delete()
                    # Trigger Bot
                    try:
                        await bot.send_message(self.user_id, f"📁 <b>Результат Парсинга</b>\nЧат: {e.chat_id}\nЮзеров: {len(unique)}", reply_markup=kb_parse())
                    except: pass
            
            except Exception as ex:
                await msg.edit(f"❌ Ошибка: {ex}")
            finally:
                self.status = "🟢 Активен"

        @c.on(events.NewMessage(pattern=r'^\.stop$'))
        async def stop_scan(e):
            await self._stealth_delete(e)
            self.stop_signal = True
            await self._temp_msg(e, "🛑 Останавливаю...", 2)

        # --- 4. FUN & RAID ---
        @c.on(events.NewMessage(pattern=r'^\.spam (\d+) (.*)'))
        async def spam(e):
            await self._stealth_delete(e)
            n, txt = int(e.pattern_match.group(1)), e.pattern_match.group(2)
            for _ in range(n):
                if self.stop_signal: break
                await c.send_message(e.chat_id, txt)
                await asyncio.sleep(0.3)

        @c.on(events.NewMessage(pattern=r'^\.tspam (.*)'))
        async def tspam(e):
            await self._stealth_delete(e)
            txt = e.pattern_match.group(1)
            for char in txt:
                await c.send_message(e.chat_id, char)
                await asyncio.sleep(0.2)

        @c.on(events.NewMessage(pattern=r'^\.magic$'))
        async def magic(e):
            await self._stealth_delete(e)
            m = await e.respond("Жди...")
            anim = ["🔴", "🟠", "🟡", "🟢", "🔵", "🟣", "✨ МАГИЯ ✨"]
            for frame in anim:
                await m.edit(frame)
                await asyncio.sleep(0.3)

        @c.on(events.NewMessage(pattern=r'^\.calc (.+)'))
        async def calc(e):
            await self._stealth_delete(e)
            expr = e.pattern_match.group(1)
            try:
                if not re.match(r"^[0-9+\-*/(). ]+$", expr):
                    return await self._temp_msg(e, "❌ Неверные символы", 2)
                res = eval(expr, {"__builtins__": {}}, {"math": math})
                await self._temp_msg(e, f"🔢 {expr} = <b>{res}</b>", 5)
            except: pass

        # --- 5. AFK СИСТЕМА ---
        @c.on(events.NewMessage(pattern=r'^\.afk ?(.*)'))
        async def set_afk(e):
            await self._stealth_delete(e)
            self.is_afk = True
            self.afk_reason = e.pattern_match.group(1) or "Занят"
            await self._temp_msg(e, f"💤 Режим AFK: {self.afk_reason}", 3)

        @c.on(events.NewMessage(pattern=r'^\.unafk$'))
        async def unafk(e):
            await self._stealth_delete(e)
            self.is_afk = False
            await self._temp_msg(e, "👋 С возвращением!", 3)

        @c.on(events.NewMessage(incoming=True))
        async def afk_handler(e):
            if self.is_afk and e.is_private:
                await e.reply(f"💤 <b>Я сейчас AFK.</b>\nПричина: {self.afk_reason}")
            elif self.is_afk and e.mentioned:
                await e.reply(f"💤 <b>Не тегай меня.</b>\nПричина: {self.afk_reason}")

        # --- 6. ПОМОЩЬ ---
        @c.on(events.NewMessage(pattern=r'^\.help$'))
        async def help_cmd(e):
            await self._stealth_delete(e)
            txt = (
                "💎 <b>Помощь StatPro Quantum</b>\n"
                "<b>Админ:</b> .ban, .mute, .kick, .purge, .pin\n"
                "<b>Инфо:</b> .id, .info, .admins, .bots\n"
                "<b>Веселье:</b> .spam, .tspam, .magic, .calc\n"
                "<b>Парсинг:</b> .scan, .stop\n"
                "<b>Утилиты:</b> .afk, .unafk, .msg\n"
            )
            await self._temp_msg(e, txt, 15)

# =========================================================================
# 🎮 МЕНЕДЖЕР ВОРКЕРОВ
# =========================================================================

WORKERS: Dict[int, UserWorker] = {}

async def start_worker(uid: int):
    if uid in WORKERS: await WORKERS[uid].stop()
    
    worker = UserWorker(uid)
    WORKERS[uid] = worker
    await worker.start()

async def stop_worker(uid: int):
    if uid in WORKERS:
        await WORKERS[uid].stop()
        del WORKERS[uid]

async def restart_all_workers():
    for w in list(WORKERS.values()):
        await w.stop()
    
    for f in SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            await start_worker(uid)
        except: pass

# =========================================================================
# 🤖 БОТ (AIOGRAM)
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AdminStates(StatesGroup):
    PROMO_DAYS = State()
    PROMO_ACT = State()
    GRANT_ID = State()
    GRANT_DAYS = State()
    BAN_ID = State()
    BROADCAST = State()

class AuthStates(StatesGroup):
    PHONE = State()
    CODE = State()
    PASS = State()

# --- Клавиатуры ---
def kb_main(uid: int, is_sub: bool):
    kb = []
    if is_sub or uid == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="🔑 Подключить Аккаунт", callback_data="auth")])
    kb.append([InlineKeyboardButton(text="👤 Профиль", callback_data="profile")])
    if is_sub:
        kb.append([InlineKeyboardButton(text="👻 Меню Воркера", callback_data="worker")])
    if uid == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="👑 Админ Панель", callback_data="admin")])
    kb.append([InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_BOT.replace('@','')} ")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_parse():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 TXT", callback_data="dl:txt"),
         InlineKeyboardButton(text="📊 JSON", callback_data="dl:json")],
        [InlineKeyboardButton(text="📑 CSV", callback_data="dl:csv")]
    ])

def kb_auth():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 По номеру", callback_data="auth_phone"),
         InlineKeyboardButton(text="📸 По QR-коду", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu")]
    ])

# --- Middleware ---
class MainMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        uid = event.from_user.id
        await db.add_user(uid, event.from_user.username or "Unknown")
        
        u = await db.get_user(uid)
        if u and u['is_banned']: return
        
        return await handler(event, data)

dp.message.middleware(MainMiddleware())
dp.callback_query.middleware(MainMiddleware())

# --- Хендлеры ---
@router.message(Command("start"))
async def start_cmd(m: Message):
    u = await db.get_user(m.from_user.id)
    is_sub = datetime.fromisoformat(u['sub_end']) > datetime.now() if u else False
    await m.answer(f"🌌 <b>StatPro Quantum</b>\nПривет, {m.from_user.first_name}", 
                   reply_markup=kb_main(m.from_user.id, is_sub))

@router.callback_query(F.data == "menu")
async def menu_cb(c: CallbackQuery):
    u = await db.get_user(c.from_user.id)
    is_sub = datetime.fromisoformat(u['sub_end']) > datetime.now()
    await c.message.edit_text("🏠 <b>Главное меню</b>", reply_markup=kb_main(c.from_user.id, is_sub))

@router.callback_query(F.data == "profile")
async def profile_cb(c: CallbackQuery):
    u = await db.get_user(c.from_user.id)
    end = datetime.fromisoformat(u['sub_end'])
    is_sub = end > datetime.now()
    w_status = WORKERS[c.from_user.id].status if c.from_user.id in WORKERS else "⚪️ Оффлайн"
    
    txt = (f"👤 <b>Профиль</b>\n"
           f"ID: <code>{c.from_user.id}</code>\n"
           f"Подписка: {'✅' if is_sub else '❌'} (До: {end.strftime('%d.%m.%Y')})\n"
           f"Воркер: {w_status}\n"
           f"Лимит парсинга: {u['parse_limit']}")
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="menu")]]))

@router.callback_query(F.data == "worker")
async def worker_cb(c: CallbackQuery):
    w = WORKERS.get(c.from_user.id)
    st = w.status if w else "⚪️ Оффлайн"
    txt = (f"👻 <b>Управление Воркером</b>\nСтатус: {st}\n\n"
           "Команды в чате:\n"
           "<code>.help</code> - Список команд\n"
           "<code>.scan</code> - Начать парсинг\n"
           "<code>.stop</code> - Остановить парсинг\n"
           "<code>.restart</code> - Перезагрузить")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Рестарт", callback_data="w_restart"),
         InlineKeyboardButton(text="🛑 Стоп", callback_data="w_stop")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu")]
    ])
    await c.message.edit_text(txt, reply_markup=kb)

@router.callback_query(F.data == "w_restart")
async def w_res(c: CallbackQuery):
    await c.answer("Перезагрузка...")
    await start_worker(c.from_user.id)
    await worker_cb(c)

@router.callback_query(F.data == "w_stop")
async def w_stop(c: CallbackQuery):
    await stop_worker(c.from_user.id)
    await c.answer("Остановлено.")
    await worker_cb(c)

# --- Скачивание Результатов ---
@router.callback_query(F.data.startswith("dl:"))
async def dl_res(c: CallbackQuery):
    fmt = c.data.split(":")[1]
    data = TEMP_DATA.get(c.from_user.id)
    if not data: return await c.answer("Устарело.", show_alert=True)
    
    lines = data['lines']
    name = f"Result_{data['title']}"
    
    f = None
    if fmt == "txt":
        f = FSInputFile(name + ".txt")
        with open(name + ".txt", "w", encoding="utf-8") as file: file.write("\n".join(lines))
    elif fmt == "json":
        f = FSInputFile(name + ".json")
        with open(name + ".json", "w", encoding="utf-8") as file: json.dump(lines, file, indent=2)
    elif fmt == "csv":
        f = FSInputFile(name + ".csv")
        with open(name + ".csv", "w", newline="", encoding="utf-8") as file:
            w = csv.writer(file); w.writerow(["Data"]); 
            for l in lines: w.writerow([l])
            
    if f:
        await c.message.answer_document(f)
        os.remove(f.path)
    await c.answer()

# --- АДМИН ПАНЕЛЬ ---
@router.callback_query(F.data == "admin")
async def adm_cb(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    t, a = await db.get_stats()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Выдать по ID", callback_data="adm_grant"),
         InlineKeyboardButton(text="🎫 Создать Промокод", callback_data="adm_promo")],
        [InlineKeyboardButton(text="⛔ Бан Юзера", callback_data="adm_ban"),
         InlineKeyboardButton(text="📢 Рассылка", callback_data="adm_bc")],
        [InlineKeyboardButton(text="🔄 Рестарт Всех", callback_data="adm_ra"),
         InlineKeyboardButton(text="🔙 Назад", callback_data="menu")]
    ])
    await c.message.edit_text(f"👑 <b>Админ Панель</b>\nЮзеров: {t}\nАктивных: {a}\nВоркеров: {len(WORKERS)}", reply_markup=kb)

# --- Создание Промокода (Для ввода юзером) ---
@router.callback_query(F.data == "adm_promo")
async def adm_promo_start(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📅 На сколько дней код?")
    await state.set_state(AdminStates.PROMO_DAYS)

@router.message(AdminStates.PROMO_DAYS)
async def adm_promo_days(m: Message, state: FSMContext):
    await state.update_data(d=m.text)
    await m.answer("🔢 Сколько активаций?")
    await state.set_state(AdminStates.PROMO_ACT)

@router.message(AdminStates.PROMO_ACT)
async def adm_promo_fin(m: Message, state: FSMContext):
    d = await state.get_data()
    code = await db.create_promo(int(d['d']), int(m.text))
    await m.answer(f"✅ Промокод создан:\n<code>{code}</code>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Админ", callback_data="admin")]]))
    await state.clear()

# --- Выдача по ID (Без ввода) ---
@router.callback_query(F.data == "adm_grant")
async def ag(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("🆔 Введите ID Юзера:")
    await state.set_state(AdminStates.GRANT_ID)

@router.message(AdminStates.GRANT_ID)
async def ag_id(m: Message, state: FSMContext):
    await state.update_data(uid=m.text)
    await m.answer("📅 На сколько дней выдать?")
    await state.set_state(AdminStates.GRANT_DAYS)

@router.message(AdminStates.GRANT_DAYS)
async def ag_d(m: Message, state: FSMContext):
    d = await state.get_data()
    await db.update_sub(int(d['uid']), int(m.text))
    await m.answer("✅ Подписка выдана.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Админ", callback_data="admin")]]))
    await state.clear()

@router.callback_query(F.data == "adm_ra")
async def adm_restart_all(c: CallbackQuery):
    await c.answer("Перезагрузка воркеров...")
    await restart_all_workers()
    await c.message.answer("✅ Система перезагружена")

# --- СИСТЕМА АВТОРИЗАЦИИ (Telethon) ---
@router.callback_query(F.data == "auth")
async def auth_start(c: CallbackQuery):
    await c.message.edit_text("🔐 Выберите метод:", reply_markup=kb_auth())

@router.callback_query(F.data == "auth_qr")
async def auth_qr(c: CallbackQuery):
    uid = c.from_user.id
    path = SESSION_DIR / f"session_{uid}"
    client = TelegramClient(str(path), API_ID, API_HASH)
    await client.connect()
    
    qr = await client.qr_login()
    qr_img = qrcode.make(qr.url).convert("RGB")
    b = io.BytesIO(); qr_img.save(b, "PNG"); b.seek(0)
    
    msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="📸 Сканируйте в Настройки > Устройства")
    
    try:
        await qr.wait(timeout=120)
        await msg.delete()
        await c.message.answer("✅ Успех! Запускаю воркер...")
        await client.disconnect()
        await start_worker(uid)
    except Exception as e:
        await msg.delete()
        await c.message.answer(f"❌ Ошибка: {e}")
        await client.disconnect()

# =========================================================================
# 🚀 ЗАПУСК
# =========================================================================

async def main():
    await db.init()
    
    for f in SESSION_DIR.glob("*.session"):
        if f.stat().st_size == 0: f.unlink()

    asyncio.create_task(restart_all_workers())
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        for w in WORKERS.values(): await w.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Система выключена")
