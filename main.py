#!/usr/bin/env python3
"""
💎 StatPro v27.0 - INFINITE EDITION
-----------------------------------
UI: Справочник перенесен в кнопку "📚 Справочник".
SCAN: .scan [любое число] или .scan all.
CORE: Полная защита, авто-рестарт, сохранение отчетов.
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
import aiosqlite
from typing import Dict, Optional, Union, List, Any
from pathlib import Path
from datetime import datetime, timedelta, timezone

# --- ЛИБЫ ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile, FSInputFile
)
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.dispatcher.middlewares.base import BaseMiddleware

from telethon import TelegramClient, events, types
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import User, ChatBannedRights

import qrcode
from PIL import Image

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ
# =========================================================================

VERSION = "v27.0 INFINITE"
MSK_TZ = timezone(timedelta(hours=3))

BASE_DIR = Path("/app")
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "infinite.db"
LOG_FILE = BASE_DIR / "bot.log"
STATE_FILE = BASE_DIR / "reports_state.json"

SESSION_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, mode='a')]
)
logger = logging.getLogger("StatPro")

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    SUPPORT_BOT = os.getenv("SUPPORT_BOT_USERNAME", "@suppor_tstatpro1bot")
except: sys.exit(1)

if not all([BOT_TOKEN, API_ID, API_HASH]): sys.exit(1)

TEMP_DATA = {} 
RE_IT_CMD = r'^\.(встал|зм|пв)\s*(\d+)$'

# =========================================================================
# 📝 БАЗА ЗНАНИЙ (ТЕКСТ)
# =========================================================================

HELP_TEXT = """
<b>📖 СПРАВОЧНИК КОМАНД</b>
━━━━━━━━━━━━━━━━━━
<b>1️⃣ 👻 ПАРСИНГ (.scan)</b>
Сканирует участников через историю сообщений (обходит скрытие).
• <code>.scan</code> — Сканировать <b>100</b> последних (быстро).
• <code>.scan 5000</code> — Сканировать <b>5000</b> (любое число).
• <code>.scan 150000</code> — Сканировать <b>150к</b> (глубокий скан).
• <code>.scan all</code> — Сканировать <b>ВСЮ</b> доступную историю.

<b>2️⃣ 💻 IT-ОТЧЕТЫ</b>
Работают в топиках или чатах. Бот сам считает время (МСК).
• <code>.айтистарт</code> — Начать смену.
• <code>.встал (номер)</code> — Запись "Встал".
• <code>.зм (номер)</code> — Запись "Замена".
• <code>.пв (номер)</code> — Запись "Повтор".
• <code>.отчетайти</code> — Посмотреть таблицу (не останавливая).
• <code>.айтистоп</code> — Закончить смену и получить итог.

<b>3️⃣ 📦 ДРОП-ОТЧЕТЫ</b>
Лог всех сообщений в теме.
• <code>.отчетыстарт</code> — Начать запись.
• <code>.отчетдропы</code> — Глянуть текущий лог.
• <code>.отчетыстоп</code> — Стоп и скачать файл.

<b>4️⃣ 🛠 МОДЕРАЦИЯ</b>
(В ответ на сообщение)
• <code>.ban</code> — Бан.
• <code>.mute 30m</code> — Мут на 30 мин (h-часы, d-дни).
• <code>.purge</code> — Удалить сообщения вниз.
• <code>.afk (причина)</code> — Автоответчик.
• <code>.unafk</code> — Я тут.

<b>5️⃣ 💡 РАЗНОЕ</b>
• <code>.id</code>, <code>.info</code>, <code>.ping</code>, <code>.calc</code>, <code>.restart</code>
━━━━━━━━━━━━━━━━━━
"""

# =========================================================================
# 🗄️ БАЗА ДАННЫХ
# =========================================================================

class DatabaseManager:
    _instance = None
    def __new__(cls):
        if cls._instance is None: cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    def __init__(self): self.path = DB_PATH
    def get_connection(self): return aiosqlite.connect(self.path, timeout=30.0)

    async def init(self):
        if self.path.exists(): shutil.copy(self.path, f"{self.path}.backup")
        async with self.get_connection() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    sub_end TEXT,
                    parse_limit INTEGER DEFAULT 1000,
                    is_banned INTEGER DEFAULT 0
                )
            """)
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
        async with self.get_connection() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, sub_end) VALUES (?, ?, ?)", (uid, uname, now))
            await db.commit()

    async def get_user(self, uid: int):
        async with self.get_connection() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as c: return await c.fetchone()

    async def check_sub(self, uid: int) -> bool:
        if uid == ADMIN_ID: return True
        u = await self.get_user(uid)
        if not u: return False
        try: return datetime.fromisoformat(u['sub_end']) > datetime.now()
        except: return False

    async def update_sub(self, uid: int, days: int):
        u = await self.get_user(uid)
        curr = datetime.fromisoformat(u['sub_end']) if u and u['sub_end'] else datetime.now()
        if curr < datetime.now(): curr = datetime.now()
        new_end = curr + timedelta(days=days)
        async with self.get_connection() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, sub_end) VALUES (?, ?)", (uid, datetime.now().isoformat()))
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()

    async def create_promo(self, days: int, acts: int):
        code = f"STAT-{random.randint(1000,9999)}-{days}D"
        async with self.get_connection() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

    async def use_promo(self, uid: int, code: str) -> int:
        async with self.get_connection() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ?", (code,)) as c:
                res = await c.fetchone()
                if not res or res[1] < 1: return 0
                days = res[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ?", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            await db.commit()
        await self.update_sub(uid, days)
        return days

db = DatabaseManager()

# =========================================================================
# 💾 СОХРАНЕНИЕ ОТЧЕТОВ
# =========================================================================

class ReportPersistence:
    @staticmethod
    def save(active_reports: dict):
        try:
            data = {k: {**v, 'start_time': v['start_time'].isoformat()} for k, v in active_reports.items()}
            with open(STATE_FILE, 'w', encoding='utf-8') as f: json.dump(data, f, ensure_ascii=False, indent=2)
        except: pass

    @staticmethod
    def load() -> dict:
        if not STATE_FILE.exists(): return {}
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f: raw = json.load(f)
            return {k: {**v, 'start_time': datetime.fromisoformat(v['start_time'])} for k, v in raw.items()}
        except: return {}

# =========================================================================
# 📊 МЕНЕДЖЕР ОТЧЕТОВ
# =========================================================================

class ReportManager:
    _shared_state = ReportPersistence.load()

    def start_it(self, chat_id, topic_id):
        key = f"{chat_id}_{topic_id}"
        self._shared_state[key] = {'type': 'it', 'data': [], 'start_time': datetime.now(MSK_TZ)}
        ReportPersistence.save(self._shared_state)

    def start_drop(self, chat_id, topic_id):
        key = f"{chat_id}_{topic_id}"
        self._shared_state[key] = {'type': 'drop', 'data': [], 'start_time': datetime.now(MSK_TZ)}
        ReportPersistence.save(self._shared_state)

    def add_it(self, chat_id, topic_id, user, action, number):
        key = f"{chat_id}_{topic_id}"
        if key in self._shared_state and self._shared_state[key]['type'] == 'it':
            t = datetime.now(MSK_TZ).strftime("%H:%M")
            self._shared_state[key]['data'].append({'time': t, 'user': user, 'action': action, 'number': number})
            ReportPersistence.save(self._shared_state)
            return True
        return False

    def add_drop(self, chat_id, topic_id, user, text):
        key = f"{chat_id}_{topic_id}"
        if key in self._shared_state and self._shared_state[key]['type'] == 'drop':
            t = datetime.now(MSK_TZ).strftime("%H:%M")
            self._shared_state[key]['data'].append(f"[{t}] {user}: {text}")
            ReportPersistence.save(self._shared_state)
            return True
        return False

    def get_data(self, chat_id, topic_id): return self._shared_state.get(f"{chat_id}_{topic_id}")

    def stop(self, chat_id, topic_id):
        key = f"{chat_id}_{topic_id}"
        if key in self._shared_state:
            data = self._shared_state.pop(key)
            ReportPersistence.save(self._shared_state)
            return data
        return None

# =========================================================================
# 🧠 ВОРКЕР
# =========================================================================

class UserWorker:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.client: Optional[TelegramClient] = None
        self.task: Optional[asyncio.Task] = None
        self.status = "⚪️ Init"
        self.reports = ReportManager()
        self.is_afk = False
        self.afk_reason = ""

    def get_session_file(self) -> Path: return SESSION_DIR / f"session_{self.user_id}"

    async def start(self):
        if not await db.check_sub(self.user_id): self.status = "⛔️ No Sub"; return False
        if self.task and not self.task.done(): self.task.cancel()
        self.task = asyncio.create_task(self._loop())
        return True

    async def stop(self):
        if self.client: await self.client.disconnect()
        if self.task: self.task.cancel()
        self.status = "🔴 Off"

    async def _msg(self, event, text, delay=2):
        try: m = await event.respond(text); await asyncio.sleep(delay); await m.delete(); await event.delete()
        except: pass

    async def _loop(self):
        self.status = "🟡 Connect..."
        try:
            sess = self.get_session_file()
            if not sess.with_suffix(".session").exists(): self.status = "🔴 No Session"; return
            self.client = TelegramClient(str(sess), API_ID, API_HASH)
            await self.client.connect()
            if not await self.client.is_user_authorized(): self.status = "🔴 Auth Failed"; return
            self.status = "🟢 Active"
            self._reg()
            await self.client.run_until_disconnected()
        except Exception as e: self.status = f"🔴 Err: {e}"
        finally: 
            if self.client: await self.client.disconnect()

    def _reg(self):
        c = self.client

        # --- NEW: UNLIMITED SCAN ---
        @c.on(events.NewMessage(pattern=r'^\.scan(?:\s+(\d+|all))?$'))
        async def flex_scan(e):
            await e.delete()
            arg = e.pattern_match.group(1)
            
            # Логика лимита
            limit = 100 
            if arg == 'all': limit = 1000000 
            elif arg: limit = int(arg)
            
            text_limit = "ALL" if limit > 999999 else str(limit)
            msg = await e.respond(f"👻 <b>Scanning...</b>\nTarget: {text_limit} msgs")
            
            unique = {}
            count = 0
            
            try:
                async for m in c.iter_messages(e.chat_id, limit=limit):
                    count += 1
                    if m.sender and isinstance(m.sender, User) and not m.sender.bot:
                        if m.sender_id not in unique:
                            uname = f"@{m.sender.username}" if m.sender.username else "NoUsername"
                            unique[m.sender_id] = f"{uname} | {m.sender.first_name or ''} | {m.sender_id}"
                    
                    if count % 1000 == 0:
                        await msg.edit(f"👻 Scan: {count}\nFound: {len(unique)}")

                res_text = "\n".join(list(unique.values()))
                TEMP_DATA[self.user_id] = {'lines': list(unique.values()), 'title': f"Scan_{e.chat_id}"}
                
                await msg.edit(f"✅ <b>Done!</b>\nScanned: {count}\nFound: {len(unique)}")
                await asyncio.sleep(3); await msg.delete()
                
                try: 
                    await bot.send_message(
                        self.user_id, 
                        f"📂 <b>Scan Result</b>\nChat: {e.chat_id}\nFound: {len(unique)}",
                        reply_markup=kb_parse()
                    )
                except: pass

            except Exception as ex:
                await msg.edit(f"❌ Err: {ex}")
                await asyncio.sleep(3); await msg.delete()

        # IT
        @c.on(events.NewMessage(pattern=r'^\.айтистарт$'))
        async def it_s(e): self.reports.start_it(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)); await self._msg(e, "💻 IT Start", 3)
        @c.on(events.NewMessage(pattern=r'^\.айтистоп$'))
        async def it_st(e): 
            res = self.reports.stop(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if res: await self._send_it(res['data']); await self._msg(e, "✅ Stopped", 3)
        @c.on(events.NewMessage(pattern=r'^\.отчетайти$'))
        async def it_v(e): 
            res = self.reports.get_data(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if res and res['type']=='it': await self._send_it(res['data']); await self._msg(e, "📨 Sent", 2)
        @c.on(events.NewMessage(pattern=RE_IT_CMD))
        async def it_h(e): 
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            if self.reports.add_it(e.chat_id, tid, e.sender.first_name, e.pattern_match.group(1).lower(), e.pattern_match.group(2)):
                try: await c(SendReactionRequest(e.chat_id, e.id, reaction=[types.ReactionEmoji(emoticon='✍️')]))
                except: pass

        # DROP
        @c.on(events.NewMessage(pattern=r'^\.отчетыстарт$'))
        async def d_s(e): self.reports.start_drop(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)); await self._msg(e, "📦 Drop Start", 3)
        @c.on(events.NewMessage(pattern=r'^\.отчетыстоп$'))
        async def d_st(e):
            res = self.reports.stop(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if res: await self._send_drop(res['data']); await self._msg(e, "✅ Stopped", 3)
        @c.on(events.NewMessage(pattern=r'^\.отчетдропы$'))
        async def d_v(e):
            res = self.reports.get_data(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if res and res['type']=='drop': await self._send_drop(res['data']); await self._msg(e, "📨 Sent", 2)
        @c.on(events.NewMessage())
        async def d_m(e):
            if e.text and not e.text.startswith(".") and not (e.sender and e.sender.bot):
                self.reports.add_drop(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0), e.sender.first_name, e.text)

        # TOOLS
        @c.on(events.NewMessage(pattern=r'^\.ban$'))
        async def ban(e): 
            if e.is_reply: await c(types.functions.channels.EditAdminRequest(e.chat_id, (await e.get_reply_message()).sender_id, ChatBannedRights(until_date=None, view_messages=True), "")); await self._msg(e, "⛔", 2)
        @c.on(events.NewMessage(pattern=r'^\.mute (\d+)([mhd])$'))
        async def mute(e):
            v, u = int(e.pattern_match.group(1)), e.pattern_match.group(2)
            td = timedelta(minutes=v) if u=='m' else timedelta(hours=v) if u=='h' else timedelta(days=v)
            if e.is_reply: await c(types.functions.channels.EditAdminRequest(e.chat_id, (await e.get_reply_message()).sender_id, ChatBannedRights(until_date=datetime.now()+td, send_messages=True), "")); await self._msg(e, "😶", 2)
        @c.on(events.NewMessage(pattern=r'^\.afk ?(.*)'))
        async def afk(e): self.is_afk=True; self.afk_reason=e.pattern_match.group(1); await self._msg(e, "💤 AFK", 3)
        @c.on(events.NewMessage(pattern=r'^\.unafk$'))
        async def unfk(e): self.is_afk=False; await self._msg(e, "👋 Online", 3)
        @c.on(events.NewMessage(incoming=True))
        async def afk_h(e): 
            if self.is_afk and e.mentioned: await e.reply(f"💤 AFK: {self.afk_reason}")
        @c.on(events.NewMessage(pattern=r'^\.restart$'))
        async def rest(e): await self._msg(e, "🔄", 2); await self.stop(); await self.start()
        @c.on(events.NewMessage(pattern=r'^\.calc (.+)'))
        async def clc(e): 
            try: await self._msg(e, f"🔢 {eval(e.pattern_match.group(1))}", 5) 
            except: pass
        @c.on(events.NewMessage(pattern=r'^\.ping$'))
        async def png(e): s=time.time(); m=await e.respond("🏓"); await m.edit(f"🏓 {int((time.time()-s)*1000)}ms"); await asyncio.sleep(2); await m.delete(); await e.delete()
        @c.on(events.NewMessage(pattern=r'^\.info$'))
        async def inf(e):
            try: f=await c(GetFullChannelRequest(e.chat_id)); await self._msg(e,f"Title: {f.chats[0].title}\nUsers: {f.full_chat.participants_count}",5)
            except: pass
        @c.on(events.NewMessage(pattern=r'^\.id$'))
        async def idd(e): await self._msg(e, f"Chat: `{e.chat_id}`", 5)
        @c.on(events.NewMessage(pattern=r'^\.purge$'))
        async def prg(e): 
            if e.is_reply: await c.delete_messages(e.chat_id, [m.id async for m in c.iter_messages(e.chat_id, min_id=(await e.get_reply_message()).id-1)]); await e.delete()

    async def _send_it(self, data):
        l = ["📅 <b>IT REPORT</b>", "", "<code>{:<6}|{:<6}|{:<10}</code>".format("TIME","ACT","NUM"), "-"*26]
        for r in data: l.append(f"<code>{r['time']:<6}|{r['action'][:3].upper():<6}|{r['number']:<10}</code>")
        try: await bot.send_message(self.user_id, "\n".join(l), parse_mode='HTML')
        except: pass

    async def _send_drop(self, data):
        fn = f"DropLog.txt"; open(fn, "w", encoding="utf-8").write("\n".join(data))
        try: await bot.send_document(self.user_id, FSInputFile(fn)); os.remove(fn)
        except: pass

# =========================================================================
# 🤖 BOT UI
# =========================================================================

WORKERS = {}
async def start_worker(uid): w=UserWorker(uid); WORKERS[uid]=w; return await w.start()
async def stop_worker(uid): 
    if uid in WORKERS: await WORKERS[uid].stop(); del WORKERS[uid]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AS(StatesGroup): PHONE=State(); CODE=State(); PASS=State()
class PS(StatesGroup): CODE=State()
class ADS(StatesGroup): DAYS=State(); ACT=State(); GID=State(); GD=State()

def kb_parse(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📂 Скачать", callback_data="dl:txt")]])
def kb_m(uid): 
    k=[[InlineKeyboardButton(text="📊 Отчеты",callback_data="rm"),InlineKeyboardButton(text="👻 Воркер",callback_data="wc")],
       [InlineKeyboardButton(text="🔑 Вход",callback_data="au"),InlineKeyboardButton(text="🎟 Промо",callback_data="ep")],
       [InlineKeyboardButton(text="📚 Справочник",callback_data="guide"),InlineKeyboardButton(text="👤 Профиль",callback_data="pr")]]
    if uid==ADMIN_ID: k.append([InlineKeyboardButton(text="👑 Админ",callback_data="ad")])
    return InlineKeyboardMarkup(inline_keyboard=k)

@router.message(Command("start"))
async def st(m: Message): 
    await db.add_user(m.from_user.id, m.from_user.username or "U")
    await m.answer("💎 <b>StatPro INFINITE</b>", reply_markup=kb_m(m.from_user.id))

@router.callback_query(F.data=="menu")
async def mn(c: CallbackQuery): await c.message.edit_text("🏠 Меню", reply_markup=kb_m(c.from_user.id))

# NEW GUIDE HANDLER
@router.callback_query(F.data=="guide")
async def guide_cb(c: CallbackQuery):
    await c.message.edit_text(HELP_TEXT, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="menu")]]))

# AUTH
@router.callback_query(F.data=="au")
async def au(c: CallbackQuery): await c.message.edit_text("Выбор:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="QR",callback_data="aq"),InlineKeyboardButton(text="Phone",callback_data="ap")],[InlineKeyboardButton(text="Back",callback_data="menu")]]))
@router.callback_query(F.data=="aq")
async def aq(c: CallbackQuery):
    if not await db.check_sub(c.from_user.id): return await c.answer("🚫 Нет подписки!", True)
    path=SESSION_DIR/f"session_{c.from_user.id}"; cl=TelegramClient(str(path), API_ID, API_HASH); await cl.connect()
    qr=await cl.qr_login(); i=qrcode.make(qr.url).convert("RGB"); b=io.BytesIO(); i.save(b,"PNG"); b.seek(0)
    m=await c.message.answer_photo(BufferedInputFile(b.read(),"qr.png")); 
    try: await qr.wait(120); await m.delete(); await c.message.answer("✅"); await cl.disconnect(); await start_worker(c.from_user.id)
    except: await m.delete()
@router.callback_query(F.data=="ap")
async def ap(c: CallbackQuery, s: FSMContext): 
    if not await db.check_sub(c.from_user.id): return await c.answer("🚫 Нет подписки!", True)
    await c.message.edit_text("📱 Номер:"); await s.set_state(AS.PHONE)
@router.message(AS.PHONE)
async def aph(m: Message, s: FSMContext):
    cl=TelegramClient(str(SESSION_DIR/f"session_{m.from_user.id}"), API_ID, API_HASH); await cl.connect()
    r=await cl.send_code_request(m.text); await s.update_data(p=m.text,h=r.phone_code_hash,cl=cl); await m.answer("📩 Код:"); await s.set_state(AS.CODE)
@router.message(AS.CODE)
async def aco(m: Message, s: FSMContext):
    d=await s.get_data(); cl=d['cl']
    try: await cl.sign_in(phone=d['p'],code=m.text,phone_code_hash=d['h']); await m.answer("✅"); await cl.disconnect(); await start_worker(m.from_user.id); await s.clear()
    except SessionPasswordNeededError: await m.answer("🔒 2FA:"); await s.set_state(AS.PASS)
@router.message(AS.PASS)
async def apa(m: Message, s: FSMContext):
    d=await s.get_data(); cl=d['cl']; await cl.sign_in(password=m.text); await m.answer("✅"); await cl.disconnect(); await start_worker(m.from_user.id); await s.clear()

# WORKER
@router.callback_query(F.data=="wc")
async def wc(c: CallbackQuery):
    s = WORKERS.get(c.from_user.id).status if c.from_user.id in WORKERS else "Off"
    await c.message.edit_text(f"Status: {s}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Start",callback_data="wr"),InlineKeyboardButton(text="Stop",callback_data="ws")],[InlineKeyboardButton(text="Back",callback_data="menu")]]))
@router.callback_query(F.data=="wr")
async def wr(c: CallbackQuery): await start_worker(c.from_user.id); await wc(c)
@router.callback_query(F.data=="ws")
async def ws(c: CallbackQuery): await stop_worker(c.from_user.id); await wc(c)
@router.callback_query(F.data.startswith("dl:"))
async def dl(c: CallbackQuery):
    d=TEMP_DATA.get(c.from_user.id); f=FSInputFile("res.txt"); open("res.txt","w").write("\n".join(d['lines'])); await c.message.answer_document(f); os.remove("res.txt")

# REPORTS
@router.callback_query(F.data=="rm")
async def rm(c: CallbackQuery): await c.message.edit_text("Отчеты", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="IT",callback_data="ri"),InlineKeyboardButton(text="Drop",callback_data="rd")],[InlineKeyboardButton(text="Back",callback_data="menu")]]))
@router.callback_query(F.data=="ri")
async def ri(c: CallbackQuery): await c.message.edit_text("IT: .айтистарт / .встал / .отчетайти", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back",callback_data="rm")]]))
@router.callback_query(F.data=="rd")
async def rd(c: CallbackQuery): await c.message.edit_text("Drop: .отчетыстарт / .отчетдропы", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back",callback_data="rm")]]))

# ADMIN & PROMO
@router.callback_query(F.data=="ep")
async def ep(c: CallbackQuery, s: FSMContext): await c.message.edit_text("Code:"); await s.set_state(PS.CODE)
@router.message(PS.CODE)
async def epc(m: Message, s: FSMContext):
    d=await db.use_promo(m.from_user.id, m.text); await m.answer(f"✅ +{d}d" if d else "❌"); await s.clear()
@router.callback_query(F.data=="ad")
async def ad(c: CallbackQuery): await c.message.edit_text("Adm", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Add Promo",callback_data="ap_add"),InlineKeyboardButton(text="Grant",callback_data="ap_gr")],[InlineKeyboardButton(text="Back",callback_data="menu")]]))
@router.callback_query(F.data=="ap_add")
async def ap_add(c: CallbackQuery, s: FSMContext): await c.message.edit_text("Days:"); await s.set_state(ADS.DAYS)
@router.message(ADS.DAYS)
async def ap_d(m: Message, s: FSMContext): await s.update_data(d=int(m.text)); await m.answer("Acts:"); await s.set_state(ADS.ACT)
@router.message(ADS.ACT)
async def ap_a(m: Message, s: FSMContext): d=await s.get_data(); c=await db.create_promo(d['d'],int(m.text)); await m.answer(f"<code>{c}</code>"); await s.clear()
@router.callback_query(F.data=="ap_gr")
async def ap_gr(c: CallbackQuery, s: FSMContext): await c.message.edit_text("ID:"); await s.set_state(ADS.GID)
@router.message(ADS.GID)
async def ap_gi(m: Message, s: FSMContext): await s.update_data(i=m.text); await m.answer("Days:"); await s.set_state(ADS.GD)
@router.message(ADS.GD)
async def ap_gd(m: Message, s: FSMContext): d=await s.get_data(); await db.update_sub(int(d['i']),int(m.text)); await m.answer("✅"); await s.clear()

@router.callback_query(F.data=="pr")
async def pr(c: CallbackQuery):
    if c.from_user.id==ADMIN_ID: s="∞ Admin"
    else: u=await db.get_user(c.from_user.id); d=datetime.fromisoformat(u['sub_end']) if u else None; s=d.strftime("%d.%m.%Y") if d and d>datetime.now() else "No"
    await c.message.edit_text(f"ID: {c.from_user.id}\nSub: {s}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back",callback_data="menu")]]))

async def main():
    await db.init()
    for f in SESSION_DIR.glob("*.session"): 
        if f.stat().st_size==0: f.unlink()
    for f in SESSION_DIR.glob("session_*.session"):
        try: 
            uid=int(f.stem.split("_")[1])
            if await db.check_sub(uid): await start_worker(uid)
        except: pass
    await bot.delete_webhook(drop_pending_updates=True); await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
