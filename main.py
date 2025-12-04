#!/usr/bin/env python3
"""
🪐 StatPro v23.1 - COMPLETE EDITION
-----------------------------------
✅ АВТОРИЗАЦИЯ: QR-код, Телефон, 2FA.
✅ ОТЧЕТЫ: IT (Таблицы, МСК время) + Дропы (Мониторинг топиков).
✅ ФУНКЦИИ: .ban, .mute, .spam, .afk, .scan, .id и др.
✅ АДМИНКА: Промокоды, Выдача подписки, Рестарт.
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
from typing import Dict, Optional, List
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
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError,
    ChatAdminRequiredError, UserNotParticipantError
)
from telethon.tl.types import (
    ChannelParticipantsAdmins, ChatBannedRights, User
)
from telethon.tl.functions.channels import (
    EditAdminRequest, JoinChannelRequest, LeaveChannelRequest, 
    GetFullChannelRequest
)
from telethon.tl.functions.messages import SendReactionRequest

import qrcode
from PIL import Image

# =========================================================================
# ⚙️ НАСТРОЙКИ
# =========================================================================

VERSION = "v23.1 COMPLETE"
START_TS = datetime.now().timestamp()
MSK_TZ = timezone(timedelta(hours=3))

BASE_DIR = Path("/app")
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "database.db"
LOG_FILE = BASE_DIR / "bot.log"

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

    async def update_sub(self, uid: int, days: int):
        u = await self.get_user(uid)
        curr = datetime.fromisoformat(u['sub_end']) if u else datetime.now()
        if curr < datetime.now(): curr = datetime.now()
        new_end = curr + timedelta(days=days)
        async with self.get_connection() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, sub_end) VALUES (?, ?)", (uid, datetime.now().isoformat()))
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), uid))
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
            async with db.execute("SELECT COUNT(*) FROM users") as c: t = (await c.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (datetime.now().isoformat(),)) as c: a = (await c.fetchone())[0]
        return t, a

db = DatabaseManager()

# =========================================================================
# 📊 МЕНЕДЖЕР ОТЧЕТОВ
# =========================================================================

class ReportManager:
    def __init__(self):
        self.active_reports = {} # {chat_topic: data}

    def start_it(self, chat_id, topic_id):
        key = f"{chat_id}_{topic_id}"
        self.active_reports[key] = {'type': 'it', 'data': [], 'start_time': datetime.now(MSK_TZ)}
        return True

    def start_drop(self, chat_id, topic_id):
        key = f"{chat_id}_{topic_id}"
        self.active_reports[key] = {'type': 'drop', 'data': [], 'start_time': datetime.now(MSK_TZ)}
        return True

    def add_it_entry(self, chat_id, topic_id, user, action, number):
        key = f"{chat_id}_{topic_id}"
        if key in self.active_reports and self.active_reports[key]['type'] == 'it':
            time_str = datetime.now(MSK_TZ).strftime("%H:%M")
            self.active_reports[key]['data'].append({'time': time_str, 'user': user, 'action': action, 'number': number})
            return True
        return False

    def add_drop_msg(self, chat_id, topic_id, user, text):
        key = f"{chat_id}_{topic_id}"
        if key in self.active_reports and self.active_reports[key]['type'] == 'drop':
            time_str = datetime.now(MSK_TZ).strftime("%H:%M")
            self.active_reports[key]['data'].append(f"[{time_str}] {user}: {text}")
            return True
        return False

    def stop_session(self, chat_id, topic_id):
        key = f"{chat_id}_{topic_id}"
        if key in self.active_reports: return self.active_reports.pop(key)
        return None

# =========================================================================
# 🧠 ВОРКЕР ЮЗЕРА
# =========================================================================

class UserWorker:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.client: Optional[TelegramClient] = None
        self.task: Optional[asyncio.Task] = None
        self.status = "⚪️ Init"
        self.stop_signal = False
        self.reports = ReportManager()
        self.is_afk = False
        self.afk_reason = ""

    def get_session_file(self) -> Path: return SESSION_DIR / f"session_{self.user_id}"

    async def start(self):
        if self.task and not self.task.done(): self.task.cancel()
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
        self.status = "🟡 Подключение..."
        try:
            sess = self.get_session_file()
            if not sess.with_suffix(".session").exists(): self.status = "🔴 Нет Сессии"; return
            self.client = TelegramClient(str(sess), API_ID, API_HASH, connection_retries=None)
            await self.client.connect()
            if not await self.client.is_user_authorized(): self.status = "🔴 Ошибка Входа"; return
            self.status = "🟢 Активен"
            self._register_handlers()
            await self.client.run_until_disconnected()
        except Exception as e: self.status = f"🔴 Ошибка: {e}"
        finally: 
            if self.client: await self.client.disconnect()

    def _register_handlers(self):
        c = self.client

        # --- IT ОТЧЕТЫ ---
        @c.on(events.NewMessage(pattern=r'^\.айтистарт$'))
        async def it_start(e):
            await self._stealth_delete(e)
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            self.reports.start_it(e.chat_id, tid)
            await self._temp_msg(e, "💻 <b>IT Старт!</b> Команды: .встал, .зм, .пв + номер", 5)

        @c.on(events.NewMessage(pattern=r'^\.айтистоп$'))
        async def it_stop(e):
            await self._stealth_delete(e)
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            res = self.reports.stop_session(e.chat_id, tid)
            if res and res['type'] == 'it':
                lines = ["📅 <b>ОТЧЕТ IT</b>", ""]
                lines.append("<code>{:<8} | {:<6} | {:<10}</code>".format("ВРЕМЯ", "ДЕЙСТВ", "НОМЕР"))
                lines.append("-" * 30)
                for row in res['data']:
                    act = "ВСТАЛ" if row['action'] == "встал" else "ЗМ" if row['action'] == "зм" else "ПВ"
                    lines.append(f"<code>{row['time']:<8} | {act:<6} | {row['number']:<10}</code>")
                TEMP_DATA[self.user_id] = {'lines': lines, 'title': f"IT_{e.chat_id}"}
                await self._temp_msg(e, "✅ IT отчет готов!", 3)
                try: await bot.send_message(self.user_id, "\n".join(lines), parse_mode='HTML')
                except: pass

        @c.on(events.NewMessage(pattern=RE_IT_CMD))
        async def it_handler(e):
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            key = f"{e.chat_id}_{tid}"
            if key in self.reports.active_reports and self.reports.active_reports[key]['type'] == 'it':
                act = e.pattern_match.group(1).lower()
                num = e.pattern_match.group(2)
                user = e.sender.first_name or "User"
                self.reports.add_it_entry(e.chat_id, tid, user, act, num)
                try: await e.client(SendReactionRequest(e.chat_id, e.id, reaction=[types.ReactionEmoji(emoticon='✍️')]))
                except: pass

        # --- ДРОП ОТЧЕТЫ ---
        @c.on(events.NewMessage(pattern=r'^\.отчетыстарт$'))
        async def drop_start(e):
            await self._stealth_delete(e)
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            self.reports.start_drop(e.chat_id, tid)
            await self._temp_msg(e, "📦 <b>Дроп Мониторинг!</b> Паршу всё...", 5)

        @c.on(events.NewMessage(pattern=r'^\.отчетыстоп$'))
        async def drop_stop(e):
            await self._stealth_delete(e)
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            res = self.reports.stop_session(e.chat_id, tid)
            if res and res['type'] == 'drop':
                fn = f"Drop_{e.chat_id}.txt"
                with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(res['data']))
                await self._temp_msg(e, "✅ Дроп отчет отправлен в ЛС", 3)
                try: await bot.send_document(self.user_id, FSInputFile(fn), caption="📦 Отчет Дропов"); os.remove(fn)
                except: pass

        @c.on(events.NewMessage())
        async def drop_monitor(e):
            if e.text and not e.text.startswith("."):
                tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
                key = f"{e.chat_id}_{tid}"
                if key in self.reports.active_reports and self.reports.active_reports[key]['type'] == 'drop':
                    user = e.sender.first_name if e.sender else "Unknown"
                    self.reports.add_drop_msg(e.chat_id, tid, user, e.text)

        # --- АДМИН/МОДЕРАЦИЯ (Вернули!) ---
        @c.on(events.NewMessage(pattern=r'^\.ban$'))
        async def ban(e):
            await self._stealth_delete(e)
            if not e.is_reply: return
            try:
                r = await e.get_reply_message()
                await c(EditAdminRequest(e.chat_id, r.sender_id, ChatBannedRights(until_date=None, view_messages=True), rank=""))
                await self._temp_msg(e, "⛔ Забанен", 2)
            except: pass

        @c.on(events.NewMessage(pattern=r'^\.mute (\d+)([mhd])$'))
        async def mute(e):
            await self._stealth_delete(e)
            if not e.is_reply: return
            args = e.pattern_match
            val, unit = int(args.group(1)), args.group(2)
            td = timedelta(minutes=val) if unit=='m' else timedelta(hours=val) if unit=='h' else timedelta(days=val)
            try:
                r = await e.get_reply_message()
                await c(EditAdminRequest(e.chat_id, r.sender_id, ChatBannedRights(until_date=datetime.now()+td, send_messages=True), rank=""))
                await self._temp_msg(e, "😶 Мут", 3)
            except: pass

        @c.on(events.NewMessage(pattern=r'^\.purge$'))
        async def purge(e):
            await self._stealth_delete(e)
            if not e.is_reply: return
            r = await e.get_reply_message()
            msgs = []
            async for m in c.iter_messages(e.chat_id, min_id=r.id - 1): msgs.append(m.id)
            await c.delete_messages(e.chat_id, msgs)

        @c.on(events.NewMessage(pattern=r'^\.spam (\d+) (.*)'))
        async def spam(e):
            await self._stealth_delete(e)
            n, txt = int(e.pattern_match.group(1)), e.pattern_match.group(2)
            for _ in range(n):
                if self.stop_signal: break
                await c.send_message(e.chat_id, txt); await asyncio.sleep(0.3)

        @c.on(events.NewMessage(pattern=r'^\.afk ?(.*)'))
        async def set_afk(e):
            await self._stealth_delete(e)
            self.is_afk = True; self.afk_reason = e.pattern_match.group(1) or "Занят"
            await self._temp_msg(e, f"💤 AFK: {self.afk_reason}", 3)

        @c.on(events.NewMessage(pattern=r'^\.unafk$'))
        async def unafk(e):
            await self._stealth_delete(e)
            self.is_afk = False
            await self._temp_msg(e, "👋 Welcome back!", 3)

        @c.on(events.NewMessage(incoming=True))
        async def afk_handler(e):
            if self.is_afk and e.mentioned: await e.reply(f"💤 Я AFK. {self.afk_reason}")

        # --- ПАРСИНГ ---
        @c.on(events.NewMessage(pattern=r'^\.scan$'))
        async def scan(e):
            await self._stealth_delete(e)
            u_lim = await db.get_user(self.user_id)
            limit = u_lim['parse_limit'] if u_lim else 500
            msg = await e.respond(f"👻 Парсинг... Лимит: {limit}")
            unique = {}
            count = 0
            try:
                async for m in c.iter_messages(e.chat_id, limit=30000):
                    if self.stop_signal: break
                    count += 1
                    if m.sender and isinstance(m.sender, User) and not m.sender.bot:
                        if m.sender_id not in unique: unique[m.sender_id] = f"@{m.sender.username or 'None'} | {m.sender.first_name} | {m.sender_id}"
                    if count % 300 == 0: await msg.edit(f"👻 {count} | Найдено: {len(unique)}")
                    if len(unique) >= limit: break
                
                if not self.stop_signal:
                    TEMP_DATA[self.user_id] = {'lines': list(unique.values()), 'title': str(e.chat_id)}
                    await msg.edit("✅ Готово."); await asyncio.sleep(1); await msg.delete()
                    try: await bot.send_message(self.user_id, f"📁 Результат: {len(unique)}", reply_markup=kb_parse())
                    except: pass
            except: await msg.delete()

        @c.on(events.NewMessage(pattern=r'^\.help$'))
        async def help_cmd(e):
            await self._stealth_delete(e)
            txt = ("💎 <b>StatPro COMPLETE</b>\n"
                   "<b>IT:</b> .айтистарт, .айтистоп, .встал, .зм, .пв\n"
                   "<b>Drop:</b> .отчетыстарт, .отчетыстоп\n"
                   "<b>Mod:</b> .ban, .mute, .purge, .spam\n"
                   "<b>Util:</b> .afk, .unafk, .scan, .id")
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
    if uid in WORKERS: await WORKERS[uid].stop(); del WORKERS[uid]

async def restart_all_workers():
    for w in list(WORKERS.values()): await w.stop()
    for f in SESSION_DIR.glob("session_*.session"):
        try: uid = int(f.stem.split("_")[1]); await start_worker(uid)
        except: pass

# =========================================================================
# 🤖 БОТ ИНТЕРФЕЙС
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthStates(StatesGroup):
    PHONE = State(); CODE = State(); PASS = State()

class AdminStates(StatesGroup):
    PROMO_DAYS = State(); PROMO_ACT = State(); GRANT_ID = State(); GRANT_DAYS = State()

# --- КЛАВИАТУРЫ ---
def kb_main(uid: int, is_sub: bool):
    kb = []
    if is_sub or uid == ADMIN_ID:
        kb.append([InlineKeyboardButton(text="📊 Отчеты", callback_data="reports_menu")])
        kb.append([InlineKeyboardButton(text="👻 Воркер", callback_data="worker")])
    else:
        kb.append([InlineKeyboardButton(text="🔑 Подключить Аккаунт", callback_data="auth")])
    kb.append([InlineKeyboardButton(text="👤 Профиль", callback_data="profile")])
    if uid == ADMIN_ID: kb.append([InlineKeyboardButton(text="👑 Админ", callback_data="admin")])
    kb.append([InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_BOT.replace('@','')} ")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_reports():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Дроп-Отчеты", callback_data="rep_drop"),
         InlineKeyboardButton(text="💻 IT-Отчеты", callback_data="rep_it")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu")]
    ])

def kb_parse():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 TXT", callback_data="dl:txt"), InlineKeyboardButton(text="📑 CSV", callback_data="dl:csv")]
    ])

def kb_auth():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 QR", callback_data="auth_qr"), InlineKeyboardButton(text="📱 Телефон", callback_data="auth_phone")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu")]
    ])

# --- ХЕНДЛЕРЫ ---
class MainMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        uid = event.from_user.id
        await db.add_user(uid, event.from_user.username or "Unknown")
        u = await db.get_user(uid)
        if u and u['is_banned']: return
        return await handler(event, data)

dp.message.middleware(MainMiddleware())
dp.callback_query.middleware(MainMiddleware())

@router.message(Command("start"))
async def start_cmd(m: Message):
    u = await db.get_user(m.from_user.id)
    is_sub = datetime.fromisoformat(u['sub_end']) > datetime.now() if u else False
    await m.answer("🪐 <b>StatPro COMPLETE</b>", reply_markup=kb_main(m.from_user.id, is_sub))

@router.callback_query(F.data == "menu")
async def menu_cb(c: CallbackQuery):
    u = await db.get_user(c.from_user.id)
    is_sub = datetime.fromisoformat(u['sub_end']) > datetime.now()
    await c.message.edit_text("🏠 <b>Меню</b>", reply_markup=kb_main(c.from_user.id, is_sub))

@router.callback_query(F.data == "reports_menu")
async def rep_menu(c: CallbackQuery):
    await c.message.edit_text("📊 <b>Отчеты</b>", reply_markup=kb_reports())

@router.callback_query(F.data == "rep_it")
async def rep_it_info(c: CallbackQuery):
    await c.message.edit_text("💻 <b>IT:</b> .айтистарт -> .встал/.зм/.пв 123 -> .айтистоп", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="reports_menu")]]))

@router.callback_query(F.data == "rep_drop")
async def rep_drop_info(c: CallbackQuery):
    await c.message.edit_text("📦 <b>Drop:</b> .отчетыстарт -> .отчетыстоп", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="reports_menu")]]))

@router.callback_query(F.data == "auth")
async def auth_start(c: CallbackQuery): await c.message.edit_text("🔐 Метод:", reply_markup=kb_auth())

@router.callback_query(F.data == "auth_qr")
async def auth_qr(c: CallbackQuery):
    uid = c.from_user.id
    path = SESSION_DIR / f"session_{uid}"
    client = TelegramClient(str(path), API_ID, API_HASH)
    await client.connect()
    qr = await client.qr_login()
    qr_img = qrcode.make(qr.url).convert("RGB")
    b = io.BytesIO(); qr_img.save(b, "PNG"); b.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="📸 Scan QR")
    try: await qr.wait(timeout=120); await msg.delete(); await c.message.answer("✅ Вход!"); await client.disconnect(); await start_worker(uid)
    except: await msg.delete(); await c.message.answer("❌ Ошибка"); await client.disconnect()

@router.callback_query(F.data == "auth_phone")
async def auth_ph(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 Введите номер:")
    await state.set_state(AuthStates.PHONE)

@router.message(AuthStates.PHONE)
async def ph_h(m: Message, state: FSMContext):
    uid = m.from_user.id
    path = SESSION_DIR / f"session_{uid}"
    client = TelegramClient(str(path), API_ID, API_HASH)
    await client.connect()
    try:
        r = await client.send_code_request(m.text)
        await state.update_data(ph=m.text, h=r.phone_code_hash, cl=client)
        await m.answer("📩 Код из Telegram:")
        await state.set_state(AuthStates.CODE)
    except Exception as e: await m.answer(f"❌ {e}")

@router.message(AuthStates.CODE)
async def co_h(m: Message, state: FSMContext):
    d = await state.get_data()
    client: TelegramClient = d['cl']
    try:
        await client.sign_in(phone=d['ph'], code=m.text, phone_code_hash=d['h'])
        await m.answer("✅ Вход!"); await client.disconnect(); await start_worker(m.from_user.id); await state.clear()
    except SessionPasswordNeededError:
        await m.answer("🔒 Пароль 2FA:")
        await state.set_state(AuthStates.PASS)
    except Exception as e: await m.answer(f"❌ {e}")

@router.message(AuthStates.PASS)
async def pa_h(m: Message, state: FSMContext):
    d = await state.get_data()
    client: TelegramClient = d['cl']
    try:
        await client.sign_in(password=m.text)
        await m.answer("✅ Вход!"); await client.disconnect(); await start_worker(m.from_user.id); await state.clear()
    except Exception as e: await m.answer(f"❌ {e}")

@router.callback_query(F.data == "worker")
async def worker_cb(c: CallbackQuery):
    w = WORKERS.get(c.from_user.id)
    st = w.status if w else "⚪️ Off"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Рестарт", callback_data="w_res"), InlineKeyboardButton(text="🛑 Стоп", callback_data="w_stop")], [InlineKeyboardButton(text="🔙", callback_data="menu")]])
    await c.message.edit_text(f"👻 <b>Воркер</b>\nСтатус: {st}", reply_markup=kb)

@router.callback_query(F.data == "w_res")
async def w_r(c: CallbackQuery): await start_worker(c.from_user.id); await c.answer("Рестарт"); await worker_cb(c)
@router.callback_query(F.data == "w_stop")
async def w_s(c: CallbackQuery): await stop_worker(c.from_user.id); await c.answer("Стоп"); await worker_cb(c)

@router.callback_query(F.data == "admin")
async def adm(c: CallbackQuery):
    if c.from_user.id == ADMIN_ID: await c.message.edit_text("👑 Админ", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎁 Выдать", callback_data="adm_grant"), InlineKeyboardButton(text="🎫 Промо", callback_data="adm_promo")], [InlineKeyboardButton(text="🔙", callback_data="menu")]]))

@router.callback_query(F.data == "adm_grant")
async def ag(c: CallbackQuery, state: FSMContext): await c.message.edit_text("🆔 ID:"); await state.set_state(AdminStates.GRANT_ID)
@router.message(AdminStates.GRANT_ID)
async def ag_i(m: Message, state: FSMContext): await state.update_data(uid=m.text); await m.answer("📅 Дней:"); await state.set_state(AdminStates.GRANT_DAYS)
@router.message(AdminStates.GRANT_DAYS)
async def ag_d(m: Message, state: FSMContext): d=await state.get_data(); await db.update_sub(int(d['uid']), int(m.text)); await m.answer("✅"); await state.clear()

@router.callback_query(F.data.startswith("dl:"))
async def dl(c: CallbackQuery):
    fmt = c.data.split(":")[1]; data = TEMP_DATA.get(c.from_user.id)
    if not data: return await c.answer("Нет данных")
    lines = data['lines']; fn = f"Res.{fmt}"
    with open(fn, "w", encoding="utf-8") as f:
        if fmt=="txt": f.write("\n".join(lines))
        else: w = csv.writer(f); w.writerow(["Data"]); [w.writerow([l]) for l in lines]
    await c.message.answer_document(FSInputFile(fn)); os.remove(fn); await c.answer()

# --- MAIN ---
async def main():
    await db.init()
    for f in SESSION_DIR.glob("*.session"): 
        if f.stat().st_size == 0: f.unlink()
    asyncio.create_task(restart_all_workers())
    try: await bot.delete_webhook(drop_pending_updates=True); await dp.start_polling(bot)
    finally: await bot.session.close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except: pass
