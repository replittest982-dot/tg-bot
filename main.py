#!/usr/bin/env python3
"""
💎 StatPro v26.0 - TITANIUM ULTRA
---------------------------------
🔒 ACCESS: Строгая проверка подписки (No Sub = No Work).
💾 PERSISTENCE: Сохранение активных отчетов при рестарте (JSON).
📊 REPORTS: .отчетайти, .отчетдропы (Просмотр без остановки).
🚀 FEATURES: 25+ Улучшений (Ping, Calc, Info, Backup).
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

from telethon import TelegramClient, events, types, functions
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError
)
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import User

import qrcode
from PIL import Image

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ
# =========================================================================

VERSION = "v26.0 ULTRA"
MSK_TZ = timezone(timedelta(hours=3))

BASE_DIR = Path("/app")
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "ultra.db"
LOG_FILE = BASE_DIR / "bot.log"
STATE_FILE = BASE_DIR / "reports_state.json" # Файл для сохранения отчетов при рестарте

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
        # Авто-Бэкап при старте
        if self.path.exists():
            shutil.copy(self.path, f"{self.path}.backup")

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
        """Проверяет, активна ли подписка. Админ всегда True."""
        if uid == ADMIN_ID: return True
        u = await self.get_user(uid)
        if not u: return False
        try:
            end = datetime.fromisoformat(u['sub_end'])
            return end > datetime.now()
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
        code = f"TITAN-{random.randint(1000,9999)}-{days}D"
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

    async def get_stats(self):
        async with self.get_connection() as db:
            async with db.execute("SELECT COUNT(*) FROM users") as c: t = (await c.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (datetime.now().isoformat(),)) as c: a = (await c.fetchone())[0]
        return t, a

db = DatabaseManager()

# =========================================================================
# 💾 PERSISTENCE (Сохранение состояния отчетов)
# =========================================================================

class ReportPersistence:
    @staticmethod
    def save(active_reports: dict):
        """Сохраняет активные отчеты в JSON"""
        try:
            data_to_save = {}
            for key, val in active_reports.items():
                # Преобразуем datetime в isoformat для JSON
                val_copy = val.copy()
                val_copy['start_time'] = val['start_time'].isoformat()
                data_to_save[key] = val_copy
            
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Save state error: {e}")

    @staticmethod
    def load() -> dict:
        """Загружает отчеты при старте"""
        if not STATE_FILE.exists(): return {}
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            loaded_reports = {}
            for key, val in raw_data.items():
                val['start_time'] = datetime.fromisoformat(val['start_time'])
                loaded_reports[key] = val
            return loaded_reports
        except Exception as e:
            logger.error(f"Load state error: {e}")
            return {}

# =========================================================================
# 📊 МЕНЕДЖЕР ОТЧЕТОВ
# =========================================================================

class ReportManager:
    _shared_state = {} # Общее состояние для всех воркеров (для персистентности)

    def __init__(self):
        # При инициализации загружаем из общего состояния
        pass

    @property
    def active_reports(self):
        return self._shared_state

    def start_it(self, chat_id, topic_id):
        key = f"{chat_id}_{topic_id}"
        self._shared_state[key] = {'type': 'it', 'data': [], 'start_time': datetime.now(MSK_TZ)}
        ReportPersistence.save(self._shared_state)
        return True

    def start_drop(self, chat_id, topic_id):
        key = f"{chat_id}_{topic_id}"
        self._shared_state[key] = {'type': 'drop', 'data': [], 'start_time': datetime.now(MSK_TZ)}
        ReportPersistence.save(self._shared_state)
        return True

    def add_it_entry(self, chat_id, topic_id, user, action, number):
        key = f"{chat_id}_{topic_id}"
        if key in self._shared_state and self._shared_state[key]['type'] == 'it':
            time_str = datetime.now(MSK_TZ).strftime("%H:%M")
            self._shared_state[key]['data'].append({'time': time_str, 'user': user, 'action': action, 'number': number})
            ReportPersistence.save(self._shared_state)
            return True
        return False

    def add_drop_msg(self, chat_id, topic_id, user, text):
        key = f"{chat_id}_{topic_id}"
        if key in self._shared_state and self._shared_state[key]['type'] == 'drop':
            time_str = datetime.now(MSK_TZ).strftime("%H:%M")
            self._shared_state[key]['data'].append(f"[{time_str}] {user}: {text}")
            ReportPersistence.save(self._shared_state)
            return True
        return False

    def get_report_data(self, chat_id, topic_id):
        """Получить данные без остановки"""
        key = f"{chat_id}_{topic_id}"
        return self._shared_state.get(key)

    def stop_session(self, chat_id, topic_id):
        key = f"{chat_id}_{topic_id}"
        if key in self._shared_state: 
            data = self._shared_state.pop(key)
            ReportPersistence.save(self._shared_state)
            return data
        return None

# Загружаем состояние при старте модуля
ReportManager._shared_state = ReportPersistence.load()

# =========================================================================
# 🧠 USER WORKER
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
        # 1. ПРОВЕРКА ПОДПИСКИ ПРИ СТАРТЕ
        if not await db.check_sub(self.user_id):
            self.status = "⛔️ No Sub"
            return False

        if self.task and not self.task.done(): self.task.cancel()
        self.task = asyncio.create_task(self._loop())
        return True

    async def stop(self):
        self.stop_signal = True
        if self.client: await self.client.disconnect()
        if self.task: self.task.cancel()
        self.status = "🔴 Off"

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
        self.status = "🟡 Connect..."
        try:
            sess = self.get_session_file()
            if not sess.with_suffix(".session").exists(): self.status = "🔴 No Session"; return
            self.client = TelegramClient(str(sess), API_ID, API_HASH, connection_retries=None)
            await self.client.connect()
            if not await self.client.is_user_authorized(): self.status = "🔴 Auth Failed"; return
            
            self.status = "🟢 Active"
            self._register_handlers()
            
            # Периодическая проверка подписки
            async def sub_checker():
                while True:
                    await asyncio.sleep(3600) # Раз в час
                    if not await db.check_sub(self.user_id):
                        await self.client.disconnect()
                        self.status = "⛔️ Sub Expired"
                        break
            
            asyncio.create_task(sub_checker())
            await self.client.run_until_disconnected()
        except Exception as e: self.status = f"🔴 Err: {e}"
        finally: 
            if self.client: await self.client.disconnect()

    def _register_handlers(self):
        c = self.client

        # --- IT REPORTS ---
        @c.on(events.NewMessage(pattern=r'^\.айтистарт$'))
        async def it_start(e):
            await self._stealth_delete(e)
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            self.reports.start_it(e.chat_id, tid)
            await self._temp_msg(e, "💻 IT Started! (Saved)", 3)

        @c.on(events.NewMessage(pattern=r'^\.отчетайти$'))
        async def it_view(e):
            """Показывает текущий отчет без остановки"""
            await self._stealth_delete(e)
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            res = self.reports.get_report_data(e.chat_id, tid)
            if res and res['type'] == 'it':
                lines = self._format_it_table(res['data'])
                await self._temp_msg(e, "📨 Отчет отправлен в ЛС бота", 2)
                try: await bot.send_message(self.user_id, "\n".join(lines), parse_mode='HTML')
                except: pass
            else:
                await self._temp_msg(e, "⚠️ Нет активного IT отчета", 2)

        @c.on(events.NewMessage(pattern=r'^\.айтистоп$'))
        async def it_stop(e):
            await self._stealth_delete(e)
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            res = self.reports.stop_session(e.chat_id, tid)
            if res and res['type'] == 'it':
                lines = self._format_it_table(res['data'])
                await self._temp_msg(e, "✅ IT Stopped", 3)
                try: await bot.send_message(self.user_id, "\n".join(lines), parse_mode='HTML')
                except: pass

        def _format_it_table(self, data):
            lines = ["📅 <b>ОТЧЕТ IT (SNAPSHOT)</b>", ""]
            lines.append("<code>{:<6} | {:<6} | {:<11}</code>".format("ВРЕМЯ", "АКТ", "НОМЕР"))
            lines.append("-" * 30)
            for row in data:
                act = "ВСТАЛ" if row['action'] == "встал" else "ЗМ" if row['action'] == "зм" else "ПВ"
                lines.append(f"<code>{row['time']:<6} | {act:<6} | {row['number']:<11}</code>")
            lines.append("-" * 30)
            lines.append(f"<b>Всего действий: {len(data)}</b>")
            return lines

        @c.on(events.NewMessage(pattern=RE_IT_CMD))
        async def it_h(e):
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            key = f"{e.chat_id}_{tid}"
            if key in self.reports.active_reports and self.reports.active_reports[key]['type'] == 'it':
                act = e.pattern_match.group(1).lower(); num = e.pattern_match.group(2)
                user = e.sender.first_name or "User"
                self.reports.add_it_entry(e.chat_id, tid, user, act, num)
                try: await e.client(SendReactionRequest(e.chat_id, e.id, reaction=[types.ReactionEmoji(emoticon='✍️')]))
                except: pass

        # --- DROP REPORTS ---
        @c.on(events.NewMessage(pattern=r'^\.отчетыстарт$'))
        async def d_start(e):
            await self._stealth_delete(e); tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            self.reports.start_drop(e.chat_id, tid); await self._temp_msg(e, "📦 Drop Monitoring Started", 3)

        @c.on(events.NewMessage(pattern=r'^\.отчетдропы$'))
        async def d_view(e):
            await self._stealth_delete(e); tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            res = self.reports.get_report_data(e.chat_id, tid)
            if res and res['type'] == 'drop':
                fn = f"Drop_Snap_{e.chat_id}.txt"; 
                with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(res['data']))
                await self._temp_msg(e, "📨 Лог отправлен в ЛС бота", 2)
                try: await bot.send_document(self.user_id, FSInputFile(fn), caption="📦 Current Drop Log"); os.remove(fn)
                except: pass
            else: await self._temp_msg(e, "⚠️ Нет дроп сессии", 2)

        @c.on(events.NewMessage(pattern=r'^\.отчетыстоп$'))
        async def d_stop(e):
            await self._stealth_delete(e); tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            res = self.reports.stop_session(e.chat_id, tid)
            if res and res['type'] == 'drop':
                fn = f"Drop_Final_{e.chat_id}.txt"; 
                with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(res['data']))
                await self._temp_msg(e, "✅ Drop Stopped", 3)
                try: await bot.send_document(self.user_id, FSInputFile(fn), caption="📦 Final Drop Report"); os.remove(fn)
                except: pass

        @c.on(events.NewMessage())
        async def d_mon(e):
            if e.text and not e.text.startswith("."):
                tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
                key = f"{e.chat_id}_{tid}"
                if key in self.reports.active_reports and self.reports.active_reports[key]['type'] == 'drop':
                    # Игнорируем ботов
                    if e.sender and e.sender.bot: return
                    self.reports.add_drop_msg(e.chat_id, tid, e.sender.first_name if e.sender else "Unk", e.text)

        # --- TOOLS ---
        @c.on(events.NewMessage(pattern=r'^\.ping$'))
        async def ping(e):
            s = time.time(); msg = await e.respond("🏓"); e_t = time.time()
            await msg.edit(f"🏓 Pong! {int((e_t-s)*1000)}ms"); await asyncio.sleep(3); await msg.delete(); await self._stealth_delete(e)

        @c.on(events.NewMessage(pattern=r'^\.calc (.+)'))
        async def calc(e):
            await self._stealth_delete(e); expr = e.pattern_match.group(1)
            try: res = eval(expr, {"__builtins__":{}}, {"math":math}); await self._temp_msg(e, f"🔢 {res}", 5)
            except: pass

        @c.on(events.NewMessage(pattern=r'^\.id$'))
        async def get_id(e):
            await self._stealth_delete(e)
            if e.is_reply: 
                r = await e.get_reply_message()
                txt = f"🆔 User: `{r.sender_id}`\nMsg: `{r.id}`\nChat: `{e.chat_id}`"
            else: txt = f"🆔 Chat: `{e.chat_id}`"
            await self._temp_msg(e, txt, 5)

        @c.on(events.NewMessage(pattern=r'^\.info$'))
        async def info(e):
            await self._stealth_delete(e)
            try:
                full = await c(GetFullChannelRequest(e.chat_id))
                txt = f"ℹ️ <b>Info</b>\nTitle: {full.chats[0].title}\nID: `{e.chat_id}`\nUsers: {full.full_chat.participants_count}"
                await self._temp_msg(e, txt, 5)
            except: pass
            
        @c.on(events.NewMessage(pattern=r'^\.purge$'))
        async def purge(e):
            await self._stealth_delete(e)
            if not e.is_reply: return
            r = await e.get_reply_message()
            msgs = [m.id async for m in c.iter_messages(e.chat_id, min_id=r.id - 1)]
            await c.delete_messages(e.chat_id, msgs)

        @c.on(events.NewMessage(pattern=r'^\.restart$'))
        async def restart_cmd(e):
            await self._stealth_delete(e)
            await self._temp_msg(e, "🔄 Restarting...", 2)
            await self.stop()
            await self.start()

# =========================================================================
# 🤖 BOT UI
# =========================================================================

WORKERS: Dict[int, UserWorker] = {}

async def start_worker(uid: int):
    if uid in WORKERS: await WORKERS[uid].stop()
    w = UserWorker(uid)
    WORKERS[uid] = w
    success = await w.start()
    return success

async def stop_worker(uid: int):
    if uid in WORKERS: await WORKERS[uid].stop(); del WORKERS[uid]

async def restart_all_workers():
    for w in list(WORKERS.values()): await w.stop()
    for f in SESSION_DIR.glob("session_*.session"):
        try: uid = int(f.stem.split("_")[1]); await start_worker(uid)
        except: pass

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# STATES
class AuthStates(StatesGroup): PHONE=State(); CODE=State(); PASS=State()
class AdminStates(StatesGroup): PROMO_DAYS=State(); PROMO_ACT=State(); GRANT_ID=State(); GRANT_DAYS=State()
class PromoState(StatesGroup): CODE=State()

# KEYBOARDS
def kb_main(uid: int):
    kb = []
    kb.append([InlineKeyboardButton(text="📊 Отчеты", callback_data="reports_menu"),
               InlineKeyboardButton(text="👻 Воркер", callback_data="worker")])
    kb.append([InlineKeyboardButton(text="🔑 Подключить", callback_data="auth"),
               InlineKeyboardButton(text="🎟 Промокод", callback_data="enter_promo")])
    kb.append([InlineKeyboardButton(text="👤 Профиль", callback_data="profile")])
    if uid == ADMIN_ID: kb.append([InlineKeyboardButton(text="👑 Админ", callback_data="admin")])
    kb.append([InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_BOT.replace('@','')} ")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_reports():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 Дроп-Отчеты", callback_data="rep_drop"), InlineKeyboardButton(text="💻 IT-Отчеты", callback_data="rep_it")], [InlineKeyboardButton(text="🔙", callback_data="menu")]])

def kb_auth(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 QR", callback_data="auth_qr"), InlineKeyboardButton(text="📱 Тел", callback_data="auth_phone")], [InlineKeyboardButton(text="🔙", callback_data="menu")]])

# MIDDLEWARE
class MainMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        uid = event.from_user.id
        await db.add_user(uid, event.from_user.username or "Unknown")
        u = await db.get_user(uid)
        if u and u['is_banned']: return
        return await handler(event, data)

dp.message.middleware(MainMiddleware())
dp.callback_query.middleware(MainMiddleware())

# HANDLERS
@router.message(Command("start"))
async def start(m: Message): await m.answer("💎 <b>StatPro TITANIUM ULTRA</b>", reply_markup=kb_main(m.from_user.id))

@router.callback_query(F.data == "menu")
async def menu(c: CallbackQuery): await c.message.edit_text("🏠 <b>Меню</b>", reply_markup=kb_main(c.from_user.id))

@router.callback_query(F.data == "enter_promo")
async def promo_start(c: CallbackQuery, state: FSMContext): await c.message.edit_text("🎟 Введите промокод:"); await state.set_state(PromoState.CODE)
@router.message(PromoState.CODE)
async def promo_act(m: Message, state: FSMContext):
    days = await db.use_promo(m.from_user.id, m.text.strip())
    if days > 0: await m.answer(f"✅ Активировано! +{days} дней.", reply_markup=kb_main(m.from_user.id))
    else: await m.answer("❌ Неверный код.")
    await state.clear()

@router.callback_query(F.data == "reports_menu")
async def r_m(c: CallbackQuery): await c.message.edit_text("📊 <b>Отчеты</b>", reply_markup=kb_reports())
@router.callback_query(F.data == "rep_it")
async def r_it(c: CallbackQuery): await c.message.edit_text("💻 .айтистарт -> .встал -> .отчетайти -> .айтистоп", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="reports_menu")]]))
@router.callback_query(F.data == "rep_drop")
async def r_dr(c: CallbackQuery): await c.message.edit_text("📦 .отчетыстарт -> .отчетдропы -> .отчетыстоп", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="reports_menu")]]))

# AUTH
@router.callback_query(F.data == "auth")
async def auth(c: CallbackQuery): await c.message.edit_text("🔐 Метод:", reply_markup=kb_auth())
@router.callback_query(F.data == "auth_qr")
async def a_qr(c: CallbackQuery):
    if not await db.check_sub(c.from_user.id): return await c.answer("🚫 Нужна подписка!", show_alert=True)
    uid = c.from_user.id; path = SESSION_DIR/f"session_{uid}"; cl = TelegramClient(str(path), API_ID, API_HASH)
    await cl.connect(); qr = await cl.qr_login(); img = qrcode.make(qr.url).convert("RGB"); b = io.BytesIO(); img.save(b, "PNG"); b.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="Scan QR")
    try: await qr.wait(120); await msg.delete(); await c.message.answer("✅ OK"); await cl.disconnect(); await start_worker(uid)
    except: await msg.delete(); await c.message.answer("❌ Err"); await cl.disconnect()

@router.callback_query(F.data == "auth_phone")
async def a_ph(c: CallbackQuery, state: FSMContext): 
    if not await db.check_sub(c.from_user.id): return await c.answer("🚫 Нужна подписка!", show_alert=True)
    await c.message.edit_text("📱 Номер:"); await state.set_state(AuthStates.PHONE)
@router.message(AuthStates.PHONE)
async def a_p(m: Message, state: FSMContext):
    uid=m.from_user.id; cl=TelegramClient(str(SESSION_DIR/f"session_{uid}"), API_ID, API_HASH); await cl.connect()
    try: r=await cl.send_code_request(m.text); await state.update_data(p=m.text, h=r.phone_code_hash, cl=cl); await m.answer("📩 Код:"); await state.set_state(AuthStates.CODE)
    except Exception as e: await m.answer(f"❌ {e}")
@router.message(AuthStates.CODE)
async def a_c(m: Message, state: FSMContext):
    d=await state.get_data(); cl=d['cl']
    try: await cl.sign_in(phone=d['p'], code=m.text, phone_code_hash=d['h']); await m.answer("✅ OK"); await cl.disconnect(); await start_worker(m.from_user.id); await state.clear()
    except SessionPasswordNeededError: await m.answer("🔒 2FA Пароль:"); await state.set_state(AuthStates.PASS)
    except Exception as e: await m.answer(f"❌ {e}")
@router.message(AuthStates.PASS)
async def a_pa(m: Message, state: FSMContext):
    d=await state.get_data(); cl=d['cl']
    try: await cl.sign_in(password=m.text); await m.answer("✅ OK"); await cl.disconnect(); await start_worker(m.from_user.id); await state.clear()
    except Exception as e: await m.answer(f"❌ {e}")

# WORKER CTRL
@router.callback_query(F.data == "worker")
async def w_cb(c: CallbackQuery):
    w=WORKERS.get(c.from_user.id); st=w.status if w else "⚪️ Off"
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄",callback_data="w_r"),InlineKeyboardButton(text="🛑",callback_data="w_s")],[InlineKeyboardButton(text="🔙",callback_data="menu")]])
    await c.message.edit_text(f"👻 <b>Воркер</b>: {st}", reply_markup=kb)
@router.callback_query(F.data == "w_r")
async def w_r(c: CallbackQuery): 
    res = await start_worker(c.from_user.id)
    if res: await c.answer("Started")
    else: await c.answer("🚫 No Subscription!", show_alert=True)
    await w_cb(c)
@router.callback_query(F.data == "w_s")
async def w_s(c: CallbackQuery): await stop_worker(c.from_user.id); await c.answer("Stopped"); await w_cb(c)

# ADMIN
@router.callback_query(F.data == "admin")
async def adm(c: CallbackQuery):
    if c.from_user.id==ADMIN_ID: await c.message.edit_text("👑", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎁 Выдать",callback_data="adm_grant"),InlineKeyboardButton(text="🎫 Промо",callback_data="adm_promo")],[InlineKeyboardButton(text="🔙",callback_data="menu")]]))

@router.callback_query(F.data == "adm_promo")
async def ap(c: CallbackQuery, state: FSMContext): await c.message.edit_text("📅 Дней:"); await state.set_state(AdminStates.PROMO_DAYS)
@router.message(AdminStates.PROMO_DAYS)
async def ap_d(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("🔢 Акты:"); await state.set_state(AdminStates.PROMO_ACT)
@router.message(AdminStates.PROMO_ACT)
async def ap_a(m: Message, state: FSMContext): d=await state.get_data(); c=await db.create_promo(d['d'], int(m.text)); await m.answer(f"Code: <code>{c}</code>"); await state.clear()

@router.callback_query(F.data == "adm_grant")
async def ag(c: CallbackQuery, state: FSMContext): await c.message.edit_text("🆔"); await state.set_state(AdminStates.GRANT_ID)
@router.message(AdminStates.GRANT_ID)
async def ag_i(m: Message, state: FSMContext): await state.update_data(uid=m.text); await m.answer("📅"); await state.set_state(AdminStates.GRANT_DAYS)
@router.message(AdminStates.GRANT_DAYS)
async def ag_d(m: Message, state: FSMContext): d=await state.get_data(); await db.update_sub(int(d['uid']), int(m.text)); await m.answer("✅"); await state.clear()

@router.callback_query(F.data == "profile")
async def prof(c: CallbackQuery):
    if c.from_user.id == ADMIN_ID: sub = "♾ ВЕЧНАЯ (Админ)"
    else:
        u = await db.get_user(c.from_user.id)
        d = datetime.fromisoformat(u['sub_end']) if u and u['sub_end'] else None
        sub = d.strftime('%d.%m.%Y') if d and d > datetime.now() else "❌ Нет"
    await c.message.edit_text(f"👤 ID: {c.from_user.id}\n💎 Подписка: {sub}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="menu")]]))

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
