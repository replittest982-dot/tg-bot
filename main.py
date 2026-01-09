#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro v78.0 - HYBRID EDITION
---------------------------------
Сборка: Auth Core v75 (Stable) + Features v77 (Ultimate)
Статус: FIXED LOGIN
"""

import asyncio
import logging
import os
import io
import random
import time
import qrcode
import aiosqlite
import csv
import sys
import re
import json
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

# --- ВНЕШНИЕ БИБЛИОТЕКИ ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, 
    Message, BufferedInputFile
)
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties

from telethon import TelegramClient, events, types, Button
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, 
    UserPrivacyRestrictedError, UserDeactivatedError, 
    PeerIdInvalidError, ChatWriteForbiddenError
)

# --- ИИ ЯДРО (G4F) ---
try:
    from g4f.client import AsyncClient
    import g4f
    g4f.debug.logging = False
except ImportError:
    os.system("pip install -U g4f[all] curl_cffi aiohttp")
    from g4f.client import AsyncClient
    import g4f

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ
# =========================================================================

class NumberStatus(Enum):
    WAITING = "waiting"
    CODE_SENT = "code_sent"
    CODE_RECEIVED = "code_received"
    PHOTO_REQUESTED = "photo_requested"
    PHOTO_RECEIVED = "photo_received"
    COMPLETED = "completed"
    FAILED = "failed"

class WorkerStatus(Enum):
    OFFLINE = "offline"
    ONLINE = "online"
    WORKING = "working"
    ERROR = "error"

@dataclass
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_v78.db"
    TEMP_DIR: Path = BASE_DIR / "temp"
    REPORTS_DIR: Path = BASE_DIR / "reports"
    
    # Лимиты
    MAX_WORKERS: int = 10
    
    # Эмуляция (Стандартная, как в v75)
    DEVICE_MODEL: str = "iPhone 15 Pro Max"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        self.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ ОШИБКА: Заполни переменные окружения!")
            sys.exit(1)

cfg = Config()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("StatPro_v78")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ (ОТ v77 - ЧТОБЫ РАБОТАЛИ ВСЕ ФУНКЦИИ)
# =========================================================================

class Database:
    def __init__(self): self.path = cfg.DB_PATH
    def get_conn(self): return aiosqlite.connect(self.path)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            # Таблица пользователей
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    username TEXT, 
                    first_name TEXT,
                    sub_end INTEGER, 
                    joined_at INTEGER
                )
            """)
            # Таблица номеров (для .u .v)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS numbers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    worker_id INTEGER,
                    status TEXT DEFAULT 'waiting',
                    created_at INTEGER,
                    code_received_at INTEGER,
                    photo_received_at INTEGER,
                    completed_at INTEGER,
                    error_message TEXT
                )
            """)
            # Промокоды
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY, days INTEGER, activations INTEGER
                )
            """)
            await db.commit()

    async def check_sub_bool(self, uid: int) -> bool:
        if uid == cfg.ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                return r[0] > int(time.time()) if (r and r[0]) else False

    async def upsert_user(self, uid: int, uname: str, fname: str = ""):
        now = int(time.time())
        uname = uname or "Unknown"
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, first_name, sub_end, joined_at) VALUES (?, ?, ?, 0, ?)", (uid, uname, fname, now))
            await db.execute("UPDATE users SET username = ?, first_name = ? WHERE user_id = ?", (uname, fname, uid))
            await db.commit()

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ?", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            
            # Продление
            now = int(time.time())
            await db.execute("INSERT OR IGNORE INTO users (user_id, sub_end, joined_at) VALUES (?, 0, ?)", (uid, now))
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c2:
                row = await c2.fetchone()
                curr = row[0] if (row and row[0]) else 0
            new_end = (curr if curr > now else now) + (days * 86400)
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"TITAN-{random.randint(100,999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

    async def get_user_info(self, uid: int):
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end, joined_at FROM users WHERE user_id = ?", (uid,)) as c:
                return await c.fetchone()

    # --- МЕТОДЫ ДЛЯ НОМЕРОВ (ИЗ v77) ---
    async def add_number(self, phone: str, user_id: int) -> bool:
        try:
            async with self.get_conn() as db:
                await db.execute("INSERT INTO numbers (phone, user_id, created_at, status) VALUES (?, ?, ?, ?)", 
                                 (phone, user_id, int(time.time()), NumberStatus.WAITING.value))
                await db.commit()
            return True
        except: return False

    async def get_available_number(self, worker_id: int) -> Optional[str]:
        async with self.get_conn() as db:
            async with db.execute("SELECT phone, id FROM numbers WHERE status=? AND worker_id IS NULL ORDER BY created_at ASC LIMIT 1", (NumberStatus.WAITING.value,)) as c:
                row = await c.fetchone()
                if row:
                    await db.execute("UPDATE numbers SET worker_id=?, status=? WHERE id=?", (worker_id, NumberStatus.PHOTO_REQUESTED.value, row[1]))
                    await db.commit()
                    return row[0]
        return None

    async def update_number_status(self, phone: str, status: NumberStatus):
        async with self.get_conn() as db:
            await db.execute("UPDATE numbers SET status=? WHERE phone=?", (status.value, phone))
            await db.commit()

    async def get_user_stats(self, uid: int):
        async with self.get_conn() as db:
            async with db.execute("SELECT COUNT(*), SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END), SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) FROM numbers WHERE user_id=?", (uid,)) as c:
                r = await c.fetchone()
                return {"total": r[0] or 0, "completed": r[1] or 0, "failed": r[2] or 0}

db = Database()

# =========================================================================
# 🧠 AI ENGINE
# =========================================================================

async def ask_gpt_safe(sys_p: str, user_p: str) -> str:
    client = AsyncClient()
    providers = [g4f.Provider.Blackbox, g4f.Provider.PollinationsAI, g4f.Provider.DeepInfra]
    for p in providers:
        try:
            response = await client.chat.completions.create(
                model="gpt-4o", provider=p,
                messages=[{"role": "system", "content": sys_p}, {"role": "user", "content": user_p}]
            )
            res = response.choices[0].message.content
            if res: return res
        except: continue
    return "❌ AI Busy"

# =========================================================================
# 🦾 WORKER (HYBRID: v75 LOGIN LOGIC + v77 HANDLERS)
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client: Optional[TelegramClient] = None
        self.spam_task = None
        self.status = WorkerStatus.OFFLINE
        
        # Переменные для обработки номеров
        self.current_phone = None
        self.processed_count = 0
        self.waiting_for_code = False
        self.waiting_for_photo = False
        self.started_at = None

    def _get_client(self, path):
        # ИСПОЛЬЗУЕМ ПРОСТОЙ КОНСТРУКТОР ИЗ v75 (ОН РАБОТАЛ)
        return TelegramClient(
            str(path), cfg.API_ID, cfg.API_HASH, 
            device_model=cfg.DEVICE_MODEL, system_version=cfg.SYSTEM_VERSION, app_version=cfg.APP_VERSION,
            sequential_updates=False
        )

    async def start(self) -> bool:
        self.client = self._get_client(cfg.SESSION_DIR / f"session_{self.uid}")
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): return False
            
            self._bind_handlers()
            asyncio.create_task(self._run_safe())
            
            self.status = WorkerStatus.ONLINE
            self.started_at = int(time.time())
            return True
        except Exception as e:
            logger.error(f"Worker start error: {e}")
            return False

    async def _run_safe(self):
        while True:
            try: await self.client.run_until_disconnected()
            except: 
                await asyncio.sleep(5)
                try: await self.client.connect()
                except: pass
            if not await self.client.is_user_authorized(): 
                self.status = WorkerStatus.ERROR
                break

    def _bind_handlers(self):
        cl = self.client

        # --- AI & TOOLS ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.g(?: |$)(.*)'))
        async def quiz(e):
            await e.edit("⚡️")
            q = e.pattern_match.group(1) or (await e.get_reply_message()).text if e.is_reply else ""
            if not q: return
            ans = await ask_gpt_safe("Ответ 1-3 слова.", q)
            await e.edit(f"<b>{ans}</b>", parse_mode='html')

        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.report$'))
        async def report(e):
            await e.edit("🕵️‍♂️")
            tid = e.reply_to.reply_to_top_id if e.reply_to else None
            logs = []
            keys = ['айти', 'вбив', 'номер', 'код', 'встал', 'слет', 'сек', 'ща', 'готово', 'сдох', 'взял', 'отстоял']
            try:
                async for m in cl.iter_messages(e.chat_id, limit=1000, reply_to=tid):
                    if m.text and any(k in m.text.lower() for k in keys):
                        logs.append(f"[{m.date.strftime('%H:%M')}] {m.sender.first_name if m.sender else 'U'}: {m.text}")
            except: return await e.edit("❌ Err")
            
            if not logs: return await e.edit("❌ Логи пусты")
            
            prompt = """Analyze logs. 1. Start: "айти"/"вбив". 2. Success: >35 min no "слет". 3. Fail: "слет". JSON: [{"num":"x","time":"x","status":"✅"}]"""
            res = await ask_gpt_safe(prompt, "\n".join(logs[::-1]))
            try:
                data = json.loads(re.search(r'\[.*\]', res, re.DOTALL).group())
                txt = "📊 <b>REPORT:</b>\n" + "\n".join([f"📱 {i.get('num','?')} | {i.get('time','0')}m | {i.get('status','?')}" for i in data])
                await e.edit(txt, parse_mode='html')
            except: await e.edit(f"📝 {res}", parse_mode='html')

        # --- NUMBER PROCESSING (ИЗ v77) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.u$'))
        async def cmd_u(e):
            phone = await db.get_available_number(self.uid)
            if not phone: return await e.edit("❌ База пуста")
            self.current_phone = phone
            self.waiting_for_photo = True
            self.status = WorkerStatus.WORKING
            await e.edit(f"📱 <b>{phone}</b>\n⏳ Жду фото...", parse_mode='html')

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.au$'))
        async def cmd_au(e):
            await e.edit("✅ <b>Привет!</b> Жду коды.", parse_mode='html')
            self.waiting_for_code = True

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.v$'))
        async def cmd_v(e):
            if not self.current_phone: return await e.edit("❌ Нет номера")
            await e.edit(f"📞 <b>{self.current_phone}</b>\n✅ Вход ОК", buttons=[[Button.inline("✅ Слёт", b"slet")]], parse_mode='html')

        @cl.on(events.CallbackQuery(pattern=b"slet"))
        async def cb_slet(e):
            if self.current_phone:
                await db.update_number_status(self.current_phone, NumberStatus.COMPLETED)
                self.processed_count += 1
                await e.edit(f"✅ Готово: {self.current_phone}")
                self.current_phone = None
                self.status = WorkerStatus.ONLINE

        @cl.on(events.NewMessage(incoming=True))
        async def income(e):
            if self.waiting_for_code and e.message.message:
                code = re.search(r'\b\d{5,6}\b', e.message.message)
                if code and self.current_phone:
                    await db.update_number_status(self.current_phone, NumberStatus.CODE_RECEIVED)
                    self.waiting_for_code = False
            if self.waiting_for_photo and e.message.photo:
                if self.current_phone:
                    await db.update_number_status(self.current_phone, NumberStatus.PHOTO_RECEIVED)

        # --- UTILS ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.scan$'))
        async def scan(e):
            await e.edit("🔎 Scan...")
            u = {}
            try:
                async for m in cl.iter_messages(e.chat_id, limit=None):
                    if m.sender and isinstance(m.sender, types.User) and not m.sender.bot:
                        u[m.sender_id] = [m.sender.username or "", m.sender.first_name or ""]
            except: pass
            out = io.StringIO(); w = csv.writer(out); w.writerow(["ID", "Username", "Name"])
            for uid, d in u.items(): w.writerow([uid, d[0], d[1]])
            out.seek(0)
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig')); bio.name = "Scan.csv"
            await cl.send_file("me", bio, caption=f"✅ {len(u)} users"); await e.edit("✅")

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def stop(e):
            if self.spam_task: self.spam_task.cancel(); await e.edit("🛑")

    async def stop(self):
        try:
            if self.client: await self.client.disconnect()
            if self.spam_task: self.spam_task.cancel()
            self.status = WorkerStatus.OFFLINE
        except: pass

    def get_uptime(self):
        if not self.started_at: return "0m"
        return f"{int((time.time() - self.started_at)//60)}m"

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthStates(StatesGroup): PHONE = State(); CODE = State(); PASSWORD = State()
class PromoStates(StatesGroup): CODE = State()
class AddNumberStates(StatesGroup): WAITING = State()
class SiphonStates(StatesGroup): FILE = State(); MSG = State(); CONFIRM = State()
class AdminStates(StatesGroup): DAYS = State(); COUNT = State()

def kb_main(uid: int):
    btns = [
        [InlineKeyboardButton(text="🚀 Запуск", callback_data="start_worker"), InlineKeyboardButton(text="🛑 Стоп", callback_data="stop_worker")],
        [InlineKeyboardButton(text="➕ Номера", callback_data="add_numbers"), InlineKeyboardButton(text="🌪 Siphon", callback_data="siphon_start")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth")],
        [InlineKeyboardButton(text="📚 Команды", callback_data="help")]
    ]
    if uid == cfg.ADMIN_ID: btns.append([InlineKeyboardButton(text="👑 Админ", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def kb_numpad():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1", callback_data="n_1"), InlineKeyboardButton(text="2", callback_data="n_2"), InlineKeyboardButton(text="3", callback_data="n_3")],
        [InlineKeyboardButton(text="4", callback_data="n_4"), InlineKeyboardButton(text="5", callback_data="n_5"), InlineKeyboardButton(text="6", callback_data="n_6")],
        [InlineKeyboardButton(text="7", callback_data="n_7"), InlineKeyboardButton(text="8", callback_data="n_8"), InlineKeyboardButton(text="9", callback_data="n_9")],
        [InlineKeyboardButton(text="🔙", callback_data="n_del"), InlineKeyboardButton(text="0", callback_data="n_0"), InlineKeyboardButton(text="✅", callback_data="n_go")]
    ])

# --- HANDLERS ---

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username, m.from_user.first_name)
    await m.answer(f"💎 <b>StatPro v78 HYBRID</b>\nСборка: Auth v75 + Logic v77", reply_markup=kb_main(m.from_user.id))

@router.callback_query(F.data == "help")
async def help_cb(c: CallbackQuery):
    t = "🤖 <b>Команды:</b>\n.u - Взять номер\n.v - Подтвердить вход\n.au - Приветствие\n.report - Отчет\n.g - AI вопрос\n.scan - База чата"
    await c.message.edit_text(t, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))

@router.callback_query(F.data == "back")
async def back(c: CallbackQuery, state: FSMContext):
    await c.message.delete(); await start(c.message, state)

@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    info = await db.get_user_info(c.from_user.id)
    stats = await db.get_user_stats(c.from_user.id)
    w = W_POOL.get(c.from_user.id)
    ws = w.status.value if w else "offline"
    sub = "✅" if info and info[0] > time.time() else "❌"
    t = f"👤 <b>ID:</b> {c.from_user.id}\n💎 Подписка: {sub}\n🔌 Воркер: {ws}\n\n📊 <b>Статистика:</b>\nВсего: {stats['total']}\nОК: {stats['completed']}"
    await c.message.edit_text(t, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Промо", callback_data="promo"), InlineKeyboardButton(text="🔙", callback_data="back")]]))

# --- AUTH (V75 LOGIC - EXACT COPY) ---

@router.callback_query(F.data == "auth")
async def auth(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("❌ Подписка!", True)
    await c.message.edit_text("🔐 <b>ВХОД</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Телефон", callback_data="ph"), InlineKeyboardButton(text="📸 QR", callback_data="qr")],
        [InlineKeyboardButton(text="🔙", callback_data="back")]
    ]))

@router.callback_query(F.data == "ph")
async def auth_ph(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 Номер (7999...):"); await state.set_state(AuthStates.PHONE)

@router.message(AuthStates.PHONE)
async def auth_ph_get(m: Message, state: FSMContext):
    ph = m.text.strip().replace("+", "")
    w = Worker(m.from_user.id)
    # Инициализация ВРЕМЕННОГО клиента (как в v75)
    w.client = TelegramClient(str(cfg.SESSION_DIR / f"login_{m.from_user.id}"), cfg.API_ID, cfg.API_HASH)
    await w.client.connect()
    try:
        sent = await w.client.send_code_request(ph)
        await state.update_data(phone=ph, hash=sent.phone_code_hash, temp_worker=w, code_input="")
        await m.answer("📩 Код:", reply_markup=kb_numpad()); await state.set_state(AuthStates.CODE)
    except Exception as e:
        await w.client.disconnect(); await m.answer(f"❌ {e}")

@router.callback_query(F.data.startswith("n_"), AuthStates.CODE)
async def auth_numpad(c: CallbackQuery, state: FSMContext):
    act = c.data.split("_")[1]
    d = await state.get_data(); code = d.get("code_input", "")
    if act == "del": code = code[:-1]
    elif act == "go":
        w = d['temp_worker']
        try:
            await w.client.sign_in(phone=d['phone'], code=code, phone_code_hash=d['hash'])
            await w.client.disconnect()
            os.rename(cfg.SESSION_DIR / f"login_{c.from_user.id}.session", cfg.SESSION_DIR / f"session_{c.from_user.id}.session")
            
            # ЗАПУСК БОЕВОГО ВОРКЕРА
            rw = Worker(c.from_user.id)
            if await rw.start(): W_POOL[c.from_user.id] = rw; await c.message.answer("✅ Вход выполнен!"); await start(c.message, state)
            else: await c.message.answer("❌ Ошибка запуска воркера")
            await state.clear(); return
        except SessionPasswordNeededError:
            await c.message.answer("🔒 Пароль 2FA:"); await state.set_state(AuthStates.PASSWORD); return
        except Exception as e:
            await c.message.answer(f"❌ {e}"); await w.client.disconnect(); return
    else: code += act
    await state.update_data(code_input=code)
    try: await c.message.edit_text(f"Код: <b>{code}</b>", reply_markup=kb_numpad())
    except: pass

@router.message(AuthStates.PASSWORD)
async def auth_pwd(m: Message, state: FSMContext):
    d = await state.get_data(); w = d['temp_worker']
    try:
        await w.client.sign_in(password=m.text); await w.client.disconnect()
        os.rename(cfg.SESSION_DIR / f"login_{m.from_user.id}.session", cfg.SESSION_DIR / f"session_{m.from_user.id}.session")
        rw = Worker(m.from_user.id)
        if await rw.start(): W_POOL[m.from_user.id] = rw; await m.answer("✅ Пароль принят!"); await start(m, state)
    except Exception as e: await m.answer(f"❌ {e}"); await w.client.disconnect()
    await state.clear()

@router.callback_query(F.data == "qr")
async def auth_qr(c: CallbackQuery, state: FSMContext):
    uid = c.from_user.id; w = Worker(uid)
    w.client = TelegramClient(str(cfg.SESSION_DIR / f"session_{uid}"), cfg.API_ID, cfg.API_HASH)
    await w.client.connect()
    try:
        qr = await w.client.qr_login()
        im = io.BytesIO(); qrcode.make(qr.url).save(im, "PNG"); im.seek(0)
        msg = await c.message.answer_photo(BufferedInputFile(im.read(), "qr.png"), caption="📸 Скан QR")
        await qr.wait(60); await msg.delete(); await w.client.disconnect()
        rw = Worker(uid)
        if await rw.start(): W_POOL[uid] = rw; await c.message.answer("✅ QR OK!")
    except: await c.message.answer("❌ Timeout")
    finally: await state.clear()

# --- OTHER HANDLERS ---

@router.callback_query(F.data == "start_worker")
async def start_w(c: CallbackQuery):
    if c.from_user.id in W_POOL: return await c.answer("Уже работает", True)
    w = Worker(c.from_user.id)
    if await w.start(): W_POOL[c.from_user.id] = w; await c.message.edit_text("✅ Воркер запущен", reply_markup=kb_main(c.from_user.id))
    else: await c.message.edit_text("❌ Ошибка. Сначала войдите.", reply_markup=kb_main(c.from_user.id))

@router.callback_query(F.data == "stop_worker")
async def stop_w(c: CallbackQuery):
    if c.from_user.id in W_POOL: await W_POOL[c.from_user.id].stop(); del W_POOL[c.from_user.id]; await c.answer("Стоп", True)
    else: await c.answer("Не запущен", True)

@router.callback_query(F.data == "add_numbers")
async def add_n(c: CallbackQuery, state: FSMContext):
    await c.message.answer("📱 Номера (каждый с новой строки):"); await state.set_state(AddNumberStates.WAITING)

@router.message(AddNumberStates.WAITING)
async def save_n(m: Message, state: FSMContext):
    cnt = 0
    for line in m.text.split("\n"):
        ph = "".join(filter(str.isdigit, line))
        if len(ph) > 9: 
            if await db.add_number("+" + ph, m.from_user.id): cnt += 1
    await m.answer(f"✅ Добавлено: {cnt}"); await state.clear()

# --- SIPHON ---
@router.callback_query(F.data == "siphon_start")
async def siphon_start(c: CallbackQuery, state: FSMContext):
    await c.message.answer("📂 Кидай файл"); await state.set_state(SiphonStates.FILE)

@router.message(SiphonStates.FILE, F.document)
async def siphon_file(m: Message, state: FSMContext):
    path = cfg.TEMP_DIR / f"s_{m.from_user.id}"; await bot.download(m.document, path)
    with open(path, errors='ignore') as f: ids = list(set(re.findall(r'\b\d{7,20}\b', f.read())))
    os.remove(path); await state.update_data(ids=ids)
    await m.answer(f"✅ Найдено: {len(ids)}\n✍️ Текст рассылки:"); await state.set_state(SiphonStates.MSG)

@router.message(SiphonStates.MSG)
async def siphon_msg(m: Message, state: FSMContext):
    d = await state.get_data(); w = W_POOL.get(m.from_user.id)
    if not w: return await m.answer("❌ Воркер оффлайн")
    await m.answer("🚀 Поехали!"); asyncio.create_task(siphon_run(m.from_user.id, w, d['ids'], m.text))
    await state.clear()

async def siphon_run(uid, w, ids, txt):
    ok = 0
    for i in ids:
        try: await w.client.send_message(int(i), txt); ok += 1; await asyncio.sleep(2)
        except: pass
    await bot.send_message(uid, f"🏁 Рассылка: {ok} доставлено")

# --- ADMIN ---
@router.callback_query(F.data == "adm")
async def adm(c: CallbackQuery):
    if c.from_user.id == cfg.ADMIN_ID: await c.message.answer("Код? (дни)", reply_markup=None); await state.set_state(AdminStates.DAYS)

# --- MAIN ---
async def main():
    await db.init()
    # Auto-restore
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try: 
            uid = int(f.stem.split("_")[1])
            w = Worker(uid)
            if await w.start(): W_POOL[uid] = w
        except: pass
    logger.info(f"Started. Workers: {len(W_POOL)}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
