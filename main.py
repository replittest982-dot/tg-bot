#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro v74.0 - KAMIKAZE EDITION
-----------------------------------
Режим: AGGRESSIVE SPAM
Особенности:
1. Игнорирует ошибки приватности/удаленных аккаунтов.
2. Пытается пробить PeerIdInvalid через get_input_entity.
3. Минимальные задержки (высокий риск бана).
4. Читает любые файлы (txt/csv/excel-csv).
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
from typing import Dict, List
from dataclasses import dataclass

# --- БИБЛИОТЕКИ ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties

from telethon import TelegramClient, events, types, functions
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, 
    UserPrivacyRestrictedError, UserDeactivatedError, 
    PeerIdInvalidError, ChatWriteForbiddenError
)

# --- AI CORE ---
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

@dataclass
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_kamikaze.db"
    TEMP_DIR: Path = BASE_DIR / "temp"
    
    DEVICE_MODEL: str = "iPhone 15 Pro Max"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ ОШИБКА: Заполни переменные окружения!")
            sys.exit(1)

cfg = Config()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("StatPro")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ
# =========================================================================

class Database:
    def __init__(self): self.path = cfg.DB_PATH
    def get_conn(self): return aiosqlite.connect(self.path)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    username TEXT, 
                    sub_end INTEGER, 
                    joined_at INTEGER
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

    async def check_sub_bool(self, uid: int) -> bool:
        if uid == cfg.ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                return r[0] > int(time.time()) if (r and r[0]) else False

    async def upsert_user(self, uid: int, uname: str):
        now = int(time.time())
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users VALUES (?, ?, 0, ?)", (uid, uname, now))
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def use_promo(self, uid: int, code: str) -> int:
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code.strip(),)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ?", (code.strip(),))
            await db.execute("INSERT OR IGNORE INTO users (user_id, sub_end, joined_at) VALUES (?, 0, ?)", (uid, int(time.time())))
            now = int(time.time())
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c2:
                row = await c2.fetchone()
                curr = row[0] if (row and row[0]) else 0
            new_end = (curr if curr > now else now) + (days * 86400)
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"KAMIKAZE-{random.randint(100,999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code
    
    async def get_user_info(self, uid: int):
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end, joined_at FROM users WHERE user_id = ?", (uid,)) as c:
                return await c.fetchone()

db = Database()

# =========================================================================
# 🧠 AI ENGINE
# =========================================================================

async def ask_gpt_safe(sys_p: str, user_p: str) -> str:
    client = AsyncClient()
    providers = [g4f.Provider.Blackbox, g4f.Provider.DeepInfra, g4f.Provider.PollinationsAI, g4f.Provider.DarkAI]
    for p in providers:
        try:
            response = await client.chat.completions.create(
                model="gpt-4o", provider=p,
                messages=[{"role": "system", "content": sys_p}, {"role": "user", "content": user_p}]
            )
            res = response.choices[0].message.content
            if res: return res
        except: continue
    return "❌ AI Error"

# =========================================================================
# 🦾 WORKER (USERBOT)
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        self.spam_task = None

    def _get_client(self, path):
        return TelegramClient(
            str(path), cfg.API_ID, cfg.API_HASH, 
            device_model=cfg.DEVICE_MODEL, system_version=cfg.SYSTEM_VERSION, app_version=cfg.APP_VERSION,
            sequential_updates=False
        )

    async def start(self):
        self.client = self._get_client(cfg.SESSION_DIR / f"session_{self.uid}")
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): return False
            self._bind()
            asyncio.create_task(self._run_safe())
            return True
        except: return False

    async def _run_safe(self):
        while True:
            try: await self.client.run_until_disconnected()
            except: 
                await asyncio.sleep(5)
                try: await self.client.connect()
                except: pass
            if not await self.client.is_user_authorized(): break

    def _bind(self):
        cl = self.client

        # --- .g ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.g(?: |$)(.*)'))
        async def quiz(e):
            await e.edit("⚡️")
            q = e.pattern_match.group(1) or (await e.get_reply_message()).text if e.is_reply else ""
            if not q: return
            ans = await ask_gpt_safe("Ответ 1-3 слова.", q)
            await e.edit(f"<b>{ans}</b>", parse_mode='html')

        # --- .report ---
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
            
            if not logs: return await e.edit("❌ Empty")
            prompt = """Analyze logs. 1. Start: "айти"/"вбив". 2. Success: >35 min no "слет". 3. Fail: "слет". JSON: [{"num":"x","time":"x","status":"✅"}]"""
            res = await ask_gpt_safe(prompt, "\n".join(logs[::-1]))
            try:
                data = json.loads(re.search(r'\[.*\]', res, re.DOTALL).group())
                txt = "📊 <b>REPORT:</b>\n" + "\n".join([f"📱 {i.get('num','?')} | {i.get('time','0')}m | {i.get('status','?')}" for i in data])
                await e.edit(txt, parse_mode='html')
            except: await e.edit(f"📝 {res}", parse_mode='html')

        # --- .scan ---
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

        # --- .spam ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.(s|spam)\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def spam(e):
            t, c, d = e.pattern_match.group(2), int(e.pattern_match.group(3)), float(e.pattern_match.group(4))
            await e.delete()
            async def r():
                for _ in range(c):
                    try: await cl.send_message(e.chat_id, t); await asyncio.sleep(d)
                    except: break
            self.spam_task = asyncio.create_task(r())

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def stop(e):
            if self.spam_task: self.spam_task.cancel(); await e.edit("🛑")

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class SiphonS(StatesGroup): FILE=State(); MSG=State(); CONFIRM=State()
class AdminS(StatesGroup): PD=State(); PA=State()

def kb_main(uid):
    btns = [
        [InlineKeyboardButton(text="🌪 ПЕРЕЛИВ (Siphon)", callback_data="siphon_start")],
        [InlineKeyboardButton(text="📚 Инфо", callback_data="help"), InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth")]
    ]
    if uid == cfg.ADMIN_ID: btns.append([InlineKeyboardButton(text="👑 Админ", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def get_numpad():
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
    await db.upsert_user(m.from_user.id, m.from_user.username)
    await m.answer(f"💎 <b>StatPro KAMIKAZE v74.0</b>\nID: <code>{m.from_user.id}</code>", reply_markup=kb_main(m.from_user.id))

@router.callback_query(F.data == "help")
async def hlp(c: CallbackQuery): await c.message.edit_text("Команды: .g, .report, .scan, .spam, .all", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))
@router.callback_query(F.data == "back")
async def back(c: CallbackQuery, state: FSMContext): await c.message.delete(); await start(c.message, state)
@router.callback_query(F.data == "profile")
async def prof(c: CallbackQuery):
    info = await db.get_user_info(c.from_user.id)
    sub = f"🟢 Актив ({int((info[0]-time.time())/86400)} дн)" if info and info[0] > time.time() else "🔴 Нет"
    stat = "🟢 ON" if c.from_user.id in W_POOL else "🔴 OFF"
    await c.message.edit_text(f"👤 <b>Профиль</b>\n💎 Подписка: {sub}\n🔌 Воркер: {stat}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Промо", callback_data="promo"), InlineKeyboardButton(text="🔙", callback_data="back")]]))

@router.callback_query(F.data == "promo")
async def prm(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Код:"); await state.set_state(PromoS.CODE)
@router.message(PromoS.CODE)
async def prm_u(m: Message, state: FSMContext):
    if await db.use_promo(m.from_user.id, m.text): await m.answer("✅ Активировано"); await start(m, state)
    else: await m.answer("❌ Ошибка")
    await state.clear()

# --- SIPHON (KAMIKAZE LOGIC) ---

@router.callback_query(F.data == "siphon_start")
async def siph(c: CallbackQuery, state: FSMContext):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("Купи подписку!", True)
    if c.from_user.id not in W_POOL: return await c.answer("Запусти воркера (Вход)!", True)
    await c.message.edit_text("📂 <b>Кидай ЛЮБОЙ файл (txt, csv)</b>:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))
    await state.set_state(SiphonS.FILE)

@router.message(SiphonS.FILE, F.document)
async def siph_f(m: Message, state: FSMContext):
    path = cfg.TEMP_DIR / f"s_{m.from_user.id}.tmp"
    try:
        await bot.download(m.document, destination=path)
        with open(path, 'r', encoding='utf-8', errors='ignore') as f: content = f.read()
        ids = list(set([int(x) for x in re.findall(r'\b\d{7,20}\b', content)])) # Universal Regex Parser
        if not ids: return await m.answer("❌ ID не найдены.")
        await state.update_data(ids=ids); await m.answer(f"✅ ID: {len(ids)}\n✍️ <b>Текст рассылки:</b>"); await state.set_state(SiphonS.MSG)
    except Exception as e: await m.answer(f"❌ Error: {e}")
    finally: 
        if os.path.exists(path): os.remove(path)

@router.message(SiphonS.MSG)
async def siph_m(m: Message, state: FSMContext):
    await state.update_data(txt=m.text); d = await state.get_data()
    await m.answer(f"🔥 Начать КАМИКАДЗЕ рассылку?\nЦелей: {len(d['ids'])}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 ПУСК", callback_data="run_k"), InlineKeyboardButton(text="❌", callback_data="back")]]))
    await state.set_state(SiphonS.CONFIRM)

@router.callback_query(F.data == "run_k", SiphonS.CONFIRM)
async def siph_run(c: CallbackQuery, state: FSMContext):
    d = await state.get_data(); w = W_POOL.get(c.from_user.id)
    if not w: return await c.answer("Воркер оффлайн", True)
    await c.message.edit_text("🚀 <b>Рассылка пошла!</b> (Режим: Игнор ошибок)")
    asyncio.create_task(kamikaze_task(c.from_user.id, w, d['ids'], d['txt']))
    await state.clear()

async def kamikaze_task(uid, w, targets, text):
    ok, skip = 0, 0
    for tid in targets:
        try:
            # Пытаемся найти сущность, если ID не в кэше
            try: entity = await w.client.get_input_entity(tid)
            except: entity = tid # Пробуем слать на сырой ID
            
            await w.client.send_message(entity, text)
            ok += 1
            await asyncio.sleep(random.uniform(1.5, 4)) # АГРЕССИВНАЯ ЗАДЕРЖКА
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds) # Обязательно ждем флуд, иначе скрипт умрет
        except (UserPrivacyRestrictedError, UserDeactivatedError, PeerIdInvalidError, ChatWriteForbiddenError):
            skip += 1 # Игнорируем приватность/бан/удаленных
        except Exception as e:
            skip += 1 # Игнорируем всё остальное
            
    try: await bot.send_message(uid, f"🏁 <b>Рассылка завершена!</b>\n✅ Доставлено: {ok}\n🗑 Пропущено (блок/приват): {skip}")
    except: pass

# --- AUTH ---
@router.callback_query(F.data == "auth")
async def au(c: CallbackQuery): await c.message.edit_text("Вход:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="QR", callback_data="qr"), InlineKeyboardButton(text="Phone", callback_data="ph")]]))
@router.callback_query(F.data == "ph")
async def ph(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Номер:"); await state.set_state(AuthS.PH)
@router.message(AuthS.PH)
async def ph_g(m: Message, state: FSMContext):
    uid = m.from_user.id; w = Worker(uid); w.client = TelegramClient(str(cfg.SESSION_DIR/f"s_{uid}"), cfg.API_ID, cfg.API_HASH); await w.client.connect()
    try: s = await w.client.send_code_request(m.text); await state.update_data(ph=m.text, h=s.phone_code_hash, uid=uid, c=""); await w.client.disconnect(); await m.answer("Код:", reply_markup=get_numpad()); await state.set_state(AuthS.CO)
    except Exception as e: await w.client.disconnect(); await m.answer(f"Error: {e}"); await state.clear()
@router.callback_query(F.data.startswith("n_"), AuthS.CO)
async def nm(c: CallbackQuery, state: FSMContext):
    act = c.data.split("_")[1]; d = await state.get_data(); curr = d.get("c", "")
    if act == "del": curr = curr[:-1]
    elif act == "go":
        await c.message.edit_text("⏳ ..."); w = Worker(d['uid']); w.client = TelegramClient(str(cfg.SESSION_DIR/f"s_{d['uid']}"), cfg.API_ID, cfg.API_HASH); await w.client.connect()
        try: await w.client.sign_in(d['ph'], curr, phone_code_hash=d['h']); await w.client.disconnect(); rw = Worker(d['uid']); await rw.start(); W_POOL[d['uid']]=rw; await c.message.answer("✅ OK"); await start(c.message, state)
        except SessionPasswordNeededError: await w.client.disconnect(); await c.message.answer("🔒 2FA:"); await state.set_state(AuthS.PA); return
        except Exception as e: await w.client.disconnect(); await c.message.answer(f"❌ {e}"); await state.clear(); return
        return
    else: curr += act
    await state.update_data(c=curr); await c.message.edit_text(f"Код: {curr}", reply_markup=get_numpad())
@router.message(AuthS.PA)
async def pa(m: Message, state: FSMContext):
    d = await state.get_data(); w = Worker(d['uid']); w.client = TelegramClient(str(cfg.SESSION_DIR/f"s_{d['uid']}"), cfg.API_ID, cfg.API_HASH); await w.client.connect()
    try: await w.client.sign_in(password=m.text); await w.client.disconnect(); rw=Worker(d['uid']); await rw.start(); W_POOL[d['uid']]=rw; await m.answer("✅ OK")
    except Exception as e: await m.answer(f"❌ {e}")
    await state.clear()
@router.callback_query(F.data == "qr")
async def qr(c: CallbackQuery, state: FSMContext):
    uid=c.from_user.id; w=Worker(uid); w.client=TelegramClient(str(cfg.SESSION_DIR/f"s_{uid}"), cfg.API_ID, cfg.API_HASH); await w.client.connect()
    try: q=await w.client.qr_login(); b=io.BytesIO(); qrcode.make(q.url).save(b,"PNG"); b.seek(0); m=await c.message.answer_photo(BufferedInputFile(b.read(),"qr.png")); await q.wait(60); await m.delete(); await w.client.disconnect(); rw=Worker(uid); await rw.start(); W_POOL[uid]=rw; await c.message.answer("✅ OK")
    except: await c.message.answer("Time out")
    finally: await state.clear()

# --- ADMIN ---
@router.callback_query(F.data == "adm")
async def adm(c: CallbackQuery): await c.message.edit_text("Adm:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Promo", callback_data="mk_p")]]))
@router.callback_query(F.data == "mk_p")
async def mk(c: CallbackQuery, state: FSMContext): await c.message.answer("D?"); await state.set_state(AdminS.PD)
@router.message(AdminS.PD)
async def mk_d(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("C?"); await state.set_state(AdminS.PA)
@router.message(AdminS.PA)
async def mk_a(m: Message, state: FSMContext): d=await state.get_data(); code=await db.create_promo(d['d'], int(m.text)); await m.answer(code); await state.clear()

async def main():
    await db.init()
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub_bool(uid):
                w = Worker(uid); await w.start(); W_POOL[uid] = w
        except: pass
    logger.info("🔥 StatPro KAMIKAZE Started")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
