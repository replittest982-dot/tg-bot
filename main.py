#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro v69.2 - TITAN REBORN
-------------------------------
Fixed: .g command silence (Updated AI Engine)
Modules: Siphon, Deep Scan, Turbo Quiz, Secure Auth
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
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

# --- LIBRARIES ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties

from telethon import TelegramClient, events, types, functions
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# --- AI CORE (UPDATED) ---
try:
    from g4f.client import AsyncClient
    from g4f.Provider import Blackbox, PollinationsAI, DarkAI
except ImportError:
    print("⚠️ Installing AI libs...")
    os.system("pip install -U g4f[all] curl_cffi aiohttp")
    from g4f.client import AsyncClient
    from g4f.Provider import Blackbox, PollinationsAI, DarkAI

# =========================================================================
# ⚙️ CONFIGURATION
# =========================================================================

@dataclass
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_titan_v2.db"
    TEMP_DIR: Path = BASE_DIR / "temp"
    
    DEVICE_MODEL: str = "iPhone 15 Pro Max"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"
    LANG_CODE: str = "en"
    SYSTEM_LANG_CODE: str = "en-US"

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ CRITICAL: Введите BOT_TOKEN, API_ID, API_HASH в переменные окружения!")
            sys.exit(1)

cfg = Config()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("StatPro")

# =========================================================================
# 🗄️ DATABASE
# =========================================================================

class Database:
    def __init__(self): self.path = cfg.DB_PATH
    def get_conn(self): return aiosqlite.connect(self.path)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, sub_end INTEGER, joined_at INTEGER)")
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations INTEGER)")
            await db.commit()

    async def get_all_users_ids(self) -> List[int]:
        async with self.get_conn() as db:
            async with db.execute("SELECT user_id FROM users") as c:
                return [r[0] for r in await c.fetchall()]

    async def upsert_user(self, uid: int, uname: str):
        now = int(time.time())
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, ?, ?)", (uid, uname, 0, now))
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def check_sub_bool(self, uid: int) -> bool:
        if uid == cfg.ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                return r[0] > int(time.time()) if (r and r[0]) else False

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            now = int(time.time())
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c2:
                row = await c2.fetchone()
                curr = row[0] if (row and row[0]) else 0
            new_end = (curr if curr > now else now) + (days * 86400)
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"TITAN-{random.randint(100,999)}-{random.randint(1000,9999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🧠 AI ENGINE (NEW CLIENT)
# =========================================================================

async def ask_gpt_turbo(question: str) -> str:
    """
    Использует новый AsyncClient для стабильности.
    """
    system_prompt = (
        "Ты играешь в викторину. Отвечай ТОЛЬКО правильным ответом."
        "Если столица - пиши только название города."
        "Если дата - пиши только год или число."
        "Максимально коротко. Без точек в конце."
    )
    
    client = AsyncClient()
    
    try:
        # Попытка 1: Автоматический выбор
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        # Попытка 2: Принудительный провайдер (если авто не сработал)
        try:
            response = await client.chat.completions.create(
                model="gpt-4",
                provider=Blackbox,
                messages=[{"role": "user", "content": question}]
            )
            return response.choices[0].message.content
        except Exception as e2:
            return f"❌ AI Error: {e2}"

# =========================================================================
# 🦾 WORKER CORE
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        self.spam_task = None

    def _get_client(self, path):
        return TelegramClient(
            str(path), cfg.API_ID, cfg.API_HASH, 
            device_model=cfg.DEVICE_MODEL, 
            system_version=cfg.SYSTEM_VERSION, 
            app_version=cfg.APP_VERSION,
            lang_code=cfg.LANG_CODE,
            system_lang_code=cfg.SYSTEM_LANG_CODE
        )

    async def start(self):
        self.client = self._get_client(cfg.SESSION_DIR / f"session_{self.uid}")
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): return False
            self._bind()
            asyncio.create_task(self.client.run_until_disconnected())
            return True
        except: return False

    def _bind(self):
        client = self.client

        # --- ⚡️ TURBO QUIZ (.g) ---
        @client.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.g(?: |$)(.*)'))
        async def quiz_cmd(e):
            # Логика захвата вопроса
            question = ""
            if e.is_reply:
                r = await e.get_reply_message()
                question = r.text or r.caption or ""
            else:
                question = e.pattern_match.group(1)
            
            if not question: 
                return await e.edit("⚡️ <b>Ошибка:</b> Нет текста вопроса!")
            
            # Визуальный эффект загрузки
            await e.edit("⚡️...")
            
            # Запрос к AI
            answer = await ask_gpt_turbo(question)
            
            # Редактирование сообщения на ответ
            await e.edit(answer)

        # --- 🧬 DEEP SCAN ---
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.scan(?:\s+(\d+))?'))
        async def deep_scan(e):
            limit_arg = e.pattern_match.group(1)
            limit = int(limit_arg) if limit_arg else None
            
            await e.edit(f"🧬 <b>Deep Scan...</b>\nTarget: {'ALL' if limit is None else limit}", parse_mode='html')
            
            users = {} 
            count = 0
            try:
                async for m in client.iter_messages(e.chat_id, limit=limit):
                    count += 1
                    if count % 500 == 0:
                        try: await e.edit(f"🧬 Scanned: {count}...")
                        except: pass
                    
                    if m.sender and isinstance(m.sender, types.User) and not m.sender.bot:
                        if m.sender.id not in users:
                            fname = m.sender.first_name or ""
                            lname = m.sender.last_name or ""
                            users[m.sender.id] = [m.sender.username or "", f"{fname} {lname}".strip()]
            except Exception as ex:
                await e.edit(f"❌ Error: {ex}")
                return

            out = io.StringIO()
            w = csv.writer(out)
            w.writerow(["ID", "Username", "Name"])
            for uid, data in users.items():
                w.writerow([uid, data[0], data[1]])
            
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig'))
            bio.name = f"Scan_{e.chat_id}.csv"
            
            await client.send_file("me", bio, caption=f"✅ <b>Scan Done</b>\nUsers: {len(users)}", force_document=True, parse_mode='html')
            await e.edit(f"✅ Saved {len(users)} users.")

        # --- SPAM ---
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(s|spam)\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def spam_cmd(e):
            txt, cnt, dly = e.pattern_match.group(2), int(e.pattern_match.group(3)), float(e.pattern_match.group(4))
            await e.delete()
            async def run():
                for _ in range(cnt):
                    try: await client.send_message(e.chat_id, txt); await asyncio.sleep(dly)
                    except FloodWaitError as f: await asyncio.sleep(f.seconds + 2)
                    except: break
            self.spam_task = asyncio.create_task(run())

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def stop_cmd(e):
            if self.spam_task: self.spam_task.cancel(); self.spam_task = None
            await e.edit("🛑 Stopped.")
            
        # --- MASS MENTION ---
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.all(?:\s+(.+))?'))
        async def all_cmd(e):
            txt = e.pattern_match.group(1) or "."
            await e.delete()
            parts = await client.get_participants(e.chat_id)
            chunk = []
            for p in parts:
                if p.bot or p.deleted: continue
                chunk.append(f"<a href='tg://user?id={p.id}'>\u200b</a>")
                if len(chunk) >= 5:
                    await client.send_message(e.chat_id, txt + "".join(chunk), parse_mode='html')
                    chunk = []
                    await asyncio.sleep(2)

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
class AdminS(StatesGroup): PD=State(); PA=State(); CAST=State()

def get_numpad_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣", callback_data="n_1"), InlineKeyboardButton(text="2️⃣", callback_data="n_2"), InlineKeyboardButton(text="3️⃣", callback_data="n_3")],
        [InlineKeyboardButton(text="4️⃣", callback_data="n_4"), InlineKeyboardButton(text="5️⃣", callback_data="n_5"), InlineKeyboardButton(text="6️⃣", callback_data="n_6")],
        [InlineKeyboardButton(text="7️⃣", callback_data="n_7"), InlineKeyboardButton(text="8️⃣", callback_data="n_8"), InlineKeyboardButton(text="9️⃣", callback_data="n_9")],
        [InlineKeyboardButton(text="🔙", callback_data="n_del"), InlineKeyboardButton(text="0️⃣", callback_data="n_0"), InlineKeyboardButton(text="✅", callback_data="n_go")]
    ])

def kb_main(uid):
    btns = [
        [InlineKeyboardButton(text="🌪 ПЕРЕЛИВ (Siphon)", callback_data="siphon_start")],
        [InlineKeyboardButton(text="📚 Команды", callback_data="help"), InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth")]
    ]
    if uid == cfg.ADMIN_ID: btns.append([InlineKeyboardButton(text="👑 Админ", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username)
    await m.answer(f"💎 <b>StatPro TITAN v69.2</b>\nID: <code>{m.from_user.id}</code>", reply_markup=kb_main(m.from_user.id))

@router.callback_query(F.data == "help")
async def help_cb(c: CallbackQuery):
    await c.message.edit_text(
        "⚡️ <b>Commands:</b>\n<code>.g</code> - AI Quiz (Reply)\n<code>.scan</code> - Scan Chat\n<code>.s [txt] [cnt] [time]</code> - Spam\n<code>.all</code> - Tag All",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]])
    )

@router.callback_query(F.data == "profile")
async def profile_cb(c: CallbackQuery):
    sub = await db.check_sub_bool(c.from_user.id)
    await c.message.edit_text(f"👤 <b>Status:</b> {'✅ Active' if sub else '❌ Expired'}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Promo", callback_data="promo")],[InlineKeyboardButton(text="🔙", callback_data="back")]]))

@router.callback_query(F.data == "promo")
async def promo_ask(c: CallbackQuery, state: FSMContext): await c.message.edit_text("🎟 Code:"); await state.set_state(PromoS.CODE)
@router.message(PromoS.CODE)
async def promo_use(m: Message, state: FSMContext):
    d = await db.use_promo(m.from_user.id, m.text)
    if d: await m.answer(f"✅ +{d} days"); await start(m, state)
    else: await m.answer("❌ Invalid")

@router.callback_query(F.data == "back")
async def back_cb(c: CallbackQuery, state: FSMContext): await c.message.delete(); await start(c.message, state)

@router.callback_query(F.data == "siphon_start")
async def siphon_init(c: CallbackQuery, state: FSMContext):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("No Sub!", True)
    if c.from_user.id not in W_POOL: return await c.answer("Login First!", True)
    await c.message.edit_text("🌪 Upload <code>.csv</code> file:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))
    await state.set_state(SiphonS.FILE)

@router.message(SiphonS.FILE, F.document)
async def siphon_file(m: Message, state: FSMContext):
    if not m.document.file_name.endswith('.csv'): return await m.answer("❌ .csv only!")
    file = await bot.get_file(m.document.file_id)
    path = cfg.TEMP_DIR / f"siphon_{m.from_user.id}.csv"
    await bot.download_file(file.file_path, path)
    ids = []
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader, None)
            for r in reader:
                if r and r[0].isdigit(): ids.append(int(r[0]))
    except: return await m.answer("❌ Bad CSV")
    finally: os.remove(path)
    if not ids: return await m.answer("❌ Empty")
    await state.update_data(targets=ids)
    await m.answer(f"✅ Loaded {len(ids)} users.\n✍️ <b>Send post text:</b>")
    await state.set_state(SiphonS.MSG)

@router.message(SiphonS.MSG)
async def siphon_msg(m: Message, state: FSMContext):
    await state.update_data(msg_text=m.text or m.caption or "Hello")
    data = await state.get_data()
    await m.answer(f"🌪 Start Siphon?\nUsers: {len(data['targets'])}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 START", callback_data="siphon_run"), InlineKeyboardButton(text="❌", callback_data="back")]]))
    await state.set_state(SiphonS.CONFIRM)

@router.callback_query(F.data == "siphon_run", SiphonS.CONFIRM)
async def siphon_exec(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    worker = W_POOL.get(c.from_user.id)
    if not worker: return await c.answer("Offline", True)
    await c.message.edit_text("🚀 Siphon Started!")
    asyncio.create_task(run_siphon(c.from_user.id, worker, data['targets'], data['msg_text']))
    await state.clear()

async def run_siphon(uid, worker, targets, text):
    ok, fail = 0, 0
    for tid in targets:
        try:
            await worker.client.send_message(tid, text)
            ok += 1
            await asyncio.sleep(random.randint(4, 8))
        except: fail += 1
    try: await bot.send_message(uid, f"✅ Siphon Done\nOK: {ok}\nFail: {fail}")
    except: pass

@router.callback_query(F.data == "auth")
async def auth_ui(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("No Sub!", True)
    await c.message.edit_text("Login:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 QR", callback_data="qr"), InlineKeyboardButton(text="📱 Phone", callback_data="ph")]]))

@router.callback_query(F.data == "ph")
async def ph_ui(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Number (79...):"); await state.set_state(AuthS.PH)
@router.message(AuthS.PH)
async def ph_send(m: Message, state: FSMContext):
    uid = m.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        s = await cl.send_code_request(m.text)
        await state.update_data(ph=m.text, h=s.phone_code_hash, uid=uid, c="")
        await cl.disconnect()
        await m.answer(f"Code for {m.text}:", reply_markup=get_numpad_kb())
        await state.set_state(AuthS.CO)
    except Exception as e: await cl.disconnect(); await m.answer(f"❌ {e}")

@router.callback_query(F.data.startswith("n_"), AuthS.CO)
async def num_h(c: CallbackQuery, state: FSMContext):
    act = c.data.split("_")[1]
    d = await state.get_data(); curr = d.get("c", "")
    if act == "del": curr = curr[:-1]
    elif act == "go":
        if not curr: return await c.answer("Empty!", True)
        await c.message.edit_text("⏳ ...")
        cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
        await cl.connect()
        try:
            await cl.sign_in(phone=d['ph'], code=curr, phone_code_hash=d['h'])
            await c.message.answer("✅ Success!"); await cl.disconnect(); await state.clear()
            w=Worker(d['uid']); await w.start(); W_POOL[d['uid']]=w
            await start(c.message, state); return
        except SessionPasswordNeededError: await c.message.answer("🔒 2FA Password:"); await cl.disconnect(); await state.set_state(AuthS.PA); return
        except Exception as e: await c.message.answer(f"❌ {e}"); await cl.disconnect(); return
    else: curr += act
    await state.update_data(c=curr)
    try: await c.message.edit_text(f"Code: {curr}", reply_markup=get_numpad_kb())
    except: pass

@router.message(AuthS.PA)
async def pa_h(m: Message, state: FSMContext):
    d = await state.get_data(); cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}"); await cl.connect()
    try: await cl.sign_in(password=m.text); await m.answer("✅"); await cl.disconnect(); await state.clear(); w=Worker(d['uid']); await w.start(); W_POOL[d['uid']]=w
    except Exception as e: await cl.disconnect(); await m.answer(f"❌ {e}")

@router.callback_query(F.data == "qr")
async def qr_h(c: CallbackQuery):
    cl = Worker(c.from_user.id)._get_client(cfg.SESSION_DIR / f"session_{c.from_user.id}"); await cl.connect()
    qr = await cl.qr_login()
    b = io.BytesIO(); qrcode.make(qr.url).save(b, "PNG"); b.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="Scan QR")
    try: await qr.wait(60); await msg.delete(); await c.message.answer("✅"); w=Worker(c.from_user.id); await w.start(); W_POOL[c.from_user.id]=w
    except: await msg.delete()
    finally: await cl.disconnect()

@router.callback_query(F.data == "adm")
async def adm(c: CallbackQuery): await c.message.edit_text("Admin:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Promo", callback_data="mk_p")], [InlineKeyboardButton(text="📢 Broadcast", callback_data="bc")]]))
@router.callback_query(F.data == "mk_p")
async def mk_p(c: CallbackQuery, state: FSMContext): await c.message.answer("Days?"); await state.set_state(AdminS.PD)
@router.message(AdminS.PD)
async def mk_pd(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Count?"); await state.set_state(AdminS.PA)
@router.message(AdminS.PA)
async def mk_pa(m: Message, state: FSMContext): d=await state.get_data(); c=await db.create_promo(d['d'], int(m.text)); await m.answer(f"Code: <code>{c}</code>"); await state.clear()
@router.callback_query(F.data == "bc")
async def bc(c: CallbackQuery, state: FSMContext): await c.message.answer("Text:"); await state.set_state(AdminS.CAST)
@router.message(AdminS.CAST)
async def bc_run(m: Message, state: FSMContext):
    u = await db.get_all_users_ids()
    for i in u:
        try: await bot.send_message(i, m.text); await asyncio.sleep(0.05)
        except: pass
    await m.answer("Done"); await state.clear()

async def main():
    await db.init()
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub_bool(uid):
                w = Worker(uid)
                if await w.start(): W_POOL[uid] = w
        except: pass
    logger.info("🔥 TITAN REBORN")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
