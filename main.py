#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro v70.2 - IMMORTAL EDITION
-----------------------------------
Architect: StatPro AI
Fixes:
1. ✅ Event Loop Lock (Non-blocking workers)
2. ✅ Topic/Forum Scanning (reply_to_top_id)
3. ✅ Auto-Reconnect Watchdog
4. ✅ FSM Memory Leaks fixed
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
import traceback
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

# --- AI CORE ---
try:
    from g4f.client import AsyncClient
    import g4f
    g4f.debug.logging = False
except ImportError:
    print("⚠️ Installing AI libs...")
    os.system("pip install -U g4f[all] curl_cffi aiohttp python-dateutil")
    from g4f.client import AsyncClient
    import g4f

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
    DB_PATH: Path = BASE_DIR / "statpro_immortal.db"
    TEMP_DIR: Path = BASE_DIR / "temp"
    
    DEVICE_MODEL: str = "iPhone 15 Pro Max"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ CRITICAL: Env variables missing.")
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

    async def check_sub_bool(self, uid: int) -> bool:
        if uid == cfg.ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                if not r or r[0] is None: return False
                return r[0] > int(time.time())

    async def upsert_user(self, uid: int, uname: str):
        now = int(time.time())
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, 0, ?)", (uid, uname, now))
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            
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
        code = f"IMMORTAL-{random.randint(100,999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🧠 AI ENGINE (ROBUST PROVIDERS)
# =========================================================================

async def safe_ai_request(system_prompt: str, user_content: str) -> str:
    """
    Ротация провайдеров для стабильности.
    """
    client = AsyncClient()
    # FIX: Updated providers list
    providers = [
        g4f.Provider.DeepInfra,
        g4f.Provider.Liaobots,
        g4f.Provider.Blackbox,
        g4f.Provider.PollinationsAI
    ]

    for provider in providers:
        try:
            response = await client.chat.completions.create(
                model="gpt-4o",
                provider=provider,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ]
            )
            res = response.choices[0].message.content
            if res and len(res) > 0:
                return res
        except Exception:
            continue
            
    return "❌ Все AI провайдеры перегружены. Попробуйте позже."

# =========================================================================
# 🦾 WORKER CORE (NON-BLOCKING)
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        self.spam_task = None
        self._connected = False

    def _get_client(self, path):
        return TelegramClient(
            str(path), cfg.API_ID, cfg.API_HASH, 
            device_model=cfg.DEVICE_MODEL, 
            system_version=cfg.SYSTEM_VERSION, 
            app_version=cfg.APP_VERSION,
            sequential_updates=False 
        )

    async def start(self):
        """
        FIX: Запуск без блокировки Event Loop
        """
        self.client = self._get_client(cfg.SESSION_DIR / f"session_{self.uid}")
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): return False
            
            # Watchdog task
            self._bind()
            asyncio.create_task(self._keep_alive())
            return True
        except Exception as e:
            logger.error(f"Worker {self.uid} start failed: {e}")
            return False

    async def _keep_alive(self):
        """
        FIX: Auto-Reconnect Watchdog
        """
        while True:
            try:
                await self.client.run_until_disconnected()
            except Exception as e:
                logger.warning(f"Worker {self.uid} disconnected: {e}. Reconnecting...")
                await asyncio.sleep(5)
                try: await self.client.connect()
                except: pass
            
            # Если мы здесь, значит run_until_disconnected завершился штатно (разлогин?) или упал
            if not await self.client.is_user_authorized():
                logger.warning(f"Worker {self.uid} unauthorized. Stopping.")
                break

    def _bind(self):
        cl = self.client

        # --- 🕵️‍♂️ .report (FORUM/TOPIC SUPPORT) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.report$'))
        async def report_cmd(e):
            await e.edit("⏳ <b>Скан топика...</b>")
            
            # FIX: Правильное определение ID топика
            topic_id = None
            if e.reply_to:
                # Пытаемся взять ID топика (reply_to_top_id)
                topic_id = e.reply_to.reply_to_top_id 
                # Если его нет, значит это и есть старт топика, или обычный чат
                if not topic_id:
                    topic_id = e.reply_to.reply_to_msg_id
            
            # Если мы просто в чате без реплая - topic_id = None (сканит весь чат)

            keywords = ['айти', 'вбив', 'номер', 'код', 'встал', 'слет', 'сек', 'ща', 'готово', 'сдох', 'взял', 'отстоял']
            logs = []
            
            try:
                # reply_to=topic_id фильтрует сообщения конкретного топика
                async for m in cl.iter_messages(e.chat_id, limit=1000, reply_to=topic_id):
                    if m.text and any(k in m.text.lower() for k in keywords):
                        ts = m.date.strftime("%H:%M")
                        name = m.sender.first_name if m.sender else "U"
                        logs.append(f"[{ts}] {name}: {m.text}")
            except Exception as ex:
                return await e.edit(f"❌ Error: {ex}")

            if not logs: return await e.edit("❌ Логи пусты.")
            
            logs = logs[::-1]
            logs_txt = "\n".join(logs)
            
            await e.edit(f"🧠 <b>Анализ ({len(logs)} строк)...</b>")
            
            prompt = """
            Анализируй логи воркеров. Строки: [Время] Имя: Текст.
            Воркфлоу:
            1. Старт: "айти"/"вбив"/"взял".
            2. Успех: Работа > 35 мин БЕЗ "слет"/"бан".
            3. Провал: "слет"/"бан"/"сдох".
            
            JSON (строго):
            [{"num": "номер", "time": "мин", "status": "✅" или "❌"}]
            """
            
            res = await safe_ai_request(prompt, logs_txt)
            
            try:
                json_str = re.sub(r'```json\s*|\s*```', '', res).strip()
                data = json.loads(json_str)
                txt = "📊 <b>REPORT:</b>\n"
                ok = 0
                for i in data:
                    st = i.get('status', '?')
                    txt += f"📱 {i.get('num','?')} | ⏱{i.get('time','0')} | {st}\n"
                    if "✅" in st: ok += 1
                txt += f"\n🏆 <b>Total OK: {ok}</b>"
                await e.edit(txt, parse_mode='html')
            except:
                await e.edit(f"📝 <b>Raw AI:</b>\n{res[:4000]}", parse_mode='html')

        # --- ⚡️ .g (TURBO) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.g(?: |$)(.*)'))
        async def quiz_cmd(e):
            await e.edit("⚡️")
            q = e.pattern_match.group(1)
            if not q and e.is_reply:
                r = await e.get_reply_message()
                q = r.text or r.caption
            
            if not q: return await e.edit("❌ ?")
            ans = await safe_ai_request("Quiz Master. Answer only 1-3 words. Correct answer only.", q)
            await e.edit(f"<b>{ans}</b>", parse_mode='html')

        # --- 🚀 .spam ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.(s|spam)\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def spam(e):
            txt, cnt, dly = e.pattern_match.group(2), int(e.pattern_match.group(3)), float(e.pattern_match.group(4))
            await e.delete()
            async def run():
                for _ in range(cnt):
                    try: await cl.send_message(e.chat_id, txt); await asyncio.sleep(dly)
                    except: break
            self.spam_task = asyncio.create_task(run())

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def stop_spam(e):
            if self.spam_task: self.spam_task.cancel(); self.spam_task = None
            await e.edit("🛑 Stopped.")

        # --- 🧬 .scan ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.scan$'))
        async def scan(e):
            await e.edit("🔎 Scanning...")
            users = {}
            async for m in cl.iter_messages(e.chat_id, limit=2000):
                if m.sender and isinstance(m.sender, types.User) and not m.sender.bot:
                    users[m.sender_id] = [m.sender.username or "", m.sender.first_name or ""]
            
            out = io.StringIO(); w = csv.writer(out); w.writerow(["ID", "User", "Name"])
            for uid, d in users.items(): w.writerow([uid, d[0], d[1]])
            out.seek(0)
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig')); bio.name = "Scan.csv"
            await cl.send_file("me", bio, caption=f"✅ {len(users)} users"); await e.edit("✅ Done.")

        # --- 📢 .all ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.all(?:\s+(.+))?'))
        async def tag_all(e):
            await e.delete()
            txt = e.pattern_match.group(1) or "."
            parts = await cl.get_participants(e.chat_id)
            chunk = []
            for p in parts:
                if not p.bot and not p.deleted:
                    chunk.append(f"<a href='tg://user?id={p.id}'>\u200b</a>")
                    if len(chunk) >= 5:
                        await cl.send_message(e.chat_id, txt + "".join(chunk), parse_mode='html')
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
        [InlineKeyboardButton(text="1", callback_data="n_1"), InlineKeyboardButton(text="2", callback_data="n_2"), InlineKeyboardButton(text="3", callback_data="n_3")],
        [InlineKeyboardButton(text="4", callback_data="n_4"), InlineKeyboardButton(text="5", callback_data="n_5"), InlineKeyboardButton(text="6", callback_data="n_6")],
        [InlineKeyboardButton(text="7", callback_data="n_7"), InlineKeyboardButton(text="8", callback_data="n_8"), InlineKeyboardButton(text="9", callback_data="n_9")],
        [InlineKeyboardButton(text="🔙", callback_data="n_del"), InlineKeyboardButton(text="0", callback_data="n_0"), InlineKeyboardButton(text="✅", callback_data="n_go")]
    ])

def kb_main(uid):
    btns = [
        [InlineKeyboardButton(text="🌪 Siphon", callback_data="siphon_start")],
        [InlineKeyboardButton(text="📚 Cmds", callback_data="help"), InlineKeyboardButton(text="👤 Profile", callback_data="profile")],
        [InlineKeyboardButton(text="🔑 Auth", callback_data="auth")]
    ]
    if uid == cfg.ADMIN_ID: btns.append([InlineKeyboardButton(text="👑 Admin", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username)
    await m.answer(f"💎 <b>StatPro IMMORTAL v70.2</b>\nID: <code>{m.from_user.id}</code>", reply_markup=kb_main(m.from_user.id))

# --- AUTH FLOW (Full) ---
@router.callback_query(F.data == "auth")
async def auth_start(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("No Sub!", True)
    await c.message.edit_text("Login:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="QR", callback_data="qr"), InlineKeyboardButton(text="Phone", callback_data="ph")]]))

@router.callback_query(F.data == "ph")
async def auth_ph(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("Phone (79...):")
    await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def auth_ph_get(m: Message, state: FSMContext):
    uid = m.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        sent = await cl.send_code_request(m.text)
        await state.update_data(ph=m.text, h=sent.phone_code_hash, uid=uid, c="")
        await cl.disconnect()
        await m.answer(f"Code for {m.text}:", reply_markup=get_numpad_kb())
        await state.set_state(AuthS.CO)
    except Exception as e:
        await cl.disconnect()
        await m.answer(f"Error: {e}")
        await state.clear() # FIX: FSM Clean

@router.callback_query(F.data.startswith("n_"), AuthS.CO)
async def auth_numpad(c: CallbackQuery, state: FSMContext):
    act = c.data.split("_")[1]
    d = await state.get_data()
    curr = d.get("c", "")
    
    if act == "del": curr = curr[:-1]
    elif act == "go":
        if not curr: return await c.answer("Empty!", True)
        await c.message.edit_text("⏳ ...")
        
        cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
        await cl.connect()
        try:
            await cl.sign_in(phone=d['ph'], code=curr, phone_code_hash=d['h'])
            await c.message.answer("✅ Success!")
            await cl.disconnect()
            await state.clear()
            w = Worker(d['uid'])
            if await w.start(): W_POOL[d['uid']] = w
            await start(c.message, state)
            return
        except SessionPasswordNeededError:
            await c.message.answer("🔒 2FA Password:")
            await cl.disconnect()
            await state.set_state(AuthS.PA)
            return
        except Exception as e:
            await c.message.answer(f"Error: {e}")
            await cl.disconnect()
            await state.clear()
            return
    else: curr += act
    
    await state.update_data(c=curr)
    try: await c.message.edit_text(f"Code: {curr}", reply_markup=get_numpad_kb())
    except: pass

@router.message(AuthS.PA)
async def auth_2fa(m: Message, state: FSMContext):
    d = await state.get_data()
    cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
    await cl.connect()
    try:
        await cl.sign_in(password=m.text)
        await m.answer("✅ Success!")
        await cl.disconnect()
        w = Worker(d['uid'])
        if await w.start(): W_POOL[d['uid']] = w
    except Exception as e:
        await m.answer(f"Error: {e}")
        await cl.disconnect()
    finally:
        await state.clear() # FIX: FSM Clean

# --- QR & REST ---
@router.callback_query(F.data == "qr")
async def qr_h(c: CallbackQuery, state: FSMContext):
    cl = Worker(c.from_user.id)._get_client(cfg.SESSION_DIR / f"session_{c.from_user.id}")
    await cl.connect()
    qr = await cl.qr_login()
    b = io.BytesIO(); qrcode.make(qr.url).save(b, "PNG"); b.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="Scan QR")
    try: 
        await qr.wait(60)
        await msg.delete()
        await c.message.answer("✅")
        w = Worker(c.from_user.id)
        if await w.start(): W_POOL[c.from_user.id] = w
    except: 
        await msg.delete()
        await c.message.answer("Timeout")
    finally:
        await cl.disconnect()
        await state.clear()

@router.callback_query(F.data == "help")
async def hlp(c: CallbackQuery): await c.message.edit_text("Cmds:\n.g\n.report\n.spam\n.scan", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))
@router.callback_query(F.data == "back")
async def bck(c: CallbackQuery, state: FSMContext): await c.message.delete(); await start(c.message, state)
@router.callback_query(F.data == "profile")
async def prf(c: CallbackQuery): 
    sub = await db.check_sub_bool(c.from_user.id)
    await c.message.edit_text(f"Sub: {'✅' if sub else '❌'}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Promo", callback_data="promo"), InlineKeyboardButton(text="🔙", callback_data="back")]]))
@router.callback_query(F.data == "promo")
async def prm(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Code:"); await state.set_state(PromoS.CODE)
@router.message(PromoS.CODE)
async def prm_use(m: Message, state: FSMContext): 
    d = await db.use_promo(m.from_user.id, m.text)
    if d: await m.answer(f"✅ +{d} days"); await start(m, state)
    else: await m.answer("❌ Invalid")
    await state.clear() # FIX

@router.callback_query(F.data == "adm")
async def adm(c: CallbackQuery): await c.message.edit_text("Admin:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Create Promo", callback_data="mk_p")]]))
@router.callback_query(F.data == "mk_p")
async def mk_p(c: CallbackQuery, state: FSMContext): await c.message.answer("Days?"); await state.set_state(AdminS.PD)
@router.message(AdminS.PD)
async def mk_pd(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Count?"); await state.set_state(AdminS.PA)
@router.message(AdminS.PA)
async def mk_pa(m: Message, state: FSMContext): 
    d=await state.get_data(); c=await db.create_promo(d['d'], int(m.text)); await m.answer(f"Code: <code>{c}</code>")
    await state.clear() # FIX

# --- MAIN ---
async def main():
    await db.init()
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            w = Worker(uid)
            # FIX: Non-blocking start
            if await w.start(): 
                W_POOL[uid] = w
        except: pass
            
    logger.info("🔥 StatPro IMMORTAL Started")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Exit.")
