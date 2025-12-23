#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro v66.4 - SECURE NUMPAD EDITION
----------------------------------------
Fixed: TypeError 'lang_pack'
New Feature: In-Line Numpad for Auth Code
Security: No text messages sent for code (Anti-Detect)
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
from typing import Dict, Set, Optional, List
from dataclasses import dataclass

from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties

from telethon import TelegramClient, events, types, functions
from telethon.errors import SessionPasswordNeededError, FloodWaitError, PhoneCodeInvalidError

# =========================================================================
# ⚙️ CONFIGURATION
# =========================================================================

@dataclass
class Config:
    # Берем переменные с хостинга
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    SUB_CHANNEL: str = "@STAT_PRO1"
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_numpad.db"
    
    # iOS Emulation (Без lang_pack, чтобы не было ошибок)
    DEVICE_MODEL: str = "iPhone 15 Pro"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"
    LANG_CODE: str = "en"
    SYSTEM_LANG_CODE: str = "en-US"
    
    def __post_init__(self):
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ FATAL: Нет переменных окружения!")
            sys.exit(1)
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)

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

    async def add_sub_days(self, uid: int, days: int):
        now = int(time.time())
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                curr = r[0] if (r and r[0]) else 0
        new_end = (curr if curr > now else now) + (days * 86400)
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return new_end

    async def use_promo(self, uid: int, code: str) -> int:
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code.strip(),)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code.strip(),))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            await db.commit()
        await self.add_sub_days(uid, days)
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"KEY-{random.randint(100,999)}-{random.randint(1000,9999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🦾 WORKER CORE
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid, self.client = uid, None
        self.spam_task = None
        self.raid_targets = set()
        self.react_map = {}
        self.ghost_mode = False
        self.afk_reason = None

    def _get_client(self, path):
        # FIX: Убрали lang_pack
        return TelegramClient(
            str(path), 
            cfg.API_ID, 
            cfg.API_HASH, 
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
        # --- HANDLERS ---
        @client.on(events.NewMessage(incoming=True))
        async def afk_h(e):
            if self.afk_reason and e.is_private and not e.out:
                try: await e.reply(f"💤 <b>AFK</b>: {self.afk_reason}", parse_mode='html')
                except: pass

        @client.on(events.NewMessage)
        async def auto_h(e):
            if e.chat_id in self.react_map and not e.out:
                try: await e.client(functions.messages.SendReactionRequest(peer=e.chat_id, msg_id=e.id, reaction=[types.ReactionEmoji(emoticon=self.react_map[e.chat_id])]))
                except: pass
            if e.sender_id in self.raid_targets:
                try: await e.reply(random.choice(["🤡", "🗑", "🤫", "L"]))
                except: pass

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(ping|p)$'))
        async def cmd_p(e):
            s = time.perf_counter()
            m = await e.edit("👻")
            await m.edit(f"📶 <b>Ping</b>: <code>{(time.perf_counter()-s)*1000:.2f}ms</code>", parse_mode='html')

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(s|spam)\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def cmd_s(e):
            t, c, d = e.pattern_match.group(2), int(e.pattern_match.group(3)), float(e.pattern_match.group(4))
            await e.delete()
            async def sl():
                for _ in range(c):
                    try: await client.send_message(e.chat_id, t); await asyncio.sleep(d)
                    except FloodWaitError as f: await asyncio.sleep(f.seconds + 2)
                    except: break
            self.spam_task = asyncio.create_task(sl())

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(sc|scan)(?:\s+(\d+))?'))
        async def cmd_sc(e):
            limit = int(e.pattern_match.group(2) or 100)
            await e.edit("🔎 Scanning...")
            users = []
            async for m in client.iter_messages(e.chat_id, limit=limit):
                if m.sender and isinstance(m.sender, types.User) and not m.sender.bot:
                    users.append([m.sender.id, m.sender.username or "", f"{m.sender.first_name or ''} {m.sender.last_name or ''}".strip()])
            
            out = io.StringIO()
            csv.writer(out).writerow(["ID", "Username", "Name"])
            csv.writer(out).writerows(users)
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig'))
            bio.name = "Scan.csv"
            await client.send_file("me", bio, caption=f"📊 Chat: {e.chat_id}\nUsers: {len(users)}", force_document=True)
            await e.edit("✅ Saved.")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.afk(?:\s+(.+))?'))
        async def cmd_afk(e):
            self.afk_reason = e.pattern_match.group(1)
            await e.edit(f"💤 AFK: {self.afk_reason or 'OFF'}")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(r|raid)$'))
        async def cmd_r(e):
            if not e.is_reply: return await e.edit("Reply!")
            tid = (await e.get_reply_message()).sender_id
            if tid in self.raid_targets: self.raid_targets.remove(tid); await e.edit("🕊 OFF")
            else: self.raid_targets.add(tid); await e.edit("☠️ ON")

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI & NUMPAD LOGIC
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminS(StatesGroup): U=State(); D=State(); PD=State(); PA=State(); CAST=State()

def get_numpad_kb(curr_code=""):
    # Генератор клавиатуры для кода
    kb = [
        [InlineKeyboardButton(text="1️⃣", callback_data="num_1"), InlineKeyboardButton(text="2️⃣", callback_data="num_2"), InlineKeyboardButton(text="3️⃣", callback_data="num_3")],
        [InlineKeyboardButton(text="4️⃣", callback_data="num_4"), InlineKeyboardButton(text="5️⃣", callback_data="num_5"), InlineKeyboardButton(text="6️⃣", callback_data="num_6")],
        [InlineKeyboardButton(text="7️⃣", callback_data="num_7"), InlineKeyboardButton(text="8️⃣", callback_data="num_8"), InlineKeyboardButton(text="9️⃣", callback_data="num_9")],
        [InlineKeyboardButton(text="🔙 Стереть", callback_data="num_del"), InlineKeyboardButton(text="0️⃣", callback_data="num_0"), InlineKeyboardButton(text="✅ Войти", callback_data="num_go")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_main(uid):
    btns = [[InlineKeyboardButton(text="📚 Команды", callback_data="help")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🔑 Вход", callback_data="auth")]]
    if uid == cfg.ADMIN_ID: btns.append([InlineKeyboardButton(text="👑 Админ", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

# --- START & MENU ---
@router.message(CommandStart())
async def st(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username)
    await m.answer(f"💎 <b>StatPro Numpad</b>\nID: <code>{m.from_user.id}</code>", reply_markup=kb_main(m.from_user.id))

@router.callback_query(F.data == "help")
async def h_cb(c: CallbackQuery):
    await c.message.edit_text("<code>.p</code>, <code>.s</code>, <code>.sc</code>, <code>.r</code>, <code>.afk</code>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))

@router.callback_query(F.data == "profile")
async def p_cb(c: CallbackQuery):
    sub = await db.check_sub_bool(c.from_user.id)
    await c.message.edit_text(f"👤 ID: <code>{c.from_user.id}</code>\nSub: {'✅' if sub else '❌'}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Промо", callback_data="promo")],[InlineKeyboardButton(text="🔙", callback_data="back")]]))

@router.callback_query(F.data == "promo")
async def pr_cb(c: CallbackQuery, state: FSMContext): await c.message.edit_text("🎟 Код:"); await state.set_state(PromoS.CODE)
@router.message(PromoS.CODE)
async def pr_m(m: Message, state: FSMContext):
    if await db.use_promo(m.from_user.id, m.text): await m.answer("✅ OK"); await st(m, state)
    else: await m.answer("❌ Error"); await st(m, state)

@router.callback_query(F.data == "back")
async def bk_cb(c: CallbackQuery, state: FSMContext): await c.message.delete(); await st(c.message, state)

# --- AUTH FLOW ---
@router.callback_query(F.data == "auth")
async def au_cb(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("Sub needed!", True)
    await c.message.edit_text("🔑 Вход:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 QR", callback_data="qr"), InlineKeyboardButton(text="📱 Phone", callback_data="ph")]]))

# QR LOGIN
@router.callback_query(F.data == "qr")
async def qr_cb(c: CallbackQuery):
    cl = Worker(c.from_user.id)._get_client(cfg.SESSION_DIR / f"session_{c.from_user.id}")
    await cl.connect()
    if await cl.is_user_authorized(): await cl.disconnect(); return await c.answer("Уже!", True)
    qr = await cl.qr_login()
    b = io.BytesIO(); qrcode.make(qr.url).save(b, "PNG"); b.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="📸 Scan QR")
    try:
        await qr.wait(60); await msg.delete(); await c.message.answer("✅ OK")
        w=Worker(c.from_user.id); await w.start(); W_POOL[c.from_user.id]=w
    except: await msg.delete(); await c.message.answer("❌ Timeout")
    finally: await cl.disconnect()

# PHONE LOGIN (WITH NUMPAD)
@router.callback_query(F.data == "ph")
async def ph_cb(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 <b>Введите номер (с кодом):</b>\nНапример: 79001234567"); await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def ph_s(m: Message, state: FSMContext):
    uid = m.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        sent = await cl.send_code_request(m.text)
        await state.update_data(ph=m.text, h=sent.phone_code_hash, uid=uid, input_code="")
        await cl.disconnect()
        # ВЫВОДИМ КЛАВИАТУРУ
        await m.answer(
            f"📩 <b>Введите код для {m.text}:</b>\n\nКод: ", 
            reply_markup=get_numpad_kb()
        )
        await state.set_state(AuthS.CO)
    except Exception as e:
        await cl.disconnect(); await m.answer(f"❌ Error: {e}")

# NUMPAD HANDLER
@router.callback_query(F.data.startswith("num_"), AuthS.CO)
async def numpad_handle(c: CallbackQuery, state: FSMContext):
    action = c.data.split("_")[1]
    data = await state.get_data()
    curr = data.get("input_code", "")
    
    if action == "del":
        curr = curr[:-1]
    elif action == "go":
        # ПОПЫТКА ВХОДА
        if not curr: return await c.answer("Введите код!", True)
        await c.message.edit_text("⏳ Проверка...", reply_markup=None)
        
        cl = Worker(data['uid'])._get_client(cfg.SESSION_DIR / f"session_{data['uid']}")
        await cl.connect()
        try:
            await cl.sign_in(phone=data['ph'], code=curr, phone_code_hash=data['h'])
            await c.message.answer("✅ <b>Успешный вход!</b>")
            await cl.disconnect(); await state.clear()
            if data['uid'] not in W_POOL: w=Worker(data['uid']); await w.start(); W_POOL[data['uid']]=w
            await st(c.message, state)
            return
        except SessionPasswordNeededError:
            await c.message.answer("🔒 <b>Введите 2FA Пароль (текстом):</b>"); await cl.disconnect(); await state.set_state(AuthS.PA); return
        except PhoneCodeInvalidError:
            await c.message.answer("❌ Неверный код! Попробуйте снова.")
            curr = "" # Сброс при ошибке
        except Exception as e:
            await c.message.answer(f"❌ Ошибка: {e}"); await cl.disconnect(); return
    else:
        # Добавляем цифру (ограничим до 6 символов, т.к. коды обычно 5-6)
        if len(curr) < 6:
            curr += action
    
    # Обновляем состояние и сообщение
    await state.update_data(input_code=curr)
    
    # Визуальное отображение кода (например, 1-2-3)
    display_code = "-".join(list(curr)) if curr else "..."
    
    try:
        await c.message.edit_text(
            f"📩 <b>Введите код для {data['ph']}:</b>\n\nКод: <code>{display_code}</code>", 
            reply_markup=get_numpad_kb()
        )
    except: pass # Если текст не изменился

@router.message(AuthS.PA)
async def pa_s(m: Message, state: FSMContext):
    d = await state.get_data()
    cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
    await cl.connect()
    try:
        await cl.sign_in(password=m.text)
        await m.answer("✅ OK"); await cl.disconnect(); await state.clear()
        if d['uid'] not in W_POOL: w=Worker(d['uid']); await w.start(); W_POOL[d['uid']]=w
        await st(m, state)
    except Exception as e:
        await cl.disconnect(); await m.answer(f"❌ {e}")

# --- ADMIN ---
@router.callback_query(F.data == "adm")
async def adm_cb(c: CallbackQuery): await c.message.edit_text("👑 Admin:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Promo", callback_data="mk_p")],[InlineKeyboardButton(text="📢 Broadcast", callback_data="bc")]]))

@router.callback_query(F.data == "bc")
async def bc_cb(c: CallbackQuery, state: FSMContext): await c.message.answer("Text:"); await state.set_state(AdminS.CAST)
@router.message(AdminS.CAST)
async def bc_m(m: Message, state: FSMContext):
    u = await db.get_all_users_ids()
    for i in u:
        try: await bot.send_message(i, m.text); await asyncio.sleep(0.05)
        except: pass
    await m.answer("Done"); await state.clear()

@router.callback_query(F.data == "mk_p")
async def mk_p(c: CallbackQuery, state: FSMContext): await c.message.answer("Days?"); await state.set_state(AdminS.PD)
@router.message(AdminS.PD)
async def mk_pd(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Count?"); await state.set_state(AdminS.PA)
@router.message(AdminS.PA)
async def mk_pa(m: Message, state: FSMContext): 
    d=await state.get_data(); c=await db.create_promo(d['d'], int(m.text))
    await m.answer(f"Code: <code>{c}</code>"); await state.clear()

async def main():
    await db.init()
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub_bool(uid):
                w = Worker(uid)
                if await w.start(): W_POOL[uid] = w
        except: pass
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
