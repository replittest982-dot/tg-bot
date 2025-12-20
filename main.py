#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
👻 StatPro v66.3 - STEALTH EDITION (ANTI-DETECT)
------------------------------------------------
Fixes:
1. "Session Reset" error (Desktop tag removed)
2. "Code Shared" blocking (Input masking added)
3. Native iOS Headers emulation
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
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# =========================================================================
# ⚙️ CONFIGURATION
# =========================================================================

@dataclass
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    SUB_CHANNEL: str = "@STAT_PRO1"
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_stealth.db"
    
    # 🕵️‍♂️ STEALTH CONFIGURATION (iOS Native Emulation)
    # Эти параметры критически важны чтобы убрать метку "Desktop"
    DEVICE_MODEL: str = "iPhone 15 Pro"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"
    LANG_CODE: str = "en"
    SYSTEM_LANG_CODE: str = "en-US"
    LANG_PACK: str = "ios"  # <-- CRITICAL FIX
    
    def __post_init__(self):
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ FATAL: Переменные окружения не найдены!")
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
        code = f"STEALTH-{random.randint(100,999)}-{random.randint(1000,9999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🦾 STEALTH WORKER
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
        # ⚠️ CRITICAL: Strict emulation of iOS Client to avoid "Desktop" tag
        return TelegramClient(
            str(path), 
            cfg.API_ID, 
            cfg.API_HASH, 
            device_model=cfg.DEVICE_MODEL, 
            system_version=cfg.SYSTEM_VERSION, 
            app_version=cfg.APP_VERSION,
            lang_code=cfg.LANG_CODE,
            system_lang_code=cfg.SYSTEM_LANG_CODE,
            lang_pack=cfg.LANG_PACK  # Force iOS packet structure
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

        @client.on(events.NewMessage(incoming=True))
        async def afk_h(e):
            if self.afk_reason and e.is_private and not e.out:
                try: await e.reply(f"💤 <b>AFK Mode</b>\nStatus: {self.afk_reason}", parse_mode='html')
                except: pass

        @client.on(events.NewMessage)
        async def auto_h(e):
            if e.chat_id in self.react_map and not e.out:
                try: await e.client(functions.messages.SendReactionRequest(peer=e.chat_id, msg_id=e.id, reaction=[types.ReactionEmoji(emoticon=self.react_map[e.chat_id])]))
                except: pass
            if e.sender_id in self.raid_targets:
                try: await e.reply(random.choice(["🤡", "🗑", "🤫", "L"]))
                except: pass

        # ALIASES: .p, .s, .r, .sc
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(ping|p)$'))
        async def cmd_p(e):
            s = time.perf_counter()
            m = await e.edit("👻")
            await m.edit(f"👻 <b>Stealth Ping</b>: <code>{(time.perf_counter()-s)*1000:.2f}ms</code>", parse_mode='html')

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
            await e.edit("🔎 Stealth Scan...")
            users = []
            async for m in client.iter_messages(e.chat_id, limit=limit):
                if m.sender and isinstance(m.sender, User) and not m.sender.bot:
                    users.append([m.sender.id, m.sender.username or "", f"{m.sender.first_name or ''} {m.sender.last_name or ''}".strip()])
            
            out = io.StringIO()
            csv.writer(out).writerow(["ID", "Username", "Name"])
            csv.writer(out).writerows(users)
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig'))
            bio.name = "Scan_Stealth.csv"
            await client.send_file("me", bio, caption=f"📊 Chat: {e.chat_id}\nUsers: {len(users)}", force_document=True)
            await e.edit("✅ Report sent to Saved Messages.")

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
# 🤖 BOT UI
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminS(StatesGroup): U=State(); D=State(); PD=State(); PA=State(); CAST=State()

def kb_main(uid):
    btns = [[InlineKeyboardButton(text="📚 Команды", callback_data="help")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🔑 Вход", callback_data="auth")]]
    if uid == cfg.ADMIN_ID: btns.append([InlineKeyboardButton(text="👑 Админ", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

@router.message(CommandStart())
async def st(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username)
    await m.answer(f"👻 <b>StatPro Stealth</b>\nID: <code>{m.from_user.id}</code>", reply_markup=kb_main(m.from_user.id))

@router.callback_query(F.data == "help")
async def h_cb(c: CallbackQuery):
    await c.message.edit_text("<code>.p</code> - Пинг\n<code>.s [txt] [cnt] [delay]</code> - Спам\n<code>.sc [limit]</code> - Скан\n<code>.r</code> - Рейд\n<code>.afk [reason]</code> - АФК", 
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))

@router.callback_query(F.data == "profile")
async def p_cb(c: CallbackQuery):
    sub = await db.check_sub_bool(c.from_user.id)
    await c.message.edit_text(f"👤 ID: <code>{c.from_user.id}</code>\nSub: {'✅ Active' if sub else '❌ Expired'}", 
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Промо", callback_data="promo")],[InlineKeyboardButton(text="🔙", callback_data="back")]]))

@router.callback_query(F.data == "promo")
async def pr_cb(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("🎟 Введите промокод:"); await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def pr_m(m: Message, state: FSMContext):
    if await db.use_promo(m.from_user.id, m.text): await m.answer("✅ Успешно!"); await st(m, state)
    else: await m.answer("❌ Неверный код."); await st(m, state)

@router.callback_query(F.data == "auth")
async def au_cb(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("Требуется подписка!", True)
    await c.message.edit_text(
        "🔑 <b>Безопасный Вход</b>\n\n"
        "⚠️ <b>РЕКОМЕНДУЕТСЯ:</b> Используйте QR-вход. Это на 100% защищает от блокировки 'Session Reset'.\n\n"
        "Если используете код, вводите его с разделителями (1-2-3-4-5)!", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 QR (Рекомендуем)", callback_data="qr"), InlineKeyboardButton(text="📱 Код (Риск)", callback_data="ph")]]))

@router.callback_query(F.data == "qr")
async def qr_cb(c: CallbackQuery):
    cl = Worker(c.from_user.id)._get_client(cfg.SESSION_DIR / f"session_{c.from_user.id}")
    await cl.connect()
    if await cl.is_user_authorized(): await cl.disconnect(); return await c.answer("Уже вошли!", True)
    
    qr = await cl.qr_login()
    b = io.BytesIO(); qrcode.make(qr.url).save(b, "PNG"); b.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="📸 <b>Сканируйте в Telegram</b>\nНастройки > Устройства > Подключить")
    try:
        await qr.wait(60)
        await msg.delete(); await c.message.answer("✅ <b>Успешно!</b> Сессия сохранена.")
        w = Worker(c.from_user.id); await w.start(); W_POOL[c.from_user.id] = w
    except: await msg.delete(); await c.message.answer("❌ Время вышло.")
    finally: await cl.disconnect()

@router.callback_query(F.data == "ph")
async def ph_cb(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 <b>Номер телефона:</b>\n(Пример: 79001234567)"); await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def ph_s(m: Message, state: FSMContext):
    uid = m.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        sent = await cl.send_code_request(m.text)
        await state.update_data(ph=m.text, h=sent.phone_code_hash, uid=uid)
        await cl.disconnect()
        # ВАЖНАЯ ИНСТРУКЦИЯ ДЛЯ ОБХОДА БЛОКИРОВКИ
        await m.answer(
            "📩 <b>Введите код ИНАЧЕ!</b>\n\n"
            "Telegram блокирует, если просто переслать код.\n"
            "Введите цифры через дефис или точку.\n"
            "✅ Правильно: <code>1-2-3-4-5</code>\n"
            "❌ Ошибка: <code>12345</code>"
        )
        await state.set_state(AuthS.CO)
    except Exception as e:
        await cl.disconnect(); await m.answer(f"❌ Ошибка: {e}")

@router.message(AuthS.CO)
async def co_s(m: Message, state: FSMContext):
    # ОЧИСТКА КОДА ОТ МАСКИРОВКИ
    raw_code = m.text
    clean_code = re.sub(r'\D', '', raw_code) # Убираем все кроме цифр
    
    d = await state.get_data()
    cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
    await cl.connect()
    try:
        # Пауза перед отправкой для имитации человека
        await asyncio.sleep(random.uniform(0.5, 1.5))
        await cl.sign_in(phone=d['ph'], code=clean_code, phone_code_hash=d['h'])
        await m.answer("✅ Вход выполнен!"); await cl.disconnect(); await state.clear()
        if d['uid'] not in W_POOL: w=Worker(d['uid']); await w.start(); W_POOL[d['uid']]=w
        await st(m, state)
    except SessionPasswordNeededError:
        await m.answer("🔒 Введите 2FA пароль:"); await cl.disconnect(); await state.set_state(AuthS.PA)
    except Exception as e:
        await cl.disconnect(); await m.answer(f"❌ Ошибка входа: {e}")

@router.message(AuthS.PA)
async def pa_s(m: Message, state: FSMContext):
    d = await state.get_data()
    cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
    await cl.connect()
    try:
        await cl.sign_in(password=m.text)
        await m.answer("✅ Пароль принят!"); await cl.disconnect(); await state.clear()
        if d['uid'] not in W_POOL: w=Worker(d['uid']); await w.start(); W_POOL[d['uid']]=w
        await st(m, state)
    except Exception as e:
        await cl.disconnect(); await m.answer(f"❌ Пароль неверный: {e}")

@router.callback_query(F.data == "adm")
async def adm_cb(c: CallbackQuery):
    await c.message.edit_text("👑 Admin:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Промо", callback_data="mk_p")],[InlineKeyboardButton(text="📢 Рассылка", callback_data="bc")]]))

@router.callback_query(F.data == "bc")
async def bc_cb(c: CallbackQuery, state: FSMContext):
    await c.message.answer("Текст:"); await state.set_state(AdminS.CAST)

@router.message(AdminS.CAST)
async def bc_m(m: Message, state: FSMContext):
    u = await db.get_all_users_ids()
    await m.answer(f"🚀 Рассылка на {len(u)}...")
    for i in u:
        try: await bot.send_message(i, m.text); await asyncio.sleep(0.05)
        except: pass
    await m.answer("✅ Готово"); await state.clear()

@router.callback_query(F.data == "mk_p")
async def mk_p(c: CallbackQuery, state: FSMContext): await c.message.answer("Дней?"); await state.set_state(AdminS.PD)
@router.message(AdminS.PD)
async def mk_pd(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Кол-во?"); await state.set_state(AdminS.PA)
@router.message(AdminS.PA)
async def mk_pa(m: Message, state: FSMContext): 
    d=await state.get_data(); c=await db.create_promo(d['d'], int(m.text))
    await m.answer(f"Code: <code>{c}</code>"); await state.clear()

@router.callback_query(F.data == "back")
async def bk_cb(c: CallbackQuery, state: FSMContext): await c.message.delete(); await st(c.message, state)

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
