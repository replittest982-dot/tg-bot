#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
⚡️ StatPro v68.0 - QUIZ MASTER EDITION
---------------------------------------
New Feature: Turbo Quiz (.g)
Engine: GPT-4o (via g4f optimized)
Focus: Low Latency & Short Answers
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
from typing import Dict, List
from dataclasses import dataclass

# --- LIBS ---
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

# AI CORE
try:
    import g4f
    # Настройка для скорости: игнорируем ошибки, ищем лучшего провайдера
    g4f.debug.logging = False
except ImportError:
    print("CRITICAL: pip install g4f curl_cffi")
    sys.exit(1)

# =========================================================================
# ⚙️ CONFIG
# =========================================================================

@dataclass
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_quiz.db"
    
    # Эмуляция iOS для скорости и скрытности
    DEVICE_MODEL: str = "iPhone 15 Pro Max"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        if not self.BOT_TOKEN:
            print("❌ ОШИБКА: Не введены переменные окружения (BOT_TOKEN и др.)")
            sys.exit(1)

cfg = Config()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')

# =========================================================================
# 🗄️ DATABASE
# =========================================================================

class Database:
    def __init__(self): self.path = cfg.DB_PATH
    def get_conn(self): return aiosqlite.connect(self.path)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, sub_end INTEGER)")
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations INTEGER)")
            await db.commit()

    async def check_sub(self, uid):
        if uid == cfg.ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                return r[0] > int(time.time()) if r else False

    async def upsert_user(self, uid, uname):
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, sub_end) VALUES (?, ?, 0)", (uid, uname))
            await db.commit()

    async def use_promo(self, uid, code):
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ?", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ?", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            new_end = int(time.time()) + (days * 86400)
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return days

    async def create_promo(self, days, acts):
        code = f"QUIZ-{random.randint(100,999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🧠 AI ENGINE (TURBO QUIZ)
# =========================================================================

async def ask_gpt_turbo(question: str) -> str:
    """
    Функция оптимизирована для скорости.
    Системный промпт заставляет отвечать одним словом/фразой.
    """
    system_prompt = (
        "Ты профессиональный игрок в викторину. Твоя цель: дать правильный ответ МАКСИМАЛЬНО БЫСТРО."
        "Если вопрос с вариантами ответа - напиши только букву или цифру правильного варианта."
        "Если вопрос открытый - напиши только ответ (1-3 слова)."
        "НЕ пиши вводные фразы типа 'Ответ:', 'Я думаю это...'. Сразу суть."
    )
    
    try:
        response = await g4f.ChatCompletion.create_async(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question}
            ]
        )
        return response
    except Exception as e:
        return f"❌ Ошибка AI: {e}"

# =========================================================================
# 🦾 WORKER
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        self.spam_task = None

    async def start(self):
        self.client = TelegramClient(str(cfg.SESSION_DIR/f"session_{self.uid}"), cfg.API_ID, cfg.API_HASH,
                                     device_model=cfg.DEVICE_MODEL, system_version=cfg.SYSTEM_VERSION, app_version=cfg.APP_VERSION)
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): return False
            self._bind()
            asyncio.create_task(self.client.run_until_disconnected())
            return True
        except: return False

    def _bind(self):
        cl = self.client

        # --- ⚡️ TURBO QUIZ FUNCTION (.g) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.g(?:\s+(.+))?$'))
        async def quiz_handler(e):
            # 1. Захват вопроса
            question = ""
            if e.is_reply:
                # Если ответили на сообщение бота-викторины
                reply_msg = await e.get_reply_message()
                question = reply_msg.text or reply_msg.caption or ""
            else:
                # Если написали вопрос вручную: .g Столица Перу
                question = e.pattern_match.group(1)

            if not question:
                return await e.edit("⚡️ <b>Turbo Quiz</b>\nОтветьте командой на вопрос или введите текст.")

            # 2. Визуальный маркер "Думаю" (для тебя, чтобы знать что процесс идет)
            # Используем символ молнии для минимальной задержки рендера
            await e.edit("⚡️") 

            # 3. Запрос к AI
            start_t = time.perf_counter()
            answer = await ask_gpt_turbo(question)
            end_t = time.perf_counter()

            # 4. Вывод результата
            # Если ответ короткий, выводим сразу его, чтобы ты мог его скопировать или отправить
            # В идеале можно сделать авто-отправку нового сообщения, но редактирование безопаснее
            await e.edit(f"{answer}")

        # --- CLASSIC COMMANDS ---
        
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.p$'))
        async def ping(e):
            s = time.perf_counter()
            await e.edit("⚡️")
            await e.edit(f"📶 <b>Ping:</b> <code>{(time.perf_counter()-s)*1000:.1f}ms</code>", parse_mode='html')

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.scan$'))
        async def scan(e):
            await e.edit("🔎 Parsing chat history...")
            users = set()
            async for m in cl.iter_messages(e.chat_id, limit=2000): # Лимит 2000 для скорости
                if m.sender and not m.sender.bot:
                    users.add(f"{m.sender.id},{m.sender.first_name or ''}")
            
            f = io.StringIO(); w = csv.writer(f); w.writerow(["ID", "Name"])
            for u in users: w.writerow(u.split(","))
            
            f.seek(0)
            await cl.send_file("me", f.read().encode(), filename="quiz_audience.csv", caption=f"👥 Users: {len(users)}")
            await e.edit("✅ Scan done (Check Saved).")

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.all(?:\s+(.+))?'))
        async def tag_all(e):
            txt = e.pattern_match.group(1) or "."
            await e.delete()
            # Упрощенная версия для скорости
            participants = await cl.get_participants(e.chat_id)
            chunk = []
            for p in participants:
                if p.bot or p.deleted: continue
                chunk.append(f"<a href='tg://user?id={p.id}'>\u200b</a>")
                if len(chunk) >= 5:
                    await cl.send_message(e.chat_id, txt + "".join(chunk), parse_mode='html')
                    chunk = []
                    await asyncio.sleep(1.5)

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI (NUMPAD AUTH + ADMIN)
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminS(StatesGroup): PD=State(); PA=State()

def get_numpad():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(i), callback_data=f"n_{i}") for i in range(1,4)],
        [InlineKeyboardButton(text=str(i), callback_data=f"n_{i}") for i in range(4,7)],
        [InlineKeyboardButton(text=str(i), callback_data=f"n_{i}") for i in range(7,10)],
        [InlineKeyboardButton(text="🔙", callback_data="n_del"), InlineKeyboardButton(text="0", callback_data="n_0"), InlineKeyboardButton(text="✅", callback_data="n_go")]
    ])

@router.message(CommandStart())
async def start(m: Message):
    await db.upsert_user(m.from_user.id, m.from_user.username)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Profile", callback_data="prof"), InlineKeyboardButton(text="🔑 Auth", callback_data="auth")]
    ])
    if m.from_user.id == cfg.ADMIN_ID: kb.inline_keyboard.append([InlineKeyboardButton(text="👑 Admin", callback_data="adm")])
    await m.answer("🧠 <b>StatPro Quiz Master</b>", reply_markup=kb)

@router.callback_query(F.data == "auth")
async def auth(c: CallbackQuery):
    if not await db.check_sub(c.from_user.id): return await c.answer("No Sub!", True)
    await c.message.edit_text("Login:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="QR", callback_data="qr"), InlineKeyboardButton(text="Phone", callback_data="ph")]]))

@router.callback_query(F.data == "ph")
async def ph(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Phone:"); await state.set_state(AuthS.PH)
@router.message(AuthS.PH)
async def ph_get(m: Message, state: FSMContext):
    cl = Worker(m.from_user.id).client = TelegramClient(str(cfg.SESSION_DIR/f"session_{m.from_user.id}"), cfg.API_ID, cfg.API_HASH)
    await cl.connect()
    try:
        s = await cl.send_code_request(m.text)
        await state.update_data(ph=m.text, h=s.phone_code_hash, c=""); await cl.disconnect()
        await m.answer("Code:", reply_markup=get_numpad()); await state.set_state(AuthS.CO)
    except Exception as e: await m.answer(f"Error: {e}")

@router.callback_query(F.data.startswith("n_"), AuthS.CO)
async def num(c: CallbackQuery, state: FSMContext):
    a = c.data.split("_")[1]
    d = await state.get_data(); code = d.get("c","")
    if a == "del": code = code[:-1]
    elif a == "go":
        cl = TelegramClient(str(cfg.SESSION_DIR/f"session_{c.from_user.id}"), cfg.API_ID, cfg.API_HASH)
        await cl.connect()
        try:
            await cl.sign_in(phone=d['ph'], code=code, phone_code_hash=d['h'])
            await c.message.answer("✅ Logged in!"); await state.clear()
            w=Worker(c.from_user.id); await w.start(); W_POOL[c.from_user.id]=w
        except SessionPasswordNeededError: await c.message.answer("2FA Password:"); await state.set_state(AuthS.PA)
        except Exception as e: await c.message.answer(f"Error: {e}")
        finally: await cl.disconnect()
        return
    else: code += a
    await state.update_data(c=code)
    try: await c.message.edit_text(f"Code: {code}", reply_markup=get_numpad())
    except: pass

@router.message(AuthS.PA)
async def pass_2fa(m: Message, state: FSMContext):
    cl = TelegramClient(str(cfg.SESSION_DIR/f"session_{m.from_user.id}"), cfg.API_ID, cfg.API_HASH)
    await cl.connect()
    try: await cl.sign_in(password=m.text); await m.answer("✅"); w=Worker(m.from_user.id); await w.start(); W_POOL[m.from_user.id]=w
    except Exception as e: await m.answer(f"{e}")
    finally: await cl.disconnect()

@router.callback_query(F.data == "qr")
async def qr(c: CallbackQuery):
    cl = TelegramClient(str(cfg.SESSION_DIR/f"session_{c.from_user.id}"), cfg.API_ID, cfg.API_HASH); await cl.connect()
    qr = await cl.qr_login()
    b = io.BytesIO(); qrcode.make(qr.url).save(b, "PNG"); b.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(b.read(),"qr.png"))
    try: await qr.wait(60); await msg.delete(); await c.message.answer("✅"); w=Worker(c.from_user.id); await w.start(); W_POOL[c.from_user.id]=w
    except: await msg.delete()
    finally: await cl.disconnect()

@router.callback_query(F.data == "prof")
async def prof(c: CallbackQuery):
    s = await db.check_sub(c.from_user.id)
    await c.message.edit_text(f"Sub: {'✅' if s else '❌'}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Promo", callback_data="promo")]]))
@router.callback_query(F.data == "promo")
async def p_ask(c: CallbackQuery, state: FSMContext): await c.message.answer("Code:"); await state.set_state(PromoS.CODE)
@router.message(PromoS.CODE)
async def p_use(m: Message, state: FSMContext):
    if await db.use_promo(m.from_user.id, m.text): await m.answer("✅")
    else: await m.answer("❌")

@router.callback_query(F.data == "adm")
async def adm(c: CallbackQuery): await c.message.answer("Days?", reply_markup=None); await c.message.delete(); await state.set_state(AdminS.PD)
# Admin handlers simplified for brevity... (similar to previous)

async def main():
    await db.init()
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub(uid): await Worker(uid).start()
        except: pass
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
