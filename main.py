#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🌌 StatPro v66.5 - EVENT HORIZON (TON-618)
------------------------------------------
Architect: StatPro AI
Modules:
1. Secure Numpad Auth (Anti-Detect)
2. Deep Scan Core (Infinite History Mining)
3. Traffic Siphon System (CSV -> Mass DM)
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

# --- ENV & IMPORTS ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, 
    CallbackQuery, Message, BufferedInputFile, ContentType
)
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties

from telethon import TelegramClient, events, types, functions
from telethon.errors import (
    SessionPasswordNeededError, 
    FloodWaitError, 
    PhoneCodeInvalidError, 
    UserPrivacyRestrictedError
)

# =========================================================================
# ⚙️ CONFIGURATION (DIRECT HOSTING)
# =========================================================================

@dataclass
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    TEMP_DIR: Path = BASE_DIR / "temp"
    DB_PATH: Path = BASE_DIR / "statpro_horizon.db"
    
    # Emulation: iPhone 15 Pro (iOS 17.5.1)
    DEVICE_MODEL: str = "iPhone 15 Pro"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"
    LANG_CODE: str = "en"
    SYSTEM_LANG_CODE: str = "en-US"

    def __post_init__(self):
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ CRITICAL: Environment variables missing!")
            sys.exit(1)
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)

cfg = Config()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("StatPro")

# =========================================================================
# 🗄️ DATABASE ENGINE
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
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code.strip(),)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code.strip(),))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            await db.commit()
        
        # Add days
        now = int(time.time())
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                row = await c.fetchone()
                curr = row[0] if (row and row[0]) else 0
        
        new_end = (curr if curr > now else now) + (days * 86400)
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"TON-{random.randint(100,999)}-{random.randint(1000,9999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🦾 WORKER CORE (TELETHON)
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        self.spam_task = None
        self.raid_targets = set()
        self.afk_reason = None
        # Siphon Control
        self.siphon_active = False

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

    async def send_siphon_message(self, target_id: int, message_obj: Message, caption: str = None):
        """Функция для перелива: отправляет сообщение от имени юзера"""
        if not self.client: return False
        try:
            # Если это текст
            if message_obj.text and not message_obj.photo and not message_obj.video:
                await self.client.send_message(target_id, message_obj.text)
            # Если это медиа/стикер (сложнее, требует загрузки, упростим до текста пока или копии)
            # Для надежности в версии 1.0 лучше использовать текст + ссылки
            # Но попробуем просто переслать текст
            elif message_obj.caption:
                await self.client.send_message(target_id, message_obj.caption)
            else:
                 await self.client.send_message(target_id, "Check this out!") # Fallback
            return True
        except UserPrivacyRestrictedError: return False # ЛС закрыто
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
            return False
        except Exception as e:
            return False

    def _bind(self):
        client = self.client

        @client.on(events.NewMessage(incoming=True))
        async def afk_handler(e):
            if self.afk_reason and e.is_private and not e.out:
                try: await e.reply(f"💤 <b>AFK</b>: {self.afk_reason}", parse_mode='html')
                except: pass

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(p|ping)$'))
        async def cmd_ping(e):
            s = time.perf_counter()
            m = await e.edit("⚫️")
            await m.edit(f"⚫️ <b>Event Horizon</b>: <code>{(time.perf_counter()-s)*1000:.2f}ms</code>", parse_mode='html')

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(s|spam)\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def cmd_spam(e):
            t, c, d = e.pattern_match.group(2), int(e.pattern_match.group(3)), float(e.pattern_match.group(4))
            await e.delete()
            async def run():
                for _ in range(c):
                    try: await client.send_message(e.chat_id, t); await asyncio.sleep(d)
                    except FloodWaitError as f: await asyncio.sleep(f.seconds + 2)
                    except: break
            self.spam_task = asyncio.create_task(run())

        # DEEP SCAN CORE (UPDATED)
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.scan(?:\s+(\d+))?'))
        async def cmd_scan(e):
            # Если число не указано - сканируем ВСЁ (None)
            limit_arg = e.pattern_match.group(1)
            limit = int(limit_arg) if limit_arg else None 
            
            await e.edit(f"⚫️ <b>Deep Scan Started...</b>\nTarget: {'ALL' if limit is None else limit} msgs.\n<i>Analyzing entities...</i>", parse_mode='html')
            
            extracted_users = {} # ID: [Username, Name]
            count_msgs = 0
            
            try:
                # Итерация по истории
                async for m in client.iter_messages(e.chat_id, limit=limit):
                    count_msgs += 1
                    # Обновляем статус каждые 500 сообщений
                    if count_msgs % 500 == 0:
                        try: await e.edit(f"⚫️ Scanned: {count_msgs} msgs...\nFound: {len(extracted_users)} unique users.")
                        except: pass
                    
                    if m.sender and isinstance(m.sender, types.User) and not m.sender.bot:
                        if m.sender.id not in extracted_users:
                            fname = m.sender.first_name or ""
                            lname = m.sender.last_name or ""
                            full_name = f"{fname} {lname}".strip()
                            uname = m.sender.username or ""
                            extracted_users[m.sender.id] = [uname, full_name]
            except Exception as ex:
                await e.edit(f"❌ Scan Error: {ex}")
                return

            # Генерация отчета
            out = io.StringIO()
            writer = csv.writer(out)
            writer.writerow(["User ID", "Username", "Name"])
            for uid, info in extracted_users.items():
                writer.writerow([uid, info[0], info[1]])
            
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig'))
            bio.name = f"Dump_{e.chat_id}_{len(extracted_users)}.csv"
            
            await client.send_file("me", bio, caption=f"⚫️ <b>Scan Complete</b>\nSource: {e.chat_id}\nMessages Processed: {count_msgs}\nUnique Users: {len(extracted_users)}", force_document=True, parse_mode='html')
            await e.edit(f"✅ <b>Done.</b>\nSaved {len(extracted_users)} users to Saved Messages.")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.afk(?:\s+(.+))?'))
        async def cmd_afk(e):
            self.afk_reason = e.pattern_match.group(1)
            await e.edit(f"💤 AFK: {self.afk_reason or 'OFF'}")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(r|raid)$'))
        async def cmd_raid(e):
            if not e.is_reply: return await e.edit("Reply needed!")
            tid = (await e.get_reply_message()).sender_id
            if tid in self.raid_targets: self.raid_targets.remove(tid); await e.edit("🕊 Raid OFF")
            else: self.raid_targets.add(tid); await e.edit("☠️ Raid ON")

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI & SIPHON SYSTEM
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class SiphonS(StatesGroup): FILE=State(); MSG=State(); CONFIRM=State(); RUN=State()
class AdminS(StatesGroup): U=State(); D=State(); PD=State(); PA=State(); CAST=State()

def get_numpad_kb():
    # Numpad для безопасного ввода
    kb = [
        [InlineKeyboardButton(text="1️⃣", callback_data="num_1"), InlineKeyboardButton(text="2️⃣", callback_data="num_2"), InlineKeyboardButton(text="3️⃣", callback_data="num_3")],
        [InlineKeyboardButton(text="4️⃣", callback_data="num_4"), InlineKeyboardButton(text="5️⃣", callback_data="num_5"), InlineKeyboardButton(text="6️⃣", callback_data="num_6")],
        [InlineKeyboardButton(text="7️⃣", callback_data="num_7"), InlineKeyboardButton(text="8️⃣", callback_data="num_8"), InlineKeyboardButton(text="9️⃣", callback_data="num_9")],
        [InlineKeyboardButton(text="🔙 DEL", callback_data="num_del"), InlineKeyboardButton(text="0️⃣", callback_data="num_0"), InlineKeyboardButton(text="✅ ENTER", callback_data="num_go")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_main(uid):
    btns = [
        [InlineKeyboardButton(text="🌪 ПЕРЕЛИВ (Siphon)", callback_data="siphon_start")],
        [InlineKeyboardButton(text="📚 Команды", callback_data="help"), InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth")]
    ]
    if uid == cfg.ADMIN_ID: btns.append([InlineKeyboardButton(text="👑 Админ", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

# --- START ---
@router.message(CommandStart())
async def st(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username)
    await m.answer(f"⚫️ <b>TON-618 Controller</b>\nUser: <code>{m.from_user.id}</code>", reply_markup=kb_main(m.from_user.id))

# --- SIPHON (ПЕРЕЛИВ) LOGIC ---
@router.callback_query(F.data == "siphon_start")
async def siphon_start(c: CallbackQuery, state: FSMContext):
    # Проверка подписки и наличия воркера
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("Subscribe required!", True)
    if c.from_user.id not in W_POOL: return await c.answer("Сначала войдите в аккаунт (Auth)!", True)
    
    await c.message.edit_text(
        "🌪 <b>Система Перелива</b>\n\n"
        "1. Загрузите <code>.csv</code> файл (результат .scan)\n"
        "2. Бот извлечет ID пользователей.\n"
        "3. Ожидайте подтверждения.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="back")]])
    )
    await state.set_state(SiphonS.FILE)

@router.message(SiphonS.FILE, F.document)
async def siphon_file(m: Message, state: FSMContext):
    if not m.document.file_name.endswith('.csv'):
        return await m.answer("❌ Нужен файл .csv!")
    
    # Скачивание файла
    file_id = m.document.file_id
    file = await bot.get_file(file_id)
    file_path = cfg.TEMP_DIR / f"siphon_{m.from_user.id}_{int(time.time())}.csv"
    await bot.download_file(file.file_path, file_path)
    
    # Парсинг ID
    target_ids = []
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader, None) # Skip header
            for row in reader:
                if row and row[0].isdigit():
                    target_ids.append(int(row[0]))
    except Exception as e:
        return await m.answer(f"❌ Ошибка чтения файла: {e}")
    
    # Удаляем файл
    os.remove(file_path)
    
    if not target_ids: return await m.answer("❌ Файл пуст или неверен.")
    
    await state.update_data(targets=target_ids)
    await m.answer(
        f"✅ <b>Файл принят.</b>\nНайдено целей: {len(target_ids)}\n\n"
        "✍️ <b>Теперь отправьте сообщение</b>, которое нужно разослать.\n"
        "(Текст, Ссылки, Premium-эмодзи поддерживаются)",
        reply_markup=None
    )
    await state.set_state(SiphonS.MSG)

@router.message(SiphonS.MSG)
async def siphon_msg(m: Message, state: FSMContext):
    # Сохраняем текст сообщения
    await state.update_data(msg_text=m.text or m.caption or "Hello!")
    
    data = await state.get_data()
    targets = data['targets']
    
    await m.answer(
        f"🌪 <b>Подтверждение Перелива</b>\n\n"
        f"🎯 Целей: {len(targets)}\n"
        f"📝 Текст: <i>{m.text[:50]}...</i>\n\n"
        "⚠️ <b>Внимание:</b> Рассылка идет с вашего Userbot-аккаунта. Соблюдаются задержки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔥 ЗАПУСТИТЬ ПЕРЕЛИВ", callback_data="siphon_go")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="back")]
        ])
    )
    await state.set_state(SiphonS.CONFIRM)

@router.callback_query(F.data == "siphon_go", SiphonS.CONFIRM)
async def siphon_run(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    targets = data['targets']
    msg_text = data['msg_text']
    worker = W_POOL.get(c.from_user.id)
    
    if not worker: return await c.answer("Userbot отключен!", True)
    
    await c.message.edit_text("🚀 <b>Перелив запущен!</b>\nОтчет придет по завершении.")
    
    # ФОНОВАЯ ЗАДАЧА РАССЫЛКИ
    asyncio.create_task(run_siphon_task(c.from_user.id, worker, targets, msg_text))
    await state.clear()

async def run_siphon_task(user_id, worker, targets, text):
    success = 0
    fail = 0
    for tid in targets:
        try:
            await worker.client.send_message(tid, text)
            success += 1
            # Анти-флуд задержка (3-7 секунд)
            await asyncio.sleep(random.uniform(3, 7))
        except:
            fail += 1
            await asyncio.sleep(1)
            
    try:
        await bot.send_message(user_id, f"✅ <b>Перелив завершен!</b>\n\nОтправлено: {success}\nНе удалось: {fail}")
    except: pass

# --- AUTH FLOW (NUMPAD) ---
@router.callback_query(F.data == "auth")
async def au_cb(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("Sub needed!", True)
    await c.message.edit_text("🔑 Вход:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 QR", callback_data="qr"), InlineKeyboardButton(text="📱 Phone", callback_data="ph")]]))

@router.callback_query(F.data == "ph")
async def ph_cb(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 Номер (7900...):"); await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def ph_s(m: Message, state: FSMContext):
    uid = m.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        sent = await cl.send_code_request(m.text)
        await state.update_data(ph=m.text, h=sent.phone_code_hash, uid=uid, input_code="")
        await cl.disconnect()
        await m.answer(f"📩 Код для {m.text}:", reply_markup=get_numpad_kb())
        await state.set_state(AuthS.CO)
    except Exception as e: await cl.disconnect(); await m.answer(f"❌ {e}")

@router.callback_query(F.data.startswith("num_"), AuthS.CO)
async def num_h(c: CallbackQuery, state: FSMContext):
    act = c.data.split("_")[1]
    d = await state.get_data(); curr = d.get("input_code", "")
    
    if act == "del": curr = curr[:-1]
    elif act == "go":
        if not curr: return await c.answer("Пусто!", True)
        await c.message.edit_text("⏳ ...")
        cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
        await cl.connect()
        try:
            await cl.sign_in(phone=d['ph'], code=curr, phone_code_hash=d['h'])
            await c.message.answer("✅ Вход!"); await cl.disconnect(); await state.clear()
            if d['uid'] not in W_POOL: w=Worker(d['uid']); await w.start(); W_POOL[d['uid']]=w
            await st(c.message, state); return
        except SessionPasswordNeededError:
            await c.message.answer("🔒 2FA Пароль (текстом):"); await cl.disconnect(); await state.set_state(AuthS.PA); return
        except Exception as e:
            await c.message.answer(f"❌ {e}"); await cl.disconnect(); return
    else: 
        if len(curr) < 6: curr += act
    
    await state.update_data(input_code=curr)
    disp = "-".join(list(curr)) if curr else "..."
    try: await c.message.edit_text(f"📩 Код: <code>{disp}</code>", reply_markup=get_numpad_kb())
    except: pass

@router.message(AuthS.PA)
async def pa_s(m: Message, state: FSMContext):
    d = await state.get_data()
    cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
    await cl.connect()
    try: await cl.sign_in(password=m.text); await m.answer("✅"); await state.clear(); w=Worker(d['uid']); await w.start(); W_POOL[d['uid']]=w
    except Exception as e: await m.answer(f"❌ {e}")
    finally: await cl.disconnect()

@router.callback_query(F.data == "qr")
async def qr_cb(c: CallbackQuery):
    cl = Worker(c.from_user.id)._get_client(cfg.SESSION_DIR / f"session_{c.from_user.id}")
    await cl.connect()
    qr = await cl.qr_login()
    b = io.BytesIO(); qrcode.make(qr.url).save(b, "PNG"); b.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="📸 Scan")
    try: await qr.wait(60); await msg.delete(); await c.message.answer("✅ OK"); w=Worker(c.from_user.id); await w.start(); W_POOL[c.from_user.id]=w
    except: await msg.delete(); await c.message.answer("❌ Timeout")
    finally: await cl.disconnect()

# --- HELPERS ---
@router.callback_query(F.data == "back")
async def bk(c: CallbackQuery, state: FSMContext): await c.message.delete(); await st(c.message, state)
@router.callback_query(F.data == "help")
async def hl(c: CallbackQuery): await c.message.edit_text(".scan, .spam, .all", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))
@router.callback_query(F.data == "profile")
async def pr(c: CallbackQuery): await c.message.edit_text("Promo:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟", callback_data="promo")],[InlineKeyboardButton(text="🔙", callback_data="back")]]))
@router.callback_query(F.data == "promo")
async def pro(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Code:"); await state.set_state(PromoS.CODE)
@router.message(PromoS.CODE)
async def prom(m: Message, state: FSMContext): 
    if await db.use_promo(m.from_user.id, m.text): await m.answer("✅"); await st(m, state)
    else: await m.answer("❌")

# --- ADMIN ---
@router.callback_query(F.data == "adm")
async def ad(c: CallbackQuery): await c.message.edit_text("Admin:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Promo", callback_data="mk")]]))
@router.callback_query(F.data == "mk")
async def mk(c: CallbackQuery, state: FSMContext): await c.message.answer("Days?"); await state.set_state(AdminS.PD)
@router.message(AdminS.PD)
async def mkd(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Count?"); await state.set_state(AdminS.PA)
@router.message(AdminS.PA)
async def mka(m: Message, state: FSMContext): d=await state.get_data(); c=await db.create_promo(d['d'], int(m.text)); await m.answer(c); await state.clear()

async def main():
    await db.init()
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub_bool(uid): await Worker(uid).start()
        except: pass
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
