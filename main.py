#!/usr/bin/env python3
"""
💀 StatPro v64.0 - WAR MACHINE EDITION
--------------------------------------
✅ REMOVED: Отчеты (полностью).
✅ ADDED: Весь функционал Userbot (.ping, .spam, .raid, .scan, .react).
✅ FIX: Маскировка под iPhone 15 Pro (решение проблем с кодом входа).
✅ CORE: Быстрая БД, Авто-реконнект, Тайм-аут 500с.
"""

import asyncio
import logging
import os
import io
import json
import random
import time
import qrcode
import aiosqlite
import csv
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Set, Optional
from dataclasses import dataclass

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Message, BufferedInputFile
)
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties

# --- TELETHON ---
from telethon import TelegramClient, events, types, functions
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import User

# =========================================================================
# ⚙️ CONFIG & LOGGING
# =========================================================================

@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "YOUR_API_HASH")
    SUB_CHANNEL: str = "@STAT_PRO1"
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_v64.db"
    
    # MASKING AS IPHONE (FIX LOGIN ISSUES)
    DEVICE_MODEL: str = "iPhone 15 Pro Max"
    SYSTEM_VERSION: str = "17.4.1"
    APP_VERSION: str = "10.9.1"
    LANG_CODE: str = "en"
    SYSTEM_LANG_CODE: str = "en-US"

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)

cfg = Config()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger("StatPro_v64")

# =========================================================================
# 🗄️ DATABASE
# =========================================================================

class Database:
    __slots__ = ('path',)
    _instance = None
    def __new__(cls):
        if cls._instance is None: cls._instance = super(Database, cls).__new__(cls)
        return cls._instance
    def __init__(self): self.path = cfg.DB_PATH
    def get_conn(self): return aiosqlite.connect(self.path, timeout=30.0)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, username TEXT, 
                    sub_end INTEGER, joined_at INTEGER
                )
            """)
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations INTEGER)")
            await db.commit()

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
        start = curr if curr > now else now
        new_end = start + (days * 86400)
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return new_end

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE code = ? AND activations <= 0", (code,))
            await db.commit()
        await self.add_sub_days(uid, days)
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"PRO-{random.randint(10000,99999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🤖 USERBOT WORKER (ALL FUNCTIONS RESTORED)
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        
        # State
        self.spam_task: Optional[asyncio.Task] = None
        self.raid_targets: Set[int] = set()
        self.react_map: Dict[int, str] = {} # chat_id -> emoji
        self.ghost_mode: bool = False
        self.afk_mode: bool = False
        self.afk_reason: str = ""

    def _get_client(self, path):
        # 🔥 FIX LOGIN: Маскируемся под официальный клиент
        return TelegramClient(
            str(path), cfg.API_ID, cfg.API_HASH,
            device_model=cfg.DEVICE_MODEL,
            system_version=cfg.SYSTEM_VERSION,
            app_version=cfg.APP_VERSION,
            lang_code=cfg.LANG_CODE,
            system_lang_code=cfg.SYSTEM_LANG_CODE,
            auto_reconnect=True
        )

    async def start(self):
        s_path = cfg.SESSION_DIR / f"session_{self.uid}"
        self.client = self._get_client(s_path)
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): return False
            self._bind_commands()
            asyncio.create_task(self.client.run_until_disconnected())
            return True
        except Exception:
            logger.exception(f"Worker {self.uid} Start Error")
            return False

    async def stop(self):
        if self.spam_task: self.spam_task.cancel()
        if self.client: await self.client.disconnect()

    def _bind_commands(self):
        """Здесь вся логика Userbot'а"""
        client = self.client

        @client.on(events.NewMessage)
        async def main_handler(e):
            # 1. GHOST MODE (Имитация нечиталки)
            # В Telethon сложно полностью запретить "чтение", но мы не отправляем read_history явно
            # Если нужно жестче - нужно использовать raw api, но это база.
            
            # 2. AUTO REACT
            if e.chat_id in self.react_map and not e.out:
                try: await e.client(functions.messages.SendReactionRequest(
                    peer=e.chat_id, msg_id=e.id, 
                    reaction=[types.ReactionEmoji(emoticon=self.react_map[e.chat_id])]
                ))
                except: pass

            # 3. RAID (Ответ на сообщения жертвы)
            if e.sender_id in self.raid_targets:
                insults = ["🗑", "🤡", "🤫", "👎", "Слабый", "Не пиши сюда"]
                try: await e.reply(random.choice(insults))
                except: pass

            # 4. AFK
            if self.afk_mode and e.mentioned and not e.out:
                try: await e.reply(f"💤 <b>AFK Mode:</b> {self.afk_reason}", parse_mode='html')
                except: pass

        # --- КОМАНДЫ ---

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.ping$'))
        async def cmd_ping(e):
            start = time.perf_counter()
            msg = await e.edit("🏓 Pong!")
            end = time.perf_counter()
            ms = (end - start) * 1000
            await msg.edit(f"🏓 <b>Pong!</b>\n📶 Ping: <code>{ms:.2f}ms</code>", parse_mode='html')

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.spam\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def cmd_spam(e):
            """ .spam текст кол-во задержка """
            if self.spam_task and not self.spam_task.done():
                return await e.edit("⚠️ Спам уже запущен!")
            
            args = e.pattern_match
            text = args.group(1)
            count = int(args.group(2))
            delay = float(args.group(3))
            
            await e.delete()
            
            async def spam_runner():
                for _ in range(count):
                    try: await client.send_message(e.chat_id, text)
                    except: break
                    await asyncio.sleep(delay)
            
            self.spam_task = asyncio.create_task(spam_runner())

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def cmd_stop_spam(e):
            if self.spam_task:
                self.spam_task.cancel()
                self.spam_task = None
                await e.edit("🛑 Спам остановлен.")
            else:
                await e.edit("⚠️ Ничего не запущено.")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.scan(?:\s+(\d+))?'))
        async def cmd_scan(e):
            """ .scan 100 - парсинг активных юзеров """
            limit = int(e.pattern_match.group(1) or 100)
            await e.edit(f"🔎 Сканирую {limit} сообщений...")
            
            users = {} # id -> name
            count = 0
            async for msg in client.iter_messages(e.chat_id, limit=limit):
                if msg.sender and isinstance(msg.sender, User) and not msg.sender.bot:
                    uid = msg.sender.id
                    name = msg.sender.first_name or "Unknown"
                    if msg.sender.username: name += f" (@{msg.sender.username})"
                    users[uid] = name
                count += 1
            
            # Save CSV
            f = io.StringIO()
            writer = csv.writer(f)
            writer.writerow(["ID", "Name/User"])
            for uid, name in users.items():
                writer.writerow([uid, name])
            
            f.seek(0)
            await e.delete()
            await client.send_file(e.chat_id, f.read().encode(), filename="users.csv", caption=f"✅ Найдено {len(users)} уникальных людей.")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.raid$'))
        async def cmd_raid(e):
            if not e.is_reply: return await e.edit("⚠️ Реплайни на жертву!")
            r = await e.get_reply_message()
            target = r.sender_id
            if target in self.raid_targets:
                self.raid_targets.remove(target)
                await e.edit("🕊 Рейд выключен.")
            else:
                self.raid_targets.add(target)
                await e.edit("☠️ <b>РЕЙД АКТИВИРОВАН!</b>", parse_mode='html')

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.react\s+(.+)$'))
        async def cmd_react(e):
            emoji = e.pattern_match.group(1).strip()
            if emoji == "off" or emoji == "stop":
                self.react_map.pop(e.chat_id, None)
                await e.edit("😐 Авто-реакции выключены.")
            else:
                self.react_map[e.chat_id] = emoji
                await e.edit(f"🔥 Авто-реакция: {emoji}")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.ghost\s+(on|off)$'))
        async def cmd_ghost(e):
            mode = e.pattern_match.group(1)
            self.ghost_mode = (mode == "on")
            await e.edit(f"👻 Призрак: <b>{mode.upper()}</b>", parse_mode='html')

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 📱 BOT HANDLERS
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router()
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminS(StatesGroup): U=State(); D=State(); PD=State(); PA=State()

def kb_main(uid):
    rows = [
        [InlineKeyboardButton(text="📚 Список Команд", callback_data="help")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🔑 Вход в аккаунт", callback_data="auth_menu")]
    ]
    if uid == cfg.ADMIN_ID: rows.append([InlineKeyboardButton(text="👑 Админ Панель", callback_data="adm_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    await db.upsert_user(uid, m.from_user.username or "User")
    
    # Check Sub
    try:
        mem = await bot.get_chat_member(cfg.SUB_CHANNEL, uid)
        if mem.status in ['left', 'kicked'] and uid != cfg.ADMIN_ID:
            return await m.answer(f"⛔️ <b>Нет подписки!</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Подписаться", url=f"https://t.me/{cfg.SUB_CHANNEL.replace('@','')}")],[InlineKeyboardButton(text="✅ Проверить", callback_data="chk")]]))
    except: pass

    await m.answer(f"🛡 <b>StatPro v64.0</b>\nID: <code>{uid}</code>\nРежим: <b>War Machine</b>", reply_markup=kb_main(uid))

@router.callback_query(F.data == "chk")
async def chk(c: CallbackQuery, state: FSMContext): await c.message.delete(); await start(c.message, state)

@router.callback_query(F.data == "help")
async def help_cmd(c: CallbackQuery):
    txt = (
        "💻 <b>Команды Воркера:</b>\n\n"
        "⚡️ <code>.ping</code> - Проверка пинга\n"
        "💣 <code>.spam [текст] [кол] [сек]</code> - Спам\n"
        "🛑 <code>.stop</code> - Стоп спама\n"
        "🔎 <code>.scan 100</code> - Парсинг юзеров\n"
        "☠️ <code>.raid</code> (реплай) - Рейд жертвы\n"
        "🔥 <code>.react 👍</code> - Авто-реакции\n"
        "👻 <code>.ghost on/off</code> - Призрак"
    )
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="chk")]]))

@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    uid = c.from_user.id
    active = await db.check_sub_bool(uid)
    end = "Активна" if active else "Истекла"
    await c.message.edit_text(f"👤 <b>Профиль</b>\nПодписка: {end}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Промокод", callback_data="promo")],[InlineKeyboardButton(text="🔙 Назад", callback_data="chk")]]))

@router.callback_query(F.data == "promo")
async def promo_in(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("🎟 Код:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="profile")]]))
    await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def promo_use(m: Message, state: FSMContext):
    d = await db.use_promo(m.from_user.id, m.text)
    if d: 
        await m.answer(f"✅ +{d} дней.")
        if m.from_user.id in W_POOL: await W_POOL[m.from_user.id].start()
    else: await m.answer("❌ Ошибка.")
    await state.clear(); await start(m, state)

# --- AUTH (IPHONE FIX) ---

def get_client(uid):
    return TelegramClient(
        str(cfg.SESSION_DIR / f"session_{uid}"), cfg.API_ID, cfg.API_HASH,
        device_model=cfg.DEVICE_MODEL, system_version=cfg.SYSTEM_VERSION, app_version=cfg.APP_VERSION
    )

@router.callback_query(F.data == "auth_menu")
async def auth_m(c: CallbackQuery):
    await c.message.edit_text("🔑 Вход", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 QR", callback_data="l_qr"), InlineKeyboardButton(text="📞 SMS", callback_data="l_ph")],[InlineKeyboardButton(text="🔙", callback_data="chk")]]))

@router.callback_query(F.data == "l_qr")
async def l_qr(c: CallbackQuery):
    uid = c.from_user.id; cl = get_client(uid); await cl.connect()
    if await cl.is_user_authorized(): await cl.disconnect(); return await c.answer("✅ Уже выполнен", True)
    qr = await cl.qr_login(); bio = io.BytesIO(); qrcode.make(qr.url).save(bio, "PNG")
    m = await c.message.answer_photo(BufferedInputFile(bio.getvalue(), "qr.png"), caption="⏳ 500 сек")
    try: await qr.wait(500); await m.delete(); await c.message.answer("✅ Готово")
    except: await m.delete(); await c.message.answer("❌ Ошибка")
    finally: await cl.disconnect()

@router.callback_query(F.data == "l_ph")
async def l_ph(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📞 Номер:"); await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def l_ph_s(m: Message, state: FSMContext):
    uid = m.from_user.id; cl = get_client(uid); await cl.connect()
    try:
        s = await cl.send_code_request(m.text)
        await state.update_data(p=m.text, h=s.phone_code_hash, uid=uid)
        await cl.disconnect(); await m.answer("📩 Код:"); await state.set_state(AuthS.CO)
    except Exception as e: await m.answer(f"❌ {e}")

@router.message(AuthS.CO)
async def l_co_s(m: Message, state: FSMContext):
    d = await state.get_data(); cl = get_client(d['uid']); await cl.connect()
    try: await cl.sign_in(phone=d['p'], code=m.text, phone_code_hash=d['h']); await m.answer("✅ OK"); await cl.disconnect(); await state.clear(); await start(m, state)
    except SessionPasswordNeededError: await m.answer("🔒 2FA:"); await cl.disconnect(); await state.set_state(AuthS.PA)
    except Exception as e: await cl.disconnect(); await m.answer(f"❌ {e}")

@router.message(AuthS.PA)
async def l_pa_s(m: Message, state: FSMContext):
    d = await state.get_data(); cl = get_client(d['uid']); await cl.connect()
    try: await cl.sign_in(password=m.text); await m.answer("✅ OK")
    except Exception as e: await m.answer(f"❌ {e}")
    finally: await cl.disconnect(); await state.clear(); await start(m, state)

# --- ADMIN ---

@router.callback_query(F.data == "adm_menu")
async def adm(c: CallbackQuery):
    if c.from_user.id != cfg.ADMIN_ID: return
    await c.message.edit_text("👑 Admin", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Promo", callback_data="m_p")],[InlineKeyboardButton(text="🎁 Sub", callback_data="g_s")],[InlineKeyboardButton(text="🔙", callback_data="chk")]]))

@router.callback_query(F.data == "m_p")
async def mk(c: CallbackQuery, state: FSMContext): await c.message.answer("Days?"); await state.set_state(AdminS.PD)
@router.message(AdminS.PD)
async def mk_d(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Acts?"); await state.set_state(AdminS.PA)
@router.message(AdminS.PA)
async def mk_a(m: Message, state: FSMContext): d=await state.get_data(); c=await db.create_promo(d['d'],int(m.text)); await m.answer(f"Code: <code>{c}</code>"); await state.clear()

@router.callback_query(F.data == "g_s")
async def gs(c: CallbackQuery, state: FSMContext): await c.message.answer("ID?"); await state.set_state(AdminS.U)
@router.message(AdminS.U)
async def gs_u(m: Message, state: FSMContext): await state.update_data(u=m.text); await m.answer("Days?"); await state.set_state(AdminS.D)
@router.message(AdminS.D)
async def gs_d(m: Message, state: FSMContext): d=await state.get_data(); await db.add_sub_days(int(d['u']), int(m.text)); await m.answer("Done"); await state.clear()

# --- MAIN ---

async def main():
    await db.init()
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub_bool(uid):
                w = Worker(uid)
                if await w.start(): W_POOL[uid] = w
        except: pass
    logger.info("🔥 StatPro v64.0 (War Machine) Started")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
