#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro v66.1 - DIAMOND HOSTING EDITION
------------------------------------------
Architect: StatPro AI
Environment: Cloud/Hosting (No .env file required)
Features:
+ Direct Env Var Reading
+ Auto-Healing Workers
+ Admin Broadcast System
+ AFK & Smart Scan
+ Full Async Core
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
from pathlib import Path
from typing import Dict, Set, Optional, List
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
from telethon.errors import (
    SessionPasswordNeededError, 
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError
)
from telethon.tl.types import User

# =========================================================================
# ⚙️ SYSTEM CONFIGURATION (DIRECT FROM HOSTING)
# =========================================================================

@dataclass
class Config:
    # Чтение переменных окружения напрямую из системы хостинга
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    SUB_CHANNEL: str = "@STAT_PRO1"  # Канал для проверки
    
    # Пути
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_hosting.db"
    
    # Маскировка под iPhone 15 Pro
    DEVICE_MODEL: str = "iPhone 15 Pro"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"
    LANG_CODE: str = "ru"
    SYSTEM_LANG_CODE: str = "ru-RU"
    
    TELETHON_TIMEOUT: float = 30.0 

    def __post_init__(self):
        # Проверка критических переменных
        if not self.BOT_TOKEN or not self.API_HASH or not self.API_ID:
            print("\n❌ FATAL ERROR: Переменные окружения не найдены!")
            print("Убедитесь, что в панели хостинга добавлены: BOT_TOKEN, API_ID, API_HASH, ADMIN_ID\n")
            sys.exit(1)
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)

cfg = Config()

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("StatPro")

# =========================================================================
# 🗄️ DATABASE ENGINE
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
            # Таблица юзеров
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, username TEXT, 
                    sub_end INTEGER, joined_at INTEGER
                )
            """)
            # Таблица промокодов
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations INTEGER)")
            await db.commit()

    async def get_all_users_ids(self) -> List[int]:
        """Получить всех пользователей для рассылки"""
        async with self.get_conn() as db:
            async with db.execute("SELECT user_id FROM users") as c:
                res = await c.fetchall()
                return [r[0] for r in res]

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
        code = f"PRO-{random.randint(100,999)}-{random.randint(1000,9999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🦾 DIAMOND WORKER (Userbot Core)
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        self.spam_task: Optional[asyncio.Task] = None
        self.raid_targets: Set[int] = set()
        self.react_map: Dict[int, str] = {}
        self.ghost_mode: bool = False
        
        # New: AFK Logic
        self.afk_reason: Optional[str] = None
        self.afk_cooldown: Dict[int, float] = {}

    def _get_client(self, path):
        return TelegramClient(
            str(path), cfg.API_ID, cfg.API_HASH,
            device_model=cfg.DEVICE_MODEL,
            system_version=cfg.SYSTEM_VERSION,
            app_version=cfg.APP_VERSION,
            lang_code=cfg.LANG_CODE,
            system_lang_code=cfg.SYSTEM_LANG_CODE,
            timeout=cfg.TELETHON_TIMEOUT,
            auto_reconnect=True,
            retry_delay=5
        )

    async def start(self):
        s_path = cfg.SESSION_DIR / f"session_{self.uid}"
        self.client = self._get_client(s_path)
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                logger.warning(f"Worker {self.uid}: Требуется вход")
                return False
            self._bind_commands()
            # Run in background
            asyncio.create_task(self._run_safe())
            logger.info(f"Worker {self.uid}: 🟢 ONLINE")
            return True
        except Exception as e:
            logger.error(f"Worker {self.uid} Error: {e}")
            return False

    async def _run_safe(self):
        """Безопасный запуск с попыткой реконнекта"""
        try:
            await self.client.run_until_disconnected()
        except Exception as e:
            logger.error(f"Worker {self.uid} Disconnected: {e}")

    async def stop(self):
        if self.spam_task: self.spam_task.cancel()
        if self.client: await self.client.disconnect()

    def _bind_commands(self):
        client = self.client

        # --- GHOST & AFK ---
        @client.on(events.NewMessage(incoming=True))
        async def incoming_h(e):
            # Ghost: просто не отправляем read state
            pass 
            
            # AFK Auto-Reply
            if self.afk_reason and e.is_private and not e.sender.bot:
                now = time.time()
                last = self.afk_cooldown.get(e.chat_id, 0)
                if now - last > 300: # 5 минут кд
                    try:
                        await e.reply(f"💤 <b>AFK Mode</b>\nЯ занят: {self.afk_reason}", parse_mode='html')
                        self.afk_cooldown[e.chat_id] = now
                    except: pass

        # --- AUTO ACTIONS ---
        @client.on(events.NewMessage)
        async def auto_h(e):
            # React
            if e.chat_id in self.react_map and not e.out:
                try: await e.client(functions.messages.SendReactionRequest(
                    peer=e.chat_id, msg_id=e.id, 
                    reaction=[types.ReactionEmoji(emoticon=self.react_map[e.chat_id])]
                ))
                except: pass
            
            # Raid
            if e.sender_id in self.raid_targets:
                insults = ["🤡", "🗑", "🤫", "Cry", "L", "Bot"]
                try: await e.reply(random.choice(insults))
                except: pass

        # --- USER COMMANDS ---

        # PING (.p)
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(ping|p)$'))
        async def cmd_ping(e):
            s = time.perf_counter()
            m = await e.edit("💎 Ping...")
            ms = (time.perf_counter() - s) * 1000
            await m.edit(f"💎 <b>Diamond Host</b>\nPing: <code>{ms:.2f}ms</code>", parse_mode='html')

        # SPAM (.s)
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(?:spam|s)\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def cmd_spam(e):
            if self.spam_task and not self.spam_task.done(): 
                return await e.edit("⚠️ Already running!")
            
            args = e.pattern_match
            txt, cnt, dly = args.group(1), int(args.group(2)), float(args.group(3))
            await e.delete()
            
            async def spam_loop():
                for _ in range(cnt):
                    try: 
                        await client.send_message(e.chat_id, txt)
                        await asyncio.sleep(dly)
                    except FloodWaitError as fw:
                        await asyncio.sleep(fw.seconds + 2)
                    except: break
            
            self.spam_task = asyncio.create_task(spam_loop())

        # STOP
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def cmd_stop(e):
            if self.spam_task: 
                self.spam_task.cancel(); self.spam_task = None
                await e.edit("🛑 Stopped.")
            else: await e.edit("⚠️ Idle.")

        # SCAN (.scan) -> FILE
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.scan(?:\s+(\d+))?'))
        async def cmd_scan(e):
            limit = int(e.pattern_match.group(1) or 100)
            await e.edit(f"🕵️‍♂️ Scanning {limit} users...")
            
            data = []
            seen = set()
            
            async for m in client.iter_messages(e.chat_id, limit=limit):
                if m.sender and isinstance(m.sender, User) and not m.sender.bot:
                    uid = m.sender.id
                    if uid not in seen:
                        seen.add(uid)
                        first = m.sender.first_name or ""
                        last = m.sender.last_name or ""
                        un = m.sender.username or ""
                        data.append([uid, un, f"{first} {last}".strip()])

            out = io.StringIO()
            w = csv.writer(out); w.writerow(["ID", "Username", "Full Name"])
            w.writerows(data)
            
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig'))
            bio.name = f"Scan_{e.chat_id}.csv"

            try:
                await client.send_file("me", bio, caption=f"📊 <b>Scan Report</b>\nChat: {e.chat_id}\nUsers: {len(data)}", parse_mode='html')
                await e.edit("✅ Report sent to Saved Messages.")
            except Exception as ex:
                await e.edit(f"❌ Error: {ex}")

        # AFK (.afk)
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.afk(?:\s+(.+))?'))
        async def cmd_afk(e):
            reason = e.pattern_match.group(1)
            if not reason or reason.lower() == 'off':
                self.afk_reason = None
                await e.edit("🏃‍♂️ <b>AFK: OFF</b>", parse_mode='html')
            else:
                self.afk_reason = reason
                await e.edit(f"💤 <b>AFK: ON</b>\nReason: {reason}", parse_mode='html')

        # RAID (.r)
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.(?:raid|r)$'))
        async def cmd_raid(e):
            if not e.is_reply: return await e.edit("Reply needed!")
            tid = (await e.get_reply_message()).sender_id
            if tid in self.raid_targets:
                self.raid_targets.remove(tid); await e.edit("🕊 Raid OFF.")
            else:
                self.raid_targets.add(tid); await e.edit("☠️ Raid ON.")

        # REACT
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.react\s+(.+)$'))
        async def cmd_react(e):
            em = e.pattern_match.group(1).strip()
            if em == 'off': self.react_map.pop(e.chat_id, None); await e.edit("😐 Reacts OFF.")
            else: self.react_map[e.chat_id] = em; await e.edit(f"🔥 React: {em}")

        # GHOST
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.ghost\s+(on|off)$'))
        async def cmd_ghost(e):
            self.ghost_mode = (e.pattern_match.group(1) == 'on')
            await e.edit(f"👻 Ghost: <b>{self.ghost_mode}</b>", parse_mode='html')

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT INTERFACE
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminS(StatesGroup): 
    U=State(); D=State(); PD=State(); PA=State()
    CAST=State() # Broadcast state

def kb_main(uid):
    rows = [
        [InlineKeyboardButton(text="📚 Команды", callback_data="help")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth_menu")]
    ]
    if uid == cfg.ADMIN_ID: rows.append([InlineKeyboardButton(text="👑 Админ Панель", callback_data="adm_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def check_sub_logic(uid):
    if uid == cfg.ADMIN_ID: return True
    try:
        m = await bot.get_chat_member(cfg.SUB_CHANNEL, uid)
        return m.status not in ['left', 'kicked', 'banned']
    except: return True

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    await db.upsert_user(uid, m.from_user.username or "User")
    
    if not await check_sub_logic(uid):
        return await m.answer(
            f"⛔️ <b>Доступ ограничен!</b>\nПодпишись: {cfg.SUB_CHANNEL}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
                [InlineKeyboardButton(text="🔄 Проверить", callback_data="chk")]
            ])
        )
    await m.answer(f"💎 <b>StatPro Hosting v66.1</b>\nID: {uid}", reply_markup=kb_main(uid))

@router.callback_query(F.data == "chk")
async def cb_chk(c: CallbackQuery, state: FSMContext):
    await c.message.delete(); await start(c.message, state)

@router.callback_query(F.data == "help")
async def cb_help(c: CallbackQuery):
    await c.message.edit_text(
        "💻 <b>Команды Userbot:</b>\n"
        "<code>.p</code> / <code>.ping</code> — Пинг\n"
        "<code>.s [txt] [cnt] [sec]</code> — Спам\n"
        "<code>.stop</code> — Стоп\n"
        "<code>.scan [num]</code> — Скан в файл\n"
        "<code>.r</code> / <code>.raid</code> — Ответы\n"
        "<code>.afk [reason]</code> — Авто-ответ\n"
        "<code>.ghost on/off</code> — Невидимка",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="chk")]])
    )

@router.callback_query(F.data == "profile")
async def cb_prof(c: CallbackQuery):
    uid = c.from_user.id
    sub = await db.check_sub_bool(uid)
    s_txt = "💎 DIAMOND" if sub else "❌ FREE"
    await c.message.edit_text(
        f"👤 <b>Профиль</b>\nID: <code>{uid}</code>\nSub: {s_txt}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎟 Ввести Промо", callback_data="promo")],
            [InlineKeyboardButton(text="🔙", callback_data="chk")]
        ])
    )

# --- PROMO ---
@router.callback_query(F.data == "promo")
async def promo_ask(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("🎟 <b>Код:</b>"); await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def promo_do(m: Message, state: FSMContext):
    d = await db.use_promo(m.from_user.id, m.text)
    if d:
        await m.answer(f"✅ Добавлено: {d} дн.")
        if m.from_user.id in W_POOL: await W_POOL[m.from_user.id].start()
        await start(m, state)
    else: await m.answer("❌ Ошибка."); await start(m, state)

# --- AUTH (LOGIN) ---
@router.callback_query(F.data == "auth_menu")
async def auth_menu(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("Нужна подписка!", True)
    await c.message.edit_text("🔑 <b>Вход:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 QR", callback_data="l_qr"), InlineKeyboardButton(text="📱 Phone", callback_data="l_ph")],
        [InlineKeyboardButton(text="🔙", callback_data="chk")]
    ]))

@router.callback_query(F.data == "l_qr")
async def login_qr(c: CallbackQuery):
    uid = c.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    if await cl.is_user_authorized(): 
        await cl.disconnect(); return await c.answer("Уже вошли!", True)
    
    qr = await cl.qr_login()
    b = io.BytesIO(); qrcode.make(qr.url).save(b, "PNG"); b.seek(0)
    m = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="📸 Scan QR")
    try:
        await qr.wait(cfg.TELETHON_TIMEOUT)
        await m.delete(); await c.message.answer("✅ Успешно!")
        if uid not in W_POOL: w=Worker(uid); await w.start(); W_POOL[uid]=w
    except: await m.delete(); await c.message.answer("❌ Тайм-аут.")
    finally: await cl.disconnect()

@router.callback_query(F.data == "l_ph")
async def login_ph(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 <b>Номер (79...):</b>"); await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def login_ph_req(m: Message, state: FSMContext):
    uid = m.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        sent = await cl.send_code_request(m.text)
        await state.update_data(ph=m.text, h=sent.phone_code_hash, uid=uid)
        await cl.disconnect()
        await m.answer("📩 <b>Код:</b>"); await state.set_state(AuthS.CO)
    except Exception as e:
        await cl.disconnect(); await m.answer(f"❌ {e}")

@router.message(AuthS.CO)
async def login_code_do(m: Message, state: FSMContext):
    d = await state.get_data()
    cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
    await cl.connect()
    try:
        await cl.sign_in(phone=d['ph'], code=m.text, phone_code_hash=d['h'])
        await m.answer("✅ Вход выполнен!"); await cl.disconnect(); await state.clear()
        if d['uid'] not in W_POOL: w=Worker(d['uid']); await w.start(); W_POOL[d['uid']]=w
        await start(m, state)
    except SessionPasswordNeededError:
        await m.answer("🔒 <b>2FA Пароль:</b>"); await cl.disconnect(); await state.set_state(AuthS.PA)
    except Exception as e:
        await cl.disconnect(); await m.answer(f"❌ {e}")

@router.message(AuthS.PA)
async def login_pass_do(m: Message, state: FSMContext):
    d = await state.get_data()
    cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
    await cl.connect()
    try:
        await cl.sign_in(password=m.text)
        await m.answer("✅ Вход выполнен!"); await cl.disconnect(); await state.clear()
        if d['uid'] not in W_POOL: w=Worker(d['uid']); await w.start(); W_POOL[d['uid']]=w
        await start(m, state)
    except Exception as e:
        await cl.disconnect(); await m.answer(f"❌ {e}")

# --- ADMIN ---
@router.callback_query(F.data == "adm_menu")
async def adm_menu(c: CallbackQuery):
    if c.from_user.id != cfg.ADMIN_ID: return
    await c.message.edit_text("👑 <b>Admin</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Промо", callback_data="mk_p")],
        [InlineKeyboardButton(text="🎁 Подписка", callback_data="g_s")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="bc_start")],
        [InlineKeyboardButton(text="🔙", callback_data="chk")]
    ]))

# BROADCAST SYSTEM
@router.callback_query(F.data == "bc_start")
async def bc_start(c: CallbackQuery, state: FSMContext):
    await c.message.answer("📢 <b>Введите текст рассылки:</b>\n(Можно HTML)")
    await state.set_state(AdminS.CAST)

@router.message(AdminS.CAST)
async def bc_run(m: Message, state: FSMContext):
    users = await db.get_all_users_ids()
    ok, bad = 0, 0
    start_msg = await m.answer(f"🚀 Запуск рассылки на {len(users)} чел...")
    
    for u in users:
        try:
            await bot.send_message(u, m.text)
            ok += 1
            await asyncio.sleep(0.05) # Anti-Flood bot
        except: bad += 1
    
    await start_msg.edit_text(f"✅ <b>Рассылка завершена!</b>\nОтправлено: {ok}\nОшибок: {bad}")
    await state.clear()

@router.callback_query(F.data == "mk_p")
async def mk_p(c: CallbackQuery, state: FSMContext):
    await c.message.answer("📅 Дней?"); await state.set_state(AdminS.PD)

@router.message(AdminS.PD)
async def mk_pd(m: Message, state: FSMContext):
    await state.update_data(d=int(m.text)); await m.answer("🔢 Кол-во?"); await state.set_state(AdminS.PA)

@router.message(AdminS.PA)
async def mk_pa(m: Message, state: FSMContext):
    d = await state.get_data()
    c = await db.create_promo(d['d'], int(m.text))
    await m.answer(f"Code: <code>{c}</code>"); await state.clear()

@router.callback_query(F.data == "g_s")
async def gs(c: CallbackQuery, state: FSMContext):
    await c.message.answer("🆔 ID?"); await state.set_state(AdminS.U)

@router.message(AdminS.U)
async def gs_u(m: Message, state: FSMContext):
    await state.update_data(u=m.text); await m.answer("📅 Дней?"); await state.set_state(AdminS.D)

@router.message(AdminS.D)
async def gs_d(m: Message, state: FSMContext):
    d = await state.get_data()
    await db.upsert_user(int(d['u']), "AdminAdd")
    await db.add_sub_days(int(d['u']), int(m.text))
    await m.answer("✅ Выдано."); await state.clear()

# --- LAUNCHER ---

async def main():
    # Инициализация
    await db.init()
    
    # Режим авто-поднятия сессий
    cnt = 0
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub_bool(uid):
                w = Worker(uid)
                # Фоновый запуск без блокировки
                if await w.start():
                    W_POOL[uid] = w
                    cnt += 1
        except Exception: pass
    
    logger.info(f"🚀 STATPRO V66.1 STARTED | WORKERS: {cnt}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
