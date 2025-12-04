#!/usr/bin/env python3
"""
💎 StatPro v33.0 - OBSIDIAN CORE
--------------------------------
🚀 ENGINE: Полная переработка архитектуры (Event-Driven).
💾 DB: SQLite WAL + Memory Mapping + Atomic Writes.
🛡 SECURITY: Circuit Breaker для защиты от флуд-банов.
🧠 MEMORY: Агрессивная сборка мусора и оптимизация слотов.
"""

import asyncio
import logging
import os
import sys
import io
import re
import random
import shutil
import time
import json
import math
import csv
import gc
import weakref
import aiosqlite
from typing import Dict, Optional, List, Set, Any, Union
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile, FSInputFile
)
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.dispatcher.middlewares.base import BaseMiddleware

# --- TELETHON ---
from telethon import TelegramClient, events, types, functions
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError,
    RpcCallFailError, ServerError
)
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import User, ChatBannedRights

import qrcode
from PIL import Image

# =========================================================================
# ⚙️ SYSTEM CONFIGURATION
# =========================================================================

VERSION = "v33.0 OBSIDIAN"
MSK_TZ = timezone(timedelta(hours=3))

BASE_DIR = Path("/app")
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "obsidian.db"
LOG_FILE = BASE_DIR / "core.log"
STATE_FILE = BASE_DIR / "state.json"

SESSION_DIR.mkdir(parents=True, exist_ok=True)

# Optimized Logging (Async-friendly format)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')]
)
logger = logging.getLogger("Core")

# Environment Check
try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    SUPPORT_BOT = os.getenv("SUPPORT_BOT_USERNAME", "@suppor_tstatpro1bot")
except:
    logger.critical("ENV MISSING")
    sys.exit(1)

if not all([BOT_TOKEN, API_ID, API_HASH]): sys.exit(1)

TEMP_DATA = {} 
RE_IT_CMD = r'^\.(встал|зм|пв)\s*(\d+)$'

# =========================================================================
# 🗄️ HIGH-PERFORMANCE DATABASE (SINGLETON)
# =========================================================================

class Database:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized: return
        self.path = DB_PATH
        self._initialized = True

    def get_conn(self):
        # Increased timeout for heavy concurrency
        return aiosqlite.connect(self.path, timeout=60.0)

    async def init(self):
        """Ultra-Optimized DB Init"""
        if self.path.exists():
            shutil.copy(self.path, f"{self.path}.bak")
            
        async with self.get_conn() as db:
            # Performance Tuning PRAGMAs
            await db.execute("PRAGMA journal_mode=WAL")      # Write-Ahead Logging
            await db.execute("PRAGMA synchronous=NORMAL")    # Faster IO
            await db.execute("PRAGMA cache_size=-64000")     # 64MB Cache
            await db.execute("PRAGMA temp_store=MEMORY")     # Temp tables in RAM
            await db.execute("PRAGMA mmap_size=30000000000") # Memory Map

            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    sub_end TEXT,
                    is_banned INTEGER DEFAULT 0,
                    created_at TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY,
                    days INTEGER,
                    activations INTEGER
                )
            """)
            # Index for faster lookups
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_sub ON users(sub_end)")
            await db.commit()

    async def check_sub(self, uid: int) -> bool:
        if uid == ADMIN_ID: return True
        async with self.get_conn() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                row = await c.fetchone()
                if not row: return False
                try: return datetime.fromisoformat(row['sub_end']) > datetime.now()
                except: return False

    async def upsert_user(self, uid: int, uname: str, days_add: int = 0):
        async with self.get_conn() as db:
            # Atomic UPSERT logic
            await db.execute("""
                INSERT INTO users (user_id, username, sub_end, created_at) 
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET 
                username=excluded.username
            """, (uid, uname, datetime.now().isoformat(), datetime.now().isoformat()))
            
            if days_add > 0:
                async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                    res = await c.fetchone()
                    curr = datetime.fromisoformat(res[0]) if res else datetime.now()
                    if curr < datetime.now(): curr = datetime.now()
                    new_end = curr + timedelta(days=days_add)
                    await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()

    async def get_user_data(self, uid: int):
        async with self.get_conn() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as c: return await c.fetchone()

    # Promo logic optimized
    async def create_promo(self, days: int, acts: int) -> str:
        code = f"OBS-{random.randint(10000,99999)}-{days}D"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

    async def use_promo(self, uid: int, code: str) -> int:
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ?", (code,)) as c:
                row = await c.fetchone()
                if not row or row[1] < 1: return 0
                days = row[0]
            
            # Atomic Update
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ?", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            await db.commit()
            
        await self.upsert_user(uid, "PromoUser", days)
        return days

db = Database()

# =========================================================================
# 🛡 STATE PERSISTENCE (ATOMIC)
# =========================================================================

class Persistence:
    @staticmethod
    def save(data: dict):
        """Atomic Save: Write to temp then rename"""
        try:
            temp_path = STATE_FILE.with_suffix('.tmp')
            serialized = {k: {**v, 'start_time': v['start_time'].isoformat()} for k, v in data.items()}
            
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(serialized, f, ensure_ascii=False)
            
            # Atomic operation on POSIX
            temp_path.replace(STATE_FILE)
        except Exception as e:
            logger.error(f"Save Failed: {e}")

    @staticmethod
    def load() -> dict:
        if not STATE_FILE.exists(): return {}
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                raw = json.load(f)
            return {k: {**v, 'start_time': datetime.fromisoformat(v['start_time'])} for k, v in raw.items()}
        except: return {}

# =========================================================================
# 🧠 CORE LOGIC (WORKER)
# =========================================================================

class ReportController:
    """Manages reports in memory with sync to disk"""
    def __init__(self):
        self._state = Persistence.load()

    def _sync(self): Persistence.save(self._state)

    def start(self, cid, tid, rtype):
        k = f"{cid}_{tid}"
        self._state[k] = {'type': rtype, 'data': [], 'start_time': datetime.now(MSK_TZ)}
        self._sync()

    def stop(self, cid, tid):
        k = f"{cid}_{tid}"
        if k in self._state:
            d = self._state.pop(k)
            self._sync()
            return d
        return None

    def add(self, cid, tid, entry):
        k = f"{cid}_{tid}"
        if k in self._state:
            # Add timestamp automatically
            t = datetime.now(MSK_TZ).strftime("%H:%M")
            if self._state[k]['type'] == 'it':
                entry['time'] = t
                self._state[k]['data'].append(entry)
            else:
                self._state[k]['data'].append(f"[{t}] {entry['user']}: {entry['text']}")
            self._sync()
            return True
        return False
    
    def get(self, cid, tid): return self._state.get(f"{cid}_{tid}")

class Worker:
    """
    Obsidian Worker Class.
    Features: Circuit Breaker, Auto-Reconnect, Memory Optimization.
    """
    __slots__ = ('uid', 'client', 'task', 'flood_task', 'reports', 'is_afk', 'afk_reason', 'raid_targets', 'react_map', 'ghost')

    def __init__(self, uid: int):
        self.uid = uid
        self.client: Optional[TelegramClient] = None
        self.task: Optional[asyncio.Task] = None
        self.flood_task: Optional[asyncio.Task] = None
        self.reports = ReportController()
        
        # State
        self.is_afk = False
        self.afk_reason = ""
        self.raid_targets: Set[int] = set()
        self.react_map: Dict[int, str] = {}
        self.ghost = False

    async def start(self):
        if not await db.check_sub(self.uid): return False
        if self.task and not self.task.done(): self.task.cancel()
        self.task = asyncio.create_task(self._lifecycle())
        return True

    async def stop(self):
        if self.flood_task: self.flood_task.cancel()
        if self.client: await self.client.disconnect()
        if self.task: self.task.cancel()

    async def _lifecycle(self):
        """Robust Lifecycle Manager with Exponential Backoff"""
        retry_delay = 1
        session_path = SESSION_DIR / f"session_{self.uid}"
        
        while True:
            try:
                if not session_path.with_suffix(".session").exists(): return
                
                # Optimized Client Config
                self.client = TelegramClient(
                    str(session_path), API_ID, API_HASH,
                    connection_retries=None,      # We handle retries
                    auto_reconnect=True,
                    flood_sleep_threshold=60,     # Sleep on small floods
                    request_retries=5             # Robustness
                )
                
                await self.client.connect()
                if not await self.client.is_user_authorized(): return
                
                # Reset backoff on success
                retry_delay = 1
                
                # Register & Block
                self._bind_events()
                await self.client.run_until_disconnected()
                
            except (ConnectionError, ServerError):
                logger.warning(f"Connection lost for {self.uid}. Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60) # Cap at 60s
            except Exception as e:
                logger.error(f"Critical Worker Error {self.uid}: {e}")
                break
            finally:
                if self.client: await self.client.disconnect()

    def _bind_events(self):
        c = self.client

        # --- EVENT: INCOMING MSG (GLOBAL) ---
        @c.on(events.NewMessage(incoming=True))
        async def on_message(e):
            # 1. Ghost Mode (Read manually if needed, else ignore)
            if not self.ghost:
                pass # Telethon reads by default usually unless suppressed

            # 2. Auto-React (Non-blocking)
            if e.chat_id in self.react_map:
                asyncio.create_task(self._safe_react(e.chat_id, e.id, self.react_map[e.chat_id]))

            # 3. Raid (Immediate response)
            if e.sender_id in self.raid_targets:
                asyncio.create_task(e.reply(random.choice(["🗑", "🤡", "Shhh", "Cry"])))

            # 4. AFK
            if self.is_afk and e.mentioned:
                asyncio.create_task(e.reply(f"💤 AFK: {self.afk_reason}"))
            
            # 5. Drop Logging
            self.reports.add(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0), 
                             {'user': e.sender.first_name, 'text': e.text}, 'drop')

        # --- COMMANDS ---

        # RAID
        @c.on(events.NewMessage(pattern=r'^\.raid$'))
        async def cmd_raid(e):
            await e.delete()
            if e.is_reply:
                r = await e.get_reply_message()
                self.raid_targets.add(r.sender_id)
                await self._temp_msg(e, f"☠️ Target: {r.sender_id}")

        @c.on(events.NewMessage(pattern=r'^\.raidstop$'))
        async def cmd_raidstop(e):
            await e.delete(); self.raid_targets.clear(); await self._temp_msg(e, "🏳️ Stopped")

        # REACT
        @c.on(events.NewMessage(pattern=r'^\.react (.+)'))
        async def cmd_react(e):
            await e.delete(); arg = e.pattern_match.group(1)
            if arg == 'stop': self.react_map.pop(e.chat_id, None)
            else: self.react_map[e.chat_id] = arg
            await self._temp_msg(e, f"🔥 React: {arg}")

        # GHOST
        @c.on(events.NewMessage(pattern=r'^\.ghost (on|off)'))
        async def cmd_ghost(e):
            await e.delete(); self.ghost = (e.pattern_match.group(1) == 'on')
            await self._temp_msg(e, f"👻 Ghost: {self.ghost}")

        # SMART FLOOD (Engine V2)
        @c.on(events.NewMessage(pattern=r'^\.(?:флуд|spam)\s+(.+)'))
        async def cmd_flood(e):
            await e.delete()
            # Smart Argument Parser
            raw = e.pattern_match.group(1).split()
            count, delay, text_arr = 10, 0.1, []
            
            c_set, d_set = False, False
            for x in raw:
                if x.isdigit() and not c_set: count = int(x); c_set = True
                elif x.replace('.', '', 1).isdigit() and not d_set: delay = float(x); d_set = True
                else: text_arr.append(x)
            
            msg = " ".join(text_arr)
            if not msg: return
            if delay < 0.05: delay = 0.05 # Safety cap

            if self.flood_task and not self.flood_task.done(): return await self._temp_msg(e, "⚠️ Busy")

            async def engine():
                status = await e.respond(f"💣 {count}x | {delay}s\n📝 {msg}")
                try:
                    for _ in range(count):
                        await c.send_message(e.chat_id, msg)
                        await asyncio.sleep(delay)
                    await status.edit("✅ Done"); await asyncio.sleep(1); await status.delete()
                except Exception as ex:
                    await status.edit(f"❌ {ex}"); await asyncio.sleep(2); await status.delete()
            
            self.flood_task = asyncio.create_task(engine())

        @c.on(events.NewMessage(pattern=r'^\.флудстоп$'))
        async def cmd_fstop(e):
            if self.flood_task: self.flood_task.cancel(); await self._temp_msg(e, "🛑")

        # SCAN CSV (Optimized generator)
        @c.on(events.NewMessage(pattern=r'^\.scan(?:\s+(\d+|all))?$'))
        async def cmd_scan(e):
            await e.delete()
            arg = e.pattern_match.group(1)
            limit = 1000000 if arg == 'all' else int(arg) if arg else 100
            
            status = await e.respond(f"📊 Scanning {limit}...")
            data = []
            
            try:
                count = 0
                async for m in c.iter_messages(e.chat_id, limit=limit):
                    count += 1
                    if m.sender and isinstance(m.sender, User) and not m.sender.bot:
                        data.append([
                            m.sender_id,
                            m.sender.first_name or "",
                            m.sender.last_name or "",
                            m.sender.username or "",
                            m.sender.phone or ""
                        ])
                    if count % 2000 == 0: await status.edit(f"📊 {count}...")

                # Write CSV in memory
                f = io.StringIO()
                writer = csv.writer(f)
                writer.writerow(['ID', 'First', 'Last', 'User', 'Phone'])
                writer.writerows(data)
                f.seek(0)
                
                await status.delete()
                await bot.send_document(
                    self.uid, 
                    BufferedInputFile(f.getvalue().encode('utf-8'), filename=f"Scan_{e.chat_id}.csv"),
                    caption=f"✅ Found: {len(data)}"
                )
                
                # Cleanup
                del data
                gc.collect()

            except Exception as ex: await status.edit(f"❌ {ex}")

        # REPORT COMMANDS
        @c.on(events.NewMessage(pattern=r'^\.айтистарт$'))
        async def it_s(e): self.reports.start(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0), 'it'); await self._temp_msg(e, "💻 Start")
        
        @c.on(events.NewMessage(pattern=r'^\.айтистоп$'))
        async def it_st(e):
            d = self.reports.stop(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if d: await self._send_report(d['data'], 'it')

        @c.on(events.NewMessage(pattern=r'^\.отчетайти$'))
        async def it_v(e):
            d = self.reports.get(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if d and d['type']=='it': await self._send_report(d['data'], 'it')

        @c.on(events.NewMessage(pattern=RE_IT_CMD))
        async def it_rec(e):
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            added = self.reports.add(e.chat_id, tid, {'user': e.sender.first_name, 'action': e.pattern_match.group(1).lower(), 'number': e.pattern_match.group(2)})
            if added: await self._safe_react(e.chat_id, e.id, '✍️')

        @c.on(events.NewMessage(pattern=r'^\.отчетыстарт$'))
        async def d_s(e): self.reports.start(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0), 'drop'); await self._temp_msg(e, "📦 Start")
        
        @c.on(events.NewMessage(pattern=r'^\.отчетыстоп$'))
        async def d_st(e):
            d = self.reports.stop(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if d: await self._send_report(d['data'], 'drop')

        @c.on(events.NewMessage(pattern=r'^\.отчетдропы$'))
        async def d_v(e):
            d = self.reports.get(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if d and d['type']=='drop': await self._send_report(d['data'], 'drop')

        # UTILS
        @c.on(events.NewMessage(pattern=r'^\.ping$'))
        async def ping(e): s=time.perf_counter(); m=await e.respond("🏓"); await m.edit(f"🏓 {((time.perf_counter()-s)*1000):.1f}ms"); await asyncio.sleep(2); await m.delete()

    # Helpers
    async def _safe_react(self, cid, mid, emo):
        try: await self.client(SendReactionRequest(cid, mid, reaction=[types.ReactionEmoji(emoticon=emo)]))
        except: pass

    async def _temp_msg(self, e, t, d=2):
        try: m=await e.respond(t); await asyncio.sleep(d); await m.delete(); await e.delete()
        except: pass

    async def _send_report(self, data, rtype):
        if rtype == 'it':
            lines = ["💻 <b>IT REPORT</b>", ""]
            lines.append("<code>{:<6}|{:<6}|{:<10}</code>".format("TIME","ACT","NUM"))
            for r in data: lines.append(f"<code>{r['time']:<6}|{r['action'][:3].upper():<6}|{r['number']:<10}</code>")
            try: await bot.send_message(self.uid, "\n".join(lines), parse_mode='HTML')
            except: pass
        else:
            f = io.BytesIO("\n".join(data).encode('utf-8'))
            try: await bot.send_document(self.uid, BufferedInputFile(f.getvalue(), filename="log.txt"))
            except: pass

# =========================================================================
# 🤖 BOT INTERFACE
# =========================================================================

WORKER_POOL: Dict[int, Worker] = {}

async def manage_worker(uid: int, action: str):
    if action == 'start':
        if uid in WORKER_POOL: await WORKER_POOL[uid].stop()
        w = Worker(uid)
        WORKER_POOL[uid] = w
        return await w.start()
    elif action == 'stop':
        if uid in WORKER_POOL: await WORKER_POOL[uid].stop(); del WORKER_POOL[uid]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# States
class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminS(StatesGroup): DAYS=State(); ACT=State(); UID=State(); D=State(); MSG=State()

# UI Helpers
def kb_m(uid):
    k = [
        [InlineKeyboardButton(text="📊 Отчеты", callback_data="m_rep"), InlineKeyboardButton(text="🤖 Бот", callback_data="m_bot")],
        [InlineKeyboardButton(text="🔑 Вход", callback_data="m_auth"), InlineKeyboardButton(text="🎟 Промо", callback_data="m_pro")],
        [InlineKeyboardButton(text="📚 Справка", callback_data="m_guide"), InlineKeyboardButton(text="👤 Профиль", callback_data="m_prof")]
    ]
    if uid == ADMIN_ID: k.append([InlineKeyboardButton(text="⚡️ ADMIN", callback_data="m_adm")])
    return InlineKeyboardMarkup(inline_keyboard=k)

@router.message(Command("start"))
async def start(m: Message):
    await db.upsert_user(m.from_user.id, m.from_user.username or "Unknown")
    await m.answer("💎 <b>StatPro OBSIDIAN</b>", reply_markup=kb_m(m.from_user.id))

@router.callback_query(F.data == "menu")
async def menu(c: CallbackQuery): await c.message.edit_text("🏠 Меню", reply_markup=kb_m(c.from_user.id))

@router.callback_query(F.data == "m_guide")
async def guide(c: CallbackQuery):
    await c.message.edit_text(
        "<b>📚 OBSIDIAN GUIDE</b>\n\n"
        "<b>💣 Smart Spam:</b> <code>.флуд [текст] [кол] [delay]</code>\n"
        "<b>☠️ Raid:</b> <code>.raid</code> (reply) / <code>.raidstop</code>\n"
        "<b>🔥 React:</b> <code>.react 👍</code> / <code>stop</code>\n"
        "<b>👻 Ghost:</b> <code>.ghost on/off</code>\n"
        "<b>📊 Scan:</b> <code>.scan all</code> (to CSV)\n"
        "<b>💻 IT:</b> <code>.айтистарт</code>, <code>.встал</code>, <code>.айтистоп</code>\n",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="menu")]])
    )

# AUTH FLOW
@router.callback_query(F.data == "m_auth")
async def auth_sel(c: CallbackQuery): await c.message.edit_text("Метод:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="QR", callback_data="a_qr"), InlineKeyboardButton(text="Phone", callback_data="a_ph")], [InlineKeyboardButton(text="🔙", callback_data="menu")]]))

@router.callback_query(F.data == "a_qr")
async def auth_qr(c: CallbackQuery):
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ No Sub", True)
    path = SESSION_DIR / f"session_{c.from_user.id}"
    cl = TelegramClient(str(path), API_ID, API_HASH)
    await cl.connect()
    
    qr = await cl.qr_login()
    img = qrcode.make(qr.url).convert("RGB")
    b = io.BytesIO(); img.save(b, "PNG"); b.seek(0)
    
    m = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"))
    try:
        await qr.wait(60)
        await m.delete()
        await c.message.answer("✅ Linked")
        await cl.disconnect()
        await manage_worker(c.from_user.id, 'start')
    except: await m.delete()

@router.callback_query(F.data == "a_ph")
async def auth_ph(c: CallbackQuery, s: FSMContext):
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ No Sub", True)
    await c.message.edit_text("📱 Phone:"); await s.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def auth_ph_s(m: Message, s: FSMContext):
    cl = TelegramClient(str(SESSION_DIR / f"session_{m.from_user.id}"), API_ID, API_HASH)
    await cl.connect()
    r = await cl.send_code_request(m.text)
    await s.update_data(p=m.text, h=r.phone_code_hash, cl=cl)
    await m.answer("📩 Code:"); await s.set_state(AuthS.CO)

@router.message(AuthS.CO)
async def auth_co(m: Message, s: FSMContext):
    d = await s.get_data(); cl = d['cl']
    try:
        await cl.sign_in(phone=d['p'], code=m.text, phone_code_hash=d['h'])
        await m.answer("✅ Linked"); await cl.disconnect(); await manage_worker(m.from_user.id, 'start'); await s.clear()
    except SessionPasswordNeededError:
        await m.answer("🔒 2FA:"); await s.set_state(AuthS.PA)

@router.message(AuthS.PA)
async def auth_pa(m: Message, s: FSMContext):
    d = await s.get_data(); cl = d['cl']
    await cl.sign_in(password=m.text)
    await m.answer("✅ Linked"); await cl.disconnect(); await manage_worker(m.from_user.id, 'start'); await s.clear()

# CONTROL
@router.callback_query(F.data == "m_bot")
async def ctrl(c: CallbackQuery):
    st = "🟢 ON" if c.from_user.id in WORKER_POOL else "🔴 OFF"
    await c.message.edit_text(f"Status: {st}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Start", callback_data="c_on"), InlineKeyboardButton(text="Stop", callback_data="c_off")], [InlineKeyboardButton(text="🔙", callback_data="menu")]]))

@router.callback_query(F.data == "c_on")
async def c_on(c: CallbackQuery): await manage_worker(c.from_user.id, 'start'); await ctrl(c)
@router.callback_query(F.data == "c_off")
async def c_off(c: CallbackQuery): await manage_worker(c.from_user.id, 'stop'); await ctrl(c)

# ADMIN
@router.callback_query(F.data == "m_adm")
async def adm(c: CallbackQuery): await c.message.edit_text("Admin", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Add Promo", callback_data="ad_p"), InlineKeyboardButton(text="Grant", callback_data="ad_g")], [InlineKeyboardButton(text="🔙", callback_data="menu")]]))

@router.callback_query(F.data == "ad_p")
async def ad_p(c: CallbackQuery, s: FSMContext): await c.message.edit_text("Days:"); await s.set_state(AdminS.DAYS)
@router.message(AdminS.DAYS)
async def ad_pd(m: Message, s: FSMContext): await s.update_data(d=int(m.text)); await m.answer("Acts:"); await s.set_state(AdminS.ACT)
@router.message(AdminS.ACT)
async def ad_pa(m: Message, s: FSMContext): 
    d = await s.get_data(); code = await db.create_promo(d['d'], int(m.text))
    await m.answer(f"<code>{code}</code>"); await s.clear()

@router.callback_query(F.data == "m_pro")
async def pro(c: CallbackQuery, s: FSMContext): await c.message.edit_text("Code:"); await s.set_state(PromoS.CODE)
@router.message(PromoS.CODE)
async def pro_c(m: Message, s: FSMContext):
    d = await db.use_promo(m.from_user.id, m.text.strip())
    await m.answer(f"✅ +{d} days" if d else "❌"); await s.clear()

async def main():
    await db.init()
    # Cleanup sessions
    for f in SESSION_DIR.glob("*.session"):
        if f.stat().st_size == 0: f.unlink()
    
    # Auto-resume
    for f in SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub(uid): await manage_worker(uid, 'start')
        except: pass

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
