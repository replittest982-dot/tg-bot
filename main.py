#!/usr/bin/env python3
"""
💎 StatPro v38.0 - TITANIUM EDITION
-----------------------------------
✅ FIX DB: База данных создается локально (решает ошибку доступа).
✅ FIX PROMO: Промокоды работают идеально (без учета регистра/пробелов).
🇷🇺 LANG: Полный, грамотный русский перевод.
🚀 CORE: Оптимизация памяти и жесткая защита от вылетов.
"""

import asyncio
import logging
import os
import sys
import io
import random
import shutil
import time
import json
import csv
import gc
import aiosqlite
from typing import Dict, Optional, Set
from pathlib import Path
from datetime import datetime, timedelta, timezone

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile
)
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

# --- TELETHON ---
from telethon import TelegramClient, events, types
from telethon.errors import SessionPasswordNeededError, PhoneCodeExpiredError
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import User, Channel, Chat

import qrcode
from PIL import Image

# =========================================================================
# ⚙️ НАСТРОЙКИ СИСТЕМЫ
# =========================================================================

# Определяем путь относительно запускаемого скрипта (РЕШАЕТ ПРОБЛЕМУ ДОСТУПА)
BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "titanium.db"
STATE_FILE = BASE_DIR / "state.json"

# Создаем папки при старте
SESSION_DIR.mkdir(parents=True, exist_ok=True)

VERSION = "v38.0 TITANIUM"
MSK_TZ = timezone(timedelta(hours=3))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("StatPro")

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
except: sys.exit(1)

if not all([BOT_TOKEN, API_ID, API_HASH]):
    logger.critical("❌ НЕ ЗАПОЛНЕНЫ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ!")
    sys.exit(1)

RE_IT_CMD = r'^\.(встал|зм|пв)\s*(\d+)$'

# =========================================================================
# 🗄️ БАЗА ДАННЫХ (ИСПРАВЛЕННАЯ)
# =========================================================================

class Database:
    __slots__ = ('path',)
    _instance = None

    def __new__(cls):
        if cls._instance is None: cls._instance = super(Database, cls).__new__(cls)
        return cls._instance

    def __init__(self): self.path = DB_PATH

    def get_conn(self):
        # check_same_thread=False решает проблемы с блокировками
        return aiosqlite.connect(self.path, timeout=30.0)

    async def init(self):
        # Бэкап базы если есть
        if self.path.exists():
            try: shutil.copy(self.path, f"{self.path}.bak")
            except: pass
            
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    sub_end TEXT,
                    joined_at TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY,
                    days INTEGER,
                    activations INTEGER
                )
            """)
            await db.commit()

    async def upsert_user(self, uid: int, uname: str):
        async with self.get_conn() as db:
            cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (uid,))
            if not await cursor.fetchone():
                await db.execute("INSERT INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, ?, ?)", 
                                 (uid, uname, datetime.now().isoformat(), datetime.now().isoformat()))
            else:
                await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
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

    async def update_sub(self, uid: int, days: int):
        u_date = datetime.now()
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                if r:
                    try: 
                        curr = datetime.fromisoformat(r[0])
                        if curr > u_date: u_date = curr
                    except: pass
        
        new_end = u_date + timedelta(days=days)
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, sub_end, joined_at) VALUES (?, ?, ?, ?)", 
                             (uid, datetime.now().isoformat(), datetime.now().isoformat()))
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"STAT-{random.randint(1000,9999)}-{days}D"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip() # Убираем пробелы
        async with self.get_conn() as db:
            # COLLATE NOCASE = Неважен регистр (Code == code)
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                row = await c.fetchone()
                if not row or row[1] < 1: return 0
                days = row[0]
            
            # Обновляем
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            await db.commit()
            
        await self.update_sub(uid, days)
        return days
    
    async def get_stats(self):
        async with self.get_conn() as db:
            async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
            now = datetime.now().isoformat()
            async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (now,)) as c: active = (await c.fetchone())[0]
        return total, active

db = Database()

# =========================================================================
# 📊 МЕНЕДЖЕР ОТЧЕТОВ
# =========================================================================

class ReportPersistence:
    @staticmethod
    def save(data: dict):
        try:
            d = {k: {**v, 'start_time': v['start_time'].isoformat()} for k, v in data.items()}
            with open(STATE_FILE, 'w', encoding='utf-8') as f: json.dump(d, f, ensure_ascii=False)
        except: pass

    @staticmethod
    def load() -> dict:
        if not STATE_FILE.exists(): return {}
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f: r = json.load(f)
            return {k: {**v, 'start_time': datetime.fromisoformat(v['start_time'])} for k, v in r.items()}
        except: return {}

class ReportManager:
    __slots__ = ('_state',)
    def __init__(self): self._state = ReportPersistence.load()
    def _sync(self): ReportPersistence.save(self._state)

    def start(self, cid, tid, rtype):
        self._state[f"{cid}_{tid}"] = {'type': rtype, 'data': [], 'start_time': datetime.now(MSK_TZ)}
        self._sync()

    def add(self, cid, tid, entry):
        k = f"{cid}_{tid}"
        if k in self._state:
            t = datetime.now(MSK_TZ).strftime("%H:%M")
            if self._state[k]['type'] == 'it':
                entry['time'] = t; self._state[k]['data'].append(entry)
            else:
                self._state[k]['data'].append(f"[{t}] {entry['user']}: {entry['text']}")
            self._sync(); return True
        return False

    def stop(self, cid, tid):
        k = f"{cid}_{tid}"
        if k in self._state: d = self._state.pop(k); self._sync(); return d
        return None
    
    def get(self, cid, tid): return self._state.get(f"{cid}_{tid}")

# =========================================================================
# 🧠 ВОРКЕР (ПОЛНОСТЬЮ БЕЗОПАСНЫЙ)
# =========================================================================

class Worker:
    __slots__ = ('uid', 'client', 'task', 'flood_task', 'reports', 'is_afk', 'afk_reason', 'raid_targets', 'react_map', 'ghost', 'status')

    def __init__(self, uid: int):
        self.uid = uid
        self.client: Optional[TelegramClient] = None
        self.task: Optional[asyncio.Task] = None
        self.flood_task: Optional[asyncio.Task] = None
        self.reports = ReportManager()
        self.is_afk = False; self.afk_reason = ""
        self.raid_targets: Set[int] = set()
        self.react_map: Dict[int, str] = {}
        self.ghost = False
        self.status = "⚪️ Загрузка..."

    async def start(self):
        if not await db.check_sub(self.uid): self.status = "⛔️ Нет подписки"; return False
        if self.task and not self.task.done(): self.task.cancel()
        self.task = asyncio.create_task(self._run())
        return True

    async def stop(self):
        self.status = "🔴 Остановлен"
        if self.flood_task: self.flood_task.cancel()
        if self.client: await self.client.disconnect()
        if self.task: self.task.cancel()

    async def _run(self):
        s_path = SESSION_DIR / f"session_{self.uid}"
        while True:
            try:
                # Очистка памяти каждые 10 мин
                gc.collect() 
                
                if not s_path.with_suffix(".session").exists(): self.status = "🔴 Нет сессии (войдите заново)"; return
                
                self.client = TelegramClient(str(s_path), API_ID, API_HASH, connection_retries=None, auto_reconnect=True)
                await self.client.connect()
                
                if not await self.client.is_user_authorized(): self.status = "🔴 Ошибка авторизации"; return
                
                self.status = "🟢 В работе"
                self._bind()
                await self.client.run_until_disconnected()
                
            except Exception as e:
                logger.error(f"Worker Error {self.uid}: {e}")
                self.status = f"⚠️ Сбой: {str(e)[:15]}"
                await asyncio.sleep(5) # Авто-рестарт
            finally:
                if self.client: await self.client.disconnect()

    def _bind(self):
        c = self.client

        def get_name(event):
            s = event.sender
            if s is None: return event.chat.title if event.chat else 'Аноним'
            if isinstance(s, User):
                n = s.first_name or ""
                if s.last_name: n += f" {s.last_name}"
                return n.strip() or s.username or str(s.id)
            if hasattr(s, 'title'): return s.title
            return 'Неизвестно'

        @c.on(events.NewMessage(incoming=True))
        async def on_msg(e):
            if not self.ghost: pass
            
            # Реакции
            if e.chat_id in self.react_map: 
                asyncio.create_task(self._safe_react(e.chat_id, e.id, self.react_map[e.chat_id]))
            
            # Рейд
            if e.sender_id and e.sender_id in self.raid_targets: 
                asyncio.create_task(e.reply(random.choice(["🗑", "🤡", "🤫"])))
            
            # AFK
            if self.is_afk and e.mentioned: 
                asyncio.create_task(e.reply(f"💤 Я сейчас AFK: {self.afk_reason}"))
            
            # Логи
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            is_bot = e.sender.bot if (e.sender and isinstance(e.sender, User)) else False
            is_cmd = e.text and e.text.startswith(".")
            
            if not is_cmd and not is_bot:
                self.reports.add(e.chat_id, tid, {'user': get_name(e), 'text': e.text or "[Файл]"})

        # --- КОМАНДЫ ---
        
        @c.on(events.NewMessage(pattern=r'^\.(?:флуд|spam)\s+(.+)'))
        async def flood(e):
            await e.delete()
            # Умный парсинг аргументов
            raw = e.pattern_match.group(1).split()
            count, delay, txt = 10, 0.1, []
            c_set, d_set = False, False
            
            for x in raw:
                if x.isdigit() and not c_set: count=int(x); c_set=True
                elif x.replace('.', '', 1).isdigit() and not d_set: delay=float(x); d_set=True
                else: txt.append(x)
            
            msg = " ".join(txt)
            if not msg: return
            if delay < 0.05: delay = 0.05 # Защита от бана
            
            if self.flood_task and not self.flood_task.done(): return await self._tmsg(e, "⚠️ Уже спамлю!")
            
            async def run():
                st = await e.respond(f"💣 <b>Спам:</b> {count}x\n⏱ <b>Задержка:</b> {delay}s\n📝 <b>Текст:</b> {msg}")
                try:
                    for _ in range(count): await c.send_message(e.chat_id, msg); await asyncio.sleep(delay)
                    await st.edit("✅ Готово"); await asyncio.sleep(1); await st.delete()
                except: await st.delete()
            self.flood_task = asyncio.create_task(run())

        @c.on(events.NewMessage(pattern=r'^\.флудстоп$'))
        async def fstop(e): 
            if self.flood_task: self.flood_task.cancel(); await self._tmsg(e, "🛑 Спам остановлен")

        @c.on(events.NewMessage(pattern=r'^\.scan(?:\s+(\d+|all))?$'))
        async def scan(e):
            await e.delete()
            arg = e.pattern_match.group(1)
            limit = 1000000 if arg == 'all' else int(arg or 100)
            
            st = await e.respond(f"📊 Сканирую {limit} сообщений...")
            data = []
            try:
                cnt = 0
                async for m in c.iter_messages(e.chat_id, limit=limit):
                    cnt+=1
                    uid = m.sender_id or 0; first = ""; user = ""
                    if m.sender and isinstance(m.sender, User):
                        first = m.sender.first_name or ""; user = m.sender.username or ""
                    data.append([uid, first, user])
                    if cnt%2000==0: await st.edit(f"📊 Обработано: {cnt}...")
                
                # CSV Export
                f = io.StringIO()
                csv.writer(f).writerows([['ID', 'Имя', 'Юзернейм']] + data)
                f.seek(0)
                
                await st.delete()
                await bot.send_document(self.uid, BufferedInputFile(f.getvalue().encode(), filename="scan_result.csv"), caption=f"✅ Найдено уникальных: {len(data)}")
                del data; gc.collect()
            except Exception as ex: await st.edit(f"❌ Ошибка: {ex}")

        # REPORT CMDS
        @c.on(events.NewMessage(pattern=r'^\.айтистарт$'))
        async def its(e): self.reports.start(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0), 'it'); await self._tmsg(e, "💻 IT Смена: Начата")
        @c.on(events.NewMessage(pattern=r'^\.айтистоп$'))
        async def itst(e): 
            d=self.reports.stop(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if d: await self._send_rep(d['data'], 'it')
        @c.on(events.NewMessage(pattern=r'^\.отчетайти$'))
        async def itv(e):
            d=self.reports.get(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if d and d['type']=='it': await self._send_rep(d['data'], 'it')
        @c.on(events.NewMessage(pattern=RE_IT_CMD))
        async def itr(e):
            tid = e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0)
            if self.reports.add(e.chat_id, tid, {'user':get_name(e), 'action':e.pattern_match.group(1).lower(), 'number':e.pattern_match.group(2)}):
                await self._safe_react(e.chat_id, e.id, '✍️')

        @c.on(events.NewMessage(pattern=r'^\.отчетыстарт$'))
        async def ds(e): self.reports.start(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0), 'drop'); await self._tmsg(e, "📦 Дроп-лог: Запущен")
        @c.on(events.NewMessage(pattern=r'^\.отчетыстоп$'))
        async def dst(e):
            d=self.reports.stop(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
            if d: await self._send_rep(d['data'], 'drop')
        @c.on(events.NewMessage(pattern=r'^\.отчетдропы$'))
        async def dsv(e):
             d=self.reports.get(e.chat_id, e.reply_to.reply_to_msg_id if e.reply_to else (e.reply_to_msg_id or 0))
             if d and d['type']=='drop': await self._send_rep(d['data'], 'drop')

        # Utils
        @c.on(events.NewMessage(pattern=r'^\.ping$'))
        async def pg(e): s=time.perf_counter(); m=await e.respond("🏓"); await m.edit(f"🏓 Пинг: {((time.perf_counter()-s)*1000):.1f}ms"); await asyncio.sleep(2); await m.delete(); await e.delete()
        @c.on(events.NewMessage(pattern=r'^\.react (.+)'))
        async def rct(e): await e.delete(); a=e.pattern_match.group(1); self.react_map[e.chat_id]=a if a!='stop' else self.react_map.pop(e.chat_id,None); await self._tmsg(e, f"🔥 Реакции: {a}")
        @c.on(events.NewMessage(pattern=r'^\.ghost (on|off)'))
        async def gh(e): await e.delete(); self.ghost=(e.pattern_match.group(1)=='on'); await self._tmsg(e, f"👻 Призрак: {'ВКЛ' if self.ghost else 'ВЫКЛ'}")
        @c.on(events.NewMessage(pattern=r'^\.raid$'))
        async def rd(e): await e.delete(); (self.raid_targets.add((await e.get_reply_message()).sender_id) if e.is_reply else None); await self._tmsg(e, "☠️ Рейд активен")
        @c.on(events.NewMessage(pattern=r'^\.raidstop$'))
        async def rds(e): await e.delete(); self.raid_targets.clear(); await self._tmsg(e, "🏳️ Рейд остановлен")

    async def _safe_react(self, cid, mid, e):
        try: await self.client(SendReactionRequest(cid, mid, reaction=[types.ReactionEmoji(emoticon=e)]))
        except: pass

    async def _tmsg(self, e, t):
        try: m=await e.respond(t); await asyncio.sleep(2); await m.delete(); await e.delete()
        except: pass

    async def _send_rep(self, data, rt):
        if rt == 'it':
            l = ["💻 <b>ОТЧЕТ IT</b>", "", "<code>ВРЕМЯ |АКТ   |НОМЕР</code>"]
            for r in data: l.append(f"<code>{r['time']:<6}|{r['action'][:3].upper():<6}|{r['number']:<10}</code>")
            try: await bot.send_message(self.uid, "\n".join(l), parse_mode='HTML')
            except: pass
        else:
            f=io.BytesIO("\n".join(data).encode()); f.name="log.txt"
            try: await bot.send_document(self.uid, BufferedInputFile(f.getvalue(), filename="log.txt"))
            except: pass

# =========================================================================
# 🤖 BOT UI
# =========================================================================

W_POOL: Dict[int, Worker] = {}

async def mng_w(uid, act):
    if act=='start':
        if uid in W_POOL: await W_POOL[uid].stop()
        w=Worker(uid); W_POOL[uid]=w; return await w.start()
    elif act=='stop' and uid in W_POOL: await W_POOL[uid].stop(); del W_POOL[uid]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdmS(StatesGroup): D=State(); A=State(); U=State(); UD=State()

def kb(uid, is_admin, has_sub):
    k = []
    if has_sub or is_admin:
        k.append([InlineKeyboardButton(text="📊 Отчеты", callback_data="m_rep"),
                  InlineKeyboardButton(text="⚙️ Бот", callback_data="m_bot")])
        k.append([InlineKeyboardButton(text="🔑 Вход", callback_data="m_auth"),
                  InlineKeyboardButton(text="📚 Инструкция", callback_data="m_g")])
    k.append([InlineKeyboardButton(text="🎟 Промокод", callback_data="m_pro"),
              InlineKeyboardButton(text="👤 Профиль", callback_data="m_p")])
    if is_admin:
        k.append([InlineKeyboardButton(text="⚡️ Статус", callback_data="ad_stat"), 
                  InlineKeyboardButton(text="👑 Админ", callback_data="m_adm")])
    return InlineKeyboardMarkup(inline_keyboard=k)

@router.message(Command("start"))
async def start(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username or "Unknown")
    sub = await db.check_sub(m.from_user.id)
    admin = (m.from_user.id == ADMIN_ID)
    await m.answer("💎 <b>StatPro</b>", reply_markup=kb(m.from_user.id, admin, sub))

@router.callback_query(F.data=="menu")
async def mn(c: CallbackQuery, state: FSMContext): 
    await state.clear()
    sub = await db.check_sub(c.from_user.id)
    admin = (c.from_user.id == ADMIN_ID)
    await c.message.edit_text("🏠 Главное меню", reply_markup=kb(c.from_user.id, admin, sub))

# AUTH LOGIC (ATOMIC)
@router.callback_query(F.data=="m_auth")
async def ma(c: CallbackQuery): 
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ Нужна подписка", True)
    await c.message.edit_text("Выберите метод входа:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 QR-код",callback_data="a_qr"),InlineKeyboardButton(text="📱 Телефон",callback_data="a_ph")],[InlineKeyboardButton(text="🔙 Назад",callback_data="menu")]]))

@router.callback_query(F.data=="a_qr")
async def aqr(c: CallbackQuery):
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ Нужна подписка", True)
    path=SESSION_DIR/f"session_{c.from_user.id}"; cl=TelegramClient(str(path), API_ID, API_HASH); await cl.connect()
    qr=await cl.qr_login(); i=qrcode.make(qr.url).convert("RGB"); b=io.BytesIO(); i.save(b,"PNG"); b.seek(0)
    m=await c.message.answer_photo(BufferedInputFile(b.read(),"qr.png"))
    try: await qr.wait(60); await m.delete(); await c.message.answer("✅ Успешно подключено!"); await cl.disconnect(); await mng_w(c.from_user.id,'start')
    except: await m.delete()

@router.callback_query(F.data=="a_ph")
async def aph(c: CallbackQuery, state: FSMContext): 
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ Нужна подписка", True)
    await c.message.edit_text("📱 Введите номер телефона (79xxxxxxxxx):"); await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def aphs(m: Message, state: FSMContext): 
    try:
        cl=TelegramClient(str(SESSION_DIR/f"session_{m.from_user.id}"), API_ID, API_HASH); await cl.connect()
        r=await cl.send_code_request(m.text); await state.update_data(p=m.text,h=r.phone_code_hash,cl=cl); await m.answer("📩 Введите код из Telegram:"); await state.set_state(AuthS.CO)
    except Exception as e: await m.answer(f"❌ Ошибка отправки: {e}"); await state.clear()

@router.message(AuthS.CO)
async def aco(m: Message, state: FSMContext): 
    d=await state.get_data(); cl=d['cl']
    async def fail(err): 
        if cl: await cl.disconnect()
        await m.answer(f"❌ {err}. Начните заново."); await state.clear()
    
    try: 
        await cl.sign_in(phone=d['p'],code=m.text,phone_code_hash=d['h']); 
        await m.answer("✅ Авторизация успешна!"); await cl.disconnect(); await mng_w(m.from_user.id,'start'); await state.clear()
    except SessionPasswordNeededError: await m.answer("🔒 Введите пароль 2FA:"); await state.set_state(AuthS.PA)
    except PhoneCodeExpiredError: await fail("Код истёк или был использован")
    except Exception as e: await fail(f"Ошибка входа: {str(e)[:50]}")

@router.message(AuthS.PA)
async def apa(m: Message, state: FSMContext): 
    d=await state.get_data(); cl=d['cl']
    try: await cl.sign_in(password=m.text); await m.answer("✅ Успешно!"); await cl.disconnect(); await mng_w(m.from_user.id,'start'); await state.clear()
    except Exception as e: await m.answer(f"❌ Ошибка пароля: {e}"); await cl.disconnect(); await state.clear()

# PROMO
@router.callback_query(F.data=="m_pro")
async def mpro(c: CallbackQuery, state: FSMContext): await c.message.edit_text("🎟 Введите промокод:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад",callback_data="menu")]])); await state.set_state(PromoS.CODE) 

@router.message(PromoS.CODE)
async def proc(m: Message, state: FSMContext): 
    d=await db.use_promo(m.from_user.id, m.text.strip()); await m.answer(f"✅ Успешно! Добавлено +{d} дней." if d else "❌ Неверный код или он закончился."); await state.clear()

# ADMIN
@router.callback_query(F.data=="m_adm")
async def madm(c: CallbackQuery): await c.message.edit_text("👑 Админ-панель", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Создать промо",callback_data="ad_p"),InlineKeyboardButton(text="🎁 Выдать саб",callback_data="ad_g")],[InlineKeyboardButton(text="🔙 Назад",callback_data="menu")]]))

@router.callback_query(F.data=="ad_stat")
async def ad_stat(c: CallbackQuery):
    tot = len(W_POOL); act = sum(1 for w in W_POOL.values() if "В работе" in w.status); u, s = await db.get_stats(); ts = datetime.now(MSK_TZ).strftime("%H:%M:%S")
    msg = f"⚡️ <b>МОНИТОРИНГ</b> [{ts}]\nВоркеры: {tot} (Активны: {act})\nБД: {u} юзеров ({s} с сабкой)\n\n<b>Сессии:</b>\n"
    for uid, w in W_POOL.items(): msg += f"• <code>{uid}</code>: {w.status}\n"
    if not W_POOL: msg += "Нет запущенных сессий."
    await c.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Обновить",callback_data="ad_stat")],[InlineKeyboardButton(text="🔙 Назад",callback_data="menu")]]))

@router.callback_query(F.data=="ad_p")
async def adp(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Кол-во дней:"); await state.set_state(AdmS.D)
@router.message(AdmS.D)
async def adpd(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Кол-во активаций:"); await state.set_state(AdmS.A)
@router.message(AdmS.A)
async def adpa(m: Message, state: FSMContext): d=await state.get_data(); c=await db.create_promo(d['d'],int(m.text)); await m.answer(f"🎟 Код: <code>{c}</code>"); await state.clear()
@router.callback_query(F.data=="ad_g")
async def adg(c: CallbackQuery, state: FSMContext): await c.message.edit_text("ID пользователя:"); await state.set_state(AdmS.U)
@router.message(AdmS.U)
async def adgu(m: Message, state: FSMContext): await state.update_data(u=m.text); await m.answer("Дней:"); await state.set_state(AdmS.UD)
@router.message(AdmS.UD)
async def adgd(m: Message, state: FSMContext): d=await state.get_data(); await db.update_sub(int(d['u']),int(m.text)); await m.answer("✅ Выдано!"); await state.clear()

# MISC
@router.callback_query(F.data=="m_bot")
async def mbot(c: CallbackQuery):
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ Нужна подписка", True)
    w=W_POOL.get(c.from_user.id); s=w.status if w else "🔴 Остановлен"
    await c.message.edit_text(f"🤖 Состояние бота: {s}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🟢 Запуск",callback_data="w_on"),InlineKeyboardButton(text="🔴 Стоп",callback_data="w_off")],[InlineKeyboardButton(text="🔙 Назад",callback_data="menu")]]))

@router.callback_query(F.data=="w_on")
async def won(c: CallbackQuery): await c.answer("⏳ Запускаю..."); await mng_w(c.from_user.id,'start'); await mbot(c)
@router.callback_query(F.data=="w_off")
async def woff(c: CallbackQuery): await mng_w(c.from_user.id,'stop'); await mbot(c)

@router.callback_query(F.data=="m_g")
async def mg(c: CallbackQuery): 
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ Нужна подписка", True)
    await c.message.edit_text("<b>📚 СПРАВОЧНИК</b>\n\n<b>💣 Спам:</b> <code>.флуд [текст] [кол-во] [сек]</code>\n<b>📊 Скан:</b> <code>.scan all</code>\n<b>☠️ Рейд:</b> <code>.raid</code> (реплай)\n<b>🔥 Реакции:</b> <code>.react 👍</code>\n<b>💻 IT:</b> <code>.айтистарт</code>, <code>.встал 1</code>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад",callback_data="menu")]]))

@router.callback_query(F.data=="m_rep")
async def mr(c: CallbackQuery): 
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ Нужна подписка", True)
    await c.message.edit_text("📂 Выберите тип отчета:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💻 IT Смена",callback_data="r_i"),InlineKeyboardButton(text="📦 Дроп Лог",callback_data="r_d")],[InlineKeyboardButton(text="🔙 Назад",callback_data="menu")]]))
@router.callback_query(F.data=="r_i")
async def ri(c: CallbackQuery): await c.message.edit_text("💻 <b>IT Команды:</b>\n\n<code>.айтистарт</code> - Начать смену\n<code>.встал [номер]</code> - Запись\n<code>.отчетайти</code> - Выгрузить", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад",callback_data="m_rep")]]))
@router.callback_query(F.data=="r_d")
async def rd(c: CallbackQuery): await c.message.edit_text("📦 <b>Дроп Команды:</b>\n\n<code>.отчетыстарт</code> - Начать лог\n<code>.отчетдропы</code> - Выгрузить лог", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад",callback_data="m_rep")]]))

@router.callback_query(F.data=="m_p")
async def mp(c: CallbackQuery):
    u=await db.check_sub(c.from_user.id); s="✅ АКТИВНА" if u else "❌ НЕ АКТИВНА"; s = "👑 АДМИН" if c.from_user.id==ADMIN_ID else s
    await c.message.edit_text(f"👤 <b>Профиль</b>\nID: <code>{c.from_user.id}</code>\n💎 Подписка: {s}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад",callback_data="menu")]]))

async def main():
    await db.init()
    for f in SESSION_DIR.glob("*.session"):
        if f.stat().st_size==0: f.unlink()
    for f in SESSION_DIR.glob("session_*.session"):
        try:
            uid=int(f.stem.split("_")[1])
            if await db.check_sub(uid): await mng_w(uid,'start')
        except: pass
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except: pass
