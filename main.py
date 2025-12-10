#!/usr/bin/env python3
"""
🧬 StatPro v63.0 - HOMO SAPIENS EDITION
---------------------------------------
✅ EVOLUTION: Асинхронное сохранение с Debounce (без лагов).
✅ PERFORMANCE: Запись файлов вынесена в Executor (не блокирует Loop).
✅ MEMORY: Оптимизированная генерация QR и буферов.
✅ SAFETY: Graceful Shutdown (сохранение при выходе).
"""

import asyncio
import logging
import os
import json
import random
import time
import qrcode
import aiosqlite
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from dataclasses import dataclass

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    CallbackQuery, Message, BufferedInputFile
)
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# --- TELETHON ---
from telethon import TelegramClient, events, types
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import User
from telethon.tl.functions.messages import SendReactionRequest

# =========================================================================
# ⚙️ CONFIGURATION CLASS
# =========================================================================

@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "ВАШ_ТОКЕН")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "ВАШ_ХЭШ")
    SUB_CHANNEL: str = "@STAT_PRO1"
    TZ: timezone = timezone(timedelta(hours=3))
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_v63.db"
    STATE_FILE: Path = BASE_DIR / "statpro_reports.json"

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)

cfg = Config()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger("StatPro_v63")

# =========================================================================
# 🛠 UTILS
# =========================================================================

def get_now_ts() -> int:
    return int(datetime.now(cfg.TZ).timestamp())

def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, cfg.TZ).strftime("%d.%m.%Y %H:%M")

LAST_ACTION: Dict[int, float] = {}

def can_toggle(uid: int, cooldown: float = 1.0) -> bool:
    now = time.time()
    last = LAST_ACTION.get(uid, 0)
    if now - last < cooldown: return False
    LAST_ACTION[uid] = now
    return True

# =========================================================================
# 🗄️ DATABASE (ASYNC WAL)
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
        now = get_now_ts()
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, ?, ?)", (uid, uname, 0, now))
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def check_sub_bool(self, uid: int) -> bool:
        if uid == cfg.ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                return r[0] > get_now_ts() if (r and r[0]) else False

    async def add_sub_days(self, uid: int, days: int):
        now = get_now_ts()
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
# 📊 REPORT MANAGER (ASYNC EVOLUTION)
# =========================================================================

class ReportManager:
    def __init__(self):
        self.data = {}
        self._save_task: Optional[asyncio.Task] = None
        self.load()

    def load(self):
        if cfg.STATE_FILE.exists():
            try:
                with open(cfg.STATE_FILE, 'r', encoding='utf-8') as f:
                    self.data = json.load(f)
            except: self.data = {}

    def _write_to_file(self):
        """Блокирующая запись, которая будет запущена в Executor"""
        try:
            with open(cfg.STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Save error: {e}")

    async def _async_save(self):
        """Асинхронная обертка с Debounce"""
        try:
            await asyncio.sleep(2.0) # Debounce: ждем 2 сек, накапливаем изменения
            loop = asyncio.get_running_loop()
            # Запуск блокирующего I/O в отдельном потоке
            await loop.run_in_executor(None, self._write_to_file)
        except Exception as e:
            logger.error(f"Async save error: {e}")
        finally:
            self._save_task = None

    def save(self):
        """Триггер сохранения"""
        if self._save_task and not self._save_task.done():
            return # Задача уже запланирована, не плодим новые
        self._save_task = asyncio.create_task(self._async_save())

    async def flush(self):
        """Принудительное сохранение при выходе"""
        if self._save_task:
            self._save_task.cancel()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._write_to_file)
        logger.info("Reports flushed to disk.")

    def start_session(self, uid: int, mode: str):
        self.data[str(uid)] = {'mode': mode, 'logs': [], 'start': get_now_ts()}
        self.save()

    def add_log(self, uid: int, entry: str):
        uid_s = str(uid)
        if uid_s in self.data:
            ts = datetime.now(cfg.TZ).strftime("%H:%M:%S")
            self.data[uid_s]['logs'].append(f"[{ts}] {entry}")
            self.save()

    def stop_session(self, uid: int):
        uid_s = str(uid)
        if uid_s in self.data:
            res = self.data.pop(uid_s)
            self.save()
            return res
        return None

    def get_mode(self, uid: int):
        return self.data.get(str(uid), {}).get('mode')

rm = ReportManager()

# =========================================================================
# 🤖 WORKER
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None

    async def start(self):
        s_path = cfg.SESSION_DIR / f"session_{self.uid}"
        self.client = TelegramClient(str(s_path), cfg.API_ID, cfg.API_HASH, 
                                     auto_reconnect=True, system_version="StatPro v63")
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): return False
            self._bind()
            asyncio.create_task(self.client.run_until_disconnected())
            return True
        except Exception:
            logger.exception(f"Worker {self.uid} Crash")
            return False

    async def stop(self):
        if self.client: await self.client.disconnect()

    def _bind(self):
        @self.client.on(events.NewMessage)
        async def handler(e):
            try:
                mode = rm.get_mode(self.uid)
                if not mode: return

                # IT Mode (Outgoing)
                if mode == 'it' and e.out:
                    txt = e.text.lower() if e.text else ""
                    if txt.startswith(('.встал', '.зм', '.пв')):
                        rm.add_log(self.uid, f"CMD: {e.text}")
                        try: await e.client(SendReactionRequest(e.chat_id, e.id, [types.ReactionEmoji(emoticon='✍️')]))
                        except: pass
                
                # Drop Mode (Incoming)
                if mode == 'drop' and e.incoming:
                    name = "Unknown"
                    sender = await e.get_sender()
                    if isinstance(sender, User): name = sender.first_name or "User"
                    elif sender: name = getattr(sender, 'title', 'Chat')
                    rm.add_log(self.uid, f"{name}: {e.text or '[Media]'}")
            except: pass

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 📱 BOT
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router()
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminSubS(StatesGroup): USER=State(); DAYS=State()
class AdminPromoS(StatesGroup): DAYS=State(); ACTS=State()

# --- KEYBOARDS ---
def kb_main(uid):
    rows = [
        [InlineKeyboardButton(text="📂 Открыть Отчеты", callback_data="open_reports")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🔑 Вход в аккаунт", callback_data="auth_menu")]
    ]
    if uid == cfg.ADMIN_ID: rows.append([InlineKeyboardButton(text="👑 Админ Панель", callback_data="adm_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_reports_reply(mode=None):
    t_d = "📦 Дроп (СТОП)" if mode == 'drop' else "📦 Дроп (СТАРТ)"
    t_i = "💻 Айти (СТОП)" if mode == 'it' else "💻 Айти (СТАРТ)"
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=t_d), KeyboardButton(text=t_i)],[KeyboardButton(text="🔙 Главное Меню")]], resize_keyboard=True, persistent=True)

# --- START ---
@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    await db.upsert_user(uid, m.from_user.username or "User")
    
    try:
        mem = await bot.get_chat_member(cfg.SUB_CHANNEL, uid)
        if mem.status in ['left', 'kicked'] and uid != cfg.ADMIN_ID:
            return await m.answer(f"⛔️ <b>Нет подписки!</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Подписаться", url=f"https://t.me/{cfg.SUB_CHANNEL.replace('@','')}")],[InlineKeyboardButton(text="✅ Проверить", callback_data="chk_s")]]))
    except: pass
    await m.answer(f"👋 <b>StatPro v63.0</b>\nID: <code>{uid}</code>", reply_markup=kb_main(uid))

@router.callback_query(F.data == "chk_s")
async def chk_s(c: CallbackQuery, state: FSMContext): await c.message.delete(); await start(c.message, state)

# --- PROFILE ---
@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    uid = c.from_user.id
    active = await db.check_sub_bool(uid)
    sub_date = "Активна" if active else "Истекла"
    await c.message.edit_text(f"👤 <b>Профиль</b>\n💎 Подписка: {sub_date}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Промокод", callback_data="promo")],[InlineKeyboardButton(text="🔙 Назад", callback_data="chk_s")]]))

@router.callback_query(F.data == "promo")
async def promo_1(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("🎟 <b>Код:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="profile")]]))
    await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def promo_2(m: Message, state: FSMContext):
    d = await db.use_promo(m.from_user.id, m.text)
    if d: await m.answer(f"✅ +{d} дней.")
    else: await m.answer("❌ Ошибка.")
    await state.clear(); await start(m, state)

# --- REPORTS ---
@router.callback_query(F.data == "open_reports")
async def open_rep(c: CallbackQuery):
    uid = c.from_user.id
    if not await db.check_sub_bool(uid): return await c.answer("⛔️ Нет подписки", True)
    if uid not in W_POOL:
        w = Worker(uid)
        if await w.start(): W_POOL[uid] = w
        else: return await c.answer("⚠️ Вы не вошли в аккаунт", True)
    await c.message.delete()
    await c.message.answer("📂 Меню", reply_markup=kb_reports_reply(rm.get_mode(uid)))

@router.message(F.text.startswith("📦 Дроп"))
async def rep_drop(m: Message):
    if not await db.check_sub_bool(m.from_user.id): return
    if not can_toggle(m.from_user.id): return
    
    uid = m.from_user.id
    if rm.get_mode(uid) == 'drop':
        d = rm.stop_session(uid)
        if d and d['logs']:
            # Оптимизированная отдача файла без лишних IO
            await m.answer_document(BufferedInputFile("\n".join(d['logs']).encode(), "drop.txt"), caption="✅", reply_markup=kb_reports_reply(None))
        else: await m.answer("⚠️ Пусто", reply_markup=kb_reports_reply(None))
    else:
        rm.stop_session(uid)
        rm.start_session(uid, 'drop'); await m.answer("🟢 REC", reply_markup=kb_reports_reply('drop'))

@router.message(F.text.startswith("💻 Айти"))
async def rep_it(m: Message):
    if not await db.check_sub_bool(m.from_user.id): return
    if not can_toggle(m.from_user.id): return

    uid = m.from_user.id
    if rm.get_mode(uid) == 'it':
        d = rm.stop_session(uid)
        if d and d['logs']:
            txt = "\n".join(d['logs'])
            if len(txt) > 4000: await m.answer_document(BufferedInputFile(txt.encode(), "it.txt"), caption="✅", reply_markup=kb_reports_reply(None))
            else: await m.answer(f"💻 REPORT:\n\n{txt}", reply_markup=kb_reports_reply(None))
        else: await m.answer("⚠️ Пусто", reply_markup=kb_reports_reply(None))
    else:
        rm.stop_session(uid)
        rm.start_session(uid, 'it'); await m.answer("🟢 ON", reply_markup=kb_reports_reply('it'))

@router.message(F.text == "🔙 Главное Меню")
async def back(m: Message, state: FSMContext):
    await m.answer("🏠", reply_markup=ReplyKeyboardRemove()); await start(m, state)

# --- AUTH (OPTIMIZED QR) ---
@router.callback_query(F.data == "auth_menu")
async def auth(c: CallbackQuery):
    await c.message.edit_text("🔑 Вход", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 QR", callback_data="l_qr")],[InlineKeyboardButton(text="📞 Tel", callback_data="l_ph")],[InlineKeyboardButton(text="🔙", callback_data="chk_s")]]))

@router.callback_query(F.data == "l_qr")
async def l_qr(c: CallbackQuery):
    uid = c.from_user.id
    client = TelegramClient(str(cfg.SESSION_DIR / f"session_{uid}"), cfg.API_ID, cfg.API_HASH)
    await client.connect()
    if await client.is_user_authorized(): await client.disconnect(); return await c.answer("✅ OK", True)
    
    qr = await client.qr_login()
    # Оптимизация памяти BytesIO
    import io
    img = qrcode.make(qr.url).convert("RGB")
    bio = io.BytesIO()
    img.save(bio, "PNG")
    # Передаем байты сразу
    await c.message.answer_photo(BufferedInputFile(bio.getvalue(), "qr.png"), caption="⏳ 500s")
    
    try: await qr.wait(500); await c.message.answer("✅ Success")
    except: await c.message.answer("❌ Timeout")
    finally: await client.disconnect()

@router.callback_query(F.data == "l_ph")
async def l_ph(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📞 Номер:"); await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def l_ph_s(m: Message, state: FSMContext):
    uid = m.from_user.id; cl = TelegramClient(str(cfg.SESSION_DIR / f"session_{uid}"), cfg.API_ID, cfg.API_HASH); await cl.connect()
    try:
        s = await cl.send_code_request(m.text)
        await state.update_data(p=m.text, h=s.phone_code_hash, s=str(cfg.SESSION_DIR / f"session_{uid}"))
        await cl.disconnect(); await m.answer("📩 Код:"); await state.set_state(AuthS.CO)
    except Exception as e: await m.answer(f"❌ {e}")

@router.message(AuthS.CO)
async def l_co_s(m: Message, state: FSMContext):
    d = await state.get_data()
    if 's' not in d: return await state.clear()
    cl = TelegramClient(d['s'], cfg.API_ID, cfg.API_HASH); await cl.connect()
    try: await cl.sign_in(phone=d['p'], code=m.text, phone_code_hash=d['h']); await m.answer("✅ OK"); await cl.disconnect(); await state.clear(); await start(m, state)
    except SessionPasswordNeededError: await m.answer("🔒 2FA:"); await cl.disconnect(); await state.set_state(AuthS.PA)
    except Exception as e: await cl.disconnect(); await m.answer(f"❌ {e}")

@router.message(AuthS.PA)
async def l_pa_s(m: Message, state: FSMContext):
    d = await state.get_data(); cl = TelegramClient(d['s'], cfg.API_ID, cfg.API_HASH); await cl.connect()
    try: await cl.sign_in(password=m.text); await m.answer("✅ OK")
    except Exception as e: await m.answer(f"❌ {e}")
    finally: await cl.disconnect(); await state.clear(); await start(m, state)

# --- ADMIN ---
@router.callback_query(F.data == "adm_menu")
async def adm(c: CallbackQuery):
    if c.from_user.id != cfg.ADMIN_ID: return
    await c.message.edit_text("👑 Admin", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Promo", callback_data="mk_p")],[InlineKeyboardButton(text="🎁 Give", callback_data="g_s")],[InlineKeyboardButton(text="🔙", callback_data="chk_s")]]))

@router.callback_query(F.data == "mk_p")
async def mk_p(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != cfg.ADMIN_ID: return
    await c.message.answer("Days?"); await state.set_state(AdminPromoS.DAYS)

@router.message(AdminPromoS.DAYS)
async def mk_p_d(m: Message, state: FSMContext):
    if m.from_user.id != cfg.ADMIN_ID: return
    await state.update_data(d=int(m.text)); await m.answer("Acts?"); await state.set_state(AdminPromoS.ACTS)

@router.message(AdminPromoS.ACTS)
async def mk_p_a(m: Message, state: FSMContext):
    if m.from_user.id != cfg.ADMIN_ID: return
    d = await state.get_data(); c = await db.create_promo(d['d'], int(m.text))
    await m.answer(f"Code: <code>{c}</code>"); await state.clear()

@router.callback_query(F.data == "g_s")
async def g_s(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != cfg.ADMIN_ID: return
    await c.message.answer("ID?"); await state.set_state(AdminSubS.USER)

@router.message(AdminSubS.USER)
async def g_s_u(m: Message, state: FSMContext):
    if m.from_user.id != cfg.ADMIN_ID: return
    await state.update_data(u=m.text); await m.answer("Days?"); await state.set_state(AdminSubS.DAYS)

@router.message(AdminSubS.DAYS)
async def g_s_d(m: Message, state: FSMContext):
    if m.from_user.id != cfg.ADMIN_ID: return
    d = await state.get_data(); await db.upsert_user(int(d['u']), "Adm"); await db.add_sub_days(int(d['u']), int(m.text))
    await m.answer("Done"); await state.clear()

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
    logger.info("🔥 StatPro v63.0 Started")
    
    try: await dp.start_polling(bot)
    finally:
        # Graceful Shutdown
        await rm.flush()
        if sys.platform != "win32": await db.get_conn().close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
