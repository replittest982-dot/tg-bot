#!/usr/bin/env python3
"""
💎 StatPro v41.0 - DIAMOND EDITION
----------------------------------
📢 SUB: Обязательная подписка на канал для доступа.
💬 SUP: Вкладка поддержки возвращена.
🎰 CASINO: Полный модуль казино (Дайсы, Слоты, Баланс).
💻 CORE: Оптимизированный StatPro Worker.
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
from typing import Dict, Optional, Set, Union
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
from aiogram.enums import ParseMode, DiceEmoji, ChatMemberStatus
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# --- TELETHON ---
from telethon import TelegramClient, events, types
from telethon.errors import SessionPasswordNeededError, PhoneCodeExpiredError
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import User

import qrcode
from PIL import Image

# =========================================================================
# ⚙️ НАСТРОЙКИ (КОНФИГ)
# =========================================================================

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "diamond.db"
SESSION_DIR.mkdir(parents=True, exist_ok=True)

VERSION = "v41.0 DIAMOND"
MSK_TZ = timezone(timedelta(hours=3))

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', handlers=[logging.StreamHandler()])
logger = logging.getLogger("StatProDiamond")

# ЗАГРУЗКА ПЕРЕМЕННЫХ
try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    
    # НОВЫЕ ПЕРЕМЕННЫЕ
    # ID канала для обязательной подписки (например: "@mychannel" или "-100123456789")
    TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "") 
    TARGET_CHANNEL_URL = os.getenv("TARGET_CHANNEL_URL", "https://t.me/telegram")
    # Ссылка на поддержку (например: "https://t.me/my_support")
    SUPPORT_URL = os.getenv("SUPPORT_URL", "https://t.me/durov") 
except: sys.exit(1)

if not all([BOT_TOKEN, API_ID, API_HASH]): 
    logger.critical("❌ Заполните BOT_TOKEN, API_ID, API_HASH!")
    sys.exit(1)

# =========================================================================
# 🗄️ БАЗА ДАННЫХ (OPTIMIZED WAL)
# =========================================================================

class Database:
    __slots__ = ('path',)
    _instance = None
    def __new__(cls):
        if cls._instance is None: cls._instance = super(Database, cls).__new__(cls)
        return cls._instance
    def __init__(self): self.path = DB_PATH
    def get_conn(self): return aiosqlite.connect(self.path, timeout=30.0)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    sub_end TEXT, 
                    balance INTEGER DEFAULT 0,
                    current_bet INTEGER DEFAULT 10,
                    joined_at TEXT
                )
            """)
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, acts INTEGER)")
            await db.commit()

    async def upsert_user(self, uid: int, uname: str):
        async with self.get_conn() as db:
            await db.execute("""
                INSERT INTO users (user_id, username, sub_end, balance, current_bet, joined_at) 
                VALUES (?, ?, ?, 0, 10, ?)
                ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
            """, (uid, uname, datetime.now().isoformat(), datetime.now().isoformat()))
            await db.commit()

    # --- КАЗИНО ---
    async def get_balance(self, uid: int):
        async with self.get_conn() as db:
            async with db.execute("SELECT balance, current_bet FROM users WHERE user_id = ?", (uid,)) as c:
                row = await c.fetchone()
                return (row[0], row[1]) if row else (0, 10)

    async def update_balance(self, uid: int, amount: int):
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, uid))
            await db.commit()
            
    async def set_bet(self, uid: int, bet: int):
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET current_bet = ? WHERE user_id = ?", (bet, uid))
            await db.commit()

    # --- STATPRO ---
    async def check_sub(self, uid: int) -> bool:
        if uid == ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                row = await c.fetchone()
                if not row: return False
                try: return datetime.fromisoformat(row[0]) > datetime.now()
                except: return False

    async def add_sub(self, uid: int, days: int):
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
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"STAT-{random.randint(1000,9999)}-{days}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, acts FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                row = await c.fetchone()
                if not row or row[1] < 1: return 0
                days = row[0]
            await db.execute("UPDATE promos SET acts = acts - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE acts <= 0")
            await db.commit()
        await self.add_sub(uid, days)
        return days
    
    async def get_stats(self):
        async with self.get_conn() as db:
            async with db.execute("SELECT COUNT(*) FROM users") as c: t = (await c.fetchone())[0]
            now = datetime.now().isoformat()
            async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (now,)) as c: a = (await c.fetchone())[0]
        return t, a

db = Database()

# =========================================================================
# 🧠 WORKER (TELETHON CORE)
# =========================================================================
class Worker:
    __slots__ = ('uid', 'client', 'task', 'status')
    def __init__(self, uid: int):
        self.uid = uid; self.client = None; self.task = None; self.status = "⚪️ Инициализация"

    async def start(self):
        if not await db.check_sub(self.uid): self.status = "⛔️ Нет подписки"; return False
        if self.task and not self.task.done(): self.task.cancel()
        self.task = asyncio.create_task(self._run())
        return True

    async def stop(self):
        self.status = "🔴 Остановлен"
        if self.client: await self.client.disconnect()
        if self.task: self.task.cancel()

    async def _run(self):
        s_path = SESSION_DIR / f"session_{self.uid}"
        while True:
            try:
                gc.collect()
                if not s_path.with_suffix(".session").exists(): self.status = "🔴 Нет файла сессии"; return
                self.client = TelegramClient(str(s_path), API_ID, API_HASH)
                await self.client.connect()
                if not await self.client.is_user_authorized(): self.status = "🔴 Ошибка авторизации"; return
                self.status = "🟢 Активен"
                
                # --- COMMANDS ---
                @self.client.on(events.NewMessage(pattern=r'^\.scan(?:\s+(\d+|all))?'))
                async def sc(e): 
                    await e.delete(); arg=e.pattern_match.group(1); lim=1000000 if arg=='all' else int(arg or 100)
                    st=await e.respond(f"📊 Сканирую {lim}..."); data=[]
                    async for m in self.client.iter_messages(e.chat_id, limit=lim):
                        if m.sender and isinstance(m.sender, User): data.append([m.sender_id, m.sender.first_name or "", m.sender.username or ""])
                    f=io.StringIO(); csv.writer(f).writerows(data); f.seek(0)
                    await st.delete(); await bot.send_document(self.uid, BufferedInputFile(f.getvalue().encode(), "scan.csv"))

                @self.client.on(events.NewMessage(pattern=r'^\.(?:флуд|spam)\s+(.+)'))
                async def fl(e):
                    await e.delete(); raw=e.pattern_match.group(1).split(); c,d,t=10,0.1,[]
                    for x in raw: (c:=int(x)) if x.isdigit() else ((d:=float(x)) if x.replace('.','',1).isdigit() else t.append(x))
                    msg=" ".join(t); 
                    if msg: 
                        for _ in range(c): await self.client.send_message(e.chat_id, msg); await asyncio.sleep(max(d, 0.1))

                await self.client.run_until_disconnected()
            except Exception as e: self.status = f"⚠️ Ошибка: {str(e)[:15]}"; await asyncio.sleep(5)
            finally: 
                if self.client: await self.client.disconnect()

W_POOL: Dict[int, Worker] = {}

async def mng_w(uid, act):
    if act=='start': 
        if uid in W_POOL: await W_POOL[uid].stop()
        w=Worker(uid); W_POOL[uid]=w; return await w.start()
    elif act=='stop' and uid in W_POOL: await W_POOL[uid].stop(); del W_POOL[uid]

# =========================================================================
# 🤖 BOT UI & LOGIC
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdmS(StatesGroup): D=State(); A=State(); U=State(); UD=State()

# --- HELPERS ---
async def check_channel_sub(user_id: int) -> bool:
    """Проверка подписки на канал (если задан)"""
    if not TARGET_CHANNEL_ID: return True
    if user_id == ADMIN_ID: return True
    try:
        m = await bot.get_chat_member(chat_id=TARGET_CHANNEL_ID, user_id=user_id)
        return m.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
    except Exception as e:
        logger.warning(f"Ошибка проверки подписки: {e}")
        return True # Если бот не админ канала, пускаем, чтобы не ломать логику

# --- KEYBOARDS ---
def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💻 StatPro User", callback_data="mode_statpro")],
        [InlineKeyboardButton(text="🎰 JackWin Casino", callback_data="mode_casino")]
    ])

def kb_sub_check(mode_callback):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться", url=TARGET_CHANNEL_URL)],
        [InlineKeyboardButton(text="✅ Проверить", callback_data=mode_callback)]
    ])

def kb_statpro(uid, is_admin):
    k = [
        [InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="m_auth"), InlineKeyboardButton(text="⚙️ Воркер", callback_data="m_bot")],
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="m_pro"), InlineKeyboardButton(text="👤 Профиль", callback_data="m_p")]
    ]
    # ВОЗВРАЩЕНО: Кнопка поддержки
    k.append([InlineKeyboardButton(text="👨‍💻 Поддержка", url=SUPPORT_URL)])
    
    if is_admin: k.append([InlineKeyboardButton(text="👑 Админ", callback_data="m_adm")])
    k.append([InlineKeyboardButton(text="🔙 Главное меню", callback_data="start")])
    return InlineKeyboardMarkup(inline_keyboard=k)

def kb_casino():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кубик (x1.8)", callback_data="game_dice"), InlineKeyboardButton(text="🏀 Баскет (x2.0)", callback_data="game_basket")],
        [InlineKeyboardButton(text="🎰 Слоты (x10)", callback_data="game_slot"), InlineKeyboardButton(text="⚽️ Футбол (x1.8)", callback_data="game_foot")],
        [InlineKeyboardButton(text="🎳 Боулинг (x5)", callback_data="game_bowl"), InlineKeyboardButton(text="🎯 Дартс (x3)", callback_data="game_dart")],
        [InlineKeyboardButton(text="💰 Изменить ставку", callback_data="c_bet"), InlineKeyboardButton(text="👤 Баланс", callback_data="c_bal")],
        [InlineKeyboardButton(text="🔙 Выход", callback_data="start")]
    ])

def kb_bets():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="10", callback_data="set_10"), InlineKeyboardButton(text="50", callback_data="set_50"), InlineKeyboardButton(text="100", callback_data="set_100")],
        [InlineKeyboardButton(text="500", callback_data="set_500"), InlineKeyboardButton(text="1000", callback_data="set_1000")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="mode_casino")]
    ])

# --- HANDLERS ---

@router.message(Command("start"))
@router.callback_query(F.data=="start")
async def start(u: Union[Message, CallbackQuery], state: FSMContext):
    await state.clear()
    uid = u.from_user.id
    username = u.from_user.username or "User"
    await db.upsert_user(uid, username)
    
    msg_text = f"💎 <b>StatPro v41</b>\nПривет, {username}! Выберите режим:"
    
    if isinstance(u, Message): await u.answer(msg_text, reply_markup=kb_main())
    else: await u.message.edit_text(msg_text, reply_markup=kb_main())

# --- STATPRO MODE (WITH SUB CHECK) ---
@router.callback_query(F.data=="mode_statpro")
async def m_stat(c: CallbackQuery):
    # ПРОВЕРКА ПОДПИСКИ
    if not await check_channel_sub(c.from_user.id):
        return await c.message.edit_text("⛔️ <b>Доступ закрыт!</b>\nДля использования бота подпишитесь на наш канал.", reply_markup=kb_sub_check("mode_statpro"))
        
    await c.message.edit_text("💻 <b>StatPro Panel</b>\nУправление юзерботом и инструментами.", reply_markup=kb_statpro(c.from_user.id, c.from_user.id==ADMIN_ID))

@router.callback_query(F.data=="m_auth")
async def ma(c: CallbackQuery): await c.message.edit_text("Метод входа:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📱 Телефон",callback_data="a_ph")],[InlineKeyboardButton(text="🔙",callback_data="mode_statpro")]]))

# Auth Logic (Robust)
@router.callback_query(F.data=="a_ph")
async def aph(c: CallbackQuery, state: FSMContext): await c.message.edit_text("📱 Введите номер телефона (79...):"); await state.set_state(AuthS.PH)
@router.message(AuthS.PH)
async def aphs(m: Message, state: FSMContext): 
    try:
        cl=TelegramClient(str(SESSION_DIR/f"session_{m.from_user.id}"), API_ID, API_HASH); await cl.connect()
        r=await cl.send_code_request(m.text); await state.update_data(p=m.text,h=r.phone_code_hash,cl=cl); await m.answer("📩 Введите код из Telegram:"); await state.set_state(AuthS.CO)
    except Exception as e: await m.answer(f"❌ Ошибка: {e}"); await state.clear()
@router.message(AuthS.CO)
async def aco(m: Message, state: FSMContext): 
    d=await state.get_data(); cl=d.get('cl')
    try: await cl.sign_in(phone=d['p'],code=m.text,phone_code_hash=d['h']); await m.answer("✅ Успешно! Аккаунт подключен."); await cl.disconnect(); await state.clear()
    except Exception as e: 
        if cl: await cl.disconnect()
        await m.answer(f"❌ Ошибка входа: {e}"); await state.clear()

@router.callback_query(F.data=="m_bot")
async def mbot(c: CallbackQuery):
    w=W_POOL.get(c.from_user.id); s=w.status if w else "🔴 Отключен"
    await c.message.edit_text(f"🤖 Воркер статус: {s}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🟢 Запуск",callback_data="w_on"),InlineKeyboardButton(text="🔴 Стоп",callback_data="w_off")],[InlineKeyboardButton(text="🔙",callback_data="mode_statpro")]]))

@router.callback_query(F.data=="w_on")
async def won(c: CallbackQuery): await c.answer("⏳ Запуск..."); await mng_w(c.from_user.id,'start'); await mbot(c)
@router.callback_query(F.data=="w_off")
async def woff(c: CallbackQuery): await mng_w(c.from_user.id,'stop'); await mbot(c)

# --- CASINO MODE (WITH SUB CHECK) ---
@router.callback_query(F.data=="mode_casino")
async def m_cas(c: CallbackQuery):
    # ПРОВЕРКА ПОДПИСКИ
    if not await check_channel_sub(c.from_user.id):
        return await c.message.edit_text("⛔️ <b>Доступ закрыт!</b>\nПодпишитесь на канал для игры.", reply_markup=kb_sub_check("mode_casino"))

    bal, bet = await db.get_balance(c.from_user.id)
    await c.message.edit_text(f"🎰 <b>JackWin Casino</b>\n💰 Баланс: <b>{bal} $</b>\n💎 Ставка: <b>{bet} $</b>\n\nВыберите игру:", reply_markup=kb_casino())

@router.callback_query(F.data=="c_bal")
async def c_bal(c: CallbackQuery): await c.answer(f"Баланс: {(await db.get_balance(c.from_user.id))[0]} $", show_alert=True)
@router.callback_query(F.data=="c_bet")
async def c_bet(c: CallbackQuery): await c.message.edit_text("Выберите ставку:", reply_markup=kb_bets())
@router.callback_query(F.data.startswith("set_"))
async def set_b(c: CallbackQuery):
    bet = int(c.data.split("_")[1])
    await db.set_bet(c.from_user.id, bet)
    await c.answer(f"Ставка: {bet} $"); await m_cas(c)

# --- GAME ENGINE ---
async def play_game(c: CallbackQuery, emoji: str, multi: float, condition: callable):
    uid = c.from_user.id; bal, bet = await db.get_balance(uid)
    if bal < bet: return await c.answer("⛔️ Недостаточно средств! Пополните у админа.", show_alert=True)
    
    await db.update_balance(uid, -bet)
    msg = await c.message.answer_dice(emoji=emoji)
    await asyncio.sleep(3.5)
    
    val = msg.dice.value
    if condition(val):
        win = int(bet * multi)
        await db.update_balance(uid, win)
        txt = f"🎉 <b>ПОБЕДА!</b>\nРезультат: {val}\n+{win} $"
    else:
        txt = f"😔 <b>Проигрыш</b>\nРезультат: {val}\n-{bet} $"
        
    await c.message.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Еще раз", callback_data=c.data)], [InlineKeyboardButton(text="🔙 Меню", callback_data="mode_casino")]]))
    try: await c.message.delete()
    except: pass

@router.callback_query(F.data=="game_dice")
async def gd(c): await play_game(c, DiceEmoji.DICE, 1.8, lambda v: v > 3)
@router.callback_query(F.data=="game_basket")
async def gb(c): await play_game(c, DiceEmoji.BASKETBALL, 2.0, lambda v: v in [4, 5])
@router.callback_query(F.data=="game_foot")
async def gf(c): await play_game(c, DiceEmoji.FOOTBALL, 1.8, lambda v: v in [3, 4, 5])
@router.callback_query(F.data=="game_bowl")
async def gbo(c): await play_game(c, DiceEmoji.BOWLING, 5.0, lambda v: v == 6)
@router.callback_query(F.data=="game_dart")
async def gda(c): await play_game(c, DiceEmoji.DARTS, 3.0, lambda v: v == 6)
@router.callback_query(F.data=="game_slot")
async def gs(c):
    uid = c.from_user.id; bal, bet = await db.get_balance(uid)
    if bal < bet: return await c.answer("Мало денег!", True)
    await db.update_balance(uid, -bet)
    msg = await c.message.answer_dice(emoji=DiceEmoji.SLOT_MACHINE)
    await asyncio.sleep(2.5)
    v = msg.dice.value
    # 64=777, 43=lemons, 22=grapes, 1=bar
    win = 0
    if v == 64: win = int(bet * 10)
    elif v == 43: win = int(bet * 3)
    elif v == 22: win = int(bet * 2)
    elif v == 1: win = int(bet * 1.5)
    
    if win > 0:
        await db.update_balance(uid, win)
        t = f"🎰 <b>ДЖЕКПОТ!</b>\n+{win} $"
    else:
        t = f"😔 Пусто\n-{bet} $"
    await c.message.answer(t, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Еще раз", callback_data="game_slot")], [InlineKeyboardButton(text="🔙 Меню", callback_data="mode_casino")]]))

# --- PROMO & ADMIN ---
@router.callback_query(F.data=="m_pro")
async def mpro(c: CallbackQuery, state: FSMContext): await c.message.edit_text("🎟 Введите промокод:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙",callback_data="mode_statpro")]])); await state.set_state(PromoS.CODE)
@router.message(PromoS.CODE)
async def proc(m: Message, state: FSMContext): 
    d=await db.use_promo(m.from_user.id, m.text.strip())
    await m.answer(f"✅ Успешно! +{d} дней" if d else "❌ Неверный код"); await state.clear()

@router.callback_query(F.data=="m_adm")
async def madm(c: CallbackQuery): await c.message.edit_text("👑 Админ-панель", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Промо",callback_data="ad_p")],[InlineKeyboardButton(text="🔙",callback_data="mode_statpro")]]))
@router.callback_query(F.data=="ad_p")
async def adp(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Дней:"); await state.set_state(AdmS.D)
@router.message(AdmS.D)
async def adpd(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Активаций:"); await state.set_state(AdmS.A)
@router.message(AdmS.A)
async def adpa(m: Message, state: FSMContext): 
    d=await state.get_data(); code=await db.create_promo(d['d'], int(m.text))
    await m.answer(f"Код: <code>{code}</code>"); await state.clear()

@router.message(Command("add"))
async def add_money(m: Message):
    if m.from_user.id != ADMIN_ID: return
    try:
        _, uid, amt = m.text.split()
        await db.update_balance(int(uid), int(amt))
        await m.answer(f"✅ Баланс {uid}: {amt} $")
    except: await m.answer("/add ID SUM")

@router.message(Command("stats"))
async def st_cmd(m: Message):
    if m.from_user.id != ADMIN_ID: return
    t, a = await db.get_stats()
    await m.answer(f"Всего: {t}\nАктивных сабов: {a}")

async def main():
    await db.init()
    for f in SESSION_DIR.glob("*.session"): 
        if f.stat().st_size == 0: f.unlink()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
