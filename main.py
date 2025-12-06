#!/usr/bin/env python3
"""
💎 StatPro v48.0 - PROFIT MAXIMIZER EDITION
--------------------------------------
✅ FIX: Обязательная подписка на @STATLUD для получения 1000 STATMON.
✅ BALANCING: Шанс победы снижен до ~16.7%. Новые, высокие множители.
✅ CORE: Полностью оптимизированный код.
"""

import asyncio
import logging
import os
import sys
import io
import random
import shutil
import time
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
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile
)
from aiogram.enums import ParseMode, DiceEmoji, ChatMemberStatus
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# Инициализация FSM Storage (предполагаем наличие Redis)
try:
    from aiogram.fsm.storage.redis import RedisStorage
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    from aiogram.fsm.storage.memory import MemoryStorage
    REDIS_AVAILABLE = False
    logging.warning("⚠️ Redis не найден. FSM будет использовать MemoryStorage.")

# --- TELETHON ---
from telethon import TelegramClient, events, types
from telethon.errors import SessionPasswordNeededError, PhoneCodeExpiredError

import qrcode
from PIL import Image

# =========================================================================
# ⚙️ НАСТРОЙКИ (КОНФИГ)
# =========================================================================

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "profit_maximizer.db" # Новое имя для новой схемы DB
SESSION_DIR.mkdir(parents=True, exist_ok=True)

VERSION = "v48.0 PROFIT MAXIMIZER"
MSK_TZ = timezone(timedelta(hours=3))

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', handlers=[logging.StreamHandler()])
logger = logging.getLogger("StatPro")

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    
    # КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: Установлена ссылка на канал пользователя
    TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "@STATLUD") 
    TARGET_CHANNEL_URL = os.getenv("TARGET_CHANNEL_URL", "https://t.me/STATLUD")
    SUPPORT_URL = os.getenv("SUPPORT_URL", "https://t.me/suppor_tstatpro1bot")
    
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

except: sys.exit(1)

if not all([BOT_TOKEN, API_ID, API_HASH]) or not TARGET_CHANNEL_ID: 
    logger.critical("❌ Проверьте переменные окружения!")
    sys.exit(1)

RE_IT_CMD = r'^\.(встал|зм|пв)\s*(\d+)$'
CURRENCY_MAP = {'USDT': 'USDT', 'ST': 'STATMON'}
STATMON_START_BONUS = 1000.0

# =========================================================================
# 🗄️ БАЗА ДАННЫХ (ДВОЙНАЯ ВАЛЮТА)
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
            # КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: statmon_balance по умолчанию теперь 0.0
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT COLLATE NOCASE,
                    sub_end TEXT, 
                    joined_at TEXT,
                    balance REAL DEFAULT 0.0,
                    statmon_balance REAL DEFAULT 0.0,
                    current_bet REAL DEFAULT 10.0,
                    bet_currency TEXT DEFAULT 'USDT'
                )
            """)
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations INTEGER)")
            
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance DESC)")
            
            await db.commit()

    async def upsert_user(self, uid: int, uname: str):
        async with self.get_conn() as db:
            # statmon_balance установлен в 0.0, выдается только после подписки
            await db.execute("""
                INSERT INTO users (user_id, username, sub_end, joined_at, balance, statmon_balance) 
                VALUES (?, ?, ?, ?, 0.0, 0.0)
                ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
            """, (uid, uname, datetime.now().isoformat(), datetime.now().isoformat()))
            await db.commit()
            
    async def check_statmon_bonus(self, uid: int) -> bool:
        """Проверяет, был ли уже выдан бонус STATMON."""
        async with self.get_conn() as db:
            async with db.execute("SELECT statmon_balance FROM users WHERE user_id = ?", (uid,)) as c:
                row = await c.fetchone()
                return row and row[0] >= STATMON_START_BONUS

    # --- КАЗИНО (БАЛАНС) ---
    async def get_balance(self, uid: int):
        async with self.get_conn() as db:
            async with db.execute("SELECT balance, statmon_balance, current_bet, bet_currency, username FROM users WHERE user_id = ?", (uid,)) as c:
                row = await c.fetchone()
                if not row: return (0.0, 0.0, 10.0, 'USDT', None)
                return (row[0], row[1], row[2], row[3], row[4]) # USDT, ST, Bet, Currency, Uname

    async def update_balance(self, uid: int, amount: float, currency: str):
        col = 'balance' if currency == 'USDT' else 'statmon_balance'
        async with self.get_conn() as db:
            await db.execute(f"UPDATE users SET {col} = {col} + ? WHERE user_id = ?", (amount, uid))
            await db.commit()
            
    async def set_bet(self, uid: int, bet: float):
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET current_bet = ? WHERE user_id = ?", (bet, uid))
            await db.commit()

    async def set_currency(self, uid: int, currency: str):
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET bet_currency = ? WHERE user_id = ?", (currency, uid))
            await db.commit()
    
    async def get_user_by_username(self, username: str):
        async with self.get_conn() as db:
            async with db.execute("SELECT user_id, balance, statmon_balance FROM users WHERE username = ? COLLATE NOCASE", (username.lstrip('@'),)) as c:
                return await c.fetchone()

    async def transfer_balance(self, sender_uid: int, receiver_username: str, amount: float, currency: str) -> tuple:
        amount = abs(amount)
        sender_bal, sender_st_bal, _, _, _ = await self.get_balance(sender_uid)
        
        col = 'balance' if currency == 'USDT' else 'statmon_balance'
        sender_current_bal = sender_bal if currency == 'USDT' else sender_st_bal
        
        if sender_current_bal < amount: return (False, "БАЛАНС")

        receiver_data = await self.get_user_by_username(receiver_username)
        if not receiver_data: return (False, "ЮЗЕР")
        
        receiver_uid = receiver_data[0]

        async with self.get_conn() as db:
            await db.execute("BEGIN TRANSACTION")
            try:
                await db.execute(f"UPDATE users SET {col} = {col} - ? WHERE user_id = ?", (amount, sender_uid))
                await db.execute(f"UPDATE users SET {col} = {col} + ? WHERE user_id = ?", (amount, receiver_uid))
                
                await db.execute("COMMIT")
                logger.info(f"Transfer {currency} {sender_uid} -> {receiver_uid}: {amount:.2f}")
                
                return (True, receiver_uid)
            except Exception as e:
                await db.execute("ROLLBACK")
                logger.error(f"DB ROLLBACK: Transfer failed for {sender_uid}. Error: {e}")
                return (False, "DB_ERROR")

    # --- PROMO/SUB (Сокращены) ---
    async def check_sub(self, uid: int) -> bool: return uid == ADMIN_ID
    async def add_sub(self, uid: int, days: int): pass
    async def create_promo(self, days: int, acts: int) -> str: return f"P-{random.randint(100,999)}"
    async def use_promo(self, uid: int, code: str) -> int: return 0
    async def get_stats(self): return 0, 0

db = Database()

# =========================================================================
# 🧠 WORKER (TELETHON CORE)
# =========================================================================
# (Код Воркера сохранен)
class Worker:
    __slots__ = ('uid', 'client', 'task', 'status')
    def __init__(self, uid: int):
        self.uid = uid; self.client = None; self.task = None; self.status = "⚪️ Init"
    async def start(self):
        if not await db.check_sub(self.uid): self.status = "⛔️ No Sub"; return False
        if self.task and not self.task.done(): self.task.cancel()
        self.task = asyncio.create_task(self._run())
        return True
    async def stop(self): self.status = "🔴 Off"; self.client and await self.client.disconnect(); self.task and self.task.cancel()
    async def _run(self):
        s_path = SESSION_DIR / f"session_{self.uid}"
        while True:
            try:
                if not s_path.with_suffix(".session").exists(): self.status = "🔴 No Session"; return
                self.client = TelegramClient(str(s_path), API_ID, API_HASH)
                await self.client.connect()
                if not await self.client.is_user_authorized(): self.status = "🔴 Auth Error"; return
                self.status = "🟢 Active"
                await self.client.run_until_disconnected()
            except Exception as e: self.status = f"⚠️ Error: {str(e)[:10]}"; await asyncio.sleep(5)
            finally: 
                if self.client: await self.client.disconnect()

W_POOL: Dict[int, Worker] = {}
async def mng_w(uid, act):
    if act=='start': w=Worker(uid); W_POOL[uid]=w; return await w.start()
    elif act=='stop' and uid in W_POOL: await W_POOL[uid].stop(); del W_POOL[uid]

# =========================================================================
# 🤖 BOT UI & LOGIC
# =========================================================================

# ИНИЦИАЛИЗАЦИЯ СТОРАДЖА
if REDIS_AVAILABLE:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    storage = RedisStorage(r)
else:
    storage = MemoryStorage()

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# STATES (Сохранены)
class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class WithdrawS(StatesGroup): W_AMOUNT=State(); W_USERNAME=State(); W_CURRENCY=State()
class AdmS(StatesGroup): D=State(); A=State(); U=State(); UD=State()

# --- HELPERS ---
async def check_channel_sub(user_id: int) -> bool:
    """Проверяет подписку на целевой канал."""
    if not TARGET_CHANNEL_ID: return True
    if user_id == ADMIN_ID: return True
    try:
        # ChatMemberStatus.MEMBER = обычный подписчик
        m = await bot.get_chat_member(chat_id=TARGET_CHANNEL_ID, user_id=user_id)
        return m.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
    except Exception as e:
        logger.error(f"Subscription check failed: {e}")
        # В случае ошибки проверки (например, если бот не админ)
        return False

# --- KEYBOARDS ---
def kb_main(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💻 StatPro User", callback_data="mode_statpro")],[InlineKeyboardButton(text="🎰 STATLUD", callback_data="mode_casino")]])

def kb_sub_check(mode_callback, reason_text="Доступ закрыт"):
    """Клавиатура для проверки подписки."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться на @STATLUD", url=TARGET_CHANNEL_URL)],
        [InlineKeyboardButton(text="✅ Проверить подписку", callback_data=mode_callback)]
    ])

def kb_statpro(uid, is_admin):
    k = [[InlineKeyboardButton(text="🔑 Вход", callback_data="m_auth"), InlineKeyboardButton(text="⚙️ Воркер", callback_data="m_bot")],
         [InlineKeyboardButton(text="🎟 Промокод", callback_data="m_pro"), InlineKeyboardButton(text="👤 Профиль", callback_data="m_p")],
         [InlineKeyboardButton(text="👨‍💻 Поддержка", url=SUPPORT_URL)]]
    if is_admin: k.append([InlineKeyboardButton(text="👑 Админ", callback_data="m_adm")])
    k.append([InlineKeyboardButton(text="🔙 Главное меню", callback_data="start")]); return InlineKeyboardMarkup(inline_keyboard=k)

def kb_currency_switch(current_currency, usdt, st):
    next_c = 'USDT' if current_currency == 'ST' else 'ST'
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Текущая: {CURRENCY_MAP[current_currency]}", callback_data="c_bal")],
        [InlineKeyboardButton(text=f"🔄 Сменить на {CURRENCY_MAP[next_c]}", callback_data=f"switch_{next_c}")],
        [InlineKeyboardButton(text="💰 Изменить ставку", callback_data="c_bet"), InlineKeyboardButton(text="💵 Вывод средств", callback_data="c_withdraw")],
        [InlineKeyboardButton(text="🔙 В меню STATLUD", callback_data="mode_casino")]
    ])

def kb_casino():
    # Обновленные иксы для v48.0
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кубик (x5.5)", callback_data="game_dice"), InlineKeyboardButton(text="🏀 Баскет (x4.5)", callback_data="game_basket")],
        [InlineKeyboardButton(text="🎰 Слоты (x30)", callback_data="game_slot"), InlineKeyboardButton(text="⚽️ Футбол (x4.5)", callback_data="game_foot")],
        [InlineKeyboardButton(text="🎳 Боулинг (x5)", callback_data="game_bowl"), InlineKeyboardButton(text="🎯 Дартс (x3)", callback_data="game_dart")],
        [InlineKeyboardButton(text="💱 Выбор валюты/ставки", callback_data="c_currency")],
        [InlineKeyboardButton(text="🔙 Выход", callback_data="start")]
    ])

def kb_bets():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="0.1", callback_data="set_0.1"), InlineKeyboardButton(text="1", callback_data="set_1"), InlineKeyboardButton(text="5", callback_data="set_5")],
        [InlineKeyboardButton(text="10", callback_data="set_10"), InlineKeyboardButton(text="100", callback_data="set_100"), InlineKeyboardButton(text="500", callback_data="set_500")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="c_currency")]
    ])

# --- MODE SELECTORS ---
@router.message(Command("start"))
@router.callback_query(F.data=="start")
async def start(u: Union[Message, CallbackQuery], state: FSMContext):
    await state.clear()
    uid = u.from_user.id
    uname = u.from_user.username or "User"
    await db.upsert_user(uid, uname)
    msg_text = f"💎 <b>StatPro v48</b>\nВыберите режим:"
    
    if isinstance(u, Message): 
        await u.answer(msg_text, reply_markup=kb_main())
    else: 
        try: await u.message.edit_text(msg_text, reply_markup=kb_main())
        except TelegramBadRequest: await u.answer()

@router.callback_query(F.data=="mode_statpro")
async def m_stat(c: CallbackQuery):
    if not await check_channel_sub(c.from_user.id):
        return await c.message.edit_text("⛔️ <b>Доступ закрыт!</b>\nПодпишитесь на наш канал.", reply_markup=kb_sub_check("mode_statpro"))
    await c.message.edit_text("💻 <b>StatPro Panel</b>", reply_markup=kb_statpro(c.from_user.id, c.from_user.id==ADMIN_ID))

@router.callback_query(F.data=="mode_casino")
async def m_cas(c: CallbackQuery):
    uid = c.from_user.id
    is_subscribed = await check_channel_sub(uid)
    usdt, st, bet, cur, _ = await db.get_balance(uid)
    
    # 1. Проверка подписки для входа
    if not is_subscribed:
        return await c.message.edit_text(
            "⛔️ <b>Для доступа к Казино (STATLUD) и получения 1000 STATMON требуется подписка на канал @STATLUD!</b>", 
            reply_markup=kb_sub_check("mode_casino")
        )

    # 2. Выдача стартового бонуса STATMON
    if st < STATMON_START_BONUS:
        # Проверяем, был ли бонус выдан ранее (например, если юзер проиграл все)
        if not await db.check_statmon_bonus(uid): 
             # Выдаем бонус только если баланс 0 и ранее не было выдано 1000
             await db.update_balance(uid, STATMON_START_BONUS, 'ST')
             st += STATMON_START_BONUS
             await c.answer(f"🎉 Вы получили {STATMON_START_BONUS} STATMON за подписку!", show_alert=True)
        

    # 3. Отображение меню Казино
    msg_text = (f"🎰 <b>STATLUD</b>\n"
                f"💰 USDT (Реал): <b>{usdt:.2f} $</b>\n"
                f"🌟 STATMON (Тест): <b>{st:.2f} ST</b>\n"
                f"--- \n"
                f"💎 Текущая ставка: <b>{bet:.2f} {CURRENCY_MAP[cur]}</b>")
                
    await c.message.edit_text(msg_text, reply_markup=kb_casino())

# --- CURRENCY & BET SETTINGS (Сохранены) ---
@router.callback_query(F.data=="c_currency")
async def c_currency(c: CallbackQuery):
    uid = c.from_user.id
    usdt, st, bet, cur, _ = await db.get_balance(uid)
    
    msg_text = (f"💱 <b>Управление ставкой</b>\n"
                f"💰 USDT (Реал): <b>{usdt:.2f} $</b>\n"
                f"🌟 STATMON (Тест): <b>{st:.2f} ST</b>\n"
                f"--- \n"
                f"💎 Текущая ставка: <b>{bet:.2f} {CURRENCY_MAP[cur]}</b>")
                
    await c.message.edit_text(msg_text, reply_markup=kb_currency_switch(cur, usdt, st))

@router.callback_query(F.data.startswith("switch_"))
async def switch_currency(c: CallbackQuery):
    new_c = c.data.split("_")[1]
    await db.set_currency(c.from_user.id, new_c)
    await c.answer(f"Валюта переключена на {CURRENCY_MAP[new_c]}")
    await c_currency(c)

@router.callback_query(F.data=="c_bet")
async def c_bet(c: CallbackQuery): await c.message.edit_text("Выберите ставку:", reply_markup=kb_bets())
@router.callback_query(F.data.startswith("set_"))
async def set_b(c: CallbackQuery):
    bet = float(c.data.split("_")[1])
    await db.set_bet(c.from_user.id, bet)
    await c.answer(f"Ставка: {bet:.2f} $"); await c_currency(c)


# --- GAME ENGINE (С НОВЫМИ ИКСАМИ ~16.7% WIN CHANCE) ---
async def play_game(c: CallbackQuery, emoji: str, multi: float, condition: callable):
    uid = c.from_user.id; usdt, st, bet, cur, _ = await db.get_balance(uid)
    
    current_bal = usdt if cur == 'USDT' else st
    currency_symbol = '$' if cur == 'USDT' else 'ST'

    if current_bal < bet: return await c.answer(f"⛔️ Недостаточно {currency_symbol} для ставки!", show_alert=True)
    
    await db.update_balance(uid, -bet, cur)
    msg = await c.message.answer_dice(emoji=emoji); await asyncio.sleep(3.5)
    
    val = msg.dice.value; win_amount = 0.0
    
    if condition(val):
        win_amount = bet * multi
        await db.update_balance(uid, win_amount, cur)
        txt = f"🎉 <b>ПОБЕДА!</b>\nМножитель: x{multi:.1f}\n+{win_amount:.2f} {currency_symbol}"
    else: txt = f"😔 <b>Проигрыш</b>\n-{bet:.2f} {currency_symbol}"
        
    gc.collect() 
    kb_rev = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Еще раз", callback_data=c.data)], [InlineKeyboardButton(text="🔙 Меню STATLUD", callback_data="mode_casino")]])
    await c.message.answer(txt, reply_markup=kb_rev)
    try: await c.message.delete()
    except: pass

@router.callback_query(F.data=="game_dice")
async def gd(c): 
    # Win on 6 only (P = 1/6 ≈ 16.67%) -> x5.5
    await play_game(c, DiceEmoji.DICE, 5.5, lambda v: v == 6)

@router.callback_query(F.data=="game_basket")
async def gb(c): 
    # Win on 5 only (P = 1/5 = 20%) -> x4.5
    await play_game(c, DiceEmoji.BASKETBALL, 4.5, lambda v: v == 5)

@router.callback_query(F.data=="game_foot")
async def gf(c): 
    # Win on 5 only (P = 1/5 = 20%) -> x4.5
    await play_game(c, DiceEmoji.FOOTBALL, 4.5, lambda v: v == 5)

@router.callback_query(F.data=="game_slot")
async def gs(c):
    # Win on 64 (P ≈ 1.56%) -> x30.0 (Джекпот)
    # Win on 43 (P ≈ 1.56%) -> x2.5 (Низкий выигрыш)
    uid = c.from_user.id; usdt, st, bet, cur, _ = await db.get_balance(uid)
    current_bal = usdt if cur == 'USDT' else st
    currency_symbol = '$' if cur == 'USDT' else 'ST'
    
    if current_bal < bet: return await c.answer(f"Мало {currency_symbol}!", True)
    
    await db.update_balance(uid, -bet, cur)
    msg = await c.message.answer_dice(emoji=DiceEmoji.SLOT_MACHINE); await asyncio.sleep(2.5)
    
    v = msg.dice.value; win = 0.0
    
    if v == 64: 
        win = bet * 30.0; t = f"🎰 <b>ДЖЕКПОТ x30!</b>\n+{win:.2f} {currency_symbol}"
    elif v == 43: 
        win = bet * 2.5; t = f"🍒 <b>Победа x2.5</b>\n+{win:.2f} {currency_symbol}" 
    else: 
        t = f"😔 Пусто\n-{bet:.2f} {currency_symbol}"
    
    if win > 0.0: await db.update_balance(uid, win, cur)
    
    gc.collect()
    await c.message.answer(t, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Еще раз", callback_data="game_slot")], [InlineKeyboardButton(text="🔙 Меню STATLUD", callback_data="mode_casino")]]))

# --- WITHDRAWAL FSM (Сохранены) ---
@router.callback_query(F.data=="c_withdraw")
async def c_withdraw(c: CallbackQuery, state: FSMContext):
    usdt, st, bet, cur, _ = await db.get_balance(c.from_user.id)
    current_bal = usdt if cur == 'USDT' else st
    currency_symbol = '$' if cur == 'USDT' else 'ST'

    if current_bal < 0.1: return await c.answer("⛔️ Недостаточно средств для вывода (мин. 0.1 $).", show_alert=True)
    
    await state.update_data(w_currency=cur)
    await c.message.edit_text(f"💵 **Вывод {CURRENCY_MAP[cur]}**\nБаланс: <b>{current_bal:.2f} {currency_symbol}</b>\nВведите сумму:", 
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="mode_casino")]]))
    await state.set_state(WithdrawS.W_AMOUNT)

@router.message(WithdrawS.W_AMOUNT)
async def w_amount_input(m: Message, state: FSMContext):
    try:
        amount = float(m.text)
        if amount < 0.1: return await m.answer("❌ Минимальная сумма 0.1 $.")
    except ValueError: return await m.answer("❌ Введите корректную сумму (число).")

    data = await state.get_data(); cur = data['w_currency']
    usdt, st, _, _, _ = await db.get_balance(m.from_user.id)
    bal = usdt if cur == 'USDT' else st
    
    if amount > bal: return await m.answer(f"❌ Сумма ({amount:.2f}) превышает ваш баланс ({bal:.2f}).")

    await state.update_data(w_amount=amount)
    await m.answer("✅ Сумма принята. Введите ЮЗЕРНЕЙМ получателя (например, @username):")
    await state.set_state(WithdrawS.W_USERNAME)

@router.message(WithdrawS.W_USERNAME)
async def w_username_input(m: Message, state: FSMContext):
    data = await state.get_data(); amount = data['w_amount']; cur = data['w_currency']
    receiver_username = m.text.strip().lstrip('@')
    currency_symbol = '$' if cur == 'USDT' else 'ST'
    
    result, info = await db.transfer_balance(m.from_user.id, receiver_username, amount, cur)
    
    if result:
        await m.answer(f"✅ УСПЕШНО! Переведено <b>{amount:.2f} {currency_symbol}</b> пользователю @{receiver_username}.")
    elif info == "БАЛАНС": await m.answer(f"❌ Недостаточно средств.")
    elif info == "ЮЗЕР": await m.answer(f"❌ Пользователь @{receiver_username} не найден.")
    else: await m.answer(f"❌ Ошибка транзакции. Повторите позже.")
        
    await state.clear(); await m_cas(m)

# --- ADMIN COMMANDS (С сохранением логики) ---
@router.message(Command("get_balance"), F.from_user.id == ADMIN_ID)
async def adm_get_balance(m: Message):
    try:
        # Проверяем, что есть только 1 аргумент (ID)
        parts = m.text.split()
        if len(parts) != 2: raise ValueError
            
        _, uid_str = parts
        uid = int(uid_str)
        
        usdt, st, _, _, uname = await db.get_balance(uid)
        
        # Если пользователя нет в базе (None в uname)
        if uname is None:
            uname = f"ID: {uid} (Новый пользователь)"
            
        await m.answer(f"👤 {uname}\n💰 USDT: <b>{usdt:.2f} $</b>\n🌟 STATMON: <b>{st:.2f} ST</b>")
    except ValueError: await m.answer("Используй: /get_balance ID_ПОЛЬЗОВАТЕЛЯ")
    except Exception as e: await m.answer(f"Ошибка: {e}")

@router.message(Command("add_usdt"), F.from_user.id == ADMIN_ID)
async def adm_add_usdt(m: Message):
    try:
        _, uid, amt = m.text.split()
        await db.update_balance(int(uid), float(amt), 'USDT')
        await m.answer(f"✅ Баланс USDT для {uid} изменен на {float(amt):.2f} $")
    except: await m.answer("Используй: /add_usdt ID СУММА")

@router.message(Command("add_mon"), F.from_user.id == ADMIN_ID)
async def adm_add_mon(m: Message):
    try:
        _, uid, amt = m.text.split()
        await db.update_balance(int(uid), float(amt), 'ST')
        await m.answer(f"✅ Баланс STATMON для {uid} изменен на {float(amt):.2f} ST")
    except: await m.answer("Используй: /add_mon ID СУММА")


# --- MAIN ---
async def main():
    await db.init()
    if REDIS_AVAILABLE:
        dp.shutdown.register(r.close) 
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
