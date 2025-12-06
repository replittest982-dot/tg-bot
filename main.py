#!/usr/bin/env python3
"""
💎 StatPro v52.0 - JACKWIN CONFIG EDITION
-----------------------------------------
✅ LOGIC: Математика выплат как в БК (Ставка * Коэфф = Выигрыш).
✅ CONFIG: Иксы (множители) настроены по видео JackWin.
✅ STORAGE: MemoryStorage (работает без Redis).
"""

import asyncio
import logging
import os
import sys
import random
from datetime import datetime, timedelta
from typing import Union, Optional

# --- AIOGRAM & LIBRARIES ---
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    ChatMemberUpdated
)
from aiogram.enums import ParseMode, DiceEmoji, ChatMemberStatus
from aiogram.filters import Command, CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ
# =========================================================================

# 📝 ВСТАВЬ ТОКЕН И ID СЮДА:
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# 🔗 КАНАЛЫ
CHANNELS = {
    "statpro": {"id": "@STAT_PRO1", "url": "https://t.me/STAT_PRO1", "name": "StatPro Channel"},
    "statlud": {"id": "@STATLUD", "url": "https://t.me/STATLUD", "name": "STATLUD Casino"}
}

# 🏦 ВАЛЮТА
CURRENCY_MAP = {'USDT': 'USDT ₮', 'ST': 'Pumpkin 🎃'} # Как в видео (Тыквы)
STATMON_BONUS = 3000.0 # Как в видео (3000 бонуса)

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger("StatPro_v52")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ
# =========================================================================

class Database:
    def __init__(self, db_path="statpro_v52.db"):
        self.path = db_path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    joined_at TEXT,
                    sub_end_date TEXT,
                    balance_usdt REAL DEFAULT 0.0,
                    balance_st REAL DEFAULT 0.0,
                    current_bet REAL DEFAULT 10.0,
                    selected_currency TEXT DEFAULT 'USDT',
                    bonus_received INTEGER DEFAULT 0
                )
            """)
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations_left INTEGER)")
            await db.commit()

    async def upsert_user(self, uid: int, uname: str):
        now = datetime.now().isoformat()
        async with aiosqlite.connect(self.path) as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, joined_at, sub_end_date) VALUES (?, ?, ?, ?)", (uid, uname, now, now))
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def get_user(self, uid: int):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as cursor:
                return await cursor.fetchone()

    # --- ФИНАНСЫ ---
    async def update_balance(self, uid: int, amount: float, currency: str):
        col = 'balance_usdt' if currency == 'USDT' else 'balance_st'
        async with aiosqlite.connect(self.path) as db:
            await db.execute(f"UPDATE users SET {col} = {col} + ? WHERE user_id = ?", (amount, uid))
            await db.commit()

    async def transfer_money(self, from_uid: int, to_username: str, amount: float, currency: str) -> str:
        col = 'balance_usdt' if currency == 'USDT' else 'balance_st'
        to_username = to_username.replace("@", "").strip()
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("SELECT user_id FROM users WHERE username = ? COLLATE NOCASE", (to_username,)) as cur:
                receiver = await cur.fetchone()
                if not receiver: return "user_not_found"
                to_uid = receiver[0]
            async with db.execute(f"SELECT {col} FROM users WHERE user_id = ?", (from_uid,)) as cur:
                res = await cur.fetchone()
                if not res or res[0] < amount: return "no_balance"
            try:
                await db.execute("BEGIN TRANSACTION")
                await db.execute(f"UPDATE users SET {col} = {col} - ? WHERE user_id = ?", (amount, from_uid))
                await db.execute(f"UPDATE users SET {col} = {col} + ? WHERE user_id = ?", (amount, to_uid))
                await db.commit()
                return "success"
            except:
                await db.rollback(); return "error"

    async def set_bet_settings(self, uid: int, bet: float = None, currency: str = None):
        async with aiosqlite.connect(self.path) as db:
            if bet: await db.execute("UPDATE users SET current_bet = ? WHERE user_id = ?", (bet, uid))
            if currency: await db.execute("UPDATE users SET selected_currency = ? WHERE user_id = ?", (currency, uid))
            await db.commit()
            
    async def claim_bonus(self, uid: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("UPDATE users SET balance_st = balance_st + ?, bonus_received = 1 WHERE user_id = ?", (STATMON_BONUS, uid))
            await db.commit()
            
    async def check_personal_sub(self, uid: int) -> bool:
        if uid == ADMIN_ID: return True
        user = await self.get_user(uid)
        if not user or not user['sub_end_date']: return False
        try: return datetime.fromisoformat(user['sub_end_date']) > datetime.now()
        except: return False

    async def activate_promo(self, uid: int, code: str) -> str:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("SELECT days, activations_left FROM promos WHERE code = ?", (code,)) as cur:
                promo = await cur.fetchone()
            if not promo: return "not_found"
            if promo[1] <= 0: return "ended"
            await db.execute("UPDATE promos SET activations_left = activations_left - 1 WHERE code = ?", (code,))
            user = await self.get_user(uid)
            current_end = datetime.fromisoformat(user['sub_end_date']) if user['sub_end_date'] else datetime.now()
            new_end = max(datetime.now(), current_end) + timedelta(days=promo[0])
            await db.execute("UPDATE users SET sub_end_date = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()
            return f"success_{promo[0]}"

db = Database()

# =========================================================================
# 🧠 ИНИЦИАЛИЗАЦИЯ
# =========================================================================

storage = MemoryStorage() # Используем память (как просил)
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

class PromoState(StatesGroup): waiting_for_code = State()
class WithdrawState(StatesGroup): amount = State(); username = State()

# =========================================================================
# 🎮 КОНФИГУРАЦИЯ ИГР (КАК В ВИДЕО JACKWIN)
# =========================================================================

# В видео:
# Кубики (Больше/Меньше) - 1.9x
# Баскетбол (Гол) - 1.8x
# Боулинг (Страйк) - 5x
# Слоты - высокие иксы

GAMES_CONFIG = {
    "game_dice": {
        "name": "Кубики 🎲",
        "emoji": DiceEmoji.DICE, 
        "win_val": [4, 5, 6], # Выигрыш если 4, 5 или 6 (Ставка "Больше")
        "multi": 1.9  # Коэффициент 1.9 как в видео
    },
    "game_basket": {
        "name": "Баскетбол 🏀",
        "emoji": DiceEmoji.BASKETBALL, 
        "win_val": [4, 5], # Гол (Чистая сетка или от щита)
        "multi": 1.8  # Коэффициент 1.8 как в видео
    },
    "game_foot": {
        "name": "Футбол ⚽",
        "emoji": DiceEmoji.FOOTBALL, 
        "win_val": [3, 4, 5], # Гол
        "multi": 1.8 
    },
    "game_bowl": {
        "name": "Боулинг 🎳",
        "emoji": DiceEmoji.BOWLING, 
        "win_val": [6], # Только страйк
        "multi": 5.0 # Коэффициент 5.0 как в видео
    },
    "game_dart": {
        "name": "Дартс 🎯",
        "emoji": DiceEmoji.DART, 
        "win_val": [6], # Центр
        "multi": 4.0 # Сделал нормальный икс (вместо 0.75)
    },
}

# =========================================================================
# 🕹 ИНТЕРФЕЙС
# =========================================================================

def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💻 StatPro Tools", callback_data="mode_statpro")],
        [InlineKeyboardButton(text="🎰 JackWin Casino", callback_data="mode_casino")]
    ])

def kb_casino_main():
    # Клавиатура похожая на видео (плитками)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кубики (x1.9)", callback_data="game_dice"), 
         InlineKeyboardButton(text="🏀 Баскет (x1.8)", callback_data="game_basket")],
        [InlineKeyboardButton(text="🎰 Слоты (Джекпот)", callback_data="game_slot"), 
         InlineKeyboardButton(text="🎯 Дартс (x4.0)", callback_data="game_dart")],
        [InlineKeyboardButton(text="⚽️ Футбол (x1.8)", callback_data="game_foot"), 
         InlineKeyboardButton(text="🎳 Боулинг (x5.0)", callback_data="game_bowl")],
        [InlineKeyboardButton(text="📝 Изменить ставку", callback_data="set_bet_menu")],
        [InlineKeyboardButton(text="💰 Кошелек / Вывод", callback_data="c_balance")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="start")]
    ])

def kb_balance_actions(curr):
    switch_to = 'ST' if curr == 'USDT' else 'USDT'
    switch_txt = "🎃 Переключить на Тыквы" if curr == 'USDT' else "💵 Переключить на USDT"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=switch_txt, callback_data=f"set_cur_{switch_to}")],
        [InlineKeyboardButton(text="💸 Вывести средства", callback_data="c_withdraw")],
        [InlineKeyboardButton(text="🔙 В казино", callback_data="mode_casino")]
    ])

def kb_bets():
    # Ставки как в видео (быстрый выбор)
    bets = [10, 50, 100, 250, 500, 750, 1000, 3000]
    rows = []; row = []
    for b in bets:
        row.append(InlineKeyboardButton(text=f"{b}", callback_data=f"set_bet_{b}"))
        if len(row) == 4: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="mode_casino")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# =========================================================================
# 🎮 ЛОГИКА
# =========================================================================

@router.message(CommandStart())
@router.callback_query(F.data == "start")
async def start(u: Union[Message, CallbackQuery], state: FSMContext):
    await state.clear()
    uid = u.from_user.id
    uname = u.from_user.username or "User"
    await db.upsert_user(uid, uname)
    msg = f"👋 <b>Привет, {uname}!</b>\nВыбери режим:"
    if isinstance(u, Message): await u.answer(msg, reply_markup=kb_main())
    else: await u.message.edit_text(msg, reply_markup=kb_main())

# --- CASINO ---
@router.callback_query(F.data == "mode_casino")
async def casino_menu(c: CallbackQuery):
    uid = c.from_user.id
    # Проверка подписки (можно отключить если не нужно)
    if not await check_sub(uid, CHANNELS['statlud']['id']):
        return await c.message.edit_text(f"⛔️ Подпишись на {CHANNELS['statlud']['url']}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Я подписался", callback_data="mode_casino")]]))
    
    user = await db.get_user(uid)
    if not user['bonus_received']:
        await db.claim_bonus(uid)
        await c.answer(f"🎁 Получено {STATMON_BONUS} Тыкв (Demo)!", show_alert=True)
        user = await db.get_user(uid)

    cur_sym = '₮' if user['selected_currency'] == 'USDT' else '🎃'
    bal = user['balance_usdt'] if user['selected_currency'] == 'USDT' else user['balance_st']
    
    txt = (f"🎰 <b>JackWin Casino</b>\n"
           f"➖➖➖➖➖➖➖➖\n"
           f"💵 Баланс: <b>{bal:.2f} {cur_sym}</b>\n"
           f"🎯 Ставка: <b>{user['current_bet']} {cur_sym}</b>\n"
           f"➖➖➖➖➖➖➖➖")
    await c.message.edit_text(txt, reply_markup=kb_casino_main())

@router.callback_query(F.data.startswith("game_"))
async def play_game(c: CallbackQuery):
    game_key = c.data
    if game_key == "game_slot": return await play_slot(c)
    
    cfg = GAMES_CONFIG.get(game_key)
    if not cfg: return

    uid = c.from_user.id
    user = await db.get_user(uid)
    bet = user['current_bet']
    cur = user['selected_currency']
    bal = user['balance_usdt'] if cur == 'USDT' else user['balance_st']
    sym = '₮' if cur == 'USDT' else '🎃'

    if bal < bet: return await c.answer(f"❌ Не хватает {sym}!", show_alert=True)

    # 1. Списываем ставку
    await db.update_balance(uid, -bet, cur)
    
    # 2. Анимация
    msg = await c.message.answer_dice(emoji=cfg['emoji'])
    await asyncio.sleep(4.0)
    
    # 3. Проверка
    val = msg.dice.value
    win_amount = 0.0
    
    if val in cfg['win_val']:
        # Формула как в ставках: Ставка * Коэфф
        win_amount = bet * cfg['multi']
        await db.update_balance(uid, win_amount, cur)
        res_text = (f"✅ <b>Победа!</b>\n"
                    f"Коэффициент: x{cfg['multi']}\n"
                    f"Выигрыш: <b>+{win_amount:.2f} {sym}</b>")
    else:
        res_text = (f"❌ <b>Проигрыш</b>\n"
                    f"Потеряно: -{bet:.2f} {sym}")

    kb_again = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Играть снова", callback_data=game_key)],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="mode_casino")]
    ])
    await c.message.answer(res_text, reply_markup=kb_again)
    try: await c.message.delete()
    except: pass

async def play_slot(c: CallbackQuery):
    uid = c.from_user.id
    user = await db.get_user(uid)
    bet = user['current_bet']; cur = user['selected_currency']
    bal = user['balance_usdt'] if cur == 'USDT' else user['balance_st']
    sym = '₮' if cur == 'USDT' else '🎃'

    if bal < bet: return await c.answer(f"❌ Не хватает {sym}!", show_alert=True)

    await db.update_balance(uid, -bet, cur)
    msg = await c.message.answer_dice(emoji=DiceEmoji.SLOT_MACHINE)
    await asyncio.sleep(2.5)
    
    val = msg.dice.value
    win = 0.0
    if val == 64: # 777
        win = bet * 10.0; txt = f"🎰 <b>JACKPOT x10!</b>\n+{win:.2f} {sym}"
    elif val in [1, 22, 43]: # Виноград/Лимон/Бар
        win = bet * 2.0; txt = f"🍒 <b>Выигрыш x2.0</b>\n+{win:.2f} {sym}"
    else:
        txt = f"❌ <b>Проигрыш</b>\n-{bet:.2f} {sym}"
    
    if win > 0: await db.update_balance(uid, win, cur)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Спин", callback_data="game_slot")], [InlineKeyboardButton(text="🔙 Меню", callback_data="mode_casino")]])
    await c.message.answer(txt, reply_markup=kb)
    try: await c.message.delete()
    except: pass

# --- BETTINGS & BALANCE ---
@router.callback_query(F.data == "set_bet_menu")
async def set_bet_ui(c: CallbackQuery):
    await c.message.edit_text("🎯 Выберите сумму ставки:", reply_markup=kb_bets())

@router.callback_query(F.data.startswith("set_bet_"))
async def set_bet_process(c: CallbackQuery):
    val = float(c.data.split("_")[2])
    await db.set_bet_settings(c.from_user.id, bet=val)
    await c.answer(f"Ставка изменена на {val}")
    await casino_menu(c)

@router.callback_query(F.data == "c_balance")
async def show_wallet(c: CallbackQuery):
    user = await db.get_user(c.from_user.id)
    cur = user['selected_currency']
    msg = (f"💳 <b>Ваш кошелек</b>\n\n"
           f"💵 USDT: {user['balance_usdt']:.2f} ₮\n"
           f"🎃 Тыквы: {user['balance_st']:.2f} (Demo)\n\n"
           f"Активная валюта: <b>{cur}</b>")
    await c.message.edit_text(msg, reply_markup=kb_balance_actions(cur))

@router.callback_query(F.data.startswith("set_cur_"))
async def change_currency(c: CallbackQuery):
    new = c.data.split("_")[2]
    await db.set_bet_settings(c.from_user.id, currency=new)
    await c.answer(f"Валюта: {new}")
    await show_wallet(c)

# --- STATPRO TOOLS (Проверка подписки) ---
@router.callback_query(F.data == "mode_statpro")
async def statpro_menu(c: CallbackQuery):
    uid = c.from_user.id
    if not await check_sub(uid, CHANNELS['statpro']['id']):
        return await c.message.edit_text(f"⛔️ Подпишись на {CHANNELS['statpro']['url']}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Я подписался", callback_data="mode_statpro")]]))
    
    if not await db.check_personal_sub(uid):
         await c.message.edit_text("🔒 <b>Доступ закрыт</b>\nНужен промокод.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Ввести промокод", callback_data="m_promo")]]))
    else:
         await c.message.edit_text("💻 <b>StatPro Panel</b>\nЛицензия активна.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="start")]]))

@router.callback_query(F.data == "m_promo")
async def promo_input(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("Введите код:")
    await state.set_state(PromoState.waiting_for_code)

@router.message(PromoState.waiting_for_code)
async def promo_check(m: Message, state: FSMContext):
    res = await db.activate_promo(m.from_user.id, m.text.strip())
    if "success" in res: await m.answer("✅ Промокод активирован!", reply_markup=kb_main())
    else: await m.answer("❌ Ошибка кода.")
    await state.clear()

# --- HELPER ---
async def check_sub(uid, cid):
    if uid == ADMIN_ID: return True
    try:
        m = await bot.get_chat_member(cid, uid)
        return m.status in ['member', 'administrator', 'creator']
    except: return False

@router.message(Command("admin_promo"), F.from_user.id == ADMIN_ID)
async def adm_promo(m: Message):
    try:
        _, c, d, a = m.text.split()
        async with aiosqlite.connect(db.path) as con:
            await con.execute("INSERT OR REPLACE INTO promos VALUES (?,?,?)", (c, int(d), int(a))); await con.commit()
        await m.answer("✅ Done")
    except: pass

# --- START ---
async def main():
    await db.init()
    logger.info("🔥 StatPro v52.0 (JackWin Config) STARTED")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
