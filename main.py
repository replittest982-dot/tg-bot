#!/usr/bin/env python3
"""
💎 StatPro v50.0 - ULTIMATE EDITION
-----------------------------------
✅ FIX: Исправлен Redis Storage для Aiogram 3.x.
✅ SECURITY: Двойная проверка подписки (Канал + Личная лицензия).
✅ LOGIC: Блокировка функционала StatPro без промокода.
✅ CASINO: Оптимизированные алгоритмы и множители.
"""

import asyncio
import logging
import os
import sys
import random
import gc
from datetime import datetime, timedelta
from typing import Union, Optional

# --- AIOGRAM & LIBRARIES ---
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    ChatMemberUpdated
)
from aiogram.enums import ParseMode, DiceEmoji, ChatMemberStatus
from aiogram.filters import Command, CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# --- TELETHON ---
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ И НАСТРОЙКИ
# =========================================================================

# 📝 Заполните эти данные или используйте .env файл
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "123456789"))
API_ID = int(os.getenv("API_ID", "12345"))      # Для Telethon
API_HASH = os.getenv("API_HASH", "your_hash")   # Для Telethon

# 🔗 КАНАЛЫ (ОБЯЗАТЕЛЬНЫЕ ПОДПИСКИ)
CHANNELS = {
    "statpro": {"id": "@STAT_PRO1", "url": "https://t.me/STAT_PRO1", "name": "StatPro Channel"},
    "statlud": {"id": "@STATLUD", "url": "https://t.me/STATLUD", "name": "STATLUD Casino"}
}

# 🏦 ВАЛЮТА И КАЗИНО
CURRENCY_MAP = {'USDT': 'USDT ($)', 'ST': 'STATMON (ST)'}
STATMON_BONUS = 1000.0

# 🛠 НАСТРОЙКИ REDIS (Опционально)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger("StatPro_v50")

# Проверка токенов
if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
    logger.critical("❌ ВЫ НЕ УКАЗАЛИ BOT_TOKEN! Заполните переменные в начале файла.")
    sys.exit(1)

# =========================================================================
# 🗄️ БАЗА ДАННЫХ (AIOSQLITE)
# =========================================================================

class Database:
    def __init__(self, db_path="statpro_v50.db"):
        self.path = db_path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            # Таблица пользователей
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    joined_at TEXT,
                    sub_end_date TEXT, -- Дата окончания личной подписки
                    balance_usdt REAL DEFAULT 0.0,
                    balance_st REAL DEFAULT 0.0,
                    current_bet REAL DEFAULT 10.0,
                    selected_currency TEXT DEFAULT 'USDT',
                    bonus_received INTEGER DEFAULT 0
                )
            """)
            # Таблица промокодов
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY,
                    days INTEGER,
                    activations_left INTEGER
                )
            """)
            await db.commit()

    async def upsert_user(self, uid: int, uname: str):
        now = datetime.now().isoformat()
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT OR IGNORE INTO users (user_id, username, joined_at, sub_end_date)
                VALUES (?, ?, ?, ?)
            """, (uid, uname, now, now)) # По умолчанию подписка истекла (равна дате регистрации)
            # Обновляем юзернейм если изменился
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def get_user(self, uid: int):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as cursor:
                return await cursor.fetchone()

    # --- ПОДПИСКИ И ПРОМО ---
    async def check_personal_sub(self, uid: int) -> bool:
        """Проверяет, активна ли ЛИЧНАЯ подписка (по дате)."""
        if uid == ADMIN_ID: return True
        user = await self.get_user(uid)
        if not user or not user['sub_end_date']: return False
        try:
            end_date = datetime.fromisoformat(user['sub_end_date'])
            return end_date > datetime.now()
        except: return False

    async def activate_promo(self, uid: int, code: str) -> str:
        async with aiosqlite.connect(self.path) as db:
            # 1. Ищем промокод
            async with db.execute("SELECT days, activations_left FROM promos WHERE code = ?", (code,)) as cur:
                promo = await cur.fetchone()
            
            if not promo: return "not_found"
            days, acts = promo
            if acts <= 0: return "ended"

            # 2. Обновляем промокод (минус 1 активация)
            await db.execute("UPDATE promos SET activations_left = activations_left - 1 WHERE code = ?", (code,))

            # 3. Обновляем юзера (добавляем дни)
            user = await self.get_user(uid)
            current_end = datetime.fromisoformat(user['sub_end_date']) if user['sub_end_date'] else datetime.now()
            # Если подписка уже истекла, начинаем отсчет от 'сейчас', иначе добавляем к текущей дате
            start_point = max(datetime.now(), current_end)
            new_end = start_point + timedelta(days=days)
            
            await db.execute("UPDATE users SET sub_end_date = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()
            return f"success_{days}"

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
            # Находим получателя
            async with db.execute("SELECT user_id FROM users WHERE username = ? COLLATE NOCASE", (to_username,)) as cur:
                receiver = await cur.fetchone()
                if not receiver: return "user_not_found"
                to_uid = receiver[0]

            # Проверяем баланс отправителя
            async with db.execute(f"SELECT {col} FROM users WHERE user_id = ?", (from_uid,)) as cur:
                res = await cur.fetchone()
                if not res or res[0] < amount: return "no_balance"

            # АТОМАРНАЯ ТРАНЗАКЦИЯ
            try:
                await db.execute("BEGIN TRANSACTION")
                await db.execute(f"UPDATE users SET {col} = {col} - ? WHERE user_id = ?", (amount, from_uid))
                await db.execute(f"UPDATE users SET {col} = {col} + ? WHERE user_id = ?", (amount, to_uid))
                await db.commit()
                return "success"
            except Exception as e:
                await db.rollback()
                logger.error(f"Transfer Error: {e}")
                return "error"

    # Сеттеры
    async def set_bet_settings(self, uid: int, bet: float = None, currency: str = None):
        async with aiosqlite.connect(self.path) as db:
            if bet: await db.execute("UPDATE users SET current_bet = ? WHERE user_id = ?", (bet, uid))
            if currency: await db.execute("UPDATE users SET selected_currency = ? WHERE user_id = ?", (currency, uid))
            await db.commit()
            
    async def claim_bonus(self, uid: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("UPDATE users SET balance_st = balance_st + ?, bonus_received = 1 WHERE user_id = ?", (STATMON_BONUS, uid))
            await db.commit()

db = Database()

# =========================================================================
# 🧠 ИНИЦИАЛИЗАЦИЯ БОТА И FSM
# =========================================================================

# Попытка подключить Redis, иначе Memory
try:
    from aiogram.fsm.storage.redis import RedisStorage
    import redis.asyncio as redis
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    storage = RedisStorage(redis=redis_client)
    logger.info("✅ Redis Storage активирован.")
except (ImportError, OSError, ConnectionError):
    from aiogram.fsm.storage.memory import MemoryStorage
    storage = MemoryStorage()
    logger.warning("⚠️ Redis не доступен. Используется MemoryStorage (не перезагружайте бота во время выплат!).")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# FSM Состояния
class PromoState(StatesGroup):
    waiting_for_code = State()

class WithdrawState(StatesGroup):
    amount = State()
    username = State()

class AuthState(StatesGroup):
    phone = State()
    code = State()
    password = State()

# =========================================================================
# 🕹 КЛАВИАТУРЫ И UI
# =========================================================================

def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💻 StatPro User (Tools)", callback_data="mode_statpro")],
        [InlineKeyboardButton(text="🎰 STATLUD (Casino)", callback_data="mode_casino")]
    ])

def kb_sub_req(channel_key):
    data = CHANNELS[channel_key]
    cb = f"check_sub_{channel_key}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"➕ Подписаться на {data['name']}", url=data['url'])],
        [InlineKeyboardButton(text="✅ Я подписался", callback_data=cb)]
    ])

# Меню StatPro (Заблокированное - нет личной подписки)
def kb_statpro_locked():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔒 Воркер (Закрыто)", callback_data="dummy_lock"), 
         InlineKeyboardButton(text="🔒 Вход (Закрыто)", callback_data="dummy_lock")],
        [InlineKeyboardButton(text="🎟 Активировать Промокод", callback_data="m_promo"), 
         InlineKeyboardButton(text="👤 Профиль", callback_data="m_profile")],
        [InlineKeyboardButton(text="🔙 Главное меню", callback_data="start")]
    ])

# Меню StatPro (Полное)
def kb_statpro_full():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Запустить Воркера", callback_data="w_start"), 
         InlineKeyboardButton(text="🔴 Стоп", callback_data="w_stop")],
        [InlineKeyboardButton(text="🔑 Авторизация", callback_data="m_auth")],
        [InlineKeyboardButton(text="🎟 Продлить (Промо)", callback_data="m_promo"), 
         InlineKeyboardButton(text="👤 Профиль", callback_data="m_profile")],
        [InlineKeyboardButton(text="👨‍💻 Поддержка", url="https://t.me/suppor_tstatpro1bot")],
        [InlineKeyboardButton(text="🔙 Главное меню", callback_data="start")]
    ])

# Меню Казино
def kb_casino_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кубик (x1.38)", callback_data="game_dice"), 
         InlineKeyboardButton(text="🏀 Баскет (x1.13)", callback_data="game_basket")],
        [InlineKeyboardButton(text="🎰 Слоты (x7.5)", callback_data="game_slot"), 
         InlineKeyboardButton(text="🎯 Дартс (x0.75)", callback_data="game_dart")],
        [InlineKeyboardButton(text="⚽️ Футбол (x1.13)", callback_data="game_foot"), 
         InlineKeyboardButton(text="🎳 Боулинг (x1.25)", callback_data="game_bowl")],
        [InlineKeyboardButton(text="💳 Баланс / Ставка", callback_data="c_balance")],
        [InlineKeyboardButton(text="🔙 Выход", callback_data="start")]
    ])

def kb_casino_balance(curr, usdt, st):
    switch_to = 'ST' if curr == 'USDT' else 'USDT'
    switch_txt = "🔄 На STATMON" if curr == 'USDT' else "🔄 На USDT"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{switch_txt}", callback_data=f"set_cur_{switch_to}")],
        [InlineKeyboardButton(text="💰 Изменить ставку", callback_data="set_bet_menu")],
        [InlineKeyboardButton(text="💸 Вывод средств", callback_data="c_withdraw")],
        [InlineKeyboardButton(text="🔙 В Казино", callback_data="mode_casino")]
    ])

def kb_bet_select():
    bets = [0.1, 1, 5, 10, 50, 100]
    rows = []
    row = []
    for b in bets:
        row.append(InlineKeyboardButton(text=f"{b}", callback_data=f"set_bet_{b}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="c_balance")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# =========================================================================
# 🛡 ЛОГИКА ПРОВЕРКИ ПОДПИСКИ
# =========================================================================

async def check_channel_subscription(user_id: int, channel_id: str) -> bool:
    if user_id == ADMIN_ID: return True
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
    except Exception as e:
        logger.error(f"Ошибка проверки подписки {channel_id}: {e}")
        return False

# =========================================================================
# 🎮 ХЕНДЛЕРЫ: СТАРТ И МЕНЮ
# =========================================================================

@router.message(CommandStart())
@router.callback_query(F.data == "start")
async def start_handler(u: Union[Message, CallbackQuery], state: FSMContext):
    await state.clear()
    uid = u.from_user.id
    uname = u.from_user.username or "User"
    await db.upsert_user(uid, uname)

    txt = f"👋 Привет, <b>{uname}</b>!\nДобро пожаловать в <b>StatPro v50.0</b>.\nВыберите режим работы:"
    
    if isinstance(u, Message):
        await u.answer(txt, reply_markup=kb_main())
    else:
        await u.message.edit_text(txt, reply_markup=kb_main())

# --- STATPRO USER LOGIC ---
@router.callback_query(F.data == "mode_statpro")
@router.callback_query(F.data == "check_sub_statpro")
async def mode_statpro(c: CallbackQuery):
    uid = c.from_user.id
    
    # 1. Проверка подписки на КАНАЛ
    if not await check_channel_subscription(uid, CHANNELS['statpro']['id']):
        return await c.message.edit_text(
            f"⛔️ <b>Доступ запрещен!</b>\nДля входа в StatPro Tools подпишитесь на канал.",
            reply_markup=kb_sub_req('statpro')
        )

    # 2. Проверка ЛИЧНОЙ ПОДПИСКИ (Promo)
    has_license = await db.check_personal_sub(uid)
    
    if not has_license:
        await c.message.edit_text(
            "💻 <b>StatPro User Panel</b>\n\n"
            "⚠️ <b>Лицензия не активна!</b>\n"
            "Функционал Воркера заблокирован. Пожалуйста, активируйте промокод в меню.",
            reply_markup=kb_statpro_locked()
        )
    else:
        user = await db.get_user(uid)
        end_date = datetime.fromisoformat(user['sub_end_date']).strftime("%d.%m.%Y %H:%M")
        await c.message.edit_text(
            f"💻 <b>StatPro User Panel</b>\n"
            f"✅ Лицензия активна до: <b>{end_date}</b>\n"
            f"Все системы в норме.",
            reply_markup=kb_statpro_full()
        )

@router.callback_query(F.data == "dummy_lock")
async def locked_alert(c: CallbackQuery):
    await c.answer("⛔️ Эта функция доступна только с активной подпиской!", show_alert=True)

# --- АКТИВАЦИЯ ПРОМО ---
@router.callback_query(F.data == "m_promo")
async def promo_start(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("🎟 <b>Активация подписки</b>\nВведите ваш промокод:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="mode_statpro")]]))
    await state.set_state(PromoState.waiting_for_code)

@router.message(PromoState.waiting_for_code)
async def promo_process(m: Message, state: FSMContext):
    code = m.text.strip()
    res = await db.activate_promo(m.from_user.id, code)
    
    if res.startswith("success"):
        days = res.split("_")[1]
        await m.answer(f"✅ <b>Успешно!</b>\nПодписка продлена на {days} дней.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="mode_statpro")]]))
        await state.clear()
    elif res == "ended":
        await m.answer("❌ Этот промокод закончился.")
    else:
        await m.answer("❌ Неверный промокод. Попробуйте еще раз.")

# --- STATLUD CASINO LOGIC ---
@router.callback_query(F.data == "mode_casino")
@router.callback_query(F.data == "check_sub_statlud")
async def mode_casino(c: CallbackQuery):
    uid = c.from_user.id
    
    # 1. Проверка подписки на КАНАЛ КАЗИНО
    if not await check_channel_subscription(uid, CHANNELS['statlud']['id']):
        return await c.message.edit_text(
            f"⛔️ <b>Доступ в Казино закрыт!</b>\nПодпишитесь на официальный канал STATLUD.",
            reply_markup=kb_sub_req('statlud')
        )
    
    # 2. Выдача бонуса новичкам
    user = await db.get_user(uid)
    if not user['bonus_received']:
        await db.claim_bonus(uid)
        await c.answer(f"🎉 Вам начислен бонус {STATMON_BONUS} STATMON!", show_alert=True)
        user = await db.get_user(uid) # Обновляем данные

    # 3. Меню
    u_bal = user['balance_usdt']
    s_bal = user['balance_st']
    bet = user['current_bet']
    cur = user['selected_currency']
    
    msg = (f"🎰 <b>STATLUD CASINO</b>\n"
           f"➖➖➖➖➖➖➖➖\n"
           f"💵 Баланс USDT: <b>{u_bal:.2f} $</b>\n"
           f"🌟 Баланс STATMON: <b>{s_bal:.2f} ST</b>\n"
           f"➖➖➖➖➖➖➖➖\n"
           f"💎 Ставка: <b>{bet} {cur}</b>")
    
    await c.message.edit_text(msg, reply_markup=kb_casino_main())

# =========================================================================
# 🎰 ИГРОВОЙ ДВИЖОК
# =========================================================================

GAMES_CONFIG = {
    "game_dice": {"emoji": DiceEmoji.DICE, "win_val": [6], "multi": 1.38},
    "game_basket": {"emoji": DiceEmoji.BASKETBALL, "win_val": [5], "multi": 1.13},
    "game_foot": {"emoji": DiceEmoji.FOOTBALL, "win_val": [5], "multi": 1.13},
    "game_bowl": {"emoji": DiceEmoji.BOWLING, "win_val": [6], "multi": 1.25}, # Страйк
    "game_dart": {"emoji": DiceEmoji.DARTS, "win_val": [6], "multi": 0.75},   # Даже при победе минус 25% (особенность)
}

@router.callback_query(F.data.startswith("game_"))
async def process_game(c: CallbackQuery):
    game_key = c.data
    if game_key == "game_slot": return await process_slot(c) # Слот отдельно
    
    cfg = GAMES_CONFIG.get(game_key)
    if not cfg: return
    
    uid = c.from_user.id
    user = await db.get_user(uid)
    
    bet = user['current_bet']
    cur = user['selected_currency']
    balance = user['balance_usdt'] if cur == 'USDT' else user['balance_st']
    
    if balance < bet:
        return await c.answer(f"❌ Недостаточно средств ({cur})!", show_alert=True)
    
    # Списание
    await db.update_balance(uid, -bet, cur)
    
    # Анимация
    msg = await c.message.answer_dice(emoji=cfg['emoji'])
    await asyncio.sleep(3.5)
    
    dice_val = msg.dice.value
    win_amt = 0.0
    
    if dice_val in cfg['win_val']:
        win_amt = bet * cfg['multi']
        await db.update_balance(uid, win_amt, cur)
        res_txt = f"🎉 <b>ПОБЕДА!</b> (+{win_amt:.2f} {cur})"
    else:
        res_txt = f"😔 <b>Мимо</b> (-{bet:.2f} {cur})"
        
    kb_replay = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Еще раз", callback_data=game_key)],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="mode_casino")]
    ])
    
    await c.message.answer(res_txt, reply_markup=kb_replay)
    try: await c.message.delete()
    except: pass

async def process_slot(c: CallbackQuery):
    uid = c.from_user.id
    user = await db.get_user(uid)
    bet = user['current_bet']; cur = user['selected_currency']
    bal = user['balance_usdt'] if cur == 'USDT' else user['balance_st']
    
    if bal < bet: return await c.answer("❌ Мало денег!", show_alert=True)
    
    await db.update_balance(uid, -bet, cur)
    msg = await c.message.answer_dice(emoji=DiceEmoji.SLOT_MACHINE)
    await asyncio.sleep(2.5)
    
    val = msg.dice.value
    # 64 - Три семерки (Джекпот), 43 - Лимоны/Вишни (примерно)
    win = 0.0
    if val == 64: 
        win = bet * 7.5
        txt = f"🎰 <b>JACKPOT!</b> (+{win:.2f} {cur})"
    elif val == 43:
        win = bet * 0.5 # Мини выигрыш
        txt = f"🍒 <b>Мини-Вин</b> (+{win:.2f} {cur})"
    else:
        txt = f"📉 <b>Пусто</b> (-{bet:.2f} {cur})"
        
    if win > 0: await db.update_balance(uid, win, cur)
    
    await c.message.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Еще раз", callback_data="game_slot")], [InlineKeyboardButton(text="🔙 Меню", callback_data="mode_casino")]]))
    try: await c.message.delete()
    except: pass

# =========================================================================
# 💰 УПРАВЛЕНИЕ ФИНАНСАМИ И ВЫВОД
# =========================================================================

@router.callback_query(F.data == "c_balance")
async def show_balance_menu(c: CallbackQuery):
    user = await db.get_user(c.from_user.id)
    await c.message.edit_text(
        f"💳 <b>Ваш Кошелек</b>\n\nUSDT: {user['balance_usdt']:.2f}\nSTATMON: {user['balance_st']:.2f}\n\n"
        f"Активная валюта: <b>{user['selected_currency']}</b>",
        reply_markup=kb_casino_balance(user['selected_currency'], user['balance_usdt'], user['balance_st'])
    )

@router.callback_query(F.data.startswith("set_cur_"))
async def set_currency(c: CallbackQuery):
    new_cur = c.data.split("_")[2]
    await db.set_bet_settings(c.from_user.id, currency=new_cur)
    await show_balance_menu(c)

@router.callback_query(F.data == "set_bet_menu")
async def bet_menu(c: CallbackQuery):
    await c.message.edit_text("Выберите размер ставки:", reply_markup=kb_bet_select())

@router.callback_query(F.data.startswith("set_bet_"))
async def set_bet_val(c: CallbackQuery):
    val = float(c.data.split("_")[2])
    await db.set_bet_settings(c.from_user.id, bet=val)
    await c.answer(f"Ставка установлена: {val}")
    await show_balance_menu(c)

# --- ВЫВОД СРЕДСТВ ---
@router.callback_query(F.data == "c_withdraw")
async def withdraw_start(c: CallbackQuery, state: FSMContext):
    user = await db.get_user(c.from_user.id)
    cur = user['selected_currency']
    bal = user['balance_usdt'] if cur == 'USDT' else user['balance_st']
    
    if bal < 0.1: return await c.answer("❌ Минимум для вывода: 0.1", show_alert=True)
    
    await state.update_data(cur=cur)
    await c.message.edit_text(f"💸 <b>Вывод {cur}</b>\nДоступно: {bal:.2f}\n\nВведите сумму для вывода:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="c_balance")]]))
    await state.set_state(WithdrawState.amount)

@router.message(WithdrawState.amount)
async def withdraw_amount(m: Message, state: FSMContext):
    try:
        amt = float(m.text)
        if amt < 0.1: raise ValueError
    except: return await m.answer("❌ Введите корректное число (мин 0.1)")
    
    await state.update_data(amount=amt)
    await m.answer("👤 Введите <b>Username</b> получателя (например @user):")
    await state.set_state(WithdrawState.username)

@router.message(WithdrawState.username)
async def withdraw_exec(m: Message, state: FSMContext):
    data = await state.get_data()
    res = await db.transfer_money(m.from_user.id, m.text, data['amount'], data['cur'])
    
    if res == "success":
        await m.answer(f"✅ <b>Успешно!</b>\nПереведено {data['amount']} {data['cur']} пользователю {m.text}.")
    elif res == "no_balance":
        await m.answer("❌ Недостаточно средств.")
    elif res == "user_not_found":
        await m.answer("❌ Пользователь не найден в боте.")
    else:
        await m.answer("❌ Ошибка системы.")
    
    await state.clear()
    await m.answer("Меню:", reply_markup=kb_casino_main())

# =========================================================================
# 👑 АДМИН КОМАНДЫ
# =========================================================================

@router.message(Command("admin_promo"), F.from_user.id == ADMIN_ID)
async def create_promo_cmd(m: Message):
    # /admin_promo CODE DAYS ACTS
    try:
        _, code, days, acts = m.text.split()
        async with aiosqlite.connect(db.path) as conn:
            await conn.execute("INSERT OR REPLACE INTO promos VALUES (?, ?, ?)", (code, int(days), int(acts)))
            await conn.commit()
        await m.answer(f"✅ Промокод <code>{code}</code> создан!\nДней: {days}, Активаций: {acts}")
    except: await m.answer("Format: /admin_promo CODE DAYS ACTS")

# =========================================================================
# 🚀 ЗАПУСК
# =========================================================================

async def main():
    await db.init()
    logger.info("🤖 Бот StatPro v50.0 запускается...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
