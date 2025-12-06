#!/usr/bin/env python3
"""
💎 StatPro v53.0 - BET SELECTION EDITION
-----------------------------------------
✅ FIX: Реализован выбор исхода (Больше/Меньше, Попал/Мимо и т.д.) перед броском.
✅ LOGIC: Математика выплат по формуле Ставка * Коэфф.
✅ STORAGE: MemoryStorage (без Redis).
"""

import asyncio
import logging
import os
import sys
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
    "statlud": {"id": "@STATLUD", "url": "https://tme/STATLUD", "name": "STATLUD Casino"}
}

# 🏦 ВАЛЮТА
CURRENCY_MAP = {'USDT': 'USDT ₮', 'ST': 'Pumpkin 🎃'}
STATMON_BONUS = 3000.0

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger("StatPro_v53")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ
# =========================================================================

class Database:
    def __init__(self, db_path="statpro_v53.db"):
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

    # --- ФИНАНСЫ, ПРОМО и тп. (Осталось без изменений) ---
    async def update_balance(self, uid: int, amount: float, currency: str):
        col = 'balance_usdt' if currency == 'USDT' else 'balance_st'
        async with aiosqlite.connect(self.path) as db:
            await db.execute(f"UPDATE users SET {col} = {col} + ? WHERE user_id = ?", (amount, uid))
            await db.commit()

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
        # ... (логика промокода, оставлена для краткости)
        return "success_30"

db = Database()

# =========================================================================
# 🧠 ИНИЦИАЛИЗАЦИЯ И FSM
# =========================================================================

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

class PromoState(StatesGroup): waiting_for_code = State()
class WithdrawState(StatesGroup): amount = State(); username = State()

# =========================================================================
# 🎮 КОНФИГУРАЦИЯ ИГР (С ИСХОДАМИ)
# =========================================================================

GAMES_CONFIG = {
    "dice": {
        "name": "Кубики 🎲",
        "emoji": DiceEmoji.DICE, 
        "options": {
            # Ставка на "Больше 3"
            "more": {"text": "Больше 3 (x1.90)", "multi": 1.9, "win_val": [4, 5, 6]},
            # Ставка на "Меньше 4"
            "less": {"text": "Меньше 4 (x1.90)", "multi": 1.9, "win_val": [1, 2, 3]},
        }
    },
    "basket": {
        "name": "Баскетбол 🏀",
        "emoji": DiceEmoji.BASKETBALL,
        "options": {
            # Выигрыш при попадании (4 или 5)
            "hit": {"text": "Попал (x1.80)", "multi": 1.8, "win_val": [4, 5]},
            # Выигрыш при промахе (1, 2, 3, 6)
            "miss": {"text": "Мимо (x2.50)", "multi": 2.5, "win_val": [1, 2, 3, 6]},
        }
    },
    "foot": {
        "name": "Футбол ⚽",
        "emoji": DiceEmoji.FOOTBALL, 
        "options": {
            # Выигрыш при Голе (3, 4, 5)
            "goal": {"text": "Гол (x1.80)", "multi": 1.8, "win_val": [3, 4, 5]},
            # Выигрыш при Промахе/Сейве (1, 2, 6)
            "nogoal": {"text": "Не забил (x2.50)", "multi": 2.5, "win_val": [1, 2, 6]},
        }
    },
    "bowl": {
        "name": "Боулинг 🎳",
        "emoji": DiceEmoji.BOWLING, 
        "options": {
            # Выигрыш при Страйке
            "strike": {"text": "Страйк (x5.00)", "multi": 5.0, "win_val": [6]},
            # Выигрыш при любом другом результате (Spare, Мимо и т.д.)
            "nostrike": {"text": "Не страйк (x1.20)", "multi": 1.2, "win_val": [1, 2, 3, 4, 5]},
        }
    },
    "dart": {
        "name": "Дартс 🎯",
        "emoji": DiceEmoji.DART, 
        "options": {
            # Выигрыш при Центре (6)
            "bullseye": {"text": "Центр (x4.00)", "multi": 4.0, "win_val": [6]},
            # Выигрыш при попадании в кольцо
            "ring": {"text": "Кольцо (x1.50)", "multi": 1.5, "win_val": [4, 5]},
        }
    },
}

# =========================================================================
# 🕹 КЛАВИАТУРЫ И UI
# =========================================================================

def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💻 StatPro Tools", callback_data="mode_statpro")],
        [InlineKeyboardButton(text="🎰 JackWin Casino", callback_data="mode_casino")]
    ])

def kb_casino_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кубики", callback_data="game_dice"), 
         InlineKeyboardButton(text="🏀 Баскет", callback_data="game_basket")],
        [InlineKeyboardButton(text="🎰 Слоты (Джекпот)", callback_data="game_slot"), 
         InlineKeyboardButton(text="🎯 Дартс", callback_data="game_dart")],
        [InlineKeyboardButton(text="⚽️ Футбол", callback_data="game_foot"), 
         InlineKeyboardButton(text="🎳 Боулинг", callback_data="game_bowl")],
        [InlineKeyboardButton(text="📝 Изменить ставку", callback_data="set_bet_menu")],
        [InlineKeyboardButton(text="💰 Кошелек / Вывод", callback_data="c_balance")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="start")]
    ])

def kb_game_options(game_key):
    """Генерирует клавиатуру с исходами для выбранной игры."""
    cfg = GAMES_CONFIG[game_key]
    buttons = []
    for outcome_key, data in cfg['options'].items():
        # Формат callback: bet_<game_key>_<outcome_key>
        cb_data = f"bet_{game_key}_{outcome_key}"
        buttons.append(InlineKeyboardButton(text=data['text'], callback_data=cb_data))
    
    # Разбиваем на ряды по 2 кнопки
    rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    
    # Кнопка возврата
    rows.append([InlineKeyboardButton(text="🔙 В меню Казино", callback_data="mode_casino")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# =========================================================================
# 🎮 ИГРОВАЯ ЛОГИКА
# =========================================================================

@router.callback_query(F.data.startswith("game_"))
@router.callback_query(F.data.startswith("bet_"))
async def handle_game_or_bet(c: CallbackQuery):
    data = c.data.split('_')
    
    # 1. Если просто выбрали игру (game_<key>) -> Показываем исходы
    if data[0] == 'game':
        game_key = data[1]
        cfg = GAMES_CONFIG.get(game_key)
        
        if game_key == "slot": 
            # Для слотов отдельный хендлер, т.к. там нет выбора исхода
            return await play_slot(c) 
            
        if not cfg: return await c.answer("❌ Ошибка игры.", show_alert=True)
        
        user = await db.get_user(c.from_user.id)
        cur_sym = '₮' if user['selected_currency'] == 'USDT' else '🎃'
        
        msg = (f"🎯 <b>{cfg['name']}</b>\n"
               f"➖➖➖➖➖➖➖➖\n"
               f"Ваша ставка: <b>{user['current_bet']} {cur_sym}</b>\n"
               f"Выберите исход:")
               
        await c.message.edit_text(msg, reply_markup=kb_game_options(game_key))
        return

    # 2. Если выбрали исход (bet_<game_key>_<outcome_key>) -> Играем
    if data[0] == 'bet':
        game_key = data[1]
        outcome_key = data[2]
        
        cfg = GAMES_CONFIG.get(game_key)
        if not cfg: return await c.answer("❌ Ошибка игры.", show_alert=True)
        
        outcome = cfg['options'].get(outcome_key)
        if not outcome: return await c.answer("❌ Ошибка исхода.", show_alert=True)
        
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
        
        if val in outcome['win_val']:
            # Формула: Ставка * Коэфф
            win_amount = bet * outcome['multi']
            await db.update_balance(uid, win_amount, cur)
            res_text = (f"✅ <b>{outcome['text']}!</b>\n"
                        f"Результат: **{val}** (Выигрыш)\n"
                        f"Выигрыш: <b>+{win_amount:.2f} {sym}</b>")
        else:
            res_text = (f"❌ <b>Не угадали</b>\n"
                        f"Результат: **{val}** (Проигрыш)\n"
                        f"Потеряно: -{bet:.2f} {sym}")

        # Кнопки для повтора
        kb_again = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Повторить ставку", callback_data=c.data)],
            [InlineKeyboardButton(text="🔙 Изменить исход", callback_data=f"game_{game_key}")],
            [InlineKeyboardButton(text="🏡 В меню Казино", callback_data="mode_casino")]
        ])
        
        # Редактируем сообщение с выбором исхода (если оно еще не удалено)
        try: await c.message.edit_text(res_text, reply_markup=kb_again)
        except TelegramBadRequest: 
            await c.message.answer(res_text, reply_markup=kb_again)
            try: await c.message.delete()
            except: pass
        return

# --- SLOTS (Оставлен отдельно, т.к. нет выбора исхода) ---
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
    
# --- ОСТАЛЬНЫЕ ХЕНДЛЕРЫ (START, CASINO_MENU, BALANCE, ADMIN) ОСТАЛИСЬ КАК В V52.0 ---

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

@router.callback_query(F.data == "mode_casino")
async def casino_menu(c: CallbackQuery):
    uid = c.from_user.id
    if not await check_sub(uid, CHANNELS['statlud']['id']):
        return await c.message.edit_text(f"⛔️ Подпишись на {CHANNELS['statlud']['id']}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Я подписался", callback_data="mode_casino")]]))
    
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

@router.callback_query(F.data == "c_balance")
async def show_wallet(c: CallbackQuery):
    # Логика кошелька
    user = await db.get_user(c.from_user.id)
    cur = user['selected_currency']
    msg = (f"💳 <b>Ваш кошелек</b>\n\n"
           f"💵 USDT: {user['balance_usdt']:.2f} ₮\n"
           f"🎃 Тыквы: {user['balance_st']:.2f} (Demo)\n\n"
           f"Активная валюта: <b>{cur}</b>")
    
    switch_to = 'ST' if cur == 'USDT' else 'USDT'
    switch_txt = "🎃 Переключить на Тыквы" if cur == 'USDT' else "💵 Переключить на USDT"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=switch_txt, callback_data=f"set_cur_{switch_to}")],
        [InlineKeyboardButton(text="💸 Вывести средства", callback_data="c_withdraw")],
        [InlineKeyboardButton(text="🔙 В казино", callback_data="mode_casino")]
    ])
    await c.message.edit_text(msg, reply_markup=kb)

# --- FSM для вывода и промо ---

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

# ... (Остальные FSM, Админ-команды и хелперы) ...

@router.callback_query(F.data.startswith("set_cur_"))
async def change_currency(c: CallbackQuery):
    new = c.data.split("_")[2]
    await db.set_bet_settings(c.from_user.id, currency=new)
    await c.answer(f"Валюта: {new}")
    await show_wallet(c)

@router.callback_query(F.data == "set_bet_menu")
async def set_bet_ui(c: CallbackQuery):
    # Ставки как в видео (быстрый выбор)
    bets = [10, 50, 100, 250, 500, 750, 1000, 3000]
    rows = []; row = []
    for b in bets:
        row.append(InlineKeyboardButton(text=f"{b}", callback_data=f"set_bet_{b}"))
        if len(row) == 4: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="mode_casino")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await c.message.edit_text("🎯 Выберите сумму ставки:", reply_markup=kb)

@router.callback_query(F.data.startswith("set_bet_"))
async def set_bet_process(c: CallbackQuery):
    val = float(c.data.split("_")[2])
    await db.set_bet_settings(c.from_user.id, bet=val)
    await c.answer(f"Ставка изменена на {val}")
    await casino_menu(c)

async def check_sub(uid, cid):
    if uid == ADMIN_ID: return True
    try:
        m = await bot.get_chat_member(cid, uid)
        return m.status in ['member', 'administrator', 'creator']
    except: return False

async def main():
    await db.init()
    logger.info("🔥 StatPro v53.0 (Bet Selection) STARTED")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
