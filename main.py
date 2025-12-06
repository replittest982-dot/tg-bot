#!/usr/bin/env python3
"""
💎 StatPro v55.1 - FINAL INTEGRATION (Fixes & Optimization)
-------------------------------------------------------------
✅ FIX: Исправлены SyntaxError (if после ;)
✅ FIX: Добавлены недостающие функции (kb_casino_main, claim_bonus и др.)
✅ IMP: Улучшена структура БД и читаемость кода
"""

import asyncio
import logging
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Union, Optional

# --- ЗАВИСИМОСТИ ---
from dotenv import load_dotenv
import aiosqlite

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
)
from aiogram.enums import ParseMode, DiceEmoji
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# --- TELETHON ---
from telethon import TelegramClient, events

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ
# =========================================================================

# Загружаем переменные окружения
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "statpro_final.db"

SESSION_DIR.mkdir(parents=True, exist_ok=True)

# Получаем настройки или ставим заглушки, чтобы не падало сразу
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    print("❌ ОШИБКА: Не найден BOT_TOKEN в .env файле или переменныx окружения!")
    sys.exit(1)

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "change_me")

TREASURY_ID = 1
STATMON_BONUS = 3000.0

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger("StatPro_v55")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ
# =========================================================================

class Database:
    __slots__ = ('path',)
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.path = DB_PATH

    def get_conn(self):
        return aiosqlite.connect(self.path, timeout=30.0)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            
            # Таблица пользователей
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    username TEXT, 
                    sub_end TEXT, 
                    joined_at TEXT,
                    balance_usdt REAL DEFAULT 0.0, 
                    balance_st REAL DEFAULT 0.0,
                    current_bet REAL DEFAULT 10.0, 
                    selected_currency TEXT DEFAULT 'USDT',
                    bonus_received INTEGER DEFAULT 0
                )
            """)
            
            # Таблица промокодов
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations INTEGER)")
            
            # Таблица казны
            await db.execute("CREATE TABLE IF NOT EXISTS treasury (id INTEGER PRIMARY KEY, balance REAL DEFAULT 0.0)")
            await db.execute("INSERT OR IGNORE INTO treasury (id, balance) VALUES (?, 0.0)", (TREASURY_ID,))
            
            await db.commit()

    async def upsert_user(self, uid: int, uname: str):
        now = datetime.now().isoformat()
        async with self.get_conn() as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, ?, ?)",
                (uid, uname, now, now)
            )
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def get_user(self, uid: int):
        async with self.get_conn() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as cursor:
                return await cursor.fetchone()

    async def check_personal_sub(self, uid: int) -> bool:
        if uid == ADMIN_ID: return True
        user = await self.get_user(uid)
        if not user or not user['sub_end']: return False
        try:
            return datetime.fromisoformat(user['sub_end']) > datetime.now()
        except ValueError:
            return False

    # --- ФИНАНСЫ И КАЗНА ---
    async def claim_bonus(self, uid: int):
        """Выдача стартового бонуса (добавлено, так как отсутствовало)"""
        async with self.get_conn() as db:
            await db.execute(
                "UPDATE users SET balance_st = balance_st + ?, bonus_received = 1 WHERE user_id = ?", 
                (STATMON_BONUS, uid)
            )
            await db.commit()

    async def update_balance(self, uid: int, amount: float, currency: str):
        col = 'balance_usdt' if currency == 'USDT' else 'balance_st'
        async with self.get_conn() as db:
            await db.execute(f"UPDATE users SET {col} = {col} + ? WHERE user_id = ?", (amount, uid))
            
            # Логика Казны работает только для USDT
            if currency == 'USDT':
                if amount < 0:
                    # Игрок проиграл (баланс уменьшился) -> Казна растет
                    await db.execute("UPDATE treasury SET balance = balance + ?", (abs(amount),))
                elif amount > 0:
                    # Игрок выиграл -> Казна уменьшается
                    await db.execute("UPDATE treasury SET balance = balance - ?", (amount,))
            
            await db.commit()

    async def get_treasury_balance(self) -> float:
        async with self.get_conn() as db:
            async with db.execute("SELECT balance FROM treasury WHERE id = ?", (TREASURY_ID,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0.0

    async def update_treasury(self, amount: float):
        async with self.get_conn() as db:
            await db.execute("UPDATE treasury SET balance = balance + ?", (amount,))
            await db.commit()

    async def get_stats(self):
        async with self.get_conn() as db:
            async with db.execute("SELECT COUNT(*) FROM users") as c:
                total = (await c.fetchone())[0]
            
            now = datetime.now().isoformat()
            async with db.execute("SELECT COUNT(*) FROM users WHERE sub_end > ?", (now,)) as c:
                active = (await c.fetchone())[0]
        return total, active

    # --- ЛОГИКА ПРОМО ---
    async def update_sub(self, uid: int, days: int):
        u_date = datetime.now()
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                if r:
                    try:
                        # ИСПРАВЛЕННАЯ СТРОКА (SyntaxError fix)
                        curr = datetime.fromisoformat(r[0])
                        if curr > u_date:
                            u_date = curr
                    except (ValueError, TypeError):
                        pass
            
            new_end = u_date + timedelta(days=days)
            # Убедимся, что юзер существует, перед апдейтом
            await self.upsert_user(uid, "Unknown")
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"STAT-{random.randint(1000,9999)}-{days}D"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                row = await c.fetchone()
                # ИСПРАВЛЕННАЯ СТРОКА (SyntaxError fix)
                if not row or row[1] < 1:
                    return 0
                days = row[0]
            
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            await db.commit()
        
        await self.update_sub(uid, days)
        return days

db = Database()

# =========================================================================
# 🧠 ВОРКЕР (Worker)
# =========================================================================

class Worker:
    __slots__ = ('uid', 'client', 'task', 'status')
    
    def __init__(self, uid: int): 
        self.uid = uid
        self.client: Optional[TelegramClient] = None
        self.task: Optional[asyncio.Task] = None
        self.status = "⚪️ Загрузка..."
    
    async def start(self):
        if not await db.check_personal_sub(self.uid):
            self.status = "⛔️ Нет подписки"
            return False
        
        if self.task and not self.task.done():
            self.task.cancel()
        
        self.task = asyncio.create_task(self._run())
        return True

    async def stop(self):
        self.status = "🔴 Остановлен"
        if self.client:
            await self.client.disconnect()
        if self.task:
            self.task.cancel()

    async def _run(self):
        s_path = SESSION_DIR / f"session_{self.uid}"
        # Для Telethon нужно имя файла без расширения в конструктор, но проверка нужна
        
        try:
            # Проверка наличия файла сессии
            if not s_path.with_suffix(".session").exists():
                self.status = "🔴 Нет сессии (войдите заново)"
                return

            self.client = TelegramClient(str(s_path), API_ID, API_HASH, 
                                       connection_retries=None, auto_reconnect=True)
            
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                self.status = "🔴 Ошибка авторизации"
                return
            
            self.status = "🟢 В работе"
            self._bind_handlers()
            await self.client.run_until_disconnected()
            
        except Exception as e:
            logger.error(f"Worker Error {self.uid}: {e}")
            self.status = f"⚠️ Сбой: {str(e)[:15]}"
            await asyncio.sleep(5)
        finally:
            if self.client:
                await self.client.disconnect()

    def _bind_handlers(self):
        """Здесь подключаются хендлеры Telethon"""
        c = self.client
        
        @c.on(events.NewMessage(pattern=r'^\.ping$'))
        async def ping_handler(e):
            start = datetime.now()
            msg = await e.respond("🏓")
            end = datetime.now()
            ms = (end - start).microseconds / 1000
            await msg.edit(f"🏓 Пинг: {ms:.1f}ms")
            await asyncio.sleep(2)
            await msg.delete()
            await e.delete()

W_POOL: Dict[int, Worker] = {}

async def manage_worker(uid, action):
    if action == 'start': 
        if uid in W_POOL:
            await W_POOL[uid].stop()
        w = Worker(uid)
        W_POOL[uid] = w
        return await w.start()
    elif action == 'stop' and uid in W_POOL:
        await W_POOL[uid].stop()
        del W_POOL[uid]
    return True

# =========================================================================
# 🎮 КОНФИГУРАЦИЯ КАЗИНО
# =========================================================================

GAMES_CONFIG = {
    "dice": {"name": "Кубики 🎲", "emoji": DiceEmoji.DICE, 
        "options": {"more": {"text": "Больше 3 (x1.90)", "multi": 1.9, "win_val": [4, 5, 6]},
                    "less": {"text": "Меньше 4 (x1.90)", "multi": 1.9, "win_val": [1, 2, 3]}}},
    "basket": {"name": "Баскетбол 🏀", "emoji": DiceEmoji.BASKETBALL,
        "options": {"hit": {"text": "Попал (x1.80)", "multi": 1.8, "win_val": [4, 5]},
                    "miss": {"text": "Мимо (x2.50)", "multi": 2.5, "win_val": [1, 2, 3, 6]}}},
    "foot": {"name": "Футбол ⚽", "emoji": DiceEmoji.FOOTBALL, 
        "options": {"goal": {"text": "Гол (x1.80)", "multi": 1.8, "win_val": [3, 4, 5]},
                    "nogoal": {"text": "Не забил (x2.50)", "multi": 2.5, "win_val": [1, 2, 6]}}},
    "bowl": {"name": "Боулинг 🎳", "emoji": DiceEmoji.BOWLING, 
        "options": {"strike": {"text": "Страйк (x5.00)", "multi": 5.0, "win_val": [6]},
                    "nostrike": {"text": "Не страйк (x1.20)", "multi": 1.2, "win_val": [1, 2, 3, 4, 5]}}},
    "dart": {"name": "Дартс 🎯", "emoji": DiceEmoji.DART, 
        "options": {"bullseye": {"text": "Центр (x4.00)", "multi": 4.0, "win_val": [6]},
                    "ring": {"text": "Кольцо (x1.50)", "multi": 1.5, "win_val": [4, 5]}}},
}

# =========================================================================
# 🤖 BOT UI & HANDLERS
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# --- СОСТОЯНИЯ ---
class PromoS(StatesGroup): CODE = State()
class TreasuryS(StatesGroup): AMT = State()

# --- КЛАВИАТУРЫ ---
def kb_main(uid):
    is_admin = (uid == ADMIN_ID)
    rows = [
        [InlineKeyboardButton(text="💻 StatPro Tools", callback_data="mode_statpro")],
        [InlineKeyboardButton(text="🎰 StatLud Casino", callback_data="mode_casino")]
    ]
    if is_admin:
        rows.append([InlineKeyboardButton(text="👑 Админ / Казна", callback_data="m_adm")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_casino_main():
    """Главное меню казино (выбор игры)"""
    rows = []
    games = list(GAMES_CONFIG.items())
    # Группируем по 2 кнопки в ряд
    for i in range(0, len(games), 2):
        row = []
        key, cfg = games[i]
        row.append(InlineKeyboardButton(text=cfg['name'], callback_data=f"game_{key}"))
        if i + 1 < len(games):
            key2, cfg2 = games[i+1]
            row.append(InlineKeyboardButton(text=cfg2['name'], callback_data=f"game_{key2}"))
        rows.append(row)
    
    # Добавляем кнопку слотов отдельно (если нужна)
    rows.append([InlineKeyboardButton(text="🎰 Слоты (Demo)", callback_data="game_slot")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_game_options(game_key):
    """Меню выбора исхода для игры"""
    cfg = GAMES_CONFIG.get(game_key)
    rows = []
    for opt_key, opt_val in cfg['options'].items():
        rows.append([InlineKeyboardButton(text=opt_val['text'], callback_data=f"bet_{game_key}_{opt_key}")])
    rows.append([InlineKeyboardButton(text="🔙 К играм", callback_data="mode_casino")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# --- HANDLERS: START ---

@router.message(CommandStart())
@router.callback_query(F.data == "start")
async def start_handler(u: Union[Message, CallbackQuery], state: FSMContext):
    await state.clear()
    uid = u.from_user.id
    uname = u.from_user.username or "User"
    await db.upsert_user(uid, uname)
    
    msg = f"👋 <b>Привет, {uname}!</b>\nВыберите режим работы <b>STATPRO</b>:"
    
    if isinstance(u, Message):
        await u.answer(msg, reply_markup=kb_main(uid))
    else:
        await u.message.edit_text(msg, reply_markup=kb_main(uid))

# --- HANDLERS: CASINO ---

@router.callback_query(F.data == "mode_casino")
async def casino_menu_handler(c: CallbackQuery):
    user = await db.get_user(c.from_user.id)
    # Если бонуса не было - выдаем
    if not user['bonus_received']:
        await db.claim_bonus(c.from_user.id)
        user = await db.get_user(c.from_user.id)
        await c.answer("🎁 Вам начислен приветственный бонус: 3000 Тыкв!", show_alert=True)

    cur_sym = '₮' if user['selected_currency'] == 'USDT' else '🎃'
    
    txt = (f"🎰 <b>StatLud Casino</b>\n"
           f"💵 USDT: <b>{user['balance_usdt']:.2f} ₮</b>\n"
           f"🎃 Тыквы: <b>{user['balance_st']:.2f} 🎃</b>\n"
           f"➖➖➖➖➖➖➖➖\n"
           f"🎯 Ставка: <b>{user['current_bet']} {cur_sym}</b>")
    await c.message.edit_text(txt, reply_markup=kb_casino_main())

async def play_slot(c: CallbackQuery):
    """Заглушка для слотов"""
    await c.message.answer_dice(emoji=DiceEmoji.SLOT_MACHINE)
    await asyncio.sleep(2)
    await c.message.answer("🎰 Слоты в разработке (Demo mode).", 
                           reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="mode_casino")]]))

@router.callback_query(F.data.startswith("game_"))
@router.callback_query(F.data.startswith("bet_"))
async def handle_game_or_bet(c: CallbackQuery):
    data = c.data.split('_')
    game_key = data[1]
    
    # Обработка выбора игры
    if data[0] == 'game':
        if game_key == "slot":
            return await play_slot(c)
            
        cfg = GAMES_CONFIG.get(game_key)
        if not cfg:
            return await c.answer("⚠️ Игра не найдена", show_alert=True)
            
        user = await db.get_user(c.from_user.id)
        cur_sym = '₮' if user['selected_currency'] == 'USDT' else '🎃'
        msg = (f"🎯 <b>{cfg['name']}</b>\n" 
               f"Ставка: <b>{user['current_bet']} {cur_sym}</b>\n" 
               f"Выберите исход:")
        await c.message.edit_text(msg, reply_markup=kb_game_options(game_key))
        return

    # Обработка ставки
    if data[0] == 'bet':
        outcome_key = data[2]
        cfg = GAMES_CONFIG.get(game_key)
        outcome = cfg['options'].get(outcome_key)
        
        uid = c.from_user.id
        user = await db.get_user(uid)
        
        bet = user['current_bet']
        cur = user['selected_currency']
        sym = '₮' if cur == 'USDT' else '🎃'
        bal = user['balance_usdt'] if cur == 'USDT' else user['balance_st']
        
        if bal < bet:
            return await c.answer(f"❌ Не хватает баланса ({sym})!", show_alert=True)

        # Списываем ставку
        await db.update_balance(uid, -bet, cur)
        
        msg = await c.message.answer_dice(emoji=cfg['emoji'])
        await asyncio.sleep(4.0)
        
        val = msg.dice.value
        win_amount = 0.0
        
        if val in outcome['win_val']:
            win_amount = bet * outcome['multi']
            await db.update_balance(uid, win_amount, cur)
            res_text = (f"✅ <b>Победа!</b> ({outcome['text']})\n" 
                        f"Выигрыш: <b>+{win_amount:.2f} {sym}</b>")
        else:
            res_text = (f"❌ <b>Проигрыш</b>\n" 
                        f"Потеряно: -{bet:.2f} {sym}")

        kb_again = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Повторить ставку", callback_data=c.data)],
            [InlineKeyboardButton(text="🔙 Изменить исход", callback_data=f"game_{game_key}")],
            [InlineKeyboardButton(text="🏡 В меню Казино", callback_data="mode_casino")]
        ])
        
        try:
            await c.message.edit_text(res_text, reply_markup=kb_again)
        except TelegramBadRequest:
            await c.message.answer(res_text, reply_markup=kb_again)

# --- HANDLERS: ADMIN ---

@router.callback_query(F.data == "m_adm")
async def madm_handler(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return await c.answer("⛔️ Доступ запрещен.", show_alert=True)
        
    treasury = await db.get_treasury_balance()
    u_total, u_active = await db.get_stats()
    
    await c.message.edit_text(
        f"👑 <b>Админ-панель</b>\n"
        f"💰 **КАЗНА (USDT):** <b>{treasury:.2f} ₮</b>\n"
        f"📊 Юзеры: {u_total} (Активны: {u_active})\n"
        f"\nВыберите действие:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Выдать Баланс (Coming Soon)", callback_data="ad_g")],
            [InlineKeyboardButton(text="💵 Пополнить Казну (CryptoBot)", callback_data="ad_treasury_start")],
            [InlineKeyboardButton(text="🔄 Обновить Статус", callback_data="m_adm")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="start")]
        ])
    )

@router.callback_query(F.data == "ad_treasury_start")
async def treasury_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID: return
    await c.message.edit_text("💵 <b>Пополнение Казны USDT</b>\nВведите сумму (число):")
    await state.set_state(TreasuryS.AMT)

@router.message(TreasuryS.AMT)
async def treasury_process(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    try:
        amount = float(m.text)
        if amount <= 0: raise ValueError
    except ValueError:
        return await m.answer("❌ Введите корректную положительную сумму.")

    await db.update_treasury(amount)
    await state.clear()
    
    treasury = await db.get_treasury_balance()
    await m.answer(
        f"✅ **Казна пополнена на {amount:.2f} ₮.**\n"
        f"Текущий баланс Казны: {treasury:.2f} ₮",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В Админ-панель", callback_data="m_adm")]])
    )

# --- HANDLERS: STATPRO TOOLS ---

@router.callback_query(F.data == "mode_statpro")
async def mode_statpro_handler(c: CallbackQuery):
    await c.message.edit_text(
        "💻 <b>StatPro Tools</b>\nВоркер-функционал:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="m_auth")],
            [InlineKeyboardButton(text="⚙️ Статус Воркера", callback_data="m_bot")],
            [InlineKeyboardButton(text="🎟 Активировать Промо", callback_data="m_pro")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="start")]
        ])
    )

@router.callback_query(F.data == "m_auth")
async def ma(c: CallbackQuery):
    if not await db.check_personal_sub(c.from_user.id):
        return await c.answer("⛔️ Нужна подписка", True)
    
    await c.answer("⚠️ Вход временно отключен (нужна session-файл).", show_alert=True)
    await c.message.edit_text(
        "Загрузите session-файл в папку /sessions (в разработке)", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="mode_statpro")]])
    )

@router.callback_query(F.data == "m_bot")
async def mbot(c: CallbackQuery):
    if not await db.check_personal_sub(c.from_user.id):
        return await c.answer("⛔️ Нужна подписка", True)
    
    w = W_POOL.get(c.from_user.id)
    s = w.status if w else "🔴 Остановлен"
    
    await c.message.edit_text(
        f"🤖 Состояние: {s}", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🟢 Запуск", callback_data="w_on"), InlineKeyboardButton(text="🔴 Стоп", callback_data="w_off")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="mode_statpro")]
        ])
    )

@router.callback_query(F.data == "w_on")
async def won(c: CallbackQuery):
    await c.answer("⏳ Запускаю...")
    await manage_worker(c.from_user.id, 'start')
    await mbot(c)

@router.callback_query(F.data == "w_off")
async def woff(c: CallbackQuery):
    await manage_worker(c.from_user.id, 'stop')
    await mbot(c)

@router.callback_query(F.data == "m_pro")
async def mpro(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text(
        "🎟 Введите промокод:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="mode_statpro")]])
    )
    await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def proc(m: Message, state: FSMContext):
    d = await db.use_promo(m.from_user.id, m.text.strip())
    await m.answer(f"✅ Активировано: +{d} дней" if d else "❌ Неверный или использованный код.")
    await state.clear()

# --- ЗАПУСК ---

async def main():
    await db.init()
    logger.info("🔥 StatPro v55.1 (Fixed Integration) STARTED")
    
    # Удаляем вебхук и запускаем полинг
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped!")
