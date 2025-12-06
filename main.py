#!/usr/bin/env python3
"""
💎 StatPro v57.0 - ULTIMATE EDITION
-----------------------------------
✅ UI: Раздельные профили (Casino/StatPro).
✅ UX: Промокод перенесен в раздел StatPro.
✅ ADMIN: Выдача баланса по ID.
✅ CORE: Авто-реконнект сессий (не вылетает после рестарта).
✅ STATS: Подсчет игр и побед в БД.
"""

import asyncio
import logging
import os
import sys
import io
import random
import json
import qrcode
import aiosqlite
from typing import Dict, Optional, Union
from pathlib import Path
from datetime import datetime, timedelta, timezone

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile, ChatMemberUpdated
)
from aiogram.enums import ParseMode, DiceEmoji, ChatMemberStatus
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, PhoneCodeExpiredError
from telethon.tl.types import User

# =========================================================================
# ⚙️ НАСТРОЙКИ
# =========================================================================

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "statpro_v57.db"

SESSION_DIR.mkdir(parents=True, exist_ok=True)

# Заполни своими данными или используй .env
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "YOUR_HASH_HERE")

# Каналы для подписки
REQUIRED_CHANNELS = ["@STAT_PRO1", "@STATLUD"]

MSK_TZ = timezone(timedelta(hours=3))
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("StatPro_v57")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ
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
            # Добавили статистику игр (games_played, games_won)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, username TEXT, sub_end TEXT, joined_at TEXT,
                    balance_usdt REAL DEFAULT 0.0, balance_st REAL DEFAULT 1000.0,
                    selected_currency TEXT DEFAULT 'ST',
                    games_played INTEGER DEFAULT 0, games_won INTEGER DEFAULT 0
                )
            """)
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations INTEGER)")
            try:
                # Миграция для старых БД (если обновляешься)
                await db.execute("ALTER TABLE users ADD COLUMN games_played INTEGER DEFAULT 0")
                await db.execute("ALTER TABLE users ADD COLUMN games_won INTEGER DEFAULT 0")
            except: pass
            await db.commit()

    async def upsert_user(self, uid: int, uname: str):
        now = datetime.now().isoformat()
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, ?, ?)", (uid, uname, now, now))
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def get_user(self, uid: int):
        async with self.get_conn() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as c: return await c.fetchone()

    async def update_balance(self, uid: int, amount: float, currency: str, is_win: bool = False, is_game: bool = False):
        col = 'balance_usdt' if currency == 'USDT' else 'balance_st'
        async with self.get_conn() as db:
            await db.execute(f"UPDATE users SET {col} = {col} + ? WHERE user_id = ?", (amount, uid))
            if is_game:
                await db.execute("UPDATE users SET games_played = games_played + 1 WHERE user_id = ?", (uid,))
                if is_win:
                    await db.execute("UPDATE users SET games_won = games_won + 1 WHERE user_id = ?", (uid,))
            await db.commit()

    async def set_currency(self, uid: int, currency: str):
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET selected_currency = ? WHERE user_id = ?", (currency, uid))
            await db.commit()

    # --- Подписки ---
    async def check_sub(self, uid: int) -> bool:
        if uid == ADMIN_ID: return True
        user = await self.get_user(uid)
        if not user or not user['sub_end']: return False
        try: return datetime.fromisoformat(user['sub_end']) > datetime.now()
        except: return False

    async def update_sub(self, uid: int, days: int):
        u_date = datetime.now()
        user = await self.get_user(uid)
        if user and user['sub_end']:
            try: 
                curr = datetime.fromisoformat(user['sub_end'])
                if curr > u_date: u_date = curr
            except: pass
        new_end = u_date + timedelta(days=days)
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                row = await c.fetchone()
                if not row or row[1] < 1: return 0
                days = row[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            await db.commit()
        await self.update_sub(uid, days)
        return days

db = Database()

# =========================================================================
# 🧠 ВОРКЕР (С Полной интеграцией команд)
# =========================================================================

class Worker:
    __slots__ = ('uid', 'client', 'task', 'status', 'react_map', 'ghost', 'raid_targets')
    def __init__(self, uid: int):
        self.uid = uid; self.client = None; self.task = None
        self.status = "⚪️ Загрузка..."
        self.react_map = {}; self.ghost = False; self.raid_targets = set()

    async def start(self):
        if not await db.check_sub(self.uid): self.status = "⛔️ Нет подписки"; return False
        if self.task: self.task.cancel()
        self.task = asyncio.create_task(self._run()); return True

    async def stop(self):
        self.status = "🔴 Остановлен"; 
        if self.client: await self.client.disconnect()
        if self.task: self.task.cancel()

    async def _run(self):
        s_path = SESSION_DIR / f"session_{self.uid}"
        try:
            # Важно: auto_reconnect=True держит сессию активной
            self.client = TelegramClient(str(s_path), API_ID, API_HASH, connection_retries=None, auto_reconnect=True)
            await self.client.connect()
            if not await self.client.is_user_authorized(): self.status = "🔴 Ошибка авторизации"; return
            self.status = "🟢 В работе"; self._bind(); await self.client.run_until_disconnected()
        except Exception as e: self.status = f"⚠️ Сбой: {e}"; await asyncio.sleep(5)
        finally: 
            if self.client: await self.client.disconnect()

    def _bind(self):
        """Здесь подключаем логику команд юзербота"""
        c = self.client
        
        @c.on(events.NewMessage(pattern=r'^\.ping$'))
        async def pg(e):
            start = datetime.now(); msg = await e.respond("🏓"); end = datetime.now()
            ms = (end - start).microseconds / 1000
            await msg.edit(f"🏓 <b>Pong!</b>\n📶 Ping: <code>{ms:.1f}ms</code>", parse_mode='html')

        @c.on(events.NewMessage(pattern=r'^\.scan(?:\s+(\d+|all))?$'))
        async def sc(e):
             # (Тут должна быть твоя логика скана из v38, сокращено для примера)
             await e.edit("🔎 Сканирую...") 

        # ... Сюда вставь остальные хендлеры из v38 (.флуд, .айти и т.д.) ...

W_POOL: Dict[int, Worker] = {}
async def mng_w(uid, act):
    if act=='start': 
        if uid in W_POOL: await W_POOL[uid].stop()
        w=Worker(uid); W_POOL[uid]=w; return await w.start()
    elif act=='stop' and uid in W_POOL: await W_POOL[uid].stop(); del W_POOL[uid]

# =========================================================================
# 🎮 КОНФИГУРАЦИЯ ИГР
# =========================================================================

CASINO_GAMES = {
    "dice_classic": {"name": "🎲 Больше/Меньше", "emoji": DiceEmoji.DICE},
    "bowl": {"name": "🎳 Боулинг", "emoji": DiceEmoji.BOWLING},
    "dart": {"name": "🎯 Дартс", "emoji": DiceEmoji.DART},
    "bask": {"name": "🏀 Баскетбол", "emoji": DiceEmoji.BASKETBALL},
    "foot": {"name": "⚽️ Футбол", "emoji": DiceEmoji.FOOTBALL},
    "slot": {"name": "🎰 Слоты", "emoji": DiceEmoji.SLOT_MACHINE},
}

# =========================================================================
# 🤖 BOT UI
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminBalS(StatesGroup): UID=State(); AMT=State(); CUR=State()
class CasinoS(StatesGroup): BET=State()

# --- Helpers ---
async def safe_edit(c: CallbackQuery, text: str, reply_markup=None):
    """Защита от ошибки TelegramBadRequest (когда текст не меняется)"""
    try: await c.message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest: await c.answer() # Просто игнорим, если ничего не изменилось

async def check_channel_sub(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    for channel in REQUIRED_CHANNELS:
        try:
            m = await bot.get_chat_member(chat_id=channel, user_id=user_id)
            if m.status in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]: return False
        except: pass
    return True

# --- KEYBOARDS ---
def kb_main(is_admin):
    rows = [
        [InlineKeyboardButton(text="🎰 CASINO", callback_data="m_casino"), InlineKeyboardButton(text="🤖 StatPro User", callback_data="m_statpro")],
        [InlineKeyboardButton(text="💬 Чат", url="https://t.me/STAT_PRO1")]
    ]
    if is_admin: rows.append([InlineKeyboardButton(text="👑 Админ Панель", callback_data="m_adm")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_statpro_menu(sub_active: bool):
    status_icon = "🟢" if sub_active else "🔴"
    rows = [
        [InlineKeyboardButton(text=f"{status_icon} Профиль StatPro", callback_data="p_statpro")],
        [InlineKeyboardButton(text="🎟 Активировать Промокод", callback_data="m_pro")],
        [InlineKeyboardButton(text="⚙️ Управление Воркером", callback_data="m_w_mng")],
        [InlineKeyboardButton(text="🔑 Авторизация (Сессия)", callback_data="m_auth")],
        [InlineKeyboardButton(text="🔙 Главное Меню", callback_data="menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_casino_menu():
    rows = [
        [InlineKeyboardButton(text="👤 Профиль Казино", callback_data="p_casino")],
        [InlineKeyboardButton(text="🎮 Игры", callback_data="cg_list"), InlineKeyboardButton(text="🏆 Топ (Скоро)", callback_data="ignore")],
        [InlineKeyboardButton(text="🔙 Главное Меню", callback_data="menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# --- START ---
@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    await db.upsert_user(uid, m.from_user.username or "User")
    
    if not await check_channel_sub(uid):
        return await m.answer("⛔️ <b>Доступ закрыт!</b>\nПодпишитесь на каналы:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Подписаться 1", url="https://t.me/STAT_PRO1")],
                [InlineKeyboardButton(text="Подписаться 2", url="https://t.me/STATLUD")],
                [InlineKeyboardButton(text="✅ Проверить", callback_data="check_sub")]
            ]))
    
    await m.answer("💎 <b>StatPro v57.0</b>\nВыберите раздел:", reply_markup=kb_main(uid==ADMIN_ID))

@router.callback_query(F.data == "check_sub")
async def chk_s(c: CallbackQuery, state: FSMContext):
    await c.message.delete()
    await start(c.message, state)

@router.callback_query(F.data == "menu")
async def menu_cb(c: CallbackQuery):
    await safe_edit(c, "💎 <b>Главное меню</b>", kb_main(c.from_user.id==ADMIN_ID))

# --- РАЗДЕЛ STATPRO ---
@router.callback_query(F.data == "m_statpro")
async def statpro_main(c: CallbackQuery):
    is_sub = await db.check_sub(c.from_user.id)
    await safe_edit(c, "🤖 <b>Раздел StatPro User</b>\nУправление вашим юзерботом.", kb_statpro_menu(is_sub))

@router.callback_query(F.data == "p_statpro")
async def profile_statpro(c: CallbackQuery):
    u = await db.get_user(c.from_user.id)
    sub_end = u['sub_end']
    # Красивая дата
    try: date_str = datetime.fromisoformat(sub_end).strftime("%d.%m.%Y %H:%M")
    except: date_str = "Не активна"
    
    is_active = await db.check_sub(c.from_user.id)
    st = "✅ АКТИВНА" if is_active else "❌ НЕ АКТИВНА"
    
    txt = (f"🤖 <b>StatPro Профиль</b>\n"
           f"🆔 ID: <code>{u['user_id']}</code>\n"
           f"📅 Дата реги: {u['joined_at'][:10]}\n"
           f"➖➖➖➖➖➖➖\n"
           f"💎 Подписка: <b>{st}</b>\n"
           f"⏳ Истекает: {date_str}")
    
    await safe_edit(c, txt, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="m_statpro")]]))

# Промокод (Теперь тут)
@router.callback_query(F.data == "m_pro")
async def promo_input(c: CallbackQuery, state: FSMContext):
    await safe_edit(c, "🎟 <b>Введите промокод:</b>", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="m_statpro")]]))
    await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def promo_act(m: Message, state: FSMContext):
    res = await db.use_promo(m.from_user.id, m.text.strip())
    if res: await m.answer(f"✅ Успех! Добавлено: <b>{res} дней</b>."); await mng_w(m.from_user.id, 'start') # Сразу перезапускаем воркера
    else: await m.answer("❌ Неверный или использованный код.")
    await state.clear()

# --- РАЗДЕЛ CASINO ---
@router.callback_query(F.data == "m_casino")
async def casino_main(c: CallbackQuery):
    await safe_edit(c, "🎰 <b>Добро пожаловать в StatLud Casino!</b>\nИграй и выигрывай.", kb_casino_menu())

@router.callback_query(F.data == "p_casino")
async def profile_casino(c: CallbackQuery):
    u = await db.get_user(c.from_user.id)
    cur = u['selected_currency']
    
    total = u['games_played']
    wins = u['games_won']
    wr = (wins / total * 100) if total > 0 else 0.0
    
    txt = (f"👤 <b>Casino Профиль</b>\n"
           f"💰 USDT: <b>{u['balance_usdt']:,.2f} $</b>\n"
           f"🎃 Тыквы: <b>{u['balance_st']:,.0f} ST</b>\n"
           f"⭐️ Выбрано: <b>{cur}</b>\n"
           f"➖➖➖➖➖➖➖\n"
           f"🎮 Всего игр: <b>{total}</b>\n"
           f"🏆 Побед: <b>{wins}</b>\n"
           f"📊 Винрейт: <b>{wr:.1f}%</b>")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Выбрать USDT", callback_data="set_USDT"), InlineKeyboardButton(text="🎃 Выбрать Тыквы", callback_data="set_ST")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="m_casino")]
    ])
    await safe_edit(c, txt, kb)

@router.callback_query(F.data.startswith("set_"))
async def set_cur(c: CallbackQuery):
    new_c = c.data.split("_")[1]
    await db.set_currency(c.from_user.id, new_c)
    await c.answer(f"✅ Валюта: {new_c}")
    await profile_casino(c)

# --- ИГРЫ (Упрощено для примера) ---
@router.callback_query(F.data == "cg_list")
async def games_list(c: CallbackQuery):
    rows = []
    for k, v in CASINO_GAMES.items():
        rows.append([InlineKeyboardButton(text=v['name'], callback_data=f"play_{k}")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="m_casino")])
    await safe_edit(c, "🎮 <b>Выберите игру:</b>", InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data.startswith("play_"))
async def play_start(c: CallbackQuery, state: FSMContext):
    key = c.data.split("_")[1]
    await state.update_data(game=key)
    await safe_edit(c, "💰 <b>Введите сумму ставки:</b>", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="cg_list")]]))
    await state.set_state(CasinoS.BET)

@router.message(CasinoS.BET)
async def play_process(m: Message, state: FSMContext):
    try: bet = float(m.text); 
    except: return await m.answer("❌ Введите число.")
    if bet < 10: return await m.answer("❌ Мин. ставка 10.")
    
    data = await state.get_data(); game = data['game']
    uid = m.from_user.id; u = await db.get_user(uid)
    cur = u['selected_currency']
    bal = u['balance_usdt'] if cur == 'USDT' else u['balance_st']
    
    if bal < bet: return await m.answer(f"❌ Не хватает баланса! У вас {bal:.2f}")
    
    # Списываем
    await db.update_balance(uid, -bet, cur, is_win=False, is_game=True)
    await state.clear()
    
    # Имитация игры (упрощенная)
    emoji = CASINO_GAMES[game]['emoji']
    msg = await m.answer_dice(emoji=emoji)
    await asyncio.sleep(4)
    val = msg.dice.value
    
    # Простая логика победы (пример)
    win = False; coef = 0
    if game == 'dice_classic':
        if val > 3: win=True; coef=1.9 # Просто пример
    elif game == 'bowl':
        if val == 6: win=True; coef=5.0
    # ... тут остальная логика игр из прошлых версий ...
    
    if win:
        prize = bet * coef
        await db.update_balance(uid, prize, cur, is_win=True, is_game=False) # is_game=False чтобы не считать за 2 игры
        await m.answer(f"✅ <b>Победа!</b> +{prize:.2f} {cur}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Еще раз", callback_data=f"play_{game}")]]))
    else:
        await m.answer("❌ Проигрыш.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Еще раз", callback_data=f"play_{game}")]]))

# --- ADMIN PANEL ---
@router.callback_query(F.data == "m_adm")
async def adm_menu(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID: return
    await safe_edit(c, "👑 <b>Админ Панель</b>", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Выдать Тест-Баланс", callback_data="adm_bal")],
        [InlineKeyboardButton(text="🔙 Выход", callback_data="menu")]
    ]))

@router.callback_query(F.data == "adm_bal")
async def adm_b_u(c: CallbackQuery, state: FSMContext):
    await safe_edit(c, "🆔 <b>Введите ID пользователя:</b>")
    await state.set_state(AdminBalS.UID)

@router.message(AdminBalS.UID)
async def adm_b_c(m: Message, state: FSMContext):
    await state.update_data(uid=int(m.text))
    await m.answer("Выберите валюту:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="USDT", callback_data="c_USDT"), InlineKeyboardButton(text="ST", callback_data="c_ST")]]))
    await state.set_state(AdminBalS.CUR)

@router.callback_query(AdminBalS.CUR)
async def adm_b_a(c: CallbackQuery, state: FSMContext):
    cur = c.data.split("_")[1]; await state.update_data(cur=cur)
    await safe_edit(c, f"🔢 <b>Введите сумму ({cur}):</b>")
    await state.set_state(AdminBalS.AMT)

@router.message(AdminBalS.AMT)
async def adm_b_f(m: Message, state: FSMContext):
    d = await state.get_data()
    await db.update_balance(d['uid'], float(m.text), d['cur'])
    await m.answer(f"✅ Выдано <b>{m.text} {d['cur']}</b> пользователю <code>{d['uid']}</code>")
    await state.clear()

# --- AUTH & WORKER MANAGEMENT ---
@router.callback_query(F.data == "m_auth")
async def auth_menu(c: CallbackQuery):
    await safe_edit(c, "📲 <b>Авторизация</b>", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 QR-Код", callback_data="auth_qr"), InlineKeyboardButton(text="📞 Телефон", callback_data="auth_ph")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="m_statpro")]
    ]))

# (Тут стандартные хендлеры авторизации auth_qr/auth_ph как в прошлой версии, только не забывай вызывать await mng_w(uid, 'start') в конце)

@router.callback_query(F.data == "m_w_mng")
async def worker_manage(c: CallbackQuery):
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ Нет подписки!", True)
    w = W_POOL.get(c.from_user.id)
    st = w.status if w else "🔴 Остановлен"
    await safe_edit(c, f"⚙️ <b>Статус:</b> {st}", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Запуск", callback_data="w_on"), InlineKeyboardButton(text="🔴 Стоп", callback_data="w_off")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="m_statpro")]
    ]))

@router.callback_query(F.data == "w_on")
async def won(c: CallbackQuery): await c.answer("⏳"); await mng_w(c.from_user.id, 'start'); await asyncio.sleep(1); await worker_manage(c)
@router.callback_query(F.data == "w_off")
async def woff(c: CallbackQuery): await c.answer("🛑"); await mng_w(c.from_user.id, 'stop'); await asyncio.sleep(0.5); await worker_manage(c)

# --- MAIN ---
async def main():
    await db.init()
    
    # 🔥 АВТО-СТАРТ СЕССИЙ (ЧТОБЫ НЕ ВЫЛЕТАЛО)
    logger.info("♻️ Проверка сохраненных сессий...")
    count = 0
    for f in SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub(uid): # Запускаем только если есть сабка
                await mng_w(uid, 'start')
                count += 1
        except Exception as e: logger.error(f"Error loading {f}: {e}")
    
    logger.info(f"✅ Восстановлено {count} воркеров. StatPro v57.0 запущен!")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
