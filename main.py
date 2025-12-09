#!/usr/bin/env python3
"""
💀 StatPro v59.0 - AUTO REPORTER (Clean & Powerful)
---------------------------------------------------
✅ MODE: Только отчеты (IT + Drop).
✅ UI: Inline в меню, Reply-кнопки в разделе отчетов.
✅ LOGIC: Авто-сохранение, защита от сбоев.
"""

import asyncio
import logging
import os
import io
import json
import aiosqlite
import qrcode
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    CallbackQuery, Message, BufferedInputFile, ChatMemberUpdated
)
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import User

# =========================================================================
# ⚙️ НАСТРОЙКИ
# =========================================================================

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "sessions"
DB_PATH = BASE_DIR / "statpro_reporter.db"
STATE_FILE = BASE_DIR / "reports_log.json"

SESSION_DIR.mkdir(parents=True, exist_ok=True)

# ⚠️ Вставь свои данные
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "YOUR_HASH")

SUB_CHANNEL = "@STAT_PRO1" # Единственный канал
MSK_TZ = timezone(timedelta(hours=3))

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger("AutoReporter")

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
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, username TEXT, 
                    sub_end TEXT, joined_at TEXT
                )
            """)
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations INTEGER)")
            await db.commit()

    async def upsert_user(self, uid: int, uname: str):
        now = datetime.now().isoformat()
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, ?, ?)", (uid, uname, now, now))
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def check_sub(self, uid: int) -> bool:
        if uid == ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                row = await c.fetchone()
                if not row or not row[0]: return False
                try: return datetime.fromisoformat(row[0]) > datetime.now()
                except: return False

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            # Продлеваем
            u_date = datetime.now()
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                ur = await c.fetchone()
                if ur and ur[0]:
                    try: 
                        curr = datetime.fromisoformat(ur[0])
                        if curr > u_date: u_date = curr
                    except: pass
            new_end = u_date + timedelta(days=days)
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end.isoformat(), uid))
            await db.commit()
        return days

db = Database()

# =========================================================================
# 🧠 REPORT ENGINE (Logic)
# =========================================================================

class ReportManager:
    """Управляет записью отчетов в JSON"""
    def __init__(self): 
        self.data = {}
        self.load()

    def load(self):
        if STATE_FILE.exists():
            try: 
                with open(STATE_FILE, 'r', encoding='utf-8') as f: 
                    raw = json.load(f)
                    # Восстанавливаем datetime
                    self.data = {int(k): {**v, 'start': datetime.fromisoformat(v['start'])} for k, v in raw.items()}
            except: self.data = {}

    def save(self):
        try:
            export = {str(k): {**v, 'start': v['start'].isoformat()} for k, v in self.data.items()}
            with open(STATE_FILE, 'w', encoding='utf-8') as f: json.dump(export, f, ensure_ascii=False)
        except: pass

    def start_session(self, uid: int, mode: str):
        self.data[uid] = {'mode': mode, 'logs': [], 'start': datetime.now(MSK_TZ)}
        self.save()

    def add_log(self, uid: int, entry: str):
        if uid in self.data:
            ts = datetime.now(MSK_TZ).strftime("%H:%M")
            self.data[uid]['logs'].append(f"[{ts}] {entry}")
            self.save()

    def stop_session(self, uid: int):
        if uid in self.data:
            session = self.data.pop(uid)
            self.save()
            return session
        return None

    def get_active_mode(self, uid: int):
        return self.data.get(uid, {}).get('mode')

rm = ReportManager()

# =========================================================================
# 🤖 USERBOT WORKER
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        self.status = "⚪️ Stopped"

    async def start(self):
        s_path = SESSION_DIR / f"session_{self.uid}"
        self.client = TelegramClient(str(s_path), API_ID, API_HASH, auto_reconnect=True)
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): 
                self.status = "🔴 Auth Failed"; return False
            self.status = "🟢 Active"
            self._bind()
            asyncio.create_task(self.client.run_until_disconnected())
            return True
        except Exception as e:
            logger.error(f"Worker Error: {e}")
            return False

    async def stop(self):
        if self.client: await self.client.disconnect()
        self.status = "🔴 Stopped"

    def _bind(self):
        """Логика перехвата сообщений"""
        
        @self.client.on(events.NewMessage(incoming=True))
        async def handler(e):
            mode = rm.get_active_mode(self.uid)
            if not mode: return

            # --- РЕЖИМ ДРОПЫ (Лог всех сообщений) ---
            if mode == 'drop':
                sender = await e.get_sender()
                name = "Unknown"
                if isinstance(sender, User):
                    name = sender.first_name or sender.username or "User"
                elif sender:
                    name = getattr(sender, 'title', 'Chat')
                
                txt = e.text or "[Media]"
                rm.add_log(self.uid, f"{name}: {txt}")

        @self.client.on(events.NewMessage(outgoing=True))
        async def out_handler(e):
            mode = rm.get_active_mode(self.uid)
            # --- РЕЖИМ IT (Лог команд) ---
            if mode == 'it':
                # Перехват команд .встал, .зм, .пв
                txt = e.text.lower()
                if txt.startswith(('.встал', '.зм', '.пв')):
                    parts = txt.split()
                    cmd = parts[0]
                    arg = parts[1] if len(parts) > 1 else ""
                    rm.add_log(self.uid, f"CMD: {cmd.upper()} | Arg: {arg}")
                    # Реакция для подтверждения
                    try: await e.client(SendReactionRequest(e.chat_id, e.id, [types.ReactionEmoji(emoticon='✍️')]))
                    except: pass

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 📱 BOT INTERFACE
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router()
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()

# --- КЛАВИАТУРЫ ---

def kb_main(uid, is_sub=False):
    """Главное меню - INLINE"""
    st = "🟢 Активна" if is_sub else "🔴 Нет подписки"
    rows = [
        [InlineKeyboardButton(text="📑 Открыть Раздел ОТЧЕТЫ", callback_data="open_reports")],
        [InlineKeyboardButton(text="🔑 Вход (QR/Тел)", callback_data="m_auth"), InlineKeyboardButton(text="🎟 Промокод", callback_data="m_pro")],
        [InlineKeyboardButton(text=f"💎 Подписка: {st}", callback_data="check_sub")]
    ]
    if uid == ADMIN_ID:
        rows.append([InlineKeyboardButton(text="👑 Админ: Создать Промо", callback_data="adm_promo")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_reports_reply(mode=None):
    """Меню отчетов - REPLY (Кнопки внизу)"""
    # Индикация на кнопках
    t_drop = "📦 Дроп (СТОП)" if mode == 'drop' else "📦 Дроп (СТАРТ)"
    t_it = "💻 Айти (СТОП)" if mode == 'it' else "💻 Айти (СТАРТ)"
    
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t_drop), KeyboardButton(text=t_it)],
            [KeyboardButton(text="🔙 Назад в меню")]
        ],
        resize_keyboard=True,
        persistent=True
    )
    return kb

# --- HANDLERS ---

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    await db.upsert_user(uid, m.from_user.username or "User")
    
    # Проверка подписки
    try:
        mem = await bot.get_chat_member(SUB_CHANNEL, uid)
        if mem.status in ['left', 'kicked'] and uid != ADMIN_ID:
            return await m.answer(f"⛔️ <b>Доступ закрыт!</b>\nПодпишись на: {SUB_CHANNEL}", 
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")]]))
    except: pass # Если бот не админ канала, пропускаем

    is_sub = await db.check_sub(uid)
    await m.answer("🤖 <b>AutoReporter v59.0</b>\nСистема автоматической отчетности.", 
                   reply_markup=kb_main(uid, is_sub))

@router.callback_query(F.data == "check_sub")
async def chk(c: CallbackQuery, state: FSMContext):
    await c.message.delete()
    await start(c.message, state)

# --- РАЗДЕЛ ОТЧЕТЫ (REPLY КНОПКИ) ---

@router.callback_query(F.data == "open_reports")
async def open_rep(c: CallbackQuery):
    if not await db.check_sub(c.from_user.id): return await c.answer("⛔️ Нужна активная подписка!", True)
    
    # Проверяем воркера
    if c.from_user.id not in W_POOL:
         # Пробуем запустить тихо
         w = Worker(c.from_user.id)
         if await w.start(): W_POOL[c.from_user.id] = w
         else: return await c.answer("⚠️ Сначала войдите в аккаунт (Кнопка Вход)!", True)

    mode = rm.get_active_mode(c.from_user.id)
    await c.message.delete() # Удаляем инлайн меню
    await c.message.answer("🗂 <b>Панель Отчетов открыта!</b>\nИспользуй кнопки внизу 👇", reply_markup=kb_reports_reply(mode))

# Обработка Reply кнопок
@router.message(F.text.startswith("📦 Дроп"))
async def toggle_drop(m: Message):
    uid = m.from_user.id
    curr = rm.get_active_mode(uid)
    
    if curr == 'drop':
        # Стоп
        data = rm.stop_session(uid)
        file_io = io.BytesIO("\n".join(data['logs']).encode('utf-8')); file_io.name = "drop_log.txt"
        await m.answer_document(BufferedInputFile(file_io.getvalue(), "drop_log.txt"), caption="✅ <b>Отчет Дропы завершен.</b>", reply_markup=kb_reports_reply(None))
    else:
        # Старт
        if curr: rm.stop_session(uid) # Остановить другой режим если был
        rm.start_session(uid, 'drop')
        await m.answer("🟢 <b>Режим ДРОПЫ включен.</b>\nЛогирую все сообщения...", reply_markup=kb_reports_reply('drop'))

@router.message(F.text.startswith("💻 Айти"))
async def toggle_it(m: Message):
    uid = m.from_user.id
    curr = rm.get_active_mode(uid)
    
    if curr == 'it':
        # Стоп
        data = rm.stop_session(uid)
        # Формируем красивый IT отчет
        lines = ["💻 <b>IT ОТЧЕТ</b>", ""]
        for log in data['logs']: lines.append(log)
        text_rep = "\n".join(lines)
        await m.answer(text_rep, reply_markup=kb_reports_reply(None))
    else:
        # Старт
        if curr: rm.stop_session(uid)
        rm.start_session(uid, 'it')
        await m.answer("🟢 <b>Режим IT включен.</b>\nЛовлю команды: <code>.встал</code>, <code>.зм</code>...", reply_markup=kb_reports_reply('it'))

@router.message(F.text == "🔙 Назад в меню")
async def back_inline(m: Message, state: FSMContext):
    # Убираем Reply клавиатуру
    dummy = await m.answer("🔄", reply_markup=ReplyKeyboardRemove())
    await dummy.delete()
    # Возвращаем Inline
    await start(m, state)

# --- АВТОРИЗАЦИЯ И ПРОМО (Inline) ---

@router.callback_query(F.data == "m_auth")
async def auth_start(c: CallbackQuery):
    await c.message.edit_text("📲 <b>Вход в аккаунт</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 QR-Код", callback_data="a_qr"), InlineKeyboardButton(text="📞 Телефон", callback_data="a_ph")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="check_sub")]
    ]))

@router.callback_query(F.data == "a_qr")
async def auth_qr(c: CallbackQuery):
    w = Worker(c.from_user.id); s_path = SESSION_DIR / f"session_{c.from_user.id}"
    cl = TelegramClient(str(s_path), API_ID, API_HASH)
    await cl.connect()
    qr = await cl.qr_login()
    img = qrcode.make(qr.url).convert("RGB"); bio = io.BytesIO(); img.save(bio, "PNG"); bio.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(bio.read(), "qr.png"), caption="📸 Сканируй! Жду 60 сек...")
    try:
        await qr.wait(60); await msg.delete(); await c.message.answer("✅ Готово! Нажми 'Назад' и открывай Отчеты.")
        await cl.disconnect()
    except: await msg.delete(); await c.message.answer("❌ Время вышло.")

@router.callback_query(F.data == "m_pro")
async def promo_ask(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("🎟 Введите код:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="check_sub")]]))
    await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def promo_use(m: Message, state: FSMContext):
    d = await db.use_promo(m.from_user.id, m.text.strip())
    if d: await m.answer(f"✅ Подписка продлена на {d} дней!")
    else: await m.answer("❌ Неверный код.")
    await state.clear(); await start(m, state)

# --- АДМИНКА ---
@router.callback_query(F.data == "adm_promo")
async def adm_promo(c: CallbackQuery):
    code = f"PRO-{random.randint(1000,9999)}"
    async with db.get_conn() as d:
        await d.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, 30, 1)); await d.commit() # 30 дней, 1 активация
    await c.answer(f"Код создан: {code}", show_alert=True)

# --- ЗАПУСК ---
async def main():
    await db.init()
    # Автостарт сессий
    for f in SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub(uid):
                w = Worker(uid)
                if await w.start(): W_POOL[uid] = w
        except: pass
        
    logger.info("🔥 AutoReporter v59.0 Started")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
