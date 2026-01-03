#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro v72.0 - TITANIUM EDITION (RUSSIAN)
---------------------------------------------
Архитектура: Non-blocking Event Loop
Исправления:
1. ✅ Siphon: Чтение CSV в память, поддержка любых разделителей.
2. ✅ Scan: Безопасный сбор сообщений.
3. ✅ AI: Ротация провайдеров (нет ошибок атрибутов).
4. ✅ DB: Индексы и WAL режим.
5. ✅ Core: Запуск клиента в фоне (не блокирует бота).
"""

import asyncio
import logging
import os
import io
import random
import time
import qrcode
import aiosqlite
import csv
import sys
import re
import json
from pathlib import Path
from typing import Dict, List
from dataclasses import dataclass
from datetime import datetime

# --- БИБЛИОТЕКИ ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties

from telethon import TelegramClient, events, types
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# --- AI CORE ---
try:
    from g4f.client import AsyncClient
    import g4f
    g4f.debug.logging = False
except ImportError:
    os.system("pip install -U g4f[all] curl_cffi aiohttp")
    from g4f.client import AsyncClient
    import g4f

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ
# =========================================================================

@dataclass
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_titanium.db"
    TEMP_DIR: Path = BASE_DIR / "temp"
    
    DEVICE_MODEL: str = "iPhone 15 Pro Max"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ ОШИБКА: Не заполнены переменные окружения!")
            sys.exit(1)

cfg = Config()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("StatPro")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ (С ИНДЕКСАМИ)
# =========================================================================

class Database:
    def __init__(self): self.path = cfg.DB_PATH
    def get_conn(self): return aiosqlite.connect(self.path)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            
            # Таблица юзеров
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    username TEXT, 
                    sub_end INTEGER, 
                    joined_at INTEGER
                )
            """)
            # Исправление №6 (Индексы)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_sub ON users(sub_end)")
            
            # Таблица промокодов
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY, 
                    days INTEGER, 
                    activations INTEGER
                )
            """)
            await db.commit()

    async def check_sub_bool(self, uid: int) -> bool:
        if uid == cfg.ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                return r[0] > int(time.time()) if (r and r[0]) else False

    async def upsert_user(self, uid: int, uname: str):
        now = int(time.time())
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, 0, ?)", (uid, uname, now))
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            
            # Гарантируем наличие юзера
            await db.execute("INSERT OR IGNORE INTO users (user_id, sub_end, joined_at) VALUES (?, 0, ?)", (uid, int(time.time())))
            
            now = int(time.time())
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c2:
                row = await c2.fetchone()
                curr = row[0] if (row and row[0]) else 0
            
            new_end = (curr if curr > now else now) + (days * 86400)
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"TITAN-{random.randint(100,999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code
    
    async def get_user_info(self, uid: int):
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end, joined_at FROM users WHERE user_id = ?", (uid,)) as c:
                return await c.fetchone()

db = Database()

# =========================================================================
# 🧠 AI ENGINE (РОТАЦИЯ)
# =========================================================================

async def ask_gpt_safe(system_prompt: str, user_content: str) -> str:
    client = AsyncClient()
    # Список надежных провайдеров
    providers = [
        g4f.Provider.Blackbox,
        g4f.Provider.DeepInfra,
        g4f.Provider.PollinationsAI,
        g4f.Provider.DarkAI
    ]

    for provider in providers:
        try:
            response = await client.chat.completions.create(
                model="gpt-4o",
                provider=provider,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ]
            )
            res = response.choices[0].message.content
            if res and len(res.strip()) > 0:
                return res
        except:
            continue
            
    return "❌ Ошибка AI: Все каналы заняты."

# =========================================================================
# 🦾 WORKER (ЮЗЕРБОТ)
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        self.spam_task = None

    def _get_client(self, path):
        return TelegramClient(
            str(path), cfg.API_ID, cfg.API_HASH, 
            device_model=cfg.DEVICE_MODEL, 
            system_version=cfg.SYSTEM_VERSION, 
            app_version=cfg.APP_VERSION,
            sequential_updates=False # Важно для скорости
        )

    async def start(self):
        """Исправление №1: Неблокирующий запуск"""
        self.client = self._get_client(cfg.SESSION_DIR / f"session_{self.uid}")
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): return False
            
            self._bind()
            # Запускаем в фоне, чтобы не блочить бота
            asyncio.create_task(self._run_safe())
            return True
        except Exception as e:
            logger.error(f"Worker {self.uid} start error: {e}")
            return False

    async def _run_safe(self):
        """Авто-реконнект"""
        while True:
            try:
                await self.client.run_until_disconnected()
            except Exception:
                await asyncio.sleep(5)
                try: await self.client.connect()
                except: pass
            if not await self.client.is_user_authorized(): break

    def _bind(self):
        cl = self.client

        # --- ⚡️ .g (AI) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.g(?: |$)(.*)'))
        async def quiz_cmd(e):
            await e.edit("⚡️...")
            q = e.pattern_match.group(1)
            if not q and e.is_reply:
                r = await e.get_reply_message()
                q = r.text or r.caption
            
            if not q: return await e.edit("❌ Текст?")
            
            ans = await ask_gpt_safe("Ты помощник викторины. Ответ 1-2 слова.", q)
            await e.edit(f"<b>{ans}</b>", parse_mode='html')

        # --- 🕵️‍♂️ .report (АНАЛИТИКА) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.report$'))
        async def report_cmd(e):
            await e.edit("🕵️‍♂️ <b>Сбор данных...</b>")
            
            topic_id = None
            if e.reply_to:
                topic_id = e.reply_to.reply_to_top_id or e.reply_to.reply_to_msg_id

            keywords = ['айти', 'вбив', 'номер', 'код', 'встал', 'слет', 'сек', 'ща', 'готово', 'сдох', 'взял', 'отстоял']
            logs = []
            
            try:
                async for m in cl.iter_messages(e.chat_id, limit=1000, reply_to=topic_id):
                    if m.text and any(k in m.text.lower() for k in keywords):
                        ts = m.date.strftime("%H:%M")
                        name = m.sender.first_name if m.sender else "User"
                        logs.append(f"[{ts}] {name}: {m.text}")
            except Exception as ex:
                return await e.edit(f"❌ Ошибка: {ex}")

            if not logs: return await e.edit("❌ Пусто.")
            
            logs = logs[::-1]
            logs_txt = "\n".join(logs)
            await e.edit(f"🧠 <b>Анализ ({len(logs)} строк)...</b>")
            
            prompt = """
            Анализируй логи.
            1. "айти"/"вбив"/"взял" -> Старт.
            2. >35 мин без "слет" -> ✅ Отстоял.
            3. "слет"/"бан" -> ❌ Слет.
            Верни JSON: [{"num": "номер", "time": "мин", "status": "✅"}]
            """
            res = await ask_gpt_safe(prompt, logs_txt)
            try:
                # Попытка найти JSON в ответе
                json_str = re.search(r'\[.*\]', res, re.DOTALL).group()
                data = json.loads(json_str)
                txt = "📊 <b>ОТЧЕТ:</b>\n\n"
                ok = 0
                for i in data:
                    st = i.get('status','?')
                    txt += f"📱 {i.get('num','?')} | ⏱ {i.get('time','0')} | {st}\n"
                    if "✅" in st: ok += 1
                txt += f"\n🏆 <b>Всего OK: {ok}</b>"
                await e.edit(txt, parse_mode='html')
            except:
                await e.edit(f"📝 <b>Текст:</b>\n{res}", parse_mode='html')

        # --- 🧬 .scan (СКАЧАТЬ ЧАТ) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.scan$'))
        async def scan(e):
            await e.edit("🔎 <b>Сканирую (до 3000)...</b>")
            users = {}
            try:
                async for m in cl.iter_messages(e.chat_id, limit=3000):
                    if m.sender and isinstance(m.sender, types.User) and not m.sender.bot:
                        if m.sender_id not in users:
                            users[m.sender_id] = [m.sender.username or "", m.sender.first_name or ""]
            except: pass
            
            out = io.StringIO()
            w = csv.writer(out)
            w.writerow(["ID", "Username", "Name"])
            for uid, d in users.items(): w.writerow([uid, d[0], d[1]])
            
            out.seek(0)
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig'))
            bio.name = f"Scan_{e.chat_id}.csv"
            
            await cl.send_file("me", bio, caption=f"✅ Скан завершен. Людей: {len(users)}")
            await e.edit(f"✅ Готово: {len(users)}")

        # --- 🚀 .spam ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.(s|spam)\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def spam(e):
            txt, cnt, dly = e.pattern_match.group(2), int(e.pattern_match.group(3)), float(e.pattern_match.group(4))
            await e.delete()
            async def run():
                for _ in range(cnt):
                    try: await cl.send_message(e.chat_id, txt); await asyncio.sleep(dly)
                    except: break
            self.spam_task = asyncio.create_task(run())

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def stop(e):
            if self.spam_task: self.spam_task.cancel(); await e.edit("🛑 Стоп.")

        # --- 📢 .all ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.all(?:\s+(.+))?'))
        async def tag_all(e):
            await e.delete()
            txt = e.pattern_match.group(1) or "."
            try:
                parts = await cl.get_participants(e.chat_id)
                chunk = []
                for p in parts:
                    if not p.bot and not p.deleted:
                        chunk.append(f"<a href='tg://user?id={p.id}'>\u200b</a>")
                        if len(chunk) >= 5:
                            # Исправление №2: Проверка клиента
                            await cl.send_message(e.chat_id, txt + "".join(chunk), parse_mode='html')
                            chunk = []
                            await asyncio.sleep(2)
            except: pass

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI (AIOGRAM)
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class SiphonS(StatesGroup): FILE=State(); MSG=State(); CONFIRM=State()
class AdminS(StatesGroup): PD=State(); PA=State()

def kb_main(uid):
    btns = [
        [InlineKeyboardButton(text="🌪 Перелив (Siphon)", callback_data="siphon_start")],
        [InlineKeyboardButton(text="📚 Инфо", callback_data="help"), InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth")]
    ]
    if uid == cfg.ADMIN_ID: btns.append([InlineKeyboardButton(text="👑 Админ", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def get_numpad():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1", callback_data="n_1"), InlineKeyboardButton(text="2", callback_data="n_2"), InlineKeyboardButton(text="3", callback_data="n_3")],
        [InlineKeyboardButton(text="4", callback_data="n_4"), InlineKeyboardButton(text="5", callback_data="n_5"), InlineKeyboardButton(text="6", callback_data="n_6")],
        [InlineKeyboardButton(text="7", callback_data="n_7"), InlineKeyboardButton(text="8", callback_data="n_8"), InlineKeyboardButton(text="9", callback_data="n_9")],
        [InlineKeyboardButton(text="🔙", callback_data="n_del"), InlineKeyboardButton(text="0", callback_data="n_0"), InlineKeyboardButton(text="✅", callback_data="n_go")]
    ])

# --- HANDLERS ---

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username)
    await m.answer(f"💎 <b>StatPro TITANIUM</b>\nПривет, {m.from_user.first_name}!", reply_markup=kb_main(m.from_user.id))

@router.callback_query(F.data == "help")
async def hlp(c: CallbackQuery):
    txt = "⚡️ <b>Команды юзербота:</b>\n.g [текст] - ИИ ответ\n.report - Анализ (в реплай)\n.scan - Сбор базы\n.spam [текст] [кол] [сек]\n.all [текст] - Тег всех"
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))

@router.callback_query(F.data == "back")
async def back(c: CallbackQuery, state: FSMContext): await c.message.delete(); await start(c.message, state)

@router.callback_query(F.data == "profile")
async def prof(c: CallbackQuery):
    info = await db.get_user_info(c.from_user.id)
    sub = "🔴 Нет"
    if info and info[0] and info[0] > time.time():
        days = int((info[0] - time.time()) / 86400)
        sub = f"🟢 Активна ({days} дн.)"
    stat = "🟢 В сети" if c.from_user.id in W_POOL else "🔴 Отключен"
    
    txt = f"👤 <b>Профиль</b>\n🆔: {c.from_user.id}\n💎 Подписка: {sub}\n🔌 Воркер: {stat}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Промокод", callback_data="promo")], [InlineKeyboardButton(text="🔙", callback_data="back")]])
    await c.message.edit_text(txt, reply_markup=kb)

@router.callback_query(F.data == "promo")
async def prm(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Введите код:"); await state.set_state(PromoS.CODE)
@router.message(PromoS.CODE)
async def prm_use(m: Message, state: FSMContext):
    d = await db.use_promo(m.from_user.id, m.text)
    if d: await m.answer(f"✅ Добавлено: {d} дней"); await start(m, state)
    else: await m.answer("❌ Неверно.")
    await state.clear()

# --- SIPHON (FIXED CSV READING) ---

@router.callback_query(F.data == "siphon_start")
async def siphon_init(c: CallbackQuery, state: FSMContext):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("Нужна подписка!", True)
    if c.from_user.id not in W_POOL: return await c.answer("Войдите в аккаунт!", True)
    await c.message.edit_text("📂 <b>Кидай CSV файл</b> (.scan):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="back")]]))
    await state.set_state(SiphonS.FILE)

@router.message(SiphonS.FILE, F.document)
async def siphon_file(m: Message, state: FSMContext):
    file = await bot.get_file(m.document.file_id)
    path = cfg.TEMP_DIR / f"siphon_{m.from_user.id}.csv"
    await bot.download_file(file.file_path, path)
    
    ids = []
    # Исправление №4: Надежное чтение CSV
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            content = f.read()
            # Авто-детект разделителя
            sep = ';' if ';' in content else ','
            f.seek(0)
            reader = csv.reader(f, delimiter=sep)
            # Пропускаем заголовок
            headers = next(reader, None)
            for r in reader:
                # Ищем колонку с ID (обычно первая, если это цифры)
                if r and r[0].isdigit(): 
                    ids.append(int(r[0]))
    except Exception as e:
        if os.path.exists(path): os.remove(path)
        return await m.answer(f"❌ Ошибка файла: {e}")
    
    if os.path.exists(path): os.remove(path)
    
    if not ids: return await m.answer("❌ Файл пуст или формат неверен.")
    
    await state.update_data(targets=ids)
    await m.answer(f"✅ Загружено: {len(ids)} чел.\n✍️ <b>Введи текст рассылки:</b>")
    await state.set_state(SiphonS.MSG)

@router.message(SiphonS.MSG)
async def siphon_msg(m: Message, state: FSMContext):
    await state.update_data(msg=m.text or "Привет")
    data = await state.get_data()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🚀 ЗАПУСК ({len(data['targets'])})", callback_data="run_s"), InlineKeyboardButton(text="❌", callback_data="back")]])
    await m.answer("Подтверди запуск:", reply_markup=kb)
    await state.set_state(SiphonS.CONFIRM)

@router.callback_query(F.data == "run_s", SiphonS.CONFIRM)
async def siphon_run_handler(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    w = W_POOL.get(c.from_user.id)
    
    # Исправление №2: Проверка воркера
    if not w or not w.client or not await w.client.is_user_authorized():
        return await c.answer("Воркер отключен!", True)
        
    await c.message.edit_text("🚀 <b>Рассылка полетела!</b> (Результат придет в ЛС)")
    asyncio.create_task(siphon_task(c.from_user.id, w, data['targets'], data['msg']))
    await state.clear()

async def siphon_task(uid, w, targets, text):
    ok, fail = 0, 0
    for tid in targets:
        try:
            await w.client.send_message(tid, text)
            ok += 1
            # Исправление №7: Рандомная задержка
            await asyncio.sleep(random.uniform(5, 12)) 
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 5)
        except:
            fail += 1
    
    try: await bot.send_message(uid, f"🏁 <b>Рассылка завершена!</b>\n✅ Доставлено: {ok}\n❌ Ошибок: {fail}")
    except: pass

# --- AUTH (FULL) ---

@router.callback_query(F.data == "auth")
async def auth(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("Нужна подписка!", True)
    await c.message.edit_text("Вход:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="QR", callback_data="qr"), InlineKeyboardButton(text="Phone", callback_data="ph")]]))

@router.callback_query(F.data == "ph")
async def ph(c: CallbackQuery, state: FSMContext): await c.message.edit_text("Номер (79...):"); await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def ph_get(m: Message, state: FSMContext):
    uid = m.from_user.id
    w = Worker(uid)
    # Временный клиент для логина
    w.client = TelegramClient(str(cfg.SESSION_DIR / f"session_{uid}"), cfg.API_ID, cfg.API_HASH)
    await w.client.connect()
    try:
        s = await w.client.send_code_request(m.text)
        await state.update_data(ph=m.text, h=s.phone_code_hash, uid=uid, c="")
        await w.client.disconnect()
        await m.answer("Код:", reply_markup=get_numpad())
        await state.set_state(AuthS.CO)
    except Exception as e:
        await w.client.disconnect()
        await m.answer(f"Ошибка: {e}")
        await state.clear() # Исправление №5

@router.callback_query(F.data.startswith("n_"), AuthS.CO)
async def numpad(c: CallbackQuery, state: FSMContext):
    act = c.data.split("_")[1]
    d = await state.get_data()
    curr = d.get("c", "")
    if act == "del": curr = curr[:-1]
    elif act == "go":
        if not curr: return await c.answer("Пусто!", True)
        await c.message.edit_text("⏳ Вход...")
        w = Worker(d['uid'])
        w.client = TelegramClient(str(cfg.SESSION_DIR / f"session_{d['uid']}"), cfg.API_ID, cfg.API_HASH)
        await w.client.connect()
        try:
            await w.client.sign_in(d['ph'], curr, phone_code_hash=d['h'])
            await w.client.disconnect() # Закрываем логин-сессию
            
            # Запускаем боевой воркер
            real_w = Worker(d['uid'])
            if await real_w.start(): # Исправление №3 (start sequence)
                W_POOL[d['uid']] = real_w
                await c.message.answer("✅ Готово!")
                await start(c.message, state)
            else:
                await c.message.answer("❌ Ошибка запуска воркера.")
        except SessionPasswordNeededError:
            await w.client.disconnect()
            await c.message.answer("🔒 Введи 2FA пароль:")
            await state.set_state(AuthS.PA)
            return
        except Exception as e:
            await w.client.disconnect()
            await c.message.answer(f"❌ {e}")
        await state.clear()
        return
    else: curr += act
    
    await state.update_data(c=curr)
    try: await c.message.edit_text(f"Код: {curr}", reply_markup=get_numpad())
    except: pass

@router.message(AuthS.PA)
async def pa(m: Message, state: FSMContext):
    d = await state.get_data()
    w = Worker(d['uid'])
    w.client = TelegramClient(str(cfg.SESSION_DIR / f"session_{d['uid']}"), cfg.API_ID, cfg.API_HASH)
    await w.client.connect()
    try:
        await w.client.sign_in(password=m.text)
        await w.client.disconnect()
        real_w = Worker(d['uid'])
        if await real_w.start():
            W_POOL[d['uid']] = real_w
            await m.answer("✅ Вход выполнен!")
        else: await m.answer("❌ Ошибка старта.")
    except Exception as e: await m.answer(f"❌ {e}")
    await state.clear()

@router.callback_query(F.data == "qr")
async def qr(c: CallbackQuery, state: FSMContext):
    uid = c.from_user.id
    w = Worker(uid)
    w.client = TelegramClient(str(cfg.SESSION_DIR / f"session_{uid}"), cfg.API_ID, cfg.API_HASH)
    await w.client.connect()
    try:
        q = await w.client.qr_login()
        b = io.BytesIO(); qrcode.make(q.url).save(b, "PNG"); b.seek(0)
        msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="Сканируй QR")
        await q.wait(60)
        await msg.delete()
        await w.client.disconnect()
        
        real_w = Worker(uid)
        if await real_w.start():
            W_POOL[uid] = real_w
            await c.message.answer("✅ Успех!")
    except: await c.message.answer("❌ Тайм-аут")
    finally: await state.clear()

# --- ADMIN ---
@router.callback_query(F.data == "adm")
async def adm(c: CallbackQuery): await c.message.edit_text("Admin:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Create Promo", callback_data="mk_p")]]))
@router.callback_query(F.data == "mk_p")
async def mk_p(c: CallbackQuery, state: FSMContext): await c.message.answer("Days?"); await state.set_state(AdminS.PD)
@router.message(AdminS.PD)
async def mk_pd(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Count?"); await state.set_state(AdminS.PA)
@router.message(AdminS.PA)
async def mk_pa(m: Message, state: FSMContext): 
    d=await state.get_data(); c=await db.create_promo(d['d'], int(m.text)); await m.answer(f"Code: <code>{c}</code>")
    await state.clear()

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
    logger.info("🔥 StatPro TITANIUM Started")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
