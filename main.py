#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro v71.0 - RUSSIAN ELITE EDITION
----------------------------------------
Architect: StatPro AI
Features:
1. 🇷🇺 Полная локализация и улучшенный UI.
2. 👤 Профиль с детальной статистикой.
3. 🕵️‍♂️ .report - Умный анализ топиков (AI).
4. ⚡️ .g - Турбо-викторина (исправлены провайдеры).
5. 🌪 Перелив, Скан, Спам, Авторизация.
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
import traceback
from pathlib import Path
from typing import Dict, List, Optional
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

# --- AI ЯДРО ---
try:
    from g4f.client import AsyncClient
    import g4f
    g4f.debug.logging = False
except ImportError:
    print("⚠️ Установка библиотек AI...")
    os.system("pip install -U g4f[all] curl_cffi aiohttp python-dateutil")
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
    DB_PATH: Path = BASE_DIR / "statpro_elite.db"
    TEMP_DIR: Path = BASE_DIR / "temp"
    
    DEVICE_MODEL: str = "iPhone 15 Pro Max"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ ОШИБКА: Не указаны переменные окружения (BOT_TOKEN, API_ID, API_HASH).")
            sys.exit(1)

cfg = Config()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("StatPro")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ
# =========================================================================

class Database:
    def __init__(self): self.path = cfg.DB_PATH
    def get_conn(self): return aiosqlite.connect(self.path)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            # Таблица пользователей
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    username TEXT, 
                    sub_end INTEGER, 
                    joined_at INTEGER
                )
            """)
            # Таблица промокодов
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY, 
                    days INTEGER, 
                    activations INTEGER
                )
            """)
            await db.commit()

    async def get_user_info(self, uid: int):
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end, joined_at FROM users WHERE user_id = ?", (uid,)) as c:
                return await c.fetchone()

    async def check_sub_bool(self, uid: int) -> bool:
        if uid == cfg.ADMIN_ID: return True
        info = await self.get_user_info(uid)
        if not info or info[0] is None: return False
        return info[0] > int(time.time())

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
            
            # Обеспечиваем существование юзера
            await db.execute("INSERT OR IGNORE INTO users (user_id, sub_end, joined_at) VALUES (?, 0, ?)", (uid, int(time.time())))
            
            # Обновляем подписку
            now = int(time.time())
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c2:
                row = await c2.fetchone()
                curr = row[0] if (row and row[0]) else 0
            
            new_end = (curr if curr > now else now) + (days * 86400)
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"PRO-{random.randint(100,999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🧠 AI ДВИЖОК (БЕЗ ОШИБОК)
# =========================================================================

async def safe_ai_request(system_prompt: str, user_content: str) -> str:
    """
    Умная ротация провайдеров. Liaobots удален.
    """
    client = AsyncClient()
    # Список только рабочих и бесплатных
    providers = [
        g4f.Provider.Blackbox,
        g4f.Provider.DeepInfra,
        g4f.Provider.DarkAI,
        g4f.Provider.PollinationsAI,
        g4f.Provider.ChatGptEs
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
        except Exception:
            continue
            
    return "❌ Ошибка AI: Все каналы заняты. Попробуйте через минуту."

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
            # Важно: отключает ожидание идеальной синхронизации (лечит TimestampOutdated)
            sequential_updates=False 
        )

    async def start(self):
        """Неблокирующий запуск"""
        self.client = self._get_client(cfg.SESSION_DIR / f"session_{self.uid}")
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized(): return False
            
            self._bind()
            # Запускаем в фоне
            asyncio.create_task(self._run_safe())
            return True
        except Exception as e:
            logger.error(f"Ошибка запуска воркера {self.uid}: {e}")
            return False

    async def _run_safe(self):
        """Авто-реконнект при разрыве"""
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

        # --- 🕵️‍♂️ .report (АНАЛИТИКА ТОПИКОВ) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.report$'))
        async def report_cmd(e):
            await e.edit("🕵️‍♂️ <b>Собираю данные...</b>")
            
            # Определяем, сканировать топик или чат
            topic_id = None
            if e.reply_to:
                topic_id = e.reply_to.reply_to_top_id or e.reply_to.reply_to_msg_id
            
            # Фильтр ключевых слов (Pre-filter)
            keywords = ['айти', 'вбив', 'номер', 'код', 'встал', 'слет', 'сек', 'ща', 'готово', 'сдох', 'взял', 'отстоял']
            logs = []
            
            try:
                async for m in cl.iter_messages(e.chat_id, limit=1000, reply_to=topic_id):
                    if m.text and any(k in m.text.lower() for k in keywords):
                        ts = m.date.strftime("%H:%M")
                        name = m.sender.first_name if m.sender else "Юзер"
                        logs.append(f"[{ts}] {name}: {m.text}")
            except Exception as ex:
                return await e.edit(f"❌ Ошибка доступа: {ex}")

            if not logs: return await e.edit("❌ Логи пусты (нет ключевых слов).")
            
            logs = logs[::-1] # Хронологический порядок
            logs_txt = "\n".join(logs)
            
            await e.edit(f"🧠 <b>Анализ {len(logs)} сообщений...</b>")
            
            prompt = """
            Ты аналитик логов. Формат: [Время] Имя: Текст.
            Цель: Найти сессии работы с номерами.
            Логика:
            1. "айти"/"вбив"/"взял" -> Старт.
            2. Если прошло >35 мин и НЕ было слов "слет"/"бан"/"сдох" -> ✅ Отстоял.
            3. Если были слова "слет"/"бан"/"сдох" -> ❌ Слет.
            
            Верни ТОЛЬКО JSON список:
            [{"num": "номер телефона или ID", "time": "время в мин", "status": "✅ Отстоял" или "❌ Слет"}]
            """
            
            res = await safe_ai_request(prompt, logs_txt)
            
            try:
                # Очистка от Markdown
                json_str = re.sub(r'```json\s*|\s*```', '', res).strip()
                data = json.loads(json_str)
                
                txt = "📊 <b>ОТЧЕТ ПО РАБОТЕ:</b>\n\n"
                ok_count = 0
                for item in data:
                    st = item.get('status', '❓')
                    txt += f"📱 <code>{item.get('num', '???')}</code>\n⏱ {item.get('time', '0')} мин | {st}\n\n"
                    if "✅" in st: ok_count += 1
                
                txt += f"🏆 <b>Успешно: {ok_count} шт.</b>"
                await e.edit(txt, parse_mode='html')
            except:
                # Если ИИ вернул текст, а не JSON
                await e.edit(f"📝 <b>Отчет (Текст):</b>\n\n{res}", parse_mode='html')

        # --- ⚡️ .g (ТУРБО ВИКТОРИНА) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.g(?: |$)(.*)'))
        async def quiz_cmd(e):
            await e.edit("⚡️")
            q = e.pattern_match.group(1)
            if not q and e.is_reply:
                r = await e.get_reply_message()
                q = r.text or r.caption
            
            if not q: return await e.edit("❌ Где вопрос?")
            
            sys_p = "Ты помощник для викторин. Отвечай только 1-3 словами. Только правильный ответ. Без знаков препинания."
            ans = await safe_ai_request(sys_p, q)
            await e.edit(f"<b>{ans}</b>", parse_mode='html')

        # --- 🚀 .spam / .stop ---
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
        async def stop_spam(e):
            if self.spam_task: 
                self.spam_task.cancel()
                self.spam_task = None
                await e.edit("🛑 Спам остановлен.")
            else:
                await e.edit("⚠️ Нет активных задач.")

        # --- 🧬 .scan (СКАНИРОВАНИЕ) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.scan$'))
        async def scan(e):
            await e.edit("🔎 <b>Сканирую чат...</b>")
            users = {}
            async for m in cl.iter_messages(e.chat_id, limit=2000):
                if m.sender and isinstance(m.sender, types.User) and not m.sender.bot:
                    users[m.sender_id] = [m.sender.username or "", m.sender.first_name or ""]
            
            # Создаем CSV с правильной кодировкой
            out = io.StringIO()
            w = csv.writer(out)
            w.writerow(["ID", "Username", "Name"])
            for uid, d in users.items(): w.writerow([uid, d[0], d[1]])
            out.seek(0)
            
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig'))
            bio.name = f"Scan_{e.chat_id}.csv"
            await cl.send_file("me", bio, caption=f"✅ Найдено пользователей: {len(users)}")
            await e.edit("✅ Готово (см. Избранное)")

        # --- 📢 .all (ТЕГ ВСЕХ) ---
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
                            await cl.send_message(e.chat_id, txt + "".join(chunk), parse_mode='html')
                            chunk = []
                            await asyncio.sleep(2)
            except Exception as ex:
                # Если нет прав админа
                await cl.send_message(e.chat_id, f"❌ Ошибка: {ex}")

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI (AIOGRAM) - ИНТЕРФЕЙС
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class SiphonS(StatesGroup): FILE=State(); MSG=State(); CONFIRM=State()
class AdminS(StatesGroup): PD=State(); PA=State()

# --- КЛАВИАТУРЫ ---

def kb_main(uid):
    btns = [
        [InlineKeyboardButton(text="🌪 Перелив", callback_data="siphon_start"), InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="📚 Команды", callback_data="help"), InlineKeyboardButton(text="🔑 Вход", callback_data="auth")]
    ]
    if uid == cfg.ADMIN_ID: btns.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def get_numpad_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1", callback_data="n_1"), InlineKeyboardButton(text="2", callback_data="n_2"), InlineKeyboardButton(text="3", callback_data="n_3")],
        [InlineKeyboardButton(text="4", callback_data="n_4"), InlineKeyboardButton(text="5", callback_data="n_5"), InlineKeyboardButton(text="6", callback_data="n_6")],
        [InlineKeyboardButton(text="7", callback_data="n_7"), InlineKeyboardButton(text="8", callback_data="n_8"), InlineKeyboardButton(text="9", callback_data="n_9")],
        [InlineKeyboardButton(text="🔙", callback_data="n_del"), InlineKeyboardButton(text="0", callback_data="n_0"), InlineKeyboardButton(text="✅ Ввод", callback_data="n_go")]
    ])

# --- ХЕНДЛЕРЫ ---

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username)
    await m.answer(f"💎 <b>StatPro ELITE v71.0</b>\n\nДобро пожаловать, <b>{m.from_user.first_name}</b>!", reply_markup=kb_main(m.from_user.id))

# --- 👤 ПРОФИЛЬ ---
@router.callback_query(F.data == "profile")
async def profile_cb(c: CallbackQuery):
    info = await db.get_user_info(c.from_user.id)
    if not info:
        return await c.message.edit_text("❌ Ошибка профиля. Нажмите /start")
    
    sub_end_ts, joined_ts = info
    
    # Расчеты дат
    reg_date = datetime.fromtimestamp(joined_ts).strftime('%d.%m.%Y')
    
    is_active = False
    sub_text = "🔴 Неактивна"
    if sub_end_ts and sub_end_ts > time.time():
        is_active = True
        days_left = int((sub_end_ts - time.time()) / 86400)
        date_end = datetime.fromtimestamp(sub_end_ts).strftime('%d.%m.%Y')
        sub_text = f"🟢 Активна (еще {days_left} дн. до {date_end})"
    
    worker_status = "🟢 Подключен" if c.from_user.id in W_POOL else "🔴 Отключен"

    text = (
        f"👤 <b>ЛИЧНЫЙ КАБИНЕТ</b>\n"
        f"➖➖➖➖➖➖➖➖➖\n"
        f"🆔 <b>ID:</b> <code>{c.from_user.id}</code>\n"
        f"📅 <b>Регистрация:</b> {reg_date}\n"
        f"💎 <b>Подписка:</b> {sub_text}\n"
        f"🔌 <b>Воркер:</b> {worker_status}\n"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎟 Активировать промо", callback_data="promo")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="profile"), InlineKeyboardButton(text="🔙 Назад", callback_data="back")]
    ])
    await c.message.edit_text(text, reply_markup=kb)

# --- АВТОРИЗАЦИЯ ---
@router.callback_query(F.data == "auth")
async def auth_start(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("❌ Нужна подписка!", True)
    await c.message.edit_text("🔐 <b>Выберите способ входа:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📸 QR Код", callback_data="qr"), InlineKeyboardButton(text="📱 По номеру", callback_data="ph")], [InlineKeyboardButton(text="🔙 Назад", callback_data="back")]]))

@router.callback_query(F.data == "ph")
async def auth_ph(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 <b>Введите номер телефона:</b>\n(Например: 79001234567)")
    await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def auth_ph_get(m: Message, state: FSMContext):
    uid = m.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        sent = await cl.send_code_request(m.text)
        await state.update_data(ph=m.text, h=sent.phone_code_hash, uid=uid, c="")
        await cl.disconnect()
        await m.answer(f"📩 Код отправлен на <b>{m.text}</b>.\nВведите его на клавиатуре:", reply_markup=get_numpad_kb())
        await state.set_state(AuthS.CO)
    except Exception as e:
        await cl.disconnect()
        await m.answer(f"❌ Ошибка: {e}")
        await state.clear()

@router.callback_query(F.data.startswith("n_"), AuthS.CO)
async def auth_numpad(c: CallbackQuery, state: FSMContext):
    act = c.data.split("_")[1]
    d = await state.get_data()
    curr = d.get("c", "")
    
    if act == "del": curr = curr[:-1]
    elif act == "go":
        if not curr: return await c.answer("Введите код!", True)
        await c.message.edit_text("⏳ <b>Авторизация...</b>")
        
        cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
        await cl.connect()
        try:
            await cl.sign_in(phone=d['ph'], code=curr, phone_code_hash=d['h'])
            await c.message.answer("✅ <b>Успешно! Воркер запущен.</b>")
            await cl.disconnect()
            await state.clear()
            w = Worker(d['uid'])
            if await w.start(): W_POOL[d['uid']] = w
            await start(c.message, state)
            return
        except SessionPasswordNeededError:
            await c.message.answer("🔒 <b>Введите 2FA пароль (Облачный пароль):</b>")
            await cl.disconnect()
            await state.set_state(AuthS.PA)
            return
        except Exception as e:
            await c.message.answer(f"❌ Ошибка: {e}")
            await cl.disconnect()
            await state.clear()
            return
    else: curr += act
    
    await state.update_data(c=curr)
    try: await c.message.edit_text(f"📩 Код: <code>{curr}</code>", reply_markup=get_numpad_kb())
    except: pass

@router.message(AuthS.PA)
async def auth_2fa(m: Message, state: FSMContext):
    d = await state.get_data()
    cl = Worker(d['uid'])._get_client(cfg.SESSION_DIR / f"session_{d['uid']}")
    await cl.connect()
    try:
        await cl.sign_in(password=m.text)
        await m.answer("✅ <b>Успешно!</b>")
        await cl.disconnect()
        w = Worker(d['uid'])
        if await w.start(): W_POOL[d['uid']] = w
    except Exception as e:
        await m.answer(f"❌ Неверный пароль: {e}")
        await cl.disconnect()
    finally:
        await state.clear()

# --- QR ВХОД ---
@router.callback_query(F.data == "qr")
async def qr_h(c: CallbackQuery, state: FSMContext):
    cl = Worker(c.from_user.id)._get_client(cfg.SESSION_DIR / f"session_{c.from_user.id}")
    await cl.connect()
    qr = await cl.qr_login()
    b = io.BytesIO(); qrcode.make(qr.url).save(b, "PNG"); b.seek(0)
    msg = await c.message.answer_photo(BufferedInputFile(b.read(), "qr.png"), caption="📸 <b>Отсканируйте QR в Telegram</b>\n(Настройки -> Устройства -> Подкл. устройство)")
    try: 
        await qr.wait(60)
        await msg.delete()
        await c.message.answer("✅ <b>Вход выполнен!</b>")
        w = Worker(c.from_user.id)
        if await w.start(): W_POOL[c.from_user.id] = w
    except: 
        await msg.delete()
        await c.message.answer("⌛️ Время вышло.")
    finally:
        await cl.disconnect()
        await state.clear()

# --- ПРОМО И HELP ---
@router.callback_query(F.data == "help")
async def hlp(c: CallbackQuery):
    txt = (
        "📚 <b>СПИСОК КОМАНД:</b>\n\n"
        "⚡️ <code>.g [вопрос]</code> — ИИ Викторина (быстрый ответ)\n"
        "🕵️‍♂️ <code>.report</code> — Анализ работы номеров (в топиках)\n"
        "🧬 <code>.scan</code> — Скачать базу участников чата (CSV)\n"
        "🚀 <code>.spam [текст] [кол-во] [сек]</code> — Спам-рассылка\n"
        "🛑 <code>.stop</code> — Остановить спам\n"
        "📢 <code>.all [текст]</code> — Тегнуть всех участников"
    )
    await c.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back")]]))

@router.callback_query(F.data == "back")
async def bck(c: CallbackQuery, state: FSMContext): await c.message.delete(); await start(c.message, state)

@router.callback_query(F.data == "promo")
async def prm(c: CallbackQuery, state: FSMContext): await c.message.edit_text("🎟 <b>Введите промокод:</b>"); await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def prm_use(m: Message, state: FSMContext): 
    d = await db.use_promo(m.from_user.id, m.text)
    if d: await m.answer(f"✅ <b>Успешно!</b> Добавлено дней: {d}"); await start(m, state)
    else: await m.answer("❌ Неверный или использованный код.")
    await state.clear()

# --- SIPHON (ПЕРЕЛИВ) ---
@router.callback_query(F.data == "siphon_start")
async def siphon_init(c: CallbackQuery, state: FSMContext):
    if not await db.check_sub_bool(c.from_user.id): return await c.answer("Нет подписки!", True)
    if c.from_user.id not in W_POOL: return await c.answer("Сначала войдите (Кнопка Вход)!", True)
    await c.message.edit_text("🌪 <b>Загрузите .CSV файл</b> (результат .scan):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back")]]))
    await state.set_state(SiphonS.FILE)

@router.message(SiphonS.FILE, F.document)
async def siphon_file(m: Message, state: FSMContext):
    if not m.document.file_name.endswith('.csv'): return await m.answer("❌ Нужен файл .csv!")
    file = await bot.get_file(m.document.file_id)
    path = cfg.TEMP_DIR / f"siphon_{m.from_user.id}.csv"
    await bot.download_file(file.file_path, path)
    ids = []
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader, None)
            for r in reader:
                if r and r[0].isdigit(): ids.append(int(r[0]))
    except: return await m.answer("❌ Битая таблица.")
    finally: os.remove(path)
    
    if not ids: return await m.answer("❌ Файл пуст.")
    await state.update_data(targets=ids)
    await m.answer(f"✅ Загружено людей: {len(ids)}\n\n✍️ <b>Отправьте текст/фото для рассылки:</b>")
    await state.set_state(SiphonS.MSG)

@router.message(SiphonS.MSG)
async def siphon_msg(m: Message, state: FSMContext):
    await state.update_data(msg_text=m.text or m.caption or "Привет!")
    data = await state.get_data()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 ЗАПУСТИТЬ", callback_data="siphon_run"), InlineKeyboardButton(text="❌ Отмена", callback_data="back")]])
    await m.answer(f"🌪 <b>Подтверждение:</b>\nЦелей: {len(data['targets'])}\n\nНачать рассылку?", reply_markup=kb)
    await state.set_state(SiphonS.CONFIRM)

@router.callback_query(F.data == "siphon_run", SiphonS.CONFIRM)
async def siphon_exec(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    worker = W_POOL.get(c.from_user.id)
    if not worker: return await c.answer("Воркер отключился", True)
    await c.message.edit_text("🚀 <b>Рассылка запущена!</b> (Смотрите ЛС)")
    asyncio.create_task(run_siphon(c.from_user.id, worker, data['targets'], data['msg_text']))
    await state.clear()

async def run_siphon(uid, worker, targets, text):
    ok, fail = 0, 0
    for tid in targets:
        try:
            await worker.client.send_message(tid, text)
            ok += 1
            await asyncio.sleep(random.randint(4, 10)) # Анти-флуд
        except: fail += 1
    try: await bot.send_message(uid, f"✅ <b>Рассылка завершена</b>\n✅ Успешно: {ok}\n❌ Ошибок: {fail}")
    except: pass

# --- ADMIN ---
@router.callback_query(F.data == "adm")
async def adm(c: CallbackQuery): await c.message.edit_text("👑 Админ-панель:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Создать Промокод", callback_data="mk_p")]]))
@router.callback_query(F.data == "mk_p")
async def mk_p(c: CallbackQuery, state: FSMContext): await c.message.answer("На сколько дней?"); await state.set_state(AdminS.PD)
@router.message(AdminS.PD)
async def mk_pd(m: Message, state: FSMContext): await state.update_data(d=int(m.text)); await m.answer("Сколько активаций?"); await state.set_state(AdminS.PA)
@router.message(AdminS.PA)
async def mk_pa(m: Message, state: FSMContext): 
    d=await state.get_data(); c=await db.create_promo(d['d'], int(m.text)); await m.answer(f"Код: <code>{c}</code>")
    await state.clear()

# --- MAIN LOOP ---
async def main():
    await db.init()
    count = 0
    # Авто-запуск сохраненных сессий
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub_bool(uid):
                w = Worker(uid)
                if await w.start(): 
                    W_POOL[uid] = w
                    count += 1
        except Exception: pass
            
    logger.info(f"🔥 StatPro ELITE v71.0 Запущен. Активных воркеров: {count}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
