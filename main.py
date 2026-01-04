#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro v75.0 - TITANIUM EDITION (FULL)
------------------------------------------
Архитектура: Monolith / Async IO
Статус: PRODUCTION
Язык: Русский (Полная локализация)

Особенности:
1. 🧠 AI Core: Ротация актуальных провайдеров 2025-2026 (Blackbox, Airforce, Pollinations).
2. 🌪 Siphon Kamikaze: Агрессивная рассылка с игнорированием ошибок приватности.
3. 📂 Universal Parser: Читает ID из любых файлов (CSV/TXT/LOG/HTML).
4. 🔐 Full Auth: Поддержка 2FA (Облачный пароль) и QR-кода.
5. 📊 Report: Умный анализ топиков с авто-определением контекста.
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
from typing import Dict, List, Optional, Union, Set
from dataclasses import dataclass
from datetime import datetime

# --- ВНЕШНИЕ БИБЛИОТЕКИ ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    CallbackQuery, 
    Message, 
    BufferedInputFile, 
    FSInputFile
)
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties

from telethon import TelegramClient, events, types, functions
from telethon.errors import (
    SessionPasswordNeededError, 
    FloodWaitError, 
    UserPrivacyRestrictedError, 
    UserDeactivatedError, 
    PeerIdInvalidError, 
    ChatWriteForbiddenError,
    RPCError
)

# --- ИИ ЯДРО (G4F) ---
try:
    from g4f.client import AsyncClient
    import g4f
    # Отключаем отладочный мусор в консоли
    g4f.debug.logging = False
except ImportError:
    print("⚠️ Библиотеки AI не найдены. Устанавливаю...")
    os.system("pip install -U g4f[all] curl_cffi aiohttp")
    from g4f.client import AsyncClient
    import g4f

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ СИСТЕМЫ
# =========================================================================

@dataclass
class Config:
    """
    Класс конфигурации. Загружает переменные окружения и настраивает пути.
    """
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    
    # Системные пути
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_titanium.db"
    TEMP_DIR: Path = BASE_DIR / "temp"
    
    # Эмуляция устройства (iOS)
    DEVICE_MODEL: str = "iPhone 15 Pro Max"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"
    LANG_CODE: str = "ru"
    SYSTEM_LANG_CODE: str = "ru-RU"

    def __post_init__(self):
        """Проверка и создание необходимых директорий"""
        try:
            self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
            self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"❌ Ошибка создания папок: {e}")
            
        if not all([self.BOT_TOKEN, self.API_ID, self.API_HASH]):
            print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не заполнены переменные окружения!")
            print("Убедитесь, что BOT_TOKEN, API_ID и API_HASH установлены.")
            sys.exit(1)

cfg = Config()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("TITAN")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ (SQLite Async)
# =========================================================================

class Database:
    """
    Асинхронный менеджер базы данных.
    Использует WAL-журналирование для высокой производительности.
    """
    def __init__(self): 
        self.path = cfg.DB_PATH

    def get_conn(self): 
        return aiosqlite.connect(self.path)

    async def init(self):
        """Инициализация таблиц и индексов"""
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            
            # Таблица пользователей
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    username TEXT, 
                    sub_end INTEGER, 
                    joined_at INTEGER
                )
            """)
            # Индекс для быстрого поиска подписок
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
            logger.info("✅ База данных успешно инициализирована")

    async def check_sub_bool(self, uid: int) -> bool:
        """Проверка наличия активной подписки"""
        if uid == cfg.ADMIN_ID: 
            return True
            
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                if not r or r[0] is None:
                    return False
                return r[0] > int(time.time())

    async def upsert_user(self, uid: int, uname: str):
        """Создание или обновление пользователя"""
        now = int(time.time())
        uname = uname or "Unknown"
        async with self.get_conn() as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, 0, ?)", 
                (uid, uname, now)
            )
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def use_promo(self, uid: int, code: str) -> int:
        """Активация промокода"""
        code = code.strip()
        async with self.get_conn() as db:
            # Проверка кода
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: 
                    return 0
                days = r[0]
            
            # Списание активации
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            
            # Гарантируем, что юзер есть в базе
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, sub_end, joined_at) VALUES (?, 0, ?)", 
                (uid, int(time.time()))
            )
            
            # Расчет нового времени
            now = int(time.time())
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c2:
                row = await c2.fetchone()
                curr_end = row[0] if (row and row[0]) else 0
            
            # Если подписка активна - продлеваем, если нет - ставим от текущего момента
            new_end = (curr_end if curr_end > now else now) + (days * 86400)
            
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
            
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        """Создание нового промокода админом"""
        code = f"TITAN-{random.randint(100,999)}-{random.randint(1000,9999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code
    
    async def get_user_info(self, uid: int):
        """Получение полной инфо о пользователе"""
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end, joined_at FROM users WHERE user_id = ?", (uid,)) as c:
                return await c.fetchone()
    
    async def get_all_users(self):
        """Получение списка всех ID пользователей"""
        async with self.get_conn() as db:
            async with db.execute("SELECT user_id FROM users") as c:
                return [row[0] for row in await c.fetchall()]

db = Database()

# =========================================================================
# 🧠 AI ENGINE (РОТАЦИЯ ПРОВАЙДЕРОВ)
# =========================================================================

async def ask_gpt_safe(system_prompt: str, user_content: str) -> str:
    """
    Безопасный запрос к ИИ с автоматической ротацией провайдеров.
    Использует актуальные на 2026 год бесплатные эндпоинты.
    """
    client = AsyncClient()
    
    # Список провайдеров в порядке приоритета
    providers = [
        g4f.Provider.Blackbox,       # Самый стабильный для кода и коротких ответов
        g4f.Provider.PollinationsAI, # Отличный текстовик, без ключей
        g4f.Provider.DeepInfra,      # Мощный, но иногда требует капчу (библиотека обходит)
        g4f.Provider.Airforce,       # Новый игрок
        g4f.Provider.DarkAI          # Резерв
    ]

    last_error = ""

    for provider in providers:
        try:
            # logger.info(f"AI: Пробую провайдера {provider.__name__}...")
            response = await client.chat.completions.create(
                model="gpt-4o",
                provider=provider,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ]
            )
            result = response.choices[0].message.content
            
            # Проверка на пустой ответ
            if result and len(result.strip()) > 0:
                return result
                
        except Exception as e:
            last_error = str(e)
            # logger.warning(f"AI: Провайдер {provider.__name__} ошибка: {e}")
            continue
            
    return f"❌ Ошибка ИИ: Все каналы перегружены. ({last_error[:50]})"

# =========================================================================
# 🦾 WORKER (ЮЗЕРБОТ)
# =========================================================================

class Worker:
    """
    Класс управления Telethon клиентом.
    Реализует логику воркера, который работает параллельно с ботом.
    """
    def __init__(self, uid: int):
        self.uid = uid
        self.client: Optional[TelegramClient] = None
        self.spam_task: Optional[asyncio.Task] = None

    def _get_client(self, path):
        """Создает экземпляр клиента с правильными заголовками"""
        return TelegramClient(
            str(path), 
            cfg.API_ID, 
            cfg.API_HASH, 
            device_model=cfg.DEVICE_MODEL, 
            system_version=cfg.SYSTEM_VERSION, 
            app_version=cfg.APP_VERSION,
            lang_code=cfg.LANG_CODE,
            system_lang_code=cfg.SYSTEM_LANG_CODE,
            # ВАЖНО: Отключает строгое ожидание обновлений, лечит TimestampOutdatedError
            sequential_updates=False 
        )

    async def start(self) -> bool:
        """
        Запускает клиента в неблокирующем режиме.
        Возвращает True, если запуск успешен.
        """
        self.client = self._get_client(cfg.SESSION_DIR / f"session_{self.uid}")
        try:
            await self.client.connect()
            
            # Проверка авторизации
            if not await self.client.is_user_authorized():
                logger.warning(f"Воркер {self.uid} не авторизован")
                return False
            
            # Регистрация хендлеров
            self._bind_handlers()
            
            # Запуск цикла обработки событий в фоне (НЕ БЛОКИРУЕТ БОТА)
            asyncio.create_task(self._run_keep_alive())
            
            logger.info(f"Воркер {self.uid} успешно запущен")
            return True
        except Exception as e:
            logger.error(f"Ошибка запуска воркера {self.uid}: {e}")
            return False

    async def _run_keep_alive(self):
        """Фоновая задача для поддержания соединения"""
        while True:
            try:
                await self.client.run_until_disconnected()
            except Exception as e:
                logger.warning(f"Воркер {self.uid} потерял соединение: {e}. Реконнект через 5 сек...")
                await asyncio.sleep(5)
                try: 
                    await self.client.connect()
                except: 
                    pass
            
            # Если после дисконнекта сессия умерла - выходим
            if not await self.client.is_user_authorized():
                logger.error(f"Воркер {self.uid} разлогинен. Остановка.")
                break

    def _bind_handlers(self):
        """Привязка всех событий (команд) к клиенту"""
        cl = self.client

        # --- КОМАНДА .g (Генерация ответа ИИ) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.g(?: |$)(.*)'))
        async def handler_quiz(e):
            await e.edit("⚡️ <b>Думаю...</b>", parse_mode='html')
            
            # Извлечение вопроса (из текста или реплая)
            question = e.pattern_match.group(1)
            if not question and e.is_reply:
                reply = await e.get_reply_message()
                question = reply.text or reply.caption or ""
            
            if not question: 
                return await e.edit("❌ <b>Ошибка:</b> Пустой запрос.", parse_mode='html')
            
            # Системный промпт
            sys_prompt = "Ты помощник для викторин. Твоя цель: дать ТОЛЬКО правильный ответ. Максимально коротко (1-3 слова). Без вводных фраз."
            
            answer = await ask_gpt_safe(sys_prompt, question)
            await e.edit(f"<b>{answer}</b>", parse_mode='html')

        # --- КОМАНДА .report (Анализ логов) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.report$'))
        async def handler_report(e):
            await e.edit("🕵️‍♂️ <b>Сбор данных...</b>", parse_mode='html')
            
            # Определение контекста (топик или чат)
            topic_id = None
            if e.reply_to:
                # Пытаемся взять ID начала ветки (для форумов)
                topic_id = e.reply_to.reply_to_top_id or e.reply_to.reply_to_msg_id

            keywords = ['айти', 'вбив', 'номер', 'код', 'встал', 'слет', 'сек', 'ща', 'готово', 'сдох', 'взял', 'отстоял']
            logs = []
            
            try:
                # Собираем сообщения
                async for m in cl.iter_messages(e.chat_id, limit=1000, reply_to=topic_id):
                    if m.text and any(k in m.text.lower() for k in keywords):
                        ts = m.date.strftime("%H:%M")
                        name = m.sender.first_name if m.sender else "User"
                        logs.append(f"[{ts}] {name}: {m.text}")
            except Exception as ex:
                return await e.edit(f"❌ Ошибка доступа к чату: {ex}")

            if not logs: 
                return await e.edit("❌ <b>Логи пусты.</b> Нет ключевых слов за последние 1000 сообщений.", parse_mode='html')
            
            # Формируем промпт
            logs = logs[::-1] # Разворачиваем (старые -> новые)
            logs_text = "\n".join(logs)
            
            prompt = """
            Ты аналитик логов. Твоя задача - найти сессии работы с номерами.
            Логика статусов:
            1. Старт работы: слова "айти", "вбив", "взял".
            2. Успех (✅): Если прошло >35 минут от старта и НЕ БЫЛО слов "слет", "бан", "сдох".
            3. Провал (❌): Если встретились слова "слет", "бан", "сдох".
            
            Верни результат СТРОГО в формате JSON списка объектов:
            [{"num": "номер телефона", "time": "время в минутах", "status": "✅" или "❌"}]
            Никакого лишнего текста.
            """
            
            await e.edit(f"🧠 <b>Анализ {len(logs)} строк...</b>", parse_mode='html')
            res = await ask_gpt_safe(prompt, logs_text)
            
            try:
                # Пытаемся найти JSON массив в ответе ИИ
                json_match = re.search(r'\[.*\]', res, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group())
                    
                    report_text = "📊 <b>ОТЧЕТ ПО СМЕНЕ:</b>\n\n"
                    ok_count = 0
                    
                    for item in data:
                        status = item.get('status', '❓')
                        report_text += f"📱 <code>{item.get('num', 'Н/Д')}</code>\n⏱ <b>{item.get('time', '0')} мин</b> | {status}\n\n"
                        if "✅" in status: 
                            ok_count += 1
                    
                    report_text += f"🏆 <b>Всего успешно: {ok_count} шт.</b>"
                    await e.edit(report_text, parse_mode='html')
                else:
                    raise ValueError("JSON не найден")
            except Exception:
                # Если ИИ вернул текст, выводим как есть
                await e.edit(f"📝 <b>Текстовый отчет:</b>\n\n{res}", parse_mode='html')

        # --- КОМАНДА .scan (Сбор базы) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.scan$'))
        async def handler_scan(e):
            await e.edit("🔎 <b>Сканирую чат (до 5000 сообщений)...</b>", parse_mode='html')
            
            users = {}
            count = 0
            
            try:
                # Сканируем сообщения для сбора активных юзеров
                async for m in cl.iter_messages(e.chat_id, limit=5000):
                    count += 1
                    if m.sender and isinstance(m.sender, types.User) and not m.sender.bot:
                        if m.sender_id not in users:
                            fname = m.sender.first_name or ""
                            lname = m.sender.last_name or ""
                            full_name = f"{fname} {lname}".strip()
                            username = m.sender.username or ""
                            users[m.sender_id] = [username, full_name]
            except Exception as ex:
                logger.warning(f"Scan warning: {ex}")
            
            # Генерация CSV
            out = io.StringIO()
            writer = csv.writer(out)
            writer.writerow(["ID", "Username", "Name"])
            for uid, info in users.items():
                writer.writerow([uid, info[0], info[1]])
            
            out.seek(0)
            # UTF-8-SIG важно для Excel в Windows
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig'))
            bio.name = f"Base_{e.chat_id}.csv"
            
            await cl.send_file("me", bio, caption=f"✅ <b>Скан завершен</b>\n👥 Уникальных пользователей: {len(users)}\n📂 Сохранено в Избранное", parse_mode='html')
            await e.edit(f"✅ <b>Готово!</b> Собрано: {len(users)} чел.", parse_mode='html')

        # --- КОМАНДА .spam (Спамер) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.(s|spam)\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def handler_spam(e):
            # Парсинг аргументов
            text = e.pattern_match.group(2)
            count = int(e.pattern_match.group(3))
            delay = float(e.pattern_match.group(4))
            
            await e.delete()
            
            async def spam_loop():
                for i in range(count):
                    try: 
                        await cl.send_message(e.chat_id, text)
                        await asyncio.sleep(delay)
                    except FloodWaitError as f:
                        # Если словили флуд - ждем
                        await asyncio.sleep(f.seconds + 5)
                    except Exception: 
                        break
            
            self.spam_task = asyncio.create_task(spam_loop())

        # --- КОМАНДА .stop (Остановка спама) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def handler_stop(e):
            if self.spam_task: 
                self.spam_task.cancel()
                self.spam_task = None
                await e.edit("🛑 <b>Задача остановлена.</b>", parse_mode='html')
            else:
                await e.edit("⚠️ Нет активных задач.", parse_mode='html')

        # --- КОМАНДА .all (Тег всех) ---
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.all(?:\s+(.+))?'))
        async def handler_all(e):
            await e.delete()
            text = e.pattern_match.group(1) or "Внимание!"
            try:
                participants = await cl.get_participants(e.chat_id)
                chunk = []
                for p in participants:
                    if not p.bot and not p.deleted:
                        # Используем невидимый символ для тега
                        chunk.append(f"<a href='tg://user?id={p.id}'>\u200b</a>")
                        if len(chunk) >= 5:
                            await cl.send_message(e.chat_id, text + "".join(chunk), parse_mode='html')
                            chunk = []
                            await asyncio.sleep(2)
            except Exception:
                pass

# Глобальный пул воркеров
W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI (AIOGRAM ИНТЕРФЕЙС)
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# --- СОСТОЯНИЯ (FSM) ---
class AuthStates(StatesGroup): 
    PHONE = State()
    CODE = State()
    PASSWORD = State()

class PromoStates(StatesGroup): 
    CODE = State()

class SiphonStates(StatesGroup): 
    FILE = State()
    MSG = State()
    CONFIRM = State()

class AdminStates(StatesGroup): 
    DAYS = State()
    COUNT = State()
    BROADCAST = State()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ UI ---

def kb_main(uid: int):
    """Главное меню"""
    btns = [
        [InlineKeyboardButton(text="🌪 ПЕРЕЛИВ (Siphon)", callback_data="siphon_start")],
        [InlineKeyboardButton(text="📚 Инфо / Команды", callback_data="help"), InlineKeyboardButton(text="👤 Мой Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🔑 Вход в аккаунт (Auth)", callback_data="auth")]
    ]
    if uid == cfg.ADMIN_ID:
        btns.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="adm")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def kb_numpad():
    """Клавиатура для ввода кода"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1", callback_data="n_1"), InlineKeyboardButton(text="2", callback_data="n_2"), InlineKeyboardButton(text="3", callback_data="n_3")],
        [InlineKeyboardButton(text="4", callback_data="n_4"), InlineKeyboardButton(text="5", callback_data="n_5"), InlineKeyboardButton(text="6", callback_data="n_6")],
        [InlineKeyboardButton(text="7", callback_data="n_7"), InlineKeyboardButton(text="8", callback_data="n_8"), InlineKeyboardButton(text="9", callback_data="n_9")],
        [InlineKeyboardButton(text="🔙 Стереть", callback_data="n_del"), InlineKeyboardButton(text="0", callback_data="n_0"), InlineKeyboardButton(text="✅ Ввод", callback_data="n_go")]
    ])

# --- ХЕНДЛЕРЫ: СТАРТ И МЕНЮ ---

@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(m.from_user.id, m.from_user.username)
    await m.answer(
        f"💎 <b>StatPro TITANIUM v75.0</b>\n\n"
        f"Добро пожаловать, <b>{m.from_user.first_name}</b>!\n"
        f"Это самая мощная версия системы для работы с Telegram.",
        reply_markup=kb_main(m.from_user.id)
    )

@router.callback_query(F.data == "help")
async def cb_help(c: CallbackQuery):
    text = (
        "📚 <b>СПРАВКА ПО КОМАНДАМ (ЮЗЕРБОТ):</b>\n\n"
        "⚡️ <code>.g [вопрос]</code> — Мгновенный ответ ИИ\n"
        "🕵️‍♂️ <code>.report</code> — Анализ логов чата/топика (реплай)\n"
        "🧬 <code>.scan</code> — Спарсить всех участников чата в файл\n"
        "🚀 <code>.spam [текст] [кол-во] [сек]</code> — Спаммер\n"
        "🛑 <code>.stop</code> — Остановить текущую задачу\n"
        "📢 <code>.all [текст]</code> — Тегнуть всех (скрытно)\n\n"
        "<i>Для работы этих команд нужно войти в аккаунт через кнопку 🔑 Вход.</i>"
    )
    await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back")]]))

@router.callback_query(F.data == "back")
async def cb_back(c: CallbackQuery, state: FSMContext):
    await c.message.delete()
    await cmd_start(c.message, state)

@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    info = await db.get_user_info(c.from_user.id)
    
    # Статус подписки
    if info and info[0] and info[0] > time.time():
        days_left = int((info[0] - time.time()) / 86400)
        sub_status = f"🟢 <b>АКТИВНА</b> (осталось {days_left} дн.)"
    else:
        sub_status = "🔴 <b>НЕАКТИВНА</b>"
    
    # Статус воркера
    worker = W_POOL.get(c.from_user.id)
    if worker and worker.client and await worker.client.is_user_authorized():
        worker_status = "🟢 <b>ПОДКЛЮЧЕН</b>"
    else:
        worker_status = "🔴 <b>ОТКЛЮЧЕН</b>"

    text = (
        f"👤 <b>ЛИЧНЫЙ КАБИНЕТ</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🆔 ID: <code>{c.from_user.id}</code>\n"
        f"💎 Подписка: {sub_status}\n"
        f"🔌 Статус бота: {worker_status}\n"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎟 Активировать промокод", callback_data="promo")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back")]
    ])
    await c.message.edit_text(text, reply_markup=kb)

# --- ХЕНДЛЕРЫ: ПРОМОКОДЫ ---

@router.callback_query(F.data == "promo")
async def cb_promo(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("🎟 <b>Введите ваш промокод:</b>")
    await state.set_state(PromoStates.CODE)

@router.message(PromoStates.CODE)
async def state_promo(m: Message, state: FSMContext):
    days = await db.use_promo(m.from_user.id, m.text)
    if days > 0:
        await m.answer(f"✅ <b>Успешно!</b> Подписка продлена на {days} дней.")
        await cmd_start(m, state)
    else:
        await m.answer("❌ <b>Ошибка:</b> Неверный код или лимит исчерпан.")
    await state.clear()

# --- ХЕНДЛЕРЫ: SIPHON (KAMIKAZE EDITION) ---

@router.callback_query(F.data == "siphon_start")
async def cb_siphon_start(c: CallbackQuery, state: FSMContext):
    # Проверки
    if not await db.check_sub_bool(c.from_user.id):
        return await c.answer("❌ Требуется активная подписка!", True)
    
    if c.from_user.id not in W_POOL:
        return await c.answer("❌ Сначала войдите в аккаунт (Кнопка Вход)!", True)
    
    await c.message.edit_text(
        "📂 <b>УНИВЕРСАЛЬНЫЙ ЗАГРУЗЧИК</b>\n\n"
        "Отправьте мне <b>ЛЮБОЙ файл</b> (.txt, .csv, .log, .json).\n"
        "Я автоматически найду в нем все ID пользователей Telegram.\n"
        "<i>Старые базы, кривые форматы - читаю всё.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="back")]])
    )
    await state.set_state(SiphonStates.FILE)

@router.message(SiphonStates.FILE, F.document)
async def state_siphon_file(m: Message, state: FSMContext):
    temp_path = cfg.TEMP_DIR / f"siphon_{m.from_user.id}.tmp"
    
    try:
        # Скачиваем файл
        await bot.download(m.document, destination=temp_path)
        
        # МАГИЧЕСКИЙ ПАРСЕР: Читаем как текст, игнорируя ошибки кодировки
        content = ""
        with open(temp_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
        # Ищем ID регуляркой (последовательности цифр от 7 до 20 символов)
        # Это захватит все UserID, но отсеет короткие числа
        raw_ids = re.findall(r'\b\d{7,20}\b', content)
        
        # Убираем дубликаты
        unique_ids = list(set([int(x) for x in raw_ids]))
        
        if not unique_ids:
            return await m.answer("❌ <b>Ошибка:</b> В файле не найдено ни одного ID.")
            
        await state.update_data(targets=unique_ids)
        await m.answer(
            f"✅ <b>Файл обработан!</b>\n"
            f"Найдено уникальных целей: <b>{len(unique_ids)}</b>\n\n"
            f"✍️ <b>Теперь отправьте текст или фото (с подписью) для рассылки:</b>"
        )
        await state.set_state(SiphonStates.MSG)
        
    except Exception as e:
        await m.answer(f"❌ Ошибка обработки файла: {e}")
    finally:
        # Удаляем времянку
        if os.path.exists(temp_path):
            os.remove(temp_path)

@router.message(SiphonStates.MSG)
async def state_siphon_msg(m: Message, state: FSMContext):
    # Сохраняем сообщение целиком (текст или медиа)
    await state.update_data(msg_content=m.text or m.caption or "Привет")
    # Если это медиа, можно усложнить, но пока берем текст для простоты
    # В этой версии передаем только текст для стабильности камикадзе-режима
    
    data = await state.get_data()
    targets_count = len(data['targets'])
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💀 ЗАПУСТИТЬ KAMIKAZE", callback_data="run_kamikaze")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back")]
    ])
    
    await m.answer(
        f"🔥 <b>ГОТОВНОСТЬ К ЗАПУСКУ</b>\n"
        f"Целей: <b>{targets_count}</b>\n\n"
        f"⚠️ <b>Внимание:</b> Включен режим KAMIKAZE.\n"
        f"Бот будет игнорировать ошибки (бан, удален, приват) и идти до конца.\n"
        f"Подтверждаете?",
        reply_markup=kb
    )
    await state.set_state(SiphonStates.CONFIRM)

@router.callback_query(F.data == "run_kamikaze", SiphonStates.CONFIRM)
async def cb_siphon_run(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    worker = W_POOL.get(c.from_user.id)
    
    if not worker or not worker.client:
        return await c.answer("❌ Воркер отключился! Перезайдите.", True)
        
    await c.message.edit_text(
        "🚀 <b>Рассылка запущена в фоне!</b>\n"
        "Я пришлю отчет, когда закончу.\n"
        "Можете пользоваться ботом дальше."
    )
    
    # Запуск фоновой задачи
    asyncio.create_task(
        task_kamikaze_siphon(
            c.from_user.id, 
            worker, 
            data['targets'], 
            data['msg_content']
        )
    )
    await state.clear()

async def task_kamikaze_siphon(uid: int, w: Worker, targets: List[int], text: str):
    """
    Асинхронная задача рассылки.
    Режим: KAMIKAZE (Ignore errors, continue pumping).
    """
    ok_count = 0
    fail_count = 0
    
    for target_id in targets:
        try:
            # 1. Пытаемся получить сущность (для пробива незнакомых ID)
            try:
                entity = await w.client.get_input_entity(target_id)
            except ValueError:
                # Если не нашли - пробуем слать просто на ID (иногда работает)
                entity = target_id
            except Exception:
                # Если совсем никак - пропускаем
                fail_count += 1
                continue

            # 2. Отправка
            await w.client.send_message(entity, text)
            ok_count += 1
            
            # 3. Агрессивная задержка (1.5 - 4 секунды)
            await asyncio.sleep(random.uniform(1.5, 4.0))
            
        except FloodWaitError as e:
            # Если Telegram дал временный бан - ждем и продолжаем (не сдаемся!)
            # logger.warning(f"FloodWait {e.seconds}s. Waiting...")
            await asyncio.sleep(e.seconds + 2)
            
        except (UserPrivacyRestrictedError, UserDeactivatedError, PeerIdInvalidError, ChatWriteForbiddenError):
            # Эти ошибки игнорируем молча (человек запретил писать или удалился)
            fail_count += 1
            
        except Exception as e:
            # Прочие ошибки
            # logger.error(f"Siphon Error: {e}")
            fail_count += 1
            
    # Финальный отчет
    try:
        await bot.send_message(
            uid,
            f"🏁 <b>РАССЫЛКА ЗАВЕРШЕНА</b>\n\n"
            f"✅ <b>Доставлено:</b> {ok_count}\n"
            f"🗑 <b>Пропущено:</b> {fail_count}\n"
            f"(Приватность, баны, удаленные аккаунты)"
        )
    except:
        pass

# --- ХЕНДЛЕРЫ: АВТОРИЗАЦИЯ (FULL FLOW) ---

@router.callback_query(F.data == "auth")
async def cb_auth(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id):
        return await c.answer("❌ Нет активной подписки!", True)
        
    await c.message.edit_text(
        "🔐 <b>АВТОРИЗАЦИЯ</b>\n"
        "Выберите удобный способ входа:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📸 Через QR-код (Быстро)", callback_data="qr")],
            [InlineKeyboardButton(text="📱 По номеру телефона", callback_data="ph")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back")]
        ])
    )

@router.callback_query(F.data == "ph")
async def cb_auth_phone(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 <b>Введите номер телефона:</b>\n(Формат: 79991234567)")
    await state.set_state(AuthStates.PHONE)

@router.message(AuthStates.PHONE)
async def state_auth_phone(m: Message, state: FSMContext):
    uid = m.from_user.id
    phone = m.text.strip().replace("+", "").replace(" ", "")
    
    # Создаем временного клиента для логина
    w = Worker(uid)
    w.client = TelegramClient(str(cfg.SESSION_DIR / f"login_{uid}"), cfg.API_ID, cfg.API_HASH)
    await w.client.connect()
    
    try:
        sent = await w.client.send_code_request(phone)
        
        # Сохраняем данные во временное хранилище
        await state.update_data(phone=phone, hash=sent.phone_code_hash, temp_worker=w, code_input="")
        
        await m.answer(
            f"📩 <b>Код отправлен на {phone}</b>\n"
            f"Введите код, используя кнопки ниже:",
            reply_markup=kb_numpad()
        )
        await state.set_state(AuthStates.CODE)
        
    except Exception as e:
        await w.client.disconnect()
        await m.answer(f"❌ <b>Ошибка:</b> {e}")
        await state.clear()

@router.callback_query(F.data.startswith("n_"), AuthStates.CODE)
async def state_auth_numpad(c: CallbackQuery, state: FSMContext):
    action = c.data.split("_")[1]
    data = await state.get_data()
    current_code = data.get("code_input", "")
    w: Worker = data.get("temp_worker") # Получаем объект воркера
    
    if action == "del":
        current_code = current_code[:-1]
    elif action == "go":
        if not current_code: return await c.answer("Введите код!", True)
        
        await c.message.edit_text("⏳ <b>Проверка кода...</b>")
        try:
            # Попытка входа
            await w.client.sign_in(phone=data['phone'], code=current_code, phone_code_hash=data['hash'])
            
            # Если успешно - переносим сессию в основную папку
            await w.client.disconnect()
            os.rename(
                cfg.SESSION_DIR / f"login_{c.from_user.id}.session", 
                cfg.SESSION_DIR / f"session_{c.from_user.id}.session"
            )
            
            # Запускаем боевого воркера
            real_worker = Worker(c.from_user.id)
            if await real_worker.start():
                W_POOL[c.from_user.id] = real_worker
                await c.message.answer("✅ <b>Успешный вход!</b> Воркер запущен.")
                await cmd_start(c.message, state)
            else:
                await c.message.answer("❌ Ошибка запуска воркера. Попробуйте снова.")
                
            await state.clear()
            return
            
        except SessionPasswordNeededError:
            await c.message.answer("🔒 <b>Требуется облачный пароль (2FA):</b>\nВведите его текстом:")
            await state.set_state(AuthStates.PASSWORD)
            return
            
        except Exception as e:
            await w.client.disconnect()
            await c.message.answer(f"❌ Ошибка входа: {e}")
            await state.clear()
            return
            
    else:
        current_code += action
    
    # Обновляем состояние и UI
    await state.update_data(code_input=current_code)
    try:
        await c.message.edit_text(f"Код: <b>{current_code}</b>", reply_markup=kb_numpad())
    except: pass

@router.message(AuthStates.PASSWORD)
async def state_auth_password(m: Message, state: FSMContext):
    data = await state.get_data()
    w: Worker = data.get("temp_worker")
    
    try:
        await w.client.sign_in(password=m.text)
        await w.client.disconnect()
        
        # Переименовываем сессию
        os.rename(
            cfg.SESSION_DIR / f"login_{m.from_user.id}.session", 
            cfg.SESSION_DIR / f"session_{m.from_user.id}.session"
        )
        
        real_worker = Worker(m.from_user.id)
        if await real_worker.start():
            W_POOL[m.from_user.id] = real_worker
            await m.answer("✅ <b>Пароль принят!</b> Воркер активен.")
            await cmd_start(m, state)
        else:
            await m.answer("❌ Ошибка запуска.")
            
    except Exception as e:
        await m.answer(f"❌ Неверный пароль: {e}")
        await w.client.disconnect()
        
    await state.clear()

@router.callback_query(F.data == "qr")
async def cb_auth_qr(c: CallbackQuery, state: FSMContext):
    uid = c.from_user.id
    w = Worker(uid)
    w.client = TelegramClient(str(cfg.SESSION_DIR / f"session_{uid}"), cfg.API_ID, cfg.API_HASH)
    await w.client.connect()
    
    try:
        qr_login = await w.client.qr_login()
        
        # Генерация картинки QR
        qr_img = io.BytesIO()
        qrcode.make(qr_login.url).save(qr_img, "PNG")
        qr_img.seek(0)
        
        msg = await c.message.answer_photo(
            BufferedInputFile(qr_img.read(), "qr.png"),
            caption="📸 <b>Отсканируйте QR код в Telegram</b>\n(Настройки -> Устройства -> Подключить устройство)"
        )
        
        # Ждем сканирования
        await qr_login.wait(60)
        await msg.delete()
        await w.client.disconnect()
        
        # Запуск
        real_worker = Worker(uid)
        if await real_worker.start():
            W_POOL[uid] = real_worker
            await c.message.answer("✅ <b>Успешный вход по QR!</b>")
        
    except asyncio.TimeoutError:
        await msg.delete()
        await c.message.answer("⌛️ <b>Время действия QR истекло.</b>")
    except Exception as e:
        await c.message.answer(f"❌ Ошибка QR: {e}")
    finally:
        await state.clear()

# --- ХЕНДЛЕРЫ: АДМИНКА ---

@router.callback_query(F.data == "adm")
async def cb_admin(c: CallbackQuery):
    await c.message.edit_text(
        "👑 <b>Админ-панель</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Создать Промокод", callback_data="mk_p")]])
    )

@router.callback_query(F.data == "mk_p")
async def cb_mk_promo(c: CallbackQuery, state: FSMContext):
    await c.message.answer("Срок действия (дней)?")
    await state.set_state(AdminStates.DAYS)

@router.message(AdminStates.DAYS)
async def state_mk_days(m: Message, state: FSMContext):
    await state.update_data(days=int(m.text))
    await m.answer("Количество активаций?")
    await state.set_state(AdminStates.COUNT)

@router.message(AdminStates.COUNT)
async def state_mk_count(m: Message, state: FSMContext):
    data = await state.get_data()
    code = await db.create_promo(data['days'], int(m.text))
    await m.answer(f"✅ Промокод создан:\n<code>{code}</code>")
    await state.clear()

# =========================================================================
# 🚀 ЗАПУСК
# =========================================================================

async def main():
    """Точка входа"""
    # 1. Инит базы
    await db.init()
    
    # 2. Восстановление сессий
    restored_count = 0
    sessions = list(cfg.SESSION_DIR.glob("session_*.session"))
    
    logger.info(f"Найдено сессий: {len(sessions)}")
    
    for sess_file in sessions:
        try:
            # Извлекаем ID из имени файла session_12345.session
            uid = int(sess_file.stem.split("_")[1])
            
            # Проверяем подписку
            if await db.check_sub_bool(uid):
                w = Worker(uid)
                if await w.start():
                    W_POOL[uid] = w
                    restored_count += 1
        except Exception as e:
            logger.error(f"Ошибка восстановления сессии {sess_file}: {e}")
            
    logger.info(f"🔥 StatPro TITANIUM v75.0 Запущен! Активных воркеров: {restored_count}")
    
    # 3. Старт бота
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен пользователем.")
