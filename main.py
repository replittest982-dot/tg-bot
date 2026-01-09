#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
💎 StatPro TITANIUM v77.0 - ULTIMATE EDITION
---------------------------------------------
Объединённая версия: StatPro v75 + Titan Pro v76
Архитектура: Monolith / Async IO
Статус: PRODUCTION
Язык: Русский (Полная локализация)

Особенности:
1. 🧠 AI Core: Ротация провайдеров (Blackbox, Airforce, Pollinations)
2. 🌪 Siphon Kamikaze: Агрессивная рассылка с игнорированием ошибок
3. 📞 Number Processing: Автоматическая обработка номеров с кодами
4. 📂 Universal Parser: Читает ID из любых файлов
5. 🔐 Full Auth: Поддержка 2FA и QR-кода
6. 📊 Advanced Reports: CSV отчёты + AI анализ логов
7. 🎯 Smart Commands: .g, .report, .scan, .spam, .au, .u, .v и др.
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
from typing import Dict, List, Optional, Union, Set, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

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
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties

from telethon import TelegramClient, events, types, functions, Button
from telethon.errors import (
    SessionPasswordNeededError, 
    FloodWaitError, 
    UserPrivacyRestrictedError, 
    UserDeactivatedError, 
    PeerIdInvalidError, 
    ChatWriteForbiddenError,
    RPCError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError
)

# --- ИИ ЯДРО (G4F) ---
try:
    from g4f.client import AsyncClient
    import g4f
    g4f.debug.logging = False
except ImportError:
    print("⚠️ Библиотеки AI не найдены. Устанавливаю...")
    os.system("pip install -U g4f[all] curl_cffi aiohttp")
    from g4f.client import AsyncClient
    import g4f

# =========================================================================
# ⚙️ КОНФИГУРАЦИЯ СИСТЕМЫ
# =========================================================================

class NumberStatus(Enum):
    """Статусы обработки номеров"""
    WAITING = "waiting"
    CODE_SENT = "code_sent"
    CODE_RECEIVED = "code_received"
    PHOTO_REQUESTED = "photo_requested"
    PHOTO_RECEIVED = "photo_received"
    COMPLETED = "completed"
    FAILED = "failed"

class WorkerStatus(Enum):
    """Статусы воркера"""
    OFFLINE = "offline"
    ONLINE = "online"
    WORKING = "working"
    ERROR = "error"

@dataclass
class Config:
    """Класс конфигурации"""
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.environ.get("ADMIN_ID", "0"))
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")
    
    # Системные пути
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_titanium_v77.db"
    TEMP_DIR: Path = BASE_DIR / "temp"
    REPORTS_DIR: Path = BASE_DIR / "reports"
    
    # Лимиты
    MAX_WORKERS: int = 10
    FLOOD_WAIT_TIME: int = 60
    CODE_TIMEOUT: int = 300
    PHOTO_TIMEOUT: int = 600
    
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
            self.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
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
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler('titan_v77.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TITAN_V77")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ (SQLite Async) - РАСШИРЕННАЯ
# =========================================================================

class Database:
    """Асинхронный менеджер базы данных (объединённая схема)"""
    def __init__(self): 
        self.path = cfg.DB_PATH

    def get_conn(self): 
        return aiosqlite.connect(self.path)

    async def init(self):
        """Инициализация всех таблиц"""
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await db.execute("PRAGMA foreign_keys=ON")
            
            # Таблица пользователей (расширенная)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    username TEXT, 
                    first_name TEXT,
                    sub_end INTEGER, 
                    joined_at INTEGER,
                    total_operations INTEGER DEFAULT 0,
                    successful_operations INTEGER DEFAULT 0,
                    last_active INTEGER
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_sub ON users(sub_end)")
            
            # Таблица промокодов
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY, 
                    days INTEGER, 
                    activations INTEGER
                )
            """)
            
            # Таблица номеров (новая)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS numbers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    worker_id INTEGER,
                    status TEXT DEFAULT 'waiting',
                    created_at INTEGER NOT NULL,
                    code_sent_at INTEGER,
                    code_received_at INTEGER,
                    photo_requested_at INTEGER,
                    photo_received_at INTEGER,
                    completed_at INTEGER,
                    error_message TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_numbers_status ON numbers(status)")
            
            # Таблица логов операций
            await db.execute("""
                CREATE TABLE IF NOT EXISTS operation_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    number_id INTEGER,
                    worker_id INTEGER,
                    action TEXT,
                    details TEXT,
                    timestamp INTEGER
                )
            """)
            
            await db.commit()
            logger.info("✅ База данных v77 успешно инициализирована")

    # --- МЕТОДЫ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ ---
    
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

    async def upsert_user(self, uid: int, uname: str = None, fname: str = None):
        """Создание или обновление пользователя"""
        now = int(time.time())
        uname = uname or "Unknown"
        fname = fname or ""
        async with self.get_conn() as db:
            await db.execute(
                """INSERT OR IGNORE INTO users 
                   (user_id, username, first_name, sub_end, joined_at, last_active) 
                   VALUES (?, ?, ?, 0, ?, ?)""", 
                (uid, uname, fname, now, now)
            )
            await db.execute(
                "UPDATE users SET username = ?, first_name = ?, last_active = ? WHERE user_id = ?", 
                (uname, fname, now, uid)
            )
            await db.commit()

    async def use_promo(self, uid: int, code: str) -> int:
        """Активация промокода"""
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: 
                    return 0
                days = r[0]
            
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE activations <= 0")
            
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, sub_end, joined_at) VALUES (?, 0, ?)", 
                (uid, int(time.time()))
            )
            
            now = int(time.time())
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c2:
                row = await c2.fetchone()
                curr_end = row[0] if (row and row[0]) else 0
            
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
            async with db.execute(
                "SELECT sub_end, joined_at, total_operations, successful_operations FROM users WHERE user_id = ?", 
                (uid,)
            ) as c:
                return await c.fetchone()
    
    async def get_all_users(self):
        """Получение списка всех ID пользователей"""
        async with self.get_conn() as db:
            async with db.execute("SELECT user_id FROM users") as c:
                return [row[0] for row in await c.fetchall()]
    
    # --- МЕТОДЫ ДЛЯ НОМЕРОВ ---
    
    async def add_number(self, phone: str, user_id: int) -> bool:
        """Добавление номера в базу"""
        now = int(time.time())
        try:
            async with self.get_conn() as db:
                await db.execute("""
                    INSERT INTO numbers (phone, user_id, created_at, status)
                    VALUES (?, ?, ?, ?)
                """, (phone, user_id, now, NumberStatus.WAITING.value))
                await db.commit()
            logger.info(f"✅ Номер {phone} добавлен пользователем {user_id}")
            return True
        except aiosqlite.IntegrityError:
            logger.warning(f"⚠️ Номер {phone} уже существует")
            return False
        except Exception as e:
            logger.error(f"Ошибка добавления номера {phone}: {e}")
            return False

    async def get_available_number(self, worker_id: int) -> Optional[str]:
        """Получение свободного номера для обработки"""
        try:
            async with self.get_conn() as db:
                async with db.execute("""
                    SELECT phone, id FROM numbers 
                    WHERE status=? AND worker_id IS NULL
                    ORDER BY created_at ASC
                    LIMIT 1
                """, (NumberStatus.WAITING.value,)) as cursor:
                    row = await cursor.fetchone()
                    
                if row:
                    phone, number_id = row
                    await db.execute(
                        "UPDATE numbers SET worker_id=?, status=? WHERE id=?",
                        (worker_id, NumberStatus.PHOTO_REQUESTED.value, number_id)
                    )
                    await db.commit()
                    return phone
                return None
        except Exception as e:
            logger.error(f"Ошибка получения номера: {e}")
            return None

    async def update_number_status(self, phone: str, status: NumberStatus, 
                                   error_message: str = None):
        """Обновление статуса номера"""
        now = int(time.time())
        field_map = {
            NumberStatus.CODE_SENT: "code_sent_at",
            NumberStatus.CODE_RECEIVED: "code_received_at",
            NumberStatus.PHOTO_REQUESTED: "photo_requested_at",
            NumberStatus.PHOTO_RECEIVED: "photo_received_at",
            NumberStatus.COMPLETED: "completed_at"
        }
        
        async with self.get_conn() as db:
            time_field = field_map.get(status)
            if time_field:
                await db.execute(f"""
                    UPDATE numbers SET status=?, {time_field}=?, error_message=?
                    WHERE phone=?
                """, (status.value, now, error_message, phone))
            else:
                await db.execute("""
                    UPDATE numbers SET status=?, error_message=?
                    WHERE phone=?
                """, (status.value, error_message, phone))
            
            await db.commit()

    async def get_user_stats(self, user_id: int) -> Dict:
        """Получение статистики пользователя"""
        async with self.get_conn() as db:
            async with db.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
                    AVG(CASE WHEN completed_at IS NOT NULL 
                        THEN completed_at - created_at END) as avg_time
                FROM numbers WHERE user_id=?
            """, (user_id,)) as cursor:
                row = await cursor.fetchone()
            
        return {
            "total": row[0] or 0,
            "completed": row[1] or 0,
            "failed": row[2] or 0,
            "avg_time": row[3] or 0
        }

    async def get_report_data(self, days: int = 7) -> List[Tuple]:
        """Получение данных для отчёта"""
        timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        async with self.get_conn() as db:
            async with db.execute("""
                SELECT 
                    n.phone,
                    u.username,
                    n.status,
                    datetime(n.created_at, 'unixepoch', 'localtime') as created,
                    datetime(n.code_received_at, 'unixepoch', 'localtime') as code_time,
                    datetime(n.photo_received_at, 'unixepoch', 'localtime') as photo_time,
                    (n.completed_at - n.created_at) as work_duration,
                    n.error_message
                FROM numbers n
                LEFT JOIN users u ON n.user_id = u.user_id
                WHERE n.created_at >= ?
                ORDER BY n.created_at DESC
            """, (timestamp,)) as cursor:
                return await cursor.fetchall()

    async def cleanup_old_data(self, days: int = 30):
        """Очистка старых данных"""
        timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        async with self.get_conn() as db:
            await db.execute(
                "DELETE FROM numbers WHERE created_at < ? AND status IN ('completed', 'failed')",
                (timestamp,)
            )
            await db.commit()
        logger.info(f"🧹 Очищены данные старше {days} дней")

db = Database()

# =========================================================================
# 🧠 AI ENGINE (РОТАЦИЯ ПРОВАЙДЕРОВ)
# =========================================================================

async def ask_gpt_safe(system_prompt: str, user_content: str) -> str:
    """Безопасный запрос к ИИ с автоматической ротацией провайдеров"""
    client = AsyncClient()
    
    providers = [
        g4f.Provider.Blackbox,
        g4f.Provider.PollinationsAI,
        g4f.Provider.DeepInfra,
        g4f.Provider.Airforce,
        g4f.Provider.DarkAI
    ]

    last_error = ""

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
            result = response.choices[0].message.content
            
            if result and len(result.strip()) > 0:
                return result
                
        except Exception as e:
            last_error = str(e)
            continue
            
    return f"❌ Ошибка ИИ: Все каналы перегружены. ({last_error[:50]})"

# =========================================================================
# 🦾 WORKER (ЮЗЕРБОТ) - ОБЪЕДИНЁННАЯ ВЕРСИЯ
# =========================================================================

class Worker:
    """Класс управления Telethon клиентом с полным функционалом"""
    def __init__(self, uid: int):
        self.uid = uid
        self.client: Optional[TelegramClient] = None
        self.spam_task: Optional[asyncio.Task] = None
        self.task: Optional[asyncio.Task] = None
        self.status: WorkerStatus = WorkerStatus.OFFLINE
        
        # Для обработки номеров
        self.current_phone: Optional[str] = None
        self.waiting_for_code: bool = False
        self.waiting_for_photo: bool = False
        
        # Статистика
        self.processed_count: int = 0
        self.error_count: int = 0
        self.started_at: Optional[int] = None
        self.last_activity: Optional[int] = None

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
            sequential_updates=False,
            connection_retries=5,
            retry_delay=3
        )

    async def start(self) -> bool:
        """Запускает клиента в неблокирующем режиме"""
        self.client = self._get_client(cfg.SESSION_DIR / f"session_{self.uid}")
        try:
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                logger.warning(f"Воркер {self.uid} не авторизован")
                return False
            
            self._bind_handlers()
            
            asyncio.create_task(self._run_keep_alive())
            
            self.status = WorkerStatus.ONLINE
            self.started_at = int(datetime.now().timestamp())
            
            logger.info(f"✅ Воркер {self.uid} успешно запущен")
            return True
        except Exception as e:
            logger.error(f"Ошибка запуска воркера {self.uid}: {e}")
            self.status = WorkerStatus.ERROR
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
            
            if not await self.client.is_user_authorized():
                logger.error(f"Воркер {self.uid} разлогинен. Остановка.")
                self.status = WorkerStatus.ERROR
                break

    def _bind_handlers(self):
        """Привязка всех событий (команд) к клиенту - ПОЛНЫЙ НАБОР"""
        cl = self.client

        # ========== AI КОМАНДЫ ==========
        
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.g(?: |$)(.*)'))
        async def handler_quiz(e):
            """Генерация ответа ИИ"""
            await e.edit("⚡️ <b>Думаю...</b>", parse_mode='html')
            
            question = e.pattern_match.group(1)
            if not question and e.is_reply:
                reply = await e.get_reply_message()
                question = reply.text or reply.caption or ""
            
            if not question: 
                return await e.edit("❌ <b>Ошибка:</b> Пустой запрос.", parse_mode='html')
            
            sys_prompt = "Ты помощник для викторин. Твоя цель: дать ТОЛЬКО правильный ответ. Максимально коротко (1-3 слова). Без вводных фраз."
            
            answer = await ask_gpt_safe(sys_prompt, question)
            await e.edit(f"<b>{answer}</b>", parse_mode='html')

        # ========== АНАЛИЗ И ОТЧЁТЫ ==========
        
        @cl.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.report$'))
        async def handler_report(e):
            """Анализ логов с помощью AI (из v75) + CSV отчёт (из v76)"""
            await e.edit("🕵️‍♂️ <b>Сбор данных...</b>", parse_mode='html')
            
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
                return await e.edit(f"❌ Ошибка доступа к чату: {ex}")

            if not logs: 
                # Если нет логов чата - генерируем CSV отчёт из БД
                await e.edit("📊 <b>Генерирую отчёт из базы данных...</b>", parse_mode='html')
                
                try:
                    data = await db.get_report_data(days=7)
                    
                    if not data:
                        return await e.edit("📊 <b>Нет данных для отчёта</b>", parse_mode='html')

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_path = cfg.REPORTS_DIR / f"report_{timestamp}.csv"
                    
                    with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                        writer = csv.writer(f, delimiter=';')
                        writer.writerow([
                            "Номер", "Пользователь", "Статус", 
                            "Создан", "Код получен", "Фото получено", 
                            "Время работы (сек)", "Ошибка"
                        ])
                        
                        for row in data:
                            writer.writerow(row)

                    await cl.send_file(
                        'me',
                        csv_path,
                        caption=f"📊 **Отчёт за последние 7 дней**\n\n"
                                f"📅 Сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                                f"📝 Записей: {len(data)}"
                    )
                    
                    await e.delete()
                    csv_path.unlink()
                    
                    logger.info(f"📊 Воркер {self.uid}: отчёт сгенерирован ({len(data)} записей)")
                    
                except Exception as ex:
                    await e.edit(f"❌ **Ошибка генерации отчёта:**\n`{str(ex)}`", parse_mode='html')
                
                return
            
            # Если есть логи - делаем AI анализ (из v75)
            logs = logs[::-1]
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
                await e.edit(f"📝 <b>Текстовый отчет:</b>\n\n{res}", parse_mode='html')

        # ========== СКАНИРОВАНИЕ БАЗЫ ==========
        
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.scan$'))
        async def handler_scan(e):
            """Сбор базы участников чата"""
            await e.edit("🔎 <b>Сканирую чат (до 5000 сообщений)...</b>", parse_mode='html')
            
            users = {}
            count = 0
            
            try:
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
            
            out = io.StringIO()
            writer = csv.writer(out)
            writer.writerow(["ID", "Username", "Name"])
            for uid, info in users.items():
                writer.writerow([uid, info[0], info[1]])
            
            out.seek(0)
            bio = io.BytesIO(out.getvalue().encode('utf-8-sig'))
            bio.name = f"Base_{e.chat_id}.csv"
            
            await cl.send_file("me", bio, caption=f"✅ <b>Скан завершен</b>\n👥 Уникальных пользователей: {len(users)}\n📂 Сохранено в Избранное", parse_mode='html')
            await e.edit(f"✅ <b>Готово!</b> Собрано: {len(users)} чел.", parse_mode='html')

        # ========== СПАМ-КОМАНДЫ ==========
        
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.(s|spam)\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def handler_spam(e):
            """Спамер"""
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
                        await asyncio.sleep(f.seconds + 5)
                    except Exception: 
                        break
            
            self.spam_task = asyncio.create_task(spam_loop())

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def handler_stop(e):
            """Остановка спама"""
            if self.spam_task: 
                self.spam_task.cancel()
                self.spam_task = None
                await e.edit("🛑 <b>Задача остановлена.</b>", parse_mode='html')
            else:
                await e.edit("⚠️ Нет активных задач.", parse_mode='html')

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.all(?:\s+(.+))?'))
        async def handler_all(e):
            """Тег всех участников"""
            await e.delete()
            text = e.pattern_match.group(1) or "Внимание!"
            try:
                participants = await cl.get_participants(e.chat_id)
                chunk = []
                for p in participants:
                    if not p.bot and not p.deleted:
                        chunk.append(f"<a href='tg://user?id={p.id}'>\u200b</a>")
                        if len(chunk) >= 5:
                            await cl.send_message(e.chat_id, text + "".join(chunk), parse_mode='html')
                            chunk = []
                            await asyncio.sleep(2)
            except Exception:
                pass

        # ========== КОМАНДЫ ОБРАБОТКИ НОМЕРОВ (из v76) ==========
        
        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.ping$'))
        async def cmd_ping(event):
            """Проверка работоспособности"""
            await event.edit(
                "🚀 **TITAN SYSTEM ONLINE**\n\n"
                f"📊 Обработано: {self.processed_count}\n"
                f"❌ Ошибок: {self.error_count}\n"
                f"⏱ Uptime: {self._get_uptime()}"
            )

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.au$'))
        async def cmd_au(event):
            """Автоматическая отправка приветствия"""
            await event.edit(
                "✅ **Приветствую!**\n\n"
                "Ответьте на это сообщение, чтобы я сохранил ваши номера "
                "и отправил вам коды на них."
            )
            self.waiting_for_code = True
            self.last_activity = int(datetime.now().timestamp())
            logger.info(f"📝 Воркер {self.uid}: команда .au выполнена")

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.u$'))
        async def cmd_u(event):
            """Запрос номера из базы"""
            phone = await db.get_available_number(self.uid)
            
            if not phone:
                await event.edit(
                    "❌ **Нет доступных номеров в базе**\n\n"
                    "Добавьте номера через бота."
                )
                return
            
            self.current_phone = phone
            self.waiting_for_photo = True
            self.status = WorkerStatus.WORKING
            
            await event.edit(
                f"📱 **Номер выдан:** `{phone}`\n\n"
                f"⏳ Ожидаю фото с кодом подтверждения..."
            )
            
            await db.update_number_status(phone, NumberStatus.PHOTO_REQUESTED)
            self.last_activity = int(datetime.now().timestamp())
            logger.info(f"📞 Воркер {self.uid}: выдан номер {phone}")

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.qr$'))
        async def cmd_qr(event):
            """Запрос QR-кода"""
            await event.edit(
                "🔲 **QR-код запрошен**\n\n"
                "Пользователь запросил авторизацию через QR-код."
            )
            self.last_activity = int(datetime.now().timestamp())
            logger.info(f"🔲 Воркер {self.uid}: запрошен QR-код")

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.v$'))
        async def cmd_v(event):
            """Подтверждение входа с инлайн кнопкой"""
            if not self.current_phone:
                await event.edit(
                    "❌ **Нет активного номера**\n\n"
                    "Используйте `.u` для получения номера."
                )
                return
            
            buttons = [[Button.inline("✅ Слёт", b"slet")]]
            
            await event.edit(
                f"📞 **Номер встал:** `{self.current_phone}`\n\n"
                f"✅ Успешный вход подтверждён",
                buttons=buttons
            )
            self.last_activity = int(datetime.now().timestamp())
            logger.info(f"✅ Воркер {self.uid}: номер {self.current_phone} встал")

        @cl.on(events.CallbackQuery(pattern=b"slet"))
        async def callback_slet(event):
            """Обработка нажатия кнопки Слёт"""
            if self.current_phone:
                await db.update_number_status(
                    self.current_phone, 
                    NumberStatus.COMPLETED
                )
                self.processed_count += 1
                
                await event.edit(
                    f"✅ **Операция завершена**\n\n"
                    f"📱 Номер: `{self.current_phone}`\n"
                    f"📊 Всего обработано: {self.processed_count}"
                )
                
                logger.info(f"✅ Воркер {self.uid}: операция с {self.current_phone} завершена")
                
                self.current_phone = None
                self.waiting_for_photo = False
                self.status = WorkerStatus.ONLINE

        @cl.on(events.NewMessage(outgoing=True, pattern=r'^\.stats$'))
        async def cmd_stats(event):
            """Статистика воркера"""
            uptime = self._get_uptime()
            success_rate = (self.processed_count / (self.processed_count + self.error_count) * 100 
                           if (self.processed_count + self.error_count) > 0 else 0)
            
            await event.edit(
                f"📊 **Статистика воркера**\n\n"
                f"🆔 ID: `{self.uid}`\n"
                f"🟢 Статус: {self.status.value}\n"
                f"⏱ Uptime: {uptime}\n"
                f"✅ Обработано: {self.processed_count}\n"
                f"❌ Ошибок: {self.error_count}\n"
                f"📈 Success rate: {success_rate:.1f}%\n"
                f"📱 Текущий номер: {self.current_phone or 'нет'}"
            )

        @cl.on(events.NewMessage(incoming=True))
        async def handle_incoming(event):
            """Обработка входящих сообщений"""
            
            if self.waiting_for_code and event.message.message:
                code_text = event.message.message
                
                code_match = re.search(r'\b\d{5,6}\b', code_text)
                
                if code_match and self.current_phone:
                    await db.update_number_status(
                        self.current_phone, 
                        NumberStatus.CODE_RECEIVED
                    )
                    logger.info(f"✅ Воркер {self.uid}: код получен для {self.current_phone}")
                    self.waiting_for_code = False

            if self.waiting_for_photo and event.message.photo:
                if self.current_phone:
                    await db.update_number_status(
                        self.current_phone, 
                        NumberStatus.PHOTO_RECEIVED
                    )
                    logger.info(f"📷 Воркер {self.uid}: фото получено для {self.current_phone}")

    def _get_uptime(self) -> str:
        """Получение времени работы воркера"""
        if not self.started_at:
            return "N/A"
        
        uptime_seconds = int(datetime.now().timestamp()) - self.started_at
        hours = uptime_seconds // 3600
        minutes = (uptime_seconds % 3600) // 60
        return f"{hours}ч {minutes}м"

    async def stop(self):
        """Остановка воркера"""
        try:
            if self.client:
                await self.client.disconnect()
            if self.task:
                self.task.cancel()
                try:
                    await self.task
                except asyncio.CancelledError:
                    pass
            if self.spam_task:
                self.spam_task.cancel()
            
            self.status = WorkerStatus.OFFLINE
            logger.info(f"🛑 Воркер {self.uid} остановлен")
            
        except Exception as e:
            logger.error(f"Ошибка остановки воркера {self.uid}: {e}")

# Глобальный пул воркеров
W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI (AIOGRAM ИНТЕРФЕЙС) - ОБЪЕДИНЁННЫЙ
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

class AddNumberStates(StatesGroup):
    waiting_for_numbers = State()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ UI ---

def kb_main(uid: int):
    """Главное меню - РАСШИРЕННОЕ"""
    btns = [
        [InlineKeyboardButton(text="🚀 Запустить воркер", callback_data="start_worker")],
        [InlineKeyboardButton(text="🌪 ПЕРЕЛИВ (Siphon)", callback_data="siphon_start")],
        [InlineKeyboardButton(text="➕ Добавить номера", callback_data="add_numbers")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="profile")],
        [InlineKeyboardButton(text="📈 Общая статистика", callback_data="global_stats")],
        [InlineKeyboardButton(text="📚 Помощь / Команды", callback_data="help")],
        [InlineKeyboardButton(text="🔑 Вход в аккаунт", callback_data="auth")],
        [InlineKeyboardButton(text="🛑 Остановить воркер", callback_data="stop_worker")]
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
    await db.upsert_user(m.from_user.id, m.from_user.username, m.from_user.first_name)
    await m.answer(
        f"💎 <b>StatPro TITANIUM v77.0 ULTIMATE</b>\n\n"
        f"Добро пожаловать, <b>{m.from_user.first_name}</b>!\n\n"
        f"🔥 <b>Объединённая версия:</b>\n"
        f"• AI-помощник (команда .g)\n"
        f"• Обработка номеров (.au, .u, .v)\n"
        f"• Массовый перелив (Siphon Kamikaze)\n"
        f"• Анализ логов + CSV отчёты (.report)\n"
        f"• Сбор базы (.scan)\n"
        f"• Спам-функции (.spam, .all)\n\n"
        f"Выберите действие:",
        reply_markup=kb_main(m.from_user.id)
    )

@router.callback_query(F.data == "help")
async def cb_help(c: CallbackQuery):
    text = (
        "📚 <b>СПРАВКА ПО КОМАНДАМ (ЮЗЕРБОТ):</b>\n\n"
        "<b>🤖 AI и Анализ:</b>\n"
        "⚡️ <code>.g [вопрос]</code> — Ответ ИИ на любой вопрос\n"
        "🕵️‍♂️ <code>.report</code> — AI анализ логов или CSV отчёт\n"
        "📊 <code>.stats</code> — Статистика воркера\n"
        "🚀 <code>.ping</code> — Проверка работоспособности\n\n"
        "<b>📞 Обработка номеров:</b>\n"
        "✅ <code>.au</code> — Отправить приветствие\n"
        "📱 <code>.u</code> — Запросить номер из базы\n"
        "✔️ <code>.v</code> — Подтвердить вход (кнопка Слёт)\n"
        "🔲 <code>.qr</code> — Запросить QR-код\n\n"
        "<b>🔧 Утилиты:</b>\n"
        "🧬 <code>.scan</code> — Спарсить участников чата\n"
        "🚀 <code>.spam [текст] [кол-во] [сек]</code> — Спаммер\n"
        "🛑 <code>.stop</code> — Остановить задачу\n"
        "📢 <code>.all [текст]</code> — Тегнуть всех\n\n"
        "<i>Для работы команд войдите через 🔑 Вход в аккаунт</i>"
    )
    await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back")]]))

@router.callback_query(F.data == "back")
async def cb_back(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.delete()
    await cmd_start(c.message, state)

@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    """Профиль пользователя с расширенной статистикой"""
    info = await db.get_user_info(c.from_user.id)
    stats = await db.get_user_stats(c.from_user.id)
    
    # Статус подписки
    if info and info[0] and info[0] > time.time():
        days_left = int((info[0] - time.time()) / 86400)
        sub_status = f"🟢 <b>АКТИВНА</b> (осталось {days_left} дн.)"
    else:
        sub_status = "🔴 <b>НЕАКТИВНА</b>"
    
    # Статус воркера
    worker = W_POOL.get(c.from_user.id)
    if worker and worker.status != WorkerStatus.OFFLINE:
        worker_status = f"🟢 <b>{worker.status.value.upper()}</b>"
    else:
        worker_status = "🔴 <b>ОТКЛЮЧЕН</b>"
    
    # Success rate
    success_rate = (stats['completed'] / stats['total'] * 100 
                    if stats['total'] > 0 else 0)
    
    avg_time_str = f"{int(stats['avg_time'])}с" if stats['avg_time'] else "N/A"

    text = (
        f"👤 <b>ЛИЧНЫЙ КАБИНЕТ</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🆔 ID: <code>{c.from_user.id}</code>\n"
        f"💎 Подписка: {sub_status}\n"
        f"🔌 Статус воркера: {worker_status}\n\n"
        f"<b>📈 Статистика операций:</b>\n"
        f"• Всего: {stats['total']}\n"
        f"• Завершено: {stats['completed']}\n"
        f"• Ошибок: {stats['failed']}\n"
        f"• Success rate: {success_rate:.1f}%\n"
        f"⏱ Среднее время: {avg_time_str}\n"
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

# --- ХЕНДЛЕРЫ: ВОРКЕР ---

@router.callback_query(F.data == "start_worker")
async def cb_start_worker(call: CallbackQuery):
    """Запуск воркера"""
    user_id = call.from_user.id
    
    if not await db.check_sub_bool(user_id):
        return await call.answer("❌ Требуется активная подписка!", True)
    
    if user_id in W_POOL and W_POOL[user_id].status != WorkerStatus.OFFLINE:
        await call.answer("⚠️ Воркер уже запущен", show_alert=True)
        return

    if len(W_POOL) >= cfg.MAX_WORKERS:
        await call.answer("⚠️ Достигнут лимит активных воркеров", show_alert=True)
        return

    await call.message.edit_text("⏳ Запускаю воркер...")
    
    worker = Worker(user_id=user_id)
    if await worker.start():
        W_POOL[user_id] = worker
        await call.message.edit_text(
            "✅ <b>Воркер успешно запущен!</b>\n\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🟢 Статус: Online\n\n"
            "Теперь используйте команды в Telegram:\n"
            "• <code>.ping</code> - проверка\n"
            "• <code>.u</code> - получить номер\n"
            "• <code>.g</code> - задать вопрос AI\n"
            "• <code>.help</code> - все команды",
            reply_markup=kb_main(user_id)
        )
    else:
        await call.message.edit_text(
            "❌ <b>Ошибка запуска воркера</b>\n\n"
            "Возможные причины:\n"
            "• Не авторизована сессия Telethon\n"
            "• Ошибка подключения к Telegram\n\n"
            "Сначала войдите через кнопку 🔑 Вход в аккаунт",
            reply_markup=kb_main(user_id)
        )

@router.callback_query(F.data == "stop_worker")
async def cb_stop_worker(call: CallbackQuery):
    """Остановка воркера"""
    user_id = call.from_user.id
    
    if user_id not in W_POOL:
        await call.answer("⚠️ Воркер не запущен", show_alert=True)
        return
    
    await W_POOL[user_id].stop()
    del W_POOL[user_id]
    
    await call.message.edit_text(
        "🛑 <b>Воркер остановлен</b>\n\n"
        "Вы можете запустить его снова в любое время.",
        reply_markup=kb_main(user_id)
    )

@router.callback_query(F.data == "global_stats")
async def cb_global_stats(call: CallbackQuery):
    """Глобальная статистика"""
    workers_count = len(W_POOL)
    active_workers = sum(1 for w in W_POOL.values() 
                         if w.status == WorkerStatus.ONLINE)
    
    total_processed = sum(w.processed_count for w in W_POOL.values())
    total_errors = sum(w.error_count for w in W_POOL.values())
    
    text = (
        f"📈 <b>ГЛОБАЛЬНАЯ СТАТИСТИКА</b>\n\n"
        f"🤖 Всего воркеров: {workers_count}\n"
        f"🟢 Активных: {active_workers}\n"
        f"📊 Обработано операций: {total_processed}\n"
        f"❌ Ошибок: {total_errors}\n"
    )
    
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back")]
    ]))

# --- ХЕНДЛЕРЫ: ДОБАВЛЕНИЕ НОМЕРОВ ---

@router.callback_query(F.data == "add_numbers")
async def cb_add_numbers(call: CallbackQuery, state: FSMContext):
    """Добавление номеров"""
    if not await db.check_sub_bool(call.from_user.id):
        return await call.answer("❌ Требуется активная подписка!", True)
    
    await state.set_state(AddNumberStates.waiting_for_numbers)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    
    await call.message.edit_text(
        "📱 <b>ДОБАВЛЕНИЕ НОМЕРОВ</b>\n\n"
        "Отправьте номера в формате:\n"
        "<code>+7XXXXXXXXXX</code>\n"
        "или\n"
        "<code>7XXXXXXXXXX</code>\n\n"
        "Можно отправить несколько номеров (каждый с новой строки):",
        reply_markup=kb
    )

@router.message(AddNumberStates.waiting_for_numbers)
async def process_numbers(msg: Message, state: FSMContext):
    """Обработка введённых номеров"""
    text = msg.text.strip()
    lines = text.split('\n')
    
    added = 0
    duplicates = 0
    errors = 0
    
    for line in lines:
        phone = ''.join(filter(str.isdigit, line))
        
        if len(phone) < 10:
            errors += 1
            continue
        
        if not phone.startswith('+'):
            phone = '+' + phone
        
        if await db.add_number(phone, msg.from_user.id):
            added += 1
        else:
            duplicates += 1
    
    result_text = (
        f"✅ <b>РЕЗУЛЬТАТ ДОБАВЛЕНИЯ:</b>\n\n"
        f"✅ Добавлено: {added}\n"
        f"⚠️ Дубликатов: {duplicates}\n"
        f"❌ Ошибок: {errors}"
    )
    
    await msg.answer(result_text, reply_markup=kb_main(msg.from_user.id))
    await state.clear()

@router.callback_query(F.data == "cancel")
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    """Отмена операции"""
    await state.clear()
    await call.message.edit_text(
        "❌ <b>Операция отменена</b>",
        reply_markup=kb_main(call.from_user.id)
    )

# --- ХЕНДЛЕРЫ: SIPHON (KAMIKAZE EDITION) ---

@router.callback_query(F.data == "siphon_start")
async def cb_siphon_start(c: CallbackQuery, state: FSMContext):
    if not await db.check_sub_bool(c.from_user.id):
        return await c.answer("❌ Требуется активная подписка!", True)
    
    if c.from_user.id not in W_POOL:
        return await c.answer("❌ Сначала запустите воркер!", True)
    
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
        await bot.download(m.document, destination=temp_path)
        
        content = ""
        with open(temp_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
        raw_ids = re.findall(r'\b\d{7,20}\b', content)
        unique_ids = list(set([int(x) for x in raw_ids]))
        
        if not unique_ids:
            return await m.answer("❌ <b>Ошибка:</b> В файле не найдено ни одного ID.")
            
        await state.update_data(targets=unique_ids)
        await m.answer(
            f"✅ <b>Файл обработан!</b>\n"
            f"Найдено уникальных целей: <b>{len(unique_ids)}</b>\n\n"
            f"✍️ <b>Теперь отправьте текст для рассылки:</b>"
        )
        await state.set_state(SiphonStates.MSG)
        
    except Exception as e:
        await m.answer(f"❌ Ошибка обработки файла: {e}")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

@router.message(SiphonStates.MSG)
async def state_siphon_msg(m: Message, state: FSMContext):
    await state.update_data(msg_content=m.text or m.caption or "Привет")
    
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
        return await c.answer("❌ Воркер отключился! Перезапустите.", True)
        
    await c.message.edit_text(
        "🚀 <b>Рассылка запущена в фоне!</b>\n"
        "Я пришлю отчет, когда закончу.\n"
        "Можете пользоваться ботом дальше."
    )
    
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
    """Асинхронная задача рассылки (KAMIKAZE MODE)"""
    ok_count = 0
    fail_count = 0
    
    for target_id in targets:
        try:
            try:
                entity = await w.client.get_input_entity(target_id)
            except ValueError:
                entity = target_id
            except Exception:
                fail_count += 1
                continue

            await w.client.send_message(entity, text)
            ok_count += 1
            
            await asyncio.sleep(random.uniform(1.5, 4.0))
            
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)
            
        except (UserPrivacyRestrictedError, UserDeactivatedError, PeerIdInvalidError, ChatWriteForbiddenError):
            fail_count += 1
            
        except Exception as e:
            fail_count += 1
            
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
    
    w = Worker(uid)
    w.client = TelegramClient(str(cfg.SESSION_DIR / f"login_{uid}"), cfg.API_ID, cfg.API_HASH)
    await w.client.connect()
    
    try:
        sent = await w.client.send_code_request(phone)
        
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
    w: Worker = data.get("temp_worker")
    
    if action == "del":
        current_code = current_code[:-1]
    elif action == "go":
        if not current_code: return await c.answer("Введите код!", True)
        
        await c.message.edit_text("⏳ <b>Проверка кода...</b>")
        try:
            await w.client.sign_in(phone=data['phone'], code=current_code, phone_code_hash=data['hash'])
            
            await w.client.disconnect()
            os.rename(
                cfg.SESSION_DIR / f"login_{c.from_user.id}.session", 
                cfg.SESSION_DIR / f"session_{c.from_user.id}.session"
            )
            
            real_worker = Worker(c.from_user.id)
            if await real_worker.start():
                W_POOL[c.from_user.id] = real_worker
                await c.message.answer("✅ <b>Успешный вход!</b> Воркер запущен.", reply_markup=kb_main(c.from_user.id))
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
        
        os.rename(
            cfg.SESSION_DIR / f"login_{m.from_user.id}.session", 
            cfg.SESSION_DIR / f"session_{m.from_user.id}.session"
        )
        
        real_worker = Worker(m.from_user.id)
        if await real_worker.start():
            W_POOL[m.from_user.id] = real_worker
            await m.answer("✅ <b>Пароль принят!</b> Воркер активен.", reply_markup=kb_main(m.from_user.id))
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
        
        qr_img = io.BytesIO()
        qrcode.make(qr_login.url).save(qr_img, "PNG")
        qr_img.seek(0)
        
        msg = await c.message.answer_photo(
            BufferedInputFile(qr_img.read(), "qr.png"),
            caption="📸 <b>Отсканируйте QR код в Telegram</b>\n(Настройки → Устройства → Подключить устройство)"
        )
        
        await qr_login.wait(60)
        await msg.delete()
        await w.client.disconnect()
        
        real_worker = Worker(uid)
        if await real_worker.start():
            W_POOL[uid] = real_worker
            await c.message.answer("✅ <b>Успешный вход по QR!</b>", reply_markup=kb_main(uid))
        
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
    if c.from_user.id != cfg.ADMIN_ID:
        return await c.answer("❌ Доступ запрещён", True)
    
    await c.message.edit_text(
        "👑 <b>АДМИН-ПАНЕЛЬ</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать Промокод", callback_data="mk_p")],
            [InlineKeyboardButton(text="📊 Полная статистика", callback_data="admin_full_stats")],
            [InlineKeyboardButton(text="🧹 Очистить старые данные", callback_data="admin_cleanup")],
            [InlineKeyboardButton(text="👥 Список воркеров", callback_data="admin_workers")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back")]
        ])
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
    await m.answer(f"✅ Промокод создан:\n<code>{code}</code>", reply_markup=kb_main(m.from_user.id))
    await state.clear()

@router.callback_query(F.data == "admin_full_stats")
async def cb_admin_full_stats(call: CallbackQuery):
    if call.from_user.id != cfg.ADMIN_ID:
        return await call.answer("❌ Доступ запрещён", show_alert=True)
    
    data = await db.get_report_data(days=30)
    
    total = len(data)
    completed = sum(1 for row in data if row[2] == 'completed')
    failed = sum(1 for row in data if row[2] == 'failed')
    
    text = (
        f"📊 <b>ПОЛНАЯ СТАТИСТИКА (30 ДНЕЙ)</b>\n\n"
        f"📝 Всего операций: {total}\n"
        f"✅ Завершено: {completed}\n"
        f"❌ Провалено: {failed}\n"
        f"📈 Success rate: {(completed/total*100 if total > 0 else 0):.1f}%\n"
    )
    
    await call.message.answer(text)

@router.callback_query(F.data == "admin_cleanup")
async def cb_admin_cleanup(call: CallbackQuery):
    if call.from_user.id != cfg.ADMIN_ID:
        return await call.answer("❌ Доступ запрещён", show_alert=True)
    
    await db.cleanup_old_data(days=30)
    await call.answer("✅ Старые данные очищены", show_alert=True)

@router.callback_query(F.data == "admin_workers")
async def cb_admin_workers(call: CallbackQuery):
    if call.from_user.id != cfg.ADMIN_ID:
        return await call.answer("❌ Доступ запрещён", show_alert=True)
    
    if not W_POOL:
        await call.message.answer("👥 <b>Нет активных воркеров</b>")
        return
    
    text = "👥 <b>АКТИВНЫЕ ВОРКЕРЫ:</b>\n\n"
    for user_id, worker in W_POOL.items():
        text += (
            f"🆔 {user_id}\n"
            f"Status: {worker.status.value}\n"
            f"Processed: {worker.processed_count}\n"
            f"Uptime: {worker._get_uptime()}\n\n"
        )
    
    await call.message.answer(text)

# --- BACKGROUND TASKS ---
async def cleanup_task():
    """Фоновая задача очистки"""
    while True:
        await asyncio.sleep(86400)
        try:
            await db.cleanup_old_data(days=30)
            logger.info("🧹 Автоматическая очистка выполнена")
        except Exception as e:
            logger.error(f"Ошибка очистки: {e}")

# =========================================================================
# 🚀 ЗАПУСК
# =========================================================================

async def main():
    """Точка входа"""
    await db.init()
    
    # Восстановление сессий
    restored_count = 0
    sessions = list(cfg.SESSION_DIR.glob("session_*.session"))
    
    logger.info(f"Найдено сессий: {len(sessions)}")
    
    for sess_file in sessions:
        try:
            uid = int(sess_file.stem.split("_")[1])
            
            if await db.check_sub_bool(uid):
                w = Worker(uid)
                if await w.start():
                    W_POOL[uid] = w
                    restored_count += 1
        except Exception as e:
            logger.error(f"Ошибка восстановления сессии {sess_file}: {e}")
            
    logger.info(f"🔥 StatPro TITANIUM v77.0 ULTIMATE Запущен! Воркеров: {restored_count}")
    
    # Фоновые задачи
    asyncio.create_task(cleanup_task())
    
    # Старт бота
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен пользователем.")
