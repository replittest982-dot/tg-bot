import asyncio
import logging
import os
import csv
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Optional

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import TelegramClient, events
from telethon.tl.types import MessageEntityCode
import aiosqlite

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIG ---
@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN")
    API_ID: int = int(os.getenv("API_ID", 0))
    API_HASH: str = os.getenv("API_HASH", "")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", 0))
    SESSION_DIR: Path = Path("sessions")
    DB_PATH: Path = Path("titan.db")

    def __post_init__(self):
        if not self.BOT_TOKEN:
            raise ValueError("❌ BOT_TOKEN не установлен")
        if not self.API_ID or not self.API_HASH:
            raise ValueError("❌ API_ID/API_HASH не установлены")
        if not self.ADMIN_ID:
            raise ValueError("❌ ADMIN_ID не установлен")
        self.SESSION_DIR.mkdir(exist_ok=True)

cfg = Config()

# --- DATABASE ---
class Database:
    def __init__(self, path: Path):
        self.path = path
        self._conn = None

    async def init(self):
        self._conn = await aiosqlite.connect(self.path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT UNIQUE,
                user_id INTEGER,
                created_at INTEGER,
                code_received_at INTEGER,
                photo_received_at INTEGER,
                status TEXT DEFAULT 'waiting'
            )
        """)
        await self._conn.commit()

    async def add_number(self, phone: str, user_id: int):
        try:
            await self._conn.execute(
                "INSERT INTO numbers (phone, user_id, created_at) VALUES (?, ?, ?)",
                (phone, user_id, int(datetime.now().timestamp()))
            )
            await self._conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def get_available_number(self):
        cursor = await self._conn.execute(
            "SELECT phone FROM numbers WHERE status='waiting' LIMIT 1"
        )
        row = await cursor.fetchone()
        return row[0] if row else None

    async def mark_code_received(self, phone: str):
        await self._conn.execute(
            "UPDATE numbers SET code_received_at=?, status='code_received' WHERE phone=?",
            (int(datetime.now().timestamp()), phone)
        )
        await self._conn.commit()

    async def mark_photo_received(self, phone: str):
        await self._conn.execute(
            "UPDATE numbers SET photo_received_at=?, status='completed' WHERE phone=?",
            (int(datetime.now().timestamp()), phone)
        )
        await self._conn.commit()

    async def get_report_data(self):
        cursor = await self._conn.execute("""
            SELECT phone, created_at, code_received_at, photo_received_at, 
                   (photo_received_at - created_at) as work_time
            FROM numbers WHERE status='completed'
        """)
        return await cursor.fetchall()

    async def close(self):
        if self._conn:
            await self._conn.close()

db = Database(cfg.DB_PATH)

# --- WORKER ---
@dataclass
class Worker:
    user_id: int
    client: Optional[TelegramClient] = None
    task: Optional[asyncio.Task] = None
    current_phone: Optional[str] = None
    waiting_for_code: bool = False
    waiting_for_photo: bool = False

    async def start(self):
        try:
            session = cfg.SESSION_DIR / f"user_{self.user_id}"
            self.client = TelegramClient(str(session), cfg.API_ID, cfg.API_HASH)
            await self.client.connect()

            if not await self.client.is_user_authorized():
                logger.error(f"User {self.user_id} не авторизован")
                return False

            self._setup_handlers()
            self.task = asyncio.create_task(self.client.run_until_disconnected())
            logger.info(f"✅ Воркер {self.user_id} запущен")
            return True
        except Exception as e:
            logger.error(f"Ошибка запуска воркера {self.user_id}: {e}")
            return False

    def _setup_handlers(self):
        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.au$'))
        async def cmd_au(event):
            await event.edit("✅ Приветствую! Ответьте на это сообщение, чтобы я сохранил ваши номера и отправил вам коды на них.")
            self.waiting_for_code = True

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.u$'))
        async def cmd_u(event):
            phone = await db.get_available_number()
            if not phone:
                await event.edit("❌ Нет доступных номеров в базе")
                return
            
            self.current_phone = phone
            self.waiting_for_photo = True
            await event.edit(f"📱 Номер: `{phone}`\nОжидаю фото с кодом...")

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.qr$'))
        async def cmd_qr(event):
            await event.edit("🔲 Пользователь запросил QR-код")
            # Логика QR тут, если нужна

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.v$'))
        async def cmd_v(event):
            if not self.current_phone:
                await event.edit("❌ Нет активного номера")
                return
            
            keyboard = self.client.build_reply_markup([
                [{"text": "✅ Слёт", "callback": b"slet"}]
            ])
            await event.edit(f"📞 Номер встал: `{self.current_phone}`", buttons=keyboard)

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.report$'))
        async def cmd_report(event):
            data = await db.get_report_data()
            if not data:
                await event.edit("📊 Нет данных для отчёта")
                return

            csv_path = Path("report.csv")
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Номер", "Время создания", "Код получен", "Фото получено", "Время работы (сек)"])
                for row in data:
                    phone, created, code_time, photo_time, work_time = row
                    writer.writerow([
                        phone,
                        datetime.fromtimestamp(created).strftime("%Y-%m-%d %H:%M:%S"),
                        datetime.fromtimestamp(code_time).strftime("%Y-%m-%d %H:%M:%S") if code_time else "-",
                        datetime.fromtimestamp(photo_time).strftime("%Y-%m-%d %H:%M:%S") if photo_time else "-",
                        work_time if work_time else "-"
                    ])

            await self.client.send_file("me", csv_path, caption="📊 Отчёт по работе")
            csv_path.unlink()
            await event.delete()

        @self.client.on(events.NewMessage(incoming=True))
        async def handle_incoming(event):
            if self.waiting_for_code and event.message.message:
                # Получили код - сохраняем
                if self.current_phone:
                    await db.mark_code_received(self.current_phone)
                    logger.info(f"✅ Код получен для {self.current_phone}")
                self.waiting_for_code = False

            if self.waiting_for_photo and event.message.photo:
                # Получили фото
                if self.current_phone:
                    await db.mark_photo_received(self.current_phone)
                    logger.info(f"✅ Фото получено для {self.current_phone}")
                    self.waiting_for_photo = False
                    self.current_phone = None

    async def stop(self):
        if self.client:
            await self.client.disconnect()
        if self.task:
            self.task.cancel()

WORKERS: Dict[int, Worker] = {}

# --- BOT ---
bot = Bot(token=cfg.BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()

@router.message(F.text == "/start")
async def cmd_start(msg: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Запустить воркер", callback_data="start_worker")],
        [InlineKeyboardButton(text="📊 Статус", callback_data="status")]
    ])
    await msg.answer("💎 **TITAN PRO v1.0**\n\nСистема автоматизации готова.", reply_markup=kb)

@router.callback_query(F.data == "start_worker")
async def cb_start_worker(call: CallbackQuery):
    user_id = call.from_user.id
    if user_id in WORKERS:
        await call.answer("⚠️ Воркер уже запущен", show_alert=True)
        return

    worker = Worker(user_id=user_id)
    if await worker.start():
        WORKERS[user_id] = worker
        await call.message.edit_text("✅ Воркер успешно запущен!")
    else:
        await call.message.edit_text("❌ Ошибка запуска. Проверьте авторизацию Telethon.")

@router.callback_query(F.data == "status")
async def cb_status(call: CallbackQuery):
    count = len(WORKERS)
    await call.answer(f"🔥 Активных воркеров: {count}", show_alert=True)

# --- MAIN ---
async def main():
    await db.init()
    dp.include_router(router)
    logger.info("🚀 Бот запущен")
    try:
        await dp.start_polling(bot)
    finally:
        for worker in WORKERS.values():
            await worker.stop()
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
