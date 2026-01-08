import asyncio
import logging
import os
import csv
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple
from enum import Enum

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, 
    InlineKeyboardButton, FSInputFile
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from telethon import TelegramClient, events
from telethon.tl.types import (
    User, Chat, Channel, MessageMediaPhoto,
    MessageEntityCode, KeyboardButtonCallback
)
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneCodeInvalidError, PhoneNumberInvalidError
)
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
import aiosqlite

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('titan.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- ENUMS ---
class NumberStatus(Enum):
    WAITING = "waiting"
    CODE_SENT = "code_sent"
    CODE_RECEIVED = "code_received"
    PHOTO_REQUESTED = "photo_requested"
    PHOTO_RECEIVED = "photo_received"
    COMPLETED = "completed"
    FAILED = "failed"

class WorkerStatus(Enum):
    OFFLINE = "offline"
    ONLINE = "online"
    WORKING = "working"
    ERROR = "error"

# --- CONFIG ---
@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN")
    API_ID: int = int(os.getenv("API_ID", 0))
    API_HASH: str = os.getenv("API_HASH", "")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", 0))
    SESSION_DIR: Path = Path("sessions")
    DB_PATH: Path = Path("titan_pro.db")
    REPORTS_DIR: Path = Path("reports")
    MAX_WORKERS: int = 10
    FLOOD_WAIT_TIME: int = 60
    CODE_TIMEOUT: int = 300
    PHOTO_TIMEOUT: int = 600

    def __post_init__(self):
        if not self.BOT_TOKEN:
            raise ValueError("❌ BOT_TOKEN не установлен в переменных окружения")
        if not self.API_ID or not self.API_HASH:
            raise ValueError("❌ API_ID/API_HASH не установлены в переменных окружения")
        if not self.ADMIN_ID:
            raise ValueError("❌ ADMIN_ID не установлен в переменных окружения")
        
        self.SESSION_DIR.mkdir(exist_ok=True)
        self.REPORTS_DIR.mkdir(exist_ok=True)
        logger.info("✅ Конфигурация загружена успешно")

cfg = Config()

# --- DATABASE ---
class Database:
    def __init__(self, path: Path):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def init(self):
        """Инициализация базы данных и создание таблиц"""
        self._conn = await aiosqlite.connect(self.path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                subscription_end INTEGER,
                total_operations INTEGER DEFAULT 0,
                successful_operations INTEGER DEFAULT 0,
                created_at INTEGER,
                last_active INTEGER
            )
        """)
        
        await self._conn.execute("""
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
                chat_id INTEGER,
                message_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                id INTEGER PRIMARY KEY,
                phone TEXT,
                status TEXT DEFAULT 'offline',
                current_task TEXT,
                total_processed INTEGER DEFAULT 0,
                errors_count INTEGER DEFAULT 0,
                started_at INTEGER,
                last_activity INTEGER
            )
        """)
        
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS operation_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                number_id INTEGER,
                worker_id INTEGER,
                action TEXT,
                details TEXT,
                timestamp INTEGER,
                FOREIGN KEY (number_id) REFERENCES numbers(id),
                FOREIGN KEY (worker_id) REFERENCES workers(id)
            )
        """)
        
        await self._conn.commit()
        logger.info("✅ База данных инициализирована")

    async def add_user(self, user_id: int, username: str = None, first_name: str = None):
        """Добавление нового пользователя"""
        now = int(datetime.now().timestamp())
        try:
            await self._conn.execute("""
                INSERT INTO users (id, username, first_name, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    username=excluded.username,
                    first_name=excluded.first_name,
                    last_active=excluded.last_active
            """, (user_id, username, first_name, now, now))
            await self._conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user_id}: {e}")
            return False

    async def check_subscription(self, user_id: int) -> bool:
        """Проверка активности подписки"""
        cursor = await self._conn.execute(
            "SELECT subscription_end FROM users WHERE id=?",
            (user_id,)
        )
        row = await cursor.fetchone()
        if not row or not row[0]:
            return False
        return row[0] > int(datetime.now().timestamp())

    async def add_number(self, phone: str, user_id: int) -> bool:
        """Добавление номера в базу"""
        now = int(datetime.now().timestamp())
        try:
            await self._conn.execute("""
                INSERT INTO numbers (phone, user_id, created_at, status)
                VALUES (?, ?, ?, ?)
            """, (phone, user_id, now, NumberStatus.WAITING.value))
            await self._conn.commit()
            logger.info(f"✅ Номер {phone} добавлен в базу пользователем {user_id}")
            return True
        except aiosqlite.IntegrityError:
            logger.warning(f"⚠️ Номер {phone} уже существует в базе")
            return False
        except Exception as e:
            logger.error(f"Ошибка добавления номера {phone}: {e}")
            return False

    async def get_available_number(self, worker_id: int) -> Optional[str]:
        """Получение свободного номера для обработки"""
        try:
            cursor = await self._conn.execute("""
                SELECT phone, id FROM numbers 
                WHERE status=? AND worker_id IS NULL
                ORDER BY created_at ASC
                LIMIT 1
            """, (NumberStatus.WAITING.value,))
            row = await cursor.fetchone()
            
            if row:
                phone, number_id = row
                await self._conn.execute(
                    "UPDATE numbers SET worker_id=?, status=? WHERE id=?",
                    (worker_id, NumberStatus.CODE_SENT.value, number_id)
                )
                await self._conn.commit()
                return phone
            return None
        except Exception as e:
            logger.error(f"Ошибка получения номера: {e}")
            return None

    async def update_number_status(self, phone: str, status: NumberStatus, 
                                   error_message: str = None):
        """Обновление статуса номера"""
        now = int(datetime.now().timestamp())
        field_map = {
            NumberStatus.CODE_SENT: "code_sent_at",
            NumberStatus.CODE_RECEIVED: "code_received_at",
            NumberStatus.PHOTO_REQUESTED: "photo_requested_at",
            NumberStatus.PHOTO_RECEIVED: "photo_received_at",
            NumberStatus.COMPLETED: "completed_at"
        }
        
        time_field = field_map.get(status)
        if time_field:
            await self._conn.execute(f"""
                UPDATE numbers SET status=?, {time_field}=?, error_message=?
                WHERE phone=?
            """, (status.value, now, error_message, phone))
        else:
            await self._conn.execute("""
                UPDATE numbers SET status=?, error_message=?
                WHERE phone=?
            """, (status.value, error_message, phone))
        
        await self._conn.commit()

    async def log_operation(self, number_id: int, worker_id: int, 
                           action: str, details: str = None):
        """Логирование операции"""
        now = int(datetime.now().timestamp())
        await self._conn.execute("""
            INSERT INTO operation_logs (number_id, worker_id, action, details, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (number_id, worker_id, action, details, now))
        await self._conn.commit()

    async def get_report_data(self, days: int = 7) -> List[Tuple]:
        """Получение данных для отчёта"""
        timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        cursor = await self._conn.execute("""
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
            LEFT JOIN users u ON n.user_id = u.id
            WHERE n.created_at >= ?
            ORDER BY n.created_at DESC
        """, (timestamp,))
        return await cursor.fetchall()

    async def get_user_stats(self, user_id: int) -> Dict:
        """Получение статистики пользователя"""
        cursor = await self._conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
                AVG(CASE WHEN completed_at IS NOT NULL 
                    THEN completed_at - created_at END) as avg_time
            FROM numbers WHERE user_id=?
        """, (user_id,))
        row = await cursor.fetchone()
        return {
            "total": row[0] or 0,
            "completed": row[1] or 0,
            "failed": row[2] or 0,
            "avg_time": row[3] or 0
        }

    async def cleanup_old_data(self, days: int = 30):
        """Очистка старых данных"""
        timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        await self._conn.execute(
            "DELETE FROM numbers WHERE created_at < ? AND status IN ('completed', 'failed')",
            (timestamp,)
        )
        await self._conn.commit()
        logger.info(f"🧹 Очищены данные старше {days} дней")

    async def close(self):
        """Закрытие соединения с БД"""
        if self._conn:
            await self._conn.close()
            logger.info("✅ Соединение с БД закрыто")

db = Database(cfg.DB_PATH)

# --- WORKER ---
@dataclass
class Worker:
    user_id: int
    client: Optional[TelegramClient] = None
    task: Optional[asyncio.Task] = None
    status: WorkerStatus = WorkerStatus.OFFLINE
    current_phone: Optional[str] = None
    waiting_for_code: bool = False
    waiting_for_photo: bool = False
    processed_count: int = 0
    error_count: int = 0
    started_at: Optional[int] = None
    last_activity: Optional[int] = None

    async def start(self) -> bool:
        """Запуск воркера"""
        try:
            session_path = cfg.SESSION_DIR / f"user_{self.user_id}"
            self.client = TelegramClient(
                str(session_path), 
                cfg.API_ID, 
                cfg.API_HASH,
                connection_retries=5,
                retry_delay=3
            )
            
            await self.client.connect()

            if not await self.client.is_user_authorized():
                logger.error(f"❌ Воркер {self.user_id} не авторизован")
                return False

            self._setup_handlers()
            self.task = asyncio.create_task(self._run())
            self.status = WorkerStatus.ONLINE
            self.started_at = int(datetime.now().timestamp())
            
            logger.info(f"✅ Воркер {self.user_id} успешно запущен")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка запуска воркера {self.user_id}: {e}")
            self.status = WorkerStatus.ERROR
            return False

    async def _run(self):
        """Основной цикл работы воркера"""
        try:
            await self.client.run_until_disconnected()
        except Exception as e:
            logger.error(f"❌ Воркер {self.user_id} упал: {e}")
            self.status = WorkerStatus.ERROR

    def _setup_handlers(self):
        """Настройка обработчиков команд"""
        
        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.ping$'))
        async def cmd_ping(event):
            """Проверка работоспособности"""
            await event.edit("🚀 **TITAN SYSTEM ONLINE**\n\n"
                           f"📊 Обработано: {self.processed_count}\n"
                           f"❌ Ошибок: {self.error_count}\n"
                           f"⏱ Uptime: {self._get_uptime()}")

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.au$'))
        async def cmd_au(event):
            """Автоматическая отправка приветствия"""
            await event.edit(
                "✅ **Приветствую!**\n\n"
                "Ответьте на это сообщение, чтобы я сохранил ваши номера "
                "и отправил вам коды на них."
            )
            self.waiting_for_code = True
            self.last_activity = int(datetime.now().timestamp())
            logger.info(f"📝 Воркер {self.user_id}: команда .au выполнена")

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.u$'))
        async def cmd_u(event):
            """Запрос номера из базы"""
            phone = await db.get_available_number(self.user_id)
            
            if not phone:
                await event.edit("❌ **Нет доступных номеров в базе**\n\n"
                               "Добавьте номера через бота.")
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
            logger.info(f"📞 Воркер {self.user_id}: выдан номер {phone}")

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.qr$'))
        async def cmd_qr(event):
            """Запрос QR-кода"""
            await event.edit(
                "🔲 **QR-код запрошен**\n\n"
                "Пользователь запросил авторизацию через QR-код."
            )
            self.last_activity = int(datetime.now().timestamp())
            logger.info(f"🔲 Воркер {self.user_id}: запрошен QR-код")

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.v$'))
        async def cmd_v(event):
            """Подтверждение входа с инлайн кнопкой"""
            if not self.current_phone:
                await event.edit("❌ **Нет активного номера**\n\n"
                               "Используйте `.u` для получения номера.")
                return
            
            from telethon import Button
            
            buttons = [[Button.inline("✅ Слёт", b"slet")]]
            
            await event.edit(
                f"📞 **Номер встал:** `{self.current_phone}`\n\n"
                f"✅ Успешный вход подтверждён",
                buttons=buttons
            )
            self.last_activity = int(datetime.now().timestamp())
            logger.info(f"✅ Воркер {self.user_id}: номер {self.current_phone} встал")@self.client.on(events.CallbackQuery(pattern=b"slet"))
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
                
                logger.info(f"✅ Воркер {self.user_id}: операция с {self.current_phone} завершена")
                
                self.current_phone = None
                self.waiting_for_photo = False
                self.status = WorkerStatus.ONLINE

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.report$'))
        async def cmd_report(event):
            """Генерация отчёта"""
            await event.edit("📊 **Генерирую отчёт...**")
            
            try:
                data = await db.get_report_data(days=7)
                
                if not data:
                    await event.edit("📊 **Нет данных для отчёта**")
                    return

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

                await self.client.send_file(
                    'me',
                    csv_path,
                    caption=f"📊 **Отчёт за последние 7 дней**\n\n"
                           f"📅 Сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                           f"📝 Записей: {len(data)}"
                )
                
                await event.delete()
                csv_path.unlink()
                
                logger.info(f"📊 Воркер {self.user_id}: отчёт сгенерирован ({len(data)} записей)")
                
            except Exception as e:
                await event.edit(f"❌ **Ошибка генерации отчёта:**\n`{str(e)}`")
                logger.error(f"Ошибка генерации отчёта: {e}")

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'^\.stats$'))
        async def cmd_stats(event):
            """Статистика воркера"""
            uptime = self._get_uptime()
            success_rate = (self.processed_count / (self.processed_count + self.error_count) * 100 
                          if (self.processed_count + self.error_count) > 0 else 0)
            
            await event.edit(
                f"📊 **Статистика воркера**\n\n"
                f"🆔 ID: `{self.user_id}`\n"
                f"🟢 Статус: {self.status.value}\n"
                f"⏱ Uptime: {uptime}\n"
                f"✅ Обработано: {self.processed_count}\n"
                f"❌ Ошибок: {self.error_count}\n"
                f"📈 Success rate: {success_rate:.1f}%\n"
                f"📱 Текущий номер: {self.current_phone or 'нет'}"
            )

        @self.client.on(events.NewMessage(incoming=True))
        async def handle_incoming(event):
            """Обработка входящих сообщений"""
            
            if self.waiting_for_code and event.message.message:
                code_text = event.message.message
                
                import re
                code_match = re.search(r'\b\d{5,6}\b', code_text)
                
                if code_match and self.current_phone:
                    await db.update_number_status(
                        self.current_phone, 
                        NumberStatus.CODE_RECEIVED
                    )
                    logger.info(f"✅ Воркер {self.user_id}: код получен для {self.current_phone}")
                    self.waiting_for_code = False

            if self.waiting_for_photo and event.message.photo:
                if self.current_phone:
                    await db.update_number_status(
                        self.current_phone, 
                        NumberStatus.PHOTO_RECEIVED
                    )
                    logger.info(f"📷 Воркер {self.user_id}: фото получено для {self.current_phone}")

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
            
            self.status = WorkerStatus.OFFLINE
            logger.info(f"🛑 Воркер {self.user_id} остановлен")
            
        except Exception as e:
            logger.error(f"Ошибка остановки воркера {self.user_id}: {e}")

WORKERS: Dict[int, Worker] = {}

# --- FSM STATES ---
class AddNumberStates(StatesGroup):
    waiting_for_numbers = State()

# --- BOT ---
bot = Bot(token=cfg.BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()

def get_main_keyboard() -> InlineKeyboardMarkup:
    """Главная клавиатура"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Запустить воркер", callback_data="start_worker")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="➕ Добавить номера", callback_data="add_numbers")],
        [InlineKeyboardButton(text="📈 Общая статистика", callback_data="global_stats")],
        [InlineKeyboardButton(text="🛑 Остановить воркер", callback_data="stop_worker")],
        [InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help")]
    ])

@router.message(Command("start"))
async def cmd_start(msg: Message):
    """Команда /start"""
    await db.add_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    
    welcome_text = (
        "💎 **TITAN PRO v76.0**\n\n"
        "🔥 Профессиональная система автоматизации\n\n"
        "**Возможности:**\n"
        "• Автоматическая обработка номеров\n"
        "• Управление через юзербота\n"
        "• Детальная статистика\n"
        "• Генерация отчётов\n\n"
        "Выберите действие:"
    )
    
    await msg.answer(welcome_text, reply_markup=get_main_keyboard())

@router.callback_query(F.data == "start_worker")
async def cb_start_worker(call: CallbackQuery):
    """Запуск воркера"""
    user_id = call.from_user.id
    
    if user_id in WORKERS and WORKERS[user_id].status != WorkerStatus.OFFLINE:
        await call.answer("⚠️ Воркер уже запущен", show_alert=True)
        return

    if len(WORKERS) >= cfg.MAX_WORKERS:
        await call.answer("⚠️ Достигнут лимит активных воркеров", show_alert=True)
        return

    await call.message.edit_text("⏳ Запускаю воркер...")
    
    worker = Worker(user_id=user_id)
    if await worker.start():
        WORKERS[user_id] = worker
        await call.message.edit_text(
            "✅ **Воркер успешно запущен!**\n\n"
            f"🆔 ID: `{user_id}`\n"
            f"🟢 Статус: Online\n\n"
            "Теперь вы можете использовать команды в Telegram.",
            reply_markup=get_main_keyboard()
        )
    else:
        await call.message.edit_text(
            "❌ **Ошибка запуска воркера**\n\n"
            "Возможные причины:\n"
            "• Не авторизована сессия Telethon\n"
            "• Ошибка подключения к Telegram\n\n"
            "Свяжитесь с администратором.",
            reply_markup=get_main_keyboard()
        )

@router.callback_query(F.data == "stop_worker")
async def cb_stop_worker(call: CallbackQuery):
    """Остановка воркера"""
    user_id = call.from_user.id
    
    if user_id not in WORKERS:
        await call.answer("⚠️ Воркер не запущен", show_alert=True)
        return
    
    await WORKERS[user_id].stop()
    del WORKERS[user_id]
    
    await call.message.edit_text(
        "🛑 **Воркер остановлен**\n\n"
        "Вы можете запустить его снова в любое время.",
        reply_markup=get_main_keyboard()
    )

@router.callback_query(F.data == "my_stats")
async def cb_my_stats(call: CallbackQuery):
    """Статистика пользователя"""
    user_id = call.from_user.id
    stats = await db.get_user_stats(user_id)
    
    success_rate = (stats['completed'] / stats['total'] * 100 
                   if stats['total'] > 0 else 0)
    
    avg_time_str = f"{int(stats['avg_time'])}с" if stats['avg_time'] else "N/A"
    
    worker_status = "🟢 Online" if user_id in WORKERS and WORKERS[user_id].status == WorkerStatus.ONLINE else "🔴 Offline"
    
    text = (
        f"📊 **Ваша статистика**\n\n"
        f"👤 User ID: `{user_id}`\n"
        f"🤖 Воркер: {worker_status}\n\n"
        f"📈 **Операции:**\n"
        f"• Всего: {stats['total']}\n"
        f"• Завершено: {stats['completed']}\n"
        f"• Ошибок: {stats['failed']}\n"
        f"• Success rate: {success_rate:.1f}%\n\n"
        f"⏱ Среднее время: {avg_time_str}"
    )
    
    await call.message.edit_text(text, reply_markup=get_main_keyboard())

@router.callback_query(F.data == "add_numbers")
async def cb_add_numbers(call: CallbackQuery, state: FSMContext):
    """Добавление номеров"""
    await state.set_state(AddNumberStates.waiting_for_numbers)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    
    await call.message.edit_text(
        "📱 **Добавление номеров**\n\n"
        "Отправьте номера в формате:\n"
        "`+7XXXXXXXXXX`\n"
        "или\n"
        "`7XXXXXXXXXX`\n\n"
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
        
        if not line.startswith('+'):
            phone = '+' + phone
        else:
            phone = '+' + phone
        
        if await db.add_number(phone, msg.from_user.id):
            added += 1
        else:
            duplicates += 1
    
    result_text = (
        f"✅ **Результат добавления:**\n\n"
        f"✅ Добавлено: {added}\n"
        f"⚠️ Дубликатов: {duplicates}\n"
        f"❌ Ошибок: {errors}"
    )
    
    await msg.answer(result_text, reply_markup=get_main_keyboard())
    await state.clear()

@router.callback_query(F.data == "cancel")
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    """Отмена операции"""
    await state.clear()
    await call.message.edit_text(
        "❌ **Операция отменена**",
        reply_markup=get_main_keyboard()
    )

@router.callback_query(F.data == "global_stats")
async def cb_global_stats(call: CallbackQuery):
    """Глобальная статистика"""
    workers_count = len(WORKERS)
    active_workers = sum(1 for w in WORKERS.values() 
                        if w.status == WorkerStatus.ONLINE)
    
    total_processed = sum(w.processed_count for w in WORKERS.values())
    total_errors = sum(w.error_count for w in WORKERS.values())
    
    text = (
        f"📈 **Глобальная статистика**\n\n"
        f"🤖 Всего воркеров: {workers_count}\n"
        f"🟢 Активных: {active_workers}\n"
        f"📊 Обработано операций: {total_processed}\n"
        f"❌ Ошибок: {total_errors}\n"
    )
    
    await call.message.edit_text(text, reply_markup=get_main_keyboard())

@router.callback_query(F.data == "help")
async def cb_help(call: CallbackQuery):
    """Справка"""
    help_text = (
        "ℹ️ **Справка по командам**\n\n"
        "**Команды юзербота:**\n"
        "`.au` - Отправить приветствие\n"
        "`.u` - Запросить номер из базы\n"
        "`.v` - Подтвердить вход (с кнопкой Слёт)\n"
        "`.qr` - Запросить QR-код\n"
        "`.report` - Сгенерировать отчёт в CSV\n"
        "`.stats` - Статистика воркера\n"
        "`.ping` - Проверка работоспособности\n\n"
        "**Работа с ботом:**\n"
        "1. Запустите воркер через кнопку\n"
        "2. Добавьте номера в базу\n"
        "3. Используйте команды в Telegram\n"
        "4. Получайте отчёты и статистику\n\n"
        "**Процесс работы:**\n"
        "• `.u` - Получить номер\n"
        "• Отправить фото с кодом\n"
        "• `.v` - Подтвердить вход\n"
        "• Нажать кнопку \"Слёт\"\n"
    )
    
    await call.message.edit_text(help_text, reply_markup=get_main_keyboard())

@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    """Админ панель"""
    if msg.from_user.id != cfg.ADMIN_ID:
        await msg.answer("❌ Доступ запрещён")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Полная статистика", callback_data="admin_full_stats")],
        [InlineKeyboardButton(text="🧹 Очистить старые данные", callback_data="admin_cleanup")],
        [InlineKeyboardButton(text="👥 Список воркеров", callback_data="admin_workers")]
    ])
    
    await msg.answer("🔐 **Админ панель**", reply_markup=kb)

@router.callback_query(F.data == "admin_full_stats")
async def cb_admin_full_stats(call: CallbackQuery):
    """Полная статистика для админа"""
    if call.from_user.id != cfg.ADMIN_ID:
        await call.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    data = await db.get_report_data(days=30)
    
    total = len(data)
    completed = sum(1 for row in data if row[2] == 'completed')
    failed = sum(1 for row in data if row[2] == 'failed')
    
    text = (
        f"📊 **Полная статистика (30 дней)**\n\n"
        f"📝 Всего операций: {total}\n"
        f"✅ Завершено: {completed}\n"
        f"❌ Провалено: {failed}\n"
        f"📈 Success rate: {(completed/total*100 if total > 0 else 0):.1f}%\n"
    )
    
    await call.message.answer(text)

@router.callback_query(F.data == "admin_cleanup")
async def cb_admin_cleanup(call: CallbackQuery):
    """Очистка старых данных"""
    if call.from_user.id != cfg.ADMIN_ID:
        await call.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await db.cleanup_old_data(days=30)
    await call.answer("✅ Старые данные очищены", show_alert=True)

@router.callback_query(F.data == "admin_workers")
async def cb_admin_workers(call: CallbackQuery):
    """Список активных воркеров"""
    if call.from_user.id != cfg.ADMIN_ID:
        await call.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    if not WORKERS:
        await call.message.answer("👥 **Нет активных воркеров**")
        return
    
    text = "👥 **Активные воркеры:**\n\n"
    for user_id, worker in WORKERS.items():
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
        await asyncio.sleep(86400)  # Раз в день
        try:
            await db.cleanup_old_data(days=30)
            logger.info("🧹 Автоматическая очистка выполнена")
        except Exception as e:
            logger.error(f"Ошибка очистки: {e}")

# --- MAIN ---
async def main():
    """Главная функция"""
    await db.init()
    dp.include_router(router)
    
    asyncio.create_task(cleanup_task())
    
    logger.info("🚀 Бот запущен и готов к работе")
    
    try:
        await dp.start_polling(bot)
    finally:
        for worker in WORKERS.values():
            await worker.stop()
        await db.close()
        logger.info("👋 Бот остановлен")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⚠️ Получен сигнал остановки")
    except Exception as e:
        logger.critical(f"💥 Критическая ошибка: {e}")
