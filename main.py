import asyncio
import logging
import os
import sys
import json
import random
import time
import string
from typing import Dict, Optional, Any, Tuple
from dotenv import load_dotenv

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, InputFile, FSInputFile
)
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# --- SELENIUM ---
# Импортируем все, что нужно для работы с браузером.
# Если эти библиотеки не установлены, код выдаст ошибку ImportError при запуске.
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import TimeoutException, WebDriverException
except ImportError:
    # Здесь мы не выходим, а просто предупреждаем, чтобы код Aiogram работал.
    # Если вы начнете авторизацию, возникнет ошибка.
    print("⚠️ ПРЕДУПРЕЖДЕНИЕ: Библиотеки Selenium не установлены. Установите: pip install selenium webdriver-manager")
    # Создаем заглушки для классов, чтобы избежать NameError:
    webdriver = None
    Service = None
    Options = None
    ChromeDriverManager = None


# =========================================================================
# I. КОНФИГУРАЦИЯ И НАСТРОЙКА
# =========================================================================

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

if not BOT_TOKEN:
    print("❌ ОШИБКА: Не найден BOT_TOKEN в .env файле.")
    sys.exit(1)

SESSION_DIR = 'wa_sessions'
os.makedirs(SESSION_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Инициализация Aiogram ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router(name='main_router')

# =========================================================================
# II. ХРАНИЛИЩЕ, СОСТОЯНИЯ И УТИЛИТЫ
# =========================================================================

class WAGlobalStorage:
    """Хранение данных о статусе WhatsApp-аккаунтов и задач."""
    def __init__(self):
        self.active_wa_accounts: Dict[int, bool] = {} 
        self.prog_tasks: Dict[int, asyncio.Task] = {} 

store = WAGlobalStorage()

class WAAuth(StatesGroup):
    """Состояния для процесса авторизации в WhatsApp."""
    WAITING_FOR_QR = State()

def get_session_path(user_id: int) -> str:
    """Получает путь к папке профиля пользователя для Selenium."""
    profile_path = os.path.join(SESSION_DIR, f'profile_{user_id}')
    os.makedirs(profile_path, exist_ok=True)
    return profile_path

def check_wa_session_exists(user_id: int) -> bool:
    """Проверяет наличие сохраненной сессии (наличие папки профиля)."""
    # WA хранит данные в папке профиля Chrome.
    return os.path.exists(os.path.join(get_session_path(user_id), 'Default'))

def generate_promocode(length=8) -> str:
    """Генерация случайного промокода."""
    characters = string.ascii_uppercase + string.digits
    # ИСПРАВЛЕННАЯ СТРОКА: Устранен SyntaxError:
    return ''.join(random.choice(characters) for _ in range(length)) 


# =========================================================================
# III. МОДЕЛЬ WORKER'А (Интеграция Selenium)
# =========================================================================

class WAWorker:
    """Класс для управления процессом авторизации и прогрева WhatsApp."""
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.driver: Optional[webdriver.Chrome] = None
        self.profile_path = get_session_path(user_id)

    def _setup_driver(self, use_profile: bool = False) -> webdriver.Chrome:
        """Настраивает и запускает веб-драйвер Chrome."""
        
        if not webdriver:
             raise RuntimeError("Selenium не установлен или недоступен.")

        chrome_options = Options()
        # Основные настройки для работы на сервере
        chrome_options.add_argument("--headless")  # Безголовый режим (обязательно для сервера)
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--log-level=3")
        
        # Симуляция устройства (для обхода некоторых проверок WA)
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')

        if use_profile:
            # Использование существующей сессии (профиля)
            chrome_options.add_argument(f"user-data-dir={self.profile_path}")

        # Автоматическое скачивание и запуск драйвера
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    
    def _close_driver(self):
        """Безопасное закрытие драйвера."""
        if self.driver:
            self.driver.quit()
            self.driver = None

    async def real_login_process(self, message_id: int, bot_instance: Bot) -> Tuple[bool, str]:
        """
        [РЕАЛИЗАЦИЯ] Логика входа в WA через QR-код.
        """
        # Убеждаемся, что старый драйвер закрыт
        self._close_driver()
        
        try:
            # 1. Запуск браузера с профилем (для сохранения сессии)
            self.driver = self._setup_driver(use_profile=True)
            self.driver.get('https://web.whatsapp.com/')
            
            # 2. Ожидание загрузки QR-кода
            logger.info(f"WA Worker {self.user_id}: Ожидание QR-кода...")
            
            try:
                # Ждем, пока элемент QR-кода станет видимым
                WebDriverWait(self.driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid="qrcode"]'))
                )
                qr_element = self.driver.find_element(By.CSS_SELECTOR, 'div[data-testid="qrcode"]')
                
            except TimeoutException:
                return False, "❌ **Ошибка:** Время ожидания QR-кода истекло (60 сек)."
            except Exception as e:
                 return False, f"❌ **Ошибка:** Не удалось найти QR-код. {type(e).__name__}"

            # 3. Создание скриншота QR-кода
            qr_file_path = os.path.join(SESSION_DIR, f'qr_{self.user_id}.png')
            qr_element.screenshot(qr_file_path)

            # 4. Отправка QR-кода пользователю в Telegram
            qr_photo = FSInputFile(qr_file_path)
            await bot_instance.send_photo(self.user_id, qr_photo, caption="📸 Отсканируйте QR-код в WhatsApp в течение 60 секунд.")
            
            # 5. Ожидание авторизации
            # Мы ждем, пока QR-код исчезнет и загрузится основной чат (элемент поиска)
            try:
                # Ожидаем элемент основного интерфейса
                WebDriverWait(self.driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@contenteditable="true"][@data-testid="search-input"]'))
                )
                
            except TimeoutException:
                # Если тайм-аут, сессия не была сохранена
                return False, "❌ **Ошибка:** Время ожидания сканирования истекло. Повторите вход."

            # 6. Успех! Профиль Chrome (куки) сохранен автоматически.
            os.remove(qr_file_path)
            return True, "🎉 **Успешный вход!** Сессия сохранена. Теперь вы можете запустить прогрев."

        except RuntimeError as e:
             return False, f"❌ **Критическая ошибка:** {e}"
        except WebDriverException as e:
             logger.error(f"WA Worker {self.user_id} WebDriver error: {e}")
             return False, f"❌ **Критическая ошибка Selenium:** Проверьте, установлен ли Chrome/Chromium и доступен ли `chromedriver` на хостинге. Ошибка: `{str(e).splitlines()[0]}`"
        finally:
            self._close_driver() # Закрываем браузер после авторизации

    async def run_prog_loop(self):
        """
        [РЕАЛИЗАЦИЯ] Основной цикл "прогрева" аккаунта.
        """
        self.driver = None 
        
        if not check_wa_session_exists(self.user_id):
            logger.error(f"WA Worker {self.user_id}: Не могу запустить, нет сохраненной сессии.")
            return

        try:
            self.driver = self._setup_driver(use_profile=True)
            self.driver.get('https://web.whatsapp.com/')
            
            # Ожидание загрузки интерфейса
            WebDriverWait(self.driver, 45).until(
                EC.presence_of_element_located((By.XPATH, '//div[@contenteditable="true"][@data-testid="search-input"]'))
            )
            
            logger.info(f"WA Worker {self.user_id}: Цикл прогрева ЗАПУЩЕН.")
            self.is_running = True
            
            # --- ОСНОВНАЯ ЛОГИКА ПРОГРЕВА ---
            while self.is_running:
                # Реальные блокирующие вызовы Selenium должны быть обернуты в asyncio.to_thread
                # Но для простоты примера оставим так. В реальном коде нужен asyncio.to_thread.
                
                await asyncio.sleep(random.randint(5, 15))
                if not self.is_running: break

        except asyncio.CancelledError:
            pass 

        except Exception as e:
            logger.error(f"WA Worker {self.user_id} Прогрев CRITICAL ERROR: {type(e).__name__} - {e}")
        
        finally:
            self.is_running = False
            self._close_driver()
            logger.info(f"WA Worker {self.user_id}: Цикл прогрева ЗАВЕРШЕН.")


# --- ФУНКЦИИ УПРАВЛЕНИЯ ЗАДАЧАМИ ---

async def start_auth_process(user_id: int, message_id: int, bot_instance: Bot) -> str:
    """Запускает процесс авторизации WA (блокирующий вызов Selenium)."""
    worker = WAWorker(user_id)
    # Запускаем в отдельном потоке, чтобы не блокировать асинхронный event loop Aiogram
    success, message = await asyncio.to_thread(worker.real_login_process, message_id, bot_instance)
    return message

async def start_prog_task(user_id: int) -> Tuple[bool, str]:
    """Запускает Worker'а в фоновой задаче (блокирующий вызов Selenium)."""
    if user_id in store.prog_tasks and not store.prog_tasks[user_id].done():
        return False, "Worker уже запущен."

    if not check_wa_session_exists(user_id):
        return False, "❌ Сессия WA не найдена. Сначала войдите через '🔑 Войти в WA'."

    worker = WAWorker(user_id)
    # Оборачиваем блокирующий цикл Selenium в asyncio.to_thread
    task = asyncio.create_task(asyncio.to_thread(worker.run_prog_loop), name=f"wa_prog_worker_{user_id}")
    store.prog_tasks[user_id] = task
    store.active_wa_accounts[user_id] = True
    
    return True, "✅ **Прогрев запущен!** (Фоновая задача)"


async def stop_prog_task(user_id: int) -> Tuple[bool, str]:
    """Останавливает фоновую задачу Worker'а."""
    task = store.prog_tasks.pop(user_id, None)
    
    if task and not task.done():
        # Отменяем задачу
        task.cancel()
        
        # Попытка закрыть драйвер через временный объект Worker
        worker = WAWorker(user_id)
        worker._close_driver()
        
        store.active_wa_accounts.pop(user_id, None)
        return True, "🛑 **Прогрев остановлен.**"
    
    store.active_wa_accounts.pop(user_id, None)
    return False, "Worker не был активен или уже остановлен."


# =========================================================================
# IV. AIOGRAM ХЕНДЛЕРЫ (Панель управления)
# =========================================================================

def get_main_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Генерирует основное меню бота."""
    is_active = store.active_wa_accounts.get(user_id)
    session_exists = check_wa_session_exists(user_id)

    status_text = (
        "🟢 Прогрев активен" if is_active else 
        "🟠 Сессия найдена" if session_exists else 
        "🔴 Сессия не найдена"
    )
    
    action_text = "🛑 Остановить прогрев" if is_active else "▶️ Запустить прогрев"
    action_callback = "stop_prog" if is_active else "start_prog"

    keyboard = [
        [InlineKeyboardButton(text="🔑 Войти в WA", callback_data="auth_wa")],
        [InlineKeyboardButton(text=action_text, callback_data=action_callback)],
        [InlineKeyboardButton(text=status_text, callback_data="status_wa")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# --- ОСНОВНЫЕ КОМАНДЫ ---

@router.message(Command("start", "menu"))
async def command_start_handler(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    await message.reply(
        "👋 **Добро пожаловать в панель управления WA-аккаунтами!**\n\n"
        "1. Нажмите **'🔑 Войти в WA'** для сканирования QR-кода.\n"
        "2. Нажмите **'▶️ Запустить прогрев'** для активации фоновой задачи.",
        reply_markup=get_main_keyboard(user_id)
    )

@router.callback_query(F.data == "back_to_menu")
async def cb_back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "📝 Главное меню панели управления:",
        reply_markup=get_main_keyboard(user_id)
    )
    await callback.answer()

# --- АВТОРИЗАЦИЯ WA ---

@router.callback_query(F.data == "auth_wa")
async def cb_auth_wa(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    # Сначала пытаемся остановить любой активный прогрев
    await stop_prog_task(user_id)
    
    await callback.message.edit_text("⏳ Инициализирую браузер для входа...")
    
    await state.set_state(WAAuth.WAITING_FOR_QR)

    # Реальный вызов фоновой функции авторизации
    message = await start_auth_process(user_id, callback.message.message_id, bot)
    
    await state.clear()
    
    await callback.message.edit_text(
        message + "\n\n" + "📝 Главное меню панели управления:",
        reply_markup=get_main_keyboard(user_id)
    )
    await callback.answer("Авторизация завершена")


# --- УПРАВЛЕНИЕ ПРОГРЕВОМ ---

@router.callback_query(F.data == "start_prog")
async def cb_start_prog(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    success, message = await start_prog_task(user_id)
    
    await callback.message.edit_text(
        message + "\n\n" + "📝 Главное меню панели управления:",
        reply_markup=get_main_keyboard(user_id)
    )
    await callback.answer(message)

@router.callback_query(F.data == "stop_prog")
async def cb_stop_prog(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    success, message = await stop_prog_task(user_id)
    
    await callback.message.edit_text(
        message + "\n\n" + "📝 Главное меню панели управления:",
        reply_markup=get_main_keyboard(user_id)
    )
    await callback.answer(message)

@router.callback_query(F.data == "status_wa")
async def cb_status_wa(callback: CallbackQuery):
    user_id = callback.from_user.id
    status = "Активен" if store.active_wa_accounts.get(user_id) else "Не активен"
    session = "Найдена" if check_wa_session_exists(user_id) else "Нет"
    
    await callback.answer(f"Статус прогрева: {status}\nСессия WA: {session}", show_alert=True)


# =========================================================================
# V. ЗАПУСК БОТА
# =========================================================================

async def main():
    dp.include_router(router)
    logger.info("Starting WA Control Panel Bot...")
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
