import asyncio
import os
import logging

from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
# 🛠️ ИСПРАВЛЕНИЕ: Это новый импорт, необходимый для настройки
# параметров по умолчанию для Bot в aiogram версии 3.7+
from aiogram.client.default import DefaultBotProperties

# 1. КОНФИГУРАЦИЯ
# =========================================================================

# Устанавливаем базовый уровень логирования
logging.basicConfig(level=logging.INFO)

# Получаем токен из переменных окружения.
# Рекомендуется заменить 'YOUR_BOT_TOKEN_HERE' на os.environ.get("BOT_TOKEN")
# и хранить токен в переменной окружения для безопасности.
BOT_TOKEN = "ВАШ_ТОКЕН_БОТА_ЗДЕСЬ" # !!! ЗАМЕНИТЕ НА СВОЙ ТОКЕН !!!

# 2. МАРШРУТИЗАТОР И ХЭНДЛЕРЫ
# =========================================================================

# Инициализация основного маршрутизатора
router = Router()

@router.message(CommandStart())
async def command_start_handler(message: types.Message) -> None:
    """
    Этот хэндлер обрабатывает команду /start.
    Отправляет приветственное сообщение пользователю.
    """
    user_name = message.from_user.full_name if message.from_user else "пользователь"
    
    # Текст сообщения в формате Markdown
    response_text = (
        f"👋 Привет, *{user_name}*! Я твой новый бот.\n\n"
        "Чтобы увидеть это сообщение, я использовал *ParseMode.MARKDOWN*.\n"
        "Я готов к работе!"
    )
    
    await message.answer(response_text)


# 3. ГЛАВНАЯ ФУНКЦИЯ ЗАПУСКА
# =========================================================================

async def main() -> None:
    """
    Основная функция для инициализации и запуска бота.
    """
    
    # 🛠️ ИСПРАВЛЕНИЕ ОШИБКИ TypeError:
    # Вместо прямого parse_mode=ParseMode.MARKDOWN теперь используем 
    # default=DefaultBotProperties(...).
    #
    # Это решает проблему в aiogram 3.7+
    
    default_properties = DefaultBotProperties(
        parse_mode=ParseMode.MARKDOWN,
        # Здесь также можно установить disable_web_page_preview и protect_content
        # disable_web_page_preview=True, 
        # protect_content=False
    )
    
    bot = Bot(token=BOT_TOKEN, default=default_properties)
    dp = Dispatcher()
    
    # Регистрируем маршрутизатор в диспетчере
    dp.include_router(router)

    # Начинаем опрос Telegram-серверов (Polling)
    logging.info("Бот запущен. Начинаю опрос (Polling)...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    # Запуск асинхронной функции main
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен вручную.")
    except Exception as e:
        logging.error(f"Произошла непредвиденная ошибка: {e}")
