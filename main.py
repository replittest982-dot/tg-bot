import os
import re
import io
import asyncio
import logging
from typing import Dict

# --- AIOGRAM IMPORTS ---
from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
# 🛠️ ИСПРАВЛЕНИЕ: Добавлен импорт для aiogram 3.7+
from aiogram.client.default import DefaultBotProperties 

# --- TELETHON IMPORTS ---
from telethon import TelegramClient
from telethon.tl.types import User
from telethon.errors.rpcerrorlist import (
    PhoneNumberInvalidError, FloodWaitError, SessionPasswordNeededError,
    PhoneCodeInvalidError, PhoneCodeExpiredError, PasswordHashInvalidError
)

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =========================================================================
# I. КОНФИГУРАЦИЯ (ВАШИ КЛЮЧИ)
# =========================================================================

API_ID = 29930612
API_HASH = "2690aa8c364b91e47b6da1f90a71f825"
# 🎯 НОВЫЙ ТОКЕН, ВСТАВЛЕН
BOT_TOKEN = "7868097991:AAEH_ftVuHXPe0428PpginsnAjF8iII1PZ8" 

USER_SESSION_DIR = "sessions"
PROXY_CONFIG = None 

if not os.path.exists(USER_SESSION_DIR):
    os.makedirs(USER_SESSION_DIR)

# =========================================================================
# II. ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ И УТИЛИТЫ
# =========================================================================

TEMP_AUTH_CLIENTS: Dict[int, TelegramClient] = {}

def get_session_path(user_id: int) -> str:
    return os.path.join(USER_SESSION_DIR, str(user_id))

def get_display_name(user: User) -> str:
    parts = []
    if user.first_name:
        parts.append(user.first_name)
    if user.last_name:
        parts.append(user.last_name)
    return " ".join(parts) if parts else "Unknown User"

# =========================================================================
# III. FSM СОСТОЯНИЯ ДЛЯ АВТОРИЗАЦИИ
# =========================================================================

class TelethonAuth(StatesGroup):
    PHONE = State()     # Ожидание ввода номера телефона
    CODE = State()      # Ожидание ввода кода
    PASSWORD = State()  # Ожидание ввода 2FA пароля

# =========================================================================
# IV. КЛАВИАТУРЫ
# =========================================================================

def get_start_kb() -> InlineKeyboardMarkup:
    """Клавиатура для начала авторизации."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Вход по Номеру/Коду", callback_data="telethon_auth_phone_start")],
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="cancel_action")],
    ])

def get_cancel_kb() -> InlineKeyboardMarkup:
    """Клавиатура для отмены действия."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="cancel_action")],
    ])

# =========================================================================
# V. ОСНОВНЫЕ ХЭНДЛЕРЫ И НАСТРОЙКА BOT (С ИСПРАВЛЕНИЕМ)
# =========================================================================

router = Router()
# Инициализация BOT с ИСПРАВЛЕНИЕМ для aiogram 3.7+
default_properties = DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
bot = Bot(token=BOT_TOKEN, default=default_properties)

@router.message(CommandStart())
async def command_start_handler(message: types.Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    await state.clear()
    
    session_exists = os.path.exists(get_session_path(user_id) + '.session')
    
    if session_exists:
        text = "✅ **Вы уже авторизованы!**\nВаша сессия сохранена."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Удалить сессию", callback_data="logout_session")],
        ])
    else:
        text = "👋 **Добро пожаловать!**\nДля использования функций бота требуется авторизация через Telegram."
        kb = get_start_kb()
        
    await message.answer(text, reply_markup=kb)

@router.callback_query(F.data == "logout_session")
async def logout_session(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await state.clear()

    try:
        # Безопасно удаляем временный клиент
        client_to_disconnect = TEMP_AUTH_CLIENTS.pop(user_id, None)
        if client_to_disconnect:
            await client_to_disconnect.disconnect()
            
        # Удаляем финальный файл сессии
        session_path = get_session_path(user_id) + '.session'
        if os.path.exists(session_path):
            os.remove(session_path)
        
        await callback.message.edit_text("❌ Сессия успешно удалена.", reply_markup=get_start_kb())
    except Exception as e:
        logger.error(f"Error during logout for {user_id}: {e}")
        await callback.message.edit_text(f"❌ Ошибка при удалении сессии: {type(e).__name__}", reply_markup=get_start_kb())


# =========================================================================
# VI. ХЭНДЛЕРЫ TELETHON АВТОРИЗАЦИИ (FSM: PHONE/CODE/PASSWORD)
# =========================================================================

# --- УТИЛИТА ЗАВЕРШЕНИЯ ВХОДА ---
async def finalize_telethon_login(user_id, client: TelegramClient, state: FSMContext, message_or_callback):
    """Финальный этап после успешного sign_in."""
    
    # 1. Переносим файл сессии
    temp_path = get_session_path(user_id) + '_temp.session'
    final_path = get_session_path(user_id) + '.session'
    
    # 2. Очищаем временные данные
    try:
        if os.path.exists(temp_path):
            if os.path.exists(final_path):
                os.remove(final_path)
            os.rename(temp_path, final_path)
            
    except Exception as e:
        logger.warning(f"Error during file rename for {user_id}: {e}")
    finally:
        if user_id in TEMP_AUTH_CLIENTS:
            del TEMP_AUTH_CLIENTS[user_id]
                
    # 3. Отправляем финальное сообщение
    try:
        me = await client.get_me()
        username = f"@{me.username}" if me.username else "Нет юзернейма"
        
        text = (
            "✅ **Авторизация успешна!**\n"
            f"Аккаунт: **{get_display_name(me)}** ({username})"
        )
    except Exception:
        text = "✅ **Авторизация успешна!**"

    
    if isinstance(message_or_callback, types.Message):
        await message_or_callback.answer(text, reply_markup=get_start_kb())
    else:
        await message_or_callback.message.edit_text(text, reply_markup=get_start_kb())
        
    await state.clear()


# --- НАЧАЛО: Вход по Номеру/Коду ---
@router.callback_query(F.data == "telethon_auth_phone_start")
async def start_telethon_auth_phone(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await callback.answer()

    await state.clear()
    await state.set_state(TelethonAuth.PHONE)
    
    try:
        if user_id in TEMP_AUTH_CLIENTS:
            await TEMP_AUTH_CLIENTS[user_id].disconnect()
            del TEMP_AUTH_CLIENTS[user_id]
        
        temp_path = get_session_path(user_id) + '_temp'
        # Создаем клиента Telethon
        temp_client = TelegramClient(temp_path, API_ID, API_HASH, proxy=PROXY_CONFIG, device_model='Android Client')
        TEMP_AUTH_CLIENTS[user_id] = temp_client
        
        await callback.message.edit_text(
            "📞 **Шаг 1/3: Ввод номера телефона**\n"
            "Введите номер телефона для авторизации в формате `+79XXXXXXXXX`:",
            reply_markup=get_cancel_kb()
        )
    except Exception as e:
        logger.error(f"Error starting temp client for {user_id}: {e}")
        TEMP_AUTH_CLIENTS.pop(user_id, None)
        await callback.message.edit_text(f"❌ Критическая ошибка: Не удалось запустить временный клиент. {type(e).__name__}", reply_markup=get_start_kb())
        await state.clear()

# --- ШАГ 1: Обработка номера ---
@router.message(TelethonAuth.PHONE)
async def process_phone(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    phone = message.text.strip()
    
    if not re.match(r'^\+?\d{10,15}$', phone): 
        await message.reply("❌ Неверный формат. Пожалуйста, введите номер в формате `+79XXXXXXXXX`.", reply_markup=get_cancel_kb())
        return

    client = TEMP_AUTH_CLIENTS.get(user_id)
    if not client:
        await message.reply("❌ Ошибка сессии. Начните авторизацию заново.", reply_markup=get_start_kb())
        await state.clear()
        return

    try:
        await message.answer("⏳ Отправляю код...")
        await client.connect() 
        # ЗАПРОС КОДА
        sent_code_hash = await client.send_code_request(phone)
        
        await state.update_data(phone=phone, sent_code_hash=sent_code_hash)
        await state.set_state(TelethonAuth.CODE)
        
        await message.answer(
            "🔑 **Шаг 2/3: Ввод кода**\n"
            f"Код отправлен на номер `{phone}`. Введите его:",
            reply_markup=get_cancel_kb()
        )
    except PhoneNumberInvalidError:
        await message.answer("❌ Неверный номер телефона. Попробуйте снова.", reply_markup=get_cancel_kb())
    except FloodWaitError as e:
        await message.answer(f"❌ Ограничение Telegram: Повторите попытку через **{e.seconds}** секунд.", reply_markup=get_cancel_kb())
    except SessionPasswordNeededError:
        # Если нужен 2FA, пропускаем шаг кода
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer(
            "🔒 **Шаг 3/3: Требуется двухфакторная аутентификация (2FA)**\n"
            "Введите пароль облачного хранилища Telegram:",
            reply_markup=get_cancel_kb()
        )
    except Exception as e:
        logger.error(f"Phone input error for {user_id}: {e}")
        await message.answer(f"❌ Неизвестная ошибка при запросе кода: {type(e).__name__}", reply_markup=get_cancel_kb())


# --- ШАГ 2: Обработка кода ---
@router.message(TelethonAuth.CODE)
async def process_code(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip()
    data = await state.get_data()
    
    if not code.isdigit() or len(code) < 4:
        await message.reply("❌ Код должен быть числом.", reply_markup=get_cancel_kb())
        return

    client = TEMP_AUTH_CLIENTS.get(user_id)
    if not client or 'phone' not in data or 'sent_code_hash' not in data:
        await message.reply("❌ Ошибка сессии. Начните авторизацию заново.", reply_markup=get_start_kb())
        await state.clear()
        return
        
    try:
        await message.answer("⏳ Проверяю код...")
        
        # ВХОД С КОДОМ
        await client.sign_in(data['phone'], code, phone_code_hash=data['sent_code_hash'].phone_code_hash)
        
        # Успешный вход!
        await finalize_telethon_login(user_id, client, state, message)
        
    except PhoneCodeInvalidError:
        await message.answer("❌ Неверный код. Попробуйте еще раз.", reply_markup=get_cancel_kb())
    except PhoneCodeExpiredError:
        await message.answer("❌ Срок действия кода истек. Начните авторизацию заново.", reply_markup=get_start_kb())
        await state.clear()
    except SessionPasswordNeededError:
        await state.set_state(TelethonAuth.PASSWORD)
        await message.answer(
            "🔒 **Шаг 3/3: Требуется двухфакторная аутентификация (2FA)**\n"
            "Введите пароль облачного хранилища Telegram:",
            reply_markup=get_cancel_kb()
        )
    except Exception as e:
        logger.error(f"Code input error for {user_id}: {e}")
        await message.answer(f"❌ Неизвестная ошибка при вводе кода: {type(e).__name__}", reply_markup=get_cancel_kb())


# --- ШАГ 3: Обработка 2FA-пароля ---
@router.message(TelethonAuth.PASSWORD)
async def process_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    password = message.text.strip()
    
    client = TEMP_AUTH_CLIENTS.get(user_id)
    if not client:
        await message.reply("❌ Ошибка сессии. Начните авторизацию заново.", reply_markup=get_start_kb())
        await state.clear()
        return
        
    try:
        await message.answer("⏳ Проверяю пароль...")
        
        # ВХОД С ПАРОЛЕМ
        await client.sign_in(password=password)
        
        # Успешный вход!
        await finalize_telethon_login(user_id, client, state, message)
        
    except PasswordHashInvalidError:
        await message.answer("❌ Неверный пароль. Попробуйте еще раз.", reply_markup=get_cancel_kb())
    except Exception as e:
        logger.error(f"Password input error for {user_id}: {e}")
        await message.answer(f"❌ Неизвестная ошибка при вводе пароля: {type(e).__name__}", reply_markup=get_cancel_kb())

# --- ГЛОБАЛЬНАЯ ОТМЕНА ДЕЙСТВИЯ FSM ---
@router.callback_query(F.data == "cancel_action", state="*")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("Действие отменено.")
        
    user_id = callback.from_user.id
    
    # Очистка временного клиента
    client_to_disconnect = TEMP_AUTH_CLIENTS.pop(user_id, None) 
    if client_to_disconnect:
        try:
            if client_to_disconnect.is_connected():
                await client_to_disconnect.disconnect()
        except:
            pass
        
    await state.clear()
    
    await callback.message.edit_text("🏠 Главное меню:", reply_markup=get_start_kb())

# =========================================================================
# VII. ЗАПУСК БОТА
# =========================================================================

async def main() -> None:
    dp = Dispatcher()
    dp.include_router(router)
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by KeyboardInterrupt.")
    except Exception as e:
        logger.error(f"Fatal error during bot runtime: {e}")
