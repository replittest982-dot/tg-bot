#!/usr/bin/env python3
"""
🚀 StatPro Auth Core v5.1 - ФИНАЛЬНАЯ РАБОЧАЯ ВЕРСИЯ
✅ Исправлена ошибка импорта 'Path'.
✅ Мультисессионная поддержка (сохранение по User ID).
✅ Автоматическое уведомление об успехе.
"""

import asyncio
import logging
import os
import sys
import io
# 💥 ИСПРАВЛЕНИЕ: Path импортируется из pathlib, а не typing.
from typing import Dict 
from pathlib import Path 

# --- AIOGRAM v3.x ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile
)
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

# --- TELETHON ---
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError, PhoneNumberInvalidError, PhoneCodeInvalidError,
    PasswordHashInvalidError
)

# --- QR/IMAGE ---
import qrcode
from PIL import Image

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0)) 
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    
    QR_TIMEOUT = int(os.getenv("QR_TIMEOUT", "180")) 
    
except ValueError as e:
    print(f"❌ ОШИБКА КОНФИГУРАЦИИ: Неверный формат числовой переменной: {e}. Проверьте ADMIN_ID или API_ID.")
    sys.exit(1)

# Проверка критических переменных
REQUIRED_ENVS = {"BOT_TOKEN": BOT_TOKEN, "API_ID": API_ID, "API_HASH": API_HASH}
if not all(REQUIRED_ENVS.values()):
    missing = [k for k, v in REQUIRED_ENVS.items() if not v]
    print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Не хватает переменных: {', '.join(missing)}")
    sys.exit(1)


SESSION_DIR = Path(__file__).parent / "sessions"
SESSION_DIR.mkdir(exist_ok=True)

def get_session_path(user_id: int) -> Path:
    """
    Путь для сохранения сессии Telethon.
    Сессия всегда привязывается к ID пользователя, который проходит вход.
    """
    return SESSION_DIR / f"session_{user_id}"

# =========================================================================
# II. ЛОГГИРОВАНИЕ
# =========================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# =========================================================================
# III. ИНИЦИАЛИЗАЦИЯ И ХРАНИЛИЩЕ
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())
auth_router = Router()
dp.include_router(auth_router)

# Хранилище для активных клиентов Telethon во время авторизации
AUTH_CLIENTS: Dict[int, TelegramClient] = {}

async def clear_auth_client(user_id: int):
    """Закрывает и удаляет временный клиент Telethon."""
    client = AUTH_CLIENTS.pop(user_id, None)
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass

# =========================================================================
# IV. FSM STATES
# =========================================================================

class AuthStates(StatesGroup):
    """Состояния для процесса авторизации."""
    PHONE = State()
    CODE = State()
    PASSWORD = State()

# =========================================================================
# V. КЛАВИАТУРЫ
# =========================================================================

def get_main_kb() -> InlineKeyboardMarkup:
    """Главная клавиатура. Кнопка проверки сессии удалена."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth_menu")],
    ])

def get_auth_menu_kb() -> InlineKeyboardMarkup:
    """Меню выбора типа входа."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 По номеру", callback_data="auth_phone"), 
         InlineKeyboardButton(text="📸 По QR-коду", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])

# =========================================================================
# VI. HANDLERS (ОСНОВНОЙ РОУТЕР)
# =========================================================================

@auth_router.message(Command("start"))
async def cmd_start(message: Message):
    """Начало работы с ботом."""
    await message.answer("👋 Добро пожаловать в Auth Core! Выберите способ входа:", reply_markup=get_main_kb())

@auth_router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """Отмена текущего действия."""
    await clear_auth_client(message.from_user.id)
    await state.clear()
    await message.answer("✅ Действие отменено.", reply_markup=get_main_kb())

@auth_router.callback_query(F.data == "main_menu")
async def cb_main_menu(call: CallbackQuery, state: FSMContext):
    """Возврат в главное меню."""
    await clear_auth_client(call.from_user.id)
    await state.clear()
    await call.message.edit_text("Главное меню:", reply_markup=get_main_kb())
    await call.answer()

@auth_router.callback_query(F.data == "auth_menu")
async def cb_auth_menu(call: CallbackQuery):
    """Меню выбора типа входа."""
    await call.message.edit_text("Выберите метод входа:", reply_markup=get_auth_menu_kb())
    await call.answer()

# --- ВХОД ПО QR-КОДУ ---

@auth_router.callback_query(F.data == "auth_qr")
async def auth_qr_start(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    await clear_auth_client(user_id)
    
    # 1. Создаем новый клиент Telethon, сессия будет называться session_<user_id>
    client = TelegramClient(str(get_session_path(user_id)), API_ID, API_HASH)
    AUTH_CLIENTS[user_id] = client
    
    try:
        await client.connect()
        qr_login = await client.qr_login()
        
        # 2. Генерируем QR-код
        qr = qrcode.QRCode(box_size=4, border=4)
        qr.add_data(qr_login.url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
        
        bio = io.BytesIO()
        img.save(bio, format='PNG')
        bio.seek(0)
        
        # 3. Отправляем QR-код и ждем сканирования
        sent = await call.message.answer_photo(
            BufferedInputFile(bio.read(), filename="qr.png"),
            caption=f"📸 **Сканируйте QR-код через Telegram!**\nЖду {QR_TIMEOUT} сек. Отправьте /cancel для отмены."
        )
        await call.message.delete()
        
        # 4. Ждем авторизации и автоматически выводим результат
        try:
            await asyncio.wait_for(qr_login.wait(), timeout=QR_TIMEOUT)
            
            if await client.is_user_authorized():
                 me = await client.get_me()
                 # ✅ АВТОМАТИЧЕСКАЯ ПРОВЕРКА: Сообщаем об успехе и ID сессии
                 await sent.edit_caption(
                     caption=f"✅ **Успешный вход!** Сессия сохранена как `session_{me.id}.session`.", 
                     reply_markup=get_main_kb()
                 )
            else:
                 await sent.edit_caption(caption="❌ **Авторизация не удалась.** Попробуйте еще раз.", reply_markup=get_main_kb())
                 
        except asyncio.TimeoutError:
            await sent.edit_caption(caption="❌ **Время на сканирование вышло.**", reply_markup=get_main_kb())
        except Exception as e:
            logger.error(f"Ошибка входа по QR (wait): {e}")
            await sent.edit_caption(caption=f"❌ **Ошибка:** {type(e).__name__}", reply_markup=get_main_kb())

    except Exception as e:
        logger.error(f"Критическая ошибка QR: {e}")
        await call.message.answer(f"❌ **Критическая ошибка QR:** {type(e).__name__}", reply_markup=get_main_kb())
    finally:
        await clear_auth_client(user_id)
    await call.answer()

# --- ВХОД ПО НОМЕРУ ---

@auth_router.callback_query(F.data == "auth_phone")
async def auth_phone_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("📱 **Введите номер телефона** (включая код страны, например, `+79xxxxxxxxx`):")
    await state.set_state(AuthStates.PHONE)
    await call.answer()

@auth_router.message(AuthStates.PHONE)
async def auth_phone_input(message: Message, state: FSMContext):
    phone = message.text.strip().replace(' ', '')
    user_id = message.from_user.id
    
    if not phone.startswith('+') or len(phone) < 8:
         await message.answer("❌ Неверный формат номера. Используйте формат `+79xxxxxxxxx`.")
         return
    
    await clear_auth_client(user_id)
    # Сессия будет называться session_<user_id>
    client = TelegramClient(str(get_session_path(user_id)), API_ID, API_HASH)
    AUTH_CLIENTS[user_id] = client
        
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
        
        await state.update_data(phone=phone, hash=sent.phone_code_hash)
        await state.set_state(AuthStates.CODE)
        await message.answer("📩 **Код отправлен!** Введите код из Telegram:")
        
    except PhoneNumberInvalidError:
         await message.answer("❌ Неверный номер телефона. Проверьте правильность и код страны.")
         await clear_auth_client(user_id)
         await state.clear()
    except Exception as e:
        logger.error(f"Ошибка отправки кода: {e}")
        await message.answer(f"❌ **Ошибка:** {type(e).__name__}", reply_markup=get_main_kb())
        await clear_auth_client(user_id)
        await state.clear()


@auth_router.message(AuthStates.CODE)
async def auth_code_input(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    client = AUTH_CLIENTS.get(user_id)
    
    if not client:
        await message.answer("❌ Сессия авторизации потеряна. Начните заново.", reply_markup=get_main_kb())
        return await state.clear()
        
    try:
        # 2. Пытаемся войти
        await client.sign_in(phone=data['phone'], code=code, phone_code_hash=data['hash'])
        
        me = await client.get_me()
        # ✅ АВТОМАТИЧЕСКАЯ ПРОВЕРКА: Сообщаем об успехе и ID сессии
        await message.answer(
            f"✅ **Успешный вход!** Сессия сохранена как `session_{me.id}.session`.", 
            reply_markup=get_main_kb()
        )
        await clear_auth_client(user_id)
        await state.clear()
        
    except SessionPasswordNeededError:
        await message.answer("🔒 **Требуется 2FA!** Введите пароль:")
        await state.set_state(AuthStates.PASSWORD)
    except PhoneCodeInvalidError:
         await message.answer("❌ Неверный код. Попробуйте еще раз.")
    except Exception as e:
        logger.error(f"Ошибка входа по коду: {e}")
        await message.answer(f"❌ **Ошибка:** {type(e).__name__}", reply_markup=get_main_kb())
        await clear_auth_client(user_id)
        await state.clear()


@auth_router.message(AuthStates.PASSWORD)
async def auth_pass_input(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    client = AUTH_CLIENTS.get(user_id)
    
    try:
        # 3. Вводим пароль 2FA
        await client.sign_in(password=password)
        
        me = await client.get_me()
        # ✅ АВТОМАТИЧЕСКАЯ ПРОВЕРКА: Сообщаем об успехе и ID сессии
        await message.answer(
            f"✅ **Успешный вход (2FA)!** Сессия сохранена как `session_{me.id}.session`.", 
            reply_markup=get_main_kb()
        )
    except PasswordHashInvalidError:
         await message.answer("❌ Неверный пароль. Попробуйте еще раз.")
         return
    except Exception as e:
        logger.error(f"Ошибка 2FA: {e}")
        await message.answer(f"❌ **Ошибка:** {type(e).__name__}", reply_markup=get_main_kb())
    finally:
        await clear_auth_client(user_id)
        await state.clear()

# =========================================================================
# VII. ЗАПУСК БОТА
# =========================================================================

async def main():
    logger.info("🚀 SYSTEM STARTED: Auth Core")
    try:
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"Критический сбой в Aiogram: {e}")
    finally:
        logger.info("🛑 SYSTEM SHUTDOWN")
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 Bye!")
