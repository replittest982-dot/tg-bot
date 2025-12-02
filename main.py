#!/usr/bin/env python3
"""
🚀 StatPro Ultimate v8.0 - FINAL STABLE
✅ Таймауты увеличены до 500 сек.
✅ Исправлены импорты.
✅ Полная интеграция Aiogram + Telethon Worker.
"""

import asyncio
import logging
import os
import sys
import io
import re
import uuid
import random
from typing import Dict, Optional, Union
from pathlib import Path
from datetime import datetime, timedelta

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile, ChatMemberStatus
)
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.dispatcher.event.bases import CancelHandler

# --- TELETHON ---
from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError, PhoneNumberInvalidError, PhoneCodeInvalidError,
    PasswordHashInvalidError, FloodWaitError
)

# --- QR ---
import qrcode
from PIL import Image

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

# Глобальные статусы
WORKER_STATUSES: Dict[int, str] = {}
COMMAND_CONFIGS: Dict[int, Dict[str, int]] = {}

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    
    # 💥 ТАЙМАУТ 500 СЕКУНД (ПО УМОЛЧАНИЮ)
    AUTH_TIMEOUT = int(os.getenv("QR_TIMEOUT", "500"))
    
    SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "@suppor_tstatpro1bot")
    TARGET_CHANNEL_URL = os.getenv("TARGET_CHANNEL_URL", "https://t.me/STAT_PRO1")
    # Для проверки подписки нужен ID канала (начинается с -100)
    TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", "0")) 
    
except ValueError as e:
    print(f"❌ ОШИБКА КОНФИГУРАЦИИ: Неверный формат числа: {e}")
    sys.exit(1)

REQUIRED_ENVS = {"BOT_TOKEN": BOT_TOKEN, "API_ID": API_ID, "API_HASH": API_HASH}
if not all(REQUIRED_ENVS.values()):
    missing = [k for k, v in REQUIRED_ENVS.items() if not v]
    print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Нет переменных: {', '.join(missing)}")
    sys.exit(1)

SESSION_DIR = Path(__file__).parent / "sessions"
SESSION_DIR.mkdir(exist_ok=True)

# Инициализация конфига админа
COMMAND_CONFIGS[ADMIN_ID] = {"check_group_limit": 900000}

def get_session_path(user_id: int) -> Path:
    """Путь к сессии (session_USERID.session)."""
    return SESSION_DIR / f"session_{user_id}"

# =========================================================================
# II. ЛОГГИРОВАНИЕ
# =========================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# =========================================================================
# III. БД ЗАГЛУШКИ (MOCK DB)
# =========================================================================

async def is_subscribed(user_id: int) -> bool:
    """Проверка внутренней подписки бота (не канала)."""
    return user_id == ADMIN_ID # Админ всегда подписан

async def get_subscription_end_date(user_id: int) -> Optional[datetime]:
    if user_id == ADMIN_ID:
        return datetime.now() + timedelta(days=365)
    return None

async def create_promo_code(days: int, max_activations: int) -> str:
    code = f"STATPRO-{str(uuid.uuid4())[:6].upper()}"
    logger.info(f"Создан промокод: {code} ({days} дн)")
    return code

async def activate_promo_code(user_id: int, code: str) -> bool:
    return code == "TEST"

# =========================================================================
# IV. MIDDLEWARE (ПРОВЕРКА ПОДПИСКИ НА КАНАЛ)
# =========================================================================

class SubscriptionCheckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data: dict):
        user_id = event.from_user.id
        
        # 1. Админа пропускаем всегда
        if user_id == ADMIN_ID:
            return await handler(event, data)
        
        # 2. Если ID канала не настроен, пропускаем (чтобы не блокировать бота)
        if TARGET_CHANNEL_ID == 0:
            return await handler(event, data)

        # 3. Проверка подписки
        try:
            member = await bot.get_chat_member(chat_id=TARGET_CHANNEL_ID, user_id=user_id)
            if member.status in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER]:
                return await handler(event, data)
        except Exception:
            pass # Ошибка проверки (канал не найден или бот не админ)
            
        # 4. Сообщение о блокировке
        text = (
            f"🚫 **Доступ закрыт!**\n\n"
            f"Подпишитесь на канал, чтобы использовать бота:\n"
            f"{TARGET_CHANNEL_URL}\n\n"
            f"После подписки нажмите /start"
        )
        
        if isinstance(event, Message):
            await event.answer(text)
        elif isinstance(event, CallbackQuery):
            await event.answer("🚫 Подпишитесь на канал!", show_alert=True)
            
        return # Прерываем обработку

# =========================================================================
# V. КЛАВИАТУРЫ
# =========================================================================

def get_main_kb(user_id: int) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="🔑 Авторизоваться", callback_data="auth_menu")],
        [InlineKeyboardButton(text="📊 Функции", callback_data="main_functions")],
        [InlineKeyboardButton(text="⭐ Подписка", callback_data="subscription_menu")],
        [InlineKeyboardButton(text="❓ Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME.replace('@', '')}")],
    ]
    if user_id == ADMIN_ID:
        kb.insert(1, [InlineKeyboardButton(text="👑 Админ Панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_auth_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 По номеру", callback_data="auth_phone"), 
         InlineKeyboardButton(text="📸 По QR-коду", callback_data="auth_qr")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])

def get_admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промокод", callback_data="create_promo")],
        [InlineKeyboardButton(text="⚙️ Конфигурация команд", callback_data="config_menu")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")],
    ])

def get_check_group_limit_kb() -> InlineKeyboardMarkup:
    ranges = {"0-10": 10, "1-50": 50, "1-500": 500, "1-5k": 5000, "МАКС": 900000}
    kb = []
    for t, v in ranges.items():
        kb.append(InlineKeyboardButton(text=t, callback_data=f"set_limit:{v}"))
    rows = [kb[i:i + 2] for i in range(0, len(kb), 2)]
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="config_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# =========================================================================
# VI. AIOGRAM HANDLERS
# =========================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

class AuthStates(StatesGroup):
    PHONE = State()
    CODE = State()
    PASSWORD = State()

class AdminStates(StatesGroup):
    PROMO_DAYS = State()
    PROMO_ACTIVATIONS = State()

# Хранилище временных клиентов для авторизации
TEMP_AUTH_CLIENTS: Dict[int, TelegramClient] = {}

async def clear_temp_client(user_id: int):
    client = TEMP_AUTH_CLIENTS.pop(user_id, None)
    if client:
        await client.disconnect()

# --- START & MENU ---

@router.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    text = (
        f"👋 Здравствуйте! Я — <b>STATPRO Bot</b>.\n"
        f"🆔 Ваш ID: <code>{user_id}</code>\n\n"
        f"Для работы функций (парсинг, рассылка) требуется авторизация."
    )
    await message.answer(text, reply_markup=get_main_kb(user_id))

@router.callback_query(F.data == "main_menu")
async def cb_main_menu(call: CallbackQuery, state: FSMContext):
    await clear_temp_client(call.from_user.id)
    await state.clear()
    await call.message.edit_text("Главное меню:", reply_markup=get_main_kb(call.from_user.id))

# --- АВТОРИЗАЦИЯ (QR) ---

@router.callback_query(F.data == "auth_menu")
async def cb_auth_menu(call: CallbackQuery):
    await call.message.edit_text("Выберите метод входа:", reply_markup=get_auth_menu_kb())

@router.callback_query(F.data == "auth_qr")
async def auth_qr_start(call: CallbackQuery):
    user_id = call.from_user.id
    await clear_temp_client(user_id)
    
    # Создаем клиент
    client = TelegramClient(str(get_session_path(user_id)), API_ID, API_HASH)
    TEMP_AUTH_CLIENTS[user_id] = client
    
    try:
        await client.connect()
        qr_login = await client.qr_login()
        
        # Генерация картинки
        qr = qrcode.QRCode(box_size=4, border=4)
        qr.add_data(qr_login.url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
        bio = io.BytesIO()
        img.save(bio, format='PNG')
        bio.seek(0)
        
        sent = await call.message.answer_photo(
            BufferedInputFile(bio.read(), filename="qr.png"),
            caption=f"📸 <b>Сканируйте QR!</b>\n⏳ Таймаут: {AUTH_TIMEOUT} сек.\nЕсли долго грузит - это нормально."
        )
        await call.message.delete()
        
        # Ожидание (500 сек)
        try:
            await asyncio.wait_for(qr_login.wait(), timeout=AUTH_TIMEOUT)
            
            if await client.is_user_authorized():
                me = await client.get_me()
                fname = f"session_{me.id}.session"
                await sent.edit_caption(
                    caption=f"✅ <b>Успешный вход!</b>\n👤 @{me.username or me.id}\n📁 Сессия: <code>{fname}</code>",
                    reply_markup=get_main_kb(user_id)
                )
            else:
                await sent.edit_caption(caption="❌ Не удалось авторизоваться.", reply_markup=get_main_kb(user_id))
        except asyncio.TimeoutError:
            await sent.edit_caption(caption="❌ Время вышло.", reply_markup=get_main_kb(user_id))
            
    except Exception as e:
        logger.error(f"QR Error: {e}")
        await call.message.answer(f"❌ Ошибка QR: {e}")
    finally:
        await clear_temp_client(user_id)

# --- АВТОРИЗАЦИЯ (ТЕЛЕФОН) ---

@router.callback_query(F.data == "auth_phone")
async def auth_phone_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("📱 Введите номер (+7...):")
    await state.set_state(AuthStates.PHONE)

@router.message(AuthStates.PHONE)
async def auth_phone_input(message: Message, state: FSMContext):
    phone = message.text.strip().replace(" ", "")
    user_id = message.from_user.id
    
    await clear_temp_client(user_id)
    client = TelegramClient(str(get_session_path(user_id)), API_ID, API_HASH)
    TEMP_AUTH_CLIENTS[user_id] = client
    
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
        await state.update_data(phone=phone, hash=sent.phone_code_hash)
        await state.set_state(AuthStates.CODE)
        await message.answer(f"📩 Введите код (Таймаут {AUTH_TIMEOUT}с):")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await clear_temp_client(user_id)

@router.message(AuthStates.CODE)
async def auth_code_input(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    client = TEMP_AUTH_CLIENTS.get(user_id)
    
    if not client:
        return await message.answer("❌ Сессия сброшена. Начните заново.")
        
    try:
        await client.sign_in(phone=data['phone'], code=code, phone_code_hash=data['hash'])
        me = await client.get_me()
        await message.answer(f"✅ <b>Успех!</b> Вошли как: @{me.username or me.id}", reply_markup=get_main_kb(user_id))
        await clear_temp_client(user_id)
        await state.clear()
    except SessionPasswordNeededError:
        await message.answer("🔒 Введите 2FA пароль:")
        await state.set_state(AuthStates.PASSWORD)
    except Exception as e:
        await message.answer(f"❌ Ошибка кода: {e}")

@router.message(AuthStates.PASSWORD)
async def auth_pass_input(message: Message, state: FSMContext):
    pwd = message.text.strip()
    user_id = message.from_user.id
    client = TEMP_AUTH_CLIENTS.get(user_id)
    
    try:
        await client.sign_in(password=pwd)
        me = await client.get_me()
        await message.answer(f"✅ <b>Успех (2FA)!</b> @{me.username}", reply_markup=get_main_kb(user_id))
    except Exception as e:
        await message.answer(f"❌ Ошибка пароля: {e}")
    finally:
        await clear_temp_client(user_id)
        await state.clear()

# --- ФУНКЦИИ И АДМИНКА ---

@router.callback_query(F.data == "main_functions")
async def cb_funcs(call: CallbackQuery):
    status = WORKER_STATUSES.get(ADMIN_ID, "Неизвестно")
    await call.message.edit_text(
        f"📊 <b>Функции</b>\n\nСтатус Worker: {status}\n\nКоманды чата:\n"
        f"<code>.чекгруппу</code> - Парсинг\n<code>.лс текст @юзер</code> - Рассылка",
        reply_markup=get_main_kb(call.from_user.id)
    )

@router.callback_query(F.data == "admin_panel")
async def cb_admin(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("👑 Админ Панель", reply_markup=get_admin_panel_kb())

@router.callback_query(F.data == "create_promo")
async def cb_promo(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("Введите кол-во дней:")
    await state.set_state(AdminStates.PROMO_DAYS)

@router.message(AdminStates.PROMO_DAYS)
async def promo_days(message: Message, state: FSMContext):
    await state.update_data(d=message.text)
    await message.answer("Введите кол-во активаций:")
    await state.set_state(AdminStates.PROMO_ACTIVATIONS)

@router.message(AdminStates.PROMO_ACTIVATIONS)
async def promo_final(message: Message, state: FSMContext):
    data = await state.get_data()
    code = await create_promo_code(int(data['d']), int(message.text))
    await message.answer(f"✅ Код: <code>{code}</code>", reply_markup=get_main_kb(message.from_user.id))
    await state.clear()

@router.callback_query(F.data == "config_menu")
async def cb_conf(call: CallbackQuery):
    await call.message.edit_text("Настройка лимита .чекгруппу:", reply_markup=get_check_group_limit_kb())

@router.callback_query(F.data.startswith("set_limit:"))
async def cb_set_limit(call: CallbackQuery):
    lim = int(call.data.split(":")[1])
    COMMAND_CONFIGS[ADMIN_ID]["check_group_limit"] = lim
    await call.answer(f"Лимит установлен: {lim}", show_alert=True)
    await call.message.edit_text(f"✅ Лимит: {lim}", reply_markup=get_admin_panel_kb())

# =========================================================================
# VII. TELETHON WORKER
# =========================================================================

async def start_worker_task():
    """Фоновый процесс Worker, использующий сессию Админа."""
    # 💥 ВАЖНО: Worker всегда ищет сессию ADMIN_ID
    sess_path = get_session_path(ADMIN_ID)
    
    if not sess_path.exists():
        WORKER_STATUSES[ADMIN_ID] = "🔴 Сессия не найдена. Авторизуйтесь!"
        logger.warning("Worker: Сессия админа не найдена.")
        return

    client = TelegramClient(str(sess_path), API_ID, API_HASH)
    
    @client.on(events.NewMessage(pattern=r'^\.чекгруппу$'))
    async def handler_check(event):
        if not event.is_group and not event.is_channel:
            return await event.reply("🚫 Только для групп.")
            
        limit = COMMAND_CONFIGS[ADMIN_ID].get("check_group_limit", 1000)
        msg = await event.reply(f"🔍 Парсинг... Лимит: {limit}")
        WORKER_STATUSES[ADMIN_ID] = f"🔄 Парсинг {event.chat_id}..."
        
        lines = []
        count = 0
        try:
            async for u in client.iter_participants(event.chat_id, limit=limit, aggressive=True):
                lines.append(f"@{u.username or 'None'} | {u.first_name} | {u.id}")
                count += 1
                if count % 200 == 0: await msg.edit(f"🔍 Найдено: {count}...")
        except Exception as e:
            return await msg.edit(f"❌ Ошибка: {e}")
            
        fname = f"users_{event.chat_id}.txt"
        with open(fname, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
            
        await client.send_file(event.chat_id, fname, caption=f"✅ Собрано: {count}")
        os.remove(fname)
        WORKER_STATUSES[ADMIN_ID] = "✅ Готов к работе"

    @client.on(events.NewMessage(pattern=r'^\.лс (.*?)(?: @(\S+))?$'))
    async def handler_dm(event):
        match = re.match(r'^\.лс (.*?)(?: @(\S+))?$', event.text, re.DOTALL)
        if not match: return await event.reply("❌ .лс текст @юзер1 @юзер2")
        
        txt = match.group(1)
        users = [u.strip().lstrip('@') for u in match.group(2).split()] if match.group(2) else []
        
        if not users: return await event.reply("❌ Нет юзеров.")
        
        await event.reply(f"🚀 Рассылка {len(users)} юзерам...")
        ok = 0
        for u in users:
            try:
                await client.send_message(u, txt)
                ok += 1
                await asyncio.sleep(random.uniform(2, 5)) # Задержка 2-5 сек
            except: pass
            
        await event.reply(f"✅ Отправлено: {ok}/{len(users)}")

    await client.start()
    WORKER_STATUSES[ADMIN_ID] = "🟢 Активен"
    logger.info("Worker Started")
    await client.run_until_disconnected()

# =========================================================================
# VIII. MAIN
# =========================================================================

async def main():
    logger.info("🚀 SYSTEM STARTED")
    
    # Middleware
    dp.message.middleware(SubscriptionCheckMiddleware())
    dp.callback_query.middleware(SubscriptionCheckMiddleware())
    
    # Запуск Worker (фоном)
    asyncio.create_task(start_worker_task())
    
    # Запуск бота
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await bot.session.close()
        logger.info("🛑 SHUTDOWN")

if __name__ == "__main__":
    asyncio.run(main())
