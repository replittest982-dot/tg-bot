#!/usr/bin/env python3
"""
🚀 StatPro Ultimate v8.1 - FIX IMPORT ERROR
✅ ИСПРАВЛЕНО: Импорт ChatMemberStatus из aiogram.enums
✅ ИСПРАВЛЕНО: Логика Middleware (убран CancelHandler)
✅ Таймауты 500 сек.
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

# --- AIOGRAM IMPORTS ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message,
    BufferedInputFile
)
# 💥 ФИКС: Импортируем статус из enums
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.dispatcher.middlewares.base import BaseMiddleware

# --- TELETHON IMPORTS ---
from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError, PhoneNumberInvalidError, PhoneCodeInvalidError,
    PasswordHashInvalidError, FloodWaitError
)

# --- QR IMPORTS ---
import qrcode
from PIL import Image

# =========================================================================
# I. КОНФИГУРАЦИЯ
# =========================================================================

WORKER_STATUSES: Dict[int, str] = {}
COMMAND_CONFIGS: Dict[int, Dict[str, int]] = {}

try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    # Используем 0 как дефолт, чтобы не падало при str->int
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    
    # Таймаут 500 секунд
    AUTH_TIMEOUT = int(os.getenv("QR_TIMEOUT", "500"))
    
    SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "@suppor_tstatpro1bot")
    TARGET_CHANNEL_URL = os.getenv("TARGET_CHANNEL_URL", "https://t.me/STAT_PRO1")
    TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", "0"))
    
except ValueError as e:
    print(f"❌ ОШИБКА КОНФИГУРАЦИИ: {e}")
    sys.exit(1)

REQUIRED_ENVS = {"BOT_TOKEN": BOT_TOKEN, "API_ID": API_ID, "API_HASH": API_HASH}
if not all(REQUIRED_ENVS.values()):
    missing = [k for k, v in REQUIRED_ENVS.items() if not v]
    print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Нет переменных: {', '.join(missing)}")
    sys.exit(1)

SESSION_DIR = Path(__file__).parent / "sessions"
SESSION_DIR.mkdir(exist_ok=True)

# Конфиг для админа
COMMAND_CONFIGS[ADMIN_ID] = {"check_group_limit": 900000}

def get_session_path(user_id: int) -> Path:
    return SESSION_DIR / f"session_{user_id}"

# =========================================================================
# II. ЛОГГИРОВАНИЕ
# =========================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# =========================================================================
# III. БД ЗАГЛУШКИ
# =========================================================================

async def is_subscribed(user_id: int) -> bool:
    return user_id == ADMIN_ID

async def get_subscription_end_date(user_id: int) -> Optional[datetime]:
    if user_id == ADMIN_ID:
        return datetime.now() + timedelta(days=365)
    return None

async def create_promo_code(days: int, max_activations: int) -> str:
    code = f"STATPRO-{str(uuid.uuid4())[:6].upper()}"
    return code

async def activate_promo_code(user_id: int, code: str) -> bool:
    return code == "TEST"

# =========================================================================
# IV. MIDDLEWARE (ПРОВЕРКА ПОДПИСКИ)
# =========================================================================

class SubscriptionCheckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data: dict):
        user_id = event.from_user.id
        
        # 1. Пропускаем админа
        if user_id == ADMIN_ID:
            return await handler(event, data)
        
        # 2. Если ID канала не задан, пропускаем
        if TARGET_CHANNEL_ID == 0:
            return await handler(event, data)

        # 3. Проверка
        try:
            member = await bot.get_chat_member(chat_id=TARGET_CHANNEL_ID, user_id=user_id)
            # Используем Enum для статусов
            allowed_statuses = [
                ChatMemberStatus.CREATOR,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.MEMBER
            ]
            if member.status in allowed_statuses:
                return await handler(event, data)
        except Exception:
            pass 
            
        # 4. Блокировка
        text = (
            f"🚫 **Доступ закрыт!**\n\n"
            f"Подпишитесь на канал: {TARGET_CHANNEL_URL}\n"
            f"Затем нажмите /start"
        )
        
        if isinstance(event, Message):
            await event.answer(text)
        elif isinstance(event, CallbackQuery):
            await event.answer("🚫 Подпишитесь на канал!", show_alert=True)
            
        # Просто делаем return None, чтобы остановить выполнение (вместо CancelHandler)
        return

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

TEMP_AUTH_CLIENTS: Dict[int, TelegramClient] = {}

async def clear_temp_client(user_id: int):
    client = TEMP_AUTH_CLIENTS.pop(user_id, None)
    if client:
        try:
            await client.disconnect()
        except: pass

# --- START & MENU ---

@router.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    text = (
        f"👋 Здравствуйте! Я — <b>STATPRO Bot</b>.\n"
        f"🆔 Ваш ID: <code>{user_id}</code>\n"
        f"Пожалуйста, авторизуйтесь."
    )
    await message.answer(text, reply_markup=get_main_kb(user_id))

@router.callback_query(F.data == "main_menu")
async def cb_main_menu(call: CallbackQuery, state: FSMContext):
    await clear_temp_client(call.from_user.id)
    await state.clear()
    await call.message.edit_text("Главное меню:", reply_markup=get_main_kb(call.from_user.id))

# --- AUTH ---

@router.callback_query(F.data == "auth_menu")
async def cb_auth_menu(call: CallbackQuery):
    await call.message.edit_text("Метод входа:", reply_markup=get_auth_menu_kb())

@router.callback_query(F.data == "auth_qr")
async def auth_qr_start(call: CallbackQuery):
    user_id = call.from_user.id
    await clear_temp_client(user_id)
    
    client = TelegramClient(str(get_session_path(user_id)), API_ID, API_HASH)
    TEMP_AUTH_CLIENTS[user_id] = client
    
    try:
        await client.connect()
        qr_login = await client.qr_login()
        
        qr = qrcode.QRCode(box_size=4, border=4)
        qr.add_data(qr_login.url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
        bio = io.BytesIO()
        img.save(bio, format='PNG')
        bio.seek(0)
        
        sent = await call.message.answer_photo(
            BufferedInputFile(bio.read(), filename="qr.png"),
            caption=f"📸 <b>QR-код (Таймаут {AUTH_TIMEOUT}с)</b>\nСканируйте через: Настройки -> Устройства -> Подключить"
        )
        await call.message.delete()
        
        try:
            await asyncio.wait_for(qr_login.wait(), timeout=AUTH_TIMEOUT)
            
            if await client.is_user_authorized():
                me = await client.get_me()
                fn = f"session_{me.id}.session"
                await sent.edit_caption(
                    caption=f"✅ <b>Вход выполнен!</b>\n@{me.username}\nФайл: <code>{fn}</code>",
                    reply_markup=get_main_kb(user_id)
                )
            else:
                await sent.edit_caption(caption="❌ Не удалось войти.", reply_markup=get_main_kb(user_id))
        except asyncio.TimeoutError:
            await sent.edit_caption(caption="❌ Время вышло.", reply_markup=get_main_kb(user_id))
            
    except Exception as e:
        logger.error(f"QR Error: {e}")
        await call.message.answer(f"❌ Ошибка: {e}")
    finally:
        await clear_temp_client(user_id)

@router.callback_query(F.data == "auth_phone")
async def auth_phone(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("📱 Номер (+7...):")
    await state.set_state(AuthStates.PHONE)

@router.message(AuthStates.PHONE)
async def auth_phone_in(msg: Message, state: FSMContext):
    ph = msg.text.strip().replace(" ", "")
    uid = msg.from_user.id
    await clear_temp_client(uid)
    
    client = TelegramClient(str(get_session_path(uid)), API_ID, API_HASH)
    TEMP_AUTH_CLIENTS[uid] = client
    
    try:
        await client.connect()
        sent = await client.send_code_request(ph)
        await state.update_data(phone=ph, hash=sent.phone_code_hash)
        await state.set_state(AuthStates.CODE)
        await msg.answer(f"📩 Код (Таймаут {AUTH_TIMEOUT}с):")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}")

@router.message(AuthStates.CODE)
async def auth_code_in(msg: Message, state: FSMContext):
    code = msg.text.strip()
    data = await state.get_data()
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    
    if not client: return await msg.answer("❌ Сессия сброшена.")
    
    try:
        await client.sign_in(phone=data['phone'], code=code, phone_code_hash=data['hash'])
        me = await client.get_me()
        await msg.answer(f"✅ Вход: @{me.username}", reply_markup=get_main_kb(uid))
        await clear_temp_client(uid)
        await state.clear()
    except SessionPasswordNeededError:
        await msg.answer("🔒 Пароль 2FA:")
        await state.set_state(AuthStates.PASSWORD)
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}")

@router.message(AuthStates.PASSWORD)
async def auth_pass_in(msg: Message, state: FSMContext):
    pwd = msg.text.strip()
    uid = msg.from_user.id
    client = TEMP_AUTH_CLIENTS.get(uid)
    
    try:
        await client.sign_in(password=pwd)
        me = await client.get_me()
        await msg.answer(f"✅ Вход (2FA): @{me.username}", reply_markup=get_main_kb(uid))
    except Exception as e:
        await msg.answer(f"❌ {e}")
    finally:
        await clear_temp_client(uid)
        await state.clear()

# --- FUNCS & ADMIN ---

@router.callback_query(F.data == "main_functions")
async def cb_funcs(call: CallbackQuery):
    st = WORKER_STATUSES.get(ADMIN_ID, "⚪️ Ожидание")
    await call.message.edit_text(
        f"📊 <b>Функции</b>\nWorker статус: {st}\n\n"
        f"Команды в чатах:\n<code>.чекгруппу</code>\n<code>.лс текст @юзер</code>",
        reply_markup=get_main_kb(call.from_user.id)
    )

@router.callback_query(F.data == "admin_panel")
async def cb_adm(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("👑 Админ Панель", reply_markup=get_admin_panel_kb())

@router.callback_query(F.data == "create_promo")
async def cb_cp(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("Дней:")
    await state.set_state(AdminStates.PROMO_DAYS)

@router.message(AdminStates.PROMO_DAYS)
async def pd(msg: Message, state: FSMContext):
    await state.update_data(d=msg.text)
    await msg.answer("Активаций:")
    await state.set_state(AdminStates.PROMO_ACTIVATIONS)

@router.message(AdminStates.PROMO_ACTIVATIONS)
async def pa(msg: Message, state: FSMContext):
    data = await state.get_data()
    c = await create_promo_code(int(data['d']), int(msg.text))
    await msg.answer(f"✅ Код: <code>{c}</code>", reply_markup=get_main_kb(msg.from_user.id))
    await state.clear()

@router.callback_query(F.data == "config_menu")
async def cb_cm(call: CallbackQuery):
    await call.message.edit_text("Лимит парсинга:", reply_markup=get_check_group_limit_kb())

@router.callback_query(F.data.startswith("set_limit:"))
async def cb_sl(call: CallbackQuery):
    lim = int(call.data.split(":")[1])
    COMMAND_CONFIGS[ADMIN_ID]["check_group_limit"] = lim
    await call.answer(f"Лимит: {lim}", show_alert=True)
    await call.message.edit_text(f"✅ Установлен лимит: {lim}", reply_markup=get_admin_panel_kb())

# =========================================================================
# VII. TELETHON WORKER
# =========================================================================

async def start_worker_task():
    # Ищем сессию АДМИНА
    sess = get_session_path(ADMIN_ID)
    if not sess.exists():
        WORKER_STATUSES[ADMIN_ID] = "🔴 Нет сессии"
        return

    client = TelegramClient(str(sess), API_ID, API_HASH)
    
    @client.on(events.NewMessage(pattern=r'^\.чекгруппу$'))
    async def h_chk(ev):
        if not (ev.is_group or ev.is_channel): return await ev.reply("🚫 Только группы.")
        lim = COMMAND_CONFIGS[ADMIN_ID].get("check_group_limit", 1000)
        
        m = await ev.reply(f"🔍 Парсинг (Лимит: {lim})...")
        WORKER_STATUSES[ADMIN_ID] = f"🔄 Парсинг {ev.chat_id}..."
        
        lines = []
        try:
            async for u in client.iter_participants(ev.chat_id, limit=lim, aggressive=True):
                lines.append(f"@{u.username or 'None'} | {u.first_name} | {u.id}")
                if len(lines) % 200 == 0: await m.edit(f"🔍 Найдено: {len(lines)}...")
        except Exception as e:
            return await m.edit(f"❌ {e}")
            
        fn = f"users_{ev.chat_id}.txt"
        with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(lines))
        await client.send_file(ev.chat_id, fn, caption=f"✅ Готово: {len(lines)}")
        os.remove(fn)
        WORKER_STATUSES[ADMIN_ID] = "🟢 Готов"

    @client.on(events.NewMessage(pattern=r'^\.лс (.*?)(?: @(\S+))?$'))
    async def h_dm(ev):
        match = re.match(r'^\.лс (.*?)(?: @(\S+))?$', ev.text, re.DOTALL)
        if not match: return await ev.reply("❌ Формат: .лс текст @юзер")
        
        txt = match.group(1)
        usrs = [u.strip().lstrip('@') for u in match.group(2).split()] if match.group(2) else []
        if not usrs: return await ev.reply("❌ Нет юзеров.")
        
        await ev.reply(f"🚀 Рассылка {len(usrs)} людям...")
        ok = 0
        for u in usrs:
            try:
                await client.send_message(u, txt)
                ok += 1
                await asyncio.sleep(random.uniform(2, 5))
            except: pass
        await ev.reply(f"✅ Отправлено: {ok}/{len(usrs)}")

    await client.start()
    WORKER_STATUSES[ADMIN_ID] = "🟢 Активен"
    await client.run_until_disconnected()

# =========================================================================
# VIII. MAIN
# =========================================================================

async def main():
    logger.info("🚀 SYSTEM STARTED")
    dp.message.middleware(SubscriptionCheckMiddleware())
    dp.callback_query.middleware(SubscriptionCheckMiddleware())
    
    asyncio.create_task(start_worker_task())
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await bot.session.close()
        logger.info("🛑 SHUTDOWN")

if __name__ == "__main__":
    asyncio.run(main())
