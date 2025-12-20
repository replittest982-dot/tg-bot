#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🛡 StatPro v65.2 - PLATINUM EDITION
-----------------------------------
Build: 2024.06.25-Stable
Architect: StatPro AI
Features:
- Silent Reports (Reports go to Saved Messages)
- UTF-8 File Fix (Readable everywhere)
- Smart Lock (Profile accessible without sub)
- Hybrid Async Core
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
from pathlib import Path
from typing import Dict, Set, Optional
from dataclasses import dataclass

# --- AIOGRAM ---
from aiogram import Bot, Dispatcher, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Message, BufferedInputFile
)
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties

# --- TELETHON ---
from telethon import TelegramClient, events, types, functions
from telethon.errors import (
    SessionPasswordNeededError, 
    PhoneCodeExpiredError, 
    PhoneCodeInvalidError,
    FloodWaitError
)
from telethon.tl.types import User

# =========================================================================
# ⚙️ НАСТРОЙКИ СИСТЕМЫ
# =========================================================================

@dataclass
class Config:
    # --- ВАШИ ДАННЫЕ НИЖЕ ---
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "YOUR_API_HASH_HERE")
    SUB_CHANNEL: str = "@STAT_PRO1"  # Канал для проверки подписки
    
    # --- СИСТЕМНЫЕ ПУТИ ---
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_platinum.db"
    
    # --- МАСКИРОВКА (iOS 17) ---
    DEVICE_MODEL: str = "iPhone 15 Pro"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"
    LANG_CODE: str = "ru"
    SYSTEM_LANG_CODE: str = "ru-RU"
    
    # --- ТАЙМАУТЫ ---
    TELETHON_TIMEOUT: float = 25.0 

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)

cfg = Config()

# Улучшенное логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("StatPro")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ (ASYNCHRONOUS WAL MODE)
# =========================================================================

class Database:
    __slots__ = ('path',)
    _instance = None

    def __new__(cls):
        if cls._instance is None: cls._instance = super(Database, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.path = cfg.DB_PATH

    def get_conn(self):
        return aiosqlite.connect(self.path, timeout=30.0)

    async def init(self):
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
            
            # Таблица промокодов
            await db.execute("""
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY, 
                    days INTEGER, 
                    activations INTEGER
                )
            """)
            await db.commit()
        logger.info("💾 DB: Titanium Storage initialized.")

    async def upsert_user(self, uid: int, uname: str):
        now = int(time.time())
        async with self.get_conn() as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, ?, ?)", 
                (uid, uname, 0, now)
            )
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def check_sub_bool(self, uid: int) -> bool:
        """Возвращает True если подписка активна"""
        if uid == cfg.ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                # Проверка: дата окончания больше текущего времени
                return r[0] > int(time.time()) if (r and r[0]) else False

    async def add_sub_days(self, uid: int, days: int):
        now = int(time.time())
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                curr = r[0] if (r and r[0]) else 0
        
        # Если подписка активна - продлеваем, если нет - начинаем с сейчас
        start = curr if curr > now else now
        new_end = start + (days * 86400)
        
        async with self.get_conn() as db:
            await db.execute("UPDATE users SET sub_end = ? WHERE user_id = ?", (new_end, uid))
            await db.commit()
        return new_end

    async def use_promo(self, uid: int, code: str) -> int:
        code = code.strip()
        async with self.get_conn() as db:
            async with db.execute("SELECT days, activations FROM promos WHERE code = ? COLLATE NOCASE", (code,)) as c:
                r = await c.fetchone()
                if not r or r[1] < 1: return 0
                days = r[0]
            
            # Уменьшаем активации
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            # Чистим пустые промо
            await db.execute("DELETE FROM promos WHERE code = ? AND activations <= 0", (code,))
            await db.commit()
        
        await self.add_sub_days(uid, days)
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"VIP-{random.randint(1000,9999)}-{random.randint(10,99)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🦾 PLATINUM WORKER (Userbot Logic)
# =========================================================================

class Worker:
    def __init__(self, uid: int):
        self.uid = uid
        self.client = None
        self.spam_task: Optional[asyncio.Task] = None
        self.raid_targets: Set[int] = set()
        self.react_map: Dict[int, str] = {}
        self.ghost_mode: bool = False

    def _get_client(self, path):
        return TelegramClient(
            str(path), cfg.API_ID, cfg.API_HASH,
            device_model=cfg.DEVICE_MODEL,
            system_version=cfg.SYSTEM_VERSION,
            app_version=cfg.APP_VERSION,
            lang_code=cfg.LANG_CODE,
            system_lang_code=cfg.SYSTEM_LANG_CODE,
            timeout=cfg.TELETHON_TIMEOUT,
            auto_reconnect=True
        )

    async def start(self):
        s_path = cfg.SESSION_DIR / f"session_{self.uid}"
        self.client = self._get_client(s_path)
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                logger.warning(f"Worker {self.uid}: Unauthorized")
                return False
            self._bind_commands()
            asyncio.create_task(self.client.run_until_disconnected())
            logger.info(f"Worker {self.uid}: 🟢 ONLINE")
            return True
        except Exception as e:
            logger.exception(f"Worker {self.uid} Start Error")
            return False

    async def stop(self):
        if self.spam_task: self.spam_task.cancel()
        if self.client: await self.client.disconnect()

    def _bind_commands(self):
        client = self.client

        # --- GHOST MODE HANDLER ---
        @client.on(events.NewMessage(incoming=True))
        async def ghost_logic(e):
            if self.ghost_mode:
                # В режиме призрака мы НЕ помечаем сообщения как прочитанные
                pass

        # --- AUTO REACTIONS ---
        @client.on(events.NewMessage)
        async def reactor(e):
            if e.chat_id in self.react_map and not e.out:
                try: 
                    await e.client(functions.messages.SendReactionRequest(
                        peer=e.chat_id, msg_id=e.id, 
                        reaction=[types.ReactionEmoji(emoticon=self.react_map[e.chat_id])]
                    ))
                except: pass
            
            # --- RAID ---
            if e.sender_id in self.raid_targets:
                insults = ["🗑", "🤡", "🤫", "Weak", "Cry about it", "Bot"]
                try: await e.reply(random.choice(insults))
                except: pass

        # --- COMMANDS ---

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.ping$'))
        async def cmd_ping(e):
            start_t = time.perf_counter()
            m = await e.edit("⌛️ Calculating...")
            end_t = time.perf_counter()
            ping = (end_t - start_t) * 1000
            await m.edit(f"🚀 <b>Platinum Core</b>\nPing: <code>{ping:.2f}ms</code>", parse_mode='html')

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.spam\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def cmd_spam(e):
            if self.spam_task and not self.spam_task.done(): 
                return await e.edit("⚠️ Spam process already active!")
            
            args = e.pattern_match
            txt, cnt, dly = args.group(1), int(args.group(2)), float(args.group(3))
            await e.delete()
            
            async def run_spam():
                for _ in range(cnt):
                    try: 
                        await client.send_message(e.chat_id, txt)
                        await asyncio.sleep(dly)
                    except FloodWaitError as fw:
                        await asyncio.sleep(fw.seconds + 2)
                    except: break
            
            self.spam_task = asyncio.create_task(run_spam())

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def cmd_stop(e):
            if self.spam_task: 
                self.spam_task.cancel()
                self.spam_task = None
                await e.edit("🛑 All tasks stopped.")
            else:
                await e.edit("⚠️ No active tasks.")

        # --- ИСПРАВЛЕННЫЙ СКАНЕР (.scan) ---
        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.scan(?:\s+(\d+))?'))
        async def cmd_scan(e):
            limit = int(e.pattern_match.group(1) or 100)
            await e.edit(f"🕵️‍♂️ Scanning {limit} users... (Result -> Saved Messages)")
            
            data = []
            count = 0
            
            async for m in client.iter_messages(e.chat_id, limit=limit):
                if m.sender and isinstance(m.sender, User) and not m.sender.bot:
                    uid = m.sender.id
                    first = m.sender.first_name or ""
                    last = m.sender.last_name or ""
                    user = m.sender.username or ""
                    full_name = f"{first} {last}".strip()
                    if uid not in [x[0] for x in data]:
                        data.append([uid, user, full_name])
                        count += 1

            # Генерация CSV в памяти (UTF-8 с BOM для корректного открытия в Excel)
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["User ID", "Username", "Full Name"]) # Заголовки
            writer.writerows(data)
            
            # Конвертация в байты
            file_bytes = output.getvalue().encode('utf-8-sig') # utf-8-sig лечит иероглифы
            file_obj = io.BytesIO(file_bytes)
            file_obj.name = f"scan_report_{e.chat_id}.csv"

            # Отправка в ИЗБРАННОЕ (Saved Messages)
            try:
                await client.send_file("me", file_obj, caption=f"📊 <b>Scan Report</b>\nChat: {e.chat_id}\nUsers: {count}", parse_mode='html')
                await e.edit("✅ <b>Готово!</b> Файл отправлен в Избранное.")
            except Exception as ex:
                await e.edit(f"❌ Error sending file: {ex}")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.raid$'))
        async def cmd_raid(e):
            if not e.is_reply: return await e.edit("⚠️ Reply to a user!")
            r = await e.get_reply_message()
            tid = r.sender_id
            if tid in self.raid_targets:
                self.raid_targets.remove(tid)
                await e.edit("🕊 Raid disabled.")
            else:
                self.raid_targets.add(tid)
                await e.edit("☠️ <b>RAID MODE: ON</b>", parse_mode='html')

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.react\s+(.+)$'))
        async def cmd_react(e):
            em = e.pattern_match.group(1).strip()
            if em in ['off', 'stop']: 
                self.react_map.pop(e.chat_id, None)
                await e.edit("😐 Reactions disabled.")
            else: 
                self.react_map[e.chat_id] = em
                await e.edit(f"🔥 Auto-react: {em}")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.ghost\s+(on|off)$'))
        async def cmd_ghost(e):
            mode = e.pattern_match.group(1)
            self.ghost_mode = (mode == 'on')
            await e.edit(f"👻 Ghost Mode: <b>{self.ghost_mode}</b>", parse_mode='html')

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 🤖 BOT UI (Aiogram 3.x)
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router()
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminS(StatesGroup): U=State(); D=State(); PD=State(); PA=State()

# --- КЛАВИАТУРЫ ---

def kb_locked():
    """Клавиатура для тех, кто не подписан"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Мой Профиль (Ввести код)", callback_data="profile")],
        [InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="chk")]
    ])

def kb_main(uid):
    """Клавиатура для подписанных"""
    rows = [
        [InlineKeyboardButton(text="📚 Команды", callback_data="help")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🔑 Вход (Auth)", callback_data="auth_menu")]
    ]
    if uid == cfg.ADMIN_ID: rows.append([InlineKeyboardButton(text="👑 ADMIN", callback_data="adm_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# --- ЛОГИКА ПРОВЕРКИ ПОДПИСКИ ---

async def is_user_subscribed(user_id: int) -> bool:
    if user_id == cfg.ADMIN_ID: return True
    try:
        mem = await bot.get_chat_member(cfg.SUB_CHANNEL, user_id)
        if mem.status in ['left', 'kicked', 'banned']:
            return False
        return True
    except Exception as e:
        logger.error(f"Sub Check Error: {e}")
        return True # Если ошибка проверки - пускаем (fail-safe)

# --- HANDLERS ---

@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    username = m.from_user.username or "User"
    await db.upsert_user(uid, username)
    
    # Жесткая проверка подписки
    if not await is_user_subscribed(uid):
        msg_text = (
            f"⛔️ <b>Доступ ограничен!</b>\n\n"
            f"Для использования бота необходимо подписаться на канал: {cfg.SUB_CHANNEL}\n\n"
            f"<i>Вы можете открыть профиль, чтобы ввести промокод.</i>"
        )
        return await m.answer(msg_text, reply_markup=kb_locked())

    await m.answer(f"👋 <b>StatPro Platinum</b>\nДобро пожаловать, {m.from_user.first_name}!", reply_markup=kb_main(uid))

@router.callback_query(F.data == "chk")
async def check_sub_cb(c: CallbackQuery, state: FSMContext):
    await c.message.delete()
    await start(c.message, state)

@router.callback_query(F.data == "help")
async def help_menu(c: CallbackQuery):
    await c.message.edit_text(
        "💻 <b>Команды Userbot:</b>\n\n"
        "⚡️ <code>.ping</code> — Скорость отклика\n"
        "💣 <code>.spam [text] [count] [delay]</code> — Спам\n"
        "🛑 <code>.stop</code> — Остановить спам\n"
        "🕵️‍♂️ <code>.scan [limit]</code> — Сканер чата (Файл придет в ЛС)\n"
        "☠️ <code>.raid</code> (reply) — Ответы жертве\n"
        "👻 <code>.ghost on/off</code> — Режим призрака",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="chk")]])
    )

@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    uid = c.from_user.id
    # Профиль доступен всем, чтобы ввести промокод
    is_sub = await db.check_sub_bool(uid)
    status = "✅ PLATINUM" if is_sub else "❌ FREE (Неактивно)"
    
    # Кнопка назад зависит от подписки на канал
    back_cb = "chk"
    
    await c.message.edit_text(
        f"👤 <b>Личный кабинет</b>\n\n🆔: <code>{uid}</code>\n💎 Статус: <b>{status}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎟 Активировать Промокод", callback_data="promo")],
            [InlineKeyboardButton(text="🔙 Главная", callback_data=back_cb)]
        ])
    )

# --- PROMO ---
@router.callback_query(F.data == "promo")
async def promo_ask(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("🎟 <b>Введите код доступа:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="profile")]]))
    await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def promo_use(m: Message, state: FSMContext):
    days = await db.use_promo(m.from_user.id, m.text)
    if days:
        await m.answer(f"✅ <b>Успех!</b> Доступ продлен на {days} дн.")
        # Рестарт воркера если он есть
        if m.from_user.id in W_POOL: await W_POOL[m.from_user.id].start()
        await start(m, state)
    else:
        await m.answer("❌ Неверный код.")
        await start(m, state)

# --- AUTH (LOGIN) ---
@router.callback_query(F.data == "auth_menu")
async def auth_ui(c: CallbackQuery):
    if not await db.check_sub_bool(c.from_user.id):
        return await c.answer("❌ Нужна активная подписка!", True)
        
    await c.message.edit_text(
        "🔑 <b>Авторизация Userbot</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📸 QR-Код", callback_data="l_qr"), InlineKeyboardButton(text="📱 Номер", callback_data="l_ph")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="chk")]
        ])
    )

# --- AUTH: QR ---
@router.callback_query(F.data == "l_qr")
async def login_qr(c: CallbackQuery):
    uid = c.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    
    if await cl.is_user_authorized():
        await cl.disconnect()
        return await c.answer("✅ Уже в системе!", True)

    qr = await cl.qr_login()
    bio = io.BytesIO()
    qrcode.make(qr.url).save(bio, "PNG")
    bio.seek(0)
    
    m = await c.message.answer_photo(BufferedInputFile(bio.read(), "qr.png"), caption="📸 <b>Сканируйте QR</b>\nНастройки -> Устройства -> Подключить")
    
    try:
        await qr.wait(cfg.TELETHON_TIMEOUT)
        await m.delete()
        await c.message.answer("✅ <b>Вход выполнен!</b>")
        if uid not in W_POOL:
            w = Worker(uid); await w.start(); W_POOL[uid] = w
    except:
        await m.delete()
        await c.message.answer("❌ Время вышло.")
    finally:
        await cl.disconnect()

# --- AUTH: PHONE ---
@router.callback_query(F.data == "l_ph")
async def login_ph(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 <b>Введите номер (с кодом страны):</b>\nПример: 79991234567")
    await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def login_ph_send(m: Message, state: FSMContext):
    uid = m.from_user.id
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        sent = await cl.send_code_request(m.text)
        await state.update_data(phone=m.text, hash=sent.phone_code_hash, uid=uid)
        await cl.disconnect()
        await m.answer("📩 <b>Введите код из Telegram:</b>")
        await state.set_state(AuthS.CO)
    except Exception as e:
        await cl.disconnect()
        await m.answer(f"❌ Ошибка: {e}")

@router.message(AuthS.CO)
async def login_code(m: Message, state: FSMContext):
    data = await state.get_data()
    uid = data.get('uid')
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        await cl.sign_in(phone=data['phone'], code=m.text, phone_code_hash=data['hash'])
        await m.answer("✅ <b>Успешный вход!</b>")
        await cl.disconnect()
        await state.clear()
        if uid not in W_POOL:
            w = Worker(uid); await w.start(); W_POOL[uid] = w
        await start(m, state)
    except SessionPasswordNeededError:
        await m.answer("🔒 <b>Введите 2FA пароль:</b>")
        await cl.disconnect()
        await state.set_state(AuthS.PA)
    except Exception as e:
        await cl.disconnect()
        await m.answer(f"❌ Ошибка: {e}")

@router.message(AuthS.PA)
async def login_pwd(m: Message, state: FSMContext):
    data = await state.get_data()
    uid = data.get('uid')
    cl = Worker(uid)._get_client(cfg.SESSION_DIR / f"session_{uid}")
    await cl.connect()
    try:
        await cl.sign_in(password=m.text)
        await m.answer("✅ <b>Вход выполнен!</b>")
        await cl.disconnect()
        await state.clear()
        if uid not in W_POOL:
            w = Worker(uid); await w.start(); W_POOL[uid] = w
        await start(m, state)
    except Exception as e:
        await cl.disconnect()
        await m.answer(f"❌ Пароль неверный: {e}")

# --- ADMIN PANEL ---
@router.callback_query(F.data == "adm_menu")
async def adm_menu(c: CallbackQuery):
    if c.from_user.id != cfg.ADMIN_ID: return
    await c.message.edit_text("👑 <b>Admin Panel</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Gen Promo", callback_data="mk_p")],
        [InlineKeyboardButton(text="🎁 Add Sub (ID)", callback_data="g_s")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="chk")]
    ]))

@router.callback_query(F.data == "mk_p")
async def mk_p(c: CallbackQuery, state: FSMContext):
    await c.message.answer("📅 Days?"); await state.set_state(AdminS.PD)

@router.message(AdminS.PD)
async def mk_pd(m: Message, state: FSMContext):
    await state.update_data(d=int(m.text)); await m.answer("🔢 Activations?"); await state.set_state(AdminS.PA)

@router.message(AdminS.PA)
async def mk_pa(m: Message, state: FSMContext):
    d = await state.get_data()
    code = await db.create_promo(d['d'], int(m.text))
    await m.answer(f"✅ Code created: <code>{code}</code>")
    await state.clear()

@router.callback_query(F.data == "g_s")
async def gs(c: CallbackQuery, state: FSMContext):
    await c.message.answer("🆔 User ID?"); await state.set_state(AdminS.U)

@router.message(AdminS.U)
async def gs_u(m: Message, state: FSMContext):
    await state.update_data(u=m.text); await m.answer("📅 Days?"); await state.set_state(AdminS.D)

@router.message(AdminS.D)
async def gs_d(m: Message, state: FSMContext):
    d = await state.get_data()
    await db.upsert_user(int(d['u']), "AdminAdd")
    await db.add_sub_days(int(d['u']), int(m.text))
    await m.answer("✅ Sub added."); await state.clear()

# --- ENTRY POINT ---

async def main():
    await db.init()
    
    # Поднимаем активные сессии
    count = 0
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub_bool(uid):
                w = Worker(uid)
                if await w.start():
                    W_POOL[uid] = w
                    count += 1
        except Exception: pass
    
    logger.info(f"🚀 System started. Active Workers: {count}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
