#!/usr/bin/env python3
"""
🛡 StatPro v65.1 - TITANIUM SECURE EDITION
------------------------------------------
✅ UI: Понятные тексты кнопок и сообщений.
✅ LOGIN: Улучшенная маскировка под iOS (iPhone 15).
✅ CORE: Защита от потери сессии и "Expired Code".
✅ FEATURES: Полный набор (.ping, .spam, .raid, .scan).
"""

import asyncio
import logging
import os
import io
import json
import random
import time
import qrcode
import aiosqlite
import csv
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
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
    # ⚠️ ВСТАВЬТЕ ВАШИ ДАННЫЕ СЮДА
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "YOUR_API_HASH_HERE")
    SUB_CHANNEL: str = "@STAT_PRO1"
    
    BASE_DIR: Path = Path(__file__).resolve().parent
    SESSION_DIR: Path = BASE_DIR / "sessions"
    DB_PATH: Path = BASE_DIR / "statpro_titanium.db"
    
    # 🕵️‍♂️ МАСКИРОВКА (ОБНОВЛЕННАЯ)
    # Используем параметры реального iOS клиента для обхода блокировок
    DEVICE_MODEL: str = "iPhone 15 Pro"
    SYSTEM_VERSION: str = "17.5.1"
    APP_VERSION: str = "10.8.1"  # Стабильная версия Telegram iOS
    LANG_CODE: str = "ru"
    SYSTEM_LANG_CODE: str = "ru-RU"
    
    # Увеличенный таймаут для стабильности входа
    TELETHON_TIMEOUT: float = 20.0 

    def __post_init__(self):
        self.SESSION_DIR.mkdir(parents=True, exist_ok=True)

cfg = Config()

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("StatPro")

# =========================================================================
# 🗄️ БАЗА ДАННЫХ
# =========================================================================

class Database:
    __slots__ = ('path',)
    _instance = None
    def __new__(cls):
        if cls._instance is None: cls._instance = super(Database, cls).__new__(cls)
        return cls._instance
    def __init__(self): self.path = cfg.DB_PATH
    def get_conn(self): return aiosqlite.connect(self.path, timeout=30.0)

    async def init(self):
        async with self.get_conn() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, username TEXT, 
                    sub_end INTEGER, joined_at INTEGER
                )
            """)
            await db.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, days INTEGER, activations INTEGER)")
            await db.commit()
        logger.info("💾 База данных загружена.")

    async def upsert_user(self, uid: int, uname: str):
        now = int(time.time())
        async with self.get_conn() as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, sub_end, joined_at) VALUES (?, ?, ?, ?)", (uid, uname, 0, now))
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (uname, uid))
            await db.commit()

    async def check_sub_bool(self, uid: int) -> bool:
        if uid == cfg.ADMIN_ID: return True
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                return r[0] > int(time.time()) if (r and r[0]) else False

    async def add_sub_days(self, uid: int, days: int):
        now = int(time.time())
        async with self.get_conn() as db:
            async with db.execute("SELECT sub_end FROM users WHERE user_id = ?", (uid,)) as c:
                r = await c.fetchone()
                curr = r[0] if (r and r[0]) else 0
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
            await db.execute("UPDATE promos SET activations = activations - 1 WHERE code = ? COLLATE NOCASE", (code,))
            await db.execute("DELETE FROM promos WHERE code = ? AND activations <= 0", (code,))
            await db.commit()
        await self.add_sub_days(uid, days)
        return days

    async def create_promo(self, days: int, acts: int) -> str:
        code = f"PRO-{random.randint(10000,99999)}"
        async with self.get_conn() as db:
            await db.execute("INSERT INTO promos VALUES (?, ?, ?)", (code, days, acts))
            await db.commit()
        return code

db = Database()

# =========================================================================
# 🦾 ЮЗЕРБОТ (ВЕСЬ ФУНКЦИОНАЛ)
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
        # Используем те же параметры, что и при входе, чтобы не сбросить сессию
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
                logger.warning(f"Воркер {self.uid}: Требуется авторизация")
                return False
            self._bind_commands()
            asyncio.create_task(self.client.run_until_disconnected())
            logger.info(f"Воркер {self.uid}: 🟢 ЗАПУЩЕН")
            return True
        except Exception as e:
            logger.exception(f"Ошибка запуска воркера {self.uid}")
            return False

    async def stop(self):
        if self.spam_task: self.spam_task.cancel()
        if self.client: await self.client.disconnect()

    def _bind_commands(self):
        client = self.client

        @client.on(events.NewMessage)
        async def main_listener(e):
            # 1. АВТО-РЕАКЦИИ
            if e.chat_id in self.react_map and not e.out:
                try: await e.client(functions.messages.SendReactionRequest(
                    peer=e.chat_id, msg_id=e.id, 
                    reaction=[types.ReactionEmoji(emoticon=self.react_map[e.chat_id])]
                ))
                except: pass

            # 2. РЕЙД (Ответы жертве)
            if e.sender_id in self.raid_targets:
                insults = ["🗑", "🤡", "🤫", "👎", "Слабый", "Не пиши сюда", "Cry more"]
                try: await e.reply(random.choice(insults))
                except: pass

        # --- КОМАНДЫ (С ТОЧКОЙ) ---

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.ping$'))
        async def cmd_ping(e):
            s = time.perf_counter()
            m = await e.edit("🏓 Pong...")
            ms = (time.perf_counter() - s) * 1000
            await m.edit(f"🏓 <b>Pong!</b>\n📶 Скорость: <code>{ms:.2f}ms</code>", parse_mode='html')

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.spam\s+(.+)\s+(\d+)\s+([\d\.]+)'))
        async def cmd_spam(e):
            if self.spam_task and not self.spam_task.done(): return await e.edit("⚠️ Спам уже запущен!")
            args = e.pattern_match
            txt, cnt, dly = args.group(1), int(args.group(2)), float(args.group(3))
            await e.delete()
            async def run():
                for _ in range(cnt):
                    try: await client.send_message(e.chat_id, txt)
                    except: break
                    await asyncio.sleep(dly)
            self.spam_task = asyncio.create_task(run())

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.stop$'))
        async def cmd_stop(e):
            if self.spam_task: 
                self.spam_task.cancel()
                self.spam_task=None
                await e.edit("🛑 Спам остановлен.")
            else: await e.edit("⚠️ Нет активных задач.")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.scan(?:\s+(\d+))?'))
        async def cmd_scan(e):
            limit = int(e.pattern_match.group(1) or 100)
            await e.edit(f"🔎 Сканирую {limit} последних сообщений...")
            users = {}
            async for m in client.iter_messages(e.chat_id, limit=limit):
                if m.sender and isinstance(m.sender, User) and not m.sender.bot:
                    name = f"{m.sender.first_name or ''} {m.sender.last_name or ''}".strip()
                    users[m.sender.id] = name
            
            f = io.StringIO(); w = csv.writer(f); w.writerow(["ID", "Name"])
            for u, n in users.items(): w.writerow([u, n])
            f.seek(0)
            await e.delete()
            await client.send_file(e.chat_id, f.read().encode(), filename="users.csv", caption=f"✅ Найдено пользователей: {len(users)}")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.raid$'))
        async def cmd_raid(e):
            if not e.is_reply: return await e.edit("⚠️ Ответьте этой командой на сообщение жертвы!")
            r = await e.get_reply_message()
            tid = r.sender_id
            if tid in self.raid_targets:
                self.raid_targets.remove(tid); await e.edit("🕊 Рейд выключен.")
            else:
                self.raid_targets.add(tid); await e.edit("☠️ <b>РЕЙД АКТИВИРОВАН</b>", parse_mode='html')

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.react\s+(.+)$'))
        async def cmd_react(e):
            em = e.pattern_match.group(1).strip()
            if em in ['off', 'stop']: self.react_map.pop(e.chat_id, None); await e.edit("😐 Авто-реакции выключены.")
            else: self.react_map[e.chat_id] = em; await e.edit(f"🔥 Авто-реакция установлена: {em}")

        @client.on(events.NewMessage(outgoing=True, pattern=r'^\.ghost\s+(on|off)$'))
        async def cmd_ghost(e):
            self.ghost_mode = (e.pattern_match.group(1) == 'on')
            await e.edit(f"👻 Режим призрака: <b>{self.ghost_mode}</b>", parse_mode='html')

W_POOL: Dict[int, Worker] = {}

# =========================================================================
# 📱 БОТ-ИНТЕРФЕЙС (AIOGRAM)
# =========================================================================

bot = Bot(token=cfg.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router()
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)

class AuthS(StatesGroup): PH=State(); CO=State(); PA=State()
class PromoS(StatesGroup): CODE=State()
class AdminS(StatesGroup): U=State(); D=State(); PD=State(); PA=State()

# --- ХЕЛПЕРЫ ДЛЯ АВТОРИЗАЦИИ ---
def get_client_for_auth(uid):
    return TelegramClient(
        str(cfg.SESSION_DIR / f"session_{uid}"), cfg.API_ID, cfg.API_HASH,
        device_model=cfg.DEVICE_MODEL, 
        system_version=cfg.SYSTEM_VERSION, 
        app_version=cfg.APP_VERSION,
        lang_code=cfg.LANG_CODE,
        system_lang_code=cfg.SYSTEM_LANG_CODE,
        timeout=cfg.TELETHON_TIMEOUT,
        auto_reconnect=True
    )

def kb_main(uid):
    rows = [
        [InlineKeyboardButton(text="📚 Список Команд", callback_data="help")],
        [InlineKeyboardButton(text="👤 Мой Профиль", callback_data="profile"), InlineKeyboardButton(text="🔑 Войти в аккаунт", callback_data="auth_menu")]
    ]
    if uid == cfg.ADMIN_ID: rows.append([InlineKeyboardButton(text="👑 Админ Панель", callback_data="adm_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# --- START ---
@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    await db.upsert_user(uid, m.from_user.username or "User")
    
    # Проверка подписки на канал
    try:
        mem = await bot.get_chat_member(cfg.SUB_CHANNEL, uid)
        if mem.status in ['left', 'kicked'] and uid != cfg.ADMIN_ID:
            return await m.answer(
                f"⛔️ <b>Доступ ограничен!</b>\nДля работы с ботом подпишитесь на наш канал.", 
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Подписаться на канал", url=f"https://t.me/{cfg.SUB_CHANNEL.replace('@','')}")],
                    [InlineKeyboardButton(text="✅ Я подписался", callback_data="chk")]
                ])
            )
    except: pass

    await m.answer(f"👋 <b>Приветствую, {m.from_user.first_name}!</b>\n\n🛡 <b>StatPro v65.1</b> готов к работе.\nВаш ID: <code>{uid}</code>", reply_markup=kb_main(uid))

@router.callback_query(F.data == "chk")
async def chk(c: CallbackQuery, state: FSMContext): 
    await c.message.delete(); await start(c.message, state)

@router.callback_query(F.data == "help")
async def help_c(c: CallbackQuery):
    await c.message.edit_text(
        "💻 <b>Доступные команды (отправлять в чат):</b>\n\n"
        "⚡️ <code>.ping</code> — Проверить скорость отклика\n"
        "💣 <code>.spam [текст] [кол] [сек]</code> — Запустить рассылку\n"
        "🛑 <code>.stop</code> — Остановить рассылку\n"
        "🔎 <code>.scan 100</code> — Собрать список участников чата\n"
        "☠️ <code>.raid</code> (ответом на смс) — Авто-ответы жертве\n"
        "🔥 <code>.react 👍</code> — Ставить реакции автоматически\n"
        "👻 <code>.ghost on/off</code> — Режим невидимки",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="chk")]])
    )

@router.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    uid = c.from_user.id
    active = await db.check_sub_bool(uid)
    sub = "✅ Активна" if active else "❌ Истекла"
    await c.message.edit_text(
        f"👤 <b>Личный кабинет</b>\n\n🆔 ID: <code>{uid}</code>\n💎 Подписка: {sub}", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎟 Активировать промокод", callback_data="promo")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="chk")]
        ])
    )

# --- ПРОМОКОДЫ ---
@router.callback_query(F.data == "promo")
async def pr(c: CallbackQuery, state: FSMContext): 
    await c.message.edit_text("🎟 <b>Введите ваш промокод:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Отмена", callback_data="profile")]]))
    await state.set_state(PromoS.CODE)

@router.message(PromoS.CODE)
async def pr_u(m: Message, state: FSMContext):
    d = await db.use_promo(m.from_user.id, m.text)
    if d: 
        await m.answer(f"✅ <b>Успешно!</b> Подписка продлена на {d} дней.")
        # Авто-рестарт воркера если он был онлайн
        if m.from_user.id in W_POOL: await W_POOL[m.from_user.id].start()
        await start(m, state)
    else: 
        await m.answer("❌ Промокод не найден или уже использован.")
        await start(m, state)

# --- АВТОРИЗАЦИЯ (ИСПРАВЛЕННАЯ ЛОГИКА) ---

@router.callback_query(F.data == "auth_menu")
async def auth_m(c: CallbackQuery):
    await c.message.edit_text(
        "🔑 <b>Вход в аккаунт Telegram</b>\nВыберите удобный способ:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📸 Через QR-код", callback_data="l_qr"), InlineKeyboardButton(text="📱 По номеру", callback_data="l_ph")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="chk")]
        ])
    )

@router.callback_query(F.data == "l_qr")
async def l_qr(c: CallbackQuery):
    uid = c.from_user.id
    cl = get_client_for_auth(uid)
    await cl.connect()
    
    if await cl.is_user_authorized(): 
        await cl.disconnect()
        return await c.answer("✅ Вы уже авторизованы!", True)
    
    qr = await cl.qr_login()
    bio = io.BytesIO()
    qrcode.make(qr.url).save(bio, "PNG")
    
    m = await c.message.answer_photo(BufferedInputFile(bio.getvalue(), "qr.png"), caption="📸 <b>Сканируйте QR-код</b>\nТайм-аут 20 секунд...")
    
    try: 
        await qr.wait(cfg.TELETHON_TIMEOUT)
        await m.delete()
        await c.message.answer("✅ <b>Вход выполнен успешно!</b>")
        if uid not in W_POOL:
             w = Worker(uid); await w.start(); W_POOL[uid] = w
    except Exception: 
        await m.delete()
        await c.message.answer("❌ Время истекло или ошибка входа.")
    finally: 
        await cl.disconnect()

@router.callback_query(F.data == "l_ph")
async def l_ph(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("📱 <b>Введите номер телефона:</b>\n(в формате 79991234567)")
    await state.set_state(AuthS.PH)

@router.message(AuthS.PH)
async def l_ph_s(m: Message, state: FSMContext):
    uid = m.from_user.id
    client = get_client_for_auth(uid)
    await client.connect()
    try:
        sent = await client.send_code_request(m.text)
        # ВАЖНО: Сохраняем HASH и отключаемся
        await state.update_data(phone=m.text, hash=sent.phone_code_hash, uid=uid)
        await client.disconnect() 
        await m.answer("📩 <b>Введите код из Telegram:</b>")
        await state.set_state(AuthS.CO)
    except FloodWaitError as e:
        await client.disconnect()
        await m.answer(f"⏳ Telegram требует подождать {e.seconds} секунд.")
    except Exception as e:
        logger.error(f"Ошибка входа (PH): {e}")
        await client.disconnect()
        await m.answer(f"❌ Ошибка: {e}")

@router.message(AuthS.CO)
async def l_co_s(m: Message, state: FSMContext):
    d = await state.get_data()
    if 'hash' not in d: return await m.answer("❌ Сессия сброшена. Начните заново.")
    
    client = get_client_for_auth(d['uid'])
    await client.connect()
    
    try:
        await client.sign_in(phone=d['phone'], code=m.text, phone_code_hash=d['hash'])
        await m.answer("✅ <b>Вход выполнен успешно!</b>")
        await client.disconnect()
        await state.clear()
        
        # Запуск воркера
        if d['uid'] not in W_POOL:
             w = Worker(d['uid']); await w.start(); W_POOL[d['uid']] = w
        await start(m, state)
             
    except SessionPasswordNeededError:
        await m.answer("🔒 <b>Введите пароль 2FA (Облачный пароль):</b>")
        await client.disconnect()
        await state.set_state(AuthS.PA)
    except PhoneCodeExpiredError:
        await m.answer("❌ Код истек. Попробуйте снова.")
        await client.disconnect(); await state.clear()
    except PhoneCodeInvalidError:
        await m.answer("❌ Неверный код.")
        await client.disconnect()
    except Exception as e:
        await client.disconnect(); await m.answer(f"❌ Ошибка: {e}"); await state.clear()

@router.message(AuthS.PA)
async def l_pa_s(m: Message, state: FSMContext):
    d = await state.get_data()
    client = get_client_for_auth(d['uid'])
    await client.connect()
    try: 
        await client.sign_in(password=m.text)
        await m.answer("✅ <b>Вход с паролем выполнен!</b>")
        await client.disconnect()
        await state.clear()
        
        if d['uid'] not in W_POOL:
             w = Worker(d['uid']); await w.start(); W_POOL[d['uid']] = w
        await start(m, state)
        
    except Exception as e: 
        await m.answer(f"❌ Ошибка пароля: {e}")
        await client.disconnect()

# --- АДМИН ПАНЕЛЬ ---

@router.callback_query(F.data == "adm_menu")
async def adm(c: CallbackQuery):
    if c.from_user.id != cfg.ADMIN_ID: return
    await c.message.edit_text("👑 <b>Панель Администратора</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промокод", callback_data="mk_p")],
        [InlineKeyboardButton(text="🎁 Выдать подписку (ID)", callback_data="g_s")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="chk")]
    ]))

@router.callback_query(F.data == "mk_p")
async def mk(c: CallbackQuery, state: FSMContext): 
    await c.message.answer("📅 На сколько дней?"); await state.set_state(AdminS.PD)

@router.message(AdminS.PD)
async def mk_d(m: Message, state: FSMContext): 
    try: await state.update_data(d=int(m.text)); await m.answer("🔢 Сколько активаций?"); await state.set_state(AdminS.PA)
    except: await m.answer("Введите число.")

@router.message(AdminS.PA)
async def mk_a(m: Message, state: FSMContext): 
    try:
        d=await state.get_data()
        c=await db.create_promo(d['d'],int(m.text))
        await m.answer(f"✅ Промокод создан:\n<code>{c}</code>")
    except: await m.answer("Ошибка.")
    await state.clear()

@router.callback_query(F.data == "g_s")
async def gs(c: CallbackQuery, state: FSMContext): 
    await c.message.answer("👤 Введите ID пользователя:"); await state.set_state(AdminS.U)

@router.message(AdminS.U)
async def gs_u(m: Message, state: FSMContext): 
    await state.update_data(u=m.text); await m.answer("📅 Сколько дней добавить?"); await state.set_state(AdminS.D)

@router.message(AdminS.D)
async def gs_d(m: Message, state: FSMContext): 
    try:
        d=await state.get_data()
        await db.upsert_user(int(d['u']), "AdminAdd")
        await db.add_sub_days(int(d['u']), int(m.text))
        await m.answer("✅ Подписка выдана.")
    except Exception as e: await m.answer(f"Ошибка: {e}")
    await state.clear()

# --- ЗАПУСК ---

async def main():
    await db.init()
    
    # Автозапуск активных сессий
    cnt = 0
    for f in cfg.SESSION_DIR.glob("session_*.session"):
        try:
            uid = int(f.stem.split("_")[1])
            if await db.check_sub_bool(uid):
                w = Worker(uid)
                if await w.start(): W_POOL[uid] = w; cnt+=1
        except: pass
        
    logger.info(f"🔥 StatPro v65.1 запущен. Активных воркеров: {cnt}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
