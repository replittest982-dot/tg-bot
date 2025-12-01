import logging
import asyncio
import os
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

from telethon import TelegramClient, functions, errors
from telethon.tl.types import User, LoginToken, LoginTokenMigrateTo

# 🎨 Добавленные библиотеки для QR-кода
import qrcode
from io import BytesIO 

# --- 1. КОНФИГУРАЦИЯ (ОБНОВЛЕНО) ---
API_ID = 35775411  
API_HASH = '4f8220840326cb5f74e1771c0c4248f2'  
BOT_TOKEN = '7868097991:AAFpy_z12t8noMn96rO1LtIJiADOhAfbwYY'  
ADMIN_ID = 6256576302 

SESSIONS_DIR = 'sessions'
DATA_DIR = 'data'
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

# --- 2. БАЗА ДАННЫХ ---
# Временное хранение данных (не сохраняется при перезапуске)
DB = {'users': {ADMIN_ID: {'subscription': datetime(2025, 12, 31, 15, 30, 41)}}, 'workers': {}}

def init_db():
    if ADMIN_ID not in DB['users']:
        DB['users'][ADMIN_ID] = {'subscription': datetime.now() + timedelta(days=30)}
    logger.info(f"Admin {ADMIN_ID} subscription: {DB['users'][ADMIN_ID]['subscription']}")

# --- 3. СОСТОЯНИЯ FSM ---
class AuthStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    # Опционально: ожидание сканирования QR
    waiting_for_qr_scan = State()

# --- 4. КЛАСС AuthClient (С QR-логикой) ---
class AuthClient:
    def __init__(self, user_id):
        self.user_id = user_id
        self.session_path = os.path.join(SESSIONS_DIR, f'{user_id}.session')
        self.phone = None
        self.phone_code_hash = None
        self.client = TelegramClient(self.session_path, API_ID, API_HASH)
    
    async def connect(self):
        if not self.client.is_connected():
            await self.client.connect()
        return self.client

    async def disconnect(self):
        if self.client and self.client.is_connected():
            await self.client.disconnect()

    def clear_session_file(self):
        try:
            if os.path.exists(self.session_path):
                os.remove(self.session_path)
            logger.info(f"Worker {self.user_id}: Temporary session file cleared.")
        except Exception as e:
            logger.error(f"Worker {self.user_id}: Error clearing session file: {e}")
    
    async def qr_login(self):
        client = await self.connect()
        user_id = self.user_id
        
        try:
            # 1. Запрос токена
            result = await client(functions.auth.ExportLoginTokenRequest(
                api_id=API_ID, api_hash=API_HASH, except_ids=[]
            ))
            
            # 2. Обработка миграции DC (Критическое исправление)
            if isinstance(result, LoginTokenMigrateTo):
                await client.disconnect() 
                self.client._sender._dc_id = result.dc_id  
                await self.client.connect()
                result = await self.client(functions.auth.ImportLoginTokenRequest(result.token))
            
            if isinstance(result, LoginToken) and result.url:
                logger.info(f"QR URL получен: {result.url[:50]}...")
                
                # 3. ✅ ГЕНЕРАЦИЯ QR-КОДА
                qr = qrcode.QRCode(
                    version=1,
                    error_correction=qrcode.constants.ERROR_CORRECT_L,
                    box_size=10,
                    border=4,
                )
                qr.add_data(result.url)
                qr.make(fit=True)
                img = qr.make_image(fill_color="black", back_color="white")
                
                # 4. Сохранение во временный файл
                qr_path = os.path.join(SESSIONS_DIR, f'{user_id}_qr.png')
                img.save(qr_path)
                
                # Возвращаем путь к файлу
                return True, qr_path
            else:
                raise Exception("QR token без URL")
                
        except Exception as e:
            await self.disconnect()
            self.clear_session_file()
            logger.error(f"QR error {user_id}: {e}")
            return False, f"❌ QR ошибка. Попробуйте по номеру: {str(e)}"
    
    # Методы send_code, sign_in, sign_in_password остаются без изменений
    async def send_code(self, phone):
        self.phone = phone
        client = await self.connect()
        try:
            result = await client.send_code_request(phone)
            self.phone_code_hash = result.phone_code_hash
            logger.info(f"Code sent to {phone}")
            return True, None
        except errors.PhoneNumberInvalidError:
            return False, "❌ Неверный формат номера (+79001234567)"
        except errors.FloodWaitError as e:
            return False, f"❌ Flood wait: {e.seconds}с"
        except Exception as e:
            await self.disconnect()
            self.clear_session_file()
            logger.error(f"Send code error {self.user_id}: {e}")
            return False, f"❌ Ошибка отправки: {str(e)}"
    
    async def sign_in(self, code):
        if not self.client or not self.phone_code_hash:
            return False, "❌ Сессия утеряна. /start"
        
        client = await self.connect() 
        try:
            user = await client.sign_in(phone=self.phone, code=code, phone_code_hash=self.phone_code_hash)
            if isinstance(user, User):
                DB['workers'][self.user_id] = {'session_path': self.session_path}
                await self.disconnect()  
                return True, f"✅ Успех! Аккаунт: {user.first_name} ({user.id})"
            return False, "❌ Неизвестная ошибка входа"
            
        except errors.SessionPasswordNeededError:
            return True, "🔑 **Требуется пароль 2FA.** Введите:"
        except errors.PhoneCodeExpiredError:
            await self.disconnect()
            self.clear_session_file() 
            return False, "⏰ Код истек. Нажми /start, чтобы начать заново."
        except errors.PhoneCodeInvalidError:
            return False, "❌ Неверный код. Попробуй ещё раз"
        except Exception as e:
            await self.disconnect()
            self.clear_session_file()
            logger.error(f"SignIn error {self.user_id}: {e}")
            return False, f"❌ Ошибка: {str(e)}. Нажми /start."
    
    async def sign_in_password(self, password):
        client = await self.connect() 
        try:
            user = await client.sign_in(password=password)
            if isinstance(user, User):
                DB['workers'][self.user_id] = {'session_path': self.session_path}
                await self.disconnect() 
                return True, f"✅ Успех! {user.first_name} ({user.id})"
            return False, "❌ Ошибка входа с паролем"
        except errors.PasswordHashInvalidError:
            return False, "❌ Неверный пароль 2FA. Попробуй ещё раз"
        except Exception as e:
            await self.disconnect()
            self.clear_session_file()
            logger.error(f"Password error {self.user_id}: {e}")
            return False, f"❌ Ошибка: {str(e)}. Нажми /start."


# --- 5. КЛАВИАТУРЫ ---
AUTH_KEYBOARD = types.InlineKeyboardMarkup(row_width=1).add(
    types.InlineKeyboardButton("🔑 QR авторизация", callback_data="qr_auth"),
    types.InlineKeyboardButton("📞 По номеру", callback_data="phone_auth")
)

RESEND_KEYBOARD = types.InlineKeyboardMarkup().add(
    types.InlineKeyboardButton("🔄 Код ещё раз", callback_data="resend_code")
)

# --- 6. ХЕНДЛЕРЫ ---
@dp.message_handler(commands=['start'])
async def start_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("🚫 Доступ только для админа")
    await state.finish()
    await message.reply("Выбери метод:", reply_markup=AUTH_KEYBOARD)

@dp.callback_query_handler(lambda c: c.data == 'qr_auth', state="*")
async def qr_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await bot.answer_callback_query(callback.id, "🚫 Нет доступа")
    
    await bot.answer_callback_query(callback.id)
    user_id = callback.from_user.id
    
    auth_client = AuthClient(user_id)
    auth_client.clear_session_file()
    await state.update_data(auth_client=auth_client)
    
    # Вызываем логику QR. result_path будет путем к файлу или сообщением об ошибке
    success, result_path = await auth_client.qr_login()
    
    if success:
        qr_path = result_path
        # ✅ ОТПРАВКА QR-КОДА
        try:
            with open(qr_path, 'rb') as photo:
                await bot.send_photo(
                    user_id,
                    photo,
                    caption="✅ **QR-код для авторизации готов!** Отсканируйте его официальным клиентом Telegram. (Действителен ~5 минут)"
                )
            await AuthStates.waiting_for_qr_scan.set()
        except Exception as e:
            logger.error(f"Error sending QR: {e}")
            await bot.send_message(user_id, "❌ Не удалось отправить QR-код. Попробуйте по номеру.")
        finally:
            # Удаляем временный файл
            if os.path.exists(qr_path):
                os.remove(qr_path)
        
    else:
        # result_path здесь содержит сообщение об ошибке
        await bot.send_message(user_id, result_path)
        await bot.send_message(user_id, "Или нажми на кнопку:", reply_markup=types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("📞 По номеру", callback_data="phone_auth")
        ))

# Этот хендлер ловит любые сообщения, пока мы ждем QR-сканирования
@dp.message_handler(state=AuthStates.waiting_for_qr_scan)
async def process_qr_wait(message: types.Message, state: FSMContext):
    # Технически, Telethon сам обрабатывает завершение сессии в фоновом режиме
    # Но мы должны уведомить пользователя, что любое сообщение пока не принимается.
    await message.reply("Ожидаем сканирования QR-кода. Если QR-код истек или не сработал, начните заново /start.")
    # Тут можно добавить логику проверки статуса клиента, но это усложнит код. 
    # Лучше просто полагаться на то, что сессия либо сохранится, либо истечет.

@dp.callback_query_handler(lambda c: c.data in ['phone_auth', 'resend_code'], state="*")
async def phone_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await bot.answer_callback_query(callback.id, "🚫 Нет доступа")
    
    await bot.answer_callback_query(callback.id)
    user_id = callback.from_user.id
    
    data = await state.get_data()
    if 'auth_client' not in data or callback.data == 'phone_auth':
        auth_client = AuthClient(user_id)
        auth_client.clear_session_file()
        await state.update_data(auth_client=auth_client)
    else:
        auth_client = data['auth_client']

    await bot.send_message(
        user_id, 
        "📞 **Введите номер** (+79001234567):"
    )
    await AuthStates.waiting_for_phone.set()


@dp.message_handler(state=AuthStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    data = await state.get_data()
    auth_client = data['auth_client']
    
    success, msg = await auth_client.send_code(message.text.strip())
    if success:
        await message.reply("🔑 **Код отправлен!** Введите код:", reply_markup=RESEND_KEYBOARD) 
        await AuthStates.waiting_for_code.set()
    else:
        await message.reply(msg)

@dp.message_handler(state=AuthStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    data = await state.get_data()
    auth_client = data['auth_client']
    
    success, msg = await auth_client.sign_in(message.text.strip())
    
    await message.reply(msg)
    
    if "✅ Успех" in msg:  # Полный успех
        await state.finish()
    elif "🔑 Требуется пароль" in msg:  # 2FA
        await AuthStates.waiting_for_password.set()
    else:  # Ошибка (Код истек/Неверный код)
        if "Нажми /start" in msg:
            await state.finish()


@dp.message_handler(state=AuthStates.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    data = await state.get_data()
    auth_client = data['auth_client']
    
    success, msg = await auth_client.sign_in_password(message.text.strip())
    await message.reply(msg)
    
    if success:
        await state.finish()
    else:
        if "Нажми /start" in msg:
            await state.finish()
        else:
            await message.reply("❌ Попробуй ещё раз или /start")

# --- 7. ЗАПУСК ---
async def on_startup(_):
    init_db()
    logger.info("✅ Bot started")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
