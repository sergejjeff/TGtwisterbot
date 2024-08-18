import asyncio
import logging
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from datetime import datetime, timedelta
import os
import json
from dotenv import load_dotenv
import pymysql
import pymysql.cursors
import pandas as pd
import tempfile
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import subprocess

# Загрузка переменных окружения
load_dotenv()

# Логирование
logging.basicConfig(level=logging.INFO)

class Database:
    def __init__(self):
        self.connection = self.create_connection()

    def create_connection(self):
        return pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            db=os.getenv('DB_NAME'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def ensure_connection(self):
        try:
            self.connection.ping(reconnect=True)
        except pymysql.MySQLError:
            self.connection = self.create_connection()

    def __enter__(self):
        self.ensure_connection()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()

    def get_user_by_telegram_id(self, telegram_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM users WHERE telegram_id = %s"
            cursor.execute(sql, (telegram_id,))
            result = cursor.fetchone()
            return result

    def get_user_by_id(self, user_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM users WHERE user_id = %s"
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            return result

    def insert_user(self, user_data):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            INSERT INTO users (telegram_id, username, first_name, last_name, joined_at, tags, is_admin, lead_magnet_id, invited_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                user_data['telegram_id'],
                user_data['username'],
                user_data['first_name'],
                user_data['last_name'],
                user_data['joined_at'],
                user_data['tags'],
                user_data['is_admin'],
                user_data['lead_magnet_id'],
                user_data['invited_by']
            ))
            self.connection.commit()

    def get_template_by_type(self, template_type):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM message_templates WHERE template_type = %s"
            cursor.execute(sql, (template_type,))
            result = cursor.fetchone()
            return result

    def set_config_value(self, name, value):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "INSERT INTO config (name, value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE value = VALUES(value)"
            cursor.execute(sql, (name, value))
            self.connection.commit()

    def update_template(self, template_id, content, media_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            UPDATE message_templates
            SET content = %s, media_id = %s
            WHERE template_id = %s
            """
            cursor.execute(sql, (content, media_id, template_id))
            self.connection.commit()

    def insert_template(self, template_type, content, media_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            INSERT INTO message_templates (template_type, content, media_id)
            VALUES (%s, %s, %s)
            """
            cursor.execute(sql, (template_type, content, media_id))
            self.connection.commit()

    def get_all_subscribers(self):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM users"
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def get_all_tags(self):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT DISTINCT JSON_EXTRACT(tags, '$[*]') as tags FROM users"
            cursor.execute(sql)
            result = cursor.fetchall()
            tags = set()
            for row in result:
                tags.update(json.loads(row['tags']))
            return list(tags)

    def get_all_templates(self):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM message_templates"
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def insert_scheduled_broadcast(self, broadcast_data):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            INSERT INTO broadcasts (template_id, tag, scheduled_time, status)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(sql, (
                broadcast_data['template_id'],
                broadcast_data['tag'],
                broadcast_data['scheduled_time'],
                broadcast_data['status']
            ))
            self.connection.commit()

    def get_subscribers_by_tag(self, tag):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT telegram_id FROM users WHERE JSON_CONTAINS(tags, %s)"
            cursor.execute(sql, (json.dumps([tag]),))
            result = cursor.fetchall()
            return result

    def get_scheduled_broadcasts(self):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            SELECT b.broadcast_id, b.tag, b.scheduled_time, t.content, t.media_id 
            FROM broadcasts b
            JOIN message_templates t ON b.template_id = t.template_id
            WHERE b.status = 'scheduled' AND b.scheduled_time <= NOW()
            """
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def update_broadcast_status(self, broadcast_id, status):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "UPDATE broadcasts SET status = %s WHERE broadcast_id = %s"
            cursor.execute(sql, (status, broadcast_id))
            self.connection.commit()

    def insert_autoresponder(self, content, delay, media_id=None):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            INSERT INTO autoresponders (content, delay, media_id)
            VALUES (%s, %s, %s)
            """
            cursor.execute(sql, (content, delay, media_id))
            self.connection.commit()

    def get_all_autoresponders(self):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM autoresponders"
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def delete_autoresponder(self, autoresponder_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "DELETE FROM autoresponders WHERE id = %s"
            cursor.execute(sql, (autoresponder_id,))
            self.connection.commit()

    def update_autoresponder(self, autoresponder_id, content, delay, media_id=None):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            UPDATE autoresponders
            SET content = %s, delay = %s, media_id = %s
            WHERE id = %s
            """
            cursor.execute(sql, (content, delay, media_id, autoresponder_id))
            self.connection.commit()

    def mark_autoresponder_as_sent(self, user_id, autoresponder_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            INSERT INTO sent_autoresponders (user_id, autoresponder_id, sent_at)
            VALUES (%s, %s, %s)
            """
            cursor.execute(sql, (user_id, autoresponder_id, datetime.now()))
            self.connection.commit()

    def is_autoresponder_sent(self, user_id, autoresponder_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            SELECT COUNT(*) as count FROM sent_autoresponders
            WHERE user_id = %s AND autoresponder_id = %s
            """
            cursor.execute(sql, (user_id, autoresponder_id))
            result = cursor.fetchone()
            return result['count'] > 0

    def insert_lead_magnet(self, name, description, image_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            INSERT INTO lead_magnets (name, description, image_id)
            VALUES (%s, %s, %s)
            """
            cursor.execute(sql, (name, description, image_id))
            self.connection.commit()

    def get_all_lead_magnets(self):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM lead_magnets"
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def delete_lead_magnet(self, lead_magnet_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "DELETE FROM lead_magnets WHERE id = %s"
            cursor.execute(sql, (lead_magnet_id,))
            self.connection.commit()

    def update_lead_magnet(self, lead_magnet_id, name, description, image_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            UPDATE lead_magnets
            SET name = %s, description = %s, image_id = %s
            WHERE id = %s
            """
            cursor.execute(sql, (name, description, image_id, lead_magnet_id))
            self.connection.commit()

    def update_user_lead_magnet(self, telegram_id, lead_magnet_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "UPDATE users SET lead_magnet_id = %s WHERE telegram_id = %s"
            cursor.execute(sql, (lead_magnet_id, telegram_id))
            self.connection.commit()

    def get_lead_magnet(self, lead_magnet_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM lead_magnets WHERE id = %s"
            cursor.execute(sql, (lead_magnet_id,))
            result = cursor.fetchone()
            return result

    def update_user_received_gift(self, user_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "UPDATE users SET received_gift = TRUE WHERE user_id = %s"
            cursor.execute(sql, (user_id,))
            self.connection.commit()

    def get_config_value(self, name):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT value FROM config WHERE name = %s"
            cursor.execute(sql, (name,))
            result = cursor.fetchone()
            return int(result['value'])

    def get_admins(self):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT telegram_id FROM users WHERE is_admin = TRUE"
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def update_referrals(self, user_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "UPDATE users SET referrals = referrals + 1 WHERE user_id = %s"
            cursor.execute(sql, (user_id,))
            self.connection.commit()

    def update_user(self, user_id, updates):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            set_clause = ", ".join([f"{key} = %s" for key in updates.keys()])
            sql = f"UPDATE users SET {set_clause} WHERE user_id = %s"
            values = list(updates.values()) + [user_id]
            cursor.execute(sql, values)
            self.connection.commit()

    def log_message(self, user_id, message_type):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            INSERT INTO message_logs (user_id, message_type, sent_at)
            VALUES (%s, %s, %s)
            """
            cursor.execute(sql, (user_id, message_type, datetime.now()))
            self.connection.commit()

    def update_message_log(self, log_id, field):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = f"UPDATE message_logs SET {field} = %s WHERE id = %s"
            cursor.execute(sql, (datetime.now(), log_id))
            self.connection.commit()

    def get_new_subscribers_count(self, start_date, end_date):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            SELECT COUNT(*) as count FROM users
            WHERE joined_at BETWEEN %s AND %s
            """
            cursor.execute(sql, (start_date, end_date))
            result = cursor.fetchone()
            return result['count']

    def get_user_activity(self, start_date, end_date):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            SELECT user_id, COUNT(*) as message_count FROM message_logs
            WHERE sent_at BETWEEN %s AND %s
            GROUP BY user_id
            """
            cursor.execute(sql, (start_date, end_date))
            result = cursor.fetchall()
            return result

    def get_sent_messages_count(self, start_date, end_date):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            SELECT COUNT(*) as count FROM message_logs
            WHERE sent_at BETWEEN %s AND %s
            """
            cursor.execute(sql, (start_date, end_date))
            result = cursor.fetchone()
            return result['count']

    def get_open_rate(self, start_date, end_date):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            SELECT COUNT(*) as opened_count FROM message_logs
            WHERE opened_at IS NOT NULL AND sent_at BETWEEN %s AND %s
            """
            cursor.execute(sql, (start_date, end_date))
            opened_result = cursor.fetchone()

            sql = """
            SELECT COUNT(*) as total_count FROM message_logs
            WHERE sent_at BETWEEN %s AND %s
            """
            cursor.execute(sql, (start_date, end_date))
            total_result = cursor.fetchone()

            if total_result['total_count'] == 0:
                return 0
            return (opened_result['opened_count'] / total_result['total_count']) * 100

    def get_response_rate(self, start_date, end_date):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            SELECT COUNT(*) as responded_count FROM message_logs
            WHERE responded_at IS NOT NULL AND sent_at BETWEEN %s AND %s
            """
            cursor.execute(sql, (start_date, end_date))
            responded_result = cursor.fetchone()

            sql = """
            SELECT COUNT(*) as total_count FROM message_logs
            WHERE sent_at BETWEEN %s AND %s
            """
            cursor.execute(sql, (start_date, end_date))
            total_result = cursor.fetchone()

            if total_result['total_count'] == 0:
                return 0
            return (responded_result['responded_count'] / total_result['total_count']) * 100

    def get_lead_magnet_effectiveness(self):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            SELECT lm.name, COUNT(*) as count FROM users u
            JOIN lead_magnets lm ON u.lead_magnet_id = lm.id
            GROUP BY lm.name
            """
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def get_user_invitations(self):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = """
            SELECT u1.username, COUNT(u2.user_id) as invitations
            FROM users u1
            JOIN users u2 ON u1.user_id = u2.invited_by
            GROUP BY u1.username
            """
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def add_user_file(self, user_id, file_size):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "INSERT INTO user_files (user_id, file_size, uploaded_at) VALUES (%s, %s, %s)"
            cursor.execute(sql, (user_id, file_size, datetime.now()))
            self.connection.commit()

    def get_user_storage_size(self, user_id):
        self.ensure_connection()
        with self.connection.cursor() as cursor:
            sql = "SELECT SUM(file_size) as total_size FROM user_files WHERE user_id = %s"
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            return result['total_size'] if result['total_size'] else 0

class TelegramBot:
    def __init__(self, api_token):
        self.bot = Bot(token=api_token)
        self.storage = MemoryStorage()
        self.dp = Dispatcher(self.bot, storage=self.storage)
        self.bot_username = None  # Инициализируем переменную для имени пользователя бота
        self.register_handlers()
        loop = asyncio.get_event_loop()
        self.setup_scheduled_tasks()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.set_bot_username())  # Получаем имя пользователя бота

    async def set_bot_username(self):
        bot_info = await self.bot.get_me()
        self.bot_username = bot_info.username

    def substitute_variables(self, template, user_data):
        required_keys = ['telegram_id', 'first_name', 'last_name', 'username', 'referrals']
        for key in required_keys:
            if key not in user_data:
                logging.error(f"Missing key in user data for template substitution: {key}")
                logging.error(f"User data: {user_data}")
                return template  # Возвращаем оригинальный шаблон без подстановок

        try:
            remaining_referrals = self.get_remaining_referrals(user_data['telegram_id'])
            invite_url = f"https://t.me/{self.bot_username}?start={user_data['telegram_id']}"
            substitutions = {
                'first_name': user_data.get('first_name', ''),
                'last_name': user_data.get('last_name', ''),
                'username': user_data.get('username', ''),
                'telegram_id': user_data.get('telegram_id', ''),
                'referrals': user_data.get('referrals', 0),
                'remaining_referrals': remaining_referrals,
                'required_referrals': self.get_required_referrals(),
                'invite_url': invite_url,
                'lead_magnet_name': user_data.get('lead_magnet_name', '')  # Добавьте другие переменные по мере необходимости
            }
            return template.format(**substitutions)
        except KeyError as e:
            logging.error(f"Missing key in user data for template substitution: {e}")
            logging.error(f"User data: {user_data}")
            return template

    def get_required_referrals(self):
        with Database() as db:
            return db.get_config_value('required_referrals')

    def get_remaining_referrals(self, telegram_id):
        with Database() as db:
            user = db.get_user_by_telegram_id(telegram_id)
            required_referrals = db.get_config_value('required_referrals')
            return max(0, required_referrals - user['referrals'])

    async def send_message_with_buttons(self, chat_id, text, image_id, buttons, user_data):
        logging.info(f"Sending message with buttons to chat_id: {chat_id}, user_data: {user_data}")
        if not user_data:  # Добавим проверку и логирование
            logging.error(f"User data is empty for chat_id: {chat_id}")
        text = self.substitute_variables(text, user_data)
        markup = types.InlineKeyboardMarkup()
        for button in buttons:
            markup.add(types.InlineKeyboardButton(text=button['text'], callback_data=button['callback_data']))
        
        if image_id:
            file = await self.bot.get_file(image_id)
            file_size = file.file_size
            if file_size > 50 * 1024 * 1024:  # 50 MB
                await self.bot.send_message(chat_id, "Размер файла превышает 50 МБ и не может быть отправлен.")
                return
            await self.bot.send_photo(chat_id, image_id, caption=text, reply_markup=markup)
        else:
            await self.bot.send_message(chat_id, text, reply_markup=markup)

    async def greet_user(self, message: types.Message):
        telegram_id = message.from_user.id
        username = message.from_user.username
        first_name = message.from_user.first_name
        last_name = message.from_user.last_name

        logging.info(f"Handling /start for user: {telegram_id}, {username}, {first_name}, {last_name}")
        
        with Database() as db:
            user = db.get_user_by_telegram_id(telegram_id)
        
        if not user:
            user_data = {
                'telegram_id': telegram_id,
                'username': username,
                'first_name': first_name,
                'last_name': last_name,
                'joined_at': datetime.now(),
                'tags': json.dumps(['Новенький']),
                'is_admin': False,
                'lead_magnet_id': None,
                'invited_by': None,
                'referrals': 0
            }
            
            logging.info(f"New user data: {user_data}")
            
            if message.get_args():
                invited_by_telegram_id = message.get_args()
                with Database() as db:
                    invited_by_user = db.get_user_by_telegram_id(invited_by_telegram_id)
                    if invited_by_user:
                        user_data['invited_by'] = invited_by_user['user_id']
                        db.update_referrals(invited_by_user['user_id'])
                        self.update_user_tags(invited_by_user['user_id'], ['Активный рефовод'])
            
            with Database() as db:
                db.insert_user(user_data)
            user = user_data
            await self.notify_admin_new_subscriber(user_data)
        else:
            logging.info(f"Existing user data: {user}")
        
        is_admin = user.get('is_admin', False)
        
        with Database() as db:
            welcome_template = db.get_template_by_type('welcome_template')
        
        if welcome_template:
            welcome_msg = welcome_template['content']
            welcome_image_id = welcome_template['media_id']
        else:
            welcome_msg = "Добро пожаловать в наш бот!"
            welcome_image_id = None
        
        buttons = [
            {'text': '🔗 Пригласительная ссылка', 'callback_data': 'send_invite_link'},
            {'text': '🎁 Получить подарок', 'callback_data': 'get_gift'}
        ]
        
        if is_admin:
            buttons.append({'text': '⚙️ Админ панель', 'callback_data': 'admin_panel'})
        
        logging.info(f"Calling send_message_with_buttons from greet_user with user_data: {user}")
        await self.send_message_with_buttons(telegram_id, welcome_msg, welcome_image_id, buttons, user)
        
        await self.show_lead_magnets(telegram_id, user)

    async def send_invite_link(self, call: types.CallbackQuery):
        telegram_id = call.message.chat.id
        with Database() as db:
            user = db.get_user_by_telegram_id(telegram_id)
        logging.info(f"Calling send_message_with_buttons from send_invite_link with user_data: {user}")
        await self.send_message_with_buttons(
            telegram_id,
            f"Ваша пригласительная ссылка: https://t.me/{self.bot_username}?start={telegram_id}",
            None,
            [],
            user
        )
    
    async def notify_admin_new_subscriber(self, user_data):
        with Database() as db:
            admins = db.get_admins()
        for admin in admins:
            message = (
                f"Новый подписчик: {user_data['first_name']} {user_data['last_name']}\n"
                f"ID: {user_data['telegram_id']}\n"
                f"Юзернейм: @{user_data['username']}\n"
                f"Дата и время подписки: {user_data['joined_at']}"
            )
            logging.info(f"Calling notify_admin_new_subscriber with user_data: {user_data}")
            await self.bot.send_message(admin['telegram_id'], message)

    async def show_admin_panel(self, chat_id):
        buttons = [
            {'text': '🔢 Установить подписчиков для подарка', 'callback_data': 'set_subscribers_for_gift', 'description': 'Установите количество подписчиков для получения подарка'},
            {'text': '📝 Менеджер сообщений', 'callback_data': 'message_manager', 'description': 'Управляйте шаблонами сообщений'},
            {'text': '📋 Список подписчиков', 'callback_data': 'show_subscribers_list', 'description': 'Просмотр всех подписчиков'},
            {'text': '📨 Мгновенная рассылка', 'callback_data': 'instant_broadcast', 'description': 'Отправить мгновенную рассылку'},
            {'text': '⏳ Отложенная рассылка', 'callback_data': 'scheduled_broadcast', 'description': 'Запланировать рассылку на будущее'},
            {'text': '🕰️ Менеджер автосообщений', 'callback_data': 'manage_autoresponders', 'description': 'Управление автоматическими сообщениями'},
            {'text': '🎁 Менеджер лид-магнитов', 'callback_data': 'manage_lead_magnets', 'description': 'Управление лид-магнитами'},
            {'text': '📊 Аналитика', 'callback_data': 'show_analytics', 'description': 'Просмотр аналитики'}  # Добавлена кнопка Аналитика
        ]
        markup = types.InlineKeyboardMarkup()
        for button in buttons:
            markup.add(types.InlineKeyboardButton(text=button['text'], callback_data=button['callback_data']))
        await self.bot.send_message(chat_id, "Добро пожаловать в админ панель! Используйте кнопки ниже для навигации:", reply_markup=markup)

    async def show_lead_magnets(self, chat_id, user):
        with Database() as db:
            lead_magnets = db.get_all_lead_magnets()
            user_lead_magnets = json.loads(user['tags']) if 'tags' in user else []
            available_lead_magnets = [lm for lm in lead_magnets if lm['id'] not in user_lead_magnets]
        if not available_lead_magnets:
            await self.bot.send_message(chat_id, "Все лид-магниты уже получены.")
            return

        lm_buttons = [{'text': lm['name'], 'callback_data': f"select_lead_magnet_{lm['id']}"} for lm in available_lead_magnets]
        logging.info(f"Calling send_message_with_buttons from show_lead_magnets with user_data: {user}")
        await self.send_message_with_buttons(chat_id, "Выберите лид-магнит:", None, lm_buttons, user)

    async def broadcast_message_by_tag(self, tag, template_type):
        with Database() as db:
            subscribers = db.get_subscribers_by_tag(tag)
            template = db.get_template_by_type(template_type)
        for sub in subscribers:
            user_data = db.get_user_by_telegram_id(sub['telegram_id'])
            await self.send_message(sub['telegram_id'], template['content'], template['media_id'], user_data)

    async def schedule_broadcast(self, chat_id, tag, template_type):
        await self.bot.send_message(chat_id, "Введите время отправки (в формате YYYY-MM-DD HH:MM):")
        await Form.scheduling_broadcast.set()
        async with self.dp.current_state(chat=chat_id).proxy() as data:
            data['tag'] = tag
            data['template_type'] = template_type

    async def manage_lead_magnets(self, chat_id):
        buttons = [
            {'text': 'Добавить лид-магнит', 'callback_data': 'add_lead_magnet'},
            {'text': 'Список лид-магнитов', 'callback_data': 'list_lead_magnets'},
            {'text': 'Назад', 'callback_data': 'admin_panel'}
        ]
        logging.info(f"Calling send_message_with_buttons from manage_lead_magnets with user_data: empty")
        await self.send_message_with_buttons(chat_id, "Менеджер лид-магнитов:", None, buttons, {})

    async def list_lead_magnets(self, call: types.CallbackQuery):
        chat_id = call.message.chat.id
        with Database() as db:
            lead_magnets = db.get_all_lead_magnets()
        if not lead_magnets:
            await self.bot.send_message(chat_id, "Список лид-магнитов пуст.")
            return

        lead_magnet_list = "\n".join([f"{lm['id']}: {lm['name']} - {lm['description']}" for lm in lead_magnets])
        await self.bot.send_message(chat_id, f"Список лид-магнитов:\n\n{lead_magnet_list}")
    
    async def process_lead_magnet_selection(self, call: types.CallbackQuery):
        lead_magnet_id = int(call.data.split('_')[-1])
        telegram_id = call.message.chat.id
        with Database() as db:
            db.update_user_lead_magnet(telegram_id, lead_magnet_id)
            lead_magnet = db.get_lead_magnet(lead_magnet_id)
            user = db.get_user_by_telegram_id(telegram_id)
            await self.send_message(
                telegram_id,
                f"Вы выбрали лид-магнит: {lead_magnet['name']}. Для его получения пригласите {db.get_config_value('required_referrals')} новых пользователей.",
                lead_magnet['image_id'],
                user
            )

    async def handle_gift_reception(self, call: types.CallbackQuery):
        telegram_id = call.message.chat.id
        with Database() as db:
            user = db.get_user_by_telegram_id(telegram_id)
            required_referrals = db.get_config_value('required_referrals')
            if user['referrals'] >= required_referrals and not user['received_gift']:
                lead_magnet = db.get_lead_magnet(user['lead_magnet_id'])
                if lead_magnet:
                    logging.info(f"Calling send_message_with_buttons from handle_gift_reception with user_data: {user}")
                    await self.send_message_with_buttons(
                        telegram_id,
                        f"Поздравляем! Вы получили подарок: {lead_magnet['name']}\n\nОписание: {lead_magnet['description']}",
                        lead_magnet['image_id'],
                        user
                    )
                    db.update_user_received_gift(user['user_id'])
                    self.update_user_tags(user['user_id'], [lead_magnet['id']])
                else:
                    await self.bot.send_message(telegram_id, "Произошла ошибка. Лид-магнит не найден.")
            elif user['referrals'] < required_referrals:
                remaining_referrals = required_referrals - user['referrals']
                await self.bot.send_message(telegram_id, f"Вам необходимо пригласить еще {remaining_referrals} пользователей для получения подарка.")
                if remaining_referrals == 1:
                    self.update_user_tags(user['user_id'], ['до подарка 1 реф'])
            else:
                await self.bot.send_message(telegram_id, "Вы уже получили свой подарок.")
                
    def update_user_tags(self, user_id, new_tags):
        with Database() as db:
            user = db.get_user_by_id(user_id)
            tags = json.loads(user['tags']) if user['tags'] else []
            for tag in new_tags:
                if tag not in tags:
                    tags.append(tag)
            db.update_user(user_id, {'tags': json.dumps(tags)})

    async def show_analytics(self, message: types.Message):
        chat_id = message.chat.id
        today = datetime.now()
        last_week = today - timedelta(days=7)

        with Database() as db:
            new_subscribers = db.get_new_subscribers_count(last_week, today)
            user_activity = db.get_user_activity(last_week, today)
            sent_messages = db.get_sent_messages_count(last_week, today)
            open_rate = db.get_open_rate(last_week, today)
            response_rate = db.get_response_rate(last_week, today)
            lead_magnet_effectiveness = db.get_lead_magnet_effectiveness()
            user_invitations = db.get_user_invitations()

        analytics_message = (
            f"📊 Аналитика за последнюю неделю:\n\n"
            f"👥 Новые подписчики: {new_subscribers}\n"
            f"✉️ Отправленные сообщения: {sent_messages}\n"
            f"📖 Процент открытий: {open_rate:.2f}%\n"
            f"💬 Процент откликов: {response_rate:.2f}%\n\n"
            f"🎁 Эффективность лид-магнитов:\n"
        )

        for lm in lead_magnet_effectiveness:
            analytics_message += f" - {lm['name']}: {lm['count']} пользователей\n"

        analytics_message += "\n👥 Количество приглашений через пользователей:\n"

        for user in user_invitations:
            analytics_message += f" - @{user['username']}: {user['invitations']} приглашений\n"

        await self.bot.send_message(chat_id, analytics_message)

    async def show_subscribers_list(self, call: types.CallbackQuery):
        chat_id = call.message.chat.id
        with Database() as db:
            subscribers = db.get_all_subscribers()
        if not subscribers:
            await self.bot.send_message(chat_id, "Список подписчиков пуст.")
            return

        subscriber_list = "\n".join([f"ID: {sub['user_id']}\nUsername: {sub['username']}\nName: {sub['first_name']} {sub['last_name']}\nJoined at: {sub['joined_at']}\nTags: {sub['tags']}" for sub in subscribers])
        await self.bot.send_message(chat_id, f"Список подписчиков:\n\n{subscriber_list}")

        # Добавляем кнопку Экспорт в Excel
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(text="Экспорт в Excel", callback_data="export_to_excel"))
        await self.bot.send_message(chat_id, "Вы можете экспортировать список в Excel:", reply_markup=markup)

    async def handle_export_to_excel(self, call: types.CallbackQuery):
        chat_id = call.message.chat.id
        file_path = self.export_subscribers_to_excel()
        with open(file_path, 'rb') as file:
            await self.bot.send_document(chat_id, file)

    def export_subscribers_to_excel(self):
        with Database() as db:
            subscribers = db.get_all_subscribers()
        df = pd.DataFrame(subscribers)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            file_path = tmp.name
        df.to_excel(file_path, index=False)
        return file_path

    def register_handlers(self):
        @self.dp.message_handler(commands=['start'])
        async def handle_start(message: types.Message):
            await self.greet_user(message)

        @self.dp.message_handler(commands=['help'])
        async def handle_help(message: types.Message):
            help_message = (
                "Доступные команды:\n"
                "/start - Запуск бота\n"
                "/help - Помощь\n"
                "/invite - Получить пригласительную ссылку\n"
                "/gift - Получить информацию о подарке\n"
                "/analytics - Показать аналитику\n"
            )
            await self.bot.send_message(message.chat.id, help_message)

        @self.dp.message_handler(commands=['analytics'])
        async def handle_analytics(message: types.Message):
            await self.show_analytics(message)

        @self.dp.callback_query_handler(lambda call: call.data == 'admin_panel')
        async def handle_admin_panel_callback(call: types.CallbackQuery):
            await self.show_admin_panel(call.message.chat.id)

        @self.dp.callback_query_handler(lambda call: call.data == 'manage_lead_magnets')
        async def handle_manage_lead_magnets_callback(call: types.CallbackQuery):
            await self.manage_lead_magnets(call.message.chat.id)

        # Новый обработчик для кнопки Аналитика
        @self.dp.callback_query_handler(lambda call: call.data == 'show_analytics')
        async def handle_show_analytics_callback(call: types.CallbackQuery):
            await self.show_analytics(call.message)

        @self.dp.callback_query_handler(lambda call: call.data == 'show_subscribers_list')
        async def handle_show_subscribers_list_callback(call: types.CallbackQuery):
            await self.show_subscribers_list(call)

        @self.dp.callback_query_handler(lambda call: call.data == 'export_to_excel')
        async def handle_export_to_excel_callback(call: types.CallbackQuery):
            await self.handle_export_to_excel(call)

        @self.dp.callback_query_handler(lambda call: call.data == 'add_lead_magnet')
        async def handle_add_lead_magnet_callback(call: types.CallbackQuery):
            await self.bot.send_message(call.message.chat.id, "Введите название лид-магнита:")
            await Form.adding_lead_magnet_name.set()

        @self.dp.message_handler(state=Form.adding_lead_magnet_name)
        async def handle_lead_magnet_name(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['name'] = message.text
            await self.bot.send_message(message.chat.id, "Введите описание лид-магнита:")
            await Form.adding_lead_magnet_description.set()

        @self.dp.message_handler(state=Form.adding_lead_magnet_description)
        async def handle_lead_magnet_description(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['description'] = message.text
            await self.bot.send_message(message.chat.id, "Отправьте изображение для лид-магнита или введите 'нет', если изображение не требуется:")
            await Form.adding_lead_magnet_image.set()

        @self.dp.message_handler(state=Form.adding_lead_magnet_image, content_types=types.ContentType.ANY)
        async def handle_lead_magnet_image(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                if message.content_type == 'photo':
                    data['image_id'] = message.photo[-1].file_id
                elif message.text.lower() == 'нет':
                    data['image_id'] = None
                else:
                    await self.bot.send_message(message.chat.id, "Пожалуйста, отправьте изображение или введите 'нет'.")
                    return

                with Database() as db:
                    db.insert_lead_magnet(data['name'], data['description'], data['image_id'])

                await self.bot.send_message(message.chat.id, "Лид-магнит успешно добавлен.")
                await state.finish()

        @self.dp.callback_query_handler(lambda call: call.data == 'list_lead_magnets')
        async def handle_list_lead_magnets_callback(call: types.CallbackQuery):
            await self.list_lead_magnets(call)

        @self.dp.callback_query_handler(lambda call: call.data.startswith('select_lead_magnet_'))
        async def handle_select_lead_magnet_callback(call: types.CallbackQuery):
            await self.process_lead_magnet_selection(call)

        @self.dp.callback_query_handler(lambda call: call.data == 'get_gift')
        async def handle_get_gift_callback(call: types.CallbackQuery):
            logging.info(f"Calling handle_get_gift_callback with user_data: {call.from_user.id}")
            await self.handle_gift_reception(call)

        @self.dp.callback_query_handler(lambda call: call.data == 'set_subscribers_for_gift')
        async def set_subscribers_for_gift(call: types.CallbackQuery):
            await self.bot.send_message(call.message.chat.id, "Введите количество подписчиков для получения подарка:")
            await Form.setting_subscribers.set()

        @self.dp.message_handler(state=Form.setting_subscribers)
        async def process_set_subscribers_for_gift(message: types.Message, state: FSMContext):
            chat_id = message.chat.id
            try:
                count = int(message.text)
                with Database() as db:
                    db.set_config_value('required_referrals', count)
                await self.bot.send_message(chat_id, f"Количество подписчиков для получения подарка установлено на {count}.")
                await state.finish()
            except ValueError:
                await self.bot.send_message(chat_id, "Пожалуйста, введите корректное число.")

        @self.dp.callback_query_handler(lambda call: call.data == 'message_manager')
        async def message_manager(call: types.CallbackQuery):
            with Database() as db:
                user = db.get_user_by_telegram_id(call.from_user.id)
            if not user:
                logging.error(f"User data is empty for chat_id: {call.message.chat.id}")
                return

            buttons = [
                {'text': 'Приветственное сообщение', 'callback_data': 'edit_template_welcome_template'},
                {'text': 'Новенький', 'callback_data': 'edit_template_newbie'},
                {'text': 'Активный рефовод', 'callback_data': 'edit_template_active_referral'},
                {'text': 'Получил подарок', 'callback_data': 'edit_template_received_gift'},
                {'text': 'Новенький, до подарка 1 реф', 'callback_data': 'edit_template_one_ref_to_gift'},
                {'text': 'Добавить шаблон', 'callback_data': 'add_template'},
                {'text': 'Назад', 'callback_data': 'admin_panel'}
            ]
            logging.info(f"Calling send_message_with_buttons from message_manager with user_data: {user}")
            await self.send_message_with_buttons(call.message.chat.id, "Менеджер сообщений", None, buttons, user)

        @self.dp.callback_query_handler(lambda call: call.data.startswith('edit_template_'))
        async def edit_template(call: types.CallbackQuery, state: FSMContext):
            template_type = call.data[len('edit_template_'):]
            with Database() as db:
                template = db.get_template_by_type(template_type)
            if not template:
                await self.bot.send_message(call.message.chat.id, f"Шаблон '{template_type}' не найден.")
                return
            await self.bot.send_message(call.message.chat.id, f"Текущий текст шаблона:\n\n{template['content']}\n\nВведите новый текст для шаблона '{template_type}':")
            await Form.editing_template.set()
            async with state.proxy() as data:
                data['template_id'] = template['template_id']
                data['template_type'] = template_type

        @self.dp.message_handler(state=Form.editing_template)
        async def process_edit_template_content(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                chat_id = message.chat.id
                new_content = message.text
                template_id = data['template_id']
                template_type = data['template_type']
                logging.info(f"Received new content for template {template_id}: {new_content}")
                await self.bot.send_message(chat_id, f"Отправьте новый мультимедийный файл для шаблона '{template_type}' или введите 'нет', если не требуется:")
                async with state.proxy() as data:
                    data['new_content'] = new_content
                await state.set_state(Form.editing_template.state + "_file")

        @self.dp.message_handler(state=Form.editing_template.state + "_file", content_types=types.ContentType.ANY)
        async def process_edit_template_file(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                chat_id = message.chat.id
                template_id = data['template_id']
                new_content = data['new_content']
                template_type = data['template_type']
                if message.content_type in ['photo', 'video', 'audio']:
                    if message.content_type == 'photo':
                        media_id = message.photo[-1].file_id
                    elif message.content_type == 'video':
                        media_id = message.video.file_id
                    elif message.content_type == 'audio':
                        media_id = message.audio.file_id
                else:
                    media_id = None if message.text.lower() == 'нет' else message.text
                logging.info(f"Updating template {template_id} with new content: {new_content} and media_id: {media_id}")
                with Database() as db:
                    db.update_template(template_id, new_content, media_id)
                await self.bot.send_message(chat_id, f"Шаблон '{template_type}' успешно обновлен.")
                await state.finish()

        @self.dp.callback_query_handler(lambda call: call.data == 'add_template')
        async def add_template(call: types.CallbackQuery):
            await self.bot.send_message(call.message.chat.id, "Введите тип нового шаблона:")
            await Form.adding_template.set()

        @self.dp.message_handler(state=Form.adding_template)
        async def process_add_template(message: types.Message, state: FSMContext):
            chat_id = message.chat.id
            template_type = message.text
            logging.info(f"Adding template with type: {template_type}")
            await self.bot.send_message(chat_id, f"Введите текст для нового шаблона '{template_type}':")
            async with state.proxy() as data:
                data['template_type'] = template_type
            await Form.adding_template_content.set()

        @self.dp.message_handler(state=Form.adding_template_content)
        async def process_add_template_content(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                chat_id = message.chat.id
                template_type = data['template_type']
                content = message.text
                logging.info(f"Adding template with type: {template_type} and content: {content}")
                await self.bot.send_message(chat_id, f"Отправьте мультимедийный файл для нового шаблона '{template_type}' или введите 'нет', если не требуется:")
                async with state.proxy() as data:
                    data['content'] = content
                await Form.adding_template_file.set()

        @self.dp.message_handler(state=Form.adding_template_file, content_types=types.ContentType.ANY)
        async def process_add_template_file(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                chat_id = message.chat.id
                template_type = data['template_type']
                content = data['content']
                if message.content_type in ['photo', 'video', 'audio']:
                    if message.content_type == 'photo':
                        media_id = message.photo[-1].file_id
                    elif message.content_type == 'video':
                        media_id = message.video.file_id
                    elif message.content_type == 'audio':
                        media_id = message.audio.file_id
                else:
                    media_id = None if message.text.lower() == 'нет' else message.text
                logging.info(f"Adding new template {template_type} with content: {content} and media_id: {media_id}")
                with Database() as db:
                    db.insert_template(template_type, content, media_id)
                await self.bot.send_message(chat_id, f"Новый шаблон '{template_type}' успешно добавлен.")
                await state.finish()

        @self.dp.callback_query_handler(lambda call: call.data == 'show_subscribers_list')
        async def show_subscribers_list(call: types.CallbackQuery):
            chat_id = call.message.chat.id
            with Database() as db:
                subscribers = db.get_all_subscribers()
            if not subscribers:
                await self.bot.send_message(chat_id, "Список подписчиков пуст.")
                return

            subscriber_list = "\n".join([f"ID: {sub['user_id']}\nUsername: {sub['username']}\nName: {sub['first_name']} {sub['last_name']}\nJoined at: {sub['joined_at']}\nTags: {sub['tags']}" for sub in subscribers])
            await self.bot.send_message(chat_id, f"Список подписчиков:\n\n{subscriber_list}")

        @self.dp.callback_query_handler(lambda call: call.data == 'instant_broadcast')
        async def instant_broadcast(call: types.CallbackQuery):
            with Database() as db:
                tags = db.get_all_tags()
            buttons = [{'text': tag, 'callback_data': f"instant_broadcast_{tag}"} for tag in tags]
            await self.send_message_with_buttons(call.message.chat.id, "Выберите тег для рассылки:", None, buttons, {})

        @self.dp.callback_query_handler(lambda call: call.data.startswith('instant_broadcast_') and len(call.data.split('_')) == 3)
        async def handle_instant_broadcast_tag_callback(call: types.CallbackQuery):
            tag = call.data.split('_')[2]
            with Database() as db:
                templates = db.get_all_templates()
            buttons = [{'text': tmpl['template_type'], 'callback_data': f"instant_broadcast_{tag}_{tmpl['template_type']}"} for tmpl in templates]
            await self.send_message_with_buttons(call.message.chat.id, "Выберите шаблон для рассылки:", None, buttons, {})

        @self.dp.callback_query_handler(lambda call: call.data.startswith('instant_broadcast_') and len(call.data.split('_')) == 4)
        async def handle_instant_broadcast_template_callback(call: types.CallbackQuery):
            try:
                _, tag, template_type = call.data.split('_')[1:]
            except ValueError:
                logging.error(f"Failed to unpack call.data: {call.data}")
                return
            await self.broadcast_message_by_tag(tag, template_type)
            await self.bot.send_message(call.message.chat.id, "Мгновенная рассылка выполнена.")

        @self.dp.callback_query_handler(lambda call: call.data == 'scheduled_broadcast')
        async def scheduled_broadcast(call: types.CallbackQuery):
            with Database() as db:
                tags = db.get_all_tags()
            buttons = [{'text': tag, 'callback_data': f"scheduled_broadcast_{tag}"} for tag in tags]
            await self.send_message_with_buttons(call.message.chat.id, "Выберите тег для рассылки:", None, buttons, {})

        @self.dp.callback_query_handler(lambda call: call.data.startswith('scheduled_broadcast_') and len(call.data.split('_')) == 3)
        async def handle_scheduled_broadcast_tag_callback(call: types.CallbackQuery):
            tag = call.data.split('_')[2]
            with Database() as db:
                templates = db.get_all_templates()
            buttons = [{'text': tmpl['template_type'], 'callback_data': f"scheduled_broadcast_{tag}_{tmpl['template_type']}"} for tmpl in templates]
            await self.send_message_with_buttons(call.message.chat.id, "Выберите шаблон для рассылки:", None, buttons, {})

        @self.dp.callback_query_handler(lambda call: call.data.startswith('scheduled_broadcast_') and len(call.data.split('_')) == 4)
        async def handle_scheduled_broadcast_template_callback(call: types.CallbackQuery):
            try:
                _, tag, template_type = call.data.split('_')[1:]
            except ValueError:
                logging.error(f"Failed to unpack call.data: {call.data}")
                return
            await self.schedule_broadcast(call.message.chat.id, tag, template_type)

        @self.dp.message_handler(state=Form.scheduling_broadcast)
        async def process_scheduled_broadcast_time(message: types.Message, state: FSMContext):
            chat_id = message.chat.id
            async with state.proxy() as data:
                tag = data['tag']
                template_type = data['template_type']
                scheduled_time = message.text
                with Database() as db:
                    template = db.get_template_by_type(template_type)
                    broadcast_data = {
                        'template_id': template['template_id'],
                        'tag': tag,
                        'scheduled_time': scheduled_time,
                        'status': 'scheduled'
                    }
                    db.insert_scheduled_broadcast(broadcast_data)
                await self.bot.send_message(chat_id, "Отложенная рассылка запланирована.")
                await state.finish()

        @self.dp.callback_query_handler(lambda call: call.data == 'manage_autoresponders')
        async def manage_autoresponders(call: types.CallbackQuery):
            chat_id = call.message.chat.id
            buttons = [
                {'text': 'Добавить автосообщение', 'callback_data': 'add_autoresponder'},
                {'text': 'Список автосообщений', 'callback_data': 'list_autoresponders'},
                {'text': 'Назад', 'callback_data': 'admin_panel'}
            ]
            await self.send_message_with_buttons(chat_id, "Менеджер автосообщений:", None, buttons, {})

        @self.dp.callback_query_handler(lambda call: call.data == 'add_autoresponder')
        async def add_autoresponder(call: types.CallbackQuery):
            chat_id = call.message.chat.id
            await self.bot.send_message(chat_id, "Введите текст автосообщения:")
            await Form.adding_autoresponder.set()

        @self.dp.message_handler(state=Form.adding_autoresponder)
        async def process_add_autoresponder(message: types.Message, state: FSMContext):
            chat_id = message.chat.id
            content = message.text
            await self.bot.send_message(chat_id, "Введите задержку (в часах) перед отправкой этого сообщения:")
            async with state.proxy() as data:
                data['content'] = content
            await Form.adding_autoresponder_delay.set()

        @self.dp.message_handler(state=Form.adding_autoresponder_delay)
        async def process_add_autoresponder_media(message: types.Message, state: FSMContext):
            chat_id = message.chat.id
            media_id = None
            file_size = 0
            if message.content_type in ['photo', 'video', 'audio']:
                if message.content_type == 'photo':
                    media_id = message.photo[-1].file_id
                    file = await self.bot.get_file(media_id)
                    file_size = file.file_size
                elif message.content_type == 'video':
                    media_id = message.video.file_id
                    file = await self.bot.get_file(media_id)
                    file_size = file.file_size
                elif message.content_type == 'audio':
                    media_id = message.audio.file_id
                    file = await self.bot.get_file(media_id)
                    file_size = file.file_size
            elif message.text.lower() != 'нет':
                media_id = message.text

            with Database() as db:
                user = db.get_user_by_telegram_id(chat_id)
                total_size = db.get_user_storage_size(user['user_id']) + file_size
                if total_size > 500 * 1024 * 1024:  # 500 MB
                    await self.bot.send_message(chat_id, "Общий объем данных превышает 500 МБ. Невозможно загрузить файл.")
                    return
                db.add_user_file(user['user_id'], file_size)

            async with state.proxy() as data:
                content = data['content']
                delay = data['delay']
                with Database() as db:
                    db.insert_autoresponder(content, delay, media_id)
                await self.bot.send_message(chat_id, "Автосообщение успешно добавлено.")
                await state.finish()

        @self.dp.callback_query_handler(lambda call: call.data == 'list_autoresponders')
        async def list_autoresponders(call: types.CallbackQuery):
            chat_id = call.message.chat.id
            with Database() as db:
                autoresponders = db.get_all_autoresponders()
            if not autoresponders:
                await self.bot.send_message(chat_id, "Список автосообщений пуст.")
                return

            autoresponder_list = "\n".join([f"{ar['id']}: {ar['content']} (Задержка: {ar['delay']} часов, Медиа: {ar['media_id']})" for ar in autoresponders])
            await self.bot.send_message(chat_id, f"Список автосообщений:\n\n{autoresponder_list}")

        @self.dp.callback_query_handler(lambda call: call.data.startswith('select_lead_magnet_'))
        async def handle_select_lead_magnet(call: types.CallbackQuery):
            try:
                lead_magnet_id = int(call.data.split('_')[-1])
                telegram_id = call.message.chat.id
                with Database() as db:
                    user = db.get_user_by_telegram_id(telegram_id)
                    logging.info(f"Calling send_message_with_buttons from handle_select_lead_magnet with user_data: {user}")
                    db.update_user_lead_magnet(telegram_id, lead_magnet_id)
                    lead_magnet = db.get_lead_magnet(lead_magnet_id)
                await self.bot.send_message(telegram_id, f"Вы выбрали лид-магнит: {lead_magnet['name']}. Спасибо!")
            except ValueError as e:
                logging.error(f"Failed to convert lead_magnet_id to int: {e}")
                await self.bot.send_message(call.message.chat.id, "Произошла ошибка при выборе лид-магнита. Попробуйте снова.")

    async def check_scheduled_broadcasts(self):
        while True:
            now = datetime.now()
            with Database() as db:
                broadcasts = db.get_scheduled_broadcasts()
            for broadcast in broadcasts:
                with Database() as db:
                    subscribers = db.get_subscribers_by_tag(broadcast['tag'])
                for sub in subscribers:
                    user_data = db.get_user_by_telegram_id(sub['telegram_id'])
                    await self.send_message(sub['telegram_id'], broadcast['content'], broadcast['media_id'], user_data)
                with Database() as db:
                    db.update_broadcast_status(broadcast['broadcast_id'], 'sent')
            await asyncio.sleep(60)  # Проверяем каждые 60 секунд

    async def check_autoresponders(self):
        while True:
            now = datetime.now()
            with Database() as db:
                autoresponders = db.get_all_autoresponders()
                users = db.get_all_subscribers()
            for user in users:
                joined_at = user['joined_at']
                for autoresponder in autoresponders:
                    send_time = joined_at + timedelta(hours=autoresponder['delay'])
                    if now >= send_time and not user['received_gift']:
                        if not db.is_autoresponder_sent(user['user_id'], autoresponder['id']):
                            await self.send_message(user['telegram_id'], autoresponder['content'], autoresponder['media_id'], user)
                            db.mark_autoresponder_as_sent(user['user_id'], autoresponder['id'])
                # Проверяем количество рефералов и отправляем подарок
                required_referrals = db.get_config_value('required_referrals')
                if user['referrals'] >= required_referrals and not user['received_gift']:
                    logging.info(f"User {user['telegram_id']} has {user['referrals']} referrals, required: {required_referrals}")
                    lead_magnet = db.get_lead_magnet(user['lead_magnet_id'])
                    if lead_magnet:
                        await self.send_message(
                            user['telegram_id'],
                            f"Поздравляем! Вы получили подарок: {lead_magnet['name']}\n\nОписание: {lead_magnet['description']}",
                            lead_magnet['image_id'],
                            user
                        )
                        db.update_user_received_gift(user['user_id'])
                    else:
                        logging.error(f"Lead magnet with ID {user['lead_magnet_id']} not found for user {user['telegram_id']}")
            await asyncio.sleep(3600)  # Проверяем каждый час

    async def send_message(self, chat_id, text, media_id=None, user_data=None):
        text = self.substitute_variables(text, user_data)
        if media_id:
            file = await self.bot.get_file(media_id)
            file_size = file.file_size
            if file_size > 50 * 1024 * 1024:  # 50 MB
                await self.bot.send_message(chat_id, "Размер файла превышает 50 МБ и не может быть отправлен.")
                return
            await self.bot.send_photo(chat_id, media_id, caption=text)
        else:
            await self.bot.send_message(chat_id, text)

    async def backup_database(self):
        backup_file = f"backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.sql"
        backup_path = os.path.join("/path/to/backup/folder", backup_file)
        try:
            subprocess.run(
                [
                    "mysqldump",
                    "-h", os.getenv('DB_HOST'),
                    "-u", os.getenv('DB_USER'),
                    f"-p{os.getenv('DB_PASSWORD')}",
                    os.getenv('DB_NAME'),
                    "--result-file", backup_path
                ],
                check=True
            )
            # Здесь добавьте код для копирования файла на удаленный сервер
            logging.info(f"Backup successful: {backup_path}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Backup failed: {e}")
    
    def restore_database(self, backup_file):
        backup_path = os.path.join("/path/to/backup/folder", backup_file)
        try:
            subprocess.run(
                [
                    "mysql",
                    "-h", os.getenv('DB_HOST'),
                    "-u", os.getenv('DB_USER'),
                    f"-p{os.getenv('DB_PASSWORD')}",
                    os.getenv('DB_NAME'),
                    "-e", f"source {backup_path}"
                ],
                check=True
            )
            logging.info(f"Restore successful: {backup_file}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Restore failed: {e}")
    
    def setup_scheduled_tasks(self):
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self.backup_database, 'cron', hour=2)  # Backup every day at 2 AM
        scheduler.start()

class Form(StatesGroup):
    editing_template = State()
    setting_subscribers = State()
    adding_template = State()
    adding_template_content = State()
    adding_template_file = State()
    scheduling_broadcast = State()
    adding_autoresponder = State()
    adding_autoresponder_delay = State()
    adding_autoresponder_media = State()
    adding_lead_magnet_name = State()
    adding_lead_magnet_description = State()
    adding_lead_magnet_image = State()

if __name__ == '__main__':
    API_TOKEN = os.getenv('API_TOKEN')
    bot = TelegramBot(API_TOKEN)
    loop = asyncio.get_event_loop()
    loop.create_task(bot.check_scheduled_broadcasts())
    loop.create_task(bot.check_autoresponders())
    executor.start_polling(bot.dp, skip_updates=True)
