import os
import re
import tempfile
import redis
import time

import requests
from django.utils import timezone
import vk_api
import gspread
from django.db.models import F
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime
import pytz

import logging

from main.models import VKGroup, UserToken, ParsingSettings

# Подключение к Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_FILE = 'parser34-9b03b5da1013.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Авторизация
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)

# Устанавливаем нужный часовой пояс
local_tz = pytz.timezone('Europe/Moscow')


def get_google_sheet(sheet_name):
    settings = ParsingSettings.objects.first()  # Получаем настройки, если их несколько, нужно изменить логику
    if not settings:
        raise ValueError("Настройки парсинга не найдены.")

    file_path = settings.get_google_sheet_credentials()
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(file_path, scope)
    client = gspread.authorize(creds)

    try:
        spreadsheet = client.open(sheet_name)
        logger.info(f"Найден существующий файл '{sheet_name}'")
        logger.info(f"URL файла: {spreadsheet.url}")
        return spreadsheet
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Файл '{sheet_name}' не найден. Убедитесь, что файл существует.")
        return None
    except Exception as e:
        print(f"Ошибка при доступе к Google Sheets: {e}")
        raise


def get_vk_session(token):
    vk_session = vk_api.VkApi(token=token)
    return vk_session.get_api()


def save_to_google_sheet_worksheet(worksheet, data):
    try:
        result = worksheet.append_row(data)
        print(f"Data successfully appended to Google Sheet: {data}")
        print(f"Google Sheets API response: {result}")
    except Exception as e:
        print(f"Error while saving to Google Sheets: {e}")


def filter_text(text, key_words, stop_words):
    """
    Фильтрует текст на основе ключевых слов и стоп-слов.
    Возвращает найденные ключевые слова и стоп-слова.
    """
    found_key_words = [kw for kw in key_words if kw.lower() in text.lower()]
    found_stop_words = [sw for sw in stop_words if sw.lower() in text.lower()]
    logger.debug(f"Filtered Key Words: {found_key_words}")
    logger.debug(f"Filtered Stop Words: {found_stop_words}")
    return found_key_words, found_stop_words


def is_id_in_redis(post_id):
    return redis_client.sismember('parsed_ids', post_id)


def add_id_to_redis(post_id):
    redis_client.sadd('parsed_ids', post_id)


def clean_text(text):
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text


def truncate_keywords(keywords):
    return [kw if len(kw) <= 8 else kw[:-2] for kw in keywords]


def create_record_key(item):
    return f"{item.get('id')}_{item.get('owner_id')}_{item.get('date')}"


def save_data_to_google_sheet(vk, table_name, sheet_name, data_type, data, group_id, key_words=None, stop_words=None):
    try:
        settings = ParsingSettings.objects.first()
        if not settings:
            logger.error("Настройки не найдены.")
            return

        google_sheet_file = settings.google_sheet_file
        if not google_sheet_file:
            logger.error("Файл авторизации не найден.")
            return

        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp_file:
            temp_file.write(google_sheet_file.read())
            temp_file_path = temp_file.name

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(temp_file_path, scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open(table_name)
        existing_worksheets = {ws.title: ws for ws in spreadsheet.worksheets()}

        worksheet1 = existing_worksheets.get(sheet_name) or spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        worksheet2 = existing_worksheets.get('Лист2') or spreadsheet.add_worksheet(title='Лист2', rows="100", cols="20")

        headers = [
            'Дата и время выгрузки',
            'Дата публикации',
            'Тип контента',
            'Текст сообщения',
            'Ссылка на источник',
            'Ссылка на профиль пользователя',
            'Город'
        ]
        if data_type == 'Post':
            headers.extend(['Название группы', 'Описание группы'])

        headers_for_sheet2 = headers + ['Ключевые слова', 'Стоп-слова']

        if not worksheet1.row_values(1):
            worksheet1.append_row(headers)
        if key_words and stop_words and not worksheet2.row_values(1):
            worksheet2.append_row(headers_for_sheet2)

        group_info = vk.groups.getById(group_id=group_id, fields=['description', 'city'])[0]
        group_name = group_info.get('name', 'Неизвестно')
        group_description = group_info.get('description', 'Неизвестно')
        group_city = group_info.get('city', {}).get('title', 'Город группы неизвестен')

        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        rows_for_sheet1 = []
        rows_for_sheet2 = []

        if data:
            for chunk in chunks(data, 20):  # Отправляем данные партиями по 20 элементов
                for item in chunk:
                    post_id = f"{item['owner_id']}_{item['id']}"
                    if is_id_in_redis(post_id):
                        logger.info(f"ID {post_id} уже существует в Redis. Пропускаем.")
                        continue

                    text = clean_text(item.get('text', ''))
                    found_key_words, found_stop_words = filter_text(text, key_words or [], stop_words or [])

                    filtered_key_words = ', '.join(found_key_words) if found_key_words else ' '
                    filtered_stop_words = ', '.join(found_stop_words) if found_stop_words else ' '

                    user_city = 'Город неизвестен'
                    if item.get('from_id') and item['from_id'] > 0:
                        user_info = vk.users.get(user_ids=item['from_id'], fields=['city'])
                        if user_info:
                            user_city = user_info[0].get('city', {}).get('title', 'Город неизвестен')
                        profile_link = f"https://vk.com/id{item['from_id']}"
                    else:
                        profile_link = f"https://vk.com/club{abs(item['owner_id'])}"

                    post_date = datetime.fromtimestamp(item['date'], pytz.utc).astimezone(local_tz)
                    formatted_post_date = post_date.strftime('%Y-%m-%d %H:%M:%S')

                    row_for_sheet1 = [
                        timezone.now().strftime('%Y-%m-%d %H:%M:%S'),
                        formatted_post_date,
                        'Пост' if data_type == 'Post' else 'Комментарий',
                        text,
                        f"https://vk.com/wall{item['owner_id']}_{item['id']}" if data_type == 'Post' else f"https://vk.com/wall{item['owner_id']}_{item.get('post_id', '')}",
                        profile_link,
                        user_city if data_type == 'Comment' else group_city
                    ]

                    if data_type == 'Post':
                        row_for_sheet1.extend([group_name, group_description])

                    rows_for_sheet1.append(row_for_sheet1)
                    if key_words and stop_words:
                        row_for_sheet2 = row_for_sheet1 + [filtered_key_words, filtered_stop_words]
                        if filtered_key_words != ' ' or filtered_stop_words != ' ':
                            rows_for_sheet2.append(row_for_sheet2)

                    add_id_to_redis(post_id)  # Добавляем ID в Redis

                if rows_for_sheet1:
                    logger.info(f"Добавляется {len(rows_for_sheet1)} строк(и) в лист '{sheet_name}'.")
                    worksheet1.append_rows(rows_for_sheet1, value_input_option='USER_ENTERED')
                    rows_for_sheet1 = []

                if key_words and stop_words and rows_for_sheet2:
                    logger.info(f"Добавляется {len(rows_for_sheet2)} строк(и) в 'Лист2'.")
                    worksheet2.append_rows(rows_for_sheet2, value_input_option='USER_ENTERED')
                    rows_for_sheet2 = []

                time.sleep(1)  # Добавление задержки между пакетами запросов

        logger.info(f"Данные успешно сохранены в лист '{sheet_name}' и 'Лист2' таблицы '{table_name}'.")

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Файл '{table_name}' не найден. Убедитесь, что файл существует.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в Google Sheets: {e}")
    finally:
        os.remove(temp_file_path)


def log_parsing_action(action):
    logger.info(action)


def get_group_id_by_domain(domain):
    try:
        token = UserToken.objects.filter(requests_used__lt=F('daily_limit')).order_by('requests_used').first()
        if not token:
            print("Нет доступных токенов.")
            return None

        url = "https://api.vk.com/method/groups.getById"
        params = {
            'group_id': domain,
            'access_token': token.access_token,
            'v': '5.131',
        }
        response = requests.get(url, params=params).json()

        token.requests_used += 1
        token.save(update_fields=['requests_used', 'last_used'])

        print(f"Ответ от VK API: {response}")
        if 'response' in response and len(response['response']) > 0:
            return response['response'][0]['id']
        else:
            print(f"Ошибка VK API: {response.get('error', 'Неизвестная ошибка')}")
        return None
    except Exception as e:
        print(f"Ошибка при получении group_id: {e}")
        return None


def get_user_token():
    tokens = UserToken.objects.filter(requests_used__lt=F('daily_limit'))
    if tokens:
        return tokens.first()  # Выбираем первый токен с доступным лимитом
    else:
        raise Exception("Нет доступных токенов с квотой")
