import os
import re
import tempfile

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
import redis
import time

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
MAX_REQUESTS_PER_MINUTE = 250  # Максимальное количество запросов в минуту
request_count = 0  # Счетчик запросов
start_time = time.time()  # Время начала отсчета запросов


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


def is_id_in_redis(post_id, sheet):
    """Checks if the post ID exists in Redis for the specific sheet (first or second)."""
    return redis_client.exists(f"{sheet}:{post_id}")


def add_id_to_redis(post_id, sheet, ttl=7776000):
    """Adds the post ID to Redis for the specific sheet with a TTL."""
    redis_client.setex(f"{sheet}:{post_id}", ttl, post_id)


def clean_text(text):
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text


def truncate_keywords(keywords):
    return [kw if len(kw) <= 8 else kw[:-2] for kw in keywords]


def create_record_key(item):
    return f"{item.get('id')}_{item.get('owner_id')}_{item.get('date')}"


def save_to_google_sheet(vk, table_name, sheet_name, data_type, data, group_id, key_words, stop_words):
    global request_count, start_time  # Используем глобальные переменные для отслеживания количества запросов

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

        try:
            worksheet2 = spreadsheet.worksheet('Лист2')
        except gspread.exceptions.WorksheetNotFound:
            worksheet2 = spreadsheet.add_worksheet(title='Лист2', rows="100", cols="20")
            request_count += 1  # Увеличиваем счетчик запросов при создании листа

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

        # Проверяем, добавлены ли заголовки на Лист2
        if not worksheet2.row_values(1):
            worksheet2.append_row(headers_for_sheet2)
            request_count += 1  # Увеличиваем счетчик запросов

        rows_with_keywords = []
        rows_with_keywords_and_stopwords = []
        rows_with_stopwords = []

        group_info = vk.groups.getById(group_id=group_id, fields=['description', 'city'])[0]
        group_name = group_info.get('name', 'Неизвестно')
        group_description = group_info.get('description', 'Неизвестно')
        group_city = group_info.get('city', {}).get('title', 'Город группы неизвестен')

        if data:
            for item in data:
                post_id = f"{item['owner_id']}_{item['id']}"  # Образование уникального ID поста
                if is_id_in_redis(post_id, 'sheet2'):
                    logger.info(f"ID {post_id} уже существует в Redis. Пропускаем.")
                    continue

                text = clean_text(item.get('text', ''))
                found_key_words, found_stop_words = filter_text(text, key_words, stop_words)

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

                row = [
                    timezone.now().strftime('%Y-%m-%d %H:%M:%S'),
                    formatted_post_date,
                    'Пост' if data_type == 'Post' else 'Комментарий',
                    text,
                    f"https://vk.com/wall{item['owner_id']}_{item['id']}" if data_type == 'Post' else f"https://vk.com/wall{item['owner_id']}_{item.get('post_id', '')}",
                    profile_link,
                    user_city if data_type == 'Comment' else group_city
                ]

                if data_type == 'Post':
                    row.extend([group_name, group_description])
                else:
                    row.extend(['', ''])

                row_for_sheet2 = row + [filtered_key_words, filtered_stop_words]

                if filtered_key_words != ' ':
                    if filtered_stop_words != ' ':
                        rows_with_keywords_and_stopwords.append(row_for_sheet2)
                    else:
                        rows_with_keywords.append(row_for_sheet2)

                add_id_to_redis(post_id, 'sheet2')  # Добавляем ID в Redis

            logger.info(f"Добавляется {len(rows_with_keywords)} строк(и) с ключевыми словами.")
            logger.info(
                f"Добавляется {len(rows_with_keywords_and_stopwords)} строк(и) с ключевыми словами и стоп-словами.")

            if rows_with_keywords or rows_with_keywords_and_stopwords or rows_with_stopwords:
                rows_to_add = rows_with_keywords + rows_with_keywords_and_stopwords + rows_with_stopwords

                # Если количество запросов превышает лимит, приостанавливаем выполнение
                if request_count + len(rows_to_add) >= MAX_REQUESTS_PER_MINUTE:
                    elapsed_time = time.time() - start_time
                    if elapsed_time < 60:
                        pause_time = 60 - elapsed_time
                        logger.info(f"Достигнут лимит запросов. Пауза на {pause_time} секунд.")
                        time.sleep(pause_time)
                    request_count = 0
                    start_time = time.time()

                # Используем batch update для добавления всех строк одновременно
                worksheet2.append_rows(rows_to_add, value_input_option='USER_ENTERED')
                request_count += len(rows_to_add)

        logger.info(f"Данные успешно сохранены в лист '{sheet_name}' таблицы '{table_name}'.")

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Файл '{table_name}' не найден. Убедитесь, что файл существует.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в Google Sheets: {e}")
    finally:
        os.remove(temp_file_path)


def save_all_posts_to_first_sheet(vk, table_name, sheet_name, data_type, data, group_id):
    global request_count, start_time  # Используем глобальные переменные для отслеживания количества запросов

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

        try:
            worksheet1 = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet1 = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
            request_count += 1  # Увеличиваем счетчик запросов при создании листа

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

        if not worksheet1.row_values(1):
            worksheet1.append_row(headers)
            request_count += 1  # Увеличиваем счетчик запросов при добавлении заголовков

        rows_for_sheet1 = []

        group_info = vk.groups.getById(group_id=group_id, fields=['description', 'city'])[0]
        group_name = group_info.get('name', 'Неизвестно')
        group_description = group_info.get('description', 'Описание недоступно')
        group_city = group_info.get('city', {}).get('title', 'Город группы неизвестен')

        if data:
            for item in data:
                post_id = f"{item['owner_id']}_{item['id']}"  # Образование уникального ID поста
                if is_id_in_redis(post_id, 'sheet1'):
                    logger.info(f"ID {post_id} уже существует в Redis. Пропускаем.")
                    continue

                post_date = datetime.fromtimestamp(item['date'], pytz.utc).astimezone(local_tz)
                text = clean_text(item.get('text', ''))
                user_city = 'Город неизвестен'
                formatted_post_date2 = post_date.strftime('%Y-%m-%d %H:%M:%S')
                if item.get('from_id') and item['from_id'] > 0:
                    user_info = vk.users.get(user_ids=item['from_id'], fields=['city'])
                    if user_info:
                        user_city = user_info[0].get('city', {}).get('title', 'Город неизвестен')
                    profile_link = f"https://vk.com/id{item['from_id']}"
                else:
                    profile_link = f"https://vk.com/club{abs(item['owner_id'])}"

                row_for_sheet1 = [
                    timezone.now().strftime('%Y-%m-%d %H:%M:%S'),
                    formatted_post_date2,
                    'Пост' if data_type == 'Post' else 'Комментарий',
                    text,
                    f"https://vk.com/wall{item['owner_id']}_{item['id']}" if data_type == 'Post' else f"https://vk.com/wall{item['owner_id']}_{item.get('post_id', '')}",
                    profile_link,
                    user_city if data_type == 'Comment' else group_city
                ]

                if data_type == 'Post':
                    row_for_sheet1.extend([group_name, group_description])

                rows_for_sheet1.append(row_for_sheet1)
                add_id_to_redis(post_id, 'sheet1')  # Добавляем ID в Redis

                request_count += 1  # Увеличиваем счетчик запросов
                if request_count >= MAX_REQUESTS_PER_MINUTE:
                    elapsed_time = time.time() - start_time
                    if elapsed_time < 60:
                        pause_time = 60 - elapsed_time
                        logger.info(f"Достигнут лимит запросов ({MAX_REQUESTS_PER_MINUTE} в минуту). Пауза на {pause_time} секунд.")
                        time.sleep(pause_time)  # Пауза для сброса лимита
                    request_count = 0  # Сбрасываем счетчик
                    start_time = time.time()  # Обновляем время старта

            logger.info(f"Добавляется {len(rows_for_sheet1)} строк(и) в лист '{sheet_name}'.")

            if rows_for_sheet1:
                worksheet1.append_rows(rows_for_sheet1, value_input_option='USER_ENTERED')

        logger.info(f"Данные успешно сохранены в лист '{sheet_name}' таблицы '{table_name}'.")

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
    tokens = UserToken.objects.filter(requests_used__lt=F('daily_limit')).order_by('last_used')

    if tokens.exists():
        token = tokens.first()  # Выбираем первый токен с доступным лимитом
        token.requests_used = F('requests_used') + 1  # Увеличиваем количество использованных запросов
        token.last_used = timezone.now()  # Обновляем время последнего использования
        token.save(update_fields=['requests_used', 'last_used'])  # Сохраняем изменения
        return token
    else:
        raise Exception("Нет доступных токенов с квотой")
