import os
import re
import tempfile

import requests
from datetime import datetime
from django.utils import timezone
import vk_api
import gspread
from django.db.models import F
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from gspread.exceptions import SpreadsheetNotFound

import logging

from main.models import VKGroup, UserToken, ParsingSettings

logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_FILE = 'peaceful-bruin-432623-t7-cd1c10956dc7.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Авторизация
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)


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
    key_words = truncate_keywords(key_words)
    key_words_regex = '|'.join(re.escape(word) for word in key_words)
    stop_words_regex = '|'.join(re.escape(word) for word in stop_words)

    has_key_words = bool(re.search(key_words_regex, text))
    has_stop_words = bool(re.search(stop_words_regex, text))

    if has_key_words and has_stop_words:
        return 'key_and_stop'
    elif has_key_words:
        return 'key_only'
    elif has_stop_words:
        return 'stop_only'
    return 'none'


def clean_text(text):
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text


def truncate_keywords(keywords):
    return [kw if len(kw) <= 8 else kw[:-2] for kw in keywords]


def save_to_google_sheet(vk, table_name, sheet_name, data_type, data, group_id, key_words, stop_words):
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

        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(temp_file_path, scope)
            client = gspread.authorize(creds)
            spreadsheet = client.open(table_name)

            try:
                worksheet1 = spreadsheet.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet1 = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")

            try:
                worksheet2 = spreadsheet.worksheet('Лист2')
            except gspread.exceptions.WorksheetNotFound:
                worksheet2 = spreadsheet.add_worksheet(title='Лист2', rows="100", cols="20")

            headers = [
                'Дата и время выгрузки', 'Дата публикации', 'Тип контента',
                'Текст сообщения', 'Ссылка на источник', 'Ссылка на профиль пользователя', 'Город'
            ]
            if data_type == 'Post':
                headers.extend(['Название группы', 'Описание группы'])

            headers_for_sheet2 = headers + ['Ключевые слова', 'Стоп-слова']

            if not worksheet1.row_values(1):
                worksheet1.append_row(headers)
                logger.info(f"Добавлены заголовки для листа '{sheet_name}'.")
            if not worksheet2.row_values(1):
                worksheet2.append_row(headers_for_sheet2)
                logger.info("Добавлены заголовки для листа 'Лист2'.")

            group_info = vk.groups.getById(group_id=group_id, fields=['description', 'city'])[0]
            group_name = group_info.get('name', 'Неизвестно')
            group_description = group_info.get('description', 'Описание недоступно')
            group_city = group_info.get('city', {}).get('title', 'Город группы неизвестен')

            rows_key_only = []
            rows_key_and_stop = []
            rows_stop_only = []
            unique_records = set()

            if data:
                for item in data:
                    text = clean_text(item.get('text', ''))
                    user_city = 'Город неизвестен'
                    profile_link = ''
                    if item.get('from_id') and item['from_id'] > 0:
                        user_info = vk.users.get(user_ids=item['from_id'], fields=['city'])
                        if user_info:
                            user_city = user_info[0].get('city', {}).get('title', 'Город неизвестен')
                        profile_link = f"https://vk.com/id{item['from_id']}"
                    else:
                        profile_link = f"https://vk.com/club{abs(item['owner_id'])}"

                    row = [
                        timezone.now().strftime('%Y-%m-%d %H:%M:%S'),
                        datetime.fromtimestamp(item['date']).strftime('%Y-%m-%d %H:%M:%S'),
                        'Пост' if data_type == 'Post' else 'Комментарий',
                        text,
                        f"https://vk.com/wall{item['owner_id']}_{item['id']}" if data_type == 'Post' else f"https://vk.com/wall{item['owner_id']}_{item.get('post_id', '')}",
                        profile_link,
                        user_city if data_type == 'Comment' else group_city
                    ]

                    if data_type == 'Post':
                        row.extend([group_name, group_description])

                    filter_type = filter_text(text, key_words, stop_words)

                    filtered_key_words = ', '.join([kw for kw in key_words if kw in text])
                    filtered_stop_words = ', '.join([sw for sw in stop_words if sw in text])

                    row_for_sheet2 = row + [filtered_key_words, filtered_stop_words]

                    if filter_type == 'key_only':
                        rows_key_only.append(row)
                    elif filter_type == 'key_and_stop':
                        rows_key_and_stop.append(row_for_sheet2)
                    elif filter_type == 'stop_only':
                        rows_stop_only.append(row_for_sheet2)

            if rows_key_only or rows_key_and_stop or rows_stop_only:
                last_row_sheet1 = len(worksheet1.get_all_values()) + 1
                last_row_sheet2 = len(worksheet2.get_all_values()) + 1

                ordered_rows_for_sheet2 = rows_key_only + rows_key_and_stop + rows_stop_only
                if ordered_rows_for_sheet2:
                    worksheet2.insert_rows(ordered_rows_for_sheet2, row=last_row_sheet2)

                all_rows = rows_key_only + rows_key_and_stop + rows_stop_only
                if all_rows:
                    worksheet1.insert_rows(all_rows, row=last_row_sheet1)

                # Use Google Sheets API to update text wrapping
                service = build('sheets', 'v4', credentials=creds)
                requests = [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": worksheet1.id,
                                "startRowIndex": 0,
                                "endRowIndex": len(all_rows),
                                "startColumnIndex": 0,
                                "endColumnIndex": len(headers),
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "wrapStrategy": "WRAP"
                                }
                            },
                            "fields": "userEnteredFormat.wrapStrategy"
                        }
                    },
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": worksheet2.id,
                                "startRowIndex": 0,
                                "endRowIndex": len(ordered_rows_for_sheet2),
                                "startColumnIndex": 0,
                                "endColumnIndex": len(headers_for_sheet2),
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "wrapStrategy": "WRAP"
                                }
                            },
                            "fields": "userEnteredFormat.wrapStrategy"
                        }
                    }
                ]
                body = {
                    'requests': requests
                }
                service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet.id, body=body).execute()

            logger.info(f"Данные успешно сохранены в лист '{sheet_name}' таблицы '{table_name}'.")

        finally:
            os.remove(temp_file_path)

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Файл '{table_name}' не найден. Убедитесь, что файл существует.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в Google Sheets: {e}")


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
