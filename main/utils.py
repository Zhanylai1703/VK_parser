import re
from datetime import datetime

import vk_api
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2 import service_account

import logging

from main.models import VKGroup

logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_FILE = 'vk-parser-433009-ba7bf6f870b6.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Авторизация
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)


def get_google_sheet(sheet_name, sheet_type):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('vk-parser-433009-ba7bf6f870b6.json', scope)
    client = gspread.authorize(creds)

    try:
        spreadsheet = client.open(sheet_name)
        if sheet_type == 'Лист1':
            sheet = spreadsheet.worksheet('Лист1')
        elif sheet_type == 'Лист2':
            sheet = spreadsheet.worksheet('Лист2')
        else:
            raise ValueError(f"Unknown sheet type: {sheet_type}")
        print(f"Found existing spreadsheet '{sheet_name}' with sheet '{sheet_type}'")
        print(f"Spreadsheet URL: {spreadsheet.url}")
        return sheet
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Spreadsheet '{sheet_name}' not found. Please make sure the spreadsheet exists.")
        return None
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_type}' not found in spreadsheet '{sheet_name}'.")
        return None
    except Exception as e:
        print(f"Error while accessing Google Sheets: {e}")
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
    key_words = truncate_keywords(key_words)  # Truncate long keywords
    key_words_regex = '|'.join(re.escape(word) for word in key_words)
    stop_words_regex = '|'.join(re.escape(word) for word in stop_words)

    if re.search(key_words_regex, text) and not re.search(stop_words_regex, text):
        return True
    return False


def clean_text(text):
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text


def truncate_keywords(keywords):
    return [kw if len(kw) <= 8 else kw[:-2] for kw in keywords]


def save_to_google_sheet(sheet_name, sheet_type, data_type, items):
    sheet = get_google_sheet(sheet_name, sheet_type)
    if sheet:
        for item in items:
            text = clean_text(item.get('text', ''))  # Clean the text
            current_date_and_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            profile_link = f"https://vk.com/id{item.get('from_id')}" if item.get('from_id') else 'N/A'
            publication_date_timestamp = item.get('date')  # Временная метка из VK API
            if publication_date_timestamp:
                publication_date = datetime.fromtimestamp(publication_date_timestamp).strftime('%Y-%m-%d %H:%M:%S')
            else:
                publication_date = 'N/A'

            group = VKGroup.objects.filter(group_id=item.get('owner_id')).first()
            if group:
                group_domain = group.domain  # Используем domain в качестве имени группы
                group_description = group.description if group.description else 'N/A'
                city = group.city if group.city else 'N/A'
            else:
                group_domain = 'N/A'
                group_description = 'N/A'
                city = 'N/A'
                logger.error(f"VKGroup with group_id {item.get('owner_id')} not found.")

            # Формируем ссылку на источник
            if data_type == 'Post':
                source_link = f"https://vk.com/{group_domain}?w=wall-{item.get('owner_id')}_{item.get('id')}"
            elif data_type == 'Comment':
                source_link = f"https://vk.com/{group_domain}?w=wall-{item.get('owner_id')}_{item.get('post_id')}&reply={item.get('id')}"
            else:
                source_link = 'N/A'

            # Формируем ссылку на профиль пользователя
            profile_link = f"https://vk.com/id{item.get('from_id')}" if item.get('from_id') else 'N/A'

            data = [
                current_date_and_time,
                publication_date,
                data_type,
                item.get('from_id'),
                text,
                source_link,
                profile_link,
                sheet_name,
                "group_description",
            ]
            save_to_google_sheet_worksheet(sheet, data)
    else:
        logger.warning(f"Skipping saving data because the spreadsheet '{sheet_name}' - '{sheet_type}' was not found.")


def log_parsing_action(action):
    logger.info(action)
