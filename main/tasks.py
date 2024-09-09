from huey import crontab
from huey.contrib.djhuey import task, periodic_task, db_task
from datetime import datetime, timedelta
from django.utils import timezone
import vk_api
import logging
from .models import ParsingSettings, VKGroup, Spam
from .utils import save_to_google_sheet, filter_text, clean_text, get_user_token

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@task()
def parse_vk_data(setting_id):
    try:
        setting = ParsingSettings.objects.get(id=setting_id)
        logger.info(f"Начат парсинг для настроек с ID {setting_id} - {timezone.now()}")

        user_token = get_user_token()
        vk_session = vk_api.VkApi(token=user_token.access_token)
        vk = vk_session.get_api()

        groups = VKGroup.objects.all()
        spam_settings = Spam.objects.first()

        # Определение даты начала парсинга
        pars_from_date = setting.pars_from or timezone.now().date()

        logger.info(f"Парсинг данных начиная с {pars_from_date}")

        for group in groups:
            logger.info(f"Парсинг группы: {group.name} - {timezone.now()}")
            all_posts = []
            filtered_posts = []
            all_comments = []
            filtered_comments = []

            is_spam_group = spam_settings and group in spam_settings.groups.all()

            if not is_spam_group:
                logger.info(f"Группа {group.name} помечена как спам. Начало сбора данных.")

                if setting.post:
                    try:
                        posts = vk.wall.get(owner_id=-int(group.group_id), count=10)
                        logger.info(f"Полученные посты: {posts}")
                        for post in posts['items']:
                            post_date = datetime.fromtimestamp(post['date']).date()
                            # Проверка даты (с этой даты)
                            if post_date >= pars_from_date:
                                all_posts.append(post)
                        logger.info(f"Получено {len(all_posts)} постов для группы {group.name}.")
                    except vk_api.exceptions.ApiError as e:
                        logger.error(f"Ошибка VK API при получении постов для группы {group.name}: {e}")
                        continue

                    for post in all_posts:
                        post_id = post['id']
                        if setting.comment:
                            try:
                                comments = vk.wall.getComments(owner_id=-int(group.group_id), post_id=post_id)
                                for comment in comments['items']:
                                    comment_date = datetime.fromtimestamp(comment['date']).date()
                                    # Проверка даты (с этой даты)
                                    if comment_date >= pars_from_date:
                                        all_comments.append(comment)
                                logger.info(f"Получено {len(all_comments)} комментариев для поста {post_id}.")
                            except vk_api.exceptions.ApiError as e:
                                logger.error(f"Ошибка VK API при получении комментариев для поста {post_id}: {e}")
                                continue

                    save_to_google_sheet(vk, setting.table_name, 'Лист1', 'Post', all_posts, group.group_id, setting.keywords, setting.stopwords)
                    save_to_google_sheet(vk, setting.table_name, 'Лист2', 'Post', filtered_posts, group.group_id, setting.keywords, setting.stopwords)

                if setting.comment:
                    try:
                        comments = vk.wall.getComments(owner_id=-int(group.group_id), count=10)
                        logger.info(f"Полученные комментарии: {comments}")
                        for comment in comments['items']:
                            comment_date = datetime.fromtimestamp(comment['date']).date()
                            # Проверка даты (с этой даты)
                            if comment_date >= pars_from_date:
                                filtered_comments.append(comment)
                        logger.info(f"Получено {len(filtered_comments)} комментариев для группы {group.name}.")
                    except vk_api.exceptions.ApiError as e:
                        logger.error(f"Ошибка VK API при получении комментариев для группы {group.name}: {e}")
                        continue

                    save_to_google_sheet(vk, setting.table_name, 'Лист1', 'Comment', all_comments, group.group_id, setting.key_words, setting.stop_words)
                    save_to_google_sheet(vk, setting.table_name, 'Лист2', 'Comment', filtered_comments, group.group_id, setting.key_words, setting.stop_words)

    except ParsingSettings.DoesNotExist:
        logger.error(f"Настройки с ID {setting_id} не найдены.")
    except Exception as e:
        logger.error(f"Ошибка при парсинге данных: {e}")


@periodic_task(crontab(minute='*/5'))
def schedule_parse_vk_data():
    try:
        settings = ParsingSettings.objects.all()
        logger.info(f"Запуск расписания парсинга данных - {timezone.now()}. Найдено {len(settings)} настроек.")

        for setting in settings:
            if setting.interval > 0:
                logger.info(f"Запуск парсинга данных для настроек с ID {setting.id}.")
                parse_vk_data(setting.id)

    except Exception as e:
        logger.error(f"Ошибка в расписании задачи parse_vk_data: {e}")
