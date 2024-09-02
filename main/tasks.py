from huey import crontab
from huey.contrib.djhuey import task, periodic_task, db_task
from datetime import datetime, timedelta
from django.utils import timezone
import vk_api
import logging
from .models import ParsingSettings, VKGroup, Spam
from .utils import save_to_google_sheet, filter_text, clean_text, get_user_token

logger = logging.getLogger(__name__)


@task()
def parse_vk_data(setting_id):
    try:
        setting = ParsingSettings.objects.get(id=setting_id)

        interval = setting.interval
        if interval > 0:
            now = timezone.now()
            last_run_time = now - timedelta(minutes=interval)
        else:
            last_run_time = timezone.now() - timedelta(minutes=1)

        user_token = get_user_token()
        vk_session = vk_api.VkApi(token=user_token.access_token)
        vk = vk_session.get_api()

        groups = VKGroup.objects.all()

        spam_settings = Spam.objects.first()

        for group in groups:
            is_spam_group = spam_settings and group in spam_settings.groups.all()

            all_posts = []
            filtered_posts = []
            all_comments = []
            filtered_comments = []

            if is_spam_group:
                if setting.post:
                    try:
                        if setting.pars_from:
                            start_date = datetime.combine(setting.pars_from, datetime.min.time())
                            end_date = timezone.now()
                            posts = vk.wall.get(owner_id=-int(group.group_id), count=100)
                            all_posts.extend([post for post in posts['items'] if
                                              datetime.fromtimestamp(post['date']) >= start_date])
                        else:
                            posts = vk.wall.get(owner_id=-int(group.group_id), count=10)
                            all_posts.extend(posts['items'])
                    except vk_api.exceptions.ApiError as e:
                        logger.error(f"Ошибка VK API при получении постов: {e}")
                        continue

                    for post in all_posts:
                        post_id = post['id']

                        if setting.comment:
                            try:
                                comments = vk.wall.getComments(
                                    owner_id=-int(group.group_id), post_id=post_id
                                )
                                all_comments.extend(comments['items'])
                            except vk_api.exceptions.ApiError as e:
                                logger.error(f"Ошибка VK API при получении комментариев: {e}")
                                continue

                            filtered_comments.extend([
                                comment for comment in all_comments
                                if filter_text(clean_text(comment['text']), setting.keywords.split(','),
                                               setting.stopwords.split(','))
                            ])

                        filtered_posts.extend([
                            post for post in all_posts
                            if filter_text(clean_text(post.get('text', '')), setting.keywords.split(','),
                                           setting.stopwords.split(','))
                        ])

            table_name = setting.table_name if setting.table_name else 'DefaultSheet'

            if setting.post:
                save_to_google_sheet(
                    vk,
                    table_name,
                    'Лист1',
                    'Post',
                    all_posts,
                    group.group_id,
                    setting.keywords.split(','),
                    setting.stopwords.split(',')
                )
                save_to_google_sheet(
                    vk,
                    table_name,
                    'Лист2',
                    'Post',
                    filtered_posts,
                    group.group_id,
                    setting.keywords.split(','),
                    setting.stopwords.split(',')
                )

            if setting.comment:
                save_to_google_sheet(
                    vk,
                    table_name,
                    'Лист1',
                    'Comment',
                    all_comments,
                    group.group_id,
                    setting.keywords.split(','),
                    setting.stopwords.split(',')
                )
                save_to_google_sheet(
                    vk,
                    table_name,
                    'Лист2',
                    'Comment',
                    filtered_comments,
                    group.group_id,
                    setting.keywords.split(','),
                    setting.stopwords.split(',')
                )

            logger.info(f"Парсинг завершён для группы: {group.name} в {timezone.now()}")

    except Exception as e:
        logger.error(f"Ошибка в задаче parse_vk_data: {e}")


@periodic_task(crontab(minute='*/5'))
def schedule_parse_vk_data():
    try:
        settings = ParsingSettings.objects.all()
        for setting in settings:
            if setting.interval > 0:
                parse_vk_data(setting.id)
    except Exception as e:
        logger.error(f"Ошибка в расписании задачи parse_vk_data: {e}")
