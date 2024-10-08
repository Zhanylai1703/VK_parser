from huey import crontab
from huey.contrib.djhuey import task, periodic_task, db_task
from datetime import datetime
from django.utils import timezone
import vk_api
import logging

from .forms import VKGroupMassCreateForm
from .models import ParsingSettings, VKGroup, Spam
from .utils import save_to_google_sheet, filter_text, clean_text, get_user_token, save_all_posts_to_first_sheet

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
                logger.info(f"Группа {group.name} не помечена как спам. Начало сбора данных.")

                if setting.post:
                    try:
                        posts = vk.wall.get(owner_id=-int(group.group_id), count=20)
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

            # Сохранение данных
            table_name = setting.table_name if setting.table_name else 'DefaultSheet'
            logger.info(f"Сохранение данных для группы {group.name} в таблицу '{table_name}'.")

            if setting.post:
                save_all_posts_to_first_sheet(vk, table_name, 'Лист1', 'Post', all_posts, group.group_id)

                if filtered_posts:  # Сохраняем на Лист2 только если есть фильтрованные посты
                    save_to_google_sheet(vk, table_name, 'Лист2', 'Post', filtered_posts, group.group_id,
                                         setting.keywords.split(','), setting.stopwords.split(','))

            if setting.comment:
                save_all_posts_to_first_sheet(vk, table_name, 'Лист1', 'Comment', all_comments, group.group_id,)

                if filtered_comments:  # Сохраняем на Лист2 только если есть фильтрованные комментарии
                    save_to_google_sheet(vk, table_name, 'Лист2', 'Comment', filtered_comments, group.group_id,
                                         setting.keywords.split(','), setting.stopwords.split(','))

        logger.info(f"Завершен парсинг для настроек с ID {setting_id} - {timezone.now()}")

    except Exception as e:
        logger.error(f"Ошибка при парсинге данных с ID {setting_id}: {e}")


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


@db_task()
def add_vk_groups_async(form_data):
    # Здесь можно использовать сохранение формы или индивидуальную обработку данных
    form = VKGroupMassCreateForm(form_data)
    if form.is_valid():
        form.save()
