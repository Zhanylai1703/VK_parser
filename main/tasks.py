from huey.contrib.djhuey import periodic_task
from django.utils.timezone import now

from huey import crontab
import vk_api
from vk_api import VkApi

from .models import VKGroup, ParsingSettings
from .utils import filter_text, clean_text, save_to_google_sheet

import logging

logger = logging.getLogger(__name__)


@periodic_task(crontab(minute='*/1'))
def run_parsing():
    vk_session = VkApi(token='vk1.a.37iyXyLQFnyFy9jYaDRj3a7mGVpisZQe0qrpb4LJJjlETVR1ea6X1h5BZZyXKUnVfULdt8BwmgrqxbJCU1RfPFpOrNwVO_73z91_itahb8Jn368LE7rAn3TiqmV0XYmJ5rzlZrh4SlkKu3aCSlHrdw-IAFuNhIZyGq1qM11nJ5UkRuuuqELLIjtUzNR2u91G7KLJKnCjvSygeXRQAX6BOw')
    vk = vk_session.get_api()

    groups = VKGroup.objects.all()

    for group in groups:
        settings = ParsingSettings.objects.get(group=group)

        all_posts = []
        filtered_posts = []
        all_comments = []
        filtered_comments = []

        if settings.post:
            try:
                posts = vk.wall.get(owner_id=-int(group.group_id), count=5)
                all_posts.extend(posts['items'])
            except vk_api.exceptions.ApiError as e:
                logger.error(f"VK API Error fetching posts: {e}")
                continue

            for post in all_posts:
                post_id = post['id']

                if settings.comment:
                    try:
                        comments = vk.wall.getComments(
                            owner_id=-int(group.group_id), post_id=post_id
                        )
                        all_comments.extend(comments['items'])
                    except vk_api.exceptions.ApiError as e:
                        logger.error(f"VK API Error fetching comments: {e}")
                        continue

                    filtered_comments.extend([
                        comment for comment in all_comments
                        if filter_text(clean_text(comment['text']), settings.keywords.split(','),
                                       settings.stopwords.split(','))
                    ])

                filtered_posts.extend([
                    post for post in all_posts
                    if filter_text(clean_text(post.get('text', '')), settings.keywords.split(','),
                                   settings.stopwords.split(','))
                ])

        if settings.post:
            save_to_google_sheet(group.name, 'Лист1', 'Post', all_posts)
            save_to_google_sheet(group.name, 'Лист2', 'Post', filtered_posts)

        if settings.comment:
            save_to_google_sheet(group.name, 'Лист1', 'Comment', all_comments)
            save_to_google_sheet(group.name, 'Лист2', 'Comment', filtered_comments)

        logger.info(f"Parsing completed for group: {group.name} at {now()}")

