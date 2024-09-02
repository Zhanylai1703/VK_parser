from django.core.management.base import BaseCommand
from django.utils import timezone

from main.models import ParsingSettings
from main.tasks import parse_vk_data


class Command(BaseCommand):
    help = 'Запуск парсинга данных из VK через Huey'

    def handle(self, *args, **options):
        settings = ParsingSettings.objects.all()

        for setting in settings:
            # Schedule the task
            parse_vk_data(setting.id)
            self.stdout.write(self.style.SUCCESS(f"Task scheduled for setting ID: {setting.id}"))
