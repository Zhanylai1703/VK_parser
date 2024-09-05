from django.db import models
from django.core.files.storage import default_storage
from singleton_model import SingletonModel


class VKGroup(models.Model):
    name = models.CharField(max_length=255)
    group_id = models.CharField(max_length=100, unique=True)
    group_domain = models.CharField(max_length=255)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Группа"
        verbose_name_plural = "Группы"


class ParsingSettings(models.Model):
    keywords = models.TextField(
        help_text="Введите ключевые слова, разделенные запятыми",
        verbose_name="Ключевые слова"
    )
    stopwords = models.TextField(
        help_text="Введите стоп-слова, разделенные запятыми",
        verbose_name="Стоп слова"
    )
    comment = models.BooleanField(default=True, verbose_name="Парсить комментарии")
    post = models.BooleanField(default=True, verbose_name="Парсить посты")
    pars_from = models.DateField(verbose_name="Парсить с даты", null=True, blank=True)
    interval = models.IntegerField(verbose_name="Интервал", default=5)
    google_sheet_file = models.FileField(
        help_text="Загрузите файл json",
        upload_to="files/%Y/%m/%d/",
        null=True, blank=True,
        verbose_name="google ключ"
    )
    table_name = models.CharField(
        help_text="Введите название таблицы",
        max_length=255, null=True, blank=True,
        verbose_name="Название таблицы"
    )

    def __str__(self):
        return f"Настройки для {self.keywords}"

    def get_google_sheet_credentials(self):
        if self.google_sheet_file:
            file_path = default_storage.path(self.google_sheet_file.name)
            return file_path
        else:
            raise ValueError("Файл с ключами Google Sheets не найден.")

    class Meta:
        verbose_name = "Настройки"
        verbose_name_plural = "Настройки"


class UserToken(models.Model):
    user_id = models.CharField(max_length=100, unique=True)
    access_token = models.CharField(max_length=255)
    requests_used = models.IntegerField(default=0)
    last_used = models.DateTimeField(auto_now=True)
    daily_limit = models.IntegerField(default=5000)

    def __str__(self):
        return f"Token for user {self.user_id}"

    class Meta:
        verbose_name = "VK аккаунт"
        verbose_name_plural = "VK аккаунты"


class Spam(SingletonModel):
    name = models.CharField(max_length=255)
    groups = models.ManyToManyField(
        VKGroup, related_name="spam_groups",
        verbose_name="Группы", blank=True
    )

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Спам"
        verbose_name_plural = "Спам"

