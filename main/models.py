from django.db import models


class VKGroup(models.Model):
    name = models.CharField(max_length=255)
    group_id = models.CharField(max_length=100)
    group_domain = models.CharField(max_length=255)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Группа"
        verbose_name_plural = "Группы"


class ParsingSettings(models.Model):
    group = models.ForeignKey(VKGroup, on_delete=models.CASCADE)
    keywords = models.TextField(help_text="Введите ключевые слова, разделенные запятыми")
    stopwords = models.TextField(help_text="Введите стоп-слова, разделенные запятыми")
    google_sheet_name = models.CharField(max_length=255)
    comment = models.BooleanField(default=True, verbose_name="Парсить комментарии")
    post = models.BooleanField(default=True, verbose_name="Парсить посты")
    spam = models.BooleanField(default=True, verbose_name="Спам")

    def __str__(self):
        return f"Настройки для {self.group.name}"

    class Meta:
        verbose_name = "Настройки"
        verbose_name_plural = "Настройки"
