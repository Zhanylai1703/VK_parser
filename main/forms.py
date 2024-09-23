from django import forms
from .models import VKGroup
from .utils import get_group_id_by_domain  # Предполагаем, что функция get_group_id_by_domain находится в utils.py


class VKGroupMassCreateForm(forms.Form):
    urls = forms.CharField(widget=forms.Textarea, help_text="Введите ссылки на группы, по одной на строку.")

    def save(self):
        # Разбиваем введенные ссылки на список строк
        urls = self.cleaned_data['urls'].splitlines()

        # Получаем список существующих доменов и group_id из базы данных
        existing_domains = set(VKGroup.objects.values_list('group_domain', flat=True))
        existing_group_ids = set(VKGroup.objects.values_list('group_id', flat=True))

        for url in urls:
            domain = url.strip().split("/")[-1]  # Извлекаем домен из URL
            print(f"Обрабатываем домен: {domain}")

            # Проверяем, существует ли уже группа с таким доменом
            if domain in existing_domains:
                print(f"Группа с доменом {domain} уже существует.")
                continue  # Пропускаем добавление этой группы

            # Получаем идентификатор группы по домену
            group_id = get_group_id_by_domain(domain)
            if group_id:
                print(f"Найден group_id: {group_id}")

                # Используем get_or_create, чтобы избежать дублирования
                vk_group, created = VKGroup.objects.get_or_create(
                    group_id=group_id,
                    defaults={'name': domain, 'group_domain': domain}
                )

                if created:
                    print(f"Группа {domain} добавлена.")
                else:
                    print(f"Группа с group_id {group_id} уже существует.")
            else:
                print(f"Не удалось получить group_id для домена: {domain}")
