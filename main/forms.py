from django import forms
from .models import VKGroup
from .utils import get_group_id_by_domain  # Предполагаем, что функция get_group_id_by_domain находится в utils.py


class VKGroupMassCreateForm(forms.Form):
    urls = forms.CharField(widget=forms.Textarea, help_text="Введите ссылки на группы, по одной на строку.")

    def save(self):
        # Разбиваем введенные ссылки на список строк
        urls = self.cleaned_data['urls'].splitlines()

        # Получаем список доменов существующих групп из базы данных
        existing_domains = VKGroup.objects.values_list('group_domain', flat=True)

        groups = []
        for url in urls:
            domain = url.strip().split("/")[-1]  # Извлекаем домен из URL
            print(f"Обрабатываем домен: {domain}")

            # Проверяем, существует ли уже группа с таким доменом в базе данных
            if domain in existing_domains:
                print(f"Группа с доменом {domain} уже существует.")
                continue  # Пропускаем добавление этой группы

            # Получаем идентификатор группы по домену
            group_id = get_group_id_by_domain(domain)
            if group_id:
                print(f"Найден group_id: {group_id}")
                # Создаем новый экземпляр VKGroup и добавляем его в список
                groups.append(VKGroup(name=domain, group_id=group_id, group_domain=domain))
            else:
                print(f"Не удалось получить group_id для домена: {domain}")

        # Оптимально добавляем новые экземпляры VKGroup в базу данных
        if groups:
            VKGroup.objects.bulk_create(groups)
            print(f"Добавлено {len(groups)} групп.")
        else:
            print("Нет групп для добавления.")

