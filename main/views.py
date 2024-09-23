# views.py
from django.shortcuts import render, redirect
from django.contrib import messages
from .forms import VKGroupMassCreateForm
from .tasks import add_vk_groups_async


def mass_create_view(request):
    if request.method == 'POST':
        form = VKGroupMassCreateForm(request.POST)
        if form.is_valid():
            # Запускаем асинхронную задачу через huey
            add_vk_groups_async(form.cleaned_data)
            messages.success(request, "Задача по добавлению групп запущена. Группы будут добавлены в фоне.")
            return redirect('/admin/main/vkgroup/')
    else:
        form = VKGroupMassCreateForm()

    return render(request, 'admin/mass_create.html', {'form': form})
