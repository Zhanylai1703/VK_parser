# views.py
from django.shortcuts import render, redirect
from django.contrib import messages
from .forms import VKGroupMassCreateForm


def mass_create_view(request):
    if request.method == 'POST':
        form = VKGroupMassCreateForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Группы успешно добавлены.")
            return redirect('/admin/main/vkgroup/')
    else:
        form = VKGroupMassCreateForm()

    return render(request, 'admin/mass_create.html', {'form': form})
