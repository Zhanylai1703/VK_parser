from django.shortcuts import render, redirect
from django.contrib import admin, messages
from django.shortcuts import render
from solo.admin import SingletonModelAdmin

from .forms import VKGroupMassCreateForm
from django.urls import path

from main.models import VKGroup, ParsingSettings, UserToken, Spam


class SpamAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        if Spam.objects.exists():
            return False
        return super().has_add_permission(request)


admin.site.register(Spam, SpamAdmin)


class VKGroupAdmin(admin.ModelAdmin):
    list_display = ['name', 'group_id']
    change_list_template = 'admin/vkgroup_change_list.html'

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('mass-create/', self.mass_create_view, name='vkgroup_mass_create'),
        ]
        return custom_urls + urls

    def mass_create_view(self, request):
        if request.method == 'POST':
            form = VKGroupMassCreateForm(request.POST)
            if form.is_valid():
                form.save()
                messages.success(request, "Группы успешно добавлены.")
                return redirect('/admin/main/vkgroup/')
        else:
            form = VKGroupMassCreateForm()

        return render(request, 'admin/mass_create.html', {'form': form})


admin.site.register(VKGroup, VKGroupAdmin)


@admin.register(ParsingSettings)
class ParsingSettingsAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        if Spam.objects.exists():
            return False
        return super().has_add_permission(request)


@admin.register(UserToken)
class UserTokenAdmin(admin.ModelAdmin):
    list_display = ['user_id', 'requests_used', 'last_used']
