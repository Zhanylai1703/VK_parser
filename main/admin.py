from django.contrib import admin

from main.models import VKGroup, ParsingSettings


@admin.register(VKGroup)
class VKGroupAdmin(admin.ModelAdmin):
    pass


@admin.register(ParsingSettings)
class ParsingSettingsAdmin(admin.ModelAdmin):
    pass