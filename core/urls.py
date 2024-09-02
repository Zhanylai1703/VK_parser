
from django.contrib import admin
from django.urls import path

from main.views import mass_create_view

urlpatterns = [
    path('admin/', admin.site.urls),
    path('mass-create/', mass_create_view, name='vkgroup_mass_create'),
]
