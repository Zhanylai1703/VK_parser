# Generated by Django 5.1 on 2024-08-29 23:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('main', '0005_usertoken'),
    ]

    operations = [
        migrations.AddField(
            model_name='usertoken',
            name='daily_limit',
            field=models.IntegerField(default=5000),
        ),
    ]
