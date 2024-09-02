# Generated by Django 5.1 on 2024-08-29 22:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('main', '0004_parsingsettings_spam'),
    ]

    operations = [
        migrations.CreateModel(
            name='UserToken',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.CharField(max_length=100, unique=True)),
                ('access_token', models.CharField(max_length=255)),
                ('requests_used', models.IntegerField(default=0)),
                ('last_used', models.DateTimeField(auto_now=True)),
            ],
        ),
    ]
