# Generated by Django 5.1 on 2024-08-27 07:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('main', '0002_alter_parsingsettings_options_alter_vkgroup_options_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='vkgroup',
            name='group_domain',
            field=models.CharField(default=1, max_length=255),
            preserve_default=False,
        ),
    ]
