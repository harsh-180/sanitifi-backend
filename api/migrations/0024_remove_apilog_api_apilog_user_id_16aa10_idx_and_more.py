# Generated by Django 5.1.5 on 2025-07-07 08:10

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0023_remove_apilog_api_apilog_user_id_16aa10_idx_and_more'),
    ]

    operations = [
        migrations.RemoveIndex(
            model_name='apilog',
            name='api_apilog_user_id_16aa10_idx',
        ),
        migrations.RemoveField(
            model_name='apilog',
            name='user_id',
        ),
        migrations.AddField(
            model_name='apilog',
            name='user',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='api.user'),
        ),
        migrations.AddIndex(
            model_name='apilog',
            index=models.Index(fields=['user', 'request_timestamp'], name='api_apilog_user_id_16aa10_idx'),
        ),
    ]
