# Generated manually for APILog model

import django.core.serializers.json
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0015_useractionlog'),
    ]

    operations = [
        migrations.CreateModel(
            name='APILog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('endpoint', models.CharField(max_length=255)),
                ('method', models.CharField(max_length=10)),
                ('ip_address', models.GenericIPAddressField(blank=True, null=True)),
                ('user_agent', models.TextField(blank=True, null=True)),
                ('request_payload', models.JSONField(blank=True, default=dict, encoder=django.core.serializers.json.DjangoJSONEncoder)),
                ('request_headers', models.JSONField(blank=True, default=dict, encoder=django.core.serializers.json.DjangoJSONEncoder)),
                ('request_params', models.JSONField(blank=True, default=dict, encoder=django.core.serializers.json.DjangoJSONEncoder)),
                ('response_status', models.IntegerField()),
                ('response_data', models.JSONField(blank=True, default=dict, encoder=django.core.serializers.json.DjangoJSONEncoder)),
                ('response_headers', models.JSONField(blank=True, default=dict, encoder=django.core.serializers.json.DjangoJSONEncoder)),
                ('request_timestamp', models.DateTimeField(auto_now_add=True)),
                ('response_timestamp', models.DateTimeField(blank=True, null=True)),
                ('duration_ms', models.IntegerField(blank=True, null=True)),
                ('error_message', models.TextField(blank=True, null=True)),
                ('error_traceback', models.TextField(blank=True, null=True)),
                ('project_id', models.IntegerField(blank=True, null=True)),
                ('file_type', models.CharField(blank=True, max_length=50, null=True)),
                ('file_name', models.CharField(blank=True, max_length=255, null=True)),
                ('sheet_name', models.CharField(blank=True, max_length=255, null=True)),
                ('user', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='api.user')),
            ],
            options={
                'ordering': ['-request_timestamp'],
            },
        ),
        migrations.AddIndex(
            model_name='apilog',
            index=models.Index(fields=['user', 'request_timestamp'], name='api_apilog_user_id_2a8b8c_idx'),
        ),
        migrations.AddIndex(
            model_name='apilog',
            index=models.Index(fields=['endpoint', 'method'], name='api_apilog_endpoin_8c8b8c_idx'),
        ),
        migrations.AddIndex(
            model_name='apilog',
            index=models.Index(fields=['response_status'], name='api_apilog_respons_9c8b8c_idx'),
        ),
        migrations.AddIndex(
            model_name='apilog',
            index=models.Index(fields=['project_id'], name='api_apilog_project_0c8b8c_idx'),
        ),
    ] 