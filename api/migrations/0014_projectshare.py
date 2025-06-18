# Generated manually for ProjectShare model

import django.core.serializers.json
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0013_projects_concatenated_file'),
    ]

    operations = [
        migrations.CreateModel(
            name='ProjectShare',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('share_type', models.CharField(choices=[('project', 'Entire Project'), ('file', 'Specific File')], default='project', max_length=10)),
                ('permission_level', models.CharField(choices=[('view', 'View Only'), ('edit', 'Edit'), ('admin', 'Admin')], default='view', max_length=10)),
                ('file_type', models.CharField(blank=True, max_length=10, null=True)),
                ('file_name', models.CharField(blank=True, max_length=255, null=True)),
                ('sheet_name', models.CharField(blank=True, max_length=255, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('is_active', models.BooleanField(default=True)),
                ('project', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='shares', to='api.projects')),
                ('shared_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='shared_projects', to='api.user')),
                ('shared_with', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='received_shares', to='api.user')),
            ],
            options={
                'ordering': ['-created_at'],
                'unique_together': {('project', 'shared_with', 'share_type', 'file_type', 'file_name', 'sheet_name')},
            },
        ),
    ]
