# Generated by Django 5.1.5 on 2025-01-31 06:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0004_projects_kpi_id_projects_media_id_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='projects',
            name='kpi_file',
            field=models.FileField(blank=True, null=True, upload_to=''),
        ),
        migrations.AlterField(
            model_name='projects',
            name='media_file',
            field=models.FileField(blank=True, null=True, upload_to=''),
        ),
    ]
