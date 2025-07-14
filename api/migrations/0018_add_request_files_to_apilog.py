# Generated manually for adding request_files field to APILog

import django.core.serializers.json
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0017_merge_20250707_1142'),
    ]

    operations = [
        migrations.AddField(
            model_name='apilog',
            name='request_files',
            field=models.JSONField(blank=True, default=dict, encoder=django.core.serializers.json.DjangoJSONEncoder),
        ),
    ] 