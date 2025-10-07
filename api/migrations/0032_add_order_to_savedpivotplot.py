from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0031_add_insights_to_savedpivotplot'),
    ]

    operations = [
        migrations.AddField(
            model_name='savedpivotplot',
            name='order_id',
            field=models.IntegerField(blank=True, default=0, null=True),
        ),
        migrations.AlterModelOptions(
            name='savedpivotplot',
            options={'ordering': ['order_id', '-updated_at']},
        ),
    ]


