# Generated by Django 4.2.11 on 2024-07-09 19:00

from django.db import migrations
import django.db.models.functions.text


class Migration(migrations.Migration):

    dependencies = [
        ("DataRepo", "0040_alter_lcmethod_run_length"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="protocol",
            options={
                "ordering": [django.db.models.functions.text.Upper("name")],
                "verbose_name": "protocol",
                "verbose_name_plural": "protocols",
            },
        ),
    ]
