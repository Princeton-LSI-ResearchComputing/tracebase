# Generated by Django 3.1.6 on 2021-03-11 22:15

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Compound",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=256, unique=True)),
                ("formula", models.CharField(max_length=256)),
                ("hmdb_id", models.CharField(blank=True, max_length=11)),
            ],
        ),
    ]
