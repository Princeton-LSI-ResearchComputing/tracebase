# Generated by Django 3.1.6 on 2021-03-22 20:29

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("DataRepo", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Protocol",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=256, unique=True)),
                ("description", models.TextField(blank=True)),
            ],
        ),
        migrations.CreateModel(
            name="Sample",
            fields=[("id", models.AutoField(primary_key=True, serialize=False)),],
        ),
        migrations.AlterField(
            model_name="compound",
            name="hmdb_id",
            field=models.CharField(max_length=11, unique=True),
        ),
        migrations.CreateModel(
            name="MSRun",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=256, unique=True)),
                ("date", models.DateField(auto_now_add=True)),
                (
                    "protocol_id",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.RESTRICT,
                        to="DataRepo.protocol",
                    ),
                ),
                (
                    "sample_id",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.RESTRICT,
                        to="DataRepo.sample",
                    ),
                ),
            ],
        ),
    ]
