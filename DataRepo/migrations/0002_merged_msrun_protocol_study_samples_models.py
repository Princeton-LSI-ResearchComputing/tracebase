# Generated by Django 3.1.6 on 2021-03-30 19:54

import datetime

import django.core.validators
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("DataRepo", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Animal",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=256, unique=True)),
                ("state", models.CharField(max_length=256)),
                (
                    "tracer_labeled_atom",
                    models.CharField(
                        blank=True,
                        choices=[
                            ("C", "Carbon"),
                            ("N", "Nitrogen"),
                            ("H", "Hydrogen"),
                            ("O", "Oxygen"),
                            ("S", "Sulfur"),
                        ],
                        default="C",
                        max_length=1,
                        null=True,
                    ),
                ),
                (
                    "tracer_labeled_count",
                    models.PositiveSmallIntegerField(
                        null=True,
                        validators=[
                            django.core.validators.MinValueValidator(1),
                            django.core.validators.MaxValueValidator(20),
                        ],
                    ),
                ),
                (
                    "tracer_infusion_rate",
                    models.FloatField(
                        null=True,
                        validators=[django.core.validators.MinValueValidator(0)],
                    ),
                ),
                (
                    "tracer_infusion_concentration",
                    models.FloatField(
                        null=True,
                        validators=[django.core.validators.MinValueValidator(0)],
                    ),
                ),
                ("genotype", models.CharField(max_length=256)),
                (
                    "body_weight",
                    models.FloatField(
                        null=True,
                        validators=[django.core.validators.MinValueValidator(0)],
                    ),
                ),
                (
                    "age",
                    models.FloatField(
                        null=True,
                        validators=[django.core.validators.MinValueValidator(0)],
                    ),
                ),
                (
                    "sex",
                    models.CharField(
                        blank=True,
                        choices=[("F", "female"), ("M", "male")],
                        max_length=1,
                        null=True,
                    ),
                ),
                ("diet", models.CharField(max_length=256, null=True)),
                ("feeding_status", models.CharField(max_length=256, null=True)),
            ],
        ),
        migrations.CreateModel(
            name="Protocol",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=256, unique=True)),
                ("description", models.TextField(blank=True)),
            ],
        ),
        migrations.CreateModel(
            name="Study",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=256, unique=True)),
                ("description", models.TextField(blank=True)),
            ],
        ),
        migrations.CreateModel(
            name="Tissue",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=256, unique=True)),
            ],
        ),
        migrations.AlterField(
            model_name="compound",
            name="hmdb_id",
            field=models.CharField(max_length=11, unique=True),
        ),
        migrations.CreateModel(
            name="Sample",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=256, unique=True)),
                ("date", models.DateField(default=datetime.date.today)),
                ("researcher", models.CharField(max_length=256)),
                (
                    "animal",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="samples",
                        to="DataRepo.animal",
                    ),
                ),
                (
                    "tissue",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.RESTRICT,
                        to="DataRepo.tissue",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="MSRun",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=256, unique=True)),
                ("date", models.DateTimeField(auto_now_add=True)),
                (
                    "protocol",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.RESTRICT,
                        to="DataRepo.protocol",
                    ),
                ),
                (
                    "sample",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.RESTRICT,
                        to="DataRepo.sample",
                    ),
                ),
            ],
        ),
        migrations.AddField(
            model_name="animal",
            name="studies",
            field=models.ManyToManyField(related_name="animals", to="DataRepo.Study"),
        ),
        migrations.AddField(
            model_name="animal",
            name="tracer_compound",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.RESTRICT,
                to="DataRepo.compound",
            ),
        ),
    ]
