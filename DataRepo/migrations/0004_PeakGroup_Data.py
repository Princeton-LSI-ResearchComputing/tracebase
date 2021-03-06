# Generated by Django 3.1.6 on 2021-04-16 00:39

import django.core.validators
import django.db.models.deletion
from django.db import migrations, models

import DataRepo.models


class Migration(migrations.Migration):

    dependencies = [
        ("DataRepo", "0003_auto_20210402_1105"),
    ]

    operations = [
        migrations.CreateModel(
            name="PeakGroup",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                (
                    "name",
                    models.CharField(
                        help_text="Compound or isomer group name [i.e. citrate/isocitrate]",
                        max_length=256,
                    ),
                ),
                (
                    "formula",
                    models.CharField(
                        help_text="molecular formula of the compound [i.e. C6H12O6]",
                        max_length=256,
                    ),
                ),
                (
                    "compounds",
                    models.ManyToManyField(
                        help_text="database identifier(s) for the TraceBase compound(s) that this PeakGroup describes",
                        related_name="peak_groups",
                        to="DataRepo.Compound",
                    ),
                ),
                (
                    "ms_run",
                    models.ForeignKey(
                        help_text="database identifier of the MS run this PeakGroup was derived from",
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="peak_groups",
                        to="DataRepo.msrun",
                    ),
                ),
            ],
            options={
                "unique_together": {("name", "ms_run")},
            },
        ),
        migrations.CreateModel(
            name="PeakData",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                (
                    "labeled_element",
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
                        help_text="the type of element that is labeled in this observation (i.e. C, H, O)",
                        max_length=1,
                        null=True,
                    ),
                ),
                (
                    "labeled_count",
                    models.PositiveSmallIntegerField(
                        blank=True,
                        help_text="the M+ value (i.e. Label) for this observation.  "
                        "'1' means one atom is labeled.  '3' means 3 atoms are labeled",
                        null=True,
                        validators=[
                            django.core.validators.MinValueValidator(1),
                            django.core.validators.MaxValueValidator(20),
                        ],
                    ),
                ),
                (
                    "raw_abundance",
                    models.FloatField(
                        help_text="ion counts or raw abundance of this observation",
                        validators=[django.core.validators.MinValueValidator(0)],
                    ),
                ),
                (
                    "corrected_abundance",
                    models.FloatField(
                        help_text="ion counts corrected for natural abundance of isotopomers",
                        validators=[django.core.validators.MinValueValidator(0)],
                    ),
                ),
                (
                    "med_mz",
                    models.FloatField(
                        help_text="median mass/charge value of this measurement",
                        validators=[django.core.validators.MinValueValidator(0)],
                    ),
                ),
                (
                    "med_rt",
                    models.FloatField(
                        help_text="median retention time value of this measurement",
                        validators=[django.core.validators.MinValueValidator(0)],
                    ),
                ),
                (
                    "peak_group",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="peak_data",
                        to="DataRepo.peakgroup",
                    ),
                ),
            ],
            options={
                "unique_together": {("peak_group", "labeled_element", "labeled_count")},
            },
            bases=(models.Model, DataRepo.models.TracerLabeledClass),
        ),
    ]
