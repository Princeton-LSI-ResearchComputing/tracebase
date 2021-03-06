# Generated by Django 3.1.8 on 2021-04-28 12:44

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("DataRepo", "0006_msrun_researcher"),
    ]

    operations = [
        migrations.AlterField(
            model_name="peakdata",
            name="labeled_count",
            field=models.PositiveSmallIntegerField(
                blank=True,
                help_text="the M+ value (i.e. Label) for this observation.  "
                "'1' means one atom is labeled.  '3' means 3 atoms are labeled",
                null=True,
                validators=[
                    django.core.validators.MinValueValidator(0),
                    django.core.validators.MaxValueValidator(20),
                ],
            ),
        ),
    ]
