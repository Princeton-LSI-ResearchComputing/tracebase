# Generated by Django 4.2.4 on 2023-12-15 23:45

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("DataRepo", "0028_alter_msrunsample_polarity"),
    ]

    operations = [
        migrations.AlterField(
            model_name="peakgroup",
            name="msrun_sample",
            field=models.ForeignKey(
                default=0,
                help_text="The MS Run this PeakGroup belongs to.",
                on_delete=django.db.models.deletion.CASCADE,
                related_name="peak_groups",
                to="DataRepo.msrunsample",
            ),
            preserve_default=False,
        ),
    ]
