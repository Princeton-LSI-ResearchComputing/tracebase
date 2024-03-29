# Generated by Django 4.2.4 on 2023-11-06 14:47

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("DataRepo", "0022_msrunsample_msrunsample_unique_msrunsample"),
    ]

    operations = [
        migrations.AlterField(
            model_name="msrunsample",
            name="ms_data_files",
            field=models.ManyToManyField(
                blank=True,
                help_text="Any mass spectrometry data files (e.g. mzXML files) associated with this sample and sequence, (potentially associated with multiple different raw files for multiple scans of the same sample).",
                related_name="msrun_samples",
                to="DataRepo.archivefile",
            ),
        ),
    ]
