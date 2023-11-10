import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("DataRepo", "0019_populate_peakgroup_peak_annotation_file"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="peakgroup",
            name="peak_group_set",
        ),
        migrations.AlterField(
            model_name="peakgroup",
            name="peak_annotation_file",
            field=models.ForeignKey(
                help_text="The data file from which this PeakGroup was imported.",
                on_delete=django.db.models.deletion.RESTRICT,
                related_name="peak_groups",
                to="DataRepo.archivefile",
            ),
        ),
    ]
