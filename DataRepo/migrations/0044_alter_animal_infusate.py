# Generated by Django 4.2.16 on 2024-12-04 18:50

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("DataRepo", "0043_alter_peakgroup_name"),
    ]

    operations = [
        migrations.AlterField(
            model_name="animal",
            name="infusate",
            field=models.ForeignKey(
                blank=True,
                help_text="The solution infused into the animal containing 1 or more tracer compounds at specific concentrations.",
                null=True,
                on_delete=django.db.models.deletion.RESTRICT,
                related_name="animals",
                to="DataRepo.infusate",
            ),
        ),
    ]