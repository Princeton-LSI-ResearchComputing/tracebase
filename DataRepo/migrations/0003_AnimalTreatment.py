# Generated by Django 3.1.8 on 2021-05-24 19:41

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("DataRepo", "0002_sample_time_collected"),
    ]

    operations = [
        migrations.AddField(
            model_name="animal",
            name="treatment",
            field=models.ForeignKey(
                blank=True,
                help_text="Lab controlled label of the actions taken on an animal.",
                null=True,
                on_delete=django.db.models.deletion.RESTRICT,
                related_name="animals",
                to="DataRepo.protocol",
            ),
        ),
        migrations.AddField(
            model_name="protocol",
            name="category",
            field=models.CharField(
                choices=[
                    ("msrun_protocol", "LC-MS Run Protocol"),
                    ("animal_treatment", "Animal Treatment"),
                ],
                default="msrun_protocol",
                help_text="Classification of the protocol, e.g. an animal treatment or MSRun procedure.",
                max_length=256,
            ),
        ),
        migrations.AlterField(
            model_name="protocol",
            name="description",
            field=models.TextField(
                blank=True, help_text="Full text of the protocol's methods"
            ),
        ),
    ]
