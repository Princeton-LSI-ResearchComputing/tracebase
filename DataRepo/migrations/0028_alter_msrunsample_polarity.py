# Generated by Django 4.2.4 on 2023-12-14 20:55

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("DataRepo", "0027_msrun_to_msrunsample_msrunsequence"),
    ]

    operations = [
        migrations.AlterField(
            model_name="msrunsample",
            name="polarity",
            field=models.CharField(
                blank=True,
                choices=[
                    ("unknown", "unknown"),
                    ("positive", "positive"),
                    ("negative", "negative"),
                ],
                help_text="The polarity mode of this mass spectrometry run.",
                max_length=8,
                null=True,
            ),
        ),
    ]