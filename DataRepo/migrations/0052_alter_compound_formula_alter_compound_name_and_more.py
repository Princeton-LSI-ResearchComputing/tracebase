# Generated by Django 4.2.16 on 2025-05-19 19:15

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("DataRepo", "0051_animal_label_combo"),
    ]

    operations = [
        migrations.AlterField(
            model_name="compound",
            name="formula",
            field=models.CharField(
                help_text="The molecular formula of the compound (e.g. 'C6H12O6', 'C16H32O2', etc.).",
                max_length=256,
            ),
        ),
        migrations.AlterField(
            model_name="compound",
            name="name",
            field=models.CharField(
                help_text="The compound name that is commonly used in the laboratory (e.g. 'glucose', 'C16:0', etc.).  Disallowed substrings: [';', '/'].",
                max_length=256,
                unique=True,
            ),
        ),
        migrations.AlterField(
            model_name="compoundsynonym",
            name="name",
            field=models.CharField(
                help_text="A synonymous name for a compound that is commonly used within the laboratory (e.g. 'palmitic acid', 'hexadecanoic acid', 'C16', and 'palmitate' as synonyms for 'C16:0').  Disallowed substrings: [';'].",
                max_length=256,
                primary_key=True,
                serialize=False,
                unique=True,
            ),
        ),
        migrations.AlterField(
            model_name="peakgroup",
            name="name",
            field=models.CharField(
                help_text="Peak group name, composed of 1 or more compound synonyms, delimited by '/', e.g. 'citrate/isocitrate'.  Note, synonyms of the same compound are considered distinct peak groups.  I.e. they may confer information about the compound that is not recorded in the compound record, such as a specific stereoisomer.  Peak group names are subject to the same character restrictions as compound names, aside from the delimiter (/).",
                max_length=256,
            ),
        ),
    ]
