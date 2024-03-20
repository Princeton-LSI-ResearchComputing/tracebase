# Generated by Django 4.2.10 on 2024-02-29 18:34

from django.db import migrations


def update_instruments(apps, schema_editor):
    MSRunSequence = apps.get_model("DataRepo", "MSRunSequence")

    instrument_conversion = {
        "HILIC": "unknown",
        "QE": "QE",
        "QE2": "QE2",
        "QTOF": "unknown",
        "unknown": "unknown",
    }

    for sequence in MSRunSequence.objects.all():
        sequence.instrument = instrument_conversion[sequence.instrument]
        sequence.save()


class Migration(migrations.Migration):
    dependencies = [
        ("DataRepo", "0035_alter_msrunsequence_instrument"),
    ]

    operations = [
        migrations.RunPython(update_instruments),
    ]