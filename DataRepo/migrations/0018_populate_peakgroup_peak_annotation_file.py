import hashlib

import django.db.models.deletion
from django.core.exceptions import ObjectDoesNotExist
from django.db import migrations, models


def populate_peak_annotation_file(apps, schema_editor):
    # We can't import the models directly as there may be a newer
    # version than this migration expects. We use the historical version.
    PeakGroup = apps.get_model("DataRepo", "PeakGroup")
    ArchiveFile = apps.get_model("DataRepo", "ArchiveFile")
    DataType = apps.get_model("DataRepo", "DataType")
    DataFormat = apps.get_model("DataRepo", "DataFormat")

    # If a pre-existing DataType is not found, create the default
    try:
        ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
    except ObjectDoesNotExist:
        ms_peak_annotation = DataType.objects.create(
            code="ms_peak_annotation",
            name="Mass Spectrometry Peak Annotation",
            description="Annotated peak data from one or more mass spectrometry runs",
        )

    # If a pre-existing DataFormat is not found, create the default
    try:
        unknown_format = DataFormat.objects.get(code="unknown")
    except ObjectDoesNotExist:
        unknown_format = DataFormat.objects.create(
            code="unknown", name="Unknown format", description="Unknown data format"
        )

    # Ensure each peakgroup has an associated peak_annotation_file
    for peakgroup in PeakGroup.objects.all():
        if not peakgroup.peak_annotation_file:
            # We don't have these files, so use filename to generate sha1
            encoded_filename = peakgroup.peak_group_set.filename.encode()
            hash_obj = hashlib.sha1(encoded_filename)
            hexa_value = hash_obj.hexdigest()

            # Create the ArchiveFile record from PeakGroupSet
            # Lookup by filename, checksum, and data_type
            # If not found, create and use defaults for data_format and file_location
            archive_file, created = ArchiveFile.objects.get_or_create(
                filename=peakgroup.peak_group_set.filename,
                checksum=hexa_value,
                data_type=ms_peak_annotation,
                defaults={
                    "data_format": unknown_format,
                    "file_location": None,
                },
            )
            peakgroup.peak_annotation_file = archive_file
            peakgroup.save()


class Migration(migrations.Migration):
    dependencies = [
        ("DataRepo", "0017_link_peakgroup_to_archive_file"),
    ]

    operations = [
        migrations.RunPython(populate_peak_annotation_file),
    ]
