from django.core.exceptions import ObjectDoesNotExist
from django.db import migrations, models

from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)

def msruns_to_msrunsample_msrunsequence(apps, _):
    # We retrieve the models using an "apps" registry, which contains historical versions of all of the models, because
    # the current version may be newer than this migration expects.
    MSRun = apps.get_model("DataRepo", "MSRun")
    MSRunSample = apps.get_model("DataRepo", "MSRunSample")
    MSRunSequence = apps.get_model("DataRepo", "MSRunSequence")

    disable_caching_updates()
    sequences = {}
    for msrun_rec in MSRun.objects.all():
        # MSRun contains:
        #   researcher, date, lc_method, sample
        # MSRunSample
        #   contains:
        #     msrun_sequence, sample, polarity, ms_raw_file, ms_data_file
        #   mapping:
        #     msrun_sequence = create as needed, based on unique researcher, date, and lc_method
        #     sample = MSRun.sample
        #     polarity = leave null
        #       Note: polarity WAS present in the protocol records previously linked from MSRun.protocol, but that was
        #             all deleted in a previous migration.  We will leave polarity null here, but it will be introduced
        #             during a fresh load, so we're going to ignore it in this migration.
        #     ms_raw_file = leave null
        #     ms_data_file = leave null
        # MSRunSequence
        #   conatins:
        #     researcher, date, instrument, lc_method, notes
        #   mapping:
        #     researcher = MSRun.researcher
        #     date = MSRun.date
        #     lc_method = MSRun.lc_method
        #     instrument = "unknown"
        #     notes = leave null
        sequence_key = f"{msrun_rec.researcher}.{msrun_rec.date}.{msrun_rec.lc_method.name}"
        if sequence_key not in sequences.keys():
            try:
                sequences[sequence_key] = MSRunSequence.objects.get(
                    researcher=msrun_rec.researcher,
                    date=msrun_rec.date,
                    lc_method=msrun_rec.lc_method,
                    # Do not include instrument
                )
            except ObjectDoesNotExist:
                sequences[sequence_key] = MSRunSequence.objects.create(
                    researcher=msrun_rec.researcher,
                    date=msrun_rec.date,
                    lc_method=msrun_rec.lc_method,
                    instrument="unknown",
                )
        MSRunSample.objects.get_or_create(
            msrun_sequence=sequences[sequence_key],
            sample=msrun_rec.sample,
        )
    enable_caching_updates()


class Migration(migrations.Migration):

    dependencies = [
        ('DataRepo', '0026_auto_20231201_1537'),
    ]

    operations = [
        migrations.AlterField(
            model_name='msrunsequence',
            name='instrument',
            field=models.CharField(choices=[('HILIC', 'HILIC'), ('QE', 'QE'), ('QE2', 'QE2'), ('QTOF', 'QTOF'), ('unknown', 'unknown')], help_text='The name of the mass spectrometer.', max_length=7),
        ),

        migrations.RunPython(msruns_to_msrunsample_msrunsequence),
    ]
