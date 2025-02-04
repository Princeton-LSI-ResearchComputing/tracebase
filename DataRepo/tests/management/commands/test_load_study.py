from django.conf import settings
from django.core.management import call_command
from django.test import override_settings

from DataRepo.models.peak_data import PeakData
from DataRepo.models.peak_group import PeakGroup
from DataRepo.models.utilities import get_all_models
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
class LoadStudyTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

    @classmethod
    def get_record_counts(cls):
        record_counts = {}
        for mdl in get_all_models():
            record_counts[mdl.__name__] = mdl.objects.all().count()
        return record_counts

    def test_load_study_v3_command(self):
        call_command(
            "load_study", infile="DataRepo/data/tests/study_doc_versions/study_v3.xlsx"
        )
        expected_counts = {
            "Compound": 17,
            "CompoundSynonym": 74,
            "LCMethod": 1,
            "Tissue": 36,
            "PeakDataLabel": 380,
            "PeakData": 200,
            "PeakGroup": 20,
            "PeakGroupLabel": 36,
            "MSRunSample": 6,
            "MSRunSequence": 2,
            "ArchiveFile": 2,
            "DataType": 2,
            "DataFormat": 6,
            "FCirc": 4,
            "Sample": 6,
            "Animal": 3,
            "AnimalLabel": 6,
            "TracerLabel": 10,
            "Tracer": 5,
            "Infusate": 3,
            "InfusateTracer": 5,
            "Protocol": 1,
            "Study": 1,
        }
        load_counts = self.get_record_counts()
        self.assertDictEqual(expected_counts, load_counts)

    def test_load_study_v2_command(self):
        # First, let's load all of the common/consolidated data that v2 of the study doc doesn't support
        call_command(
            "load_study",
            infile="DataRepo/data/tests/study_doc_versions/consolidated.xlsx",
        )
        call_command(
            "load_study", infile="DataRepo/data/tests/study_doc_versions/study_v2.xlsx"
        )
        # Note, the v2 study doc load will not load peak annotation files, so we don't need to worry about those. Nor
        # does V2 support loading of MSRunSequence or MSRunSample, but we don't need to pre-load those, because they're
        # only needed to load the peak annotation files.

    def test_load_small_obob_study(self):
        call_command(
            "load_lcprotocols",
            infile="DataRepo/data/tests/study_doc_versions/study_v3.xlsx",
        )
        call_command(
            "load_study",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table_blank_sample.xlsx"
            ),
        )
        COMPOUNDS_COUNT = 2
        SAMPLES_COUNT = 14
        PEAKDATA_ROWS = 11

        self.assertEqual(
            PeakGroup.objects.all().count(), COMPOUNDS_COUNT * SAMPLES_COUNT
        )
        self.assertEqual(PeakData.objects.all().count(), PEAKDATA_ROWS * SAMPLES_COUNT)
