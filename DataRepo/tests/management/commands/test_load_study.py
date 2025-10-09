from datetime import datetime, timedelta

import pandas as pd
from django.conf import settings
from django.core.management import call_command
from django.test import override_settings

from DataRepo.models import (
    Animal,
    ArchiveFile,
    Compound,
    Infusate,
    PeakData,
    PeakGroup,
    Sample,
    Tissue,
)
from DataRepo.models.utilities import get_all_models
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs


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

    def test_get_dataframe(self):
        """Asserts that the '1' in the Label Positions column of the Tracers sheet is read in as a string and not an
        int.  This basically tests that code in get_dataframe correctly compiles a dtype dict and passes it on to
        read_from_file so that pandas doesn't dynamically assign a type it thinks the column is, when we know that the
        type should be something else.  In this case, the Label Positions should be a string because we call .split on
        it to divide comma-separated values."""
        from DataRepo.management.commands.load_study import Command

        lsc = Command()
        lsc.options = {
            "infile": "DataRepo/data/tests/load_study/single_tracer_label_position.xlsx"
        }
        df_dict = lsc.get_dataframe()
        pd.testing.assert_frame_equal(
            pd.DataFrame.from_dict(
                {
                    "Tracer Row Group": [1],
                    "Compound": ["creatine"],
                    "Mass Number": [13],
                    "Element": ["C"],
                    "Label Count": [1],
                    "Label Positions": ["1"],  # Ensure this is a string!
                    "Tracer Name": ["creatine-[1-13C1]"],
                },
            ),
            df_dict["Tracers"],
            check_like=True,
        )

    def create_sample_record_for_mzxml_tests(self):
        """This creates supporting records for tests test_init_mzxml_files_default and test_init_skip_mzxml_files"""

        call_command("loaddata", "lc_methods")
        Compound.objects.create(
            name="gluc",
            formula="C6H12O6",
            hmdb_id="HMDB0000122",
        )
        infobj = parse_infusate_name_with_concs("gluc-[13C6][10]")
        inf, _ = Infusate.objects.get_or_create_infusate(infobj)
        inf.save()
        anml = Animal.objects.create(
            name="test_animal",
            age=timedelta(weeks=int(13)),
            sex="M",
            genotype="WT",
            body_weight=200,
            diet="normal",
            feeding_status="fed",
            infusate=inf,
        )
        tsu = Tissue.objects.create(name="Brain")
        Sample.objects.create(
            # This is the sample name in DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx
            name="Sample Name",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )

    def test_init_mzxml_files_default(self):
        """This test ensures that mzXML files are found and loaded by default"""
        # Load data required by the test
        self.create_sample_record_for_mzxml_tests()
        # Run the loader
        call_command(
            "load_study",
            infile="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",
            # Excluding the peak annot files, since it's expensive and unnecessary for this test
            exclude_sheets=["Peak Annotation Files"],
        )
        # NOTE: This count includes the 1 raw file and 2 mzXMLs.
        self.assertEqual(3, ArchiveFile.objects.count())

    def test_init_skip_mzxml_files(self):
        """This test ensures that mzXML file loads can be skipped"""
        # Load data required by the test
        self.create_sample_record_for_mzxml_tests()
        # Run the loader
        call_command(
            "load_study",
            infile="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",
            # Excluding the peak annot files, since it's expensive and unnecessary for this test
            exclude_sheets=["Peak Annotation Files"],
            skip_mzxmls=True,
        )
        self.assertEqual(0, ArchiveFile.objects.count())
