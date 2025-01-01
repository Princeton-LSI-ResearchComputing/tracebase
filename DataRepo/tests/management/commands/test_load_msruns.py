from pathlib import Path

from django.core.management import call_command

from DataRepo.models import LCMethod, MSRunSequence
from DataRepo.models.archive_file import ArchiveFile
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.msrun_sample import MSRunSample
from DataRepo.models.peak_group import PeakGroup
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConditionallyRequiredOptions,
    DefaultSequenceNotFound,
)
from DataRepo.utils.file_utils import string_to_date


class LoadMSRunsCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/mzxml_study_doc.xlsx",
        )

    def test_conditionally_required_options_all_custom_opts(self):
        MSRunSequence.objects.create(
            researcher="George Santos",
            date=string_to_date("2024-05-02"),
            lc_method=LCMethod.objects.get(name="polar-HILIC-25-min"),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
        )
        call_command(
            "load_msruns",
            mzxml_files=[
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
            ],
            operator="George Santos",
            date="2024-05-02",
            lc_protocol_name="polar-HILIC-25-min",
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
        )
        # This does produce a warning about no --infile data, but that's expected
        # No exception = success

    def test_conditionally_required_options_defaults_file(self):
        MSRunSequence.objects.create(
            researcher="Xianfeng Zeng",
            date=string_to_date("2020-11-01"),
            lc_method=LCMethod.objects.get(name="polar-HILIC-25-min"),
            instrument="QE",
        )
        call_command(
            "load_msruns",
            mzxml_files=[
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
            ],
            defaults_file="DataRepo/data/tests/submission_v3/defaults.tsv",
        )
        # This does produce a warning about no --infile data, but that's expected
        # No exception = success

    def test_conditionally_required_options_missing_instrument(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_msruns",
                mzxml_files=[
                    "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
                ],
                operator="George Santos",
                date="2024-05-02",
                lc_protocol_name="polar-HILIC-25-min",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(ConditionallyRequiredOptions, type(aes.exceptions[0]))
        self.assertIn("['instrument']", str(aes.exceptions[0]))

    def test_conditionally_required_options_missing_mzxml(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_msruns",
                operator="George Santos",
                date="2024-05-02",
                lc_protocol_name="polar-HILIC-25-min",
                instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
            )
        aes = ar.exception
        nl = "\n"
        self.assertEqual(
            1,
            len(aes.exceptions),
            msg=(
                "Should be 1 ConditionallyRequiredOptions exception in: "
                f"{nl.join([type(e).__name__ + ': ' + str(e) for e in aes.exceptions])}"
            ),
        )
        self.assertEqual(ConditionallyRequiredOptions, type(aes.exceptions[0]))
        self.assertIn(
            "--mzxml-dir (with a directory containing mzxml files), --mzxml-files, or --infile",
            str(aes.exceptions[0]),
        )

    def test_mzxml_dir(self):
        seq = MSRunSequence.objects.get()
        call_command(
            "load_msruns",
            mzxml_dir="DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/",
            operator="Kamala Harris",
            date="2024-11-05",
            lc_protocol_name="polar-HILIC-25-min",
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
        )
        self.assertEqual(4, ArchiveFile.objects.count())  # 2 mzxmls and 2 raw
        self.assertEqual(
            2, MSRunSample.objects.filter(msrun_sequence=seq).count()
        )  # One for each file

    def test_wrong_instrument(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_msruns",
                mzxml_dir="DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/",
                operator="Kamala Harris",
                date="2024-11-05",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="HILIC",  # Invalid
            )
        aes: AggregatedErrors = ar.exception
        self.assertEqual(1, len(aes.exceptions))  # Ensure MissingRecords was removed
        self.assertTrue(aes.exception_type_exists(DefaultSequenceNotFound))


# Transferred and converted from test_load_accucor_msruns
class MSRunSampleSequenceTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/glucose/small_obob_animal_and_sample_table.xlsx",
        )

        cls.MSRUNSAMPLE_COUNT = 4  # 2 concrete and 2 placeholder
        cls.MSRUNSEQUENCE_COUNT = 1

        super().setUpTestData()

    @MaintainedModel.no_autoupdates()
    def test_msrunsample_and_msrunsequence_are_loaded(self):
        """
        Issue #712
        Requirement: 2. accucor_data_loader loads MSRunSample and MSRunSequence
        NOTE: This should also ensure that ms_data_file and ms_raw_file are loaded and that the files are archived
        """
        self.assertEqual(self.MSRUNSAMPLE_COUNT, MSRunSample.objects.count())
        self.assertEqual(self.MSRUNSEQUENCE_COUNT, MSRunSequence.objects.count())

        # mzXML ArchiveFile records exist
        ArchiveFile.objects.get(filename="BAT-xz971.mzXML")
        ArchiveFile.objects.get(filename="Br-xz971.mzXML")

        # Files exist in the archive
        batmz_rec = ArchiveFile.objects.get(filename="BAT-xz971.mzXML")
        brmz_rec = ArchiveFile.objects.get(filename="Br-xz971.mzXML")
        batmz_loc = Path(str(batmz_rec.file_location))
        brmz_loc = Path(str(brmz_rec.file_location))
        # Reading the file instead of using .is_file() due to the usage of InMemoryStorage for tests
        self.assertEqual(
            829,
            len(batmz_rec.file_location.read()),
            msg=f"{str(batmz_loc)} must be a real file",
        )
        self.assertEqual(
            828,
            len(brmz_rec.file_location.read()),
            msg=f"{str(brmz_loc)} must be a real file",
        )

        # RAW ArchiveFile records exist
        batraw_rec = ArchiveFile.objects.get(filename="BAT-xz971.raw")
        brraw_rec = ArchiveFile.objects.get(filename="Br-xz971.raw")

        # Checksums are correct
        self.assertEqual(
            "31bc554534cf9f1e568529d110caa85f1fd0a8c8", batraw_rec.checksum
        )
        self.assertEqual("a129d2228d5a693875d2bb03fb03830becdeecc1", brraw_rec.checksum)

        # Raw files do not exist
        batraw_loc = Path(str(batraw_rec.file_location))
        brraw_loc = Path(str(brraw_rec.file_location))
        self.assertFalse(batraw_loc.is_file())
        self.assertFalse(brraw_loc.is_file())

    def test_peakgroup_msrunsample_null_is_false(self):
        """
        Issue #712
        Requirement: 3. PeakGroup.msrun_sample.null must be set to False
        Requirement: 4. Add migration for PeakGroup.msrun_sample change
        """
        self.assertFalse(PeakGroup.msrun_sample.__dict__["field"].null)

    # NOTE: Test for Issue #712, Requirement 5 (All broken_until_issue712 test tags must be removed) is unnecessary
    # NOTE: Test for Issue #712, Requirement 6 is in test_exceptions.py

    def test_polarity_choices_includes_unknown(self):
        """
        Issue #712
        Requirement: 7.1. Add a polarity choices value: "unknown"
        """
        choices = [
            ("unknown", "unknown"),  # This is the one essential for the test
            ("positive", "positive"),  # The others are a bonus check
            ("negative", "negative"),
        ]
        self.assertEqual(choices, MSRunSample.POLARITY_CHOICES)

    # NOTE: Test for Issue #712, Requirement 7.2 was moved to test_msruns_loader.py

    # NOTE: Test for Issue #712, Requirement 7.3 (A default polarity should be removed from the study submission form)
    # is unnecessary

    # NOTE: Test for Issue #712, Requirement 7.5 (A default polarity should be removed from the study submission form)
    # is unnecessary

    # NOTE: test_msruns_loader.MSRunsLoaderTests.test_get_or_create_mzxml_and_raw_archive_files_str exists, so I don't
    # need to transfer test: test_load_accucor_msruns.MSRunSampleSequenceTests.test_get_or_create_raw_file

    # NOTE: test_load_accucor_msruns.MSRunSampleSequenceTests.test_hash_file_allow_missing_true (and _false) are for a
    # method that moved to the ArchiveFile model class, for which there already exists tests, so no need to transfer.
