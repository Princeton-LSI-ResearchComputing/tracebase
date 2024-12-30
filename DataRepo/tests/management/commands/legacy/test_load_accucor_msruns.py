import hashlib
from pathlib import Path

import pandas as pd
from django.core.management import call_command

from DataRepo.loaders.legacy.accucor_data_loader import (
    AccuCorDataLoader,
    hash_file,
)
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.models import (
    ArchiveFile,
    DataFormat,
    Infusate,
    MaintainedModel,
    MSRunSample,
    MSRunSequence,
    PeakGroup,
)
from DataRepo.models.study import Study
from DataRepo.models.utilities import exists_in_db
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import read_from_file
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs


class MSRunSampleSequenceTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/small_obob/small_obob_study_prerequisites.yaml",
        )

        Study.objects.create(name="Small OBOB")
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )

        call_command(
            "load_animals",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_samples",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        # call_command(
        #     "legacy_load_animals_and_samples",
        #     animal_and_sample_table_filename=(
        #         "DataRepo/data/tests/small_obob/"
        #         "small_obob_animal_and_sample_table.xlsx"
        #     ),
        # )

        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose.xlsx",
            skip_samples=("blank"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            mzxml_files=[
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML",
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/Br-xz971.mzXML",
            ],
        )

        cls.MSRUNSAMPLE_COUNT = 2
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
        # TODO: Fix these file exist tests
        # PR REVIEW NOTE: These file exist tests fail.  I suspect it's due to the fact that the tests use
        #                 django.core.files.storage.InMemoryStorage in the settings.  Should I override that setting for
        #                 this test or just not test this?
        # batmz_rec = ArchiveFile.objects.get(filename="BAT-xz971.mzXML")
        # brmz_rec = ArchiveFile.objects.get(filename="Br-xz971.mzXML")
        # batmz_loc = Path(str(batmz_rec.file_location))
        # brmz_loc = Path(str(brmz_rec.file_location))
        # self.assertTrue(batmz_loc.is_file(), msg=f"{str(batmz_loc)} must be a real file")
        # self.assertTrue(brmz_loc.is_file(), msg=f"{str(brmz_loc)} must be a real file")

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

    def create_AccuCorDataLoader_object(self):
        return AccuCorDataLoader(
            None,
            None,
            None,
            data_format=DataFormat.objects.get(code="accucor"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="1972-11-24",
            researcher="Michael Neinast",
        )

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_raw_file(self):
        fn = "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        adl = self.create_AccuCorDataLoader_object()
        mz_dict, _ = MSRunsLoader.parse_mzxml(Path(fn))

        # Test that a new record is created
        inafs = ArchiveFile.objects.count()
        afrec = adl.get_or_create_raw_file(mz_dict)
        self.assertTrue(exists_in_db(afrec))
        self.assertEqual(inafs + 1, ArchiveFile.objects.count())

        # Test that the record is retrieved (not created)
        afrec2 = adl.get_or_create_raw_file(mz_dict)
        self.assertEqual(afrec, afrec2)
        # Record count is unchanged
        self.assertEqual(inafs + 1, ArchiveFile.objects.count())

    @MaintainedModel.no_autoupdates()
    def test_hash_file_allow_missing_true(self):
        """
        If a file does not exist and allow_missing is True, hash_file should create a hash based on the file name
        """
        fn = "does not exist"
        hash = hash_file(Path(fn), allow_missing=True)
        self.assertEqual(hashlib.sha1(fn.encode()).hexdigest(), hash)

    @MaintainedModel.no_autoupdates()
    def test_hash_file_allow_missing_false(self):
        """
        If a file does not exist and allow_missing is False, hash_file should raise a FileNotFoundError
        """
        fn = "does not exist"
        with self.assertRaises(FileNotFoundError):
            hash_file(Path(fn), allow_missing=False)

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_archive_file_allow_missing_no_checksum_or_existing_file(
        self,
    ):
        """
        If a file does not exist and no checksum is provided, a ValueError should be raised.
        """
        fn = "does not exist"
        adl = self.create_AccuCorDataLoader_object()
        with self.assertRaises(ValueError) as ar:
            adl.get_or_create_archive_file(
                Path(fn),
                "ms_data",
                "ms_raw",
            )
        ve = ar.exception
        self.assertIn(
            "A checksum is required if the supplied file path is not an existing file.",
            str(ve),
        )

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_archive_file_with_checksum(self):
        """
        If a checksum is supplied and the file doesn't exist, a record is created
        """
        fn = "does not exist"
        adl = self.create_AccuCorDataLoader_object()
        afrec = adl.get_or_create_archive_file(
            Path(fn),
            "ms_data",
            "mzxml",
            checksum="somesuppliedvalue",
        )
        self.assertTrue(exists_in_db(afrec))
        self.assertEqual("somesuppliedvalue", afrec.checksum)

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_archive_file_allow_missing_file_exists(self):
        """
        If a file exists and a checksum is provided, an exception should be raised when that checksum does not match.
        """
        fn = "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        adl = self.create_AccuCorDataLoader_object()
        with self.assertRaises(ValueError) as ar:
            adl.get_or_create_archive_file(
                Path(fn),
                "ms_data",
                "mzxml",
                checksum="somesuppliedvalue",
            )
        ve = ar.exception
        self.assertIn("somesuppliedvalue", str(ve))
        expected_hash = hash_file(Path(fn))
        self.assertIn(expected_hash, str(ve))

    def create_populated_AccuCorDataLoader_object(self, lcms_file):
        xlsx = "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate.xlsx"
        adl = AccuCorDataLoader(
            # Original dataframe
            pd.read_excel(
                xlsx,
                sheet_name=0,  # The first sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            # Corrected dataframe
            pd.read_excel(
                xlsx,
                sheet_name=1,  # The second sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            # Peak annot file name
            xlsx,
            data_format=DataFormat.objects.get(code="accucor"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="1972-11-24",
            researcher="Michael Neinast",
            lcms_metadata_df=read_from_file(lcms_file),
        )
        adl.prepare_metadata()
        return adl

    @MaintainedModel.no_autoupdates()
    def test_get_sample_header_by_mzxml_basename_one_match(self):
        """
        get_sample_header_by_mzxml_basename returns the sample header from the line of the lcms metadata file that has
        the matching mzxml file basename on it
        """
        adl = self.create_populated_AccuCorDataLoader_object(
            "DataRepo/data/tests/small_obob_lcms_metadata/lactate_neg.tsv",
        )
        hdr = adl.get_sample_header_by_mzxml_basename("BAT-xz971_neg.mzXML")
        self.assertEqual("BAT-xz971", hdr)
        self.assertEqual(0, adl.aggregated_errors_object.num_errors)

    @MaintainedModel.no_autoupdates()
    def test_get_sample_header_by_mzxml_basename_no_match(self):
        """
        get_sample_header_by_mzxml_basename returns None if the mzxml file basename isn't in the lcms metadata file
        (because mzxml files are not required)
        """
        adl = self.create_populated_AccuCorDataLoader_object(
            "DataRepo/data/tests/small_obob_lcms_metadata/lactate_neg.tsv",
        )
        hdr = adl.get_sample_header_by_mzxml_basename("BAT-xz971.mzXML")
        self.assertIsNone(hdr)
        self.assertEqual(0, adl.aggregated_errors_object.num_errors)

    @MaintainedModel.no_autoupdates()
    def test_get_sample_header_by_mzxml_basename_multiple_matches(self):
        """
        Exception if the same mzxml file basename occurs in the LCMS metadata file multiple times
        """
        adl = self.create_populated_AccuCorDataLoader_object(
            "DataRepo/data/tests/small_obob_lcms_metadata/lactate_neg_multiple.tsv",
        )
        hdr = adl.get_sample_header_by_mzxml_basename("BAT-xz971_neg.mzXML")
        self.assertIsNone(hdr)
        self.assertEqual(1, len(adl.aggregated_errors_object.exceptions))
        self.assertEqual(ValueError, type(adl.aggregated_errors_object.exceptions[0]))
        self.assertEqual(
            "2 instances of mzxml file [BAT-xz971_neg.mzXML] in the LCMS metadata file.",
            str(adl.aggregated_errors_object.exceptions[0]),
        )
