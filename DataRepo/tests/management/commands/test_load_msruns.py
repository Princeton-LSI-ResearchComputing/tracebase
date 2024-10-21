from django.core.management import call_command

from DataRepo.models import LCMethod, MSRunSequence
from DataRepo.models.archive_file import ArchiveFile
from DataRepo.models.msrun_sample import MSRunSample
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
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
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
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
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
                    "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
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
            mzxml_dir="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/",
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
                mzxml_dir="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/",
                operator="Kamala Harris",
                date="2024-11-05",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="HILIC",  # Invalid
            )
        aes: AggregatedErrors = ar.exception
        self.assertEqual(1, len(aes.exceptions))  # Ensure MissingRecords was removed
        self.assertTrue(aes.exception_type_exists(DefaultSequenceNotFound))
