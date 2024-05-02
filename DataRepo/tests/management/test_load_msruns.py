from django.core.management import call_command

from DataRepo.models.msrun_sequence import MSRunSequence
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConditionallyRequiredOptions,
)


class LoadMSRunsCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    def test_conditionally_required_options_all_custom_opts(self):
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
        # No exception = success

    def test_conditionally_required_options_defaults_file(self):
        call_command(
            "load_msruns",
            mzxml_files=[
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
            ],
            defaults_file="DataRepo/data/tests/submission_v3/defaults.tsv",
        )
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
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(ConditionallyRequiredOptions, type(aes.exceptions[0]))
        self.assertIn("['mzxml_files', 'infile']", str(aes.exceptions[0]))
