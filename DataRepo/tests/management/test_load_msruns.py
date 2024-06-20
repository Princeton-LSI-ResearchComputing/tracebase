from datetime import datetime, timedelta

from django.core.management import call_command

from DataRepo.models import (
    Animal,
    Infusate,
    LCMethod,
    MSRunSequence,
    Sample,
    Tissue,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConditionallyRequiredOptions,
)
from DataRepo.utils.file_utils import string_to_datetime


class LoadMSRunsCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        inf = Infusate()
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
            name="BAT-xz971",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )

    def test_conditionally_required_options_all_custom_opts(self):
        MSRunSequence.objects.create(
            researcher="George Santos",
            date=string_to_datetime("2024-05-02"),
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
            date=string_to_datetime("2020-11-01"),
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
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(ConditionallyRequiredOptions, type(aes.exceptions[0]))
        self.assertIn("['mzxml_files', 'infile']", str(aes.exceptions[0]))
