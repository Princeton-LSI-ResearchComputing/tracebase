from copy import deepcopy

import pandas as pd
from django.conf import settings
from django.core.management import CommandError, call_command
from django.test import tag

from DataRepo.models import Protocol
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import ProtocolsLoader
from DataRepo.utils.exceptions import LoadingError


@tag("protocols")
@tag("multi_working")
class ProtocolLoadingTests(TracebaseTestCase):
    """Test ProtocolsLoader"""

    @classmethod
    def setUpTestData(cls):

        # initialize list of lists
        data = [
            ["no treatment", "No treatment was applied to the animal."],
            ["some treatment", "Animal was challenged."],
        ]
        cls.SETUP_PROTOCOL_COUNT = 2
        data_differently = deepcopy(data)
        # change the description
        data_differently[1][1] = "Animal was treated differently."
        template_headers = ["name", "description"]
        # Create the pandas DataFrame
        cls.working_df = pd.DataFrame(data, columns=template_headers)
        cls.working_differently_df = pd.DataFrame(
            data_differently, columns=template_headers
        )

    def load_dataframe_as_animal_treatment(self, df):
        """Load a working dataframe to protocols table"""
        protocol_loader = ProtocolsLoader(
            protocols=df,
            category=Protocol.ANIMAL_TREATMENT,
            dry_run=False,
        )
        protocol_loader.load()

    def test_protocols_loader(self):
        """Test the ProtocolsLoader class"""
        self.load_dataframe_as_animal_treatment(self.working_df)
        self.assertEqual(Protocol.objects.count(), self.SETUP_PROTOCOL_COUNT)

    def test_protocols_loader_failing_different_descs(self):
        """Test the ProtocolsLoader class"""
        self.load_dataframe_as_animal_treatment(self.working_df)

        with self.assertRaisesRegex(
            LoadingError, r"Key \(name\)=\(some treatment\) already exists."
        ):
            self.load_dataframe_as_animal_treatment(self.working_differently_df)

        # but the other first "working" protocols are still there]
        self.assertEqual(Protocol.objects.count(), self.SETUP_PROTOCOL_COUNT)

    def test_protocols_loader_without_category_error(self):
        """Test the ProtocolsLoader with dataframe missing category"""
        protocol_loader = ProtocolsLoader(protocols=self.working_df)

        with self.assertRaisesRegex(LoadingError, "Errors during protocol loading"):
            protocol_loader.load()
        # If errors are found, no records should be loaded
        self.assertEqual(Protocol.objects.count(), 0)

    def test_protocols_loader_with_bad_category_error(self):
        """Test the ProtocolsLoader with an improper category"""
        protocol_loader = ProtocolsLoader(
            protocols=self.working_df,
            category="Some Nonsense Category",
        )
        with self.assertRaisesRegex(LoadingError, "Errors during protocol loading"):
            protocol_loader.load()
        # If errors are found, no records should be loaded
        self.assertEqual(Protocol.objects.count(), 0)

    def test_load_protocols_tsv(self):
        """Test loading the protocols from a TSV containing previously loaded data"""
        call_command(
            "load_protocols",
            protocols="DataRepo/example_data/protocols/protocols.tsv",
        )
        self.assertEqual(Protocol.objects.count(), 16)
        # a few of these were msrun protocols
        self.assertEqual(
            Protocol.objects.filter(category=Protocol.MSRUN_PROTOCOL).count(), 8
        )

    def test_load_protocols_xlxs(self):
        """Test loading the protocols from a Treatments sheet in the xlxs workbook"""
        call_command(
            "load_protocols",
            protocols="DataRepo/example_data/small_dataset/small_obob_animal_and_sample_table.xlsx",
        )
        self.assertEqual(Protocol.objects.count(), 2)
        # and these are all animal treatments
        self.assertEqual(
            Protocol.objects.filter(category=Protocol.ANIMAL_TREATMENT).count(), 2
        )

    def test_load_protocols_xlxs_validation(self):
        """Test loading the protocols from a Treatments sheet in the xlxs workbook"""
        val_db = settings.VALIDATION_DB
        call_command(
            "load_protocols",
            protocols="DataRepo/example_data/small_dataset/small_obob_animal_and_sample_table.xlsx",
            database=val_db,
        )
        self.assertEqual(Protocol.objects.using(val_db).count(), 2)
        # and none in default
        self.assertEqual(Protocol.objects.count(), 0)

    def test_load_protocols_tsv_with_workarounds(self):
        """Test loading the protocols from a TSV containing duplicates and mungeable data"""
        call_command(
            "load_protocols",
            protocols="DataRepo/example_data/testing_data/protocols/protocols_with_workarounds.tsv",
        )
        # two protocols loaded, but 3 lines in file (1 redundatn)
        self.assertEqual(Protocol.objects.count(), 2)
        # test data trimming
        self.assertEqual(Protocol.objects.filter(name="trimmed treatment").count(), 1)
        self.assertEqual(
            Protocol.objects.filter(description="trimmed description").count(), 1
        )

    def test_load_protocols_with_bad_examples(self):
        """Test loading the protocols from a TSV containing questionable data"""
        with self.assertRaisesRegex(
            CommandError,
            r"3 errors loading protocol records from .*protocols_with_errors\.tsv - NO RECORDS SAVED",
        ):
            call_command(
                "load_protocols",
                protocols="DataRepo/example_data/testing_data/protocols/protocols_with_errors.tsv",
            )
        # and no protocols should be loaded
        self.assertEqual(Protocol.objects.count(), 0)
