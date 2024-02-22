from copy import deepcopy

import pandas as pd
from django.core.management import call_command
from django.test import tag

from DataRepo.models import Protocol
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import ProtocolsLoader
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueErrors,
    DryRun,
    DuplicateValueErrors,
    InfileDatabaseError,
    RequiredColumnValues,
)


@tag("protocols")
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
        template_headers = ["Name", "Description"]
        # Create the pandas DataFrame
        cls.working_df = pd.DataFrame(data, columns=template_headers)
        cls.working_differently_df = pd.DataFrame(
            data_differently, columns=template_headers
        )

    def load_dataframe_as_animal_treatment(self, df, dry_run=False):
        """Load a working dataframe to protocols table"""
        protocol_loader = ProtocolsLoader(
            df=df,
            dry_run=dry_run,
        )
        protocol_loader.set_defaults(
            custom_defaults={
                ProtocolsLoader.CAT_KEY: Protocol.ANIMAL_TREATMENT,
            },
        )
        protocol_loader.load_data()

    def test_protocols_loader(self):
        """Test the ProtocolsLoader class"""
        self.load_dataframe_as_animal_treatment(self.working_df)
        self.assertEqual(Protocol.objects.count(), self.SETUP_PROTOCOL_COUNT)

    def test_protocols_loader_failing_different_descs(self):
        """Test the ProtocolsLoader class"""
        self.load_dataframe_as_animal_treatment(self.working_df)

        with self.assertRaises(AggregatedErrors) as ar:
            self.load_dataframe_as_animal_treatment(self.working_differently_df)
        aes = ar.exception
        self.assertEqual((1, 0), (aes.num_errors, aes.num_warnings))
        self.assertEqual(ConflictingValueErrors, type(aes.exceptions[0]))
        self.assertIn("[description] values differ:", str(aes.exceptions[0]))
        # but the other first "working" protocols are still there]
        self.assertEqual(Protocol.objects.count(), self.SETUP_PROTOCOL_COUNT)

    def test_protocols_loader_without_category_error(self):
        """Test the ProtocolsLoader with dataframe missing category"""
        # The DefaultValues namedtuple in ProtocolsLoader sets a category default of Protocol.ANIMAL_TREATMENT, so in
        # order to make the error occur, we must set that default to None
        protocol_loader = ProtocolsLoader(self.working_df)
        protocol_loader.set_defaults(
            custom_defaults={
                ProtocolsLoader.CAT_KEY: None,
            },
        )

        with self.assertRaises(AggregatedErrors) as ar:
            protocol_loader.load_data()
        aes = ar.exception
        self.assertEqual(1, aes.num_errors)
        self.assertEqual(
            0,
            aes.num_warnings,
            msg=(
                "There should be 0 warnings (1 exceptions total). Exceptions: "
                f"{', '.join([type(e).__name__ for e in aes.exceptions])}"
            ),
        )
        self.assertEqual(RequiredColumnValues, type(aes.exceptions[0]))
        self.assertIn(
            (
                "Required column values missing on the indicated rows:\n"
                "\tthe load file data\n"
                "\t\tColumn: [Category] on rows: ['2-3']\n"
            ),
            str(aes.exceptions[0]),
        )
        # If errors are found, no records should be loaded
        self.assertEqual(0, Protocol.objects.count())

    def test_protocols_loader_with_bad_category_error(self):
        """Test the ProtocolsLoader with an improper category"""
        protocol_loader = ProtocolsLoader(
            self.working_df,
        )
        protocol_loader.set_defaults(
            custom_defaults={
                ProtocolsLoader.CAT_KEY: "Some Nonsense Category",
            }
        )
        with self.assertRaises(AggregatedErrors) as ar:
            protocol_loader.load_data()
        aes = ar.exception
        self.assertEqual(1, aes.num_errors)
        self.assertEqual(0, aes.num_warnings)
        self.assertEqual(InfileDatabaseError, type(aes.exceptions[0]))
        self.assertIn("category", str(aes.exceptions[0]))
        self.assertIn("is not a valid choice", str(aes.exceptions[0]))
        # If errors are found, no records should be loaded
        self.assertEqual(0, Protocol.objects.count())

    def test_load_protocols_tsv(self):
        """Test loading the protocols from a TSV containing previously loaded data"""
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/protocols/protocols.tsv",
        )
        self.assertEqual(Protocol.objects.count(), 8)
        # all of these were animal treatments
        self.assertEqual(
            Protocol.objects.filter(category=Protocol.ANIMAL_TREATMENT).count(), 8
        )

    def test_load_protocols_xlxs(self):
        """Test loading the protocols from a Treatments sheet in the xlxs workbook"""
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table.xlsx",
        )
        self.assertEqual(Protocol.objects.count(), 2)
        # and these are all animal treatments
        self.assertEqual(
            Protocol.objects.filter(category=Protocol.ANIMAL_TREATMENT).count(), 2
        )

    def test_load_protocols_xlxs_validation(self):
        """Test loading the protocols from a Treatments sheet in the xlxs workbook"""
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table.xlsx",
            dry_run=True,
        )
        # none in default
        self.assertEqual(Protocol.objects.count(), 0)

    def test_load_protocols_tsv_with_workarounds(self):
        """Test loading the protocols from a TSV containing duplicates and mungeable data"""
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/protocols/protocols_with_workarounds.tsv",
        )
        self.assertEqual(Protocol.objects.count(), 1)
        # test data trimming
        self.assertEqual(Protocol.objects.filter(name="trimmed treatment").count(), 1)
        self.assertEqual(
            Protocol.objects.filter(description="trimmed description").count(), 1
        )

    def test_load_protocols_with_bad_examples(self):
        """Test loading the protocols from a TSV containing questionable data"""
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_protocols",
                infile="DataRepo/data/tests/protocols/protocols_with_errors.tsv",
            )
        aes = ar.exception

        self.assertEqual((2, 0), (aes.num_errors, aes.num_warnings))

        self.assertEqual(DuplicateValueErrors, type(aes.exceptions[0]))
        self.assertIn("treatment 1 (rows*: 2-3)", str(aes.exceptions[0]))

        self.assertEqual(RequiredColumnValues, type(aes.exceptions[1]))
        self.assertIn("Column: [Name] on rows: ['4']", str(aes.exceptions[1]))
        # The defaults namedtuple (containing a default category) should avoid this error.
        self.assertNotIn(
            "Column: [Category] on rows: ['5']",
            str(aes.exceptions[1]),
        )

        # and no protocols should be loaded
        self.assertEqual(Protocol.objects.count(), 0)

    def test_protocol_load_in_debug(self):
        pre_load_counts = self.get_record_counts()

        with self.assertRaises(DryRun):
            self.load_dataframe_as_animal_treatment(self.working_df, dry_run=True)

        post_load_counts = self.get_record_counts()

        self.assertEqual(
            pre_load_counts,
            post_load_counts,
            msg="DryRun mode doesn't change any table's record count.",
        )
