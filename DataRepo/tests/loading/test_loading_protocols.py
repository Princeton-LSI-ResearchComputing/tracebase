import pandas as pd
from django.test import tag

from DataRepo.models import Protocol
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import ProtocolsLoader
from DataRepo.utils.exceptions import LoadingError


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
        template_headers = ["name", "description"]
        # Create the pandas DataFrame
        cls.working_df = pd.DataFrame(data, columns=template_headers)

    def test_protocols_loader(self):
        """Test the ProtocolsLoader class"""
        protocol_loader = ProtocolsLoader(
            protocols=self.working_df,
            category=Protocol.ANIMAL_TREATMENT,
        )

        protocol_loader.load()
        self.assertEqual(Protocol.objects.count(), 2)

    def test_protocols_loader_without_category_error(self):
        """Test the ProtocolsLoader with dataframe missing category"""
        protocol_loader = ProtocolsLoader(protocols=self.working_df)

        with self.assertRaisesRegex(LoadingError, "Errors during protocol loading"):
            protocol_loader.load()
        # If errors are found, no records should be loaded
        self.assertEqual(Protocol.objects.count(), 0)

    def test_protocols_loader_without_bad_category_error(self):
        """Test the ProtocolsLoader with an improper category"""
        protocol_loader = ProtocolsLoader(
            protocols=self.working_df,
            category="Some Nonsense Category",
        )
        with self.assertRaisesRegex(LoadingError, "Errors during protocol loading"):
            protocol_loader.load()
        # If errors are found, no records should be loaded
        self.assertEqual(Protocol.objects.count(), 0)
