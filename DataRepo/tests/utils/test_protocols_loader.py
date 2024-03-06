import pandas as pd

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.protocols_loader import ProtocolsLoader


class ProtocolsLoaderTests(TracebaseTestCase):
    def test_get_pretty_headers(self):
        pl = ProtocolsLoader()
        self.assertEqual(
            (
                "[Name*, Category, Description*] (or, if the input file is an excel file: [Animal Treatment*, "
                "Treatment Description*]) (* = Required)"
            ),
            pl.get_pretty_headers(),
        )

    def test_set_headers_default(self):
        pl = ProtocolsLoader()
        # Headers are the class defaults
        expected = pl.DataTableHeaders(
            NAME="Name",
            CATEGORY="Category",
            DESCRIPTION="Description",
        )
        self.assertEqual(expected, pl.headers)

    def test_set_headers_excel(self):
        df = pd.DataFrame.from_dict(
            {
                "Animal Treatment": ["no treatment"],
                "Treatment Description": [
                    "No treatment was applied to the animal.  Animal was maintained on normal diet (LabDiet #5053 "
                    '"Maintenance"), housed at room temperature with a normal light cycle.'
                ],
            },
        )
        pl = ProtocolsLoader(df=df, file="DataRepo/data/tests/submission_v3/study.xlsx")
        expected = pl.DataTableHeaders(
            NAME="Animal Treatment",
            CATEGORY="Category",
            DESCRIPTION="Treatment Description",
        )
        # Headers are the class defaults
        self.assertEqual(expected, pl.headers)
