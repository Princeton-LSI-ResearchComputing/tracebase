import pandas as pd

from DataRepo.loaders.lcprotocols_loader import LCProtocolsLoader
from DataRepo.models import LCMethod
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.file_utils import read_from_file


class LCProtocolsLoaderTests(TracebaseTestCase):
    TEST_DF = pd.DataFrame.from_dict(
        {
            "Name": ["polar-HILIC-25-min"],
            "LC Protocol": ["polar-HILIC"],
            "Run Length": [25],
            "Description": ["This is a Polar HILIC description"],
        },
    )

    def test_load_data(self):
        df = read_from_file(
            "DataRepo/data/tests/submission_v3/study.xlsx",
            LCProtocolsLoader.DataSheetName,
            dtype=LCProtocolsLoader.header_key_to_name(
                LCProtocolsLoader.DataColumnTypes
            ),
        )
        ll = LCProtocolsLoader(
            df=df, file="DataRepo/data/tests/submission_v3/study.xlsx"
        )
        ll.load_data()
        self.assertEqual(1, LCMethod.objects.count())
        lcr = LCMethod.objects.first()
        self.assertEqual("polar-HILIC-25-min", lcr.name)

    def test_get_or_create_lc_method(self):
        for _, row in self.TEST_DF.iterrows():
            break
        ll = LCProtocolsLoader()
        rec, created = ll.get_or_create_lc_method(row)
        self.assertEqual(0, len(ll.aggregated_errors_object.exceptions))
        self.assertTrue(created)
        self.assertEqual("polar-HILIC-25-min", rec.name)
