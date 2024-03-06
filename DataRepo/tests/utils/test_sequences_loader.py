import pandas as pd

from DataRepo.models import LCMethod, MSRunSequence
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.file_utils import read_from_file
from DataRepo.utils.sequences_loader import SequencesLoader


class SequencesLoaderTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    TEST_DF = pd.DataFrame.from_dict(
        {
            "Sequence Number": [1],
            "Operator": ["Xianfeng Zeng"],
            "Date": ["2021-10-19 00:00:00"],
            "Instrument": ["HILIC"],
            "LC Protocol": ["polar-HILIC"],
            "LC Run Length": [25],
            "LC Description": [""],
            "Notes": [""],
        },
    )

    def test_load_data(self):
        df = read_from_file(
            "DataRepo/data/tests/submission_v3/study.xlsx",
            SequencesLoader.DataSheetName,
            dtype=SequencesLoader.header_key_to_name(SequencesLoader.DataColumnTypes),
        )
        sl = SequencesLoader(df=df, file="DataRepo/data/tests/submission_v3/study.xlsx")
        sl.load_data()
        self.assertEqual(3, MSRunSequence.objects.count())
        seq = MSRunSequence.objects.filter(researcher="Xianfeng Zeng").first()

        lcr = seq.lc_method
        self.assertEqual("polar-HILIC-25-min", lcr.name)
        self.assertEqual("polar-HILIC", lcr.type)
        self.assertEqual(25, int(lcr.run_length.total_seconds() / 60))

    def test_get_or_create_lc_method(self):
        for _, row in self.TEST_DF.iterrows():
            break
        sl = SequencesLoader()
        rec, created = sl.get_or_create_lc_method(row)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))
        self.assertFalse(created)
        self.assertEqual("polar-HILIC-25-min", rec.name)

    def test_get_or_create_sequence(self):
        for _, row in self.TEST_DF.iterrows():
            break
        sl = SequencesLoader()
        lcrec = LCMethod.objects.get(name="polar-HILIC-25-min")
        rec, created = sl.get_or_create_sequence(row, lcrec)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))
        self.assertTrue(created)
        self.assertEqual("polar-HILIC-25-min", rec.lc_method.name)
        self.assertEqual("Xianfeng Zeng", rec.researcher)
        self.assertEqual("HILIC", rec.instrument)
