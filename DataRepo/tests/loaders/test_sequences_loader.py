import pandas as pd

from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.models import LCMethod, MSRunSequence
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.file_utils import read_from_file


class SequencesLoaderTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    TEST_DF = pd.DataFrame.from_dict(
        {
            "Sequence Name": ["Xianfeng Zeng, polar-HILIC-25-min, QE2, 10/19/2021"],
            "Operator": ["Xianfeng Zeng"],
            "Date": ["2021-10-19 00:00:00"],
            "Instrument": ["QE2"],
            "LC Protocol Name": ["polar-HILIC-25-min"],
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

        self.assertEqual("polar-HILIC-25-min", seq.lc_method.name)

    def test_get_lc_method(self):
        sl = SequencesLoader()
        rec = sl.get_lc_method("polar-HILIC-25-min")
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))
        self.assertEqual("polar-HILIC-25-min", rec.name)

    def test_get_or_create_sequence(self):
        _, row = next(self.TEST_DF.iterrows())
        sl = SequencesLoader()
        lcrec = LCMethod.objects.get(name="polar-HILIC-25-min")
        rec, created = sl.get_or_create_sequence(row, lcrec)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))
        self.assertTrue(created)
        self.assertEqual("polar-HILIC-25-min", rec.lc_method.name)
        self.assertEqual("Xianfeng Zeng", rec.researcher)
        self.assertEqual("QE2", rec.instrument)
