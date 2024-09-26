from DataRepo.loaders.peak_group_conflicts import PeakGroupConflicts
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import InfileError
from DataRepo.utils.file_utils import read_from_file


class PeakGroupConflictsTests(TracebaseTestCase):
    def test_get_selected_representations(self):
        file = "DataRepo/data/tests/multiple_representations/mult_reps.tsv"
        df = read_from_file(file)
        # Deferring raising of the exception, so we can obtain the data
        pgc = PeakGroupConflicts(df=df, file=file)
        selected_dict = pgc.get_selected_representations()
        expected = {
            # Duplicates that only issue a warning and still skip
            "sample1": {
                "lysine": "accucor_pos.xlsx",
            },
            "sample2": {
                "lysine": "accucor_pos.xlsx",
            },
            "sample3": {
                "lysine": "accucor_pos.xlsx",
            },
            "sampleA": {
                # Version with no issues
                "asparagine": "isocorr1.xlsx",
                # Differing conflict resolutions because equivalent names (despite case and order differences)
                "l-aspartame/r-aspartame": None,
            },
            "sampleB": {
                # Version with no issues
                "asparagine": "isocorr1.xlsx",
                # Differing conflict resolutions because equivalent names (despite case and order differences)
                "l-aspartame/r-aspartame": None,
            },
        }
        self.assertDictEqual(expected, dict(selected_dict))
        self.assertEqual(1, pgc.aggregated_errors_object.num_errors)
        self.assertEqual(1, pgc.aggregated_errors_object.num_warnings)
        self.assertEqual(2, len(pgc.aggregated_errors_object.exceptions))
        self.assertEqual(
            [InfileError], pgc.aggregated_errors_object.get_exception_types()
        )
        self.assertIn(
            "resolutions for 'Peak Group Conflict'",
            str(pgc.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "resolutions for 'Peak Group Conflict'",
            str(pgc.aggregated_errors_object.exceptions[1]),
        )
