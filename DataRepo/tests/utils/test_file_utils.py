import pandas as pd

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.file_utils import get_column_dupes, get_one_column_dupes


class FileUtilsTests(TracebaseTestCase):
    def test_get_one_column_dupes(self):
        """Test that get_one_column_dupes identifies dupes in col2, row indexes 0 and 1 only (2 is ignored)."""
        pddata = pd.DataFrame.from_dict(
            {
                "col1": ["A", "B", "C"],
                "col2": ["x", "x", "x"],
                "col3": ["1", "2", "3"],
            },
        )
        outdict, outlist = get_one_column_dupes(pddata, "col2", ignore_row_idxs=[2])
        self.assertEqual({"x": [0, 1]}, outdict)
        self.assertEqual([0, 1], outlist)

    def test_get_column_dupes(self):
        """
        Test that get_one_column_dupes identifies dupe combos in col2 and col3, row indexes 1, and 3 only (2 is ignored)
        """
        pddata = pd.DataFrame.from_dict(
            {
                "col1": ["A", "A", "A", "A"],
                "col2": ["y", "x", "x", "x"],
                "col3": [2, 2, 2, 2],
            },
        )
        outdict, outlist = get_column_dupes(
            pddata, ["col2", "col3"], ignore_row_idxs=[2]
        )
        expected = {
            "col2: [x], col3: [2]": {
                "rowidxs": [1, 3],
                "vals": {
                    "col2": "x",
                    "col3": 2,
                },
            }
        }
        self.assertEqual(expected, outdict)
        self.assertEqual([1, 3], outlist)
