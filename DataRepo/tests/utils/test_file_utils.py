import pandas as pd

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.file_utils import (
    _get_file_type,
    get_column_dupes,
    read_headers_from_file,
)


class FileUtilsTests(TracebaseTestCase):
    # TODO: When the SampleTableLoader is converted to a derived class of TraceBaseLoader, move this test to test_loader
    def test_get_column_dupes(self):
        """
        Test that get_column_dupes identifies dupe combos in col2 and col3, row indexes 1, and 3 only (2 is ignored)
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

    def test_read_headers_from_file_tsv(self):
        headers = read_headers_from_file(
            "DataRepo/data/tests/compounds/short_compound_list.tsv"
        )
        self.assertEqual(["Compound", "Formula", "HMDB ID", "Synonyms"], headers)

    def test_read_headers_from_file_xlsx(self):
        headers = read_headers_from_file(
            "DataRepo/data/tests/load_table/test.xlsx", "MyDefaults"
        )
        self.assertEqual(["Sheet Name", "Column Header", "Default Value"], headers)

    def test_read_headers_from_file_csv(self):
        headers = read_headers_from_file("DataRepo/data/tests/load_table/defaults.csv")
        self.assertEqual(["Sheet Name", "Column Header", "Default Value"], headers)

    def test__get_file_type_excel(self):
        self.assertEqual(
            "excel", _get_file_type("DataRepo/data/tests/load_table/test.xlsx")
        )
        self.assertEqual(
            "tsv", _get_file_type("DataRepo/data/tests/load_table/test.tsv")
        )
        self.assertEqual(
            "csv", _get_file_type("DataRepo/data/tests/load_table/test.csv")
        )
        self.assertEqual(
            "excel",
            _get_file_type("DataRepo/data/tests/load_table/test.weird", "excel"),
        )
