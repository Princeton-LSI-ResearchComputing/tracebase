import pandas as pd
from django.core.files.uploadedfile import TemporaryUploadedFile

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.file_utils import (
    _get_file_type,
    _read_from_xlsx,
    get_column_dupes,
    read_headers_from_file,
    string_to_datetime,
)


class FileUtilsTests(TracebaseTestCase):
    # TODO: When the SampleTableLoader is converted to a derived class of TableLoader, move this test to test_loader
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

    def test_string_to_datetime(self):
        date = string_to_datetime("2022-1-22 00:10:00")
        self.assertEqual("2022-01-22", str(date))

    def test_read_from_xlsx_multiple_sheets_with_dtypes(self):
        study_xlsx = "DataRepo/data/tests/submission_v3/study.xlsx"
        dtypes = {
            "Treatments": {
                "Animal Treatment": str,
                "Treatment Description": str,
            },
            "Sequences": {
                "Sequence Number": int,
                "Operator": str,
                "Date": str,
                "Instrument": str,
                "LC Protocol": str,
                "LC Run Length": int,
                "LC Description": str,
                "Notes": str,
            },
        }
        expected = {
            "Treatments": {
                "Animal Treatment": {0: "no treatment"},
                "Treatment Description": {
                    0: (
                        "No treatment was applied to the animal.  Animal was maintained on normal diet (LabDiet #5053 "
                        '"Maintenance"), housed at room temperature with a normal light cycle.'
                    ),
                },
            },
            "Sequences": {
                "Sequence Name": {
                    0: "Xianfeng Zeng, polar-HILIC-25-min, QE2, 6/8/2021",
                    1: "Xianfeng Zeng, polar-HILIC-25-min, QE2, 10/19/2021",
                    2: "Xianfeng Zeng, polar-HILIC-25-min, QE2, 7/22/2020",
                },
                "Operator": {
                    0: "Xianfeng Zeng",
                    1: "Xianfeng Zeng",
                    2: "Xianfeng Zeng",
                },
                "Date": {
                    0: "2021-06-08 00:00:00",
                    1: "2021-10-19 00:00:00",
                    2: "2020-07-22 00:00:00",
                },
                "Instrument": {0: "QE2", 1: "QE2", 2: "QE2"},
                "LC Protocol Name": {
                    0: "polar-HILIC-25-min",
                    1: "polar-HILIC-25-min",
                    2: "polar-HILIC-25-min",
                },
                "Notes": {0: "", 1: "", 2: ""},
            },
        }
        dfs_dict = _read_from_xlsx(
            study_xlsx, sheet=list(expected.keys()), dtype=dtypes
        )
        self.assertEqual(len(expected.keys()), len(dfs_dict.keys()))
        self.assertDictEqual(expected["Treatments"], dfs_dict["Treatments"].to_dict())
        self.assertDictEqual(expected["Sequences"], dfs_dict["Sequences"].to_dict())

    def test_get_file_type_temp_file(self):
        """Assert the _get_file_type() works when supplied a TemporaryUploadedFile."""
        tuf = TemporaryUploadedFile("test.tsv", None, None, None)
        self.assertEqual("tsv", _get_file_type(tuf))
