import re

from DataRepo.tests.tracebase_test_case import TracebaseTransactionTestCase
from DataRepo.views.upload.validation import DataValidationView


class DataValidationViewTests(TracebaseTransactionTestCase):
    LCMS_DICT = {
        "a": {
            "sort level": 1,
            "tracebase sample name": "a",
            "sample data header": "a",
            "peak annotation filename": "accucor.xlsx",
            "mzxml filename": "",
        },
        "b": {
            "sort level": 0,
            "tracebase sample name": "b",
            "sample data header": "b",
            "peak annotation filename": "accucor.xlsx",
            "mzxml filename": "b.typo",
        },
        "c": {
            "sort level": 0,
            "tracebase sample name": "c",
            "sample data header": "c",
            "peak annotation filename": "accucor.xlsx",
            "mzxml filename": "c.mzXML",
        },
        "d_pos": {
            "sort level": 1,
            "tracebase sample name": "d",
            "sample data header": "d_pos",
            "peak annotation filename": "accucor.xlsx",
            "mzxml filename": "",
        },
        "extra": {
            "sort level": 2,
            "tracebase sample name": "extra",
            "sample data header": "",
            "peak annotation filename": "",
            "mzxml filename": "extra.mzxml",
        },
        "ERROR: c DUPLICATE HEADER 1": {
            "sort level": 3,
            "tracebase sample name": "",
            "sample data header": "ERROR: c DUPLICATE HEADER 1",
            "peak annotation filename": "accucor.xlsx",
            "mzxml filename": "",
        },
        "ERROR: b.dupe DUPLICATE MZXML BASENAME 1": {
            "sort level": 4,
            "tracebase sample name": "",
            "sample data header": "",
            "peak annotation filename": "",
            "mzxml filename": "ERROR: b.dupe DUPLICATE MZXML BASENAME 1",
        },
    }

    def test_build_lcms_dict(self):
        lcms_dict = DataValidationView.build_lcms_dict(
            ["a", "b", "c", "c", "d_pos"],  # Sample headers
            ["b.typo", "b.dupe", "c.mzXML", "extra.mzxml"],  # List of mzxml file names
            "accucor.xlsx",  # Peak annot file
        )
        self.assertDictEqual(
            self.LCMS_DICT,
            dict(lcms_dict),
        )

    def test_get_approx_sample_header_replacement_regex_default(self):
        pattern = DataValidationView.get_approx_sample_header_replacement_regex()
        samplename = re.sub(pattern, "", "mysample_neg_pos_scan2")
        self.assertEqual("mysample", samplename)

    def test_get_approx_sample_header_replacement_regex_add_custom(self):
        pattern = DataValidationView.get_approx_sample_header_replacement_regex(
            [r"_blah"]
        )
        samplename = re.sub(pattern, "", "mysample_pos_blah_scan1")
        self.assertEqual("mysample", samplename)

    def test_get_approx_sample_header_replacement_regex_just_custom(self):
        pattern = DataValidationView.get_approx_sample_header_replacement_regex(
            [r"_blah"], add=False
        )
        samplename = re.sub(pattern, "", "mysample_pos_blah")
        self.assertEqual("mysample_pos", samplename)

    def test_lcms_dict_to_tsv_string(self):
        lcms_data = DataValidationView.lcms_dict_to_tsv_string(self.LCMS_DICT)
        self.assertEqual(
            (
                "tracebase sample name\tsample data header\tmzxml filename\tpeak annotation filename\n"
                "b\tb\tb.typo\taccucor.xlsx\n"
                "c\tc\tc.mzXML\taccucor.xlsx\n"
                "a\ta\t\taccucor.xlsx\n"
                "d\td_pos\t\taccucor.xlsx\n"
                "extra\t\textra.mzxml\t\n"
                "\tERROR: c DUPLICATE HEADER 1\t\taccucor.xlsx\n"
                "\t\tERROR: b.dupe DUPLICATE MZXML BASENAME 1\t\n"
            ),
            lcms_data,
        )
