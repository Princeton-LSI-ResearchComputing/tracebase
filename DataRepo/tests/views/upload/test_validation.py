import re

from DataRepo.tests.tracebase_test_case import TracebaseTransactionTestCase
from DataRepo.utils.exceptions import NonUniqueSampleDataHeader
from DataRepo.views.upload.validation import DataValidationView


class DataValidationViewTests(TracebaseTransactionTestCase):
    LCMS_DICT = {
        "a": {
            "sort level": 0,
            "tracebase sample name": "a",
            "sample data header": "a",
            "peak annotation filename": "accucor.xlsx",
        },
        "b": {
            "sort level": 0,
            "tracebase sample name": "b",
            "sample data header": "b",
            "peak annotation filename": "accucor.xlsx",
        },
        "d_pos": {
            "sort level": 0,
            "tracebase sample name": "d",
            "sample data header": "d_pos",
            "peak annotation filename": "accucor.xlsx",
        },
        "c": {
            "sort level": 1,
            "error": NonUniqueSampleDataHeader("c", {"accucor.xlsx": 2}),
            "tracebase sample name": "c",
            "sample data header": "c",
            "peak annotation filename": "accucor.xlsx",
        },
    }

    def test_build_lcms_dict(self):
        dvv = DataValidationView()
        lcms_dict = dvv.build_lcms_dict(
            ["a", "b", "c", "c", "d_pos"],  # Sample headers
            "accucor.xlsx",  # Peak annot file
        )
        # assertDictEqual doesn't work with the exception object, so asserting each individually and comparing exception
        # strings
        self.assertEqual(
            len(self.LCMS_DICT.keys()),
            len(lcms_dict.keys()),
        )
        self.assertDictEqual(
            self.LCMS_DICT["a"],
            lcms_dict["a"],
        )
        self.assertDictEqual(
            self.LCMS_DICT["b"],
            lcms_dict["b"],
        )
        self.assertEqual(
            str(self.LCMS_DICT["c"]["error"]),
            str(lcms_dict["c"]["error"]),
        )
        self.assertEqual(
            self.LCMS_DICT["c"]["peak annotation filename"],
            lcms_dict["c"]["peak annotation filename"],
        )
        self.assertEqual(
            self.LCMS_DICT["c"]["sample data header"],
            lcms_dict["c"]["sample data header"],
        )
        self.assertEqual(
            self.LCMS_DICT["c"]["sort level"],
            lcms_dict["c"]["sort level"],
        )
        self.assertEqual(
            self.LCMS_DICT["c"]["tracebase sample name"],
            lcms_dict["c"]["tracebase sample name"],
        )
        self.assertDictEqual(
            self.LCMS_DICT["d_pos"],
            lcms_dict["d_pos"],
        )
        self.assertEqual(
            str(self.LCMS_DICT["c"]["error"]), str(dvv.lcms_build_errors.nusdh_list[0])
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
                "tracebase sample name\tsample data header\tpeak annotation filename\n"
                "a\ta\taccucor.xlsx\n"
                "b\tb\taccucor.xlsx\n"
                "d\td_pos\taccucor.xlsx\n"
                "c\tc\taccucor.xlsx\n"
            ),
            lcms_data,
        )
