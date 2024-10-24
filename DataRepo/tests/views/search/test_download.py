import json

from django.http import StreamingHttpResponse

from DataRepo.forms import AdvSearchDownloadForm
from DataRepo.loaders.study_loader import StudyV3Loader
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.file_utils import read_from_file
from DataRepo.views.search.download import (
    AdvancedSearchDownloadMzxmlTSVView,
    AdvancedSearchDownloadView,
    Echo,
    PeakDataToMzxmlTSV,
    PeakGroupsToMzxmlTSV,
    RecordToMzxmlTSV,
)

empty_tree = {
    "type": "group",
    "val": "all",
    "static": False,
    "queryGroup": [
        {
            "type": "query",
            "pos": "",
            "static": False,
            "fld": "",
            "ncmp": "",
            "val": "",
            "units": "",
        },
    ],
}
test_qry = {
    "selectedtemplate": "pgtemplate",
    "searches": {
        "pgtemplate": {
            "name": "PeakGroups",
            "tree": empty_tree,
        },
        "pdtemplate": {
            "name": "PeakData",
            "tree": empty_tree,
        },
        "fctemplate": {
            "name": "Fcirc",
            "tree": empty_tree,
        },
    },
}


class EchoTests(TracebaseTestCase):
    def test_echo_write(self):
        self.assertEqual("test", Echo().write("test"))


def assert_StreamingHttpResponse(testcase_obj, response):
    testcase_obj.assertIsInstance(response, StreamingHttpResponse)
    testcase_obj.assertEqual(200, response.status_code)
    testcase_obj.assertIn("attachment", response.headers["Content-Disposition"])
    testcase_obj.assertIn(
        "filename=PeakGroups_", response.headers["Content-Disposition"]
    )
    testcase_obj.assertEqual("application/text", response.headers["Content-Type"])


class AdvancedSearchDownloadViewTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    qry = test_qry

    @classmethod
    def setUpTestData(cls):
        sl = StudyV3Loader(
            file="DataRepo/data/tests/full_tiny_study/study.xlsx",
            df=read_from_file(
                "DataRepo/data/tests/full_tiny_study/study.xlsx", sheet=None
            ),
            mzxml_dir="DataRepo/data/tests/full_tiny_study",
        )
        sl.load_data()
        super().setUpTestData()

    def test_get_qry(self):
        form_data = {"qryjson": json.dumps(self.qry)}
        form = AdvSearchDownloadForm(data=form_data)
        # This creates form.cleaned_data (so that form_valid doesn't complain)
        form.is_valid()
        asdv = AdvancedSearchDownloadView()
        self.assertDictEqual(self.qry, asdv.get_qry(form.cleaned_data))

    def test_get_query_results(self):
        asdv = AdvancedSearchDownloadView()
        res = asdv.get_query_results(self.qry)
        self.assertEqual(36, res.count())

    def test_form_valid(self):
        form_data = {"qryjson": json.dumps(self.qry)}
        form = AdvSearchDownloadForm(data=form_data)
        # This creates form.cleaned_data (so that form_valid doesn't complain)
        form.is_valid()
        asdv = AdvancedSearchDownloadView()
        response = asdv.form_valid(form)
        assert_StreamingHttpResponse(self, response)
        expected1 = "# Download Time: ".encode()
        expected2 = (
            "# Advanced Search Query: {'selectedtemplate': 'pgtemplate', "
            "'searches': {'pgtemplate': {'name': 'PeakGroups', 'tree': {'type': 'group', 'val': 'all', 'static': "
            "False, 'queryGroup': [{'type': 'query', 'pos': '', 'static': False, 'fld': '', 'ncmp': '', 'val': '', "
            "'units': ''}]}}, 'pdtemplate': {'name': 'PeakData', 'tree': {'type': 'group', 'val': 'all', 'static': "
            "False, 'queryGroup': [{'type': 'query', 'pos': '', 'static': False, 'fld': '', 'ncmp': '', 'val': '', "
            "'units': ''}]}}, 'fctemplate': {'name': 'Fcirc', 'tree': {'type': 'group', 'val': 'all', 'static': False, "
            "'queryGroup': [{'type': 'query', 'pos': '', 'static': False, 'fld': '', 'ncmp': '', 'val': '', 'units': "
            "''}]}}}}\n"
            "#\n"
            "\n"
            "Sample\tTissue\tTime Collected (m)\tPeak Group\tMeasured Compound(s)\tMeasured Compound Synonym(s)\t"
            "Formula\tLabeled Element\tMZ Data File(s)\tTotal Abundance\tEnrichment Fraction\tEnrichment Abundance\t"
            "Normalized Labeling\tPeak Annotation Filename\tAnimal\tGenotype\tBody Weight (g)\tAge (weeks)\tSex\tDiet\t"
            "Feeding Status\tTreatment\tInfusate\tTracer(s)\tTracer Compound(s)\tTracer Concentration(s) (mM)\t"
            "Infusion Rate (ul/min/g)\tStudies\n"
        ).encode()
        expected3 = (
            "xzl5_t\tserum_plasma_tail\t150.0\talanine\talanine\talanine/L-alanine/ala\tC3H7NO2\tC\txzl5_t.mzXML\t"
            "6587116.743783121\t0.2235244143081364\t1472381.4121334408\t1.0\talafasted_cor.xlsx\txzl5\tWT\t27.5\t14.0\t"
            "M\tPicoLab Rodent 20 5053\tfasted\tno treatment\talanine-[13C3,15N1][180]\talanine-[13C3,15N1]\talanine\t"
            "180.0\t0.1\ttest v3 study\nxzl5_t\tserum_plasma_tail\t150.0\talanine\talanine\talanine/L-alanine/ala\t"
            "C3H7NO2\tN\txzl5_t.mzXML\t6587116.743783121\t0.30075567022988536\t1981112.7111589927\t1.0\t"
            "alafasted_cor.xlsx\txzl5\tWT\t27.5\t14.0\tM\tPicoLab Rodent 20 5053\tfasted\tno treatment\t"
            "alanine-[13C3,15N1][180]\talanine-[13C3,15N1]\talanine\t180.0\t0.1\ttest v3 study\nxzl5_panc\tpancreas\t"
            "150.0\talanine\talanine\talanine/L-alanine/ala\tC3H7NO2\tC\txzl5_panc.mzXML\t43695995.42306948\t"
            "0.06929539990964839\t3027931.477291765\t0.31001266740429617\talafasted_cor.xlsx\txzl5\tWT\t27.5\t14.0\tM\t"
            "PicoLab Rodent 20 5053\tfasted\tno treatment\talanine-[13C3,15N1][180]\talanine-[13C3,15N1]\talanine\t"
            "180.0\t0.1\ttest v3 study\nxzl5_panc\tpancreas\t150.0\talanine\talanine\talanine/L-alanine/ala\tC3H7NO2\tN"
            "\txzl5_panc.mzXML\t43695995.42306948\t0.09250135755812483\t4041938.896487536\t0.3075631375043422\t"
            "alafasted_cor.xlsx\txzl5\tWT\t27.5\t14.0\tM\tPicoLab Rodent 20 5053\tfasted\tno treatment\t"
            "alanine-[13C3,15N1][180]\talanine-[13C3,15N1]\talanine\t180.0\t0.1\ttest v3 study\nxzl4_t\t"
            "serum_plasma_tail\t150.0"  # A portion of the file
        ).encode()
        content = str(response.getvalue())
        # `[2:-1]` removes the "b'" and last "'" from the beginning and end of the converted bytes to string
        self.assertIn(str(expected1)[2:-1], content)
        self.assertIn(str(expected2)[2:-1], content)
        self.assertIn(str(expected3)[2:-1], content)


class RecordToMzxmlTSVTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    asdv = None
    res = None

    @classmethod
    def setUpTestData(cls):
        sl = StudyV3Loader(
            file="DataRepo/data/tests/full_tiny_study/study.xlsx",
            df=read_from_file(
                "DataRepo/data/tests/full_tiny_study/study.xlsx", sheet=None
            ),
            mzxml_dir="DataRepo/data/tests/full_tiny_study",
        )
        sl.load_data()
        cls.asdv = AdvancedSearchDownloadView()
        cls.res = cls.asdv.get_query_results(test_qry)
        super().setUpTestData()

    def test_headers(self):
        self.assertEqual(
            [
                "mzXML File",
                "Polarity",
                "MZ Min",
                "MZ Max",
                "Sample",
                "Tissue",
                "Date Collected",
                "Collection Time (m)",
                "Handler",
                "Animal",
                "Age",
                "Sex",
                "Genotype",
                "Weight (g)",
                "Diet",
                "Feeding Status",
                "Treatment",
                "Infusate",
                "Operator",
                "Instrument",
                "LC Protocol",
                "Date",
            ],
            RecordToMzxmlTSV.headers,
        )

    def test_get_rec_to_rows_method(self):
        self.assertEqual(
            "PeakGroupsToMzxmlTSV.rec_to_rows",
            RecordToMzxmlTSV.get_rec_to_rows_method("pgtemplate").__qualname__,
        )

    def test_PeakGroupsToMzxmlTSV_msrun_sample_rec_to_row(self):
        pgtmt = PeakGroupsToMzxmlTSV()
        row = pgtmt.msrun_sample_rec_to_row(self.res.first().msrun_sample)
        self.assertEqual(
            [
                "2020-07-22/Xianfeng Zeng/QE2/polar-HILIC-25-min/positive/1-503/xzl5_t.mzXML",
                "positive",
                1.0,
                502.9,
                "xzl5_t",
                "serum_plasma_tail",
                "2020-07-22",
                150.0,
                "Xianfeng Zeng",
                "xzl5",
                14.0,
                "M",
                "WT",
                27.5,
                "PicoLab Rodent 20 5053",
                "fasted",
                "no treatment",
                "alanine-[13C3,15N1][180]",
                "Xianfeng Zeng",
                "QE2",
                "polar-HILIC-25-min",
                "2020-07-22",
            ],
            row,
        )

    def test_rec_to_rows(self):
        pgtmt = PeakGroupsToMzxmlTSV()
        row = pgtmt.rec_to_rows(self.res.first())
        self.assertEqual(
            [
                [
                    "2020-07-22/Xianfeng Zeng/QE2/polar-HILIC-25-min/positive/1-503/xzl5_t.mzXML",
                    "positive",
                    1.0,
                    502.9,
                    "xzl5_t",
                    "serum_plasma_tail",
                    "2020-07-22",
                    150.0,
                    "Xianfeng Zeng",
                    "xzl5",
                    14.0,
                    "M",
                    "WT",
                    27.5,
                    "PicoLab Rodent 20 5053",
                    "fasted",
                    "no treatment",
                    "alanine-[13C3,15N1][180]",
                    "Xianfeng Zeng",
                    "QE2",
                    "polar-HILIC-25-min",
                    "2020-07-22",
                ],
            ],
            row,
        )

    def test_PeakDataToMzxmlTSV_rec_to_rows(self):
        pdqry = test_qry.copy()
        pdqry["selectedtemplate"] = "pdtemplate"
        pdtmt = PeakDataToMzxmlTSV()
        res = self.asdv.get_query_results(pdqry)
        row = pdtmt.rec_to_rows(res.first())
        self.assertEqual(
            [
                [
                    "2020-07-22/Xianfeng Zeng/QE2/polar-HILIC-25-min/positive/1-503/xzl5_panc.mzXML",
                    "positive",
                    1.0,
                    502.9,
                    "xzl5_panc",
                    "pancreas",
                    "2020-07-22",
                    150.0,
                    "Xianfeng Zeng",
                    "xzl5",
                    14.0,
                    "M",
                    "WT",
                    27.5,
                    "PicoLab Rodent 20 5053",
                    "fasted",
                    "no treatment",
                    "alanine-[13C3,15N1][180]",
                    "Xianfeng Zeng",
                    "QE2",
                    "polar-HILIC-25-min",
                    "2020-07-22",
                ],
            ],
            row,
        )


class AdvancedSearchDownloadMzxmlTSVViewTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        sl = StudyV3Loader(
            file="DataRepo/data/tests/full_tiny_study/study.xlsx",
            df=read_from_file(
                "DataRepo/data/tests/full_tiny_study/study.xlsx", sheet=None
            ),
            mzxml_dir="DataRepo/data/tests/full_tiny_study",
        )
        sl.load_data()
        super().setUpTestData()

    def test_form_valid(self):
        form_data = {"qryjson": json.dumps(test_qry)}
        form = AdvSearchDownloadForm(data=form_data)
        # This creates form.cleaned_data (so that form_valid doesn't complain)
        form.is_valid()
        asdv = AdvancedSearchDownloadMzxmlTSVView()
        response = asdv.form_valid(form)
        assert_StreamingHttpResponse(self, response)
        expected1 = "# Download Time: ".encode()
        expected2 = (
            "nmzXML File\tPolarity\tMZ Min\tMZ Max\tSample\tTissue\tDate Collected\tCollection Time (m)\tHandler\t"
            "Animal\tAge\tSex\tGenotype\tWeight (g)\tDiet\tFeeding Status\tTreatment\tInfusate\tOperator\tInstrument\t"
            "LC Protocol\tDate\r\n2020-07-22/Xianfeng Zeng/QE2/polar-HILIC-25-min/positive/1-503/xzl5_t.mzXML\tpositive"
            "\t1.0\t502.9\txzl5_t\tserum_plasma_tail\t2020-07-22\t150.0\tXianfeng Zeng\txzl5\t14.0\tM\tWT\t27.5\t"
            "PicoLab Rodent 20 5053\tfasted\tno treatment\talanine-[13C3,15N1][180]\tXianfeng Zeng\tQE2\t"
            "polar-HILIC-25-min\t2020-07-22\r\n2020-07-22/Xianfeng Zeng/QE2/polar-HILIC-25-min/positive/1-503/"
            "xzl5_t.mzXML\tpositive\t1.0\t502.9\txzl5_t\tserum_plasma_tail\t2020-07-22\t150.0\tXianfeng Zeng\txzl5\t"
            "14.0\tM\tWT\t27.5\tPicoLab Rodent 20 5053\tfasted\tno treatment\talanine-[13C3,15N1][180]\tXianfeng Zeng\t"
            "QE2\tpolar-HILIC-25-min\t2020-07-22\r\n"  # There is more, but this is sufficient
        ).encode()
        content = str(response.getvalue())
        # `[2:-1]` removes the "b'" and last "'" from the beginning and end of the converted bytes to string
        self.assertIn(str(expected1)[2:-1], content)
        self.assertIn(str(expected2)[2:-1], content)
