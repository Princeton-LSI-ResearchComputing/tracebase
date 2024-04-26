from DataRepo.loaders.accucor_data_loader import AccuCorDataLoader
from DataRepo.models import DataFormat
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class AccuCorDataLoaderTests(TracebaseTestCase):

    fixtures = ["data_formats.yaml"]

    def test_detect_filetype_accucor_xlsx(self):
        accucor_format = DataFormat.objects.get(code="accucor")
        self.assertEquals(
            AccuCorDataLoader.detect_data_format(
                file="DataRepo/data/tests/accucor_with_multiple_labels/accucor.xlsx"
            ),
            accucor_format,
        )

    def test_detect_filetype_accucor_csv(self):
        accucor_format = DataFormat.objects.get(code="accucor")
        self.assertEquals(
            AccuCorDataLoader.detect_data_format(
                file="DataRepo/data/tests/singly_labeled_isocorr/small_cor.csv"
            ),
            accucor_format,
        )

    def test_detect_filetype_isocorr(self):
        isocorr_format = DataFormat.objects.get(code="isocorr")
        self.assertEquals(
            AccuCorDataLoader.detect_data_format(
                file="DataRepo/data/tests/multiple_tracers/bcaafasted_cor.xlsx"
            ),
            isocorr_format,
        )

    def test_detect_filetype_none(self):
        self.assertEquals(
            AccuCorDataLoader.detect_data_format(
                file="DataRepo/data/tests/submission_v3/study.xlsx"
            ),
            None,
        )

    def test_is_accucor_file(self):
        self.assertFalse(
            AccuCorDataLoader.is_accucor(
                file="DataRepo/data/tests/submission_v3/study.xlsx"
            )
        )
        self.assertTrue(
            AccuCorDataLoader.is_accucor(
                file="DataRepo/data/tests/accucor_with_multiple_labels/accucor.xlsx"
            )
        )
        self.assertFalse(
            AccuCorDataLoader.is_accucor(
                file="DataRepo/data/tests/multiple_tracers/bcaafasted_cor.xlsx"
            )
        )

    def test_excel_only(self):
        self.assertFalse(
            AccuCorDataLoader.is_isocorr(
                "DataRepo/data/tests/singly_labeled_isocorr/small_cor.csv"
            )
        )

    def test_is_isocorr_sheets(self):
        self.assertFalse(
            AccuCorDataLoader.is_isocorr(
                file="DataRepo/data/tests/submission_v3/study.xlsx"
            )
        )
        self.assertTrue(
            AccuCorDataLoader.is_isocorr(
                file="DataRepo/data/tests/multiple_tracers/bcaafasted_cor.xlsx"
            )
        )
        self.assertFalse(
            AccuCorDataLoader.is_isocorr(
                file="DataRepo/data/tests/accucor_with_multiple_labels/accucor.xlsx"
            )
        )
