from django.core.management import call_command

from DataRepo.models.archive_file import ArchiveFile
from DataRepo.models.peak_group import PeakGroup
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class LoadAccucorFilesWithMultipleTracersLabelsCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        # Importing here to avoid running those tests
        from DataRepo.tests.management.test_load_peak_annotations import (
            LoadAccucorWithMultipleTracersLabelsCommandTests,
        )

        # Calling from other test class for its data setup (for code re-use)
        LoadAccucorWithMultipleTracersLabelsCommandTests.setUpTestData()
        super().setUpTestData()

    def test_load_multiple_accucor_labels(self):
        """
        The infusate has tracers that cumulatively contain multiple Tracers/labels.  This tests that it loads without
        error
        """
        call_command(
            "load_peak_annotation_files",
            infile="DataRepo/data/tests/accucor_with_multiple_labels/accucor_peak_annot_files.tsv",
        )
        # Assert the loader created the 1 ArchiveFile record
        self.assertEqual(
            1,
            ArchiveFile.objects.filter(
                filename="accucor.xlsx", data_format__code="accucor"
            ).count(),
        )
        # Assert the loader ran the PeakAnnotationsLoader on the accucor file in the input
        self.assertGreater(PeakGroup.objects.count(), 0)


class LoadIsocorrFilesCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        # Importing here to avoid running those tests
        from DataRepo.tests.management.test_load_peak_annotations import (
            LoadIsocorrCommandTests,
        )

        # Calling from other test class for its data setup (for code re-use)
        LoadIsocorrCommandTests.setUpTestData()
        super().setUpTestData()

    def test_load_singly_labeled_isocorr_load(self):
        pre_pg_load_count = PeakGroup.objects.count()
        call_command(
            "load_peak_annotation_files",
            infile="DataRepo/data/tests/singly_labeled_isocorr/small_cor_peak_annot_files.tsv",
        )
        post_pg_load_count = PeakGroup.objects.count()
        self.assertGreater(post_pg_load_count, pre_pg_load_count)
