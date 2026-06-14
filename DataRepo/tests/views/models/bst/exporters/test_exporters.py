from datetime import datetime
from io import StringIO
from unittest.mock import patch

from django.db.models import CharField
from django.template.backends.django import Template

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst.exporters.exporters import BSTExportView

BSTEVStudyTestModel = create_test_model(
    "BSTLVStudyTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": ["name"]},
        ),
    },
)


class BSTExportViewTests(TracebaseTestCase):
    def test_bstexportview_abstract(self):
        with self.assertRaises(TypeError) as ar:
            BSTExportView(  # pylint: disable=abstract-class-instantiated
                BSTEVStudyTestModel
            )
        exc = ar.exception
        self.assertEqual(
            "Can't instantiate abstract class BSTExportView with abstract method buffer_file",
            str(exc),
        )

    def test_bstexportview_setup(self):
        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO
            extension = "csv"

            def buffer_file(self, header_content: str):
                pass

        bstev = BSTCSVExportView(BSTEVStudyTestModel)

        try:
            # Attempt to parse the string using the provided format
            datetime.strptime(bstev.fileheader_timestamp, bstev.header_time_format)
        except ValueError:
            raise AssertionError(
                f"String '{bstev.fileheader_timestamp}' does not match format "
                f"'{bstev.header_time_format}'"
            )

        self.assertEqual(BSTEVStudyTestModel, bstev.model)

        self.assertIn("BSTLVStudyTestModel.", bstev.export_file)
        self.assertNotIn("-", bstev.export_file)
        self.assertNotIn(":", bstev.export_file)
        self.assertIn(".csv", bstev.export_file)
        self.assertIsInstance(bstev.download_header_template, Template)

    def test_init_subclass_noname(self):
        class BSTCSVExportView(BSTExportView):
            # No name <- invalid
            content_type = "text/csv"
            buffer = StringIO
            extension = "csv"

            def buffer_file(self, header_content: str):
                pass

        with self.assertRaises(TypeError):
            BSTCSVExportView(BSTEVStudyTestModel)

    def test_init_subclass_bufferinstance(self):
        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO()  # type: ignore[assignment]
            extension = "csv"

            def buffer_file(self, header_content: str):
                pass

        with self.assertRaises(TypeError):
            BSTCSVExportView(BSTEVStudyTestModel)

    def test_init_subclass_novalue(self):
        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type: str  # type: ignore[misc]
            buffer = StringIO
            extension = "csv"

            def buffer_file(self, header_content: str):
                pass

        with self.assertRaises(TypeError):
            BSTCSVExportView(BSTEVStudyTestModel)

    def test_init_subclass_wrongtype(self):
        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO
            extension = 1

            def buffer_file(self, header_content: str):
                pass

        with self.assertRaises(TypeError):
            BSTCSVExportView(BSTEVStudyTestModel)

    def test_get_exporter_classes(self):
        class ExpView(BSTExportView):
            pass

        classes = BSTExportView.get_exporter_classes()
        self.assertIn(ExpView, classes)

    def test_gather_exporters_works(self):
        """Asserts that gather_exporters builds a dict of exporter classes keyed on export name."""

        class BSTTSVExportView(BSTExportView):
            name = "TSV"
            content_type = "text/tsv"
            buffer = StringIO
            extension = "tsv"

            def buffer_file(self, header_content: str):
                pass

        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO
            extension = "csv"

            def buffer_file(self, header_content: str):
                pass

        # Control the conditions by overriding the behavior of get_exporter_classes
        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[BSTTSVExportView, BSTCSVExportView],
        ):
            exporters = BSTExportView.gather_exporters()
            self.assertEquivalent(
                {
                    "TSV": BSTTSVExportView,
                    "CSV": BSTCSVExportView,
                },
                exporters,
            )

    def test_gather_exporters_raises_on_duplicates(self):
        """Asserts that gather_exporters raises a KeyError when the subclasses have duplicate export names
        (e.g. 'csv').
        """

        class BSTCSVExportView1(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO
            extension = "csv"

            def buffer_file(self, header_content: str):
                pass

        class BSTCSVExportView2(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO
            extension = "csv"

            def buffer_file(self, header_content: str):
                pass

        # Control the conditions by overriding the behavior of get_exporter_classes
        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[BSTCSVExportView1, BSTCSVExportView2],
        ):
            with self.assertRaises(KeyError):
                BSTExportView.gather_exporters()
