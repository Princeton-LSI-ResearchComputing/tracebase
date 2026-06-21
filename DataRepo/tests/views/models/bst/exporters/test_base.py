from contextlib import contextmanager
from unittest.mock import ANY, MagicMock, Mock, patch

from django.db.models import CharField
from django.template.backends.django import Template
from django.test import RequestFactory
from django.urls import NoReverseMatch

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst.export import BSTExportedListView
from DataRepo.views.models.bst.exporters.base import BSTExportView
from DataRepo.views.models.bst.exporters.delimited.csv import CSVBSTExportView
from DataRepo.views.models.bst.exporters.excel import ExcelBSTExportView

BSTEVStudyTestModel = create_test_model(
    "BSTEVStudyTestModel",
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


class StudyELV(BSTExportedListView):
    model = BSTEVStudyTestModel


@contextmanager
def patch_exporter_classes(*exporter_classes):
    with patch.object(
        BSTExportView,
        "get_exporter_classes",
        return_value=list(exporter_classes),
    ):
        yield


class BSTExportViewTests(TracebaseTestCase):
    def test_bstexportview_setup(self):
        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            extension = "csv"
            view_name = "csv_exp_list_view"

        BSTCSVExportView()

    def test_init_subclass_noname(self):
        with self.assertRaises(TypeError):

            class BSTCSVExportView(BSTExportView):
                # No name <- invalid
                content_type = "text/csv"
                extension = "csv"
                view_name = "csv_exp_list_view"

            BSTCSVExportView()

    def test_init_subclass_novalue(self):
        with self.assertRaises(TypeError):

            class BSTCSVExportView(BSTExportView):
                name = "CSV"
                content_type: str  # type: ignore[misc]
                extension = "csv"
                view_name = "csv_exp_list_view"

            BSTCSVExportView()

    def test_init_subclass_wrongtype(self):
        with self.assertRaises(TypeError):

            class BSTCSVExportView(BSTExportView):
                name = "CSV"
                content_type = "text/csv"
                extension = 1
                view_name = "csv_exp_list_view"

            BSTCSVExportView()

    def test_get_exporter_classes(self):
        class ExpView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            extension = "csv"
            view_name = "csv_exp_list_view"

        classes = BSTExportView.get_exporter_classes()
        self.assertIn(ExpView, classes)

    def test_gather_exporters_works(self):
        """Asserts that gather_exporters builds a dict of exporter classes keyed on export name."""

        class BSTTSVExportView(BSTExportView):
            name = "TSV"
            content_type = "text/tsv"
            extension = "tsv"
            view_name = "tsv_exp_list_view"

        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            extension = "csv"
            view_name = "csv_exp_list_view"

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
            extension = "csv"
            view_name = "csv_exp_list_view"

        class BSTCSVExportView2(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            extension = "csv"
            view_name = "csv_exp_list_view"

        # Control the conditions by overriding the behavior of get_exporter_classes
        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[BSTCSVExportView1, BSTCSVExportView2],
        ):
            with self.assertRaises(KeyError):
                BSTExportView.gather_exporters()

    def test_get_header_context(self):
        class BSTTSVExportView(BSTExportView):
            name = "TSV"
            content_type = "text/tsv"
            extension = "tsv"
            view_name = "tsv_exp_list_view"

        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[BSTTSVExportView],
        ):
            btev = BSTTSVExportView()
            source_view = StudyELV()
            btev.init_export(source_view)
            context = btev.get_header_context(source_view)
            self.assertEqual(
                set(
                    [
                        "asc",
                        "columns",
                        "export_filters",
                        "search",
                        "sortcol",
                        "table_name",
                        "timestamp",
                        "total",
                    ]
                ),
                set(context.keys()),
            )
            self.assertTrue(context["asc"])
            self.assertEqual(2, len(context["columns"]))
            self.assertEqual({}, context["export_filters"])
            self.assertIsNone(context["search"])
            self.assertEqual("name", context["sortcol"].name)
            self.assertEqual("BSTEV Study Test Models", context["table_name"])
            self.assertIn("-", context["timestamp"])
            self.assertIn(":", context["timestamp"])
            self.assertEqual(0, context["total"])

    @patch("DataRepo.views.models.bst.exporters.base.resolve")
    @patch("DataRepo.views.models.bst.exporters.base.reverse")
    def test_get_source_view_success(self, mock_reverse, mock_resolve):
        """Assert that the source view is obtained when given the request"""

        # Simulate the export URL: /export_bstlv_csv/?source=animal_list
        request = RequestFactory().get(
            "/export_bstlv_csv/",
            {"source": "animal_list"},
        )

        # This is the object we want returned by get_source_view().
        # It stands in for an initialized AnimalListView instance.
        source_view = MagicMock(spec=BSTExportedListView)

        # This stands in for: AnimalListView when we instantiate with source_view_class(), this will return source_view.
        source_view_class = MagicMock(return_value=source_view)

        # Pretend: reverse("animal_list") returns: "/animals/"
        mock_reverse.return_value = "/animals/"

        # resolve("/animals/") normally returns a ResolverMatch.  We fake one here.
        resolved_match = MagicMock()
        resolved_match.func.view_class = source_view_class
        mock_resolve.return_value = resolved_match

        # This is what we're testing
        exporter = CSVBSTExportView()
        result = exporter.get_source_view(request)

        # Verify we got back the instantiated source view.
        self.assertIs(result, source_view)

        # Verify: reverse("animal_list") was called.
        mock_reverse.assert_called_once_with("animal_list")
        # Verify: resolve("/animals/") was called.
        mock_resolve.assert_called_once_with("/animals/")
        # Verify: source_view_class() was called to create the view instance.
        source_view_class.assert_called_once_with()

    @patch("DataRepo.views.models.bst.exporters.base.reverse")
    def test_get_source_view_bad_source(self, mock_reverse):
        """Assert that a bad source view catches and raises a more informative exception"""
        request = RequestFactory().get(
            "/export_bstlv_csv/",
            {"source": "bad"},
        )

        # This simulates the reverse method raising a NoReverseMatch exception
        mock_reverse.side_effect = NoReverseMatch()
        exporter = CSVBSTExportView()

        with self.assertRaisesRegex(
            ValueError,
            "Invalid or unsupported view class: bad",
        ):
            exporter.get_source_view(request)

    @patch.object(ExcelBSTExportView, "buffer_file")
    @patch.object(ExcelBSTExportView, "get_header_context")
    @patch.object(ExcelBSTExportView, "get_source_view")
    def test_get_with_header_template(
        self,
        mock_get_source_view,
        mock_get_header_context,
        mock_buffer_file,
    ):
        """Verify that get() renders download metadata and supplies the rendered header content to buffer_file() when a
        download header template is configured.
        """

        # Mock the source view returned from request resolution.
        source_view = Mock()
        mock_get_source_view.return_value = source_view
        mock_get_header_context.return_value = {"foo": "bar"}

        # Create a concrete exporter to test with.
        exporter = ExcelBSTExportView()

        # Mock the header template so we can verify that the rendered content is passed through to buffer_file().
        exporter.download_header_template = Mock(spec=Template)
        exporter.download_header_template.render.return_value = "HEADER"

        # Provide a buffer because get() expects init_export() to create one.  Since init_export() is mocked below, we
        # must supply it ourselves, and set export_file.
        exporter.export_file = "test.xlsx"

        # Mock init_export() so that this test focuses only on get() behavior.
        with patch.object(exporter, "init_export") as mock_init_export:
            exporter.get(Mock())

        # Verify that the source view was resolved from the request.
        mock_get_source_view.assert_called_once()

        # Verify that export initialization was performed.
        mock_init_export.assert_called_once_with(source_view)

        # Verify that get_header_context was called
        exporter.download_header_template.render.assert_called_once_with({"foo": "bar"})

        # Verify that the download metadata template was rendered.
        exporter.download_header_template.render.assert_called_once()

        # Verify that the rendered header content was supplied to buffer_file().
        mock_buffer_file.assert_called_once_with(source_view, "HEADER", ANY)

    @patch.object(ExcelBSTExportView, "buffer_file")
    @patch.object(ExcelBSTExportView, "get_source_view")
    def test_get_without_header_template(
        self,
        mock_get_source_view,
        mock_buffer_file,
    ):
        """Verify that get() supplies an empty header string to buffer_file() when no download header template is
        configured.
        """

        # Mock the source view returned from request resolution.
        source_view = Mock()
        mock_get_source_view.return_value = source_view

        # Create a concrete exporter to test and explicitly disable the header template.
        exporter = ExcelBSTExportView()
        exporter.download_header_template = None

        # Provide a buffer because init_export() is mocked below.
        exporter.export_file = "test"

        # Mock init_export() so that this test focuses only on the branch that
        # determines the header content passed to buffer_file().
        with patch.object(exporter, "init_export"):
            exporter.get(Mock())

        # Verify that an empty header string was supplied when no template exists.
        mock_buffer_file.assert_called_once_with(source_view, "", ANY)

    @patch.object(ExcelBSTExportView, "buffer_file")
    @patch.object(ExcelBSTExportView, "get_source_view")
    def test_get_returns_download_response(
        self,
        mock_get_source_view,
        mock_buffer_file,
    ):
        """Verify that get() returns an HTTP download response containing the generated file contents and expected
        download headers.
        """

        # Mock the source view returned from request resolution.
        source_view = Mock()
        mock_get_source_view.return_value = source_view

        def fake_buffer_file(source_view, header_content, buffer):
            buffer.write(b"excel-bytes")

        mock_buffer_file.side_effect = fake_buffer_file

        # Create a concrete exporter to test with.
        exporter = ExcelBSTExportView()

        # Disable header rendering because it is not relevant to this test.
        exporter.download_header_template = None

        # Populate the values that would normally be set by init_export().
        exporter.export_file = "test.xlsx"

        # Mock init_export() so that the test can focus solely on response creation.
        with patch.object(exporter, "init_export"):
            response = exporter.get(Mock())

        # Verify that the response contains the generated file contents.
        self.assertEqual(b"excel-bytes", response.content)

        # Verify that the response advertises the correct content type.
        self.assertEqual(
            exporter.content_type,
            response["Content-Type"],
        )

        # Verify that the response is configured as a file attachment with the expected filename.
        self.assertEqual(
            'attachment; filename="test.xlsx"',
            response["Content-Disposition"],
        )
        mock_buffer_file.assert_called_once_with(source_view, "", ANY)
