from io import StringIO
from unittest.mock import MagicMock, patch

from django.db.models import CharField
from django.test import RequestFactory
from django.urls import NoReverseMatch

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst.export import BSTExportedListView
from DataRepo.views.models.bst.exporters.exporters import (
    BSTExportView,
    CSVBSTExportView,
    TSVBSTExportView,
)

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


class StudyELV(BSTExportedListView):
    model = BSTEVStudyTestModel


class BSTExportViewTests(TracebaseTestCase):
    def test_bstexportview_abstract(self):
        with self.assertRaises(TypeError) as ar:
            # Disable the linter so we can test it croaks
            BSTExportView()  # pylint: disable=abstract-class-instantiated
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

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        BSTCSVExportView()

    def test_init_subclass_noname(self):
        class BSTCSVExportView(BSTExportView):
            # No name <- invalid
            content_type = "text/csv"
            buffer = StringIO
            extension = "csv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        with self.assertRaises(TypeError):
            BSTCSVExportView()

    def test_init_subclass_bufferinstance(self):
        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO()  # type: ignore[assignment]
            extension = "csv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        with self.assertRaises(TypeError):
            BSTCSVExportView()

    def test_init_subclass_novalue(self):
        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type: str  # type: ignore[misc]
            buffer = StringIO
            extension = "csv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        with self.assertRaises(TypeError):
            BSTCSVExportView()

    def test_init_subclass_wrongtype(self):
        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO
            extension = 1

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        with self.assertRaises(TypeError):
            BSTCSVExportView()

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

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO
            extension = "csv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
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

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        class BSTCSVExportView2(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO
            extension = "csv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

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
            buffer = StringIO
            extension = "tsv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[BSTTSVExportView],
        ):
            btev = BSTTSVExportView()
            source_view = StudyELV()
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
            self.assertEqual("BSTLV Study Test Models", context["table_name"])
            self.assertIn("-", context["timestamp"])
            self.assertIn(":", context["timestamp"])
            self.assertEqual(0, context["total"])

    @patch("DataRepo.views.models.bst.exporters.exporters.resolve")
    @patch("DataRepo.views.models.bst.exporters.exporters.reverse")
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

    @patch("DataRepo.views.models.bst.exporters.exporters.reverse")
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


class CSVBSTExportViewTests(TracebaseTestCase):
    def test_buffer_file_writes_header_and_rows(self):
        exporter = CSVBSTExportView()
        exporter.buffer = StringIO()

        source_view = MagicMock()
        source_view.rows_iterator.return_value = [
            ["a", "b"],
            ["c", "d"],
        ]

        exporter.buffer_file(source_view, "# HEADER\n")

        self.assertEqual(
            exporter.buffer.getvalue(),
            "# HEADER\na,b\r\nc,d\r\n",
        )

    def test_buffer_file_stringifies_values(self):
        exporter = CSVBSTExportView()
        exporter.buffer = StringIO()

        source_view = MagicMock()
        source_view.rows_iterator.return_value = [[1, True, None]]

        exporter.buffer_file(source_view, "")

        self.assertEqual(
            exporter.buffer.getvalue(),
            "1,True,None\r\n",
        )


class TSVBSTExportViewTests(TracebaseTestCase):
    def test_buffer_file_uses_tab_delimiter(self):
        exporter = TSVBSTExportView()
        exporter.buffer = StringIO()

        source_view = MagicMock()
        source_view.rows_iterator.return_value = [["a", "b"]]

        exporter.buffer_file(source_view, "")

        self.assertEqual(
            exporter.buffer.getvalue(),
            "a\tb\r\n",
        )
