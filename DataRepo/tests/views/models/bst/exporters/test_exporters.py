from contextlib import contextmanager
from io import BytesIO, StringIO
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
from django.db.models import CharField
from django.template.backends.django import Template
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
    ExcelBSTExportView,
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


@contextmanager
def patch_exporter_classes(*exporter_classes):
    with patch.object(
        BSTExportView,
        "get_exporter_classes",
        return_value=list(exporter_classes),
    ):
        yield


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
            buffer_class = StringIO
            extension = "csv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        BSTCSVExportView()

    def test_init_subclass_noname(self):
        with self.assertRaises(TypeError):

            class BSTCSVExportView(BSTExportView):
                # No name <- invalid
                content_type = "text/csv"
                buffer_class = StringIO
                extension = "csv"

                def buffer_file(
                    self, source_view: BSTExportedListView, header_content: str
                ):
                    pass

            BSTCSVExportView()

    def test_init_subclass_bufferinstance(self):
        with self.assertRaises(TypeError):

            class BSTCSVExportView(BSTExportView):
                name = "CSV"
                content_type = "text/csv"
                buffer = StringIO()  # type: ignore[assignment]
                extension = "csv"

                def buffer_file(
                    self, source_view: BSTExportedListView, header_content: str
                ):
                    pass

            BSTCSVExportView()

    def test_init_subclass_novalue(self):
        with self.assertRaises(TypeError):

            class BSTCSVExportView(BSTExportView):
                name = "CSV"
                content_type: str  # type: ignore[misc]
                buffer_class = StringIO
                extension = "csv"

                def buffer_file(
                    self, source_view: BSTExportedListView, header_content: str
                ):
                    pass

            BSTCSVExportView()

    def test_init_subclass_wrongtype(self):
        with self.assertRaises(TypeError):

            class BSTCSVExportView(BSTExportView):
                name = "CSV"
                content_type = "text/csv"
                buffer_class = StringIO
                extension = 1

                def buffer_file(
                    self, source_view: BSTExportedListView, header_content: str
                ):
                    pass

            BSTCSVExportView()

    def test_get_exporter_classes(self):
        class ExpView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer_class = StringIO
            extension = "csv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        classes = BSTExportView.get_exporter_classes()
        self.assertIn(ExpView, classes)

    def test_gather_exporters_works(self):
        """Asserts that gather_exporters builds a dict of exporter classes keyed on export name."""

        class BSTTSVExportView(BSTExportView):
            name = "TSV"
            content_type = "text/tsv"
            buffer_class = StringIO
            extension = "tsv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer_class = StringIO
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
            buffer_class = StringIO
            extension = "csv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        class BSTCSVExportView2(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer_class = StringIO
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
            buffer_class = StringIO
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
        exporter.buffer = BytesIO()
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
        mock_buffer_file.assert_called_once_with(source_view, "HEADER")

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
        exporter.buffer = BytesIO()
        exporter.export_file = "test"

        # Mock init_export() so that this test focuses only on the branch that
        # determines the header content passed to buffer_file().
        with patch.object(exporter, "init_export"):
            exporter.get(Mock())

        # Verify that an empty header string was supplied when no template exists.
        mock_buffer_file.assert_called_once_with(source_view, "")

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

        # Create a concrete exporter to test with.
        exporter = ExcelBSTExportView()

        # Disable header rendering because it is not relevant to this test.
        exporter.download_header_template = None

        # Populate the values that would normally be set by init_export().
        exporter.export_file = "test.xlsx"
        exporter.buffer = BytesIO(b"excel-bytes")

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
            "attachment; filename='test.xlsx'",
            response["Content-Disposition"],
        )
        mock_buffer_file.assert_called_once_with(source_view, "")


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


class ExcelBSTExportViewTests(TracebaseTestCase):
    @patch("DataRepo.views.models.bst.exporters.exporters.pd.DataFrame")
    @patch("DataRepo.views.models.bst.exporters.exporters.pd.ExcelWriter")
    def test_buffer_file(
        self,
        mock_excel_writer_cls,
        mock_dataframe_cls,
    ):
        """Verify that buffer_file() transforms exported rows into a dataframe, writes the dataframe to an Excel
        worksheet, populates workbook metadata, applies worksheet formatting, and saves the workbook.
        """
        # Create the exporter being tested.
        exporter = ExcelBSTExportView()

        # Normally init_export() creates the buffer.  We provide one directly because this test is only exercising
        # buffer_file().
        exporter.buffer = BytesIO()

        # Mock the source view so that we control the worksheet name, column headers, and exported rows.
        source_view = Mock()
        source_view.model_title_plural = "Animals"
        source_view.row_headers.return_value = ["Name", "Age"]
        source_view.rows_iterator.return_value = [
            ["Dog", 3],
            ["Cat", 5],
        ]

        # Mock the ExcelWriter instance returned by pandas.  The implementation uses the writer's workbook object and
        # worksheet dictionary, so those need to exist.
        mock_writer = Mock()
        mock_writer.sheets = {"Animals": Mock()}
        mock_excel_writer_cls.return_value = mock_writer

        # Mock the dataframe instance returned by DataFrame.from_dict(). This allows us to verify that the expected
        # export data is passed into pandas without actually generating an Excel file.
        mock_dataframe = Mock()
        mock_dataframe_cls.from_dict.return_value = mock_dataframe

        # Execute the method under test.
        exporter.buffer_file(source_view, "header text")

        # Verify that an Excel writer was created using the exporter's buffer and the expected engine.
        mock_excel_writer_cls.assert_called_once_with(
            exporter.buffer,
            engine="xlsxwriter",
        )

        # Verify that workbook metadata is populated from the source view and supplied header content.
        mock_writer.book.set_properties.assert_called_once_with(
            {
                "title": "Animals",
                "author": "Robert Leach",
                "company": "Princeton University",
                "comments": "header text",
            }
        )

        # Verify that rows were transformed into the expected column-oriented dictionary before dataframe creation.
        mock_dataframe_cls.from_dict.assert_called_once_with(
            {
                "Name": ["Dog", "Cat"],
                "Age": ["3", "5"],
            }
        )

        # Verify that the dataframe was exported to the expected sheet using the expected column ordering.
        mock_dataframe.to_excel.assert_called_once_with(
            excel_writer=mock_writer,
            sheet_name="Animals",
            columns=["Name", "Age"],
            index=False,
        )

        # Verify that worksheet formatting and workbook finalization were performed.
        mock_writer.sheets["Animals"].autofit.assert_called_once_with()
        mock_writer.save.assert_called_once_with()

    def test_excel_export_view_attributes(self):
        """Verify that the Excel exporter declares the expected export metadata, including its export name, file
        extension, content type, and buffer implementation.
        """
        # These values are consumed by exporter discovery and response generation logic elsewhere in the application.
        self.assertEqual("Excel", ExcelBSTExportView.name)
        self.assertIs(BytesIO, ExcelBSTExportView.buffer_class)
        self.assertEqual("xlsx", ExcelBSTExportView.extension)
        self.assertEqual(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ExcelBSTExportView.content_type,
        )

    def test_excel_export_view_registered(self):
        """Verify that the Excel exporter is discoverable through exporter registration and is associated with its
        advertised export name.
        """
        # Gather all concrete exporter implementations.
        with patch_exporter_classes(ExcelBSTExportView):
            exporters = BSTExportView.gather_exporters()

        # Verify that the Excel exporter is discoverable using its advertised export name.
        self.assertIn("Excel", exporters)

        # Verify that discovery returns the expected concrete class.
        self.assertIs(exporters["Excel"], ExcelBSTExportView)

    def test_buffer_file_creates_valid_xlsx(self):
        """Verify that buffer_file() generates a valid Excel workbook containing the expected worksheet data when given
        representative export rows.
        """

        # Create a real exporter and backing buffer.
        exporter = ExcelBSTExportView()
        exporter.buffer = BytesIO()

        # Mock the source view with representative export data.
        source_view = Mock()
        source_view.model_title_plural = "Animals"
        source_view.row_headers.return_value = ["Name", "Age"]
        source_view.rows_iterator.return_value = [
            ["Dog", 3],
            ["Cat", 5],
        ]

        # Generate an actual workbook in memory.
        exporter.buffer_file(source_view, "header")

        # Rewind the buffer so pandas can read from the beginning.
        exporter.buffer.seek(0)

        # Load the generated workbook and convert it back into records.
        df = pd.read_excel(exporter.buffer)

        # Verify that the exported workbook contains the expected data.
        self.assertEqual(
            [
                {"Name": "Dog", "Age": 3},
                {"Name": "Cat", "Age": 5},
            ],
            df.to_dict("records"),
        )
