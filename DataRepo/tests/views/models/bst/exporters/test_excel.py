from contextlib import contextmanager
from io import BytesIO
from unittest.mock import Mock, patch

import pandas as pd

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst.export import BSTExportView
from DataRepo.views.models.bst.exporters.excel import ExcelBSTExportView


@contextmanager
def patch_exporter_classes(*exporter_classes):
    with patch.object(
        BSTExportView,
        "get_exporter_classes",
        return_value=list(exporter_classes),
    ):
        yield


class ExcelBSTExportViewTests(TracebaseTestCase):
    @patch("DataRepo.views.models.bst.exporters.excel.pd.DataFrame")
    @patch("DataRepo.views.models.bst.exporters.excel.pd.ExcelWriter")
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
        exporter.buffer_file(source_view, "header text", BytesIO())

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

        # Create a real exporter
        exporter = ExcelBSTExportView()

        # Mock the source view with representative export data.
        source_view = Mock()
        source_view.model_title_plural = "Animals"
        source_view.row_headers.return_value = ["Name", "Age"]
        source_view.rows_iterator.return_value = [
            ["Dog", 3],
            ["Cat", 5],
        ]

        buffer = BytesIO()

        # Generate an actual workbook in memory.
        exporter.buffer_file(source_view, "header", buffer)

        # Rewind the buffer so pandas can read from the beginning.
        buffer.seek(0)

        # Load the generated workbook and convert it back into records.
        df = pd.read_excel(buffer)

        # Verify that the exported workbook contains the expected data.
        self.assertEqual(
            [
                {"Name": "Dog", "Age": 3},
                {"Name": "Cat", "Age": 5},
            ],
            df.to_dict("records"),
        )
