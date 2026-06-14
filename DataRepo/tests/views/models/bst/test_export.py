from io import StringIO
from unittest.mock import MagicMock, patch

from django.test import RequestFactory

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst.export import BSTExportedListView
from DataRepo.views.models.bst.exporters.exporters import (
    BSTExportView,
    NoExporters,
)


class BSTExportedListViewTests(TracebaseTestCase):
    def test_bstexportedlistview_noexporters(self):
        """Test that the constructor raises NoExporters when no export format derived subclasses exist.
        See Test 1 in https://princeton-university.atlassian.net/wiki/x/IIAWH
        """
        # Control the conditions by overriding the behavior of get_exporter_classes
        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[],
        ):
            with self.assertRaises(NoExporters):
                BSTExportedListView()

    def test_bstexportedlistview_setup(self):
        """Assert that export_enabled and javascripts are correctly populated."""

        class BSTCSVExportView:
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
            return_value=[BSTCSVExportView],
        ):
            bstelv = BSTExportedListView()

        self.assertTrue(bstelv.export_enabled)
        self.assertIn("js/bst/exporter.js", bstelv.javascripts)

    @patch("DataRepo.views.models.bst.export.reverse")
    def test_get_context_data(self, mock_reverse: MagicMock):
        mock_reverse.side_effect = lambda name: f"/url/{name}/"

        # This creates a GET request.  The URL argument doesn't matter.  We just want the request object, with a little
        # bit of setup.
        request = RequestFactory().get("/")

        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer = StringIO
            extension = "csv"

            def buffer_file(self, header_content: str):
                pass

        class BSTTSVExportView(BSTExportView):
            name = "TSV"
            content_type = "text/Tsv"
            buffer = StringIO
            extension = "tsv"

            def buffer_file(self, header_content: str):
                pass

        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[BSTCSVExportView, BSTTSVExportView],
        ):
            bstelv = BSTExportedListView(request=request)
            bstelv.export_enabled = True
            bstelv.export_enabled_var_name = "export_enabled"
            bstelv.export_types_var_name = "export_types"
            bstelv.exporters = {"CSV": BSTCSVExportView, "TSV": BSTTSVExportView}
            bstelv.object_list = []

        context = bstelv.get_context_data()

        self.assertTrue(context[bstelv.export_enabled_var_name])

        self.assertEqual(
            [
                {
                    "name": "CSV",
                    "url": "/url/BSTCSVExportView/",
                },
                {
                    "name": "TSV",
                    "url": "/url/BSTTSVExportView/",
                },
            ],
            context[bstelv.export_types_var_name],
        )

        self.assertEqual(2, mock_reverse.call_count)
