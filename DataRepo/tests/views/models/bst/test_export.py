from unittest.mock import MagicMock, patch

from django.test import RequestFactory

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.export import BSTExportedListView, NoExporters
from DataRepo.views.models.bst.exporters.exporters import BSTExportView


class BSTExportedListViewTests(TracebaseTestCase):
    def test_bstexportedlistview_noexporters(self):
        """Test that the constructor raises NoExporters when no export format derived subclasses exist.
        See Test 1 in https://princeton-university.atlassian.net/wiki/x/IIAWH
        """
        # Control the conditions by overriding the behavior of get_exporter_classes
        with patch.object(
            BSTExportedListView,
            "get_exporter_classes",
            return_value=[],
        ):
            with self.assertRaises(NoExporters):
                BSTExportedListView()

    def test_bstexportedlistview_warns_when_get_exporters_invalid(self):
        class BSTCSVExportView1(BSTExportView):
            @classmethod
            def get_exporters(cls):
                # This is invalid <- This is what is tested
                pass

        class BSTCSVExportView2(BSTExportView):
            @classmethod
            def get_exporters(cls):
                return {"csv": __class__}

        # Control the conditions by overriding the behavior of get_exporter_classes
        with patch.object(
            BSTExportedListView,
            "get_exporter_classes",
            return_value=[BSTCSVExportView1, BSTCSVExportView2],
        ):
            with self.assertWarns(DeveloperWarning):
                BSTExportedListView()

    def test_bstexportedlistview_setup(self):
        """Assert that export_enabled and javascripts are correctly populated."""

        class BSTCSVExportView:
            @classmethod
            def get_exporters(cls):
                return {"csv": __class__}

        # Control the conditions by overriding the behavior of get_exporter_classes
        with patch.object(
            BSTExportedListView,
            "get_exporter_classes",
            return_value=[BSTCSVExportView],
        ):
            bstelv = BSTExportedListView()

        self.assertTrue(bstelv.export_enabled)
        self.assertIn("js/bst/exporter.js", bstelv.javascripts)

    def test_get_exporter_classes(self):
        class ExpView(BSTExportView):
            pass

        class ExtendedFeatureView(BSTExportedListView):
            pass

        classes = BSTExportedListView.get_exporter_classes()

        self.assertIn(ExpView, classes)
        self.assertNotIn(ExtendedFeatureView, classes)

    def test_gather_exporters_works(self):
        """Asserts that gather_exporters builds a dict of exporter classes keyed on export name."""

        class BSTCSVExportView1(BSTExportView):
            @classmethod
            def get_exporters(cls):
                return {"tsv": __class__}

        class BSTCSVExportView2(BSTExportView):
            @classmethod
            def get_exporters(cls):
                return {"csv": __class__}

        # Control the conditions by overriding the behavior of get_exporter_classes
        with patch.object(
            BSTExportedListView,
            "get_exporter_classes",
            return_value=[BSTCSVExportView1, BSTCSVExportView2],
        ):
            bstelv = BSTExportedListView()
            self.assertEquivalent(
                {
                    "tsv": BSTCSVExportView1,
                    "csv": BSTCSVExportView2,
                },
                bstelv.gather_exporters(),
            )

    def test_gather_exporters_raises_on_duplicates(self):
        """Asserts that gather_exporters raises a KeyError when the subclasses have duplicate export names
        (e.g. 'csv').
        """

        class BSTCSVExportView1(BSTExportView):
            @classmethod
            def get_exporters(cls):
                return {"csv": __class__}

        class BSTCSVExportView2(BSTExportView):
            @classmethod
            def get_exporters(cls):
                return {"csv": __class__}

        # Control the conditions by overriding the behavior of get_exporter_classes
        with patch.object(
            BSTExportedListView,
            "get_exporter_classes",
            return_value=[BSTCSVExportView1, BSTCSVExportView2],
        ):
            with self.assertRaises(KeyError):
                BSTExportedListView()

    def test_get_exporters(self):
        """Assert that get_exporters in the bas class is not implemented (i.e. should only contain `pass`)."""
        self.assertIsNone(BSTExportedListView.get_exporters())

    @patch("DataRepo.views.models.bst.export.reverse")
    def test_get_context_data(self, mock_reverse: MagicMock):
        mock_reverse.side_effect = lambda name: f"/url/{name}/"

        # This creates a GET request.  The URL argument doesn't matter.  We just want the request object, with a little
        # bit of setup.
        request = RequestFactory().get("/")

        class BSTCSVExportView(BSTExportView):
            name = "CSV"

            @classmethod
            def get_exporters(cls):
                return {"csv": __class__}

        class BSTTSVExportView(BSTExportView):
            name = "TSV"

            @classmethod
            def get_exporters(cls):
                return {"tsv": __class__}

        with patch.object(
            BSTExportedListView,
            "get_exporter_classes",
            return_value=[BSTCSVExportView, BSTTSVExportView],
        ):
            bstelv = BSTExportedListView(request=request)
            bstelv.export_enabled = True
            bstelv.export_enabled_var_name = "export_enabled"
            bstelv.export_types_var_name = "export_types"
            bstelv.exporters = [BSTCSVExportView(), BSTTSVExportView()]
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
