from contextlib import contextmanager
from io import StringIO
from typing import cast
from unittest.mock import MagicMock, Mock, patch

from django.db.models import (
    CASCADE,
    CharField,
    ForeignKey,
    IntegerField,
    ManyToManyField,
)
from django.db.models.aggregates import Count
from django.http import HttpRequest
from django.test import RequestFactory

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.field import BSTColumn
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.many_related_group import BSTColumnGroup
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn
from DataRepo.views.models.bst.export import BSTExportedListView
from DataRepo.views.models.bst.exporters.base import BSTExportView, NoExporters
from DataRepo.views.models.bst.exporters.delimited.base import (
    TextBSTExportView,
)

BSTELVStudyTestModel = create_test_model(
    "BSTELVStudyTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader"},
        ),
    },
)

BSTELVAnimalTestModel = create_test_model(
    "BSTELVAnimalTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
        "studies": ManyToManyField(
            to="loader.BSTELVStudyTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.BSTELVTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
        ),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": ["-name"]},
        ),
    },
)

BSTELVTreatmentTestModel = create_test_model(
    "BSTELVTreatmentTestModel",
    {"name": CharField(unique=True), "desc": CharField()},
)


class StudyELV(BSTExportedListView):
    model = BSTELVStudyTestModel


# Must create a concrete export view because the AnimalWithMultipleStudyColsLV constructor calls
# self.export_view_class.gather_exporters()
class BSTTSVExportView(TextBSTExportView):
    name = "TSV"
    extension = "tsv"
    view_name = "tsv_exp_list_view"
    delim = "\t"


@contextmanager
def patch_exporter_classes(*exporter_classes):
    with patch.object(
        BSTExportView,
        "get_exporter_classes",
        return_value=list(exporter_classes),
    ):
        yield


class AnimalWithMultipleStudyColsLV(BSTExportedListView):
    model = BSTELVAnimalTestModel
    column_ordering = ["name", "desc", "treatment", "studies__name", "studies__desc"]
    exclude = ["id", "studies"]

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            columns=[
                BSTColumnGroup(
                    BSTManyRelatedColumn(
                        "studies__name", AnimalWithMultipleStudyColsLV.model
                    ),
                    BSTManyRelatedColumn(
                        "studies__desc", AnimalWithMultipleStudyColsLV.model
                    ),
                ),
            ],
            **kwargs,
        )


class BSTExportedListViewTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        cls.t1 = BSTELVTreatmentTestModel.objects.create(name="T1", desc="t1")
        cls.t2 = BSTELVTreatmentTestModel.objects.create(name="oddball", desc="t2")
        cls.s1 = BSTELVStudyTestModel.objects.create(name="S1", desc="s1")
        cls.s2 = BSTELVStudyTestModel.objects.create(name="S2", desc="s2")
        cls.a1 = BSTELVAnimalTestModel.objects.create(
            name="A1", desc="a1", treatment=cls.t1
        )
        cls.a1.studies.add(cls.s1)
        cls.a2 = BSTELVAnimalTestModel.objects.create(
            name="A2", desc="a2", treatment=cls.t2
        )
        cls.a2.studies.add(cls.s1)
        cls.a2.studies.add(cls.s2)
        super().setUpTestData()

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
            buffer_class = StringIO
            extension = "csv"
            view_name = "csv_exp_list_view"

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
        request.resolver_match = Mock(view_name="test-view")

        class BSTCSVExportView(TextBSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer_class = StringIO
            extension = "csv"
            view_name = "csv_exp_list_view"

        class BSTTSVExportView(TextBSTExportView):
            name = "TSV"
            content_type = "text/Tsv"
            buffer_class = StringIO
            extension = "tsv"
            view_name = "tsv_exp_list_view"

        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[BSTCSVExportView, BSTTSVExportView],
        ):
            bstelv = BSTExportedListView(request=request)
            bstelv.export_enabled = True
            bstelv.export_enabled_var_name = "export_enabled"
            bstelv.export_types_var_name = "export_types"
            bstelv.exporters = {
                # mypy has issues typing ABC-derived concrete classes defined in methods.
                "CSV": cast(type[BSTExportView], BSTCSVExportView),
                "TSV": cast(type[BSTExportView], BSTTSVExportView),
            }
            bstelv.object_list = []

        context = bstelv.get_context_data()

        self.assertTrue(context[bstelv.export_enabled_var_name])

        self.assertEqual(
            [
                {
                    "name": "CSV",
                    "url": "/url/csv_exp_list_view/?source=test-view",
                },
                {
                    "name": "TSV",
                    "url": "/url/tsv_exp_list_view/?source=test-view",
                },
            ],
            context[bstelv.export_types_var_name],
        )

        self.assertEqual(2, mock_reverse.call_count)

    def test_get_column_val(self):
        """Asserts that get_column_val can retrieve values for all 4 column types from the root model in the
        BSTExportedListView"""

        with patch_exporter_classes(BSTTSVExportView):
            bealv = AnimalWithMultipleStudyColsLV()
            bealv.init_interface()
            qs = bealv.get_queryset()
            for rec in qs.all():
                if rec.name == "A1":
                    animal_rec = rec
                    break

            namecol = BSTColumn("name", BSTELVAnimalTestModel)
            nameval = bealv.get_column_val(animal_rec, namecol)
            self.assertEqual("A1", nameval)

            trtdsccol = BSTRelatedColumn("treatment__desc", BSTELVAnimalTestModel)
            trtdscval = bealv.get_column_val(animal_rec, trtdsccol)
            self.assertEqual("t1", trtdscval)

            stdynmcol = BSTManyRelatedColumn("studies__name", BSTELVAnimalTestModel)
            stdynmval = bealv.get_column_val(animal_rec, stdynmcol)
            self.assertEqual("S1", stdynmval)

            stdycntcol = BSTAnnotColumn(
                BSTManyRelatedColumn.get_count_name("studies", bealv.model),
                Count("studies", distinct=True, output_field=IntegerField()),
                header="Studies Count",
                filterer="strictFilterer",
                sorter="numericSorter",
            )
            stdycntval = bealv.get_column_val(animal_rec, stdycntcol)
            self.assertEqual("1", stdycntval)

    def test_row_headers(self):
        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[BSTTSVExportView],
        ):
            bealv = AnimalWithMultipleStudyColsLV()
        row_headers = bealv.row_headers()
        self.assertEqual(
            [
                "BSTELV Animal Test Model",
                "Desc",
                "Treatment",
                "Studies Count",
                "Studies",
                "Descs",
            ],
            row_headers,
        )

    def test_rows_iterator(self):
        with patch.object(
            BSTExportView,
            "get_exporter_classes",
            return_value=[BSTTSVExportView],
        ):
            bealv = AnimalWithMultipleStudyColsLV()
            bealv.init_interface()

            rows = [r for r in bealv.rows_iterator()]
            self.assertEqual(
                [
                    [
                        "BSTELV Animal Test Model",
                        "Desc",
                        "Treatment",
                        "Studies Count",
                        "Studies",
                        "Descs",
                    ],
                    ["A2", "a2", "oddball", "2", "S1; S2", "s1; s2"],
                    ["A1", "a1", "T1", "1", "S1", "s1"],
                ],
                rows,
            )

    def test_rec_to_row(self):
        with patch_exporter_classes(BSTTSVExportView):
            bealv = AnimalWithMultipleStudyColsLV()
            bealv.init_interface()
            qs = bealv.get_queryset()
            for rec in qs.all():
                if rec.name == "A1":
                    animal_rec = rec
                    break

            row = bealv.rec_to_row(animal_rec)
            self.assertEqual(["A1", "a1", "T1", "1", "S1", "s1"], row)

    @staticmethod
    def parse_export(content: str):
        """Split an exported file into metadata, column header, and data rows.
        This method supports test_export_download_header_matches_exported_data.
        """

        lines = content.splitlines()

        metadata = []
        data_start = None

        for i, line in enumerate(lines):
            # Skip metadata comments
            if line.startswith("#"):
                metadata.append(line)
                continue

            # Skip blank separator lines
            if not line.strip():
                continue

            data_start = i
            break

        if data_start is None:
            raise AssertionError("No tabular data found in export.")

        column_header = lines[data_start]
        data_start += 1
        data_rows = lines[data_start:]

        return metadata, column_header, data_rows

    @patch("DataRepo.views.models.bst.exporters.base.resolve")
    @patch("DataRepo.views.models.bst.exporters.base.reverse")
    def test_export_download_header_matches_exported_data(
        self,
        mock_reverse,
        mock_resolve,
    ):
        """Regression test that exercises the BSTExportView.get() request path.

        This test verifies that:

        1. The source view is resolved and queried correctly.
        2. The metadata header reflects the exported dataset.
        3. The reported row count matches the actual exported row count.
        4. The exported data matches the applied filters.

        URL resolution is mocked so this test remains focused on the export
        framework and does not depend on any particular URL configuration.
        """

        # Mock the URL resolution performed by BSTExportView.get_source_view().
        # The specific source name is irrelevant because reverse() and resolve()
        # are mocked.
        mock_reverse.return_value = "/fake/source/view/"

        resolved_match = Mock()
        resolved_match.func.view_class = StudyELV
        mock_resolve.return_value = resolved_match

        request = HttpRequest()

        request.GET = {
            # Arbitrary source name.  reverse() and resolve() are mocked.
            "source": "fake_source_view",
        }

        # Simulate the filters/search/sort a user would have applied on the list
        # view before downloading the export.
        request.COOKIES.update(
            {
                f"{StudyELV.__name__}-{StudyELV.search_cookie_name}": "S1",
                f"{StudyELV.__name__}-{StudyELV.filter_cookie_name}-name": "S1",
                f"{StudyELV.__name__}-{StudyELV.sortcol_cookie_name}": "name",
                f"{StudyELV.__name__}-{StudyELV.asc_cookie_name}": "true",
            }
        )

        with patch_exporter_classes(BSTTSVExportView):
            # Determine the expected row count using the same source view that
            # the export view will resolve.
            source_view = StudyELV(request=request)
            source_view.init_interface()

            expected_count = source_view.get_queryset().count()

            # Execute the actual export view request path.
            export_view = BSTTSVExportView()

            response = export_view.get(request)

        content = b"".join(response.streaming_content).decode("utf-8")

        metadata, column_header, data_rows = self.parse_export(content)

        # Verify metadata generated from the source view state.
        self.assertIn(f"# Rows: {expected_count}", metadata)
        self.assertIn("# Global Search Term: 'S1'", metadata)
        self.assertIn("#   name: 'S1'", metadata)
        self.assertIn("# Sort: name, ascending", metadata)

        # Regression check for the bug where the header row count and exported
        # data row count diverged.
        self.assertEqual(expected_count, len(data_rows))

        # Verify the exported dataset reflects the applied filter.
        exported_text = "\n".join(data_rows)

        self.assertIn("S1", exported_text)
        self.assertNotIn("S2", exported_text)

        # Sanity checks.
        self.assertGreater(len(column_header), 0)
        self.assertEqual(200, response.status_code)
