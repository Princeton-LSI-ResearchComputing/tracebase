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
from DataRepo.views.models.bst.exporters.exporters import (
    BSTExportView,
    NoExporters,
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


# Must create a concrete export view because the AnimalWithMultipleStudyColsLV constructor calls
# self.export_view_class.gather_exporters()
class BSTTSVExportView(BSTExportView):
    name = "TSV"
    content_type = "text/tsv"
    buffer_class = StringIO
    extension = "tsv"

    def buffer_file(self, source_view: BSTExportedListView, header_content: str):
        pass


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

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
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
        request.resolver_match = Mock(view_name="test-view")

        class BSTCSVExportView(BSTExportView):
            name = "CSV"
            content_type = "text/csv"
            buffer_class = StringIO
            extension = "csv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
                pass

        class BSTTSVExportView(BSTExportView):
            name = "TSV"
            content_type = "text/Tsv"
            buffer_class = StringIO
            extension = "tsv"

            def buffer_file(
                self, source_view: BSTExportedListView, header_content: str
            ):
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
                    "url": "/url/BSTCSVExportView/?source=test-view",
                },
                {
                    "name": "TSV",
                    "url": "/url/BSTTSVExportView/?source=test-view",
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
