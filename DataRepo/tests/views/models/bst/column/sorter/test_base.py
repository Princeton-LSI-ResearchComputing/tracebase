from django.db.models import CharField, DateField, IntegerField, Value
from django.db.models.aggregates import Count
from django.db.models.functions import Lower, Trunc
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst.column.sorter.base import BSTBaseSorter


class TestAnnotSorter(BSTBaseSorter):
    is_annotation = True


class TestFieldSorter(BSTBaseSorter):
    pass


@override_settings(DEBUG=True)
class BSTBaseSorterTests(TracebaseTestCase):
    def test_init_annot_name(self):
        self.assertEqual(
            "whatever" + BSTBaseSorter.sort_annot_suffix,
            TestAnnotSorter("whatever", Value(1)).annot_name,
        )
        self.assertEqual(
            "whatever" + BSTBaseSorter.sort_annot_suffix,
            TestFieldSorter("whatever", Value(1)).annot_name,
        )

    def test_is_sort_annotation(self):
        self.assertTrue(
            BSTBaseSorter.is_sort_annotation(
                "whatever" + BSTBaseSorter.sort_annot_suffix
            )
        )
        self.assertFalse(BSTBaseSorter.is_sort_annotation("whatever"))

    def test_sort_annot_name_to_col_name(self):
        self.assertEqual(
            "whatever",
            BSTBaseSorter.sort_annot_name_to_col_name(
                "whatever" + BSTBaseSorter.sort_annot_suffix
            ),
        )
        self.assertEqual(
            "whatever",
            BSTBaseSorter.sort_annot_name_to_col_name("whatever"),
        )

    def test_init_transform_server_sorter(self):
        tas1 = TestAnnotSorter(Lower("name", output_field=CharField()))
        self.assertEqual(BSTBaseSorter.SERVER_SORTERS.ALPHANUMERIC, tas1._server_sorter)
        tas2 = TestAnnotSorter(Count("name", output_field=IntegerField()))
        self.assertEqual(BSTBaseSorter.SERVER_SORTERS.NUMERIC, tas2._server_sorter)
        tas3 = TestAnnotSorter(Trunc("date", kind="year", output_field=DateField()))
        self.assertEqual(Trunc, tas3._server_sorter)
