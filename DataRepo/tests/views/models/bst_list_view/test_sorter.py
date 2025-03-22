from django.db.models import CharField, IntegerField
from django.db.models.functions import Lower, Upper
from django.templatetags.static import static
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst_list_view.sorter import BSTSorter


class BSTSorterTests(TracebaseTestCase):
    def test_init_none(self):
        s = BSTSorter()
        self.assertEqual(BSTSorter.identity, s.transform)
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_JS_ALPHANUMERIC, s.client_sorter)

    def test_init_charfield(self):
        s = BSTSorter(field=CharField())
        self.assertEqual(Lower, s.transform)
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_JS_ALPHANUMERIC, s.client_sorter)

    def test_init_integerfield(self):
        s = BSTSorter(field=IntegerField())
        self.assertEqual(BSTSorter.identity, s.transform)
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_JS_NUMERIC, s.client_sorter)

    def test_init_transform(self):
        s = BSTSorter(transform=Upper)
        self.assertEqual(Upper, s.transform)
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_JS_ALPHANUMERIC, s.client_sorter)

    def test_init_sorter_bst(self):
        s = BSTSorter(client_sorter=BSTSorter.SORTER_BST_NUMERIC)
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_BST_NUMERIC, s.client_sorter)

    @override_settings(DEBUG=True)
    def test_init_sorter_custom(self):
        with self.assertWarns(UserWarning):
            s = BSTSorter(client_sorter="mySorter")
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual("mySorter", s.client_sorter)

    def test_init_invalid_transform_object(self):
        with self.assertRaises(ValueError) as ar:
            BSTSorter(transform="name")
        ve = ar.exception
        self.assertEqual(
            "transform must be a Combinable, e.g. type Transform.", str(ve)
        )

    def test_init_invalid_transform_class(self):
        with self.assertRaises(ValueError) as ar:
            BSTSorter(transform=str)
        ve = ar.exception
        self.assertEqual(
            "transform must be a Combinable, e.g. type Transform.", str(ve)
        )

    def test_identity_transform(self):
        order_by_val = BSTSorter.identity(Upper("name"))
        self.assertEqual(Upper("name"), order_by_val)

    def test_identity_str(self):
        order_by_val = BSTSorter.identity("-name")
        self.assertEqual("-name", order_by_val)

    def test_str(self):
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, str(BSTSorter()))

    def test_javascript(self):
        self.assertEqual(
            f"<script src='{static(BSTSorter.JAVASCRIPT)}'></script>",
            BSTSorter.javascript,
        )
