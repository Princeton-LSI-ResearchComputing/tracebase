from django.db.models import CharField, F, IntegerField
from django.db.models.functions import Lower, Upper
from django.templatetags.static import static
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst_list_view.sorter import BSTSorter

BSTSTestModel = create_test_model(
    "BSTSTestModel",
    {
        "name": CharField(max_length=255),
        "value": IntegerField(),
    },
)


class BSTSorterTests(TracebaseTestCase):
    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_charfield(self):
        s = BSTSorter(CharField(name="name"))
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_JS_ALPHANUMERIC, s.client_sorter)
        self.assertIsInstance(s.sort_expression, Lower)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_integerfield(self):
        s = BSTSorter(IntegerField(name="value"))
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_JS_NUMERIC, s.client_sorter)
        self.assertIsInstance(s.sort_expression, F)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_path_str_and_model(self):
        s = BSTSorter("value", model=BSTSTestModel)
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_JS_NUMERIC, s.client_sorter)
        self.assertIsInstance(s.sort_expression, F)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_path_f_and_model(self):
        s = BSTSorter(F("name"), model=BSTSTestModel)
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_JS_ALPHANUMERIC, s.client_sorter)
        self.assertIsInstance(s.sort_expression, Lower)

    @override_settings(DEBUG=True)
    def test_init_path_expression_and_model(self):
        with self.assertWarns(UserWarning) as aw:
            s = BSTSorter(Upper("name"), model=BSTSTestModel)
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "without a corresponding client_sorter", str(aw.warnings[0].message)
        )
        self.assertIn("Unable to select a client_sorter", str(aw.warnings[0].message))
        self.assertIn(
            "Server sort may differ from client sort", str(aw.warnings[0].message)
        )
        self.assertIn(
            "client_sorter based on the field type 'CharField'",
            str(aw.warnings[0].message),
        )
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual(BSTSorter.SORTER_JS_ALPHANUMERIC, s.client_sorter)
        self.assertIsInstance(s.sort_expression, Upper)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_path_expression_model_and_clientsorter(self):
        BSTSorter(Upper("name"), model=BSTSTestModel, client_sorter="upperSorter")

    @override_settings(DEBUG=True)
    def test_init_sorter_custom(self):
        with self.assertWarns(UserWarning) as aw:
            s = BSTSorter("value", client_sorter="mySorter")
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "field_expression (value) supplied without a model.",
            str(aw.warnings[0].message),
        )
        self.assertIn("Unable to determine field type", str(aw.warnings[0].message))
        self.assertIn(
            "transform ('Lower') that matches the client_sorter",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Server sort may differ from client sort", str(aw.warnings[0].message)
        )
        self.assertIn("Defaulting to expression as-is.", str(aw.warnings[0].message))
        self.assertEqual(BSTSorter.SORTER_JS_DJANGO, s.sorter)
        self.assertEqual("mySorter", s.client_sorter)
        self.assertIsInstance(s.sort_expression, F)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_str(self):
        self.assertEqual(
            BSTSorter.SORTER_JS_DJANGO, str(BSTSorter(CharField(name="name")))
        )

    def test_javascript(self):
        self.assertEqual(
            f"<script src='{static(BSTSorter.JAVASCRIPT)}'></script>",
            BSTSorter.javascript,
        )

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_set_client_mode(self):
        s = BSTSorter(CharField(name="name"))
        self.assertFalse(s.client_mode)
        s.set_client_mode()
        self.assertTrue(s.client_mode)
        s.set_client_mode(enabled=False)
        self.assertFalse(s.client_mode)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_set_server_mode(self):
        s = BSTSorter(CharField(name="name"))
        s.set_server_mode()
        self.assertFalse(s.client_mode)
        s.set_server_mode(enabled=False)
        self.assertTrue(s.client_mode)
