from django.db.models import CharField, F, IntegerField
from django.db.models.functions import Lower, Upper
from django.templatetags.static import static
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst_list_view.column.sorter.field import BSTSorter

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
        s = BSTSorter(CharField(name="name"), BSTSTestModel)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.ALPHANUMERIC, s.client_sorter)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertIsInstance(s.sort_expression, Lower)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_integerfield(self):
        s = BSTSorter(IntegerField(name="value"), BSTSTestModel)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertIsInstance(s.sort_expression, F)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NUMERIC, s.client_sorter)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_path_str_and_model(self):
        s = BSTSorter("value", BSTSTestModel)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NUMERIC, s.client_sorter)
        self.assertIsInstance(s.sort_expression, F)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_path_f_and_model(self):
        s = BSTSorter(F("name"), BSTSTestModel)
        self.assertIsInstance(s.sort_expression, Lower)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.ALPHANUMERIC, s.client_sorter)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_path_expression_and_model(self):
        # We assert NOT warns because Upper has a default output_field type which we recognize and can apply our case
        # insensitivity to (using Lower).  This is a nonsensical example, but where this makes sense is when for
        # example, fields are being concatenated or other operations are happening.  The point is that 'Lower' is
        # applied if the **output_field** is a compatible type.
        s = BSTSorter(Upper("name"), BSTSTestModel)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.ALPHANUMERIC, s.client_sorter)
        self.assertIsInstance(s.sort_expression, Lower)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_path_expression_model_and_clientsorter(self):
        BSTSorter(Upper("name"), BSTSTestModel, client_sorter="upperSorter")

    @override_settings(DEBUG=True)
    def test_init_sorter_custom(self):
        with self.assertWarns(UserWarning) as aw:
            s = BSTSorter("value", BSTSTestModel, client_sorter="mySorter")
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "server-side Django sort expression 'F(value)' and client_sorter 'mySorter'",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Server sort may differ from client sort",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "set the field_expression and/or client_sorter to match",
            str(aw.warnings[0].message),
        )
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertEqual("mySorter", s.client_sorter)
        self.assertIsInstance(s.sort_expression, F)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_str(self):
        self.assertEqual(
            BSTSorter.CLIENT_SORTERS.NONE,
            str(BSTSorter(CharField(name="name"), BSTSTestModel)),
        )

    def test_javascript(self):
        self.assertEqual(
            f"<script src='{static(BSTSorter.JAVASCRIPT)}'></script>",
            BSTSorter.javascript,
        )

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_set_client_mode(self):
        s = BSTSorter(CharField(name="name"), BSTSTestModel)
        self.assertFalse(s.client_mode)
        s.set_client_mode()
        self.assertTrue(s.client_mode)
        s.set_client_mode(enabled=False)
        self.assertFalse(s.client_mode)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_set_server_mode(self):
        s = BSTSorter(CharField(name="name"), BSTSTestModel)
        s.set_server_mode()
        self.assertFalse(s.client_mode)
        s.set_server_mode(enabled=False)
        self.assertTrue(s.client_mode)
