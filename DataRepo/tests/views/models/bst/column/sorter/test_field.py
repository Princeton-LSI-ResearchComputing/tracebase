from django.db.models import CharField, DateField, F, IntegerField
from django.db.models.aggregates import Count
from django.db.models.functions import Lower, Trunc, Upper
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.sorter.field import BSTSorter

BSTSTestModel = create_test_model(
    "BSTSTestModel",
    {
        "name": CharField(max_length=255),
        "value": IntegerField(),
        "date": DateField(),
    },
)


@override_settings(DEBUG=True)
class BSTSorterTests(TracebaseTestCase):
    @TracebaseTestCase.assertNotWarns()
    def test_init_charfield(self):
        s = BSTSorter(CharField(name="name"), BSTSTestModel)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.ALPHANUMERIC, s.client_sorter)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertIsInstance(s.expression, Lower)

    @TracebaseTestCase.assertNotWarns()
    def test_init_integerfield(self):
        s = BSTSorter(IntegerField(name="value"), BSTSTestModel)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertIsInstance(s.expression, F)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NUMERIC, s.client_sorter)

    @TracebaseTestCase.assertNotWarns()
    def test_init_path_str_and_model(self):
        s = BSTSorter("value", BSTSTestModel)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NUMERIC, s.client_sorter)
        self.assertIsInstance(s.expression, F)

    @TracebaseTestCase.assertNotWarns()
    def test_init_path_f_and_model(self):
        s = BSTSorter(F("name"), BSTSTestModel)
        self.assertIsInstance(s.expression, Lower)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.ALPHANUMERIC, s.client_sorter)

    def test_init_path_expression_and_model(self):
        # We assert warns because Upper doesn't have a default output_field set and we have no way to know the default
        # output field type that Upper works on.  In the future, a feature to infer the default output field type would
        # be nice.
        with self.assertWarns(DeveloperWarning) as aw:
            s = BSTSorter(Upper("name"), BSTSTestModel)
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "expression Upper(F(name)) has no output_field", str(aw.warnings[0].message)
        )
        self.assertIn(
            "Unable to apply default server-side sort behavior",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "set the output_field or supply a _server_sorter",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "or a custom client_sorter to the constructor", str(aw.warnings[0].message)
        )
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertEqual(BSTSorter.CLIENT_SORTERS.NONE, s.client_sorter)
        self.assertIsInstance(s.expression, Upper)

    @TracebaseTestCase.assertNotWarns()
    def test_init_path_expression_model_and_clientsorter(self):
        BSTSorter(Upper("name"), BSTSTestModel, client_sorter="upperSorter")

    def test_init_sorter_custom(self):
        with self.assertWarns(DeveloperWarning) as aw:
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
        self.assertIsInstance(s.expression, F)

    @TracebaseTestCase.assertNotWarns()
    def test_str(self):
        self.assertEqual(
            BSTSorter.CLIENT_SORTERS.NONE,
            str(BSTSorter(CharField(name="name"), BSTSTestModel)),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_set_client_mode(self):
        s = BSTSorter(CharField(name="name"), BSTSTestModel)
        self.assertFalse(s.client_mode)
        s.set_client_mode()
        self.assertTrue(s.client_mode)
        s.set_client_mode(enabled=False)
        self.assertFalse(s.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_set_server_mode(self):
        s = BSTSorter(CharField(name="name"), BSTSTestModel)
        s.set_server_mode()
        self.assertFalse(s.client_mode)
        s.set_server_mode(enabled=False)
        self.assertTrue(s.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_order_by(self):
        sorter = BSTSorter(CharField(name="name"), BSTSTestModel)
        self.assertEqual(
            "OrderBy(F(name_bstrowsort), descending=False)",
            str(sorter.order_by),
        )
        # Make sure the expression the annotation refers to is correct
        self.assertEqual(
            "Lower(F(name))",
            str(sorter.expression),
        )

    def test_get_server_sorter_matching_expression(self):
        self.assertEqual(
            BSTSorter.SERVER_SORTERS.ALPHANUMERIC,
            BSTSorter.get_server_sorter_matching_expression(
                Lower("test", output_field=CharField())
            ),
        )
        self.assertEqual(
            BSTSorter.SERVER_SORTERS.NUMERIC,
            BSTSorter.get_server_sorter_matching_expression(
                Lower("test", output_field=IntegerField())
            ),
        )
        self.assertEqual(F, BSTSorter.get_server_sorter_matching_expression(F("test")))

    def test_init_str_server_sorter(self):
        bs1 = BSTSorter("name", BSTSTestModel)
        self.assertEqual(BSTSorter.SERVER_SORTERS.ALPHANUMERIC, bs1._server_sorter)
        bs2 = BSTSorter("value", BSTSTestModel)
        self.assertEqual(BSTSorter.SERVER_SORTERS.NUMERIC, bs2._server_sorter)
        bs3 = BSTSorter("date", BSTSTestModel)
        self.assertEqual(BSTSorter.SERVER_SORTERS.UNKNOWN, bs3._server_sorter)

    def test_init_field_server_sorter(self):
        bs1 = BSTSorter(CharField(name="name", max_length=255), BSTSTestModel)
        self.assertEqual(BSTSorter.SERVER_SORTERS.ALPHANUMERIC, bs1._server_sorter)
        bs2 = BSTSorter(IntegerField(name="value"), BSTSTestModel)
        self.assertEqual(BSTSorter.SERVER_SORTERS.NUMERIC, bs2._server_sorter)
        bs3 = BSTSorter(DateField(name="date"), BSTSTestModel)
        self.assertEqual(BSTSorter.SERVER_SORTERS.UNKNOWN, bs3._server_sorter)

    def test_init_transform_server_sorter(self):
        tas1 = BSTSorter(Lower("name", output_field=CharField()), BSTSTestModel)
        self.assertEqual(BSTSorter.SERVER_SORTERS.ALPHANUMERIC, tas1._server_sorter)
        tas2 = BSTSorter(Count("value", output_field=IntegerField()), BSTSTestModel)
        self.assertEqual(BSTSorter.SERVER_SORTERS.NUMERIC, tas2._server_sorter)
        tas3 = BSTSorter(
            Trunc("date", kind="year", output_field=DateField()), BSTSTestModel
        )
        self.assertEqual(Trunc, tas3._server_sorter)
