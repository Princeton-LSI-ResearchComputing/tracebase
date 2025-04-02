from django.db.models import CharField, F
from django.db.models.functions import Lower, Upper
from django.templatetags.static import static
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst_list_view.column.sorter.annotation import (
    BSTAnnotSorter,
)


class BSTAnnotSorterTests(TracebaseTestCase):
    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_str_expression(self):
        s = BSTAnnotSorter(
            "name", _server_sorter=BSTAnnotSorter.SERVER_SORTERS.ALPHANUMERIC
        )
        self.assertEqual(BSTAnnotSorter.CLIENT_SORTERS.ALPHANUMERIC, s.client_sorter)
        self.assertEqual(BSTAnnotSorter.CLIENT_SORTERS.NONE, s.sorter)
        self.assertEqual("name", s.name)
        self.assertEqual(s.SERVER_SORTERS.ALPHANUMERIC, s._server_sorter)
        self.assertIsInstance(s.sort_expression, Lower)
        self.assertFalse(s.client_mode)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_path_f_expression(self):
        s = BSTAnnotSorter(
            F("name"), _server_sorter=BSTAnnotSorter.SERVER_SORTERS.ALPHANUMERIC
        )
        self.assertEqual(BSTAnnotSorter.CLIENT_SORTERS.ALPHANUMERIC, s.client_sorter)
        self.assertEqual("name", s.name)
        self.assertIsInstance(s.sort_expression, Lower)
        self.assertEqual(s.SERVER_SORTERS.ALPHANUMERIC, s._server_sorter)

    @override_settings(DEBUG=True)
    def test_init_expression_nofield_only(self):
        # We assert NOT warns because Upper has a default output_field type which we recognize and can apply our case
        # insensitivity to (using Lower).  This is a nonsensical example, but where this makes sense is when for
        # example, fields are being concatenated or other operations are happening.  The point is that 'Lower' is
        # applied if the **output_field** is a compatible type.
        with self.assertWarns(UserWarning) as aw:
            s = BSTAnnotSorter(Upper("name"))
        self.assertEqual(2, len(aw.warnings))
        self.assertIn(
            "Unable to apply default server-side sort behavior",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Server sort may differ from client",
            str(aw.warnings[1].message),
        )
        self.assertEqual(BSTAnnotSorter.CLIENT_SORTERS.NONE, s.client_sorter)
        self.assertEqual(BSTAnnotSorter.SERVER_SORTERS.UNKNOWN, s._server_sorter)
        self.assertEqual("name", s.name)
        self.assertIsInstance(s.sort_expression, Upper)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_expression_nofield_server_sorter_known(self):
        BSTAnnotSorter(
            Upper("name"), _server_sorter=BSTAnnotSorter.SERVER_SORTERS.ALPHANUMERIC
        )

    @override_settings(DEBUG=True)
    def test_init_expression_nofield_server_sorter_custom(self):
        # Allow users to craft their own server sorter, but warn that we cannot apply case insensitivity due to the lack
        # of an output_field and we cannot guranatee that the client sort will match if a custom client_sorter is not
        # also specified.
        with self.assertWarns(UserWarning) as aw:
            BSTAnnotSorter(Upper("name"), _server_sorter=Upper)
        self.assertIn(
            "Upper(F(name)) has no output_field",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Server sort may differ from client",
            str(aw.warnings[1].message),
        )

    @override_settings(DEBUG=True)
    def test_init_expression_nofield_client_sorter_known_debug(self):
        with self.assertWarns(UserWarning) as aw:
            BSTAnnotSorter(
                Upper("name"), client_sorter=BSTAnnotSorter.CLIENT_SORTERS.ALPHANUMERIC
            )
        self.assertEqual(2, len(aw.warnings))
        self.assertIn(
            "Upper(F(name)) has no output_field",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Server sort may differ from client",
            str(aw.warnings[1].message),
        )

    @override_settings(DEBUG=False)
    @TracebaseTestCase.assertNotWarns()
    def test_init_no_warn_when_not_in_debug_mode(self):
        BSTAnnotSorter(
            Upper("name"), client_sorter=BSTAnnotSorter.CLIENT_SORTERS.ALPHANUMERIC
        )

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_expression_nofield_client_sorter_unknown(self):
        # We assert NOT warns because Upper has a default output_field type which we recognize and can apply our case
        # insensitivity to (using Lower).  This is a nonsensical example, but where this makes sense is when for
        # example, fields are being concatenated or other operations are happening.  The point is that 'Lower' is
        # applied if the **output_field** is a compatible type.
        BSTAnnotSorter(Upper("name"), client_sorter="myUpperSorter")

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_expression_field_charfield(self):
        # We assert NOT warns because Upper has a default output_field type which we recognize and can apply our case
        # insensitivity to (using Lower).  This is a nonsensical example, but where this makes sense is when for
        # example, fields are being concatenated or other operations are happening.  The point is that 'Lower' is
        # applied if the **output_field** is a compatible type.
        s = BSTAnnotSorter(Upper("name", output_field=CharField(name="name")))
        self.assertEqual(Lower, s._server_sorter)
        self.assertEqual(BSTAnnotSorter.CLIENT_SORTERS.ALPHANUMERIC, s.client_sorter)
        self.assertEqual("name", s.name)
        self.assertIsInstance(s.sort_expression, Lower)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_init_expression_nofield_and_clientsorter(self):
        s = BSTAnnotSorter(Upper("name"), client_sorter="upperSorter")
        self.assertEqual("upperSorter", s.client_sorter)
        self.assertEqual("name", s.name)
        self.assertIsInstance(s.sort_expression, Upper)
        self.assertEqual(BSTAnnotSorter.SERVER_SORTERS.UNKNOWN, s._server_sorter)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_str(self):
        self.assertEqual(
            BSTAnnotSorter.CLIENT_SORTERS.NONE,
            str(
                BSTAnnotSorter(
                    "name", _server_sorter=BSTAnnotSorter.SERVER_SORTERS.ALPHANUMERIC
                )
            ),
        )
        self.assertEqual(
            BSTAnnotSorter.CLIENT_SORTERS.ALPHANUMERIC,
            str(
                BSTAnnotSorter(
                    Lower("name", output_field=CharField()),
                    client_sorter=BSTAnnotSorter.CLIENT_SORTERS.ALPHANUMERIC,
                    client_mode=True,
                )
            ),
        )
        self.assertEqual(
            "upperSorter",
            str(
                BSTAnnotSorter(
                    Upper("name"), client_sorter="upperSorter", client_mode=True
                )
            ),
        )

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_sorter(self):
        self.assertEqual(
            BSTAnnotSorter.CLIENT_SORTERS.NONE,
            BSTAnnotSorter(
                "name", _server_sorter=BSTAnnotSorter.SERVER_SORTERS.ALPHANUMERIC
            ).sorter,
        )
        self.assertEqual(
            BSTAnnotSorter.CLIENT_SORTERS.ALPHANUMERIC,
            BSTAnnotSorter(
                Lower("name", output_field=CharField()),
                client_sorter=BSTAnnotSorter.CLIENT_SORTERS.ALPHANUMERIC,
                client_mode=True,
            ).sorter,
        )
        self.assertEqual(
            "upperSorter",
            BSTAnnotSorter(
                Upper("name"), client_sorter="upperSorter", client_mode=True
            ).sorter,
        )

    def test_javascript(self):
        self.assertEqual(
            f"<script src='{static(BSTAnnotSorter.JAVASCRIPT)}'></script>",
            BSTAnnotSorter.javascript,
        )

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_set_client_mode(self):
        s = BSTAnnotSorter(
            CharField(name="name"),
            _server_sorter=BSTAnnotSorter.SERVER_SORTERS.ALPHANUMERIC,
        )
        self.assertFalse(s.client_mode)
        s.set_client_mode()
        self.assertTrue(s.client_mode)
        s.set_client_mode(enabled=False)
        self.assertFalse(s.client_mode)

    @override_settings(DEBUG=True)
    @TracebaseTestCase.assertNotWarns()
    def test_set_server_mode(self):
        s = BSTAnnotSorter(
            CharField(name="name"),
            _server_sorter=BSTAnnotSorter.SERVER_SORTERS.ALPHANUMERIC,
        )
        s.set_server_mode()
        self.assertFalse(s.client_mode)
        s.set_server_mode(enabled=False)
        self.assertTrue(s.client_mode)
