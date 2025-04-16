from django.templatetags.static import static
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.filterer.annotation import (
    BSTAnnotFilterer,
)


@override_settings(DEBUG=True)
class BSTAnnotFiltererTests(TracebaseTestCase):
    @TracebaseTestCase.assertNotWarns()
    def test_init_str_expression(self):
        f = BSTAnnotFilterer(
            "name", _server_filterer=BSTAnnotFilterer.SERVER_FILTERERS.CONTAINS
        )
        self.assertEqual(f.CLIENT_FILTERERS.CONTAINS, f.client_filterer)
        self.assertEqual(f.CLIENT_FILTERERS.NONE, f.filterer)
        self.assertEqual("name", f.name)
        self.assertEqual(f.SERVER_FILTERERS.CONTAINS, f._server_filterer)
        self.assertFalse(f.client_mode)
        self.assertEqual(f.INPUT_METHODS.TEXT, f.input_method)
        self.assertIsNone(f.initial)
        self.assertIsNone(f.choices)

    @TracebaseTestCase.assertNotWarns()
    def test_str(self):
        self.assertEqual(
            BSTAnnotFilterer.CLIENT_FILTERERS.NONE,
            str(
                BSTAnnotFilterer(
                    "name", _server_filterer=BSTAnnotFilterer.SERVER_FILTERERS.CONTAINS
                )
            ),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_filterer(self):
        self.assertEqual(
            BSTAnnotFilterer.CLIENT_FILTERERS.NONE,
            BSTAnnotFilterer(
                "name", _server_filterer=BSTAnnotFilterer.SERVER_FILTERERS.CONTAINS
            ).filterer,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_script(self):
        f = BSTAnnotFilterer(
            "name",
            _server_filterer=BSTAnnotFilterer.SERVER_FILTERERS.CONTAINS,
        )
        self.assertEqual(
            f"<script src='{static(BSTAnnotFilterer.script_name)}'></script>",
            f.script,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_set_client_mode(self):
        f = BSTAnnotFilterer(
            "name",
            _server_filterer=BSTAnnotFilterer.SERVER_FILTERERS.CONTAINS,
        )
        self.assertFalse(f.client_mode)
        f.set_client_mode()
        self.assertTrue(f.client_mode)
        f.set_client_mode(enabled=False)
        self.assertFalse(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_set_server_mode(self):
        f = BSTAnnotFilterer(
            "name",
            _server_filterer=BSTAnnotFilterer.SERVER_FILTERERS.CONTAINS,
        )
        f.set_server_mode()
        self.assertFalse(f.client_mode)
        f.set_server_mode(enabled=False)
        self.assertTrue(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_contains(self):
        f = BSTAnnotFilterer(
            "name",
            client_filterer=BSTAnnotFilterer.CLIENT_FILTERERS.CONTAINS,
        )
        self.assertEqual(BSTAnnotFilterer.CLIENT_FILTERERS.CONTAINS, f.client_filterer)
        self.assertEqual(BSTAnnotFilterer.SERVER_FILTERERS.CONTAINS, f._server_filterer)

    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_strict_single(self):
        f = BSTAnnotFilterer(
            "name",
            client_filterer=BSTAnnotFilterer.CLIENT_FILTERERS.STRICT_SINGLE,
        )
        self.assertEqual(
            BSTAnnotFilterer.CLIENT_FILTERERS.STRICT_SINGLE, f.client_filterer
        )
        self.assertEqual(
            BSTAnnotFilterer.SERVER_FILTERERS.STRICT_SINGLE, f._server_filterer
        )

    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_strict_multiple(self):
        f = BSTAnnotFilterer(
            "name",
            client_filterer=BSTAnnotFilterer.CLIENT_FILTERERS.STRICT_MULTIPLE,
        )
        self.assertEqual(
            BSTAnnotFilterer.CLIENT_FILTERERS.STRICT_MULTIPLE, f.client_filterer
        )
        self.assertEqual(
            BSTAnnotFilterer.SERVER_FILTERERS.STRICT_MULTIPLE, f._server_filterer
        )

    def test_init_client_filterer_none_text_input(self):
        with self.assertWarns(DeveloperWarning) as aw:
            f = BSTAnnotFilterer(
                "name",
                client_filterer=BSTAnnotFilterer.CLIENT_FILTERERS.NONE,
            )
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "selected based on the input method 'input'", str(aw.warnings[0].message)
        )
        self.assertIn(
            "Cannot guarantee that the behavior of the default _server_filterer 'icontains'",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "match the behavior of the custom client_filterer 'djangoFilterer'",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Server filtering may differ from client filtering",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Supply a custom _server_filterer to guarantee matching behavior.",
            str(aw.warnings[0].message),
        )
        self.assertEqual(BSTAnnotFilterer.CLIENT_FILTERERS.NONE, f.client_filterer)
        self.assertEqual(BSTAnnotFilterer.SERVER_FILTERERS.CONTAINS, f._server_filterer)

    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_none_select_input_error(self):
        with self.assertRaises(ValueError) as ar:
            BSTAnnotFilterer(
                "name",
                input_method=BSTAnnotFilterer.INPUT_METHODS.SELECT,
            )
        self.assertIn("input_method 'select' requires that choices", str(ar.exception))

    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_none_select_input_strict(self):
        f = BSTAnnotFilterer(
            "name",
            input_method=BSTAnnotFilterer.INPUT_METHODS.SELECT,
            choices={"A": "A", "B": "B"},
        )
        self.assertEqual(
            BSTAnnotFilterer.CLIENT_FILTERERS.STRICT_SINGLE, f.client_filterer
        )
        self.assertEqual(
            BSTAnnotFilterer.SERVER_FILTERERS.STRICT_SINGLE, f._server_filterer
        )

    def test_init_client_filterer_unknown(self):
        with self.assertWarns(DeveloperWarning) as aw:
            f = BSTAnnotFilterer(
                "name",
                client_filterer="myFilterer",
            )
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Cannot guarantee that the behavior of the default _server_filterer 'icontains'",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "selected based on the input method 'input'", str(aw.warnings[0].message)
        )
        self.assertIn(
            "match the behavior of the custom client_filterer 'myFilterer'",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Server filtering may differ from client filtering",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Supply a custom _server_filterer to guarantee matching behavior",
            str(aw.warnings[0].message),
        )
        self.assertEqual("myFilterer", f.client_filterer)
        self.assertEqual(BSTAnnotFilterer.SERVER_FILTERERS.CONTAINS, f._server_filterer)

    @TracebaseTestCase.assertNotWarns()
    def test_init_choices_list(self):
        f = BSTAnnotFilterer(
            "name",
            choices=["A", "B"],
        )
        self.assertEqual(
            BSTAnnotFilterer.SERVER_FILTERERS.STRICT_SINGLE, f._server_filterer
        )
        self.assertEqual(
            BSTAnnotFilterer.CLIENT_FILTERERS.STRICT_SINGLE, f.client_filterer
        )
        self.assertEqual({"A": "A", "B": "B"}, f.choices)
        self.assertEqual(f.INPUT_METHODS.SELECT, f.input_method)

    @TracebaseTestCase.assertNotWarns()
    def test_init_choices_dict(self):
        f = BSTAnnotFilterer(
            "name",
            choices={"A": "A", "B": "B"},
        )
        self.assertEqual(
            BSTAnnotFilterer.CLIENT_FILTERERS.STRICT_SINGLE, f.client_filterer
        )
        self.assertEqual(
            BSTAnnotFilterer.SERVER_FILTERERS.STRICT_SINGLE, f._server_filterer
        )
        self.assertEqual({"A": "A", "B": "B"}, f.choices)
        self.assertEqual(f.INPUT_METHODS.SELECT, f.input_method)

    @TracebaseTestCase.assertNotWarns()
    def test_init_initial_filter(self):
        f = BSTAnnotFilterer(
            "name",
            initial="A",
        )
        self.assertEqual("A", f.initial)

    @TracebaseTestCase.assertNotWarns()
    def test_filter(self):
        # TODO: Implement test
        pass
