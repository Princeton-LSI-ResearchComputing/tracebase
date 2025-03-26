from django.db.models import (
    CASCADE,
    CharField,
    FloatField,
    ForeignKey,
    ManyToManyField,
)
from django.templatetags.static import static
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst_list_view.filterer import BSTFilterer

BSTFStudyTestModel = create_test_model(
    "BSTFStudyTestModel",
    {"name": CharField(max_length=255)},
)
BSTFAnimalTestModel = create_test_model(
    "BSTFAnimalTestModel",
    {
        "sex": CharField(choices=[("F", "female"), ("M", "male")]),
        "body_weight": FloatField(verbose_name="Weight (g)"),
        "studies": ManyToManyField(
            to="loader.BSTFStudyTestModel", related_name="animals"
        ),
    },
)
BSTFSampleTestModel = create_test_model(
    "BSTFSampleTestModel",
    {
        "name": CharField(max_length=255),
        "animal": ForeignKey(
            to="loader.BSTFAnimalTestModel", related_name="samples", on_delete=CASCADE
        ),
    },
)


class BSTFiltererTests(TracebaseTestCase):
    def test_init_none(self):
        f = BSTFilterer()
        self.assertEqual(f.INPUT_METHOD_TEXT, f.input_method)
        self.assertEqual(f.FILTERER_CONTAINS, f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertIsNone(f.lookup)
        self.assertFalse(f.client_mode)

    def test_init_charfield(self):
        f = BSTFilterer(field_path="name", model=BSTFSampleTestModel)
        self.assertEqual(f.INPUT_METHOD_TEXT, f.input_method)
        self.assertEqual(f.FILTERER_CONTAINS, f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertEqual(f.LOOKUP_CONTAINS, f.lookup)
        self.assertFalse(f.client_mode)

    def test_init_integerfield(self):
        f = BSTFilterer(field_path="animal__body_weight", model=BSTFSampleTestModel)
        self.assertEqual(f.INPUT_METHOD_TEXT, f.input_method)
        self.assertEqual(f.FILTERER_STRICT, f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertIsNone(f.lookup)
        self.assertFalse(f.client_mode)

    def test_init_choicesfield(self):
        f = BSTFilterer(field_path="animal__sex", model=BSTFSampleTestModel)
        self.assertEqual(f.INPUT_METHOD_SELECT, f.input_method)
        self.assertEqual(f.FILTERER_STRICT, f.client_filterer)
        self.assertDictEqual({"F": "female", "M": "male"}, f.choices)
        self.assertIsNone(f.lookup)
        self.assertFalse(f.client_mode)

    def test_init_choicesmanyrelatedfield(self):
        f = BSTFilterer(field_path="animals__sex", model=BSTFStudyTestModel)
        self.assertEqual(f.INPUT_METHOD_SELECT, f.input_method)
        self.assertEqual(f.FILTERER_CONTAINS, f.client_filterer)
        self.assertDictEqual({"F": "female", "M": "male"}, f.choices)
        self.assertIsNone(f.lookup)
        self.assertFalse(f.client_mode)

    def test_init_input_method_select_error(self):
        with self.assertRaises(ValueError) as ar:
            BSTFilterer(input_method=BSTFilterer.INPUT_METHOD_SELECT)
        self.assertIn("choices", str(ar.exception))

    def test_init_input_method_select_works(self):
        f = BSTFilterer(
            input_method=BSTFilterer.INPUT_METHOD_SELECT, choices=["A", "B"]
        )
        self.assertEqual(f.INPUT_METHOD_SELECT, f.input_method)
        self.assertEqual(f.FILTERER_STRICT, f.client_filterer)
        self.assertEqual(["A", "B"], f.choices)
        self.assertIsNone(f.lookup)
        self.assertFalse(f.client_mode)

    def test_init_input_method_text(self):
        f = BSTFilterer(input_method=BSTFilterer.INPUT_METHOD_TEXT)
        self.assertEqual(f.INPUT_METHOD_TEXT, f.input_method)
        self.assertEqual(f.FILTERER_CONTAINS, f.client_filterer)
        self.assertIsNone(f.lookup)
        self.assertIsNone(f.choices)
        self.assertFalse(f.client_mode)

    def test_init_client_filterer_works(self):
        f = BSTFilterer(
            input_method=BSTFilterer.INPUT_METHOD_SELECT,
            choices=["A", "B"],
            client_filterer=BSTFilterer.FILTERER_CONTAINS,
        )
        self.assertEqual(f.INPUT_METHOD_SELECT, f.input_method)
        self.assertEqual(f.FILTERER_CONTAINS, f.client_filterer)
        self.assertEqual(["A", "B"], f.choices)
        self.assertIsNone(f.lookup)
        self.assertFalse(f.client_mode)

    @override_settings(DEBUG=True)
    def test_init_client_filterer_custom_warns(self):
        with self.assertWarns(UserWarning):
            f = BSTFilterer(client_filterer="myFilterer")
        self.assertEqual(f.INPUT_METHOD_TEXT, f.input_method)
        self.assertEqual("myFilterer", f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertIsNone(f.lookup)
        self.assertFalse(f.client_mode)

    @override_settings(DEBUG=False)
    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_custom_nowarn_prod(self):
        BSTFilterer(client_filterer="myFilterer")

    @override_settings(DEBUG=False)
    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_custom_nowarn_valid(self):
        BSTFilterer(client_filterer="myFilterer", lookup="mylookup")

    def test_init_lookup(self):
        f = BSTFilterer(lookup="istartswith")
        self.assertEqual(f.INPUT_METHOD_TEXT, f.input_method)
        # TODO: Enforce somehow that the client filterer must match the lookup (e.g. "startswith" instead of "contains")
        self.assertEqual(f.FILTERER_CONTAINS, f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertEqual("istartswith", f.lookup)
        self.assertFalse(f.client_mode)

    def test_init_client_mode(self):
        f = BSTFilterer(client_mode=True)
        self.assertEqual(f.INPUT_METHOD_TEXT, f.input_method)
        self.assertEqual(f.FILTERER_CONTAINS, f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertIsNone(f.lookup)
        self.assertTrue(f.client_mode)

    def test_filterer_client_mode(self):
        self.assertEqual(BSTFilterer.FILTERER_DJANGO, str(BSTFilterer()))

    def test_filterer_server_mode(self):
        self.assertEqual(
            BSTFilterer.FILTERER_CONTAINS, str(BSTFilterer(client_mode=True))
        )

    def test_str_client_mode(self):
        self.assertEqual(BSTFilterer.FILTERER_DJANGO, str(BSTFilterer()))

    def test_str_server_mode(self):
        self.assertEqual(
            BSTFilterer.FILTERER_CONTAINS, str(BSTFilterer(client_mode=True))
        )

    def test_javascript(self):
        self.assertEqual(
            f"<script src='{static(BSTFilterer.JAVASCRIPT)}'></script>",
            BSTFilterer.javascript,
        )

    def test_set_client_mode(self):
        f = BSTFilterer()
        self.assertFalse(f.client_mode)
        f.set_client_mode()
        self.assertTrue(f.client_mode)
        f.set_client_mode(enabled=False)
        self.assertFalse(f.client_mode)

    def test_set_server_mode(self):
        f = BSTFilterer()
        f.set_server_mode()
        self.assertFalse(f.client_mode)
        f.set_server_mode(enabled=False)
        self.assertTrue(f.client_mode)
