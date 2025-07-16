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
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.filterer.field import BSTFilterer

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


@override_settings(DEBUG=True)
class BSTFiltererTests(TracebaseTestCase):

    @TracebaseTestCase.assertNotWarns()
    def test_init_charfield(self):
        f = BSTFilterer("name", BSTFSampleTestModel)
        self.assertEqual(f.INPUT_METHODS.TEXT, f.input_method)
        self.assertEqual(f.CLIENT_FILTERERS.CONTAINS, f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertEqual(f.SERVER_FILTERERS.CONTAINS, f._server_filterer)
        self.assertFalse(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_init_integerfield(self):
        f = BSTFilterer("animal__body_weight", BSTFSampleTestModel)
        self.assertEqual(f.INPUT_METHODS.TEXT, f.input_method)
        self.assertEqual(f.CLIENT_FILTERERS.STRICT_SINGLE, f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertEqual(f.SERVER_FILTERERS.STRICT_SINGLE, f._server_filterer)
        self.assertFalse(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_init_choicesfield(self):
        f = BSTFilterer("animal__sex", BSTFSampleTestModel)
        self.assertEqual(f.INPUT_METHODS.SELECT, f.input_method)
        self.assertEqual(f.CLIENT_FILTERERS.STRICT_SINGLE, f.client_filterer)
        self.assertDictEqual({"F": "female", "M": "male"}, f.choices)
        self.assertEqual(f.SERVER_FILTERERS.STRICT_SINGLE, f._server_filterer)
        self.assertFalse(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_init_choicesmanyrelatedfield(self):
        f = BSTFilterer("animals__sex", BSTFStudyTestModel)
        self.assertEqual(f.INPUT_METHODS.SELECT, f.input_method)
        self.assertEqual(f.CLIENT_FILTERERS.STRICT_MULTIPLE, f.client_filterer)
        self.assertDictEqual({"F": "female", "M": "male"}, f.choices)
        self.assertEqual(f.SERVER_FILTERERS.STRICT_MULTIPLE, f._server_filterer)
        self.assertFalse(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_init_input_method_select_error(self):
        with self.assertRaises(ValueError) as ar:
            BSTFilterer(
                BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
                BSTFStudyTestModel,
                input_method=BSTFilterer.INPUT_METHODS.SELECT,
            )
        self.assertIn("choices", str(ar.exception))

    @TracebaseTestCase.assertNotWarns()
    def test_init_input_method_select_works(self):
        f = BSTFilterer(
            BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
            BSTFStudyTestModel,
            input_method=BSTFilterer.INPUT_METHODS.SELECT,
            choices=["A", "B"],
        )
        self.assertEqual(f.INPUT_METHODS.SELECT, f.input_method)
        self.assertEqual(f.CLIENT_FILTERERS.STRICT_SINGLE, f.client_filterer)
        self.assertEqual({"A": "A", "B": "B"}, f.choices)
        self.assertEqual(f.SERVER_FILTERERS.STRICT_SINGLE, f._server_filterer)
        self.assertFalse(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_init_input_method_text(self):
        f = BSTFilterer(
            BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
            BSTFStudyTestModel,
            input_method=BSTFilterer.INPUT_METHODS.TEXT,
        )
        self.assertEqual(f.INPUT_METHODS.TEXT, f.input_method)
        self.assertEqual(f.CLIENT_FILTERERS.CONTAINS, f.client_filterer)
        self.assertEqual(f.SERVER_FILTERERS.CONTAINS, f._server_filterer)
        self.assertIsNone(f.choices)
        self.assertFalse(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_works(self):
        f = BSTFilterer(
            BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
            BSTFStudyTestModel,
            input_method=BSTFilterer.INPUT_METHODS.SELECT,
            choices=["A", "B"],
            client_filterer=BSTFilterer.CLIENT_FILTERERS.CONTAINS,
        )
        self.assertEqual(f.INPUT_METHODS.SELECT, f.input_method)
        self.assertEqual(f.CLIENT_FILTERERS.CONTAINS, f.client_filterer)
        self.assertEqual({"A": "A", "B": "B"}, f.choices)
        self.assertEqual(f.SERVER_FILTERERS.CONTAINS, f._server_filterer)
        self.assertFalse(f.client_mode)

    def test_init_client_filterer_custom_warns(self):
        with self.assertWarns(DeveloperWarning) as aw:
            f = BSTFilterer(
                BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
                BSTFStudyTestModel,
                client_filterer="myFilterer",
            )
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Cannot guarantee that the behavior of the default _server_filterer 'icontains'",
            str(aw.warnings[0].message),
        )
        self.assertIn("based on the input method 'input'", str(aw.warnings[0].message))
        self.assertIn(
            "custom client_filterer 'myFilterer'", str(aw.warnings[0].message)
        )
        self.assertIn(
            "Server filtering may differ from client filtering",
            str(aw.warnings[0].message),
        )
        self.assertIn("Supply a custom _server_filterer", str(aw.warnings[0].message))
        self.assertEqual(f.INPUT_METHODS.TEXT, f.input_method)
        self.assertEqual("myFilterer", f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertEqual(f.SERVER_FILTERERS.CONTAINS, f._server_filterer)
        self.assertFalse(f.client_mode)

    @override_settings(DEBUG=False)
    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_custom_nowarn_in_production(self):
        BSTFilterer(
            BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
            BSTFStudyTestModel,
            client_filterer="myFilterer",
        )

    @TracebaseTestCase.assertNotWarns()
    def test_init_client_filterer_custom_nowarn_valid(self):
        BSTFilterer(
            BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
            BSTFStudyTestModel,
            client_filterer="myFilterer",
            _server_filterer="mylookup",
        )

    def test_init_server_filterer(self):
        with self.assertWarns(DeveloperWarning) as aw:
            f = BSTFilterer(
                BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
                BSTFStudyTestModel,
                _server_filterer="istartswith",
            )
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Cannot guarantee that the client_filterer 'djangoFilterer' behavior will match",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "_server_filterer 'CustomLookup' behavior", str(aw.warnings[0].message)
        )
        self.assertIn(
            "Server filtering may differ from client filtering.",
            str(aw.warnings[0].message),
        )
        self.assertEqual(f.INPUT_METHODS.TEXT, f.input_method)
        self.assertEqual(f.CLIENT_FILTERERS.NONE, f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertEqual("istartswith", str(f._server_filterer))
        self.assertFalse(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_init_client_mode(self):
        f = BSTFilterer(
            BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
            BSTFStudyTestModel,
            client_mode=True,
        )
        self.assertEqual(f.INPUT_METHODS.TEXT, f.input_method)
        self.assertEqual(f.CLIENT_FILTERERS.CONTAINS, f.client_filterer)
        self.assertIsNone(f.choices)
        self.assertEqual(BSTFilterer.SERVER_FILTERERS.CONTAINS, f._server_filterer)
        self.assertTrue(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_filterer_client_mode(self):
        self.assertEqual(
            BSTFilterer.CLIENT_FILTERERS.NONE,
            str(
                BSTFilterer(
                    BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
                    BSTFStudyTestModel,
                )
            ),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_filterer_server_mode(self):
        self.assertEqual(
            BSTFilterer.CLIENT_FILTERERS.CONTAINS,
            str(
                BSTFilterer(
                    BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
                    BSTFStudyTestModel,
                    client_mode=True,
                )
            ),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_str_client_mode(self):
        self.assertEqual(
            BSTFilterer.CLIENT_FILTERERS.NONE,
            str(
                BSTFilterer(
                    BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
                    BSTFStudyTestModel,
                )
            ),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_str_server_mode(self):
        self.assertEqual(
            BSTFilterer.CLIENT_FILTERERS.CONTAINS,
            str(
                BSTFilterer(
                    BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
                    BSTFStudyTestModel,
                    client_mode=True,
                )
            ),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_script(self):
        f = BSTFilterer(
            BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
            BSTFStudyTestModel,
        )
        self.assertEqual(
            f"<script src='{static(BSTFilterer.script_name)}'></script>",
            f.script,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_set_client_mode(self):
        f = BSTFilterer(
            BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
            BSTFStudyTestModel,
        )
        self.assertFalse(f.client_mode)
        f.set_client_mode()
        self.assertTrue(f.client_mode)
        f.set_client_mode(enabled=False)
        self.assertFalse(f.client_mode)

    @TracebaseTestCase.assertNotWarns()
    def test_set_server_mode(self):
        f = BSTFilterer(
            BSTFStudyTestModel.name.field.name,  # pylint: disable=no-member
            BSTFStudyTestModel,
        )
        f.set_server_mode()
        self.assertFalse(f.client_mode)
        f.set_server_mode(enabled=False)
        self.assertTrue(f.client_mode)
