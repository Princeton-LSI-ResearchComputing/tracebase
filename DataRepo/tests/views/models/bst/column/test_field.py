from django.db.models import (
    CASCADE,
    CharField,
    FloatField,
    ForeignKey,
    ManyToManyField,
)

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.text_utils import camel_to_title, underscored_to_title
from DataRepo.views.models.bst.column.field import BSTColumn
from DataRepo.views.models.bst.column.filterer.field import BSTFilterer
from DataRepo.views.models.bst.column.sorter.field import BSTSorter

BSTCStudyTestModel = create_test_model(
    "BSTCStudyTestModel",
    {"name": CharField(max_length=255, unique=True)},
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "verbose_name": "Study"},
        ),
    },
)
BSTCAnimalTestModel = create_test_model(
    "BSTCAnimalTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "sex": CharField(choices=[("F", "female"), ("M", "male")]),
        "body_weight": FloatField(verbose_name="Weight (g)"),
        "studies": ManyToManyField(
            to="loader.BSTCStudyTestModel", related_name="animals"
        ),
    },
    attrs={
        "get_absolute_url": lambda self: f"/DataRepo/animal/{self.pk}/",
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "verbose_name": "animal"},
        ),
    },
)
BSTCSampleTestModel = create_test_model(
    "BSTCSampleTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "animal": ForeignKey(
            to="loader.BSTCAnimalTestModel", related_name="samples", on_delete=CASCADE
        ),
    },
    attrs={
        "get_absolute_url": lambda self: f"/DataRepo/sample/{self.pk}/",
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader"},
        ),
    },
)
BSTCMSRunSampleTestModel = create_test_model(
    "BSTCMSRunSampleTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "sample": ForeignKey(
            to="loader.BSTCSampleTestModel",
            related_name="msrun_samples",
            on_delete=CASCADE,
        ),
    },
)
BSTCTissueTestModel = create_test_model(
    "BSTCTissueTestModel",
    {"name": CharField(max_length=255)},
)


class BSTColumnTests(TracebaseTestCase):

    def test_init_field_path_required(self):
        # Test ValueError - "field_path is required"
        with self.assertRaises(ValueError) as ar:
            BSTColumn(None, BSTCStudyTestModel)
        self.assertIn("field_path is required", str(ar.exception))

    def test_init_suggest_annot(self):
        with self.assertRaises(AttributeError) as ar:
            BSTColumn("annot", BSTCStudyTestModel)
        self.assertIn("('annot') is not an attribute", str(ar.exception))
        self.assertIn("BSTAnnotColumn", str(ar.exception))

    def test_init_name_set_to_field_path(self):
        # Test self.name == self.field_path
        mdl = BSTCStudyTestModel
        fld = "name"
        c = BSTColumn(fld, mdl)
        self.assertEqual(fld, c.name)
        # Test if sorter is None - self.sorter = BSTSorter(self.field_path, self.model)
        self.assertEqual(BSTSorter(fld, mdl), c.sorter)
        # Test if filterer is None - self.filterer = BSTFilterer(self.model, self.field_path)
        self.assertEqual(BSTFilterer(fld, mdl), c.filterer)

    def test_init_no_many_related(self):
        # Test if is_many_related and not self.is_many_related and not self.is_annotation - ValueError
        with self.assertRaises(ValueError) as ar:
            BSTColumn("studies__name", BSTCAnimalTestModel)
        self.assertIn("must not be many-related", str(ar.exception))
        self.assertIn(
            "use BSTManyRelatedColumn to create a delimited-value column",
            str(ar.exception),
        )

    def test_init_donot_link_related(self):
        # Test if self.link and "__" in self.field_path - ValueError
        with self.assertRaises(ValueError) as ar:
            BSTColumn("animal__body_weight", BSTCSampleTestModel, linked=True)
        self.assertIn(
            "'linked' must not be true when 'field_path' 'animal__body_weight' passes through a related model",
            str(ar.exception),
        )

    def test_init_link_needs_get_abs_url(self):
        # Test if self.link and no get_absolute_url - ValueError
        with self.assertRaises(ValueError) as ar:
            BSTColumn("name", BSTCStudyTestModel, linked=True)
        self.assertIn(
            "'linked' must not be true when model 'BSTCStudyTestModel' does not have a 'get_absolute_url'",
            str(ar.exception),
        )
        # No error = successful test:
        BSTColumn("body_weight", BSTCAnimalTestModel, linked=True)

    def test_init_sorter_filterer_str(self):
        # Test if sorter is None - self.sorter = BSTSorter(self.field_path, self.model)
        # Test if filterer is None - self.filterer = BSTFilterer(self.model, self.field_path)
        mdl = BSTCStudyTestModel
        fld = "name"
        my_sorter = "mySorter"
        my_filterer = "myFilterer"
        c = BSTColumn(fld, mdl, sorter=my_sorter, filterer=my_filterer)
        self.assertEqual(
            str(BSTSorter(fld, mdl, client_sorter=my_sorter)), str(c.sorter)
        )
        self.assertEqual(
            str(BSTFilterer(fld, mdl, client_filterer=my_filterer)),
            str(c.filterer),
        )

    def test_init_sorter_filterer_object(self):
        # Test if isinstance(sorter, BSTSorter) - self.sorter = sorter
        # Test if isinstance(filterer, BSTFilterer) - self.filterer = filterer
        mdl = BSTCStudyTestModel
        fld = "name"
        my_sorter = "mySorter"
        my_filterer = "myFilterer"
        bsts = BSTSorter(fld, mdl, client_sorter=my_sorter)
        bstf = BSTFilterer(fld, mdl, client_filterer=my_filterer)
        c = BSTColumn(fld, mdl, sorter=bsts, filterer=bstf)
        self.assertEqual(bsts, c.sorter)
        self.assertEqual(bstf, c.filterer)

    def test_init_sorter_wrong_type(self):
        # Test if sorter type wrong - ValueError - "sorter must be a str or a BSTSorter"
        mdl = BSTCStudyTestModel
        fld = "name"
        with self.assertRaises(TypeError) as ar:
            BSTColumn(fld, mdl, sorter=1)
        self.assertIn(
            "sorter must be a str or a BSTBaseSorter, not a 'int'", str(ar.exception)
        )

    def test_init_filterer_wrong_type(self):
        # Test if filterer type wrong - ValueError("filterer must be a str or a BSTFilterer.")
        mdl = BSTCStudyTestModel
        fld = "name"
        with self.assertRaises(TypeError) as ar:
            BSTColumn(fld, mdl, filterer=1)
        self.assertIn(
            "filterer must be a str or a BSTBaseFilterer, not a 'int'",
            str(ar.exception),
        )

    def test_init_is_fk(self):
        self.assertTrue(BSTColumn("animal", BSTCSampleTestModel).is_fk)
        self.assertFalse(BSTColumn("name", BSTCSampleTestModel).is_fk)
        # TODO: Put this test in the tests for BSTRelatedColumn
        # self.assertTrue(BSTColumn("animal__studies", BSTCSampleTestModel).is_fk)
        # self.assertFalse(BSTColumn("animal__studies__name", BSTCSampleTestModel).is_fk)

    def test_eq(self):
        # Test __eq__ works when other val is string
        sexfldp = "sex"
        bwfldp = "body_weight"
        sexcol = BSTColumn(sexfldp, BSTCAnimalTestModel)
        sexcol2 = BSTColumn(sexfldp, BSTCAnimalTestModel)
        bwcol = BSTColumn(bwfldp, BSTCAnimalTestModel)
        self.assertTrue(sexcol == sexfldp)
        self.assertFalse(sexcol == bwfldp)
        self.assertTrue(bwcol == bwfldp)
        self.assertFalse(bwcol == sexfldp)
        self.assertTrue(sexcol2 == sexcol)

    def test_generate_header_field_verbose_name(self):
        # Test if field has verbose name with caps - return field.verbose_name
        c = BSTColumn("body_weight", BSTCAnimalTestModel)
        bwh = c.generate_header()
        self.assertEqual(
            BSTCAnimalTestModel.body_weight.field.verbose_name,  # pylint: disable=no-member
            bwh,
        )
        self.assertEqual("Weight (g)", bwh)

    def test_generate_header_field_name_to_model_name(self):
        # Test if self.field_path == "name" and is_unique_field(field) - Use the model's name
        c = BSTColumn("name", BSTCSampleTestModel)
        sh = c.generate_header()
        self.assertEqual(camel_to_title("BSTCSampleTestModel"), sh)

    def test_generate_header_field_name_to_model_cap_verbose_name(self):
        # Test caps model verbose_name used as-is
        c = BSTColumn("name", BSTCStudyTestModel)
        sh = c.generate_header()
        self.assertEqual(
            BSTCStudyTestModel._meta.__dict__[  # pylint: disable=no-member
                "verbose_name"
            ],
            sh,
        )

    def test_generate_header_field_name_to_model_diff_verbose_name(self):
        # Test diff model verbose_name used as-is
        c = BSTColumn("name", BSTCAnimalTestModel)
        ah = c.generate_header()
        self.assertEqual(
            underscored_to_title(
                BSTCAnimalTestModel._meta.__dict__[  # pylint: disable=no-member
                    "verbose_name"
                ]
            ),
            ah,
        )

    def test_generate_header_field_name_not_unique_not_changed_to_model_name(self):
        # Test diff model verbose_name used as-is
        c = BSTColumn("name", BSTCTissueTestModel)
        th = c.generate_header()
        self.assertEqual(underscored_to_title("name"), th)
