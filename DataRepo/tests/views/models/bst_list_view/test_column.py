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
from DataRepo.views.models.bst_list_view.column import BSTColumn
from DataRepo.views.models.bst_list_view.filterer import BSTFilterer
from DataRepo.views.models.bst_list_view.sorter import BSTSorter

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
    def setUp(self):
        # Some tests change these class attributes, so they need to be reset for each test to the default
        BSTColumn.is_annotation = False
        BSTColumn.is_many_related = False
        BSTColumn.is_fk = False
        super().setUp()

    def test_init_field_path_required(self):
        # Test ValueError - "field_path is required"
        with self.assertRaises(ValueError) as ar:
            BSTColumn(BSTCStudyTestModel, None)
        self.assertIn("field_path is required", str(ar.exception))

    def test_init_annot_requires_name(self):
        # Test when name not set and self.is_annotation - AttributeError - "name not set by BSTAnnotColumn"
        BSTColumn.is_annotation = True
        with self.assertRaises(AttributeError) as ar:
            BSTColumn(BSTCStudyTestModel, "annot")
        self.assertIn("'name' not set by BSTAnnotColumn", str(ar.exception))

    def test_init_name_set_to_field_path(self):
        # Test self.name == self.field_path
        mdl = BSTCStudyTestModel
        fld = "name"
        c = BSTColumn(mdl, fld)
        self.assertEqual(fld, c.name)
        # Test if sorter is None - self.sorter = BSTSorter(self.field_path, model=self.model)
        self.assertEqual(BSTSorter(fld, model=mdl), c.sorter)
        # Test if filterer is None - self.filterer = BSTFilterer(model=self.model, field_path=self.field_path)
        self.assertEqual(BSTFilterer(field=fld, model=mdl), c.filterer)

    def test_init_no_many_related(self):
        # Test if is_many_related and not self.is_many_related and not self.is_annotation - ValueError
        with self.assertRaises(ValueError) as ar:
            BSTColumn(BSTCAnimalTestModel, "studies__name")
        self.assertIn("must not be many-related", str(ar.exception))
        self.assertIn("Use BSTAnnotColumn or BSTManyRelatedColumn", str(ar.exception))

    def test_init_donot_link_many_related(self):
        # Test if self.link and is_many_related - ValueError
        BSTColumn.is_many_related = True
        with self.assertRaises(ValueError) as ar:
            BSTColumn(BSTCSampleTestModel, "animal__studies__name", link=True)
        self.assertIn(
            "'link' must not be true when 'field_path' 'animal__studies__name' is many-related",
            str(ar.exception),
        )

    def test_init_donot_link_related(self):
        # Test if self.link and "__" in self.field_path - ValueError
        with self.assertRaises(ValueError) as ar:
            BSTColumn(BSTCSampleTestModel, "animal__body_weight", link=True)
        self.assertIn(
            "'link' must not be true when 'field_path' 'animal__body_weight' passes through a related model",
            str(ar.exception),
        )

    def test_init_link_needs_get_abs_url(self):
        # Test if self.link and no get_absolute_url - ValueError
        with self.assertRaises(ValueError) as ar:
            BSTColumn(BSTCStudyTestModel, "name", link=True)
        self.assertIn(
            "'link' must not be true when 'model' 'BSTCStudyTestModel' does not have a 'get_absolute_url'",
            str(ar.exception),
        )
        # No error = successful test:
        BSTColumn(BSTCAnimalTestModel, "body_weight", link=True)

    def test_init_sorter_filterer_str(self):
        # Test if sorter is None - self.sorter = BSTSorter(self.field_path, model=self.model)
        # Test if filterer is None - self.filterer = BSTFilterer(model=self.model, field_path=self.field_path)
        mdl = BSTCStudyTestModel
        fld = "name"
        my_sorter = "mySorter"
        my_filterer = "myFilterer"
        c = BSTColumn(mdl, fld, sorter=my_sorter, filterer=my_filterer)
        self.assertEqual(
            str(BSTSorter(fld, model=mdl, client_sorter=my_sorter)), str(c.sorter)
        )
        self.assertEqual(
            str(BSTFilterer(field=fld, model=mdl, client_filterer=my_filterer)),
            str(c.filterer),
        )

    def test_init_sorter_filterer_object(self):
        # Test if isinstance(sorter, BSTSorter) - self.sorter = sorter
        # Test if isinstance(filterer, BSTFilterer) - self.filterer = filterer
        mdl = BSTCStudyTestModel
        fld = "name"
        my_sorter = "mySorter"
        my_filterer = "myFilterer"
        bsts = BSTSorter(fld, model=mdl, client_sorter=my_sorter)
        bstf = BSTFilterer(field=fld, model=mdl, client_filterer=my_filterer)
        c = BSTColumn(mdl, fld, sorter=bsts, filterer=bstf)
        self.assertEqual(bsts, c.sorter)
        self.assertEqual(bstf, c.filterer)

    def test_init_sorter_wrong_type(self):
        # Test if sorter type wrong - ValueError - "sorter must be a str or a BSTSorter"
        mdl = BSTCStudyTestModel
        fld = "name"
        with self.assertRaises(ValueError) as ar:
            BSTColumn(mdl, fld, sorter=1)
        self.assertIn("sorter must be a str or a BSTSorter", str(ar.exception))

    def test_init_filterer_wrong_type(self):
        # Test if filterer type wrong - ValueError("filterer must be a str or a BSTFilterer.")
        mdl = BSTCStudyTestModel
        fld = "name"
        with self.assertRaises(ValueError) as ar:
            BSTColumn(mdl, fld, filterer=1)
        self.assertIn("filterer must be a str or a BSTFilterer", str(ar.exception))

    def test_eq(self):
        # Test __eq__ works when other val is string
        sexfldp = "sex"
        bwfldp = "body_weight"
        sexcol = BSTColumn(BSTCAnimalTestModel, sexfldp)
        sexcol2 = BSTColumn(BSTCAnimalTestModel, sexfldp)
        bwcol = BSTColumn(BSTCAnimalTestModel, bwfldp)
        self.assertTrue(sexcol == sexfldp)
        self.assertFalse(sexcol == bwfldp)
        self.assertTrue(bwcol == bwfldp)
        self.assertFalse(bwcol == sexfldp)
        self.assertTrue(sexcol2 == sexcol)

    def test_generate_header_annotation(self):
        # Test if self.is_annotation is None - return underscored_to_title(self.name)
        c = BSTColumn(BSTCStudyTestModel, "name")
        c.is_annotation = True
        ann = "unique_count"
        c.name = ann
        an = c.generate_header()
        self.assertEqual(underscored_to_title(ann), an)
        self.assertEqual("Unique Count", an)

    def test_generate_header_field_verbose_name(self):
        # Test if field has verbose name with caps - return field.verbose_name
        c = BSTColumn(BSTCAnimalTestModel, "body_weight")
        bwh = c.generate_header()
        self.assertEqual(
            BSTCAnimalTestModel.body_weight.field.verbose_name,  # pylint: disable=no-member
            bwh,
        )
        self.assertEqual("Weight (g)", bwh)

    def test_generate_header_field_name_to_model_name(self):
        # Test if self.field_path == "name" and is_unique_field(field) - Use the model's name
        c = BSTColumn(BSTCSampleTestModel, "name")
        sh = c.generate_header()
        self.assertEqual(camel_to_title("BSTCSampleTestModel"), sh)

    def test_generate_header_field_name_to_model_cap_verbose_name(self):
        # Test caps model verbose_name used as-is
        c = BSTColumn(BSTCStudyTestModel, "name")
        sh = c.generate_header()
        self.assertEqual(
            BSTCStudyTestModel._meta.__dict__[  # pylint: disable=no-member
                "verbose_name"
            ],
            sh,
        )

    def test_generate_header_field_name_to_model_diff_verbose_name(self):
        # Test diff model verbose_name used as-is
        c = BSTColumn(BSTCAnimalTestModel, "name")
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
        c = BSTColumn(BSTCTissueTestModel, "name")
        th = c.generate_header()
        self.assertEqual(underscored_to_title("name"), th)

    def test_generate_header_related_model_name_field_to_fkey_name(self):
        # Test when related model unique field, uses foreign key name when field is "name"
        c = BSTColumn(BSTCSampleTestModel, "animal__name")
        ah = c.generate_header()
        self.assertEqual(underscored_to_title("animal"), ah)

    def test_generate_header_related_model_uses_last_fkey(self):
        # Test that every other field uses - underscored_to_title("_".join(path_tail))
        c = BSTColumn(BSTCMSRunSampleTestModel, "sample__animal__sex")
        sh = c.generate_header()
        self.assertEqual(underscored_to_title("animal_sex"), sh)
