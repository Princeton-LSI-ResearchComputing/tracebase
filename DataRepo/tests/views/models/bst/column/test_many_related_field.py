from django.db import ProgrammingError
from django.db.models import (
    CASCADE,
    CharField,
    FloatField,
    ForeignKey,
    IntegerField,
    ManyToManyField,
)
from django.db.models.aggregates import Count
from django.db.models.functions import Length
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.sorter.many_related_field import (
    BSTManyRelatedSorter,
)

BSTMRCStudyTestModel = create_test_model(
    "BSTMRCStudyTestModel",
    {"name": CharField(max_length=255, unique=True)},
)
BSTMRCAnimalTestModel = create_test_model(
    "BSTMRCAnimalTestModel",
    {
        "name": CharField(max_length=255),
        "body_weight": FloatField(verbose_name="Weight (g)"),
        "sex": CharField(choices=[("F", "female"), ("M", "male")]),
        "studies": ManyToManyField(
            to="loader.BSTMRCStudyTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.BSTMRCTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
            verbose_name="Animal Treatment",
        ),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {
                "app_label": "loader",
                "verbose_name_plural": "The Animals",
            },
        ),
    },
)
BSTMRCSampleTestModel = create_test_model(
    "BSTMRCSampleTestModel",
    {
        "animal": ForeignKey(
            to="loader.BSTMRCAnimalTestModel", related_name="samples", on_delete=CASCADE
        ),
        "characteristic": CharField(),
        "name": CharField(max_length=255, unique=True),
        "tissue": ForeignKey(
            to="loader.BSTMRCTissueTestModel", related_name="samples", on_delete=CASCADE
        ),
    },
)
BSTMRCMSRunSampleTestModel = create_test_model(
    "BSTMRCMSRunSampleTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "sample": ForeignKey(
            to="loader.BSTMRCSampleTestModel",
            related_name="msrun_samples",
            on_delete=CASCADE,
        ),
    },
)
BSTMRCTissueTestModel = create_test_model(
    "BSTMRCTissueTestModel",
    {"name": CharField(max_length=255)},
)
BSTMRCTreatmentTestModel = create_test_model(
    "BSTMRCTreatmentTestModel",
    {"name": CharField(unique=True), "desc": CharField()},
    attrs={
        "Meta": type(
            "Meta",
            (),
            {
                "app_label": "loader",
                "verbose_name_plural": "Animal Treatments",
            },
        ),
    },
)


@override_settings(DEBUG=True)
class BSTManyRelatedColumnTests(TracebaseTestCase):

    @TracebaseTestCase.assertNotWarns()
    def test_init_basic_defaults(self):
        # self.list_attr_name
        # self.count_attr_name
        # self.delim
        # self.limit
        # self.unique
        c = BSTManyRelatedColumn("studies__name", BSTMRCAnimalTestModel)
        self.assertEqual(
            f"studies_name{BSTManyRelatedColumn._list_attr_tail}", c.list_attr_name
        )
        self.assertEqual(
            f"studies{BSTManyRelatedColumn._count_attr_tail}", c.count_attr_name
        )
        self.assertEqual(BSTManyRelatedColumn.delimiter, c.delim)
        self.assertEqual(BSTManyRelatedColumn.limit, c.limit)
        self.assertEqual(BSTManyRelatedColumn.ascending, c.asc)
        self.assertEqual(BSTManyRelatedColumn.unique, c.unique)

        d = BSTManyRelatedColumn("studies__name", BSTMRCAnimalTestModel, unique=True)
        self.assertTrue(d.unique)

    @TracebaseTestCase.assertNotWarns()
    def test_init_fk_field_attrs(self):
        c = BSTManyRelatedColumn("studies", BSTMRCAnimalTestModel)
        self.assertEqual(
            f"studies{BSTManyRelatedColumn._list_attr_tail}", c.list_attr_name
        )
        self.assertEqual(
            f"studies{BSTManyRelatedColumn._count_attr_tail}", c.count_attr_name
        )

    @TracebaseTestCase.assertNotWarns()
    def test_init_many_related_model_path(self):
        c = BSTManyRelatedColumn("animals__samples__tissue__name", BSTMRCStudyTestModel)
        self.assertEqual(
            "animals__samples",
            c.many_related_model_path,
            msg="The last many-related foreign key is set as the many_related_model_path",
        )

    @TracebaseTestCase.assertNotWarns()
    def test_create_sorter_default_char(self):
        c = BSTManyRelatedColumn("animals__samples__tissue", BSTMRCStudyTestModel)
        self.assertIsInstance(c.sorter, BSTManyRelatedSorter)
        self.assertEqual("animals__samples__tissue", c.sorter.name)
        self.assertTrue(c.sorter.asc)
        self.assertEqual(
            "Min(Lower(F(animals__samples__tissue__name)))", str(c.sorter.expression)
        )

    @TracebaseTestCase.assertNotWarns()
    def test_create_sorter_default_float(self):
        c = BSTManyRelatedColumn("animals__body_weight", BSTMRCStudyTestModel)
        self.assertIsInstance(c.sorter, BSTManyRelatedSorter)
        self.assertEqual("animals__body_weight", c.sorter.name)
        self.assertTrue(c.sorter.asc)
        self.assertEqual("Min(F(animals__body_weight))", str(c.sorter.expression))

    @TracebaseTestCase.assertNotWarns()
    def test_create_sorter_max_char(self):
        c = BSTManyRelatedColumn(
            "animals__samples__tissue", BSTMRCStudyTestModel, asc=False
        )
        self.assertIsInstance(c.sorter, BSTManyRelatedSorter)
        self.assertEqual("animals__samples__tissue", c.sorter.name)
        self.assertFalse(c.sorter.asc)
        self.assertEqual(
            "Max(Lower(F(animals__samples__tissue__name)))", str(c.sorter.expression)
        )

    @TracebaseTestCase.assertNotWarns()
    def test_create_sorter_max_float(self):
        c = BSTManyRelatedColumn(
            "animals__body_weight", BSTMRCStudyTestModel, asc=False
        )
        self.assertIsInstance(c.sorter, BSTManyRelatedSorter)
        self.assertEqual("animals__body_weight", c.sorter.name)
        self.assertFalse(c.sorter.asc)
        self.assertEqual("Max(F(animals__body_weight))", str(c.sorter.expression))

    @TracebaseTestCase.assertNotWarns()
    def test_create_sorter_custom_sort_expression(self):
        c = BSTManyRelatedColumn(
            "samples__characteristic",
            BSTMRCAnimalTestModel,
            sort_expression=Length(
                "samples__characteristic", output_field=IntegerField
            ),
            asc=False,
        )
        self.assertIsInstance(c.sorter, BSTManyRelatedSorter)
        self.assertEqual("samples__characteristic", c.sorter.name)
        self.assertFalse(c.sorter.asc)
        self.assertEqual(
            "Max(Length(F(samples__characteristic)))", str(c.sorter.expression)
        )

    @TracebaseTestCase.assertNotWarns()
    def test_create_sorter_custom_sort_field(self):
        c = BSTManyRelatedColumn(
            "samples__characteristic",
            BSTMRCAnimalTestModel,
            sort_expression="samples__name",
        )
        self.assertIsInstance(c.sorter, BSTManyRelatedSorter)
        self.assertEqual("samples__characteristic", c.sorter.name)
        self.assertTrue(c.sorter.asc)
        self.assertEqual("Min(Lower(F(samples__name)))", str(c.sorter.expression))

    def test_create_sorter_custom_agg_warning(self):
        with self.assertWarns(DeveloperWarning) as aw:
            BSTManyRelatedColumn(
                "samples__characteristic",
                BSTMRCAnimalTestModel,
                sort_expression=Count(
                    "samples__characteristic", output_field=IntegerField, distinct=True
                ),
            )
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Unable to apply aggregate function 'Min' to the sorter for column 'samples__characteristic'",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "already has an aggregate function 'Count(F(samples__characteristic), distinct=True)'",
            str(aw.warnings[0].message),
        )
        self.assertIn("first or last delimited value", str(aw.warnings[0].message))
        self.assertIn("delimited values to be sorted", str(aw.warnings[0].message))
        self.assertIn(
            "must not already be wrapped in an aggregate", str(aw.warnings[0].message)
        )
        self.assertIn(
            "will not base row position on the min/max related value",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "sort of the delimited values will be static", str(aw.warnings[0].message)
        )
        self.assertIn(
            "intended to be an annotation column, use BSTAnnotColumn",
            str(aw.warnings[0].message),
        )

    def test_get_attr_stub(self):
        self.assertEqual(
            "samples_animal_body_weight",
            BSTManyRelatedColumn.get_attr_stub(
                "samples__animal__body_weight", BSTMRCTissueTestModel
            ),
        )

    def test_get_count_name(self):
        self.assertEqual(
            f"samples{BSTManyRelatedColumn._count_attr_tail}",
            BSTManyRelatedColumn.get_count_name("samples", BSTMRCTissueTestModel),
        )
        with self.assertRaises(ProgrammingError) as ar:
            BSTManyRelatedColumn.get_count_name(
                "samples__animal__body_weight", BSTMRCTissueTestModel
            )
        self.assertIn(
            "get_count_name must only be used for many_related_model_path",
            str(ar.exception),
        )
        self.assertIn(
            "last field in the path 'samples__animal__body_weight'", str(ar.exception)
        )
        self.assertIn("not many-related to its parent field", str(ar.exception))

    def test_get_list_name(self):
        self.assertEqual(
            f"samples_animal_body_weight{BSTManyRelatedColumn._list_attr_tail}",
            BSTManyRelatedColumn.get_list_name(
                "samples__animal__body_weight", BSTMRCTissueTestModel
            ),
        )

    def test_generate_header_reverse_related_model_uses_last_fkey(self):
        # Test that reverse relations use the related_name
        c = BSTManyRelatedColumn("msrun_samples__name", BSTMRCSampleTestModel)
        sh = c.generate_header()
        self.assertEqual(underscored_to_title("msrun_samples"), sh)

    def test_generate_header_reverse_relation(self):
        # Test that the field name "animal" is not used
        c = BSTManyRelatedColumn("samples", BSTMRCAnimalTestModel)
        sh = c.generate_header()
        self.assertEqual(underscored_to_title("samples"), sh)

        c = BSTManyRelatedColumn("samples__name", BSTMRCAnimalTestModel)
        sh = c.generate_header()
        self.assertEqual(underscored_to_title("samples"), sh)

        c = BSTManyRelatedColumn("samples__characteristic", BSTMRCAnimalTestModel)
        sh = c.generate_header()
        self.assertEqual(underscored_to_title("characteristics"), sh)

    def test_init_is_fk(self):
        self.assertTrue(
            BSTManyRelatedColumn("animal__studies", BSTMRCSampleTestModel).is_fk
        )
        self.assertFalse(
            BSTManyRelatedColumn("animal__studies__name", BSTMRCSampleTestModel).is_fk
        )

    def test_generate_header_related_model_name_field_to_fkey_name(self):
        # Test when related model unique field, uses foreign key name when field is "name"
        c = BSTManyRelatedColumn("studies__name", BSTMRCAnimalTestModel)
        ah = c.generate_header()
        self.assertEqual(underscored_to_title("studies"), ah)

    def test_generate_header_related_model_uses_last_fkey(self):
        # Test that every other field uses - underscored_to_title("_".join(path_tail))
        c = BSTManyRelatedColumn("animals__sex", BSTMRCStudyTestModel)
        sh = c.generate_header()
        self.assertEqual(underscored_to_title("sexes"), sh)

    def test_generate_header_field_verbose_name(self):
        # Test if field has verbose name with caps - return pluralized version of field.verbose_name
        c = BSTManyRelatedColumn("animals__treatment", BSTMRCStudyTestModel)
        th = c.generate_header()
        self.assertEqual(
            BSTMRCAnimalTestModel.treatment.field.verbose_name  # pylint: disable=no-member
            + "s",
            th,
        )
        self.assertEqual("Animal Treatments", th)

    def test_generate_header_field_name_to_model_name(self):
        # In a related model, instead of using the model name, the foreign key name is used, because it conveys context
        c = BSTManyRelatedColumn("samples__name", BSTMRCTissueTestModel)
        sh = c.generate_header()
        self.assertEqual(underscored_to_title("samples"), sh)

    def test_generate_header_field_name_to_model_cap_verbose_name(self):
        # Test caps model verbose_name NOT used because the foreign key is used for context
        c = BSTManyRelatedColumn("animals__treatment__name", BSTMRCStudyTestModel)
        sh = c.generate_header()
        self.assertEqual(
            underscored_to_title("treatments"),
            sh,
        )

    def test_generate_header_field_name_not_unique_not_changed_to_model_name(self):
        # Test diff model verbose_name used as-is
        c = BSTManyRelatedColumn("samples__tissue__name", BSTMRCAnimalTestModel)
        th = c.generate_header()
        self.assertEqual(underscored_to_title("names"), th)
