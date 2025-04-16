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
        "studies": ManyToManyField(
            to="loader.BSTMRCStudyTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.BSTMRCTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
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
    {"name": CharField(), "desc": CharField()},
)


@override_settings(DEBUG=True)
class BSTManyRelatedColumnTests(TracebaseTestCase):

    @TracebaseTestCase.assertNotWarns()
    def test_init_basic_defaults(self):
        # self.list_attr_name
        # self.count_attr_name
        # self.delim
        # self.limit
        c = BSTManyRelatedColumn("studies__name", BSTMRCAnimalTestModel)
        self.assertEqual(
            f"studies_name{BSTManyRelatedColumn.list_attr_tail}", c.list_attr_name
        )
        self.assertEqual(
            f"studies_name{BSTManyRelatedColumn.count_attr_tail}", c.count_attr_name
        )
        self.assertEqual(BSTManyRelatedColumn.delimiter, c.delim)
        self.assertEqual(BSTManyRelatedColumn.limit, c.limit)
        self.assertEqual(BSTManyRelatedColumn.ascending, c.asc)

    @TracebaseTestCase.assertNotWarns()
    def test_init_fk_field_attrs(self):
        c = BSTManyRelatedColumn("studies", BSTMRCAnimalTestModel)
        self.assertEqual(
            f"studies{BSTManyRelatedColumn.list_attr_tail}", c.list_attr_name
        )
        self.assertEqual(
            f"studies{BSTManyRelatedColumn.count_attr_tail}", c.count_attr_name
        )

    @TracebaseTestCase.assertNotWarns()
    def test_init_many_related_model_path(self):
        c = BSTManyRelatedColumn("animals__samples__tissue__name", BSTMRCStudyTestModel)
        self.assertEqual("animals", c.many_related_model_path)

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
