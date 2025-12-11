from django.db.models import (
    CASCADE,
    Case,
    CharField,
    DurationField,
    F,
    ForeignKey,
    Func,
    IntegerField,
    Value,
    When,
)
from django.db.models.aggregates import Count
from django.db.models.functions import Extract, Lower
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.tests.views.models.bst.column.test_field import (
    BSTCStudyTestModel,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.filterer.annotation import (
    BSTAnnotFilterer,
)
from DataRepo.views.models.bst.column.filterer.field import BSTFilterer
from DataRepo.views.models.bst.column.sorter.annotation import BSTAnnotSorter
from DataRepo.views.models.bst.column.sorter.field import BSTSorter

BACSampleTestModel = create_test_model(
    "BACSampleTestModel",
    {
        "name": CharField(max_length=255, unique=True, help_text="Sample name."),
        "time": DurationField(help_text="Storage time.", null=True),
        "tissue": ForeignKey(
            to="loader.BACTissueTestModel",
            related_name="samples",
            on_delete=CASCADE,
        ),
        "animal": ForeignKey(
            to="loader.BACAnimalTestModel",
            related_name="samples",
            on_delete=CASCADE,
            null=True,
        ),
    },
)

BACTissueTestModel = create_test_model(
    "BACTissueTestModel",
    {"name": CharField(max_length=255, help_text="Tissue name.")},
)

BACAnimalTestModel = create_test_model(
    "BACAnimalTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "treatment": CharField(max_length=255),
    },
)


class BSTAnnotColumnTests(TracebaseTestCase):

    def test_init_sorter_filterer_defaults(self):
        ann = "meaning_of_life"
        c = BSTAnnotColumn(ann, Value(42))
        self.assertEqual(BSTAnnotSorter, type(c.sorter))
        self.assertEqual(BSTAnnotFilterer, type(c.filterer))

    def test_init_sorter_filterer_str(self):
        ann = "meaning_of_life"
        c = BSTAnnotColumn(ann, Value(42), sorter="mySorter", filterer="myFilterer")
        self.assertEqual(BSTAnnotSorter, type(c.sorter))
        self.assertEqual(BSTAnnotFilterer, type(c.filterer))

    def test_init_sorter_invalid(self):
        ann = "meaning_of_life"
        sorter = BSTSorter("name", BSTCStudyTestModel, name=ann)
        # Testing to make sure that BSTSorter, as a type, is caught as wrong.  Must be a BSTAnnotSorter.
        with self.assertRaises(TypeError):
            BSTAnnotColumn(ann, Value(42), sorter=sorter)

    def test_init_filterer_invalid(self):
        ann = "meaning_of_life"
        filterer = BSTFilterer("name", BSTCStudyTestModel)
        # Testing to make sure that BSTFilterer, as a type, is caught as wrong.  Must be a BSTAnnotFilterer.
        with self.assertRaises(TypeError):
            BSTAnnotColumn(ann, Value(42), filterer=filterer)

    def test_generate_header_annotation(self):
        ann = "meaning_of_life"
        c = BSTAnnotColumn(ann, Value(42))
        an = c.generate_header()
        self.assertEqual(underscored_to_title(ann), an)
        self.assertEqual("Meaning of Life", an)

    def test_tooltip(self):
        # basic case
        c = BSTAnnotColumn(
            "lower_name",
            Lower(F("name"), output_field=CharField()),
            model=BACTissueTestModel,
        )
        self.assertEqual("Tissue name.", c.tooltip)

        # More complex case
        converter = Extract(
            F("time"),
            "epoch",
        ) / Value(604800)
        c = BSTAnnotColumn("time_weeks", converter, model=BACSampleTestModel)
        self.assertEqual("Storage time.", c.tooltip)

        # Exclude help_text
        c = BSTAnnotColumn(
            "time_weeks", converter, model=BACSampleTestModel, help_text=False
        )
        self.assertIsNone(c.tooltip)

        # help_text appended to
        c = BSTAnnotColumn(
            "time_weeks", converter, model=BACSampleTestModel, tooltip="Units: weeks."
        )
        self.assertEqual("Storage time.\n\nUnits: weeks.", c.tooltip)

        # Related model case
        converter = Count("samples__name", output_field=IntegerField(), distinct=True)
        c = BSTAnnotColumn(
            "sample_count",
            converter,
            tooltip="Count of sample names.",
            model=BACTissueTestModel,
        )
        self.assertEqual("Count of sample names.", c.tooltip)

    def test_related_model(self):
        # basic case
        c = BSTAnnotColumn(
            "alternate_name",
            Case(
                When(
                    name="x",
                    then="animal__treatment",
                ),
                When(
                    name="y",
                    then="animal__name",
                ),
                output_field=CharField(),
            ),
            model=BACSampleTestModel,
        )
        self.assertEqual(BACAnimalTestModel, c.related_model)

    def test_related_model_paths(self):
        # basic case
        c = BSTAnnotColumn(
            "alternate_name",
            Case(
                When(
                    name="x",
                    then="animal__name",
                ),
                When(
                    name="y",
                    then="tissue__name",
                ),
                output_field=CharField(),
            ),
            model=BACSampleTestModel,
        )
        self.assertEqual(["animal", "tissue"], c.related_model_paths)

    def test_get_model_object_fk_output(self):
        """This tests that when a converter's output_field is a ForeignKey field, get_model_object will retrieve the
        model object"""
        nog = BACTissueTestModel.objects.create(id=1, name="noggin")
        c = BSTAnnotColumn(
            "tissue_annot",
            Value(
                1,
                output_field=ForeignKey(
                    to="loader.BACTissueTestModel", on_delete=CASCADE
                ),
            ),
        )
        self.assertEquivalent(nog, c.get_model_object(1))

    @override_settings(DEBUG=True)
    def test_get_model_object_extracted_from_field_path(self):
        """This tests that when a converter contains a field path to a foreign key (even if the output_field is not a
        key field), get_model_object will retrieve the model object"""
        nog = BACTissueTestModel.objects.create(id=5, name="noggin")
        BACSampleTestModel.objects.create(id=1, name="s1", tissue=nog)
        c = BSTAnnotColumn(
            "lower_tissue",
            # All I need is an expression that takes the foreign key field path to be extracted
            Func("tissue", output_field=IntegerField()),
            model=BACSampleTestModel,
        )
        self.assertEqual(c.related_model, BACTissueTestModel)
        self.assertEquivalent(nog, c.get_model_object(5))
        # Assert that when the record is not found, it returns None
        with self.assertWarns(DeveloperWarning) as aw:
            self.assertIsNone(c.get_model_object(6))
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "BACTissueTestModel record not found using annotation value '6' from annotation column 'lower_tissue'.",
            str(aw.warnings[0].message),
        )

    def test_is_related_to_many_related_model_path(self):
        converter = Count("samples", output_field=IntegerField(), distinct=True)
        c = BSTAnnotColumn(
            "sample_count",
            converter,
            model=BACAnimalTestModel,
        )
        self.assertTrue(c.is_related_to_many_related_model_path("samples"))
        self.assertFalse(c.is_related_to_many_related_model_path("studies"))
