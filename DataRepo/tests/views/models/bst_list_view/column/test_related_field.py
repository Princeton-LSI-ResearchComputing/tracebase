from django.db.models import (
    CASCADE,
    CharField,
    FloatField,
    ForeignKey,
    ManyToManyField,
)
from django.db.models.functions import Lower
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst_list_view.column.related_field import (
    BSTRelatedColumn,
)

BSTRCStudyTestModel = create_test_model(
    "BSTRCStudyTestModel",
    {"name": CharField(max_length=255, unique=True)},
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "verbose_name": "Study"},
        ),
    },
)
BSTRCAnimalTestModel = create_test_model(
    "BSTRCAnimalTestModel",
    {
        "name": CharField(max_length=255),
        "sex": CharField(choices=[("F", "female"), ("M", "male")]),
        "body_weight": FloatField(verbose_name="Weight (g)"),
        "studies": ManyToManyField(
            to="loader.BSTRCStudyTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.BSTRCTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
        ),
    },
    attrs={
        "get_absolute_url": lambda self: f"/DataRepo/animal/{self.pk}/",
        "Meta": type(
            "Meta",
            (),
            {
                "app_label": "loader",
                "verbose_name": "animal",
                "ordering": [Lower("name")],
            },
        ),
    },
)
BSTRCSampleTestModel = create_test_model(
    "BSTRCSampleTestModel",
    {
        "animal": ForeignKey(
            to="loader.BSTRCAnimalTestModel", related_name="samples", on_delete=CASCADE
        ),
        "characteristic": CharField(),
        "name": CharField(max_length=255, unique=True),
        "tissue": ForeignKey(
            to="loader.BSTRCTissueTestModel", related_name="samples", on_delete=CASCADE
        ),
    },
    attrs={
        "get_absolute_url": lambda self: f"/DataRepo/sample/{self.pk}/",
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": ["characteristic", "animal__name"]},
        ),
    },
)
BSTRCMSRunSampleTestModel = create_test_model(
    "BSTRCMSRunSampleTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "sample": ForeignKey(
            to="loader.BSTRCSampleTestModel",
            related_name="msrun_samples",
            on_delete=CASCADE,
        ),
    },
)
BSTRCTissueTestModel = create_test_model(
    "BSTRCTissueTestModel",
    {"name": CharField(max_length=255)},
)
BSTRCTreatmentTestModel = create_test_model(
    "BSTRCTreatmentTestModel",
    {"name": CharField(), "desc": CharField()},
)


@override_settings(DEBUG=True)
class BSTRelatedColumnTests(TracebaseTestCase):

    @TracebaseTestCase.assertNotWarns()
    def test_init_display_field_fk(self):
        c = BSTRelatedColumn("sample", model=BSTRCMSRunSampleTestModel)
        self.assertEqual(
            "sample__name",
            c.display_field_path,
            msg="Gets first unique field when ordering has multiple fields",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.searchable)

        c = BSTRelatedColumn("sample__animal", model=BSTRCMSRunSampleTestModel)
        self.assertEqual(
            "sample__animal__name",
            c.display_field_path,
            msg="Gets and resolves ordering field, if one",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.searchable)

        c = BSTRelatedColumn("tissue", model=BSTRCSampleTestModel)
        self.assertEqual(
            "tissue__name",
            c.display_field_path,
            msg="Gets only field",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.searchable)

        c = BSTRelatedColumn(
            "sample",
            model=BSTRCMSRunSampleTestModel,
            display_field_path="sample__characteristic",
        )
        self.assertEqual(
            "sample__characteristic",
            c.display_field_path,
            msg="Custom display field",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.searchable)

        c = BSTRelatedColumn(
            "sample",
            model=BSTRCMSRunSampleTestModel,
            display_field_path="sample__animal__name",
        )
        self.assertEqual(
            "sample__animal__name",
            c.display_field_path,
            msg="Custom display field from related model",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.searchable)

    @TracebaseTestCase.assertNotWarns()
    def test_init_display_field_nonfk(self):
        c = BSTRelatedColumn("sample__characteristic", model=BSTRCMSRunSampleTestModel)
        self.assertEqual(
            "sample__characteristic",
            c.display_field_path,
            msg="Display field is the same as field_path when not as foreign key",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.searchable)

    @TracebaseTestCase.assertNotWarns()
    def test_init_display_field_invalid_nonfk(self):
        with self.assertRaises(ValueError) as ar:
            # Different field
            BSTRelatedColumn(
                "sample__characteristic",
                model=BSTRCMSRunSampleTestModel,
                display_field_path="sample__name",
            ),
        self.assertEqual(
            (
                "display_field_path 'sample__name' is only allowed to differ from field_path 'sample__characteristic' "
                "when the field is a foreign key."
            ),
            str(ar.exception),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_init_display_field_invalid_fk(self):
        with self.assertRaises(ValueError) as ar:
            # Different field not under path
            BSTRelatedColumn(
                "sample__animal",
                model=BSTRCMSRunSampleTestModel,
                display_field_path="sample__name",
            ),
        self.assertEqual(
            "display_field_path 'sample__name' must start with the field_path 'sample__animal'.",
            str(ar.exception),
        )

    def test_init_searchable_disabled(self):
        with self.assertWarns(DeveloperWarning) as aw:
            c = BSTRelatedColumn("treatment", model=BSTRCAnimalTestModel)
        self.assertFalse(c.searchable)
        self.assertIsNone(c.display_field_path)
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Unable to automatically select an appropriate display_field_path",
            str(aw.warnings[0].message),
        )
        self.assertIn("foreign key field_path 'treatment'", str(aw.warnings[0].message))
        self.assertIn(
            "'BSTRCTreatmentTestModel._meta.ordering[0]' when only 1 ordering",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "first non-ID unique field in 'BSTRCTreatmentTestModel'",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "only field if there is only 1 non-ID field", str(aw.warnings[0].message)
        )
        self.assertIn("cannot be searchable or sortable", str(aw.warnings[0].message))
        self.assertIn(
            "unless a display_field_path is supplied", str(aw.warnings[0].message)
        )

    def test_init_sortable_disabled(self):
        with self.assertWarns(DeveloperWarning) as aw:
            c = BSTRelatedColumn("treatment", model=BSTRCAnimalTestModel)
        self.assertFalse(c.sortable)
        self.assertIsNone(c.display_field_path)
        self.assertEqual(1, len(aw.warnings))

    def test_init_searchable_sortable_disabled(self):
        with self.assertWarns(DeveloperWarning) as aw:
            c = BSTRelatedColumn("treatment", model=BSTRCAnimalTestModel)
        self.assertFalse(c.sortable)
        self.assertFalse(c.searchable)
        self.assertIsNone(c.display_field_path)
        self.assertEqual(1, len(aw.warnings))

    def test_init_searchable_disallowed(self):
        with self.assertWarns(DeveloperWarning):
            with self.assertRaises(ValueError) as ar:
                BSTRelatedColumn(
                    "treatment", model=BSTRCAnimalTestModel, searchable=True
                )
        self.assertIn("['searchable'] cannot be True", str(ar.exception))
        self.assertIn("field_path is a foreign key", str(ar.exception))
        self.assertIn(
            "default display_field_path could not be determined", str(ar.exception)
        )
        self.assertIn(
            "Supply display_field_path to allow search/sort", str(ar.exception)
        )

    def test_init_sortable_disallowed(self):
        with self.assertWarns(DeveloperWarning):
            with self.assertRaises(ValueError) as ar:
                BSTRelatedColumn("treatment", model=BSTRCAnimalTestModel, sortable=True)
        self.assertIn("['sortable'] cannot be True", str(ar.exception))
        self.assertIn("field_path is a foreign key", str(ar.exception))
        self.assertIn(
            "default display_field_path could not be determined", str(ar.exception)
        )
        self.assertIn(
            "Supply display_field_path to allow search/sort", str(ar.exception)
        )

    def test_init_searchable_sortable_disallowed(self):
        with self.assertWarns(DeveloperWarning):
            with self.assertRaises(ValueError) as ar:
                BSTRelatedColumn(
                    "treatment",
                    model=BSTRCAnimalTestModel,
                    searchable=True,
                    sortable=True,
                )
        self.assertIn("['searchable', 'sortable'] cannot be True", str(ar.exception))
        self.assertIn("field_path is a foreign key", str(ar.exception))
        self.assertIn(
            "default display_field_path could not be determined", str(ar.exception)
        )
        self.assertIn(
            "Supply display_field_path to allow search/sort", str(ar.exception)
        )
