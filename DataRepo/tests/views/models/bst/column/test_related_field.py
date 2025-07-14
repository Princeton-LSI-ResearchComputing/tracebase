from django.db.models import (
    CASCADE,
    CharField,
    FloatField,
    ForeignKey,
    ManyToManyField,
    OneToOneField,
)
from django.db.models.functions import Lower
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn

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
        "name": CharField(max_length=255, unique=True),
        "sex": CharField(choices=[("F", "female"), ("M", "male")]),
        "body_weight": FloatField(verbose_name="Weight (g)"),
        "studies": ManyToManyField(
            to="loader.BSTRCStudyTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.BSTRCTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
            verbose_name="Animal Treatment",
        ),
        "weirdone": OneToOneField(
            to="loader.BSTRCWeirdOneTestModel",
            on_delete=CASCADE,
            primary_key=True,
            related_name="weird_animal",
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
BSTRCWeirdOneTestModel = create_test_model(
    "BSTRCWeirdOneTestModel",
    {"name": CharField(max_length=255, unique=True)},
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "verbose_name": "BSTrcWeirdOneTestModel"},
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
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "verbose_name": "BSTrcTissueTestModel"},
        ),
    },
)
BSTRCTreatmentTestModel = create_test_model(
    "BSTRCTreatmentTestModel",
    {"name": CharField(), "desc": CharField()},
)


@override_settings(DEBUG=True)
class BSTRelatedColumnTests(TracebaseTestCase):

    @TracebaseTestCase.assertNotWarns()
    def test_init_display_field_fk(self):
        c = BSTRelatedColumn("sample", BSTRCMSRunSampleTestModel)
        self.assertEqual(
            "sample__name",
            c.display_field_path,
            msg="Gets first unique field when ordering has multiple fields",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.filterable)

        c = BSTRelatedColumn("sample__animal", BSTRCMSRunSampleTestModel)
        self.assertEqual(
            "sample__animal__name",
            c.display_field_path,
            msg="Gets and resolves ordering field, if one",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.filterable)

        c = BSTRelatedColumn("tissue", BSTRCSampleTestModel)
        self.assertEqual(
            "tissue__name",
            c.display_field_path,
            msg="Gets only field",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.filterable)

        c = BSTRelatedColumn(
            "sample",
            BSTRCMSRunSampleTestModel,
            display_field_path="sample__characteristic",
        )
        self.assertEqual(
            "sample__characteristic",
            c.display_field_path,
            msg="Custom display field",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.filterable)

        c = BSTRelatedColumn(
            "sample",
            BSTRCMSRunSampleTestModel,
            display_field_path="sample__animal__name",
        )
        self.assertEqual(
            "sample__animal__name",
            c.display_field_path,
            msg="Custom display field from related model",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.filterable)

    def test_no_representative(self):
        """This tests that when a related model has no representative field, it is automatically not filterable or
        sortable, and a tooltip is set"""
        BSTRCNoRepTestModel = create_test_model(  # noqa: F841
            "BSTRCNoRepTestModel",
            {
                # No unique field, i.e. no representative for display when a related model creates a column for the
                # foreign key to this model
                "value1": CharField(max_length=255),
                "value2": CharField(max_length=255),
            },
        )
        BSTRCMainTestModel = create_test_model(
            "BSTRCMainTestModel",
            {
                "name": CharField(max_length=255, unique=True),
                "norep": ForeignKey(
                    to="loader.BSTRCNoRepTestModel",
                    related_name="parent",
                    on_delete=CASCADE,
                ),
            },
        )
        c = BSTRelatedColumn(
            "norep",
            BSTRCMainTestModel,
        )
        self.assertEqual(
            (
                "Search and sort is disabled for this field because the displayed values do not exist in the database "
                "as a single field"
            ),
            c.tooltip,
        )
        self.assertFalse(c.filterable)
        self.assertFalse(c.sortable)
        self.assertEqual(
            (
                "Test tooltip.\n\nSearch and sort is disabled for this field because the displayed values do not "
                "exist in the database as a single field"
            ),
            BSTRelatedColumn(
                "norep",
                BSTRCMainTestModel,
                tooltip="Test tooltip.",
            ).tooltip,
        )

        # Make sure that the 1:1 reverse relation gets the right tooltip
        c = BSTRelatedColumn("weirdone", BSTRCAnimalTestModel)
        self.assertEqual(underscored_to_title("weirdone"), c.generate_header())

    @TracebaseTestCase.assertNotWarns()
    def test_init_display_field_nonfk(self):
        c = BSTRelatedColumn("sample__characteristic", BSTRCMSRunSampleTestModel)
        self.assertEqual(
            "sample__characteristic",
            c.display_field_path,
            msg="Display field is the same as field_path when not as foreign key",
        )
        self.assertTrue(c.sortable)
        self.assertTrue(c.filterable)

    @TracebaseTestCase.assertNotWarns()
    def test_init_display_field_invalid_nonfk(self):
        with self.assertRaises(ValueError) as ar:
            # Different field
            BSTRelatedColumn(
                "sample__characteristic",
                BSTRCMSRunSampleTestModel,
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
                BSTRCMSRunSampleTestModel,
                display_field_path="sample__name",
            ),
        self.assertEqual(
            "display_field_path 'sample__name' must start with the field_path 'sample__animal'.",
            str(ar.exception),
        )

    def test_init_filterable_disabled(self):
        with self.assertWarns(DeveloperWarning) as aw:
            c = BSTRelatedColumn("treatment", BSTRCAnimalTestModel)
        self.assertFalse(c.filterable)
        self.assertEqual("treatment", c.display_field_path)
        self.assertIsInstance(c.display_field, ForeignKey)
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Unable to automatically select a searchable/sortable display_field_path",
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

    def test_init_sortable_disabled(self):
        with self.assertWarns(DeveloperWarning) as aw:
            c = BSTRelatedColumn("treatment", BSTRCAnimalTestModel)
        self.assertFalse(c.sortable)
        self.assertEqual("treatment", c.display_field_path)
        self.assertIsInstance(c.display_field, ForeignKey)
        self.assertEqual(1, len(aw.warnings))

    def test_init_filterable_sortable_disabled(self):
        with self.assertWarns(DeveloperWarning) as aw:
            c = BSTRelatedColumn("treatment", BSTRCAnimalTestModel)
        self.assertFalse(c.sortable)
        self.assertFalse(c.filterable)
        self.assertEqual("treatment", c.display_field_path)
        self.assertIsInstance(c.display_field, ForeignKey)
        self.assertEqual(1, len(aw.warnings))

    def test_init_filterable_disallowed(self):
        with self.assertWarns(DeveloperWarning):
            with self.assertRaises(ValueError) as ar:
                BSTRelatedColumn("treatment", BSTRCAnimalTestModel, filterable=True)
        self.assertIn("['filterable'] cannot be True", str(ar.exception))
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
                BSTRelatedColumn("treatment", BSTRCAnimalTestModel, sortable=True)
        self.assertIn("['sortable'] cannot be True", str(ar.exception))
        self.assertIn("field_path is a foreign key", str(ar.exception))
        self.assertIn(
            "default display_field_path could not be determined", str(ar.exception)
        )
        self.assertIn(
            "Supply display_field_path to allow search/sort", str(ar.exception)
        )

    def test_init_filterable_sortable_disallowed(self):
        with self.assertWarns(DeveloperWarning):
            with self.assertRaises(ValueError) as ar:
                BSTRelatedColumn(
                    "treatment",
                    BSTRCAnimalTestModel,
                    filterable=True,
                    sortable=True,
                )
        self.assertIn("['filterable', 'sortable'] cannot be True", str(ar.exception))
        self.assertIn("field_path is a foreign key", str(ar.exception))
        self.assertIn(
            "default display_field_path could not be determined", str(ar.exception)
        )
        self.assertIn(
            "Supply display_field_path to allow search/sort", str(ar.exception)
        )

    def test_generate_header_related_model_name_field_to_fkey_name(self):
        # Test when related model unique field, uses foreign key name when field is "name"
        c = BSTRelatedColumn("animal__name", BSTRCSampleTestModel)
        ah = c.generate_header()
        self.assertEqual(underscored_to_title("animal"), ah)

    def test_generate_header_related_model_uses_last_fkey(self):
        # Test that every other field uses - underscored_to_title("_".join(path_tail))
        c = BSTRelatedColumn("sample__animal__sex", BSTRCMSRunSampleTestModel)
        sh = c.generate_header()
        self.assertEqual(underscored_to_title("sex"), sh)

        # Make sure that the 1:1 reverse relation gets the right header
        c = BSTRelatedColumn("weirdone", BSTRCAnimalTestModel)
        self.assertEqual(underscored_to_title("weirdone"), c.generate_header())

    def test_generate_header_field_verbose_name(self):
        # Test if field has verbose name with caps - return field.verbose_name
        c = BSTRelatedColumn("treatment", BSTRCAnimalTestModel)
        th = c.generate_header()
        self.assertEqual(
            BSTRCAnimalTestModel.treatment.field.verbose_name,  # pylint: disable=no-member
            th,
        )
        self.assertEqual("Animal Treatment", th)

    def test_generate_header_field_name_to_model_name(self):
        # In a related model, instead of using the model name, the foreign key name is used, because it conveys context
        c = BSTRelatedColumn("sample__name", BSTRCMSRunSampleTestModel)
        sh = c.generate_header()
        self.assertEqual(underscored_to_title("sample"), sh)

    def test_generate_header_field_name_to_model_cap_verbose_name(self):
        # Test caps model verbose_name NOT used because the foreign key is used for context
        c = BSTRelatedColumn("weirdone__name", BSTRCAnimalTestModel)
        sh = c.generate_header()
        self.assertEqual(
            underscored_to_title("weirdone"),
            sh,
        )

    def test_generate_header_field_name_to_model_diff_verbose_name(self):
        # Test diff model verbose_name used as-is
        c = BSTRelatedColumn("animal__name", BSTRCSampleTestModel)
        ah = c.generate_header()
        self.assertEqual(
            underscored_to_title(
                BSTRCAnimalTestModel._meta.__dict__[  # pylint: disable=no-member
                    "verbose_name"
                ]
            ),
            ah,
        )

    def test_generate_header_field_name_not_unique_not_changed_to_model_name(self):
        # Test diff model verbose_name used as-is
        c = BSTRelatedColumn("tissue__name", BSTRCSampleTestModel)
        th = c.generate_header()
        self.assertEqual(underscored_to_title("name"), th)
