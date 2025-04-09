from django.db.models import (
    CASCADE,
    CharField,
    FloatField,
    ForeignKey,
    ManyToManyField,
)
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst_list_view.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst_list_view.column.many_related_group import (
    BSTColumnGroup,
)
from DataRepo.views.models.bst_list_view.column.related_field import (
    BSTRelatedColumn,
)

BSTCGCompoundTestModel = create_test_model(
    "BSTCGCompoundTestModel",
    {"name": CharField(max_length=255, unique=True)},
)
BSTCGTracerTestModel = create_test_model(
    "BSTCGTracerTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "compound": ForeignKey(
            to="loader.BSTCGCompoundTestModel",
            related_name="tracers",
            on_delete=CASCADE,
        ),
    },
)
BSTCGInfusateTracerTestModel = create_test_model(
    "BSTCGInfusateTracerTestModel",
    {
        "infusate": ForeignKey(
            to="loader.BSTCGInfusateTestModel",
            related_name="tracer_links",
            on_delete=CASCADE,
        ),
        "tracer": ForeignKey(
            to="loader.BSTCGTracerTestModel",
            related_name="infusate_links",
            on_delete=CASCADE,
        ),
        "concentration": FloatField(),
    },
)
BSTCGInfusateTestModel = create_test_model(
    "BSTCGInfusateTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "tracers": ManyToManyField(
            "loader.BSTCGTracerTestModel",
            through="loader.BSTCGInfusateTracerTestModel",
            related_name="infusates",
        ),
    },
)
BSTCGAnimalTestModel = create_test_model(
    "BSTCGAnimalTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "infusate": ForeignKey(
            to="loader.BSTCGInfusateTestModel",
            related_name="animals",
            on_delete=CASCADE,
        ),
    },
)
BSTCGSampleTestModel = create_test_model(
    "BSTCGSampleTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "animal": ForeignKey(
            to="loader.BSTCGAnimalTestModel", related_name="samples", on_delete=CASCADE
        ),
    },
)


@override_settings(DEBUG=True)
class BSTColumnGroupTests(TracebaseTestCase):

    def test_init_wrong_col_type(self):
        with self.assertRaises(TypeError) as ar:
            BSTColumnGroup(
                BSTRelatedColumn("animal__infusate__name", BSTCGSampleTestModel),
                BSTRelatedColumn("animal__name", BSTCGSampleTestModel),
            )
        # Shows the problem scope
        self.assertIn("2 of the 2 columns", str(ar.exception))
        # States the requirement (1)
        self.assertIn("wrong type", str(ar.exception))
        # Shows the problem data
        self.assertIn(
            "column 1, 'animal__infusate__name': BSTRelatedColumn", str(ar.exception)
        )
        self.assertIn("column 2, 'animal__name': BSTRelatedColumn", str(ar.exception))
        # States the requirement (2)
        # Shows the required data
        # Suggests how to fix it
        self.assertIn("must be BSTManyRelatedColumn", str(ar.exception))

    def test_init_too_few_cols(self):
        with self.assertRaises(ValueError) as ar:
            BSTColumnGroup(
                BSTManyRelatedColumn(
                    "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
                ),
            )
        self.assertIn("must be more than 1.", str(ar.exception))

    def test_init_invalid_initial_col(self):
        with self.assertRaises(ValueError) as ar:
            BSTColumnGroup(
                BSTManyRelatedColumn(
                    "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
                ),
                BSTManyRelatedColumn(
                    "animal__infusate__tracer_links__tracer__compound",
                    BSTCGSampleTestModel,
                ),
                initial="animal__infusate__tracer_links__concentration",
            )
        # Shows the problem scope
        # States the requirement
        # Shows the problem data
        self.assertIn(
            "Initial column 'animal__infusate__tracer_links__concentration' does not match",
            str(ar.exception),
        )
        # Shows the required data
        # Suggests how to fix it
        self.assertIn("animal__infusate__tracer_links__tracer__name", str(ar.exception))
        self.assertIn(
            "animal__infusate__tracer_links__tracer__compound", str(ar.exception)
        )

    def test_init_invalid_root_model(self):
        with self.assertRaises(ValueError) as ar:
            BSTColumnGroup(
                BSTManyRelatedColumn(
                    "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
                ),
                BSTManyRelatedColumn(
                    "infusate__tracer_links__tracer__compound", BSTCGAnimalTestModel
                ),
            )
        # Shows the problem scope
        self.assertIn("1 of the 2 columns", str(ar.exception))
        # States the requirement (1)
        self.assertIn("do not have the same root model", str(ar.exception))
        # Shows the required data
        # Suggests how to fix it
        self.assertIn("'BSTCGSampleTestModel'", str(ar.exception))
        # States the requirement (2)
        self.assertIn("as the first column", str(ar.exception))
        # Shows the problem data
        self.assertIn(
            "['infusate__tracer_links__tracer__compound: BSTCGAnimalTestModel']",
            str(ar.exception),
        )

    def test_init_invalid_mm_path(self):
        with self.assertRaises(ValueError) as ar:
            BSTColumnGroup(
                BSTManyRelatedColumn(
                    "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
                ),
                BSTManyRelatedColumn(
                    "animal__infusate__tracers__compound", BSTCGSampleTestModel
                ),
            )
        # States the requirement
        self.assertIn(
            "All columns' field_paths must start with the same many-related model 'animal__infusate__tracer_links'",
            str(ar.exception),
        )
        # Shows the problem scope
        self.assertIn(
            "The following column field_path(s) do not match",
            str(ar.exception),
        )
        # Shows the required data
        self.assertIn(
            "'animal__infusate__tracer_links'",
            str(ar.exception),
        )
        # Explains the problem
        self.assertIn("field_path(s) do not match", str(ar.exception))
        # Shows the problem data
        self.assertIn("\tanimal__infusate__tracers__compound\n", str(ar.exception))
        # Suggests how to fix it
        self.assertIn(
            "supply different columns, adjust their field_paths, or supply related_model_path",
            str(ar.exception),
        )

    def test_init_invalid_related_model(self):
        with self.assertRaises(ValueError) as ar:
            BSTColumnGroup(
                BSTManyRelatedColumn(
                    "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
                ),
                BSTManyRelatedColumn(
                    "animal__infusate__tracer_links__tracer__compound",
                    BSTCGSampleTestModel,
                ),
                related_model_path="animal__infusate__tracer_links__tracer",
            )
        # Shows the problem scope
        self.assertIn("The related_model_path", str(ar.exception))
        # States the requirement
        # Explains the problem
        self.assertIn("not many-related to the root model", str(ar.exception))
        # Shows the required data
        self.assertIn("BSTCGSampleTestModel", str(ar.exception))
        # Shows the problem data
        self.assertIn("animal__infusate__tracer_links__tracer", str(ar.exception))
        # Suggests how to fix it
        self.assertIn("Try setting 'animal__infusate__tracer_links'", str(ar.exception))

    def test_init_dupe_col_name(self):
        with self.assertRaises(ValueError) as ar:
            BSTColumnGroup(
                BSTManyRelatedColumn(
                    "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
                ),
                BSTManyRelatedColumn(
                    "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
                ),
            )
        # States the requirement
        # Shows the required data
        # Suggests how to fix it
        self.assertIn("Duplicate column names not allowed", str(ar.exception))
        # Shows the problem scope
        self.assertIn("1 occurrence(s) in 2 columns", str(ar.exception))
        # Shows the problem data
        self.assertIn("animal__infusate__tracer_links__tracer__name", str(ar.exception))
        # Explains the problem
        self.assertIn("2 occurrences", str(ar.exception))

    def assert_sorters(self, cg: BSTColumnGroup):
        for c in cg.columns:
            self.assertEqual(cg.sorter, c.sorter)

    def test_init_success(self):
        # check defaults:
        cgasc = BSTColumnGroup(
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__compound", BSTCGSampleTestModel
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__concentration", BSTCGSampleTestModel
            ),
        )
        # asc
        self.assertTrue(cgasc.asc)
        # initial
        self.assertEqual("animal__infusate__tracer_links__tracer__name", cgasc.initial)
        # model
        self.assertEqual(BSTCGSampleTestModel, cgasc.model)
        # related_model_path
        self.assertEqual("animal__infusate__tracer_links", cgasc.related_model_path)
        # controlling_column
        self.assertEqual(
            "animal__infusate__tracer_links__tracer__name",
            cgasc.controlling_column.name,
        )
        # sorter.asc
        self.assertTrue(cgasc.sorter.asc)
        # sorter.expression
        self.assertEqual(
            "Min(Lower(F(animal__infusate__tracer_links__tracer__name)))",
            str(cgasc.sorter.expression),
        )
        # columns' sorters
        self.assert_sorters(cgasc)

        # Check defaults for asc=False
        cgdsc = BSTColumnGroup(
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__name",
                BSTCGSampleTestModel,
                asc=False,
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__compound",
                BSTCGSampleTestModel,
                asc=False,
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__concentration",
                BSTCGSampleTestModel,
                asc=False,
            ),
        )
        # sorter.asc
        self.assertFalse(cgdsc.sorter.asc)
        # sorter.expression
        self.assertEqual(
            "Max(Lower(F(animal__infusate__tracer_links__tracer__name)))",
            str(cgdsc.sorter.expression),
        )
        # columns' sorters
        self.assert_sorters(cgdsc)

    def test_set_sorters_invalid_column(self):
        cg = BSTColumnGroup(
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__compound", BSTCGSampleTestModel
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__concentration", BSTCGSampleTestModel
            ),
        )
        with self.assertRaises(ValueError) as ar:
            cg.set_sorters("animal__infusate__tracer_links__tracer")
        # Shows the problem scope
        # Shows the problem data
        self.assertIn(
            "Column 'animal__infusate__tracer_links__tracer'", str(ar.exception)
        )
        # States the requirement
        # Explains the problem
        self.assertIn(
            "does not match any of the columns in this group", str(ar.exception)
        )
        # Suggests how to fix it
        # Shows the required data
        self.assertIn("animal__infusate__tracer_links__tracer__name", str(ar.exception))
        self.assertIn(
            "animal__infusate__tracer_links__tracer__compound", str(ar.exception)
        )
        self.assertIn(
            "animal__infusate__tracer_links__concentration", str(ar.exception)
        )

    def test_set_sorters_column(self):
        cg = BSTColumnGroup(
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__compound", BSTCGSampleTestModel
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__concentration", BSTCGSampleTestModel
            ),
        )
        cg.set_sorters("animal__infusate__tracer_links__concentration", asc=False)
        # sorter.expression
        self.assertEqual(
            "Max(F(animal__infusate__tracer_links__concentration))",
            str(cg.sorter.expression),
        )
        # sorter.asc
        self.assertFalse(cg.sorter.asc)
        # columns' sorters
        self.assert_sorters(cg)

    def test_set_sorters_asc(self):
        cg1 = BSTColumnGroup(
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__name",
                BSTCGSampleTestModel,
                asc=False,
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__compound",
                BSTCGSampleTestModel,
                asc=False,
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__concentration",
                BSTCGSampleTestModel,
                asc=False,
            ),
        )
        cg1.set_sorters("animal__infusate__tracer_links__concentration")
        # sorter.asc - defaults to True for new field
        self.assertTrue(cg1.sorter.asc)
        # sorter.expression
        self.assertEqual(
            "Min(F(animal__infusate__tracer_links__concentration))",
            str(cg1.sorter.expression),
        )
        # columns' sorters
        self.assert_sorters(cg1)

        cg2 = BSTColumnGroup(
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__name",
                BSTCGSampleTestModel,
                asc=False,
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__compound",
                BSTCGSampleTestModel,
                asc=False,
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__concentration",
                BSTCGSampleTestModel,
                asc=False,
            ),
        )
        cg2.set_sorters("animal__infusate__tracer_links__tracer__name")
        # sorter.asc - stays False for same
        self.assertFalse(cg2.sorter.asc)
        # sorter.expression
        self.assertEqual(
            "Max(Lower(F(animal__infusate__tracer_links__tracer__name)))",
            str(cg2.sorter.expression),
        )
        # columns' sorters
        self.assert_sorters(cg2)

        cg3 = BSTColumnGroup(
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__name",
                BSTCGSampleTestModel,
                asc=False,
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__tracer__compound",
                BSTCGSampleTestModel,
                asc=False,
            ),
            BSTManyRelatedColumn(
                "animal__infusate__tracer_links__concentration",
                BSTCGSampleTestModel,
                asc=False,
            ),
        )
        cg3.set_sorters(asc=True)
        # sorter.asc - sets True for current field
        self.assertTrue(cg3.sorter.asc)
        # sorter.expression
        self.assertEqual(
            "Min(Lower(F(animal__infusate__tracer_links__tracer__name)))",
            str(cg3.sorter.expression),
        )
        # columns' sorters
        self.assert_sorters(cg3)
