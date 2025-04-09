from django.db.models import (
    CASCADE,
    CharField,
    FloatField,
    ForeignKey,
)
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst_list_view.column.related_field import (
    BSTRelatedColumn,
)
from DataRepo.views.models.bst_list_view.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst_list_view.column.many_related_group import (
    BSTColumnGroup,
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
    "BSTCGInfusateTestModel",
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
    {"name": CharField(max_length=255, unique=True)},
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
                BSTRelatedColumn("animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel),
                BSTRelatedColumn("animal__infusate__tracer_links__tracer__compound", BSTCGSampleTestModel),
                BSTRelatedColumn("animal__infusate__tracer_links__concentration", BSTCGSampleTestModel),
            )
        self.assertIn("columns are the wrong type", ar.exception)
        self.assertIn("animal__infusate__tracer_links__tracer__name", ar.exception)
        self.assertIn("animal__infusate__tracer_links__tracer__compound", ar.exception)
        self.assertIn("animal__infusate__tracer_links__concentration", ar.exception)

    def test_init_too_few_cols(self):
        with self.assertRaises(ValueError) as ar:
            BSTColumnGroup(
                BSTManyRelatedColumn("animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel),
            )
        self.assertIn("must be more than 1.", ar.exception)

    def test_init_invalid_initial_col(self):
        with self.assertRaises(ValueError) as ar:
            BSTColumnGroup(
                BSTManyRelatedColumn("animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel),
                BSTManyRelatedColumn("animal__infusate__tracer_links__tracer__compound", BSTCGSampleTestModel),
                initial="animal__infusate__tracer_links__concentration",
            )
        self.assertIn("Initial column 'animal__infusate__tracer_links__concentration' does not match", ar.exception)
        self.assertIn("animal__infusate__tracer_links__tracer__name", ar.exception)
        self.assertIn("animal__infusate__tracer_links__tracer__compound", ar.exception)

    def test_init_invalid_root_model(self):
        with self.assertRaises(ValueError) as ar:
            BSTColumnGroup(
                BSTManyRelatedColumn("animal__infusate__tracer_links__tracer__name", BSTCGSampleTestModel),
                BSTManyRelatedColumn("infusate__tracer_links__tracer__compound", BSTCGAnimalTestModel),
            )
        self.assertIn("1 of the 2 columns do not have the same root model 'BSTCGSampleTestModel'", ar.exception)
        self.assertIn("as the first column: ['BSTCGAnimalTestModel']", ar.exception)

    def test_init_invalid_mm_path(self):
        pass

    def test_init_invalid_related_model(self):
        pass

    def test_init_dupe_col_name(self):
        pass

    def assert_sorters(self, columns, column, asc):
        pass

    def test_init_success(self):
        # check defaults:
        #  asc
        #  initial
        #  model
        #  related_model_path
        #  controlling_column
        #  sorter.expression
        #  sorter.asc
        #  columns' sorters
        # asc=False
        #  check
        #   sorter.expression
        #   sorter.asc
        #   assert_sorters
        pass

    def test_set_sorters_invalid_column(self):
        pass

    def test_set_sorters_column(self):
        # check
        #  sorter.expression
        #  sorter.asc
        #  assert_sorters
        pass

    def test_set_sorters_asc(self):
        # check
        #  sorter.expression
        #  sorter.asc
        #  assert_sorters
        pass
