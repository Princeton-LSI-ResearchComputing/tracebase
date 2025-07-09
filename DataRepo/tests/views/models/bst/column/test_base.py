from typing import Optional

from django.db.models import CharField

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.filterer.base import BSTBaseFilterer
from DataRepo.views.models.bst.column.filterer.field import BSTFilterer
from DataRepo.views.models.bst.column.sorter.field import BSTSorter

BSTBCStudyTestModel = create_test_model(
    "BSTBCStudyTestModel",
    {"name": CharField(max_length=255, unique=True)},
)
BSTBCAnimalTestModel = create_test_model(
    "BSTBCAnimalTestModel",
    {"name": CharField(max_length=255, unique=True)},
    attrs={"get_absolute_url": lambda self: f"/DataRepo/animal/{self.pk}/"},
)


class BSTBaseColumnTest(BSTBaseColumn):
    def create_sorter(self, **kwargs):
        # This test class only supports BSTBCStudyTestModel.name
        return BSTSorter("name", BSTBCStudyTestModel, **kwargs)

    def create_filterer(self, field: Optional[str] = None, **kwargs) -> BSTFilterer:
        return BSTFilterer("name", BSTBCStudyTestModel, **kwargs)


class BSTBaseColumnTests(TracebaseTestCase):
    def test_has_detail(self):
        self.assertTrue(BSTBaseColumn.has_detail(BSTBCAnimalTestModel))
        self.assertFalse(BSTBaseColumn.has_detail(BSTBCStudyTestModel))

    def test_init_filter_dict_callable(self):
        """This tests that a BSTBaseColumn constructor can set the filterer to a dict containing a callable for its
        choices argument."""
        expected = {}
        for n in range(4):
            name = f"name{n}"
            BSTBCStudyTestModel.objects.create(name=name)
            expected[name] = name

        def get_choices():
            return list(BSTBCStudyTestModel.objects.values_list("name", flat=True))

        bstbct = BSTBaseColumnTest("name", filterer={"choices": get_choices})
        self.assertDictEquivalent(expected, bstbct.filterer.choices)
        self.assertEqual(
            BSTBaseFilterer.INPUT_METHODS.SELECT, bstbct.filterer.input_method
        )

    def test_BSTBaseColumn(self):
        bstbct1 = BSTBaseColumnTest("name", hidable=False, visible=False)
        self.assertFalse(bstbct1.hidable)
        self.assertTrue(bstbct1.visible)  # visible=False ignored, since not hidable
        self.assertFalse(bstbct1.wrapped)

        bstbct2 = BSTBaseColumnTest("name", hidable=True, visible=False, wrapped=True)
        self.assertTrue(bstbct2.hidable)
        self.assertFalse(bstbct2.visible)  # visible=False ignored, since not hidable
        self.assertTrue(bstbct2.wrapped)

    def test_generate_header(self):
        bstbct = BSTBaseColumnTest("name")
        self.assertEqual(underscored_to_title("name"), bstbct.generate_header())
