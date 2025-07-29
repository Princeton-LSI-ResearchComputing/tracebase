from django.db.models import Value

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.tests.views.models.bst.column.test_field import (
    BSTCStudyTestModel,
)
from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.filterer.annotation import (
    BSTAnnotFilterer,
)
from DataRepo.views.models.bst.column.filterer.field import BSTFilterer
from DataRepo.views.models.bst.column.sorter.annotation import BSTAnnotSorter
from DataRepo.views.models.bst.column.sorter.field import BSTSorter


class BSTAnnotColumnTests(TracebaseTestCase):

    def test_init_sorter_filterer_defaults(self):
        # Test if self.is_annotation is None - return underscored_to_title(self.name)
        ann = "meaning_of_life"
        c = BSTAnnotColumn(ann, Value(42))
        self.assertEqual(BSTAnnotSorter, type(c.sorter))
        self.assertEqual(BSTAnnotFilterer, type(c.filterer))

    def test_init_sorter_filterer_str(self):
        # Test if self.is_annotation is None - return underscored_to_title(self.name)
        ann = "meaning_of_life"
        c = BSTAnnotColumn(ann, Value(42), sorter="mySorter", filterer="myFilterer")
        self.assertEqual(BSTAnnotSorter, type(c.sorter))
        self.assertEqual(BSTAnnotFilterer, type(c.filterer))

    def test_init_sorter_invalid(self):
        # Test if self.is_annotation is None - return underscored_to_title(self.name)
        ann = "meaning_of_life"
        sorter = BSTSorter("name", BSTCStudyTestModel)
        with self.assertRaises(TypeError):
            BSTAnnotColumn(ann, Value(42), sorter=sorter)

    def test_init_filterer_invalid(self):
        # Test if self.is_annotation is None - return underscored_to_title(self.name)
        ann = "meaning_of_life"
        filterer = BSTFilterer("name", BSTCStudyTestModel)
        with self.assertRaises(TypeError):
            BSTAnnotColumn(ann, Value(42), filterer=filterer)

    def test_generate_header_annotation(self):
        # Test if self.is_annotation is None - return underscored_to_title(self.name)
        ann = "meaning_of_life"
        c = BSTAnnotColumn(ann, Value(42))
        an = c.generate_header()
        self.assertEqual(underscored_to_title(ann), an)
        self.assertEqual("Meaning of Life", an)
