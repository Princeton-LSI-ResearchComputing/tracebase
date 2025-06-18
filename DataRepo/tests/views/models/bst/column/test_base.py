from django.db.models import CharField

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst.column.base import BSTBaseColumn

BSTBCStudyTestModel = create_test_model(
    "BSTBCStudyTestModel",
    {"name": CharField(max_length=255, unique=True)},
)
BSTBCAnimalTestModel = create_test_model(
    "BSTBCAnimalTestModel",
    {"name": CharField(max_length=255, unique=True)},
    attrs={"get_absolute_url": lambda self: f"/DataRepo/animal/{self.pk}/"},
)


class BSTBaseColumnTests(TracebaseTestCase):
    def test_has_detail(self):
        self.assertTrue(BSTBaseColumn.has_detail(BSTBCAnimalTestModel))
        self.assertFalse(BSTBaseColumn.has_detail(BSTBCStudyTestModel))
