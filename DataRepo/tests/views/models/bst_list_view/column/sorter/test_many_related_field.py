from django.db.models import CharField, F, IntegerField
from django.db.models.functions import Lower, Upper
from django.templatetags.static import static
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst_list_view.column.sorter.field import BSTSorter

BSTSTestModel = create_test_model(
    "BSTSTestModel",
    {
        "name": CharField(max_length=255),
        "value": IntegerField(),
    },
)


@override_settings(DEBUG=True)
class BSTManyRelatedSorterTests(TracebaseTestCase):
    pass
