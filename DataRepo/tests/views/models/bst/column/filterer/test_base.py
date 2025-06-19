from django.db.models import CharField
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst.column.filterer.base import BSTBaseFilterer

BSTBFStudyTestModel = create_test_model(
    "BSTBFStudyTestModel",
    {"name": CharField(max_length=255)},
)


class FiltererTest(BSTBaseFilterer):
    pass


@override_settings(DEBUG=True)
class BSTFiltererTests(TracebaseTestCase):

    @TracebaseTestCase.assertNotWarns()
    def test_init_choices_callable_list(self):
        expected = {}
        for n in range(4):
            name = f"name{n}"
            BSTBFStudyTestModel.objects.create(name=name)
            expected[name] = name

        def get_choices_list():
            return list(BSTBFStudyTestModel.objects.values_list("name", flat=True))

        f = FiltererTest("name", choices=get_choices_list)
        self.assertDictEquivalent(expected, f.choices)
        self.assertEqual(BSTBaseFilterer.INPUT_METHODS.SELECT, f.input_method)

    @TracebaseTestCase.assertNotWarns()
    def test_init_choices_callable_dict(self):
        expected = {}
        for n in range(4):
            name = f"name{n}"
            BSTBFStudyTestModel.objects.create(name=name)
            expected[name] = name

        def get_choices_dict():
            return dict(
                (n, n)
                for n in list(
                    BSTBFStudyTestModel.objects.values_list("name", flat=True)
                )
            )

        f = FiltererTest("name", choices=get_choices_dict)
        self.assertDictEquivalent(expected, f.choices)
        self.assertEqual(BSTBaseFilterer.INPUT_METHODS.SELECT, f.input_method)
