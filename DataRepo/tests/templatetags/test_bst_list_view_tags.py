from django.db.models import CharField
from django.urls import reverse

from DataRepo.templatetags.bst_list_view_tags import (
    get_attr,
    has_detail_url,
    is_model_obj,
)
from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)

# Dynamically create models for these tests
BSTLVStudy = create_test_model(
    "BSTLVStudy",
    {
        "name": CharField(),
        "description": CharField(),
    },
    attrs={
        "get_absolute_url": lambda self: reverse(
            "study_detail", kwargs={"pk": self.pk}
        ),
    },
)

BSTLVCompoundSynonym = create_test_model(
    "BSTLVCompoundSynonym",
    {
        "name": CharField(),
    },
)


class BSTListViewTagsTests(TracebaseTestCase):

    @classmethod
    def setUpTestData(cls):
        BSTLVStudy.objects.create(name="a", description="b")

    def test_is_model_obj(self):
        self.assertTrue(is_model_obj(BSTLVStudy.objects.first()))
        self.assertFalse(is_model_obj(BSTLVStudy))
        self.assertFalse(is_model_obj("Study"))

    def test_get_attr(self):
        s = BSTLVStudy.objects.first()
        self.assertEqual(s.name, get_attr(s, "name"))

    def test_has_detail_url(self):
        self.assertTrue(has_detail_url(BSTLVStudy.objects.first()))
        self.assertTrue(has_detail_url(BSTLVStudy))
        self.assertFalse(has_detail_url(BSTLVCompoundSynonym))
