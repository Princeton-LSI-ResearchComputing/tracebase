from django.db.models import CharField
from django.urls import reverse

from DataRepo.templatetags.bst_list_view_tags import (
    get_absolute_url,
    get_attr,
    has_attr,
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
        self.assertTrue(has_attr(BSTLVStudy.objects.first(), "get_absolute_url"))
        self.assertTrue(has_attr(BSTLVStudy, "get_absolute_url"))
        self.assertFalse(has_attr(BSTLVCompoundSynonym, "get_absolute_url"))

    def test_get_detail_url(self):
        self.assertEqual(f"/DataRepo/studies/{s.pk}/", get_absolute_url(s))
        s = BSTLVStudy.objects.first()
