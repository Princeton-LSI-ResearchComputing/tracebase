from django.db.models import CharField, Value
from django.test import override_settings
from django.urls import reverse

from DataRepo.templatetags.bsttags import (
    get_absolute_url,
    get_attr,
    get_rec_val,
    has_attr,
    is_model_obj,
    keys,
)
from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.field import BSTColumn

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


@override_settings(DEBUG=True)
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
        s = BSTLVStudy.objects.first()
        self.assertEqual(f"/DataRepo/studies/{s.pk}/", get_absolute_url(s))

    @TracebaseTestCase.assertNotWarns()
    def test_get_rec_val(self):
        rec = BSTLVStudy.objects.first()
        col = BSTColumn("name", BSTLVStudy)
        self.assertEqual("a", get_rec_val(rec, col))

        # Test error handling
        badcol = BSTAnnotColumn("badname", Value(1))
        with self.assertWarns(DeveloperWarning) as aw:
            self.assertEqual("ERROR", get_rec_val(rec, badcol))
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "problem was encountered while processing BSTAnnotColumn 'badname'",
            str(aw.warnings[0].message),
        )
        self.assertIn("Exception:", str(aw.warnings[0].message))
        self.assertIn("AttributeError", str(aw.warnings[0].message))
        self.assertIn(
            "'BSTLVStudy' object has no attribute 'badname'",
            str(aw.warnings[0].message),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_keys(self):
        self.assertEqual(["a", "b"], keys({"a": 1, "b": 2}))
