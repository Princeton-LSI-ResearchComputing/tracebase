from django.db.models import CASCADE, CharField, ForeignKey, ManyToManyField
from django.db.models.functions import Lower

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)

BTTStudyTestModel = create_test_model(
    "BTTStudyTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": [Lower("name").desc()]},
        ),
    },
)

BTTAnimalTestModel = create_test_model(
    "BTTAnimalTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
        "studies": ManyToManyField(
            to="loader.BTTStudyTestModel", related_name="animals"
        ),
        "friends": ManyToManyField(
            to="loader.BTTFriendTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.BTTTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
        ),
        "housing": ForeignKey(
            to="loader.BTTHousingTestModel",
            null=True,
            related_name="animals",
            on_delete=CASCADE,
        ),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": ["-name"]},
        ),
    },
)

BTTTreatmentTestModel = create_test_model(
    "BTTTreatmentTestModel",
    {"name": CharField(unique=True), "desc": CharField()},
    attrs={
        "get_absolute_url": lambda _: "thisisaurl",
        "__str__": lambda slf: f"{slf.name}, {slf.desc}",
    },
)

BTTHousingTestModel = create_test_model(
    "BTTHousingTestModel",
    # No unique field to intentionally cause no representative field
    {"name": CharField(null=True), "desc": CharField(null=True)},
    # No get_absolute_url or __str__ methods to test that a default object str is used
)

BTTFriendTestModel = create_test_model(
    "BTTFriendTestModel",
    # No unique field to intentionally cause no representative field
    {"name": CharField(null=True), "desc": CharField(null=True)},
    # No get_absolute_url or __str__ methods to test that a default object str is used
)


class BaseTemplateTests(TracebaseTestCase):

    @classmethod
    def setUpTestData(cls):
        cls.t1 = BTTTreatmentTestModel.objects.create(name="T1", desc="t1")
        cls.t2 = BTTTreatmentTestModel.objects.create(name="oddball", desc="t2")
        cls.s1 = BTTStudyTestModel.objects.create(name="S1", desc="s1")
        cls.s2 = BTTStudyTestModel.objects.create(name="S2", desc="s2")
        cls.a1 = BTTAnimalTestModel.objects.create(
            name="A1", desc="a1", treatment=cls.t1
        )
        cls.a1.studies.add(cls.s1)
        cls.h1 = BTTHousingTestModel.objects.create(name="H1", desc="h1")
        cls.a2 = BTTAnimalTestModel.objects.create(
            name="A2", desc="a2", treatment=cls.t2, housing=cls.h1
        )
        cls.a2.studies.add(cls.s1)
        cls.a2.studies.add(cls.s2)
        cls.f1 = BTTFriendTestModel.objects.create(name="F1", desc="f1")
        cls.a2.friends.add(cls.f1)
        super().setUpTestData()
