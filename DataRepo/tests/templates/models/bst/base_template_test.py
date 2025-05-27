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
        "treatment": ForeignKey(
            to="loader.BTTTreatmentTestModel",
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
        "__str__": lambda slf: slf.name,
    },
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
        cls.a2 = BTTAnimalTestModel.objects.create(
            name="A2", desc="a2", treatment=cls.t2
        )
        cls.a2.studies.add(cls.s1)
        cls.a2.studies.add(cls.s2)
        super().setUpTestData()
