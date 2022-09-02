from datetime import timedelta

from django.conf import settings
from django.test import override_settings, tag

from DataRepo.models import Animal, Infusate
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
@tag("animal")
@tag("multi_working")
class AnimalTests(TracebaseTestCase):
    def setUp(self):
        infusate = Infusate()
        infusate.save()
        Animal.objects.create(
            name="test_animal",
            age=timedelta(weeks=int(13)),
            sex="M",
            genotype="WT",
            body_weight=200,
            diet="normal",
            feeding_status="fed",
            infusate=infusate,
        )

    def test_animal_validation(self):
        animal = Animal.objects.get(name="test_animal")
        animal.full_clean()

    def test_animal_name(self):
        """Animal lookup by name"""
        animal = Animal.objects.get(name="test_animal")
        self.assertEqual(animal.name, "test_animal")
