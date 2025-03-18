from django.test import tag
from django.urls import reverse

from DataRepo.models import Animal
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class AnimalViewTests(ModelViewTests):
    @tag("animal")
    def test_animal_list(self):
        response = self.client.get(reverse("animal_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/animal/animal_list.html")
        self.assertEqual(len(response.context["animal_list"]), self.ALL_ANIMALS_COUNT)
        self.assertEqual(len(response.context["df"]), self.ALL_ANIMALS_COUNT)

    @tag("animal")
    def test_animal_detail(self):
        a1 = Animal.objects.filter(name="971").get()
        response = self.client.get(reverse("animal_detail", args=[a1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/animal/animal_detail.html")
        self.assertEqual(response.context["animal"].name, "971")
        self.assertEqual(self.ALL_MSRUN_SAMPLES_COUNT, len(response.context["df"]))

    @tag("animal")
    def test_animal_detail_404(self):
        a = Animal.objects.order_by("id").last()
        response = self.client.get(reverse("animal_detail", args=[a.id + 1]))
        self.assertEqual(response.status_code, 404)


# This runs the above tests again with auto-update diabled
AnimalViewNullToleranceTests = create_null_tolerance_test_class(AnimalViewTests)
