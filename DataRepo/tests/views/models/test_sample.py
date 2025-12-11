from django.test import tag
from django.urls import reverse

from DataRepo.models import Animal, Sample
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class SampleViewTests(ModelViewTests):
    @tag("sample")
    def test_sample_list(self):
        response = self.client.get("/DataRepo/samples/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ALL_SAMPLES_COUNT, len(response.context["sample_list"]))

    @tag("sample")
    def test_sample_list_per_animal(self):
        a1 = Animal.objects.filter(name="971").get()
        s1 = Sample.objects.filter(animal_id=a1.id)
        response = self.client.get("/DataRepo/samples/?animal=" + str(a1.pk))
        # The initial response is redirected to add &subquery=true, after clearing search/filter cookies.
        self.assertEqual(response.status_code, 302)
        response = self.client.get(
            "/DataRepo/samples/?subquery=true&animal=" + str(a1.pk)
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(s1.count(), len(response.context["sample_list"]))

    @tag("sample")
    def test_sample_detail(self):
        s1 = Sample.objects.filter(name="BAT-xz971").get()
        response = self.client.get(reverse("sample_detail", args=[s1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/sample/sample_detail.html")
        self.assertEqual(response.context["sample"].name, "BAT-xz971")

    @tag("sample")
    def test_sample_detail_404(self):
        s = Sample.objects.order_by("id").last()
        response = self.client.get(reverse("sample_detail", args=[s.id + 1]))
        self.assertEqual(response.status_code, 404)


# This runs the above tests again with auto-update diabled
SampleViewNullToleranceTests = create_null_tolerance_test_class(SampleViewTests)
