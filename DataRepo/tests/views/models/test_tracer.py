from django.urls import reverse

from DataRepo.models.tracer import Tracer
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class TracerViewTests(ModelViewTests):
    def test_tracer_list(self):
        response = self.client.get(reverse("tracer_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/bst/list_view.html")
        self.assertTemplateUsed(response, "models/bst/th.html")
        self.assertTemplateUsed(response, "models/bst/td.html")
        self.assertTemplateUsed(response, "models/bst/value.html")
        self.assertTemplateUsed(response, "models/bst/value_list.html")
        self.assertTemplateUsed(response, "models/tracer/infusates_td.html")
        self.assertTemplateUsed(response, "models/tracer/infusates_value_list.html")
        self.assertEqual(len(response.context["object_list"]), 1)

    def test_tracer_detail(self):
        t1 = Tracer.objects.first()
        response = self.client.get(reverse("tracer_detail", args=[t1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/bst/detail_view.html")
        self.assertTemplateUsed(response, "models/bst/value.html")
        self.assertTemplateUsed(response, "models/bst/value_list.html")
        self.assertTemplateUsed(response, "models/tracer/infusates_value_detail.html")
        self.assertEqual(t1.id, response.context["object"].id)

    def test_tracer_detail_404(self):
        t = Tracer.objects.order_by("id").last()
        response = self.client.get(reverse("tracer_detail", args=[t.id + 1]))
        self.assertEqual(response.status_code, 404)


# This runs the above tests again with auto-update disabled
TracerViewNullToleranceTests = create_null_tolerance_test_class(TracerViewTests)
