from django.urls import reverse

from DataRepo.models import LCMethod
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class LCMethodViewTests(ModelViewTests):
    def test_lc_method_detail(self):
        lc1 = LCMethod.objects.filter(name="polar-HILIC-25-min").get()
        response = self.client.get(reverse("lcmethod_detail", args=[lc1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/lcmethod/lcmethod_detail.html")
        self.assertEqual(
            lc1,
            response.context["object"],
        )

    def test_lc_method_detail_404(self):
        ms = LCMethod.objects.order_by("id").last()
        response = self.client.get(reverse("lcmethod_detail", args=[ms.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_lc_method_list(self):
        response = self.client.get(reverse("lcmethod_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/bst/list_view.html")
        self.assertTemplateUsed(response, "models/bst/th.html")
        self.assertTemplateUsed(response, "models/bst/td.html")
        self.assertTemplateUsed(response, "models/bst/value.html")
        self.assertEqual(len(response.context["object_list"]), 5)


# This runs the above tests again with auto-update diabled
LCMethodViewNullToleranceTests = create_null_tolerance_test_class(LCMethodViewTests)
