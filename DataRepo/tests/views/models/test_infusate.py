from django.test import tag
from django.urls import reverse

from DataRepo.models import Infusate
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class InfusateViewTests(ModelViewTests):
    @tag("compound")
    def test_infusate_detail(self):
        infusate = Infusate.objects.filter(
            tracers__compound__name__icontains="lysine"
        ).first()
        response = self.client.get(reverse("infusate_detail", args=[infusate.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/infusate/infusate_detail.html")

    @tag("compound")
    def test_infusate_detail_404(self):
        inf = Infusate.objects.order_by("id").last()
        response = self.client.get(reverse("infusate_detail", args=[inf.id + 1]))
        self.assertEqual(response.status_code, 404)

    @tag("compound")
    def test_infusate_list(self):
        response = self.client.get(reverse("infusate_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/bst/list_view.html")
        self.assertTemplateUsed(response, "models/bst/th.html")
        self.assertTemplateUsed(response, "models/bst/td.html")
        self.assertTemplateUsed(response, "models/bst/value.html")
        self.assertTemplateUsed(response, "models/infusate/infusate_td.html")
        self.assertTemplateUsed(response, "models/infusate/infusate_value.html")
        self.assertEqual(len(response.context["object_list"]), 1)


# This runs the above tests again with auto-update diabled
InfusateViewNullToleranceTests = create_null_tolerance_test_class(InfusateViewTests)
