from django.test import tag
from django.urls import reverse

from DataRepo.models import Study
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class StudyViewTests(ModelViewTests):
    @tag("study")
    def test_study_list(self):
        response = self.client.get(reverse("study_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/bst/list_view.html")
        self.assertTemplateUsed(response, "models/bst/th.html")
        self.assertTemplateUsed(response, "models/bst/td.html")
        self.assertTemplateUsed(response, "models/bst/value.html")
        self.assertTemplateUsed(response, "models/bst/value_list.html")
        self.assertTemplateUsed(response, "models/study/infusates_td.html")
        self.assertTemplateUsed(response, "models/study/infusates_value_list.html")
        self.assertEqual(len(response.context["object_list"]), self.ALL_ANIMALS_COUNT)
        self.assertTemplateUsed(response, "models/study/below_table.html")
        self.assertEqual(len(response.context["object_list"]), 1)

    @tag("study")
    def test_study_summary(self):
        response = self.client.get("/DataRepo/studies/study_summary/")
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/study/study_summary.html")

    @tag("study")
    def test_study_detail(self):
        obob_fasted = Study.objects.filter(name="Small OBOB").get()
        response = self.client.get(reverse("study_detail", args=[obob_fasted.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/study/study_detail.html")
        self.assertEqual(response.context["study"].name, "Small OBOB")
        self.assertEqual(len(response.context["stats_df"]), 1)

    @tag("study")
    def test_study_detail_404(self):
        s = Study.objects.order_by("id").last()
        response = self.client.get(reverse("study_detail", args=[s.id + 1]))
        self.assertEqual(response.status_code, 404)


# This runs the above tests again with auto-update diabled
StudyViewNullToleranceTests = create_null_tolerance_test_class(StudyViewTests)
