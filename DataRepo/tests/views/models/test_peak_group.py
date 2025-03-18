from django.urls import reverse

from DataRepo.models import MSRunSample, PeakGroup
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class PeakGroupViewTests(ModelViewTests):
    def test_peakgroup_list(self):
        response = self.client.get("/DataRepo/peakgroups/")
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/peakgroup/peakgroup_list.html")
        self.assertEqual(
            len(response.context["peakgroup_list"]),
            self.INF_PEAKGROUP_COUNT + self.SERUM_PEAKGROUP_COUNT,
        )

    def test_peakgroup_list_per_msrun_sample(self):
        ms1 = MSRunSample.objects.filter(
            sample__name="BAT-xz971", ms_data_file__isnull=True
        ).get()
        pg1 = PeakGroup.objects.filter(msrun_sample_id=ms1.id)
        response = self.client.get(
            "/DataRepo/peakgroups/?msrun_sample_id=" + str(ms1.pk)
        )
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/peakgroup/peakgroup_list.html")
        self.assertEqual(len(response.context["peakgroup_list"]), pg1.count())

    def test_peakgroup_detail(self):
        ms1 = MSRunSample.objects.filter(
            sample__name="BAT-xz971", ms_data_file__isnull=True
        ).get()
        pg1 = PeakGroup.objects.filter(msrun_sample_id=ms1.id).first()
        response = self.client.get(reverse("peakgroup_detail", args=[pg1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/peakgroup/peakgroup_detail.html")
        self.assertEqual(response.context["peakgroup"].name, pg1.name)

    def test_peakgroup_detail_404(self):
        pg = PeakGroup.objects.order_by("id").last()
        response = self.client.get(reverse("peakgroup_detail", args=[pg.id + 1]))
        self.assertEqual(response.status_code, 404)


# This runs the above tests again with auto-update diabled
PeakGroupViewNullToleranceTests = create_null_tolerance_test_class(PeakGroupViewTests)
