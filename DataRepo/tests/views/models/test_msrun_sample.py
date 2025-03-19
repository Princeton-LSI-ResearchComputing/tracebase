from django.urls import reverse

from DataRepo.models import MSRunSample, MSRunSequence
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class MSRunSampleViewTests(ModelViewTests):
    def test_msrun_sample_list(self):
        response = self.client.get(reverse("msrunsample_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/msrunsample/msrunsample_list.html")
        self.assertEqual(
            len(response.context["msrun_samples"]), self.ALL_MSRUN_SAMPLES_COUNT
        )

    def test_msrun_sequence_list(self):
        response = self.client.get(reverse("msrunsequence_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(
            response, "models/msrunsequence/msrunsequence_list.html"
        )
        self.assertEqual(len(response.context["sequences"]), self.ALL_SEQUENCES_COUNT)

    def test_msrun_sample_detail(self):
        ms1 = MSRunSample.objects.filter(
            sample__name="BAT-xz971", ms_data_file__isnull=True
        ).get()
        response = self.client.get(reverse("msrunsample_detail", args=[ms1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/msrunsample/msrunsample_detail.html")
        self.assertEqual(response.context["msrun_sample"].sample.name, "BAT-xz971")

    def test_msrun_sample_detail_404(self):
        ms = MSRunSample.objects.order_by("id").last()
        response = self.client.get(reverse("msrunsample_detail", args=[ms.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_msrun_sequence_detail(self):
        ms1 = MSRunSequence.objects.filter(
            msrun_samples__sample__name="BAT-xz971",
            msrun_samples__ms_data_file__isnull=True,
        ).get()
        response = self.client.get(reverse("msrunsequence_detail", args=[ms1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(
            response, "models/msrunsequence/msrunsequence_detail.html"
        )
        self.assertEqual(
            self.ALL_MSRUN_SAMPLES_COUNT,
            response.context["sequence"].msrun_samples.count(),
        )


# This runs the above tests again with auto-update diabled
MSRunSampleViewNullToleranceTests = create_null_tolerance_test_class(
    MSRunSampleViewTests
)
