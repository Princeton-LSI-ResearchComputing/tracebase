from django.urls import reverse

from DataRepo.models import MSRunSequence
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class MSRunSequenceViewTests(ModelViewTests):
    def test_msrun_sequence_detail(self):
        ms1 = MSRunSequence.objects.filter(
            msrun_samples__sample__name="BAT-xz971",
            msrun_samples__ms_data_file__isnull=True,
        ).get()
        response = self.client.get(reverse("msrunsequence_detail", args=[ms1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/msrunsequence_detail.html")
        self.assertEqual(
            self.ALL_MSRUN_SAMPLES_COUNT,
            response.context["sequence"].msrun_samples.count(),
        )

    def test_msrun_sequence_detail_404(self):
        ms = MSRunSequence.objects.order_by("id").last()
        response = self.client.get(reverse("msrunsequence_detail", args=[ms.id + 1]))
        self.assertEqual(response.status_code, 404)


# This runs the above tests again with auto-update diabled
MSRunSequenceViewNullToleranceTests = create_null_tolerance_test_class(
    MSRunSequenceViewTests
)
