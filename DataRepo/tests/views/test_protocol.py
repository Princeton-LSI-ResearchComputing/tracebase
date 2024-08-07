from django.core.management import call_command
from django.urls import reverse

from DataRepo.models import Protocol
from DataRepo.models.maintained_model import (
    MaintainedModel,
    UncleanBufferError,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


def assert_coordinator_state_is_initialized():
    # Obtain all coordinators that exist
    all_coordinators = [MaintainedModel._get_default_coordinator()]
    all_coordinators.extend(MaintainedModel._get_coordinator_stack())
    if 1 != len(all_coordinators):
        raise ValueError(
            f"Before setting up test data, there are {len(all_coordinators)} MaintainedModelCoordinators."
        )
    if all_coordinators[0].auto_update_mode != "always":
        raise ValueError(
            "Before setting up test data, the default coordinator is not in always autoupdate mode."
        )
    if 0 != all_coordinators[0].buffer_size():
        raise UncleanBufferError()


class ProtocolViewTests(TracebaseTestCase):
    """
    Test two list views for subsets of protocols
    Test detail views for protocols
    expected protocol for animal treatment: "no treatment"
    """

    @classmethod
    def setUpTestData(cls):
        # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after itself
        assert_coordinator_state_is_initialized()

        call_command("loaddata", "lc_methods")
        call_command("legacy_load_study", "DataRepo/data/tests/dataframes/loading.yaml")

    def test_animal_treatment_list(self):
        response = self.client.get(reverse("animal_treatment_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/animal_treatments.html")
        self.assertEqual(len(response.context["animal_treatment_list"]), 8)
        self.assertTrue(
            any(
                treatment.name == "no treatment"
                for treatment in response.context["animal_treatment_list"]
            )
        )

    def test_protocol_detail_404(self):
        p = Protocol.objects.order_by("id").last()
        response = self.client.get(reverse("protocol_detail", args=[p.id + 1]))
        self.assertEqual(response.status_code, 404)
