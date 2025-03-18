from django.core.management import call_command
from django.urls import reverse

from DataRepo.models import Protocol
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class ProtocolViewTests(TracebaseTestCase):
    """
    Test two list views for subsets of protocols
    Test detail views for protocols
    expected protocol for animal treatment: "no treatment"
    """

    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/dataframes/protocols.tsv",
        )

    def test_animal_treatment_list(self):
        response = self.client.get(reverse("animal_treatment_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "models/protocol/animal_treatments.html")
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
