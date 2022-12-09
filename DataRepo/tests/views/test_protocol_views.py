from django.core.management import call_command
from django.urls import reverse

from DataRepo.models import Protocol
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class ProtocolViewTests(TracebaseTestCase):
    """
    Test two list views for subsets of protocols
    Test detail views for protocols
    expected protocol for animal treatment: "no treatment"
    expected protocol for MSRun protocol: "Default"
    """

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/test_dataframes/loading.yaml")

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

    def test_msrun_protocol_list(self):
        response = self.client.get(reverse("msrun_protocol_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/msrun_protocols.html")
        self.assertEqual(len(response.context["msrun_protocol_list"]), 8)
        self.assertTrue(
            any(
                msrun_protocol.name == "Default"
                for msrun_protocol in response.context["msrun_protocol_list"]
            )
        )

    def test_protocol_detail(self):
        p1 = Protocol.objects.filter(name="Default").get()
        response = self.client.get(reverse("protocol_detail", args=[p1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/protocol_detail.html")
        self.assertEqual(response.context["protocol"].name, "Default")
        self.assertEqual(response.context["proto_display"], "MSRun Protocol")

    def test_protocol_detail_404(self):
        p = Protocol.objects.order_by("id").last()
        response = self.client.get(reverse("protocol_detail", args=[p.id + 1]))
        self.assertEqual(response.status_code, 404)
