from django.urls import reverse

from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class HomeTemplateTests(TracebaseTestCase):
    """
    Test that home templates get the right info and that views use the correct templates
    """

    def test_home_card_attr_list(self):
        response = self.client.get(reverse("home"))
        self.assertEqual(len(response.context["card_grid"]), 3)
