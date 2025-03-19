from django.urls import reverse

from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class UrlTests(TracebaseTestCase):
    """
    Test Urls
    """

    def test_root(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)

    def test_home(self):
        response = self.client.get(reverse("home"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "home/base.html")

    def test_list_urls(self):
        self.assertEqual("/DataRepo/studies/", reverse("study_list"))
        self.assertEqual("/DataRepo/animals/", reverse("animal_list"))
        self.assertEqual("/DataRepo/tissues/", reverse("tissue_list"))
        self.assertEqual("/DataRepo/samples/", reverse("sample_list"))
        self.assertEqual("/DataRepo/compounds/", reverse("compound_list"))
        self.assertEqual(
            "/DataRepo/protocols/animal_treatments/", reverse("animal_treatment_list")
        )
        self.assertEqual("/DataRepo/search_advanced/", reverse("search_advanced"))
