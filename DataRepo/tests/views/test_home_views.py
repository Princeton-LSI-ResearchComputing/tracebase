from django.core.management import call_command
from django.urls import reverse

from DataRepo.models import (
    Animal,
    Compound,
    PeakGroupSet,
    Protocol,
    Sample,
    Study,
    Tissue,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class HomeViewTests(TracebaseTestCase):
    """
    Test hoem views
    """

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/test_dataframes/loading.yaml")
        cls.ALL_TISSUES_COUNT = 37
        cls.ALL_COMPOUNDS_COUNT = 51
        cls.ALL_TRACERS_COUNT = 9
        cls.ALL_STUDIES_COUNT = 2
        cls.ALL_ANIMALS_COUNT = 4
        cls.ALL_SAMPLES_COUNT = 8
        cls.ALL_ANIMALTREATMENTS_COUNT = 8
        cls.ALL_MSRUN_PROTOCOLS_COUNT = 8
        cls.ALL_ACCUCOR_FILE_COUNT = 1

    def test_home_url_exists_at_desired_location(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)

    def test_home_url_accessible_by_name(self):
        response = self.client.get(reverse("home"))
        self.assertEqual(response.status_code, 200)

    def test_home_uses_correct_template(self):
        response = self.client.get(reverse("home"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "home.html")

    def test_home_card_attr_list(self):
        # spot check: counts, urls for card attributes
        study_count = Study.objects.all().count()
        animal_count = Animal.objects.all().count()
        tissue_count = Tissue.objects.all().count()
        sample_count = Sample.objects.all().count()
        accucor_file_count = PeakGroupSet.objects.all().count()
        compound_count = Compound.objects.all().count()
        tracer_count = (
            Animal.objects.exclude(infusate__tracers__compound__id__isnull=True)
            .order_by("infusate__tracers__compound__id")
            .values_list("infusate__tracers__compound__id")
            .distinct("infusate__tracers__compound__id")
            .count()
        )
        animal_treatment_count = Protocol.objects.filter(
            category=Protocol.ANIMAL_TREATMENT
        ).count()
        msrun_protocol_count = Protocol.objects.filter(
            category=Protocol.MSRUN_PROTOCOL
        ).count()

        self.assertEqual(study_count, self.ALL_STUDIES_COUNT)
        self.assertEqual(animal_count, self.ALL_ANIMALS_COUNT)
        self.assertEqual(tissue_count, self.ALL_TISSUES_COUNT)
        self.assertEqual(sample_count, self.ALL_SAMPLES_COUNT)
        self.assertEqual(accucor_file_count, self.ALL_ACCUCOR_FILE_COUNT)
        self.assertEqual(compound_count, self.ALL_COMPOUNDS_COUNT)
        self.assertEqual(tracer_count, self.ALL_TRACERS_COUNT)
        self.assertEqual(animal_treatment_count, self.ALL_ANIMALTREATMENTS_COUNT)
        self.assertEqual(msrun_protocol_count, self.ALL_MSRUN_PROTOCOLS_COUNT)

        # check url for each card
        study_url = reverse("study_list")
        animal_url = reverse("animal_list")
        tissue_url = reverse("tissue_list")
        sample_url = reverse("sample_list")
        compound_url = reverse("compound_list")
        accucor_file_url = reverse("peakgroupset_list")
        animal_treatment_url = reverse("animal_treatment_list")
        msrun_protocol_url = reverse("msrun_protocol_list")
        advance_search_url = reverse("search_advanced")
        response = self.client.get(reverse("home"))
        self.assertEqual(study_url, "/DataRepo/studies/")
        self.assertEqual(animal_url, "/DataRepo/animals/")
        self.assertEqual(tissue_url, "/DataRepo/tissues/")
        self.assertEqual(sample_url, "/DataRepo/samples/")
        self.assertEqual(accucor_file_url, "/DataRepo/peakgroupsets/")
        self.assertEqual(compound_url, "/DataRepo/compounds/")
        self.assertEqual(animal_treatment_url, "/DataRepo/protocols/animal_treatments/")
        self.assertEqual(msrun_protocol_url, "/DataRepo/protocols/msrun_protocols/")
        self.assertEqual(advance_search_url, "/DataRepo/search_advanced/")
        self.assertEqual(len(response.context["card_rows"]), 2)
