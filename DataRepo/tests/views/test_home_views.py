from django.core.management import call_command
from django.urls import reverse

from DataRepo.models import (
    Animal,
    ArchiveFile,
    Compound,
    Protocol,
    Sample,
    Study,
    Tissue,
)
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
    if all_coordinators[0].auto_update_mode != "immediate":
        raise ValueError(
            "Before setting up test data, the default coordinator is not in immediate autoupdate mode."
        )
    if 0 != all_coordinators[0].buffer_size():
        raise UncleanBufferError()


class HomeViewTests(TracebaseTestCase):
    """
    Test home views
    """

    @classmethod
    def setUpTestData(cls):
        # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after itself
        assert_coordinator_state_is_initialized()

        call_command("loaddata", "lc_methods")
        call_command(
            "load_study",
            "DataRepo/data/tests/dataframes/loading.yaml",
            verbosity=6,
        )
        cls.ALL_TISSUES_COUNT = 37
        cls.ALL_COMPOUNDS_COUNT = 51
        cls.ALL_TRACERS_COUNT = 9
        cls.ALL_STUDIES_COUNT = 2
        cls.ALL_ANIMALS_COUNT = 4
        cls.ALL_SAMPLES_COUNT = 8
        cls.ALL_ANIMALTREATMENTS_COUNT = 8
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
        peak_annotation_file_count = ArchiveFile.objects.filter(
            data_type__code="ms_peak_annotation"
        ).count()
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

        self.assertEqual(study_count, self.ALL_STUDIES_COUNT)
        self.assertEqual(animal_count, self.ALL_ANIMALS_COUNT)
        self.assertEqual(tissue_count, self.ALL_TISSUES_COUNT)
        self.assertEqual(sample_count, self.ALL_SAMPLES_COUNT)
        self.assertEqual(peak_annotation_file_count, self.ALL_ACCUCOR_FILE_COUNT)
        self.assertEqual(compound_count, self.ALL_COMPOUNDS_COUNT)
        self.assertEqual(tracer_count, self.ALL_TRACERS_COUNT)
        self.assertEqual(animal_treatment_count, self.ALL_ANIMALTREATMENTS_COUNT)

        # check url for each card
        study_url = reverse("study_list")
        animal_url = reverse("animal_list")
        tissue_url = reverse("tissue_list")
        sample_url = reverse("sample_list")
        compound_url = reverse("compound_list")
        accucor_file_url = reverse("peakgroupset_list")
        animal_treatment_url = reverse("animal_treatment_list")
        advance_search_url = reverse("search_advanced")
        response = self.client.get(reverse("home"))
        self.assertEqual(study_url, "/DataRepo/studies/")
        self.assertEqual(animal_url, "/DataRepo/animals/")
        self.assertEqual(tissue_url, "/DataRepo/tissues/")
        self.assertEqual(sample_url, "/DataRepo/samples/")
        self.assertEqual(accucor_file_url, "/DataRepo/peakgroupsets/")
        self.assertEqual(compound_url, "/DataRepo/compounds/")
        self.assertEqual(animal_treatment_url, "/DataRepo/protocols/animal_treatments/")
        self.assertEqual(advance_search_url, "/DataRepo/search_advanced/")
        self.assertEqual(len(response.context["card_grid"]), 3)
