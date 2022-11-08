from datetime import timedelta

from django.conf import settings
from django.core.management import call_command
from django.test import override_settings, tag

from DataRepo.models import Animal, Infusate, Sample
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
@tag("animal")
class AnimalTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()
        infusate = Infusate()
        infusate.save()
        Animal.objects.create(
            name="test_animal",
            age=timedelta(weeks=int(13)),
            sex="M",
            genotype="WT",
            body_weight=200,
            diet="normal",
            feeding_status="fed",
            infusate=infusate,
        )

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
        )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_serum.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=False,
        )

        super().setUpTestData()

    def createNewLastSerumSample(self):
        # Get an animal (assuming it has an infusate/tracers/etc)
        animal = Animal.objects.filter(last_serum_sample__isnull=False).last()
        print(f"Animal: {animal.name} Last serum sample: {animal.last_serum_sample}")
        # Get its last serum sample
        lss = animal.last_serum_sample

        # For the validity of the test, assert there exist FCirc records and that they are for the last peak groups
        self.assertTrue(lss.fcircs.count() > 0)
        for fco in lss.fcircs.all():
            self.assertTrue(fco.is_last)

        # Now create a new last serum sample (without any peak groups)
        tissue = lss.tissue
        tc = lss.time_collected + timedelta(seconds=1)
        nlss = Sample.objects.create(animal=animal, tissue=tissue, time_collected=tc)

        self.animal = animal
        self.lss = lss
        self.newlss = nlss

    def test_animal_validation(self):
        animal = Animal.objects.get(name="test_animal")
        animal.full_clean()

    def test_animal_name(self):
        """Animal lookup by name"""
        animal = Animal.objects.get(name="test_animal")
        self.assertEqual(animal.name, "test_animal")

    def test_last_serum_sample_autoupdates(self):
        """
        Issue #460, test 3.1.3.
        3. Updates of FCirc.is_last, Sample.is_serum_sample, and Animal.last_serum_sample are triggered by themselves
           and by changes to models down to PeakGroup.
          1. Create a new serum sample whose time collected is later than existing serum samples.
            3. Confirm Animal.last_serum_sample points to the new sample.
        """
        self.createNewLastSerumSample()
        # Assert that the animal's last_serum_sample is autoupdated
        self.assertEqual(self.newlss, self.animal.last_serum_sample)

    def test_animal_is_queryable(self):
        """
        The query below needs some explanation.  It can produce an exception that references
        "Animal.last_serum_sample".  This is the association:

        This specific query exists inside DataRepo.models.researcher.Researcher.animals().  It was throwing an
        exception about Animal.last_serum_sample_id not existing in the database. The fix was to add db_column to the
        ForeignKey arguments.  This test assures that it is there by assuring that the query produces the expected
        result without an exception.

        The reason it happens is because every Sample links to Animal and Animal *can* link to the "last" serum sample.
        """
        ac = (
            Animal.objects.filter(samples__researcher="Xianfeng Zeng")
            .distinct()
            .count()
        )
        self.assertEqual(1, ac)
