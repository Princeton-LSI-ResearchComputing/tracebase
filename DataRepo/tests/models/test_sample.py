from datetime import timedelta

from django.conf import settings
from django.core.management import call_command
from django.test import override_settings

from DataRepo.models import Animal, Sample
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
class SampleTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
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

    def test_is_serum_sample_autoupdates(self):
        """
        Issue #460, test 3.1.2.
        3. Updates of FCirc.is_last, Sample.is_serum_sample, and Animal.last_serum_sample are triggered by themselves
           and by changes to models down to PeakGroup.
          1. Create a new serum sample whose time collected is later than existing serum samples.
            2. Confirm the new sample's Sample.is_serum_sample is True.
        """
        # Get an animal (assuming it has an infusate/tracers/etc)
        animal = Animal.objects.last()
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

        # Assert that the new serum sample's is_serum_sample is autoupdated to True
        self.assertTrue(nlss.is_serum_sample)
