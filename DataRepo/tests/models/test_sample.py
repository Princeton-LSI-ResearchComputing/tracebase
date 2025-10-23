from datetime import timedelta

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.test import override_settings

from DataRepo.models import Animal, Sample
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
class SampleTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_no_newsample.xlsx",
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

    def test_is_a_blank(self):
        self.assertTrue(Sample.is_a_blank("a Blank sample"))
        self.assertFalse(Sample.is_a_blank("sample1"))

    def test_clean_time_collected(self):
        sample = Sample.objects.first()
        # test time_collected exceeding MAXIMUM_VALID_TIME_COLLECTED fails
        with self.assertRaises(ValidationError):
            sample.time_collected = timedelta(days=91)
            # validation errors are raised upon cleaning
            sample.full_clean()
        # test time_collected exceeding MINIMUM_VALID_TIME_COLLECTED fails
        with self.assertRaises(ValidationError):
            sample.time_collected = timedelta(minutes=-2000)
            sample.full_clean()

    def test_is_a_blank_none(self):
        self.assertFalse(Sample.is_a_blank(None))
