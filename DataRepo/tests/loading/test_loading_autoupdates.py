from django.core.management import call_command

from DataRepo.models import (
    Animal,
    FCirc,
    Infusate,
    Sample,
    Tracer,
    TracerLabel,
)
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class AutoupdateLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )

    def tearDown(self):
        MaintainedModel.clear_update_buffer()
        super().tearDown()

    def test_defer_autoupdates_animal_accucor(self):
        self.assert_no_names_to_start()
        self.assert_no_fcirc_data_to_start()

        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename="DataRepo/example_data/small_dataset/"
            "small_obob_animal_and_sample_table.xlsx",
            defer_autoupdates=True,
        )

        # Since autoupdates were defered (and we did not run perform_buffered_updates)
        self.assert_names_are_unupdated()
        bs1 = MaintainedModel.buffer_size()
        self.assertGreater(bs1, 0)
        first_buffered_model_object = MaintainedModel.data.update_buffer[0]

        self.assert_fcirc_data_is_unupdated()

        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_blank_sample.xlsx",
            skip_samples=("blank"),
            protocol="Default",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            defer_autoupdates=True,
        )

        # Since autoupdates were defered (and we did not run perform_buffered_updates)
        self.assert_fcirc_data_is_unupdated()
        # The buffer should have grown
        self.assertGreater(MaintainedModel.buffer_size(), bs1)
        # The first buffered object from the first load script should be the same.  I.e. Running a second load script
        # without clearing the buffer should just append to the buffer.
        self.assertEqual(
            first_buffered_model_object, MaintainedModel.data.update_buffer[0]
        )

    def test_defer_autoupdates_sample(self):
        self.assert_no_names_to_start()
        self.assert_no_fcirc_data_to_start()

        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
            defer_autoupdates=True,
        )

        # Since autoupdates were defered (and we did not run perform_buffered_updates)
        self.assert_names_are_unupdated()
        self.assert_fcirc_data_is_unupdated()

    def test_load_study_runs_autoupdates(self):
        self.assert_no_names_to_start()
        self.assert_no_fcirc_data_to_start()

        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_params.yaml",
        )

        self.assert_names_are_unupdated(False)
        self.assert_fcirc_data_is_unupdated(False)
        self.assertEqual(0, MaintainedModel.buffer_size())

    def assert_no_names_to_start(self):
        num_orig_infusates = Infusate.objects.count()
        self.assertEqual(0, num_orig_infusates)
        num_orig_tracers = Tracer.objects.count()
        self.assertEqual(0, num_orig_tracers)
        num_orig_tracerlabels = TracerLabel.objects.count()
        self.assertEqual(0, num_orig_tracerlabels)

    def assert_names_are_unupdated(self, unupdated=True):
        num_infusates = Infusate.objects.count()
        num_null_infusates = Infusate.objects.filter(name__isnull=unupdated).count()
        self.assertGreater(num_infusates, 0)
        self.assertEqual(num_infusates, num_null_infusates)

        num_tracers = Tracer.objects.count()
        num_null_tracers = Tracer.objects.filter(name__isnull=unupdated).count()
        self.assertGreater(num_tracers, 0)
        self.assertEqual(num_tracers, num_null_tracers)

        num_tracerlabels = TracerLabel.objects.count()
        num_null_tracerlabels = TracerLabel.objects.filter(
            name__isnull=unupdated
        ).count()
        self.assertGreater(num_tracerlabels, 0)
        self.assertEqual(num_tracerlabels, num_null_tracerlabels)

    def assert_no_fcirc_data_to_start(self):
        num_orig_animals = Animal.objects.count()
        self.assertEqual(0, num_orig_animals)
        num_orig_samples = Sample.objects.count()
        self.assertEqual(0, num_orig_samples)
        num_orig_fcircs = FCirc.objects.count()
        self.assertEqual(0, num_orig_fcircs)

    def assert_fcirc_data_is_unupdated(self, unupdated=True):
        num_animals = Animal.objects.count()
        num_null_animals = Animal.objects.filter(
            last_serum_sample__isnull=unupdated
        ).count()
        self.assertGreater(num_animals, 0)
        self.assertEqual(num_animals, num_null_animals)

        num_samples = Sample.objects.count()
        self.assertGreater(num_samples, 0)
        if unupdated:
            # Test every is_serum_sample is the default (i.e. False)
            num_default_samples = Sample.objects.filter(is_serum_sample=False).count()
            self.assertEqual(num_samples, num_default_samples)
        else:
            # Test some is_serum_sample are not the default (i.e. True)
            num_nondefault_samples = Sample.objects.filter(is_serum_sample=True).count()
            self.assertGreater(num_nondefault_samples, 0)

        num_fcircs = FCirc.objects.count()
        self.assertGreater(num_fcircs, 0)
        if unupdated:
            # Test every is_last is the default (i.e. False)
            num_default_fcircs = FCirc.objects.filter(is_last=False).count()
            self.assertEqual(num_fcircs, num_default_fcircs)
        else:
            # Test some is_last are not the default (i.e. True)
            num_nondefault_fcircs = FCirc.objects.filter(is_last=False).count()
            self.assertGreater(num_nondefault_fcircs, 0)
