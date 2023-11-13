from django.core.management import call_command

from DataRepo.models import (
    Animal,
    FCirc,
    Infusate,
    Sample,
    Tracer,
    TracerLabel,
)
from DataRepo.models.maintained_model import (
    MaintainedModel,
    MaintainedModelCoordinator,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class AutoupdateLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )

    def tearDown(self):
        MaintainedModel._reset_coordinators()
        super().tearDown()

    def assert_coordinator_state_is_initialized(
        self, msg="MaintainedModelCoordinators are in the default state."
    ):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        # Make sure there is only the default coordinator
        self.assertEqual(
            1, len(all_coordinators), msg=msg + "  The coordinator_stack is empty."
        )
        # Make sure that its mode is "immediate"
        self.assertEqual(
            "immediate",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode is 'immediate'.",
        )
        # Make sure that the buffer is empty to start
        for coordinator in all_coordinators:
            self.assertEqual(
                0, coordinator.buffer_size(), msg=msg + "  The buffer is empty."
            )

    def test_defer_autoupdates_animal_accucor(self):
        self.assert_no_names_to_start()
        self.assert_no_fcirc_data_to_start()

        self.assert_coordinator_state_is_initialized()
        # We need a parent coordinator to catch and test the buffered changes.  Otherwise, the deferred coordinator
        # would perform the mass auto-update
        parent_coordinator = MaintainedModelCoordinator(auto_update_mode="deferred")
        with MaintainedModel.custom_coordinator(parent_coordinator):
            current_coordinator = MaintainedModel._get_current_coordinator()
            # Make sure that the coordinator stack is populated
            self.assertEqual(parent_coordinator, current_coordinator)

            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename="DataRepo/example_data/small_dataset/"
                "small_obob_animal_and_sample_table.xlsx",
                # No longer need the defer_autoupdates option.  That is handled by a context manager.
                # defer_autoupdates=True,
            )

            # Assure that the load_animals_and_samples decorator's coordinator passed up it's buffer before exiting.
            bs1 = parent_coordinator.buffer_size()
            self.assertGreater(bs1, 0)
            # Record the first buffered object
            first_buffered_model_object = parent_coordinator._peek_update_buffer(0)

            # Since autoupdates were defered (and we did not run perform_buffered_updates)
            self.assert_names_are_unupdated()
            self.assert_fcirc_data_is_unupdated()

            child_coordinator = MaintainedModelCoordinator(auto_update_mode="deferred")
            with MaintainedModel.custom_coordinator(child_coordinator):
                call_command(
                    "load_accucor_msruns",
                    accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_blank_sample.xlsx",
                    skip_samples=("blank"),
                    protocol="Default",
                    date="2021-04-29",
                    researcher="Michael Neinast",
                    new_researcher=True,
                )

                # Since autoupdates were defered (and we did not run perform_buffered_updates)
                self.assert_fcirc_data_is_unupdated()
                # The buffer should have grown and been passed up to the parent coordinator
                self.assertGreater(child_coordinator.buffer_size(), bs1)

                # We don't want to actually perform a mass autoupdate when we leave this test context, so purge the
                # buffer.  This should not be necessary because the coordinator is popped off the stack automatically,
                # but it's a good test
                child_coordinator.clear_update_buffer()
                self.assertEqual(0, child_coordinator.buffer_size())

            # The first buffered object from the first load script should be the same.  I.e. Running a second load
            # script without clearing the buffer should just append to the buffer.
            self.assertEqual(
                first_buffered_model_object,
                parent_coordinator._peek_update_buffer(0),
            )

    def test_defer_autoupdates_sample(self):
        self.assert_no_names_to_start()
        self.assert_no_fcirc_data_to_start()

        self.assert_coordinator_state_is_initialized()
        # We need a parent coordinator to catch and test the buffered changes.  Otherwise, the deferred coordinator
        # would perform the mass auto-update
        parent_coordinator = MaintainedModelCoordinator(auto_update_mode="deferred")
        with MaintainedModel.custom_coordinator(parent_coordinator):
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
        self.assert_coordinator_state_is_initialized()
        self.assert_no_names_to_start()
        self.assert_no_fcirc_data_to_start()

        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_params.yaml",
        )

        self.assert_names_are_unupdated(False)
        self.assert_fcirc_data_is_unupdated(False)
        self.assert_coordinator_state_is_initialized()

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
