import time
from copy import deepcopy

from django.core.management import call_command
from django.test import tag

from DataRepo.models import (
    Animal,
    Compound,
    FCirc,
    Infusate,
    MaintainedModel,
    Sample,
    Study,
    Tracer,
    TracerLabel,
)
from DataRepo.models.maintained_model import (
    AutoUpdateFailed,
    MaintainedModelCoordinator,
    ModelNotMaintained,
)
from DataRepo.tests.models.test_infusate import create_infusate_records
from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    TracebaseTransactionTestCase,
)
from DataRepo.tests.tracebase_thread_test import (
    ChildException,
    run_child_during_parent_thread,
    run_in_child_thread,
    run_parent_during_child_thread,
)
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs


class MaintainedModelTestBase(TracebaseTestCase):
    """This base class is just for the assert_coordinator_state_is_initialized method."""

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
        # Make sure that its mode is "always"
        self.assertEqual(
            "always",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode should be 'always'.",
        )
        # Make sure that the buffer is empty to start
        for coordinator in all_coordinators:
            self.assertEqual(
                0, coordinator.buffer_size(), msg=msg + "  The buffer is empty."
            )


class MaintainedModelTests(MaintainedModelTestBase):
    """Tests that maintained fields are auto-updated"""

    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "lc_methods")
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_study_prerequisites.xlsx",
        )

    def tearDown(self):
        MaintainedModel._reset_coordinators()
        super().tearDown()

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

            Study.objects.create(name="Small OBOB")
            Infusate.objects.get_or_create_infusate(
                parse_infusate_name_with_concs("lysine-[13C6][23.2]")
            )

            call_command(
                "load_animals",
                infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_blank_sample.xlsx",
            )
            call_command(
                "load_samples",
                infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_blank_sample.xlsx",
            )

            # Assure that the loaders' decorator's coordinator passed up it's buffer to the parent_coordinator before
            # exiting.
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
                    "load_sequences",
                    infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_blank_sample.xlsx",
                )
                call_command(
                    "load_msruns",
                    infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_blank_sample.xlsx",
                    skip_mzxmls=True,
                )
                call_command(
                    "load_peak_annotation_files",
                    infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_blank_sample.xlsx",
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
                "load_study",
                infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table.xlsx",
                exclude_sheets=[
                    "Sequences",
                    "Peak Annotation Files",
                    "Peak Annotation Details",
                ],
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
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table.xlsx",
            exclude_sheets=[
                "Sequences",
                "Peak Annotation Files",
                "Peak Annotation Details",
            ],
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

    def test_get_my_update_labels(self):
        labels = Animal.get_my_update_labels()
        self.assertEqual(sorted(["fcirc_calcs", "label_combo", "tracer_stat"]), labels)

    def test_get_child_instances(self):
        # Load a study with animals and samples
        Study.objects.create(name="Small OBOB")
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )
        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_blank_sample.xlsx",
        )
        call_command(
            "load_samples",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_blank_sample.xlsx",
        )

        # Test get_child_instances with and without the label_filters and filter_in arguments
        animal: MaintainedModel = Animal.objects.first()
        samples = list(animal.samples.all())
        all_parents = animal.get_child_instances()
        self.assertEqual(samples, all_parents)
        name_parents = animal.get_child_instances(label_filters=["name"])
        self.assertEqual([], name_parents)
        non_name_parents = animal.get_child_instances(
            label_filters=["name"], filter_in=False
        )
        self.assertEqual(samples, non_name_parents)

    def test_get_parent_instances(self):
        # Load an infusate with supporting records, obtaining the expected records from the tests
        infusate1, infusate2 = create_infusate_records()
        tracer: MaintainedModel = infusate1.tracers.filter(
            compound__name="glucose"
        ).first()
        compound = tracer.compound

        # Test get_parent_instances with and without the label_filters and filter_in arguments
        all_parents = tracer.get_parent_instances()
        self.assertEqual(set([infusate1, infusate2, compound]), set(all_parents))
        name_parents = tracer.get_parent_instances(label_filters=["name"])
        self.assertEqual(set([infusate1, infusate2]), set(name_parents))
        non_name_parents = tracer.get_parent_instances(
            label_filters=["name", "label_combo"], filter_in=False
        )
        self.assertEqual(set([compound]), set(non_name_parents))


class MaintainedModelThreadTests(TracebaseTransactionTestCase):
    def create_tracer(self):
        cmpd = Compound.objects.create(name="glucose", formula="C6H12O6", hmdb_id="1")
        return Tracer.objects.create(compound=cmpd)

    def test_TestThreadRunner_raises_exceptions_from_child(self):
        """
        This is a meta-test sanity check to ensure that exceptions in the child are raised in the parent as a
        ChildException
        """

        def child_func():
            time.sleep(0.2)
            raise ValueError("Sanity check")

        def parent_func():
            return None

        with self.assertRaises(ChildException):
            run_parent_during_child_thread(parent_func, child_func)

    def test_create_tracer_in_disabled_coordinator_works_in_child_thread(self):
        def child_func():
            disabled = MaintainedModelCoordinator("disabled")
            with MaintainedModel.custom_coordinator(disabled):
                cmpd = Compound.objects.create(
                    name="lysine", formula="C6H14N2O2", hmdb_id="2"
                )
                _ = Tracer.objects.create(compound=cmpd)
                # Make sure the child runs long enough for any exceptions it raises to be collected by the parent
                time.sleep(0.25)

        # Just test that there are no exceptions
        run_in_child_thread(child_func)

    def test_failure_when_coordinator_access_shortcut_taken(self):
        """
        This tests the negative case.  This proves that thread-local data is not setup if you circumvent the accessors.
        """
        with self.assertRaises(AttributeError) as ar:
            t1_trcr = self.create_tracer()
            _ = t1_trcr.default_coordinator
        # Assert the exception mentions the default coordinator
        self.assertIn("default_coordinator", str(ar.exception))

    def test_failure_when_coordinator_access_shortcut_taken_in_thread(self):
        """
        This tests the negative case.  This proves that thread-local data is not setup if you circumvent the accessors.
        """

        def child_func():
            t2_trcr = self.create_tracer()
            _ = t2_trcr.default_coordinator

        with self.assertRaises(ChildException) as ar:
            run_in_child_thread(child_func)
        # Assert the exception mentions the default coordinator
        self.assertIn("default_coordinator", str(ar.exception))

    def test_success_when_coordinator_accessed_via_getter(self):
        """
        This tests the main thread case.  This proves that thread-local data is correctly set up in the main thread.
        """
        t1_trcr = self.create_tracer()
        _ = t1_trcr._get_default_coordinator()

    def test_success_when_coordinator_accessed_via_getter_in_thread(self):
        """
        This tests an alternate thread case.  This proves that thread-local data is correctly set up in new threads.
        """

        def child_func():
            t2_trcr = self.create_tracer()
            _ = t2_trcr._get_default_coordinator()
            # Allow time to let the parent be assured there was no exception
            time.sleep(0.2)

        # Just test that there are no exceptions
        run_in_child_thread(child_func)

    def test_coordinator_stack(self):
        """
        This tests the main thread case.  This proves that thread-local data is correctly set up in the main thread.
        """
        disabled = MaintainedModelCoordinator("disabled")
        with MaintainedModel.custom_coordinator(disabled):
            t1_trcr = self.create_tracer()
            self.assertEqual(
                "disabled", t1_trcr._get_current_coordinator().auto_update_mode
            )
        self.assertEqual("always", t1_trcr._get_current_coordinator().auto_update_mode)

    def test_parent_thread_coordinator_unaffected_by_custom_child_coordinator(self):
        def child_func():
            """
            Child thread puts a disabled coordinator on the coordinator stack
            """
            disabled = MaintainedModelCoordinator("disabled")
            with MaintainedModel.custom_coordinator(disabled):
                # Sleep to give parent time to check its auto-update mode
                time.sleep(0.25)

        def parent_func():
            """
            Assert that the current coordinator is still the default when the child adds a disabled coordinator to its
            own coordinator_stack
            """
            parent_coordinator = MaintainedModel._get_current_coordinator()
            self.assertEqual("always", parent_coordinator.auto_update_mode)

        run_parent_during_child_thread(parent_func, child_func)

    def test_child_thread_coordinator_unaffected_by_custom_parent_coordinator(self):
        def child_func():
            """
            The child thread will sleep a bit, then obtain the current coordinator (run during a time when the parent
            has added a disabled coordinator to the coordinator stack)
            """
            # Sleep 0.1 seconds so that the parent's coordinator context is active
            time.sleep(0.1)
            child_coordinator = MaintainedModel._get_current_coordinator()
            # Wasn't sure if I could use self.assertEqual()
            if "always" != child_coordinator.auto_update_mode:
                raise ValueError(
                    "The child thread's default coordinator should be 'always', but it is "
                    f"{child_coordinator.auto_update_mode}"
                )
            # Allow time to let the parent be assured there was no exception
            time.sleep(0.4)

        def parent_func():
            """
            The parent thread will run  after the child starts, but the child will wait a bit, so that the parent, run
            after, will establish a new context, so we can check if the child's context has changed.
            """
            disabled = MaintainedModelCoordinator("disabled")
            with MaintainedModel.custom_coordinator(disabled):
                # Sleep to give parent time to check its auto-update mode
                time.sleep(0.2)

        run_child_during_parent_thread(parent_func, child_func)


@tag("load_study")
class MaintainedModelDeferredTests(TracebaseTestCase):
    def setUp(self):
        # Load data and buffer autoupdates before each test
        super().setUp()
        # Reset the coordinators at the start of each test
        MaintainedModel._reset_coordinators()
        # Create a parent deferred coordinator to catch all buffered updates without performing the mass updates
        # We're saving this in `self` so that we can query it in the tests.
        self.test_coordinator = MaintainedModelCoordinator("deferred")
        # Add the parent coordinator manually, so that we can catch the contents of the buffered items
        MaintainedModel._add_coordinator(self.test_coordinator)
        # Now create a temporary deferred coordinator.  Since a deferred parent coordinator exists, it will not perform
        # a mass autoupdate on the buffered contents. Instead, it will pass its buffer contents up to the parent
        # coordinator
        tmp_coordinator = MaintainedModelCoordinator("deferred")
        with MaintainedModel.custom_coordinator(tmp_coordinator):
            # This should not mass auto-update and pass its buffer contents up to the test_coordinator
            create_infusate_records()

    def tearDown(self):
        MaintainedModel._reset_coordinators()
        super().tearDown()

    @classmethod
    def setUpTestData(cls):
        # Load compounds, tissues, and protocol data before any of the tests run
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_study_prerequisites.xlsx",
        )
        super().setUpTestData()

    def test_nullable(self):
        """
        MaintainedModel doesn't require the update_field be nullable, but it helps to make the
        test_disabled_autoupdates test straightforward
        """
        self.assertTrue(TracerLabel._meta.get_field("name").null)
        self.assertTrue(Tracer._meta.get_field("name").null)
        self.assertTrue(Infusate._meta.get_field("name").null)

    def test_disabled_autoupdates(self):
        """
        Makes sure name is not autoupdated in TracerLabel, Tracer, nor Infusate
        """
        self.assertIsNone(TracerLabel.objects.first().name)
        self.assertIsNone(Tracer.objects.first().name)
        self.assertIsNone(Infusate.objects.first().name)

    def test_mass_autoupdate(self):
        """
        Ensures that the name fields were all updated, updated correctly, and the buffer emptied.
        """
        # perform the updates that are saved in the test buffer
        self.test_coordinator.perform_buffered_updates()
        # Ensure all the auto-updated fields not have values (correctness of values tested elsewhere)
        for tl in TracerLabel.objects.all():
            self.assertIsNotNone(tl.name)
            self.assertEqual(tl.name, tl._name())
        for t in Tracer.objects.all():
            self.assertIsNotNone(t.name)
            self.assertEqual(t.name, t._name())
        for i in Infusate.objects.all():
            self.assertIsNotNone(i.name)
            self.assertEqual(i.name, i._name())
        # Ensure the buffer was emptied by perform_buffered_updates
        self.assertEqual(self.test_coordinator.buffer_size(), 0)

    def test_lazy_autoupdate_blocked_in_deferred_mode(self):
        """
        Since a parent coordinator is deferred, auto-update should not happen.
        """
        io, _ = create_infusate_records()
        with MaintainedModel.custom_coordinator(MaintainedModelCoordinator("lazy")):
            # Since a parent coordinator is set to deferred in setUp(), auto-update should not happen, but _name()
            # always returns a value.  Test this to ensure the results of the following test will be valid.
            expected_name = "ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}"
            self.assertEqual(expected_name, io._name())

            # Assert lazy autoupdate does not auto-update if parent coordinator is deferred. This should call the lazy-
            # autoupdate code in the from_db override
            io_again = Infusate.objects.get(id__exact=io.id)
            self.assertIsNone(io_again.name)

    def test_pretty_name_deferred(self):
        io, _ = create_infusate_records()
        # The expected name should always be populated because it's dynamically populated
        expected_name = (
            "ti {\nC16:0-[5,6-13C2,17O2][2];\nglucose-[2,3-13C2,4-17O1][1]\n}"
        )
        self.assertEqual(expected_name, io.pretty_name)
        # Assert method pretty_name does not auto-update if parent coordinator is deferred
        io_again = Infusate.objects.get(id__exact=io.id)
        self.assertIsNone(io_again.name)

    def test_lazy_autoupdate_deferred_immediate(self):
        """
        Since a parent coordinator is deferred, auto-update should not happen, even if child coordinator is "immediate".
        """
        # Create a new "immediate" mode coordinator
        tmp_coordinator = MaintainedModelCoordinator("immediate")
        with MaintainedModel.custom_coordinator(tmp_coordinator):
            io, _ = create_infusate_records()
            # Since a parent coordinator is deferred, auto-update should not happen, but _name() always returns a
            # value.  This test ensures the next test is meaningful.
            expected_name = "ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}"
            self.assertEqual(expected_name, io._name())

            # Assert lazy-autoupdate does not auto-update if parent coordinator is deferred
            io_again = Infusate.objects.get(id__exact=io.id)
            self.assertIsNone(io_again.name)

    def test_deferred_sets_label_filters_in_buffered_objects_when_no_default(self):
        tmp_coordinator = MaintainedModelCoordinator("deferred")
        with MaintainedModel.custom_coordinator(tmp_coordinator):
            create_infusate_records()

            io: MaintainedModel = tmp_coordinator._peek_update_buffer(0)
            self.assertIsInstance(io, Tracer)
            self.assertEqual(
                sorted(["label_combo", "name", "tracer_stat"]), io.label_filters
            )
            self.assertTrue(io.filter_in)

            io2: MaintainedModel = tmp_coordinator._peek_update_buffer(1)
            self.assertIsInstance(io2, TracerLabel)
            self.assertEqual(sorted(["name"]), io2.label_filters)
            self.assertTrue(io2.filter_in)

    def test_deferred_sets_default_label_filters_in_buffered_objects(self):
        """This asserts that the only autoupdates that are buffered are those that have the label_filters that were
        set.
        """
        tmp_coordinator = MaintainedModelCoordinator("deferred")
        tmp_coordinator.default_label_filters = ["name"]
        with MaintainedModel.custom_coordinator(tmp_coordinator):
            create_infusate_records()

            buffered_item: MaintainedModel
            for buffered_item in tmp_coordinator.update_buffer:
                self.assertEqual(["name"], buffered_item.label_filters)
                self.assertTrue(buffered_item.filter_in)

    def test_deferred_only_buffers_matches(self):
        """This asserts that the only autoupdates that are buffered are those that have the label_filters that were
        set.
        """
        tmp_coordinator = MaintainedModelCoordinator("deferred")
        tmp_coordinator.default_label_filters = ["irrelevant"]
        with MaintainedModel.custom_coordinator(tmp_coordinator):
            create_infusate_records()
            self.assertEqual(0, len(tmp_coordinator.update_buffer))

    def test_buffer_update_buffers_when_label_filters_differ(self):
        """This asserts that a duplicate object with different label_filters gets buffered, so that both autoupdates are
        performed, as specified in the label_filters attribute.
        """
        tmp_coordinator = MaintainedModelCoordinator("deferred")
        tmp_coordinator.default_label_filters = ["tracer_stat"]
        with MaintainedModel.custom_coordinator(tmp_coordinator):
            # The first thing this buffers is a Tracer object
            create_infusate_records()
            buffered_tracer: MaintainedModel = tmp_coordinator._peek_update_buffer(0)

            tracer_copy = deepcopy(buffered_tracer)
            # This should cause the duplicate to buffer, because it performs other autoupdates.
            tracer_copy.label_filters = ["name"]

            # This won't buffer a duplicate object, but only when the label_filters are the same
            tmp_coordinator.buffer_update(tracer_copy)
            # The last buffered object should be the one we explicitly buffered
            buffered_tracer_2 = tmp_coordinator._peek_update_buffer(-1)

            self.assertEqual(buffered_tracer, buffered_tracer_2)

    def test_delete_updates_label_filters(self):
        """Assert that buffered parent instances have their label_filters set based on their decorators"""
        infusate, _ = create_infusate_records()
        tracer: MaintainedModel = infusate.tracers.first()
        tmp_coordinator = MaintainedModelCoordinator("deferred")
        with MaintainedModel.custom_coordinator(tmp_coordinator):
            tracer.delete()
            infusate: MaintainedModel = tmp_coordinator._peek_update_buffer(0)
            self.assertEqual(
                ["label_combo", "name", "tracer_stat"], infusate.label_filters
            )


@tag("load_study")
class MaintainedModelImmediateTests(MaintainedModelTestBase):
    def setUp(self):
        # Load data and buffer autoupdates before each test
        super().setUp()
        # Reset the coordinators at the start of each test
        MaintainedModel._reset_coordinators()

    def tearDown(self):
        MaintainedModel._reset_coordinators()
        super().tearDown()

    @classmethod
    def setUpTestData(cls):
        # Load data before any of the tests run
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_study_prerequisites.xlsx",
        )
        super().setUpTestData()

    def test_infusate_name_lazy_autoupdate(self):
        """
        Disable auto-updates to ensure the name field in the infusate model will be None.
        """
        with MaintainedModel.custom_coordinator(MaintainedModelCoordinator("disabled")):
            io, _ = create_infusate_records()

        # Should be initially none
        self.assertIsNone(io.name)

        with MaintainedModel.custom_coordinator(MaintainedModelCoordinator("lazy")):
            # This triggers a lazy autoupdate
            io_via_query = Infusate.objects.get(id__exact=io.id)

        expected_name = "ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}"
        # And now the field should be updated
        self.assertEqual(expected_name, io_via_query.name)

    def test_infusate_name_immediate_lazy_autoupdate(self):
        """
        Lazy auto-updates should not update upon save
        """
        with MaintainedModel.custom_coordinator(MaintainedModelCoordinator("lazy")):
            io, _ = create_infusate_records()
        # Should be none, because save shouldn't update - only from_db
        self.assertIsNone(io.name)
        # Would have been "ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}" in immediate mode

    def test_enable_autoupdates(self):
        """
        Ensures that the name field was constructed.
        """
        lys = Compound.objects.create(name="lysine2", formula="C6H14N2O2", hmdb_id=3)
        Tracer.objects.create(compound=lys)
        Tracer.objects.get(name="lysine2")

    def test_buffer_cleared_after_sample_load(self):
        """Ensure the sample load doesn't leave stuff in the buffer when it exits successfully"""
        # The sample load only auto-updates fields with the "name" label in the decorator, so the sample_table_loader
        # must clear the buffer when it ends successfully
        Study.objects.create(name="Small OBOB")
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )
        call_command(
            "load_animals",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_samples",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        self.assert_coordinator_state_is_initialized()

    def test_error_when_buffer_not_clear(self):
        """Ensure that stale buffer contents before a load produces a helpful error"""
        with self.assertRaisesRegex(AutoUpdateFailed, ".+clear_update_buffer.+"):
            # Create infusate records while auto updates are disabled, so that they buffer
            tmp_coordinator = MaintainedModelCoordinator("deferred")
            with MaintainedModel.custom_coordinator(tmp_coordinator):
                infusate1, infusate2 = create_infusate_records()
                # Delete the records and do not clear the buffer
                infusate1.delete()
                infusate2.delete()
                # When we leave this context, autoupdates will be attempted to be performed by the temporary coordinator


class RebuildMaintainedModelFieldsTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()
        # Do not perform nor buffer autoupdates (disable autoupdates).
        # This creates records with maintained fields that are empty.
        disabled_coordinator = MaintainedModelCoordinator("disabled")
        with MaintainedModel.custom_coordinator(disabled_coordinator):
            create_infusate_records()

    def test_rebuild_maintained_fields(self):
        # Perform all updates of all maintained fields in every record
        MaintainedModel.rebuild_maintained_fields()
        # Ensure all the auto-updated fields not have values (correctness of values tested elsewhere)
        for i in Infusate.objects.all():
            self.assertIsNotNone(i.name)
            self.assertEqual(i.name, i._name())
        for t in Tracer.objects.all():
            self.assertIsNotNone(t.name)
            self.assertEqual(t.name, t._name())
        for tl in TracerLabel.objects.all():
            self.assertIsNotNone(tl.name)
            self.assertEqual(tl.name, tl._name())
        coordinator = MaintainedModel._get_current_coordinator()
        # Ensure the buffer was emptied by rebuild_maintained_fields
        self.assertEqual(coordinator.buffer_size(), 0)


class MaintainedModelMainTests(TracebaseTestCase):
    def test_ModelNotMaintained(self):
        mnm = ModelNotMaintained(Compound)
        self.assertIn(
            "Model class 'Compound' must inherit from MaintainedModel.", str(mnm)
        )
