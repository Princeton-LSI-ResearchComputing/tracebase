import time

from DataRepo.models import Compound, MaintainedModel, Tracer
from DataRepo.models.maintained_model import MaintainedModelCoordinator
from DataRepo.tests.tracebase_test_case import TracebaseTransactionTestCase
from DataRepo.tests.tracebase_thread_test import (
    ChildException,
    run_child_during_parent_thread,
    run_in_child_thread,
    run_parent_during_child_thread,
)


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
            raise Exception("Sanity check")

        def parent_func():
            return None

        with self.assertRaises(ChildException):
            run_parent_during_child_thread(parent_func, child_func)

    def create_tracer_in_disabled_coordinator_works_in_child_thread(self):
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
        self.assertEqual(
            "immediate", t1_trcr._get_current_coordinator().auto_update_mode
        )

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
            self.assertEqual("immediate", parent_coordinator.auto_update_mode)

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
            if "immediate" != child_coordinator.auto_update_mode:
                raise Exception(
                    "The child thread's default coordinator should be 'immediate', but it is "
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
