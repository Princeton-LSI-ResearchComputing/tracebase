from django.core.management import call_command
from django.test import tag

from DataRepo.models.compound import Compound
from DataRepo.models.infusate import Infusate
from DataRepo.models.infusate_tracer import InfusateTracer
from DataRepo.models.maintained_model import (
    AutoUpdateFailed,
    MaintainedFieldNotSettable,
    MaintainedModel,
    MaintainedModelCoordinator,
)
from DataRepo.models.tracer import Tracer
from DataRepo.models.tracer_label import TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


def create_infusate_records():
    (glu, _) = Compound.objects.get_or_create(
        name="glucose", formula="C6H12O6", hmdb_id="HMDB0000122"
    )
    (c16, _) = Compound.objects.get_or_create(
        name="C16:0", formula="C16H32O2", hmdb_id="HMDB0000220"
    )
    glu_t = Tracer.objects.create(compound=glu)
    c16_t = Tracer.objects.create(compound=c16)
    TracerLabel.objects.create(
        tracer=glu_t, count=2, element="C", positions=[2, 3], mass_number=13
    )
    TracerLabel.objects.create(
        tracer=glu_t, count=1, element="O", positions=[4], mass_number=17
    )
    TracerLabel.objects.create(
        tracer=c16_t, count=2, element="C", positions=[5, 6], mass_number=13
    )
    TracerLabel.objects.create(tracer=c16_t, count=2, element="O", mass_number=17)
    io = Infusate.objects.create(tracer_group_name="ti")
    InfusateTracer.objects.create(infusate=io, tracer=glu_t, concentration=1.0)
    InfusateTracer.objects.create(infusate=io, tracer=c16_t, concentration=2.0)
    io2 = Infusate.objects.create()
    InfusateTracer.objects.create(infusate=io2, tracer=glu_t, concentration=3.0)
    InfusateTracer.objects.create(infusate=io2, tracer=c16_t, concentration=4.0)

    return io, io2


@tag("load_study")
class InfusateTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()
        MaintainedModel._reset_coordinators()
        self.INFUSATE1, self.INFUSATE2 = create_infusate_records()

    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            "DataRepo/data/tests/tissues/loading.yaml",
            verbosity=2,
        )
        call_command(
            "load_compounds",
            compounds="DataRepo/data/tests/compounds/consolidated_tracebase_compound_list.tsv",
            verbosity=2,
        )
        super().setUpTestData()

    def test_infusate_record(self):
        infusate = Infusate.objects.first()
        infusate.full_clean()

    def test_infusate_name_method(self):
        self.assertEqual(
            "ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}",
            self.INFUSATE1._name(),
        )
        self.assertEqual(
            "C16:0-[5,6-13C2,17O2][4];glucose-[2,3-13C2,4-17O1][3]",
            self.INFUSATE2._name(),
        )

    def test_name_not_settable(self):
        with self.assertRaises(MaintainedFieldNotSettable):
            Infusate.objects.create(
                name="test infusate",
                tracer_group_name="ti2",
            )

    def test_name_autoupdated(self):
        """
        Make sure that the name field was set automatically - triggered by the InfusateTracer record creation.
        """
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(
            name="ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}"
        )

    def test_name_none(self):
        """
        Make sure that the name field was set automatically - triggered by the InfusateTracer record creation.
        """
        self.assertEqual(
            "C16:0-[5,6-13C2,17O2][4];glucose-[2,3-13C2,4-17O1][3]",
            self.INFUSATE2.name,
        )
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(
            name__exact="C16:0-[5,6-13C2,17O2][4];glucose-[2,3-13C2,4-17O1][3]"
        )

    def test_name_self_autoupdated(self):
        """
        Make sure that the name field was set automatically - triggered by the Infusate record creation.
        """
        ti3 = Infusate.objects.create(tracer_group_name="ti3")
        self.assertEqual("ti3", ti3.name)
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(name="ti3")

    def test_delete_autoupdate(self):
        """
        Make sure parent records are updated when a child record is deleted
        """
        tl = TracerLabel.objects.get(name="2,3-13C2")
        tl.delete()
        # get fresh objects
        i1 = Infusate.objects.get(id__exact=self.INFUSATE1.id)
        i2 = Infusate.objects.get(id__exact=self.INFUSATE2.id)
        # The deletion affects the tracer name (which should have been autoupdated)
        self.assertEqual("glucose-[4-17O1]", tl.tracer.name)
        # The deletion also affects the names of both infusates that had that tracer
        self.assertEqual("ti {C16:0-[5,6-13C2,17O2][2];glucose-[4-17O1][1]}", i1.name)
        self.assertEqual("C16:0-[5,6-13C2,17O2][4];glucose-[4-17O1][3]", i2.name)


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
            "DataRepo/data/tests/small_obob/small_obob_study_prerequisites.yaml",
            verbosity=2,
        )
        super().setUpTestData()

    def assert_coordinator_state_is_initialized(
        self,
        msg="MaintainedModelCoordinators are in the expected test start state.",
    ):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        # Make sure there is only the default coordinator
        self.assertEqual(
            2,
            len(all_coordinators),
            msg=msg + "  The coordinator_stack has the test coordinator.",
        )
        # Make sure that its mode is "immediate"
        self.assertEqual(
            "immediate",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode is 'immediate'.",
        )
        # Make sure the test coordinator's mode is "deferred"
        self.assertEqual(
            "deferred",
            all_coordinators[1].auto_update_mode,
            msg=msg + "  Mode is 'immediate'.",
        )
        # Make sure that the buffer is empty to start
        self.assertEqual(
            0,
            all_coordinators[0].buffer_size(),
            msg=msg + "  The default coordinator buffer is empty.",
        )
        self.assertGreater(
            all_coordinators[1].buffer_size(),
            0,
            msg=msg + "  The test coordinator buffer is populated.",
        )

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

    def test_get_name_orig_deferred(self):
        """
        Since a parent coordinator is deferred, auto-update should not happen.
        """
        io, _ = create_infusate_records()
        # Since a parent coordinator is deferred, auto-update should not happen, but get_name always returns a value.
        # See MaintainedModelImmediateTests.test_get_name_orig_updated
        expected_name = "ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}"
        self.assertEqual(expected_name, io.get_name)
        # Assert method get_name does not auto-update if parent coordinator is deferred
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

    def test_get_name_orig_deferred_immediate(self):
        """
        Since a parent coordinator is deferred, auto-update should not happen, even if child coordinator is "immediate".
        """
        # Create a new "immediate" mode coordinator
        tmp_coordinator = MaintainedModelCoordinator("immediate")
        with MaintainedModel.custom_coordinator(tmp_coordinator):
            io, _ = create_infusate_records()
            # Since a parent coordinator is deferred, auto-update should not happen, but get_name always returns a
            # value.
            io.get_name
            # Assert method get_name does not auto-update if parent coordinator is deferred
            io_again = Infusate.objects.get(id__exact=io.id)
            self.assertIsNone(io_again.name)


@tag("load_study")
class MaintainedModelImmediateTests(TracebaseTestCase):
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
            "DataRepo/data/tests/small_obob/small_obob_study_prerequisites.yaml",
            verbosity=2,
        )
        super().setUpTestData()

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

    def test_get_name_triggers_autoupdate(self):
        """
        By disabling buffering, we ensure that the name field in the infusate model will be None, so it we get a value,
        we infer it used the `._name()` method.
        """
        tmp_coordinator = MaintainedModelCoordinator("disabled")
        with MaintainedModel.custom_coordinator(tmp_coordinator):
            io, _ = create_infusate_records()
        # The coordinator that was attached to that object was disabled.  To have a new coordinator, we either need to
        # retrieve a new object or explicitly set the coordinator. Maybe I should be resetting the coordinator in the
        # .save() override...
        io_again = Infusate.objects.get(id__exact=io.id)

        # Should be initially none
        self.assertIsNone(io_again.name)

        expected_name = "ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}"
        # Returned value should be equal
        self.assertEqual(expected_name, io_again.get_name)
        # And now the field should be updated
        self.assertEqual(expected_name, io_again.name)

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
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
            dry_run=False,
        )
        self.assert_coordinator_state_is_initialized()

    def test_error_when_buffer_not_clear(self):
        """Ensure that stale buffer contents before a load produces a helpful error"""
        # with self.assertRaises(Exception) as ar:
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
