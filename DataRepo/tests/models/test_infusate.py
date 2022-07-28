from django.test import tag

from DataRepo.management.commands.rebuild_maintained_fields import (
    rebuild_maintained_fields,
)
from DataRepo.models.compound import Compound
from DataRepo.models.infusate import Infusate
from DataRepo.models.infusate_tracer import InfusateTracer
from DataRepo.models.maintained_model import (
    MaintainedFieldNotSettable,
    buffer_size,
    clear_update_buffer,
    disable_autoupdates,
    enable_autoupdates,
    perform_buffered_updates,
)
from DataRepo.models.tracer import Tracer
from DataRepo.models.tracer_label import TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


def create_infusate_records():
    glu = Compound.objects.create(
        name="glucose", formula="C6H12O6", hmdb_id="HMDB0000122"
    )
    c16 = Compound.objects.create(
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


@tag("multi_working")
class InfusateTests(TracebaseTestCase):
    def setUp(self):
        create_infusate_records()

    def test_infusate_record(self):
        infusate = Infusate.objects.first()
        infusate.full_clean()

    def test_infusate_name_method(self):
        infusate = Infusate.objects.last()
        self.assertEqual(
            infusate._name(),
            "ti {C16:0-(5,6-13C2,17O2)[2];glucose-(2,3-13C2,4-17O1)[1]}",
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
            name="ti {C16:0-(5,6-13C2,17O2)[2];glucose-(2,3-13C2,4-17O1)[1]}"
        )

    def test_name_none(self):
        """
        Make sure that the name field was set automatically - triggered by the InfusateTracer record creation.
        """
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(
            name="C16:0-(5,6-13C2,17O2)[4];glucose-(2,3-13C2,4-17O1)[3]"
        )

    def test_name_self_autoupdated(self):
        """
        Make sure that the name field was set automatically - triggered by the Infusate record creation.
        """
        Infusate.objects.create(tracer_group_name="ti3")
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(name="ti3")

    def test_delete_autoupdate(self):
        """
        Make sure parent records are updated when a child record is deleted
        """
        tl = TracerLabel.objects.get(name="2,3-13C2")
        tl.delete()
        # These queries will raise an exception if the name was not auto-updated
        Tracer.objects.get(name="glucose-(4-17O1)")
        Infusate.objects.get(name="C16:0-(5,6-13C2,17O2)[4];glucose-(4-17O1)[3]")


@tag("multi_working")
class MaintainedModelTests(TracebaseTestCase):
    def setUp(self):
        # Each test first reruns the setup and the DB load adds the same records to the buffer. The DB is emptied after
        # the test runs, but the buffer needs to be explicitly emptied
        clear_update_buffer()
        disable_autoupdates()
        create_infusate_records()
        enable_autoupdates()

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
        disable_autoupdates()  # Required for buffered updates to prevent DFS update behavior
        perform_buffered_updates()
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
        self.assertEqual(buffer_size(), 0)
        enable_autoupdates()

    def test_enable_autoupdates(self):
        """
        Ensures that the name field was constructed.
        """
        enable_autoupdates()
        lys = Compound.objects.create(name="lysine", formula="C6H14N2O2", hmdb_id=3)
        Tracer.objects.create(compound=lys)
        Tracer.objects.get(name="lysine")


@tag("multi_working")
class RebuildMaintainedModelFieldsTests(TracebaseTestCase):
    def setUp(self):
        # Each test first reruns the setup and the DB load adds the same records to the buffer. The DB is emptied after
        # the test runs, but the buffer needs to be explicitly emptied
        disable_autoupdates()
        create_infusate_records()
        enable_autoupdates()

    def test_rebuild_maintained_fields(self):
        rebuild_maintained_fields()
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
        # Ensure the buffer was emptied by perform_buffered_updates
        self.assertEqual(buffer_size(), 0)
