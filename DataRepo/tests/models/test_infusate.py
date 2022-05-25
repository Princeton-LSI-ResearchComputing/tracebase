from DataRepo.models import (
    Compound,
    Infusate,
    InfusateTracer,
    Tracer,
    TracerLabel,
)
from DataRepo.models.maintained_model import (
    MaintainedFieldNotSettable,
    buffer_size,
    clear_update_buffer,
    disable_autoupdates,
    enable_autoupdates,
    perform_buffered_updates,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class InfusateTests(TracebaseTestCase):
    def setUp(self):
        glu = Compound.objects.create(name="glucose", formula="C6H12O6", hmdb_id=1)
        c16 = Compound.objects.create(name="C16:0", formula="C16H32O2", hmdb_id=2)
        glu_t = Tracer.objects.create(compound=glu)
        c16_t = Tracer.objects.create(compound=c16)
        TracerLabel.objects.create(
            tracer=glu_t, count=5, element="C", positions=[2, 3], mass_number=13
        )
        TracerLabel.objects.create(
            tracer=glu_t, count=1, element="O", positions=[4], mass_number=17
        )
        TracerLabel.objects.create(
            tracer=c16_t, count=5, element="C", positions=[5, 6], mass_number=13
        )
        TracerLabel.objects.create(tracer=c16_t, count=1, element="O", mass_number=17)
        io = Infusate.objects.create(short_name="ti")
        InfusateTracer.objects.create(infusate=io, tracer=glu_t, concentration=1.0)
        InfusateTracer.objects.create(infusate=io, tracer=c16_t, concentration=2.0)

    def test_infusate_record(self):
        infusate = Infusate.objects.first()
        infusate.full_clean()

    def test_infusate_name_method(self):
        infusate = Infusate.objects.first()
        self.assertEqual(
            infusate._name(), "ti{C16:0-[5,6-13C5,17O1];glucose-[2,3-13C5,4-17O1]}"
        )

    def test_name_not_settable(self):
        with self.assertRaises(MaintainedFieldNotSettable):
            Infusate.objects.create(
                name="test infusate",
                short_name="ti2",
            )

    def test_name_autoupdated(self):
        """
        Make sure that the name field was set automatically - triggered by the InfusateTracer record creation.
        """
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(name="ti{C16:0-[5,6-13C5,17O1];glucose-[2,3-13C5,4-17O1]}")

    def test_name_self_autoupdated(self):
        """
        Make sure that the name field was set automatically - triggered by the Infusate record creation.
        """
        Infusate.objects.create(short_name="ti3")
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(name="ti3")


class MaintainedModelTests(TracebaseTestCase):
    def setUp(self):
        # Each test first reruns the setup and the DB load adds the same records to the buffer. The DB is emptied after
        # the test runs, but the buffer needs to be explicitly emptied
        clear_update_buffer()
        disable_autoupdates()
        print("CREATING RECORDS WITHOUT AUTOUPDATE TURNED ON")
        print("CREATING GLUCOSE")
        glu = Compound.objects.create(name="glucose", formula="C6H12O6", hmdb_id=1)
        print("CREATING C16")
        c16 = Compound.objects.create(name="C16:0", formula="C16H32O2", hmdb_id=2)
        print("CREATING GLUCOSE TRACER")
        glu_t = Tracer.objects.create(compound=glu)
        print("CREATING C16 TRACER")
        c16_t = Tracer.objects.create(compound=c16)
        print("CREATING LABEL 2,3-13C5")
        TracerLabel.objects.create(
            tracer=glu_t, count=5, element="C", positions=[2, 3], mass_number=13
        )
        print("CREATING LABEL 2,3-17O1")
        TracerLabel.objects.create(
            tracer=glu_t, count=1, element="O", positions=[4], mass_number=17
        )
        print("CREATING LABEL 5,6-13C5")
        TracerLabel.objects.create(
            tracer=c16_t, count=5, element="C", positions=[5, 6], mass_number=13
        )
        print("CREATING LABEL 17O1")
        TracerLabel.objects.create(tracer=c16_t, count=1, element="O", mass_number=17)
        print("CREATING INFUSATE ti")
        io = Infusate.objects.create(short_name="ti")
        print("CREATING INFUSATE TRACER ti glucose tracer c1")
        InfusateTracer.objects.create(infusate=io, tracer=glu_t, concentration=1.0)
        print("CREATING INFUSATE TRACER ti c16 tracer c2")
        InfusateTracer.objects.create(infusate=io, tracer=c16_t, concentration=2.0)

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
        print(
            f"RUNNING DISABLED AUTOUPDATE TEST {TracerLabel.objects.first()} name: "
            f"[{TracerLabel.objects.first().name}] isNone: [{TracerLabel.objects.first().name is None}]"
        )
        self.assertIsNone(TracerLabel.objects.first().name)
        self.assertIsNone(Tracer.objects.first().name)
        self.assertIsNone(Infusate.objects.first().name)

    def test_mass_autoupdate(self):
        """
        Ensures that the name fields were all updated, updated correctly, and the buffer emptied.
        """
        print("RUNNING MASS AUTOUPDATE TEST")
        num_labelrecs_buffered = buffer_size(generation=3)
        print(f"There are {num_labelrecs_buffered} buffered label records")
        perform_buffered_updates()
        tls = TracerLabel.objects.all()
        for tl in tls:
            self.assertIsNotNone(tl.name)
            self.assertEqual(tl.name, tl._name())
        for t in Tracer.objects.all():
            self.assertIsNotNone(t.name)
            self.assertEqual(t.name, t._name())
        for i in Infusate.objects.all():
            self.assertIsNotNone(i.name)
            self.assertEqual(i.name, i._name())
        self.assertEqual(buffer_size(), 0)

    def test_enable_autoupdates(self):
        """
        Ensures that the name field was constructed.
        """
        print("RUNNING ENABLED AUTOUPDATE TEST")
        enable_autoupdates()
        lys = Compound.objects.create(name="lysine", formula="C6H14N2O2", hmdb_id=3)
        Tracer.objects.create(compound=lys)
        Tracer.objects.get(name="lysine")
