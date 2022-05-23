from DataRepo.models import (
    Compound,
    Infusate,
    InfusateTracer,
    Tracer,
    TracerLabel,
)
from DataRepo.models.maintained_model import MaintainedFieldNotSettable
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class InfusateTests(TracebaseTestCase):
    def setUp(self):
        glu = Compound.objects.create(name="glucose", formula="C6H12O6", hmdb_id=1)
        c16 = Compound.objects.create(name="C16:0", formula="C16H32O2", hmdb_id=2)
        glu_t = Tracer.objects.create(
            # name="gluc13",
            compound=glu,
        )
        c16_t = Tracer.objects.create(
            # name="c16c13",
            compound=c16,
        )
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
        # Throws DoesNotExist exception if not found
        infusate = Infusate.objects.first()
        print(f"Name: {infusate.name}")
        Infusate.objects.get(name="ti{C16:0-[5,6-13C5,17O1];glucose-[2,3-13C5,4-17O1]}")
