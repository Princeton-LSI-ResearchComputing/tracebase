from DataRepo.tests.tracebase_test_case import TracebaseTestCase

from DataRepo.models import InfusateTracer, Infusate, Tracer, TracerLabel, Compound


class InfusateTests(TracebaseTestCase):
    def setUp(self):
        glu = Compound.objects.create(name="glucose", formula="C6H12O6", hmdb_id=1)
        c16 = Compound.objects.create(name="C16:0", formula="C16H32O2", hmdb_id=2)
        glu_t = Tracer.objects.create(name="gluc13", compound=glu)
        c16_t = Tracer.objects.create(name="c16c13", compound=c16)
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
        io = Infusate.objects.create(
            name="test infusate",
            short_name="ti",
        )
        InfusateTracer.objects.create(infusate=io, tracer=glu_t, concentration=1.0)
        InfusateTracer.objects.create(infusate=io, tracer=c16_t, concentration=2.0)

    def test_infusate_record(self):
        infusate = Infusate.objects.get(name="test infusate")
        infusate.full_clean()

    def test_infusate_name(self):
        infusate = Infusate.objects.get(name="test infusate")
        self.assertEqual(
            infusate._name, "ti{C16:0-[5,6-13C5,17O1];glucose-[2,3-13C5,4-17O1]}"
        )
