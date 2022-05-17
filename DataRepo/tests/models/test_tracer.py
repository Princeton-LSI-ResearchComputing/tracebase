from DataRepo.tests.tracebase_test_case import TracebaseTestCase

from DataRepo.models import Tracer, TracerLabel, Compound


class TracerTests(TracebaseTestCase):
    def setUp(self):
        glu = Compound.objects.create(name="glucose", formula="C6H12O6", hmdb_id=1)
        glu_t = Tracer.objects.create(name="gluc13", compound=glu)
        TracerLabel.objects.create(
            tracer=glu_t, count=5, element="C", positions=[2, 3], mass_number=13
        )
        TracerLabel.objects.create(
            tracer=glu_t, count=1, element="O", positions=[4], mass_number=17
        )

    def test_tracer_name(self):
        infusate = Tracer.objects.get(name="gluc13")
        self.assertEqual(infusate._name, "glucose-[2,3-13C5,4-17O1]")
