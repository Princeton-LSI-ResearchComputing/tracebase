from django.db.utils import IntegrityError
from DataRepo.tests.tracebase_test_case import TracebaseTestCase

from DataRepo.models import InfusateTracer, Infusate, Tracer, TracerLabel, Compound


class InfusateTests(TracebaseTestCase):
    def setUp(self):
        glu = Compound.objects.create(name="glucose", formula="C6H12O6", hmdb_id=1)
        glu_t = Tracer.objects.create(name="gluc13", compound=glu)
        TracerLabel.objects.create(
            tracer=glu_t, count=5, element="C", positions=[2, 3], mass_number=13
        )
        TracerLabel.objects.create(
            tracer=glu_t, count=1, element="O", positions=[4], mass_number=17
        )
        Infusate.objects.create(
            name="test infusate",
            short_name="ti",
        )

    def test_no_concentration(self):
        with self.assertRaises(IntegrityError):
            tracer = Tracer.objects.get(name="gluc13")
            infusate = Infusate.objects.get(name="test infusate")
            InfusateTracer.objects.create(infusate=infusate, tracer=tracer)
