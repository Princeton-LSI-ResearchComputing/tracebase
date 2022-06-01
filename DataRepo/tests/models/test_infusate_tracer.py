from django.db.utils import IntegrityError

from DataRepo.models import (
    Compound,
    Infusate,
    InfusateTracer,
    Tracer,
    TracerLabel,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class InfusateTracerTests(TracebaseTestCase):
    def setUp(self):
        glu = Compound.objects.create(
            name="glucose", formula="C6H12O6", hmdb_id="HMDB0000122"
        )
        glu_t = Tracer.objects.create(compound=glu)
        TracerLabel.objects.create(
            tracer=glu_t, count=2, element="C", positions=[2, 3], mass_number=13
        )
        TracerLabel.objects.create(
            tracer=glu_t, count=1, element="O", positions=[4], mass_number=17
        )
        Infusate.objects.create(short_name="ti")

    def test_no_concentration(self):
        with self.assertRaises(IntegrityError):
            tracer = Tracer.objects.first()
            infusate = Infusate.objects.first()
            InfusateTracer.objects.create(infusate=infusate, tracer=tracer)
