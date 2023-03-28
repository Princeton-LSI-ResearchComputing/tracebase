from django.db.utils import IntegrityError

from DataRepo.models.compound import Compound
from DataRepo.models.infusate import Infusate
from DataRepo.models.infusate_tracer import InfusateTracer
from DataRepo.models.tracer import Tracer
from DataRepo.models.tracer_label import TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class InfusateTracerTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()
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
        Infusate.objects.create(tracer_group_name="ti")

    def test_no_concentration(self):
        with self.assertRaises(IntegrityError):
            tracer = Tracer.objects.first()
            infusate = Infusate.objects.first()
            InfusateTracer.objects.create(infusate=infusate, tracer=tracer)
