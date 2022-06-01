from DataRepo.models import Compound, Tracer, TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class TracerLabelTests(TracebaseTestCase):
    def setUp(self):
        glu = Compound.objects.create(name="glucose", formula="C6H12O6")
        glu_t = Tracer.objects.create(compound=glu)
        TracerLabel.objects.create(
            tracer=glu_t, count=2, element="C", positions=[2, 3], mass_number=13
        )

    def test_tracer_label_implicit_name(self):
        tl = TracerLabel.objects.all()[0]
        self.assertEqual(str(tl), "2,3-13C2")

    def test_tracer_label_explicit_name(self):
        tl = TracerLabel.objects.all()[0]
        self.assertEqual(tl._name(), "2,3-13C2")
