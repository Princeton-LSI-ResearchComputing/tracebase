from DataRepo.models.compound import Compound
from DataRepo.models.maintained_model import (
    MaintainedFieldNotSettable,
    are_autoupdates_enabled,
)
from DataRepo.models.tracer import Tracer
from DataRepo.models.tracer_label import TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


def create_tracer_record():
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

    return glu_t


class TracerTests(TracebaseTestCase):
    def test_tracer_name(self):
        tracer = create_tracer_record()
        self.assertEqual(tracer._name(), "glucose-[2,3-13C2,4-17O1]")

    def test_name_not_settable(self):
        c16 = Compound.objects.create(name="C16:0", formula="C16H32O2", hmdb_id=2)
        with self.assertRaises(MaintainedFieldNotSettable):
            Tracer.objects.create(
                name="test tracer",
                compound=c16,
            )

    def test_name_autoupdated(self):
        """
        Make sure that the name field was set automatically - updates are triggered by the tracer record creation and
        each TracerLabel record creation, after which it has its final value.
        """
        # Throws DoesNotExist exception if not found
        self.assertTrue(are_autoupdates_enabled())
        to = create_tracer_record()
        self.assertEqual("glucose-[2,3-13C2,4-17O1]", to.name)
