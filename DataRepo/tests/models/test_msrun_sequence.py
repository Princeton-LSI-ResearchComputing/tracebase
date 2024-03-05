import dateutil.parser
from django.core.exceptions import ValidationError

from DataRepo.models import LCMethod, MSRunSequence
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class MSRunSequenceTests(TracebaseTestCase):
    def setUp(self):
        self.lcm = LCMethod(
            name="L.C. McMethod",
            type="unknown",
            description="A long time ago, in a galaxy far far away...",
        )
        self.lcm.full_clean()
        self.lcm.save()
        super().setUp()

    def test_msrun_sequence(self):
        seq = MSRunSequence(
            researcher="Jerry Seinfeld",
            date=dateutil.parser.parse("11-24-1972").date(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
            lc_method=self.lcm,
        )
        seq.full_clean()
        seq.save()

    def test_instrument_choices(self):
        with self.assertRaises(Exception) as ar:
            seq = MSRunSequence(
                researcher="Jerry Seinfeld",
                date=dateutil.parser.parse("11-24-1972").date(),
                instrument="invalid instrument",
                lc_method=self.lcm,
            )
            seq.full_clean()
            seq.save()
        exc = ar.exception
        self.assertEqual(ValidationError, type(exc))
