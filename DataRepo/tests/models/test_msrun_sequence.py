import dateutil.parser
from django.core.exceptions import ValidationError

from DataRepo.models import LCMethod, MSRunSequence
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class MSRunSequenceTests(TracebaseTestCase):
    def setUp(self):
        self.lcm = LCMethod(
            name="unknown",
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

    def test_parse_sequence_name(self):
        (operator, lc_protocol_name, instrument, date) = (
            MSRunSequence.parse_sequence_name("Rob, polar-HILIC-25-min, QE, 1972-11-24")
        )
        self.assertEqual(
            ("Rob", "polar-HILIC-25-min", "QE", "1972-11-24"),
            (operator, lc_protocol_name, instrument, date),
        )

    def test_sequence_name(self):
        lcm = LCMethod(
            name="polar-HILIC-25-min",
            type="polar-HILIC",
            description="Here it is! 2487. You go and get her! I'll wait here!",
        )
        self.lcm.full_clean()
        self.lcm.save()
        seq = MSRunSequence(
            researcher="Mark Hamill",
            date=dateutil.parser.parse("5-4-1977").date(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
            lc_method=lcm,
        )
        self.assertEqual(
            "Mark Hamill, polar-HILIC-25-min, QE, 1977-05-04", seq.sequence_name
        )
