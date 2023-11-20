from datetime import timedelta

import dateutil.parser

from DataRepo.models import (
    Animal,
    Infusate,
    LCMethod,
    MSRunSample,
    MSRunSequence,
    Sample,
    Tissue,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class MSRunSampleTests(TracebaseTestCase):
    def setUp(self):
        lcm = LCMethod(
            name="L.C. McMethod",
            type="unknown",
            description="A long time ago, in a galaxy far far away...",
        )
        lcm.full_clean()
        lcm.save()

        self.seq = MSRunSequence(
            researcher="Jerry Seinfeld",
            date=dateutil.parser.parse("11-24-1972").date(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][1],
            lc_method=lcm,
        )
        self.seq.full_clean()
        self.seq.save()

        inf = Infusate()
        inf.full_clean()
        inf.save()

        anml = Animal.objects.create(
            name="test_animal",
            age=timedelta(weeks=int(13)),
            sex="M",
            genotype="WT",
            body_weight=200,
            diet="normal",
            feeding_status="fed",
            infusate=inf,
        )
        anml.full_clean()
        anml.save()

        tis = Tissue(
            name="liver",
            description="What dies of alcohol poisoning?",
        )
        tis.full_clean()
        tis.save()

        tc = timedelta(seconds=1)

        self.smpl = Sample.objects.create(
            name="lvr1",
            animal=anml,
            tissue=tis,
            time_collected=tc,
            researcher="Cosmo Kramer",
        )
        self.smpl.full_clean()
        self.smpl.save()

        super().setUp()

    def test_msrun_sample(self):
        seq = MSRunSample(
            msrun_sequence=self.seq,
            sample=self.smpl,
        )
        seq.full_clean()
        seq.save()
