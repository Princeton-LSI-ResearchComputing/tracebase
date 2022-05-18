from datetime import datetime, timedelta

from DataRepo.models import (
    Animal,
    MSRun,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupSet,
    Protocol,
    Sample,
    Tissue,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class PeakDataLabelTests(TracebaseTestCase):
    def setUp(self):
        anml = Animal.objects.create(
            name="test_animal",
            age=timedelta(weeks=int(13)),
            sex="M",
            genotype="WT",
            body_weight=200,
            diet="normal",
            feeding_status="fed",
        )
        tsu = Tissue.objects.create(name="Brain")
        smpl = Sample.objects.create(
            name="Sample Name",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        ptl = Protocol.objects.create(
            name="p1",
            description="p1desc",
            category=Protocol.MSRUN_PROTOCOL,
        )
        msr = MSRun.objects.create(
            researcher="John Doe",
            date=datetime.now(),
            sample=smpl,
            protocol=ptl,
        )
        pgs = PeakGroupSet.objects.create(filename="testing_dataset_file")
        pg = PeakGroup.objects.create(
            name="gluc",
            formula="C6H12O6",
            msrun=msr,
            peak_group_set=pgs,
        )
        pd = PeakData.objects.create(
            raw_abundance=1000.0,
            corrected_abundance=1000.0,
            peak_group=pg,
            med_mz=1.0,
            med_rt=1.0,
        )
        PeakDataLabel.objects.create(
            peak_data=pd,
            element="C",
            count=5,
            mass_number=13,
        )

    def test_record(self):
        rec = PeakDataLabel.objects.get(element="C")
        rec.full_clean()
