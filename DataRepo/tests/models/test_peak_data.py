from datetime import datetime, timedelta

from django.db.utils import IntegrityError

from DataRepo.models import (
    Animal,
    Infusate,
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


class PeakDataData(TracebaseTestCase):
    def setUp(self):
        super().setUp()
        inf = Infusate()
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
        PeakData.objects.create(
            raw_abundance=1000.0,
            corrected_abundance=1000.0,
            peak_group=pg,
            med_mz=1.0,
            med_rt=1.0,
        )


class PeakDataTests(PeakDataData):
    def test_record(self):
        rec = PeakData.objects.get(raw_abundance=1000.0)
        rec.full_clean()

    def test_multiple_labels(self):
        pd = PeakData.objects.get(raw_abundance=1000.0)
        PeakDataLabel.objects.create(
            peak_data=pd,
            element="C",
            count=5,
            mass_number=13,
        )
        PeakDataLabel.objects.create(
            peak_data=pd,
            element="O",
            count=1,
            mass_number=17,
        )
        self.assertEqual(pd.labels.count(), 2)


class PeakDataLabelTests(PeakDataData):
    def setUp(self):
        super().setUp()
        pd = PeakData.objects.get(raw_abundance=1000.0)
        PeakDataLabel.objects.create(
            peak_data=pd,
            element="C",
            count=5,
            mass_number=13,
        )

    def test_record(self):
        rec = PeakDataLabel.objects.get(element="C")
        rec.full_clean()

    def test_multiple_labels_with_same_elem(self):
        """Test creating a second PeakDataLabel with the same element"""
        pd = PeakData.objects.get(raw_abundance=1000.0)
        with self.assertRaisesRegex(IntegrityError, r"\(\d+, C\)"):
            PeakDataLabel.objects.create(
                peak_data=pd,
                element="C",
                count=1,
                mass_number=13,
            )
