from datetime import datetime, timedelta
from pathlib import Path

from django.core.files import File
from django.db.utils import IntegrityError

from DataRepo.models import (
    Animal,
    ArchiveFile,
    DataFormat,
    DataType,
    Infusate,
    LCMethod,
    MSRunSample,
    MSRunSequence,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    Sample,
    Tissue,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class PeakDataData(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

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
        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")

        seq = MSRunSequence(
            researcher="John Doe",
            date=datetime.now(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][1],
            lc_method=lcm,
        )
        seq.full_clean()
        seq.save()
        mstype = DataType.objects.get(code="ms_data")
        rawfmt = DataFormat.objects.get(code="ms_raw")
        mzxfmt = DataFormat.objects.get(code="mzxml")
        rawrec = ArchiveFile.objects.create(
            filename="test.raw",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c5",
            data_type=mstype,
            data_format=rawfmt,
        )
        mzxrec = ArchiveFile.objects.create(
            filename="test.mzxml",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c4",
            data_type=mstype,
            data_format=mzxfmt,
        )
        msr = MSRunSample(
            msrun_sequence=seq,
            sample=smpl,
            polarity="positive",
            ms_raw_file=rawrec,
            ms_data_file=mzxrec,
        )
        msr.full_clean()
        msr.save()

        path = Path("DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx")
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)
            ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
            accucor_format = DataFormat.objects.get(code="accucor")
            accucor_file = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
                data_type=ms_peak_annotation,
                data_format=accucor_format,
            )
            accucor_file.save()

        pg = PeakGroup.objects.create(
            name="gluc",
            formula="C6H12O6",
            msrun_sample=msr,
            peak_annotation_file=accucor_file,
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
