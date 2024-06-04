from datetime import timedelta

import dateutil.parser
from django.core.exceptions import ValidationError

from DataRepo.models import (
    Animal,
    ArchiveFile,
    DataFormat,
    DataType,
    Infusate,
    LCMethod,
    MSRunSample,
    MSRunSequence,
    Sample,
    Tissue,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class MSRunSampleTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

    def setUp(self):
        lcm = LCMethod(
            name="unknown",
            type="unknown",
            description="A long time ago, in a galaxy far far away...",
        )
        lcm.full_clean()
        lcm.save()

        self.seq = MSRunSequence(
            researcher="Jerry Seinfeld",
            date=dateutil.parser.parse("11-24-1972").date(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
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
        msrs = MSRunSample(
            msrun_sequence=self.seq,
            sample=self.smpl,
        )
        msrs.full_clean()
        msrs.save()

    def test_msrun_sample_all(self):
        mstype = DataType.objects.get(code="ms_data")
        rawfmt = DataFormat.objects.get(code="ms_raw")
        mzxfmt = DataFormat.objects.get(code="mzxml")
        rawrec = ArchiveFile.objects.create(
            filename="test.raw",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
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
        msrs = MSRunSample(
            msrun_sequence=self.seq,
            sample=self.smpl,
            polarity=MSRunSample.POSITIVE_POLARITY,
            ms_raw_file=rawrec,
            ms_data_file=mzxrec,
        )
        msrs.full_clean()
        msrs.save()

    def test_msdata_format_unknown(self):
        mstype = DataType.objects.get(code="ms_data")
        mztfmt = DataFormat.objects.get(code="unknown")
        mztrec = ArchiveFile.objects.create(
            filename="test.mztab",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
            data_type=mstype,
            data_format=mztfmt,
        )
        msrs = MSRunSample(
            msrun_sequence=self.seq,
            sample=self.smpl,
            ms_data_file=mztrec,
        )
        msrs.full_clean()
        msrs.save()

    def test_bad_polarity(self):
        with self.assertRaises(ValidationError) as ar:
            seq = MSRunSample(
                msrun_sequence=self.seq,
                sample=self.smpl,
                polarity="invalid",
            )
            seq.full_clean()
            seq.save()
        exc = ar.exception
        self.assertIn("polarity", str(exc))

    def assert_archive_file_exception(self, fn, typ, fmt, raw=True):
        """
        Supply values that should cause the creation of an ArchiveFile record to produce an exception.  That exception
        will be returned.
        Note, this method was made in order to avoid JSCPD errors.
        """
        afrec = ArchiveFile.objects.create(
            filename=fn,
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
            data_type=typ,
            data_format=fmt,
        )
        with self.assertRaises(ValidationError) as ar:
            if raw:
                msrs = MSRunSample(
                    msrun_sequence=self.seq,
                    sample=self.smpl,
                    ms_raw_file=afrec,
                )
            else:
                msrs = MSRunSample(
                    msrun_sequence=self.seq,
                    sample=self.smpl,
                    ms_data_file=afrec,
                )
            msrs.full_clean()
            msrs.save()
        return ar.exception

    def test_bad_raw_type(self):
        mstype = DataType.objects.get(code="ms_peak_annotation")
        rawfmt = DataFormat.objects.get(code="ms_raw")
        exc = self.assert_archive_file_exception("test.raw", mstype, rawfmt)
        self.assertIn("ms_raw_file", str(exc))
        self.assertIn("data type", str(exc))

    def test_bad_raw_fmt(self):
        mstype = DataType.objects.get(code="ms_data")
        rawfmt = DataFormat.objects.get(code="accucor")
        exc = self.assert_archive_file_exception("test.raw", mstype, rawfmt)
        self.assertIn("ms_raw_file", str(exc))
        self.assertIn("data format", str(exc))

    def test_bad_mzx_type(self):
        mstype = DataType.objects.get(code="ms_peak_annotation")
        mzxfmt = DataFormat.objects.get(code="mzxml")
        exc = self.assert_archive_file_exception("test.mzxml", mstype, mzxfmt, False)
        self.assertIn("ms_data_file", str(exc))
        self.assertIn("data type", str(exc))

    def test_bad_mzx_fmt(self):
        mstype = DataType.objects.get(code="ms_data")
        mzxfmt = DataFormat.objects.get(code="accucor")
        exc = self.assert_archive_file_exception("test.mzxml", mstype, mzxfmt, False)
        self.assertIn("ms_data_file", str(exc))
        self.assertIn("data format", str(exc))
