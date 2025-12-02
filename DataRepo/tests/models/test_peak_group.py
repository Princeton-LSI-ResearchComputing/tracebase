from datetime import datetime, timedelta
from pathlib import Path

from django.core.files import File
from django.core.management import call_command
from django.db import IntegrityError

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
    PeakGroup,
    Sample,
    Tissue,
)
from DataRepo.models.compound import Compound
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    MultiplePeakGroupRepresentation,
    NoTracerLabeledElements,
)
from DataRepo.utils.infusate_name_parser import parse_infusate_name


class PeakGroupTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        Compound.objects.create(
            name="Leucine", formula="C6H13NO2", hmdb_id="HMDB0000687"
        )
        trcr = parse_infusate_name("Leucine-[1,2-13C2]", [1.0])
        inf, _ = Infusate.objects.get_or_create_infusate(trcr)
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
        cls.smpl = Sample.objects.create(
            name="Sample Name",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")

        cls.seq = MSRunSequence.objects.create(
            researcher="John Doe",
            date=datetime.now(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
            lc_method=lcm,
        )
        cls.seq.full_clean()
        cls.mstype = DataType.objects.get(code="ms_data")
        cls.rawfmt = DataFormat.objects.get(code="ms_raw")
        cls.mzxfmt = DataFormat.objects.get(code="mzxml")
        rawrec = ArchiveFile.objects.create(
            filename="test.raw",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c5",
            data_type=cls.mstype,
            data_format=cls.rawfmt,
        )
        mzxrec = ArchiveFile.objects.create(
            filename="test.mzxml",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c4",
            data_type=cls.mstype,
            data_format=cls.mzxfmt,
        )
        msr = MSRunSample.objects.create(
            msrun_sequence=cls.seq,
            sample=cls.smpl,
            polarity=MSRunSample.POSITIVE_POLARITY,
            ms_raw_file=rawrec,
            ms_data_file=mzxrec,
        )
        msr.full_clean()

        cls.ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
        cls.accucor_format = DataFormat.objects.get(code="accucor")
        path = Path("DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx")
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)
            accucor_file = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
                data_type=cls.ms_peak_annotation,
                data_format=cls.accucor_format,
            )

        cls.pg = PeakGroup.objects.create(
            name="gluc",
            formula="C6H12O6",
            msrun_sample=msr,
            peak_annotation_file=accucor_file,
        )
        PeakData.objects.create(
            raw_abundance=1000.0,
            corrected_abundance=1000.0,
            peak_group=cls.pg,
            med_mz=4.0,
            med_rt=1.0,
        )
        PeakData.objects.create(
            raw_abundance=2000.0,
            corrected_abundance=2000.0,
            peak_group=cls.pg,
            med_mz=2.0,
            med_rt=2.0,
        )
        super().setUpTestData()

    def test_min_med_mz(self):
        self.assertEqual(2.0, self.pg.min_med_mz)

    def test_max_med_mz(self):
        self.assertEqual(4.0, self.pg.max_med_mz)

    def prepare_peak_group_creation(self):
        rawrec = ArchiveFile.objects.create(
            filename="test.raw",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c6",
            data_type=self.mstype,
            data_format=self.rawfmt,
        )
        mzxrec = ArchiveFile.objects.create(
            filename="test.mzxml",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c7",
            data_type=self.mstype,
            data_format=self.mzxfmt,
        )
        msr = MSRunSample.objects.create(
            msrun_sequence=self.seq,
            sample=self.smpl,
            polarity="negative",
            ms_raw_file=rawrec,
            ms_data_file=mzxrec,
        )
        msr.full_clean()
        accucor_file = ArchiveFile.objects.create(
            filename="small_obob_maven_6eaas_inf2.xlsx",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c8",
            data_type=self.ms_peak_annotation,
            data_format=self.accucor_format,
        )
        return msr, accucor_file

    def test_save_raises_MultiplePeakGroupRepresentations(self):
        msr, accucor_file = self.prepare_peak_group_creation()

        # The save method should raise a MultiplePeakGroupRepresentation exception
        with self.assertRaises(MultiplePeakGroupRepresentation):
            PeakGroup.objects.create(
                name="gluc",
                formula="C6H12O6",
                msrun_sample=msr,
                peak_annotation_file=accucor_file,
            )

    def test_clean_raises_NoTracerLabeledElements(self):
        msr, accucor_file = self.prepare_peak_group_creation()
        pg = PeakGroup.objects.create(
            name="water",
            formula="H2O",
            msrun_sample=msr,
            peak_annotation_file=accucor_file,
        )

        # The clean method should raise a NoTracerLabeledElements exception
        with self.assertRaises(NoTracerLabeledElements):
            pg.full_clean()

    def test_get_or_create_compound_link(self):
        cmpd = Compound.objects.create(
            name="glucose",
            formula="C6H12O6",
            hmdb_id="HMDB0000122",
        )
        rec, cre = self.pg.get_or_create_compound_link(cmpd)
        self.assertTrue(cre)
        self.assertIsNotNone(rec)
        rec, cre = self.pg.get_or_create_compound_link(cmpd)
        self.assertFalse(cre)
        self.assertIsNotNone(rec)

    def test_total_abundance(self):
        self.assertAlmostEqual(self.pg._total_abundance(), 3000)

    def test_unique_constraint(self):
        self.assertRaises(
            IntegrityError,
            lambda: PeakGroup.objects.create(
                name=self.pg.name, msrun_sample=self.pg.msrun_sample
            ),
        )


class MultiLabelPeakGroupTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

    @classmethod
    @MaintainedModel.no_autoupdates()
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/multiple_labels/animal_sample_table_v3.xlsx",
            exclude_sheets=["Peak Annotation Files"],
        )
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/multiple_labels/alafasted_cor.xlsx",
        )

        super().setUpTestData()

    @MaintainedModel.no_autoupdates()
    def test_peak_labeled_elements_one(self):
        # succinate has no nitrogen
        pg = PeakGroup.objects.filter(msrun_sample__sample__name="xzl5_panc").get(
            name="succinate"
        )
        output = pg.peak_labeled_elements
        # One common element
        expected = ["C"]
        self.assertEqual(expected, output)

    @MaintainedModel.no_autoupdates()
    def test_peak_labeled_elements_two(self):
        pg = PeakGroup.objects.filter(msrun_sample__sample__name="xzl5_panc").get(
            name="glutamine"
        )
        output = pg.peak_labeled_elements
        expected = ["C", "N"]
        self.assertEqual(expected, output)
