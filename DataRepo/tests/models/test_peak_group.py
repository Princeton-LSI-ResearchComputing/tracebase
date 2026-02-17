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
    ComplexPeakGroupDuplicate,
    DuplicatePeakGroup,
    MultiplePeakGroupRepresentation,
    NoTracerLabeledElements,
    TechnicalPeakGroupDuplicate,
)
from DataRepo.utils.infusate_name_parser import (
    ObservedIsotopeData,
    parse_infusate_name,
)


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
        self.assertAlmostEqual(self.pg.total_abundance, 3000)

    def test_unique_constraint(self):
        with self.assertRaises(IntegrityError) as ar:
            PeakGroup.objects.create(
                name=self.pg.name,
                msrun_sample=self.pg.msrun_sample,
                peak_annotation_file=self.pg.peak_annotation_file,
            )
        self.assertIn(
            "duplicate key value violates unique constraint", str(ar.exception)
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

    @MaintainedModel.no_autoupdates()
    def test_possible_isotope_observations(self):
        pg = PeakGroup.objects.filter(msrun_sample__sample__name="xzl5_panc").get(
            name="glutamine"
        )
        output = pg.possible_isotope_observations
        expected = [
            ObservedIsotopeData(
                element="C",
                mass_number=13,
                count=0,
                parent=True,
            ),
            ObservedIsotopeData(
                element="N",
                mass_number=15,
                count=0,
                parent=True,
            ),
        ]
        self.assertEqual(expected, output)

    @MaintainedModel.no_autoupdates()
    def test_check_for_multiple_representations_integrity_error_fallback(self):
        """This test asserts that check_for_multiple_representations raises no error when this is a simple
        UniqueConstraint violation."""
        existing_pg = PeakGroup.objects.filter(
            msrun_sample__sample__name="xzl5_panc"
        ).get(name="glutamine")
        new_pg = PeakGroup(
            name=existing_pg.name,
            formula=existing_pg.formula,
            msrun_sample=existing_pg.msrun_sample,
            peak_annotation_file=existing_pg.peak_annotation_file,
        )
        # No error raised = successful test
        new_pg.check_for_multiple_representations()

    @MaintainedModel.no_autoupdates()
    def test_check_for_multiple_representations_duplicate_peak_group(self):
        """This test asserts that check_for_multiple_representations raises a DuplicatePeakGroup error when the
        MSRunSample differs."""
        existing_pg = PeakGroup.objects.filter(
            msrun_sample__sample__name="xzl5_panc"
        ).get(name="glutamine")
        # To test this, we need a concrete MSRunSample record, and for that we need an mzXML ArchiveFile record
        mzf, _ = ArchiveFile.objects.get_or_create(
            filename="test_mz_file",
            checksum="23456789010",
            data_type=DataType.objects.get(code="ms_data"),
            data_format=DataFormat.objects.get(code="mzxml"),
        )
        concrete_msrun_sample, _ = MSRunSample.objects.get_or_create(
            sample=existing_pg.msrun_sample.sample,
            msrun_sequence=existing_pg.msrun_sample.msrun_sequence,
            ms_data_file=mzf,
        )
        new_pg = PeakGroup(
            name=existing_pg.name,
            formula=existing_pg.formula,
            msrun_sample=concrete_msrun_sample,
            peak_annotation_file=existing_pg.peak_annotation_file,
        )
        with self.assertRaises(DuplicatePeakGroup):
            new_pg.check_for_multiple_representations()

    @MaintainedModel.no_autoupdates()
    def test_check_for_multiple_representations_technical_peak_group_duplicate(self):
        """This test asserts that check_for_multiple_representations raises a TechnicalPeakGroupDuplicate error when the
        peak annotation file has the same name, but its content differs (i.e. the checksum differs).
        """
        existing_pg = PeakGroup.objects.filter(
            msrun_sample__sample__name="xzl5_panc"
        ).get(name="glutamine")
        # To test this, we need an edited peak annotation file (i.e. same file name but different checksum)
        paaf = ArchiveFile.objects.create(
            filename=existing_pg.peak_annotation_file.filename,
            file_location=None,
            checksum="23456789011",
            data_type=DataType.objects.get(code="ms_peak_annotation"),
            data_format=DataFormat.objects.get(code="accucor"),
        )
        new_pg = PeakGroup(
            name=existing_pg.name,
            formula=existing_pg.formula,
            msrun_sample=existing_pg.msrun_sample,
            peak_annotation_file=paaf,
        )
        with self.assertRaises(TechnicalPeakGroupDuplicate):
            new_pg.check_for_multiple_representations()

    @MaintainedModel.no_autoupdates()
    def test_check_for_multiple_representations_complex_peak_group_duplicate(self):
        """This test asserts that check_for_multiple_representations raises a ComplexPeakGroupDuplicate error when the
        record qualitatively differs (e.g. the formula is different) and either the peak annotation file has the same
        name, but its content differs or the msrun_sample differs."""
        existing_pg = PeakGroup.objects.filter(
            msrun_sample__sample__name="xzl5_panc"
        ).get(name="glutamine")
        # To test this, we need an edited peak annotation file (i.e. same file name but different checksum) and...
        paaf = ArchiveFile.objects.create(
            filename=existing_pg.peak_annotation_file.filename,
            checksum="23456789011",
            file_location=None,
            data_type=DataType.objects.get(code="ms_peak_annotation"),
            data_format=DataFormat.objects.get(code="accucor"),
        )
        # We need a different formula
        new_pg1 = PeakGroup(
            name=existing_pg.name,
            formula="C4H10",
            msrun_sample=existing_pg.msrun_sample,
            peak_annotation_file=paaf,
        )
        with self.assertRaises(ComplexPeakGroupDuplicate):
            new_pg1.check_for_multiple_representations()

        # We will also test the case of a different MSRunSample

        # To test this, we need a concrete MSRunSample record, and for that we need an mzXML ArchiveFile record
        mzf, _ = ArchiveFile.objects.get_or_create(
            filename="test_mz_file",
            checksum="23456789010",
            data_type=DataType.objects.get(code="ms_data"),
            data_format=DataFormat.objects.get(code="mzxml"),
        )
        concrete_msrun_sample = MSRunSample.objects.create(
            sample=existing_pg.msrun_sample.sample,
            msrun_sequence=existing_pg.msrun_sample.msrun_sequence,
            ms_data_file=mzf,
        )
        new_pg2 = PeakGroup(
            name=existing_pg.name,
            formula="C4H10",
            msrun_sample=concrete_msrun_sample,
            peak_annotation_file=existing_pg.peak_annotation_file,
        )
        with self.assertRaises(ComplexPeakGroupDuplicate):
            new_pg2.check_for_multiple_representations()

    @MaintainedModel.no_autoupdates()
    def test_check_for_multiple_representations_multiple_representation(self):
        """This test asserts that check_for_multiple_representations raises a MultiplePeakGroupRepresentation error in
        the cannonical case where it is simply the same sample, same name, but different peak annotations file.
        """
        existing_pg = PeakGroup.objects.filter(
            msrun_sample__sample__name="xzl5_panc"
        ).get(name="glutamine")
        # To test this, we need an edited peak annotation file (i.e. same file name but different checksum)
        paaf = ArchiveFile.objects.create(
            filename="some_other_file.xlsx",
            file_location=None,
            checksum="23456789012",
            data_type=DataType.objects.get(code="ms_peak_annotation"),
            data_format=DataFormat.objects.get(code="accucor"),
        )
        new_pg = PeakGroup(
            name=existing_pg.name,
            msrun_sample=existing_pg.msrun_sample,
            formula=existing_pg.formula,
            peak_annotation_file=paaf,
        )
        with self.assertRaises(MultiplePeakGroupRepresentation):
            new_pg.check_for_multiple_representations()
