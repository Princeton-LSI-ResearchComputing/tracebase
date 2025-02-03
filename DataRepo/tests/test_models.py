from datetime import datetime, timedelta
from unittest import skip, skipIf

import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.core.management import call_command
from django.db import IntegrityError
from django.db.models.deletion import RestrictedError
from django.test import override_settings, tag

from DataRepo.loaders import SampleTableLoader
from DataRepo.management.commands.legacy_load_study import Command as LSCommand
from DataRepo.models import (
    Animal,
    ArchiveFile,
    Compound,
    DataFormat,
    DataType,
    ElementLabel,
    Infusate,
    LCMethod,
    MaintainedModel,
    MSRunSample,
    MSRunSequence,
    PeakData,
    PeakGroup,
    Protocol,
    Researcher,
    Sample,
    Study,
    Tissue,
)
from DataRepo.models.researcher import UnknownResearcherError
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import (
    AggregatedErrors,
    AllMissingCompoundsErrors,
    AllMissingSamplesError,
    AllMissingTissuesErrors,
    ConflictingValueError,
    IsotopeParsingError,
    MissingCompoundsError,
    MissingSamplesError,
    MissingTissue,
    RequiredSampleValuesError,
    SheetMergeError,
    get_column_dupes,
    leaderboard_data,
    parse_infusate_name,
    parse_tracer_concentrations,
)
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs


class ExampleDataConsumer:
    def get_sample_test_dataframe(self):
        # making this a dataframe, if more rows are need for future tests, or we
        # switch to a file based test
        test_df = pd.DataFrame(
            {
                "Sample Name": ["bat-xz969"],
                "Date Collected": ["2020-11-18"],
                "Researcher Name": ["Xianfeng Zeng"],
                "Tissue": ["BAT"],
                "Animal ID": ["969"],
                "Animal Genotype": ["WT"],
                "Animal Body Weight": ["27.2"],
                "Infusate": ["C16:0-[13C16]"],
                "Infusion Rate": ["0.55"],
                "Tracer Concentrations": ["8.00"],
                "Animal State": ["Fasted"],
                "Study Name": ["obob_fasted"],
            }
        )
        return test_df

    def get_peak_group_test_dataframe(self):
        peak_data_df = pd.DataFrame(
            {
                "labeled_element": ["C", "C"],
                "labeled_count": [0, 1],
                "raw_abundance": [187608.7, 11873.74],
                "corrected_abundance": [203286.917004701, 0],
                "med_mz": [179.0558, 180.0592],
                "med_rt": [11.22489, 11.21671],
            }
        )
        peak_group_df = pd.DataFrame(
            {"name": ["glucose"], "formula": ["C6H12O6"], "peak_data": [peak_data_df]}
        )
        return peak_group_df


@override_settings(CACHES=settings.TEST_CACHES)
class StudyTests(TracebaseTestCase, ExampleDataConsumer):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    def setUp(self):
        super().setUp()
        # Get test data
        self.testdata = self.get_sample_test_dataframe()
        first = self.testdata.iloc[0]
        self.first = first

        # Create animal with tracer
        Compound.objects.create(name="C16:0", formula="C16H32O2", hmdb_id="HMDB0000220")
        tracer_concs = parse_tracer_concentrations(first["Tracer Concentrations"])
        infusate_data = parse_infusate_name(first["Infusate"], tracer_concs)
        (infusate, created) = Infusate.objects.get_or_create_infusate(infusate_data)
        self.infusate = infusate
        self.animal_treatment = Protocol.objects.create(
            name="treatment_1",
            description="treatment_1_desc",
            category=Protocol.ANIMAL_TREATMENT,
        )
        self.animal = Animal.objects.create(
            name=first["Animal ID"],
            feeding_status=first["Animal State"],
            body_weight=first["Animal Body Weight"],
            genotype=first["Animal Genotype"],
            infusate=self.infusate,
            infusion_rate=first["Infusion Rate"],
            treatment=self.animal_treatment,
        )

        # Create a sample from the animal
        self.tissue = Tissue.objects.create(name=first["Tissue"])
        self.sample = Sample.objects.create(
            name=first["Sample Name"],
            tissue=self.tissue,
            animal=self.animal,
            researcher=first["Researcher Name"],
            date=first["Date Collected"],
        )

        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")

        seq = MSRunSequence(
            researcher="John Doe",
            date=datetime.now(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
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
        self.msrs = MSRunSample(
            msrun_sequence=seq,
            sample=self.sample,
            polarity=MSRunSample.POSITIVE_POLARITY,
            ms_raw_file=rawrec,
            ms_data_file=mzxrec,
        )
        self.msrs.full_clean()
        self.msrs.save()

        self.ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
        self.accucor_format = DataFormat.objects.get(code="accucor")
        self.peak_annotation_file = ArchiveFile.objects.create(
            filename="test_data_file",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
            data_type=self.ms_peak_annotation,
            data_format=self.accucor_format,
        )

        self.peak_group_df = self.get_peak_group_test_dataframe()
        initial_peak_group = self.peak_group_df.iloc[0]
        self.peak_group = PeakGroup.objects.create(
            name=initial_peak_group["name"],
            formula=initial_peak_group["formula"],
            msrun_sample=self.msrs,
            peak_annotation_file=self.peak_annotation_file,
        )
        # actual code would have to more careful in retrieving compounds based
        # on the data's peak_group name
        compound_fk = Compound.objects.create(
            name=self.peak_group.name,
            formula=self.peak_group.formula,
            hmdb_id="HMDB0000122",
        )
        self.peak_group.compounds.add(compound_fk)
        self.peak_group.save()

        initial_peak_data_df = initial_peak_group["peak_data"]
        for index, row in initial_peak_data_df.iterrows():
            model = PeakData()
            model.peak_group = self.peak_group
            model.labeled_element = row["labeled_element"]
            model.labeled_count = row["labeled_count"]
            model.raw_abundance = row["raw_abundance"]
            model.corrected_abundance = row["corrected_abundance"]
            model.med_mz = row["med_mz"]
            model.med_rt = row["med_rt"]
            model.save()

    def test_tracer(self):
        self.assertIsNotNone(self.infusate.tracers.first().name)
        self.assertEqual(
            self.infusate.tracers.first().name, self.infusate.tracers.first()._name()
        )

    def test_tissue(self):
        self.assertEqual(self.tissue.name, self.first["Tissue"])

    def test_animal(self):
        self.assertEqual(self.animal.name, self.first["Animal ID"])
        self.assertEqual(
            self.animal.infusate.tracers.first().compound.name,
            "C16:0",
        )
        self.assertEqual(self.animal.treatment, self.animal_treatment)

    def test_study(self):
        """create study and associate animal"""
        # Create a study
        study = Study.objects.create(name=self.first["Study Name"])
        self.assertEqual(study.name, self.first["Study Name"])

        # add the animal to the study
        study.animals.add(self.animal)
        self.assertEqual(study.animals.get().name, self.animal.name)

    def test_sample(self):
        # Test sample relations
        self.assertEqual(self.sample.name, self.first["Sample Name"])
        self.assertEqual(self.sample.tissue.name, self.first["Tissue"])
        self.assertEqual(self.sample.animal.name, self.first["Animal ID"])
        # test time_collected exceeding MAXIMUM_VALID_TIME_COLLECTED fails
        with self.assertRaises(ValidationError):
            self.sample.time_collected = timedelta(days=91)
            # validation errors are raised upon cleaning
            self.sample.full_clean()
        # test time_collected exceeding MINIMUM_VALID_TIME_COLLECTED fails
        with self.assertRaises(ValidationError):
            self.sample.time_collected = timedelta(minutes=-2000)
            self.sample.full_clean()

    def test_peak_group(self):
        t_peak_group = PeakGroup.objects.get(name=self.peak_group.name)
        self.assertEqual(t_peak_group.peak_data.count(), 2)
        self.assertEqual(t_peak_group.name, self.peak_group.name)
        self.assertAlmostEqual(t_peak_group.total_abundance, 203286.917004701)

    def test_peak_group_atom_count(self):
        """PeakGroup atom_count"""
        t_peak_group = PeakGroup.objects.get(name=self.peak_group.name)
        self.assertEqual(t_peak_group.compounds.first().atom_count("C"), 6)

    def test_peak_group_unique_constraint(self):
        self.assertRaises(
            IntegrityError,
            lambda: PeakGroup.objects.create(
                name=self.peak_group.name, msrun_sample=self.msrs
            ),
        )


@override_settings(CACHES=settings.TEST_CACHES)
class DataLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "lc_methods")
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/small_obob2/protocols.tsv",
        )
        call_command(
            "load_tissues",
            infile="DataRepo/data/tests/small_obob2/tissues.tsv",
        )
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/small_obob2/compounds.tsv",
        )
        cls.ALL_COMPOUNDS_COUNT = 20

        # initialize some sample-table-dependent counters
        cls.ALL_SAMPLES_COUNT = 0
        cls.ALL_ANIMALS_COUNT = 0
        cls.ALL_STUDIES_COUNT = 0

        call_command(
            "legacy_load_animals_and_samples",
            sample_table_filename="DataRepo/data/tests/small_obob2/obob_samples_table.tsv",
            animal_table_filename="DataRepo/data/tests/small_obob2/obob_animals_table.tsv",
            table_headers="DataRepo/data/tests/small_obob2/sample_and_animal_tables_headers.yaml",
        )

        # from DataRepo/data/tests/small_obob2/obob_samples_table.tsv, not counting the header and BLANK samples
        cls.ALL_SAMPLES_COUNT += 10
        # not counting the header and the BLANK animal
        cls.ALL_OBOB_ANIMALS_COUNT = 7
        cls.ALL_ANIMALS_COUNT += cls.ALL_OBOB_ANIMALS_COUNT
        cls.ALL_STUDIES_COUNT += 1

        call_command(
            "legacy_load_samples",
            "DataRepo/data/tests/small_obob2/serum_lactate_sample_table.tsv",
            sample_table_headers="DataRepo/data/tests/small_obob2/sample_table_headers.yaml",
            skip_researcher_check=True,
        )
        # from DataRepo/data/tests/small_obob2/serum_lactate_sample_table.tsv, not counting the header
        cls.ALL_SAMPLES_COUNT += 5
        # not counting the header
        cls.ALL_ANIMALS_COUNT += 1
        cls.ALL_STUDIES_COUNT += 1

        call_command(
            "legacy_load_accucor_msruns",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            accucor_file="DataRepo/data/tests/small_obob2/obob_maven_6eaas_inf.xlsx",
            date="2021-04-29",
            researcher="Michael Neinast",
        )
        cls.PEAK_ANNOTATION_FILE_COUNT = 1
        cls.INF_COMPOUNDS_COUNT = 7
        cls.INF_SAMPLES_COUNT = 2
        cls.INF_PEAKDATA_ROWS = 38

        call_command(
            "legacy_load_accucor_msruns",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            accucor_file="DataRepo/data/tests/small_obob2/obob_maven_6eaas_serum.xlsx",
            date="2021-04-29",
            researcher="Michael Neinast",
        )
        cls.PEAK_ANNOTATION_FILE_COUNT += 1
        cls.SERUM_COMPOUNDS_COUNT = 13
        cls.SERUM_SAMPLES_COUNT = 4
        cls.SERUM_PEAKDATA_ROWS = 85

        # TODO: Skipping because the new PeakGroup unique constraint that removes MSRunSequence causes
        # MultiplePeakGroupRepresentation exceptions in the setUpTestData load for these tests
        # @tag("broken")
        # @skip("violates_new_peakgroup_unique_constraint")
        # # test load CSV file of corrected data, with no "original counterpart"
        # call_command(
        #     "legacy_load_accucor_msruns",
        #     lc_protocol_name="polar-HILIC-25-min",
        #     instrument="unknown",
        #     accucor_file="DataRepo/data/tests/small_obob2/obob_maven_6eaas_inf_corrected.csv",
        #     data_format="accucor",
        #     date="2021-10-14",
        #     researcher="Michael Neinast",
        # )
        # cls.PEAK_ANNOTATION_FILE_COUNT += 1
        # cls.NULL_ORIG_COMPOUNDS_COUNT = 7
        # cls.NULL_ORIG_SAMPLES_COUNT = 2
        # cls.NULL_ORIG_PEAKDATA_ROWS = 38
        cls.NULL_ORIG_COMPOUNDS_COUNT = 0
        cls.NULL_ORIG_SAMPLES_COUNT = 0
        cls.NULL_ORIG_PEAKDATA_ROWS = 0

        # defining a primary animal object for repeated tests
        cls.MAIN_SERUM_ANIMAL = Animal.objects.get(name="971")

        super().setUpTestData()

    def test_compounds_loaded(self):
        self.assertEqual(Compound.objects.all().count(), self.ALL_COMPOUNDS_COUNT)

    def test_samples_loaded(self):
        self.assertEqual(Sample.objects.all().count(), self.ALL_SAMPLES_COUNT)

        self.assertEqual(Animal.objects.all().count(), self.ALL_ANIMALS_COUNT)

        self.assertEqual(Study.objects.all().count(), self.ALL_STUDIES_COUNT)

        study = Study.objects.get(name="obob_fasted")
        self.assertEqual(study.animals.count(), self.ALL_OBOB_ANIMALS_COUNT)

        # MSRunSample should be equivalent to the samples
        MSRUNSAMPLE_COUNT = (
            self.INF_SAMPLES_COUNT
            + self.SERUM_SAMPLES_COUNT
            + self.NULL_ORIG_SAMPLES_COUNT
        )
        self.assertEqual(MSRUNSAMPLE_COUNT, MSRunSample.objects.count())

    def test_sample_data(self):
        sample = Sample.objects.get(name="bat-xz969")
        self.assertEqual(sample.time_collected, timedelta(minutes=150))
        self.assertEqual(sample.researcher, "Xianfeng Zeng")
        self.assertEqual(sample.animal.name, "969")
        self.assertEqual(sample.tissue.name, "brown_adipose_tissue")

    @tag("serum")
    def test_sample_is_serum(self):
        serum = Sample.objects.get(name="serum-xz971")
        self.assertTrue(serum.is_serum_sample)
        nonserum = Sample.objects.get(name="bat-xz969")
        self.assertFalse(nonserum.is_serum_sample)

    # TODO: Skipping because the new PeakGroup unique constraint that removes MSRunSequence causes
    # MultiplePeakGroupRepresentation exceptions in the setUpTestData load for these tests
    @tag("broken")
    @skip("violates_new_peakgroup_unique_constraint")
    def test_peak_groups_set_loaded(self):
        # 2 peak group sets , 1 for each call to legacy_load_accucor_msruns
        self.assertEqual(
            ArchiveFile.objects.all().count(), self.PEAK_ANNOTATION_FILE_COUNT
        )
        self.assertTrue(
            ArchiveFile.objects.filter(filename="obob_maven_6eaas_inf.xlsx").exists()
        )
        self.assertTrue(
            ArchiveFile.objects.filter(filename="obob_maven_6eaas_serum.xlsx").exists()
        )
        self.assertTrue(
            ArchiveFile.objects.filter(
                filename="obob_maven_6eaas_inf_corrected.csv"
            ).exists()
        )

    def test_peak_groups_multiple_compounds(self):
        """
        Test that a peakgroup that is named with two compounds separated by a
        slash ("/") is properly associated with two compounds
        """
        pg = PeakGroup.objects.filter(name="citrate/isocitrate").first()
        self.assertEqual(pg.compounds.count(), 2)
        self.assertEqual(pg.compounds.first().name, "citrate")
        self.assertEqual(pg.compounds.last().name, "isocitrate")

    def test_animal_tracers(self):
        a = Animal.objects.get(name="969")
        c = Compound.objects.get(name="C16:0")
        self.assertEqual(a.infusate.tracers.first().compound, c)
        self.assertEqual(
            a.infusate.tracers.first().labels.first().element, ElementLabel.CARBON
        )
        self.assertEqual(a.infusate.tracers.count(), 1)
        self.assertEqual(a.infusate.tracers.first().labels.count(), 1)
        self.assertEqual(a.sex, None)

    def test_animal_treatments_loaded(self):
        a = Animal.objects.get(name="969")
        self.assertEqual(a.treatment, None)
        a = Animal.objects.get(name="exp024f_M2")
        self.assertEqual(a.treatment.name, "T3")
        self.assertEqual(
            a.treatment.description,
            "For protocol's full text, please consult Michael Neinast.",
        )

    @tag("serum")
    def test_animal_serum_sample_methods(self):
        animal = self.MAIN_SERUM_ANIMAL
        serum_samples = animal.samples.filter(
            tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        )
        self.assertEqual(serum_samples.count(), 1)
        last_serum_sample = animal.last_serum_sample
        self.assertEqual(last_serum_sample.name, "serum-xz971")
        self.assertEqual(last_serum_sample.name, serum_samples.last().name)

    @tag("serum")
    def test_missing_time_collected_warning(self):
        last_serum_sample = self.MAIN_SERUM_ANIMAL.last_serum_sample
        # pretend the time_collected did not exist
        last_serum_sample.time_collected = None
        with self.assertWarns(UserWarning):
            # The auto-update of the MaintainedField generates the warning
            last_serum_sample.save()
            # refeshed_animal = Animal.objects.get(name="971")
            # last_serum_sample = refeshed_animal.last_serum_sample

    def test_restricted_animal_treatment_deletion(self):
        treatment = Animal.objects.get(name="exp024f_M2").treatment
        with self.assertRaises(RestrictedError):
            # test a restricted deletion
            treatment.delete()

    def test_peak_groups_loaded(self):
        # inf data file: compounds * samples
        INF_PEAKGROUP_COUNT = self.INF_COMPOUNDS_COUNT * self.INF_SAMPLES_COUNT
        # serum data file: compounds * samples
        SERUM_PEAKGROUP_COUNT = self.SERUM_COMPOUNDS_COUNT * self.SERUM_SAMPLES_COUNT
        # null original data file: compounds * samples
        NULL_ORIG_PEAKGROUP_COUNT = (
            self.NULL_ORIG_COMPOUNDS_COUNT * self.NULL_ORIG_SAMPLES_COUNT
        )

        self.assertEqual(
            INF_PEAKGROUP_COUNT + SERUM_PEAKGROUP_COUNT + NULL_ORIG_PEAKGROUP_COUNT,
            PeakGroup.objects.count(),
            msg=(
                f"{INF_PEAKGROUP_COUNT} + {SERUM_PEAKGROUP_COUNT} + {NULL_ORIG_PEAKGROUP_COUNT} "
                f"!= {PeakGroup.objects.count()}"
            ),
        )

    def test_peak_data_loaded(self):
        # inf data file: PeakData rows * samples
        INF_PEAKDATA_COUNT = self.INF_PEAKDATA_ROWS * self.INF_SAMPLES_COUNT
        # serum data file: PeakData rows * samples
        SERUM_PEAKDATA_COUNT = self.SERUM_PEAKDATA_ROWS * self.SERUM_SAMPLES_COUNT
        # null original version of INF data
        NULL_ORIG_PEAKDATA_COUNT = (
            self.NULL_ORIG_PEAKDATA_ROWS * self.NULL_ORIG_SAMPLES_COUNT
        )

        self.assertEqual(
            INF_PEAKDATA_COUNT + SERUM_PEAKDATA_COUNT + NULL_ORIG_PEAKDATA_COUNT,
            PeakData.objects.count(),
            msg=(
                f"{INF_PEAKDATA_COUNT} + {SERUM_PEAKDATA_COUNT} + {NULL_ORIG_PEAKDATA_COUNT} "
                f"!= {PeakData.objects.count()}"
            ),
        )

    def test_peak_group_peak_data_2(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="histidine")
            .filter(msrun_sample__sample__name="serum-xz971")
            .get()
        )
        # There should be a peak_data for each label count 0-6
        self.assertEqual(peak_group.peak_data.count(), 7)
        # The peak_data for labeled_count==2 is missing, thus values should be 0
        peak_data = peak_group.peak_data.filter(labels__count=2).get()
        self.assertEqual(peak_data.raw_abundance, 0)
        self.assertEqual(peak_data.med_mz, 0)
        self.assertEqual(peak_data.med_rt, 0)
        self.assertEqual(peak_data.corrected_abundance, 0)

    def test_peak_group_peak_data_3(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="histidine")
            .filter(msrun_sample__sample__name="serum-xz971")
            .get()
        )

        peak_data = peak_group.peak_data.filter(labels__count=5).get()
        self.assertAlmostEqual(peak_data.raw_abundance, 1356.587)
        self.assertEqual(peak_data.corrected_abundance, 0)

    @MaintainedModel.no_autoupdates()
    def test_dupe_sample_load_fails(self):
        # Insert the dupe sample.  Samples are required to pre-exist for the accucor loader.
        sample = Sample(
            name="tst-dupe1",
            researcher="Michael",
            time_collected=timedelta(minutes=5),
            animal=Animal.objects.all()[0],
            tissue=Tissue.objects.all()[0],
        )
        sample.full_clean()
        sample.save()

        with self.assertRaises(ValidationError):
            call_command(
                "legacy_load_accucor_msruns",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
                accucor_file="DataRepo/data/tests/small_obob2/obob_maven_6eaas_inf_sample_dupe.xlsx",
                date="2021-08-20",
                researcher="Michael",
            )

    def test_dupe_samples_not_loaded(self):
        self.assertEqual(Sample.objects.filter(name__exact="tst-dupe1").count(), 0)

    @MaintainedModel.no_autoupdates()
    def test_ls_new_researcher_and_aggregate_errors(self):
        # The error string must include:
        #   The new researcher is in the error
        #   Hidden flag is suggested
        #   Existing researchers are shown
        exp_err = (
            "1 researchers: [Han Solo] out of 1 do not exist in the database.  Current researchers are:\n\tMichael "
            "Neinast\n\tXianfeng Zeng\nIf all researchers are valid new researchers, add --skip-researcher-check to "
            "your command."
        )
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "legacy_load_samples",
                "DataRepo/data/tests/small_obob2/serum_lactate_sample_table_han_solo.tsv",
                sample_table_headers="DataRepo/data/tests/small_obob2/sample_table_headers.yaml",
            )
        aes = ar.exception
        ures = [e for e in aes.exceptions if isinstance(e, UnknownResearcherError)]
        self.assertEqual(1, len(ures))
        self.assertIn(
            exp_err,
            str(ures[0]),
        )
        # There are 5 conflicts due to this file being a copy of a file already loaded, with the reseacher changed.
        self.assertEqual(6, len(aes.exceptions))

    @MaintainedModel.no_autoupdates()
    def test_ls_new_researcher_confirmed(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "legacy_load_samples",
                "DataRepo/data/tests/small_obob2/serum_lactate_sample_table_han_solo.tsv",
                sample_table_headers="DataRepo/data/tests/small_obob2/sample_table_headers.yaml",
                skip_researcher_check=True,
            )
        aes = ar.exception
        # Test that no researcher exception occurred
        ures = [e for e in aes.exceptions if isinstance(e, UnknownResearcherError)]
        self.assertEqual(0, len(ures))
        # There are 5 ConflictingValueErrors expected (Same samples with different researcher: Han Solo)
        cves = [e for e in aes.exceptions if isinstance(e, ConflictingValueError)]
        self.assertIn("Han Solo", str(cves[0]))
        self.assertEqual(5, len(cves))
        # There are 24 expected errors total
        self.assertEqual(5, len(aes.exceptions))
        self.assertIn(
            "5 exceptions occurred, including type(s): [ConflictingValueError].",
            str(ar.exception),
        )

    @tag("fcirc")
    def test_peakgroup_from_serum_sample_false(self):
        # get a tracer compound from a non-serum sample
        sample = Sample.objects.get(name="Liv-xz982")
        pgl = sample.msrun_samples.last().peak_groups.last().labels.first()
        with self.assertWarns(UserWarning):
            self.assertFalse(pgl.from_serum_sample)

    @tag("synonym_data_loading")
    @MaintainedModel.no_autoupdates()
    def test_valid_synonym_accucor_load(self):
        # this file contains 1 valid synonym for glucose, "dextrose"
        call_command(
            "legacy_load_accucor_msruns",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            accucor_file="DataRepo/data/tests/small_obob2/obob_maven_6eaas_inf_corrected_valid_syn.csv",
            data_format="accucor",
            date="2021-11-19",
            researcher="Michael Neinast",
        )

        self.assertTrue(
            ArchiveFile.objects.filter(
                filename="obob_maven_6eaas_inf_corrected_valid_syn.csv"
            ).exists()
        )
        peak_group = PeakGroup.objects.filter(
            peak_annotation_file__filename="obob_maven_6eaas_inf_corrected_valid_syn.csv"
        ).first()
        self.assertEqual(peak_group.name, "dextrose")
        self.assertEqual(peak_group.compounds.first().name, "glucose")

    @tag("synonym_data_loading")
    @MaintainedModel.no_autoupdates()
    def test_invalid_synonym_accucor_load(self):
        with self.assertRaises(
            AggregatedErrors,
            msg="Should complain about a missing compound (due to a synonym renamed to 'table sugar')",
        ) as ar:
            # this file contains 1 invalid synonym for glucose "table sugar"
            call_command(
                "legacy_load_accucor_msruns",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
                accucor_file="DataRepo/data/tests/small_obob2/obob_maven_6eaas_inf_corrected_invalid_syn.csv",
                data_format="accucor",
                date="2021-11-18",
                researcher="Michael Neinast",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(
            isinstance(aes.exceptions[0], MissingCompoundsError),
            msg=f"Exception [{type(aes.exceptions[0]).__name__}: {aes.exceptions[0]}] is MissingCompounds?",
        )
        exp_str = "1 compounds were not found in the database:\n\ttable sugar"
        self.assertIn(
            exp_str,
            str(aes.exceptions[0]),
            msg=f"Exception must contain {exp_str}",
        )


@override_settings(CACHES=settings.TEST_CACHES)
class AnimalAndSampleLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        if 1 != len(all_coordinators):
            raise ValueError(
                f"Before setting up test data, there are {len(all_coordinators)} MaintainedModelCoordinators."
            )
        if all_coordinators[0].auto_update_mode != "always":
            raise ValueError(
                "Before setting up test data, the default coordinator is not in immediate autoupdate mode."
            )
        if 0 != all_coordinators[0].buffer_size():
            raise ValueError(
                f"Before setting up test data, there are {all_coordinators[0].buffer_size()} items in the buffer."
            )

        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/small_obob/small_obob_study_prerequisites.yaml",
        )

        if 0 != all_coordinators[0].buffer_size():
            raise ValueError(
                f"legacy_load_study left {all_coordinators[0].buffer_size()} items in the buffer."
            )

        super().setUpTestData()

    def setUp(self):
        # Load data and buffer autoupdates before each test
        MaintainedModel._reset_coordinators()
        super().setUp()

    def tearDown(self):
        self.assert_coordinator_state_is_initialized()
        super().tearDown()

    def assert_coordinator_state_is_initialized(
        self, msg="MaintainedModelCoordinators are in the default state."
    ):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        # Make sure there is only the default coordinator
        self.assertEqual(
            1, len(all_coordinators), msg=msg + "  The coordinator_stack is empty."
        )
        # Make sure that its mode is "always"
        self.assertEqual(
            "always",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode should be 'always'.",
        )
        # Make sure that the buffer is empty to start
        for coordinator in all_coordinators:
            self.assertEqual(
                0, coordinator.buffer_size(), msg=msg + "  The buffer is empty."
            )

    @MaintainedModel.no_autoupdates()
    def test_animal_and_sample_load_xlsx(self):
        # initialize some sample-table-dependent counters
        SAMPLES_COUNT = 16
        ANIMALS_COUNT = 1
        STUDIES_COUNT = 1

        Study.objects.create(name="Small OBOB")
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )
        call_command(
            "load_animals",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_samples",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        # call_command(
        #     "legacy_load_animals_and_samples",
        #     animal_and_sample_table_filename=(
        #         "DataRepo/data/tests/small_obob/"
        #         "small_obob_animal_and_sample_table.xlsx"
        #     ),
        #     dry_run=False,
        # )

        self.assertEqual(Sample.objects.all().count(), SAMPLES_COUNT)
        self.assertEqual(Animal.objects.all().count(), ANIMALS_COUNT)
        self.assertEqual(Study.objects.all().count(), STUDIES_COUNT)

        study = Study.objects.get(name="Small OBOB")
        self.assertEqual(study.animals.count(), ANIMALS_COUNT)

    # TODO: Obsolete, delete
    # def test_animal_and_sample_load_in_dry_run(self):
    #     # Load some data to ensure that none of it changes during the actual test
    #     call_command(
    #         "legacy_load_animals_and_samples",
    #         animal_and_sample_table_filename=(
    #             "DataRepo/data/tests/small_multitracer/animal_sample_table.xlsx"
    #         ),
    #         skip_researcher_check=True,
    #     )

    #     pre_load_counts = self.get_record_counts()
    #     pre_load_maintained_values = MaintainedModel.get_all_maintained_field_values(
    #         "DataRepo.models"
    #     )
    #     self.assertGreater(
    #         len(pre_load_maintained_values.keys()),
    #         0,
    #         msg="Ensure there is data in the database before the test",
    #     )
    #     self.assert_coordinator_state_is_initialized()

    #     with self.assertRaises(DryRun):
    #         call_command(
    #             "legacy_load_animals_and_samples",
    #             animal_and_sample_table_filename=(
    #                 "DataRepo/data/tests/small_obob/"
    #                 "small_obob_animal_and_sample_table.xlsx"
    #             ),
    #             dry_run=True,
    #         )

    #     post_load_maintained_values = MaintainedModel.get_all_maintained_field_values(
    #         "DataRepo.models"
    #     )
    #     post_load_counts = self.get_record_counts()

    #     self.assertEqual(
    #         pre_load_counts,
    #         post_load_counts,
    #         msg="DryRun mode doesn't change any table's record count.",
    #     )
    #     self.assertEqual(
    #         pre_load_maintained_values,
    #         post_load_maintained_values,
    #         msg="DryRun mode doesn't autoupdate.",
    #     )

    def test_get_column_dupes(self):
        col_keys = ["Sample Name", "Study Name"]
        data = [
            {"Sample Name": "q2", "Study Name": "TCA Flux"},
            {"Sample Name": "q2", "Study Name": "TCA Flux"},
        ]
        dupes, rows = get_column_dupes(data, col_keys)
        self.assertEqual(
            {
                "Sample Name: [q2], Study Name: [TCA Flux]": {
                    "rowidxs": [0, 1],
                    "vals": {
                        "Sample Name": "q2",
                        "Study Name": "TCA Flux",
                    },
                },
            },
            dupes,
        )
        self.assertEqual([0, 1], rows)

    @MaintainedModel.no_autoupdates()
    def test_empty_row(self):
        """
        Ensures SheetMergeError doesn't include completely empty rows - asserted by an animal sample table with an
        empty row raising no error at all.

        Also ensures RequiredSampleValuesError doesn't include completely empty rows
        """
        call_command(
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_empty_row.xlsx"
            ),
        )

    @MaintainedModel.no_autoupdates()
    def test_required_sample_values_error_ignores_emptyanimal_animalsheet(self):
        """
        Ensures RequiredSampleValuesError doesn't include rows with a missing animal ID (but has other values).
        Note, this should raise a SheetMergeError
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "legacy_load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/data/tests/small_obob/"
                    "small_obob_animal_and_sample_table_empty_animalid_in_animalsheet.xlsx"
                ),
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], SheetMergeError))
        self.assertEqual(1, len(aes.exceptions[0].row_idxs))
        self.assertEqual("Animal ID", aes.exceptions[0].animal_col_name)

        # Since there's a sheet merge error, the sample row with the empty animal ID will be merged with every empty
        # row in the animal sheet.  This test ensures that that logic is correct and that the user is warned about the
        # row number inaccuracy in this case.
        self.assertEqual(16, aes.exceptions[0].row_idxs[0])
        self.assertIn("row numbers can be inaccurate", str(aes.exceptions[0]))

    @MaintainedModel.no_autoupdates()
    def test_required_sample_values_error_ignores_emptyanimal_samplesheet(self):
        """
        Ensures RequiredSampleValuesError doesn't include rows with a missing animal ID (but has other values).
        Note, this should raise a SheetMergeError
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "legacy_load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/data/tests/small_obob/"
                    "small_obob_animal_and_sample_table_empty_animalid_in_samplesheet.xlsx"
                ),
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], SheetMergeError))
        self.assertEqual(1, len(aes.exceptions[0].row_idxs))
        self.assertEqual("Animal ID", aes.exceptions[0].animal_col_name)

        # Since there's a sheet merge error, the sample row with the empty animal ID will be merged with every empty
        # row in the animal sheet.  This test ensures that that logic is correct and that the user is warned about the
        # row number inaccuracy in this case.
        self.assertEqual(18, aes.exceptions[0].row_idxs[0])
        self.assertIn("row numbers can be inaccurate", str(aes.exceptions[0]))

    @tag("broken")
    @skipIf(True, "This test demonstrates a current bug.")
    @MaintainedModel.no_autoupdates()
    def test_unraised_samplesheet_error_case(self):
        """
        This test demonstrates a current bug.  If there are no empty rows between populated rows in the Animals sheet,
        then a row in the Samples sheet that has an empty Animal ID is completely ignored and that sample is never
        loaded.  This should generate an error, but it does not.
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "legacy_load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/data/tests/small_obob/"
                    "small_obob_animal_and_sample_table_empty_animalid_in_samplesheet_silent.xlsx"
                ),
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], SheetMergeError))
        self.assertEqual(1, len(aes.exceptions[0].row_idxs))
        self.assertEqual("Animal ID", aes.exceptions[0].animal_col_name)

        # Since there's a sheet merge error, the sample row with the empty animal ID will be merged with every empty
        # row in the animal sheet.  This test ensures that that logic is correct and that the user is warned about the
        # row number inaccuracy in this case.
        self.assertEqual(18, aes.exceptions[0].row_idxs[0])
        self.assertIn("row numbers can be inaccurate", str(aes.exceptions[0]))

    @MaintainedModel.no_autoupdates()
    def test_check_required_values(self):
        """
        Check that missing required vals are added to stl.missing_values
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "legacy_load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_missing_rqd_vals.xlsx"
                ),
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], RequiredSampleValuesError))
        self.assertEqual(11, len(aes.exceptions[0].missing.keys()))
        self.assertEqual(
            {
                "Sample Name": {"rows": [16], "animals": ["971"]},
                "Date Collected": {"rows": [16], "animals": ["971"]},
                "Researcher Name": {"rows": [16], "animals": ["971"]},
                "Collection Time": {"rows": [16], "animals": ["971"]},
                "Study Name": {"rows": [17, 18, 19, 20], "animals": ["972"]},
                "Animal Body Weight": {"rows": [17, 18, 19, 20], "animals": ["972"]},
                "Animal Genotype": {"rows": [17, 18, 19, 20], "animals": ["972"]},
                "Feeding Status": {"rows": [17, 18, 19, 20], "animals": ["972"]},
                "Infusate": {"rows": [17, 18, 19, 20], "animals": ["972"]},
                "Infusion Rate": {"rows": [17, 18, 19, 20], "animals": ["972"]},
                "Tracer Concentrations": {"rows": [17, 18, 19, 20], "animals": ["972"]},
            },
            aes.exceptions[0].missing,
        )
        self.assertIn(
            "row numbers could reflect a sheet merge and may be inaccurate",
            str(aes.exceptions[0]),
        )

    def test_strip_units(self):
        stl = SampleTableLoader()
        stripped_val = stl.strip_units("3.3 ul/m/g", "ANIMAL_INFUSION_RATE", 3)
        stripped_val = stl.strip_units("3.3 ul/m/g", "ANIMAL_INFUSION_RATE", 4)
        self.assertEqual("3.3", stripped_val)
        self.assertEqual(0, len(stl.units_errors.keys()))

    def test_strip_units_errors(self):
        stl = SampleTableLoader()
        stripped_val = stl.strip_units("3.3 non/sense", "ANIMAL_INFUSION_RATE", 3)
        stripped_val = stl.strip_units("3.3 non/sense", "ANIMAL_INFUSION_RATE", 4)
        self.assertEqual(
            "3.3", stripped_val, msg="Still strips to avoid subsequent errors"
        )
        self.assertEqual(
            {
                "Infusion Rate": {
                    "example_val": "3.3 non/sense",
                    "expected": "ul/m/g",
                    "rows": [5, 6],
                    "units": "non/sense",
                },
            },
            stl.units_errors,
        )


@override_settings(CACHES=settings.TEST_CACHES)
@tag("load_study")
class StudyLoadingTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

    @classmethod
    @MaintainedModel.no_autoupdates()
    def setUpTestData(cls):
        call_command("loaddata", "lc_methods")
        call_command("legacy_load_study", "DataRepo/data/tests/tissues/loading.yaml")
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/small_obob/small_obob_compounds.tsv",
        )
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/small_obob/small_obob_protocols.tsv",
        )
        Study.objects.create(name="Small OBOB")
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )
        call_command(
            "load_animals",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_samples",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_blank_sample.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            skip_samples=("blank"),
        )
        # call_command(
        #     "legacy_load_study",
        #     "DataRepo/data/tests/small_obob/small_obob_study_params.yaml",
        # )
        cls.COMPOUNDS_COUNT = 2
        cls.SAMPLES_COUNT = 14
        cls.PEAKDATA_ROWS = 11

        super().setUpTestData()

    def test_load_small_obob_study(self):
        self.assertEqual(
            PeakGroup.objects.all().count(), self.COMPOUNDS_COUNT * self.SAMPLES_COUNT
        )
        self.assertEqual(
            PeakData.objects.all().count(), self.PEAKDATA_ROWS * self.SAMPLES_COUNT
        )

    def test_researcher_dne(self):
        with self.assertRaises(ObjectDoesNotExist):
            Researcher(name="New Researcher")

    def test_researcher_eq(self):
        r1 = Researcher(name="Xianfeng Zeng")
        r2 = Researcher(name="Xianfeng Zeng")
        self.assertEqual(r1, r2)

    def test_researcher_studies(self):
        researcher = Researcher(name="Xianfeng Zeng")
        self.assertEqual(researcher.studies.count(), 1)

    def test_researcher_animals(self):
        researcher = Researcher(name="Xianfeng Zeng")
        self.assertEqual(researcher.animals.count(), 1)

    def test_researcher_peakgroups(self):
        researcher = Researcher(name="Xianfeng Zeng")
        self.assertEqual(
            researcher.peakgroups.count(), self.COMPOUNDS_COUNT * self.SAMPLES_COUNT
        )

    def test_leaderboards(self):
        expected_leaderboard = {
            "studies_leaderboard": [
                (Researcher(name="Xianfeng Zeng"), 1),
            ],
            "animals_leaderboard": [
                (Researcher(name="Xianfeng Zeng"), 1),
            ],
            "peakgroups_leaderboard": [
                (
                    Researcher(name="Xianfeng Zeng"),
                    self.COMPOUNDS_COUNT * self.SAMPLES_COUNT,
                ),
            ],
        }
        self.maxDiff = None
        self.assertDictEqual(expected_leaderboard, leaderboard_data())

    def test_create_grouped_exceptions(self):
        """
        Assures that every AllMissingTissues, MissingCompounds, and MissingSamplesError exception brings about the
        creation of a consolidated AllMissing{Tissues,Compounds,Samples} exceptions and that the original exceptions are
        changed to a warning status (technically - if they are the only exception)
        """
        lsc = LSCommand()
        exceptions = [
            AllMissingTissuesErrors(
                [
                    MissingTissue(tissue_name="spleen", column="Tissue", rownum=1),
                    MissingTissue(tissue_name="spleen", column="Tissue", rownum=2),
                ]
            ),
            MissingCompoundsError({"lysine": {"formula": "C2N2O2", "rownums": [3, 4]}}),
            MissingSamplesError(["a", "b"]),
        ]
        aes = AggregatedErrors(exceptions=exceptions)
        lsc.package_group_exceptions(aes, "accucor.xlsx")
        lsc.create_grouped_exceptions()

        # There should be 4 keys (one for the file "accucor.xlsx", which should have been changed to a warning and 3
        # different group exceptions (AllMissingSamples, AllMissingCompounds, and AllMissingTissues) should have been
        # added
        self.assertEqual(4, len(lsc.load_statuses.statuses.keys()))
        # 3 errors (AllMissingSamples, AllMissingCompounds, and AllMissingTissues)
        self.assertEqual(
            3,
            lsc.load_statuses.num_errors,
            msg=(
                "There should be 3 errors (6 exceptions total). Exceptions: "
                f"{', '.join([type(e).__name__ for e in aes.exceptions])}"
            ),
        )
        # The file had 3 errors that should have been changed to warnings (MissingSamplesError, MissingCompounds, and
        # MissingTissues)
        self.assertEqual(3, lsc.load_statuses.num_warnings)

        # Each of these keys should have been added as error categories
        self.assertIn(
            "All Samples Present in Sample Table File",
            lsc.load_statuses.statuses.keys(),
        )
        self.assertIn(
            "All Tissues Exist in the Database", lsc.load_statuses.statuses.keys()
        )
        self.assertIn(
            "All Compounds Exist in the Database", lsc.load_statuses.statuses.keys()
        )
        self.assertIn("accucor.xlsx", lsc.load_statuses.statuses.keys())

        # Each one should be designated as occurring at the top of the error output except the file
        self.assertTrue(
            lsc.load_statuses.statuses["All Samples Present in Sample Table File"][
                "top"
            ]
        )
        self.assertTrue(
            lsc.load_statuses.statuses["All Tissues Exist in the Database"]["top"]
        )
        self.assertTrue(
            lsc.load_statuses.statuses["All Compounds Exist in the Database"]["top"]
        )
        self.assertFalse(lsc.load_statuses.statuses["accucor.xlsx"]["top"])

        # Number of errors in the MultiLoadStatus objects is correct (the accucor file's errors were changed to
        # warnings)
        self.assertEqual(
            1,
            lsc.load_statuses.statuses["All Samples Present in Sample Table File"][
                "aggregated_errors"
            ].num_errors,
        )
        self.assertEqual(
            1,
            lsc.load_statuses.statuses["All Tissues Exist in the Database"][
                "aggregated_errors"
            ].num_errors,
        )
        self.assertEqual(
            1,
            lsc.load_statuses.statuses["All Compounds Exist in the Database"][
                "aggregated_errors"
            ].num_errors,
        )
        self.assertEqual(
            0,
            lsc.load_statuses.statuses["accucor.xlsx"]["aggregated_errors"].num_errors,
        )

        # Number of warnings in the MultiLoadStatus objects is correct (the accucor file's errors were changed to
        # warnings)
        self.assertEqual(
            0,
            lsc.load_statuses.statuses["All Samples Present in Sample Table File"][
                "aggregated_errors"
            ].num_warnings,
        )
        self.assertEqual(
            0,
            lsc.load_statuses.statuses["All Tissues Exist in the Database"][
                "aggregated_errors"
            ].num_warnings,
        )
        self.assertEqual(
            0,
            lsc.load_statuses.statuses["All Compounds Exist in the Database"][
                "aggregated_errors"
            ].num_warnings,
        )
        self.assertEqual(
            3,
            lsc.load_statuses.statuses["accucor.xlsx"][
                "aggregated_errors"
            ].num_warnings,
        )

        # Every error/warning group is inside an AggregatedErrors object
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["All Samples Present in Sample Table File"][
                    "aggregated_errors"
                ],
                AggregatedErrors,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["All Tissues Exist in the Database"][
                    "aggregated_errors"
                ],
                AggregatedErrors,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["All Compounds Exist in the Database"][
                    "aggregated_errors"
                ],
                AggregatedErrors,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["accucor.xlsx"]["aggregated_errors"],
                AggregatedErrors,
            ),
        )

        # The number of exceptions in each AggregatedErrors object is correct
        self.assertEqual(
            1,
            len(
                lsc.load_statuses.statuses["All Samples Present in Sample Table File"][
                    "aggregated_errors"
                ].exceptions
            ),
        )
        self.assertEqual(
            1,
            len(
                lsc.load_statuses.statuses["All Tissues Exist in the Database"][
                    "aggregated_errors"
                ].exceptions
            ),
        )
        self.assertEqual(
            1,
            len(
                lsc.load_statuses.statuses["All Compounds Exist in the Database"][
                    "aggregated_errors"
                ].exceptions
            ),
        )
        self.assertEqual(
            3,
            len(
                lsc.load_statuses.statuses["accucor.xlsx"][
                    "aggregated_errors"
                ].exceptions
            ),
        )

        # The exceptions contained in the AggregatedErrors objects are correct
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["All Samples Present in Sample Table File"][
                    "aggregated_errors"
                ].exceptions[0],
                AllMissingSamplesError,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["All Tissues Exist in the Database"][
                    "aggregated_errors"
                ].exceptions[0],
                AllMissingTissuesErrors,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["All Compounds Exist in the Database"][
                    "aggregated_errors"
                ].exceptions[0],
                AllMissingCompoundsErrors,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["accucor.xlsx"][
                    "aggregated_errors"
                ].exceptions[0],
                AllMissingTissuesErrors,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["accucor.xlsx"][
                    "aggregated_errors"
                ].exceptions[1],
                MissingCompoundsError,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["accucor.xlsx"][
                    "aggregated_errors"
                ].exceptions[2],
                MissingSamplesError,
            ),
        )

    @MaintainedModel.no_autoupdates()
    def test_singly_labeled_isocorr_study(self):
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/singly_labeled_isocorr/loading.yaml",
            verbosity=2,
        )

    @MaintainedModel.no_autoupdates()
    def test_multi_tracer_isocorr_study(self):
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/multiple_tracers/loading.yaml",
        )

    @MaintainedModel.no_autoupdates()
    def test_multi_label_isocorr_study(self):
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/multiple_labels/loading.yaml",
        )


@override_settings(CACHES=settings.TEST_CACHES)
@tag("animal")
@tag("loading")
class AnimalLoadingTests(TracebaseTestCase):
    """Tests parsing various Animal attributes"""

    @classmethod
    @MaintainedModel.no_autoupdates()
    def setUpTestData(cls):
        call_command("loaddata", "lc_methods")
        call_command("legacy_load_study", "DataRepo/data/tests/protocols/loading.yaml")
        call_command("legacy_load_study", "DataRepo/data/tests/tissues/loading.yaml")
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/consolidated_tracebase_compound_list.tsv",
        )

        super().setUpTestData()

    @MaintainedModel.no_autoupdates()
    def test_labeled_element_parsing(self):
        call_command(
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/small_obob/animal_sample_table_labeled_elements.xlsx"
            ),
        )
        self.assertEqual(
            Animal.objects.get(name="test_animal_1")
            .infusate.tracers.first()
            .labels.first()
            .element,
            "C",
        )
        self.assertEqual(
            Animal.objects.get(name="test_animal_2")
            .infusate.tracers.first()
            .labels.first()
            .element,
            "N",
        )
        self.assertEqual(
            Animal.objects.get(name="test_animal_3")
            .infusate.tracers.first()
            .labels.first()
            .element,
            "H",
        )
        self.assertEqual(
            Animal.objects.get(name="test_animal_4")
            .infusate.tracers.first()
            .labels.first()
            .element,
            "O",
        )
        self.assertEqual(
            Animal.objects.get(name="test_animal_5")
            .infusate.tracers.first()
            .labels.first()
            .element,
            "S",
        )

    @MaintainedModel.no_autoupdates()
    def test_labeled_element_parsing_invalid(self):
        with self.assertRaisesMessage(
            IsotopeParsingError, "Encoded isotopes: [13Invalid6] cannot be parsed."
        ):
            call_command(
                "legacy_load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/data/tests/small_obob/animal_sample_table_labeled_elements_invalid.xlsx"
                ),
            )
