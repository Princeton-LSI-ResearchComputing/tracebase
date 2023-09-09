from datetime import datetime, timedelta
from unittest import skipIf

import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.core.management import call_command
from django.db import IntegrityError
from django.db.models.deletion import RestrictedError
from django.test import override_settings, tag

from DataRepo.management.commands.load_study import Command as LoadStudyCommand
from DataRepo.models import (
    Animal,
    AnimalLabel,
    Compound,
    ElementLabel,
    Infusate,
    MaintainedModel,
    MSRun,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupLabel,
    PeakGroupSet,
    Protocol,
    Researcher,
    Sample,
    Study,
    Tissue,
)
from DataRepo.models.hier_cached_model import set_cache
from DataRepo.models.peak_group_label import NoCommonLabel
from DataRepo.models.researcher import UnknownResearcherError
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import (
    AccuCorDataLoader,
    AggregatedErrors,
    AllMissingCompounds,
    AllMissingSamples,
    AllMissingTissues,
    ConflictingValueError,
    DryRun,
    DupeCompoundIsotopeCombos,
    IsotopeObservationData,
    IsotopeObservationParsingError,
    IsotopeParsingError,
    MissingCompounds,
    MissingSamplesError,
    MissingTissues,
    RequiredSampleValuesError,
    SampleTableLoader,
    SheetMergeError,
    leaderboard_data,
    parse_infusate_name,
    parse_tracer_concentrations,
)


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

        self.protocol = Protocol.objects.create(
            name="p1",
            description="p1desc",
            category=Protocol.MSRUN_PROTOCOL,
        )
        self.msrun = MSRun.objects.create(
            researcher="John Doe",
            date=datetime.now(),
            protocol=self.protocol,
            sample=self.sample,
        )

        self.peak_group_set = PeakGroupSet.objects.create(
            filename="testing_dataset_file"
        )

        self.peak_group_df = self.get_peak_group_test_dataframe()
        initial_peak_group = self.peak_group_df.iloc[0]
        self.peak_group = PeakGroup.objects.create(
            name=initial_peak_group["name"],
            formula=initial_peak_group["formula"],
            msrun=self.msrun,
            peak_group_set=self.peak_group_set,
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

    def test_animal_treatment_validation(self):
        """
        Here we are purposefully misassociating an animal with the previously
        created msrun protocol, to test model validation upon full_clean
        """
        self.animal.treatment = self.protocol
        with self.assertRaises(ValidationError):
            self.animal.full_clean()

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
            self.sample.time_collected = timedelta(minutes=11000)
            # validation errors are raised upon cleaning
            self.sample.full_clean()
        # test time_collected exceeding MINIMUM_VALID_TIME_COLLECTED fails
        with self.assertRaises(ValidationError):
            self.sample.time_collected = timedelta(minutes=-2000)
            self.sample.full_clean()

    def test_msrun_protocol(self):
        """MSRun lookup by primary key"""
        msr = MSRun.objects.get(id=self.msrun.pk)
        self.assertEqual(msr.protocol.name, "p1")
        self.assertEqual(msr.protocol.category, Protocol.MSRUN_PROTOCOL)
        with self.assertRaises(RestrictedError):
            # test a restricted deletion
            msr.protocol.delete()

    def test_msrun_protocol_validation(self):
        msr = MSRun.objects.get(id=self.msrun.pk)
        msr.protocol.category = Protocol.ANIMAL_TREATMENT
        with self.assertRaises(ValidationError):
            msr.full_clean()

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
                name=self.peak_group.name, msrun=self.msrun
            ),
        )


@override_settings(CACHES=settings.TEST_CACHES)
@tag("protocol")
class ProtocolTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()
        self.p1 = Protocol.objects.create(
            name="Protocol 1",
            category=Protocol.MSRUN_PROTOCOL,
            description="Description",
        )
        self.p1.save()

    def test_retrieve_protocol_by_id(self):
        p = Protocol.objects.filter(name="Protocol 1").get()
        ptest, created = Protocol.retrieve_or_create_protocol(p.id)
        self.assertEqual(self.p1, ptest)

    def test_retrieve_protocol_by_name(self):
        ptest, created = Protocol.retrieve_or_create_protocol(
            "Protocol 1",
            Protocol.MSRUN_PROTOCOL,
            "Description",
        )
        self.assertEqual(self.p1, ptest)

    def test_create_protocol_by_name(self):
        test_protocol_name = "Protocol 2"
        ptest, created = Protocol.retrieve_or_create_protocol(
            test_protocol_name,
            Protocol.MSRUN_PROTOCOL,
            "Description",
        )
        self.assertEqual(ptest, Protocol.objects.filter(name=test_protocol_name).get())

    def test_get_protocol_by_id_dne(self):
        with self.assertRaises(Protocol.DoesNotExist):
            Protocol.retrieve_or_create_protocol(
                100,
                Protocol.MSRUN_PROTOCOL,
                "Description",
            )

    def test_create_protocol_by_invalid_category(self):
        test_protocol_name = "Protocol 2"
        with self.assertRaises(ValidationError):
            Protocol.retrieve_or_create_protocol(
                test_protocol_name,
                "Invalid Category",
                "Description",
            )


@override_settings(CACHES=settings.TEST_CACHES)
class DataLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/protocols/loading.yaml")
        call_command(
            "load_protocols",
            protocols="DataRepo/example_data/protocols/T3_protocol.tsv",
        )
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )
        cls.ALL_COMPOUNDS_COUNT = 51

        # initialize some sample-table-dependent counters
        cls.ALL_SAMPLES_COUNT = 0
        cls.ALL_ANIMALS_COUNT = 0
        cls.ALL_STUDIES_COUNT = 0

        call_command(
            "load_animals_and_samples",
            sample_table_filename="DataRepo/example_data/obob_samples_table.tsv",
            animal_table_filename="DataRepo/example_data/obob_animals_table.tsv",
            table_headers="DataRepo/example_data/sample_and_animal_tables_headers.yaml",
        )

        # from DataRepo/example_data/obob_sample_table.tsv, not counting the header and BLANK samples
        cls.ALL_SAMPLES_COUNT += 106
        # not counting the header and the BLANK animal
        cls.ALL_OBOB_ANIMALS_COUNT = 7
        cls.ALL_ANIMALS_COUNT += cls.ALL_OBOB_ANIMALS_COUNT
        cls.ALL_STUDIES_COUNT += 1

        call_command(
            "load_samples",
            "DataRepo/example_data/serum_lactate_timecourse_treatment.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
            skip_researcher_check=True,
        )
        # from DataRepo/example_data/serum_lactate_timecourse_treatment.tsv, not counting the header
        cls.ALL_SAMPLES_COUNT += 24
        # not counting the header
        cls.ALL_ANIMALS_COUNT += 5
        cls.ALL_STUDIES_COUNT += 1

        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_6eaas_inf.xlsx",
            date="2021-04-29",
            researcher="Michael Neinast",
        )
        cls.ALL_PEAKGROUPSETS_COUNT = 1
        cls.INF_COMPOUNDS_COUNT = 7
        cls.INF_SAMPLES_COUNT = 56
        cls.INF_PEAKDATA_ROWS = 38

        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_6eaas_serum.xlsx",
            date="2021-04-29",
            researcher="Michael Neinast",
        )
        cls.ALL_PEAKGROUPSETS_COUNT += 1
        cls.SERUM_COMPOUNDS_COUNT = 13
        cls.SERUM_SAMPLES_COUNT = 4
        cls.SERUM_PEAKDATA_ROWS = 85

        # test load CSV file of corrected data, with no "original counterpart"
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_corrected.csv",
            date="2021-10-14",
            researcher="Michael Neinast",
        )
        cls.ALL_PEAKGROUPSETS_COUNT += 1
        cls.NULL_ORIG_COMPOUNDS_COUNT = 7
        cls.NULL_ORIG_SAMPLES_COUNT = 56
        cls.NULL_ORIG_PEAKDATA_ROWS = 38

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

        # MsRun should be equivalent to the samples
        MSRUN_COUNT = (
            self.INF_SAMPLES_COUNT
            + self.SERUM_SAMPLES_COUNT
            + self.NULL_ORIG_SAMPLES_COUNT
        )
        self.assertEqual(MSRun.objects.all().count(), MSRUN_COUNT)

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

    def test_peak_groups_set_loaded(self):
        # 2 peak group sets , 1 for each call to load_accucor_msruns
        self.assertEqual(
            PeakGroupSet.objects.all().count(), self.ALL_PEAKGROUPSETS_COUNT
        )
        self.assertTrue(
            PeakGroupSet.objects.filter(filename="obob_maven_6eaas_inf.xlsx").exists()
        )
        self.assertTrue(
            PeakGroupSet.objects.filter(filename="obob_maven_6eaas_serum.xlsx").exists()
        )
        self.assertTrue(
            PeakGroupSet.objects.filter(
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
            PeakGroup.objects.all().count(),
            INF_PEAKGROUP_COUNT + SERUM_PEAKGROUP_COUNT + NULL_ORIG_PEAKGROUP_COUNT,
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
            PeakData.objects.all().count(),
            INF_PEAKDATA_COUNT + SERUM_PEAKDATA_COUNT + NULL_ORIG_PEAKDATA_COUNT,
        )

    def test_peak_group_peak_data_2(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="histidine")
            .filter(msrun__sample__name="serum-xz971")
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
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )

        peak_data = peak_group.peak_data.filter(labels__count=5).get()
        self.assertAlmostEqual(peak_data.raw_abundance, 1356.587)
        self.assertEqual(peak_data.corrected_abundance, 0)

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
                "load_accucor_msruns",
                protocol="Default",
                accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_sample_dupe.xlsx",
                date="2021-08-20",
                researcher="Michael",
            )

    def test_dupe_samples_not_loaded(self):
        self.assertEqual(Sample.objects.filter(name__exact="tst-dupe1").count(), 0)

    def test_adl_existing_researcher(self):
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_new_researcher_err.xlsx",
            date="2021-04-30",
            researcher="Michael Neinast",
            new_researcher=False,
        )
        # Test that basically, no exception occurred
        self.assertTrue(True)

    def test_adl_new_researcher(self):
        # The error string must include:
        #   The new researcher is in the error
        #   Hidden flag is suggested
        #   Existing researchers are shown
        with self.assertRaises(AggregatedErrors) as ar:
            # Now load with a new researcher (and no --new-researcher flag)
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_new_researcher_err2.xlsx",
                date="2021-04-30",
                researcher="Luke Skywalker",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue("Luke Skywalker" in str(aes.exceptions[0]))
        self.assertTrue(
            "add --new-researcher to your command" in str(aes.exceptions[0])
        )
        self.assertTrue(
            "Michael Neinast\n\tXianfeng Zeng" in str(aes.exceptions[0]),
            msg=f"String [Michael Neinast\nXianfeng Zeng] must be in {str(aes.exceptions[0])}",
        )

    def test_adl_new_researcher_confirmed(self):
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_new_researcher_err2.xlsx",
            date="2021-04-30",
            researcher="Luke Skywalker",
            new_researcher=True,
        )
        # Test that basically, no exception occurred
        self.assertTrue(True)

    def test_adl_existing_researcher_marked_new(self):
        # The error string must include:
        #   The new researcher is in the error
        #   Hidden flag is suggested
        #   Existing researchers are shown
        exp_err = (
            "Researcher [Michael Neinast] exists.  --new-researcher cannot be used for existing researchers.  Current "
            "researchers are:\nMichael Neinast\nXianfeng Zeng"
        )
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_new_researcher_err2.xlsx",
                date="2021-04-30",
                researcher="Michael Neinast",
                new_researcher=True,
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(exp_err in str(aes.exceptions[0]))

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
                "load_samples",
                "DataRepo/example_data/serum_lactate_timecourse_treatment_new_researcher.tsv",
                sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
            )
        aes = ar.exception
        ures = [e for e in aes.exceptions if isinstance(e, UnknownResearcherError)]
        self.assertEqual(1, len(ures))
        self.assertIn(
            exp_err,
            str(ures[0]),
        )
        # There are 24 conflicts due to this file being a copy of a file already loaded, with the reseacher changed.
        self.assertEqual(25, len(aes.exceptions))

    def test_ls_new_researcher_confirmed(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_samples",
                "DataRepo/example_data/serum_lactate_timecourse_treatment_new_researcher.tsv",
                sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
                skip_researcher_check=True,
            )
        aes = ar.exception
        # Test that no researcher exception occurred
        ures = [e for e in aes.exceptions if isinstance(e, UnknownResearcherError)]
        self.assertEqual(0, len(ures))
        # There are 24 ConflictingValueErrors expected (Same samples with different researcher: Han Solo)
        cves = [e for e in aes.exceptions if isinstance(e, ConflictingValueError)]
        self.assertIn("Han Solo", str(cves[0]))
        self.assertEqual(24, len(cves))
        # There are 24 expected errors total
        self.assertEqual(24, len(aes.exceptions))
        self.assertIn(
            "24 exceptions occurred, including type(s): [ConflictingValueError].",
            str(ar.exception),
        )

    @tag("fcirc")
    def test_peakgroup_from_serum_sample_false(self):
        # get a tracer compound from a non-serum sample
        sample = Sample.objects.get(name="Liv-xz982")
        pgl = sample.msruns.last().peak_groups.last().labels.first()
        with self.assertWarns(UserWarning):
            self.assertFalse(pgl.from_serum_sample)

    @tag("synonym_data_loading")
    def test_valid_synonym_accucor_load(self):
        # this file contains 1 valid synonym for glucose, "dextrose"
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_corrected_valid_syn.csv",
            date="2021-11-19",
            researcher="Michael Neinast",
        )

        self.assertTrue(
            PeakGroupSet.objects.filter(
                filename="obob_maven_6eaas_inf_corrected_valid_syn.csv"
            ).exists()
        )
        peak_group = PeakGroup.objects.filter(
            peak_group_set__filename="obob_maven_6eaas_inf_corrected_valid_syn.csv"
        ).first()
        self.assertEqual(peak_group.name, "dextrose")
        self.assertEqual(peak_group.compounds.first().name, "glucose")

    @tag("synonym_data_loading")
    def test_invalid_synonym_accucor_load(self):
        with self.assertRaises(
            AggregatedErrors,
            msg="Should complain about a missing compound (due to a synonym renamed to 'table sugar')",
        ) as ar:
            # this file contains 1 invalid synonym for glucose "table sugar"
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_corrected_invalid_syn.csv",
                date="2021-11-18",
                researcher="Michael Neinast",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], MissingCompounds))
        exp_str = "1 compounds were not found in the database:\n\ttable sugar"
        self.assertIn(
            exp_str,
            str(aes.exceptions[0]),
            msg=f"Exception must contain {exp_str}",
        )


@override_settings(CACHES=settings.TEST_CACHES)
class PropertyTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )
        call_command(
            "load_protocols",
            protocols="DataRepo/example_data/protocols/T3_protocol.tsv",
        )
        cls.ALL_COMPOUNDS_COUNT = 47

        # initialize some sample-table-dependent counters
        cls.ALL_SAMPLES_COUNT = 0
        cls.ALL_ANIMALS_COUNT = 0
        cls.ALL_STUDIES_COUNT = 0

        call_command(
            "load_animals_and_samples",
            sample_table_filename="DataRepo/example_data/obob_samples_table.tsv",
            animal_table_filename="DataRepo/example_data/obob_animals_table.tsv",
            table_headers="DataRepo/example_data/sample_and_animal_tables_headers.yaml",
        )

        # from DataRepo/example_data/obob_sample_table.tsv, not counting the header and BLANK samples
        cls.ALL_SAMPLES_COUNT += 106
        # not counting the header and the BLANK animal
        cls.ALL_OBOB_ANIMALS_COUNT = 7
        cls.ALL_ANIMALS_COUNT += cls.ALL_OBOB_ANIMALS_COUNT
        cls.ALL_STUDIES_COUNT += 1

        call_command(
            "load_samples",
            "DataRepo/example_data/serum_lactate_timecourse_treatment.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
            skip_researcher_check=True,
        )
        # from DataRepo/example_data/serum_lactate_timecourse_treatment.tsv, not counting the header
        cls.ALL_SAMPLES_COUNT += 24
        # not counting the header
        cls.ALL_ANIMALS_COUNT += 5
        cls.ALL_STUDIES_COUNT += 1

        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_6eaas_inf.xlsx",
            date="2021-04-29",
            researcher="Michael Neinast",
        )
        cls.ALL_PEAKGROUPSETS_COUNT = 1
        cls.INF_COMPOUNDS_COUNT = 7
        cls.INF_SAMPLES_COUNT = 56
        cls.INF_PEAKDATA_ROWS = 38

        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_6eaas_serum.xlsx",
            date="2021-04-29",
            researcher="Michael Neinast",
        )
        cls.ALL_PEAKGROUPSETS_COUNT += 1
        cls.SERUM_COMPOUNDS_COUNT = 13
        cls.SERUM_SAMPLES_COUNT = 4
        cls.SERUM_PEAKDATA_ROWS = 85

        # test load CSV file of corrected data, with no "original counterpart"
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_corrected.csv",
            date="2021-10-14",
            researcher="Michael Neinast",
        )
        cls.ALL_PEAKGROUPSETS_COUNT += 1
        cls.NULL_ORIG_COMPOUNDS_COUNT = 7
        cls.NULL_ORIG_SAMPLES_COUNT = 56
        cls.NULL_ORIG_PEAKDATA_ROWS = 38

        # defining a primary animal object for repeated tests
        cls.MAIN_SERUM_ANIMAL = Animal.objects.get(name="971")

        super().setUpTestData()

    @tag("serum")
    def test_sample_peak_groups(self):
        animal = self.MAIN_SERUM_ANIMAL
        last_serum_sample = animal.last_serum_sample
        peak_groups = PeakGroup.objects.filter(
            msrun__sample__id__exact=last_serum_sample.id
        )
        # ALL the sample's PeakGroup objects in the QuerySet total 13
        self.assertEqual(peak_groups.count(), self.SERUM_COMPOUNDS_COUNT)
        # but if limited to only the tracer, it is just 1 object in the QuerySet
        sample_tracer_peak_groups = last_serum_sample.last_tracer_peak_groups
        self.assertEqual(sample_tracer_peak_groups.count(), 1)
        # and test that the Animal convenience method is equivalent for this
        # particular sample/animal
        pg = animal.labels.first().last_serum_tracer_label_peak_groups.first()
        self.assertEqual(sample_tracer_peak_groups.get().id, pg.id)

    @tag("fcirc", "serum")
    def test_missing_serum_sample_peak_data(self):
        animal = self.MAIN_SERUM_ANIMAL
        last_serum_sample = animal.last_serum_sample
        # Sample->MSRun is a restricted relationship, so the MSRuns must be deleted before the sample can be deleted
        serum_sample_msrun = MSRun.objects.filter(
            sample__name=last_serum_sample.name
        ).get()
        serum_sample_msrun.delete()
        """
        with the msrun deleted, the 7 rows of prior peak data
        (test_sample_peak_data, above) are now 0/gone
        """
        peakdata = PeakData.objects.filter(
            peak_group__msrun__sample__exact=last_serum_sample
        ).filter(
            peak_group__compounds__id=animal.infusate.tracers.first().compound.id,
        )
        self.assertEqual(peakdata.count(), 0)
        with self.assertWarns(UserWarning):
            last_serum_sample.delete()
        # with the sample deleted, there are no more serum records...
        # so if we refresh, with no cached final serum values...
        refeshed_animal = Animal.objects.get(name="971")
        refeshed_animal_label = refeshed_animal.labels.first()
        serum_samples = refeshed_animal.samples.filter(
            tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        )
        # so zero length list
        self.assertEqual(serum_samples.count(), 0)
        with self.assertWarns(UserWarning):
            self.assertEqual(
                refeshed_animal_label.last_serum_tracer_label_peak_groups.count(),
                0,
            )

    def test_peak_group_peak_data_1(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf.xlsx")
            .get()
        )
        peak_data = peak_group.peak_data.filter(labels__count=0).get()
        self.assertEqual(peak_data.raw_abundance, 8814287)
        self.assertAlmostEqual(peak_data.corrected_abundance, 9553199.89089051)
        self.assertAlmostEqual(peak_group.total_abundance, 9599112.684, places=3)
        self.assertAlmostEqual(
            peak_group.labels.first().enrichment_fraction, 0.001555566789
        )
        self.assertAlmostEqual(
            peak_group.labels.first().enrichment_abundance,
            14932.06089,
            places=5,
        )
        self.assertAlmostEqual(
            peak_group.labels.first().normalized_labeling, 0.009119978074
        )

    def test_peak_group_peak_data_4(self):
        # null original data
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf_corrected.csv")
            .get()
        )
        peak_data = peak_group.peak_data.filter(labels__count=0).get()
        # so some data is unavialable
        self.assertIsNone(peak_data.raw_abundance)
        self.assertIsNone(peak_data.med_mz)
        self.assertIsNone(peak_data.med_rt)
        # but presumably these are all computed from the corrected data
        self.assertAlmostEqual(peak_data.corrected_abundance, 9553199.89089051)
        self.assertAlmostEqual(peak_group.total_abundance, 9599112.684, places=3)
        self.assertAlmostEqual(
            peak_group.labels.first().enrichment_fraction, 0.001555566789
        )
        self.assertAlmostEqual(
            peak_group.labels.first().enrichment_abundance,
            14932.06089,
            places=5,
        )
        self.assertAlmostEqual(
            peak_group.labels.first().normalized_labeling, 0.009119978074
        )

    def test_peak_group_peak_data_serum(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        peak_data = peak_group.peak_data.filter(labels__count=0).get()
        self.assertAlmostEqual(peak_data.raw_abundance, 205652.5)
        self.assertAlmostEqual(peak_data.corrected_abundance, 222028.365565823)
        self.assertAlmostEqual(peak_group.total_abundance, 267686.902436353)
        self.assertAlmostEqual(
            peak_group.labels.first().enrichment_fraction, 0.1705669439
        )
        self.assertAlmostEqual(
            peak_group.labels.first().enrichment_abundance,
            45658.53687,
            places=5,
        )
        self.assertAlmostEqual(peak_group.labels.first().normalized_labeling, 1)

    def test_no_peak_labeled_elements(self):
        # This creates an animal with a notrogen-labeled tracer (among others)
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/testing_data/animal_sample_table_labeled_elements.xlsx"
            ),
            skip_researcher_check=True,
        )

        # Retrieve a sample associated with an animal that has a tracer with only a nitrogen label
        sample = Sample.objects.get(name__exact="test_animal_2_sample_1")
        pc = Protocol(name=Protocol.MSRUN_PROTOCOL)
        pc.save()
        msrun = MSRun(
            sample=sample,
            researcher="george",
            date=datetime.strptime("1992-1-1".strip(), "%Y-%m-%d"),
            protocol=pc,
        )
        msrun.save()
        pgs = PeakGroupSet()
        pgs.save()
        pg = PeakGroup(name="lactate", peak_group_set=pgs, msrun=msrun)
        pg.save()

        # Add a compound to the peak group that does not have a nitrogen
        cpd = Compound.objects.get(name="lactate", formula="C3H6O3")
        pg.compounds.add(cpd)

        # make sure we get only 1 labeled element of nitrogen
        self.assertEqual(
            ["N"],
            sample.animal.infusate.tracer_labeled_elements(),
            msg="Make sure the tracer labeled elements are set for the animal this peak group is linked to.",
        )

        # Create the peak group label that would be created if the accucor/isocorr data was loaded
        PeakGroupLabel.objects.create(
            peak_group=pg,
            element="N",
        )

        # Now try to trigger a NoCommonLabel exception
        with self.assertRaises(
            NoCommonLabel,
            msg=(
                "PeakGroup lactate found associated with measured compounds: [lactate] that does not contain labeled "
                "element C (from the tracers in the infusate [methionine-(15N1)[200]])."
            ),
        ):
            pg.labels.first().enrichment_fraction

    def test_enrichment_fraction_missing_peak_group_formula(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        peak_group.formula = None
        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().enrichment_fraction)

    def test_enrichment_fraction_missing_bad_formula(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        peak_group.formula = "H2O"
        with self.assertRaises(NoCommonLabel):
            peak_group.labels.first().enrichment_fraction

    def test_enrichment_fraction_missing_labeled_element(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )

        for peak_data in peak_group.peak_data.all():
            for pdl in peak_data.labels.all():
                pdl.delete()
            peak_data.save()

        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().enrichment_fraction)

    def test_peak_group_peak_labeled_elements(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )

        self.assertEqual(["C"], peak_group.peak_labeled_elements)

    def test_peak_group_tracer_labeled_elements(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )

        self.assertEqual(
            ["C"], peak_group.msrun.sample.animal.infusate.tracer_labeled_elements()
        )

    def test_normalized_labeling_latest_serum(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf.xlsx")
            .get()
        )

        first_serum_sample = Sample.objects.filter(name="serum-xz971").get()
        second_serum_sample = Sample.objects.create(
            date=first_serum_sample.date,
            name="serum-xz971.2",
            researcher=first_serum_sample.researcher,
            animal=first_serum_sample.animal,
            tissue=first_serum_sample.tissue,
            time_collected=first_serum_sample.time_collected + timedelta(minutes=1),
        )

        serum_samples = first_serum_sample.animal.samples.filter(
            tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        )
        # there should now be 2 serum samples for this animal
        self.assertEqual(serum_samples.count(), 2)
        last_serum_sample = first_serum_sample.animal.last_serum_sample
        # and the final one should now be the second one (just created)
        self.assertEqual(last_serum_sample.name, second_serum_sample.name)

        msrun = MSRun.objects.create(
            researcher="John Doe",
            date=datetime.now(),
            protocol=first_serum_sample.msruns.first().protocol,
            sample=second_serum_sample,
        )
        second_serum_peak_group = PeakGroup.objects.create(
            name=peak_group.name,
            formula=peak_group.formula,
            msrun=msrun,
            peak_group_set=peak_group.peak_group_set,
        )
        second_serum_peak_group.compounds.add(
            peak_group.msrun.sample.animal.infusate.tracers.first().compound
        )
        first_serum_peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        first_serum_peak_group_label = first_serum_peak_group.labels.first()
        PeakGroupLabel.objects.create(
            peak_group=second_serum_peak_group,
            element=first_serum_peak_group_label.element,
        )
        for orig_peak_data in first_serum_peak_group.peak_data.all():
            pdr = PeakData.objects.create(
                raw_abundance=orig_peak_data.raw_abundance,
                corrected_abundance=orig_peak_data.corrected_abundance,
                peak_group=second_serum_peak_group,
                med_mz=orig_peak_data.med_mz,
                med_rt=orig_peak_data.med_rt,
            )
            PeakDataLabel.objects.create(
                peak_data=pdr,
                element=orig_peak_data.labels.first().element,
                count=orig_peak_data.labels.first().count,
                mass_number=orig_peak_data.labels.first().mass_number,
            )
        second_peak_data = second_serum_peak_group.peak_data.order_by(
            "labels__count"
        ).last()
        second_peak_data.corrected_abundance = 100
        second_peak_data.save()
        self.assertEqual(peak_group.labels.count(), 1, msg="Assure load was complete")
        self.assertAlmostEqual(
            peak_group.labels.first().normalized_labeling, 3.455355083
        )

    def test_normalized_labeling_latest_serum_no_peakgroup(self):
        """
        The calculation of any peak group's normalized labeling utilizes the serum's enrichment fraction of each of the
        tarcer peak groups involved.  This test messes with those tracer peak groups and the serum samples to make sure
        it uses the right serum tracer peak groups and issues an error if they are missing.
        """
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf.xlsx")
            .get()
        )

        first_serum_sample = Sample.objects.filter(name="serum-xz971").get()
        second_serum_sample = Sample.objects.create(
            date=first_serum_sample.date,
            name="serum-xz971.3",
            researcher=first_serum_sample.researcher,
            animal=first_serum_sample.animal,
            tissue=first_serum_sample.tissue,
            time_collected=first_serum_sample.time_collected + timedelta(minutes=1),
        )

        serum_samples = first_serum_sample.animal.samples.filter(
            tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        )
        # there should now be 2 serum samples for this animal
        self.assertEqual(serum_samples.count(), 2)
        last_serum_sample = first_serum_sample.animal.last_serum_sample
        # and the final one should now be the second one (just created)
        self.assertEqual(last_serum_sample.name, second_serum_sample.name)

        # Confirm the original calculated normalized labeling using the existing final serum sample
        self.assertAlmostEqual(
            0.00911997807399377,
            peak_group.labels.first().normalized_labeling,
        )

        # Create a later msrun with the later serum sample (but no peak group)
        msrun = MSRun.objects.create(
            researcher="John Doe",
            date=datetime.now(),
            protocol=first_serum_sample.msruns.first().protocol,
            sample=second_serum_sample,
        )
        # DO NOT CREATE A PEAKGROUP FOR THE TRACER
        self.assertEqual(peak_group.labels.count(), 1, msg="Assure load was complete")
        # With the new logic of obtaining the last instance of a peak group among serum samples, this should still
        # produce a calculation even though the last serum sample doesn't have a peak group for the tracer. It will
        # just use the one from the first
        self.assertAlmostEqual(
            0.00911997807399377,
            peak_group.labels.first().normalized_labeling,
        )

        # Now add a peak group to the new last serum sample and change the corrected abundance to confirm it uses the
        # new last sample's peak group
        second_serum_peak_group = PeakGroup.objects.create(
            name=peak_group.name,
            formula=peak_group.formula,
            msrun=msrun,
            peak_group_set=peak_group.peak_group_set,
        )
        second_serum_peak_group.compounds.add(
            peak_group.msrun.sample.animal.infusate.tracers.first().compound
        )
        first_serum_peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        PeakGroupLabel.objects.create(
            peak_group=second_serum_peak_group,
            element=peak_group.labels.first().element,
        )
        # We do not need to add a peak group label (i.e. make it missing), because it's not used in this calculation
        for orig_peak_data in first_serum_peak_group.peak_data.all():
            pdr = PeakData.objects.create(
                raw_abundance=orig_peak_data.raw_abundance,
                corrected_abundance=orig_peak_data.corrected_abundance,
                peak_group=second_serum_peak_group,
                med_mz=orig_peak_data.med_mz,
                med_rt=orig_peak_data.med_rt,
            )
            PeakDataLabel.objects.create(
                peak_data=pdr,
                element=orig_peak_data.labels.first().element,
                count=orig_peak_data.labels.first().count,
                mass_number=orig_peak_data.labels.first().mass_number,
            )
        second_peak_data = second_serum_peak_group.peak_data.order_by(
            "labels__count"
        ).last()
        second_peak_data.corrected_abundance = 100
        second_peak_data.save()
        # Now confirm the different calculated value
        self.assertAlmostEqual(
            3.4553550826083774, peak_group.labels.first().normalized_labeling
        )

        # Now let's delete both peak groups and confirm the value can no longer be calculated and that a warning is
        # issued

        # Now let's delete the first serum peak group's peak group label record that still exists
        first_serum_peak_group.delete()
        second_serum_peak_group.delete()
        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().normalized_labeling)

    def test_animal_label_populated(self):
        self.assertEqual(AnimalLabel.objects.count(), 12)

    def test_peak_group_label_populated(self):
        self.assertEqual(PeakGroupLabel.objects.count(), 836)

    def test_normalized_labeling_missing_serum_peak_group(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf.xlsx")
            .get()
        )

        peak_group_serum = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        peak_group_serum.delete()

        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().normalized_labeling)

    def test_normalized_labeling_missing_serum_sample(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf.xlsx")
            .get()
        )

        serum_sample_msrun = MSRun.objects.filter(sample__name="serum-xz971").get()
        serum_sample_msrun.delete()
        serum_sample = Sample.objects.filter(name="serum-xz971").get()
        serum_sample.delete()

        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().normalized_labeling)

    def test_peak_data_fraction(self):
        peak_data = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf.xlsx")
            .get()
            .peak_data.filter(labels__count=0)
            .get()
        )
        self.assertAlmostEqual(peak_data.fraction, 0.9952169753)

    def test_peak_group_total_abundance_zero(self):
        # Test various calculations do not raise exceptions when total_abundance is zero
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf.xlsx")
            .get()
        )

        msrun = MSRun.objects.create(
            researcher="John Doe",
            date=datetime.now(),
            protocol=peak_group.msrun.protocol,
            sample=peak_group.msrun.sample,
        )
        peak_group_zero = PeakGroup.objects.create(
            name=peak_group.name,
            formula=peak_group.formula,
            msrun=msrun,
            peak_group_set=peak_group.peak_group_set,
        )

        labeled_elems = []
        for orig_peak_data in peak_group.peak_data.all():
            pd = PeakData.objects.create(
                raw_abundance=0,
                corrected_abundance=0,
                peak_group=peak_group_zero,
                med_mz=orig_peak_data.med_mz,
                med_rt=orig_peak_data.med_rt,
            )
            # Fraction is not defined when total_abundance is zero
            self.assertIsNone(pd.fraction)
            for orig_peak_label in orig_peak_data.labels.all():
                if orig_peak_label.element not in labeled_elems:
                    labeled_elems.append(orig_peak_label.element)
                PeakDataLabel.objects.create(
                    peak_data=pd,
                    element=orig_peak_label.element,
                    mass_number=orig_peak_label.mass_number,
                    count=orig_peak_label.count,
                )
        for pgl in labeled_elems:
            PeakGroupLabel.objects.create(
                peak_group=peak_group_zero,
                element=pgl,
            )

        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group_zero.labels.first().enrichment_fraction)
        self.assertIsNone(peak_group_zero.labels.first().enrichment_abundance)
        self.assertIsNone(peak_group_zero.labels.first().normalized_labeling)
        self.assertEqual(peak_group_zero.total_abundance, 0)

    @tag("fcirc")
    def test_peakgroup_is_tracer_label_compound_group_false(self):
        # get a non tracer compound from a serum sample
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.msruns.last().peak_groups.filter(name__exact="tryptophan").last()
        pgl = pg.labels.first()
        self.assertFalse(pgl.is_tracer_label_compound_group)

    @tag("fcirc")
    def test_peakgroup_can_compute_tracer_label_rates_true(self):
        # get a tracer compound from a  sample
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.last_tracer_peak_groups.last()
        pgl = pg.labels.first()
        self.assertTrue(pgl.can_compute_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_tracer_label_rates_false_no_rate(self):
        # get a tracer compound from a  sample
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.last_tracer_peak_groups.last()
        animal = pg.animal
        # but if the animal infusion_rate is not defined...
        orig_tir = animal.infusion_rate
        animal.infusion_rate = None
        animal.save()
        pgf = animal.labels.first().last_serum_tracer_label_peak_groups.first()
        pglf = pgf.labels.first()
        with self.assertWarns(UserWarning):
            self.assertFalse(pglf.can_compute_tracer_label_rates)
        # revert
        animal.infusion_rate = orig_tir
        animal.save()

    @tag("fcirc")
    def test_peakgroup_can_compute_tracer_label_rates_false_no_conc(self):
        # get a tracer compound from a sample
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.last_tracer_peak_groups.last()
        animal = pg.animal
        al = animal.labels.first()
        pgf = al.last_serum_tracer_label_peak_groups.last()
        pglf = pgf.labels.first()
        # but if the animal tracer_concentration is not defined...
        set_cache(pglf, "tracer_concentration", None)
        with self.assertWarns(UserWarning):
            self.assertFalse(pglf.can_compute_intact_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_body_weight_intact_tracer_label_rates_true(self):
        animal = self.MAIN_SERUM_ANIMAL
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        self.assertTrue(pgl.can_compute_body_weight_intact_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_body_weight_tracer_label_rates_false(self):
        animal = self.MAIN_SERUM_ANIMAL
        orig_bw = animal.body_weight
        animal.body_weight = None
        animal.save()
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        with self.assertWarns(UserWarning):
            self.assertFalse(pgl.can_compute_body_weight_intact_tracer_label_rates)
        # revert
        animal.body_weight = orig_bw
        animal.save()

    @tag("fcirc")
    def test_peakgroup_can_compute_intact_tracer_label_rates_true(self):
        animal = self.MAIN_SERUM_ANIMAL
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        self.assertTrue(pgl.can_compute_intact_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_intact_tracer_label_rates_false(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        pg = animal.last_serum_tracer_peak_groups.first()
        pgid = pg.id
        tracer_labeled_count = tracer.labels.first().count
        intact_peakdata = pg.peak_data.filter(labels__count=tracer_labeled_count).get()
        intact_peakdata_label = intact_peakdata.labels.get(
            count__exact=tracer_labeled_count
        )
        # set to something crazy, or None
        intact_peakdata_label.count = 42
        intact_peakdata_label.save()
        pgf = PeakGroup.objects.get(id=pgid)
        pglf = pgf.labels.first()
        with self.assertWarns(UserWarning):
            self.assertFalse(pglf.can_compute_intact_tracer_label_rates)
        # revert
        intact_peakdata_label.count = tracer_labeled_count
        intact_peakdata_label.save()

    @tag("fcirc")
    def test_peakgroup_can_compute_average_tracer_label_rates_true(self):
        animal = self.MAIN_SERUM_ANIMAL
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        self.assertTrue(pgl.can_compute_average_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_average_tracer_label_rates_false(self):
        # need to invalidate the computed/cached enrichment_fraction, somehow
        animal = self.MAIN_SERUM_ANIMAL
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        set_cache(pgl, "enrichment_fraction", None)
        with self.assertWarns(UserWarning):
            self.assertFalse(pgl.can_compute_average_tracer_label_rates)


@override_settings(CACHES=settings.TEST_CACHES)
class MultiTracerLabelPropertyTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/obob_fasted_glc_lac_gln_ala_multiple_labels/animal_sample_table.xlsx"
            ),
            skip_researcher_check=True,
        )
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/obob_fasted_glc_lac_gln_ala_multiple_labels/"
            "alafasted_cor.xlsx",
            protocol="Default",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )

        super().setUpTestData()

    def test_tracer_labeled_elements(self):
        anml = Animal.objects.get(name="xzl1")
        expected = ["C", "N"]
        output = anml.infusate.tracer_labeled_elements()
        self.assertEqual(expected, output)

    def test_serum_tracers_enrichment_fraction(self):
        anml = Animal.objects.get(name="xzl5")
        recs = anml.labels.all()
        outputc = recs.get(element__exact="C").serum_tracers_enrichment_fraction
        outputn = recs.get(element__exact="N").serum_tracers_enrichment_fraction
        self.assertEqual(2, recs.count())
        self.assertAlmostEqual(0.2235244143081364, outputc)
        self.assertAlmostEqual(0.30075567022988536, outputn)

    def test_peak_labeled_elements_one(self):
        # succinate has no nitrogen
        pg = PeakGroup.objects.filter(msrun__sample__name="xzl5_panc").get(
            name="succinate"
        )
        output = pg.peak_labeled_elements
        # One common element
        expected = ["C"]
        self.assertEqual(expected, output)

    def test_peak_labeled_elements_two(self):
        pg = PeakGroup.objects.filter(msrun__sample__name="xzl5_panc").get(
            name="glutamine"
        )
        output = pg.peak_labeled_elements
        expected = ["C", "N"]
        self.assertEqual(expected, output)

    def test_enrichment_abundance(self):
        pg = PeakGroup.objects.filter(msrun__sample__name="xzl5_panc").get(
            name="glutamine"
        )
        pgc = pg.labels.get(element__exact="C").enrichment_abundance
        pgn = pg.labels.get(element__exact="N").enrichment_abundance
        expectedc = 1369911.2746615328
        expectedn = 6571127.3714690255
        self.assertEqual(pg.labels.count(), 2)
        self.assertAlmostEqual(expectedc, pgc)
        self.assertAlmostEqual(expectedn, pgn)

    def test_normalized_labeling(self):
        pg = PeakGroup.objects.filter(msrun__sample__name="xzl5_panc").get(
            name="glutamine"
        )
        pgc = pg.labels.get(element__exact="C").normalized_labeling
        pgn = pg.labels.get(element__exact="N").normalized_labeling
        expectedc = 0.06287501342027346
        expectedn = 0.2241489339907528
        self.assertEqual(pg.labels.count(), 2)
        self.assertAlmostEqual(expectedc, pgc)
        self.assertAlmostEqual(expectedn, pgn)


@override_settings(CACHES=settings.TEST_CACHES)
class TracerRateTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )

        call_command(
            "load_animals_and_samples",
            sample_table_filename="DataRepo/example_data/obob_samples_table.tsv",
            animal_table_filename="DataRepo/example_data/obob_animals_table.tsv",
            table_headers="DataRepo/example_data/sample_and_animal_tables_headers.yaml",
        )

        # for the fcirc and rate-calculation tests
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/obob_maven_c160_serum.xlsx",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
        )

        # defining a primary animal object for repeated tests
        cls.MAIN_SERUM_ANIMAL = Animal.objects.get(name="970")

        super().setUpTestData()

    @tag("fcirc")
    def test_peakgroup_is_tracer_label_compound_group(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        fpgl = pg.labels.filter(element=element).first()
        self.assertTrue(fpgl.is_tracer_label_compound_group)
        self.assertEqual(fpgl.peak_group.name, "C16:0")

    @tag("fcirc")
    def test_peakgroup_from_serum_sample(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        fpgl = pg.labels.filter(element=element).first()
        self.assertTrue(fpgl.from_serum_sample)

    @tag("fcirc")
    def test_peakgroup_can_compute_tracer_label_rates(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        fpgl = pg.labels.filter(element=element).first()
        self.assertTrue(fpgl.can_compute_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_body_weight_intact_tracer_label_rates(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        fpgl = pg.labels.filter(element=element).first()
        self.assertTrue(fpgl.can_compute_body_weight_intact_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_body_weight_average_tracer_label_rates(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        fpgl = pg.labels.filter(element=element).first()
        self.assertTrue(fpgl.can_compute_body_weight_average_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_intact_tracer_label_rates(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        fpgl = pg.labels.filter(element=element).first()
        self.assertTrue(fpgl.can_compute_intact_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_average_tracer_label_rates(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        fpgl = pg.labels.filter(element=element).first()
        self.assertTrue(fpgl.can_compute_average_tracer_label_rates)

    @tag("fcirc")
    def test_nontracer_peakgroup_calculation_attempts(self):
        animal = self.MAIN_SERUM_ANIMAL
        nontracer_compound = Compound.objects.get(name="succinate")
        non_tracer_pg_label = (
            animal.last_serum_sample.msruns.first()
            .peak_groups.get(compounds__exact=nontracer_compound)
            .labels.first()
        )
        # # but let's get a peakgroup for a compound we know is not the tracer
        # pgs = animal.last_serum_sample.peak_groups(nontracer_compound)
        # # should only be one in this specific case
        # non_tracer_pg = pgs[0]
        # tryptophan is not the tracer
        self.assertFalse(non_tracer_pg_label.is_tracer_label_compound_group)
        # and none of these should return a value
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_disappearance_intact_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_appearance_intact_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_disappearance_intact_per_animal)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_appearance_intact_per_animal)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_disappearance_average_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_appearance_average_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_disappearance_average_per_animal)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_appearance_average_per_animal)

    @tag("fcirc")
    def test_last_serum_tracer_rate_disappearance_intact_per_gram(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.filter(element=element).first()
        self.assertAlmostEqual(
            pgl.rate_disappearance_intact_per_gram,
            38.83966501,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_appearance_intact_per_gram(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.filter(element=element).first()
        self.assertAlmostEqual(
            pgl.rate_appearance_intact_per_gram,
            34.35966501,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_disappearance_intact_per_animal(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.filter(element=element).first()
        self.assertAlmostEqual(
            pgl.rate_disappearance_intact_per_animal,
            1040.903022,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_appearance_intact_per_animal(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.filter(element=element).first()
        self.assertAlmostEqual(
            pgl.rate_appearance_intact_per_animal,
            920.8390222,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_disappearance_average_per_gram(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.filter(element=element).first()
        self.assertAlmostEqual(
            pgl.rate_disappearance_average_per_gram,
            37.36671487,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_appearance_average_per_gram(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.filter(element=element).first()
        self.assertAlmostEqual(
            pgl.rate_appearance_average_per_gram,
            32.88671487,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_disappearance_average_per_animal(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        element = tracer.labels.first().element
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.filter(element=element).first()
        # doublecheck weight, because test is not exact but test_tracer_Rd_avg_g was fine
        self.assertAlmostEqual(
            pgl.rate_disappearance_average_per_animal,
            1001.427958,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_appearance_average_per_animal(self):
        animal = self.MAIN_SERUM_ANIMAL
        # Uses Animal.last_serum_sample and Sample.peak_groups
        pgl = animal.last_serum_sample.last_tracer_peak_groups.first().labels.first()
        self.assertAlmostEqual(
            pgl.rate_appearance_average_per_animal,
            881.3639585,
            places=2,
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
        if all_coordinators[0].auto_update_mode != "immediate":
            raise ValueError(
                "Before setting up test data, the default coordinator is not in immediate autoupdate mode."
            )
        if 0 != all_coordinators[0].buffer_size():
            raise ValueError(
                f"Before setting up test data, there are {all_coordinators[0].buffer_size()} items in the buffer."
            )

        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )

        if 0 != all_coordinators[0].buffer_size():
            raise ValueError(
                f"load_study left {all_coordinators[0].buffer_size()} items in the buffer."
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
        # Make sure that its mode is "immediate"
        self.assertEqual(
            "immediate",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode is 'immediate'.",
        )
        # Make sure that the buffer is empty to start
        for coordinator in all_coordinators:
            self.assertEqual(
                0, coordinator.buffer_size(), msg=msg + "  The buffer is empty."
            )

    def test_animal_and_sample_load_xlsx(self):
        # initialize some sample-table-dependent counters
        SAMPLES_COUNT = 16
        ANIMALS_COUNT = 1
        STUDIES_COUNT = 1

        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/small_dataset/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
            dry_run=False,
        )

        self.assertEqual(Sample.objects.all().count(), SAMPLES_COUNT)
        self.assertEqual(Animal.objects.all().count(), ANIMALS_COUNT)
        self.assertEqual(Study.objects.all().count(), STUDIES_COUNT)

        study = Study.objects.get(name="Small OBOB")
        self.assertEqual(study.animals.count(), ANIMALS_COUNT)

    def test_animal_and_sample_load_in_dry_run(self):
        # Load some data to ensure that none of it changes during the actual test
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/small_multitracer_data/animal_sample_table.xlsx"
            ),
            skip_researcher_check=True,
        )

        pre_load_counts = self.get_record_counts()
        coordinator = MaintainedModel._get_current_coordinator()
        pre_load_maintained_values = coordinator.get_all_maintained_field_values(
            "DataRepo.models"
        )
        self.assertGreater(
            len(pre_load_maintained_values.keys()),
            0,
            msg="Ensure there is data in the database before the test",
        )
        self.assert_coordinator_state_is_initialized()

        with self.assertRaises(DryRun):
            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/example_data/small_dataset/"
                    "small_obob_animal_and_sample_table.xlsx"
                ),
                dry_run=True,
            )

        post_load_maintained_values = coordinator.get_all_maintained_field_values(
            "DataRepo.models"
        )
        post_load_counts = self.get_record_counts()

        self.assertEqual(
            pre_load_counts,
            post_load_counts,
            msg="DryRun mode doesn't change any table's record count.",
        )
        self.assertEqual(
            pre_load_maintained_values,
            post_load_maintained_values,
            msg="DryRun mode doesn't autoupdate.",
        )

    def test_get_column_dupes(self):
        stl = SampleTableLoader()
        col_keys = ["Sample Name", "Study Name"]
        data = [
            {"Sample Name": "q2", "Study Name": "TCA Flux"},
            {"Sample Name": "q2", "Study Name": "TCA Flux"},
        ]
        dupes, rows = stl.get_column_dupes(data, col_keys)
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

    def test_empty_row(self):
        """
        Ensures SheetMergeError doesn't include completely empty rows - asserted by an animal sample table with an
        empty row raising no error at all.

        Also ensures RequiredSampleValuesError doesn't include completely empty rows
        """
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/testing_data/small_obob_animal_and_sample_table_empty_row.xlsx"
            ),
        )

    def test_required_sample_values_error_ignores_emptyanimal_animalsheet(self):
        """
        Ensures RequiredSampleValuesError doesn't include rows with a missing animal ID (but has other values).
        Note, this should raise a SheetMergeError
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/example_data/testing_data/"
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

    def test_required_sample_values_error_ignores_emptyanimal_samplesheet(self):
        """
        Ensures RequiredSampleValuesError doesn't include rows with a missing animal ID (but has other values).
        Note, this should raise a SheetMergeError
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/example_data/testing_data/"
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
    def test_unraised_samplesheet_error_case(self):
        """
        This test demonstrates a current bug.  If there are no empty rows between populated rows in the Animals sheet,
        then a row in the Samples sheet that has an empty Animal ID is completely ignored and that sample is never
        loaded.  This should generate an error, but it does not.
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/example_data/testing_data/"
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

    def test_check_required_values(self):
        """
        Check that missing required vals are added to stl.missing_values
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/example_data/testing_data/"
                    "small_obob_animal_and_sample_table_missing_rqd_vals.xlsx"
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
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_params.yaml",
        )
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
        Assures that every MissingTissues, MissingCompounds, and MissingSamplesError exception brings about the
        creation of AllMissing{Tissues,Compounds,Samples} exceptions and that the original exceptions are changed to a
        warning status (technically - if they are the only exception)
        """
        lsc = LoadStudyCommand()
        exceptions = [
            MissingTissues({"spleen": [1, 2]}, ["brain", "butt"]),
            MissingCompounds({"lysine": {"formula": "C2N2O2", "rownums": [3, 4]}}),
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
        self.assertEqual(3, lsc.load_statuses.num_errors)
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
                "num_errors"
            ],
        )
        self.assertEqual(
            1,
            lsc.load_statuses.statuses["All Tissues Exist in the Database"][
                "num_errors"
            ],
        )
        self.assertEqual(
            1,
            lsc.load_statuses.statuses["All Compounds Exist in the Database"][
                "num_errors"
            ],
        )
        self.assertEqual(0, lsc.load_statuses.statuses["accucor.xlsx"]["num_errors"])

        # Number of warnings in the MultiLoadStatus objects is correct (the accucor file's errors were changed to
        # warnings)
        self.assertEqual(
            0,
            lsc.load_statuses.statuses["All Samples Present in Sample Table File"][
                "num_warnings"
            ],
        )
        self.assertEqual(
            0,
            lsc.load_statuses.statuses["All Tissues Exist in the Database"][
                "num_warnings"
            ],
        )
        self.assertEqual(
            0,
            lsc.load_statuses.statuses["All Compounds Exist in the Database"][
                "num_warnings"
            ],
        )
        self.assertEqual(3, lsc.load_statuses.statuses["accucor.xlsx"]["num_warnings"])

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
                AllMissingSamples,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["All Tissues Exist in the Database"][
                    "aggregated_errors"
                ].exceptions[0],
                AllMissingTissues,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["All Compounds Exist in the Database"][
                    "aggregated_errors"
                ].exceptions[0],
                AllMissingCompounds,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["accucor.xlsx"][
                    "aggregated_errors"
                ].exceptions[0],
                MissingTissues,
            ),
        )
        self.assertTrue(
            isinstance(
                lsc.load_statuses.statuses["accucor.xlsx"][
                    "aggregated_errors"
                ].exceptions[1],
                MissingCompounds,
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

    def test_singly_labeled_isocorr_study(self):
        call_command(
            "load_study",
            "DataRepo/example_data/AsaelR_13C-Valine+PI3Ki_flank-KPC_2021-12_isocorr_CN-corrected/loading.yaml",
            verbosity=2,
        )

    def test_multi_tracer_isocorr_study(self):
        call_command(
            "load_study",
            "DataRepo/example_data/obob_fasted_ace_glycerol_3hb_citrate_eaa_fa_multiple_tracers/loading.yaml",
        )

    def test_multi_label_isocorr_study(self):
        call_command(
            "load_study",
            "DataRepo/example_data/obob_fasted_glc_lac_gln_ala_multiple_labels/loading.yaml",
        )


@override_settings(CACHES=settings.TEST_CACHES)
@tag("load_study")
class ParseIsotopeLabelTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )

        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )

        super().setUpTestData()

    def get_labeled_elements(self):
        return [
            IsotopeObservationData(
                element="C",
                mass_number=13,
                count=0,
                parent=True,
            )
        ]

    def test_parse_parent_isotope_label(self):
        tracer_labeled_elements = self.get_labeled_elements()
        self.assertEqual(
            AccuCorDataLoader.parse_isotope_string(
                "C12 PARENT", tracer_labeled_elements
            ),
            [{"element": "C", "count": 0, "mass_number": 13, "parent": True}],
        )

    def test_parse_isotope_label(self):
        tracer_labeled_elements = self.get_labeled_elements()
        self.assertEqual(
            AccuCorDataLoader.parse_isotope_string(
                "C13-label-5",
                tracer_labeled_elements,
            ),
            [{"element": "C", "count": 5, "mass_number": 13, "parent": False}],
        )

    def test_parse_isotope_label_bad(self):
        tracer_labeled_elements = self.get_labeled_elements()
        with self.assertRaises(IsotopeObservationParsingError):
            AccuCorDataLoader.parse_isotope_string(
                "label-5",
                tracer_labeled_elements,
            )

    def test_parse_isotope_label_empty(self):
        tracer_labeled_elements = self.get_labeled_elements()
        with self.assertRaises(IsotopeObservationParsingError):
            AccuCorDataLoader.parse_isotope_string(
                "",
                tracer_labeled_elements,
            )

    def test_parse_isotope_label_none(self):
        tracer_labeled_elements = self.get_labeled_elements()
        with self.assertRaises(TypeError):
            AccuCorDataLoader.parse_isotope_string(
                None,
                tracer_labeled_elements,
            )

    def test_parse_isotope_label_no_carbon(self):
        tracer_labeled_elements = [
            IsotopeObservationData(element="N", mass_number=14, count=2, parent=True),
            IsotopeObservationData(element="O", mass_number=16, count=1, parent=True),
        ]
        self.assertEqual(
            AccuCorDataLoader.parse_isotope_string(
                "C12 PARENT", tracer_labeled_elements
            ),
            tracer_labeled_elements,
        )

    def test_dupe_compound_isotope_pairs(self):
        # Error must contain:
        #   all compound/isotope pairs that were dupes
        #   all line numbers the dupes were on
        exp_err = (
            "The following duplicate compound/isotope combinations were found in the data:\n"
            "\toriginal sheet:\n"
            "\t\tCompound: [glucose], Label: [C12 PARENT] on row(s): ['2-3']\n"
            "\t\tCompound: [lactate], Label: [C12 PARENT] on row(s): ['4-5']\n"
            "\tcorrected sheet:\n"
            "\t\tCompound: [glucose], C_Label: [0] on row(s): ['2-3']\n"
            "\t\tCompound: [lactate], C_Label: [0] on row(s): ['4-5']"
        )
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_dupes.xlsx",
                date="2021-06-03",
                researcher="Xianfeng Zeng",
            )
        aes = ar.exception
        aes.print_summary()
        aes.print_all_buffered_exceptions()
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], DupeCompoundIsotopeCombos))
        self.assertTrue("original" in aes.exceptions[0].dupe_dict.keys())
        self.assertTrue("corrected" in aes.exceptions[0].dupe_dict.keys())
        self.assertEqual(exp_err, str(aes.exceptions[0]), msg=str(aes.exceptions[0]))
        # Data was not loaded
        self.assertEqual(PeakGroup.objects.filter(name__exact="glucose").count(), 0)
        self.assertEqual(PeakGroup.objects.filter(name__exact="lactate").count(), 0)

    def test_multiple_labeled_elements(self):
        dual_label = "C13N15-label-1-2"
        self.assertEqual(
            AccuCorDataLoader.parse_isotope_string(dual_label),
            [
                {"element": "C", "count": 1, "mass_number": 13, "parent": False},
                {"element": "N", "count": 2, "mass_number": 15, "parent": False},
            ],
        )


@override_settings(CACHES=settings.TEST_CACHES)
@tag("animal")
@tag("loading")
class AnimalLoadingTests(TracebaseTestCase):
    """Tests parsing various Animal attributes"""

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/protocols/loading.yaml")
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )

        super().setUpTestData()

    def test_labeled_element_parsing(self):
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/testing_data/animal_sample_table_labeled_elements.xlsx"
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

    def test_labeled_element_parsing_invalid(self):
        with self.assertRaisesMessage(
            IsotopeParsingError, "Encoded isotopes: [13Invalid6] cannot be parsed."
        ):
            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/example_data/testing_data/animal_sample_table_labeled_elements_invalid.xlsx"
                ),
            )
