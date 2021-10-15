from datetime import datetime, timedelta

import pandas as pd
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.db import IntegrityError
from django.db.models.deletion import RestrictedError
from django.test import TestCase, tag

from DataRepo.models import (
    Animal,
    Compound,
    MSRun,
    PeakData,
    PeakGroup,
    PeakGroupSet,
    Protocol,
    Sample,
    Study,
    Tissue,
    TracerLabeledClass,
)
from DataRepo.utils import AccuCorDataLoader, MissingSamplesError


class ExampleDataConsumer:
    def get_sample_test_dataframe(self):

        # making this a dataframe, if more rows are need for future tests, or we
        # switch to a file based test
        test_df = pd.DataFrame(
            {
                "Sample Name": ["bat-xz969"],
                "Date Collected": ["2020-11-18"],
                "Researcher Name": ["Xianfeng Zhang"],
                "Tissue": ["BAT"],
                "Animal ID": ["969"],
                "Animal Genotype": ["WT"],
                "Animal Body Weight": ["27.2"],
                "Tracer Compound": ["C16:0"],
                "Tracer Labeled Atom": ["C"],
                "Tracer Label Atom Count": ["16.00"],
                "Tracer Infusion Rate": ["0.55"],
                "Tracer Concentration": ["8.00"],
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


class CompoundTests(TestCase):
    def setUp(self):
        Compound.objects.create(
            name="alanine", formula="C3H7NO2", hmdb_id="HMDB0000161"
        )

    def test_compound_name(self):
        """Compound lookup by name"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.name, "alanine")

    def test_compound_hmdb_url(self):
        """Compound hmdb url"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.hmdb_url, f"{Compound.HMDB_CPD_URL}/{alanine.hmdb_id}")

    def test_compound_atom_count(self):
        """Compound atom_count"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.atom_count("C"), 3)

    def test_compound_atom_count_zero(self):
        """Compound atom_count"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.atom_count("F"), 0)

    def test_compound_atom_count_invalid(self):
        """Compound atom_count"""
        alanine = Compound.objects.get(name="alanine")
        with self.assertWarns(UserWarning):
            self.assertEqual(alanine.atom_count("Abc"), None)


class StudyTests(TestCase, ExampleDataConsumer):
    def setUp(self):
        # Get test data
        self.testdata = self.get_sample_test_dataframe()
        first = self.testdata.iloc[0]
        self.first = first

        # Create animal with tracer
        self.tracer = Compound.objects.create(name=first["Tracer Compound"])
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
            tracer_compound=self.tracer,
            tracer_labeled_atom=first["Tracer Labeled Atom"],
            tracer_labeled_count=int(float(first["Tracer Label Atom Count"])),
            tracer_infusion_rate=first["Tracer Infusion Rate"],
            tracer_infusion_concentration=first["Tracer Concentration"],
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
        self.assertEqual(self.tracer.name, self.first["Tracer Compound"])

    def test_tissue(self):
        self.assertEqual(self.tissue.name, self.first["Tissue"])

    def test_animal(self):
        self.assertEqual(self.animal.name, self.first["Animal ID"])
        self.assertEqual(
            self.animal.tracer_compound.name, self.first["Tracer Compound"]
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
        self.assertEqual(t_peak_group.atom_count("C"), 6)

    def test_peak_group_unique_constraint(self):
        self.assertRaises(
            IntegrityError,
            lambda: PeakGroup.objects.create(
                name=self.peak_group.name, msrun=self.msrun
            ),
        )


@tag("protocol")
class ProtocolTests(TestCase):
    def setUp(self):
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


class DataLoadingTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "tissues.yaml")
        call_command("load_compounds", "DataRepo/example_data/obob_compounds.tsv")
        cls.ALL_COMPOUNDS_COUNT = 32

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
        self.assertEqual(sample.researcher, "Xianfeng Zhang")
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
        pgs = PeakGroupSet.objects.all().first()
        self.assertEqual(pgs.filename, "obob_maven_6eaas_inf.xlsx")

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
        self.assertEqual(a.tracer_compound, c)
        self.assertEqual(a.tracer_labeled_atom, TracerLabeledClass.CARBON)
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
        serum_samples = animal.all_serum_samples
        self.assertEqual(serum_samples.count(), 1)
        final_serum_sample = animal.final_serum_sample
        self.assertEqual(final_serum_sample.name, "serum-xz971")
        self.assertEqual(final_serum_sample.name, serum_samples.last().name)

    @tag("serum")
    def test_sample_peak_groups(self):
        animal = self.MAIN_SERUM_ANIMAL
        final_serum_sample = animal.final_serum_sample
        peak_groups = final_serum_sample.peak_groups()
        # ALL the sample's PeakGroup objects in the QuerySet total 13
        self.assertEqual(peak_groups.count(), self.SERUM_COMPOUNDS_COUNT)
        # but if limited to only the tracer, it is just 1 object in the QuerySet
        sample_tracer_peak_groups = final_serum_sample.peak_groups(
            animal.tracer_compound
        )
        self.assertEqual(sample_tracer_peak_groups.count(), 1)
        # and test that the Animal convenience method is equivalent for this
        # particular sample/animal
        animal_tracer_peak_group = animal.final_serum_sample_tracer_peak_group
        self.assertEqual(
            sample_tracer_peak_groups.get().id, animal_tracer_peak_group.id
        )

    @tag("serum")
    def test_sample_peak_data(self):
        animal = self.MAIN_SERUM_ANIMAL
        final_serum_sample = animal.final_serum_sample
        peakdata = final_serum_sample.peak_data()
        # ALL the sample's peakdata objects total 85
        self.assertEqual(peakdata.count(), self.SERUM_PEAKDATA_ROWS)
        # but if limited to only the tracer's data, it is just 7 objects
        peakdata = final_serum_sample.peak_data(animal.tracer_compound)
        self.assertEqual(peakdata.count(), 7)
        # and test that the Animal convenience method is equivalent to the above
        peakdata2 = animal.final_serum_sample_tracer_peak_data
        self.assertEqual(peakdata.last().id, peakdata2.last().id)

    @tag("serum")
    def test_missing_time_collected_warning(self):
        final_serum_sample = self.MAIN_SERUM_ANIMAL.final_serum_sample
        # pretend the time_collected did not exist
        final_serum_sample.time_collected = None
        final_serum_sample.save()
        # so if we refresh, with no cached final serum values...
        refeshed_animal = Animal.objects.get(name="971")
        with self.assertWarns(UserWarning):
            final_serum_sample = refeshed_animal.final_serum_sample

    @tag("fcirc", "serum")
    def test_missing_serum_sample_peak_data(self):
        animal = self.MAIN_SERUM_ANIMAL
        final_serum_sample = animal.final_serum_sample
        # do some deletion tests
        serum_sample_msrun = MSRun.objects.filter(
            sample__name=final_serum_sample.name
        ).get()
        serum_sample_msrun.delete()
        """
        with the msrun deleted, the 7 rows of prior peak data
        (test_sample_peak_data, above) are now 0/gone
        """
        peakdata = final_serum_sample.peak_data(animal.tracer_compound)
        self.assertEqual(peakdata.count(), 0)
        animal.final_serum_sample.delete()
        # with the sample deleted, there are no more serum records...
        # so if we refresh, with no cached final serum values...
        refeshed_animal = Animal.objects.get(name="971")
        serum_samples = refeshed_animal.all_serum_samples
        # so zero length list
        self.assertEqual(serum_samples.count(), 0)
        with self.assertWarns(UserWarning):
            # and attempts to retrieve the final_serum_sample get None
            self.assertIsNone(refeshed_animal.final_serum_sample)
        with self.assertWarns(UserWarning):
            self.assertIsNone(
                refeshed_animal.final_serum_tracer_rate_disappearance_intact_per_gram
            )
        with self.assertWarns(UserWarning):
            self.assertIsNone(
                refeshed_animal.final_serum_tracer_rate_appearance_intact_per_gram
            )
        with self.assertWarns(UserWarning):
            self.assertIsNone(
                refeshed_animal.final_serum_tracer_rate_disappearance_intact_per_animal
            )
        with self.assertWarns(UserWarning):
            self.assertIsNone(
                refeshed_animal.final_serum_tracer_rate_appearance_intact_per_animal
            )
        with self.assertWarns(UserWarning):
            self.assertIsNone(
                refeshed_animal.final_serum_tracer_rate_disappearance_average_per_gram
            )
        with self.assertWarns(UserWarning):
            self.assertIsNone(
                refeshed_animal.final_serum_tracer_rate_appearance_average_per_gram
            )
        with self.assertWarns(UserWarning):
            self.assertIsNone(
                refeshed_animal.final_serum_tracer_rate_disappearance_average_per_animal
            )
        with self.assertWarns(UserWarning):
            self.assertIsNone(
                refeshed_animal.final_serum_tracer_rate_appearance_average_per_animal
            )

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

    def test_peak_group_peak_data_1(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf.xlsx")
            .get()
        )
        peak_data = peak_group.peak_data.filter(labeled_count=0).get()
        self.assertEqual(peak_data.raw_abundance, 8814287)
        self.assertAlmostEqual(peak_data.corrected_abundance, 9553199.89089051)
        self.assertAlmostEqual(peak_group.total_abundance, 9599112.684, places=3)
        self.assertAlmostEqual(peak_group.enrichment_fraction, 0.001555566789)
        self.assertAlmostEqual(peak_group.enrichment_abundance, 14932.06089, places=5)
        self.assertAlmostEqual(peak_group.normalized_labeling, 0.009119978074)

    def test_peak_group_peak_data_2(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="histidine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        # There should be a peak_data for each label count 0-6
        self.assertEqual(peak_group.peak_data.count(), 7)
        # The peak_data for labeled_count==2 is missing, thus values should be 0
        peak_data = peak_group.peak_data.filter(labeled_count=2).get()
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
        peak_data = peak_group.peak_data.filter(labeled_count=5).get()
        self.assertAlmostEqual(peak_data.raw_abundance, 1356.587)
        self.assertEqual(peak_data.corrected_abundance, 0)

    def test_peak_group_peak_data_4(self):
        # null original data
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf_corrected.csv")
            .get()
        )
        peak_data = peak_group.peak_data.filter(labeled_count=0).get()
        # so some data is unavialable
        self.assertIsNone(peak_data.raw_abundance)
        self.assertIsNone(peak_data.med_mz)
        self.assertIsNone(peak_data.med_rt)
        # but presumably these are all computed from the corrected data
        self.assertAlmostEqual(peak_data.corrected_abundance, 9553199.89089051)
        self.assertAlmostEqual(peak_group.total_abundance, 9599112.684, places=3)
        self.assertAlmostEqual(peak_group.enrichment_fraction, 0.001555566789)
        self.assertAlmostEqual(peak_group.enrichment_abundance, 14932.06089, places=5)
        self.assertAlmostEqual(peak_group.normalized_labeling, 0.009119978074)

    def test_peak_group_peak_data_serum(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        peak_data = peak_group.peak_data.filter(labeled_count=0).get()
        self.assertAlmostEqual(peak_data.raw_abundance, 205652.5)
        self.assertAlmostEqual(peak_data.corrected_abundance, 222028.365565823)
        self.assertAlmostEqual(peak_group.total_abundance, 267686.902436353)
        self.assertAlmostEqual(peak_group.enrichment_fraction, 0.1705669439)
        self.assertAlmostEqual(peak_group.enrichment_abundance, 45658.53687, places=5)
        self.assertAlmostEqual(peak_group.normalized_labeling, 1)

    def test_enrichment_fraction_missing_compounds(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        peak_group.compounds.clear()
        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.enrichment_fraction)

    def test_enrichment_fraction_missing_labeled_element(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )

        for peak_data in peak_group.peak_data.all():
            peak_data.labeled_element = None
            peak_data.save()

        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.enrichment_fraction)

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

        serum_samples = first_serum_sample.animal.all_serum_samples
        # there should now be 2 serum samples for this animal
        self.assertEqual(serum_samples.count(), 2)
        final_serum_sample = first_serum_sample.animal.final_serum_sample
        # and the final one should now be the second one (just created)
        self.assertEqual(final_serum_sample.name, second_serum_sample.name)

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
            peak_group.msrun.sample.animal.tracer_compound
        )
        first_serum_peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun__sample__name="serum-xz971")
            .get()
        )
        for orig_peak_data in first_serum_peak_group.peak_data.all():
            PeakData.objects.create(
                raw_abundance=orig_peak_data.raw_abundance,
                corrected_abundance=orig_peak_data.corrected_abundance,
                labeled_element=orig_peak_data.labeled_element,
                labeled_count=orig_peak_data.labeled_count,
                peak_group=second_serum_peak_group,
                med_mz=orig_peak_data.med_mz,
                med_rt=orig_peak_data.med_rt,
            )
        second_peak_data = second_serum_peak_group.peak_data.order_by(
            "labeled_count"
        ).last()
        second_peak_data.corrected_abundance = 100
        second_peak_data.save()
        self.assertAlmostEqual(peak_group.normalized_labeling, 3.455355083)

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
            self.assertIsNone(peak_group.normalized_labeling)

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
            self.assertIsNone(peak_group.normalized_labeling)

    def test_peak_data_fraction(self):
        peak_data = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun__sample__name="BAT-xz971")
            .filter(peak_group_set__filename="obob_maven_6eaas_inf.xlsx")
            .get()
            .peak_data.filter(labeled_count=0)
            .get()
        )
        self.assertAlmostEqual(peak_data.fraction, 0.9952169753)

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
        exp_err = (
            "Researcher [Luke Skywalker] does not exist.  Please either choose from the following researchers, or if "
            "this is a new researcher, add --new-researcher to your command (leaving `--researcher Luke Skywalker` "
            "as-is).  Current researchers are:\nMichael Neinast\nXianfeng Zhang"
        )
        with self.assertRaises(Exception, msg=exp_err):
            # Now load with a new researcher (and no --new-researcher flag)
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_new_researcher_err2.xlsx",
                date="2021-04-30",
                researcher="Luke Skywalker",
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
            "researchers are:\nMichael Neinast\nXianfeng Zhang"
        )
        with self.assertRaises(Exception, msg=exp_err):
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file="DataRepo/example_data/obob_maven_6eaas_inf_new_researcher_err2.xlsx",
                date="2021-04-30",
                researcher="Michael Neinast",
                new_researcher=True,
            )

    def test_ls_new_researcher(self):
        # The error string must include:
        #   The new researcher is in the error
        #   Hidden flag is suggested
        #   Existing researchers are shown
        exp_err = (
            "1 researchers from the sample file: [Han Solo] out of 1 researchers do not exist in the database.  "
            "Please ensure they are not variants of existing researchers in the database:\nMichael Neinast\nXianfeng "
            "Zhang\nIf all researchers are valid new researchers, add --skip-researcher-check to your command."
        )
        with self.assertRaises(Exception, msg=exp_err):
            call_command(
                "load_samples",
                "DataRepo/example_data/serum_lactate_timecourse_treatment_new_researcher.tsv",
                sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
            )

    def test_ls_new_researcher_confirmed(self):
        call_command(
            "load_samples",
            "DataRepo/example_data/serum_lactate_timecourse_treatment_new_researcher.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
            skip_researcher_check=True,
        )
        # Test that basically, no exception occurred
        self.assertTrue(True)

    @tag("fcirc")
    def test_peakgroup_is_tracer_compound_group_false(self):
        # get a non tracer compound from a serum sample
        compound = Compound.objects.get(name="tryptophan")
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.peak_groups(compound).last()
        with self.assertWarns(UserWarning):
            self.assertFalse(pg.is_tracer_compound_group)

    @tag("fcirc")
    def test_peakgroup_from_serum_sample_false(self):
        # get a tracer compound from a non-serum sample
        compound = Compound.objects.get(name="glucose")
        sample = Sample.objects.get(name="Liv-xz982")
        pg = sample.peak_groups(compound).last()
        with self.assertWarns(UserWarning):
            self.assertFalse(pg.from_serum_sample)

    @tag("fcirc")
    def test_peakgroup_can_compute_tracer_rates_true(self):
        # get a tracer compound from a  sample
        compound = Compound.objects.get(name="lysine")
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.peak_groups(compound).last()
        self.assertTrue(pg.can_compute_tracer_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_tracer_rates_false_no_rate(self):
        # get a tracer compound from a  sample
        compound = Compound.objects.get(name="lysine")
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.peak_groups(compound).last()
        animal = pg.animal
        # but if the animal tracer_infusion_rate is not defined...
        orig_tir = animal.tracer_infusion_rate
        animal.tracer_infusion_rate = None
        animal.save()
        pgf = animal.final_serum_sample_tracer_peak_group
        with self.assertWarns(UserWarning):
            self.assertFalse(pgf.can_compute_tracer_rates)
        # revert
        animal.tracer_infusion_rate = orig_tir
        animal.save()

    @tag("fcirc")
    def test_peakgroup_can_compute_tracer_rates_false_no_conc(self):
        # get a tracer compound from a sample
        compound = Compound.objects.get(name="lysine")
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.peak_groups(compound).last()
        animal = pg.animal
        # but if the animal tracer_infusion_concentration is not defined...
        orig_tic = animal.tracer_infusion_concentration
        animal.tracer_infusion_concentration = None
        animal.save()
        pgf = animal.final_serum_sample_tracer_peak_group
        with self.assertWarns(UserWarning):
            self.assertFalse(pgf.can_compute_tracer_rates)
        # revert
        animal.tracer_infusion_concentration = orig_tic
        animal.save()

    @tag("fcirc")
    def test_peakgroup_can_compute_body_weight_tracer_rates_true(self):
        animal = self.MAIN_SERUM_ANIMAL
        pg = animal.final_serum_sample_tracer_peak_group
        self.assertTrue(pg.can_compute_body_weight_tracer_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_body_weight_tracer_rates_false(self):
        animal = self.MAIN_SERUM_ANIMAL
        orig_bw = animal.body_weight
        animal.body_weight = None
        animal.save()
        pg = animal.final_serum_sample_tracer_peak_group
        with self.assertWarns(UserWarning):
            self.assertFalse(pg.can_compute_body_weight_tracer_rates)
        # revert
        animal.body_weight = orig_bw
        animal.save()

    @tag("fcirc")
    def test_peakgroup_can_compute_intact_tracer_rates_true(self):
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        self.assertTrue(pg.can_compute_intact_tracer_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_intact_tracer_rates_false(self):
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        pgid = pg.id
        intact_peakdata = pg.peak_data.filter(
            labeled_count=self.MAIN_SERUM_ANIMAL.tracer_labeled_count
        ).get()
        orig_lc = intact_peakdata.labeled_count
        # set to something crazy, or None
        intact_peakdata.labeled_count = 42
        intact_peakdata.save()
        pgf = PeakGroup.objects.get(id=pgid)
        with self.assertWarns(UserWarning):
            self.assertFalse(pgf.can_compute_intact_tracer_rates)
        # revert
        intact_peakdata.labeled_count = orig_lc
        intact_peakdata.save()

    @tag("fcirc")
    def test_peakgroup_can_compute_average_tracer_rates_true(self):
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        self.assertTrue(pg.can_compute_average_tracer_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_average_tracer_rates_false(self):
        # need to invalidate the computed/cached enrichment_fraction, somehow
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        # simplest way?
        pg.enrichment_fraction = None
        with self.assertWarns(UserWarning):
            self.assertFalse(pg.can_compute_average_tracer_rates)


class TracerRateTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "tissues.yaml")
        call_command("load_compounds", "DataRepo/example_data/obob_compounds.tsv")

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
            researcher="Xianfeng Zhang",
        )

        # defining a primary animal object for repeated tests
        cls.MAIN_SERUM_ANIMAL = Animal.objects.get(name="970")

    @tag("fcirc")
    def test_peakgroup_is_tracer_compound_group(self):
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        self.assertTrue(pg.is_tracer_compound_group)
        self.assertEqual(pg.name, "C16:0")

    @tag("fcirc")
    def test_peakgroup_from_serum_sample(self):
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        self.assertTrue(pg.from_serum_sample)

    @tag("fcirc")
    def test_peakgroup_can_compute_tracer_rates(self):
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        self.assertTrue(pg.can_compute_tracer_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_body_weight_tracer_rates(self):
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        self.assertTrue(pg.can_compute_body_weight_tracer_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_intact_tracer_rates(self):
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        self.assertTrue(pg.can_compute_intact_tracer_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_average_tracer_rates(self):
        pg = self.MAIN_SERUM_ANIMAL.final_serum_sample_tracer_peak_group
        self.assertTrue(pg.can_compute_average_tracer_rates)

    @tag("fcirc")
    def test_nontracer_peakgroup_calculation_attempts(self):
        animal = self.MAIN_SERUM_ANIMAL
        nontracer_compound = Compound.objects.get(name="succinate")
        # but let's get a peakgroup for a compound we know is not the tracer
        pgs = animal.final_serum_sample.peak_groups(nontracer_compound)
        # should only be one in this specific case
        non_tracer_pg = pgs[0]
        with self.assertWarns(UserWarning):
            # tryptophan is not the tracer
            self.assertFalse(non_tracer_pg.is_tracer_compound_group)
        # and none of these should return a value
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg.rate_disappearance_intact_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg.rate_appearance_intact_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg.rate_disappearance_intact_per_animal)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg.rate_appearance_intact_per_animal)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg.rate_disappearance_average_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg.rate_appearance_average_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg.rate_disappearance_average_per_animal)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg.rate_appearance_average_per_animal)

    @tag("fcirc")
    def test_final_serum_tracer_rate_disappearance_intact_per_gram(self):
        animal = self.MAIN_SERUM_ANIMAL
        self.assertAlmostEqual(
            animal.final_serum_tracer_rate_disappearance_intact_per_gram,
            38.83966501,
            places=2,
        )

    @tag("fcirc")
    def test_final_serum_tracer_rate_appearance_intact_per_gram(self):
        animal = self.MAIN_SERUM_ANIMAL
        self.assertAlmostEqual(
            animal.final_serum_tracer_rate_appearance_intact_per_gram,
            34.35966501,
            places=2,
        )

    @tag("fcirc")
    def test_final_serum_tracer_rate_disappearance_intact_per_animal(self):
        animal = self.MAIN_SERUM_ANIMAL
        self.assertAlmostEqual(
            animal.final_serum_tracer_rate_disappearance_intact_per_animal,
            1040.903022,
            places=2,
        )

    @tag("fcirc")
    def test_final_serum_tracer_rate_appearance_intact_per_animal(self):
        animal = self.MAIN_SERUM_ANIMAL
        self.assertAlmostEqual(
            animal.final_serum_tracer_rate_appearance_intact_per_animal,
            920.8390222,
            places=2,
        )

    @tag("fcirc")
    def test_final_serum_tracer_rate_disappearance_average_per_gram(self):
        animal = self.MAIN_SERUM_ANIMAL
        self.assertAlmostEqual(
            animal.final_serum_tracer_rate_disappearance_average_per_gram,
            37.36671487,
            places=2,
        )

    @tag("fcirc")
    def test_final_serum_tracer_rate_appearance_average_per_gram(self):
        animal = self.MAIN_SERUM_ANIMAL
        self.assertAlmostEqual(
            animal.final_serum_tracer_rate_appearance_average_per_gram,
            32.88671487,
            places=2,
        )

    @tag("fcirc")
    def test_final_serum_tracer_rate_disappearance_average_per_animal(self):
        animal = self.MAIN_SERUM_ANIMAL
        # doublecheck weight, because test is not exact but test_tracer_Rd_avg_g was fine
        self.assertAlmostEqual(
            animal.final_serum_tracer_rate_disappearance_average_per_animal,
            1001.427958,
            places=2,
        )

    @tag("fcirc")
    def test_final_serum_tracer_rate_appearance_average_per_animal(self):
        animal = self.MAIN_SERUM_ANIMAL
        self.assertAlmostEqual(
            animal.final_serum_tracer_rate_appearance_average_per_animal,
            881.3639585,
            places=2,
        )


class AnimalAndSampleLoadingTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "tissues.yaml")
        call_command("load_compounds", "DataRepo/example_data/obob_compounds.tsv")
        cls.ALL_COMPOUNDS_COUNT = 32

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
            table_headers="DataRepo/example_data/sample_and_animal_tables_headers.yaml",
            debug=False,
        )

        self.assertEqual(Sample.objects.all().count(), SAMPLES_COUNT)
        self.assertEqual(Animal.objects.all().count(), ANIMALS_COUNT)
        self.assertEqual(Study.objects.all().count(), STUDIES_COUNT)

        study = Study.objects.get(name="Small OBOB")
        self.assertEqual(study.animals.count(), ANIMALS_COUNT)


class AccuCorDataLoadingTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "tissues.yaml")
        call_command("load_compounds", "DataRepo/example_data/obob_compounds.tsv")

        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/small_dataset/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
            table_headers="DataRepo/example_data/sample_and_animal_tables_headers.yaml",
        )
        cls.COMPOUNDS_COUNT = 2

    def test_accucor_load_blank_fail(self):
        with self.assertRaises(MissingSamplesError, msg="1 samples are missing."):
            call_command(
                "load_accucor_msruns",
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_blank_sample.xlsx",
                protocol="Default",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
            )

    def test_accucor_load_blank_skip(self):
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_blank_sample.xlsx",
            skip_samples=("blank"),
            protocol="Default",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
        )
        SAMPLES_COUNT = 14
        PEAKDATA_ROWS = 11

        self.assertEqual(
            PeakGroup.objects.all().count(), self.COMPOUNDS_COUNT * SAMPLES_COUNT
        )
        self.assertEqual(PeakData.objects.all().count(), PEAKDATA_ROWS * SAMPLES_COUNT)

    def test_accucor_load_sample_prefix(self):
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_req_prefix.xlsx",
            sample_name_prefix="PREFIX_",
            skip_samples=("blank"),
            protocol="Default",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
        )
        SAMPLES_COUNT = 1
        PEAKDATA_ROWS = 11

        self.assertEqual(
            PeakGroup.objects.all().count(), self.COMPOUNDS_COUNT * SAMPLES_COUNT
        )
        self.assertEqual(PeakData.objects.all().count(), PEAKDATA_ROWS * SAMPLES_COUNT)

    def test_accucor_load_sample_prefix_missing(self):
        with self.assertRaises(MissingSamplesError, msg="1 samples are missing."):
            call_command(
                "load_accucor_msruns",
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_req_prefix.xlsx",
                skip_samples=("blank"),
                protocol="Default",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
            )


@tag("load_study")
class StudyLoadingTests(TestCase):
    def test_load_small_obob_study(self):
        call_command("loaddata", "tissues.yaml")
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_params.yaml",
        )
        COMPOUNDS_COUNT = 2
        SAMPLES_COUNT = 14
        PEAKDATA_ROWS = 11

        self.assertEqual(
            PeakGroup.objects.all().count(), COMPOUNDS_COUNT * SAMPLES_COUNT
        )
        self.assertEqual(PeakData.objects.all().count(), PEAKDATA_ROWS * SAMPLES_COUNT)


class ParseIsotopeLabelTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "tissues.yaml")
        call_command("load_compounds", "DataRepo/example_data/obob_compounds.tsv")

        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )

    def test_parse_parent_isotope_label(self):
        self.assertEqual(
            AccuCorDataLoader.parse_isotope_label("C12 PARENT"), ("C12", 0)
        )

    def test_parse_isotope_label(self):
        self.assertEqual(
            AccuCorDataLoader.parse_isotope_label("C13-label-5"), ("C13", 5)
        )

    def test_parse_isotope_label_bad(self):
        with self.assertRaises(ValueError):
            AccuCorDataLoader.parse_isotope_label("label-5")

    def test_parse_isotope_label_empty(self):
        with self.assertRaises(ValueError):
            AccuCorDataLoader.parse_isotope_label("")

    def test_parse_isotope_label_none(self):
        with self.assertRaises(TypeError):
            AccuCorDataLoader.parse_isotope_label(None)

    def test_dupe_compound_isotope_pairs(self):
        # Error must contain:
        #   all compound/isotope pairs that were dupes
        #   all line numbers the dupes were on
        exp_err = (
            "The following duplicate compound/isotope pairs were found in the original data: [glucose & C12 PARENT on "
            "rows: 1,2; lactate & C12 PARENT on rows: 3,4]"
        )
        with self.assertRaises(Exception, msg=exp_err):
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_dupes.xlsx",
                date="2021-06-03",
                researcher="Michael",
            )
        # Data was not loaded
        self.assertEqual(PeakGroup.objects.filter(name__exact="glucose").count(), 0)
        self.assertEqual(PeakGroup.objects.filter(name__exact="lactate").count(), 0)
