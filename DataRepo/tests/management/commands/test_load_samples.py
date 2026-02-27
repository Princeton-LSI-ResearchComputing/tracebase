from datetime import timedelta

from django.conf import settings
from django.core.management import call_command
from django.test import override_settings

from DataRepo.models import (
    Animal,
    Infusate,
    MaintainedModel,
    Sample,
    Study,
    Tissue,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueErrors,
    MissingRecords,
    NewResearchers,
    RequiredColumnValues,
)
from DataRepo.utils.infusate_name_parser import (
    parse_infusate_name,
    parse_infusate_name_with_concs,
)


@override_settings(CACHES=settings.TEST_CACHES)
class LoadSamplesSmallObob2Tests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        Study.objects.create(name="obob_fasted")
        Study.objects.create(name="exp024_michael lactate timecourse")

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

        Infusate.objects.get_or_create_infusate(
            parse_infusate_name("lysine-[13C6]", [2])
        )
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name("C16:0-[13C16]", [1])
        )
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name("lactate-[13C3]", [148.88])
        )

        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob2/animals_table.tsv",
            headers="DataRepo/data/tests/small_obob2/animal_headers.yaml",
        )
        call_command(
            "load_samples",
            infile="DataRepo/data/tests/small_obob2/samples_table.tsv",
            headers="DataRepo/data/tests/small_obob2/sample_headers.yaml",
        )

        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob2/serum_lactate_animal_table.tsv",
            headers="DataRepo/data/tests/small_obob2/animal_headers.yaml",
        )
        call_command(
            "load_samples",
            infile="DataRepo/data/tests/small_obob2/serum_lactate_sample_table_new.tsv",
            headers="DataRepo/data/tests/small_obob2/sample_headers.yaml",
        )

    def test_samples_loaded(self):
        self.assertEqual(15, Sample.objects.all().count())

    def test_sample_data(self):
        sample = Sample.objects.get(name="bat-xz969")
        self.assertEqual(sample.time_collected, timedelta(minutes=150))
        self.assertEqual(sample.researcher, "Xianfeng Zeng")
        self.assertEqual(sample.animal.name, "969")
        self.assertEqual(sample.tissue.name, "brown_adipose_tissue")

    def test_sample_is_serum(self):
        serum = Sample.objects.get(name="serum-xz971")
        self.assertTrue(serum.is_serum_sample)
        nonserum = Sample.objects.get(name="bat-xz969")
        self.assertFalse(nonserum.is_serum_sample)

    def test_animal_serum_sample_methods(self):
        animal = Animal.objects.get(name="971")
        serum_samples = animal.samples.filter(
            tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        )
        self.assertEqual(serum_samples.count(), 1)
        last_serum_sample = animal.last_serum_sample
        self.assertEqual(last_serum_sample.name, "serum-xz971")
        self.assertEqual(last_serum_sample.name, serum_samples.last().name)

    def test_missing_time_collected_warning(self):
        animal = Animal.objects.get(name="971")
        last_serum_sample = animal.last_serum_sample
        # pretend the time_collected did not exist
        last_serum_sample.time_collected = None
        with self.assertWarns(UserWarning):
            # The auto-update of the MaintainedField generates the warning
            last_serum_sample.save()

    def test_dupe_samples_not_loaded(self):
        self.assertEqual(Sample.objects.filter(name__exact="tst-dupe1").count(), 0)

    @MaintainedModel.no_autoupdates()
    def test_ls_new_researcher_and_aggregate_errors(self):
        # The error string must include:
        #   The new researcher is in the error
        #   Hidden flag is suggested
        #   Existing researchers are shown
        exp_err = "check the existing researchers:\n\tMichael Neinast\n\tXianfeng Zeng"
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_samples",
                infile="DataRepo/data/tests/small_obob2/serum_lactate_sample_table_han_solo_new.tsv",
                headers="DataRepo/data/tests/small_obob2/sample_headers.yaml",
            )
        aes = ar.exception
        ures = [e for e in aes.exceptions if isinstance(e, NewResearchers)]
        self.assertEqual(1, len(ures))
        self.assertIn(
            exp_err,
            str(ures[0]),
        )
        # There are conflicts due to this file being a copy of a file already loaded, with the reseacher changed.
        self.assertEqual(2, len(aes.exceptions))

    @MaintainedModel.no_autoupdates()
    def test_ls_new_researcher_confirmed(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_samples",
                infile="DataRepo/data/tests/small_obob2/serum_lactate_sample_table_han_solo_v3.tsv",
                headers="DataRepo/data/tests/small_obob2/sample_table_headers_v3.yaml",
            )
        aes = ar.exception
        # Test that no researcher exception occurred
        ures = [e for e in aes.exceptions if isinstance(e, NewResearchers)]
        self.assertEqual(1, len(ures))
        cves = [e for e in aes.exceptions if isinstance(e, ConflictingValueErrors)]
        self.assertIn("Han Solo", str(cves[0]))
        self.assertEqual(1, len(cves))
        # There are 5 ConflictingValueErrors expected (Same samples with different researcher: Han Solo)
        self.assertEqual(5, len(cves[0].exceptions))
        self.assertEqual(2, len(aes.exceptions))
        self.assertEqual(1, aes.num_warnings)
        self.assertIn(
            "2 exceptions occurred, including type(s): [NewResearchers, ConflictingValueErrors].",
            str(ar.exception),
        )


@override_settings(CACHES=settings.TEST_CACHES)
class LoadSamplesSmallObobTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_study_prerequisites.xlsx",
        )
        Study.objects.create(name="Small OBOB")
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )
        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob/study.xlsx",
        )
        super().setUpTestData()

    @MaintainedModel.no_autoupdates()
    def test_animal_and_sample_load_xlsx(self):
        call_command(
            "load_samples",
            infile="DataRepo/data/tests/small_obob/study.xlsx",
        )
        self.assertEqual(16, Sample.objects.all().count())

    def test_animal_and_sample_load_in_dry_run(self):
        # Load some data to ensure that none of it changes during the actual test
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/small_multitracer/compounds.tsv",
        )
        bcaa = "BCAAs (VLI) {valine-[13C5,15N1][20]; leucine-[13C6,15N1][24]; isoleucine-[13C6,15N1][12]}"
        Infusate.objects.get_or_create_infusate(parse_infusate_name_with_concs(bcaa))
        eaa6 = (
            "6EAAs (MFWKHT) {methionine-[13C5][14]; phenylalanine-[13C9][18]; tryptophan-[13C11][5]; "
            "lysine-[13C6][23]; histidine-[13C6][10]; threonine-[13C4][15]}"
        )
        Infusate.objects.get_or_create_infusate(parse_infusate_name_with_concs(eaa6))
        # Protocol.objects.create(name="no treatment", category=Protocol.ANIMAL_TREATMENT)
        # Protocol.objects.create(name="obob_fasted", category=Protocol.ANIMAL_TREATMENT)
        Study.objects.create(name="ob/ob Fasted")
        Study.objects.create(name="obob_fasted")
        # Study.objects.create(name="Small OBOB")
        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_multitracer/study.xlsx",
        )
        call_command(
            "load_samples",
            infile="DataRepo/data/tests/small_multitracer/study.xlsx",
        )

        pre_load_counts = self.get_record_counts()
        pre_load_maintained_values = MaintainedModel.get_all_maintained_field_values(
            "DataRepo.models"
        )
        self.assertGreater(
            len(pre_load_maintained_values.keys()),
            0,
            msg="Ensure there is data in the database before the test",
        )

        call_command(
            "load_samples",
            infile="DataRepo/data/tests/small_obob/study.xlsx",
            dry_run=True,
        )

        post_load_maintained_values = MaintainedModel.get_all_maintained_field_values(
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

    @MaintainedModel.no_autoupdates()
    def test_samples_loader_check_required_values(self):
        """Check that missing required vals are raised as errors"""
        # Note, animal 972 was not loaded, so we expect another error unrelated to this test.  The old test for the old
        # loader merged the sheets and counted all of the missing animal columns for animal 972.
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_samples",
                infile="DataRepo/data/tests/small_obob/study_missing_rqd_vals.xlsx",
            )
        aes = ar.exception
        self.assertEqual(2, len(aes.exceptions))
        self.assertIsInstance(aes.exceptions[0], RequiredColumnValues)
        self.assertEqual(
            1,
            len(aes.exceptions[0].exceptions),
            msg="1 row (with animal name only) with missing required values (completely empty row ignored)",
        )
        self.assertIn(
            "[Sample, Date Collected, Researcher Name, Tissue] on rows: ['17']",
            str(aes.exceptions[0]),
        )
        # Error unrelated to this test:
        self.assertIsInstance(aes.exceptions[1], MissingRecords)
