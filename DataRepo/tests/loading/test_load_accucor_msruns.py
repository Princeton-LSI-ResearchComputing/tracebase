from django.conf import settings
from django.core.management import call_command
from django.test import override_settings, tag

from DataRepo.models import (
    Infusate,
    InfusateTracer,
    MaintainedModel,
    PeakData,
    PeakGroup,
    Sample,
    Tracer,
    TracerLabel,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import (
    AccuCorDataLoader,
    AggregatedErrors,
    ConflictingValueError,
    DryRun,
    NoSamplesError,
    TracerLabeledElementNotFound,
    UnskippedBlanksError,
)
from DataRepo.utils.exceptions import (
    ConflictingValueErrors,
    DuplicatePeakGroup,
)


@override_settings(CACHES=settings.TEST_CACHES)
class AccuCorDataLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "lc_methods")
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )

        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/small_dataset/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )

        super().setUpTestData()

    @classmethod
    def load_glucose_data(cls):
        """Load small_dataset Glucose data"""
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
            skip_samples=("blank"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
        )

    def test_accucor_load_blank_fail(self):
        with self.assertRaises(AggregatedErrors, msg="1 samples are missing.") as ar:
            call_command(
                "load_accucor_msruns",
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_blank_sample.xlsx",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], UnskippedBlanksError))

    def test_accucor_load_blank_skip(self):
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_blank_sample.xlsx",
            skip_samples=("blank"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
        )
        SAMPLES_COUNT = 14
        PEAKDATA_ROWS = 11
        MEASURED_COMPOUNDS_COUNT = 2  # Glucose and lactate

        self.assertEqual(
            PeakGroup.objects.count(), MEASURED_COMPOUNDS_COUNT * SAMPLES_COUNT
        )
        self.assertEqual(PeakData.objects.all().count(), PEAKDATA_ROWS * SAMPLES_COUNT)

    def test_accucor_load_sample_prefix(self):
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_req_prefix.xlsx",
            sample_name_prefix="PREFIX_",
            skip_samples=("blank"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
        )
        SAMPLES_COUNT = 1
        PEAKDATA_ROWS = 11
        MEASURED_COMPOUNDS_COUNT = 2  # Glucose and lactate

        self.assertEqual(
            PeakGroup.objects.count(), MEASURED_COMPOUNDS_COUNT * SAMPLES_COUNT
        )
        self.assertEqual(PeakData.objects.all().count(), PEAKDATA_ROWS * SAMPLES_COUNT)

    def test_accucor_load_sample_prefix_missing(self):
        with self.assertRaises(AggregatedErrors, msg="1 samples are missing.") as ar:
            call_command(
                "load_accucor_msruns",
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_req_prefix.xlsx",
                skip_samples=("blank"),
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
            )
        aes = ar.exception
        nl = "\n"
        self.assertEqual(
            1,
            len(aes.exceptions),
            msg=(
                f"Should be 1 error (NoSamplesError), but there were {len(aes.exceptions)} "
                f"errors:{nl}{nl.join(list(map(lambda s: str(s), aes.exceptions)))}"
            ),
        )
        self.assertTrue(isinstance(aes.exceptions[0], NoSamplesError))

    def assure_coordinator_state_is_initialized(
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

    def test_accucor_load_in_debug(self):
        pre_load_counts = self.get_record_counts()
        coordinator = MaintainedModel._get_default_coordinator()
        pre_load_maintained_values = coordinator.get_all_maintained_field_values()
        self.assertGreater(
            len(pre_load_maintained_values.keys()),
            0,
            msg="Ensure there is data in the database before the test",
        )
        # Check the state of the coordinators
        self.assure_coordinator_state_is_initialized()

        with self.assertRaises(DryRun):
            call_command(
                "load_accucor_msruns",
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_blank_sample.xlsx",
                skip_samples=("blank"),
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
                dry_run=True,
            )

        post_load_maintained_values = coordinator.get_all_maintained_field_values()
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
        self.assure_coordinator_state_is_initialized(
            msg="DryRun mode doesn't leave buffered autoupdates."
        )

    def test_record_missing_compound(self):
        adl = AccuCorDataLoader(
            None,
            None,
            date="1972-11-24",
            researcher="",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="",
            peak_group_set_filename="",
            mzxml_files=[],
        )
        adl.record_missing_compound("new compound", "C1H4", 9)
        self.assertEqual(
            {
                "new compound": {
                    "formula": ["C1H4"],
                    "rownums": [11],
                }
            },
            adl.missing_compounds,
        )

    @tag("multi-msrun")
    def test_conflicting_peakgroups(self):
        """Test loading two conflicting PeakGroups rasies ConflictingValueErrors

        Attempt to load two PeakGroups for the same Compound in the same MSRun
        but from different PeakGroupSets (filenames)
        """

        self.load_glucose_data()

        # The same PeakGroup, but from a different accucor file
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose_conflicting.xlsx",
                skip_samples=("blank"),
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=False,
            )

        aes = ar.exception
        self.assertEqual(1, aes.num_errors)
        self.assertEqual(ConflictingValueErrors, type(aes.exceptions[0]))
        # 1 compounds, 2 samples -> 2 PeakGroups
        self.assertEqual(2, len(aes.exceptions[0].conflicting_value_errors))

    @tag("multi-msrun")
    def test_duplicate_peak_group(self):
        """Test inerting two identical PeakGroups raises an DuplicatePeakGroup error

        This tests the AccuCorDataLoader.insert_peak_group method directly.
        """

        self.load_glucose_data()

        # Setup an AccuCorDataLoader object with minimal info
        # Required since using the "load_accucor_msruns" will not allow
        # multiple loads of the same accucor_file, meaning two PeakGroups will
        # differ in PeakGroupSet and raise ConflictingValueErrors, not DuplicatePeakGroup
        adl = AccuCorDataLoader(
            None,
            None,
            date="2023-01-01",
            researcher="",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="",
            peak_group_set_filename="peak_group_set_filename.tsv",
            mzxml_files=[],
        )
        # Get the first PeakGroup, and collect attributes
        peak_group = PeakGroup.objects.first()
        peak_group_attrs = {
            "name": peak_group.name,
            "formula": peak_group.formula,
            "compounds": peak_group.compounds,
        }

        # Test the instance method "insert_peak_group" rasies and error
        # when inserting an exact duplicate PeakGroup
        with self.assertRaises(DuplicatePeakGroup):
            adl.insert_peak_group(
                peak_group_attrs,
                msrun=peak_group.msrun,
                peak_group_set=peak_group.peak_group_set,
            )

    @tag("multi-msrun")
    def test_conflicting_peak_group(self):
        """Test inserting two conflicting PeakGroups raises ConflictingValueErrors

        Insert two PeakGroups that differ only in Forumla.

        This tests the AccuCorDataLoader.insert_peak_group method directly.
        """

        self.load_glucose_data()

        # Setup an AccuCorDataLoader object with minimal info
        adl = AccuCorDataLoader(
            None,
            None,
            date="2023-01-01",
            researcher="",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="",
            peak_group_set_filename="peak_group_set_filename.tsv",
            mzxml_files=[],
        )
        # Get the first PeakGroup, collect the attributes and change the formula
        peak_group = PeakGroup.objects.first()
        peak_group_attrs = {
            "name": peak_group.name,
            "formula": f"{peak_group.formula}S",
            "compounds": peak_group.compounds,
        }

        with self.assertRaises(ConflictingValueError):
            adl.insert_peak_group(
                peak_group_attrs,
                msrun=peak_group.msrun,
                peak_group_set=peak_group.peak_group_set,
            )

    def test_multiple_accucor_labels(self):
        """
        The infusate has tracers that cumulatively contain multiple Tracers/labels.  This tests that it loads without
        error
        """
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/testing_data/accucor_with_multiple_labels/"
                "samples.xlsx"
            ),
        )
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/testing_data/accucor_with_multiple_labels/accucor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="anonymous",
            new_researcher=False,
        )

    def test_accucor_bad_label(self):
        """
        This tests that a bad label in the accucor file (containing an element not in the tracers) generates a single
        TracerLabeledElementNotFound error
        """
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/testing_data/accucor_with_multiple_labels/"
                "samples.xlsx"
            ),
        )
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                accucor_file="DataRepo/example_data/testing_data/accucor_with_multiple_labels/accucor_bad_label.xlsx",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="anonymous",
                new_researcher=False,
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(
            isinstance(aes.exceptions[0], TracerLabeledElementNotFound),
            msg="First exception must be TracerLabeledElementNotFound, but it was: "
            f"[{type(aes.exceptions[0]).__name__}].",
        )

    @tag("multi-msrun")
    def test_multiple_accucor_one_msrun(self):
        """
        Test that we can load different compounds in separate data files for the same sample run (MSRun)
        """
        self.load_glucose_data()
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_lactate.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=False,
        )
        SAMPLES_COUNT = 2
        PEAKDATA_ROWS = 11
        MEASURED_COMPOUNDS_COUNT = 2  # Glucose and lactate

        self.assertEqual(
            PeakGroup.objects.count(), MEASURED_COMPOUNDS_COUNT * SAMPLES_COUNT
        )
        self.assertEqual(PeakData.objects.all().count(), PEAKDATA_ROWS * SAMPLES_COUNT)

    @tag("multi-msrun")
    def test_duplicate_compounds_one_msrun(self):
        """
        Test that we do not allow the same compound to be measured from the
        same sample run (MSRun) more than once
        """
        self.load_glucose_data()
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                # We just need a different file name with the same data, so _2 is a copy of the original
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose_2.xlsx",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=False,
            )
        # Check second file failed (duplicate compounds)
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], ConflictingValueErrors))
        self.assertEqual(2, len(aes.exceptions[0].conflicting_value_errors))

        # Check first file loaded
        SAMPLES_COUNT = 2
        PEAKDATA_ROWS = 7
        MEASURED_COMPOUNDS_COUNT = 1  # Glucose and lactate

        self.assertEqual(
            PeakGroup.objects.count(), MEASURED_COMPOUNDS_COUNT * SAMPLES_COUNT
        )
        self.assertEqual(PeakData.objects.all().count(), PEAKDATA_ROWS * SAMPLES_COUNT)


@MaintainedModel.no_autoupdates()
@override_settings(CACHES=settings.TEST_CACHES)
class IsoCorrDataLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "lc_methods")
        call_command(
            "load_study",
            "DataRepo/example_data/protocols/loading.yaml",
            verbosity=2,
        )
        call_command(
            "load_study",
            "DataRepo/example_data/tissues/loading.yaml",
            verbosity=2,
        )
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
            verbosity=2,
        )

        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/AsaelR_13C-Valine+PI3Ki_flank-KPC_2021-12_isocorr_CN-corrected/"
                "TraceBase Animal and Sample Table Templates_AR.xlsx"
            ),
            skip_researcher_check=True,
        )

        super().setUpTestData()

    def load_multitracer_data(self):
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/obob_fasted_ace_glycerol_3hb_citrate_eaa_fa_multiple_tracers/"
                "animal_sample_table.xlsx"
            ),
            skip_researcher_check=True,
        )

        num_samples = 120
        num_infusates = 2
        num_infusatetracers = 3
        num_tracers = 6
        num_tracerlabels = 12

        return (
            num_samples,
            num_infusates,
            num_infusatetracers,
            num_tracers,
            num_tracerlabels,
        )

    def load_multilabel_data(self):
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/obob_fasted_glc_lac_gln_ala_multiple_labels/animal_sample_table.xlsx"
            ),
            skip_researcher_check=True,
        )

        num_samples = 156
        num_infusates = 2
        num_infusatetracers = 2
        num_tracers = 2
        num_tracerlabels = 3

        return (
            num_samples,
            num_infusates,
            num_infusatetracers,
            num_tracers,
            num_tracerlabels,
        )

    def test_singly_labeled_isocorr_load(self):
        pre_pg_load_count = PeakGroup.objects.count()
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/AsaelR_13C-Valine+PI3Ki_flank-KPC_2021-12_isocorr_CN-corrected/"
            "Serum results_cor.csv",
            skip_samples=(
                "Blank01",
                "Blank02",
                "Blank03",
                "Blank04",
            ),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            isocorr_format=True,
        )
        post_pg_load_count = PeakGroup.objects.count()
        # The number of samples in the isocorr csv file (not the samples file)
        SAMPLES_COUNT = 19
        PEAKDATA_ROWS = 24
        MEASURED_COMPOUNDS_COUNT = 6

        self.assertEqual(
            post_pg_load_count - pre_pg_load_count,
            MEASURED_COMPOUNDS_COUNT * SAMPLES_COUNT,
            msg=f"PeakGroup record count should be the number of compounds [{MEASURED_COMPOUNDS_COUNT}] times the "
            f"number of samples [{SAMPLES_COUNT}] = [{MEASURED_COMPOUNDS_COUNT * SAMPLES_COUNT}].",
        )
        self.assertEqual(
            PeakData.objects.all().count(),
            PEAKDATA_ROWS * SAMPLES_COUNT,
            msg=f"PeakData record count should be the number of peakdata rows [{PEAKDATA_ROWS}] times the number of "
            f"samples [{SAMPLES_COUNT}] = [{PEAKDATA_ROWS * SAMPLES_COUNT}].",
        )

    def test_singly_labeled_isocorr_missing_flag_error(self):
        """
        Test to make sure the isocorr option is suggested when not supplied
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                accucor_file="DataRepo/example_data/AsaelR_13C-Valine+PI3Ki_flank-KPC_2021-12_isocorr_CN-corrected/"
                "Serum results_cor.csv",
                skip_samples=(
                    "Blank01",
                    "Blank02",
                    "Blank03",
                    "Blank04",
                ),
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertIn("--isocorr-format", str(aes.exceptions[0]))

    def get_model_counts(self):
        return (
            Sample.objects.count(),
            Infusate.objects.count(),
            InfusateTracer.objects.count(),
            Tracer.objects.count(),
            TracerLabel.objects.count(),
        )

    def test_multitracer_sample_table_load(self):
        num_samples = 120
        num_infusates = 2
        num_infusatetracers = 9
        num_tracers = 9
        num_tracerlabels = 12

        (
            pre_samples,
            pre_infusates,
            pre_inftrcs,
            pre_tracers,
            pre_trclbls,
        ) = self.get_model_counts()

        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/obob_fasted_ace_glycerol_3hb_citrate_eaa_fa_multiple_tracers/"
                "animal_sample_table.xlsx"
            ),
            skip_researcher_check=True,
        )

        (
            post_samples,
            post_infusates,
            post_inftrcs,
            post_tracers,
            post_trclbls,
        ) = self.get_model_counts()

        self.assert_model_counts(
            num_samples,
            num_infusates,
            num_infusatetracers,
            num_tracers,
            num_tracerlabels,
            pre_samples,
            pre_infusates,
            pre_inftrcs,
            pre_tracers,
            pre_trclbls,
            post_samples,
            post_infusates,
            post_inftrcs,
            post_tracers,
            post_trclbls,
        )

    def assert_model_counts(
        self,
        num_samples,
        num_infusates,
        num_infusatetracers,
        num_tracers,
        num_tracerlabels,
        pre_samples,
        pre_infusates,
        pre_inftrcs,
        pre_tracers,
        pre_trclbls,
        post_samples,
        post_infusates,
        post_inftrcs,
        post_tracers,
        post_trclbls,
    ):
        self.assertEqual(num_samples, post_samples - pre_samples)
        self.assertEqual(num_infusates, post_infusates - pre_infusates)
        self.assertEqual(
            num_infusatetracers,
            post_inftrcs - pre_inftrcs,
        )
        self.assertEqual(num_tracers, post_tracers - pre_tracers)
        self.assertEqual(num_tracerlabels, post_trclbls - pre_trclbls)

    def test_multitracer_isocorr_load_1(self):
        self.load_multitracer_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/obob_fasted_ace_glycerol_3hb_citrate_eaa_fa_multiple_tracers/"
            "6eaafasted1_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        post_load_group_count = PeakGroup.objects.count()
        # The number of samples in the isocorr xlsx file (not the samples file)
        SAMPLES_COUNT = 30
        PEAKDATA_ROWS = 86
        PARENT_REC_COUNT = 15

        self.assertEqual(
            post_load_group_count - pre_load_group_count,
            PARENT_REC_COUNT * SAMPLES_COUNT,
            msg=f"PeakGroup record count should be the number of C12 PARENT lines [{PARENT_REC_COUNT}] times the "
            f"number of samples [{SAMPLES_COUNT}] = [{PARENT_REC_COUNT * SAMPLES_COUNT}].",
        )
        self.assertEqual(
            PeakData.objects.count(),
            PEAKDATA_ROWS * SAMPLES_COUNT,
            msg=f"PeakData record count should be the number of peakdata rows [{PEAKDATA_ROWS}] times the number of "
            f"samples [{SAMPLES_COUNT}] = [{PEAKDATA_ROWS * SAMPLES_COUNT}].",
        )

    def test_multitracer_isocorr_load_2(self):
        self.load_multitracer_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/obob_fasted_ace_glycerol_3hb_citrate_eaa_fa_multiple_tracers/"
            "6eaafasted2_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        post_load_group_count = PeakGroup.objects.count()
        # The number of samples in the isocorr xlsx file (not the samples file)
        SAMPLES_COUNT = 30
        PEAKDATA_ROWS = 81
        PARENT_REC_COUNT = 15

        self.assertEqual(
            post_load_group_count - pre_load_group_count,
            PARENT_REC_COUNT * SAMPLES_COUNT,
            msg=f"PeakGroup record count should be the number of C12 PARENT lines [{PARENT_REC_COUNT}] times the "
            f"number of samples [{SAMPLES_COUNT}] = [{PARENT_REC_COUNT * SAMPLES_COUNT}].",
        )
        self.assertEqual(
            PeakData.objects.count(),
            PEAKDATA_ROWS * SAMPLES_COUNT,
            msg=f"PeakData record count should be the number of peakdata rows [{PEAKDATA_ROWS}] times the number of "
            f"samples [{SAMPLES_COUNT}] = [{PEAKDATA_ROWS * SAMPLES_COUNT}].",
        )

    def test_multitracer_isocorr_load_3(self):
        self.load_multitracer_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/obob_fasted_ace_glycerol_3hb_citrate_eaa_fa_multiple_tracers/"
            "bcaafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        post_load_group_count = PeakGroup.objects.count()
        # The number of samples in the isocorr xlsx file (not the samples file)
        SAMPLES_COUNT = 60
        PEAKDATA_ROWS = 143
        PARENT_REC_COUNT = 20

        self.assertEqual(
            post_load_group_count - pre_load_group_count,
            PARENT_REC_COUNT * SAMPLES_COUNT,
            msg=f"PeakGroup record count should be the number of C12 PARENT lines [{PARENT_REC_COUNT}] times the "
            f"number of samples [{SAMPLES_COUNT}] = [{PARENT_REC_COUNT * SAMPLES_COUNT}].",
        )
        self.assertEqual(
            PeakData.objects.count(),
            PEAKDATA_ROWS * SAMPLES_COUNT,
            msg=f"PeakData record count should be the number of peakdata rows [{PEAKDATA_ROWS}] times the number of "
            f"samples [{SAMPLES_COUNT}] = [{PEAKDATA_ROWS * SAMPLES_COUNT}].",
        )

    def test_multilabel_sample_table_load(self):
        num_samples = 156
        num_infusates = 2
        num_infusatetracers = 2
        num_tracers = 2
        num_tracerlabels = 4  # TracerLabel records are not unique. Note there would be 3 unique label records

        (
            pre_samples,
            pre_infusates,
            pre_inftrcs,
            pre_tracers,
            pre_trclbls,
        ) = self.get_model_counts()

        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/obob_fasted_glc_lac_gln_ala_multiple_labels/animal_sample_table.xlsx"
            ),
            skip_researcher_check=True,
        )

        (
            post_samples,
            post_infusates,
            post_inftrcs,
            post_tracers,
            post_trclbls,
        ) = self.get_model_counts()

        # Comment to break up JSCPD detection of clones

        self.assert_model_counts(
            num_samples,
            num_infusates,
            num_infusatetracers,
            num_tracers,
            num_tracerlabels,
            pre_samples,
            pre_infusates,
            pre_inftrcs,
            pre_tracers,
            pre_trclbls,
            post_samples,
            post_infusates,
            post_inftrcs,
            post_tracers,
            post_trclbls,
        )

    def test_multilabel_isocorr_load_1(self):
        self.load_multilabel_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/obob_fasted_glc_lac_gln_ala_multiple_labels/"
            "alafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        post_load_group_count = PeakGroup.objects.count()

        self.assert_peak_group_counts(
            pre_load_group_count, post_load_group_count, 84, 94, 13
        )

    def test_multilabel_isocorr_load_2(self):
        self.load_multilabel_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/obob_fasted_glc_lac_gln_ala_multiple_labels/"
            "glnfasted1_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        post_load_group_count = PeakGroup.objects.count()

        self.assert_peak_group_counts(
            pre_load_group_count, post_load_group_count, 36, 95, 13
        )

    def test_multilabel_isocorr_load_3(self):
        self.load_multilabel_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/obob_fasted_glc_lac_gln_ala_multiple_labels/"
            "glnfasted2_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
            skip_samples=("bk",),
        )
        post_load_group_count = PeakGroup.objects.count()

        self.assert_peak_group_counts(
            pre_load_group_count, post_load_group_count, 36, 95, 13
        )

    def assert_peak_group_counts(
        self,
        pre_load_group_count,
        post_load_group_count,
        samples_count,
        peakdata_rows,
        parent_rec_count,
    ):
        self.assertEqual(
            post_load_group_count - pre_load_group_count,
            parent_rec_count * samples_count,
            msg=f"PeakGroup record count should be the number of C12 PARENT lines [{parent_rec_count}] times the "
            f"number of samples [{samples_count}] = [{parent_rec_count * samples_count}].",
        )
        self.assertEqual(
            PeakData.objects.count(),
            peakdata_rows * samples_count,
            msg=f"PeakData record count should be the number of peakdata rows [{peakdata_rows}] times the number of "
            f"samples [{samples_count}] = [{peakdata_rows * samples_count}].",
        )

    def test_labeled_elements_common_with_compound(self):
        """
        Test to ensure count 0 entries are not created when measured compound doesn't have that element
        """
        self.load_multilabel_data()
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/example_data/obob_fasted_glc_lac_gln_ala_multiple_labels/"
            "alafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        pg = (
            PeakGroup.objects.filter(msrun__sample__name="xzl5_panc")
            .filter(name__exact="serine")
            .filter(
                peak_group_set__filename="alafasted_cor.xlsx",
            )
            .order_by("id", "peak_data__labels__element")
            .distinct("id", "peak_data__labels__element")
        )

        self.assertEqual(pg.count(), 2)
        self.assertEqual(pg.filter(peak_data__labels__element__exact="C").count(), 1)
        self.assertEqual(pg.filter(peak_data__labels__element__exact="N").count(), 1)
