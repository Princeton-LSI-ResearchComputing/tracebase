import hashlib
from pathlib import Path

import pandas as pd
from django.conf import settings
from django.core.management import CommandError, call_command
from django.test import override_settings, tag

from DataRepo.loaders.legacy.accucor_data_loader import (
    AccuCorDataLoader,
    hash_file,
)
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.models import (
    ArchiveFile,
    DataFormat,
    Infusate,
    InfusateTracer,
    MaintainedModel,
    MSRunSample,
    MSRunSequence,
    PeakData,
    PeakGroup,
    Sample,
    Tracer,
    TracerLabel,
)
from DataRepo.models.study import Study
from DataRepo.models.utilities import exists_in_db
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import (
    AggregatedErrors,
    DryRun,
    NoSamplesError,
    TracerLabeledElementNotFound,
    read_from_file,
)
from DataRepo.utils.exceptions import (
    DuplicatePeakGroup,
    MultiplePeakGroupRepresentation,
)
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs


@override_settings(CACHES=settings.TEST_CACHES)
class AccuCorDataLoadingTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_studies",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_compounds",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_tracers",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_infusates",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_protocols",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_animals",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_tissues",
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
        super().setUpTestData()

    @classmethod
    def load_glucose_data(cls):
        """Load small_dataset Glucose data"""
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose.xlsx",
            skip_samples=("blank"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
        )

    def test_accucor_load_sample_prefix(self):
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_req_prefix.xlsx",
            sample_name_prefix="PREFIX_",
            skip_samples=("blank"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
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
                "legacy_load_accucor_msruns",
                accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_req_prefix.xlsx",
                skip_samples=("blank"),
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
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

    def test_accucor_load_in_debug(self):
        pre_load_counts = self.get_record_counts()
        pre_load_maintained_values = MaintainedModel.get_all_maintained_field_values()
        self.assertGreater(
            len(pre_load_maintained_values.keys()),
            0,
            msg="Ensure there is data in the database before the test",
        )
        # Check the state of the coordinators
        self.assure_coordinator_state_is_initialized()

        with self.assertRaises(DryRun):
            call_command(
                "legacy_load_accucor_msruns",
                accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_blank_sample.xlsx",
                skip_samples=("blank"),
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
                dry_run=True,
            )

        post_load_maintained_values = MaintainedModel.get_all_maintained_field_values()
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
            peak_annotation_file=None,
            data_format=DataFormat.objects.get(code="accucor"),
            date="1972-11-24",
            researcher="",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="",
            peak_annotation_filename="",
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
        """
        Test loading two conflicting PeakGroups rasies ConflictingValueErrors

        Attempt to load two PeakGroups for the same Compound in the same MSRunSample
        Note, when there are 2 different peak annotation files, that is an AmbiguousMSRuns error, but when other data
        differs, it's a ConflictingValueErrors.  The formula for glucose was changed in the conflicting file.
        """

        self.load_glucose_data()

        # The same PeakGroup, but from a different accucor file
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "legacy_load_accucor_msruns",
                accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_conflicting.xlsx",
                skip_samples=("blank"),
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=False,
            )

        aes = ar.exception
        # This legacy loader doesn't summarize MultiplePeakGroupRepresentation exceptions
        # 1 compounds, 2 samples -> 2 PeakGroups
        self.assertEqual(2, aes.num_errors)
        self.assertEqual(MultiplePeakGroupRepresentation, type(aes.exceptions[0]))

    @tag("multi-msrun")
    def test_duplicate_peak_group(self):
        """Test inerting two identical PeakGroups raises an DuplicatePeakGroup error

        This tests the AccuCorDataLoader.insert_peak_group method directly.
        """

        self.load_glucose_data()

        # Setup an AccuCorDataLoader object with minimal info
        # Required since using the "legacy_load_accucor_msruns" will not allow
        # multiple loads of the same accucor_file, meaning two PeakGroups will
        # differ in ArchiveFiles (peak annotation files) and raise ConflictingValueErrors, not DuplicatePeakGroup
        adl = AccuCorDataLoader(
            None,
            None,
            peak_annotation_file=None,
            data_format=DataFormat.objects.get(code="accucor"),
            date="2023-01-01",
            researcher="",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="",
            peak_annotation_filename="peak_annotation_filename.tsv",
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
        with self.assertRaises(Exception) as ar:
            adl.insert_peak_group(
                peak_group_attrs,
                msrun_sample=peak_group.msrun_sample,
                peak_annotation_file=peak_group.peak_annotation_file,
            )
        exc = ar.exception
        self.assertEqual(
            DuplicatePeakGroup,
            type(exc),
            msg=f"{DuplicatePeakGroup} expected. Got [{type(exc).__name__}: {exc}].",
        )

    @tag("multi-msrun")
    def test_conflicting_peak_group(self):
        """Test inserting two conflicting PeakGroups raises ConflictingValueErrors

        Insert two PeakGroups that differ only in Formula.

        This tests the AccuCorDataLoader.insert_peak_group method directly.
        """

        self.load_glucose_data()

        # Setup an AccuCorDataLoader object with minimal info
        adl = AccuCorDataLoader(
            None,
            None,
            peak_annotation_file=None,
            data_format=DataFormat.objects.get(code="accucor"),
            date="2023-01-01",
            researcher="",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="",
            peak_annotation_filename="peak_annotation_filename.tsv",
            mzxml_files=[],
        )
        # Get the first PeakGroup, collect the attributes and change the formula
        peak_group = PeakGroup.objects.first()
        peak_group_attrs = {
            "name": peak_group.name,
            "formula": f"{peak_group.formula}S",
            "compounds": peak_group.compounds,
        }

        with self.assertRaises(MultiplePeakGroupRepresentation):
            adl.insert_peak_group(
                peak_group_attrs,
                msrun_sample=peak_group.msrun_sample,
                peak_annotation_file=peak_group.peak_annotation_file,
            )

    def test_multiple_accucor_labels(self):
        """
        The infusate has tracers that cumulatively contain multiple Tracers/labels.  This tests that it loads without
        error
        """
        call_command(
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/accucor_with_multiple_labels/samples.xlsx"
            ),
        )
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/accucor_with_multiple_labels/accucor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="anonymous",
            new_researcher=False,
        )

        # Test peak data labels
        peak_data = PeakData.objects.filter(peak_group__name="Glycerol").filter(
            peak_group__msrun_sample__sample__name="M1_mix1_T150"
        )

        peak_data_labels = []
        for peakdata in peak_data.all():
            pdl = peakdata.labels.values("element", "mass_number", "count")
            peak_data_labels.append(list(pdl))

        expected = [
            [
                {"element": "C", "mass_number": 13, "count": 0},
            ],
            [
                {"element": "C", "mass_number": 13, "count": 1},
            ],
            [
                {"element": "C", "mass_number": 13, "count": 2},
            ],
            [
                {"element": "C", "mass_number": 13, "count": 3},
            ],
        ]

        self.assertEqual(expected, list(peak_data_labels))

    def test_accucor_bad_label(self):
        """
        This tests that a bad label in the accucor file (containing an element not in the tracers) generates a single
        TracerLabeledElementNotFound error
        """
        call_command(
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/accucor_with_multiple_labels/samples.xlsx"
            ),
        )
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "legacy_load_accucor_msruns",
                accucor_file="DataRepo/data/tests/accucor_with_multiple_labels/accucor_invalid_label.xlsx",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
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
        Test that we can load different compounds in separate data files for the same sample run (MSRunSample)
        """
        self.load_glucose_data()
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
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
    def test_ambiguous_msruns_error(self):
        """
        Tests that an AmbiguousMSRuns exception is raised when a duplicate sample.peak group is encountered and the
        peak annotation file names differ

        This also tests that we do not allow the same compound to be measured from the
        same sample run (MSRunSample) more than once
        """
        self.load_glucose_data()
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "legacy_load_accucor_msruns",
                # We just need a different file name with the same data, so _2 is a copy of the original
                accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_2.xlsx",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=False,
            )
        # Check second file failed (duplicate compound)
        aes = ar.exception
        # This legacy loader doesn't summarize MultiplePeakGroupRepresentation exceptions
        self.assertEqual(2, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], MultiplePeakGroupRepresentation))


@override_settings(CACHES=settings.TEST_CACHES)
class IsoCorrDataLoadingTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    @MaintainedModel.no_autoupdates()
    def setUpTestData(cls):
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/protocols/loading.yaml",
            verbosity=2,
        )
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/tissues/loading.yaml",
            verbosity=2,
        )
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/consolidated_tracebase_compound_list.tsv",
            verbosity=2,
        )

        call_command(
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/singly_labeled_isocorr/animals_samples.xlsx"
            ),
            skip_researcher_check=True,
        )

        super().setUpTestData()

    @MaintainedModel.no_autoupdates()
    def load_multitracer_data(self):
        call_command(
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename="DataRepo/data/tests/multiple_tracers/animal_sample_table.xlsx",
            skip_researcher_check=True,
        )

        num_samples = 4
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

    @MaintainedModel.no_autoupdates()
    def load_multilabel_data(self):
        call_command(
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/multiple_labels/animal_sample_table.xlsx"
            ),
            skip_researcher_check=True,
        )

        num_samples = 5
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

    @MaintainedModel.no_autoupdates()
    def test_singly_labeled_isocorr_load(self):
        pre_pg_load_count = PeakGroup.objects.count()
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/singly_labeled_isocorr/small_cor.csv",
            skip_samples=("Blank01",),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            isocorr_format=True,
        )
        post_pg_load_count = PeakGroup.objects.count()
        # The number of samples in the isocorr csv file (not the samples file)
        SAMPLES_COUNT = 3
        PEAKDATA_ROWS = 6
        MEASURED_COMPOUNDS_COUNT = 2

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

    @MaintainedModel.no_autoupdates()
    def test_singly_labeled_isocorr_missing_flag_error(self):
        """
        Test to make sure the data-format option is suggested when not supplied
        """
        with self.assertRaises(CommandError) as ce:
            call_command(
                "legacy_load_accucor_msruns",
                accucor_file="DataRepo/data/tests/singly_labeled_isocorr/small_cor.csv",
                skip_samples=("Blank01",),
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
            )
        self.assertIn(
            "--data-format",
            repr(ce.exception),
            msg=f"This error should reference --data-format: [{ce.exception}]",
        )

    def get_model_counts(self):
        return (
            Sample.objects.count(),
            Infusate.objects.count(),
            InfusateTracer.objects.count(),
            Tracer.objects.count(),
            TracerLabel.objects.count(),
        )

    @MaintainedModel.no_autoupdates()
    def test_multitracer_sample_table_load(self):
        num_samples = 4
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
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename="DataRepo/data/tests/multiple_tracers/animal_sample_table.xlsx",
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

    def assert_group_data_sample_counts(
        self,
        SAMPLES_COUNT,
        PEAKDATA_ROWS,
        PARENT_REC_COUNT,
        pre_load_group_count,
        post_load_group_count,
    ):
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

    @MaintainedModel.no_autoupdates()
    def test_multitracer_isocorr_load_1(self):
        self.load_multitracer_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/multiple_tracers/6eaafasted1_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        post_load_group_count = PeakGroup.objects.count()
        # The number of samples in the isocorr xlsx file (not the samples file)
        SAMPLES_COUNT = 2
        PEAKDATA_ROWS = 21
        PARENT_REC_COUNT = 3
        self.assert_group_data_sample_counts(
            SAMPLES_COUNT,
            PEAKDATA_ROWS,
            PARENT_REC_COUNT,
            pre_load_group_count,
            post_load_group_count,
        )

    @MaintainedModel.no_autoupdates()
    def test_multitracer_isocorr_load_2(self):
        self.load_multitracer_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/multiple_tracers/bcaafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        post_load_group_count = PeakGroup.objects.count()
        # The number of samples in the isocorr xlsx file (not the samples file)
        SAMPLES_COUNT = 2
        PEAKDATA_ROWS = 24
        PARENT_REC_COUNT = 2
        self.assert_group_data_sample_counts(
            SAMPLES_COUNT,
            PEAKDATA_ROWS,
            PARENT_REC_COUNT,
            pre_load_group_count,
            post_load_group_count,
        )

    @MaintainedModel.no_autoupdates()
    def test_multilabel_sample_table_load(self):
        num_samples = 6
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
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/multiple_labels/animal_sample_table.xlsx"
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

    @MaintainedModel.no_autoupdates()
    def test_multilabel_isocorr_load_1(self):
        self.load_multilabel_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/multiple_labels/alafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        post_load_group_count = PeakGroup.objects.count()

        self.assert_peak_group_counts(
            pre_load_group_count, post_load_group_count, 4, 37, 4
        )

    @MaintainedModel.no_autoupdates()
    def test_multilabel_isocorr_load_2(self):
        self.load_multilabel_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/multiple_labels/glnfasted1_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        post_load_group_count = PeakGroup.objects.count()

        self.assert_peak_group_counts(
            pre_load_group_count, post_load_group_count, 2, 26, 2
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

    @MaintainedModel.no_autoupdates()
    def test_labeled_elements_common_with_compound(self):
        """
        Test to ensure count 0 entries are not created when measured compound doesn't have that element
        """
        self.load_multilabel_data()
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/multiple_labels/alafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
        )
        pg = (
            PeakGroup.objects.filter(msrun_sample__sample__name="xzl5_panc")
            .filter(name__exact="serine")
            .filter(
                peak_annotation_file__filename="alafasted_cor.xlsx",
            )
            .order_by("id", "peak_data__labels__element")
            .distinct("id", "peak_data__labels__element")
        )

        self.assertEqual(pg.count(), 2)
        self.assertEqual(pg.filter(peak_data__labels__element__exact="C").count(), 1)
        self.assertEqual(pg.filter(peak_data__labels__element__exact="N").count(), 1)


@tag("isoautocorr")
@override_settings(CACHES=settings.TEST_CACHES)
class IsoAutoCorrDataLoadingTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/protocols/loading.yaml",
            verbosity=2,
        )
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/tissues/loading.yaml",
            verbosity=2,
        )
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/consolidated_tracebase_compound_list.tsv",
            verbosity=2,
        )
        call_command(
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/isoautocorr/test-isoautocorr-study/test-isoautocorr-studydoc.xlsx"
            ),
        )
        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/isoautocorr/test-isoautocorr-study/test-isoautocorr-negative-cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="Exploris480",
            date="2024-05-23",
            researcher="Michael Neinast",
            new_researcher=False,
        )
        cls.SAMPLES_COUNT = 4
        cls.PEAKDATA_ROWS = 14
        cls.MEASURED_COMPOUNDS_COUNT = 2  # L-Serine and Glucose

        super().setUpTestData()

    def test_isoautocorr_load(self):
        """Load test-isoautocorr data"""

        self.assertEqual(
            PeakGroup.objects.count(),
            self.MEASURED_COMPOUNDS_COUNT * self.SAMPLES_COUNT,
        )
        self.assertEqual(
            PeakData.objects.all().count(), self.PEAKDATA_ROWS * self.SAMPLES_COUNT
        )

    def test_isoautocorr_peakdatalabels(self):
        """Check peak data labels for isoautocorr loading"""

        peak_data = PeakData.objects.filter(peak_group__name="Glycine").filter(
            peak_group__msrun_sample__sample__name="His_neg_M3_T02_liv"
        )

        peak_data_labels = []
        for peakdata in peak_data.all():
            pdl = peakdata.labels.values("element", "mass_number", "count")
            peak_data_labels.append(list(pdl))

        expected = [
            [
                {"element": "C", "mass_number": 13, "count": 0},
                {"element": "N", "mass_number": 15, "count": 0},
            ],
            [
                {"element": "C", "mass_number": 13, "count": 0},
                {"element": "N", "mass_number": 15, "count": 1},
            ],
            [
                {"element": "C", "mass_number": 13, "count": 2},
                {"element": "N", "mass_number": 15, "count": 0},
            ],
            [
                {"element": "C", "mass_number": 13, "count": 1},
                {"element": "N", "mass_number": 15, "count": 0},
            ],
            [
                {"element": "C", "mass_number": 13, "count": 1},
                {"element": "N", "mass_number": 15, "count": 1},
            ],
            [
                {"element": "C", "mass_number": 13, "count": 2},
                {"element": "N", "mass_number": 15, "count": 1},
            ],
        ]

        self.assertEqual(expected, list(peak_data_labels))


class MSRunSampleSequenceTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/small_obob/small_obob_study_prerequisites.yaml",
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
        # call_command(
        #     "legacy_load_animals_and_samples",
        #     animal_and_sample_table_filename=(
        #         "DataRepo/data/tests/small_obob/"
        #         "small_obob_animal_and_sample_table.xlsx"
        #     ),
        # )

        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose.xlsx",
            skip_samples=("blank"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            mzxml_files=[
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML",
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/Br-xz971.mzXML",
            ],
        )

        cls.MSRUNSAMPLE_COUNT = 2
        cls.MSRUNSEQUENCE_COUNT = 1

        super().setUpTestData()

    @MaintainedModel.no_autoupdates()
    def test_msrunsample_and_msrunsequence_are_loaded(self):
        """
        Issue #712
        Requirement: 2. accucor_data_loader loads MSRunSample and MSRunSequence
        NOTE: This should also ensure that ms_data_file and ms_raw_file are loaded and that the files are archived
        """
        self.assertEqual(self.MSRUNSAMPLE_COUNT, MSRunSample.objects.count())
        self.assertEqual(self.MSRUNSEQUENCE_COUNT, MSRunSequence.objects.count())

        # mzXML ArchiveFile records exist
        ArchiveFile.objects.get(filename="BAT-xz971.mzXML")
        ArchiveFile.objects.get(filename="Br-xz971.mzXML")

        # Files exist in the archive
        # TODO: Fix these file exist tests
        # PR REVIEW NOTE: These file exist tests fail.  I suspect it's due to the fact that the tests use
        #                 django.core.files.storage.InMemoryStorage in the settings.  Should I override that setting for
        #                 this test or just not test this?
        # batmz_rec = ArchiveFile.objects.get(filename="BAT-xz971.mzXML")
        # brmz_rec = ArchiveFile.objects.get(filename="Br-xz971.mzXML")
        # batmz_loc = Path(str(batmz_rec.file_location))
        # brmz_loc = Path(str(brmz_rec.file_location))
        # self.assertTrue(batmz_loc.is_file(), msg=f"{str(batmz_loc)} must be a real file")
        # self.assertTrue(brmz_loc.is_file(), msg=f"{str(brmz_loc)} must be a real file")

        # RAW ArchiveFile records exist
        batraw_rec = ArchiveFile.objects.get(filename="BAT-xz971.raw")
        brraw_rec = ArchiveFile.objects.get(filename="Br-xz971.raw")

        # Checksums are correct
        self.assertEqual(
            "31bc554534cf9f1e568529d110caa85f1fd0a8c8", batraw_rec.checksum
        )
        self.assertEqual("a129d2228d5a693875d2bb03fb03830becdeecc1", brraw_rec.checksum)

        # Raw files do not exist
        batraw_loc = Path(str(batraw_rec.file_location))
        brraw_loc = Path(str(brraw_rec.file_location))
        self.assertFalse(batraw_loc.is_file())
        self.assertFalse(brraw_loc.is_file())

    def test_peakgroup_msrunsample_null_is_false(self):
        """
        Issue #712
        Requirement: 3. PeakGroup.msrun_sample.null must be set to False
        Requirement: 4. Add migration for PeakGroup.msrun_sample change
        """
        self.assertFalse(PeakGroup.msrun_sample.__dict__["field"].null)

    # NOTE: Test for Issue #712, Requirement 5 (All broken_until_issue712 test tags must be removed) is unnecessary
    # NOTE: Test for Issue #712, Requirement 6 is in test_exceptions.py

    def test_polarity_choices_includes_unknown(self):
        """
        Issue #712
        Requirement: 7.1. Add a polarity choices value: "unknown"
        """
        choices = [
            ("unknown", "unknown"),  # This is the one essential for the test
            ("positive", "positive"),  # The others are a bonus check
            ("negative", "negative"),
        ]
        self.assertEqual(choices, MSRunSample.POLARITY_CHOICES)

    # NOTE: Test for Issue #712, Requirement 7.2 was moved to test_msruns_loader.py

    # NOTE: Test for Issue #712, Requirement 7.3 (A default polarity should be removed from the study submission form)
    # is unnecessary

    # NOTE: Test for Issue #712, Requirement 7.5 (A default polarity should be removed from the study submission form)
    # is unnecessary

    def create_AccuCorDataLoader_object(self):
        return AccuCorDataLoader(
            None,
            None,
            None,
            data_format=DataFormat.objects.get(code="accucor"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="1972-11-24",
            researcher="Michael Neinast",
        )

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_raw_file(self):
        fn = "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        adl = self.create_AccuCorDataLoader_object()
        mz_dict, _ = MSRunsLoader.parse_mzxml(Path(fn))

        # Test that a new record is created
        inafs = ArchiveFile.objects.count()
        afrec = adl.get_or_create_raw_file(mz_dict)
        self.assertTrue(exists_in_db(afrec))
        self.assertEqual(inafs + 1, ArchiveFile.objects.count())

        # Test that the record is retrieved (not created)
        afrec2 = adl.get_or_create_raw_file(mz_dict)
        self.assertEqual(afrec, afrec2)
        # Record count is unchanged
        self.assertEqual(inafs + 1, ArchiveFile.objects.count())

    @MaintainedModel.no_autoupdates()
    def test_hash_file_allow_missing_true(self):
        """
        If a file does not exist and allow_missing is True, hash_file should create a hash based on the file name
        """
        fn = "does not exist"
        hash = hash_file(Path(fn), allow_missing=True)
        self.assertEqual(hashlib.sha1(fn.encode()).hexdigest(), hash)

    @MaintainedModel.no_autoupdates()
    def test_hash_file_allow_missing_false(self):
        """
        If a file does not exist and allow_missing is False, hash_file should raise a FileNotFoundError
        """
        fn = "does not exist"
        with self.assertRaises(FileNotFoundError):
            hash_file(Path(fn), allow_missing=False)

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_archive_file_allow_missing_no_checksum_or_existing_file(
        self,
    ):
        """
        If a file does not exist and no checksum is provided, a ValueError should be raised.
        """
        fn = "does not exist"
        adl = self.create_AccuCorDataLoader_object()
        with self.assertRaises(ValueError) as ar:
            adl.get_or_create_archive_file(
                Path(fn),
                "ms_data",
                "ms_raw",
            )
        ve = ar.exception
        self.assertIn(
            "A checksum is required if the supplied file path is not an existing file.",
            str(ve),
        )

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_archive_file_with_checksum(self):
        """
        If a checksum is supplied and the file doesn't exist, a record is created
        """
        fn = "does not exist"
        adl = self.create_AccuCorDataLoader_object()
        afrec = adl.get_or_create_archive_file(
            Path(fn),
            "ms_data",
            "mzxml",
            checksum="somesuppliedvalue",
        )
        self.assertTrue(exists_in_db(afrec))
        self.assertEqual("somesuppliedvalue", afrec.checksum)

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_archive_file_allow_missing_file_exists(self):
        """
        If a file exists and a checksum is provided, an exception should be raised when that checksum does not match.
        """
        fn = "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        adl = self.create_AccuCorDataLoader_object()
        with self.assertRaises(ValueError) as ar:
            adl.get_or_create_archive_file(
                Path(fn),
                "ms_data",
                "mzxml",
                checksum="somesuppliedvalue",
            )
        ve = ar.exception
        self.assertIn("somesuppliedvalue", str(ve))
        expected_hash = hash_file(Path(fn))
        self.assertIn(expected_hash, str(ve))

    def create_populated_AccuCorDataLoader_object(self, lcms_file):
        xlsx = "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate.xlsx"
        adl = AccuCorDataLoader(
            # Original dataframe
            pd.read_excel(
                xlsx,
                sheet_name=0,  # The first sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            # Corrected dataframe
            pd.read_excel(
                xlsx,
                sheet_name=1,  # The second sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            # Peak annot file name
            xlsx,
            data_format=DataFormat.objects.get(code="accucor"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="1972-11-24",
            researcher="Michael Neinast",
            lcms_metadata_df=read_from_file(lcms_file),
        )
        adl.prepare_metadata()
        return adl

    @MaintainedModel.no_autoupdates()
    def test_get_sample_header_by_mzxml_basename_one_match(self):
        """
        get_sample_header_by_mzxml_basename returns the sample header from the line of the lcms metadata file that has
        the matching mzxml file basename on it
        """
        adl = self.create_populated_AccuCorDataLoader_object(
            "DataRepo/data/tests/small_obob_lcms_metadata/lactate_neg.tsv",
        )
        hdr = adl.get_sample_header_by_mzxml_basename("BAT-xz971_neg.mzXML")
        self.assertEqual("BAT-xz971", hdr)
        self.assertEqual(0, adl.aggregated_errors_object.num_errors)

    @MaintainedModel.no_autoupdates()
    def test_get_sample_header_by_mzxml_basename_no_match(self):
        """
        get_sample_header_by_mzxml_basename returns None if the mzxml file basename isn't in the lcms metadata file
        (because mzxml files are not required)
        """
        adl = self.create_populated_AccuCorDataLoader_object(
            "DataRepo/data/tests/small_obob_lcms_metadata/lactate_neg.tsv",
        )
        hdr = adl.get_sample_header_by_mzxml_basename("BAT-xz971.mzXML")
        self.assertIsNone(hdr)
        self.assertEqual(0, adl.aggregated_errors_object.num_errors)

    @MaintainedModel.no_autoupdates()
    def test_get_sample_header_by_mzxml_basename_multiple_matches(self):
        """
        Exception if the same mzxml file basename occurs in the LCMS metadata file multiple times
        """
        adl = self.create_populated_AccuCorDataLoader_object(
            "DataRepo/data/tests/small_obob_lcms_metadata/lactate_neg_multiple.tsv",
        )
        hdr = adl.get_sample_header_by_mzxml_basename("BAT-xz971_neg.mzXML")
        self.assertIsNone(hdr)
        self.assertEqual(1, len(adl.aggregated_errors_object.exceptions))
        self.assertEqual(ValueError, type(adl.aggregated_errors_object.exceptions[0]))
        self.assertEqual(
            "2 instances of mzxml file [BAT-xz971_neg.mzXML] in the LCMS metadata file.",
            str(adl.aggregated_errors_object.exceptions[0]),
        )
