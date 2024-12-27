from datetime import datetime, timedelta

import pandas as pd
from django.core.management import CommandError, call_command

from DataRepo.loaders import MSRunsLoader
from DataRepo.loaders.peak_annotation_files_loader import (
    PeakAnnotationFilesLoader,
)
from DataRepo.models import (
    Animal,
    Infusate,
    InfusateTracer,
    LCMethod,
    MaintainedModel,
    MSRunSequence,
    PeakData,
    PeakGroup,
    Sample,
    Tissue,
    Tracer,
    TracerLabel,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    DuplicateFileHeaders,
    MultiplePeakGroupRepresentations,
    UnskippedBlanks,
)
from DataRepo.utils.file_utils import read_from_file

# TODO: Swap out all of the calls to legacy_load_animals_and_samples and legacy_load_samples once those loaders are
# refactored to inherit from TableLoader.


def create_test_sequence(researcher, date):
    # Load a sequence and all the MSRunSamples
    lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")
    MSRunSequence.objects.create(
        researcher=researcher,
        date=datetime.strptime(date, "%Y-%m-%d"),
        instrument="unknown",
        lc_method=lcm,
    )


class LoadAccucorSmallObobCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

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
        call_command(
            "load_sequences",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_msruns",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        super().setUpTestData()

    def assure_coordinator_state_is_initialized(
        self, msg="MaintainedModelCoordinators are in the default state."
    ):
        """This ensures validity of the tests.  Probably unnecessary now."""
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
        # Check the state of the coordinators
        self.assure_coordinator_state_is_initialized()

        create_test_sequence("Michael Neinast", "2021-04-29")
        MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    "Sample Name": [
                        "BAT-xz971",
                        "Br-xz971",
                        "Dia-xz971",
                        "gas-xz971",
                        "gWAT-xz971",
                        "H-xz971",
                        "Kid-xz971",
                        "Liv-xz971",
                        "Lu-xz971",
                        "Pc-xz971",
                        "Q-xz971",
                        "SI-xz971",
                        "Sol-xz971",
                        "Sp-xz971",
                    ],
                    "Sample Data Header": [
                        "BAT-xz971",
                        "Br-xz971",
                        "Dia-xz971",
                        "gas-xz971",
                        "gWAT-xz971",
                        "H-xz971",
                        "Kid-xz971",
                        "Liv-xz971",
                        "Lu-xz971",
                        "Pc-xz971",
                        "Q-xz971",
                        "SI-xz971",
                        "Sol-xz971",
                        "Sp-xz971",
                    ],
                    "mzXML File Name": [None for _ in range(14)],
                    "Peak Annotation File Name": [
                        "small_obob_maven_6eaas_inf_blank_sample.xlsx"
                        for _ in range(14)
                    ],
                    "Sequence": [
                        "Michael Neinast, polar-HILIC-25-min, unknown, 2021-04-29"
                        for _ in range(14)
                    ],
                },
            ),
        ).load_data()

        pre_load_counts = self.get_record_counts()
        pre_load_maintained_values = MaintainedModel.get_all_maintained_field_values()
        self.assertGreater(
            len(pre_load_maintained_values.keys()),
            0,
            msg="Ensure there is data in the database before the test",
        )

        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_blank_sample.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            operator="Michael Neinast",
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

    def test_conflicting_peakgroups(self):
        """
        Test loading two conflicting PeakGroups rasies ConflictingValueErrors

        Attempt to load two PeakGroups for the same Compound in the same MSRunSample
        Note, when there are 2 different peak annotation files, that is an AmbiguousMSRuns error, but when other data
        differs, it's a ConflictingValueErrors.  The formula for glucose was changed in the conflicting file.
        """

        create_test_sequence("Michael Neinast", "2021-04-29")
        MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    "Sample Name": ["BAT-xz971", "Br-xz971", "BAT-xz971", "Br-xz971"],
                    "Sample Data Header": [
                        "BAT-xz971",
                        "Br-xz971",
                        "BAT-xz971",
                        "Br-xz971",
                    ],
                    "mzXML File Name": [None, None, None, None],
                    "Peak Annotation File Name": [
                        "small_obob_maven_6eaas_inf_glucose.xlsx",
                        "small_obob_maven_6eaas_inf_glucose.xlsx",
                        "small_obob_maven_6eaas_inf_glucose_conflicting.xlsx",
                        "small_obob_maven_6eaas_inf_glucose_conflicting.xlsx",
                    ],
                    "Sequence": [
                        "Michael Neinast, polar-HILIC-25-min, unknown, 2021-04-29"
                        for _ in range(4)
                    ],
                },
            ),
        ).load_data()

        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            operator="Michael Neinast",
        )

        # The same PeakGroup, but from a different accucor file
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_peak_annotations",
                infile="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_conflicting.xlsx",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
                date="2021-04-29",
                operator="Michael Neinast",
            )

        aes = ar.exception
        self.assertEqual(1, aes.num_errors)
        self.assertEqual(MultiplePeakGroupRepresentations, type(aes.exceptions[0]))
        # 1 compounds, 2 samples -> 2 PeakGroups
        # This error occurs on each of 7 rows, twice (once for each sample)
        self.assertEqual(14, len(aes.exceptions[0].exceptions))

    @MaintainedModel.no_autoupdates()
    def test_blank_skip(self):
        # NOTE: setUpTestData already loaded animals, samples, and all other basal data
        pre_peak_data = PeakData.objects.count()
        pre_peak_group = PeakGroup.objects.count()
        # Load MSRunSequences
        call_command(
            "load_sequences",
            infile="DataRepo/data/tests/blank_samples/blanks1/blank_sample_skip_study.xlsx",
        )
        # Load MSRunSamples
        call_command(
            "load_msruns",
            infile="DataRepo/data/tests/blank_samples/blanks1/blank_sample_skip_study.xlsx",
        )
        # This should skip a sample column named "blank" in blank_samples/blanks1/accucor_with_blank.xlsx
        call_command(
            "load_peak_annotation_files",
            infile="DataRepo/data/tests/blank_samples/blanks1/blank_sample_skip_study.xlsx",
        )
        SAMPLES_COUNT = 1
        PEAKDATA_ROWS = 11
        MEASURED_COMPOUNDS_COUNT = 2  # Glucose and lactate
        self.assertEqual(
            MEASURED_COMPOUNDS_COUNT * SAMPLES_COUNT,
            PeakGroup.objects.count() - pre_peak_group,
        )
        self.assertEqual(
            PEAKDATA_ROWS * SAMPLES_COUNT,
            PeakData.objects.all().count() - pre_peak_data,
        )

    @MaintainedModel.no_autoupdates()
    def test_blank_warn(self):
        # NOTE: setUpTestData already loaded animals, samples, and all other basal data
        pre_peak_data = PeakData.objects.count()
        pre_peak_group = PeakGroup.objects.count()

        # Load MSRunSequences
        call_command(
            "load_sequences",
            infile="DataRepo/data/tests/blank_samples/blanks1/blank_sample_warn_study.xlsx",
        )
        # Load MSRunSamples
        # This should skip a row with a sample named "blank" with a warning because the sample wasn't loaded from the
        # Samples sheet, but the name has "blank" in it.
        call_command(
            "load_msruns",
            infile="DataRepo/data/tests/blank_samples/blanks1/blank_sample_warn_study.xlsx",
        )

        # This should also skip a sample *column* named "blank" in blank_samples/blanks1/accucor_with_blank.xlsx with a
        # warning because an MSRunSample record wasn't found, but "blank" is in the header.
        pafl = PeakAnnotationFilesLoader(
            df=read_from_file(
                "DataRepo/data/tests/blank_samples/blanks1/blank_sample_warn_study.xlsx",
                sheet=PeakAnnotationFilesLoader.DataSheetName,
            ),
            file="DataRepo/data/tests/blank_samples/blanks1/blank_sample_warn_study.xlsx",
        )
        pafl.load_data()

        self.assertEqual(
            1, len(pafl.aggregated_errors_dict["accucor_with_blank.xlsx"].exceptions)
        )
        self.assertEqual(
            1, pafl.aggregated_errors_dict["accucor_with_blank.xlsx"].num_warnings
        )
        self.assertEqual(
            1,
            pafl.aggregated_errors_dict[
                "accucor_with_blank.xlsx"
            ].exception_type_exists(UnskippedBlanks),
        )

        SAMPLES_COUNT = 1
        PEAKDATA_ROWS = 11
        MEASURED_COMPOUNDS_COUNT = 2  # Glucose and lactate

        # The one valid sample loaded
        self.assertEqual(
            MEASURED_COMPOUNDS_COUNT * SAMPLES_COUNT,
            PeakGroup.objects.count() - pre_peak_group,
        )
        self.assertEqual(
            PEAKDATA_ROWS * SAMPLES_COUNT,
            PeakData.objects.all().count() - pre_peak_data,
        )

    def test_accucor_load_sample_prefix(self):
        """Loads an accucor with 1 sample, which has a prefix "PREFIX_" in the peak annot details sheet"""
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_req_prefix.xlsx",
            peak_annotation_details_file=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table_newsample.xlsx"
            ),
        )
        SAMPLES_COUNT = 1
        PEAKDATA_ROWS = 11
        MEASURED_COMPOUNDS_COUNT = 2  # Glucose and lactate

        self.assertEqual(
            PeakGroup.objects.count(), MEASURED_COMPOUNDS_COUNT * SAMPLES_COUNT
        )
        self.assertEqual(PeakData.objects.all().count(), PEAKDATA_ROWS * SAMPLES_COUNT)


class LoadAccucorSmallObob2CommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
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

        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_peak_annotations",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
                infile="DataRepo/data/tests/small_obob2/obob_maven_6eaas_inf_sample_dupe.xlsx",
                date="2021-08-20",
                operator="Michael",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(aes.exception_type_exists(DuplicateFileHeaders))


class LoadAccucorWithMultipleTracersLabelsCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/small_obob/small_obob_study_prerequisites.yaml",
        )
        call_command(
            "legacy_load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/accucor_with_multiple_labels/samples.xlsx"
            ),
        )
        # Load a sequence and all the MSRunSamples
        create_test_sequence("anonymous", "2022-08-18")
        MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    "Sample Name": [
                        "M1_mix1_T150",
                        "M2_mix1_T150",
                        "M3_glycerol_T150",
                        "M4_glycerol_T150",
                    ],
                    "Sample Data Header": [
                        "M1_mix1_T150",
                        "M2_mix1_T150",
                        "M3_glycerol_T150",
                        "M4_glycerol_T150",
                    ],
                    "mzXML File Name": [None for _ in range(4)],
                    "Peak Annotation File Name": ["accucor.xlsx" for _ in range(4)],
                    "Sequence": [
                        "anonymous, polar-HILIC-25-min, unknown, 2022-08-18"
                        for _ in range(4)
                    ],
                },
            ),
        ).load_data()
        super().setUpTestData()

    def test_multiple_accucor_labels(self):
        """
        The infusate has tracers that cumulatively contain multiple Tracers/labels.  This tests that it loads without
        error
        """
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/accucor_with_multiple_labels/accucor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2022-08-18",
            operator="anonymous",
        )


class LoadIsocorrCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

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

        create_test_sequence("Michael Neinast", "2021-04-29")
        MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    "Sample Name": ["t0_a146", "t90_a146", "t120_a146"],
                    "Sample Data Header": ["t0_a146", "t90_a146", "t120_a146"],
                    "mzXML File Name": [None for _ in range(3)],
                    "Peak Annotation File Name": [
                        "small_cor.csv",
                        "small_cor.csv",
                        "small_cor.csv",
                    ],
                    # TODO: Just realized that we either should not allow commas in names or change the seq name delim
                    "Sequence": [
                        "Michael Neinast, polar-HILIC-25-min, unknown, 2021-04-29"
                        for _ in range(3)
                    ],
                },
            ),
        ).load_data()

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

        # Load a sequence and all the MSRunSamples
        create_test_sequence("Xianfeng Zeng", "2021-04-29")
        MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    "Sample Name": ["xz971_bat", "xz971_br", "xz1079_bat", "xz1079_br"],
                    "Sample Data Header": [
                        "xz971_bat",
                        "xz971_br",
                        "xz1079_bat",
                        "xz1079_br",
                    ],
                    "mzXML File Name": [None for _ in range(4)],
                    "Peak Annotation File Name": [
                        "6eaafasted1_cor.xlsx",
                        "6eaafasted1_cor.xlsx",
                        "bcaafasted_cor.xlsx",
                        "bcaafasted_cor.xlsx",
                    ],
                    "Sequence": [
                        "Xianfeng Zeng, polar-HILIC-25-min, unknown, 2021-04-29"
                        for _ in range(4)
                    ],
                },
            ),
        ).load_data()

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

        # Load a sequence and all the MSRunSamples
        create_test_sequence("Xianfeng Zeng", "2021-04-29")
        MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    "Sample Name": [
                        "xzl5_t",
                        "xzl5_panc",
                        "xzl4_t",
                        "xzl4_sp",
                        "xzl1_brain",
                        "xzl1_brownFat",
                    ],
                    "Sample Data Header": [
                        "xzl5_t",
                        "xzl5_panc",
                        "xzl4_t",
                        "xzl4_sp",
                        "xzl1_brain",
                        "xzl1_brownFat",
                    ],
                    "mzXML File Name": [None for _ in range(6)],
                    "Peak Annotation File Name": [
                        "alafasted_cor.xlsx",
                        "alafasted_cor.xlsx",
                        "alafasted_cor.xlsx",
                        "alafasted_cor.xlsx",
                        "glnfasted_cor.xlsx",
                        "glnfasted_cor.xlsx",
                    ],
                    "Sequence": [
                        "Xianfeng Zeng, polar-HILIC-25-min, unknown, 2021-04-29"
                        for _ in range(6)
                    ],
                },
            ),
        ).load_data()

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
            "load_peak_annotations",
            infile="DataRepo/data/tests/singly_labeled_isocorr/small_cor.csv",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            operator="Michael Neinast",
            format="isocorr",
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
        Test to make sure the --format option is suggested when necessary and not supplied
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_peak_annotations",
                infile="DataRepo/data/tests/singly_labeled_isocorr/small_cor.csv",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="unknown",
                date="2021-04-29",
                operator="Michael Neinast",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertIsInstance(aes.exceptions[0], CommandError)
        self.assertIn(
            "--format",
            str(aes.exceptions[0]),
            msg=f"This error should reference --format: [{aes.exceptions[0]}]",
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
            "load_peak_annotations",
            infile="DataRepo/data/tests/multiple_tracers/6eaafasted1_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            operator="Xianfeng Zeng",
        )
        post_load_group_count = PeakGroup.objects.count()
        # The number of samples in the isocorr xlsx file (not the samples file)
        SAMPLES_COUNT = 2
        # There is no Nitrogen in the tracers, so 7 rows with N in the isotopeLabel do not load with a warning, leaving
        # 14 rows to load.  This was a bug in the accucor_data_loader that the peak annotations loader reveals.
        # PEAKDATA_ROWS = 21
        PEAKDATA_ROWS_WITHOUT_NITROGEN = 14
        PARENT_REC_COUNT = 3
        self.assert_group_data_sample_counts(
            SAMPLES_COUNT,
            PEAKDATA_ROWS_WITHOUT_NITROGEN,
            PARENT_REC_COUNT,
            pre_load_group_count,
            post_load_group_count,
        )

    @MaintainedModel.no_autoupdates()
    def test_multitracer_isocorr_load_2(self):
        self.load_multitracer_data()
        pre_load_group_count = PeakGroup.objects.count()
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/multiple_tracers/bcaafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            operator="Xianfeng Zeng",
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
            "load_peak_annotations",
            infile="DataRepo/data/tests/multiple_labels/alafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            operator="Xianfeng Zeng",
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
            "load_peak_annotations",
            infile="DataRepo/data/tests/multiple_labels/glnfasted1_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            operator="Xianfeng Zeng",
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
        Test to ensure label count 0 entries are not created when measured compound doesn't have that element (succinate
        doesn't have Nitrogen, but Nitrogren was labeled in the tracer).
        """
        self.load_multilabel_data()
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/multiple_labels/alafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            operator="Xianfeng Zeng",
        )
        pg = (
            PeakGroup.objects.filter(msrun_sample__sample__name="xzl5_panc")
            .filter(name__exact="succinate")
            .get(
                peak_annotation_file__filename="alafasted_cor.xlsx",
            )
        )
        self.assertEqual(1, pg.labels.count())


class LoadIsoautocorrCommandTests(TracebaseTestCase):
    # fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]
    # TODO: Implement tests after rebase that brings in Lance's isoautocorr test files
    pass


class LoadUnicorrCommandTests(TracebaseTestCase):
    # fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]
    # TODO: Implement tests that supply files in the converted format
    pass
