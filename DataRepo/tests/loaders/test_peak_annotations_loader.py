import pandas as pd

from DataRepo.loaders.peak_annotations_loader import (
    AccucorLoader,
    IsoautocorrLoader,
    IsocorrLoader,
    PeakAnnotationsLoader,
)
from DataRepo.models.compound import Compound
from DataRepo.models.sample import Sample
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    DuplicateCompoundIsotopes,
    DuplicateValues,
    MissingCompounds,
    MissingSamples,
    NoSamples,
    RecordDoesNotExist,
    UnexpectedSamples,
    UnskippedBlanks,
)


class PeakAnnotationsLoaderTests(TracebaseTestCase):
    def test_initialize_msrun_data(self):
        # TODO: Implement test
        pass

    def test_load_data(self):
        # TODO: Implement test
        pass

    def test_get_or_create_annot_file(self):
        # TODO: Implement test
        pass

    def test_get_or_create_peak_group(self):
        # TODO: Implement test
        pass

    def test_get_msrun_sample_no_annot_details_df(self):
        # TODO: Implement test
        pass

    def test_get_msrun_sample_with_blank_skip(self):
        # TODO: Implement test
        pass

    def test_get_or_create_peak_data(self):
        # TODO: Implement test
        pass

    def test_get_or_create_labels(self):
        # TODO: Implement test
        pass

    def test_get_or_create_peak_group_label(self):
        # TODO: Implement test
        pass

    def test_get_or_create_peak_data_label(self):
        # TODO: Implement test
        pass

    def test_get_or_create_peak_group_compound_link(self):
        # TODO: Implement test
        pass

    def test_handle_file_exceptions(self):
        al = AccucorLoader()
        # Buffer a duplicate values exception to assert it is summarized as a DuplicateCompoundIsotopes exception
        al.aggregated_errors_object.buffer_error(
            DuplicateValues(
                {
                    "s1, Lysine, C13-label-2": [13],
                },
                [
                    al.SAMPLEHEADER_KEY,
                    al.COMPOUND_KEY,
                    al.ISOTOPELABEL_KEY,
                ],
            )
        )
        # Buffer a RecordDoesNotExist exception for the Compound model to assert that it is summarized in a
        # MissingCompounds exception
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Compound,
                Compound.get_name_query_expression("Lysine"),
                rownum=3,
                column="compound",
                file="accucor.xlsx",
            )
        )
        # Set up a RecordDoesNotExist exception for the Sample model to assert it is summarized as a NoSamples exception
        al.msrun_sample_dict["s1"] = {}
        al.msrun_sample_dict["s1"]["seen"] = True
        al.msrun_sample_dict["s1"]["MSRunSample"] = None
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s1"},
                rownum=3,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )

        al.handle_file_exceptions()

        self.assertEqual(3, len(al.aggregated_errors_object.exceptions))
        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(DuplicateCompoundIsotopes)
        )
        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(MissingCompounds)
        )
        self.assertTrue(al.aggregated_errors_object.exception_type_exists(NoSamples))

    def test_report_discrepant_headers_some(self):
        al = AccucorLoader()
        # s1 = "loaded"
        # s2 = missing
        # s3 = unexpected
        # blank - blank
        for sample_header in ["s1", "s2", "s3", "blank"]:
            al.msrun_sample_dict[sample_header] = {}
            # This sample was encountered during processing
            al.msrun_sample_dict[sample_header]["seen"] = True
            # No MSRunSample record was created for it
            al.msrun_sample_dict[sample_header]["MSRunSample"] = None
        # Change this sample to have not been encountered (i.e. it was in the peak annotation details, but not in the
        # accucor file)
        al.msrun_sample_dict["s3"]["seen"] = False
        # Change s1 to having has an MSRunSample record created for it
        al.msrun_sample_dict["s1"][
            "MSRunSample"
        ] = "not None"  # simulate "found"/"loaded"

        # Buffer errors aboud having search for, but not found Sample records
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s2"},
                rownum=3,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "blank"},
                rownum=4,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )

        al.report_discrepant_headers()

        self.assertEqual(3, len(al.aggregated_errors_object.exceptions))
        self.assertFalse(
            al.aggregated_errors_object.exception_type_exists(RecordDoesNotExist)
        )

        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(MissingSamples)
        )
        self.assertEqual(
            ["s2"],
            al.aggregated_errors_object.get_exception_type(MissingSamples)[
                0
            ].missing_samples,
        )

        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(UnskippedBlanks)
        )
        self.assertEqual(
            ["blank"],
            al.aggregated_errors_object.get_exception_type(UnskippedBlanks)[
                0
            ].missing_samples,
        )

        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(UnexpectedSamples)
        )
        self.assertEqual(
            ["s3"],
            al.aggregated_errors_object.get_exception_type(UnexpectedSamples)[
                0
            ].missing_samples,
        )

    def test_report_discrepant_headers_all(self):
        al = AccucorLoader()
        al.msrun_sample_dict["s1"] = {}
        al.msrun_sample_dict["s1"]["seen"] = True
        al.msrun_sample_dict["s1"]["MSRunSample"] = None

        # Buffer errors aboud having search for, but not found Sample records
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s1"},
                rownum=3,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )

        al.report_discrepant_headers()

        self.assertTrue(al.aggregated_errors_object.exception_type_exists(NoSamples))
        self.assertEqual(
            ["s1"],
            al.aggregated_errors_object.get_exception_type(NoSamples)[
                0
            ].missing_samples,
        )

    def test_is_a_blank(self):
        self.assertTrue(PeakAnnotationsLoader.is_a_blank("a Blank sample"))
        self.assertFalse(PeakAnnotationsLoader.is_a_blank("sample1"))


class DerivedPeakAnnotationsLoaderTestCase(TracebaseTestCase):
    """This just contains data to be used in the AccucorLoaderTests, IsocorrLoaderTests, and IsoautocorrLoaderTests."""

    COMMON_DICT = {
        "medMz": [104.035217, 105.038544, 74.024673, 75.028],
        "medRt": [12.73, 12.722, 12.621, 12.614],
        "isotopeLabel": ["C12 PARENT", "C13-label-1", "C12 PARENT", "C13-label-1"],
        "compound": ["Serine", "Serine", "Glycine", "Glycine"],
        "formula": ["C3H7NO3", "C3H7NO3", "C2H5NO2", "C2H5NO2"],
    }

    RAW_SAMPLES = {
        "blank_1_404020": [1, 2, 3, 4],
        "072920_XXX1_1_TS1": [5, 6, 7, 8],
        "072920_XXX1_2_bra": [9, 10, 11, 12],
    }

    CORRECTED_SAMPLES = {
        "blank_1_404020": [966.2099201, 0, 1230.735038, 0],
        "072920_XXX1_1_TS1": [124298.5248, 393.3480206, 90053.99839, 0],
        "072920_XXX1_2_bra": [2106922.129, 0, 329490.6364, 4910.491834],
    }

    def get_converted_with_raw_df(self):
        return pd.DataFrame.from_dict(
            {
                "MedMz": [
                    104.035217,
                    105.038544,
                    74.024673,
                    75.028,
                    104.035217,
                    105.038544,
                    74.024673,
                    75.028,
                    104.035217,
                    105.038544,
                    74.024673,
                    75.028,
                ],
                "MedRt": [
                    12.73,
                    12.722,
                    12.621,
                    12.614,
                    12.73,
                    12.722,
                    12.621,
                    12.614,
                    12.73,
                    12.722,
                    12.621,
                    12.614,
                ],
                "IsotopeLabel": [
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                ],
                "Formula": [
                    "C3H7NO3",
                    "C3H7NO3",
                    "C2H5NO2",
                    "C2H5NO2",
                    "C3H7NO3",
                    "C3H7NO3",
                    "C2H5NO2",
                    "C2H5NO2",
                    "C3H7NO3",
                    "C3H7NO3",
                    "C2H5NO2",
                    "C2H5NO2",
                ],
                "Compound": [
                    "Serine",
                    "Serine",
                    "Glycine",
                    "Glycine",
                    "Serine",
                    "Serine",
                    "Glycine",
                    "Glycine",
                    "Serine",
                    "Serine",
                    "Glycine",
                    "Glycine",
                ],
                "Sample Header": [
                    "blank_1_404020",
                    "blank_1_404020",
                    "blank_1_404020",
                    "blank_1_404020",
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_2_bra",
                    "072920_XXX1_2_bra",
                    "072920_XXX1_2_bra",
                    "072920_XXX1_2_bra",
                ],
                "Raw Abundance": [
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                ],
                "Corrected Abundance": [
                    966.2099201,
                    0,
                    1230.735038,
                    0,
                    124298.5248,
                    393.3480206,
                    90053.99839,
                    0,
                    2106922.129,
                    0,
                    329490.6364,
                    4910.491834,
                ],
            },
        )

    def get_converted_without_raw_df(self):
        return (
            self.get_converted_with_raw_df()
            .copy(deep=True)
            .drop(["Raw Abundance"], axis=1, errors="ignore")
        )


class IsocorrLoaderTests(DerivedPeakAnnotationsLoaderTestCase):
    ABSO_DICT = {
        **DerivedPeakAnnotationsLoaderTestCase.COMMON_DICT,
        **DerivedPeakAnnotationsLoaderTestCase.CORRECTED_SAMPLES,
    }
    DF_DICT = {
        "absolte": pd.DataFrame.from_dict(ABSO_DICT),
    }

    def test_IsocorrLoader(self):
        il = IsocorrLoader(df=self.DF_DICT, file="isocorr.xlsx")
        pd.testing.assert_frame_equal(
            self.get_converted_without_raw_df(), il.df, check_like=True
        )


class AccucorLoaderTests(DerivedPeakAnnotationsLoaderTestCase):
    ORIG_DICT = {
        **DerivedPeakAnnotationsLoaderTestCase.COMMON_DICT,
        **DerivedPeakAnnotationsLoaderTestCase.RAW_SAMPLES,
    }
    CORR_DICT = {
        "Compound": ["Serine", "Serine", "Glycine", "Glycine"],
        "C_Label": [0, 1, 0, 1],
        **DerivedPeakAnnotationsLoaderTestCase.CORRECTED_SAMPLES,
    }
    DF_DICT = {
        "Original": pd.DataFrame.from_dict(ORIG_DICT),
        "Corrected": pd.DataFrame.from_dict(CORR_DICT),
    }

    def test_AccucorLoader(self):
        il = AccucorLoader(df=self.DF_DICT, file="accucor.xlsx")
        pd.testing.assert_frame_equal(
            self.get_converted_with_raw_df(), il.df, check_like=True
        )


class IsoautocorrLoaderTests(DerivedPeakAnnotationsLoaderTestCase):
    ORIG_DICT = {
        **DerivedPeakAnnotationsLoaderTestCase.COMMON_DICT,
        **DerivedPeakAnnotationsLoaderTestCase.RAW_SAMPLES,
    }
    CORR_DICT = {
        **DerivedPeakAnnotationsLoaderTestCase.COMMON_DICT,
        **DerivedPeakAnnotationsLoaderTestCase.CORRECTED_SAMPLES,
    }
    DF_DICT = {
        "original": pd.DataFrame.from_dict(ORIG_DICT),
        "cor_pct": pd.DataFrame.from_dict(CORR_DICT),
    }

    def test_IsoautocorrLoader(self):
        il = IsoautocorrLoader(df=self.DF_DICT, file="isoautocorr.xlsx")
        pd.testing.assert_frame_equal(
            self.get_converted_with_raw_df(), il.df, check_like=True
        )
