from collections import namedtuple

import pandas as pd
from django.db.models import AutoField, CharField, Model
from django.test.utils import isolate_apps

from DataRepo.loaders.base.converted_table_loader import ConvertedTableLoader
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import AggregatedErrors, RequiredHeadersError


@isolate_apps("DataRepo.tests.apps.loader")
class TestConvertedLoaderTests(TracebaseTestCase):
    # Going to create independent copies of the TestConvertedLoader1 and TestConvertedLoader2 classes in order to make
    # these tests independent

    # These are initialized in setUpTestData()
    TestConvertedLoader1 = None
    TestConvertedLoader2 = None

    @classmethod
    def generate_test_model(cls):
        # Model used for testing
        class TestCvtrModel(Model):
            id = AutoField(primary_key=True)
            test = CharField(unique=True)

            class Meta:
                app_label = "loader"

        return TestCvtrModel

    @classmethod
    def generate_test_table_loader(cls, mdl):
        class TestTableLoader(ConvertedTableLoader):
            DataSheetName = "test"
            DataTableHeaders = namedtuple(
                "DataTableHeaders",
                [
                    "MEDMZ",
                    "MEDRT",
                    "ISOTOPELABEL",
                    "FORMULA",
                    "COMPOUND",
                    "SAMPLEHEADER",
                    "RAW",
                    "CORRECTED",
                ],
            )
            DataHeaders = DataTableHeaders(
                MEDMZ="MedMz",
                MEDRT="MedRt",
                ISOTOPELABEL="IsotopeLabel",
                FORMULA="Formula",
                COMPOUND="Compound",
                SAMPLEHEADER="mzXML Name",
                RAW="Raw Abundance",
                CORRECTED="Corrected Abundance",
            )
            DataRequiredHeaders = [
                "MEDMZ",
                "MEDRT",
                "ISOTOPELABEL",
                "FORMULA",
                "COMPOUND",
                "SAMPLEHEADER",
                "CORRECTED",
            ]
            DataRequiredValues = DataRequiredHeaders
            DataUniqueColumnConstraints = []
            FieldToDataHeaderKey = {}
            DataColumnMetadata = DataTableHeaders(
                MEDMZ=None,
                MEDRT=None,
                ISOTOPELABEL=None,
                FORMULA=None,
                COMPOUND=None,
                SAMPLEHEADER=None,
                RAW=None,
                CORRECTED=None,
            )
            Models = [mdl]

            def load_data(self):
                pass

        return TestTableLoader

    @classmethod
    def generate_test_converted_loader1(cls, TestTableLoader):
        class TestConvertedLoader1(TestTableLoader):
            merged_column_rename_dict = {
                "formula": "Formula",
                "medMz": "MedMz",
                "medRt": "MedRt",
                "isotopeLabel": "IsotopeLabel",
            }

            merged_drop_columns_list = [
                "compound",
                "C_Label",
                "adductName",
                "metaGroupId",
            ]

            condense_columns_dict = {
                "Original": {
                    "header_column": "mzXML Name",
                    "value_column": "Raw Abundance",
                    "uncondensed_columns": [
                        "medMz",
                        "medRt",
                        "isotopeLabel",
                        "compound",
                        "formula",
                        "Compound",  # From add_columns_dict
                        "C_Label",  # From add_columns_dict
                        "metaGroupId",
                    ],
                },
                "Corrected": {
                    "header_column": "mzXML Name",
                    "value_column": "Corrected Abundance",
                    "uncondensed_columns": [
                        "Compound",
                        "C_Label",
                        "adductName",
                    ],
                },
            }

            add_columns_dict = {
                "Original": {
                    "C_Label": (
                        lambda df: df["isotopeLabel"]
                        .str.split("-")
                        .str.get(-1)
                        .replace({"C12 PARENT": "0"})
                        .astype(int)
                    ),
                    "Compound": lambda df: df["compound"],
                }
            }

            merge_dict = {
                "first_sheet": "Corrected",
                "next_merge_dict": {
                    "on": ["Compound", "C_Label", "mzXML Name"],
                    "left_columns": None,
                    "right_sheet": "Original",
                    "right_columns": [
                        "formula",
                        "medMz",
                        "medRt",
                        "isotopeLabel",
                        "Raw Abundance",
                    ],
                    "how": "left",
                    "next_merge_dict": None,
                },
            }

        return TestConvertedLoader1

    @classmethod
    def generate_test_converted_loader2(cls, TestTableLoader):
        class TestConvertedLoader2(TestTableLoader):
            merged_column_rename_dict = {
                "formula": "Formula",
                "medMz": "MedMz",
                "medRt": "MedRt",
                "isotopeLabel": "IsotopeLabel",
                "compound": "Compound",
            }

            merged_drop_columns_list = [
                "compound",
                "metaGroupId",
            ]

            condense_columns_dict = {
                "absolte": {
                    "header_column": "mzXML Name",
                    "value_column": "Corrected Abundance",
                    "uncondensed_columns": [
                        "formula",
                        "medMz",
                        "medRt",
                        "isotopeLabel",
                        "compound",
                        "metaGroupId",
                        "adductName",
                    ],
                },
            }

            add_columns_dict = None

            merge_dict = {
                "first_sheet": "absolte",
                "next_merge_dict": None,
            }

        return TestConvertedLoader2

    ORIG_DICT = {
        "medMz": [104.035217, 105.038544, 74.024673, 75.028],
        "medRt": [12.73, 12.722, 12.621, 12.614],
        "isotopeLabel": ["C12 PARENT", "C13-label-1", "C12 PARENT", "C13-label-1"],
        "compound": ["Serine", "Serine", "Glycine", "Glycine"],
        "formula": ["C3H7NO3", "C3H7NO3", "C2H5NO2", "C2H5NO2"],
        "metaGroupId": [2, 2, 3, 3],
        "blank_1_404020": [1, 2, 3, 4],
        "072920_XXX1_1_TS1": [5, 6, 7, 8],
        "072920_XXX1_2_bra": [9, 10, 11, 12],
    }
    CORR_DICT = {
        "Compound": ["Serine", "Serine", "Glycine", "Glycine"],
        "C_Label": [0, 1, 0, 1],
        "adductName": ["x", "x", "x", "x"],
        "blank_1_404020": [966.2099201, 0, 1230.735038, 0],
        "072920_XXX1_1_TS1": [124298.5248, 393.3480206, 90053.99839, 0],
        "072920_XXX1_2_bra": [2106922.129, 0, 329490.6364, 4910.491834],
    }
    ACCUCOR_DF_DICT = {
        "Original": pd.DataFrame.from_dict(ORIG_DICT),
        "Corrected": pd.DataFrame.from_dict(CORR_DICT),
    }
    ABSO_DICT = {
        "formula": ["C4H6O4", "C4H6O4", "C5H10N2O3", "C5H10N2O3"],
        "medMz": [117.0192, 118.0225, 145.0617, 147.062],
        "medRt": [12.17531, 12.17531, 12.62156, 12.64316],
        "isotopeLabel": ["C12 PARENT", "C13-label-1", "C12 PARENT", "C13N15-label-1-1"],
        "compound": ["succinate", "succinate", "glutamine", "glutamine"],
        "xz971_bat": [148238551.1, 517370.4887, 15618575.73, 0],
        "xz971_br": [74223142.68, 310838.2786, 37308464.02, 0],
    }
    ISOCORR_DF_DICT = {
        "absolte": pd.DataFrame.from_dict(ABSO_DICT),
    }

    @classmethod
    def setUpTestData(cls):
        mdl = cls.generate_test_model()
        TestTableLoader = cls.generate_test_table_loader(mdl)
        cls.TestConvertedLoader1 = cls.generate_test_converted_loader1(TestTableLoader)
        cls.TestConvertedLoader2 = cls.generate_test_converted_loader2(TestTableLoader)
        super().setUpTestData()

    def get_accucor_df_with_added_columns(self):
        expected_orig_dict = self.ORIG_DICT.copy()
        expected_orig_dict["Compound"] = self.ORIG_DICT["compound"]
        expected_orig_dict["C_Label"] = [0, 1, 0, 1]
        expected_orig_df = pd.DataFrame.from_dict(expected_orig_dict)
        # Sort the columns (because the column order doesn't matter)
        expected_orig_df = expected_orig_df.reindex(
            sorted(expected_orig_df.columns), axis=1
        )
        return {
            "Original": expected_orig_df,
            "Corrected": self.ACCUCOR_DF_DICT["Corrected"],
        }

    def test_add_df_columns_accucor(self):
        tmpdf = dict(
            (sheet, adf.copy(deep=True)) for sheet, adf in self.ACCUCOR_DF_DICT.items()
        )
        self.TestConvertedLoader1().add_df_columns(  # pylint: disable=not-callable
            tmpdf
        )
        expected_df_dict = self.get_accucor_df_with_added_columns()

        # Sort the columns (because the column order doesn't matter)
        tmpdf["Original"] = tmpdf["Original"].reindex(
            sorted(tmpdf["Original"].columns), axis=1
        )

        pd.testing.assert_frame_equal(expected_df_dict["Original"], tmpdf["Original"])
        pd.testing.assert_frame_equal(expected_df_dict["Corrected"], tmpdf["Corrected"])
        self.assertEqual(len(self.ACCUCOR_DF_DICT.keys()), len(tmpdf.keys()))

    def test_add_df_columns_isocorr(self):
        tmpdf = dict(
            (sheet, adf.copy(deep=True)) for sheet, adf in self.ISOCORR_DF_DICT.items()
        )
        self.TestConvertedLoader2().add_df_columns(  # pylint: disable=not-callable
            tmpdf
        )
        pd.testing.assert_frame_equal(self.ISOCORR_DF_DICT["absolte"], tmpdf["absolte"])
        self.assertEqual(len(self.ISOCORR_DF_DICT.keys()), len(tmpdf.keys()))

    def test_merge_df_sheets_accucor(self):
        indf = self.get_accucor_df_with_added_columns()
        # I'm being lazy by calling condense_columns to have data to test merge_df_sheets.  I really should be
        # generating the indf from scratch
        indf = self.TestConvertedLoader1().condense_columns(  # pylint: disable=not-callable
            indf
        )
        outdf = (
            self.TestConvertedLoader1().merge_df_sheets(  # pylint: disable=not-callable
                indf
            )
        )

        # We're going to massage the fully converted dict to back it up to partially converted (instead of declare a
        # whole new structure).  So we're converting the output to a dict...
        expected_dict = self.get_converted_accucor_df().to_dict()
        # This is before the extra columns have been removed, so add C_Label and adductName
        expected_dict["C_Label"] = {
            0: 0,
            1: 1,
            2: 0,
            3: 1,
            4: 0,
            5: 1,
            6: 0,
            7: 1,
            8: 0,
            9: 1,
            10: 0,
            11: 1,
        }
        expected_dict["adductName"] = {
            0: "x",
            1: "x",
            2: "x",
            3: "x",
            4: "x",
            5: "x",
            6: "x",
            7: "x",
            8: "x",
            9: "x",
            10: "x",
            11: "x",
        }
        # Now convert back to a dataframe
        expected = pd.DataFrame.from_dict(expected_dict)
        # Revert the renames
        expected = expected.rename(
            columns={
                "MedMz": "medMz",
                "MedRt": "medRt",
                "IsotopeLabel": "isotopeLabel",
                "Formula": "formula",
            }
        )

        # Sort the columns of both dataframes (because the column order doesn't matter)
        expected = expected.reindex(sorted(expected.columns), axis=1)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)

        pd.testing.assert_frame_equal(expected, outdf)

    def test_merge_df_sheets_isocorr(self):
        tmpdf = dict(
            (sheet, adf.copy(deep=True)) for sheet, adf in self.ISOCORR_DF_DICT.items()
        )
        outdf = (
            self.TestConvertedLoader2().merge_df_sheets(  # pylint: disable=not-callable
                tmpdf
            )
        )
        # We should get back the dataframe of the absolte sheet, unchanged
        pd.testing.assert_frame_equal(self.ISOCORR_DF_DICT["absolte"], outdf)

    def get_converted_accucor_df(self):
        df = pd.DataFrame.from_dict(
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
                "mzXML Name": [
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
        return df.reindex(sorted(df.columns), axis=1)

    def test_convert_df_accucor_excel(self):
        tmpdf = dict(
            (sheet, adf.copy(deep=True)) for sheet, adf in self.ACCUCOR_DF_DICT.items()
        )
        outdf = self.TestConvertedLoader1(  # pylint: disable=not-callable
            df=tmpdf
        ).convert_df()
        expected = self.get_converted_accucor_df()

        # Sort the columns of both dataframes (because the column order doesn't matter)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)

        pd.testing.assert_frame_equal(expected, outdf)

    def test_convert_df_accucor_tsv(self):
        """The user provides only a single sheet (Corrected), so we expect to get an exception about missing headers."""
        with self.assertRaises(AggregatedErrors) as ar:
            self.TestConvertedLoader1(  # pylint: disable=not-callable
                df=self.ACCUCOR_DF_DICT["Corrected"]
            ).convert_df()
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertIsInstance(aes.exceptions[0], RequiredHeadersError)
        self.assertIn(
            (
                "missing: {'Unnamed sheet': ['medMz', 'medRt', 'isotopeLabel', 'formula', 'mzXML Name', "
                "'Corrected Abundance']}"
            ),
            str(aes.exceptions[0]),
        )

    def get_converted_isocorr_df(self):
        df = pd.DataFrame.from_dict(
            {
                "Formula": [
                    "C4H6O4",
                    "C4H6O4",
                    "C5H10N2O3",
                    "C5H10N2O3",
                    "C4H6O4",
                    "C4H6O4",
                    "C5H10N2O3",
                    "C5H10N2O3",
                ],
                "MedMz": [
                    117.0192,
                    118.0225,
                    145.0617,
                    147.062,
                    117.0192,
                    118.0225,
                    145.0617,
                    147.062,
                ],
                "MedRt": [
                    12.17531,
                    12.17531,
                    12.62156,
                    12.64316,
                    12.17531,
                    12.17531,
                    12.62156,
                    12.64316,
                ],
                "IsotopeLabel": [
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13N15-label-1-1",
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13N15-label-1-1",
                ],
                "mzXML Name": [
                    "xz971_bat",
                    "xz971_bat",
                    "xz971_bat",
                    "xz971_bat",
                    "xz971_br",
                    "xz971_br",
                    "xz971_br",
                    "xz971_br",
                ],
                "Compound": [
                    "succinate",
                    "succinate",
                    "glutamine",
                    "glutamine",
                    "succinate",
                    "succinate",
                    "glutamine",
                    "glutamine",
                ],
                "Corrected Abundance": [
                    148238551.1,
                    517370.4887,
                    15618575.73,
                    0,
                    74223142.68,
                    310838.2786,
                    37308464.02,
                    0,
                ],
            },
        )
        return df.reindex(sorted(df.columns), axis=1)

    def test_convert_df_isocorr_excel(self):
        tmpdf = dict(
            (sheet, adf.copy(deep=True)) for sheet, adf in self.ISOCORR_DF_DICT.items()
        )
        expected = self.get_converted_isocorr_df()
        outdf = self.TestConvertedLoader2(  # pylint: disable=not-callable
            df=tmpdf
        ).convert_df()
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_convert_df_isocorr_tsv(self):
        tmpdf = self.ISOCORR_DF_DICT["absolte"].copy(deep=True)
        expected = self.get_converted_isocorr_df()
        outdf = self.TestConvertedLoader2(  # pylint: disable=not-callable
            df=tmpdf
        ).convert_df()
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_extra_columns_excluded_accucor_excel(self):
        tmporig = self.ORIG_DICT.copy()
        tmporig["metaGroupId"] = [2, 2, 3, 3]
        tmpcorr = self.CORR_DICT.copy()
        tmpcorr["adductName"] = [0, 0, 0, 0]
        tmpdf = {
            "Original": pd.DataFrame.from_dict(tmporig),
            "Corrected": pd.DataFrame.from_dict(tmpcorr),
        }
        expected = self.get_converted_accucor_df()
        outdf = self.TestConvertedLoader1(  # pylint: disable=not-callable
            df=tmpdf
        ).convert_df()
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_extra_columns_excluded_accucor_tsv(self):
        tmporig = self.ORIG_DICT.copy()
        tmporig["metaGroupId"] = [2, 2, 3, 3]
        tmpcorr = self.CORR_DICT.copy()
        tmpcorr["adductName"] = [0, 0, 0, 0]
        tmpdict = {
            "Original": pd.DataFrame.from_dict(tmporig),
            "Corrected": pd.DataFrame.from_dict(tmpcorr),
        }
        expected = self.get_converted_accucor_df()
        outdf = self.TestConvertedLoader1(  # pylint: disable=not-callable
            df=tmpdict
        ).convert_df()
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_extra_columns_excluded_isocorr_excel(self):
        tmpabso = self.ABSO_DICT.copy()
        tmpabso["metaGroupId"] = [2, 2, 3, 3]
        tmpdf = {
            "absolte": pd.DataFrame.from_dict(tmpabso),
        }
        expected = self.get_converted_isocorr_df()
        outdf = self.TestConvertedLoader2(  # pylint: disable=not-callable
            df=tmpdf
        ).convert_df()
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_extra_columns_excluded_isocorr_tsv(self):
        tmpabso = self.ABSO_DICT.copy()
        tmpabso["metaGroupId"] = [2, 2, 3, 3]
        tmpdf = pd.DataFrame.from_dict(tmpabso)
        expected = self.get_converted_isocorr_df()
        outdf = self.TestConvertedLoader2(  # pylint: disable=not-callable
            df=tmpdf
        ).convert_df()
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_constructor_conversion_merge_sheets(self):
        al = self.TestConvertedLoader1(  # pylint: disable=not-callable
            df=self.ACCUCOR_DF_DICT, file="test.xlsx"
        )
        outdf = al.df
        expected = self.get_converted_accucor_df()

        # Sort the columns of both dataframes (because the column order doesn't matter)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)

        pd.testing.assert_frame_equal(expected, outdf)

    def test_constructor_conversion_single_sheet(self):
        il = self.TestConvertedLoader2(  # pylint: disable=not-callable
            df=self.ISOCORR_DF_DICT, file="test.xlsx"
        )
        outdf = il.df
        expected = self.get_converted_isocorr_df()

        # Sort the columns of both dataframes (because the column order doesn't matter)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)

        pd.testing.assert_frame_equal(expected, outdf)
