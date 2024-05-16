import pandas as pd

from DataRepo.loaders.peak_annotations_loader import (
    AccucorLoader,
    IsocorrLoader,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import RequiredHeadersError


class PeakAnnotationsLoaderTests(TracebaseTestCase):
    ORIG_DICT = {
        "medMz": [104.035217, 105.038544, 74.024673, 75.028],
        "medRt": [12.73, 12.722, 12.621, 12.614],
        "isotopeLabel": ["C12 PARENT", "C13-label-1", "C12 PARENT", "C13-label-1"],
        "compound": ["Serine", "Serine", "Glycine", "Glycine"],
        "formula": ["C3H7NO3", "C3H7NO3", "C2H5NO2", "C2H5NO2"],
    }
    CORR_DICT = {
        "Compound": ["Serine", "Serine", "Glycine", "Glycine"],
        "C_Label": [0, 1, 0, 1],
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
        AccucorLoader().add_df_columns(tmpdf)
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
        IsocorrLoader().add_df_columns(tmpdf)
        pd.testing.assert_frame_equal(self.ISOCORR_DF_DICT["absolte"], tmpdf["absolte"])
        self.assertEqual(len(self.ISOCORR_DF_DICT.keys()), len(tmpdf.keys()))

    def test_merge_df_sheets_accucor(self):
        outdf = AccucorLoader().merge_df_sheets(
            self.get_accucor_df_with_added_columns()
        )
        expected = pd.DataFrame.from_dict(
            {
                "medMz": [104.035217, 105.038544, 74.024673, 75.028],
                "medRt": [12.73, 12.722, 12.621, 12.614],
                "isotopeLabel": [
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                ],
                "compound": ["Serine", "Serine", "Glycine", "Glycine"],
                "formula": ["C3H7NO3", "C3H7NO3", "C2H5NO2", "C2H5NO2"],
                "Compound": ["Serine", "Serine", "Glycine", "Glycine"],
                "C_Label": [0, 1, 0, 1],
                "blank_1_404020": [966.2099201, 0, 1230.735038, 0],
                "072920_XXX1_1_TS1": [124298.5248, 393.3480206, 90053.99839, 0],
                "072920_XXX1_2_bra": [2106922.129, 0, 329490.6364, 4910.491834],
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
        outdf = IsocorrLoader().merge_df_sheets(tmpdf)
        # We should get back the dataframe of the absolte sheet, unchanged
        pd.testing.assert_frame_equal(self.ISOCORR_DF_DICT["absolte"], outdf)

    def get_converted_accucor_df(self):
        df = pd.DataFrame.from_dict(
            {
                "MedMz": [104.035217, 105.038544, 74.024673, 75.028],
                "MedRt": [12.73, 12.722, 12.621, 12.614],
                "IsotopeLabel": [
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                ],
                "Formula": ["C3H7NO3", "C3H7NO3", "C2H5NO2", "C2H5NO2"],
                "Compound": ["Serine", "Serine", "Glycine", "Glycine"],
                "blank_1_404020": [966.2099201, 0, 1230.735038, 0],
                "072920_XXX1_1_TS1": [124298.5248, 393.3480206, 90053.99839, 0],
                "072920_XXX1_2_bra": [2106922.129, 0, 329490.6364, 4910.491834],
            },
        )
        return df.reindex(sorted(df.columns), axis=1)

    def test_convert_df_accucor_excel(self):
        tmpdf = dict(
            (sheet, adf.copy(deep=True)) for sheet, adf in self.ACCUCOR_DF_DICT.items()
        )
        outdf = AccucorLoader().convert_df(tmpdf)
        expected = self.get_converted_accucor_df()

        # Sort the columns of both dataframes (because the column order doesn't matter)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)

        pd.testing.assert_frame_equal(expected, outdf)

    def test_convert_df_accucor_tsv(self):
        with self.assertRaises(RequiredHeadersError) as ar:
            AccucorLoader().convert_df(self.ACCUCOR_DF_DICT["Corrected"])
        self.assertIn(
            "missing: ['MedMz', 'MedRt', 'IsotopeLabel', 'Formula']", str(ar.exception)
        )

    def get_converted_isocorr_df(self):
        df = pd.DataFrame.from_dict(
            {
                "Formula": ["C4H6O4", "C4H6O4", "C5H10N2O3", "C5H10N2O3"],
                "MedMz": [117.0192, 118.0225, 145.0617, 147.062],
                "MedRt": [12.17531, 12.17531, 12.62156, 12.64316],
                "IsotopeLabel": [
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13N15-label-1-1",
                ],
                "Compound": ["succinate", "succinate", "glutamine", "glutamine"],
                "xz971_bat": [148238551.1, 517370.4887, 15618575.73, 0],
                "xz971_br": [74223142.68, 310838.2786, 37308464.02, 0],
            },
        )
        return df.reindex(sorted(df.columns), axis=1)

    def test_convert_df_isocorr_excel(self):
        tmpdf = dict(
            (sheet, adf.copy(deep=True)) for sheet, adf in self.ISOCORR_DF_DICT.items()
        )
        expected = self.get_converted_isocorr_df()
        outdf = IsocorrLoader().convert_df(tmpdf)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_convert_df_isocorr_tsv(self):
        tmpdf = self.ISOCORR_DF_DICT["absolte"].copy(deep=True)
        expected = self.get_converted_isocorr_df()
        outdf = IsocorrLoader().convert_df(tmpdf)
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
        outdf = AccucorLoader().convert_df(tmpdf)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_extra_columns_excluded_accucor_tsv(self):
        tmporig = self.ORIG_DICT.copy()
        tmporig["metaGroupId"] = [2, 2, 3, 3]
        tmpcorr = self.CORR_DICT.copy()
        tmpcorr["adductName"] = [0, 0, 0, 0]
        # Merge the sheets (they happen to have the rows in the same order)
        tmpdict = dict(**tmporig, **tmpcorr)
        tmpdf = pd.DataFrame.from_dict(tmpdict)
        expected = self.get_converted_accucor_df()
        outdf = AccucorLoader().convert_df(tmpdf)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_extra_columns_excluded_isocorr_excel(self):
        tmpabso = self.ABSO_DICT.copy()
        tmpabso["metaGroupId"] = [2, 2, 3, 3]
        tmpdf = {
            "absolte": pd.DataFrame.from_dict(tmpabso),
        }
        expected = self.get_converted_isocorr_df()
        outdf = IsocorrLoader().convert_df(tmpdf)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_extra_columns_excluded_isocorr_tsv(self):
        tmpabso = self.ABSO_DICT.copy()
        tmpabso["metaGroupId"] = [2, 2, 3, 3]
        tmpdf = pd.DataFrame.from_dict(tmpabso)
        expected = self.get_converted_isocorr_df()
        outdf = IsocorrLoader().convert_df(tmpdf)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)
        pd.testing.assert_frame_equal(expected, outdf)

    def test_AccucorLoader_constructor_conversion(self):
        al = AccucorLoader(df=self.ACCUCOR_DF_DICT)
        outdf = al.df
        expected = self.get_converted_accucor_df()

        # Sort the columns of both dataframes (because the column order doesn't matter)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)

        pd.testing.assert_frame_equal(expected, outdf)

    def test_IsocorrLoader_constructor_conversion(self):
        il = IsocorrLoader(df=self.ISOCORR_DF_DICT)
        outdf = il.df
        expected = self.get_converted_isocorr_df()

        # Sort the columns of both dataframes (because the column order doesn't matter)
        outdf = outdf.reindex(sorted(outdf.columns), axis=1)

        pd.testing.assert_frame_equal(expected, outdf)
