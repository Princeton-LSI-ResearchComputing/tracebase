import json

import pandas as pd
from django.core.management import call_command
from django.test import tag
from django.utils import dateparse

from DataRepo.models.maintained_model import (
    disable_buffering,
    enable_buffering,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class QuerysetToPandasDataFrameBaseTests(TracebaseTestCase):
    """
    Do not pass or tag this class or the methods in it.  Instead override any tests in the derived classes and call
    their `super()` version because these tests are re-used under difference conditions
    """

    @classmethod
    def setUpTestData(cls):
        cls.load_data()

    @classmethod
    def load_data(self):
        call_command("load_study", "DataRepo/example_data/test_dataframes/loading.yaml")

    def get_example_study_dict(self):
        exmaple_study_dict = {
            "study": "Study Test1",
            "total_animal": 3,
            "total_tissue": 3,
            "total_sample": 7,
            "total_msrun": 3,
            "sample_owners": ["Xianfeng Zeng"],
            "genotypes": ["C57BL/6N", "WT", "ob/ob"],
        }
        return exmaple_study_dict

    def get_example_animal_dict(self):
        exmaple_animal_dict = {
            "animal": "a1_Lys_13C",
            "infusate_name": "lysine-[13C6][15]",
            "infusion_rate": 0.11,
            "genotype": "WT",
            "body_weight": 26.2,
            "sex": "M",
            "diet": "Diet 1",
            "feeding_status": "fasted",
            "treatment": "no treatment",
            "treatment_category": "animal_treatment",
            "total_tissue": 3,
            "total_sample": 3,
            "total_msrun": 3,
            "sample_owners": ["Xianfeng Zeng"],
            "studies": ["Study Test1", "Study Test2"],
        }
        return exmaple_animal_dict

    def get_example_sample1_dict(self):
        example_sample1_dict = {
            "animal": "a1_Lys_13C",
            "infusate_name": "lysine-[13C6][15]",
            "tracer_group_name": None,
            "tracers": ["lysine-[13C6]"],
            "labeled_elements": ["C"],
            "concentrations": [15],
            "tissue": "kidney",
            "sample": "a1_kd",
            "sample_owner": "Xianfeng Zeng",
            "msrun_owner": "Xianfeng Zeng",
            "msrun_protocol": "Default",
            "studies": ["Study Test1", "Study Test2"],
        }
        return example_sample1_dict

    def get_example_sample2_dict(self):
        example_sample2_dict = {
            "animal": "a2_VLI_13C15N",
            "infusate_name": "BCAAs (VLI) {isoleucine-[13C6,15N1][12];leucine-[13C6,15N1][24];valine-[13C5,15N1][20]}",
            "tissue": "liver",
            "sample": "a2_liv",
            "sample_owner": "Xianfeng Zeng",
        }
        return example_sample2_dict

    def get_example_compound_dict(self):
        example_compound_dict = {
            "compound_name": "lysine",
            "formula": "C6H14N2O2",
            "hmdb_id": "HMDB0000182",
            "synonyms": ["Lysine", "lys", "lysine"],
            "total_animal_by_compound": 2,
            "total_infusate_by_compound": 2,
        }
        return example_compound_dict

    def get_example_infusate_dict(self):
        example_infusate_dict = {
            "infusate_name": "BCAAs (VLI) {isoleucine-[13C6,15N1][12];leucine-[13C6,15N1][24];valine-[13C5,15N1][20]}",
            "tracer_group_name": "BCAAs (VLI)",
            "tracers": [
                "isoleucine-[13C6,15N1]",
                "leucine-[13C6,15N1]",
                "valine-[13C5,15N1]",
            ],
            "concentrations": [12, 24, 20],
            "labeled_elements": ["C,N"],
        }
        return example_infusate_dict

    def test_study_list_stat_df(self):
        """
        get data from the data frame for selected study with selected columns,
        then convert the data to dictionary to compare with the example data.
        """
        example_study_dict = self.get_example_study_dict()
        study_list_stats_df = qs2df.get_study_list_stats_df()
        stud1_list_stats_df = study_list_stats_df[
            study_list_stats_df["study"] == "Study Test1"
        ]
        selected_columns = list(example_study_dict.keys())
        out_df = stud1_list_stats_df[selected_columns]
        stud1_list_stats_dict = qs2df.df_to_list_of_dict(out_df)[0]

        # Sort the lists so that they can be equated
        stud1_list_stats_dict["genotypes"] = sorted(stud1_list_stats_dict["genotypes"])
        example_study_dict["genotypes"] = sorted(example_study_dict["genotypes"])

        self.assertEqual(stud1_list_stats_dict, example_study_dict)

    def test_animal_list_stat_df(self, example_animal_dict=None):
        """
        get data from the data frame for selected animal with selected columns,
        then convert the data to dictionary to compare with the example data.
        test studies as an unordered list
        """

        if example_animal_dict is None:
            example_animal_dict = self.get_example_animal_dict()
        anim_list_stats_df = qs2df.get_animal_list_stats_df()

        anim1_list_stats_df = anim_list_stats_df[
            anim_list_stats_df["animal"] == "a1_Lys_13C"
        ]
        selected_columns = list(example_animal_dict.keys())
        out_df = anim1_list_stats_df[selected_columns]
        anim1_list_stats_dict = qs2df.df_to_list_of_dict(out_df)[0]
        self.assertEqual(anim1_list_stats_dict, example_animal_dict)

    def test_animal_sample_msrun_df(
        self, example_sample1_dict=None, example_sample2_dict=None
    ):
        """
        get data from the data frame for selected sample with selected columns,
        then convert the data to dictionary to compare with the example data.
        test studies as an unordered list
        """
        # get data for examples
        if example_sample1_dict is None:
            example_sample1_dict = self.get_example_sample1_dict()
        if example_sample2_dict is None:
            example_sample2_dict = self.get_example_sample2_dict()

        anim_msrun_df = qs2df.get_animal_msrun_all_df()

        # sample1 test
        sam1_msrun_df = anim_msrun_df[anim_msrun_df["sample"] == "a1_kd"]
        sam1_columns = list(example_sample1_dict.keys())
        out_df = sam1_msrun_df[sam1_columns]
        sam1_msrun_dict = qs2df.df_to_list_of_dict(out_df)[0]
        self.assertEqual(sam1_msrun_dict, example_sample1_dict)

        # sample2 test
        sam2_msrun_df = anim_msrun_df[anim_msrun_df["sample"] == "a2_liv"]
        sam2_columns = list(example_sample2_dict.keys())
        sam2_msrun_sel_dict = sam2_msrun_df[sam2_columns].iloc[0].to_dict()
        sam2_msrun_all_dict = sam2_msrun_df.iloc[0].to_dict()

        self.assertEqual(sam2_msrun_sel_dict, example_sample2_dict)

        # test values for age and sample_time_collected
        # expected values
        expected_sam2_age_week = 21.0
        expected_sam2_time_collected_mins = 120.0

        # verify the values from the Dataframe
        sam2_age_to_week = (sam2_msrun_all_dict["age"]).days // 7

        # age: timedelta64[ns] type in DataFrame vs. isoformat in json output
        sam2_msrun_df_json = sam2_msrun_df.to_json(
            orient="records", date_format="iso", date_unit="ns"
        )
        sam2_msrun_df_json_data = json.loads(sam2_msrun_df_json)
        sam2_age_in_json_data = sam2_msrun_df_json_data[0]["age"]
        sam2_age_in_json_data_to_weeks = (
            dateparse.parse_duration(sam2_age_in_json_data).days // 7
        )
        self.assertEqual(sam2_age_to_week, expected_sam2_age_week)
        self.assertEqual(sam2_age_in_json_data_to_weeks, expected_sam2_age_week)

        # sample_time_collected: timedelta64[ns] type in DataFrame vs. isoformat in json output
        sam2_time_collected_to_mins = (
            sam2_msrun_all_dict["sample_time_collected"]
        ).seconds // 60
        sam2_time_collected_in_json_data = sam2_msrun_df_json_data[0][
            "sample_time_collected"
        ]
        sam2_time_collected_in_json_data_to_mins = (
            dateparse.parse_duration(sam2_time_collected_in_json_data).seconds // 60
        )
        self.assertEqual(sam2_time_collected_to_mins, expected_sam2_time_collected_mins)
        self.assertEqual(
            sam2_time_collected_in_json_data_to_mins, expected_sam2_time_collected_mins
        )

        # sample2 has no MSRun data
        self.assertTrue(sam2_msrun_all_dict["msrun_id"] is pd.NA)
        self.assertTrue(sam2_msrun_all_dict["msrun_owner"] is pd.NA)

    def test_comp_list_stats_df(self):
        """
        get data from the data frame for selected compound with selected columns,
        then convert the data to dictionary to compare with the example data.
        """
        example_compound_dict = self.get_example_compound_dict()

        comp_list_stats_df = qs2df.get_compound_list_stats_df()
        comp1_df = comp_list_stats_df[comp_list_stats_df["compound_name"] == "lysine"]

        selected_columns = list(example_compound_dict.keys())
        out_df = comp1_df[selected_columns]
        comp1_dict = qs2df.df_to_list_of_dict(out_df)[0]

        self.assertEqual(comp1_dict, example_compound_dict)

    def test_infusate_list_df(self):
        """
        get data from the data frame for selected infusate with selected columns,
        then convert the data to dictionary to compare with the example data.
        """
        example_infusate_dict = self.get_example_infusate_dict()

        infusate_list_df = qs2df.get_infusate_list_df()
        inf1_df = infusate_list_df[
            infusate_list_df["infusate_name"] == example_infusate_dict["infusate_name"]
        ]

        selected_columns = list(example_infusate_dict.keys())
        out_df = inf1_df[selected_columns]
        inf1_dict = qs2df.df_to_list_of_dict(out_df)[0]

        self.assertEqual(inf1_dict, example_infusate_dict)


@tag("qs2df")
@tag("multi_working")
class QuerysetToPandasDataFrameTests(QuerysetToPandasDataFrameBaseTests):
    @classmethod
    def setUpTestData(cls):
        enable_buffering()
        super().setUpTestData()


@tag("multi_working")
class QuerysetToPandasDataFrameNullToleranceTests(QuerysetToPandasDataFrameBaseTests):
    @classmethod
    def setUpTestData(cls):
        # Silently dis-allow auto-updates by disabling buffering
        disable_buffering()
        try:
            super().setUpTestData()
        except Exception as e:
            raise e
        finally:
            enable_buffering()

    def test_study_list_stat_df(self):
        super().test_study_list_stat_df()

    def test_animal_list_stat_df(self):
        example_animal_dict = self.get_example_animal_dict()
        example_animal_dict["infusate_name"] = None
        super().test_animal_list_stat_df(example_animal_dict=example_animal_dict)

    def test_animal_sample_msrun_df(self):
        example_sample1_dict = self.get_example_sample1_dict()
        example_sample1_dict["concentrations"] = None
        example_sample1_dict["infusate_name"] = None
        example_sample1_dict["labeled_elements"] = None
        example_sample1_dict["tracers"] = None
        example_sample2_dict = self.get_example_sample2_dict()
        example_sample2_dict["infusate_name"] = None
        super().test_animal_sample_msrun_df(
            example_sample1_dict=example_sample1_dict,
            example_sample2_dict=example_sample2_dict,
        )

    def test_infusate_list_df(self):
        with self.assertRaises(IndexError):
            super().test_infusate_list_df()
