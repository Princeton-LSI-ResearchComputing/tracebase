import json

import pandas as pd
from django.conf import settings
from django.core.management import call_command
from django.test import TestCase
from django.utils import dateparse

from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class QuerysetToPandasDataFrameTests(TestCase):
    databases = ["default", settings.VALIDATION_DB]

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table_2ndstudy.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/samples_4_test_data_frames_set1.tsv",
            sample_table_headers="DataRepo/example_data/small_dataset/samples_headers_4_test_data_frames_set1.yaml",
            skip_researcher_check=True,
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/samples_4_test_data_frames_set2.tsv",
            sample_table_headers="DataRepo/example_data/small_dataset/samples_headers_4_test_data_frames_set2.yaml",
            skip_researcher_check=True,
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/samples_4_test_data_frames_set3.tsv",
            sample_table_headers="DataRepo/example_data/small_dataset/samples_headers_4_test_data_frames_set3.yaml",
            skip_researcher_check=True,
        )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
        )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_serum.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=False,
        )

    def get_example_study_list(self):
        return ["obob_fasted", "small_obob", "test_data_frames"]

    def get_example_study_dict(self):
        exmaple_study_dict = {
            "study": "obob_fasted",
            "total_animal": 1,
            "total_tissue": 15,
            "total_sample": 15,
            "total_msrun": 15,
            "sample_owners": ["Xianfeng Zeng"],
            "genotypes": ["WT"],
        }
        return exmaple_study_dict

    def get_example_animal_dict(self):
        exmaple_animal_dict = {
            "animal": "971",
            "tracer": "lysine",
            "tracer_labeled_atom": "C",
            "tracer_labeled_count": 6,
            "tracer_infusion_rate": 0.11,
            "tracer_infusion_concentration": 23.2,
            "genotype": "WT",
            "body_weight": 26.3,
            "feeding_status": "Fasted",
            "total_tissue": 15,
            "total_sample": 15,
            "sample_owners": ["Xianfeng Zeng"],
        }
        return exmaple_animal_dict

    def get_example_sample1_dict(self):
        example_sample1_dict = {
            "animal": "971",
            "tracer": "lysine",
            "tracer_labeled_atom": "C",
            "tracer_labeled_count": 6,
            "tracer_infusion_rate": 0.11,
            "tracer_infusion_concentration": 23.2,
            "genotype": "WT",
            "body_weight": 26.3,
            "feeding_status": "Fasted",
            "tissue": "brown_adipose_tissue",
            "sample": "BAT-xz971",
            "sample_owner": "Xianfeng Zeng",
            "msrun_owner": "Michael Neinast",
            "msrun_protocol": "Default",
        }
        return example_sample1_dict

    def get_example_sample2_dict(self):
        example_sample2_dict = {
            "animal": "t1-no-tracer-age",
            "sample": "Br-t1-no-tracer-age",
            "sample_owner": "Matthew McBride",
            "treatment": "Control diet",
        }
        return example_sample2_dict

    def get_example_compound_dict(self):
        example_compound_dict = {
            "compound_name": "lysine",
            "formula": "C6H14N2O2",
            "hmdb_id": "HMDB0000182",
            "tracer": "lysine",
            "total_animal_by_tracer": 1,
        }
        return example_compound_dict

    def test_study_list_stat_df(self):
        """
        get data from the data frame for selected study with selected columns,
        then convert the data to dictionary to compare with the example data.
        """
        example_study_dict = self.get_example_study_dict()

        study_list_stats_df = qs2df.get_study_list_stats_df()
        stud1_list_stats_df = study_list_stats_df[
            study_list_stats_df["study"] == "obob_fasted"
        ]
        selected_columns = list(example_study_dict.keys())
        stud1_list_stats_dict = stud1_list_stats_df[selected_columns].loc[0].to_dict()

        self.assertEqual(stud1_list_stats_dict, example_study_dict)

    def test_animal_list_stat_df(self):
        """
        get data from the data frame for selected animal with selected columns,
        then convert the data to dictionary to compare with the example data.
        test studies as an unordered list
        """

        example_animal_dict = self.get_example_animal_dict()
        example_studies = self.get_example_study_list()

        anim_list_stats_df = qs2df.get_animal_list_stats_df()

        anim1_list_stats_df = anim_list_stats_df[anim_list_stats_df["animal"] == "971"]
        selected_columns = list(example_animal_dict.keys())
        anim1_list_stats_dict = anim1_list_stats_df[selected_columns].iloc[0].to_dict()

        studies = anim1_list_stats_df["studies"].iloc[0].tolist()

        self.assertEqual(anim1_list_stats_dict, example_animal_dict)
        self.assertEqual(len(studies), len(example_studies))
        self.assertEqual(any(studies), any(example_studies))

    def test_animal_sample_msrun_df(self):
        """
        get data from the data frame for selected sample with selected columns,
        then convert the data to dictionary to compare with the example data.
        test studies as an unordered list
        """
        # get data for examples
        example_sample1_dict = self.get_example_sample1_dict()
        example_sample2_dict = self.get_example_sample2_dict()
        example_studies = self.get_example_study_list()

        anim_msrun_df = qs2df.get_animal_msrun_all_df()

        # sample1 test
        sam1_msrun_df = anim_msrun_df[anim_msrun_df["sample"] == "BAT-xz971"]
        sam1_columns = list(example_sample1_dict.keys())
        sam1_msrun_dict = sam1_msrun_df[sam1_columns].iloc[0].to_dict()
        sam1_studies = sam1_msrun_df["studies"].iloc[0].tolist()

        self.assertEqual(sam1_msrun_dict, example_sample1_dict)
        self.assertEqual(len(sam1_studies), len(example_studies))
        self.assertEqual(any(sam1_studies), any(example_studies))

        # sample2 test
        sam2_msrun_df = anim_msrun_df[anim_msrun_df["sample"] == "Br-t1-no-tracer-age"]
        sam2_columns = list(example_sample2_dict.keys())
        sam2_msrun_sel_dict = sam2_msrun_df[sam2_columns].iloc[0].to_dict()
        sam2_msrun_all_dict = sam2_msrun_df.iloc[0].to_dict()

        self.assertEqual(sam2_msrun_sel_dict, example_sample2_dict)

        # test values for age and sample_time_collected
        # expected values
        expected_sam2_age_week = 6.0
        expected_sam2_time_collected_mins = 150.0

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

        # sample2 has no tracer and MSRun data
        self.assertTrue(sam2_msrun_all_dict["tracer"] is pd.NA)
        self.assertTrue(sam2_msrun_all_dict["msrun_id"] is pd.NA)
        self.assertTrue(sam2_msrun_all_dict["msrun_owner"] is pd.NA)

    def test_comp_tracer_list_df(self):
        """
        get data from the data frame for selected compound with selected columns,
        then convert the data to dictionary to compare with the example data.
        """
        example_compound_dict = self.get_example_compound_dict()

        comp_tracer_list_df = qs2df.get_compound_synonym_list_df()
        comp1_tracer_list_df = comp_tracer_list_df[
            comp_tracer_list_df["compound_name"] == "lysine"
        ]

        selected_columns = list(example_compound_dict.keys())
        comp1_tracer_dict = comp1_tracer_list_df[selected_columns].iloc[0].to_dict()

        self.assertEqual(comp1_tracer_dict, example_compound_dict)

        # check synonym values
        expected_comp_symnonym_list = ["Lysine", "lys", "lysine"]
        comp_symnonym_in_df = comp1_tracer_list_df.iloc[0].synonyms.tolist()
        self.assertEqual(len(comp_symnonym_in_df), len(expected_comp_symnonym_list))
        self.assertEqual(any(comp_symnonym_in_df), any(comp_symnonym_in_df))
