from django.core.management import call_command
from django.test import TestCase

from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class QuerysetToPandasDataFrameTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "tissues.yaml")
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
        return ["obob_fasted", "small_obob"]

    def get_example_study_dict(self):
        exmaple_study_dict = {
            "study": "obob_fasted",
            "total_animal": 1,
            "total_tissue": 15,
            "total_sample": 15,
            "total_msrun": 15,
            "sample_owners": ["Xianfeng Zhang"],
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
            "sample_owners": ["Xianfeng Zhang"],
        }
        return exmaple_animal_dict

    def get_example_sample_dict(self):
        example_sample_dict = {
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
            "sample_owner": "Xianfeng Zhang",
            "sample_date": "2020-11-19",
            "collect_time_in_minutes": 150.0,
            "msrun_owner": "Michael Neinast",
            "msrun_date": "2021-06-03",
            "msrun_protocol": "Default",
        }
        return example_sample_dict

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
        example_sample_dict = self.get_example_sample_dict()
        example_studies = self.get_example_study_list()

        anim_msrun_df = qs2df.get_animal_msrun_all_df()

        sam1_msrun_df = anim_msrun_df[anim_msrun_df["sample"] == "BAT-xz971"]
        selected_columns = list(example_sample_dict.keys())
        sam1_msrun_dict = sam1_msrun_df[selected_columns].iloc[0].to_dict()

        studies = sam1_msrun_df["studies"].iloc[0].tolist()

        self.assertEqual(sam1_msrun_dict, example_sample_dict)
        self.assertEqual(len(studies), len(example_studies))
        self.assertEqual(any(studies), any(example_studies))
