import json

import pandas as pd
from django.core.management import call_command
from django.utils import dateparse

from DataRepo.models import Animal, Infusate, Tracer, TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class QuerysetToPandasDataFrameBaseTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        # load small set of data
        call_command("load_study", "DataRepo/example_data/test_dataframes/loading.yaml")

        # define expected data in dictionaries
        cls.study1_dict = {
            "study": "Study Test1",
            "total_animal": 3,
            "total_tissue": 3,
            "total_sample": 7,
            "total_msrun": 3,
            "sample_owners": ["Xianfeng Zeng"],
            "genotypes": ["C57BL/6N", "WT", "ob/ob"],
        }

        cls.study2_dict = {
            "study": "Study Test2",
            "animal1": "a1_Lys_13C",
            "animal2": "a4_Val_13C5",
        }

        cls.animal1_dict = {
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

        cls.animal2_dict = {
            "animal": "a4_Val_13C5",
            "infusate_name": "valine-[13C2][22]",
            "treatment": "no treatment",
            "studies": ["Study Test2"],
        }

        cls.sample1_dict = {
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

        cls.sample2_dict = {
            "animal": "a2_VLI_13C15N",
            "infusate_name": "BCAAs (VLI) {isoleucine-[13C6,15N1][12];leucine-[13C6,15N1][24];valine-[13C5,15N1][20]}",
            "tissue": "liver",
            "sample": "a2_liv",
            "sample_owner": "Xianfeng Zeng",
        }

        cls.compound_dict = {
            "compound_name": "lysine",
            "formula": "C6H14N2O2",
            "hmdb_id": "HMDB0000182",
            "synonyms": ["Lysine", "lys", "lysine"],
            "total_animal_by_compound": 2,
            "total_infusate_by_compound": 2,
        }

        cls.infusate1_dict = {
            "infusate_name": "lysine-[13C6][15]",
            "tracer_group_name": None,
            "tracers": ["lysine-[13C6]"],
            "concentrations": [15],
            "labeled_elements": ["C"],
        }

        cls.infusate2_dict = {
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

        super().setUpTestData()

    def test_study_list_stat_df(self):
        """
        get data from the DataFrame for a selected study with selected columns,
        then convert the data to dictionary to compare with expected data.
        """
        stud1_dict = self.study1_dict
        stud1_name = stud1_dict["study"]
        study_list_stats_df = qs2df.get_study_list_stats_df()
        # slicing DataFrame for selected study
        stud1_list_stats_df = study_list_stats_df[
            study_list_stats_df["study"] == stud1_name
        ]
        selected_columns = list(stud1_dict.keys())
        out_df = stud1_list_stats_df[selected_columns]
        stud1_list_stats_dict = qs2df.df_to_list_of_dict(out_df)[0]

        # Sort the lists so that they can be equated
        stud1_list_stats_dict["genotypes"] = sorted(stud1_list_stats_dict["genotypes"])
        stud1_dict["genotypes"] = sorted(stud1_dict["genotypes"])

        self.assertEqual(stud1_list_stats_dict, stud1_dict)

    def test_animal_list_stat_df(self):
        """
        get data from the DataFrame for a selected animal with selected columns,
        then convert the data to dictionary to compare with expected data.
        test studies as an unordered list
        """
        anim1_dict = self.animal1_dict
        anim1_name = anim1_dict["animal"]

        anim_list_stats_df = qs2df.get_animal_list_stats_df()
        # slicing DataFrame for selected animal
        anim1_list_stats_df = anim_list_stats_df[
            anim_list_stats_df["animal"] == anim1_name
        ]
        selected_columns = list(anim1_dict.keys())
        out_df = anim1_list_stats_df[selected_columns]
        anim1_list_stats_dict = qs2df.df_to_list_of_dict(out_df)[0]
        self.assertEqual(anim1_list_stats_dict, anim1_dict)

    def test_animal_sample_msrun_df(self):
        """
        get data from the DataFrame for selected samples with selected columns,
        then convert the data to dictionary to compare with expected data.
        test studies as an unordered list
        """
        # get data for examples
        sam1_dict = self.sample1_dict
        sam2_dict = self.sample2_dict
        sam1_name = sam1_dict["sample"]
        sam2_name = sam2_dict["sample"]
        anim_msrun_df = qs2df.get_animal_msrun_all_df()

        # test for sample 1
        sam1_msrun_df = anim_msrun_df[anim_msrun_df["sample"] == sam1_name]
        sam1_columns = list(sam1_dict.keys())
        out_df = sam1_msrun_df[sam1_columns]
        sam1_msrun_dict = qs2df.df_to_list_of_dict(out_df)[0]
        self.assertEqual(sam1_msrun_dict, sam1_dict)

        # test for sample 2
        sam2_msrun_df = anim_msrun_df[anim_msrun_df["sample"] == sam2_name]
        sam2_columns = list(sam2_dict.keys())
        sam2_msrun_sel_dict = sam2_msrun_df[sam2_columns].iloc[0].to_dict()
        sam2_msrun_all_dict = sam2_msrun_df.iloc[0].to_dict()
        self.assertEqual(sam2_msrun_sel_dict, sam2_dict)

        # test values for age and sample_time_collected
        # expected values
        exp_sam2_age_week = 21.0
        exp_sam2_time_collected_mins = 120.0

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
        self.assertEqual(sam2_age_to_week, exp_sam2_age_week)
        self.assertEqual(sam2_age_in_json_data_to_weeks, exp_sam2_age_week)

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
        self.assertEqual(sam2_time_collected_to_mins, exp_sam2_time_collected_mins)
        self.assertEqual(
            sam2_time_collected_in_json_data_to_mins, exp_sam2_time_collected_mins
        )

        # sample2 has no MSRun data
        self.assertTrue(sam2_msrun_all_dict["msrun_id"] is pd.NA)
        self.assertTrue(sam2_msrun_all_dict["msrun_owner"] is pd.NA)

    def test_comp_list_stats_df(self):
        """
        get data from the DataFrame for a selected compound with selected columns,
        then convert the data to dictionary to compare with expected data.
        """
        comp1_dict = self.compound_dict
        comp1_name = comp1_dict["compound_name"]

        comp_list_stats_df = qs2df.get_compound_list_stats_df()
        comp1_df = comp_list_stats_df[comp_list_stats_df["compound_name"] == comp1_name]

        selected_columns = list(comp1_dict.keys())
        out_df = comp1_df[selected_columns]
        comp1_out_dict = qs2df.df_to_list_of_dict(out_df)[0]

        self.assertEqual(comp1_out_dict, comp1_dict)

    def test_infusate_list_df(self):
        """
        get data from the DataFrame for selected infusate with selected columns,
        then convert the data to dictionary to compare with expected data.
        """
        infusate_list_df = qs2df.get_infusate_list_df()
        # infusate 1
        inf1_dict = self.infusate1_dict
        inf1_df = infusate_list_df[
            infusate_list_df["infusate_name"] == inf1_dict["infusate_name"]
        ]
        inf1_columns = list(inf1_dict.keys())
        out1_df = inf1_df[inf1_columns]
        inf1_out_dict = qs2df.df_to_list_of_dict(out1_df)[0]
        self.assertEqual(inf1_out_dict, inf1_dict)
        # infusate 2
        inf2_dict = self.infusate2_dict
        inf2_df = infusate_list_df[
            infusate_list_df["infusate_name"] == inf2_dict["infusate_name"]
        ]
        set2_columns = list(inf2_dict.keys())
        out2_df = inf2_df[set2_columns]
        inf2_out_dict = qs2df.df_to_list_of_dict(out2_df)[0]
        self.assertEqual(inf2_out_dict, inf2_dict)

    def test_treatment_null(self):
        """
        test null values handled by DataFrames if Animal.treatment_id is null
        """
        # get IDs and names for selected studies and animal before update
        stud2_name = self.study2_dict["study"]
        anim1_name = self.study2_dict["animal1"]
        anim2_name = self.study2_dict["animal2"]
        anim1_id = Animal.objects.get(name=anim1_name).id

        # update treatment_id to null for animal1
        Animal.objects.filter(id=anim1_id).update(treatment_id=None)

        # check values in queryset, DataFrame, output dictionary after update
        self.assertEqual(Animal.objects.get(id=anim1_id).name, anim1_name)
        self.assertEqual(Animal.objects.get(id=anim1_id).treatment, None)
        # get DataFrame and output dictonary after update
        anim1_df = qs2df().get_per_animal_msrun_df(anim1_id)
        anim1_out_dict = qs2df.df_to_list_of_dict(anim1_df)[0]
        # expected values in DataFrame
        self.assertTrue(anim1_df.iloc[0]["treatment_id"] is pd.NA)
        self.assertTrue(anim1_df.iloc[0]["treatment"] is pd.NA)
        self.assertTrue(anim1_df.iloc[0]["treatment_category"] is pd.NA)
        # expected values in output dictionary
        self.assertEqual(anim1_out_dict["treatment_id"], None)
        self.assertEqual(anim1_out_dict["treatment"], None)
        self.assertEqual(anim1_out_dict["treatment_category"], None)

        # check string in study list when treatment_id is null
        stud_list_stats_df = qs2df.get_study_list_stats_df()
        stud2_df = stud_list_stats_df[stud_list_stats_df["study"] == stud2_name]
        # the string used to replace null value in DataFrame for study list
        # no update was made for treatment for animal2
        anim2_treatment = Animal.objects.get(name=anim2_name).treatment
        anim2_treatment_id_name = str(anim2_treatment.id) + "||" + anim2_treatment.name
        # the treatment_id_name for animal1 was replaced by null_rpl_str in DataFrame
        anim1_treatment_id_name = qs2df.null_rpl_str
        exp_treatment_id_name_list = [anim1_treatment_id_name, anim2_treatment_id_name]
        # compare sorted lists
        out_list = sorted(stud2_df.iloc[0]["treatment_id_name_list"])
        exp_list = sorted(exp_treatment_id_name_list)
        self.assertEqual(len(out_list), 2)
        self.assertEqual(out_list, exp_list)

    def test_infusate_tracer_name_null(self):
        """
        The following names are allowed to be null:
          Animal.treatment, Infusate.name. Tracer.name, TracerLabel.name
          e.g. data loading with dis-allow auto-updates by disabling buffering
          test missing names are handled properly in DataFrames, expecially for study list
        """
        # the string used to replace null value in DataFrames
        null_rpl_str = qs2df.null_rpl_str
        # use study 2 including two animals/infusates for tests
        stud2_name = self.study2_dict["study"]
        anim1_name = self.study2_dict["animal1"]
        anim2_name = self.study2_dict["animal2"]
        inf2_id = Animal.objects.get(name=anim2_name).infusate.id
        inf2_name = Animal.objects.get(name=anim2_name).infusate.name

        # infusate1
        inf1_name = self.infusate1_dict["infusate_name"]
        inf1_id = Infusate.objects.get(name=inf1_name).id
        tracer_name = self.infusate1_dict["tracers"][0]
        tracer_id = Tracer.objects.get(name=tracer_name).id
        tracerlabel_id = TracerLabel.objects.get(
            tracer_id=tracer_id, element="C", count="6", mass_number=13
        ).id
        # animal1 infusated with infusate1
        self.assertEqual(Animal.objects.get(name=anim1_name).infusate_id, inf1_id)
        self.assertEqual(inf1_name, "lysine-[13C6][15]")
        self.assertEqual(tracer_name, "lysine-[13C6]")

        # update name values for infusate1
        Infusate.objects.filter(id=inf1_id).update(name=None)
        Tracer.objects.filter(id=tracer_id).update(name=None)
        TracerLabel.objects.filter(id=tracerlabel_id).update(name=None)

        # check null values for name fileds are replaced by null_rpl_str in DataFrames
        # no changes to IDs
        # check IDs and Names in infusate DataFrame
        infusate_all_df = qs2df.get_infusate_all_df()
        inf1_df = infusate_all_df[infusate_all_df["infusate_id"] == inf1_id]
        self.assertEqual(inf1_df.iloc[0]["infusate_id"], inf1_id)
        self.assertEqual(inf1_df.iloc[0]["tracer_id"], tracer_id)
        self.assertEqual(inf1_df.iloc[0]["infusate_name"], null_rpl_str)
        self.assertEqual(inf1_df.iloc[0]["tracer_name"], null_rpl_str)
        self.assertEqual(inf1_df.iloc[0]["tracer_label"], null_rpl_str)

        # check infusate and tracer names in animal list DataFrame
        anim_list_stats_df = qs2df.get_animal_list_stats_df()
        # slicing DataFrame for selected animal
        anim1_list_stats_df = anim_list_stats_df[
            anim_list_stats_df["animal"] == anim1_name
        ]
        self.assertEqual(anim1_list_stats_df.iloc[0]["infusate_name"], null_rpl_str)
        self.assertEqual(anim1_list_stats_df.iloc[0]["tracers"], [null_rpl_str])

        # check infusate_id_name_list in study list DataFrame
        stud_list_stats_df = qs2df.get_study_list_stats_df()
        # slicing DataFrame for selected animal
        stud2_list_stats_df = stud_list_stats_df[
            stud_list_stats_df["study"] == stud2_name
        ]
        # id_name and id_name_list: output vs. expected
        exp_inf1_id_name = str(inf1_id) + "||" + null_rpl_str
        exp_inf2_id_name = str(inf2_id) + "||" + inf2_name
        exp_infusate_id_name_list = [exp_inf1_id_name, exp_inf2_id_name]
        # compare sorted lists
        out_list = sorted(stud2_list_stats_df.iloc[0]["infusate_id_name_list"])
        exp_list = sorted(exp_infusate_id_name_list)
        self.assertEqual(out_list, exp_list)
