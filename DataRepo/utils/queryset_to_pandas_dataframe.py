import json

import numpy as np
import pandas as pd

from DataRepo.models import (
    Animal,
    CompoundSynonym,
    Infusate,
    MSRun,
    Sample,
    Study,
)


class QuerysetToPandasDataFrame:
    """
    convert several querysets to Pandas DataFrames, then create additional
    DataFrames for study or animal based summary data
    """

    infusate_column_names = [
        "infusate_id",
        "infusate_name",
        "tracer_group_name",
        "tracer_id",
        "tracer_name",
        "tracer_concentration",
        "tracer_label",
        "labeled_element",
        "labeled_count",
        "compound_id",
        "compound_name",
    ]

    animal_column_names = [
        "animal_id",
        "animal",
        "infusate_id",
        "infusate_name",
        "tracer_group_name",
        "tracers",
        "labeled_elements",
        "concentrations",
        "compound_id_name_list",
        "tracer_id_name_list",
        "infusion_rate",
        "genotype",
        "body_weight",
        "age",
        "sex",
        "diet",
        "feeding_status",
        "treatment_id",
        "treatment",
        "treatment_category",
    ]

    animal_tissue_sample_msrun_column_names = [
        "animal_id",
        "animal",
        "infusate_id",
        "infusate_name",
        "tracer_group_name",
        "tracers",
        "labeled_elements",
        "concentrations",
        "compound_id_name_list",
        "tracer_id_name_list",
        "infusion_rate",
        "genotype",
        "body_weight",
        "age",
        "sex",
        "diet",
        "feeding_status",
        "treatment_id",
        "treatment",
        "treatment_category",
        "tissue_id",
        "tissue",
        "sample_id",
        "sample",
        "sample_owner",
        "sample_date",
        "sample_time_collected",
        "msrun_id",
        "msrun_owner",
        "msrun_date",
        "msrun_protocol_id",
        "msrun_protocol",
    ]

    # the string for replacing null value for treatment, infusate, tracer
    # prefer to use "None" as it's consistent with null value displayed on webpage
    null_rpl_str = "None"

    @staticmethod
    def qs_to_df(qs, qry_to_df_fields):
        """
        convert a queryset to a Pandas DataFrame using defined field names.
        qry_to_df_fields is a dictionary mapping query fields to column names
        of the DataFrame

        Notes on handling null values in DataFrame:
        np.nan is used in DataFrame for a null value when converting data from a queryset.
        np.nan causes dtype conversion from int to float silently when a column contains null values.
        convert columns to best possible dtypes by calling convert_dtypes():
            automitically replace np.nan with pd.NA.
            an integer column with null values will be converted from float64 to Int64 type.
        pd.NA value is converted into null in json format. rendered as None in Django templates.
        """
        qry_fields = qry_to_df_fields.keys()
        qs1 = qs.values_list(*qry_fields)
        df_with_qry_fields = pd.DataFrame.from_records(qs1, columns=qry_fields)
        # rename columns for df
        renamed_df = df_with_qry_fields.rename(columns=qry_to_df_fields)
        # convert to best possible dtypes
        out_df = renamed_df.convert_dtypes()
        return out_df

    @staticmethod
    def df_to_list_of_dict(df):
        """
        convert Pandas DataFrame into a list of dictionary, each item of the list
        is a dictionary converted from a row of the DataFrame (column_name:column_value)
        The output can be used directly for template rendering
        need to use "ns" unit to ensure correct value convertion to json for time duration
        """
        # parsing the DataFrame to JSON records.
        json_records = df.to_json(orient="records", date_format="iso", date_unit="ns")
        # output to a list of dictionary
        data = []
        data = json.loads(json_records)
        return data

    @classmethod
    def get_infusate_all_df(cls):
        """
        get joined data for all infusates, including parent compound, labeled element(s),
        concentration for each tracer associated with an infusate
        Notes:
        Infusate.name, Tracer.name, and TracerLabel.name are allowed to be null based on current design.
        For any missing name value, null will be replaced by the value assigned to "null_rpl_str".
        This prevent silent error when calling "groupby" method in DataFrames to get unique list of infusates
        or tracers.
        """
        inf_qs = (
            Infusate.objects.prefetch_related("tracers")
            .all()
            .order_by("name", "tracers__name", "tracers__labels__element")
        )
        qry_to_df_fields = {
            "id": "infusate_id",
            "name": "infusate_name",
            "tracer_group_name": "tracer_group_name",
            "tracers__id": "tracer_id",
            "tracers__name": "tracer_name",
            "tracer_links__concentration": "tracer_concentration",
            "tracers__labels__name": "tracer_label",
            "tracers__labels__element": "labeled_element",
            "tracers__labels__count": "labeled_count",
            "tracers__compound__id": "compound_id",
            "tracers__compound__name": "compound_name",
        }
        infusate_all_df = cls.qs_to_df(inf_qs, qry_to_df_fields)

        # handle possible null values for three name fields
        infusate_all_df[
            ["infusate_name", "tracer_name", "tracer_label"]
        ] = infusate_all_df[["infusate_name", "tracer_name", "tracer_label"]].fillna(
            cls.null_rpl_str
        )

        # re-index based on column order
        column_names = cls.infusate_column_names
        infusate_all_df = infusate_all_df.reindex(columns=column_names)
        return infusate_all_df

    @classmethod
    def get_infusate_gb_tracer_df(cls):
        """
        get unqiue lists of labeled element(s), element:count grouped by
        a tracer for each infusate.
        """
        infusate_all_df = cls.get_infusate_all_df()

        # add a column to join element and count
        infusate_all_df["element_count"] = (
            infusate_all_df["labeled_element"]
            + ":"
            + infusate_all_df["labeled_count"].astype(str)
        )
        # group data by infusate and tracer
        infusate_gb_df1 = (
            infusate_all_df.groupby(
                [
                    "infusate_id",
                    "compound_id",
                    "compound_name",
                    "tracer_id",
                    "tracer_name",
                    "tracer_concentration",
                ]
            )
            .agg(
                labeled_elements=("labeled_element", "unique"),
                element_count_list=("element_count", "unique"),
            )
            .reset_index()
        )
        # sort lists
        infusate_gb_df1["labeled_elements"] = (
            infusate_gb_df1["labeled_elements"].map(np.sort).map(list)
        )
        infusate_gb_df1["element_count_list"] = (
            infusate_gb_df1["element_count_list"].map(np.sort).map(list)
        )

        infusate_gb_tracer_df = infusate_gb_df1.convert_dtypes()
        return infusate_gb_tracer_df

    @classmethod
    def get_infusate_list_df(cls):
        """
        generate a DataFrame to include compound/tracer data grouped by an infusate,
        which can be retrieved or merged with animal related DataFrames easily
        """
        infusate_all_df = cls.get_infusate_all_df()
        infusate_gb_tracer_df = cls.get_infusate_gb_tracer_df()

        infusate_gb_df1 = infusate_gb_tracer_df.copy()

        infusate_gb_df1["compound_id_name"] = (
            infusate_gb_df1["compound_id"].astype(str)
            + "||"
            + infusate_gb_df1["compound_name"].astype(str)
        )
        # add a column to join compound_id and tracer_name
        infusate_gb_df1["tracer_id_name"] = (
            infusate_gb_df1["compound_id"].astype(str)
            + "||"
            + infusate_gb_df1["tracer_name"].astype(str)
        )
        # convert array to str before grouping
        infusate_gb_df1["elements_as_str"] = infusate_gb_df1["labeled_elements"].apply(
            ",".join
        )

        # groupby infusate
        infusate_list_df1 = (
            infusate_gb_df1.groupby("infusate_id")
            .agg(
                tracers=("tracer_name", list),
                concentrations=("tracer_concentration", list),
                labeled_elements=("elements_as_str", "unique"),
                compounds=("compound_name", list),
                compound_id_name_list=("compound_id_name", list),
                tracer_id_name_list=("tracer_id_name", list),
            )
            .reset_index()
        )
        # unique infusates
        infusate_df = infusate_all_df[
            ["infusate_id", "infusate_name", "tracer_group_name"]
        ].drop_duplicates()
        # merge data frames
        infusate_list_df2 = pd.merge(
            infusate_df,
            infusate_list_df1,
            left_on="infusate_id",
            right_on="infusate_id",
            how="left",
        )
        # convert Pandas StringArray to np.array to avoid error for converting to json format
        infusate_list_df2["tracers"] = infusate_list_df2["tracers"].apply(
            lambda x: np.array(x)
        )
        infusate_list_df2["labeled_elements"] = infusate_list_df2[
            "labeled_elements"
        ].apply(lambda x: np.array(x))
        infusate_list_df2["concentrations"] = infusate_list_df2["concentrations"].apply(
            lambda x: np.array(x)
        )
        infusate_list_df2["compound_id_name_list"] = infusate_list_df2[
            "compound_id_name_list"
        ].apply(lambda x: np.array(x))
        infusate_list_df2["tracer_id_name_list"] = infusate_list_df2[
            "tracer_id_name_list"
        ].apply(lambda x: np.array(x))
        # convert to best possible dtypes
        infusate_list_df = infusate_list_df2.convert_dtypes()

        return infusate_list_df

    @classmethod
    def get_study_list_df(cls):
        """
        convert all study records to a DataFrame with defined column names
        """
        qs = Study.objects.all()
        qry_to_df_fields = {
            "id": "study_id",
            "name": "study",
            "description": "study_description",
        }
        stud_list_df = cls.qs_to_df(qs, qry_to_df_fields)
        # stud_list_df.fillna({"tracer_name": ""}, inplace=True)
        return stud_list_df

    @classmethod
    def get_study_animal_all_df(cls):
        """
        generate a DataFrame for joining all studies and animals based on
        many-to-many relationships
        """
        qs = Study.objects.all().prefetch_related("animals")
        qry_to_df_fields = {
            "id": "study_id",
            "name": "study",
            "description": "study_description",
            "animals__id": "animal_id",
            "animals__name": "animal",
        }
        all_stud_anim_df = cls.qs_to_df(qs, qry_to_df_fields)
        return all_stud_anim_df

    @classmethod
    def get_animal_list_df(cls):
        """
        get all animal records with related fields for infusate and treatments,
        convert to a DataFrame with defined column names
        """
        qs = Animal.objects.select_related("protocol").all()
        qry_to_df_fields = {
            "id": "animal_id",
            "name": "animal",
            "infusate_id": "infusate_id",
            "infusion_rate": "infusion_rate",
            "genotype": "genotype",
            "body_weight": "body_weight",
            "age": "age",
            "sex": "sex",
            "diet": "diet",
            "feeding_status": "feeding_status",
            "treatment_id": "treatment_id",
            "treatment__name": "treatment",
            "treatment__category": "treatment_category",
        }
        anim_list_df1 = cls.qs_to_df(qs, qry_to_df_fields)
        # infusate data frame
        infusate_list_df = cls.get_infusate_list_df()
        # merge two data frames
        anim_list_df2 = pd.merge(
            anim_list_df1,
            infusate_list_df,
            left_on="infusate_id",
            right_on="infusate_id",
            how="left",
        )
        anim_list_df = anim_list_df2.convert_dtypes()

        column_names = cls.animal_column_names
        anim_list_df = anim_list_df.reindex(columns=column_names)

        return anim_list_df

    @classmethod
    def get_study_gb_animal_df(cls):
        """
        generate a DataFrame for studies grouped by animal_id
        adding a column named study_id_name_list
        example for data format: ['1||obob_fasted']
        """
        stud_anim_df = cls.get_study_animal_all_df()

        # add a column by joining id and name for each study
        stud_anim_df["study_id_name"] = (
            stud_anim_df["study_id"].astype(str) + "||" + stud_anim_df["study"]
        )

        # generate DataFrame grouped by animal_id and animal
        # columns=['animal_id', 'animal', 'studies', 'study_id_name_list']
        stud_gb_anim_df1 = (
            stud_anim_df.groupby(["animal_id", "animal"])
            .agg(
                studies=("study", "unique"),
                study_id_name_list=("study_id_name", "unique"),
            )
            .reset_index()
        )
        # convert Pandas StringArray to np.array to avoid error for converting to json format
        # have to apply to each column separately; got error with multiple columns
        # also sort array elements
        stud_gb_anim_df1["studies"] = stud_gb_anim_df1["studies"].map(np.sort).map(list)
        stud_gb_anim_df1["study_id_name_list"] = (
            stud_gb_anim_df1["study_id_name_list"].map(np.sort).map(list)
        )
        # convert to best possible dtypes
        stud_gb_anim_df = stud_gb_anim_df1.convert_dtypes()

        return stud_gb_anim_df

    @classmethod
    def get_sample_msrun_all_df(cls):
        """
        generate a DataFrame for all sample and MSRun records
        including animal data fields
        Use left join to merge sample and MSRun records, since a sample may not have MSRun data
        """
        sam_qs = Sample.objects.select_related().all()
        qry_to_df_fields = {
            "id": "sample_id",
            "name": "sample",
            "date": "sample_date",
            "researcher": "sample_owner",
            "time_collected": "sample_time_collected",
            "tissue_id": "tissue_id",
            "tissue__name": "tissue",
            "animal_id": "animal_id",
            "animal__name": "animal",
        }
        all_sam_df = cls.qs_to_df(sam_qs, qry_to_df_fields)

        msrun_qs = MSRun.objects.all()
        qry_to_df_fields = {
            "id": "msrun_id",
            "researcher": "msrun_owner",
            "date": "msrun_date",
            "protocol_id": "msrun_protocol_id",
            "protocol__name": "msrun_protocol",
            "sample_id": "sample_id",
        }
        all_ms_df = cls.qs_to_df(msrun_qs, qry_to_df_fields)

        # join two DataFrames
        all_sam_msrun_df1 = pd.merge(
            all_sam_df, all_ms_df, left_on="sample_id", right_on="sample_id", how="left"
        )

        # null values converted to nan for msrun_date, set to pd.NA for consistence
        all_sam_msrun_df1.replace({np.nan: pd.NA}, inplace=True)
        # get best possible dtypes
        all_sam_msrun_df = all_sam_msrun_df1.convert_dtypes()
        # list the order of columns
        column_names = [
            "sample_id",
            "sample",
            "sample_date",
            "sample_owner",
            "sample_time_collected",
            "tissue_id",
            "tissue",
            "animal_id",
            "animal",
            "msrun_id",
            "msrun_owner",
            "msrun_date",
            "msrun_protocol_id",
            "msrun_protocol",
        ]
        all_sam_msrun_df = all_sam_msrun_df.reindex(columns=column_names)
        return all_sam_msrun_df

    @classmethod
    def get_animal_msrun_all_df(cls):
        """
        generate a DataFrame for all animals, sample and MSRun records
        include study list for each animal
        """
        all_sam_msrun_df = cls.get_sample_msrun_all_df()
        anim_list_df = cls.get_animal_list_df()
        stud_gb_anim_df = cls.get_study_gb_animal_df()

        # merge DataFrames to get animal based summary data
        all_anim_msrun_df1 = anim_list_df.merge(
            all_sam_msrun_df, on=["animal_id", "animal"]
        ).merge(stud_gb_anim_df, on=["animal_id", "animal"])
        # get best possible dtypes
        all_anim_msrun_df = all_anim_msrun_df1.convert_dtypes()

        # reindex with defined column names
        # re-order columns (animal, tissue, sample, MSrun, studies)
        study_column_names = ["studies", "study_id_name_list"]
        column_names = cls.animal_tissue_sample_msrun_column_names + study_column_names

        all_anim_msrun_df = all_anim_msrun_df.reindex(columns=column_names)
        return all_anim_msrun_df

    @classmethod
    def get_animal_list_stats_df(cls):
        """
        generate a DataFrame by adding columns to animal list, including counts
            or unique values for selected data fields grouped by an animal
        """
        anim_list_df = cls.get_animal_list_df()
        all_anim_msrun_df = cls.get_animal_msrun_all_df()
        stud_gb_anim_df = cls.get_study_gb_animal_df()

        # get unique count or values for selected fields grouped by animal_id
        anim_gb_df1 = (
            all_anim_msrun_df.groupby("animal_id")
            .agg(
                total_tissue=("tissue", "nunique"),
                total_sample=("sample_id", "nunique"),
                total_msrun=("msrun_id", "nunique"),
                sample_owners=("sample_owner", "unique"),
            )
            .reset_index()
        )
        # convert Pandas StringArray to np.array
        anim_gb_df1["sample_owners"] = anim_gb_df1["sample_owners"].apply(
            lambda x: np.array(x)
        )
        # convert to best possible dtypes
        anim_gb_df = anim_gb_df1.convert_dtypes()

        # merge DataFrames to add stats and studies to each row of animal list
        anim_list_stats_df1 = anim_list_df.merge(anim_gb_df, on="animal_id").merge(
            stud_gb_anim_df, on=["animal_id", "animal"]
        )
        anim_list_stats_df = anim_list_stats_df1.convert_dtypes()

        # reindex with defined column names
        stats_column_names = [
            "total_tissue",
            "total_sample",
            "total_msrun",
            "sample_owners",
            "studies",
            "study_id_name_list",
        ]
        column_names = cls.animal_column_names + stats_column_names
        anim_list_stats_df = anim_list_stats_df.reindex(columns=column_names)
        return anim_list_stats_df

    @classmethod
    def get_study_msrun_all_df(cls):
        """
        generate a DataFrame for study based summary data including animal, sample, and MSRun
        data fields
        """
        all_stud_anim_df = cls.get_study_animal_all_df()
        all_anim_msrun_df = cls.get_animal_msrun_all_df()

        # all_anim_msrun_df contains columns for studies, drop them
        all_anim_msrun_df1 = all_anim_msrun_df.drop(
            columns=["studies", "study_id_name_list"]
        )

        # merge DataFrames to get study based summary data
        all_stud_msrun_df1 = all_stud_anim_df.merge(
            all_anim_msrun_df1, on=["animal_id", "animal"]
        )
        all_stud_msrun_df = all_stud_msrun_df1.convert_dtypes()

        # reindex with defined column names and column order
        study_column_names = [
            "study_id",
            "study",
            "study_description",
        ]
        column_names = study_column_names + cls.animal_tissue_sample_msrun_column_names
        all_stud_msrun_df = all_stud_msrun_df.reindex(columns=column_names)
        return all_stud_msrun_df

    @classmethod
    def get_study_list_stats_df(cls):
        """
        generate a DataFrame to add columns to study list including counts or unique values
        for selected data fields grouped by a study
        """
        stud_list_df = cls.get_study_list_df()
        all_stud_msrun_df = cls.get_study_msrun_all_df()
        try:
            # convert values of array columns to strings before grouping
            all_stud_msrun_df["compounds_as_str"] = all_stud_msrun_df[
                "compound_id_name_list"
            ].apply(";".join)
        except TypeError:
            # When compound_id_name_list is empty, a TypeError is raised
            # empty list is not expected, unless tracer data were not loaded for an infusate
            # keep the code for handling exception just in case
            all_stud_msrun_df["compounds_as_str"] = cls.null_rpl_str
        try:
            all_stud_msrun_df["elements_as_str"] = all_stud_msrun_df[
                "labeled_elements"
            ].apply(";".join)
        except TypeError:
            # When labeled_elements is empty, a TypeError is raised
            all_stud_msrun_df["elements_as_str"] = cls.null_rpl_str
        # drop columns
        all_stud_msrun_df.drop(columns=["compound_id_name_list", "labeled_elements"])
        # add a column to join infusate id and name
        all_stud_msrun_df["infusate_id_name"] = (
            all_stud_msrun_df["infusate_id"].astype(str)
            + "||"
            + all_stud_msrun_df["infusate_name"].astype(str)
        )
        # add a column to join treatment_id and treatment
        # need to handle null value for treatment id and name, since it is optional in Animal model
        # if treatment_id or treament is pd.NA, concatenated value will be pd.NA
        all_stud_msrun_df["treatment_id_name"] = (
            all_stud_msrun_df["treatment_id"].astype(str)
            + "||"
            + all_stud_msrun_df["treatment"]
        )
        all_stud_msrun_df["treatment_id_name"] = all_stud_msrun_df[
            "treatment_id_name"
        ].fillna(cls.null_rpl_str)

        # generate a DataFrame containing stats columns grouped by study_id
        stud_gb_df1 = (
            all_stud_msrun_df.groupby("study_id")
            .agg(
                total_animal=("animal_id", "nunique"),
                total_tissue=("tissue", "nunique"),
                total_sample=("sample_id", "nunique"),
                total_msrun=("msrun_id", "nunique"),
                sample_owners=("sample_owner", "unique"),
                genotypes=("genotype", "unique"),
                infusate_id_name_list=("infusate_id_name", "unique"),
                compound_id_name_list=("compounds_as_str", "unique"),
                labeled_elements=("elements_as_str", "unique"),
                treatment_id_name_list=("treatment_id_name", "unique"),
            )
            .reset_index()
        )
        # convert StringArray to np.array, do one by one, as got error with applying multiple columns
        stud_gb_df1["sample_owners"] = stud_gb_df1["sample_owners"].apply(
            lambda x: np.array(x)
        )
        stud_gb_df1["genotypes"] = stud_gb_df1["genotypes"].apply(lambda x: np.array(x))
        stud_gb_df1["infusate_id_name_list"] = stud_gb_df1[
            "infusate_id_name_list"
        ].apply(lambda x: np.array(x))

        # get unique list of compound_id_name from nested lists, avoiding using comma
        stud_gb_df1["compound_id_name_list"] = (
            stud_gb_df1["compound_id_name_list"]
            .str.join(";")
            .str.split(";")
            .map(np.unique)
            .map(np.sort)
        )
        # get unique list of elements from nested lists
        stud_gb_df1["labeled_elements"] = (
            stud_gb_df1["labeled_elements"]
            .str.join(",")
            .str.split(",")
            .map(np.unique)
            .map(np.sort)
        )

        stud_gb_df1["treatment_id_name_list"] = stud_gb_df1[
            "treatment_id_name_list"
        ].apply(lambda x: np.array(x))

        stud_gb_df = stud_gb_df1.convert_dtypes()

        # merge DataFrames to add stats to each row of study list
        stud_list_stats_df = stud_list_df.merge(stud_gb_df, on="study_id")

        # reindex with defined column names
        column_names = [
            "study_id",
            "study",
            "study_description",
            "total_animal",
            "total_tissue",
            "total_sample",
            "total_msrun",
            "sample_owners",
            "genotypes",
            "infusate_id_name_list",
            "compound_id_name_list",
            "labeled_elements",
            "treatment_id_name_list",
        ]
        stud_list_stats_df = stud_list_stats_df.reindex(columns=column_names)
        return stud_list_stats_df

    def get_per_study_msrun_df(self, study_id):
        """
        generate a DataFrame for summary data including animal, sample, and MSRun
        data fields for a study
        """
        all_stud_msrun_df = self.get_study_msrun_all_df()
        self.study_id = study_id
        per_stud_msrun_df = all_stud_msrun_df[all_stud_msrun_df["study_id"] == study_id]
        return per_stud_msrun_df

    def get_per_study_stat_df(self, study_id):
        """
        generate a DataFrame for summary data including animal, sample, and MSRun
        counts for a study
        """
        stud_list_stats_df = self.get_study_list_stats_df()
        self.study_id = study_id
        per_stud_stat_df = stud_list_stats_df[
            stud_list_stats_df["study_id"] == study_id
        ]
        return per_stud_stat_df

    def get_per_animal_msrun_df(self, animal_id):
        """
        generate a DataFrame for summary data including animal, sample, and MSRun
        data fields for an animal
        """
        all_anim_msrun_df = self.get_animal_msrun_all_df()
        self.animal_id = animal_id
        per_anim_msrun_df = all_anim_msrun_df[
            all_anim_msrun_df["animal_id"] == animal_id
        ]
        return per_anim_msrun_df

    @classmethod
    def get_all_compound_synonym_df(cls):
        """
        convert compound synonym records to a DataFrame with defined column names
        """
        qs = CompoundSynonym.objects.select_related().all()
        qry_to_df_fields = {
            "compound_id": "compound_id",
            "compound__name": "compound_name",
            "compound__formula": "formula",
            "compound__hmdb_id": "hmdb_id",
            "name": "synonym",
        }

        all_comp_synonym_df = cls.qs_to_df(qs, qry_to_df_fields)
        return all_comp_synonym_df

    @classmethod
    def get_compound_list_stats_df(cls):
        """
        generate a DataFrame by adding columns to compound list, including counts
            for animals and infusates grouped by a compound
        """
        all_comp_synonym_df = cls.get_all_compound_synonym_df()
        # anim_list_stats_df = cls.get_animal_list_stats_df()

        synonym_gb_comp_df1 = (
            all_comp_synonym_df.groupby(
                ["compound_id", "compound_name", "formula", "hmdb_id"]
            )
            .agg(
                synonyms=("synonym", "unique"),
            )
            .reset_index()
        )
        # convert Pandas StringArray to np.array
        synonym_gb_comp_df1["synonyms"] = (
            synonym_gb_comp_df1["synonyms"].map(np.sort).map(list)
        )
        # convert to best possible dtypes
        synonym_gb_comp_df = synonym_gb_comp_df1.convert_dtypes()

        # get infusate with associated compound(s)
        infusate_all_df = cls.get_infusate_all_df()

        animal_list_df = cls.get_animal_list_df()
        animal_infusate_df = animal_list_df[
            ["animal_id", "animal", "infusate_id"]
        ].drop_duplicates()
        anim_inf_tracer_comp_df = pd.merge(
            animal_infusate_df,
            infusate_all_df,
            left_on="infusate_id",
            right_on="infusate_id",
            how="left",
        )
        # group animal by compound
        anim_gb_comp_df1 = (
            anim_inf_tracer_comp_df.groupby(["compound_id", "compound_name"])
            .agg(
                total_animal_by_compound=("animal_id", "nunique"),
                total_infusate_by_compound=("infusate_id", "nunique"),
            )
            .reset_index()
        )
        # convert to best possible dtypes
        anim_gb_comp_df = anim_gb_comp_df1.convert_dtypes()

        comp_list_stats_df1 = pd.merge(
            synonym_gb_comp_df,
            anim_gb_comp_df,
            left_on=["compound_id", "compound_name"],
            right_on=["compound_id", "compound_name"],
            how="left",
        )

        column_names = [
            "compound_id",
            "compound_name",
            "formula",
            "hmdb_id",
            "synonyms",
            "total_animal_by_compound",
            "total_infusate_by_compound",
        ]
        comp_list_stats_df = comp_list_stats_df1.reindex(columns=column_names)

        return comp_list_stats_df
