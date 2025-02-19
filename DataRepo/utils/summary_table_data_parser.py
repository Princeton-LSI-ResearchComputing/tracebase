import os
import yaml

from collections import namedtuple
from django.utils import dateparse
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df
from TraceBase import settings

import pandas as pd


class SummaryTableData:
    """
    Contains methods for generating pandas dataframes as well as other data structures that serves
    as data sources for displaying summary tables on webspages as well as in output files
    """

    # sample yaml file for metadata
    sample_metadata_yaml_file = "DataRepo/fixtures/sample_list_summary_metadata.yaml"

    @staticmethod
    def yaml_file_to_df(file_path):
        # convert input yaml file to a Pandas dataframe or raise error
        if not os.path.isfile(file_path):
            raise FileNotFoundError(file_path)
        else:
            try:
                with open(file_path, "r") as infile:
                    yaml_data = yaml.safe_load(infile)
                # convert data to Pandas dataframe
                df = pd.DataFrame(yaml_data)
            except yaml.YAMLError as exception:
                raise exception
            return df

    def get_summary_column_info(self, yaml_file):
        """
        parse the metadata yaml file and output the column info as
          Namedtuple.
        """
        self.yaml_file = yaml_file
        metadata_yaml_path = os.path.join(settings.BASE_DIR, yaml_file)
        # convert yaml file to pandas dataframe
        metadata_df = self.yaml_file_to_df(metadata_yaml_path)
        # filter the dataframes for web display
        display_df = metadata_df[metadata_df["is_web_display"]]
        # a list for all columns
        all_col_list = metadata_df["column_name"].unique().tolist()
        # a list of columns for web display only
        display_col_list = display_df["column_name"].unique().tolist()
        # a dictionary mapping column names with display names
        col_display_mapping_dict = dict(
            zip(display_df["column_name"], display_df["display_name"])
        )
        # output in namedtuple
        SummColInfo = namedtuple(
            "SummColInfo",
            "yaml_file, all_col_list, display_col_list, col_display_mapping_dict",
        )
        summ_col_info = SummColInfo(
            yaml_file, all_col_list, display_col_list, col_display_mapping_dict
        )
        return summ_col_info

    @classmethod
    def get_sample_summary_column_info(cls):
        sample_yaml_file = cls.sample_metadata_yaml_file
        sample_summ_col_info = cls.get_summary_column_info(cls, sample_yaml_file)
        return sample_summ_col_info

    @classmethod
    def get_sample_summary_df(cls):

        # get all sample data in data frame before formatting age, dates
        all_anim_msrun_df = qs2df.get_animal_msrun_all_df()
        sam_df = all_anim_msrun_df.copy()
        # add field to convert age to weeks
        sam_df["age_in_weeks"] = sam_df["age"].apply(
            lambda x: (
                dateparse.parse_duration(str(x)).days // 7 if not pd.isna(x) else x
            )
        )
        # add field to convert collection time to minutes
        sam_df["sample_time_collected_m"] = sam_df["sample_time_collected"].apply(
            lambda x: (
                dateparse.parse_duration(str(x)).seconds // 60 if not pd.isna(x) else x
            )
        )
        # convert sample_date to string using yyyy-mm-dd format
        sam_df["sample_date_formatted"] = sam_df["sample_date"].apply(lambda x: str(x))
        # convert msrun_date to string using yyyy-mm-dd format
        sam_df["msrunsample_date_formatted"] = sam_df["msrunsample_date"].apply(
            lambda x: str(x)
        )

        # check if columns in dataframe are not defined in yaml file
        sam_df_columns = sam_df.columns.to_list()
        # defined columns in yaml file
        sample_summ_all_columns = cls.get_sample_summary_column_info().all_col_list
        # missing column list
        missing_columns = list(set(sam_df_columns) - set(sample_summ_all_columns))
        if len(missing_columns) > 0:
            raise ValueError(
                f"Sample dataframe columns are not defined in yaml file: {missing_columns}"
            )
        else:
            return sam_df

    @classmethod
    def get_sample_summary_download_df(cls):
        # get dataframe with all columns
        sam_df = cls.get_sample_summary_df()
        sample_summ_col_info = cls.get_sample_summary_column_info()
        sam_col_display_mapping_dict = sample_summ_col_info.col_display_mapping_dict
        # remove an item
        sam_col_display_mapping_dict.pop("msrunsample_id")
        sam_download_col_list = list(sam_col_display_mapping_dict.keys())
        sam_download_df = sam_df[sam_download_col_list]
        # rename column to get display names
        sam_download_rename_df = sam_download_df.rename(
            columns=sam_col_display_mapping_dict
        )
        return sam_download_rename_df
