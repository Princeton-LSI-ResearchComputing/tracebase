from abc import ABC, abstractmethod
import pandas as pd

from DataRepo.loaders.table_loader import TableLoader
from DataRepo.models import PeakData, PeakGroup


class PeakAnnotationsLoader(TableLoader, ABC):
    @property
    @abstractmethod
    def merged_column_rename_dict(self):
        """A dict that describes how to rename all columns in the final merged pandas DataFrame.
        It does not have to have a key for every column - just the ones that need to be renamed.
        Example:
            {
                "formula": "Formula",
                "medMz": "MedMz",
                "medRt": "MedRt",
                "isotopeLabel": "IsotopeLabel",
                "compound": "Compound",
            }
        """
        pass

    @property
    @abstractmethod
    def merge_dict(self):
        """A recursively constructed dict describing how to merge the sheets in df_dict.
        Example:
            {
                "first_sheet": "Corrected",  # This key ponly occurs once in the outermost dict
                "next_merge_dict": {
                    "on": ["Compound", "C_Label"],
                    "left_columns": None,  # all
                    "right_sheet": "Original",
                    "right_columns": ["formula", "medMz", "medRt", "isotopeLabel"],
                    "how": "left",
                    "next_merge_dict": None,
                }
            }
        """
        pass

    @property
    @abstractmethod
    def add_columns_dict(self):
        """2D dict of methods that take a pandas DataFrame, keyed on sheet name and new column name.
        Example:
            {
                "Original": {
                    "C_Label": lambda df: df["isotopeLabel"].str.split("-").str.get(-1).replace({"C12 PARENT": "0"}),
                    "Compound": lambda df: df["compound"],
                }
            }
        """
        pass

    def convert_df(self, df):

        single_sheet = self.merge_dict["first_sheet"]
        # If there's only 1 key in the merge_dict and its value is None, it is inferred to mean that no merge is
        # necessary.  Just set the outdf to that one dataframe indicated by the sole sheet key.
        if self.merge_dict["next_merge_dict"] is not None:
            single_sheet = None

        if isinstance(df, pd.DataFrame):
            outdf = df.copy(deep=True)
            if single_sheet is None:
                ValueError(f"A dataframe dict containing the following sheets/keys: {list(df.keys())} is required.")
            self.add_df_columns({single_sheet: outdf})
        elif isinstance(df, dict):
            outdf = dict((sheet, adf.copy(deep=True)) for sheet, adf in df.items())
            # If there's only 1 sheet that we need
            if single_sheet is not None:
                if single_sheet in outdf.keys():
                    self.add_df_columns(outdf)
                else:
                    ValueError(f"Sheet [{single_sheet}] missing in the dataframe dict: {list(outdf.keys())}")

            outdf = self.merge_df_sheets(outdf)

        return outdf.rename(columns=self.column_renames)

    def add_df_columns(self, df_dict: dict):
        """Creates/adds columns to a pandas DataFrame dict based on the methods in self.add_columns_dict that generate
        columns from existing DataFrame columns.
        Args:
            df_dict (dict of pandas DataFrames)
        Exceptions:
            None
        Returns:
            None
        """
        for sheet, column_dict in self.add_columns_dict.items():
            for new_column, method in column_dict.items():
                df_dict[sheet][new_column] = method(df_dict[sheet])

    def merge_df_sheets(self, df_dict, _outdf=None, _merge_dict=None, _first_sheet=None):
        """Uses self.merge_dict to recursively merge df_dict's sheets into a single merged dataframe.
        Args:
            df_dict (dict of pandas DataFrames): A dict of dataframes keyed on sheet names (i.e. the return of
                read_from_file when called with an excel doc)
            _outdf (Optional[pandas DataFrame]) [df_dict[self.merge_dict["first_sheet"]]]: Used in recursive calls to
                build up a dataframe from 2 or more sheets in df_dict.
            _merge_dict (Optional[dict]) [self.merge_dict]: A recursively constructed dict describing how to merge the
                sheets in df_dict.  Example:
                    {
                        "first_sheet": "Corrected",  # This key ponly occurs once in the outermost dict
                        "next_merge_dict": {
                            "on": ["Compound", "C_Label"],
                            "left_columns": None,  # all
                            "right_sheet": "Original",
                            "right_columns": ["formula", "medMz", "medRt", "isotopeLabel"],
                            "how": "left",
                            "next_merge_dict": None,
                        }
                    }
            _first_sheet (Optional[str]) [self.merge_dict["first_sheet"]]: The first sheet to use as the left dataframe
                in the first merge.  Every subsequence recursive merge uses outdf and does not use _first_sheet.
        Exceptions:
            Buffers:
                None
            Raises:
                KeyError
        Returns:
            _outdf (pandas DataFrame): Dataframe that has been merged from df_dict based on _merge_dict.  Note, if
                _merge_dict["next_merge_dict"] is None at the outermost level of the dict,
                df_dict[self.merge_dict["first_sheet"]] is returned (i.e. no merge is performed).
        """
        if _merge_dict is None:
            _merge_dict = self.merge_dict.copy()

        if _outdf is None:
            if _first_sheet is None:
                if "first_sheet" not in _merge_dict.keys():
                    raise KeyError("'first_sheet' not supplied and not in merge_dict.")
                _first_sheet = _merge_dict["first_sheet"]
            _outdf = df_dict[_first_sheet]

        if _merge_dict["next_merge_dict"] is None:
            return _outdf

        left_df = _outdf
        if _merge_dict["left_columns"] is not None:
            left_df = _outdf.drop_duplicates(subset=_merge_dict["left_columns"])

        right_df = df_dict[_merge_dict["right_sheet"]]
        if _merge_dict["right_columns"] is not None:
            right_df = _outdf.drop_duplicates(subset=_merge_dict["right_columns"])

        _outdf = pd.merge(
            left=left_df,
            right=right_df,
            on=_merge_dict["on"],
            how=_merge_dict["how"],
        )

        if _merge_dict["next_merge_dict"] is None:
            return _outdf
        
        return self.merge_df_sheets(
            df_dict,
            _outdf=_outdf,
            _merge_dict=_merge_dict["next_merge_dict"].copy(),
        )


class IsocorrLoader(PeakAnnotationsLoader):
    merged_column_rename_dict = {
        "formula": "Formula",
        "medMz": "MedMz",
        "medRt": "MedRt",
        "isotopeLabel": "IsotopeLabel",
        "compound": "Compound",
    }

    add_columns_dict = None

    # No merge necessary, just use the absolte sheet
    merge_dict = {
        "first_sheet": "absolte",
        "next_merge_dict": None,
    }

class AccucorLoader(PeakAnnotationsLoader):
    merged_column_rename_dict = {
        "formula": "Formula",
        "medMz": "MedMz",
        "medRt": "MedRt",
        "isotopeLabel": "IsotopeLabel",
        "Compound": "Compound",
    }

    add_columns_dict = {
        # Sheet: dict
        "Original": {
            # New column name: method that takes a dataframe to create the new column
            "C_Label": lambda df: df["isotopeLabel"].str.split("-").str.get(-1).replace({"C12 PARENT": "0"}),
            "Compound": lambda df: df["compound"],
        }
    }

    merge_dict = {
        "first_sheet": "Corrected",  # This key ponly occurs once in the outermost dict
        "next_merge_dict": {
            "on": ["Compound", "C_Label"],
            "left_columns": None,  # all
            "right_sheet": "Original",
            "right_columns": ["formula", "medMz", "medRt", "isotopeLabel"],
            "how": "left",
            "next_merge_dict": None,
        }
    }
