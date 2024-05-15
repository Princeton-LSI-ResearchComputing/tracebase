from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Dict, Optional

import pandas as pd

from DataRepo.utils.exceptions import RequiredHeadersError

# from DataRepo.loaders.table_loader import TableLoader
# from DataRepo.models import PeakData, PeakGroup


# class PeakAnnotationsLoader(TableLoader, ABC):
class PeakAnnotationsLoader(ABC):
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

    """add_columns_dict is a 2D dict of methods that take a pandas DataFrame, keyed on sheet name and new column name.
    Example:
        {
            "Original": {
                "C_Label": lambda df: df["isotopeLabel"].str.split("-").str.get(-1).replace({"C12 PARENT": "0"}),
                "Compound": lambda df: df["compound"],
            }
        }
    """
    add_columns_dict: Optional[dict] = None

    """merged_drop_columns_list is a list of columns to specifically remove after merge.  No error will be raised if the
    column is already absent.
    Example:
        ["compound"]
    """
    merged_drop_columns_list: Optional[list] = None

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    MEDMZ_KEY = "MEDMZ"
    MEDRT_KEY = "MEDRT"
    ISOTOPELABEL_KEY = "ISOTOPELABEL"
    FORMULA_KEY = "FORMULA"
    COMPOUND_KEY = "COMPOUND"

    DataSheetName = "Corrected"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "MEDMZ",
            "MEDRT",
            "ISOTOPELABEL",
            "FORMULA",
            "COMPOUND",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        MEDMZ="MedMz",
        MEDRT="MedRt",
        ISOTOPELABEL="IsotopeLabel",
        FORMULA="Formula",
        COMPOUND="Compound",
    )

    # List of required header keys
    DataRequiredHeaders = [
        MEDMZ_KEY,
        MEDRT_KEY,
        ISOTOPELABEL_KEY,
        FORMULA_KEY,
        COMPOUND_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # No DataDefaultValues needed

    DataColumnTypes: Dict[str, type] = {
        MEDMZ_KEY: float,
        MEDRT_KEY: float,
        ISOTOPELABEL_KEY: str,
        FORMULA_KEY: str,
        COMPOUND_KEY: str,
    }

    def __init__(self, *args, **kwargs):
        """Constructor.
        Args:
            Superclass Args:
                df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
                dry_run (Optional[boolean]) [False]: Dry run mode.
                defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT
                    MUST HANDLE THE ROLLBACK.
                data_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                defaults_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                file (Optional[str]) [None]: File name (for error reporting).
                user_headers (Optional[dict]): Header names by header key.
                defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
                defaults_file (Optional[str]) [None]: Defaults file name (None if the same as infile).
                headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
                defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.
            Derived (this) class Args:
                None
        Exceptions:
            None
        Returns:
            None
        """
        if "df" in kwargs.keys():
            kwargs["df"] = self.convert_df(kwargs["df"])
        super().__init__(*args, **kwargs)

    @classmethod
    def convert_df(cls, df):
        """Uses the abstract properties defined in a derived class to convert the given data format (e.g. an accucor
        excel file or an isocorr csv file) into a universal format accepted by this parent class's loading code.
        Args:
            df (pandas DataFrame or dict of pandas DataFrames): Basically, anything returned by the read_from_file
                method in the file_utils library
        Exceptions:
            ValueError
        Returns:
            outdf (pandas DataFrame): Converted single DataFrame
        """
        # If there's only 1 key in the merge_dict and its value is None, it is inferred to mean that no merge is
        # necessary.  Just set the outdf to that one dataframe indicated by the sole sheet key.
        single_sheet = (
            cls.merge_dict["first_sheet"]
            if cls.merge_dict["next_merge_dict"] is not None
            else None
        )

        if isinstance(df, pd.DataFrame):
            outdf = df.copy(deep=True)
            if single_sheet is not None:
                try:
                    cls.add_df_columns({single_sheet: outdf})
                except KeyError:
                    # We will assume they did the merge themselves and that KeyErrors from adding columns come from
                    # sheets without the corrected sample data, and that adding columns to other sheets is only to
                    # facilitate the merge, and thus, we can ignore all KeyErrors
                    # An error will occur below if this is not the case
                    pass
            # If we're getting a single dataframe when multiple sheets to merge are required, we will assume the merge
            # has already been done and that all required columns are present
        elif isinstance(df, dict):
            outdf = dict((sheet, adf.copy(deep=True)) for sheet, adf in df.items())
            cls.add_df_columns(outdf)
            outdf = cls.merge_df_sheets(outdf)

        try:
            outdf = outdf.rename(columns=cls.merged_column_rename_dict)
        except Exception as e:
            if isinstance(df, pd.DataFrame) and single_sheet is None:
                raise ValueError(
                    f"A dataframe dict containing the following sheets/keys: {list(df.keys())} is required."
                )
            else:
                raise e

        # TODO: Replace this with a mechanism that uses self.headers once we inherit from TableLoader
        missing = []
        for hdr in cls.DataHeaders._asdict().values():
            if hdr not in outdf.columns:
                missing.append(hdr)
        if len(missing) > 0:
            raise RequiredHeadersError(missing)

        return outdf.drop(cls.merged_drop_columns_list, axis=1, errors="ignore")

    @classmethod
    def add_df_columns(cls, df_dict: dict):
        """Creates/adds columns to a pandas DataFrame dict based on the methods in cls.add_columns_dict that generate
        columns from existing DataFrame columns.
        Args:
            df_dict (dict of pandas DataFrames)
        Exceptions:
            None
        Returns:
            None
        """
        if cls.add_columns_dict is not None:
            for sheet, column_dict in cls.add_columns_dict.items():
                for new_column, method in column_dict.items():
                    df_dict[sheet][new_column] = method(df_dict[sheet])

    @classmethod
    def merge_df_sheets(cls, df_dict, _outdf=None, _merge_dict=None, _first_sheet=None):
        """Uses cls.merge_dict to recursively merge df_dict's sheets into a single merged dataframe.
        Args:
            df_dict (dict of pandas DataFrames): A dict of dataframes keyed on sheet names (i.e. the return of
                read_from_file when called with an excel doc)
            _outdf (Optional[pandas DataFrame]) [df_dict[cls.merge_dict["first_sheet"]]]: Used in recursive calls to
                build up a dataframe from 2 or more sheets in df_dict.
            _merge_dict (Optional[dict]) [cls.merge_dict]: A recursively constructed dict describing how to merge the
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
            _first_sheet (Optional[str]) [cls.merge_dict["first_sheet"]]: The first sheet to use as the left dataframe
                in the first merge.  Every subsequence recursive merge uses outdf and does not use _first_sheet.
        Exceptions:
            Buffers:
                None
            Raises:
                KeyError
        Returns:
            _outdf (pandas DataFrame): Dataframe that has been merged from df_dict based on _merge_dict.  Note, if
                _merge_dict["next_merge_dict"] is None at the outermost level of the dict,
                df_dict[cls.merge_dict["first_sheet"]] is returned (i.e. no merge is performed).
        """
        if _merge_dict is None:
            _merge_dict = cls.merge_dict.copy()

        if _outdf is None:
            if _first_sheet is None:
                if "first_sheet" not in _merge_dict.keys():
                    raise KeyError("'first_sheet' not supplied and not in merge_dict.")
                _first_sheet = _merge_dict["first_sheet"]
            _outdf = df_dict[_first_sheet]

            if _merge_dict["next_merge_dict"] is None:
                return _outdf
            _merge_dict = _merge_dict["next_merge_dict"]

        left_df = _outdf
        if _merge_dict["left_columns"] is not None:
            left_df = _outdf.drop_duplicates(subset=_merge_dict["left_columns"])

        right_df = df_dict[_merge_dict["right_sheet"]]
        if _merge_dict["right_columns"] is not None:
            right_sheet = _merge_dict["right_sheet"]
            right_df = df_dict[right_sheet].drop_duplicates(
                subset=_merge_dict["right_columns"]
            )

        _outdf = pd.merge(
            left=left_df,
            right=right_df,
            on=_merge_dict["on"],
            how=_merge_dict["how"],
        )

        if _merge_dict["next_merge_dict"] is None:
            return _outdf

        return cls.merge_df_sheets(
            df_dict,
            _outdf=_outdf,
            _merge_dict=_merge_dict["next_merge_dict"].copy(),
        )


class IsocorrLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that just defines how to convert an isocorr excel file to the format
    accepted by the parent class's load_data method.

    PeakAnnotationsLoader is an abstract base class.  It has a method called convert_df() that uses the data described
    here to automatically convert self.df to the format it accepts.
    """

    merged_column_rename_dict = {
        "formula": "Formula",
        "medMz": "MedMz",
        "medRt": "MedRt",
        "isotopeLabel": "IsotopeLabel",
        "compound": "Compound",
    }

    merged_drop_columns_list = [
        "compound",
        "label",
        "metaGroupId",
        "groupId",
        "goodPeakCount",
        "maxQuality",
        "compoundId",
        "expectedRtDiff",
        "ppmDiff",
        "parent",
    ]

    # add_columns_dict is unnecessary (no columns to add)

    # No merge necessary, just use the absolte sheet
    merge_dict = {
        "first_sheet": "absolte",
        "next_merge_dict": None,
    }


class AccucorLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that just defines how to convert an accucor excel file to the format
    accepted by the parent class's load_data method.

    PeakAnnotationsLoader is an abstract base class.  It has a method called convert_df() that uses the data described
    here to automatically convert self.df to the format it accepts.
    """

    merged_column_rename_dict = {
        "formula": "Formula",
        "medMz": "MedMz",
        "medRt": "MedRt",
        "isotopeLabel": "IsotopeLabel",
    }

    merged_drop_columns_list = [
        "compound",
        "adductName",
        "label",
        "metaGroupId",
        "groupId",
        "goodPeakCount",
        "maxQuality",
        "compoundId",
        "expectedRtDiff",
        "ppmDiff",
        "parent",
        "C_Label",
    ]

    add_columns_dict = {
        # Sheet: dict
        "Original": {
            # New column name: method that takes a dataframe to create the new column
            "C_Label": (
                lambda df: df["isotopeLabel"]
                .str.split("-")
                .str.get(-1)
                .replace({"C12 PARENT": "0"})
                .astype(int)
            ),
            # Rename happens after merge, but before merge, we want matching column names in each sheet, so...
            "Compound": lambda df: df["compound"],
        }
    }

    merge_dict = {
        "first_sheet": "Corrected",  # This key only occurs once in the outermost dict
        "next_merge_dict": {
            "on": ["Compound", "C_Label"],
            "left_columns": None,  # all
            "right_sheet": "Original",
            "right_columns": ["formula", "medMz", "medRt", "isotopeLabel"],
            "how": "left",
            "next_merge_dict": None,
        },
    }
