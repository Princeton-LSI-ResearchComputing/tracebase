from abc import ABC, abstractmethod

import pandas as pd

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
        single_sheet = cls.merge_dict["first_sheet"]
        # If there's only 1 key in the merge_dict and its value is None, it is inferred to mean that no merge is
        # necessary.  Just set the outdf to that one dataframe indicated by the sole sheet key.
        if cls.merge_dict["next_merge_dict"] is not None:
            single_sheet = None

        if isinstance(df, pd.DataFrame):
            outdf = df.copy(deep=True)
            if single_sheet is None:
                raise ValueError(
                    f"A dataframe dict containing the following sheets/keys: {list(df.keys())} is required."
                )
            cls.add_df_columns({single_sheet: outdf})
        elif isinstance(df, dict):
            outdf = dict((sheet, adf.copy(deep=True)) for sheet, adf in df.items())
            # If there's only 1 sheet that we need
            if single_sheet is not None:
                if single_sheet in outdf.keys():
                    cls.add_df_columns(outdf)
                else:
                    raise ValueError(
                        f"Sheet [{single_sheet}] missing in the dataframe dict: {list(outdf.keys())}"
                    )

            outdf = cls.merge_df_sheets(outdf)

        return outdf.rename(columns=cls.merged_column_rename_dict)

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

    add_columns_dict = None

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
        "Compound": "Compound",
    }

    add_columns_dict = {
        # Sheet: dict
        "Original": {
            # New column name: method that takes a dataframe to create the new column
            "C_Label": (
                lambda df: df["isotopeLabel"]
                .str.split("-")
                .str.get(-1)
                .replace({"C12 PARENT": "0"})
            ),
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
        },
    }
