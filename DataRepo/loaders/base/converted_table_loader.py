from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from typing import Optional

import pandas as pd

from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    EmptyColumns,
    MissingColumnGroup,
    RequiredHeadersError,
    UnequalColumnGroups,
)


class ConvertedTableLoader(TableLoader, ABC):
    """This class is a middleware class that serves to convert 1 or more dataframes into a single dataframe that is
    compatible with TableLoader.  Its purpose is to convert multiple similar data formats into a single universal format
    that can be processed by a single TableLoader class.  To use it, create a TableLoader derived class that inherits
    from ConvertedTableLoader instead of TableLoader that works on a universal format.  Then create a class for each
    format to be converted from that derive from that class, and set the values for the abstract properties defined
    below.  No other code is necessary.

    NOTE: When creating an instance of a derived class (e.g. "Format1Loader" below), supply a dataframe or dict of
    dataframes, but note that properties like merge_dict only operate on a dict of dataframes.  Any non-dict dataframe
    supplied to the constructor will be assumed to be the sheet defined in merge_dict["first_sheet"] and any conversion
    steps that operate on any other sheet will simply be skipped and assumed to have been done manually, prior to
    instantiation.

    Example:

    class UniversalFormatLoader(ConvertedTableLoader):
        # Define as you would any TableLoader class
        pass

    class Format1Loader(UniversalFormatLoader):
        merged_column_rename_dict = See abstractmethod docstring below...
        condense_columns_dict = See abstractmethod docstring below...
        merge_dict = See abstractmethod docstring below...
        add_columns_dict = See abstractmethod docstring below...
        merged_drop_columns_list = See abstractmethod docstring below...

    class Format2Loader(UniversalFormatLoader):
        merged_column_rename_dict = See abstractmethod docstring below...
        condense_columns_dict = See abstractmethod docstring below...
        merge_dict = See abstractmethod docstring below...
        add_columns_dict = See abstractmethod docstring below...
        merged_drop_columns_list = See abstractmethod docstring below...
    """

    @property
    @abstractmethod
    def OrigDataTableHeaders(self):
        # namedtuple spec that accounts for ALL headers from ALL sheets.  Needed for OrigData* attributes.
        pass

    @property
    @abstractmethod
    def OrigDataHeaders(self):
        # namedtuple of strings - should include headers from ALL sheets that will be merged.  Needed for evaluating
        # suitability of input data and reverting new to old versions.
        pass

    @property
    @abstractmethod
    def OrigDataColumnTypes(self) -> Optional[dict]:
        """A dict that sets the types of each original column (to be provided to read_from_file as its dtypes option).

        Example:
            {
                "FORMULA": str,
                "MEDMZ": float,
                "MEDRT": float,
                "ISOTOPELABEL": str,
                "COMPOUND": str,
            }
        """
        pass

    @property
    @abstractmethod
    def OrigDataRequiredHeaders(self):
        # Dict of lists of header key strings keyed on sheet name.
        pass

    @property
    @abstractmethod
    def add_columns_dict(self) -> Optional[dict]:
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

    @property
    @abstractmethod
    def condense_columns_dict(self) -> Optional[dict]:
        """A dict keyed on sheet name, containing a list of all of the columns to keep as-is, and the names of 2 new
        columns (one that will contain the headers of the remaining columns and one to contain the values).  The
        original columns not in the keep-list (there should be more than 1) will end up being removed and the resulting
        dataframe will be taller (the number of starting rows times the number of remaining columns).

        This will happen BEFORE the sheet merge, thus the merge_dict can refer to the column created here.

        It also happens AFTER the add_columns_dict is applied, thus it should refer to added columns.

        Example:
            {
                "Original": {
                    "header_column": "Sample",
                    "value_column": "Raw Abundance",
                    "uncondensed_columns": ["label", "metaGroupId", "groupId", "goodPeakCount", "medMz", "medRt",
                        "maxQuality", "adductName", "isotopeLabel", "compound", "compoundId", "formula",
                        "expectedRtDiff", "ppmDiff", "parent", "Compound", "C_Label"],
                },
                "Corrected": {
                    "header_column": "Sample",
                    "value_column": "Corrected Abundance",
                    "uncondensed_columns": ["Compound", "C_Label", "adductName"],
                },
            }
        """
        pass

    @property
    @abstractmethod
    def merge_dict(self) -> dict:
        """A recursively constructed dict describing how to merge the sheets in the input dataframe (dict).
        NOTE: The first sheet must be the one with the corrected abundances.
        NOTE: Setting right_columns to None or an empty list joins all columns.
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
    def nan_defaults_dict(self) -> Optional[dict]:
        """If the left-merge results in columns with NaN values filled in, use this dict to specify default values to
        fill in in their place.
        Example:
            {
                "Raw Abundance": 0,
                "medMz": 0,
                "medRt": 0,
            }
        """
        pass

    @property
    @abstractmethod
    def sort_columns(self) -> Optional[list]:
        """Sort the merged dataframe based on these columns.  Define this if nan_filldown_columns is defined.
        Example:
            ["Sample Header", "Compound", "C_Label"]
        """
        pass

    @property
    @abstractmethod
    def nan_filldown_columns(self) -> Optional[list]:
        """If the left-merge results in columns with NaN values filled in, use this list to fill them using values
        defined on previous rows.  Be sure to define sort_columns.
        Example:
            ["formula"]
        """
        pass

    @property
    @abstractmethod
    def merged_column_rename_dict(self) -> Optional[dict]:
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
    def merged_drop_columns_list(self) -> Optional[list]:
        """List of columns to specifically remove after merge.  No error will be raised if the column is already absent.
        Example:
            ["compound"]
        """
        pass

    def convert_df(self):
        """Uses the abstract properties defined in a derived class to convert the given data format (e.g. an accucor
        excel file or an isocorr csv file) into a universal format accepted by this parent class's loading code.

        NOTE: Always (makes a deep copy of and) operates on self.orig_df.

        Args:
            None
        Exceptions:
            Buffers:
                ValueError
            Raises:
                AggregatedErrors
        Returns:
            outdf (Optional[pandas DataFrame]): Converted single DataFrame
        """
        indf = deepcopy(self.orig_df)

        # If the value for key 'next_merge_dict' is None, it is inferred to mean that no merge is
        # necessary.  Just set the outdf to that one dataframe indicated by the sole sheet key.
        single_sheet = self.get_single_sheet()

        # Create keys for recording the columns present in the dataframe (dict)
        self.initialize_merge_dict()

        # Get initial output dataframe
        if isinstance(indf, pd.DataFrame):
            outdf = indf.copy(deep=True)
        elif isinstance(indf, dict):
            outdf = dict((sheet, adf.copy(deep=True)) for sheet, adf in indf.items())
        else:
            raise TypeError("df must be either a pandas.DataFrame or a dict.")

        try:
            # Add columns
            self.add_df_columns(outdf)

            # Condense multiple columns into 2 columns (a header name column and a value column)
            outdf = self.condense_columns(outdf)

            # Merge sheets
            outdf = self.merge_df_sheets(outdf)

            # Fill in NaN values resulting from the left merge
            outdf = self.update_nans_with_defaults(outdf)
            outdf = self.sort_df(outdf)
            outdf = self.fill_down_nan_columns(outdf)
        except AggregatedErrors:
            raise
        except KeyError as ke:
            if isinstance(indf, pd.DataFrame):
                self.check_output_dataframe(outdf)
            raise self.aggregated_errors_object.buffer_error(ke)
        except Exception as e:
            raise self.aggregated_errors_object.buffer_error(e)

        # Rename columns
        try:
            if self.merged_column_rename_dict is not None:
                outdf = outdf.rename(columns=self.merged_column_rename_dict)
        except Exception as e:
            if isinstance(indf, pd.DataFrame) and single_sheet is None:
                raise self.aggregated_errors_object.buffer_error(
                    ValueError(
                        f"A dataframe dict containing the following sheets/keys: {self.get_required_sheets()} is "
                        "required."
                    ),
                    orig_exception=e,
                )
            else:
                raise self.aggregated_errors_object.buffer_error(e)

        # Drop unwanted columns
        if self.merged_drop_columns_list is not None:
            outdf = outdf.drop(self.merged_drop_columns_list, axis=1, errors="ignore")

        # Check the results for validity
        self.check_output_dataframe(outdf)

        self.df = outdf

        return outdf

    def check_output_dataframe(self, outdf):
        """Checks to make sure the supplied output dataframe has all the required headers.

        Args:
            outdf (pandas.DataFrame): This should be a fully converted dataframe.
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                RequiredHeadersError
        Returns:
            None
        """
        missing = []
        for hk in self.DataRequiredHeaders:
            hdr = getattr(self.DataHeaders, hk)
            if hdr not in outdf.columns:
                missing.append(hdr)
        if len(missing) > 0:
            raise AggregatedErrors().buffer_error(
                RequiredHeadersError(self.revert_headers(missing))
            )

    def get_single_sheet(self):
        """If there is nothing to merge, the one sheet that the conversion is based on is returned.

        Args:
            None
        Exceptions:
            None
        Returns:
            sheet (str)
        """
        return (
            self.merge_dict["first_sheet"]
            if self.merge_dict["next_merge_dict"] is None
            else None
        )

    def add_df_columns(self, df):
        """Creates/adds columns to pandas DataFrames based on the methods in self.add_columns_dict that generate columns
        from existing DataFrame columns.

        Args:
            df (dict of pandas DataFrames OR a pandas DataFrame)
        Exceptions:
            Buffers:
                None
            Raises:
                AggregatedErrors
        Returns:
            None
        """
        if self.add_columns_dict is None:
            return

        if isinstance(df, pd.DataFrame):
            single_sheet = self.get_single_sheet()
            if (
                single_sheet is not None
                and single_sheet in self.add_columns_dict.keys()
            ):
                try:
                    for new_column, method in self.add_columns_dict[
                        single_sheet
                    ].items():
                        df[new_column] = method(df)
                except KeyError:
                    # We will assume they did the merge themselves and that KeyErrors from adding columns come from
                    # sheets without the corrected sample data, and that adding columns to other sheets is only to
                    # facilitate the merge, and thus, we can ignore all KeyErrors
                    # An error will occur below if this is not the case
                    pass
            # If we're getting a single dataframe when multiple sheets to merge are required, we will assume the merge
            # has already been done and that all required columns are present
        elif isinstance(df, dict):
            for sheet, column_dict in self.add_columns_dict.items():
                for new_column, method in column_dict.items():
                    df[sheet][new_column] = method(df[sheet])
        else:
            raise TypeError("df must be either a pandas.DataFrame or a dict.")

    def condense_columns(self, in_df):
        """Creates/adds columns to pandas DataFrames from multiple other columns, based on self.condense_columns_dict.

        NOTE: Only a single set of columns to condense can be specified, defined by the columns not included in the
        uncondensed_columns list.  See the example in the abstract definition of self.condense_columns_dict.

        Args:
            df (dict of pandas DataFrames OR a pandas DataFrame)
        Exceptions:
            Buffers:
                None
            Raises:
                AggregatedErrors
        Returns:
            outdf (pandas.DataFrame or dict of pandas.DataFrames)
        """
        outdf = in_df

        if self.condense_columns_dict is None:
            return outdf

        if isinstance(in_df, pd.DataFrame):
            single_sheet = self.get_single_sheet()
            if (
                single_sheet is not None
                and single_sheet in self.condense_columns_dict.keys()
            ):
                try:
                    # This prevents errors about missing id_vars columns (eg. if the user has done their own conversion)
                    existing_static_columns = self.get_existing_static_columns(
                        single_sheet,
                        {single_sheet: in_df},
                        False,
                    )

                    outdf = in_df.melt(
                        var_name=self.condense_columns_dict[single_sheet][
                            "header_column"
                        ],
                        value_name=self.condense_columns_dict[single_sheet][
                            "value_column"
                        ],
                        id_vars=existing_static_columns,
                    )
                except Exception as e:
                    raise AggregatedErrors().buffer_error(e)
            # Else, we will assume the user did the conversion themselves
        elif isinstance(in_df, dict):
            check_sheets_consistent = True
            for sheet in self.condense_columns_dict.keys():
                try:
                    # This prevents errors about missing id_vars columns (eg. if the user has done their own conversion)
                    existing_static_columns = self.get_existing_static_columns(
                        sheet,
                        in_df,
                        check_sheets_consistent,
                    )
                    # Only need to do this once (and buffer only 1 error about it)
                    check_sheets_consistent = False

                    outdf[sheet] = in_df[sheet].melt(
                        var_name=self.condense_columns_dict[sheet]["header_column"],
                        value_name=self.condense_columns_dict[sheet]["value_column"],
                        id_vars=existing_static_columns,
                    )
                except Exception as e:
                    raise AggregatedErrors().buffer_error(e)
        else:
            raise TypeError("df must be either a pandas.DataFrame or a dict.")

        return outdf

    def get_existing_static_columns(self, sheet, df_dict, check_consistency=True):
        """Performs error checks and returns the columns existing in the supplied sheet's dataframe that will not be
        pivoted.

        Args:
            sheet (str): Sheet name
            df_dict (dict of pandas.DataFrames): Dataframes keyed on sheet
            check_consistency (bool) [True]: Check if multiple sheets containing the same column type have the same
                number of condense columns.
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                MissingColumnGroup (e.g. when there are no supplied sample columns in any of the sheets to be condensed/
                    melted)
                UnequalColumnGroups (e.g. when the number of sample columns differs between the original and corrected
                    sheets)
                EmptyColumns (Columns picked up by pandas that are actually empty and have no header, to which pandas
                    assigns a name starting with "Unnamed: ...")
        Returns:
            existing_static_columns (List[str]): These are what DataFrame.melt's id_vars arg takes.  It's necessary to
                whittle it down to the ones that exist in the dataframe to avoid errors (e.g. if the user has done their
                own conversion, or if the derived class does the conversion)
        """
        if (
            self.condense_columns_dict is None
            or sheet not in self.condense_columns_dict.keys()
        ):
            return

        is_fatal = False

        # These are columns that won't be "condensed"
        existing_static_columns = []

        # Exclude missing id_vars columns
        for hdr in self.condense_columns_dict[sheet]["uncondensed_columns"]:
            if hdr in df_dict[sheet].columns:
                existing_static_columns.append(hdr)

        # The condense column list and metadata keyed by header_column and sheet
        # "header_column" is the "type" of all the columns that will be pivoted, e.g. "sample names"
        condense_cols = defaultdict(lambda: defaultdict(list))
        # We're also going to check the "condense" columns for validity.  Sometimes, an excel sheet has empty columns
        # that pandas thinks are populated columns.  When that happens, it prepends a randomized header with "Unnamed: "
        erroneous_condense_columns = []

        # Check for missing column groups (e.g. missing sample columns) and build the condense_cols dict to check for a
        # consistent number of "sample" columns between sheets (that will be condensed)
        for sheet_key in self.condense_columns_dict.keys():
            condense_columns_exist = False
            for hdr in df_dict[sheet_key].columns:
                if (
                    hdr
                    not in self.condense_columns_dict[sheet_key]["uncondensed_columns"]
                ):
                    condense_columns_exist = True
                    condense_cols[
                        self.condense_columns_dict[sheet_key]["header_column"]
                    ][sheet_key].append(hdr)
            if not condense_columns_exist:
                is_fatal = True
                self.aggregated_errors_object.buffer_error(
                    MissingColumnGroup(
                        self.condense_columns_dict[sheet_key]["header_column"],
                        sheet=sheet_key,
                        file=self.friendly_file,
                    )
                )

        # This checks that (for example) the number of sample columns is the same in each sheet (as defined in the
        # condense_columns_dict)
        if check_consistency:
            for coltype in condense_cols.keys():
                # If there are multiple sheets with the same type of "sample" cols
                if len(condense_cols[coltype].keys()) > 1:
                    num_cols = -1

                    # For each sheet with "sample" columns
                    for sheet in condense_cols[coltype].keys():
                        # Initialize the number of "sample" columns with the first one
                        if num_cols == -1:
                            num_cols = len(condense_cols[coltype][sheet])
                        elif num_cols != len(condense_cols[coltype][sheet]):
                            # If any sheet has a different number as the first, buffer an error
                            self.aggregated_errors_object.buffer_error(
                                UnequalColumnGroups(
                                    coltype,
                                    condense_cols[coltype],
                                    file=self.friendly_file,
                                )
                            )
                            break

        # Check for empty columns
        for hdr in list(df_dict[sheet].columns):
            if hdr not in self.condense_columns_dict[sheet][
                "uncondensed_columns"
            ] and hdr.startswith("Unnamed: "):
                erroneous_condense_columns.append(hdr)
        if len(erroneous_condense_columns) > 0:
            df_dict[sheet].drop(columns=erroneous_condense_columns, inplace=True)
            group = list(set(df_dict[sheet].columns) - set(existing_static_columns))

            # We will error if this means there are no sample columns.  Otherwise, empty columns will be skipped/ignored
            # (with a warning)
            is_error = len(group) == 0
            if is_error:
                msg = "There are no columns left to pivot.  Please fix the file to proceed."
                is_fatal = True
            else:
                msg = "These unnamed columns will be skipped."
            self.aggregated_errors_object.buffer_exception(
                EmptyColumns(
                    self.condense_columns_dict[sheet]["header_column"],
                    existing_static_columns,
                    erroneous_condense_columns,
                    list(df_dict[sheet].columns),
                    sheet=sheet,
                    file=self.friendly_file,
                    addendum=msg,
                ),
                is_error=is_error,
                is_fatal=self.validate,
            )

        if is_fatal and self.aggregated_errors_object.should_raise():
            raise self.aggregated_errors_object

        return existing_static_columns

    def revert_headers(self, headers):
        """This method takes a list of headers from the universal PeakAnnotationsLoader headers, converts them back to
        the original input format's headers (using self.merged_column_rename_dict) and then organizes them into their
        original sheets in a dict.

        Args:
            headers (list): header names
        Exceptions:
            None
        Returns:
            orig_headers (dict): header name lists keyed on sheet names
        """
        rev_rename_dict = {}
        if self.merged_column_rename_dict is not None:
            # Assumes self.merged_column_rename_dict values are unique
            rev_rename_dict = dict(
                (v, k) for k, v in self.merged_column_rename_dict.items()
            )

        # Un-rename the merged headers
        rev_headers = [
            (rev_rename_dict[h] if h in rev_rename_dict.keys() else h) for h in headers
        ]

        # Final output dict (reverted headers keyed on the original sheets)
        rev_headers_dict = defaultdict(list)

        # The first sheet from the merge dict is the default (because headers not explicitly in the merge dict will be
        # there), unless the user did their own conversion, then we don't know the original sheet name, in which case,
        # there could be unexpected columns
        default_sheet = (
            self.merge_dict["first_sheet"]
            if isinstance(self.orig_df, dict)
            else "Unnamed sheet"
        )

        if self.merge_dict["next_merge_dict"] is None:
            rev_headers_dict[default_sheet] = rev_headers
            return dict(rev_headers_dict)

        # For each reverted header, we will search the merge dict for it
        for rh in rev_headers:
            if (
                self.merge_dict["left_all_columns"] is not None
                and len(self.merge_dict["left_all_columns"]) > 0
                and rh in self.merge_dict["left_all_columns"]
            ):
                rev_headers_dict[self.merge_dict["first_sheet"]].append(rh)
            else:
                merge_dict = self.merge_dict["next_merge_dict"].copy()
                right_sheet = default_sheet
                while merge_dict is not None:
                    if (
                        merge_dict["right_all_columns"] is not None
                        and len(merge_dict["right_all_columns"]) > 0
                        and rh in merge_dict["right_all_columns"]
                    ):
                        rev_headers_dict[merge_dict["right_sheet"]].append(rh)
                    elif merge_dict["next_merge_dict"] is None:
                        rev_headers_dict[right_sheet].append(rh)
                    if merge_dict["next_merge_dict"] is not None:
                        merge_dict = merge_dict["next_merge_dict"].copy()
                    else:
                        merge_dict = merge_dict["next_merge_dict"]

        return dict(rev_headers_dict)

    def original_column_lookup(self, new_column):
        """Given a new column name from the universal format (/converted dataframe), return the original source column
        name and the original sheet it was in.

        Note, if the new_column supplied was generated by add_columns() or condense_columns(), the new_column and
        the default sheet (self.merge_dict["first_sheet"]) will be returned, as the reversion of those columns is not
        yet supported.

        Args:
            new_column (str): The column name from the converted dataframe
        Exceptions:
            None
        Returns:
            old_column (str)
            old_sheet (str)
        """
        rev_headers_dict = self.revert_headers([new_column])
        old_column, old_sheet = list(
            (orig_col_lst[0], orig_sheet)
            for orig_sheet, orig_col_lst in rev_headers_dict.items()
        )[0]
        return old_column, old_sheet

    def initialize_merge_dict(self):
        """Initialize the left/right_all_columns values.  (This is so the derived class doesn't have to initialize it.)

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        merge_dict = self.merge_dict
        merge_dict["left_all_columns"] = None
        while merge_dict["next_merge_dict"] is not None:
            merge_dict = merge_dict["next_merge_dict"]
            merge_dict["right_all_columns"] = None

    @classmethod
    def get_required_sheets(cls):
        """Returns a list of required original sheets.

        Args:
            None
        Exceptions:
            None
        Returns:
            sheets (List[str])
        """
        return list(cls.OrigDataRequiredHeaders.keys())

    @classmethod
    def get_required_headers(cls, sheet=None):
        """Returns a list of required original headers in the supplied required sheet.  Returns headers from all
        required sheets if the supplied sheet is None.

        Args:
            None
        Exceptions:
            None
        Returns:
            sheets (List[str])
        """
        if sheet is None:
            all_hdr_keys = []
            for lst in cls.OrigDataRequiredHeaders.values():
                for hdr_key in lst:
                    if hdr_key not in all_hdr_keys:
                        all_hdr_keys.append(hdr_key)
        else:
            all_hdr_keys = cls.OrigDataRequiredHeaders[sheet]

        return [getattr(cls.OrigDataHeaders, hdr_key) for hdr_key in all_hdr_keys]

    def merge_df_sheets(self, in_df, _outdf=None, _merge_dict=None, _first_sheet=None):
        """Uses self.merge_dict to recursively merge in_df's sheets into a single merged dataframe (if in_df is a dict).

        Args:
            in_df (dict of pandas DataFrames or a pandas DataFrame): i.e. the return of read_from_file
            _outdf (Optional[pandas DataFrame]) [in_df[self.merge_dict["first_sheet"]]]: Used in recursive calls to
                build up a dataframe from 2 or more sheets in in_df.
            _merge_dict (Optional[dict]) [self.merge_dict]: A recursively constructed dict describing how to merge the
                sheets in in_df.  Example:
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
            _outdf (pandas DataFrame): Dataframe that has been merged from in_df based on _merge_dict.  Note, if
                _merge_dict["next_merge_dict"] is None at the outermost level of the dict,
                in_df[self.merge_dict["first_sheet"]] is returned (i.e. no merge is performed).
        """
        if isinstance(in_df, pd.DataFrame):
            return in_df

        if _merge_dict is None:
            self.initialize_merge_dict()
            _merge_dict = self.merge_dict.copy()

        if _outdf is None:
            if _first_sheet is None:
                if "first_sheet" not in _merge_dict.keys():
                    raise KeyError("'first_sheet' not supplied and not in merge_dict.")
                _first_sheet = _merge_dict["first_sheet"]
            _outdf = in_df[_first_sheet]

            # Record the original headers in this sheet (assumes _merge_dict was None, above - should be, since this is
            # a private argument)
            self.merge_dict["left_all_columns"] = list(_outdf.columns)

            if _merge_dict["next_merge_dict"] is None:
                return _outdf
            _merge_dict = _merge_dict["next_merge_dict"].copy()

        left_df = _outdf
        if (
            _merge_dict["left_columns"] is not None
            and len(_merge_dict["left_columns"]) > 0
        ):
            left_df = _outdf[_merge_dict["left_columns"]]

        right_sheet = _merge_dict["right_sheet"]
        right_df = in_df[right_sheet]
        _merge_dict["right_all_columns"] = list(right_df.columns)
        right_columns = _merge_dict["right_columns"]

        # Make sure that the join columns are included (if specific columns are being extracted)
        if right_columns is not None and len(right_columns) > 0:
            for on_col in _merge_dict["on"]:
                if on_col not in right_columns:
                    right_columns.append(on_col)

        # If we are only merging a subset of headers
        if right_columns is not None and len(right_columns) > 0:
            # Only merge the columns specified in _merge_dict["right_columns"] by dropping all but the given subset:
            right_df = in_df[right_sheet][right_columns]

        # The user may have edited the file to remove unwanted/unnecessary columns, so in order to avoid errors from
        # pandas.merge, let's not join on columns that are absent in one sheet or the other
        on_columns = []
        for on_col in _merge_dict["on"]:
            if on_col in list(right_df.columns) or on_col in list(left_df.columns):
                on_columns.append(on_col)

        if len(on_columns) == 0:
            if len(_merge_dict["on"]) == 0:
                raise ValueError(
                    "merge_dict is malformed.  No common columns were specified for the merge."
                )
            raise ValueError(
                "merge_dict is malformed.  None of the common column s to merge on were present in the input "
                "dataframe."
            )

        _outdf = pd.merge(
            left=left_df,
            right=right_df,
            on=on_columns,
            how=_merge_dict["how"],
            suffixes=("_x", "_y"),  # Explicit defaults
        )

        # Since we did a left join, rows missing in the right dataframe will end up with NaNs.  If their type is
        # numeric, fill them with zeros.
        num_type_fill_dict = {}
        for col, typ in self.get_column_types().items():
            if col in _outdf.columns and typ in [
                int,
                float,
                pd.Int64Dtype(),
                pd.Float64Dtype(),
            ]:
                num_type_fill_dict[col] = 0
        if len(num_type_fill_dict.keys()) > 0:
            _outdf.fillna(num_type_fill_dict, inplace=True)

        if _merge_dict["next_merge_dict"] is None:
            return _outdf

        return self.merge_df_sheets(
            in_df,
            _outdf=_outdf,
            _merge_dict=_merge_dict["next_merge_dict"].copy(),
        )

    def get_column_types(self, **kwargs):
        """Override of TableLoader.get_column_types, to add the original header types to the converted header types.
        Returns a dict of column types by header name (not header key).

        Limitations:
            1. Does not support custom headers.
            2. Headers with the same name in multiple sheets can only have 1 type
        Args:
            None
        Raises:
            Nothing
        Returns:
            dtypes (dict): Types by header name (instead of by header key)
        """
        # Get the converted header types
        dtypes = super().get_column_types(**kwargs)
        if dtypes is None:
            dtypes = {}

        headers = self.OrigDataHeaders

        for key in self.OrigDataColumnTypes.keys():
            hdr = getattr(headers, key)
            dtypes[hdr] = self.OrigDataColumnTypes[key]

        return dtypes

    def update_nans_with_defaults(self, indf):
        """Uses self.nan_defaults_dict to fill in NaN values with default values.  The keys of the dict are column names
        to change and the values are either static values of functions that take a dataframe as input.

        Example:
            Using:
                self.nan_defaults_dict = {"isotopeLabel": lambda df: "C13-label-" + df["C_Label"]}
            You get:
                C_Label isotopeLabel    ->  C_Label isotopeLabel
                2       NaN             ->  2       C13-label-2

        Args:
            indf (pd.DataFrame)
        Exceptions:
            None
        Returns:
            outdf (pd.DataFrame)
        """
        outdf = indf.copy(deep=True)
        if (
            self.nan_defaults_dict is not None
            and len(self.nan_defaults_dict.keys()) > 0
        ):
            for col, val_or_method in self.nan_defaults_dict.items():
                if col not in outdf.columns:
                    continue
                if type(val_or_method).__name__ == "function":
                    outdf[col].fillna(val_or_method(outdf), inplace=True)
                else:
                    outdf[col].fillna(val_or_method, inplace=True)
        return outdf

    def sort_df(self, indf):
        """Sorts a dataframe by self.sort_columns."""
        outdf = indf.copy(deep=True)
        if self.sort_columns is not None and len(self.sort_columns) > 0:
            outdf.sort_values(by=self.sort_columns, inplace=True)
        return outdf

    def fill_down_nan_columns(self, indf):
        """'Fill down' values from previous rows in place of NaN values.

        Args:
            indf (pd.DataFrame)
        Exceptions:
            None
        Returns:
            outdf (pd.DataFrame)
        """
        outdf = indf.copy(deep=True)
        if self.nan_filldown_columns is not None and len(self.nan_filldown_columns) > 0:
            for col in self.nan_filldown_columns:
                if col in outdf.columns:
                    outdf[col].ffill(inplace=True)
        return outdf

    def __init__(self, *args, **kwargs):
        """Constructor.

        Limitations:
            Custom headers for the peak annotation details file are not (yet) supported.  Only the class defaults of the
                MSRunsLoader are allowed.

        *NOTE: This constructor requires the file argument (which is an optional argument to the superclass) if the df
        argument is supplied.

        Args:
            Superclass Args:
                df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
                dry_run (Optional[boolean]) [False]: Dry run mode.
                defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT
                    MUST HANDLE THE ROLLBACK.
                data_sheet (Optional[str]): Sheet name (for error reporting).
                defaults_sheet (Optional[str]): Sheet name (for error reporting).
                *file (Optional[str]): File path.
                filename (Optional[str]): Filename (for error reporting).
                user_headers (Optional[dict]): Header names by header key.
                defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
                defaults_file (Optional[str]): Defaults file name (None if the same as infile).
                headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
                defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.
                extra_headers (Optional[List[str]]): Use for dynamic headers (different in every file).  To allow any
                    unknown header, supply an empty list.
                _validate (bool): If true, runs in validate mode, perhaps better described as "non-curator mode".  This
                    is intended for use by the web validation interface.  It's similar to dry-run mode, in that it never
                    commits anything, but it also raises warnings as fatal (so they can be reported through the web
                    interface and seen by researchers, among other behaviors specific to non-privileged users).
            Derived (this) class Args:
                df (Optional[pandas dataframe|Dict[str, pandas dataframe]]): This constructor intercepts the
                    TableLoader's df argument, modifying it if a dict of dataframes is supplied, so that it can convert
                    it to a pandas dataframe.
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                ConditionallyRequiredArgs
        Returns:
            None
        """
        # Preserve the original df
        self.orig_df = kwargs.pop("df", None)

        # TODO: Add support for df being a dict to TableLoader (having it obtain the df using the DataSheetName).  Then
        # remove this code that modifies the df (so that the TableLoader code that obtains column header (when there are
        # user defaults) will work).
        if self.orig_df is not None:
            # This simulates a pandas dataframe where the sheet names are the "headers" and the columns are empty.  We
            # are only doing this because the TableLoader constructor retrieves the df.columns when initializing user
            # defaults.  And, the new StudyLoader class overloads this for that prupose.  The TODO item above is to
            # formally codify this as a supported use-case in TableLoader.
            tmp_df_dict = {}
            if isinstance(self.orig_df, dict):
                for sheetname in self.orig_df.keys():
                    tmp_df_dict[sheetname] = {}
                kwargs["df"] = pd.DataFrame.from_dict(tmp_df_dict)

        # Cannot call super().__init__() because ABC.__init__() takes a custom argument
        TableLoader.__init__(self, *args, **kwargs)
        # Overwrite what the superclass saved with a converted version.  The constructor does nothing with the df, and
        # convert_df() makes a deep copy, so this is OK.
        if self.orig_df is not None:
            try:
                # Overwrites self.df
                self.convert_df()
            except AggregatedErrors:
                raise
            except Exception as e:
                raise self.aggregated_errors_object.buffer_error(e)
