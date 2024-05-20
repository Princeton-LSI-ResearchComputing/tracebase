from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
from sqlite3 import ProgrammingError
from typing import Dict, List, Optional, TypedDict

import pandas as pd
import regex
from django.db import transaction

from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.loaders.table_column import TableColumn
from DataRepo.loaders.table_loader import TableLoader
from DataRepo.models import (
    ElementLabel,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupLabel,
)
from DataRepo.models.archive_file import ArchiveFile, DataFormat, DataType
from DataRepo.models.msrun_sample import MSRunSample
from DataRepo.models.sample import Sample
from DataRepo.models.tracer_label import TracerLabel
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConditionallyRequiredArgs,
    HeaderAsSampleDoesNotExist,
    IsotopeObservationParsingError,
    NoTracerLabeledElements,
    RecordDoesNotExist,
    RequiredHeadersError,
    RollbackException,
    UnknownHeaderError,
)
from DataRepo.utils.file_utils import string_to_datetime

# regex has the ability to store repeated capture groups' values and put them in a list
ISOTOPE_LABEL_PATTERN = regex.compile(
    # Match repeated elements and mass numbers (e.g. "C13N15")
    r"^(?:(?P<elements>["
    + "".join(ElementLabel.labeled_elements_list())
    + r"]{1,2})(?P<mass_numbers>\d+))+"
    # Match either " PARENT" or repeated counts (e.g. "-labels-2-1")
    + r"(?: (?P<parent>PARENT)|-label(?:-(?P<counts>\d+))+)$"
)


class IsotopeObservationData(TypedDict):
    element: str
    mass_number: int
    count: int
    parent: bool


class PeakAnnotationsLoader(TableLoader, ABC):
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
    def merge_dict(self) -> dict:
        """A recursively constructed dict describing how to merge the sheets in df_dict.
        NOTE: The first sheet must be the one with the corrected abundances.
        NOTE: Setting right_columns to None or an empty list joins all columns.
        NOTE: Set right_has_raw_abunds to True if the right sheet
        Example:
            {
                "first_sheet": "Corrected",  # This key ponly occurs once in the outermost dict
                "next_merge_dict": {
                    "on": ["Compound", "C_Label"],
                    "left_columns": None,  # all
                    "right_sheet": "Original",
                    "right_columns": ["formula", "medMz", "medRt", "isotopeLabel"],
                    "right_has_raw_abunds": True,
                    "how": "left",
                    "next_merge_dict": None,
                }
            }
        """
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
    def merged_drop_columns_list(self) -> Optional[list]:
        """List of columns to specifically remove after merge.  No error will be raised if the column is already absent.
        Example:
            ["compound"]
        """
        pass

    @property
    @abstractmethod
    def format_code(self) -> str:
        """The DataFormat.code for the peak annotation file"""
        pass

    # Prepend this to sample header for columns containing raw abundances.  See merge_dict: right_has_raw_abunds.
    raw_abund_prefix = "RAW:"

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    MEDMZ_KEY = "MEDMZ"
    MEDRT_KEY = "MEDRT"
    ISOTOPELABEL_KEY = "ISOTOPELABEL"
    FORMULA_KEY = "FORMULA"
    COMPOUND_KEY = "COMPOUND"

    DataSheetName = "Corrected"  # The official sole sheet name of the converted/merged peak annotation data

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

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        [COMPOUND_KEY, ISOTOPELABEL_KEY],
    ]

    # A mapping of database field to column.  Only set when 1 field maps to 1 column.  Omit others.
    # NOTE: The sample headers are always different, so we cannot map those here
    FieldToDataHeaderKey = {
        PeakGroup.__name__: {
            "name": COMPOUND_KEY,
            "formula": FORMULA_KEY,
        },
        PeakGroupLabel.__name__: {
            "element": ISOTOPELABEL_KEY,
        },
        PeakData.__name__: {
            "med_mz": MEDMZ_KEY,
            "med_rt": MEDRT_KEY,
        },
        PeakDataLabel.__name__: {
            "element": ISOTOPELABEL_KEY,
            "count": ISOTOPELABEL_KEY,
            "mass_number": ISOTOPELABEL_KEY,
        },
    }

    DataColumnMetadata = DataTableHeaders(
        MEDMZ=TableColumn.init_flat(name=DataHeaders.MEDMZ, field=PeakData.med_mz),
        MEDRT=TableColumn.init_flat(name=DataHeaders.MEDRT, field=PeakData.med_rt),
        FORMULA=TableColumn.init_flat(
            name=DataHeaders.FORMULA, field=PeakGroup.formula
        ),
        COMPOUND=TableColumn.init_flat(name=DataHeaders.COMPOUND, field=PeakGroup.name),
        ISOTOPELABEL=TableColumn.init_flat(
            name=DataHeaders.ISOTOPELABEL,
            type=str,
            help_text=(
                "A formatted string describing the labeled elements identified in the compound in this specific peak."
            ),
            format=(
                "Either 'C12 PARENT' for unlabeled compounds or a formatted string describing the element(s), mass "
                "number(s), and label counts, e.g. 'C13N15-label-3-2' which contains 3 Carbons with a mass number of "
                "13 and 2 Nitrogens with a mass number of 15."
            ),
            header_required=True,
            value_required=True,
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling.
    Models = [PeakData, PeakDataLabel, PeakGroup, PeakGroupLabel]

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
                data_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                defaults_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                *file (Optional[str]) [None]: File name (for error reporting).
                user_headers (Optional[dict]): Header names by header key.
                defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
                defaults_file (Optional[str]) [None]: Defaults file name (None if the same as infile).
                headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
                defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.
                extra_headers (Optional[List[str]]): Use for dynamic headers (different in every file).  To allow any
                    unknown header, supply an empty list.
            Derived (this) class Args:
                peak_annotation_details_file (Optional[str]): The name of the file that the Peak Annotation Details came
                    from.
                peak_annotation_details_sheet (Optional[str]): The name of the sheet that the Peak Annotation Details
                    came from (if it was an excel file).
                peak_annotation_details_df (Optional[pandas DataFrame]): The DataFrame of the Peak Annotation Details
                    sheet/file that will be supplied to the MSRunsLoader class (that is an instance meber of this
                    instance)
                operator (Optional[str]): The researcher who ran the mass spec.  Mutually exclusive with defaults_df
                    (when it has a default for the operator column for the Sequences sheet).
                lc_protocol_name (Optional[str]): Name of the liquid chromatography method.  Mutually exclusive with
                    defaults_df (when it has a default for the lc_protocol_name column for the Sequences sheet).
                instrument (Optional[str]): Name of the mass spec instrument.  Mutually exclusive with defaults_df
                    (when it has a default for the instrument column for the Sequences sheet).
                date (Optional[str]): Date the Mass spec instrument was run.  Format: YYYY-MM-DD.  Mutually exclusive
                    with defaults_df (when it has a default for the date column for the Sequences sheet).
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                ConditionallyRequiredArgs
        Returns:
            None
        """
        peak_annotation_details_file = kwargs.pop("peak_annotation_details_file", None)
        peak_annotation_details_sheet = kwargs.pop(
            "peak_annotation_details_sheet", None
        )
        peak_annotation_details_df = kwargs.pop("peak_annotation_details_df", None)
        operator_default = kwargs.pop("operator", None)
        date_default = kwargs.pop("date", None)
        lc_protocol_name_default = kwargs.pop("lc_protocol_name", None)
        instrument_default = kwargs.pop("instrument", None)

        self.sample_headers = []
        if kwargs.get("df") is not None:
            # Require the file argument if df is supplied
            if kwargs.get("file") is None:
                raise AggregatedErrors().buffer_error(
                    ConditionallyRequiredArgs(
                        "The [file] argument is required is the [df] argument is supplied."
                    )
                )

            kwargs["df"] = self.convert_df(kwargs["df"])

            self.sample_headers = self.get_sample_headers(kwargs["df"].columns)

            # If the derived class did not provide sample headers, get them dynamically
            if "extra_headers" in kwargs.keys() and sorted(
                self.sample_headers
            ) != sorted(kwargs["extra_headers"]):
                raise AggregatedErrors().buffer_error(
                    NotImplementedError(
                        (
                            "The extra_headers argument is not allowed.  The argument is used to define the "
                            f"automatically extracted sample headers {self.sample_headers}.  If there is a problem "
                            "with the sample header extraction or extra headers are needed for another reason, a code "
                            "refactor is necessary."
                        )
                    )
                )

            # Set things up for processing each sample column
            kwargs["extra_headers"] = self.sample_headers
            # TODO: Use this to supply column info to get_row_val for checking column not defined in the class
            self.sample_columns = {}
            for hdr in self.sample_headers:
                self.sample_columns[hdr] = TableColumn.init_flat(
                    name=hdr, type=float, value_required=True
                )

        # We are going to use defaults as processed by the MSRunsLoader (which uses the SequencesLoader) in  order to be
        # able to obtain the correct MSRunSample record that each PeakGroup belongs to
        msrunsloader = MSRunsLoader(
            df=peak_annotation_details_df,
            defaults_df=kwargs.get("defaults_df"),
            defaults_file=kwargs.get("defaults_file"),
            operator=operator_default,
            date=date_default,
            lc_protocol_name=lc_protocol_name_default,
            instrument=instrument_default,
            data_sheet=peak_annotation_details_sheet,
            file=peak_annotation_details_file,
        )

        # Peak annotation details are optional if the operator, date, lc name, and instrument are provided AND the
        # sample headers match the database sample names.  (In which case, using the MSRunsLoader instance above was
        # just to process the default arguments)
        self.msrun_sample_dict = {}
        if peak_annotation_details_df is not None:
            self.msrun_sample_dict = msrunsloader.get_loaded_msrun_sample_dict(
                peak_annot_file=kwargs["file"]
            )

        TableLoader.__init__(self, *args, **kwargs)

        # TODO: Figure out a better way to handle buffered exceptions from another class that are only raised from a
        # specific method, so that methods raise them as a group instead of needing to incorporate instance loaders like
        # this for buffered errors
        self.aggregated_errors_object.merge_aggregated_errors_object(
            msrunsloader.aggregated_errors_object
        )

        # Set the MSRunSequence defaults as a fallback in case a peak annotation details file was not provided
        self.operator_default = msrunsloader.operator_default
        self.date_default = None
        if msrunsloader.date_default is not None:
            self.date_default = string_to_datetime(msrunsloader.date_default)
        self.lc_protocol_name_default = msrunsloader.lc_protocol_name_default
        self.instrument_default = msrunsloader.instrument_default

        # Error tracking
        self.missing_headers_as_samples = []

    def convert_df(self, df):
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
        # If the value for key 'next_merge_dict' is None, it is inferred to mean that no merge is
        # necessary.  Just set the outdf to that one dataframe indicated by the sole sheet key.
        single_sheet = (
            self.merge_dict["first_sheet"]
            if self.merge_dict["next_merge_dict"] is not None
            else None
        )

        if isinstance(df, pd.DataFrame):
            self.initialize_merge_dict()
            outdf = df.copy(deep=True)
            if single_sheet is not None:
                try:
                    self.add_df_columns({single_sheet: outdf})
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
            self.add_df_columns(outdf)
            outdf = self.merge_df_sheets(outdf)

        try:
            if self.merged_column_rename_dict is not None:
                outdf = outdf.rename(columns=self.merged_column_rename_dict)
        except Exception as e:
            if isinstance(df, pd.DataFrame) and single_sheet is None:
                raise AggregatedErrors().buffer_error(
                    ValueError(
                        f"A dataframe dict containing the following sheets/keys: {list(df.keys())} is required."
                    ),
                    orig_exception=e,
                )
            else:
                raise AggregatedErrors().buffer_error(e)

        missing = []
        for hdr in self.DataHeaders._asdict().values():
            if hdr not in outdf.columns:
                missing.append(hdr)
        if len(missing) > 0:
            raise AggregatedErrors().buffer_error(
                RequiredHeadersError(self.revert_headers(missing))
            )

        if self.merged_drop_columns_list is not None:
            outdf = outdf.drop(self.merged_drop_columns_list, axis=1, errors="ignore")

        return outdf

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
        if self.add_columns_dict is not None:
            for sheet, column_dict in self.add_columns_dict.items():
                for new_column, method in column_dict.items():
                    df_dict[sheet][new_column] = method(df_dict[sheet])

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
            (
                rev_rename_dict[h]
                if h in rev_rename_dict.keys()
                else h.removeprefix(self.raw_abund_prefix)
            )
            for h in headers
        ]

        # Final output dict (reverted headers keyed on the original sheets)
        rev_headers_dict = defaultdict(list)

        # The first sheet from the merge dict is the default (because headers not explicitly in the merge dict will be
        # there)
        default_sheet = (
            self.merge_dict["first_sheet"]
            if isinstance(self.df, dict)
            else "Unnamed sheet"
        )

        if self.merge_dict["next_merge_dict"] is not None:
            for rh in rev_headers:
                # For each reverted header, we will search the merge dict for it
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
        else:
            rev_headers_dict[default_sheet] = rev_headers

        return dict(rev_headers_dict)

    def initialize_merge_dict(self):
        # Initialize the right_all_columns values.  (This is so the derived class doesn't have to initialize it)
        if self.merge_dict["next_merge_dict"] is not None:
            merge_dict = self.merge_dict
            while merge_dict["next_merge_dict"] is not None:
                merge_dict = merge_dict["next_merge_dict"]
                if "right_all_columns" not in merge_dict.keys():
                    merge_dict["right_all_columns"] = None

    def merge_df_sheets(
        self, df_dict, _outdf=None, _merge_dict=None, _first_sheet=None
    ):
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
                            "right_has_raw_abunds": True,
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
            self.initialize_merge_dict()
            _merge_dict = self.merge_dict.copy()

        if _outdf is None:
            if _first_sheet is None:
                if "first_sheet" not in _merge_dict.keys():
                    raise KeyError("'first_sheet' not supplied and not in merge_dict.")
                _first_sheet = _merge_dict["first_sheet"]
            _outdf = df_dict[_first_sheet]

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
        right_df = df_dict[right_sheet]
        _merge_dict["right_all_columns"] = list(right_df.columns)
        right_columns = _merge_dict["right_columns"]

        # Make sure that the join columns are included (if specific columns are being extracted)
        if right_columns is not None and len(right_columns) > 0:
            for on_col in _merge_dict["on"]:
                if on_col not in right_columns:
                    right_columns.append(on_col)

        # If the right sheet has (optional) raw abundances to add
        if _merge_dict["right_has_raw_abunds"]:
            # Get the sample headers
            sample_headers = self.get_sample_headers(list(right_df.columns))

            # Prepend the raw abundance prefix to each of them in a dict of original header to prefixed header
            rename_sample_headers_dict = dict(
                (hdr, f"{self.raw_abund_prefix}{hdr}") for hdr in sample_headers
            )

            # Rename the headers in the dataframe (so that the merged headers will be unique)
            df_dict[right_sheet].rename(columns=rename_sample_headers_dict)

            # If we are only merging a subset of headers, add the renamed sample headers to the subset
            if right_columns is not None and len(right_columns) > 0:
                right_columns.extend(list(rename_sample_headers_dict.values()))

        # If we are only merging a subset of headers
        if right_columns is not None and len(right_columns) > 0:
            # Only merge the columns specified in _merge_dict["right_columns"] by dropping all but the given subset:
            right_df = df_dict[right_sheet][right_columns]

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

    def get_sample_headers(self, all_headers: list):
        """Takes a list of all headers and returns a list of sample headers.
        Args:
            all_headers (List[str]): List of all headers (e.g. df.columns)
        Exceptions:
            None
        Returns:
            sample_headers (List[str]): All headers from all_headers that are sample headers
        """
        sample_headers = []
        non_sample_headers = self.get_non_sample_headers()
        for hdr in all_headers:
            if hdr not in non_sample_headers and not hdr.startswith(
                self.raw_abund_prefix
            ):
                sample_headers.append(hdr)
        return sample_headers

    def get_non_sample_headers(self):
        """This includes all self.get_headers(), self.merged_drop_columns_list, and all keys in
        self.merged_column_rename_dict.

        This takes the "safe" route and includes absolutely everything.

        Args:
            None
        Exceptions:
            None
        Returns:
            non_sample_headers (List[str]): All original format and universal format non-sample headers.
        """
        universal_default_headers = set(self.DataHeaders._asdict().values())
        universal_custom_headers = set(self.get_headers()._asdict().values())
        original_saved_headers = set(self.merged_column_rename_dict.keys())
        original_dropped_headers = set(self.merged_drop_columns_list)
        return list(
            universal_default_headers.union(universal_custom_headers)
            .union(original_saved_headers)
            .union(original_dropped_headers)
        )

    def load_data(self):
        """Loads the PeakGroup, PeakGroupLabel, PeakData, and PeakDataLabel tables from the dataframe.
        Args:
            None
        Raises:
            None
        Returns:
            None
        """
        try:
            annot_file_rec, _ = self.get_or_create_annot_file()
        except RollbackException:
            # We cannot go any further, because an ArchiveFile record is required to make all other records
            # TODO: Add in skipped stats here
            return

        for _, row in self.df.iterrows():
            try:
                self.process_peak_annotation_row(row, annot_file_rec)
            except Exception as e:
                # Buffer unexpected errors
                self.aggregated_errors_object.buffer_error(e)

    @transaction.atomic
    def get_or_create_annot_file(self):
        """Gets or creates an ArchiveFile record from self.file
        Args:
            mzxml_file (str or Path object)
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                DataType.DoesNotExist
                DataFormat.DoesNotExist
        Returns:
            mzaf_rec (ArchiveFile)
            mzaf_created (boolean)
            rawaf_rec (ArchiveFile)
            rawaf_created (boolean)
        """
        # Get or create the ArchiveFile record for the mzXML
        try:
            rec_dict = {
                # "filename": xxx,  # Gets automatically filled in by the override of get_or_create
                # "checksum": xxx,  # Gets automatically filled in by the override of get_or_create
                # "is_binary": xxx,  # Gets automatically filled in by the override of get_or_create
                # "imported_timestamp": xxx,  # Gets automatically filled in by the model
                "file_location": self.file,  # Intentionally a string and not a File object
                "data_type": DataType.objects.get(code="ms_peak_annotation"),
                "data_format": DataFormat.objects.get(code=self.format_code),
            }
            rec, created = ArchiveFile.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(ArchiveFile.__name__)
            else:
                self.existed(ArchiveFile.__name__)
        except (DataType.DoesNotExist, DataFormat.DoesNotExist) as dne:
            self.aggregated_errors_object.buffer_error(dne)
            self.skipped(ArchiveFile.__name__)
            raise RollbackException()
        except Exception as e:
            self.handle_load_db_errors(e, ArchiveFile, rec_dict)
            self.errored(ArchiveFile.__name__)
            raise RollbackException()
        return rec, created

    @transaction.atomic
    def process_peak_annotation_row(self, row, annot_file_rec):
        med_mz = self.get_row_val(row, self.headers.MEDMZ)
        med_rt = self.get_row_val(row, self.headers.MEDRT)
        isotope_label = self.get_row_val(row, self.headers.ISOTOPELABEL)
        formula = self.get_row_val(row, self.headers.FORMULA)
        compound_name = self.get_row_val(row, self.headers.COMPOUND)

        for sample_header in self.sample_headers:
            # Obtain the MSRunSample record
            msrun_sample = self.get_msrun_sample(sample_header)

            if msrun_sample is None:
                # TODO: Add skip counts
                continue

            # Get or create a PeakGroup record
            try:
                pgrec, _ = self.get_or_create_peak_group(
                    msrun_sample, compound_name, formula, annot_file_rec
                )
            except RollbackException:
                # TODO: Add skip counts
                continue

            if pgrec is None:
                # TODO: Add skip counts
                continue

            # Obtain the raw and corrected abundances
            try:
                raw_abundance = self.get_row_val(
                    row, f"{self.raw_abund_prefix}{sample_header}"
                )
            except UnknownHeaderError:
                # Raw abundance is optional (e.g. isocorr files don't have it)
                raw_abundance = None
            corrected_abundance = self.get_row_val(row, sample_header)

            # Get or create a PeakData record
            try:
                pdrec, _ = self.get_or_create_peak_data(
                    pgrec, med_mz, med_rt, corrected_abundance, raw_abundance
                )
            except RollbackException:
                # TODO: Add skip counts
                continue

            # Check the labeled elements from the PeakGroup's compound(s)
            if len(pgrec.peak_labeled_elements) == 0:
                self.aggregated_errors_object.buffer_error(
                    # TODO: Define self.tracer_labeled_elements
                    NoTracerLabeledElements(pgrec.name, pgrec.tracer_labeled_elements)
                )
                # TODO: Add skip counts
                continue

            possible_observations = self.get_possible_isotope_observations(pgrec)

            # Parse the isotope obsevations
            try:
                label_observations = self.parse_isotope_string(
                    isotope_label, possible_observations
                )
            except IsotopeObservationParsingError as iope:
                self.aggregated_errors_object.buffer_error(iope)
                # TODO: Add skip counts
                continue

            # Get or create the PeakGroupLabel and PeakDataLabel records
            for label_obs in label_observations:
                try:
                    self.get_or_create_peak_group_label(pgrec, label_obs["element"])
                except RollbackException:
                    self.skipped(PeakDataLabel.__name__)
                    continue

                try:
                    self.get_or_create_peak_data_label(
                        pdrec,
                        label_obs["element"],
                        label_obs["count"],
                        label_obs["mass_number"],
                    )
                except RollbackException:
                    continue

    def get_possible_isotope_observations(
        self, peak_group: PeakGroup
    ) -> List[IsotopeObservationData]:
        """Get the possible isotope observations from a peak group, i.e. all the IsotopeObservationData objects for
        elements from the peak group's compount that exist as labels in the tracers.

        Args:
            peak_group (PeakGroup)
        Exceptions:
            None
        Returns:
            possible_observations (List[IsotopeObservationData])
        """
        possible_observations = []
        tracer_labels = (
            TracerLabel.objects.filter(
                tracer__infusates__id=peak_group.msrun_sample.sample.animal.infusate.id
            )
            .order_by("element")
            .distinct("element")
        )
        for label in tracer_labels:
            possible_observations.append(
                IsotopeObservationData(
                    element=label.element,
                    mass_number=label.mass_number,
                    count=0,
                    parent=True,
                )
            )
        return possible_observations

    @transaction.atomic
    def get_or_create_peak_data_label(self, peak_data, element, count, mass_number):
        rec_dict = {
            "peak_data": peak_data,
            "element": element,
            "count": count,
            "mass_number": mass_number,
        }
        try:
            rec, created = PeakDataLabel.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(PeakDataLabel.__name__)
            else:
                self.existed(PeakDataLabel.__name__)
        except Exception as e:
            self.handle_load_db_errors(e, PeakDataLabel, rec_dict)
            self.errored(PeakDataLabel.__name__)
            raise RollbackException()
        return rec, created

    @transaction.atomic
    def get_or_create_peak_data(
        self, peak_group, med_mz, med_rt, corrected_abundance, raw_abundance=None
    ):
        rec_dict = {
            "peak_group": peak_group,
            "raw_abundance": raw_abundance,
            "corrected_abundance": corrected_abundance,
            "med_mz": med_mz,
            "med_rt": med_rt,
        }
        try:
            rec, created = PeakData.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(PeakData.__name__)
            else:
                self.existed(PeakData.__name__)
        except Exception as e:
            self.handle_load_db_errors(e, PeakData, rec_dict)
            self.errored(PeakData.__name__)
            raise RollbackException()
        return rec, created

    @transaction.atomic
    def get_or_create_peak_group_label(self, peak_group, element):
        rec_dict = {
            "peak_group": peak_group,
            "element": element,
        }
        try:
            rec, created = PeakGroupLabel.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(PeakGroupLabel.__name__)
            else:
                self.existed(PeakGroupLabel.__name__)
        except Exception as e:
            self.handle_load_db_errors(e, PeakGroupLabel, rec_dict)
            self.errored(PeakGroupLabel.__name__)
            raise RollbackException()
        return rec, created

    def get_msrun_sample(self, sample_header):
        """Retrieves the MSRunSample record, either as determined by the self.msrun_sample_dict that was returned by the
        member MSRunsLoader object (via the peak Annotation Details sheet) or by assuming that the sample header matches
        the sample name exactly and using the sequence defaults.

        In the latter case, it first tries to use just the sample name.  If it matches and exists in only a single
        MSRunSample record, it is enough.  If there are multiple, it tries to use the sequence defaults that were either
        provided in the defaults sheet/file or via the command line.  There still could theoretically be multiple
        matches if there are multiple mzXML files all with the same name, but in those cases, the headers must differ to
        be unique and the database sample name cannot match the header, in which case, you would need a PeakAnnotation
        Details sheet/file.

        Args:
            sample_header (str)
        Exceptions:
            None
        Returns:
            msrun_sample (Optional[MSRunSample])
        """
        if (
            sample_header in self.msrun_sample_dict.keys()
            and self.msrun_sample_dict[sample_header] is not None
        ):
            return self.msrun_sample_dict[sample_header]

        # TODO: Not necessarily here, but I should raise errors about:
        # AmbiguousMSRun,
        # AmbiguousMSRuns,
        # DupeCompoundIsotopeCombos,
        # DuplicatePeakGroup,
        # DuplicatePeakGroups,
        # EmptyColumnsError,
        # IsotopeStringDupe,
        # MismatchedSampleHeaderMZXML,
        # MissingCompounds,
        # MissingLCMSSampleDataHeaders,
        # MissingSamplesError,
        # NoSampleHeaders,
        # NoSamplesError,
        # NoTracerLabeledElements,
        # PeakAnnotFileMismatches,
        # ResearcherNotNew,
        # SampleColumnInconsistency,
        # SampleIndexNotFound,
        # TracerLabeledElementNotFound,
        # UnexpectedIsotopes,
        # UnexpectedLCMSSampleDataHeaders,
        # UnskippedBlanksError,

        # We could query MSRunSample directly, but it's useful to know if the sample itself doesn't exist, so we can
        # issue only 1 error
        if sample_header in self.missing_headers_as_samples:
            return None
        samples = Sample.objects.filter(name=sample_header)
        if samples.count() == 0:
            self.aggregated_errors_object.buffer_error(
                HeaderAsSampleDoesNotExist(
                    sample_header,
                    suggestion=(
                        f"Please add a row to {MSRunsLoader.DataSheetName} that matches the sample header to a "
                        "TraceBase sample name."
                    ),
                    file=self.file,
                )
            )
            self.missing_headers_as_samples.append(sample_header)
            return None
        sample = samples.get()

        # Try and get the MSRunSample record only by exact sample name match to the sample header
        msrun_samples = MSRunSample.objects.filter(sample__pk=sample.pk)

        if msrun_samples.count() == 0:
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(MSRunSample, {"sample__name": sample_header})
            )
            return None
        elif msrun_samples.count() == 1:
            return msrun_samples.get()

        # We can assume there were multiple results.  Let's see if if can be narrowed to a single record using the
        # sequence defaults
        if (
            self.operator_default is None
            and self.date_default is None
            and self.lc_protocol_name_default is None
            and self.instrument_default is None
        ):
            if not self.aggregated_errors_object.exception_type_exists(
                ConditionallyRequiredArgs
            ):
                self.aggregated_errors_object.buffer_error(
                    ConditionallyRequiredArgs(
                        "The following arguments supplied to the constructor were insufficient.  Either "
                        "peak_annotation_details_df wasn't supplied or did not have enough information for every "
                        "sample column, in which case, enough of the following default arguments are required to match "
                        "each sample header with the already loaded MSRunSample records: [operator, lc_protocol_name, "
                        "instrument, and/or date."
                    )
                )
            return None

        query_dict = {}
        if self.operator_default is not None:
            query_dict["msrun_sequence__researcher"] = self.operator_default
        if self.lc_protocol_name_default is not None:
            query_dict["msrun_sequence__lc_method__name"] = (
                self.lc_protocol_name_default
            )
        if self.instrument_default is not None:
            query_dict["msrun_sequence__instrument"] = self.instrument_default
        if self.date_default is not None:
            query_dict["msrun_sequence__date"] = self.date_default

        msrun_samples = msrun_samples.filter(**query_dict)

        if msrun_samples.count() == 0:
            query_dict["sample__name"] = sample_header
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(MSRunSample, query_dict)
            )
            return None
        elif msrun_samples.count() > 1:
            try:
                msrun_samples.get()
                self.aggregated_errors_object.buffer_error(
                    ProgrammingError("Well this is unexpected.")
                )
            except Exception as e:
                self.aggregated_errors_object.buffer_error(e)
            return None

        return msrun_samples.get()

    @transaction.atomic
    def get_or_create_peak_group(
        self, msrun_sample, compound_name, formula, peak_annot_file
    ):
        rec_dict = {
            "msrun_sample": msrun_sample,
            "name": compound_name,
            "formula": formula,
            "peak_annotation_file": peak_annot_file,
        }
        try:
            peak_group, created = PeakGroup.objects.get_or_create(**rec_dict)
            if created:
                peak_group.full_clean()
                self.created(PeakGroup.__name__)
            else:
                self.existed(PeakGroup.__name__)
        except Exception as e:
            self.handle_load_db_errors(e, PeakGroup, rec_dict)
            self.errored(PeakGroup.__name__)
            raise RollbackException()
        return peak_group, created

    def get_tracer_labeled_elements(
        self, peak_group: PeakGroup
    ) -> List[IsotopeObservationData]:
        """Returns a list of IsotopeObservationData objects describing the elements that exist among the tracers as if
        they were parent observations (i.e. count=0, parent=True).

        Args:
            peak_group (PeakGroup): A PeakGroup record that does not necessarily have PeakGroupLabel or PeakData records
                linked to it.
        Exceptions:
            None
        Returns:
            tracer_labeled_elements (List[IsotopeObservationData])
        """
        tracers_qs = peak_group.tracer_labeled_elements

        tracer_labeled_elements = []
        for tracer in tracers_qs.all():
            for label in tracer.labels.all():
                this_label = IsotopeObservationData(
                    element=label.element,
                    mass_number=label.mass_number,
                    count=0,
                    parent=True,
                )
                if this_label not in tracer_labeled_elements:
                    tracer_labeled_elements.append(this_label)

        return tracer_labeled_elements

    @classmethod
    def parse_isotope_string(
        cls, label, possible_observations: Optional[List[IsotopeObservationData]] = None
    ) -> List[IsotopeObservationData]:
        """Parse an El-Maven style isotope label string, e.g. C12 PARENT, C13-label-1, C13N15-label-2-1.

        The isotope label string only includes elements observed in the peak reported on the row and a row only exists
        if at least 1 isotope was detected.  However, when an isotope is present, we want to report 0 counts for
        elements present (as labeled) in the tracers when the compound being recorded on has an element that is labeled
        in the tracers, so to include these 0 counts, supply possible_observations.

        NOTE: The isotope label string only includes elements whose label count is greater than 0.  If the tracers
        contain labeled elements that happen to not have been observed in a peak on the row containing the isotope label
        string, that element will not be parsed from the string. For example, on "PARENT" rows, even though "C12" exists
        in the string, an empty list is returned.

        Args:
            label (str): The isotopeLabel string from the DataFrame.
            possible_observations (Optional[List[IsotopeObservationData]]): A list of isotopes that are potentially
                present (e.g. present in the tracers).  Causes 0-counts to be added to non-parent observations.
        Exceptions:
            Raises:
                IsotopeObservationParsingError
            Buffers:
                None
        Returns:
            isotope_observations (List[IsotopeObservationData])
        """
        isotope_observations = []

        match = regex.match(ISOTOPE_LABEL_PATTERN, label)

        if match:
            elements = match.captures("elements")
            mass_numbers = match.captures("mass_numbers")
            counts = match.captures("counts")
            parent_str = match.group("parent")
            parent = False

            if parent_str is not None and parent_str == "PARENT":
                return []
            else:
                if len(elements) != len(mass_numbers) or len(elements) != len(counts):
                    raise IsotopeObservationParsingError(
                        f"Unable to parse the same number of elements ({len(elements)}), mass numbers "
                        f"({len(mass_numbers)}), and counts ({len(counts)}) from isotope label: [{label}]"
                    )
                else:
                    for index in range(len(elements)):
                        isotope_observations.append(
                            IsotopeObservationData(
                                element=elements[index],
                                mass_number=int(mass_numbers[index]),
                                count=int(counts[index]),
                                parent=parent,
                            )
                        )
                    # Record 0-counts for isotopes that were not observed, but could have been
                    if possible_observations is not None:
                        for parent_obs in possible_observations:
                            if parent_obs["element"] not in elements:
                                isotope_observations.append(parent_obs)
        else:
            raise IsotopeObservationParsingError(
                f"Unable to parse isotope label: [{label}]"
            )

        return isotope_observations


class IsocorrLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that just defines how to convert an isocorr excel file to the format
    accepted by the parent class's load_data method.

    PeakAnnotationsLoader is an abstract base class.  It has a method called convert_df() that uses the data described
    here to automatically convert self.df to the format it accepts.
    """

    format_code = "isocorr"

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

    # No columns to add
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

    format_code = "accucor"

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
            "right_has_raw_abunds": True,
            "how": "left",
            "next_merge_dict": None,
        },
    }
