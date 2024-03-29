import pathlib
from collections import defaultdict
from datetime import datetime
from zipfile import BadZipFile

import pandas as pd
import yaml
from django.core.files.uploadedfile import TemporaryUploadedFile
from django.core.management import CommandError
from openpyxl.utils.exceptions import InvalidFileException

from DataRepo.utils.exceptions import (
    DateParseError,
    DuplicateFileHeaders,
    ExcelSheetNotFound,
    InvalidDtypeDict,
    InvalidDtypeKeys,
    InvalidHeaders,
)


def read_from_file(
    filepath,
    sheet=0,
    filetype=None,
    dtype=None,
    keep_default_na=False,
    dropna=True,
    na_values=None,
    expected_headers=None,
):
    """Converts either an excel or tab delimited file into a dataframe.

    Args:
        filepath (str): Path to infile
        sheet (str): Name of excel sheet
        filetype (str): Enumeration ["csv", "tsv", "excel", "yaml"]
        dtype (Dict(str)): header: type
        keep_default_na (bool): The keep_default_na arg to pandas
        dropna (bool): Whether to drop na
        na_values (bool): The na_values arg to pandas
        expected_headers (List(str)): List of all expected header names

    Raises:
        CommandError

    Returns:
        Pandas dataframe of parsed and processed infile data.
        Or, if the filetype is yaml, returns a python object.
    """
    filetype = _get_file_type(filepath, filetype=filetype)
    retval = None

    if filetype == "excel":
        retval = _read_from_xlsx(
            filepath,
            sheet=sheet,
            keep_default_na=keep_default_na,
            dropna=dropna,
            na_values=na_values,
            dtype=dtype,
            expected_headers=expected_headers,
        )
    elif filetype == "tsv":
        retval = _read_from_tsv(
            filepath,
            dtype=dtype,
            keep_default_na=keep_default_na,
            dropna=dropna,
            na_values=na_values,
            expected_headers=expected_headers,
        )
    elif filetype == "csv":
        retval = _read_from_csv(
            filepath,
            dtype=dtype,
            keep_default_na=keep_default_na,
            dropna=dropna,
            na_values=na_values,
            expected_headers=expected_headers,
        )
    elif filetype == "yaml":
        retval = _read_from_yaml(filepath)

    return retval


def _check_dtype_arg(
    filepath,
    result,
    sheet=0,
    dtype=None,
):
    # Error-check the dtype argument supplied
    if dtype is not None and len(dtype.keys()) > 0 and result is not None:
        # This assumes the retval is a dataframe
        missing = []
        for dtk in dtype.keys():
            if dtk not in result.columns:
                missing.append(dtk)
        if len(missing) == len(dtype.keys()):
            # None of the keys are present in the dataframe
            # Raise programming errors immediately
            raise InvalidDtypeDict(
                dtype,
                file=filepath,
                sheet=sheet,
                columns=list(result.columns),
            )
        elif len(missing) > 0:
            idk = InvalidDtypeKeys(
                missing,
                file=filepath,
                sheet=sheet,
                columns=list(result.columns),
            )
            # Some columns may be optional, so if at least 1 is correct, just issue a warning.
            print(f"WARNING: {type(idk).__name__}: {idk}")


def read_headers_from_file(
    filepath,
    sheet=0,
    filetype=None,
):
    """Converts either an excel or tab delimited file into a dataframe.

    Args:
        filepath (str): Path to infile
        sheet (str): Name of excel sheet
        filetype (str): Enumeration ["csv", "tsv", "excel", "yaml"]
        expected_headers (List(str)): List of all expected header names

    Raises:
        CommandError

    Returns:
        headers (list of string)
    """
    filetype = _get_file_type(filepath, filetype=filetype)
    retval = None

    if filetype == "excel":
        retval = _read_headers_from_xlsx(filepath, sheet=sheet)
    elif filetype == "tsv":
        retval = _read_headers_from_tsv(filepath)
    elif filetype == "csv":
        retval = _read_headers_from_csv(filepath)
    elif filetype == "yaml":
        raise CommandError(
            'Invalid file type: "%s", yaml files do not have headers', filetype
        )

    return retval


def _get_file_type(filepath, filetype=None):
    filetypes = ["csv", "tsv", "excel", "yaml"]
    extensions = {
        "csv": "csv",
        "tsv": "tsv",
        "xlsx": "excel",
        "yaml": "yaml",
        "yml": "yaml",
    }

    if isinstance(filepath, TemporaryUploadedFile):
        filepath = filepath.temporary_file_path()

    if filetype is None:
        ext = pathlib.Path(filepath).suffix.strip(".")

        if ext in extensions.keys():
            filetype = extensions[ext]
        else:
            try:
                pd.ExcelFile(filepath, engine="openpyxl")
                filetype = "excel"
            except (InvalidFileException, ValueError, BadZipFile):  # type: ignore
                raise CommandError(
                    'Invalid file extension: "%s", expected one of %s',
                    ext,
                    extensions.keys(),
                )
    elif filetype not in filetypes:
        raise CommandError(
            'Invalid file type: "%s", expected one of %s',
            filetype,
            filetypes,
        )

    return filetype


def _read_from_yaml(filepath):
    with open(filepath) as headers_file:
        return yaml.safe_load(headers_file)


def _read_from_xlsx(
    filepath,
    sheet=0,
    dtype=None,
    keep_default_na=False,
    dropna=True,
    expected_headers=None,
    na_values=None,
):
    sheet_name = sheet
    sheets = get_sheet_names(filepath)

    if sheet is None:
        sheet_name = sheets

    # If more than 1 sheet is being read, make recursive calls to get dataframes using the intended dtype dict
    if isinstance(sheet_name, list):
        if expected_headers is not None:
            raise NotImplementedError(
                "expected_headers not supported with multiple sheets."
            )

        # dtype is assumed to be a 2D dict by sheet and column
        df_dict = {}
        for sheet_n in sheet_name:
            dtype_n = None
            if isinstance(dtype, dict):
                dtype_n = dtype.get(sheet_n, None)

            # Recursive calls
            df_dict[sheet_n] = read_from_file(
                filepath,
                sheet=sheet_n,
                dtype=dtype_n,
                keep_default_na=keep_default_na,
                dropna=dropna,
                # TODO: Add support for expected headers
                # expected_headers=None,
                na_values=na_values,
            )

        return df_dict

    if (
        sheet_name is not None
        and not isinstance(sheet_name, int)
        and sheet_name not in sheets
        and (expected_headers is not None or len(sheets) == 1)
    ):
        # If we know the expected headers or there's only 1 sheet, let's take a chance that the first sheet is correct,
        # despite a name mismatch.  If this isn't true, there will either be an IndexError or a downstream error.
        sheet_name = 0

    try:
        validate_headers(
            filepath,
            _read_headers_from_xlsx(filepath, sheet=sheet_name),
            expected_headers,
        )
    except IndexError as ie:
        if (
            sheet_name is not None
            and not isinstance(sheet_name, int)
            and sheet_name not in sheets
        ):
            raise ExcelSheetNotFound(sheet=sheet_name, file=filepath, all_sheets=sheets)
        raise ie

    kwargs = {
        "sheet_name": sheet_name,
        "engine": "openpyxl",
        "keep_default_na": keep_default_na,
    }
    if dtype is not None:
        kwargs["dtype"] = dtype
    if na_values is not None:
        kwargs["na_values"] = na_values

    df = pd.read_excel(filepath, **kwargs)

    if dtype is not None:
        # astype() requires the keys be present in the columns (as opposed to dtype)
        astype = {}
        for k, v in dtype.items():
            if k in df.columns:
                astype[k] = v
        if len(astype.keys()) > 0:
            df = df.astype(astype)

    if keep_default_na or na_values is not None:
        dropna = False

    if dropna:
        df = df.dropna(axis=0, how="all")

    _check_dtype_arg(
        filepath,
        df,
        sheet=sheet,
        dtype=dtype,
    )

    return df


def _read_from_tsv(
    filepath,
    dtype=None,
    keep_default_na=False,
    dropna=True,
    expected_headers=None,
    na_values=None,
):
    kwargs = _collect_kwargs(
        keep_default_na=keep_default_na, na_values=na_values, dtype=dtype
    )

    df = pd.read_table(filepath, **kwargs)

    validate_headers(
        filepath,
        _read_headers_from_tsv(filepath),
        expected_headers,
    )

    if keep_default_na or na_values is not None:
        dropna = False

    if dropna:
        df = df.dropna(axis=0, how="all")

    _check_dtype_arg(
        filepath,
        df,
        dtype=dtype,
    )

    return df


def _read_from_csv(
    filepath,
    dtype=None,
    keep_default_na=False,
    dropna=True,
    expected_headers=None,
    na_values=None,
):
    kwargs = _collect_kwargs(
        keep_default_na=keep_default_na, na_values=na_values, dtype=dtype
    )

    df = pd.read_csv(filepath, **kwargs)

    validate_headers(
        filepath,
        _read_headers_from_csv(filepath),
        expected_headers,
    )

    if keep_default_na or na_values is not None:
        dropna = False

    if dropna:
        df = df.dropna(axis=0, how="all")

    _check_dtype_arg(
        filepath,
        df,
        dtype=dtype,
    )

    return df


def _collect_kwargs(dtype=None, keep_default_na=False, na_values=None):
    """
    Compiles a dict with keep_default_na and only the remaining keyword arguments that have values.

    Note, this function was created solely to avoid a JSCPD error.
    """
    kwargs = {"keep_default_na": keep_default_na}
    if na_values is not None:
        kwargs["na_values"] = na_values
    if dtype is not None:
        kwargs["dtype"] = dtype
    return kwargs


def validate_headers(filepath, headers, expected_headers=None):
    """Checks that all headers are the expected headers.

    Args:
        filepath (str): Path to infile
        headers (List(str)): List of present header names
        expected_headers (List(str)): List of all expected header names

    Raises:
        DuplicateHeaders
        InvalidHeaders

    Returns:
        Nothing
    """
    not_unique, nuniqs, nall = _headers_are_not_unique(headers)

    if not_unique:
        raise DuplicateFileHeaders(filepath, nall, nuniqs, headers)

    if expected_headers is not None and not headers_are_as_expected(
        expected_headers, headers
    ):
        raise InvalidHeaders(headers, expected_headers, filepath)


def _read_headers_from_xlsx(filepath, sheet=0):
    sheet_name = sheet
    sheets = get_sheet_names(filepath)
    if str(sheet_name) not in sheets:
        sheet_name = 0

    # Note, setting `mangle_dupe_cols=False` would overwrite duplicates instead of raise an exception, so we're
    # checking for duplicate headers manually here.
    raw_headers = (
        pd.read_excel(
            filepath,
            nrows=1,  # Read only the first row
            header=None,
            sheet_name=sheet_name,
            engine="openpyxl",
        )
        .squeeze("columns")
        .iloc[0]
    )
    # Apparently, if there's only 1 header, .iloc[0] returns a string, otherwise a series
    if type(raw_headers) == str:
        return [raw_headers]
    return raw_headers.to_list()


def _read_headers_from_tsv(filepath):
    # Note, setting `mangle_dupe_cols=False` would overwrite duplicates instead of raise an exception, so we're
    # checking for duplicate headers manually here.
    raw_headers = (
        pd.read_table(
            filepath,
            nrows=1,
            header=None,
        )
        .squeeze("columns")
        .iloc[0]
    )
    # Apparently, if there's only 1 header, .iloc[0] returns a string, otherwise a series
    if type(raw_headers) == str:
        return [raw_headers]
    return raw_headers.to_list()


def _read_headers_from_csv(filepath):
    # Note, setting `mangle_dupe_cols=False` would overwrite duplicates instead of raise an exception, so we're
    # checking for duplicate headers manually here.
    raw_headers = (
        pd.read_csv(
            filepath,
            nrows=1,
            header=None,
        )
        .squeeze("columns")
        .iloc[0]
    )
    # Apparently, if there's only 1 header, .iloc[0] returns a string, otherwise a series
    if type(raw_headers) == str:
        return [raw_headers]
    return raw_headers.to_list()


def headers_are_as_expected(expected, headers):
    """Confirms all headers are present, irrespective of case and order.

    Args:
        expected (List(str)): List of all expected header names
        headers (List(str)): List of present header names

    Raises:
        Nothing

    Returns:
        bool: Whether headers are valid or not
    """
    return sorted([s.lower() for s in headers]) == sorted([s.lower() for s in expected])


def get_sheet_names(filepath):
    """Returns a list of sheet names in an excel file.

    Args:
        filepath (str): Path to infile

    Raises:
        InvalidFileException
        ValueError
        BadZipFile

    Returns:
        List(str): Sheet names
    """
    return pd.ExcelFile(filepath, engine="openpyxl").sheet_names


def is_excel(filepath):
    """Determines whether a file is an excel file or not.

    Args:
        filepath (str): Path to infile

    Raises:
        Nothing

    Returns:
        bool: Whether the file is an excel file or not
    """
    try:
        return filepath is not None and _get_file_type(filepath) == "excel"
    except (InvalidFileException, ValueError, BadZipFile):  # type: ignore
        return False


def merge_dataframes(left, right, on):
    """Merges 2 sheets using a common column.

    Args:
        left (str): Name of excel sheet
        right (str): Name of excel sheet
        on (str): Name of column in both the left and right sheets

    Raises:
        Nothing

    Returns:
        Pandas dataframe of merged sheet data
    """
    return pd.merge(left=left, right=right, on=on)


def _headers_are_not_unique(headers):
    num_uniq_heads = len(pd.unique(headers))
    num_heads = len(headers)
    if num_uniq_heads != num_heads:
        return True, num_uniq_heads, num_heads
    return False, num_uniq_heads, num_heads


# TODO: When the SampleTableLoader is converted to a derived class of TableLoader, remove this method
def get_column_dupes(data, unique_col_keys, ignore_row_idxs=None):
    """Find combination duplicates from file table data.

    Args:
        data (DataFrame or list of dicts): The table data parsed from a file.
        unique_col_keys (list of column name strings): Column names whose combination must be unique.
        ignore_row_idxs (list of integers): Rows to ignore.

    Returns:
        1. A dict keyed on the composite duplicate value (with embedded header names).  The value is a dict with
        the keys "rowidxs" and "vals". rowidxs has a list of indexes of the rows containing the combo value and vals
        contains a dict of the column name and value pairs.
        2. A list of all row indexes containing duplicate data.
    """
    val_locations = defaultdict(dict)
    dupe_dict = defaultdict(dict)
    all_row_idxs_with_dupes = []
    dict_list = data if type(data) == list else data.to_dict("records")

    for rowidx, row in enumerate(dict_list):
        # Ignore rows where the animal name is empty
        if ignore_row_idxs is not None and rowidx in ignore_row_idxs:
            continue

        # Ignore empty combos
        empty_combo = True
        for ck in unique_col_keys:
            val = row.get(ck, None)
            if val is not None or not isinstance(val, str) or val == "":
                empty_combo = False
                break
        if empty_combo:
            continue

        composite_val = ", ".join(
            list(map(lambda ck: f"{ck}: [{str(row.get(ck, 'None'))}]", unique_col_keys))
        )

        if len(val_locations[composite_val].keys()) > 0:
            val_locations[composite_val]["rowidxs"].append(rowidx)
        else:
            val_locations[composite_val]["rowidxs"] = [rowidx]
            val_locations[composite_val]["vals"] = {}
            for ck in unique_col_keys:
                val_locations[composite_val]["vals"][ck] = row.get(ck, None)

    # Now create the dupe dict to contain values encountered more than once
    for val in val_locations.keys():
        row_list = val_locations[val]["rowidxs"]
        if len(row_list) > 1:
            dupe_dict[val]["rowidxs"] = row_list
            dupe_dict[val]["vals"] = val_locations[val]["vals"]
            all_row_idxs_with_dupes += row_list

    return dupe_dict, all_row_idxs_with_dupes


def string_to_datetime(
    date_str, format_str=None, file=None, sheet=None, rownum=None, column=None
):
    if type(date_str) != str:
        # Raise a programming error immediately
        raise TypeError(
            f"date_str {date_str} must be a string, but got {type(date_str)}"
        )

    if format_str is None:
        # This format assumes that the date_str is from an excel column with converted dates
        format_str = "%Y-%m-%d"
        # Note, excel "general" columns detect and covert what looks like dates to '%Y-%m-%d' with " 00:00:00" appended
        # This replaces " 00:00:00" with an empty string to avoid a ValueError exception
        date_str = date_str.replace(" 00:00:00", "")

    try:
        dt = datetime.strptime(date_str.strip(), format_str)
    except ValueError as ve:
        if "unconverted data remains" in str(ve):
            raise DateParseError(
                date_str,
                ve,
                format=format_str,
                file=file,
                sheet=sheet,
                rownum=rownum,
                column=column,
            )
        raise ve
    return dt
