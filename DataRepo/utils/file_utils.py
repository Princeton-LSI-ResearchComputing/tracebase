import pathlib
from zipfile import BadZipFile

import pandas as pd
from django.core.management import CommandError
from openpyxl.utils.exceptions import InvalidFileException

from DataRepo.utils.exceptions import DuplicateHeaders, InvalidHeaders


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
        filetype (str): Enumeration ["csv", "tsv", "excel"]
        dtype (Dict(str)): header: type
        keep_default_na (bool): The keep_default_na arg to pandas
        dropna (bool): Whether to drop na
        na_values (bool): The na_values arg to pandas
        expected_headers (List(str)): List of all expected header names

    Raises:
        CommandError

    Returns:
        Pandas dataframe of parsed and processed infile data
    """
    filetypes = ["csv", "tsv", "excel"]
    extensions = {
        "csv": "csv",
        "tsv": "tsv",
        "xlsx": "excel",
    }

    if filetype is None:
        ext = pathlib.Path(filepath).suffix.strip(".")
        if ext in extensions.keys():
            filetype = extensions[ext]
        elif ext not in extensions.keys():
            if is_excel(filepath):
                filetype = "excel"
            else:
                raise CommandError(
                    'Invalid file extension: "%s", expected one of %s',
                    extensions.keys(),
                    ext,
                )
    elif filetype not in filetypes:
        raise CommandError(
            'Invalid file type: "%s", expected one of %s',
            filetypes,
            filetype,
        )

    if filetype == "excel":
        dataframe = _read_from_xlsx(
            filepath,
            sheet=sheet,
            keep_default_na=keep_default_na,
            dropna=dropna,
            na_values=na_values,
            dtype=dtype,
            expected_headers=expected_headers,
        )
    elif filetype == "tsv":
        dataframe = _read_from_tsv(
            filepath,
            dtype=dtype,
            keep_default_na=keep_default_na,
            dropna=dropna,
            na_values=na_values,
            expected_headers=expected_headers,
        )
    elif filetype == "csv":
        dataframe = _read_from_csv(
            filepath,
            dtype=dtype,
            keep_default_na=keep_default_na,
            dropna=dropna,
            na_values=na_values,
            expected_headers=expected_headers,
        )

    return dataframe


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
    sheets = pd.ExcelFile(filepath, engine="openpyxl").sheet_names
    if str(sheet_name) not in sheets:
        sheet_name = 0

    validate_headers(
        filepath,
        _read_headers_from_xlsx(filepath, sheet=sheet_name),
        expected_headers,
    )

    kwargs = {
        "sheet_name": sheet_name,  # The first sheet
        "engine": "openpyxl",
        "keep_default_na": keep_default_na,
    }
    if dtype is not None:
        kwargs["dtype"] = dtype
    if na_values is not None:
        kwargs["na_values"] = na_values

    df = pd.read_excel(filepath, **kwargs)

    if keep_default_na or na_values is not None:
        dropna = False

    if dropna:
        return df.dropna(axis=0, how="all")

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
        return df.dropna(axis=0, how="all")

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
        return df.dropna(axis=0, how="all")
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
        raise DuplicateHeaders(filepath, nall, nuniqs)

    if expected_headers is not None and not headers_are_as_expected(
        expected_headers, headers
    ):
        raise InvalidHeaders(headers, expected_headers, filepath)


def _read_headers_from_xlsx(filepath, sheet=0):
    sheet_name = sheet
    sheets = pd.ExcelFile(filepath, engine="openpyxl").sheet_names
    if str(sheet_name) not in sheets:
        sheet_name = 0

    # Note, setting `mangle_dupe_cols=False` would overwrite duplicates instead of raise an exception, so we're
    # checking for duplicate headers manually here.
    return (
        pd.read_excel(
            filepath,
            nrows=1,  # Read only the first row
            header=None,
            sheet_name=sheet_name,  # The first sheet
            engine="openpyxl",
        )
        .squeeze("columns")
        .iloc[0]
    )


def _read_headers_from_tsv(filepath):
    # Note, setting `mangle_dupe_cols=False` would overwrite duplicates instead of raise an exception, so we're
    # checking for duplicate headers manually here.
    return (
        pd.read_table(
            filepath,
            nrows=1,
            header=None,
        )
        .squeeze("columns")
        .iloc[0]
        .to_list()
    )


def _read_headers_from_csv(filepath):
    # Note, setting `mangle_dupe_cols=False` would overwrite duplicates instead of raise an exception, so we're
    # checking for duplicate headers manually here.
    return (
        pd.read_csv(
            filepath,
            nrows=1,
            header=None,
        )
        .squeeze("columns")
        .iloc[0]
        .to_list()
    )


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
        pd.ExcelFile(filepath, engine="openpyxl")
        return True
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
