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
    """
    Converts either an excel or tab delimited file into a dataframe.
    """
    filetypes = ["csv", "tsv", "excel"]
    extensions = ["csv", "tsv", "xlsx"]
    ext = pathlib.Path(filepath).suffix.strip(".")

    if filetype is None and ext not in extensions:
        try:
            dataframe = _read_from_xlsx(
                filepath, sheet=sheet, keep_default_na=keep_default_na, dropna=dropna
            )
        except (InvalidFileException, ValueError, BadZipFile):  # type: ignore
            try:
                dataframe = _read_from_tsv(
                    filepath, keep_default_na=keep_default_na, dropna=dropna
                )
            except Exception:
                raise CommandError(
                    'Invalid file extension: "%s", expected one of %s',
                    extensions,
                    ext,
                )
    elif filetype == "excel" or ext == "xlsx":
        dataframe = _read_from_xlsx(
            filepath,
            sheet=sheet,
            keep_default_na=keep_default_na,
            dropna=dropna,
            na_values=na_values,
            dtype=dtype,
            expected_headers=expected_headers,
        )
    elif filetype == "tsv" or ext == "tsv":
        dataframe = _read_from_tsv(
            filepath,
            dtype=dtype,
            keep_default_na=keep_default_na,
            dropna=dropna,
            na_values=na_values,
            expected_headers=expected_headers,
        )
    elif filetype == "csv" or ext == "csv":
        dataframe = _read_from_csv(
            filepath,
            dtype=dtype,
            keep_default_na=keep_default_na,
            dropna=dropna,
            na_values=na_values,
            expected_headers=expected_headers,
        )
    else:
        if filetype is not None and filetype not in filetypes:
            raise CommandError(
                'Invalid file type: "%s", expected one of %s',
                filetypes,
                filetype,
            )
        else:
            raise CommandError(
                'Invalid file extension: "%s", expected one of %s',
                extensions,
                ext,
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

    _validate_headers(
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

    _validate_headers(
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

    _validate_headers(
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


def _validate_headers(filepath, headers, expected_headers=None):
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
    """Confirms all headers are present, irrespective of case and order."""
    return sorted([s.lower() for s in headers]) == sorted([s.lower() for s in expected])


def get_sheet_names(filepath):
    """
    Returns a list of sheet names in an excel file.  Returns None if the file is not an excel file.
    """
    try:
        return pd.ExcelFile(filepath, engine="openpyxl").sheet_names
    except (InvalidFileException, ValueError, BadZipFile):  # type: ignore
        return None  # Not an excel file


def merge_dataframes(left, right, on):
    return pd.merge(left=left, right=right, on=on)


def _headers_are_not_unique(headers):
    num_uniq_heads = len(pd.unique(headers))
    num_heads = len(headers)
    if num_uniq_heads != num_heads:
        return True, num_uniq_heads, num_heads
    return False, num_uniq_heads, num_heads
