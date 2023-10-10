import os
from collections import defaultdict
from datetime import timedelta

import pandas as pd
from django.core.exceptions import ValidationError

from DataRepo.models.lc_method import LCMethod

LCMS_HEADERS = (
    "tracebase sample name",  # Required
    "sample data header",  # Required
    "mzxml filename",
    "peak annotation filename",
    "instrument",
    "operator",
    "date",
    "ms mode",
    "lc method",
    "lc run length",
    "lc description",
)


def lcms_df_to_dict(df, aes=None):
    """
    Parse the LCMS dataframe created from an LCMS file, and key by sample data header (which must be unique).
    Takes an optional aggregated errors object.
    """
    lcms_metadata = {}
    samples = []
    dupes = {}
    missing_reqd_vals = defaultdict(list)

    if df is None:
        return lcms_metadata

    for idx, row in df.iterrows():
        # Convert empty strings to None
        for key in row.keys():
            if (
                row[key] is not None
                and type(row[key]) == str
                and row[key].strip() == ""
            ):
                row[key] = None

        sample_name = row["tracebase sample name"]
        if sample_name is None:
            missing_reqd_vals["tracebase sample name"].append(idx + 2)
        elif sample_name not in samples:
            samples.append(sample_name)

        sample_header = row["sample data header"]
        if sample_header is None:
            missing_reqd_vals["sample data header"].append(idx + 2)
        elif sample_header in lcms_metadata.keys():
            if sample_header in dupes.keys():
                dupes[sample_header].append(str(idx + 2))
            else:
                dupes[sample_header] = [
                    lcms_metadata[sample_header]["row_num"],
                    str(idx + 2),
                ]
            continue

        lc_name = None
        if row["lc method"] is not None and row["lc run length"] is not None:
            lc_name = LCMethod.create_name(row["lc method"], row["lc run length"])

        peak_annot = None
        if row["peak annotation filename"] is not None:
            peak_annot = os.path.basename(row["peak annotation filename"]).strip()

        run_len = None
        if row["lc run length"] is not None:
            run_len = timedelta(minutes=int(row["lc run length"]))

        lcms_metadata[sample_header] = {
            "sample_header": sample_header,
            "sample_name": sample_name,
            "peak_annotation": peak_annot,
            "mzxml": row["mzxml filename"],
            "ms_protocol_name": row["ms mode"],
            "researcher": row["instrument"],
            "instrument": row["operator"],
            "date": row["date"],
            "lc_type": row["lc method"],
            "lc_run_length": run_len,
            "lc_description": row["lc description"],
            "lc_name": lc_name,
            "row_num": str(idx + 2),  # From 1, not including header row
        }

    if len(dupes.keys()) > 0:
        exc = DuplicateSampleDataHeaders(dupes, lcms_metadata, samples)
        if aes is not None:
            aes.buffer_error(exc)
        else:
            raise exc

    if len(missing_reqd_vals.keys()) > 0:
        exc = MissingRequiredLCMSValues(missing_reqd_vals)
        if aes is not None:
            aes.buffer_error(exc)
        else:
            raise exc

    return lcms_metadata


def lcms_metadata_to_samples(lcms_metadata):
    """
    Parse the LCMS dataframe created from an LCMS file, and key by sample (which must be unique)
    """
    samples = []

    for sample_header in lcms_metadata.keys():
        sample_name = lcms_metadata[sample_header]["sample_name"]
        if sample_name not in samples:
            samples.append(sample_name)

    return samples


def extract_dataframes_from_lcms_xlsx(lcms_file):
    headers = (
        pd.read_excel(
            lcms_file,
            nrows=1,  # Read only the first row
            header=None,
            sheet_name=0,  # The first sheet
            engine="openpyxl",
        )
        .squeeze("columns")
        .iloc[0]
    )

    if not lcms_headers_are_valid(headers):
        raise InvalidLCMSHeaders(headers, LCMS_HEADERS, lcms_file)

    return pd.read_excel(
        lcms_file,
        sheet_name=0,  # The first sheet
        engine="openpyxl",
    ).dropna(axis=0, how="all")


def extract_dataframes_from_lcms_tsv(lcms_file):
    headers = (
        pd.read_table(
            lcms_file,
            nrows=1,
            header=None,
        )
        .squeeze("columns")
        .iloc[0]
        .to_list()
    )

    if not lcms_headers_are_valid(headers):
        raise InvalidLCMSHeaders(headers, LCMS_HEADERS, lcms_file)

    return pd.read_table(
        lcms_file,
        keep_default_na=False,
    ).dropna(axis=0, how="all")


def lcms_headers_are_valid(headers):
    """Confiorms all headers are present, irrespective of case and order."""
    return sorted([s.lower() for s in headers]) == sorted(
        [s.lower() for s in LCMS_HEADERS]
    )


class DuplicateSampleDataHeaders(Exception):
    def __init__(self, dupes, lcms_metadata, samples):
        cs = ", "
        dupes_str = "\n\t".join(
            [f"{k} rows: [{cs.join(dupes[k])}]" for k in dupes.keys()]
        )
        message = (
            "The following sample data headers were found to have duplicates on the LCMS metadata file on the "
            "indicated rows:\n\n"
            f"\t{dupes_str}"
        )
        super().__init__(message)
        self.dupes = dupes
        # used by code that catches this exception
        self.lcms_metadata = lcms_metadata
        self.samples = samples


class InvalidLCMSHeaders(ValidationError):
    def __init__(self, headers, expected_headers=None, lcms_file=None):
        if expected_headers is None:
            expected_headers = LCMS_HEADERS
        message = "LCMS metadata "
        if lcms_file is not None:
            message += f"file [{lcms_file}] "
        missing = [i for i in expected_headers if i not in headers]
        unexpected = [i for i in headers if i not in expected_headers]
        if len(missing) > 0:
            message += f"is missing headers {type(missing)}: {missing}"
        if len(missing) > 0 and len(unexpected) > 0:
            message += " and "
        if len(unexpected) > 0:
            message += f" has unexpected headers: {unexpected}"
        super().__init__(message)
        self.headers = headers
        self.expected_headers = expected_headers
        self.lcms_file = lcms_file
        self.missing = missing
        self.unexpected = unexpected


class MissingRequiredLCMSValues(Exception):
    def __init__(self, header_rownums_dict):
        head_rows_str = ""
        cs = ", "
        for header in header_rownums_dict.keys():
            head_rows_str += f"\n\t{header}: {cs.join(header_rownums_dict[header])}"
        message = f"The following required values are missing on the indicated rows:\n{head_rows_str}"
        super().__init__(message)
        self.header_rownums_dict = header_rownums_dict
