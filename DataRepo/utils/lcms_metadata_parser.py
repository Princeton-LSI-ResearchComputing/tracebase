import os

import pandas as pd
from django.core.exceptions import ValidationError

from DataRepo.models.lc_method import LCMethod

LCMS_HEADERS = (
    "tracebase sample name",
    "sample data header",
    "peak annotation filename",
    "mzxml filename",
    "ms mode",
    "instrument",
    "operator",
    "date",
    "lc method",
    "lc run length",
    "lc description",
)


def lcms_df_to_dict(df):
    """
    Parse the LCMS dataframe created from an LCMS file, and key by sample data header (which must be unique)
    """
    lcms_metadata = {}
    samples = []
    dupes = {}
    if df is None:
        return lcms_metadata
    for idx, row in df.iterrows():
        sample_header = row["sample data header"]
        sample_name = row["tracebase sample name"]
        if sample_name not in samples:
            samples.append(sample_name)
        if sample_header in lcms_metadata.keys():
            if sample_header in dupes.keys():
                dupes[sample_header].append(idx)
            else:
                dupes[sample_header] = [
                    lcms_metadata[sample_header]["row_num"],
                    idx + 2,
                ]
            continue
        lcms_metadata[sample_header] = {
            "sample_header": sample_header,
            "sample_name": sample_name,
            "peak_annotation": os.path.basename(
                row["peak annotation filename"]
            ).strip(),
            "mzxml": row["mzxml filename"],
            "ms_protocol_name": row["ms mode"],
            "researcher": row["instrument"],
            "instrument": row["operator"],
            "date": row["date"],
            "lc_type": row["lc method"],
            "lc_run_length": row["lc run length"],
            "lc_description": row["lc description"],
            "lc_name": LCMethod.create_name(row["lc method"], row["lc run length"]),
            "row_num": idx + 2,  # From 1, not including header row
        }

    if len(dupes.keys()) > 0:
        raise DuplicateSampleDataHeaders(dupes, lcms_metadata, samples)

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
            sheet_name=1,  # The second sheet
            engine="openpyxl",
        )
        .squeeze("columns")
        .iloc[0]
    )

    if not lcms_headers_are_valid(headers):
        raise InvalidLCMSHeaders(headers, lcms_file)

    return pd.read_excel(
        lcms_file,
        sheet_name=0,  # The first sheet
        engine="openpyxl",
    ).dropna(axis=0, how="all")


def extract_dataframes_from_lcms_csv(lcms_file):
    headers = (
        pd.read_csv(
            lcms_file,
            nrows=1,
            header=None,
        )
        .squeeze("columns")
        .iloc[0]
    )

    if not lcms_headers_are_valid(headers):
        raise InvalidLCMSHeaders(headers, lcms_file)

    return pd.read_csv(lcms_file).dropna(axis=0, how="all")


def lcms_headers_are_valid(headers):
    return headers == LCMS_HEADERS


class DuplicateSampleDataHeaders(Exception):
    def __init__(self, dupes, lcms_metadata, samples):
        cs = ", "
        dupes_str = "\n\t".join(
            [k + f" rows: [{cs.join(dupes[k])}]" for k in dupes.keys()]
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
        if len(headers) != len(expected_headers):
            message += (
                f"should have {len(expected_headers)} headers, but {len(headers)} were found.  Expected headers: "
                f"[{expected_headers}]."
            )
        else:
            missing = []
            unexpected = []
            for i in range(len(headers)):
                if headers[i] not in expected_headers:
                    unexpected.append(headers[i])
                if expected_headers[i] not in headers:
                    missing.append(expected_headers[i])
            if len(missing) > 0:
                message += f"is missing headers: [{missing}]"
            if len(missing) > 0 and len(unexpected) > 0:
                message += " and "
            if len(unexpected) > 0:
                message += f" has unexpected headers: [{unexpected}]"
        super().__init__(message)
        self.headers = headers
        self.expected_headers = expected_headers
        self.lcms_file = lcms_file
