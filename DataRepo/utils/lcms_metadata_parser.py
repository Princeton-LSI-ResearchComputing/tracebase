import os
from collections import defaultdict
from datetime import timedelta
from zipfile import BadZipFile

import pandas as pd
from openpyxl.utils.exceptions import InvalidFileException

from DataRepo.models.lc_method import LCMethod
from DataRepo.utils.exceptions import (
    DupeAnnotSampleHeaders,
    DuplicateSampleDataHeaders,
    InvalidLCMSHeaders,
    LCMSAnnotFileConflicts,
    MissingLCMSSampleDataHeaders,
    MissingPeakAnnotationFiles,
    MissingRequiredLCMSValues,
)

LCMS_HEADERS = (
    "tracebase sample name",  # Required
    "sample data header",  # Required
    "mzxml filename",
    "peak annotation filename",
    "instrument",
    "operator",
    "date",
    "lc method",
    "lc run length",
    "lc description",
)


def lcms_df_to_dict(
    df, headers=None, annot_filename=None, all_samples=None, aes=None, missing_defaults=None
):
    """
    Parse the LCMS dataframe created from an LCMS file, and key by sample data header (which must be unique).
    Takes an optional aggregated errors object.
    """
    lcms_metadata = {}
    lcms_samples = []
    dupes = {}
    missing_reqd_vals = defaultdict(list)

    if df is None:
        return lcms_metadata
    if missing_defaults is None:
        missing_defaults = []

    for idx, row in df.iterrows():
        # Convert empty strings to None
        for key in row.keys():
            if row[key] is not None and (
                str(row[key]) == "nan"
                or (type(row[key]) == str and row[key].strip() == "")
            ):
                row[key] = None

        # Get the sample name from the LCMS dataframe
        sample_name = row["tracebase sample name"]
        if sample_name is None:
            missing_reqd_vals["tracebase sample name"].append(idx + 2)
        elif sample_name not in lcms_samples:
            lcms_samples.append(sample_name)

        # Get the sample header from the LCMS dataframe
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

        # Get the LCMethod name from the LCMS dataframe
        lc_name = None
        if row["lc method"] is not None and row["lc run length"] is not None:
            lc_name = LCMethod.create_name(row["lc method"], row["lc run length"])

        # Get the peak annotation file from the LCMS dataframe
        peak_annot = None
        if row["peak annotation filename"] is not None:
            peak_annot = os.path.basename(row["peak annotation filename"]).strip()

        # Get the LCMethod run length from the LCMS dataframe
        run_len = None
        if row["lc run length"] is not None:
            run_len = timedelta(minutes=int(row["lc run length"]))

        lcms_metadata[sample_header] = create_lcms_metadata_value(
            {
                "sample_header": sample_header,
                "sample_name": sample_name,
                "peak_annot_file": peak_annot,
                "mzxml": row["mzxml filename"],
                "researcher": row["operator"],
                "instrument": row["instrument"],
                "date": row["date"],
                "lc_protocol_name": lc_name,
                "lc_type": row["lc method"],
                "lc_run_length": run_len,
                "lc_description": row["lc description"],
                "row_num": str(idx + 2),  # From 1, not including header row
            }
        )

    # If annot_filename is none, it's assumed this method is being called from the sample table loader
    if annot_filename is not None and headers is not None:
        # Now let's fill in defaults that are not explicitly set
        added = {}
        annot_dupes = {}
        missing_headers = []
        mismatched_annots = {}
        for annot_header in headers:
            if annot_header not in lcms_metadata.keys():
                if all_samples is not None and annot_header in all_samples:
                    lcms_metadata[annot_header] = create_lcms_metadata_value(
                        {
                            "sample_header": annot_header,
                            "sample_name": annot_header,
                            "peak_annot_file": annot_filename,
                        }
                    )
                    added[annot_header] = True
                elif annot_header not in missing_headers:
                    missing_headers.append(annot_header)
            elif all_samples is not None and annot_header in added.keys():
                if annot_header not in annot_dupes.keys():
                    annot_dupes[annot_header] = 2
                else:
                    annot_dupes[annot_header] += 1
            elif all_samples is not None and annot_header in lcms_metadata.keys():
                # Fill in annot file or look for discrepancy
                if lcms_metadata[annot_header]["peak_annot_file"] is None:
                    lcms_metadata[annot_header]["peak_annot_file"] = annot_filename
                elif lcms_metadata[annot_header]["peak_annot_file"] != annot_filename:
                    mismatched_annots[annot_header] = lcms_metadata[annot_header][
                        "peak_annot_file"
                    ]
            elif all_samples is None and annot_header not in lcms_metadata.keys():
                if annot_header not in missing_headers:
                    missing_headers.append(annot_header)

        if len(annot_dupes.keys()) > 0:
            # These are duplicate sample headers from the peak annotation file
            exc = DupeAnnotSampleHeaders(annot_dupes, annot_filename)
            if aes is not None:
                aes.buffer_error(exc)
            else:
                raise exc

        if len(missing_headers) > 0:
            # These are sample headers from the peak annotation file that are missing from both the sample table
            # (all_samples is derived from the sample table) and the LCMS metadata (derived from the LCMS metadata file)
            exc = MissingLCMSSampleDataHeaders(
                missing_headers,
                annot_filename,
                missing_defaults,
                all_samples is not None,
            )
            if aes is not None:
                aes.buffer_error(exc)
            else:
                raise exc

        if len(mismatched_annots.keys()) > 0:
            # These are peak annotation files from the LCMS metadata (file) that do not match the peak annotation file
            # that is being loaded, but the header from that file and the header from the LCMS metadata file match.
            # (Note, they should be unique for the whole study.)
            exc = LCMSAnnotFileConflicts(mismatched_annots, annot_filename)
            if aes is not None:
                aes.buffer_error(exc)
            else:
                raise exc
    elif annot_filename is not None and headers is None:
        exc = ValueError("The headers argument is required if annot_filename is supplied.")
        if aes is not None:
            aes.buffer_error(exc)
        else:
            raise exc

    if len(dupes.keys()) > 0:
        exc = DuplicateSampleDataHeaders(dupes, lcms_metadata, lcms_samples)
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


def create_lcms_metadata_value(partial_dict):
    """
    Creates a full dict value for a sample header key from a partially populated dict
    """
    if (
        "sample_header" not in partial_dict.keys()
        or partial_dict["sample_header"] is None
        or "sample_name" not in partial_dict.keys()
        or partial_dict["sample_name"] is None
    ):
        raise ValueError("Both sample_name and sample_header values are required.")

    full_dict = partial_dict
    keys = [
        "sample_header",
        "sample_name",
        "peak_annot_file",
        "mzxml",
        "researcher",
        "instrument",
        "date",
        "lc_protocol_name",
        "lc_type",
        "lc_run_length",
        "lc_description",
        "row_num",  # TODO: Make sure this being None is OK
    ]

    for key in keys:
        if key not in partial_dict.keys():
            full_dict[key] = None

    return full_dict


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


def get_lcms_metadata_dict_from_file(lcms_file, aes=None):
    try:
        lcms_metadata_df = extract_dataframes_from_lcms_xlsx(lcms_file)
    except (InvalidFileException, ValueError, BadZipFile):  # type: ignore
        lcms_metadata_df = extract_dataframes_from_lcms_tsv(lcms_file)
    return lcms_df_to_dict(lcms_metadata_df, aes)


def check_peak_annotation_files(
    annot_files_from_study, lcms_metadata_dict=None, lcms_file=None, aes=None
):
    """
    Check that all peak annotation files explicitly listed in the LCMS metadata are in the supplied peak annot file
    list.  This is intended to be used by load_study to ensure that a user remembered to supply all files and that they
    didn't get any file names wrong.

    Note, since annot_files_from_study represents files listing in the loading.yaml file, this function does not check
    that peak annotation files supplied in the study directory are in the study yaml file.
    """
    # Initialize lcms_metadata
    lcms_metadata = {}
    if lcms_metadata_dict is None and lcms_file is None:
        # This is a coding error, so buffering is not necessary.  If it's coded correctly, this will never happen
        raise ValueError("Either an lcms_metadata_dict or an lcms_file are required")
    elif lcms_metadata_dict is None:
        lcms_metadata = get_lcms_metadata_dict_from_file(lcms_file, aes)
    else:
        lcms_metadata = lcms_metadata_dict

    peak_annot_files_from_lcms = []
    missing_peak_annot_files = []

    # Obtain a unique set of peak annotation file names
    for sample_data_header in lcms_metadata.keys():
        if (
            lcms_metadata[sample_data_header]["peak_annot_file"] is not None
            and lcms_metadata[sample_data_header]["peak_annot_file"].strip() != ""
            and lcms_metadata[sample_data_header]["peak_annot_file"].strip()
            not in peak_annot_files_from_lcms
        ):
            peak_annot_files_from_lcms.append(
                lcms_metadata[sample_data_header]["peak_annot_file"].strip()
            )
            if (
                lcms_metadata[sample_data_header]["peak_annot_file"].strip()
                not in annot_files_from_study
            ):
                missing_peak_annot_files.append(
                    lcms_metadata[sample_data_header]["peak_annot_file"].strip()
                )

    extra_peak_annot_files = []

    # Check for missing files
    for annot_file_from_study in annot_files_from_study:
        if annot_file_from_study not in peak_annot_files_from_lcms:
            extra_peak_annot_files.append(
                lcms_metadata[sample_data_header]["peak_annot_file"].strip()
            )

    lcms_file_basename = os.path.basename(lcms_file)
    if len(missing_peak_annot_files) > 0:
        exc = MissingPeakAnnotationFiles(
            missing_peak_annot_files, extra_peak_annot_files, lcms_file_basename
        )
        if aes is not None:
            aes.buffer_error(exc)
        else:
            raise exc


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
