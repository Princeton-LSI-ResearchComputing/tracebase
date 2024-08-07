import os
from collections import defaultdict
from datetime import timedelta

from DataRepo.models.lc_method import LCMethod
from DataRepo.utils.exceptions import (
    DuplicateSampleDataHeaders,
    InvalidHeaders,
    InvalidLCMSHeaders,
    MissingPeakAnnotationFiles,
    MissingRequiredLCMSValues,
)
from DataRepo.utils.file_utils import headers_are_as_expected, read_from_file

LCMS_DB_SAMPLE_HDR = "tracebase sample name"
LCMS_FL_SAMPLE_HDR = "sample data header"
LCMS_MZXML_HDR = "mzxml filename"
LCMS_PEAK_ANNOT_HDR = "peak annotation filename"
LCMS_INSTRUMENT_HDR = "instrument"
LCMS_OPERATOR_HDR = "operator"
LCMS_DATE_HDR = "date"
LCMS_LCMETHOD_HDR = "lc method"
LCMS_RUNLEN_HDR = "lc run length"
LCMS_LCDESC_HDR = "lc description"

LCMS_HEADERS = (
    LCMS_DB_SAMPLE_HDR,
    LCMS_FL_SAMPLE_HDR,
    LCMS_MZXML_HDR,
    LCMS_PEAK_ANNOT_HDR,
    LCMS_INSTRUMENT_HDR,
    LCMS_OPERATOR_HDR,
    LCMS_DATE_HDR,
    LCMS_LCMETHOD_HDR,
    LCMS_RUNLEN_HDR,
    LCMS_LCDESC_HDR,
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
            if row[key] is not None and (
                str(row[key]) == "nan"
                or (isinstance(row[key], str) and row[key].strip() == "")
            ):
                row[key] = None

        sample_name = row[LCMS_DB_SAMPLE_HDR]
        if sample_name is None:
            missing_reqd_vals[LCMS_DB_SAMPLE_HDR].append(idx + 2)
        elif sample_name not in samples:
            samples.append(sample_name)

        sample_header = row[LCMS_FL_SAMPLE_HDR]
        if sample_header is None:
            missing_reqd_vals[LCMS_FL_SAMPLE_HDR].append(idx + 2)
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
        if row[LCMS_LCMETHOD_HDR] is not None and row[LCMS_RUNLEN_HDR] is not None:
            lc_name = LCMethod.create_name(row[LCMS_LCMETHOD_HDR], row[LCMS_RUNLEN_HDR])

        peak_annot = None
        if row[LCMS_PEAK_ANNOT_HDR] is not None:
            peak_annot = os.path.basename(row[LCMS_PEAK_ANNOT_HDR]).strip()

        run_len = None
        if row[LCMS_RUNLEN_HDR] is not None:
            run_len = timedelta(minutes=int(row[LCMS_RUNLEN_HDR]))

        lcms_metadata[sample_header] = {
            "sample_header": sample_header,
            "sample_name": sample_name,
            "peak_annot_file": peak_annot,
            "mzxml": row[LCMS_MZXML_HDR],
            "researcher": row[LCMS_OPERATOR_HDR],
            "instrument": row[LCMS_INSTRUMENT_HDR],
            "date": row[LCMS_DATE_HDR],
            "lc_protocol_name": lc_name,
            "lc_type": row[LCMS_LCMETHOD_HDR],
            "lc_run_length": run_len,
            "lc_description": row[LCMS_LCDESC_HDR],
            "polarity": None,
            "mz_min": None,
            "mz_max": None,
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


def get_lcms_metadata_dict_from_file(lcms_file, aes=None):
    lcms_metadata_df = read_from_file(
        lcms_file,
        sheet="LCMS Metadata",
        expected_headers=LCMS_HEADERS,
    )
    return lcms_df_to_dict(lcms_metadata_df, aes)


def read_lcms_metadata_from_file(lcms_file):
    try:
        return read_from_file(
            lcms_file,
            sheet="LCMS Metadata",
            expected_headers=LCMS_HEADERS,
        )
    except InvalidHeaders as ih:
        raise InvalidLCMSHeaders(
            headers=ih.headers, expected_headers=ih.expected_headers, file=ih.file
        )


def check_peak_annotation_files(
    annot_files_from_study, lcms_metadata_dict=None, lcms_file=None, aes=None
):
    """
    Check that all peak annotation files explicitly listed in the LCMS metadata are in the supplied peak annot file
    list.  This is intended to be used by legacy_load_study to ensure that a user remembered to supply all files and
    that they didn't get any file names wrong.

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


def lcms_headers_are_valid(headers):
    return headers_are_as_expected(LCMS_HEADERS, headers)
