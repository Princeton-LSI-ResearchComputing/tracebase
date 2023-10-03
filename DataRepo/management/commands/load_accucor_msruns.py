import argparse
import os.path
from zipfile import BadZipFile

import pandas as pd
from django.core.exceptions import ValidationError
from django.core.management import BaseCommand
from openpyxl.utils.exceptions import InvalidFileException

from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.utils import (
    AccuCorDataLoader,
    InvalidLCMSHeaders,
    LCMSHeadersAreValid,
)


class Command(BaseCommand):
    help = (
        "Loads data from an Accucor or Isocorr excel file (e.g. a workbook containing corrected (and optional "
        "original) data worksheets), or a csv file exported from only the corrected worksheet into tracebase"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--accucor-file",
            type=str,
            help="Filepath of either an Accucor xlsx output, an Accucor csv export of the corrected data worksheet, "
            "or (with --isocorr-format) an Isocorr corrected data csv output.",
            required=True,
        )
        parser.add_argument(
            "--isocorr-format",
            required=False,
            action="store_true",
            default=False,
            help="Supply this flag if the file supplied to --accucor-file is an Isocorr csv format file.",
        )
        parser.add_argument(
            "--lcms-file",
            type=str,
            help=(
                "Filepath of either an xlsx or csv file containing metadata associated with the liquid chromatography "
                "and mass spec instrument run."
            ),
            default=None,
            required=False,
        )
        parser.add_argument(
            "--lc-protocol-name",
            type=str,
            help="Default LCMethod.name of the liquid chromatography protocol used",
            default=None,
            required=False,
        )
        parser.add_argument(
            "--ms-protocol-name",
            type=str,
            help="Default name of the mass spectrometry protocol used",
            default=None,
            required=False,
        )
        parser.add_argument(
            "--instrument",
            type=str,
            help="Name of the LCMS instrument that analyzed the samples",
            default=None,
            required=False,
        )
        parser.add_argument(
            "--date",
            type=str,
            help="Date MSRun was performed, formatted as YYYY-MM-DD",
            default=None,
            required=False,
        )
        parser.add_argument(
            "--researcher",
            type=str,
            help="Database name or ID of the researcher",
            default=None,
            required=False,
        )
        parser.add_argument(
            "--skip-samples",
            type=str,
            nargs="+",
            help="List of sample names to skip loading (useful for blank samples)",
            required=False,
        )
        parser.add_argument(
            "--sample-name-prefix",
            type=str,
            help="Sample name prefix",
            default=None,
            required=False,
        )
        # optional dry run argument
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Dry run mode. Will not change the database.",
        )
        # optional new researcher argument (circumvents existing researcher check)
        parser.add_argument(
            "--new-researcher",
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )
        # Used internally by the DataValidationView
        parser.add_argument(
            "--validate",
            required=False,
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )
        # Used internally by the DataValidationView to avoid referencing randomized temporary accucor file names
        parser.add_argument(
            "--accucor-file-name",
            type=str,
            help=argparse.SUPPRESS,
            default=None,
        )

    @MaintainedModel.defer_autoupdates(
        disable_opt_names=["validate", "dry_run"],
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def handle(self, *args, **options):
        lcms_metadata = None
        if options["lcms_file"] is not None:
            try:
                lcms_metadata = self.extract_dataframes_from_lcms_xlsx(
                    options["lcms_file"]
                )
            except (InvalidFileException, ValueError, BadZipFile):
                lcms_metadata = self.extract_dataframes_from_lcms_csv(
                    options["lcms_file"]
                )

        fmt = "Isocorr" if options["isocorr_format"] else "Accucor"
        print(f"Reading {fmt} file: {options['accucor_file']}")
        print(f"LOADING WITH PREFIX: {options['sample_name_prefix']}")

        try:
            self.extract_dataframes_from_peakannotation_xlsx(
                options["isocorr_format"], options["accucor_file"]
            )
        except (InvalidFileException, ValueError, BadZipFile):
            self.extract_dataframes_from_peakannotation_csv(options["accucor_file"])

        # we'll use the basename to group the PeakGroupSets generated by this file
        pgs_filename = os.path.basename(options["accucor_file"]).strip()
        if options["accucor_file_name"] is not None:
            pgs_filename = options["accucor_file_name"]

        loader = AccuCorDataLoader(
            # Peak annotation file data
            isocorr_format=options["isocorr_format"],
            accucor_original_df=self.original,
            accucor_corrected_df=self.corrected,
            peak_group_set_filename=pgs_filename,
            # LCMS metadata
            lcms_metadata=lcms_metadata,
            # LCMS batch defaults
            date=options["date"],
            lc_protocol_name=options["lc_protocol_name"],
            ms_protocol_name=options["ms_protocol_name"],
            researcher=options["researcher"],
            instrument=options["instrument"],
            # Sample options
            skip_samples=options["skip_samples"],
            sample_name_prefix=options["sample_name_prefix"],
            # Modes
            allow_new_researchers=options["new_researcher"],
            validate=options["validate"],
            verbosity=options["verbosity"],
            dry_run=options["dry_run"],
        )

        loader.load_accucor_data()

        print(f"Done loading {fmt} data into MsRun, PeakGroups, and PeakData")

    def extract_dataframes_from_lcms_xlsx(self, lcms_file):
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

        if not LCMSHeadersAreValid(headers):
            raise InvalidLCMSHeaders(headers, lcms_file)

        return pd.read_excel(
            lcms_file,
            sheet_name=0,  # The first sheet
            engine="openpyxl",
        ).dropna(axis=0, how="all")

    def extract_dataframes_from_lcms_csv(self, lcms_file):
        headers = (
            pd.read_csv(
                lcms_file,
                nrows=1,
                header=None,
            )
            .squeeze("columns")
            .iloc[0]
        )

        if not LCMSHeadersAreValid(headers):
            raise InvalidLCMSHeaders(headers, lcms_file)

        return pd.read_csv(lcms_file).dropna(axis=0, how="all")

    def extract_dataframes_from_peakannotation_xlsx(self, is_isocorr, peak_annot_file):
        # For checking the sheets
        sheet_names = pd.ExcelFile(peak_annot_file, engine="openpyxl").sheet_names
        if is_isocorr:
            if sheet_names[1] != "absolte":
                raise WrongExcelSheet("Isocorr", sheet_names[1], "absolte", 2)

            self.original = None
        else:
            if sheet_names[0] != "Original":
                raise WrongExcelSheet("Accucor", sheet_names[0], "Original", 1)
            if sheet_names[1] != "Corrected":
                raise WrongExcelSheet("Accucor", sheet_names[1], "Corrected", 2)

            # Note, setting `mangle_dupe_cols=False` would overwrite duplicates instead of raise an exception, so we're
            # checking for duplicate headers manually here.
            orig_heads = (
                pd.read_excel(
                    peak_annot_file,
                    nrows=1,  # Read only the first row
                    header=None,
                    sheet_name=0,  # The first sheet
                    engine="openpyxl",
                )
                .squeeze("columns")
                .iloc[0]
            )

            if self.headers_are_not_unique(orig_heads):
                raise ValidationError(
                    f"Column headers in Original data sheet are not unique. There are {self.num_heads} columns and "
                    f"{self.num_uniq_heads} unique values"
                )

            # get the first 2 sheets as the original and corrected data
            self.original = pd.read_excel(
                peak_annot_file,
                sheet_name=0,  # The first sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all")

        corr_heads = (
            pd.read_excel(
                peak_annot_file,
                nrows=1,  # Read only the first row
                header=None,
                sheet_name=1,  # The second sheet
                engine="openpyxl",
            )
            .squeeze("columns")
            .iloc[0]
        )

        if self.headers_are_not_unique(corr_heads):
            raise ValidationError(
                f"Column headers in Corrected data sheet are not unique. There are {self.num_heads} columns and "
                f"{self.num_uniq_heads} unique values"
            )

        self.corrected = pd.read_excel(
            peak_annot_file,
            sheet_name=1,  # The second sheet
            engine="openpyxl",
        ).dropna(axis=0, how="all")

    def extract_dataframes_from_peakannotation_csv(self, peak_annot_file):
        corr_heads = (
            pd.read_csv(
                peak_annot_file,
                nrows=1,
                header=None,
            )
            .squeeze("columns")
            .iloc[0]
        )

        if self.headers_are_not_unique(corr_heads):
            raise ValidationError(
                f"Column headers in Corrected data sheet are not unique. There are {self.num_heads} columns and "
                f"{self.num_uniq_heads} unique values"
            )

        self.original = None
        self.corrected = pd.read_csv(peak_annot_file).dropna(axis=0, how="all")

    def headers_are_not_unique(self, headers):
        self.num_uniq_heads = len(pd.unique(headers))
        self.num_heads = len(headers)
        if self.num_uniq_heads != self.num_heads:
            return True
        return False


class WrongExcelSheet(Exception):
    def __init__(self, file_type, sheet_name, expected_sheet_name, sheet_num):
        message = (
            f"Expected [{file_type}] Excel sheet [{sheet_num}] to be named [{expected_sheet_name}], but got "
            f"[{sheet_name}]."
        )
        super().__init__(message)
