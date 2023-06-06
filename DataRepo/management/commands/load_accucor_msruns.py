import argparse
import os.path
from zipfile import BadZipFile

import pandas as pd
from django.core.exceptions import ValidationError
from django.core.management import BaseCommand
from openpyxl.utils.exceptions import InvalidFileException

from DataRepo.utils import AccuCorDataLoader


class Command(BaseCommand):
    # Show this when the user supplies -h
    help = (
        "Loads data from an Accucor excel file "
        "(e.g. a workbook containing both original and corrected data worksheets), or "
        "a csv file exported from only the corrected worksheet "
        "(e.g. no original data available) "
        "into Protocol, MsRun, PeakGroupSet, PeakGroup, and PeakData database tables"
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
            "--protocol",
            type=str,
            help="Database name or ID of the protocol",
            required=True,
        )
        parser.add_argument(
            "--date",
            type=str,
            help="Date MSRun was performed, formatted as YYYY-MM-DD",
            required=True,
        )
        parser.add_argument(
            "--researcher",
            type=str,
            help="Database name or ID of the researcher",
            required=True,
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
        # Intended for use by load_study to prevent individual loader autoupdates and buffer clearing, then perform all
        # mass autoupdates/buffer-clearings after all load scripts are complete
        parser.add_argument(
            "--defer-autoupdates",
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        fmt = "Accucor"
        if options["isocorr_format"]:
            fmt = "Isocorr"
        print(f"Reading {fmt} file: {options['accucor_file']}")
        print(f"LOADING WITH PREFIX: {options['sample_name_prefix']}")

        try:
            self.extract_dataframes_from_accucor_xlsx(options)
        except (InvalidFileException, ValueError, BadZipFile):
            self.extract_dataframes_from_csv(options)

        # we'll use the basename to group the PeakGroupSets generated by this file
        pgs_filename = os.path.basename(options["accucor_file"]).strip()
        if options["accucor_file_name"] is not None:
            pgs_filename = options["accucor_file_name"]

        loader = AccuCorDataLoader(
            accucor_original_df=self.original,
            accucor_corrected_df=self.corrected,
            date=options["date"],
            protocol_input=options["protocol"],
            researcher=options["researcher"],
            peak_group_set_filename=pgs_filename,
            skip_samples=options["skip_samples"],
            sample_name_prefix=options["sample_name_prefix"],
            new_researcher=options["new_researcher"],
            validate=options["validate"],
            isocorr_format=options["isocorr_format"],
            verbosity=options["verbosity"],
            defer_autoupdates=options["defer_autoupdates"],
            dry_run=options["dry_run"],
        )

        loader.load_accucor_data()

        print(f"Done loading {fmt} data into MsRun, PeakGroups, and PeakData")

    def extract_dataframes_from_accucor_xlsx(self, options):
        if not options["isocorr_format"]:
            # Note, setting `mangle_dupe_cols=False` would overwrite duplicates instead of raise an exception, so we're
            # checking for duplicate headers manually here.
            orig_heads = pd.read_excel(
                options["accucor_file"],
                nrows=1,
                header=None,
                sheet_name=0,
                engine="openpyxl",
                squeeze=True,
            ).iloc[0]

            if self.headers_are_not_unique(orig_heads):
                raise ValidationError(
                    f"Column headers in Original data sheet are not unique. There are {self.num_heads} columns and "
                    f"{self.num_uniq_heads} unique values"
                )

            # get the first 2 sheets as the original and corrected data
            self.original = pd.read_excel(
                options["accucor_file"], sheet_name=0, engine="openpyxl"
            ).dropna(axis=0, how="all")
        else:
            self.original = None

        corr_heads = pd.read_excel(
            options["accucor_file"],
            nrows=1,
            header=None,
            sheet_name=1,
            engine="openpyxl",
            squeeze=True,
        ).iloc[0]

        if self.headers_are_not_unique(corr_heads):
            raise ValidationError(
                f"Column headers in Corrected data sheet are not unique. There are {self.num_heads} columns and "
                f"{self.num_uniq_heads} unique values"
            )

        self.corrected = pd.read_excel(
            options["accucor_file"], sheet_name=1, engine="openpyxl"
        ).dropna(axis=0, how="all")

    def extract_dataframes_from_csv(self, options):
        corr_heads = pd.read_csv(
            options["accucor_file"],
            nrows=1,
            header=None,
            squeeze=True,
        ).iloc[0]

        if self.headers_are_not_unique(corr_heads):
            raise ValidationError(
                f"Column headers in Corrected data sheet are not unique. There are {self.num_heads} columns and "
                f"{self.num_uniq_heads} unique values"
            )

        self.original = None
        self.corrected = pd.read_csv(options["accucor_file"]).dropna(axis=0, how="all")

    def headers_are_not_unique(self, headers):
        self.num_uniq_heads = len(pd.unique(headers))
        self.num_heads = len(headers)
        if self.num_uniq_heads != self.num_heads:
            return True
        return False
