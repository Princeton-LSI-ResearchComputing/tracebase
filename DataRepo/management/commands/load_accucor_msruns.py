import os.path

import pandas as pd
from django.core.exceptions import ValidationError
from django.core.management import BaseCommand

from DataRepo.utils import AccuCorDataLoader


class Command(BaseCommand):
    # Show this when the user supplies -h
    help = (
        "Loads data from an accucor excel file into"
        "Protocol, MsRun, PeakGroup, and PeakData database tables"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--accucor-file",
            type=str,
            help="Accucor data xlxs output filepath",
            required=True,
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
            help="Date MSRun was performed",
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
        # optional debug argument
        parser.add_argument(
            "--debug",
            action="store_true",
            default=False,
            # This issues a "debug-only" error, to abort the transaction
            help="Debug mode. Will not change the database.",
        )

    def handle(self, *args, **options):
        print("Reading accucor file: " + options["accucor_file"])

        # Note, setting `mangle_dupe_cols=False` results in `Setting mangle_dupe_cols=False is not supported yet`, so
        # the following is to catch duplicate headers
        orig_heads = pd.read_excel(
            options["accucor_file"],
            nrows=1,
            header=None,
            sheet_name=0,
            engine="openpyxl",
            squeeze=True,
        ).iloc[0]
        num_uniq_orig_heads = len(pd.unique(orig_heads))
        num_orig_heads = len(orig_heads)
        if num_uniq_orig_heads != num_orig_heads:
            raise ValidationError(
                f"Column headers in Original data sheet are not unique. There are {num_orig_heads} columns and "
                f"{num_uniq_orig_heads} unique values"
            )

        corr_heads = pd.read_excel(
            options["accucor_file"],
            nrows=1,
            header=None,
            sheet_name=1,
            engine="openpyxl",
            squeeze=True,
        ).iloc[0]
        num_uniq_corr_heads = len(pd.unique(corr_heads))
        num_corr_heads = len(corr_heads)
        if num_uniq_corr_heads != num_corr_heads:
            raise ValidationError(
                "Column headers in Corrected data sheet are not unique. There are "
                + str(num_corr_heads)
                + " columns and "
                + str(num_uniq_corr_heads)
                + " unique values"
            )

        # get the first 2 sheets as the original and corrected data
        original = pd.read_excel(
            options["accucor_file"], sheet_name=0, engine="openpyxl"
        ).dropna(axis=0, how="all")

        corrected = pd.read_excel(
            options["accucor_file"], sheet_name=1, engine="openpyxl"
        ).dropna(axis=0, how="all")

        # we'll use the basename to group the PeakGroupSets generated by this file
        pgs_filename = os.path.basename(options["accucor_file"])

        loader = AccuCorDataLoader(
            accucor_original_df=original,
            accucor_corrected_df=corrected,
            date=options["date"],
            protocol_input=options["protocol"],
            researcher=options["researcher"],
            peak_group_set_filename=pgs_filename,
            skip_samples=options["skip_samples"],
            debug=options["debug"],
        )

        loader.load_accucor_data()

        print("Done loading Accucor data into MsRun, PeakGroups, and PeakData")
