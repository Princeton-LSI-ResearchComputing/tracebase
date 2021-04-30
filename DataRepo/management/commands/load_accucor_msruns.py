import pandas as pd
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
            "--accucor_filename",
            type=str,
            help="Accucor data xlxs output filepath",
            required=True,
        )
        parser.add_argument(
            "--protocol",
            type=str,
            help="protocol database identifier or name",
            required=True,
        )
        parser.add_argument(
            "--date", type=str, help="MSRun date performed", required=True
        )
        parser.add_argument(
            "--researcher",
            type=str,
            help="Name or identifier of the researcher",
            required=True,
        )
        # optional debug argument
        parser.add_argument(
            "--debug",
            action="store_true",
            default=False,
            # This issues a "debug-only" error, to abort the transaction
            help='Debug mode. Will not change the database.',
        )

    def handle(self, *args, **options):
        print("Reading accucor file: " + options["accucor_filename"])
        # get the first 2 sheets as the original and corrected data
        original = pd.read_excel(
            options["accucor_filename"], sheet_name=0, engine="openpyxl"
        ).dropna(axis=0, how="all")
        corrected = pd.read_excel(
            options["accucor_filename"], sheet_name=1, engine="openpyxl"
        ).dropna(axis=0, how="all")

        loader = AccuCorDataLoader(
            accucor_original_df=original,
            accucor_corrected_df=corrected,
            date=options["date"],
            protocol_input=options["protocol"],
            researcher=options["researcher"],
            debug=options["debug"],
        )

        loader.load_accucor_data()

        print("Done loading Accucor data into MsRun, PeakGroups, and PeakData")
