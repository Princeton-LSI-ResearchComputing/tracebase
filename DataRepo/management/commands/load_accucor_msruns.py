import pandas as pd
from django.core.management import BaseCommand

from DataRepo.utils import AccuCorDataLoader


class Command(BaseCommand):
    # Show this when the user types help
    help = (
        "Loads data from an accucor file table into"
        "Protocol, MsRun, PeakGroup, and PeakData database tables"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--accucor_filename", type=str, help="Accucor data xlxs output filepath"
        )
        parser.add_argument(
            "--protocol", type=str, help="protocol database identifier or name"
        )
        parser.add_argument("--date", type=str, help="MSRun date performed")
        parser.add_argument(
            "--researcher", type=str, help="Name or identifier of the researcher"
        )
        # optional debug argument
        parser.add_argument(
            "--debug",
            action="store_true",
            default=False,
            help='Raise a "debug-only" error, to abort transaction',
        )

    def handle(self, *args, **options):
        print("Reading accucor file(s)")
        # get the first 2 sheets as the original and corrected data
        original = pd.read_excel(options["accucor_filename"], sheet_name=0)
        corrected = pd.read_excel(options["accucor_filename"], sheet_name=1)

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
