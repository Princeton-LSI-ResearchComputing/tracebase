import argparse

from django.core.management import BaseCommand

from DataRepo.utils import (
    AggregatedErrors,
    CompoundsLoader,
    DryRun,
    is_excel,
    read_from_file,
)


class Command(BaseCommand):
    help = "Loads data from a compound table into the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--compounds",
            type=str,
            help=(
                "Path to either an excel file containing a sheet named 'Compounds' or a tab-delimited file, containing "
                "headers: 'Compound', 'Formula', 'HMDB ID', and 'Synonyms'; required."
            ),
            required=True,
        )

        parser.add_argument(
            "--sheet",
            type=str,
            help="Name of excel sheet/tab.  Only used if --compounds is an excel spreadsheet.  Default: 'Compounds'.",
            default="Compounds",
        )

        parser.add_argument(
            "--synonym-separator",
            type=str,
            help="Character separating multiple synonyms in 'Synonyms' column (default '%(default)s')",
            default=";",
            required=False,
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help=(
                "Dry Run mode. If specified, command will not change the database, "
                "but simply report back potential work or issues."
            ),
        )

        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY.  A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        saved_aes = None
        try:
            sheet = options["sheet"] if is_excel(options["compounds"]) else None
            self.compounds_df = read_from_file(options["compounds"], sheet=sheet)

            # Initialize loader class
            loader = CompoundsLoader(
                compounds_df=self.compounds_df,
                synonym_separator=options["synonym_separator"],
                dry_run=options["dry_run"],
                defer_rollback=options["defer_rollback"],
            )

            loader.load_compound_data()
        except DryRun:
            pass
        except AggregatedErrors as aes:
            saved_aes = aes
        except Exception as e:
            # Add this unanticipated error to a new aggregated errors object
            saved_aes = AggregatedErrors()
            saved_aes.buffer_error(e)

        if saved_aes is not None and saved_aes.should_raise():
            saved_aes.print_summary()
            raise saved_aes
