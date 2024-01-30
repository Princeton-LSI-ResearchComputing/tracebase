import argparse

from django.core.management import BaseCommand

from DataRepo.models.compound import Compound, CompoundSynonym
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
            "--table-headers",
            type=str,
            help=f"YAML file defining headers to be used.",
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
        msg = (
            "Done.\n"
            "Compound records loaded: [%i], skipped: [%i], and errored: [%i].\n"
            "CompoundSynonym records loaded: [%i], skipped: [%i], and errored: [%i]."
        )
        saved_aes = None

        try:
            sheet = options["sheet"] if is_excel(options["compounds"]) else None
            self.compounds_df = read_from_file(options["compounds"], sheet=sheet)
            custom_header_data = (
                read_from_file(options["table_headers"]) if options["table_headers"]
                else None
            )

            # Initialize loader class
            loader = CompoundsLoader(
                compounds_df=self.compounds_df,
                headers=custom_header_data,
                synonym_separator=options["synonym_separator"],
                dry_run=options["dry_run"],
                defer_rollback=options["defer_rollback"],
                sheet=sheet,
                file=options["compounds"],
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

        load_stats = loader.get_load_stats()
        status = msg % (
            load_stats[Compound.__name__]["created"],
            load_stats[Compound.__name__]["skipped"],
            load_stats[Compound.__name__]["errored"],
            load_stats[CompoundSynonym.__name__]["created"],
            load_stats[CompoundSynonym.__name__]["skipped"],
            load_stats[CompoundSynonym.__name__]["errored"],
        )

        if saved_aes is not None and saved_aes.get_num_errors() > 0:
            status_msg = self.style.ERROR(status)
        elif saved_aes is not None and saved_aes.get_num_warnings() > 0:
            status_msg = self.style.WARNING(status)
        else:
            status_msg = self.style.SUCCESS(status)

        if options["verbosity"] > 0:
            self.stdout.write(status_msg)

        if saved_aes is not None and saved_aes.should_raise():
            saved_aes.print_summary()
            raise saved_aes
