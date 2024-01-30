import argparse
import os

from django.core.management import BaseCommand

from DataRepo.utils import (
    AggregatedErrors,
    DryRun,
    StudyTableLoader,
    is_excel,
    read_from_file,
)


class Command(BaseCommand):
    help = "Loads data from a file into the database."

    def __init__(self, loader_class, sheet_default):
        self.loader_class = loader_class
        self.sheet_default = sheet_default

    def add_arguments(self, parser):
        parser.add_argument(
            "--infile",
            type=str,
            help=(
                "Path to either a tab-delimited or excel file (with a sheet named 'Study').  "
                f"Default headers: {self.loader_class.get_pretty_default_headers()}.  See --headers."
            ),
            required=True,
        )

        parser.add_argument(
            "--sheet",
            type=str,
            help=f"Name of excel sheet/tab.  Only used if --infile is an excel spreadsheet.  Default: [{self.sheet_default}].",
            default=self.sheet_default,
        )

        parser.add_argument(
            "--headers",
            type=str,
            help=f"YAML file defining headers to be used.",
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="If supplied, nothing will be saved to the database.",
        )

        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY.  A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        saved_aes = None

        try:
            sheet = options["sheet"] if is_excel(options["infile"]) else None
            df = read_from_file(options["infile"], sheet=sheet)
            custom_header_data = (
                read_from_file(options["headers"]) if options["headers"]
                else None
            )

            loader = self.loader_class(
                # Data
                df,
                headers=custom_header_data,
                # Modes
                dry_run=options["dry_run"],
                defer_rollback=options["defer_rollback"],
                # Used for error-reporting
                sheet=sheet,
                file=os.path.basename(options["infile"]),
            )

            loader.load_study_table()

        except DryRun:
            pass
        except AggregatedErrors as aes:
            saved_aes = aes
        except Exception as e:
            # Add this error (which wasn't added to the aggregated errors, because it was unanticipated) to the
            # other buffered errors
            saved_aes = AggregatedErrors()
            saved_aes.buffer_error(e)

        msg = "Dry-run complete.  The following would occur during a real load:\n" if options["dry_run"] else "Done.\n"
        load_stats = loader.get_load_stats()
        for mdl in self.loader_class.models:
            mdl_name = mdl.__name__
            if mdl_name in load_stats.keys():
                msg += "%s records loaded: [%i], skipped: [%i], and errored: [%i]." % (
                    mdl_name,
                    load_stats[mdl_name]["created"],
                    load_stats[mdl_name]["skipped"],
                    load_stats[mdl_name]["errored"],
                )

        if saved_aes is not None and saved_aes.get_num_errors() > 0:
            status = self.style.ERROR(msg)
        elif saved_aes is not None and saved_aes.get_num_warnings() > 0:
            status = self.style.WARNING(msg)
        else:
            status = self.style.SUCCESS(msg)

        if options["verbosity"] > 0:
            self.stdout.write(status)

        if saved_aes is not None and saved_aes.should_raise():
            saved_aes.print_summary()
            raise saved_aes
