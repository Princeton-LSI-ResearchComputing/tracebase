import argparse

from django.core.management import BaseCommand

from DataRepo.utils import (
    AggregatedErrors,
    DryRun,
    TissuesLoader,
    is_excel,
    read_from_file,
)


class Command(BaseCommand):
    help = "Loads data from a tissue table into the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--tissues",
            type=str,
            help=(
                "Path to either a tab-delimited file or excel file with a sheet named 'Tissues'.  "
                "Required headers: 'Tissue' & 'Description'"
            ),
            required=True,
        )

        parser.add_argument(
            "--sheet",
            type=str,
            help="Name of excel sheet/tab.  Only used if --tissues is an excel spreadsheet.  Default: 'Tissues'.",
            default="Tissues",
        )

        parser.add_argument(
            "-n",
            "--dry-run",
            action="store_true",
            default=False,
            help=("Dry-run. If supplied, nothing will be saved to the database."),
        )

        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY - A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        saved_aes = None
        msg = "Done. Tissue records loaded: [%i], skipped: [%i], and erroneous: [%i]."
        created = 0
        skipped = 0
        errors = 0

        try:
            sheet = options["sheet"] if is_excel(options["tissues"]) else None
            new_tissues = read_from_file(
                options["tissues"],
                dtype={"Name": str, "Description": str},
                sheet=sheet,
            )

            self.tissue_loader = TissuesLoader(
                tissues=new_tissues,
                dry_run=options["dry_run"],
                defer_rollback=options["defer_rollback"],
                sheet=sheet,
                file=options["tissues"],
            )

            created, skipped, errors = self.tissue_loader.load_tissue_data()
        except DryRun:
            pass
        except AggregatedErrors as aes:
            saved_aes = aes
        except Exception as e:
            # Add this unanticipated error to a new aggregated errors object
            saved_aes = AggregatedErrors()
            saved_aes.buffer_error(e)

        status = msg % (created, skipped, errors)

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
