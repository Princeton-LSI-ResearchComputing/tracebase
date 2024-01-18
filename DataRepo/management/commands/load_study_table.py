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
    help = "Loads data from a study table (e.g. study code, name, and description) into the database."

    def add_arguments(self, parser):
        parser.add_argument(
            "--study-table",
            type=str,
            help=(
                "Path to either a tab-delimited or excel file (with a sheet named 'Study').  "
                "Required headers: 'Study ID', 'Name', and 'Description'."
            ),
            required=True,
        )

        parser.add_argument(
            "--sheet",
            type=str,
            help="Name of excel sheet/tab.  Only used if --study-table is an excel spreadsheet.  Default: 'Study'.",
            default="Study",
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="If supplied, nothing will be saved to the database.",
        )

        # Intended for use by load_study to prevent rollback of changes in the event of an error so that for example,
        # subsequent loading scripts can validate with all necessary data present.
        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY.  A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        saved_aes = None
        try:
            sheet = options["sheet"] if is_excel(options["study_table"]) else None
            study_table_df = read_from_file(options["study_table"], sheet=sheet)

            self.study_table_loader = StudyTableLoader(
                # Data
                study_table_df=study_table_df,
                # Modes
                dry_run=options["dry_run"],
                defer_rollback=options["defer_rollback"],
                # Used for error-reporting
                sheet=sheet,
                file=os.path.basename(sheet),
            )

            self.study_table_loader.load_study_table()

        except DryRun:
            pass
        except AggregatedErrors as aes:
            saved_aes = aes
        except Exception as e:
            # Add this error (which wasn't added to the aggregated errors, because it was unanticipated) to the
            # other buffered errors
            saved_aes = AggregatedErrors()
            saved_aes.buffer_error(e)
        finally:
            if saved_aes is not None and saved_aes.should_raise():
                saved_aes.print_summary()
                raise saved_aes
