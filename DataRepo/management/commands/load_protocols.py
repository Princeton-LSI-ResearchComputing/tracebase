import argparse
import pathlib

import numpy as np
from django.core.management import BaseCommand, CommandError

from DataRepo.models.protocol import Protocol
from DataRepo.utils import AggregatedErrors, DryRun, ProtocolsLoader
from DataRepo.utils.file_utils import is_excel, read_from_file


class Command(BaseCommand):
    help = "Loads data from a protocol table into the database"

    name_header = "Name"
    category_header = "Category"
    description_header = "Description"

    # default XLXS template headers and assumed category value
    TREATMENTS_SHEET_NAME = "Treatments"
    TREATMENTS_NAME_HEADER = "Animal Treatment"
    TREATMENTS_DESC_HEADER = "Treatment Description"
    TREATMENTS_CATEGORY_VALUE = Protocol.ANIMAL_TREATMENT

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_category = None  # Set from the excel spreadsheet

    def add_arguments(self, parser):
        parser.add_argument(
            "--protocols",
            type=str,
            help=(
                "Path to either a tab-delimited file containing the headers "
                f"'{self.name_header}','{self.category_header}','{self.description_header}' "
                f"OR a path to an xlsx workbook file containing a sheet named '{self.TREATMENTS_SHEET_NAME}' "
                f"with the headers '{self.TREATMENTS_NAME_HEADER}','{self.TREATMENTS_DESC_HEADER}'"
            ),
            required=True,
        )

        parser.add_argument(
            "--sheet",
            type=str,
            help="Name of excel sheet/tab.  Only used if --protocols is an excel spreadsheet.  Default: 'Treatments'.",
            default="Tissues",
        )

        parser.add_argument(
            "--table-headers",
            type=str,
            help=f"YAML file defining headers to be used.",
        )

        parser.add_argument(
            "-n",
            "--dry-run",
            action="store_true",
            default=False,
            help=("Dry-run. If supplied, nothing will be saved to the database. "),
        )

        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY - A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        saved_aes = None
        msg = "Done. Protocol records loaded: [%i], skipped: [%i], and errored: [%i]."

        try:
            # TODO: Deal with the alternate headers for the treatments sheet
            header_data = read_from_file(options["table_headers"]) if options["table_headers"] else None
            headers = ProtocolsLoader.get_headers(header_data)
            sheet = None
            default_values = None
            if is_excel(options["protocols"]):
                # If this is an excel file, we will assume it is the treatments sheet
                if options["table_headers"] is None:
                    headers = ProtocolsLoader.get_headers(
                        {
                            ProtocolsLoader.NAME_KEY: self.TREATMENTS_NAME_HEADER,
                            ProtocolsLoader.CAT_KEY: None,
                            ProtocolsLoader.DESC_KEY: self.TREATMENTS_DESC_HEADER,
                        }
                    )

                if options["sheet"]:
                    sheet = options["sheet"]
                else:
                    sheet = self.TREATMENTS_SHEET_NAME

                default_values = ProtocolsLoader.get_defaults(
                    {ProtocolsLoader.CAT_KEY: self.TREATMENTS_CATEGORY_VALUE}
                )

            protocols_df = read_from_file(
                options["protocols"],
                dtype=ProtocolsLoader.get_column_types(headers)
            )

            loader = ProtocolsLoader(
                protocols=protocols_df,
                headers=headers,
                defaults=default_values,
                category=self.batch_category,
                dry_run=options["dry_run"],
                defer_rollback=options["defer_rollback"],
                sheet=sheet,
                file=options["protocols"],
            )

            loader.load_protocol_data()

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
            load_stats[Protocol.__name__]["created"],
            load_stats[Protocol.__name__]["skipped"],
            load_stats[Protocol.__name__]["errored"],
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
