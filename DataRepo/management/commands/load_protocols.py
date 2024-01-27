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
            sheet = options["sheet"] if is_excel(options["protocols"]) else None
            self.protocols_df, self.batch_category = self.read_from_file(
                options["protocols"]
            )

            loader = ProtocolsLoader(
                protocols=self.protocols_df,
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

    def read_from_file(self, filename, format=None):
        """
        Read protocols from a file and buffer their contents in the instance object to be loaded later
        """
        format_choices = ("tsv", "xlsx")
        batch_category = None

        if format is None:
            format = pathlib.Path(filename).suffix.strip(".")

        self.stdout.write(self.style.MIGRATE_HEADING("Loading animal treatments..."))
        if format == "tsv":
            protocols_df = self.read_protocols_tsv(filename)
        elif format == "xlsx":
            protocols_df, batch_category = self.read_protocols_xlsx(filename)
        else:
            raise CommandError(
                f"Invalid file format reading samples: [{format}], expected one of [{', '.join(format_choices)}].",
            )

        # Tidy the data up
        protocols_df = protocols_df.replace(np.nan, "", regex=True)
        for col in protocols_df.columns:
            protocols_df[col] = protocols_df[col].str.strip()

        return protocols_df, batch_category

    def read_protocols_tsv(self, protocols_tsv):
        # Keeping `na` to differentiate between intentional empty descriptions and spaces in the first column that were
        # intended to be tab characters
        protocols_df = read_from_file(
            protocols_tsv,
            dtype=object,
            keep_default_na=False,
            na_values="",
        )
        # rename template columns to ProtocolsLoader expectations
        protocols_df.rename(
            inplace=True,
            columns={
                str(self.name_header): ProtocolsLoader.NAME_HEADER,
                str(self.category_header): ProtocolsLoader.CTGY_HEADER,
                str(self.description_header): ProtocolsLoader.DESC_HEADER,
            },
        )

        return protocols_df

    def read_protocols_xlsx(self, xlxs_file_containing_treatments_sheet):
        name_header = self.TREATMENTS_NAME_HEADER
        description_header = self.TREATMENTS_DESC_HEADER

        protocols_df = read_from_file(
            xlxs_file_containing_treatments_sheet,
            sheet=self.TREATMENTS_SHEET_NAME,
            dtype={
                name_header: str,
                description_header: str,
            },
            keep_default_na=False,
        )

        # rename template columns to ProtocolsLoader expectations
        protocols_df.rename(
            inplace=True,
            columns={
                str(name_header): ProtocolsLoader.NAME_HEADER,
                str(description_header): ProtocolsLoader.DESC_HEADER,
            },
        )

        return protocols_df, self.TREATMENTS_CATEGORY_VALUE
